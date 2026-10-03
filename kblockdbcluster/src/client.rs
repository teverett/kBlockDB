//! The connecting side of a peer link -- one long-running task per
//! configured peer, kept running by the embedder for the life of the
//! process, that keeps a connection to that one peer alive and streams it
//! every local write (see `hub::ReplicationHub`) as `ChangeBatch` frames.
//! See `wire.rs` for the wire format and `server.rs` for the accepting
//! side.

use crate::peers::PeerSet;
use crate::wire::{PeerMessage, PEER_PROTOCOL_VERSION};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::broadcast;

/// Whether a `run` task's connection to its one peer is currently up, and
/// if not, since when -- cheap to clone (`Arc`-shared) and read from
/// anywhere, so an embedder can report live link status (e.g. from a
/// health endpoint) without `run` itself knowing anything about who's
/// asking. Starts down (as of creation); `run` marks it up right after
/// `Hello`/`HelloOk` succeeds and down again the instant the connection
/// ends, for any reason. `PeerSet::prune` uses `down_for` to find peers
/// that have been unreachable too long.
#[derive(Clone)]
pub struct ConnectionStatus(Arc<Mutex<LinkState>>);

struct LinkState {
    connected: bool,
    /// `Some(when)` while down -- when it went down (or was created).
    down_since: Option<Instant>,
}

impl Default for ConnectionStatus {
    fn default() -> Self {
        ConnectionStatus(Arc::new(Mutex::new(LinkState {
            connected: false,
            down_since: Some(Instant::now()),
        })))
    }
}

impl ConnectionStatus {
    pub fn new() -> Self {
        Self::default()
    }

    /// Like `new`, but starts already marked connected -- for an
    /// embedder's own tests to simulate a peer that's up, without a real
    /// connection.
    pub fn connected() -> Self {
        let status = Self::new();
        status.set(true);
        status
    }

    pub fn is_connected(&self) -> bool {
        self.0.lock().unwrap().connected
    }

    /// How long the link has been continuously down, or `None` if it's up.
    pub fn down_for(&self) -> Option<Duration> {
        self.0
            .lock()
            .unwrap()
            .down_since
            .map(|since| since.elapsed())
    }

    /// Whether `self` and `other` are the same shared flag -- i.e. the
    /// same `PeerSet` entry, not merely one for the same address.
    pub(crate) fn same(&self, other: &ConnectionStatus) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    fn set(&self, connected: bool) {
        let mut state = self.0.lock().unwrap();
        if connected {
            state.down_since = None;
        } else if state.down_since.is_none() {
            state.down_since = Some(Instant::now());
        }
        state.connected = connected;
    }
}

/// How long to wait before the first reconnect attempt after a dropped
/// connection or a failed `connect` -- doubled on every further failure
/// up to `MAX_RECONNECT_DELAY`, so a peer that's briefly restarting is
/// retried quickly, but a peer that's gone for a while doesn't get
/// hammered.
const INITIAL_RECONNECT_DELAY: Duration = Duration::from_millis(500);
const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(30);

/// Up to how many entries get coalesced into one `ChangeBatch` frame --
/// after awaiting the first one, this task drains anything else already
/// sitting in the channel (non-blocking) up to this cap before sending,
/// rather than always sending one entry per frame. Just an efficiency
/// knob: correctness doesn't depend on the batch size.
const MAX_BATCH_SIZE: usize = 256;

/// Runs until the process exits: connect, authenticate, then forward every
/// entry the hub publishes -- plus a `PeerList` whenever `peers` changes
/// (gossip, see `peers.rs`) -- until the connection breaks, at which point
/// this reconnects with backoff and resumes. "Resumes" means picks up with
/// whatever's published *after* reconnecting, not a replay of anything
/// missed while disconnected (see docs/clustering.md's "Catch-up scope":
/// live-forward only for v1). Stops for good, instead, if `address` turns
/// out to reach this process itself or a node already linked under another
/// address (see `PeerSet::claim`), or once its entry has been pruned from
/// `peers` for being unreachable too long (see `PeerSet::prune`).
pub async fn run(address: String, peers: PeerSet, status: ConnectionStatus) {
    let mut delay = INITIAL_RECONNECT_DELAY;
    loop {
        if !peers.is_current(&address, &status) {
            return;
        }
        match connect_and_forward(&address, &peers, &status).await {
            Ok(()) => {
                status.set(false);
                return;
            }
            Err(e) => {
                status.set(false);
                eprintln!("peer client '{address}': {e} -- retrying in {delay:?}");
            }
        }
        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(MAX_RECONNECT_DELAY);
    }
}

/// `Ok(())` means stop for good (a self/alias address, or the hub was
/// dropped); `Err` means reconnect.
async fn connect_and_forward(
    address: &str,
    peers: &PeerSet,
    status: &ConnectionStatus,
) -> std::io::Result<()> {
    // Subscribed *before* connecting, not after `HelloOk` -- otherwise any
    // entry published while the TCP handshake/`Hello` round trip is still
    // in flight would be published to no one and silently lost (a
    // `broadcast` channel never buffers for a receiver that doesn't exist
    // yet). Subscribing first means those entries sit in this receiver's
    // backlog instead, same as while this task is connected and merely
    // slow to drain.
    let mut rx = peers.hub().subscribe();
    let mut changes = peers.subscribe_changes();

    let mut stream = TcpStream::connect(address).await?;
    crate::socket::tune(&stream, peers.keepalive());

    let identity = peers.identity();
    crate::wire::write_message(
        &mut stream,
        &PeerMessage::Hello {
            secret: identity.cluster_secret.clone(),
            server_id: identity.server_id.clone(),
            peer_port: identity.peer_port,
            node_id: peers.node_id(),
            protocol_version: PEER_PROTOCOL_VERSION,
        },
    )
    .await?;

    let remote_node = match crate::wire::read_message(&mut stream).await? {
        Some(PeerMessage::HelloOk { node_id }) => node_id,
        Some(PeerMessage::HelloRejected(reason)) => {
            return Err(std::io::Error::other(format!("rejected: {reason}")));
        }
        Some(_) => return Err(std::io::Error::other("unexpected reply to Hello")),
        None => return Err(std::io::Error::other("connection closed during Hello")),
    };
    // Pruned while this attempt was connecting -- don't revive it.
    if !peers.is_current(address, status) {
        return Ok(());
    }
    if !peers.claim(address, remote_node) {
        eprintln!(
            "peer client '{address}': reaches this server or an already-linked peer -- dropped"
        );
        return Ok(());
    }
    status.set(true);
    peers.notify();
    eprintln!("peer client '{address}': connected");

    // The far side never sends anything after `HelloOk`, so reading is
    // purely to notice it going away: EOF or a reset ends the link (and
    // starts the dead-peer clock) right away, instead of only on this
    // side's next write -- which on a quiet cluster may never come.
    let (mut reader, mut writer) = stream.split();
    let mut scratch = [0u8; 64];

    // Initial gossip, then again on every later change to `peers`.
    changes.borrow_and_update();
    send_peer_list(&mut writer, peers, address).await?;

    loop {
        tokio::select! {
            read = reader.read(&mut scratch) => {
                if read? == 0 {
                    return Err(std::io::Error::other("closed by peer"));
                }
            }
            received = rx.recv() => {
                // Take the first entry, then opportunistically drain more
                // without waiting, so a burst of writes becomes one batch
                // instead of one frame per entry.
                let first = match received {
                    Ok(entry) => entry,
                    Err(broadcast::error::RecvError::Closed) => return Ok(()),
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        // Fell behind the hub's buffer -- some entries were
                        // skipped. Accepted under this feature's
                        // "best-effort, live-forward-only" v1 scope; just
                        // resume with whatever comes next.
                        continue;
                    }
                };
                let mut batch = vec![first];
                while batch.len() < MAX_BATCH_SIZE {
                    match rx.try_recv() {
                        Ok(entry) => batch.push(entry),
                        Err(_) => break,
                    }
                }
                crate::wire::write_message(&mut writer, &PeerMessage::ChangeBatch(batch))
                    .await?;
            }
            changed = changes.changed() => {
                if changed.is_err() {
                    return Ok(());
                }
                send_peer_list(&mut writer, peers, address).await?;
            }
        }
    }
}

async fn send_peer_list<W: AsyncWrite + Unpin>(
    stream: &mut W,
    peers: &PeerSet,
    address: &str,
) -> std::io::Result<()> {
    crate::wire::write_message(stream, &PeerMessage::PeerList(peers.gossip_list(address))).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hub::{publish_remove, publish_set};
    use crate::test_support::{spawn_node, wait_until, TestNode, CLUSTER_SECRET};
    use kblockdblib::{CellMeta, Value};

    const DB: &str = "db";

    /// A `PeerSet` for a standalone `run` task to link from. Its claimed
    /// `peer_port` has nothing listening on it, so the receiving node's
    /// dial-back just retries harmlessly -- these tests exercise one
    /// direction only.
    fn standalone_peers() -> PeerSet {
        PeerSet::new(
            crate::peers::LocalIdentity {
                cluster_secret: CLUSTER_SECRET.to_string(),
                server_id: "node-a".to_string(),
                peer_port: 1,
            },
            crate::hub::ReplicationHub::new(),
        )
    }

    /// Retries `attempt` (expected to be `hub::publish_*`, cheap and
    /// idempotent-enough to call repeatedly with the same arguments) every
    /// 10ms for up to 2s, checking `condition` after each one, until it's
    /// satisfied. A single `publish` can land before `run`'s background
    /// task has gotten far enough to `subscribe` (a `broadcast` channel
    /// never buffers for a receiver that doesn't exist yet), so this is
    /// the tests' way of saying "keep trying until the other side is
    /// definitely listening" rather than racing a fixed delay.
    async fn publish_until_seen(mut attempt: impl FnMut(), mut condition: impl FnMut() -> bool) {
        for _ in 0..200 {
            attempt();
            if condition() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("condition was never met within 2s");
    }

    /// Regression: the link used to notice a dead peer only on its next
    /// write, so on a quiet cluster a peer that went away stayed
    /// "connected" forever -- and so was never pruned. The peer here
    /// handshakes, then closes without either side writing anything.
    #[tokio::test]
    async fn an_idle_link_notices_the_peer_closing() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let (close_tx, close_rx) = tokio::sync::oneshot::channel::<()>();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            crate::wire::read_message(&mut stream).await.unwrap();
            crate::wire::write_message(&mut stream, &PeerMessage::HelloOk { node_id: 42 })
                .await
                .unwrap();
            // Drain the link's initial gossip first: closing with unread
            // data would send a reset instead of a clean EOF, and this is
            // about the EOF.
            assert!(matches!(
                crate::wire::read_message(&mut stream).await.unwrap(),
                Some(PeerMessage::PeerList(_))
            ));
            let _ = close_rx.await;
            // Dropping both the stream and the listener: the link sees
            // EOF, and its reconnect attempts are refused.
        });

        let peers = standalone_peers();
        peers.add(addr.clone());
        wait_until(|| peers.snapshot() == vec![(addr.clone(), true)]).await;

        close_tx.send(()).unwrap();
        wait_until(|| peers.snapshot() == vec![(addr.clone(), false)]).await;
    }

    #[tokio::test]
    async fn a_published_entry_reaches_the_peer_over_a_real_connection() {
        let TestNode { addr, sink, .. } = spawn_node("node-b").await;
        let peers = standalone_peers();
        peers.add(addr.to_string());
        let replication = Some(peers.hub().clone());

        publish_until_seen(
            || {
                publish_set(
                    &replication,
                    DB,
                    &[1, 2, 3],
                    "material",
                    Value::Str("stone".to_string()),
                    CellMeta {
                        created_at_ms: 10,
                        modified_at_ms: 10,
                        version: 0,
                    },
                );
            },
            || {
                sink.get(DB, &[1, 2, 3], "material")
                    == Some((Value::Str("stone".to_string()), 10, 10, 0))
            },
        )
        .await;
    }

    #[tokio::test]
    async fn a_published_remove_reaches_the_peer_over_a_real_connection() {
        let TestNode { addr, sink, .. } = spawn_node("node-b").await;
        sink.apply_set_now(DB, &[4, 5, 6], "material", Value::Str("stone".to_string()));

        let peers = standalone_peers();
        peers.add(addr.to_string());
        let replication = Some(peers.hub().clone());

        publish_until_seen(
            || publish_remove(&replication, DB, &[4, 5, 6], "material", 20),
            || sink.is_removed(DB, &[4, 5, 6], "material"),
        )
        .await;
    }

    #[tokio::test]
    async fn multiple_published_entries_all_reach_the_peer() {
        let TestNode { addr, sink, .. } = spawn_node("node-b").await;
        let peers = standalone_peers();
        peers.add(addr.to_string());
        let replication = Some(peers.hub().clone());

        for i in 0..5 {
            publish_until_seen(
                || {
                    publish_set(
                        &replication,
                        DB,
                        &[i, 0, 0],
                        "k",
                        Value::I64(i as i64),
                        CellMeta {
                            created_at_ms: 1,
                            modified_at_ms: 1,
                            version: 0,
                        },
                    );
                },
                || sink.get(DB, &[i, 0, 0], "k") == Some((Value::I64(i as i64), 1, 1, 0)),
            )
            .await;
        }
    }

    #[test]
    fn a_new_status_is_down_and_counting() {
        let status = ConnectionStatus::new();
        assert!(!status.is_connected());
        assert!(status.down_for().is_some());
    }

    #[test]
    fn going_up_clears_down_for_and_going_down_restarts_it() {
        let status = ConnectionStatus::new();
        std::thread::sleep(Duration::from_millis(20));
        status.set(true);
        assert_eq!(status.down_for(), None);

        status.set(false);
        assert!(status.down_for().unwrap() < Duration::from_millis(20));
    }

    #[test]
    fn staying_down_keeps_the_original_down_since() {
        let status = ConnectionStatus::new();
        std::thread::sleep(Duration::from_millis(20));
        status.set(false); // another failed reconnect attempt
        assert!(status.down_for().unwrap() >= Duration::from_millis(20));
    }

    #[tokio::test]
    async fn a_link_comes_up_once_hello_succeeds() {
        let TestNode { addr, .. } = spawn_node("node-b").await;
        let peers = standalone_peers();
        peers.add(addr.to_string());
        wait_until(|| peers.snapshot() == vec![(addr.to_string(), true)]).await;
    }

    #[tokio::test]
    async fn a_link_to_an_unreachable_peer_stays_down() {
        let peers = standalone_peers();
        // Nothing is listening on this port.
        peers.add("127.0.0.1:1".to_string());
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(peers.snapshot(), vec![("127.0.0.1:1".to_string(), false)]);
    }

    #[tokio::test]
    async fn a_pruned_link_task_stops_instead_of_reviving_its_peer() {
        let TestNode { addr, .. } = spawn_node("node-b").await;
        let peers = standalone_peers();
        let status = ConnectionStatus::new();
        // An entry whose status is a *different* flag than the one this
        // task holds -- exactly what a task sees after its entry was
        // pruned and the address re-added.
        peers.insert(addr.to_string(), ConnectionStatus::new());

        run(addr.to_string(), peers.clone(), status.clone()).await;
        assert!(!status.is_connected());
    }
}
