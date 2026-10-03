//! The connecting side of a peer link -- one long-running task per
//! configured peer, kept running by the embedder for the life of the
//! process, that keeps a connection to that one peer alive and streams it
//! every local write (see `hub::ReplicationHub`) as `ChangeBatch` frames.
//! See `wire.rs` for the wire format and `server.rs` for the accepting
//! side.

use crate::hub::ReplicationHub;
use crate::wire::{PeerMessage, PEER_PROTOCOL_VERSION};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::broadcast;

/// Whether a `run` task's connection to its one peer is currently up --
/// cheap to clone (an `Arc`'d flag) and read from anywhere, so an embedder
/// can report live outbound connection status (e.g. from a health
/// endpoint) without `run` itself knowing anything about who's asking.
/// Starts `false`; `run` flips it `true` right after `Hello`/`HelloOk`
/// succeeds and back to `false` the instant the connection ends, for any
/// reason (including right before every reconnect attempt, which may
/// itself take a while under backoff).
#[derive(Clone, Default)]
pub struct ConnectionStatus(Arc<AtomicBool>);

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
        self.0.load(Ordering::Relaxed)
    }

    fn set(&self, connected: bool) {
        self.0.store(connected, Ordering::Relaxed);
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

/// Runs forever (until the process exits): connect, authenticate, then
/// forward every entry `hub` publishes until the connection breaks, at
/// which point this reconnects with backoff and resumes -- "resumes"
/// meaning picks up with whatever's published *after* reconnecting, not
/// a replay of anything missed while disconnected (see
/// docs/clustering.md's "Catch-up scope": live-forward only for v1).
pub async fn run(
    address: String,
    cluster_secret: String,
    server_id: String,
    hub: Arc<ReplicationHub>,
    status: ConnectionStatus,
) {
    let mut delay = INITIAL_RECONNECT_DELAY;
    loop {
        match connect_and_forward(&address, &cluster_secret, &server_id, &hub, &status).await {
            Ok(()) => {
                // `connect_and_forward` only returns `Ok` if the hub's
                // sender was dropped, which never happens while the
                // server process is alive -- but if it ever did, retrying
                // forever would be pointless spinning.
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

async fn connect_and_forward(
    address: &str,
    cluster_secret: &str,
    server_id: &str,
    hub: &Arc<ReplicationHub>,
    status: &ConnectionStatus,
) -> std::io::Result<()> {
    // Subscribed *before* connecting, not after `HelloOk` -- otherwise any
    // entry published while the TCP handshake/`Hello` round trip is still
    // in flight would be published to no one and silently lost (a
    // `broadcast` channel never buffers for a receiver that doesn't exist
    // yet). Subscribing first means those entries sit in this receiver's
    // backlog instead, same as while this task is connected and merely
    // slow to drain.
    let mut rx = hub.subscribe();

    let mut stream = TcpStream::connect(address).await?;
    let _ = stream.set_nodelay(true);

    crate::wire::write_message(
        &mut stream,
        &PeerMessage::Hello {
            secret: cluster_secret.to_string(),
            server_id: server_id.to_string(),
            protocol_version: PEER_PROTOCOL_VERSION,
        },
    )
    .await?;

    match crate::wire::read_message(&mut stream).await? {
        Some(PeerMessage::HelloOk) => {}
        Some(PeerMessage::HelloRejected(reason)) => {
            return Err(std::io::Error::other(format!("rejected: {reason}")));
        }
        Some(_) => return Err(std::io::Error::other("unexpected reply to Hello")),
        None => return Err(std::io::Error::other("connection closed during Hello")),
    }
    status.set(true);
    eprintln!("peer client '{address}': connected");

    loop {
        // Block for the first entry, then opportunistically drain more
        // without waiting, so a burst of writes becomes one batch instead
        // of one frame per entry.
        let first = match rx.recv().await {
            Ok(entry) => entry,
            Err(broadcast::error::RecvError::Closed) => return Ok(()),
            Err(broadcast::error::RecvError::Lagged(_)) => {
                // Fell behind the hub's buffer -- some entries were
                // skipped. Accepted under this feature's "best-effort,
                // live-forward-only" v1 scope (see this module's doc
                // comment); just resume with whatever comes next.
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
        crate::wire::write_message(&mut stream, &PeerMessage::ChangeBatch(batch)).await?;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hub::{publish_remove, publish_set};
    use crate::test_support::RecordingSink;
    use kblockdblib::{CellMeta, Value};
    use tokio::net::TcpListener;

    const CLUSTER_SECRET: &str = "cluster-secret";
    const DB: &str = "db";

    /// A real `server::serve`, backed by a `RecordingSink` rather than any
    /// actual storage -- the accepting side of the end-to-end tests below.
    async fn spawn_test_server() -> (std::net::SocketAddr, RecordingSink) {
        let sink = RecordingSink::new();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(crate::server::serve(
            listener,
            sink.clone(),
            CLUSTER_SECRET.to_string(),
            crate::registry::PeerRegistry::new(),
        ));
        (addr, sink)
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

    #[tokio::test]
    async fn a_published_entry_reaches_the_peer_over_a_real_connection() {
        let (addr, sink) = spawn_test_server().await;
        let hub = ReplicationHub::new();
        tokio::spawn(run(
            addr.to_string(),
            CLUSTER_SECRET.to_string(),
            "node-a".to_string(),
            hub.clone(),
            ConnectionStatus::new(),
        ));
        let replication = Some(hub);

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
        let (addr, sink) = spawn_test_server().await;
        sink.apply_set_now(DB, &[4, 5, 6], "material", Value::Str("stone".to_string()));

        let hub = ReplicationHub::new();
        tokio::spawn(run(
            addr.to_string(),
            CLUSTER_SECRET.to_string(),
            "node-a".to_string(),
            hub.clone(),
            ConnectionStatus::new(),
        ));
        let replication = Some(hub);

        publish_until_seen(
            || publish_remove(&replication, DB, &[4, 5, 6], "material", 20),
            || sink.is_removed(DB, &[4, 5, 6], "material"),
        )
        .await;
    }

    #[tokio::test]
    async fn multiple_published_entries_all_reach_the_peer() {
        let (addr, sink) = spawn_test_server().await;
        let hub = ReplicationHub::new();
        tokio::spawn(run(
            addr.to_string(),
            CLUSTER_SECRET.to_string(),
            "node-a".to_string(),
            hub.clone(),
            ConnectionStatus::new(),
        ));
        let replication = Some(hub);

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

    #[tokio::test]
    async fn status_is_disconnected_until_hello_succeeds() {
        let (addr, _sink) = spawn_test_server().await;
        let status = ConnectionStatus::new();
        assert!(!status.is_connected());

        tokio::spawn(run(
            addr.to_string(),
            CLUSTER_SECRET.to_string(),
            "node-a".to_string(),
            ReplicationHub::new(),
            status.clone(),
        ));

        wait_until(|| status.is_connected()).await;
    }

    #[tokio::test]
    async fn status_goes_back_to_disconnected_when_the_peer_is_unreachable() {
        // Nothing is listening on this port.
        let status = ConnectionStatus::new();
        tokio::spawn(run(
            "127.0.0.1:1".to_string(),
            CLUSTER_SECRET.to_string(),
            "node-a".to_string(),
            ReplicationHub::new(),
            status.clone(),
        ));

        // Give the first connect attempt a moment to fail, then assert it
        // never flipped to connected.
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!status.is_connected());
    }

    async fn wait_until(mut condition: impl FnMut() -> bool) {
        for _ in 0..200 {
            if condition() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("condition was never met within 2s");
    }
}
