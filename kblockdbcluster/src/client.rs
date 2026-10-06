//! The connecting side of a peer link -- one long-running task per
//! configured peer, kept running by the embedder for the life of the
//! process, that keeps a connection to that one peer alive and streams it
//! every local write (see `hub::ReplicationHub`) as `ChangeBatch` frames,
//! plus whatever catch-up the peer asks for (see `peers.rs`'s
//! "Catch-up"). See `wire.rs` for the wire format and `server.rs` for the
//! accepting side.

use crate::peers::PeerSet;
use crate::wire::{ChangeEntry, PeerMessage, PEER_PROTOCOL_VERSION};
use kblockdblib::{Stamp, VersionVector};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::AsyncWrite;
use tokio::net::TcpStream;
use tokio::sync::{broadcast, mpsc, Notify};

/// Whether a `run` task's connection to its one peer is currently up, and
/// if not, since when -- cheap to clone (`Arc`-shared) and read from
/// anywhere, so an embedder can report live link status (e.g. from a
/// health endpoint) without `run` itself knowing anything about who's
/// asking. Starts down (as of creation); `run` marks it up right after
/// `Hello`/`HelloOk` succeeds and down again the instant the connection
/// ends, for any reason. `PeerSet::prune` uses `down_for` to find peers
/// that have been unreachable too long.
///
/// Also carries the link's *wake* signal (see `wake`), which cuts a
/// reconnect backoff short.
#[derive(Clone)]
pub struct ConnectionStatus {
    state: Arc<Mutex<LinkState>>,
    wake: Arc<Notify>,
}

struct LinkState {
    connected: bool,
    /// `Some(when)` while down -- when it went down (or was created).
    down_since: Option<Instant>,
}

impl Default for ConnectionStatus {
    fn default() -> Self {
        ConnectionStatus {
            state: Arc::new(Mutex::new(LinkState {
                connected: false,
                down_since: Some(Instant::now()),
            })),
            wake: Arc::new(Notify::new()),
        }
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
        self.state.lock().unwrap().connected
    }

    /// How long the link has been continuously down, or `None` if it's up.
    pub fn down_for(&self) -> Option<Duration> {
        self.state
            .lock()
            .unwrap()
            .down_since
            .map(|since| since.elapsed())
    }

    /// Whether `self` and `other` are the same shared flag -- i.e. the
    /// same `PeerSet` entry, not merely one for the same address.
    pub(crate) fn same(&self, other: &ConnectionStatus) -> bool {
        Arc::ptr_eq(&self.state, &other.state)
    }

    /// Ends the link's current reconnect backoff early: it retries now, and
    /// from the initial delay again. A no-op for a link that's up; if it's
    /// mid-attempt, the attempt's own outcome decides, and a failure
    /// retries straight away.
    pub fn wake(&self) {
        self.wake.notify_one();
    }

    fn set(&self, connected: bool) {
        let mut state = self.state.lock().unwrap();
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

/// How often a caught-up, idle link confirms with `Synced` -- the most a
/// receiver's vector entry for this process can lag behind.
const SYNCED_INTERVAL: Duration = if cfg!(test) {
    Duration::from_millis(100)
} else {
    Duration::from_secs(5)
};

/// How often this link sends a `SyncReport` (see `source::ChangeSource::
/// sync_state`) -- its own cadence, slower and independent of
/// `SYNCED_INTERVAL`: a content digest comparison is a periodic
/// "actually verify the data matches" check, not part of the
/// write-replication/catch-up machinery `Synced` confirms.
const SYNC_REPORT_INTERVAL: Duration = if cfg!(test) {
    Duration::from_millis(100)
} else {
    Duration::from_secs(60)
};

/// Catch-up batches buffered between the blocking scan and the link: a
/// slow peer holds the scan back instead of it buffering a whole database.
const CATCH_UP_BUFFER: usize = 4;

/// A catch-up in progress: batches from `ChangeSource::changes_since`
/// running on a blocking thread, and this process's own vector as of just
/// before it started -- what the `Synced` sent once it finishes confirms:
/// every write that vector covers is either in the scan or (if it landed
/// after the scan passed its chunk) one of this node's own, published
/// live.
struct CatchUp {
    batches: mpsc::Receiver<Result<Vec<ChangeEntry>, String>>,
    start: VersionVector,
}

/// Aborts a spawned task when dropped -- this link's reader task, so it
/// never outlives the link.
struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Runs until the process exits: connect, authenticate, then forward every
/// entry the hub publishes -- plus a `PeerList` whenever `peers` changes
/// (gossip, see `peers.rs`), and any catch-up the peer asks for -- until
/// the connection breaks, at which point this reconnects with backoff and
/// resumes. Anything missed while disconnected comes back through the
/// peer's next `CatchUpRequest`, not from here. Stops for good, instead, if `address` turns
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
                // A link that was up starts its backoff over: it's a fresh
                // drop, not the latest in a run of failed attempts.
                if status.is_connected() {
                    delay = INITIAL_RECONNECT_DELAY;
                }
                status.set(false);
                eprintln!("peer client '{address}': {e} -- retrying in {delay:?}");
            }
        }
        // `wake` (the peer just connected in, so it's back) cuts this
        // short -- see `PeerSet::wake`.
        let woken = tokio::select! {
            _ = tokio::time::sleep(delay) => false,
            _ = status.wake.notified() => true,
        };
        delay = if woken {
            eprintln!("peer client '{address}': peer is back -- reconnecting now");
            INITIAL_RECONNECT_DELAY
        } else {
            (delay * 2).min(MAX_RECONNECT_DELAY)
        };
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
    let mut index_ops = peers.hub().subscribe_index_ops();
    let mut changes = peers.subscribe_changes();
    let mut resync = peers.hub().sequencer().subscribe_resync();

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
            advertised_host: identity.advertised_host.clone(),
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

    // The far side only ever sends `CatchUpRequest`s, but reading also
    // notices it going away: EOF or a reset ends the link (and starts the
    // dead-peer clock) right away, instead of only on this side's next
    // write -- which on a quiet cluster may never come. Frames are read on
    // their own task: a half-read frame can't be abandoned mid-`select!`.
    let (mut reader, mut writer) = stream.into_split();
    let (incoming_tx, mut incoming) = mpsc::channel::<std::io::Result<PeerMessage>>(8);
    let _reader_task = AbortOnDrop(tokio::spawn(async move {
        loop {
            let msg = match crate::wire::read_message(&mut reader).await {
                Ok(Some(msg)) => Ok(msg),
                Ok(None) => Err(std::io::Error::other("closed by peer")),
                Err(e) => Err(e),
            };
            let failed = msg.is_err();
            if incoming_tx.send(msg).await.is_err() || failed {
                return;
            }
        }
    }));

    // Initial gossip, then again on every later change to `peers`.
    changes.borrow_and_update();
    send_peer_list(&mut writer, peers, address).await?;

    let node_id = peers.node_id();
    let mut catch_up: Option<CatchUp> = None;
    // Another catch-up to run once the current one finishes, from here.
    let mut queued: Option<VersionVector> = None;
    // The last vector confirmed to this peer -- `None` until its first
    // catch-up finishes.
    let mut confirmed: Option<VersionVector> = None;
    // Sent once, right after this link's *first* `Synced` -- not
    // eagerly on connect, which would race the catch-up this same link
    // is about to stream: a database an `IndexState` entry names might
    // not exist on the peer yet (it's created by the very `ChangeBatch`
    // this catch-up is sending), so sending index state only once that's
    // confirmed delivered means `apply_index_op` never sees a database
    // that's about to exist but doesn't yet -- see
    // `source::ChangeSource::indexed_keys`'s doc comment.
    let mut index_state_sent = false;
    let mut synced_tick = tokio::time::interval(SYNCED_INTERVAL);
    synced_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Starts ticking immediately (unlike waiting for the first `Synced`,
    // `IndexState`'s reasoning doesn't apply here: a `SyncReport` only
    // ever reports on databases this process already has a content
    // digest for -- see `ChangeSource::sync_state`'s doc comment -- so
    // there's no "reports on a database that doesn't exist yet" race to
    // avoid).
    let mut sync_report_tick = tokio::time::interval(SYNC_REPORT_INTERVAL);
    sync_report_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    resync.borrow_and_update();

    loop {
        // Catch the peer up from `known`: now, or after the running one.
        let mut request_catch_up = |known: VersionVector, catch_up: &mut Option<CatchUp>| {
            if catch_up.is_some() {
                queued = Some(match queued.take() {
                    Some(q) => q.meet(&known),
                    None => known,
                });
            } else {
                *catch_up = start_catch_up(peers, address, known);
            }
        };
        tokio::select! {
            msg = incoming.recv() => {
                match msg {
                    Some(Ok(PeerMessage::CatchUpRequest { known })) => {
                        request_catch_up(known, &mut catch_up);
                    }
                    // The accepting side's last `SyncReport` disagreed on
                    // `database` -- send it our own `chunk_digests()` for
                    // it, so it can find exactly which chunk(s) differ.
                    Some(Ok(PeerMessage::ChunkDigestsRequest { database })) => {
                        send_chunk_digests(&mut writer, peers, database).await?;
                    }
                    // Nothing else is expected this direction; skip it.
                    Some(Ok(_)) => {}
                    Some(Err(e)) => return Err(e),
                    None => return Err(std::io::Error::other("closed by peer")),
                }
            }
            batch = next_catch_up_batch(&mut catch_up) => {
                match batch {
                    Some(Ok(entries)) => {
                        crate::wire::write_message(&mut writer, &PeerMessage::ChangeBatch(entries))
                            .await?;
                    }
                    Some(Err(e)) => {
                        // The peer asks again when the link comes back.
                        return Err(std::io::Error::other(format!("catch-up failed: {e}")));
                    }
                    None => {
                        let start = catch_up.take().map(|c| c.start).unwrap_or_default();
                        if let Some(known) = queued.take() {
                            catch_up = start_catch_up(peers, address, known);
                        } else {
                            let synced = PeerMessage::Synced {
                                confirmed: start.clone(),
                                vector: peers.local_vector(),
                            };
                            crate::wire::write_message(&mut writer, &synced).await?;
                            confirmed = Some(start);
                            if !index_state_sent {
                                send_index_state(&mut writer, peers).await?;
                                index_state_sent = true;
                            }
                        }
                    }
                }
            }
            _ = synced_tick.tick() => {
                // Only once caught up, and only while nothing is pending:
                // `Synced` promises everything it confirms has been sent.
                // This node's confirmed seq is read *before* checking the
                // hub is drained, so every write up to it -- published
                // before that read -- has already gone out on this link.
                if let (None, None, Some(confirmed)) = (&catch_up, &queued, &mut confirmed) {
                    let through = peers.hub().sequencer().confirmed_through();
                    // A pending resync means `through` may count a write
                    // that was stored but never sent (see
                    // `Sequencer::release`): catch up first, confirm after.
                    let resync_pending = resync.has_changed().unwrap_or(false);
                    if rx.is_empty() && !resync_pending {
                        confirmed.observe(Stamp::new(node_id, through));
                        let synced = PeerMessage::Synced {
                            confirmed: [(node_id, through)].into_iter().collect(),
                            vector: peers.local_vector(),
                        };
                        crate::wire::write_message(&mut writer, &synced).await?;
                    }
                }
            }
            changed = resync.changed() => {
                if changed.is_err() {
                    return Ok(());
                }
                // A write landed in storage but was never published (see
                // `Sequencer::release`): catch up from what this peer was
                // last confirmed, which can't include it.
                if let Some(base) = catch_up.as_ref().map(|c| c.start.clone()).or(confirmed.clone()) {
                    eprintln!("peer client '{address}': a write was never published -- catching up again");
                    request_catch_up(base, &mut catch_up);
                }
            }
            received = rx.recv() => {
                // Take the first entry, then opportunistically drain more
                // without waiting, so a burst of writes becomes one batch
                // instead of one frame per entry.
                let first = match received {
                    Ok(entry) => entry,
                    Err(broadcast::error::RecvError::Closed) => return Ok(()),
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        // Fell behind the hub's buffer -- `skipped` entries
                        // are gone from it. They're still in storage, so
                        // catch this peer up again from what it was last
                        // confirmed (or the running catch-up's start).
                        // Before any catch-up was asked for, there's
                        // nothing to resume from.
                        if let Some(base) = catch_up.as_ref().map(|c| c.start.clone()).or(confirmed.clone()) {
                            eprintln!(
                                "peer client '{address}': fell {skipped} entries behind -- \
                                 catching up again"
                            );
                            request_catch_up(base, &mut catch_up);
                        }
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
            received = index_ops.recv() => {
                match received {
                    Ok(entry) => {
                        crate::wire::write_message(&mut writer, &PeerMessage::IndexOp(entry))
                            .await?;
                    }
                    Err(broadcast::error::RecvError::Closed) => return Ok(()),
                    // Fire-and-forget, same as `IndexOpEntry`'s doc comment
                    // says: there's no catch-up to fall back on for a
                    // missed index op, unlike a lagged `ChangeEntry` --
                    // the operator just re-runs the statement.
                    Err(broadcast::error::RecvError::Lagged(_)) => {}
                }
            }
            _ = sync_report_tick.tick() => {
                send_sync_report(&mut writer, peers).await?;
            }
        }
    }
}

/// Starts streaming every change `known` lacks from `peers`' change source
/// on a blocking thread. `None` (logged) if there's no source.
fn start_catch_up(peers: &PeerSet, address: &str, known: VersionVector) -> Option<CatchUp> {
    let Some(source) = peers.change_source().cloned() else {
        eprintln!("peer client '{address}': asked to catch up, but no change source is set");
        return None;
    };
    eprintln!("peer client '{address}': catching up from {known:?}");
    // Taken before the scan starts -- see `CatchUp::start`.
    let start = peers.local_vector();
    let legacy_origin = kblockdblib::legacy_origin(peers.node_id());
    let (tx, batches) = mpsc::channel(CATCH_UP_BUFFER);
    tokio::task::spawn_blocking(move || {
        let mut emit = |mut batch: Vec<ChangeEntry>| {
            while !batch.is_empty() {
                let rest = batch.split_off(batch.len().min(MAX_BATCH_SIZE));
                if tx.blocking_send(Ok(batch)).is_err() {
                    return false; // the link went away
                }
                batch = rest;
            }
            true
        };
        if let Err(e) = source.changes_since(&known, legacy_origin, &mut emit) {
            let _ = tx.blocking_send(Err(e));
        }
    });
    Some(CatchUp { batches, start })
}

/// The next batch of the running catch-up; `None` once it's finished.
/// Never resolves when there's no catch-up running.
async fn next_catch_up_batch(
    catch_up: &mut Option<CatchUp>,
) -> Option<Result<Vec<ChangeEntry>, String>> {
    match catch_up {
        Some(catch_up) => catch_up.batches.recv().await,
        None => std::future::pending().await,
    }
}

async fn send_peer_list<W: AsyncWrite + Unpin>(
    stream: &mut W,
    peers: &PeerSet,
    address: &str,
) -> std::io::Result<()> {
    crate::wire::write_message(stream, &PeerMessage::PeerList(peers.gossip_list(address))).await
}

/// Sends this process's full current index state (see
/// `source::ChangeSource::indexed_keys`) as one `IndexState` frame, or
/// nothing at all if there's no change source or it reports no indexes --
/// an empty frame would be a harmless no-op on the receiving end anyway,
/// but there's no reason to send one.
async fn send_index_state<W: AsyncWrite + Unpin>(
    stream: &mut W,
    peers: &PeerSet,
) -> std::io::Result<()> {
    let Some(source) = peers.change_source().cloned() else {
        return Ok(());
    };
    let entries = tokio::task::spawn_blocking(move || source.indexed_keys())
        .await
        .map_err(|e| std::io::Error::other(format!("indexed_keys panicked: {e}")))?
        .map_err(std::io::Error::other)?;
    if entries.is_empty() {
        return Ok(());
    }
    crate::wire::write_message(stream, &PeerMessage::IndexState(entries)).await
}

/// Sends this process's current sync state (see `source::ChangeSource::
/// sync_state`) as one `SyncReport` frame, on its own `SYNC_REPORT_
/// INTERVAL` cadence -- or nothing if there's no change source or it
/// reports on no databases (no content digest enabled anywhere), same
/// "no reason to send an empty frame" reasoning as `send_index_state`.
async fn send_sync_report<W: AsyncWrite + Unpin>(
    stream: &mut W,
    peers: &PeerSet,
) -> std::io::Result<()> {
    let Some(source) = peers.change_source().cloned() else {
        return Ok(());
    };
    let report = tokio::task::spawn_blocking(move || source.sync_state())
        .await
        .map_err(|e| std::io::Error::other(format!("sync_state panicked: {e}")))?
        .map_err(std::io::Error::other)?;
    if report.is_empty() {
        return Ok(());
    }
    crate::wire::write_message(stream, &PeerMessage::SyncReport(report)).await
}

/// Sends this process's `chunk_digests()` for `database` (see
/// `source::ChangeSource::chunk_digests`) as one `ChunkDigests` frame, in
/// reply to a `ChunkDigestsRequest` -- unlike `send_sync_report`/
/// `send_index_state`, always sent (even if empty), since it's a direct
/// answer to an explicit request rather than something offered on a
/// cadence.
async fn send_chunk_digests<W: AsyncWrite + Unpin>(
    stream: &mut W,
    peers: &PeerSet,
    database: String,
) -> std::io::Result<()> {
    let Some(source) = peers.change_source().cloned() else {
        return Ok(());
    };
    let for_scan = database.clone();
    let digests = tokio::task::spawn_blocking(move || source.chunk_digests(&for_scan))
        .await
        .map_err(|e| std::io::Error::other(format!("chunk_digests panicked: {e}")))?
        .map_err(std::io::Error::other)?;
    crate::wire::write_message(stream, &PeerMessage::ChunkDigests { database, digests }).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hub::{publish_remove, publish_set};
    use crate::test_support::{spawn_node, wait_until, RecordingSink, TestNode, CLUSTER_SECRET};
    use crate::wire::ChangeOp;
    use kblockdblib::{CellMeta, Stamp, Value};

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
                advertised_host: None,
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

    /// A `standalone_peers` link that catches up from `source`, through a
    /// hub of `hub_capacity`.
    fn catch_up_peers(source: &RecordingSink, hub_capacity: usize) -> PeerSet {
        PeerSet::new(
            crate::peers::LocalIdentity {
                cluster_secret: CLUSTER_SECRET.to_string(),
                server_id: "node-a".to_string(),
                peer_port: 1,
                advertised_host: None,
            },
            crate::hub::ReplicationHub::with_capacity(hub_capacity),
        )
        .with_change_source(Arc::new(source.clone()))
    }

    /// Plays the receiving side by hand: accepts the link's connection,
    /// answers `Hello`, and reads its initial `PeerList`.
    async fn accept_link(listener: &tokio::net::TcpListener) -> TcpStream {
        let (mut stream, _) = listener.accept().await.unwrap();
        assert!(matches!(
            crate::wire::read_message(&mut stream).await.unwrap(),
            Some(PeerMessage::Hello { .. })
        ));
        crate::wire::write_message(&mut stream, &PeerMessage::HelloOk { node_id: 42 })
            .await
            .unwrap();
        assert!(matches!(
            crate::wire::read_message(&mut stream).await.unwrap(),
            Some(PeerMessage::PeerList(_))
        ));
        stream
    }

    #[tokio::test]
    async fn connecting_sends_its_configured_advertised_host_in_hello() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peers = PeerSet::new(
            crate::peers::LocalIdentity {
                cluster_secret: CLUSTER_SECRET.to_string(),
                server_id: "node-a".to_string(),
                peer_port: 1,
                advertised_host: Some("gateway.invalid".to_string()),
            },
            crate::hub::ReplicationHub::new(),
        );
        peers.add(listener.local_addr().unwrap().to_string());

        let (mut stream, _) = listener.accept().await.unwrap();
        match crate::wire::read_message(&mut stream).await.unwrap() {
            Some(PeerMessage::Hello { advertised_host, .. }) => {
                assert_eq!(advertised_host.as_deref(), Some("gateway.invalid"));
            }
            other => panic!("expected Hello, got {other:?}"),
        }
    }

    /// Reads until a `Synced`, returning every entry received before it
    /// and the vector it confirmed.
    async fn read_until_synced(stream: &mut TcpStream) -> (Vec<ChangeEntry>, VersionVector) {
        let mut entries = Vec::new();
        loop {
            let msg =
                tokio::time::timeout(Duration::from_secs(2), crate::wire::read_message(stream))
                    .await
                    .expect("no Synced within 2s")
                    .unwrap();
            match msg {
                Some(PeerMessage::ChangeBatch(batch)) => entries.extend(batch),
                Some(PeerMessage::Synced { confirmed, .. }) => return (entries, confirmed),
                Some(PeerMessage::PeerList(_)) => {}
                other => panic!("unexpected {other:?}"),
            }
        }
    }

    fn vector(entries: &[(u64, u64)]) -> VersionVector {
        entries.iter().copied().collect()
    }

    const OTHER: u64 = 0xB;

    #[tokio::test]
    async fn a_catch_up_request_streams_what_the_vector_lacks_then_synced() {
        let source = RecordingSink::new();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peers = catch_up_peers(&source, 64);
        let me = peers.node_id();
        source.seed(DB, &[1], "k", Value::I64(1), 100, Stamp::new(OTHER, 1));
        source.seed(DB, &[2], "k", Value::I64(2), 300, Stamp::new(OTHER, 2));
        source.seed_removed(DB, &[3], "k", 400, Stamp::new(OTHER, 3));
        source.seed(DB, &[4], "k", Value::I64(4), 50, Stamp::NONE);
        peers.add(listener.local_addr().unwrap().to_string());
        let mut stream = accept_link(&listener).await;

        let request = PeerMessage::CatchUpRequest {
            known: vector(&[(OTHER, 1)]),
        };
        crate::wire::write_message(&mut stream, &request)
            .await
            .unwrap();
        let (mut entries, confirmed) = read_until_synced(&mut stream).await;
        entries.sort_by(|a, b| a.coord.cmp(&b.coord));
        let got: Vec<(Vec<i32>, ChangeOp, Stamp)> = entries
            .iter()
            .map(|e| (e.coord.clone(), e.op.clone(), e.stamp()))
            .collect();
        assert_eq!(
            got,
            vec![
                (vec![2], ChangeOp::Set(Value::I64(2)), Stamp::new(OTHER, 2)),
                (vec![3], ChangeOp::Remove, Stamp::new(OTHER, 3)),
                // Legacy data: the request had no entry for this node's.
                (vec![4], ChangeOp::Set(Value::I64(4)), Stamp::NONE),
            ]
        );
        // Confirms the sender's own vector as of the catch-up's start:
        // its own writes (none yet) and its legacy data.
        assert_eq!(
            confirmed,
            vector(&[(me, 0), (kblockdblib::legacy_origin(me), 0)])
        );

        // Then, idle and caught up, it keeps confirming its own writes.
        let seq = peers.hub().sequencer().assign().seq;
        peers.hub().sequencer().published(seq);
        let (entries, confirmed) = read_until_synced(&mut stream).await;
        assert!(entries.is_empty());
        assert!(confirmed.get(me) <= Some(seq));
        loop {
            let (_, confirmed) = read_until_synced(&mut stream).await;
            if confirmed == vector(&[(me, seq)]) {
                break;
            }
        }
    }

    #[tokio::test]
    async fn no_synced_is_sent_before_a_catch_up_was_asked_for() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peers = catch_up_peers(&RecordingSink::new(), 64);
        peers.add(listener.local_addr().unwrap().to_string());
        let mut stream = accept_link(&listener).await;
        let next =
            tokio::time::timeout(SYNCED_INTERVAL * 4, crate::wire::read_message(&mut stream)).await;
        assert!(next.is_err(), "expected nothing, got {next:?}");
    }

    #[tokio::test]
    async fn a_link_that_falls_behind_the_hub_catches_itself_up() {
        let source = RecordingSink::new();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peers = catch_up_peers(&source, 4);
        let replication = Some(peers.hub().clone());
        peers.add(listener.local_addr().unwrap().to_string());
        let mut stream = accept_link(&listener).await;
        let request = PeerMessage::CatchUpRequest {
            known: VersionVector::new(),
        };
        crate::wire::write_message(&mut stream, &request)
            .await
            .unwrap();
        read_until_synced(&mut stream).await;

        // 20 writes with no await in between: on this single-threaded
        // runtime the link can't drain the 4-entry hub until they're all
        // published, so it lags and loses most of them from the hub.
        for i in 0..20 {
            let meta = CellMeta {
                created_at_ms: 100,
                modified_at_ms: 100,
                version: 0,
            };
            let stamp = peers.hub().sequencer().assign();
            source.seed(DB, &[i], "k", Value::I64(i64::from(i)), 100, stamp);
            publish_set(
                &replication,
                DB,
                &[i],
                "k",
                Value::I64(i64::from(i)),
                meta,
                stamp,
            );
        }

        let mut seen = std::collections::HashSet::new();
        let all_seen = tokio::time::timeout(Duration::from_secs(3), async {
            while seen.len() < 20 {
                let (entries, _) = read_until_synced(&mut stream).await;
                seen.extend(entries.into_iter().map(|e| e.coord));
            }
        })
        .await;
        assert!(
            all_seen.is_ok(),
            "only {} of 20 entries arrived",
            seen.len()
        );
    }

    #[tokio::test]
    async fn a_write_that_was_never_published_still_reaches_the_peer() {
        let source = RecordingSink::new();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peers = catch_up_peers(&source, 64);
        let replication = Some(peers.hub().clone());
        peers.add(listener.local_addr().unwrap().to_string());
        let mut stream = accept_link(&listener).await;
        let request = PeerMessage::CatchUpRequest {
            known: VersionVector::new(),
        };
        crate::wire::write_message(&mut stream, &request)
            .await
            .unwrap();
        read_until_synced(&mut stream).await;

        // Stored, but the write "failed" before publishing: the stamper
        // releases it, which has the link catch the peer up.
        {
            let stamper = crate::hub::Stamper::new(&replication);
            source.seed(DB, &[9], "k", Value::I64(9), 100, stamper.next());
        }
        let arrived = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let (entries, _) = read_until_synced(&mut stream).await;
                if entries.iter().any(|e| e.coord == vec![9]) {
                    return;
                }
            }
        })
        .await;
        assert!(arrived.is_ok(), "the released write never arrived");
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
                    Stamp::NONE,
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
            || publish_remove(&replication, DB, &[4, 5, 6], "material", 20, Stamp::NONE),
            || sink.is_removed(DB, &[4, 5, 6], "material"),
        )
        .await;
    }

    #[tokio::test]
    async fn a_published_index_op_reaches_the_peer_over_a_real_connection() {
        let TestNode { addr, sink, .. } = spawn_node("node-b").await;
        let peers = standalone_peers();
        peers.add(addr.to_string());
        let replication = Some(peers.hub().clone());

        publish_until_seen(
            || {
                crate::hub::publish_index_op(
                    &replication,
                    DB,
                    "material",
                    crate::wire::IndexOp::Create,
                );
            },
            || {
                sink.index_ops()
                    == vec![(
                        DB.to_string(),
                        "material".to_string(),
                        crate::wire::IndexOp::Create,
                    )]
            },
        )
        .await;
    }

    #[tokio::test]
    async fn connecting_sends_current_index_state_which_the_peer_applies() {
        // The connecting side already has an index built (as if it built
        // it before this peer ever linked, or while this peer was down)
        // -- it must still reach the peer, via `IndexState` on connect,
        // not just a live `IndexOp` it would have missed.
        let source = RecordingSink::new();
        source.seed_indexed(DB, "material");
        let TestNode { addr, sink, .. } = spawn_node("node-b").await;
        let peers = catch_up_peers(&source, 64);
        peers.add(addr.to_string());

        wait_until(|| {
            sink.index_ops()
                == vec![(DB.to_string(), "material".to_string(), crate::wire::IndexOp::Create)]
        })
        .await;
    }

    #[tokio::test]
    async fn connecting_periodically_sends_its_sync_state_which_the_peer_applies() {
        let source = RecordingSink::new();
        let report = vec![crate::wire::DatabaseSync {
            database: DB.to_string(),
            content_digest: 0xABCD,
            indexed_keys: vec!["material".to_string()],
        }];
        source.seed_sync_state(report.clone());
        let TestNode { addr, sink, .. } = spawn_node("node-b").await;
        let peers = catch_up_peers(&source, 64);
        let me = peers.node_id();
        peers.add(addr.to_string());

        wait_until(|| sink.sync_reports().contains(&(me, report.clone()))).await;
    }

    #[tokio::test]
    async fn a_chunk_digests_request_is_answered_on_the_same_connection() {
        let source = RecordingSink::new();
        let digests = vec![(vec![1, 0, 0], 0xABCD_u64), (vec![-1, 0, 0], 0)];
        source.seed_chunk_digests(DB, digests.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peers = catch_up_peers(&source, 64);
        peers.add(listener.local_addr().unwrap().to_string());
        let mut stream = accept_link(&listener).await;
        let request = PeerMessage::CatchUpRequest {
            known: VersionVector::new(),
        };
        crate::wire::write_message(&mut stream, &request)
            .await
            .unwrap();
        read_until_synced(&mut stream).await;

        crate::wire::write_message(
            &mut stream,
            &PeerMessage::ChunkDigestsRequest {
                database: DB.to_string(),
            },
        )
        .await
        .unwrap();

        // Idle `Synced` ticks (`SYNCED_INTERVAL`, 100ms in tests) keep
        // firing independently of this request, so skip over any that
        // land first rather than assuming the very next frame is the
        // reply.
        let reply = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                match crate::wire::read_message(&mut stream).await.unwrap() {
                    Some(PeerMessage::Synced { .. }) => continue,
                    other => return other,
                }
            }
        })
        .await
        .expect("no ChunkDigests within 2s");
        assert_eq!(
            reply,
            Some(PeerMessage::ChunkDigests {
                database: DB.to_string(),
                digests,
            })
        );
    }

    #[tokio::test]
    async fn no_sync_report_is_sent_when_there_is_no_sync_state() {
        // No seeded sync state -- `send_sync_report` must not send an
        // empty frame (harmless either way, but there's no reason to).
        let TestNode { addr, sink, .. } = spawn_node("node-b").await;
        let peers = catch_up_peers(&RecordingSink::new(), 64);
        peers.add(addr.to_string());

        wait_until(|| peers.snapshot() == vec![(addr.to_string(), true)]).await;
        tokio::time::sleep(Duration::from_millis(250)).await;
        assert!(sink.sync_reports().is_empty());
    }

    #[tokio::test]
    async fn no_index_state_is_sent_when_there_is_nothing_indexed() {
        // No seeded index -- `send_index_state` must not send an empty
        // frame (harmless either way, but there's no reason to).
        let TestNode { addr, sink, .. } = spawn_node("node-b").await;
        let peers = catch_up_peers(&RecordingSink::new(), 64);
        peers.add(addr.to_string());

        wait_until(|| peers.snapshot() == vec![(addr.to_string(), true)]).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(sink.index_ops().is_empty());
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
                        Stamp::NONE,
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
    async fn wake_cuts_a_reconnect_backoff_short() {
        let addr = crate::test_support::free_addr();
        let peers = standalone_peers();
        peers.add(addr.to_string());
        // Attempts at ~0s, 0.5s and 1.5s fail; it's now waiting until ~3.5s.
        tokio::time::sleep(Duration::from_millis(1700)).await;
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        let _node = crate::test_support::spawn_node_on(
            "node-b",
            listener,
            RecordingSink::new(),
            crate::hub::ReplicationHub::new(),
            |p| p,
        );

        peers.wake(&addr.to_string(), 0);
        // Well inside the ~1.8s the backoff had left to run.
        let up = tokio::time::timeout(Duration::from_millis(500), async {
            while peers.snapshot() != vec![(addr.to_string(), true)] {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        assert!(up.is_ok(), "the woken link didn't reconnect promptly");
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
