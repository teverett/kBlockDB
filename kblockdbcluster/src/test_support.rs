//! A trivial in-memory [`crate::server::ReplicationSink`] and
//! [`crate::source::ChangeSource`], shared by `client.rs`'s and
//! `server.rs`'s own test modules to exercise the peer protocol end-to-end
//! (connect, `Hello`, `ChangeBatch` forwarding and application, catch-up)
//! without any real storage behind it. It keeps the last thing applied to
//! each (database, coord, key) -- a value, or when it was removed -- with
//! its stamp, applying last-write-wins on `(modified_at_ms, stamp)` like a
//! real embedder, so catch-up tests can check a cluster converges.
//! Auto-creating a database is the embedder's business, not modelled here.

use crate::server::ReplicationSink;
use crate::source::ChangeSource;
use crate::wire::{ChangeEntry, ChangeOp, DatabaseSync, IndexOp};
use kblockdblib::{CellMeta, Stamp, Value, VersionVector};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

type Key = (String, Vec<i32>, String);

#[derive(Clone)]
enum Applied {
    Set(Value, CellMeta, Stamp),
    Removed(u64, Stamp),
}

impl Applied {
    /// What last-write-wins compares.
    fn version(&self) -> (u64, Stamp) {
        match self {
            Applied::Set(_, meta, stamp) => (meta.modified_at_ms, *stamp),
            Applied::Removed(at, stamp) => (*at, *stamp),
        }
    }

    fn stamp(&self) -> Stamp {
        self.version().1
    }
}

#[derive(Clone, Default)]
pub struct RecordingSink {
    applied: Arc<Mutex<HashMap<Key, Applied>>>,
    /// Every `apply_index_op` call this sink has received, in order, as
    /// `(database, key, op)` -- for a test to assert an `IndexOp` actually
    /// reached it.
    index_ops: Arc<Mutex<Vec<(String, String, IndexOp)>>>,
    /// `(database, key)` pairs currently "indexed" per `apply_index_op`'s
    /// `Create`/`Drop`/`Rebuild` calls -- lets this sink double as a
    /// `ChangeSource::indexed_keys()`, for tests exercising `IndexState`
    /// reconciliation on connect.
    indexed: Arc<Mutex<std::collections::HashSet<(String, String)>>>,
    /// This sink's own `sync_state()` content, seeded by `seed_sync_state`
    /// -- what it reports as its current fingerprint, for a test playing
    /// the "already has a digest" side of a `SyncReport` exchange.
    sync_state: Arc<Mutex<Vec<DatabaseSync>>>,
    /// Every `apply_sync_report` call this sink has received, in order,
    /// as `(from_node, report)` -- for a test to assert a `SyncReport`
    /// actually reached it.
    sync_reports: Arc<Mutex<Vec<(u64, Vec<DatabaseSync>)>>>,
    /// Database names `apply_sync_report` should report as mismatched --
    /// what it returns, for a test to drive the drill-down request that
    /// follows. Seeded by `seed_mismatched`; empty (no drill-down) by
    /// default.
    mismatched: Arc<Mutex<Vec<String>>>,
    /// This sink's own `chunk_digests()` content, seeded by
    /// `seed_chunk_digests` -- what it reports in reply to a
    /// `ChunkDigestsRequest`.
    chunk_digests: Arc<Mutex<HashMap<String, Vec<(Vec<i32>, u64)>>>>,
    /// Every `apply_chunk_digests` call this sink has received, in order,
    /// as `(from_node, database, digests)` -- for a test to assert a
    /// `ChunkDigests` reply actually reached it.
    chunk_digests_received: Arc<Mutex<Vec<(u64, String, Vec<(Vec<i32>, u64)>)>>>,
    /// When `true`, the next `apply_set`/`apply_remove` call returns an
    /// error instead of recording anything, then resets to `false` --
    /// see `fail_next`.
    fail_next: Arc<Mutex<bool>>,
}

impl RecordingSink {
    pub fn new() -> Self {
        Self::default()
    }

    fn key(database: &str, coord: &[i32], key: &str) -> Key {
        (database.to_string(), coord.to_vec(), key.to_string())
    }

    /// The value and metadata last applied at (database, coord, key), as
    /// `(value, created_at_ms, modified_at_ms, version)` -- flattened for
    /// easy `==` comparison in tests. `None` if nothing was ever applied
    /// there, or the last thing applied there was a remove.
    pub fn get(&self, database: &str, coord: &[i32], key: &str) -> Option<(Value, u64, u64, u64)> {
        match self
            .applied
            .lock()
            .unwrap()
            .get(&Self::key(database, coord, key))?
        {
            Applied::Set(value, meta, _) => Some((
                value.clone(),
                meta.created_at_ms,
                meta.modified_at_ms,
                meta.version,
            )),
            Applied::Removed(..) => None,
        }
    }

    /// The stamp of whatever was last applied at (database, coord, key).
    pub fn stamp(&self, database: &str, coord: &[i32], key: &str) -> Option<Stamp> {
        Some(
            self.applied
                .lock()
                .unwrap()
                .get(&Self::key(database, coord, key))?
                .stamp(),
        )
    }

    /// Whether (database, coord, key) was removed (and nothing set again
    /// since) -- distinct from "never touched at all", which `get`
    /// alone can't tell apart from a removal.
    pub fn is_removed(&self, database: &str, coord: &[i32], key: &str) -> bool {
        matches!(
            self.applied
                .lock()
                .unwrap()
                .get(&Self::key(database, coord, key)),
            Some(Applied::Removed(..))
        )
    }

    /// Seeds (database, coord, key) with a value written at
    /// `modified_at_ms` (also used as `created_at_ms`) by `stamp`,
    /// bypassing last-write-wins -- for a test's starting data.
    pub fn seed(
        &self,
        database: &str,
        coord: &[i32],
        key: &str,
        value: Value,
        modified_at_ms: u64,
        stamp: Stamp,
    ) {
        let meta = CellMeta {
            created_at_ms: modified_at_ms,
            modified_at_ms,
            version: 0,
        };
        self.applied.lock().unwrap().insert(
            Self::key(database, coord, key),
            Applied::Set(value, meta, stamp),
        );
    }

    /// Records a removal at `removed_at_ms` by `stamp`, bypassing
    /// last-write-wins.
    pub fn seed_removed(
        &self,
        database: &str,
        coord: &[i32],
        key: &str,
        removed_at_ms: u64,
        stamp: Stamp,
    ) {
        self.applied.lock().unwrap().insert(
            Self::key(database, coord, key),
            Applied::Removed(removed_at_ms, stamp),
        );
    }

    /// Seeds (database, coord, key) directly with an unstamped value and
    /// zero metadata -- for a test that needs existing state before a
    /// `ChangeBatch` arrives, without caring about either.
    pub fn apply_set_now(&self, database: &str, coord: &[i32], key: &str, value: Value) {
        self.seed(database, coord, key, value, 0, Stamp::NONE);
    }

    /// Applies `entry` if it's newer than what's there -- last-write-wins.
    fn apply_if_newer(&self, key: Key, entry: Applied) {
        let mut applied = self.applied.lock().unwrap();
        let wins = applied
            .get(&key)
            .is_none_or(|existing| entry.version() > existing.version());
        if wins {
            applied.insert(key, entry);
        }
    }

    /// Makes the very next `apply_set`/`apply_remove` call fail (and
    /// record nothing), to test that `server::serve` logs and moves on
    /// rather than dropping the connection.
    pub fn fail_next(&self) {
        *self.fail_next.lock().unwrap() = true;
    }

    fn take_fail_next(&self) -> bool {
        let mut guard = self.fail_next.lock().unwrap();
        std::mem::take(&mut *guard)
    }

    /// Every index op this sink has received so far, in order.
    pub fn index_ops(&self) -> Vec<(String, String, IndexOp)> {
        self.index_ops.lock().unwrap().clone()
    }

    /// Marks `(database, key)` as already indexed, bypassing
    /// `apply_index_op` -- for a test's starting state (the side that
    /// should send it via `IndexState` on connect), as opposed to a live
    /// `IndexOp` this sink is meant to receive and record.
    pub fn seed_indexed(&self, database: &str, key: &str) {
        self.indexed
            .lock()
            .unwrap()
            .insert((database.to_string(), key.to_string()));
    }

    /// Sets what this sink's own `sync_state()` reports -- the "already
    /// has a digest" side of a `SyncReport` exchange test.
    pub fn seed_sync_state(&self, reports: Vec<DatabaseSync>) {
        *self.sync_state.lock().unwrap() = reports;
    }

    /// Every `SyncReport` this sink has received via `apply_sync_report`,
    /// as `(from_node, report)`, in order.
    pub fn sync_reports(&self) -> Vec<(u64, Vec<DatabaseSync>)> {
        self.sync_reports.lock().unwrap().clone()
    }

    /// Sets which database names `apply_sync_report` reports as
    /// mismatched (its return value) -- what drives `handle_connection`
    /// into sending a `ChunkDigestsRequest` for each, in a drill-down
    /// test.
    pub fn seed_mismatched(&self, databases: Vec<String>) {
        *self.mismatched.lock().unwrap() = databases;
    }

    /// Sets what this sink's own `chunk_digests()` reports for `database`
    /// -- the "already has the data" side of a `ChunkDigests` exchange
    /// test.
    pub fn seed_chunk_digests(&self, database: &str, digests: Vec<(Vec<i32>, u64)>) {
        self.chunk_digests
            .lock()
            .unwrap()
            .insert(database.to_string(), digests);
    }

    /// Every `ChunkDigests` reply this sink has received via
    /// `apply_chunk_digests`, as `(from_node, database, digests)`, in
    /// order.
    pub fn chunk_digests_received(&self) -> Vec<(u64, String, Vec<(Vec<i32>, u64)>)> {
        self.chunk_digests_received.lock().unwrap().clone()
    }
}

impl ReplicationSink for RecordingSink {
    async fn apply_set(
        &self,
        database: String,
        coord: Vec<i32>,
        key: String,
        value: Value,
        meta: CellMeta,
        stamp: Stamp,
    ) -> Result<(), String> {
        if self.take_fail_next() {
            return Err("forced failure (RecordingSink::fail_next)".to_string());
        }
        self.apply_if_newer((database, coord, key), Applied::Set(value, meta, stamp));
        Ok(())
    }

    async fn apply_remove(
        &self,
        database: String,
        coord: Vec<i32>,
        key: String,
        modified_at_ms: u64,
        stamp: Stamp,
    ) -> Result<(), String> {
        if self.take_fail_next() {
            return Err("forced failure (RecordingSink::fail_next)".to_string());
        }
        self.apply_if_newer(
            (database, coord, key),
            Applied::Removed(modified_at_ms, stamp),
        );
        Ok(())
    }

    async fn apply_index_op(&self, database: String, key: String, op: IndexOp) -> Result<(), String> {
        let mut indexed = self.indexed.lock().unwrap();
        match op {
            IndexOp::Create | IndexOp::Rebuild => {
                indexed.insert((database.clone(), key.clone()));
            }
            IndexOp::Drop => {
                indexed.remove(&(database.clone(), key.clone()));
            }
        }
        drop(indexed);
        self.index_ops.lock().unwrap().push((database, key, op));
        Ok(())
    }

    async fn apply_sync_report(&self, from_node: u64, report: Vec<DatabaseSync>) -> Vec<String> {
        self.sync_reports.lock().unwrap().push((from_node, report));
        self.mismatched.lock().unwrap().clone()
    }

    async fn apply_chunk_digests(
        &self,
        from_node: u64,
        database: String,
        digests: Vec<(Vec<i32>, u64)>,
    ) {
        self.chunk_digests_received
            .lock()
            .unwrap()
            .push((from_node, database, digests));
    }
}

impl ChangeSource for RecordingSink {
    fn changes_since(
        &self,
        known: &VersionVector,
        legacy_origin: u64,
        emit: &mut dyn FnMut(Vec<ChangeEntry>) -> bool,
    ) -> Result<(), String> {
        let batch: Vec<ChangeEntry> = self
            .applied
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, applied)| {
                let stamp = applied.stamp();
                !known.has(if stamp == Stamp::NONE {
                    Stamp::new(legacy_origin, 0)
                } else {
                    stamp
                })
            })
            .map(|((database, coord, key), applied)| {
                let (op, meta, stamp) = match applied {
                    Applied::Set(value, meta, stamp) => {
                        (ChangeOp::Set(value.clone()), *meta, *stamp)
                    }
                    Applied::Removed(at, stamp) => (
                        ChangeOp::Remove,
                        CellMeta {
                            created_at_ms: 0,
                            modified_at_ms: *at,
                            version: 0,
                        },
                        *stamp,
                    ),
                };
                ChangeEntry {
                    database: database.clone(),
                    coord: coord.clone(),
                    key: key.clone(),
                    op,
                    created_at_ms: meta.created_at_ms,
                    modified_at_ms: meta.modified_at_ms,
                    version: meta.version,
                    origin: stamp.origin,
                    seq: stamp.seq,
                }
            })
            .collect();
        if !batch.is_empty() {
            emit(batch);
        }
        Ok(())
    }

    fn indexed_keys(&self) -> Result<Vec<(String, String)>, String> {
        let mut entries: Vec<(String, String)> = self.indexed.lock().unwrap().iter().cloned().collect();
        entries.sort();
        Ok(entries)
    }

    fn sync_state(&self) -> Result<Vec<DatabaseSync>, String> {
        Ok(self.sync_state.lock().unwrap().clone())
    }

    fn chunk_digests(&self, database: &str) -> Result<Vec<(Vec<i32>, u64)>, String> {
        Ok(self
            .chunk_digests
            .lock()
            .unwrap()
            .get(database)
            .cloned()
            .unwrap_or_default())
    }
}

pub const CLUSTER_SECRET: &str = "cluster-secret";

/// One complete in-process node: a real `server::serve` listener on an
/// OS-assigned port, backed by a `RecordingSink`, with its own `PeerSet`
/// and `ReplicationHub` -- what every end-to-end test in this crate builds
/// a cluster out of.
pub struct TestNode {
    pub addr: std::net::SocketAddr,
    pub sink: RecordingSink,
    pub peers: crate::peers::PeerSet,
    pub hub: Arc<crate::hub::ReplicationHub>,
}

pub async fn spawn_node(server_id: &str) -> TestNode {
    spawn_node_with(
        server_id,
        RecordingSink::new(),
        crate::hub::ReplicationHub::new(),
        |peers| peers,
    )
    .await
}

/// `spawn_node`, with given starting data in `sink`, a given hub (and so
/// node id/sequencer), and a chance to configure the `PeerSet` (a
/// persisted vector, say) before it starts serving. The sink is both
/// where incoming changes land and where catch-ups are read from.
pub async fn spawn_node_with(
    server_id: &str,
    sink: RecordingSink,
    hub: Arc<crate::hub::ReplicationHub>,
    configure: impl FnOnce(crate::peers::PeerSet) -> crate::peers::PeerSet,
) -> TestNode {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    spawn_node_on(server_id, listener, sink, hub, configure)
}

/// An address nothing is listening on (yet): a port the OS just handed
/// out and was released -- for a test that brings a node up there later.
pub fn free_addr() -> std::net::SocketAddr {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}

/// `spawn_node_with`, serving on an already-bound `listener` (e.g. one
/// bound to a `free_addr`).
pub fn spawn_node_on(
    server_id: &str,
    listener: tokio::net::TcpListener,
    sink: RecordingSink,
    hub: Arc<crate::hub::ReplicationHub>,
    configure: impl FnOnce(crate::peers::PeerSet) -> crate::peers::PeerSet,
) -> TestNode {
    let addr = listener.local_addr().unwrap();
    let peers = configure(
        crate::peers::PeerSet::new(
            crate::peers::LocalIdentity {
                cluster_secret: CLUSTER_SECRET.to_string(),
                server_id: server_id.to_string(),
                peer_port: addr.port(),
            },
            hub.clone(),
        )
        .with_change_source(Arc::new(sink.clone())),
    );
    tokio::spawn(crate::server::serve(listener, sink.clone(), peers.clone()));
    TestNode {
        addr,
        sink,
        peers,
        hub,
    }
}

/// Polls `condition` every 10ms for up to 2s.
pub async fn wait_until(mut condition: impl FnMut() -> bool) {
    for _ in 0..200 {
        if condition() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("condition was never met within 2s");
}
