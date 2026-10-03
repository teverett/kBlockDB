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
use crate::wire::{ChangeEntry, ChangeOp};
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
