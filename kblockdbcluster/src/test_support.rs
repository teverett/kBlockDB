//! A trivial in-memory [`crate::server::ReplicationSink`] and
//! [`crate::source::ChangeSource`], shared by `client.rs`'s and
//! `server.rs`'s own test modules to exercise the peer protocol end-to-end
//! (connect, `Hello`, `ChangeBatch` forwarding and application, catch-up)
//! without any real storage behind it. It keeps the last thing applied to
//! each (database, coord, key) -- a value, or when it was removed -- with
//! last-write-wins on `modified_at_ms`, like a real embedder, so catch-up
//! tests can check a cluster converges. Auto-creating a database is the
//! embedder's business, not modelled here.

use crate::server::ReplicationSink;
use crate::source::ChangeSource;
use crate::wire::{ChangeEntry, ChangeOp};
use kblockdblib::{CellMeta, Value};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

type Key = (String, Vec<i32>, String);

#[derive(Clone)]
enum Applied {
    Set(Value, CellMeta),
    Removed(u64),
}

impl Applied {
    fn modified_at_ms(&self) -> u64 {
        match self {
            Applied::Set(_, meta) => meta.modified_at_ms,
            Applied::Removed(at) => *at,
        }
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
            Applied::Set(value, meta) => Some((
                value.clone(),
                meta.created_at_ms,
                meta.modified_at_ms,
                meta.version,
            )),
            Applied::Removed(_) => None,
        }
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
            Some(Applied::Removed(_))
        )
    }

    /// Like `apply_set_now`, with a given `modified_at_ms` (also used as
    /// `created_at_ms`) -- for a catch-up test's starting data.
    pub fn seed(
        &self,
        database: &str,
        coord: &[i32],
        key: &str,
        value: Value,
        modified_at_ms: u64,
    ) {
        let meta = CellMeta {
            created_at_ms: modified_at_ms,
            modified_at_ms,
            version: 0,
        };
        self.applied
            .lock()
            .unwrap()
            .insert(Self::key(database, coord, key), Applied::Set(value, meta));
    }

    /// Records a removal at `removed_at_ms`, bypassing last-write-wins.
    pub fn seed_removed(&self, database: &str, coord: &[i32], key: &str, removed_at_ms: u64) {
        self.applied.lock().unwrap().insert(
            Self::key(database, coord, key),
            Applied::Removed(removed_at_ms),
        );
    }

    /// Applies `entry` if it's newer than what's there -- last-write-wins.
    fn apply_if_newer(&self, key: Key, entry: Applied) {
        let mut applied = self.applied.lock().unwrap();
        let wins = applied
            .get(&key)
            .is_none_or(|existing| entry.modified_at_ms() > existing.modified_at_ms());
        if wins {
            applied.insert(key, entry);
        }
    }

    /// Seeds (database, coord, key) directly, bypassing the
    /// `ReplicationSink` trait -- for a test that needs existing state
    /// before a `ChangeBatch` arrives, without caring about its metadata.
    pub fn apply_set_now(&self, database: &str, coord: &[i32], key: &str, value: Value) {
        self.seed(database, coord, key, value, 0);
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
    ) -> Result<(), String> {
        if self.take_fail_next() {
            return Err("forced failure (RecordingSink::fail_next)".to_string());
        }
        self.apply_if_newer((database, coord, key), Applied::Set(value, meta));
        Ok(())
    }

    async fn apply_remove(
        &self,
        database: String,
        coord: Vec<i32>,
        key: String,
        modified_at_ms: u64,
    ) -> Result<(), String> {
        if self.take_fail_next() {
            return Err("forced failure (RecordingSink::fail_next)".to_string());
        }
        self.apply_if_newer((database, coord, key), Applied::Removed(modified_at_ms));
        Ok(())
    }
}

impl ChangeSource for RecordingSink {
    fn changes_since(
        &self,
        since_ms: u64,
        emit: &mut dyn FnMut(Vec<ChangeEntry>) -> bool,
    ) -> Result<(), String> {
        let batch: Vec<ChangeEntry> = self
            .applied
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, applied)| applied.modified_at_ms() > since_ms)
            .map(|((database, coord, key), applied)| {
                let (op, meta) = match applied {
                    Applied::Set(value, meta) => (ChangeOp::Set(value.clone()), *meta),
                    Applied::Removed(at) => (
                        ChangeOp::Remove,
                        CellMeta {
                            created_at_ms: 0,
                            modified_at_ms: *at,
                            version: 0,
                        },
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
    spawn_node_with(server_id, RecordingSink::new(), |peers| peers).await
}

/// `spawn_node`, with given starting data in `sink` and a chance to
/// configure the `PeerSet` (watermarks, margin) before it starts serving.
/// The sink is both where incoming changes land and where catch-ups are
/// read from.
pub async fn spawn_node_with(
    server_id: &str,
    sink: RecordingSink,
    configure: impl FnOnce(crate::peers::PeerSet) -> crate::peers::PeerSet,
) -> TestNode {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let hub = crate::hub::ReplicationHub::new();
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
