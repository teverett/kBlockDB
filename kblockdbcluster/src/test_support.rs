//! A trivial in-memory [`crate::server::ReplicationSink`], shared by
//! `client.rs`'s and `server.rs`'s own test modules to exercise the peer
//! protocol end-to-end (connect, `Hello`, `ChangeBatch` forwarding and
//! application) without any real storage behind it. Conflict resolution
//! and auto-creating a database are the embedder's responsibility (see
//! `server::ReplicationSink`'s doc comment), not something this crate's
//! own tests need to re-verify -- this sink just records the last thing
//! applied to each (database, coord, key).

use crate::server::ReplicationSink;
use kblockdblib::{CellMeta, Value};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

type Key = (String, Vec<i32>, String);
type Applied = HashMap<Key, Option<(Value, CellMeta)>>;

#[derive(Clone, Default)]
pub struct RecordingSink {
    applied: Arc<Mutex<Applied>>,
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
        let applied = self.applied.lock().unwrap();
        let (value, meta) = applied.get(&Self::key(database, coord, key))?.clone()?;
        Some((value, meta.created_at_ms, meta.modified_at_ms, meta.version))
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
            Some(None)
        )
    }

    /// Seeds (database, coord, key) directly, bypassing the
    /// `ReplicationSink` trait -- for a test that needs existing state
    /// before a `ChangeBatch` arrives, without caring about its metadata.
    pub fn apply_set_now(&self, database: &str, coord: &[i32], key: &str, value: Value) {
        self.applied.lock().unwrap().insert(
            Self::key(database, coord, key),
            Some((
                value,
                CellMeta {
                    created_at_ms: 0,
                    modified_at_ms: 0,
                    version: 0,
                },
            )),
        );
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
        self.applied
            .lock()
            .unwrap()
            .insert((database, coord, key), Some((value, meta)));
        Ok(())
    }

    async fn apply_remove(
        &self,
        database: String,
        coord: Vec<i32>,
        key: String,
        _modified_at_ms: u64,
    ) -> Result<(), String> {
        if self.take_fail_next() {
            return Err("forced failure (RecordingSink::fail_next)".to_string());
        }
        self.applied
            .lock()
            .unwrap()
            .insert((database, coord, key), None);
        Ok(())
    }
}
