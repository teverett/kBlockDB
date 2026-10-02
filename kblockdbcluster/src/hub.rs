//! The in-process fan-out point between a local write -- in
//! `kblockdbserver`, a REST/binary-protocol write or the query language's
//! `SET`/`UPDATE`/`DELETE` -- and every outbound peer connection
//! (`client.rs`) that wants to ship it onward. See
//! `docs/clustering.md` (at the repository root) for the feature as a
//! whole.
//!
//! The embedder builds one [`ChangeEntry`] per cell/key it actually
//! changed (via [`publish_set`]/[`publish_remove`]) right after a
//! successful local write; every `client.rs` task subscribes its own
//! [`tokio::sync::broadcast::Receiver`] and forwards what it receives to
//! its one peer. Deliberately *not* a queue with per-consumer
//! backpressure: a peer that's behind or disconnected just misses entries
//! until it reconnects (see `client.rs`'s doc comment on `Lagged`) --
//! acceptable for this feature's "live-forward only, no durability
//! guarantee" v1 scope (see `docs/clustering.md`'s "Limitations"
//! section).

use crate::wire::{ChangeEntry, ChangeOp};
use kblockdblib::{CellMeta, Value};
use std::sync::Arc;
use tokio::sync::broadcast;

/// How many unconsumed entries a lagging peer connection can fall behind
/// by before `tokio::sync::broadcast` starts dropping its oldest ones
/// (reported to that receiver as `RecvError::Lagged`) -- generous enough
/// that an ordinary reconnect blip doesn't lose anything, not a
/// correctness guarantee for a peer that's down for a long time (see this
/// module's doc comment).
const CHANGE_LOG_CAPACITY: usize = 4096;

pub struct ReplicationHub {
    sender: broadcast::Sender<ChangeEntry>,
}

impl ReplicationHub {
    pub fn new() -> Arc<Self> {
        let (sender, _) = broadcast::channel(CHANGE_LOG_CAPACITY);
        Arc::new(ReplicationHub { sender })
    }

    pub fn subscribe(&self) -> broadcast::Receiver<ChangeEntry> {
        self.sender.subscribe()
    }

    /// Fire-and-forget: a `SendError` only ever means no peer connection
    /// is currently subscribed (every `client.rs` task has
    /// disconnected, or none are configured), which is a normal, expected
    /// state, not an error worth surfacing to the write that triggered
    /// this.
    fn publish(&self, entry: ChangeEntry) {
        let _ = self.sender.send(entry);
    }
}

/// Publishes a local `Set` -- a no-op if `replication` is `None` (not
/// clustered). `meta` is whatever `World::set`/`set_region` just
/// returned, applied verbatim by every peer (see
/// `World::apply_replicated`), so the write means the same thing
/// everywhere it lands.
pub fn publish_set(
    replication: &Option<Arc<ReplicationHub>>,
    database: &str,
    coord: &[i32],
    key: &str,
    value: Value,
    meta: CellMeta,
) {
    if let Some(hub) = replication {
        hub.publish(ChangeEntry {
            database: database.to_string(),
            coord: coord.to_vec(),
            key: key.to_string(),
            op: ChangeOp::Set(value),
            created_at_ms: meta.created_at_ms,
            modified_at_ms: meta.modified_at_ms,
            version: meta.version,
        });
    }
}

/// Publishes a local `Remove` -- a no-op if `replication` is `None`.
/// `kblockdblib::World::remove` has no `CellMeta` of its own to report
/// (a removed cell has no metadata left), so the caller passes
/// `modified_at_ms` as this removal's own logical timestamp (captured via
/// [`now_ms`] right before the local `remove` call) -- what every peer's
/// `apply_replicated_remove` compares against to decide whether this
/// removal is newer than whatever it currently has.
pub fn publish_remove(
    replication: &Option<Arc<ReplicationHub>>,
    database: &str,
    coord: &[i32],
    key: &str,
    modified_at_ms: u64,
) {
    if let Some(hub) = replication {
        hub.publish(ChangeEntry {
            database: database.to_string(),
            coord: coord.to_vec(),
            key: key.to_string(),
            op: ChangeOp::Remove,
            created_at_ms: 0, // unused by apply_replicated_remove
            modified_at_ms,
            version: 0, // unused by apply_replicated_remove
        });
    }
}

/// The current time in milliseconds since the Unix epoch -- mirrors
/// `kblockdblib::world`'s own private `now_ms` helper, duplicated here
/// since a `Remove` call site needs a timestamp *before* calling
/// `World::remove` (which doesn't take or return one, unlike `set`/
/// `set_region`) to use as that removal's replicated logical timestamp.
pub fn now_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publish_set_is_a_no_op_without_a_hub() {
        // Just needs to not panic -- there's no observable effect to
        // assert when `replication` is `None`.
        publish_set(
            &None,
            "demo",
            &[0, 0, 0],
            "material",
            Value::Str("stone".to_string()),
            CellMeta {
                created_at_ms: 1,
                modified_at_ms: 1,
                version: 0,
            },
        );
    }

    #[test]
    fn publish_set_reaches_a_subscriber() {
        let hub = ReplicationHub::new();
        let mut rx = hub.subscribe();
        let replication = Some(hub);

        publish_set(
            &replication,
            "demo",
            &[1, 2, 3],
            "material",
            Value::Str("stone".to_string()),
            CellMeta {
                created_at_ms: 10,
                modified_at_ms: 20,
                version: 1,
            },
        );

        let entry = rx.try_recv().unwrap();
        assert_eq!(entry.database, "demo");
        assert_eq!(entry.coord, vec![1, 2, 3]);
        assert_eq!(entry.key, "material");
        assert_eq!(entry.op, ChangeOp::Set(Value::Str("stone".to_string())));
        assert_eq!(entry.created_at_ms, 10);
        assert_eq!(entry.modified_at_ms, 20);
        assert_eq!(entry.version, 1);
    }

    #[test]
    fn publish_remove_reaches_a_subscriber() {
        let hub = ReplicationHub::new();
        let mut rx = hub.subscribe();
        let replication = Some(hub);

        publish_remove(&replication, "demo", &[1, 2, 3], "material", 42);

        let entry = rx.try_recv().unwrap();
        assert_eq!(entry.database, "demo");
        assert_eq!(entry.coord, vec![1, 2, 3]);
        assert_eq!(entry.key, "material");
        assert_eq!(entry.op, ChangeOp::Remove);
        assert_eq!(entry.modified_at_ms, 42);
    }

    #[test]
    fn now_ms_returns_something_plausibly_current() {
        // Sanity check only -- it's wall-clock time, not a value this
        // test can pin down exactly.
        assert!(now_ms() > 1_700_000_000_000);
    }
}
