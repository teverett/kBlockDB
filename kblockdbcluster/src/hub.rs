//! The in-process fan-out point between a local write -- in
//! `kblockdbserver`, a REST/binary-protocol write or the query language's
//! `SET`/`UPDATE`/`DELETE` -- and every outbound peer connection
//! (`client.rs`) that wants to ship it onward. See
//! `docs/clustering.md` (at the repository root) for the feature as a
//! whole.
//!
//! Every local write is stamped with this node's next sequence number
//! (the hub's [`Sequencer`]), handed out by a [`Stamper`] as the write
//! lands in storage, and then published here as a [`ChangeEntry`]; every
//! `client.rs` task subscribes its own
//! [`tokio::sync::broadcast::Receiver`] and forwards what it receives to
//! its one peer. Deliberately *not* a queue with per-consumer
//! backpressure: a peer that's behind misses entries from here (see
//! `client.rs`'s handling of `Lagged`) and one that's disconnected misses
//! everything -- both get them back from storage instead, through
//! catch-up (see `peers.rs`).

use crate::sequence::Sequencer;
use crate::wire::{ChangeEntry, ChangeOp};
use kblockdblib::{CellMeta, Stamp, Value};
use std::sync::{Arc, Mutex};
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
    sequencer: Sequencer,
}

impl ReplicationHub {
    /// A hub with an in-memory sequencer (random node id, nothing
    /// persisted) -- for tests. A server uses `with_sequencer`.
    pub fn new() -> Arc<Self> {
        Self::with_sequencer(Sequencer::in_memory())
    }

    pub fn with_sequencer(sequencer: Sequencer) -> Arc<Self> {
        Self::build(CHANGE_LOG_CAPACITY, sequencer)
    }

    /// A hub buffering only `capacity` entries -- for tests that need a
    /// link to fall behind (`RecvError::Lagged`) quickly.
    #[cfg(test)]
    pub(crate) fn with_capacity(capacity: usize) -> Arc<Self> {
        Self::build(capacity, Sequencer::in_memory())
    }

    fn build(capacity: usize, sequencer: Sequencer) -> Arc<Self> {
        let (sender, _) = broadcast::channel(capacity);
        Arc::new(ReplicationHub { sender, sequencer })
    }

    pub fn subscribe(&self) -> broadcast::Receiver<ChangeEntry> {
        self.sender.subscribe()
    }

    pub fn sequencer(&self) -> &Sequencer {
        &self.sequencer
    }

    /// Fire-and-forget: a `SendError` only ever means no peer connection
    /// is currently subscribed (every `client.rs` task has
    /// disconnected, or none are configured), which is a normal, expected
    /// state, not an error worth surfacing to the write that triggered
    /// this. Marks this node's own seq published either way.
    fn publish(&self, entry: ChangeEntry) {
        let (origin, seq) = (entry.origin, entry.seq);
        let _ = self.sender.send(entry);
        if origin == self.sequencer.node_id() {
            self.sequencer.published(seq);
        }
    }
}

/// Publishes a local `Set` stamped `stamp` -- a no-op if `replication` is
/// `None` (not clustered). `meta` is whatever `World::set_stamped`/
/// `set_region_stamped` just returned, applied verbatim by every peer (see
/// `World::apply_replicated_stamped`), so the write means the same thing
/// everywhere it lands.
pub fn publish_set(
    replication: &Option<Arc<ReplicationHub>>,
    database: &str,
    coord: &[i32],
    key: &str,
    value: Value,
    meta: CellMeta,
    stamp: Stamp,
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
            origin: stamp.origin,
            seq: stamp.seq,
        });
    }
}

/// Publishes a local `Remove` stamped `stamp` -- a no-op if `replication`
/// is `None`. `removed_at_ms` is the time `World::remove_stamped`/
/// `remove_region_stamped` recorded for it -- what every peer's
/// last-write-wins compares.
pub fn publish_remove(
    replication: &Option<Arc<ReplicationHub>>,
    database: &str,
    coord: &[i32],
    key: &str,
    removed_at_ms: u64,
    stamp: Stamp,
) {
    if let Some(hub) = replication {
        hub.publish(ChangeEntry {
            database: database.to_string(),
            coord: coord.to_vec(),
            key: key.to_string(),
            op: ChangeOp::Remove,
            created_at_ms: 0, // unused by a removal
            modified_at_ms: removed_at_ms,
            version: 0, // unused by a removal
            origin: stamp.origin,
            seq: stamp.seq,
        });
    }
}

/// Hands out stamps for one local write operation and makes sure every
/// one is either published or released. Pass `&mut || stamper.next()` to
/// a `World::*_stamped` method, then publish what it returns through
/// `publish_set`/`publish_remove`. Cheap to clone: a write closure moved
/// onto a blocking thread takes one clone, the caller keeps another.
///
/// When the last clone drops, any stamp handed out but never published --
/// the write failed partway, after some cells were already stored -- is
/// released (see `Sequencer::release`), which makes every link catch its
/// peer up, so those cells still replicate.
///
/// Without clustering, hands out `Stamp::NONE` and does nothing else.
#[derive(Clone)]
pub struct Stamper(Arc<StamperInner>);

struct StamperInner {
    hub: Option<Arc<ReplicationHub>>,
    handed_out: Mutex<Vec<u64>>,
}

impl Stamper {
    pub fn new(replication: &Option<Arc<ReplicationHub>>) -> Self {
        Stamper(Arc::new(StamperInner {
            hub: replication.clone(),
            handed_out: Mutex::new(Vec::new()),
        }))
    }

    pub fn next(&self) -> Stamp {
        match &self.0.hub {
            Some(hub) => {
                let stamp = hub.sequencer.assign();
                self.0.handed_out.lock().unwrap().push(stamp.seq);
                stamp
            }
            None => Stamp::NONE,
        }
    }

    /// `publish_set`, and no longer waiting on `stamp`.
    pub fn publish_set(
        &self,
        database: &str,
        coord: &[i32],
        key: &str,
        value: Value,
        meta: CellMeta,
        stamp: Stamp,
    ) {
        publish_set(&self.0.hub, database, coord, key, value, meta, stamp);
        self.forget(stamp);
    }

    /// `publish_remove`, and no longer waiting on `stamp`.
    pub fn publish_remove(
        &self,
        database: &str,
        coord: &[i32],
        key: &str,
        removed_at_ms: u64,
        stamp: Stamp,
    ) {
        publish_remove(&self.0.hub, database, coord, key, removed_at_ms, stamp);
        self.forget(stamp);
    }

    fn forget(&self, stamp: Stamp) {
        let mut handed_out = self.0.handed_out.lock().unwrap();
        if let Some(i) = handed_out.iter().position(|&seq| seq == stamp.seq) {
            handed_out.swap_remove(i);
        }
    }
}

impl Drop for StamperInner {
    fn drop(&mut self) {
        if let Some(hub) = &self.hub {
            let leftover = std::mem::take(&mut *self.handed_out.lock().unwrap());
            hub.sequencer.release(&leftover);
        }
    }
}

/// The current time in milliseconds since the Unix epoch -- mirrors
/// `kblockdblib::world`'s own private `now_ms` helper.
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

    fn meta() -> CellMeta {
        CellMeta {
            created_at_ms: 10,
            modified_at_ms: 20,
            version: 1,
        }
    }

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
            meta(),
            Stamp::NONE,
        );
    }

    #[test]
    fn publish_set_reaches_a_subscriber_with_its_stamp() {
        let hub = ReplicationHub::new();
        let mut rx = hub.subscribe();
        let stamp = hub.sequencer().assign();
        let replication = Some(hub.clone());

        publish_set(
            &replication,
            "demo",
            &[1, 2, 3],
            "material",
            Value::Str("stone".to_string()),
            meta(),
            stamp,
        );

        let entry = rx.try_recv().unwrap();
        assert_eq!(entry.database, "demo");
        assert_eq!(entry.coord, vec![1, 2, 3]);
        assert_eq!(entry.key, "material");
        assert_eq!(entry.op, ChangeOp::Set(Value::Str("stone".to_string())));
        assert_eq!(entry.created_at_ms, 10);
        assert_eq!(entry.modified_at_ms, 20);
        assert_eq!(entry.version, 1);
        assert_eq!((entry.origin, entry.seq), (stamp.origin, stamp.seq));
        // Published, so confirmed.
        assert_eq!(hub.sequencer().confirmed_through(), stamp.seq);
    }

    #[test]
    fn publish_remove_reaches_a_subscriber() {
        let hub = ReplicationHub::new();
        let mut rx = hub.subscribe();
        let replication = Some(hub);

        publish_remove(
            &replication,
            "demo",
            &[1, 2, 3],
            "material",
            42,
            Stamp::new(7, 3),
        );

        let entry = rx.try_recv().unwrap();
        assert_eq!(entry.coord, vec![1, 2, 3]);
        assert_eq!(entry.op, ChangeOp::Remove);
        assert_eq!(entry.modified_at_ms, 42);
        assert_eq!((entry.origin, entry.seq), (7, 3));
    }

    #[test]
    fn a_stamper_without_a_hub_hands_out_none() {
        assert_eq!(Stamper::new(&None).next(), Stamp::NONE);
    }

    #[test]
    fn a_stamper_releases_what_it_never_published() {
        let hub = ReplicationHub::new();
        let resync = hub.sequencer().subscribe_resync();
        let replication = Some(hub.clone());
        {
            let stamper = Stamper::new(&replication);
            let published = stamper.next();
            let _unpublished = stamper.clone().next();
            stamper.publish_remove("demo", &[0], "k", 1, published);
            // Seq 1 published; seq 2 still in flight.
            assert_eq!(hub.sequencer().confirmed_through(), 1);
        }
        assert!(resync.has_changed().unwrap());
        assert_eq!(hub.sequencer().confirmed_through(), 2);
    }

    #[test]
    fn a_stamper_that_published_everything_asks_for_no_resync() {
        let hub = ReplicationHub::new();
        let resync = hub.sequencer().subscribe_resync();
        let replication = Some(hub.clone());
        {
            let stamper = Stamper::new(&replication);
            let stamp = stamper.next();
            stamper.publish_set("demo", &[0], "k", Value::I64(1), meta(), stamp);
        }
        assert!(!resync.has_changed().unwrap());
        assert_eq!(hub.sequencer().confirmed_through(), 1);
    }

    #[test]
    fn now_ms_returns_something_plausibly_current() {
        // Sanity check only -- it's wall-clock time, not a value this
        // test can pin down exactly.
        assert!(now_ms() > 1_700_000_000_000);
    }
}
