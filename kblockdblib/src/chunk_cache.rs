//! In-process, per-chunk write-through cache, bounded by a configurable
//! LRU eviction policy.
//!
//! kBlockDB assumes exactly one `World` is ever open against a given data
//! directory at a time (see `World`'s "Concurrency" doc comment), with
//! concurrency coming from multiple threads sharing that one `World`.
//! Since this process is always the sole writer, a chunk's in-memory
//! contents -- once read off disk -- can never fall behind what's on disk
//! (nothing else can change the file underneath it), so `World` can safely
//! keep every chunk it touches cached here instead of re-reading it from
//! disk on every call. Every write still goes straight to disk before its
//! call returns (see `World::with_chunk_write`) -- this is a read cache
//! plus a write-*through*, not a write-*behind*, so the durability
//! guarantee ("a `set` is durable the instant it returns") is unchanged.

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, Mutex, PoisonError, RwLock};

/// One cached slot plus the bookkeeping `evict_one` needs to find the
/// least-recently-used entry.
struct Entry<V> {
    data: Arc<RwLock<Option<V>>>,
    /// This entry's position in `Inner::clock` as of its last `slot()`
    /// lookup (whether that found it already cached or just created it) --
    /// the smallest value among all entries is the least recently used
    /// one.
    last_used: u64,
}

struct Inner<K, V> {
    slots: HashMap<K, Entry<V>>,
    /// Ticks up on every `slot()` call and is stamped onto whichever entry
    /// that call touched -- a plain logical clock, not a wall-clock time,
    /// so "least recently used" is just "smallest `last_used`".
    clock: u64,
}

/// Hands out one `RwLock<Option<V>>` "slot" per distinct `K`, creating an
/// empty (not-yet-loaded) one the first time that key is asked for.
/// `None` means "this `World` hasn't loaded `K` yet" -- distinct from a
/// cached-but-empty `V`, which is `Some` -- so the first touch always
/// consults disk exactly once, and every touch after that doesn't need to
/// at all (until `K` is evicted and re-touched).
///
/// Bounded by `max_entries`: inserting a new key when the table is already
/// at capacity evicts the least-recently-used entry first -- see
/// `evict_one`. A `Chunk` with no columns costs next to nothing (see
/// `Chunk::new`), so a large `max_entries` is cheap for a world touched at
/// millions of distinct chunks, but a workload that reads many chunks it
/// never writes (e.g. scanning large empty regions) will still cycle
/// through the cache under a small one -- pick `max_entries` for your
/// workload's *working set* (the chunks touched repeatedly), not for the
/// world's nominal size.
pub struct ChunkCache<K, V> {
    max_entries: usize,
    inner: Mutex<Inner<K, V>>,
}

impl<K: Eq + Hash + Clone, V> ChunkCache<K, V> {
    pub fn new(max_entries: usize) -> Self {
        ChunkCache {
            max_entries,
            inner: Mutex::new(Inner {
                slots: HashMap::new(),
                clock: 0,
            }),
        }
    }

    pub fn max_entries(&self) -> usize {
        self.max_entries
    }

    /// The slot for `key`, creating it (empty) if this is the first time
    /// `key` has been asked for (evicting the least-recently-used existing
    /// entry first, if the table is already at `max_entries`). Briefly
    /// locks the whole table to look up, evict, or insert, then hands back
    /// an `Arc` the caller locks (shared or exclusive) independently -- so
    /// two calls for two *different* keys only ever contend on this quick
    /// lookup, never on each other's actual chunk access.
    pub fn slot(&self, key: &K) -> Arc<RwLock<Option<V>>> {
        let mut inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        inner.clock += 1;
        let clock = inner.clock;

        if let Some(entry) = inner.slots.get_mut(key) {
            entry.last_used = clock;
            return entry.data.clone();
        }

        if inner.slots.len() >= self.max_entries {
            evict_one(&mut inner.slots);
        }

        let data = Arc::new(RwLock::new(None));
        inner.slots.insert(
            key.clone(),
            Entry {
                data: data.clone(),
                last_used: clock,
            },
        );
        data
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.inner
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .slots
            .len()
    }
}

/// Removes the least-recently-used entry in `slots` -- *among entries
/// nobody currently holds a reference to* (`Arc::strong_count(&e.data) ==
/// 1`, i.e. only this table itself is holding it). Skipping a pinned
/// entry, rather than evicting it anyway, matters for correctness, not
/// just fairness: `World::with_chunk_read`/`with_chunk_write` hold their
/// own clone of a slot's `Arc` for as long as they're using it, so if an
/// in-flight write's slot were evicted out from under it, a *different*
/// thread's `slot()` call for that same key, racing the write, would
/// create a brand new, independent slot and reload the chunk from disk --
/// possibly *before* the in-flight write lands, permanently forking the
/// cache into two entries for one chunk that disagree with each other.
/// Leaving every currently-pinned entry alone avoids that: a slot can only
/// ever be evicted while nothing is using it, so a fresh `slot()` call for
/// an evicted key is always a genuine, unambiguous cache miss.
///
/// If every entry happens to be pinned right now, this simply evicts
/// nothing -- the table temporarily exceeds `max_entries` rather than
/// evicting something still in use or busy-waiting for one to free up.
/// O(n) in the table's current size: fine for a table that's bounded (that
/// being the whole point of `max_entries`) and only scanned when it's
/// actually full, not on every access.
fn evict_one<K: Eq + Hash + Clone, V>(slots: &mut HashMap<K, Entry<V>>) {
    let victim = slots
        .iter()
        .filter(|(_, entry)| Arc::strong_count(&entry.data) == 1)
        .min_by_key(|(_, entry)| entry.last_used)
        .map(|(key, _)| key.clone());
    if let Some(key) = victim {
        slots.remove(&key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn a_fresh_slot_starts_empty() {
        let cache: ChunkCache<&str, i32> = ChunkCache::new(10);
        let slot = cache.slot(&"chunk-1");
        assert!(slot.read().unwrap().is_none());
    }

    #[test]
    fn the_same_key_always_yields_the_same_slot() {
        let cache: ChunkCache<&str, i32> = ChunkCache::new(10);
        let a = cache.slot(&"chunk-1");
        let b = cache.slot(&"chunk-1");
        assert!(Arc::ptr_eq(&a, &b));
    }

    #[test]
    fn different_keys_yield_different_slots() {
        let cache: ChunkCache<&str, i32> = ChunkCache::new(10);
        let a = cache.slot(&"chunk-1");
        let b = cache.slot(&"chunk-2");
        assert!(!Arc::ptr_eq(&a, &b));
    }

    #[test]
    fn a_value_written_into_a_slot_is_visible_through_a_later_lookup() {
        let cache: ChunkCache<&str, i32> = ChunkCache::new(10);
        *cache.slot(&"chunk-1").write().unwrap() = Some(42);
        assert_eq!(*cache.slot(&"chunk-1").read().unwrap(), Some(42));
    }

    #[test]
    fn multiple_shared_reads_of_the_same_slot_coexist() {
        let cache: ChunkCache<&str, i32> = ChunkCache::new(10);
        let slot = cache.slot(&"chunk-1");
        let a = slot.read().unwrap();
        let b = slot.read().unwrap();
        drop((a, b));
    }

    #[test]
    fn an_exclusive_lock_blocks_a_concurrent_exclusive_lock_on_the_same_slot() {
        let cache = Arc::new(ChunkCache::<&str, i32>::new(10));
        let barrier = Arc::new(Barrier::new(2));

        let holder_cache = cache.clone();
        let holder_barrier = barrier.clone();
        let holder = thread::spawn(move || {
            let slot = holder_cache.slot(&"chunk-1");
            let mut guard = slot.write().unwrap();
            *guard = Some(1);
            holder_barrier.wait(); // let the other thread confirm it's blocked
            thread::sleep(Duration::from_millis(200));
        });

        barrier.wait();
        let start = std::time::Instant::now();
        let slot = cache.slot(&"chunk-1");
        let _guard = slot.write().unwrap(); // must wait for `holder` to drop its guard
        assert!(
            start.elapsed() >= Duration::from_millis(150),
            "acquired the exclusive lock too fast -- it wasn't actually excluded"
        );

        holder.join().unwrap();
    }

    #[test]
    fn an_exclusive_lock_on_one_key_does_not_block_another_key() {
        let cache = Arc::new(ChunkCache::<&str, i32>::new(10));
        let barrier = Arc::new(Barrier::new(2));

        let holder_cache = cache.clone();
        let holder_barrier = barrier.clone();
        let holder = thread::spawn(move || {
            let slot = holder_cache.slot(&"chunk-1");
            let _guard = slot.write().unwrap();
            holder_barrier.wait();
            thread::sleep(Duration::from_millis(200));
        });

        barrier.wait();
        let start = std::time::Instant::now();
        let slot = cache.slot(&"chunk-2"); // a different key -- must not contend
        let _guard = slot.write().unwrap();
        assert!(
            start.elapsed() < Duration::from_millis(150),
            "an unrelated key's slot waited on chunk-1's holder"
        );

        holder.join().unwrap();
    }

    // --- LRU eviction ---

    #[test]
    fn the_table_never_exceeds_max_entries_once_full() {
        let cache: ChunkCache<i32, i32> = ChunkCache::new(3);
        for k in 0..10 {
            cache.slot(&k);
            assert!(cache.len() <= 3);
        }
        assert_eq!(cache.len(), 3);
    }

    #[test]
    fn inserting_past_capacity_evicts_the_least_recently_used_key() {
        let cache: ChunkCache<i32, i32> = ChunkCache::new(2);
        cache.slot(&1);
        cache.slot(&2);
        cache.slot(&3); // table is full (1, 2) -- 1 is the least recently used, evicted

        assert_eq!(cache.len(), 2);
        // 2 and 3 are still the same slots as before (no eviction touched
        // them); 1 is gone, so asking for it again allocates a brand new
        // slot rather than returning the old one.
        let fresh_one = cache.slot(&1);
        assert!(
            fresh_one.read().unwrap().is_none(),
            "key 1 should have been evicted and this should be a fresh slot"
        );
    }

    #[test]
    fn touching_a_key_again_protects_it_from_eviction() {
        // Neither slot() call's returned Arc is held onto here, so both
        // entries are unpinned (strong_count 1) the moment each statement
        // ends -- eviction below is decided purely by recency, not by one
        // of them still being in use (see the pinning test above for that
        // case instead). A written sentinel value is what lets this test
        // tell "still the original entry" apart from "evicted and
        // recreated empty", since a fresh slot is indistinguishable from
        // an untouched one otherwise.
        let cache: ChunkCache<i32, i32> = ChunkCache::new(2);
        *cache.slot(&1).write().unwrap() = Some(1);
        *cache.slot(&2).write().unwrap() = Some(2);
        cache.slot(&1); // re-touch 1 -- now 2 is the least recently used
        cache.slot(&3); // table's full (1, 2) -- evicts 2, not 1

        assert_eq!(
            *cache.slot(&1).read().unwrap(),
            Some(1),
            "1 was touched more recently than 2, so it should have survived eviction"
        );
        assert_eq!(
            *cache.slot(&2).read().unwrap(),
            None,
            "2 should have been evicted and replaced with a fresh, empty slot"
        );
    }

    #[test]
    fn a_slot_still_in_use_is_not_evicted_even_though_its_least_recently_used() {
        let cache: ChunkCache<i32, i32> = ChunkCache::new(1);
        let one = cache.slot(&1); // held alive for the rest of this test
        *one.write().unwrap() = Some(100);

        // The table is at capacity (1 entry) and key 1 is the only (and
        // therefore least-recently-used) entry, but this caller is still
        // holding `one` -- inserting a new key must not evict it out from
        // under that reference.
        cache.slot(&2);

        assert_eq!(*one.read().unwrap(), Some(100), "still the same, live slot");
    }

    #[test]
    fn max_entries_zero_still_lets_every_call_succeed() {
        // A degenerate but legitimate config: caching is effectively
        // disabled (every slot() is immediately eviction-eligible), but
        // nothing should panic or hang.
        let cache: ChunkCache<i32, i32> = ChunkCache::new(0);
        for k in 0..5 {
            let slot = cache.slot(&k);
            assert!(slot.read().unwrap().is_none());
        }
    }
}
