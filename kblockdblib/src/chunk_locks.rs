//! In-process, per-chunk locking.
//!
//! kBlockDB now assumes exactly one `World` is ever open against a given
//! data directory at a time (see `World`'s "Concurrency" doc comment), with
//! concurrency coming from multiple *threads* sharing that one `World`
//! rather than from multiple processes -- or multiple independent `World`
//! handles in one process -- pointed at the same directory. That means
//! `with_chunk`'s per-chunk exclusion no longer needs to cross a process
//! boundary: a plain in-memory `RwLock`, one per chunk actually touched, is
//! all that's needed, and it costs no syscalls (no `open`/`lock` on a
//! sidecar `.lock` file, the way the old cross-process design worked).

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, Mutex, PoisonError, RwLock};

/// Hands out one `RwLock<()>` per distinct `K`, creating it the first time
/// that key is asked for and keeping it for the life of the table -- a
/// chunk, once touched, keeps its lock for the life of the owning `World`
/// (a few dozen bytes per distinct chunk ever touched in this process, not
/// per chunk that could ever exist -- a large, mostly-empty world costs
/// nothing here for the chunks nobody's touched).
pub struct ChunkLocks<K> {
    locks: Mutex<HashMap<K, Arc<RwLock<()>>>>,
}

impl<K: Eq + Hash + Clone> ChunkLocks<K> {
    pub fn new() -> Self {
        ChunkLocks {
            locks: Mutex::new(HashMap::new()),
        }
    }

    /// The lock for `key`, creating it if this is the first time `key` has
    /// been asked for. Briefly locks the whole table to look up or insert
    /// the entry, then hands back an `Arc` the caller locks (shared or
    /// exclusive) independently -- so two calls for two *different* keys
    /// only ever contend on this quick lookup, never on each other's
    /// actual chunk I/O.
    pub fn get(&self, key: &K) -> Arc<RwLock<()>> {
        let mut locks = self.locks.lock().unwrap_or_else(PoisonError::into_inner);
        locks
            .entry(key.clone())
            .or_insert_with(|| Arc::new(RwLock::new(())))
            .clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn the_same_key_always_yields_the_same_lock() {
        let locks = ChunkLocks::new();
        let a = locks.get(&"chunk-1");
        let b = locks.get(&"chunk-1");
        assert!(Arc::ptr_eq(&a, &b));
    }

    #[test]
    fn different_keys_yield_different_locks() {
        let locks = ChunkLocks::new();
        let a = locks.get(&"chunk-1");
        let b = locks.get(&"chunk-2");
        assert!(!Arc::ptr_eq(&a, &b));
    }

    #[test]
    fn multiple_shared_locks_on_the_same_key_coexist() {
        let locks: ChunkLocks<&str> = ChunkLocks::new();
        let lock = locks.get(&"chunk-1");
        let a = lock.read().unwrap();
        let b = lock.read().unwrap();
        drop((a, b));
    }

    #[test]
    fn an_exclusive_lock_blocks_a_concurrent_exclusive_lock_on_the_same_key() {
        let locks = Arc::new(ChunkLocks::new());
        let barrier = Arc::new(Barrier::new(2));

        let holder_locks = locks.clone();
        let holder_barrier = barrier.clone();
        let holder = thread::spawn(move || {
            let lock = holder_locks.get(&"chunk-1");
            let _guard = lock.write().unwrap();
            holder_barrier.wait(); // let the other thread confirm it's blocked
            thread::sleep(Duration::from_millis(200));
        });

        barrier.wait();
        let start = std::time::Instant::now();
        let lock = locks.get(&"chunk-1");
        let _guard = lock.write().unwrap(); // must wait for `holder` to drop its guard
        assert!(
            start.elapsed() >= Duration::from_millis(150),
            "acquired the exclusive lock too fast -- it wasn't actually excluded"
        );

        holder.join().unwrap();
    }

    #[test]
    fn an_exclusive_lock_on_one_key_does_not_block_another_key() {
        let locks = Arc::new(ChunkLocks::new());
        let barrier = Arc::new(Barrier::new(2));

        let holder_locks = locks.clone();
        let holder_barrier = barrier.clone();
        let holder = thread::spawn(move || {
            let lock = holder_locks.get(&"chunk-1");
            let _guard = lock.write().unwrap();
            holder_barrier.wait();
            thread::sleep(Duration::from_millis(200));
        });

        barrier.wait();
        let start = std::time::Instant::now();
        let lock = locks.get(&"chunk-2"); // a different key -- must not contend
        let _guard = lock.write().unwrap();
        assert!(
            start.elapsed() < Duration::from_millis(150),
            "an unrelated key's lock waited on chunk-1's holder"
        );

        holder.join().unwrap();
    }
}
