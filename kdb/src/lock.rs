//! Cross-*process* advisory file locking, via the OS (through
//! `std::fs::File::lock`/`lock_shared`, stable in `std` -- no external
//! crate needed to keep `kdb` dependency-free even for this).
//!
//! Everything inside one `World` value is already single-threaded-safe by
//! construction (every method takes `&mut self`), but nothing stops two
//! different *processes* -- e.g. two `kdbserver` instances pointed at the
//! same `--data-dir` -- from opening the same world at once. Only the OS
//! can arbitrate across that boundary, which is what this module is for.

use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;

/// An OS-level advisory lock, held on a (possibly newly-created) file at
/// `path` for as long as this value lives. The OS releases the lock when
/// the underlying file descriptor is closed, so simply dropping a
/// `FileLock` releases it -- there's no separate `unlock` to remember to
/// call, and a process that dies while holding one doesn't leave it stuck
/// (the OS cleans it up when the process's file descriptors close).
///
/// This is advisory, not mandatory: it only excludes *other holders of a
/// `FileLock` on the same path*, the same way every lock in this crate is
/// used. It does nothing to stop a process from, say, editing the target
/// file directly while ignoring the lock.
pub struct FileLock {
    _file: File,
}

impl FileLock {
    /// Blocks until an exclusive lock on `path` is held, creating the file
    /// first if it doesn't exist. At most one exclusive lock (and no
    /// shared locks) can be held on a given path at once. Use this for
    /// anything that's about to write.
    pub fn exclusive(path: &Path) -> io::Result<FileLock> {
        let file = open(path)?;
        file.lock()?;
        Ok(FileLock { _file: file })
    }

    /// Blocks until a shared lock on `path` is held, creating the file
    /// first if it doesn't exist. Any number of shared locks can be held
    /// at once; an exclusive lock waits for all of them to release first.
    /// Use this for anything that only reads, to still exclude a
    /// concurrent writer (so a reader can never observe another process's
    /// write half-finished).
    pub fn shared(path: &Path) -> io::Result<FileLock> {
        let file = open(path)?;
        file.lock_shared()?;
        Ok(FileLock { _file: file })
    }
}

fn open(path: &Path) -> io::Result<File> {
    // Explicitly not `truncate(true)`: some callers (`Schema`) lock a file
    // that's also their actual content, not just a sidecar -- truncating
    // it just from taking a lock would be catastrophic.
    OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Barrier};
    use std::thread;

    fn temp_path(tag: &str) -> std::path::PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("kdb-lock-test-{tag}-{}-{n}", std::process::id()))
    }

    #[test]
    fn exclusive_creates_the_file_if_missing() {
        let path = temp_path("create");
        assert!(!path.exists());
        let _lock = FileLock::exclusive(&path).unwrap();
        assert!(path.exists());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn multiple_shared_locks_coexist() {
        let path = temp_path("shared-shared");
        let a = FileLock::shared(&path).unwrap();
        let b = FileLock::shared(&path).unwrap();
        drop(a);
        drop(b);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn dropping_a_lock_releases_it() {
        // Two exclusive locks can't coexist -- if the first `FileLock`
        // weren't releasing on drop, acquiring the second (from another
        // thread, so it's a genuinely different OS-level lock holder,
        // not just a reentrant acquisition by the same file description)
        // would hang. A bounded wait via a background thread proves it
        // doesn't.
        let path = temp_path("release-on-drop");
        {
            let _lock = FileLock::exclusive(&path).unwrap();
        } // dropped here

        let (tx, rx) = std::sync::mpsc::channel();
        let path2 = path.clone();
        thread::spawn(move || {
            let _lock = FileLock::exclusive(&path2).unwrap();
            tx.send(()).unwrap();
        });
        rx.recv_timeout(std::time::Duration::from_secs(5))
            .expect("second exclusive lock never acquired -- first one didn't release on drop");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn an_exclusive_lock_blocks_a_concurrent_exclusive_lock() {
        let path = temp_path("exclusive-excludes-exclusive");
        let barrier = Arc::new(Barrier::new(2));

        let holder_path = path.clone();
        let holder_barrier = barrier.clone();
        let holder = thread::spawn(move || {
            let _lock = FileLock::exclusive(&holder_path).unwrap();
            holder_barrier.wait(); // let the other thread confirm it's blocked
            thread::sleep(std::time::Duration::from_millis(200));
        });

        barrier.wait();
        let start = std::time::Instant::now();
        let _lock = FileLock::exclusive(&path).unwrap(); // must wait for `holder` to drop its lock
        assert!(
            start.elapsed() >= std::time::Duration::from_millis(150),
            "acquired the exclusive lock too fast -- it wasn't actually excluded"
        );

        holder.join().unwrap();
        let _ = std::fs::remove_file(&path);
    }
}
