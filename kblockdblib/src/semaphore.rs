//! A small hand-rolled counting semaphore (`std::sync::Mutex` +
//! `Condvar` -- `std` never shipped a general-purpose one, and pulling in
//! a crate just for this would mean a dependency `kblockdblib` doesn't
//! have to take on).
//!
//! `World` uses this to cap how much real concurrent filesystem work
//! (`with_chunk`'s `create_dir_all`/file I/O) is in flight at once -- see
//! the "Concurrency" section of `World`'s doc comment for why that cap
//! exists at all: on at least one real, fast, local SSD (see that doc
//! comment for how this was measured), concurrent small-file metadata
//! operations stopped scaling well past a few dozen in flight, and piling
//! on more made *aggregate* throughput worse, not better.

use std::sync::{Condvar, Mutex, PoisonError};

pub struct Semaphore {
    capacity: usize,
    permits: Mutex<usize>,
    available: Condvar,
}

impl Semaphore {
    pub fn new(permits: usize) -> Semaphore {
        Semaphore {
            capacity: permits,
            permits: Mutex::new(permits),
            available: Condvar::new(),
        }
    }

    /// The total number of permits this semaphore was created with --
    /// fixed for its lifetime, unlike the number currently available.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// How many permits are free right now.
    ///
    /// Inherently a snapshot -- any other thread can acquire or release
    /// between this returning and the caller looking at it -- so it's no
    /// basis for deciding whether a subsequent `acquire` will block. It
    /// exists for assertions made from *inside* a held permit, where the
    /// caller knows what it is itself holding, which is why it's
    /// test-only: `World`'s own code has no business branching on it.
    #[cfg(test)]
    pub fn available(&self) -> usize {
        *self.permits.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Blocks the calling thread until a permit is available, then holds
    /// it until the returned guard drops.
    pub fn acquire(&self) -> SemaphorePermit<'_> {
        let mut permits = self.permits.lock().unwrap_or_else(PoisonError::into_inner);
        while *permits == 0 {
            permits = self
                .available
                .wait(permits)
                .unwrap_or_else(PoisonError::into_inner);
        }
        *permits -= 1;
        SemaphorePermit { sem: self }
    }
}

/// Releases its permit (and wakes one waiter, if any) when dropped.
pub struct SemaphorePermit<'a> {
    sem: &'a Semaphore,
}

impl Drop for SemaphorePermit<'_> {
    fn drop(&mut self) {
        let mut permits = self
            .sem
            .permits
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        *permits += 1;
        // One waiter is enough: whichever thread wakes either finds a
        // permit (takes it) or, if another release raced it, just loops
        // back to sleep -- see `acquire`'s `while`, not `if`.
        self.sem.available.notify_one();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};
    use std::thread;
    use std::time::Duration;

    #[test]
    fn permits_up_to_capacity_are_granted_without_blocking() {
        let sem = Semaphore::new(3);
        let a = sem.acquire();
        let b = sem.acquire();
        let c = sem.acquire();
        drop((a, b, c));
    }

    #[test]
    fn acquire_blocks_when_no_permits_are_available() {
        let sem = Arc::new(Semaphore::new(1));
        let holder_ready = Arc::new(Barrier::new(2));

        let sem2 = sem.clone();
        let barrier2 = holder_ready.clone();
        let holder = thread::spawn(move || {
            let _permit = sem2.acquire(); // takes the only permit
            barrier2.wait(); // let the main thread know it's held
            thread::sleep(Duration::from_millis(200));
            // permit released here, on drop
        });

        holder_ready.wait();
        let start = std::time::Instant::now();
        let _permit = sem.acquire(); // must wait for `holder` to release
        assert!(
            start.elapsed() >= Duration::from_millis(150),
            "acquired a permit too fast -- the only one wasn't actually held"
        );
        holder.join().unwrap();
    }

    #[test]
    fn dropping_a_permit_makes_it_available_again() {
        let sem = Semaphore::new(1);
        {
            let _permit = sem.acquire();
        } // dropped here
        let _permit = sem.acquire(); // would hang if the first didn't release
    }

    #[test]
    fn available_tracks_permits_taken_and_returned() {
        let sem = Semaphore::new(3);
        assert_eq!(sem.available(), 3);
        let a = sem.acquire();
        assert_eq!(sem.available(), 2);
        let b = sem.acquire();
        assert_eq!(sem.available(), 1);
        drop(b);
        assert_eq!(sem.available(), 2);
        drop(a);
        assert_eq!(sem.available(), 3);
    }

    #[test]
    fn only_n_threads_run_inside_the_critical_section_at_once() {
        // n threads all try to acquire a 2-permit semaphore and record the
        // concurrent occupancy while they hold it; occupancy must never
        // exceed 2.
        let sem = Arc::new(Semaphore::new(2));
        let occupancy = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let max_seen = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let handles: Vec<_> = (0..8)
            .map(|_| {
                let sem = sem.clone();
                let occupancy = occupancy.clone();
                let max_seen = max_seen.clone();
                thread::spawn(move || {
                    let _permit = sem.acquire();
                    let now = occupancy.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                    max_seen.fetch_max(now, std::sync::atomic::Ordering::SeqCst);
                    thread::sleep(Duration::from_millis(20));
                    occupancy.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }

        assert!(max_seen.load(std::sync::atomic::Ordering::SeqCst) <= 2);
    }
}
