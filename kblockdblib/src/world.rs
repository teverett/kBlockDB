use crate::chunk::{self, Chunk};
use crate::chunk_cache::ChunkCache;
pub use crate::coord::Coord;
use crate::params::WorldParams;
use crate::schema::Schema;
use crate::semaphore::Semaphore;
use crate::value::Value;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard, PoisonError};

/// Default number of spatial axes for a *new* world, i.e. what `main`'s demo
/// calls `World::create` with. This is only a default: a world's real axis
/// count is a per-world runtime value fixed at `create()` time and
/// persisted in `world.txt` (see `params::WorldParams`), not a compile-time
/// constant baked into the binary -- `World::open` reads it from disk, so
/// changing this constant later doesn't (and can't) alter any world that
/// already exists on disk.
pub const AXES: usize = 3;

/// Default cells per axis for a *new* world -- see `AXES`'s doc comment;
/// the same "default at create time, persisted and authoritative
/// afterward" rule applies.
pub const WORLD_DIM: u32 = 10_000;

/// Default cells per axis *within a chunk* for a *new* world -- see
/// `AXES`'s doc comment; the same "default at create time, persisted and
/// authoritative afterward" rule applies. With the default 3 axes, that's
/// 32^3 = 32,768 cells/chunk (see `chunk::chunk_cells`). A larger value
/// means fewer, bigger chunk files (each write-through write rewrites the
/// *whole* chunk -- see `World`'s "Concurrency" doc comment -- so bigger
/// chunks mean more bytes rewritten per single-cell write, but fewer
/// distinct chunk files/directories for a given world); a smaller value is
/// the opposite trade. Not a measured optimum for any particular
/// deployment -- measure it for yours (e.g. with `kblockdbperf`).
pub const DEFAULT_CHUNK_DIM: u32 = 32;

/// Default cap on how many `with_chunk` calls -- i.e. real concurrent
/// filesystem operations (`create_dir_all`/file I/O) -- a `World`
/// will have in flight at once. Override with
/// `World::with_max_concurrent_disk_ops`.
///
/// This exists because concurrent small-file/metadata operations don't
/// scale indefinitely on every filesystem: measured on one real, fast,
/// local SSD, throughput roughly doubled going from 1 to 8-32 concurrent
/// `with_chunk` calls, then *fell below the single-digit-concurrency
/// baseline* at 128 -- letting too many pile up was worse than capping
/// them, not just a diminishing return. 32 is a reasonable starting point,
/// not a measured optimum for any particular deployment; the right number
/// is a property of the underlying filesystem/storage, not of `kblockdblib` --
/// measure it for yours (e.g. with `kblockdbperf`'s `concurrency_scan`) rather
/// than trusting this default.
pub const DEFAULT_MAX_CONCURRENT_DISK_OPS: usize = 32;

/// Default cap on how many distinct chunks the write-through cache (see
/// `World`'s "Concurrency" doc comment and `crate::chunk_cache`) keeps in
/// memory at once. Once a `World` has touched this many distinct chunks,
/// touching a new one evicts the least-recently-used one to make room.
/// Override with `World::with_max_cached_chunks`.
///
/// 100,000 is a reasonable starting point -- an empty (never-written)
/// chunk costs next to nothing (see `Chunk::new`), so this comfortably
/// covers a working set of a few hundred thousand touched chunks without
/// growing unbounded for the life of the process -- not a measured optimum
/// for any particular deployment; the right number depends on how large
/// your actual hot working set is and how much memory you can spare for
/// it.
pub const DEFAULT_MAX_CACHED_CHUNKS: usize = 100_000;

const _: () = assert!(AXES >= 1, "AXES must be at least 1");
const _: () = assert!(
    DEFAULT_CHUNK_DIM >= 1,
    "DEFAULT_CHUNK_DIM must be at least 1"
);

type ChunkKey = Coord;

/// An axis-aligned box of cells: `origin` plus `extent`, i.e. the set of
/// coordinates `c` with `origin[a] <= c[a] < origin[a] + extent[a]` on
/// every axis `a`. `origin.len()` and `extent.len()` must both equal the
/// region's axis count. Bundles what would otherwise be `2*axes` separate
/// parameters on every `*_region` method, and may span or partially cover
/// any number of chunks -- see `World::get_region`/`set_region`/
/// `remove_region`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Region {
    pub origin: Coord,
    pub extent: Coord,
}

impl Region {
    /// Builds a `Region` from any origin/extent that convert to `Coord`.
    /// Doesn't itself require `origin.len() == extent.len()` -- `Region` is
    /// a plain data holder, and this crate is a library other code (e.g. a
    /// server parsing coordinates out of a URL) can call with attacker- or
    /// user-controlled input, so it's not this constructor's place to
    /// `panic!`/`debug_assert!` on a shape mismatch. `World::check_region`
    /// (run by every `*_region` method before touching any data) is what
    /// actually validates a `Region` and reports a mismatch as a proper
    /// `io::Result` error instead.
    pub fn new(origin: impl Into<Coord>, extent: impl Into<Coord>) -> Self {
        Region {
            origin: origin.into(),
            extent: extent.into(),
        }
    }

    pub fn volume(&self) -> u64 {
        self.extent.iter().map(|&d| u64::from(d)).product()
    }

    fn axes(&self) -> usize {
        self.origin.len()
    }
}

/// Iterates every coordinate inside a `Region`, axis 0 fastest and the last
/// axis slowest -- the generalization of an x-fastest/y/z convention to an
/// arbitrary number of axes. `get_region`'s result and `set_region`'s
/// expected `values` are in exactly this order.
struct RegionIter {
    origin: Coord,
    extent: Coord,
    cursor: Coord,
    done: bool,
}

impl RegionIter {
    fn new(region: &Region) -> Self {
        RegionIter {
            origin: region.origin.clone(),
            done: region.extent.contains(&0), // any zero-length axis makes the whole region empty
            extent: region.extent.clone(),
            cursor: Coord::zeros(region.axes()),
        }
    }
}

impl Iterator for RegionIter {
    type Item = Coord;

    fn next(&mut self) -> Option<Coord> {
        if self.done {
            return None;
        }
        let coord: Coord = self
            .origin
            .iter()
            .zip(self.cursor.iter())
            .map(|(&o, &c)| o + c)
            .collect();

        // Advance like an odometer: axis 0 rolls over fastest, and once the
        // last axis rolls over every coordinate has been visited.
        let last = self.cursor.len() - 1;
        for (a, cur) in self.cursor.iter_mut().enumerate() {
            *cur += 1;
            if *cur < self.extent[a] {
                break;
            }
            *cur = 0;
            if a == last {
                self.done = true;
            }
        }
        Some(coord)
    }
}

/// The on-disk, chunked, columnar key-value world.
///
/// Layout on disk under `root`:
///   root/world.txt                  -- this world's axes/world_dim (see params.rs)
///   root/schema.txt                 -- key string <-> id registry (see schema.rs)
///   root/<c0>/<c1>/.../<c_n-1>.chunk -- one file per non-empty chunk,
///                                       nested axes-1 directories deep
///
/// The directory nesting keeps any single directory to at most
/// `chunks_per_axis()` entries no matter how large the world gets, and
/// chunks with no data in them are simply never written -- a 10,000^3
/// world (1 trillion cells) that's mostly empty costs disk space
/// proportional to how much of it is actually populated, not to its
/// nominal size.
///
/// **Concurrency.** Exactly one `World` is ever meant to be open against a
/// given `root` at a time, shared across as many *threads* of that one
/// process as needed -- not one `World` per thread, and never two
/// processes pointed at the same directory at once (see `kblockdbserver/
/// src/state.rs`, which holds its one `World` behind a plain `Arc`, shared
/// by every request-handling thread). Within that one `World`, every
/// operation locks (via `crate::chunk_cache::ChunkCache`, a plain
/// in-memory per-chunk `RwLock`, not an OS-level file lock) exactly the
/// chunk(s) it touches -- shared for a read, exclusive for a write.
///
/// **Chunks are cached, not just locked.** Since this process is always
/// the sole writer, a chunk's in-memory contents -- once read off disk --
/// can never fall behind what's on disk, so `with_chunk_read`/
/// `with_chunk_write` load a chunk from disk (or synthesize an empty one,
/// if it's never been written) only the *first* time this `World` touches
/// it (or the first time again, after it's been evicted -- see below);
/// every access in between reads the cached copy straight out of memory,
/// no disk I/O at all. Writes are still *write-through*, not write-behind:
/// `with_chunk_write` still writes the chunk straight back to disk before
/// releasing its lock and returning, so `set`/`remove` stay exactly as
/// durable as before -- caching only removes redundant *reads*, never
/// defers a write. The cache itself is bounded (`DEFAULT_MAX_CACHED_CHUNKS`/
/// `World::with_max_cached_chunks`, an LRU eviction policy once full -- see
/// `crate::chunk_cache`), so memory doesn't grow forever for a workload
/// that keeps touching new chunks. The schema (`schema.rs`) gets the same
/// treatment: it's held behind `World`'s own `Mutex<Schema>`, so two
/// threads racing to intern two *different* new keys can't collide on the
/// same id. What this does *not* give you is cross-call atomicity: a `get`
/// immediately followed by a `set` from the same caller is two separate
/// locked operations, not one transaction, so another thread's write can
/// land in between them -- same as most simple key/value stores without
/// an explicit read-modify-write or transaction API.
///
/// **All of this is also why every method takes `&self`, not `&mut
/// self`.** `World`'s only interior state is `schema` (behind its own
/// `Mutex`), a lazily-populated cache of per-chunk locks and contents, and
/// a couple of atomic counters -- so the one process can hold its one
/// `World` behind an `Arc` (no outer `Mutex<World>` needed) and let
/// concurrent requests actually run concurrently, limited only by the same
/// per-chunk locks that keep two threads from corrupting the same chunk.
/// `kblockdbserver` does exactly this: earlier, it serialized every
/// request through one `Mutex<World>`, which meant one slow request -- or
/// just a lot of concurrent ones -- stalled every other request
/// regardless of whether they touched the same chunk at all.
///
/// **Opening more than one `World` (in this process or another) against
/// the same `root` at the same time is not supported and will corrupt
/// data.** Each `World` keeps its own private chunk cache and its own
/// private `Schema` cache, so two independent `World`s have no way to
/// coordinate a write to the same chunk file -- exactly the failure mode
/// the old, OS-level-file-lock-based design existed to prevent when
/// multiple processes were a supported deployment shape. They no longer
/// are: run exactly one process, with as many threads as you like, sharing
/// one `World`.
/// Aggregate on-disk statistics for a world's data, as of whenever
/// `World::stats` was called -- a live snapshot, not tracked incrementally,
/// so it reflects concurrent writers too (see `World::stats`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Stats {
    /// Number of `.chunk` files on disk, i.e. how many chunks currently
    /// hold at least one cell (an emptied chunk's file is deleted, not
    /// left behind empty -- see `with_chunk`).
    pub total_chunks: u64,
    /// Sum of those files' sizes, in bytes.
    pub total_bytes: u64,
    /// Sum of those files' actual on-disk allocation, in 512-byte blocks
    /// (`stat(2)`'s `st_blocks`) -- can be smaller than `total_bytes / 512`
    /// for chunks with sparse (never-written) regions, which is most of
    /// them in a large, mostly-empty world (see `World`'s "Layout on disk"
    /// doc comment). Platforms without that metadata (Windows) fall back
    /// to `total_bytes` rounded up to whole 512-byte blocks -- an upper
    /// bound, not a real sparse-file measurement, on those platforms.
    pub total_blocks: u64,
}

pub struct World {
    root: PathBuf,
    axes: usize,
    world_dim: u32,
    chunk_dim: u32,
    chunk_cells: usize,
    schema: Mutex<Schema>,
    /// Number of chunk files this `World` has actually opened and read
    /// from disk since it was opened -- not every `get`/`set`/`remove`
    /// call: this is 0 for a chunk that's never been written (there's no
    /// file to read) and at most 1 per chunk ever touched, no matter how
    /// many times it's queried after the first -- see `chunk_cache`. Read
    /// via `chunks_read_from_disk()`.
    chunks_read_from_disk: AtomicU64,
    /// Number of chunk files this `World` has (over)written to disk since
    /// it was opened. Unlike `chunks_read_from_disk`, this *does* count
    /// every write, not just the first: caching removes redundant reads,
    /// not writes (writes are still write-through -- see `World`'s
    /// "Concurrency" doc comment), so a cell `set` four times in the same
    /// chunk, even in a row, is still four writes. Read via
    /// `chunks_written_to_disk()`.
    chunks_written_to_disk: AtomicU64,
    /// Caps how many actual disk operations (a cache-miss read, or any
    /// write) run concurrently -- see `DEFAULT_MAX_CONCURRENT_DISK_OPS`. A
    /// cache-hit read doesn't touch this at all: there's no disk operation
    /// to cap.
    disk_io: Semaphore,
    /// Chunks this `World` has touched, cached in memory up to
    /// `DEFAULT_MAX_CACHED_CHUNKS`/`with_max_cached_chunks`'s cap (an LRU
    /// eviction policy beyond that) -- see `with_chunk_read`/
    /// `with_chunk_write` and `crate::chunk_cache`.
    chunk_cache: ChunkCache<ChunkKey, Chunk>,
}

// `World` needs to be usable as `Arc<World>` shared across threads (see its
// doc comment above) -- this is a compile-time check that it actually is,
// rather than leaving that as an implicit assumption a later change could
// silently break.
const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<World>();
};

impl World {
    /// Creates a new world at `root`, or validates an existing one there.
    ///
    /// If `root/world.txt` doesn't exist yet, this world is brand new: it's
    /// created with exactly the given `axes`/`world_dim`/`chunk_dim`,
    /// persisted so every later `open`/`create` of this directory sees the
    /// same shape. If `root/world.txt` already exists, all three must match
    /// it exactly, or this fails with `InvalidInput` *without creating or
    /// opening anything* -- a world's shape can never change underneath
    /// data already written for it.
    pub fn create<P: AsRef<Path>>(
        root: P,
        axes: usize,
        world_dim: u32,
        chunk_dim: u32,
    ) -> io::Result<Self> {
        if axes == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "axes must be at least 1",
            ));
        }
        if chunk_dim == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "chunk_dim must be at least 1",
            ));
        }
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root)?;
        let requested = WorldParams {
            axes,
            world_dim,
            chunk_dim,
        };

        // Atomic with respect to any other process doing the same thing at
        // the same moment -- see `WorldParams::create_or_validate`.
        if WorldParams::create_or_validate(&root, requested)? {
            crate::logger::info(format!(
                "created world at {} (axes={axes}, world_dim={world_dim}, chunk_dim={chunk_dim})",
                root.display()
            ));
        }
        Self::open_inner(root, requested)
    }

    /// Opens an existing world at `root`. `root/world.txt` must already
    /// exist (via a prior `World::create`) -- `open` reads the world's
    /// shape from disk rather than assuming any default, so it always
    /// matches whatever the world was actually created with.
    pub fn open<P: AsRef<Path>>(root: P) -> io::Result<Self> {
        let root = root.as_ref().to_path_buf();
        let params = WorldParams::read(&root)?.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "no world.txt at {} -- call World::create to initialize a new world here \
                     first",
                    root.display()
                ),
            )
        })?;
        Self::open_inner(root, params)
    }

    fn open_inner(root: PathBuf, params: WorldParams) -> io::Result<Self> {
        fs::create_dir_all(&root)?;
        let schema = Schema::open(&root)?;
        crate::logger::info(format!(
            "world opened at {} (axes={}, world_dim={}, chunk_dim={}, {} keys already interned)",
            root.display(),
            params.axes,
            params.world_dim,
            params.chunk_dim,
            schema.len()
        ));
        Ok(World {
            root,
            axes: params.axes,
            world_dim: params.world_dim,
            chunk_dim: params.chunk_dim,
            chunk_cells: chunk::chunk_cells(params.axes, params.chunk_dim),
            schema: Mutex::new(schema),
            chunks_read_from_disk: AtomicU64::new(0),
            chunks_written_to_disk: AtomicU64::new(0),
            disk_io: Semaphore::new(DEFAULT_MAX_CONCURRENT_DISK_OPS),
            chunk_cache: ChunkCache::new(DEFAULT_MAX_CACHED_CHUNKS),
        })
    }

    /// Overrides the cap on concurrent `with_chunk` calls (real concurrent
    /// filesystem operations) -- see `DEFAULT_MAX_CONCURRENT_DISK_OPS` for
    /// why this exists and how to pick a value. `0` is treated as `1`
    /// (a cap of zero would mean no call could ever acquire a permit,
    /// deadlocking every operation forever -- clearly not what "0" was
    /// meant to ask for).
    ///
    /// Chainable right after `create`/`open`, since it takes and returns
    /// `Self`:
    /// ```no_run
    /// # fn main() -> std::io::Result<()> {
    /// let world = kblockdblib::World::create("./data", 3, 10_000, 32)?
    ///     .with_max_concurrent_disk_ops(8);
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_max_concurrent_disk_ops(mut self, n: usize) -> Self {
        self.disk_io = Semaphore::new(n.max(1));
        self
    }

    /// Overrides the cap on how many distinct chunks the write-through
    /// cache keeps in memory at once -- see `DEFAULT_MAX_CACHED_CHUNKS` for
    /// why this exists and how to pick a value. Unlike
    /// `with_max_concurrent_disk_ops`, `0` is a legitimate (if
    /// self-defeating) value here: it doesn't deadlock anything, it just
    /// means every chunk is evicted about as soon as it's cached, so
    /// caching stops helping -- there's no correctness reason to floor it.
    ///
    /// Chainable right after `create`/`open`, same as
    /// `with_max_concurrent_disk_ops`:
    /// ```no_run
    /// # fn main() -> std::io::Result<()> {
    /// let world = kblockdblib::World::create("./data", 3, 10_000, 32)?
    ///     .with_max_cached_chunks(1_000_000);
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_max_cached_chunks(mut self, n: usize) -> Self {
        self.chunk_cache = ChunkCache::new(n);
        self
    }

    /// Locks `schema` (recovering rather than panicking if a prior panic
    /// left it poisoned: `Schema`'s own mutations build a replacement
    /// `HashMap`/`Vec` and only assign them in as the last step, so a
    /// panic partway through never leaves the *old*, already-consistent
    /// state half-updated -- recovering here can't hand back a corrupt
    /// `Schema`, only, at worst, a slightly stale one).
    fn schema(&self) -> MutexGuard<'_, Schema> {
        self.schema.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Number of chunk files this `World` has read since it was opened --
    /// see the field doc comment for what "read" counts.
    pub fn chunks_read_from_disk(&self) -> u64 {
        self.chunks_read_from_disk.load(Ordering::Relaxed)
    }

    /// Number of chunk files this `World` has (over)written since it was
    /// opened -- see the field doc comment for what "written" counts.
    pub fn chunks_written_to_disk(&self) -> u64 {
        self.chunks_written_to_disk.load(Ordering::Relaxed)
    }

    /// The current cap on concurrent `with_chunk` calls -- see
    /// `DEFAULT_MAX_CONCURRENT_DISK_OPS`/`with_max_concurrent_disk_ops`.
    pub fn max_concurrent_disk_ops(&self) -> usize {
        self.disk_io.capacity()
    }

    /// The current cap on how many distinct chunks the write-through cache
    /// keeps in memory at once -- see
    /// `DEFAULT_MAX_CACHED_CHUNKS`/`with_max_cached_chunks`.
    pub fn max_cached_chunks(&self) -> usize {
        self.chunk_cache.max_entries()
    }

    pub fn axes(&self) -> usize {
        self.axes
    }

    pub fn world_dim(&self) -> u32 {
        self.world_dim
    }

    /// Cells per axis within a chunk -- see `DEFAULT_CHUNK_DIM`.
    pub fn chunk_dim(&self) -> u32 {
        self.chunk_dim
    }

    pub fn chunks_per_axis(&self) -> u32 {
        self.world_dim.div_ceil(self.chunk_dim)
    }

    pub fn schema_len(&self) -> usize {
        self.schema().len()
    }

    /// Walks every chunk file on disk under `root` and totals their count,
    /// size, and disk-block usage into a [`Stats`]. A live filesystem walk,
    /// not a running total -- proportional in cost to how many chunks
    /// currently exist, not to the world's nominal size -- so it reflects
    /// whatever's on disk *right now*, including concurrent writers'
    /// chunks appearing or (once emptied) disappearing mid-walk; such a
    /// chunk is counted or not on a best-effort basis rather than causing
    /// this to fail.
    pub fn stats(&self) -> io::Result<Stats> {
        let mut stats = Stats::default();
        accumulate_chunk_stats(&self.root, &mut stats)?;
        Ok(stats)
    }

    fn chunk_path(&self, ckey: &ChunkKey) -> PathBuf {
        let mut p = self.root.clone();
        for &c in &ckey[..self.axes - 1] {
            p = p.join(c.to_string());
        }
        p.join(format!("{}.chunk", ckey[self.axes - 1]))
    }

    fn split(&self, coord: &[u32]) -> io::Result<(ChunkKey, usize)> {
        if coord.len() != self.axes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "coordinate {coord:?} has {} axes but this world has {}",
                    coord.len(),
                    self.axes
                ),
            ));
        }
        if let Some(&bad) = coord.iter().find(|&&c| c >= self.world_dim) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "coordinate {coord:?} is out of range: {bad} >= world_dim {}",
                    self.world_dim
                ),
            ));
        }
        let mut ckey: ChunkKey = Coord::zeros(self.axes);
        let mut local_idx = 0usize;
        let mut mult = 1usize;
        for (a, c) in coord.iter().enumerate() {
            ckey[a] = c / self.chunk_dim;
            local_idx += (c % self.chunk_dim) as usize * mult;
            mult *= self.chunk_dim as usize;
        }
        Ok((ckey, local_idx))
    }

    /// Runs `f` against chunk `ckey`'s cached contents, read-only. If this
    /// `World` has already touched `ckey`, this is a plain in-memory
    /// shared read -- no disk I/O, no exclusive lock, so any number of
    /// concurrent `with_chunk_read` calls (on this chunk or any other) run
    /// fully in parallel. Otherwise (the first touch), this loads the
    /// chunk from disk once (see `load_chunk`) and caches it before
    /// running `f` -- see the "Concurrency" section of `World`'s own doc
    /// comment.
    fn with_chunk_read<T>(&self, ckey: &ChunkKey, f: impl FnOnce(&Chunk) -> T) -> io::Result<T> {
        let slot = self.chunk_cache.slot(ckey);

        // Fast path: already cached.
        {
            let guard = slot.read().unwrap_or_else(PoisonError::into_inner);
            if let Some(chunk) = guard.as_ref() {
                return Ok(f(chunk));
            }
        }

        // Slow path: never touched by this `World` before. Locks the slot
        // exclusively for the load so two readers racing the very first
        // touch don't both hit disk -- checked again after acquiring the
        // lock in case another thread's load already won that race while
        // this one was waiting for it.
        let mut guard = slot.write().unwrap_or_else(PoisonError::into_inner);
        if guard.is_none() {
            *guard = Some(self.load_chunk(ckey)?);
        }
        Ok(f(guard.as_ref().unwrap()))
    }

    /// Runs `f` against chunk `ckey`'s cached contents, mutably -- loading
    /// it first (same as `with_chunk_read`) if this is the first touch --
    /// and writes the result straight back to disk (or deletes the
    /// chunk's file, if `f` left it empty) before releasing the chunk's
    /// lock and returning. Always exclusive, and always write-*through*:
    /// caching means a repeated `get` skips disk entirely once a chunk is
    /// loaded, but a `set`/`remove` still hits disk on *every* call, so
    /// it's durable the instant this returns -- see the "Concurrency"
    /// section of `World`'s own doc comment.
    fn with_chunk_write<T>(
        &self,
        ckey: &ChunkKey,
        f: impl FnOnce(&mut Chunk) -> T,
    ) -> io::Result<T> {
        // Held for the rest of this function -- see `disk_io`'s field doc
        // comment and `DEFAULT_MAX_CONCURRENT_DISK_OPS`. Acquired before
        // any filesystem call at all, including `create_dir_all`: that one
        // showed the same concurrency-128 slowdown as the read/write calls
        // below it when measured in isolation, so it's inside the cap too,
        // not just the chunk file I/O.
        let _permit = self.disk_io.acquire();

        let slot = self.chunk_cache.slot(ckey);
        let mut guard = slot.write().unwrap_or_else(PoisonError::into_inner);
        if guard.is_none() {
            *guard = Some(self.load_chunk_unmetered(ckey)?);
        }
        let chunk = guard.as_mut().unwrap();

        let result = f(chunk);

        let chunk_path = self.chunk_path(ckey);
        if chunk.is_empty() {
            // Nothing left in this chunk (e.g. every cell was removed) --
            // don't leave a pointless empty file around.
            let _ = fs::remove_file(&chunk_path);
        } else {
            fs::create_dir_all(chunk_path.parent().unwrap())?;
            let file = File::create(&chunk_path)?;
            let mut w = BufWriter::new(file);
            chunk.write_to(&mut w)?;
            self.chunks_written_to_disk.fetch_add(1, Ordering::Relaxed);
        }

        Ok(result)
    }

    /// Reads chunk `ckey` fresh off disk, or synthesizes an empty one if
    /// it's never been written -- the one place a cache slot is first
    /// populated. Gated by `disk_io`, same as the write path in
    /// `with_chunk_write`.
    fn load_chunk(&self, ckey: &ChunkKey) -> io::Result<Chunk> {
        let _permit = self.disk_io.acquire();
        self.load_chunk_unmetered(ckey)
    }

    /// Core of `load_chunk`, minus the `disk_io` permit -- for
    /// `with_chunk_write`, which already holds one for its whole
    /// load-apply-write sequence and would deadlock acquiring a second.
    fn load_chunk_unmetered(&self, ckey: &ChunkKey) -> io::Result<Chunk> {
        let chunk_path = self.chunk_path(ckey);
        if chunk_path.exists() {
            let f = File::open(&chunk_path)?;
            let mut r = BufReader::new(f);
            self.chunks_read_from_disk.fetch_add(1, Ordering::Relaxed);
            Chunk::read_from(&mut r, self.chunk_cells)
        } else {
            Ok(Chunk::new(self.chunk_cells))
        }
    }

    /// Every write this crate has ever made is already durable the moment
    /// its call returns (see `with_chunk_write`) -- there's no dirty
    /// (unwritten) state to flush, only *cached-and-already-written*
    /// state. Kept as a no-op, rather than removed, so code written
    /// against the version of this API that *did* batch writes (this
    /// crate's own demo included) doesn't need to change.
    pub fn flush(&self) -> io::Result<()> {
        Ok(())
    }

    pub fn get(&self, coord: &[u32], key: &str) -> io::Result<Option<Value>> {
        // Validate the coordinate *before* consulting the schema: a
        // malformed/out-of-range coordinate must always be rejected the
        // same way, regardless of whether `key` happens to be known yet --
        // this doubles as a request-validation boundary for callers (like
        // kblockdbserver) that pass through attacker-/user-supplied coordinates.
        let (ckey, local_idx) = self.split(coord)?;
        let Some(key_id) = self.schema().id_for_key(key) else {
            return Ok(None); // this key has never been written anywhere in the world
        };
        self.with_chunk_read(&ckey, |chunk| chunk.get(local_idx, key_id))
    }

    pub fn set(&self, coord: &[u32], key: &str, value: Value) -> io::Result<()> {
        // Validate the coordinate before interning `key`: a failed `set`
        // shouldn't have the side effect of permanently registering a new
        // key that was never actually written anywhere.
        let (ckey, local_idx) = self.split(coord)?;
        // intern also enforces `key`'s type (fixed the first time it's set
        // -- see `Schema`'s doc comment), so a type mismatch fails here,
        // before touching any chunk, as a normal `InvalidInput` error.
        let key_id = self.schema().intern(key, value.value_type())?;
        self.with_chunk_write(&ckey, |chunk| chunk.set(local_idx, key_id, value))
    }

    pub fn remove(&self, coord: &[u32], key: &str) -> io::Result<()> {
        // See `get`: validate the coordinate before the key-existence
        // short-circuit, so a bad coordinate is never silently absorbed
        // into remove's usual "unknown key is a harmless no-op" behavior.
        let (ckey, local_idx) = self.split(coord)?;
        let Some(key_id) = self.schema().id_for_key(key) else {
            // Distinct from "key exists but isn't set on this cell"
            // (routine, logged as INFO below): this key has never been
            // interned anywhere in the world, which more likely means a
            // typo than a deliberate no-op.
            crate::logger::warn(format!(
                "remove() called with unknown key '{key}' at {coord:?} -- no-op"
            ));
            return Ok(());
        };
        self.with_chunk_write(&ckey, |chunk| chunk.remove(local_idx, key_id))?;
        crate::logger::info(format!("removed key '{key}' at {coord:?}"));
        Ok(())
    }

    /// Checks that `region`'s origin and extent both have this world's axis
    /// count -- *not* just that they match each other, since `Region::new`
    /// itself doesn't enforce that (see its doc comment) -- and that it
    /// fits inside the world without overflowing `u32` along the way.
    fn check_region(&self, region: &Region) -> io::Result<()> {
        if region.origin.len() != self.axes || region.extent.len() != self.axes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "region {region:?} has {} origin axes and {} extent axes but this world \
                     has {}",
                    region.origin.len(),
                    region.extent.len(),
                    self.axes
                ),
            ));
        }
        let in_range =
            |lo: u32, len: u32| lo.checked_add(len).is_some_and(|hi| hi <= self.world_dim);
        let fits = region
            .origin
            .iter()
            .zip(region.extent.iter())
            .all(|(&lo, &len)| in_range(lo, len));
        if fits {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "region {region:?} doesn't fit in a {}^{} world",
                    self.world_dim, self.axes
                ),
            ))
        }
    }

    /// Reads `key` for every cell in `region`. The box may span any number
    /// of chunks and cover only part of a chunk along any face -- each cell
    /// is resolved independently through the same chunk/local-index split
    /// as a single-cell `get`, so chunk boundaries need no special-casing
    /// here.
    ///
    /// Results come back in `RegionIter` order (axis 0 fastest): `result[i]`
    /// is `RegionIter::new(region).nth(i)`.
    pub fn get_region(&self, region: &Region, key: &str) -> io::Result<Vec<Option<Value>>> {
        self.check_region(region)?;
        let mut out = vec![None; region.volume() as usize];
        let Some(key_id) = self.schema().id_for_key(key) else {
            return Ok(out); // never interned anywhere: every cell is None
        };
        for (ckey, cells) in self.group_region_by_chunk(region)? {
            let values = self.with_chunk_read(&ckey, |chunk| {
                cells
                    .iter()
                    .map(|&(i, local_idx)| (i, chunk.get(local_idx, key_id)))
                    .collect::<Vec<_>>()
            })?;
            for (i, v) in values {
                out[i] = v;
            }
        }
        Ok(out)
    }

    /// Sets `key` on every cell in `region` from `values`, spanning chunks
    /// and partial chunks exactly like `get_region`. `values` holds one
    /// value per cell, in the same order as `get_region`'s result (see
    /// `RegionIter`), and its length must equal `region.volume()` exactly,
    /// or this returns an `InvalidInput` error without writing anything.
    /// Every value in `values` must also be the same type, and match
    /// `key`'s type if it's been set anywhere before (see `Schema`'s doc
    /// comment) -- same error, same all-or-nothing guarantee. A no-op
    /// region (any axis's extent zero, so `values` must be empty too)
    /// doesn't even intern `key`.
    pub fn set_region(&self, region: &Region, key: &str, values: &[Value]) -> io::Result<()> {
        self.check_region(region)?;
        let volume = region.volume() as usize;
        if values.len() != volume {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "set_region: region {region:?} holds {volume} cells but {} values were given",
                    values.len()
                ),
            ));
        }
        if volume == 0 {
            return Ok(());
        }
        let value_type = values[0].value_type();
        if let Some(bad) = values.iter().position(|v| v.value_type() != value_type) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "set_region: values must all be the same type -- value 0 is {} but \
                     value {bad} is {}",
                    value_type.as_str(),
                    values[bad].value_type().as_str()
                ),
            ));
        }
        let key_id = self.schema().intern(key, value_type)?;
        for (ckey, cells) in self.group_region_by_chunk(region)? {
            self.with_chunk_write(&ckey, |chunk| {
                for (i, local_idx) in cells {
                    chunk.set(local_idx, key_id, values[i].clone());
                }
            })?;
        }
        crate::logger::info(format!(
            "set region {region:?} key '{key}' from {volume} per-cell values"
        ));
        Ok(())
    }

    /// Removes `key` from every cell in `region`, spanning chunks and
    /// partial chunks exactly like `get_region`. Unlike the single-cell
    /// `remove`, this logs one summary line for the whole region rather
    /// than one line per cell, so clearing a large region doesn't flood
    /// `kblockdblib.log`.
    pub fn remove_region(&self, region: &Region, key: &str) -> io::Result<()> {
        self.check_region(region)?;
        if region.volume() == 0 {
            return Ok(());
        }
        let Some(key_id) = self.schema().id_for_key(key) else {
            crate::logger::warn(format!(
                "remove_region() called with unknown key '{key}' at {region:?} -- no-op"
            ));
            return Ok(());
        };
        for (ckey, cells) in self.group_region_by_chunk(region)? {
            self.with_chunk_write(&ckey, |chunk| {
                for (_, local_idx) in cells {
                    chunk.remove(local_idx, key_id);
                }
            })?;
        }
        crate::logger::info(format!(
            "removed region {region:?} key '{key}' ({} cells)",
            region.volume()
        ));
        Ok(())
    }

    /// Groups every coordinate in `region` by which chunk it falls in, so
    /// `get_region`/`set_region`/`remove_region` touch (lock + read [+
    /// write]) each chunk exactly once no matter how many of the region's
    /// cells land in it, rather than once per cell -- an 8x8x8 region
    /// wholly inside one chunk is one chunk touch, not 512. Each cell is
    /// paired with its index in `RegionIter`/`get_region`-result order,
    /// which callers need to scatter results back or pick the matching
    /// input value.
    fn group_region_by_chunk(
        &self,
        region: &Region,
    ) -> io::Result<HashMap<ChunkKey, Vec<(usize, usize)>>> {
        let mut by_chunk: HashMap<ChunkKey, Vec<(usize, usize)>> = HashMap::new();
        for (i, coord) in RegionIter::new(region).enumerate() {
            let (ckey, local_idx) = self.split(&coord)?;
            by_chunk.entry(ckey).or_default().push((i, local_idx));
        }
        Ok(by_chunk)
    }
}

/// Recursively walks `dir` (a world's `root`, or one of its nested
/// per-axis subdirectories -- see `World`'s "Layout on disk" doc comment)
/// and folds every `.chunk` file it finds into `stats`. Tolerates a
/// directory or file vanishing between being listed and being stat'd (a
/// concurrent writer emptying and deleting a chunk) by simply skipping it,
/// rather than failing the whole walk over a single-chunk race -- see
/// `World::stats`.
fn accumulate_chunk_stats(dir: &Path, stats: &mut Stats) -> io::Result<()> {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e),
    };
    for entry in entries {
        let entry = entry?;
        let file_type = match entry.file_type() {
            Ok(file_type) => file_type,
            Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e),
        };
        if file_type.is_dir() {
            accumulate_chunk_stats(&entry.path(), stats)?;
            continue;
        }
        // Excludes `<...>.chunk.lock` files (whose extension is `lock`,
        // not `chunk`) and `world.txt`/`schema.txt` (directly under
        // `root`, so never seen here as anything but a sibling to walk
        // past) without needing to name any of them explicitly.
        if entry.path().extension().is_none_or(|ext| ext != "chunk") {
            continue;
        }
        let metadata = match entry.metadata() {
            Ok(metadata) => metadata,
            Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e),
        };
        stats.total_chunks += 1;
        stats.total_bytes += metadata.len();
        stats.total_blocks += disk_blocks(&metadata);
    }
    Ok(())
}

#[cfg(unix)]
fn disk_blocks(metadata: &std::fs::Metadata) -> u64 {
    use std::os::unix::fs::MetadataExt;
    metadata.blocks()
}

#[cfg(not(unix))]
fn disk_blocks(metadata: &std::fs::Metadata) -> u64 {
    metadata.len().div_ceil(512)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::thread;

    /// A scratch directory under the OS temp dir, unique per test, removed
    /// when it goes out of scope.
    struct TempDir(PathBuf);

    impl TempDir {
        fn new(tag: &str) -> Self {
            static COUNTER: AtomicU64 = AtomicU64::new(0);
            let n = COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "kblockdblib-world-test-{tag}-{}-{n}",
                std::process::id()
            ));
            TempDir(path)
        }
    }

    impl AsRef<Path> for TempDir {
        fn as_ref(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    /// Creates a fresh 3-axis, `WORLD_DIM`-sized, `DEFAULT_CHUNK_DIM`-chunked
    /// world at `dir` -- the default shape every test below assumes.
    fn create(dir: &TempDir) -> World {
        World::create(dir, AXES, WORLD_DIM, DEFAULT_CHUNK_DIM).unwrap()
    }

    fn coord3(x: u32, y: u32, z: u32) -> Coord {
        Coord::from([x, y, z])
    }

    #[test]
    fn create_writes_world_txt_and_open_reads_it_back() {
        let dir = TempDir::new("create-basic");
        {
            let w = World::create(&dir, 3, 10_000, 32).unwrap();
            assert_eq!(w.axes(), 3);
            assert_eq!(w.world_dim(), 10_000);
            assert_eq!(w.chunk_dim(), 32);
        }

        let w = World::open(&dir).unwrap();
        assert_eq!(w.axes(), 3);
        assert_eq!(w.world_dim(), 10_000);
        assert_eq!(w.chunk_dim(), 32);
    }

    #[test]
    fn open_without_a_prior_create_is_not_found() {
        let dir = TempDir::new("open-no-create");
        let err = World::open(&dir).err().unwrap();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
    }

    #[test]
    fn create_is_idempotent_with_matching_params() {
        let dir = TempDir::new("create-idempotent");
        World::create(&dir, 3, 100, 32).unwrap();
        // Calling create again with the same params re-opens cleanly.
        let w = World::create(&dir, 3, 100, 32).unwrap();
        assert_eq!(w.axes(), 3);
        assert_eq!(w.world_dim(), 100);
    }

    #[test]
    fn create_with_mismatched_params_fails_without_changing_anything() {
        let dir = TempDir::new("create-mismatch");
        World::create(&dir, 3, 100, 32).unwrap();

        assert_eq!(
            World::create(&dir, 4, 100, 32).err().unwrap().kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(
            World::create(&dir, 3, 200, 32).err().unwrap().kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(
            World::create(&dir, 3, 100, 16).err().unwrap().kind(),
            io::ErrorKind::InvalidInput
        );

        // The original world is untouched and still openable with its
        // original shape.
        let w = World::open(&dir).unwrap();
        assert_eq!(w.axes(), 3);
        assert_eq!(w.world_dim(), 100);
        assert_eq!(w.chunk_dim(), 32);
    }

    #[test]
    fn create_rejects_zero_axes() {
        let dir = TempDir::new("create-zero-axes");
        assert_eq!(
            World::create(&dir, 0, 100, 32).err().unwrap().kind(),
            io::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn create_rejects_zero_chunk_dim() {
        let dir = TempDir::new("create-zero-chunk-dim");
        assert_eq!(
            World::create(&dir, 3, 100, 0).err().unwrap().kind(),
            io::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn a_world_can_have_a_different_chunk_dim() {
        let dir = TempDir::new("create-small-chunk-dim");
        let w = World::create(&dir, 3, 100, 4).unwrap();
        assert_eq!(w.chunk_dim(), 4);
        // 100 cells per axis, 4 cells/chunk -> 25 chunks per axis exactly.
        assert_eq!(w.chunks_per_axis(), 25);

        w.set(&[0, 0, 0], "material", Value::Str("stone".into()))
            .unwrap();
        // Cell 4 is the first cell of the *second* chunk on axis 0 (a
        // chunk_dim of 4, not the default 32) -- a distinct cell, not the
        // one just set.
        assert_eq!(w.get(&[4, 0, 0], "material").unwrap(), None);
        assert_eq!(
            w.get(&[0, 0, 0], "material").unwrap(),
            Some(Value::Str("stone".into()))
        );

        w.flush().unwrap();
        let reopened = World::open(&dir).unwrap();
        assert_eq!(reopened.chunk_dim(), 4);
        assert_eq!(
            reopened.get(&[0, 0, 0], "material").unwrap(),
            Some(Value::Str("stone".into()))
        );
    }

    #[test]
    fn a_world_can_have_a_different_axis_count() {
        let dir = TempDir::new("create-2d");
        let w = World::create(&dir, 2, 50, 32).unwrap();

        w.set(&[1, 2], "material", Value::Str("stone".into()))
            .unwrap();
        assert_eq!(
            w.get(&[1, 2], "material").unwrap(),
            Some(Value::Str("stone".into()))
        );

        // A 3-coordinate lookup against a 2-axis world is a caller error,
        // not a panic.
        assert_eq!(
            w.get(&[1, 2, 3], "material").unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn get_on_untouched_world_is_none() {
        let dir = TempDir::new("untouched");
        let w = create(&dir);

        // Key never interned anywhere in the world.
        assert_eq!(w.get(&coord3(0, 0, 0), "material").unwrap(), None);
        assert_eq!(w.schema_len(), 0);
    }

    #[test]
    fn get_validates_the_coordinate_even_for_a_never_interned_key() {
        // Regression: get() used to check schema membership before
        // validating the coordinate, so a bad coordinate against an
        // unknown key silently returned Ok(None) instead of erroring.
        let dir = TempDir::new("get-validates-oob-unknown-key");
        let w = create(&dir);

        assert_eq!(
            w.get(&coord3(WORLD_DIM, 0, 0), "never-set")
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(
            w.get(&[1, 2], "never-set").unwrap_err().kind(), // wrong axis count
            io::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn remove_validates_the_coordinate_even_for_a_never_interned_key() {
        // Regression: same short-circuit-before-validation bug as get(),
        // in remove()'s "unknown key is a harmless no-op" path.
        let dir = TempDir::new("remove-validates-oob-unknown-key");
        let w = create(&dir);

        assert_eq!(
            w.remove(&coord3(WORLD_DIM, 0, 0), "never-set")
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn set_does_not_intern_the_key_if_the_coordinate_is_invalid() {
        // Regression: set() used to intern the key before validating the
        // coordinate, so a failed set() still had the permanent side
        // effect of registering a key nothing was ever actually stored
        // under.
        let dir = TempDir::new("set-no-intern-on-bad-coord");
        let w = create(&dir);

        let err = w.set(&coord3(WORLD_DIM, 0, 0), "material", Value::I64(1));
        assert_eq!(err.unwrap_err().kind(), io::ErrorKind::InvalidInput);
        assert_eq!(w.schema_len(), 0, "key must not be interned on failure");
    }

    #[test]
    fn set_then_get_roundtrips_each_value_type() {
        let dir = TempDir::new("set-get-types");
        let w = create(&dir);
        let c = coord3(1, 2, 3);

        w.set(&c, "material", Value::Str("stone".into())).unwrap();
        w.set(&c, "density", Value::F64(2.5)).unwrap();
        w.set(&c, "hardness", Value::I64(7)).unwrap();

        assert_eq!(
            w.get(&c, "material").unwrap(),
            Some(Value::Str("stone".into()))
        );
        assert_eq!(w.get(&c, "density").unwrap(), Some(Value::F64(2.5)));
        assert_eq!(w.get(&c, "hardness").unwrap(), Some(Value::I64(7)));
    }

    #[test]
    fn set_overwrites_previous_value_at_same_cell_and_key() {
        let dir = TempDir::new("overwrite");
        let w = create(&dir);
        let c = coord3(9, 9, 9);

        w.set(&c, "material", Value::Str("stone".into())).unwrap();
        w.set(&c, "material", Value::Str("air".into())).unwrap();

        assert_eq!(
            w.get(&c, "material").unwrap(),
            Some(Value::Str("air".into()))
        );
    }

    #[test]
    fn set_rejects_a_different_type_for_an_already_used_key_anywhere_in_the_world() {
        let dir = TempDir::new("type-mismatch-set");
        let w = create(&dir);
        w.set(&coord3(0, 0, 0), "temperature", Value::F64(20.0))
            .unwrap();

        // Same key, a *different, far-away cell* (a different chunk
        // entirely) -- the type conflict must still be caught, since it's
        // recorded in the schema, not per-chunk.
        let err = w
            .set(&coord3(9_000, 9_000, 9_000), "temperature", Value::I64(20))
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(err.to_string().contains("temperature"));

        // The failed set didn't write anything, and the original value is
        // untouched.
        assert_eq!(
            w.get(&coord3(9_000, 9_000, 9_000), "temperature").unwrap(),
            None
        );
        assert_eq!(
            w.get(&coord3(0, 0, 0), "temperature").unwrap(),
            Some(Value::F64(20.0))
        );
    }

    #[test]
    fn set_on_one_cell_does_not_affect_neighbors_or_other_keys() {
        let dir = TempDir::new("isolation");
        let w = create(&dir);

        w.set(&coord3(0, 0, 0), "material", Value::Str("stone".into()))
            .unwrap();

        // Same key, different cell in the same chunk: untouched.
        assert_eq!(w.get(&coord3(1, 0, 0), "material").unwrap(), None);
        // Same cell, different key: untouched.
        assert_eq!(w.get(&coord3(0, 0, 0), "density").unwrap(), None);
    }

    #[test]
    fn set_across_multiple_chunks() {
        let dir = TempDir::new("multi-chunk");
        let w = create(&dir);

        // DEFAULT_CHUNK_DIM cells apart on axis 0 guarantees these land in
        // different chunks.
        let far = coord3(DEFAULT_CHUNK_DIM, 0, 0);
        w.set(&coord3(0, 0, 0), "material", Value::Str("stone".into()))
            .unwrap();
        w.set(&far, "material", Value::Str("air".into())).unwrap();

        assert_eq!(
            w.get(&coord3(0, 0, 0), "material").unwrap(),
            Some(Value::Str("stone".into()))
        );
        assert_eq!(
            w.get(&far, "material").unwrap(),
            Some(Value::Str("air".into()))
        );
    }

    #[test]
    fn interning_a_new_key_grows_the_schema() {
        let dir = TempDir::new("schema-growth");
        let w = create(&dir);
        assert_eq!(w.schema_len(), 0);

        w.set(&coord3(0, 0, 0), "material", Value::Str("stone".into()))
            .unwrap();
        assert_eq!(w.schema_len(), 1);

        // Reusing the same key does not add another schema entry.
        w.set(&coord3(1, 1, 1), "material", Value::Str("air".into()))
            .unwrap();
        assert_eq!(w.schema_len(), 1);

        w.set(&coord3(0, 0, 0), "density", Value::F64(1.0)).unwrap();
        assert_eq!(w.schema_len(), 2);
    }

    #[test]
    fn set_persists_across_flush_and_reopen() {
        let dir = TempDir::new("set-persist");
        let c = coord3(100, 200, 300);
        {
            let w = create(&dir);
            w.set(&c, "material", Value::Str("stone".into())).unwrap();
            w.set(&c, "density", Value::F64(2.5)).unwrap();
            w.flush().unwrap();
        }

        let w = World::open(&dir).unwrap();
        assert_eq!(
            w.get(&c, "material").unwrap(),
            Some(Value::Str("stone".into()))
        );
        assert_eq!(w.get(&c, "density").unwrap(), Some(Value::F64(2.5)));
        // The schema (key registry) is durable too.
        assert_eq!(w.schema_len(), 2);
    }

    #[test]
    fn remove_clears_cell_in_same_session() {
        let dir = TempDir::new("same-session");
        let w = create(&dir);
        let c = coord3(1, 2, 3);

        w.set(&c, "material", Value::Str("stone".into())).unwrap();
        assert_eq!(
            w.get(&c, "material").unwrap(),
            Some(Value::Str("stone".into()))
        );

        w.remove(&c, "material").unwrap();
        assert_eq!(w.get(&c, "material").unwrap(), None);
    }

    #[test]
    fn remove_of_unset_key_is_a_harmless_noop() {
        let dir = TempDir::new("noop");
        let w = create(&dir);
        let c = coord3(4, 5, 6);

        // Key was never interned anywhere in the world.
        w.remove(&c, "nonexistent").unwrap();
        assert_eq!(w.get(&c, "nonexistent").unwrap(), None);

        // Key exists in the schema, but not on this particular cell.
        w.set(&c, "material", Value::I64(1)).unwrap();
        w.remove(&coord3(7, 8, 9), "material").unwrap();
        assert_eq!(w.get(&c, "material").unwrap(), Some(Value::I64(1)));
    }

    #[test]
    fn remove_persists_across_flush_and_reopen() {
        let dir = TempDir::new("persist");
        let c = coord3(100, 200, 300);
        {
            let w = create(&dir);
            w.set(&c, "material", Value::Str("stone".into())).unwrap();
            w.remove(&c, "material").unwrap();
            w.flush().unwrap();
        }

        let w = World::open(&dir).unwrap();
        assert_eq!(w.get(&c, "material").unwrap(), None);
    }

    #[test]
    fn removing_every_cell_in_a_chunk_deletes_its_file() {
        let dir = TempDir::new("empty-chunk-gc");
        let c = coord3(0, 0, 0);
        let chunk_path;
        {
            let w = create(&dir);
            w.set(&c, "material", Value::Str("stone".into())).unwrap();
            w.flush().unwrap();
            let (ckey, _) = w.split(&c).unwrap();
            chunk_path = w.chunk_path(&ckey);
        }
        assert!(
            chunk_path.exists(),
            "chunk file should exist once populated"
        );

        let w = World::open(&dir).unwrap();
        w.remove(&c, "material").unwrap();
        w.flush().unwrap();
        assert!(
            !chunk_path.exists(),
            "emptied chunk file should be cleaned up"
        );
    }

    /// `n` copies of `value`, for tests that don't care about per-cell
    /// variation and just want to fill a region uniformly.
    fn fill(region: &Region, value: Value) -> Vec<Value> {
        vec![value; region.volume() as usize]
    }

    #[test]
    fn get_region_on_untouched_world_is_all_none() {
        let dir = TempDir::new("region-untouched");
        let w = create(&dir);

        let region = Region::new(coord3(5, 5, 5), coord3(3, 2, 2));
        let got = w.get_region(&region, "material").unwrap();
        assert_eq!(got.len(), region.volume() as usize);
        assert!(got.iter().all(Option::is_none));
    }

    #[test]
    fn set_region_then_get_region_roundtrips_within_one_chunk() {
        let dir = TempDir::new("region-roundtrip");
        let w = create(&dir);

        let region = Region::new(coord3(1, 1, 1), coord3(4, 3, 2));
        w.set_region(
            &region,
            "material",
            &fill(&region, Value::Str("stone".into())),
        )
        .unwrap();

        let got = w.get_region(&region, "material").unwrap();
        assert_eq!(got.len(), region.volume() as usize);
        assert!(got.iter().all(|v| *v == Some(Value::Str("stone".into()))));
    }

    #[test]
    fn set_region_writes_distinct_per_cell_values() {
        let dir = TempDir::new("region-per-cell");
        let w = create(&dir);

        let region = Region::new(coord3(10, 20, 30), coord3(3, 2, 2));
        let values: Vec<Value> = (0..region.volume())
            .map(|i| Value::I64(i as i64 * 7))
            .collect();
        w.set_region(&region, "n", &values).unwrap();

        let got = w.get_region(&region, "n").unwrap();
        for (i, v) in values.iter().enumerate() {
            assert_eq!(got[i], Some(v.clone()));
        }
    }

    #[test]
    fn set_region_rejects_a_mismatched_value_count() {
        let dir = TempDir::new("region-bad-count");
        let w = create(&dir);

        let region = Region::new(coord3(0, 0, 0), coord3(2, 2, 2));
        let too_few = vec![Value::I64(0); region.volume() as usize - 1];
        let err = w.set_region(&region, "n", &too_few).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);

        // The rejected call must not have written anything.
        let got = w.get_region(&region, "n").unwrap();
        assert!(got.iter().all(Option::is_none));
    }

    #[test]
    fn set_region_rejects_mixed_value_types_in_one_call() {
        let dir = TempDir::new("region-mixed-types");
        let w = create(&dir);

        let region = Region::new(coord3(0, 0, 0), coord3(2, 2, 2)); // 8 cells
        let mut values = vec![Value::I64(0); 8];
        values[5] = Value::Str("oops".into());
        let err = w.set_region(&region, "n", &values).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);

        // All-or-nothing: not even the 5 consistent values before the odd
        // one out got written.
        let got = w.get_region(&region, "n").unwrap();
        assert!(got.iter().all(Option::is_none));
    }

    #[test]
    fn set_region_rejects_a_type_that_does_not_match_the_keys_existing_type() {
        let dir = TempDir::new("region-type-mismatch");
        let w = create(&dir);
        w.set(&coord3(0, 0, 0), "n", Value::I64(1)).unwrap();

        let region = Region::new(coord3(0, 0, 0), coord3(2, 2, 2));
        let values = vec![Value::F64(1.0); region.volume() as usize];
        let err = w.set_region(&region, "n", &values).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);

        // The pre-existing cell is untouched, and the rest of the region
        // still reads as unset.
        assert_eq!(w.get(&coord3(0, 0, 0), "n").unwrap(), Some(Value::I64(1)));
    }

    #[test]
    fn set_region_does_not_leak_outside_its_bounds() {
        let dir = TempDir::new("region-bounds");
        let w = create(&dir);

        let region = Region::new(coord3(10, 10, 10), coord3(2, 2, 2));
        w.set_region(
            &region,
            "material",
            &fill(&region, Value::Str("stone".into())),
        )
        .unwrap();

        // One past the region on each axis: untouched.
        assert_eq!(w.get(&coord3(12, 10, 10), "material").unwrap(), None);
        assert_eq!(w.get(&coord3(10, 12, 10), "material").unwrap(), None);
        assert_eq!(w.get(&coord3(10, 10, 12), "material").unwrap(), None);
        // Just inside on each axis: set.
        assert_eq!(
            w.get(&coord3(11, 10, 10), "material").unwrap(),
            Some(Value::Str("stone".into()))
        );
    }

    #[test]
    fn get_region_orders_results_axis0_fastest() {
        let dir = TempDir::new("region-order");
        let w = create(&dir);

        let region = Region::new(coord3(10, 20, 30), coord3(3, 2, 2));

        // Give every cell in the region a distinct value derived from its
        // offset, then check RegionIter's enumeration order matches
        // get_region's result order exactly.
        for (i, coord) in RegionIter::new(&region).enumerate() {
            w.set(&coord, "n", Value::I64(i as i64)).unwrap();
        }

        let got = w.get_region(&region, "n").unwrap();
        for (i, coord) in RegionIter::new(&region).enumerate() {
            assert_eq!(got[i], Some(Value::I64(i as i64)), "mismatch at {coord:?}");
        }
    }

    #[test]
    fn region_spans_chunks_including_partial_chunks() {
        let dir = TempDir::new("region-span-chunks");
        let w = create(&dir);

        // Starts 5 cells before a chunk boundary and ends 5 cells past the
        // next one, on every axis: covers the tail of one chunk, all of a
        // second, and the head of a third, on each axis.
        let x0 = DEFAULT_CHUNK_DIM - 5;
        let d = DEFAULT_CHUNK_DIM + 10;
        let region = Region::new(vec![x0; 3], vec![d; 3]);

        w.set_region(
            &region,
            "material",
            &fill(&region, Value::Str("stone".into())),
        )
        .unwrap();
        w.flush().unwrap();

        // Reopen as a brand new `World` handle (nothing carries over
        // in-process anymore -- every read already goes straight to disk,
        // see `with_chunk`) to double-check a fresh handle sees exactly
        // what was written, the same as a second process attaching would.
        let w = World::open(&dir).unwrap();
        let got = w.get_region(&region, "material").unwrap();
        assert_eq!(got.len(), region.volume() as usize);
        assert!(got.iter().all(|v| *v == Some(Value::Str("stone".into()))));

        // Just outside the region on the low and high corners: untouched.
        assert_eq!(w.get(&[x0 - 1; 3], "material").unwrap(), None);
        assert_eq!(w.get(&[x0 + d; 3], "material").unwrap(), None);
    }

    #[test]
    fn remove_region_clears_key_across_chunks_without_touching_others() {
        let dir = TempDir::new("region-remove");
        let w = create(&dir);

        let x0 = DEFAULT_CHUNK_DIM - 2;
        let region = Region::new(coord3(x0, 0, 0), coord3(5, 5, 5));
        w.set_region(
            &region,
            "material",
            &fill(&region, Value::Str("stone".into())),
        )
        .unwrap();
        w.set_region(&region, "density", &fill(&region, Value::F64(2.6)))
            .unwrap();

        w.remove_region(&region, "material").unwrap();

        let material = w.get_region(&region, "material").unwrap();
        assert!(material.iter().all(Option::is_none));
        // A different key on the same cells is untouched by the removal.
        let density = w.get_region(&region, "density").unwrap();
        assert!(density.iter().all(|v| *v == Some(Value::F64(2.6))));
    }

    #[test]
    fn remove_region_of_unknown_key_is_a_harmless_noop() {
        let dir = TempDir::new("region-remove-unknown");
        let w = create(&dir);
        // Should not error even though "material" has never been interned.
        w.remove_region(&Region::new(coord3(0, 0, 0), coord3(4, 4, 4)), "material")
            .unwrap();
    }

    #[test]
    fn zero_sized_region_is_a_harmless_noop() {
        let dir = TempDir::new("region-zero");
        let w = create(&dir);

        let zero_extent = coord3(0, 5, 5); // zero on any single axis empties the whole region

        assert_eq!(
            w.get_region(
                &Region::new(coord3(0, 0, 0), zero_extent.clone()),
                "material"
            )
            .unwrap(),
            vec![]
        );
        w.set_region(
            &Region::new(coord3(0, 0, 0), zero_extent.clone()),
            "material",
            &[],
        )
        .unwrap();
        // A no-op set_region shouldn't even intern the key.
        assert_eq!(w.schema_len(), 0);
        w.remove_region(&Region::new(coord3(0, 0, 0), zero_extent), "material")
            .unwrap();
    }

    #[test]
    fn region_out_of_world_bounds_is_an_error() {
        let dir = TempDir::new("region-oob");
        let w = create(&dir);

        let region = Region::new(coord3(WORLD_DIM - 1, 0, 0), coord3(2, 1, 1));
        assert_eq!(
            w.get_region(&region, "material").unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );

        // Overflowing u32 entirely must not panic or wrap around.
        let region = Region::new(coord3(u32::MAX - 1, 0, 0), coord3(5, 1, 1));
        assert_eq!(
            w.set_region(
                &region,
                "material",
                &vec![Value::I64(0); region.volume() as usize],
            )
            .unwrap_err()
            .kind(),
            io::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn region_with_wrong_axis_count_is_an_error() {
        let dir = TempDir::new("region-wrong-axes");
        let w = create(&dir); // 3-axis world

        let region = Region::new(vec![0, 0], vec![2, 2]); // 2 axes
        assert_eq!(
            w.get_region(&region, "material").unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn region_with_mismatched_origin_and_extent_axes_is_an_error() {
        // Regression: Region::new doesn't (and, taking origin/extent from
        // untrusted input like an HTTP request, can't) itself guarantee
        // origin.len() == extent.len() -- check_region must catch this
        // rather than leaving it to panic or index out of bounds later.
        let dir = TempDir::new("region-mismatched-origin-extent");
        let w = create(&dir); // 3-axis world

        let region = Region::new(vec![0, 0, 0], vec![2, 2]); // 3 origin axes, 2 extent axes
        assert_eq!(
            w.get_region(&region, "material").unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn set_region_persists_across_flush_and_reopen() {
        let dir = TempDir::new("region-persist");
        let region = Region::new(coord3(100, 100, 100), coord3(3, 3, 3));
        {
            let w = create(&dir);
            w.set_region(
                &region,
                "material",
                &fill(&region, Value::Str("stone".into())),
            )
            .unwrap();
            w.flush().unwrap();
        }

        let w = World::open(&dir).unwrap();
        let got = w.get_region(&region, "material").unwrap();
        assert!(got.iter().all(|v| *v == Some(Value::Str("stone".into()))));
    }

    // --- Concurrency: many threads sharing one `World` ---
    //
    // kBlockDB now assumes exactly one `World` is ever open against a given
    // directory at a time (see `World`'s "Concurrency" doc comment), so
    // these share one `World` behind an `Arc` across every thread -- the
    // actually-supported shape -- rather than opening a separate `World`
    // handle per thread the way an older, cross-process-safe design's
    // tests did.

    #[test]
    fn concurrent_writers_to_sibling_keys_on_the_same_cell_do_not_lose_updates() {
        let dir = TempDir::new("concurrent-siblings");
        let w = std::sync::Arc::new(create(&dir));
        let c = coord3(1, 1, 1);
        let n = 16;

        let handles: Vec<_> = (0..n)
            .map(|i| {
                let w = std::sync::Arc::clone(&w);
                let c = c.clone();
                thread::spawn(move || {
                    w.set(&c, &format!("key{i}"), Value::I64(i)).unwrap();
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }

        for i in 0..n {
            assert_eq!(
                w.get(&c, &format!("key{i}")).unwrap(),
                Some(Value::I64(i)),
                "key{i} lost -- a concurrent writer's chunk rewrite clobbered it"
            );
        }
    }

    #[test]
    fn concurrent_interning_of_different_new_keys_does_not_collide() {
        // Regression: Schema::intern used to compute a new key's id from
        // its own in-memory count without any synchronization, so two
        // threads interning two different new keys at once could both
        // assign the same id -- corrupting schema.txt (a non-dense/
        // out-of-order id sequence) badly enough that even *reopening* the
        // world later would panic. `World`'s `Mutex<Schema>` is what
        // prevents that now (see `Schema`'s "Concurrency" doc comment).
        let dir = TempDir::new("concurrent-intern");
        let w = std::sync::Arc::new(create(&dir));
        let n = 16;

        let handles: Vec<_> = (0..n)
            .map(|i| {
                let w = std::sync::Arc::clone(&w);
                thread::spawn(move || {
                    w.set(&coord3(0, 0, 0), &format!("key{i}"), Value::I64(i))
                        .unwrap();
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }

        // Every key must have made it in with its own distinct id -- if
        // two keys had collided on one id, at least one of these reads
        // would come back wrong (a different key's value, or a type
        // mismatch panic in Chunk::set from two different value types
        // sharing a column). Reopening must also not panic (the
        // dense/in-order id invariant must still hold on disk).
        assert_eq!(w.schema_len(), n as usize);
        for i in 0..n {
            assert_eq!(
                w.get(&coord3(0, 0, 0), &format!("key{i}")).unwrap(),
                Some(Value::I64(i))
            );
        }
        let reopened = World::open(&dir).unwrap();
        assert_eq!(reopened.schema_len(), n as usize);
    }

    #[test]
    fn concurrent_set_region_on_overlapping_regions_does_not_corrupt_a_chunk() {
        let dir = TempDir::new("concurrent-overlapping-regions");
        let w = std::sync::Arc::new(create(&dir));

        // Same box, different keys: both threads' writes land fully in the
        // same set of chunks, at the same time, under different keys --
        // this is what would surface a chunk-file torn write/lost update
        // if with_chunk's lock+read+write weren't actually atomic.
        let region = Region::new(coord3(0, 0, 0), coord3(10, 10, 10));

        let handles: Vec<_> = ["a", "b"]
            .into_iter()
            .map(|key| {
                let w = std::sync::Arc::clone(&w);
                let region = region.clone();
                let values = fill(&region, Value::Str(key.into()));
                thread::spawn(move || {
                    w.set_region(&region, key, &values).unwrap();
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }

        let a = w.get_region(&region, "a").unwrap();
        let b = w.get_region(&region, "b").unwrap();
        assert!(a.iter().all(|v| *v == Some(Value::Str("a".into()))));
        assert!(b.iter().all(|v| *v == Some(Value::Str("b".into()))));
    }

    #[test]
    fn one_world_is_usable_concurrently_via_arc_with_no_external_lock() {
        // The point of &self (not &mut self) throughout: a single process
        // can share one `World` behind a plain `Arc` -- no
        // `Mutex<World>` wrapper needed -- and let concurrent calls into
        // it actually run concurrently, limited only by the per-chunk file
        // locks `with_chunk` already takes. This wouldn't even compile if
        // any method still required `&mut self`.
        let dir = TempDir::new("arc-shared-concurrent");
        let w = std::sync::Arc::new(create(&dir));
        let n = 32;

        let handles: Vec<_> = (0..n)
            .map(|i| {
                let w = std::sync::Arc::clone(&w);
                thread::spawn(move || {
                    // Disjoint cells -- this exercises real concurrent
                    // execution (different chunks, no lock contention
                    // between threads), not just concurrent *calls* that
                    // happen to serialize.
                    let c = coord3(i, i, i);
                    w.set(&c, "bench", Value::I64(i as i64)).unwrap();
                    w.get(&c, "bench").unwrap()
                })
            })
            .collect();

        for (i, h) in handles.into_iter().enumerate() {
            assert_eq!(h.join().unwrap(), Some(Value::I64(i as i64)));
        }

        // And everything actually landed, from any handle.
        for i in 0..n {
            assert_eq!(
                w.get(&coord3(i, i, i), "bench").unwrap(),
                Some(Value::I64(i as i64))
            );
        }
    }

    // --- Write-through chunk cache ---

    #[test]
    fn repeated_gets_on_the_same_chunk_only_read_from_disk_once() {
        let dir = TempDir::new("cache-repeated-get");
        let w = create(&dir);
        let c = coord3(1, 1, 1);
        w.set(&c, "material", Value::Str("stone".into())).unwrap();
        assert_eq!(
            w.chunks_read_from_disk(),
            0,
            "set() populates the cache directly, it shouldn't need to read the chunk back"
        );

        for _ in 0..5 {
            assert_eq!(
                w.get(&c, "material").unwrap(),
                Some(Value::Str("stone".into()))
            );
        }
        assert_eq!(
            w.chunks_read_from_disk(),
            0,
            "every get() after the set() above should hit the in-memory cache, not disk"
        );
    }

    #[test]
    fn a_fresh_world_handle_reads_a_pre_existing_chunk_from_disk_exactly_once() {
        let dir = TempDir::new("cache-cold-start");
        let c = coord3(2, 2, 2);
        {
            let w = create(&dir);
            w.set(&c, "material", Value::Str("stone".into())).unwrap();
        }

        // A brand new `World` handle -- its cache starts empty, so the
        // first touch of this chunk must go to disk once, and every touch
        // after that must not.
        let w = World::open(&dir).unwrap();
        assert_eq!(w.chunks_read_from_disk(), 0);
        assert_eq!(
            w.get(&c, "material").unwrap(),
            Some(Value::Str("stone".into()))
        );
        assert_eq!(w.chunks_read_from_disk(), 1);
        for _ in 0..5 {
            w.get(&c, "material").unwrap();
        }
        assert_eq!(
            w.chunks_read_from_disk(),
            1,
            "only the first get() on a freshly opened World should read this chunk from disk"
        );
    }

    #[test]
    fn repeated_sets_on_the_same_chunk_still_write_to_disk_every_time() {
        // The cache is write-through, not write-behind: caching removes
        // redundant reads, never defers or coalesces a write, so
        // durability is unchanged -- see World's "Concurrency" doc
        // comment.
        let dir = TempDir::new("cache-write-through");
        let w = create(&dir);
        let c = coord3(3, 3, 3);

        for i in 0..5 {
            w.set(&c, "n", Value::I64(i)).unwrap();
        }
        assert_eq!(w.chunks_written_to_disk(), 5);

        // And each write really did land on disk immediately, not just in
        // the cache: a completely independent handle over the same
        // directory sees the last value without this `w` doing anything
        // else.
        let reopened = World::open(&dir).unwrap();
        assert_eq!(reopened.get(&c, "n").unwrap(), Some(Value::I64(4)));
    }

    #[test]
    fn repeated_gets_on_a_never_written_chunk_stay_correct_and_touch_no_file() {
        // A chunk that's never been written has no file to read at all --
        // `chunks_read_from_disk` (which only counts an actual file read)
        // must stay 0 here regardless of how many times it's queried,
        // unlike the pre-existing-chunk case above.
        let dir = TempDir::new("cache-never-written");
        let w = create(&dir);
        let c = coord3(9_999, 0, 0); // a chunk nothing has ever touched

        for _ in 0..5 {
            assert_eq!(w.get(&c, "material").unwrap(), None);
        }
        assert_eq!(
            w.chunks_read_from_disk(),
            0,
            "there was never a file to read for this chunk"
        );
    }

    #[test]
    fn default_max_cached_chunks_is_the_documented_constant() {
        let dir = TempDir::new("cache-cap-default");
        let w = create(&dir);
        assert_eq!(w.max_cached_chunks(), DEFAULT_MAX_CACHED_CHUNKS);
    }

    #[test]
    fn with_max_cached_chunks_overrides_the_default() {
        let dir = TempDir::new("cache-cap-override");
        let w = World::create(&dir, AXES, WORLD_DIM, DEFAULT_CHUNK_DIM)
            .unwrap()
            .with_max_cached_chunks(3);
        assert_eq!(w.max_cached_chunks(), 3);
    }

    #[test]
    fn evicting_a_chunk_from_the_cache_makes_a_later_touch_re_read_it_from_disk() {
        // A tiny cache (cap 2) forces eviction almost immediately: touching
        // a 3rd, 4th, and 5th distinct chunk each evict the
        // least-recently-touched one so far. Re-touching an evicted chunk
        // must still read the right data back -- correctness, not just
        // that *a* read happens -- and `chunks_read_from_disk` proves it
        // really did go back to disk rather than getting a lucky
        // in-memory hit.
        let dir = TempDir::new("cache-eviction-reread");
        let w = World::create(&dir, AXES, WORLD_DIM, DEFAULT_CHUNK_DIM)
            .unwrap()
            .with_max_cached_chunks(2);

        // Each of these is DEFAULT_CHUNK_DIM apart on axis 0, so each lands in a
        // distinct chunk.
        let chunk_coord = |n: u32| coord3(n * DEFAULT_CHUNK_DIM, 0, 0);

        w.set(&chunk_coord(0), "material", Value::I64(0)).unwrap(); // cache: {0}
        w.set(&chunk_coord(1), "material", Value::I64(1)).unwrap(); // cache: {0, 1} -- full
                                                                    // Touching a 3rd distinct chunk evicts the least-recently-touched
                                                                    // one so far (chunk 0) to make room.
        w.set(&chunk_coord(2), "material", Value::I64(2)).unwrap(); // cache: {1, 2}
        assert_eq!(w.chunks_read_from_disk(), 0); // every set() above populated its own cache slot directly

        // Re-reading evicted chunk 0 must go back to disk, and still
        // return the value that was durably written to it earlier.
        assert_eq!(
            w.get(&chunk_coord(0), "material").unwrap(),
            Some(Value::I64(0))
        );
        assert_eq!(w.chunks_read_from_disk(), 1);
    }

    #[test]
    fn default_max_concurrent_disk_ops_is_the_documented_constant() {
        let dir = TempDir::new("cap-default");
        let w = create(&dir);
        assert_eq!(w.max_concurrent_disk_ops(), DEFAULT_MAX_CONCURRENT_DISK_OPS);
    }

    #[test]
    fn with_max_concurrent_disk_ops_overrides_the_default() {
        let dir = TempDir::new("cap-override");
        let w = World::create(&dir, AXES, WORLD_DIM, DEFAULT_CHUNK_DIM)
            .unwrap()
            .with_max_concurrent_disk_ops(3);
        assert_eq!(w.max_concurrent_disk_ops(), 3);
    }

    #[test]
    fn zero_max_concurrent_disk_ops_is_treated_as_one_not_a_deadlock() {
        let dir = TempDir::new("cap-zero");
        let w = World::create(&dir, AXES, WORLD_DIM, DEFAULT_CHUNK_DIM)
            .unwrap()
            .with_max_concurrent_disk_ops(0);
        assert_eq!(w.max_concurrent_disk_ops(), 1);
        // And it must still actually work, not just report "1".
        w.set(&coord3(0, 0, 0), "material", Value::I64(1)).unwrap();
        assert_eq!(
            w.get(&coord3(0, 0, 0), "material").unwrap(),
            Some(Value::I64(1))
        );
    }

    #[test]
    fn a_tight_cap_does_not_break_correctness_under_real_concurrency() {
        // Reruns the same shape as
        // concurrent_writers_to_sibling_keys_on_the_same_cell_do_not_lose_updates,
        // but with the cap forced down to 1 -- i.e. with_chunk calls fully
        // serialized -- to make sure the semaphore is a pure scheduling
        // change, not a correctness one.
        let dir = TempDir::new("cap-one-correctness");
        let w = std::sync::Arc::new(
            World::create(&dir, AXES, WORLD_DIM, DEFAULT_CHUNK_DIM)
                .unwrap()
                .with_max_concurrent_disk_ops(1),
        );
        let c = coord3(1, 1, 1);
        let n = 16;

        let handles: Vec<_> = (0..n)
            .map(|i| {
                let w = std::sync::Arc::clone(&w);
                let c = c.clone();
                thread::spawn(move || {
                    w.set(&c, &format!("key{i}"), Value::I64(i)).unwrap();
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }

        for i in 0..n {
            assert_eq!(w.get(&c, &format!("key{i}")).unwrap(), Some(Value::I64(i)));
        }
    }

    #[test]
    fn max_concurrent_disk_ops_actually_limits_concurrency() {
        // A tight cap forces what would otherwise be concurrent,
        // disjoint-chunk writes to run effectively serially; a generous
        // cap lets them overlap freely. If with_max_concurrent_disk_ops
        // didn't actually gate with_chunk, these two would take about the
        // same wall-clock time -- they shouldn't.
        //
        // Each of `workers` threads does `ops_per_worker` real,
        // disjoint-chunk writes (not just `workers` total): a handful of
        // threads doing one op each makes this a wall-clock comparison
        // between real (small, sub-millisecond-scale) filesystem work and
        // one-time OS-thread-spawn overhead, which is noisy enough to make
        // the test flaky (confirmed while writing it). Enough aggregate
        // work per worker makes the real per-op I/O time dominate that
        // fixed overhead instead.
        fn time_writes(
            w: &std::sync::Arc<World>,
            workers: u32,
            ops_per_worker: u32,
        ) -> std::time::Duration {
            let t0 = std::time::Instant::now();
            let handles: Vec<_> = (0..workers)
                .map(|worker| {
                    let w = std::sync::Arc::clone(w);
                    thread::spawn(move || {
                        for op in 0..ops_per_worker {
                            let seed = worker * ops_per_worker + op;
                            w.set(&coord3(seed, seed, seed), "bench", Value::I64(seed as i64))
                                .unwrap();
                        }
                    })
                })
                .collect();
            for h in handles {
                h.join().unwrap();
            }
            t0.elapsed()
        }

        let workers = 8;
        let ops_per_worker = 30;

        let dir_serial = TempDir::new("cap-timing-serial");
        let w_serial = std::sync::Arc::new(
            World::create(&dir_serial, AXES, WORLD_DIM, DEFAULT_CHUNK_DIM)
                .unwrap()
                .with_max_concurrent_disk_ops(1),
        );
        let serial = time_writes(&w_serial, workers, ops_per_worker);

        let dir_parallel = TempDir::new("cap-timing-parallel");
        let w_parallel = std::sync::Arc::new(
            World::create(&dir_parallel, AXES, WORLD_DIM, DEFAULT_CHUNK_DIM)
                .unwrap()
                .with_max_concurrent_disk_ops(workers as usize),
        );
        let parallel = time_writes(&w_parallel, workers, ops_per_worker);

        assert!(
            serial > parallel,
            "serial (cap=1, {serial:?}) should be slower than parallel (cap={workers}, \
             {parallel:?}) -- the cap doesn't seem to be limiting anything"
        );
    }

    #[test]
    fn stats_of_an_untouched_world_are_all_zero() {
        let dir = TempDir::new("stats-empty");
        let w = create(&dir);
        assert_eq!(w.stats().unwrap(), Stats::default());
    }

    #[test]
    fn stats_count_one_chunk_per_touched_chunk_not_per_cell() {
        let dir = TempDir::new("stats-one-chunk");
        let w = create(&dir);
        // All within DEFAULT_CHUNK_DIM (32) of each other, and of (0,0,0) -- same
        // one chunk, whether one cell or many are set in it.
        w.set(&coord3(0, 0, 0), "k", Value::I64(1)).unwrap();
        w.set(&coord3(1, 2, 3), "k", Value::I64(2)).unwrap();
        w.set(&coord3(0, 0, 0), "other", Value::I64(3)).unwrap();

        let stats = w.stats().unwrap();
        assert_eq!(stats.total_chunks, 1);
        assert!(stats.total_bytes > 0);
        assert!(stats.total_blocks > 0);
    }

    #[test]
    fn stats_count_distinct_chunks_across_far_apart_cells() {
        let dir = TempDir::new("stats-many-chunks");
        let w = create(&dir);
        // Each at least DEFAULT_CHUNK_DIM (32) apart on every axis -- three
        // distinct chunks.
        w.set(&coord3(0, 0, 0), "k", Value::I64(1)).unwrap();
        w.set(&coord3(100, 0, 0), "k", Value::I64(2)).unwrap();
        w.set(&coord3(0, 100, 0), "k", Value::I64(3)).unwrap();

        assert_eq!(w.stats().unwrap().total_chunks, 3);
    }

    #[test]
    fn stats_reflect_a_chunk_emptied_back_out() {
        let dir = TempDir::new("stats-emptied");
        let w = create(&dir);
        w.set(&coord3(0, 0, 0), "k", Value::I64(1)).unwrap();
        assert_eq!(w.stats().unwrap().total_chunks, 1);

        w.remove(&coord3(0, 0, 0), "k").unwrap();
        assert_eq!(w.stats().unwrap(), Stats::default());
    }

    #[test]
    fn stats_ignore_world_txt_and_schema_txt() {
        let dir = TempDir::new("stats-ignores-metadata-files");
        let w = create(&dir);
        // world.txt and schema.txt both exist directly under root by now
        // (schema.txt from interning "k" below) -- neither should be
        // mistaken for a chunk file.
        w.set(&coord3(0, 0, 0), "k", Value::I64(1)).unwrap();
        assert_eq!(w.stats().unwrap().total_chunks, 1);
    }
}
