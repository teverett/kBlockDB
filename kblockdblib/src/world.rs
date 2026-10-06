use crate::chunk::{self, CellMeta, ChangeKind, Chunk};
use crate::chunk_cache::ChunkCache;
pub use crate::coord::Coord;
use crate::index::ValueIndex;
use crate::params::WorldParams;
use crate::schema::{ColumnInfo, Schema};
use crate::semaphore::Semaphore;
use crate::stamp::{Stamp, VersionVector};
use crate::value::{Value, ValueType};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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

/// zstd compression level used for chunk files when compression is on
/// (see `World::with_compression`). 3 is zstd's own default: the level
/// the format is tuned around, and the one that keeps compression cheap
/// enough to sit on the write-through path of every `set`/`remove`.
const ZSTD_LEVEL: i32 = 3;

/// The four bytes every zstd frame starts with. Chunk files are sniffed
/// for this on read so a world can be read back whether or not it was
/// written with compression on -- see `World::with_compression`. The
/// uncompressed chunk format can't collide with it: its first four bytes
/// are a little-endian column count, and `0xFD2FB528` columns is far more
/// than the `u32` key-id space could ever hold.
const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];

const _: () = assert!(AXES >= 1, "AXES must be at least 1");
const _: () = assert!(
    DEFAULT_CHUNK_DIM >= 1,
    "DEFAULT_CHUNK_DIM must be at least 1"
);

type ChunkKey = Coord;

/// Name of the file at a world's root persisting its current content
/// digest (see `World::with_content_digest`) -- 8 little-endian bytes,
/// rewritten in full on every change (not appended -- a single `u64` has
/// no history worth keeping).
const CONTENT_DIGEST_FILE: &str = "content_digest.bin";

/// A deterministic hash of one cell/key's full content -- its coordinate,
/// key name, value, and metadata -- used as `World`'s content digest's
/// per-entry contribution (see `ContentDigest`). Built from
/// `DefaultHasher` directly, not `RandomState`/`HashMap`'s default: that
/// one reseeds randomly per process, which would make two different
/// server processes compute different hashes for identical content --
/// useless for comparing one node's data against another's, the entire
/// point here. `DefaultHasher::new()` uses fixed keys, so the same
/// content always hashes the same way, on any node, any run.
fn entry_digest(coord: &[i32], key: &str, value: &Value, meta: &CellMeta) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut h = DefaultHasher::new();
    coord.hash(&mut h);
    key.hash(&mut h);
    match value {
        Value::Str(s) => {
            0u8.hash(&mut h);
            s.hash(&mut h);
        }
        Value::F64(f) => {
            1u8.hash(&mut h);
            f.to_bits().hash(&mut h);
        }
        Value::I64(n) => {
            2u8.hash(&mut h);
            n.hash(&mut h);
        }
        Value::Bool(b) => {
            3u8.hash(&mut h);
            b.hash(&mut h);
        }
    }
    meta.created_at_ms.hash(&mut h);
    meta.modified_at_ms.hash(&mut h);
    meta.version.hash(&mut h);
    h.finish()
}

/// A world's running content digest: the XOR of `entry_digest` over every
/// currently-live cell/key -- order-independent (XOR doesn't care what
/// order its operands arrive in), so it can be maintained incrementally,
/// one write at a time, without ever needing every cell in memory or on
/// disk at once the way recomputing it from a full scan would. Two worlds
/// with the same digest almost certainly hold the same data; this is a
/// checksum for catching accidental divergence (a missed replicated
/// write, a bug, an out-of-band edit), not a cryptographic proof -- a
/// coincidental XOR cancellation is possible in principle, vanishingly
/// unlikely in practice with a 64-bit hash.
///
/// Persisted to `CONTENT_DIGEST_FILE` on every change -- cheap relative to
/// the chunk file rewrite that already happens on every single-cell write
/// (see `World`'s "Concurrency" doc comment), so there's no reason to
/// defer it or batch it.
struct ContentDigest {
    value: Mutex<u64>,
    path: PathBuf,
}

impl ContentDigest {
    /// Loads a persisted digest from `path`, or (if nothing's there yet)
    /// computes one from scratch via `backfill` and persists that --
    /// `World::with_content_digest`'s one-time cost for a world that
    /// already has data when the digest is first turned on, same spirit
    /// as `create_index`'s backfill.
    fn open(path: PathBuf, backfill: impl FnOnce() -> io::Result<u64>) -> io::Result<ContentDigest> {
        let value = if path.exists() {
            let bytes = fs::read(&path)?;
            let bytes: [u8; 8] = bytes.try_into().map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("corrupt {CONTENT_DIGEST_FILE}: expected 8 bytes"),
                )
            })?;
            u64::from_le_bytes(bytes)
        } else {
            let value = backfill()?;
            fs::write(&path, value.to_le_bytes())?;
            value
        };
        Ok(ContentDigest {
            value: Mutex::new(value),
            path,
        })
    }

    fn get(&self) -> u64 {
        *self.value.lock().unwrap()
    }

    /// Folds in one entry's change: XORs out `old`'s contribution (if it
    /// had one) and XORs in `new`'s (if it has one), persisting the
    /// result.
    fn update(&self, old: Option<u64>, new: Option<u64>) -> io::Result<()> {
        let mut value = self.value.lock().unwrap();
        if let Some(h) = old {
            *value ^= h;
        }
        if let Some(h) = new {
            *value ^= h;
        }
        fs::write(&self.path, value.to_le_bytes())
    }
}

/// Milliseconds since the Unix epoch, for stamping `CellMeta` on every
/// `set` -- the clock is read here, once per `set`/`set_region` call, and
/// passed down to `Chunk::set` as a plain argument rather than read there,
/// so `Chunk`'s own logic stays a deterministic, easily testable function
/// of its arguments (see `chunk.rs`'s tests). Falls back to 0 if the system
/// clock is set before the epoch -- absurd, but not a reason to panic a
/// storage engine.
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Fills `buf` from `r`, stopping early only at end of input -- unlike
/// `read_exact`, a short file isn't an error, and unlike a single `read`,
/// a short read isn't mistaken for one. Used to sniff a chunk file's
/// leading bytes for `ZSTD_MAGIC`.
fn read_up_to<R: Read>(r: &mut R, buf: &mut [u8]) -> io::Result<usize> {
    let mut filled = 0;
    while filled < buf.len() {
        match r.read(&mut buf[filled..]) {
            Ok(0) => break,
            Ok(n) => filled += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(filled)
}

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

    /// A negative extent component (meaningless as a size) contributes 0
    /// rather than panicking or wrapping -- same "don't validate in a plain
    /// data holder" stance as `Region::new`'s doc comment; `World::check_region`
    /// is what actually rejects a negative extent as an error.
    pub fn volume(&self) -> u64 {
        self.extent.iter().map(|&d| d.max(0) as u64).product()
    }

    fn axes(&self) -> usize {
        self.origin.len()
    }

    /// Every coordinate inside this region, axis 0 fastest and the last
    /// axis slowest -- same order `get_region`'s result and `set_region`'s
    /// expected `values` use (see `RegionIter`). An empty (any zero-or-
    /// negative-extent axis) region yields nothing. Exposed so callers that
    /// need to enumerate a region's coordinates one at a time (rather than
    /// go through `get_region`/`set_region`/`remove_region`) -- e.g.
    /// `kblockdbserver`'s query language, upserting a `WHERE`-filtered
    /// subset of a range -- don't have to reimplement this odometer logic
    /// themselves.
    pub fn iter(&self) -> impl Iterator<Item = Coord> {
        RegionIter::new(self)
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
///   root/indexes/<key_id>/          -- one secondary index's on-disk LSM
///                                       state per indexed key (see
///                                       `create_index`, `crate::index`,
///                                       `crate::lsm`); which keys are
///                                       indexed is exactly which
///                                       subdirectories exist here, not a
///                                       separate manifest
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

/// One populated cell (a coordinate with at least one key set), as
/// returned by `World::list_cells` -- every key set there, alongside its
/// value and `CellMeta`. `values` is sorted ascending by key name.
#[derive(Debug, Clone, PartialEq)]
pub struct CellEntry {
    pub coord: Coord,
    pub values: Vec<(String, Value, CellMeta)>,
}

/// One cell/key's current state, as reported by `World::changes_since`
/// (or handed to `World::apply_changes`): its value and metadata, or when
/// it was removed -- plus who made that write.
#[derive(Debug, Clone, PartialEq)]
pub struct Change {
    pub coord: Coord,
    pub key: String,
    pub kind: ChangeKind,
    pub stamp: Stamp,
}

/// What `World::remove_region` actually removed: the time recorded for
/// the removal (also every tombstone's time, when tombstones are kept),
/// and each cell that held a value for the key, with the stamp its
/// removal got -- cells that were already empty aren't listed.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct RemovedCells {
    pub at_ms: u64,
    pub cells: Vec<(Coord, Stamp)>,
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
    /// Whether newly written chunk files are zstd-compressed -- see
    /// `with_compression`. Writes only: reads detect each file's encoding
    /// from its own first bytes, so flipping this flag never strands data
    /// written under the other setting.
    compression: bool,
    /// Whether removals leave a tombstone, and for how long -- see
    /// `with_tombstone_retention`.
    tombstone_retention: Option<Duration>,
    /// Secondary equality indexes, one per key opted in via `create_index`
    /// -- see `World::create_index`/`lookup_eq` and `crate::index`. Empty
    /// (no keys indexed, zero overhead on every write) until a caller asks
    /// for one.
    value_index: ValueIndex,
    /// This world's running content digest, if enabled (see
    /// `with_content_digest`) -- `None` means disabled, the default.
    content_digest: Option<ContentDigest>,
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
        // Opens whatever secondary indexes already exist under
        // `root/indexes/` -- each one's own on-disk LSM state (segments +
        // WAL, see `crate::index`/`crate::lsm`) already *is* its
        // persisted content, so unlike the very first `create_index` for
        // a key, this needs no `list_cells`-style rescan to restore it.
        let value_index = ValueIndex::open(&root)?;
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
            compression: false,
            tombstone_retention: None,
            value_index,
            content_digest: None,
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

    /// Turns zstd compression of chunk files on or off. Off by default,
    /// matching every world written before this option existed.
    ///
    /// This setting applies to *writes* only, and is deliberately not
    /// persisted in `world.txt`: reads sniff zstd's magic number off the
    /// front of each file (see `load_chunk_unmetered`), so a single world
    /// can hold a mix of compressed and uncompressed chunks and stays
    /// fully readable whichever way the flag is set. Turning compression
    /// on doesn't rewrite existing files -- each one is compressed the
    /// next time its chunk is written.
    ///
    /// Chainable right after `create`/`open`, same as
    /// `with_max_concurrent_disk_ops`:
    /// ```no_run
    /// # fn main() -> std::io::Result<()> {
    /// let world = kblockdblib::World::create("./data", 3, 10_000, 32)?
    ///     .with_compression(true);
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_compression(mut self, compression: bool) -> Self {
        self.compression = compression;
        self
    }

    /// Makes every removal leave a tombstone -- when the value was
    /// removed -- kept for `retention` and purged the next time its chunk
    /// is written after that. `None` (the default) keeps none: a remove
    /// just erases, as it always has.
    ///
    /// Tombstones exist for replication: they're what lets
    /// `apply_replicated` refuse a late-arriving, older write to a key
    /// that's since been deleted, and what lets `changes_since` report
    /// deletes. They're never visible to `get`/`get_region`/`list_cells`.
    /// Only a cell that actually held a value gets one.
    pub fn with_tombstone_retention(mut self, retention: Option<Duration>) -> Self {
        self.tombstone_retention = retention;
        self
    }

    /// See `with_tombstone_retention`.
    pub fn tombstone_retention(&self) -> Option<Duration> {
        self.tombstone_retention
    }

    /// Whether this world zstd-compresses the chunk files it writes -- see
    /// `with_compression`.
    pub fn compression(&self) -> bool {
        self.compression
    }

    /// Turns this world's content digest on or off -- off by default.
    /// When on, every `set`/`remove` (and their region/replicated/batch
    /// counterparts) updates a running digest of every currently-live
    /// cell's content -- coordinate, key, value, and metadata -- so
    /// `content_digest()` can answer "does this world's data match
    /// another node's" without comparing cell by cell. Meant for a
    /// clustered deployment's periodic sync check
    /// (`kblockdbcluster`/`docs/clustering.md`), not for everyday use --
    /// see `ContentDigest`'s doc comment for the digest itself and why
    /// XOR makes it incremental.
    ///
    /// Off by default because it isn't free: unlike a secondary index
    /// (paid only by the keys someone actually indexes), the digest
    /// covers *every* key, so turning it on costs one extra in-memory
    /// lookup -- the cell's *old* value and metadata, needed to remove
    /// their old contribution before folding in the new one -- on every
    /// write, for every key. Worth it only when something is actually
    /// going to compare this world's digest against a peer's.
    ///
    /// Turning it on for the first time on a world that already has data
    /// costs one `list_cells`-equivalent full scan, to compute a
    /// starting value -- same one-time cost `create_index` pays for a
    /// brand new index. After that, it's persisted (`content_digest.bin`
    /// at the world root) and loaded directly on every later `open`, no
    /// rescan needed. Turning it back off stops maintaining it but
    /// leaves that file as-is (harmless, just unread while disabled) --
    /// turning it on again later picks the old value back up rather than
    /// rescanning again, which is correct exactly because disabled means
    /// "stopped updating it," not "the data changed without it."
    pub fn with_content_digest(mut self, enabled: bool) -> io::Result<Self> {
        if !enabled {
            self.content_digest = None;
            return Ok(self);
        }
        if self.content_digest.is_none() {
            let path = self.root.join(CONTENT_DIGEST_FILE);
            self.content_digest = Some(ContentDigest::open(path, || {
                let mut value = 0u64;
                for cell in self.list_cells()? {
                    for (key, cell_value, meta) in &cell.values {
                        value ^= entry_digest(&cell.coord, key, cell_value, meta);
                    }
                }
                Ok(value)
            })?);
        }
        Ok(self)
    }

    /// This world's current content digest, or `None` if not enabled --
    /// see `with_content_digest`.
    pub fn content_digest(&self) -> Option<u64> {
        self.content_digest.as_ref().map(ContentDigest::get)
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

    /// This world's valid coordinate range per axis, `[low, high)` --
    /// `world_dim` cells wide, centered on zero (half below, half at or
    /// above it; an odd `world_dim` puts the extra cell on the positive
    /// side). Computed in `i64` so this can't overflow `i32` even at
    /// extreme `world_dim` values, even though every coordinate that
    /// actually falls in this range fits in `i32`.
    fn axis_bounds(&self) -> (i64, i64) {
        let low = -((self.world_dim / 2) as i64);
        (low, low + self.world_dim as i64)
    }

    /// Distinct chunk-key values a full axis spans, i.e. `chunks_per_axis()
    /// ^ axes` chunk files at most (most are never written -- see `World`'s
    /// "Layout on disk" doc comment). Since `axis_bounds()`'s low edge isn't
    /// generally a multiple of `chunk_dim`, this is the exact inclusive
    /// span of chunk-key buckets the valid range touches, not just
    /// `world_dim.div_ceil(chunk_dim)` -- the two agree only when the low
    /// edge happens to land on a chunk boundary (e.g. `world_dim` even and
    /// a lower edge of exactly 0, as when `world_dim` was always the whole
    /// range starting at zero).
    pub fn chunks_per_axis(&self) -> u32 {
        let (lo, hi) = self.axis_bounds();
        let chunk_dim = self.chunk_dim as i64;
        ((hi - 1).div_euclid(chunk_dim) - lo.div_euclid(chunk_dim) + 1) as u32
    }

    pub fn schema_len(&self) -> usize {
        self.schema().len()
    }

    /// Every column in this world's schema -- each live key and the value
    /// type fixed for it -- sorted by key.
    pub fn columns(&self) -> Vec<ColumnInfo> {
        self.schema().columns()
    }

    /// Adds `key` as a new column of type `value_type`, fixing its type
    /// before any cell has ever been written to it. Fails with
    /// `AlreadyExists` if the column already exists, and with
    /// `InvalidInput` if `key` isn't a name the schema can store.
    ///
    /// Columns are also created implicitly by `set`/`set_region` (which
    /// take their type from the value being written), so this isn't
    /// required before writing -- it's for declaring a world's shape up
    /// front, and for pinning a key's type without having to write a
    /// throwaway value to do it.
    pub fn add_column(&self, key: &str, value_type: ValueType) -> io::Result<()> {
        self.schema().add(key, value_type)?;
        crate::logger::info(format!(
            "added column '{key}' ({}) to {}",
            value_type.as_str(),
            self.root.display()
        ));
        Ok(())
    }

    /// Drops `key` from the schema *and* erases every value ever written
    /// for it, from every chunk in the world. Returns false, having
    /// changed nothing, if `key` isn't a column.
    ///
    /// This is irreversible and proportional in cost to how many chunk
    /// files exist: it walks all of them, rewriting each one that held the
    /// column (and deleting any left with nothing in it). The schema
    /// tombstone is written first, so the column stops being readable
    /// immediately and a crash partway through the walk can never leave
    /// the key partly alive -- at worst it leaves some unreachable bytes
    /// in chunks the walk hadn't reached, which the next
    /// `remove_column` of that same key can no longer clean up, since the
    /// id is gone from the schema by then.
    ///
    /// A later `add_column`/`set` of the same key starts over: it gets a
    /// brand new id, with no connection to the removed column's data, and
    /// so may use a different type than the old one did.
    pub fn remove_column(&self, key: &str) -> io::Result<bool> {
        // The tombstone goes down under the same lock that `set`'s
        // `intern` takes, so no write can start using this id afterward
        // -- a concurrent `set` of this key either already has the old id
        // (and may leave bytes the walk below misses, unreachable by
        // name), or interns a brand new id that the walk won't touch.
        let Some(key_id) = self.schema().remove(key)? else {
            return Ok(false);
        };

        let mut chunk_keys = Vec::new();
        collect_chunk_keys(&self.root, self.axes, &mut Vec::new(), &mut chunk_keys)?;
        let mut purged = 0u64;
        let digested = self.digest_enabled();
        for ckey in &chunk_keys {
            // `with_chunk_write` is what makes this safe against
            // concurrent single-cell writes: it takes the same per-chunk
            // lock they do, and writes the chunk back out (or deletes it,
            // if this emptied it) exactly as they would.
            let (removed, old_hashes) = self.with_chunk_maybe_write(ckey, |chunk| {
                let old_hashes: Vec<u64> = digested
                    .then(|| {
                        chunk
                            .column_entries(key_id)
                            .into_iter()
                            .map(|(local_idx, value, meta)| {
                                entry_digest(&self.unsplit(ckey, local_idx), key, &value, &meta)
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                let removed = chunk.remove_column(key_id);
                ((removed, old_hashes), removed)
            })?;
            if removed {
                purged += 1;
            }
            for old_hash in old_hashes {
                self.apply_digest_update(Some(old_hash), None);
            }
        }
        self.value_index.drop_index(key_id)?;
        crate::logger::info(format!(
            "removed column '{key}' from {} ({purged} chunk(s) rewritten)",
            self.root.display()
        ));
        Ok(true)
    }

    /// Builds a secondary equality index on `key`: after this returns,
    /// `lookup_eq(key, value)` answers "which cells have `key == value`" in
    /// time proportional to the number of matches, instead of `list_cells`'s
    /// full chunk-by-chunk decode of the whole world. Every `set`/
    /// `set_region`/`remove`/`remove_region` (and their replicated/
    /// stamped/batch counterparts) of `key` keeps the index up to date from
    /// here on, at the cost of one hashmap update per write to `key` --
    /// writes to every other key are unaffected.
    ///
    /// Equality only: there's no ordering here, so a `<`/`>` comparison on
    /// `key` still needs a full scan even after this. Interns `key` if it
    /// isn't already a column (an index on a key nothing has written yet is
    /// legal, just trivially empty until something is).
    ///
    /// Idempotent: calling this again on an already-indexed key is a cheap
    /// no-op, not a rebuild -- use `rebuild_index` if you suspect it's gone
    /// stale (it shouldn't; every write path keeps it in sync).
    ///
    /// Cost: one `list_cells`-equivalent full-world scan, done once, here --
    /// the same cost a single unindexed `WHERE key = ...` query would have
    /// paid anyway, just paid up front instead of on every query.
    pub fn create_index(&self, key: &str) -> io::Result<()> {
        let Some(key_id) = self.schema().id_for_key(key) else {
            // Nothing has ever written this key: there's no type to fix
            // yet, so there's nothing to scan either -- just register an
            // empty index under a freshly reserved id isn't possible
            // without a type, so instead this key simply isn't indexable
            // until something sets it. Reported as success (an empty
            // index is a legitimate, if currently moot, state) rather than
            // an error that would surprise a caller indexing a key ahead of
            // any data.
            return Ok(());
        };
        if self.value_index.is_indexed(key_id) {
            return Ok(());
        }
        self.build_index(key, key_id)
    }

    /// Drops `key`'s secondary index (if it has one) and builds a fresh one
    /// from scratch, same cost as `create_index` -- a full `list_cells`-
    /// equivalent scan. Unlike `create_index`, this always rebuilds even if
    /// `key` is already indexed: for a caller that suspects it's gone
    /// stale (it shouldn't -- every write path keeps it in sync, see
    /// `create_index`'s doc comment -- but this is the recovery lever if
    /// that invariant were ever violated, e.g. by a bug or a direct edit
    /// to the data directory outside this `World`).
    ///
    /// A no-op, same as `create_index`, if `key` has never been written --
    /// there's nothing to rebuild.
    pub fn rebuild_index(&self, key: &str) -> io::Result<()> {
        let Some(key_id) = self.schema().id_for_key(key) else {
            return Ok(());
        };
        self.value_index.drop_index(key_id)?;
        self.build_index(key, key_id)
    }

    /// Shared by `create_index`/`rebuild_index`: registers `key_id` as
    /// indexed (starting empty, its on-disk LSM directory freshly created
    /// -- see `crate::index`/`crate::lsm`) and backfills it from every
    /// cell `list_cells` currently reports holding `key`.
    fn build_index(&self, key: &str, key_id: u32) -> io::Result<()> {
        self.value_index.create(key_id)?;
        for cell in self.list_cells()? {
            if let Some((_, value, _)) = cell.values.iter().find(|(k, ..)| k == key) {
                self.value_index.record(key_id, &cell.coord, None, Some(value))?;
            }
        }
        crate::logger::info(format!("(re)built index on key '{key}' at {}", self.root.display()));
        Ok(())
    }

    /// `ValueIndex::record`, logging (rather than propagating) a failure
    /// -- every write-path call site below calls this, not
    /// `self.value_index.record` directly. A secondary index's own disk
    /// I/O failing doesn't fail the cell write that triggered it: that
    /// write already succeeded, and an index is a derived, rebuildable
    /// structure (`rebuild_index`), not the source of truth -- the same
    /// "fall back, don't fail" stance `lookup_eq` takes on the read side.
    fn record_index_change(
        &self,
        key_id: u32,
        coord: &Coord,
        old: Option<&Value>,
        new: Option<&Value>,
    ) {
        if let Err(e) = self.value_index.record(key_id, coord, old, new) {
            crate::logger::warn(format!("secondary index update failed at {coord:?}: {e}"));
        }
    }

    /// Whether this world's content digest is enabled -- see
    /// `with_content_digest`. A write-path call site checks this (cheap:
    /// no locking, just reading an `Option`) to decide whether it's worth
    /// fetching a cell/key's *old* value and metadata at all.
    fn digest_enabled(&self) -> bool {
        self.content_digest.is_some()
    }

    /// `ContentDigest::update`, logging (rather than propagating) a
    /// failure -- the same "fall back, don't fail the write" stance
    /// `record_index_change` takes, for the same reason: the digest is a
    /// derived summary, not the data itself. `old`/`new` are already-
    /// computed `entry_digest` hashes (or `None`, meaning "wasn't/isn't
    /// set"), not raw values -- every call site computes them inline,
    /// right where it already has the old and new value/metadata at
    /// hand, rather than carrying clones of either out to here.
    fn apply_digest_update(&self, old: Option<u64>, new: Option<u64>) {
        let Some(digest) = &self.content_digest else {
            return;
        };
        if let Err(e) = digest.update(old, new) {
            crate::logger::warn(format!("content digest update failed: {e}"));
        }
    }

    /// Stops maintaining `key`'s secondary index and deletes its on-disk
    /// state. Returns whether it was indexed. After this, `lookup_eq(key,
    /// ...)` falls back to reporting "not indexed" (`None`) the same as a
    /// key that was never indexed at all.
    pub fn drop_index(&self, key: &str) -> io::Result<bool> {
        let Some(key_id) = self.schema().id_for_key(key) else {
            return Ok(false);
        };
        self.value_index.drop_index(key_id)
    }

    /// Every key currently indexed (see `create_index`), sorted.
    pub fn indexed_keys(&self) -> Vec<String> {
        let schema = self.schema();
        let mut keys: Vec<String> = self
            .value_index
            .indexed_key_ids()
            .into_iter()
            .filter_map(|id| schema.key_for_id(id).map(str::to_string))
            .collect();
        keys.sort();
        keys
    }

    /// Every coordinate currently holding `value` under `key`, using its
    /// secondary index -- `None` if `key` isn't indexed (see
    /// `create_index`), in which case the caller should fall back to a full
    /// `list_cells` scan filtered in memory. `Some(vec![])` is a real
    /// answer ("indexed, zero matches"), not "unknown".
    ///
    /// `value`'s own type doesn't have to match `key`'s column type exactly
    /// -- an `I64`/`F64` mismatch is reconciled the same way `eval_compare`
    /// (`kblockdbquery`) treats `WHERE key = <literal>` for a numeric
    /// column, via `compare_f64`'s plain `==`: `5` matches a `F64` column
    /// holding `5.0`, and `5.0` matches an `I64` column holding `5`, but
    /// `5.5` can never match an `I64` column at all (reported here as
    /// `Some(vec![])`, since that's a precise answer, not "unknown").
    /// `Str`/`Bool` compared against the wrong type never match either --
    /// same "doesn't apply, not an error" rule as `eval_compare`.
    pub fn lookup_eq(&self, key: &str, value: &Value) -> Option<Vec<Coord>> {
        let schema = self.schema();
        let key_id = schema.id_for_key(key)?;
        if !self.value_index.is_indexed(key_id) {
            return None;
        }
        // `is_indexed` being true means `key` was a real column at some
        // point (see `create_index`), so it always has a type here.
        let column_type = schema.type_for_key(key)?;
        drop(schema);
        let normalized = match normalize_value_for_type(value, column_type) {
            Some(value) => value,
            None => return Some(Vec::new()),
        };
        // A read error here (disk I/O, same as any other file access in
        // this crate) falls back to "not indexed" rather than a hard
        // error: the caller's response is the same either way -- a full
        // `list_cells` scan -- and that's a better outcome than failing a
        // read outright over a structure that exists purely to make reads
        // faster, not to be their only path to a correct answer.
        match self.value_index.lookup_eq(key_id, &normalized) {
            Ok(result) => result,
            Err(e) => {
                crate::logger::warn(format!(
                    "secondary index lookup failed for key '{key}': {e} -- falling back to a \
                     full scan"
                ));
                None
            }
        }
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

    /// Every populated cell in the world (every coordinate with at least
    /// one key set), each with the full set of keys/values/metadata set
    /// there, sorted ascending by coordinate (axis 0 most significant --
    /// plain lexicographic order over the coordinate's components, same as
    /// comparing two `Vec<i32>`s).
    ///
    /// A live filesystem walk plus a full decode of every chunk file --
    /// like `stats()`, but heavier, since this reads and decodes whole
    /// chunk contents rather than just file sizes, so cost is proportional
    /// to how much data actually exists on disk right now. Meant for
    /// small-to-moderate worlds (e.g. kblockdbserver's `/` data browser),
    /// not for scanning a world with millions of populated cells on every
    /// call.
    pub fn list_cells(&self) -> io::Result<Vec<CellEntry>> {
        let mut chunk_keys = Vec::new();
        collect_chunk_keys(&self.root, self.axes, &mut Vec::new(), &mut chunk_keys)?;

        // Snapshotted once, up front, rather than locked for the whole
        // (potentially slow, disk-bound) walk below -- schema.txt is small
        // (one entry per distinct key ever used in the whole world, not per
        // cell, see `Schema`'s doc comment), so this costs little and lets
        // concurrent `set`s of a brand new key proceed without waiting on
        // this call.
        let key_names: Vec<Option<String>> = {
            let schema = self.schema();
            (0..schema.id_space() as u32)
                .map(|id| schema.key_for_id(id).map(str::to_string))
                .collect()
        };

        let mut cells = Vec::new();
        for ckey in &chunk_keys {
            // Through the cache, under the chunk's lock, not straight off
            // disk: chunk files are rewritten in place, so reading one
            // while a write to it is in flight could see half a file.
            let entries_by_cell = self.with_chunk_read(ckey, Chunk::entries_by_local_idx)?;
            for (local_idx, entries) in entries_by_cell {
                let coord = self.unsplit(ckey, local_idx);
                let mut values: Vec<(String, Value, CellMeta)> = entries
                    .into_iter()
                    // Anything under an id with no live key is skipped,
                    // not reported under a placeholder name: that's either
                    // a removed column whose purge didn't reach this chunk
                    // (see `remove_column`) or an id from another world.
                    .filter_map(|(key_id, value, meta)| {
                        let name = key_names.get(key_id as usize)?.clone()?;
                        Some((name, value, meta))
                    })
                    .collect();
                if values.is_empty() {
                    continue;
                }
                values.sort_by(|a, b| a.0.cmp(&b.0));
                cells.push(CellEntry { coord, values });
            }
        }
        cells.sort_by(|a, b| a.coord.iter().cmp(b.coord.iter()));
        Ok(cells)
    }

    /// Every currently non-empty chunk's own content digest -- the XOR of
    /// `entry_digest` over just that chunk's live cells, keyed by the
    /// chunk's own coordinate (not a cell coordinate -- see `split`'s doc
    /// comment on what a chunk key means). A chunk with nothing live in
    /// it has no entry at all, same as it has no file on disk.
    ///
    /// Unlike `content_digest()`, this is always available, whether or
    /// not `with_content_digest` is enabled -- and it isn't incrementally
    /// maintained: it's a full chunk-by-chunk scan, the same cost class
    /// as `list_cells`, recomputed fresh on every call. Meant for
    /// localizing a mismatch `content_digest()` already reported some
    /// other way (see `docs/clustering.md`'s "Sync check"): comparing two
    /// nodes' `chunk_digests()` for the same database pinpoints exactly
    /// which chunk(s) differ, which the single whole-world digest can't
    /// -- not for routine use, since it costs a full scan every time.
    ///
    /// XORing every value in the returned map together reproduces
    /// exactly what `content_digest()` computes from the same data (both
    /// use the same per-entry hash, just grouped differently -- XOR
    /// doesn't care about grouping), so a whole-database mismatch is
    /// guaranteed to show up as a difference somewhere in this map too:
    /// either a shared chunk key with a different value, or a chunk
    /// present on only one side.
    pub fn chunk_digests(&self) -> io::Result<HashMap<Coord, u64>> {
        let mut chunk_keys = Vec::new();
        collect_chunk_keys(&self.root, self.axes, &mut Vec::new(), &mut chunk_keys)?;
        let key_names: Vec<Option<String>> = {
            let schema = self.schema();
            (0..schema.id_space() as u32)
                .map(|id| schema.key_for_id(id).map(str::to_string))
                .collect()
        };
        let mut out = HashMap::new();
        for ckey in &chunk_keys {
            let entries_by_cell = self.with_chunk_read(ckey, Chunk::entries_by_local_idx)?;
            let mut digest = 0u64;
            let mut any = false;
            for (local_idx, entries) in entries_by_cell {
                let coord = self.unsplit(ckey, local_idx);
                for (key_id, value, meta) in entries {
                    let Some(name) = key_names.get(key_id as usize).and_then(Clone::clone) else {
                        continue;
                    };
                    digest ^= entry_digest(&coord, &name, &value, &meta);
                    any = true;
                }
            }
            if any {
                out.insert(ckey.clone(), digest);
            }
        }
        Ok(out)
    }

    /// This single coordinate's full cell state (every key set there,
    /// alongside its value and `CellMeta`) -- `list_cells` restricted to
    /// one cell, for a caller that already knows exactly which coordinates
    /// it wants (e.g. `lookup_eq`'s candidates) instead of discovering them
    /// by scanning every chunk. `None` if nothing's set there.
    ///
    /// Cost is one chunk read (cached after the first touch, same as
    /// `get`) plus decoding that one chunk's columns for this cell --
    /// nowhere near a full `list_cells` walk, but still proportional to how
    /// many distinct keys this chunk holds, not O(1) the way `get` is.
    pub fn cell_entry_at(&self, coord: &[i32]) -> io::Result<Option<CellEntry>> {
        let (ckey, local_idx) = self.split(coord)?;
        let key_names: Vec<Option<String>> = {
            let schema = self.schema();
            (0..schema.id_space() as u32)
                .map(|id| schema.key_for_id(id).map(str::to_string))
                .collect()
        };
        let entries = self.with_chunk_read(&ckey, |chunk| chunk.entries_at(local_idx))?;
        let mut values: Vec<(String, Value, CellMeta)> = entries
            .into_iter()
            .filter_map(|(key_id, value, meta)| {
                let name = key_names.get(key_id as usize)?.clone()?;
                Some((name, value, meta))
            })
            .collect();
        if values.is_empty() {
            return Ok(None);
        }
        values.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(Some(CellEntry {
            coord: Coord::from(coord),
            values,
        }))
    }

    /// Every value and tombstone (see `with_tombstone_retention`) whose
    /// stamp `known` doesn't cover -- every write the holder of `known`
    /// hasn't seen -- handed to `emit` one chunk's worth at a time. `emit`
    /// returning `false` stops the walk early. This is what a replication
    /// peer's catch-up is built from.
    ///
    /// Cheaper than `list_cells` when `known` is close to current: each
    /// chunk file's header records the highest seq per origin stored in
    /// it, so a chunk `known` already covers is skipped after reading a
    /// few bytes, without being decoded. A chunk that does need reading
    /// goes through the cache (like `get`), so it never sees a file
    /// mid-write. Older-format files are always read.
    ///
    /// Legacy (`Stamp::NONE`) data counts as written by `legacy_origin`
    /// -- the caller's `stamp::legacy_origin(node_id)` -- so it's sent only
    /// when `known` lacks that entry. It's still reported with its real
    /// stamp, `Stamp::NONE`.
    pub fn changes_since(
        &self,
        known: &VersionVector,
        legacy_origin: u64,
        mut emit: impl FnMut(Vec<Change>) -> bool,
    ) -> io::Result<()> {
        let legacy = Stamp::new(legacy_origin, 0);
        let mut chunk_keys = Vec::new();
        collect_chunk_keys(&self.root, self.axes, &mut Vec::new(), &mut chunk_keys)?;
        for ckey in &chunk_keys {
            // Any error peeking (a file being rewritten, or deleted, right
            // now) just means "can't tell" -- fall through to a real read.
            if let Ok(Some(max_seq)) = self.peek_max_seq(ckey) {
                if chunk::covers_max_seq(known, &max_seq, legacy) {
                    continue;
                }
            }
            let changes = self.with_chunk_read(ckey, |chunk| chunk.changes_since(known, legacy))?;
            if changes.is_empty() {
                continue;
            }
            let batch: Vec<Change> = {
                let schema = self.schema();
                changes
                    .into_iter()
                    // A removed column's leftovers aren't reported -- see
                    // `list_cells`.
                    .filter_map(|(local_idx, key_id, kind, stamp)| {
                        Some(Change {
                            coord: self.unsplit(ckey, local_idx),
                            key: schema.key_for_id(key_id)?.to_string(),
                            kind,
                            stamp,
                        })
                    })
                    .collect()
            };
            if !batch.is_empty() && !emit(batch) {
                break;
            }
        }
        Ok(())
    }

    /// Chunk `ckey`'s `max_seq` from its file header alone (see
    /// `Chunk::read_max_seq`): empty if there's no file, `None` for an
    /// older-format file.
    fn peek_max_seq(&self, ckey: &ChunkKey) -> io::Result<Option<VersionVector>> {
        let _permit = self.disk_io.acquire();
        let f = match File::open(self.chunk_path(ckey)) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Some(VersionVector::new())),
            Err(e) => return Err(e),
        };
        let mut r = BufReader::new(f);
        let mut head = [0u8; ZSTD_MAGIC.len()];
        let head_len = read_up_to(&mut r, &mut head)?;
        let compressed = head[..head_len] == ZSTD_MAGIC;
        let mut r = io::Cursor::new(&head[..head_len]).chain(r);
        if compressed {
            Chunk::read_max_seq(&mut zstd::stream::read::Decoder::new(r)?)
        } else {
            Chunk::read_max_seq(&mut r)
        }
    }

    fn chunk_path(&self, ckey: &ChunkKey) -> PathBuf {
        let mut p = self.root.clone();
        for &c in &ckey[..self.axes - 1] {
            p = p.join(c.to_string());
        }
        p.join(format!("{}.chunk", ckey[self.axes - 1]))
    }

    fn split(&self, coord: &[i32]) -> io::Result<(ChunkKey, usize)> {
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
        let (lo, hi) = self.axis_bounds();
        if let Some(&bad) = coord.iter().find(|&&c| (c as i64) < lo || (c as i64) >= hi) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("coordinate {coord:?} is out of range: {bad} is not in [{lo}, {hi})"),
            ));
        }
        // Floor (Euclidean) division/remainder, not Rust's default
        // truncate-toward-zero `/`/`%` -- with a negative `c`, truncation
        // would put e.g. -1 and chunk_dim-1's negation in inconsistent
        // chunks and could yield a negative `local_idx`. `div_euclid`/
        // `rem_euclid` keep chunk boundaries evenly spaced across zero and
        // guarantee `local_idx`'s per-axis component always lands in
        // `0..chunk_dim`.
        let chunk_dim = self.chunk_dim as i32;
        let mut ckey: ChunkKey = Coord::zeros(self.axes);
        let mut local_idx = 0usize;
        let mut mult = 1usize;
        for (a, c) in coord.iter().enumerate() {
            ckey[a] = c.div_euclid(chunk_dim);
            local_idx += c.rem_euclid(chunk_dim) as usize * mult;
            mult *= self.chunk_dim as usize;
        }
        Ok((ckey, local_idx))
    }

    /// The inverse of `split`: given a chunk's key and a local index within
    /// it, the full world coordinate that maps to that (chunk, local index)
    /// pair. Used by `list_cells`, which discovers cells chunk-by-chunk
    /// (via `Chunk::entries_by_local_idx`) and needs each one's real
    /// coordinate, not just its opaque local index.
    fn unsplit(&self, ckey: &ChunkKey, mut local_idx: usize) -> Coord {
        let chunk_dim = self.chunk_dim as usize;
        let mut coord = Coord::zeros(self.axes);
        for a in 0..self.axes {
            coord[a] = ckey[a] * self.chunk_dim as i32 + (local_idx % chunk_dim) as i32;
            local_idx /= chunk_dim;
        }
        coord
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
        self.with_chunk_maybe_write(ckey, |chunk| (f(chunk), true))
    }

    /// `with_chunk_write`, but `f` also says whether it actually changed
    /// anything: when it returns `false` the chunk's file is left exactly
    /// as it was, untouched and uncounted. For `remove_column`, which
    /// visits *every* chunk in the world but typically only finds the
    /// column in some of them -- rewriting byte-identical files for all
    /// the rest would make dropping a rare column as expensive as
    /// rewriting the entire world.
    fn with_chunk_maybe_write<T>(
        &self,
        ckey: &ChunkKey,
        f: impl FnOnce(&mut Chunk) -> (T, bool),
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

        let (result, changed) = f(chunk);
        if !changed {
            return Ok(result);
        }
        if let Some(retention) = self.tombstone_retention {
            let retention_ms = u64::try_from(retention.as_millis()).unwrap_or(u64::MAX);
            let cutoff = now_ms().saturating_sub(retention_ms);
            chunk.purge_tombstones_older_than(cutoff);
        }

        let chunk_path = self.chunk_path(ckey);
        if chunk.is_empty() {
            // Nothing left in this chunk (e.g. every cell was removed) --
            // don't leave a pointless empty file around.
            let _ = fs::remove_file(&chunk_path);
        } else {
            fs::create_dir_all(chunk_path.parent().unwrap())?;
            let file = File::create(&chunk_path)?;
            let mut w = BufWriter::new(file);
            if self.compression {
                let mut encoder = zstd::stream::write::Encoder::new(&mut w, ZSTD_LEVEL)?;
                chunk.write_to(&mut encoder)?;
                // Writes zstd's frame epilogue; without it the file is a
                // truncated frame that no reader can decode.
                encoder.finish()?;
            } else {
                chunk.write_to(&mut w)?;
            }
            // `BufWriter` flushes on drop but swallows any error doing so,
            // which would turn a failed write into a silently "successful"
            // one -- flush explicitly so the caller sees it.
            w.flush()?;
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
            // Each file says for itself whether it's compressed, rather
            // than the world's current `compression` setting deciding --
            // that's what lets the flag be flipped on an existing world
            // (see `with_compression`).
            let mut head = [0u8; ZSTD_MAGIC.len()];
            let head_len = read_up_to(&mut r, &mut head)?;
            let compressed = head[..head_len] == ZSTD_MAGIC;
            let mut r = io::Cursor::new(&head[..head_len]).chain(r);
            if compressed {
                let mut decoder = zstd::stream::read::Decoder::new(r)?;
                Chunk::read_from(&mut decoder, self.chunk_cells)
            } else {
                Chunk::read_from(&mut r, self.chunk_cells)
            }
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

    pub fn get(&self, coord: &[i32], key: &str) -> io::Result<Option<Value>> {
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

    /// This cell/key pair's bookkeeping -- when it was first set, when it
    /// was last changed, and how many times it's been set since (see
    /// `CellMeta`'s doc comment) -- or `None` under exactly the same
    /// conditions `get` would return `None`.
    pub fn get_meta(&self, coord: &[i32], key: &str) -> io::Result<Option<CellMeta>> {
        let (ckey, local_idx) = self.split(coord)?;
        let Some(key_id) = self.schema().id_for_key(key) else {
            return Ok(None);
        };
        self.with_chunk_read(&ckey, |chunk| chunk.get_meta(local_idx, key_id))
    }

    /// `get` and `get_meta` together, guaranteed consistent with each other
    /// -- both are read from the same chunk snapshot under one lock hold,
    /// so (unlike calling `get` and `get_meta` separately) a concurrent
    /// write to this exact cell/key can never land between them and pair
    /// one call's value with a *different* call's meta.
    pub fn get_with_meta(&self, coord: &[i32], key: &str) -> io::Result<Option<(Value, CellMeta)>> {
        let (ckey, local_idx) = self.split(coord)?;
        let Some(key_id) = self.schema().id_for_key(key) else {
            return Ok(None);
        };
        self.with_chunk_read(&ckey, |chunk| {
            chunk
                .get(local_idx, key_id)
                .zip(chunk.get_meta(local_idx, key_id))
        })
    }

    /// Returns the `CellMeta` this write just produced -- callers that
    /// replicate writes to peer servers (`kblockdbserver`) use this to
    /// ship the exact metadata a peer should apply, rather than needing a
    /// second lookup.
    pub fn set(&self, coord: &[i32], key: &str, value: Value) -> io::Result<CellMeta> {
        Ok(self.set_stamped(coord, key, value, &mut || Stamp::NONE)?.0)
    }

    /// `set`, with the write's origin taken from `stamp` -- called exactly
    /// once if the write happens, not at all if it fails before writing.
    /// Returns the stamp alongside the `CellMeta`, for a replicating
    /// caller to ship.
    pub fn set_stamped(
        &self,
        coord: &[i32],
        key: &str,
        value: Value,
        stamp: &mut dyn FnMut() -> Stamp,
    ) -> io::Result<(CellMeta, Stamp)> {
        // Validate the coordinate before interning `key`: a failed `set`
        // shouldn't have the side effect of permanently registering a new
        // key that was never actually written anywhere.
        let (ckey, local_idx) = self.split(coord)?;
        // intern also enforces `key`'s type (fixed the first time it's set
        // -- see `Schema`'s doc comment), so a type mismatch fails here,
        // before touching any chunk, as a normal `InvalidInput` error.
        let key_id = self.schema().intern(key, value.value_type())?;
        let now_ms = now_ms();
        // Only indexed keys (or, below, a digest-enabled world) pay for
        // reading the old value back out of the chunk before overwriting
        // it -- see `create_index`'s doc comment on the "zero overhead
        // for unindexed keys" guarantee.
        let indexed = self.value_index.is_indexed(key_id);
        let digested = self.digest_enabled();
        let new_value_for_index = indexed.then(|| value.clone());
        let new_value_for_digest = digested.then(|| value.clone());
        let ((meta, stamp), old_value, old_hash) = self.with_chunk_write(&ckey, |chunk| {
            let old_value = (indexed || digested).then(|| chunk.get(local_idx, key_id)).flatten();
            let old_hash = digested
                .then(|| {
                    old_value
                        .as_ref()
                        .zip(chunk.get_meta(local_idx, key_id))
                        .map(|(v, m)| entry_digest(coord, key, v, &m))
                })
                .flatten();
            let stamp = stamp();
            let meta = chunk.set_stamped(local_idx, key_id, value, now_ms, stamp);
            ((meta, stamp), old_value, old_hash)
        })?;
        if indexed {
            self.record_index_change(
                key_id,
                &Coord::from(coord),
                old_value.as_ref(),
                new_value_for_index.as_ref(),
            );
        }
        if digested {
            let new_hash = new_value_for_digest.map(|v| entry_digest(coord, key, &v, &meta));
            self.apply_digest_update(old_hash, new_hash);
        }
        Ok((meta, stamp))
    }

    /// Applies a replicated write -- `value`/`meta` as reported by the
    /// peer that originated it -- with last-write-wins conflict
    /// resolution: if this cell/key already holds a value whose
    /// `modified_at_ms` is greater than or equal to `meta.modified_at_ms`,
    /// or a tombstone at least that recent (it was deleted later than
    /// this write), the incoming write is simply discarded (a tie keeps
    /// whatever is already here -- an accepted, documented imprecision of
    /// millisecond-resolution timestamps, not a correctness bug). Returns
    /// whether the write was actually applied.
    ///
    /// Unlike `set`, this never derives its own timestamp/version --
    /// `meta` is applied verbatim (see `Chunk::set_with_meta`) -- so every
    /// node in a cluster converges on the exact same `CellMeta` for a
    /// given logical write, not a new one per node that received it.
    ///
    /// The read-compare-write happens inside one `with_chunk_maybe_write`
    /// closure so it's atomic with respect to any other write (local or
    /// replicated) landing on the same chunk concurrently.
    pub fn apply_replicated(
        &self,
        coord: &[i32],
        key: &str,
        value: Value,
        meta: CellMeta,
    ) -> io::Result<bool> {
        self.apply_replicated_stamped(coord, key, value, meta, Stamp::NONE)
    }

    /// `apply_replicated`, recording the write's origin `stamp` -- which
    /// also breaks a tie between equal `modified_at_ms` (see `Stamp`).
    pub fn apply_replicated_stamped(
        &self,
        coord: &[i32],
        key: &str,
        value: Value,
        meta: CellMeta,
        stamp: Stamp,
    ) -> io::Result<bool> {
        let (ckey, local_idx) = self.split(coord)?;
        let key_id = self.schema().intern(key, value.value_type())?;
        let indexed = self.value_index.is_indexed(key_id);
        let digested = self.digest_enabled();
        let new_value_for_index = indexed.then(|| value.clone());
        let new_value_for_digest = digested.then(|| value.clone());
        let (wins, old_value, old_hash) = self.with_chunk_maybe_write(&ckey, |chunk| {
            let old_value = (indexed || digested).then(|| chunk.get(local_idx, key_id)).flatten();
            let old_hash = digested
                .then(|| {
                    old_value
                        .as_ref()
                        .zip(chunk.get_meta(local_idx, key_id))
                        .map(|(v, m)| entry_digest(coord, key, v, &m))
                })
                .flatten();
            let wins = apply_set_if_newer(chunk, local_idx, key_id, value, meta, stamp);
            ((wins, old_value, old_hash), wins)
        })?;
        if indexed && wins {
            self.record_index_change(
                key_id,
                &Coord::from(coord),
                old_value.as_ref(),
                new_value_for_index.as_ref(),
            );
        }
        if digested && wins {
            let new_hash = new_value_for_digest.map(|v| entry_digest(coord, key, &v, &meta));
            self.apply_digest_update(old_hash, new_hash);
        }
        Ok(wins)
    }

    /// `apply_replicated`'s counterpart for a replicated removal: applies
    /// last-write-wins the same way, comparing `modified_at_ms` against
    /// whatever this cell/key currently holds -- a value, or a tombstone.
    ///
    /// When this world keeps tombstones, a winning removal records one at
    /// `modified_at_ms`, even if there was no value here yet: a delete
    /// that arrives before the write it deletes then still beats it. A
    /// key this world has never interned at all is a no-op either way
    /// (there's no id to record a tombstone under). Without tombstones, a
    /// cell with nothing set is a no-op. Returns whether anything changed.
    pub fn apply_replicated_remove(
        &self,
        coord: &[i32],
        key: &str,
        modified_at_ms: u64,
    ) -> io::Result<bool> {
        self.apply_replicated_remove_stamped(coord, key, modified_at_ms, Stamp::NONE)
    }

    /// `apply_replicated_remove`, recording the removal's origin `stamp`.
    pub fn apply_replicated_remove_stamped(
        &self,
        coord: &[i32],
        key: &str,
        modified_at_ms: u64,
        stamp: Stamp,
    ) -> io::Result<bool> {
        let (ckey, local_idx) = self.split(coord)?;
        let Some(key_id) = self.schema().id_for_key(key) else {
            return Ok(false); // never interned anywhere: nothing to remove
        };
        let indexed = self.value_index.is_indexed(key_id);
        let digested = self.digest_enabled();
        let keep_tombstones = self.tombstone_retention.is_some();
        let (wins, old_value, old_hash) = self.with_chunk_maybe_write(&ckey, |chunk| {
            let old_value = (indexed || digested).then(|| chunk.get(local_idx, key_id)).flatten();
            let old_hash = digested
                .then(|| {
                    old_value
                        .as_ref()
                        .zip(chunk.get_meta(local_idx, key_id))
                        .map(|(v, m)| entry_digest(coord, key, v, &m))
                })
                .flatten();
            let wins = apply_remove_if_newer(
                chunk,
                local_idx,
                key_id,
                modified_at_ms,
                stamp,
                keep_tombstones,
            );
            ((wins, old_value, old_hash), wins)
        })?;
        if indexed && wins {
            self.record_index_change(key_id, &Coord::from(coord), old_value.as_ref(), None);
        }
        if digested && wins {
            self.apply_digest_update(old_hash, None);
        }
        Ok(wins)
    }

    /// `apply_replicated`/`apply_replicated_remove` for a whole batch, in
    /// order, with each chunk the batch touches read and written once
    /// rather than once per change -- a catch-up streams thousands of
    /// changes, often many to the same chunk, and rewriting the whole
    /// chunk file for each one would make that quadratic. Returns one
    /// result per change, in input order: whether it was applied, or why
    /// it couldn't be (a bad coordinate, a type mismatch, a failed write).
    pub fn apply_changes(&self, changes: Vec<Change>) -> Vec<io::Result<bool>> {
        let mut results: Vec<Option<io::Result<bool>>> = (0..changes.len()).map(|_| None).collect();
        // Chunks in first-touched order; each one's changes in input order.
        let mut chunk_order: Vec<ChunkKey> = Vec::new();
        let mut by_chunk: HashMap<ChunkKey, Vec<PendingChange>> = HashMap::new();
        for (i, change) in changes.into_iter().enumerate() {
            let (ckey, local_idx) = match self.split(&change.coord) {
                Ok(split) => split,
                Err(e) => {
                    results[i] = Some(Err(e));
                    continue;
                }
            };
            let key_id = match &change.kind {
                ChangeKind::Set(value, _) => {
                    match self.schema().intern(&change.key, value.value_type()) {
                        Ok(key_id) => key_id,
                        Err(e) => {
                            results[i] = Some(Err(e));
                            continue;
                        }
                    }
                }
                // Same as `apply_replicated_remove`: a key never interned
                // here has nothing to remove and no id for a tombstone.
                ChangeKind::Removed(_) => match self.schema().id_for_key(&change.key) {
                    Some(key_id) => key_id,
                    None => {
                        results[i] = Some(Ok(false));
                        continue;
                    }
                },
            };
            by_chunk
                .entry(ckey.clone())
                .or_insert_with(|| {
                    chunk_order.push(ckey);
                    Vec::new()
                })
                .push((i, local_idx, key_id, change.key, change.kind, change.stamp));
        }

        let keep_tombstones = self.tombstone_retention.is_some();
        let digested = self.digest_enabled();
        for ckey in chunk_order {
            let group = by_chunk.remove(&ckey).unwrap_or_default();
            let indices: Vec<usize> = group.iter().map(|(i, ..)| *i).collect();
            // Index updates for keys with a secondary index (see
            // `create_index`) are collected here and applied after the
            // chunk's lock is released below, rather than from inside the
            // closure -- `ValueIndex` has its own lock, independent of the
            // chunk's, so there's no ordering requirement, but doing it
            // outside keeps the write-through chunk lock held for the
            // shortest time possible.
            let outcome = self.with_chunk_maybe_write(&ckey, |chunk| {
                let mut applied: Vec<bool> = Vec::with_capacity(group.len());
                let mut index_updates: Vec<(usize, u32, Option<Value>, Option<Value>)> =
                    Vec::new();
                let mut digest_updates: Vec<(Option<u64>, Option<u64>)> = Vec::new();
                for (_, local_idx, key_id, key, kind, stamp) in group {
                    let indexed = self.value_index.is_indexed(key_id);
                    let old_value = (indexed || digested)
                        .then(|| chunk.get(local_idx, key_id))
                        .flatten();
                    let old_hash = digested
                        .then(|| {
                            old_value.as_ref().zip(chunk.get_meta(local_idx, key_id)).map(
                                |(v, m)| entry_digest(&self.unsplit(&ckey, local_idx), &key, v, &m),
                            )
                        })
                        .flatten();
                    let wins = match kind {
                        ChangeKind::Set(value, meta) => {
                            let new_for_index = indexed.then(|| value.clone());
                            let new_for_digest = digested.then(|| value.clone());
                            let wins =
                                apply_set_if_newer(chunk, local_idx, key_id, value, meta, stamp);
                            if indexed && wins {
                                index_updates.push((
                                    local_idx,
                                    key_id,
                                    old_value.clone(),
                                    new_for_index,
                                ));
                            }
                            if digested && wins {
                                let new_hash = new_for_digest.map(|v| {
                                    entry_digest(&self.unsplit(&ckey, local_idx), &key, &v, &meta)
                                });
                                digest_updates.push((old_hash, new_hash));
                            }
                            wins
                        }
                        ChangeKind::Removed(at_ms) => {
                            let wins = apply_remove_if_newer(
                                chunk,
                                local_idx,
                                key_id,
                                at_ms,
                                stamp,
                                keep_tombstones,
                            );
                            if indexed && wins {
                                index_updates.push((local_idx, key_id, old_value.clone(), None));
                            }
                            if digested && wins {
                                digest_updates.push((old_hash, None));
                            }
                            wins
                        }
                    };
                    applied.push(wins);
                }
                let changed = applied.iter().any(|&a| a);
                ((applied, index_updates, digest_updates), changed)
            });
            match outcome {
                Ok((applied, index_updates, digest_updates)) => {
                    for (local_idx, key_id, old, new) in index_updates {
                        self.record_index_change(
                            key_id,
                            &self.unsplit(&ckey, local_idx),
                            old.as_ref(),
                            new.as_ref(),
                        );
                    }
                    for (old_hash, new_hash) in digest_updates {
                        self.apply_digest_update(old_hash, new_hash);
                    }
                    for (i, applied) in indices.into_iter().zip(applied) {
                        results[i] = Some(Ok(applied));
                    }
                }
                Err(e) => {
                    for i in indices {
                        results[i] = Some(Err(io::Error::new(e.kind(), e.to_string())));
                    }
                }
            }
        }
        results
            .into_iter()
            .map(|r| r.expect("every change gets a result"))
            .collect()
    }

    /// Removes `key` from this cell, returning when -- the time also
    /// recorded in its tombstone, if this world keeps them (see
    /// `with_tombstone_retention`), and the timestamp a replicating caller
    /// should ship -- or `None` if there was no value to remove.
    pub fn remove(&self, coord: &[i32], key: &str) -> io::Result<Option<u64>> {
        Ok(self
            .remove_stamped(coord, key, &mut || Stamp::NONE)?
            .map(|(at_ms, _)| at_ms))
    }

    /// `remove`, with the removal's origin taken from `stamp` -- called
    /// exactly once if there was a value to remove, not at all otherwise.
    /// Returns the removal time and stamp, for a replicating caller to
    /// ship.
    pub fn remove_stamped(
        &self,
        coord: &[i32],
        key: &str,
        stamp: &mut dyn FnMut() -> Stamp,
    ) -> io::Result<Option<(u64, Stamp)>> {
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
            return Ok(None);
        };
        let at_ms = now_ms();
        let keep_tombstones = self.tombstone_retention.is_some();
        let indexed = self.value_index.is_indexed(key_id);
        let digested = self.digest_enabled();
        let (removed, old_value, old_hash) = self.with_chunk_maybe_write(&ckey, |chunk| {
            let old_value = (indexed || digested).then(|| chunk.get(local_idx, key_id)).flatten();
            let old_hash = digested
                .then(|| {
                    old_value
                        .as_ref()
                        .zip(chunk.get_meta(local_idx, key_id))
                        .map(|(v, m)| entry_digest(coord, key, v, &m))
                })
                .flatten();
            let removed = remove_stamping(chunk, local_idx, key_id, at_ms, keep_tombstones, stamp);
            let changed = removed.is_some();
            ((removed, old_value, old_hash), changed)
        })?;
        if indexed && removed.is_some() {
            self.record_index_change(key_id, &Coord::from(coord), old_value.as_ref(), None);
        }
        if digested && removed.is_some() {
            self.apply_digest_update(old_hash, None);
        }
        crate::logger::info(format!("removed key '{key}' at {coord:?}"));
        Ok(removed.map(|stamp| (at_ms, stamp)))
    }

    /// Checks that `region`'s origin and extent both have this world's axis
    /// count -- *not* just that they match each other, since `Region::new`
    /// itself doesn't enforce that (see its doc comment) -- that `extent`
    /// has no negative component (meaningless as a size), and that it fits
    /// inside the world's `axis_bounds()`. Widening to `i64` for the fit
    /// check means this can't overflow `i32` along the way, unlike a plain
    /// `origin + extent` in `i32`.
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
        if let Some(&bad) = region.extent.iter().find(|&&len| len < 0) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("region {region:?} has a negative extent component ({bad})"),
            ));
        }
        let (lo_bound, hi_bound) = self.axis_bounds();
        let in_range = |lo: i32, len: i32| {
            let lo = lo as i64;
            lo >= lo_bound && lo + len as i64 <= hi_bound
        };
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
                    "region {region:?} doesn't fit in a {}^{} world with range [{lo_bound}, {hi_bound})",
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
    /// Returns each cell's resulting `CellMeta`, in the same `RegionIter`
    /// order as `values`/`get_region`'s result -- same reasoning as
    /// `set`'s own return value.
    pub fn set_region(
        &self,
        region: &Region,
        key: &str,
        values: &[Value],
    ) -> io::Result<Vec<CellMeta>> {
        Ok(self
            .set_region_stamped(region, key, values, &mut || Stamp::NONE)?
            .into_iter()
            .map(|(meta, _)| meta)
            .collect())
    }

    /// `set_region`, with each cell's write stamped by a call to `stamp`
    /// (once per cell written). Returns each cell's `CellMeta` and stamp,
    /// in the same order as `values`.
    pub fn set_region_stamped(
        &self,
        region: &Region,
        key: &str,
        values: &[Value],
        stamp: &mut dyn FnMut() -> Stamp,
    ) -> io::Result<Vec<(CellMeta, Stamp)>> {
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
            return Ok(Vec::new());
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
        let now_ms = now_ms();
        let indexed = self.value_index.is_indexed(key_id);
        let digested = self.digest_enabled();
        let mut metas = vec![
            (
                CellMeta {
                    created_at_ms: 0,
                    modified_at_ms: 0,
                    version: 0
                },
                Stamp::NONE
            );
            volume
        ];
        let mut index_updates: Vec<(Coord, Option<Value>, Option<Value>)> = Vec::new();
        let mut digest_updates: Vec<(Option<u64>, Option<u64>)> = Vec::new();
        for (ckey, cells) in self.group_region_by_chunk(region)? {
            let (updates, deltas) = self.with_chunk_write(&ckey, |chunk| {
                let mut updates = Vec::new();
                let mut deltas = Vec::new();
                for (i, local_idx) in cells {
                    let old_value = (indexed || digested)
                        .then(|| chunk.get(local_idx, key_id))
                        .flatten();
                    let old_hash = digested
                        .then(|| {
                            old_value.as_ref().zip(chunk.get_meta(local_idx, key_id)).map(
                                |(v, m)| entry_digest(&self.unsplit(&ckey, local_idx), key, v, &m),
                            )
                        })
                        .flatten();
                    let stamp = stamp();
                    let meta =
                        chunk.set_stamped(local_idx, key_id, values[i].clone(), now_ms, stamp);
                    metas[i] = (meta, stamp);
                    if indexed {
                        updates.push((
                            self.unsplit(&ckey, local_idx),
                            old_value,
                            Some(values[i].clone()),
                        ));
                    }
                    if digested {
                        let new_hash = entry_digest(
                            &self.unsplit(&ckey, local_idx),
                            key,
                            &values[i],
                            &meta,
                        );
                        deltas.push((old_hash, Some(new_hash)));
                    }
                }
                (updates, deltas)
            })?;
            index_updates.extend(updates);
            digest_updates.extend(deltas);
        }
        for (coord, old, new) in index_updates {
            self.record_index_change(key_id, &coord, old.as_ref(), new.as_ref());
        }
        for (old_hash, new_hash) in digest_updates {
            self.apply_digest_update(old_hash, new_hash);
        }
        crate::logger::info(format!(
            "set region {region:?} key '{key}' from {volume} per-cell values"
        ));
        Ok(metas)
    }

    /// Removes `key` from every cell in `region`, spanning chunks and
    /// partial chunks exactly like `get_region`. Unlike the single-cell
    /// `remove`, this logs one summary line for the whole region rather
    /// than one line per cell, so clearing a large region doesn't flood
    /// `kblockdblib.log`. Reports which cells actually held a value (see
    /// `RemovedCells`); every removal shares one timestamp.
    pub fn remove_region(&self, region: &Region, key: &str) -> io::Result<RemovedCells> {
        self.remove_region_stamped(region, key, &mut || Stamp::NONE)
    }

    /// `remove_region`, with each removal stamped by a call to `stamp`
    /// (once per cell that held a value).
    pub fn remove_region_stamped(
        &self,
        region: &Region,
        key: &str,
        stamp: &mut dyn FnMut() -> Stamp,
    ) -> io::Result<RemovedCells> {
        self.check_region(region)?;
        let at_ms = now_ms();
        let mut removed = RemovedCells {
            at_ms,
            cells: Vec::new(),
        };
        if region.volume() == 0 {
            return Ok(removed);
        }
        let Some(key_id) = self.schema().id_for_key(key) else {
            crate::logger::warn(format!(
                "remove_region() called with unknown key '{key}' at {region:?} -- no-op"
            ));
            return Ok(removed);
        };
        let keep_tombstones = self.tombstone_retention.is_some();
        let indexed = self.value_index.is_indexed(key_id);
        let digested = self.digest_enabled();
        for (ckey, cells) in self.group_region_by_chunk(region)? {
            let removed_here = self.with_chunk_maybe_write(&ckey, |chunk| {
                let removed_here: Vec<(usize, Stamp, Option<Value>, Option<u64>)> = cells
                    .into_iter()
                    .filter_map(|(_, local_idx)| {
                        let old_value = (indexed || digested)
                            .then(|| chunk.get(local_idx, key_id))
                            .flatten();
                        let old_hash = digested
                            .then(|| {
                                old_value.as_ref().zip(chunk.get_meta(local_idx, key_id)).map(
                                    |(v, m)| {
                                        entry_digest(&self.unsplit(&ckey, local_idx), key, v, &m)
                                    },
                                )
                            })
                            .flatten();
                        remove_stamping(chunk, local_idx, key_id, at_ms, keep_tombstones, stamp)
                            .map(|stamp| (local_idx, stamp, old_value, old_hash))
                    })
                    .collect();
                let changed = !removed_here.is_empty();
                (removed_here, changed)
            })?;
            if indexed {
                for (local_idx, _, old_value, _) in &removed_here {
                    self.record_index_change(
                        key_id,
                        &self.unsplit(&ckey, *local_idx),
                        old_value.as_ref(),
                        None,
                    );
                }
            }
            if digested {
                for (_, _, _, old_hash) in &removed_here {
                    self.apply_digest_update(*old_hash, None);
                }
            }
            removed.cells.extend(
                removed_here
                    .into_iter()
                    .map(|(local_idx, stamp, ..)| (self.unsplit(&ckey, local_idx), stamp)),
            );
        }
        crate::logger::info(format!(
            "removed region {region:?} key '{key}' ({} cells, {} held a value)",
            region.volume(),
            removed.cells.len()
        ));
        Ok(removed)
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

/// One change in `World::apply_changes`, resolved against its chunk:
/// (input index, local cell index, key id, key name, the change, its
/// stamp). The key name is carried alongside the id (a small, cheap
/// clone) only so a digest-enabled world can compute `entry_digest`
/// without a second schema lookup per change.
type PendingChange = (usize, usize, u32, String, ChangeKind, Stamp);

/// Converts `value` to `target`'s type if it can possibly equal a value of
/// that type, mirroring the numeric cross-type equality `kblockdbquery`'s
/// `eval_compare`/`compare_f64` already allows for a `WHERE key = <literal>`
/// comparison -- see `World::lookup_eq`'s doc comment. `None` means `value`
/// can never equal anything `target`-typed (a fractional `F64` compared
/// against an `I64` column, or a `Str`/`Bool` mismatch).
fn normalize_value_for_type(value: &Value, target: ValueType) -> Option<Value> {
    if value.value_type() == target {
        return Some(value.clone());
    }
    match (value, target) {
        // Every `i64` is exactly representable as `f64` (well inside its
        // 2^53 exact-integer range for any realistic coordinate/attribute
        // value), so this direction never loses precision.
        (Value::I64(n), ValueType::F64) => Some(Value::F64(*n as f64)),
        (Value::F64(f), ValueType::I64)
            if f.fract() == 0.0 && *f >= i64::MIN as f64 && *f <= i64::MAX as f64 =>
        {
            Some(Value::I64(*f as i64))
        }
        _ => None,
    }
}

/// What last-write-wins compares a cell/key's current state by: when it
/// was last written (a value's `modified_at_ms`, or a tombstone's removal
/// time) then, to break a tie the same way on every node, the write's
/// stamp. `None` if there's neither a value nor a tombstone.
fn current_version(chunk: &Chunk, local_idx: usize, key_id: u32) -> Option<(u64, Stamp)> {
    match chunk.get_meta(local_idx, key_id) {
        Some(meta) => Some((
            meta.modified_at_ms,
            chunk.stamp_at(local_idx, key_id).unwrap_or_default(),
        )),
        None => chunk.tombstone_at(local_idx, key_id),
    }
}

/// Last-write-wins for a replicated set: applies it if `(modified_at_ms,
/// stamp)` is newer than this cell/key's current value or tombstone.
/// Returns whether it was applied. Shared by `apply_replicated` and
/// `apply_changes`.
fn apply_set_if_newer(
    chunk: &mut Chunk,
    local_idx: usize,
    key_id: u32,
    value: Value,
    meta: CellMeta,
    stamp: Stamp,
) -> bool {
    let wins = current_version(chunk, local_idx, key_id)
        .is_none_or(|current| (meta.modified_at_ms, stamp) > current);
    if wins {
        chunk.set_replicated(local_idx, key_id, value, meta, stamp);
    }
    wins
}

/// Last-write-wins for a replicated removal -- see
/// `World::apply_replicated_remove`. Shared with `apply_changes`.
fn apply_remove_if_newer(
    chunk: &mut Chunk,
    local_idx: usize,
    key_id: u32,
    modified_at_ms: u64,
    stamp: Stamp,
    keep_tombstones: bool,
) -> bool {
    let wins = match current_version(chunk, local_idx, key_id) {
        Some(current) => (modified_at_ms, stamp) > current,
        None => keep_tombstones,
    };
    if wins {
        chunk.remove(local_idx, key_id);
        if keep_tombstones {
            chunk.put_tombstone(local_idx, key_id, modified_at_ms, stamp);
        }
    }
    wins
}

/// A local removal: removes the value (leaving a tombstone if the world
/// keeps them) and, only if there was one, takes a stamp for it --
/// returned for the caller to replicate.
fn remove_stamping(
    chunk: &mut Chunk,
    local_idx: usize,
    key_id: u32,
    at_ms: u64,
    keep_tombstones: bool,
    stamp: &mut dyn FnMut() -> Stamp,
) -> Option<Stamp> {
    if !chunk.remove(local_idx, key_id) {
        return None;
    }
    let stamp = stamp();
    if keep_tombstones {
        chunk.put_tombstone(local_idx, key_id, at_ms, stamp);
    }
    Some(stamp)
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

/// Recursively walks `dir` (mirrors `accumulate_chunk_stats`'s traversal,
/// but tracks the numeric path segments down to each `.chunk` file, since
/// `list_cells` needs each chunk's actual key -- `<root>/<c0>/<c1>/.../
/// <c_{axes-1}>.chunk`, one path segment per axis, see `World`'s "Layout on
/// disk" doc comment -- not just a count of them.
///
/// `prefix` holds the directory segments seen so far, pushed before
/// recursing into a numeric subdirectory and popped after, so it's back to
/// its caller's value once this returns. A directory or file name that
/// doesn't parse as an `i32` (a chunk key can be negative now -- see
/// `World::split` -- so a leading `-` is expected, not unusual; anything
/// else failing to parse is unexpected, but not this function's place to
/// fail over) is silently skipped, same tolerance-for-surprises philosophy
/// as `accumulate_chunk_stats`.
fn collect_chunk_keys(
    dir: &Path,
    axes: usize,
    prefix: &mut Vec<i32>,
    out: &mut Vec<Coord>,
) -> io::Result<()> {
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
        let path = entry.path();
        if file_type.is_dir() {
            let Some(n) = path
                .file_name()
                .and_then(|s| s.to_str())
                .and_then(|s| s.parse::<i32>().ok())
            else {
                continue;
            };
            prefix.push(n);
            collect_chunk_keys(&path, axes, prefix, out)?;
            prefix.pop();
            continue;
        }
        if path.extension().is_none_or(|ext| ext != "chunk") {
            continue;
        }
        let Some(n) = path
            .file_stem()
            .and_then(|s| s.to_str())
            .and_then(|s| s.parse::<i32>().ok())
        else {
            continue;
        };
        if prefix.len() + 1 == axes {
            let mut ckey = prefix.clone();
            ckey.push(n);
            out.push(Coord::from(ckey));
        }
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
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
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

    fn coord3(x: i32, y: i32, z: i32) -> Coord {
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
        // 100 cells per axis, centered on zero, is the range [-50, 50) --
        // not a multiple of chunk_dim=4, so it spans 26 chunk-key buckets
        // (-13..=12), not the 25 a naive 100/4 would suggest -- see
        // `chunks_per_axis`'s doc comment.
        assert_eq!(w.chunks_per_axis(), 26);

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
            w.get(&coord3(WORLD_DIM as i32, 0, 0), "never-set")
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
            w.remove(&coord3(WORLD_DIM as i32, 0, 0), "never-set")
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

        let err = w.set(&coord3(WORLD_DIM as i32, 0, 0), "material", Value::I64(1));
        assert_eq!(err.unwrap_err().kind(), io::ErrorKind::InvalidInput);
        assert_eq!(w.schema_len(), 0, "key must not be interned on failure");
    }

    #[test]
    fn negative_coordinates_round_trip_through_get_and_set() {
        let dir = TempDir::new("negative-coords");
        let w = create(&dir);
        let c = coord3(-1, -2, -3);

        w.set(&c, "material", Value::Str("stone".into())).unwrap();
        assert_eq!(
            w.get(&c, "material").unwrap(),
            Some(Value::Str("stone".into()))
        );
        // A different (positive) cell is untouched.
        assert_eq!(w.get(&coord3(1, 2, 3), "material").unwrap(), None);
    }

    #[test]
    fn coordinates_below_the_lower_bound_are_rejected() {
        let dir = TempDir::new("negative-coords-oob");
        let w = create(&dir); // WORLD_DIM=10_000 -> valid range is [-5000, 5000)

        assert_eq!(
            w.get(&coord3(-5_001, 0, 0), "material").unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        // Just inside the lower edge is fine.
        assert_eq!(w.get(&coord3(-5_000, 0, 0), "material").unwrap(), None);
    }

    #[test]
    fn chunk_splitting_is_consistent_across_the_zero_boundary() {
        // Regression: Rust's `/`/`%` truncate toward zero for signed
        // integers, which would put -1 and -32 in inconsistent chunks (or
        // give a negative local index) -- World::split must use
        // div_euclid/rem_euclid instead, so chunk boundaries stay evenly
        // spaced across zero the same as everywhere else.
        let dir = TempDir::new("chunk-split-zero");
        let w = create(&dir); // DEFAULT_CHUNK_DIM = 32

        // -32 and -1 land in the chunk just below zero; 0 and 31 land in
        // the chunk at/above zero -- -1 and 0, one cell apart, are in
        // different chunks.
        w.set(&coord3(-1, 0, 0), "k", Value::I64(-1)).unwrap();
        w.set(&coord3(0, 0, 0), "k", Value::I64(0)).unwrap();
        w.set(&coord3(-32, 0, 0), "k", Value::I64(-32)).unwrap();
        w.set(&coord3(31, 0, 0), "k", Value::I64(31)).unwrap();

        assert_eq!(w.get(&coord3(-1, 0, 0), "k").unwrap(), Some(Value::I64(-1)));
        assert_eq!(w.get(&coord3(0, 0, 0), "k").unwrap(), Some(Value::I64(0)));
        assert_eq!(
            w.get(&coord3(-32, 0, 0), "k").unwrap(),
            Some(Value::I64(-32))
        );
        assert_eq!(w.get(&coord3(31, 0, 0), "k").unwrap(), Some(Value::I64(31)));

        // list_cells sorts ascending by coordinate -- negative components
        // must sort before non-negative ones, not after (which a naive
        // unsigned-style comparison would get wrong).
        let cells = w.list_cells().unwrap();
        let coords: Vec<Coord> = cells.into_iter().map(|c| c.coord).collect();
        assert_eq!(
            coords,
            vec![
                coord3(-32, 0, 0),
                coord3(-1, 0, 0),
                coord3(0, 0, 0),
                coord3(31, 0, 0),
            ]
        );
    }

    #[test]
    fn set_then_get_roundtrips_each_value_type() {
        let dir = TempDir::new("set-get-types");
        let w = create(&dir);
        let c = coord3(1, 2, 3);

        w.set(&c, "material", Value::Str("stone".into())).unwrap();
        w.set(&c, "density", Value::F64(2.5)).unwrap();
        w.set(&c, "hardness", Value::I64(7)).unwrap();
        w.set(&c, "flammable", Value::Bool(false)).unwrap();

        assert_eq!(
            w.get(&c, "material").unwrap(),
            Some(Value::Str("stone".into()))
        );
        assert_eq!(w.get(&c, "density").unwrap(), Some(Value::F64(2.5)));
        assert_eq!(w.get(&c, "hardness").unwrap(), Some(Value::I64(7)));
        assert_eq!(w.get(&c, "flammable").unwrap(), Some(Value::Bool(false)));
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
    fn get_meta_of_a_never_set_cell_is_none() {
        let dir = TempDir::new("meta-never-set");
        let w = create(&dir);
        assert_eq!(w.get_meta(&coord3(0, 0, 0), "material").unwrap(), None);
    }

    #[test]
    fn a_fresh_set_starts_at_version_0() {
        let dir = TempDir::new("meta-fresh-set");
        let w = create(&dir);
        let c = coord3(1, 2, 3);

        w.set(&c, "material", Value::Str("stone".into())).unwrap();

        let meta = w.get_meta(&c, "material").unwrap().unwrap();
        assert_eq!(meta.version, 0);
        assert_eq!(meta.created_at_ms, meta.modified_at_ms);
        assert!(meta.created_at_ms > 0);
    }

    #[test]
    fn overwriting_increments_version_and_modified_but_not_created() {
        let dir = TempDir::new("meta-overwrite");
        let w = create(&dir);
        let c = coord3(4, 5, 6);

        w.set(&c, "material", Value::Str("stone".into())).unwrap();
        let first = w.get_meta(&c, "material").unwrap().unwrap();

        w.set(&c, "material", Value::Str("air".into())).unwrap();
        let second = w.get_meta(&c, "material").unwrap().unwrap();
        w.set(&c, "material", Value::Str("dirt".into())).unwrap();
        let third = w.get_meta(&c, "material").unwrap().unwrap();

        assert_eq!(second.created_at_ms, first.created_at_ms);
        assert_eq!(third.created_at_ms, first.created_at_ms);
        assert!(second.modified_at_ms >= first.modified_at_ms);
        assert!(third.modified_at_ms >= second.modified_at_ms);
        assert_eq!(first.version, 0);
        assert_eq!(second.version, 1);
        assert_eq!(third.version, 2);
    }

    #[test]
    fn removing_then_setting_again_resets_version_to_0() {
        let dir = TempDir::new("meta-remove-reset");
        let w = create(&dir);
        let c = coord3(2, 2, 2);

        w.set(&c, "material", Value::I64(1)).unwrap();
        w.set(&c, "material", Value::I64(2)).unwrap();
        assert_eq!(w.get_meta(&c, "material").unwrap().unwrap().version, 1);

        w.remove(&c, "material").unwrap();
        assert_eq!(w.get_meta(&c, "material").unwrap(), None);

        w.set(&c, "material", Value::I64(3)).unwrap();
        let meta = w.get_meta(&c, "material").unwrap().unwrap();
        assert_eq!(meta.version, 0);
        assert_eq!(meta.created_at_ms, meta.modified_at_ms);
    }

    // --- apply_replicated / apply_replicated_remove ---

    #[test]
    fn apply_replicated_writes_a_fresh_cell_with_the_given_meta_verbatim() {
        let dir = TempDir::new("replicated-fresh");
        let w = create(&dir);
        let c = coord3(1, 2, 3);
        let meta = CellMeta {
            created_at_ms: 100,
            modified_at_ms: 100,
            version: 5,
        };

        let applied = w
            .apply_replicated(&c, "material", Value::Str("stone".into()), meta)
            .unwrap();
        assert!(applied);
        assert_eq!(
            w.get(&c, "material").unwrap(),
            Some(Value::Str("stone".into()))
        );
        // Applied verbatim -- not re-derived the way a local `set` would
        // (which would start a fresh cell at version 0).
        assert_eq!(w.get_meta(&c, "material").unwrap(), Some(meta));
    }

    #[test]
    fn apply_replicated_discards_a_write_older_than_what_is_already_here() {
        let dir = TempDir::new("replicated-stale");
        let w = create(&dir);
        let c = coord3(1, 2, 3);
        let newer = CellMeta {
            created_at_ms: 100,
            modified_at_ms: 200,
            version: 1,
        };
        w.apply_replicated(&c, "material", Value::Str("granite".into()), newer)
            .unwrap();

        let older = CellMeta {
            created_at_ms: 50,
            modified_at_ms: 150,
            version: 9,
        };
        let applied = w
            .apply_replicated(&c, "material", Value::Str("stone".into()), older)
            .unwrap();

        assert!(!applied);
        assert_eq!(
            w.get(&c, "material").unwrap(),
            Some(Value::Str("granite".into()))
        );
        assert_eq!(w.get_meta(&c, "material").unwrap(), Some(newer));
    }

    #[test]
    fn apply_replicated_applies_a_write_newer_than_what_is_already_here() {
        let dir = TempDir::new("replicated-newer-wins");
        let w = create(&dir);
        let c = coord3(1, 2, 3);
        let older = CellMeta {
            created_at_ms: 50,
            modified_at_ms: 100,
            version: 0,
        };
        w.apply_replicated(&c, "material", Value::Str("stone".into()), older)
            .unwrap();

        let newer = CellMeta {
            created_at_ms: 50,
            modified_at_ms: 200,
            version: 1,
        };
        let applied = w
            .apply_replicated(&c, "material", Value::Str("granite".into()), newer)
            .unwrap();

        assert!(applied);
        assert_eq!(
            w.get(&c, "material").unwrap(),
            Some(Value::Str("granite".into()))
        );
    }

    #[test]
    fn apply_replicated_with_an_equal_timestamp_keeps_the_existing_value() {
        let dir = TempDir::new("replicated-tie");
        let w = create(&dir);
        let c = coord3(1, 2, 3);
        let meta = CellMeta {
            created_at_ms: 50,
            modified_at_ms: 100,
            version: 0,
        };
        w.apply_replicated(&c, "material", Value::Str("stone".into()), meta)
            .unwrap();

        let applied = w
            .apply_replicated(&c, "material", Value::Str("granite".into()), meta)
            .unwrap();

        assert!(!applied);
        assert_eq!(
            w.get(&c, "material").unwrap(),
            Some(Value::Str("stone".into()))
        );
    }

    #[test]
    fn apply_replicated_remove_removes_a_cell_newer_than_what_is_already_here() {
        let dir = TempDir::new("replicated-remove-wins");
        let w = create(&dir);
        let c = coord3(1, 2, 3);
        w.apply_replicated(
            &c,
            "material",
            Value::Str("stone".into()),
            CellMeta {
                created_at_ms: 50,
                modified_at_ms: 100,
                version: 0,
            },
        )
        .unwrap();

        let applied = w.apply_replicated_remove(&c, "material", 200).unwrap();
        assert!(applied);
        assert_eq!(w.get(&c, "material").unwrap(), None);
    }

    #[test]
    fn apply_replicated_remove_ignores_a_removal_older_than_what_is_already_here() {
        let dir = TempDir::new("replicated-remove-stale");
        let w = create(&dir);
        let c = coord3(1, 2, 3);
        w.apply_replicated(
            &c,
            "material",
            Value::Str("stone".into()),
            CellMeta {
                created_at_ms: 50,
                modified_at_ms: 200,
                version: 0,
            },
        )
        .unwrap();

        let applied = w.apply_replicated_remove(&c, "material", 100).unwrap();
        assert!(!applied);
        assert_eq!(
            w.get(&c, "material").unwrap(),
            Some(Value::Str("stone".into()))
        );
    }

    #[test]
    fn apply_replicated_remove_of_a_never_set_key_is_a_harmless_noop() {
        let dir = TempDir::new("replicated-remove-missing");
        let w = create(&dir);
        let c = coord3(1, 2, 3);
        assert!(!w.apply_replicated_remove(&c, "material", 100).unwrap());
    }

    #[test]
    fn get_with_meta_of_a_never_set_cell_is_none() {
        let dir = TempDir::new("meta-with-value-never-set");
        let w = create(&dir);
        assert_eq!(w.get_with_meta(&coord3(0, 0, 0), "material").unwrap(), None);
    }

    #[test]
    fn get_with_meta_matches_get_and_get_meta_separately() {
        let dir = TempDir::new("meta-with-value-matches");
        let w = create(&dir);
        let c = coord3(1, 2, 3);
        w.set(&c, "material", Value::Str("stone".into())).unwrap();
        w.set(&c, "material", Value::Str("air".into())).unwrap();

        let value = w.get(&c, "material").unwrap().unwrap();
        let meta = w.get_meta(&c, "material").unwrap().unwrap();
        let (v2, m2) = w.get_with_meta(&c, "material").unwrap().unwrap();

        assert_eq!(v2, value);
        assert_eq!(m2, meta);
    }

    #[test]
    fn a_different_cells_meta_is_independent() {
        let dir = TempDir::new("meta-independent");
        let w = create(&dir);
        let a = coord3(0, 0, 0);
        let b = coord3(1, 1, 1);

        w.set(&a, "material", Value::I64(1)).unwrap();
        w.set(&a, "material", Value::I64(2)).unwrap();
        w.set(&b, "material", Value::I64(9)).unwrap();

        assert_eq!(w.get_meta(&a, "material").unwrap().unwrap().version, 1);
        assert_eq!(w.get_meta(&b, "material").unwrap().unwrap().version, 0);
    }

    #[test]
    fn meta_persists_across_flush_and_reopen() {
        let dir = TempDir::new("meta-persist");
        let c = coord3(3, 3, 3);
        let before = {
            let w = create(&dir);
            w.set(&c, "material", Value::Str("stone".into())).unwrap();
            w.set(&c, "material", Value::Str("air".into())).unwrap();
            w.flush().unwrap();
            w.get_meta(&c, "material").unwrap().unwrap()
        };

        let w = World::open(&dir).unwrap();
        let after = w.get_meta(&c, "material").unwrap().unwrap();
        assert_eq!(after, before);
        assert_eq!(after.version, 1);
    }

    #[test]
    fn set_region_stamps_meta_for_every_cell_at_version_0() {
        let dir = TempDir::new("meta-set-region");
        let w = create(&dir);
        let region = Region::new(vec![0, 0, 0], vec![2, 2, 2]);
        let values: Vec<Value> = (0..region.volume() as i64).map(Value::I64).collect();

        w.set_region(&region, "material", &values).unwrap();

        for coord in RegionIter::new(&region) {
            let meta = w.get_meta(&coord, "material").unwrap().unwrap();
            assert_eq!(meta.version, 0);
            assert_eq!(meta.created_at_ms, meta.modified_at_ms);
        }
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
            .set(
                &coord3(-4_000, -4_000, -4_000),
                "temperature",
                Value::I64(20),
            )
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(err.to_string().contains("temperature"));

        // The failed set didn't write anything, and the original value is
        // untouched.
        assert_eq!(
            w.get(&coord3(-4_000, -4_000, -4_000), "temperature")
                .unwrap(),
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
        let far = coord3(DEFAULT_CHUNK_DIM as i32, 0, 0);
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
    fn region_iter_yields_every_coordinate_axis0_fastest() {
        let region = Region::new(coord3(10, 20, 30), coord3(2, 2, 1));
        let coords: Vec<Coord> = region.iter().collect();
        assert_eq!(
            coords,
            vec![
                coord3(10, 20, 30),
                coord3(11, 20, 30),
                coord3(10, 21, 30),
                coord3(11, 21, 30),
            ]
        );
    }

    #[test]
    fn region_iter_of_a_zero_extent_axis_is_empty() {
        let region = Region::new(coord3(0, 0, 0), coord3(5, 0, 5));
        assert_eq!(region.iter().count(), 0);
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
        let x0 = (DEFAULT_CHUNK_DIM - 5) as i32;
        let d = (DEFAULT_CHUNK_DIM + 10) as i32;
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

        let x0 = (DEFAULT_CHUNK_DIM - 2) as i32;
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

        let region = Region::new(coord3(WORLD_DIM as i32 - 1, 0, 0), coord3(2, 1, 1));
        assert_eq!(
            w.get_region(&region, "material").unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );

        // Overflowing i32 entirely must not panic or wrap around.
        let region = Region::new(coord3(i32::MAX - 1, 0, 0), coord3(5, 1, 1));
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

        // Same, at the opposite (negative) extreme.
        let region = Region::new(coord3(i32::MIN + 1, 0, 0), coord3(5, 1, 1));
        assert_eq!(
            w.get_region(&region, "material").unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn a_region_spanning_zero_round_trips() {
        let dir = TempDir::new("region-spans-zero");
        let w = create(&dir);

        let region = Region::new(coord3(-2, -2, -2), coord3(5, 5, 5)); // covers -2..3 on every axis
        w.set_region(
            &region,
            "material",
            &fill(&region, Value::Str("stone".into())),
        )
        .unwrap();

        let got = w.get_region(&region, "material").unwrap();
        assert_eq!(got.len(), region.volume() as usize);
        assert!(got.iter().all(|v| *v == Some(Value::Str("stone".into()))));

        // Just outside the region on the low and high corners: untouched.
        assert_eq!(w.get(&coord3(-3, -2, -2), "material").unwrap(), None);
        assert_eq!(w.get(&coord3(3, -2, -2), "material").unwrap(), None);
    }

    #[test]
    fn a_region_with_a_negative_extent_component_is_an_error() {
        let dir = TempDir::new("region-negative-extent");
        let w = create(&dir);

        let region = Region::new(coord3(0, 0, 0), coord3(-1, 2, 2));
        assert_eq!(
            w.get_region(&region, "material").unwrap_err().kind(),
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
        let c = coord3(4_999, 0, 0); // a chunk nothing has ever touched

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
        let chunk_coord = |n: i32| coord3(n * DEFAULT_CHUNK_DIM as i32, 0, 0);

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
    fn a_chunk_operation_holds_a_disk_io_permit_for_its_whole_duration() {
        // The narrow wiring question -- does `with_chunk` actually go
        // through `disk_io` at all? -- answered by looking from *inside*
        // the operation, where this thread knows it holds exactly one
        // permit itself. No threads and no clock involved, so this can't
        // be flaky: if the permit were dropped early (or never taken),
        // `available` would read back as the full capacity.
        let dir = TempDir::new("cap-permit-held");
        let w = World::create(&dir, AXES, WORLD_DIM, DEFAULT_CHUNK_DIM)
            .unwrap()
            .with_max_concurrent_disk_ops(4);
        assert_eq!(w.disk_io.available(), 4, "a permit is held before the call");

        let (ckey, _) = w.split(&coord3(1, 1, 1)).unwrap();
        let seen = w
            .with_chunk_maybe_write(&ckey, |_chunk| (w.disk_io.available(), false))
            .unwrap();
        assert_eq!(seen, 3, "the operation ran without holding a permit");
        assert_eq!(w.disk_io.available(), 4, "the permit wasn't released");
    }

    #[test]
    fn max_concurrent_disk_ops_actually_limits_concurrency() {
        // Observes real occupancy -- how many threads are inside a chunk
        // operation at the same moment -- rather than comparing wall-clock
        // times between a tight and a generous cap. The timing version of
        // this was genuinely flaky: on a fast filesystem the per-op work
        // is small enough that scheduling noise swamps the difference, and
        // it failed on FreeBSD with the two runs 2% apart. Occupancy is
        // the property actually being claimed, and counting it is exact.
        fn max_occupancy(cap: usize, workers: usize) -> usize {
            let dir = TempDir::new(&format!("cap-occupancy-{cap}"));
            let w = std::sync::Arc::new(
                World::create(&dir, AXES, WORLD_DIM, DEFAULT_CHUNK_DIM)
                    .unwrap()
                    .with_max_concurrent_disk_ops(cap),
            );
            let occupancy = std::sync::Arc::new(AtomicUsize::new(0));
            let max_seen = std::sync::Arc::new(AtomicUsize::new(0));
            // Released only once every worker has spawned, so they all
            // contend at once. It has to be waited on *outside* the chunk
            // operation: waiting inside would deadlock at `cap` < workers,
            // where some threads can't get in until others leave.
            let start = std::sync::Arc::new(std::sync::Barrier::new(workers));

            let handles: Vec<_> = (0..workers)
                .map(|i| {
                    let w = std::sync::Arc::clone(&w);
                    let occupancy = std::sync::Arc::clone(&occupancy);
                    let max_seen = std::sync::Arc::clone(&max_seen);
                    let start = std::sync::Arc::clone(&start);
                    thread::spawn(move || {
                        // Disjoint chunks, so nothing but `disk_io` can
                        // serialize these -- a shared chunk's own lock
                        // would otherwise be the thing under test.
                        let far = (i as i32 + 1) * DEFAULT_CHUNK_DIM as i32;
                        let (ckey, _) = w.split(&coord3(far, far, far)).unwrap();
                        start.wait();
                        w.with_chunk_maybe_write(&ckey, |_chunk| {
                            let now = occupancy.fetch_add(1, Ordering::SeqCst) + 1;
                            max_seen.fetch_max(now, Ordering::SeqCst);
                            // Long enough that every thread the cap allows
                            // in is still inside when the others arrive --
                            // without this, threads could file through one
                            // at a time and show an occupancy of 1 even
                            // uncapped.
                            thread::sleep(std::time::Duration::from_millis(20));
                            occupancy.fetch_sub(1, Ordering::SeqCst);
                            ((), false)
                        })
                        .unwrap();
                    })
                })
                .collect();
            for h in handles {
                h.join().unwrap();
            }
            max_seen.load(Ordering::SeqCst)
        }

        let workers = 8;
        assert_eq!(
            max_occupancy(1, workers),
            1,
            "cap=1 let more than one chunk operation run at once"
        );
        // The other direction, so this can't pass by the cap simply
        // blocking everything: a generous cap has to actually let work
        // overlap. Asserts >1 rather than exactly `workers` -- the OS owes
        // no guarantee that all 8 are scheduled simultaneously, but any
        // overlap at all is impossible if the cap were stuck at 1.
        assert!(
            max_occupancy(workers, workers) > 1,
            "cap={workers} never ran two chunk operations at once"
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

    #[test]
    fn list_cells_of_an_untouched_world_is_empty() {
        let dir = TempDir::new("list-cells-empty");
        let w = create(&dir);
        assert_eq!(w.list_cells().unwrap(), Vec::new());
    }

    #[test]
    fn list_cells_reports_a_single_cells_full_value_and_meta() {
        let dir = TempDir::new("list-cells-single");
        let w = create(&dir);
        w.set(&coord3(1, 2, 3), "material", Value::Str("stone".into()))
            .unwrap();

        let cells = w.list_cells().unwrap();
        assert_eq!(cells.len(), 1);
        assert_eq!(cells[0].coord, coord3(1, 2, 3));
        assert_eq!(cells[0].values.len(), 1);
        let (key, value, meta) = &cells[0].values[0];
        assert_eq!(key, "material");
        assert_eq!(*value, Value::Str("stone".into()));
        assert_eq!(meta.version, 0);
        assert_eq!(meta.created_at_ms, meta.modified_at_ms);
    }

    #[test]
    fn list_cells_groups_multiple_keys_at_the_same_cell_sorted_by_key_name() {
        let dir = TempDir::new("list-cells-multi-key");
        let w = create(&dir);
        let c = coord3(5, 5, 5);
        w.set(&c, "temperature", Value::F64(20.0)).unwrap();
        w.set(&c, "material", Value::Str("stone".into())).unwrap();

        let cells = w.list_cells().unwrap();
        assert_eq!(cells.len(), 1);
        let keys: Vec<&str> = cells[0].values.iter().map(|(k, _, _)| k.as_str()).collect();
        assert_eq!(keys, vec!["material", "temperature"]); // alphabetical
    }

    #[test]
    fn list_cells_is_sorted_ascending_by_coordinate_across_chunks() {
        let dir = TempDir::new("list-cells-sorted");
        let w = create(&dir);
        // Deliberately set out of order, and far enough apart (see
        // DEFAULT_CHUNK_DIM) to land in different chunks.
        w.set(&coord3(100, 0, 0), "k", Value::I64(2)).unwrap();
        w.set(&coord3(0, 0, 0), "k", Value::I64(0)).unwrap();
        w.set(&coord3(0, 100, 0), "k", Value::I64(1)).unwrap();

        let cells = w.list_cells().unwrap();
        let coords: Vec<Coord> = cells.into_iter().map(|c| c.coord).collect();
        assert_eq!(
            coords,
            vec![coord3(0, 0, 0), coord3(0, 100, 0), coord3(100, 0, 0)]
        );
    }

    #[test]
    fn list_cells_excludes_a_cell_after_its_only_key_is_removed() {
        let dir = TempDir::new("list-cells-remove");
        let w = create(&dir);
        let c = coord3(1, 1, 1);
        w.set(&c, "k", Value::I64(1)).unwrap();
        assert_eq!(w.list_cells().unwrap().len(), 1);

        w.remove(&c, "k").unwrap();
        assert_eq!(w.list_cells().unwrap(), Vec::new());
    }

    #[test]
    fn list_cells_reflects_a_removed_key_leaving_others_at_the_same_cell() {
        let dir = TempDir::new("list-cells-partial-remove");
        let w = create(&dir);
        let c = coord3(2, 2, 2);
        w.set(&c, "a", Value::I64(1)).unwrap();
        w.set(&c, "b", Value::I64(2)).unwrap();
        w.remove(&c, "a").unwrap();

        let cells = w.list_cells().unwrap();
        assert_eq!(cells.len(), 1);
        assert_eq!(cells[0].values.len(), 1);
        assert_eq!(cells[0].values[0].0, "b");
    }

    #[test]
    fn list_cells_persists_across_flush_and_reopen() {
        let dir = TempDir::new("list-cells-persist");
        {
            let w = create(&dir);
            w.set(&coord3(1, 2, 3), "material", Value::Str("stone".into()))
                .unwrap();
            w.flush().unwrap();
        }

        let w = World::open(&dir).unwrap();
        let cells = w.list_cells().unwrap();
        assert_eq!(cells.len(), 1);
        assert_eq!(cells[0].coord, coord3(1, 2, 3));
    }

    // --- Compression ---

    /// The on-disk bytes of the chunk file holding `c`, which must exist.
    fn chunk_bytes(w: &World, c: &Coord) -> Vec<u8> {
        let (ckey, _) = w.split(c).unwrap();
        fs::read(w.chunk_path(&ckey)).unwrap()
    }

    fn is_zstd(bytes: &[u8]) -> bool {
        bytes.starts_with(&ZSTD_MAGIC)
    }

    #[test]
    fn compression_is_off_by_default() {
        let dir = TempDir::new("compression-default-off");
        let w = create(&dir);
        assert!(!w.compression());

        let c = coord3(1, 2, 3);
        w.set(&c, "material", Value::Str("stone".into())).unwrap();
        assert!(
            !is_zstd(&chunk_bytes(&w, &c)),
            "a default world must keep writing plain, uncompressed chunk files"
        );
    }

    #[test]
    fn with_compression_writes_zstd_chunk_files_that_read_back() {
        let dir = TempDir::new("compression-roundtrip");
        let w = create(&dir).with_compression(true);
        assert!(w.compression());

        let c = coord3(1, 2, 3);
        w.set(&c, "material", Value::Str("stone".into())).unwrap();
        w.set(&c, "n", Value::I64(42)).unwrap();
        w.set(&c, "x", Value::F64(1.5)).unwrap();
        w.set(&c, "flag", Value::Bool(true)).unwrap();

        assert!(is_zstd(&chunk_bytes(&w, &c)));
        assert_eq!(
            w.get(&c, "material").unwrap(),
            Some(Value::Str("stone".into()))
        );

        // Not just the in-memory cache answering: a fresh handle decodes
        // the compressed file off disk.
        let reopened = World::open(&dir).unwrap().with_compression(true);
        assert_eq!(
            reopened.get(&c, "material").unwrap(),
            Some(Value::Str("stone".into()))
        );
        assert_eq!(reopened.get(&c, "n").unwrap(), Some(Value::I64(42)));
        assert_eq!(reopened.get(&c, "x").unwrap(), Some(Value::F64(1.5)));
        assert_eq!(reopened.get(&c, "flag").unwrap(), Some(Value::Bool(true)));
        assert_eq!(reopened.chunks_read_from_disk(), 1);
    }

    #[test]
    fn a_compressed_chunk_is_readable_with_compression_turned_back_off() {
        // The flag governs writes only -- turning it off must never strand
        // data already written compressed.
        let dir = TempDir::new("compression-read-with-flag-off");
        let c = coord3(5, 6, 7);
        {
            let w = create(&dir).with_compression(true);
            w.set(&c, "material", Value::Str("dirt".into())).unwrap();
            assert!(is_zstd(&chunk_bytes(&w, &c)));
        }

        let w = World::open(&dir).unwrap();
        assert!(!w.compression());
        assert_eq!(
            w.get(&c, "material").unwrap(),
            Some(Value::Str("dirt".into()))
        );
    }

    #[test]
    fn an_uncompressed_chunk_is_readable_with_compression_turned_on() {
        let dir = TempDir::new("compression-read-legacy");
        let c = coord3(8, 9, 10);
        {
            let w = create(&dir);
            w.set(&c, "material", Value::Str("sand".into())).unwrap();
            assert!(!is_zstd(&chunk_bytes(&w, &c)));
        }

        let w = World::open(&dir).unwrap().with_compression(true);
        assert_eq!(
            w.get(&c, "material").unwrap(),
            Some(Value::Str("sand".into()))
        );
    }

    #[test]
    fn one_world_can_hold_both_compressed_and_uncompressed_chunks() {
        let dir = TempDir::new("compression-mixed");
        let plain = coord3(0, 0, 0);
        let packed = coord3(2_000, 2_000, 2_000); // a different chunk
        {
            let w = create(&dir);
            w.set(&plain, "material", Value::Str("stone".into()))
                .unwrap();
        }
        {
            let w = World::open(&dir).unwrap().with_compression(true);
            w.set(&packed, "material", Value::Str("water".into()))
                .unwrap();
            assert!(!is_zstd(&chunk_bytes(&w, &plain)));
            assert!(is_zstd(&chunk_bytes(&w, &packed)));
        }

        let w = World::open(&dir).unwrap();
        assert_eq!(
            w.get(&plain, "material").unwrap(),
            Some(Value::Str("stone".into()))
        );
        assert_eq!(
            w.get(&packed, "material").unwrap(),
            Some(Value::Str("water".into()))
        );
    }

    #[test]
    fn rewriting_a_chunk_re_encodes_it_with_the_current_setting() {
        // Flipping the flag doesn't rewrite anything on its own, but the
        // next write to a chunk re-encodes that chunk either way.
        let dir = TempDir::new("compression-re-encode");
        let c = coord3(11, 12, 13);
        {
            let w = create(&dir).with_compression(true);
            w.set(&c, "material", Value::Str("stone".into())).unwrap();
            assert!(is_zstd(&chunk_bytes(&w, &c)));
        }

        let w = World::open(&dir).unwrap(); // compression off
        w.set(&c, "n", Value::I64(7)).unwrap();
        assert!(
            !is_zstd(&chunk_bytes(&w, &c)),
            "the rewrite should have produced a plain file"
        );
        // Both the pre-existing compressed value and the new one survived.
        let reopened = World::open(&dir).unwrap();
        assert_eq!(
            reopened.get(&c, "material").unwrap(),
            Some(Value::Str("stone".into()))
        );
        assert_eq!(reopened.get(&c, "n").unwrap(), Some(Value::I64(7)));
    }

    #[test]
    fn compression_shrinks_a_repetitive_chunk() {
        let dir = TempDir::new("compression-smaller");
        let c = coord3(14, 15, 16);
        let value = || Value::Str("stone".repeat(200));

        let plain_len = {
            let w = create(&dir);
            for i in 0..64 {
                w.set(&c, &format!("k{i}"), value()).unwrap();
            }
            chunk_bytes(&w, &c).len()
        };
        let packed_len = {
            let w = World::open(&dir).unwrap().with_compression(true);
            w.set(&c, "k0", value()).unwrap(); // rewrites the whole chunk
            chunk_bytes(&w, &c).len()
        };
        assert!(
            packed_len < plain_len,
            "compressed chunk ({packed_len} bytes) should be smaller than \
             the plain one ({plain_len} bytes)"
        );
    }

    #[test]
    fn emptying_a_compressed_chunk_removes_its_file() {
        let dir = TempDir::new("compression-remove");
        let w = create(&dir).with_compression(true);
        let c = coord3(17, 18, 19);
        w.set(&c, "material", Value::Str("stone".into())).unwrap();

        let (ckey, _) = w.split(&c).unwrap();
        let path = w.chunk_path(&ckey);
        assert!(path.exists());

        w.remove(&c, "material").unwrap();
        assert!(!path.exists());
        assert_eq!(w.get(&c, "material").unwrap(), None);
    }

    #[test]
    fn regions_round_trip_through_compressed_chunks() {
        let dir = TempDir::new("compression-region");
        let region = Region::new([100, 100, 100], [40, 2, 2]); // spans chunks
        {
            let w = create(&dir).with_compression(true);
            let values: Vec<Value> = (0..160).map(Value::I64).collect();
            w.set_region(&region, "n", &values).unwrap();
        }

        let w = World::open(&dir).unwrap();
        let read = w.get_region(&region, "n").unwrap();
        assert_eq!(read.len(), 160);
        assert_eq!(read[0], Some(Value::I64(0)));
        assert_eq!(read[159], Some(Value::I64(159)));
    }

    // --- Columns: add and remove ---

    #[test]
    fn add_column_declares_a_column_with_no_data() {
        let dir = TempDir::new("add-column");
        let w = create(&dir);
        w.add_column("material", ValueType::Str).unwrap();

        assert_eq!(
            w.columns(),
            vec![ColumnInfo {
                key: "material".to_string(),
                value_type: ValueType::Str,
            }]
        );
        // Declared, but with nothing written anywhere.
        assert_eq!(w.get(&coord3(1, 2, 3), "material").unwrap(), None);
        assert_eq!(w.stats().unwrap().total_chunks, 0);
    }

    #[test]
    fn add_column_fixes_the_type_for_later_sets() {
        let dir = TempDir::new("add-column-type");
        let w = create(&dir);
        w.add_column("hardness", ValueType::I64).unwrap();
        let c = coord3(1, 2, 3);

        let err = w
            .set(&c, "hardness", Value::Str("soft".into()))
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        w.set(&c, "hardness", Value::I64(7)).unwrap();
        assert_eq!(w.get(&c, "hardness").unwrap(), Some(Value::I64(7)));
    }

    #[test]
    fn add_column_rejects_a_duplicate() {
        let dir = TempDir::new("add-column-duplicate");
        let w = create(&dir);
        w.add_column("material", ValueType::Str).unwrap();

        let err = w.add_column("material", ValueType::Str).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
    }

    #[test]
    fn add_column_conflicts_with_a_column_created_by_set() {
        let dir = TempDir::new("add-column-after-set");
        let w = create(&dir);
        w.set(&coord3(1, 2, 3), "material", Value::Str("stone".into()))
            .unwrap();

        let err = w.add_column("material", ValueType::Str).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
    }

    #[test]
    fn columns_reports_every_key_whatever_created_it() {
        let dir = TempDir::new("columns-list");
        let w = create(&dir);
        w.add_column("density", ValueType::F64).unwrap();
        w.set(&coord3(1, 2, 3), "material", Value::Str("stone".into()))
            .unwrap();

        let columns = w.columns();
        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].key, "density");
        assert_eq!(columns[1].key, "material");
        assert_eq!(columns[1].value_type, ValueType::Str);
    }

    #[test]
    fn a_column_added_without_data_survives_a_reopen() {
        let dir = TempDir::new("add-column-persists");
        {
            let w = create(&dir);
            w.add_column("material", ValueType::Str).unwrap();
        }

        let w = World::open(&dir).unwrap();
        assert_eq!(w.columns().len(), 1);
        assert_eq!(w.columns()[0].value_type, ValueType::Str);
    }

    #[test]
    fn remove_column_erases_its_data_everywhere() {
        let dir = TempDir::new("remove-column");
        let w = create(&dir);
        // Spread across several distinct chunks.
        let coords = [coord3(1, 2, 3), coord3(500, 500, 500), coord3(9, 9, 9)];
        for c in &coords {
            w.set(c, "material", Value::Str("stone".into())).unwrap();
            w.set(c, "keep", Value::I64(1)).unwrap();
        }

        assert!(w.remove_column("material").unwrap());

        for c in &coords {
            assert_eq!(w.get(c, "material").unwrap(), None, "at {c:?}");
            // The other column in the very same cells is untouched.
            assert_eq!(w.get(c, "keep").unwrap(), Some(Value::I64(1)), "at {c:?}");
        }
        assert_eq!(
            w.columns(),
            vec![ColumnInfo {
                key: "keep".to_string(),
                value_type: ValueType::I64,
            }]
        );
    }

    #[test]
    fn remove_column_erases_data_on_disk_not_just_in_the_cache() {
        let dir = TempDir::new("remove-column-on-disk");
        let c = coord3(1, 2, 3);
        {
            let w = create(&dir);
            w.set(&c, "material", Value::Str("stone".into())).unwrap();
            w.set(&c, "keep", Value::I64(1)).unwrap();
            assert!(w.remove_column("material").unwrap());
        }

        // A brand new handle, with an empty cache, reading the rewritten
        // chunk file back off disk.
        let w = World::open(&dir).unwrap();
        assert_eq!(w.get(&c, "material").unwrap(), None);
        assert_eq!(w.get(&c, "keep").unwrap(), Some(Value::I64(1)));
        assert!(w.columns().iter().all(|col| col.key != "material"));
    }

    #[test]
    fn remove_column_deletes_a_chunk_file_it_empties() {
        let dir = TempDir::new("remove-column-empties-chunk");
        let w = create(&dir);
        let c = coord3(1, 2, 3);
        w.set(&c, "material", Value::Str("stone".into())).unwrap();

        let (ckey, _) = w.split(&c).unwrap();
        let path = w.chunk_path(&ckey);
        assert!(path.exists());

        assert!(w.remove_column("material").unwrap());
        assert!(
            !path.exists(),
            "a chunk with nothing left in it shouldn't stay on disk"
        );
        assert_eq!(w.stats().unwrap().total_chunks, 0);
    }

    #[test]
    fn remove_column_leaves_untouched_chunks_unwritten() {
        // The walk visits every chunk, but must only rewrite the ones that
        // actually held the column -- otherwise dropping a rare key costs
        // a rewrite of the whole world.
        let dir = TempDir::new("remove-column-no-pointless-writes");
        let w = create(&dir);
        w.set(&coord3(1, 2, 3), "doomed", Value::I64(1)).unwrap();
        for i in 0..4 {
            w.set(&coord3(500 + i * 100, 0, 0), "keep", Value::I64(1))
                .unwrap();
        }
        assert_eq!(w.stats().unwrap().total_chunks, 5);

        let before = w.chunks_written_to_disk();
        assert!(w.remove_column("doomed").unwrap());
        assert_eq!(
            w.chunks_written_to_disk() - before,
            0,
            "the only chunk holding the column was emptied, so it should \
             have been deleted rather than rewritten, and no other chunk \
             should have been written at all"
        );
        assert_eq!(w.stats().unwrap().total_chunks, 4);
    }

    #[test]
    fn remove_column_rewrites_only_the_chunks_that_held_the_column() {
        let dir = TempDir::new("remove-column-selective-writes");
        let w = create(&dir);
        // Two chunks hold both keys; two hold only `keep`.
        for i in 0..2 {
            let c = coord3(i * 100, 0, 0);
            w.set(&c, "doomed", Value::I64(1)).unwrap();
            w.set(&c, "keep", Value::I64(1)).unwrap();
        }
        for i in 2..4 {
            w.set(&coord3(i * 100, 0, 0), "keep", Value::I64(1))
                .unwrap();
        }

        let before = w.chunks_written_to_disk();
        assert!(w.remove_column("doomed").unwrap());
        assert_eq!(
            w.chunks_written_to_disk() - before,
            2,
            "only the two chunks that actually held 'doomed' should be rewritten"
        );
        assert_eq!(w.stats().unwrap().total_chunks, 4);
    }

    #[test]
    fn remove_column_of_an_unknown_key_is_false_and_changes_nothing() {
        let dir = TempDir::new("remove-column-missing");
        let w = create(&dir);
        w.set(&coord3(1, 2, 3), "keep", Value::I64(1)).unwrap();

        let before = w.chunks_written_to_disk();
        assert!(!w.remove_column("nonexistent").unwrap());
        assert_eq!(w.chunks_written_to_disk(), before);
        assert_eq!(w.columns().len(), 1);
    }

    #[test]
    fn a_removed_column_is_gone_from_list_cells() {
        let dir = TempDir::new("remove-column-list-cells");
        let w = create(&dir);
        let c = coord3(1, 2, 3);
        w.set(&c, "material", Value::Str("stone".into())).unwrap();
        w.set(&c, "keep", Value::I64(1)).unwrap();

        w.remove_column("material").unwrap();
        let cells = w.list_cells().unwrap();
        assert_eq!(cells.len(), 1);
        assert_eq!(cells[0].values.len(), 1);
        assert_eq!(cells[0].values[0].0, "keep");
    }

    #[test]
    fn a_cell_left_with_nothing_disappears_from_list_cells() {
        let dir = TempDir::new("remove-column-empties-cell");
        let w = create(&dir);
        w.set(&coord3(1, 2, 3), "material", Value::Str("stone".into()))
            .unwrap();

        w.remove_column("material").unwrap();
        assert!(w.list_cells().unwrap().is_empty());
    }

    #[test]
    fn a_removed_column_can_come_back_with_a_different_type() {
        let dir = TempDir::new("remove-column-readd");
        let w = create(&dir);
        let c = coord3(1, 2, 3);
        w.set(&c, "material", Value::Str("stone".into())).unwrap();
        w.remove_column("material").unwrap();

        // The old column was Str; this one is I64, which would be
        // impossible if the id had been reused.
        w.add_column("material", ValueType::I64).unwrap();
        assert_eq!(w.get(&c, "material").unwrap(), None);
        w.set(&c, "material", Value::I64(9)).unwrap();
        assert_eq!(w.get(&c, "material").unwrap(), Some(Value::I64(9)));
    }

    #[test]
    fn setting_a_removed_column_recreates_it() {
        let dir = TempDir::new("remove-column-then-set");
        let w = create(&dir);
        let c = coord3(1, 2, 3);
        w.set(&c, "material", Value::Str("stone".into())).unwrap();
        w.remove_column("material").unwrap();
        assert!(w.columns().is_empty());

        w.set(&c, "material", Value::Str("dirt".into())).unwrap();
        assert_eq!(
            w.get(&c, "material").unwrap(),
            Some(Value::Str("dirt".into()))
        );
        assert_eq!(w.columns().len(), 1);
    }

    #[test]
    fn remove_column_clears_region_reads_too() {
        let dir = TempDir::new("remove-column-region");
        let w = create(&dir);
        let region = Region::new([100, 100, 100], [40, 2, 2]); // spans chunks
        let values: Vec<Value> = (0..160).map(Value::I64).collect();
        w.set_region(&region, "n", &values).unwrap();

        assert!(w.remove_column("n").unwrap());
        let read = w.get_region(&region, "n").unwrap();
        assert_eq!(read.len(), 160);
        assert!(read.iter().all(Option::is_none));
    }

    #[test]
    fn remove_column_works_on_compressed_chunks() {
        let dir = TempDir::new("remove-column-compressed");
        let w = create(&dir).with_compression(true);
        let c = coord3(1, 2, 3);
        w.set(&c, "material", Value::Str("stone".into())).unwrap();
        w.set(&c, "keep", Value::I64(1)).unwrap();

        assert!(w.remove_column("material").unwrap());
        assert!(
            is_zstd(&chunk_bytes(&w, &c)),
            "still compressed after the rewrite"
        );

        let reopened = World::open(&dir).unwrap();
        assert_eq!(reopened.get(&c, "material").unwrap(), None);
        assert_eq!(reopened.get(&c, "keep").unwrap(), Some(Value::I64(1)));
    }

    #[test]
    fn add_column_rejects_a_key_the_schema_cannot_store() {
        let dir = TempDir::new("add-column-invalid-key");
        let w = create(&dir);
        let err = w.add_column("has\ttab", ValueType::Str).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(w.columns().is_empty());
    }

    /// Tombstones kept "forever", so tests can use small, fixed
    /// timestamps without them being purged as expired.
    fn create_with_tombstones(dir: &TempDir) -> World {
        create(dir).with_tombstone_retention(Some(Duration::MAX))
    }

    fn meta_at(ms: u64) -> CellMeta {
        CellMeta {
            created_at_ms: ms,
            modified_at_ms: ms,
            version: 0,
        }
    }

    #[test]
    fn remove_returns_its_time_only_when_a_value_was_removed() {
        let dir = TempDir::new("remove-returns-time");
        let w = create(&dir);
        let c = coord3(1, 2, 3);
        w.set(&c, "material", Value::Str("stone".into())).unwrap();
        let before = now_ms();
        let at = w
            .remove(&c, "material")
            .unwrap()
            .expect("a value was removed");
        assert!(at >= before);
        assert_eq!(w.remove(&c, "material").unwrap(), None);
        assert_eq!(w.remove(&c, "never-set").unwrap(), None);
    }

    #[test]
    fn without_retention_a_remove_leaves_no_tombstone() {
        let dir = TempDir::new("no-tombstones");
        let w = create(&dir);
        let c = coord3(1, 2, 3);
        w.apply_replicated(&c, "material", Value::Str("stone".into()), meta_at(100))
            .unwrap();
        w.remove(&c, "material").unwrap();
        // With no tombstone, an older replicated write is accepted again.
        assert!(w
            .apply_replicated(&c, "material", Value::Str("old".into()), meta_at(50))
            .unwrap());
        // ... and the chunk file is gone after the remove, as before.
        let mut seen = 0;
        w.changes_since(&VersionVector::new(), LEGACY, |batch| {
            seen += batch.len();
            true
        })
        .unwrap();
        assert_eq!(seen, 1);
    }

    #[test]
    fn a_late_older_write_loses_to_a_tombstone() {
        let dir = TempDir::new("tombstone-beats-late-write");
        let w = create_with_tombstones(&dir);
        let c = coord3(1, 2, 3);
        w.apply_replicated(&c, "material", Value::Str("stone".into()), meta_at(100))
            .unwrap();
        assert!(w.apply_replicated_remove(&c, "material", 200).unwrap());
        assert!(!w
            .apply_replicated(&c, "material", Value::Str("late".into()), meta_at(150))
            .unwrap());
        assert_eq!(w.get(&c, "material").unwrap(), None);
        // A write newer than the delete still wins.
        assert!(w
            .apply_replicated(&c, "material", Value::Str("new".into()), meta_at(300))
            .unwrap());
        assert_eq!(
            w.get(&c, "material").unwrap(),
            Some(Value::Str("new".into()))
        );
    }

    #[test]
    fn a_delete_arriving_before_its_write_still_wins() {
        let dir = TempDir::new("delete-before-write");
        let w = create_with_tombstones(&dir);
        // Interns the key, as any other write of it anywhere would have.
        w.apply_replicated(
            &coord3(9, 9, 9),
            "material",
            Value::Str("x".into()),
            meta_at(1),
        )
        .unwrap();
        let c = coord3(1, 2, 3);
        assert!(w.apply_replicated_remove(&c, "material", 200).unwrap());
        assert!(!w
            .apply_replicated(&c, "material", Value::Str("stone".into()), meta_at(100))
            .unwrap());
        assert_eq!(w.get(&c, "material").unwrap(), None);
    }

    #[test]
    fn tombstones_survive_a_reopen() {
        let dir = TempDir::new("tombstones-reopen");
        let c = coord3(1, 2, 3);
        {
            let w = create_with_tombstones(&dir);
            w.apply_replicated(&c, "material", Value::Str("stone".into()), meta_at(100))
                .unwrap();
            w.apply_replicated_remove(&c, "material", 200).unwrap();
        }
        let w = World::open(&dir)
            .unwrap()
            .with_tombstone_retention(Some(Duration::MAX));
        assert!(!w
            .apply_replicated(&c, "material", Value::Str("late".into()), meta_at(150))
            .unwrap());
    }

    #[test]
    fn expired_tombstones_are_purged_on_the_next_write_to_their_chunk() {
        let dir = TempDir::new("tombstones-expire");
        let w = create(&dir).with_tombstone_retention(Some(Duration::from_secs(60)));
        let c = coord3(1, 2, 3);
        w.apply_replicated(&c, "material", Value::Str("stone".into()), meta_at(100))
            .unwrap();
        // Long expired by the wall clock: purged as part of this very write.
        w.apply_replicated_remove(&c, "material", 200).unwrap();
        assert!(w
            .apply_replicated(&c, "material", Value::Str("late".into()), meta_at(150))
            .unwrap());
    }

    #[test]
    fn remove_region_reports_only_cells_that_held_a_value() {
        let dir = TempDir::new("remove-region-reports");
        let w = create_with_tombstones(&dir);
        w.set(&coord3(0, 0, 0), "material", Value::Str("a".into()))
            .unwrap();
        w.set(&coord3(40, 0, 0), "material", Value::Str("b".into()))
            .unwrap();
        let region = Region::new(coord3(0, 0, 0), coord3(64, 1, 1));
        let removed = w.remove_region(&region, "material").unwrap();
        let mut coords: Vec<Coord> = removed.cells.iter().map(|(c, _)| c.clone()).collect();
        coords.sort_by(|a, b| a.iter().cmp(b.iter()));
        assert_eq!(coords, vec![coord3(0, 0, 0), coord3(40, 0, 0)]);
        assert!(removed.at_ms > 0);

        let mut tombstones = Vec::new();
        w.changes_since(&VersionVector::new(), LEGACY, |batch| {
            tombstones.extend(batch);
            true
        })
        .unwrap();
        assert_eq!(tombstones.len(), 2);
        assert!(tombstones
            .iter()
            .all(|c| c.kind == ChangeKind::Removed(removed.at_ms)));

        assert!(w
            .remove_region(&region, "material")
            .unwrap()
            .cells
            .is_empty());
    }

    const A: u64 = 0xA;
    const B: u64 = 0xB;
    /// The legacy pseudo-origin these tests' worlds report as.
    const LEGACY: u64 = 0xC | crate::stamp::LEGACY_BIT;

    fn vector(entries: &[(u64, u64)]) -> VersionVector {
        entries.iter().copied().collect()
    }

    fn all_changes_since(w: &World, known: &VersionVector) -> Vec<Change> {
        let mut out = Vec::new();
        w.changes_since(known, LEGACY, |batch| {
            out.extend(batch);
            true
        })
        .unwrap();
        out.sort_by(|a, b| a.coord.iter().cmp(b.coord.iter()));
        out
    }

    /// A stamp source handing out `origin`'s seqs from `next`, recording
    /// how many it handed out.
    fn stamps_from(origin: u64, next: &mut u64) -> impl FnMut() -> Stamp + '_ {
        move || {
            *next += 1;
            Stamp::new(origin, *next - 1)
        }
    }

    #[test]
    fn stamped_writes_take_a_stamp_only_for_what_they_actually_write() {
        let dir = TempDir::new("stamped-writes");
        let w = create_with_tombstones(&dir);
        let mut next = 1;
        let (_, stamp) = w
            .set_stamped(
                &coord3(1, 0, 0),
                "k",
                Value::I64(1),
                &mut stamps_from(A, &mut next),
            )
            .unwrap();
        assert_eq!(stamp, Stamp::new(A, 1));

        // A type mismatch fails before writing: no stamp taken.
        assert!(w
            .set_stamped(
                &coord3(2, 0, 0),
                "k",
                Value::Bool(true),
                &mut stamps_from(A, &mut next)
            )
            .is_err());
        assert_eq!(next, 2);

        // Removing an empty cell takes none; removing a value takes one.
        assert_eq!(
            w.remove_stamped(&coord3(9, 0, 0), "k", &mut stamps_from(A, &mut next))
                .unwrap(),
            None
        );
        let (_, stamp) = w
            .remove_stamped(&coord3(1, 0, 0), "k", &mut stamps_from(A, &mut next))
            .unwrap()
            .unwrap();
        assert_eq!(stamp, Stamp::new(A, 2));

        let region = Region::new(coord3(0, 0, 0), coord3(3, 1, 1));
        let written = w
            .set_region_stamped(
                &region,
                "k",
                &vec![Value::I64(7); 3],
                &mut stamps_from(A, &mut next),
            )
            .unwrap();
        let mut seqs: Vec<u64> = written.iter().map(|(_, s)| s.seq).collect();
        seqs.sort();
        assert_eq!(seqs, vec![3, 4, 5]);

        w.remove(&coord3(1, 0, 0), "k").unwrap();
        let removed = w
            .remove_region_stamped(&region, "k", &mut stamps_from(A, &mut next))
            .unwrap();
        assert_eq!(removed.cells.len(), 2);
        assert_eq!(next, 8);
    }

    #[test]
    fn equal_timestamps_are_broken_by_stamp_the_same_in_either_order() {
        let c = coord3(1, 0, 0);
        let first = (Value::Str("from-a".into()), Stamp::new(A, 7));
        let second = (Value::Str("from-b".into()), Stamp::new(B, 1));
        for (tag, order) in [("ab", [&first, &second]), ("ba", [&second, &first])] {
            let dir = TempDir::new(&format!("tie-break-{tag}"));
            let w = create_with_tombstones(&dir);
            for (value, stamp) in order {
                w.apply_replicated_stamped(&c, "k", value.clone(), meta_at(100), *stamp)
                    .unwrap();
            }
            // B > A as an origin, so B's write wins at the same ms.
            assert_eq!(w.get(&c, "k").unwrap(), Some(Value::Str("from-b".into())));
        }
    }

    #[test]
    fn a_removal_at_the_same_ms_is_ordered_by_stamp_too() {
        let dir = TempDir::new("tie-break-remove");
        let w = create_with_tombstones(&dir);
        let c = coord3(1, 0, 0);
        w.apply_replicated_stamped(&c, "k", Value::I64(1), meta_at(100), Stamp::new(B, 1))
            .unwrap();
        assert!(!w
            .apply_replicated_remove_stamped(&c, "k", 100, Stamp::new(A, 1))
            .unwrap());
        assert!(w
            .apply_replicated_remove_stamped(&c, "k", 100, Stamp::new(B, 2))
            .unwrap());
        assert_eq!(w.get(&c, "k").unwrap(), None);
    }

    #[test]
    fn changes_since_reports_what_the_vector_lacks_by_key_name() {
        let dir = TempDir::new("changes-since");
        let w = create_with_tombstones(&dir);
        w.apply_replicated_stamped(
            &coord3(1, 0, 0),
            "material",
            Value::Str("old".into()),
            meta_at(100),
            Stamp::new(A, 1),
        )
        .unwrap();
        w.apply_replicated_stamped(
            &coord3(2, 0, 0),
            "material",
            Value::Str("new".into()),
            meta_at(300),
            Stamp::new(A, 2),
        )
        .unwrap();
        w.apply_replicated_stamped(
            &coord3(3, 0, 0),
            "material",
            Value::Str("gone".into()),
            meta_at(100),
            Stamp::new(B, 1),
        )
        .unwrap();
        w.apply_replicated_remove_stamped(&coord3(3, 0, 0), "material", 400, Stamp::new(B, 2))
            .unwrap();

        assert_eq!(
            all_changes_since(&w, &vector(&[(A, 1), (B, 1)])),
            vec![
                Change {
                    coord: coord3(2, 0, 0),
                    key: "material".into(),
                    kind: ChangeKind::Set(Value::Str("new".into()), meta_at(300)),
                    stamp: Stamp::new(A, 2),
                },
                Change {
                    coord: coord3(3, 0, 0),
                    key: "material".into(),
                    kind: ChangeKind::Removed(400),
                    stamp: Stamp::new(B, 2),
                },
            ]
        );
        assert_eq!(all_changes_since(&w, &VersionVector::new()).len(), 3);
        assert!(all_changes_since(&w, &vector(&[(A, 2), (B, 2)])).is_empty());
    }

    #[test]
    fn changes_since_skips_chunks_the_vector_covers_without_decoding_them() {
        let dir = TempDir::new("changes-since-skips");
        {
            let w = create_with_tombstones(&dir);
            // Three chunks (DEFAULT_CHUNK_DIM apart); only one has seq 3.
            for (x, seq) in [(0, 1), (64, 2), (128, 3)] {
                w.apply_replicated_stamped(
                    &coord3(x, 0, 0),
                    "material",
                    Value::I64(1),
                    meta_at(100),
                    Stamp::new(A, seq),
                )
                .unwrap();
            }
        }
        let w = World::open(&dir).unwrap();
        assert_eq!(all_changes_since(&w, &vector(&[(A, 2)])).len(), 1);
        assert_eq!(w.chunks_read_from_disk(), 1);
    }

    #[test]
    fn changes_since_reads_compressed_and_older_format_chunks() {
        let dir = TempDir::new("changes-since-formats");
        let w = create_with_tombstones(&dir).with_compression(true);
        w.apply_replicated_stamped(
            &coord3(0, 0, 0),
            "material",
            Value::I64(1),
            meta_at(500),
            Stamp::new(A, 5),
        )
        .unwrap();
        assert_eq!(all_changes_since(&w, &vector(&[(A, 4)])).len(), 1);
        assert!(all_changes_since(&w, &vector(&[(A, 5)])).is_empty());

        // A header-less file can't be skipped by its header, and its data
        // is legacy (`Stamp::NONE`): sent unless the vector has `0: 0`.
        let c = coord3(64, 0, 0);
        w.set(&c, "material", Value::I64(2)).unwrap();
        let (ckey, _) = w.split(&c).unwrap();
        let path = w.chunk_path(&ckey);
        let chunk = w.load_chunk(&ckey).unwrap();
        let mut buf = Vec::new();
        chunk.write_to(&mut buf).unwrap();
        // Rebuild it header-less and stamp-less: num_columns, then the
        // one column without its 16-byte stamp, then no tombstones.
        let header = 4 + 1 + 4 + 16;
        let column_head =
            4 + 4 + 1 + crate::chunk::chunk_cells(AXES, DEFAULT_CHUNK_DIM).div_ceil(8);
        let mut old = buf[header..header + column_head + 24].to_vec();
        old.extend_from_slice(&buf[header + column_head + 24 + 16..buf.len() - 4]);
        fs::write(&path, &old).unwrap();
        let reopened = World::open(&dir).unwrap();
        assert_eq!(reopened.get(&c, "material").unwrap(), Some(Value::I64(2)));
        assert_eq!(all_changes_since(&reopened, &vector(&[(A, 5)])).len(), 1);
        assert!(all_changes_since(&reopened, &vector(&[(A, 5), (LEGACY, 0)])).is_empty());
    }

    #[test]
    fn changes_since_stops_when_emit_returns_false() {
        let dir = TempDir::new("changes-since-stop");
        let w = create(&dir);
        for x in [0, 64, 128] {
            w.set(&coord3(x, 0, 0), "material", Value::I64(1)).unwrap();
        }
        let mut batches = 0;
        w.changes_since(&VersionVector::new(), LEGACY, |_| {
            batches += 1;
            false
        })
        .unwrap();
        assert_eq!(batches, 1);
    }

    /// Regression: `list_cells` used to read chunk files straight off disk,
    /// so a write rewriting the same file at the same moment could hand it
    /// half a file ("failed to fill whole buffer").
    #[test]
    fn list_cells_never_sees_a_half_written_chunk() {
        let dir = TempDir::new("list-cells-vs-writes");
        let w = std::sync::Arc::new(create(&dir));
        // A large chunk, so each rewrite takes long enough to overlap.
        let region = Region::new(coord3(0, 0, 0), coord3(32, 32, 16));
        let cells = region.volume() as usize;
        let values: Vec<Value> = (0..cells)
            .map(|i| Value::Str(format!("value-{i:08}")))
            .collect();
        w.set_region(&region, "material", &values).unwrap();

        let writer = {
            let w = w.clone();
            std::thread::spawn(move || {
                for i in 0..40 {
                    w.set(&coord3(0, 0, 0), "material", Value::Str(format!("v{i}")))
                        .unwrap();
                }
            })
        };
        for _ in 0..40 {
            assert_eq!(w.list_cells().unwrap().len(), cells);
        }
        writer.join().unwrap();
    }

    #[test]
    fn apply_changes_applies_a_batch_with_one_write_per_chunk() {
        let dir = TempDir::new("apply-changes");
        let w = create_with_tombstones(&dir);
        let set = |x: i32, ms: u64| Change {
            coord: coord3(x, 0, 0),
            key: "material".into(),
            kind: ChangeKind::Set(Value::I64(i64::from(x)), meta_at(ms)),
            stamp: Stamp::new(A, x as u64 + 1),
        };
        let mut changes: Vec<Change> = (0..20).map(|x| set(x, 100)).collect();
        changes.push(set(64, 100)); // a second chunk
        changes.push(Change {
            coord: coord3(3, 0, 0),
            key: "material".into(),
            kind: ChangeKind::Removed(200),
            stamp: Stamp::new(B, 1),
        });
        changes.push(set(3, 150)); // older than the removal just before it
        let before = w.chunks_written_to_disk();
        let results = w.apply_changes(changes);
        assert_eq!(w.chunks_written_to_disk() - before, 2);
        assert_eq!(results.len(), 23);
        assert!(results[..22].iter().all(|r| *r.as_ref().unwrap()));
        assert!(!results[22].as_ref().unwrap());
        assert_eq!(w.get(&coord3(3, 0, 0), "material").unwrap(), None);
        assert_eq!(
            w.get(&coord3(64, 0, 0), "material").unwrap(),
            Some(Value::I64(64))
        );
        // Stamps are stored: covered by A's 20 (+65) and B's 1, nothing's new.
        assert!(all_changes_since(&w, &vector(&[(A, 65), (B, 1)])).is_empty());
        assert_eq!(all_changes_since(&w, &vector(&[(A, 65)])).len(), 1);
    }

    #[test]
    fn apply_changes_reports_a_bad_change_without_failing_the_rest() {
        let dir = TempDir::new("apply-changes-errors");
        let w = create(&dir);
        w.set(&coord3(0, 0, 0), "material", Value::Str("stone".into()))
            .unwrap();
        let results = w.apply_changes(vec![
            Change {
                coord: Coord::from(vec![1, 2]), // wrong axis count
                key: "material".into(),
                kind: ChangeKind::Removed(1),
                stamp: Stamp::NONE,
            },
            Change {
                coord: coord3(1, 0, 0),
                key: "material".into(), // a str key, given an i64
                kind: ChangeKind::Set(Value::I64(1), meta_at(1)),
                stamp: Stamp::NONE,
            },
            Change {
                coord: coord3(2, 0, 0),
                key: "never-set".into(),
                kind: ChangeKind::Removed(1),
                stamp: Stamp::NONE,
            },
            Change {
                coord: coord3(3, 0, 0),
                key: "material".into(),
                kind: ChangeKind::Set(Value::Str("sand".into()), meta_at(1)),
                stamp: Stamp::NONE,
            },
        ]);
        assert!(results[0].is_err());
        assert!(results[1].is_err());
        assert!(!results[2].as_ref().unwrap());
        assert!(*results[3].as_ref().unwrap());
    }

    #[test]
    fn lookup_eq_on_an_unindexed_key_is_none() {
        let dir = TempDir::new("index-unindexed");
        let w = create(&dir);
        w.set(&coord3(0, 0, 0), "material", Value::Str("stone".into()))
            .unwrap();
        assert_eq!(w.lookup_eq("material", &Value::Str("stone".into())), None);
        assert_eq!(w.indexed_keys(), Vec::<String>::new());
    }

    #[test]
    fn create_index_backfills_existing_data() {
        let dir = TempDir::new("index-backfill");
        let w = create(&dir);
        w.set(&coord3(0, 0, 0), "material", Value::Str("stone".into()))
            .unwrap();
        w.set(&coord3(1, 0, 0), "material", Value::Str("stone".into()))
            .unwrap();
        w.set(&coord3(2, 0, 0), "material", Value::Str("dirt".into()))
            .unwrap();

        w.create_index("material").unwrap();
        assert_eq!(w.indexed_keys(), vec!["material".to_string()]);

        let mut stone = w.lookup_eq("material", &Value::Str("stone".into())).unwrap();
        stone.sort_by(|a, b| a.iter().cmp(b.iter()));
        assert_eq!(stone, vec![coord3(0, 0, 0), coord3(1, 0, 0)]);
        assert_eq!(
            w.lookup_eq("material", &Value::Str("dirt".into())),
            Some(vec![coord3(2, 0, 0)])
        );
        assert_eq!(
            w.lookup_eq("material", &Value::Str("lava".into())),
            Some(vec![])
        );
    }

    #[test]
    fn create_index_on_a_never_written_key_is_an_empty_noop() {
        let dir = TempDir::new("index-never-written");
        let w = create(&dir);
        w.create_index("material").unwrap();
        assert_eq!(w.indexed_keys(), Vec::<String>::new());
    }

    #[test]
    fn set_keeps_an_existing_index_up_to_date() {
        let dir = TempDir::new("index-set-live");
        let w = create(&dir);
        w.set(&coord3(0, 0, 0), "material", Value::Str("stone".into()))
            .unwrap();
        w.create_index("material").unwrap();

        // A brand new cell under the indexed key.
        w.set(&coord3(5, 0, 0), "material", Value::Str("stone".into()))
            .unwrap();
        let mut stone = w.lookup_eq("material", &Value::Str("stone".into())).unwrap();
        stone.sort_by(|a, b| a.iter().cmp(b.iter()));
        assert_eq!(stone, vec![coord3(0, 0, 0), coord3(5, 0, 0)]);

        // Overwriting an already-indexed cell moves it to its new value.
        w.set(&coord3(0, 0, 0), "material", Value::Str("dirt".into()))
            .unwrap();
        assert_eq!(
            w.lookup_eq("material", &Value::Str("stone".into())),
            Some(vec![coord3(5, 0, 0)])
        );
        assert_eq!(
            w.lookup_eq("material", &Value::Str("dirt".into())),
            Some(vec![coord3(0, 0, 0)])
        );
    }

    #[test]
    fn remove_clears_the_cell_from_an_existing_index() {
        let dir = TempDir::new("index-remove-live");
        let w = create(&dir);
        w.set(&coord3(0, 0, 0), "material", Value::Str("stone".into()))
            .unwrap();
        w.create_index("material").unwrap();

        w.remove(&coord3(0, 0, 0), "material").unwrap();
        assert_eq!(
            w.lookup_eq("material", &Value::Str("stone".into())),
            Some(vec![])
        );
    }

    #[test]
    fn set_region_and_remove_region_keep_an_existing_index_up_to_date() {
        let dir = TempDir::new("index-region-live");
        let w = create(&dir);
        // `create_index` only tracks a key once it's a known column (see
        // its doc comment) -- fix the type up front, same as a real
        // deployment indexing a key ahead of its first `set`.
        w.add_column("material", ValueType::Str).unwrap();
        w.create_index("material").unwrap();

        let region = Region::new([0, 0, 0], [2, 1, 1]);
        w.set_region(
            &region,
            "material",
            &[Value::Str("stone".into()), Value::Str("dirt".into())],
        )
        .unwrap();
        assert_eq!(
            w.lookup_eq("material", &Value::Str("stone".into())),
            Some(vec![coord3(0, 0, 0)])
        );
        assert_eq!(
            w.lookup_eq("material", &Value::Str("dirt".into())),
            Some(vec![coord3(1, 0, 0)])
        );

        w.remove_region(&region, "material").unwrap();
        assert_eq!(
            w.lookup_eq("material", &Value::Str("stone".into())),
            Some(vec![])
        );
        assert_eq!(
            w.lookup_eq("material", &Value::Str("dirt".into())),
            Some(vec![])
        );
    }

    #[test]
    fn apply_replicated_keeps_an_existing_index_up_to_date() {
        let dir = TempDir::new("index-replicated-live");
        let w = create(&dir);
        w.add_column("material", ValueType::Str).unwrap();
        w.create_index("material").unwrap();

        let meta = CellMeta {
            created_at_ms: 1,
            modified_at_ms: 1,
            version: 0,
        };
        w.apply_replicated(&coord3(0, 0, 0), "material", Value::Str("stone".into()), meta)
            .unwrap();
        assert_eq!(
            w.lookup_eq("material", &Value::Str("stone".into())),
            Some(vec![coord3(0, 0, 0)])
        );

        assert!(w
            .apply_replicated_remove(&coord3(0, 0, 0), "material", 2)
            .unwrap());
        assert_eq!(
            w.lookup_eq("material", &Value::Str("stone".into())),
            Some(vec![])
        );
    }

    #[test]
    fn apply_changes_keeps_an_existing_index_up_to_date() {
        let dir = TempDir::new("index-apply-changes-live");
        let w = create(&dir);
        w.add_column("material", ValueType::Str).unwrap();
        w.create_index("material").unwrap();

        let meta_at = |ms: u64| CellMeta {
            created_at_ms: ms,
            modified_at_ms: ms,
            version: 0,
        };
        let results = w.apply_changes(vec![
            Change {
                coord: coord3(0, 0, 0),
                key: "material".into(),
                kind: ChangeKind::Set(Value::Str("stone".into()), meta_at(1)),
                stamp: Stamp::NONE,
            },
            Change {
                coord: coord3(1, 0, 0),
                key: "material".into(),
                kind: ChangeKind::Set(Value::Str("stone".into()), meta_at(1)),
                stamp: Stamp::NONE,
            },
        ]);
        assert!(results.iter().all(|r| *r.as_ref().unwrap()));
        let mut stone = w.lookup_eq("material", &Value::Str("stone".into())).unwrap();
        stone.sort_by(|a, b| a.iter().cmp(b.iter()));
        assert_eq!(stone, vec![coord3(0, 0, 0), coord3(1, 0, 0)]);

        let remove_results = w.apply_changes(vec![Change {
            coord: coord3(0, 0, 0),
            key: "material".into(),
            kind: ChangeKind::Removed(2),
            stamp: Stamp::NONE,
        }]);
        assert!(*remove_results[0].as_ref().unwrap());
        assert_eq!(
            w.lookup_eq("material", &Value::Str("stone".into())),
            Some(vec![coord3(1, 0, 0)])
        );
    }

    #[test]
    fn drop_index_stops_maintenance_and_lookup_falls_back_to_unindexed() {
        let dir = TempDir::new("index-drop-live");
        let w = create(&dir);
        w.set(&coord3(0, 0, 0), "material", Value::Str("stone".into()))
            .unwrap();
        w.create_index("material").unwrap();
        assert!(w.drop_index("material").unwrap());
        assert!(!w.drop_index("material").unwrap());

        assert_eq!(w.lookup_eq("material", &Value::Str("stone".into())), None);
        // A set after dropping must not resurrect/maintain the index.
        w.set(&coord3(1, 0, 0), "material", Value::Str("stone".into()))
            .unwrap();
        assert_eq!(w.lookup_eq("material", &Value::Str("stone".into())), None);
    }

    #[test]
    fn rebuild_index_on_a_never_indexed_key_builds_it_from_scratch() {
        let dir = TempDir::new("index-rebuild-fresh");
        let w = create(&dir);
        w.set(&coord3(0, 0, 0), "material", Value::Str("stone".into()))
            .unwrap();
        w.rebuild_index("material").unwrap();
        assert_eq!(w.indexed_keys(), vec!["material".to_string()]);
        assert_eq!(
            w.lookup_eq("material", &Value::Str("stone".into())),
            Some(vec![coord3(0, 0, 0)])
        );
    }

    #[test]
    fn rebuild_index_on_a_never_written_key_is_an_empty_noop() {
        let dir = TempDir::new("index-rebuild-never-written");
        let w = create(&dir);
        w.rebuild_index("material").unwrap();
        assert_eq!(w.indexed_keys(), Vec::<String>::new());
    }

    #[test]
    fn rebuild_index_discards_stale_entries_an_already_built_index_would_keep() {
        let dir = TempDir::new("index-rebuild-discards-stale");
        let w = create(&dir);
        w.set(&coord3(0, 0, 0), "material", Value::Str("stone".into()))
            .unwrap();
        w.create_index("material").unwrap();

        // Simulate the index having drifted stale by writing directly to
        // the underlying `ValueIndex`, bypassing every write path that
        // would normally keep it in sync -- `rebuild_index` is the
        // recovery lever for exactly this (see its doc comment), so this
        // confirms it actually discards what a cheap `create_index`
        // no-op would have left behind.
        w.value_index
            .record(
                w.schema().id_for_key("material").unwrap(),
                &coord3(9, 9, 9),
                None,
                Some(&Value::Str("stone".into())),
            )
            .unwrap();
        assert_eq!(
            w.lookup_eq("material", &Value::Str("stone".into()))
                .unwrap()
                .len(),
            2
        );

        w.rebuild_index("material").unwrap();
        assert_eq!(
            w.lookup_eq("material", &Value::Str("stone".into())),
            Some(vec![coord3(0, 0, 0)])
        );
    }

    #[test]
    fn remove_column_drops_its_index_too() {
        let dir = TempDir::new("index-remove-column");
        let w = create(&dir);
        w.set(&coord3(0, 0, 0), "material", Value::Str("stone".into()))
            .unwrap();
        w.create_index("material").unwrap();
        assert!(w.remove_column("material").unwrap());
        assert_eq!(w.indexed_keys(), Vec::<String>::new());
    }

    #[test]
    fn an_index_survives_closing_and_reopening_the_world() {
        let dir = TempDir::new("index-survives-reopen");
        {
            let w = create(&dir);
            w.set(&coord3(0, 0, 0), "material", Value::Str("stone".into()))
                .unwrap();
            w.set(&coord3(1, 0, 0), "material", Value::Str("dirt".into()))
                .unwrap();
            w.create_index("material").unwrap();
        }

        // A brand new `World` over the same directory -- this can only
        // know "material" is indexed, and what it currently holds, by
        // having opened its on-disk LSM segments back under
        // `root/indexes/`, not from any in-memory state carried over.
        let w = World::open(&dir).unwrap();
        assert_eq!(w.indexed_keys(), vec!["material".to_string()]);
        assert_eq!(
            w.lookup_eq("material", &Value::Str("stone".into())),
            Some(vec![coord3(0, 0, 0)])
        );
        assert_eq!(
            w.lookup_eq("material", &Value::Str("dirt".into())),
            Some(vec![coord3(1, 0, 0)])
        );
    }

    #[test]
    fn a_reopened_index_keeps_itself_up_to_date() {
        let dir = TempDir::new("index-reopen-live");
        {
            let w = create(&dir);
            w.set(&coord3(0, 0, 0), "material", Value::Str("stone".into()))
                .unwrap();
            w.create_index("material").unwrap();
        }

        let w = World::open(&dir).unwrap();
        w.set(&coord3(0, 0, 0), "material", Value::Str("dirt".into()))
            .unwrap();
        assert_eq!(
            w.lookup_eq("material", &Value::Str("stone".into())),
            Some(vec![])
        );
        assert_eq!(
            w.lookup_eq("material", &Value::Str("dirt".into())),
            Some(vec![coord3(0, 0, 0)])
        );
    }

    #[test]
    fn a_dropped_index_stays_dropped_across_a_reopen() {
        let dir = TempDir::new("index-drop-survives-reopen");
        {
            let w = create(&dir);
            w.set(&coord3(0, 0, 0), "material", Value::Str("stone".into()))
                .unwrap();
            w.create_index("material").unwrap();
            w.drop_index("material").unwrap();
        }

        let w = World::open(&dir).unwrap();
        assert_eq!(w.indexed_keys(), Vec::<String>::new());
        assert_eq!(w.lookup_eq("material", &Value::Str("stone".into())), None);
    }

    #[test]
    fn an_unindexed_world_has_nothing_to_restore_on_reopen() {
        let dir = TempDir::new("index-none-no-file");
        {
            let w = create(&dir);
            w.set(&coord3(0, 0, 0), "material", Value::Str("stone".into()))
                .unwrap();
        }
        let w = World::open(&dir).unwrap();
        assert_eq!(w.indexed_keys(), Vec::<String>::new());
    }

    #[test]
    fn a_removed_columns_index_does_not_come_back_on_reopen() {
        let dir = TempDir::new("index-removed-column-no-reopen");
        {
            let w = create(&dir);
            w.set(&coord3(0, 0, 0), "material", Value::Str("stone".into()))
                .unwrap();
            w.create_index("material").unwrap();
            w.remove_column("material").unwrap();
        }

        let w = World::open(&dir).unwrap();
        assert_eq!(w.indexed_keys(), Vec::<String>::new());
    }

    #[test]
    fn cell_entry_at_reports_every_key_set_at_that_cell() {
        let dir = TempDir::new("cell-entry-at-basic");
        let w = create(&dir);
        w.set(&coord3(1, 2, 3), "material", Value::Str("stone".into()))
            .unwrap();
        w.set(&coord3(1, 2, 3), "hardness", Value::I64(7)).unwrap();
        w.set(&coord3(9, 9, 9), "material", Value::Str("air".into()))
            .unwrap();

        let entry = w.cell_entry_at(&coord3(1, 2, 3)).unwrap().unwrap();
        assert_eq!(entry.coord, coord3(1, 2, 3));
        assert_eq!(
            entry.values.iter().map(|(k, ..)| k.clone()).collect::<Vec<_>>(),
            vec!["hardness".to_string(), "material".to_string()],
        );
    }

    #[test]
    fn cell_entry_at_is_none_for_an_unset_cell() {
        let dir = TempDir::new("cell-entry-at-unset");
        let w = create(&dir);
        w.set(&coord3(0, 0, 0), "material", Value::Str("stone".into()))
            .unwrap();
        assert_eq!(w.cell_entry_at(&coord3(1, 1, 1)).unwrap(), None);
    }

    #[test]
    fn cell_entry_at_matches_list_cells_for_the_same_cell() {
        let dir = TempDir::new("cell-entry-at-matches-list-cells");
        let w = create(&dir);
        w.set(&coord3(2, 2, 2), "material", Value::Str("stone".into()))
            .unwrap();
        w.set(&coord3(2, 2, 2), "density", Value::F64(2.5)).unwrap();

        let from_list = w
            .list_cells()
            .unwrap()
            .into_iter()
            .find(|c| c.coord == coord3(2, 2, 2))
            .unwrap();
        let from_single = w.cell_entry_at(&coord3(2, 2, 2)).unwrap().unwrap();
        assert_eq!(from_list, from_single);
    }

    #[test]
    fn lookup_eq_reconciles_an_int_literal_against_an_f64_column() {
        let dir = TempDir::new("index-lookup-eq-int-vs-f64");
        let w = create(&dir);
        w.set(&coord3(0, 0, 0), "density", Value::F64(5.0)).unwrap();
        w.create_index("density").unwrap();

        assert_eq!(
            w.lookup_eq("density", &Value::I64(5)),
            Some(vec![coord3(0, 0, 0)])
        );
        // A fractional float can never equal anything in this I64... er,
        // F64 column that isn't itself 5.0 -- not indexed under Int(6).
        assert_eq!(w.lookup_eq("density", &Value::I64(6)), Some(vec![]));
    }

    #[test]
    fn lookup_eq_reconciles_a_float_literal_against_an_i64_column() {
        let dir = TempDir::new("index-lookup-eq-f64-vs-int");
        let w = create(&dir);
        w.set(&coord3(0, 0, 0), "hardness", Value::I64(7)).unwrap();
        w.create_index("hardness").unwrap();

        assert_eq!(
            w.lookup_eq("hardness", &Value::F64(7.0)),
            Some(vec![coord3(0, 0, 0)])
        );
        // 7.5 can never equal an I64 column's value -- a precise empty
        // answer, not "unknown" (which would send the caller to a full
        // scan that would also find nothing).
        assert_eq!(w.lookup_eq("hardness", &Value::F64(7.5)), Some(vec![]));
    }

    #[test]
    fn lookup_eq_of_a_mismatched_non_numeric_type_is_a_precise_empty_result() {
        let dir = TempDir::new("index-lookup-eq-type-mismatch");
        let w = create(&dir);
        w.set(&coord3(0, 0, 0), "material", Value::Str("stone".into()))
            .unwrap();
        w.create_index("material").unwrap();

        assert_eq!(w.lookup_eq("material", &Value::Bool(true)), Some(vec![]));
    }

    // --- Content digest ---

    #[test]
    fn disabled_by_default_content_digest_is_none() {
        let dir = TempDir::new("digest-disabled-default");
        let w = create(&dir);
        assert_eq!(w.content_digest(), None);
    }

    #[test]
    fn enabling_an_empty_world_starts_at_a_fixed_baseline() {
        let dir = TempDir::new("digest-empty-baseline");
        let w = create(&dir).with_content_digest(true).unwrap();
        // An empty world's digest is the XOR of nothing: zero, every
        // time, on every node -- the trivial but real baseline every
        // write then moves away from.
        assert_eq!(w.content_digest(), Some(0));
    }

    #[test]
    fn a_set_changes_the_digest_and_a_matching_remove_undoes_it() {
        let dir = TempDir::new("digest-set-remove");
        let w = create(&dir).with_content_digest(true).unwrap();
        let empty = w.content_digest().unwrap();

        w.set(&coord3(0, 0, 0), "material", Value::Str("stone".into()))
            .unwrap();
        let after_set = w.content_digest().unwrap();
        assert_ne!(after_set, empty);

        w.remove(&coord3(0, 0, 0), "material").unwrap();
        assert_eq!(w.content_digest(), Some(empty));
    }

    #[test]
    fn two_worlds_with_identical_data_written_in_different_orders_agree() {
        // The whole point of an XOR digest: order-independent. Two
        // worlds that end up holding the same data, even via writes in a
        // different order (as two real cluster nodes applying the same
        // changes via different replication paths might), must land on
        // the same digest. Uses `apply_replicated` with an explicit,
        // identical `CellMeta` for both worlds rather than `set` (which
        // stamps each write with the real wall clock) -- the digest
        // hashes metadata too, so comparing two independently-timed
        // `set` calls for equality would be comparing real timestamps
        // that have no reason to match, not testing this property.
        let dir_a = TempDir::new("digest-order-a");
        let dir_b = TempDir::new("digest-order-b");
        let a = create(&dir_a).with_content_digest(true).unwrap();
        let b = create(&dir_b).with_content_digest(true).unwrap();
        let meta = CellMeta {
            created_at_ms: 100,
            modified_at_ms: 100,
            version: 0,
        };

        a.apply_replicated(&coord3(0, 0, 0), "material", Value::Str("stone".into()), meta)
            .unwrap();
        a.apply_replicated(&coord3(1, 0, 0), "material", Value::Str("dirt".into()), meta)
            .unwrap();
        b.apply_replicated(&coord3(1, 0, 0), "material", Value::Str("dirt".into()), meta)
            .unwrap();
        b.apply_replicated(&coord3(0, 0, 0), "material", Value::Str("stone".into()), meta)
            .unwrap();

        assert_eq!(a.content_digest(), b.content_digest());
    }

    #[test]
    fn a_different_value_at_the_same_cell_changes_the_digest() {
        let dir_a = TempDir::new("digest-differs-a");
        let dir_b = TempDir::new("digest-differs-b");
        let a = create(&dir_a).with_content_digest(true).unwrap();
        let b = create(&dir_b).with_content_digest(true).unwrap();

        a.set(&coord3(0, 0, 0), "material", Value::Str("stone".into()))
            .unwrap();
        b.set(&coord3(0, 0, 0), "material", Value::Str("dirt".into()))
            .unwrap();

        assert_ne!(a.content_digest(), b.content_digest());
    }

    #[test]
    fn overwriting_a_cell_folds_out_the_old_value_and_in_the_new_one() {
        let dir = TempDir::new("digest-overwrite");
        let w = create(&dir).with_content_digest(true).unwrap();
        w.set(&coord3(0, 0, 0), "material", Value::Str("stone".into()))
            .unwrap();
        let after_stone = w.content_digest().unwrap();

        w.set(&coord3(0, 0, 0), "material", Value::Str("dirt".into()))
            .unwrap();
        let after_dirt = w.content_digest().unwrap();
        assert_ne!(after_stone, after_dirt);

        w.set(&coord3(0, 0, 0), "material", Value::Str("stone".into()))
            .unwrap();
        // Back to the same value -- metadata (version, timestamps)
        // changed along the way, but a fresh third write landing on the
        // original value again isn't required to reproduce the exact
        // same digest, since metadata is part of what's hashed. Just
        // confirm it moved again, consistently.
        assert_ne!(w.content_digest(), Some(after_dirt));
    }

    #[test]
    fn set_region_and_remove_region_update_the_digest() {
        let dir = TempDir::new("digest-region");
        let w = create(&dir).with_content_digest(true).unwrap();
        let empty = w.content_digest().unwrap();

        let region = Region::new([0, 0, 0], [2, 1, 1]);
        w.set_region(
            &region,
            "material",
            &[Value::Str("stone".into()), Value::Str("dirt".into())],
        )
        .unwrap();
        let after_set = w.content_digest().unwrap();
        assert_ne!(after_set, empty);

        w.remove_region(&region, "material").unwrap();
        assert_eq!(w.content_digest(), Some(empty));
    }

    #[test]
    fn apply_replicated_and_apply_replicated_remove_update_the_digest() {
        let dir = TempDir::new("digest-replicated");
        let w = create(&dir).with_content_digest(true).unwrap();
        let empty = w.content_digest().unwrap();

        let meta = CellMeta {
            created_at_ms: 1,
            modified_at_ms: 1,
            version: 0,
        };
        w.apply_replicated(&coord3(0, 0, 0), "material", Value::Str("stone".into()), meta)
            .unwrap();
        assert_ne!(w.content_digest(), Some(empty));

        w.apply_replicated_remove(&coord3(0, 0, 0), "material", 2)
            .unwrap();
        assert_eq!(w.content_digest(), Some(empty));
    }

    #[test]
    fn a_replicated_write_that_loses_last_write_wins_does_not_change_the_digest() {
        let dir = TempDir::new("digest-replicated-loses");
        let w = create(&dir).with_content_digest(true).unwrap();
        let newer = CellMeta {
            created_at_ms: 100,
            modified_at_ms: 100,
            version: 0,
        };
        w.apply_replicated(&coord3(0, 0, 0), "material", Value::Str("stone".into()), newer)
            .unwrap();
        let after_newer = w.content_digest().unwrap();

        // Older than what's already there -- discarded, so the digest
        // must not move.
        let older = CellMeta {
            created_at_ms: 1,
            modified_at_ms: 1,
            version: 0,
        };
        let applied = w
            .apply_replicated(&coord3(0, 0, 0), "material", Value::Str("dirt".into()), older)
            .unwrap();
        assert!(!applied);
        assert_eq!(w.content_digest(), Some(after_newer));
    }

    #[test]
    fn apply_changes_batch_updates_the_digest() {
        let dir = TempDir::new("digest-apply-changes");
        let w = create(&dir).with_content_digest(true).unwrap();
        let empty = w.content_digest().unwrap();

        let meta = CellMeta {
            created_at_ms: 1,
            modified_at_ms: 1,
            version: 0,
        };
        let results = w.apply_changes(vec![
            Change {
                coord: coord3(0, 0, 0),
                key: "material".into(),
                kind: ChangeKind::Set(Value::Str("stone".into()), meta),
                stamp: Stamp::NONE,
            },
            Change {
                coord: coord3(1, 0, 0),
                key: "material".into(),
                kind: ChangeKind::Set(Value::Str("dirt".into()), meta),
                stamp: Stamp::NONE,
            },
        ]);
        assert!(results.iter().all(|r| *r.as_ref().unwrap()));
        let after_batch = w.content_digest().unwrap();
        assert_ne!(after_batch, empty);

        let remove_results = w.apply_changes(vec![
            Change {
                coord: coord3(0, 0, 0),
                key: "material".into(),
                kind: ChangeKind::Removed(2),
                stamp: Stamp::NONE,
            },
            Change {
                coord: coord3(1, 0, 0),
                key: "material".into(),
                kind: ChangeKind::Removed(2),
                stamp: Stamp::NONE,
            },
        ]);
        assert!(remove_results.iter().all(|r| *r.as_ref().unwrap()));
        assert_eq!(w.content_digest(), Some(empty));
    }

    #[test]
    fn remove_column_folds_out_every_cell_it_held() {
        let dir = TempDir::new("digest-remove-column");
        let w = create(&dir).with_content_digest(true).unwrap();
        let empty = w.content_digest().unwrap();

        w.set(&coord3(0, 0, 0), "material", Value::Str("stone".into()))
            .unwrap();
        w.set(&coord3(1, 0, 0), "material", Value::Str("dirt".into()))
            .unwrap();
        assert_ne!(w.content_digest(), Some(empty));

        w.remove_column("material").unwrap();
        assert_eq!(w.content_digest(), Some(empty));
    }

    #[test]
    fn enabling_the_digest_on_a_world_with_existing_data_backfills_it() {
        let dir = TempDir::new("digest-backfill");
        let w = create(&dir);
        w.set(&coord3(0, 0, 0), "material", Value::Str("stone".into()))
            .unwrap();
        w.set(&coord3(1, 0, 0), "material", Value::Str("dirt".into()))
            .unwrap();

        // Computed independently, straight from `list_cells`, before
        // `with_content_digest` ever runs -- not compared against a
        // second `World`'s own live-maintained digest, since two
        // separately-created worlds would pick up different real
        // wall-clock timestamps in their `CellMeta` (which the digest
        // hashes) and could never match on that basis alone.
        let mut expected = 0u64;
        for cell in w.list_cells().unwrap() {
            for (key, value, meta) in &cell.values {
                expected ^= entry_digest(&cell.coord, key, value, meta);
            }
        }

        let w = w.with_content_digest(true).unwrap();
        assert_eq!(w.content_digest(), Some(expected));
        assert_ne!(expected, 0);
    }

    #[test]
    fn a_content_digest_survives_closing_and_reopening_the_world() {
        let dir = TempDir::new("digest-reopen");
        let before = {
            let w = create(&dir).with_content_digest(true).unwrap();
            w.set(&coord3(0, 0, 0), "material", Value::Str("stone".into()))
                .unwrap();
            w.content_digest().unwrap()
        };

        // A brand new `World` over the same directory, with the digest
        // turned on again -- this can only report the same value by
        // having read `content_digest.bin` back, not from any in-memory
        // state carried over.
        let w = World::open(&dir).unwrap().with_content_digest(true).unwrap();
        assert_eq!(w.content_digest(), Some(before));
    }

    #[test]
    fn disabling_then_reenabling_the_digest_picks_up_where_it_left_off() {
        let dir = TempDir::new("digest-disable-reenable");
        let w = create(&dir).with_content_digest(true).unwrap();
        w.set(&coord3(0, 0, 0), "material", Value::Str("stone".into()))
            .unwrap();
        let value = w.content_digest().unwrap();

        let w = w.with_content_digest(false).unwrap();
        assert_eq!(w.content_digest(), None);

        let w = w.with_content_digest(true).unwrap();
        assert_eq!(w.content_digest(), Some(value));
    }

    #[test]
    fn a_write_to_an_unrelated_key_does_not_change_the_digest_of_a_removed_one() {
        // Sanity check that per-entry hashing really is scoped by key
        // name, not just coordinate -- two different keys at the same
        // cell must contribute independently.
        let dir = TempDir::new("digest-independent-keys");
        let w = create(&dir).with_content_digest(true).unwrap();
        w.set(&coord3(0, 0, 0), "material", Value::Str("stone".into()))
            .unwrap();
        let after_material = w.content_digest().unwrap();

        w.set(&coord3(0, 0, 0), "hardness", Value::I64(7)).unwrap();
        let after_hardness = w.content_digest().unwrap();
        assert_ne!(after_material, after_hardness);

        w.remove(&coord3(0, 0, 0), "hardness").unwrap();
        assert_eq!(w.content_digest(), Some(after_material));
    }

    // --- Chunk digests ---

    #[test]
    fn chunk_digests_is_empty_for_an_empty_world() {
        let dir = TempDir::new("chunk-digests-empty");
        let w = create(&dir);
        assert!(w.chunk_digests().unwrap().is_empty());
    }

    #[test]
    fn chunk_digests_does_not_require_content_digest_to_be_enabled() {
        let dir = TempDir::new("chunk-digests-no-flag-needed");
        let w = create(&dir); // content digest left off
        w.set(&coord3(0, 0, 0), "material", Value::Str("stone".into()))
            .unwrap();
        assert_eq!(w.chunk_digests().unwrap().len(), 1);
    }

    #[test]
    fn chunk_digests_has_one_entry_per_non_empty_chunk() {
        let dir = TempDir::new("chunk-digests-per-chunk");
        let w = create(&dir);
        // Same chunk (default chunk_dim 32).
        w.set(&coord3(0, 0, 0), "material", Value::Str("stone".into()))
            .unwrap();
        w.set(&coord3(1, 0, 0), "material", Value::Str("dirt".into()))
            .unwrap();
        // A different chunk.
        w.set(&coord3(100, 0, 0), "material", Value::Str("sand".into()))
            .unwrap();

        assert_eq!(w.chunk_digests().unwrap().len(), 2);
    }

    #[test]
    fn chunk_digests_xor_together_to_the_whole_database_digest() {
        let dir = TempDir::new("chunk-digests-xor-to-whole");
        let w = create(&dir).with_content_digest(true).unwrap();
        w.set(&coord3(0, 0, 0), "material", Value::Str("stone".into()))
            .unwrap();
        w.set(&coord3(100, 0, 0), "material", Value::Str("dirt".into()))
            .unwrap();
        w.set(&coord3(-50, 7, 0), "hardness", Value::I64(3))
            .unwrap();

        let combined = w.chunk_digests().unwrap().values().fold(0u64, |a, &b| a ^ b);
        assert_eq!(Some(combined), w.content_digest());
    }

    #[test]
    fn a_different_value_in_one_chunk_changes_only_that_chunks_digest() {
        let dir_a = TempDir::new("chunk-digests-differ-a");
        let dir_b = TempDir::new("chunk-digests-differ-b");
        let a = create(&dir_a);
        let b = create(&dir_b);
        let meta = CellMeta {
            created_at_ms: 1,
            modified_at_ms: 1,
            version: 0,
        };
        // Same chunk, same everything, on both.
        a.apply_replicated(&coord3(100, 0, 0), "material", Value::Str("dirt".into()), meta)
            .unwrap();
        b.apply_replicated(&coord3(100, 0, 0), "material", Value::Str("dirt".into()), meta)
            .unwrap();
        // A different chunk -- different value on each.
        a.apply_replicated(&coord3(0, 0, 0), "material", Value::Str("stone".into()), meta)
            .unwrap();
        b.apply_replicated(&coord3(0, 0, 0), "material", Value::Str("sand".into()), meta)
            .unwrap();

        let (digests_a, digests_b) = (a.chunk_digests().unwrap(), b.chunk_digests().unwrap());
        // Chunk *keys* are coordinates divided by `chunk_dim` (see
        // `split`), not cell coordinates -- (100, 0, 0) at the default
        // chunk_dim (32) falls in chunk key (3, 0, 0).
        let shared_chunk = coord3(100i32.div_euclid(DEFAULT_CHUNK_DIM as i32), 0, 0);
        let differing_chunk = coord3(0, 0, 0);
        assert_eq!(
            digests_a.get(&shared_chunk),
            digests_b.get(&shared_chunk),
            "the untouched-by-the-difference chunk must still match"
        );
        assert_ne!(
            digests_a.get(&differing_chunk),
            digests_b.get(&differing_chunk),
            "the chunk holding the differing value must disagree"
        );
    }
}
