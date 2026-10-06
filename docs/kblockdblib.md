# `kblockdblib`: the storage engine

A `World` is one self-contained storage root: its own `world.txt`/
`schema.txt`/chunk tree under whatever directory it's pointed at. This
crate knows nothing about "databases" or multiple `World`s sharing a
parent directory -- that's `kblockdbserver`'s job (see its
[`Databases` manager](kblockdbserver.md#databases)), which manages any
number of independent `World`s, one per database, each rooted at its own
subdirectory of `--data-dir`. Everything below describes a single `World`
in isolation, same as if it were the only one on disk.

## Design

- **Chunking, not one file per cell.** The world is split into
  `chunk_dim`^axes cell chunks -- 32x32x32 (32,768 cells/chunk) by default.
  A 10,000^3 world at that default needs 313 chunks/axis (313^3 ~= 30.6M
  chunks total), and each chunk is at most one file. Chunks with no data at
  all are never written, so a mostly-empty world costs disk space
  proportional to what's actually populated. `chunk_dim` is a per-world
  parameter, not a compile-time constant (see the axis-count bullet below)
  -- a bigger value means fewer, larger chunk files (more bytes rewritten
  per single-cell write, since writes are write-through -- see
  "Concurrency" below); a smaller value is the opposite trade.

- **Columnar storage per chunk, not a hashmap per cell.** Each chunk holds
  one sparse array ("column") per key that actually appears somewhere in
  that chunk -- not one per cell. A column is a fixed-size presence bitmap
  (1 bit/cell) plus a dense array of values for only the cells that are
  set. This avoids repeating key strings a trillion times and is the layout
  simulation code actually wants: "add 1.0 to temperature for every cell in
  this chunk" touches one contiguous array, not a trillion separate hashmap
  lookups.

- **Every set cell/key pair carries its own metadata.** Alongside the value
  itself, each entry in a column tracks `created_at_ms` (when it was first
  set), `modified_at_ms` (when it was last set), and `version` (how many
  times it's been overwritten since, starting at `0`) -- see
  `kblockdblib::CellMeta`, `World::get_meta`/`get_with_meta`. Persisted
  alongside the value on disk, not derived; removing a value and setting it
  again later starts a fresh `0`/`created_at_ms`, not a continuation of the
  old history.

- **Global key interning, type fixed on first use.** Key strings
  ("temperature", "material", ...) are interned once into small integer ids
  in `schema.txt` at the world root (append-only, so ids never change), and
  the value type (`str`/`f64`/`i64`/`bool`) `set` first used for that key is
  recorded alongside the id and permanently fixed from then on, world-wide
  -- a later `set` for the same key with a different type fails with a
  normal `InvalidInput` error rather than writing anything. Chunks store
  columns by id, not by string.

- **A write-through, per-chunk cache -- every write is still durable and
  lock-guarded on its own.** `get`/`set`/`remove` lock (shared for a read,
  exclusive for a write) exactly the chunk(s) they touch, apply the
  change, and (for writes) write straight back before releasing the lock
  and returning -- see "Concurrency" below. A `set` is durable the instant
  its call returns, same as ever; caching only removes *redundant reads*
  of a chunk this process has already touched, it never defers or
  batches a write.

- **Directory nesting.** Chunk files live at `<root>/<c0>/<c1>/.../<cn>.chunk`
  (one path segment per axis), which keeps any one directory to at most
  `chunks_per_axis()` entries no matter how large the world gets.

- **Axis count, per-axis size, and chunk size are a world property, not a
  build-time constant.** A world can have any number of axes (2D, 3D, 4D,
  ...), any `world_dim`, and any `chunk_dim`; all three are chosen once,
  when the world is created (`World::create(root, axes, world_dim,
  chunk_dim)`), and persisted to `world.txt` at the world root. `World::open`
  reads them back from that file rather than assuming a default, and a
  later `World::create` against the same directory must pass matching
  numbers or it fails with `InvalidInput` *without touching anything* -- a
  world's shape can't silently change out from under data already written
  for it. `world::AXES`/`world::WORLD_DIM`/`world::DEFAULT_CHUNK_DIM` are
  only the defaults `main`'s demo happens to call `create` with.

- **Coordinates are signed and centered on zero.** Each axis component is
  an `i32`, and a world's valid range per axis is `[-(world_dim/2),
  world_dim/2)` -- `world_dim` cells wide either way, split evenly around
  the origin (an odd `world_dim` puts the extra cell on the positive
  side). `world_dim` itself is unchanged (still the same `u32` persisted to
  `world.txt`); only where the valid window sits relative to zero is new.
  Chunk indexing uses floor (Euclidean) division/remainder rather than
  Rust's default truncate-toward-zero `/`/`%`, so chunk boundaries stay
  evenly spaced across zero the same as everywhere else, and a chunk's
  on-disk directory/file name (`kblockdblib::World`'s "Layout on disk" doc
  comment) is just that chunk key's plain decimal form, negative sign
  included where it applies (e.g. `<root>/-3/0/2.chunk`).

## Concurrency

**kBlockDB assumes exactly one process, with as many threads as you like,
ever has a given world directory open at a time.** There is no
cross-process coordination any more (see below for why) -- run exactly one
`kblockdbserver` per `--data-dir`, never two pointed at the same
directory, and never open more than one `World` handle on the same
directory concurrently within one process either.

Within that one process, any number of threads can share one `World`
safely. Every `get`/`set`/`remove` (and, at chunk granularity, every
`*_region` call) locks exactly the chunk(s) it touches -- shared for a
read, exclusive for a write, via `kblockdblib/src/chunk_cache.rs`'s
`ChunkCache`, a plain `std::sync::RwLock<Option<Chunk>>` per chunk, created
lazily the first time that chunk is touched. The schema (`schema.txt`)
gets the same treatment: it lives behind `World`'s own `Mutex<Schema>`, so
two threads racing to intern two *different* new keys can't collide on
the same id. `World::create` is similarly race-free under concurrent
threads: a process-wide lock guards the whole check-then-maybe-write
sequence, so two threads racing to create the very same fresh directory,
possibly with *different* `axes`/`world_dim`, resolve safely.

**Chunks are cached, not just locked.** Because this process is always the
sole writer, a chunk's in-memory contents -- once read off disk -- can
never fall behind what's on disk, so a chunk is loaded from disk (or
synthesized empty, if it's never been written) only the *first* time this
`World` touches it; every `get`/`set`/`remove` after that reads the cached
copy straight out of memory, and a repeated read of an already-cached
chunk needs only a *shared* lock, so concurrent readers of the same chunk
run fully in parallel too, not just readers of different chunks. Writes
are still **write-through, not write-behind**: `set`/`remove` still write
the chunk straight back to disk before releasing its lock and returning,
so durability is exactly what it was before caching -- caching removes
redundant *reads*, it never defers or batches a *write*. A chunk touched
by four `set` calls in a row, even for four different keys on the same
cell, is still four separate chunk-file rewrites, not one batched one; a
chunk `get`/`set` four times in a row is one disk read (the first touch)
plus (for the writes) four disk writes, not four reads plus four writes.
`get_region`/`set_region`/`remove_region` claw back the *write* side of
that for the common case where a region spans far fewer chunks than
cells: they group a region's cells by chunk first, so each chunk a region
touches is still locked and written back exactly once, no matter how many
of the region's cells land in it.

The cache is bounded, not unbounded: once a `World` has touched
`DEFAULT_MAX_CACHED_CHUNKS` (`World::with_max_cached_chunks` to override)
distinct chunks, touching one more evicts the least-recently-touched
chunk to make room, same idea as any other fixed-size LRU cache. Eviction
skips any chunk a call is *currently* using (mid-read or mid-write) rather
than pulling it out from under that call -- see
`kblockdblib/src/chunk_cache.rs`'s `evict_one` for why that specifically
matters for correctness here, not just fairness. A `Chunk` with no columns
costs next to nothing (see `Chunk::new`), so a large cap is cheap even for
a world touched at millions of distinct chunks; pick a cap sized to your
workload's actual hot working set, not the world's nominal size -- a
workload that reads many chunks it never revisits (e.g. a one-time scan
over large empty regions) will just cycle chunks through the cache
without ever benefiting much from it, which is fine, just not a win.

**Measured effect of the cache.** Comparing two otherwise-identical
`kblockdbserver` instances, one started with `--max-cached-chunks 0` and
one with `--max-cached-chunks 10000` (`kblockdbperf` has no flag to pass
`--max-cached-chunks` through to an instance it spawns itself, so this
means starting two instances by hand and pointing `kblockdbperf --url` at
each, on fresh data directories, 5,000 cells, single-cell scenarios only
-- see the caveat below for why `region` is left out):

| scenario | cap=0 | cap=10000 | speedup |
|---|---|---|---|
| `set_cell` | 2,198 ops/sec (0.454ms mean) | 2,295 ops/sec (0.435ms mean) | ~1.04x (noise) |
| `get_cell` | 604 ops/sec (1.653ms mean) | 8,131 ops/sec (0.122ms mean) | ~13.5x |
| `remove_cell` | 879 ops/sec (1.137ms mean) | 3,633 ops/sec (0.275ms mean) | ~4.1x |

Exactly the shape the design predicts. `set_cell` writes brand-new cells
with nothing to hit on, and a write is write-through -- a real disk write
-- regardless of cache size, so it comes out flat either way (the ~4%
gap is noise, not signal). `get_cell`/`remove_cell` both populate their
cells first, then re-touch the *same* ones: under `--max-cached-chunks 0`,
each newly-touched chunk evicts the previous one almost immediately, so
the re-touch is a disk re-read every time; under `10000`, all 5,000
cells' chunks comfortably fit and stay resident, so the re-touch is a
pure memory hit. `concurrency_scan`/`contended_cell` (not shown) came out
equal either way, since neither ever revisits a chunk within one run --
there's nothing for the cache to help with regardless of its size.
`region` is deliberately excluded here: a single region call can carry
tens of thousands of JSON-encoded values in one request, so its timing is
dominated by JSON/HTTP payload cost, not chunk-cache hits -- it produced
large but inconsistent differences between runs when tried, which is a
sign of an uncontrolled variable, not a real effect worth reporting as
one.

What none of this gives you is cross-call atomicity: a `get` immediately
followed by a `set` from the same caller is two separate locked
operations, not one transaction, so another thread's write can land in
between them -- same as most simple key/value stores without an explicit
read-modify-write or transaction API.

**Every `World` method takes `&self`, not `&mut self`.** `World`'s only
interior state is a locked `Schema`, the lazily-populated chunk cache
above, and a couple of atomic counters -- so the one process can share one
`World` behind a plain `Arc` (no `Mutex<World>` needed) and let concurrent
calls actually run concurrently, limited only by the same per-chunk locks
that keep two threads from corrupting the same chunk. `kblockdbserver`
does exactly this.

**Why not just keep the old OS-level file locks and support multiple
processes too?** An earlier version of kBlockDB did exactly that (an
advisory `flock` per chunk, so any number of processes -- e.g. several
`kblockdbserver`s behind a load balancer -- could safely share one world
directory). Committing to a single multi-threaded process instead trades
that away for two things: an in-memory `RwLock` needs no syscall to
acquire (the old design's `open`/`lock` on a sidecar `.lock` file per
chunk was itself real, measurable filesystem work -- see the concurrency
cap below), and the schema no longer needs to re-read `schema.txt` on
every cache miss (see `Schema`'s doc comment) since nothing else can have
appended to it behind this process's back.

**Concurrent filesystem operations don't scale indefinitely, though.**
Measured on one real, fast, local SSD: going from 1 to 8-32 concurrent
disk-touching chunk operations (disjoint chunks, so no lock contention
between them) roughly doubled throughput, as expected -- but pushing to
128 concurrent calls made aggregate throughput *worse* than at 1, not just
diminishing -- confirmed to be real OS/filesystem-level contention
(reproduced with a synthetic probe doing only `create_dir_all`/file I/O,
no `kblockdblib` code at all, and ruled out an in-memory `Mutex`
bottleneck the same way: a pure lock-contention probe at the same thread
count showed no degradation at all). `World` caps how many of these run
concurrently (`DEFAULT_MAX_CONCURRENT_DISK_OPS`,
`World::with_max_concurrent_disk_ops` to override) for exactly this
reason -- a cache hit doesn't count against this cap at all, since it
touches no disk. The right cap is a property of the underlying
filesystem/storage, not of `kblockdblib` -- the default is a reasonable
starting point, not a measured optimum for any particular deployment;
measure yours with `kblockdbperf`'s `concurrency_scan`.

## A real trade-off this prototype makes visible

The presence bitmap is a *fixed* size per column regardless of how many
cells in the chunk actually use that key. That's cheap when chunks are
densely populated (real simulations usually have spatial locality: nearby
cells tend to be "on" together), but it's wasteful in the pathological case
where a chunk holds only one or two populated cells -- which is exactly
what the demo's random scatter produces, on purpose, as a worst case.

Two straightforward next steps if that trade-off matters for your actual
access pattern:

1. **Compress each chunk file** (e.g. zstd/gzip) before writing -- a bitmap
   that's almost all zero bytes compresses extremely well, cheaply. This
   one *is* implemented, as an opt-in: see "Compression" below.
2. **Switch to a run-length or sparse-index presence encoding** (a list of
   set cell indices) instead of a flat bitmap when a chunk's occupancy is
   very low, and only use the flat bitmap once occupancy passes some
   threshold (this is basically what real sparse-array libraries like Zarr
   do internally).

The second isn't implemented here -- the prototype optimizes for showing
the mechanism clearly, not for the last byte of density.

## Columns

A world's schema is the set of keys it has ever stored, each fixed to one
`ValueType`. It's normally built implicitly -- the first `set` of a key
interns it and pins its type -- but it can also be managed directly:

```rust
world.add_column("hardness", kblockdblib::ValueType::F64)?;
for column in world.columns() {
    println!("{} {}", column.key, column.value_type.as_str());
}
world.remove_column("hardness")?;
```

- `columns()` lists every live column, sorted by key -- implicitly
  created ones included.
- `add_column(key, type)` declares a column up front, so a key's type is
  fixed before any value is written. `AlreadyExists` if the key already
  has a column; the only way to change a column's type is to remove it
  and add it back.
- `remove_column(key)` drops the column *and every value ever written for
  it*, across every chunk in the world. Returns `false` if there was no
  such column.

`schema.txt` is append-only, so a removal is a tombstone line rather than
a rewrite, and ids are never reissued. Two consequences follow from that:

- A removed key that comes back -- whether through `add_column` or just a
  `set` -- gets a brand new id, which is why it may come back with a
  *different* type than it had.
- `remove_column` writes its tombstone first and only then walks the
  chunks purging data. A crash partway through leaves unreachable bytes
  in some chunk files (invisible, since nothing maps that id to a key any
  more) rather than a half-dropped column that's still partly readable.

Dropping a column rewrites only the chunk files that actually held a
value for it; a rare column doesn't cost a full-world rewrite.

## Indexes

A secondary index on one key, letting `lookup_eq`/`lookup_range` answer
"which cells have `key == value`" or "which cells have `key` between
these bounds" in time proportional to the number of matches instead of a
full `list_cells` scan:

```rust
world.create_index("material")?;
world.lookup_eq("material", &kblockdblib::Value::Str("stone".into()))?;
world.lookup_range(
    "hardness",
    std::ops::Bound::Included(&kblockdblib::Value::I64(5)),
    std::ops::Bound::Unbounded,
)?;
world.drop_index("material")?;
world.rebuild_index("material")?; // discards and rebuilds from scratch
world.indexed_keys(); // every key currently indexed, sorted
```

- `create_index(key)` backfills from every cell that currently holds
  `key` (one `list_cells`-equivalent scan), then every later `set`/
  `remove`/region/replicated write to `key` keeps it up to date for free.
  Idempotent -- a no-op on an already-indexed key. A no-op, not an error,
  on a key that's never been written (there's no type to fix an index to
  yet).
- `drop_index(key)` stops maintaining it and deletes its on-disk state;
  `rebuild_index` is `drop_index` followed by `create_index`'s backfill
  in one call, for a caller that suspects an index has gone stale (it
  shouldn't -- every write path keeps it in sync; this is the recovery
  lever in case that invariant were ever violated).
- `lookup_range(key, lower, upper)` serves `<`/`<=`/`>`/`>=` the same
  way `lookup_eq` serves `=`: both ends are `std::ops::Bound`, so
  `Unbounded` on either side leaves that end open. A literal that
  doesn't fit the column's type exactly still usually has an exact
  *bound* even without an exact *value* -- `hardness > 5.5` on an `I64`
  column is reconciled to exactly `hardness >= 6`, the same numeric
  cross-type reconciliation `lookup_eq`/`eval_compare` already do for
  `=`, generalized to a bound (see `lookup_range`'s own doc comment for
  the exact rules, including the `Str`/`Bool` mismatch and
  out-of-`I64`-range cases).

**Backed by an on-disk LSM structure, not an in-memory map** (`crate::lsm`,
wrapped per-key by `crate::index::ValueIndex`), so an index on a key most
of a huge world holds doesn't need to fit in RAM: writes buffer in a small
bounded memtable (durably logged to a WAL first), flushed to an immutable
sorted segment file once the memtable passes a size threshold, with
segments merged (compacted) once they accumulate past their own
threshold. A lookup checks the memtable then every segment, using each
segment's small in-memory sparse index to avoid reading it in full. See
`kblockdblib/src/lsm.rs`'s own doc comment for the exact shape and its
deliberate simplifications (one compaction tier, no bloom filters).

Each indexed key gets its own directory, `root/indexes/<key_id>/` (see
"Layout" below) -- which keys are indexed is exactly which subdirectories
exist there, not a separate manifest. Because each index's segments and
WAL *are* its persisted state, `World::open`/`create` restore every
existing index by just opening its directory (reading each segment's
small header/footer, replaying the WAL) -- unlike the very first
`create_index` for a key, this needs no `list_cells`-style rescan, so a
world with a huge existing index reopens cheaply, not in time proportional
to how much it indexes.

## Content digest

A running checksum of a world's entire current content, for comparing
against another world's (another node's, in a cluster -- see
`docs/clustering.md`'s "Sync check") without comparing cell by cell:

```rust
let world = kblockdblib::World::create("./data", 3, 10_000, 32)?
    .with_content_digest(true)?;
world.content_digest(); // Some(u64), once enabled
```

- Off by default -- `with_content_digest(false)` (or never calling it)
  costs nothing. Turning it on makes every `set`/`remove` (and their
  region/replicated/batch counterparts) fetch the cell/key's *old* value
  and metadata before overwriting it, for *every* key, not just indexed
  ones -- unlike a secondary index, there's no "only pay for what you
  use" here, since the digest covers everything. Worth it only once
  something is actually going to compare this world's digest against a
  peer's.
- The digest itself is the XOR of a deterministic hash
  (`DefaultHasher`, not `HashMap`'s randomly-seeded default -- the same
  content must hash the same way on every node, every run) over every
  currently-live cell/key's coordinate, key, value, and metadata. XOR is
  what makes it maintainable incrementally: a write folds its old
  contribution out and its new one in with one `^=`, and -- being
  order-independent -- two worlds that reach the same data via writes
  applied in a different order land on the same value. It's a checksum
  for catching accidental divergence, not a cryptographic proof: a
  coincidental cancellation is possible in principle, vanishingly
  unlikely in practice with a 64-bit hash.
- Turning it on for the first time on a world with existing data costs
  one `list_cells`-equivalent full scan to compute a starting value --
  same one-time cost `create_index` pays for a brand new index. After
  that it's persisted (`content_digest.bin` at the world root) and
  loaded directly on every later `open`, no rescan needed.

`World::chunk_digests()` answers the follow-up question once two worlds'
whole-database digests have already shown a mismatch: *which* chunk is
actually different. It returns every currently non-empty chunk's own
digest (same per-entry hash as `content_digest`, just grouped by chunk
key instead of XORed into one running total), keyed by the chunk's own
coordinate, not a cell coordinate. Unlike `content_digest`, it needs no
flag enabled and isn't maintained incrementally -- it's a full,
chunk-by-chunk scan recomputed fresh on every call, the same cost class
as `list_cells`, meant to be called rarely (only after a mismatch is
already known) rather than on any routine cadence. XORing every value
it returns together reproduces exactly what `content_digest()` computes
from the same data, so comparing two worlds' `chunk_digests()` for the
same database always pinpoints a mismatch `content_digest()` already
flagged, down to the chunk.

## Compression

`World::with_compression(bool)` -- off by default -- zstd-compresses
every chunk file the world writes, at zstd level 3. This is the only
thing the crate uses an external dependency for; the chunk format
underneath is unchanged, with compression applied as a transparent
wrapper around the same bytes.

```rust
let world = kblockdblib::World::create("./data", 3, 10_000, 32)?
    .with_compression(true);
```

It can be turned on or off at any time, on an existing world as well as
a new one:

- It governs *writes* only. `load_chunk` sniffs zstd's magic number off
  the front of each file, so a single world can hold a mix of compressed
  and uncompressed chunks and reads correctly either way. (The two can't
  be confused: an uncompressed chunk starts with a little-endian column
  count, and zstd's magic would mean more columns than the `u32` key-id
  space can hold.)
- Flipping it rewrites nothing on its own -- each existing file is
  re-encoded the next time its chunk is written.
- It is deliberately *not* stored in `world.txt`. Unlike
  `axes`/`world_dim`/`chunk_dim`, it isn't part of a world's fixed shape,
  so nothing about it has to stay constant for a world's lifetime.

`kblockdbserver` exposes it as the `compression` key in its config file
-- see the [server documentation](kblockdbserver.md#compression).

## `Chunk`'s two per-cell costs, and which one is fixed

Every `get`/`set`/`remove` on a column needs that cell's *rank* (its
position among the column's set cells) to index into the column's dense,
packed value array. That costs two different things:

- **Computing the rank** used to be an O(presence_bytes) linear popcount
  scan of the whole bitmap before the target bit -- up to ~4096 byte
  popcounts per call for the default chunk shape, on *every* `get`/`set`/
  `remove`, back when every call re-read the chunk from disk fresh and
  there was nothing to amortize it across (see "Concurrency" above).
  `Bitset` now keeps a small per-64-byte-block running-count index
  (`block_counts`, updated in O(1) on every `set`, never persisted to
  disk -- rebuilt once, from the on-disk presence bitmap, the first time a
  chunk is read, and from then on kept up to date in memory for as long as
  that chunk stays cached), turning `rank` into summing a handful of block
  counts plus scanning one partial block: measured (a standalone probe,
  not this crate's own benchmarking machinery, comparing both
  implementations directly) at roughly 4x faster on average and ~7x faster
  for a cell near the end of a densely-populated bitmap -- the case this
  project's own design rationale says is the common one, not the
  pathological one. The write-through chunk cache (see "Concurrency"
  above) means this rebuild-from-disk now happens at most once per chunk
  per `World`, not once per call the way it did originally.
- **Using the rank** to insert/remove a value still means an
  `Vec::insert`/`Vec::remove` into `ColumnData`'s packed array -- an
  O(occupancy) shift of every value after that point in the column. This
  one is *not* fixed here: doing so would mean allocating a full
  `chunk_cells`-sized slot per column instead of a packed one (the same
  "fixed size regardless of occupancy" trade-off the presence bitmap
  already makes, extended to values too), which is a real, opposite-facing
  cost -- multiplying a sparse column's on-disk/in-memory footprint by
  potentially tens of thousands, in exactly the scattered/sparse-chunk
  case this document already calls out as the worst case this prototype
  demonstrates on purpose. Worth doing if your workload is genuinely
  dense-per-chunk and memory isn't the constraint; not done by default
  because it isn't, universally.

## Layout

- `kblockdblib/src/lib.rs`    -- the library crate root; re-exports `World`,
  `Value`, `Region`, `Coord`, `WorldParams`, `AXES`, `WORLD_DIM`.
- `kblockdblib/src/value.rs`  -- the `Value` enum (Str/F64/I64/Bool), its on-disk type
  tags, and `ValueType` (a `Value` without the value itself -- what
  `Schema` records per key).
- `kblockdblib/src/coord.rs`  -- `Coord`, a small-vec-style `i32` sequence (inline
  up to 8 axes, heap beyond that) used for coordinates and chunk keys.
  Signed so a coordinate can be negative (see "Coordinate space" below). A
  world's axis count is a runtime value (see `params.rs`), so a coordinate
  can't be a fixed-size array the way a single-world-shape version of this
  prototype could use -- `Coord` avoids a `Vec<i32>`-per-coordinate heap
  allocation for the common case (a handful of axes) without adding a
  `smallvec` dependency.
- `kblockdblib/src/chunk_cache.rs` -- `ChunkCache`, the in-memory,
  per-chunk `RwLock<Option<Chunk>>` table `World::with_chunk_read`/
  `with_chunk_write` lock and cache against, bounded by an LRU eviction
  policy once it hits its configured `max_entries` (see "Concurrency"
  above). Private to the crate -- an implementation detail, not part of the public
  API.
- `kblockdblib/src/params.rs` -- `WorldParams` (axes, world_dim), read/written as
  `world.txt` at the world root, and `create_or_validate`, the one
  lock-guarded operation `World::create` needs.
- `kblockdblib/src/schema.rs` -- global key-string <-> id registry (`schema.txt`),
  lock-guarded so concurrent interning of different new keys can't collide.
  Also records each key's value type on first use and enforces it on every
  later `intern` call, world-wide (see `ValueType` in `value.rs`).
- `kblockdblib/src/chunk.rs`  -- the columnar chunk: bitset, columns, per-cell
  `CellMeta`, binary serialization, unit tests. Its cell count
  (`chunk_cells(axes, chunk_dim)`) is computed at runtime from the owning
  world's axis count and chunk size (both per-world runtime parameters,
  not compile-time constants -- see the axis-count/chunking bullets above).
- `kblockdblib/src/index.rs`  -- `ValueIndex`, the secondary index
  registry a key can be built on (see "Indexes" above): one
  `crate::lsm::LsmIndex` per indexed key id, plus the `Value` ->
  order-preserving-bytes encoding (`sortable_bytes`) `lsm.rs` itself
  stays opaque to. `World` is what backfills a brand new index and keeps
  every existing one in sync with every write; this module has no notion
  of chunk storage of its own.
- `kblockdblib/src/lsm.rs`  -- `LsmIndex`, the on-disk log-structured-merge
  engine one secondary index's directory is -- memtable, WAL, immutable
  sorted segments with a sparse in-memory footer index, and single-tier
  compaction. Generic over `Vec<u8>` keys and `Coord`s; knows nothing
  about `Value`/`World`.
- `kblockdblib/src/world.rs`  -- `World::create`/`open`, coordinate -> chunk
  mapping, chunk file paths, `with_chunk_read`/`with_chunk_write` (the
  cache-then-apply[-then-write] cycle every operation goes through),
  `get`/`set`/`remove`/`flush`, and their `*_region` counterparts for
  arbitrary axis-aligned boxes of cells (`Region`) that may span or
  partially cover any number of chunks. `get_meta`/`get_with_meta` read a
  cell/key's `CellMeta` (the latter atomically alongside its value, from
  the same chunk snapshot). `get`/`set`/`remove` always validate the
  coordinate itself before consulting anything else (key existence,
  schema, ...) -- this crate is a library other code (like
  `kblockdbserver`) calls with unvalidated/attacker-controlled input, so a
  malformed coordinate is always rejected the same way rather than
  sometimes being silently absorbed by an unrelated short-circuit. Also
  `stats()` -- a live filesystem walk totaling chunk count, size, and
  disk-block usage into a `Stats`, which `kblockdbserver` exposes per
  database as `/rest/db/{db}/stats` -- and `list_cells()`, a heavier live
  walk that decodes every chunk file into a sorted `Vec<CellEntry>` (every
  populated cell's full keys/values/metadata), which `kblockdbserver`'s
  data browser (`/`, with a database dropdown) is built on.
- `kblockdblib/src/logger.rs` -- minimal dependency-free logger, appends to
  `kblockdblib.log` in the working directory.
- `kblockdblib/src/main.rs`   -- demo/benchmark driver (the `kblockdblib` binary).
