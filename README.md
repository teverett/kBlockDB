# kdb

Prototype storage engine for a huge simulation grid where every cell is its
own key/value store (string keys; string, f64, or i64 values), sized for
something like 10,000 x 10,000 x 10,000 cells (1 trillion cells) -- too big
for one-file-per-cell or an RDBMS row-per-cell.

Zero external dependencies -- pure `std`, so `cargo build` needs no network
access and `rustc` alone could compile it if you inlined the modules.

## Design

- **Chunking, not one file per cell.** The world is split into 32x32x32
  cell chunks (32,768 cells/chunk). A 10,000^3 world needs 313 chunks/axis
  (313^3 ~= 30.6M chunks total), and each chunk is at most one file. Chunks
  with no data at all are never written, so a mostly-empty world costs disk
  space proportional to what's actually populated.

- **Columnar storage per chunk, not a hashmap per cell.** Each chunk holds
  one sparse array ("column") per key that actually appears somewhere in
  that chunk -- not one per cell. A column is a fixed-size presence bitmap
  (1 bit/cell = 4096 bytes) plus a dense array of values for only the cells
  that are set. This avoids repeating key strings a trillion times and is
  the layout simulation code actually wants: "add 1.0 to temperature for
  every cell in this chunk" touches one contiguous array, not a trillion
  separate hashmap lookups.

- **Global key interning.** Key strings ("temperature", "material", ...)
  are interned once into small integer ids in `schema.txt` at the world
  root (append-only, so ids never change). Chunks store columns by id, not
  by string.

- **Bounded in-memory cache.** `World` keeps at most a couple hundred
  chunks resident and evicts (flushing dirty ones) beyond that, so the
  process addressing a trillion-cell world only ever holds tens of MB in
  RAM.

- **Directory nesting.** Chunk files live at `<root>/<c0>/<c1>/.../<cn>.chunk`
  (one path segment per axis), which keeps any one directory to at most
  `chunks_per_axis()` entries no matter how large the world gets.

- **Axis count and per-axis size are a world property, not a build-time
  constant.** A world can have any number of axes (2D, 3D, 4D, ...) and any
  `world_dim`; both are chosen once, when the world is created
  (`World::create(root, axes, world_dim)`), and persisted to `world.txt` at
  the world root. `World::open` reads them back from that file rather than
  assuming a default, and a later `World::create` against the same
  directory must pass matching numbers or it fails with `InvalidInput`
  *without touching anything* -- a world's shape can't silently change out
  from under data already written for it. `world::AXES`/`world::WORLD_DIM`
  are only the defaults `main`'s demo happens to call `create` with.

## A real trade-off this prototype makes visible

The presence bitmap is a *fixed* 4KB per column regardless of how many
cells in the chunk actually use that key. That's cheap when chunks are
densely populated (real simulations usually have spatial locality: nearby
cells tend to be "on" together), but it's wasteful in the pathological case
where a chunk holds only one or two populated cells -- which is exactly
what the demo's random scatter produces, on purpose, as a worst case. Run
it and you'll see ~5000 chunks average ~12KB each for a single populated
cell apiece.

Two straightforward next steps if that trade-off matters for your actual
access pattern:

1. **Compress each chunk file** (e.g. zstd/gzip) before writing -- a bitmap
   that's almost all zero bytes compresses extremely well, cheaply.
2. **Switch to a run-length or sparse-index presence encoding** (a list of
   set cell indices) instead of a flat bitmap when a chunk's occupancy is
   very low, and only use the flat bitmap once occupancy passes some
   threshold (this is basically what real sparse-array libraries like Zarr
   do internally).

Neither is implemented here -- the prototype optimizes for showing the
mechanism clearly, not for the last byte of density.

## Build & run

```sh
cargo build --release
cargo test               # unit tests for the chunk binary format and World
./target/release/kdb ./data   # runs a small demo: writes ~5000 scattered
                               # cells, reads some back, reports disk usage.
                               # Re-run with the same data dir and it loads
                               # existing chunks instead of starting empty.
```

## Layout

- `src/value.rs`  -- the `Value` enum (Str/F64/I64) and its type tags.
- `src/coord.rs`  -- `Coord`, a small-vec-style `u32` sequence (inline up
  to 8 axes, heap beyond that) used for coordinates and chunk keys. A
  world's axis count is a runtime value (see `params.rs`), so a coordinate
  can't be a fixed-size array the way a single-world-shape version of this
  prototype could use -- `Coord` avoids a `Vec<u32>`-per-coordinate heap
  allocation for the common case (a handful of axes) without adding a
  `smallvec` dependency.
- `src/params.rs` -- `WorldParams` (axes, world_dim), read/written as
  `world.txt` at the world root.
- `src/schema.rs` -- global key-string <-> id registry (`schema.txt`).
- `src/chunk.rs`  -- the columnar chunk: bitset, columns, binary
  serialization, unit tests. Its cell count (`chunk_cells(axes)`) is
  computed at runtime from the owning world's axis count.
- `src/world.rs`  -- `World::create`/`open`, coordinate -> chunk mapping,
  chunk file paths, the bounded LRU-ish cache, `get`/`set`/`remove`/
  `flush`, and their `*_region` counterparts for arbitrary axis-aligned
  boxes of cells (`Region`) that may span or partially cover any number of
  chunks.
- `src/logger.rs` -- minimal dependency-free logger, appends to `kdb.log`.
- `src/main.rs`   -- demo/benchmark driver.
