# kdb

A Cargo workspace with two crates:

- **`kdb`** -- a prototype storage engine for a huge simulation grid where
  every cell is its own key/value store (string keys; string, f64, or i64
  values), sized for something like 10,000 x 10,000 x 10,000 cells (1
  trillion cells) -- too big for one-file-per-cell or an RDBMS row-per-cell.
  Zero external dependencies -- pure `std`. A library first, with a small
  demo/benchmark binary (`kdb`) built on top of it.
- **`kdbserver`** -- a RESTful HTTP server that embeds `kdb` as a library
  and exposes `get`/`set`/`remove` for individual cells and for
  axis-aligned regions of cells over HTTP. Unlike `kdb`, it takes on the
  standard modern Rust web stack (axum + tokio + serde) -- that
  dependency-free constraint was specific to `kdb`'s storage format, not to
  everything built on top of it.

```
kdb/         the storage engine (library `kdb` + demo binary `kdb`)
kdbserver/   the REST server (binary `kdbserver`, depends on kdb)
```

## `kdb`: the storage engine

### Design

- **Chunking, not one file per cell.** The world is split into 32x32x32
  cell chunks (32,768 cells/chunk). A 10,000^3 world needs 313 chunks/axis
  (313^3 ~= 30.6M chunks total), and each chunk is at most one file. Chunks
  with no data at all are never written, so a mostly-empty world costs disk
  space proportional to what's actually populated.

- **Columnar storage per chunk, not a hashmap per cell.** Each chunk holds
  one sparse array ("column") per key that actually appears somewhere in
  that chunk -- not one per cell. A column is a fixed-size presence bitmap
  (1 bit/cell) plus a dense array of values for only the cells that are
  set. This avoids repeating key strings a trillion times and is the layout
  simulation code actually wants: "add 1.0 to temperature for every cell in
  this chunk" touches one contiguous array, not a trillion separate hashmap
  lookups.

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

### A real trade-off this prototype makes visible

The presence bitmap is a *fixed* size per column regardless of how many
cells in the chunk actually use that key. That's cheap when chunks are
densely populated (real simulations usually have spatial locality: nearby
cells tend to be "on" together), but it's wasteful in the pathological case
where a chunk holds only one or two populated cells -- which is exactly
what the demo's random scatter produces, on purpose, as a worst case.

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

### Layout

- `kdb/src/lib.rs`    -- the library crate root; re-exports `World`,
  `Value`, `Region`, `Coord`, `WorldParams`, `AXES`, `WORLD_DIM`.
- `kdb/src/value.rs`  -- the `Value` enum (Str/F64/I64) and its type tags.
- `kdb/src/coord.rs`  -- `Coord`, a small-vec-style `u32` sequence (inline
  up to 8 axes, heap beyond that) used for coordinates and chunk keys. A
  world's axis count is a runtime value (see `params.rs`), so a coordinate
  can't be a fixed-size array the way a single-world-shape version of this
  prototype could use -- `Coord` avoids a `Vec<u32>`-per-coordinate heap
  allocation for the common case (a handful of axes) without adding a
  `smallvec` dependency.
- `kdb/src/params.rs` -- `WorldParams` (axes, world_dim), read/written as
  `world.txt` at the world root.
- `kdb/src/schema.rs` -- global key-string <-> id registry (`schema.txt`).
- `kdb/src/chunk.rs`  -- the columnar chunk: bitset, columns, binary
  serialization, unit tests. Its cell count (`chunk_cells(axes)`) is
  computed at runtime from the owning world's axis count.
- `kdb/src/world.rs`  -- `World::create`/`open`, coordinate -> chunk
  mapping, chunk file paths, the bounded LRU-ish cache, `get`/`set`/
  `remove`/`flush`, and their `*_region` counterparts for arbitrary
  axis-aligned boxes of cells (`Region`) that may span or partially cover
  any number of chunks. `get`/`set`/`remove` always validate the
  coordinate itself before consulting anything else (key existence,
  schema, ...) -- this crate is a library other code (like `kdbserver`)
  calls with unvalidated/attacker-controlled input, so a malformed
  coordinate is always rejected the same way rather than sometimes being
  silently absorbed by an unrelated short-circuit.
- `kdb/src/logger.rs` -- minimal dependency-free logger, appends to
  `kdb.log` in the working directory.
- `kdb/src/main.rs`   -- demo/benchmark driver (the `kdb` binary).

## `kdbserver`: the REST server

### Build & run

```sh
cargo build --release
cargo run -p kdbserver -- --data-dir ./data --addr 127.0.0.1:8080
```

```
USAGE:
    kdbserver [OPTIONS]

OPTIONS:
    --data-dir <path>    World data directory (default: ./data)
    --axes <n>           Axis count for a brand-new world (default: 3)
    --world-dim <n>      Cells per axis for a brand-new world (default: 10000)
    --addr <host:port>   Address to listen on (default: 127.0.0.1:8080)
    -h, --help           Print help
```

`--axes`/`--world-dim` only matter the *first* time a world is created at
`--data-dir` (via `World::create`); reopening an existing one reads its
real shape back from its `world.txt` and ignores these flags.

### REST API

Every coordinate, and every region origin/extent, is a comma-separated
list of `u32`s in the URL, one per axis (`1,2,3` for a 3-axis world) --
there's nothing 3-axis-specific about the API; it works the same way for
whatever axis count the world was created with.

A cell value on the wire is a small tagged JSON object:

```json
{"type": "str", "value": "stone"}
{"type": "f64", "value": 2.6}
{"type": "i64", "value": 7}
```

| Method | Path | Body | Response |
|---|---|---|---|
| `GET` | `/health` | | `200` `{"status":"ok","axes":3,"world_dim":10000}` |
| `GET` | `/cells/{coords}/{key}` | | `200 {"value": <value>}`, or `404` if unset |
| `PUT` | `/cells/{coords}/{key}` | `<value>` | `204` |
| `DELETE` | `/cells/{coords}/{key}` | | `204` |
| `GET` | `/regions/{origin}/{extent}/{key}` | | `200 {"values": [<value or null>, ...]}` |
| `PUT` | `/regions/{origin}/{extent}/{key}` | `{"values": [<value>, ...]}` | `204` |
| `DELETE` | `/regions/{origin}/{extent}/{key}` | | `204` |

Region `values` arrays are in axis-0-fastest order (matching
`kdb::World::get_region`/`set_region`): index `i` is offset
`(i % extent[0], (i / extent[0]) % extent[1], ...)` from `origin`. A `PUT`
to a region must supply exactly one value per cell (`extent[0] * extent[1]
* ...`), in that order, or it fails with `400`.

Errors are `{"error": "<message>"}`, with the status code reflecting the
cause: `400` for a malformed/out-of-range coordinate, a region whose axis
count doesn't match the world's, or a `values` array of the wrong length
(all of these are `kdb`'s own validation surfacing through); `404` for a
`GET` that found nothing; `500` for anything on the server's side (disk
I/O, ...).

```sh
# set a cell
curl -X PUT localhost:8080/cells/1,2,3/material \
  -H 'content-type: application/json' -d '{"type":"str","value":"stone"}'

# read it back
curl localhost:8080/cells/1,2,3/material
# {"value":{"type":"str","value":"stone"}}

# fill an 8x8x8 region with distinct per-cell values (512 of them, omitted here)
curl -X PUT localhost:8080/regions/0,0,0/8,8,8/material \
  -H 'content-type: application/json' -d '{"values":[...]}'

# read the whole region back
curl localhost:8080/regions/0,0,0/8,8,8/material

# clear a cell / a region
curl -X DELETE localhost:8080/cells/1,2,3/material
curl -X DELETE localhost:8080/regions/0,0,0/8,8,8/material
```

### Layout

- `kdbserver/src/main.rs`       -- CLI arg parsing, opens the world, starts
  the server (with graceful shutdown on Ctrl+C).
- `kdbserver/src/routes.rs`     -- the router and all HTTP handlers.
- `kdbserver/src/state.rs`      -- `AppState` (the shared, mutex-guarded
  `World`) and `with_world`, which runs each `World` call on a
  `spawn_blocking` thread so `World`'s synchronous file I/O never blocks
  the async runtime.
- `kdbserver/src/value_json.rs` -- `ValueJson`, the JSON wire format for
  `kdb::Value` (kept in this crate, not `kdb`, since `kdb` itself doesn't
  depend on `serde`).
- `kdbserver/src/coords.rs`     -- parses the comma-separated coordinate
  path segments.
- `kdbserver/src/error.rs`      -- `ApiError`, the one error type every
  handler returns, and its mapping to HTTP status codes (including
  `From<std::io::Error>`, so `kdb`'s own `InvalidInput`/`NotFound` errors
  become `400`/`404` automatically).
- `kdbserver/src/tests.rs`      -- HTTP-level integration tests (real
  requests through the real `Router` via `tower::ServiceExt::oneshot`, no
  TCP socket needed).

## Build & test everything

```sh
cargo build --workspace --release
cargo test --workspace
```
