# kdb

A Cargo workspace with three crates:

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
- **`kdbperf`** -- a performance test suite that drives a real `kdbserver`
  (or several) over real HTTP and measures it: single-cell and region
  throughput/latency, concurrency scaling, lock contention, and multi-process
  scaling across several instances sharing one data directory.

```
kdb/         the storage engine (library `kdb` + demo binary `kdb`)
kdbserver/   the REST server (binary `kdbserver`, depends on kdb)
kdbperf/     the performance test suite (binary `kdbperf`, drives kdbserver over HTTP)
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

- **No client-side cache -- every operation is durable and lock-guarded on
  its own.** `get`/`set`/`remove` lock (shared for a read, exclusive for a
  write), read, apply, and (for writes) write back exactly the chunk
  file(s) they touch, then release, all before returning -- see
  "Concurrency" below. A `set` is durable the instant its call returns;
  there's nothing to evict or flush, and a process addressing a
  trillion-cell world only ever holds the one or two chunks a given call
  actually needs in memory.

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

### Concurrency

Any number of processes -- multiple `kdbserver` instances included -- can
safely share one world directory. There's no in-process cache to go stale
or to lose an update on eviction: every `get`/`set`/`remove` (and, at chunk
granularity, every `*_region` call) takes an OS-level advisory lock
(`kdb/src/lock.rs`, via `std::fs::File::lock`/`lock_shared` -- stable in
`std`, so this needs no dependency either) on exactly the chunk file(s) it
touches, shared for a read and exclusive for a write, reads that chunk's
*current* on-disk contents fresh, applies the change, and (for a write)
writes it straight back before releasing the lock and returning. The
schema (`schema.txt`) gets the same treatment: interning a new key takes an
exclusive lock and re-reads the file first, so two processes racing to
intern two *different* new keys can't collide on the same id (which used
to be able to corrupt `schema.txt` badly enough that even reopening the
world later would fail). `World::create` is similarly race-free: two
processes racing to create the very same fresh directory, possibly with
*different* `axes`/`world_dim`, resolve safely -- one creates it, the other
sees what was just created and is validated against it like any other
pre-existing world, under one exclusive lock around the whole
check-then-maybe-write sequence.

What this does *not* give you is cross-call atomicity: a `get` immediately
followed by a `set` from the same caller is two separate locked
operations, not one transaction, so another process's write can land in
between them -- same as most simple key/value stores without an explicit
read-modify-write or transaction API.

The cost of dropping the in-process cache is that every single-cell
operation is now its own disk round trip (a chunk touched by four `set`
calls in a row, even for four different keys on the same cell, is four
separate chunk-file rewrites, not one batched one) -- correctness across
processes traded away the free win a private, unsynchronized cache used to
give a single process. `get_region`/`set_region`/`remove_region` claw part
of that back for the common case where a region spans far fewer chunks
than cells: they group a region's cells by chunk first, so each chunk a
region touches is still locked, read, and (for a write) written back
exactly once, no matter how many of the region's cells land in it.

**Every `World` method takes `&self`, not `&mut self`.** `World` holds no
cache -- its only interior state is a locked `Schema` and a couple of
atomic counters -- so a single process can share one `World` behind a
plain `Arc` (no `Mutex<World>` needed) and let concurrent calls actually
run concurrently, limited only by the same per-chunk file locks that
already make concurrent *processes* safe. `kdbserver` does exactly this.

**Concurrent filesystem operations don't scale indefinitely, though.**
Measured on one real, fast, local SSD: going from 1 to 8-32 concurrent
`with_chunk` calls (disjoint chunks, so no lock contention between them)
roughly doubled throughput, as expected -- but pushing to 128 concurrent
calls made aggregate throughput *worse* than at 1, not just diminishing --
confirmed to be real OS/filesystem-level contention (reproduced with a
synthetic probe doing only `create_dir_all`/`flock`/file I/O, no `kdb` code
at all, and ruled out an in-memory `Mutex` bottleneck the same way: a pure
lock-contention probe at the same thread count showed no degradation at
all). `World` caps how many `with_chunk` calls run concurrently
(`DEFAULT_MAX_CONCURRENT_DISK_OPS`, `World::with_max_concurrent_disk_ops`
to override) for exactly this reason. The right cap is a property of the
underlying filesystem/storage, not of `kdb` -- the default is a reasonable
starting point, not a measured optimum for any particular deployment;
measure yours with `kdbperf`'s `concurrency_scan`.

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
- `kdb/src/lock.rs`   -- `FileLock`, the RAII wrapper around
  `std::fs::File::lock`/`lock_shared` everything else in this list uses for
  cross-process locking (see "Concurrency" above). Private to the crate --
  an implementation detail, not part of the public API.
- `kdb/src/params.rs` -- `WorldParams` (axes, world_dim), read/written as
  `world.txt` at the world root, and `create_or_validate`, the one
  lock-guarded operation `World::create` needs.
- `kdb/src/schema.rs` -- global key-string <-> id registry (`schema.txt`),
  lock-guarded so concurrent interning of different new keys can't collide.
- `kdb/src/chunk.rs`  -- the columnar chunk: bitset, columns, binary
  serialization, unit tests. Its cell count (`chunk_cells(axes)`) is
  computed at runtime from the owning world's axis count.
- `kdb/src/world.rs`  -- `World::create`/`open`, coordinate -> chunk
  mapping, chunk file paths, `with_chunk` (the lock-read-apply-write cycle
  every operation goes through), `get`/`set`/`remove`/`flush`, and their
  `*_region` counterparts for arbitrary axis-aligned boxes of cells
  (`Region`) that may span or partially cover any number of chunks.
  `get`/`set`/`remove` always validate the coordinate itself before
  consulting anything else (key existence, schema, ...) -- this crate is a
  library other code (like `kdbserver`) calls with unvalidated/
  attacker-controlled input, so a malformed coordinate is always rejected
  the same way rather than sometimes being silently absorbed by an
  unrelated short-circuit.
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
    --data-dir <path>              World data directory (default: ./data)
    --axes <n>                     Axis count for a brand-new world (default: 3)
    --world-dim <n>                Cells per axis for a brand-new world (default: 10000)
    --addr <host:port>             Address to listen on (default: 127.0.0.1:8080)
    --max-concurrent-disk-ops <n>  Cap on concurrent filesystem operations
                                    (default: 32 -- see kdb's "Concurrency"
                                    section; measure the right value for your
                                    filesystem with kdbperf's concurrency_scan)
    -h, --help                     Print help
```

`--axes`/`--world-dim` only matter the *first* time a world is created at
`--data-dir` (via `World::create`); reopening an existing one reads its
real shape back from its `world.txt` and ignores these flags.

**Multiple `kdbserver` instances can safely point `--data-dir` at the same
directory** -- e.g. several instances behind a load balancer -- and read
and write concurrently without corrupting anything. `kdb` itself is what
makes that safe (see its "Concurrency" section above); this server doesn't
need to know or do anything special.

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

## `kdbperf`: the performance test suite

Drives a real `kdbserver` process (by default, one or more it spawns and
tears down itself) over real HTTP and measures it -- this is a measurement
of what a client actually experiences, not a microbenchmark of `kdb`'s
internals.

### Build & run

```sh
cargo build --workspace --release
./target/release/kdbperf                       # spawns its own instance(s), runs every scenario
./target/release/kdbperf --scenario set_cell    # just one scenario
./target/release/kdbperf --json > results.json  # machine-readable output
./target/release/kdbperf --url http://localhost:8080   # target an already-running instance instead
```

Run `--help` for the full flag list (concurrency levels, op counts, region
sizes, instance count, etc.) -- everything has a default chosen to finish a
full run in well under a minute, and every default is overridable.

### Scenarios

- **`set_cell` / `get_cell` / `remove_cell`** -- sequential (one client, no
  concurrency) single-cell operations on `--cells` distinct cells.
  `get_cell`/`remove_cell` populate their own data first (untimed), so
  they measure just the operation named, and are runnable on their own.
- **`region`** -- `set_region`/`get_region` at each edge length in
  `--region-edges` (a cube of that edge on every axis), repeated
  `--region-reps` times. Because `kdb`'s region methods touch each chunk a
  region spans exactly once regardless of how many cells land in it (see
  `kdb`'s "Concurrency" section above), these routinely report far higher
  effective cells/sec than the single-cell scenarios -- that gap *is* the
  batching win region operations exist for.
- **`concurrency_scan`** -- `set` from `--concurrency` concurrent clients,
  each on its own disjoint cells (different chunks, typically), at each
  level in the list. Shows how throughput scales with concurrency when
  requests don't contend for the same chunk.
- **`contended_cell`** -- the same concurrency sweep, but every client
  targets the *same* cell (different keys, so it's not just racing an
  identical overwrite). `kdb::World::set` takes an exclusive lock on that
  cell's chunk file per call and must read-modify-write the *whole* chunk
  file every time (more expensive the more distinct keys have accumulated
  in it), so this is typically much slower than `concurrency_scan` even at
  the same concurrency level -- contrasting the two is the point.
- **`multi_instance`** -- the same disjoint-cell concurrency sweep, run
  once against one server and once against several servers (`--instances`,
  sharing one data directory, round-robin) at the same total concurrency.
  A single `kdbserver` process serializes every request through one
  `Mutex<World>` regardless of `kdb`'s own per-chunk locking (see
  `kdbserver/src/state.rs`), so that lock only stops being the bottleneck
  once there's more than one *process* to spread load across -- this is
  the scenario that actually demonstrates multi-process scaling, and the
  reason `kdbperf` spawns multiple instances by default. Interpreting the
  result honestly: on one machine, multiple `kdbserver` processes also
  compete for the same CPU cores and disk, so how much (if any) improvement
  shows up depends on real available headroom -- this scenario is most
  meaningful comparing genuinely separate deployments (e.g. `--url` pointed
  at instances on different machines/containers), where that competition
  doesn't exist.

### Layout

- `kdbperf/src/main.rs`      -- CLI parsing and orchestration: spawn or
  connect to server(s), run the selected scenarios, print the report.
- `kdbperf/src/client.rs`    -- a thin async HTTP client for kdbserver's
  REST API (every value used is an `i64`, so payload shape stays constant
  across scenarios).
- `kdbperf/src/server.rs`    -- `ManagedServer`, which spawns a `kdbserver`
  child process and kills it on drop, and locates the `kdbserver` binary
  built alongside this one.
- `kdbperf/src/scenarios.rs` -- the scenarios themselves.
- `kdbperf/src/stats.rs`     -- latency percentiles and throughput,
  computed from a plain sorted `Vec<Duration>` (sample counts here are
  thousands, not millions -- a histogram crate would be solving a problem
  this doesn't have).
- `kdbperf/src/report.rs`    -- table/JSON output.
- `kdbperf/src/tests.rs`     -- integration tests that spawn a real
  `kdbserver` (or two, sharing one data dir) and run a real scenario
  against it.

## Build & test everything

```sh
cargo build --workspace --release
cargo test --workspace
```
