# kBlockDB

A Cargo workspace with four crates:

- **`kblockdblib`** -- a prototype storage engine for a huge simulation grid where
  every cell is its own key/value store (string keys; string, f64, or i64
  values), sized for something like 10,000 x 10,000 x 10,000 cells (1
  trillion cells) -- too big for one-file-per-cell or an RDBMS row-per-cell.
  Zero external dependencies -- pure `std`. A library first, with a small
  demo/benchmark binary (`kblockdblib`) built on top of it.
- **`kblockdbserver`** -- a RESTful HTTP server that embeds `kblockdblib` as a library
  and exposes `get`/`set`/`remove` for individual cells and for
  axis-aligned regions of cells over HTTP. Unlike `kblockdblib`, it takes on the
  standard modern Rust web stack (axum + tokio + serde) -- that
  dependency-free constraint was specific to `kblockdblib`'s storage format, not to
  everything built on top of it.
- **`kblockdbperf`** -- a performance test suite that drives a real `kblockdbserver`
  (or several) over real HTTP and measures it: single-cell and region
  throughput/latency, concurrency scaling, lock contention, and multi-process
  scaling across several instances sharing one data directory.
- **`kblockdbcli`** -- a small command-line client for kblockdbserver's REST API:
  `get`/`set`/`remove` a single cell's value from a shell, authenticating
  like `curl -u` would. A pure HTTP client, same as `kblockdbperf` -- it treats
  kblockdbserver as a black box over its REST API, not `kblockdblib` directly.

```
kblockdblib/         the storage engine (library `kblockdblib` + demo binary `kblockdblib`)
kblockdbserver/   the REST server (binary `kblockdbserver`, depends on kblockdblib)
kblockdbperf/     the performance test suite (binary `kblockdbperf`, drives kblockdbserver over HTTP)
kblockdbcli/      the command-line client (binary `kblockdbcli`, drives kblockdbserver over HTTP)
```

## `kblockdblib`: the storage engine

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

- **Global key interning, type fixed on first use.** Key strings
  ("temperature", "material", ...) are interned once into small integer ids
  in `schema.txt` at the world root (append-only, so ids never change), and
  the value type (`str`/`f64`/`i64`) `set` first used for that key is
  recorded alongside the id and permanently fixed from then on, world-wide
  -- a later `set` for the same key with a different type fails with a
  normal `InvalidInput` error rather than writing anything. Chunks store
  columns by id, not by string.

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

Any number of processes -- multiple `kblockdbserver` instances included -- can
safely share one world directory. There's no in-process cache to go stale
or to lose an update on eviction: every `get`/`set`/`remove` (and, at chunk
granularity, every `*_region` call) takes an OS-level advisory lock
(`kblockdblib/src/lock.rs`, via `std::fs::File::lock`/`lock_shared` -- stable in
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
already make concurrent *processes* safe. `kblockdbserver` does exactly this.

**Concurrent filesystem operations don't scale indefinitely, though.**
Measured on one real, fast, local SSD: going from 1 to 8-32 concurrent
`with_chunk` calls (disjoint chunks, so no lock contention between them)
roughly doubled throughput, as expected -- but pushing to 128 concurrent
calls made aggregate throughput *worse* than at 1, not just diminishing --
confirmed to be real OS/filesystem-level contention (reproduced with a
synthetic probe doing only `create_dir_all`/`flock`/file I/O, no `kblockdblib` code
at all, and ruled out an in-memory `Mutex` bottleneck the same way: a pure
lock-contention probe at the same thread count showed no degradation at
all). `World` caps how many `with_chunk` calls run concurrently
(`DEFAULT_MAX_CONCURRENT_DISK_OPS`, `World::with_max_concurrent_disk_ops`
to override) for exactly this reason. The right cap is a property of the
underlying filesystem/storage, not of `kblockdblib` -- the default is a reasonable
starting point, not a measured optimum for any particular deployment;
measure yours with `kblockdbperf`'s `concurrency_scan`.

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

### `Chunk`'s two per-cell costs, and which one is fixed

Every `get`/`set`/`remove` on a column needs that cell's *rank* (its
position among the column's set cells) to index into the column's dense,
packed value array. That costs two different things:

- **Computing the rank** used to be an O(presence_bytes) linear popcount
  scan of the whole bitmap before the target bit -- up to ~4096 byte
  popcounts per call for the default chunk shape, on *every* `get`/`set`/
  `remove`, since there's no cache to amortize it across calls anymore
  (see "Concurrency" above). `Bitset` now keeps a small per-64-byte-block
  running-count index (`block_counts`, updated in O(1) on every `set`,
  never persisted -- rebuilt once when a chunk is read off disk), turning
  `rank` into summing a handful of block counts plus scanning one partial
  block: measured (a standalone probe, not this crate's own benchmarking
  machinery, comparing both implementations directly) at roughly 4x faster
  on average and ~7x faster for a cell near the end of a densely-populated
  bitmap -- the case this project's own design rationale says is the
  common one, not the pathological one.
- **Using the rank** to insert/remove a value still means an
  `Vec::insert`/`Vec::remove` into `ColumnData`'s packed array -- an
  O(occupancy) shift of every value after that point in the column. This
  one is *not* fixed here: doing so would mean allocating a full
  `chunk_cells`-sized slot per column instead of a packed one (the same
  "fixed size regardless of occupancy" trade-off the presence bitmap
  already makes, extended to values too), which is a real, opposite-facing
  cost -- multiplying a sparse column's on-disk/in-memory footprint by
  potentially tens of thousands, in exactly the scattered/sparse-chunk
  case this README already calls out as the worst case this prototype
  demonstrates on purpose. Worth doing if your workload is genuinely
  dense-per-chunk and memory isn't the constraint; not done by default
  because it isn't, universally.

### Layout

- `kblockdblib/src/lib.rs`    -- the library crate root; re-exports `World`,
  `Value`, `Region`, `Coord`, `WorldParams`, `AXES`, `WORLD_DIM`.
- `kblockdblib/src/value.rs`  -- the `Value` enum (Str/F64/I64), its on-disk type
  tags, and `ValueType` (a `Value` without the value itself -- what
  `Schema` records per key).
- `kblockdblib/src/coord.rs`  -- `Coord`, a small-vec-style `u32` sequence (inline
  up to 8 axes, heap beyond that) used for coordinates and chunk keys. A
  world's axis count is a runtime value (see `params.rs`), so a coordinate
  can't be a fixed-size array the way a single-world-shape version of this
  prototype could use -- `Coord` avoids a `Vec<u32>`-per-coordinate heap
  allocation for the common case (a handful of axes) without adding a
  `smallvec` dependency.
- `kblockdblib/src/lock.rs`   -- `FileLock`, the RAII wrapper around
  `std::fs::File::lock`/`lock_shared` everything else in this list uses for
  cross-process locking (see "Concurrency" above). Private to the crate --
  an implementation detail, not part of the public API.
- `kblockdblib/src/params.rs` -- `WorldParams` (axes, world_dim), read/written as
  `world.txt` at the world root, and `create_or_validate`, the one
  lock-guarded operation `World::create` needs.
- `kblockdblib/src/schema.rs` -- global key-string <-> id registry (`schema.txt`),
  lock-guarded so concurrent interning of different new keys can't collide.
  Also records each key's value type on first use and enforces it on every
  later `intern` call, world-wide (see `ValueType` in `value.rs`).
- `kblockdblib/src/chunk.rs`  -- the columnar chunk: bitset, columns, binary
  serialization, unit tests. Its cell count (`chunk_cells(axes)`) is
  computed at runtime from the owning world's axis count.
- `kblockdblib/src/world.rs`  -- `World::create`/`open`, coordinate -> chunk
  mapping, chunk file paths, `with_chunk` (the lock-read-apply-write cycle
  every operation goes through), `get`/`set`/`remove`/`flush`, and their
  `*_region` counterparts for arbitrary axis-aligned boxes of cells
  (`Region`) that may span or partially cover any number of chunks.
  `get`/`set`/`remove` always validate the coordinate itself before
  consulting anything else (key existence, schema, ...) -- this crate is a
  library other code (like `kblockdbserver`) calls with unvalidated/
  attacker-controlled input, so a malformed coordinate is always rejected
  the same way rather than sometimes being silently absorbed by an
  unrelated short-circuit. Also `stats()` -- a live filesystem walk
  totaling chunk count, size, and disk-block usage into a `Stats`, which
  `kblockdbserver` exposes as `/stats`.
- `kblockdblib/src/logger.rs` -- minimal dependency-free logger, appends to
  `kblockdblib.log` in the working directory.
- `kblockdblib/src/main.rs`   -- demo/benchmark driver (the `kblockdblib` binary).

## `kblockdbserver`: the REST server

### Build & run

```sh
cargo build --release
cargo run -p kblockdbserver -- --data-dir ./data --addr 127.0.0.1:8080
```

A default `kblockdbserver.toml` (admin/`changeme`, see below) is checked in at
the repo root so this works out of the box -- **change `admin_password`
before running this anywhere reachable by anyone you don't trust.**

```
USAGE:
    kblockdbserver [OPTIONS]

OPTIONS:
    --config <path>                Config file (default: ./kblockdbserver.toml). Required --
                                    holds admin_password and, optionally, [[users]], plus
                                    optional addr/data_dir/axes/world_dim/
                                    max_concurrent_disk_ops (each overridden by the
                                    matching CLI flag below, if given)
    --data-dir <path>              World data directory (default: ./data)
    --axes <n>                     Axis count for a brand-new world (default: 3)
    --world-dim <n>                Cells per axis for a brand-new world (default: 10000)
    --addr <host:port>             Address to listen on (default: 127.0.0.1:8080)
    --max-concurrent-disk-ops <n>  Cap on concurrent filesystem operations
                                    (default: 32 -- see kblockdblib's "Concurrency"
                                    section; measure the right value for your
                                    filesystem with kblockdbperf's concurrency_scan)
    -h, --help                     Print help
```

`--axes`/`--world-dim` only matter the *first* time a world is created at
`--data-dir` (via `World::create`); reopening an existing one reads its
real shape back from its `world.txt` and ignores these flags.

**Multiple `kblockdbserver` instances can safely point `--data-dir` at the same
directory** -- e.g. several instances behind a load balancer -- and read
and write concurrently without corrupting anything. `kblockdblib` itself is what
makes that safe (see its "Concurrency" section above); this server doesn't
need to know or do anything special.

### Config file

`--config` (default `./kblockdbserver.toml`) is TOML and is required to start
the server -- it's the only place credentials can come from (never a CLI
flag, so they don't end up in shell history or `ps` output):

```toml
addr = "127.0.0.1:8080"      # optional; same defaults/precedence as the CLI flags
data_dir = "./data"          # optional
axes = 3                     # optional
world_dim = 10000            # optional
max_concurrent_disk_ops = 32 # optional
admin_password = "change-me" # required

[[users]]
username = "alice"
password = "alice-password"

[[users]]
username = "viewer"
password = "viewer-password"
read_only = true              # optional, defaults to false
```

`admin_password` and each `[[users]]` entry are separate accounts.
`admin` is a reserved username (it can't also appear in `[[users]]`),
usernames must be unique, and no password may be empty. A `[[users]]`
entry defaults to full read/write access, same as `admin`; set
`read_only = true` to limit it to `GET` (see below).

### REST API

An OpenAPI spec for everything below is generated (via
[utoipa](https://github.com/juhaku/utoipa)) straight from the same
`#[utoipa::path(...)]` annotations on each handler in
`kblockdbserver/src/routes.rs` -- served as JSON at `GET /api-docs/openapi.json`
and browsable interactively at `GET /swagger-ui/`, both unauthenticated
(like `/health`, they describe the API, not any of its data). Because the
spec is generated from the same annotations the router is built from,
adding or changing a route without updating its annotation is a compile
error, not documentation that silently drifts from what the server
actually does.

Every endpoint below except `/health` requires **HTTP Basic Auth** against
one of the config file's accounts (`admin`/`admin_password`, or a
`[[users]]` entry) -- a request with no `Authorization` header, an unknown
username, or the wrong password gets `401`. A `read_only` account gets
`403` on anything but `GET` (`PUT`/`DELETE` are writes). `/health` is left
open so load balancers/orchestrators can poll liveness without
credentials; it exposes nothing more sensitive than the world's shape and
this server's clock.

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
| `GET` | `/health` | | `200` `{"status":"ok","axes":3,"world_dim":10000,"timestamp":1735689600}` |
| `GET` | `/cells/{coords}/{key}` | | `200 {"value": <value>}`, or `404` if unset |
| `PUT` | `/cells/{coords}/{key}` | `<value>` | `204` |
| `DELETE` | `/cells/{coords}/{key}` | | `204` |
| `GET` | `/regions/{origin}/{extent}/{key}` | | `200 {"values": [<value or null>, ...]}` |
| `PUT` | `/regions/{origin}/{extent}/{key}` | `{"values": [<value>, ...]}` | `204` |
| `DELETE` | `/regions/{origin}/{extent}/{key}` | | `204` |
| `GET` | `/stats` | | `200` `{"total_chunks":2,"total_bytes":8227,"total_blocks":32}` |

`/stats` walks the on-disk chunk files under `--data-dir` and reports:
`total_chunks` (chunk files currently on disk -- see `kblockdblib`'s "Layout on
disk" doc comment, a chunk with no cells set in it is never written and an
emptied one is deleted, not left behind empty), `total_bytes` (their
combined size), and `total_blocks` (their combined actual disk-block
allocation -- smaller than `total_bytes / 512` for chunks with large
never-written, and so sparse, regions; on Windows this is instead
`total_bytes` rounded up to whole 512-byte blocks, an upper bound rather
than a real sparse-file measurement). It's a live filesystem walk each
call, not a running counter, so it costs time proportional to how many
chunks currently exist. Like the cell/region endpoints (and unlike
`/health`), it requires auth, but being a `GET` it's available to
`read_only` accounts too.

Region `values` arrays are in axis-0-fastest order (matching
`kblockdblib::World::get_region`/`set_region`): index `i` is offset
`(i % extent[0], (i / extent[0]) % extent[1], ...)` from `origin`. A `PUT`
to a region must supply exactly one value per cell (`extent[0] * extent[1]
* ...`), in that order, or it fails with `400`.

Errors are `{"error": "<message>"}`, with the status code reflecting the
cause: `400` for a malformed/out-of-range coordinate, a region whose axis
count doesn't match the world's, or a `values` array of the wrong length
(all of these are `kblockdblib`'s own validation surfacing through); `401` for
missing/invalid credentials; `403` for a `read_only` account attempting a
write; `404` for a `GET` that found nothing; `500` for anything on the
server's side (disk I/O, ...).

```sh
# set a cell
curl -u admin:change-me -X PUT localhost:8080/cells/1,2,3/material \
  -H 'content-type: application/json' -d '{"type":"str","value":"stone"}'

# read it back
curl -u admin:change-me localhost:8080/cells/1,2,3/material
# {"value":{"type":"str","value":"stone"}}

# fill an 8x8x8 region with distinct per-cell values (512 of them, omitted here)
curl -u admin:change-me -X PUT localhost:8080/regions/0,0,0/8,8,8/material \
  -H 'content-type: application/json' -d '{"values":[...]}'

# read the whole region back
curl -u admin:change-me localhost:8080/regions/0,0,0/8,8,8/material

# clear a cell / a region
curl -u admin:change-me -X DELETE localhost:8080/cells/1,2,3/material
curl -u admin:change-me -X DELETE localhost:8080/regions/0,0,0/8,8,8/material

# on-disk stats
curl -u admin:change-me localhost:8080/stats
# {"total_chunks":2,"total_bytes":8227,"total_blocks":32}
```

### Layout

- `kblockdbserver/src/main.rs`       -- CLI arg parsing, loads the config file,
  opens the world, starts the server (with graceful shutdown on Ctrl+C).
- `kblockdbserver/src/config.rs`     -- `Config`, the `--config` TOML file
  (addr/data_dir/axes/world_dim/max_concurrent_disk_ops, admin_password,
  `[[users]]`) and its validation.
- `kblockdbserver/src/auth.rs`       -- the HTTP Basic Auth middleware applied
  to every route except `/health`, including the `read_only` write check.
- `kblockdbserver/src/routes.rs`     -- the router, all HTTP handlers, and each
  one's `#[utoipa::path(...)]` OpenAPI annotation.
- `kblockdbserver/src/openapi.rs`    -- `ApiDoc`, the `utoipa::OpenApi` derive
  that collects every handler's annotation (and every response type's
  `#[derive(ToSchema)]`) into the spec served at `/api-docs/openapi.json`,
  plus the `basic_auth` security scheme those annotations reference.
- `kblockdbserver/src/state.rs`      -- `AppState` (the shared, mutex-guarded
  `World`, plus the configured accounts) and `with_world`, which runs each
  `World` call on a `spawn_blocking` thread so `World`'s synchronous file
  I/O never blocks the async runtime.
- `kblockdbserver/src/value_json.rs` -- `ValueJson`, the JSON wire format for
  `kblockdblib::Value` (kept in this crate, not `kblockdblib`, since `kblockdblib` itself doesn't
  depend on `serde`), also `ToSchema` for its OpenAPI schema.
- `kblockdbserver/src/coords.rs`     -- parses the comma-separated coordinate
  path segments.
- `kblockdbserver/src/error.rs`      -- `ApiError`, the one error type every
  handler returns, and its mapping to HTTP status codes (including
  `From<std::io::Error>`, so `kblockdblib`'s own `InvalidInput`/`NotFound` errors
  become `400`/`404` automatically).
- `kblockdbserver/src/tests.rs`      -- HTTP-level integration tests (real
  requests through the real `Router` via `tower::ServiceExt::oneshot`, no
  TCP socket needed).

## `kblockdbperf`: the performance test suite

Drives a real `kblockdbserver` process (by default, one or more it spawns and
tears down itself) over real HTTP and measures it -- this is a measurement
of what a client actually experiences, not a microbenchmark of `kblockdblib`'s
internals.

### Build & run

```sh
cargo build --workspace --release
./target/release/kblockdbperf                       # spawns its own instance(s), runs every scenario
./target/release/kblockdbperf --scenario set_cell    # just one scenario
./target/release/kblockdbperf --json > results.json  # machine-readable output

# target an already-running instance instead (its REST API requires login --
# see kblockdbserver's config file above -- so --password is required here)
./target/release/kblockdbperf --url http://localhost:8080 --user admin --password change-me
```

When it spawns its own instance(s), kblockdbperf writes each one a minimal
config file itself (`admin_password` only) and authenticates as `admin`
automatically -- `--user`/`--password` only matter with `--url`, against a
server whose config you don't control. Pass `--password` to pin the
generated instances' password too (e.g. to `curl` one mid-run); otherwise
it's a random one-off.

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
  `--region-reps` times. Because `kblockdblib`'s region methods touch each chunk a
  region spans exactly once regardless of how many cells land in it (see
  `kblockdblib`'s "Concurrency" section above), these routinely report far higher
  effective cells/sec than the single-cell scenarios -- that gap *is* the
  batching win region operations exist for.
- **`concurrency_scan`** -- `set` from `--concurrency` concurrent clients,
  each on its own disjoint cells (different chunks, typically), at each
  level in the list. Shows how throughput scales with concurrency when
  requests don't contend for the same chunk.
- **`contended_cell`** -- the same concurrency sweep, but every client
  targets the *same* cell (different keys, so it's not just racing an
  identical overwrite). `kblockdblib::World::set` takes an exclusive lock on that
  cell's chunk file per call and must read-modify-write the *whole* chunk
  file every time (more expensive the more distinct keys have accumulated
  in it), so this is typically much slower than `concurrency_scan` even at
  the same concurrency level -- contrasting the two is the point.
- **`multi_instance`** -- the same disjoint-cell concurrency sweep, run
  once against one server and once against several servers (`--instances`,
  sharing one data directory, round-robin) at the same total concurrency.
  A single `kblockdbserver` process serializes every request through one
  `Mutex<World>` regardless of `kblockdblib`'s own per-chunk locking (see
  `kblockdbserver/src/state.rs`), so that lock only stops being the bottleneck
  once there's more than one *process* to spread load across -- this is
  the scenario that actually demonstrates multi-process scaling, and the
  reason `kblockdbperf` spawns multiple instances by default. Interpreting the
  result honestly: on one machine, multiple `kblockdbserver` processes also
  compete for the same CPU cores and disk, so how much (if any) improvement
  shows up depends on real available headroom -- this scenario is most
  meaningful comparing genuinely separate deployments (e.g. `--url` pointed
  at instances on different machines/containers), where that competition
  doesn't exist.

### Layout

- `kblockdbperf/src/main.rs`      -- CLI parsing and orchestration: spawn or
  connect to server(s), run the selected scenarios, print the report.
- `kblockdbperf/src/client.rs`    -- a thin async HTTP client for kblockdbserver's
  REST API (every value used is an `i64`, so payload shape stays constant
  across scenarios).
- `kblockdbperf/src/server.rs`    -- `ManagedServer`, which spawns a `kblockdbserver`
  child process and kills it on drop, and locates the `kblockdbserver` binary
  built alongside this one.
- `kblockdbperf/src/scenarios.rs` -- the scenarios themselves.
- `kblockdbperf/src/stats.rs`     -- latency percentiles and throughput,
  computed from a plain sorted `Vec<Duration>` (sample counts here are
  thousands, not millions -- a histogram crate would be solving a problem
  this doesn't have).
- `kblockdbperf/src/report.rs`    -- table/JSON output.
- `kblockdbperf/src/tests.rs`     -- integration tests that spawn a real
  `kblockdbserver` (or two, sharing one data dir) and run a real scenario
  against it.

## `kblockdbcli`: the command-line client

A thin wrapper over kblockdbserver's `/cells/{coords}/{key}` endpoint -- `get`,
`set`, and `remove` one cell's value from a shell, with the same HTTP
Basic Auth every other client of kblockdbserver's REST API needs.

### Build & run

```sh
cargo build --release
./target/release/kblockdbcli --password change-me set 1,2,3 material str stone
./target/release/kblockdbcli --password change-me get 1,2,3 material
# str stone
./target/release/kblockdbcli --password change-me remove 1,2,3 material
```

```
USAGE:
    kblockdbcli [OPTIONS] <COMMAND> [ARGS]

COMMANDS:
    get <coords> <key>                  Print a cell's value, as `<type> <value>`
    set <coords> <key> <type> <value>   Set a cell's value (type: str, f64, or i64)
    remove <coords> <key>               Clear a cell's value

OPTIONS:
    --url <url>        kblockdbserver base URL (default: http://127.0.0.1:8080)
    --user <name>      Username (default: admin)
    --password <pw>    Password (or set the KBLOCKDBCLI_PASSWORD env var, so it
                        doesn't end up in shell history)
    -h, --help         Print this help
```

`<coords>` is a comma-separated coordinate, one `u32` per axis (`1,2,3` for
a 3-axis world), matching however many axes the target world was created
with -- same convention as the REST API itself.

`get`'s output and `set`'s trailing two arguments share one format
(`<type> <value>`, e.g. `str stone` or `i64 42`) on purpose, so the two
compose directly: `kblockdbcli ... set 4,5,6 backup $(kblockdbcli ... get 1,2,3
material)` copies one cell's value to another. Errors (a malformed
coordinate, wrong credentials, no value set, ...) print kblockdbserver's own
error message to stderr and exit non-zero -- nothing is swallowed or
retried silently.

### Layout

- `kblockdbcli/src/main.rs`  -- CLI parsing, the HTTP calls (via
  `reqwest::blocking`, so a one-shot command doesn't need an async
  runtime), and the `<type> <value>` <-> kblockdbserver's tagged-JSON
  conversion (`build_value_json`/`describe_value`).
- `kblockdbcli/src/tests.rs` -- integration tests that spawn a real `kblockdbserver`
  and run the actual compiled `kblockdbcli` binary against it via
  `std::process::Command`, checking real stdout and exit codes.

## Build & test everything

```sh
cargo build --workspace --release
cargo test --workspace
```
