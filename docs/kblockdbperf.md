# `kblockdbperf`: the performance test suite

Drives a real `kblockdbserver` process (by default, one it spawns and tears
down itself) over real HTTP -- and, for the `binary_*` scenarios, its
binary protocol too -- and measures it: this is a measurement of what a
client actually experiences, not a microbenchmark of `kblockdblib`'s
internals.

## Build & run

```sh
cargo build --workspace --release
./target/release/kblockdbperf --db perf                       # spawns its own instance, runs every scenario
./target/release/kblockdbperf --db perf --scenario set_cell    # just one scenario
./target/release/kblockdbperf --db perf --json > results.json  # machine-readable output

# target an already-running instance instead (its REST API requires login,
# so --password is required here)
./target/release/kblockdbperf --db perf --url http://localhost:8080 --user admin --password change-me
```

`--db <name>` is required on every run: kblockdbperf creates that database
(shaped axes=3, world_dim=10000, chunk_size=32) up front via
`PUT /rest/databases/{name}` if it doesn't already exist -- tolerating
`409 Conflict` so a repeat run against the same name reuses whatever data
it left behind -- then runs every scenario against it, over both REST and
(for the `binary_*` scenarios) the binary protocol.

When it spawns its own instance, kblockdbperf writes it a minimal config
file itself (`admin_password` only) and authenticates as `admin`
automatically -- `--user`/`--password` only matter with `--url`, against a
server whose config you don't control. Pass `--password` to pin the
generated instance's password too (e.g. to `curl` it mid-run); otherwise
it's a random one-off.

A spawned instance always has its binary protocol enabled too (default
port `<port + 1>`, overridable with `--binary-port`), so the
`binary_*` scenarios always have something to run against. Against an
existing instance (`--url`), pass `--binary-addr <host:port>` to point at
its binary listener as well -- a connect address, which is why it's
separate from `--binary-port`'s bind port -- without it, `binary_*` scenarios are
skipped (or, if explicitly requested with `--scenario`, kblockdbperf exits
with an error rather than silently produce an incomplete report).

Run `--help` for the full flag list (concurrency levels, op counts, region
sizes, etc.) -- everything has a default chosen to finish a full run in
well under a minute, and every default is overridable.

## Scenarios

- **`set_cell` / `get_cell` / `remove_cell`** -- sequential (one client, no
  concurrency) single-cell operations on `--cells` distinct cells.
  `get_cell`/`remove_cell` populate their own data first (untimed), so
  they measure just the operation named, and are runnable on their own.
- **`region`** -- `set_region`/`get_region` at each edge length in
  `--region-edges` (a cube of that edge on every axis), repeated
  `--region-reps` times. Because `kblockdblib`'s region methods touch each chunk a
  region spans exactly once regardless of how many cells land in it (see
  [`kblockdblib` concurrency](kblockdblib.md#concurrency)), these routinely report far higher
  effective cells/sec than the single-cell scenarios -- that gap *is* the
  batching win region operations exist for.
- **`concurrency_scan`** -- `set` from `--concurrency` concurrent clients,
  each on its own disjoint cells (different chunks, typically), at each
  level in the list. Shows how throughput scales with concurrency when
  requests don't contend for the same chunk.
- **`contended_cell`** -- the same concurrency sweep, but every client
  targets the *same* cell (different keys, so it's not just racing an
  identical overwrite). `kblockdblib::World::set` takes an exclusive lock on
  that cell's chunk per call and must write back the *whole* chunk file
  every time regardless of caching (writes are write-through, not
  write-behind -- see [`kblockdblib` concurrency](kblockdblib.md#concurrency); more
  expensive the more distinct keys have accumulated in the chunk), so this
  is typically much slower than `concurrency_scan` even at the same
  concurrency level -- contrasting the two is the point.
- **`binary_set_cell` / `binary_get_cell` / `binary_remove_cell`** -- the
  same sequential single-cell workload as `set_cell`/`get_cell`/
  `remove_cell`, but over the
  [binary protocol](binary-protocol.md) instead of REST,
  via `binary_client.rs`. Comparing these against
  their REST counterparts is the point -- same database, same semantics,
  just without HTTP/JSON's per-call overhead.

## Layout

- `kblockdbperf/src/main.rs`      -- CLI parsing and orchestration: spawn or
  connect to a server, run the selected scenarios, print the report.
- `kblockdbperf/src/client.rs`    -- a thin async HTTP client for kblockdbserver's
  REST API (every value used is an `i64`, so payload shape stays constant
  across scenarios).
- `kblockdbperf/src/binary_client.rs` -- the binary-protocol counterpart to
  `client.rs`, built on `kblockdbserver::wire` so it never reimplements the
  wire format itself.
- `kblockdbperf/src/server.rs`    -- `ManagedServer`, which spawns a `kblockdbserver`
  child process (REST API and, always, its binary protocol) and kills it on
  drop, and locates the `kblockdbserver` binary built alongside this one.
- `kblockdbperf/src/scenarios.rs` -- the scenarios themselves.
- `kblockdbperf/src/stats.rs`     -- latency percentiles and throughput,
  computed from a plain sorted `Vec<Duration>` (sample counts here are
  thousands, not millions -- a histogram crate would be solving a problem
  this doesn't have).
- `kblockdbperf/src/report.rs`    -- table/JSON output.
- `kblockdbperf/src/tests.rs`     -- integration tests that spawn a real
  `kblockdbserver` and run a real scenario against it.
