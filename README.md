# kBlockDB

A Cargo workspace with four crates:

- **`kblockdblib`** -- a prototype storage engine for a huge simulation grid where
  every cell is its own key/value store (string keys; string, f64, i64, or
  bool values), sized for something like 10,000 x 10,000 x 10,000 cells (1
  trillion cells) -- too big for one-file-per-cell or an RDBMS row-per-cell.
  Exactly one external dependency, `zstd`, used only by the optional
  `compression` flag; everything else is pure `std`. A library first,
  with a small demo/benchmark binary (`kblockdblib`) built on top of it.
- **`kblockdbserver`** -- a server that embeds `kblockdblib` as a library and
  exposes `get`/`set`/`remove` for individual cells and for axis-aligned
  regions of cells, over a RESTful HTTP API and (optionally, as a peer to
  it, not a replacement) a minimal binary protocol with less per-call
  overhead. Also a small SQL-like query language (`SELECT`/`SET`/`UPDATE`/
  `DELETE` over `POST /rest/query`) and a read-only web data browser at `/`. Unlike
  `kblockdblib`, it takes on the standard modern Rust web stack (axum +
  tokio + serde + pest) -- `kblockdblib`'s near-dependency-free constraint
  was specific to its storage format, not to everything built on top of it.
- **`kblockdbperf`** -- a performance test suite that drives a real `kblockdbserver`
  over real HTTP and measures it: single-cell and region throughput/latency,
  concurrency scaling, and lock contention.
- **`kblockdbcli`** -- a small command-line client for kblockdbserver's REST API:
  `get`/`set`/`remove` a single cell's value, or run a
  `SELECT`/`SET`/`UPDATE`/`DELETE` query, from a shell, authenticating like
  `curl -u` would. A pure HTTP client, same as `kblockdbperf` -- it treats
  kblockdbserver as a black box over its REST API, not `kblockdblib` directly.

Plus standalone, dependency-free Java and Python clients under `client/`.
Both expose the complete binary API.

```
kblockdblib/         the storage engine (library `kblockdblib` + demo binary `kblockdblib`)
kblockdbserver/   the REST server (binary `kblockdbserver`, depends on kblockdblib)
kblockdbperf/     the performance test suite (binary `kblockdbperf`, drives kblockdbserver over HTTP)
kblockdbcli/      the command-line client (binary `kblockdbcli`, drives kblockdbserver over HTTP)
client/java/      the Java client (KBlockDBClient, speaks the binary protocol; Maven, not Cargo)
client/python/    the Python 3 client (KBlockDBClient, standard library only)
```

## Documentation

- [`kblockdblib`](docs/kblockdblib.md) -- storage engine design, concurrency, and layout.
- [`kblockdbserver`](docs/kblockdbserver.md) -- server configuration, REST API, query language, browser, and binary protocol.
- [`kblockdbperf`](docs/kblockdbperf.md) -- performance suite usage and scenarios.
- [`kblockdbcli`](docs/kblockdbcli.md) -- command-line client usage.
- [Java client](docs/java-client.md) -- dependency-free Java binary-protocol client.
- [Python client](docs/python-client.md) -- dependency-free Python binary-protocol client.

## Build & test everything

```sh
cargo build --workspace --release
cargo test --workspace
```
