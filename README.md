
[![CI](https://github.com/teverett/kBlockDB/actions/workflows/ci.yml/badge.svg)](https://github.com/teverett/kBlockDB/actions/workflows/ci.yml)
[![Java client](https://github.com/teverett/kBlockDB/actions/workflows/java-client.yml/badge.svg)](https://github.com/teverett/kBlockDB/actions/workflows/java-client.yml)
[![Python client](https://github.com/teverett/kBlockDB/actions/workflows/python-client.yml/badge.svg)](https://github.com/teverett/kBlockDB/actions/workflows/python-client.yml)

# kBlockDB

A database for a huge simulation grid, where every cell of the grid is its
own key/value store -- string keys, with string, f64, i64, or bool values.
It's sized for worlds like 10,000 x 10,000 x 10,000 cells (a trillion
cells), which is far too many for one file per cell or one RDBMS row per
cell. The axis count, world size, and chunk size are all per-world
settings, so a world can be 2-dimensional, 4-dimensional, or larger.

Storage is chunked: the world is divided into fixed-size blocks of cells,
each at most one file, and chunks that hold no data are never written at
all -- so a mostly-empty world costs disk proportional to what's actually
in it. Every value carries metadata (creation time, modification time,
and a version counter).

## Quick start

```sh
cargo build --release
cargo run -p kblockdbserver          # re ads ./kblockdbserver.toml
```

The checked-in `kblockdbserver.toml` is a local-development placeholder
(admin / `changeme`) so this runs out of the box. **Change
`admin_password` before exposing the server to anyone you don't trust.**

```sh
# a single cell, over REST
curl -u admin:changeme -X PUT -H 'Content-Type: application/json' \
    -d '{"type":"str","value":"stone"}' \
    'http://127.0.0.1:8080/rest/cells/1,2,3,0/material'
curl -u admin:changeme 'http://127.0.0.1:8080/rest/cells/1,2,3,0/material'

# or a query, from the command line
./target/release/kblockdbcli --password changeme query \
    "SELECT * FROM (0,0,0,0) TO (9,9,9,1) WHERE material = 'stone'"
```

A read-only web browser for the data is served at
<http://127.0.0.1:8080/>; it prompts for the same credentials.

## What's here

A Cargo workspace of four Rust crates, plus two standalone clients:

- **`kblockdblib`** -- the storage engine, as a library plus a small
  demo/benchmark binary. Chunked on-disk format, a write-through LRU chunk
  cache, a cap on concurrent disk operations, and optional zstd
  compression of chunk files. `zstd` is its only external dependency, and
  only the compression option uses it; everything else is `std`.
- **`kblockdbserver`** -- embeds `kblockdblib` and serves it over a REST
  HTTP API, a read-only web data browser, and (optionally, as a peer to
  REST rather than a replacement) a compact binary protocol with less
  per-call overhead. Supports single cells and axis-aligned regions, a
  SQL-like query language (`SELECT`/`SET`/`UPDATE`/`DELETE`), schema
  column management, and HTTP Basic Auth with per-account read-only
  access. Unlike `kblockdblib` it
  uses the usual modern Rust web stack (axum, tokio, serde, pest) --
  the near-dependency-free constraint applies to the storage format, not
  to everything built on top of it.
- **`kblockdbperf`** -- drives a real server over real HTTP and measures
  it: single-cell and region throughput and latency, concurrency scaling,
  and lock contention.
- **`kblockdbcli`** -- a command-line client for the REST API: get, set,
  or remove one cell, run a query, or list/add/drop schema columns,
  authenticating the way `curl -u` would.
- **Java and Python clients** (`client/java`, `client/python`) -- each
  dependency-free, each covering the complete binary API: health, stats,
  single cells, regions, schema columns, and queries.

```
kblockdblib/      the storage engine (library + demo binary `kblockdblib`)
kblockdbserver/   the server (binary `kblockdbserver`, embeds kblockdblib)
kblockdbperf/     the performance suite (binary `kblockdbperf`, over HTTP)
kblockdbcli/      the command-line client (binary `kblockdbcli`, over HTTP)
client/java/      the Java client (binary protocol; Maven, not Cargo)
client/python/    the Python 3 client (binary protocol; standard library only)
docs/             per-component documentation
```

## Documentation

- [`kblockdblib`](docs/kblockdblib.md) -- storage engine design, chunk
  format, caching, concurrency, and compression.
- [`kblockdbserver`](docs/kblockdbserver.md) -- configuration, REST API,
  query language, data browser, and binary protocol.
- [`kblockdbperf`](docs/kblockdbperf.md) -- performance suite usage and
  scenarios.
- [`kblockdbcli`](docs/kblockdbcli.md) -- command-line client usage.
- [Java client](docs/java-client.md) -- the Java binary-protocol client.
- [Python client](docs/python-client.md) -- the Python binary-protocol
  client.

## Build and test

The Rust workspace:

```sh
cargo build --workspace --release
cargo test --workspace
```

The clients build separately, and their tests run against a real server
binary:

```sh
cargo build -p kblockdbserver

mvn -f client/java/pom.xml package

PYTHONPATH=client/python/src \
    python3 -m unittest discover -s client/python/tests
```

`build.sh`, `run.sh`, `perf.sh`, and `load.sh` at the repository root are
one-line shortcuts for building, running the server, running the
performance suite, and loading some sample data.

## License

BSD 3-Clause. See [LICENSE](LICENSE).
