
[![CI](https://github.com/teverett/kBlockDB/actions/workflows/ci.yml/badge.svg)](https://github.com/teverett/kBlockDB/actions/workflows/ci.yml)
[![Java client](https://github.com/teverett/kBlockDB/actions/workflows/java-client.yml/badge.svg)](https://github.com/teverett/kBlockDB/actions/workflows/java-client.yml)
[![Python client](https://github.com/teverett/kBlockDB/actions/workflows/python-client.yml/badge.svg)](https://github.com/teverett/kBlockDB/actions/workflows/python-client.yml)

# kBlockDB

A database for a huge simulation grid, where every cell of the grid is its
own key/value store -- string keys, with string, f64, i64, or bool values.
It's sized for worlds like 10,000 x 10,000 x 10,000 cells (a trillion
cells), which is far too many for one file per cell or one RDBMS row per
cell. One server manages any number of independent **databases**, each its
own directory under `--data-dir`; the axis count, world size, and chunk
size are all per-database settings fixed when that database is created, so
one database can be 2-dimensional, another 4-dimensional, or larger.

Storage is chunked: each database is divided into fixed-size blocks of
cells, each at most one file, and chunks that hold no data are never
written at all -- so a mostly-empty database costs disk proportional to
what's actually in it. Every value carries metadata (creation time,
modification time, and a version counter), which queries can filter on.

Servers can be clustered: each replicates its writes to its peers, peers
find each other by gossip, and a server that's new or was down catches up
on exactly what it missed -- tracked with per-server sequence numbers, so
two servers can tell at a glance whether they're in sync.

## Quick start

```sh
cargo build --release
cargo run -p kblockdbserver          # reads ./kblockdbserver.toml
```

The checked-in `kblockdbserver.toml` is a local-development placeholder
(admin / `changeme`) so this runs out of the box. **Change
`admin_password` before exposing the server to anyone you don't trust.**

No database exists until you create one -- there's no implicit default:

```sh
# create a database named "demo" (4 axes, using the server's configured
# default shape -- see kblockdbserver.toml's [worldparameters])
curl -u admin:changeme -X PUT -H 'Content-Type: application/json' \
    -d '{}' 'http://127.0.0.1:8080/rest/databases/demo'

# a single cell in it, over REST
curl -u admin:changeme -X PUT -H 'Content-Type: application/json' \
    -d '{"type":"str","value":"stone"}' \
    'http://127.0.0.1:8080/rest/db/demo/cells/1,2,3,0/material'
curl -u admin:changeme 'http://127.0.0.1:8080/rest/db/demo/cells/1,2,3,0/material'

# or a query, from the command line
./target/release/kblockdbcli --password changeme create-database demo
./target/release/kblockdbcli --password changeme --db demo query \
    "SELECT * FROM (0,0,0,0) TO (9,9,9,1) WHERE material = 'stone'"

# queries can filter on metadata, compare against now(), and aggregate
./target/release/kblockdbcli --password changeme --db demo query \
    "SELECT count(*), max(updated) FROM (0,0,0,0) TO (9,9,9,1) WHERE updated < now()"
```

A web data browser is served at <http://127.0.0.1:8080/>; it prompts for
the same credentials. Its **Browser** tab runs read-only queries against
any database and pages through the matching cells -- or, for an aggregate
query like the one above, shows a table of the results, with timestamp
aggregates such as `max(updated)` as readable dates. Its **Cluster** tab
lists every known peer with its connection status, sequence number, sync
state, and when it last synced, refreshing every few seconds; a browser
refresh keeps you on whichever tab you were on.

## Clustering

Clustering is off unless the config's `[cluster]` table sets a
`cluster_secret`. A minimal setup -- the same secret on every server, and
each server pointed at one other:

```toml
[cluster]
cluster_secret = "a-shared-secret"
peer_port = 8082

[[peers]]
address = "10.0.0.2:8082"
```

- **Replication.** Every local write -- REST, binary protocol, or query
  -- is shipped to every peer. Peers are symmetric (if A lists B, B
  replicates to A too) and share their peer lists by gossip, so the
  cluster forms a full mesh from a single seed each.
- **Catch-up.** Every server numbers its own writes, and keeps a *version
  vector*: per origin server, the highest of its writes it's guaranteed
  to have. When a link comes up, the receiving server sends its vector and
  gets back exactly the writes -- and deletes -- it's missing. A brand-new
  server fills itself this way; one that restarts catches up within
  moments. **Two servers with equal vectors have seen exactly the same
  writes.**
- **Conflicts** are last-write-wins by modification time, with ties broken
  the same way on every server.
- **Deletes** leave a tombstone (kept a week by default) so they replicate
  too, including to a server that was offline.
- **Monitoring.** `GET /rest/health` reports this server's node id and
  vector; `GET /rest/cluster` lists every peer with its vector, sequence
  number, sync state (`in_sync`, `behind`, `ahead`, `diverged`), and last
  sync time -- what the data browser's Cluster tab shows.
- Dead learned peers are dropped after a timeout, and TCP keepalive
  notices peers that vanish without closing their connection.

All nodes must run the same version. See [clustering.md](docs/clustering.md)
for the details, every setting, and the limitations.

## What's here

A Cargo workspace of six Rust crates, plus two standalone clients:

- **`kblockdblib`** -- the storage engine, as a library plus a small
  demo/benchmark binary. Chunked on-disk format, a write-through LRU chunk
  cache, a cap on concurrent disk operations, and optional zstd
  compression of chunk files. For replication it also records each write's
  origin and sequence number, keeps tombstones for deletes, and can list
  every change a version vector lacks without decoding chunks that have
  none. `zstd` is its only external dependency, and only the compression
  option uses it; everything else is `std`.
- **`kblockdbquery`** -- the SQL-like query language
  (`SELECT`/`SET`/`UPDATE`/`DELETE`, with `created`/`updated`/`version`
  metadata, `now()`, and `count`/`sum`/`mean`/`min`/`max` aggregates over
  values or metadata) as
  a standalone crate: grammar
  (parsed with [pest](https://pest.rs)), AST, parsing, and in-memory
  evaluation against a `kblockdblib::CellEntry`. No I/O, no REST/binary
  transport concerns -- `kblockdbserver` embeds it and owns every actual
  `World` call a parsed statement implies.
- **`kblockdbserver`** -- embeds `kblockdblib`, `kblockdbquery`, and
  `kblockdbcluster`, and serves them over a REST HTTP API, a web data
  browser (read-only queries and aggregates, plus a live cluster view),
  and (optionally, as a peer to REST rather than a replacement) a compact
  binary protocol with less per-call overhead.
  Manages any number of independent databases under one data directory
  (`/rest/databases` to list/create/delete); every per-database route is
  scoped under `/rest/db/{name}/...` and supports single cells and
  axis-aligned regions, the query language, and schema column management,
  all behind HTTP Basic Auth with per-account read-only access. Unlike
  `kblockdblib` it uses the usual modern Rust web stack (axum, tokio,
  serde) -- the near-dependency-free constraint applies to the storage
  format, not to everything built on top of it.
- **`kblockdbcluster`** -- server-to-server replication as a standalone
  crate: the peer wire protocol, the in-process publish hub every local
  write feeds, both sides of a peer link, gossip and dead-peer pruning,
  and catch-up -- this server's persistent node id and write counter, and
  the version vector of what it has. It's generic over how an embedder
  applies and lists changes (`ReplicationSink` and `ChangeSource` traits),
  so it knows nothing about `kblockdbserver`'s `AppState`/`World`/
  accounts. See [clustering.md](docs/clustering.md).
- **`kblockdbperf`** -- drives a real server over real HTTP and measures
  it: single-cell and region throughput and latency, concurrency scaling,
  and lock contention.
- **`kblockdbcli`** -- a command-line client for the REST API: get, set,
  or remove one cell, run a query, list/add/drop schema columns, or
  list/create/delete databases, authenticating the way `curl -u` would.
- **Java and Python clients** (`client/java`, `client/python`) -- each
  dependency-free, each covering the complete binary API: database
  management, health, stats, single cells, regions, schema columns, and
  queries. Connecting selects both an account and a database for the
  connection's life, same as the binary protocol itself.

```
kblockdblib/      the storage engine (library + demo binary `kblockdblib`)
kblockdbquery/    the query language (library only, embedded by kblockdbserver)
kblockdbcluster/  server-to-server replication (library only, embedded by kblockdbserver)
kblockdbserver/   the server (binary `kblockdbserver`, embeds kblockdblib/kblockdbquery/kblockdbcluster)
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
  and data browser.
- [Query language](docs/query-language.md) -- the SQL-like
  `SELECT`/`SET`/`UPDATE`/`DELETE` grammar every transport shares.
- [Binary protocol](docs/binary-protocol.md) -- the compact TCP protocol
  that peers with the REST API.
- [Clustering](docs/clustering.md) -- replication, gossip, catch-up and
  version vectors, deletes and tombstones, and monitoring a cluster.
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
