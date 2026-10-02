# `kblockdbserver`: the REST server

## Build & run

```sh
cargo build --release
cargo run -p kblockdbserver -- --data-dir ./data --http-port 8080
```

A default `kblockdbserver.toml` (admin/`changeme`, see below) is checked in at
the repo root so this works out of the box -- **change `admin_password`
before running this anywhere reachable by anyone you don't trust.**

On boot it prints where to find its three entry points, so you don't have
to go looking up paths and port numbers:

```
kblockdbserver listening on http://10.0.0.7:8080
  data browser    http://10.0.0.7:8080/
  health API      http://10.0.0.7:8080/rest/health
  databases API   http://10.0.0.7:8080/rest/databases
```

Only a **port** is configurable, never a bind host: the server always
listens on `0.0.0.0`, every interface, so it's reachable at whatever
address this host happens to have without anyone having to name that
address up front -- which often isn't knowable in advance anyway (DHCP,
containers, moving between networks). **This means the server is exposed
to the network by default, so set a real `admin_password` before running
it anywhere untrusted.**

IPv4's wildcard rather than IPv6's `[::]`, even though the latter is
dual-stack on Linux and macOS: FreeBSD defaults `net.inet6.ip6.v6only=1`,
where an IPv6 wildcard listener accepts no IPv4 connections at all.
Binding IPv4 behaves identically everywhere.

The banner prints this host's primary IP rather than `0.0.0.0`, which
isn't itself a connectable address -- so the URLs are ones another
machine can use, which is the point of binding every interface. That IP
comes from the routing table (a connected UDP socket, which sends no
packets, read back for its local address) rather than from resolving the
hostname, since hostname lookups usually answer `127.0.0.1` and can
block on DNS. On a host with no route out at all it falls back to
loopback. The port comes from what the listener *actually* bound, so
`--http-port 0` -- which asks the OS to pick a free one -- reports the
real port rather than a useless `:0`.

```
USAGE:
    kblockdbserver [OPTIONS]

OPTIONS:
    --config <path>                Config file (default: ./kblockdbserver.toml). Required --
                                    holds admin_password and, optionally, [[users]], plus
                                    optional http_port/binary_port/data_dir/
                                    max_concurrent_disk_ops/max_cached_chunks/compression
                                    and a [worldparameters] table (each overridden by the
                                    matching CLI flag below, if given; compression is
                                    config-file-only)
    --data-dir <path>              Data directory; each database is its own subdirectory
                                    of this one (default: ./data). No database exists
                                    until you create one -- see "Databases" below
    --axes <n>                     Axis count for a database created without an explicit
                                    shape of its own (default: 3)
    --world-dim <n>                Cells per axis for a database created without an
                                    explicit shape (default: 10000)
    --chunk-size <n>               Cells per axis within a chunk, same default-only rule
                                    (default: 32 -- see kblockdblib's chunking design;
                                    bigger means fewer/larger chunk files, smaller
                                    means the opposite trade)
    --http-port <port>             Port to listen on for the REST API (default: 8080).
                                    Binds every interface; 0 asks the OS for a free port
    --binary-port <port>           Also listen on this port for the binary protocol
                                    (see docs/binary-protocol.md); disabled unless given
    --peer-port <port>             Port the peer-replication protocol listens on, when
                                    clustering is enabled (config file's cluster_secret --
                                    see docs/clustering.md); default 8082 if
                                    cluster_secret is set but this isn't
    --max-concurrent-disk-ops <n>  Cap on concurrent filesystem operations
                                    (default: 32 -- see kblockdblib's "Concurrency"
                                    section; measure the right value for your
                                    filesystem with kblockdbperf's concurrency_scan)
    --max-cached-chunks <n>        Cap on distinct chunks kept in the write-through
                                    cache at once, LRU-evicted beyond that (default:
                                    100000 -- see kblockdblib's "Concurrency" section;
                                    0 disables the cache's benefit without disabling
                                    the server)
    -h, --help                     Print help
```

`--axes`/`--world-dim`/`--chunk-size` only matter for a database created
*without* an explicit shape of its own (an empty `PUT /rest/databases/{name}`
body, or the binary protocol's `CreateDatabase`, which has no way to
override them at all); reopening an existing database reads its real shape
back from its own `world.txt` and ignores these flags, and creating one
with an explicit shape (`PUT /rest/databases/{name}` with a body) ignores
them too.

**Run exactly one `kblockdbserver` process per `--data-dir`.** `kblockdblib`'s
locking is in-process only now (see its
[concurrency documentation](kblockdblib.md#concurrency)), so a
second `kblockdbserver` -- or anything else -- pointed at the same
directory at the same time has no way to coordinate with the first and
can corrupt data. Scale by giving this one process more concurrent
requests (it already handles them on as many threads as the async runtime
has), not by running more of it.

## Config file

`--config` (default `./kblockdbserver.toml`) is TOML and is required to start
the server -- it's the only place credentials can come from (never a CLI
flag, so they don't end up in shell history or `ps` output):

```toml
http_port = 8080             # optional; same defaults/precedence as the CLI flags
binary_port = 8081           # optional; disabled unless given (see docs/binary-protocol.md)
data_dir = "./data"          # optional
max_concurrent_disk_ops = 32 # optional
max_cached_chunks = 100000   # optional
compression = false          # optional; zstd-compress every chunk file written
hostname = "db-1.example.com" # optional; what /rest/health reports as this
                             # instance's name (defaults to the OS hostname)
admin_password = "change-me" # required

# Only matters for a database created without an explicit shape of its own
# (see "Databases" below) -- reopening an existing database, or creating one
# with an explicit shape, ignores these. Each field, and the whole table,
# is optional; any missing field falls back to the matching CLI flag, then
# to kblockdblib's own default.
[worldparameters]
axes = 3
world_dim = 10000
chunk_size = 32

[[users]]
username = "alice"
password = "alice-password"

[[users]]
username = "viewer"
password = "viewer-password"
read_only = true              # optional, defaults to false

# Clustering (see docs/clustering.md) -- optional; off entirely unless
# cluster_secret is set.
[cluster]
peer_port = 8082
cluster_secret = "a-shared-secret-only-this-clusters-nodes-know"

[[peers]]
address = "10.0.0.2:8082"
```

`admin_password` and each `[[users]]` entry are separate accounts.
`admin` is a reserved username (it can't also appear in `[[users]]`),
usernames must be unique, and no password may be empty. A `[[users]]`
entry defaults to full read/write access, same as `admin`; set
`read_only = true` to limit it to `GET` (see below).

## Compression

`compression` (config file only, default `false`) zstd-compresses every
chunk file the server writes, for every database. It trades CPU on each
write and each cache-missing read for a smaller database on disk; how much
smaller depends entirely on the data, since the chunk format is already
compact and sparse (see [kblockdblib](kblockdblib.md)).

It is safe to turn on or off at any time, on an existing database as well
as a new one:

- The setting governs *writes* only. Reads detect each file's encoding
  from its own leading bytes, so a database may hold a mix of compressed
  and uncompressed chunks and stays fully readable either way.
- Flipping it rewrites nothing by itself. An existing chunk file is
  re-encoded the next time something writes to that chunk.
- It is not recorded in `world.txt`: unlike `axes`/`world_dim`/
  `chunk_dim`, it isn't part of a database's fixed shape.

The library exposes the same switch as
`kblockdblib::World::with_compression(bool)`.

## Databases

One `kblockdbserver` process manages any number of independent databases
under one `--data-dir`, each one its own directory (`<data-dir>/<name>/`,
with its own `world.txt`/`schema.txt`/chunk tree -- exactly what used to be
the whole `--data-dir` before multi-database support existed). Nothing is
created automatically -- there's no implicit default -- so every database
must be created explicitly before anything can be read or written in it.

| Method | Path | Body | Response |
|---|---|---|---|
| `GET` | `/rest/databases` | | `200 {"databases": ["demo", "other"]}` |
| `PUT` | `/rest/databases/{name}` | `{"axes"?, "world_dim"?, "chunk_size"?}` | `204`, or `409` if it exists, `400` for a bad name |
| `DELETE` | `/rest/databases/{name}` | | `204`, or `404` if there's no such database |

`PUT`'s body is a JSON object whose three fields are all optional --
`{}` is valid and means "use the server's configured default shape" (see
`--axes`/`--world-dim`/`--chunk-size`/`[worldparameters]` above). Once
created, a database's shape is fixed for its lifetime, same as before.

`DELETE` removes the database's directory and **every byte of data in
it**, immediately and irreversibly -- there is no confirmation step or
trash. Both `PUT` and `DELETE` are writes, so a `read_only` account gets
`403`; `GET /rest/databases` is readable by any account.

```sh
curl -u admin:change-me -X PUT -H 'content-type: application/json' \
     -d '{}' http://127.0.0.1:8080/rest/databases/demo

curl -u admin:change-me http://127.0.0.1:8080/rest/databases
# {"databases":["demo"]}

curl -u admin:change-me -X DELETE http://127.0.0.1:8080/rest/databases/demo
```

## REST API

Every per-database route below is scoped under `/rest/db/{db}/...`, naming
which database it operates on -- `{db}` must already exist (see
"Databases" above) or the route 404s. Routes are mounted under the `/rest`
context path generally (`/rest/db/{db}/cells/...`, `/rest/databases`,
`/rest/health`, ...) -- kept separate from the
[binary protocol](binary-protocol.md)'s own listener and from whatever
else might one day share this HTTP server.

An OpenAPI spec for everything below is generated (via
[utoipa](https://github.com/juhaku/utoipa)) straight from the same
`#[utoipa::path(...)]` annotations on each handler in
`kblockdbserver/src/routes.rs` -- served as JSON at `GET /rest/api-docs/openapi.json`
and browsable interactively at `GET /rest/swagger-ui/`, both unauthenticated
(like `/rest/health`, they describe the API, not any of its data). Because
the spec is generated from the same annotations the router is built from,
adding or changing a route without updating its annotation is a compile
error, not documentation that silently drifts from what the server
actually does.

Every endpoint below except `/rest/health` requires **HTTP Basic Auth**
against one of the config file's accounts (`admin`/`admin_password`, or a
`[[users]]` entry) -- a request with no `Authorization` header, an unknown
username, or the wrong password gets `401`. A `read_only` account gets
`403` on anything but `GET` (`PUT`/`DELETE` are writes). `/rest/health` is
left open so load balancers/orchestrators can poll liveness without
credentials; it exposes nothing more sensitive than this server's clock and
how many databases it's managing.

Every coordinate, and every region origin/extent, is a comma-separated
list of `i32`s in the URL, one per axis (`1,2,3` for a 3-axis database, or
`-1,2,-3` -- a negative component needs no special URL encoding, `-` is a
plain path character) -- there's nothing 3-axis-specific about the API; it
works the same way for whatever axis count the target database was created
with. See "Coordinate space" above for a database's valid range per axis.

A cell value on the wire is a small tagged JSON object:

```json
{"type": "str", "value": "stone"}
{"type": "f64", "value": 2.6}
{"type": "i64", "value": 7}
{"type": "bool", "value": true}
```

| Method | Path | Body | Response |
|---|---|---|---|
| `GET` | `/rest/health` | | `200` `{"status":"ok","hostname":"db-1","database_count":2,"timestamp":1735689600,"peers":[]}` |
| `GET` | `/rest/db/{db}/cells/{coords}/{key}` | | `200 {"value": <value>, "created_at_ms": ..., "modified_at_ms": ..., "version": ...}`, or `404` if unset or `{db}` doesn't exist |
| `PUT` | `/rest/db/{db}/cells/{coords}/{key}` | `<value>` | `204` |
| `DELETE` | `/rest/db/{db}/cells/{coords}/{key}` | | `204` |
| `GET` | `/rest/db/{db}/regions/{origin}/{extent}/{key}` | | `200 {"values": [<value or null>, ...]}` |
| `PUT` | `/rest/db/{db}/regions/{origin}/{extent}/{key}` | `{"values": [<value>, ...]}` | `204` |
| `DELETE` | `/rest/db/{db}/regions/{origin}/{extent}/{key}` | | `204` |
| `GET` | `/rest/db/{db}/stats` | | `200` `{"total_chunks":2,"total_bytes":8227,"total_blocks":32}` |
| `GET` | `/rest/db/{db}/columns` | | `200 {"columns": [{"key": "material", "type": "str"}, ...]}` |
| `PUT` | `/rest/db/{db}/columns/{key}` | `{"type": "str"}` | `204`, or `409` if the column exists |
| `DELETE` | `/rest/db/{db}/columns/{key}` | | `204`, or `404` if there's no such column |

(See "Databases" above for `GET/PUT/DELETE /rest/databases[/{name}]`.)

`/rest/health`'s `hostname` identifies *which* instance answered, which is
what makes it useful behind a load balancer: without it, polling a pool
tells you some server is up but never which one. It's the OS hostname by
default, or the config file's `hostname` key when set (useful when the OS
name isn't the name you route by). It's never empty -- a server that
can't determine its own hostname reports `"unknown"` rather than failing
the health check, since an unnameable host is still a serving one.
`database_count` is a live count of how many databases this server
currently manages (same number `GET /rest/databases` would list) -- health
is server-wide, not scoped to any one database, so it has no per-database
shape to report the way the old single-world `/rest/health` once did; ask
`GET /rest/db/{db}/stats` or a binary-protocol `Hello` for a specific
database's shape. `peers` lists this instance's configured `[[peers]]`
addresses (see [clustering.md](clustering.md)) -- empty unless clustering
is configured, and always just the configured list, not live
connected/reconnecting status for each one.

`/rest/db/{db}/stats` walks the on-disk chunk files under that database's
directory and reports: `total_chunks` (chunk files currently on disk --
see `kblockdblib`'s "Layout on disk" doc comment, a chunk with no cells set
in it is never written and an emptied one is deleted, not left behind
empty), `total_bytes` (their combined size), and `total_blocks` (their
combined actual disk-block allocation -- smaller than `total_bytes / 512`
for chunks with large never-written, and so sparse, regions; on Windows
this is instead `total_bytes` rounded up to whole 512-byte blocks, an
upper bound rather than a real sparse-file measurement). It's a live
filesystem walk each call, not a running counter, so it costs time
proportional to how many chunks currently exist in that database. Like the
cell/region endpoints (and unlike `/rest/health`), it requires auth, but
being a `GET` it's available to `read_only` accounts too.

Region `values` arrays are in axis-0-fastest order (matching
`kblockdblib::World::get_region`/`set_region`): index `i` is offset
`(i % extent[0], (i / extent[0]) % extent[1], ...)` from `origin`. A `PUT`
to a region must supply exactly one value per cell (`extent[0] * extent[1]
* ...`), in that order, or it fails with `400`.

Errors are `{"error": "<message>"}`, with the status code reflecting the
cause: `400` for a malformed/out-of-range coordinate, a region whose axis
count doesn't match the database's, a bad database name, or a `values`
array of the wrong length (all of these are `kblockdblib`'s own validation,
or `state::validate_database_name`, surfacing through); `401` for
missing/invalid credentials; `403` for a `read_only` account attempting a
write; `404` for a `GET` that found nothing, or for `{db}` naming a
database that doesn't exist; `409` for creating a database that already
exists; `500` for anything on the server's side (disk I/O, ...).

A single-cell `GET` reports three extra fields alongside the value itself
(see `kblockdblib::CellMeta`): `created_at_ms`/`modified_at_ms`
(milliseconds since the Unix epoch) and `version`, a count of how many
times this key has been overwritten at this cell (`0` for a value that's
never been overwritten, incremented on every later `set`). Removing a
value and setting it again later starts a fresh `0`/`created_at_ms` --
it's a new history, not a continuation of the old one. Region reads don't
currently report per-cell metadata, only the single-cell endpoint does.

```sh
# set a cell in database "demo"
curl -u admin:change-me -X PUT localhost:8080/rest/db/demo/cells/1,2,3/material \
  -H 'content-type: application/json' -d '{"type":"str","value":"stone"}'

# read it back
curl -u admin:change-me localhost:8080/rest/db/demo/cells/1,2,3/material
# {"value":{"type":"str","value":"stone"},"created_at_ms":1735689600000,
#  "modified_at_ms":1735689600000,"version":0}

# fill an 8x8x8 region with distinct per-cell values (512 of them, omitted here)
curl -u admin:change-me -X PUT localhost:8080/rest/db/demo/regions/0,0,0/8,8,8/material \
  -H 'content-type: application/json' -d '{"values":[...]}'

# read the whole region back
curl -u admin:change-me localhost:8080/rest/db/demo/regions/0,0,0/8,8,8/material

# clear a cell / a region
curl -u admin:change-me -X DELETE localhost:8080/rest/db/demo/cells/1,2,3/material
curl -u admin:change-me -X DELETE localhost:8080/rest/db/demo/regions/0,0,0/8,8,8/material

# on-disk stats
curl -u admin:change-me localhost:8080/rest/db/demo/stats
# {"total_chunks":2,"total_bytes":8227,"total_blocks":32}
```

### Columns

`/rest/db/{db}/columns` is that database's schema: every key it has ever
stored, each fixed to one of the four value types. Most columns get
created implicitly -- the first `PUT` of a cell value interns its key and
pins its type -- so `GET /rest/db/{db}/columns` lists those alongside any
declared with `PUT /rest/db/{db}/columns/{key}`, sorted by key.

```sh
curl -u admin:change-me http://127.0.0.1:8080/rest/db/demo/columns
# {"columns":[{"key":"hardness","type":"f64"},{"key":"material","type":"str"}]}

curl -u admin:change-me -X PUT http://127.0.0.1:8080/rest/db/demo/columns/hardness \
     -H 'content-type: application/json' -d '{"type": "f64"}'

curl -u admin:change-me -X DELETE http://127.0.0.1:8080/rest/db/demo/columns/hardness
```

`PUT` only ever *creates*: a key that already has a column gets `409`,
never a silent type change. To change a column's type, `DELETE` it and
`PUT` it back -- the key gets a fresh id, so it's free to come back as a
different type.

`DELETE` drops the column and **every value ever written for it**, across
the whole database, and is not reversible. It's a write, so a `read_only`
account gets `403`; `GET /rest/db/{db}/columns` is readable by any
account. See the [storage engine's notes](kblockdblib.md#columns) for how
removal interacts with the append-only `schema.txt`.

### Query

`POST /rest/db/{db}/query` runs the [query language](query-language.md)
against that database -- see that page for the full grammar (statement
kinds, ranges, criteria including `EXISTS`) and semantics; this is just
the REST-specific wire shape. The request body is `{"query": "<text>"}`.
A plain `SELECT`'s response has `total_rows`/`rows` (each row's `values`
entries carrying the same `created_at_ms`/`modified_at_ms`/`version`
metadata the single-cell `GET` reports, not just the value); `SELECT
count(*)`/`sum(...)`/`mean(...)`/`max(...)`/`min(...)`'s has `aggregates`
instead -- one `{"label": ..., "value": ...}` per function, summarizing
every matching cell rather than listing them (`value` is `null` only for
`mean`/`max`/`min` when no matching cell had a numeric value for the
key); `SET`/`UPDATE`/`DELETE`'s has `affected_cells` instead of either
(the fields the statement kind doesn't produce are omitted, not null).
This is the one route in the whole REST
API where `read_only` isn't decided by HTTP method the way it is
everywhere else (see `routes.rs`'s doc comment) -- all four statement
kinds share this one `POST` endpoint, so it's decided by which kind was
actually sent, checked after parsing, before touching anything.

```sh
curl -u admin:change-me -X POST localhost:8080/rest/db/demo/query \
  -H 'content-type: application/json' \
  -d '{"query": "SELECT material, density WHERE x0 >= 10 AND x0 < 20 AND material = '"'"'stone'"'"'"}'
# {"total_rows":1,"rows":[{"coord":[15,3,7],"values":[
#   {"key":"density","value":{"type":"f64","value":2.6},
#    "created_at_ms":1735689600000,"modified_at_ms":1735689600000,"version":0},
#   {"key":"material","value":{"type":"str","value":"stone"},
#    "created_at_ms":1735689600000,"modified_at_ms":1735689650000,"version":2}]}]}

# upsert: fills every cell in the box with material='stone', creating any
# that don't already exist.
curl -u admin:change-me -X POST localhost:8080/rest/db/demo/query \
  -H 'content-type: application/json' \
  -d '{"query": "SET (material='"'"'stone'"'"') IN (0,0,0) TO (20,20,20)"}'
# {"affected_cells":8000}

# EXISTS: cells that do (or don't) have a key set at all, regardless of value
curl -u admin:change-me -X POST localhost:8080/rest/db/demo/query \
  -H 'content-type: application/json' \
  -d '{"query": "SELECT * WHERE EXISTS(density)"}'

curl -u admin:change-me -X POST localhost:8080/rest/db/demo/query \
  -H 'content-type: application/json' \
  -d '{"query": "DELETE WHERE material = '"'"'air'"'"'"}'
# {"affected_cells":1}

# aggregates: one summary result per function, not one row per cell
curl -u admin:change-me -X POST localhost:8080/rest/db/demo/query \
  -H 'content-type: application/json' \
  -d '{"query": "SELECT count(*), mean(density) WHERE material = '"'"'stone'"'"'"}'
# {"aggregates":[{"label":"count(*)","value":8000.0},{"label":"mean(density)","value":2.6}]}
```

## Data browser

`GET /` (note: *not* under `/rest`) serves one small self-contained
HTML/JS page with a database dropdown (populated from `GET
/rest/databases`, called directly by the page's own JS) and a table
listing every populated cell in whichever database is selected, one row
per cell, sorted ascending by coordinate, with a search bar and paging
controls. Switching the dropdown reloads the table against the newly
selected database -- there's no separate page per database. Clicking a
row opens a modal with that cell's full keys, values, and metadata
(`created_at_ms`/`modified_at_ms`/`version` per key -- see "REST API"
above). It's read-only (there's no way to edit anything from here) and
requires the same HTTP Basic Auth as the REST API -- a `read_only` account
can browse same as any other, since it's all `GET`. The browser's own
credential prompt (triggered by a `401`) is what a plain HTML page gets
for free from the browser itself; the page's own JS never handles a
password. If no databases exist yet, the dropdown is empty and the page
says so instead of trying to list rows for nothing.

`GET /rows?db={name}&page=&page_size=&search=` is the JSON endpoint the
page's JS calls (`db` is required -- no default, matching the rest of the
server's "every request names its database explicitly" rule, and a
request missing it gets a plain `400` from the `Query` extractor itself;
1-based `page`, default 50/max 500 `page_size`, an optional
case-insensitive `search` matched against a cell's coordinate, any key
name, or any value's rendered text) -- each row already carries its full
per-key breakdown, so opening a modal needs no second request. `404`s if
`db` names a database that doesn't exist. Built on
`kblockdblib::World::list_cells`, which -- like `/rest/db/{db}/stats`
above, but heavier, since it decodes whole chunk files rather than just
reading their sizes -- is a live, uncached filesystem walk redone on every
call. Fine for a database browsed occasionally; not meant for one with
millions of populated cells polled repeatedly.

Deliberately kept outside `/rest`: this is a convenience UI over the same
data, not part of the versioned REST API surface -- it has no OpenAPI
annotation and doesn't appear in `/rest/api-docs/openapi.json`.

## Binary protocol

A compact binary protocol covering this same REST API's full surface, as
a *peer* to it rather than a replacement -- same databases, accounts, and
semantics, just without HTTP/JSON's per-call overhead. Disabled by
default; enable it with `--binary-port <port>` (or `binary_port` in the
config file) alongside `--http-port`. See [Binary protocol](binary-protocol.md)
for the full story: connection-scoped auth and database selection, the
bootstrap flow for a not-yet-created database, protocol versioning, and
the frame/request/response byte layout.

## Layout

- `kblockdbserver/src/main.rs`       -- CLI arg parsing, loads the config file,
  builds the `Databases` manager (no database is opened yet), starts the
  server (with graceful shutdown on Ctrl+C).
- `kblockdbserver/src/config.rs`     -- `Config`, the `--config` TOML file
  (http_port/binary_port/data_dir/max_concurrent_disk_ops/max_cached_chunks/
  compression, hostname,
  admin_password, `[[users]]`, and a `[worldparameters]` table for the
  default axes/world_dim/chunk_size a new database gets when none is given
  explicitly) and its validation.
- `kblockdbserver/src/auth.rs`       -- the HTTP Basic Auth middleware applied
  to every route except `/rest/health`, including the `read_only` write
  check; also `account_from_headers`, the same check factored out for
  `/rest/db/{db}/query`'s handler, which can't use the middleware itself
  (see `routes.rs`'s doc comment).
- `kblockdbserver/src/routes.rs`     -- the router (mounted under `/rest`,
  see "Databases"/"REST API" above), all HTTP handlers, and each one's
  `#[utoipa::path(...)]` OpenAPI annotation. Its query handler (`### Query`
  above) is the only caller of the [`kblockdbquery`](query-language.md)
  crate -- grammar, AST, parsing, and in-memory evaluation all live there,
  not in this crate; `routes.rs` owns every actual `World` call a parsed
  statement implies.
- `kblockdbserver/src/openapi.rs`    -- `ApiDoc`, the `utoipa::OpenApi` derive
  that collects every handler's annotation (and every response type's
  `#[derive(ToSchema)]`) into the spec served at `/rest/api-docs/openapi.json`,
  plus the `basic_auth` security scheme those annotations reference.
- `kblockdbserver/src/browser.rs`    -- the data browser (see "Data
  browser" above): `GET /` (the one page) and `GET /rows?db={name}` (its
  JSON backend, built on `kblockdblib::World::list_cells`).
- `kblockdbserver/src/browser.html`  -- the browser page's self-contained
  HTML/CSS/JS, embedded into the binary via `include_str!` (no external
  scripts/styles, no build step) -- its database dropdown calls `GET
  /rest/databases` directly rather than this crate duplicating that
  listing logic in a second place.
- `kblockdbserver/src/state.rs`      -- `AppState` (the configured accounts)
  and `Databases` (every database this process manages, each an
  independent `kblockdblib::World` opened lazily on first touch and cached
  thereafter -- see its own doc comment for the create/get/list/remove API
  and the locking that keeps two threads from racing a database's first
  open); `with_database`, which resolves a database by name and runs a
  call against it on a `spawn_blocking` thread so `World`'s synchronous
  file I/O never blocks the async runtime.
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
- `kblockdbserver/src/wire.rs`       -- the
  [binary protocol](binary-protocol.md)'s wire format: frame I/O, and
  every request/response kind's encode/decode, plus their own round-trip
  tests.
- `kblockdbserver/src/binary_server.rs` -- the binary protocol's TCP
  listener and per-connection handler, reusing the same `AppState` the
  REST API's handlers do.
- `kblockdbserver/src/cluster.rs`    -- the only clustering-related code
  that lives in this crate: implements `kblockdbcluster::server::
  ReplicationSink` for `AppState` (applying an incoming change against
  this server's actual `World`s, auto-creating an unseen database).
  Everything else -- the peer wire format, the in-process publish hub,
  the connecting and accepting sides of a peer link -- lives in the
  separate [`kblockdbcluster`](clustering.md) crate, which this crate
  depends on but which knows nothing about `AppState`/`World`/accounts in
  return; see that crate's own doc comment for why the split is shaped
  this way. `main.rs` wires `kblockdbcluster::server::serve`/
  `kblockdbcluster::client::run` into this server's config and startup;
  every write call site in `routes.rs`/`binary_server.rs` publishes
  through `kblockdbcluster::hub::publish_set`/`publish_remove`.
