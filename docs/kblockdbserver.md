# `kblockdbserver`: the REST server

## Build & run

```sh
cargo build --release
cargo run -p kblockdbserver -- --data-dir ./data --http-addr 127.0.0.1:8080
```

A default `kblockdbserver.toml` (admin/`changeme`, see below) is checked in at
the repo root so this works out of the box -- **change `admin_password`
before running this anywhere reachable by anyone you don't trust.**

On boot it prints where to find its three entry points, so you don't have
to go looking up paths and port numbers:

```
kblockdbserver listening on http://127.0.0.1:8080
  data browser  http://127.0.0.1:8080/
  health API    http://127.0.0.1:8080/rest/health
  stats API     http://127.0.0.1:8080/rest/stats
```

These are built from the address the listener *actually* bound, not the
one requested, so `--http-addr 127.0.0.1:0` prints the real port the OS
picked rather than a useless `:0`. A wildcard bind (`0.0.0.0` or `[::]`)
prints loopback instead, since the wildcard address isn't itself
reliably connectable while loopback is always one of the interfaces it
just claimed.

```
USAGE:
    kblockdbserver [OPTIONS]

OPTIONS:
    --config <path>                Config file (default: ./kblockdbserver.toml). Required --
                                    holds admin_password and, optionally, [[users]], plus
                                    optional http_addr/binary_addr/data_dir/
                                    max_concurrent_disk_ops/max_cached_chunks/compression
                                    and a [worldparameters] table (each overridden by the
                                    matching CLI flag below, if given; compression is
                                    config-file-only)
    --data-dir <path>              World data directory (default: ./data)
    --axes <n>                     Axis count for a brand-new world (default: 3)
    --world-dim <n>                Cells per axis for a brand-new world (default: 10000)
    --chunk-size <n>               Cells per axis within a chunk, for a brand-new world
                                    (default: 32 -- see kblockdblib's chunking design;
                                    bigger means fewer/larger chunk files, smaller
                                    means the opposite trade)
    --http-addr <host:port>        Address to listen on for the REST API (default: 127.0.0.1:8080)
    --binary-addr <host:port>      Also listen on this address for the binary protocol
                                    (see "Binary protocol" below); disabled unless given
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

`--axes`/`--world-dim`/`--chunk-size` only matter the *first* time a world
is created at `--data-dir` (via `World::create`); reopening an existing one
reads its real shape back from its `world.txt` and ignores these flags.

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
http_addr = "127.0.0.1:8080" # optional; same defaults/precedence as the CLI flags
binary_addr = "127.0.0.1:8081" # optional; disabled unless given (see "Binary protocol" below)
data_dir = "./data"          # optional
max_concurrent_disk_ops = 32 # optional
max_cached_chunks = 100000   # optional
compression = false          # optional; zstd-compress every chunk file written
hostname = "db-1.example.com" # optional; what /rest/health reports as this
                             # instance's name (defaults to the OS hostname)
admin_password = "change-me" # required

# Only matters the first time a world is created at data_dir above --
# reopening an existing one reads its real shape from its world.txt and
# ignores these (World::open does, not create). Each field, and the whole
# table, is optional; any missing field falls back to the matching CLI
# flag, then to kblockdblib's own default.
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
```

`admin_password` and each `[[users]]` entry are separate accounts.
`admin` is a reserved username (it can't also appear in `[[users]]`),
usernames must be unique, and no password may be empty. A `[[users]]`
entry defaults to full read/write access, same as `admin`; set
`read_only = true` to limit it to `GET` (see below).

## Compression

`compression` (config file only, default `false`) zstd-compresses every
chunk file the server writes. It trades CPU on each write and each
cache-missing read for a smaller world on disk; how much smaller depends
entirely on the data, since the chunk format is already compact and
sparse (see [kblockdblib](kblockdblib.md)).

It is safe to turn on or off at any time, on an existing world as well
as a new one:

- The setting governs *writes* only. Reads detect each file's encoding
  from its own leading bytes, so a world may hold a mix of compressed and
  uncompressed chunks and stays fully readable either way.
- Flipping it rewrites nothing by itself. An existing chunk file is
  re-encoded the next time something writes to that chunk.
- It is not recorded in `world.txt`: unlike `axes`/`world_dim`/
  `chunk_dim`, it isn't part of a world's fixed shape.

The library exposes the same switch as
`kblockdblib::World::with_compression(bool)`.

## REST API

Every route below is mounted under the `/rest` context path (`/rest/cells/...`,
`/rest/health`, ...) -- kept separate from the binary protocol's own
listener (see "Binary protocol" below) and from whatever else might one
day share this HTTP server.

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
credentials; it exposes nothing more sensitive than the world's shape and
this server's clock.

Every coordinate, and every region origin/extent, is a comma-separated
list of `i32`s in the URL, one per axis (`1,2,3` for a 3-axis world, or
`-1,2,-3` -- a negative component needs no special URL encoding, `-` is a
plain path character) -- there's nothing 3-axis-specific about the API; it
works the same way for whatever axis count the world was created with. See
"Coordinate space" above for a world's valid range per axis.

A cell value on the wire is a small tagged JSON object:

```json
{"type": "str", "value": "stone"}
{"type": "f64", "value": 2.6}
{"type": "i64", "value": 7}
{"type": "bool", "value": true}
```

| Method | Path | Body | Response |
|---|---|---|---|
| `GET` | `/rest/health` | | `200` `{"status":"ok","hostname":"db-1","axes":3,"world_dim":10000,"chunk_dim":32,"timestamp":1735689600}` |
| `GET` | `/rest/cells/{coords}/{key}` | | `200 {"value": <value>, "created_at_ms": ..., "modified_at_ms": ..., "version": ...}`, or `404` if unset |
| `PUT` | `/rest/cells/{coords}/{key}` | `<value>` | `204` |
| `DELETE` | `/rest/cells/{coords}/{key}` | | `204` |
| `GET` | `/rest/regions/{origin}/{extent}/{key}` | | `200 {"values": [<value or null>, ...]}` |
| `PUT` | `/rest/regions/{origin}/{extent}/{key}` | `{"values": [<value>, ...]}` | `204` |
| `DELETE` | `/rest/regions/{origin}/{extent}/{key}` | | `204` |
| `GET` | `/rest/stats` | | `200` `{"total_chunks":2,"total_bytes":8227,"total_blocks":32}` |
| `GET` | `/rest/columns` | | `200 {"columns": [{"key": "material", "type": "str"}, ...]}` |
| `PUT` | `/rest/columns/{key}` | `{"type": "str"}` | `204`, or `409` if the column exists |
| `DELETE` | `/rest/columns/{key}` | | `204`, or `404` if there's no such column |

`/rest/health`'s `hostname` identifies *which* instance answered, which is
what makes it useful behind a load balancer: without it, polling a pool
tells you some server is up but never which one. It's the OS hostname by
default, or the config file's `hostname` key when set (useful when the OS
name isn't the name you route by). It's never empty -- a server that
can't determine its own hostname reports `"unknown"` rather than failing
the health check, since an unnameable host is still a serving one.
`chunk_dim` is the world's on-disk granularity, cells per axis in one
chunk file, fixed when the world was created; together with `axes` and
`world_dim` it's the whole shape, so a client can size its region reads
to chunk boundaries without a separate call.

`/rest/stats` walks the on-disk chunk files under `--data-dir` and reports:
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
`/rest/health`), it requires auth, but being a `GET` it's available to
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

A single-cell `GET` reports three extra fields alongside the value itself
(see `kblockdblib::CellMeta`): `created_at_ms`/`modified_at_ms`
(milliseconds since the Unix epoch) and `version`, a count of how many
times this key has been overwritten at this cell (`0` for a value that's
never been overwritten, incremented on every later `set`). Removing a
value and setting it again later starts a fresh `0`/`created_at_ms` --
it's a new history, not a continuation of the old one. Region reads don't
currently report per-cell metadata, only the single-cell endpoint does.

```sh
# set a cell
curl -u admin:change-me -X PUT localhost:8080/rest/cells/1,2,3/material \
  -H 'content-type: application/json' -d '{"type":"str","value":"stone"}'

# read it back
curl -u admin:change-me localhost:8080/rest/cells/1,2,3/material
# {"value":{"type":"str","value":"stone"},"created_at_ms":1735689600000,
#  "modified_at_ms":1735689600000,"version":0}

# fill an 8x8x8 region with distinct per-cell values (512 of them, omitted here)
curl -u admin:change-me -X PUT localhost:8080/rest/regions/0,0,0/8,8,8/material \
  -H 'content-type: application/json' -d '{"values":[...]}'

# read the whole region back
curl -u admin:change-me localhost:8080/rest/regions/0,0,0/8,8,8/material

# clear a cell / a region
curl -u admin:change-me -X DELETE localhost:8080/rest/cells/1,2,3/material
curl -u admin:change-me -X DELETE localhost:8080/rest/regions/0,0,0/8,8,8/material

# on-disk stats
curl -u admin:change-me localhost:8080/rest/stats
# {"total_chunks":2,"total_bytes":8227,"total_blocks":32}
```

### Columns

`/rest/columns` is the world's schema: every key it has ever stored, each
fixed to one of the four value types. Most columns get created
implicitly -- the first `PUT` of a cell value interns its key and pins its
type -- so `GET /rest/columns` lists those alongside any declared with
`PUT /rest/columns/{key}`, sorted by key.

```sh
curl -u admin:change-me http://127.0.0.1:8080/rest/columns
# {"columns":[{"key":"hardness","type":"f64"},{"key":"material","type":"str"}]}

curl -u admin:change-me -X PUT http://127.0.0.1:8080/rest/columns/hardness \
     -H 'content-type: application/json' -d '{"type": "f64"}'

curl -u admin:change-me -X DELETE http://127.0.0.1:8080/rest/columns/hardness
```

`PUT` only ever *creates*: a key that already has a column gets `409`,
never a silent type change. To change a column's type, `DELETE` it and
`PUT` it back -- the key gets a fresh id, so it's free to come back as a
different type.

`DELETE` drops the column and **every value ever written for it**, across
the whole world, and is not reversible. It's a write, so a `read_only`
account gets `403`; `GET /rest/columns` is readable by any account. See
the [storage engine's notes](kblockdblib.md#columns) for how removal
interacts with the append-only `schema.txt`.

## Query language

`POST /rest/query` runs a small SQL-like query language over the world's
cells, parsed with a [pest](https://pest.rs) grammar
(`kblockdbserver/src/query.pest`/`query.rs`). Four statement kinds:

```text
SELECT <columns> [FROM <range>] [WHERE <criteria>]
SET (<key>=<value>, ...) [WHERE <criteria>] IN <range>
UPDATE (<key>=<value>, ...) [WHERE <criteria>] [IN <range>]
DELETE [WHERE <criteria>] [IN <range>]
```

- `<columns>` is `*` or a comma-separated key list (`material, density`).
- `<range>` is `(o0,o1,...) TO (e0,e1,...)` -- an axis-aligned box, `o`
  inclusive/`e` exclusive on every axis, same convention as
  `kblockdblib::Region` (origin + extent), just written as two corners.
  Each component is a signed integer (a world's valid range is centered on
  zero -- see "Coordinate space" above), e.g. `(-10,-10,-10) TO (10,10,10)`.
  Its axis count must match the world's, or the query is rejected with
  `400` before touching any data.
- `<criteria>` is a boolean expression: comparisons (`=`, `!=`, `<`, `<=`,
  `>`, `>=`) combined with `AND`/`OR`/`NOT` and parentheses, standard
  precedence (`NOT` binds tightest, then `AND`, then `OR`). A comparison's
  left side is either `x<N>` (coordinate axis `N`, zero-indexed) or a key
  name; its right side is a string (`'stone'`), integer, float, or boolean
  (`true`/`false`, case-insensitive) literal (`x0 >= -10` works the same as
  any other comparison). Keywords are case-insensitive; key/axis names are
  not. A key literally named e.g. `x0` can't be addressed this way -- a
  known limitation of a generic axis-count grammar.
- A comparison against a key that isn't set at a given cell, or whose
  value's type doesn't match the literal's (a string literal against a
  numeric key, say), simply doesn't match that cell -- never an error, the
  same "total, not partial" philosophy `kblockdblib` itself uses.

**`SET` is an upsert; `UPDATE` is not.** `SELECT`/`UPDATE`/`DELETE` all run
on `kblockdblib::World::list_cells` -- i.e. only cells that already have at
least one key set somewhere. `UPDATE` can only ever change such a cell,
never create one, same as this whole language's original `SET` used to
work. `SET` is different: it upserts every coordinate in its `IN <range>`
that satisfies `WHERE`, creating a cell there if one doesn't already exist
-- which is exactly why `IN <range>` isn't optional for `SET` the way it is
for `UPDATE`/`DELETE`: "upsert everywhere" has no meaningful bound. Because
a `WHERE` clause comparing against a *key* can never match a cell that
doesn't exist yet (a missing key is always "doesn't match", per the bullet
above), a key-based `WHERE` makes `SET` behave exactly like `UPDATE` in
practice -- the two only diverge with no `WHERE` at all, or one that only
compares axis coordinates (`x<N>`), where `SET` can genuinely bring new
cells into existence.

`SELECT` is a read; `SET`/`UPDATE`/`DELETE` are writes. All four are behind
the same `POST /rest/query`, so this is the one route in the whole REST API
where `read_only` isn't decided by HTTP method the way it is everywhere
else (see `routes.rs`'s doc comment) -- it's decided by which kind of
statement was actually sent, checked after parsing, before touching
anything.

```sh
curl -u admin:change-me -X POST localhost:8080/rest/query \
  -H 'content-type: application/json' \
  -d '{"query": "SELECT material, density WHERE x0 >= 10 AND x0 < 20 AND material = '"'"'stone'"'"'"}'
# {"total_rows":1,"rows":[{"coord":[15,3,7],"values":[
#   {"key":"density","value":{"type":"f64","value":2.6},
#    "created_at_ms":1735689600000,"modified_at_ms":1735689600000,"version":0},
#   {"key":"material","value":{"type":"str","value":"stone"},
#    "created_at_ms":1735689600000,"modified_at_ms":1735689650000,"version":2}]}]}

# upsert: fills every cell in the box with material='stone', creating any
# that don't already exist.
curl -u admin:change-me -X POST localhost:8080/rest/query \
  -H 'content-type: application/json' \
  -d '{"query": "SET (material='"'"'stone'"'"') IN (0,0,0) TO (20,20,20)"}'
# {"affected_cells":8000}

# update: only changes cells that already have material='stone' -- never
# creates one, whether or not IN is given.
curl -u admin:change-me -X POST localhost:8080/rest/query \
  -H 'content-type: application/json' \
  -d '{"query": "UPDATE (material='"'"'basalt'"'"', hardness=9) WHERE material = '"'"'stone'"'"' IN (0,0,0) TO (20,20,20)"}'
# {"affected_cells":1}

curl -u admin:change-me -X POST localhost:8080/rest/query \
  -H 'content-type: application/json' \
  -d '{"query": "DELETE WHERE material = '"'"'air'"'"'"}'
# {"affected_cells":1}
```

`SELECT`'s response has `total_rows`/`rows`; `SET`/`UPDATE`/`DELETE`'s has
`affected_cells` instead (the fields the statement kind doesn't produce
are omitted, not null). Each row's `values` entries carry the same
`created_at_ms`/`modified_at_ms`/`version` metadata the single-cell `GET`
reports (see "Cells and regions" above), not just the value. `DELETE`
clears *every* key set at each matching cell -- there's no column list to
delete only some of them.

`SELECT`/`UPDATE`/`DELETE` run on `kblockdblib::World::list_cells` under
the hood (the same full-chunk-decode walk the `/` data browser below uses)
-- `SELECT` filters and projects it directly; `UPDATE`/`DELETE` use it to
find matching coordinates, then apply the write in a second pass. `SET`
without a `WHERE` skips `list_cells` entirely and goes straight to
`World::set_region` (the same primitive `/rest/regions` uses) -- one call
per assignment, as efficient as the region endpoints; `SET` *with* a
`WHERE` falls back to a `list_cells`-plus-per-coordinate-`Region::iter`
scan, since which coordinates match can depend on a cell's existing
values. None of this is atomic with respect to a concurrent writer
touching the same range in between the scan and the write -- a real (if
narrow) race, same honest trade-off `list_cells`'s own doc comment already
makes for reads. Fine for the occasional bulk edit; not meant for a world
with millions of populated cells or for these write statements racing each
other at high frequency.

## Data browser

`GET /` (note: *not* under `/rest` -- see below) serves a small
self-contained HTML/JS page listing every populated cell in the world, one
row per cell, sorted ascending by coordinate, with a search bar and paging
controls. Clicking a row opens a modal with that cell's full keys, values,
and metadata (`created_at_ms`/`modified_at_ms`/`version` per key -- see
"REST API" above). It's read-only (there's no way to edit anything from
here) and requires the same HTTP Basic Auth as the REST API -- a
`read_only` account can browse same as any other, since it's all `GET`.
The browser's own credential prompt (triggered by `/`'s `401`) is what a
plain HTML page gets for free from the browser itself; the page's own JS
never handles a password.

`GET /rows?page=&page_size=&search=` is the JSON endpoint the page's JS
calls (1-based `page`, default 50/max 500 `page_size`, an optional
case-insensitive `search` matched against a cell's coordinate, any key
name, or any value's rendered text) -- each row already carries its full
per-key breakdown, so opening a modal needs no second request. Built on
`kblockdblib::World::list_cells`, which -- like `/rest/stats` above, but
heavier, since it decodes whole chunk files rather than just reading their
sizes -- is a live, uncached filesystem walk redone on every call. Fine
for a world browsed occasionally; not meant for a world with millions of
populated cells polled repeatedly.

Deliberately kept outside `/rest`: this is a convenience UI over the same
data, not part of the versioned REST API surface -- it has no OpenAPI
annotation and doesn't appear in `/rest/api-docs/openapi.json`.

## Binary protocol

A binary protocol covering the full REST API surface above -- health,
stats, single-cell and region operations, schema columns, and queries --
as a *peer* to
that API, not a replacement for it. It uses the same `World`, accounts,
and semantics, just without HTTP/JSON's per-call overhead (see the
[storage engine's cache measurements](kblockdblib.md#concurrency) for why
that overhead is worth caring about in the first place: on a cache-hit
`get`, roughly three-quarters of the total call time measured there was
HTTP/transport, not the actual work). Disabled by default -- enable it
with `--binary-addr <host:port>` (or `binary_addr` in the config file)
alongside the REST API's own `--http-addr`; both can run at once, against
the same `World`.

Authentication here is per-*connection*, not per-request the way HTTP
Basic Auth is: a client sends one `Hello` right after connecting
(username/password), and every request after that on the same connection
is treated as that account until the connection closes (or a later
`Hello` re-authenticates as someone else -- allowed, not required). One
request, one response, strictly in order -- this minimal version doesn't
pipeline multiple in-flight requests on one connection; a client that
wants more throughput than one connection's round-trip latency allows
should open more connections, the same way it would against the REST
API.

Every message, either direction, is a length-prefixed frame
(`[u32 LE payload_len][payload_len bytes]`) -- see
`kblockdbserver/src/wire.rs`'s doc comment for the exact byte-level format
of every request (`Hello`, health/stats, cell/region operations, column
add/remove/list, and queries) and response kind. The `Health` response
carries the same five fields as `/rest/health` -- hostname, axes,
world_dim, chunk_dim, timestamp -- read from the same server state, so
the two transports can never disagree about what this instance is. A
successful `Get`'s `Value` response carries the cell's metadata alongside
its value -- `created_at_ms`/`modified_at_ms`/`version`, the same three
fields the REST API's single-cell `GET` reports (see "Cells and regions"
above) -- read from the same server-side snapshot, so the two can never
disagree. Framing only depends on the length prefix, never on
understanding the payload, so a malformed request (an unknown opcode, a
bad coordinate, and so on) becomes an error response rather than closing
the connection.

`kblockdbperf/src/binary_client.rs` uses the server's published `wire`
module directly. The standalone [Java client](java-client.md) and
[Python client](python-client.md) reimplement the format for their
respective runtimes.

## Layout

- `kblockdbserver/src/main.rs`       -- CLI arg parsing, loads the config file,
  opens the world, starts the server (with graceful shutdown on Ctrl+C).
- `kblockdbserver/src/config.rs`     -- `Config`, the `--config` TOML file
  (http_addr/binary_addr/data_dir/max_concurrent_disk_ops/max_cached_chunks/
  compression, hostname,
  admin_password, `[[users]]`, and a `[worldparameters]` table for
  axes/world_dim/chunk_size) and its validation.
- `kblockdbserver/src/auth.rs`       -- the HTTP Basic Auth middleware applied
  to every route except `/rest/health`, including the `read_only` write
  check; also `account_from_headers`, the same check factored out for
  `/rest/query`'s handler, which can't use the middleware itself (see
  `routes.rs`'s doc comment).
- `kblockdbserver/src/routes.rs`     -- the router (mounted under `/rest`,
  see "REST API" above), all HTTP handlers, and each one's
  `#[utoipa::path(...)]` OpenAPI annotation.
- `kblockdbserver/src/query.pest`/`query.rs` -- the query language (see
  "Query language" above): grammar, AST, parsing, and in-memory evaluation
  against a `kblockdblib::CellEntry` (no I/O -- `routes.rs`'s query handler
  owns every actual `World` call the parsed statement implies).
- `kblockdbserver/src/openapi.rs`    -- `ApiDoc`, the `utoipa::OpenApi` derive
  that collects every handler's annotation (and every response type's
  `#[derive(ToSchema)]`) into the spec served at `/rest/api-docs/openapi.json`,
  plus the `basic_auth` security scheme those annotations reference.
- `kblockdbserver/src/browser.rs`    -- the `/` data browser (see "Data
  browser" above): `GET /` (the page) and `GET /rows` (its JSON backend,
  built on `kblockdblib::World::list_cells`).
- `kblockdbserver/src/browser.html`  -- the browser's self-contained
  HTML/CSS/JS, embedded into the binary via `include_str!` (no external
  scripts/styles, no build step).
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
- `kblockdbserver/src/wire.rs`       -- the binary protocol's wire format
  (see "Binary protocol" above): frame I/O, and every request/response
  kind's encode/decode, plus their own round-trip tests.
- `kblockdbserver/src/binary_server.rs` -- the binary protocol's TCP
  listener and per-connection handler, reusing the same `AppState` the
  REST API's handlers do.
