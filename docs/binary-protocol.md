# Binary protocol

A binary protocol covering [the REST API](kblockdbserver.md)'s full
surface -- database management, health, stats, single-cell and region
operations, schema columns, and [queries](query-language.md) -- as a
*peer* to that API, not a replacement for it. It uses the same databases,
accounts, and semantics, just without HTTP/JSON's per-call overhead (see
the [storage engine's cache measurements](kblockdblib.md#concurrency) for
why that overhead is worth caring about in the first place: on a
cache-hit `get`, roughly three-quarters of the total call time measured
there was HTTP/transport, not the actual work). Disabled by default --
enable it with `--binary-port <port>` (or `binary_port` in the config
file) alongside the REST API's own `--http-port`; both can run at once,
against the same databases.

Authentication *and database selection* here are per-*connection*, not
per-request the way HTTP Basic Auth plus a `/db/{name}/` path segment are:
a client sends one `Hello` right after connecting (username, password,
*and* the database to select), and every request after that on the same
connection is treated as that account against that database until the
connection closes (or a later `Hello` re-selects either -- allowed, not
required). If `Hello`'s named database doesn't exist yet, the account
still authenticates -- so `ListDatabases`/`CreateDatabase`/
`RemoveDatabase` work regardless -- but no database is selected, and every
data request (`Get`/`Set`/`Query`/...) gets a `BadRequest` until the
client creates that database and sends `Hello` again on the same
connection to actually select it. (The REST API has no equivalent
wrinkle: Basic Auth is already per-request, so `PUT /rest/databases/{name}`
needs nothing more than valid credentials.) One request, one response,
strictly in order -- this minimal version doesn't pipeline multiple
in-flight requests on one connection; a client that wants more throughput
than one connection's round-trip latency allows should open more
connections, the same way it would against the REST API.

`Hello` also carries the client's protocol version (`PROTOCOL_VERSION`,
`1` as of this writing), and `HelloOk` echoes back the version the server
speaks -- the one place every client already round-trips before anything
else, so it's the one choke point a version mismatch needs catching at,
rather than every opcode carrying its own version. A client claiming a
version *newer* than the server understands is rejected with `BadRequest`
before credentials are even checked, since past that one byte the server
has no way to know what that version's field shapes look like; a client
claiming its own version or older is accepted. There's only ever been one
version so far, so none of this has had to do anything interesting yet --
it exists so a future, genuinely incompatible wire change has a number to
bump and a place to branch on it, and so a client built against a newer
protocol can read `HelloOk`'s reported version and adapt to an older
server if it needs to.

Every message, either direction, is a length-prefixed frame
(`[u32 LE payload_len][payload_len bytes]`) -- see
`kblockdbserver/src/wire.rs`'s doc comment for the exact byte-level format
of every request (`Hello`, health/stats, cell/region operations, column
add/remove/list, database list/create/remove, and queries) and response
kind. The `Health` response carries the same fields as `GET /rest/health`
-- hostname, database count, timestamp -- read from the same server
state, so the two transports can never disagree about what this instance
is. A successful `Hello` (one that selected a database) reports that
database's axes/world_dim/chunk_dim, the same shape a
`GET /rest/db/{db}/stats` call's target database has. A successful `Get`'s
`Value` response carries the cell's metadata alongside its value --
`created_at_ms`/`modified_at_ms`/`version`, the same three fields the REST
API's single-cell `GET` reports -- read from the same server-side
snapshot, so the two can never disagree. Framing only depends on the
length prefix, never on understanding the payload, so a malformed request
(an unknown opcode, a bad coordinate, and so on) becomes an error response
rather than closing the connection.

`kblockdbperf/src/binary_client.rs` uses the server's published `wire`
module directly. The standalone [Java client](java-client.md) and
[Python client](python-client.md) reimplement the format for their
respective runtimes.
