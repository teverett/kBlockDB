# Java client

`client/java` contains `KBlockDBClient`, a dependency-free Java 17 client
for kBlockDB's binary protocol. It covers the full API surface: database
management, health, stats, single-cell operations, region operations, and
`SELECT`/`SET`/`UPDATE`/`DELETE` queries.

Each client instance owns one authenticated TCP connection that selects
one database for its whole life. Calls are strictly ordered and the client
is not thread-safe; use one client per concurrent connection.

`connect()` always names a database. If it doesn't exist yet, the
connection still authenticates (`databaseSelected()` is `false`, and
`axes()`/`worldDim()`/`chunkDim()` are meaningless `0`s), but every data
call (`get`, `set`, `query`, ...) gets a `BadRequestException` from the
server until the database is created (`createDatabase`, which needs no
selected database) and then selected on this same connection
(`useDatabase`, which re-sends `Hello` with the credentials `connect` was
given):

```java
try (KBlockDBClient db =
        KBlockDBClient.connect("localhost", 8081, "admin", "change-me", "newdb")) {
    if (!db.databaseSelected()) {
        db.createDatabase("newdb");
        db.useDatabase("newdb");
    }
    // ... db.set(...), db.get(...), ...
}
```

## Build

Build and test the client from the repository root:

```sh
cargo build -p kblockdbserver
mvn -f client/java/pom.xml package
```

The resulting dependency-free jar is written to
`client/java/target/kblockdb-client-0.2.0.jar`.

The server's binary listener must be enabled with `--binary-port` or the
`binary_port` configuration setting. See the
[binary protocol documentation](binary-protocol.md).

## Usage

```java
import com.kblockdb.client.Column;
import com.kblockdb.client.Health;
import com.kblockdb.client.KBlockDBClient;
import com.kblockdb.client.QueryResult;
import com.kblockdb.client.Stats;
import com.kblockdb.client.Value;
import com.kblockdb.client.ValueType;
import com.kblockdb.client.ValueWithMeta;

import java.util.List;
import java.util.Optional;

try (KBlockDBClient db =
        KBlockDBClient.connect("localhost", 8081, "admin", "change-me", "demo")) {
    if (!db.databaseSelected()) {
        db.createDatabase("demo");
        db.useDatabase("demo");
    }

    Health health = db.health();
    Stats stats = db.stats();

    int[] coord = {1, 2, 3};
    db.set(coord, "material", new Value.Str("stone"));
    Optional<ValueWithMeta> cell = db.getWithMeta(coord, "material");
    db.remove(coord, "material");

    int[] origin = {0, 0, 0};
    int[] extent = {2, 1, 1};
    db.setRegion(
        origin,
        extent,
        "material",
        List.of(new Value.Str("stone"), new Value.Str("air")));
    List<Optional<Value>> region =
        db.getRegion(origin, extent, "material");
    db.removeRegion(origin, extent, "material");

    QueryResult result =
        db.query("SELECT material WHERE material = 'stone'");

    db.addColumn("hardness", ValueType.F64);
    List<Column> columns = db.columns();
    db.removeColumn("hardness");
}
```

## API

| Method | Result | Description |
|---|---|---|
| `connect(host, port, username, password, database)` | `KBlockDBClient` | Connects, authenticates, and selects `database` (see above if it doesn't exist yet). |
| `reauthenticate(username, password)` | `void` | Changes the account on the existing connection, keeping the selected database. |
| `useDatabase(name)` | `void` | Selects a different database on the existing connection, reusing the cached credentials. |
| `databaseSelected()` | `boolean` | Whether this connection currently has a database selected. |
| `serverVersion()` | `int` | This server's binary protocol version, as reported by the most recent `Hello` -- meaningful whether or not a database is selected. |
| `listDatabases()` | `List<String>` | Every database the server manages, sorted by name. Needs no selected database. |
| `createDatabase(name)` | `void` | Creates a database with the server's default shape. Needs no selected database. (No way to override the shape over this protocol -- use the REST API's `PUT /rest/databases/{name}` for that.) |
| `removeDatabase(name)` | `boolean` | Deletes a database and all its data; `false` if it didn't exist. |
| `health()` | `Health` | Returns the server's `hostname`, `databaseCount`, and `timestamp`. Needs no selected database. |
| `stats()` | `Stats` | Returns live on-disk chunk, byte, and block totals for the selected database. |
| `get(coord, key)` | `Optional<Value>` | Reads one value. |
| `getWithMeta(coord, key)` | `Optional<ValueWithMeta>` | Reads one value with timestamps and version. |
| `set(coord, key, value)` | `void` | Writes one value. |
| `remove(coord, key)` | `void` | Clears one value. |
| `getRegion(origin, extent, key)` | `List<Optional<Value>>` | Reads a region in axis-0-fastest order. |
| `setRegion(origin, extent, key, values)` | `void` | Writes a region in axis-0-fastest order. |
| `removeRegion(origin, extent, key)` | `void` | Clears a key throughout a region. |
| `query(query)` | `QueryResult` | Executes any query-language statement. |
| `columns()` | `List<Column>` | Lists the selected database's schema, sorted by key. |
| `addColumn(key, valueType)` | `void` | Creates a column, fixing its type. |
| `removeColumn(key)` | `boolean` | Drops a column and all its values; false if it didn't exist. |

Every data method (everything but `connect`/`reauthenticate`/
`useDatabase`/`listDatabases`/`createDatabase`/`removeDatabase`/`health`)
throws `BadRequestException` from the server if no database is currently
selected -- see the bootstrap flow above. Unlike the Python client, this
is a server round trip, not a client-side pre-check; call
`databaseSelected()` first if you want to avoid it.

`ValueType` names those same four types without a value attached, and is
what a `Column` carries. `addColumn` only ever creates -- re-adding an
existing key throws `ConflictException` rather than changing its type --
and `removeColumn` drops every value ever written for the key, across the
whole database. See the [server's notes](kblockdbserver.md#columns) for
the details.

`Value` has `Str`, `F64`, `I64`, and `Bool` variants. A `SELECT` returns
`QueryResult.Rows`; mutating statements return `QueryResult.Affected`.
See the [query language documentation](query-language.md) for the grammar
and examples.

## Errors

All server and protocol errors extend `IOException`:

- `BadRequestException` reports malformed coordinates, regions, values,
  or queries.
- `UnauthorizedException` reports rejected credentials.
- `ForbiddenException` reports a write attempted through a read-only
  account.
- `ConflictException` reports an `addColumn` for a key that already has a
  column, or a `createDatabase` for a name that already exists.
- `InternalErrorException` reports a server-side storage failure.
- `ProtocolException` reports malformed or incompatible wire data.

## Layout

- `client/java/src/main/java/com/kblockdb/client/KBlockDBClient.java` --
  public connection and API implementation.
- `client/java/src/main/java/com/kblockdb/client/Wire.java` -- binary
  request encoding, response decoding, and frame I/O.
- `client/java/src/main/java/com/kblockdb/client/Value.java` -- typed
  values.
- `client/java/src/main/java/com/kblockdb/client/ValueType.java` and
  `Column.java` -- schema models.
- `client/java/src/main/java/com/kblockdb/client/Query*.java` -- query
  result models.
- `client/java/src/test/java/com/kblockdb/client/WireTest.java` -- pinned
  byte-level protocol tests.
- `client/java/src/test/java/com/kblockdb/client/KBlockDBClientIntegrationTest.java`
  -- end-to-end tests against a real server.
