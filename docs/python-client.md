# Python client

`client/python` contains `KBlockDBClient`, a dependency-free synchronous
Python 3.10+ client for kBlockDB's binary protocol. It covers database
management, health, stats, single-cell operations, region operations, and
`SELECT`/`SET`/`UPDATE`/`DELETE` queries.

Each client owns one authenticated TCP connection that selects one
database for its whole life. Calls are strictly ordered and a client is
not thread-safe; use one client per concurrent connection.

`connect()` always names a database. If it doesn't exist yet, the
connection still authenticates (`client.database_selected` is `False`),
but every data method raises `ProtocolError` until the database is created
(`create_database`, which needs no selected database) and then selected on
this same connection (`use_database`, which re-sends `Hello` with the
credentials `connect` was given):

```python
client = KBlockDBClient.connect("localhost", 8081, "admin", "change-me", "newdb")
if not client.database_selected:
    client.create_database("newdb")
    client.use_database("newdb")
```

## Install

Install the package from the repository:

```sh
python3 -m pip install ./client/python
```

The server's binary listener must be enabled with `--binary-port` or the
`binary_port` configuration setting. See the
[server documentation](kblockdbserver.md#binary-protocol).

## Usage

```python
from kblockdb import Affected, I64, KBlockDBClient, Rows, Str, ValueType

with KBlockDBClient.connect(
    "localhost", 8081, "admin", "change-me", "demo"
) as db:
    if not db.database_selected:
        db.create_database("demo")
        db.use_database("demo")

    health = db.health()
    stats = db.stats()

    coord = (1, 2, 3)
    db.set(coord, "material", Str("stone"))
    cell = db.get_with_meta(coord, "material")
    db.remove(coord, "material")

    origin = (0, 0, 0)
    extent = (2, 1, 1)
    db.set_region(
        origin,
        extent,
        "material",
        (Str("stone"), Str("air")),
    )
    region = db.get_region(origin, extent, "material")
    db.remove_region(origin, extent, "material")

    result = db.query("SELECT material WHERE material = 'stone'")
    if isinstance(result, Rows):
        print(result.total_rows)
    elif isinstance(result, Affected):
        print(result.affected_cells)

    db.add_column("hardness", ValueType.F64)
    columns = db.columns()
    db.remove_column("hardness")
```

## API

| Method | Result | Description |
|---|---|---|
| `connect(host, port, username, password, database, timeout=None)` | `KBlockDBClient` | Connects, authenticates, and selects `database` (see above if it doesn't exist yet). |
| `reauthenticate(username, password)` | `None` | Changes the account on the existing connection, keeping the selected database. |
| `use_database(name)` | `None` | Selects a different database on the existing connection, reusing the cached credentials. |
| `list_databases()` | `tuple[str, ...]` | Every database the server manages, sorted by name. Needs no selected database. |
| `create_database(name, *, axes=None, world_dim=None, chunk_size=None)` | `None` | Creates a database with the server's default shape. Needs no selected database. Passing a shape kwarg raises `ValueError` -- the binary protocol can't express an override; use the REST API for that. |
| `remove_database(name)` | `bool` | Deletes a database and all its data; `False` if it didn't exist. |
| `health()` | `Health` | Returns the server's `hostname`, `database_count`, and `timestamp`. Needs no selected database. |
| `stats()` | `Stats` | Returns live on-disk chunk, byte, and block totals for the selected database. |
| `get(coord, key)` | `Value \| None` | Reads one value. |
| `get_with_meta(coord, key)` | `ValueWithMeta \| None` | Reads one value with timestamps and version. |
| `set(coord, key, value)` | `None` | Writes one value. |
| `remove(coord, key)` | `None` | Clears one value. |
| `get_region(origin, extent, key)` | `tuple[Value \| None, ...]` | Reads a region in axis-0-fastest order. |
| `set_region(origin, extent, key, values)` | `None` | Writes a region in axis-0-fastest order. |
| `remove_region(origin, extent, key)` | `None` | Clears a key throughout a region. |
| `query(query)` | `Rows \| Affected` | Executes any query-language statement. |
| `columns()` | `tuple[Column, ...]` | Lists the selected database's schema, sorted by key. |
| `add_column(key, value_type)` | `None` | Creates a column, fixing its type. |
| `remove_column(key)` | `bool` | Drops a column and all its values; `False` if it didn't exist. |

Every data method (everything but `connect`/`reauthenticate`/
`use_database`/`list_databases`/`create_database`/`remove_database`/
`health`) raises `ProtocolError` immediately, without a round trip, if no
database is currently selected (`client.database_selected` is `False`) --
see the bootstrap flow above.

Values use explicit `Str`, `F64`, `I64`, and `Bool` wrappers so their
binary type is never ambiguous. Coordinates and region values accept any
iterable and responses use immutable tuples and frozen dataclasses.

`ValueType` names those same four types without a value attached, and is
what a `Column` carries. `add_column` only ever creates -- re-adding an
existing key raises `ConflictError` rather than changing its type -- and
`remove_column` drops every value ever written for the key, across the
whole database. See the [server's notes](kblockdbserver.md#columns) for
the details.

See the [query language documentation](kblockdbserver.md#query-language)
for the grammar and examples.

## Errors

All server and protocol errors extend `OSError`:

- `BadRequestError` reports malformed coordinates, regions, values, or
  queries.
- `UnauthorizedError` reports rejected credentials.
- `ForbiddenError` reports a write attempted through a read-only account.
- `ConflictError` reports an `add_column` for a key that already has a
  column, or a `create_database` for a name that already exists.
- `InternalServerError` reports a server-side storage failure.
- `ProtocolError` reports malformed or incompatible wire data.

Socket errors and clean or partial disconnects retain their standard
Python exception types.

A request that fails part way through writing its frame or reading the
response leaves the stream at an unknown offset, so the client closes
the socket and permanently marks it unusable; every later call on that
client raises `ProtocolError`. Errors that arrive as a complete
response frame -- including `BadRequestError` -- leave the connection
fully usable, matching the server's own guarantee.

## Test

Build the real server used by the integration tests, then run the
standard-library test suite:

```sh
cargo build -p kblockdbserver
PYTHONPATH=client/python/src \
  python3 -m unittest discover -s client/python/tests -v
```

## Layout

- `client/python/src/kblockdb/client.py` -- public connection and API.
- `client/python/src/kblockdb/_wire.py` -- binary request encoding,
  response decoding, and frame I/O.
- `client/python/src/kblockdb/models.py` -- values and response models.
- `client/python/src/kblockdb/exceptions.py` -- typed errors.
- `client/python/tests/test_wire.py` -- pinned byte-level protocol tests.
- `client/python/tests/test_client_integration.py` -- end-to-end tests
  against a real server.
