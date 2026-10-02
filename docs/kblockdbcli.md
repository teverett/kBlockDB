# `kblockdbcli`: the command-line client

A thin wrapper over kblockdbserver's `/rest/databases`,
`/rest/db/{db}/cells/{coords}/{key}`, and `/rest/db/{db}/query` endpoints --
list/create/delete databases; `get`, `set`, and `remove` one cell's value;
or run a `SELECT`/`SET`/`UPDATE`/`DELETE` query -- from a shell, with the
same HTTP Basic Auth every other client of kblockdbserver's REST API needs.

No database exists until you create one -- there's no implicit default, so
`create-database` has to run before any `--db` command targeting that name
will work.

## Build & run

```sh
cargo build --release
./target/release/kblockdbcli --password change-me create-database myapp
./target/release/kblockdbcli --password change-me --db myapp set 1,2,3 material str stone
./target/release/kblockdbcli --password change-me --db myapp get 1,2,3 material
# str stone (created=1735689600000 modified=1735689600000 version=0)
./target/release/kblockdbcli --password change-me --db myapp remove 1,2,3 material
./target/release/kblockdbcli --password change-me --db myapp query \
    "SELECT * FROM (0,0,0) TO (9,9,9) WHERE material = 'stone'"
# (1,2,3) material=stone (str) (created=1735689600000 modified=1735689600000 version=0)
# 1 row(s)
./target/release/kblockdbcli --password change-me --db myapp add-column hardness f64
./target/release/kblockdbcli --password change-me --db myapp columns
# hardness f64
# material str
# 2 column(s)
./target/release/kblockdbcli --password change-me --db myapp remove-column hardness

./target/release/kblockdbcli --password change-me databases
# myapp
# 1 database(s)
./target/release/kblockdbcli --password change-me remove-database myapp
```

```
USAGE:
    kblockdbcli [OPTIONS] <COMMAND> [ARGS]

COMMANDS:
    get <coords> <key>                  Print a cell's value and metadata, as
                                         `<type> <value> (created=<ms> modified=<ms> version=<n>)`
    set <coords> <key> <type> <value>   Set a cell's value (type: str, f64, i64, or bool)
    remove <coords> <key>               Clear a cell's value
    query <query-text>                  Run a SELECT/SET/UPDATE/DELETE query (see below)
    columns                             List the database's schema, one `<key> <type>` per line
    add-column <key> <type>             Create a column (type: str, f64, i64, or bool)
    remove-column <key>                 Drop a column and every value ever written for it
    databases                           List every database on the server
    create-database <name>              Create a database (see --axes/--world-dim/
                                         --chunk-size below to override the server's defaults)
    remove-database <name>              Delete a database and every byte of its data

OPTIONS:
    --url <url>        kblockdbserver base URL (default: http://127.0.0.1:8080)
    --user <name>      Username (default: admin)
    --password <pw>    Password (or set the KBLOCKDBCLI_PASSWORD env var, so it
                        doesn't end up in shell history)
    --db <name>        Database to operate on -- required for get/set/remove/query/
                        columns/add-column/remove-column; not used by databases/
                        create-database/remove-database, which name their database as
                        a plain argument instead
    --axes <n>         create-database only: axis count, if not the server's default
    --world-dim <n>    create-database only: cells per axis, if not the server's default
    --chunk-size <n>   create-database only: cells per axis within a chunk, if not the
                        server's default
    -h, --help         Print this help
```

`<coords>` is a comma-separated coordinate, one `i32` per axis (`1,2,3` for
a 3-axis database, or `-1,2,-3` -- a database's valid range is centered on
zero), matching however many axes the target database was created with --
same convention as the REST API itself.

`get`'s output and `set`'s trailing two arguments share one format
(`<type> <value>`, e.g. `str stone` or `i64 42`) on purpose, so the two
compose directly: `kblockdbcli ... set 4,5,6 backup $(kblockdbcli ... get 1,2,3
material)` copies one cell's value to another. Errors (a malformed
coordinate, wrong credentials, no value set, no such database, ...) print
kblockdbserver's own error message to stderr and exit non-zero -- nothing
is swallowed or retried silently.

`query` takes the entire query text as one shell-quoted argument (see the
[query language](kblockdbserver.md#query-language) for the grammar) and
posts it to `/rest/db/{db}/query`. A `SELECT` prints one line per matching
cell, as `(coords) key=value (type) (created=<ms> modified=<ms> version=<n>), ...`
-- the same per-key metadata `get` reports -- followed by a `<n> row(s)`
summary; `SET`, `UPDATE`, and `DELETE` print `<n> cell(s) affected`. `SET`
is an upsert and requires `IN <range>` (it can create cells; `UPDATE` only
ever changes cells that already exist). All three writes need a
non-read-only account, same as `set`/`remove`.

`columns`/`add-column`/`remove-column` wrap the server's
[`/rest/db/{db}/columns`](kblockdbserver.md#columns) resource. `columns`
prints one `<key> <type>` line per column -- the same `<type> <value>`
ordering `get`/`set` use, so a column line's second field is directly
usable as a `set` type argument -- then a `<n> column(s)` summary.
`add-column` only ever creates: a key that already has a column is an
error, not a silent type change. `remove-column` drops the column *and
every value ever written for it*, across the whole database, and can't be
undone. Both writes need a non-read-only account.

`databases`/`create-database`/`remove-database` wrap
[`/rest/databases`](kblockdbserver.md#databases). `databases` prints one
name per line, then a `<n> database(s)` summary. `create-database` takes
its shape from `--axes`/`--world-dim`/`--chunk-size` when given, otherwise
the server's own configured default; a database that already exists is an
error. `remove-database` deletes the database and **every byte of its
data**, immediately and irreversibly. Both writes need a non-read-only
account.

## Layout

- `kblockdbcli/src/main.rs`  -- CLI parsing, the HTTP calls (via
  `reqwest::blocking`, so a one-shot command doesn't need an async
  runtime), the `<type> <value>` <-> kblockdbserver's tagged-JSON
  conversion (`build_value_json`/`describe_value`), and `query`'s response
  rendering (`print_query_response`/`describe_query_row`).
- `kblockdbcli/src/tests.rs` -- integration tests that spawn a real `kblockdbserver`
  and run the actual compiled `kblockdbcli` binary against it via
  `std::process::Command`, checking real stdout and exit codes.
