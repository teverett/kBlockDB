# `kblockdbcli`: the command-line client

A thin wrapper over kblockdbserver's `/rest/cells/{coords}/{key}` and
`/rest/query` endpoints -- `get`, `set`, and `remove` one cell's value, or run
a `SELECT`/`SET`/`UPDATE`/`DELETE` query, from a shell, with the same HTTP
Basic Auth every other client of kblockdbserver's REST API needs.

## Build & run

```sh
cargo build --release
./target/release/kblockdbcli --password change-me set 1,2,3 material str stone
./target/release/kblockdbcli --password change-me get 1,2,3 material
# str stone (created=1735689600000 modified=1735689600000 version=0)
./target/release/kblockdbcli --password change-me remove 1,2,3 material
./target/release/kblockdbcli --password change-me query \
    "SELECT * FROM (0,0,0) TO (9,9,9) WHERE material = 'stone'"
# (1,2,3) material=stone (str) (created=1735689600000 modified=1735689600000 version=0)
# 1 row(s)
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

OPTIONS:
    --url <url>        kblockdbserver base URL (default: http://127.0.0.1:8080)
    --user <name>      Username (default: admin)
    --password <pw>    Password (or set the KBLOCKDBCLI_PASSWORD env var, so it
                        doesn't end up in shell history)
    -h, --help         Print this help
```

`<coords>` is a comma-separated coordinate, one `i32` per axis (`1,2,3` for
a 3-axis world, or `-1,2,-3` -- a world's valid range is centered on zero),
matching however many axes the target world
was created with -- same convention as the REST API itself.

`get`'s output and `set`'s trailing two arguments share one format
(`<type> <value>`, e.g. `str stone` or `i64 42`) on purpose, so the two
compose directly: `kblockdbcli ... set 4,5,6 backup $(kblockdbcli ... get 1,2,3
material)` copies one cell's value to another. Errors (a malformed
coordinate, wrong credentials, no value set, ...) print kblockdbserver's own
error message to stderr and exit non-zero -- nothing is swallowed or
retried silently.

`query` takes the entire query text as one shell-quoted argument (see the
[query language](kblockdbserver.md#query-language) for the grammar) and posts it to
`/rest/query`. A `SELECT` prints one line per matching cell, as `(coords)
key=value (type) (created=<ms> modified=<ms> version=<n>), ...` -- the same
per-key metadata `get` reports -- followed by a `<n> row(s)` summary; `SET`, `UPDATE`,
and `DELETE` print `<n> cell(s) affected`. `SET` is an upsert and requires
`IN <range>` (it can create cells; `UPDATE` only ever changes cells that
already exist). All three writes need a non-read-only account, same as
`set`/`remove`.

## Layout

- `kblockdbcli/src/main.rs`  -- CLI parsing, the HTTP calls (via
  `reqwest::blocking`, so a one-shot command doesn't need an async
  runtime), the `<type> <value>` <-> kblockdbserver's tagged-JSON
  conversion (`build_value_json`/`describe_value`), and `query`'s response
  rendering (`print_query_response`/`describe_query_row`).
- `kblockdbcli/src/tests.rs` -- integration tests that spawn a real `kblockdbserver`
  and run the actual compiled `kblockdbcli` binary against it via
  `std::process::Command`, checking real stdout and exit codes.
