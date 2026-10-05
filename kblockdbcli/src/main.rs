//! `kblockdbcli` -- a command-line client for kblockdbserver's REST API: get, set,
//! and remove a single cell's value for a key, plus `query` for the
//! `SELECT`/`SET`/`UPDATE`/`DELETE` query language over `POST
//! /rest/db/{db}/query`, plus managing which databases exist on the server.
//! A thin wrapper over HTTP Basic Auth and those endpoints, nothing more
//! (see kblockdbserver's README section for the fuller API this could grow
//! into covering, e.g. `/rest/db/{db}/regions`).

#[cfg(test)]
mod tests;

use serde_json::{json, Value as Json};
use std::process::ExitCode;

/// Every command name `parse_args` accepts, for the two "expected one of"
/// messages -- kept in one place so adding a command can't leave one of
/// them listing a stale set.
const COMMAND_LIST: &str = "get, set, remove, query, columns, add-column, remove-column, \
     databases, create-database, remove-database";

struct Args {
    url: String,
    user: String,
    password: String,
    /// Required (and validated by `parse_args`) for every command that
    /// operates on a specific database's data; unused by `databases`/
    /// `create-database`/`remove-database`, which name their database
    /// positionally instead -- see this module's doc comment and
    /// `COMMAND_LIST`.
    db: Option<String>,
    command: Command,
}

enum Command {
    Get {
        coords: String,
        key: String,
    },
    Set {
        coords: String,
        key: String,
        value_type: String,
        value: String,
    },
    Remove {
        coords: String,
        key: String,
    },
    Query {
        query: String,
    },
    Columns,
    AddColumn {
        key: String,
        value_type: String,
    },
    RemoveColumn {
        key: String,
    },
    Databases,
    CreateDatabase {
        name: String,
        axes: Option<usize>,
        world_dim: Option<u32>,
        chunk_size: Option<u32>,
    },
    RemoveDatabase {
        name: String,
    },
}

/// Every `Command` that operates on one specific database's data, and so
/// needs `Args.db` to already be `Some` -- `parse_args` enforces this
/// before `main` ever sees the parsed `Args`. `Databases`/`CreateDatabase`/
/// `RemoveDatabase` aren't here: they name their database positionally
/// instead (see this module's doc comment).
fn command_needs_db(command: &Command) -> bool {
    !matches!(
        command,
        Command::Databases | Command::CreateDatabase { .. } | Command::RemoveDatabase { .. }
    )
}

fn main() -> ExitCode {
    let args = parse_args();
    let client = reqwest::blocking::Client::new();

    let result = match &args.command {
        Command::Get { coords, key } => run_get(&client, &args, coords, key),
        Command::Set {
            coords,
            key,
            value_type,
            value,
        } => run_set(&client, &args, coords, key, value_type, value),
        Command::Remove { coords, key } => run_remove(&client, &args, coords, key),
        Command::Query { query } => run_query(&client, &args, query),
        Command::Columns => run_columns(&client, &args),
        Command::AddColumn { key, value_type } => run_add_column(&client, &args, key, value_type),
        Command::RemoveColumn { key } => run_remove_column(&client, &args, key),
        Command::Databases => run_databases(&client, &args),
        Command::CreateDatabase {
            name,
            axes,
            world_dim,
            chunk_size,
        } => run_create_database(&client, &args, name, *axes, *world_dim, *chunk_size),
        Command::RemoveDatabase { name } => run_remove_database(&client, &args, name),
    };

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("{message}");
            ExitCode::FAILURE
        }
    }
}

fn parse_args() -> Args {
    let mut url = "http://127.0.0.1:8080".to_string();
    let mut user = "admin".to_string();
    let mut password = std::env::var("KBLOCKDBCLI_PASSWORD").ok();
    let mut db: Option<String> = None;
    let mut shape_axes: Option<usize> = None;
    let mut shape_world_dim: Option<u32> = None;
    let mut shape_chunk_size: Option<u32> = None;
    let mut command_name: Option<String> = None;
    let mut positional: Vec<String> = Vec::new();

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--url" => url = expect_value(&mut args, "--url"),
            "--user" => user = expect_value(&mut args, "--user"),
            "--password" => password = Some(expect_value(&mut args, "--password")),
            "--db" => db = Some(expect_value(&mut args, "--db")),
            // Only meaningful for `create-database`; parsed globally here
            // same as every other flag, rather than hand-rolled inside that
            // one command's own branch below.
            "--axes" => shape_axes = Some(expect_parsed(&mut args, "--axes")),
            "--world-dim" => shape_world_dim = Some(expect_parsed(&mut args, "--world-dim")),
            "--chunk-size" => shape_chunk_size = Some(expect_parsed(&mut args, "--chunk-size")),
            "-h" | "--help" => {
                print_help();
                std::process::exit(0);
            }
            other if command_name.is_none() => command_name = Some(other.to_string()),
            other => positional.push(other.to_string()),
        }
    }

    let command_name = command_name.unwrap_or_else(|| {
        eprintln!("missing command -- expected one of: {COMMAND_LIST}\n");
        print_help();
        std::process::exit(1);
    });

    let command = match command_name.as_str() {
        "get" => match positional.as_slice() {
            [coords, key] => Command::Get {
                coords: coords.clone(),
                key: key.clone(),
            },
            _ => usage_error("get <coords> <key>", &positional),
        },
        "set" => match positional.as_slice() {
            [coords, key, value_type, value] => Command::Set {
                coords: coords.clone(),
                key: key.clone(),
                value_type: value_type.clone(),
                value: value.clone(),
            },
            _ => usage_error("set <coords> <key> <type> <value>", &positional),
        },
        "remove" => match positional.as_slice() {
            [coords, key] => Command::Remove {
                coords: coords.clone(),
                key: key.clone(),
            },
            _ => usage_error("remove <coords> <key>", &positional),
        },
        "query" => {
            if positional.is_empty() {
                usage_error("query <query-text>", &positional)
            } else {
                Command::Query {
                    query: positional.join(" "),
                }
            }
        }
        "columns" => match positional.as_slice() {
            [] => Command::Columns,
            _ => usage_error("columns", &positional),
        },
        "add-column" => match positional.as_slice() {
            [key, value_type] => Command::AddColumn {
                key: key.clone(),
                value_type: value_type.clone(),
            },
            _ => usage_error("add-column <key> <type>", &positional),
        },
        "remove-column" => match positional.as_slice() {
            [key] => Command::RemoveColumn { key: key.clone() },
            _ => usage_error("remove-column <key>", &positional),
        },
        "databases" => match positional.as_slice() {
            [] => Command::Databases,
            _ => usage_error("databases", &positional),
        },
        "create-database" => match positional.as_slice() {
            [name] => Command::CreateDatabase {
                name: name.clone(),
                axes: shape_axes,
                world_dim: shape_world_dim,
                chunk_size: shape_chunk_size,
            },
            _ => usage_error(
                "create-database <name> [--axes N] [--world-dim N] [--chunk-size N]",
                &positional,
            ),
        },
        "remove-database" => match positional.as_slice() {
            [name] => Command::RemoveDatabase { name: name.clone() },
            _ => usage_error("remove-database <name>", &positional),
        },
        other => {
            eprintln!("unknown command '{other}' -- expected one of: {COMMAND_LIST}\n");
            print_help();
            std::process::exit(1);
        }
    };

    if command_needs_db(&command) && db.is_none() {
        eprintln!("--db <name> is required for this command\n");
        print_help();
        std::process::exit(1);
    }

    let password = password.unwrap_or_else(|| {
        eprintln!("--password (or the KBLOCKDBCLI_PASSWORD env var) is required\n");
        print_help();
        std::process::exit(1);
    });

    Args {
        url,
        user,
        password,
        db,
        command,
    }
}

fn expect_value(args: &mut impl Iterator<Item = String>, flag: &str) -> String {
    args.next().unwrap_or_else(|| {
        eprintln!("{flag} requires a value");
        std::process::exit(1);
    })
}

fn expect_parsed<T: std::str::FromStr>(args: &mut impl Iterator<Item = String>, flag: &str) -> T {
    expect_value(args, flag).parse().unwrap_or_else(|_| {
        eprintln!("{flag} requires a numeric value");
        std::process::exit(1);
    })
}

fn usage_error(usage: &str, got: &[String]) -> ! {
    eprintln!(
        "usage: kblockdbcli [OPTIONS] {usage}\ngot {} argument(s): {got:?}\n",
        got.len()
    );
    print_help();
    std::process::exit(1);
}

fn print_help() {
    println!(
        "kblockdbcli -- a command-line client for kblockdbserver's REST API\n\n\
         USAGE:\n    kblockdbcli [OPTIONS] <COMMAND> [ARGS]\n\n\
         COMMANDS:\n    \
         get <coords> <key>                  Print a cell's value and metadata, as\n                                              \
         `<type> <value> (created=<ms> modified=<ms> version=<n>)`\n    \
         set <coords> <key> <type> <value>   Set a cell's value (type: str, f64, i64, or bool)\n    \
         remove <coords> <key>               Clear a cell's value\n    \
         query <query-text>                  Run a SELECT/SET/UPDATE/DELETE query (see below)\n    \
         columns                             List the database's schema, one `<key> <type>` per line\n    \
         add-column <key> <type>             Create a column (type: str, f64, i64, or bool)\n    \
         remove-column <key>                 Drop a column and every value ever written for it\n    \
         databases                           List every database on the server\n    \
         create-database <name>              Create a database (see --axes/--world-dim/\n                                              \
         --chunk-size below to override the server's defaults)\n    \
         remove-database <name>              Delete a database and every byte of its data\n\n\
         OPTIONS:\n    \
         --url <url>        kblockdbserver base URL (default: http://127.0.0.1:8080)\n    \
         --user <name>      Username (default: admin)\n    \
         --password <pw>    Password (or set the KBLOCKDBCLI_PASSWORD env var, so it\n                        \
         doesn't end up in shell history)\n    \
         --db <name>        Database to operate on -- required for get/set/remove/query/\n                        \
         columns/add-column/remove-column; not used by databases/\n                        \
         create-database/remove-database, which name their database as a\n                        \
         plain argument instead\n    \
         --axes <n>         create-database only: axis count, if not the server's default\n    \
         --world-dim <n>    create-database only: cells per axis, if not the server's default\n    \
         --chunk-size <n>   create-database only: cells per axis within a chunk, if not the\n                        \
         server's default\n    \
         -h, --help         Print this help\n\n\
         <coords> is a comma-separated coordinate, one i32 per axis (e.g. 1,2,3\n\
         or -1,2,-3 -- a database's valid range is centered on zero, see the\n\
         server's README), matching however many axes the target database was\n\
         created with.\n\n\
         EXAMPLES:\n    \
         kblockdbcli --password change-me create-database myapp\n    \
         kblockdbcli --password change-me --db myapp set 1,2,3 material str stone\n    \
         kblockdbcli --password change-me --db myapp get 1,2,3 material\n    \
         kblockdbcli --password change-me --db myapp remove 1,2,3 material\n    \
         kblockdbcli --password change-me --db myapp query \"SELECT * FROM (0,0,0) TO (9,9,9) WHERE material = 'stone'\"\n    \
         kblockdbcli --password change-me --db myapp query \"SET (material='stone') IN (0,0,0) TO (9,9,9)\"\n    \
         kblockdbcli --password change-me --db myapp query \"UPDATE (material='dirt') WHERE material = 'stone'\"\n    \
         kblockdbcli --password change-me --db myapp query \"DELETE WHERE material = 'air'\"\n    \
         kblockdbcli --password change-me --db myapp columns\n    \
         kblockdbcli --password change-me --db myapp add-column hardness f64\n    \
         kblockdbcli --password change-me --db myapp remove-column hardness\n    \
         kblockdbcli --password change-me databases\n    \
         kblockdbcli --password change-me remove-database myapp\n\n\
         Wrap the whole query in one shell-quoted argument -- it may contain\n\
         spaces and single-quoted string literals of its own. SET is an\n\
         upsert and requires IN <range> (it can create cells; UPDATE only\n\
         ever changes cells that already exist). SET, UPDATE, DELETE,\n\
         create-database, and remove-database all require a non-read-only\n\
         account. No database is ever created implicitly -- create-database\n\
         must run before anything else targets that --db name."
    );
}

fn cell_url(args: &Args, coords: &str, key: &str) -> String {
    format!(
        "{}/rest/db/{}/cells/{coords}/{key}",
        args.url.trim_end_matches('/'),
        db_name(args)
    )
}

/// `args.db`, assuming `parse_args`' `command_needs_db` check already
/// guaranteed it's `Some` for whichever command is calling this -- every
/// URL builder below is only ever reached by a command that needs it.
fn db_name(args: &Args) -> &str {
    args.db
        .as_deref()
        .expect("command_needs_db should have required --db before this ran")
}

fn run_get(
    client: &reqwest::blocking::Client,
    args: &Args,
    coords: &str,
    key: &str,
) -> Result<(), String> {
    let resp = client
        .get(cell_url(args, coords, key))
        .basic_auth(&args.user, Some(&args.password))
        .send()
        .map_err(|e| format!("request failed: {e}"))?;
    let status = resp.status();
    let body: Json = resp
        .json()
        .map_err(|e| format!("couldn't parse the server's response: {e}"))?;
    if !status.is_success() {
        return Err(server_error_message(status, &body));
    }
    let (value_type, rendered) = describe_value(&body["value"])?;
    let meta = describe_meta(&body)?;
    println!("{value_type} {rendered} {meta}");
    Ok(())
}

fn run_set(
    client: &reqwest::blocking::Client,
    args: &Args,
    coords: &str,
    key: &str,
    value_type: &str,
    value: &str,
) -> Result<(), String> {
    let body = build_value_json(value_type, value)?;
    let resp = client
        .put(cell_url(args, coords, key))
        .basic_auth(&args.user, Some(&args.password))
        .json(&body)
        .send()
        .map_err(|e| format!("request failed: {e}"))?;
    check_success(resp)
}

fn run_remove(
    client: &reqwest::blocking::Client,
    args: &Args,
    coords: &str,
    key: &str,
) -> Result<(), String> {
    let resp = client
        .delete(cell_url(args, coords, key))
        .basic_auth(&args.user, Some(&args.password))
        .send()
        .map_err(|e| format!("request failed: {e}"))?;
    check_success(resp)
}

fn columns_url(args: &Args) -> String {
    format!(
        "{}/rest/db/{}/columns",
        args.url.trim_end_matches('/'),
        db_name(args)
    )
}

fn databases_url(args: &Args) -> String {
    format!("{}/rest/databases", args.url.trim_end_matches('/'))
}

fn run_columns(client: &reqwest::blocking::Client, args: &Args) -> Result<(), String> {
    let resp = client
        .get(columns_url(args))
        .basic_auth(&args.user, Some(&args.password))
        .send()
        .map_err(|e| format!("request failed: {e}"))?;
    let status = resp.status();
    let body: Json = resp
        .json()
        .map_err(|e| format!("couldn't parse the server's response: {e}"))?;
    if !status.is_success() {
        return Err(server_error_message(status, &body));
    }
    print_columns_response(&body)
}

/// Renders a `GET /rest/columns` response (kblockdbserver's
/// `ColumnsResponse`) as one `<key> <type>` line per column, then a count
/// -- same shape as `query`'s row listing, so the two read alike.
fn print_columns_response(body: &Json) -> Result<(), String> {
    let columns = body
        .get("columns")
        .and_then(Json::as_array)
        .ok_or("response is missing 'columns'")?;
    for column in columns {
        let key = column["key"]
            .as_str()
            .ok_or("column entry is missing 'key'")?;
        let value_type = column["type"]
            .as_str()
            .ok_or("column entry is missing 'type'")?;
        println!("{key} {value_type}");
    }
    println!("{} column(s)", columns.len());
    Ok(())
}

fn run_add_column(
    client: &reqwest::blocking::Client,
    args: &Args,
    key: &str,
    value_type: &str,
) -> Result<(), String> {
    let body = build_column_json(value_type)?;
    let resp = client
        .put(format!("{}/{key}", columns_url(args)))
        .basic_auth(&args.user, Some(&args.password))
        .json(&body)
        .send()
        .map_err(|e| format!("request failed: {e}"))?;
    check_success(resp)
}

fn run_remove_column(
    client: &reqwest::blocking::Client,
    args: &Args,
    key: &str,
) -> Result<(), String> {
    let resp = client
        .delete(format!("{}/{key}", columns_url(args)))
        .basic_auth(&args.user, Some(&args.password))
        .send()
        .map_err(|e| format!("request failed: {e}"))?;
    check_success(resp)
}

/// The body of `PUT /rest/db/{db}/columns/{key}` (kblockdbserver's
/// `AddColumnBody`), validating the type name up front so a typo is a
/// local error rather than a round trip ending in a 400.
fn build_column_json(value_type: &str) -> Result<Json, String> {
    match value_type {
        "str" | "f64" | "i64" | "bool" => Ok(json!({"type": value_type})),
        other => Err(format!(
            "unknown type '{other}' -- expected one of: str, f64, i64, bool"
        )),
    }
}

fn run_databases(client: &reqwest::blocking::Client, args: &Args) -> Result<(), String> {
    let resp = client
        .get(databases_url(args))
        .basic_auth(&args.user, Some(&args.password))
        .send()
        .map_err(|e| format!("request failed: {e}"))?;
    let status = resp.status();
    let body: Json = resp
        .json()
        .map_err(|e| format!("couldn't parse the server's response: {e}"))?;
    if !status.is_success() {
        return Err(server_error_message(status, &body));
    }
    print_databases_response(&body)
}

/// Renders a `GET /rest/databases` response (kblockdbserver's
/// `DatabasesResponse`) as one name per line, then a count -- same shape as
/// `columns`'s listing, so the two read alike.
fn print_databases_response(body: &Json) -> Result<(), String> {
    let databases = body
        .get("databases")
        .and_then(Json::as_array)
        .ok_or("response is missing 'databases'")?;
    for database in databases {
        let name = database.as_str().ok_or("database entry is not a string")?;
        println!("{name}");
    }
    println!("{} database(s)", databases.len());
    Ok(())
}

/// The body of `PUT /rest/databases/{name}` (kblockdbserver's
/// `CreateDatabaseBody`): every field omitted that wasn't given on the
/// command line, so the server falls back to its own configured defaults
/// for whichever ones are missing.
fn build_create_database_json(
    axes: Option<usize>,
    world_dim: Option<u32>,
    chunk_size: Option<u32>,
) -> Json {
    let mut body = json!({});
    if let Some(axes) = axes {
        body["axes"] = json!(axes);
    }
    if let Some(world_dim) = world_dim {
        body["world_dim"] = json!(world_dim);
    }
    if let Some(chunk_size) = chunk_size {
        body["chunk_size"] = json!(chunk_size);
    }
    body
}

fn run_create_database(
    client: &reqwest::blocking::Client,
    args: &Args,
    name: &str,
    axes: Option<usize>,
    world_dim: Option<u32>,
    chunk_size: Option<u32>,
) -> Result<(), String> {
    let resp = client
        .put(format!("{}/{name}", databases_url(args)))
        .basic_auth(&args.user, Some(&args.password))
        .json(&build_create_database_json(axes, world_dim, chunk_size))
        .send()
        .map_err(|e| format!("request failed: {e}"))?;
    check_success(resp)
}

fn run_remove_database(
    client: &reqwest::blocking::Client,
    args: &Args,
    name: &str,
) -> Result<(), String> {
    let resp = client
        .delete(format!("{}/{name}", databases_url(args)))
        .basic_auth(&args.user, Some(&args.password))
        .send()
        .map_err(|e| format!("request failed: {e}"))?;
    check_success(resp)
}

fn run_query(client: &reqwest::blocking::Client, args: &Args, query: &str) -> Result<(), String> {
    let resp = client
        .post(format!(
            "{}/rest/db/{}/query",
            args.url.trim_end_matches('/'),
            db_name(args)
        ))
        .basic_auth(&args.user, Some(&args.password))
        .json(&json!({"query": query}))
        .send()
        .map_err(|e| format!("request failed: {e}"))?;
    let status = resp.status();
    let body: Json = resp
        .json()
        .map_err(|e| format!("couldn't parse the server's response: {e}"))?;
    if !status.is_success() {
        return Err(server_error_message(status, &body));
    }
    print_query_response(&body)
}

/// Renders a `/rest/query` response (kblockdbserver's `QueryResponse`),
/// which is shaped one way for a plain `SELECT` (`rows`), another for a
/// `SELECT count(*)`/`sum(...)`/... (`aggregates`), a third for
/// `SET`/`UPDATE`/`DELETE` (`affected_cells`), and an empty object `{}` for
/// `CREATE INDEX`/`DROP INDEX`/`REBUILD INDEX` (none of `rows`/
/// `aggregates`/`affected_cells` apply to a schema-level operation on a
/// whole column rather than a set of cells).
fn print_query_response(body: &Json) -> Result<(), String> {
    if let Some(rows) = body.get("rows").and_then(Json::as_array) {
        for row in rows {
            println!("{}", describe_query_row(row)?);
        }
        println!("{} row(s)", rows.len());
        Ok(())
    } else if let Some(aggregates) = body.get("aggregates").and_then(Json::as_array) {
        for result in aggregates {
            println!("{}", describe_aggregate_result(result)?);
        }
        Ok(())
    } else if let Some(affected) = body.get("affected_cells").and_then(Json::as_u64) {
        println!("{affected} cell(s) affected");
        Ok(())
    } else if body.as_object().is_some_and(|o| o.is_empty()) {
        println!("ok");
        Ok(())
    } else {
        Err("unrecognized query response shape".to_string())
    }
}

/// Renders one `AggregateResponse` as `label = value` -- e.g.
/// `count(*) = 5` or `mean(density) = null` when nothing numeric matched.
fn describe_aggregate_result(result: &Json) -> Result<String, String> {
    let label = result["label"]
        .as_str()
        .ok_or("aggregate result is missing 'label'")?;
    let value = match &result["value"] {
        Json::Null => "null".to_string(),
        other => other.to_string(),
    };
    Ok(format!("{label} = {value}"))
}

/// Renders one `QueryResponse` row as `(c0,c1,...) key=value (type), ...`.
fn describe_query_row(row: &Json) -> Result<String, String> {
    let coord = row["coord"]
        .as_array()
        .ok_or("query row is missing 'coord'")?
        .iter()
        .map(Json::to_string)
        .collect::<Vec<_>>()
        .join(",");
    let values = row["values"]
        .as_array()
        .ok_or("query row is missing 'values'")?;
    let mut parts = Vec::with_capacity(values.len());
    for kv in values {
        let key = kv["key"]
            .as_str()
            .ok_or("query row entry is missing 'key'")?;
        let (value_type, rendered) = describe_value(&kv["value"])?;
        // Each `values` entry carries the same created_at_ms/modified_at_ms/
        // version fields as a `get` response, as siblings of "key"/"value"
        // rather than nested under it -- describe_meta reads them straight
        // off `kv` the same way it reads them off a `get` response body.
        let meta = describe_meta(kv)?;
        parts.push(format!("{key}={rendered} ({value_type}) {meta}"));
    }
    Ok(format!("({coord}) {}", parts.join(", ")))
}

fn check_success(resp: reqwest::blocking::Response) -> Result<(), String> {
    let status = resp.status();
    if status.is_success() {
        return Ok(());
    }
    let body: Json = resp.json().unwrap_or(Json::Null);
    Err(server_error_message(status, &body))
}

/// Builds kblockdbserver's tagged-value wire format (`{"type": ..., "value":
/// ...}` -- see kblockdbserver's `value_json.rs`) from a CLI-friendly
/// `<type> <value>` pair, parsing `value` according to `value_type`.
fn build_value_json(value_type: &str, value: &str) -> Result<Json, String> {
    match value_type {
        "str" => Ok(json!({"type": "str", "value": value})),
        "f64" => value
            .parse::<f64>()
            .map(|v| json!({"type": "f64", "value": v}))
            .map_err(|_| format!("'{value}' is not a valid f64")),
        "i64" => value
            .parse::<i64>()
            .map(|v| json!({"type": "i64", "value": v}))
            .map_err(|_| format!("'{value}' is not a valid i64")),
        "bool" => value
            .parse::<bool>()
            .map(|v| json!({"type": "bool", "value": v}))
            .map_err(|_| format!("'{value}' is not a valid bool (expected 'true' or 'false')")),
        other => Err(format!(
            "unknown type '{other}' -- expected one of: str, f64, i64, bool"
        )),
    }
}

/// The inverse of `build_value_json`: turns a tagged value from a response
/// body back into a `(type, rendered value)` pair for `get` to print.
fn describe_value(value: &Json) -> Result<(String, String), String> {
    let value_type = value["type"]
        .as_str()
        .ok_or("response value is missing its 'type'")?
        .to_string();
    let rendered = match value_type.as_str() {
        "str" => value["value"]
            .as_str()
            .ok_or("expected 'value' to be a string")?
            .to_string(),
        _ => value["value"].to_string(),
    };
    Ok((value_type, rendered))
}

/// Renders a `get` response's `created_at_ms`/`modified_at_ms`/`version`
/// fields (see kblockdbserver's `CellResponse`) as one
/// `(created=... modified=... version=...)` suffix for `get` to print
/// alongside the value.
fn describe_meta(body: &Json) -> Result<String, String> {
    let created_at_ms = body["created_at_ms"]
        .as_u64()
        .ok_or("response is missing 'created_at_ms'")?;
    let modified_at_ms = body["modified_at_ms"]
        .as_u64()
        .ok_or("response is missing 'modified_at_ms'")?;
    let version = body["version"]
        .as_u64()
        .ok_or("response is missing 'version'")?;
    Ok(format!(
        "(created={created_at_ms} modified={modified_at_ms} version={version})"
    ))
}

fn server_error_message(status: reqwest::StatusCode, body: &Json) -> String {
    let detail = body
        .get("error")
        .and_then(Json::as_str)
        .unwrap_or("(no error message)");
    format!("server returned {status}: {detail}")
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn build_value_json_encodes_each_type() {
        assert_eq!(
            build_value_json("str", "stone").unwrap(),
            json!({"type": "str", "value": "stone"})
        );
        assert_eq!(
            build_value_json("f64", "2.6").unwrap(),
            json!({"type": "f64", "value": 2.6})
        );
        assert_eq!(
            build_value_json("i64", "7").unwrap(),
            json!({"type": "i64", "value": 7})
        );
        assert_eq!(
            build_value_json("bool", "true").unwrap(),
            json!({"type": "bool", "value": true})
        );
        assert_eq!(
            build_value_json("bool", "false").unwrap(),
            json!({"type": "bool", "value": false})
        );
    }

    #[test]
    fn build_value_json_rejects_an_unparseable_number() {
        assert!(build_value_json("i64", "not-a-number").is_err());
        assert!(build_value_json("f64", "not-a-number").is_err());
    }

    #[test]
    fn build_value_json_rejects_an_unparseable_bool() {
        let err = build_value_json("bool", "yes").unwrap_err();
        assert!(err.contains("yes"));
    }

    #[test]
    fn build_value_json_rejects_an_unknown_type() {
        let err = build_value_json("complex", "true").unwrap_err();
        assert!(err.contains("complex"));
    }

    #[test]
    fn describe_value_round_trips_build_value_json() {
        for (value_type, value) in [
            ("str", "stone"),
            ("f64", "2.6"),
            ("i64", "7"),
            ("bool", "true"),
            ("bool", "false"),
        ] {
            let built = build_value_json(value_type, value).unwrap();
            let (described_type, rendered) = describe_value(&built).unwrap();
            assert_eq!(described_type, value_type);
            assert_eq!(rendered, value);
        }
    }

    #[test]
    fn describe_value_rejects_a_value_with_no_type() {
        assert!(describe_value(&json!({"value": "stone"})).is_err());
    }

    #[test]
    fn describe_meta_renders_all_three_fields() {
        let body = json!({
            "value": {"type": "str", "value": "stone"},
            "created_at_ms": 1000,
            "modified_at_ms": 2000,
            "version": 1,
        });
        assert_eq!(
            describe_meta(&body).unwrap(),
            "(created=1000 modified=2000 version=1)"
        );
    }

    #[test]
    fn describe_meta_rejects_a_response_missing_any_field() {
        assert!(describe_meta(&json!({"modified_at_ms": 1, "version": 0})).is_err());
        assert!(describe_meta(&json!({"created_at_ms": 1, "version": 0})).is_err());
        assert!(describe_meta(&json!({"created_at_ms": 1, "modified_at_ms": 1})).is_err());
    }

    #[test]
    fn server_error_message_includes_the_status_and_error_detail() {
        let msg = server_error_message(
            reqwest::StatusCode::NOT_FOUND,
            &json!({"error": "no value set"}),
        );
        assert!(msg.contains("404"));
        assert!(msg.contains("no value set"));
    }

    #[test]
    fn server_error_message_tolerates_a_body_with_no_error_field() {
        let msg = server_error_message(reqwest::StatusCode::INTERNAL_SERVER_ERROR, &Json::Null);
        assert!(msg.contains("500"));
    }

    #[test]
    fn describe_query_row_renders_coord_values_and_metadata() {
        let row = json!({
            "coord": [1, 2, 3],
            "values": [
                {
                    "key": "material", "value": {"type": "str", "value": "stone"},
                    "created_at_ms": 1000, "modified_at_ms": 2000, "version": 1,
                },
                {
                    "key": "hardness", "value": {"type": "f64", "value": 2.6},
                    "created_at_ms": 1000, "modified_at_ms": 1000, "version": 0,
                },
            ],
        });
        assert_eq!(
            describe_query_row(&row).unwrap(),
            "(1,2,3) material=stone (str) (created=1000 modified=2000 version=1), \
             hardness=2.6 (f64) (created=1000 modified=1000 version=0)"
        );
    }

    #[test]
    fn describe_query_row_rejects_a_row_missing_coord_or_values() {
        assert!(describe_query_row(&json!({"values": []})).is_err());
        assert!(describe_query_row(&json!({"coord": [0]})).is_err());
    }

    #[test]
    fn describe_query_row_rejects_a_value_entry_missing_metadata() {
        let row = json!({
            "coord": [0, 0, 0],
            "values": [{"key": "material", "value": {"type": "str", "value": "stone"}}],
        });
        assert!(describe_query_row(&row).is_err());
    }

    #[test]
    fn print_query_response_accepts_a_select_shape() {
        let body = json!({"total_rows": 0, "rows": []});
        assert!(print_query_response(&body).is_ok());
    }

    #[test]
    fn print_query_response_accepts_a_write_shape() {
        let body = json!({"affected_cells": 3});
        assert!(print_query_response(&body).is_ok());
    }

    #[test]
    fn print_query_response_accepts_an_aggregate_shape() {
        let body = json!({"aggregates": [{"label": "count(*)", "value": 5.0}]});
        assert!(print_query_response(&body).is_ok());
    }

    #[test]
    fn print_query_response_accepts_an_index_shape() {
        // CREATE INDEX/DROP INDEX/REBUILD INDEX return an empty object --
        // none of rows/aggregates/affected_cells apply to a schema-level
        // operation on a whole column.
        let body = json!({});
        assert!(print_query_response(&body).is_ok());
    }

    #[test]
    fn print_query_response_rejects_an_unrecognized_shape() {
        assert!(print_query_response(&json!({"something_else": 1})).is_err());
    }

    #[test]
    fn describe_aggregate_result_renders_label_equals_value() {
        assert_eq!(
            describe_aggregate_result(&json!({"label": "count(*)", "value": 5.0})).unwrap(),
            "count(*) = 5.0"
        );
    }

    #[test]
    fn describe_aggregate_result_renders_null_for_a_missing_value() {
        assert_eq!(
            describe_aggregate_result(&json!({"label": "mean(density)", "value": null})).unwrap(),
            "mean(density) = null"
        );
    }

    #[test]
    fn describe_aggregate_result_rejects_a_missing_label() {
        assert!(describe_aggregate_result(&json!({"value": 1.0})).is_err());
    }

    #[test]
    fn build_column_json_accepts_every_value_type() {
        for value_type in ["str", "f64", "i64", "bool"] {
            assert_eq!(
                build_column_json(value_type).unwrap(),
                json!({ "type": value_type })
            );
        }
    }

    #[test]
    fn build_column_json_rejects_an_unknown_type() {
        let err = build_column_json("complex").unwrap_err();
        assert!(err.contains("complex"));
    }

    #[test]
    fn print_columns_response_accepts_a_list_of_columns() {
        let body = json!({"columns": [
            {"key": "hardness", "type": "f64"},
            {"key": "material", "type": "str"},
        ]});
        assert!(print_columns_response(&body).is_ok());
    }

    #[test]
    fn print_columns_response_accepts_an_empty_schema() {
        assert!(print_columns_response(&json!({"columns": []})).is_ok());
    }

    #[test]
    fn print_columns_response_rejects_an_unrecognized_shape() {
        assert!(print_columns_response(&json!({})).is_err());
        assert!(print_columns_response(&json!({"columns": [{"key": "material"}]})).is_err());
        assert!(print_columns_response(&json!({"columns": [{"type": "str"}]})).is_err());
    }

    // --- Multi-database support ---

    fn args_with(db: Option<&str>, command: Command) -> Args {
        Args {
            url: "http://localhost:8080".to_string(),
            user: "admin".to_string(),
            password: "pw".to_string(),
            db: db.map(str::to_string),
            command,
        }
    }

    #[test]
    fn cell_url_includes_the_database_segment() {
        let args = args_with(Some("mydb"), Command::Columns);
        assert_eq!(
            cell_url(&args, "1,2,3", "material"),
            "http://localhost:8080/rest/db/mydb/cells/1,2,3/material"
        );
    }

    #[test]
    fn columns_url_includes_the_database_segment() {
        let args = args_with(Some("mydb"), Command::Columns);
        assert_eq!(
            columns_url(&args),
            "http://localhost:8080/rest/db/mydb/columns"
        );
    }

    #[test]
    fn databases_url_has_no_database_segment() {
        let args = args_with(None, Command::Databases);
        assert_eq!(databases_url(&args), "http://localhost:8080/rest/databases");
    }

    #[test]
    #[should_panic(expected = "command_needs_db")]
    fn db_name_panics_if_called_without_a_selected_database() {
        let args = args_with(None, Command::Columns);
        db_name(&args);
    }

    #[test]
    fn command_needs_db_is_true_for_every_data_command() {
        assert!(command_needs_db(&Command::Get {
            coords: "0".into(),
            key: "k".into()
        }));
        assert!(command_needs_db(&Command::Set {
            coords: "0".into(),
            key: "k".into(),
            value_type: "str".into(),
            value: "v".into()
        }));
        assert!(command_needs_db(&Command::Remove {
            coords: "0".into(),
            key: "k".into()
        }));
        assert!(command_needs_db(&Command::Query {
            query: "SELECT *".into()
        }));
        assert!(command_needs_db(&Command::Columns));
        assert!(command_needs_db(&Command::AddColumn {
            key: "k".into(),
            value_type: "str".into()
        }));
        assert!(command_needs_db(&Command::RemoveColumn { key: "k".into() }));
    }

    #[test]
    fn command_needs_db_is_false_for_every_database_management_command() {
        assert!(!command_needs_db(&Command::Databases));
        assert!(!command_needs_db(&Command::CreateDatabase {
            name: "x".into(),
            axes: None,
            world_dim: None,
            chunk_size: None,
        }));
        assert!(!command_needs_db(&Command::RemoveDatabase {
            name: "x".into()
        }));
    }

    #[test]
    fn build_create_database_json_with_no_overrides_is_an_empty_object() {
        assert_eq!(build_create_database_json(None, None, None), json!({}));
    }

    #[test]
    fn build_create_database_json_includes_only_the_given_overrides() {
        assert_eq!(
            build_create_database_json(Some(4), None, Some(16)),
            json!({"axes": 4, "chunk_size": 16})
        );
        assert_eq!(
            build_create_database_json(Some(3), Some(10_000), Some(32)),
            json!({"axes": 3, "world_dim": 10_000, "chunk_size": 32})
        );
    }

    #[test]
    fn print_databases_response_accepts_a_list_of_names() {
        let body = json!({"databases": ["a", "b"]});
        assert!(print_databases_response(&body).is_ok());
    }

    #[test]
    fn print_databases_response_accepts_an_empty_list() {
        assert!(print_databases_response(&json!({"databases": []})).is_ok());
    }

    #[test]
    fn print_databases_response_rejects_an_unrecognized_shape() {
        assert!(print_databases_response(&json!({})).is_err());
        assert!(print_databases_response(&json!({"databases": [1]})).is_err());
    }
}
