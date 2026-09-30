//! `kblockdbcli` -- a command-line client for kblockdbserver's REST API: get, set,
//! and remove a single cell's value for a key. A thin wrapper over HTTP
//! Basic Auth and `/rest/cells/{coords}/{key}`, nothing more (see kblockdbserver's
//! README section for the fuller API this could grow into covering, e.g.
//! `/rest/regions`).

#[cfg(test)]
mod tests;

use serde_json::{json, Value as Json};
use std::process::ExitCode;

struct Args {
    url: String,
    user: String,
    password: String,
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
    let mut command_name: Option<String> = None;
    let mut positional: Vec<String> = Vec::new();

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--url" => url = expect_value(&mut args, "--url"),
            "--user" => user = expect_value(&mut args, "--user"),
            "--password" => password = Some(expect_value(&mut args, "--password")),
            "-h" | "--help" => {
                print_help();
                std::process::exit(0);
            }
            other if command_name.is_none() => command_name = Some(other.to_string()),
            other => positional.push(other.to_string()),
        }
    }

    let command_name = command_name.unwrap_or_else(|| {
        eprintln!("missing command -- expected one of: get, set, remove\n");
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
        other => {
            eprintln!("unknown command '{other}' -- expected one of: get, set, remove\n");
            print_help();
            std::process::exit(1);
        }
    };

    let password = password.unwrap_or_else(|| {
        eprintln!("--password (or the KBLOCKDBCLI_PASSWORD env var) is required\n");
        print_help();
        std::process::exit(1);
    });

    Args {
        url,
        user,
        password,
        command,
    }
}

fn expect_value(args: &mut impl Iterator<Item = String>, flag: &str) -> String {
    args.next().unwrap_or_else(|| {
        eprintln!("{flag} requires a value");
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
         set <coords> <key> <type> <value>   Set a cell's value (type: str, f64, or i64)\n    \
         remove <coords> <key>               Clear a cell's value\n\n\
         OPTIONS:\n    \
         --url <url>        kblockdbserver base URL (default: http://127.0.0.1:8080)\n    \
         --user <name>      Username (default: admin)\n    \
         --password <pw>    Password (or set the KBLOCKDBCLI_PASSWORD env var, so it\n                        \
         doesn't end up in shell history)\n    \
         -h, --help         Print this help\n\n\
         <coords> is a comma-separated coordinate, one u32 per axis (e.g. 1,2,3),\n\
         matching however many axes the target world was created with.\n\n\
         EXAMPLES:\n    \
         kblockdbcli --password change-me set 1,2,3 material str stone\n    \
         kblockdbcli --password change-me get 1,2,3 material\n    \
         kblockdbcli --password change-me remove 1,2,3 material"
    );
}

fn cell_url(args: &Args, coords: &str, key: &str) -> String {
    format!(
        "{}/rest/cells/{coords}/{key}",
        args.url.trim_end_matches('/')
    )
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
        other => Err(format!(
            "unknown type '{other}' -- expected one of: str, f64, i64"
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
    }

    #[test]
    fn build_value_json_rejects_an_unparseable_number() {
        assert!(build_value_json("i64", "not-a-number").is_err());
        assert!(build_value_json("f64", "not-a-number").is_err());
    }

    #[test]
    fn build_value_json_rejects_an_unknown_type() {
        let err = build_value_json("bool", "true").unwrap_err();
        assert!(err.contains("bool"));
    }

    #[test]
    fn describe_value_round_trips_build_value_json() {
        for (value_type, value) in [("str", "stone"), ("f64", "2.6"), ("i64", "7")] {
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
}
