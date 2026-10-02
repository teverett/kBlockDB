mod binary_client;
mod client;
mod report;
mod scenarios;
mod server;
mod stats;
#[cfg(test)]
mod tests;

use binary_client::BinaryClient;
use client::Client;
use server::{default_kblockdbserver_bin, ManagedServer};
use std::path::PathBuf;

struct Args {
    url: Option<String>,
    kblockdbserver_bin: Option<PathBuf>,
    port: u16,
    /// Where to find an *existing* server's binary protocol -- only
    /// meaningful with `--url`. The spawned-instance equivalent is
    /// `binary_port`: a bind port and a connect address aren't the same
    /// thing, and one flag serving both made it ambiguous which was
    /// being given.
    binary_addr: Option<String>,
    binary_port: Option<u16>,
    concurrency: Vec<usize>,
    ops_per_client: usize,
    cells: usize,
    region_edges: Vec<u32>,
    region_reps: usize,
    scenarios: Vec<String>,
    json: bool,
    keep_data_dir: bool,
    user: String,
    password: Option<String>,
    /// The one database this run targets -- created (or reused, if it
    /// already exists) before any scenario runs, since kblockdbserver never
    /// creates a database implicitly. Required: there's deliberately no
    /// default, matching the server's "every request must name a database"
    /// model.
    db: Option<String>,
}

/// The shape this tool creates its target database with, when it doesn't
/// already exist -- mirrors `kblockdblib::AXES`/`WORLD_DIM`/
/// `DEFAULT_CHUNK_DIM`, duplicated as literals (not a dependency) since
/// kblockdbperf deliberately treats kblockdbserver as a black box over
/// HTTP/the binary protocol, never linking `kblockdblib` directly. Also
/// used directly as the `axes`/`world_dim` scenarios build coordinates
/// against, since a database's shape is no longer reported back by
/// `/rest/health` (a server can manage many databases, each its own
/// shape) -- this tool already knows it, because it's the one that chose
/// it.
const TARGET_AXES: usize = 3;
const TARGET_WORLD_DIM: u32 = 10_000;
const TARGET_CHUNK_SIZE: u32 = 32;

impl Default for Args {
    fn default() -> Args {
        Args {
            url: None,
            kblockdbserver_bin: None,
            port: 18_080,
            binary_port: None,
            binary_addr: None,
            concurrency: vec![1, 8, 32, 128],
            ops_per_client: 50,
            cells: 500,
            region_edges: vec![4, 8, 16, 32],
            region_reps: 5,
            scenarios: Vec::new(),
            json: false,
            keep_data_dir: false,
            user: "admin".to_string(),
            password: None,
            db: None,
        }
    }
}

const SCENARIO_NAMES: &[&str] = &[
    "set_cell",
    "get_cell",
    "remove_cell",
    "region",
    "concurrency_scan",
    "contended_cell",
    "binary_set_cell",
    "binary_get_cell",
    "binary_remove_cell",
];

fn parse_args() -> Args {
    let mut args = Args::default();
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--url" => args.url = Some(expect_value(&mut it, "--url")),
            "--kblockdbserver-bin" => {
                args.kblockdbserver_bin =
                    Some(PathBuf::from(expect_value(&mut it, "--kblockdbserver-bin")))
            }
            "--port" => args.port = expect_parsed(&mut it, "--port"),
            "--binary-addr" => args.binary_addr = Some(expect_value(&mut it, "--binary-addr")),
            "--binary-port" => args.binary_port = Some(expect_parsed(&mut it, "--binary-port")),
            "--concurrency" => args.concurrency = expect_list(&mut it, "--concurrency"),
            "--ops-per-client" => args.ops_per_client = expect_parsed(&mut it, "--ops-per-client"),
            "--cells" => args.cells = expect_parsed(&mut it, "--cells"),
            "--region-edges" => args.region_edges = expect_list(&mut it, "--region-edges"),
            "--region-reps" => args.region_reps = expect_parsed(&mut it, "--region-reps"),
            "--scenario" => {
                let name = expect_value(&mut it, "--scenario");
                if !SCENARIO_NAMES.contains(&name.as_str()) {
                    eprintln!(
                        "unknown scenario '{name}' -- valid names: {}",
                        SCENARIO_NAMES.join(", ")
                    );
                    std::process::exit(1);
                }
                args.scenarios.push(name);
            }
            "--json" => args.json = true,
            "--keep-data-dir" => args.keep_data_dir = true,
            "--user" => args.user = expect_value(&mut it, "--user"),
            "--password" => args.password = Some(expect_value(&mut it, "--password")),
            "--db" => args.db = Some(expect_value(&mut it, "--db")),
            "-h" | "--help" => {
                print_help();
                std::process::exit(0);
            }
            other => {
                eprintln!("unknown argument: {other}\n");
                print_help();
                std::process::exit(1);
            }
        }
    }
    if args.db.is_none() {
        eprintln!("--db is required -- every kblockdbserver request must name a database\n");
        print_help();
        std::process::exit(1);
    }
    args
}

fn expect_value(args: &mut impl Iterator<Item = String>, flag: &str) -> String {
    args.next().unwrap_or_else(|| {
        eprintln!("{flag} requires a value");
        std::process::exit(1);
    })
}

fn expect_parsed<T: std::str::FromStr>(args: &mut impl Iterator<Item = String>, flag: &str) -> T {
    expect_value(args, flag).parse().unwrap_or_else(|_| {
        eprintln!("{flag}: not a valid number");
        std::process::exit(1);
    })
}

fn expect_list<T>(args: &mut impl Iterator<Item = String>, flag: &str) -> Vec<T>
where
    T: std::str::FromStr,
{
    expect_value(args, flag)
        .split(',')
        .map(|s| {
            s.trim().parse().unwrap_or_else(|_| {
                eprintln!("{flag}: '{s}' is not a valid number in a comma-separated list");
                std::process::exit(1);
            })
        })
        .collect()
}

fn print_help() {
    let default = Args::default();
    println!(
        "kblockdbperf -- a performance test suite that drives a real kblockdbserver over HTTP\n\
         and (optionally) its binary protocol\n\n\
         USAGE:\n    kblockdbperf [OPTIONS]\n\n\
         By default, spawns its own kblockdbserver instance against a fresh temp data\n\
         dir and tears it down when done. Pass --url to target an already-running\n\
         instance instead.\n\n\
         OPTIONS:\n    \
         --url <url>              Target an existing kblockdbserver instead of spawning one\n    \
         --kblockdbserver-bin <path>   kblockdbserver binary to spawn (default: next to kblockdbperf's\n                              \
         own binary)\n    \
         --port <n>               Port for the spawned instance (default: {})\n    \
         --binary-addr <host:port>\n                              \
         With --url, the target's binary protocol address -- binary_*\n                              \
         scenarios are skipped if not given. Ignored when spawning an\n                              \
         instance; use --binary-port for that.\n    \
         --binary-port <n>        Binary protocol port for the spawned instance (default:\n                              \
         <port + 1>) -- a spawned instance always has one. Ignored\n                              \
         with --url.\n    \
         --concurrency <list>     Comma-separated concurrency levels (default: {})\n    \
         --ops-per-client <n>     Ops each concurrent client runs per level (default: {})\n    \
         --cells <n>              Cells for sequential single-cell scenarios (default: {})\n    \
         --region-edges <list>    Comma-separated region edge lengths (default: {})\n    \
         --region-reps <n>        Repetitions per region size (default: {})\n    \
         --scenario <name>        Run only this scenario (repeatable; default: all).\n                              \
         One of: {}\n    \
         --json                   Print results as JSON instead of a table\n    \
         --keep-data-dir          Don't delete the spawned temp data dir on exit\n    \
         --user <name>            Username for kblockdbserver's REST API (default: admin;\n                              \
         only meaningful with --url -- a spawned instance is always\n                              \
         driven as admin)\n    \
         --password <pw>          Password for --user. Required with --url (kblockdbserver\n                              \
         requires login). When spawning an instance, defaults to a random\n                              \
         per-run password used for both the spawned config and the client\n    \
         --db <name>              Required. The database this run targets -- created\n                              \
         (shaped axes={}, world_dim={}, chunk_size={}) if it doesn't\n                              \
         already exist, reused otherwise. Same database name for\n                              \
         repeat runs reuses whatever data they left behind.\n    \
         -h, --help               Print this help",
        default.port,
        fmt_list(&default.concurrency),
        default.ops_per_client,
        default.cells,
        fmt_list(&default.region_edges),
        default.region_reps,
        SCENARIO_NAMES.join(", "),
        TARGET_AXES,
        TARGET_WORLD_DIM,
        TARGET_CHUNK_SIZE,
    );
}

fn fmt_list<T: std::fmt::Display>(xs: &[T]) -> String {
    xs.iter().map(T::to_string).collect::<Vec<_>>().join(",")
}

/// Removes the spawned data dir on drop, unless told to keep it -- mirrors
/// `ManagedServer` (which this always outlives, since it's created first
/// and thus dropped last) so a run never litters the temp dir with data
/// directories, but `--keep-data-dir` still lets you inspect one after a
/// run to debug something odd.
struct TempDataDir {
    path: PathBuf,
    keep: bool,
}

impl Drop for TempDataDir {
    fn drop(&mut self) {
        if !self.keep {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }
}

#[tokio::main]
async fn main() {
    let args = parse_args();

    // Order matters: `_server` must drop (killing the process) before
    // `_data_dir` drops (deleting its data), so declare it first -- Rust
    // drops locals in reverse declaration order.
    let _data_dir: Option<TempDataDir>;
    let _server: Option<ManagedServer>;
    let client: Client;
    // The address to try a `BinaryClient` against, if any -- `None` means
    // "don't bother", not "connection failed" (that's handled separately
    // below, once we actually try).
    let binary_target_addr: Option<String>;
    let binary_user: String;
    let binary_password: String;
    // Checked by `parse_args` already -- `args.db` is always `Some` here.
    let db = args.db.clone().expect("--db is required");

    if let Some(url) = args.url.clone() {
        let password = args.password.clone().unwrap_or_else(|| {
            eprintln!("--password is required with --url (kblockdbserver requires login)");
            std::process::exit(1);
        });
        _data_dir = None;
        _server = None;
        binary_target_addr = args.binary_addr.clone();
        binary_user = args.user.clone();
        binary_password = password.clone();
        client = Client::new(url, args.user.clone(), password, db.clone());
        println!("targeting existing instance\n");
    } else {
        let bin = args
            .kblockdbserver_bin
            .clone()
            .unwrap_or_else(default_kblockdbserver_bin);
        if !bin.exists() {
            eprintln!(
                "kblockdbserver binary not found at {} -- build it first \
                 (`cargo build --release --workspace`) or pass --kblockdbserver-bin",
                bin.display()
            );
            std::process::exit(1);
        }

        let data_dir = std::env::temp_dir().join(format!(
            "kblockdbperf-{}-{}",
            std::process::id(),
            rand_suffix()
        ));
        std::fs::create_dir_all(&data_dir).unwrap_or_else(|e| {
            eprintln!("failed to create temp data dir {}: {e}", data_dir.display());
            std::process::exit(1);
        });
        println!(
            "spawning kblockdbserver instance against {}",
            data_dir.display()
        );

        // Always the `admin` account here, regardless of `--user`: the
        // config this spawns the instance with only ever creates that one
        // account (see `ManagedServer::spawn`), so `--user` only matters
        // against an already-running server (`--url`).
        let admin_password = args
            .password
            .clone()
            .unwrap_or_else(|| format!("kblockdbperf-{}", rand_suffix()));

        let binary_port = args.binary_port.unwrap_or(args.port + 1);
        let server = ManagedServer::spawn(
            &bin,
            &data_dir,
            args.port,
            Some(binary_port),
            &admin_password,
        )
        .await;
        binary_target_addr = server.binary_addr.clone();
        binary_user = "admin".to_string();
        binary_password = admin_password.clone();
        client = Client::new(server.url.clone(), "admin", admin_password, db.clone());
        _server = Some(server);
        _data_dir = Some(TempDataDir {
            path: data_dir,
            keep: args.keep_data_dir,
        });
    }

    let primary = &client;
    primary
        .ensure_database(TARGET_AXES, TARGET_WORLD_DIM, TARGET_CHUNK_SIZE)
        .await
        .unwrap_or_else(|e| {
            eprintln!("failed to create/confirm database '{db}': {e}");
            std::process::exit(1);
        });
    let axes = TARGET_AXES;
    let world_dim = TARGET_WORLD_DIM;
    println!("target database '{db}': axes={axes}, world_dim={world_dim}");
    if let Some(health) = primary.health().await {
        println!(
            "target server: hostname={}, {} database(s) total\n",
            health.hostname, health.database_count
        );
    } else {
        println!();
    }

    let want_binary = SCENARIO_NAMES
        .iter()
        .filter(|n| n.starts_with("binary_"))
        .any(|n| args.scenarios.is_empty() || args.scenarios.iter().any(|s| s == n));
    let mut binary_client = if want_binary {
        match &binary_target_addr {
            Some(addr) => {
                match BinaryClient::connect(addr, &binary_user, &binary_password, &db).await {
                    Some((client, bh)) => {
                        println!(
                            "binary protocol reachable at {addr} (axes={}, world_dim={})\n",
                            bh.axes, bh.world_dim
                        );
                        Some(client)
                    }
                    None => {
                        println!(
                            "note: couldn't reach the binary protocol at {addr} -- \
                         skipping binary_* scenarios\n"
                        );
                        None
                    }
                }
            }
            None => {
                if !args.scenarios.is_empty() {
                    // Explicitly asked for a binary_* scenario with nothing
                    // to run it against -- worth failing loudly rather than
                    // silently producing an incomplete report.
                    eprintln!(
                        "a binary_* scenario was requested, but there's no binary protocol \
                         target -- pass --binary-addr alongside --url (when spawning an \
                         instance one always exists; --binary-port overrides its port)"
                    );
                    std::process::exit(1);
                }
                None
            }
        }
    } else {
        None
    };

    let mut results = Vec::new();
    let run_all = args.scenarios.is_empty();
    let want = |name: &str| run_all || args.scenarios.iter().any(|s| s == name);

    if want("set_cell") {
        results.push(scenarios::set_cell(primary, axes, world_dim, args.cells).await);
    }
    if want("get_cell") {
        results.push(scenarios::get_cell(primary, axes, world_dim, args.cells).await);
    }
    if want("remove_cell") {
        results.push(scenarios::remove_cell(primary, axes, world_dim, args.cells).await);
    }
    if want("region") {
        results.extend(
            scenarios::region_sweep(primary, axes, &args.region_edges, args.region_reps).await,
        );
    }
    if want("concurrency_scan") {
        results.extend(
            scenarios::concurrency_scan(
                primary,
                axes,
                world_dim,
                &args.concurrency,
                args.ops_per_client,
            )
            .await,
        );
    }
    if want("contended_cell") {
        results.extend(
            scenarios::contended_cell(primary, axes, &args.concurrency, args.ops_per_client).await,
        );
    }
    if let Some(bc) = binary_client.as_mut() {
        if want("binary_set_cell") {
            results.push(scenarios::binary_set_cell(bc, axes, world_dim, args.cells).await);
        }
        if want("binary_get_cell") {
            results.push(scenarios::binary_get_cell(bc, axes, world_dim, args.cells).await);
        }
        if want("binary_remove_cell") {
            results.push(scenarios::binary_remove_cell(bc, axes, world_dim, args.cells).await);
        }
    }
    println!();
    if args.json {
        report::print_json(&results);
    } else {
        report::print_table(&results);
    }
}

/// A short, cheap-to-generate suffix so two `kblockdbperf` runs started in the
/// same process-id-reuse window (rare, but seen on fast CI loops) don't
/// collide on the same temp data dir.
fn rand_suffix() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}
