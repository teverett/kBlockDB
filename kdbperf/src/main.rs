mod client;
mod report;
mod scenarios;
mod server;
mod stats;
#[cfg(test)]
mod tests;

use client::Client;
use server::{default_kdbserver_bin, ManagedServer};
use std::path::PathBuf;

struct Args {
    urls: Vec<String>,
    kdbserver_bin: Option<PathBuf>,
    instances: usize,
    base_port: u16,
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
}

impl Default for Args {
    fn default() -> Args {
        Args {
            urls: Vec::new(),
            kdbserver_bin: None,
            instances: 2,
            base_port: 18_080,
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
    "multi_instance",
];

fn parse_args() -> Args {
    let mut args = Args::default();
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--url" => args.urls.push(expect_value(&mut it, "--url")),
            "--kdbserver-bin" => {
                args.kdbserver_bin = Some(PathBuf::from(expect_value(&mut it, "--kdbserver-bin")))
            }
            "--instances" => args.instances = expect_parsed(&mut it, "--instances"),
            "--base-port" => args.base_port = expect_parsed(&mut it, "--base-port"),
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
        "kdbperf -- a performance test suite that drives a real kdbserver over HTTP\n\n\
         USAGE:\n    kdbperf [OPTIONS]\n\n\
         By default, spawns its own kdbserver instance(s) against a fresh temp data\n\
         dir and tears them down when done. Pass --url to target an already-running\n\
         instance instead (repeat --url for a multi_instance comparison against\n\
         real, separately-deployed instances).\n\n\
         OPTIONS:\n    \
         --url <url>              Target an existing kdbserver instead of spawning one\n                              \
         (repeatable)\n    \
         --kdbserver-bin <path>   kdbserver binary to spawn (default: next to kdbperf's\n                              \
         own binary)\n    \
         --instances <n>          Instances to spawn, sharing one data dir, for\n                              \
         multi_instance (default: {})\n    \
         --base-port <n>          First port used for spawned instances (default: {})\n    \
         --concurrency <list>     Comma-separated concurrency levels (default: {})\n    \
         --ops-per-client <n>     Ops each concurrent client runs per level (default: {})\n    \
         --cells <n>              Cells for sequential single-cell scenarios (default: {})\n    \
         --region-edges <list>    Comma-separated region edge lengths (default: {})\n    \
         --region-reps <n>        Repetitions per region size (default: {})\n    \
         --scenario <name>        Run only this scenario (repeatable; default: all).\n                              \
         One of: {}\n    \
         --json                   Print results as JSON instead of a table\n    \
         --keep-data-dir          Don't delete the spawned temp data dir on exit\n    \
         --user <name>            Username for kdbserver's REST API (default: admin;\n                              \
         only meaningful with --url -- spawned instances are always\n                              \
         driven as admin)\n    \
         --password <pw>          Password for --user. Required with --url (kdbserver\n                              \
         requires login). When spawning instances, defaults to a random\n                              \
         per-run password used for both the spawned config and the client\n    \
         -h, --help               Print this help",
        default.instances,
        default.base_port,
        fmt_list(&default.concurrency),
        default.ops_per_client,
        default.cells,
        fmt_list(&default.region_edges),
        default.region_reps,
        SCENARIO_NAMES.join(", "),
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

    // Order matters: `_servers` must drop (killing the processes) before
    // `_data_dir` drops (deleting their data), so declare it first --
    // Rust drops locals in reverse declaration order.
    let _data_dir: Option<TempDataDir>;
    let _servers: Vec<ManagedServer>;
    let clients: Vec<Client>;

    if !args.urls.is_empty() {
        let password = args.password.clone().unwrap_or_else(|| {
            eprintln!("--password is required with --url (kdbserver requires login)");
            std::process::exit(1);
        });
        _data_dir = None;
        _servers = Vec::new();
        clients = args
            .urls
            .iter()
            .map(|u| Client::new(u.clone(), args.user.clone(), password.clone()))
            .collect();
        println!("targeting {} existing instance(s)\n", clients.len());
    } else {
        let bin = args
            .kdbserver_bin
            .clone()
            .unwrap_or_else(default_kdbserver_bin);
        if !bin.exists() {
            eprintln!(
                "kdbserver binary not found at {} -- build it first \
                 (`cargo build --release --workspace`) or pass --kdbserver-bin",
                bin.display()
            );
            std::process::exit(1);
        }

        let data_dir =
            std::env::temp_dir().join(format!("kdbperf-{}-{}", std::process::id(), rand_suffix()));
        std::fs::create_dir_all(&data_dir).unwrap_or_else(|e| {
            eprintln!("failed to create temp data dir {}: {e}", data_dir.display());
            std::process::exit(1);
        });
        println!(
            "spawning {} kdbserver instance(s) sharing {}",
            args.instances,
            data_dir.display()
        );

        // Always the `admin` account here, regardless of `--user`: the
        // config this spawns each instance with only ever creates that one
        // account (see `ManagedServer::spawn`), so `--user` only matters
        // against an already-running server (`--url`).
        let admin_password = args
            .password
            .clone()
            .unwrap_or_else(|| format!("kdbperf-{}", rand_suffix()));

        let mut servers = Vec::with_capacity(args.instances);
        for i in 0..args.instances {
            let addr = format!("127.0.0.1:{}", args.base_port + i as u16);
            servers.push(ManagedServer::spawn(&bin, &data_dir, &addr, &admin_password).await);
        }
        clients = servers
            .iter()
            .map(|s| Client::new(s.url.clone(), "admin", admin_password.clone()))
            .collect();
        _servers = servers;
        _data_dir = Some(TempDataDir {
            path: data_dir,
            keep: args.keep_data_dir,
        });
    }

    let primary = &clients[0];
    let health = primary.health().await.unwrap_or_else(|| {
        eprintln!("failed to query /health on the target server");
        std::process::exit(1);
    });
    println!(
        "target world: axes={}, world_dim={}\n",
        health.axes, health.world_dim
    );

    let mut results = Vec::new();
    let run_all = args.scenarios.is_empty();
    let want = |name: &str| run_all || args.scenarios.iter().any(|s| s == name);

    if want("set_cell") {
        results.push(scenarios::set_cell(primary, health.axes, health.world_dim, args.cells).await);
    }
    if want("get_cell") {
        results.push(scenarios::get_cell(primary, health.axes, health.world_dim, args.cells).await);
    }
    if want("remove_cell") {
        results
            .push(scenarios::remove_cell(primary, health.axes, health.world_dim, args.cells).await);
    }
    if want("region") {
        results.extend(
            scenarios::region_sweep(primary, health.axes, &args.region_edges, args.region_reps)
                .await,
        );
    }
    if want("concurrency_scan") {
        results.extend(
            scenarios::concurrency_scan(
                primary,
                health.axes,
                health.world_dim,
                &args.concurrency,
                args.ops_per_client,
            )
            .await,
        );
    }
    if want("contended_cell") {
        results.extend(
            scenarios::contended_cell(primary, health.axes, &args.concurrency, args.ops_per_client)
                .await,
        );
    }
    if want("multi_instance") {
        if clients.len() >= 2 {
            for &level in &args.concurrency {
                results.push(
                    scenarios::multi_instance(
                        &clients[..1],
                        health.axes,
                        health.world_dim,
                        level,
                        args.ops_per_client,
                    )
                    .await,
                );
                results.push(
                    scenarios::multi_instance(
                        &clients,
                        health.axes,
                        health.world_dim,
                        level,
                        args.ops_per_client,
                    )
                    .await,
                );
            }
        } else {
            eprintln!(
                "skipping multi_instance: needs 2+ servers (pass --instances 2 or more, or \
                 two or more --url)"
            );
        }
    }

    println!();
    if args.json {
        report::print_json(&results);
    } else {
        report::print_table(&results);
    }
}

/// A short, cheap-to-generate suffix so two `kdbperf` runs started in the
/// same process-id-reuse window (rare, but seen on fast CI loops) don't
/// collide on the same temp data dir.
fn rand_suffix() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}
