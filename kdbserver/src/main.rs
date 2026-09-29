mod auth;
mod config;
mod coords;
mod error;
mod routes;
mod state;
#[cfg(test)]
mod tests;
mod value_json;

use config::Config;
use state::AppState;
use std::path::PathBuf;

/// Only the flags a caller explicitly passed are `Some` here -- final
/// values are resolved in `main` as `args.field.or(config.field).unwrap_or(default)`,
/// so a CLI flag overrides the config file, which overrides the built-in
/// default. `admin_password`/`users` have no CLI equivalent on purpose
/// (see `config.rs`): they only ever come from `--config`.
struct Args {
    config_path: PathBuf,
    data_dir: Option<String>,
    axes: Option<usize>,
    world_dim: Option<u32>,
    addr: Option<String>,
    max_concurrent_disk_ops: Option<usize>,
}

fn parse_args() -> Args {
    let mut config_path = PathBuf::from("./kdbserver.toml");
    let mut data_dir = None;
    let mut axes = None;
    let mut world_dim = None;
    let mut addr = None;
    let mut max_concurrent_disk_ops = None;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => config_path = PathBuf::from(expect_value(&mut args, "--config")),
            "--data-dir" => data_dir = Some(expect_value(&mut args, "--data-dir")),
            "--axes" => {
                axes = Some(
                    expect_value(&mut args, "--axes")
                        .parse()
                        .unwrap_or_else(|_| {
                            eprintln!("--axes must be a positive integer");
                            std::process::exit(1);
                        }),
                )
            }
            "--world-dim" => {
                world_dim = Some(
                    expect_value(&mut args, "--world-dim")
                        .parse()
                        .unwrap_or_else(|_| {
                            eprintln!("--world-dim must be a positive integer");
                            std::process::exit(1);
                        }),
                )
            }
            "--addr" => addr = Some(expect_value(&mut args, "--addr")),
            "--max-concurrent-disk-ops" => {
                max_concurrent_disk_ops = Some(
                    expect_value(&mut args, "--max-concurrent-disk-ops")
                        .parse()
                        .unwrap_or_else(|_| {
                            eprintln!("--max-concurrent-disk-ops must be a positive integer");
                            std::process::exit(1);
                        }),
                )
            }
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

    Args {
        config_path,
        data_dir,
        axes,
        world_dim,
        addr,
        max_concurrent_disk_ops,
    }
}

fn expect_value(args: &mut impl Iterator<Item = String>, flag: &str) -> String {
    args.next().unwrap_or_else(|| {
        eprintln!("{flag} requires a value");
        std::process::exit(1);
    })
}

fn print_help() {
    println!(
        "kdbserver -- a RESTful HTTP front end for the kdb storage engine\n\n\
         USAGE:\n    kdbserver [OPTIONS]\n\n\
         OPTIONS:\n    \
         --config <path>      Config file (default: ./kdbserver.toml). Required --\n                          \
         holds admin_password and, optionally, [[users]], plus optional\n                          \
         addr/data_dir/axes/world_dim/max_concurrent_disk_ops (each\n                          \
         overridden by the matching CLI flag below, if given). Example:\n                          \
         \n                          \
         addr = \"127.0.0.1:8080\"\n                          \
         data_dir = \"./data\"\n                          \
         admin_password = \"change-me\"\n                          \
         \n                          \
         [[users]]\n                          \
         username = \"alice\"\n                          \
         password = \"alice-password\"\n    \
         --data-dir <path>    World data directory (default: ./data)\n    \
         --axes <n>           Axis count for a brand-new world (default: {})\n    \
         --world-dim <n>      Cells per axis for a brand-new world (default: {})\n    \
         --addr <host:port>   Address to listen on (default: 127.0.0.1:8080)\n    \
         --max-concurrent-disk-ops <n>\n                          \
         Cap on concurrent filesystem operations\n                          \
         (default: {}; see kdb::DEFAULT_MAX_CONCURRENT_DISK_OPS --\n                          \
         the right number depends on your filesystem/storage;\n                          \
         measure it with kdbperf's concurrency_scan)\n    \
         -h, --help           Print this help\n\n\
         --axes/--world-dim only matter the first time a world is created at\n\
         --data-dir; reopening an existing one reads its real shape from its\n\
         world.txt and ignores these flags (World::open does, not create).\n\n\
         Every REST endpoint except /health requires HTTP Basic Auth against\n\
         an account from the config file (admin_password, or a [[users]] entry).\n\
         admin_password/users are config-file-only -- never CLI flags -- so\n\
         credentials don't end up in shell history or `ps` output.",
        kdb::AXES,
        kdb::WORLD_DIM,
        kdb::DEFAULT_MAX_CONCURRENT_DISK_OPS
    );
}

#[tokio::main]
async fn main() {
    let args = parse_args();

    let config = Config::load(&args.config_path).unwrap_or_else(|e| {
        eprintln!(
            "{e}\n\nSee `kdbserver --help` for the config file's format, or point\n\
             --config at a different file."
        );
        std::process::exit(1);
    });

    let data_dir = args
        .data_dir
        .or_else(|| config.data_dir.clone())
        .unwrap_or_else(|| "./data".to_string());
    let axes = args.axes.or(config.axes).unwrap_or(kdb::AXES);
    let world_dim = args
        .world_dim
        .or(config.world_dim)
        .unwrap_or(kdb::WORLD_DIM);
    let addr = args
        .addr
        .or_else(|| config.addr.clone())
        .unwrap_or_else(|| "127.0.0.1:8080".to_string());
    let max_concurrent_disk_ops = args
        .max_concurrent_disk_ops
        .or(config.max_concurrent_disk_ops);

    let mut world = kdb::World::create(&data_dir, axes, world_dim).unwrap_or_else(|e| {
        eprintln!("failed to open world at {data_dir}: {e}");
        std::process::exit(1);
    });
    if let Some(n) = max_concurrent_disk_ops {
        world = world.with_max_concurrent_disk_ops(n);
    }
    let credentials = config.credentials();
    println!(
        "kdbserver: world at {data_dir} (axes={}, world_dim={}, max_concurrent_disk_ops={}), \
         {} account(s) configured",
        world.axes(),
        world.world_dim(),
        world.max_concurrent_disk_ops(),
        credentials.len()
    );

    let app = routes::router(AppState::new(world, std::sync::Arc::new(credentials)));

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .unwrap_or_else(|e| {
            eprintln!("failed to bind {addr}: {e}");
            std::process::exit(1);
        });
    println!("kdbserver listening on http://{addr}");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .unwrap_or_else(|e| {
            eprintln!("server error: {e}");
            std::process::exit(1);
        });
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("failed to install Ctrl+C handler");
    println!("\nkdbserver shutting down");
}
