mod coords;
mod error;
mod routes;
mod state;
#[cfg(test)]
mod tests;
mod value_json;

use state::AppState;

struct Args {
    data_dir: String,
    axes: usize,
    world_dim: u32,
    addr: String,
    max_concurrent_disk_ops: Option<usize>,
}

fn parse_args() -> Args {
    let mut data_dir = "./data".to_string();
    let mut axes = kdb::AXES;
    let mut world_dim = kdb::WORLD_DIM;
    let mut addr = "127.0.0.1:8080".to_string();
    let mut max_concurrent_disk_ops = None;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--data-dir" => data_dir = expect_value(&mut args, "--data-dir"),
            "--axes" => {
                axes = expect_value(&mut args, "--axes")
                    .parse()
                    .unwrap_or_else(|_| {
                        eprintln!("--axes must be a positive integer");
                        std::process::exit(1);
                    })
            }
            "--world-dim" => {
                world_dim = expect_value(&mut args, "--world-dim")
                    .parse()
                    .unwrap_or_else(|_| {
                        eprintln!("--world-dim must be a positive integer");
                        std::process::exit(1);
                    })
            }
            "--addr" => addr = expect_value(&mut args, "--addr"),
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
         world.txt and ignores these flags (World::open does, not create).",
        kdb::AXES,
        kdb::WORLD_DIM,
        kdb::DEFAULT_MAX_CONCURRENT_DISK_OPS
    );
}

#[tokio::main]
async fn main() {
    let args = parse_args();

    let mut world =
        kdb::World::create(&args.data_dir, args.axes, args.world_dim).unwrap_or_else(|e| {
            eprintln!("failed to open world at {}: {e}", args.data_dir);
            std::process::exit(1);
        });
    if let Some(n) = args.max_concurrent_disk_ops {
        world = world.with_max_concurrent_disk_ops(n);
    }
    println!(
        "kdbserver: world at {} (axes={}, world_dim={}, max_concurrent_disk_ops={})",
        args.data_dir,
        world.axes(),
        world.world_dim(),
        world.max_concurrent_disk_ops()
    );

    let app = routes::router(AppState::new(world));

    let listener = tokio::net::TcpListener::bind(&args.addr)
        .await
        .unwrap_or_else(|e| {
            eprintln!("failed to bind {}: {e}", args.addr);
            std::process::exit(1);
        });
    println!("kdbserver listening on http://{}", args.addr);

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
