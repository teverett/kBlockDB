mod auth;
mod binary_server;
mod browser;
mod config;
mod coords;
mod error;
mod openapi;
mod query;
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
    chunk_size: Option<u32>,
    http_addr: Option<String>,
    binary_addr: Option<String>,
    max_concurrent_disk_ops: Option<usize>,
    max_cached_chunks: Option<usize>,
}

fn parse_args() -> Args {
    let mut config_path = PathBuf::from("./kblockdbserver.toml");
    let mut data_dir = None;
    let mut axes = None;
    let mut world_dim = None;
    let mut chunk_size = None;
    let mut http_addr = None;
    let mut binary_addr = None;
    let mut max_concurrent_disk_ops = None;
    let mut max_cached_chunks = None;

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
            "--chunk-size" => {
                chunk_size = Some(
                    expect_value(&mut args, "--chunk-size")
                        .parse()
                        .unwrap_or_else(|_| {
                            eprintln!("--chunk-size must be a positive integer");
                            std::process::exit(1);
                        }),
                )
            }
            "--http-addr" => http_addr = Some(expect_value(&mut args, "--http-addr")),
            "--binary-addr" => binary_addr = Some(expect_value(&mut args, "--binary-addr")),
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
            "--max-cached-chunks" => {
                max_cached_chunks = Some(
                    expect_value(&mut args, "--max-cached-chunks")
                        .parse()
                        .unwrap_or_else(|_| {
                            eprintln!("--max-cached-chunks must be a non-negative integer");
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
        chunk_size,
        http_addr,
        binary_addr,
        max_concurrent_disk_ops,
        max_cached_chunks,
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
        "kblockdbserver -- a RESTful HTTP front end for the kblockdblib storage engine\n\n\
         USAGE:\n    kblockdbserver [OPTIONS]\n\n\
         OPTIONS:\n    \
         --config <path>      Config file (default: ./kblockdbserver.toml). Required --\n                          \
         holds admin_password and, optionally, [[users]], plus optional\n                          \
         http_addr/binary_addr/data_dir/max_concurrent_disk_ops/\n                          \
         max_cached_chunks/compression and a [worldparameters] table (each\n                          \
         overridden by the matching CLI flag below, if given; compression\n                          \
         is config-file-only). Example:\n                          \
         \n                          \
         http_addr = \"127.0.0.1:8080\"\n                          \
         data_dir = \"./data\"\n                          \
         admin_password = \"change-me\"\n                          \
         compression = false\n                          \
         \n                          \
         [worldparameters]\n                          \
         axes = 3\n                          \
         world_dim = 10000\n                          \
         chunk_size = 32\n                          \
         \n                          \
         [[users]]\n                          \
         username = \"alice\"\n                          \
         password = \"alice-password\"\n    \
         --data-dir <path>    World data directory (default: ./data)\n    \
         --axes <n>           Axis count for a brand-new world (default: {})\n    \
         --world-dim <n>      Cells per axis for a brand-new world (default: {})\n    \
         --chunk-size <n>     Cells per axis within a chunk, for a brand-new world\n                          \
         (default: {}; see kblockdblib::DEFAULT_CHUNK_DIM -- a\n                          \
         bigger chunk means fewer, larger chunk files, so more\n                          \
         bytes rewritten per single-cell write but less filesystem\n                          \
         metadata overhead; measure the right value for yours)\n    \
         --http-addr <host:port>\n                          \
         Address to listen on for the REST API (default: 127.0.0.1:8080)\n    \
         --binary-addr <host:port>\n                          \
         Also listen on this address for the binary protocol (see\n                          \
         wire.rs and the README's \"Binary protocol\" section) --\n                          \
         disabled unless given; same World, same accounts as the\n                          \
         REST API, just without HTTP/JSON overhead\n    \
         --max-concurrent-disk-ops <n>\n                          \
         Cap on concurrent filesystem operations\n                          \
         (default: {}; see kblockdblib::DEFAULT_MAX_CONCURRENT_DISK_OPS --\n                          \
         the right number depends on your filesystem/storage;\n                          \
         measure it with kblockdbperf's concurrency_scan)\n    \
         --max-cached-chunks <n>\n                          \
         Cap on distinct chunks kept in the write-through cache at\n                          \
         once, LRU-evicted beyond that (default: {}; see\n                          \
         kblockdblib::DEFAULT_MAX_CACHED_CHUNKS -- 0 disables the\n                          \
         cache's benefit without disabling the server)\n    \
         -h, --help           Print this help\n\n\
         hostname (config file only) overrides what /rest/health reports as this\n\
         instance's name; it defaults to the OS hostname.\n\n\
         compression (config file only, default false) zstd-compresses every chunk\n\
         file the server writes. It can be turned on or off on an existing world at\n\
         any time: each chunk file records its own encoding, so a world may hold a\n\
         mix, and existing files are only re-encoded when their chunk is next\n\
         written.\n\n\
         --axes/--world-dim/--chunk-size only matter the first time a world is\n\
         created at --data-dir; reopening an existing one reads its real shape\n\
         from its world.txt and ignores these flags (World::open does, not\n\
         create).\n\n\
         Every REST endpoint except /health requires HTTP Basic Auth against\n\
         an account from the config file (admin_password, or a [[users]] entry).\n\
         admin_password/users are config-file-only -- never CLI flags -- so\n\
         credentials don't end up in shell history or `ps` output.",
        kblockdblib::AXES,
        kblockdblib::WORLD_DIM,
        kblockdblib::DEFAULT_CHUNK_DIM,
        kblockdblib::DEFAULT_MAX_CONCURRENT_DISK_OPS,
        kblockdblib::DEFAULT_MAX_CACHED_CHUNKS
    );
}

#[tokio::main]
async fn main() {
    let args = parse_args();

    let config = Config::load(&args.config_path).unwrap_or_else(|e| {
        eprintln!(
            "{e}\n\nSee `kblockdbserver --help` for the config file's format, or point\n\
             --config at a different file."
        );
        std::process::exit(1);
    });

    let data_dir = args
        .data_dir
        .or_else(|| config.data_dir.clone())
        .unwrap_or_else(|| "./data".to_string());
    let axes = args
        .axes
        .or(config.worldparameters.axes)
        .unwrap_or(kblockdblib::AXES);
    let world_dim = args
        .world_dim
        .or(config.worldparameters.world_dim)
        .unwrap_or(kblockdblib::WORLD_DIM);
    let chunk_size = args
        .chunk_size
        .or(config.worldparameters.chunk_size)
        .unwrap_or(kblockdblib::DEFAULT_CHUNK_DIM);
    let http_addr = args
        .http_addr
        .or_else(|| config.http_addr.clone())
        .unwrap_or_else(|| "127.0.0.1:8080".to_string());
    let binary_addr = args.binary_addr.or_else(|| config.binary_addr.clone());
    let max_concurrent_disk_ops = args
        .max_concurrent_disk_ops
        .or(config.max_concurrent_disk_ops);
    let max_cached_chunks = args.max_cached_chunks.or(config.max_cached_chunks);

    let mut world = kblockdblib::World::create(&data_dir, axes, world_dim, chunk_size)
        .unwrap_or_else(|e| {
            eprintln!("failed to open world at {data_dir}: {e}");
            std::process::exit(1);
        })
        .with_compression(config.compression);
    if let Some(n) = max_concurrent_disk_ops {
        world = world.with_max_concurrent_disk_ops(n);
    }
    if let Some(n) = max_cached_chunks {
        world = world.with_max_cached_chunks(n);
    }
    let credentials = config.credentials();
    println!(
        "kblockdbserver: world at {data_dir} (axes={}, world_dim={}, chunk_size={}, \
         max_concurrent_disk_ops={}, max_cached_chunks={}, compression={}), \
         {} account(s) configured",
        world.axes(),
        world.world_dim(),
        world.chunk_dim(),
        world.max_concurrent_disk_ops(),
        world.max_cached_chunks(),
        world.compression(),
        credentials.len()
    );

    let mut state = AppState::new(world, std::sync::Arc::new(credentials));
    if let Some(hostname) = config.hostname.clone() {
        state = state.with_hostname(hostname);
    }
    println!("kblockdbserver: reporting hostname '{}'", state.hostname);

    if let Some(binary_addr) = binary_addr {
        let binary_listener = tokio::net::TcpListener::bind(&binary_addr)
            .await
            .unwrap_or_else(|e| {
                eprintln!("failed to bind binary protocol address {binary_addr}: {e}");
                std::process::exit(1);
            });
        println!("kblockdbserver binary protocol listening on {binary_addr}");
        let binary_state = state.clone();
        tokio::spawn(async move {
            binary_server::serve(binary_listener, binary_state).await;
        });
    }

    let app = routes::router(state);

    let listener = tokio::net::TcpListener::bind(&http_addr)
        .await
        .unwrap_or_else(|e| {
            eprintln!("failed to bind {http_addr}: {e}");
            std::process::exit(1);
        });
    println!("kblockdbserver listening on http://{http_addr}");

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
    println!("\nkblockdbserver shutting down");
}
