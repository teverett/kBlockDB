mod auth;
mod binary_server;
mod browser;
mod cluster;
mod config;
mod coords;
mod error;
mod openapi;
mod routes;
mod state;
#[cfg(test)]
mod tests;
mod value_json;

use config::Config;
use state::AppState;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::PathBuf;

/// The REST API's port when neither `--http-port` nor the config file
/// says otherwise.
const DEFAULT_HTTP_PORT: u16 = 8080;

/// The peer-replication protocol's port when clustering is enabled (see
/// `config::ClusterConfig::cluster_secret`) but neither `--peer-port` nor
/// the config file names one -- unlike `binary_port`, which stays off
/// entirely with no default, clustering being *on* already implies a
/// peer listener is wanted, so this one has a default the way
/// `http_port` does.
const DEFAULT_PEER_PORT: u16 = 8082;

/// Where to listen for a given port.
///
/// Only the port is configurable: the server always binds every
/// interface, so it's reachable at whichever address this host happens
/// to have without anyone naming that address up front -- which often
/// isn't knowable in advance anyway (DHCP, containers, moving between
/// networks).
///
/// `0.0.0.0` rather than the IPv6 `[::]`, even though the latter is
/// dual-stack on Linux and macOS and would cover both families at once:
/// FreeBSD ships `net.inet6.ip6.v6only=1` by default, where an IPv6
/// wildcard listener accepts *no* IPv4 connections at all. Binding IPv4
/// behaves the same everywhere, which matters more here than IPv6 reach
/// does.
fn bind_addr(port: u16) -> SocketAddr {
    SocketAddr::from((Ipv4Addr::UNSPECIFIED, port))
}

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
    http_port: Option<u16>,
    binary_port: Option<u16>,
    peer_port: Option<u16>,
    max_concurrent_disk_ops: Option<usize>,
    max_cached_chunks: Option<usize>,
}

fn parse_args() -> Args {
    let mut config_path = PathBuf::from("./kblockdbserver.toml");
    let mut data_dir = None;
    let mut axes = None;
    let mut world_dim = None;
    let mut chunk_size = None;
    let mut http_port = None;
    let mut binary_port = None;
    let mut peer_port = None;
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
            "--http-port" => http_port = Some(expect_port(&mut args, "--http-port")),
            "--binary-port" => binary_port = Some(expect_port(&mut args, "--binary-port")),
            "--peer-port" => peer_port = Some(expect_port(&mut args, "--peer-port")),
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
        http_port,
        binary_port,
        peer_port,
        max_concurrent_disk_ops,
        max_cached_chunks,
    }
}

/// Like `expect_value`, but for a port number.
///
/// `0` is allowed and meaningful -- it asks the OS to pick a free port,
/// which the startup banner then reports, so a throwaway instance doesn't
/// have to guess at what's free.
fn expect_port(args: &mut impl Iterator<Item = String>, flag: &str) -> u16 {
    expect_value(args, flag).parse().unwrap_or_else(|_| {
        eprintln!("{flag} must be a port number between 0 and 65535");
        std::process::exit(1);
    })
}

fn expect_value(args: &mut impl Iterator<Item = String>, flag: &str) -> String {
    args.next().unwrap_or_else(|| {
        eprintln!("{flag} requires a value");
        std::process::exit(1);
    })
}

fn print_help() {
    println!(
        "kblockdbserver -- a RESTful HTTP front end for the kblockdblib storage engine,\n\
         managing any number of independent databases under one data directory\n\n\
         USAGE:\n    kblockdbserver [OPTIONS]\n\n\
         OPTIONS:\n    \
         --config <path>      Config file (default: ./kblockdbserver.toml). Required --\n                          \
         holds admin_password and, optionally, [[users]], plus optional\n                          \
         http_port/binary_port/data_dir/max_concurrent_disk_ops/\n                          \
         max_cached_chunks/compression and a [worldparameters] table (each\n                          \
         overridden by the matching CLI flag below, if given; compression\n                          \
         is config-file-only). Example:\n                          \
         \n                          \
         http_port = 8080\n                          \
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
         --data-dir <path>    Data directory; each database is its own subdirectory\n                          \
         of this one (default: ./data). No database is created\n                          \
         automatically -- see PUT /rest/databases/{{name}} (or the\n                          \
         binary protocol's CreateDatabase)\n    \
         --axes <n>           Axis count for a brand-new database, when none is given\n                          \
         explicitly at creation time (default: {})\n    \
         --world-dim <n>      Cells per axis for a brand-new database, same default-only\n                          \
         rule as --axes (default: {})\n    \
         --chunk-size <n>     Cells per axis within a chunk, for a brand-new database,\n                          \
         same default-only rule as --axes\n                          \
         (default: {}; see kblockdblib::DEFAULT_CHUNK_DIM -- a\n                          \
         bigger chunk means fewer, larger chunk files, so more\n                          \
         bytes rewritten per single-cell write but less filesystem\n                          \
         metadata overhead; measure the right value for yours)\n    \
         --http-port <port>   Port to listen on for the REST API (default: {}).\n                          \
         Binds every interface, so the server is reachable at any\n                          \
         of this host's addresses -- set admin_password before\n                          \
         running it somewhere untrusted. 0 asks the OS for a free\n                          \
         port, which the startup banner then reports.\n    \
         --binary-port <port> Also listen on this port for the binary protocol (see\n                          \
         wire.rs and docs/binary-protocol.md) -- disabled unless given;\n                          \
         same databases, same accounts as the\n                          \
         REST API, just without HTTP/JSON overhead\n    \
         --peer-port <port>   Port the peer-replication protocol listens on, when\n                          \
         clustering is enabled (config file's cluster_secret -- see\n                          \
         docs/clustering.md); default {} if cluster_secret is set but this\n                          \
         isn't. Has no effect at all without cluster_secret\n    \
         --max-concurrent-disk-ops <n>\n                          \
         Cap on concurrent filesystem operations, applied to every\n                          \
         database (default: {}; see\n                          \
         kblockdblib::DEFAULT_MAX_CONCURRENT_DISK_OPS -- the right\n                          \
         number depends on your filesystem/storage; measure it with\n                          \
         kblockdbperf's concurrency_scan)\n    \
         --max-cached-chunks <n>\n                          \
         Cap on distinct chunks kept in the write-through cache at\n                          \
         once, per database, LRU-evicted beyond that (default: {};\n                          \
         see kblockdblib::DEFAULT_MAX_CACHED_CHUNKS -- 0 disables the\n                          \
         cache's benefit without disabling the server)\n    \
         -h, --help           Print this help\n\n\
         hostname (config file only) overrides what /rest/health reports as this\n\
         instance's name; it defaults to the OS hostname.\n\n\
         compression (config file only, default false) zstd-compresses every chunk\n\
         file the server writes, for every database. It can be turned on or off at any\n\
         time: each chunk file records its own encoding, so a database may hold a mix,\n\
         and existing files are only re-encoded when their chunk is next written.\n\n\
         --axes/--world-dim/--chunk-size only matter for a database created without an\n\
         explicit shape of its own (an empty PUT /rest/databases/{{name}} body, or the\n\
         binary protocol's CreateDatabase, which has no way to override them at all);\n\
         reopening an existing database reads its real shape from its own world.txt and\n\
         ignores these flags.\n\n\
         Every REST endpoint except /health requires HTTP Basic Auth against\n\
         an account from the config file (admin_password, or a [[users]] entry).\n\
         admin_password/users are config-file-only -- never CLI flags -- so\n\
         credentials don't end up in shell history or `ps` output.",
        kblockdblib::AXES,
        kblockdblib::WORLD_DIM,
        kblockdblib::DEFAULT_CHUNK_DIM,
        DEFAULT_HTTP_PORT,
        DEFAULT_PEER_PORT,
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
    let http_addr = bind_addr(
        args.http_port
            .or(config.http_port)
            .unwrap_or(DEFAULT_HTTP_PORT),
    );
    let binary_addr = args.binary_port.or(config.binary_port).map(bind_addr);
    let max_concurrent_disk_ops = args
        .max_concurrent_disk_ops
        .or(config.max_concurrent_disk_ops);
    let max_cached_chunks = args.max_cached_chunks.or(config.max_cached_chunks);

    let default_shape = state::WorldShape {
        axes,
        world_dim,
        chunk_dim: chunk_size,
    };
    let databases = state::Databases::new(&data_dir, default_shape)
        .with_compression(config.compression)
        .with_max_concurrent_disk_ops(max_concurrent_disk_ops)
        .with_max_cached_chunks(max_cached_chunks)
        // Tombstones only matter for replication -- see
        // `World::with_tombstone_retention`.
        .with_tombstone_retention(
            config
                .cluster
                .cluster_secret
                .as_ref()
                .map(|_| config.cluster.tombstone_retention()),
        );
    let existing = databases.list().unwrap_or_else(|e| {
        eprintln!("failed to read data directory {data_dir}: {e}");
        std::process::exit(1);
    });
    let credentials = config.credentials();
    println!(
        "kblockdbserver: data dir {data_dir} ({} existing database(s): {}), default shape for a \
         new database: axes={default_shape_axes}, world_dim={default_shape_world_dim}, \
         chunk_size={default_shape_chunk_dim}, max_concurrent_disk_ops={}, \
         max_cached_chunks={}, compression={}, {} account(s) configured",
        existing.len(),
        if existing.is_empty() {
            "none".to_string()
        } else {
            existing.join(", ")
        },
        max_concurrent_disk_ops
            .map(|n| n.to_string())
            .unwrap_or_else(|| kblockdblib::DEFAULT_MAX_CONCURRENT_DISK_OPS.to_string()),
        max_cached_chunks
            .map(|n| n.to_string())
            .unwrap_or_else(|| kblockdblib::DEFAULT_MAX_CACHED_CHUNKS.to_string()),
        config.compression,
        credentials.len(),
        default_shape_axes = default_shape.axes,
        default_shape_world_dim = default_shape.world_dim,
        default_shape_chunk_dim = default_shape.chunk_dim,
    );

    let mut state = AppState::new(databases, std::sync::Arc::new(credentials));
    if let Some(hostname) = config.hostname.clone() {
        state = state.with_hostname(hostname);
    }
    println!("kblockdbserver: reporting hostname '{}'", state.hostname);

    // Clustering is entirely opt-in: nothing in this block runs, and
    // `state.replication` stays `None`, unless the config sets
    // `cluster_secret` -- see `config::Config::validate`'s matching rule
    // that `cluster_secret` is required whenever `[[peers]]` is non-empty
    // (but not the other way around: a `cluster_secret` with no `peers`
    // still starts a peer listener, just with nothing to connect out to --
    // e.g. a node everyone else points at).
    if let Some(cluster_secret) = config.cluster.cluster_secret.clone() {
        let peer_addr = bind_addr(
            args.peer_port
                .or(config.cluster.peer_port)
                .unwrap_or(DEFAULT_PEER_PORT),
        );
        let peer_listener = tokio::net::TcpListener::bind(&peer_addr)
            .await
            .unwrap_or_else(|e| {
                eprintln!("failed to bind peer protocol address {peer_addr}: {e}");
                std::process::exit(1);
            });
        let shown = peer_listener
            .local_addr()
            .map(reachable_addr)
            .unwrap_or_else(|_| reachable_addr(peer_addr));
        println!(
            "kblockdbserver peer protocol listening on {shown} ({} peer(s) configured)",
            config.peers.len()
        );

        let hub = kblockdbcluster::hub::ReplicationHub::new();
        // Every known peer -- configured below, or learned when one
        // connects in -- is replicated to as well as from (see
        // `kblockdbcluster::peers`); `add` spawns the outbound link.
        let peers = kblockdbcluster::peers::PeerSet::new(
            kblockdbcluster::peers::LocalIdentity {
                cluster_secret,
                server_id: state.hostname.to_string(),
                peer_port: peer_listener
                    .local_addr()
                    .map(|a| a.port())
                    .unwrap_or(peer_addr.port()),
            },
            hub.clone(),
        )
        .with_keepalive(config.cluster.keepalive())
        .with_change_source(std::sync::Arc::new(state.clone()))
        .with_catch_up_margin(config.cluster.catch_up_margin())
        .with_watermarks(
            std::path::Path::new(&data_dir)
                .join(".cluster")
                .join("watermarks"),
        )
        .unwrap_or_else(|e| {
            eprintln!("failed to read cluster watermarks in {data_dir}/.cluster: {e}");
            std::process::exit(1);
        });
        // A peer last synced longer ago than tombstones are kept may have
        // missed deletes that no catch-up can now report.
        for (address, through_ms) in peers.stale_watermarks(config.cluster.tombstone_retention()) {
            eprintln!(
                "kblockdbserver: warning: last synced with {address} at {through_ms} ms, longer \
                 ago than tombstone_retention_secs -- deletes made since may be missing here; \
                 see docs/clustering.md"
            );
        }
        for peer in &config.peers {
            peers.add_configured(peer.address.clone());
        }
        let dead_peer_timeout = config
            .cluster
            .dead_peer_timeout_secs
            .unwrap_or(config::DEFAULT_DEAD_PEER_TIMEOUT_SECS);
        if dead_peer_timeout > 0 {
            tokio::spawn(
                peers
                    .clone()
                    .prune_forever(std::time::Duration::from_secs(dead_peer_timeout)),
            );
        }
        state = state.with_replication(hub).with_peers(peers.clone());

        let peer_state = state.clone();
        tokio::spawn(async move {
            kblockdbcluster::server::serve(peer_listener, peer_state, peers).await;
        });
    }

    if let Some(binary_addr) = binary_addr {
        let binary_listener = tokio::net::TcpListener::bind(&binary_addr)
            .await
            .unwrap_or_else(|e| {
                eprintln!("failed to bind binary protocol address {binary_addr}: {e}");
                std::process::exit(1);
            });
        // Same bound-address-over-requested reasoning as the HTTP
        // banner below, and the same wildcard-to-loopback rewrite -- the
        // point of printing it is that it can be connected to.
        let shown = binary_listener
            .local_addr()
            .map(reachable_addr)
            .unwrap_or_else(|_| reachable_addr(binary_addr));
        println!("kblockdbserver binary protocol listening on {shown}");
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
    // Prefer the address the listener actually bound over the one asked
    // for: `--http-port 0` picks a real port only at bind time, so the
    // requested one would print a useless `:0`.
    let base = listener
        .local_addr()
        .map(base_url)
        .unwrap_or_else(|_| base_url(http_addr));
    println!("kblockdbserver listening on {base}");
    println!("  data browser    {base}/");
    println!("  health API      {base}/rest/health");
    println!("  databases API   {base}/rest/databases");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .unwrap_or_else(|e| {
            eprintln!("server error: {e}");
            std::process::exit(1);
        });
}

/// This host's primary IP -- the source address the kernel would use to
/// reach the wider network.
///
/// Found by asking the routing table, not by resolving the hostname:
/// hostname lookups routinely answer `127.0.0.1` (that's what
/// `/etc/hosts` usually says) which is exactly the useless answer this
/// exists to avoid, and they can block on DNS. Connecting a UDP socket
/// sends no packets at all -- it only makes the kernel pick a route and
/// bind a local address, which is then read back. The destination is a
/// well-known public address used purely as a routing hint; it is never
/// contacted and need not be reachable.
///
/// Returns `None` when there's no route to the outside world at all (an
/// isolated container, a laptop with every interface down), where there
/// genuinely is no better answer than loopback.
fn primary_local_ip() -> Option<IpAddr> {
    let socket = std::net::UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)).ok()?;
    socket.connect((Ipv4Addr::new(192, 0, 2, 1), 9)).ok()?;
    let ip = socket.local_addr().ok()?.ip();
    (!ip.is_unspecified() && !ip.is_loopback()).then_some(ip)
}

/// An address a listener is bound to, rewritten into one a client can
/// actually connect to.
///
/// A wildcard bind (`0.0.0.0` / `[::]`) means "every interface on this
/// host", which is not itself a connectable address -- pasting
/// `http://0.0.0.0:8080` into a browser does nothing useful. Report this
/// host's primary IP instead, so what's printed is an address other
/// machines can actually reach, which is the point of binding every
/// interface in the first place. Loopback only as a last resort, when
/// the host has no route out. Concrete addresses pass through untouched.
fn reachable_addr(addr: SocketAddr) -> SocketAddr {
    if !addr.ip().is_unspecified() {
        return addr;
    }
    let ip = primary_local_ip().unwrap_or_else(|| match addr.ip() {
        IpAddr::V4(_) => IpAddr::V4(Ipv4Addr::LOCALHOST),
        IpAddr::V6(_) => IpAddr::V6(Ipv6Addr::LOCALHOST),
    });
    SocketAddr::new(ip, addr.port())
}

/// The browsable base URL for an address the server is listening on.
fn base_url(addr: SocketAddr) -> String {
    // `SocketAddr`'s Display already brackets IPv6 for URLs.
    format!("http://{}", reachable_addr(addr))
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("failed to install Ctrl+C handler");
    println!("\nkblockdbserver shutting down");
}
