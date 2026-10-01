//! Integration tests: spawn a real `kblockdbserver` and run a real
//! scenario against it, checking the result is sane (no errors, the right
//! op count) rather than just that the helper functions compute correctly
//! in isolation (see the `#[cfg(test)]` blocks in `stats.rs`/`client.rs`/
//! `scenarios.rs` for those). Skips (rather than failing) if the
//! `kblockdbserver` binary isn't built yet, since `cargo test -p kblockdbperf` alone
//! doesn't imply `cargo build -p kblockdbserver` ran first.

use crate::binary_client::BinaryClient;
use crate::client::Client;
use crate::scenarios;
use crate::server::{default_kblockdbserver_bin, ManagedServer};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_PORT: AtomicU64 = AtomicU64::new(19_080);

fn next_port() -> u16 {
    NEXT_PORT.fetch_add(1, Ordering::Relaxed) as u16
}

const TEST_ADMIN_PASSWORD: &str = "kblockdbperf-test-admin-password";

fn temp_data_dir(tag: &str) -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "kblockdbperf-test-{tag}-{n}-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).expect("failed to create temp data dir");
    dir
}

/// `None` (the test should skip, not fail) if `kblockdbserver` hasn't been
/// built into this same `target/<profile>/` directory yet. Always brings up
/// the binary protocol alongside the REST API (see `ManagedServer::binary_addr`),
/// so every test can exercise either without needing a separate spawn path.
async fn spawn_test_server(dir: &std::path::Path) -> Option<ManagedServer> {
    let bin = default_kblockdbserver_bin();
    if !bin.exists() {
        eprintln!(
            "skipping: {} not found -- run `cargo build -p kblockdbserver` first",
            bin.display()
        );
        return None;
    }
    let binary_port = next_port();
    Some(
        ManagedServer::spawn(
            &bin,
            dir,
            next_port(),
            Some(binary_port),
            TEST_ADMIN_PASSWORD,
        )
        .await,
    )
}

fn test_client(server: &ManagedServer) -> Client {
    Client::new(server.url.clone(), "admin", TEST_ADMIN_PASSWORD)
}

#[tokio::test]
async fn set_cell_scenario_runs_against_a_real_server_with_no_errors() {
    let dir = temp_data_dir("set-cell");
    let Some(server) = spawn_test_server(&dir).await else {
        return;
    };

    let client = test_client(&server);
    let health = client.health().await.expect("health check failed");
    assert_eq!(health.axes, kblockdblib_default_axes());

    let result = scenarios::set_cell(&client, health.axes, health.world_dim, 20).await;
    assert_eq!(result.throughput.ops, 20);
    assert_eq!(result.latency.count, 20);
    assert_eq!(result.latency.errors, 0);

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn get_and_remove_cell_scenarios_populate_their_own_data() {
    let dir = temp_data_dir("get-remove-cell");
    let Some(server) = spawn_test_server(&dir).await else {
        return;
    };
    let client = test_client(&server);
    let health = client.health().await.unwrap();

    // These scenarios must work even though nothing was set beforehand --
    // they populate what they need themselves.
    let get = scenarios::get_cell(&client, health.axes, health.world_dim, 10).await;
    assert_eq!(get.latency.errors, 0);
    assert_eq!(get.latency.count, 10);

    let remove = scenarios::remove_cell(&client, health.axes, health.world_dim, 10).await;
    assert_eq!(remove.latency.errors, 0);
    assert_eq!(remove.latency.count, 10);

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn region_sweep_scenario_runs_with_no_errors() {
    let dir = temp_data_dir("region-sweep");
    let Some(server) = spawn_test_server(&dir).await else {
        return;
    };
    let client = test_client(&server);
    let health = client.health().await.unwrap();

    let results = scenarios::region_sweep(&client, health.axes, &[2, 4], 2).await;
    // 2 edge sizes x (set_region + get_region) = 4 results.
    assert_eq!(results.len(), 4);
    for r in &results {
        assert_eq!(r.latency.errors, 0, "{} {} had errors", r.name, r.detail);
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn concurrency_scan_scenario_runs_concurrently_with_no_errors() {
    let dir = temp_data_dir("concurrency-scan");
    let Some(server) = spawn_test_server(&dir).await else {
        return;
    };
    let client = test_client(&server);
    let health = client.health().await.unwrap();

    let results =
        scenarios::concurrency_scan(&client, health.axes, health.world_dim, &[1, 8], 5).await;
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].throughput.ops, 5); // concurrency=1
    assert_eq!(results[1].throughput.ops, 40); // concurrency=8
    for r in &results {
        assert_eq!(r.latency.errors, 0);
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn binary_set_cell_scenario_runs_against_a_real_server_with_no_errors() {
    let dir = temp_data_dir("binary-set-cell");
    let Some(server) = spawn_test_server(&dir).await else {
        return;
    };
    let client = test_client(&server);
    let health = client.health().await.expect("health check failed");

    let binary_addr = server
        .binary_addr
        .as_deref()
        .expect("binary protocol enabled");
    let (mut bc, bhealth) = BinaryClient::connect(binary_addr, "admin", TEST_ADMIN_PASSWORD)
        .await
        .expect("failed to connect the binary client");
    assert_eq!(bhealth.axes, health.axes);
    assert_eq!(bhealth.world_dim, health.world_dim);

    let result = scenarios::binary_set_cell(&mut bc, health.axes, health.world_dim, 20).await;
    assert_eq!(result.throughput.ops, 20);
    assert_eq!(result.latency.count, 20);
    assert_eq!(result.latency.errors, 0);

    let _ = std::fs::remove_dir_all(&dir);
}

/// `kblockdblib::AXES`'s default, duplicated as a literal since kblockdbperf
/// deliberately doesn't depend on `kblockdblib` (it treats kblockdbserver as a black
/// box over HTTP) -- used only to sanity-check a freshly spawned test
/// server came up with the shape we expect.
fn kblockdblib_default_axes() -> usize {
    3
}
