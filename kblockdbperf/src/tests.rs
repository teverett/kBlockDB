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
use crate::{TARGET_AXES, TARGET_CHUNK_SIZE, TARGET_WORLD_DIM};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_PORT: AtomicU64 = AtomicU64::new(19_080);

fn next_port() -> u16 {
    NEXT_PORT.fetch_add(1, Ordering::Relaxed) as u16
}

const TEST_ADMIN_PASSWORD: &str = "kblockdbperf-test-admin-password";
/// The database every test below targets -- created fresh (via
/// `ensure_database`) in each test's own temp data dir, so tests never
/// share data with each other.
const TEST_DB: &str = "perf-test-db";

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

/// A `Client` targeting `TEST_DB` on `server`, with that database already
/// created and shaped `TARGET_AXES`/`TARGET_WORLD_DIM`/`TARGET_CHUNK_SIZE`
/// -- every test below builds on top of this, since kblockdbserver never
/// creates a database implicitly.
async fn test_client(server: &ManagedServer) -> Client {
    let client = Client::new(server.url.clone(), "admin", TEST_ADMIN_PASSWORD, TEST_DB);
    client
        .ensure_database(TARGET_AXES, TARGET_WORLD_DIM, TARGET_CHUNK_SIZE)
        .await
        .expect("failed to create the test database");
    client
}

#[tokio::test]
async fn ensure_database_creates_it_and_is_idempotent_on_a_second_call() {
    let dir = temp_data_dir("ensure-database");
    let Some(server) = spawn_test_server(&dir).await else {
        return;
    };
    let client = Client::new(server.url.clone(), "admin", TEST_ADMIN_PASSWORD, TEST_DB);

    client
        .ensure_database(TARGET_AXES, TARGET_WORLD_DIM, TARGET_CHUNK_SIZE)
        .await
        .expect("first ensure_database should create the database");
    // A second call against the same (now-existing) database must not
    // error -- a repeat perf run against the same --db name reuses it.
    client
        .ensure_database(TARGET_AXES, TARGET_WORLD_DIM, TARGET_CHUNK_SIZE)
        .await
        .expect("second ensure_database should treat 409 as success");

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn health_reports_hostname_and_the_database_count() {
    let dir = temp_data_dir("health");
    let Some(server) = spawn_test_server(&dir).await else {
        return;
    };
    let client = test_client(&server).await;

    let health = client.health().await.expect("health check failed");
    assert!(!health.hostname.is_empty());
    assert_eq!(health.database_count, 1);

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn set_cell_scenario_runs_against_a_real_server_with_no_errors() {
    let dir = temp_data_dir("set-cell");
    let Some(server) = spawn_test_server(&dir).await else {
        return;
    };

    let client = test_client(&server).await;

    let result = scenarios::set_cell(&client, TARGET_AXES, TARGET_WORLD_DIM, 20).await;
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
    let client = test_client(&server).await;

    // These scenarios must work even though nothing was set beforehand --
    // they populate what they need themselves.
    let get = scenarios::get_cell(&client, TARGET_AXES, TARGET_WORLD_DIM, 10).await;
    assert_eq!(get.latency.errors, 0);
    assert_eq!(get.latency.count, 10);

    let remove = scenarios::remove_cell(&client, TARGET_AXES, TARGET_WORLD_DIM, 10).await;
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
    let client = test_client(&server).await;

    let results = scenarios::region_sweep(&client, TARGET_AXES, &[2, 4], 2).await;
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
    let client = test_client(&server).await;

    let results =
        scenarios::concurrency_scan(&client, TARGET_AXES, TARGET_WORLD_DIM, &[1, 8], 5).await;
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
    // Only needed for its side effect: creating `TEST_DB` before the
    // binary client below connects to it.
    let _client = test_client(&server).await;

    let binary_addr = server
        .binary_addr
        .as_deref()
        .expect("binary protocol enabled");
    let (mut bc, bhealth) =
        BinaryClient::connect(binary_addr, "admin", TEST_ADMIN_PASSWORD, TEST_DB)
            .await
            .expect("failed to connect the binary client");
    assert_eq!(bhealth.axes, TARGET_AXES);
    assert_eq!(bhealth.world_dim, TARGET_WORLD_DIM);

    let result = scenarios::binary_set_cell(&mut bc, TARGET_AXES, TARGET_WORLD_DIM, 20).await;
    assert_eq!(result.throughput.ops, 20);
    assert_eq!(result.latency.count, 20);
    assert_eq!(result.latency.errors, 0);

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn binary_connect_against_a_not_yet_created_database_selects_nothing() {
    let dir = temp_data_dir("binary-unselected");
    let Some(server) = spawn_test_server(&dir).await else {
        return;
    };
    let binary_addr = server
        .binary_addr
        .as_deref()
        .expect("binary protocol enabled");

    // No `ensure_database` call first -- the database genuinely doesn't
    // exist, so `connect` (which requires `HelloOk.database` to be
    // present) must report this as a connection failure.
    let result = BinaryClient::connect(binary_addr, "admin", TEST_ADMIN_PASSWORD, "never-created")
        .await;
    assert!(result.is_none());

    let _ = std::fs::remove_dir_all(&dir);
}
