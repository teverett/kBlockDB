//! Integration tests: spawn a real `kblockdbserver` and drive the actual
//! compiled `kblockdbcli` binary against it via `std::process::Command`,
//! checking real stdout/exit codes -- not just that `main.rs`'s
//! `unit_tests` compute correctly in isolation. Skips (rather than
//! failing) if either binary hasn't been built yet, since `cargo test -p
//! kblockdbcli` alone doesn't imply `cargo build -p kblockdbserver` ran too.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

const TEST_PASSWORD: &str = "kblockdbcli-test-password";

/// A `kblockdbserver` child process, killed (and its temp config file removed)
/// on drop -- mirrors `kblockdbperf/src/server.rs`'s `ManagedServer`, just
/// synchronous throughout since `kblockdbcli` itself has no async runtime.
struct ManagedServer {
    child: Child,
    config_path: PathBuf,
    url: String,
}

impl ManagedServer {
    fn spawn(kblockdbserver_bin: &Path, data_dir: &Path, http_port: u16) -> ManagedServer {
        let config_path = std::env::temp_dir().join(format!(
            "kblockdbcli-test-kblockdbserver-config-{http_port}.toml"
        ));
        std::fs::write(
            &config_path,
            format!("admin_password = {TEST_PASSWORD:?}\n"),
        )
        .unwrap_or_else(|e| {
            panic!(
                "failed to write kblockdbserver config {}: {e}",
                config_path.display()
            )
        });

        let child = Command::new(kblockdbserver_bin)
            .arg("--data-dir")
            .arg(data_dir)
            .arg("--http-port")
            .arg(http_port.to_string())
            .arg("--config")
            .arg(&config_path)
            .stdout(Stdio::null())
            .stderr(Stdio::piped()) // kept, not discarded, so a startup failure is diagnosable
            .spawn()
            .unwrap_or_else(|e| panic!("failed to spawn {}: {e}", kblockdbserver_bin.display()));

        // The server binds every interface; connect over loopback.
        let url = format!("http://127.0.0.1:{http_port}");
        wait_until_ready(&url);
        ManagedServer {
            child,
            config_path,
            url,
        }
    }
}

impl Drop for ManagedServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_file(&self.config_path);
    }
}

fn wait_until_ready(url: &str) {
    let http = reqwest::blocking::Client::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if http
            .get(format!("{url}/rest/health"))
            .send()
            .is_ok_and(|r| r.status().is_success())
        {
            return;
        }
        if Instant::now() >= deadline {
            panic!("kblockdbserver at {url} never became ready within 10s");
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Locates a sibling binary built alongside this test binary -- same
/// two-places trick as `kblockdbperf/src/server.rs`'s `default_kblockdbserver_bin`
/// (a plain `cargo test` binary runs one level below `target/<profile>/`,
/// where the actual binaries live).
fn find_bin(name: &str) -> PathBuf {
    let exe = std::env::current_exe().expect("failed to locate this test binary's own path");
    let exe_dir = exe
        .parent()
        .expect("executable path has no parent directory");
    let file_name = format!("{name}{}", std::env::consts::EXE_SUFFIX);

    let sibling = exe_dir.join(&file_name);
    if sibling.exists() {
        return sibling;
    }
    if let Some(one_up) = exe_dir.parent() {
        let cousin = one_up.join(&file_name);
        if cousin.exists() {
            return cousin;
        }
    }
    sibling // doesn't exist either, but gives the caller a sensible path to report
}

static NEXT_PORT: AtomicU64 = AtomicU64::new(19_180);

fn next_port() -> u16 {
    NEXT_PORT.fetch_add(1, Ordering::Relaxed) as u16
}

fn temp_data_dir(tag: &str) -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir =
        std::env::temp_dir().join(format!("kblockdbcli-test-{tag}-{n}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("failed to create temp data dir");
    dir
}

/// Spawns a test server plus locates the `kblockdbcli` binary under test, or
/// `None` (the test should skip, not fail) if either hasn't been built.
fn test_fixture(tag: &str) -> Option<(ManagedServer, PathBuf, PathBuf)> {
    let kblockdbserver_bin = find_bin("kblockdbserver");
    if !kblockdbserver_bin.exists() {
        eprintln!(
            "skipping: {} not found -- run `cargo build -p kblockdbserver` first",
            kblockdbserver_bin.display()
        );
        return None;
    }
    let kblockdbcli_bin = find_bin("kblockdbcli");
    if !kblockdbcli_bin.exists() {
        eprintln!(
            "skipping: {} not found -- run `cargo build -p kblockdbcli` first",
            kblockdbcli_bin.display()
        );
        return None;
    }

    let dir = temp_data_dir(tag);
    let server = ManagedServer::spawn(&kblockdbserver_bin, &dir, next_port());
    Some((server, dir, kblockdbcli_bin))
}

fn run_kblockdbcli(kblockdbcli_bin: &Path, server: &ManagedServer, rest: &[&str]) -> Output {
    let mut args = vec![
        "--url",
        &server.url,
        "--user",
        "admin",
        "--password",
        TEST_PASSWORD,
    ];
    args.extend_from_slice(rest);
    Command::new(kblockdbcli_bin)
        .args(&args)
        .output()
        .expect("failed to run kblockdbcli")
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).trim().to_string()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).trim().to_string()
}

#[test]
fn set_then_get_then_remove_round_trips_through_a_real_server() {
    let Some((server, dir, kblockdbcli_bin)) = test_fixture("round-trip") else {
        return;
    };

    let set = run_kblockdbcli(
        &kblockdbcli_bin,
        &server,
        &["set", "1,2,3", "material", "str", "stone"],
    );
    assert!(set.status.success(), "set failed: {}", stderr(&set));

    let get = run_kblockdbcli(&kblockdbcli_bin, &server, &["get", "1,2,3", "material"]);
    assert!(get.status.success(), "get failed: {}", stderr(&get));
    let get_out = stdout(&get);
    assert!(
        get_out.starts_with("str stone (created="),
        "unexpected get output: {get_out}"
    );
    assert!(
        get_out.contains("version=0"),
        "unexpected get output: {get_out}"
    );

    let remove = run_kblockdbcli(&kblockdbcli_bin, &server, &["remove", "1,2,3", "material"]);
    assert!(
        remove.status.success(),
        "remove failed: {}",
        stderr(&remove)
    );

    let get_after_remove =
        run_kblockdbcli(&kblockdbcli_bin, &server, &["get", "1,2,3", "material"]);
    assert!(!get_after_remove.status.success());
    assert!(
        stderr(&get_after_remove).contains("404"),
        "expected a 404 in stderr, got: {}",
        stderr(&get_after_remove)
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn negative_coordinates_round_trip_through_a_real_server() {
    let Some((server, dir, kblockdbcli_bin)) = test_fixture("negative-coords") else {
        return;
    };

    let set = run_kblockdbcli(
        &kblockdbcli_bin,
        &server,
        &["set", "-1,-2,-3", "material", "str", "stone"],
    );
    assert!(set.status.success(), "set failed: {}", stderr(&set));

    let get = run_kblockdbcli(&kblockdbcli_bin, &server, &["get", "-1,-2,-3", "material"]);
    assert!(get.status.success(), "get failed: {}", stderr(&get));
    assert!(
        stdout(&get).starts_with("str stone (created="),
        "unexpected get output: {}",
        stdout(&get)
    );

    let remove = run_kblockdbcli(
        &kblockdbcli_bin,
        &server,
        &["remove", "-1,-2,-3", "material"],
    );
    assert!(
        remove.status.success(),
        "remove failed: {}",
        stderr(&remove)
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn get_reports_an_incrementing_version_after_repeated_sets() {
    let Some((server, dir, kblockdbcli_bin)) = test_fixture("version-increment") else {
        return;
    };

    for value in ["stone", "air", "dirt"] {
        let set = run_kblockdbcli(
            &kblockdbcli_bin,
            &server,
            &["set", "1,2,3", "material", "str", value],
        );
        assert!(set.status.success(), "set failed: {}", stderr(&set));
    }

    let get = run_kblockdbcli(&kblockdbcli_bin, &server, &["get", "1,2,3", "material"]);
    assert!(get.status.success(), "get failed: {}", stderr(&get));
    let get_out = stdout(&get);
    assert!(
        get_out.starts_with("str dirt (created="),
        "unexpected get output: {get_out}"
    );
    assert!(
        get_out.contains("version=2"), // 3 sets total: version 0, 1, 2
        "unexpected get output: {get_out}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn set_then_get_round_trip_numeric_types() {
    let Some((server, dir, kblockdbcli_bin)) = test_fixture("numeric-types") else {
        return;
    };

    // A different key per type: kblockdblib's chunk format fixes a key's value
    // type the first time it's set (see kblockdblib/src/chunk.rs), so reusing one
    // key across incompatible types isn't a case these commands can
    // legitimately round-trip.
    for (value_type, value) in [("i64", "42"), ("f64", "2.6"), ("bool", "true")] {
        let key = format!("n_{value_type}");
        let set = run_kblockdbcli(
            &kblockdbcli_bin,
            &server,
            &["set", "5,5,5", &key, value_type, value],
        );
        assert!(set.status.success(), "set failed: {}", stderr(&set));

        let get = run_kblockdbcli(&kblockdbcli_bin, &server, &["get", "5,5,5", &key]);
        assert!(get.status.success(), "get failed: {}", stderr(&get));
        let get_out = stdout(&get);
        assert!(
            get_out.starts_with(&format!("{value_type} {value} (created=")),
            "unexpected get output: {get_out}"
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn the_wrong_password_is_rejected() {
    let Some((server, dir, kblockdbcli_bin)) = test_fixture("wrong-password") else {
        return;
    };

    let output = Command::new(&kblockdbcli_bin)
        .args([
            "--url",
            &server.url,
            "--user",
            "admin",
            "--password",
            "not-the-real-password",
            "get",
            "1,2,3",
            "material",
        ])
        .output()
        .expect("failed to run kblockdbcli");

    assert!(!output.status.success());
    assert!(
        stderr(&output).contains("401"),
        "expected a 401 in stderr, got: {}",
        stderr(&output)
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn query_select_set_delete_round_trip_through_a_real_server() {
    let Some((server, dir, kblockdbcli_bin)) = test_fixture("query-round-trip") else {
        return;
    };

    let set = run_kblockdbcli(
        &kblockdbcli_bin,
        &server,
        &["set", "1,2,3", "material", "str", "stone"],
    );
    assert!(set.status.success(), "set failed: {}", stderr(&set));

    let select = run_kblockdbcli(
        &kblockdbcli_bin,
        &server,
        &[
            "query",
            "SELECT * FROM (0,0,0) TO (9,9,9) WHERE material = 'stone'",
        ],
    );
    assert!(
        select.status.success(),
        "select failed: {}",
        stderr(&select)
    );
    let select_out = stdout(&select);
    assert!(
        select_out.contains("(1,2,3) material=stone (str)"),
        "unexpected select output: {select_out}"
    );
    assert!(
        select_out.ends_with("1 row(s)"),
        "unexpected select output: {select_out}"
    );

    let set_query = run_kblockdbcli(
        &kblockdbcli_bin,
        &server,
        &[
            "query",
            "SET (material='dirt') WHERE material = 'stone' IN (0,0,0) TO (9,9,9)",
        ],
    );
    assert!(
        set_query.status.success(),
        "set query failed: {}",
        stderr(&set_query)
    );
    assert_eq!(stdout(&set_query), "1 cell(s) affected");

    let get = run_kblockdbcli(&kblockdbcli_bin, &server, &["get", "1,2,3", "material"]);
    assert!(get.status.success(), "get failed: {}", stderr(&get));
    assert!(stdout(&get).starts_with("str dirt (created="));

    let delete_query = run_kblockdbcli(
        &kblockdbcli_bin,
        &server,
        &[
            "query",
            "DELETE WHERE material = 'dirt' IN (0,0,0) TO (9,9,9)",
        ],
    );
    assert!(
        delete_query.status.success(),
        "delete query failed: {}",
        stderr(&delete_query)
    );
    assert_eq!(stdout(&delete_query), "1 cell(s) affected");

    let get_after_delete =
        run_kblockdbcli(&kblockdbcli_bin, &server, &["get", "1,2,3", "material"]);
    assert!(!get_after_delete.status.success());
    assert!(
        stderr(&get_after_delete).contains("404"),
        "expected a 404 in stderr, got: {}",
        stderr(&get_after_delete)
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn set_upserts_a_cell_that_did_not_exist_before() {
    let Some((server, dir, kblockdbcli_bin)) = test_fixture("query-set-upsert") else {
        return;
    };

    // No `set` beforehand -- SET is an upsert, so it must create this cell
    // on its own, unlike UPDATE.
    let set_query = run_kblockdbcli(
        &kblockdbcli_bin,
        &server,
        &["query", "SET (material='stone') IN (1,2,3) TO (2,3,4)"],
    );
    assert!(
        set_query.status.success(),
        "set query failed: {}",
        stderr(&set_query)
    );
    assert_eq!(stdout(&set_query), "1 cell(s) affected");

    let get = run_kblockdbcli(&kblockdbcli_bin, &server, &["get", "1,2,3", "material"]);
    assert!(get.status.success(), "get failed: {}", stderr(&get));
    assert!(stdout(&get).starts_with("str stone (created="));

    // UPDATE, by contrast, must never create a cell.
    let update_query = run_kblockdbcli(
        &kblockdbcli_bin,
        &server,
        &[
            "query",
            "UPDATE (material='stone') IN (9,9,9) TO (10,10,10)",
        ],
    );
    assert!(
        update_query.status.success(),
        "update query failed: {}",
        stderr(&update_query)
    );
    assert_eq!(stdout(&update_query), "0 cell(s) affected");

    let get_never_created =
        run_kblockdbcli(&kblockdbcli_bin, &server, &["get", "9,9,9", "material"]);
    assert!(!get_never_created.status.success());

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_malformed_query_is_rejected_with_the_servers_error_message() {
    let Some((server, dir, kblockdbcli_bin)) = test_fixture("bad-query") else {
        return;
    };

    let output = run_kblockdbcli(&kblockdbcli_bin, &server, &["query", "SELECT FROM WHERE"]);
    assert!(!output.status.success());
    assert!(
        stderr(&output).contains("400"),
        "expected a 400 in stderr, got: {}",
        stderr(&output)
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn an_invalid_value_type_is_rejected_before_any_request_is_sent() {
    let Some((server, dir, kblockdbcli_bin)) = test_fixture("bad-type") else {
        return;
    };

    let output = run_kblockdbcli(
        &kblockdbcli_bin,
        &server,
        &["set", "1,2,3", "material", "complex", "true"],
    );
    assert!(!output.status.success());
    assert!(stderr(&output).contains("complex"));

    let _ = std::fs::remove_dir_all(&dir);
}
