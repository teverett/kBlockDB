//! Integration tests: spawn a real `kdbserver` and drive the actual
//! compiled `kdbcli` binary against it via `std::process::Command`,
//! checking real stdout/exit codes -- not just that `main.rs`'s
//! `unit_tests` compute correctly in isolation. Skips (rather than
//! failing) if either binary hasn't been built yet, since `cargo test -p
//! kdbcli` alone doesn't imply `cargo build -p kdbserver` ran too.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

const TEST_PASSWORD: &str = "kdbcli-test-password";

/// A `kdbserver` child process, killed (and its temp config file removed)
/// on drop -- mirrors `kdbperf/src/server.rs`'s `ManagedServer`, just
/// synchronous throughout since `kdbcli` itself has no async runtime.
struct ManagedServer {
    child: Child,
    config_path: PathBuf,
    url: String,
}

impl ManagedServer {
    fn spawn(kdbserver_bin: &Path, data_dir: &Path, addr: &str) -> ManagedServer {
        let config_path = std::env::temp_dir().join(format!(
            "kdbcli-test-kdbserver-config-{}.toml",
            addr.replace(':', "-")
        ));
        std::fs::write(
            &config_path,
            format!("admin_password = {TEST_PASSWORD:?}\n"),
        )
        .unwrap_or_else(|e| {
            panic!(
                "failed to write kdbserver config {}: {e}",
                config_path.display()
            )
        });

        let child = Command::new(kdbserver_bin)
            .arg("--data-dir")
            .arg(data_dir)
            .arg("--addr")
            .arg(addr)
            .arg("--config")
            .arg(&config_path)
            .stdout(Stdio::null())
            .stderr(Stdio::piped()) // kept, not discarded, so a startup failure is diagnosable
            .spawn()
            .unwrap_or_else(|e| panic!("failed to spawn {}: {e}", kdbserver_bin.display()));

        let url = format!("http://{addr}");
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
            .get(format!("{url}/health"))
            .send()
            .is_ok_and(|r| r.status().is_success())
        {
            return;
        }
        if Instant::now() >= deadline {
            panic!("kdbserver at {url} never became ready within 10s");
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Locates a sibling binary built alongside this test binary -- same
/// two-places trick as `kdbperf/src/server.rs`'s `default_kdbserver_bin`
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

fn next_port() -> u64 {
    NEXT_PORT.fetch_add(1, Ordering::Relaxed)
}

fn temp_data_dir(tag: &str) -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("kdbcli-test-{tag}-{n}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("failed to create temp data dir");
    dir
}

/// Spawns a test server plus locates the `kdbcli` binary under test, or
/// `None` (the test should skip, not fail) if either hasn't been built.
fn test_fixture(tag: &str) -> Option<(ManagedServer, PathBuf, PathBuf)> {
    let kdbserver_bin = find_bin("kdbserver");
    if !kdbserver_bin.exists() {
        eprintln!(
            "skipping: {} not found -- run `cargo build -p kdbserver` first",
            kdbserver_bin.display()
        );
        return None;
    }
    let kdbcli_bin = find_bin("kdbcli");
    if !kdbcli_bin.exists() {
        eprintln!(
            "skipping: {} not found -- run `cargo build -p kdbcli` first",
            kdbcli_bin.display()
        );
        return None;
    }

    let dir = temp_data_dir(tag);
    let server = ManagedServer::spawn(&kdbserver_bin, &dir, &format!("127.0.0.1:{}", next_port()));
    Some((server, dir, kdbcli_bin))
}

fn run_kdbcli(kdbcli_bin: &Path, server: &ManagedServer, rest: &[&str]) -> Output {
    let mut args = vec![
        "--url",
        &server.url,
        "--user",
        "admin",
        "--password",
        TEST_PASSWORD,
    ];
    args.extend_from_slice(rest);
    Command::new(kdbcli_bin)
        .args(&args)
        .output()
        .expect("failed to run kdbcli")
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).trim().to_string()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).trim().to_string()
}

#[test]
fn set_then_get_then_remove_round_trips_through_a_real_server() {
    let Some((server, dir, kdbcli_bin)) = test_fixture("round-trip") else {
        return;
    };

    let set = run_kdbcli(
        &kdbcli_bin,
        &server,
        &["set", "1,2,3", "material", "str", "stone"],
    );
    assert!(set.status.success(), "set failed: {}", stderr(&set));

    let get = run_kdbcli(&kdbcli_bin, &server, &["get", "1,2,3", "material"]);
    assert!(get.status.success(), "get failed: {}", stderr(&get));
    assert_eq!(stdout(&get), "str stone");

    let remove = run_kdbcli(&kdbcli_bin, &server, &["remove", "1,2,3", "material"]);
    assert!(
        remove.status.success(),
        "remove failed: {}",
        stderr(&remove)
    );

    let get_after_remove = run_kdbcli(&kdbcli_bin, &server, &["get", "1,2,3", "material"]);
    assert!(!get_after_remove.status.success());
    assert!(
        stderr(&get_after_remove).contains("404"),
        "expected a 404 in stderr, got: {}",
        stderr(&get_after_remove)
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn set_then_get_round_trip_numeric_types() {
    let Some((server, dir, kdbcli_bin)) = test_fixture("numeric-types") else {
        return;
    };

    // A different key per type: kdb's chunk format fixes a key's value
    // type the first time it's set (see kdb/src/chunk.rs), so reusing one
    // key across incompatible types isn't a case these commands can
    // legitimately round-trip.
    for (value_type, value) in [("i64", "42"), ("f64", "2.6")] {
        let key = format!("n_{value_type}");
        let set = run_kdbcli(
            &kdbcli_bin,
            &server,
            &["set", "5,5,5", &key, value_type, value],
        );
        assert!(set.status.success(), "set failed: {}", stderr(&set));

        let get = run_kdbcli(&kdbcli_bin, &server, &["get", "5,5,5", &key]);
        assert!(get.status.success(), "get failed: {}", stderr(&get));
        assert_eq!(stdout(&get), format!("{value_type} {value}"));
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn the_wrong_password_is_rejected() {
    let Some((server, dir, kdbcli_bin)) = test_fixture("wrong-password") else {
        return;
    };

    let output = Command::new(&kdbcli_bin)
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
        .expect("failed to run kdbcli");

    assert!(!output.status.success());
    assert!(
        stderr(&output).contains("401"),
        "expected a 401 in stderr, got: {}",
        stderr(&output)
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn an_invalid_value_type_is_rejected_before_any_request_is_sent() {
    let Some((server, dir, kdbcli_bin)) = test_fixture("bad-type") else {
        return;
    };

    let output = run_kdbcli(
        &kdbcli_bin,
        &server,
        &["set", "1,2,3", "material", "bool", "true"],
    );
    assert!(!output.status.success());
    assert!(stderr(&output).contains("bool"));

    let _ = std::fs::remove_dir_all(&dir);
}
