//! Spawns (and, on drop, tears down) one or more real `kdbserver`
//! processes for the benchmarks to drive over HTTP -- this crate measures
//! what a client actually experiences, not kdb's internals directly.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

/// A `kdbserver` child process. Killed automatically when this drops, so a
/// benchmark run (or a panic partway through one) never leaves a stray
/// server listening. Also cleans up the config file `spawn` wrote it (see
/// there) for kdbserver's now-mandatory `--config`.
pub struct ManagedServer {
    child: Child,
    config_path: PathBuf,
    pub url: String,
}

impl ManagedServer {
    /// Spawns `kdbserver_bin` against `data_dir`, listening on `addr`, and
    /// waits (polling `/health`) for it to come up before returning.
    ///
    /// kdbserver requires a config file with an `admin_password` (its REST
    /// API needs HTTP Basic Auth on everything but `/health`), so this
    /// writes a minimal one -- named after `addr` so concurrently spawned
    /// instances (even ones sharing `data_dir`, as `multi_instance` does)
    /// never collide on the same path -- and points `--config` at it.
    /// Callers authenticate as `admin`/`admin_password` (see `Client`).
    pub async fn spawn(
        kdbserver_bin: &Path,
        data_dir: &Path,
        addr: &str,
        admin_password: &str,
    ) -> ManagedServer {
        let config_path =
            std::env::temp_dir().join(format!("kdbserver-config-{}.toml", addr.replace(':', "-")));
        std::fs::write(
            &config_path,
            format!("admin_password = {admin_password:?}\n"),
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
        wait_until_ready(&url).await;
        ManagedServer {
            child,
            config_path,
            url,
        }
    }
}

async fn wait_until_ready(url: &str) {
    let http = reqwest::Client::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if http
            .get(format!("{url}/health"))
            .send()
            .await
            .is_ok_and(|r| r.status().is_success())
        {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("kdbserver at {url} never became ready within 10s");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

impl Drop for ManagedServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_file(&self.config_path);
    }
}

/// Locates the `kdbserver` binary built alongside this one -- kdbperf and
/// kdbserver come from the same workspace, so `cargo build --release`/
/// `cargo build --workspace` (or plain `cargo run -p kdbperf`) always
/// produces both under the same `target/<profile>/`.
///
/// Checks two places because `current_exe()` doesn't mean the same thing
/// in every context: a plain `cargo run`/direct invocation puts this
/// binary straight in `target/<profile>/`, where `kdbserver` is a sibling;
/// but a `cargo test` binary runs from `target/<profile>/deps/`, one level
/// *below* where `kdbserver` actually is.
pub fn default_kdbserver_bin() -> PathBuf {
    let exe = std::env::current_exe().expect("failed to locate kdbperf's own executable path");
    let exe_dir = exe
        .parent()
        .expect("executable path has no parent directory");
    let name = format!("kdbserver{}", std::env::consts::EXE_SUFFIX);

    let sibling = exe_dir.join(&name);
    if sibling.exists() {
        return sibling;
    }
    if let Some(one_up) = exe_dir.parent() {
        let cousin = one_up.join(&name);
        if cousin.exists() {
            return cousin;
        }
    }
    sibling // doesn't exist either, but gives the caller a sensible path to report
}
