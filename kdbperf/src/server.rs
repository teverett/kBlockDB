//! Spawns (and, on drop, tears down) one or more real `kdbserver`
//! processes for the benchmarks to drive over HTTP -- this crate measures
//! what a client actually experiences, not kdb's internals directly.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

/// A `kdbserver` child process. Killed automatically when this drops, so a
/// benchmark run (or a panic partway through one) never leaves a stray
/// server listening.
pub struct ManagedServer {
    child: Child,
    pub url: String,
}

impl ManagedServer {
    /// Spawns `kdbserver_bin` against `data_dir`, listening on `addr`, and
    /// waits (polling `/health`) for it to come up before returning.
    pub async fn spawn(kdbserver_bin: &Path, data_dir: &Path, addr: &str) -> ManagedServer {
        let child = Command::new(kdbserver_bin)
            .arg("--data-dir")
            .arg(data_dir)
            .arg("--addr")
            .arg(addr)
            .stdout(Stdio::null())
            .stderr(Stdio::piped()) // kept, not discarded, so a startup failure is diagnosable
            .spawn()
            .unwrap_or_else(|e| panic!("failed to spawn {}: {e}", kdbserver_bin.display()));

        let url = format!("http://{addr}");
        wait_until_ready(&url).await;
        ManagedServer { child, url }
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
