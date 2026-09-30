//! Shared server state: the one `World` this server process manages, and
//! the accounts allowed to use its REST API (see `auth.rs`).

use crate::error::ApiError;
use kblockdblib::World;
use std::collections::HashMap;
use std::sync::Arc;

/// One configured account: its password and whether it's read-only (see
/// `config::UserConfig::read_only`). `admin` is always full access -- there's
/// no config field to make it read-only, since it's meant as the one
/// account guaranteed to be able to do anything.
#[derive(Clone)]
pub struct Account {
    pub password: String,
    pub read_only: bool,
}

/// No `Mutex` on `world` on purpose. `World`'s own methods take `&self` and
/// are safe to call concurrently from many threads of *this one process* --
/// its only interior state is a locked `Schema`, a lazily-populated table
/// of per-chunk `RwLock`s, and a couple of atomic counters (see
/// `kblockdblib`'s "Concurrency" doc comment on `World`). A `Mutex<World>`
/// here would serialize every request through one lock regardless of which
/// chunk it touched, throwing that away -- two requests to unrelated cells
/// would contend for no reason.
///
/// This server must be the only process with this `World`'s data directory
/// open -- `World`'s locking is in-process only now, not OS-level, so a
/// second `kblockdbserver` (or anything else) pointed at the same
/// `--data-dir` at the same time would race it with no coordination at all
/// and can corrupt data. Run exactly one `kblockdbserver` per data
/// directory; scale by giving it more threads (it already uses as many as
/// the async runtime has, see `with_world`), not by running more of it.
#[derive(Clone)]
pub struct AppState {
    pub world: Arc<World>,
    credentials: Arc<HashMap<String, Account>>,
}

impl AppState {
    pub fn new(world: World, credentials: Arc<HashMap<String, Account>>) -> Self {
        AppState {
            world: Arc::new(world),
            credentials,
        }
    }

    /// The account `username`/`password` matches, if any. The password
    /// comparison is constant-time so a request can't learn anything about
    /// the real password from how long the check took; the username lookup
    /// itself isn't (usernames aren't secret).
    pub fn authenticate(&self, username: &str, password: &str) -> Option<Account> {
        let account = self.credentials.get(username)?;
        constant_time_eq(account.password.as_bytes(), password.as_bytes()).then(|| account.clone())
    }

    /// Runs `f` against the shared `World` on a blocking-task thread pool
    /// thread, not on the async runtime's own worker threads.
    ///
    /// `World`'s operations do synchronous file I/O (`std::fs`); running
    /// that directly inside an `async fn` handler would block whichever
    /// tokio worker thread happened to be running it, stalling every other
    /// request that worker was multiplexing. `spawn_blocking` moves it to a
    /// thread pool meant for exactly this -- and since `World` needs no
    /// external lock, many of these can run at once.
    pub async fn with_world<T, F>(&self, f: F) -> Result<T, ApiError>
    where
        T: Send + 'static,
        F: FnOnce(&World) -> std::io::Result<T> + Send + 'static,
    {
        let world = self.world.clone();
        tokio::task::spawn_blocking(move || f(&world))
            .await
            .expect("worker thread panicked")
            .map_err(ApiError::from)
    }
}

/// Compares two byte strings in time that depends only on their lengths,
/// not on where they first differ -- a plain `==` short-circuits at the
/// first mismatching byte, which a network attacker can in principle use
/// to recover a password one byte at a time by timing responses.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b) {
        diff |= x ^ y;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Cleans up its temp directory on drop, same as `tests.rs`'s `TestDir`
    /// -- `AppState::new` needs a real `World`, and `World` needs a real
    /// directory on disk, even for a test that never touches any cells.
    struct TestState {
        state: AppState,
        dir: std::path::PathBuf,
    }

    impl Drop for TestState {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    fn state_with_accounts() -> TestState {
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "kblockdbserver-state-test-{n}-{}",
            std::process::id()
        ));
        let world = World::create(&dir, 1, 1, 32).unwrap();
        let mut credentials = HashMap::new();
        credentials.insert(
            "admin".to_string(),
            Account {
                password: "admin-pw".to_string(),
                read_only: false,
            },
        );
        credentials.insert(
            "viewer".to_string(),
            Account {
                password: "viewer-pw".to_string(),
                read_only: true,
            },
        );
        TestState {
            state: AppState::new(world, Arc::new(credentials)),
            dir,
        }
    }

    #[test]
    fn authenticate_accepts_correct_credentials_and_reports_the_account_role() {
        let test_state = state_with_accounts();
        let admin = test_state.state.authenticate("admin", "admin-pw").unwrap();
        assert!(!admin.read_only);
        let viewer = test_state
            .state
            .authenticate("viewer", "viewer-pw")
            .unwrap();
        assert!(viewer.read_only);
    }

    #[test]
    fn authenticate_rejects_wrong_password() {
        let test_state = state_with_accounts();
        assert!(test_state.state.authenticate("admin", "wrong").is_none());
    }

    #[test]
    fn authenticate_rejects_unknown_username() {
        let test_state = state_with_accounts();
        assert!(test_state
            .state
            .authenticate("nobody", "whatever")
            .is_none());
    }

    #[test]
    fn constant_time_eq_matches_equal_slices() {
        assert!(constant_time_eq(b"hunter2", b"hunter2"));
    }

    #[test]
    fn constant_time_eq_rejects_different_content_of_the_same_length() {
        assert!(!constant_time_eq(b"hunter2", b"hunter3"));
    }

    #[test]
    fn constant_time_eq_rejects_different_lengths() {
        assert!(!constant_time_eq(b"short", b"a-lot-longer"));
    }

    #[test]
    fn constant_time_eq_treats_empty_slices_as_equal() {
        assert!(constant_time_eq(b"", b""));
    }
}
