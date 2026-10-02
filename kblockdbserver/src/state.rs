//! Shared server state: every database this server process manages (each
//! one an independent `kblockdblib::World`, lazily opened on first use --
//! see [`Databases`]), and the accounts allowed to use the REST/binary
//! APIs (see `auth.rs`).

use crate::error::ApiError;
use kblockdblib::World;
use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, PoisonError};

/// One configured account: its password and whether it's read-only (see
/// `config::UserConfig::read_only`). `admin` is always full access -- there's
/// no config field to make it read-only, since it's meant as the one
/// account guaranteed to be able to do anything.
#[derive(Clone)]
pub struct Account {
    pub password: String,
    pub read_only: bool,
}

/// The three numbers that fix a *new* database's shape -- see
/// `kblockdblib::params::WorldParams`, which this mirrors field-for-field.
/// Used as `Databases`' default for a database created without an explicit
/// override (REST's `PUT /rest/databases/{name}` body, or the config
/// file's `[worldparameters]`/matching CLI flags) -- the binary protocol's
/// `CreateDatabase` has no way to override this at all, see `wire.rs`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorldShape {
    pub axes: usize,
    pub world_dim: u32,
    pub chunk_dim: u32,
}

/// Every database name must be a plain path segment of its own: no empty
/// name, no `.`/`..`, no path separator, nothing but ASCII
/// alphanumerics/`-`/`_` -- this is what stands between a request-supplied
/// name and `fs::create_dir_all`/`fs::remove_dir_all` on `data_dir.join(name)`,
/// so it's also what rules out escaping `data_dir` entirely (e.g. a name of
/// `..`).
pub fn validate_database_name(name: &str) -> Result<(), ApiError> {
    if name.is_empty() {
        return Err(ApiError::BadRequest(
            "database name must not be empty".to_string(),
        ));
    }
    let valid = name
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_');
    if !valid {
        return Err(ApiError::BadRequest(format!(
            "invalid database name '{name}' -- only ASCII letters, digits, '-', and '_' are allowed"
        )));
    }
    Ok(())
}

/// Every database this server manages, each one an independent `World`
/// rooted at `data_dir/<name>/` -- the multi-database equivalent of what
/// used to be a single `World` rooted directly at `data_dir`.
///
/// Opened lazily and cached, never all at once: `get` only ever touches
/// disk (via `World::open`) the first time a given name is requested in
/// this process, same "load once, cache forever (until evicted)" spirit as
/// `World`'s own chunk cache -- except a `World` itself is cheap to keep
/// open indefinitely (see `World`'s doc comment on why it's `Arc`-shared,
/// not per-request), so there's no eviction here at all. `open` is guarded
/// by a plain `Mutex` on the lookup table only -- never held while a
/// `World`'s own operations run, so two requests to two different
/// (or even the same, once cached) databases never contend on it for long.
///
/// Mirrors `World`'s own "exactly one process, any number of threads, never
/// two `World`s against the same root" rule, just one level up: this
/// process must be the only one with `data_dir` as its `--data-dir`.
#[derive(Clone)]
pub struct Databases {
    data_dir: PathBuf,
    default_shape: WorldShape,
    max_concurrent_disk_ops: Option<usize>,
    max_cached_chunks: Option<usize>,
    compression: bool,
    open: Arc<Mutex<HashMap<String, Arc<World>>>>,
}

impl Databases {
    pub fn new(data_dir: impl Into<PathBuf>, default_shape: WorldShape) -> Self {
        Databases {
            data_dir: data_dir.into(),
            default_shape,
            max_concurrent_disk_ops: None,
            max_cached_chunks: None,
            compression: false,
            open: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn with_max_concurrent_disk_ops(mut self, n: Option<usize>) -> Self {
        self.max_concurrent_disk_ops = n;
        self
    }

    pub fn with_max_cached_chunks(mut self, n: Option<usize>) -> Self {
        self.max_cached_chunks = n;
        self
    }

    pub fn with_compression(mut self, compression: bool) -> Self {
        self.compression = compression;
        self
    }

    pub fn default_shape(&self) -> WorldShape {
        self.default_shape
    }

    fn path(&self, name: &str) -> PathBuf {
        self.data_dir.join(name)
    }

    fn apply_settings(&self, world: World) -> World {
        let mut world = world.with_compression(self.compression);
        if let Some(n) = self.max_concurrent_disk_ops {
            world = world.with_max_concurrent_disk_ops(n);
        }
        if let Some(n) = self.max_cached_chunks {
            world = world.with_max_cached_chunks(n);
        }
        world
    }

    fn table(&self) -> std::sync::MutexGuard<'_, HashMap<String, Arc<World>>> {
        self.open.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Every database that currently exists on disk under `data_dir`, sorted
    /// by name -- a live scan (one level deep, each entry checked for its
    /// own `world.txt`), not a cache, so it reflects databases created or
    /// removed by this process (or, like `World::stats`, a concurrent one)
    /// since the last call, including ones this process has never opened.
    pub fn list(&self) -> io::Result<Vec<String>> {
        let mut names = Vec::new();
        let entries = match fs::read_dir(&self.data_dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(names),
            Err(e) => return Err(e),
        };
        for entry in entries {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            if entry.path().join("world.txt").is_file() {
                if let Some(name) = entry.file_name().to_str() {
                    names.push(name.to_string());
                }
            }
        }
        names.sort();
        Ok(names)
    }

    /// The named database, opening it from disk (and caching it) on first
    /// touch. `NotFound` if no database by this name has ever been created
    /// -- `get` never creates one implicitly; see `create`.
    pub fn get(&self, name: &str) -> Result<Arc<World>, ApiError> {
        validate_database_name(name)?;
        // Held across the `World::open` below, not just the lookup -- two
        // threads racing the very first touch of the same not-yet-cached
        // database must never both call `World::open` on the same root at
        // once (see `World`'s own "never two `World`s against one root"
        // rule). The cost is that two threads opening two *different*
        // databases for the first time at the same moment also serialize
        // here; past each database's first touch, every later `get` is a
        // plain, fast map lookup.
        let mut table = self.table();
        if let Some(world) = table.get(name) {
            return Ok(world.clone());
        }
        let path = self.path(name);
        if !path.join("world.txt").is_file() {
            return Err(ApiError::NotFound(format!(
                "no such database '{name}' -- create it first"
            )));
        }
        let world = Arc::new(self.apply_settings(World::open(&path)?));
        Ok(table.entry(name.to_string()).or_insert(world).clone())
    }

    /// Creates a brand new database named `name`, shaped `shape` (or this
    /// `Databases`' configured default, if `None`). `Conflict` if one by
    /// this name already exists -- unlike `World::create`, which treats a
    /// matching re-`create` as a no-op, database creation here is meant as
    /// an explicit, one-time provisioning step (REST `PUT
    /// /rest/databases/{name}`, the binary protocol's `CreateDatabase`), so
    /// a second call is almost always a caller mistake worth surfacing
    /// rather than silently accepting.
    pub fn create(&self, name: &str, shape: Option<WorldShape>) -> Result<(), ApiError> {
        validate_database_name(name)?;
        let path = self.path(name);
        // Held across the existence check, `World::create`, and the insert
        // -- same "don't let two racing callers both think they're first"
        // reasoning as `get`'s own lock span.
        let mut table = self.table();
        if path.join("world.txt").is_file() || table.contains_key(name) {
            return Err(ApiError::Conflict(format!(
                "database '{name}' already exists"
            )));
        }
        let shape = shape.unwrap_or(self.default_shape);
        let world = Arc::new(self.apply_settings(World::create(
            &path,
            shape.axes,
            shape.world_dim,
            shape.chunk_dim,
        )?));
        table.insert(name.to_string(), world);
        drop(table);
        kblockdblib::logger::info(format!("created database '{name}' at {}", path.display()));
        Ok(())
    }

    /// Deletes database `name` -- its directory and every byte of data in
    /// it -- and drops it from the open-database cache. Returns `false`,
    /// having changed nothing, if there was no such database.
    ///
    /// Irreversible, same as `World::remove_column`, and carries the same
    /// "don't run two `World`s against one root at once" hazard as any
    /// concurrent access to a `World` this process still has open elsewhere
    /// -- there is no coordination with an in-flight request against this
    /// same database beyond what dropping its cache entry here already
    /// gives.
    pub fn remove(&self, name: &str) -> Result<bool, ApiError> {
        validate_database_name(name)?;
        self.table().remove(name);
        let path = self.path(name);
        if !path.join("world.txt").is_file() {
            return Ok(false);
        }
        fs::remove_dir_all(&path)?;
        kblockdblib::logger::info(format!("removed database '{name}' at {}", path.display()));
        Ok(true)
    }
}

/// No `Mutex` on a database's `World` on purpose. `World`'s own methods
/// take `&self` and are safe to call concurrently from many threads of
/// *this one process* -- its only interior state is a locked `Schema`, a
/// lazily-populated table of per-chunk `RwLock`s, and a couple of atomic
/// counters (see `kblockdblib`'s "Concurrency" doc comment on `World`). A
/// `Mutex<World>` here would serialize every request through one lock
/// regardless of which chunk (or even which database) it touched, throwing
/// that away.
///
/// This server must be the only process with `--data-dir` open -- `World`'s
/// locking is in-process only, not OS-level, so a second `kblockdbserver`
/// (or anything else) pointed at the same `--data-dir` at the same time
/// would race it with no coordination at all and can corrupt data. Run
/// exactly one `kblockdbserver` per data directory; scale by giving it more
/// threads (it already uses as many as the async runtime has, see
/// `with_database`), not by running more of it.
#[derive(Clone)]
pub struct AppState {
    pub databases: Databases,
    /// What `/rest/health` (and the binary protocol's `Health`) reports as
    /// this instance's name -- see [`AppState::with_hostname`].
    pub hostname: Arc<str>,
    /// `None` unless clustering is configured (a `cluster_secret` is set --
    /// see `config::Config`) -- every write call site in `routes.rs`/
    /// `binary_server.rs` publishes through this when it's `Some`, and is a
    /// no-op otherwise, so an unclustered server behaves exactly as it did
    /// before this field existed. See `kblockdbcluster::hub` and
    /// `cluster.rs` (this server's `ReplicationSink` impl, which
    /// `kblockdbcluster::server::serve` applies incoming changes through).
    pub replication: Option<Arc<kblockdbcluster::hub::ReplicationHub>>,
    /// The `address` of every configured `[[peers]]` entry -- empty
    /// unless clustering is configured. Reported by `/rest/health` so a
    /// caller can see this instance's cluster membership without reading
    /// its config file. Just the configured list, not live connection
    /// status: `kblockdbcluster::client::run` doesn't report its own
    /// connected/reconnecting state back anywhere today.
    pub peers: Arc<Vec<String>>,
    credentials: Arc<HashMap<String, Account>>,
}

impl AppState {
    pub fn new(databases: Databases, credentials: Arc<HashMap<String, Account>>) -> Self {
        AppState {
            databases,
            hostname: Arc::from(os_hostname()),
            replication: None,
            peers: Arc::new(Vec::new()),
            credentials,
        }
    }

    /// Overrides the reported hostname, for a deployment where the OS
    /// hostname isn't the name callers should see -- several instances
    /// behind one load balancer, say, where what identifies the instance
    /// that answered is a name the orchestrator assigned rather than
    /// whatever `gethostname` returns. `kblockdbserver`'s `hostname`
    /// config key is the only caller.
    pub fn with_hostname(mut self, hostname: String) -> Self {
        self.hostname = Arc::from(hostname);
        self
    }

    /// Enables replication -- called once at startup, only when the
    /// config sets a `cluster_secret`. See this struct's `replication`
    /// field doc comment.
    pub fn with_replication(mut self, hub: Arc<kblockdbcluster::hub::ReplicationHub>) -> Self {
        self.replication = Some(hub);
        self
    }

    /// Records this instance's configured peer addresses, for
    /// `/rest/health` to report. See this struct's `peers` field doc
    /// comment.
    pub fn with_peers(mut self, peers: Vec<String>) -> Self {
        self.peers = Arc::new(peers);
        self
    }

    /// The account `username`/`password` matches, if any. The password
    /// comparison is constant-time so a request can't learn anything about
    /// the real password from how long the check took; the username lookup
    /// itself isn't (usernames aren't secret).
    pub fn authenticate(&self, username: &str, password: &str) -> Option<Account> {
        let account = self.credentials.get(username)?;
        constant_time_eq(account.password.as_bytes(), password.as_bytes()).then(|| account.clone())
    }

    /// Resolves database `name` (see `Databases::get`), then runs `f`
    /// against it on a blocking-task thread pool thread, not on the async
    /// runtime's own worker threads.
    ///
    /// `World`'s operations do synchronous file I/O (`std::fs`); running
    /// that directly inside an `async fn` handler would block whichever
    /// tokio worker thread happened to be running it, stalling every other
    /// request that worker was multiplexing. `spawn_blocking` moves it to a
    /// thread pool meant for exactly this -- and since no database needs an
    /// external lock, many of these can run at once, including against
    /// different databases entirely.
    pub async fn with_database<T, F>(&self, name: &str, f: F) -> Result<T, ApiError>
    where
        T: Send + 'static,
        F: FnOnce(&World) -> std::io::Result<T> + Send + 'static,
    {
        let databases = self.databases.clone();
        let name = name.to_string();
        tokio::task::spawn_blocking(move || {
            let world = databases.get(&name)?;
            f(&world).map_err(ApiError::from)
        })
        .await
        .expect("worker thread panicked")
    }

    /// Resolves database `name` without running anything against it -- same
    /// blocking-task-thread reasoning as `with_database`, for a caller (just
    /// `Hello`, today) that only needs to know whether/how a database
    /// exists, not to perform an operation on it.
    pub async fn resolve_database(&self, name: &str) -> Result<Arc<World>, ApiError> {
        let databases = self.databases.clone();
        let name = name.to_string();
        tokio::task::spawn_blocking(move || databases.get(&name))
            .await
            .expect("worker thread panicked")
    }

    /// Every database that currently exists, on a blocking-task thread --
    /// see `with_database`'s doc comment on why; `Databases::list` does a
    /// real directory scan, same disk-I/O-off-the-async-runtime reasoning
    /// applies.
    pub async fn list_databases(&self) -> Result<Vec<String>, ApiError> {
        let databases = self.databases.clone();
        tokio::task::spawn_blocking(move || databases.list())
            .await
            .expect("worker thread panicked")
            .map_err(ApiError::from)
    }

    /// Creates database `name` (see `Databases::create`) on a blocking-task
    /// thread.
    pub async fn create_database(
        &self,
        name: &str,
        shape: Option<WorldShape>,
    ) -> Result<(), ApiError> {
        let databases = self.databases.clone();
        let name = name.to_string();
        tokio::task::spawn_blocking(move || databases.create(&name, shape))
            .await
            .expect("worker thread panicked")
    }

    /// Deletes database `name` (see `Databases::remove`) on a blocking-task
    /// thread.
    pub async fn remove_database(&self, name: &str) -> Result<bool, ApiError> {
        let databases = self.databases.clone();
        let name = name.to_string();
        tokio::task::spawn_blocking(move || databases.remove(&name))
            .await
            .expect("worker thread panicked")
    }
}

/// This machine's hostname, or `"unknown"` if the OS won't say (or says
/// something that isn't UTF-8). Deliberately not an error: `/rest/health`
/// is the endpoint a load balancer polls to decide whether this process
/// is alive, so failing to name the host must never be what makes it look
/// unhealthy.
pub fn os_hostname() -> String {
    hostname::get()
        .ok()
        .and_then(|name| name.into_string().ok())
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| "unknown".to_string())
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
    /// -- `AppState::new`/`Databases::new` need a real directory on disk,
    /// even for a test that never actually creates a database in it.
    struct TestState {
        state: AppState,
        dir: std::path::PathBuf,
    }

    impl Drop for TestState {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    const SHAPE: WorldShape = WorldShape {
        axes: 1,
        world_dim: 100,
        chunk_dim: 32,
    };

    fn state_with_accounts() -> TestState {
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "kblockdbserver-state-test-{n}-{}",
            std::process::id()
        ));
        let databases = Databases::new(&dir, SHAPE);
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
            state: AppState::new(databases, Arc::new(credentials)),
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
    fn os_hostname_is_never_empty() {
        // Whatever this machine is called -- and even if the OS won't say
        // at all -- health must always have a name to report.
        assert!(!os_hostname().is_empty());
    }

    #[test]
    fn app_state_defaults_its_hostname_to_the_os_hostname() {
        let test = state_with_accounts();
        assert_eq!(&*test.state.hostname, os_hostname().as_str());
    }

    #[test]
    fn with_hostname_overrides_the_os_hostname() {
        let test = state_with_accounts();
        let state = test
            .state
            .clone()
            .with_hostname("db-1.example.com".to_string());
        assert_eq!(&*state.hostname, "db-1.example.com");
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

    // --- Databases ---

    #[test]
    fn validate_database_name_rejects_empty() {
        assert!(validate_database_name("").is_err());
    }

    #[test]
    fn validate_database_name_rejects_path_traversal() {
        assert!(validate_database_name("..").is_err());
        assert!(validate_database_name(".").is_err());
        assert!(validate_database_name("a/b").is_err());
        assert!(validate_database_name("../escape").is_err());
    }

    #[test]
    fn validate_database_name_accepts_alphanumeric_dash_underscore() {
        assert!(validate_database_name("my-db_1").is_ok());
        assert!(validate_database_name("ABC123").is_ok());
    }

    #[test]
    fn validate_database_name_rejects_other_punctuation() {
        assert!(validate_database_name("a b").is_err());
        assert!(validate_database_name("a.b").is_err());
        assert!(validate_database_name("a:b").is_err());
    }

    #[test]
    fn list_on_a_fresh_data_dir_is_empty() {
        let test = state_with_accounts();
        assert_eq!(test.state.databases.list().unwrap(), Vec::<String>::new());
    }

    #[test]
    fn get_before_create_is_not_found() {
        let test = state_with_accounts();
        assert!(matches!(
            test.state.databases.get("foo"),
            Err(ApiError::NotFound(_))
        ));
    }

    #[test]
    fn create_then_list_then_get_round_trips() {
        let test = state_with_accounts();
        test.state.databases.create("foo", None).unwrap();
        assert_eq!(test.state.databases.list().unwrap(), vec!["foo"]);
        let world = test.state.databases.get("foo").unwrap();
        assert_eq!(world.axes(), SHAPE.axes);
        assert_eq!(world.world_dim(), SHAPE.world_dim);
    }

    #[test]
    fn create_honors_an_explicit_shape_override() {
        let test = state_with_accounts();
        let shape = WorldShape {
            axes: 2,
            world_dim: 50,
            chunk_dim: 16,
        };
        test.state.databases.create("foo", Some(shape)).unwrap();
        let world = test.state.databases.get("foo").unwrap();
        assert_eq!(world.axes(), 2);
        assert_eq!(world.world_dim(), 50);
    }

    #[test]
    fn create_twice_is_a_conflict() {
        let test = state_with_accounts();
        test.state.databases.create("foo", None).unwrap();
        assert!(matches!(
            test.state.databases.create("foo", None),
            Err(ApiError::Conflict(_))
        ));
    }

    #[test]
    fn create_rejects_an_invalid_name_without_touching_disk() {
        let test = state_with_accounts();
        assert!(matches!(
            test.state.databases.create("..", None),
            Err(ApiError::BadRequest(_))
        ));
        assert_eq!(test.state.databases.list().unwrap(), Vec::<String>::new());
    }

    #[test]
    fn remove_of_an_unknown_database_returns_false() {
        let test = state_with_accounts();
        assert!(!test.state.databases.remove("nope").unwrap());
    }

    #[test]
    fn create_then_remove_then_get_is_not_found_again() {
        let test = state_with_accounts();
        test.state.databases.create("foo", None).unwrap();
        assert!(test.state.databases.remove("foo").unwrap());
        assert!(matches!(
            test.state.databases.get("foo"),
            Err(ApiError::NotFound(_))
        ));
        assert_eq!(test.state.databases.list().unwrap(), Vec::<String>::new());
    }

    #[test]
    fn two_databases_are_independent_on_disk() {
        let test = state_with_accounts();
        test.state.databases.create("a", None).unwrap();
        test.state.databases.create("b", None).unwrap();

        let world_a = test.state.databases.get("a").unwrap();
        let world_b = test.state.databases.get("b").unwrap();
        world_a.set(&[0], "k", kblockdblib::Value::I64(1)).unwrap();
        world_b.set(&[0], "k", kblockdblib::Value::I64(2)).unwrap();

        assert_eq!(
            world_a.get(&[0], "k").unwrap(),
            Some(kblockdblib::Value::I64(1))
        );
        assert_eq!(
            world_b.get(&[0], "k").unwrap(),
            Some(kblockdblib::Value::I64(2))
        );
        assert_eq!(test.state.databases.list().unwrap(), vec!["a", "b"]);

        assert!(test.state.databases.remove("a").unwrap());
        assert_eq!(test.state.databases.list().unwrap(), vec!["b"]);
        assert_eq!(
            world_b.get(&[0], "k").unwrap(),
            Some(kblockdblib::Value::I64(2))
        );
    }
}
