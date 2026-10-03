//! The `--config <path>` TOML file: the port/address to listen on, the
//! data directory, and the accounts (admin + regular users) allowed to
//! use the REST API. Kept separate from `main.rs`'s CLI parsing since it
//! has its own file-format concerns (parsing, validation) and is unit
//! tested against raw TOML text rather than real files.
//!
//! Deliberately config-file-only, not a CLI flag: putting passwords on the
//! command line leaks them into shell history and `ps` output.

use crate::state::Account;
use kblockdbcluster::socket::Keepalive;
use serde::Deserialize;
use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

#[derive(Debug, Deserialize)]
pub struct UserConfig {
    pub username: String,
    pub password: String,
    /// If true, this account can only `GET` -- `PUT`/`DELETE` get `403`.
    /// Defaults to false (full read/write), matching every account before
    /// this field existed.
    #[serde(default)]
    pub read_only: bool,
}

/// One `[[peers]]` entry: another `kblockdbserver` instance to replicate
/// local writes to -- see `replication.rs`/`peer_client.rs`. `address` is
/// a `host:port` pair naming that peer's *peer* port (its own `peer_port`
/// config/`--peer-port` flag, not its HTTP or binary-protocol port).
#[derive(Debug, Deserialize, Clone)]
pub struct PeerConfig {
    pub address: String,
}

/// The `[cluster]` table: `peer_port`/`cluster_secret`, grouped under
/// their own table rather than flat keys on `Config` -- same
/// "only ever make sense together" reasoning as `[worldparameters]`'s own
/// doc comment, applied here to clustering's pair instead of a world's
/// shape. `[[peers]]` stays its own top-level array-of-tables rather than
/// nested under here: it's a list of however many peers, not a second
/// member of this pair.
#[derive(Debug, Deserialize, Default)]
pub struct ClusterConfig {
    /// The port the peer-replication protocol listens on -- see
    /// `peer_server.rs`. Like `binary_port`, has no default: clustering
    /// stays off unless `cluster_secret` is also set (see
    /// `Config::validate`).
    #[serde(default)]
    pub peer_port: Option<u16>,
    /// Shared secret every peer connection (both directions) is checked
    /// against -- see `peer_server.rs`'s `Hello` handling. Required (and
    /// must be non-empty) if `[[peers]]` is non-empty; clustering is
    /// entirely opt-in and disabled by default (no `[[peers]]`, no
    /// `cluster_secret`).
    #[serde(default)]
    pub cluster_secret: Option<String>,
    /// How long a peer *learned* at runtime (it connected in, or was
    /// gossiped) may stay continuously unreachable before it's dropped --
    /// see `kblockdbcluster::peers::PeerSet::prune`. Configured
    /// `[[peers]]` are never dropped. Defaults to
    /// `DEFAULT_DEAD_PEER_TIMEOUT_SECS`; `0` disables dropping entirely.
    #[serde(default)]
    pub dead_peer_timeout_secs: Option<u64>,
    /// TCP keepalive on every peer link (see `kblockdbcluster::socket`):
    /// seconds a link may be idle before the first probe. Defaults to 30.
    #[serde(default)]
    pub keepalive_idle_secs: Option<u64>,
    /// Seconds between unanswered keepalive probes. Defaults to 10.
    #[serde(default)]
    pub keepalive_interval_secs: Option<u64>,
    /// Unanswered keepalive probes before a link is dropped. Defaults to 3.
    #[serde(default)]
    pub keepalive_retries: Option<u32>,
    /// How long a deleted cell's tombstone is kept (see
    /// `World::with_tombstone_retention`) -- and so the longest a node can
    /// be offline and still catch up correctly on deletes. Defaults to
    /// `DEFAULT_TOMBSTONE_RETENTION_SECS`; must be greater than 0.
    #[serde(default)]
    pub tombstone_retention_secs: Option<u64>,
    /// How far before a peer's watermark a catch-up starts -- see
    /// `kblockdbcluster::peers::PeerSet::with_catch_up_margin`. Defaults to
    /// 60.
    #[serde(default)]
    pub catch_up_margin_secs: Option<u64>,
}

/// `ClusterConfig::tombstone_retention_secs`' default: a week.
pub const DEFAULT_TOMBSTONE_RETENTION_SECS: u64 = 7 * 24 * 60 * 60;

impl ClusterConfig {
    /// The keepalive settings for peer links: this table's values, each
    /// falling back to `kblockdbcluster::socket::Keepalive`'s default.
    pub fn keepalive(&self) -> Keepalive {
        let defaults = Keepalive::default();
        Keepalive {
            idle: self
                .keepalive_idle_secs
                .map_or(defaults.idle, Duration::from_secs),
            interval: self
                .keepalive_interval_secs
                .map_or(defaults.interval, Duration::from_secs),
            retries: self.keepalive_retries.unwrap_or(defaults.retries),
        }
    }

    pub fn tombstone_retention(&self) -> Duration {
        Duration::from_secs(
            self.tombstone_retention_secs
                .unwrap_or(DEFAULT_TOMBSTONE_RETENTION_SECS),
        )
    }

    pub fn catch_up_margin(&self) -> Duration {
        self.catch_up_margin_secs.map_or(
            kblockdbcluster::peers::DEFAULT_CATCH_UP_MARGIN,
            Duration::from_secs,
        )
    }
}

/// `ClusterConfig::dead_peer_timeout_secs`' default: five minutes -- long
/// enough to ride out a restart or a brief network blip, short enough that
/// a node that's really gone stops being retried by everyone.
pub const DEFAULT_DEAD_PEER_TIMEOUT_SECS: u64 = 300;

/// The `[worldparameters]` table: the three numbers that fix a *new*
/// world's shape (see `kblockdblib::params::WorldParams`) -- meaningless,
/// and ignored, when reopening an existing one (`World::open` reads the
/// real shape back from `world.txt` instead; see `main.rs`). Grouped under
/// their own table, rather than flat keys on `Config` like `http_port`/
/// `data_dir`, because the three only ever make sense together -- a config
/// setting one without the others is a different kind of setting than one
/// setting only `binary_port`, say.
///
/// The field name `worldparameters` (one word, not `world_parameters`) is
/// deliberate: it's the literal TOML table name below, and `serde` matches
/// TOML keys to field names by exact spelling with no `rename` in play.
#[derive(Debug, Deserialize, Default)]
pub struct WorldParametersConfig {
    #[serde(default)]
    pub axes: Option<usize>,
    #[serde(default)]
    pub world_dim: Option<u32>,
    #[serde(default)]
    pub chunk_size: Option<u32>,
}

#[derive(Debug, Deserialize)]
pub struct Config {
    /// The port the REST API listens on. There's deliberately no way to
    /// configure the bind *host*: the server always binds every
    /// interface (see `main.rs`), so the only question left is which
    /// port.
    #[serde(default)]
    pub http_port: Option<u16>,
    /// The port the binary protocol listens on. Unlike `http_port` this
    /// has no default -- the binary protocol stays off unless a port is
    /// named.
    #[serde(default)]
    pub binary_port: Option<u16>,
    #[serde(default)]
    pub data_dir: Option<String>,
    #[serde(default)]
    pub worldparameters: WorldParametersConfig,
    #[serde(default)]
    pub max_concurrent_disk_ops: Option<usize>,
    #[serde(default)]
    pub max_cached_chunks: Option<usize>,
    /// If true, every chunk file this server writes is zstd-compressed
    /// (see `kblockdblib::World::with_compression`). Defaults to false,
    /// matching every world written before this flag existed. Safe to
    /// flip either way on an existing world: reads detect each file's
    /// encoding individually.
    #[serde(default)]
    pub compression: bool,
    /// What `/rest/health` reports as this instance's name. Defaults to
    /// the OS hostname; set this when that isn't the name callers should
    /// see -- several instances behind one load balancer, say. An empty
    /// string is rejected rather than silently reporting nothing.
    #[serde(default)]
    pub hostname: Option<String>,
    pub admin_password: String,
    #[serde(default)]
    pub users: Vec<UserConfig>,
    /// `peer_port`/`cluster_secret` -- see `ClusterConfig`'s doc comment
    /// for why these two are grouped under their own table.
    #[serde(default)]
    pub cluster: ClusterConfig,
    /// Other servers to replicate local writes to -- see `replication.rs`.
    #[serde(default)]
    pub peers: Vec<PeerConfig>,
}

impl Config {
    /// Reads and parses the config file at `path`, then validates it (see
    /// [`Config::validate`]). The one place callers need to handle both
    /// I/O and parse/validation failures, all as one displayable error.
    pub fn load(path: &Path) -> Result<Config, String> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| format!("failed to read config file {}: {e}", path.display()))?;
        Config::from_toml_str(&text)
            .map_err(|e| format!("invalid config file {}: {e}", path.display()))
    }

    fn from_toml_str(text: &str) -> Result<Config, String> {
        let config: Config = toml::from_str(text).map_err(|e| e.to_string())?;
        config.validate()?;
        Ok(config)
    }

    /// `admin_password` must be set (the REST API has no other way in),
    /// every user needs a non-empty password, usernames must be unique,
    /// and `admin` is reserved for `admin_password` itself -- a `[[users]]`
    /// entry named `admin` would silently be unreachable otherwise.
    fn validate(&self) -> Result<(), String> {
        if self.admin_password.is_empty() {
            return Err("admin_password must not be empty".to_string());
        }
        if self.hostname.as_deref().is_some_and(str::is_empty) {
            return Err("hostname must not be empty".to_string());
        }
        if !self.peers.is_empty() {
            match self.cluster.cluster_secret.as_deref() {
                None | Some("") => {
                    return Err(
                        "[cluster].cluster_secret must be set (and non-empty) to use [[peers]]"
                            .to_string(),
                    )
                }
                Some(_) => {}
            }
        }
        for (name, value) in [
            ("keepalive_idle_secs", self.cluster.keepalive_idle_secs),
            (
                "keepalive_interval_secs",
                self.cluster.keepalive_interval_secs,
            ),
            (
                "keepalive_retries",
                self.cluster.keepalive_retries.map(u64::from),
            ),
            (
                "tombstone_retention_secs",
                self.cluster.tombstone_retention_secs,
            ),
        ] {
            if value == Some(0) {
                return Err(format!("[cluster].{name} must be greater than 0"));
            }
        }
        let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();
        seen.insert("admin");
        for user in &self.users {
            if user.username == "admin" {
                return Err(
                    "'admin' is reserved for admin_password; it can't also appear in [[users]]"
                        .to_string(),
                );
            }
            if user.password.is_empty() {
                return Err(format!("user '{}' has an empty password", user.username));
            }
            if !seen.insert(user.username.as_str()) {
                return Err(format!("duplicate username '{}'", user.username));
            }
        }
        Ok(())
    }

    /// Every account this config grants API access to, `admin` included.
    pub fn credentials(&self) -> HashMap<String, Account> {
        let mut map = HashMap::with_capacity(self.users.len() + 1);
        map.insert(
            "admin".to_string(),
            Account {
                password: self.admin_password.clone(),
                read_only: false,
            },
        );
        for user in &self.users {
            map.insert(
                user.username.clone(),
                Account {
                    password: user.password.clone(),
                    read_only: user.read_only,
                },
            );
        }
        map
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_minimal_config() {
        let config = Config::from_toml_str(r#"admin_password = "secret""#).unwrap();
        assert_eq!(config.admin_password, "secret");
        assert!(config.http_port.is_none());
        assert!(config.binary_port.is_none());
        assert!(config.users.is_empty());
        assert!(config.worldparameters.axes.is_none());
        assert!(config.worldparameters.world_dim.is_none());
        assert!(config.worldparameters.chunk_size.is_none());
    }

    #[test]
    fn parses_every_field() {
        let config = Config::from_toml_str(
            r#"
            http_port = 9090
            binary_port = 9091
            data_dir = "/var/lib/kblockdblib"
            max_concurrent_disk_ops = 16
            max_cached_chunks = 5000
            admin_password = "secret"

            [worldparameters]
            axes = 4
            world_dim = 500
            chunk_size = 16

            [[users]]
            username = "alice"
            password = "alice-pw"

            [[users]]
            username = "bob"
            password = "bob-pw"
            "#,
        )
        .unwrap();
        assert_eq!(config.http_port, Some(9090));
        assert_eq!(config.binary_port, Some(9091));
        assert_eq!(config.data_dir.as_deref(), Some("/var/lib/kblockdblib"));
        assert_eq!(config.worldparameters.axes, Some(4));
        assert_eq!(config.worldparameters.world_dim, Some(500));
        assert_eq!(config.worldparameters.chunk_size, Some(16));
        assert_eq!(config.max_concurrent_disk_ops, Some(16));
        assert_eq!(config.max_cached_chunks, Some(5000));
        assert_eq!(config.users.len(), 2);
    }

    #[test]
    fn worldparameters_table_can_be_omitted_entirely() {
        let config = Config::from_toml_str(
            r#"
            admin_password = "secret"
            max_cached_chunks = 5000
            "#,
        )
        .unwrap();
        assert!(config.worldparameters.axes.is_none());
        assert!(config.worldparameters.world_dim.is_none());
        assert!(config.worldparameters.chunk_size.is_none());
    }

    #[test]
    fn worldparameters_table_can_set_only_some_of_its_fields() {
        let config = Config::from_toml_str(
            r#"
            admin_password = "secret"
            [worldparameters]
            chunk_size = 8
            "#,
        )
        .unwrap();
        assert!(config.worldparameters.axes.is_none());
        assert!(config.worldparameters.world_dim.is_none());
        assert_eq!(config.worldparameters.chunk_size, Some(8));
    }

    #[test]
    fn missing_admin_password_is_rejected() {
        let err = Config::from_toml_str(r#"addr = "127.0.0.1:8080""#).unwrap_err();
        assert!(err.contains("admin_password"), "unexpected error: {err}");
    }

    #[test]
    fn empty_admin_password_is_rejected() {
        let err = Config::from_toml_str(r#"admin_password = """#).unwrap_err();
        assert!(err.contains("admin_password"), "unexpected error: {err}");
    }

    #[test]
    fn user_named_admin_is_rejected() {
        let err = Config::from_toml_str(
            r#"
            admin_password = "secret"
            [[users]]
            username = "admin"
            password = "whatever"
            "#,
        )
        .unwrap_err();
        assert!(err.contains("reserved"), "unexpected error: {err}");
    }

    #[test]
    fn duplicate_usernames_are_rejected() {
        let err = Config::from_toml_str(
            r#"
            admin_password = "secret"
            [[users]]
            username = "alice"
            password = "one"
            [[users]]
            username = "alice"
            password = "two"
            "#,
        )
        .unwrap_err();
        assert!(err.contains("duplicate"), "unexpected error: {err}");
    }

    #[test]
    fn empty_user_password_is_rejected() {
        let err = Config::from_toml_str(
            r#"
            admin_password = "secret"
            [[users]]
            username = "alice"
            password = ""
            "#,
        )
        .unwrap_err();
        assert!(err.contains("alice"), "unexpected error: {err}");
    }

    #[test]
    fn credentials_include_admin_and_every_user() {
        let config = Config::from_toml_str(
            r#"
            admin_password = "secret"
            [[users]]
            username = "alice"
            password = "alice-pw"
            "#,
        )
        .unwrap();
        let creds = config.credentials();
        assert_eq!(creds.get("admin").unwrap().password, "secret");
        assert_eq!(creds.get("alice").unwrap().password, "alice-pw");
        assert_eq!(creds.len(), 2);
    }

    #[test]
    fn admin_is_never_read_only() {
        let config = Config::from_toml_str(r#"admin_password = "secret""#).unwrap();
        assert!(!config.credentials().get("admin").unwrap().read_only);
    }

    #[test]
    fn compression_defaults_to_off() {
        let config = Config::from_toml_str(r#"admin_password = "secret""#).unwrap();
        assert!(!config.compression);
    }

    #[test]
    fn compression_can_be_turned_on() {
        let config = Config::from_toml_str(
            r#"
            admin_password = "secret"
            compression = true
            "#,
        )
        .unwrap();
        assert!(config.compression);
    }

    #[test]
    fn compression_must_be_a_boolean() {
        let err = Config::from_toml_str(
            r#"
            admin_password = "secret"
            compression = "yes"
            "#,
        )
        .unwrap_err();
        assert!(err.contains("compression"), "{err}");
    }

    #[test]
    fn a_port_above_the_u16_range_is_rejected() {
        // Caught by the type, not by `validate` -- worth a test anyway so
        // the error stays a config error rather than becoming a panic or
        // a silently truncated port.
        let err = Config::from_toml_str(
            r#"
            admin_password = "secret"
            http_port = 70000
            "#,
        )
        .unwrap_err();
        assert!(err.contains("http_port"), "{err}");
    }

    #[test]
    fn a_port_written_as_a_string_is_rejected() {
        // `http_port = "8080"` is the shape someone migrating from the
        // old `http_addr` string key would most likely write.
        let err = Config::from_toml_str(
            r#"
            admin_password = "secret"
            http_port = "8080"
            "#,
        )
        .unwrap_err();
        assert!(err.contains("http_port"), "{err}");
    }

    #[test]
    fn hostname_defaults_to_unset_meaning_the_os_hostname() {
        let config = Config::from_toml_str(r#"admin_password = "secret""#).unwrap();
        assert_eq!(config.hostname, None);
    }

    #[test]
    fn hostname_can_be_overridden() {
        let config = Config::from_toml_str(
            r#"
            admin_password = "secret"
            hostname = "db-1.example.com"
            "#,
        )
        .unwrap();
        assert_eq!(config.hostname.as_deref(), Some("db-1.example.com"));
    }

    #[test]
    fn an_empty_hostname_is_rejected() {
        let err = Config::from_toml_str(
            r#"
            admin_password = "secret"
            hostname = ""
            "#,
        )
        .unwrap_err();
        assert!(err.contains("hostname"), "{err}");
    }

    #[test]
    fn a_user_defaults_to_read_write_but_can_be_marked_read_only() {
        let config = Config::from_toml_str(
            r#"
            admin_password = "secret"
            [[users]]
            username = "alice"
            password = "alice-pw"
            [[users]]
            username = "bob"
            password = "bob-pw"
            read_only = true
            "#,
        )
        .unwrap();
        let creds = config.credentials();
        assert!(!creds.get("alice").unwrap().read_only);
        assert!(creds.get("bob").unwrap().read_only);
    }

    #[test]
    fn peers_and_cluster_default_to_empty_and_unset() {
        let config = Config::from_toml_str(r#"admin_password = "secret""#).unwrap();
        assert!(config.peers.is_empty());
        assert_eq!(config.cluster.cluster_secret, None);
        assert_eq!(config.cluster.peer_port, None);
    }

    #[test]
    fn parses_the_cluster_table_and_peers() {
        let config = Config::from_toml_str(
            r#"
            admin_password = "secret"

            [cluster]
            peer_port = 8082
            cluster_secret = "shh"

            [[peers]]
            address = "10.0.0.2:8082"

            [[peers]]
            address = "10.0.0.3:8082"
            "#,
        )
        .unwrap();
        assert_eq!(config.cluster.peer_port, Some(8082));
        assert_eq!(config.cluster.cluster_secret.as_deref(), Some("shh"));
        assert_eq!(
            config
                .peers
                .iter()
                .map(|p| p.address.as_str())
                .collect::<Vec<_>>(),
            vec!["10.0.0.2:8082", "10.0.0.3:8082"]
        );
    }

    #[test]
    fn dead_peer_timeout_defaults_to_unset_and_can_be_set() {
        let config = Config::from_toml_str(r#"admin_password = "secret""#).unwrap();
        assert_eq!(config.cluster.dead_peer_timeout_secs, None);

        let config = Config::from_toml_str(
            r#"
            admin_password = "secret"
            [cluster]
            cluster_secret = "shh"
            dead_peer_timeout_secs = 60
            "#,
        )
        .unwrap();
        assert_eq!(config.cluster.dead_peer_timeout_secs, Some(60));
    }

    #[test]
    fn keepalive_defaults_when_unset() {
        let config = Config::from_toml_str(r#"admin_password = "secret""#).unwrap();
        assert_eq!(config.cluster.keepalive(), Keepalive::default());
    }

    #[test]
    fn keepalive_settings_can_be_set_individually() {
        let config = Config::from_toml_str(
            r#"
            admin_password = "secret"
            [cluster]
            cluster_secret = "shh"
            keepalive_idle_secs = 60
            keepalive_retries = 5
            "#,
        )
        .unwrap();
        assert_eq!(
            config.cluster.keepalive(),
            Keepalive {
                idle: Duration::from_secs(60),
                interval: Keepalive::default().interval,
                retries: 5,
            }
        );
    }

    #[test]
    fn catch_up_settings_default_when_unset() {
        let config = Config::from_toml_str(r#"admin_password = "secret""#).unwrap();
        assert_eq!(
            config.cluster.tombstone_retention(),
            Duration::from_secs(DEFAULT_TOMBSTONE_RETENTION_SECS)
        );
        assert_eq!(
            config.cluster.catch_up_margin(),
            kblockdbcluster::peers::DEFAULT_CATCH_UP_MARGIN
        );
    }

    #[test]
    fn catch_up_settings_can_be_set() {
        let config = Config::from_toml_str(
            r#"
            admin_password = "secret"
            [cluster]
            cluster_secret = "shh"
            tombstone_retention_secs = 3600
            catch_up_margin_secs = 0
            "#,
        )
        .unwrap();
        assert_eq!(
            config.cluster.tombstone_retention(),
            Duration::from_secs(3600)
        );
        // 0 is allowed: no margin at all.
        assert_eq!(config.cluster.catch_up_margin(), Duration::ZERO);
    }

    #[test]
    fn zero_keepalive_settings_are_rejected() {
        for key in [
            "keepalive_idle_secs",
            "keepalive_interval_secs",
            "keepalive_retries",
            "tombstone_retention_secs",
        ] {
            let err = Config::from_toml_str(&format!(
                "admin_password = \"secret\"\n[cluster]\n{key} = 0\n"
            ))
            .unwrap_err();
            assert!(err.contains(key), "{key}: {err}");
        }
    }

    #[test]
    fn a_cluster_table_after_an_array_of_tables_still_parses_correctly() {
        // The bug `[cluster]` was introduced to close: a bare
        // `peer_port = ...`/`cluster_secret = ...` placed after
        // `[[users]]` used to be silently absorbed into that `[[users]]`
        // entry instead of reaching the document root (TOML attaches a
        // later bare `key = value` to whichever table/array entry was
        // most recently opened). An explicit `[cluster]` table header
        // reopens at the document root regardless of what came before
        // it, so this ordering -- which used to lose the settings
        // entirely -- now parses exactly as if `[cluster]` had come
        // first.
        let config = Config::from_toml_str(
            r#"
            admin_password = "secret"

            [[users]]
            username = "alice"
            password = "alice-pw"

            [cluster]
            peer_port = 8082
            cluster_secret = "shh"

            [[peers]]
            address = "10.0.0.2:8082"
            "#,
        )
        .unwrap();
        assert_eq!(config.cluster.peer_port, Some(8082));
        assert_eq!(config.cluster.cluster_secret.as_deref(), Some("shh"));
        assert_eq!(config.peers.len(), 1);
    }

    #[test]
    fn peers_without_a_cluster_secret_is_rejected() {
        let err = Config::from_toml_str(
            r#"
            admin_password = "secret"
            [[peers]]
            address = "10.0.0.2:8082"
            "#,
        )
        .unwrap_err();
        assert!(err.contains("cluster_secret"), "{err}");
    }

    #[test]
    fn peers_with_an_empty_cluster_secret_is_rejected() {
        let err = Config::from_toml_str(
            r#"
            admin_password = "secret"
            [cluster]
            cluster_secret = ""
            [[peers]]
            address = "10.0.0.2:8082"
            "#,
        )
        .unwrap_err();
        assert!(err.contains("cluster_secret"), "{err}");
    }

    #[test]
    fn a_cluster_secret_with_no_peers_is_fine() {
        // Accepting connections without replicating to anyone is a valid
        // (if unusual) setup -- e.g. a node everyone else points at.
        let config = Config::from_toml_str(
            r#"
            admin_password = "secret"
            [cluster]
            cluster_secret = "shh"
            "#,
        )
        .unwrap();
        assert!(config.peers.is_empty());
    }

    #[test]
    fn load_reports_a_readable_error_for_a_missing_file() {
        let err = Config::load(Path::new("/nonexistent/kblockdbserver.toml")).unwrap_err();
        assert!(err.contains("failed to read config file"), "{err}");
    }
}
