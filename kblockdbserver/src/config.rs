//! The `--config <path>` TOML file: the port/address to listen on, the
//! data directory, and the accounts (admin + regular users) allowed to
//! use the REST API. Kept separate from `main.rs`'s CLI parsing since it
//! has its own file-format concerns (parsing, validation) and is unit
//! tested against raw TOML text rather than real files.
//!
//! Deliberately config-file-only, not a CLI flag: putting passwords on the
//! command line leaks them into shell history and `ps` output.

use crate::state::Account;
use serde::Deserialize;
use std::collections::HashMap;
use std::path::Path;

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

#[derive(Debug, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub http_addr: Option<String>,
    #[serde(default)]
    pub binary_addr: Option<String>,
    #[serde(default)]
    pub data_dir: Option<String>,
    #[serde(default)]
    pub axes: Option<usize>,
    #[serde(default)]
    pub world_dim: Option<u32>,
    #[serde(default)]
    pub max_concurrent_disk_ops: Option<usize>,
    #[serde(default)]
    pub max_cached_chunks: Option<usize>,
    pub admin_password: String,
    #[serde(default)]
    pub users: Vec<UserConfig>,
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
        assert!(config.http_addr.is_none());
        assert!(config.users.is_empty());
    }

    #[test]
    fn parses_every_field() {
        let config = Config::from_toml_str(
            r#"
            http_addr = "0.0.0.0:9090"
            binary_addr = "0.0.0.0:9091"
            data_dir = "/var/lib/kblockdblib"
            axes = 4
            world_dim = 500
            max_concurrent_disk_ops = 16
            max_cached_chunks = 5000
            admin_password = "secret"

            [[users]]
            username = "alice"
            password = "alice-pw"

            [[users]]
            username = "bob"
            password = "bob-pw"
            "#,
        )
        .unwrap();
        assert_eq!(config.http_addr.as_deref(), Some("0.0.0.0:9090"));
        assert_eq!(config.binary_addr.as_deref(), Some("0.0.0.0:9091"));
        assert_eq!(config.data_dir.as_deref(), Some("/var/lib/kblockdblib"));
        assert_eq!(config.axes, Some(4));
        assert_eq!(config.world_dim, Some(500));
        assert_eq!(config.max_concurrent_disk_ops, Some(16));
        assert_eq!(config.max_cached_chunks, Some(5000));
        assert_eq!(config.users.len(), 2);
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
    fn load_reports_a_readable_error_for_a_missing_file() {
        let err = Config::load(Path::new("/nonexistent/kblockdbserver.toml")).unwrap_err();
        assert!(err.contains("failed to read config file"), "{err}");
    }
}
