//! A thin async HTTP client for kblockdbserver's REST API -- just enough to
//! drive the scenarios in `scenarios.rs`, not a general-purpose SDK. Every
//! value used here is an `i64`, on purpose: keeping the payload shape
//! constant means differences between scenarios reflect kblockdbserver/kblockdblib
//! behavior, not incidental payload-size effects.
//!
//! One `Client` always targets exactly one database (`database`, given to
//! `new`) -- kblockdbserver manages any number of them under one
//! `--data-dir`, and every per-cell/region call here is scoped under
//! `/rest/db/{database}/...`. `ensure_database` must be called once, before
//! any of those, since kblockdbserver never creates a database implicitly.

use serde_json::json;
use std::time::{Duration, Instant};

/// Cheap to clone (wraps a `reqwest::Client`, itself `Arc`-backed and
/// meant to be shared/cloned across concurrent tasks so they share one
/// connection pool).
#[derive(Clone)]
pub struct Client {
    http: reqwest::Client,
    base_url: String,
    username: String,
    password: String,
    database: String,
}

/// The outcome of one timed HTTP call.
pub struct Timed {
    pub elapsed: Duration,
    pub ok: bool,
}

/// What a target kblockdbserver's `/rest/health` reports -- used only for a
/// connectivity/diagnostic print; the database shape itself (`axes`/
/// `world_dim`) is no longer part of this response (a server manages many
/// databases, each with its own shape), so callers that need it use the
/// values they themselves passed to `ensure_database`.
pub struct HealthInfo {
    pub hostname: String,
    pub database_count: u64,
}

impl Client {
    /// `username`/`password` authenticate every call below except
    /// `health()` -- kblockdbserver's REST API requires HTTP Basic Auth on
    /// everything but `/rest/health` (see kblockdbserver's `routes.rs`).
    /// `database` is the one database this client's cell/region calls
    /// target; create it first with `ensure_database`.
    pub fn new(
        base_url: impl Into<String>,
        username: impl Into<String>,
        password: impl Into<String>,
        database: impl Into<String>,
    ) -> Client {
        Client {
            http: reqwest::Client::new(),
            base_url: base_url.into(),
            username: username.into(),
            password: password.into(),
            database: database.into(),
        }
    }

    pub async fn health(&self) -> Option<HealthInfo> {
        let resp = self
            .http
            .get(format!("{}/rest/health", self.base_url))
            .send()
            .await
            .ok()?;
        if !resp.status().is_success() {
            return None;
        }
        let body: serde_json::Value = resp.json().await.ok()?;
        Some(HealthInfo {
            hostname: body.get("hostname")?.as_str()?.to_string(),
            database_count: body.get("database_count")?.as_u64()?,
        })
    }

    /// Makes sure this client's target database exists, creating it shaped
    /// `axes`/`world_dim`/`chunk_size` if it doesn't. An already-existing
    /// database (`409 Conflict`) is treated as success, not an error -- a
    /// perf run against a `--db` name reused from an earlier run should
    /// reuse that database, not fail.
    pub async fn ensure_database(
        &self,
        axes: usize,
        world_dim: u32,
        chunk_size: u32,
    ) -> Result<(), String> {
        let url = format!("{}/rest/databases/{}", self.base_url, self.database);
        let body = json!({"axes": axes, "world_dim": world_dim, "chunk_size": chunk_size});
        let resp = self
            .http
            .put(url)
            .basic_auth(&self.username, Some(&self.password))
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("request failed: {e}"))?;
        let status = resp.status();
        if status.is_success() || status == reqwest::StatusCode::CONFLICT {
            return Ok(());
        }
        let text = resp.text().await.unwrap_or_default();
        Err(format!("server returned {status}: {text}"))
    }

    /// `{base_url}/rest/db/{database}/{suffix}` -- every per-cell/region
    /// endpoint shares this prefix.
    fn db_url(&self, suffix: &str) -> String {
        format!("{}/rest/db/{}/{suffix}", self.base_url, self.database)
    }

    fn coords(coord: &[i32]) -> String {
        coord
            .iter()
            .map(i32::to_string)
            .collect::<Vec<_>>()
            .join(",")
    }

    pub async fn set_cell(&self, coord: &[i32], key: &str, value: i64) -> Timed {
        let url = self.db_url(&format!("cells/{}/{key}", Self::coords(coord)));
        let body = json!({"type": "i64", "value": value});
        let t0 = Instant::now();
        let ok = self
            .http
            .put(url)
            .basic_auth(&self.username, Some(&self.password))
            .json(&body)
            .send()
            .await
            .is_ok_and(|r| r.status().is_success());
        Timed {
            elapsed: t0.elapsed(),
            ok,
        }
    }

    pub async fn get_cell(&self, coord: &[i32], key: &str) -> Timed {
        let url = self.db_url(&format!("cells/{}/{key}", Self::coords(coord)));
        let t0 = Instant::now();
        let ok = self
            .http
            .get(url)
            .basic_auth(&self.username, Some(&self.password))
            .send()
            .await
            .is_ok_and(|r| r.status().is_success());
        Timed {
            elapsed: t0.elapsed(),
            ok,
        }
    }

    pub async fn remove_cell(&self, coord: &[i32], key: &str) -> Timed {
        let url = self.db_url(&format!("cells/{}/{key}", Self::coords(coord)));
        let t0 = Instant::now();
        let ok = self
            .http
            .delete(url)
            .basic_auth(&self.username, Some(&self.password))
            .send()
            .await
            .is_ok_and(|r| r.status().is_success());
        Timed {
            elapsed: t0.elapsed(),
            ok,
        }
    }

    pub async fn set_region(
        &self,
        origin: &[i32],
        extent: &[i32],
        key: &str,
        values: &[i64],
    ) -> Timed {
        let url = self.db_url(&format!(
            "regions/{}/{}/{key}",
            Self::coords(origin),
            Self::coords(extent)
        ));
        let body = json!({
            "values": values.iter().map(|v| json!({"type": "i64", "value": v})).collect::<Vec<_>>(),
        });
        let t0 = Instant::now();
        let ok = self
            .http
            .put(url)
            .basic_auth(&self.username, Some(&self.password))
            .json(&body)
            .send()
            .await
            .is_ok_and(|r| r.status().is_success());
        Timed {
            elapsed: t0.elapsed(),
            ok,
        }
    }

    pub async fn get_region(&self, origin: &[i32], extent: &[i32], key: &str) -> Timed {
        let url = self.db_url(&format!(
            "regions/{}/{}/{key}",
            Self::coords(origin),
            Self::coords(extent)
        ));
        let t0 = Instant::now();
        let ok = self
            .http
            .get(url)
            .basic_auth(&self.username, Some(&self.password))
            .send()
            .await
            .is_ok_and(|r| r.status().is_success());
        Timed {
            elapsed: t0.elapsed(),
            ok,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coords_join_with_commas() {
        assert_eq!(Client::coords(&[1, 2, 3]), "1,2,3");
        assert_eq!(Client::coords(&[0]), "0");
        assert_eq!(Client::coords(&[]), "");
    }

    #[test]
    fn db_url_scopes_under_the_clients_database() {
        let client = Client::new("http://x", "admin", "pw", "mydb");
        assert_eq!(client.db_url("stats"), "http://x/rest/db/mydb/stats");
    }
}
