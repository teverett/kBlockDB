//! A thin async HTTP client for kblockdbserver's REST API -- just enough to
//! drive the scenarios in `scenarios.rs`, not a general-purpose SDK. Every
//! value used here is an `i64`, on purpose: keeping the payload shape
//! constant means differences between scenarios reflect kblockdbserver/kblockdblib
//! behavior, not incidental payload-size effects.

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
}

/// The outcome of one timed HTTP call.
pub struct Timed {
    pub elapsed: Duration,
    pub ok: bool,
}

/// What a target kblockdbserver's `/health` reports about the world it's
/// serving -- scenarios use this to build coordinates that fit.
pub struct HealthInfo {
    pub axes: usize,
    pub world_dim: u32,
}

impl Client {
    /// `username`/`password` authenticate every call below except
    /// `health()` -- kblockdbserver's REST API requires HTTP Basic Auth on
    /// everything but `/health` (see kblockdbserver's `routes.rs`).
    pub fn new(
        base_url: impl Into<String>,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Client {
        Client {
            http: reqwest::Client::new(),
            base_url: base_url.into(),
            username: username.into(),
            password: password.into(),
        }
    }

    pub async fn health(&self) -> Option<HealthInfo> {
        let resp = self
            .http
            .get(format!("{}/health", self.base_url))
            .send()
            .await
            .ok()?;
        if !resp.status().is_success() {
            return None;
        }
        let body: serde_json::Value = resp.json().await.ok()?;
        Some(HealthInfo {
            axes: body.get("axes")?.as_u64()? as usize,
            world_dim: body.get("world_dim")?.as_u64()? as u32,
        })
    }

    fn coords(coord: &[u32]) -> String {
        coord
            .iter()
            .map(u32::to_string)
            .collect::<Vec<_>>()
            .join(",")
    }

    pub async fn set_cell(&self, coord: &[u32], key: &str, value: i64) -> Timed {
        let url = format!("{}/cells/{}/{key}", self.base_url, Self::coords(coord));
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

    pub async fn get_cell(&self, coord: &[u32], key: &str) -> Timed {
        let url = format!("{}/cells/{}/{key}", self.base_url, Self::coords(coord));
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

    pub async fn remove_cell(&self, coord: &[u32], key: &str) -> Timed {
        let url = format!("{}/cells/{}/{key}", self.base_url, Self::coords(coord));
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
        origin: &[u32],
        extent: &[u32],
        key: &str,
        values: &[i64],
    ) -> Timed {
        let url = format!(
            "{}/regions/{}/{}/{key}",
            self.base_url,
            Self::coords(origin),
            Self::coords(extent)
        );
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

    pub async fn get_region(&self, origin: &[u32], extent: &[u32], key: &str) -> Timed {
        let url = format!(
            "{}/regions/{}/{}/{key}",
            self.base_url,
            Self::coords(origin),
            Self::coords(extent)
        );
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
}
