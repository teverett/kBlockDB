//! A thin client for kblockdbserver's *binary* protocol -- just enough to
//! drive the `binary_*` scenarios in `scenarios.rs`, the binary-protocol
//! counterpart to `client.rs`'s `Client` (the REST API's equivalent).
//! Uses `kblockdbserver::wire` directly (see that crate's `[lib]` target)
//! so this never reimplements the wire format from its doc comments and
//! risks drifting from what the server actually speaks -- and, since that
//! module re-exports `kblockdblib::Value`, without needing a direct
//! `kblockdblib` dependency either (see `wire.rs`'s doc comment on why
//! that's kept re-exported).
//!
//! Unlike `Client` (which wraps a `reqwest::Client` -- itself `Arc`-backed
//! and meant to be cloned/shared across concurrent tasks), `BinaryClient`
//! owns one TCP connection outright and authenticates it once via
//! `Hello` -- see the binary protocol's per-connection auth model
//! (`wire.rs`'s doc comment). One connection is strictly
//! request-then-response, never multiple in flight at once (this minimal
//! protocol doesn't pipeline), so every method here takes `&mut self`.

use kblockdbserver::wire::{self, Request, Response, Value};
use std::time::{Duration, Instant};
use tokio::net::TcpStream;

/// The outcome of one timed request -- same shape as `client::Timed`, so
/// `binary_*` scenarios can build a `ScenarioResult` the same way the
/// HTTP ones do.
pub struct Timed {
    pub elapsed: Duration,
    pub ok: bool,
}

/// What a target's `Hello` response reports about the world it's serving
/// -- the binary protocol's equivalent of `client::HealthInfo`.
pub struct HealthInfo {
    pub axes: usize,
    pub world_dim: u32,
}

pub struct BinaryClient {
    stream: TcpStream,
}

impl BinaryClient {
    /// Connects to `addr` and authenticates as `username`/`password` in
    /// one step (this protocol always needs a successful `Hello` before
    /// anything else, so there's no useful "connected but not
    /// authenticated" state to hand back separately). `None` on any
    /// connection failure, decode failure, or rejected `Hello`.
    pub async fn connect(
        addr: &str,
        username: &str,
        password: &str,
    ) -> Option<(BinaryClient, HealthInfo)> {
        let mut stream = TcpStream::connect(addr).await.ok()?;
        stream.set_nodelay(true).ok()?;
        let hello = Request::Hello {
            username: username.to_string(),
            password: password.to_string(),
        };
        wire::write_frame(&mut stream, &wire::encode_request(&hello))
            .await
            .ok()?;
        let payload = wire::read_frame(&mut stream).await.ok()??;
        match wire::decode_response(&payload).ok()? {
            Response::HelloOk {
                axes, world_dim, ..
            } => Some((
                BinaryClient { stream },
                HealthInfo {
                    axes: axes as usize,
                    world_dim,
                },
            )),
            _ => None,
        }
    }

    /// Sends `req` and returns the decoded `Response`, or `None` if
    /// anything at the connection/framing/decoding level went wrong --
    /// distinct from a well-formed *error* response (`BadRequest`,
    /// `Unauthorized`, ...), which comes back `Some(...)` same as success
    /// does, and is `set_cell`/`get_cell`/`remove_cell`'s job to
    /// interpret.
    async fn roundtrip(&mut self, req: &Request) -> Option<Response> {
        wire::write_frame(&mut self.stream, &wire::encode_request(req))
            .await
            .ok()?;
        let payload = wire::read_frame(&mut self.stream).await.ok()??;
        wire::decode_response(&payload).ok()
    }

    pub async fn set_cell(&mut self, coord: &[u32], key: &str, value: i64) -> Timed {
        let req = Request::Set {
            coord: coord.to_vec(),
            key: key.to_string(),
            value: Value::I64(value),
        };
        let t0 = Instant::now();
        let ok = matches!(self.roundtrip(&req).await, Some(Response::Ok));
        Timed {
            elapsed: t0.elapsed(),
            ok,
        }
    }

    pub async fn get_cell(&mut self, coord: &[u32], key: &str) -> Timed {
        let req = Request::Get {
            coord: coord.to_vec(),
            key: key.to_string(),
        };
        let t0 = Instant::now();
        let ok = matches!(
            self.roundtrip(&req).await,
            Some(Response::Value(_)) | Some(Response::NotFound)
        );
        Timed {
            elapsed: t0.elapsed(),
            ok,
        }
    }

    pub async fn remove_cell(&mut self, coord: &[u32], key: &str) -> Timed {
        let req = Request::Remove {
            coord: coord.to_vec(),
            key: key.to_string(),
        };
        let t0 = Instant::now();
        let ok = matches!(self.roundtrip(&req).await, Some(Response::Ok));
        Timed {
            elapsed: t0.elapsed(),
            ok,
        }
    }
}
