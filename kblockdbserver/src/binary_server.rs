//! The binary protocol's TCP listener and per-connection handler -- see
//! `wire.rs` for the wire format itself, and the README's "Binary
//! protocol" section for why this exists alongside the REST API rather
//! than instead of it.
//!
//! Authentication here is per-*connection*, not per-request the way HTTP
//! Basic Auth is: a client sends one `Hello` right after connecting, and
//! every request after that on the same connection is treated as that
//! account until the connection closes (or a later `Hello` re-
//! authenticates as someone else -- allowed, not required). This is the
//! one place this protocol's semantics genuinely differ from the REST
//! API's, and it's why a raw `nc`/telnet session can't just fire
//! `Get`/`Set` at this listener the way `curl` can against the REST one --
//! see `kblockdbcli` (or a future binary-protocol client) for something
//! that actually speaks it.

use crate::error::ApiError;
use crate::state::{Account, AppState};
use kblockdbserver::wire::{self, QueryResult, QueryRow, QueryValue, Request, Response};
use tokio::net::{TcpListener, TcpStream};

/// `wire::Response` is defined in the published `wire` module and
/// deliberately knows nothing about this binary's own `ApiError` (see
/// `wire.rs`'s doc comment on why it's kept self-contained) -- this is the
/// server-side half of that mapping, the binary-protocol equivalent of
/// `ApiError`'s `IntoResponse` impl for the REST API.
fn response_from_error(e: ApiError) -> Response {
    match e {
        // `ApiError::NotFound` carries a message (`"no value set for key
        // ... at (...)"`, see `routes.rs`'s `get_cell`), but this
        // protocol's `NotFound` doesn't and discards it -- a client that
        // gets `NotFound` already knows exactly which request it sent, so
        // the extra text the REST API's JSON body needs (to stand alone
        // in a log line, say) isn't pulling its weight here.
        ApiError::NotFound(_) => Response::NotFound,
        ApiError::BadRequest(m) => Response::BadRequest(m),
        ApiError::Internal(m) => Response::Internal(m),
        ApiError::Unauthorized(m) => Response::Unauthorized(m),
        ApiError::Forbidden(m) => Response::Forbidden(m),
    }
}

/// Accepts connections on `listener` forever, spawning one task per
/// connection to run `handle_connection`. Doesn't participate in the HTTP
/// server's graceful shutdown -- when the process exits (right after the
/// HTTP server's own graceful shutdown completes), this task and every
/// connection it spawned are simply dropped, same as a plain `kill` would
/// do to them. Acceptable for a minimal first cut: no request is left
/// half-applied either way, since every `Request::Set`/`Remove` is still
/// durable (written to disk) before its `Response` goes out -- a dropped
/// connection only ever loses a response in flight, never a write.
pub async fn serve(listener: TcpListener, state: AppState) {
    loop {
        let (stream, _addr) = match listener.accept().await {
            Ok(pair) => pair,
            Err(e) => {
                eprintln!("binary protocol: accept failed: {e}");
                continue;
            }
        };
        let _ = stream.set_nodelay(true);
        let state = state.clone();
        tokio::spawn(async move {
            handle_connection(stream, state).await;
        });
    }
}

/// One client's connection: reads a request frame, dispatches it, writes
/// back a response frame, and repeats until the connection closes or a
/// frame-level I/O problem makes it unrecoverable (see
/// `wire::read_frame`'s doc comment). A malformed but well-*framed*
/// request (an unknown opcode, a truncated field, ...) becomes a
/// `BadRequest` response, not a closed connection -- only I/O-level
/// problems (a truncated frame, an oversized one, the socket itself
/// breaking) end the loop.
async fn handle_connection(mut stream: TcpStream, state: AppState) {
    let mut account: Option<Account> = None;

    loop {
        let payload = match wire::read_frame(&mut stream).await {
            Ok(Some(p)) => p,
            Ok(None) => return, // clean disconnect, right at a frame boundary
            Err(_) => return,   // truncated/oversized frame, or a socket error
        };

        let response = match wire::decode_request(&payload) {
            Ok(req) => handle_request(req, &state, &mut account).await,
            Err(e) => Response::BadRequest(e.to_string()),
        };

        let mut response_payload = wire::encode_response(&response);
        if response_payload.len() as u64 > wire::MAX_FRAME_LEN as u64 {
            response_payload = wire::encode_response(&Response::BadRequest(
                "result exceeds the binary protocol frame limit; narrow the region or query"
                    .to_string(),
            ));
        }
        if wire::write_frame(&mut stream, &response_payload)
            .await
            .is_err()
        {
            return; // peer gone -- nothing left to do
        }
    }
}

/// This connection's account so far, or a `Response` explaining why there
/// isn't one yet -- shared by every request kind that needs *some*
/// authenticated account (i.e. everything but `Hello` itself).
fn require_authenticated(account: &Option<Account>) -> Result<&Account, Response> {
    account
        .as_ref()
        .ok_or_else(|| Response::Unauthorized("send Hello first".to_string()))
}

/// Same as `require_authenticated`, plus the `read_only` check every
/// `Set`/`Remove` needs -- mirrors `auth.rs`'s middleware, just checked
/// here instead of before a handler runs, since this protocol has no
/// middleware layer to put it in.
fn require_write(account: &Option<Account>) -> Result<(), Response> {
    let account = require_authenticated(account)?;
    if account.read_only {
        return Err(Response::Forbidden("this account is read-only".to_string()));
    }
    Ok(())
}

async fn handle_request(req: Request, state: &AppState, account: &mut Option<Account>) -> Response {
    match req {
        Request::Hello { username, password } => match state.authenticate(&username, &password) {
            Some(acc) => {
                let response = Response::HelloOk {
                    axes: state.world.axes() as u8,
                    world_dim: state.world.world_dim(),
                    read_only: acc.read_only,
                };
                *account = Some(acc);
                response
            }
            None => Response::Unauthorized("invalid username or password".to_string()),
        },
        Request::Get { coord, key } => {
            if let Err(response) = require_authenticated(account) {
                return response;
            }
            match state
                .with_world(move |w| w.get_with_meta(&coord, &key))
                .await
            {
                Ok(Some((value, meta))) => Response::Value {
                    value,
                    created_at_ms: meta.created_at_ms,
                    modified_at_ms: meta.modified_at_ms,
                    version: meta.version,
                },
                Ok(None) => Response::NotFound,
                Err(e) => response_from_error(e),
            }
        }
        Request::Set { coord, key, value } => {
            if let Err(response) = require_write(account) {
                return response;
            }
            match state.with_world(move |w| w.set(&coord, &key, value)).await {
                Ok(()) => Response::Ok,
                Err(e) => response_from_error(e),
            }
        }
        Request::Remove { coord, key } => {
            if let Err(response) = require_write(account) {
                return response;
            }
            match state.with_world(move |w| w.remove(&coord, &key)).await {
                Ok(()) => Response::Ok,
                Err(e) => response_from_error(e),
            }
        }
        Request::Health => Response::Health {
            axes: state.world.axes() as u8,
            world_dim: state.world.world_dim(),
            timestamp: crate::routes::unix_timestamp(),
        },
        Request::Stats => {
            if let Err(response) = require_authenticated(account) {
                return response;
            }
            match state.with_world(kblockdblib::World::stats).await {
                Ok(stats) => Response::Stats {
                    total_chunks: stats.total_chunks,
                    total_bytes: stats.total_bytes,
                    total_blocks: stats.total_blocks,
                },
                Err(e) => response_from_error(e),
            }
        }
        Request::GetRegion {
            origin,
            extent,
            key,
        } => {
            if let Err(response) = require_authenticated(account) {
                return response;
            }
            let region = kblockdblib::Region::new(origin, extent);
            match state.with_world(move |w| w.get_region(&region, &key)).await {
                Ok(values) => Response::RegionValues(values),
                Err(e) => response_from_error(e),
            }
        }
        Request::SetRegion {
            origin,
            extent,
            key,
            values,
        } => {
            if let Err(response) = require_write(account) {
                return response;
            }
            let region = kblockdblib::Region::new(origin, extent);
            match state
                .with_world(move |w| w.set_region(&region, &key, &values))
                .await
            {
                Ok(()) => Response::Ok,
                Err(e) => response_from_error(e),
            }
        }
        Request::RemoveRegion {
            origin,
            extent,
            key,
        } => {
            if let Err(response) = require_write(account) {
                return response;
            }
            let region = kblockdblib::Region::new(origin, extent);
            match state
                .with_world(move |w| w.remove_region(&region, &key))
                .await
            {
                Ok(()) => Response::Ok,
                Err(e) => response_from_error(e),
            }
        }
        Request::Query { query } => {
            let authenticated = match require_authenticated(account) {
                Ok(account) => account,
                Err(response) => return response,
            };
            match crate::routes::execute_query(state, authenticated, &query).await {
                Ok(result) => {
                    if let Some(rows) = result.rows {
                        Response::Query(QueryResult::Rows(
                            rows.into_iter()
                                .map(|row| QueryRow {
                                    coord: row.coord,
                                    values: row
                                        .values
                                        .into_iter()
                                        .map(|value| QueryValue {
                                            key: value.key,
                                            value: value.value.into(),
                                            created_at_ms: value.created_at_ms,
                                            modified_at_ms: value.modified_at_ms,
                                            version: value.version,
                                        })
                                        .collect(),
                                })
                                .collect(),
                        ))
                    } else {
                        Response::Query(QueryResult::Affected(
                            result.affected_cells.unwrap_or_default() as u64,
                        ))
                    }
                }
                Err(e) => response_from_error(e),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kblockdblib::{Value, World};
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpStream as ClientStream;

    const ADMIN_PASSWORD: &str = "admin-pw";
    const VIEWER_PASSWORD: &str = "viewer-pw";

    /// Spawns a real binary-protocol listener on an OS-assigned port,
    /// backed by a fresh temp world (`axes`, `world_dim` as given, two
    /// accounts: `admin` full access, `viewer` read-only), and returns the
    /// address to connect to. The temp directory is removed when the
    /// returned guard drops.
    struct TestServer {
        addr: std::net::SocketAddr,
        dir: std::path::PathBuf,
    }

    impl Drop for TestServer {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    async fn spawn_test_server(axes: usize, world_dim: u32) -> TestServer {
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "kblockdbserver-binary-test-{n}-{}",
            std::process::id()
        ));
        let world = World::create(&dir, axes, world_dim, 32).unwrap();

        let mut credentials = HashMap::new();
        credentials.insert(
            "admin".to_string(),
            Account {
                password: ADMIN_PASSWORD.to_string(),
                read_only: false,
            },
        );
        credentials.insert(
            "viewer".to_string(),
            Account {
                password: VIEWER_PASSWORD.to_string(),
                read_only: true,
            },
        );
        let state = AppState::new(world, Arc::new(credentials));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(listener, state));

        TestServer { addr, dir }
    }

    async fn connect(server: &TestServer) -> ClientStream {
        ClientStream::connect(server.addr).await.unwrap()
    }

    async fn roundtrip(stream: &mut ClientStream, req: &Request) -> Response {
        wire::write_frame(stream, &wire::encode_request(req))
            .await
            .unwrap();
        let payload = wire::read_frame(stream).await.unwrap().unwrap();
        wire::decode_response(&payload).unwrap()
    }

    async fn hello(stream: &mut ClientStream, username: &str, password: &str) -> Response {
        roundtrip(
            stream,
            &Request::Hello {
                username: username.to_string(),
                password: password.to_string(),
            },
        )
        .await
    }

    /// Unwraps a `Response::Value`'s value, ignoring its meta -- for tests
    /// that only care about the value itself (see
    /// `a_get_response_reports_created_modified_and_version` for one that
    /// checks the meta fields).
    fn expect_value(response: Response) -> Value {
        match response {
            Response::Value { value, .. } => value,
            other => panic!("expected Response::Value, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn hello_with_correct_credentials_returns_the_worlds_shape() {
        let server = spawn_test_server(3, 10_000).await;
        let mut stream = connect(&server).await;

        let response = hello(&mut stream, "admin", ADMIN_PASSWORD).await;
        assert_eq!(
            response,
            Response::HelloOk {
                axes: 3,
                world_dim: 10_000,
                read_only: false,
            }
        );
    }

    #[tokio::test]
    async fn hello_reports_a_read_only_accounts_role() {
        let server = spawn_test_server(3, 10_000).await;
        let mut stream = connect(&server).await;

        let response = hello(&mut stream, "viewer", VIEWER_PASSWORD).await;
        assert_eq!(
            response,
            Response::HelloOk {
                axes: 3,
                world_dim: 10_000,
                read_only: true,
            }
        );
    }

    #[tokio::test]
    async fn hello_with_the_wrong_password_is_unauthorized() {
        let server = spawn_test_server(3, 10_000).await;
        let mut stream = connect(&server).await;

        let response = hello(&mut stream, "admin", "wrong").await;
        assert!(matches!(response, Response::Unauthorized(_)));
    }

    #[tokio::test]
    async fn operations_before_hello_are_unauthorized() {
        let server = spawn_test_server(3, 10_000).await;
        let mut stream = connect(&server).await;

        let response = roundtrip(
            &mut stream,
            &Request::Get {
                coord: vec![1, 2, 3],
                key: "material".to_string(),
            },
        )
        .await;
        assert!(matches!(response, Response::Unauthorized(_)));

        for request in [
            Request::Stats,
            Request::GetRegion {
                origin: vec![0, 0, 0],
                extent: vec![1, 1, 1],
                key: "material".to_string(),
            },
            Request::Query {
                query: "SELECT *".to_string(),
            },
        ] {
            let response = roundtrip(&mut stream, &request).await;
            assert!(matches!(response, Response::Unauthorized(_)));
        }

        assert!(matches!(
            roundtrip(&mut stream, &Request::Health).await,
            Response::Health { .. }
        ));
    }

    #[tokio::test]
    async fn set_then_get_then_remove_round_trips_through_a_real_connection() {
        let server = spawn_test_server(3, 10_000).await;
        let mut stream = connect(&server).await;
        hello(&mut stream, "admin", ADMIN_PASSWORD).await;

        let coord = vec![1, 2, 3];
        let set = roundtrip(
            &mut stream,
            &Request::Set {
                coord: coord.clone(),
                key: "material".to_string(),
                value: Value::Str("stone".to_string()),
            },
        )
        .await;
        assert_eq!(set, Response::Ok);

        let get = roundtrip(
            &mut stream,
            &Request::Get {
                coord: coord.clone(),
                key: "material".to_string(),
            },
        )
        .await;
        assert_eq!(expect_value(get), Value::Str("stone".to_string()));

        let remove = roundtrip(
            &mut stream,
            &Request::Remove {
                coord: coord.clone(),
                key: "material".to_string(),
            },
        )
        .await;
        assert_eq!(remove, Response::Ok);

        let get_after_remove = roundtrip(
            &mut stream,
            &Request::Get {
                coord,
                key: "material".to_string(),
            },
        )
        .await;
        assert_eq!(get_after_remove, Response::NotFound);
    }

    #[tokio::test]
    async fn a_negative_coordinate_round_trips_through_a_real_connection() {
        let server = spawn_test_server(3, 10_000).await; // valid range: [-5000, 5000)
        let mut stream = connect(&server).await;
        hello(&mut stream, "admin", ADMIN_PASSWORD).await;

        let coord = vec![-1, -2, -3];
        let set = roundtrip(
            &mut stream,
            &Request::Set {
                coord: coord.clone(),
                key: "material".to_string(),
                value: Value::Str("stone".to_string()),
            },
        )
        .await;
        assert_eq!(set, Response::Ok);

        let get = roundtrip(
            &mut stream,
            &Request::Get {
                coord,
                key: "material".to_string(),
            },
        )
        .await;
        assert_eq!(expect_value(get), Value::Str("stone".to_string()));
    }

    #[tokio::test]
    async fn a_get_response_reports_created_modified_and_version() {
        let server = spawn_test_server(3, 10_000).await;
        let mut stream = connect(&server).await;
        hello(&mut stream, "admin", ADMIN_PASSWORD).await;
        let coord = vec![1, 2, 3];

        roundtrip(
            &mut stream,
            &Request::Set {
                coord: coord.clone(),
                key: "material".to_string(),
                value: Value::Str("stone".to_string()),
            },
        )
        .await;
        let first = roundtrip(
            &mut stream,
            &Request::Get {
                coord: coord.clone(),
                key: "material".to_string(),
            },
        )
        .await;
        let Response::Value {
            value,
            created_at_ms,
            modified_at_ms,
            version,
        } = first
        else {
            panic!("expected Response::Value, got {first:?}");
        };
        assert_eq!(value, Value::Str("stone".to_string()));
        assert_eq!(version, 0);
        assert_eq!(created_at_ms, modified_at_ms);
        assert!(created_at_ms > 0);

        roundtrip(
            &mut stream,
            &Request::Set {
                coord: coord.clone(),
                key: "material".to_string(),
                value: Value::Str("air".to_string()),
            },
        )
        .await;
        let second = roundtrip(
            &mut stream,
            &Request::Get {
                coord,
                key: "material".to_string(),
            },
        )
        .await;
        let Response::Value {
            created_at_ms: created_at_ms_2,
            modified_at_ms: modified_at_ms_2,
            version: version_2,
            ..
        } = second
        else {
            panic!("expected Response::Value, got {second:?}");
        };
        assert_eq!(created_at_ms_2, created_at_ms);
        assert!(modified_at_ms_2 >= modified_at_ms);
        assert_eq!(version_2, 1);
    }

    #[tokio::test]
    async fn a_read_only_account_can_get_but_not_set_or_remove() {
        let server = spawn_test_server(3, 10_000).await;

        // Populate one cell as admin first.
        let mut admin_stream = connect(&server).await;
        hello(&mut admin_stream, "admin", ADMIN_PASSWORD).await;
        roundtrip(
            &mut admin_stream,
            &Request::Set {
                coord: vec![1, 2, 3],
                key: "material".to_string(),
                value: Value::I64(1),
            },
        )
        .await;

        let mut stream = connect(&server).await;
        hello(&mut stream, "viewer", VIEWER_PASSWORD).await;

        let get = roundtrip(
            &mut stream,
            &Request::Get {
                coord: vec![1, 2, 3],
                key: "material".to_string(),
            },
        )
        .await;
        assert_eq!(expect_value(get), Value::I64(1));

        let set = roundtrip(
            &mut stream,
            &Request::Set {
                coord: vec![1, 2, 3],
                key: "material".to_string(),
                value: Value::I64(2),
            },
        )
        .await;
        assert!(matches!(set, Response::Forbidden(_)));

        let remove = roundtrip(
            &mut stream,
            &Request::Remove {
                coord: vec![1, 2, 3],
                key: "material".to_string(),
            },
        )
        .await;
        assert!(matches!(remove, Response::Forbidden(_)));

        let set_region = roundtrip(
            &mut stream,
            &Request::SetRegion {
                origin: vec![0, 0, 0],
                extent: vec![1, 1, 1],
                key: "material".to_string(),
                values: vec![Value::I64(2)],
            },
        )
        .await;
        assert!(matches!(set_region, Response::Forbidden(_)));

        let remove_region = roundtrip(
            &mut stream,
            &Request::RemoveRegion {
                origin: vec![0, 0, 0],
                extent: vec![1, 1, 1],
                key: "material".to_string(),
            },
        )
        .await;
        assert!(matches!(remove_region, Response::Forbidden(_)));

        let query = roundtrip(
            &mut stream,
            &Request::Query {
                query: "SET (material=2) IN (0,0,0) TO (1,1,1)".to_string(),
            },
        )
        .await;
        assert!(matches!(query, Response::Forbidden(_)));
    }

    #[tokio::test]
    async fn a_bad_coordinate_is_a_bad_request_not_a_closed_connection() {
        let server = spawn_test_server(3, 10_000).await; // 3 axes
        let mut stream = connect(&server).await;
        hello(&mut stream, "admin", ADMIN_PASSWORD).await;

        let bad = roundtrip(
            &mut stream,
            &Request::Get {
                coord: vec![1, 2], // wrong axis count
                key: "material".to_string(),
            },
        )
        .await;
        assert!(matches!(bad, Response::BadRequest(_)));

        // The connection must still be usable after that.
        let good = roundtrip(
            &mut stream,
            &Request::Get {
                coord: vec![1, 2, 3],
                key: "material".to_string(),
            },
        )
        .await;
        assert_eq!(good, Response::NotFound);
    }

    #[tokio::test]
    async fn a_malformed_frame_is_a_bad_request_not_a_closed_connection() {
        let server = spawn_test_server(3, 10_000).await;
        let mut stream = connect(&server).await;
        hello(&mut stream, "admin", ADMIN_PASSWORD).await;

        // A well-framed payload the server can't decode at all (unknown
        // opcode) -- write_frame/read_frame directly, since wire::Request
        // has no variant for this.
        wire::write_frame(&mut stream, &[0xFF]).await.unwrap();
        let payload = wire::read_frame(&mut stream).await.unwrap().unwrap();
        assert!(matches!(
            wire::decode_response(&payload).unwrap(),
            Response::BadRequest(_)
        ));

        let good = roundtrip(
            &mut stream,
            &Request::Get {
                coord: vec![1, 2, 3],
                key: "material".to_string(),
            },
        )
        .await;
        assert_eq!(good, Response::NotFound);
    }

    #[tokio::test]
    async fn re_sending_hello_re_authenticates_as_a_different_account() {
        let server = spawn_test_server(3, 10_000).await;
        let mut stream = connect(&server).await;

        hello(&mut stream, "admin", ADMIN_PASSWORD).await;
        let set = roundtrip(
            &mut stream,
            &Request::Set {
                coord: vec![1, 2, 3],
                key: "material".to_string(),
                value: Value::I64(1),
            },
        )
        .await;
        assert_eq!(set, Response::Ok);

        // Re-authenticate as the read-only account on the same connection.
        hello(&mut stream, "viewer", VIEWER_PASSWORD).await;
        let set_after_switch = roundtrip(
            &mut stream,
            &Request::Set {
                coord: vec![1, 2, 3],
                key: "material".to_string(),
                value: Value::I64(2),
            },
        )
        .await;
        assert!(matches!(set_after_switch, Response::Forbidden(_)));
    }

    #[tokio::test]
    async fn closing_the_connection_does_not_panic_the_server_task() {
        let server = spawn_test_server(3, 10_000).await;
        let mut stream = connect(&server).await;
        hello(&mut stream, "admin", ADMIN_PASSWORD).await;
        stream.shutdown().await.unwrap();
        drop(stream);

        // A brand new connection to the same server must still work fine.
        let mut stream2 = connect(&server).await;
        let response = hello(&mut stream2, "admin", ADMIN_PASSWORD).await;
        assert!(matches!(response, Response::HelloOk { .. }));
    }
}
