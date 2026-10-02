//! The binary protocol's TCP listener and per-connection handler -- see
//! `wire.rs` for the wire format itself, and the README's "Binary
//! protocol" section for why this exists alongside the REST API rather
//! than instead of it.
//!
//! Authentication *and database selection* here are per-*connection*, not
//! per-request the way HTTP Basic Auth plus a `/db/{name}/` path segment
//! are: a client sends one `Hello` (username, password, database) right
//! after connecting, and every request after that on the same connection
//! is treated as that account against that database until the connection
//! closes (or a later `Hello` re-selects either -- allowed, not required).
//! This is the one place this protocol's semantics genuinely differ from
//! the REST API's, and it's why a raw `nc`/telnet session can't just fire
//! `Get`/`Set` at this listener the way `curl` can against the REST one --
//! see `kblockdbcli` (or a future binary-protocol client) for something
//! that actually speaks it.
//!
//! If `Hello`'s named database doesn't exist yet, the account still
//! authenticates (so `CreateDatabase` can be sent), but no database is
//! selected -- see `wire.rs`'s doc comment for the full bootstrap flow.

use crate::error::ApiError;
use crate::state::{Account, AppState};
use kblockdbserver::wire::{
    self, Column, DatabaseShape, QueryResult, QueryRow, QueryValue, Request, Response,
};
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
        ApiError::Conflict(m) => Response::Conflict(m),
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
    let mut session = Session::default();

    loop {
        let payload = match wire::read_frame(&mut stream).await {
            Ok(Some(p)) => p,
            Ok(None) => return, // clean disconnect, right at a frame boundary
            Err(_) => return,   // truncated/oversized frame, or a socket error
        };

        let response = match wire::decode_request(&payload) {
            Ok(req) => handle_request(req, &state, &mut session).await,
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

/// A connection's state: the account `Hello` authenticated as (if any), and
/// the database it selected (if any -- see this module's doc comment on why
/// those can diverge).
#[derive(Default)]
struct Session {
    account: Option<Account>,
    database: Option<String>,
}

/// This connection's account so far, or a `Response` explaining why there
/// isn't one yet -- shared by every request kind that needs *some*
/// authenticated account (i.e. everything but `Hello`/`Health`).
fn require_authenticated(session: &Session) -> Result<&Account, Response> {
    session
        .account
        .as_ref()
        .ok_or_else(|| Response::Unauthorized("send Hello first".to_string()))
}

/// Same as `require_authenticated`, plus the `read_only` check every
/// `Set`/`Remove`/`CreateDatabase`/`RemoveDatabase` needs -- mirrors
/// `auth.rs`'s middleware, just checked here instead of before a handler
/// runs, since this protocol has no middleware layer to put it in.
fn require_write(session: &Session) -> Result<(), Response> {
    let account = require_authenticated(session)?;
    if account.read_only {
        return Err(Response::Forbidden("this account is read-only".to_string()));
    }
    Ok(())
}

/// This connection's selected database, or a `Response` explaining why
/// there isn't one -- every data request (`Get`/`Set`/`Query`/
/// `ListColumns`/...) needs this; `ListDatabases`/`CreateDatabase`/
/// `RemoveDatabase` don't (see this module's doc comment).
fn require_database(session: &Session) -> Result<&str, Response> {
    require_authenticated(session)?;
    session.database.as_deref().ok_or_else(|| {
        Response::BadRequest(
            "no database selected -- Hello with an existing database name, or create one \
             first with CreateDatabase and Hello again"
                .to_string(),
        )
    })
}

/// `require_database` plus the `read_only` check -- the write-request
/// equivalent.
fn require_write_database(session: &Session) -> Result<&str, Response> {
    require_write(session)?;
    // `require_write` already confirmed `session.account` is `Some`, so
    // this can't fail on the auth half -- only ever on "no database".
    require_database(session)
}

async fn handle_request(req: Request, state: &AppState, session: &mut Session) -> Response {
    match req {
        Request::Hello {
            username,
            password,
            database,
        } => match state.authenticate(&username, &password) {
            Some(acc) => {
                let read_only = acc.read_only;
                session.account = Some(acc);
                match state.resolve_database(&database).await {
                    Ok(world) => {
                        session.database = Some(database);
                        Response::HelloOk {
                            read_only,
                            database: Some(DatabaseShape {
                                axes: world.axes() as u8,
                                world_dim: world.world_dim(),
                                chunk_dim: world.chunk_dim(),
                            }),
                        }
                    }
                    Err(_) => {
                        session.database = None;
                        Response::HelloOk {
                            read_only,
                            database: None,
                        }
                    }
                }
            }
            None => Response::Unauthorized("invalid username or password".to_string()),
        },
        Request::Get { coord, key } => {
            let db = match require_database(session) {
                Ok(db) => db.to_string(),
                Err(response) => return response,
            };
            match state
                .with_database(&db, move |w| w.get_with_meta(&coord, &key))
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
            let db = match require_write_database(session) {
                Ok(db) => db.to_string(),
                Err(response) => return response,
            };
            match state
                .with_database(&db, move |w| w.set(&coord, &key, value))
                .await
            {
                Ok(()) => Response::Ok,
                Err(e) => response_from_error(e),
            }
        }
        Request::Remove { coord, key } => {
            let db = match require_write_database(session) {
                Ok(db) => db.to_string(),
                Err(response) => return response,
            };
            match state
                .with_database(&db, move |w| w.remove(&coord, &key))
                .await
            {
                Ok(()) => Response::Ok,
                Err(e) => response_from_error(e),
            }
        }
        Request::Health => {
            let database_count = match state.list_databases().await {
                Ok(names) => names.len() as u32,
                Err(e) => return response_from_error(e),
            };
            Response::Health {
                timestamp: crate::routes::unix_timestamp(),
                database_count,
                hostname: state.hostname.to_string(),
            }
        }
        Request::Stats => {
            let db = match require_database(session) {
                Ok(db) => db.to_string(),
                Err(response) => return response,
            };
            match state.with_database(&db, kblockdblib::World::stats).await {
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
            let db = match require_database(session) {
                Ok(db) => db.to_string(),
                Err(response) => return response,
            };
            let region = kblockdblib::Region::new(origin, extent);
            match state
                .with_database(&db, move |w| w.get_region(&region, &key))
                .await
            {
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
            let db = match require_write_database(session) {
                Ok(db) => db.to_string(),
                Err(response) => return response,
            };
            let region = kblockdblib::Region::new(origin, extent);
            match state
                .with_database(&db, move |w| w.set_region(&region, &key, &values))
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
            let db = match require_write_database(session) {
                Ok(db) => db.to_string(),
                Err(response) => return response,
            };
            let region = kblockdblib::Region::new(origin, extent);
            match state
                .with_database(&db, move |w| w.remove_region(&region, &key))
                .await
            {
                Ok(()) => Response::Ok,
                Err(e) => response_from_error(e),
            }
        }
        Request::ListColumns => {
            let db = match require_database(session) {
                Ok(db) => db.to_string(),
                Err(response) => return response,
            };
            match state.with_database(&db, |w| Ok(w.columns())).await {
                Ok(columns) => Response::Columns(
                    columns
                        .into_iter()
                        .map(|c| Column {
                            key: c.key,
                            value_type: c.value_type,
                        })
                        .collect(),
                ),
                Err(e) => response_from_error(e),
            }
        }
        Request::AddColumn { key, value_type } => {
            let db = match require_write_database(session) {
                Ok(db) => db.to_string(),
                Err(response) => return response,
            };
            match state
                .with_database(&db, move |w| w.add_column(&key, value_type))
                .await
            {
                Ok(()) => Response::Ok,
                Err(e) => response_from_error(e),
            }
        }
        Request::RemoveColumn { key } => {
            let db = match require_write_database(session) {
                Ok(db) => db.to_string(),
                Err(response) => return response,
            };
            match state
                .with_database(&db, move |w| w.remove_column(&key))
                .await
            {
                Ok(true) => Response::Ok,
                Ok(false) => Response::NotFound,
                Err(e) => response_from_error(e),
            }
        }
        Request::Query { query } => {
            let db = match require_database(session) {
                Ok(db) => db.to_string(),
                Err(response) => return response,
            };
            // `require_database` already confirmed `session.account` is
            // `Some`.
            let account = session.account.clone().unwrap();
            match crate::routes::execute_query(state, &db, &account, &query).await {
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
        Request::ListDatabases => {
            if let Err(response) = require_authenticated(session) {
                return response;
            }
            match state.list_databases().await {
                Ok(names) => Response::Databases(names),
                Err(e) => response_from_error(e),
            }
        }
        Request::CreateDatabase { name } => {
            if let Err(response) = require_write(session) {
                return response;
            }
            match state.create_database(&name, None).await {
                Ok(()) => Response::Ok,
                Err(e) => response_from_error(e),
            }
        }
        Request::RemoveDatabase { name } => {
            if let Err(response) = require_write(session) {
                return response;
            }
            match state.remove_database(&name).await {
                Ok(true) => Response::Ok,
                Ok(false) => Response::NotFound,
                Err(e) => response_from_error(e),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{Databases, WorldShape};
    use kblockdblib::Value;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpStream as ClientStream;

    const ADMIN_PASSWORD: &str = "admin-pw";
    const VIEWER_PASSWORD: &str = "viewer-pw";
    /// The database every `spawn_test_server` pre-creates, so existing data
    /// tests don't each need their own `CreateDatabase` round trip -- tests
    /// that exercise the "no database selected"/bootstrap flow itself use a
    /// name that was deliberately never created instead.
    const DB: &str = "db";

    /// Spawns a real binary-protocol listener on an OS-assigned port,
    /// backed by a fresh temp data dir with one pre-created database (`DB`,
    /// shaped `axes`/`world_dim` as given), two accounts: `admin` full
    /// access, `viewer` read-only. The temp directory is removed when the
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
        spawn_server(axes, world_dim, None).await
    }

    async fn spawn_test_server_with_hostname(
        axes: usize,
        world_dim: u32,
        hostname: &str,
    ) -> TestServer {
        spawn_server(axes, world_dim, Some(hostname.to_string())).await
    }

    async fn spawn_server(axes: usize, world_dim: u32, hostname: Option<String>) -> TestServer {
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "kblockdbserver-binary-test-{n}-{}",
            std::process::id()
        ));
        let shape = WorldShape {
            axes,
            world_dim,
            chunk_dim: 32,
        };
        let databases = Databases::new(&dir, shape);
        databases.create(DB, None).unwrap();

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
        let mut state = AppState::new(databases, Arc::new(credentials));
        if let Some(hostname) = hostname {
            state = state.with_hostname(hostname);
        }

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

    /// `Hello` against `DB` -- the database every `spawn_test_server`
    /// pre-creates -- the shape almost every test below needs.
    async fn hello(stream: &mut ClientStream, username: &str, password: &str) -> Response {
        hello_db(stream, username, password, DB).await
    }

    async fn hello_db(
        stream: &mut ClientStream,
        username: &str,
        password: &str,
        database: &str,
    ) -> Response {
        roundtrip(
            stream,
            &Request::Hello {
                username: username.to_string(),
                password: password.to_string(),
                database: database.to_string(),
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
    async fn hello_with_correct_credentials_returns_the_databases_shape() {
        let server = spawn_test_server(3, 10_000).await;
        let mut stream = connect(&server).await;

        let response = hello(&mut stream, "admin", ADMIN_PASSWORD).await;
        assert_eq!(
            response,
            Response::HelloOk {
                read_only: false,
                database: Some(DatabaseShape {
                    axes: 3,
                    world_dim: 10_000,
                    chunk_dim: 32,
                }),
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
                read_only: true,
                database: Some(DatabaseShape {
                    axes: 3,
                    world_dim: 10_000,
                    chunk_dim: 32,
                }),
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
    async fn hello_against_an_unknown_database_still_authenticates_but_selects_nothing() {
        let server = spawn_test_server(3, 10_000).await;
        let mut stream = connect(&server).await;

        let response = hello_db(&mut stream, "admin", ADMIN_PASSWORD, "nope").await;
        assert_eq!(
            response,
            Response::HelloOk {
                read_only: false,
                database: None,
            }
        );
    }

    #[tokio::test]
    async fn data_ops_on_a_connection_with_no_database_selected_are_bad_requests() {
        let server = spawn_test_server(3, 10_000).await;
        let mut stream = connect(&server).await;
        hello_db(&mut stream, "admin", ADMIN_PASSWORD, "nope").await;

        for request in [
            Request::Get {
                coord: vec![1, 2, 3],
                key: "material".to_string(),
            },
            Request::Set {
                coord: vec![1, 2, 3],
                key: "material".to_string(),
                value: Value::I64(1),
            },
            Request::Stats,
            Request::ListColumns,
            Request::Query {
                query: "SELECT *".to_string(),
            },
        ] {
            let response = roundtrip(&mut stream, &request).await;
            assert!(
                matches!(response, Response::BadRequest(_)),
                "got {response:?}"
            );
        }
    }

    #[tokio::test]
    async fn creating_the_selected_database_then_re_hello_selects_it() {
        let server = spawn_test_server(3, 10_000).await;
        let mut stream = connect(&server).await;

        assert_eq!(
            hello_db(&mut stream, "admin", ADMIN_PASSWORD, "fresh").await,
            Response::HelloOk {
                read_only: false,
                database: None,
            }
        );

        let create = roundtrip(
            &mut stream,
            &Request::CreateDatabase {
                name: "fresh".to_string(),
            },
        )
        .await;
        assert_eq!(create, Response::Ok);

        // Data ops still fail until Hello re-selects it.
        let too_soon = roundtrip(
            &mut stream,
            &Request::Get {
                coord: vec![0, 0, 0],
                key: "k".to_string(),
            },
        )
        .await;
        assert!(matches!(too_soon, Response::BadRequest(_)));

        let response = hello_db(&mut stream, "admin", ADMIN_PASSWORD, "fresh").await;
        assert_eq!(
            response,
            Response::HelloOk {
                read_only: false,
                database: Some(DatabaseShape {
                    axes: 3,
                    world_dim: 10_000,
                    chunk_dim: 32,
                }),
            }
        );
        let get = roundtrip(
            &mut stream,
            &Request::Get {
                coord: vec![0, 0, 0],
                key: "k".to_string(),
            },
        )
        .await;
        assert_eq!(get, Response::NotFound);
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
    async fn health_reports_the_database_count_and_this_instances_hostname() {
        let server = spawn_test_server(3, 10_000).await;
        let mut stream = connect(&server).await;

        match roundtrip(&mut stream, &Request::Health).await {
            Response::Health {
                database_count,
                hostname,
                ..
            } => {
                // `spawn_test_server` pre-creates exactly one database.
                assert_eq!(database_count, 1);
                // Which hostname the test machine has isn't knowable
                // here; that one was reported at all is.
                assert!(!hostname.is_empty());
            }
            other => panic!("expected Response::Health, got {other:?}"),
        }
    }

    /// The binary protocol and the REST API read these from the same
    /// `AppState`, so a `hostname` config override has to reach both.
    #[tokio::test]
    async fn health_reports_the_configured_hostname_override() {
        let server = spawn_test_server_with_hostname(3, 10_000, "db-1.example.com").await;
        let mut stream = connect(&server).await;

        match roundtrip(&mut stream, &Request::Health).await {
            Response::Health { hostname, .. } => assert_eq!(hostname, "db-1.example.com"),
            other => panic!("expected Response::Health, got {other:?}"),
        }
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

    // --- Columns (the schema API) ---

    /// Unwraps a `Response::Columns` into `(key, type)` pairs.
    fn expect_columns(response: Response) -> Vec<(String, kblockdblib::ValueType)> {
        match response {
            Response::Columns(columns) => {
                columns.into_iter().map(|c| (c.key, c.value_type)).collect()
            }
            other => panic!("expected Response::Columns, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn list_columns_on_a_fresh_world_is_empty() {
        let server = spawn_test_server(3, 10_000).await;
        let mut stream = connect(&server).await;
        hello(&mut stream, "admin", ADMIN_PASSWORD).await;

        let response = roundtrip(&mut stream, &Request::ListColumns).await;
        assert_eq!(expect_columns(response), vec![]);
    }

    #[tokio::test]
    async fn add_column_then_list_reports_it_sorted_by_key() {
        let server = spawn_test_server(3, 10_000).await;
        let mut stream = connect(&server).await;
        hello(&mut stream, "admin", ADMIN_PASSWORD).await;

        for (key, value_type) in [
            ("material", kblockdblib::ValueType::Str),
            ("hardness", kblockdblib::ValueType::F64),
        ] {
            let response = roundtrip(
                &mut stream,
                &Request::AddColumn {
                    key: key.to_string(),
                    value_type,
                },
            )
            .await;
            assert_eq!(response, Response::Ok);
        }

        let response = roundtrip(&mut stream, &Request::ListColumns).await;
        assert_eq!(
            expect_columns(response),
            vec![
                ("hardness".to_string(), kblockdblib::ValueType::F64),
                ("material".to_string(), kblockdblib::ValueType::Str),
            ]
        );
    }

    #[tokio::test]
    async fn adding_a_column_that_already_exists_is_a_conflict() {
        let server = spawn_test_server(3, 10_000).await;
        let mut stream = connect(&server).await;
        hello(&mut stream, "admin", ADMIN_PASSWORD).await;

        let add = || Request::AddColumn {
            key: "material".to_string(),
            value_type: kblockdblib::ValueType::Str,
        };
        assert_eq!(roundtrip(&mut stream, &add()).await, Response::Ok);
        let response = roundtrip(&mut stream, &add()).await;
        assert!(
            matches!(response, Response::Conflict(_)),
            "got {response:?}"
        );
    }

    #[tokio::test]
    async fn removing_a_column_drops_its_values_too() {
        let server = spawn_test_server(3, 10_000).await;
        let mut stream = connect(&server).await;
        hello(&mut stream, "admin", ADMIN_PASSWORD).await;

        roundtrip(
            &mut stream,
            &Request::Set {
                coord: vec![1, 2, 3],
                key: "material".to_string(),
                value: Value::Str("stone".to_string()),
            },
        )
        .await;

        let response = roundtrip(
            &mut stream,
            &Request::RemoveColumn {
                key: "material".to_string(),
            },
        )
        .await;
        assert_eq!(response, Response::Ok);

        let response = roundtrip(
            &mut stream,
            &Request::Get {
                coord: vec![1, 2, 3],
                key: "material".to_string(),
            },
        )
        .await;
        assert_eq!(response, Response::NotFound);
        assert_eq!(
            expect_columns(roundtrip(&mut stream, &Request::ListColumns).await),
            vec![]
        );
    }

    #[tokio::test]
    async fn removing_a_column_that_doesnt_exist_is_not_found() {
        let server = spawn_test_server(3, 10_000).await;
        let mut stream = connect(&server).await;
        hello(&mut stream, "admin", ADMIN_PASSWORD).await;

        let response = roundtrip(
            &mut stream,
            &Request::RemoveColumn {
                key: "nope".to_string(),
            },
        )
        .await;
        assert_eq!(response, Response::NotFound);
    }

    #[tokio::test]
    async fn column_requests_require_hello_first() {
        let server = spawn_test_server(3, 10_000).await;
        let mut stream = connect(&server).await;

        for request in [
            Request::ListColumns,
            Request::AddColumn {
                key: "material".to_string(),
                value_type: kblockdblib::ValueType::Str,
            },
            Request::RemoveColumn {
                key: "material".to_string(),
            },
        ] {
            let response = roundtrip(&mut stream, &request).await;
            assert!(
                matches!(response, Response::Unauthorized(_)),
                "got {response:?}"
            );
        }
    }

    #[tokio::test]
    async fn a_read_only_account_can_list_but_not_change_columns() {
        let server = spawn_test_server(3, 10_000).await;
        let mut stream = connect(&server).await;
        hello(&mut stream, "viewer", VIEWER_PASSWORD).await;

        let response = roundtrip(&mut stream, &Request::ListColumns).await;
        assert_eq!(expect_columns(response), vec![]);

        for request in [
            Request::AddColumn {
                key: "material".to_string(),
                value_type: kblockdblib::ValueType::Str,
            },
            Request::RemoveColumn {
                key: "material".to_string(),
            },
        ] {
            let response = roundtrip(&mut stream, &request).await;
            assert!(
                matches!(response, Response::Forbidden(_)),
                "got {response:?}"
            );
        }
    }

    // --- Database management ---

    #[tokio::test]
    async fn list_databases_reports_the_pre_created_database() {
        let server = spawn_test_server(3, 10_000).await;
        let mut stream = connect(&server).await;
        hello(&mut stream, "admin", ADMIN_PASSWORD).await;

        let response = roundtrip(&mut stream, &Request::ListDatabases).await;
        assert_eq!(response, Response::Databases(vec![DB.to_string()]));
    }

    #[tokio::test]
    async fn create_then_list_then_remove_a_database_round_trips() {
        let server = spawn_test_server(3, 10_000).await;
        let mut stream = connect(&server).await;
        hello(&mut stream, "admin", ADMIN_PASSWORD).await;

        let create = roundtrip(
            &mut stream,
            &Request::CreateDatabase {
                name: "extra".to_string(),
            },
        )
        .await;
        assert_eq!(create, Response::Ok);

        let Response::Databases(mut names) = roundtrip(&mut stream, &Request::ListDatabases).await
        else {
            panic!("expected Response::Databases");
        };
        names.sort();
        assert_eq!(names, vec![DB.to_string(), "extra".to_string()]);

        let remove = roundtrip(
            &mut stream,
            &Request::RemoveDatabase {
                name: "extra".to_string(),
            },
        )
        .await;
        assert_eq!(remove, Response::Ok);
        assert_eq!(
            roundtrip(&mut stream, &Request::ListDatabases).await,
            Response::Databases(vec![DB.to_string()])
        );
    }

    #[tokio::test]
    async fn creating_a_database_that_already_exists_is_a_conflict() {
        let server = spawn_test_server(3, 10_000).await;
        let mut stream = connect(&server).await;
        hello(&mut stream, "admin", ADMIN_PASSWORD).await;

        let response = roundtrip(
            &mut stream,
            &Request::CreateDatabase {
                name: DB.to_string(),
            },
        )
        .await;
        assert!(matches!(response, Response::Conflict(_)), "{response:?}");
    }

    #[tokio::test]
    async fn removing_an_unknown_database_is_not_found() {
        let server = spawn_test_server(3, 10_000).await;
        let mut stream = connect(&server).await;
        hello(&mut stream, "admin", ADMIN_PASSWORD).await;

        let response = roundtrip(
            &mut stream,
            &Request::RemoveDatabase {
                name: "nope".to_string(),
            },
        )
        .await;
        assert_eq!(response, Response::NotFound);
    }

    #[tokio::test]
    async fn database_management_requests_require_hello_first() {
        let server = spawn_test_server(3, 10_000).await;
        let mut stream = connect(&server).await;

        for request in [
            Request::ListDatabases,
            Request::CreateDatabase {
                name: "x".to_string(),
            },
            Request::RemoveDatabase {
                name: "x".to_string(),
            },
        ] {
            let response = roundtrip(&mut stream, &request).await;
            assert!(
                matches!(response, Response::Unauthorized(_)),
                "got {response:?}"
            );
        }
    }

    #[tokio::test]
    async fn a_read_only_account_can_list_but_not_create_or_remove_databases() {
        let server = spawn_test_server(3, 10_000).await;
        let mut stream = connect(&server).await;
        hello(&mut stream, "viewer", VIEWER_PASSWORD).await;

        assert_eq!(
            roundtrip(&mut stream, &Request::ListDatabases).await,
            Response::Databases(vec![DB.to_string()])
        );

        for request in [
            Request::CreateDatabase {
                name: "extra".to_string(),
            },
            Request::RemoveDatabase {
                name: DB.to_string(),
            },
        ] {
            let response = roundtrip(&mut stream, &request).await;
            assert!(
                matches!(response, Response::Forbidden(_)),
                "got {response:?}"
            );
        }
    }

    /// A connection selecting database management requests works
    /// regardless of whether `Hello` selected a database -- the whole point
    /// of decoupling the two (see this module's doc comment).
    #[tokio::test]
    async fn database_management_works_without_a_selected_database() {
        let server = spawn_test_server(3, 10_000).await;
        let mut stream = connect(&server).await;
        hello_db(&mut stream, "admin", ADMIN_PASSWORD, "not-created-yet").await;

        let create = roundtrip(
            &mut stream,
            &Request::CreateDatabase {
                name: "not-created-yet".to_string(),
            },
        )
        .await;
        assert_eq!(create, Response::Ok);
    }

    #[tokio::test]
    async fn two_databases_are_isolated_over_one_connection() {
        let server = spawn_test_server(3, 10_000).await;
        let mut stream = connect(&server).await;
        hello(&mut stream, "admin", ADMIN_PASSWORD).await;
        roundtrip(
            &mut stream,
            &Request::CreateDatabase {
                name: "other".to_string(),
            },
        )
        .await;

        // Write to `DB` on this connection.
        roundtrip(
            &mut stream,
            &Request::Set {
                coord: vec![0, 0, 0],
                key: "k".to_string(),
                value: Value::I64(1),
            },
        )
        .await;

        // Switch this same connection to `other` and confirm it sees
        // nothing there.
        hello_db(&mut stream, "admin", ADMIN_PASSWORD, "other").await;
        let get_other = roundtrip(
            &mut stream,
            &Request::Get {
                coord: vec![0, 0, 0],
                key: "k".to_string(),
            },
        )
        .await;
        assert_eq!(get_other, Response::NotFound);

        // Switching back to `DB` still sees the original write.
        hello(&mut stream, "admin", ADMIN_PASSWORD).await;
        let get_db = roundtrip(
            &mut stream,
            &Request::Get {
                coord: vec![0, 0, 0],
                key: "k".to_string(),
            },
        )
        .await;
        assert_eq!(expect_value(get_db), Value::I64(1));
    }
}
