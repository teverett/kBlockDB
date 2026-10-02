//! HTTP-level integration tests: real requests through the real `Router`
//! (via `tower::ServiceExt::oneshot`, no TCP socket needed), one per
//! endpoint/behavior. Complements the narrower unit tests in
//! `coords.rs`/`value_json.rs`.

use crate::routes::router;
use crate::state::{Account, AppState, Databases, WorldShape};
use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum_extra::headers::{Authorization, HeaderMapExt};
use http_body_util::BodyExt;
use serde_json::{json, Value as Json};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tower::ServiceExt;

/// The accounts every `test_app()` is configured with -- `get`/`put`/
/// `delete` below authenticate as `TEST_ADMIN` by default so the existing
/// (pre-auth) tests don't need to know auth exists at all; tests that
/// exercise auth itself build requests without those helpers.
const TEST_ADMIN: &str = "admin";
const TEST_ADMIN_PASSWORD: &str = "test-admin-password";
const TEST_USER: &str = "alice";
const TEST_USER_PASSWORD: &str = "alice-password";
const TEST_READ_ONLY_USER: &str = "bob";
const TEST_READ_ONLY_PASSWORD: &str = "bob-password";

/// The shape every `test_app()`-created database uses, including the one
/// `test_app()` itself pre-creates (see `DB`).
const TEST_SHAPE: WorldShape = WorldShape {
    axes: 3,
    world_dim: 100,
    chunk_dim: 32,
};

/// The database `test_app()` pre-creates, so every test written before
/// multi-database support existed keeps working unchanged against
/// `/rest/db/{DB}/...` -- tests that exercise database management
/// (creating/listing/deleting, cross-database isolation, 404s against an
/// unknown database) use other names instead.
const DB: &str = "db";

/// Removes its temp directory when dropped. Each test binds this to a local
/// so the data dir outlives the `Router` built on top of it, then gets
/// cleaned up at the end of the test.
struct TestDir(PathBuf);

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A fresh data directory in its own temp directory, with one pre-created
/// database (`DB`, shaped `TEST_SHAPE`), wrapped in a router -- what every
/// test below starts from. Keep the returned `TestDir` alive (bind it,
/// don't `let _ =`) for the test's duration.
fn test_app() -> (axum::Router, TestDir) {
    let (databases, dir) = test_databases();
    databases.create(DB, None).unwrap();
    (router(test_state(databases)), dir)
}

/// Core of `test_app`, minus the pre-created database and the router --
/// for tests that need to drive `Databases`/`AppState` directly (e.g. to
/// reopen the same data dir with different credentials) rather than through
/// a fresh `test_app()`.
fn test_databases() -> (Databases, TestDir) {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir: PathBuf =
        std::env::temp_dir().join(format!("kblockdbserver-test-{n}-{}", std::process::id()));
    (Databases::new(&dir, TEST_SHAPE), TestDir(dir))
}

fn test_credentials() -> HashMap<String, Account> {
    let mut credentials = HashMap::new();
    credentials.insert(
        TEST_ADMIN.to_string(),
        Account {
            password: TEST_ADMIN_PASSWORD.to_string(),
            read_only: false,
        },
    );
    credentials.insert(
        TEST_USER.to_string(),
        Account {
            password: TEST_USER_PASSWORD.to_string(),
            read_only: false,
        },
    );
    credentials.insert(
        TEST_READ_ONLY_USER.to_string(),
        Account {
            password: TEST_READ_ONLY_PASSWORD.to_string(),
            read_only: true,
        },
    );
    credentials
}

fn test_state(databases: Databases) -> AppState {
    AppState::new(databases, Arc::new(test_credentials()))
}

/// Adds a Basic Auth header for `user`/`password` to `req`.
fn with_auth(mut req: Request<Body>, user: &str, password: &str) -> Request<Body> {
    req.headers_mut()
        .typed_insert(Authorization::basic(user, password));
    req
}

/// Sends `req` and returns its status plus its body. Axum's own built-in
/// extractor rejections (e.g. a malformed JSON body never reaching our
/// handlers at all) don't necessarily come back as JSON, so a body that
/// doesn't parse as JSON falls back to a plain JSON string of the raw
/// bytes rather than panicking -- tests that hit those paths only assert
/// on the status code anyway.
async fn send(app: axum::Router, req: Request<Body>) -> (StatusCode, Json) {
    let resp = app.oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let body: Json = if bytes.is_empty() {
        Json::Null
    } else {
        serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| Json::String(String::from_utf8_lossy(&bytes).into_owned()))
    };
    (status, body)
}

fn get(path: &str) -> Request<Body> {
    with_auth(
        Request::builder()
            .method("GET")
            .uri(path)
            .body(Body::empty())
            .unwrap(),
        TEST_ADMIN,
        TEST_ADMIN_PASSWORD,
    )
}

fn put(path: &str, body: Json) -> Request<Body> {
    with_auth(
        Request::builder()
            .method("PUT")
            .uri(path)
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap(),
        TEST_ADMIN,
        TEST_ADMIN_PASSWORD,
    )
}

fn post(path: &str, body: Json) -> Request<Body> {
    with_auth(
        Request::builder()
            .method("POST")
            .uri(path)
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap(),
        TEST_ADMIN,
        TEST_ADMIN_PASSWORD,
    )
}

fn delete(path: &str) -> Request<Body> {
    with_auth(
        Request::builder()
            .method("DELETE")
            .uri(path)
            .body(Body::empty())
            .unwrap(),
        TEST_ADMIN,
        TEST_ADMIN_PASSWORD,
    )
}

#[tokio::test]
async fn health_reports_status_and_the_database_count() {
    let (status, body) = send(test_app().0, get("/rest/health")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "ok");
    // `test_app()` pre-creates exactly one database (`DB`).
    assert_eq!(body["database_count"], 1);
}

#[tokio::test]
async fn health_database_count_reflects_databases_created_and_removed() {
    let (app, _dir) = test_app();
    send(app.clone(), put("/rest/databases/extra", json!({}))).await;

    let (_, body) = send(app.clone(), get("/rest/health")).await;
    assert_eq!(body["database_count"], 2);

    send(app.clone(), delete("/rest/databases/extra")).await;
    let (_, body) = send(app, get("/rest/health")).await;
    assert_eq!(body["database_count"], 1);
}

/// Nothing here can assert *which* hostname the machine running the
/// tests has, only that one was reported and that it isn't empty --
/// `os_hostname` substitutes `"unknown"` rather than ever reporting
/// nothing.
#[tokio::test]
async fn health_reports_a_non_empty_hostname() {
    let (status, body) = send(test_app().0, get("/rest/health")).await;
    assert_eq!(status, StatusCode::OK);
    let hostname = body["hostname"]
        .as_str()
        .expect("hostname should be a string");
    assert!(!hostname.is_empty());
}

#[tokio::test]
async fn health_reports_the_configured_hostname_override() {
    let (databases, _dir) = test_databases();
    let state = AppState::new(databases, Arc::new(HashMap::new()))
        .with_hostname("db-1.example.com".to_string());

    let (status, body) = send(router(state), get("/rest/health")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["hostname"], "db-1.example.com");
}

/// `/rest/health` is what a load balancer polls, so it must stay
/// unauthenticated even though it now names the instance.
#[tokio::test]
async fn health_still_needs_no_credentials() {
    let req = Request::builder()
        .method("GET")
        .uri("/rest/health")
        .body(Body::empty())
        .unwrap();
    let (status, body) = send(test_app().0, req).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["hostname"].is_string());
    assert_eq!(body["database_count"], 1);
}

#[tokio::test]
async fn health_includes_a_current_unix_timestamp() {
    let before = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let (status, body) = send(test_app().0, get("/rest/health")).await;
    assert_eq!(status, StatusCode::OK);
    let timestamp = body["timestamp"]
        .as_u64()
        .expect("timestamp should be a number");

    let after = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    assert!(
        (before..=after).contains(&timestamp),
        "timestamp {timestamp} not within [{before}, {after}]"
    );
}

#[tokio::test]
async fn stats_of_an_untouched_world_are_all_zero() {
    let (status, body) = send(test_app().0, get("/rest/db/db/stats")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body,
        json!({"total_chunks": 0, "total_bytes": 0, "total_blocks": 0})
    );
}

#[tokio::test]
async fn stats_reflect_data_written_through_the_api() {
    let (app, _dir) = test_app();
    send(
        app.clone(),
        put(
            "/rest/db/db/cells/1,2,3/material",
            json!({"type": "str", "value": "stone"}),
        ),
    )
    .await;

    let (status, body) = send(app, get("/rest/db/db/stats")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["total_chunks"], 1);
    assert!(body["total_bytes"].as_u64().unwrap() > 0);
}

#[tokio::test]
async fn stats_requires_auth() {
    let req = Request::builder()
        .method("GET")
        .uri("/rest/db/db/stats")
        .body(Body::empty())
        .unwrap();
    let (status, _) = send(test_app().0, req).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn a_read_only_user_can_get_stats() {
    let req = with_auth(
        Request::builder()
            .method("GET")
            .uri("/rest/db/db/stats")
            .body(Body::empty())
            .unwrap(),
        TEST_READ_ONLY_USER,
        TEST_READ_ONLY_PASSWORD,
    );
    let (status, _) = send(test_app().0, req).await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn get_on_a_never_set_cell_is_404() {
    let (status, body) = send(test_app().0, get("/rest/db/db/cells/1,2,3/material")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(body["error"].is_string());
}

#[tokio::test]
async fn set_then_get_a_cell_roundtrips() {
    let (app, _dir) = test_app();

    let (status, _) = send(
        app.clone(),
        put(
            "/rest/db/db/cells/1,2,3/material",
            json!({"type": "str", "value": "stone"}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (status, body) = send(app, get("/rest/db/db/cells/1,2,3/material")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["value"], json!({"type": "str", "value": "stone"}));
    // A fresh set: version 0, created_at/modified_at equal, both real
    // (nonzero) timestamps -- see get_cell_reports_incrementing_version_
    // and_stable_created_at for the overwrite case.
    assert_eq!(body["version"], 0);
    assert!(body["created_at_ms"].as_u64().unwrap() > 0);
    assert_eq!(body["created_at_ms"], body["modified_at_ms"]);
}

#[tokio::test]
async fn a_bool_cell_roundtrips_through_set_get_and_select() {
    let (app, _dir) = test_app();

    let (status, _) = send(
        app.clone(),
        put(
            "/rest/db/db/cells/1,2,3/flammable",
            json!({"type": "bool", "value": true}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (status, body) = send(app.clone(), get("/rest/db/db/cells/1,2,3/flammable")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["value"], json!({"type": "bool", "value": true}));

    let (status, body) = send(
        app,
        post(
            "/rest/db/db/query",
            query("SELECT * WHERE flammable = true"),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["total_rows"], 1);
    assert_eq!(
        body["rows"][0]["values"][0]["value"],
        json!({"type": "bool", "value": true})
    );
}

#[tokio::test]
async fn get_cell_reports_incrementing_version_and_stable_created_at() {
    let (app, _dir) = test_app();

    for value in ["stone", "air", "dirt"] {
        let (status, _) = send(
            app.clone(),
            put(
                "/rest/db/db/cells/1,2,3/material",
                json!({"type": "str", "value": value}),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
    }

    let (status, body) = send(app, get("/rest/db/db/cells/1,2,3/material")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["value"], json!({"type": "str", "value": "dirt"}));
    assert_eq!(body["version"], 2); // 3 sets total: version 0, 1, 2
    let created = body["created_at_ms"].as_u64().unwrap();
    let modified = body["modified_at_ms"].as_u64().unwrap();
    assert!(modified >= created);
}

#[tokio::test]
async fn setting_a_key_to_a_different_type_than_it_already_holds_is_400_not_a_crash() {
    let (app, _dir) = test_app();
    let (status, _) = send(
        app.clone(),
        put(
            "/rest/db/db/cells/1,2,3/material",
            json!({"type": "str", "value": "stone"}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    // Same key, a different cell (so it isn't just an overwrite), wrong
    // type -- rejected as a normal 400, not a panicked/dropped connection
    // (see kblockdblib::Schema, which is what actually enforces this).
    let (status, body) = send(
        app.clone(),
        put(
            "/rest/db/db/cells/9,9,9/material",
            json!({"type": "i64", "value": 7}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body["error"].as_str().unwrap().contains("material"));

    // The connection survives, and the original value is untouched --
    // this really was handled as an ordinary rejected request.
    let (status, body) = send(app, get("/rest/db/db/cells/1,2,3/material")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["value"], json!({"type": "str", "value": "stone"}));
}

#[tokio::test]
async fn set_then_delete_then_get_a_cell_is_404_again() {
    let (app, _dir) = test_app();
    send(
        app.clone(),
        put(
            "/rest/db/db/cells/5,5,5/hardness",
            json!({"type": "i64", "value": 10}),
        ),
    )
    .await;

    let (status, _) = send(app.clone(), delete("/rest/db/db/cells/5,5,5/hardness")).await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (status, _) = send(app, get("/rest/db/db/cells/5,5,5/hardness")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn a_cell_coordinate_with_the_wrong_axis_count_is_400() {
    let (status, body) = send(test_app().0, get("/rest/db/db/cells/1,2/material")).await; // 2 coords, 3-axis world
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body["error"].is_string());
}

#[tokio::test]
async fn a_non_numeric_coordinate_is_400() {
    let (status, _) = send(test_app().0, get("/rest/db/db/cells/1,x,3/material")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn an_out_of_bounds_coordinate_is_400() {
    // world_dim is 100 in test_app().
    let (status, _) = send(test_app().0, get("/rest/db/db/cells/999,0,0/material")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn malformed_json_body_is_a_client_error() {
    let req = with_auth(
        Request::builder()
            .method("PUT")
            .uri("/rest/db/db/cells/0,0,0/material")
            .header("content-type", "application/json")
            .body(Body::from("not json"))
            .unwrap(),
        TEST_ADMIN,
        TEST_ADMIN_PASSWORD,
    );
    let (status, _) = send(test_app().0, req).await;
    assert!(status.is_client_error(), "expected 4xx, got {status}");
    assert_ne!(
        status,
        StatusCode::UNAUTHORIZED,
        "should fail on the malformed body, not on auth"
    );
}

#[tokio::test]
async fn set_region_then_get_region_roundtrips_per_cell_values() {
    let (app, _dir) = test_app();

    let values = json!({"values": [
        {"type": "i64", "value": 0},
        {"type": "i64", "value": 1},
        {"type": "i64", "value": 2},
        {"type": "i64", "value": 3},
    ]});
    let (status, _) = send(
        app.clone(),
        put("/rest/db/db/regions/0,0,0/2,2,1/n", values),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (status, body) = send(app, get("/rest/db/db/regions/0,0,0/2,2,1/n")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body,
        json!({"values": [
            {"type": "i64", "value": 0},
            {"type": "i64", "value": 1},
            {"type": "i64", "value": 2},
            {"type": "i64", "value": 3},
        ]})
    );
}

#[tokio::test]
async fn get_region_on_an_untouched_area_is_all_null() {
    let (status, body) = send(
        test_app().0,
        get("/rest/db/db/regions/0,0,0/2,2,2/material"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["values"].as_array().unwrap().len(), 8);
    assert!(body["values"].as_array().unwrap().iter().all(Json::is_null));
}

#[tokio::test]
async fn set_region_with_the_wrong_number_of_values_is_400() {
    let values = json!({"values": [{"type": "i64", "value": 0}]}); // region holds 8 cells
    let (status, body) = send(
        test_app().0,
        put("/rest/db/db/regions/0,0,0/2,2,2/material", values),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body["error"].is_string());
}

#[tokio::test]
async fn remove_region_clears_every_cell_in_it() {
    let (app, _dir) = test_app();
    let values = json!({"values": [
        {"type": "str", "value": "stone"},
        {"type": "str", "value": "stone"},
    ]});
    send(
        app.clone(),
        put("/rest/db/db/regions/0,0,0/2,1,1/material", values),
    )
    .await;

    let (status, _) = send(
        app.clone(),
        delete("/rest/db/db/regions/0,0,0/2,1,1/material"),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (_, body) = send(app, get("/rest/db/db/regions/0,0,0/2,1,1/material")).await;
    assert!(body["values"].as_array().unwrap().iter().all(Json::is_null));
}

#[tokio::test]
async fn a_region_with_mismatched_origin_and_extent_axes_is_400() {
    let (status, _) = send(test_app().0, get("/rest/db/db/regions/0,0,0/2,2/material")).await; // 3 vs 2 axes
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn health_does_not_require_auth() {
    let req = Request::builder()
        .method("GET")
        .uri("/rest/health")
        .body(Body::empty())
        .unwrap();
    let (status, _) = send(test_app().0, req).await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn a_protected_endpoint_without_credentials_is_401() {
    let req = Request::builder()
        .method("GET")
        .uri("/rest/db/db/cells/1,2,3/material")
        .body(Body::empty())
        .unwrap();
    let (status, body) = send(test_app().0, req).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert!(body["error"].is_string());
}

#[tokio::test]
async fn a_protected_endpoint_with_the_wrong_password_is_401() {
    let req = with_auth(
        Request::builder()
            .method("GET")
            .uri("/rest/db/db/cells/1,2,3/material")
            .body(Body::empty())
            .unwrap(),
        TEST_ADMIN,
        "not-the-real-password",
    );
    let (status, _) = send(test_app().0, req).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn a_protected_endpoint_with_an_unknown_username_is_401() {
    let req = with_auth(
        Request::builder()
            .method("GET")
            .uri("/rest/db/db/cells/1,2,3/material")
            .body(Body::empty())
            .unwrap(),
        "nobody",
        "whatever",
    );
    let (status, _) = send(test_app().0, req).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn a_non_admin_configured_user_can_use_protected_endpoints() {
    let (app, _dir) = test_app();
    let req = with_auth(
        Request::builder()
            .method("PUT")
            .uri("/rest/db/db/cells/1,2,3/material")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({"type": "str", "value": "stone"}).to_string(),
            ))
            .unwrap(),
        TEST_USER,
        TEST_USER_PASSWORD,
    );
    let (status, _) = send(app, req).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn a_read_only_user_can_get_a_cell() {
    let (app, _dir) = test_app();
    send(
        app.clone(),
        put(
            "/rest/db/db/cells/1,2,3/material",
            json!({"type": "str", "value": "stone"}),
        ),
    )
    .await;

    let req = with_auth(
        Request::builder()
            .method("GET")
            .uri("/rest/db/db/cells/1,2,3/material")
            .body(Body::empty())
            .unwrap(),
        TEST_READ_ONLY_USER,
        TEST_READ_ONLY_PASSWORD,
    );
    let (status, body) = send(app, req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["value"], json!({"type": "str", "value": "stone"}));
}

#[tokio::test]
async fn a_read_only_user_cannot_put_a_cell() {
    let req = with_auth(
        Request::builder()
            .method("PUT")
            .uri("/rest/db/db/cells/1,2,3/material")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({"type": "str", "value": "stone"}).to_string(),
            ))
            .unwrap(),
        TEST_READ_ONLY_USER,
        TEST_READ_ONLY_PASSWORD,
    );
    let (status, body) = send(test_app().0, req).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert!(body["error"].is_string());
}

#[tokio::test]
async fn a_read_only_user_cannot_delete_a_cell() {
    let (app, _dir) = test_app();
    send(
        app.clone(),
        put(
            "/rest/db/db/cells/1,2,3/material",
            json!({"type": "str", "value": "stone"}),
        ),
    )
    .await;

    let req = with_auth(
        Request::builder()
            .method("DELETE")
            .uri("/rest/db/db/cells/1,2,3/material")
            .body(Body::empty())
            .unwrap(),
        TEST_READ_ONLY_USER,
        TEST_READ_ONLY_PASSWORD,
    );
    let (status, _) = send(app.clone(), req).await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    // The value survives -- the DELETE was actually refused, not silently
    // accepted and ignored.
    let (status, body) = send(app, get("/rest/db/db/cells/1,2,3/material")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["value"], json!({"type": "str", "value": "stone"}));
}

#[tokio::test]
async fn a_read_only_user_cannot_put_a_region() {
    let values = vec![json!({"type": "i64", "value": 0}); 8];
    let req = with_auth(
        Request::builder()
            .method("PUT")
            .uri("/rest/db/db/regions/0,0,0/2,2,2/material")
            .header("content-type", "application/json")
            .body(Body::from(json!({"values": values}).to_string()))
            .unwrap(),
        TEST_READ_ONLY_USER,
        TEST_READ_ONLY_PASSWORD,
    );
    let (status, _) = send(test_app().0, req).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn openapi_json_is_served_without_auth_and_describes_every_path() {
    let req = Request::builder()
        .method("GET")
        .uri("/rest/api-docs/openapi.json")
        .body(Body::empty())
        .unwrap();
    let (status, body) = send(test_app().0, req).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["openapi"].is_string());

    let paths = body["paths"]
        .as_object()
        .expect("openapi spec should have a paths object");
    for path in [
        "/rest/health",
        "/rest/databases",
        "/rest/databases/{name}",
        "/rest/db/{db}/stats",
        "/rest/db/{db}/cells/{coords}/{key}",
        "/rest/db/{db}/regions/{origin}/{extent}/{key}",
    ] {
        assert!(paths.contains_key(path), "spec is missing path {path}");
    }
}

#[tokio::test]
async fn swagger_ui_is_served_without_auth() {
    let req = Request::builder()
        .method("GET")
        .uri("/rest/swagger-ui/")
        .body(Body::empty())
        .unwrap();
    let (status, _) = send(test_app().0, req).await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn the_root_data_browser_page_requires_auth() {
    let req = Request::builder()
        .method("GET")
        .uri("/")
        .body(Body::empty())
        .unwrap();
    let (status, _) = send(test_app().0, req).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn the_root_data_browser_page_is_served_to_an_authenticated_user() {
    let req = Request::builder()
        .method("GET")
        .uri("/")
        .body(Body::empty())
        .unwrap();
    let (status, _) = send(
        test_app().0,
        with_auth(req, TEST_ADMIN, TEST_ADMIN_PASSWORD),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn rows_without_a_db_query_param_is_a_bad_request() {
    let req = with_auth(
        Request::builder()
            .method("GET")
            .uri("/rows")
            .body(Body::empty())
            .unwrap(),
        TEST_ADMIN,
        TEST_ADMIN_PASSWORD,
    );
    let (status, _) = send(test_app().0, req).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn rows_404s_for_an_unknown_database() {
    let req = with_auth(
        Request::builder()
            .method("GET")
            .uri("/rows?db=nope")
            .body(Body::empty())
            .unwrap(),
        TEST_ADMIN,
        TEST_ADMIN_PASSWORD,
    );
    let (status, _) = send(test_app().0, req).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn rows_requires_auth() {
    let req = Request::builder()
        .method("GET")
        .uri(format!("/rows?db={DB}"))
        .body(Body::empty())
        .unwrap();
    let (status, _) = send(test_app().0, req).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn a_read_only_user_can_list_rows() {
    let req = with_auth(
        Request::builder()
            .method("GET")
            .uri(format!("/rows?db={DB}"))
            .body(Body::empty())
            .unwrap(),
        TEST_READ_ONLY_USER,
        TEST_READ_ONLY_PASSWORD,
    );
    let (status, _) = send(test_app().0, req).await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn rows_of_an_untouched_world_is_an_empty_page() {
    let (status, body) = send(test_app().0, get(&format!("/rows?db={DB}"))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["total_rows"], 0);
    assert_eq!(body["rows"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn rows_lists_a_set_cell_with_its_metadata() {
    let (app, _dir) = test_app();
    send(
        app.clone(),
        put(
            "/rest/db/db/cells/1,2,3/material",
            json!({"type": "str", "value": "stone"}),
        ),
    )
    .await;

    let (status, body) = send(app, get(&format!("/rows?db={DB}"))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["total_rows"], 1);
    let row = &body["rows"][0];
    assert_eq!(row["coord"], json!([1, 2, 3]));
    assert_eq!(row["key_count"], 1);
    assert_eq!(row["keys"][0]["key"], "material");
    assert_eq!(
        row["keys"][0]["value"],
        json!({"type": "str", "value": "stone"})
    );
    assert_eq!(row["keys"][0]["version"], 0);
}

#[tokio::test]
async fn rows_are_scoped_to_the_requested_database() {
    let (app, _dir) = test_app();
    send(app.clone(), put("/rest/databases/other", json!({}))).await;
    send(
        app.clone(),
        put(
            "/rest/db/db/cells/1,2,3/material",
            json!({"type": "str", "value": "stone"}),
        ),
    )
    .await;

    let (_, db_body) = send(app.clone(), get(&format!("/rows?db={DB}"))).await;
    assert_eq!(db_body["total_rows"], 1);

    let (_, other_body) = send(app, get("/rows?db=other")).await;
    assert_eq!(other_body["total_rows"], 0);
}

#[tokio::test]
async fn rows_are_sorted_ascending_by_coordinate() {
    let (app, _dir) = test_app();
    for coords in ["40,0,0", "1,2,3", "-10,0,0", "0,0,0"] {
        send(
            app.clone(),
            put(
                &format!("/rest/db/db/cells/{coords}/k"),
                json!({"type": "i64", "value": 1}),
            ),
        )
        .await;
    }

    let (status, body) = send(app, get(&format!("/rows?db={DB}"))).await;
    assert_eq!(status, StatusCode::OK);
    let coords: Vec<_> = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["coord"].clone())
        .collect();
    assert_eq!(
        coords,
        vec![
            json!([-10, 0, 0]),
            json!([0, 0, 0]),
            json!([1, 2, 3]),
            json!([40, 0, 0]),
        ]
    );
}

#[tokio::test]
async fn rows_search_filters_by_coordinate_key_or_value() {
    let (app, _dir) = test_app();
    send(
        app.clone(),
        put(
            "/rest/db/db/cells/1,2,3/material",
            json!({"type": "str", "value": "stone"}),
        ),
    )
    .await;
    send(
        app.clone(),
        put(
            "/rest/db/db/cells/9,9,9/temperature",
            json!({"type": "f64", "value": 20.0}),
        ),
    )
    .await;

    let (_, body) = send(app.clone(), get(&format!("/rows?db={DB}&search=stone"))).await;
    assert_eq!(body["total_rows"], 1);
    assert_eq!(body["rows"][0]["coord"], json!([1, 2, 3]));

    let (_, body) = send(
        app.clone(),
        get(&format!("/rows?db={DB}&search=temperature")),
    )
    .await;
    assert_eq!(body["total_rows"], 1);
    assert_eq!(body["rows"][0]["coord"], json!([9, 9, 9]));

    let (_, body) = send(app.clone(), get(&format!("/rows?db={DB}&search=9,9,9"))).await;
    assert_eq!(body["total_rows"], 1);

    let (_, body) = send(app, get(&format!("/rows?db={DB}&search=nonexistent"))).await;
    assert_eq!(body["total_rows"], 0);
}

#[tokio::test]
async fn rows_paginates() {
    let (app, _dir) = test_app();
    for i in 0..5u32 {
        send(
            app.clone(),
            put(
                &format!("/rest/db/db/cells/{i},0,0/k"),
                json!({"type": "i64", "value": 1}),
            ),
        )
        .await;
    }

    let (status, body) = send(
        app.clone(),
        get(&format!("/rows?db={DB}&page=1&page_size=2")),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["total_rows"], 5);
    assert_eq!(body["total_pages"], 3);
    assert_eq!(body["rows"].as_array().unwrap().len(), 2);
    assert_eq!(body["rows"][0]["coord"], json!([0, 0, 0]));

    let (_, body) = send(
        app.clone(),
        get(&format!("/rows?db={DB}&page=3&page_size=2")),
    )
    .await;
    assert_eq!(body["rows"].as_array().unwrap().len(), 1);
    assert_eq!(body["rows"][0]["coord"], json!([4, 0, 0]));

    let (_, body) = send(app, get(&format!("/rows?db={DB}&page=4&page_size=2"))).await;
    assert_eq!(body["rows"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn rows_rejects_a_zero_page_or_an_oversized_page_size() {
    let (app, _dir) = test_app();
    let (status, _) = send(app.clone(), get(&format!("/rows?db={DB}&page=0"))).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let (status, _) = send(app, get(&format!("/rows?db={DB}&page_size=100000"))).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn rows_reflects_a_removed_cell() {
    let (app, _dir) = test_app();
    send(
        app.clone(),
        put(
            "/rest/db/db/cells/1,2,3/k",
            json!({"type": "i64", "value": 1}),
        ),
    )
    .await;
    send(app.clone(), delete("/rest/db/db/cells/1,2,3/k")).await;

    let (_, body) = send(app, get(&format!("/rows?db={DB}"))).await;
    assert_eq!(body["total_rows"], 0);
}

// --- /rest/query ---

fn query(q: &str) -> Json {
    json!({"query": q})
}

#[tokio::test]
async fn query_requires_auth() {
    let req = Request::builder()
        .method("POST")
        .uri("/rest/db/db/query")
        .header("content-type", "application/json")
        .body(Body::from(query("SELECT *").to_string()))
        .unwrap();
    let (status, _) = send(test_app().0, req).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn select_star_returns_every_cell_with_every_key() {
    let (app, _dir) = test_app();
    send(
        app.clone(),
        put(
            "/rest/db/db/cells/1,2,3/material",
            json!({"type": "str", "value": "stone"}),
        ),
    )
    .await;
    send(
        app.clone(),
        put(
            "/rest/db/db/cells/1,2,3/hardness",
            json!({"type": "i64", "value": 7}),
        ),
    )
    .await;

    let (status, body) = send(app, post("/rest/db/db/query", query("SELECT *"))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["total_rows"], 1);
    let row = &body["rows"][0];
    assert_eq!(row["coord"], json!([1, 2, 3]));
    assert_eq!(row["values"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn select_rows_report_each_keys_metadata() {
    let (app, _dir) = test_app();
    send(
        app.clone(),
        put(
            "/rest/db/db/cells/1,2,3/material",
            json!({"type": "str", "value": "stone"}),
        ),
    )
    .await;
    // Overwritten once, so version=1 and modified_at_ms should have moved
    // past created_at_ms -- distinct from a fresh set's version=0 (already
    // covered elsewhere), so this specifically exercises a non-zero value.
    send(
        app.clone(),
        put(
            "/rest/db/db/cells/1,2,3/material",
            json!({"type": "str", "value": "granite"}),
        ),
    )
    .await;

    let (status, body) = send(app, post("/rest/db/db/query", query("SELECT *"))).await;
    assert_eq!(status, StatusCode::OK);
    let value = &body["rows"][0]["values"][0];
    assert_eq!(value["key"], "material");
    assert_eq!(value["version"], 1);
    assert!(value["created_at_ms"].as_u64().unwrap() > 0);
    assert!(value["modified_at_ms"].as_u64().unwrap() >= value["created_at_ms"].as_u64().unwrap());
}

#[tokio::test]
async fn select_named_columns_omits_the_rest() {
    let (app, _dir) = test_app();
    send(
        app.clone(),
        put(
            "/rest/db/db/cells/1,2,3/material",
            json!({"type": "str", "value": "stone"}),
        ),
    )
    .await;
    send(
        app.clone(),
        put(
            "/rest/db/db/cells/1,2,3/hardness",
            json!({"type": "i64", "value": 7}),
        ),
    )
    .await;

    let (status, body) = send(app, post("/rest/db/db/query", query("SELECT material"))).await;
    assert_eq!(status, StatusCode::OK);
    let values = body["rows"][0]["values"].as_array().unwrap();
    assert_eq!(values.len(), 1);
    assert_eq!(values[0]["key"], "material");
}

#[tokio::test]
async fn select_count_star_counts_matching_cells() {
    let (app, _dir) = test_app();
    for (coords, material) in [("1,2,3", "stone"), ("4,5,6", "stone"), ("7,8,9", "air")] {
        send(
            app.clone(),
            put(
                &format!("/rest/db/db/cells/{coords}/material"),
                json!({"type": "str", "value": material}),
            ),
        )
        .await;
    }

    let (status, body) = send(
        app.clone(),
        post(
            "/rest/db/db/query",
            query("SELECT count(*) WHERE material = 'stone'"),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["aggregates"],
        json!([{"label": "count(*)", "value": 2.0}])
    );
    // Not a row listing -- aggregates replace rows/total_rows, they don't
    // sit alongside them.
    assert!(body.get("rows").is_none());
    assert!(body.get("total_rows").is_none());

    let (_, body) = send(app, post("/rest/db/db/query", query("SELECT count(*)"))).await;
    assert_eq!(
        body["aggregates"],
        json!([{"label": "count(*)", "value": 3.0}])
    );
}

#[tokio::test]
async fn select_sum_mean_max_min_reduce_a_keys_numeric_value() {
    let (app, _dir) = test_app();
    for (coords, density) in [("1,0,0", 2.0), ("2,0,0", 5.0), ("3,0,0", 8.0)] {
        send(
            app.clone(),
            put(
                &format!("/rest/db/db/cells/{coords}/density"),
                json!({"type": "f64", "value": density}),
            ),
        )
        .await;
    }

    let (status, body) = send(
        app,
        post(
            "/rest/db/db/query",
            query("SELECT sum(density), mean(density), max(density), min(density)"),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["aggregates"],
        json!([
            {"label": "sum(density)", "value": 15.0},
            {"label": "mean(density)", "value": 5.0},
            {"label": "max(density)", "value": 8.0},
            {"label": "min(density)", "value": 2.0},
        ])
    );
}

#[tokio::test]
async fn select_mean_max_min_of_a_key_no_matching_cell_has_is_null_not_zero() {
    let (app, _dir) = test_app();
    send(
        app.clone(),
        put(
            "/rest/db/db/cells/1,2,3/material",
            json!({"type": "str", "value": "stone"}),
        ),
    )
    .await;

    let (status, body) = send(
        app,
        post(
            "/rest/db/db/query",
            query("SELECT sum(density), mean(density), max(density), min(density)"),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["aggregates"],
        json!([
            {"label": "sum(density)", "value": 0.0},
            {"label": "mean(density)", "value": null},
            {"label": "max(density)", "value": null},
            {"label": "min(density)", "value": null},
        ])
    );
}

#[tokio::test]
async fn select_where_filters_by_value() {
    let (app, _dir) = test_app();
    send(
        app.clone(),
        put(
            "/rest/db/db/cells/1,2,3/material",
            json!({"type": "str", "value": "stone"}),
        ),
    )
    .await;
    send(
        app.clone(),
        put(
            "/rest/db/db/cells/4,5,6/material",
            json!({"type": "str", "value": "air"}),
        ),
    )
    .await;

    let (status, body) = send(
        app,
        post(
            "/rest/db/db/query",
            query("SELECT * WHERE material = 'stone'"),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["total_rows"], 1);
    assert_eq!(body["rows"][0]["coord"], json!([1, 2, 3]));
}

#[tokio::test]
async fn select_where_exists_filters_by_whether_a_key_is_set() {
    let (app, _dir) = test_app();
    send(
        app.clone(),
        put(
            "/rest/db/db/cells/1,2,3/material",
            json!({"type": "str", "value": "stone"}),
        ),
    )
    .await;
    send(
        app.clone(),
        put(
            "/rest/db/db/cells/1,2,3/density",
            json!({"type": "f64", "value": 2.6}),
        ),
    )
    .await;
    send(
        app.clone(),
        put(
            "/rest/db/db/cells/4,5,6/material",
            json!({"type": "str", "value": "air"}),
        ),
    )
    .await;

    let (status, body) = send(
        app.clone(),
        post("/rest/db/db/query", query("SELECT * WHERE EXISTS(density)")),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["total_rows"], 1);
    assert_eq!(body["rows"][0]["coord"], json!([1, 2, 3]));

    let (_, body) = send(
        app,
        post(
            "/rest/db/db/query",
            query("SELECT * WHERE NOT EXISTS(density)"),
        ),
    )
    .await;
    assert_eq!(body["total_rows"], 1);
    assert_eq!(body["rows"][0]["coord"], json!([4, 5, 6]));
}

#[tokio::test]
async fn select_where_filters_by_axis_coordinate() {
    let (app, _dir) = test_app();
    for coords in ["1,1,1", "40,40,40"] {
        send(
            app.clone(),
            put(
                &format!("/rest/db/db/cells/{coords}/k"),
                json!({"type": "i64", "value": 1}),
            ),
        )
        .await;
    }

    let (_, body) = send(
        app,
        post("/rest/db/db/query", query("SELECT * WHERE x0 >= 10")),
    )
    .await;
    assert_eq!(body["total_rows"], 1);
    assert_eq!(body["rows"][0]["coord"], json!([40, 40, 40]));
}

#[tokio::test]
async fn select_from_range_scopes_to_the_box() {
    let (app, _dir) = test_app();
    for coords in ["1,1,1", "40,40,40"] {
        send(
            app.clone(),
            put(
                &format!("/rest/db/db/cells/{coords}/k"),
                json!({"type": "i64", "value": 1}),
            ),
        )
        .await;
    }

    let (_, body) = send(
        app,
        post(
            "/rest/db/db/query",
            query("SELECT * FROM (0,0,0) TO (10,10,10)"),
        ),
    )
    .await;
    assert_eq!(body["total_rows"], 1);
    assert_eq!(body["rows"][0]["coord"], json!([1, 1, 1]));
}

#[tokio::test]
async fn a_malformed_query_is_400() {
    let (app, _dir) = test_app();
    let (status, body) = send(
        app,
        post("/rest/db/db/query", query("NOT VALID SQL AT ALL")),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body["error"].is_string());
}

#[tokio::test]
async fn a_range_with_the_wrong_axis_count_is_400() {
    let (app, _dir) = test_app();
    let (status, _) = send(
        app,
        post("/rest/db/db/query", query("SELECT * FROM (0,0) TO (10,10)")),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn update_writes_the_given_keys_to_every_matching_cell() {
    let (app, _dir) = test_app();
    for coords in ["1,1,1", "2,2,2"] {
        send(
            app.clone(),
            put(
                &format!("/rest/db/db/cells/{coords}/k"),
                json!({"type": "i64", "value": 1}),
            ),
        )
        .await;
    }

    let (status, body) = send(
        app.clone(),
        post(
            "/rest/db/db/query",
            query("UPDATE (material='stone', hardness=7) WHERE k = 1"),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["affected_cells"], 2);

    let (_, cell) = send(app, get("/rest/db/db/cells/1,1,1/material")).await;
    assert_eq!(cell["value"], json!({"type": "str", "value": "stone"}));
}

#[tokio::test]
async fn update_never_creates_a_cell_that_does_not_already_exist() {
    // Regression: UPDATE is the old SET behavior -- it must only ever
    // touch cells World::list_cells already reports, never bring a new
    // one into existence the way SET (upsert) now can.
    let (app, _dir) = test_app();
    let (status, body) = send(
        app.clone(),
        post(
            "/rest/db/db/query",
            query("UPDATE (material='stone') IN (0,0,0) TO (10,10,10)"),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["affected_cells"], 0);

    let (status, _) = send(app, get("/rest/db/db/cells/1,1,1/material")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn a_read_only_account_cannot_set() {
    let (app, _dir) = test_app();
    let req = with_auth(
        post(
            "/rest/db/db/query",
            query("SET (k = 1) IN (0,0,0) TO (1,1,1)"),
        ),
        TEST_READ_ONLY_USER,
        TEST_READ_ONLY_PASSWORD,
    );
    let (status, _) = send(app, req).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn a_read_only_account_cannot_update() {
    let (app, _dir) = test_app();
    let req = with_auth(
        post("/rest/db/db/query", query("UPDATE (k = 1)")),
        TEST_READ_ONLY_USER,
        TEST_READ_ONLY_PASSWORD,
    );
    let (status, _) = send(app, req).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn set_without_a_range_is_400() {
    // SET is an upsert -- unlike UPDATE, it requires IN <range> (see
    // kblockdbquery's doc comment), so omitting it is a parse error, not a
    // request that silently does nothing.
    let (app, _dir) = test_app();
    let (status, _) = send(app, post("/rest/db/db/query", query("SET (k = 1)"))).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn set_upserts_every_cell_in_range_when_there_is_no_where_clause() {
    let (app, _dir) = test_app();

    let (status, body) = send(
        app.clone(),
        post(
            "/rest/db/db/query",
            query("SET (material='stone') IN (0,0,0) TO (2,2,1)"),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["affected_cells"], 4); // 2x2x1 box

    for coords in ["0,0,0", "1,0,0", "0,1,0", "1,1,0"] {
        let (_, cell) = send(
            app.clone(),
            get(&format!("/rest/db/db/cells/{coords}/material")),
        )
        .await;
        assert_eq!(cell["value"], json!({"type": "str", "value": "stone"}));
    }
}

#[tokio::test]
async fn set_with_an_axis_where_clause_only_upserts_matching_coordinates() {
    let (app, _dir) = test_app();

    let (status, body) = send(
        app.clone(),
        post(
            "/rest/db/db/query",
            query("SET (material='stone') WHERE x0 >= 1 IN (0,0,0) TO (2,1,1)"),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["affected_cells"], 1);

    let (status, _) = send(app.clone(), get("/rest/db/db/cells/0,0,0/material")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (_, cell) = send(app, get("/rest/db/db/cells/1,0,0/material")).await;
    assert_eq!(cell["value"], json!({"type": "str", "value": "stone"}));
}

#[tokio::test]
async fn set_with_a_key_based_where_clause_never_creates_a_new_cell() {
    // A key-based WHERE can never match a cell that doesn't exist yet (same
    // "missing key never matches" rule eval uses everywhere else), so this
    // behaves like UPDATE even though it's SET -- see kblockdbquery's doc
    // comment on why that's the intended, not a surprising, outcome.
    let (app, _dir) = test_app();
    send(
        app.clone(),
        put(
            "/rest/db/db/cells/0,0,0/material",
            json!({"type": "str", "value": "stone"}),
        ),
    )
    .await;

    let (status, body) = send(
        app.clone(),
        post(
            "/rest/db/db/query",
            query("SET (hardness=7) WHERE material = 'stone' IN (0,0,0) TO (2,2,2)"),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["affected_cells"], 1);

    let (_, cell) = send(app.clone(), get("/rest/db/db/cells/0,0,0/hardness")).await;
    assert_eq!(cell["value"], json!({"type": "i64", "value": 7}));
    // Never-populated neighbor cells in the range must not have been
    // created just because they were in bounds.
    let (status, _) = send(app, get("/rest/db/db/cells/1,1,1/hardness")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn a_read_only_account_can_select() {
    let (app, _dir) = test_app();
    let req = with_auth(
        post("/rest/db/db/query", query("SELECT *")),
        TEST_READ_ONLY_USER,
        TEST_READ_ONLY_PASSWORD,
    );
    let (status, _) = send(app, req).await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn delete_removes_every_key_at_matching_cells() {
    let (app, _dir) = test_app();
    send(
        app.clone(),
        put(
            "/rest/db/db/cells/1,2,3/material",
            json!({"type": "str", "value": "stone"}),
        ),
    )
    .await;
    send(
        app.clone(),
        put(
            "/rest/db/db/cells/1,2,3/hardness",
            json!({"type": "i64", "value": 7}),
        ),
    )
    .await;
    send(
        app.clone(),
        put(
            "/rest/db/db/cells/9,9,9/material",
            json!({"type": "str", "value": "air"}),
        ),
    )
    .await;

    let (status, body) = send(
        app.clone(),
        post(
            "/rest/db/db/query",
            query("DELETE WHERE material = 'stone'"),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["affected_cells"], 1);

    let (status, _) = send(app.clone(), get("/rest/db/db/cells/1,2,3/material")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = send(app, get("/rest/db/db/cells/9,9,9/material")).await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn a_read_only_account_cannot_delete() {
    let (app, _dir) = test_app();
    let req = with_auth(
        post("/rest/db/db/query", query("DELETE")),
        TEST_READ_ONLY_USER,
        TEST_READ_ONLY_PASSWORD,
    );
    let (status, _) = send(app, req).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn query_is_documented_in_the_openapi_spec() {
    let req = Request::builder()
        .method("GET")
        .uri("/rest/api-docs/openapi.json")
        .body(Body::empty())
        .unwrap();
    let (_, body) = send(test_app().0, req).await;
    let paths = body["paths"].as_object().unwrap();
    assert!(
        paths.contains_key("/rest/db/{db}/query"),
        "spec is missing /rest/db/{{db}}/query"
    );
}

// --- Columns (the schema API) ---

#[tokio::test]
async fn columns_of_a_fresh_world_are_empty() {
    let (app, _dir) = test_app();
    let (status, body) = send(app, get("/rest/db/db/columns")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["columns"], json!([]));
}

#[tokio::test]
async fn add_column_then_list_reports_it() {
    let (app, _dir) = test_app();
    let (status, _) = send(
        app.clone(),
        put("/rest/db/db/columns/hardness", json!({"type": "f64"})),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (status, body) = send(app, get("/rest/db/db/columns")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["columns"], json!([{"key": "hardness", "type": "f64"}]));
}

/// Writing a cell interns its key, so the schema picks up columns nobody
/// ever declared explicitly -- `/rest/columns` has to show those too, or
/// it'd be describing only half the world's shape.
#[tokio::test]
async fn setting_a_cell_creates_a_column_implicitly() {
    let (app, _dir) = test_app();
    let (status, _) = send(
        app.clone(),
        put(
            "/rest/db/db/cells/1,2,3/material",
            json!({"type": "str", "value": "stone"}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (_, body) = send(app, get("/rest/db/db/columns")).await;
    assert_eq!(body["columns"], json!([{"key": "material", "type": "str"}]));
}

#[tokio::test]
async fn columns_are_listed_sorted_by_key() {
    let (app, _dir) = test_app();
    for key in ["material", "hardness", "visible"] {
        let (status, _) = send(
            app.clone(),
            put(
                &format!("/rest/db/db/columns/{key}"),
                json!({"type": "str"}),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
    }
    let (_, body) = send(app, get("/rest/db/db/columns")).await;
    let keys: Vec<&str> = body["columns"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["key"].as_str().unwrap())
        .collect();
    assert_eq!(keys, ["hardness", "material", "visible"]);
}

#[tokio::test]
async fn adding_a_column_that_already_exists_is_409() {
    let (app, _dir) = test_app();
    send(
        app.clone(),
        put("/rest/db/db/columns/material", json!({"type": "str"})),
    )
    .await;
    let (status, body) = send(
        app,
        put("/rest/db/db/columns/material", json!({"type": "str"})),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert!(body["error"].as_str().unwrap().contains("material"));
}

#[tokio::test]
async fn adding_a_column_with_an_unknown_type_is_a_client_error() {
    let (app, _dir) = test_app();
    let (status, _) = send(
        app,
        put("/rest/db/db/columns/material", json!({"type": "blob"})),
    )
    .await;
    // Axum's own `Json` extractor rejects this before the handler runs,
    // same as any other unparseable body -- see
    // `malformed_json_body_is_a_client_error`.
    assert!(status.is_client_error(), "expected 4xx, got {status}");
}

#[tokio::test]
async fn adding_a_column_whose_key_the_schema_cant_store_is_400() {
    let (app, _dir) = test_app();
    // A tab would split the key across `schema.txt`'s own field
    // separator, so the schema rejects it rather than writing a line it
    // couldn't read back.
    let (status, _) = send(
        app,
        put("/rest/db/db/columns/bad%09key", json!({"type": "str"})),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn removing_a_column_drops_its_values_too() {
    let (app, _dir) = test_app();
    send(
        app.clone(),
        put(
            "/rest/db/db/cells/1,2,3/material",
            json!({"type": "str", "value": "stone"}),
        ),
    )
    .await;

    let (status, _) = send(app.clone(), delete("/rest/db/db/columns/material")).await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (status, _) = send(app.clone(), get("/rest/db/db/cells/1,2,3/material")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (_, body) = send(app, get("/rest/db/db/columns")).await;
    assert_eq!(body["columns"], json!([]));
}

/// A removed column's type isn't sticky: the key gets a fresh id when it
/// comes back, so it can be re-created as something else entirely.
#[tokio::test]
async fn a_removed_column_can_be_re_added_with_a_different_type() {
    let (app, _dir) = test_app();
    send(
        app.clone(),
        put("/rest/db/db/columns/material", json!({"type": "str"})),
    )
    .await;
    send(app.clone(), delete("/rest/db/db/columns/material")).await;

    let (status, _) = send(
        app.clone(),
        put("/rest/db/db/columns/material", json!({"type": "i64"})),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (_, body) = send(app, get("/rest/db/db/columns")).await;
    assert_eq!(body["columns"], json!([{"key": "material", "type": "i64"}]));
}

#[tokio::test]
async fn removing_a_column_that_doesnt_exist_is_404() {
    let (app, _dir) = test_app();
    let (status, body) = send(app, delete("/rest/db/db/columns/nope")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(body["error"].as_str().unwrap().contains("nope"));
}

#[tokio::test]
async fn listing_columns_requires_auth() {
    let (app, _dir) = test_app();
    let req = Request::builder()
        .method("GET")
        .uri("/rest/db/db/columns")
        .body(Body::empty())
        .unwrap();
    let (status, _) = send(app, req).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn a_read_only_user_can_list_but_not_change_columns() {
    let (app, _dir) = test_app();
    let list = with_auth(
        Request::builder()
            .method("GET")
            .uri("/rest/db/db/columns")
            .body(Body::empty())
            .unwrap(),
        TEST_READ_ONLY_USER,
        TEST_READ_ONLY_PASSWORD,
    );
    let (status, _) = send(app.clone(), list).await;
    assert_eq!(status, StatusCode::OK);

    let add = with_auth(
        Request::builder()
            .method("PUT")
            .uri("/rest/db/db/columns/material")
            .header("content-type", "application/json")
            .body(Body::from(json!({"type": "str"}).to_string()))
            .unwrap(),
        TEST_READ_ONLY_USER,
        TEST_READ_ONLY_PASSWORD,
    );
    let (status, _) = send(app.clone(), add).await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    let remove = with_auth(
        Request::builder()
            .method("DELETE")
            .uri("/rest/db/db/columns/material")
            .body(Body::empty())
            .unwrap(),
        TEST_READ_ONLY_USER,
        TEST_READ_ONLY_PASSWORD,
    );
    let (status, _) = send(app, remove).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

#[test]
fn bind_addr_listens_on_every_interface() {
    // Only a port is configurable, so this is what makes the server
    // reachable at whatever address the host happens to have.
    let addr = crate::bind_addr(8080);
    assert!(addr.ip().is_unspecified(), "{addr} isn't a wildcard bind");
    assert_eq!(addr.port(), 8080);
    // IPv4 specifically: an IPv6 wildcard doesn't accept IPv4 on
    // FreeBSD, where net.inet6.ip6.v6only defaults to 1.
    assert!(addr.is_ipv4(), "{addr} should be an IPv4 wildcard");
}

#[test]
fn bind_addr_passes_port_zero_through_for_the_os_to_choose() {
    assert_eq!(crate::bind_addr(0).port(), 0);
}

#[test]
fn base_url_keeps_a_concrete_bind_address_as_is() {
    assert_eq!(
        crate::base_url("127.0.0.1:8080".parse().unwrap()),
        "http://127.0.0.1:8080"
    );
    assert_eq!(
        crate::base_url("192.168.1.5:9000".parse().unwrap()),
        "http://192.168.1.5:9000"
    );
}

#[test]
fn base_url_resolves_a_wildcard_bind_to_a_connectable_address() {
    // `http://0.0.0.0:8080` is not something a client can connect to,
    // so the banner must never print the wildcard back. Which concrete
    // address is right depends on the host's routing (this test machine
    // may or may not have a route out), so assert the property that
    // actually matters rather than a specific IP.
    for bind in ["0.0.0.0:8080", "[::]:8080"] {
        let url = crate::base_url(bind.parse().unwrap());
        assert!(
            !url.contains("0.0.0.0") && !url.contains("[::]"),
            "{bind} printed back a wildcard: {url}"
        );
        assert!(url.ends_with(":8080"), "{bind} lost its port: {url}");
    }
}

#[test]
fn a_wildcard_bind_prefers_the_host_ip_over_loopback() {
    // The whole point of binding every interface is being reachable from
    // other machines, so the banner should name an address that gets
    // someone there -- loopback only when the host has no route out.
    let shown = crate::reachable_addr("0.0.0.0:8080".parse().unwrap()).ip();
    match crate::primary_local_ip() {
        Some(ip) => assert_eq!(shown, ip, "a routable host IP exists but wasn't used"),
        None => assert!(
            shown.is_loopback(),
            "no route out, so loopback was expected"
        ),
    }
}

#[test]
fn primary_local_ip_is_never_a_useless_answer() {
    // Loopback or `0.0.0.0` would both defeat the purpose -- the caller
    // treats `None` as "fall back", so those must not come back as
    // `Some`.
    if let Some(ip) = crate::primary_local_ip() {
        assert!(!ip.is_loopback(), "{ip} is loopback");
        assert!(!ip.is_unspecified(), "{ip} is unspecified");
    }
}

#[test]
fn base_url_brackets_ipv6_so_the_port_is_unambiguous() {
    assert_eq!(
        crate::base_url("[::1]:8080".parse().unwrap()),
        "http://[::1]:8080"
    );
}

// --- Database management (/rest/databases) ---

#[tokio::test]
async fn databases_lists_the_pre_created_database() {
    let (status, body) = send(test_app().0, get("/rest/databases")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["databases"], json!([DB]));
}

#[tokio::test]
async fn creating_a_database_with_default_shape_then_using_it_round_trips() {
    let (app, dir) = test_app();
    let (status, _) = send(app.clone(), put("/rest/databases/extra", json!({}))).await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    // It's on disk, as its own directory with its own world.txt.
    assert!(dir.0.join("extra").join("world.txt").is_file());

    let mut names = send(app.clone(), get("/rest/databases"))
        .await
        .1
        .get("databases")
        .unwrap()
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect::<Vec<_>>();
    names.sort();
    assert_eq!(names, vec![DB.to_string(), "extra".to_string()]);

    // Usable immediately for data ops, shaped like `TEST_SHAPE` (the
    // server's configured default, since no override was given).
    let (status, _) = send(
        app,
        put(
            "/rest/db/extra/cells/1,2,3/material",
            json!({"type": "str", "value": "stone"}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn creating_a_database_honors_an_explicit_shape_override() {
    let (app, _dir) = test_app();
    let (status, _) = send(
        app.clone(),
        put(
            "/rest/databases/small",
            json!({"axes": 1, "world_dim": 10, "chunk_size": 4}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    // A 2-axis coordinate against a 1-axis database is rejected, proving
    // the override actually took.
    let (status, _) = send(app, get("/rest/db/small/cells/1,2/k")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn creating_a_database_that_already_exists_is_409() {
    let (app, _dir) = test_app();
    let (status, body) = send(app, put(&format!("/rest/databases/{DB}"), json!({}))).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert!(body["error"].as_str().unwrap().contains(DB));
}

#[tokio::test]
async fn creating_a_database_with_an_invalid_name_is_400() {
    let (app, _dir) = test_app();
    let (status, _) = send(app, put("/rest/databases/..", json!({}))).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn removing_a_database_deletes_its_directory() {
    let (app, dir) = test_app();
    send(app.clone(), put("/rest/databases/extra", json!({}))).await;
    assert!(dir.0.join("extra").is_dir());

    let (status, _) = send(app.clone(), delete("/rest/databases/extra")).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert!(!dir.0.join("extra").exists());

    let (status, _) = send(app, get("/rest/db/extra/stats")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn removing_an_unknown_database_is_404() {
    let (app, _dir) = test_app();
    let (status, _) = send(app, delete("/rest/databases/nope")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn databases_endpoints_require_auth() {
    let req = Request::builder()
        .method("GET")
        .uri("/rest/databases")
        .body(Body::empty())
        .unwrap();
    let (status, _) = send(test_app().0, req).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn a_read_only_account_can_list_but_not_create_or_remove_databases() {
    let (app, _dir) = test_app();
    let list = with_auth(
        Request::builder()
            .method("GET")
            .uri("/rest/databases")
            .body(Body::empty())
            .unwrap(),
        TEST_READ_ONLY_USER,
        TEST_READ_ONLY_PASSWORD,
    );
    let (status, _) = send(app.clone(), list).await;
    assert_eq!(status, StatusCode::OK);

    let create = with_auth(
        Request::builder()
            .method("PUT")
            .uri("/rest/databases/extra")
            .header("content-type", "application/json")
            .body(Body::from(json!({}).to_string()))
            .unwrap(),
        TEST_READ_ONLY_USER,
        TEST_READ_ONLY_PASSWORD,
    );
    let (status, _) = send(app.clone(), create).await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    let remove = with_auth(
        Request::builder()
            .method("DELETE")
            .uri(format!("/rest/databases/{DB}"))
            .body(Body::empty())
            .unwrap(),
        TEST_READ_ONLY_USER,
        TEST_READ_ONLY_PASSWORD,
    );
    let (status, _) = send(app, remove).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

// --- Cross-database isolation and unknown-database 404s ---

#[tokio::test]
async fn two_databases_are_fully_isolated() {
    let (app, _dir) = test_app();
    send(app.clone(), put("/rest/databases/other", json!({}))).await;

    send(
        app.clone(),
        put(
            &format!("/rest/db/{DB}/cells/1,2,3/material"),
            json!({"type": "str", "value": "stone"}),
        ),
    )
    .await;

    // The same coordinate/key in the other database sees nothing.
    let (status, _) = send(app.clone(), get("/rest/db/other/cells/1,2,3/material")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // Columns are independent too.
    send(
        app.clone(),
        put("/rest/db/other/columns/hardness", json!({"type": "f64"})),
    )
    .await;
    let (_, db_columns) = send(app.clone(), get(&format!("/rest/db/{DB}/columns"))).await;
    assert_eq!(
        db_columns["columns"],
        json!([{"key": "material", "type": "str"}])
    );
    let (_, other_columns) = send(app, get("/rest/db/other/columns")).await;
    assert_eq!(
        other_columns["columns"],
        json!([{"key": "hardness", "type": "f64"}])
    );
}

#[tokio::test]
async fn every_per_database_route_404s_for_an_unknown_database() {
    let (app, _dir) = test_app();

    let (status, _) = send(app.clone(), get("/rest/db/nope/cells/0,0,0/k")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, _) = send(
        app.clone(),
        put(
            "/rest/db/nope/cells/0,0,0/k",
            json!({"type": "i64", "value": 1}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, _) = send(app.clone(), delete("/rest/db/nope/cells/0,0,0/k")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, _) = send(app.clone(), get("/rest/db/nope/regions/0,0,0/1,1,1/k")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, _) = send(app.clone(), get("/rest/db/nope/stats")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, _) = send(app.clone(), get("/rest/db/nope/columns")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, _) = send(app, post("/rest/db/nope/query", query("SELECT *"))).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}
