//! HTTP-level integration tests: real requests through the real `Router`
//! (via `tower::ServiceExt::oneshot`, no TCP socket needed), one per
//! endpoint/behavior. Complements the narrower unit tests in
//! `coords.rs`/`value_json.rs`.

use crate::routes::router;
use crate::state::{Account, AppState};
use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum_extra::headers::{Authorization, HeaderMapExt};
use http_body_util::BodyExt;
use kdb::World;
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

/// Removes its temp directory when dropped. Each test binds this to a local
/// so the world's data dir outlives the `Router` built on top of it, then
/// gets cleaned up at the end of the test.
struct TestDir(PathBuf);

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A fresh 3-axis, world_dim=100 world in its own temp directory, wrapped
/// in a router -- what every test below starts from. Keep the returned
/// `TestDir` alive (bind it, don't `let _ =`) for the test's duration.
fn test_app() -> (axum::Router, TestDir) {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir: PathBuf =
        std::env::temp_dir().join(format!("kdbserver-test-{n}-{}", std::process::id()));
    let world = World::create(&dir, 3, 100).unwrap();
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
    (
        router(AppState::new(world, Arc::new(credentials))),
        TestDir(dir),
    )
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
async fn health_reports_the_worlds_shape() {
    let (status, body) = send(test_app().0, get("/health")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "ok");
    assert_eq!(body["axes"], 3);
    assert_eq!(body["world_dim"], 100);
}

#[tokio::test]
async fn health_includes_a_current_unix_timestamp() {
    let before = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let (status, body) = send(test_app().0, get("/health")).await;
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
    let (status, body) = send(test_app().0, get("/stats")).await;
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
            "/cells/1,2,3/material",
            json!({"type": "str", "value": "stone"}),
        ),
    )
    .await;

    let (status, body) = send(app, get("/stats")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["total_chunks"], 1);
    assert!(body["total_bytes"].as_u64().unwrap() > 0);
}

#[tokio::test]
async fn stats_requires_auth() {
    let req = Request::builder()
        .method("GET")
        .uri("/stats")
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
            .uri("/stats")
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
    let (status, body) = send(test_app().0, get("/cells/1,2,3/material")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(body["error"].is_string());
}

#[tokio::test]
async fn set_then_get_a_cell_roundtrips() {
    let (app, _dir) = test_app();

    let (status, _) = send(
        app.clone(),
        put(
            "/cells/1,2,3/material",
            json!({"type": "str", "value": "stone"}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (status, body) = send(app, get("/cells/1,2,3/material")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!({"value": {"type": "str", "value": "stone"}}));
}

#[tokio::test]
async fn set_then_delete_then_get_a_cell_is_404_again() {
    let (app, _dir) = test_app();
    send(
        app.clone(),
        put("/cells/5,5,5/hardness", json!({"type": "i64", "value": 10})),
    )
    .await;

    let (status, _) = send(app.clone(), delete("/cells/5,5,5/hardness")).await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (status, _) = send(app, get("/cells/5,5,5/hardness")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn a_cell_coordinate_with_the_wrong_axis_count_is_400() {
    let (status, body) = send(test_app().0, get("/cells/1,2/material")).await; // 2 coords, 3-axis world
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body["error"].is_string());
}

#[tokio::test]
async fn a_non_numeric_coordinate_is_400() {
    let (status, _) = send(test_app().0, get("/cells/1,x,3/material")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn an_out_of_bounds_coordinate_is_400() {
    // world_dim is 100 in test_app().
    let (status, _) = send(test_app().0, get("/cells/999,0,0/material")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn malformed_json_body_is_a_client_error() {
    let req = with_auth(
        Request::builder()
            .method("PUT")
            .uri("/cells/0,0,0/material")
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
    let (status, _) = send(app.clone(), put("/regions/0,0,0/2,2,1/n", values)).await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (status, body) = send(app, get("/regions/0,0,0/2,2,1/n")).await;
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
    let (status, body) = send(test_app().0, get("/regions/0,0,0/2,2,2/material")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["values"].as_array().unwrap().len(), 8);
    assert!(body["values"].as_array().unwrap().iter().all(Json::is_null));
}

#[tokio::test]
async fn set_region_with_the_wrong_number_of_values_is_400() {
    let values = json!({"values": [{"type": "i64", "value": 0}]}); // region holds 8 cells
    let (status, body) = send(test_app().0, put("/regions/0,0,0/2,2,2/material", values)).await;
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
    send(app.clone(), put("/regions/0,0,0/2,1,1/material", values)).await;

    let (status, _) = send(app.clone(), delete("/regions/0,0,0/2,1,1/material")).await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (_, body) = send(app, get("/regions/0,0,0/2,1,1/material")).await;
    assert!(body["values"].as_array().unwrap().iter().all(Json::is_null));
}

#[tokio::test]
async fn a_region_with_mismatched_origin_and_extent_axes_is_400() {
    let (status, _) = send(test_app().0, get("/regions/0,0,0/2,2/material")).await; // 3 vs 2 axes
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn health_does_not_require_auth() {
    let req = Request::builder()
        .method("GET")
        .uri("/health")
        .body(Body::empty())
        .unwrap();
    let (status, _) = send(test_app().0, req).await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn a_protected_endpoint_without_credentials_is_401() {
    let req = Request::builder()
        .method("GET")
        .uri("/cells/1,2,3/material")
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
            .uri("/cells/1,2,3/material")
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
            .uri("/cells/1,2,3/material")
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
            .uri("/cells/1,2,3/material")
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
            "/cells/1,2,3/material",
            json!({"type": "str", "value": "stone"}),
        ),
    )
    .await;

    let req = with_auth(
        Request::builder()
            .method("GET")
            .uri("/cells/1,2,3/material")
            .body(Body::empty())
            .unwrap(),
        TEST_READ_ONLY_USER,
        TEST_READ_ONLY_PASSWORD,
    );
    let (status, body) = send(app, req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!({"value": {"type": "str", "value": "stone"}}));
}

#[tokio::test]
async fn a_read_only_user_cannot_put_a_cell() {
    let req = with_auth(
        Request::builder()
            .method("PUT")
            .uri("/cells/1,2,3/material")
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
            "/cells/1,2,3/material",
            json!({"type": "str", "value": "stone"}),
        ),
    )
    .await;

    let req = with_auth(
        Request::builder()
            .method("DELETE")
            .uri("/cells/1,2,3/material")
            .body(Body::empty())
            .unwrap(),
        TEST_READ_ONLY_USER,
        TEST_READ_ONLY_PASSWORD,
    );
    let (status, _) = send(app.clone(), req).await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    // The value survives -- the DELETE was actually refused, not silently
    // accepted and ignored.
    let (status, body) = send(app, get("/cells/1,2,3/material")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!({"value": {"type": "str", "value": "stone"}}));
}

#[tokio::test]
async fn a_read_only_user_cannot_put_a_region() {
    let values = vec![json!({"type": "i64", "value": 0}); 8];
    let req = with_auth(
        Request::builder()
            .method("PUT")
            .uri("/regions/0,0,0/2,2,2/material")
            .header("content-type", "application/json")
            .body(Body::from(json!({"values": values}).to_string()))
            .unwrap(),
        TEST_READ_ONLY_USER,
        TEST_READ_ONLY_PASSWORD,
    );
    let (status, _) = send(test_app().0, req).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}
