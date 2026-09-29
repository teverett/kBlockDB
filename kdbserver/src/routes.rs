//! The REST API surface: two resources, `/cells/...` (a single cell) and
//! `/regions/...` (an axis-aligned box of cells), each with GET (read),
//! PUT (write) and DELETE (remove) -- plus read-only `/stats` and
//! `/health`.
//!
//! Coordinates and region origin/extent are comma-separated path segments
//! (`/cells/1,2,3/material`, `/regions/0,0,0/8,8,8/material`), matching
//! however many axes the world was created with -- there's nothing
//! 3-axis-specific here, same as in `kdb` itself.
//!
//! Every route below requires HTTP Basic Auth (see `auth.rs`) except
//! `/health`, left open so load balancers/orchestrators can poll liveness
//! without credentials -- it exposes nothing more sensitive than the
//! world's shape and this server's clock. `/stats` is a `GET`, so a
//! `read_only` account can use it same as any other account.

use crate::auth::require_auth;
use crate::coords::parse_coords;
use crate::error::ApiError;
use crate::state::AppState;
use crate::value_json::ValueJson;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::middleware;
use axum::routing::get;
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::json;

pub fn router(state: AppState) -> Router {
    let protected = Router::new()
        .route(
            "/cells/:coords/:key",
            get(get_cell).put(set_cell).delete(remove_cell),
        )
        .route(
            "/regions/:origin/:extent/:key",
            get(get_region).put(set_region).delete(remove_region),
        )
        .route("/stats", get(stats))
        .route_layer(middleware::from_fn_with_state(state.clone(), require_auth));

    Router::new()
        .route("/health", get(health))
        .merge(protected)
        .with_state(state)
}

async fn health(State(state): State<AppState>) -> Json<serde_json::Value> {
    // No lock needed at all: World::axes/world_dim are plain field reads
    // via &self, and state.world is a bare Arc<World> now (see state.rs).
    Json(json!({
        "status": "ok",
        "axes": state.world.axes(),
        "world_dim": state.world.world_dim(),
        "timestamp": unix_timestamp(),
    }))
}

/// Seconds since the Unix epoch, per this server's own clock -- lets a
/// caller sanity-check clock skew or confirm the response isn't a stale
/// cached one, without pulling in a full date/time formatting dependency
/// for one field.
fn unix_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// On-disk statistics for the world's data (see `kdb::World::stats`):
/// total chunk files, their combined size, and their combined actual
/// disk-block usage (smaller than size/512 would suggest for chunks with
/// sparse, never-written regions -- see `kdb::Stats`'s doc comment).
async fn stats(State(state): State<AppState>) -> Result<Json<serde_json::Value>, ApiError> {
    let stats = state.with_world(kdb::World::stats).await?;
    Ok(Json(json!({
        "total_chunks": stats.total_chunks,
        "total_bytes": stats.total_bytes,
        "total_blocks": stats.total_blocks,
    })))
}

async fn get_cell(
    State(state): State<AppState>,
    Path((coords, key)): Path<(String, String)>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let coord = parse_coords(&coords)?;
    let lookup_key = key.clone();
    let found = state
        .with_world(move |w| w.get(&coord, &lookup_key))
        .await?;
    match found {
        Some(v) => Ok(Json(json!({ "value": ValueJson::from(v) }))),
        None => Err(ApiError::NotFound(format!(
            "no value set for key '{key}' at ({coords})"
        ))),
    }
}

async fn set_cell(
    State(state): State<AppState>,
    Path((coords, key)): Path<(String, String)>,
    Json(body): Json<ValueJson>,
) -> Result<StatusCode, ApiError> {
    let coord = parse_coords(&coords)?;
    let value: kdb::Value = body.into();
    state
        .with_world(move |w| w.set(&coord, &key, value))
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn remove_cell(
    State(state): State<AppState>,
    Path((coords, key)): Path<(String, String)>,
) -> Result<StatusCode, ApiError> {
    let coord = parse_coords(&coords)?;
    state.with_world(move |w| w.remove(&coord, &key)).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn get_region(
    State(state): State<AppState>,
    Path((origin, extent, key)): Path<(String, String, String)>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let origin = parse_coords(&origin)?;
    let extent = parse_coords(&extent)?;
    let region = kdb::Region::new(origin, extent);
    let values = state
        .with_world(move |w| w.get_region(&region, &key))
        .await?;
    let values: Vec<Option<ValueJson>> =
        values.into_iter().map(|v| v.map(ValueJson::from)).collect();
    Ok(Json(json!({ "values": values })))
}

/// `PUT /regions/.../.../<key>` body: one value per cell, in the same
/// axis-0-fastest order `GET` returns them in. Its length must equal the
/// region's volume exactly -- `World::set_region` itself enforces that and
/// this surfaces as `400 Bad Request` via `ApiError::from(io::Error)`.
#[derive(Deserialize)]
struct SetRegionBody {
    values: Vec<ValueJson>,
}

async fn set_region(
    State(state): State<AppState>,
    Path((origin, extent, key)): Path<(String, String, String)>,
    Json(body): Json<SetRegionBody>,
) -> Result<StatusCode, ApiError> {
    let origin = parse_coords(&origin)?;
    let extent = parse_coords(&extent)?;
    let region = kdb::Region::new(origin, extent);
    let values: Vec<kdb::Value> = body.values.into_iter().map(Into::into).collect();
    state
        .with_world(move |w| w.set_region(&region, &key, &values))
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn remove_region(
    State(state): State<AppState>,
    Path((origin, extent, key)): Path<(String, String, String)>,
) -> Result<StatusCode, ApiError> {
    let origin = parse_coords(&origin)?;
    let extent = parse_coords(&extent)?;
    let region = kdb::Region::new(origin, extent);
    state
        .with_world(move |w| w.remove_region(&region, &key))
        .await?;
    Ok(StatusCode::NO_CONTENT)
}
