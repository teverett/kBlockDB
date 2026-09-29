//! The REST API surface: two resources, `/cells/...` (a single cell) and
//! `/regions/...` (an axis-aligned box of cells), each with GET (read),
//! PUT (write) and DELETE (remove) -- plus `/health`.
//!
//! Coordinates and region origin/extent are comma-separated path segments
//! (`/cells/1,2,3/material`, `/regions/0,0,0/8,8,8/material`), matching
//! however many axes the world was created with -- there's nothing
//! 3-axis-specific here, same as in `kdb` itself.

use crate::coords::parse_coords;
use crate::error::ApiError;
use crate::state::AppState;
use crate::value_json::ValueJson;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::json;

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route(
            "/cells/:coords/:key",
            get(get_cell).put(set_cell).delete(remove_cell),
        )
        .route(
            "/regions/:origin/:extent/:key",
            get(get_region).put(set_region).delete(remove_region),
        )
        .with_state(state)
}

async fn health(State(state): State<AppState>) -> Json<serde_json::Value> {
    // Brief, non-blocking lock: just two field reads, no I/O, so this is
    // fine directly in an async handler (unlike the World operations
    // below, which all go through AppState::with_world).
    let (axes, world_dim) = {
        let w = state.world.lock().unwrap_or_else(|p| p.into_inner());
        (w.axes(), w.world_dim())
    };
    Json(json!({ "status": "ok", "axes": axes, "world_dim": world_dim }))
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
