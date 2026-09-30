//! The REST API surface: two resources, `/cells/...` (a single cell) and
//! `/regions/...` (an axis-aligned box of cells), each with GET (read),
//! PUT (write) and DELETE (remove) -- plus read-only `/stats` and
//! `/health`.
//!
//! Coordinates and region origin/extent are comma-separated path segments
//! (`/cells/1,2,3/material`, `/regions/0,0,0/8,8,8/material`), matching
//! however many axes the world was created with -- there's nothing
//! 3-axis-specific here, same as in `kblockdblib` itself.
//!
//! Every route below requires HTTP Basic Auth (see `auth.rs`) except
//! `/health`, left open so load balancers/orchestrators can poll liveness
//! without credentials -- it exposes nothing more sensitive than the
//! world's shape and this server's clock. `/stats` is a `GET`, so a
//! `read_only` account can use it same as any other account.
//!
//! Every handler below carries a `#[utoipa::path(...)]` annotation, which
//! is how `openapi.rs`'s spec (served at `/api-docs/openapi.json` and
//! browsable at `/swagger-ui`) stays in sync with the router: it's
//! generated from these annotations, not hand-maintained separately, so
//! adding/changing a route without updating its annotation is a compile
//! error (`OpenApi` derive in `openapi.rs` lists every path below by
//! name), not a spec that silently drifts from reality.

use crate::auth::require_auth;
use crate::coords::parse_coords;
use crate::error::{ApiError, ErrorBody};
use crate::openapi::ApiDoc;
use crate::state::AppState;
use crate::value_json::ValueJson;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::middleware;
use axum::routing::get;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use utoipa::{OpenApi, ToSchema};
use utoipa_swagger_ui::SwaggerUi;

pub fn router(state: AppState) -> Router {
    let protected = Router::new()
        .route(
            "/cells/{coords}/{key}",
            get(get_cell).put(set_cell).delete(remove_cell),
        )
        .route(
            "/regions/{origin}/{extent}/{key}",
            get(get_region).put(set_region).delete(remove_region),
        )
        .route("/stats", get(stats))
        .route_layer(middleware::from_fn_with_state(state.clone(), require_auth));

    // Unauthenticated, like /health -- the spec/UI describe the API, they
    // don't expose any of its data.
    let docs = SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", ApiDoc::openapi());

    Router::new()
        .route("/health", get(health))
        .merge(protected)
        .merge(docs)
        .with_state(state)
}

#[derive(Serialize, ToSchema)]
pub struct HealthResponse {
    status: String,
    axes: usize,
    world_dim: u32,
    /// Seconds since the Unix epoch, per this server's own clock -- lets a
    /// caller sanity-check clock skew or confirm the response isn't a
    /// stale cached one.
    timestamp: u64,
}

#[utoipa::path(
    get,
    path = "/health",
    tag = "health",
    responses(
        (status = 200, description = "The server is up", body = HealthResponse),
    ),
)]
async fn health(State(state): State<AppState>) -> Json<HealthResponse> {
    // No lock needed at all: World::axes/world_dim are plain field reads
    // via &self, and state.world is a bare Arc<World> now (see state.rs).
    Json(HealthResponse {
        status: "ok".to_string(),
        axes: state.world.axes(),
        world_dim: state.world.world_dim(),
        timestamp: unix_timestamp(),
    })
}

fn unix_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// On-disk statistics for the world's data -- see `kblockdblib::Stats`'s doc
/// comment for what each field means and how `total_blocks` differs from
/// `total_bytes / 512` for a mostly-empty (sparse) world.
#[derive(Serialize, ToSchema)]
pub struct StatsResponse {
    total_chunks: u64,
    total_bytes: u64,
    total_blocks: u64,
}

#[utoipa::path(
    get,
    path = "/stats",
    tag = "stats",
    responses(
        (status = 200, description = "On-disk statistics for the world's data", body = StatsResponse),
        (status = 401, description = "Missing or invalid credentials", body = ErrorBody),
    ),
    security(("basic_auth" = [])),
)]
async fn stats(State(state): State<AppState>) -> Result<Json<StatsResponse>, ApiError> {
    let stats = state.with_world(kblockdblib::World::stats).await?;
    Ok(Json(StatsResponse {
        total_chunks: stats.total_chunks,
        total_bytes: stats.total_bytes,
        total_blocks: stats.total_blocks,
    }))
}

#[derive(Serialize, ToSchema)]
pub struct CellResponse {
    value: ValueJson,
}

#[utoipa::path(
    get,
    path = "/cells/{coords}/{key}",
    tag = "cells",
    params(
        ("coords" = String, Path, description = "Comma-separated coordinate, one u32 per axis (e.g. `1,2,3`)"),
        ("key" = String, Path, description = "The column/key to read"),
    ),
    responses(
        (status = 200, description = "The cell's value for this key", body = CellResponse),
        (status = 400, description = "Malformed or out-of-range coordinate", body = ErrorBody),
        (status = 401, description = "Missing or invalid credentials", body = ErrorBody),
        (status = 404, description = "No value set for this key at this cell", body = ErrorBody),
    ),
    security(("basic_auth" = [])),
)]
async fn get_cell(
    State(state): State<AppState>,
    Path((coords, key)): Path<(String, String)>,
) -> Result<Json<CellResponse>, ApiError> {
    let coord = parse_coords(&coords)?;
    let lookup_key = key.clone();
    let found = state
        .with_world(move |w| w.get(&coord, &lookup_key))
        .await?;
    match found {
        Some(v) => Ok(Json(CellResponse {
            value: ValueJson::from(v),
        })),
        None => Err(ApiError::NotFound(format!(
            "no value set for key '{key}' at ({coords})"
        ))),
    }
}

#[utoipa::path(
    put,
    path = "/cells/{coords}/{key}",
    tag = "cells",
    params(
        ("coords" = String, Path, description = "Comma-separated coordinate, one u32 per axis (e.g. `1,2,3`)"),
        ("key" = String, Path, description = "The column/key to write"),
    ),
    request_body = ValueJson,
    responses(
        (status = 204, description = "The value was set"),
        (status = 400, description = "Malformed or out-of-range coordinate, or malformed body", body = ErrorBody),
        (status = 401, description = "Missing or invalid credentials", body = ErrorBody),
        (status = 403, description = "This account is read-only", body = ErrorBody),
    ),
    security(("basic_auth" = [])),
)]
async fn set_cell(
    State(state): State<AppState>,
    Path((coords, key)): Path<(String, String)>,
    Json(body): Json<ValueJson>,
) -> Result<StatusCode, ApiError> {
    let coord = parse_coords(&coords)?;
    let value: kblockdblib::Value = body.into();
    state
        .with_world(move |w| w.set(&coord, &key, value))
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    delete,
    path = "/cells/{coords}/{key}",
    tag = "cells",
    params(
        ("coords" = String, Path, description = "Comma-separated coordinate, one u32 per axis (e.g. `1,2,3`)"),
        ("key" = String, Path, description = "The column/key to clear"),
    ),
    responses(
        (status = 204, description = "The cell's value for this key was cleared (or was already unset)"),
        (status = 400, description = "Malformed or out-of-range coordinate", body = ErrorBody),
        (status = 401, description = "Missing or invalid credentials", body = ErrorBody),
        (status = 403, description = "This account is read-only", body = ErrorBody),
    ),
    security(("basic_auth" = [])),
)]
async fn remove_cell(
    State(state): State<AppState>,
    Path((coords, key)): Path<(String, String)>,
) -> Result<StatusCode, ApiError> {
    let coord = parse_coords(&coords)?;
    state.with_world(move |w| w.remove(&coord, &key)).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// One value per cell in the region, in axis-0-fastest order -- index `i`
/// is offset `(i % extent[0], (i / extent[0]) % extent[1], ...)` from the
/// region's origin. `null` for a cell with no value set for this key.
#[derive(Serialize, ToSchema)]
pub struct RegionValuesResponse {
    values: Vec<Option<ValueJson>>,
}

#[utoipa::path(
    get,
    path = "/regions/{origin}/{extent}/{key}",
    tag = "regions",
    params(
        ("origin" = String, Path, description = "Comma-separated region origin, one u32 per axis"),
        ("extent" = String, Path, description = "Comma-separated region extent, one u32 per axis"),
        ("key" = String, Path, description = "The column/key to read"),
    ),
    responses(
        (status = 200, description = "One value (or null) per cell in the region", body = RegionValuesResponse),
        (status = 400, description = "Malformed/out-of-range coordinate, or mismatched axis counts", body = ErrorBody),
        (status = 401, description = "Missing or invalid credentials", body = ErrorBody),
    ),
    security(("basic_auth" = [])),
)]
async fn get_region(
    State(state): State<AppState>,
    Path((origin, extent, key)): Path<(String, String, String)>,
) -> Result<Json<RegionValuesResponse>, ApiError> {
    let origin = parse_coords(&origin)?;
    let extent = parse_coords(&extent)?;
    let region = kblockdblib::Region::new(origin, extent);
    let values = state
        .with_world(move |w| w.get_region(&region, &key))
        .await?;
    let values: Vec<Option<ValueJson>> =
        values.into_iter().map(|v| v.map(ValueJson::from)).collect();
    Ok(Json(RegionValuesResponse { values }))
}

/// `PUT /regions/.../.../<key>` body: one value per cell, in the same
/// axis-0-fastest order `GET` returns them in. Its length must equal the
/// region's volume exactly -- `World::set_region` itself enforces that and
/// this surfaces as `400 Bad Request` via `ApiError::from(io::Error)`.
#[derive(Deserialize, ToSchema)]
pub struct SetRegionBody {
    values: Vec<ValueJson>,
}

#[utoipa::path(
    put,
    path = "/regions/{origin}/{extent}/{key}",
    tag = "regions",
    params(
        ("origin" = String, Path, description = "Comma-separated region origin, one u32 per axis"),
        ("extent" = String, Path, description = "Comma-separated region extent, one u32 per axis"),
        ("key" = String, Path, description = "The column/key to write"),
    ),
    request_body = SetRegionBody,
    responses(
        (status = 204, description = "Every cell in the region was set"),
        (status = 400, description = "Malformed/out-of-range coordinate, mismatched axis counts, or wrong number of values", body = ErrorBody),
        (status = 401, description = "Missing or invalid credentials", body = ErrorBody),
        (status = 403, description = "This account is read-only", body = ErrorBody),
    ),
    security(("basic_auth" = [])),
)]
async fn set_region(
    State(state): State<AppState>,
    Path((origin, extent, key)): Path<(String, String, String)>,
    Json(body): Json<SetRegionBody>,
) -> Result<StatusCode, ApiError> {
    let origin = parse_coords(&origin)?;
    let extent = parse_coords(&extent)?;
    let region = kblockdblib::Region::new(origin, extent);
    let values: Vec<kblockdblib::Value> = body.values.into_iter().map(Into::into).collect();
    state
        .with_world(move |w| w.set_region(&region, &key, &values))
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    delete,
    path = "/regions/{origin}/{extent}/{key}",
    tag = "regions",
    params(
        ("origin" = String, Path, description = "Comma-separated region origin, one u32 per axis"),
        ("extent" = String, Path, description = "Comma-separated region extent, one u32 per axis"),
        ("key" = String, Path, description = "The column/key to clear"),
    ),
    responses(
        (status = 204, description = "Every cell in the region had this key cleared"),
        (status = 400, description = "Malformed/out-of-range coordinate, or mismatched axis counts", body = ErrorBody),
        (status = 401, description = "Missing or invalid credentials", body = ErrorBody),
        (status = 403, description = "This account is read-only", body = ErrorBody),
    ),
    security(("basic_auth" = [])),
)]
async fn remove_region(
    State(state): State<AppState>,
    Path((origin, extent, key)): Path<(String, String, String)>,
) -> Result<StatusCode, ApiError> {
    let origin = parse_coords(&origin)?;
    let extent = parse_coords(&extent)?;
    let region = kblockdblib::Region::new(origin, extent);
    state
        .with_world(move |w| w.remove_region(&region, &key))
        .await?;
    Ok(StatusCode::NO_CONTENT)
}
