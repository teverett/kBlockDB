//! The REST API surface, mounted under the `/rest` context path:
//! `/rest/databases` (and `/rest/databases/{name}`) manage which databases
//! exist, and every per-database resource -- `/rest/db/{db}/cells/...`
//! (a single cell), `/rest/db/{db}/regions/...` (an axis-aligned box of
//! cells), `/rest/db/{db}/stats`, `/rest/db/{db}/columns`, and
//! `/rest/db/{db}/query` -- is scoped under a `{db}` path segment naming
//! which one to operate on. Every one of those routes 404s if `{db}` names
//! a database that doesn't exist -- there's no implicit creation; create it
//! first via `PUT /rest/databases/{name}`.
//!
//! Coordinates and region origin/extent are comma-separated path segments
//! (`/rest/db/{db}/cells/1,2,3/material`,
//! `/rest/db/{db}/regions/0,0,0/8,8,8/material`), matching however many axes
//! that database was created with -- there's nothing 3-axis-specific here,
//! same as in `kblockdblib` itself.
//!
//! Every route below requires HTTP Basic Auth (see `auth.rs`) except
//! `/rest/health`, left open so load balancers/orchestrators can poll
//! liveness without credentials -- it exposes nothing more sensitive than
//! this server's clock and how many databases it's managing. `GET
//! /rest/databases` and `GET /rest/db/{db}/stats` are `GET`s, so a
//! `read_only` account can use them same as any other account.
//!
//! Every handler below carries a `#[utoipa::path(...)]` annotation, which
//! is how `openapi.rs`'s spec (served at `/rest/api-docs/openapi.json` and
//! browsable at `/rest/swagger-ui`) stays in sync with the router: it's
//! generated from these annotations, not hand-maintained separately, so
//! adding/changing a route without updating its annotation is a compile
//! error (`OpenApi` derive in `openapi.rs` lists every path below by
//! name), not a spec that silently drifts from reality. Each annotation's
//! `path = "..."` must include the `/rest` prefix too, since utoipa has no
//! way to know about the `.nest("/rest", ...)` in `router()` below --
//! it only ever sees the literal string given.
//!
//! `router()` also mounts `browser::router` at the top level (`/` and
//! `/db/{name}/...`, see that module) -- a read-only data browser over
//! these same databases, deliberately kept outside `/rest` since it isn't
//! part of the versioned REST API surface.
//!
//! `POST /rest/db/{db}/query` (see `run_query` below) is the one route here
//! that doesn't use `require_auth`: that middleware's `read_only` check is
//! purely a function of HTTP method (`GET` = read, anything else = write),
//! but one query can be *either* depending on the query text itself
//! (`SELECT` vs `SET`/`UPDATE`/`DELETE` -- see `kblockdbquery`'s
//! `Statement::is_write`). `run_query` authenticates the same way
//! `require_auth` does (`auth::account_from_headers`) and only then checks
//! `read_only` against the parsed statement, not the HTTP method.

use crate::auth::require_auth;
use crate::browser;
use crate::coords::parse_coords;
use crate::error::{ApiError, ErrorBody};
use crate::openapi::ApiDoc;
use crate::state::{AppState, WorldShape};
use crate::value_json::{ValueJson, ValueTypeJson};
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::middleware;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use kblockdbquery as query;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use utoipa::{OpenApi, ToSchema};
use utoipa_swagger_ui::SwaggerUi;

pub fn router(state: AppState) -> Router {
    let protected = Router::new()
        .route("/databases", get(list_databases))
        .route("/cluster", get(cluster))
        .route(
            "/databases/{name}",
            put(create_database).delete(remove_database),
        )
        .route(
            "/db/{db}/cells/{coords}/{key}",
            get(get_cell).put(set_cell).delete(remove_cell),
        )
        .route(
            "/db/{db}/regions/{origin}/{extent}/{key}",
            get(get_region).put(set_region).delete(remove_region),
        )
        .route("/db/{db}/stats", get(stats))
        .route("/db/{db}/columns", get(list_columns))
        .route(
            "/db/{db}/columns/{key}",
            put(add_column).delete(remove_column),
        )
        .route_layer(middleware::from_fn_with_state(state.clone(), require_auth));

    // Not under `protected` -- see this module's doc comment on why
    // `/db/{db}/query` needs its own auth check instead of `require_auth`'s.
    let query_route = Router::new().route("/db/{db}/query", post(run_query));

    let rest = Router::new()
        .route("/health", get(health))
        .merge(protected)
        .merge(query_route);

    // Built with the *final*, post-nesting absolute paths (`/rest/...`),
    // not nested itself: utoipa_swagger_ui bakes whatever string `.url(...)`
    // is given into the served page's own JS as an absolute fetch target,
    // not something resolved relative to wherever this router ends up
    // mounted -- nesting it under `/rest` a second time here would double
    // the prefix. Unauthenticated, like /rest/health -- the spec/UI
    // describe the API, they don't expose any of its data.
    let docs =
        SwaggerUi::new("/rest/swagger-ui").url("/rest/api-docs/openapi.json", ApiDoc::openapi());

    Router::new()
        .nest("/rest", rest)
        .merge(docs)
        .merge(browser::router(state.clone()))
        .with_state(state)
}

#[derive(Serialize, ToSchema)]
pub struct HealthResponse {
    status: String,
    /// Which instance answered -- the OS hostname, or whatever the
    /// `hostname` config key overrides it to. Useful behind a load
    /// balancer, where the point of polling `/rest/health` is often to
    /// find out *which* backend is unhealthy.
    hostname: String,
    /// How many databases this server currently manages -- a live count
    /// (see `GET /rest/databases`), not a cached one.
    database_count: usize,
    /// Seconds since the Unix epoch, per this server's own clock -- lets a
    /// caller sanity-check clock skew or confirm the response isn't a
    /// stale cached one.
    timestamp: u64,
    /// The address of every known peer (see `GET /rest/cluster`) this
    /// instance's link to is *currently up* -- a peer that's down or still
    /// being retried isn't listed. Empty unless clustering is configured.
    peers: Vec<String>,
}

#[utoipa::path(
    get,
    path = "/rest/health",
    tag = "health",
    responses(
        (status = 200, description = "The server is up", body = HealthResponse),
    ),
)]
async fn health(State(state): State<AppState>) -> Result<Json<HealthResponse>, ApiError> {
    let database_count = state.list_databases().await?.len();
    let peers = peer_snapshot(&state)
        .into_iter()
        .filter(|(_, connected)| *connected)
        .map(|(address, _)| address)
        .collect();
    Ok(Json(HealthResponse {
        status: "ok".to_string(),
        hostname: state.hostname.to_string(),
        database_count,
        timestamp: unix_timestamp(),
        peers,
    }))
}

/// Every known peer and whether this instance's link to it is up, sorted
/// by address -- empty when clustering isn't configured.
fn peer_snapshot(state: &AppState) -> Vec<(String, bool)> {
    state
        .peers
        .as_ref()
        .map(kblockdbcluster::peers::PeerSet::snapshot)
        .unwrap_or_default()
}

/// One host this instance knows about, for `GET /rest/cluster` -- unlike
/// `/rest/health`'s `peers`, this includes a peer even while it's
/// unreachable, with `connected` saying which.
#[derive(Serialize, ToSchema)]
pub struct ClusterPeer {
    /// The peer's address -- as configured in `[[peers]]`, or
    /// `<source IP>:<its peer port>` if it was learned by connecting in.
    host: String,
    /// Whether this instance's link to it is up right now.
    connected: bool,
}

#[derive(Serialize, ToSchema)]
pub struct ClusterResponse {
    /// This instance's own name, same as `HealthResponse::hostname`.
    hostname: String,
    peers: Vec<ClusterPeer>,
}

#[utoipa::path(
    get,
    path = "/rest/cluster",
    tag = "cluster",
    responses(
        (status = 200, description = "Every peer this instance knows about, with live connection status", body = ClusterResponse),
        (status = 401, description = "Missing or invalid credentials", body = ErrorBody),
    ),
    security(("basic_auth" = [])),
)]
async fn cluster(State(state): State<AppState>) -> Json<ClusterResponse> {
    let peers = peer_snapshot(&state)
        .into_iter()
        .map(|(host, connected)| ClusterPeer { host, connected })
        .collect();
    Json(ClusterResponse {
        hostname: state.hostname.to_string(),
        peers,
    })
}

pub(crate) fn unix_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Every database this server currently manages, sorted by name.
#[derive(Serialize, ToSchema)]
pub struct DatabasesResponse {
    databases: Vec<String>,
}

#[utoipa::path(
    get,
    path = "/rest/databases",
    tag = "databases",
    responses(
        (status = 200, description = "Every database this server manages", body = DatabasesResponse),
        (status = 401, description = "Missing or invalid credentials", body = ErrorBody),
    ),
    security(("basic_auth" = [])),
)]
async fn list_databases(
    State(state): State<AppState>,
) -> Result<Json<DatabasesResponse>, ApiError> {
    let databases = state.list_databases().await?;
    Ok(Json(DatabasesResponse { databases }))
}

/// The body of `PUT /rest/databases/{name}`: every field optional, each
/// defaulting to the server's own configured shape (`[worldparameters]`/
/// the matching CLI flags) when omitted -- `{}` is a valid body, meaning
/// "use the defaults".
#[derive(Deserialize, ToSchema, Default)]
pub struct CreateDatabaseBody {
    axes: Option<usize>,
    world_dim: Option<u32>,
    chunk_size: Option<u32>,
}

#[utoipa::path(
    put,
    path = "/rest/databases/{name}",
    tag = "databases",
    params(
        ("name" = String, Path, description = "The database to create"),
    ),
    request_body = CreateDatabaseBody,
    responses(
        (status = 204, description = "The database was created"),
        (status = 400, description = "An invalid database name", body = ErrorBody),
        (status = 401, description = "Missing or invalid credentials", body = ErrorBody),
        (status = 403, description = "This account is read-only", body = ErrorBody),
        (status = 409, description = "A database with this name already exists", body = ErrorBody),
    ),
    security(("basic_auth" = [])),
)]
async fn create_database(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(body): Json<CreateDatabaseBody>,
) -> Result<StatusCode, ApiError> {
    let default_shape = state.databases.default_shape();
    let shape = WorldShape {
        axes: body.axes.unwrap_or(default_shape.axes),
        world_dim: body.world_dim.unwrap_or(default_shape.world_dim),
        chunk_dim: body.chunk_size.unwrap_or(default_shape.chunk_dim),
    };
    state.create_database(&name, Some(shape)).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    delete,
    path = "/rest/databases/{name}",
    tag = "databases",
    params(
        ("name" = String, Path, description = "The database to delete, with all of its data"),
    ),
    responses(
        (status = 204, description = "The database, and every byte of data in it, was removed"),
        (status = 401, description = "Missing or invalid credentials", body = ErrorBody),
        (status = 403, description = "This account is read-only", body = ErrorBody),
        (status = 404, description = "No such database", body = ErrorBody),
    ),
    security(("basic_auth" = [])),
)]
async fn remove_database(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<StatusCode, ApiError> {
    let removed = state.remove_database(&name).await?;
    if removed {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(ApiError::NotFound(format!("no such database '{name}'")))
    }
}

/// One column in the world's schema: a key, and the value type fixed for
/// it when the column was created.
#[derive(Serialize, ToSchema)]
pub struct ColumnResponse {
    key: String,
    #[serde(rename = "type")]
    value_type: ValueTypeJson,
}

/// Every column in the world's schema, sorted by key.
#[derive(Serialize, ToSchema)]
pub struct ColumnsResponse {
    columns: Vec<ColumnResponse>,
}

/// The body of `PUT /rest/db/{db}/columns/{key}`: the type to fix the new
/// column to, e.g. `{"type": "str"}`.
#[derive(Deserialize, ToSchema)]
pub struct AddColumnBody {
    #[serde(rename = "type")]
    value_type: ValueTypeJson,
}

#[utoipa::path(
    get,
    path = "/rest/db/{db}/columns",
    tag = "columns",
    params(
        ("db" = String, Path, description = "The database to read"),
    ),
    responses(
        (status = 200, description = "Every column in the database's schema", body = ColumnsResponse),
        (status = 401, description = "Missing or invalid credentials", body = ErrorBody),
        (status = 404, description = "No such database", body = ErrorBody),
    ),
    security(("basic_auth" = [])),
)]
async fn list_columns(
    State(state): State<AppState>,
    Path(db): Path<String>,
) -> Result<Json<ColumnsResponse>, ApiError> {
    let columns = state.with_database(&db, |w| Ok(w.columns())).await?;
    Ok(Json(ColumnsResponse {
        columns: columns
            .into_iter()
            .map(|c| ColumnResponse {
                key: c.key,
                value_type: c.value_type.into(),
            })
            .collect(),
    }))
}

#[utoipa::path(
    put,
    path = "/rest/db/{db}/columns/{key}",
    tag = "columns",
    params(
        ("db" = String, Path, description = "The database to write"),
        ("key" = String, Path, description = "The column/key to create"),
    ),
    request_body = AddColumnBody,
    responses(
        (status = 204, description = "The column was created"),
        (status = 400, description = "Malformed body, or a key the schema can't store", body = ErrorBody),
        (status = 401, description = "Missing or invalid credentials", body = ErrorBody),
        (status = 403, description = "This account is read-only", body = ErrorBody),
        (status = 404, description = "No such database", body = ErrorBody),
        (status = 409, description = "A column with this key already exists", body = ErrorBody),
    ),
    security(("basic_auth" = [])),
)]
async fn add_column(
    State(state): State<AppState>,
    Path((db, key)): Path<(String, String)>,
    Json(body): Json<AddColumnBody>,
) -> Result<StatusCode, ApiError> {
    let value_type: kblockdblib::ValueType = body.value_type.into();
    state
        .with_database(&db, move |w| w.add_column(&key, value_type))
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    delete,
    path = "/rest/db/{db}/columns/{key}",
    tag = "columns",
    params(
        ("db" = String, Path, description = "The database to write"),
        ("key" = String, Path, description = "The column/key to drop"),
    ),
    responses(
        (status = 204, description = "The column, and every value ever written for it, was removed"),
        (status = 401, description = "Missing or invalid credentials", body = ErrorBody),
        (status = 403, description = "This account is read-only", body = ErrorBody),
        (status = 404, description = "No such database, or no such column", body = ErrorBody),
    ),
    security(("basic_auth" = [])),
)]
async fn remove_column(
    State(state): State<AppState>,
    Path((db, key)): Path<(String, String)>,
) -> Result<StatusCode, ApiError> {
    let lookup_key = key.clone();
    let removed = state
        .with_database(&db, move |w| w.remove_column(&lookup_key))
        .await?;
    if removed {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(ApiError::NotFound(format!("no column named '{key}'")))
    }
}

#[derive(Serialize, ToSchema)]
pub struct CellResponse {
    value: ValueJson,
    /// Milliseconds since the Unix epoch when this key was first set at
    /// this cell (see `kblockdblib::CellMeta`).
    created_at_ms: u64,
    /// Milliseconds since the Unix epoch when this key was last set at
    /// this cell -- equal to `created_at_ms` if it's never been
    /// overwritten.
    modified_at_ms: u64,
    /// How many times this key has been overwritten at this cell since it
    /// was first set: 0 for a value that's never been overwritten, 1 after
    /// one overwrite, and so on. Resets to 0 if the value is removed and
    /// later set again.
    version: u64,
}

#[utoipa::path(
    get,
    path = "/rest/db/{db}/cells/{coords}/{key}",
    tag = "cells",
    params(
        ("db" = String, Path, description = "The database to read"),
        ("coords" = String, Path, description = "Comma-separated coordinate, one i32 per axis (e.g. `1,2,3`)"),
        ("key" = String, Path, description = "The column/key to read"),
    ),
    responses(
        (status = 200, description = "The cell's value for this key", body = CellResponse),
        (status = 400, description = "Malformed or out-of-range coordinate", body = ErrorBody),
        (status = 401, description = "Missing or invalid credentials", body = ErrorBody),
        (status = 404, description = "No such database, or no value set for this key at this cell", body = ErrorBody),
    ),
    security(("basic_auth" = [])),
)]
async fn get_cell(
    State(state): State<AppState>,
    Path((db, coords, key)): Path<(String, String, String)>,
) -> Result<Json<CellResponse>, ApiError> {
    let coord = parse_coords(&coords)?;
    let lookup_key = key.clone();
    let found = state
        .with_database(&db, move |w| w.get_with_meta(&coord, &lookup_key))
        .await?;
    match found {
        Some((v, meta)) => Ok(Json(CellResponse {
            value: ValueJson::from(v),
            created_at_ms: meta.created_at_ms,
            modified_at_ms: meta.modified_at_ms,
            version: meta.version,
        })),
        None => Err(ApiError::NotFound(format!(
            "no value set for key '{key}' at ({coords})"
        ))),
    }
}

#[utoipa::path(
    put,
    path = "/rest/db/{db}/cells/{coords}/{key}",
    tag = "cells",
    params(
        ("db" = String, Path, description = "The database to write"),
        ("coords" = String, Path, description = "Comma-separated coordinate, one i32 per axis (e.g. `1,2,3`)"),
        ("key" = String, Path, description = "The column/key to write"),
    ),
    request_body = ValueJson,
    responses(
        (status = 204, description = "The value was set"),
        (status = 400, description = "Malformed or out-of-range coordinate, or malformed body", body = ErrorBody),
        (status = 401, description = "Missing or invalid credentials", body = ErrorBody),
        (status = 403, description = "This account is read-only", body = ErrorBody),
        (status = 404, description = "No such database", body = ErrorBody),
    ),
    security(("basic_auth" = [])),
)]
async fn set_cell(
    State(state): State<AppState>,
    Path((db, coords, key)): Path<(String, String, String)>,
    Json(body): Json<ValueJson>,
) -> Result<StatusCode, ApiError> {
    let coord = parse_coords(&coords)?;
    let value: kblockdblib::Value = body.into();
    let (coord2, key2, value2) = (coord.clone(), key.clone(), value.clone());
    let meta = state
        .with_database(&db, move |w| w.set(&coord, &key, value))
        .await?;
    kblockdbcluster::hub::publish_set(&state.replication, &db, &coord2, &key2, value2, meta);
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    delete,
    path = "/rest/db/{db}/cells/{coords}/{key}",
    tag = "cells",
    params(
        ("db" = String, Path, description = "The database to write"),
        ("coords" = String, Path, description = "Comma-separated coordinate, one i32 per axis (e.g. `1,2,3`)"),
        ("key" = String, Path, description = "The column/key to clear"),
    ),
    responses(
        (status = 204, description = "The cell's value for this key was cleared (or was already unset)"),
        (status = 400, description = "Malformed or out-of-range coordinate", body = ErrorBody),
        (status = 401, description = "Missing or invalid credentials", body = ErrorBody),
        (status = 403, description = "This account is read-only", body = ErrorBody),
        (status = 404, description = "No such database", body = ErrorBody),
    ),
    security(("basic_auth" = [])),
)]
async fn remove_cell(
    State(state): State<AppState>,
    Path((db, coords, key)): Path<(String, String, String)>,
) -> Result<StatusCode, ApiError> {
    let coord = parse_coords(&coords)?;
    let (coord2, key2) = (coord.clone(), key.clone());
    let removed_at = state
        .with_database(&db, move |w| w.remove(&coord, &key))
        .await?;
    // Only a value that was actually there is replicated, at the time
    // `remove` recorded for it.
    if let Some(removed_at) = removed_at {
        kblockdbcluster::hub::publish_remove(&state.replication, &db, &coord2, &key2, removed_at);
    }
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
    path = "/rest/db/{db}/regions/{origin}/{extent}/{key}",
    tag = "regions",
    params(
        ("db" = String, Path, description = "The database to read"),
        ("origin" = String, Path, description = "Comma-separated region origin, one i32 per axis"),
        ("extent" = String, Path, description = "Comma-separated region extent, one i32 per axis"),
        ("key" = String, Path, description = "The column/key to read"),
    ),
    responses(
        (status = 200, description = "One value (or null) per cell in the region", body = RegionValuesResponse),
        (status = 400, description = "Malformed/out-of-range coordinate, or mismatched axis counts", body = ErrorBody),
        (status = 401, description = "Missing or invalid credentials", body = ErrorBody),
        (status = 404, description = "No such database", body = ErrorBody),
    ),
    security(("basic_auth" = [])),
)]
async fn get_region(
    State(state): State<AppState>,
    Path((db, origin, extent, key)): Path<(String, String, String, String)>,
) -> Result<Json<RegionValuesResponse>, ApiError> {
    let origin = parse_coords(&origin)?;
    let extent = parse_coords(&extent)?;
    let region = kblockdblib::Region::new(origin, extent);
    let values = state
        .with_database(&db, move |w| w.get_region(&region, &key))
        .await?;
    let values: Vec<Option<ValueJson>> =
        values.into_iter().map(|v| v.map(ValueJson::from)).collect();
    Ok(Json(RegionValuesResponse { values }))
}

/// `PUT /db/{db}/regions/.../.../<key>` body: one value per cell, in the
/// same axis-0-fastest order `GET` returns them in. Its length must equal
/// the region's volume exactly -- `World::set_region` itself enforces that
/// and this surfaces as `400 Bad Request` via `ApiError::from(io::Error)`.
#[derive(Deserialize, ToSchema)]
pub struct SetRegionBody {
    values: Vec<ValueJson>,
}

#[utoipa::path(
    put,
    path = "/rest/db/{db}/regions/{origin}/{extent}/{key}",
    tag = "regions",
    params(
        ("db" = String, Path, description = "The database to write"),
        ("origin" = String, Path, description = "Comma-separated region origin, one i32 per axis"),
        ("extent" = String, Path, description = "Comma-separated region extent, one i32 per axis"),
        ("key" = String, Path, description = "The column/key to write"),
    ),
    request_body = SetRegionBody,
    responses(
        (status = 204, description = "Every cell in the region was set"),
        (status = 400, description = "Malformed/out-of-range coordinate, mismatched axis counts, or wrong number of values", body = ErrorBody),
        (status = 401, description = "Missing or invalid credentials", body = ErrorBody),
        (status = 403, description = "This account is read-only", body = ErrorBody),
        (status = 404, description = "No such database", body = ErrorBody),
    ),
    security(("basic_auth" = [])),
)]
async fn set_region(
    State(state): State<AppState>,
    Path((db, origin, extent, key)): Path<(String, String, String, String)>,
    Json(body): Json<SetRegionBody>,
) -> Result<StatusCode, ApiError> {
    let origin = parse_coords(&origin)?;
    let extent = parse_coords(&extent)?;
    let region = kblockdblib::Region::new(origin, extent);
    let values: Vec<kblockdblib::Value> = body.values.into_iter().map(Into::into).collect();
    let (region2, key2, values2) = (region.clone(), key.clone(), values.clone());
    let metas = state
        .with_database(&db, move |w| w.set_region(&region, &key, &values))
        .await?;
    for ((coord, value), meta) in region2.iter().zip(values2).zip(metas) {
        kblockdbcluster::hub::publish_set(&state.replication, &db, &coord, &key2, value, meta);
    }
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    delete,
    path = "/rest/db/{db}/regions/{origin}/{extent}/{key}",
    tag = "regions",
    params(
        ("db" = String, Path, description = "The database to write"),
        ("origin" = String, Path, description = "Comma-separated region origin, one i32 per axis"),
        ("extent" = String, Path, description = "Comma-separated region extent, one i32 per axis"),
        ("key" = String, Path, description = "The column/key to clear"),
    ),
    responses(
        (status = 204, description = "Every cell in the region had this key cleared"),
        (status = 400, description = "Malformed/out-of-range coordinate, or mismatched axis counts", body = ErrorBody),
        (status = 401, description = "Missing or invalid credentials", body = ErrorBody),
        (status = 403, description = "This account is read-only", body = ErrorBody),
        (status = 404, description = "No such database", body = ErrorBody),
    ),
    security(("basic_auth" = [])),
)]
async fn remove_region(
    State(state): State<AppState>,
    Path((db, origin, extent, key)): Path<(String, String, String, String)>,
) -> Result<StatusCode, ApiError> {
    let origin = parse_coords(&origin)?;
    let extent = parse_coords(&extent)?;
    let region = kblockdblib::Region::new(origin, extent);
    let key2 = key.clone();
    let removed = state
        .with_database(&db, move |w| w.remove_region(&region, &key))
        .await?;
    for coord in &removed.coords {
        kblockdbcluster::hub::publish_remove(&state.replication, &db, coord, &key2, removed.at_ms);
    }
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Serialize, ToSchema)]
pub struct StatsResponse {
    total_chunks: u64,
    total_bytes: u64,
    total_blocks: u64,
}

#[utoipa::path(
    get,
    path = "/rest/db/{db}/stats",
    tag = "stats",
    params(
        ("db" = String, Path, description = "The database to inspect"),
    ),
    responses(
        (status = 200, description = "On-disk statistics for this database's data", body = StatsResponse),
        (status = 401, description = "Missing or invalid credentials", body = ErrorBody),
        (status = 404, description = "No such database", body = ErrorBody),
    ),
    security(("basic_auth" = [])),
)]
async fn stats(
    State(state): State<AppState>,
    Path(db): Path<String>,
) -> Result<Json<StatsResponse>, ApiError> {
    let stats = state.with_database(&db, kblockdblib::World::stats).await?;
    Ok(Json(StatsResponse {
        total_chunks: stats.total_chunks,
        total_bytes: stats.total_bytes,
        total_blocks: stats.total_blocks,
    }))
}

#[derive(Deserialize, ToSchema)]
pub struct QueryRequest {
    /// e.g. `SELECT material, density WHERE x0 >= 10 AND x0 < 20` -- see
    /// the README's "Query language" section for the full grammar and
    /// more examples (`SET`/`UPDATE`/`DELETE`, ranges, `AND`/`OR`/`NOT`).
    query: String,
}

#[derive(Serialize, ToSchema)]
pub struct QueryKeyValue {
    pub(crate) key: String,
    pub(crate) value: ValueJson,
    /// Milliseconds since the Unix epoch when this key was first set at
    /// this cell -- same field `CellResponse` (the single-cell `GET`)
    /// reports (see `kblockdblib::CellMeta`).
    pub(crate) created_at_ms: u64,
    /// Milliseconds since the Unix epoch when this key was last set at
    /// this cell -- equal to `created_at_ms` if it's never been
    /// overwritten.
    pub(crate) modified_at_ms: u64,
    /// How many times this key has been overwritten at this cell since it
    /// was first set: 0 for a value that's never been overwritten.
    pub(crate) version: u64,
}

#[derive(Serialize, ToSchema)]
pub struct QueryRow {
    pub(crate) coord: Vec<i32>,
    pub(crate) values: Vec<QueryKeyValue>,
}

/// One `count`/`sum`/`mean`/`max`/`min` result from a `SELECT` whose
/// `<columns>` was an aggregate list rather than `*`/a key list -- see
/// `kblockdbquery::AggregateResult`.
#[derive(Serialize, ToSchema)]
pub struct AggregateResponse {
    /// Echoes the function call it was parsed from, e.g. `"sum(density)"`.
    pub(crate) label: String,
    /// `None` only for `mean`/`max`/`min` when no matching cell had a
    /// numeric value for the key in question -- never an error, same
    /// "doesn't apply" philosophy as the rest of this query language.
    pub(crate) value: Option<f64>,
}

/// `SELECT *`/`SELECT <columns>` populates `total_rows`/`rows`; `SELECT
/// count(*)`/`sum(...)`/... populates `aggregates` instead (one result
/// summarizing every matching cell, not one row per cell);
/// `SET`/`UPDATE`/`DELETE` populate `affected_cells` instead of either --
/// one statement, one endpoint, but a different result shape depending on
/// which kind was sent (fields the statement kind doesn't produce are
/// omitted, not null).
#[derive(Serialize, ToSchema)]
pub struct QueryResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) total_rows: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) rows: Option<Vec<QueryRow>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) aggregates: Option<Vec<AggregateResponse>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) affected_cells: Option<usize>,
}

impl QueryResponse {
    fn rows(rows: Vec<QueryRow>) -> Self {
        QueryResponse {
            total_rows: Some(rows.len()),
            rows: Some(rows),
            aggregates: None,
            affected_cells: None,
        }
    }

    fn aggregates(results: Vec<query::AggregateResult>) -> Self {
        QueryResponse {
            total_rows: None,
            rows: None,
            aggregates: Some(
                results
                    .into_iter()
                    .map(|r| AggregateResponse {
                        label: r.label,
                        value: r.value,
                    })
                    .collect(),
            ),
            affected_cells: None,
        }
    }

    fn affected(n: usize) -> Self {
        QueryResponse {
            total_rows: None,
            rows: None,
            aggregates: None,
            affected_cells: Some(n),
        }
    }
}

#[utoipa::path(
    post,
    path = "/rest/db/{db}/query",
    tag = "query",
    params(
        ("db" = String, Path, description = "The database to query"),
    ),
    request_body = QueryRequest,
    responses(
        (status = 200, description = "SELECT: the matching rows. SET/UPDATE/DELETE: how many cells were affected", body = QueryResponse),
        (status = 400, description = "Malformed query, or a range whose axis count doesn't match the database's", body = ErrorBody),
        (status = 401, description = "Missing or invalid credentials", body = ErrorBody),
        (status = 403, description = "A read-only account attempted a SET, UPDATE, or DELETE", body = ErrorBody),
        (status = 404, description = "No such database", body = ErrorBody),
    ),
    security(("basic_auth" = [])),
)]
async fn run_query(
    State(state): State<AppState>,
    Path(db): Path<String>,
    headers: HeaderMap,
    Json(body): Json<QueryRequest>,
) -> Response {
    let Some(account) = crate::auth::account_from_headers(&headers, &state) else {
        return crate::auth::unauthorized_response();
    };
    match execute_query(&state, &db, &account, &body.query).await {
        Ok(resp) => Json(resp).into_response(),
        Err(e) => e.into_response(),
    }
}

pub(crate) async fn execute_query(
    state: &AppState,
    db: &str,
    account: &crate::state::Account,
    query_text: &str,
) -> Result<QueryResponse, ApiError> {
    let stmt = query::parse(query_text)
        .map_err(|e| ApiError::BadRequest(format!("invalid query: {e}")))?;

    // Checked by statement kind, not HTTP method -- see this module's doc
    // comment.
    if stmt.is_write() && account.read_only {
        return Err(ApiError::Forbidden("this account is read-only".to_string()));
    }
    let axes = state.with_database(db, |w| Ok(w.axes())).await?;
    check_range_axes(&stmt, axes)?;

    match stmt {
        query::Statement::Select {
            columns,
            range,
            where_clause,
        } => {
            let cells = state
                .with_database(db, kblockdblib::World::list_cells)
                .await?;
            let matching: Vec<&kblockdblib::CellEntry> = cells
                .iter()
                .filter(|c| query::matches(range.as_ref(), where_clause.as_ref(), c))
                .collect();
            match &columns {
                query::Columns::Aggregates(aggregates) => Ok(QueryResponse::aggregates(
                    query::aggregate(aggregates, &matching),
                )),
                _ => {
                    let rows = matching.iter().map(|c| project_row(&columns, c)).collect();
                    Ok(QueryResponse::rows(rows))
                }
            }
        }
        query::Statement::Set {
            assignments,
            where_clause,
            range,
        } => execute_set(state, db, assignments, where_clause, range).await,
        query::Statement::Update {
            assignments,
            where_clause,
            range,
        } => {
            let cells = state
                .with_database(db, kblockdblib::World::list_cells)
                .await?;
            // Snapshotted here, then mutated in a second `with_database`
            // call below -- not atomic with respect to a concurrent writer
            // touching the same cells in between (a classic find-then-
            // mutate race), same honest caveat as any bulk operation built
            // on a point-in-time `list_cells` scan rather than a
            // world-wide lock.
            let matching: Vec<Vec<i32>> = cells
                .iter()
                .filter(|c| query::matches(range.as_ref(), where_clause.as_ref(), c))
                .map(|c| c.coord.to_vec())
                .collect();
            let affected = matching.len();
            let written: Vec<(Vec<i32>, String, kblockdblib::Value, kblockdblib::CellMeta)> = state
                .with_database(db, move |w| {
                    let mut written = Vec::new();
                    for coord in &matching {
                        for (key, literal) in &assignments {
                            let value = literal.to_value();
                            let meta = w.set(coord, key, value.clone())?;
                            written.push((coord.clone(), key.clone(), value, meta));
                        }
                    }
                    Ok(written)
                })
                .await?;
            for (coord, key, value, meta) in written {
                kblockdbcluster::hub::publish_set(
                    &state.replication,
                    db,
                    &coord,
                    &key,
                    value,
                    meta,
                );
            }
            Ok(QueryResponse::affected(affected))
        }
        query::Statement::Delete {
            where_clause,
            range,
        } => {
            let cells = state
                .with_database(db, kblockdblib::World::list_cells)
                .await?;
            let matching: Vec<(Vec<i32>, Vec<String>)> = cells
                .iter()
                .filter(|c| query::matches(range.as_ref(), where_clause.as_ref(), c))
                .map(|c| {
                    (
                        c.coord.to_vec(),
                        c.values.iter().map(|(k, _, _)| k.clone()).collect(),
                    )
                })
                .collect();
            let affected = matching.len();
            let removed = state
                .with_database(db, move |w| {
                    let mut removed = Vec::new();
                    for (coord, keys) in matching {
                        for key in keys {
                            if let Some(at) = w.remove(&coord, &key)? {
                                removed.push((coord.clone(), key, at));
                            }
                        }
                    }
                    Ok(removed)
                })
                .await?;
            for (coord, key, at) in &removed {
                kblockdbcluster::hub::publish_remove(&state.replication, db, coord, key, *at);
            }
            Ok(QueryResponse::affected(affected))
        }
    }
}

/// `SET`'s upsert: every coordinate in `range` (mandatory -- see
/// `kblockdbquery`'s doc comment) satisfying `where_clause` gets `assignments`
/// written, whether or not a cell already existed there.
///
/// Two paths, chosen for cost, not just convenience:
/// - **No `WHERE`**: every coordinate in `range` matches unconditionally,
///   so this is exactly `World::set_region` once per assignment -- the
///   same primitive the `/rest/db/{db}/regions` endpoints use, and just as
///   efficient (no per-coordinate work in this handler at all).
/// - **With a `WHERE`**: which coordinates match can depend on a cell's
///   *existing* values, so each candidate coordinate needs to be checked
///   individually against either its existing `CellEntry` (if any) or a
///   synthetic empty one (if not -- under the same "a missing key never
///   matches" rule `query::eval` already applies everywhere else, so a
///   key-based `WHERE` naturally excludes not-yet-existing cells from
///   being created; only an axis-based or `WHERE`-less `SET` can actually
///   upsert).
///
/// Either way, not atomic with respect to a concurrent writer touching the
/// same range in between the read (existing cells, `WHERE` path only) and
/// the write -- same honest caveat as `UPDATE`/`DELETE`'s own
/// snapshot-then-mutate approach.
async fn execute_set(
    state: &AppState,
    db: &str,
    assignments: Vec<(String, query::Literal)>,
    where_clause: Option<query::Expr>,
    range: query::Range,
) -> Result<QueryResponse, ApiError> {
    let region = range_to_region(&range);

    let Some(expr) = where_clause else {
        let volume = region.volume() as usize;
        let values: Vec<(String, kblockdblib::Value)> = assignments
            .into_iter()
            .map(|(key, literal)| (key, literal.to_value()))
            .collect();
        let region2 = region.clone();
        let values2 = values.clone();
        let per_key_metas: Vec<Vec<kblockdblib::CellMeta>> = state
            .with_database(db, move |w| {
                let mut per_key_metas = Vec::with_capacity(values.len());
                for (key, value) in &values {
                    per_key_metas.push(w.set_region(&region, key, &vec![value.clone(); volume])?);
                }
                Ok(per_key_metas)
            })
            .await?;
        for ((key, value), metas) in values2.into_iter().zip(per_key_metas) {
            for (coord, meta) in region2.iter().zip(metas) {
                kblockdbcluster::hub::publish_set(
                    &state.replication,
                    db,
                    &coord,
                    &key,
                    value.clone(),
                    meta,
                );
            }
        }
        return Ok(QueryResponse::affected(volume));
    };

    let cells = state
        .with_database(db, kblockdblib::World::list_cells)
        .await?;
    let existing: HashMap<Vec<i32>, &kblockdblib::CellEntry> =
        cells.iter().map(|c| (c.coord.to_vec(), c)).collect();

    let targets: Vec<Vec<i32>> = region
        .iter()
        .map(|c| c.to_vec())
        .filter(|coord| match existing.get(coord) {
            Some(cell) => query::eval(&expr, cell),
            None => {
                let synthetic = kblockdblib::CellEntry {
                    coord: coord.as_slice().into(),
                    values: Vec::new(),
                };
                query::eval(&expr, &synthetic)
            }
        })
        .collect();

    let affected = targets.len();
    let written: Vec<(Vec<i32>, String, kblockdblib::Value, kblockdblib::CellMeta)> = state
        .with_database(db, move |w| {
            let mut written = Vec::new();
            for coord in &targets {
                for (key, literal) in &assignments {
                    let value = literal.to_value();
                    let meta = w.set(coord, key, value.clone())?;
                    written.push((coord.clone(), key.clone(), value, meta));
                }
            }
            Ok(written)
        })
        .await?;
    for (coord, key, value, meta) in written {
        kblockdbcluster::hub::publish_set(&state.replication, db, &coord, &key, value, meta);
    }
    Ok(QueryResponse::affected(affected))
}

fn range_to_region(range: &query::Range) -> kblockdblib::Region {
    let extent: Vec<i32> = range
        .to
        .iter()
        .zip(&range.from)
        .map(|(&to, &from)| to - from)
        .collect();
    kblockdblib::Region::new(range.from.clone(), extent)
}

/// A query's optional `FROM`/`IN` range must name exactly as many axes as
/// the target database has -- `kblockdbquery` itself can't check this (it
/// has no `World` to check against), so `execute_query` does, before running
/// anything, the same "reject up front, don't touch any data" policy
/// `check_region` (used by the region endpoints above) follows.
fn check_range_axes(stmt: &query::Statement, axes: usize) -> Result<(), ApiError> {
    let range: Option<&query::Range> = match stmt {
        query::Statement::Select { range, .. }
        | query::Statement::Update { range, .. }
        | query::Statement::Delete { range, .. } => range.as_ref(),
        query::Statement::Set { range, .. } => Some(range),
    };
    match range {
        Some(r) if r.from.len() != axes => Err(ApiError::BadRequest(format!(
            "range has {} axes but this database has {axes}",
            r.from.len()
        ))),
        _ => Ok(()),
    }
}

/// Never called with `columns: &query::Columns::Aggregates(_)` -- the
/// caller (`execute_query`'s `Select` arm) branches on that case itself
/// and calls `query::aggregate` instead, since an aggregate `SELECT`
/// produces one result per function, not one row per cell the way `All`/
/// `Named` do here.
fn project_row(columns: &query::Columns, cell: &kblockdblib::CellEntry) -> QueryRow {
    let selected: Box<dyn Iterator<Item = &(String, kblockdblib::Value, kblockdblib::CellMeta)>> =
        match columns {
            query::Columns::All => Box::new(cell.values.iter()),
            query::Columns::Named(names) => Box::new(
                names
                    .iter()
                    .filter_map(|name| cell.values.iter().find(|(k, _, _)| k == name)),
            ),
            query::Columns::Aggregates(_) => {
                unreachable!("execute_query's Select arm never calls project_row for Aggregates")
            }
        };
    let values = selected
        .map(|(k, v, meta)| QueryKeyValue {
            key: k.clone(),
            value: ValueJson::from(v.clone()),
            created_at_ms: meta.created_at_ms,
            modified_at_ms: meta.modified_at_ms,
            version: meta.version,
        })
        .collect();
    QueryRow {
        coord: cell.coord.to_vec(),
        values,
    }
}
