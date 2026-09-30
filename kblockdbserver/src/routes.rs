//! The REST API surface, mounted under the `/rest` context path: two
//! resources, `/rest/cells/...` (a single cell) and `/rest/regions/...`
//! (an axis-aligned box of cells), each with GET (read), PUT (write) and
//! DELETE (remove) -- plus read-only `/rest/stats` and `/rest/health`.
//!
//! Coordinates and region origin/extent are comma-separated path segments
//! (`/rest/cells/1,2,3/material`, `/rest/regions/0,0,0/8,8,8/material`),
//! matching however many axes the world was created with -- there's
//! nothing 3-axis-specific here, same as in `kblockdblib` itself.
//!
//! Every route below requires HTTP Basic Auth (see `auth.rs`) except
//! `/rest/health`, left open so load balancers/orchestrators can poll
//! liveness without credentials -- it exposes nothing more sensitive than
//! the world's shape and this server's clock. `/rest/stats` is a `GET`, so
//! a `read_only` account can use it same as any other account.
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
//! `/rows`, see that module) -- a read-only data browser over this same
//! world, deliberately kept outside `/rest` since it isn't part of the
//! versioned REST API surface.
//!
//! `POST /rest/query` (see `run_query` below) is the one route here that
//! doesn't use `require_auth`: that middleware's `read_only` check is
//! purely a function of HTTP method (`GET` = read, anything else = write),
//! but one `POST /rest/query` can be *either* depending on the query text
//! itself (`SELECT` vs `SET`/`UPDATE`/`DELETE` -- see `query.rs`'s
//! `Statement::is_write`). `run_query` authenticates the same way
//! `require_auth` does (`auth::account_from_headers`) and only then checks
//! `read_only` against the parsed statement, not the HTTP method.

use crate::auth::require_auth;
use crate::browser;
use crate::coords::parse_coords;
use crate::error::{ApiError, ErrorBody};
use crate::openapi::ApiDoc;
use crate::query;
use crate::state::AppState;
use crate::value_json::ValueJson;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::middleware;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
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

    // Not under `protected` -- see this module's doc comment on why
    // `/query` needs its own auth check instead of `require_auth`'s.
    let query_route = Router::new().route("/query", post(run_query));

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
    axes: usize,
    world_dim: u32,
    /// Seconds since the Unix epoch, per this server's own clock -- lets a
    /// caller sanity-check clock skew or confirm the response isn't a
    /// stale cached one.
    timestamp: u64,
}

#[utoipa::path(
    get,
    path = "/rest/health",
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
    path = "/rest/stats",
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
    path = "/rest/cells/{coords}/{key}",
    tag = "cells",
    params(
        ("coords" = String, Path, description = "Comma-separated coordinate, one i32 per axis (e.g. `1,2,3`)"),
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
        .with_world(move |w| w.get_with_meta(&coord, &lookup_key))
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
    path = "/rest/cells/{coords}/{key}",
    tag = "cells",
    params(
        ("coords" = String, Path, description = "Comma-separated coordinate, one i32 per axis (e.g. `1,2,3`)"),
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
    path = "/rest/cells/{coords}/{key}",
    tag = "cells",
    params(
        ("coords" = String, Path, description = "Comma-separated coordinate, one i32 per axis (e.g. `1,2,3`)"),
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
    path = "/rest/regions/{origin}/{extent}/{key}",
    tag = "regions",
    params(
        ("origin" = String, Path, description = "Comma-separated region origin, one i32 per axis"),
        ("extent" = String, Path, description = "Comma-separated region extent, one i32 per axis"),
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
    path = "/rest/regions/{origin}/{extent}/{key}",
    tag = "regions",
    params(
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
    path = "/rest/regions/{origin}/{extent}/{key}",
    tag = "regions",
    params(
        ("origin" = String, Path, description = "Comma-separated region origin, one i32 per axis"),
        ("extent" = String, Path, description = "Comma-separated region extent, one i32 per axis"),
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

#[derive(Deserialize, ToSchema)]
pub struct QueryRequest {
    /// e.g. `SELECT material, density WHERE x0 >= 10 AND x0 < 20` -- see
    /// the README's "Query language" section for the full grammar and
    /// more examples (`SET`/`UPDATE`/`DELETE`, ranges, `AND`/`OR`/`NOT`).
    query: String,
}

#[derive(Serialize, ToSchema)]
pub struct QueryKeyValue {
    key: String,
    value: ValueJson,
}

#[derive(Serialize, ToSchema)]
pub struct QueryRow {
    coord: Vec<i32>,
    values: Vec<QueryKeyValue>,
}

/// `SELECT` populates `total_rows`/`rows`; `SET`/`UPDATE`/`DELETE` populate
/// `affected_cells` instead -- one statement, one endpoint, but a
/// different result shape depending on which kind was sent (fields the
/// statement kind doesn't produce are omitted, not null).
#[derive(Serialize, ToSchema)]
pub struct QueryResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    total_rows: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    rows: Option<Vec<QueryRow>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    affected_cells: Option<usize>,
}

impl QueryResponse {
    fn rows(rows: Vec<QueryRow>) -> Self {
        QueryResponse {
            total_rows: Some(rows.len()),
            rows: Some(rows),
            affected_cells: None,
        }
    }

    fn affected(n: usize) -> Self {
        QueryResponse {
            total_rows: None,
            rows: None,
            affected_cells: Some(n),
        }
    }
}

#[utoipa::path(
    post,
    path = "/rest/query",
    tag = "query",
    request_body = QueryRequest,
    responses(
        (status = 200, description = "SELECT: the matching rows. SET/UPDATE/DELETE: how many cells were affected", body = QueryResponse),
        (status = 400, description = "Malformed query, or a range whose axis count doesn't match the world's", body = ErrorBody),
        (status = 401, description = "Missing or invalid credentials", body = ErrorBody),
        (status = 403, description = "A read-only account attempted a SET, UPDATE, or DELETE", body = ErrorBody),
    ),
    security(("basic_auth" = [])),
)]
async fn run_query(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<QueryRequest>,
) -> Response {
    let Some(account) = crate::auth::account_from_headers(&headers, &state) else {
        return crate::auth::unauthorized_response();
    };
    match execute_query(&state, &account, &body.query).await {
        Ok(resp) => Json(resp).into_response(),
        Err(e) => e.into_response(),
    }
}

async fn execute_query(
    state: &AppState,
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
    check_range_axes(&stmt, state.world.axes())?;

    match stmt {
        query::Statement::Select {
            columns,
            range,
            where_clause,
        } => {
            let cells = state.with_world(kblockdblib::World::list_cells).await?;
            let rows = cells
                .iter()
                .filter(|c| query::matches(range.as_ref(), where_clause.as_ref(), c))
                .map(|c| project_row(&columns, c))
                .collect();
            Ok(QueryResponse::rows(rows))
        }
        query::Statement::Set {
            assignments,
            where_clause,
            range,
        } => execute_set(state, assignments, where_clause, range).await,
        query::Statement::Update {
            assignments,
            where_clause,
            range,
        } => {
            let cells = state.with_world(kblockdblib::World::list_cells).await?;
            // Snapshotted here, then mutated in a second `with_world` call
            // below -- not atomic with respect to a concurrent writer
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
            state
                .with_world(move |w| {
                    for coord in &matching {
                        for (key, literal) in &assignments {
                            w.set(coord, key, literal.to_value())?;
                        }
                    }
                    Ok(())
                })
                .await?;
            Ok(QueryResponse::affected(affected))
        }
        query::Statement::Delete {
            where_clause,
            range,
        } => {
            let cells = state.with_world(kblockdblib::World::list_cells).await?;
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
            state
                .with_world(move |w| {
                    for (coord, keys) in &matching {
                        for key in keys {
                            w.remove(coord, key)?;
                        }
                    }
                    Ok(())
                })
                .await?;
            Ok(QueryResponse::affected(affected))
        }
    }
}

/// `SET`'s upsert: every coordinate in `range` (mandatory -- see
/// `query.rs`'s doc comment) satisfying `where_clause` gets `assignments`
/// written, whether or not a cell already existed there.
///
/// Two paths, chosen for cost, not just convenience:
/// - **No `WHERE`**: every coordinate in `range` matches unconditionally,
///   so this is exactly `World::set_region` once per assignment -- the
///   same primitive the `/rest/regions` endpoints use, and just as
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
        state
            .with_world(move |w| {
                for (key, value) in &values {
                    w.set_region(&region, key, &vec![value.clone(); volume])?;
                }
                Ok(())
            })
            .await?;
        return Ok(QueryResponse::affected(volume));
    };

    let cells = state.with_world(kblockdblib::World::list_cells).await?;
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
    state
        .with_world(move |w| {
            for coord in &targets {
                for (key, literal) in &assignments {
                    w.set(coord, key, literal.to_value())?;
                }
            }
            Ok(())
        })
        .await?;
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
/// the target world has -- `query.rs` itself can't check this (it has no
/// `World` to check against), so `execute_query` does, before running
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
            "range has {} axes but this world has {axes}",
            r.from.len()
        ))),
        _ => Ok(()),
    }
}

fn project_row(columns: &query::Columns, cell: &kblockdblib::CellEntry) -> QueryRow {
    let selected: Box<dyn Iterator<Item = &(String, kblockdblib::Value, kblockdblib::CellMeta)>> =
        match columns {
            query::Columns::All => Box::new(cell.values.iter()),
            query::Columns::Named(names) => Box::new(
                names
                    .iter()
                    .filter_map(|name| cell.values.iter().find(|(k, _, _)| k == name)),
            ),
        };
    let values = selected
        .map(|(k, v, _)| QueryKeyValue {
            key: k.clone(),
            value: ValueJson::from(v.clone()),
        })
        .collect();
    QueryRow {
        coord: cell.coord.to_vec(),
        values,
    }
}
