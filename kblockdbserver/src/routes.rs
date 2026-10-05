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
use kblockdbcluster::hub::Stamper;
use kblockdblib::{VersionVector, LEGACY_BIT};
use kblockdbquery as query;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
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
        .route("/db/{db}/indexes", get(list_indexes))
        .route(
            "/db/{db}/indexes/{key}",
            put(create_index).delete(remove_index),
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
    /// This instance's cluster node id, as 16 hex digits; `null` unless
    /// clustering is configured.
    node_id: Option<String>,
    /// This instance's version vector: per origin node (hex id; a
    /// `-legacy` suffix marks that node's pre-clustering data), the
    /// highest of its writes this instance is guaranteed to have (see
    /// docs/clustering.md's "Catch-up"). Two instances with equal vectors
    /// have seen exactly the same writes. Empty unless clustering is
    /// configured.
    vector: BTreeMap<String, u64>,
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
    let peers: Vec<String> = peer_snapshot(&state)
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
        node_id: state.peers.as_ref().map(|p| node_id_hex(p.node_id())),
        vector: state
            .peers
            .as_ref()
            .map(|p| vector_json(&p.local_vector()))
            .unwrap_or_default(),
    }))
}

/// A node id as JSON shows it: 16 hex digits (a u64 doesn't fit in a
/// JavaScript number).
fn node_id_hex(node_id: u64) -> String {
    format!("{node_id:016x}")
}

/// A version vector as JSON shows it: keyed by `node_id_hex`, with a
/// node's legacy pseudo-origin (see `kblockdblib::legacy_origin`) as its
/// id plus `-legacy`.
fn vector_json(vector: &VersionVector) -> BTreeMap<String, u64> {
    vector
        .iter()
        .map(|(origin, seq)| {
            let key = if origin & LEGACY_BIT != 0 {
                format!("{}-legacy", node_id_hex(origin & !LEGACY_BIT))
            } else {
                node_id_hex(origin)
            };
            (key, seq)
        })
        .collect()
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
    /// The peer's node id (see `HealthResponse::node_id`); `null` until
    /// this instance's link to it has connected once.
    node_id: Option<String>,
    /// The peer's version vector as it last reported it (every few
    /// seconds while its link to this instance is up); `null` until it
    /// has.
    vector: Option<BTreeMap<String, u64>>,
    /// The peer's own sequence number: the latest of its own writes it has
    /// confirmed (its own entry in `vector`). Until it has reported a
    /// vector, the latest of its writes this instance is known to have;
    /// `null` if neither is known yet.
    seq: Option<u64>,
    /// When the peer last confirmed it had sent this instance everything
    /// (its last `Synced`, every few seconds while its link is up), in ms
    /// since the Unix epoch by this instance's clock; `null` if it hasn't
    /// yet since this instance started.
    last_synced_ms: Option<u64>,
    /// The peer's vector compared with this instance's.
    sync: SyncState,
    /// Roughly how many writes this instance has that the peer doesn't:
    /// the sum of the sequence-number gaps. Approximate -- a restart can
    /// skip sequence numbers.
    behind_by: u64,
    /// Roughly how many writes the peer has that this instance doesn't.
    ahead_by: u64,
}

/// How a peer's version vector compares with this instance's.
#[derive(Serialize, ToSchema, Debug, PartialEq, Eq, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum SyncState {
    /// Both have seen exactly the same writes.
    InSync,
    /// The peer is missing writes this instance has (it'll catch up).
    Behind,
    /// This instance is missing writes the peer has.
    Ahead,
    /// Each is missing some of the other's writes.
    Diverged,
    /// The peer hasn't reported its vector yet.
    Unknown,
}

/// Compares `theirs` with `mine`: the sync state, then roughly how many
/// writes they're missing (`behind_by`) and have that `mine` doesn't
/// (`ahead_by`). An origin one side lacks entirely counts as missing.
fn compare_vectors(mine: &VersionVector, theirs: &VersionVector) -> (SyncState, u64, u64) {
    let (mut behind, mut ahead) = (false, false);
    let (mut behind_by, mut ahead_by) = (0, 0);
    for (origin, seq) in mine.iter() {
        match theirs.get(origin) {
            None => {
                behind = true;
                behind_by += seq;
            }
            Some(theirs) if theirs < seq => {
                behind = true;
                behind_by += seq - theirs;
            }
            Some(theirs) if theirs > seq => {
                ahead = true;
                ahead_by += theirs - seq;
            }
            Some(_) => {}
        }
    }
    for (origin, seq) in theirs.iter() {
        if mine.get(origin).is_none() {
            ahead = true;
            ahead_by += seq;
        }
    }
    let state = match (behind, ahead) {
        (false, false) => SyncState::InSync,
        (true, false) => SyncState::Behind,
        (false, true) => SyncState::Ahead,
        (true, true) => SyncState::Diverged,
    };
    (state, behind_by, ahead_by)
}

#[derive(Serialize, ToSchema)]
pub struct ClusterResponse {
    /// This instance's own name, same as `HealthResponse::hostname`.
    hostname: String,
    /// Same as `HealthResponse::node_id`.
    node_id: Option<String>,
    /// Same as `HealthResponse::vector`.
    vector: BTreeMap<String, u64>,
    /// This instance's own sequence number: the latest of its own writes
    /// it has confirmed (its own entry in `vector`). `null` unless
    /// clustering is configured.
    seq: Option<u64>,
    /// This instance's current time, ms since the Unix epoch -- the clock
    /// each peer's `last_synced_ms` is by, for working out how long ago.
    now_ms: u64,
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
    let Some(peer_set) = state.peers.as_ref() else {
        return Json(ClusterResponse {
            hostname: state.hostname.to_string(),
            node_id: None,
            vector: BTreeMap::new(),
            seq: None,
            now_ms: kblockdbcluster::hub::now_ms(),
            peers: Vec::new(),
        });
    };
    let mine = peer_set.local_vector();
    let peers = peer_set
        .snapshot()
        .into_iter()
        .map(|(host, connected)| {
            let node_id = peer_set.node_for(&host);
            let theirs = node_id.and_then(|id| peer_set.vectors().peer(id));
            let (sync, behind_by, ahead_by) = match &theirs {
                Some(theirs) => compare_vectors(&mine, theirs),
                None => (SyncState::Unknown, 0, 0),
            };
            let seq = node_id.and_then(|id| {
                theirs
                    .as_ref()
                    .and_then(|theirs| theirs.get(id))
                    .or_else(|| mine.get(id))
            });
            ClusterPeer {
                host,
                connected,
                node_id: node_id.map(node_id_hex),
                vector: theirs.as_ref().map(vector_json),
                seq,
                last_synced_ms: node_id.and_then(|id| peer_set.vectors().peer_last_synced(id)),
                sync,
                behind_by,
                ahead_by,
            }
        })
        .collect();
    Json(ClusterResponse {
        hostname: state.hostname.to_string(),
        node_id: Some(node_id_hex(peer_set.node_id())),
        seq: mine.get(peer_set.node_id()),
        now_ms: kblockdbcluster::hub::now_ms(),
        vector: vector_json(&mine),
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

/// Every key with a secondary equality index built on it (see
/// `kblockdblib::World::create_index`), sorted.
#[derive(Serialize, ToSchema)]
pub struct IndexesResponse {
    indexes: Vec<String>,
}

#[utoipa::path(
    get,
    path = "/rest/db/{db}/indexes",
    tag = "indexes",
    params(
        ("db" = String, Path, description = "The database to read"),
    ),
    responses(
        (status = 200, description = "Every key with a secondary index built on it", body = IndexesResponse),
        (status = 401, description = "Missing or invalid credentials", body = ErrorBody),
        (status = 404, description = "No such database", body = ErrorBody),
    ),
    security(("basic_auth" = [])),
)]
async fn list_indexes(
    State(state): State<AppState>,
    Path(db): Path<String>,
) -> Result<Json<IndexesResponse>, ApiError> {
    let indexes = state.with_database(&db, |w| Ok(w.indexed_keys())).await?;
    Ok(Json(IndexesResponse { indexes }))
}

#[utoipa::path(
    put,
    path = "/rest/db/{db}/indexes/{key}",
    tag = "indexes",
    params(
        ("db" = String, Path, description = "The database to write"),
        ("key" = String, Path, description = "The column/key to build a secondary index on"),
    ),
    responses(
        (status = 204, description = "The index now exists (freshly built, or already did)"),
        (status = 401, description = "Missing or invalid credentials", body = ErrorBody),
        (status = 403, description = "This account is read-only", body = ErrorBody),
        (status = 404, description = "No such database", body = ErrorBody),
    ),
    security(("basic_auth" = [])),
)]
async fn create_index(
    State(state): State<AppState>,
    Path((db, key)): Path<(String, String)>,
) -> Result<StatusCode, ApiError> {
    state
        .with_database(&db, move |w| w.create_index(&key))
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    delete,
    path = "/rest/db/{db}/indexes/{key}",
    tag = "indexes",
    params(
        ("db" = String, Path, description = "The database to write"),
        ("key" = String, Path, description = "The indexed column/key to stop indexing"),
    ),
    responses(
        (status = 204, description = "The index was dropped"),
        (status = 401, description = "Missing or invalid credentials", body = ErrorBody),
        (status = 403, description = "This account is read-only", body = ErrorBody),
        (status = 404, description = "No such database, or no index on this key", body = ErrorBody),
    ),
    security(("basic_auth" = [])),
)]
async fn remove_index(
    State(state): State<AppState>,
    Path((db, key)): Path<(String, String)>,
) -> Result<StatusCode, ApiError> {
    let lookup_key = key.clone();
    let dropped = state
        .with_database(&db, move |w| Ok(w.drop_index(&lookup_key)))
        .await?;
    if dropped {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(ApiError::NotFound(format!("no index on key '{key}'")))
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
    let stamper = Stamper::new(&state.replication);
    let stamps = stamper.clone();
    let (meta, stamp) = state
        .with_database(&db, move |w| {
            w.set_stamped(&coord, &key, value, &mut || stamps.next())
        })
        .await?;
    stamper.publish_set(&db, &coord2, &key2, value2, meta, stamp);
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
    let stamper = Stamper::new(&state.replication);
    let stamps = stamper.clone();
    let removed = state
        .with_database(&db, move |w| {
            w.remove_stamped(&coord, &key, &mut || stamps.next())
        })
        .await?;
    // Only a value that was actually there is replicated, at the time
    // `remove` recorded for it.
    if let Some((removed_at, stamp)) = removed {
        stamper.publish_remove(&db, &coord2, &key2, removed_at, stamp);
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
    let stamper = Stamper::new(&state.replication);
    let stamps = stamper.clone();
    let written = state
        .with_database(&db, move |w| {
            w.set_region_stamped(&region, &key, &values, &mut || stamps.next())
        })
        .await?;
    for ((coord, value), (meta, stamp)) in region2.iter().zip(values2).zip(written) {
        stamper.publish_set(&db, &coord, &key2, value, meta, stamp);
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
    let stamper = Stamper::new(&state.replication);
    let stamps = stamper.clone();
    let removed = state
        .with_database(&db, move |w| {
            w.remove_region_stamped(&region, &key, &mut || stamps.next())
        })
        .await?;
    for (coord, stamp) in &removed.cells {
        stamper.publish_remove(&db, coord, &key2, removed.at_ms, *stamp);
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

    /// `CREATE INDEX`/`DROP INDEX`: an empty success body (`{}`) -- neither
    /// statement produces rows, aggregates, or an affected-cell count (an
    /// index is a schema-level thing, not a per-cell one).
    fn ok() -> Self {
        QueryResponse {
            total_rows: None,
            rows: None,
            aggregates: None,
            affected_cells: None,
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
        (status = 200, description = "SELECT: the matching rows. SET/UPDATE/DELETE: how many cells were affected. CREATE INDEX/DROP INDEX/REBUILD INDEX: an empty body", body = QueryResponse),
        (status = 400, description = "Malformed query, or a range whose axis count doesn't match the database's", body = ErrorBody),
        (status = 401, description = "Missing or invalid credentials", body = ErrorBody),
        (status = 403, description = "A read-only account attempted a SET, UPDATE, DELETE, CREATE INDEX, DROP INDEX, or REBUILD INDEX", body = ErrorBody),
        (status = 404, description = "No such database, or (for DROP INDEX) no index on the given key", body = ErrorBody),
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

/// Every cell matching `range`/`where_clause`, preferring a secondary
/// index over a full `list_cells` scan whenever `where_clause` makes that
/// possible -- used by `SELECT`/`UPDATE`/`DELETE` (not `SET`'s `WHERE`
/// path, which needs every candidate *coordinate* in its range, including
/// ones with no cell yet, so an index over existing values can't help it
/// the same way).
///
/// Tries `query::candidate_coords` first: if `where_clause` narrows down
/// to a concrete coordinate set (every key compared with `=` at the
/// expression's top level has an index -- see `kblockdblib::World::
/// create_index`), this reads just those cells (`World::cell_entry_at`)
/// instead of decoding the whole world. `candidate_coords` only ever
/// returns a *superset* when part of `where_clause` couldn't be narrowed
/// (an `OR`, a `<`/`>` comparison, `EXISTS`, ...), so every candidate is
/// still re-checked against the full `range`/`where_clause` via
/// `query::matches` before it's reported as a real match -- same final
/// result as the full-scan path, just reached without decoding cells that
/// could never have matched anyway.
///
/// Falls back to the full `World::list_cells` scan, filtered the same way,
/// whenever `where_clause` is `None` or doesn't narrow at all.
fn matching_cells(
    world: &kblockdblib::World,
    range: Option<&query::Range>,
    where_clause: Option<&query::Expr>,
) -> std::io::Result<Vec<kblockdblib::CellEntry>> {
    if let Some(expr) = where_clause {
        if let Some(coords) =
            query::candidate_coords(expr, &|key, value| world.lookup_eq(key, value))
        {
            let mut cells = Vec::with_capacity(coords.len());
            for coord in &coords {
                if let Some(cell) = world.cell_entry_at(coord)? {
                    if query::matches(range, Some(expr), &cell) {
                        cells.push(cell);
                    }
                }
            }
            return Ok(cells);
        }
    }
    Ok(world
        .list_cells()?
        .into_iter()
        .filter(|c| query::matches(range, where_clause, c))
        .collect())
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
                .with_database(db, move |w| {
                    matching_cells(w, range.as_ref(), where_clause.as_ref())
                })
                .await?;
            match &columns {
                query::Columns::Aggregates(aggregates) => {
                    let refs: Vec<&kblockdblib::CellEntry> = cells.iter().collect();
                    Ok(QueryResponse::aggregates(query::aggregate(
                        aggregates, &refs,
                    )))
                }
                _ => {
                    let rows = cells.iter().map(|c| project_row(&columns, c)).collect();
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
            // Snapshotted here (via `matching_cells`, index-assisted when
            // possible -- see its doc comment), then mutated in a second
            // `with_database` call below -- not atomic with respect to a
            // concurrent writer touching the same cells in between (a
            // classic find-then-mutate race), same honest caveat as any
            // bulk operation built on a point-in-time scan rather than a
            // world-wide lock.
            let cells = state
                .with_database(db, move |w| {
                    matching_cells(w, range.as_ref(), where_clause.as_ref())
                })
                .await?;
            let matching: Vec<Vec<i32>> = cells.iter().map(|c| c.coord.to_vec()).collect();
            let affected = matching.len();
            let stamper = Stamper::new(&state.replication);
            let written = set_each(state, db, matching, assignments, &stamper).await?;
            for (coord, key, value, meta, stamp) in written {
                stamper.publish_set(db, &coord, &key, value, meta, stamp);
            }
            Ok(QueryResponse::affected(affected))
        }
        query::Statement::Delete {
            where_clause,
            range,
        } => {
            let cells = state
                .with_database(db, move |w| {
                    matching_cells(w, range.as_ref(), where_clause.as_ref())
                })
                .await?;
            let matching: Vec<(Vec<i32>, Vec<String>)> = cells
                .iter()
                .map(|c| {
                    (
                        c.coord.to_vec(),
                        c.values.iter().map(|(k, _, _)| k.clone()).collect(),
                    )
                })
                .collect();
            let affected = matching.len();
            let stamper = Stamper::new(&state.replication);
            let stamps = stamper.clone();
            let removed = state
                .with_database(db, move |w| {
                    let mut removed = Vec::new();
                    for (coord, keys) in matching {
                        for key in keys {
                            if let Some((at, stamp)) =
                                w.remove_stamped(&coord, &key, &mut || stamps.next())?
                            {
                                removed.push((coord.clone(), key, at, stamp));
                            }
                        }
                    }
                    Ok(removed)
                })
                .await?;
            for (coord, key, at, stamp) in &removed {
                stamper.publish_remove(db, coord, key, *at, *stamp);
            }
            Ok(QueryResponse::affected(affected))
        }
        query::Statement::CreateIndex { key } => {
            let key2 = key.clone();
            state.with_database(db, move |w| w.create_index(&key2)).await?;
            Stamper::new(&state.replication).publish_index_op(
                db,
                &key,
                kblockdbcluster::wire::IndexOp::Create,
            );
            Ok(QueryResponse::ok())
        }
        query::Statement::DropIndex { key } => {
            let lookup_key = key.clone();
            let dropped = state
                .with_database(db, move |w| Ok(w.drop_index(&lookup_key)))
                .await?;
            if dropped {
                Stamper::new(&state.replication).publish_index_op(
                    db,
                    &key,
                    kblockdbcluster::wire::IndexOp::Drop,
                );
                Ok(QueryResponse::ok())
            } else {
                Err(ApiError::NotFound(format!("no index on key '{key}'")))
            }
        }
        query::Statement::RebuildIndex { key } => {
            let key2 = key.clone();
            state
                .with_database(db, move |w| w.rebuild_index(&key2))
                .await?;
            Stamper::new(&state.replication).publish_index_op(
                db,
                &key,
                kblockdbcluster::wire::IndexOp::Rebuild,
            );
            Ok(QueryResponse::ok())
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
        let stamper = Stamper::new(&state.replication);
        let stamps = stamper.clone();
        let per_key_written = state
            .with_database(db, move |w| {
                let mut per_key_written = Vec::with_capacity(values.len());
                for (key, value) in &values {
                    per_key_written.push(w.set_region_stamped(
                        &region,
                        key,
                        &vec![value.clone(); volume],
                        &mut || stamps.next(),
                    )?);
                }
                Ok(per_key_written)
            })
            .await?;
        for ((key, value), written) in values2.into_iter().zip(per_key_written) {
            for (coord, (meta, stamp)) in region2.iter().zip(written) {
                stamper.publish_set(db, &coord, &key, value.clone(), meta, stamp);
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
    let stamper = Stamper::new(&state.replication);
    let written = set_each(state, db, targets, assignments, &stamper).await?;
    for (coord, key, value, meta, stamp) in written {
        stamper.publish_set(db, &coord, &key, value, meta, stamp);
    }
    Ok(QueryResponse::affected(affected))
}

/// One write of a query's `UPDATE`/`SET ... WHERE`: what was written where,
/// and the metadata and stamp it got.
type Written = (
    Vec<i32>,
    String,
    kblockdblib::Value,
    kblockdblib::CellMeta,
    kblockdblib::Stamp,
);

/// Writes every assignment at every coordinate in `coords`, stamping each
/// write from `stamper` -- shared by `UPDATE` and `SET ... WHERE`.
async fn set_each(
    state: &AppState,
    db: &str,
    coords: Vec<Vec<i32>>,
    assignments: Vec<(String, query::Literal)>,
    stamper: &Stamper,
) -> Result<Vec<Written>, ApiError> {
    let stamps = stamper.clone();
    state
        .with_database(db, move |w| {
            let mut written = Vec::new();
            for coord in &coords {
                for (key, literal) in &assignments {
                    let value = literal.to_value();
                    let (meta, stamp) =
                        w.set_stamped(coord, key, value.clone(), &mut || stamps.next())?;
                    written.push((coord.clone(), key.clone(), value, meta, stamp));
                }
            }
            Ok(written)
        })
        .await
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
        query::Statement::CreateIndex { .. }
        | query::Statement::DropIndex { .. }
        | query::Statement::RebuildIndex { .. } => None,
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
