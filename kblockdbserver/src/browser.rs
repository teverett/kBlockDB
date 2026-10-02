//! The data browser: one self-contained HTML/JS page (see `browser.html`)
//! at `/`, with a dropdown (populated from `GET /rest/databases`) to pick
//! which database to browse. Once one's selected, it lists every populated
//! cell in it, one row per cell, paged and searchable, sorted ascending by
//! coordinate, with a modal showing a cell's full keys/values/metadata on
//! click.
//!
//! Read-only (there's no way to edit anything from here), so it's mounted
//! behind the same auth as the REST API's `GET` endpoints -- a `read_only`
//! account can use it same as any other. Deliberately kept outside `/rest`
//! (see `routes.rs`): this isn't part of the versioned REST API surface
//! (it has no OpenAPI annotation, and isn't in `openapi.rs`'s spec), just a
//! convenience UI on top of the same data -- it even reuses `GET
//! /rest/databases` directly from its own JS rather than duplicating that
//! listing logic here.
//!
//! `GET /rows?db=<name>&...` backs the page: it calls
//! `kblockdblib::World::list_cells`, which -- like `World::stats` -- is a
//! live, full filesystem walk (here, a full *decode* of every chunk file,
//! heavier than `stats`' file-size-only walk), redone on every call rather
//! than cached. That's the right trade-off for a small-to-moderate database
//! browsed occasionally, not for one with millions of populated cells
//! polled repeatedly -- see `list_cells`'s own doc comment.
//!
//! `POST /query` backs the query box above the toolbar: it reuses
//! `routes::execute_query` (the same evaluator `POST /rest/db/{db}/query`
//! runs), but refuses `SET`/`UPDATE`/`DELETE` unconditionally -- even for a
//! full-access account -- since this page's whole premise is that there's
//! no way to edit anything from here (see `run_select`).
//!
//! `browser.css` (served at `GET /browser.css`) is this page's styling,
//! kept in its own file rather than inlined in `browser.html` so it reads
//! and edits like an ordinary stylesheet.

use crate::auth::require_auth;
use crate::error::ApiError;
use crate::state::{Account, AppState};
use crate::value_json::ValueJson;
use axum::extract::{Query, State};
use axum::http::header;
use axum::middleware;
use axum::response::Html;
use axum::routing::{get, post};
use axum::{Json, Router};
use kblockdblib::{CellEntry, Value};
use serde::{Deserialize, Serialize};

const PAGE_HTML: &str = include_str!("browser.html");
const PAGE_CSS: &str = include_str!("browser.css");

/// A `Router<AppState>`, not yet given its state -- merged into
/// `routes::router`'s own router before that calls `.with_state`, same
/// pattern as `routes.rs`'s own `protected` sub-router.
pub fn router(state: AppState) -> Router<AppState> {
    let protected = Router::new()
        .route("/", get(page))
        .route("/browser.css", get(css))
        .route("/rows", get(rows))
        .route_layer(middleware::from_fn_with_state(state, require_auth));

    // Not under `protected`: `require_auth`'s read_only check is purely by
    // HTTP method (any non-`GET` is a write), but this is a `POST` that's
    // never a write -- `run_select` enforces its own "SELECT only, no
    // exceptions" rule after parsing, the same reason `routes.rs`'s own
    // `/db/{db}/query` needs its own auth check instead of `require_auth`'s
    // (see that module's doc comment).
    let query_route = Router::new().route("/query", post(run_select));

    protected.merge(query_route)
}

async fn page() -> Html<&'static str> {
    Html(PAGE_HTML)
}

async fn css() -> impl axum::response::IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/css; charset=utf-8")],
        PAGE_CSS,
    )
}

fn default_page() -> usize {
    1
}

fn default_page_size() -> usize {
    50
}

/// Pages beyond this are refused with `400` rather than silently clamped --
/// see `rows`'s own comment on why capping (rather than allowing e.g.
/// `page_size=1000000`) matters here, given `list_cells`' cost.
const MAX_PAGE_SIZE: usize = 500;

#[derive(Deserialize)]
struct RowsQuery {
    /// Which database to list rows from -- required, no default, matching
    /// the rest of the server's "every request names its database
    /// explicitly" rule. The page's own JS always sends this once it's
    /// populated its database dropdown from `GET /rest/databases`; a
    /// request missing it is almost certainly a stale/hand-built URL, not a
    /// legitimate use of this endpoint, so it's rejected by the `Query`
    /// extractor itself (a plain `400`) rather than treated as "no
    /// database" some other way.
    db: String,
    #[serde(default = "default_page")]
    page: usize,
    #[serde(default = "default_page_size")]
    page_size: usize,
    #[serde(default)]
    search: String,
}

#[derive(Serialize)]
struct KeyEntry {
    key: String,
    value: ValueJson,
    created_at_ms: u64,
    modified_at_ms: u64,
    version: u64,
}

#[derive(Serialize)]
struct RowEntry {
    coord: Vec<i32>,
    key_count: usize,
    /// The earliest `created_at_ms` among this cell's keys -- when this
    /// cell was first touched at all.
    created_at_ms: u64,
    /// The latest `modified_at_ms` among this cell's keys -- when this
    /// cell was last changed, by any key.
    modified_at_ms: u64,
    /// Every key set at this cell, sorted by key name (see
    /// `kblockdblib::CellEntry`) -- included here (rather than behind a
    /// second, per-cell endpoint) so the page's modal has everything it
    /// needs already in hand from the same response that populated the
    /// row, with no extra round trip.
    keys: Vec<KeyEntry>,
}

#[derive(Serialize)]
struct RowsResponse {
    page: usize,
    page_size: usize,
    total_rows: usize,
    total_pages: usize,
    rows: Vec<RowEntry>,
}

async fn rows(
    State(state): State<AppState>,
    Query(query): Query<RowsQuery>,
) -> Result<Json<RowsResponse>, ApiError> {
    if query.page == 0 {
        return Err(ApiError::BadRequest("page must be at least 1".to_string()));
    }
    if query.page_size == 0 || query.page_size > MAX_PAGE_SIZE {
        return Err(ApiError::BadRequest(format!(
            "page_size must be between 1 and {MAX_PAGE_SIZE}"
        )));
    }
    let search = query.search.trim().to_lowercase();

    let cells = state
        .with_database(&query.db, kblockdblib::World::list_cells)
        .await?;
    let filtered: Vec<&CellEntry> = cells
        .iter()
        .filter(|cell| search.is_empty() || cell_matches(cell, &search))
        .collect();

    let total_rows = filtered.len();
    let total_pages = total_rows.div_ceil(query.page_size).max(1);
    let start = (query.page - 1) * query.page_size;
    let rows = filtered
        .into_iter()
        .skip(start)
        .take(query.page_size)
        .map(to_row_entry)
        .collect();

    Ok(Json(RowsResponse {
        page: query.page,
        page_size: query.page_size,
        total_rows,
        total_pages,
        rows,
    }))
}

#[derive(Deserialize)]
struct QueryBody {
    db: String,
    query: String,
}

/// Runs a `SELECT` from the browser's own query box, reusing
/// `routes::execute_query` -- but unlike `POST /rest/db/{db}/query`,
/// `SET`/`UPDATE`/`DELETE` are refused unconditionally here, regardless
/// of the account's own `read_only` flag: this module's whole premise is
/// "there's no way to edit anything from here" (see this module's doc
/// comment), so a write must never reach `execute_query` through this
/// path even for a full-access account. The `read_only: true` synthetic
/// `Account` passed to `execute_query` is belt-and-suspenders -- its own
/// `stmt.is_write() && account.read_only` check would also catch it if
/// the `is_write` check just below this were ever accidentally removed.
///
/// Authenticates itself (`account_from_headers`, not the `require_auth`
/// middleware `router`'s other routes use) for the same reason
/// `routes.rs`'s own query handler does: `require_auth`'s read_only check
/// is purely by HTTP method, and this is a `POST` that's never actually a
/// write, so a read-only account must not be blocked from it by method
/// alone (see `routes.rs`'s doc comment).
async fn run_select(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Json(body): Json<QueryBody>,
) -> axum::response::Response {
    use axum::response::IntoResponse;

    if crate::auth::account_from_headers(&headers, &state).is_none() {
        return crate::auth::unauthorized_response();
    }
    let stmt = match kblockdbquery::parse(&body.query) {
        Ok(stmt) => stmt,
        Err(e) => return ApiError::BadRequest(format!("invalid query: {e}")).into_response(),
    };
    if stmt.is_write() {
        return ApiError::Forbidden(
            "the data browser is read-only -- only SELECT is allowed here".to_string(),
        )
        .into_response();
    }
    let read_only_account = Account {
        password: String::new(),
        read_only: true,
    };
    match crate::routes::execute_query(&state, &body.db, &read_only_account, &body.query).await {
        Ok(resp) => Json(resp).into_response(),
        Err(e) => e.into_response(),
    }
}

/// Whether `cell` matches `search` (already trimmed/lowercased) against its
/// coordinate (comma-joined, e.g. `"1,2,3"`), any key name, or any value's
/// rendered form.
fn cell_matches(cell: &CellEntry, search: &str) -> bool {
    let coord_str = cell
        .coord
        .iter()
        .map(i32::to_string)
        .collect::<Vec<_>>()
        .join(",");
    if coord_str.contains(search) {
        return true;
    }
    cell.values.iter().any(|(key, value, _)| {
        key.to_lowercase().contains(search) || value_text(value).to_lowercase().contains(search)
    })
}

fn value_text(value: &Value) -> String {
    match value {
        Value::Str(s) => s.clone(),
        Value::F64(x) => x.to_string(),
        Value::I64(x) => x.to_string(),
        Value::Bool(b) => b.to_string(),
    }
}

fn to_row_entry(cell: &CellEntry) -> RowEntry {
    let keys: Vec<KeyEntry> = cell
        .values
        .iter()
        .map(|(key, value, meta)| KeyEntry {
            key: key.clone(),
            value: ValueJson::from(value.clone()),
            created_at_ms: meta.created_at_ms,
            modified_at_ms: meta.modified_at_ms,
            version: meta.version,
        })
        .collect();
    let created_at_ms = keys.iter().map(|k| k.created_at_ms).min().unwrap_or(0);
    let modified_at_ms = keys.iter().map(|k| k.modified_at_ms).max().unwrap_or(0);
    RowEntry {
        coord: cell.coord.to_vec(),
        key_count: keys.len(),
        created_at_ms,
        modified_at_ms,
        keys,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kblockdblib::CellMeta;

    fn cell(coord: &[i32], values: Vec<(&str, Value)>) -> CellEntry {
        CellEntry {
            coord: coord.into(),
            values: values
                .into_iter()
                .map(|(k, v)| {
                    (
                        k.to_string(),
                        v,
                        CellMeta {
                            created_at_ms: 1000,
                            modified_at_ms: 1000,
                            version: 0,
                        },
                    )
                })
                .collect(),
        }
    }

    #[test]
    fn cell_matches_by_coordinate_substring() {
        let c = cell(&[1, 2, 3], vec![("k", Value::I64(1))]);
        assert!(cell_matches(&c, "1,2,3"));
        assert!(cell_matches(&c, "2,3"));
        assert!(!cell_matches(&c, "9,9,9"));
    }

    #[test]
    fn cell_matches_by_negative_coordinate_substring() {
        let c = cell(&[-1, 2, -3], vec![("k", Value::I64(1))]);
        assert!(cell_matches(&c, "-1,2,-3"));
        assert!(cell_matches(&c, "-3"));
        assert!(!cell_matches(&c, "9,9,9"));
    }

    #[test]
    fn cell_matches_by_key_name_case_insensitively() {
        let c = cell(&[0, 0, 0], vec![("Material", Value::Str("stone".into()))]);
        assert!(cell_matches(&c, "material"));
        assert!(cell_matches(&c, "MATERIAL".to_lowercase().as_str()));
    }

    #[test]
    fn cell_matches_by_value_text() {
        let c = cell(&[0, 0, 0], vec![("material", Value::Str("stone".into()))]);
        assert!(cell_matches(&c, "sto"));
        assert!(!cell_matches(&c, "granite"));

        let n = cell(&[0, 0, 0], vec![("hardness", Value::I64(42))]);
        assert!(cell_matches(&n, "42"));

        let b = cell(&[0, 0, 0], vec![("flammable", Value::Bool(true))]);
        assert!(cell_matches(&b, "true"));
        assert!(!cell_matches(&b, "false"));
    }

    #[test]
    fn to_row_entry_aggregates_min_created_and_max_modified() {
        let c = CellEntry {
            coord: [1, 1, 1].into(),
            values: vec![
                (
                    "a".to_string(),
                    Value::I64(1),
                    CellMeta {
                        created_at_ms: 2000,
                        modified_at_ms: 2000,
                        version: 0,
                    },
                ),
                (
                    "b".to_string(),
                    Value::I64(2),
                    CellMeta {
                        created_at_ms: 1000,
                        modified_at_ms: 3000,
                        version: 1,
                    },
                ),
            ],
        };
        let row = to_row_entry(&c);
        assert_eq!(row.key_count, 2);
        assert_eq!(row.created_at_ms, 1000);
        assert_eq!(row.modified_at_ms, 3000);
    }
}
