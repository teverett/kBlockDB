//! The data browser: one self-contained HTML/JS page (see `browser.html`)
//! at `/`, with a dropdown (populated from `GET /rest/databases`) to pick
//! which database to browse, and a query box that's the page's *only* way
//! to populate the table below -- there's no separate plain-listing
//! endpoint; every row shown is the result of running a `SELECT` (see
//! `POST /query` below). Clicking a row opens a modal with that cell's
//! full keys, values, and metadata.
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
//! `POST /query` backs the query box above the table: it reuses
//! `routes::execute_query` (the same evaluator `POST /rest/db/{db}/query`
//! runs), but refuses `SET`/`UPDATE`/`DELETE` unconditionally -- even for a
//! full-access account -- since this page's whole premise is that there's
//! no way to edit anything from here (see `run_select`). The page defaults
//! the box to `SELECT *` and runs it immediately on load/database change,
//! so there's always something to show without the user having to type
//! anything first. Paging past the first page is purely client-side (the
//! query itself is never paginated server-side), which is the right
//! trade-off for a small-to-moderate database browsed occasionally, not
//! for one with millions of cells matching a broad query.
//!
//! `browser.css` (served at `GET /browser.css`) is this page's styling,
//! kept in its own file rather than inlined in `browser.html` so it reads
//! and edits like an ordinary stylesheet.

use crate::auth::require_auth;
use crate::error::ApiError;
use crate::state::{Account, AppState};
use axum::extract::State;
use axum::http::header;
use axum::middleware;
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;

const PAGE_HTML: &str = include_str!("browser.html");
const PAGE_CSS: &str = include_str!("browser.css");

/// A `Router<AppState>`, not yet given its state -- merged into
/// `routes::router`'s own router before that calls `.with_state`, same
/// pattern as `routes.rs`'s own `protected` sub-router.
pub fn router(state: AppState) -> Router<AppState> {
    let protected = Router::new()
        .route("/", get(page))
        .route("/browser.css", get(css))
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

async fn css() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/css; charset=utf-8")],
        PAGE_CSS,
    )
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
) -> Response {
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
