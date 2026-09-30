//! HTTP Basic Auth middleware: applied to every route except `/health`
//! (see `routes.rs`), so a request without a matching username/password
//! from `AppState::authenticate` never reaches a handler -- and a
//! read-only account's write (anything but `GET`) never does either.

use crate::error::ApiError;
use crate::state::{Account, AppState};
use axum::extract::{Request, State};
use axum::http::{header, HeaderMap, Method};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum_extra::headers::authorization::Basic;
use axum_extra::headers::{Authorization, HeaderMapExt};

/// The account `headers` authenticates as, if any -- the same Basic Auth
/// check `require_auth` applies to every `/rest` route, factored out so
/// `routes.rs`'s query handler (which can't use `require_auth` itself; see
/// its own doc comment on why) can run the identical check.
pub fn account_from_headers(headers: &HeaderMap, state: &AppState) -> Option<Account> {
    headers
        .typed_get::<Authorization<Basic>>()
        .and_then(|auth| state.authenticate(auth.username(), auth.password()))
}

pub async fn require_auth(State(state): State<AppState>, req: Request, next: Next) -> Response {
    let Some(account) = account_from_headers(req.headers(), &state) else {
        return unauthorized_response();
    };

    if account.read_only && *req.method() != Method::GET {
        return ApiError::Forbidden("this account is read-only".to_string()).into_response();
    }

    next.run(req).await
}

/// A `401` with the `WWW-Authenticate` header a browser/curl needs to know
/// this wants Basic Auth credentials -- `pub(crate)` so the query handler
/// (see `account_from_headers`'s doc comment) gets the exact same response
/// shape a `require_auth`-gated route would.
pub(crate) fn unauthorized_response() -> Response {
    let mut resp = ApiError::Unauthorized("authentication required".to_string()).into_response();
    // Standard signal to a browser/curl that this 401 wants Basic Auth
    // credentials, not some other authorization scheme.
    resp.headers_mut().insert(
        header::WWW_AUTHENTICATE,
        header::HeaderValue::from_static(r#"Basic realm="kblockdbserver""#),
    );
    resp
}
