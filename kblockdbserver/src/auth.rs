//! HTTP Basic Auth middleware: applied to every route except `/health`
//! (see `routes.rs`), so a request without a matching username/password
//! from `AppState::authenticate` never reaches a handler -- and a
//! read-only account's write (anything but `GET`) never does either.

use crate::error::ApiError;
use crate::state::AppState;
use axum::extract::{Request, State};
use axum::http::{header, Method};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum_extra::headers::authorization::Basic;
use axum_extra::headers::{Authorization, HeaderMapExt};

pub async fn require_auth(State(state): State<AppState>, req: Request, next: Next) -> Response {
    let account = req
        .headers()
        .typed_get::<Authorization<Basic>>()
        .and_then(|auth| state.authenticate(auth.username(), auth.password()));

    let Some(account) = account else {
        return unauthorized_response();
    };

    if account.read_only && *req.method() != Method::GET {
        return ApiError::Forbidden("this account is read-only".to_string()).into_response();
    }

    next.run(req).await
}

fn unauthorized_response() -> Response {
    let mut resp = ApiError::Unauthorized("authentication required".to_string()).into_response();
    // Standard signal to a browser/curl that this 401 wants Basic Auth
    // credentials, not some other authorization scheme.
    resp.headers_mut().insert(
        header::WWW_AUTHENTICATE,
        header::HeaderValue::from_static(r#"Basic realm="kblockdbserver""#),
    );
    resp
}
