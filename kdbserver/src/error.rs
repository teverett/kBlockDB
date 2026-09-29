//! The one error type every handler returns, and how it becomes an HTTP
//! response. Keeps status-code choices in one place instead of scattered
//! across handlers.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;
use std::io;

#[derive(Debug)]
pub enum ApiError {
    /// The request itself is malformed: an unparseable coordinate, a
    /// region whose axis count doesn't match the world's, a coordinate
    /// out of world bounds, a `values` array of the wrong length, a
    /// malformed JSON body. Maps to `kdb`'s own `InvalidInput` errors too
    /// -- `World`'s region/coordinate validation surfaces exactly these
    /// caller-error cases.
    BadRequest(String),
    /// A `get` found nothing for that key at that cell.
    NotFound(String),
    /// Something went wrong on this end (disk I/O, a corrupt file, ...)
    /// that the caller couldn't have prevented by sending a different
    /// request.
    Internal(String),
    /// No credentials, or credentials that don't match any configured
    /// account. See `auth.rs`.
    Unauthorized(String),
    /// Valid credentials, but for a read-only account attempting a write.
    /// See `auth.rs`.
    Forbidden(String),
}

impl From<io::Error> for ApiError {
    fn from(e: io::Error) -> Self {
        match e.kind() {
            io::ErrorKind::InvalidInput => ApiError::BadRequest(e.to_string()),
            io::ErrorKind::NotFound => ApiError::NotFound(e.to_string()),
            _ => ApiError::Internal(e.to_string()),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            ApiError::BadRequest(m) => (StatusCode::BAD_REQUEST, m),
            ApiError::NotFound(m) => (StatusCode::NOT_FOUND, m),
            ApiError::Internal(m) => (StatusCode::INTERNAL_SERVER_ERROR, m),
            ApiError::Unauthorized(m) => (StatusCode::UNAUTHORIZED, m),
            ApiError::Forbidden(m) => (StatusCode::FORBIDDEN, m),
        };
        (status, Json(json!({ "error": message }))).into_response()
    }
}
