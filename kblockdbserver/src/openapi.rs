//! The OpenAPI spec itself: which paths and schemas it's built from, and
//! the one thing `#[utoipa::path(...)]` annotations can't express inline
//! (the `basic_auth` security scheme every non-`/health` path references).
//!
//! Adding a route means adding it to `paths(...)` below too -- easy to
//! forget, but `cargo build` catches it: `paths` names the annotated
//! handler functions directly, so a typo or omission is a compile error,
//! not a spec that silently omits or misdescribes a real endpoint.

use crate::error::ErrorBody;
use crate::routes::{
    self, CellResponse, HealthResponse, RegionValuesResponse, SetRegionBody, StatsResponse,
};
use crate::value_json::ValueJson;
use utoipa::openapi::security::{HttpAuthScheme, HttpBuilder, SecurityScheme};
use utoipa::{Modify, OpenApi};

#[derive(OpenApi)]
#[openapi(
    info(
        title = "kblockdbserver",
        description = "A RESTful HTTP front end for the kblockdblib storage engine. \
                       Every path except /health requires HTTP Basic Auth \
                       against an account from kblockdbserver's config file.",
    ),
    paths(
        routes::health,
        routes::stats,
        routes::get_cell,
        routes::set_cell,
        routes::remove_cell,
        routes::get_region,
        routes::set_region,
        routes::remove_region,
    ),
    components(schemas(
        HealthResponse,
        StatsResponse,
        CellResponse,
        RegionValuesResponse,
        SetRegionBody,
        ValueJson,
        ErrorBody,
    )),
    tags(
        (name = "health", description = "Liveness -- unauthenticated"),
        (name = "stats", description = "On-disk statistics for the world's data"),
        (name = "cells", description = "Single-cell reads and writes"),
        (name = "regions", description = "Axis-aligned box-of-cells reads and writes"),
    ),
    modifiers(&SecurityAddon),
)]
pub struct ApiDoc;

struct SecurityAddon;

impl Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        let components = openapi
            .components
            .as_mut()
            .expect("ApiDoc declares components(schemas(...)), so this is always Some");
        components.add_security_scheme(
            "basic_auth",
            SecurityScheme::Http(HttpBuilder::new().scheme(HttpAuthScheme::Basic).build()),
        );
    }
}
