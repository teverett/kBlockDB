//! kBlockDB's binary protocol wire format, published as a library so a
//! client can share the exact same encode/decode logic the
//! `kblockdbserver` binary itself uses instead of reimplementing it from
//! its doc comments and risking drift -- see `wire.rs` for the format
//! itself and `kblockdbperf/src/binary_client.rs` (currently the only
//! client) for something that uses it.
//!
//! Everything else about this server -- auth, config, the REST API, the
//! binary listener itself -- stays private to the `kblockdbserver`
//! binary; this library exists for the wire format alone, not as a
//! general-purpose SDK.

pub mod wire;
