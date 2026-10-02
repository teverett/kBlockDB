//! Server-to-server replication for kBlockDB: each server can list peers
//! and ship its own local writes to every one of them, so a small
//! cluster converges on the same data over time. See docs/clustering.md
//! (at the repository root) for the feature as a whole, config shape,
//! and known limitations.
//!
//! This crate is storage-agnostic -- it knows nothing about
//! `kblockdblib::World`, databases, or accounts. The accepting side
//! ([`server::serve`]) applies each incoming change through a
//! [`server::ReplicationSink`] the embedder provides; `kblockdbserver` is
//! the one embedder today, implementing that trait for its own
//! `AppState`. This keeps the two crates decoupled in the direction that
//! matters: `kblockdbserver` depends on this crate, never the other way
//! around (so this crate's own tests use a trivial in-memory
//! [`ReplicationSink`](server::ReplicationSink), see `test_support.rs`,
//! rather than any real storage).
//!
//! - `wire` -- the peer protocol's wire format: frame I/O, `Hello`/
//!   `HelloOk` version/secret negotiation, `ChangeBatch` encode/decode.
//! - `hub` -- `ReplicationHub`, the in-process point an embedder's local
//!   write publishes a `ChangeEntry` to.
//! - `client` -- the connecting side of a peer link: one long-running,
//!   auto-reconnecting task per configured peer that drains a
//!   `ReplicationHub` and streams it to that peer.
//! - `server` -- the accepting side: the peer protocol's TCP listener,
//!   per-connection `Hello` authentication, and applying every incoming
//!   `ChangeEntry` via a `ReplicationSink`.

pub mod client;
pub mod hub;
pub mod server;
pub mod wire;

#[cfg(test)]
mod test_support;
