//! The read side an embedder provides for catch-up -- the counterpart of
//! `server::ReplicationSink`, and storage-agnostic for the same reason.
//! When a peer asks for everything its version vector lacks (a
//! `CatchUpRequest`, see `peers.rs`), the link to it calls
//! [`ChangeSource::changes_since`] on a blocking thread and streams what
//! it yields as ordinary `ChangeBatch` frames.

use crate::wire::ChangeEntry;
use kblockdblib::VersionVector;

pub trait ChangeSource: Send + Sync + 'static {
    /// Every value and removal this process holds -- across every
    /// database, whoever originally made it -- whose stamp `known` doesn't
    /// cover, handed to `emit` in batches of any size. Legacy
    /// (`Stamp::NONE`) data counts as written by `legacy_origin` (see
    /// `kblockdblib::legacy_origin`). Stops early, returning `Ok`, when
    /// `emit` returns `false` (the link went away).
    ///
    /// Blocking: always called from `tokio::task::spawn_blocking`.
    fn changes_since(
        &self,
        known: &VersionVector,
        legacy_origin: u64,
        emit: &mut dyn FnMut(Vec<ChangeEntry>) -> bool,
    ) -> Result<(), String>;

    /// Every `(database, key)` pair this process currently has a
    /// secondary index built on (see `kblockdblib::World::indexed_keys`).
    /// Sent once per connection as a `wire::PeerMessage::IndexState`,
    /// right after the initial `PeerList` gossip -- see `client.rs`'s
    /// `connect_and_forward` -- so a peer that missed earlier live
    /// `IndexOp`s (it was down, or this is the first time it's ever
    /// linked to the sender) still ends up with the same `CREATE INDEX`/
    /// `REBUILD INDEX`s already applied elsewhere, instead of only ever
    /// learning about one it happened to be connected for. See
    /// `wire::IndexOpEntry`'s doc comment on why this is still one-way
    /// (creates, never drops) and not tracked by any version vector.
    ///
    /// Blocking, same as `changes_since`, though in practice this is
    /// cheap (reading an in-memory index registry, not scanning data).
    /// The default returns nothing: an embedder with no concept of a
    /// secondary index (or that doesn't want to replicate this) simply
    /// never sends a reconciliation.
    fn indexed_keys(&self) -> Result<Vec<(String, String)>, String> {
        Ok(Vec::new())
    }
}
