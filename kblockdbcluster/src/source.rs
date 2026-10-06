//! The read side an embedder provides for catch-up -- the counterpart of
//! `server::ReplicationSink`, and storage-agnostic for the same reason.
//! When a peer asks for everything its version vector lacks (a
//! `CatchUpRequest`, see `peers.rs`), the link to it calls
//! [`ChangeSource::changes_since`] on a blocking thread and streams what
//! it yields as ordinary `ChangeBatch` frames.

use crate::wire::{ChangeEntry, DatabaseSync};
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

    /// This process's current fingerprint for every database it
    /// maintains a content digest for (see
    /// `kblockdblib::World::with_content_digest`/`content_digest`) --
    /// sent periodically (every 60s, its own cadence, independent of
    /// catch-up/`IndexState`) as a `wire::PeerMessage::SyncReport`, so
    /// the periodic cluster sync check (`docs/clustering.md`) can answer
    /// "does this node actually hold the same data as its peers," not
    /// just "has it seen the same number of writes" (what equal version
    /// vectors already show).
    ///
    /// A database with no digest enabled simply isn't included -- this
    /// is a report of what *can* be compared, not a claim about every
    /// database this process happens to hold. Blocking, same as
    /// `changes_since`/`indexed_keys`. The default reports nothing: an
    /// embedder with no concept of a content digest never sends one.
    fn sync_state(&self) -> Result<Vec<DatabaseSync>, String> {
        Ok(Vec::new())
    }

    /// This process's `kblockdblib::World::chunk_digests()` for `database`,
    /// as `(chunk key, digest)` pairs -- sent in reply to a
    /// `wire::PeerMessage::ChunkDigestsRequest`, itself only ever sent
    /// after a `SyncReport` already showed a whole-database mismatch (see
    /// `server::ReplicationSink::apply_sync_report`'s return value). Unlike
    /// `sync_state`, this isn't periodic and isn't filtered by "do I have
    /// a digest enabled" -- the request already implies the asker wants
    /// to localize a mismatch it already knows is there.
    ///
    /// Blocking, same as the rest of this trait. The default reports
    /// nothing: an embedder with no concept of a chunk digest (or no such
    /// database) never answers.
    fn chunk_digests(&self, database: &str) -> Result<Vec<(Vec<i32>, u64)>, String> {
        let _ = database;
        Ok(Vec::new())
    }
}
