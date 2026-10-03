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
}
