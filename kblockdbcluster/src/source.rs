//! The read side an embedder provides for catch-up -- the counterpart of
//! `server::ReplicationSink`, and storage-agnostic for the same reason.
//! When a peer asks for everything changed since its watermark (a
//! `CatchUpRequest`, see `peers.rs`), the link to it calls
//! [`ChangeSource::changes_since`] on a blocking thread and streams what
//! it yields as ordinary `ChangeBatch` frames.

use crate::wire::ChangeEntry;

pub trait ChangeSource: Send + Sync + 'static {
    /// Every change this process holds -- across every database, whoever
    /// originally made it -- with `modified_at_ms` (or, for a removal,
    /// the time it was removed) strictly after `since_ms`, handed to
    /// `emit` in batches of any size. Stops early, returning `Ok`, when
    /// `emit` returns `false` (the link went away).
    ///
    /// Blocking: always called from `tokio::task::spawn_blocking`.
    fn changes_since(
        &self,
        since_ms: u64,
        emit: &mut dyn FnMut(Vec<ChangeEntry>) -> bool,
    ) -> Result<(), String>;
}
