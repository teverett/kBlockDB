//! Shared server state: the one `World` this server process manages.

use crate::error::ApiError;
use kdb::World;
use std::sync::Arc;

/// No `Mutex` here on purpose. `World`'s own methods take `&self` and are
/// safe to call concurrently -- its only interior state is a locked
/// `Schema` and a couple of atomic counters, and the actual per-chunk
/// concurrency safety comes from OS-level file locks in `with_chunk` (see
/// `kdb`'s "Concurrency" doc comment on `World`), the same mechanism that
/// already makes it safe for *separate processes* to share a world. A
/// `Mutex<World>` here would serialize every request through one lock
/// regardless of which chunk it touched, throwing that away -- two
/// requests to unrelated cells would contend for no reason.
#[derive(Clone)]
pub struct AppState {
    pub world: Arc<World>,
}

impl AppState {
    pub fn new(world: World) -> Self {
        AppState {
            world: Arc::new(world),
        }
    }

    /// Runs `f` against the shared `World` on a blocking-task thread pool
    /// thread, not on the async runtime's own worker threads.
    ///
    /// `World`'s operations do synchronous file I/O (`std::fs`); running
    /// that directly inside an `async fn` handler would block whichever
    /// tokio worker thread happened to be running it, stalling every other
    /// request that worker was multiplexing. `spawn_blocking` moves it to a
    /// thread pool meant for exactly this -- and since `World` needs no
    /// external lock, many of these can run at once.
    pub async fn with_world<T, F>(&self, f: F) -> Result<T, ApiError>
    where
        T: Send + 'static,
        F: FnOnce(&World) -> std::io::Result<T> + Send + 'static,
    {
        let world = self.world.clone();
        tokio::task::spawn_blocking(move || f(&world))
            .await
            .expect("worker thread panicked")
            .map_err(ApiError::from)
    }
}
