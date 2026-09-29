//! Shared server state: the one `World` this server process manages, behind
//! a lock so concurrent requests serialize their access to it.

use crate::error::ApiError;
use kdb::World;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct AppState {
    pub world: Arc<Mutex<World>>,
}

impl AppState {
    pub fn new(world: World) -> Self {
        AppState {
            world: Arc::new(Mutex::new(world)),
        }
    }

    /// Runs `f` against the shared `World` on a blocking-task thread pool
    /// thread, not on the async runtime's own worker threads.
    ///
    /// `World`'s operations do synchronous file I/O (`std::fs`); running
    /// that directly inside an `async fn` handler would block whichever
    /// tokio worker thread happened to be running it, stalling every other
    /// request that worker was multiplexing. `spawn_blocking` moves it to a
    /// thread pool meant for exactly this.
    pub async fn with_world<T, F>(&self, f: F) -> Result<T, ApiError>
    where
        T: Send + 'static,
        F: FnOnce(&mut World) -> std::io::Result<T> + Send + 'static,
    {
        let world = self.world.clone();
        tokio::task::spawn_blocking(move || {
            let mut w = world
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            f(&mut w)
        })
        .await
        .expect("worker thread panicked")
        .map_err(ApiError::from)
    }
}
