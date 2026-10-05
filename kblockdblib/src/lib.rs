//! `kblockdblib`: a prototype storage engine for a huge simulation grid where every
//! cell is its own key/value store, chunked and stored columnarly on disk.
//! See `README.md` for the design.
//!
//! This crate is the storage engine only -- no networking, no server. It's
//! meant to be embedded: `kblockdbserver` (a sibling crate in this workspace)
//! wraps a [`World`] in a REST API. The most-used items are re-exported at
//! the crate root; the individual modules are public too for anything more
//! specific (e.g. `kblockdblib::chunk::chunk_cells`).

pub mod chunk;
mod chunk_cache;
pub mod coord;
pub mod index;
pub mod logger;
mod lsm;
pub mod params;
pub mod schema;
mod semaphore;
pub mod stamp;
pub mod value;
pub mod world;

pub use chunk::{CellMeta, ChangeKind};
pub use coord::Coord;
pub use params::WorldParams;
pub use schema::ColumnInfo;
pub use stamp::{legacy_origin, Stamp, VersionVector, LEGACY_BIT};
pub use value::{Value, ValueType};
pub use world::{
    CellEntry, Change, Region, RemovedCells, Stats, World, AXES, DEFAULT_CHUNK_DIM,
    DEFAULT_MAX_CACHED_CHUNKS, DEFAULT_MAX_CONCURRENT_DISK_OPS, WORLD_DIM,
};
