use crate::chunk::{self, Chunk, CHUNK_DIM};
pub use crate::coord::Coord;
use crate::params::WorldParams;
use crate::schema::Schema;
use crate::value::Value;
use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter};
use std::path::{Path, PathBuf};

/// Default number of spatial axes for a *new* world, i.e. what `main`'s demo
/// calls `World::create` with. This is only a default: a world's real axis
/// count is a per-world runtime value fixed at `create()` time and
/// persisted in `world.txt` (see `params::WorldParams`), not a compile-time
/// constant baked into the binary -- `World::open` reads it from disk, so
/// changing this constant later doesn't (and can't) alter any world that
/// already exists on disk.
pub const AXES: usize = 3;

/// Default cells per axis for a *new* world -- see `AXES`'s doc comment;
/// the same "default at create time, persisted and authoritative
/// afterward" rule applies.
pub const WORLD_DIM: u32 = 10_000;

const _: () = assert!(AXES >= 1, "AXES must be at least 1");

type ChunkKey = Coord;

/// An axis-aligned box of cells: `origin` plus `extent`, i.e. the set of
/// coordinates `c` with `origin[a] <= c[a] < origin[a] + extent[a]` on
/// every axis `a`. `origin.len()` and `extent.len()` must both equal the
/// region's axis count. Bundles what would otherwise be `2*axes` separate
/// parameters on every `*_region` method, and may span or partially cover
/// any number of chunks -- see `World::get_region`/`set_region`/
/// `remove_region`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Region {
    pub origin: Coord,
    pub extent: Coord,
}

impl Region {
    /// Builds a `Region` from any origin/extent that convert to `Coord`.
    /// Doesn't itself require `origin.len() == extent.len()` -- `Region` is
    /// a plain data holder, and this crate is a library other code (e.g. a
    /// server parsing coordinates out of a URL) can call with attacker- or
    /// user-controlled input, so it's not this constructor's place to
    /// `panic!`/`debug_assert!` on a shape mismatch. `World::check_region`
    /// (run by every `*_region` method before touching any data) is what
    /// actually validates a `Region` and reports a mismatch as a proper
    /// `io::Result` error instead.
    pub fn new(origin: impl Into<Coord>, extent: impl Into<Coord>) -> Self {
        Region {
            origin: origin.into(),
            extent: extent.into(),
        }
    }

    pub fn volume(&self) -> u64 {
        self.extent.iter().map(|&d| u64::from(d)).product()
    }

    fn axes(&self) -> usize {
        self.origin.len()
    }
}

/// Iterates every coordinate inside a `Region`, axis 0 fastest and the last
/// axis slowest -- the generalization of an x-fastest/y/z convention to an
/// arbitrary number of axes. `get_region`'s result and `set_region`'s
/// expected `values` are in exactly this order.
struct RegionIter {
    origin: Coord,
    extent: Coord,
    cursor: Coord,
    done: bool,
}

impl RegionIter {
    fn new(region: &Region) -> Self {
        RegionIter {
            origin: region.origin.clone(),
            done: region.extent.contains(&0), // any zero-length axis makes the whole region empty
            extent: region.extent.clone(),
            cursor: Coord::zeros(region.axes()),
        }
    }
}

impl Iterator for RegionIter {
    type Item = Coord;

    fn next(&mut self) -> Option<Coord> {
        if self.done {
            return None;
        }
        let coord: Coord = self
            .origin
            .iter()
            .zip(self.cursor.iter())
            .map(|(&o, &c)| o + c)
            .collect();

        // Advance like an odometer: axis 0 rolls over fastest, and once the
        // last axis rolls over every coordinate has been visited.
        let last = self.cursor.len() - 1;
        for (a, cur) in self.cursor.iter_mut().enumerate() {
            *cur += 1;
            if *cur < self.extent[a] {
                break;
            }
            *cur = 0;
            if a == last {
                self.done = true;
            }
        }
        Some(coord)
    }
}

/// The on-disk, chunked, columnar key-value world.
///
/// Layout on disk under `root`:
///   root/world.txt                  -- this world's axes/world_dim (see params.rs)
///   root/schema.txt                 -- key string <-> id registry (see schema.rs)
///   root/<c0>/<c1>/.../<c_n-1>.chunk -- one file per non-empty chunk,
///                                        nested axes-1 directories deep
///
/// That nesting keeps any single directory to at most `chunks_per_axis()`
/// entries no matter how large the world gets, and chunks with no data in
/// them are simply never written -- a 10,000^3 world (1 trillion cells)
/// that's mostly empty costs disk space proportional to how much of it is
/// actually populated, not to its nominal size.
pub struct World {
    root: PathBuf,
    axes: usize,
    world_dim: u32,
    chunk_cells: usize,
    schema: Schema,
    cache: HashMap<ChunkKey, Chunk>,
    dirty: HashSet<ChunkKey>,
    cache_capacity: usize,
    pub chunks_read_from_disk: u64,
    pub chunks_written_to_disk: u64,
}

impl World {
    /// Creates a new world at `root`, or validates an existing one there.
    ///
    /// If `root/world.txt` doesn't exist yet, this world is brand new: it's
    /// created with exactly the given `axes`/`world_dim`, persisted so every
    /// later `open`/`create` of this directory sees the same shape. If
    /// `root/world.txt` already exists, `axes` and `world_dim` must match it
    /// exactly, or this fails with `InvalidInput` *without creating or
    /// opening anything* -- a world's shape can never change underneath
    /// data already written for it.
    pub fn create<P: AsRef<Path>>(root: P, axes: usize, world_dim: u32) -> io::Result<Self> {
        if axes == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "axes must be at least 1",
            ));
        }
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root)?;
        let requested = WorldParams { axes, world_dim };

        match WorldParams::read(&root)? {
            Some(existing) if existing == requested => {}
            Some(existing) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "world at {} already exists with axes={}, world_dim={} -- \
                         create() was called with axes={axes}, world_dim={world_dim}",
                        root.display(),
                        existing.axes,
                        existing.world_dim
                    ),
                ));
            }
            None => {
                WorldParams::write(&root, requested)?;
                crate::logger::info(format!(
                    "created world at {} (axes={axes}, world_dim={world_dim})",
                    root.display()
                ));
            }
        }
        Self::open_inner(root, requested)
    }

    /// Opens an existing world at `root`. `root/world.txt` must already
    /// exist (via a prior `World::create`) -- `open` reads the world's
    /// shape from disk rather than assuming any default, so it always
    /// matches whatever the world was actually created with.
    pub fn open<P: AsRef<Path>>(root: P) -> io::Result<Self> {
        let root = root.as_ref().to_path_buf();
        let params = WorldParams::read(&root)?.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "no world.txt at {} -- call World::create to initialize a new world here \
                     first",
                    root.display()
                ),
            )
        })?;
        Self::open_inner(root, params)
    }

    fn open_inner(root: PathBuf, params: WorldParams) -> io::Result<Self> {
        fs::create_dir_all(&root)?;
        let schema = Schema::open(&root)?;
        crate::logger::info(format!(
            "world opened at {} (axes={}, world_dim={}, {} keys already interned)",
            root.display(),
            params.axes,
            params.world_dim,
            schema.len()
        ));
        Ok(World {
            root,
            axes: params.axes,
            world_dim: params.world_dim,
            chunk_cells: chunk::chunk_cells(params.axes),
            schema,
            cache: HashMap::new(),
            dirty: HashSet::new(),
            cache_capacity: 256,
            chunks_read_from_disk: 0,
            chunks_written_to_disk: 0,
        })
    }

    pub fn axes(&self) -> usize {
        self.axes
    }

    pub fn world_dim(&self) -> u32 {
        self.world_dim
    }

    pub fn chunks_per_axis(&self) -> u32 {
        self.world_dim.div_ceil(CHUNK_DIM)
    }

    pub fn schema_len(&self) -> usize {
        self.schema.len()
    }

    fn chunk_path(&self, ckey: &ChunkKey) -> PathBuf {
        let mut p = self.root.clone();
        for &c in &ckey[..self.axes - 1] {
            p = p.join(c.to_string());
        }
        p.join(format!("{}.chunk", ckey[self.axes - 1]))
    }

    fn split(&self, coord: &[u32]) -> io::Result<(ChunkKey, usize)> {
        if coord.len() != self.axes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "coordinate {coord:?} has {} axes but this world has {}",
                    coord.len(),
                    self.axes
                ),
            ));
        }
        if let Some(&bad) = coord.iter().find(|&&c| c >= self.world_dim) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "coordinate {coord:?} is out of range: {bad} >= world_dim {}",
                    self.world_dim
                ),
            ));
        }
        let mut ckey: ChunkKey = Coord::zeros(self.axes);
        let mut local_idx = 0usize;
        let mut mult = 1usize;
        for (a, c) in coord.iter().enumerate() {
            ckey[a] = c / CHUNK_DIM;
            local_idx += (c % CHUNK_DIM) as usize * mult;
            mult *= CHUNK_DIM as usize;
        }
        Ok((ckey, local_idx))
    }

    fn load_chunk(&mut self, ckey: &ChunkKey) -> io::Result<()> {
        if self.cache.contains_key(ckey) {
            return Ok(());
        }
        self.evict_if_needed()?;

        let path = self.chunk_path(ckey);
        let chunk = if path.exists() {
            let f = File::open(&path)?;
            let mut r = BufReader::new(f);
            self.chunks_read_from_disk += 1;
            Chunk::read_from(&mut r, self.chunk_cells)?
        } else {
            Chunk::new(self.chunk_cells)
        };
        self.cache.insert(ckey.clone(), chunk);
        Ok(())
    }

    /// Keeps the resident chunk cache bounded, which is what makes it fine to
    /// address a 10,000^3 (1-trillion-cell) world from a process that only
    /// ever holds a couple hundred chunks -- a few tens of MB -- in memory at
    /// once. A real system would use LRU; this prototype evicts an arbitrary
    /// clean entry (flushing first if it was dirty) to keep the mechanism
    /// easy to follow.
    fn evict_if_needed(&mut self) -> io::Result<()> {
        if self.cache.len() < self.cache_capacity {
            return Ok(());
        }
        let victim = self
            .cache
            .keys()
            .find(|k| !self.dirty.contains(*k))
            .cloned()
            .or_else(|| self.cache.keys().next().cloned());
        if let Some(k) = victim {
            // Routine, not a problem: this is how the bounded cache stays
            // bounded. Not WARN-level -- a world touching more chunks than
            // fit in cache is the expected case, not an error condition.
            crate::logger::info(format!(
                "chunk cache at capacity ({}); evicting {k:?}",
                self.cache_capacity
            ));
            self.flush_one(&k)?;
            self.cache.remove(&k);
        }
        Ok(())
    }

    fn flush_one(&mut self, ckey: &ChunkKey) -> io::Result<()> {
        if !self.dirty.remove(ckey) {
            return Ok(());
        }
        let chunk = match self.cache.get(ckey) {
            Some(c) => c,
            None => return Ok(()),
        };
        let path = self.chunk_path(ckey);
        if chunk.is_empty() {
            // Nothing left in this chunk (e.g. every cell was removed) --
            // don't leave a pointless empty file around.
            let _ = fs::remove_file(&path);
            return Ok(());
        }
        fs::create_dir_all(path.parent().unwrap())?;
        let f = File::create(&path)?;
        let mut w = BufWriter::new(f);
        chunk.write_to(&mut w)?;
        self.chunks_written_to_disk += 1;
        Ok(())
    }

    /// Write every dirty chunk currently in the cache back to disk.
    pub fn flush(&mut self) -> io::Result<()> {
        let keys: Vec<_> = self.dirty.iter().cloned().collect();
        if !keys.is_empty() {
            crate::logger::info(format!("flushing {} dirty chunk(s)", keys.len()));
        }
        for k in keys {
            self.flush_one(&k)?;
        }
        Ok(())
    }

    pub fn get(&mut self, coord: &[u32], key: &str) -> io::Result<Option<Value>> {
        // Validate the coordinate *before* consulting the schema: a
        // malformed/out-of-range coordinate must always be rejected the
        // same way, regardless of whether `key` happens to be known yet --
        // this doubles as a request-validation boundary for callers (like
        // kdbserver) that pass through attacker-/user-supplied coordinates.
        let (ckey, local_idx) = self.split(coord)?;
        let key_id = match self.schema.id_for_key(key) {
            Some(id) => id,
            None => return Ok(None), // this key has never been written anywhere in the world
        };
        self.load_chunk(&ckey)?;
        Ok(self.cache[&ckey].get(local_idx, key_id))
    }

    pub fn set(&mut self, coord: &[u32], key: &str, value: Value) -> io::Result<()> {
        // Validate the coordinate before interning `key`: a failed `set`
        // shouldn't have the side effect of permanently registering a new
        // key that was never actually written anywhere.
        let (ckey, local_idx) = self.split(coord)?;
        let key_id = self.schema.intern(key)?;
        self.load_chunk(&ckey)?;
        self.cache
            .get_mut(&ckey)
            .unwrap()
            .set(local_idx, key_id, value);
        self.dirty.insert(ckey);
        Ok(())
    }

    pub fn remove(&mut self, coord: &[u32], key: &str) -> io::Result<()> {
        // See `get`: validate the coordinate before the key-existence
        // short-circuit, so a bad coordinate is never silently absorbed
        // into remove's usual "unknown key is a harmless no-op" behavior.
        let (ckey, local_idx) = self.split(coord)?;
        let key_id = match self.schema.id_for_key(key) {
            Some(id) => id,
            None => {
                // Distinct from "key exists but isn't set on this cell"
                // (routine, logged as INFO below): this key has never been
                // interned anywhere in the world, which more likely means a
                // typo than a deliberate no-op.
                crate::logger::warn(format!(
                    "remove() called with unknown key '{key}' at {coord:?} -- no-op"
                ));
                return Ok(());
            }
        };
        self.remove_cell_at(ckey, local_idx, key_id)?;
        crate::logger::info(format!("removed key '{key}' at {coord:?}"));
        Ok(())
    }

    /// Core of `remove`/`remove_region`, without the per-call logging --
    /// `remove_region` logs one summary line for the whole box instead of
    /// one per cell.
    fn remove_cell(&mut self, coord: &[u32], key_id: u32) -> io::Result<()> {
        let (ckey, local_idx) = self.split(coord)?;
        self.remove_cell_at(ckey, local_idx, key_id)
    }

    fn remove_cell_at(&mut self, ckey: ChunkKey, local_idx: usize, key_id: u32) -> io::Result<()> {
        self.load_chunk(&ckey)?;
        self.cache.get_mut(&ckey).unwrap().remove(local_idx, key_id);
        self.dirty.insert(ckey);
        Ok(())
    }

    /// Checks that `region`'s origin and extent both have this world's axis
    /// count -- *not* just that they match each other, since `Region::new`
    /// itself doesn't enforce that (see its doc comment) -- and that it
    /// fits inside the world without overflowing `u32` along the way.
    fn check_region(&self, region: &Region) -> io::Result<()> {
        if region.origin.len() != self.axes || region.extent.len() != self.axes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "region {region:?} has {} origin axes and {} extent axes but this world \
                     has {}",
                    region.origin.len(),
                    region.extent.len(),
                    self.axes
                ),
            ));
        }
        let in_range =
            |lo: u32, len: u32| lo.checked_add(len).is_some_and(|hi| hi <= self.world_dim);
        let fits = region
            .origin
            .iter()
            .zip(region.extent.iter())
            .all(|(&lo, &len)| in_range(lo, len));
        if fits {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "region {region:?} doesn't fit in a {}^{} world",
                    self.world_dim, self.axes
                ),
            ))
        }
    }

    /// Reads `key` for every cell in `region`. The box may span any number
    /// of chunks and cover only part of a chunk along any face -- each cell
    /// is resolved independently through the same chunk/local-index split
    /// as a single-cell `get`, so chunk boundaries need no special-casing
    /// here.
    ///
    /// Results come back in `RegionIter` order (axis 0 fastest): `result[i]`
    /// is `RegionIter::new(region).nth(i)`.
    pub fn get_region(&mut self, region: &Region, key: &str) -> io::Result<Vec<Option<Value>>> {
        self.check_region(region)?;
        let mut out = Vec::with_capacity(region.volume() as usize);
        for coord in RegionIter::new(region) {
            out.push(self.get(&coord, key)?);
        }
        Ok(out)
    }

    /// Sets `key` on every cell in `region` from `values`, spanning chunks
    /// and partial chunks exactly like `get_region`. `values` holds one
    /// value per cell, in the same order as `get_region`'s result (see
    /// `RegionIter`), and its length must equal `region.volume()` exactly,
    /// or this returns an `InvalidInput` error without writing anything. A
    /// no-op region (any axis's extent zero, so `values` must be empty too)
    /// doesn't even intern `key`.
    pub fn set_region(&mut self, region: &Region, key: &str, values: &[Value]) -> io::Result<()> {
        self.check_region(region)?;
        let volume = region.volume() as usize;
        if values.len() != volume {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "set_region: region {region:?} holds {volume} cells but {} values were given",
                    values.len()
                ),
            ));
        }
        if volume == 0 {
            return Ok(());
        }
        let key_id = self.schema.intern(key)?;
        for (coord, value) in RegionIter::new(region).zip(values) {
            let (ckey, local_idx) = self.split(&coord)?;
            self.load_chunk(&ckey)?;
            self.cache
                .get_mut(&ckey)
                .unwrap()
                .set(local_idx, key_id, value.clone());
            self.dirty.insert(ckey);
        }
        crate::logger::info(format!(
            "set region {region:?} key '{key}' from {volume} per-cell values"
        ));
        Ok(())
    }

    /// Removes `key` from every cell in `region`, spanning chunks and
    /// partial chunks exactly like `get_region`. Unlike the single-cell
    /// `remove`, this logs one summary line for the whole region rather
    /// than one line per cell, so clearing a large region doesn't flood
    /// `kdb.log`.
    pub fn remove_region(&mut self, region: &Region, key: &str) -> io::Result<()> {
        self.check_region(region)?;
        if region.volume() == 0 {
            return Ok(());
        }
        let key_id = match self.schema.id_for_key(key) {
            Some(id) => id,
            None => {
                crate::logger::warn(format!(
                    "remove_region() called with unknown key '{key}' at {region:?} -- no-op"
                ));
                return Ok(());
            }
        };
        for coord in RegionIter::new(region) {
            self.remove_cell(&coord, key_id)?;
        }
        crate::logger::info(format!(
            "removed region {region:?} key '{key}' ({} cells)",
            region.volume()
        ));
        Ok(())
    }
}

impl Drop for World {
    fn drop(&mut self) {
        // Best-effort: make sure a `World` that goes out of scope without an
        // explicit flush() doesn't silently lose writes.
        let _ = self.flush();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// A scratch directory under the OS temp dir, unique per test, removed
    /// when it goes out of scope.
    struct TempDir(PathBuf);

    impl TempDir {
        fn new(tag: &str) -> Self {
            static COUNTER: AtomicU64 = AtomicU64::new(0);
            let n = COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir()
                .join(format!("kdb-world-test-{tag}-{}-{n}", std::process::id()));
            TempDir(path)
        }
    }

    impl AsRef<Path> for TempDir {
        fn as_ref(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    /// Creates a fresh 3-axis, `WORLD_DIM`-sized world at `dir` -- the
    /// default shape every test below assumes.
    fn create(dir: &TempDir) -> World {
        World::create(dir, AXES, WORLD_DIM).unwrap()
    }

    fn coord3(x: u32, y: u32, z: u32) -> Coord {
        Coord::from([x, y, z])
    }

    #[test]
    fn create_writes_world_txt_and_open_reads_it_back() {
        let dir = TempDir::new("create-basic");
        {
            let w = World::create(&dir, 3, 10_000).unwrap();
            assert_eq!(w.axes(), 3);
            assert_eq!(w.world_dim(), 10_000);
        }

        let w = World::open(&dir).unwrap();
        assert_eq!(w.axes(), 3);
        assert_eq!(w.world_dim(), 10_000);
    }

    #[test]
    fn open_without_a_prior_create_is_not_found() {
        let dir = TempDir::new("open-no-create");
        let err = World::open(&dir).err().unwrap();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
    }

    #[test]
    fn create_is_idempotent_with_matching_params() {
        let dir = TempDir::new("create-idempotent");
        World::create(&dir, 3, 100).unwrap();
        // Calling create again with the same params re-opens cleanly.
        let w = World::create(&dir, 3, 100).unwrap();
        assert_eq!(w.axes(), 3);
        assert_eq!(w.world_dim(), 100);
    }

    #[test]
    fn create_with_mismatched_params_fails_without_changing_anything() {
        let dir = TempDir::new("create-mismatch");
        World::create(&dir, 3, 100).unwrap();

        assert_eq!(
            World::create(&dir, 4, 100).err().unwrap().kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(
            World::create(&dir, 3, 200).err().unwrap().kind(),
            io::ErrorKind::InvalidInput
        );

        // The original world is untouched and still openable with its
        // original shape.
        let w = World::open(&dir).unwrap();
        assert_eq!(w.axes(), 3);
        assert_eq!(w.world_dim(), 100);
    }

    #[test]
    fn create_rejects_zero_axes() {
        let dir = TempDir::new("create-zero-axes");
        assert_eq!(
            World::create(&dir, 0, 100).err().unwrap().kind(),
            io::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn a_world_can_have_a_different_axis_count() {
        let dir = TempDir::new("create-2d");
        let mut w = World::create(&dir, 2, 50).unwrap();

        w.set(&[1, 2], "material", Value::Str("stone".into()))
            .unwrap();
        assert_eq!(
            w.get(&[1, 2], "material").unwrap(),
            Some(Value::Str("stone".into()))
        );

        // A 3-coordinate lookup against a 2-axis world is a caller error,
        // not a panic.
        assert_eq!(
            w.get(&[1, 2, 3], "material").unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn get_on_untouched_world_is_none() {
        let dir = TempDir::new("untouched");
        let mut w = create(&dir);

        // Key never interned anywhere in the world.
        assert_eq!(w.get(&coord3(0, 0, 0), "material").unwrap(), None);
        assert_eq!(w.schema_len(), 0);
    }

    #[test]
    fn get_validates_the_coordinate_even_for_a_never_interned_key() {
        // Regression: get() used to check schema membership before
        // validating the coordinate, so a bad coordinate against an
        // unknown key silently returned Ok(None) instead of erroring.
        let dir = TempDir::new("get-validates-oob-unknown-key");
        let mut w = create(&dir);

        assert_eq!(
            w.get(&coord3(WORLD_DIM, 0, 0), "never-set")
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(
            w.get(&[1, 2], "never-set").unwrap_err().kind(), // wrong axis count
            io::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn remove_validates_the_coordinate_even_for_a_never_interned_key() {
        // Regression: same short-circuit-before-validation bug as get(),
        // in remove()'s "unknown key is a harmless no-op" path.
        let dir = TempDir::new("remove-validates-oob-unknown-key");
        let mut w = create(&dir);

        assert_eq!(
            w.remove(&coord3(WORLD_DIM, 0, 0), "never-set")
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn set_does_not_intern_the_key_if_the_coordinate_is_invalid() {
        // Regression: set() used to intern the key before validating the
        // coordinate, so a failed set() still had the permanent side
        // effect of registering a key nothing was ever actually stored
        // under.
        let dir = TempDir::new("set-no-intern-on-bad-coord");
        let mut w = create(&dir);

        let err = w.set(&coord3(WORLD_DIM, 0, 0), "material", Value::I64(1));
        assert_eq!(err.unwrap_err().kind(), io::ErrorKind::InvalidInput);
        assert_eq!(w.schema_len(), 0, "key must not be interned on failure");
    }

    #[test]
    fn set_then_get_roundtrips_each_value_type() {
        let dir = TempDir::new("set-get-types");
        let mut w = create(&dir);
        let c = coord3(1, 2, 3);

        w.set(&c, "material", Value::Str("stone".into())).unwrap();
        w.set(&c, "density", Value::F64(2.5)).unwrap();
        w.set(&c, "hardness", Value::I64(7)).unwrap();

        assert_eq!(
            w.get(&c, "material").unwrap(),
            Some(Value::Str("stone".into()))
        );
        assert_eq!(w.get(&c, "density").unwrap(), Some(Value::F64(2.5)));
        assert_eq!(w.get(&c, "hardness").unwrap(), Some(Value::I64(7)));
    }

    #[test]
    fn set_overwrites_previous_value_at_same_cell_and_key() {
        let dir = TempDir::new("overwrite");
        let mut w = create(&dir);
        let c = coord3(9, 9, 9);

        w.set(&c, "material", Value::Str("stone".into())).unwrap();
        w.set(&c, "material", Value::Str("air".into())).unwrap();

        assert_eq!(
            w.get(&c, "material").unwrap(),
            Some(Value::Str("air".into()))
        );
    }

    #[test]
    fn set_on_one_cell_does_not_affect_neighbors_or_other_keys() {
        let dir = TempDir::new("isolation");
        let mut w = create(&dir);

        w.set(&coord3(0, 0, 0), "material", Value::Str("stone".into()))
            .unwrap();

        // Same key, different cell in the same chunk: untouched.
        assert_eq!(w.get(&coord3(1, 0, 0), "material").unwrap(), None);
        // Same cell, different key: untouched.
        assert_eq!(w.get(&coord3(0, 0, 0), "density").unwrap(), None);
    }

    #[test]
    fn set_across_multiple_chunks() {
        let dir = TempDir::new("multi-chunk");
        let mut w = create(&dir);

        // CHUNK_DIM cells apart on axis 0 guarantees these land in different
        // chunks.
        let far = coord3(CHUNK_DIM, 0, 0);
        w.set(&coord3(0, 0, 0), "material", Value::Str("stone".into()))
            .unwrap();
        w.set(&far, "material", Value::Str("air".into())).unwrap();

        assert_eq!(
            w.get(&coord3(0, 0, 0), "material").unwrap(),
            Some(Value::Str("stone".into()))
        );
        assert_eq!(
            w.get(&far, "material").unwrap(),
            Some(Value::Str("air".into()))
        );
    }

    #[test]
    fn interning_a_new_key_grows_the_schema() {
        let dir = TempDir::new("schema-growth");
        let mut w = create(&dir);
        assert_eq!(w.schema_len(), 0);

        w.set(&coord3(0, 0, 0), "material", Value::Str("stone".into()))
            .unwrap();
        assert_eq!(w.schema_len(), 1);

        // Reusing the same key does not add another schema entry.
        w.set(&coord3(1, 1, 1), "material", Value::Str("air".into()))
            .unwrap();
        assert_eq!(w.schema_len(), 1);

        w.set(&coord3(0, 0, 0), "density", Value::F64(1.0)).unwrap();
        assert_eq!(w.schema_len(), 2);
    }

    #[test]
    fn set_persists_across_flush_and_reopen() {
        let dir = TempDir::new("set-persist");
        let c = coord3(100, 200, 300);
        {
            let mut w = create(&dir);
            w.set(&c, "material", Value::Str("stone".into())).unwrap();
            w.set(&c, "density", Value::F64(2.5)).unwrap();
            w.flush().unwrap();
        }

        let mut w = World::open(&dir).unwrap();
        assert_eq!(
            w.get(&c, "material").unwrap(),
            Some(Value::Str("stone".into()))
        );
        assert_eq!(w.get(&c, "density").unwrap(), Some(Value::F64(2.5)));
        // The schema (key registry) is durable too.
        assert_eq!(w.schema_len(), 2);
    }

    #[test]
    fn remove_clears_cell_in_same_session() {
        let dir = TempDir::new("same-session");
        let mut w = create(&dir);
        let c = coord3(1, 2, 3);

        w.set(&c, "material", Value::Str("stone".into())).unwrap();
        assert_eq!(
            w.get(&c, "material").unwrap(),
            Some(Value::Str("stone".into()))
        );

        w.remove(&c, "material").unwrap();
        assert_eq!(w.get(&c, "material").unwrap(), None);
    }

    #[test]
    fn remove_of_unset_key_is_a_harmless_noop() {
        let dir = TempDir::new("noop");
        let mut w = create(&dir);
        let c = coord3(4, 5, 6);

        // Key was never interned anywhere in the world.
        w.remove(&c, "nonexistent").unwrap();
        assert_eq!(w.get(&c, "nonexistent").unwrap(), None);

        // Key exists in the schema, but not on this particular cell.
        w.set(&c, "material", Value::I64(1)).unwrap();
        w.remove(&coord3(7, 8, 9), "material").unwrap();
        assert_eq!(w.get(&c, "material").unwrap(), Some(Value::I64(1)));
    }

    #[test]
    fn remove_persists_across_flush_and_reopen() {
        let dir = TempDir::new("persist");
        let c = coord3(100, 200, 300);
        {
            let mut w = create(&dir);
            w.set(&c, "material", Value::Str("stone".into())).unwrap();
            w.remove(&c, "material").unwrap();
            w.flush().unwrap();
        }

        let mut w = World::open(&dir).unwrap();
        assert_eq!(w.get(&c, "material").unwrap(), None);
    }

    #[test]
    fn removing_every_cell_in_a_chunk_deletes_its_file() {
        let dir = TempDir::new("empty-chunk-gc");
        let c = coord3(0, 0, 0);
        let chunk_path;
        {
            let mut w = create(&dir);
            w.set(&c, "material", Value::Str("stone".into())).unwrap();
            w.flush().unwrap();
            let (ckey, _) = w.split(&c).unwrap();
            chunk_path = w.chunk_path(&ckey);
        }
        assert!(
            chunk_path.exists(),
            "chunk file should exist once populated"
        );

        let mut w = World::open(&dir).unwrap();
        w.remove(&c, "material").unwrap();
        w.flush().unwrap();
        assert!(
            !chunk_path.exists(),
            "emptied chunk file should be cleaned up"
        );
    }

    /// `n` copies of `value`, for tests that don't care about per-cell
    /// variation and just want to fill a region uniformly.
    fn fill(region: &Region, value: Value) -> Vec<Value> {
        vec![value; region.volume() as usize]
    }

    #[test]
    fn get_region_on_untouched_world_is_all_none() {
        let dir = TempDir::new("region-untouched");
        let mut w = create(&dir);

        let region = Region::new(coord3(5, 5, 5), coord3(3, 2, 2));
        let got = w.get_region(&region, "material").unwrap();
        assert_eq!(got.len(), region.volume() as usize);
        assert!(got.iter().all(Option::is_none));
    }

    #[test]
    fn set_region_then_get_region_roundtrips_within_one_chunk() {
        let dir = TempDir::new("region-roundtrip");
        let mut w = create(&dir);

        let region = Region::new(coord3(1, 1, 1), coord3(4, 3, 2));
        w.set_region(
            &region,
            "material",
            &fill(&region, Value::Str("stone".into())),
        )
        .unwrap();

        let got = w.get_region(&region, "material").unwrap();
        assert_eq!(got.len(), region.volume() as usize);
        assert!(got.iter().all(|v| *v == Some(Value::Str("stone".into()))));
    }

    #[test]
    fn set_region_writes_distinct_per_cell_values() {
        let dir = TempDir::new("region-per-cell");
        let mut w = create(&dir);

        let region = Region::new(coord3(10, 20, 30), coord3(3, 2, 2));
        let values: Vec<Value> = (0..region.volume())
            .map(|i| Value::I64(i as i64 * 7))
            .collect();
        w.set_region(&region, "n", &values).unwrap();

        let got = w.get_region(&region, "n").unwrap();
        for (i, v) in values.iter().enumerate() {
            assert_eq!(got[i], Some(v.clone()));
        }
    }

    #[test]
    fn set_region_rejects_a_mismatched_value_count() {
        let dir = TempDir::new("region-bad-count");
        let mut w = create(&dir);

        let region = Region::new(coord3(0, 0, 0), coord3(2, 2, 2));
        let too_few = vec![Value::I64(0); region.volume() as usize - 1];
        let err = w.set_region(&region, "n", &too_few).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);

        // The rejected call must not have written anything.
        let got = w.get_region(&region, "n").unwrap();
        assert!(got.iter().all(Option::is_none));
    }

    #[test]
    fn set_region_does_not_leak_outside_its_bounds() {
        let dir = TempDir::new("region-bounds");
        let mut w = create(&dir);

        let region = Region::new(coord3(10, 10, 10), coord3(2, 2, 2));
        w.set_region(
            &region,
            "material",
            &fill(&region, Value::Str("stone".into())),
        )
        .unwrap();

        // One past the region on each axis: untouched.
        assert_eq!(w.get(&coord3(12, 10, 10), "material").unwrap(), None);
        assert_eq!(w.get(&coord3(10, 12, 10), "material").unwrap(), None);
        assert_eq!(w.get(&coord3(10, 10, 12), "material").unwrap(), None);
        // Just inside on each axis: set.
        assert_eq!(
            w.get(&coord3(11, 10, 10), "material").unwrap(),
            Some(Value::Str("stone".into()))
        );
    }

    #[test]
    fn get_region_orders_results_axis0_fastest() {
        let dir = TempDir::new("region-order");
        let mut w = create(&dir);

        let region = Region::new(coord3(10, 20, 30), coord3(3, 2, 2));

        // Give every cell in the region a distinct value derived from its
        // offset, then check RegionIter's enumeration order matches
        // get_region's result order exactly.
        for (i, coord) in RegionIter::new(&region).enumerate() {
            w.set(&coord, "n", Value::I64(i as i64)).unwrap();
        }

        let got = w.get_region(&region, "n").unwrap();
        for (i, coord) in RegionIter::new(&region).enumerate() {
            assert_eq!(got[i], Some(Value::I64(i as i64)), "mismatch at {coord:?}");
        }
    }

    #[test]
    fn region_spans_chunks_including_partial_chunks() {
        let dir = TempDir::new("region-span-chunks");
        let mut w = create(&dir);

        // Starts 5 cells before a chunk boundary and ends 5 cells past the
        // next one, on every axis: covers the tail of one chunk, all of a
        // second, and the head of a third, on each axis.
        let x0 = CHUNK_DIM - 5;
        let d = CHUNK_DIM + 10;
        let region = Region::new(vec![x0; 3], vec![d; 3]);

        w.set_region(
            &region,
            "material",
            &fill(&region, Value::Str("stone".into())),
        )
        .unwrap();
        w.flush().unwrap();

        // Reopen so the read exercises chunks freshly loaded from disk, not
        // just what's still resident in cache.
        let mut w = World::open(&dir).unwrap();
        let got = w.get_region(&region, "material").unwrap();
        assert_eq!(got.len(), region.volume() as usize);
        assert!(got.iter().all(|v| *v == Some(Value::Str("stone".into()))));

        // Just outside the region on the low and high corners: untouched.
        assert_eq!(w.get(&[x0 - 1; 3], "material").unwrap(), None);
        assert_eq!(w.get(&[x0 + d; 3], "material").unwrap(), None);
    }

    #[test]
    fn remove_region_clears_key_across_chunks_without_touching_others() {
        let dir = TempDir::new("region-remove");
        let mut w = create(&dir);

        let x0 = CHUNK_DIM - 2;
        let region = Region::new(coord3(x0, 0, 0), coord3(5, 5, 5));
        w.set_region(
            &region,
            "material",
            &fill(&region, Value::Str("stone".into())),
        )
        .unwrap();
        w.set_region(&region, "density", &fill(&region, Value::F64(2.6)))
            .unwrap();

        w.remove_region(&region, "material").unwrap();

        let material = w.get_region(&region, "material").unwrap();
        assert!(material.iter().all(Option::is_none));
        // A different key on the same cells is untouched by the removal.
        let density = w.get_region(&region, "density").unwrap();
        assert!(density.iter().all(|v| *v == Some(Value::F64(2.6))));
    }

    #[test]
    fn remove_region_of_unknown_key_is_a_harmless_noop() {
        let dir = TempDir::new("region-remove-unknown");
        let mut w = create(&dir);
        // Should not error even though "material" has never been interned.
        w.remove_region(&Region::new(coord3(0, 0, 0), coord3(4, 4, 4)), "material")
            .unwrap();
    }

    #[test]
    fn zero_sized_region_is_a_harmless_noop() {
        let dir = TempDir::new("region-zero");
        let mut w = create(&dir);

        let zero_extent = coord3(0, 5, 5); // zero on any single axis empties the whole region

        assert_eq!(
            w.get_region(
                &Region::new(coord3(0, 0, 0), zero_extent.clone()),
                "material"
            )
            .unwrap(),
            vec![]
        );
        w.set_region(
            &Region::new(coord3(0, 0, 0), zero_extent.clone()),
            "material",
            &[],
        )
        .unwrap();
        // A no-op set_region shouldn't even intern the key.
        assert_eq!(w.schema_len(), 0);
        w.remove_region(&Region::new(coord3(0, 0, 0), zero_extent), "material")
            .unwrap();
    }

    #[test]
    fn region_out_of_world_bounds_is_an_error() {
        let dir = TempDir::new("region-oob");
        let mut w = create(&dir);

        let region = Region::new(coord3(WORLD_DIM - 1, 0, 0), coord3(2, 1, 1));
        assert_eq!(
            w.get_region(&region, "material").unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );

        // Overflowing u32 entirely must not panic or wrap around.
        let region = Region::new(coord3(u32::MAX - 1, 0, 0), coord3(5, 1, 1));
        assert_eq!(
            w.set_region(
                &region,
                "material",
                &vec![Value::I64(0); region.volume() as usize],
            )
            .unwrap_err()
            .kind(),
            io::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn region_with_wrong_axis_count_is_an_error() {
        let dir = TempDir::new("region-wrong-axes");
        let mut w = create(&dir); // 3-axis world

        let region = Region::new(vec![0, 0], vec![2, 2]); // 2 axes
        assert_eq!(
            w.get_region(&region, "material").unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn region_with_mismatched_origin_and_extent_axes_is_an_error() {
        // Regression: Region::new doesn't (and, taking origin/extent from
        // untrusted input like an HTTP request, can't) itself guarantee
        // origin.len() == extent.len() -- check_region must catch this
        // rather than leaving it to panic or index out of bounds later.
        let dir = TempDir::new("region-mismatched-origin-extent");
        let mut w = create(&dir); // 3-axis world

        let region = Region::new(vec![0, 0, 0], vec![2, 2]); // 3 origin axes, 2 extent axes
        assert_eq!(
            w.get_region(&region, "material").unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn set_region_persists_across_flush_and_reopen() {
        let dir = TempDir::new("region-persist");
        let region = Region::new(coord3(100, 100, 100), coord3(3, 3, 3));
        {
            let mut w = create(&dir);
            w.set_region(
                &region,
                "material",
                &fill(&region, Value::Str("stone".into())),
            )
            .unwrap();
            w.flush().unwrap();
        }

        let mut w = World::open(&dir).unwrap();
        let got = w.get_region(&region, "material").unwrap();
        assert!(got.iter().all(|v| *v == Some(Value::Str("stone".into()))));
    }
}
