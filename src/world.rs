use crate::chunk::{Chunk, CHUNK_DIM};
use crate::schema::Schema;
use crate::value::Value;
use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter};
use std::path::{Path, PathBuf};

/// Cells per axis for the whole simulated volume.
pub const WORLD_DIM: u32 = 10_000;
/// Chunks per axis needed to cover WORLD_DIM cells (313, since 313*32 = 10,016).
pub const CHUNKS_PER_AXIS: u32 = WORLD_DIM.div_ceil(CHUNK_DIM);

type ChunkKey = (u32, u32, u32);

/// An axis-aligned box of cells: origin `(x, y, z)` plus extent
/// `(dx, dy, dz)`, i.e. `[x, x+dx) x [y, y+dy) x [z, z+dz)`. Bundles what
/// would otherwise be six separate parameters on every `*_region` method,
/// and may span or partially cover any number of chunks -- see
/// `World::get_region`/`set_region`/`remove_region`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Region {
    pub x: u32,
    pub y: u32,
    pub z: u32,
    pub dx: u32,
    pub dy: u32,
    pub dz: u32,
}

impl Region {
    pub fn new(x: u32, y: u32, z: u32, dx: u32, dy: u32, dz: u32) -> Self {
        Region {
            x,
            y,
            z,
            dx,
            dy,
            dz,
        }
    }

    pub fn volume(&self) -> u64 {
        u64::from(self.dx) * u64::from(self.dy) * u64::from(self.dz)
    }
}

/// The on-disk, chunked, columnar key-value world.
///
/// Layout on disk under `root`:
///   root/schema.txt              -- key string <-> id registry (see schema.rs)
///   root/<cx>/<cy>/<cz>.chunk    -- one file per non-empty chunk
///
/// Nesting chunk files two directories deep keeps any single directory to at
/// most CHUNKS_PER_AXIS (313) entries no matter how large the world gets, and
/// chunks with no data in them are simply never written -- a 10,000^3 world
/// (1 trillion cells) that's mostly empty costs disk space proportional to
/// how much of it is actually populated, not to its nominal size.
pub struct World {
    root: PathBuf,
    schema: Schema,
    cache: HashMap<ChunkKey, Chunk>,
    dirty: HashSet<ChunkKey>,
    cache_capacity: usize,
    pub chunks_read_from_disk: u64,
    pub chunks_written_to_disk: u64,
}

impl World {
    pub fn open<P: AsRef<Path>>(root: P) -> io::Result<Self> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root)?;
        let schema = Schema::open(&root)?;
        crate::logger::info(format!(
            "world opened at {} ({} keys already interned)",
            root.display(),
            schema.len()
        ));
        Ok(World {
            root,
            schema,
            cache: HashMap::new(),
            dirty: HashSet::new(),
            cache_capacity: 256,
            chunks_read_from_disk: 0,
            chunks_written_to_disk: 0,
        })
    }

    pub fn schema_len(&self) -> usize {
        self.schema.len()
    }

    fn chunk_path(&self, (cx, cy, cz): ChunkKey) -> PathBuf {
        self.root
            .join(cx.to_string())
            .join(cy.to_string())
            .join(format!("{cz}.chunk"))
    }

    fn split(x: u32, y: u32, z: u32) -> (ChunkKey, usize) {
        debug_assert!(
            x < WORLD_DIM && y < WORLD_DIM && z < WORLD_DIM,
            "coordinate out of range"
        );
        let (cx, lx) = (x / CHUNK_DIM, x % CHUNK_DIM);
        let (cy, ly) = (y / CHUNK_DIM, y % CHUNK_DIM);
        let (cz, lz) = (z / CHUNK_DIM, z % CHUNK_DIM);
        let local_idx = (lx + ly * CHUNK_DIM + lz * CHUNK_DIM * CHUNK_DIM) as usize;
        ((cx, cy, cz), local_idx)
    }

    fn load_chunk(&mut self, ckey: ChunkKey) -> io::Result<()> {
        if self.cache.contains_key(&ckey) {
            return Ok(());
        }
        self.evict_if_needed()?;

        let path = self.chunk_path(ckey);
        let chunk = if path.exists() {
            let f = File::open(&path)?;
            let mut r = BufReader::new(f);
            self.chunks_read_from_disk += 1;
            Chunk::read_from(&mut r)?
        } else {
            Chunk::new()
        };
        self.cache.insert(ckey, chunk);
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
            .copied()
            .or_else(|| self.cache.keys().next().copied());
        if let Some(k) = victim {
            // Routine, not a problem: this is how the bounded cache stays
            // bounded. Not WARN-level -- a world touching more chunks than
            // fit in cache is the expected case, not an error condition.
            crate::logger::info(format!(
                "chunk cache at capacity ({}); evicting {k:?}",
                self.cache_capacity
            ));
            self.flush_one(k)?;
            self.cache.remove(&k);
        }
        Ok(())
    }

    fn flush_one(&mut self, ckey: ChunkKey) -> io::Result<()> {
        if !self.dirty.remove(&ckey) {
            return Ok(());
        }
        let chunk = match self.cache.get(&ckey) {
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
        let keys: Vec<_> = self.dirty.iter().copied().collect();
        if !keys.is_empty() {
            crate::logger::info(format!("flushing {} dirty chunk(s)", keys.len()));
        }
        for k in keys {
            self.flush_one(k)?;
        }
        Ok(())
    }

    pub fn get(&mut self, x: u32, y: u32, z: u32, key: &str) -> io::Result<Option<Value>> {
        let key_id = match self.schema.id_for_key(key) {
            Some(id) => id,
            None => return Ok(None), // this key has never been written anywhere in the world
        };
        let (ckey, local_idx) = Self::split(x, y, z);
        self.load_chunk(ckey)?;
        Ok(self.cache[&ckey].get(local_idx, key_id))
    }

    pub fn set(&mut self, x: u32, y: u32, z: u32, key: &str, value: Value) -> io::Result<()> {
        let key_id = self.schema.intern(key)?;
        let (ckey, local_idx) = Self::split(x, y, z);
        self.load_chunk(ckey)?;
        self.cache
            .get_mut(&ckey)
            .unwrap()
            .set(local_idx, key_id, value);
        self.dirty.insert(ckey);
        Ok(())
    }

    pub fn remove(&mut self, x: u32, y: u32, z: u32, key: &str) -> io::Result<()> {
        let key_id = match self.schema.id_for_key(key) {
            Some(id) => id,
            None => {
                // Distinct from "key exists but isn't set on this cell"
                // (routine, logged as INFO below): this key has never been
                // interned anywhere in the world, which more likely means a
                // typo than a deliberate no-op.
                crate::logger::warn(format!(
                    "remove() called with unknown key '{key}' at ({x}, {y}, {z}) -- no-op"
                ));
                return Ok(());
            }
        };
        self.remove_cell(x, y, z, key_id)?;
        crate::logger::info(format!("removed key '{key}' at ({x}, {y}, {z})"));
        Ok(())
    }

    /// Core of `remove`/`remove_region`, without the per-call logging --
    /// `remove_region` logs one summary line for the whole box instead of
    /// one per cell.
    fn remove_cell(&mut self, x: u32, y: u32, z: u32, key_id: u32) -> io::Result<()> {
        let (ckey, local_idx) = Self::split(x, y, z);
        self.load_chunk(ckey)?;
        self.cache.get_mut(&ckey).unwrap().remove(local_idx, key_id);
        self.dirty.insert(ckey);
        Ok(())
    }

    /// Checks that `region` fits inside the world without overflowing `u32`
    /// along the way.
    fn check_region(region: Region) -> io::Result<()> {
        let Region {
            x,
            y,
            z,
            dx,
            dy,
            dz,
        } = region;
        let in_range = |lo: u32, len: u32| lo.checked_add(len).is_some_and(|hi| hi <= WORLD_DIM);
        if in_range(x, dx) && in_range(y, dy) && in_range(z, dz) {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "region {region:?} doesn't fit in a {0}x{0}x{0} world",
                    WORLD_DIM
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
    /// Results come back in `dx*dy*dz` order, x-fastest then y then z:
    /// `result[rx + ry*region.dx + rz*region.dx*region.dy]` is cell
    /// `(region.x+rx, region.y+ry, region.z+rz)`.
    pub fn get_region(&mut self, region: Region, key: &str) -> io::Result<Vec<Option<Value>>> {
        Self::check_region(region)?;
        let Region {
            x,
            y,
            z,
            dx,
            dy,
            dz,
        } = region;
        let mut out = Vec::with_capacity(region.volume() as usize);
        for rz in 0..dz {
            for ry in 0..dy {
                for rx in 0..dx {
                    out.push(self.get(x + rx, y + ry, z + rz, key)?);
                }
            }
        }
        Ok(out)
    }

    /// Sets `key` on every cell in `region` from `values`, spanning chunks
    /// and partial chunks exactly like `get_region`. `values` holds one
    /// value per cell, in the same `dx*dy*dz`, x-fastest/y/z order as
    /// `get_region`'s result -- `values[rx + ry*region.dx +
    /// rz*region.dx*region.dy]` is written to cell `(region.x+rx,
    /// region.y+ry, region.z+rz)` -- and its length must equal
    /// `region.volume()` exactly, or this returns an `InvalidInput` error
    /// without writing anything. A no-op region (any of `dx`/`dy`/`dz`
    /// zero, so `values` must be empty too) doesn't even intern `key`.
    pub fn set_region(&mut self, region: Region, key: &str, values: &[Value]) -> io::Result<()> {
        Self::check_region(region)?;
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
        let Region {
            x,
            y,
            z,
            dx,
            dy,
            dz,
        } = region;
        let key_id = self.schema.intern(key)?;
        let mut i = 0;
        for rz in 0..dz {
            for ry in 0..dy {
                for rx in 0..dx {
                    let (ckey, local_idx) = Self::split(x + rx, y + ry, z + rz);
                    self.load_chunk(ckey)?;
                    self.cache
                        .get_mut(&ckey)
                        .unwrap()
                        .set(local_idx, key_id, values[i].clone());
                    self.dirty.insert(ckey);
                    i += 1;
                }
            }
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
    pub fn remove_region(&mut self, region: Region, key: &str) -> io::Result<()> {
        Self::check_region(region)?;
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
        let Region {
            x,
            y,
            z,
            dx,
            dy,
            dz,
        } = region;
        for rz in 0..dz {
            for ry in 0..dy {
                for rx in 0..dx {
                    self.remove_cell(x + rx, y + ry, z + rz, key_id)?;
                }
            }
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

    #[test]
    fn get_on_untouched_world_is_none() {
        let dir = TempDir::new("untouched");
        let mut w = World::open(&dir).unwrap();

        // Key never interned anywhere in the world.
        assert_eq!(w.get(0, 0, 0, "material").unwrap(), None);
        assert_eq!(w.schema_len(), 0);
    }

    #[test]
    fn set_then_get_roundtrips_each_value_type() {
        let dir = TempDir::new("set-get-types");
        let mut w = World::open(&dir).unwrap();

        w.set(1, 2, 3, "material", Value::Str("stone".into()))
            .unwrap();
        w.set(1, 2, 3, "density", Value::F64(2.5)).unwrap();
        w.set(1, 2, 3, "hardness", Value::I64(7)).unwrap();

        assert_eq!(
            w.get(1, 2, 3, "material").unwrap(),
            Some(Value::Str("stone".into()))
        );
        assert_eq!(w.get(1, 2, 3, "density").unwrap(), Some(Value::F64(2.5)));
        assert_eq!(w.get(1, 2, 3, "hardness").unwrap(), Some(Value::I64(7)));
    }

    #[test]
    fn set_overwrites_previous_value_at_same_cell_and_key() {
        let dir = TempDir::new("overwrite");
        let mut w = World::open(&dir).unwrap();

        w.set(9, 9, 9, "material", Value::Str("stone".into()))
            .unwrap();
        w.set(9, 9, 9, "material", Value::Str("air".into()))
            .unwrap();

        assert_eq!(
            w.get(9, 9, 9, "material").unwrap(),
            Some(Value::Str("air".into()))
        );
    }

    #[test]
    fn set_on_one_cell_does_not_affect_neighbors_or_other_keys() {
        let dir = TempDir::new("isolation");
        let mut w = World::open(&dir).unwrap();

        w.set(0, 0, 0, "material", Value::Str("stone".into()))
            .unwrap();

        // Same key, different cell in the same chunk: untouched.
        assert_eq!(w.get(1, 0, 0, "material").unwrap(), None);
        // Same cell, different key: untouched.
        assert_eq!(w.get(0, 0, 0, "density").unwrap(), None);
    }

    #[test]
    fn set_across_multiple_chunks() {
        let dir = TempDir::new("multi-chunk");
        let mut w = World::open(&dir).unwrap();

        // CHUNK_DIM cells apart in x guarantees these land in different chunks.
        let far = CHUNK_DIM;
        w.set(0, 0, 0, "material", Value::Str("stone".into()))
            .unwrap();
        w.set(far, 0, 0, "material", Value::Str("air".into()))
            .unwrap();

        assert_eq!(
            w.get(0, 0, 0, "material").unwrap(),
            Some(Value::Str("stone".into()))
        );
        assert_eq!(
            w.get(far, 0, 0, "material").unwrap(),
            Some(Value::Str("air".into()))
        );
    }

    #[test]
    fn interning_a_new_key_grows_the_schema() {
        let dir = TempDir::new("schema-growth");
        let mut w = World::open(&dir).unwrap();
        assert_eq!(w.schema_len(), 0);

        w.set(0, 0, 0, "material", Value::Str("stone".into()))
            .unwrap();
        assert_eq!(w.schema_len(), 1);

        // Reusing the same key does not add another schema entry.
        w.set(1, 1, 1, "material", Value::Str("air".into()))
            .unwrap();
        assert_eq!(w.schema_len(), 1);

        w.set(0, 0, 0, "density", Value::F64(1.0)).unwrap();
        assert_eq!(w.schema_len(), 2);
    }

    #[test]
    fn set_persists_across_flush_and_reopen() {
        let dir = TempDir::new("set-persist");
        {
            let mut w = World::open(&dir).unwrap();
            w.set(100, 200, 300, "material", Value::Str("stone".into()))
                .unwrap();
            w.set(100, 200, 300, "density", Value::F64(2.5)).unwrap();
            w.flush().unwrap();
        }

        let mut w = World::open(&dir).unwrap();
        assert_eq!(
            w.get(100, 200, 300, "material").unwrap(),
            Some(Value::Str("stone".into()))
        );
        assert_eq!(
            w.get(100, 200, 300, "density").unwrap(),
            Some(Value::F64(2.5))
        );
        // The schema (key registry) is durable too.
        assert_eq!(w.schema_len(), 2);
    }

    #[test]
    fn remove_clears_cell_in_same_session() {
        let dir = TempDir::new("same-session");
        let mut w = World::open(&dir).unwrap();

        w.set(1, 2, 3, "material", Value::Str("stone".into()))
            .unwrap();
        assert_eq!(
            w.get(1, 2, 3, "material").unwrap(),
            Some(Value::Str("stone".into()))
        );

        w.remove(1, 2, 3, "material").unwrap();
        assert_eq!(w.get(1, 2, 3, "material").unwrap(), None);
    }

    #[test]
    fn remove_of_unset_key_is_a_harmless_noop() {
        let dir = TempDir::new("noop");
        let mut w = World::open(&dir).unwrap();

        // Key was never interned anywhere in the world.
        w.remove(4, 5, 6, "nonexistent").unwrap();
        assert_eq!(w.get(4, 5, 6, "nonexistent").unwrap(), None);

        // Key exists in the schema, but not on this particular cell.
        w.set(4, 5, 6, "material", Value::I64(1)).unwrap();
        w.remove(7, 8, 9, "material").unwrap();
        assert_eq!(w.get(4, 5, 6, "material").unwrap(), Some(Value::I64(1)));
    }

    #[test]
    fn remove_persists_across_flush_and_reopen() {
        let dir = TempDir::new("persist");
        {
            let mut w = World::open(&dir).unwrap();
            w.set(100, 200, 300, "material", Value::Str("stone".into()))
                .unwrap();
            w.remove(100, 200, 300, "material").unwrap();
            w.flush().unwrap();
        }

        let mut w = World::open(&dir).unwrap();
        assert_eq!(w.get(100, 200, 300, "material").unwrap(), None);
    }

    #[test]
    fn removing_every_cell_in_a_chunk_deletes_its_file() {
        let dir = TempDir::new("empty-chunk-gc");
        {
            let mut w = World::open(&dir).unwrap();
            w.set(0, 0, 0, "material", Value::Str("stone".into()))
                .unwrap();
            w.flush().unwrap();
        }
        let (ckey, _) = World::split(0, 0, 0);
        let chunk_path;
        {
            let w = World::open(&dir).unwrap();
            chunk_path = w.chunk_path(ckey);
        }
        assert!(
            chunk_path.exists(),
            "chunk file should exist once populated"
        );

        let mut w = World::open(&dir).unwrap();
        w.remove(0, 0, 0, "material").unwrap();
        w.flush().unwrap();
        assert!(
            !chunk_path.exists(),
            "emptied chunk file should be cleaned up"
        );
    }

    /// `n` copies of `value`, for tests that don't care about per-cell
    /// variation and just want to fill a region uniformly.
    fn fill(region: Region, value: Value) -> Vec<Value> {
        vec![value; region.volume() as usize]
    }

    #[test]
    fn get_region_on_untouched_world_is_all_none() {
        let dir = TempDir::new("region-untouched");
        let mut w = World::open(&dir).unwrap();

        let region = Region::new(5, 5, 5, 3, 2, 2);
        let got = w.get_region(region, "material").unwrap();
        assert_eq!(got.len(), 3 * 2 * 2);
        assert!(got.iter().all(Option::is_none));
    }

    #[test]
    fn set_region_then_get_region_roundtrips_within_one_chunk() {
        let dir = TempDir::new("region-roundtrip");
        let mut w = World::open(&dir).unwrap();

        let region = Region::new(1, 1, 1, 4, 3, 2);
        w.set_region(
            region,
            "material",
            &fill(region, Value::Str("stone".into())),
        )
        .unwrap();

        let got = w.get_region(region, "material").unwrap();
        assert_eq!(got.len(), 4 * 3 * 2);
        assert!(got.iter().all(|v| *v == Some(Value::Str("stone".into()))));
    }

    #[test]
    fn set_region_writes_distinct_per_cell_values() {
        let dir = TempDir::new("region-per-cell");
        let mut w = World::open(&dir).unwrap();

        let region = Region::new(10, 20, 30, 3, 2, 2);
        let values: Vec<Value> = (0..region.volume())
            .map(|i| Value::I64(i as i64 * 7))
            .collect();
        w.set_region(region, "n", &values).unwrap();

        let got = w.get_region(region, "n").unwrap();
        for (i, v) in values.iter().enumerate() {
            assert_eq!(got[i], Some(v.clone()));
        }
    }

    #[test]
    fn set_region_rejects_a_mismatched_value_count() {
        let dir = TempDir::new("region-bad-count");
        let mut w = World::open(&dir).unwrap();

        let region = Region::new(0, 0, 0, 2, 2, 2); // volume 8
        let too_few = vec![Value::I64(0); 7];
        let err = w.set_region(region, "n", &too_few).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);

        // The rejected call must not have written anything.
        let got = w.get_region(region, "n").unwrap();
        assert!(got.iter().all(Option::is_none));
    }

    #[test]
    fn set_region_does_not_leak_outside_its_bounds() {
        let dir = TempDir::new("region-bounds");
        let mut w = World::open(&dir).unwrap();

        let region = Region::new(10, 10, 10, 2, 2, 2);
        w.set_region(
            region,
            "material",
            &fill(region, Value::Str("stone".into())),
        )
        .unwrap();

        // One past the region on each axis: untouched.
        assert_eq!(w.get(12, 10, 10, "material").unwrap(), None);
        assert_eq!(w.get(10, 12, 10, "material").unwrap(), None);
        assert_eq!(w.get(10, 10, 12, "material").unwrap(), None);
        // Just inside on each axis: set.
        assert_eq!(
            w.get(11, 10, 10, "material").unwrap(),
            Some(Value::Str("stone".into()))
        );
    }

    #[test]
    fn get_region_orders_results_x_fastest_then_y_then_z() {
        let dir = TempDir::new("region-order");
        let mut w = World::open(&dir).unwrap();

        let (x0, y0, z0) = (10, 20, 30);
        let (dx, dy, dz) = (3u32, 2u32, 2u32);
        for rz in 0..dz {
            for ry in 0..dy {
                for rx in 0..dx {
                    let v = (rx as i64) + (ry as i64) * 100 + (rz as i64) * 10_000;
                    w.set(x0 + rx, y0 + ry, z0 + rz, "n", Value::I64(v))
                        .unwrap();
                }
            }
        }

        let got = w
            .get_region(Region::new(x0, y0, z0, dx, dy, dz), "n")
            .unwrap();
        for rz in 0..dz {
            for ry in 0..dy {
                for rx in 0..dx {
                    let idx = (rx + ry * dx + rz * dx * dy) as usize;
                    let expected = (rx as i64) + (ry as i64) * 100 + (rz as i64) * 10_000;
                    assert_eq!(got[idx], Some(Value::I64(expected)));
                }
            }
        }
    }

    #[test]
    fn region_spans_chunks_including_partial_chunks() {
        let dir = TempDir::new("region-span-chunks");
        let mut w = World::open(&dir).unwrap();

        // Starts 5 cells before a chunk boundary and ends 5 cells past the
        // next one, on every axis: covers the tail of one chunk, all of a
        // second, and the head of a third, on each axis.
        let x0 = CHUNK_DIM - 5;
        let d = CHUNK_DIM + 10;
        let region = Region::new(x0, x0, x0, d, d, d);

        w.set_region(
            region,
            "material",
            &fill(region, Value::Str("stone".into())),
        )
        .unwrap();
        w.flush().unwrap();

        // Reopen so the read exercises chunks freshly loaded from disk, not
        // just what's still resident in cache.
        let mut w = World::open(&dir).unwrap();
        let got = w.get_region(region, "material").unwrap();
        assert_eq!(got.len(), d as usize * d as usize * d as usize);
        assert!(got.iter().all(|v| *v == Some(Value::Str("stone".into()))));

        // Just outside the region on the low and high corners: untouched.
        assert_eq!(w.get(x0 - 1, x0 - 1, x0 - 1, "material").unwrap(), None);
        assert_eq!(w.get(x0 + d, x0 + d, x0 + d, "material").unwrap(), None);
    }

    #[test]
    fn remove_region_clears_key_across_chunks_without_touching_others() {
        let dir = TempDir::new("region-remove");
        let mut w = World::open(&dir).unwrap();

        let x0 = CHUNK_DIM - 2;
        let region = Region::new(x0, 0, 0, 5, 5, 5);
        w.set_region(
            region,
            "material",
            &fill(region, Value::Str("stone".into())),
        )
        .unwrap();
        w.set_region(region, "density", &fill(region, Value::F64(2.6)))
            .unwrap();

        w.remove_region(region, "material").unwrap();

        let material = w.get_region(region, "material").unwrap();
        assert!(material.iter().all(Option::is_none));
        // A different key on the same cells is untouched by the removal.
        let density = w.get_region(region, "density").unwrap();
        assert!(density.iter().all(|v| *v == Some(Value::F64(2.6))));
    }

    #[test]
    fn remove_region_of_unknown_key_is_a_harmless_noop() {
        let dir = TempDir::new("region-remove-unknown");
        let mut w = World::open(&dir).unwrap();
        // Should not error even though "material" has never been interned.
        w.remove_region(Region::new(0, 0, 0, 4, 4, 4), "material")
            .unwrap();
    }

    #[test]
    fn zero_sized_region_is_a_harmless_noop() {
        let dir = TempDir::new("region-zero");
        let mut w = World::open(&dir).unwrap();

        assert_eq!(
            w.get_region(Region::new(0, 0, 0, 0, 5, 5), "material")
                .unwrap(),
            vec![]
        );
        w.set_region(Region::new(0, 0, 0, 5, 0, 5), "material", &[])
            .unwrap();
        // A no-op set_region shouldn't even intern the key.
        assert_eq!(w.schema_len(), 0);
        w.remove_region(Region::new(0, 0, 0, 5, 5, 0), "material")
            .unwrap();
    }

    #[test]
    fn region_out_of_world_bounds_is_an_error() {
        let dir = TempDir::new("region-oob");
        let mut w = World::open(&dir).unwrap();

        assert_eq!(
            w.get_region(Region::new(WORLD_DIM - 1, 0, 0, 2, 1, 1), "material")
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
        // Overflowing u32 entirely must not panic or wrap around.
        assert_eq!(
            w.set_region(
                Region::new(u32::MAX - 1, 0, 0, 5, 1, 1),
                "material",
                &vec![Value::I64(0); 5],
            )
            .unwrap_err()
            .kind(),
            io::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn set_region_persists_across_flush_and_reopen() {
        let dir = TempDir::new("region-persist");
        let region = Region::new(100, 100, 100, 3, 3, 3);
        {
            let mut w = World::open(&dir).unwrap();
            w.set_region(
                region,
                "material",
                &fill(region, Value::Str("stone".into())),
            )
            .unwrap();
            w.flush().unwrap();
        }

        let mut w = World::open(&dir).unwrap();
        let got = w.get_region(region, "material").unwrap();
        assert!(got.iter().all(|v| *v == Some(Value::Str("stone".into()))));
    }
}
