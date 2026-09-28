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
pub const CHUNKS_PER_AXIS: u32 = (WORLD_DIM + CHUNK_DIM - 1) / CHUNK_DIM;

type ChunkKey = (u32, u32, u32);

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
        debug_assert!(x < WORLD_DIM && y < WORLD_DIM && z < WORLD_DIM, "coordinate out of range");
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
        self.cache.get_mut(&ckey).unwrap().set(local_idx, key_id, value);
        self.dirty.insert(ckey);
        Ok(())
    }

    pub fn remove(&mut self, x: u32, y: u32, z: u32, key: &str) -> io::Result<()> {
        let key_id = match self.schema.id_for_key(key) {
            Some(id) => id,
            None => return Ok(()),
        };
        let (ckey, local_idx) = Self::split(x, y, z);
        self.load_chunk(ckey)?;
        self.cache.get_mut(&ckey).unwrap().remove(local_idx, key_id);
        self.dirty.insert(ckey);
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
            let path = std::env::temp_dir().join(format!(
                "kdb-world-test-{tag}-{}-{n}",
                std::process::id()
            ));
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

        w.set(1, 2, 3, "material", Value::Str("stone".into())).unwrap();
        w.set(1, 2, 3, "density", Value::F64(2.5)).unwrap();
        w.set(1, 2, 3, "hardness", Value::I64(7)).unwrap();

        assert_eq!(w.get(1, 2, 3, "material").unwrap(), Some(Value::Str("stone".into())));
        assert_eq!(w.get(1, 2, 3, "density").unwrap(), Some(Value::F64(2.5)));
        assert_eq!(w.get(1, 2, 3, "hardness").unwrap(), Some(Value::I64(7)));
    }

    #[test]
    fn set_overwrites_previous_value_at_same_cell_and_key() {
        let dir = TempDir::new("overwrite");
        let mut w = World::open(&dir).unwrap();

        w.set(9, 9, 9, "material", Value::Str("stone".into())).unwrap();
        w.set(9, 9, 9, "material", Value::Str("air".into())).unwrap();

        assert_eq!(w.get(9, 9, 9, "material").unwrap(), Some(Value::Str("air".into())));
    }

    #[test]
    fn set_on_one_cell_does_not_affect_neighbors_or_other_keys() {
        let dir = TempDir::new("isolation");
        let mut w = World::open(&dir).unwrap();

        w.set(0, 0, 0, "material", Value::Str("stone".into())).unwrap();

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
        w.set(0, 0, 0, "material", Value::Str("stone".into())).unwrap();
        w.set(far, 0, 0, "material", Value::Str("air".into())).unwrap();

        assert_eq!(w.get(0, 0, 0, "material").unwrap(), Some(Value::Str("stone".into())));
        assert_eq!(w.get(far, 0, 0, "material").unwrap(), Some(Value::Str("air".into())));
    }

    #[test]
    fn interning_a_new_key_grows_the_schema() {
        let dir = TempDir::new("schema-growth");
        let mut w = World::open(&dir).unwrap();
        assert_eq!(w.schema_len(), 0);

        w.set(0, 0, 0, "material", Value::Str("stone".into())).unwrap();
        assert_eq!(w.schema_len(), 1);

        // Reusing the same key does not add another schema entry.
        w.set(1, 1, 1, "material", Value::Str("air".into())).unwrap();
        assert_eq!(w.schema_len(), 1);

        w.set(0, 0, 0, "density", Value::F64(1.0)).unwrap();
        assert_eq!(w.schema_len(), 2);
    }

    #[test]
    fn set_persists_across_flush_and_reopen() {
        let dir = TempDir::new("set-persist");
        {
            let mut w = World::open(&dir).unwrap();
            w.set(100, 200, 300, "material", Value::Str("stone".into())).unwrap();
            w.set(100, 200, 300, "density", Value::F64(2.5)).unwrap();
            w.flush().unwrap();
        }

        let mut w = World::open(&dir).unwrap();
        assert_eq!(w.get(100, 200, 300, "material").unwrap(), Some(Value::Str("stone".into())));
        assert_eq!(w.get(100, 200, 300, "density").unwrap(), Some(Value::F64(2.5)));
        // The schema (key registry) is durable too.
        assert_eq!(w.schema_len(), 2);
    }

    #[test]
    fn remove_clears_cell_in_same_session() {
        let dir = TempDir::new("same-session");
        let mut w = World::open(&dir).unwrap();

        w.set(1, 2, 3, "material", Value::Str("stone".into())).unwrap();
        assert_eq!(w.get(1, 2, 3, "material").unwrap(), Some(Value::Str("stone".into())));

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
            w.set(100, 200, 300, "material", Value::Str("stone".into())).unwrap();
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
            w.set(0, 0, 0, "material", Value::Str("stone".into())).unwrap();
            w.flush().unwrap();
        }
        let (ckey, _) = World::split(0, 0, 0);
        let chunk_path;
        {
            let w = World::open(&dir).unwrap();
            chunk_path = w.chunk_path(ckey);
        }
        assert!(chunk_path.exists(), "chunk file should exist once populated");

        let mut w = World::open(&dir).unwrap();
        w.remove(0, 0, 0, "material").unwrap();
        w.flush().unwrap();
        assert!(!chunk_path.exists(), "emptied chunk file should be cleaned up");
    }
}
