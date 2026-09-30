use crate::value::ValueType;
use std::collections::HashMap;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

/// Global key-string <-> integer-id registry for the whole world.
///
/// Storing "temperature" as a 4-byte id instead of an 11-byte string in every
/// one of a trillion cells is most of the win in this design: interning turns
/// "give every cell its own hashmap of strings" into "look up one of a small
/// number of columns by a small integer id".
///
/// The mapping is append-only and persisted as a plain `id\tkey\ttype` text
/// file (`schema.txt`) at the world root, so ids never move and old chunk
/// files stay valid no matter how many new keys get introduced later.
///
/// **A key's type is fixed the first time it's ever set, for the life of
/// the world.** `intern` records it then and rejects any later `set` for
/// that key with a different type -- world-wide, not just within one
/// chunk. (Before this, that check happened lazily and per-chunk, in
/// `Chunk::set`: two `set`s of the same key with different types could
/// each succeed if they landed in different chunks, only to panic later if
/// a third one collided with either -- surprising and inconsistent
/// depending on which chunk a coordinate happened to fall in. Recording
/// the type here instead means every `set` is checked against one
/// world-wide answer, consistently, and a mismatch is a normal
/// `InvalidInput` error, not a panic.)
///
/// **Concurrency.** Exactly one `Schema` is ever open against a world's
/// `schema.txt` at a time (see `World`'s "Concurrency" doc comment) --
/// `World` keeps it behind a `Mutex`, so every thread's `get`/`intern`
/// funnels through this same in-memory instance, one at a time.
/// `key_to_id`/`id_to_key`/`id_to_type` are therefore never stale: nothing
/// else can append to `schema.txt` behind this instance's back, so `load`
/// only needs to run once, at `open`, to pick up whatever an earlier
/// process run already persisted.
pub struct Schema {
    path: PathBuf,
    key_to_id: HashMap<String, u32>,
    id_to_key: Vec<String>,
    id_to_type: Vec<ValueType>,
}

impl Schema {
    pub fn open(world_root: &Path) -> io::Result<Self> {
        let mut schema = Schema {
            path: world_root.join("schema.txt"),
            key_to_id: HashMap::new(),
            id_to_key: Vec::new(),
            id_to_type: Vec::new(),
        };
        schema.load()?;
        Ok(schema)
    }

    /// Reads `schema.txt` from disk, if it exists, into this `Schema`'s
    /// in-memory state -- called once, by `open`. Nothing else needs to
    /// call this afterward: see `Schema`'s "Concurrency" doc comment for
    /// why the in-memory state can never fall behind the file past this
    /// first load.
    fn load(&mut self) -> io::Result<()> {
        if !self.path.exists() {
            return Ok(()); // nothing interned by anyone, ever, yet
        }
        let text = fs::read_to_string(&self.path)?;
        // Re-parse from scratch every time rather than trying to resume
        // from wherever we last left off: schema.txt is small (one line
        // per distinct key *ever used in the whole world*, not per cell),
        // so this stays cheap, and "just re-read the whole thing" is far
        // more obviously correct than an incremental-resume scheme would
        // be.
        let mut key_to_id = HashMap::with_capacity(self.id_to_key.len());
        let mut id_to_key = Vec::with_capacity(self.id_to_key.len());
        let mut id_to_type = Vec::with_capacity(self.id_to_key.len());
        for line in text.lines() {
            if line.is_empty() {
                continue;
            }
            let mut parts = line.splitn(3, '\t');
            let id_str = parts
                .next()
                .expect("corrupt schema.txt: expected 'id<TAB>key<TAB>type' per line");
            let key = parts
                .next()
                .expect("corrupt schema.txt: expected 'id<TAB>key<TAB>type' per line");
            let type_str = parts
                .next()
                .expect("corrupt schema.txt: expected 'id<TAB>key<TAB>type' per line");
            let id: u32 = id_str.parse().expect("corrupt schema.txt: non-numeric id");
            let value_type = ValueType::parse(type_str)
                .unwrap_or_else(|| panic!("corrupt schema.txt: unknown type '{type_str}'"));
            assert_eq!(
                id as usize,
                id_to_key.len(),
                "corrupt schema.txt: ids must be dense and in order"
            );
            id_to_key.push(key.to_string());
            id_to_type.push(value_type);
            key_to_id.insert(key.to_string(), id);
        }
        self.key_to_id = key_to_id;
        self.id_to_key = id_to_key;
        self.id_to_type = id_to_type;
        Ok(())
    }

    /// Looks up `key`'s id, if it's been interned. No disk access -- see
    /// the "Concurrency" note on `Schema` for why the in-memory map is
    /// always complete.
    pub fn id_for_key(&self, key: &str) -> Option<u32> {
        self.key_to_id.get(key).copied()
    }

    /// The inverse of `id_for_key`/`intern`: the key string `id` was
    /// interned for, or `None` if `id` was never issued by this `Schema`
    /// (e.g. a stale id from a different world).
    pub fn key_for_id(&self, id: u32) -> Option<&str> {
        self.id_to_key.get(id as usize).map(String::as_str)
    }

    pub fn len(&self) -> usize {
        self.id_to_key.len()
    }

    pub fn is_empty(&self) -> bool {
        self.id_to_key.is_empty()
    }

    /// Look up `key`'s id, interning (and durably persisting) a new one --
    /// permanently fixing its type to `value_type` -- if this is the first
    /// time this world has ever seen it. If `key` is already known,
    /// `value_type` must match what it was first interned with, or this
    /// returns an `InvalidInput` error instead of an id (see `Schema`'s
    /// doc comment).
    pub fn intern(&mut self, key: &str, value_type: ValueType) -> io::Result<u32> {
        if let Some(&id) = self.key_to_id.get(key) {
            return self.check_type(id, key, value_type).map(|()| id);
        }

        let id = self.id_to_key.len() as u32;

        // Append-only on disk too: we never rewrite schema.txt, only add
        // to it. One write_all call (not e.g. writeln!, which could split
        // across several small writes to an unbuffered File) so the
        // append is a single, atomically-visible block from any
        // concurrent reader's point of view.
        let mut f = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        f.write_all(format!("{id}\t{key}\t{}\n", value_type.as_str()).as_bytes())?;

        self.id_to_key.push(key.to_string());
        self.id_to_type.push(value_type);
        self.key_to_id.insert(key.to_string(), id);
        Ok(id)
    }

    /// `key` (already interned as `id`) must have been first interned with
    /// `value_type`, or this is the caller's bug/bad input, not ours to
    /// silently allow -- see `Schema`'s doc comment.
    fn check_type(&self, id: u32, key: &str, value_type: ValueType) -> io::Result<()> {
        let existing = self.id_to_type[id as usize];
        if existing == value_type {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "key '{key}' already holds {} values; can't set a {} value for it \
                     (a key's value type is fixed the first time it's used)",
                    existing.as_str(),
                    value_type.as_str()
                ),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// A scratch directory under the OS temp dir, unique per test, removed
    /// when it goes out of scope -- same pattern as `world.rs`'s own
    /// `TempDir`, kept separate since each file's tests own their fixtures.
    struct TempDir(PathBuf);

    impl TempDir {
        fn new(tag: &str) -> Self {
            static COUNTER: AtomicU64 = AtomicU64::new(0);
            let n = COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "kblockdblib-schema-test-{tag}-{}-{n}",
                std::process::id()
            ));
            fs::create_dir_all(&path).unwrap();
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
    fn intern_assigns_dense_sequential_ids_and_is_idempotent() {
        let dir = TempDir::new("dense-ids");
        let mut schema = Schema::open(dir.as_ref()).unwrap();
        assert_eq!(schema.intern("a", ValueType::Str).unwrap(), 0);
        assert_eq!(schema.intern("b", ValueType::I64).unwrap(), 1);
        assert_eq!(schema.intern("a", ValueType::Str).unwrap(), 0);
        assert_eq!(schema.len(), 2);
    }

    #[test]
    fn different_keys_can_have_different_types() {
        let dir = TempDir::new("different-types");
        let mut schema = Schema::open(dir.as_ref()).unwrap();
        schema.intern("material", ValueType::Str).unwrap();
        schema.intern("hardness", ValueType::I64).unwrap();
        schema.intern("temperature", ValueType::F64).unwrap();
        assert_eq!(schema.len(), 3);
    }

    #[test]
    fn intern_rejects_a_different_type_for_an_already_interned_key() {
        let dir = TempDir::new("type-mismatch");
        let mut schema = Schema::open(dir.as_ref()).unwrap();
        schema.intern("temperature", ValueType::F64).unwrap();

        let err = schema.intern("temperature", ValueType::I64).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(err.to_string().contains("temperature"));

        // Rejected, and the original type is still what's recorded -- a
        // failed attempt to change it doesn't corrupt or overwrite it.
        assert!(schema.intern("temperature", ValueType::F64).is_ok());
    }

    #[test]
    fn a_keys_type_is_enforced_even_in_a_freshly_reopened_schema() {
        let dir = TempDir::new("type-persists");
        {
            let mut schema = Schema::open(dir.as_ref()).unwrap();
            schema.intern("material", ValueType::Str).unwrap();
        }

        // A brand new `Schema` over the same directory, so this can only
        // know about "material"'s type by having read it back from
        // schema.txt, not from any in-memory state carried over.
        let mut schema = Schema::open(dir.as_ref()).unwrap();
        assert!(schema.intern("material", ValueType::Str).is_ok());
        let err = schema.intern("material", ValueType::I64).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn id_for_key_finds_an_interned_key_without_touching_its_type() {
        let dir = TempDir::new("id-for-key");
        let mut schema = Schema::open(dir.as_ref()).unwrap();
        let id = schema.intern("material", ValueType::Str).unwrap();
        assert_eq!(schema.id_for_key("material"), Some(id));
    }

    #[test]
    fn id_for_key_of_a_never_interned_key_is_none() {
        let dir = TempDir::new("id-for-key-miss");
        let schema = Schema::open(dir.as_ref()).unwrap();
        assert_eq!(schema.id_for_key("nonexistent"), None);
    }

    #[test]
    fn key_for_id_is_the_inverse_of_id_for_key() {
        let dir = TempDir::new("key-for-id");
        let mut schema = Schema::open(dir.as_ref()).unwrap();
        let id = schema.intern("material", ValueType::Str).unwrap();
        assert_eq!(schema.key_for_id(id), Some("material"));
    }

    #[test]
    fn key_for_id_of_an_unissued_id_is_none() {
        let dir = TempDir::new("key-for-id-miss");
        let schema = Schema::open(dir.as_ref()).unwrap();
        assert_eq!(schema.key_for_id(0), None);
    }
}
