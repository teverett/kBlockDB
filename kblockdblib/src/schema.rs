use crate::value::ValueType;
use std::collections::HashMap;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

/// The 4th field marking a `schema.txt` line as a tombstone rather than a
/// new entry -- see `Schema`'s doc comment.
const REMOVED_MARKER: &str = "removed";

/// Rejects key names `schema.txt`'s one-line-per-key, tab-separated format
/// can't round-trip. Without this a key containing a tab or newline would
/// be written out happily and then either mis-parse or panic the *next*
/// time the world was opened -- a corruption that outlives the process
/// that caused it.
fn validate_key(key: &str) -> io::Result<()> {
    let problem = if key.is_empty() {
        "must not be empty"
    } else if key.contains('\t') {
        "must not contain a tab"
    } else if key.contains('\n') || key.contains('\r') {
        "must not contain a line break"
    } else {
        return Ok(());
    };
    Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("invalid key '{key}': a key {problem}"),
    ))
}

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
/// **Removal is a tombstone, never a rewrite.** `remove` appends an
/// `id\tkey\ttype\tremoved` line rather than deleting the original, so ids
/// stay dense and in order and a removed id is never reissued. Re-adding a
/// removed key gets a brand new id, which is what lets it come back with a
/// different type than it had before. Lines are matched to ids positionally
/// on load, so a file written before removal existed parses identically.
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
    /// Parallel to `id_to_key`: whether that id has been `remove`d. A
    /// removed id keeps its slot (ids must stay dense) but is gone from
    /// `key_to_id`, so it can never be looked up or reissued again.
    id_removed: Vec<bool>,
}

/// One live column in a world's schema: a key and the value type fixed for
/// it when it was created. Returned by `Schema::columns`/`World::columns`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnInfo {
    pub key: String,
    pub value_type: ValueType,
}

impl Schema {
    pub fn open(world_root: &Path) -> io::Result<Self> {
        let mut schema = Schema {
            path: world_root.join("schema.txt"),
            key_to_id: HashMap::new(),
            id_to_key: Vec::new(),
            id_to_type: Vec::new(),
            id_removed: Vec::new(),
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
        let mut id_removed = Vec::with_capacity(self.id_to_key.len());
        for line in text.lines() {
            if line.is_empty() {
                continue;
            }
            let mut parts = line.splitn(4, '\t');
            let id_str = parts
                .next()
                .expect("corrupt schema.txt: expected 'id<TAB>key<TAB>type' per line");
            let key = parts
                .next()
                .expect("corrupt schema.txt: expected 'id<TAB>key<TAB>type' per line");
            let type_str = parts
                .next()
                .expect("corrupt schema.txt: expected 'id<TAB>key<TAB>type' per line");
            // A 4th field marks a tombstone for an id written earlier in
            // the file (see `remove`); its absence is the original,
            // pre-removal line format, which still parses unchanged.
            let flag = parts.next();
            let id: u32 = id_str.parse().expect("corrupt schema.txt: non-numeric id");
            let value_type = ValueType::parse(type_str)
                .unwrap_or_else(|| panic!("corrupt schema.txt: unknown type '{type_str}'"));

            match flag {
                None => {
                    assert_eq!(
                        id as usize,
                        id_to_key.len(),
                        "corrupt schema.txt: ids must be dense and in order"
                    );
                    id_to_key.push(key.to_string());
                    id_to_type.push(value_type);
                    id_removed.push(false);
                    key_to_id.insert(key.to_string(), id);
                }
                Some(REMOVED_MARKER) => {
                    assert!(
                        (id as usize) < id_to_key.len(),
                        "corrupt schema.txt: removal of id {id}, which was never added"
                    );
                    id_removed[id as usize] = true;
                    // Only if this id is still the one that owns the key:
                    // the key may since have been re-added under a later
                    // id, whose mapping this removal must not clobber.
                    if key_to_id.get(key) == Some(&id) {
                        key_to_id.remove(key);
                    }
                }
                Some(other) => {
                    panic!("corrupt schema.txt: unknown flag '{other}' on id {id}")
                }
            }
        }
        self.key_to_id = key_to_id;
        self.id_to_key = id_to_key;
        self.id_to_type = id_to_type;
        self.id_removed = id_removed;
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
        if *self.id_removed.get(id as usize)? {
            // Data written under a removed id is unreachable by name --
            // reporting it as a live key would resurrect a dropped column.
            return None;
        }
        self.id_to_key.get(id as usize).map(String::as_str)
    }

    /// How many ids have ever been issued, removed ones included -- the
    /// exclusive upper bound for `key_for_id`. Distinct from `len`, which
    /// counts only live columns.
    pub fn id_space(&self) -> usize {
        self.id_to_key.len()
    }

    /// Every live (non-removed) column, sorted by key.
    pub fn columns(&self) -> Vec<ColumnInfo> {
        let mut columns: Vec<ColumnInfo> = self
            .key_to_id
            .iter()
            .map(|(key, &id)| ColumnInfo {
                key: key.clone(),
                value_type: self.id_to_type[id as usize],
            })
            .collect();
        columns.sort_by(|a, b| a.key.cmp(&b.key));
        columns
    }

    /// The value type fixed for `key`, or `None` if it isn't a live
    /// column.
    pub fn type_for_key(&self, key: &str) -> Option<ValueType> {
        self.key_to_id
            .get(key)
            .map(|&id| self.id_to_type[id as usize])
    }

    pub fn len(&self) -> usize {
        self.key_to_id.len()
    }

    pub fn is_empty(&self) -> bool {
        self.key_to_id.is_empty()
    }

    /// Creates `key` as a brand new column of type `value_type`, failing
    /// with `AlreadyExists` if it's already a live column -- unlike
    /// `intern`, which is get-or-create and happily returns the existing
    /// id. This is the explicit "add a column" operation: it fixes the
    /// key's type up front, before any cell has ever been written to it.
    ///
    /// A key that was previously `remove`d counts as absent, and gets a
    /// fresh id here -- so it may come back with a different type.
    pub fn add(&mut self, key: &str, value_type: ValueType) -> io::Result<u32> {
        if self.key_to_id.contains_key(key) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("key '{key}' already exists"),
            ));
        }
        self.intern(key, value_type)
    }

    /// Drops `key` from the schema, returning the id it had, or `None` if
    /// it wasn't a live column. The id is tombstoned, never reused or
    /// renumbered (see `Schema`'s doc comment).
    ///
    /// This is a schema-level operation only: it does nothing about cell
    /// data already written under the old id. `World::remove_column` is
    /// what pairs this with purging that data.
    pub fn remove(&mut self, key: &str) -> io::Result<Option<u32>> {
        let Some(id) = self.key_to_id.get(key).copied() else {
            return Ok(None);
        };
        let value_type = self.id_to_type[id as usize];

        // Persisted before the in-memory state changes, so a failure here
        // leaves the schema exactly as it was rather than dropping a
        // column only until the next restart.
        self.append_line(&format!(
            "{id}\t{key}\t{}\t{REMOVED_MARKER}\n",
            value_type.as_str()
        ))?;

        self.key_to_id.remove(key);
        self.id_removed[id as usize] = true;
        Ok(Some(id))
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
        validate_key(key)?;

        let id = self.id_to_key.len() as u32;
        self.append_line(&format!("{id}\t{key}\t{}\n", value_type.as_str()))?;

        self.id_to_key.push(key.to_string());
        self.id_to_type.push(value_type);
        self.id_removed.push(false);
        self.key_to_id.insert(key.to_string(), id);
        Ok(id)
    }

    /// Append-only on disk: we never rewrite schema.txt, only add to it.
    /// One `write_all` call (not e.g. `writeln!`, which could split across
    /// several small writes to an unbuffered `File`) so the append is a
    /// single, atomically-visible block from any concurrent reader's point
    /// of view.
    fn append_line(&self, line: &str) -> io::Result<()> {
        let mut f = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        f.write_all(line.as_bytes())
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

    // --- Columns: add and remove ---

    #[test]
    fn add_creates_a_column_with_a_fixed_type() {
        let dir = TempDir::new("add-column");
        let mut schema = Schema::open(dir.as_ref()).unwrap();
        let id = schema.add("material", ValueType::Str).unwrap();

        assert_eq!(schema.id_for_key("material"), Some(id));
        assert_eq!(schema.type_for_key("material"), Some(ValueType::Str));
        // The type is fixed by `add` alone, with no value ever written.
        let err = schema.intern("material", ValueType::I64).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn add_rejects_a_column_that_already_exists() {
        let dir = TempDir::new("add-duplicate");
        let mut schema = Schema::open(dir.as_ref()).unwrap();
        schema.add("material", ValueType::Str).unwrap();

        let err = schema.add("material", ValueType::Str).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
        // Same answer even when the requested type differs.
        let err = schema.add("material", ValueType::I64).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
        assert_eq!(schema.len(), 1);
    }

    #[test]
    fn add_rejects_a_key_the_file_format_cannot_hold() {
        let dir = TempDir::new("add-invalid-key");
        let mut schema = Schema::open(dir.as_ref()).unwrap();
        for bad in ["", "has\ttab", "has\nnewline", "has\rreturn"] {
            let err = schema.add(bad, ValueType::Str).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::InvalidInput, "for {bad:?}");
        }
        assert_eq!(schema.len(), 0);

        // And the rejected keys never reached schema.txt: reopening finds
        // a file that still parses.
        let schema = Schema::open(dir.as_ref()).unwrap();
        assert_eq!(schema.len(), 0);
    }

    #[test]
    fn intern_rejects_an_unstorable_key_too() {
        let dir = TempDir::new("intern-invalid-key");
        let mut schema = Schema::open(dir.as_ref()).unwrap();
        let err = schema.intern("has\ttab", ValueType::Str).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn remove_drops_a_column_and_reports_its_old_id() {
        let dir = TempDir::new("remove-column");
        let mut schema = Schema::open(dir.as_ref()).unwrap();
        let id = schema.add("material", ValueType::Str).unwrap();

        assert_eq!(schema.remove("material").unwrap(), Some(id));
        assert_eq!(schema.id_for_key("material"), None);
        assert_eq!(schema.type_for_key("material"), None);
        assert_eq!(schema.key_for_id(id), None);
        assert_eq!(schema.len(), 0);
    }

    #[test]
    fn remove_of_an_unknown_column_is_none_not_an_error() {
        let dir = TempDir::new("remove-missing");
        let mut schema = Schema::open(dir.as_ref()).unwrap();
        assert_eq!(schema.remove("nonexistent").unwrap(), None);
        // Removing twice is the same: the second call is a no-op.
        schema.add("material", ValueType::Str).unwrap();
        assert!(schema.remove("material").unwrap().is_some());
        assert_eq!(schema.remove("material").unwrap(), None);
    }

    #[test]
    fn a_removal_survives_a_reopen() {
        let dir = TempDir::new("remove-persists");
        let id = {
            let mut schema = Schema::open(dir.as_ref()).unwrap();
            schema.add("keep", ValueType::I64).unwrap();
            let id = schema.add("drop", ValueType::Str).unwrap();
            schema.remove("drop").unwrap();
            id
        };

        let schema = Schema::open(dir.as_ref()).unwrap();
        assert_eq!(schema.id_for_key("drop"), None);
        assert_eq!(schema.key_for_id(id), None);
        assert_eq!(schema.id_for_key("keep"), Some(0));
        assert_eq!(schema.len(), 1);
        // The tombstoned id still occupies its slot, so ids stay dense.
        assert_eq!(schema.id_space(), 2);
    }

    #[test]
    fn a_removed_key_comes_back_with_a_new_id_and_may_change_type() {
        let dir = TempDir::new("remove-readd");
        let mut schema = Schema::open(dir.as_ref()).unwrap();
        let first = schema.add("material", ValueType::Str).unwrap();
        schema.remove("material").unwrap();

        let second = schema.add("material", ValueType::I64).unwrap();
        assert_ne!(first, second, "a removed id must never be reissued");
        assert_eq!(schema.type_for_key("material"), Some(ValueType::I64));
        assert_eq!(schema.key_for_id(first), None);
        assert_eq!(schema.key_for_id(second), Some("material"));
    }

    #[test]
    fn a_re_added_key_survives_a_reopen_without_the_tombstone_winning() {
        // The tombstone line for the old id sits *before* the re-add line
        // in schema.txt, so replaying the file in order must leave the key
        // live, not removed.
        let dir = TempDir::new("remove-readd-reopen");
        {
            let mut schema = Schema::open(dir.as_ref()).unwrap();
            schema.add("material", ValueType::Str).unwrap();
            schema.remove("material").unwrap();
            schema.add("material", ValueType::Bool).unwrap();
        }

        let schema = Schema::open(dir.as_ref()).unwrap();
        assert_eq!(schema.id_for_key("material"), Some(1));
        assert_eq!(schema.type_for_key("material"), Some(ValueType::Bool));
        assert_eq!(schema.len(), 1);
    }

    #[test]
    fn a_schema_file_written_before_removals_existed_still_parses() {
        // The pre-removal format is three tab-separated fields with no
        // flag; it must keep loading exactly as it always did.
        let dir = TempDir::new("legacy-format");
        fs::write(
            dir.0.join("schema.txt"),
            "0\tmaterial\tstr\n1\thardness\ti64\n",
        )
        .unwrap();

        let schema = Schema::open(dir.as_ref()).unwrap();
        assert_eq!(schema.len(), 2);
        assert_eq!(schema.id_for_key("material"), Some(0));
        assert_eq!(schema.type_for_key("hardness"), Some(ValueType::I64));
    }

    #[test]
    fn columns_lists_live_columns_sorted_by_key() {
        let dir = TempDir::new("columns-list");
        let mut schema = Schema::open(dir.as_ref()).unwrap();
        schema.add("material", ValueType::Str).unwrap();
        schema.add("density", ValueType::F64).unwrap();
        schema.add("solid", ValueType::Bool).unwrap();
        schema.remove("material").unwrap();

        assert_eq!(
            schema.columns(),
            vec![
                ColumnInfo {
                    key: "density".to_string(),
                    value_type: ValueType::F64,
                },
                ColumnInfo {
                    key: "solid".to_string(),
                    value_type: ValueType::Bool,
                },
            ]
        );
    }

    #[test]
    fn columns_of_a_fresh_schema_is_empty() {
        let dir = TempDir::new("columns-empty");
        let schema = Schema::open(dir.as_ref()).unwrap();
        assert!(schema.columns().is_empty());
        assert!(schema.is_empty());
    }

    #[test]
    fn intern_recreates_a_removed_key_with_a_new_id() {
        // `set` goes through `intern`, not `add`, so a plain write to a
        // removed key must bring it back rather than fail.
        let dir = TempDir::new("intern-after-remove");
        let mut schema = Schema::open(dir.as_ref()).unwrap();
        let first = schema.intern("material", ValueType::Str).unwrap();
        schema.remove("material").unwrap();

        let second = schema.intern("material", ValueType::F64).unwrap();
        assert_ne!(first, second);
        assert_eq!(schema.type_for_key("material"), Some(ValueType::F64));
    }
}
