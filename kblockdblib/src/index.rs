//! A secondary, equality-only index from a key's value to the coordinates
//! holding it -- what lets `World::lookup_eq` answer "which cells have
//! `key == value`" in time proportional to the number of matches, instead
//! of `World::list_cells`'s full chunk-by-chunk decode of the whole world
//! (see `World::create_index`'s doc comment for the tradeoff this opts
//! into per key).
//!
//! Backed by `crate::lsm::LsmIndex`, one per indexed key, each at its own
//! directory (named by key id) under the world's `indexes/` directory --
//! not an in-memory map, so an index on a key most of a trillion-cell
//! world holds doesn't need to fit in RAM. See `lsm`'s own doc comment
//! for the on-disk shape.
//!
//! Only ever consulted/maintained through `World`, which is the only thing
//! that knows a cell's *old* value at the moment it's overwritten -- see
//! `record`, the single method every write/remove path reports a
//! before/after pair through.

use crate::coord::Coord;
use crate::lsm::LsmIndex;
use crate::value::Value;
use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// Subdirectory of a world's root holding one directory per indexed key
/// (named by its schema key id) -- see `ValueIndex::open`.
const INDEXES_DIR: &str = "indexes";

/// A `Value` as order-preserving bytes, for `LsmIndex`'s sorted on-disk
/// storage. Each encoding keeps `Value::PartialEq`'s notion of equality
/// (what matters for `lookup_eq`) and, as a bonus for free, orders values
/// the same way `kblockdbquery`'s `<`/`>` comparisons do -- not exploited
/// today (`LsmIndex` only ever gets exact-match lookups), but means this
/// encoding wouldn't need to change if range lookups were added later.
///
/// `f64`'s `to_bits` (not IEEE equality/ordering) is what makes a `Value`
/// hashable/orderable at all here, same reasoning as the old purely
/// in-memory version of this module -- two NaNs with the same bits are
/// "the same value" for indexing purposes, consistent with the fact that
/// nothing else in this crate claims IEEE NaN semantics either.
fn sortable_bytes(value: &Value) -> Vec<u8> {
    match value {
        Value::Str(s) => s.as_bytes().to_vec(),
        Value::Bool(b) => vec![u8::from(*b)],
        // Flips the sign bit: two's-complement ordering becomes plain
        // unsigned big-endian byte ordering, the standard trick for a
        // byte-comparable signed integer.
        Value::I64(n) => ((*n as u64) ^ (1u64 << 63)).to_be_bytes().to_vec(),
        // Standard IEEE-754 sortable-bytes trick: flip every bit for a
        // negative value (so larger magnitude sorts smaller, which is
        // what descending-looking negative floats need), or just set the
        // sign bit for a non-negative one.
        Value::F64(f) => {
            let bits = f.to_bits();
            let sortable = if bits >> 63 == 1 {
                !bits
            } else {
                bits | (1u64 << 63)
            };
            sortable.to_be_bytes().to_vec()
        }
    }
}

/// One `World`'s whole set of indexed keys, keyed by schema key id (not
/// key name -- consistent with how `Chunk` itself addresses columns).
///
/// Equality lookups only: there's no ordering exposed here (see
/// `sortable_bytes`'s doc comment on why the underlying encoding happens
/// to support it anyway), so `Lt`/`Gt`/etc. still need a full scan.
pub struct ValueIndex {
    root: PathBuf,
    open: Mutex<HashMap<u32, LsmIndex>>,
}

impl ValueIndex {
    /// Opens every secondary index already on disk under `root`'s
    /// `indexes/` directory (one subdirectory per indexed key id,
    /// discovered by listing it, not by reading a separate manifest) --
    /// each index's own segment files already *are* its persisted state,
    /// so this needs no `list_cells`-style rescan to restore them, unlike
    /// the very first `create_index` for a key.
    pub fn open(root: &Path) -> io::Result<ValueIndex> {
        let dir = root.join(INDEXES_DIR);
        let mut open = HashMap::new();
        if dir.exists() {
            for entry in std::fs::read_dir(&dir)? {
                let entry = entry?;
                if let Some(key_id) = entry
                    .file_name()
                    .to_str()
                    .and_then(|name| name.parse::<u32>().ok())
                {
                    open.insert(key_id, LsmIndex::open(&entry.path())?);
                }
            }
        }
        Ok(ValueIndex {
            root: root.to_path_buf(),
            open: Mutex::new(open),
        })
    }

    /// Whether `key_id` currently has an index built for it. No disk
    /// access -- a plain in-memory registry check.
    pub fn is_indexed(&self, key_id: u32) -> bool {
        self.open.lock().unwrap().contains_key(&key_id)
    }

    /// Every key id currently indexed, in no particular order.
    pub fn indexed_key_ids(&self) -> Vec<u32> {
        self.open.lock().unwrap().keys().copied().collect()
    }

    /// Starts tracking `key_id`: creates its on-disk directory and an
    /// empty `LsmIndex` there. The caller (`World::create_index`) is
    /// responsible for backfilling it from existing data, since this
    /// module has no access to chunk storage. A no-op, not an error, if
    /// `key_id` is already indexed (its existing contents are left
    /// alone).
    pub fn create(&self, key_id: u32) -> io::Result<()> {
        let mut open = self.open.lock().unwrap();
        if open.contains_key(&key_id) {
            return Ok(());
        }
        let lsm = LsmIndex::create(&self.root.join(INDEXES_DIR).join(key_id.to_string()))?;
        open.insert(key_id, lsm);
        Ok(())
    }

    /// Stops tracking `key_id` and deletes its on-disk directory. Returns
    /// whether it was indexed.
    pub fn drop_index(&self, key_id: u32) -> io::Result<bool> {
        let mut open = self.open.lock().unwrap();
        if open.remove(&key_id).is_none() {
            return Ok(false);
        }
        std::fs::remove_dir_all(self.root.join(INDEXES_DIR).join(key_id.to_string()))?;
        Ok(true)
    }

    /// Records that `coord`'s value under `key_id` changed from `old` to
    /// `new` (either side `None` meaning "wasn't/isn't set"). A no-op if
    /// `key_id` isn't indexed -- callers aren't expected to check
    /// `is_indexed` first, so every write path can call this
    /// unconditionally without an extra branch.
    pub fn record(
        &self,
        key_id: u32,
        coord: &Coord,
        old: Option<&Value>,
        new: Option<&Value>,
    ) -> io::Result<()> {
        let mut open = self.open.lock().unwrap();
        let Some(lsm) = open.get_mut(&key_id) else {
            return Ok(());
        };
        let coord_vec = coord.to_vec();
        if let Some(old) = old {
            lsm.remove(sortable_bytes(old), coord_vec.clone())?;
        }
        if let Some(new) = new {
            lsm.insert(sortable_bytes(new), coord_vec)?;
        }
        Ok(())
    }

    /// Every coordinate currently holding `value` under `key_id`, or
    /// `None` if `key_id` isn't indexed (the caller should fall back to a
    /// full scan). An indexed key with no cell holding `value` returns
    /// `Some(vec![])`, not `None` -- that's a real, authoritative answer
    /// ("zero matches"), not "I don't know".
    pub fn lookup_eq(&self, key_id: u32, value: &Value) -> io::Result<Option<Vec<Coord>>> {
        let open = self.open.lock().unwrap();
        let Some(lsm) = open.get(&key_id) else {
            return Ok(None);
        };
        let coords = lsm
            .lookup(&sortable_bytes(value))?
            .into_iter()
            .map(Coord::from)
            .collect();
        Ok(Some(coords))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    struct TempDir(PathBuf);

    impl TempDir {
        fn new(tag: &str) -> Self {
            static COUNTER: AtomicU64 = AtomicU64::new(0);
            let n = COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "kblockdblib-index-test-{tag}-{}-{n}",
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
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn coord(parts: &[i32]) -> Coord {
        Coord::from(parts.to_vec())
    }

    #[test]
    fn unindexed_key_reports_not_indexed_and_records_nothing() {
        let dir = TempDir::new("unindexed");
        let index = ValueIndex::open(dir.as_ref()).unwrap();
        assert!(!index.is_indexed(1));
        index
            .record(1, &coord(&[0, 0, 0]), None, Some(&Value::I64(5)))
            .unwrap();
        assert_eq!(index.lookup_eq(1, &Value::I64(5)).unwrap(), None);
    }

    #[test]
    fn create_then_record_then_lookup_roundtrips() {
        let dir = TempDir::new("roundtrip");
        let index = ValueIndex::open(dir.as_ref()).unwrap();
        index.create(7).unwrap();
        assert!(index.is_indexed(7));
        index
            .record(7, &coord(&[1, 2, 3]), None, Some(&Value::Str("stone".into())))
            .unwrap();
        index
            .record(7, &coord(&[4, 5, 6]), None, Some(&Value::Str("stone".into())))
            .unwrap();
        index
            .record(7, &coord(&[9, 9, 9]), None, Some(&Value::Str("dirt".into())))
            .unwrap();

        let mut stone = index.lookup_eq(7, &Value::Str("stone".into())).unwrap().unwrap();
        stone.sort_by(|a, b| a.iter().cmp(b.iter()));
        assert_eq!(stone, vec![coord(&[1, 2, 3]), coord(&[4, 5, 6])]);

        assert_eq!(
            index.lookup_eq(7, &Value::Str("dirt".into())).unwrap(),
            Some(vec![coord(&[9, 9, 9])])
        );
        assert_eq!(
            index.lookup_eq(7, &Value::Str("lava".into())).unwrap(),
            Some(vec![])
        );
    }

    #[test]
    fn overwrite_moves_coord_between_values() {
        let dir = TempDir::new("overwrite");
        let index = ValueIndex::open(dir.as_ref()).unwrap();
        index.create(1).unwrap();
        let c = coord(&[0, 0, 0]);
        index.record(1, &c, None, Some(&Value::I64(1))).unwrap();
        index
            .record(1, &c, Some(&Value::I64(1)), Some(&Value::I64(2)))
            .unwrap();

        assert_eq!(index.lookup_eq(1, &Value::I64(1)).unwrap(), Some(vec![]));
        assert_eq!(index.lookup_eq(1, &Value::I64(2)).unwrap(), Some(vec![c]));
    }

    #[test]
    fn removal_clears_the_coord_from_its_old_value() {
        let dir = TempDir::new("removal");
        let index = ValueIndex::open(dir.as_ref()).unwrap();
        index.create(1).unwrap();
        let c = coord(&[0, 0, 0]);
        index.record(1, &c, None, Some(&Value::Bool(true))).unwrap();
        index.record(1, &c, Some(&Value::Bool(true)), None).unwrap();

        assert_eq!(index.lookup_eq(1, &Value::Bool(true)).unwrap(), Some(vec![]));
    }

    #[test]
    fn drop_index_stops_tracking_and_forgets_contents() {
        let dir = TempDir::new("drop");
        let index = ValueIndex::open(dir.as_ref()).unwrap();
        index.create(1).unwrap();
        index
            .record(1, &coord(&[0, 0, 0]), None, Some(&Value::I64(1)))
            .unwrap();
        assert!(index.drop_index(1).unwrap());
        assert!(!index.is_indexed(1));
        assert_eq!(index.lookup_eq(1, &Value::I64(1)).unwrap(), None);
        assert!(!index.drop_index(1).unwrap());
    }

    #[test]
    fn nan_is_indexed_by_bit_pattern_not_ieee_equality() {
        let dir = TempDir::new("nan");
        let index = ValueIndex::open(dir.as_ref()).unwrap();
        index.create(1).unwrap();
        let c = coord(&[0, 0, 0]);
        index
            .record(1, &c, None, Some(&Value::F64(f64::NAN)))
            .unwrap();
        assert_eq!(
            index.lookup_eq(1, &Value::F64(f64::NAN)).unwrap(),
            Some(vec![c])
        );
    }

    #[test]
    fn an_index_survives_closing_and_reopening() {
        let dir = TempDir::new("reopen");
        {
            let index = ValueIndex::open(dir.as_ref()).unwrap();
            index.create(1).unwrap();
            index
                .record(1, &coord(&[1, 2, 3]), None, Some(&Value::Str("stone".into())))
                .unwrap();
        }
        let index = ValueIndex::open(dir.as_ref()).unwrap();
        assert!(index.is_indexed(1));
        assert_eq!(
            index.lookup_eq(1, &Value::Str("stone".into())).unwrap(),
            Some(vec![coord(&[1, 2, 3])])
        );
    }

    #[test]
    fn a_dropped_index_does_not_come_back_on_reopen() {
        let dir = TempDir::new("drop-reopen");
        {
            let index = ValueIndex::open(dir.as_ref()).unwrap();
            index.create(1).unwrap();
            index.drop_index(1).unwrap();
        }
        let index = ValueIndex::open(dir.as_ref()).unwrap();
        assert!(!index.is_indexed(1));
        assert_eq!(index.indexed_key_ids(), Vec::<u32>::new());
    }

    #[test]
    fn multiple_keys_are_independent() {
        let dir = TempDir::new("multiple-keys");
        let index = ValueIndex::open(dir.as_ref()).unwrap();
        index.create(1).unwrap();
        index.create(2).unwrap();
        index
            .record(1, &coord(&[0, 0, 0]), None, Some(&Value::Str("stone".into())))
            .unwrap();
        index
            .record(2, &coord(&[0, 0, 0]), None, Some(&Value::I64(7)))
            .unwrap();

        assert_eq!(
            index.lookup_eq(1, &Value::Str("stone".into())).unwrap(),
            Some(vec![coord(&[0, 0, 0])])
        );
        assert_eq!(
            index.lookup_eq(2, &Value::I64(7)).unwrap(),
            Some(vec![coord(&[0, 0, 0])])
        );
        assert!(index.drop_index(1).unwrap());
        assert!(index.is_indexed(2));
    }
}
