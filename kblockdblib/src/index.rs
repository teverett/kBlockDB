//! A secondary, equality-only, in-memory index from a key's value to the
//! coordinates holding it -- what lets `World::lookup_eq` answer "which
//! cells have `key == value`" in time proportional to the number of
//! matches, instead of `World::list_cells`'s full chunk-by-chunk decode of
//! the whole world (see `World::create_index`'s doc comment for the
//! tradeoff this opts into per key).
//!
//! Only ever consulted/maintained through `World`, which is the only thing
//! that knows a cell's *old* value at the moment it's overwritten -- see
//! `World::record_index_change`, the single place every write/remove path
//! reports a before/after pair here.

use crate::coord::Coord;
use crate::value::Value;
use std::collections::{HashMap, HashSet};
use std::sync::RwLock;

/// A `Value`, minus its `f64` variant's inability to be hashed/compared by
/// `==` in a way that respects NaN -- indexed by its bit pattern instead, so
/// two NaNs with the same bits are "the same value" for lookup purposes
/// (they'd never compare equal via `==`, but they're also never written by
/// anything other than a literal copy of the same bits, so this doesn't
/// cause any observable inconsistency with `get`/`list_cells`, which never
/// claim NaN-aware equality either).
#[derive(Clone, PartialEq, Eq, Hash)]
enum IndexKey {
    Str(String),
    Int(i64),
    Bool(bool),
    FloatBits(u64),
}

impl IndexKey {
    fn from_value(value: &Value) -> Self {
        match value {
            Value::Str(s) => IndexKey::Str(s.clone()),
            Value::I64(i) => IndexKey::Int(*i),
            Value::Bool(b) => IndexKey::Bool(*b),
            Value::F64(f) => IndexKey::FloatBits(f.to_bits()),
        }
    }
}

#[derive(Default)]
struct KeyIndex {
    by_value: HashMap<IndexKey, HashSet<Coord>>,
}

/// One `World`'s whole set of indexed keys and their value->coords maps,
/// keyed by schema key id (not key name -- consistent with how `Chunk`
/// itself addresses columns, and immune to a key being renamed... except
/// `kblockdblib` has no rename, so this is really just "consistent with the
/// rest of the crate").
///
/// Equality lookups only: there's no ordering here, so `Lt`/`Gt`/etc. can't
/// be served by this structure. A key with many distinct cells sharing the
/// same value is exactly as well served as one where every value is unique
/// -- the cost is proportional to how many cells match, not to how many
/// distinct values exist.
#[derive(Default)]
pub struct ValueIndex {
    by_key_id: RwLock<HashMap<u32, KeyIndex>>,
}

impl ValueIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether `key_id` currently has an index built for it.
    pub fn is_indexed(&self, key_id: u32) -> bool {
        self.by_key_id.read().unwrap().contains_key(&key_id)
    }

    /// Every key id currently indexed, in no particular order.
    pub fn indexed_key_ids(&self) -> Vec<u32> {
        self.by_key_id.read().unwrap().keys().copied().collect()
    }

    /// Starts tracking `key_id`, empty -- the caller (`World::create_index`)
    /// is responsible for backfilling it from existing data, since this
    /// module has no access to chunk storage. A no-op, not an error, if
    /// `key_id` is already indexed (its existing contents are left alone).
    pub fn create(&self, key_id: u32) {
        self.by_key_id.write().unwrap().entry(key_id).or_default();
    }

    /// Stops tracking `key_id` and discards its contents. Returns whether
    /// it was indexed.
    pub fn drop_index(&self, key_id: u32) -> bool {
        self.by_key_id.write().unwrap().remove(&key_id).is_some()
    }

    /// Records that `coord`'s value under `key_id` changed from `old` to
    /// `new` (either side `None` meaning "wasn't/isn't set"). A no-op if
    /// `key_id` isn't indexed -- callers aren't expected to check
    /// `is_indexed` first, so every write path can call this unconditionally
    /// without an extra branch.
    pub fn record(&self, key_id: u32, coord: &Coord, old: Option<&Value>, new: Option<&Value>) {
        let mut guard = self.by_key_id.write().unwrap();
        let Some(key_index) = guard.get_mut(&key_id) else {
            return;
        };
        if let Some(old) = old {
            let ikey = IndexKey::from_value(old);
            if let Some(coords) = key_index.by_value.get_mut(&ikey) {
                coords.remove(coord);
                if coords.is_empty() {
                    key_index.by_value.remove(&ikey);
                }
            }
        }
        if let Some(new) = new {
            key_index
                .by_value
                .entry(IndexKey::from_value(new))
                .or_default()
                .insert(coord.clone());
        }
    }

    /// Every coordinate currently holding `value` under `key_id`, or `None`
    /// if `key_id` isn't indexed (the caller should fall back to a full
    /// scan). An indexed key with no cell holding `value` returns
    /// `Some(vec![])`, not `None` -- that's a real, authoritative answer
    /// ("zero matches"), not "I don't know".
    pub fn lookup_eq(&self, key_id: u32, value: &Value) -> Option<Vec<Coord>> {
        let guard = self.by_key_id.read().unwrap();
        let key_index = guard.get(&key_id)?;
        let ikey = IndexKey::from_value(value);
        Some(
            key_index
                .by_value
                .get(&ikey)
                .map(|coords| coords.iter().cloned().collect())
                .unwrap_or_default(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn coord(parts: &[i32]) -> Coord {
        Coord::from(parts.to_vec())
    }

    #[test]
    fn unindexed_key_reports_not_indexed_and_records_nothing() {
        let index = ValueIndex::new();
        assert!(!index.is_indexed(1));
        index.record(1, &coord(&[0, 0, 0]), None, Some(&Value::I64(5)));
        assert_eq!(index.lookup_eq(1, &Value::I64(5)), None);
    }

    #[test]
    fn create_then_record_then_lookup_roundtrips() {
        let index = ValueIndex::new();
        index.create(7);
        assert!(index.is_indexed(7));
        index.record(7, &coord(&[1, 2, 3]), None, Some(&Value::Str("stone".into())));
        index.record(7, &coord(&[4, 5, 6]), None, Some(&Value::Str("stone".into())));
        index.record(7, &coord(&[9, 9, 9]), None, Some(&Value::Str("dirt".into())));

        let mut stone = index.lookup_eq(7, &Value::Str("stone".into())).unwrap();
        stone.sort_by(|a, b| a.iter().cmp(b.iter()));
        assert_eq!(stone, vec![coord(&[1, 2, 3]), coord(&[4, 5, 6])]);

        assert_eq!(
            index.lookup_eq(7, &Value::Str("dirt".into())),
            Some(vec![coord(&[9, 9, 9])])
        );
        assert_eq!(index.lookup_eq(7, &Value::Str("lava".into())), Some(vec![]));
    }

    #[test]
    fn overwrite_moves_coord_between_values() {
        let index = ValueIndex::new();
        index.create(1);
        let c = coord(&[0, 0, 0]);
        index.record(1, &c, None, Some(&Value::I64(1)));
        index.record(1, &c, Some(&Value::I64(1)), Some(&Value::I64(2)));

        assert_eq!(index.lookup_eq(1, &Value::I64(1)), Some(vec![]));
        assert_eq!(index.lookup_eq(1, &Value::I64(2)), Some(vec![c]));
    }

    #[test]
    fn removal_clears_the_coord_from_its_old_value() {
        let index = ValueIndex::new();
        index.create(1);
        let c = coord(&[0, 0, 0]);
        index.record(1, &c, None, Some(&Value::Bool(true)));
        index.record(1, &c, Some(&Value::Bool(true)), None);

        assert_eq!(index.lookup_eq(1, &Value::Bool(true)), Some(vec![]));
    }

    #[test]
    fn drop_index_stops_tracking_and_forgets_contents() {
        let index = ValueIndex::new();
        index.create(1);
        index.record(1, &coord(&[0, 0, 0]), None, Some(&Value::I64(1)));
        assert!(index.drop_index(1));
        assert!(!index.is_indexed(1));
        assert_eq!(index.lookup_eq(1, &Value::I64(1)), None);
        assert!(!index.drop_index(1));
    }

    #[test]
    fn nan_is_indexed_by_bit_pattern_not_ieee_equality() {
        let index = ValueIndex::new();
        index.create(1);
        let c = coord(&[0, 0, 0]);
        index.record(1, &c, None, Some(&Value::F64(f64::NAN)));
        assert_eq!(index.lookup_eq(1, &Value::F64(f64::NAN)), Some(vec![c]));
    }
}
