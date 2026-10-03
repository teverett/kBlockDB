//! Who made a write, and where it falls in that server's own sequence --
//! what replication uses to tell exactly which writes a server has seen
//! (see `VersionVector`, and docs/clustering.md's "Catch-up").

use std::collections::BTreeMap;

/// The origin of a write: the node id of the server that made it, and that
/// server's own sequence number for it (1, 2, 3, ... per server, never
/// reused). Stored with every value and tombstone.
///
/// Ordered by `(origin, seq)` -- the tie-break, after `modified_at_ms`,
/// that makes last-write-wins pick the same winner on every node whatever
/// order the writes arrive in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Stamp {
    pub origin: u64,
    pub seq: u64,
}

impl Stamp {
    /// Data with no known origin: written by a server without clustering,
    /// or before stamps existed.
    pub const NONE: Stamp = Stamp { origin: 0, seq: 0 };

    pub fn new(origin: u64, seq: u64) -> Self {
        Stamp { origin, seq }
    }
}

/// Set in every legacy pseudo-origin, and clear in every real node id.
pub const LEGACY_BIT: u64 = 1 << 63;

/// The pseudo-origin a node's *legacy* (`Stamp::NONE`) data is tracked
/// under when it's shipped to peers: `node_id` with `LEGACY_BIT` set.
/// Legacy data has no real origin, and different nodes can hold different
/// legacy data (each was written somewhere before clustering), so it's
/// tracked per holder: a receiver that's had node A's legacy data has
/// `legacy_origin(A): 0` in its vector, and isn't sent it again -- but
/// still gets node B's. Real node ids never have `LEGACY_BIT` set.
pub fn legacy_origin(node_id: u64) -> u64 {
    node_id | LEGACY_BIT
}

/// Per origin, the highest sequence number covered: "every write by
/// origin `o` with `seq <= self[o]`". An origin that's absent covers
/// nothing at all -- not even `Stamp::NONE`'s seq 0, which is how a new
/// server that has never synced gets legacy (unstamped) data too.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct VersionVector(BTreeMap<u64, u64>);

impl VersionVector {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, origin: u64) -> Option<u64> {
        self.0.get(&origin).copied()
    }

    /// Sets `origin`'s entry to `seq`, replacing whatever was there.
    pub fn set(&mut self, origin: u64, seq: u64) {
        self.0.insert(origin, seq);
    }

    /// Raises `origin`'s entry to at least `seq` (adding it if absent).
    pub fn observe(&mut self, stamp: Stamp) {
        let entry = self.0.entry(stamp.origin).or_insert(stamp.seq);
        *entry = (*entry).max(stamp.seq);
    }

    /// Whether the write stamped `stamp` is covered.
    pub fn has(&self, stamp: Stamp) -> bool {
        self.get(stamp.origin).is_some_and(|seq| stamp.seq <= seq)
    }

    /// Whether every write `other` covers is covered here too.
    pub fn covers(&self, other: &VersionVector) -> bool {
        other
            .iter()
            .all(|(origin, seq)| self.has(Stamp::new(origin, seq)))
    }

    /// Raises every entry to at least `other`'s, returning whether
    /// anything changed.
    pub fn merge(&mut self, other: &VersionVector) -> bool {
        let mut changed = false;
        for (origin, seq) in other.iter() {
            let entry = self.0.entry(origin).or_insert_with(|| {
                changed = true;
                seq
            });
            if seq > *entry {
                *entry = seq;
                changed = true;
            }
        }
        changed
    }

    /// The writes covered by both `self` and `other`: per origin, the
    /// lower entry, and only origins both have.
    pub fn meet(&self, other: &VersionVector) -> VersionVector {
        self.iter()
            .filter_map(|(origin, seq)| Some((origin, seq.min(other.get(origin)?))))
            .collect()
    }

    pub fn iter(&self) -> impl Iterator<Item = (u64, u64)> + '_ {
        self.0.iter().map(|(&origin, &seq)| (origin, seq))
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl FromIterator<(u64, u64)> for VersionVector {
    fn from_iter<I: IntoIterator<Item = (u64, u64)>>(iter: I) -> Self {
        let mut vector = VersionVector::new();
        for (origin, seq) in iter {
            vector.observe(Stamp::new(origin, seq));
        }
        vector
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_absent_origin_covers_nothing_not_even_seq_0() {
        let v = VersionVector::new();
        assert!(!v.has(Stamp::NONE));
        let v: VersionVector = [(0, 0)].into_iter().collect();
        assert!(v.has(Stamp::NONE));
    }

    #[test]
    fn has_covers_every_seq_up_to_the_entry() {
        let v: VersionVector = [(7, 10)].into_iter().collect();
        assert!(v.has(Stamp::new(7, 1)));
        assert!(v.has(Stamp::new(7, 10)));
        assert!(!v.has(Stamp::new(7, 11)));
        assert!(!v.has(Stamp::new(8, 1)));
    }

    #[test]
    fn merge_takes_the_maximum_per_origin() {
        let mut a: VersionVector = [(1, 5), (2, 9)].into_iter().collect();
        let b: VersionVector = [(1, 7), (2, 3), (3, 1)].into_iter().collect();
        assert!(a.merge(&b));
        assert_eq!(a, [(1, 7), (2, 9), (3, 1)].into_iter().collect());
        assert!(!a.merge(&b));
    }

    #[test]
    fn meet_keeps_the_lower_entry_of_origins_both_have() {
        let a: VersionVector = [(1, 5), (2, 9), (3, 1)].into_iter().collect();
        let b: VersionVector = [(1, 7), (2, 3)].into_iter().collect();
        assert_eq!(a.meet(&b), [(1, 5), (2, 3)].into_iter().collect());
    }

    #[test]
    fn covers_compares_every_entry() {
        let a: VersionVector = [(1, 5), (2, 9)].into_iter().collect();
        assert!(a.covers(&[(1, 5)].into_iter().collect()));
        assert!(a.covers(&VersionVector::new()));
        assert!(!a.covers(&[(1, 6)].into_iter().collect()));
        assert!(!a.covers(&[(3, 0)].into_iter().collect()));
    }

    #[test]
    fn stamps_order_by_origin_then_seq() {
        assert!(Stamp::new(1, 9) < Stamp::new(2, 1));
        assert!(Stamp::new(2, 1) < Stamp::new(2, 2));
        assert!(Stamp::NONE < Stamp::new(1, 1));
    }
}
