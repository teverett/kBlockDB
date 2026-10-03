use crate::stamp::{Stamp, VersionVector};
use crate::value::Value;
use std::collections::{BTreeMap, HashMap};
use std::io::{self, Read, Write};

/// Cells in one chunk, for a world with `axes` axes and `chunk_dim` cells
/// per axis within a chunk: `chunk_dim^axes`. With the default 3 axes and
/// `chunk_dim` 32, that's 32^3 = 32,768 cells/chunk. Both are per-world
/// runtime parameters (see `world::WorldParams`/`World::create`), so this
/// is always a runtime value, never a compile-time constant.
pub fn chunk_cells(axes: usize, chunk_dim: u32) -> usize {
    (chunk_dim as usize).pow(axes as u32)
}

/// Bytes per `rank`-acceleration block -- see `Bitset::block_counts`. 64 is
/// a small, simple, fixed constant (not derived per chunk shape): big
/// enough to keep `block_counts` cheap (1/32 of the bitmap's own size, in
/// `u16`s), small enough that the "scan the rest of the target block"
/// part of `rank` stays cheap too. Not tuned to any particular `axes`.
const RANK_BLOCK_BYTES: usize = 64;

/// A bitmap, one bit per cell in the chunk, marking whether that cell has a
/// value in a given column. Sized at construction from the owning world's
/// `chunk_cells(axes, chunk_dim)` -- neither is known until a `World` is
/// opened, so this can't be a fixed-size array the way a single-world-shape
/// version of this prototype could use.
#[derive(Clone)]
struct Bitset {
    bits: Vec<u8>,
    /// Number of set bits in each `RANK_BLOCK_BYTES`-byte block of `bits`,
    /// kept in sync with `bits` on every `set` call -- see `rank`, the
    /// reason this exists.
    block_counts: Vec<u16>,
}

impl Bitset {
    /// `byte_len` must be `cell_count.div_ceil(8)` for whatever `cell_count`
    /// this bitmap is meant to cover -- with a configurable `chunk_dim`,
    /// `cell_count` is no longer guaranteed to be a multiple of 8, so the
    /// last byte may have a handful of trailing unused (always-0) bits.
    fn new(byte_len: usize) -> Self {
        Bitset {
            bits: vec![0u8; byte_len],
            block_counts: vec![0u16; byte_len.div_ceil(RANK_BLOCK_BYTES)],
        }
    }

    /// Rebuilds a `Bitset` (and its `block_counts` index) from raw bytes
    /// read off disk -- `block_counts` is a derived, in-memory-only
    /// accelerator, never itself persisted (see the on-disk format comment
    /// below), so loading a chunk means computing it once here.
    fn from_bits(bits: Vec<u8>) -> Self {
        let block_counts = bits
            .chunks(RANK_BLOCK_BYTES)
            .map(|block| block.iter().map(|&b| b.count_ones() as u16).sum())
            .collect();
        Bitset { bits, block_counts }
    }

    fn get(&self, idx: usize) -> bool {
        (self.bits[idx >> 3] >> (idx & 7)) & 1 == 1
    }

    fn set(&mut self, idx: usize, v: bool) {
        let byte_idx = idx >> 3;
        let mask = 1u8 << (idx & 7);
        let was_set = self.bits[byte_idx] & mask != 0;
        if v == was_set {
            return; // no change -- block_counts is already correct
        }
        if v {
            self.bits[byte_idx] |= mask;
            self.block_counts[byte_idx / RANK_BLOCK_BYTES] += 1;
        } else {
            self.bits[byte_idx] &= !mask;
            self.block_counts[byte_idx / RANK_BLOCK_BYTES] -= 1;
        }
    }

    /// Number of set bits strictly before `idx` -- i.e. this cell's position
    /// within the column's dense `values` vec, if it's present at all.
    ///
    /// A naive version of this scans every byte before `idx` and sums their
    /// popcounts -- O(presence_bytes) per call, which is most of a chunk's
    /// presence bitmap (up to a few thousand bytes) on *every*
    /// `get`/`set`/`remove`. `block_counts` turns that into
    /// O(presence_bytes/RANK_BLOCK_BYTES + RANK_BLOCK_BYTES): sum the
    /// (few) whole blocks before `idx`'s block from the precomputed
    /// per-block counts, then only popcount-scan the one partial block
    /// `idx` actually falls in.
    fn rank(&self, idx: usize) -> usize {
        let byte_idx = idx >> 3;
        let block_idx = byte_idx / RANK_BLOCK_BYTES;
        let block_start = block_idx * RANK_BLOCK_BYTES;

        let mut count: usize = self.block_counts[..block_idx]
            .iter()
            .map(|&c| c as usize)
            .sum();
        count += self.bits[block_start..byte_idx]
            .iter()
            .map(|b| b.count_ones() as usize)
            .sum::<usize>();

        let bit_in_byte = idx & 7;
        if bit_in_byte > 0 {
            let partial_mask = (1u8 << bit_in_byte) - 1;
            count += (self.bits[byte_idx] & partial_mask).count_ones() as usize;
        }
        count
    }

    fn count(&self) -> usize {
        self.block_counts.iter().map(|&c| c as usize).sum()
    }

    /// Every set index, ascending -- the k-th yielded index (0-indexed) is
    /// always exactly the cell at `rank` k in this column's dense value
    /// array, since `rank(idx)` counts set bits before `idx` and these are
    /// yielded in increasing `idx` order. Used by `Chunk::entries_by_local_idx`
    /// to walk a whole column's entries without probing every possible
    /// index with `get`/`rank` one at a time.
    fn iter_set(&self) -> impl Iterator<Item = usize> + '_ {
        self.bits.iter().enumerate().flat_map(|(byte_idx, &byte)| {
            (0..8u8).filter_map(move |bit| {
                (byte & (1 << bit) != 0).then_some(byte_idx * 8 + bit as usize)
            })
        })
    }
}

/// One column's storage: which cells have this key, and their values packed
/// densely (no gaps) in cell order.
enum ColumnData {
    F64(Vec<f64>),
    I64(Vec<i64>),
    Str(Vec<String>),
    Bool(Vec<bool>),
}

/// Every set cell/key pair's bookkeeping, alongside its value: when it was
/// first set, when it was last changed, and how many times it's been set
/// since (0 for the initial `set`, incremented on every one after that).
/// Cleared along with the value on `remove` -- a cell set again later after
/// being removed starts a brand new history, not a continuation of the old
/// one. (A *tombstone* may record when it was removed -- see
/// `Chunk::tombstones` -- but that's separate from, and never visible as,
/// a `CellMeta`.)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CellMeta {
    pub created_at_ms: u64,
    pub modified_at_ms: u64,
    pub version: u64,
}

struct Column {
    presence: Bitset,
    data: ColumnData,
    /// Parallel to `data` -- `meta[rank]` describes the same cell as
    /// whatever's at `data`'s index `rank` (see `Bitset::rank`), so every
    /// insert/overwrite/remove on `data` makes the identical change to
    /// `meta` at the same index.
    meta: Vec<CellMeta>,
    /// Parallel to `meta`: who made each value's current write (see
    /// `Stamp`). `Stamp::NONE` for unclustered and legacy writes.
    stamps: Vec<Stamp>,
}

/// A `chunk_cells(axes, chunk_dim)`-cell block of cells, stored columnarly:
/// one sparse array per key that actually appears *somewhere in this
/// chunk*, rather than one hashmap per cell. Columns are created lazily on
/// first write, so a chunk that only ever sees 2 distinct keys allocates
/// exactly 2 columns, no matter how many keys exist elsewhere in the world.
///
/// `Chunk` itself doesn't know the world's coordinate system -- `local_idx`
/// is an opaque flat cell index within `[0, chunk_cells(axes, chunk_dim))`;
/// `World::split` is what maps a coordinate to one. It does need to know
/// the chunk's cell count indirectly, though: `presence_bytes`
/// (`chunk_cells(axes, chunk_dim).div_ceil(8)`) is how big a newly-created
/// column's presence bitmap must be.
pub struct Chunk {
    columns: HashMap<u32, Column>,
    presence_bytes: usize,
    /// When, and by whom, each removed cell/key was removed, by key id then
    /// local cell index -- only recorded when the owning world keeps
    /// tombstones (see `World::with_tombstone_retention`), so a replicated
    /// cluster can tell "deleted" apart from "never written" and ship
    /// deletes in a catch-up. Kept outside `columns` on purpose:
    /// `get`/`get_meta`/`entries_by_local_idx` never see a tombstone, so no
    /// read path has to filter one out. A cell/key never has both a value
    /// and a tombstone: setting it clears the tombstone.
    tombstones: HashMap<u32, BTreeMap<usize, (u64, Stamp)>>,
    /// Per origin, the highest seq ever stored in this chunk (values and
    /// tombstones alike). Never decreases -- not even when that write is
    /// later overwritten, removed or purged -- so it's always a safe
    /// summary for "could this chunk hold a write a given vector lacks?":
    /// `World::changes_since` reads it from the file header alone to skip
    /// whole chunks.
    max_seq: VersionVector,
}

/// One cell/key's state as reported by `Chunk::changes_since` /
/// `World::changes_since`.
#[derive(Debug, Clone, PartialEq)]
pub enum ChangeKind {
    Set(Value, CellMeta),
    /// Removed at this time (ms since the Unix epoch).
    Removed(u64),
}

impl Chunk {
    pub fn new(cell_count: usize) -> Self {
        Chunk {
            columns: HashMap::new(),
            presence_bytes: cell_count.div_ceil(8),
            tombstones: HashMap::new(),
            max_seq: VersionVector::new(),
        }
    }

    /// See the `max_seq` field.
    pub fn max_seq(&self) -> &VersionVector {
        &self.max_seq
    }

    /// When, and by whom, this cell/key was removed, if a tombstone
    /// records it.
    pub fn tombstone_at(&self, local_idx: usize, key_id: u32) -> Option<(u64, Stamp)> {
        self.tombstones.get(&key_id)?.get(&local_idx).copied()
    }

    /// Who made this cell/key's current value, if it has one.
    pub fn stamp_at(&self, local_idx: usize, key_id: u32) -> Option<Stamp> {
        let col = self.columns.get(&key_id)?;
        if !col.presence.get(local_idx) {
            return None;
        }
        Some(col.stamps[col.presence.rank(local_idx)])
    }

    /// Records that this cell/key was removed at `at_ms` by `stamp`,
    /// replacing any older tombstone. Doesn't touch a value -- callers
    /// remove that first (see `remove_with_tombstone`).
    pub fn put_tombstone(&mut self, local_idx: usize, key_id: u32, at_ms: u64, stamp: Stamp) {
        self.tombstones
            .entry(key_id)
            .or_default()
            .insert(local_idx, (at_ms, stamp));
        self.max_seq.observe(stamp);
    }

    fn clear_tombstone(&mut self, local_idx: usize, key_id: u32) {
        if let Some(cells) = self.tombstones.get_mut(&key_id) {
            cells.remove(&local_idx);
            if cells.is_empty() {
                self.tombstones.remove(&key_id);
            }
        }
    }

    /// `remove`, plus a tombstone at `at_ms` -- but only if there was a
    /// value to remove, so deleting a large, mostly empty region doesn't
    /// fill chunks with markers for cells that never held anything.
    /// Returns whether a value was removed.
    pub fn remove_with_tombstone(
        &mut self,
        local_idx: usize,
        key_id: u32,
        at_ms: u64,
        stamp: Stamp,
    ) -> bool {
        let removed = self.remove(local_idx, key_id);
        if removed {
            self.put_tombstone(local_idx, key_id, at_ms, stamp);
        }
        removed
    }

    /// Drops every tombstone older than `cutoff_ms`, returning whether any
    /// were dropped.
    pub fn purge_tombstones_older_than(&mut self, cutoff_ms: u64) -> bool {
        let mut purged = false;
        self.tombstones.retain(|_, cells| {
            let before = cells.len();
            cells.retain(|_, (at, _)| *at >= cutoff_ms);
            purged |= cells.len() != before;
            !cells.is_empty()
        });
        purged
    }

    /// Every value and tombstone whose stamp `known` doesn't cover, as
    /// `(local_idx, key_id, change, stamp)`. A legacy (`Stamp::NONE`)
    /// entry counts as `legacy`'s write -- see `stamp::legacy_origin`.
    /// O(chunk contents), like `entries_by_local_idx`.
    pub fn changes_since(
        &self,
        known: &VersionVector,
        legacy: Stamp,
    ) -> Vec<(usize, u32, ChangeKind, Stamp)> {
        let mut out = Vec::new();
        if covers_max_seq(known, &self.max_seq, legacy) {
            return out;
        }
        let needs = |stamp: Stamp| !known.has(if stamp == Stamp::NONE { legacy } else { stamp });
        for (&key_id, col) in &self.columns {
            for (rank, local_idx) in col.presence.iter_set().enumerate() {
                let stamp = col.stamps[rank];
                if needs(stamp) {
                    let change = ChangeKind::Set(col.value_at(rank), col.meta[rank]);
                    out.push((local_idx, key_id, change, stamp));
                }
            }
        }
        for (&key_id, cells) in &self.tombstones {
            for (&local_idx, &(at, stamp)) in cells {
                if needs(stamp) {
                    out.push((local_idx, key_id, ChangeKind::Removed(at), stamp));
                }
            }
        }
        out
    }

    pub fn get(&self, local_idx: usize, key_id: u32) -> Option<Value> {
        let col = self.columns.get(&key_id)?;
        if !col.presence.get(local_idx) {
            return None;
        }
        Some(col.value_at(col.presence.rank(local_idx)))
    }

    pub fn get_meta(&self, local_idx: usize, key_id: u32) -> Option<CellMeta> {
        let col = self.columns.get(&key_id)?;
        if !col.presence.get(local_idx) {
            return None;
        }
        Some(col.meta[col.presence.rank(local_idx)])
    }

    /// `value`'s type must match whatever `key_id` already holds elsewhere
    /// in this chunk (if anything) -- callers (`World::set`/`set_region`)
    /// are expected to have already checked that against `Schema`, which
    /// records a key's type once, world-wide, the first time it's ever set
    /// (see `Schema`'s doc comment). The `panic!`s below are that
    /// assumption made explicit: reaching them means a caller skipped that
    /// check, not a normal, reachable-from-user-input outcome -- unlike
    /// `Schema::intern`'s type check, which returns a graceful
    /// `InvalidInput` error for exactly this situation.
    ///
    /// The presence/data half of a write -- creating the column on first
    /// use in this chunk, then either overwriting or inserting `value` --
    /// shared by `set_stamped` and `set_replicated`. Returns whether the
    /// cell already held a value for this key (so the caller knows whether
    /// to bump a version/keep `created_at_ms` or start fresh) and its rank
    /// within the column, for indexing `col.meta`/`col.stamps`.
    fn store_value(&mut self, local_idx: usize, key_id: u32, value: Value) -> (bool, usize) {
        let presence_bytes = self.presence_bytes;
        let col = self.columns.entry(key_id).or_insert_with(|| Column {
            presence: Bitset::new(presence_bytes),
            data: match &value {
                Value::F64(_) => ColumnData::F64(Vec::new()),
                Value::I64(_) => ColumnData::I64(Vec::new()),
                Value::Str(_) => ColumnData::Str(Vec::new()),
                Value::Bool(_) => ColumnData::Bool(Vec::new()),
            },
            meta: Vec::new(),
            stamps: Vec::new(),
        });

        let already_present = col.presence.get(local_idx);
        let rank = col.presence.rank(local_idx);

        if already_present {
            match (&mut col.data, value) {
                (ColumnData::F64(v), Value::F64(x)) => v[rank] = x,
                (ColumnData::I64(v), Value::I64(x)) => v[rank] = x,
                (ColumnData::Str(v), Value::Str(x)) => v[rank] = x,
                (ColumnData::Bool(v), Value::Bool(x)) => v[rank] = x,
                _ => panic!(
                    "key {key_id} already holds a different value type in this chunk -- \
                     caller should have checked this against Schema first"
                ),
            }
        } else {
            col.presence.set(local_idx, true);
            match (&mut col.data, value) {
                (ColumnData::F64(v), Value::F64(x)) => v.insert(rank, x),
                (ColumnData::I64(v), Value::I64(x)) => v.insert(rank, x),
                (ColumnData::Str(v), Value::Str(x)) => v.insert(rank, x),
                (ColumnData::Bool(v), Value::Bool(x)) => v.insert(rank, x),
                _ => panic!(
                    "key {key_id} already holds a different value type in this chunk -- \
                     caller should have checked this against Schema first"
                ),
            }
        }
        (already_present, rank)
    }

    /// `set_stamped` with `Stamp::NONE` -- an unclustered write.
    pub fn set(&mut self, local_idx: usize, key_id: u32, value: Value, now_ms: u64) -> CellMeta {
        self.set_stamped(local_idx, key_id, value, now_ms, Stamp::NONE)
    }

    /// A local write: derives a fresh `CellMeta` from `now_ms` (bumping
    /// the version of an existing value) and records `stamp` as its
    /// origin.
    ///
    /// `now_ms` is milliseconds since the Unix epoch, supplied by the
    /// caller (`World`) rather than read here, so this stays a pure
    /// function of its arguments -- deterministic and easy to test without
    /// depending on wall-clock time.
    pub fn set_stamped(
        &mut self,
        local_idx: usize,
        key_id: u32,
        value: Value,
        now_ms: u64,
        stamp: Stamp,
    ) -> CellMeta {
        self.clear_tombstone(local_idx, key_id);
        self.max_seq.observe(stamp);
        let (already_present, rank) = self.store_value(local_idx, key_id, value);
        let col = self
            .columns
            .get_mut(&key_id)
            .expect("store_value just populated this column");
        if already_present {
            let meta = &mut col.meta[rank];
            meta.modified_at_ms = now_ms;
            meta.version += 1;
            col.stamps[rank] = stamp;
            *meta
        } else {
            let meta = CellMeta {
                created_at_ms: now_ms,
                modified_at_ms: now_ms,
                version: 0,
            };
            col.meta.insert(rank, meta);
            col.stamps.insert(rank, stamp);
            meta
        }
    }

    /// `set_replicated` with `Stamp::NONE`.
    pub fn set_with_meta(&mut self, local_idx: usize, key_id: u32, value: Value, meta: CellMeta) {
        self.set_replicated(local_idx, key_id, value, meta, Stamp::NONE);
    }

    /// Like `set_stamped`, but applies `meta` verbatim instead of deriving
    /// one from `now_ms` -- no version bump, no created/modified
    /// computation. Used only to apply a replicated write with its
    /// origin's own metadata and stamp (see `World::apply_replicated`), so
    /// the cluster converges on the same `CellMeta` for a given write
    /// everywhere, not a new one per node that received it.
    pub fn set_replicated(
        &mut self,
        local_idx: usize,
        key_id: u32,
        value: Value,
        meta: CellMeta,
        stamp: Stamp,
    ) {
        self.clear_tombstone(local_idx, key_id);
        self.max_seq.observe(stamp);
        let (already_present, rank) = self.store_value(local_idx, key_id, value);
        let col = self
            .columns
            .get_mut(&key_id)
            .expect("store_value just populated this column");
        if already_present {
            col.meta[rank] = meta;
            col.stamps[rank] = stamp;
        } else {
            col.meta.insert(rank, meta);
            col.stamps.insert(rank, stamp);
        }
    }

    /// Removes this cell/key's value (no tombstone -- see
    /// `remove_with_tombstone`), returning whether there was one.
    pub fn remove(&mut self, local_idx: usize, key_id: u32) -> bool {
        if let Some(col) = self.columns.get_mut(&key_id) {
            if col.presence.get(local_idx) {
                let rank = col.presence.rank(local_idx);
                col.presence.set(local_idx, false);
                match &mut col.data {
                    ColumnData::F64(v) => {
                        v.remove(rank);
                    }
                    ColumnData::I64(v) => {
                        v.remove(rank);
                    }
                    ColumnData::Str(v) => {
                        v.remove(rank);
                    }
                    ColumnData::Bool(v) => {
                        v.remove(rank);
                    }
                }
                col.meta.remove(rank);
                col.stamps.remove(rank);
                return true;
            }
        }
        false
    }

    /// Drops `key_id`'s entire column from this chunk -- every cell's
    /// value and metadata for that key at once -- returning whether there
    /// was one to drop. Unlike `remove`, which clears a single cell, this
    /// is what `World::remove_column` uses to erase a key world-wide.
    pub fn remove_column(&mut self, key_id: u32) -> bool {
        let had_tombstones = self.tombstones.remove(&key_id).is_some();
        self.columns.remove(&key_id).is_some() || had_tombstones
    }

    /// True if no cell in this chunk has any value set and no tombstone is
    /// recorded -- such chunks aren't written to disk at all (see
    /// `World::flush_one`), which is how a mostly empty 10,000^3 world
    /// avoids allocating 30 million chunk files. A chunk holding only
    /// tombstones isn't empty: its file is what keeps them.
    pub fn is_empty(&self) -> bool {
        self.columns.values().all(|c| c.presence.count() == 0) && self.tombstones.is_empty()
    }

    /// Every `(key_id, value, meta)` set anywhere in this chunk, grouped by
    /// local cell index (the union of every column's presence bitmap) --
    /// used by `World::list_cells` to build a whole-world cell listing.
    /// Nothing else in this crate needs "every set cell in this chunk", so
    /// unlike `get`/`set`/`remove` (each O(1)-ish, on the hot path of every
    /// single-cell operation), this is O(chunk contents) and only meant to
    /// be called occasionally, whole-chunk at a time.
    pub fn entries_by_local_idx(&self) -> BTreeMap<usize, Vec<(u32, Value, CellMeta)>> {
        let mut out: BTreeMap<usize, Vec<(u32, Value, CellMeta)>> = BTreeMap::new();
        for (&key_id, col) in &self.columns {
            for (rank, local_idx) in col.presence.iter_set().enumerate() {
                out.entry(local_idx).or_default().push((
                    key_id,
                    col.value_at(rank),
                    col.meta[rank],
                ));
            }
        }
        out
    }

    // --- Binary format (CHUNK_FORMAT 2) ---
    //
    //   [4 bytes CHUNK_MAGIC][u8 format = 2]
    //   [u32 n][n x ([u64 origin][u64 max_seq])]   (max_seq, by origin)
    //   [u32 num_columns]
    //   repeated num_columns times, sorted by key_id:
    //     [u32 key_id]
    //     [u8  type_tag]              (0=Str, 1=F64, 2=I64, 3=Bool)
    //     [presence bitmap, chunk_cells.div_ceil(8) bytes]
    //     entries, one per set bit in the bitmap, in cell-index order:
    //       [u64 created_at_ms][u64 modified_at_ms][u64 version]  (CellMeta)
    //       [u64 origin][u64 seq]                                (Stamp)
    //       then the value itself:
    //         F64/I64: 8 bytes little-endian
    //         Str:     [u32 len][len bytes, utf-8]
    //         Bool:    1 byte (0 or 1)
    //
    //   [u32 num_tombstone_keys]
    //   repeated num_tombstone_keys times, sorted by key_id:
    //     [u32 key_id][u32 count]
    //     count times, ascending:
    //       [u32 local_idx][u64 removed_at_ms][u64 origin][u64 seq]
    //
    // There is no per-cell overhead beyond 1 bit in the presence map plus
    // CellMeta's 24 bytes and Stamp's 16: a cell that doesn't use a key
    // costs nothing but that bit.
    //
    // Older files still load, with every stamp `Stamp::NONE`:
    //   - format 1: header `[magic][u8 1][u64 newest_ms]`, no stamps in
    //     entries or tombstones;
    //   - no header at all (written before tombstones): starts straight at
    //     `num_columns` -- CHUNK_MAGIC read as a little-endian `num_columns`
    //     would be ~1.1 billion columns, which no real file has -- and has
    //     no tombstone section.
    // This always writes format 2, so a chunk rewritten by this version
    // can't be read by an older one.

    pub fn write_to<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_all(&CHUNK_MAGIC)?;
        w.write_all(&[CHUNK_FORMAT])?;
        w.write_all(&(self.max_seq.len() as u32).to_le_bytes())?;
        for (origin, seq) in self.max_seq.iter() {
            w.write_all(&origin.to_le_bytes())?;
            w.write_all(&seq.to_le_bytes())?;
        }
        w.write_all(&(self.columns.len() as u32).to_le_bytes())?;

        let mut ids: Vec<&u32> = self.columns.keys().collect();
        ids.sort();

        for &key_id in ids {
            let col = &self.columns[&key_id];
            w.write_all(&key_id.to_le_bytes())?;
            let tag = match &col.data {
                ColumnData::F64(_) => Value::TAG_F64,
                ColumnData::I64(_) => Value::TAG_I64,
                ColumnData::Str(_) => Value::TAG_STR,
                ColumnData::Bool(_) => Value::TAG_BOOL,
            };
            w.write_all(&[tag])?;
            w.write_all(&col.presence.bits)?;
            for rank in 0..col.meta.len() {
                write_meta(w, &col.meta[rank])?;
                write_stamp(w, col.stamps[rank])?;
                match &col.data {
                    ColumnData::F64(v) => w.write_all(&v[rank].to_le_bytes())?,
                    ColumnData::I64(v) => w.write_all(&v[rank].to_le_bytes())?,
                    ColumnData::Str(v) => {
                        let bytes = v[rank].as_bytes();
                        w.write_all(&(bytes.len() as u32).to_le_bytes())?;
                        w.write_all(bytes)?;
                    }
                    ColumnData::Bool(v) => w.write_all(&[u8::from(v[rank])])?,
                }
            }
        }

        let mut ids: Vec<&u32> = self.tombstones.keys().collect();
        ids.sort();
        w.write_all(&(ids.len() as u32).to_le_bytes())?;
        for &key_id in ids {
            let cells = &self.tombstones[&key_id];
            w.write_all(&key_id.to_le_bytes())?;
            w.write_all(&(cells.len() as u32).to_le_bytes())?;
            for (&local_idx, &(at, stamp)) in cells {
                w.write_all(&(local_idx as u32).to_le_bytes())?;
                w.write_all(&at.to_le_bytes())?;
                write_stamp(w, stamp)?;
            }
        }
        Ok(())
    }

    /// Reads just a chunk file's `max_seq` from its header, without
    /// decoding the rest -- `None` for a file in an older format (the
    /// caller then has to load it to find out).
    pub fn read_max_seq<R: Read>(r: &mut R) -> io::Result<Option<VersionVector>> {
        if read_arr4(r)? != CHUNK_MAGIC {
            return Ok(None);
        }
        match read_format(r)? {
            CHUNK_FORMAT => Ok(Some(read_vector(r)?)),
            _ => Ok(None),
        }
    }

    /// `cell_count` must be the owning world's `chunk_cells(axes, chunk_dim)`
    /// -- the caller (`World`) knows this from its own `axes`/`chunk_dim`,
    /// persisted in `world.txt`, so it isn't stored redundantly in every
    /// chunk file.
    pub fn read_from<R: Read>(r: &mut R, cell_count: usize) -> io::Result<Self> {
        let presence_bytes = cell_count.div_ceil(8);
        let first = read_arr4(r)?;
        // 0 = no header (pre-tombstone), else the header's format byte.
        let (format, header_max_seq, num_columns) = if first == CHUNK_MAGIC {
            match read_format(r)? {
                1 => {
                    let _newest_ms = read_arr8(r)?;
                    (1, None, read_u32(r)?)
                }
                _ => {
                    let max_seq = read_vector(r)?;
                    (CHUNK_FORMAT, Some(max_seq), read_u32(r)?)
                }
            }
        } else {
            (0, None, u32::from_le_bytes(first))
        };
        let stamped = format == CHUNK_FORMAT;
        let mut columns = HashMap::with_capacity(num_columns as usize);

        for _ in 0..num_columns {
            let key_id = read_u32(r)?;

            let mut tag = [0u8; 1];
            r.read_exact(&mut tag)?;

            let mut bits = vec![0u8; presence_bytes];
            r.read_exact(&mut bits)?;
            let presence = Bitset::from_bits(bits);
            let count = presence.count();

            let mut meta = Vec::with_capacity(count);
            let mut stamps = Vec::with_capacity(count);
            let mut read_entry_head = |r: &mut R| -> io::Result<()> {
                meta.push(read_meta(r)?);
                stamps.push(if stamped { read_stamp(r)? } else { Stamp::NONE });
                Ok(())
            };
            let data = match tag[0] {
                t if t == Value::TAG_F64 => {
                    let mut v = Vec::with_capacity(count);
                    for _ in 0..count {
                        read_entry_head(r)?;
                        v.push(f64::from_le_bytes(read_arr8(r)?));
                    }
                    ColumnData::F64(v)
                }
                t if t == Value::TAG_I64 => {
                    let mut v = Vec::with_capacity(count);
                    for _ in 0..count {
                        read_entry_head(r)?;
                        v.push(i64::from_le_bytes(read_arr8(r)?));
                    }
                    ColumnData::I64(v)
                }
                t if t == Value::TAG_STR => {
                    let mut v = Vec::with_capacity(count);
                    for _ in 0..count {
                        read_entry_head(r)?;
                        let len = read_u32(r)? as usize;
                        let mut buf = vec![0u8; len];
                        r.read_exact(&mut buf)?;
                        v.push(
                            String::from_utf8(buf)
                                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
                        );
                    }
                    ColumnData::Str(v)
                }
                t if t == Value::TAG_BOOL => {
                    let mut v = Vec::with_capacity(count);
                    for _ in 0..count {
                        read_entry_head(r)?;
                        let mut b = [0u8; 1];
                        r.read_exact(&mut b)?;
                        v.push(b[0] != 0);
                    }
                    ColumnData::Bool(v)
                }
                other => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("corrupt chunk: unknown type tag {other}"),
                    ))
                }
            };

            columns.insert(
                key_id,
                Column {
                    presence,
                    data,
                    meta,
                    stamps,
                },
            );
        }

        let mut tombstones: HashMap<u32, BTreeMap<usize, (u64, Stamp)>> = HashMap::new();
        if format >= 1 {
            for _ in 0..read_u32(r)? {
                let key_id = read_u32(r)?;
                let cells = tombstones.entry(key_id).or_default();
                for _ in 0..read_u32(r)? {
                    let local_idx = read_u32(r)? as usize;
                    if local_idx >= cell_count {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("corrupt chunk: tombstone cell {local_idx} out of range"),
                        ));
                    }
                    let at = u64::from_le_bytes(read_arr8(r)?);
                    let stamp = if stamped { read_stamp(r)? } else { Stamp::NONE };
                    cells.insert(local_idx, (at, stamp));
                }
            }
            tombstones.retain(|_, cells| !cells.is_empty());
        }

        let mut chunk = Chunk {
            columns,
            presence_bytes,
            tombstones,
            max_seq: header_max_seq.unwrap_or_default(),
        };
        // An older file holds only legacy (`Stamp::NONE`) writes.
        if !stamped && !chunk.is_empty() {
            chunk.max_seq.observe(Stamp::NONE);
        }
        Ok(chunk)
    }

    /// Exact on-disk size in bytes, for reporting/benchmarking.
    #[cfg(test)]
    pub fn byte_len(&self) -> usize {
        let mut n = 4 + 1 + 4 + self.max_seq.len() * 16 + 4; // header, num_columns
        for col in self.columns.values() {
            n += 4 + 1 + self.presence_bytes;
            n += col.meta.len() * (24 + 16); // CellMeta (3 u64) + Stamp (2 u64)
            n += match &col.data {
                ColumnData::F64(v) => v.len() * 8,
                ColumnData::I64(v) => v.len() * 8,
                ColumnData::Str(v) => v.iter().map(|s| 4 + s.len()).sum(),
                ColumnData::Bool(v) => v.len(),
            };
        }
        n += 4; // num_tombstone_keys
        for cells in self.tombstones.values() {
            n += 4 + 4 + cells.len() * (4 + 8 + 16);
        }
        n
    }
}

impl Column {
    fn value_at(&self, rank: usize) -> Value {
        match &self.data {
            ColumnData::F64(v) => Value::F64(v[rank]),
            ColumnData::I64(v) => Value::I64(v[rank]),
            ColumnData::Str(v) => Value::Str(v[rank].clone()),
            ColumnData::Bool(v) => Value::Bool(v[rank]),
        }
    }
}

/// Starts every chunk file written since tombstones were added -- see the
/// format comment on `Chunk::write_to`. The leading 0xFF keeps it from
/// ever matching an old file's little-endian `num_columns`, and from
/// matching the zstd frame magic (`world::ZSTD_MAGIC`).
pub const CHUNK_MAGIC: [u8; 4] = [0xFF, b'K', b'B', b'C'];

/// The chunk file format `write_to` writes. Format 1 (tombstones, no
/// stamps) is still read.
const CHUNK_FORMAT: u8 = 2;

/// Whether `known` covers everything a chunk with this `max_seq` could
/// hold, counting its legacy (origin 0) entry as `legacy`'s.
pub fn covers_max_seq(known: &VersionVector, max_seq: &VersionVector, legacy: Stamp) -> bool {
    max_seq.iter().all(|(origin, seq)| {
        known.has(if origin == 0 {
            legacy
        } else {
            Stamp::new(origin, seq)
        })
    })
}

/// Reads a header's format byte, rejecting one newer than this build.
fn read_format<R: Read>(r: &mut R) -> io::Result<u8> {
    let mut format = [0u8; 1];
    r.read_exact(&mut format)?;
    if (1..=CHUNK_FORMAT).contains(&format[0]) {
        Ok(format[0])
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "chunk file format {} is newer than this build supports",
                format[0]
            ),
        ))
    }
}

fn read_vector<R: Read>(r: &mut R) -> io::Result<VersionVector> {
    let mut vector = VersionVector::new();
    for _ in 0..read_u32(r)? {
        vector.observe(read_stamp(r)?);
    }
    Ok(vector)
}

fn write_stamp<W: Write>(w: &mut W, stamp: Stamp) -> io::Result<()> {
    w.write_all(&stamp.origin.to_le_bytes())?;
    w.write_all(&stamp.seq.to_le_bytes())
}

fn read_stamp<R: Read>(r: &mut R) -> io::Result<Stamp> {
    Ok(Stamp {
        origin: u64::from_le_bytes(read_arr8(r)?),
        seq: u64::from_le_bytes(read_arr8(r)?),
    })
}

fn write_meta<W: Write>(w: &mut W, m: &CellMeta) -> io::Result<()> {
    w.write_all(&m.created_at_ms.to_le_bytes())?;
    w.write_all(&m.modified_at_ms.to_le_bytes())?;
    w.write_all(&m.version.to_le_bytes())?;
    Ok(())
}

fn read_meta<R: Read>(r: &mut R) -> io::Result<CellMeta> {
    Ok(CellMeta {
        created_at_ms: u64::from_le_bytes(read_arr8(r)?),
        modified_at_ms: u64::from_le_bytes(read_arr8(r)?),
        version: u64::from_le_bytes(read_arr8(r)?),
    })
}

fn read_u32<R: Read>(r: &mut R) -> io::Result<u32> {
    Ok(u32::from_le_bytes(read_arr4(r)?))
}

fn read_arr4<R: Read>(r: &mut R) -> io::Result<[u8; 4]> {
    let mut b = [0u8; 4];
    r.read_exact(&mut b)?;
    Ok(b)
}

fn read_arr8<R: Read>(r: &mut R) -> io::Result<[u8; 8]> {
    let mut b = [0u8; 8];
    r.read_exact(&mut b)?;
    Ok(b)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A default-shaped chunk's cell count (3 axes, chunk_dim 32): the
    /// tests below use cell indices that only need to fit within this.
    const CELLS: usize = 32 * 32 * 32;

    #[test]
    fn roundtrip_and_sparsity() {
        let mut c = Chunk::new(CELLS);
        assert!(c.is_empty());

        c.set(0, 10, Value::Str("stone".into()), 1000);
        c.set(5, 10, Value::Str("air".into()), 1000);
        c.set(0, 11, Value::F64(3.5), 1000);
        c.set(31, 12, Value::I64(-7), 1000);

        assert!(!c.is_empty());
        assert_eq!(c.get(0, 10), Some(Value::Str("stone".into())));
        assert_eq!(c.get(5, 10), Some(Value::Str("air".into())));
        assert_eq!(c.get(1, 10), None); // untouched cell, same column
        assert_eq!(c.get(0, 11), Some(Value::F64(3.5)));
        assert_eq!(c.get(31, 12), Some(Value::I64(-7)));
        assert_eq!(c.get(0, 999), None); // column never created

        let mut buf = Vec::new();
        c.write_to(&mut buf).unwrap();
        assert_eq!(buf.len(), c.byte_len());

        let c2 = Chunk::read_from(&mut &buf[..], CELLS).unwrap();
        assert_eq!(c2.get(0, 10), Some(Value::Str("stone".into())));
        assert_eq!(c2.get(5, 10), Some(Value::Str("air".into())));
        assert_eq!(c2.get(0, 11), Some(Value::F64(3.5)));
        assert_eq!(c2.get(31, 12), Some(Value::I64(-7)));
    }

    #[test]
    fn overwrite_and_remove() {
        let mut c = Chunk::new(CELLS);
        c.set(100, 1, Value::I64(1), 1000);
        c.set(100, 1, Value::I64(2), 2000); // overwrite same cell/key
        assert_eq!(c.get(100, 1), Some(Value::I64(2)));

        c.remove(100, 1);
        assert_eq!(c.get(100, 1), None);
        assert!(c.is_empty());
    }

    #[test]
    fn bool_values_round_trip_through_write_to_and_read_from() {
        let mut c = Chunk::new(CELLS);
        c.set(0, 20, Value::Bool(true), 1000);
        c.set(5, 20, Value::Bool(false), 1000);

        assert_eq!(c.get(0, 20), Some(Value::Bool(true)));
        assert_eq!(c.get(5, 20), Some(Value::Bool(false)));
        assert_eq!(c.get(1, 20), None); // untouched cell, same column

        let mut buf = Vec::new();
        c.write_to(&mut buf).unwrap();
        assert_eq!(buf.len(), c.byte_len());

        let c2 = Chunk::read_from(&mut &buf[..], CELLS).unwrap();
        assert_eq!(c2.get(0, 20), Some(Value::Bool(true)));
        assert_eq!(c2.get(5, 20), Some(Value::Bool(false)));
    }

    #[test]
    fn bool_overwrite_and_remove() {
        let mut c = Chunk::new(CELLS);
        c.set(100, 1, Value::Bool(false), 1000);
        c.set(100, 1, Value::Bool(true), 2000); // overwrite same cell/key
        assert_eq!(c.get(100, 1), Some(Value::Bool(true)));

        c.remove(100, 1);
        assert_eq!(c.get(100, 1), None);
        assert!(c.is_empty());
    }

    #[test]
    fn a_fresh_set_starts_at_version_0_with_matching_created_and_modified() {
        let mut c = Chunk::new(CELLS);
        c.set(0, 1, Value::I64(1), 1000);
        assert_eq!(
            c.get_meta(0, 1),
            Some(CellMeta {
                created_at_ms: 1000,
                modified_at_ms: 1000,
                version: 0,
            })
        );
    }

    #[test]
    fn overwriting_increments_version_and_modified_but_not_created() {
        let mut c = Chunk::new(CELLS);
        c.set(0, 1, Value::I64(1), 1000);
        c.set(0, 1, Value::I64(2), 2000);
        c.set(0, 1, Value::I64(3), 3000);
        assert_eq!(
            c.get_meta(0, 1),
            Some(CellMeta {
                created_at_ms: 1000,
                modified_at_ms: 3000,
                version: 2,
            })
        );
    }

    #[test]
    fn setting_a_different_cell_in_the_same_column_does_not_affect_anothers_meta() {
        let mut c = Chunk::new(CELLS);
        c.set(0, 1, Value::I64(1), 1000);
        c.set(5, 1, Value::I64(2), 2000);
        c.set(0, 1, Value::I64(9), 3000); // overwrite cell 0 again

        assert_eq!(
            c.get_meta(0, 1),
            Some(CellMeta {
                created_at_ms: 1000,
                modified_at_ms: 3000,
                version: 1,
            })
        );
        assert_eq!(
            c.get_meta(5, 1),
            Some(CellMeta {
                created_at_ms: 2000,
                modified_at_ms: 2000,
                version: 0,
            })
        );
    }

    #[test]
    fn get_meta_of_an_unset_cell_or_unknown_key_is_none() {
        let mut c = Chunk::new(CELLS);
        c.set(0, 1, Value::I64(1), 1000);
        assert_eq!(c.get_meta(1, 1), None); // same column, untouched cell
        assert_eq!(c.get_meta(0, 999), None); // column never created
    }

    #[test]
    fn removing_then_setting_again_starts_a_fresh_history() {
        let mut c = Chunk::new(CELLS);
        c.set(0, 1, Value::I64(1), 1000);
        c.set(0, 1, Value::I64(2), 2000);
        c.remove(0, 1);
        c.set(0, 1, Value::I64(3), 5000);

        assert_eq!(
            c.get_meta(0, 1),
            Some(CellMeta {
                created_at_ms: 5000,
                modified_at_ms: 5000,
                version: 0,
            })
        );
    }

    #[test]
    fn meta_round_trips_through_write_to_and_read_from() {
        let mut c = Chunk::new(CELLS);
        c.set(0, 1, Value::Str("stone".into()), 1000);
        c.set(0, 1, Value::Str("air".into()), 2000); // overwrite -> version 1
        c.set(5, 2, Value::I64(0), 3000); // distinct key/column, distinct type

        let mut buf = Vec::new();
        c.write_to(&mut buf).unwrap();
        assert_eq!(buf.len(), c.byte_len());

        let c2 = Chunk::read_from(&mut &buf[..], CELLS).unwrap();
        assert_eq!(
            c2.get_meta(0, 1),
            Some(CellMeta {
                created_at_ms: 1000,
                modified_at_ms: 2000,
                version: 1,
            })
        );
        assert_eq!(
            c2.get_meta(5, 2),
            Some(CellMeta {
                created_at_ms: 3000,
                modified_at_ms: 3000,
                version: 0,
            })
        );
    }

    #[test]
    fn entries_by_local_idx_includes_bool_values() {
        let mut c = Chunk::new(CELLS);
        c.set(0, 1, Value::Bool(true), 1000);

        let entries = c.entries_by_local_idx();
        assert_eq!(
            entries[&0],
            vec![(
                1,
                Value::Bool(true),
                CellMeta {
                    created_at_ms: 1000,
                    modified_at_ms: 1000,
                    version: 0,
                }
            )]
        );
    }

    #[test]
    fn entries_by_local_idx_is_empty_for_an_empty_chunk() {
        let c = Chunk::new(CELLS);
        assert!(c.entries_by_local_idx().is_empty());
    }

    #[test]
    fn entries_by_local_idx_groups_multiple_keys_at_the_same_cell() {
        let mut c = Chunk::new(CELLS);
        c.set(0, 1, Value::Str("stone".into()), 1000);
        c.set(0, 2, Value::I64(7), 2000);
        c.set(5, 1, Value::Str("air".into()), 3000);

        let entries = c.entries_by_local_idx();
        assert_eq!(entries.len(), 2); // two distinct local indices: 0 and 5

        let at_0 = &entries[&0];
        assert_eq!(at_0.len(), 2);
        assert!(at_0.contains(&(
            1,
            Value::Str("stone".into()),
            CellMeta {
                created_at_ms: 1000,
                modified_at_ms: 1000,
                version: 0,
            }
        )));
        assert!(at_0.contains(&(
            2,
            Value::I64(7),
            CellMeta {
                created_at_ms: 2000,
                modified_at_ms: 2000,
                version: 0,
            }
        )));

        let at_5 = &entries[&5];
        assert_eq!(
            at_5,
            &vec![(
                1,
                Value::Str("air".into()),
                CellMeta {
                    created_at_ms: 3000,
                    modified_at_ms: 3000,
                    version: 0,
                }
            )]
        );
    }

    #[test]
    fn entries_by_local_idx_reflects_a_remove() {
        let mut c = Chunk::new(CELLS);
        c.set(0, 1, Value::I64(1), 1000);
        c.set(0, 2, Value::I64(2), 1000);
        c.remove(0, 1);

        let entries = c.entries_by_local_idx();
        assert_eq!(entries.len(), 1);
        assert_eq!(
            entries[&0],
            vec![(
                2,
                Value::I64(2),
                CellMeta {
                    created_at_ms: 1000,
                    modified_at_ms: 1000,
                    version: 0,
                }
            )]
        );
    }

    #[test]
    fn different_axis_counts_get_different_sized_chunks() {
        // 2 axes: 32^2 = 1024 cells/chunk, a much smaller presence bitmap
        // than the 3-axis default.
        let mut c = Chunk::new(chunk_cells(2, 32));
        c.set(0, 0, Value::I64(1), 1000);
        let mut buf = Vec::new();
        c.write_to(&mut buf).unwrap();
        assert_eq!(buf.len(), c.byte_len());

        let c2 = Chunk::read_from(&mut &buf[..], chunk_cells(2, 32)).unwrap();
        assert_eq!(c2.get(0, 0), Some(Value::I64(1)));
    }

    #[test]
    fn a_chunk_dim_giving_a_cell_count_not_a_multiple_of_8_is_handled_correctly() {
        // chunk_dim=3, 2 axes: 9 cells, not a multiple of 8 -- exercises the
        // `div_ceil(8)` presence-byte sizing (a plain `/ 8` would floor to 1
        // byte for 9 cells and panic indexing bit 8 into it).
        let cells = chunk_cells(2, 3);
        assert_eq!(cells, 9);

        let mut c = Chunk::new(cells);
        for idx in 0..cells {
            c.set(idx, 0, Value::I64(idx as i64), 1000);
        }
        for idx in 0..cells {
            assert_eq!(c.get(idx, 0), Some(Value::I64(idx as i64)));
        }

        let mut buf = Vec::new();
        c.write_to(&mut buf).unwrap();
        assert_eq!(buf.len(), c.byte_len());
        let c2 = Chunk::read_from(&mut &buf[..], cells).unwrap();
        for idx in 0..cells {
            assert_eq!(c2.get(idx, 0), Some(Value::I64(idx as i64)));
        }
    }

    // --- Bitset::rank's block-count acceleration ---
    //
    // The tests above only ever touch indices below 137 (well within
    // RANK_BLOCK_BYTES=64 bytes -- i.e. index 512 -- of bits), so they
    // never exercise `rank`'s "sum the whole blocks before this one" path
    // at all. These specifically spread across many blocks.

    #[test]
    fn rank_is_correct_across_many_block_boundaries() {
        // CELLS = 32,768 cells = 64 blocks of 512 bits each. An odd
        // stride so set cells land at varying offsets within their block,
        // not just at block-aligned positions.
        let mut c = Chunk::new(CELLS);
        let indices: Vec<usize> = (0..CELLS).step_by(137).collect();
        for (i, &idx) in indices.iter().enumerate() {
            c.set(idx, 42, Value::I64(i as i64), 1000);
        }
        for (i, &idx) in indices.iter().enumerate() {
            assert_eq!(
                c.get(idx, 42),
                Some(Value::I64(i as i64)),
                "wrong value at idx {idx}"
            );
        }
    }

    #[test]
    fn rank_is_correct_exactly_at_block_boundaries() {
        // 511/512/513 straddle the boundary between block 0 and block 1;
        // 1023/1024 straddle block 1/block 2 -- the off-by-one-prone edges
        // of the block-counts acceleration.
        let mut c = Chunk::new(CELLS);
        let boundary_indices = [0usize, 511, 512, 513, 1023, 1024, 1535, 1536];
        for (i, &idx) in boundary_indices.iter().enumerate() {
            c.set(idx, 7, Value::I64(i as i64), 1000);
        }
        for (i, &idx) in boundary_indices.iter().enumerate() {
            assert_eq!(
                c.get(idx, 7),
                Some(Value::I64(i as i64)),
                "wrong value at idx {idx}"
            );
        }
    }

    #[test]
    fn removing_a_cell_in_an_earlier_block_does_not_corrupt_a_later_blocks_ranks() {
        // A later cell's rank depends on the block_counts sum over every
        // earlier block -- this specifically checks that a `set(.., false)`
        // in block 0 correctly updates block_counts so a lookup in a much
        // later block still resolves to the right value.
        let mut c = Chunk::new(CELLS);
        c.set(10, 1, Value::I64(100), 1000); // block 0
        c.set(2000, 1, Value::I64(200), 2000); // block 3 (2000 / 512 = 3)
        assert_eq!(c.get(2000, 1), Some(Value::I64(200)));

        c.remove(10, 1);
        assert_eq!(c.get(10, 1), None);
        assert_eq!(
            c.get(2000, 1),
            Some(Value::I64(200)),
            "later block's value corrupted after removing an earlier cell"
        );
    }

    #[test]
    fn count_matches_set_cells_across_many_blocks() {
        // is_empty() (and, in World, chunk-file garbage collection) routes
        // through Bitset::count(), which now sums block_counts instead of
        // rescanning every byte -- exercise it across many blocks, both
        // ways.
        let mut c = Chunk::new(CELLS);
        let indices: Vec<usize> = (0..CELLS).step_by(97).collect();
        for &idx in &indices {
            c.set(idx, 3, Value::I64(0), 1000);
        }
        assert!(!c.is_empty());
        for &idx in &indices {
            c.remove(idx, 3);
        }
        assert!(c.is_empty());
    }

    #[test]
    fn from_bits_reconstructs_block_counts_matching_incremental_set() {
        // write_to/read_from (Bitset::from_bits) must produce a Bitset
        // whose rank() behaves identically to one built incrementally via
        // set() -- from_bits is the only other place a Bitset's
        // block_counts get built.
        let mut c = Chunk::new(CELLS);
        for idx in (0..CELLS).step_by(211) {
            c.set(idx, 5, Value::I64(idx as i64), 1000);
        }

        let mut buf = Vec::new();
        c.write_to(&mut buf).unwrap();
        let c2 = Chunk::read_from(&mut &buf[..], CELLS).unwrap();

        for idx in (0..CELLS).step_by(211) {
            assert_eq!(c2.get(idx, 5), Some(Value::I64(idx as i64)));
        }
    }

    fn round_trip(c: &Chunk) -> Chunk {
        let mut buf = Vec::new();
        c.write_to(&mut buf).unwrap();
        assert_eq!(buf.len(), c.byte_len());
        Chunk::read_from(&mut &buf[..], CELLS).unwrap()
    }

    const A: u64 = 0xA;
    const B: u64 = 0xB;

    fn vector(entries: &[(u64, u64)]) -> VersionVector {
        entries.iter().copied().collect()
    }

    #[test]
    fn remove_with_tombstone_only_records_one_where_a_value_existed() {
        let mut c = Chunk::new(CELLS);
        c.set(3, 1, Value::I64(7), 1000);
        assert!(c.remove_with_tombstone(3, 1, 2000, Stamp::new(A, 2)));
        assert_eq!(c.get(3, 1), None);
        assert_eq!(c.tombstone_at(3, 1), Some((2000, Stamp::new(A, 2))));

        assert!(!c.remove_with_tombstone(4, 1, 2000, Stamp::new(A, 3)));
        assert_eq!(c.tombstone_at(4, 1), None);
    }

    #[test]
    fn setting_a_cell_clears_its_tombstone() {
        let mut c = Chunk::new(CELLS);
        c.put_tombstone(3, 1, 1000, Stamp::new(A, 1));
        c.set(3, 1, Value::I64(7), 2000);
        assert_eq!(c.tombstone_at(3, 1), None);

        c.put_tombstone(5, 1, 1000, Stamp::new(A, 2));
        let meta = CellMeta {
            created_at_ms: 3000,
            modified_at_ms: 3000,
            version: 0,
        };
        c.set_replicated(5, 1, Value::I64(8), meta, Stamp::new(B, 1));
        assert_eq!(c.tombstone_at(5, 1), None);
    }

    #[test]
    fn tombstones_are_invisible_to_reads() {
        let mut c = Chunk::new(CELLS);
        c.set(3, 1, Value::I64(7), 1000);
        c.remove_with_tombstone(3, 1, 2000, Stamp::NONE);
        assert_eq!(c.get(3, 1), None);
        assert_eq!(c.get_meta(3, 1), None);
        assert_eq!(c.stamp_at(3, 1), None);
        assert!(c.entries_by_local_idx().is_empty());
    }

    #[test]
    fn a_chunk_holding_only_tombstones_is_not_empty() {
        let mut c = Chunk::new(CELLS);
        c.set(3, 1, Value::I64(7), 1000);
        c.remove_with_tombstone(3, 1, 2000, Stamp::NONE);
        assert!(!c.is_empty());
        c.purge_tombstones_older_than(u64::MAX);
        assert!(c.is_empty());
    }

    #[test]
    fn purge_drops_only_tombstones_older_than_the_cutoff() {
        let mut c = Chunk::new(CELLS);
        c.put_tombstone(1, 1, 1000, Stamp::NONE);
        c.put_tombstone(2, 1, 3000, Stamp::NONE);
        c.put_tombstone(3, 2, 500, Stamp::NONE);
        assert!(c.purge_tombstones_older_than(2000));
        assert_eq!(c.tombstone_at(1, 1), None);
        assert_eq!(c.tombstone_at(2, 1), Some((3000, Stamp::NONE)));
        assert_eq!(c.tombstone_at(3, 2), None);
        assert!(!c.purge_tombstones_older_than(2000));
    }

    #[test]
    fn remove_column_drops_its_tombstones_too() {
        let mut c = Chunk::new(CELLS);
        c.put_tombstone(1, 9, 1000, Stamp::NONE);
        assert!(c.remove_column(9));
        assert_eq!(c.tombstone_at(1, 9), None);
        assert!(c.is_empty());
    }

    #[test]
    fn stamps_are_kept_per_value_and_replaced_on_overwrite() {
        let mut c = Chunk::new(CELLS);
        c.set_stamped(1, 1, Value::I64(1), 1000, Stamp::new(A, 1));
        c.set_stamped(2, 1, Value::I64(2), 1000, Stamp::new(A, 2));
        assert_eq!(c.stamp_at(1, 1), Some(Stamp::new(A, 1)));
        c.set_stamped(1, 1, Value::I64(3), 2000, Stamp::new(B, 7));
        assert_eq!(c.stamp_at(1, 1), Some(Stamp::new(B, 7)));
        assert_eq!(c.stamp_at(2, 1), Some(Stamp::new(A, 2)));
        // Removing an earlier cell shifts ranks; stamps must follow.
        c.remove(1, 1);
        assert_eq!(c.stamp_at(2, 1), Some(Stamp::new(A, 2)));
        assert_eq!(
            c.set(4, 1, Value::I64(4), 1),
            CellMeta {
                created_at_ms: 1,
                modified_at_ms: 1,
                version: 0,
            }
        );
        assert_eq!(c.stamp_at(4, 1), Some(Stamp::NONE));
    }

    #[test]
    fn max_seq_tracks_every_stamp_per_origin_and_never_decreases() {
        let mut c = Chunk::new(CELLS);
        assert!(c.max_seq().is_empty());
        c.set_stamped(1, 1, Value::I64(1), 1000, Stamp::new(A, 5));
        c.set_stamped(2, 1, Value::I64(1), 1000, Stamp::new(A, 3));
        c.remove_with_tombstone(1, 1, 2000, Stamp::new(B, 9));
        assert_eq!(c.max_seq(), &vector(&[(A, 5), (B, 9)]));
        c.purge_tombstones_older_than(u64::MAX);
        c.remove(2, 1);
        assert_eq!(c.max_seq(), &vector(&[(A, 5), (B, 9)]));
    }

    #[test]
    fn stamps_tombstones_and_max_seq_round_trip_through_write_to_and_read_from() {
        let mut c = Chunk::new(CELLS);
        c.set_stamped(1, 1, Value::Str("a".into()), 1000, Stamp::new(A, 1));
        c.set_stamped(2, 1, Value::Str("b".into()), 1000, Stamp::new(A, 2));
        c.set_stamped(7, 2, Value::Bool(true), 1000, Stamp::new(B, 4));
        c.remove_with_tombstone(2, 1, 4000, Stamp::new(B, 5));
        c.put_tombstone(CELLS - 1, 7, 3000, Stamp::new(A, 6));

        let back = round_trip(&c);
        assert_eq!(back.get(1, 1), Some(Value::Str("a".into())));
        assert_eq!(back.stamp_at(1, 1), Some(Stamp::new(A, 1)));
        assert_eq!(back.stamp_at(7, 2), Some(Stamp::new(B, 4)));
        assert_eq!(back.get(2, 1), None);
        assert_eq!(back.tombstone_at(2, 1), Some((4000, Stamp::new(B, 5))));
        assert_eq!(
            back.tombstone_at(CELLS - 1, 7),
            Some((3000, Stamp::new(A, 6)))
        );
        assert_eq!(back.max_seq(), &vector(&[(A, 6), (B, 5)]));
    }

    #[test]
    fn read_max_seq_reads_just_the_header() {
        let mut c = Chunk::new(CELLS);
        c.set_stamped(1, 1, Value::I64(1), 1234, Stamp::new(A, 42));
        let mut buf = Vec::new();
        c.write_to(&mut buf).unwrap();
        // magic + format + count + one (origin, seq): the rest can be missing.
        assert_eq!(
            Chunk::read_max_seq(&mut &buf[..4 + 1 + 4 + 16]).unwrap(),
            Some(vector(&[(A, 42)]))
        );
    }

    /// The format before tombstones: no header, starts at `num_columns`,
    /// no stamps, no tombstone section.
    fn headerless_format_bytes() -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&1u32.to_le_bytes()); // num_columns
        buf.extend_from_slice(&5u32.to_le_bytes()); // key_id
        buf.push(Value::TAG_I64);
        let mut bits = vec![0u8; CELLS.div_ceil(8)];
        bits[0] = 0b0000_0100; // cell 2
        buf.extend_from_slice(&bits);
        for x in [100u64, 900, 3] {
            buf.extend_from_slice(&x.to_le_bytes()); // created, modified, version
        }
        buf.extend_from_slice(&42i64.to_le_bytes());
        buf
    }

    /// Format 1: `[magic][1][u64 newest_ms]`, the same columns as the
    /// header-less format, then tombstones with no stamps.
    fn format_1_bytes() -> Vec<u8> {
        let mut buf = CHUNK_MAGIC.to_vec();
        buf.push(1);
        buf.extend_from_slice(&900u64.to_le_bytes());
        buf.extend_from_slice(&headerless_format_bytes());
        buf.extend_from_slice(&1u32.to_le_bytes()); // one tombstone key
        buf.extend_from_slice(&6u32.to_le_bytes()); // key_id
        buf.extend_from_slice(&1u32.to_le_bytes()); // one cell
        buf.extend_from_slice(&9u32.to_le_bytes()); // local_idx
        buf.extend_from_slice(&800u64.to_le_bytes()); // removed_at_ms
        buf
    }

    #[test]
    fn older_format_files_load_with_every_stamp_none() {
        for buf in [headerless_format_bytes(), format_1_bytes()] {
            let c = Chunk::read_from(&mut &buf[..], CELLS).unwrap();
            assert_eq!(c.get(2, 5), Some(Value::I64(42)));
            assert_eq!(
                c.get_meta(2, 5),
                Some(CellMeta {
                    created_at_ms: 100,
                    modified_at_ms: 900,
                    version: 3,
                })
            );
            assert_eq!(c.stamp_at(2, 5), Some(Stamp::NONE));
            assert_eq!(c.max_seq(), &vector(&[(0, 0)]));
            assert_eq!(Chunk::read_max_seq(&mut &buf[..]).unwrap(), None);
            // Rewritten, it's in the current format.
            assert_eq!(round_trip(&c).get(2, 5), Some(Value::I64(42)));
        }
        let c = Chunk::read_from(&mut &format_1_bytes()[..], CELLS).unwrap();
        assert_eq!(c.tombstone_at(9, 6), Some((800, Stamp::NONE)));
    }

    #[test]
    fn a_newer_format_byte_is_rejected() {
        let mut buf = Vec::new();
        Chunk::new(CELLS).write_to(&mut buf).unwrap();
        buf[4] = CHUNK_FORMAT + 1;
        assert!(Chunk::read_from(&mut &buf[..], CELLS).is_err());
        assert!(Chunk::read_max_seq(&mut &buf[..]).is_err());
    }

    #[test]
    fn changes_since_reports_values_and_tombstones_the_vector_lacks() {
        let mut c = Chunk::new(CELLS);
        c.set_stamped(1, 1, Value::I64(1), 100, Stamp::new(A, 1));
        c.set_stamped(2, 1, Value::I64(2), 300, Stamp::new(A, 2));
        c.set_stamped(3, 1, Value::I64(3), 100, Stamp::new(B, 1));
        c.remove_with_tombstone(3, 1, 400, Stamp::new(B, 2));
        c.set(4, 1, Value::I64(4), 50); // legacy: Stamp::NONE

        let legacy = Stamp::new(crate::stamp::legacy_origin(0xC), 0);
        let mut changes = c.changes_since(&vector(&[(A, 1), (B, 1), (legacy.origin, 0)]), legacy);
        changes.sort_by_key(|(idx, ..)| *idx);
        assert_eq!(
            changes,
            vec![
                (
                    2,
                    1,
                    ChangeKind::Set(
                        Value::I64(2),
                        CellMeta {
                            created_at_ms: 300,
                            modified_at_ms: 300,
                            version: 0,
                        }
                    ),
                    Stamp::new(A, 2)
                ),
                (3, 1, ChangeKind::Removed(400), Stamp::new(B, 2)),
            ]
        );
        // Everything covered: nothing.
        assert!(c
            .changes_since(&vector(&[(A, 2), (B, 2), (legacy.origin, 0)]), legacy)
            .is_empty());
        // Without this holder's legacy entry: its legacy data too -- even
        // if another holder's is there.
        let other = crate::stamp::legacy_origin(0xD);
        assert_eq!(
            c.changes_since(&vector(&[(A, 2), (B, 2), (other, 0)]), legacy)
                .len(),
            1
        );
        // An empty vector: everything.
        assert_eq!(c.changes_since(&VersionVector::new(), legacy).len(), 4);
    }
}
