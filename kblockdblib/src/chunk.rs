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
/// one.
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
}

impl Chunk {
    pub fn new(cell_count: usize) -> Self {
        Chunk {
            columns: HashMap::new(),
            presence_bytes: cell_count.div_ceil(8),
        }
    }

    pub fn get(&self, local_idx: usize, key_id: u32) -> Option<Value> {
        let col = self.columns.get(&key_id)?;
        if !col.presence.get(local_idx) {
            return None;
        }
        let rank = col.presence.rank(local_idx);
        Some(match &col.data {
            ColumnData::F64(v) => Value::F64(v[rank]),
            ColumnData::I64(v) => Value::I64(v[rank]),
            ColumnData::Str(v) => Value::Str(v[rank].clone()),
            ColumnData::Bool(v) => Value::Bool(v[rank]),
        })
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
    /// `now_ms` is milliseconds since the Unix epoch, supplied by the
    /// caller (`World`) rather than read here, so this stays a pure
    /// function of its arguments -- deterministic and easy to test without
    /// depending on wall-clock time.
    pub fn set(&mut self, local_idx: usize, key_id: u32, value: Value, now_ms: u64) {
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
            let meta = &mut col.meta[rank];
            meta.modified_at_ms = now_ms;
            meta.version += 1;
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
            col.meta.insert(
                rank,
                CellMeta {
                    created_at_ms: now_ms,
                    modified_at_ms: now_ms,
                    version: 0,
                },
            );
        }
    }

    pub fn remove(&mut self, local_idx: usize, key_id: u32) {
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
            }
        }
    }

    /// True if no cell in this chunk has any value set -- such chunks aren't
    /// written to disk at all (see `World::flush_one`), which is how a mostly
    /// empty 10,000^3 world avoids allocating 30 million chunk files.
    pub fn is_empty(&self) -> bool {
        self.columns.values().all(|c| c.presence.count() == 0)
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
                let value = match &col.data {
                    ColumnData::F64(v) => Value::F64(v[rank]),
                    ColumnData::I64(v) => Value::I64(v[rank]),
                    ColumnData::Str(v) => Value::Str(v[rank].clone()),
                    ColumnData::Bool(v) => Value::Bool(v[rank]),
                };
                out.entry(local_idx)
                    .or_default()
                    .push((key_id, value, col.meta[rank]));
            }
        }
        out
    }

    // --- Binary format ---
    //
    //   [u32 num_columns]
    //   repeated num_columns times, sorted by key_id:
    //     [u32 key_id]
    //     [u8  type_tag]              (0=Str, 1=F64, 2=I64, 3=Bool)
    //     [4096 bytes presence bitmap]
    //     entries, one per set bit in the bitmap, in cell-index order:
    //       [u64 created_at_ms][u64 modified_at_ms][u64 version]  (CellMeta)
    //       then the value itself:
    //         F64/I64: 8 bytes little-endian
    //         Str:     [u32 len][len bytes, utf-8]
    //         Bool:    1 byte (0 or 1)
    //
    // There is no per-cell overhead beyond 1 bit in the presence map plus
    // CellMeta's 24 bytes: a cell that doesn't use a key costs nothing but
    // that bit.

    pub fn write_to<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_all(&(self.columns.len() as u32).to_le_bytes())?;

        let mut ids: Vec<&u32> = self.columns.keys().collect();
        ids.sort();

        for &key_id in ids {
            let col = &self.columns[&key_id];
            w.write_all(&key_id.to_le_bytes())?;
            match &col.data {
                ColumnData::F64(v) => {
                    w.write_all(&[Value::TAG_F64])?;
                    w.write_all(&col.presence.bits)?;
                    for (x, m) in v.iter().zip(&col.meta) {
                        write_meta(w, m)?;
                        w.write_all(&x.to_le_bytes())?;
                    }
                }
                ColumnData::I64(v) => {
                    w.write_all(&[Value::TAG_I64])?;
                    w.write_all(&col.presence.bits)?;
                    for (x, m) in v.iter().zip(&col.meta) {
                        write_meta(w, m)?;
                        w.write_all(&x.to_le_bytes())?;
                    }
                }
                ColumnData::Str(v) => {
                    w.write_all(&[Value::TAG_STR])?;
                    w.write_all(&col.presence.bits)?;
                    for (s, m) in v.iter().zip(&col.meta) {
                        write_meta(w, m)?;
                        let bytes = s.as_bytes();
                        w.write_all(&(bytes.len() as u32).to_le_bytes())?;
                        w.write_all(bytes)?;
                    }
                }
                ColumnData::Bool(v) => {
                    w.write_all(&[Value::TAG_BOOL])?;
                    w.write_all(&col.presence.bits)?;
                    for (b, m) in v.iter().zip(&col.meta) {
                        write_meta(w, m)?;
                        w.write_all(&[u8::from(*b)])?;
                    }
                }
            }
        }
        Ok(())
    }

    /// `cell_count` must be the owning world's `chunk_cells(axes, chunk_dim)`
    /// -- the caller (`World`) knows this from its own `axes`/`chunk_dim`,
    /// persisted in `world.txt`, so it isn't stored redundantly in every
    /// chunk file.
    pub fn read_from<R: Read>(r: &mut R, cell_count: usize) -> io::Result<Self> {
        let presence_bytes = cell_count.div_ceil(8);
        let num_columns = read_u32(r)?;
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
            let data = match tag[0] {
                t if t == Value::TAG_F64 => {
                    let mut v = Vec::with_capacity(count);
                    for _ in 0..count {
                        meta.push(read_meta(r)?);
                        v.push(f64::from_le_bytes(read_arr8(r)?));
                    }
                    ColumnData::F64(v)
                }
                t if t == Value::TAG_I64 => {
                    let mut v = Vec::with_capacity(count);
                    for _ in 0..count {
                        meta.push(read_meta(r)?);
                        v.push(i64::from_le_bytes(read_arr8(r)?));
                    }
                    ColumnData::I64(v)
                }
                t if t == Value::TAG_STR => {
                    let mut v = Vec::with_capacity(count);
                    for _ in 0..count {
                        meta.push(read_meta(r)?);
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
                        meta.push(read_meta(r)?);
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
                },
            );
        }

        Ok(Chunk {
            columns,
            presence_bytes,
        })
    }

    /// Exact on-disk size in bytes, for reporting/benchmarking.
    #[cfg(test)]
    pub fn byte_len(&self) -> usize {
        let mut n = 4;
        for col in self.columns.values() {
            n += 4 + 1 + self.presence_bytes;
            n += col.meta.len() * 24; // CellMeta: 3 u64 fields
            n += match &col.data {
                ColumnData::F64(v) => v.len() * 8,
                ColumnData::I64(v) => v.len() * 8,
                ColumnData::Str(v) => v.iter().map(|s| 4 + s.len()).sum(),
                ColumnData::Bool(v) => v.len(),
            };
        }
        n
    }
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
}
