use crate::value::Value;
use std::collections::HashMap;
use std::io::{self, Read, Write};

/// Cells per axis within one chunk. 32^3 = 32,768 cells/chunk.
pub const CHUNK_DIM: u32 = 32;
pub const CHUNK_CELLS: usize = (CHUNK_DIM * CHUNK_DIM * CHUNK_DIM) as usize;
const PRESENCE_BYTES: usize = CHUNK_CELLS / 8; // 4096 bytes = one bit per cell

/// A fixed-size bitmap: one bit per cell in the chunk, marking whether that
/// cell has a value in a given column.
#[derive(Clone)]
struct Bitset {
    bits: Box<[u8; PRESENCE_BYTES]>,
}

impl Bitset {
    fn new() -> Self {
        Bitset {
            bits: Box::new([0u8; PRESENCE_BYTES]),
        }
    }

    fn get(&self, idx: usize) -> bool {
        (self.bits[idx >> 3] >> (idx & 7)) & 1 == 1
    }

    fn set(&mut self, idx: usize, v: bool) {
        let mask = 1u8 << (idx & 7);
        if v {
            self.bits[idx >> 3] |= mask;
        } else {
            self.bits[idx >> 3] &= !mask;
        }
    }

    /// Number of set bits strictly before `idx` -- i.e. this cell's position
    /// within the column's dense `values` vec, if it's present at all.
    fn rank(&self, idx: usize) -> usize {
        let byte_idx = idx >> 3;
        let mut count: usize = self.bits[..byte_idx]
            .iter()
            .map(|b| b.count_ones() as usize)
            .sum();
        let bit_in_byte = idx & 7;
        if bit_in_byte > 0 {
            let partial_mask = (1u8 << bit_in_byte) - 1;
            count += (self.bits[byte_idx] & partial_mask).count_ones() as usize;
        }
        count
    }

    fn count(&self) -> usize {
        self.bits.iter().map(|b| b.count_ones() as usize).sum()
    }
}

/// One column's storage: which cells have this key, and their values packed
/// densely (no gaps) in cell order.
enum ColumnData {
    F64(Vec<f64>),
    I64(Vec<i64>),
    Str(Vec<String>),
}

struct Column {
    presence: Bitset,
    data: ColumnData,
}

/// A CHUNK_DIM^3 block of cells, stored columnarly: one sparse array per key
/// that actually appears *somewhere in this chunk*, rather than one hashmap
/// per cell. Columns are created lazily on first write, so a chunk that only
/// ever sees 2 distinct keys allocates exactly 2 columns, no matter how many
/// keys exist elsewhere in the world.
pub struct Chunk {
    columns: HashMap<u32, Column>,
}

impl Chunk {
    pub fn new() -> Self {
        Chunk {
            columns: HashMap::new(),
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
        })
    }

    pub fn set(&mut self, local_idx: usize, key_id: u32, value: Value) {
        let col = self.columns.entry(key_id).or_insert_with(|| Column {
            presence: Bitset::new(),
            data: match &value {
                Value::F64(_) => ColumnData::F64(Vec::new()),
                Value::I64(_) => ColumnData::I64(Vec::new()),
                Value::Str(_) => ColumnData::Str(Vec::new()),
            },
        });

        let already_present = col.presence.get(local_idx);
        let rank = col.presence.rank(local_idx);

        if already_present {
            match (&mut col.data, value) {
                (ColumnData::F64(v), Value::F64(x)) => v[rank] = x,
                (ColumnData::I64(v), Value::I64(x)) => v[rank] = x,
                (ColumnData::Str(v), Value::Str(x)) => v[rank] = x,
                _ => panic!(
                    "key {key_id} already holds a different value type in this chunk; \
                     this prototype doesn't support changing a key's type"
                ),
            }
        } else {
            col.presence.set(local_idx, true);
            match (&mut col.data, value) {
                (ColumnData::F64(v), Value::F64(x)) => v.insert(rank, x),
                (ColumnData::I64(v), Value::I64(x)) => v.insert(rank, x),
                (ColumnData::Str(v), Value::Str(x)) => v.insert(rank, x),
                _ => panic!(
                    "key {key_id} already holds a different value type in this chunk; \
                     this prototype doesn't support changing a key's type"
                ),
            }
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
                }
            }
        }
    }

    /// True if no cell in this chunk has any value set -- such chunks aren't
    /// written to disk at all (see `World::flush_one`), which is how a mostly
    /// empty 10,000^3 world avoids allocating 30 million chunk files.
    pub fn is_empty(&self) -> bool {
        self.columns.values().all(|c| c.presence.count() == 0)
    }

    // --- Binary format ---
    //
    //   [u32 num_columns]
    //   repeated num_columns times, sorted by key_id:
    //     [u32 key_id]
    //     [u8  type_tag]              (0=Str, 1=F64, 2=I64)
    //     [4096 bytes presence bitmap]
    //     values, one per set bit in the bitmap, in cell-index order:
    //       F64/I64: 8 bytes little-endian
    //       Str:     [u32 len][len bytes, utf-8]
    //
    // There is no per-cell overhead beyond 1 bit in the presence map: a cell
    // that doesn't use a key costs nothing but that bit.

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
                    w.write_all(&*col.presence.bits)?;
                    for x in v {
                        w.write_all(&x.to_le_bytes())?;
                    }
                }
                ColumnData::I64(v) => {
                    w.write_all(&[Value::TAG_I64])?;
                    w.write_all(&*col.presence.bits)?;
                    for x in v {
                        w.write_all(&x.to_le_bytes())?;
                    }
                }
                ColumnData::Str(v) => {
                    w.write_all(&[Value::TAG_STR])?;
                    w.write_all(&*col.presence.bits)?;
                    for s in v {
                        let bytes = s.as_bytes();
                        w.write_all(&(bytes.len() as u32).to_le_bytes())?;
                        w.write_all(bytes)?;
                    }
                }
            }
        }
        Ok(())
    }

    pub fn read_from<R: Read>(r: &mut R) -> io::Result<Self> {
        let num_columns = read_u32(r)?;
        let mut columns = HashMap::with_capacity(num_columns as usize);

        for _ in 0..num_columns {
            let key_id = read_u32(r)?;

            let mut tag = [0u8; 1];
            r.read_exact(&mut tag)?;

            let mut bits = Box::new([0u8; PRESENCE_BYTES]);
            r.read_exact(&mut *bits)?;
            let presence = Bitset { bits };
            let count = presence.count();

            let data = match tag[0] {
                t if t == Value::TAG_F64 => {
                    let mut v = Vec::with_capacity(count);
                    for _ in 0..count {
                        v.push(f64::from_le_bytes(read_arr8(r)?));
                    }
                    ColumnData::F64(v)
                }
                t if t == Value::TAG_I64 => {
                    let mut v = Vec::with_capacity(count);
                    for _ in 0..count {
                        v.push(i64::from_le_bytes(read_arr8(r)?));
                    }
                    ColumnData::I64(v)
                }
                t if t == Value::TAG_STR => {
                    let mut v = Vec::with_capacity(count);
                    for _ in 0..count {
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
                other => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("corrupt chunk: unknown type tag {other}"),
                    ))
                }
            };

            columns.insert(key_id, Column { presence, data });
        }

        Ok(Chunk { columns })
    }

    /// Exact on-disk size in bytes, for reporting/benchmarking.
    pub fn byte_len(&self) -> usize {
        let mut n = 4;
        for col in self.columns.values() {
            n += 4 + 1 + PRESENCE_BYTES;
            n += match &col.data {
                ColumnData::F64(v) => v.len() * 8,
                ColumnData::I64(v) => v.len() * 8,
                ColumnData::Str(v) => v.iter().map(|s| 4 + s.len()).sum(),
            };
        }
        n
    }
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

    #[test]
    fn roundtrip_and_sparsity() {
        let mut c = Chunk::new();
        assert!(c.is_empty());

        c.set(0, 10, Value::Str("stone".into()));
        c.set(5, 10, Value::Str("air".into()));
        c.set(0, 11, Value::F64(3.5));
        c.set(31, 12, Value::I64(-7));

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

        let c2 = Chunk::read_from(&mut &buf[..]).unwrap();
        assert_eq!(c2.get(0, 10), Some(Value::Str("stone".into())));
        assert_eq!(c2.get(5, 10), Some(Value::Str("air".into())));
        assert_eq!(c2.get(0, 11), Some(Value::F64(3.5)));
        assert_eq!(c2.get(31, 12), Some(Value::I64(-7)));
    }

    #[test]
    fn overwrite_and_remove() {
        let mut c = Chunk::new();
        c.set(100, 1, Value::I64(1));
        c.set(100, 1, Value::I64(2)); // overwrite same cell/key
        assert_eq!(c.get(100, 1), Some(Value::I64(2)));

        c.remove(100, 1);
        assert_eq!(c.get(100, 1), None);
        assert!(c.is_empty());
    }
}
