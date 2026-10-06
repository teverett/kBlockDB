//! A minimal on-disk LSM (log-structured merge) index: a durable
//! `value_bytes -> {present coords}` multimap that never needs its whole
//! content in memory at once -- what makes `kblockdblib::index::ValueIndex`
//! (and so `World::create_index`) viable on a world with far more matching
//! cells than fit in RAM.
//!
//! **Shape.** Writes (`insert`/`remove`, the latter a tombstone -- there's
//! no in-place update) land in an in-memory `memtable`, a small sorted
//! `(value_bytes, coord) -> presence` map, logged to an append-only `wal.log`
//! first so a crash before the next flush doesn't lose them. Once the
//! memtable passes [`MEMTABLE_FLUSH_THRESHOLD`] entries, it's written out
//! as a new immutable, sorted segment file (`<id>.seg`) and the WAL is
//! reset. Once segment files accumulate past [`COMPACT_SEGMENT_THRESHOLD`],
//! they're all merged into one (a full, not leveled/tiered, compaction --
//! see `compact`'s doc comment for why that's the deliberate simplification
//! here).
//!
//! **Reads.** `lookup(value)` checks the memtable, then every segment
//! newest-to-oldest, keeping only the first (= newest) entry seen per
//! coordinate -- a coordinate's value can change and change back, so an
//! older segment's stale entry for `value` must never shadow a newer one.
//! Each segment is searched without reading its full content: a small
//! sparse "checkpoint" index (one entry per [`SPARSE_INDEX_INTERVAL`]
//! records, loaded into memory when the segment is opened) locates roughly
//! where a value's records start, and the search scans forward from there
//! -- bounded, not a full-file read, the same trick an SSTable's sparse
//! index uses.
//!
//! **What this doesn't do.** No bloom filters (a lookup for a value with no
//! matches still does one bounded scan per segment, not an early-out), no
//! leveled/tiered compaction (just one tier, fully merged on threshold), no
//! concurrent/async compaction (it runs inline, synchronously, the moment a
//! flush crosses the segment-count threshold). All three are real,
//! addressable costs if this ever needs to scale further -- not correctness
//! gaps.

use std::collections::{BTreeMap, BinaryHeap, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::ops::Bound;
use std::path::{Path, PathBuf};

/// How many entries accumulate in the in-memory memtable before it's
/// flushed to a new immutable segment file -- bounds this structure's RAM
/// use to O(this), not O(how many cells currently match something), which
/// is the entire point for a world with far more of those than fit in
/// memory.
const MEMTABLE_FLUSH_THRESHOLD: usize = 10_000;

/// Once this many segment files accumulate, the flush that crosses the
/// threshold triggers a full compaction (every segment merged into one,
/// tombstones and superseded entries dropped) instead of just adding one
/// more. Bounds a lookup's cost to "one bounded scan per segment, for at
/// most this many segments" and reclaims the space old tombstones would
/// otherwise waste forever.
const COMPACT_SEGMENT_THRESHOLD: usize = 8;

/// Every this-many-th record in a segment gets a sparse-index checkpoint
/// (its value, coordinate, and byte offset) kept in memory once the
/// segment is open -- see this module's doc comment. Memory cost per open
/// segment is its entry count divided by this, not its full size.
const SPARSE_INDEX_INTERVAL: u32 = 128;

type CoordVec = Vec<i32>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Flag {
    Present,
    Tombstone,
}

impl Flag {
    fn to_byte(self) -> u8 {
        match self {
            Flag::Present => 0,
            Flag::Tombstone => 1,
        }
    }

    fn from_byte(b: u8) -> io::Result<Flag> {
        match b {
            0 => Ok(Flag::Present),
            1 => Ok(Flag::Tombstone),
            other => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("corrupt lsm segment: unknown flag byte {other}"),
            )),
        }
    }
}

/// One sparse-index entry: the value and coordinate of the record at
/// `offset` (byte offset from the start of the segment file), which is the
/// `record_index`-th record in the file -- see this module's doc comment.
#[derive(Debug, Clone)]
struct Checkpoint {
    offset: u64,
    record_index: u32,
    value: Vec<u8>,
    coord: CoordVec,
}

/// An immutable, already-flushed segment: its path and its in-memory
/// sparse index (see this module's doc comment), loaded once when the
/// segment is opened. Never holds an open file handle -- lookups/scans
/// open the file fresh each time, since segments are read rarely compared
/// to how long they live.
struct Segment {
    path: PathBuf,
    entry_count: u32,
    checkpoints: Vec<Checkpoint>,
}

impl Segment {
    /// Reads `path`'s header (entry count) and footer (sparse index) --
    /// not its records, which stay on disk until actually scanned.
    fn open(path: PathBuf) -> io::Result<Segment> {
        let mut f = File::open(&path)?;
        let entry_count = read_u32(&mut f)?;
        f.seek(SeekFrom::End(-8))?;
        let footer_offset = read_u64(&mut f)?;
        f.seek(SeekFrom::Start(footer_offset))?;
        let mut r = BufReader::new(f);
        let checkpoint_count = read_u32(&mut r)?;
        let mut checkpoints = Vec::with_capacity(checkpoint_count as usize);
        for _ in 0..checkpoint_count {
            let offset = read_u64(&mut r)?;
            let record_index = read_u32(&mut r)?;
            let value = read_bytes(&mut r)?;
            let coord = read_coord(&mut r)?;
            checkpoints.push(Checkpoint {
                offset,
                record_index,
                value,
                coord,
            });
        }
        Ok(Segment {
            path,
            entry_count,
            checkpoints,
        })
    }

    /// Every `(coord, flag)` this segment records for `target` -- found by
    /// seeking to the latest checkpoint at or before `target` (or the
    /// start of the records, if none qualifies) and scanning forward only
    /// until the sorted order guarantees nothing more can match. Bounded
    /// by `SPARSE_INDEX_INTERVAL` plus however many records actually share
    /// `target`'s value.
    fn lookup(&self, target: &[u8]) -> io::Result<Vec<(CoordVec, Flag)>> {
        // The checkpoint strictly *before* `target` -- not the last one
        // *at or before* it (`<=`), which, when many consecutive records
        // share `target`'s exact value, can itself already be in the
        // middle of that run, well past where it starts. Checkpoint
        // values are non-decreasing, so the one right before the first
        // index with `value >= target` is guaranteed to sit before every
        // record equal to `target`, wherever that run begins.
        let start = self
            .checkpoints
            .partition_point(|c| c.value.as_slice() < target);
        let (offset, record_index) = match start.checked_sub(1) {
            Some(i) => (self.checkpoints[i].offset, self.checkpoints[i].record_index),
            None => (4, 0), // just past the entry-count header; no checkpoint qualifies
        };
        let mut f = BufReader::new(File::open(&self.path)?);
        f.seek(SeekFrom::Start(offset))?;
        let mut remaining = self.entry_count - record_index;
        let mut out = Vec::new();
        while remaining > 0 {
            let (value, coord, flag) = read_record(&mut f)?;
            remaining -= 1;
            match value.as_slice().cmp(target) {
                std::cmp::Ordering::Less => continue,
                std::cmp::Ordering::Equal => out.push((coord, flag)),
                std::cmp::Ordering::Greater => break,
            }
        }
        Ok(out)
    }

    /// Every `(value, coord, flag)` this segment records with a value
    /// inside `[lower, upper)` (per `Bound`'s own inclusive/exclusive
    /// semantics on each end) -- same bounded-scan idea as `lookup`,
    /// generalized from "exactly one value" to "a sorted span of them":
    /// seek to the latest checkpoint strictly before `lower` (or the start
    /// of the records, if none qualifies), then scan forward until a
    /// record's value no longer satisfies `upper`, at which point sorted
    /// order guarantees nothing further in the file can either.
    fn range(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> io::Result<Vec<(Vec<u8>, CoordVec, Flag)>> {
        let start = match lower {
            Bound::Unbounded => 0,
            Bound::Included(v) | Bound::Excluded(v) => {
                self.checkpoints.partition_point(|c| c.value.as_slice() < v)
            }
        };
        let (offset, record_index) = match start.checked_sub(1) {
            Some(i) => (self.checkpoints[i].offset, self.checkpoints[i].record_index),
            None => (4, 0), // just past the entry-count header; no checkpoint qualifies
        };
        let mut f = BufReader::new(File::open(&self.path)?);
        f.seek(SeekFrom::Start(offset))?;
        let mut remaining = self.entry_count - record_index;
        let mut out = Vec::new();
        while remaining > 0 {
            let (value, coord, flag) = read_record(&mut f)?;
            remaining -= 1;
            if !satisfies_upper(&value, upper) {
                break;
            }
            if !satisfies_lower(&value, lower) {
                continue;
            }
            out.push((value, coord, flag));
        }
        Ok(out)
    }

    /// Every record in this segment, in file order (= sorted order) -- for
    /// `compact`'s merge. Reads sequentially; never seeks past the footer.
    fn iter_all(&self) -> io::Result<SegmentRecords> {
        let mut f = File::open(&self.path)?;
        f.seek(SeekFrom::Start(4))?; // past the entry-count header
        Ok(SegmentRecords {
            reader: BufReader::new(f),
            remaining: self.entry_count,
        })
    }
}

/// Sequential reader over one segment's records, oldest-offset-first =
/// sorted order -- see `Segment::iter_all`.
struct SegmentRecords {
    reader: BufReader<File>,
    remaining: u32,
}

impl SegmentRecords {
    fn next(&mut self) -> io::Result<Option<(Vec<u8>, CoordVec, Flag)>> {
        if self.remaining == 0 {
            return Ok(None);
        }
        self.remaining -= 1;
        read_record(&mut self.reader).map(Some)
    }
}

fn satisfies_lower(value: &[u8], lower: Bound<&[u8]>) -> bool {
    match lower {
        Bound::Unbounded => true,
        Bound::Included(b) => value >= b,
        Bound::Excluded(b) => value > b,
    }
}

fn satisfies_upper(value: &[u8], upper: Bound<&[u8]>) -> bool {
    match upper {
        Bound::Unbounded => true,
        Bound::Included(b) => value <= b,
        Bound::Excluded(b) => value < b,
    }
}

/// Whether `[lower, upper)` (per `Bound`'s own semantics on each end)
/// provably contains no value at all -- `lower` strictly after `upper`,
/// or the two equal with at least one side excluded. `Unbounded` on
/// either side can never make a range empty by itself.
fn range_is_empty(lower: &Bound<Vec<u8>>, upper: &Bound<Vec<u8>>) -> bool {
    let (lower_value, lower_inclusive) = match lower {
        Bound::Unbounded => return false,
        Bound::Included(v) => (v, true),
        Bound::Excluded(v) => (v, false),
    };
    let (upper_value, upper_inclusive) = match upper {
        Bound::Unbounded => return false,
        Bound::Included(v) => (v, true),
        Bound::Excluded(v) => (v, false),
    };
    match lower_value.cmp(upper_value) {
        std::cmp::Ordering::Greater => true,
        std::cmp::Ordering::Equal => !(lower_inclusive && upper_inclusive),
        std::cmp::Ordering::Less => false,
    }
}

/// `bound.as_ref().map(Vec::as_slice)`, spelled out: `Bound::as_ref`
/// alone would hand back `Bound<&Vec<u8>>`, and `satisfies_lower`/
/// `satisfies_upper`/`Segment::range` all want `Bound<&[u8]>`.
fn bound_as_slice(bound: &Bound<Vec<u8>>) -> Bound<&[u8]> {
    match bound {
        Bound::Included(v) => Bound::Included(v.as_slice()),
        Bound::Excluded(v) => Bound::Excluded(v.as_slice()),
        Bound::Unbounded => Bound::Unbounded,
    }
}

/// A durable `value_bytes -> {present coords}` multimap for one key -- see
/// this module's doc comment for the on-disk shape. One `LsmIndex` lives at
/// its own directory (`kblockdblib::index::ValueIndex` gives each indexed
/// key its own, named by key id).
pub struct LsmIndex {
    dir: PathBuf,
    /// Sorted by value, then by coordinate within a value -- exactly the
    /// order a flush needs to write a segment in, for free.
    memtable: BTreeMap<Vec<u8>, BTreeMap<CoordVec, Flag>>,
    memtable_len: usize,
    segments: Vec<Segment>,
    next_segment_id: u64,
    wal: File,
    flush_threshold: usize,
    compact_threshold: usize,
}

impl LsmIndex {
    /// Creates a brand new, empty index at `dir` (which must not already
    /// hold one -- `World::create_index`'s caller is responsible for not
    /// calling this on an already-indexed key).
    pub fn create(dir: &Path) -> io::Result<LsmIndex> {
        fs::create_dir_all(dir)?;
        Self::open(dir)
    }

    /// Opens the index at `dir`, which may be freshly created (empty) or
    /// hold segments and a WAL from an earlier run -- either way, this
    /// restores exactly the state `dir`'s files durably recorded: every
    /// segment is picked up (header + sparse index only, not its records --
    /// see `Segment::open`), and the WAL (if any) is replayed into a fresh
    /// memtable, so a write that landed after the last flush but before a
    /// crash isn't lost.
    pub fn open(dir: &Path) -> io::Result<LsmIndex> {
        Self::open_with_thresholds(dir, MEMTABLE_FLUSH_THRESHOLD, COMPACT_SEGMENT_THRESHOLD)
    }

    /// `open`, with the flush/compaction thresholds overridden -- for
    /// tests that need to exercise a flush or a compaction without writing
    /// tens of thousands of entries first.
    pub fn open_with_thresholds(
        dir: &Path,
        flush_threshold: usize,
        compact_threshold: usize,
    ) -> io::Result<LsmIndex> {
        fs::create_dir_all(dir)?;
        let mut segment_paths: Vec<(u64, PathBuf)> = Vec::new();
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let Some(id_str) = name.to_str().and_then(|n| n.strip_suffix(".seg")) else {
                continue;
            };
            let Ok(id) = id_str.parse::<u64>() else {
                continue;
            };
            segment_paths.push((id, entry.path()));
        }
        segment_paths.sort_by_key(|(id, _)| *id);
        let next_segment_id = segment_paths.last().map(|(id, _)| id + 1).unwrap_or(0);
        let segments = segment_paths
            .into_iter()
            .map(|(_, path)| Segment::open(path))
            .collect::<io::Result<Vec<_>>>()?;

        let wal_path = dir.join("wal.log");
        let mut memtable: BTreeMap<Vec<u8>, BTreeMap<CoordVec, Flag>> = BTreeMap::new();
        let mut memtable_len = 0;
        if wal_path.exists() {
            let mut r = BufReader::new(File::open(&wal_path)?);
            while let Some((value, coord, flag)) = read_record_opt(&mut r)? {
                let inner = memtable.entry(value).or_default();
                if inner.insert(coord, flag).is_none() {
                    memtable_len += 1;
                }
            }
        }
        let wal = OpenOptions::new().create(true).append(true).open(&wal_path)?;

        Ok(LsmIndex {
            dir: dir.to_path_buf(),
            memtable,
            memtable_len,
            segments,
            next_segment_id,
            wal,
            flush_threshold,
            compact_threshold,
        })
    }

    /// Records `coord` as currently holding `value`.
    pub fn insert(&mut self, value: Vec<u8>, coord: CoordVec) -> io::Result<()> {
        self.apply(value, coord, Flag::Present)
    }

    /// Records that `coord` no longer holds `value` (a tombstone -- see
    /// this module's doc comment).
    pub fn remove(&mut self, value: Vec<u8>, coord: CoordVec) -> io::Result<()> {
        self.apply(value, coord, Flag::Tombstone)
    }

    fn apply(&mut self, value: Vec<u8>, coord: CoordVec, flag: Flag) -> io::Result<()> {
        write_record(&mut self.wal, &value, &coord, flag)?;
        self.wal.flush()?;
        let inner = self.memtable.entry(value).or_default();
        if inner.insert(coord, flag).is_none() {
            self.memtable_len += 1;
        }
        if self.memtable_len >= self.flush_threshold {
            self.flush()?;
        }
        Ok(())
    }

    /// Every coordinate currently holding `value` -- memtable first (it's
    /// always the newest state), then every segment newest-to-oldest,
    /// keeping only the first (newest) entry seen per coordinate and
    /// reporting it iff that entry is `Present` -- see this module's doc
    /// comment on why segment recency order matters here.
    pub fn lookup(&self, value: &[u8]) -> io::Result<Vec<CoordVec>> {
        let mut seen: std::collections::HashMap<CoordVec, Flag> = std::collections::HashMap::new();
        if let Some(coords) = self.memtable.get(value) {
            for (coord, &flag) in coords {
                seen.entry(coord.clone()).or_insert(flag);
            }
        }
        for segment in self.segments.iter().rev() {
            for (coord, flag) in segment.lookup(value)? {
                seen.entry(coord).or_insert(flag);
            }
        }
        Ok(seen
            .into_iter()
            .filter(|(_, flag)| *flag == Flag::Present)
            .map(|(coord, _)| coord)
            .collect())
    }

    /// Every coordinate currently holding a value inside `[lower, upper)`
    /// -- `lookup`, generalized from one exact value to a sorted span of
    /// them. Resolution is keyed by `(value, coord)`, not just `coord`:
    /// a write that *moves* a coordinate from one value to another always
    /// tombstones its old `(value, coord)` entry (see `index::ValueIndex::
    /// record`), so at most one `(value, coord)` pair for a given
    /// coordinate is ever `Present` across the whole index at once --
    /// keying on the pair instead of the bare coordinate just lets two
    /// *different* coordinates' histories under two different values in
    /// range resolve independently, the same way `lookup` already relies
    /// on for one value at a time.
    pub fn range(&self, lower: Bound<Vec<u8>>, upper: Bound<Vec<u8>>) -> io::Result<Vec<CoordVec>> {
        // `BTreeMap::range` panics on a reversed or degenerate (equal,
        // both excluded) bound pair -- a caller translating a `WHERE`
        // clause like `key > 5 AND key < 5` can produce exactly that, and
        // it's a legitimate "no matches" query, not a bug. Checked once,
        // up front, rather than guarding every caller.
        if range_is_empty(&lower, &upper) {
            return Ok(Vec::new());
        }
        let mut seen: HashMap<(Vec<u8>, CoordVec), Flag> = HashMap::new();
        for (value, coords) in self.memtable.range((lower.clone(), upper.clone())) {
            for (coord, &flag) in coords {
                seen.entry((value.clone(), coord.clone())).or_insert(flag);
            }
        }
        let (lower, upper) = (bound_as_slice(&lower), bound_as_slice(&upper));
        for segment in self.segments.iter().rev() {
            for (value, coord, flag) in segment.range(lower, upper)? {
                seen.entry((value, coord)).or_insert(flag);
            }
        }
        Ok(seen
            .into_iter()
            .filter(|(_, flag)| *flag == Flag::Present)
            .map(|((_, coord), _)| coord)
            .collect())
    }

    /// Flushes the memtable to a new segment (a no-op if it's empty),
    /// resets the WAL, and compacts if that pushed the segment count past
    /// this index's threshold.
    fn flush(&mut self) -> io::Result<()> {
        if self.memtable.is_empty() {
            return Ok(());
        }
        let id = self.next_segment_id;
        self.next_segment_id += 1;
        let path = self.dir.join(format!("{id:020}.seg"));
        let count = self.memtable_len as u32;
        let entries = self
            .memtable
            .iter()
            .flat_map(|(value, coords)| coords.iter().map(move |(coord, &flag)| (value.clone(), coord.clone(), flag)));
        write_segment(&path, count, entries)?;
        self.segments.push(Segment::open(path)?);
        self.memtable.clear();
        self.memtable_len = 0;
        self.wal = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(self.dir.join("wal.log"))?;
        if self.segments.len() > self.compact_threshold {
            self.compact()?;
        }
        Ok(())
    }

    /// Merges every current segment into one, in a single streaming pass
    /// (an n-way merge over already-sorted inputs -- never holds more than
    /// one record per segment in memory at once), dropping any entry a
    /// newer segment supersedes and every tombstone outright (with every
    /// segment included in the merge, a tombstone has nothing left to
    /// shadow once this finishes, so it's not written forward).
    ///
    /// Deliberately one tier, triggered by total segment count, not a
    /// leveled/tiered scheme that would merge only some segments at a
    /// time: simpler, and correct for this structure's expected scale (a
    /// handful of indexed keys, not thousands) -- see this module's doc
    /// comment.
    fn compact(&mut self) -> io::Result<()> {
        let mut iters: Vec<SegmentRecords> = self
            .segments
            .iter()
            .map(Segment::iter_all)
            .collect::<io::Result<_>>()?;
        let mut heap: BinaryHeap<HeapItem> = BinaryHeap::new();
        for (i, it) in iters.iter_mut().enumerate() {
            if let Some((value, coord, flag)) = it.next()? {
                heap.push(HeapItem {
                    key: (value, coord),
                    flag,
                    segment: i,
                });
            }
        }

        let tmp_path = self.dir.join(format!("{:020}.seg.tmp", self.next_segment_id));
        let mut count = 0u32;
        {
            let mut writer = SegmentBuilder::create(&tmp_path)?;
            while let Some(top) = heap.pop() {
                let HeapItem { key, flag, segment } = top;
                if let Some((value, coord, flag)) = iters[segment].next()? {
                    heap.push(HeapItem {
                        key: (value, coord),
                        flag,
                        segment,
                    });
                }
                // Every other heap entry sharing this exact key is a
                // duplicate from an older segment -- discard it (but still
                // advance that segment's iterator), since the one just
                // popped (newest among the ties, by `HeapItem::cmp`) is
                // authoritative.
                while heap.peek().is_some_and(|next| next.key == key) {
                    let dup = heap.pop().unwrap();
                    if let Some((value, coord, flag)) = iters[dup.segment].next()? {
                        heap.push(HeapItem {
                            key: (value, coord),
                            flag,
                            segment: dup.segment,
                        });
                    }
                }
                if flag == Flag::Present {
                    writer.write(&key.0, &key.1, Flag::Present)?;
                    count += 1;
                }
            }
            writer.finish()?;
        }
        let final_path = self.dir.join(format!("{:020}.seg", self.next_segment_id));
        patch_entry_count(&tmp_path, count)?;
        fs::rename(&tmp_path, &final_path)?;
        self.next_segment_id += 1;

        for segment in self.segments.drain(..) {
            let _ = fs::remove_file(&segment.path);
        }
        self.segments.push(Segment::open(final_path)?);
        Ok(())
    }
}

/// One candidate in `compact`'s merge: ordered so `BinaryHeap` (a max-heap)
/// pops the *smallest* key first, and -- among entries sharing a key --
/// pops the newest segment first (a later-in-`self.segments` index is
/// newer), so the first pop for any key is always the authoritative one.
struct HeapItem {
    key: (Vec<u8>, CoordVec),
    flag: Flag,
    segment: usize,
}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.segment == other.segment
    }
}
impl Eq for HeapItem {}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other
            .key
            .cmp(&self.key)
            .then_with(|| self.segment.cmp(&other.segment))
    }
}
impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Builds one segment file: a placeholder entry count, then records
/// written as `write` is called (must already be in sorted order -- every
/// caller here feeds it one), then a sparse-index footer, finalized by
/// `finish` patching the real count in at the start.
struct SegmentBuilder {
    writer: BufWriter<File>,
    count: u32,
    checkpoints: Vec<Checkpoint>,
}

impl SegmentBuilder {
    fn create(path: &Path) -> io::Result<SegmentBuilder> {
        let mut writer = BufWriter::new(File::create(path)?);
        writer.write_all(&0u32.to_le_bytes())?; // patched in `finish`
        Ok(SegmentBuilder {
            writer,
            count: 0,
            checkpoints: Vec::new(),
        })
    }

    fn write(&mut self, value: &[u8], coord: &[i32], flag: Flag) -> io::Result<()> {
        let offset = self.writer.stream_position()?;
        if self.count % SPARSE_INDEX_INTERVAL == 0 {
            self.checkpoints.push(Checkpoint {
                offset,
                record_index: self.count,
                value: value.to_vec(),
                coord: coord.to_vec(),
            });
        }
        write_record(&mut self.writer, value, coord, flag)?;
        self.count += 1;
        Ok(())
    }

    fn finish(mut self) -> io::Result<()> {
        let footer_offset = self.writer.stream_position()?;
        write_u32(&mut self.writer, self.checkpoints.len() as u32)?;
        for cp in &self.checkpoints {
            write_u64(&mut self.writer, cp.offset)?;
            write_u32(&mut self.writer, cp.record_index)?;
            write_bytes(&mut self.writer, &cp.value)?;
            write_coord(&mut self.writer, &cp.coord)?;
        }
        write_u64(&mut self.writer, footer_offset)?;
        self.writer.flush()?;
        drop(self.writer);
        Ok(())
    }
}

/// Writes a whole segment in one call from an already-sorted `entries`
/// iterator with a known final `count` -- `LsmIndex::flush`'s case, where
/// the memtable already knows its own length, so there's no need for
/// `SegmentBuilder`'s patch-after-the-fact count (used instead by
/// `compact`, whose final count isn't known until the merge finishes).
fn write_segment(
    path: &Path,
    count: u32,
    entries: impl Iterator<Item = (Vec<u8>, CoordVec, Flag)>,
) -> io::Result<()> {
    let mut builder = SegmentBuilder::create(path)?;
    for (value, coord, flag) in entries {
        builder.write(&value, &coord, flag)?;
    }
    builder.finish()?;
    patch_entry_count(path, count)
}

fn patch_entry_count(path: &Path, count: u32) -> io::Result<()> {
    let mut f = OpenOptions::new().write(true).open(path)?;
    f.seek(SeekFrom::Start(0))?;
    f.write_all(&count.to_le_bytes())
}

// --- Record encoding: [u32 value_len][value][u8 axes][axes * i32][u8 flag] ---

fn write_record(w: &mut impl Write, value: &[u8], coord: &[i32], flag: Flag) -> io::Result<()> {
    write_bytes(w, value)?;
    write_coord(w, coord)?;
    w.write_all(&[flag.to_byte()])
}

fn read_record(r: &mut impl Read) -> io::Result<(Vec<u8>, CoordVec, Flag)> {
    let value = read_bytes(r)?;
    let coord = read_coord(r)?;
    let mut flag_buf = [0u8; 1];
    r.read_exact(&mut flag_buf)?;
    Ok((value, coord, Flag::from_byte(flag_buf[0])?))
}

/// `read_record`, but `Ok(None)` on a clean end of stream (nothing more to
/// read) instead of an error -- for WAL replay, which doesn't know its
/// entry count up front the way a segment's header-prefixed format does.
fn read_record_opt(r: &mut impl Read) -> io::Result<Option<(Vec<u8>, CoordVec, Flag)>> {
    let mut len_buf = [0u8; 4];
    let mut filled = 0;
    while filled < 4 {
        match r.read(&mut len_buf[filled..])? {
            0 if filled == 0 => return Ok(None),
            0 => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "corrupt lsm WAL: truncated record",
                ))
            }
            n => filled += n,
        }
    }
    let value_len = u32::from_le_bytes(len_buf) as usize;
    let mut value = vec![0u8; value_len];
    r.read_exact(&mut value)?;
    let coord = read_coord(r)?;
    let mut flag_buf = [0u8; 1];
    r.read_exact(&mut flag_buf)?;
    Ok(Some((value, coord, Flag::from_byte(flag_buf[0])?)))
}

fn write_bytes(w: &mut impl Write, bytes: &[u8]) -> io::Result<()> {
    w.write_all(&(bytes.len() as u32).to_le_bytes())?;
    w.write_all(bytes)
}

fn read_bytes(r: &mut impl Read) -> io::Result<Vec<u8>> {
    let len = read_u32(r)? as usize;
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf)?;
    Ok(buf)
}

fn write_coord(w: &mut impl Write, coord: &[i32]) -> io::Result<()> {
    w.write_all(&[coord.len() as u8])?;
    for &c in coord {
        w.write_all(&c.to_le_bytes())?;
    }
    Ok(())
}

fn read_coord(r: &mut impl Read) -> io::Result<CoordVec> {
    let mut axes_buf = [0u8; 1];
    r.read_exact(&mut axes_buf)?;
    (0..axes_buf[0]).map(|_| read_i32(r)).collect()
}

fn write_u32(w: &mut impl Write, n: u32) -> io::Result<()> {
    w.write_all(&n.to_le_bytes())
}
fn write_u64(w: &mut impl Write, n: u64) -> io::Result<()> {
    w.write_all(&n.to_le_bytes())
}
fn read_u32(r: &mut impl Read) -> io::Result<u32> {
    let mut buf = [0u8; 4];
    r.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}
fn read_u64(r: &mut impl Read) -> io::Result<u64> {
    let mut buf = [0u8; 8];
    r.read_exact(&mut buf)?;
    Ok(u64::from_le_bytes(buf))
}
fn read_i32(r: &mut impl Read) -> io::Result<i32> {
    let mut buf = [0u8; 4];
    r.read_exact(&mut buf)?;
    Ok(i32::from_le_bytes(buf))
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
                "kblockdblib-lsm-test-{tag}-{}-{n}",
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

    fn v(s: &str) -> Vec<u8> {
        s.as_bytes().to_vec()
    }

    fn c(coord: &[i32]) -> CoordVec {
        coord.to_vec()
    }

    #[test]
    fn insert_then_lookup_roundtrips_within_the_memtable() {
        let dir = TempDir::new("basic");
        let mut lsm = LsmIndex::create(dir.as_ref()).unwrap();
        lsm.insert(v("stone"), c(&[1, 2, 3])).unwrap();
        lsm.insert(v("stone"), c(&[4, 5, 6])).unwrap();
        lsm.insert(v("dirt"), c(&[9, 9, 9])).unwrap();

        let mut stone = lsm.lookup(&v("stone")).unwrap();
        stone.sort();
        assert_eq!(stone, vec![c(&[1, 2, 3]), c(&[4, 5, 6])]);
        assert_eq!(lsm.lookup(&v("dirt")).unwrap(), vec![c(&[9, 9, 9])]);
        assert_eq!(lsm.lookup(&v("lava")).unwrap(), Vec::<CoordVec>::new());
    }

    #[test]
    fn remove_clears_a_coord_from_its_value() {
        let dir = TempDir::new("remove");
        let mut lsm = LsmIndex::create(dir.as_ref()).unwrap();
        lsm.insert(v("stone"), c(&[1, 2, 3])).unwrap();
        lsm.remove(v("stone"), c(&[1, 2, 3])).unwrap();
        assert_eq!(lsm.lookup(&v("stone")).unwrap(), Vec::<CoordVec>::new());
    }

    #[test]
    fn a_flush_survives_a_reopen() {
        let dir = TempDir::new("flush-reopen");
        {
            let mut lsm = LsmIndex::open_with_thresholds(dir.as_ref(), 2, 100).unwrap();
            lsm.insert(v("stone"), c(&[1])).unwrap();
            lsm.insert(v("stone"), c(&[2])).unwrap(); // crosses the threshold -> flush
        }
        let lsm = LsmIndex::open_with_thresholds(dir.as_ref(), 2, 100).unwrap();
        let mut stone = lsm.lookup(&v("stone")).unwrap();
        stone.sort();
        assert_eq!(stone, vec![c(&[1]), c(&[2])]);
    }

    #[test]
    fn an_unflushed_write_survives_a_reopen_via_the_wal() {
        let dir = TempDir::new("wal-replay");
        {
            // Threshold never reached -- this never flushes to a segment,
            // only the WAL has it.
            let mut lsm = LsmIndex::open_with_thresholds(dir.as_ref(), 1000, 100).unwrap();
            lsm.insert(v("stone"), c(&[1, 2, 3])).unwrap();
        }
        let lsm = LsmIndex::open_with_thresholds(dir.as_ref(), 1000, 100).unwrap();
        assert_eq!(lsm.lookup(&v("stone")).unwrap(), vec![c(&[1, 2, 3])]);
    }

    #[test]
    fn a_value_moved_away_and_back_is_resolved_by_segment_recency_not_shadowed() {
        let dir = TempDir::new("recency");
        // Threshold of 1 flushes a new segment after every single write.
        let mut lsm = LsmIndex::open_with_thresholds(dir.as_ref(), 1, 100).unwrap();
        lsm.insert(v("stone"), c(&[1])).unwrap(); // segment 0: stone present
        lsm.remove(v("stone"), c(&[1])).unwrap(); // segment 1: stone tombstoned
        lsm.insert(v("stone"), c(&[1])).unwrap(); // segment 2: stone present again

        assert_eq!(lsm.lookup(&v("stone")).unwrap(), vec![c(&[1])]);
    }

    #[test]
    fn compaction_merges_segments_and_drops_resolved_tombstones() {
        let dir = TempDir::new("compact");
        let mut lsm = LsmIndex::open_with_thresholds(dir.as_ref(), 1, 3).unwrap();
        for i in 0..3 {
            lsm.insert(v("stone"), c(&[i])).unwrap(); // 3 segments
        }
        lsm.remove(v("stone"), c(&[1])).unwrap(); // crosses compact_threshold -> compacts

        // Still exactly one segment after compaction (nothing re-triggers
        // another compaction immediately after it ran).
        assert_eq!(lsm.segments.len(), 1);
        let mut stone = lsm.lookup(&v("stone")).unwrap();
        stone.sort();
        assert_eq!(stone, vec![c(&[0]), c(&[2])]);
    }

    #[test]
    fn compaction_survives_a_reopen() {
        let dir = TempDir::new("compact-reopen");
        {
            let mut lsm = LsmIndex::open_with_thresholds(dir.as_ref(), 1, 3).unwrap();
            for i in 0..5 {
                lsm.insert(v("stone"), c(&[i])).unwrap();
            }
        }
        let lsm = LsmIndex::open_with_thresholds(dir.as_ref(), 1, 3).unwrap();
        let mut stone = lsm.lookup(&v("stone")).unwrap();
        stone.sort();
        assert_eq!(stone, (0..5).map(|i| c(&[i])).collect::<Vec<_>>());
    }

    #[test]
    fn many_distinct_values_round_trip_across_several_flushes() {
        let dir = TempDir::new("many-values");
        let mut lsm = LsmIndex::open_with_thresholds(dir.as_ref(), 10, 100).unwrap();
        for i in 0..200 {
            lsm.insert(format!("v{i}").into_bytes(), c(&[i])).unwrap();
        }
        for i in 0..200 {
            assert_eq!(
                lsm.lookup(format!("v{i}").into_bytes().as_slice()).unwrap(),
                vec![c(&[i])],
                "value v{i}"
            );
        }
    }

    #[test]
    fn a_lookup_past_the_sparse_index_still_finds_its_value() {
        // Forces several checkpoints within one segment (interval 128) by
        // writing enough distinct values in one flush.
        let dir = TempDir::new("sparse-index");
        let mut lsm = LsmIndex::open_with_thresholds(dir.as_ref(), 500, 100).unwrap();
        for i in 0..500 {
            lsm.insert(format!("v{i:04}").into_bytes(), c(&[i])).unwrap();
        }
        assert_eq!(
            lsm.lookup(b"v0333").unwrap(),
            vec![c(&[333])]
        );
        assert_eq!(lsm.lookup(b"v9999").unwrap(), Vec::<CoordVec>::new());
    }

    #[test]
    fn a_value_shared_by_more_coords_than_one_sparse_interval_is_found_in_full() {
        // Regression: when *every* checkpoint in a segment qualifies as
        // "at or before the target" (because they all share the exact
        // same value), picking the *last* one to start scanning from
        // would land in the middle of the matching run instead of before
        // it, silently dropping every earlier match. 1000 coords under
        // one value, with a checkpoint every `SPARSE_INDEX_INTERVAL`
        // (128) records, forces several checkpoints to all equal the
        // target within a single segment.
        let dir = TempDir::new("one-value-many-checkpoints");
        let mut lsm = LsmIndex::open_with_thresholds(dir.as_ref(), 1000, 100).unwrap();
        for i in 0..1000 {
            lsm.insert(v("bulk"), c(&[i])).unwrap();
        }
        let mut bulk = lsm.lookup(&v("bulk")).unwrap();
        bulk.sort();
        assert_eq!(bulk, (0..1000).map(|i| c(&[i])).collect::<Vec<_>>());
    }

    #[test]
    fn multiple_coords_can_share_one_value_across_many_flushes() {
        let dir = TempDir::new("shared-value");
        let mut lsm = LsmIndex::open_with_thresholds(dir.as_ref(), 2, 100).unwrap();
        for i in 0..10 {
            lsm.insert(v("stone"), c(&[i])).unwrap();
        }
        let mut stone = lsm.lookup(&v("stone")).unwrap();
        stone.sort();
        assert_eq!(stone, (0..10).map(|i| c(&[i])).collect::<Vec<_>>());
    }

    #[test]
    fn empty_index_lookup_is_empty_not_an_error() {
        let dir = TempDir::new("empty");
        let lsm = LsmIndex::create(dir.as_ref()).unwrap();
        assert_eq!(lsm.lookup(&v("anything")).unwrap(), Vec::<CoordVec>::new());
    }

    #[test]
    fn multi_axis_coordinates_round_trip() {
        let dir = TempDir::new("multi-axis");
        let mut lsm = LsmIndex::open_with_thresholds(dir.as_ref(), 1, 100).unwrap();
        lsm.insert(v("stone"), c(&[-5, 0, 10, 2])).unwrap();
        assert_eq!(lsm.lookup(&v("stone")).unwrap(), vec![c(&[-5, 0, 10, 2])]);
    }

    // --- Range ---

    fn sorted(mut coords: Vec<CoordVec>) -> Vec<CoordVec> {
        coords.sort();
        coords
    }

    #[test]
    fn range_within_the_memtable_finds_every_value_in_bounds() {
        let dir = TempDir::new("range-memtable");
        let mut lsm = LsmIndex::create(dir.as_ref()).unwrap();
        for i in 0..10 {
            lsm.insert(format!("v{i:02}").into_bytes(), c(&[i])).unwrap();
        }
        let got = lsm
            .range(
                Bound::Included(v("v03")),
                Bound::Excluded(v("v07")),
            )
            .unwrap();
        assert_eq!(sorted(got), (3..7).map(|i| c(&[i])).collect::<Vec<_>>());
    }

    #[test]
    fn range_bounds_are_respected_as_included_or_excluded() {
        let dir = TempDir::new("range-bounds");
        let mut lsm = LsmIndex::create(dir.as_ref()).unwrap();
        for i in 0..5 {
            lsm.insert(format!("v{i}").into_bytes(), c(&[i])).unwrap();
        }
        assert_eq!(
            sorted(
                lsm.range(Bound::Included(v("v1")), Bound::Included(v("v3")))
                    .unwrap()
            ),
            vec![c(&[1]), c(&[2]), c(&[3])]
        );
        assert_eq!(
            sorted(
                lsm.range(Bound::Excluded(v("v1")), Bound::Excluded(v("v3")))
                    .unwrap()
            ),
            vec![c(&[2])]
        );
    }

    #[test]
    fn unbounded_range_ends_cover_everything_on_that_side() {
        let dir = TempDir::new("range-unbounded");
        let mut lsm = LsmIndex::create(dir.as_ref()).unwrap();
        for i in 0..5 {
            lsm.insert(format!("v{i}").into_bytes(), c(&[i])).unwrap();
        }
        assert_eq!(
            sorted(lsm.range(Bound::Unbounded, Bound::Included(v("v1"))).unwrap()),
            vec![c(&[0]), c(&[1])]
        );
        assert_eq!(
            sorted(lsm.range(Bound::Excluded(v("v3")), Bound::Unbounded).unwrap()),
            vec![c(&[4])]
        );
        assert_eq!(
            sorted(lsm.range(Bound::Unbounded, Bound::Unbounded).unwrap()),
            (0..5).map(|i| c(&[i])).collect::<Vec<_>>()
        );
    }

    #[test]
    fn range_spans_segments_flushed_across_several_values() {
        let dir = TempDir::new("range-segments");
        // Flush threshold 1: every insert becomes its own segment.
        let mut lsm = LsmIndex::open_with_thresholds(dir.as_ref(), 1, 100).unwrap();
        for i in 0..10 {
            lsm.insert(format!("v{i:02}").into_bytes(), c(&[i])).unwrap();
        }
        let got = lsm
            .range(Bound::Included(v("v02")), Bound::Included(v("v05")))
            .unwrap();
        assert_eq!(sorted(got), (2..=5).map(|i| c(&[i])).collect::<Vec<_>>());
    }

    #[test]
    fn a_coord_moved_out_of_range_is_not_reported_by_a_stale_segment_entry() {
        let dir = TempDir::new("range-moved-out");
        let mut lsm = LsmIndex::open_with_thresholds(dir.as_ref(), 1, 100).unwrap();
        lsm.insert(v("v01"), c(&[0])).unwrap(); // segment 0: in range
        lsm.remove(v("v01"), c(&[0])).unwrap(); // segment 1: tombstoned
        lsm.insert(v("v09"), c(&[0])).unwrap(); // segment 2: moved out of range

        let got = lsm
            .range(Bound::Included(v("v00")), Bound::Included(v("v05")))
            .unwrap();
        assert_eq!(got, Vec::<CoordVec>::new());
    }

    #[test]
    fn a_coord_moved_into_range_from_outside_is_found() {
        let dir = TempDir::new("range-moved-in");
        let mut lsm = LsmIndex::open_with_thresholds(dir.as_ref(), 1, 100).unwrap();
        lsm.insert(v("v09"), c(&[0])).unwrap(); // segment 0: outside the query range
        lsm.remove(v("v09"), c(&[0])).unwrap(); // segment 1: tombstoned
        lsm.insert(v("v01"), c(&[0])).unwrap(); // segment 2: moved into range

        let got = lsm
            .range(Bound::Included(v("v00")), Bound::Included(v("v05")))
            .unwrap();
        assert_eq!(got, vec![c(&[0])]);
    }

    #[test]
    fn range_past_the_sparse_index_still_finds_every_match() {
        // Same shape as `a_lookup_past_the_sparse_index_still_finds_its_value`,
        // but asking for a span rather than one exact value, so the scan
        // has to cross several checkpoints (interval 128) and stop at the
        // right one.
        let dir = TempDir::new("range-sparse-index");
        let mut lsm = LsmIndex::open_with_thresholds(dir.as_ref(), 500, 100).unwrap();
        for i in 0..500 {
            lsm.insert(format!("v{i:04}").into_bytes(), c(&[i])).unwrap();
        }
        let got = lsm
            .range(Bound::Included(v("v0330")), Bound::Excluded(v("v0335")))
            .unwrap();
        assert_eq!(sorted(got), (330..335).map(|i| c(&[i])).collect::<Vec<_>>());
    }

    #[test]
    fn empty_index_range_is_empty_not_an_error() {
        let dir = TempDir::new("range-empty");
        let lsm = LsmIndex::create(dir.as_ref()).unwrap();
        assert_eq!(
            lsm.range(Bound::Unbounded, Bound::Unbounded).unwrap(),
            Vec::<CoordVec>::new()
        );
    }

    #[test]
    fn an_empty_range_where_lower_meets_upper_exclusively_matches_nothing() {
        let dir = TempDir::new("range-degenerate");
        let mut lsm = LsmIndex::create(dir.as_ref()).unwrap();
        lsm.insert(v("v1"), c(&[0])).unwrap();
        assert_eq!(
            lsm.range(Bound::Excluded(v("v1")), Bound::Excluded(v("v1")))
                .unwrap(),
            Vec::<CoordVec>::new()
        );
    }

    #[test]
    fn a_range_where_lower_meets_upper_inclusively_matches_just_that_value() {
        let dir = TempDir::new("range-single-point");
        let mut lsm = LsmIndex::create(dir.as_ref()).unwrap();
        lsm.insert(v("v1"), c(&[0])).unwrap();
        assert_eq!(
            lsm.range(Bound::Included(v("v1")), Bound::Included(v("v1")))
                .unwrap(),
            vec![c(&[0])]
        );
    }

    #[test]
    fn a_reversed_range_matches_nothing_instead_of_panicking() {
        let dir = TempDir::new("range-reversed");
        let mut lsm = LsmIndex::create(dir.as_ref()).unwrap();
        lsm.insert(v("v1"), c(&[0])).unwrap();
        lsm.insert(v("v9"), c(&[1])).unwrap();
        assert_eq!(
            lsm.range(Bound::Included(v("v9")), Bound::Included(v("v1")))
                .unwrap(),
            Vec::<CoordVec>::new()
        );
    }
}

