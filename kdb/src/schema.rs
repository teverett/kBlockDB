use crate::lock::FileLock;
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
/// The mapping is append-only and persisted as a plain `id\tkey` text file
/// (`schema.txt`) at the world root, so ids never move and old chunk files
/// stay valid no matter how many new keys get introduced later.
///
/// **Concurrency.** `key_to_id`/`id_to_key` are a *cache* of `schema.txt`,
/// not the source of truth -- other processes can append new keys to it at
/// any time. That cache is always safe to trust on a hit: `schema.txt` is
/// append-only and an id, once assigned, never changes or moves, so once
/// this process has seen a key it can answer for it forever without
/// touching disk again. A miss re-reads `schema.txt` (under a lock, so it
/// can't observe another process's write half-finished) before concluding
/// a key is genuinely unknown -- to *this* process, as of that read;
/// another process can always still be about to intern it a moment later,
/// same as any check-then-use race. `intern` takes an exclusive lock for
/// its whole reload-check-append sequence, so two processes racing to
/// intern two different new keys can't collide on the same id.
pub struct Schema {
    path: PathBuf,
    key_to_id: HashMap<String, u32>,
    id_to_key: Vec<String>,
}

impl Schema {
    pub fn open(world_root: &Path) -> io::Result<Self> {
        let mut schema = Schema {
            path: world_root.join("schema.txt"),
            key_to_id: HashMap::new(),
            id_to_key: Vec::new(),
        };
        schema.reload()?;
        Ok(schema)
    }

    /// Re-reads `schema.txt` from disk under a shared lock, so this can
    /// never observe a concurrent `intern`'s write half-finished. Safe to
    /// call any time: `schema.txt` is append-only, so this only ever grows
    /// `id_to_key`/`key_to_id`, never invalidates an id already known to
    /// be correct.
    fn reload(&mut self) -> io::Result<()> {
        if !self.path.exists() {
            return Ok(()); // nothing interned by anyone, ever, yet
        }
        let _lock = FileLock::shared(&self.path)?;
        self.reload_locked()
    }

    /// Core of `reload`, minus the locking -- for `intern`, which already
    /// holds the (exclusive) lock itself and would deadlock taking a
    /// second one.
    fn reload_locked(&mut self) -> io::Result<()> {
        let text = fs::read_to_string(&self.path)?;
        // Re-parse from scratch every time rather than trying to resume
        // from wherever we last left off: schema.txt is small (one line
        // per distinct key *ever used in the whole world*, not per cell),
        // so this stays cheap, and "just re-read the whole thing" is far
        // more obviously correct than an incremental-resume scheme would
        // be.
        let mut key_to_id = HashMap::with_capacity(self.id_to_key.len());
        let mut id_to_key = Vec::with_capacity(self.id_to_key.len());
        for line in text.lines() {
            if line.is_empty() {
                continue;
            }
            let (id_str, key) = line
                .split_once('\t')
                .expect("corrupt schema.txt: expected 'id<TAB>key' per line");
            let id: u32 = id_str.parse().expect("corrupt schema.txt: non-numeric id");
            assert_eq!(
                id as usize,
                id_to_key.len(),
                "corrupt schema.txt: ids must be dense and in order"
            );
            id_to_key.push(key.to_string());
            key_to_id.insert(key.to_string(), id);
        }
        self.key_to_id = key_to_id;
        self.id_to_key = id_to_key;
        Ok(())
    }

    /// Looks up `key`'s id. A hit (this process has already seen `key`,
    /// from this call or an earlier one) costs nothing -- see the
    /// "Concurrency" note on `Schema` for why that's always safe. A miss
    /// re-reads `schema.txt` once, in case another process interned `key`
    /// since this process last looked, before answering `None`.
    pub fn id_for_key(&mut self, key: &str) -> io::Result<Option<u32>> {
        if let Some(&id) = self.key_to_id.get(key) {
            return Ok(Some(id));
        }
        self.reload()?;
        Ok(self.key_to_id.get(key).copied())
    }

    pub fn len(&self) -> usize {
        self.id_to_key.len()
    }

    pub fn is_empty(&self) -> bool {
        self.id_to_key.is_empty()
    }

    /// Look up `key`'s id, interning (and durably persisting) a new one if
    /// this is the first time *anyone* (any process) has ever seen it.
    pub fn intern(&mut self, key: &str) -> io::Result<u32> {
        if let Some(&id) = self.key_to_id.get(key) {
            return Ok(id);
        }

        // Hold the lock for the whole reload-check-append sequence: two
        // processes racing to intern two *different* new keys must not
        // both compute the same "next" id from a schema.txt neither of
        // them has re-read since the other's write.
        let _lock = FileLock::exclusive(&self.path)?;
        if self.path.exists() {
            self.reload_locked()?;
            if let Some(&id) = self.key_to_id.get(key) {
                return Ok(id); // someone else just interned it -- use their id
            }
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
        f.write_all(format!("{id}\t{key}\n").as_bytes())?;

        self.id_to_key.push(key.to_string());
        self.key_to_id.insert(key.to_string(), id);
        Ok(id)
    }
}
