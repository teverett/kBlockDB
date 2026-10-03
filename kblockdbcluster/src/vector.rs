//! What this server knows it has, per origin (a `VersionVector`, see
//! docs/clustering.md's "Catch-up"), persisted across restarts -- plus,
//! for display, the vector each peer last reported.

use kblockdblib::VersionVector;
use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

#[derive(Default)]
pub struct VectorStore {
    state: Mutex<State>,
    /// `None` keeps everything in memory (tests).
    path: Option<PathBuf>,
}

#[derive(Default)]
struct State {
    known: VersionVector,
    peers: HashMap<u64, VersionVector>,
}

impl VectorStore {
    pub fn in_memory() -> Self {
        Self::default()
    }

    /// Loads `path` (lines of `origin_hex\tseq`; a missing file means
    /// nothing known yet), and saves there as the vector advances.
    pub fn load(path: &Path) -> io::Result<Self> {
        let mut known = VersionVector::new();
        match fs::read_to_string(path) {
            Ok(text) => {
                for line in text.lines().filter(|l| !l.trim().is_empty()) {
                    let parsed = line.split_once('\t').and_then(|(origin, seq)| {
                        Some((u64::from_str_radix(origin, 16).ok()?, seq.parse().ok()?))
                    });
                    match parsed {
                        Some((origin, seq)) => known.set(origin, seq),
                        None => {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                format!("malformed line in {}: {line:?}", path.display()),
                            ))
                        }
                    }
                }
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        Ok(VectorStore {
            state: Mutex::new(State {
                known,
                peers: HashMap::new(),
            }),
            path: Some(path.to_path_buf()),
        })
    }

    /// Every origin's confirmed seq received from peers.
    pub fn known(&self) -> VersionVector {
        self.state.lock().unwrap().known.clone()
    }

    /// Raises the known vector to cover `confirmed` too, saving it if it
    /// changed. Only ever called with what a peer has *confirmed* sending.
    pub fn merge(&self, confirmed: &VersionVector) {
        let mut state = self.state.lock().unwrap();
        if state.known.merge(confirmed) {
            if let Err(e) = self.save(&state.known) {
                eprintln!("peer protocol: couldn't save the version vector: {e}");
            }
        }
    }

    /// Records the full vector peer `node_id` last reported, for display.
    pub fn record_peer(&self, node_id: u64, vector: VersionVector) {
        self.state.lock().unwrap().peers.insert(node_id, vector);
    }

    pub fn peer(&self, node_id: u64) -> Option<VersionVector> {
        self.state.lock().unwrap().peers.get(&node_id).cloned()
    }

    fn save(&self, known: &VersionVector) -> io::Result<()> {
        let Some(path) = &self.path else {
            return Ok(());
        };
        if let Some(dir) = path.parent() {
            fs::create_dir_all(dir)?;
        }
        let text: String = known
            .iter()
            .map(|(origin, seq)| format!("{origin:016x}\t{seq}\n"))
            .collect();
        let tmp = path.with_extension("tmp");
        fs::write(&tmp, text)?;
        fs::rename(&tmp, path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vector(entries: &[(u64, u64)]) -> VersionVector {
        entries.iter().copied().collect()
    }

    #[test]
    fn merge_only_raises_entries() {
        let store = VectorStore::in_memory();
        store.merge(&vector(&[(1, 5), (2, 3)]));
        store.merge(&vector(&[(1, 2), (3, 1)]));
        assert_eq!(store.known(), vector(&[(1, 5), (2, 3), (3, 1)]));
    }

    #[test]
    fn the_vector_persists_and_peer_vectors_do_not() {
        let dir =
            std::env::temp_dir().join(format!("kblockdbcluster-vector-{}", std::process::id()));
        let path = dir.join("vector");
        let _ = fs::remove_dir_all(&dir);
        let store = VectorStore::load(&path).unwrap();
        assert!(store.known().is_empty());
        store.merge(&vector(&[(0xABC, 42), (u64::MAX, 7)]));
        store.record_peer(9, vector(&[(9, 1)]));

        let reloaded = VectorStore::load(&path).unwrap();
        assert_eq!(reloaded.known(), vector(&[(0xABC, 42), (u64::MAX, 7)]));
        assert_eq!(reloaded.peer(9), None);
        assert_eq!(store.peer(9), Some(vector(&[(9, 1)])));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_malformed_vector_file_is_an_error() {
        let dir =
            std::env::temp_dir().join(format!("kblockdbcluster-vector-bad-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("vector");
        fs::write(&path, "nonsense\n").unwrap();
        assert!(VectorStore::load(&path).is_err());
        let _ = fs::remove_dir_all(&dir);
    }
}
