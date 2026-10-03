//! This server's identity in the cluster and its own write counter --
//! see docs/clustering.md's "Catch-up".
//!
//! **Node id.** A random id, generated once and kept in `<dir>/node_id`,
//! that every write this server makes is stamped with (see
//! `kblockdblib::Stamp`) and that it says in `Hello`. Never copy it to
//! another server: two servers sharing an id would each think the other
//! is itself.
//!
//! **Sequence numbers.** Every write this server makes gets the next one.
//! They must never be reused -- a peer that has seen this server's seq 50
//! ignores any later write calling itself 50 -- so the counter is
//! persisted by *lease*: before handing out a seq past the leased bound,
//! the bound is raised by `SEQ_LEASE` and written to `<dir>/sequence`
//! (fsynced). A restart resumes from the saved bound, skipping whatever
//! was leased but unused. Gaps are harmless: a vector entry means "every
//! write with seq <= S", and a seq that was never used is trivially had.
//!
//! **Confirmation.** A seq is assigned when its write lands in storage, but
//! published to peers afterwards, so publishes can go out of order. Only a
//! prefix with nothing still *in flight* (assigned, not yet published) can
//! be promised to peers -- `confirmed_through`.

use kblockdblib::{Stamp, LEGACY_BIT};
use std::collections::BTreeSet;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use tokio::sync::watch;

/// How many seqs each lease covers -- the most a restart can skip.
pub const SEQ_LEASE: u64 = 1000;

pub struct Sequencer {
    node_id: u64,
    state: Mutex<State>,
    /// Where the lease bound is saved; `None` keeps it in memory (tests).
    lease_path: Option<PathBuf>,
    /// Bumped by `release`: a written write was never published, so every
    /// link should catch its peer up again (see `client.rs`).
    resync: watch::Sender<u64>,
}

struct State {
    next: u64,
    /// Seqs below this are leased (may have been handed out).
    leased_to: u64,
    in_flight: BTreeSet<u64>,
}

impl Sequencer {
    /// Loads (or, the first time, creates) the node id and lease bound
    /// kept in `dir`.
    pub fn load_or_create(dir: &Path) -> io::Result<Self> {
        fs::create_dir_all(dir)?;
        let id_path = dir.join("node_id");
        let node_id = match fs::read_to_string(&id_path) {
            Ok(text) => u64::from_str_radix(text.trim(), 16)
                .ok()
                .filter(|id| *id != 0 && id & LEGACY_BIT == 0)
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("{} doesn't hold a valid node id", id_path.display()),
                    )
                })?,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                let node_id = random_node_id();
                write_synced(&id_path, &format!("{node_id:016x}\n"))?;
                node_id
            }
            Err(e) => return Err(e),
        };
        let lease_path = dir.join("sequence");
        let leased_to = match fs::read_to_string(&lease_path) {
            Ok(text) => text.trim().parse::<u64>().map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("{} doesn't hold a sequence number", lease_path.display()),
                )
            })?,
            Err(e) if e.kind() == io::ErrorKind::NotFound => 1,
            Err(e) => return Err(e),
        };
        Ok(Sequencer {
            node_id,
            state: Mutex::new(State {
                next: leased_to,
                leased_to,
                in_flight: BTreeSet::new(),
            }),
            lease_path: Some(lease_path),
            resync: watch::channel(0).0,
        })
    }

    /// A sequencer with a random node id and nothing persisted.
    pub fn in_memory() -> Self {
        Sequencer {
            node_id: random_node_id(),
            state: Mutex::new(State {
                next: 1,
                leased_to: u64::MAX,
                in_flight: BTreeSet::new(),
            }),
            lease_path: None,
            resync: watch::channel(0).0,
        }
    }

    pub fn node_id(&self) -> u64 {
        self.node_id
    }

    /// The next seq, stamped with this node's id, marked in flight until
    /// `published` (or `release`d).
    pub fn assign(&self) -> Stamp {
        let mut state = self.state.lock().unwrap();
        if state.next >= state.leased_to {
            let bound = state.next + SEQ_LEASE;
            if let Some(path) = &self.lease_path {
                // Can't fail the write that's asking -- it's already in
                // storage. A seq might be reused only if this process then
                // crashes before the next successful lease.
                if let Err(e) = write_synced(path, &format!("{bound}\n")) {
                    eprintln!("peer protocol: couldn't save sequence lease: {e}");
                }
            }
            state.leased_to = bound;
        }
        let seq = state.next;
        state.next += 1;
        state.in_flight.insert(seq);
        Stamp::new(self.node_id, seq)
    }

    /// `seq` has been published to every link.
    pub fn published(&self, seq: u64) {
        self.state.lock().unwrap().in_flight.remove(&seq);
    }

    /// `seqs` were assigned and written but will never be published (the
    /// write failed partway): stop waiting on them, and have every link
    /// catch its peer up so they still get there.
    pub fn release(&self, seqs: &[u64]) {
        if seqs.is_empty() {
            return;
        }
        let mut state = self.state.lock().unwrap();
        for seq in seqs {
            state.in_flight.remove(seq);
        }
        drop(state);
        self.resync.send_modify(|n| *n += 1);
    }

    /// Every seq up to here has been published (or never used): what this
    /// node can promise peers it has sent.
    pub fn confirmed_through(&self) -> u64 {
        let state = self.state.lock().unwrap();
        match state.in_flight.first() {
            Some(first) => first - 1,
            None => state.next - 1,
        }
    }

    pub fn subscribe_resync(&self) -> watch::Receiver<u64> {
        self.resync.subscribe()
    }
}

/// Writes `text` to `path` via a temp file, fsynced, then a rename -- a
/// crash leaves either the old file or the new one.
fn write_synced(path: &Path, text: &str) -> io::Result<()> {
    let tmp = path.with_extension("tmp");
    let mut f = fs::File::create(&tmp)?;
    f.write_all(text.as_bytes())?;
    f.sync_all()?;
    fs::rename(&tmp, path)
}

/// A random node id: nonzero (0 is `Stamp::NONE`'s origin) and without
/// `LEGACY_BIT`. `RandomState` is seeded randomly per instance, so this
/// needs no extra dependency; it only has to be unique among a cluster's
/// servers, not cryptographically strong.
fn random_node_id() -> u64 {
    use std::hash::{BuildHasher, Hasher};
    loop {
        let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
        hasher.write_u32(std::process::id());
        if let Ok(elapsed) = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
            hasher.write_u128(elapsed.as_nanos());
        }
        let id = hasher.finish() & !LEGACY_BIT;
        if id != 0 {
            return id;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TempDir(PathBuf);

    impl TempDir {
        fn new(tag: &str) -> Self {
            static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            TempDir(std::env::temp_dir().join(format!(
                "kblockdbcluster-seq-{tag}-{}-{n}",
                std::process::id()
            )))
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn seqs_count_up_from_1_stamped_with_the_node_id() {
        let s = Sequencer::in_memory();
        assert_ne!(s.node_id(), 0);
        assert_eq!(s.node_id() & LEGACY_BIT, 0);
        assert_eq!(s.assign(), Stamp::new(s.node_id(), 1));
        assert_eq!(s.assign(), Stamp::new(s.node_id(), 2));
    }

    #[test]
    fn confirmed_through_waits_for_every_earlier_seq_to_be_published() {
        let s = Sequencer::in_memory();
        assert_eq!(s.confirmed_through(), 0);
        let (a, b, c) = (s.assign().seq, s.assign().seq, s.assign().seq);
        assert_eq!(s.confirmed_through(), 0);
        s.published(b);
        s.published(c);
        assert_eq!(s.confirmed_through(), 0); // `a` is still in flight
        s.published(a);
        assert_eq!(s.confirmed_through(), 3);
    }

    #[test]
    fn release_stops_waiting_and_asks_for_a_resync() {
        let s = Sequencer::in_memory();
        let resync = s.subscribe_resync();
        let a = s.assign().seq;
        s.assign();
        s.release(&[a]);
        assert!(resync.has_changed().unwrap());
        assert_eq!(s.confirmed_through(), 1);
    }

    #[test]
    fn the_node_id_persists_and_a_restart_never_reuses_a_seq() {
        let dir = TempDir::new("persist");
        let first = Sequencer::load_or_create(&dir.0).unwrap();
        let used: Vec<u64> = (0..5).map(|_| first.assign().seq).collect();
        assert_eq!(used, vec![1, 2, 3, 4, 5]);
        drop(first);

        let second = Sequencer::load_or_create(&dir.0).unwrap();
        let next = second.assign();
        assert!(next.seq > 5, "reused seq {}", next.seq);
        assert_eq!(next.seq, 1 + SEQ_LEASE);
        let third = Sequencer::load_or_create(&dir.0).unwrap();
        assert_eq!(third.node_id(), second.node_id());
        // A restart counts as having confirmed everything before it.
        assert_eq!(third.confirmed_through(), 2 * SEQ_LEASE);
    }

    #[test]
    fn a_corrupt_node_id_file_is_an_error() {
        let dir = TempDir::new("corrupt");
        fs::create_dir_all(&dir.0).unwrap();
        fs::write(dir.0.join("node_id"), "not hex").unwrap();
        assert!(Sequencer::load_or_create(&dir.0).is_err());
    }
}
