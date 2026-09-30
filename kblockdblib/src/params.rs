use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, PoisonError};

/// Guards `world.txt`'s check-then-maybe-write sequence
/// (`create_or_validate`) against concurrent *threads* in this process --
/// there's no other process to guard against any more, since kBlockDB now
/// assumes exactly one process ever has a world open at a time (see
/// `World`'s "Concurrency" doc comment). Global rather than one lock per
/// directory: `World::create`/`open` are cheap, rare, startup-time calls,
/// so a single process-wide lock is simpler than a table of per-path locks
/// for no measurable cost.
static LOCK: Mutex<()> = Mutex::new(());

/// The three numbers that fix a world's shape for its entire lifetime: how
/// many axes it has, how many cells wide each axis is, and how many cells
/// wide each axis is *within a chunk* (see `chunk::chunk_cells`). Persisted
/// as `world.txt` at the world root
/// (`axes\t<n>\nworld_dim\t<n>\nchunk_dim\t<n>\n`), written once by
/// `World::create` (via `create_or_validate`) and never rewritten, so
/// reopening a world -- or a second thread `create`-ing one in the same
/// directory with different numbers, possibly at the very same moment --
/// can't silently change its shape out from under data already on disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorldParams {
    pub axes: usize,
    pub world_dim: u32,
    pub chunk_dim: u32,
}

impl WorldParams {
    fn path(world_root: &Path) -> PathBuf {
        world_root.join("world.txt")
    }

    /// Reads `world.txt` at `world_root`, or `None` if this directory has
    /// no world yet.
    pub fn read(world_root: &Path) -> io::Result<Option<Self>> {
        let _guard = LOCK.lock().unwrap_or_else(PoisonError::into_inner);
        Self::read_inner(world_root)
    }

    /// Core of `read`, minus the locking -- callers that already hold
    /// `LOCK` (i.e. `create_or_validate`) call this directly instead of
    /// taking a second, reentrant-deadlocking lock.
    fn read_inner(world_root: &Path) -> io::Result<Option<Self>> {
        let path = Self::path(world_root);
        if !path.exists() {
            return Ok(None);
        }
        let text = fs::read_to_string(&path)?;

        let mut axes = None;
        let mut world_dim = None;
        let mut chunk_dim = None;
        for line in text.lines() {
            if line.is_empty() {
                continue;
            }
            let (key, value) = line
                .split_once('\t')
                .ok_or_else(|| corrupt(&path, "expected 'key<TAB>value' per line"))?;
            match key {
                "axes" => {
                    axes = Some(
                        value
                            .parse::<usize>()
                            .map_err(|_| corrupt(&path, "non-numeric axes"))?,
                    );
                }
                "world_dim" => {
                    world_dim = Some(
                        value
                            .parse::<u32>()
                            .map_err(|_| corrupt(&path, "non-numeric world_dim"))?,
                    );
                }
                "chunk_dim" => {
                    chunk_dim = Some(
                        value
                            .parse::<u32>()
                            .map_err(|_| corrupt(&path, "non-numeric chunk_dim"))?,
                    );
                }
                other => return Err(corrupt(&path, &format!("unknown key '{other}'"))),
            }
        }

        let axes = axes.ok_or_else(|| corrupt(&path, "missing 'axes'"))?;
        let world_dim = world_dim.ok_or_else(|| corrupt(&path, "missing 'world_dim'"))?;
        let chunk_dim = chunk_dim.ok_or_else(|| corrupt(&path, "missing 'chunk_dim'"))?;
        Ok(Some(WorldParams {
            axes,
            world_dim,
            chunk_dim,
        }))
    }

    /// The one operation `World::create` needs: if `world_root` has no
    /// world yet, creates it with `requested` and returns `true`; if it
    /// already has one, `requested` must match it exactly (returning
    /// `false`) or this errors. The whole check-then-maybe-write sequence
    /// runs under `LOCK`, so two threads racing to create the very same
    /// fresh directory -- possibly with *different* numbers -- can't both
    /// "win": the second one to actually run sees what the first one wrote
    /// and is validated against it like any other pre-existing world.
    pub fn create_or_validate(world_root: &Path, requested: WorldParams) -> io::Result<bool> {
        let _guard = LOCK.lock().unwrap_or_else(PoisonError::into_inner);
        match Self::read_inner(world_root)? {
            Some(existing) if existing == requested => Ok(false),
            Some(existing) => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "world at {} already exists with axes={}, world_dim={}, chunk_dim={} -- \
                     requested axes={}, world_dim={}, chunk_dim={}",
                    world_root.display(),
                    existing.axes,
                    existing.world_dim,
                    existing.chunk_dim,
                    requested.axes,
                    requested.world_dim,
                    requested.chunk_dim
                ),
            )),
            None => {
                fs::write(
                    Self::path(world_root),
                    format!(
                        "axes\t{}\nworld_dim\t{}\nchunk_dim\t{}\n",
                        requested.axes, requested.world_dim, requested.chunk_dim
                    ),
                )?;
                Ok(true)
            }
        }
    }
}

fn corrupt(path: &Path, why: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("corrupt {}: {why}", path.display()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::thread;

    struct TempDir(PathBuf);

    impl TempDir {
        fn new(tag: &str) -> Self {
            static COUNTER: AtomicU64 = AtomicU64::new(0);
            let n = COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "kblockdblib-params-test-{tag}-{}-{n}",
                std::process::id()
            ));
            fs::create_dir_all(&path).unwrap();
            TempDir(path)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn read_of_missing_file_is_none() {
        let dir = TempDir::new("missing");
        assert_eq!(WorldParams::read(&dir.0).unwrap(), None);
    }

    #[test]
    fn create_or_validate_then_read_roundtrips() {
        let dir = TempDir::new("roundtrip");
        let params = WorldParams {
            axes: 4,
            world_dim: 777,
            chunk_dim: 16,
        };
        assert!(
            WorldParams::create_or_validate(&dir.0, params).unwrap(),
            "first call must report a fresh creation"
        );
        assert_eq!(WorldParams::read(&dir.0).unwrap(), Some(params));
    }

    #[test]
    fn create_or_validate_is_idempotent_with_matching_params() {
        let dir = TempDir::new("idempotent");
        let params = WorldParams {
            axes: 3,
            world_dim: 100,
            chunk_dim: 32,
        };
        assert!(WorldParams::create_or_validate(&dir.0, params).unwrap());
        assert!(
            !WorldParams::create_or_validate(&dir.0, params).unwrap(),
            "second call with the same params must report no new creation"
        );
    }

    #[test]
    fn create_or_validate_rejects_mismatched_params() {
        let dir = TempDir::new("mismatch");
        let original = WorldParams {
            axes: 3,
            world_dim: 100,
            chunk_dim: 32,
        };
        WorldParams::create_or_validate(&dir.0, original).unwrap();

        let different = WorldParams {
            axes: 4,
            world_dim: 100,
            chunk_dim: 32,
        };
        let err = WorldParams::create_or_validate(&dir.0, different).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);

        // Untouched: still reads back as the original.
        assert_eq!(WorldParams::read(&dir.0).unwrap(), Some(original));
    }

    #[test]
    fn create_or_validate_rejects_a_mismatched_chunk_dim_alone() {
        let dir = TempDir::new("chunk-dim-mismatch");
        let original = WorldParams {
            axes: 3,
            world_dim: 100,
            chunk_dim: 32,
        };
        WorldParams::create_or_validate(&dir.0, original).unwrap();

        let different = WorldParams {
            chunk_dim: 16,
            ..original
        };
        let err = WorldParams::create_or_validate(&dir.0, different).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(err.to_string().contains("chunk_dim"), "{err}");

        assert_eq!(WorldParams::read(&dir.0).unwrap(), Some(original));
    }

    #[test]
    fn read_of_corrupt_file_is_an_error() {
        let dir = TempDir::new("corrupt");
        fs::write(dir.0.join("world.txt"), "not the right format at all\n").unwrap();
        let err = WorldParams::read(&dir.0).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn read_of_incomplete_file_is_an_error() {
        let dir = TempDir::new("incomplete");
        fs::write(dir.0.join("world.txt"), "axes\t3\n").unwrap(); // missing world_dim
        let err = WorldParams::read(&dir.0).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn read_missing_only_chunk_dim_is_an_error() {
        let dir = TempDir::new("missing-chunk-dim");
        fs::write(dir.0.join("world.txt"), "axes\t3\nworld_dim\t100\n").unwrap();
        let err = WorldParams::read(&dir.0).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("chunk_dim"), "{err}");
    }

    #[test]
    fn concurrent_create_or_validate_with_the_same_params_creates_exactly_once() {
        // Regression: World::create used to check "does world.txt exist"
        // then, separately, write it -- a classic TOCTOU race between two
        // processes doing this at once. Here, many threads race to
        // create_or_validate the same fresh directory with identical
        // params; exactly one may report having created it, the rest must
        // see it as already-existing-and-matching, and none may error.
        let dir = TempDir::new("concurrent-create");
        let params = WorldParams {
            axes: 3,
            world_dim: 50,
            chunk_dim: 32,
        };

        let handles: Vec<_> = (0..16)
            .map(|_| {
                let path = dir.0.clone();
                thread::spawn(move || WorldParams::create_or_validate(&path, params).unwrap())
            })
            .collect();

        let results: Vec<bool> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        assert_eq!(
            results.iter().filter(|&&created| created).count(),
            1,
            "exactly one racer should have created the world; results were {results:?}"
        );
        assert_eq!(WorldParams::read(&dir.0).unwrap(), Some(params));
    }
}
