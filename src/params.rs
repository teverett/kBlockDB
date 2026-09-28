use std::fs;
use std::io;
use std::path::{Path, PathBuf};

/// The two numbers that fix a world's shape for its entire lifetime: how
/// many axes it has, and how many cells wide each axis is. Persisted as
/// `world.txt` at the world root (`axes\t<n>\nworld_dim\t<n>\n`), written
/// once by `World::create` and never rewritten, so reopening a world --
/// or accidentally `create`-ing a second one in the same directory with
/// different numbers -- can't silently change its shape out from under
/// data already on disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorldParams {
    pub axes: usize,
    pub world_dim: u32,
}

impl WorldParams {
    fn path(world_root: &Path) -> PathBuf {
        world_root.join("world.txt")
    }

    /// Reads `world.txt` at `world_root`, or `None` if this directory has
    /// no world yet.
    pub fn read(world_root: &Path) -> io::Result<Option<Self>> {
        let path = Self::path(world_root);
        if !path.exists() {
            return Ok(None);
        }
        let text = fs::read_to_string(&path)?;

        let mut axes = None;
        let mut world_dim = None;
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
                other => return Err(corrupt(&path, &format!("unknown key '{other}'"))),
            }
        }

        let axes = axes.ok_or_else(|| corrupt(&path, "missing 'axes'"))?;
        let world_dim = world_dim.ok_or_else(|| corrupt(&path, "missing 'world_dim'"))?;
        Ok(Some(WorldParams { axes, world_dim }))
    }

    /// Writes `world.txt` at `world_root`. Only ever called once per world,
    /// by `World::create` the first time it sees a directory with no
    /// existing `world.txt`.
    pub fn write(world_root: &Path, params: WorldParams) -> io::Result<()> {
        fs::write(
            Self::path(world_root),
            format!("axes\t{}\nworld_dim\t{}\n", params.axes, params.world_dim),
        )
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

    struct TempDir(PathBuf);

    impl TempDir {
        fn new(tag: &str) -> Self {
            static COUNTER: AtomicU64 = AtomicU64::new(0);
            let n = COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir()
                .join(format!("kdb-params-test-{tag}-{}-{n}", std::process::id()));
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
    fn write_then_read_roundtrips() {
        let dir = TempDir::new("roundtrip");
        let params = WorldParams {
            axes: 4,
            world_dim: 777,
        };
        WorldParams::write(&dir.0, params).unwrap();
        assert_eq!(WorldParams::read(&dir.0).unwrap(), Some(params));
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
}
