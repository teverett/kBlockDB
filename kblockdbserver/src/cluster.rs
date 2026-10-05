//! Wires `AppState` into `kblockdbcluster`'s generic peer protocol:
//! implements `ReplicationSink` so `kblockdbcluster::server::serve` can
//! apply an incoming change against this server's actual `World`s, and
//! `ChangeSource` so a peer's catch-up can be read back out of them,
//! without `kblockdbcluster` itself knowing anything about `AppState`/
//! `ApiError`/auto-create-on-unseen-database semantics -- those live here,
//! the one place that's specific to how *this* server manages databases.
//! See `docs/clustering.md` (at the repository root) for the feature as a
//! whole, and `kblockdbcluster`'s own doc comment for why the crate split
//! is shaped this way.

use crate::error::ApiError;
use crate::state::AppState;
use kblockdbcluster::server::ReplicationSink;
use kblockdbcluster::source::ChangeSource;
use kblockdbcluster::wire::{ChangeEntry, ChangeOp, IndexOp};
use kblockdblib::{CellMeta, Change, ChangeKind, Stamp, Value, VersionVector};

impl ReplicationSink for AppState {
    async fn apply_set(
        &self,
        database: String,
        coord: Vec<i32>,
        key: String,
        value: Value,
        meta: CellMeta,
        stamp: Stamp,
    ) -> Result<(), String> {
        let (coord2, key2, value2) = (coord.clone(), key.clone(), value.clone());
        match self
            .with_database(&database, move |w| {
                w.apply_replicated_stamped(&coord, &key, value, meta, stamp)
                    .map(|_| ())
            })
            .await
        {
            Ok(()) => Ok(()),
            Err(ApiError::NotFound(_)) => {
                // Unseen database -- create it (with this server's own
                // default shape; ignoring a `Conflict` from a concurrent
                // create racing this one) and retry exactly once. See
                // docs/clustering.md's "Replication scope" decision.
                match self.create_database(&database, None).await {
                    Ok(()) | Err(ApiError::Conflict(_)) => self
                        .with_database(&database, move |w| {
                            w.apply_replicated_stamped(&coord2, &key2, value2, meta, stamp)
                                .map(|_| ())
                        })
                        .await
                        .map_err(|e| format!("{e:?}")),
                    Err(e) => Err(format!("couldn't create database '{database}': {e:?}")),
                }
            }
            Err(e) => Err(format!("{e:?}")),
        }
    }

    async fn apply_remove(
        &self,
        database: String,
        coord: Vec<i32>,
        key: String,
        modified_at_ms: u64,
        stamp: Stamp,
    ) -> Result<(), String> {
        // No auto-create-on-NotFound here: a database this server has
        // never seen can't have anything to remove from anyway, so a
        // removal for an unseen database is simply a no-op (same outcome
        // auto-creating it and then finding nothing to remove would
        // reach, without the extra round trip).
        match self
            .with_database(&database, move |w| {
                w.apply_replicated_remove_stamped(&coord, &key, modified_at_ms, stamp)
                    .map(|_| ())
            })
            .await
        {
            Ok(()) | Err(ApiError::NotFound(_)) => Ok(()),
            Err(e) => Err(format!("{e:?}")),
        }
    }

    /// One `World::apply_changes` per database in the batch -- one write
    /// per chunk touched, rather than one per entry. Same auto-create rule
    /// as `apply_set`: an unseen database is created if the batch sets
    /// anything in it, and skipped if it only removes.
    async fn apply_batch(&self, entries: Vec<ChangeEntry>) {
        let mut by_database: Vec<(String, Vec<Change>)> = Vec::new();
        for entry in entries {
            let change = change_from_entry(&entry);
            match by_database.iter_mut().find(|(db, _)| *db == entry.database) {
                Some((_, changes)) => changes.push(change),
                None => by_database.push((entry.database, vec![change])),
            }
        }
        for (database, changes) in by_database {
            let sets_anything = changes
                .iter()
                .any(|c| matches!(c.kind, ChangeKind::Set(..)));
            if sets_anything {
                match self.create_database(&database, None).await {
                    Ok(()) | Err(ApiError::Conflict(_)) => {}
                    Err(e) => {
                        eprintln!("peer protocol: couldn't create database '{database}': {e:?}");
                        continue;
                    }
                }
            }
            let coords: Vec<Vec<i32>> = changes.iter().map(|c| c.coord.to_vec()).collect();
            match self
                .with_database(&database, move |w| Ok(w.apply_changes(changes)))
                .await
            {
                Ok(results) => {
                    for (coord, result) in coords.iter().zip(results) {
                        if let Err(e) = result {
                            eprintln!(
                                "peer protocol: dropping entry for '{database}' at {coord:?}: {e}"
                            );
                        }
                    }
                }
                Err(ApiError::NotFound(_)) => {}
                Err(e) => eprintln!("peer protocol: dropping batch for '{database}': {e:?}"),
            }
        }
    }

    /// No auto-create here, same reasoning as `apply_remove`: a database
    /// this server has never seen has no data an index could ever match,
    /// so building/dropping/rebuilding an index on it is a harmless no-op
    /// rather than a reason to create an empty database for.
    async fn apply_index_op(&self, database: String, key: String, op: IndexOp) -> Result<(), String> {
        match self
            .with_database(&database, move |w| match op {
                IndexOp::Create => w.create_index(&key),
                IndexOp::Drop => w.drop_index(&key).map(|_| ()),
                IndexOp::Rebuild => w.rebuild_index(&key),
            })
            .await
        {
            Ok(()) | Err(ApiError::NotFound(_)) => Ok(()),
            Err(e) => Err(format!("{e:?}")),
        }
    }
}

fn change_from_entry(entry: &ChangeEntry) -> Change {
    Change {
        coord: kblockdblib::Coord::from(entry.coord.clone()),
        key: entry.key.clone(),
        kind: match &entry.op {
            ChangeOp::Set(value) => ChangeKind::Set(
                value.clone(),
                CellMeta {
                    created_at_ms: entry.created_at_ms,
                    modified_at_ms: entry.modified_at_ms,
                    version: entry.version,
                },
            ),
            ChangeOp::Remove => ChangeKind::Removed(entry.modified_at_ms),
        },
        stamp: entry.stamp(),
    }
}

/// Every database's changes a vector lacks, read straight from each
/// `World` (this runs on a blocking thread -- see `ChangeSource`). A
/// database removed mid-scan is skipped.
impl ChangeSource for AppState {
    fn changes_since(
        &self,
        known: &VersionVector,
        legacy_origin: u64,
        emit: &mut dyn FnMut(Vec<ChangeEntry>) -> bool,
    ) -> Result<(), String> {
        let names = self.databases.list().map_err(|e| e.to_string())?;
        for database in names {
            let world = match self.databases.get(&database) {
                Ok(world) => world,
                Err(ApiError::NotFound(_)) => continue,
                Err(e) => return Err(format!("{e:?}")),
            };
            let mut more = true;
            world
                .changes_since(known, legacy_origin, |batch| {
                    let entries = batch
                        .into_iter()
                        .map(|change| change_entry(&database, change))
                        .collect();
                    more = emit(entries);
                    more
                })
                .map_err(|e| format!("database '{database}': {e}"))?;
            if !more {
                break;
            }
        }
        Ok(())
    }

    fn indexed_keys(&self) -> Result<Vec<(String, String)>, String> {
        let names = self.databases.list().map_err(|e| e.to_string())?;
        let mut out = Vec::new();
        for database in names {
            let world = match self.databases.get(&database) {
                Ok(world) => world,
                Err(ApiError::NotFound(_)) => continue,
                Err(e) => return Err(format!("{e:?}")),
            };
            out.extend(world.indexed_keys().into_iter().map(|key| (database.clone(), key)));
        }
        Ok(out)
    }
}

fn change_entry(database: &str, change: Change) -> ChangeEntry {
    let (op, meta) = match change.kind {
        ChangeKind::Set(value, meta) => (ChangeOp::Set(value), meta),
        ChangeKind::Removed(at_ms) => (
            ChangeOp::Remove,
            CellMeta {
                created_at_ms: 0,
                modified_at_ms: at_ms,
                version: 0,
            },
        ),
    };
    ChangeEntry {
        database: database.to_string(),
        coord: change.coord.to_vec(),
        key: change.key,
        op,
        created_at_ms: meta.created_at_ms,
        modified_at_ms: meta.modified_at_ms,
        version: meta.version,
        origin: change.stamp.origin,
        seq: change.stamp.seq,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{Account, Databases, WorldShape};
    use std::collections::HashMap;
    use std::sync::Arc;

    fn meta_at(ms: u64) -> CellMeta {
        CellMeta {
            created_at_ms: ms,
            modified_at_ms: ms,
            version: 0,
        }
    }

    const A: u64 = 0xA;
    const B: u64 = 0xB;
    const LEGACY: u64 = 0xC | kblockdblib::LEGACY_BIT;

    fn vector(entries: &[(u64, u64)]) -> VersionVector {
        entries.iter().copied().collect()
    }

    fn all_changes_since(state: &AppState, known: &VersionVector) -> Vec<ChangeEntry> {
        let mut out = Vec::new();
        state
            .changes_since(known, LEGACY, &mut |batch| {
                out.extend(batch);
                true
            })
            .unwrap();
        out.sort_by(|a, b| (&a.database, &a.coord).cmp(&(&b.database, &b.coord)));
        out
    }

    #[test]
    fn the_change_source_reports_every_databases_sets_and_removes() {
        let dir = temp_dir("change-source");
        let shape = WorldShape {
            axes: 3,
            world_dim: 100,
            chunk_dim: 32,
        };
        let databases =
            Databases::new(&dir.0, shape).with_tombstone_retention(Some(std::time::Duration::MAX));
        let state = AppState::new(databases, Arc::new(HashMap::new()));
        state.databases.create(DB, None).unwrap();
        state.databases.create("other", None).unwrap();
        let db = state.databases.get(DB).unwrap();
        let other = state.databases.get("other").unwrap();
        db.apply_replicated_stamped(
            &[1, 1, 1],
            "k",
            Value::I64(1),
            meta_at(100),
            Stamp::new(A, 1),
        )
        .unwrap();
        db.apply_replicated_stamped(
            &[2, 2, 2],
            "k",
            Value::I64(2),
            meta_at(300),
            Stamp::new(A, 2),
        )
        .unwrap();
        db.apply_replicated_remove_stamped(&[1, 1, 1], "k", 400, Stamp::new(B, 1))
            .unwrap();
        other
            .apply_replicated_stamped(
                &[3, 3, 3],
                "k",
                Value::I64(3),
                meta_at(500),
                Stamp::new(B, 2),
            )
            .unwrap();

        let changes = all_changes_since(&state, &vector(&[(A, 1)]));
        assert_eq!(changes.len(), 3);
        assert_eq!(
            (
                changes[0].database.as_str(),
                &changes[0].coord,
                &changes[0].op
            ),
            (DB, &vec![1, 1, 1], &ChangeOp::Remove)
        );
        assert_eq!(changes[0].modified_at_ms, 400);
        assert_eq!(
            (
                changes[1].database.as_str(),
                &changes[1].coord,
                &changes[1].op
            ),
            (DB, &vec![2, 2, 2], &ChangeOp::Set(Value::I64(2)))
        );
        assert_eq!(changes[1].modified_at_ms, 300);
        assert_eq!(changes[2].database, "other");

        assert_eq!(changes[0].stamp(), Stamp::new(B, 1));
        assert_eq!(changes[2].stamp(), Stamp::new(B, 2));

        let changes = all_changes_since(&state, &vector(&[(A, 2), (B, 1)]));
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].database, "other");
        assert!(all_changes_since(&state, &vector(&[(A, 2), (B, 2)])).is_empty());
    }

    fn entry(database: &str, coord: Vec<i32>, op: ChangeOp, ms: u64) -> ChangeEntry {
        ChangeEntry {
            database: database.to_string(),
            coord,
            key: "k".to_string(),
            op,
            created_at_ms: ms,
            modified_at_ms: ms,
            version: 0,
            origin: A,
            seq: ms,
        }
    }

    #[tokio::test]
    async fn apply_batch_applies_every_database_and_auto_creates_on_a_set() {
        let dir = temp_dir("apply-batch");
        let state = test_state(&dir.0);
        let written_before = state.databases.get(DB).unwrap().chunks_written_to_disk();
        state
            .apply_batch(vec![
                entry(DB, vec![1, 1, 1], ChangeOp::Set(Value::I64(1)), 100),
                entry(DB, vec![2, 2, 2], ChangeOp::Set(Value::I64(2)), 100),
                entry(DB, vec![1, 1, 1], ChangeOp::Remove, 200),
                entry(DB, vec![1, 2], ChangeOp::Set(Value::I64(3)), 100), // bad: logged
                entry("fresh", vec![0, 0, 0], ChangeOp::Set(Value::I64(4)), 100),
                entry("removes-only", vec![0, 0, 0], ChangeOp::Remove, 100),
            ])
            .await;

        let db = state.databases.get(DB).unwrap();
        // Three good changes, all in one chunk: one write.
        assert_eq!(db.chunks_written_to_disk() - written_before, 1);
        assert_eq!(db.get(&[1, 1, 1], "k").unwrap(), None);
        assert_eq!(db.get(&[2, 2, 2], "k").unwrap(), Some(Value::I64(2)));
        assert_eq!(
            state
                .databases
                .get("fresh")
                .unwrap()
                .get(&[0, 0, 0], "k")
                .unwrap(),
            Some(Value::I64(4))
        );
        assert!(matches!(
            state.databases.get("removes-only"),
            Err(ApiError::NotFound(_))
        ));
    }

    #[test]
    fn the_change_source_stops_when_emit_returns_false() {
        let dir = temp_dir("change-source-stop");
        let state = test_state(&dir.0);
        state.databases.create("other", None).unwrap();
        for name in [DB, "other"] {
            state
                .databases
                .get(name)
                .unwrap()
                .set(&[0, 0, 0], "k", Value::I64(1))
                .unwrap();
        }
        let mut batches = 0;
        state
            .changes_since(&VersionVector::new(), LEGACY, &mut |_| {
                batches += 1;
                false
            })
            .unwrap();
        assert_eq!(batches, 1);
    }

    #[test]
    fn indexed_keys_reports_every_databases_indexes() {
        let dir = temp_dir("indexed-keys");
        let state = test_state(&dir.0);
        state.databases.create("other", None).unwrap();
        let db = state.databases.get(DB).unwrap();
        db.set(&[0, 0, 0], "material", Value::Str("stone".into()))
            .unwrap();
        db.set(&[0, 0, 0], "hardness", Value::I64(1)).unwrap();
        db.create_index("material").unwrap();
        db.create_index("hardness").unwrap();
        let other = state.databases.get("other").unwrap();
        other
            .set(&[0, 0, 0], "density", Value::F64(1.0))
            .unwrap();
        other.create_index("density").unwrap();

        let mut got = state.indexed_keys().unwrap();
        got.sort();
        assert_eq!(
            got,
            vec![
                (DB.to_string(), "hardness".to_string()),
                (DB.to_string(), "material".to_string()),
                ("other".to_string(), "density".to_string()),
            ]
        );
    }

    #[test]
    fn indexed_keys_is_empty_when_nothing_is_indexed() {
        let dir = temp_dir("indexed-keys-empty");
        let state = test_state(&dir.0);
        assert_eq!(state.indexed_keys().unwrap(), Vec::new());
    }

    const DB: &str = "db";

    fn test_state(dir: &std::path::Path) -> AppState {
        let shape = WorldShape {
            axes: 3,
            world_dim: 100,
            chunk_dim: 32,
        };
        let databases = Databases::new(dir, shape);
        databases.create(DB, None).unwrap();
        let mut credentials = HashMap::new();
        credentials.insert(
            "admin".to_string(),
            Account {
                password: "admin-pw".to_string(),
                read_only: false,
            },
        );
        AppState::new(databases, Arc::new(credentials))
    }

    struct TempDir(std::path::PathBuf);
    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn temp_dir(tag: &str) -> TempDir {
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "kblockdbserver-cluster-test-{tag}-{n}-{}",
            std::process::id()
        ));
        TempDir(dir)
    }

    #[tokio::test]
    async fn apply_set_writes_to_an_existing_database() {
        let dir = temp_dir("existing-db");
        let state = test_state(&dir.0);

        state
            .apply_set(
                DB.to_string(),
                vec![1, 2, 3],
                "material".to_string(),
                Value::Str("stone".to_string()),
                CellMeta {
                    created_at_ms: 10,
                    modified_at_ms: 10,
                    version: 0,
                },
                Stamp::NONE,
            )
            .await
            .unwrap();

        assert_eq!(
            state
                .databases
                .get(DB)
                .unwrap()
                .get(&[1, 2, 3], "material")
                .unwrap(),
            Some(Value::Str("stone".to_string()))
        );
    }

    #[tokio::test]
    async fn apply_set_auto_creates_an_unseen_database() {
        let dir = temp_dir("auto-create");
        let state = test_state(&dir.0);

        state
            .apply_set(
                "brand-new".to_string(),
                vec![0, 0, 0],
                "k".to_string(),
                Value::I64(1),
                CellMeta {
                    created_at_ms: 1,
                    modified_at_ms: 1,
                    version: 0,
                },
                Stamp::NONE,
            )
            .await
            .unwrap();

        assert_eq!(
            state
                .databases
                .get("brand-new")
                .unwrap()
                .get(&[0, 0, 0], "k")
                .unwrap(),
            Some(Value::I64(1))
        );
    }

    #[tokio::test]
    async fn apply_set_respects_last_write_wins() {
        let dir = temp_dir("lww");
        let state = test_state(&dir.0);
        state
            .apply_set(
                DB.to_string(),
                vec![2, 2, 2],
                "material".to_string(),
                Value::Str("granite".to_string()),
                CellMeta {
                    created_at_ms: 100,
                    modified_at_ms: 200,
                    version: 1,
                },
                Stamp::NONE,
            )
            .await
            .unwrap();

        // Older than what's already there -- applied without error (this
        // layer doesn't distinguish "discarded, too old" from "applied"),
        // but the value must not actually change.
        state
            .apply_set(
                DB.to_string(),
                vec![2, 2, 2],
                "material".to_string(),
                Value::Str("stone".to_string()),
                CellMeta {
                    created_at_ms: 50,
                    modified_at_ms: 150,
                    version: 9,
                },
                Stamp::NONE,
            )
            .await
            .unwrap();

        assert_eq!(
            state
                .databases
                .get(DB)
                .unwrap()
                .get(&[2, 2, 2], "material")
                .unwrap(),
            Some(Value::Str("granite".to_string()))
        );
    }

    #[tokio::test]
    async fn apply_remove_for_an_unseen_database_is_a_harmless_no_op() {
        let dir = temp_dir("remove-unseen");
        let state = test_state(&dir.0);

        state
            .apply_remove(
                "never-created".to_string(),
                vec![0, 0, 0],
                "k".to_string(),
                1,
                Stamp::NONE,
            )
            .await
            .unwrap();

        assert!(state.databases.get("never-created").is_err());
    }

    #[tokio::test]
    async fn apply_remove_clears_an_existing_value() {
        let dir = temp_dir("remove-existing");
        let state = test_state(&dir.0);
        state
            .apply_set(
                DB.to_string(),
                vec![1, 1, 1],
                "material".to_string(),
                Value::Str("stone".to_string()),
                CellMeta {
                    created_at_ms: 10,
                    modified_at_ms: 10,
                    version: 0,
                },
                Stamp::NONE,
            )
            .await
            .unwrap();

        state
            .apply_remove(
                DB.to_string(),
                vec![1, 1, 1],
                "material".to_string(),
                20,
                Stamp::NONE,
            )
            .await
            .unwrap();

        assert_eq!(
            state
                .databases
                .get(DB)
                .unwrap()
                .get(&[1, 1, 1], "material")
                .unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn apply_index_op_create_builds_an_index_on_an_existing_database() {
        let dir = temp_dir("index-op-create");
        let state = test_state(&dir.0);
        state
            .databases
            .get(DB)
            .unwrap()
            .set(&[1, 1, 1], "material", Value::Str("stone".to_string()))
            .unwrap();

        state
            .apply_index_op(DB.to_string(), "material".to_string(), IndexOp::Create)
            .await
            .unwrap();

        assert_eq!(
            state.databases.get(DB).unwrap().indexed_keys(),
            vec!["material".to_string()]
        );
    }

    #[tokio::test]
    async fn apply_index_op_drop_removes_an_existing_index() {
        let dir = temp_dir("index-op-drop");
        let state = test_state(&dir.0);
        let world = state.databases.get(DB).unwrap();
        world
            .set(&[1, 1, 1], "material", Value::Str("stone".to_string()))
            .unwrap();
        world.create_index("material").unwrap();

        state
            .apply_index_op(DB.to_string(), "material".to_string(), IndexOp::Drop)
            .await
            .unwrap();

        assert_eq!(world.indexed_keys(), Vec::<String>::new());
    }

    #[tokio::test]
    async fn apply_index_op_rebuild_rebuilds_an_existing_index() {
        let dir = temp_dir("index-op-rebuild");
        let state = test_state(&dir.0);
        let world = state.databases.get(DB).unwrap();
        world
            .set(&[1, 1, 1], "material", Value::Str("stone".to_string()))
            .unwrap();
        world.create_index("material").unwrap();

        state
            .apply_index_op(DB.to_string(), "material".to_string(), IndexOp::Rebuild)
            .await
            .unwrap();

        assert_eq!(
            world.lookup_eq("material", &Value::Str("stone".to_string())),
            Some(vec![kblockdblib::Coord::from([1, 1, 1])])
        );
    }

    #[tokio::test]
    async fn apply_index_op_for_an_unseen_database_is_a_harmless_no_op() {
        let dir = temp_dir("index-op-unseen");
        let state = test_state(&dir.0);

        state
            .apply_index_op(
                "never-created".to_string(),
                "material".to_string(),
                IndexOp::Create,
            )
            .await
            .unwrap();

        assert!(state.databases.get("never-created").is_err());
    }
}
