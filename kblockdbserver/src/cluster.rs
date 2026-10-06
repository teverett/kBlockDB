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
use kblockdbcluster::wire::{ChangeEntry, ChangeOp, DatabaseSync, IndexOp};
use kblockdblib::{CellMeta, Change, ChangeKind, Stamp, Value, VersionVector};

/// One peer's latest `SyncReport`, compared against this server's own
/// current state at the moment it arrived -- see `AppState::
/// apply_sync_report` (which both logs any mismatch and records one of
/// these) and `routes.rs`'s `/rest/cluster` surfacing.
#[derive(Debug, Clone)]
pub struct PeerSyncStatus {
    /// This server's own clock, when the report was applied.
    pub received_at_ms: u64,
    pub databases: Vec<DatabaseSyncStatus>,
}

/// One database's comparison, for one peer's report -- only ever built
/// for a database *this* server also has a content digest for (see
/// `AppState::apply_sync_report`); a database the peer reported that this
/// server doesn't know about (yet, or at all) is simply skipped, not
/// reported as a mismatch -- there's nothing to compare it against.
#[derive(Debug, Clone)]
pub struct DatabaseSyncStatus {
    pub database: String,
    pub content_in_sync: bool,
    pub indexed_keys_in_sync: bool,
    /// This server's own `World::content_digest()` for this database, as
    /// of the moment the comparison ran -- for `/rest/cluster` to show
    /// side by side with `peer_content_digest`, so a mismatch is visibly
    /// a mismatch, not just a bare `false`.
    pub local_content_digest: u64,
    /// The peer's own reported digest for this database, from the same
    /// `SyncReport` -- equal to `local_content_digest` exactly when
    /// `content_in_sync` is `true`.
    pub peer_content_digest: u64,
    /// Sorted, for a stable, directly-comparable rendering.
    pub local_indexed_keys: Vec<String>,
    pub peer_indexed_keys: Vec<String>,
    /// `None` until a drill-down for this mismatch completes (see
    /// `AppState::apply_chunk_digests`); `Some(chunks)` once it does, where
    /// `chunks` is every chunk key (sorted) that the peer's
    /// `chunk_digests()` disagreed with this server's own on, or that
    /// only one side had. Only ever requested when `content_in_sync` is
    /// `false`, so this stays `None` for a database that's actually in
    /// sync.
    pub differing_chunks: Option<Vec<Vec<i32>>>,
}

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

    /// Compares `report` -- `from_node`'s current fingerprint for each of
    /// its databases -- against this server's own, logging any
    /// disagreement and recording the result for `/rest/cluster` to
    /// surface (`AppState::peer_sync`). A database `report` names that
    /// this server has no content digest for (never enabled, or a
    /// database it doesn't have at all) is skipped, not reported as a
    /// mismatch -- there's nothing on this side to compare it against.
    ///
    /// Returns every database whose content digest disagreed, so
    /// `kblockdbcluster::server::handle_connection` can ask `from_node`
    /// for a chunk-digest drill-down on each one (see
    /// `apply_chunk_digests`) -- an indexed-keys-only disagreement isn't
    /// included, since there's no chunk to localize that to.
    async fn apply_sync_report(&self, from_node: u64, report: Vec<DatabaseSync>) -> Vec<String> {
        let mut databases = Vec::with_capacity(report.len());
        let mut mismatched = Vec::new();
        for peer_db in report {
            let local = self
                .with_database(&peer_db.database, |w| {
                    Ok((w.content_digest(), w.indexed_keys()))
                })
                .await;
            let Ok((Some(local_digest), mut local_keys)) = local else {
                continue;
            };
            local_keys.sort();
            let mut peer_keys = peer_db.indexed_keys;
            peer_keys.sort();
            let content_in_sync = local_digest == peer_db.content_digest;
            let indexed_keys_in_sync = local_keys == peer_keys;
            if !content_in_sync || !indexed_keys_in_sync {
                eprintln!(
                    "cluster sync check: this server and peer node {from_node:016x} disagree on \
                     database '{}' -- content_in_sync={content_in_sync} (local digest \
                     {local_digest:016x}, peer {:016x}), indexed_keys_in_sync={indexed_keys_in_sync} \
                     (local {local_keys:?}, peer {peer_keys:?})",
                    peer_db.database, peer_db.content_digest,
                );
            }
            if !content_in_sync {
                mismatched.push(peer_db.database.clone());
            }
            databases.push(DatabaseSyncStatus {
                database: peer_db.database,
                content_in_sync,
                indexed_keys_in_sync,
                local_content_digest: local_digest,
                peer_content_digest: peer_db.content_digest,
                local_indexed_keys: local_keys,
                peer_indexed_keys: peer_keys,
                differing_chunks: None,
            });
        }
        let status = PeerSyncStatus {
            received_at_ms: kblockdbcluster::hub::now_ms(),
            databases,
        };
        self.peer_sync.lock().unwrap().insert(from_node, status);
        mismatched
    }

    /// Diffs `digests` -- `from_node`'s `chunk_digests()` for `database`,
    /// sent in reply to this server's drill-down request -- against this
    /// server's own, to find exactly which chunk(s) the whole-database
    /// mismatch `apply_sync_report` already found is in. Logs the result
    /// and fills in `differing_chunks` on the matching `DatabaseSyncStatus`
    /// entry recorded for `from_node` (a no-op if that entry is somehow
    /// gone by the time the reply arrives, e.g. a fresher `SyncReport`
    /// already replaced it).
    async fn apply_chunk_digests(
        &self,
        from_node: u64,
        database: String,
        digests: Vec<(Vec<i32>, u64)>,
    ) {
        let local = match self.with_database(&database, |w| w.chunk_digests()).await {
            Ok(local) => local,
            Err(_) => return,
        };
        let mut peer: std::collections::HashMap<Vec<i32>, u64> = digests.into_iter().collect();
        let mut differing = Vec::new();
        for (chunk_key, local_digest) in &local {
            let chunk_key = chunk_key.to_vec();
            match peer.remove(&chunk_key) {
                Some(peer_digest) if peer_digest == *local_digest => {}
                _ => differing.push(chunk_key),
            }
        }
        differing.extend(peer.into_keys());
        differing.sort();
        eprintln!(
            "cluster sync check: drill-down against peer node {from_node:016x} for database \
             '{database}' found {} differing chunk(s): {differing:?}",
            differing.len()
        );
        if let Some(status) = self.peer_sync.lock().unwrap().get_mut(&from_node) {
            if let Some(db_status) = status.databases.iter_mut().find(|d| d.database == database)
            {
                db_status.differing_chunks = Some(differing);
            }
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

    fn sync_state(&self) -> Result<Vec<DatabaseSync>, String> {
        let names = self.databases.list().map_err(|e| e.to_string())?;
        let mut out = Vec::new();
        for database in names {
            let world = match self.databases.get(&database) {
                Ok(world) => world,
                Err(ApiError::NotFound(_)) => continue,
                Err(e) => return Err(format!("{e:?}")),
            };
            if let Some(content_digest) = world.content_digest() {
                out.push(DatabaseSync {
                    database,
                    content_digest,
                    indexed_keys: world.indexed_keys(),
                });
            }
        }
        Ok(out)
    }

    fn chunk_digests(&self, database: &str) -> Result<Vec<(Vec<i32>, u64)>, String> {
        let world = match self.databases.get(database) {
            Ok(world) => world,
            Err(ApiError::NotFound(_)) => return Ok(Vec::new()),
            Err(e) => return Err(format!("{e:?}")),
        };
        let digests = world.chunk_digests().map_err(|e| e.to_string())?;
        Ok(digests.into_iter().map(|(k, v)| (k.to_vec(), v)).collect())
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

    #[test]
    fn sync_state_reports_every_digested_databases_fingerprint() {
        let dir = temp_dir("sync-state");
        let state = test_state_with_digest(&dir.0);
        let db = state.databases.get(DB).unwrap();
        db.set(&[0, 0, 0], "material", Value::Str("stone".into()))
            .unwrap();
        db.create_index("material").unwrap();

        let report = state.sync_state().unwrap();
        assert_eq!(report.len(), 1);
        assert_eq!(report[0].database, DB);
        assert_eq!(report[0].indexed_keys, vec!["material".to_string()]);
        assert_eq!(report[0].content_digest, db.content_digest().unwrap());
    }

    #[test]
    fn sync_state_is_empty_when_no_database_has_a_content_digest() {
        let dir = temp_dir("sync-state-no-digest");
        // Plain `test_state` -- content digests disabled.
        let state = test_state(&dir.0);
        assert_eq!(state.sync_state().unwrap(), Vec::new());
    }

    #[test]
    fn change_source_chunk_digests_matches_the_worlds_own() {
        let dir = temp_dir("change-source-chunk-digests");
        let state = test_state(&dir.0);
        let db = state.databases.get(DB).unwrap();
        db.set(&[0, 0, 0], "material", Value::Str("stone".into()))
            .unwrap();
        db.set(&[40, 0, 0], "material", Value::Str("dirt".into()))
            .unwrap();

        let mut got = state.chunk_digests(DB).unwrap();
        got.sort();
        let mut expected: Vec<(Vec<i32>, u64)> = db
            .chunk_digests()
            .unwrap()
            .into_iter()
            .map(|(k, v)| (k.to_vec(), v))
            .collect();
        expected.sort();
        assert_eq!(got, expected);
    }

    #[test]
    fn change_source_chunk_digests_for_an_unseen_database_is_empty() {
        let dir = temp_dir("change-source-chunk-digests-unseen");
        let state = test_state(&dir.0);
        assert_eq!(state.chunk_digests("never-created").unwrap(), Vec::new());
    }

    #[tokio::test]
    async fn apply_sync_report_records_a_matching_comparison() {
        let dir = temp_dir("sync-report-match");
        let state = test_state_with_digest(&dir.0);
        let db = state.databases.get(DB).unwrap();
        db.set(&[0, 0, 0], "material", Value::Str("stone".into()))
            .unwrap();
        db.create_index("material").unwrap();

        let report = vec![kblockdbcluster::wire::DatabaseSync {
            database: DB.to_string(),
            content_digest: db.content_digest().unwrap(),
            indexed_keys: vec!["material".to_string()],
        }];
        state.apply_sync_report(0xA, report).await;

        let stored = state.peer_sync.lock().unwrap().get(&0xA).unwrap().clone();
        assert_eq!(stored.databases.len(), 1);
        assert!(stored.databases[0].content_in_sync);
        assert!(stored.databases[0].indexed_keys_in_sync);
    }

    #[tokio::test]
    async fn apply_sync_report_records_a_mismatch() {
        let dir = temp_dir("sync-report-mismatch");
        let state = test_state_with_digest(&dir.0);
        let db = state.databases.get(DB).unwrap();
        db.set(&[0, 0, 0], "material", Value::Str("stone".into()))
            .unwrap();
        db.create_index("material").unwrap();

        // Wrong digest, and an indexed-key set the peer doesn't have.
        let report = vec![kblockdbcluster::wire::DatabaseSync {
            database: DB.to_string(),
            content_digest: db.content_digest().unwrap() ^ 1,
            indexed_keys: vec![],
        }];
        state.apply_sync_report(0xB, report).await;

        let stored = state.peer_sync.lock().unwrap().get(&0xB).unwrap().clone();
        assert_eq!(stored.databases.len(), 1);
        assert!(!stored.databases[0].content_in_sync);
        assert!(!stored.databases[0].indexed_keys_in_sync);
        assert_eq!(
            stored.databases[0].local_indexed_keys,
            vec!["material".to_string()]
        );
        assert_eq!(stored.databases[0].peer_indexed_keys, Vec::<String>::new());
    }

    #[tokio::test]
    async fn apply_sync_report_skips_a_database_with_no_local_digest() {
        let dir = temp_dir("sync-report-no-local-digest");
        // Note: plain `test_state`, not `test_state_with_digest` -- this
        // server has no content digest enabled on `DB` at all.
        let state = test_state(&dir.0);

        let report = vec![kblockdbcluster::wire::DatabaseSync {
            database: DB.to_string(),
            content_digest: 42,
            indexed_keys: vec![],
        }];
        state.apply_sync_report(0xC, report).await;

        let stored = state.peer_sync.lock().unwrap().get(&0xC).unwrap().clone();
        assert!(stored.databases.is_empty());
    }

    #[tokio::test]
    async fn apply_sync_report_returns_the_mismatched_database() {
        let dir = temp_dir("sync-report-returns-mismatch");
        let state = test_state_with_digest(&dir.0);
        let db = state.databases.get(DB).unwrap();
        db.set(&[0, 0, 0], "material", Value::Str("stone".into()))
            .unwrap();

        let matching_report = vec![kblockdbcluster::wire::DatabaseSync {
            database: DB.to_string(),
            content_digest: db.content_digest().unwrap(),
            indexed_keys: vec![],
        }];
        assert_eq!(
            state.apply_sync_report(0xA, matching_report).await,
            Vec::<String>::new()
        );

        let mismatched_report = vec![kblockdbcluster::wire::DatabaseSync {
            database: DB.to_string(),
            content_digest: db.content_digest().unwrap() ^ 1,
            indexed_keys: vec![],
        }];
        assert_eq!(
            state.apply_sync_report(0xA, mismatched_report).await,
            vec![DB.to_string()]
        );
    }

    #[tokio::test]
    async fn apply_chunk_digests_records_exactly_which_chunks_differ() {
        let dir = temp_dir("chunk-digests-diff");
        let state = test_state_with_digest(&dir.0);
        let db = state.databases.get(DB).unwrap();
        db.set(&[0, 0, 0], "material", Value::Str("stone".into()))
            .unwrap();
        db.set(&[40, 0, 0], "material", Value::Str("dirt".into()))
            .unwrap();
        // Seed a prior sync-report entry for this peer/database, same as
        // a real drill-down's request would have already recorded.
        state.peer_sync.lock().unwrap().insert(
            0xA,
            PeerSyncStatus {
                received_at_ms: 1,
                databases: vec![DatabaseSyncStatus {
                    database: DB.to_string(),
                    content_in_sync: false,
                    indexed_keys_in_sync: true,
                    local_content_digest: 0,
                    peer_content_digest: 1,
                    local_indexed_keys: vec![],
                    peer_indexed_keys: vec![],
                    differing_chunks: None,
                }],
            },
        );

        let mut local = db.chunk_digests().unwrap().into_iter().collect::<Vec<_>>();
        local.sort_by_key(|(k, _)| k.to_vec());
        // The peer agrees on the chunk holding (0, 0, 0), disagrees on the
        // chunk holding (40, 0, 0), and is missing a third chunk entirely.
        let peer_digests: Vec<(Vec<i32>, u64)> = vec![
            (local[0].0.to_vec(), local[0].1),
            (local[1].0.to_vec(), local[1].1 ^ 1),
            (vec![9, 9, 9], 0xDEAD),
        ];

        state
            .apply_chunk_digests(0xA, DB.to_string(), peer_digests)
            .await;

        let stored = state.peer_sync.lock().unwrap().get(&0xA).unwrap().clone();
        let differing = stored.databases[0].differing_chunks.clone().unwrap();
        let mut expected = vec![local[1].0.to_vec(), vec![9, 9, 9]];
        expected.sort();
        assert_eq!(differing, expected);
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

    /// `test_state`, with every database's content digest enabled -- for
    /// tests exercising `sync_state`/`apply_sync_report`, which have
    /// nothing to report or compare without one.
    fn test_state_with_digest(dir: &std::path::Path) -> AppState {
        let shape = WorldShape {
            axes: 3,
            world_dim: 100,
            chunk_dim: 32,
        };
        let databases = Databases::new(dir, shape).with_content_digest(true);
        databases.create(DB, None).unwrap();
        AppState::new(databases, Arc::new(HashMap::new()))
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
