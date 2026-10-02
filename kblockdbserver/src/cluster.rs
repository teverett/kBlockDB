//! Wires `AppState` into `kblockdbcluster`'s generic peer protocol:
//! implements `ReplicationSink` so `kblockdbcluster::server::serve` can
//! apply an incoming change against this server's actual `World`s,
//! without `kblockdbcluster` itself knowing anything about `AppState`/
//! `ApiError`/auto-create-on-unseen-database semantics -- those live here,
//! the one place that's specific to how *this* server manages databases.
//! See `docs/clustering.md` (at the repository root) for the feature as a
//! whole, and `kblockdbcluster`'s own doc comment for why the crate split
//! is shaped this way.

use crate::error::ApiError;
use crate::state::AppState;
use kblockdbcluster::server::ReplicationSink;
use kblockdblib::{CellMeta, Value};

impl ReplicationSink for AppState {
    async fn apply_set(
        &self,
        database: String,
        coord: Vec<i32>,
        key: String,
        value: Value,
        meta: CellMeta,
    ) -> Result<(), String> {
        let (coord2, key2, value2) = (coord.clone(), key.clone(), value.clone());
        match self
            .with_database(&database, move |w| {
                w.apply_replicated(&coord, &key, value, meta).map(|_| ())
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
                            w.apply_replicated(&coord2, &key2, value2, meta).map(|_| ())
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
    ) -> Result<(), String> {
        // No auto-create-on-NotFound here: a database this server has
        // never seen can't have anything to remove from anyway, so a
        // removal for an unseen database is simply a no-op (same outcome
        // auto-creating it and then finding nothing to remove would
        // reach, without the extra round trip).
        match self
            .with_database(&database, move |w| {
                w.apply_replicated_remove(&coord, &key, modified_at_ms)
                    .map(|_| ())
            })
            .await
        {
            Ok(()) | Err(ApiError::NotFound(_)) => Ok(()),
            Err(e) => Err(format!("{e:?}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{Account, Databases, WorldShape};
    use std::collections::HashMap;
    use std::sync::Arc;

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
            )
            .await
            .unwrap();

        state
            .apply_remove(DB.to_string(), vec![1, 1, 1], "material".to_string(), 20)
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
}
