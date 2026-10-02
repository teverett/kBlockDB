//! The accepting side of a peer link -- listens for connections from
//! other servers listed in *their* peer config, authenticates them
//! against a shared `cluster_secret`, and applies every `ChangeEntry`
//! they stream by handing it to a [`ReplicationSink`] the embedder
//! provides. See `wire.rs` for the wire format and `client.rs` for the
//! connecting side.

use crate::registry::PeerRegistry;
use crate::wire::{ChangeEntry, ChangeOp, PeerMessage};
use std::future::Future;
use tokio::net::{TcpListener, TcpStream};

/// What `serve` applies each incoming `ChangeEntry` against -- implemented
/// by the embedder (e.g. `kblockdbserver`'s `AppState`) so this crate
/// stays storage-agnostic: it knows nothing about databases, `World`, or
/// accounts, only "apply this set/remove, report success or a message".
/// Auto-creating a database this process has never seen before (if that's
/// even a concept for the embedder), and any retry around doing so, is
/// entirely the implementation's responsibility -- this trait doesn't
/// distinguish "unknown database" from any other failure, and `serve`
/// calls each method exactly once per entry.
pub trait ReplicationSink: Clone + Send + Sync + 'static {
    fn apply_set(
        &self,
        database: String,
        coord: Vec<i32>,
        key: String,
        value: kblockdblib::Value,
        meta: kblockdblib::CellMeta,
    ) -> impl Future<Output = Result<(), String>> + Send;

    fn apply_remove(
        &self,
        database: String,
        coord: Vec<i32>,
        key: String,
        modified_at_ms: u64,
    ) -> impl Future<Output = Result<(), String>> + Send;
}

/// Accepts connections forever, spawning one task per connection -- same
/// shape as `kblockdbserver`'s own binary-protocol listener. Doesn't
/// participate in any graceful shutdown of its own: a dropped connection
/// mid-stream only ever loses change entries still in flight, never
/// corrupts anything already applied (an embedder's `ReplicationSink`
/// impl is expected to make each entry durable before resolving, the same
/// way a local write would be).
///
/// `registry` records every peer currently connected (by its own
/// self-reported `server_id`) for as long as its connection lasts -- see
/// `PeerRegistry`. Pass the same registry the embedder also reads (e.g.
/// to report connected peers from a health endpoint); `serve` only ever
/// adds to and removes from it, never reads it back.
pub async fn serve<S: ReplicationSink>(
    listener: TcpListener,
    sink: S,
    cluster_secret: String,
    registry: PeerRegistry,
) {
    loop {
        let (stream, _addr) = match listener.accept().await {
            Ok(pair) => pair,
            Err(e) => {
                eprintln!("peer protocol: accept failed: {e}");
                continue;
            }
        };
        let _ = stream.set_nodelay(true);
        let sink = sink.clone();
        let cluster_secret = cluster_secret.clone();
        let registry = registry.clone();
        tokio::spawn(async move {
            handle_connection(stream, sink, &cluster_secret, registry).await;
        });
    }
}

/// One peer's connection: `Hello` (checked against `cluster_secret`),
/// then an unbounded stream of `ChangeBatch` frames applied as they
/// arrive -- no responses are ever sent back for those (see `wire.rs`'s
/// doc comment on why this protocol is push-only).
async fn handle_connection<S: ReplicationSink>(
    mut stream: TcpStream,
    sink: S,
    cluster_secret: &str,
    registry: PeerRegistry,
) {
    let hello = match crate::wire::read_message(&mut stream).await {
        Ok(Some(msg)) => msg,
        Ok(None) => return,
        Err(_) => return,
    };
    let PeerMessage::Hello {
        secret,
        server_id,
        protocol_version,
    } = hello
    else {
        let _ = crate::wire::write_message(
            &mut stream,
            &PeerMessage::HelloRejected("expected Hello first".to_string()),
        )
        .await;
        return;
    };

    // Same "reject a client newer than me" reasoning as the end-user
    // binary protocol's own `Hello`/`HelloOk` -- checked before the
    // secret so a too-new peer learns nothing about whether its secret
    // would otherwise have been accepted.
    if protocol_version > crate::wire::PEER_PROTOCOL_VERSION {
        let _ = crate::wire::write_message(
            &mut stream,
            &PeerMessage::HelloRejected(format!(
                "peer protocol version {protocol_version} is newer than this server supports \
                 (version {})",
                crate::wire::PEER_PROTOCOL_VERSION
            )),
        )
        .await;
        return;
    }

    // Plain equality, not constant-time -- unlike an end-user password
    // check, which matters because a remote attacker can make unlimited
    // guesses against it. A peer secret is an operator-chosen,
    // cluster-wide value checked over a link that's expected to sit on a
    // trusted/firewalled network in the first place (see
    // docs/clustering.md's "Limitations"); adding a dependency just for
    // this comparison isn't worth it for v1.
    if secret != cluster_secret {
        let _ = crate::wire::write_message(
            &mut stream,
            &PeerMessage::HelloRejected("bad cluster secret".to_string()),
        )
        .await;
        return;
    }
    if crate::wire::write_message(&mut stream, &PeerMessage::HelloOk)
        .await
        .is_err()
    {
        return;
    }
    eprintln!("peer protocol: '{server_id}' connected");
    // Held for the rest of this connection -- dropping it (whichever of
    // this loop's several exit points runs) removes `server_id` from
    // `registry` again, so a connected peer only ever appears in it for
    // as long as the connection actually lasts.
    let _connected = registry.track(server_id);

    loop {
        let msg = match crate::wire::read_message(&mut stream).await {
            Ok(Some(msg)) => msg,
            Ok(None) => return,
            Err(_) => return,
        };
        let PeerMessage::ChangeBatch(entries) = msg else {
            // Only Hello/ChangeBatch ever travel this direction -- anything
            // else is a protocol error, but per this module's "one bad
            // frame doesn't end the connection" policy, just skip it
            // rather than disconnecting.
            continue;
        };
        for entry in entries {
            apply_entry(&sink, entry).await;
        }
    }
}

/// Applies one replicated change via `sink`, logging (rather than
/// propagating) a failure -- a single bad entry (an axis-count mismatch
/// between differently-shaped databases, say) must never kill the whole
/// connection.
async fn apply_entry<S: ReplicationSink>(sink: &S, entry: ChangeEntry) {
    let ChangeEntry {
        database,
        coord,
        key,
        op,
        created_at_ms,
        modified_at_ms,
        version,
    } = entry;
    let result = match op {
        ChangeOp::Set(value) => {
            let meta = kblockdblib::CellMeta {
                created_at_ms,
                modified_at_ms,
                version,
            };
            sink.apply_set(database.clone(), coord.clone(), key, value, meta)
                .await
        }
        ChangeOp::Remove => {
            sink.apply_remove(database.clone(), coord.clone(), key, modified_at_ms)
                .await
        }
    };
    if let Err(e) = result {
        eprintln!("peer protocol: dropping entry for '{database}' at {coord:?}: {e}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::RecordingSink;
    use kblockdblib::Value;
    use tokio::net::TcpStream as ClientStream;

    const CLUSTER_SECRET: &str = "cluster-secret";
    const DB: &str = "db";

    async fn spawn_test_server() -> (std::net::SocketAddr, RecordingSink, PeerRegistry) {
        let sink = RecordingSink::new();
        let registry = PeerRegistry::new();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(
            listener,
            sink.clone(),
            CLUSTER_SECRET.to_string(),
            registry.clone(),
        ));
        (addr, sink, registry)
    }

    async fn hello(stream: &mut ClientStream, secret: &str) -> PeerMessage {
        crate::wire::write_message(
            stream,
            &PeerMessage::Hello {
                secret: secret.to_string(),
                server_id: "test-peer".to_string(),
                protocol_version: crate::wire::PEER_PROTOCOL_VERSION,
            },
        )
        .await
        .unwrap();
        crate::wire::read_message(stream).await.unwrap().unwrap()
    }

    #[tokio::test]
    async fn hello_with_the_right_secret_is_accepted() {
        let (addr, _sink, _registry) = spawn_test_server().await;
        let mut stream = ClientStream::connect(addr).await.unwrap();
        assert_eq!(
            hello(&mut stream, CLUSTER_SECRET).await,
            PeerMessage::HelloOk
        );
    }

    #[tokio::test]
    async fn a_connected_peer_is_added_to_the_registry() {
        let (addr, _sink, registry) = spawn_test_server().await;
        let mut stream = ClientStream::connect(addr).await.unwrap();
        hello(&mut stream, CLUSTER_SECRET).await;

        wait_until(|| registry.connected() == vec!["test-peer".to_string()]).await;
    }

    #[tokio::test]
    async fn a_disconnected_peer_is_removed_from_the_registry() {
        let (addr, _sink, registry) = spawn_test_server().await;
        let mut stream = ClientStream::connect(addr).await.unwrap();
        hello(&mut stream, CLUSTER_SECRET).await;
        wait_until(|| !registry.connected().is_empty()).await;

        drop(stream);
        wait_until(|| registry.connected().is_empty()).await;
    }

    #[tokio::test]
    async fn a_rejected_hello_never_reaches_the_registry() {
        let (addr, _sink, registry) = spawn_test_server().await;
        let mut stream = ClientStream::connect(addr).await.unwrap();
        hello(&mut stream, "wrong").await;

        // Give the (incorrect, if it happened) registration a moment to
        // land, then assert it didn't.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(registry.connected().is_empty());
    }

    #[tokio::test]
    async fn hello_with_the_wrong_secret_is_rejected() {
        let (addr, _sink, _registry) = spawn_test_server().await;
        let mut stream = ClientStream::connect(addr).await.unwrap();
        assert!(matches!(
            hello(&mut stream, "wrong").await,
            PeerMessage::HelloRejected(_)
        ));
    }

    #[tokio::test]
    async fn hello_with_a_newer_protocol_version_is_rejected() {
        let (addr, _sink, _registry) = spawn_test_server().await;
        let mut stream = ClientStream::connect(addr).await.unwrap();
        crate::wire::write_message(
            &mut stream,
            &PeerMessage::Hello {
                secret: CLUSTER_SECRET.to_string(),
                server_id: "test-peer".to_string(),
                protocol_version: crate::wire::PEER_PROTOCOL_VERSION + 1,
            },
        )
        .await
        .unwrap();
        assert!(matches!(
            crate::wire::read_message(&mut stream)
                .await
                .unwrap()
                .unwrap(),
            PeerMessage::HelloRejected(_)
        ));
    }

    #[tokio::test]
    async fn a_change_batch_set_is_applied_via_the_sink() {
        let (addr, sink, _registry) = spawn_test_server().await;
        let mut stream = ClientStream::connect(addr).await.unwrap();
        hello(&mut stream, CLUSTER_SECRET).await;

        crate::wire::write_message(
            &mut stream,
            &PeerMessage::ChangeBatch(vec![ChangeEntry {
                database: DB.to_string(),
                coord: vec![1, 2, 3],
                key: "material".to_string(),
                op: ChangeOp::Set(Value::Str("stone".to_string())),
                created_at_ms: 100,
                modified_at_ms: 100,
                version: 0,
            }]),
        )
        .await
        .unwrap();

        wait_until(|| {
            sink.get(DB, &[1, 2, 3], "material")
                == Some((Value::Str("stone".to_string()), 100, 100, 0))
        })
        .await;
    }

    #[tokio::test]
    async fn a_change_batch_remove_is_applied_via_the_sink() {
        let (addr, sink, _registry) = spawn_test_server().await;
        sink.apply_set_now(DB, &[1, 1, 1], "material", Value::Str("stone".to_string()));

        let mut stream = ClientStream::connect(addr).await.unwrap();
        hello(&mut stream, CLUSTER_SECRET).await;
        crate::wire::write_message(
            &mut stream,
            &PeerMessage::ChangeBatch(vec![ChangeEntry {
                database: DB.to_string(),
                coord: vec![1, 1, 1],
                key: "material".to_string(),
                op: ChangeOp::Remove,
                created_at_ms: 0,
                modified_at_ms: 20,
                version: 0,
            }]),
        )
        .await
        .unwrap();

        wait_until(|| sink.is_removed(DB, &[1, 1, 1], "material")).await;
    }

    #[tokio::test]
    async fn a_sink_error_is_logged_and_does_not_close_the_connection() {
        let (addr, sink, _registry) = spawn_test_server().await;
        sink.fail_next();
        let mut stream = ClientStream::connect(addr).await.unwrap();
        hello(&mut stream, CLUSTER_SECRET).await;

        // The first entry fails inside the sink; the connection must stay
        // open and keep applying later entries regardless.
        crate::wire::write_message(
            &mut stream,
            &PeerMessage::ChangeBatch(vec![
                ChangeEntry {
                    database: DB.to_string(),
                    coord: vec![0, 0, 0],
                    key: "k".to_string(),
                    op: ChangeOp::Set(Value::I64(1)),
                    created_at_ms: 1,
                    modified_at_ms: 1,
                    version: 0,
                },
                ChangeEntry {
                    database: DB.to_string(),
                    coord: vec![1, 0, 0],
                    key: "k".to_string(),
                    op: ChangeOp::Set(Value::I64(2)),
                    created_at_ms: 1,
                    modified_at_ms: 1,
                    version: 0,
                },
            ]),
        )
        .await
        .unwrap();

        assert!(sink.get(DB, &[0, 0, 0], "k").is_none());
        wait_until(|| sink.get(DB, &[1, 0, 0], "k") == Some((Value::I64(2), 1, 1, 0))).await;
    }

    async fn wait_until(mut condition: impl FnMut() -> bool) {
        for _ in 0..200 {
            if condition() {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("condition was never met within 2s");
    }
}
