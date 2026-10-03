//! The accepting side of a peer link -- listens for connections from
//! other servers listed in *their* peer config, authenticates them
//! against a shared `cluster_secret`, and applies every `ChangeEntry`
//! they stream by handing it to a [`ReplicationSink`] the embedder
//! provides. See `wire.rs` for the wire format and `client.rs` for the
//! connecting side.

use crate::peers::PeerSet;
use crate::wire::{ChangeEntry, ChangeOp, PeerMessage};
use std::future::Future;
use std::net::{IpAddr, SocketAddr};
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

    /// Applies a whole `ChangeBatch`, in order. The default calls
    /// `apply_set`/`apply_remove` once per entry, logging any failure; an
    /// embedder that can apply many changes more cheaply together (one
    /// write per storage chunk, say -- a catch-up is thousands of
    /// entries) overrides this, and then logs its own failures.
    fn apply_batch(&self, entries: Vec<ChangeEntry>) -> impl Future<Output = ()> + Send {
        async move {
            for entry in entries {
                apply_entry(self, entry).await;
            }
        }
    }
}

/// Accepts connections forever, spawning one task per connection -- same
/// shape as `kblockdbserver`'s own binary-protocol listener. Doesn't
/// participate in any graceful shutdown of its own: a dropped connection
/// mid-stream only ever loses change entries still in flight, never
/// corrupts anything already applied (an embedder's `ReplicationSink`
/// impl is expected to make each entry durable before resolving, the same
/// way a local write would be).
///
/// Every peer that says a valid `Hello` is added to `peers` (keyed by
/// its source IP plus the `peer_port` it reported), which starts
/// replicating back to it if it wasn't already known -- see `peers.rs` on
/// why peers are symmetric. The cluster secret checked against is
/// `peers.identity().cluster_secret`.
pub async fn serve<S: ReplicationSink>(listener: TcpListener, sink: S, peers: PeerSet) {
    loop {
        let (stream, remote) = match listener.accept().await {
            Ok(pair) => pair,
            Err(e) => {
                eprintln!("peer protocol: accept failed: {e}");
                continue;
            }
        };
        crate::socket::tune(&stream, peers.keepalive());
        let sink = sink.clone();
        let peers = peers.clone();
        tokio::spawn(async move {
            handle_connection(stream, sink, peers, remote.ip()).await;
        });
    }
}

/// One peer's connection: `Hello` (checked against `cluster_secret`),
/// a `CatchUpRequest` from this process's watermark for that peer (see
/// `peers.rs`'s "Catch-up"), then an unbounded stream of `ChangeBatch`
/// frames applied as they arrive -- no responses are ever sent back for
/// those -- with the occasional `Synced` advancing the watermark.
async fn handle_connection<S: ReplicationSink>(
    mut stream: TcpStream,
    sink: S,
    peers: PeerSet,
    remote_ip: IpAddr,
) {
    let cluster_secret = peers.identity().cluster_secret.as_str();
    let hello = match crate::wire::read_message(&mut stream).await {
        Ok(Some(msg)) => msg,
        Ok(None) => return,
        Err(_) => return,
    };
    let PeerMessage::Hello {
        secret,
        server_id,
        peer_port,
        node_id,
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
    // Our own node id goes back in `HelloOk` so the connecting side can
    // tell if it just dialed itself (or an alias of a peer it already
    // has) -- see `PeerSet::claim`. It drops the link in that case.
    let ok = PeerMessage::HelloOk {
        node_id: peers.node_id(),
    };
    if crate::wire::write_message(&mut stream, &ok).await.is_err() {
        return;
    }
    if node_id == peers.node_id() {
        // Dialed by ourselves, via some address that reaches this process
        // -- nothing to learn or apply.
        return;
    }
    let dial_back = SocketAddr::new(remote_ip, peer_port).to_string();
    eprintln!("peer protocol: '{server_id}' connected from {dial_back}");
    // Peers are symmetric: a peer that connected in is replicated *to*
    // as well -- a no-op if it's already known (e.g. configured here too).
    if peers.add(dial_back.clone()) {
        eprintln!("peer protocol: learned new peer {dial_back}");
    }

    // Ask for everything missed since we last heard it was synced. An
    // older peer wouldn't understand the request (and would never confirm
    // it), so it just gets live changes, as before.
    if protocol_version >= crate::wire::CATCH_UP_PROTOCOL_VERSION {
        let since_ms = peers.catch_up_since(&dial_back);
        eprintln!("peer protocol: asking {dial_back} for changes since {since_ms}");
        let request = PeerMessage::CatchUpRequest { since_ms };
        if crate::wire::write_message(&mut stream, &request)
            .await
            .is_err()
        {
            return;
        }
    }

    loop {
        let msg = match crate::wire::read_message(&mut stream).await {
            Ok(Some(msg)) => msg,
            Ok(None) => return,
            Err(_) => return,
        };
        match msg {
            PeerMessage::ChangeBatch(entries) => sink.apply_batch(entries).await,
            // Gossip: learn any peer the sender is connected to that we
            // don't know yet -- `add` spawns our link to it.
            PeerMessage::PeerList(addresses) => {
                for address in addresses {
                    if peers.add(address.clone()) {
                        eprintln!("peer protocol: learned {address} via gossip from '{server_id}'");
                    }
                }
            }
            PeerMessage::Synced { through_ms } => peers.record_synced(&dial_back, through_ms),
            // Only Hello/ChangeBatch/PeerList/Synced travel this direction --
            // anything else is a protocol error, but per this module's "one
            // bad frame doesn't end the connection" policy, just skip it
            // rather than disconnecting.
            _ => continue,
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
    use crate::test_support::{
        spawn_node, spawn_node_with, wait_until, RecordingSink, TestNode, CLUSTER_SECRET,
    };
    use kblockdblib::Value;
    use std::time::Duration;
    use tokio::net::TcpStream as ClientStream;

    const DB: &str = "db";

    /// The `peer_port` a raw test client claims in `Hello`. Nothing
    /// listens there, so the dial-back this triggers just retries
    /// harmlessly in the background.
    const UNREACHABLE_PEER_PORT: u16 = 1;

    /// The node id a raw test client claims -- anything but the server's.
    const TEST_CLIENT_NODE_ID: u64 = 42;

    async fn hello(stream: &mut ClientStream, secret: &str) -> PeerMessage {
        hello_as_version(stream, secret, crate::wire::PEER_PROTOCOL_VERSION).await
    }

    async fn hello_as_version(
        stream: &mut ClientStream,
        secret: &str,
        protocol_version: u8,
    ) -> PeerMessage {
        crate::wire::write_message(
            stream,
            &PeerMessage::Hello {
                secret: secret.to_string(),
                server_id: "test-peer".to_string(),
                peer_port: UNREACHABLE_PEER_PORT,
                node_id: TEST_CLIENT_NODE_ID,
                protocol_version,
            },
        )
        .await
        .unwrap();
        crate::wire::read_message(stream).await.unwrap().unwrap()
    }

    /// The address a raw test client is known by on the server: its
    /// source IP plus the `peer_port` it claimed.
    fn raw_client_address() -> String {
        format!("127.0.0.1:{UNREACHABLE_PEER_PORT}")
    }

    /// Connects a raw client, says `Hello`, and returns the stream plus
    /// the `CatchUpRequest` the server sends right after `HelloOk`.
    async fn connect_and_read_catch_up_request(addr: std::net::SocketAddr) -> (ClientStream, u64) {
        let mut stream = ClientStream::connect(addr).await.unwrap();
        assert!(matches!(
            hello(&mut stream, CLUSTER_SECRET).await,
            PeerMessage::HelloOk { .. }
        ));
        match crate::wire::read_message(&mut stream).await.unwrap() {
            Some(PeerMessage::CatchUpRequest { since_ms }) => (stream, since_ms),
            other => panic!("expected CatchUpRequest, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn with_no_watermark_the_receiver_asks_for_everything() {
        let TestNode { addr, .. } = spawn_node("node-b").await;
        let (_stream, since_ms) = connect_and_read_catch_up_request(addr).await;
        assert_eq!(since_ms, 0);
    }

    #[tokio::test]
    async fn the_receiver_asks_from_its_watermark_minus_the_margin() {
        let TestNode { addr, .. } = spawn_node_with("node-b", RecordingSink::new(), |peers| {
            peers.record_synced(&raw_client_address(), 50_000);
            peers.with_catch_up_margin(Duration::from_secs(1))
        })
        .await;
        let (_stream, since_ms) = connect_and_read_catch_up_request(addr).await;
        assert_eq!(since_ms, 49_000);
    }

    #[tokio::test]
    async fn synced_advances_the_watermark_but_a_dropped_catch_up_does_not() {
        let TestNode { addr, peers, .. } = spawn_node_with("node-b", RecordingSink::new(), |p| {
            p.with_catch_up_margin(Duration::ZERO)
        })
        .await;
        let (mut stream, since_ms) = connect_and_read_catch_up_request(addr).await;
        assert_eq!(since_ms, 0);
        crate::wire::write_message(&mut stream, &PeerMessage::Synced { through_ms: 7_000 })
            .await
            .unwrap();
        wait_until(|| peers.watermark(&raw_client_address()) == 7_000).await;
        drop(stream);

        // Asked again from 7000; drops before confirming anything ...
        let (stream, since_ms) = connect_and_read_catch_up_request(addr).await;
        assert_eq!(since_ms, 7_000);
        drop(stream);

        // ... so the next request still starts from 7000.
        let (_stream, since_ms) = connect_and_read_catch_up_request(addr).await;
        assert_eq!(since_ms, 7_000);
    }

    #[tokio::test]
    async fn an_older_peer_gets_no_catch_up_request_but_is_still_applied() {
        let TestNode { addr, sink, .. } = spawn_node("node-b").await;
        let mut stream = ClientStream::connect(addr).await.unwrap();
        assert!(matches!(
            hello_as_version(&mut stream, CLUSTER_SECRET, 2).await,
            PeerMessage::HelloOk { .. }
        ));
        let next = tokio::time::timeout(
            Duration::from_millis(300),
            crate::wire::read_message(&mut stream),
        )
        .await;
        assert!(
            next.is_err(),
            "expected nothing after HelloOk, got {next:?}"
        );

        let entry = ChangeEntry {
            database: DB.to_string(),
            coord: vec![1, 2, 3],
            key: "material".to_string(),
            op: ChangeOp::Set(Value::Str("stone".to_string())),
            created_at_ms: 10,
            modified_at_ms: 10,
            version: 0,
        };
        crate::wire::write_message(&mut stream, &PeerMessage::ChangeBatch(vec![entry]))
            .await
            .unwrap();
        wait_until(|| sink.get(DB, &[1, 2, 3], "material").is_some()).await;
    }

    #[tokio::test]
    async fn a_new_node_gets_a_full_copy_and_a_restarted_one_only_what_it_missed() {
        let watermarks = std::env::temp_dir().join(format!(
            "kblockdbcluster-catch-up-{}/watermarks",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&watermarks);
        let with_watermarks = |path: std::path::PathBuf| {
            move |peers: PeerSet| {
                peers
                    .with_watermarks(&path)
                    .unwrap()
                    .with_catch_up_margin(Duration::ZERO)
            }
        };

        // A holds data from before B ever existed.
        let a_data = RecordingSink::new();
        a_data.seed(DB, &[0, 0, 1], "material", Value::Str("one".into()), 1_000);
        let a = spawn_node_with("node-a", a_data.clone(), |p| p).await;
        let a_address = a.addr.to_string();

        // B is new: it gets everything.
        let b = spawn_node_with(
            "node-b",
            RecordingSink::new(),
            with_watermarks(watermarks.clone()),
        )
        .await;
        a.peers.add(b.addr.to_string());
        wait_until(|| b.sink.get(DB, &[0, 0, 1], "material").is_some()).await;
        wait_until(|| b.peers.watermark(&a_address) > 0).await;
        let synced_through = b.peers.watermark(&a_address);

        // While "B is down", A changes: an older write B already had (so
        // it's before the watermark), a new write, and a delete.
        a_data.seed(DB, &[0, 0, 0], "material", Value::Str("old".into()), 500);
        a_data.seed(
            DB,
            &[0, 0, 2],
            "material",
            Value::Str("two".into()),
            synced_through + 10,
        );
        a_data.seed_removed(DB, &[0, 0, 1], "material", synced_through + 20);

        // B restarts with its data and its saved watermark.
        let b_data = RecordingSink::new();
        b_data.seed(DB, &[0, 0, 1], "material", Value::Str("one".into()), 1_000);
        let b2 = spawn_node_with("node-b", b_data, with_watermarks(watermarks.clone())).await;
        assert_eq!(b2.peers.watermark(&a_address), synced_through);
        a.peers.add(b2.addr.to_string());
        wait_until(|| {
            b2.sink.get(DB, &[0, 0, 2], "material").is_some()
                && b2.sink.is_removed(DB, &[0, 0, 1], "material")
        })
        .await;
        assert_eq!(b2.sink.get(DB, &[0, 0, 0], "material"), None);
        let _ = std::fs::remove_dir_all(watermarks.parent().unwrap());
    }

    #[tokio::test]
    async fn hello_with_the_right_secret_is_accepted_with_the_servers_node_id() {
        let TestNode { addr, peers, .. } = spawn_node("node-a").await;
        let mut stream = ClientStream::connect(addr).await.unwrap();
        assert_eq!(
            hello(&mut stream, CLUSTER_SECRET).await,
            PeerMessage::HelloOk {
                node_id: peers.node_id()
            }
        );
    }

    #[tokio::test]
    async fn a_peer_list_teaches_the_receiver_new_peers() {
        let TestNode { addr, peers, .. } = spawn_node("node-a").await;
        let mut stream = ClientStream::connect(addr).await.unwrap();
        hello(&mut stream, CLUSTER_SECRET).await;
        crate::wire::write_message(
            &mut stream,
            &PeerMessage::PeerList(vec!["127.0.0.1:2".to_string()]),
        )
        .await
        .unwrap();

        wait_until(|| peers.snapshot().iter().any(|(a, _)| a == "127.0.0.1:2")).await;
    }

    #[tokio::test]
    async fn three_nodes_converge_to_a_full_mesh_by_gossip() {
        let a = spawn_node("node-a").await;
        let b = spawn_node("node-b").await;
        let c = spawn_node("node-c").await;
        // A and C only know B; neither is told about the other.
        a.peers.add(b.addr.to_string());
        c.peers.add(b.addr.to_string());

        let (a_addr, b_addr, c_addr) = (a.addr.to_string(), b.addr.to_string(), c.addr.to_string());
        let mut expect_a = vec![(b_addr.clone(), true), (c_addr.clone(), true)];
        let mut expect_c = vec![(a_addr.clone(), true), (b_addr.clone(), true)];
        expect_a.sort();
        expect_c.sort();
        wait_until(|| a.peers.snapshot() == expect_a).await;
        wait_until(|| c.peers.snapshot() == expect_c).await;

        // A's writes now reach C over the link gossip created.
        crate::hub::publish_set(
            &Some(a.hub.clone()),
            DB,
            &[1, 2, 3],
            "material",
            Value::Str("stone".to_string()),
            kblockdblib::CellMeta {
                created_at_ms: 1,
                modified_at_ms: 1,
                version: 0,
            },
        );
        wait_until(|| c.sink.get(DB, &[1, 2, 3], "material").is_some()).await;
    }

    #[tokio::test]
    async fn a_node_handed_its_own_address_drops_it() {
        let a = spawn_node("node-a").await;
        a.peers.add(a.addr.to_string());
        wait_until(|| a.peers.snapshot().is_empty()).await;
        // And it stays dropped.
        assert!(!a.peers.add(a.addr.to_string()));
    }

    #[tokio::test]
    async fn a_second_address_for_an_already_linked_peer_is_dropped() {
        let a = spawn_node("node-a").await;
        let b = spawn_node("node-b").await;
        let by_ip = b.addr.to_string();
        let by_name = format!("localhost:{}", b.addr.port());
        a.peers.add(by_ip.clone());
        wait_until(|| a.peers.snapshot() == vec![(by_ip.clone(), true)]).await;

        a.peers.add(by_name);
        // Give the alias link time to connect and be dropped, then check
        // only the original remains.
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        assert_eq!(a.peers.snapshot(), vec![(by_ip, true)]);
    }

    #[tokio::test]
    async fn a_peer_that_connects_in_is_learned_by_its_dial_back_address() {
        let TestNode { addr, peers, .. } = spawn_node("node-a").await;
        let mut stream = ClientStream::connect(addr).await.unwrap();
        hello(&mut stream, CLUSTER_SECRET).await;

        let expected = format!("127.0.0.1:{UNREACHABLE_PEER_PORT}");
        wait_until(|| peers.snapshot().iter().any(|(a, _)| *a == expected)).await;
    }

    #[tokio::test]
    async fn a_rejected_hello_is_never_learned() {
        let TestNode { addr, peers, .. } = spawn_node("node-a").await;
        let mut stream = ClientStream::connect(addr).await.unwrap();
        hello(&mut stream, "wrong").await;

        // Give the (incorrect, if it happened) learning a moment to land,
        // then assert it didn't.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(peers.snapshot().is_empty());
    }

    #[tokio::test]
    async fn peers_are_symmetric_once_one_side_knows_the_other() {
        let a = spawn_node("node-a").await;
        let b = spawn_node("node-b").await;
        // Only A is told about B -- B must learn A from A's `Hello` and
        // dial back on its own.
        a.peers.add(b.addr.to_string());

        let a_addr = a.addr.to_string();
        wait_until(|| b.peers.snapshot() == vec![(a_addr.clone(), true)]).await;
        wait_until(|| a.peers.snapshot() == vec![(b.addr.to_string(), true)]).await;

        // And B's writes now reach A, over the link B opened itself.
        let replication = Some(b.hub.clone());
        crate::hub::publish_set(
            &replication,
            DB,
            &[1, 2, 3],
            "material",
            Value::Str("stone".to_string()),
            kblockdblib::CellMeta {
                created_at_ms: 1,
                modified_at_ms: 1,
                version: 0,
            },
        );
        wait_until(|| a.sink.get(DB, &[1, 2, 3], "material").is_some()).await;
    }

    #[tokio::test]
    async fn hello_with_the_wrong_secret_is_rejected() {
        let TestNode { addr, .. } = spawn_node("node-a").await;
        let mut stream = ClientStream::connect(addr).await.unwrap();
        assert!(matches!(
            hello(&mut stream, "wrong").await,
            PeerMessage::HelloRejected(_)
        ));
    }

    #[tokio::test]
    async fn hello_with_a_newer_protocol_version_is_rejected() {
        let TestNode { addr, .. } = spawn_node("node-a").await;
        let mut stream = ClientStream::connect(addr).await.unwrap();
        crate::wire::write_message(
            &mut stream,
            &PeerMessage::Hello {
                secret: CLUSTER_SECRET.to_string(),
                server_id: "test-peer".to_string(),
                peer_port: UNREACHABLE_PEER_PORT,
                node_id: TEST_CLIENT_NODE_ID,
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
        let TestNode { addr, sink, .. } = spawn_node("node-a").await;
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
        let TestNode { addr, sink, .. } = spawn_node("node-a").await;
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
        let TestNode { addr, sink, .. } = spawn_node("node-a").await;
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
}
