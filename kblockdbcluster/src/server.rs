//! The accepting side of a peer link -- listens for connections from
//! other servers listed in *their* peer config, authenticates them
//! against a shared `cluster_secret`, and applies every `ChangeEntry`
//! they stream by handing it to a [`ReplicationSink`] the embedder
//! provides. See `wire.rs` for the wire format and `client.rs` for the
//! connecting side.

use crate::peers::PeerSet;
use crate::wire::{ChangeEntry, ChangeOp, IndexOp, PeerMessage};
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
        stamp: kblockdblib::Stamp,
    ) -> impl Future<Output = Result<(), String>> + Send;

    fn apply_remove(
        &self,
        database: String,
        coord: Vec<i32>,
        key: String,
        modified_at_ms: u64,
        stamp: kblockdblib::Stamp,
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

    /// Applies a cluster-wide `CREATE INDEX`/`DROP INDEX`/`REBUILD INDEX`
    /// (see `wire::IndexOpEntry`) against `database`'s own copy of `key`.
    /// The default is a no-op, `Ok(())` -- an embedder that doesn't have a
    /// concept of a secondary index (or doesn't want to replicate this)
    /// simply ignores it; `kblockdbserver`'s `AppState` overrides this to
    /// call `kblockdblib::World::create_index`/`drop_index`/
    /// `rebuild_index`.
    fn apply_index_op(
        &self,
        database: String,
        key: String,
        op: IndexOp,
    ) -> impl Future<Output = Result<(), String>> + Send {
        let _ = (database, key, op);
        async move { Ok(()) }
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
/// a `CatchUpRequest` carrying this process's version vector (see
/// `peers.rs`'s "Catch-up"), then an unbounded stream of `ChangeBatch`
/// frames applied as they arrive -- no responses are ever sent back for
/// those -- with the occasional `Synced` advancing the vector.
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

    // Only exactly this version links: older peers can't stamp writes or
    // exchange version vectors, and a mixed cluster could never agree on
    // what "in sync" means. Checked before the secret so a mismatched peer
    // learns nothing about whether its secret would otherwise have been
    // accepted.
    if protocol_version != crate::wire::PEER_PROTOCOL_VERSION {
        let _ = crate::wire::write_message(
            &mut stream,
            &PeerMessage::HelloRejected(format!(
                "peer protocol version {protocol_version} isn't supported -- this server speaks \
                 version {}; upgrade every node in the cluster together",
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
    } else {
        // Known already, and evidently back up (a restart, say): if our
        // link to it is waiting out a reconnect backoff, retry now, so it
        // catches up straight away.
        peers.wake(&dial_back, node_id);
    }

    // Ask for everything this server doesn't have yet -- see `peers.rs`'s
    // "Catch-up".
    let known = peers.local_vector();
    eprintln!("peer protocol: asking {dial_back} for every change not in {known:?}");
    let request = PeerMessage::CatchUpRequest { known };
    if crate::wire::write_message(&mut stream, &request)
        .await
        .is_err()
    {
        return;
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
            PeerMessage::Synced { confirmed, vector } => {
                peers.vectors().merge(&confirmed);
                peers.vectors().record_peer(node_id, vector);
            }
            PeerMessage::IndexOp(entry) => {
                let (database, key) = (entry.database.clone(), entry.key.clone());
                if let Err(e) = sink.apply_index_op(entry.database, entry.key, entry.op).await {
                    eprintln!(
                        "peer protocol: dropping index op for '{database}' key '{key}': {e}"
                    );
                }
            }
            // Catch-up for whatever `IndexOp`s this link's other side
            // already held before this connection started -- always
            // `Create` (never `Drop`): see `wire::IndexState`'s doc
            // comment on why reconciliation only ever adds.
            PeerMessage::IndexState(entries) => {
                for (database, key) in entries {
                    let (database2, key2) = (database.clone(), key.clone());
                    if let Err(e) = sink.apply_index_op(database, key, IndexOp::Create).await {
                        eprintln!(
                            "peer protocol: dropping index-state entry for '{database2}' key \
                             '{key2}': {e}"
                        );
                    }
                }
            }
            // Only Hello/ChangeBatch/PeerList/Synced/IndexOp/IndexState
            // travel this direction -- anything else is a protocol error,
            // but per this module's "one bad frame doesn't end the
            // connection" policy, just skip it rather than disconnecting.
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
        origin,
        seq,
    } = entry;
    let stamp = kblockdblib::Stamp::new(origin, seq);
    let result = match op {
        ChangeOp::Set(value) => {
            let meta = kblockdblib::CellMeta {
                created_at_ms,
                modified_at_ms,
                version,
            };
            sink.apply_set(database.clone(), coord.clone(), key, value, meta, stamp)
                .await
        }
        ChangeOp::Remove => {
            sink.apply_remove(database.clone(), coord.clone(), key, modified_at_ms, stamp)
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
    use kblockdblib::{Value, VersionVector};
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

    /// Connects a raw client, says `Hello`, and returns the stream plus
    /// the vector in the `CatchUpRequest` the server sends right after
    /// `HelloOk`.
    async fn connect_and_read_catch_up_request(
        addr: std::net::SocketAddr,
    ) -> (ClientStream, VersionVector) {
        let mut stream = ClientStream::connect(addr).await.unwrap();
        assert!(matches!(
            hello(&mut stream, CLUSTER_SECRET).await,
            PeerMessage::HelloOk { .. }
        ));
        match crate::wire::read_message(&mut stream).await.unwrap() {
            Some(PeerMessage::CatchUpRequest { known }) => (stream, known),
            other => panic!("expected CatchUpRequest, got {other:?}"),
        }
    }

    fn vector(entries: &[(u64, u64)]) -> VersionVector {
        entries.iter().copied().collect()
    }

    /// A local write on `node`, as an embedder makes one: stamped with
    /// the node's next seq, stored, then published.
    fn local_write(node: &TestNode, coord: &[i32], value: Option<&str>) {
        let replication = Some(node.hub.clone());
        let stamp = node.hub.sequencer().assign();
        let ms = crate::hub::now_ms();
        match value {
            Some(value) => {
                let value = Value::Str(value.to_string());
                node.sink
                    .seed(DB, coord, "material", value.clone(), ms, stamp);
                let meta = kblockdblib::CellMeta {
                    created_at_ms: ms,
                    modified_at_ms: ms,
                    version: 0,
                };
                crate::hub::publish_set(&replication, DB, coord, "material", value, meta, stamp);
            }
            None => {
                node.sink.seed_removed(DB, coord, "material", ms, stamp);
                crate::hub::publish_remove(&replication, DB, coord, "material", ms, stamp);
            }
        }
    }

    #[tokio::test]
    async fn the_receiver_asks_with_its_own_vector() {
        let TestNode { addr, peers, .. } = spawn_node("node-b").await;
        let (_stream, known) = connect_and_read_catch_up_request(addr).await;
        let me = peers.node_id();
        assert_eq!(
            known,
            vector(&[(me, 0), (kblockdblib::legacy_origin(me), 0)])
        );

        peers.vectors().merge(&vector(&[(0xA, 7)]));
        let (_stream, known) = connect_and_read_catch_up_request(addr).await;
        assert_eq!(known.get(0xA), Some(7));
    }

    #[tokio::test]
    async fn synced_merges_what_it_confirms_but_a_dropped_catch_up_advances_nothing() {
        let TestNode { addr, peers, .. } = spawn_node("node-b").await;
        let (mut stream, known) = connect_and_read_catch_up_request(addr).await;
        assert_eq!(known.get(TEST_CLIENT_NODE_ID), None);
        let synced = PeerMessage::Synced {
            confirmed: vector(&[(TEST_CLIENT_NODE_ID, 5)]),
            vector: vector(&[(TEST_CLIENT_NODE_ID, 9)]),
        };
        crate::wire::write_message(&mut stream, &synced)
            .await
            .unwrap();
        wait_until(|| peers.local_vector().get(TEST_CLIENT_NODE_ID) == Some(5)).await;
        // `vector` is only recorded for display, never merged.
        assert_eq!(
            peers.vectors().peer(TEST_CLIENT_NODE_ID),
            Some(vector(&[(TEST_CLIENT_NODE_ID, 9)]))
        );
        drop(stream);

        // Asked again from 5; drops before confirming anything ...
        let (stream, known) = connect_and_read_catch_up_request(addr).await;
        assert_eq!(known.get(TEST_CLIENT_NODE_ID), Some(5));
        drop(stream);

        // ... so the next request still starts from 5.
        let (_stream, known) = connect_and_read_catch_up_request(addr).await;
        assert_eq!(known.get(TEST_CLIENT_NODE_ID), Some(5));
    }

    /// A restarted peer connecting in wakes this node's own link to it, so
    /// it catches the peer up at once instead of after its backoff.
    #[tokio::test]
    async fn a_peer_connecting_in_wakes_the_link_back_to_it() {
        let a = spawn_node("node-a").await;
        let b_addr = crate::test_support::free_addr();
        // A knows B, which is down: A's link backs off (it's now waiting
        // until ~3.5s), and A writes something B will need.
        a.peers.add(b_addr.to_string());
        local_write(&a, &[7], Some("written-while-b-was-down"));
        tokio::time::sleep(Duration::from_millis(1700)).await;

        // B comes up and dials A (as if A were in its config).
        let listener = tokio::net::TcpListener::bind(b_addr).await.unwrap();
        let b = crate::test_support::spawn_node_on(
            "node-b",
            listener,
            RecordingSink::new(),
            crate::hub::ReplicationHub::new(),
            |p| p,
        );
        b.peers.add(a.addr.to_string());

        let caught_up = tokio::time::timeout(Duration::from_millis(800), async {
            while b.sink.get(DB, &[7], "material").is_none() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        assert!(
            caught_up.is_ok(),
            "B wasn't caught up before A's backoff ran out"
        );
    }

    #[tokio::test]
    async fn an_older_peer_is_rejected() {
        let TestNode { addr, .. } = spawn_node("node-b").await;
        let mut stream = ClientStream::connect(addr).await.unwrap();
        match hello_as_version(&mut stream, CLUSTER_SECRET, 3).await {
            PeerMessage::HelloRejected(reason) => assert!(reason.contains("upgrade"), "{reason}"),
            other => panic!("expected HelloRejected, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn two_nodes_end_up_with_equal_vectors() {
        let a = spawn_node("node-a").await;
        let b = spawn_node("node-b").await;
        for i in 0..3 {
            local_write(&a, &[i], Some("from-a"));
        }
        for i in 0..2 {
            local_write(&b, &[10 + i], Some("from-b"));
        }
        a.peers.add(b.addr.to_string());
        wait_until(|| {
            let (va, vb) = (a.peers.local_vector(), b.peers.local_vector());
            va == vb && va.get(a.peers.node_id()) == Some(3) && va.get(b.peers.node_id()) == Some(2)
        })
        .await;
        for i in 0..3 {
            assert!(b.sink.get(DB, &[i], "material").is_some());
        }
        // Replicated writes keep their origin's stamp.
        assert_eq!(
            b.sink.stamp(DB, &[0], "material"),
            Some(kblockdblib::Stamp::new(a.peers.node_id(), 1))
        );
        for i in 0..2 {
            assert!(a.sink.get(DB, &[10 + i], "material").is_some());
        }
    }

    #[tokio::test]
    async fn a_new_node_gets_a_full_copy_and_a_restarted_one_only_what_it_missed() {
        let dir =
            std::env::temp_dir().join(format!("kblockdbcluster-catch-up-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let with_vectors =
            |path: std::path::PathBuf| move |peers: PeerSet| peers.with_vectors(&path).unwrap();

        // A holds a write of its own, and legacy data from before
        // clustering.
        let a = spawn_node("node-a").await;
        let a_id = a.peers.node_id();
        local_write(&a, &[0, 0, 1], Some("one"));
        a.sink.seed(
            DB,
            &[0, 0, 9],
            "material",
            Value::Str("legacy".into()),
            5,
            kblockdblib::Stamp::NONE,
        );

        // B is new: it gets everything, and saves A's confirmed vector.
        let b = spawn_node_with(
            "node-b",
            RecordingSink::new(),
            crate::hub::ReplicationHub::new(),
            with_vectors(dir.join("b/vector")),
        )
        .await;
        a.peers.add(b.addr.to_string());
        wait_until(|| {
            b.sink.get(DB, &[0, 0, 9], "material").is_some()
                && b.peers.vectors().known().get(a_id) == Some(1)
        })
        .await;
        // "B goes down": its saved vector is what a restart would load.
        std::fs::create_dir_all(dir.join("b2")).unwrap();
        std::fs::copy(dir.join("b/vector"), dir.join("b2/vector")).unwrap();

        // Meanwhile A writes and deletes.
        local_write(&a, &[0, 0, 2], Some("two"));
        local_write(&a, &[0, 0, 1], None);

        // B restarts with its data and its saved vector: it gets only what
        // it missed -- not A's first write or legacy data again.
        let b_data = RecordingSink::new();
        b_data.seed(
            DB,
            &[0, 0, 1],
            "material",
            Value::Str("one".into()),
            1,
            kblockdblib::Stamp::new(a_id, 1),
        );
        let b2 = spawn_node_with(
            "node-b",
            b_data,
            crate::hub::ReplicationHub::new(),
            with_vectors(dir.join("b2/vector")),
        )
        .await;
        assert_eq!(b2.peers.vectors().known().get(a_id), Some(1));
        a.peers.add(b2.addr.to_string());
        wait_until(|| {
            b2.sink.get(DB, &[0, 0, 2], "material").is_some()
                && b2.sink.is_removed(DB, &[0, 0, 1], "material")
                && b2.peers.vectors().known().get(a_id) == Some(3)
        })
        .await;
        assert_eq!(b2.sink.get(DB, &[0, 0, 9], "material"), None);
        let _ = std::fs::remove_dir_all(&dir);
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
            kblockdblib::Stamp::NONE,
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
            kblockdblib::Stamp::NONE,
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
                origin: 0,
                seq: 0,
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
                origin: 0,
                seq: 0,
            }]),
        )
        .await
        .unwrap();

        wait_until(|| sink.is_removed(DB, &[1, 1, 1], "material")).await;
    }

    #[tokio::test]
    async fn an_index_op_is_applied_via_the_sink() {
        let TestNode { addr, sink, .. } = spawn_node("node-a").await;
        let mut stream = ClientStream::connect(addr).await.unwrap();
        hello(&mut stream, CLUSTER_SECRET).await;

        crate::wire::write_message(
            &mut stream,
            &PeerMessage::IndexOp(crate::wire::IndexOpEntry {
                database: DB.to_string(),
                key: "material".to_string(),
                op: IndexOp::Create,
            }),
        )
        .await
        .unwrap();

        wait_until(|| {
            sink.index_ops()
                == vec![(DB.to_string(), "material".to_string(), IndexOp::Create)]
        })
        .await;
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
                    origin: 0,
                    seq: 0,
                },
                ChangeEntry {
                    database: DB.to_string(),
                    coord: vec![1, 0, 0],
                    key: "k".to_string(),
                    op: ChangeOp::Set(Value::I64(2)),
                    created_at_ms: 1,
                    modified_at_ms: 1,
                    version: 0,
                    origin: 0,
                    seq: 0,
                },
            ]),
        )
        .await
        .unwrap();

        assert!(sink.get(DB, &[0, 0, 0], "k").is_none());
        wait_until(|| sink.get(DB, &[1, 0, 0], "k") == Some((Value::I64(2), 1, 1, 0))).await;
    }
}
