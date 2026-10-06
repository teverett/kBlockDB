//! Every peer this process knows about, and its live link status. A peer
//! is *symmetric*: however it became known -- listed in this server's
//! config, learned because it connected in and said `Hello`, or learned by
//! gossip -- this server both accepts its changes (`server::serve`) and
//! replicates its own to it (one `client::run` task per known peer,
//! spawned the moment it's added here).
//!
//! **Gossip.** Over each of its links, a process sends a `PeerList` of
//! every peer it's *currently connected to* (see `gossip_list` -- dead
//! addresses aren't spread), right after connecting and again whenever its
//! own set changes. The receiver adds any it doesn't know, which spawns
//! links to them, and so on -- so the cluster converges on a full mesh as
//! long as its peers form one connected graph, however few each server
//! has configured.
//!
//! **Node ids.** Gossip can hand a process its *own* address, or a second
//! address for a peer it's already linked to (a hostname vs. an IP, say).
//! Addresses alone can't tell, so each process picks a random `node_id`
//! at startup and the two sides swap them in `Hello`/`HelloOk`. When a
//! link turns out to reach this process itself, or a node already linked
//! under a different address, `claim` drops that address and ignores it
//! from then on (see `client::connect_and_forward`).
//!
//! **Dead peers.** A peer that goes away keeps its entry for a while --
//! its link retries with backoff and reads "not connected" -- in case it's
//! just restarting. Once a *learned* peer (from an inbound `Hello` or
//! gossip) has been unreachable continuously for longer than the
//! embedder's timeout, `prune` drops it and its link task exits. Configured
//! peers are never pruned: they're the operator's stated intent, and the
//! seeds a node needs to rejoin. A pruned peer that comes back is simply
//! relearned (it connects in, or a peer gossips it once connected).
//! The peer list isn't persisted; after a restart a server dials its
//! configured peers and relearns the rest.
//!
//! **Catch-up.** Every write is stamped with its origin node's id and
//! that node's own sequence number (see `sequence.rs`), and every process
//! keeps a *version vector*: per origin, the highest seq it's guaranteed
//! to have (`local_vector`). Each peer's changes arrive on the connection
//! *it* dials in (`server::serve`). Right after `HelloOk` the accepting
//! side sends `CatchUpRequest{known}` -- its vector. The sending side
//! replies with every change it holds that `known` doesn't cover (from the
//! embedder's `ChangeSource`, as ordinary `ChangeBatch` frames interleaved
//! with live ones), then `Synced{confirmed}` with its own vector as of the
//! catch-up's start; while the link stays up and idle it repeats `Synced`
//! every few seconds, confirming its own latest seq. The receiver merges
//! `confirmed` into its vector (`VectorStore`, persisted), so a link that
//! drops mid-catch-up just asks again from the old vector next time, and
//! a restarted server picks up where it left off. A new server's vector
//! is empty: it gets everything. Every peer is asked, not just one, and
//! last-write-wins makes the overlap harmless. Two servers whose vectors
//! are equal have seen exactly the same writes.

use crate::client::{self, ConnectionStatus};
use crate::hub::ReplicationHub;
use crate::socket::Keepalive;
use crate::source::ChangeSource;
use crate::vector::VectorStore;
use kblockdblib::{legacy_origin, Stamp, VersionVector};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::io;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::watch;

/// What this process tells every peer about itself in `Hello` -- shared by
/// every `client::run` task, and checked against by `server::serve`.
#[derive(Clone)]
pub struct LocalIdentity {
    pub cluster_secret: String,
    /// Shown in peers' logs; not used to identify peers (addresses and
    /// node ids are).
    pub server_id: String,
    /// The port this process's own peer listener is bound to, sent in
    /// `Hello` so the other side can dial back.
    pub peer_port: u16,
    /// This process's own stated host (IP or DNS name, no port) to dial
    /// back on, sent in `Hello` as `advertised_host` -- `None` leaves a
    /// peer to dial back on whatever source IP it observed the
    /// connection arrive from, same as before this field existed. This
    /// crate itself never sets a default here (it has no concept of "this
    /// host's own address" -- that's the embedder's business):
    /// `kblockdbserver` auto-detects its own best-guess outbound IP
    /// unless its `[cluster].advertised_host` config overrides it (see
    /// `docs/clustering.md`'s "Peer addresses" section).
    pub advertised_host: Option<String>,
}

struct Entry {
    status: ConnectionStatus,
    /// From the embedder's config (`add_configured`) -- never pruned.
    configured: bool,
}

#[derive(Default)]
struct Inner {
    known: BTreeMap<String, Entry>,
    /// Addresses found to reach this process itself, or a node already
    /// linked under another address -- never re-added.
    ignored: HashSet<String>,
    /// Which address each remote node id was first reached at.
    claims: HashMap<u64, String>,
}

#[derive(Clone)]
pub struct PeerSet {
    inner: Arc<Mutex<Inner>>,
    identity: LocalIdentity,
    node_id: u64,
    hub: Arc<ReplicationHub>,
    /// TCP keepalive for every link, both the ones this server dials and
    /// the ones it accepts.
    keepalive: Keepalive,
    /// Bumped on every change worth re-gossiping (a peer added, dropped,
    /// or newly connected); each link watches it.
    changes: Arc<watch::Sender<u64>>,
    /// Where a catch-up a peer asks for is read from -- see
    /// `with_change_source`.
    change_source: Option<Arc<dyn ChangeSource>>,
    vectors: Arc<VectorStore>,
}

impl PeerSet {
    pub fn new(identity: LocalIdentity, hub: Arc<ReplicationHub>) -> Self {
        PeerSet {
            inner: Arc::new(Mutex::new(Inner::default())),
            identity,
            node_id: hub.sequencer().node_id(),
            hub,
            keepalive: Keepalive::default(),
            changes: Arc::new(watch::channel(0).0),
            change_source: None,
            vectors: Arc::new(VectorStore::in_memory()),
        }
    }

    /// Lets this process answer peers' `CatchUpRequest`s from `source`.
    /// Without one, requests are ignored (and logged): peers then only
    /// ever get live changes from this process. Call before adding any
    /// peer, like `with_keepalive`.
    pub fn with_change_source(mut self, source: Arc<dyn ChangeSource>) -> Self {
        self.change_source = Some(source);
        self
    }

    pub(crate) fn change_source(&self) -> Option<&Arc<dyn ChangeSource>> {
        self.change_source.as_ref()
    }

    /// Loads this process's version vector from `path` (a missing file
    /// means nothing known yet: everything is caught up) and saves it there
    /// as it advances. Without this it's kept in memory only, so every
    /// restart catches up from scratch.
    pub fn with_vectors(mut self, path: impl AsRef<Path>) -> io::Result<Self> {
        self.vectors = Arc::new(VectorStore::load(path.as_ref())?);
        Ok(self)
    }

    pub fn vectors(&self) -> &VectorStore {
        &self.vectors
    }

    /// Every write this process is guaranteed to have, per origin: what
    /// peers have confirmed sending it, plus its own writes (everything up
    /// to `Sequencer::confirmed_through`) and its own legacy data (see
    /// `kblockdblib::legacy_origin`).
    pub fn local_vector(&self) -> VersionVector {
        let mut vector = self.vectors.known();
        vector.set(self.node_id, self.hub.sequencer().confirmed_through());
        vector.observe(Stamp::new(legacy_origin(self.node_id), 0));
        vector
    }

    /// The node id the link to `address` reached, once it has connected.
    pub fn node_for(&self, address: &str) -> Option<u64> {
        self.inner
            .lock()
            .unwrap()
            .claims
            .iter()
            .find(|(_, claimed)| *claimed == address)
            .map(|(&node_id, _)| node_id)
    }

    /// Records that the link to `address` reached `node_id` -- for an
    /// embedder's own tests, alongside `insert`.
    pub fn insert_claim(&self, address: &str, node_id: u64) {
        self.inner
            .lock()
            .unwrap()
            .claims
            .insert(node_id, address.to_string());
    }

    /// Replaces the default TCP keepalive settings. Call before adding any
    /// peer or serving: links already up keep the settings they had.
    pub fn with_keepalive(mut self, keepalive: Keepalive) -> Self {
        self.keepalive = keepalive;
        self
    }

    pub fn keepalive(&self) -> &Keepalive {
        &self.keepalive
    }

    pub fn identity(&self) -> &LocalIdentity {
        &self.identity
    }

    pub fn node_id(&self) -> u64 {
        self.node_id
    }

    pub fn hub(&self) -> &Arc<ReplicationHub> {
        &self.hub
    }

    /// Adds a *learned* peer (from an inbound `Hello` or gossip) unless
    /// it's already known (or was dropped as a self/alias address),
    /// spawning the `client::run` task that links to it. Returns whether
    /// it was newly added. Must be called from within a Tokio runtime.
    pub fn add(&self, address: String) -> bool {
        self.add_entry(address, false)
    }

    /// Like `add`, for a peer from the embedder's own config -- one that's
    /// never pruned however long it's unreachable (see `prune`).
    pub fn add_configured(&self, address: String) -> bool {
        self.add_entry(address, true)
    }

    fn add_entry(&self, address: String, configured: bool) -> bool {
        let status = {
            let mut inner = self.inner.lock().unwrap();
            if inner.known.contains_key(&address) || inner.ignored.contains(&address) {
                return false;
            }
            let status = ConnectionStatus::new();
            inner.known.insert(
                address.clone(),
                Entry {
                    status: status.clone(),
                    configured,
                },
            );
            status
        };
        self.notify();
        tokio::spawn(client::run(address, self.clone(), status));
        true
    }

    /// Registers a learned peer at `address` with `status` *without*
    /// spawning a client -- for an embedder's own tests, to fake a peer in
    /// a given state.
    pub fn insert(&self, address: String, status: ConnectionStatus) {
        self.inner.lock().unwrap().known.insert(
            address,
            Entry {
                status,
                configured: false,
            },
        );
    }

    /// Cuts short the reconnect backoff of the link to a peer that's just
    /// shown it's up -- by connecting in from `address` as node `node_id`
    /// -- so it reconnects (and catches the peer up) now instead of
    /// whenever its backoff would have retried. Wakes the link known by
    /// that address and, if different, the one that reached that node id
    /// before (a peer configured by hostname, say).
    pub fn wake(&self, address: &str, node_id: u64) {
        let inner = self.inner.lock().unwrap();
        let claimed = inner.claims.get(&node_id).map(String::as_str);
        for address in std::iter::once(address).chain(claimed) {
            if let Some(entry) = inner.known.get(address) {
                if !entry.status.is_connected() {
                    entry.status.wake();
                }
            }
        }
    }

    /// Whether `status` is still the live entry for `address` -- false
    /// once it's been pruned or dropped, even if the same address has
    /// since been re-added (with a fresh status). A `client::run` task
    /// exits as soon as this goes false.
    pub(crate) fn is_current(&self, address: &str, status: &ConnectionStatus) -> bool {
        self.inner
            .lock()
            .unwrap()
            .known
            .get(address)
            .is_some_and(|entry| entry.status.same(status))
    }

    /// Drops every learned (not configured) peer whose link has been down
    /// continuously for at least `timeout`, returning their addresses.
    /// Its node-id claim goes too, so if that node comes back -- at the
    /// same address or another -- it's accepted as new.
    pub fn prune(&self, timeout: Duration) -> Vec<String> {
        let mut inner = self.inner.lock().unwrap();
        let dead: Vec<String> = inner
            .known
            .iter()
            .filter(|(_, entry)| {
                !entry.configured && entry.status.down_for().is_some_and(|down| down >= timeout)
            })
            .map(|(address, _)| address.clone())
            .collect();
        for address in &dead {
            inner.known.remove(address);
        }
        inner.claims.retain(|_, claimed| !dead.contains(claimed));
        drop(inner);
        if !dead.is_empty() {
            self.notify();
        }
        dead
    }

    /// Runs `prune(timeout)` periodically, forever -- spawn it once at
    /// startup. Checks a few times per `timeout`, so a dead peer is
    /// dropped within roughly 1.25x `timeout` of going down.
    pub async fn prune_forever(self, timeout: Duration) {
        let interval = (timeout / 4).max(Duration::from_millis(50));
        loop {
            tokio::time::sleep(interval).await;
            for address in self.prune(timeout) {
                eprintln!("peer protocol: dropped {address} (unreachable for over {timeout:?})");
            }
        }
    }

    /// Every known peer and whether this process's link to it is up right
    /// now, sorted by address.
    pub fn snapshot(&self) -> Vec<(String, bool)> {
        self.inner
            .lock()
            .unwrap()
            .known
            .iter()
            .map(|(address, entry)| (address.clone(), entry.status.is_connected()))
            .collect()
    }

    /// What to gossip over the link to `destination`: every peer currently
    /// connected, other than `destination` itself.
    pub(crate) fn gossip_list(&self, destination: &str) -> Vec<String> {
        self.inner
            .lock()
            .unwrap()
            .known
            .iter()
            .filter(|(address, entry)| *address != destination && entry.status.is_connected())
            .map(|(address, _)| address.clone())
            .collect()
    }

    /// Records that the link to `address` reached the node `node_id`.
    /// Returns `false` -- having dropped `address` and added it to the
    /// ignore list -- if that's this process itself, or a node already
    /// reached at a different address; the caller's link should then stop
    /// for good.
    pub(crate) fn claim(&self, address: &str, node_id: u64) -> bool {
        let mut inner = self.inner.lock().unwrap();
        let duplicate = node_id == self.node_id
            || inner
                .claims
                .get(&node_id)
                .is_some_and(|claimed| claimed != address);
        if duplicate {
            inner.known.remove(address);
            inner.ignored.insert(address.to_string());
            drop(inner);
            self.notify();
            return false;
        }
        inner.claims.insert(node_id, address.to_string());
        true
    }

    pub(crate) fn subscribe_changes(&self) -> watch::Receiver<u64> {
        self.changes.subscribe()
    }

    pub(crate) fn notify(&self) {
        self.changes.send_modify(|version| *version += 1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peer_set() -> PeerSet {
        PeerSet::new(
            LocalIdentity {
                cluster_secret: "s".to_string(),
                server_id: "node-a".to_string(),
                peer_port: 1,
                advertised_host: None,
            },
            ReplicationHub::new(),
        )
    }

    #[test]
    fn a_fresh_set_is_empty() {
        assert!(peer_set().snapshot().is_empty());
    }

    #[test]
    fn the_node_id_comes_from_the_hubs_sequencer() {
        let hub = ReplicationHub::new();
        let peers = PeerSet::new(
            LocalIdentity {
                cluster_secret: "s".to_string(),
                server_id: "node-a".to_string(),
                peer_port: 1,
                advertised_host: None,
            },
            hub.clone(),
        );
        assert_eq!(peers.node_id(), hub.sequencer().node_id());
    }

    #[test]
    fn the_local_vector_has_own_writes_own_legacy_data_and_what_peers_confirmed() {
        let peers = peer_set();
        let me = peers.node_id();
        let expected = |own: u64, others: &[(u64, u64)]| -> VersionVector {
            let mut v: VersionVector = others.iter().copied().collect();
            v.set(me, own);
            v.set(legacy_origin(me), 0);
            v
        };
        assert_eq!(peers.local_vector(), expected(0, &[]));

        let seq = peers.hub().sequencer().assign().seq;
        peers.hub().sequencer().published(seq);
        peers.vectors().merge(&[(0xB, 7)].into_iter().collect());
        assert_eq!(peers.local_vector(), expected(1, &[(0xB, 7)]));
    }

    #[test]
    fn node_for_finds_the_claimed_address() {
        let peers = peer_set();
        assert_eq!(peers.node_for("10.0.0.2:8082"), None);
        peers.insert_claim("10.0.0.2:8082", 0xB);
        assert_eq!(peers.node_for("10.0.0.2:8082"), Some(0xB));
    }

    #[test]
    fn keepalive_defaults_and_can_be_overridden() {
        assert_eq!(*peer_set().keepalive(), Keepalive::default());
        let custom = Keepalive {
            idle: Duration::from_secs(5),
            interval: Duration::from_secs(1),
            retries: 9,
        };
        let peers = peer_set().with_keepalive(custom);
        assert_eq!(*peers.keepalive(), custom);
        // Shared by every clone -- each link task holds one.
        assert_eq!(*peers.clone().keepalive(), custom);
    }

    #[test]
    fn node_ids_differ_between_sets() {
        assert_ne!(peer_set().node_id(), peer_set().node_id());
    }

    #[tokio::test]
    async fn adding_a_peer_twice_only_adds_it_once() {
        let peers = peer_set();
        // Nothing listens on port 1, so the spawned client just retries.
        assert!(peers.add("127.0.0.1:1".to_string()));
        assert!(!peers.add("127.0.0.1:1".to_string()));
        assert_eq!(peers.snapshot(), vec![("127.0.0.1:1".to_string(), false)]);
    }

    #[test]
    fn snapshot_reports_each_peers_status_sorted_by_address() {
        let peers = peer_set();
        peers.insert("10.0.0.3:8082".to_string(), ConnectionStatus::new());
        peers.insert("10.0.0.2:8082".to_string(), ConnectionStatus::connected());
        assert_eq!(
            peers.snapshot(),
            vec![
                ("10.0.0.2:8082".to_string(), true),
                ("10.0.0.3:8082".to_string(), false),
            ]
        );
    }

    #[test]
    fn prune_drops_learned_peers_down_longer_than_the_timeout() {
        let peers = peer_set();
        peers.insert("10.0.0.2:8082".to_string(), ConnectionStatus::new());
        peers.insert("10.0.0.3:8082".to_string(), ConnectionStatus::connected());
        std::thread::sleep(Duration::from_millis(30));

        assert_eq!(
            peers.prune(Duration::from_millis(20)),
            vec!["10.0.0.2:8082".to_string()]
        );
        assert_eq!(peers.snapshot(), vec![("10.0.0.3:8082".to_string(), true)]);
    }

    #[test]
    fn prune_keeps_a_peer_that_has_not_been_down_long_enough() {
        let peers = peer_set();
        peers.insert("10.0.0.2:8082".to_string(), ConnectionStatus::new());
        assert!(peers.prune(Duration::from_secs(60)).is_empty());
        assert_eq!(peers.snapshot().len(), 1);
    }

    #[tokio::test]
    async fn prune_never_drops_a_configured_peer() {
        let peers = peer_set();
        // Nothing listens on port 1, so it stays down.
        peers.add_configured("127.0.0.1:1".to_string());
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(peers.prune(Duration::from_millis(1)).is_empty());
        assert_eq!(peers.snapshot(), vec![("127.0.0.1:1".to_string(), false)]);
    }

    #[tokio::test]
    async fn prune_forever_drops_a_learned_peer_that_never_comes_up() {
        let peers = peer_set();
        // Nothing listens on port 1.
        peers.add("127.0.0.1:1".to_string());
        peers.add_configured("127.0.0.1:2".to_string());
        tokio::spawn(peers.clone().prune_forever(Duration::from_millis(100)));

        crate::test_support::wait_until(|| {
            peers.snapshot() == vec![("127.0.0.1:2".to_string(), false)]
        })
        .await;
    }

    #[test]
    fn a_pruned_peers_node_id_can_be_claimed_again() {
        let peers = peer_set();
        peers.insert("10.0.0.2:8082".to_string(), ConnectionStatus::new());
        assert!(peers.claim("10.0.0.2:8082", 42));
        peers.prune(Duration::ZERO);

        // The same node, back at a different address, isn't an alias.
        peers.insert("10.0.0.9:8082".to_string(), ConnectionStatus::new());
        assert!(peers.claim("10.0.0.9:8082", 42));
    }

    #[tokio::test]
    async fn a_pruned_address_can_be_relearned() {
        let peers = peer_set();
        peers.insert("127.0.0.1:1".to_string(), ConnectionStatus::new());
        peers.prune(Duration::ZERO);
        assert!(peers.add("127.0.0.1:1".to_string()));
    }

    #[test]
    fn gossip_list_is_connected_peers_other_than_the_destination() {
        let peers = peer_set();
        peers.insert("10.0.0.2:8082".to_string(), ConnectionStatus::connected());
        peers.insert("10.0.0.3:8082".to_string(), ConnectionStatus::connected());
        peers.insert("10.0.0.4:8082".to_string(), ConnectionStatus::new());
        assert_eq!(
            peers.gossip_list("10.0.0.2:8082"),
            vec!["10.0.0.3:8082".to_string()]
        );
    }

    #[tokio::test]
    async fn claiming_this_processs_own_node_id_drops_and_ignores_the_address() {
        let peers = peer_set();
        peers.insert("10.0.0.9:8082".to_string(), ConnectionStatus::new());

        assert!(!peers.claim("10.0.0.9:8082", peers.node_id()));
        assert!(peers.snapshot().is_empty());
        // And it's never re-added, e.g. by gossip.
        assert!(!peers.add("10.0.0.9:8082".to_string()));
    }

    #[test]
    fn a_second_address_for_an_already_claimed_node_is_dropped() {
        let peers = peer_set();
        peers.insert("10.0.0.2:8082".to_string(), ConnectionStatus::new());
        peers.insert("node-b:8082".to_string(), ConnectionStatus::new());

        assert!(peers.claim("10.0.0.2:8082", 42));
        assert!(!peers.claim("node-b:8082", 42));
        assert_eq!(peers.snapshot(), vec![("10.0.0.2:8082".to_string(), false)]);
        // Re-claiming by the original address (a reconnect) is fine.
        assert!(peers.claim("10.0.0.2:8082", 42));
    }
}
