//! Every peer this process knows about, and its live link status. A peer
//! is *symmetric*: however it became known -- listed in this server's
//! config, or by connecting in and saying `Hello` -- this server both
//! accepts its changes (`server::serve`) and replicates its own to it (one
//! `client::run` task per known peer, spawned the moment it's added here).
//! So the first time A dials B, B learns A's address and dials back,
//! without B's config ever having to name A.
//!
//! The set only grows for the life of the process: a peer that goes away
//! keeps its entry (its `client::run` task keeps retrying with backoff,
//! and its status reads "not connected") rather than being forgotten.
//! Peers learned at runtime aren't persisted -- after a restart, only
//! configured peers are dialed, and the rest are relearned when they
//! reconnect in.
//!
//! Peers are keyed by address string, exactly as configured or as derived
//! from an inbound connection (`<source IP>:<Hello's peer_port>`). A peer
//! configured by hostname and later seen connecting from its IP shows up
//! as two entries -- configure peers by IP to avoid that.

use crate::client::{self, ConnectionStatus};
use crate::hub::ReplicationHub;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

/// What this process tells every peer about itself in `Hello` -- shared by
/// every `client::run` task, and checked against by `server::serve`.
#[derive(Clone)]
pub struct LocalIdentity {
    pub cluster_secret: String,
    /// Shown in peers' logs; not used to identify peers (addresses are).
    pub server_id: String,
    /// The port this process's own peer listener is bound to, sent in
    /// `Hello` so the other side can dial back.
    pub peer_port: u16,
}

#[derive(Clone)]
pub struct PeerSet {
    known: Arc<Mutex<BTreeMap<String, ConnectionStatus>>>,
    identity: LocalIdentity,
    hub: Arc<ReplicationHub>,
}

impl PeerSet {
    pub fn new(identity: LocalIdentity, hub: Arc<ReplicationHub>) -> Self {
        PeerSet {
            known: Arc::new(Mutex::new(BTreeMap::new())),
            identity,
            hub,
        }
    }

    pub fn identity(&self) -> &LocalIdentity {
        &self.identity
    }

    /// Adds `address` if it isn't already known, spawning the
    /// `client::run` task that replicates to it. Returns whether it was
    /// newly added. Must be called from within a Tokio runtime.
    pub fn add(&self, address: String) -> bool {
        let status = {
            let mut known = self.known.lock().unwrap();
            if known.contains_key(&address) {
                return false;
            }
            let status = ConnectionStatus::new();
            known.insert(address.clone(), status.clone());
            status
        };
        tokio::spawn(client::run(
            address,
            self.identity.clone(),
            self.hub.clone(),
            status,
        ));
        true
    }

    /// Registers `address` with `status` *without* spawning a client --
    /// for an embedder's own tests, to fake a peer in a given state.
    pub fn insert(&self, address: String, status: ConnectionStatus) {
        self.known.lock().unwrap().insert(address, status);
    }

    /// Every known peer and whether this process's link to it is up right
    /// now, sorted by address.
    pub fn snapshot(&self) -> Vec<(String, bool)> {
        self.known
            .lock()
            .unwrap()
            .iter()
            .map(|(address, status)| (address.clone(), status.is_connected()))
            .collect()
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
            },
            ReplicationHub::new(),
        )
    }

    #[test]
    fn a_fresh_set_is_empty() {
        assert!(peer_set().snapshot().is_empty());
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
}
