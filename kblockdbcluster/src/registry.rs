//! Tracks which peers currently have a live inbound connection to this
//! process's `server::serve` listener -- see `server.rs`'s doc comment.
//! Purely a snapshot of who's *currently* connected (a peer is removed
//! the instant its connection ends, for any reason); not a durable
//! membership list or a record of who has ever connected.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

#[derive(Clone, Default)]
pub struct PeerRegistry {
    connected: Arc<Mutex<HashSet<String>>>,
}

impl PeerRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Every peer (identified by its own self-reported `server_id`, from
    /// `Hello`) with a live connection right now, in no particular order.
    /// Two connections that happen to report the same `server_id` are
    /// indistinguishable here -- that's a misconfiguration (two peers
    /// shouldn't share one identity) this registry doesn't try to detect.
    pub fn connected(&self) -> Vec<String> {
        self.connected.lock().unwrap().iter().cloned().collect()
    }

    /// Marks `server_id` connected, returning a guard that removes it
    /// again on drop -- so however `handle_connection` exits (clean
    /// close, a read/write error, a rejected `Hello`), the registry never
    /// keeps stale entries past the connection's actual lifetime. `pub`
    /// (not `pub(crate)`) so an embedder's own tests can simulate a
    /// connected peer directly, same as `kblockdbserver`'s do.
    pub fn track(&self, server_id: String) -> ConnectionGuard {
        self.connected.lock().unwrap().insert(server_id.clone());
        ConnectionGuard {
            registry: self.clone(),
            server_id,
        }
    }
}

pub struct ConnectionGuard {
    registry: PeerRegistry,
    server_id: String,
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.registry
            .connected
            .lock()
            .unwrap()
            .remove(&self.server_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_fresh_registry_is_empty() {
        assert!(PeerRegistry::new().connected().is_empty());
    }

    #[test]
    fn tracking_a_peer_adds_it_until_the_guard_drops() {
        let registry = PeerRegistry::new();
        let guard = registry.track("node-a".to_string());
        assert_eq!(registry.connected(), vec!["node-a".to_string()]);

        drop(guard);
        assert!(registry.connected().is_empty());
    }

    #[test]
    fn tracks_several_peers_independently() {
        let registry = PeerRegistry::new();
        let a = registry.track("node-a".to_string());
        let b = registry.track("node-b".to_string());

        let mut connected = registry.connected();
        connected.sort();
        assert_eq!(connected, vec!["node-a".to_string(), "node-b".to_string()]);

        drop(a);
        assert_eq!(registry.connected(), vec!["node-b".to_string()]);
        drop(b);
        assert!(registry.connected().is_empty());
    }
}
