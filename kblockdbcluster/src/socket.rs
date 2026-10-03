//! Socket options shared by both ends of a peer link.
//!
//! TCP keepalive is what notices a peer that vanished *silently* -- power
//! loss, a partition that drops packets -- on a link with nothing to send.
//! Such a peer never closes its socket, so without keepalive the link
//! stays "connected" until this side's next write times out, which on a
//! quiet cluster may be never (and then `PeerSet::prune` never sees it as
//! down either). With [`Keepalive`]'s defaults an idle link is probed
//! after 30s, and declared dead after 3 more unanswered probes 10s apart
//! -- about a minute in all. An embedder can change them with
//! `PeerSet::with_keepalive`.

use socket2::{SockRef, TcpKeepalive};
use std::time::Duration;
use tokio::net::TcpStream;

/// `Keepalive::idle`'s default.
pub const DEFAULT_KEEPALIVE_IDLE: Duration = Duration::from_secs(30);
/// `Keepalive::interval`'s default.
pub const DEFAULT_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);
/// `Keepalive::retries`' default.
pub const DEFAULT_KEEPALIVE_RETRIES: u32 = 3;

/// TCP keepalive settings for every peer link, both ends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Keepalive {
    /// How long a link may be idle before the first probe.
    pub idle: Duration,
    /// The gap between unanswered probes.
    pub interval: Duration,
    /// Unanswered probes before the OS drops the connection.
    pub retries: u32,
}

impl Default for Keepalive {
    fn default() -> Self {
        Keepalive {
            idle: DEFAULT_KEEPALIVE_IDLE,
            interval: DEFAULT_KEEPALIVE_INTERVAL,
            retries: DEFAULT_KEEPALIVE_RETRIES,
        }
    }
}

/// Applies `TCP_NODELAY` and `settings` to a freshly connected or
/// accepted peer stream. Failures are logged, not returned: either option
/// is an improvement, never a requirement for the link to work.
pub(crate) fn tune(stream: &TcpStream, settings: &Keepalive) {
    let _ = stream.set_nodelay(true);
    if let Err(e) = SockRef::from(stream).set_tcp_keepalive(&to_socket2(settings)) {
        eprintln!("peer protocol: couldn't enable TCP keepalive: {e}");
    }
}

fn to_socket2(settings: &Keepalive) -> TcpKeepalive {
    let keepalive = TcpKeepalive::new().with_time(settings.idle);
    // Platforms without these fall back to the OS defaults (often ~2h idle
    // plus several minutes of probes) -- still eventually detected.
    #[cfg(any(
        target_os = "linux",
        target_os = "android",
        target_os = "macos",
        target_os = "ios",
        target_os = "freebsd",
        target_os = "netbsd",
        target_os = "windows",
    ))]
    let keepalive = keepalive
        .with_interval(settings.interval)
        .with_retries(settings.retries);
    keepalive
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    async fn connected_pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (client, accepted) = tokio::join!(TcpStream::connect(addr), listener.accept());
        (client.unwrap(), accepted.unwrap().0)
    }

    async fn assert_tune_applies(settings: Keepalive) {
        let (client, accepted) = connected_pair().await;
        for stream in [&client, &accepted] {
            tune(stream, &settings);
            assert!(stream.nodelay().unwrap());
            let socket = SockRef::from(stream);
            assert!(socket.keepalive().unwrap());
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            {
                assert_eq!(socket.tcp_keepalive_time().unwrap(), settings.idle);
                assert_eq!(socket.tcp_keepalive_interval().unwrap(), settings.interval);
                assert_eq!(socket.tcp_keepalive_retries().unwrap(), settings.retries);
            }
        }
    }

    #[tokio::test]
    async fn tune_enables_nodelay_and_default_keepalive() {
        assert_tune_applies(Keepalive::default()).await;
    }

    #[tokio::test]
    async fn tune_applies_custom_keepalive_settings() {
        assert_tune_applies(Keepalive {
            idle: Duration::from_secs(7),
            interval: Duration::from_secs(2),
            retries: 5,
        })
        .await;
    }
}
