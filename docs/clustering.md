# Clustering

A `kblockdbserver` instance can list other instances as peers and
replicate its own local writes to each of them, so a small cluster of
servers converges on the same data over time. This is **opt-in and off by
default** -- nothing in this page applies unless the config file's
`[cluster]` table sets `cluster_secret`.

## Config

```toml
[cluster]
# Enables clustering. Required (and must be non-empty) if [[peers]] is
# non-empty; a server with cluster_secret set but no peers still accepts
# incoming peer connections, it just has nothing to connect out to.
cluster_secret = "a-shared-secret-only-this-clusters-nodes-know"

# The port the peer protocol listens on (default 8082 if cluster_secret
# is set but this isn't). Distinct from http_port/binary_port.
peer_port = 8082

# How long a peer learned at runtime (it connected in, or was gossiped)
# may stay unreachable before it's dropped from the peer set. Default
# 300 (five minutes); 0 never drops anyone. Configured [[peers]] are never
# dropped regardless.
dead_peer_timeout_secs = 300

# TCP keepalive on every peer link, so a peer that vanishes without
# closing its connection is still noticed: probe a link idle for
# keepalive_idle_secs, every keepalive_interval_secs, and drop it after
# keepalive_retries unanswered probes. All optional (defaults below);
# each must be greater than 0.
keepalive_idle_secs = 30
keepalive_interval_secs = 10
keepalive_retries = 3

# Other servers to replicate with -- "address" is that peer's own
# peer_port, not its HTTP or binary-protocol port. One reachable peer is
# enough: the rest of the cluster is discovered by gossip (see below).
[[peers]]
address = "10.0.0.2:8082"

[[peers]]
address = "10.0.0.3:8082"
```

`--peer-port` is the matching CLI flag, same override order as every
other port (`--peer-port` > config file > default).

### Peers are symmetric

A peer has no direction. However a server learns about a peer -- from its
own `[[peers]]`, or because that peer connected in -- it both accepts the
peer's changes *and* replicates its own changes to it. `Hello` carries the
connecting server's `peer_port`, so the accepting side dials back to
`<source IP>:<peer_port>` the first time it sees a new peer. So for any
pair of servers, only *one* side has to list the other: if A's config names
B, B learns A the moment A connects and starts sending to A as well.

### Gossip

Peers share their peer lists. Over each of its links, a server sends a
`PeerList` of every peer it's *currently connected to* -- right after the
link comes up, and again whenever its own peer set changes (a peer added,
dropped, or newly connected). The receiver adds any address it doesn't
know, which starts a link to it, and so on. So the cluster converges on a
**full mesh** as long as the configured peers form one connected graph:
give every node a single seed node to start from, and they'll all find
each other. Only connected peers are gossiped, so an address nobody can
reach isn't spread around.

Each process picks a random **node id** at startup and the two sides of
every link swap them in `Hello`/`HelloOk`. That catches the two ways
gossip can hand a server a bad address: its *own* address (it dials
itself) or a *second* address for a peer it's already linked to (say a
hostname for a peer it learned by IP). Either way the link sees a node id
it already has, drops that address, and ignores it from then on -- so
there's no self-link and no duplicate link, and the Cluster view shows
each peer once, under the address it was first reached at.

### Dead peers

A peer that goes away stays known for a while -- shown as not connected,
its link retrying with backoff -- so a restart or a short network blip
doesn't lose it. But a peer *learned* at runtime (it connected in, or
arrived by gossip) that stays unreachable for `dead_peer_timeout_secs`
straight (default five minutes) is dropped: its link task stops, it
disappears from `/rest/cluster` and the Cluster tab, and it's no longer
gossiped. The clock is "continuously down": any successful reconnect
resets it. It starts as soon as the link notices the peer is gone -- right
away when the peer's process exits or its socket is closed or reset. A
host that vanishes *silently* (power loss, a network partition that drops
packets) sends nothing, so it's caught by TCP keepalive instead: both
ends of every peer link enable it, by default probing a link idle for 30s
every 10s and dropping it after 3 unanswered probes -- so such a peer
shows as not connected about a minute after it disappears. Tune this with
the `keepalive_*` settings in `[cluster]` (see "Config", and
`kblockdbcluster/src/socket.rs`; on platforms that don't support setting
the probe interval and count, the OS defaults apply and it takes longer).

Peers listed in this server's own `[[peers]]` are **never** dropped --
the operator asked for them explicitly, so they're retried forever.
A dropped peer isn't blacklisted either: if it comes back and connects
in, or another node gossips it again, it's simply learned afresh.

Nothing is written back to the config -- after a restart a server dials
its configured peers and relearns the rest by gossip.

## How it works

Every local write -- a REST `PUT`/`DELETE` on a cell or region, the
binary protocol's equivalent requests, or the query language's
`SET`/`UPDATE`/`DELETE` -- publishes a `ChangeEntry` (database, coordinate,
key, the new value or a removal, and the write's own `created_at_ms`/
`modified_at_ms`/`version`) to an in-process hub. One long-running task
per known peer drains that hub and streams the entries over its own
TCP connection to that peer, authenticated once per connection with
`Hello`/`cluster_secret`. The accepting side applies each entry directly
against its matching database (auto-creating it, with its own default
shape, the first time it sees a database name it doesn't have yet).

A write is **never relayed onward** by the node that receives it -- only
locally-originated writes are published to the hub. Gossip makes the
cluster a full mesh, so every node has a direct link to every other and
nothing needs multi-hop forwarding. The flip side: a write made while the
mesh is still forming, before a node has its direct link to some peer,
never reaches that peer (same live-forward-only rule as a reconnect, see
"Limitations").

Two endpoints report cluster membership, both live:

- `GET /rest/health`'s `"peers": [...]` lists the address of every known
  peer this server's link to is *currently up*. A peer that's down or
  still being retried isn't listed.
- `GET /rest/cluster` lists every known peer, connected or not:
  `{"host": <address>, "connected": <bool>}`. The data browser's
  **Cluster** tab renders this.

Both read `kblockdbcluster::peers::PeerSet`, where each peer's
`connected` flag is flipped by its `client::run` task as the link comes
up and drops.

## Crate split

The protocol, the hub, and both sides of a peer link live in their own
crate, `kblockdbcluster`, not in `kblockdbserver` itself. That crate is
storage-agnostic -- it knows nothing about `World`/databases/accounts, only
"apply this set/remove, report success or a message" (its
`server::ReplicationSink` trait). `kblockdbserver/src/cluster.rs`
implements that trait for its own `AppState` (the one place auto-create-
on-unseen-database and similar server-specific behavior lives), and
`main.rs` wires `kblockdbcluster::server::serve`/`client::run` into this
server's config and startup. The dependency only ever goes one way --
`kblockdbserver` depends on `kblockdbcluster`, never the reverse -- so
`kblockdbcluster`'s own tests exercise the protocol against a trivial
in-memory sink rather than any real storage.

## Conflict resolution

If two nodes each write the same cell/key without coordinating, the
receiving side applies **last-write-wins by `modified_at_ms`**: an
incoming write/removal is only applied if its timestamp is strictly newer
than whatever that cell/key currently holds (locally or from an earlier
replicated write); otherwise it's silently discarded. A tie (equal
millisecond timestamps) keeps whatever is already there -- an accepted
imprecision, not a bug, given millisecond resolution. This is the same
mechanism `SELECT`'s `created`/`updated`/`version` keywords expose (see
[query-language.md](query-language.md)), applied automatically rather
than queried.

## Limitations

This is a v1, intentionally minimal design:

- **No backfill.** A peer only receives changes made *after* its
  connection is established. Nothing missed while disconnected, or made
  before the peer link ever existed, is replayed. Bootstrap a new node's
  starting data by other means (copying the data directory, say) before
  pointing it at peers.
- **No durability guarantee for the replication stream itself.** Entries
  are held in a bounded in-process buffer; a peer connection that falls
  far enough behind (or is down for a while) can simply miss entries
  rather than catching up on them. The *local* write itself is always
  durable (written to disk before the response is sent) -- only its
  propagation to a lagging/disconnected peer isn't guaranteed.
- **No quorum or strong consistency.** This is eventually-consistent,
  best-effort replication, not a consensus protocol -- there's no
  guarantee all nodes agree at any given instant, only that they tend to
  converge.
- **Cleartext, shared-secret auth.** The peer link has no transport
  encryption, consistent with the REST and binary protocols both being
  cleartext + Basic Auth today. Run it on a trusted/firewalled network.
  The secret comparison is a plain (not constant-time) equality check --
  acceptable for an operator-chosen, cluster-wide value on a link that
  shouldn't be reachable by an untrusted party in the first place.
- **Matching shapes expected.** Auto-creating a database on first
  incoming write uses the receiving server's own default shape
  (`[worldparameters]`); if peers are configured with different defaults,
  a database auto-created this way can end up differently shaped on
  different nodes. Create databases with an explicit, matching shape up
  front if your peers' defaults differ.
- **Full mesh, small clusters.** Every node links to every other, so a
  cluster of N nodes holds N*(N-1) peer connections and every write is
  sent N-1 times by its origin. Fine for a handful of nodes, not for
  hundreds.
- **Gossiped addresses are as the gossiper sees them.** A peer learned by
  connecting in is recorded as `<source IP>:<peer_port>` -- that's the
  address passed on by gossip. Across NAT or between networks where
  nodes see each other at different addresses, a gossiped address may be
  unreachable from the receiver; it then just shows "not connected" and
  keeps retrying. Configure peers with addresses every node can reach.
- **Configured peers are never forgotten.** Learned peers are dropped
  after `dead_peer_timeout_secs` unreachable (see "Dead peers"), but a
  permanently-gone node that's listed in some server's `[[peers]]` stays
  in *that* server's peer set, retrying with backoff, until it's removed
  from the config and the server restarted. Since only connected peers
  are gossiped, it doesn't spread back to anyone else.
