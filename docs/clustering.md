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

# How long a deleted cell's tombstone is kept (default 604800, a week;
# must be > 0). A node offline longer than this can't safely catch up --
# see "Deletes and tombstones".
tombstone_retention_secs = 604800

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

### Catch-up

A server that's new, restarting, or whose link to one peer dropped for a
while gets what it missed when the link comes back. That's tracked with
**sequence numbers**, not clocks:

- **Node id.** Each server has a random id, made once and kept in
  `<data_dir>/.cluster/node_id`.
- **Sequence numbers.** Every write a server makes -- set or delete -- is
  numbered 1, 2, 3, ... by that server, and carries `(node id, seq)` (its
  *stamp*) wherever it's stored or sent. The counter is saved in
  `.cluster/sequence` in blocks of 1000, so a restart never reuses a
  number (it may skip up to 1000, which is harmless).
- **Version vector.** Every server keeps, per origin node, the highest
  seq it's *guaranteed* to have -- e.g. `{a1b2...: 1042, c3d4...: 977}` --
  saved in `.cluster/vector`. **Two servers with equal vectors have seen
  exactly the same writes.** A lower entry means a server is missing that
  origin's writes after it.

Each peer's changes arrive on the connection *that peer* dials in, so
the catch-up is requested there:

1. Right after `HelloOk`, the receiving server sends
   `CatchUpRequest{known}`: its vector.
2. The sending server replies with every change it holds -- every
   database, whoever originally made each write, deletes included --
   whose stamp `known` doesn't cover. These go out as ordinary
   `ChangeBatch` frames, interleaved with live ones and applied with the
   same last-write-wins rule, so the overlap is harmless.
3. When it's done, the sender sends `Synced`, confirming its own vector as
   of when the catch-up started; the receiver merges that into its
   vector. While the link stays up and caught up, the sender confirms
   its own latest seq every few seconds.

A link that's down retries with backoff (0.5s, doubling up to 30s). When a
peer comes back -- a restart, say -- and connects in, the server wakes its
own link back to that peer, which reconnects at once instead of waiting
out the backoff; so a restarted server is caught up within moments, not
up to 30 seconds later.

A vector entry only ever advances on such a confirmation, and a sender
only confirms a seq once every write up to it has actually been sent
(writes are numbered when stored and published just after, so they can
go out slightly out of order). A link that drops mid-catch-up therefore
just asks again from the old vector. A new server's vector is empty, so
it gets a full copy: point it at one peer and it fills itself from every
peer it finds -- no copying of data directories.

Every peer is asked, not just one. That's simplest and most robust (a
peer that also missed something doesn't leave a gap), but it costs
bandwidth: a new node joining an N-node cluster receives N-1 full copies,
and each existing node receives a full copy of what the new node then
holds. Fine for the small clusters this is meant for.

**Data from before clustering** (or before this version) has no stamp. Each
server's unstamped data is tracked as its own pseudo-origin -- shown as
`<node id>-legacy` -- so it's sent to each peer once, even when different
servers hold different unstamped data, as in a cluster being upgraded.

A sender whose link falls behind its in-memory buffer (more than 4096
unsent writes), or whose write was stored but failed before being sent,
doesn't lose those writes either: it catches its peer up again from the
last vector it confirmed.

### Deletes and tombstones

A delete has to be replicated too, including to a server that was offline
when it happened -- so with clustering on, deleting a value leaves a
**tombstone** recording when. Tombstones are never visible to reads,
queries or the data browser. They let last-write-wins refuse an older
write that arrives after the delete, and let a catch-up include deletes.
Only a cell that actually held a value gets one, so deleting a large,
mostly empty region doesn't fill the disk with them.

Tombstones are kept for `tombstone_retention_secs` (default a week), then
removed the next time their chunk is written. That makes the retention
the longest a server can be offline and still catch up correctly: after
that, the tombstones for deletes it missed may be gone, so the data they
deleted would stay on that server. The server warns at startup if its
vector was last updated longer ago than the retention. In that case, stop
it, delete its data directory (including `.cluster/`), and start it again
to take a fresh copy.

Without clustering, no tombstones are kept and a delete just erases, as
before.

## How it works

Every local write -- a REST `PUT`/`DELETE` on a cell or region, the
binary protocol's equivalent requests, or the query language's
`SET`/`UPDATE`/`DELETE` -- publishes a `ChangeEntry` (database, coordinate,
key, the new value or a removal, the write's own `created_at_ms`/
`modified_at_ms`/`version`, and its stamp) to an in-process hub. One long-running task
per known peer drains that hub and streams the entries over its own
TCP connection to that peer, authenticated once per connection with
`Hello`/`cluster_secret`. The accepting side applies each entry directly
against its matching database (auto-creating it, with its own default
shape, the first time it sees a database name it doesn't have yet).

A write is **never relayed onward** live by the node that receives it --
only locally-originated writes are published to the hub. Gossip makes the
cluster a full mesh, so every node has a direct link to every other and
nothing needs multi-hop forwarding. A write made while the mesh is still
forming, before a node has its direct link to some peer, reaches it when
that link comes up, through catch-up (which does include other nodes'
writes the sender holds).

Two endpoints report cluster membership and sync state, both live:

- `GET /rest/health`'s `"peers": [...]` lists the address of every known
  peer this server's link to is *currently up*. A peer that's down or
  still being retried isn't listed. `"node_id"` is this server's node id
  and `"vector"` its version vector (see "Catch-up"), keyed by node id
  (hex). Compare two servers' `vector`s: equal means in sync.
- `GET /rest/cluster` lists every known peer, connected or not:
  `{"host", "connected", "node_id", "vector", "seq", "sync", "behind_by",
  "ahead_by"}`. `vector` is the peer's own vector as it last reported it
  (every few seconds while its link to this server is up), and `seq` the
  peer's own sequence number -- the latest of its own writes, i.e. its
  own entry in that vector (before it has reported, the latest of its
  writes this server has; `null` if neither is known). `last_synced_ms`
  is when the peer last confirmed it had sent this server everything (its
  last `Synced`, every 5s while its link is up), by this server's clock,
  or `null` if it hasn't since this server started. The response's
  top-level `seq` is this server's own, and `now_ms` this server's
  current time (what `last_synced_ms` is measured against). `sync`
  compares it with this server's: `in_sync`, `behind` (the peer is
  missing writes this server has -- it's catching up), `ahead` (this
  server is missing writes the peer has), `diverged` (both), or `unknown`
  (no vector reported yet). `behind_by`/`ahead_by` are roughly how many
  writes -- approximate, since a restart can skip sequence numbers. A
  peer's reported vector trails by up to ~10 seconds (a confirmation
  every 5s each way), so right after a write, or under steady writes,
  peers show `behind`/`ahead` briefly before settling on `in_sync`. The
  data browser's **Cluster** tab renders this, with **Seq**, **Sync** and
  **Last synced** columns, refreshing every 5 seconds while it's open
  (and paused while the browser tab is hidden).

Both read `kblockdbcluster::peers::PeerSet`, where each peer's
`connected` flag is flipped by its `client::run` task as the link comes
up and drops.

## Crate split

The protocol, the hub, and both sides of a peer link live in their own
crate, `kblockdbcluster`, not in `kblockdbserver` itself. That crate is
storage-agnostic -- it knows nothing about `World`/databases/accounts, only
"apply this set/remove, report success or a message" (its
`server::ReplicationSink` trait) and "list every change since T" (its
`source::ChangeSource` trait). `kblockdbserver/src/cluster.rs`
implements both for its own `AppState` (the one place auto-create-
on-unseen-database and similar server-specific behavior lives), and
`main.rs` wires `kblockdbcluster::server::serve`/`client::run` into this
server's config and startup. The dependency only ever goes one way --
`kblockdbserver` depends on `kblockdbcluster`, never the reverse -- so
`kblockdbcluster`'s own tests exercise the protocol against a trivial
in-memory sink rather than any real storage.

## Conflict resolution

If two nodes each write the same cell/key without coordinating, the
receiving side applies **last-write-wins by `modified_at_ms`**: an
incoming write/removal is only applied if its timestamp is newer than
whatever that cell/key currently holds -- a value, or a tombstone
recording when it was deleted (locally or from an earlier replicated
write); otherwise it's silently discarded. A tie (equal millisecond
timestamps) is broken by the writes' stamps (origin node id, then seq), so
every server picks the same winner whatever order the writes arrive in --
which is what makes equal vectors mean equal data. This is the same
mechanism `SELECT`'s `created`/`updated`/`version` keywords expose (see
[query-language.md](query-language.md)), applied automatically rather
than queried.

## Index operations

`CREATE INDEX`/`DROP INDEX`/`REBUILD INDEX` (see
[query-language.md](query-language.md)) are cluster-wide: running one
against any node publishes an `IndexOp` (`kblockdbcluster::wire::IndexOp`)
to every connected peer, which runs the same operation against its own
copy of the database (`kblockdbserver/src/cluster.rs`'s
`apply_index_op`). This is the one exception to "no database-level
replication" below -- a secondary index is local performance state, not
data, but keeping it in sync across the cluster without a separate manual
step was worth the small addition.

Unlike a cell write, an `IndexOp` carries no stamp and isn't tracked by
any version vector -- it's fire-and-forget while both sides are
connected, the same spirit as `PeerList` gossip. But *reconnecting*
catches up: right after the initial `PeerList`, every connection also
sends one `IndexState` frame (`kblockdbcluster::wire::PeerMessage::
IndexState`) -- the sender's complete current `(database, key)` index
list (`source::ChangeSource::indexed_keys`) -- and the receiver builds
whatever it's missing from it. So a peer that was down, lagged past the
hub's (small, 64-entry) index-op buffer, or is linking to this sender for
the very first time, still ends up with every `CREATE INDEX`/`REBUILD
INDEX` already applied elsewhere, as soon as its link comes back up --
no manual re-run needed. `CREATE INDEX`/`REBUILD INDEX` on a key an
unseen database doesn't have is a harmless no-op on the receiving peer
(same as `DROP INDEX`), and never auto-creates a database the way a
`Set`/`Remove` can.

**`DROP INDEX` is the one gap left.** `IndexState` only ever *creates* --
it never tells a peer to drop an index merely because the index isn't in
the list it just received (a peer that's still catching up, or has a
different working set of databases, would otherwise have real indexes
removed out from under it for no good reason). So a `DROP INDEX` a peer
missed while disconnected doesn't undo itself on reconnect; the extra
index just keeps being correctly maintained by ordinary replicated
writes from then on (it's accurate, just no longer wanted) until the
operator runs `DROP INDEX` again, which the live `IndexOp` path still
replicates immediately to whoever's currently connected.

## Limitations

This is a v1, intentionally minimal design:

- **Catch-up is bounded by tombstone retention.** A server offline for
  longer than `tombstone_retention_secs` must be wiped and re-copied (see
  "Deletes and tombstones").
- **A delete for a key a server has never seen is dropped.** A tombstone
  is recorded under the server's own id for that key, so if no cell in
  that database has ever had the key, there's nothing to record it under.
  A later, older write of the key from a peer that missed the delete
  would then be accepted. This needs a key's very first write anywhere to
  arrive after its delete, so it's rare.
- **Upgrade every node together.** Servers only link with a peer speaking
  exactly the same peer protocol version (5), so a mixed-version cluster
  doesn't replicate at all between old and new servers.
- **Never copy `.cluster/` to another server.** It holds the node id;
  two servers sharing one would each take the other for itself and
  refuse to link. (Copying a data directory *without* `.cluster/` is
  fine, though no longer needed -- catch-up fills a new server.)
- **Chunk files are upgraded one way.** Stamps and tombstones needed a
  new chunk file format. Older files are still read, but every chunk this
  version writes is in the new format, which an older build can't read.
- **No database-level replication.** Creating or removing a database, and
  adding or removing a column, aren't replicated; a database a peer
  hasn't seen is auto-created on its first incoming write. Equal vectors
  say nothing about these. Secondary indexes are the one exception --
  see "Index operations" above -- and even those are best-effort, not
  tracked by a vector either.
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
