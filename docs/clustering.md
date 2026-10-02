# Clustering

A `kblockdbserver` instance can list other instances as peers and
replicate its own local writes to each of them, so a small cluster of
servers converges on the same data over time. This is **opt-in and off by
default** -- nothing in this page applies unless the config file sets
`cluster_secret`.

## Config

```toml
# Enables clustering. Required (and must be non-empty) if [[peers]] is
# non-empty; a server with cluster_secret set but no peers still accepts
# incoming peer connections, it just has nothing to connect out to.
cluster_secret = "a-shared-secret-only-this-clusters-nodes-know"

# The port the peer protocol listens on (default 8082 if cluster_secret
# is set but this isn't). Distinct from http_port/binary_port.
peer_port = 8082

# Other servers to replicate local writes to -- "address" is that peer's
# own peer_port, not its HTTP or binary-protocol port.
[[peers]]
address = "10.0.0.2:8082"

[[peers]]
address = "10.0.0.3:8082"
```

A full mesh (every node replicates to every other node) means every
node's config lists every *other* node -- there's no gossip or
auto-discovery, each server only knows the peers explicitly listed in its
own file. `--peer-port` is the matching CLI flag, same override order as
every other port (`--peer-port` > config file > default).

## How it works

Every local write -- a REST `PUT`/`DELETE` on a cell or region, the
binary protocol's equivalent requests, or the query language's
`SET`/`UPDATE`/`DELETE` -- publishes a `ChangeEntry` (database, coordinate,
key, the new value or a removal, and the write's own `created_at_ms`/
`modified_at_ms`/`version`) to an in-process hub. One long-running task
per configured peer drains that hub and streams the entries over its own
TCP connection to that peer, authenticated once per connection with
`Hello`/`cluster_secret`. The accepting side applies each entry directly
against its matching database (auto-creating it, with its own default
shape, the first time it sees a database name it doesn't have yet).

A write is **never relayed onward** by the node that receives it -- only
locally-originated writes are published to the hub, so in a full mesh
every node already has a direct connection to every other node and
nothing needs multi-hop forwarding.

`GET /rest/health` reports this instance's configured peer addresses as
`"peers": [...]` (empty unless clustering is configured) -- just the
configured list, not each one's live connected/reconnecting status.

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
