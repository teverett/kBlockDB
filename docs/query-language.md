# Query language

A small SQL-like language over a database's cells, implemented in its own
crate (`kblockdbquery/src/lib.rs`/`query.pest`), parsed with a
[pest](https://pest.rs) grammar. `kblockdbserver` embeds it; it has no I/O
or transport concerns of its own -- see `kblockdbquery/src/lib.rs`'s own
doc comment. It's the same grammar regardless of how you send it: `POST
/rest/db/{db}/query` on the [REST API](kblockdbserver.md#query), the
[binary protocol](binary-protocol.md)'s `Query` request, or
[`kblockdbcli query`](kblockdbcli.md) -- this page covers the grammar and
its semantics; each transport's own doc covers how to actually send it and
what the response looks like on the wire.

Seven statement kinds:

```text
SELECT <columns> [FROM <range>] [WHERE <criteria>]
SET (<key>=<value>, ...) [WHERE <criteria>] IN <range>
UPDATE (<key>=<value>, ...) [WHERE <criteria>] [IN <range>]
DELETE [WHERE <criteria>] [IN <range>]
CREATE INDEX ON <key>
DROP INDEX ON <key>
REBUILD INDEX ON <key>
```

- `<columns>` is `*`, a comma-separated key list (`material, density`), or
  a comma-separated aggregate list (`count(*)`, `sum(density)`,
  `mean(density), max(density), min(density)`) -- see "Aggregates" below.
  A query can't mix the two: it's either plain columns or aggregates, not
  both (there's no `GROUP BY` to make a mix meaningful).
- `<range>` is `(o0,o1,...) TO (e0,e1,...)` -- an axis-aligned box, `o`
  inclusive/`e` exclusive on every axis, same convention as
  `kblockdblib::Region` (origin + extent), just written as two corners.
  Each component is a signed integer (a database's valid range is centered
  on zero -- see [kblockdblib's design notes](kblockdblib.md#design) on
  coordinates), e.g. `(-10,-10,-10) TO (10,10,10)`. Its axis count must
  match the database's, or the query is rejected with an error before
  touching any data.
- `<criteria>` is a boolean expression:
  - Comparisons (`=`, `!=`, `<`, `<=`, `>`, `>=`) between an operand and a
    literal. An operand's left side is `x<N>` (coordinate axis `N`,
    zero-indexed), one of the metadata keywords below, or a key name; its
    right side is a string (`'stone'`), integer, float, boolean
    (`true`/`false`, case-insensitive), or `now()` literal (`x0 >= -10`
    works the same as any other comparison). A key literally named e.g.
    `x0`, `created`, `updated`, or `version` can't be addressed this way --
    a known limitation of a generic axis-count grammar, extended here to
    the metadata keywords too.
  - `now()` is the current time in milliseconds since the Unix epoch --
    same units as `created`/`updated` -- resolved once, when the query is
    parsed, so every use of it within one statement (even across several
    comparisons, or in a `SET`/`UPDATE` assignment) is the same instant,
    e.g. `WHERE updated < now()` (cells not touched since the query
    started) or `SET (seen_at = now())`.
  - `created`, `updated`, and `version` address a cell's per-key metadata
    (the same `created_at_ms`/`modified_at_ms`/`version` every query
    result already reports -- see below) instead of a value, e.g.
    `WHERE version > 0` or `WHERE created < 1700000000000`. Only integer
    and float literals compare against them (a string or boolean literal
    never matches, same as any other type mismatch). Metadata is recorded
    per *key*, not per cell -- a cell with several keys set has a separate
    `created`/`updated`/`version` for each -- so a comparison against one
    of these keywords matches a cell if *any* value set there satisfies it,
    the same "is this true of something here" spirit as `EXISTS` below.
  - `EXISTS(<key>)` -- whether `<key>` is set at a cell at all, regardless
    of its value or type. Unlike a comparison, which only ever matches a
    *particular* value, this is how you ask "is this key set here" on its
    own, e.g. `WHERE EXISTS(density)`. Its argument is always a plain key
    name, never `x<N>`: every cell has every axis coordinate, so "does
    this axis exist" isn't a meaningful question the way "is this key set"
    is.
  - Any of the above combined with `AND`/`OR`/`NOT` and parentheses,
    standard precedence (`NOT` binds tightest, then `AND`, then `OR`).
    `EXISTS` composes with these exactly like a comparison does --
    `NOT EXISTS(density)`, `EXISTS(density) AND density > 1`,
    `EXISTS(a) OR EXISTS(b)`, and so on.
  - Keywords (including `EXISTS` and the metadata keywords) are
    case-insensitive and word-bounded, so a key that merely starts with
    one -- `existsflag`, `versioning`, `andrew` -- is still an ordinary
    key name, never mistaken for the keyword; key/axis names themselves
    are case-sensitive.
- A comparison against a key that isn't set at a given cell, or whose
  value's type doesn't match the literal's (a string literal against a
  numeric key, say), simply doesn't match that cell -- never an error, the
  same "total, not partial" philosophy `kblockdblib` itself uses. `EXISTS`
  follows the same spirit from the other direction: it's how you
  deliberately test for that "not set" case instead of just falling
  through it.

**Aggregates.** `SELECT`'s `<columns>` can instead be a comma-separated
list of `count(*)`, `sum(<key>)`, `mean(<key>)`, `max(<key>)`, or
`min(<key>)` calls (case-insensitive, e.g. `SELECT COUNT(*)` works too).
Each collapses every cell `FROM`/`WHERE` matched into a single number,
rather than one row per cell:

- `count(*)` -- how many cells matched, regardless of any key.
- `sum`/`mean`/`max`/`min(<key>)` -- that reduction of `<key>`'s numeric
  value across every matching cell where it's *set to a number*. A cell
  where `<key>` is missing, or set to a string or boolean, is simply
  skipped for that aggregate -- same "doesn't apply, not an error"
  philosophy as everywhere else here (it does *not* disqualify the cell
  from `count(*)` or from any other aggregate in the same `SELECT`).
  `sum` of nothing is `0`; `mean`/`max`/`min` of nothing is `null` --
  unlike `sum`, there's no sensible number to report for an empty set.
- `sum`/`mean`/`max`/`min(created | updated | version)` -- the same
  reductions over metadata. Metadata is per key (see the metadata keywords
  above), so these take in *every key set* at every matching cell:
  `max(updated)` is when anything in the matching cells last changed (ms
  since the Unix epoch), `min(created)` when the oldest value was first
  written, `max(version)` the most-overwritten value's version. Like the
  WHERE keywords, a key literally named `created`, `updated` or `version`
  can't be aggregated.

A key literally named e.g. `count`, `sum`, `mean`, `max`, or `min` can
still be selected or compared against as an ordinary column/key -- same
"the `(` right after disambiguates it, same as `now()`" rule as the
metadata keywords above; only when immediately followed by `(` does one
of these parse as an aggregate call.

```text
SELECT count(*) FROM (0,0,0,0) TO (9,9,9,1) WHERE material = 'stone'
SELECT sum(density), mean(density), max(density), min(density)
SELECT count(*), max(updated), min(created) WHERE material = 'stone'
```

**`SET` is an upsert; `UPDATE` is not.** `SELECT`/`UPDATE`/`DELETE` all run
on `kblockdblib::World::list_cells` -- i.e. only cells that already have at
least one key set somewhere. `UPDATE` can only ever change such a cell,
never create one, same as this whole language's original `SET` used to
work. `SET` is different: it upserts every coordinate in its `IN <range>`
that satisfies `WHERE`, creating a cell there if one doesn't already exist
-- which is exactly why `IN <range>` isn't optional for `SET` the way it is
for `UPDATE`/`DELETE`: "upsert everywhere" has no meaningful bound. Because
a `WHERE` clause comparing against a *key* (including `EXISTS`) can never
match a cell that doesn't exist yet (a missing key is always "doesn't
match", per the bullet above), a key-based `WHERE` makes `SET` behave
exactly like `UPDATE` in practice -- the two only diverge with no `WHERE`
at all, or one that only compares axis coordinates (`x<N>`), where `SET`
can genuinely bring new cells into existence.

`SELECT` is a read; `SET`/`UPDATE`/`DELETE`/`CREATE INDEX`/`DROP INDEX`/
`REBUILD INDEX` are writes. Every transport exposes all seven behind one
call (`POST /rest/db/{db}/query` over REST, `Query` over the binary
protocol) rather than one per statement kind, so `read_only` is decided by
which kind of statement was actually sent, checked after parsing and
before touching anything -- not by HTTP method or opcode the way every
other operation's read/write split works.

```text
SELECT material, density WHERE x0 >= 10 AND x0 < 20 AND material = 'stone'
SELECT * WHERE EXISTS(density)
SELECT * WHERE NOT EXISTS(density)
SELECT * WHERE version > 0
SELECT * WHERE created >= 1700000000000 AND updated < 1700000100000
SELECT * WHERE updated < now()

# upsert: fills every cell in the box with material='stone', creating any
# that don't already exist.
SET (material='stone') IN (0,0,0) TO (20,20,20)

# update: only changes cells that already have material='stone' -- never
# creates one, whether or not IN is given.
UPDATE (material='basalt', hardness=9) WHERE material = 'stone' IN (0,0,0) TO (20,20,20)

DELETE WHERE material = 'air'
```

A plain `SELECT` returns the matching rows (each with every requested
column's value and metadata); an aggregate `SELECT` (`count(*)`,
`sum(...)`, ...) returns one summary result per function instead, over
every matching cell, not one row per cell; `SET`/`UPDATE`/`DELETE` return
how many cells were affected instead of either. `DELETE` clears *every*
key set at each matching cell --
there's no column list to delete only some of them. See
[the REST API](kblockdbserver.md#query) or the [binary
protocol](binary-protocol.md) for the exact response shape on the wire.

`SELECT`/`UPDATE`/`DELETE` run on `kblockdblib::World::list_cells` under
the hood (the same full-chunk-decode walk the
[data browser](kblockdbserver.md#data-browser) uses) -- `SELECT` filters
and projects it directly; `UPDATE`/`DELETE` use it to find matching
coordinates, then apply the write in a second pass. `SET` without a
`WHERE` skips `list_cells` entirely and goes straight to
`World::set_region` (the same primitive the REST API's `/rest/db/{db}/regions`
endpoints use) -- one call per assignment, as efficient as the region
endpoints; `SET` *with* a `WHERE` falls back to a
`list_cells`-plus-per-coordinate-`Region::iter` scan, since which
coordinates match can depend on a cell's existing values. None of this is
atomic with respect to a concurrent writer touching the same range in
between the scan and the write -- a real (if narrow) race, same honest
trade-off `list_cells`'s own doc comment already makes for reads. Fine for
the occasional bulk edit; not meant for a database with millions of
populated cells or for these write statements racing each other at high
frequency -- unless the key a `WHERE` filters on has a secondary index
(see "Indexes" below), in which case `SELECT`/`UPDATE`/`DELETE` skip
`list_cells` and look the matching coordinates up directly.

**Indexes.** `CREATE INDEX ON <key>` builds a secondary equality index on
`key` (see [kblockdblib's design notes](kblockdblib.md) and
`kblockdblib::World::create_index`): an on-disk map from `key`'s value to
every coordinate currently holding it (not an in-memory structure -- it
doesn't need to fit in RAM, see kblockdblib's own "Indexes" section),
kept up to date by every later write to `key`. `DROP INDEX ON <key>`
discards it. `REBUILD INDEX ON <key>`
discards whatever's there (if anything) and builds a fresh one from
scratch -- the recovery lever if an index is ever suspected to have gone
stale, though it shouldn't: every write path keeps it in sync. All three
are schema-level operations on the whole column -- `IN <range>`/`WHERE`
aren't meaningful here and aren't accepted. `CREATE INDEX` is idempotent
(building an already-indexed key's index again is a no-op); `REBUILD
INDEX` is not -- it always rebuilds, indexed or not. `CREATE INDEX`/
`REBUILD INDEX` on a key that's never been written are both no-ops (there's
no type to fix an index to yet) rather than an error -- they simply do
nothing until something sets that key. `DROP INDEX` on a key with no index
is an error (404 over REST).

In a clustered deployment (see [clustering.md](clustering.md)), all three
are cluster-wide: running one against any node builds/drops/rebuilds the
same index on every connected peer too, not just the node the statement
was sent to. `CREATE INDEX`/`REBUILD INDEX` also catch a peer up on
reconnect, so one that was disconnected when you ran it still ends up
with the index once its link comes back -- `DROP INDEX` doesn't (see
clustering.md's "Index operations" for why).

```text
CREATE INDEX ON material
DROP INDEX ON material
REBUILD INDEX ON material
```

Once an index exists, no further query syntax is needed to benefit from
it: `SELECT`/`UPDATE`/`DELETE ... WHERE material = 'stone'` automatically
resolves `material`'s matching coordinates from the index instead of
scanning every chunk, then still re-checks the full `WHERE` clause against
each candidate (so an `AND`ed, `OR`ed, or otherwise more complex clause
stays correct, not just fast). Only `=`/`!=`-style equality against an
indexed key is ever served directly from it; a `<`/`<=`/`>`/`>=`
comparison, `EXISTS`, `OR`, or `NOT` still falls back to scanning. `SET`'s
`WHERE` path doesn't use an index at all: it needs every candidate
*coordinate* in its range, including ones with no cell yet, which a value
index can't help with.
