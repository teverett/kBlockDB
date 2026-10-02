# Query language

A small SQL-like language over a database's cells, parsed with a
[pest](https://pest.rs) grammar (`kblockdbserver/src/query.pest`/`query.rs`).
It's the same grammar regardless of how you send it: `POST
/rest/db/{db}/query` on the [REST API](kblockdbserver.md#query), the
[binary protocol](binary-protocol.md)'s `Query` request, or
[`kblockdbcli query`](kblockdbcli.md) -- this page covers the grammar and
its semantics; each transport's own doc covers how to actually send it and
what the response looks like on the wire.

Four statement kinds:

```text
SELECT <columns> [FROM <range>] [WHERE <criteria>]
SET (<key>=<value>, ...) [WHERE <criteria>] IN <range>
UPDATE (<key>=<value>, ...) [WHERE <criteria>] [IN <range>]
DELETE [WHERE <criteria>] [IN <range>]
```

- `<columns>` is `*` or a comma-separated key list (`material, density`).
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
    literal. An operand's left side is either `x<N>` (coordinate axis `N`,
    zero-indexed) or a key name; its right side is a string (`'stone'`),
    integer, float, or boolean (`true`/`false`, case-insensitive) literal
    (`x0 >= -10` works the same as any other comparison). A key literally
    named e.g. `x0` can't be addressed this way -- a known limitation of a
    generic axis-count grammar.
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
  - Keywords (including `EXISTS`) are case-insensitive and word-bounded,
    so a key that merely starts with one -- `existsflag`, `andrew` -- is
    still an ordinary key name, never mistaken for the keyword; key/axis
    names themselves are case-sensitive.
- A comparison against a key that isn't set at a given cell, or whose
  value's type doesn't match the literal's (a string literal against a
  numeric key, say), simply doesn't match that cell -- never an error, the
  same "total, not partial" philosophy `kblockdblib` itself uses. `EXISTS`
  follows the same spirit from the other direction: it's how you
  deliberately test for that "not set" case instead of just falling
  through it.

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

`SELECT` is a read; `SET`/`UPDATE`/`DELETE` are writes. Every transport
exposes all four behind one call (`POST /rest/db/{db}/query` over REST,
`Query` over the binary protocol) rather than one per statement kind, so
`read_only` is decided by which kind of statement was actually sent,
checked after parsing and before touching anything -- not by HTTP method
or opcode the way every other operation's read/write split works.

```text
SELECT material, density WHERE x0 >= 10 AND x0 < 20 AND material = 'stone'
SELECT * WHERE EXISTS(density)
SELECT * WHERE NOT EXISTS(density)

# upsert: fills every cell in the box with material='stone', creating any
# that don't already exist.
SET (material='stone') IN (0,0,0) TO (20,20,20)

# update: only changes cells that already have material='stone' -- never
# creates one, whether or not IN is given.
UPDATE (material='basalt', hardness=9) WHERE material = 'stone' IN (0,0,0) TO (20,20,20)

DELETE WHERE material = 'air'
```

`SELECT` returns the matching rows (each with every requested column's
value and metadata); `SET`/`UPDATE`/`DELETE` return how many cells were
affected instead. `DELETE` clears *every* key set at each matching cell --
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
frequency.
