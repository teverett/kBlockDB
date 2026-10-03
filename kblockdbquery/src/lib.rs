//! kBlockDB's query language: a small SQL-like language over a world's
//! cells, with four statement kinds --
//!
//! ```text
//! SELECT <columns> [FROM <range>] [WHERE <criteria>]
//! SET (<key>=<value>, ...) [WHERE <criteria>] IN <range>
//! UPDATE (<key>=<value>, ...) [WHERE <criteria>] [IN <range>]
//! DELETE [WHERE <criteria>] [IN <range>]
//! ```
//!
//! `<columns>` is `*` or a comma-separated key list. `<range>` is
//! `(o0,o1,...) TO (e0,e1,...)`, inclusive of `o`/exclusive of `e` on every
//! axis (matching `kblockdblib::Region`'s own origin/extent convention).
//! `<criteria>` is a boolean expression combining comparisons
//! (`key = 'value'`, `x0 >= 10`, ...) and `EXISTS(key)` (whether `key` is
//! set at a cell at all, regardless of its value) with `AND`/`OR`/`NOT` and
//! parentheses; `x<N>` addresses coordinate axis `N`, `created`/`updated`/
//! `version` address a per-key `kblockdblib::CellMeta` field (see
//! `Operand::Meta`/`MetaField`), anything else is a key name. A literal
//! may also be `now()` -- the current time in milliseconds since the Unix
//! epoch, same units as `created`/`updated` (e.g. `WHERE updated < now()`)
//! -- resolved once, at parse time, so every use of it within one
//! statement is the same instant. See `docs/query-language.md` (at the
//! repository root) for the full grammar and worked examples.
//!
//! **`SET` is an upsert, `UPDATE` is not.** `SELECT`/`UPDATE`/`DELETE` all
//! operate on `kblockdblib::World::list_cells` -- i.e. only cells that
//! already have at least one key set somewhere -- so `UPDATE` (like the
//! `SET` this replaced) can only ever change cells that already exist.
//! `SET` is different: it upserts every coordinate in its (mandatory) `IN
//! <range>` that satisfies `WHERE`, creating a cell there if it doesn't
//! already exist. Because a `WHERE` clause comparing against a *key* can
//! never match a cell that doesn't exist yet (a missing key is always
//! "doesn't match", same as everywhere else in this language -- see
//! "Evaluation" below), a key-based `WHERE` makes `SET` behave exactly like
//! `UPDATE` in practice; the difference only shows up with no `WHERE` at
//! all, or one that only compares against axis coordinates (`x<N>`), where
//! `SET` can genuinely bring new cells into existence and `UPDATE` cannot.
//! `SET` requires `IN <range>` (not optional, unlike `UPDATE`'s) because
//! "upsert everywhere" has no meaningful bound -- `kblockdbserver`'s
//! `routes.rs` needs a range to know which coordinates to even consider
//! creating.
//!
//! `SELECT` is a read; `SET`/`UPDATE`/`DELETE` are writes -- see
//! `Statement::is_write`, which `kblockdbserver`'s query handler uses to
//! reject a `read_only` account's writes the same way the REST API's
//! `PUT`/`DELETE` handlers do, just checked explicitly there instead of by
//! HTTP method (this whole language shares one endpoint and one HTTP
//! method -- see `kblockdbserver/src/routes.rs`'s doc comment on why).
//!
//! This crate only builds and evaluates the AST against
//! `kblockdblib::CellEntry` values (in memory, no I/O) -- `kblockdbserver`
//! drives the actual `World::list_cells`/`get_region`/`set_region`/`set`/
//! `remove` calls the parsed statement implies, and every REST/binary-
//! protocol wire format that carries query text in and results back out.

use kblockdblib::{CellEntry, Value};
use pest::iterators::Pair;
use pest::Parser;
use pest_derive::Parser as PestParser;
use std::fmt;

#[derive(PestParser)]
#[grammar = "query.pest"]
struct QueryGrammar;

#[derive(Debug, Clone, PartialEq)]
pub enum Columns {
    All,
    Named(Vec<String>),
    /// `SELECT count(*), sum(density), ...` -- unlike `All`/`Named`, this
    /// collapses every matching cell into one aggregate result per
    /// function, not one row per cell. See `aggregate` for how.
    Aggregates(Vec<Aggregate>),
}

/// One aggregate function over a `SELECT`'s matching cells -- `count(*)`
/// counts cells; the rest reduce an `AggregateArg` -- a key's numeric
/// value, or a metadata field -- across the matching cells (ignoring
/// cells where a key is missing or non-numeric, same "doesn't apply, not
/// an error" philosophy as the rest of this language). See `aggregate`.
#[derive(Debug, Clone, PartialEq)]
pub enum Aggregate {
    /// `count(*)`: how many cells matched, regardless of any key.
    Count,
    /// `sum(arg)`: the sum of every value `arg` takes. `0.0` if none.
    Sum(AggregateArg),
    /// `mean(arg)`: the arithmetic mean of every value `arg` takes.
    /// `None` (not `0.0`/`NaN`) if none -- a mean of nothing is
    /// undefined, not zero.
    Mean(AggregateArg),
    /// `max(arg)`: the largest value `arg` takes. `None` if none.
    Max(AggregateArg),
    /// `min(arg)`: the smallest value `arg` takes. `None` if none.
    Min(AggregateArg),
}

/// What `sum`/`mean`/`max`/`min` reduce.
#[derive(Debug, Clone, PartialEq)]
pub enum AggregateArg {
    /// A key's numeric value, at every matching cell where it's set to a
    /// number.
    Key(String),
    /// `created`/`updated`/`version`: a metadata field of *every key* set
    /// at every matching cell -- metadata is per key, not per cell (see
    /// `Operand::Meta`), so e.g. `max(updated)` is the latest change to
    /// anything in the matching cells, `min(created)` the earliest
    /// creation.
    Meta(MetaField),
}

impl std::fmt::Display for AggregateArg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AggregateArg::Key(key) => f.write_str(key),
            AggregateArg::Meta(MetaField::Created) => f.write_str("created"),
            AggregateArg::Meta(MetaField::Updated) => f.write_str("updated"),
            AggregateArg::Meta(MetaField::Version) => f.write_str("version"),
        }
    }
}

impl Aggregate {
    /// The label this aggregate's result is reported under -- e.g.
    /// `"sum(density)"`, `"max(updated)"` -- echoing the function call it
    /// was parsed from (metadata keywords lowercased) rather than
    /// inventing a separate naming scheme.
    pub fn label(&self) -> String {
        match self {
            Aggregate::Count => "count(*)".to_string(),
            Aggregate::Sum(arg) => format!("sum({arg})"),
            Aggregate::Mean(arg) => format!("mean({arg})"),
            Aggregate::Max(arg) => format!("max({arg})"),
            Aggregate::Min(arg) => format!("min({arg})"),
        }
    }
}

/// One `Aggregate`'s result, labeled for display -- see `aggregate`.
#[derive(Debug, Clone, PartialEq)]
pub struct AggregateResult {
    pub label: String,
    /// `None` only for `mean`/`max`/`min` when no matching cell had a
    /// numeric value for the key in question -- `count`/`sum` are always
    /// `Some` (0 is a perfectly good count or sum of nothing).
    pub value: Option<f64>,
}

/// An axis-aligned box: `from` inclusive, `to` exclusive on every axis --
/// same convention as `kblockdblib::Region` (origin + extent), just
/// expressed as two corners instead of a corner + a size. `from.len()` and
/// `to.len()` are always equal (the grammar can't produce a mismatch --
/// both come from the same `point` rule applied twice), but may differ
/// from the world's actual axis count, which only `kblockdbserver`'s
/// `routes.rs` (the one place that knows the target `World`) can check.
#[derive(Debug, Clone, PartialEq)]
pub struct Range {
    pub from: Vec<i32>,
    pub to: Vec<i32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompareOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    Str(String),
    Int(i64),
    Float(f64),
    Bool(bool),
}

impl Literal {
    /// This literal as a `kblockdblib::Value` -- used by `SET`'s
    /// assignments, where a literal becomes the value actually written.
    pub fn to_value(&self) -> Value {
        match self {
            Literal::Str(s) => Value::Str(s.clone()),
            Literal::Int(n) => Value::I64(*n),
            Literal::Float(f) => Value::F64(*f),
            Literal::Bool(b) => Value::Bool(*b),
        }
    }
}

/// A cell's per-key `CellMeta` field, addressed by one of the `created`/
/// `updated`/`version` keywords. See `Operand::Meta` and
/// `eval_compare`'s handling of it for how a comparison against this
/// actually matches a cell.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetaField {
    /// `created`: `CellMeta::created_at_ms`.
    Created,
    /// `updated`: `CellMeta::modified_at_ms`.
    Updated,
    /// `version`: `CellMeta::version`.
    Version,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Operand {
    /// `x<N>`: coordinate axis `N`.
    Axis(usize),
    /// `created`/`updated`/`version`: a per-key metadata field.
    Meta(MetaField),
    /// Anything else: a key name.
    Key(String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    Not(Box<Expr>),
    Compare(Operand, CompareOp, Literal),
    /// `EXISTS(key)`: whether `key` is set at a cell at all, regardless of
    /// its value or type -- unlike `Compare`, which only ever matches a
    /// *particular* value, this is how `WHERE` asks "is this key set here"
    /// on its own.
    Exists(String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Select {
        columns: Columns,
        range: Option<Range>,
        where_clause: Option<Expr>,
    },
    /// An upsert: every coordinate in `range` satisfying `where_clause` is
    /// written, whether or not a cell already existed there. `range` isn't
    /// optional -- see this crate's doc comment on why.
    Set {
        assignments: Vec<(String, Literal)>,
        where_clause: Option<Expr>,
        range: Range,
    },
    /// Same shape as the old `SET`: only ever touches cells `World::list_cells`
    /// already reports (i.e. that already have some key set) -- never
    /// creates one. See this crate's doc comment on how this differs from
    /// `Set`.
    Update {
        assignments: Vec<(String, Literal)>,
        where_clause: Option<Expr>,
        range: Option<Range>,
    },
    Delete {
        where_clause: Option<Expr>,
        range: Option<Range>,
    },
}

impl Statement {
    /// `SET`/`UPDATE`/`DELETE` are writes; `SELECT` is a read. See this
    /// crate's doc comment on why this -- not HTTP method -- is what
    /// gates a `read_only` account here.
    pub fn is_write(&self) -> bool {
        matches!(
            self,
            Statement::Set { .. } | Statement::Update { .. } | Statement::Delete { .. }
        )
    }
}

#[derive(Debug, PartialEq)]
pub struct ParseError(String);

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for ParseError {}

pub fn parse(input: &str) -> Result<Statement, ParseError> {
    let mut pairs =
        QueryGrammar::parse(Rule::statement, input).map_err(|e| ParseError(e.to_string()))?;
    let statement_pair = pairs
        .next()
        .expect("Rule::statement always produces one pair");
    let inner = statement_pair.into_inner().next().expect(
        "statement = { ... ~ EOI }: the first inner pair is always the matched alternative",
    );
    match inner.as_rule() {
        Rule::select_stmt => build_select(inner),
        Rule::set_stmt => build_set(inner),
        Rule::update_stmt => build_update(inner),
        Rule::delete_stmt => build_delete(inner),
        other => {
            unreachable!("statement can only contain select/set/update/delete_stmt, got {other:?}")
        }
    }
}

/// True for any `Rule::kw_*` pair -- see query.pest's doc comment on why
/// those are atomic (and so, unlike every other keyword-adjacent detail in
/// this grammar, visible here) rather than silent. Every `for p in
/// pair.into_inner()` loop below skips them with this.
fn is_keyword(pair: &Pair<Rule>) -> bool {
    matches!(
        pair.as_rule(),
        Rule::kw_select
            | Rule::kw_set
            | Rule::kw_update
            | Rule::kw_delete
            | Rule::kw_from
            | Rule::kw_where
            | Rule::kw_in
            | Rule::kw_to
            | Rule::kw_and
            | Rule::kw_or
            | Rule::kw_not
    )
}

fn build_select(pair: Pair<Rule>) -> Result<Statement, ParseError> {
    let mut columns = Columns::All;
    let mut range = None;
    let mut where_clause = None;
    for p in pair.into_inner() {
        if is_keyword(&p) {
            continue;
        }
        match p.as_rule() {
            Rule::columns => columns = build_columns(p),
            Rule::range => range = Some(build_range(p)?),
            Rule::expr => where_clause = Some(build_expr(p)),
            other => unreachable!("unexpected rule in select_stmt: {other:?}"),
        }
    }
    Ok(Statement::Select {
        columns,
        range,
        where_clause,
    })
}

fn build_set(pair: Pair<Rule>) -> Result<Statement, ParseError> {
    let mut assignments = Vec::new();
    let mut range = None;
    let mut where_clause = None;
    for p in pair.into_inner() {
        if is_keyword(&p) {
            continue;
        }
        match p.as_rule() {
            Rule::assignment_list => assignments = build_assignment_list(p)?,
            Rule::range => range = Some(build_range(p)?),
            Rule::expr => where_clause = Some(build_expr(p)),
            other => unreachable!("unexpected rule in set_stmt: {other:?}"),
        }
    }
    Ok(Statement::Set {
        assignments,
        where_clause,
        // set_stmt = { ... ~ kw_in ~ range } -- the grammar makes IN <range>
        // mandatory, so `range` is always populated by the loop above.
        range: range.expect("set_stmt's grammar requires a range"),
    })
}

fn build_update(pair: Pair<Rule>) -> Result<Statement, ParseError> {
    let mut assignments = Vec::new();
    let mut range = None;
    let mut where_clause = None;
    for p in pair.into_inner() {
        if is_keyword(&p) {
            continue;
        }
        match p.as_rule() {
            Rule::assignment_list => assignments = build_assignment_list(p)?,
            Rule::range => range = Some(build_range(p)?),
            Rule::expr => where_clause = Some(build_expr(p)),
            other => unreachable!("unexpected rule in update_stmt: {other:?}"),
        }
    }
    Ok(Statement::Update {
        assignments,
        where_clause,
        range,
    })
}

fn build_delete(pair: Pair<Rule>) -> Result<Statement, ParseError> {
    let mut range = None;
    let mut where_clause = None;
    for p in pair.into_inner() {
        if is_keyword(&p) {
            continue;
        }
        match p.as_rule() {
            Rule::range => range = Some(build_range(p)?),
            Rule::expr => where_clause = Some(build_expr(p)),
            other => unreachable!("unexpected rule in delete_stmt: {other:?}"),
        }
    }
    Ok(Statement::Delete {
        where_clause,
        range,
    })
}

fn build_columns(pair: Pair<Rule>) -> Columns {
    match pair.into_inner().next() {
        None => Columns::All, // matched the literal "*", which has no inner rule
        Some(inner) => match inner.as_rule() {
            Rule::column_list => Columns::Named(
                inner
                    .into_inner()
                    .map(|ident| ident.as_str().to_string())
                    .collect(),
            ),
            Rule::aggregate_list => {
                Columns::Aggregates(inner.into_inner().map(build_aggregate_call).collect())
            }
            other => {
                unreachable!("columns can only contain column_list/aggregate_list, got {other:?}")
            }
        },
    }
}

fn build_aggregate_call(pair: Pair<Rule>) -> Aggregate {
    // aggregate_call = { count_call | sum_call | mean_call | max_call | min_call }
    let inner = pair.into_inner().next().unwrap();
    match inner.as_rule() {
        Rule::count_call => Aggregate::Count,
        // sum_call = { ^"SUM" ~ "(" ~ aggregate_arg ~ ")" }, etc. -- the
        // literal keyword text and parens produce no pairs of their own,
        // so the single remaining inner pair is always the argument.
        Rule::sum_call => Aggregate::Sum(aggregate_arg(inner)),
        Rule::mean_call => Aggregate::Mean(aggregate_arg(inner)),
        Rule::max_call => Aggregate::Max(aggregate_arg(inner)),
        Rule::min_call => Aggregate::Min(aggregate_arg(inner)),
        other => unreachable!(
            "aggregate_call can only contain count/sum/mean/max/min_call, got {other:?}"
        ),
    }
}

fn aggregate_arg(call: Pair<Rule>) -> AggregateArg {
    // aggregate_arg = { meta | ident }
    let arg = call
        .into_inner()
        .next()
        .expect("sum/mean/max/min_call always has an aggregate_arg pair")
        .into_inner()
        .next()
        .expect("aggregate_arg is always a meta or an ident");
    match arg.as_rule() {
        Rule::meta => AggregateArg::Meta(build_meta_field(arg)),
        _ => AggregateArg::Key(arg.as_str().to_string()),
    }
}

fn build_assignment_list(pair: Pair<Rule>) -> Result<Vec<(String, Literal)>, ParseError> {
    pair.into_inner()
        .map(|assignment| {
            let mut inner = assignment.into_inner();
            let key = inner.next().unwrap().as_str().to_string();
            let literal = build_literal(inner.next().unwrap())?;
            Ok((key, literal))
        })
        .collect()
}

fn build_range(pair: Pair<Rule>) -> Result<Range, ParseError> {
    // range = { point ~ kw_to ~ point } -- skip the (now-visible, see
    // is_keyword) kw_to pair sitting between the two points.
    let mut points = pair.into_inner().filter(|p| !is_keyword(p));
    let from = build_point(points.next().unwrap())?;
    let to = build_point(points.next().unwrap())?;
    Ok(Range { from, to })
}

fn build_point(pair: Pair<Rule>) -> Result<Vec<i32>, ParseError> {
    pair.into_inner()
        .map(|int| {
            int.as_str().parse().map_err(|_| {
                ParseError(format!(
                    "coordinate '{}' doesn't fit in an i32",
                    int.as_str()
                ))
            })
        })
        .collect()
}

fn build_expr(pair: Pair<Rule>) -> Expr {
    build_or_expr(pair.into_inner().next().unwrap())
}

fn build_or_expr(pair: Pair<Rule>) -> Expr {
    // or_expr = { and_expr ~ (kw_or ~ and_expr)* } -- skip the interspersed
    // (now-visible) kw_or pairs between the and_expr operands.
    let mut parts = pair
        .into_inner()
        .filter(|p| !is_keyword(p))
        .map(build_and_expr);
    let mut result = parts.next().unwrap();
    for part in parts {
        result = Expr::Or(Box::new(result), Box::new(part));
    }
    result
}

fn build_and_expr(pair: Pair<Rule>) -> Expr {
    let mut parts = pair
        .into_inner()
        .filter(|p| !is_keyword(p))
        .map(build_not_expr);
    let mut result = parts.next().unwrap();
    for part in parts {
        result = Expr::And(Box::new(result), Box::new(part));
    }
    result
}

fn build_not_expr(pair: Pair<Rule>) -> Expr {
    // not_expr = { kw_not ~ not_expr | primary } -- when the NOT branch
    // matched, inner() now yields [kw_not, not_expr] (see is_keyword), so
    // find the not_expr/primary pair rather than assuming it's first.
    let inner = pair
        .into_inner()
        .find(|p| !is_keyword(p))
        .expect("not_expr always has a not_expr or primary pair besides any keyword");
    match inner.as_rule() {
        Rule::not_expr => Expr::Not(Box::new(build_not_expr(inner))),
        Rule::primary => build_primary(inner),
        other => unreachable!("not_expr can only contain not_expr/primary, got {other:?}"),
    }
}

fn build_primary(pair: Pair<Rule>) -> Expr {
    let inner = pair.into_inner().next().unwrap();
    match inner.as_rule() {
        Rule::expr => build_expr(inner),
        Rule::exists => build_exists(inner),
        Rule::comparison => build_comparison(inner),
        other => unreachable!("primary can only contain expr/exists/comparison, got {other:?}"),
    }
}

fn build_exists(pair: Pair<Rule>) -> Expr {
    // exists = { kw_exists ~ "(" ~ ident ~ ")" } -- kw_exists is atomic, so
    // (like the statement/connective keywords elsewhere in this file) it
    // shows up as its own pair in inner(), ahead of the ident we actually
    // want.
    let ident = pair
        .into_inner()
        .find(|p| p.as_rule() == Rule::ident)
        .expect("exists always has an ident pair besides its keyword");
    Expr::Exists(ident.as_str().to_string())
}

fn build_comparison(pair: Pair<Rule>) -> Expr {
    let mut inner = pair.into_inner();
    let operand = build_operand(inner.next().unwrap());
    let op = build_compare_op(inner.next().unwrap());
    // A comparison's literal is only ever malformed the same ways a SET
    // assignment's is (an out-of-range number) -- see build_literal --
    // which can't actually happen here since the grammar's `int`/`float`
    // rules only ever match digits `str::parse` already accepts. Panicking
    // via `expect` would be reachable only from a grammar/parser bug, not
    // from user input.
    let literal = build_literal(inner.next().unwrap())
        .expect("a WHERE-clause literal always parses -- see build_literal");
    Expr::Compare(operand, op, literal)
}

fn build_operand(pair: Pair<Rule>) -> Operand {
    let inner = pair.into_inner().next().unwrap();
    match inner.as_rule() {
        Rule::axis => {
            let s = inner.as_str(); // "x0", "x12", ...
            let n: usize = s[1..]
                .parse()
                .expect("axis = { \"x\" ~ ASCII_DIGIT+ }: always digits after 'x'");
            Operand::Axis(n)
        }
        Rule::meta => Operand::Meta(build_meta_field(inner)),
        Rule::ident => Operand::Key(inner.as_str().to_string()),
        other => unreachable!("operand can only contain axis/meta/ident, got {other:?}"),
    }
}

fn build_meta_field(pair: Pair<Rule>) -> MetaField {
    // meta = { kw_created | kw_updated | kw_version } -- exactly one of
    // these three (now-visible, see is_keyword) keyword pairs is always
    // present.
    let kw = pair.into_inner().next().unwrap();
    match kw.as_rule() {
        Rule::kw_created => MetaField::Created,
        Rule::kw_updated => MetaField::Updated,
        Rule::kw_version => MetaField::Version,
        other => {
            unreachable!("meta can only contain kw_created/kw_updated/kw_version, got {other:?}")
        }
    }
}

fn build_compare_op(pair: Pair<Rule>) -> CompareOp {
    match pair.as_str() {
        "=" => CompareOp::Eq,
        "!=" => CompareOp::Ne,
        "<=" => CompareOp::Le,
        ">=" => CompareOp::Ge,
        "<" => CompareOp::Lt,
        ">" => CompareOp::Gt,
        other => unreachable!("compare_op can only match one of =,!=,<=,>=,<,> got {other:?}"),
    }
}

fn build_literal(pair: Pair<Rule>) -> Result<Literal, ParseError> {
    let inner = pair.into_inner().next().unwrap();
    match inner.as_rule() {
        Rule::string => {
            let raw = inner.as_str();
            Ok(Literal::Str(raw[1..raw.len() - 1].to_string()))
        }
        Rule::float => inner
            .as_str()
            .parse()
            .map(Literal::Float)
            .map_err(|_| ParseError(format!("'{}' is not a valid float", inner.as_str()))),
        Rule::int => inner
            .as_str()
            .parse()
            .map(Literal::Int)
            .map_err(|_| ParseError(format!("'{}' is not a valid integer", inner.as_str()))),
        Rule::bool_lit => Ok(Literal::Bool(inner.as_str().eq_ignore_ascii_case("true"))),
        // `now()` is resolved to a concrete millisecond timestamp right
        // here, at parse time -- not lazily, at eval time -- so every
        // comparison and assignment in one statement sees the *same*
        // "now", however many cells it ends up evaluated against. See
        // this crate's doc comment for why reading the wall clock (as
        // opposed to touching disk or a `World`) is the one exception to
        // "no I/O" here.
        Rule::now_call => Ok(Literal::Int(now_ms())),
        other => unreachable!(
            "literal can only contain string/float/int/bool_lit/now_call, got {other:?}"
        ),
    }
}

/// The current time in milliseconds since the Unix epoch -- same units as
/// `CellMeta::created_at_ms`/`modified_at_ms`, which is what makes `now()`
/// meaningful to compare against `created`/`updated`. A clock read that
/// can't go backwards relative to the epoch is assumed to always succeed;
/// the `unwrap_or` only guards a clock set before 1970, which would be a
/// misconfigured host, not a bug here.
fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

// --- Evaluation ---
//
// Pure functions over an in-memory `CellEntry` -- no I/O, no `World`. See
// this crate's doc comment for why `kblockdbserver` owns everything that
// actually touches disk.

/// Whether `cell` falls inside `range` (or `range` is `None`, matching
/// everything) *and* satisfies `where_clause` (or it's `None`, same).
pub fn matches(range: Option<&Range>, where_clause: Option<&Expr>, cell: &CellEntry) -> bool {
    if let Some(range) = range {
        let in_range = range
            .from
            .iter()
            .zip(&range.to)
            .enumerate()
            .all(|(axis, (&from, &to))| cell.coord.get(axis).is_some_and(|&c| c >= from && c < to));
        if !in_range {
            return false;
        }
    }
    where_clause.is_none_or(|expr| eval(expr, cell))
}

/// `eval` alone, without a range check -- `kblockdbserver`'s `SET` (upsert)
/// handling needs this directly: it already knows a candidate coordinate
/// is in range (from `kblockdblib::Region::iter()`), and evaluates
/// `WHERE` against either that coordinate's existing `CellEntry` or a
/// synthetic empty one if it doesn't exist yet.
pub fn eval(expr: &Expr, cell: &CellEntry) -> bool {
    match expr {
        Expr::And(a, b) => eval(a, cell) && eval(b, cell),
        Expr::Or(a, b) => eval(a, cell) || eval(b, cell),
        Expr::Not(a) => !eval(a, cell),
        Expr::Compare(operand, op, literal) => eval_compare(operand, *op, literal, cell),
        Expr::Exists(key) => cell.values.iter().any(|(k, _, _)| k == key),
    }
}

fn eval_compare(operand: &Operand, op: CompareOp, literal: &Literal, cell: &CellEntry) -> bool {
    match operand {
        Operand::Axis(i) => match (cell.coord.get(*i), literal) {
            (Some(&c), Literal::Int(n)) => compare_f64(f64::from(c), op, *n as f64),
            (Some(&c), Literal::Float(n)) => compare_f64(f64::from(c), op, *n),
            // A coordinate is never a string, and an axis past the world's
            // own axis count can't match anything -- both are "doesn't
            // match", not an error (see this crate's doc comment on
            // mismatched types).
            _ => false,
        },
        // Metadata is per *key*, not per cell (a cell with several keys
        // set has a separate created/modified/version for each one), so
        // there's no single value to compare against here the way an
        // axis coordinate has. Matching the same spirit as `EXISTS` and
        // `Operand::Key` below -- "is this true of some value here" --
        // this matches if *any* value set at the cell satisfies the
        // comparison against its own metadata.
        Operand::Meta(field) => cell.values.iter().any(|(_, _, meta)| {
            let n = match field {
                MetaField::Created => meta.created_at_ms,
                MetaField::Updated => meta.modified_at_ms,
                MetaField::Version => meta.version,
            } as f64;
            match literal {
                Literal::Int(lit) => compare_f64(n, op, *lit as f64),
                Literal::Float(lit) => compare_f64(n, op, *lit),
                // A metadata field is never a string or bool -- "doesn't
                // match", not an error, same as everywhere else.
                _ => false,
            }
        }),
        Operand::Key(name) => {
            let Some((_, value, _)) = cell.values.iter().find(|(k, _, _)| k == name) else {
                return false; // key not set at this cell -- doesn't match
            };
            match (value, literal) {
                (Value::Str(s), Literal::Str(lit)) => compare_str(s, op, lit),
                (Value::I64(n), Literal::Int(lit)) => compare_f64(*n as f64, op, *lit as f64),
                (Value::I64(n), Literal::Float(lit)) => compare_f64(*n as f64, op, *lit),
                (Value::F64(n), Literal::Float(lit)) => compare_f64(*n, op, *lit),
                (Value::F64(n), Literal::Int(lit)) => compare_f64(*n, op, *lit as f64),
                (Value::Bool(b), Literal::Bool(lit)) => compare_bool(*b, op, *lit),
                // A string compared against a number, or vice versa: not
                // an error, just never matches -- same "total, never
                // panics" philosophy as a missing key above.
                _ => false,
            }
        }
    }
}

/// Computes every `aggregates` entry over `cells` -- the cells a
/// `SELECT`'s `range`/`where_clause` already matched, via `matches`. One
/// result per entry, in the same order, labeled by `Aggregate::label`.
///
/// `cells` is iterated once per aggregate (cheap: `Vec::iter`, not a
/// re-scan of `World`), so this is just as happy with one aggregate as
/// with several in the same `SELECT`.
pub fn aggregate(aggregates: &[Aggregate], cells: &[&CellEntry]) -> Vec<AggregateResult> {
    aggregates
        .iter()
        .map(|agg| AggregateResult {
            label: agg.label(),
            value: match agg {
                Aggregate::Count => Some(cells.len() as f64),
                Aggregate::Sum(arg) => Some(arg_values(cells, arg).sum()),
                Aggregate::Mean(arg) => {
                    let values: Vec<f64> = arg_values(cells, arg).collect();
                    if values.is_empty() {
                        None
                    } else {
                        Some(values.iter().sum::<f64>() / values.len() as f64)
                    }
                }
                Aggregate::Max(arg) => arg_values(cells, arg)
                    .fold(None, |max, n| Some(max.map_or(n, |max: f64| max.max(n)))),
                Aggregate::Min(arg) => arg_values(cells, arg)
                    .fold(None, |min, n| Some(min.map_or(n, |min: f64| min.min(n)))),
            },
        })
        .collect()
}

/// Every value `arg` takes across `cells`, as `f64`: a key's numeric
/// values (see `numeric_values`), or a metadata field of every key set at
/// every cell. Millisecond timestamps are well inside `f64`'s exact
/// integer range (2^53), so `created`/`updated` lose nothing.
fn arg_values<'a>(
    cells: &'a [&CellEntry],
    arg: &'a AggregateArg,
) -> Box<dyn Iterator<Item = f64> + 'a> {
    match arg {
        AggregateArg::Key(key) => Box::new(numeric_values(cells, key)),
        AggregateArg::Meta(field) => Box::new(cells.iter().flat_map(move |cell| {
            cell.values.iter().map(move |(_, _, meta)| match field {
                MetaField::Created => meta.created_at_ms as f64,
                MetaField::Updated => meta.modified_at_ms as f64,
                MetaField::Version => meta.version as f64,
            })
        })),
    }
}

/// `key`'s numeric value (`I64` or `F64`, as `f64`) at every cell in
/// `cells` where it's set to one -- skipping a cell where it's missing,
/// or set to a `Str`/`Bool`, same "doesn't apply, not an error"
/// philosophy as everywhere else in this crate.
fn numeric_values<'a>(cells: &'a [&CellEntry], key: &'a str) -> impl Iterator<Item = f64> + 'a {
    cells.iter().filter_map(move |cell| {
        cell.values.iter().find_map(|(k, v, _)| {
            if k != key {
                return None;
            }
            match v {
                Value::I64(n) => Some(*n as f64),
                Value::F64(n) => Some(*n),
                _ => None,
            }
        })
    })
}

fn compare_f64(a: f64, op: CompareOp, b: f64) -> bool {
    match op {
        CompareOp::Eq => a == b,
        CompareOp::Ne => a != b,
        CompareOp::Lt => a < b,
        CompareOp::Le => a <= b,
        CompareOp::Gt => a > b,
        CompareOp::Ge => a >= b,
    }
}

fn compare_str(a: &str, op: CompareOp, b: &str) -> bool {
    match op {
        CompareOp::Eq => a == b,
        CompareOp::Ne => a != b,
        CompareOp::Lt => a < b,
        CompareOp::Le => a <= b,
        CompareOp::Gt => a > b,
        CompareOp::Ge => a >= b,
    }
}

/// `bool` is `Ord` in Rust (`false < true`), so `<`/`<=`/`>`/`>=` are
/// well-defined here too, same as `compare_str`/`compare_f64` -- not just
/// `=`/`!=`. Kept as plain `<`/`>` (clippy's suggested `!a & b`/`a & !b`
/// rewrite is less readable than what it's "simplifying") for symmetry with
/// every other `compare_*` function's identical match arms.
#[allow(clippy::bool_comparison)]
fn compare_bool(a: bool, op: CompareOp, b: bool) -> bool {
    match op {
        CompareOp::Eq => a == b,
        CompareOp::Ne => a != b,
        CompareOp::Lt => a < b,
        CompareOp::Le => a <= b,
        CompareOp::Gt => a > b,
        CompareOp::Ge => a >= b,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kblockdblib::CellMeta;

    fn cell(coord: &[i32], values: Vec<(&str, Value)>) -> CellEntry {
        CellEntry {
            coord: coord.into(),
            values: values
                .into_iter()
                .map(|(k, v)| {
                    (
                        k.to_string(),
                        v,
                        CellMeta {
                            created_at_ms: 0,
                            modified_at_ms: 0,
                            version: 0,
                        },
                    )
                })
                .collect(),
        }
    }

    // --- Parsing: SELECT ---

    #[test]
    fn parses_select_star_with_no_clauses() {
        let stmt = parse("SELECT *").unwrap();
        assert_eq!(
            stmt,
            Statement::Select {
                columns: Columns::All,
                range: None,
                where_clause: None,
            }
        );
    }

    #[test]
    fn parses_select_with_named_columns() {
        let stmt = parse("SELECT material, density").unwrap();
        let Statement::Select { columns, .. } = stmt else {
            panic!("expected Select");
        };
        assert_eq!(
            columns,
            Columns::Named(vec!["material".to_string(), "density".to_string()])
        );
    }

    #[test]
    fn parses_select_with_from_range() {
        let stmt = parse("SELECT * FROM (0,0,0) TO (10,10,10)").unwrap();
        let Statement::Select { range, .. } = stmt else {
            panic!("expected Select");
        };
        assert_eq!(
            range,
            Some(Range {
                from: vec![0, 0, 0],
                to: vec![10, 10, 10],
            })
        );
    }

    #[test]
    fn parses_select_with_where_clause() {
        let stmt =
            parse("SELECT material WHERE x0 >= 10 AND x0 < 20 AND material = 'stone'").unwrap();
        let Statement::Select { where_clause, .. } = stmt else {
            panic!("expected Select");
        };
        assert!(where_clause.is_some());
    }

    #[test]
    fn parses_select_with_from_and_where_together() {
        let stmt = parse(
            "SELECT material, density FROM (0,0,0) TO (100,100,100) WHERE material = 'stone'",
        )
        .unwrap();
        let Statement::Select {
            columns,
            range,
            where_clause,
        } = stmt
        else {
            panic!("expected Select");
        };
        assert_eq!(
            columns,
            Columns::Named(vec!["material".to_string(), "density".to_string()])
        );
        assert!(range.is_some());
        assert!(where_clause.is_some());
    }

    #[test]
    fn parses_a_range_with_negative_points() {
        let stmt = parse("SELECT * FROM (-10,-10,-10) TO (10,10,10)").unwrap();
        let Statement::Select { range, .. } = stmt else {
            panic!("expected Select");
        };
        assert_eq!(
            range,
            Some(Range {
                from: vec![-10, -10, -10],
                to: vec![10, 10, 10],
            })
        );
    }

    #[test]
    fn keywords_are_case_insensitive() {
        assert!(parse("select * where x0 = 1").is_ok());
        assert!(parse("Select * Where x0 = 1 and x1 = 2").is_ok());
    }

    // --- Parsing: SET (upsert -- range is mandatory) ---

    #[test]
    fn parses_set_with_a_single_assignment() {
        let stmt = parse("SET (material = 'stone') IN (0,0,0) TO (10,10,10)").unwrap();
        assert_eq!(
            stmt,
            Statement::Set {
                assignments: vec![("material".to_string(), Literal::Str("stone".to_string()))],
                where_clause: None,
                range: Range {
                    from: vec![0, 0, 0],
                    to: vec![10, 10, 10],
                },
            }
        );
    }

    #[test]
    fn parses_a_bool_literal_assignment_and_is_case_insensitive() {
        let stmt = parse("SET (flammable = TRUE) IN (0,0,0) TO (1,1,1)").unwrap();
        let Statement::Set { assignments, .. } = stmt else {
            panic!("expected Set");
        };
        assert_eq!(
            assignments,
            vec![("flammable".to_string(), Literal::Bool(true))]
        );

        let stmt = parse("SET (flammable = false) IN (0,0,0) TO (1,1,1)").unwrap();
        let Statement::Set { assignments, .. } = stmt else {
            panic!("expected Set");
        };
        assert_eq!(
            assignments,
            vec![("flammable".to_string(), Literal::Bool(false))]
        );
    }

    #[test]
    fn parses_set_with_multiple_assignments_and_a_where_clause() {
        let stmt =
            parse("SET (material='stone', hardness=7) WHERE x0 >= 10 IN (0,0,0) TO (20,20,20)")
                .unwrap();
        let Statement::Set {
            assignments,
            where_clause,
            range,
        } = stmt
        else {
            panic!("expected Set");
        };
        assert_eq!(
            assignments,
            vec![
                ("material".to_string(), Literal::Str("stone".to_string())),
                ("hardness".to_string(), Literal::Int(7)),
            ]
        );
        assert!(where_clause.is_some());
        assert_eq!(
            range,
            Range {
                from: vec![0, 0, 0],
                to: vec![20, 20, 20],
            }
        );
    }

    #[test]
    fn set_without_a_range_is_rejected() {
        // Unlike UPDATE, SET is an upsert -- "upsert everywhere" has no
        // meaningful bound, so IN <range> is mandatory.
        assert!(parse("SET (k = 1)").is_err());
        assert!(parse("SET (k = 1) WHERE x0 >= 10").is_err());
    }

    #[test]
    fn set_is_a_write() {
        let stmt = parse("SET (k = 1) IN (0,0,0) TO (1,1,1)").unwrap();
        assert!(stmt.is_write());
    }

    // --- Parsing: UPDATE (range and WHERE both optional, like the old SET) ---

    #[test]
    fn parses_update_with_a_single_assignment() {
        let stmt = parse("UPDATE (material = 'stone')").unwrap();
        assert_eq!(
            stmt,
            Statement::Update {
                assignments: vec![("material".to_string(), Literal::Str("stone".to_string()))],
                where_clause: None,
                range: None,
            }
        );
    }

    #[test]
    fn parses_update_with_multiple_assignments_and_clauses() {
        let stmt =
            parse("UPDATE (material='stone', hardness=7) WHERE x0 >= 10 IN (0,0,0) TO (20,20,20)")
                .unwrap();
        let Statement::Update {
            assignments,
            where_clause,
            range,
        } = stmt
        else {
            panic!("expected Update");
        };
        assert_eq!(
            assignments,
            vec![
                ("material".to_string(), Literal::Str("stone".to_string())),
                ("hardness".to_string(), Literal::Int(7)),
            ]
        );
        assert!(where_clause.is_some());
        assert!(range.is_some());
    }

    #[test]
    fn update_is_a_write() {
        let stmt = parse("UPDATE (k = 1)").unwrap();
        assert!(stmt.is_write());
    }

    #[test]
    fn update_keyword_is_case_insensitive() {
        assert!(parse("update (k = 1)").is_ok());
    }

    // --- Parsing: DELETE ---

    #[test]
    fn parses_bare_delete() {
        let stmt = parse("DELETE").unwrap();
        assert_eq!(
            stmt,
            Statement::Delete {
                where_clause: None,
                range: None,
            }
        );
    }

    #[test]
    fn parses_delete_with_where_and_in() {
        let stmt = parse("DELETE WHERE material = 'air' IN (0,0,0) TO (10,10,10)").unwrap();
        let Statement::Delete {
            where_clause,
            range,
        } = stmt
        else {
            panic!("expected Delete");
        };
        assert!(where_clause.is_some());
        assert!(range.is_some());
    }

    #[test]
    fn delete_is_a_write() {
        assert!(parse("DELETE").unwrap().is_write());
    }

    #[test]
    fn select_is_not_a_write() {
        assert!(!parse("SELECT *").unwrap().is_write());
    }

    // --- Parsing: operator precedence, grouping, errors ---

    #[test]
    fn not_binds_tighter_than_and_which_binds_tighter_than_or() {
        // NOT x0=1 AND x1=2 OR x2=3  ==  ((NOT x0=1) AND x1=2) OR x2=3
        let stmt = parse("SELECT * WHERE NOT x0 = 1 AND x1 = 2 OR x2 = 3").unwrap();
        let Statement::Select {
            where_clause: Some(expr),
            ..
        } = stmt
        else {
            panic!("expected a WHERE clause");
        };
        let Expr::Or(lhs, rhs) = expr else {
            panic!("expected top-level OR, got {expr:?}");
        };
        assert!(matches!(
            *rhs,
            Expr::Compare(Operand::Axis(2), CompareOp::Eq, Literal::Int(3))
        ));
        let Expr::And(and_lhs, and_rhs) = *lhs else {
            panic!("expected AND on the OR's left, got {lhs:?}");
        };
        assert!(matches!(*and_lhs, Expr::Not(_)));
        assert!(matches!(
            *and_rhs,
            Expr::Compare(Operand::Axis(1), CompareOp::Eq, Literal::Int(2))
        ));
    }

    #[test]
    fn parses_exists() {
        let stmt = parse("SELECT * WHERE EXISTS(density)").unwrap();
        let Statement::Select {
            where_clause: Some(expr),
            ..
        } = stmt
        else {
            panic!("expected a WHERE clause");
        };
        assert_eq!(expr, Expr::Exists("density".to_string()));
    }

    #[test]
    fn exists_is_case_insensitive_like_other_keywords() {
        assert!(parse("SELECT * WHERE exists(density)").is_ok());
        assert!(parse("SELECT * WHERE Exists(density)").is_ok());
    }

    #[test]
    fn a_key_named_like_the_exists_keyword_still_works_as_an_ordinary_comparison() {
        // Same word-boundary guarantee every other keyword has (see
        // query.pest's kw_* doc comment): "EXISTS" only matches the
        // keyword at a word boundary, so a key that merely starts with it
        // is still an ordinary identifier.
        let stmt = parse("SELECT * WHERE existsflag = true").unwrap();
        let Statement::Select {
            where_clause: Some(expr),
            ..
        } = stmt
        else {
            panic!("expected a WHERE clause");
        };
        assert_eq!(
            expr,
            Expr::Compare(
                Operand::Key("existsflag".to_string()),
                CompareOp::Eq,
                Literal::Bool(true)
            )
        );
    }

    #[test]
    fn exists_without_parentheses_or_a_key_is_rejected() {
        assert!(parse("SELECT * WHERE EXISTS").is_err());
        assert!(parse("SELECT * WHERE EXISTS density").is_err());
        assert!(parse("SELECT * WHERE EXISTS()").is_err());
        assert!(parse("SELECT * WHERE EXISTS(1)").is_err()); // not an ident
    }

    #[test]
    fn parentheses_override_precedence() {
        // (x0=1 OR x1=2) AND x2=3 -- top level must be AND, not OR.
        let stmt = parse("SELECT * WHERE (x0 = 1 OR x1 = 2) AND x2 = 3").unwrap();
        let Statement::Select {
            where_clause: Some(expr),
            ..
        } = stmt
        else {
            panic!("expected a WHERE clause");
        };
        assert!(matches!(expr, Expr::And(_, _)));
    }

    #[test]
    fn rejects_garbage_input() {
        assert!(parse("").is_err());
        assert!(parse("SELECT").is_err()); // no columns
        assert!(parse("SELECT * WHERE").is_err()); // WHERE with nothing after
        assert!(parse("SELECT * WHERE x0 = ").is_err());
        assert!(parse("BOGUS *").is_err());
        assert!(parse("SELECT * FROM (0,0,0)").is_err()); // FROM needs a TO
    }

    #[test]
    fn trailing_garbage_after_a_valid_statement_is_rejected() {
        // EOI is required -- a statement can't be a valid prefix of a
        // longer, malformed input.
        assert!(parse("SELECT * WHERE x0 = 1 GARBAGE").is_err());
    }

    // --- Evaluation ---

    #[test]
    fn matches_with_no_range_or_where_matches_everything() {
        let c = cell(&[1, 2, 3], vec![("k", Value::I64(1))]);
        assert!(matches(None, None, &c));
    }

    #[test]
    fn matches_range_is_inclusive_from_exclusive_to() {
        let range = Range {
            from: vec![0, 0, 0],
            to: vec![10, 10, 10],
        };
        assert!(matches(Some(&range), None, &cell(&[0, 0, 0], vec![])));
        assert!(matches(Some(&range), None, &cell(&[9, 9, 9], vec![])));
        assert!(!matches(Some(&range), None, &cell(&[10, 0, 0], vec![])));
        assert!(!matches(Some(&range), None, &cell(&[0, 0, 10], vec![])));
    }

    #[test]
    fn matches_combines_range_and_where_with_and_semantics() {
        let range = Range {
            from: vec![0, 0, 0],
            to: vec![10, 10, 10],
        };
        let where_clause = parse("SELECT * WHERE material = 'stone'").unwrap();
        let Statement::Select {
            where_clause: Some(expr),
            ..
        } = where_clause
        else {
            unreachable!()
        };

        let in_range_matching = cell(&[5, 5, 5], vec![("material", Value::Str("stone".into()))]);
        let in_range_not_matching = cell(&[5, 5, 5], vec![("material", Value::Str("air".into()))]);
        let out_of_range_matching =
            cell(&[50, 5, 5], vec![("material", Value::Str("stone".into()))]);

        assert!(matches(Some(&range), Some(&expr), &in_range_matching));
        assert!(!matches(Some(&range), Some(&expr), &in_range_not_matching));
        assert!(!matches(Some(&range), Some(&expr), &out_of_range_matching));
    }

    #[test]
    fn eval_compares_string_values() {
        let c = cell(&[0, 0, 0], vec![("material", Value::Str("stone".into()))]);
        assert!(eval(&parse_expr("material = 'stone'"), &c));
        assert!(!eval(&parse_expr("material = 'air'"), &c));
        assert!(eval(&parse_expr("material != 'air'"), &c));
    }

    #[test]
    fn eval_compares_bool_values() {
        let c = cell(&[0, 0, 0], vec![("flammable", Value::Bool(true))]);
        assert!(eval(&parse_expr("flammable = true"), &c));
        assert!(!eval(&parse_expr("flammable = false"), &c));
        assert!(eval(&parse_expr("flammable != FALSE"), &c)); // case-insensitive
        assert!(eval(&parse_expr("flammable = True"), &c));
    }

    #[test]
    fn eval_compares_numeric_values_across_int_and_float_literals() {
        let c = cell(&[0, 0, 0], vec![("hardness", Value::I64(7))]);
        assert!(eval(&parse_expr("hardness > 5"), &c));
        assert!(eval(&parse_expr("hardness >= 7"), &c));
        assert!(!eval(&parse_expr("hardness < 7"), &c));
        assert!(eval(&parse_expr("hardness < 7.5"), &c));
    }

    #[test]
    fn eval_of_a_missing_key_is_false_not_an_error() {
        let c = cell(&[0, 0, 0], vec![]);
        assert!(!eval(&parse_expr("material = 'stone'"), &c));
    }

    #[test]
    fn eval_of_a_type_mismatched_comparison_is_false_not_an_error() {
        let c = cell(&[0, 0, 0], vec![("material", Value::Str("stone".into()))]);
        assert!(!eval(&parse_expr("material = 7"), &c));
        let n = cell(&[0, 0, 0], vec![("hardness", Value::I64(7))]);
        assert!(!eval(&parse_expr("hardness = 'seven'"), &n));
        let b = cell(&[0, 0, 0], vec![("flammable", Value::Bool(true))]);
        assert!(!eval(&parse_expr("flammable = 1"), &b));
        assert!(!eval(&parse_expr("flammable = 'true'"), &b));
    }

    #[test]
    fn eval_compares_axis_coordinates() {
        let c = cell(&[10, 20, 30], vec![]);
        assert!(eval(&parse_expr("x0 = 10"), &c));
        assert!(eval(&parse_expr("x1 > 15"), &c));
        assert!(!eval(&parse_expr("x2 < 30"), &c));
    }

    #[test]
    fn eval_compares_negative_axis_coordinates() {
        let c = cell(&[-10, -20, 30], vec![]);
        assert!(eval(&parse_expr("x0 = -10"), &c));
        assert!(eval(&parse_expr("x1 < -15"), &c));
        assert!(eval(&parse_expr("x0 > -20 AND x0 < 0"), &c));
    }

    #[test]
    fn matches_range_spanning_zero() {
        let range = Range {
            from: vec![-5, -5, -5],
            to: vec![5, 5, 5],
        };
        assert!(matches(Some(&range), None, &cell(&[-5, -5, -5], vec![])));
        assert!(matches(Some(&range), None, &cell(&[0, 0, 0], vec![])));
        assert!(matches(Some(&range), None, &cell(&[4, 4, 4], vec![])));
        assert!(!matches(Some(&range), None, &cell(&[-6, 0, 0], vec![])));
        assert!(!matches(Some(&range), None, &cell(&[5, 0, 0], vec![])));
    }

    #[test]
    fn eval_and_or_not_combine_correctly() {
        let c = cell(
            &[10, 20, 30],
            vec![("material", Value::Str("stone".into()))],
        );
        assert!(eval(&parse_expr("x0 = 10 AND material = 'stone'"), &c));
        assert!(!eval(&parse_expr("x0 = 99 AND material = 'stone'"), &c));
        assert!(eval(&parse_expr("x0 = 99 OR material = 'stone'"), &c));
        assert!(eval(&parse_expr("NOT x0 = 99"), &c));
        assert!(!eval(&parse_expr("NOT x0 = 10"), &c));
    }

    #[test]
    fn eval_exists_is_true_when_the_key_is_set_regardless_of_its_value() {
        let c = cell(
            &[0, 0, 0],
            vec![
                ("material", Value::Str("stone".into())),
                ("hardness", Value::I64(0)), // a falsy-looking value still counts
                ("flammable", Value::Bool(false)),
            ],
        );
        assert!(eval(&parse_expr("EXISTS(material)"), &c));
        assert!(eval(&parse_expr("EXISTS(hardness)"), &c));
        assert!(eval(&parse_expr("EXISTS(flammable)"), &c));
    }

    #[test]
    fn eval_exists_is_false_when_the_key_is_not_set() {
        let c = cell(&[0, 0, 0], vec![("material", Value::Str("stone".into()))]);
        assert!(!eval(&parse_expr("EXISTS(density)"), &c));
    }

    // --- Parsing/evaluation: metadata keywords (created/updated/version) ---

    #[test]
    fn parses_created_updated_version_as_operands() {
        let stmt = parse("SELECT * WHERE created > 0 AND updated > 0 AND version > 0").unwrap();
        let Statement::Select {
            where_clause: Some(expr),
            ..
        } = stmt
        else {
            panic!("expected a WHERE clause");
        };
        assert_eq!(
            expr,
            Expr::And(
                Box::new(Expr::And(
                    Box::new(Expr::Compare(
                        Operand::Meta(MetaField::Created),
                        CompareOp::Gt,
                        Literal::Int(0)
                    )),
                    Box::new(Expr::Compare(
                        Operand::Meta(MetaField::Updated),
                        CompareOp::Gt,
                        Literal::Int(0)
                    )),
                )),
                Box::new(Expr::Compare(
                    Operand::Meta(MetaField::Version),
                    CompareOp::Gt,
                    Literal::Int(0)
                )),
            )
        );
    }

    #[test]
    fn metadata_keywords_are_case_insensitive_and_word_bounded() {
        assert!(parse("SELECT * WHERE CREATED > 0").is_ok());
        assert!(parse("SELECT * WHERE Version = 0").is_ok());

        // A key that merely starts with a metadata keyword is still an
        // ordinary identifier -- same word-boundary guarantee every other
        // keyword here has.
        let stmt = parse("SELECT * WHERE versioning = true").unwrap();
        let Statement::Select {
            where_clause: Some(expr),
            ..
        } = stmt
        else {
            panic!("expected a WHERE clause");
        };
        assert_eq!(
            expr,
            Expr::Compare(
                Operand::Key("versioning".to_string()),
                CompareOp::Eq,
                Literal::Bool(true)
            )
        );
    }

    #[test]
    fn eval_compares_metadata_fields() {
        let mut c = cell(&[0, 0, 0], vec![("material", Value::Str("stone".into()))]);
        c.values[0].2 = CellMeta {
            created_at_ms: 1_000,
            modified_at_ms: 3_000,
            version: 2,
        };

        assert!(eval(&parse_expr("created = 1000"), &c));
        assert!(eval(&parse_expr("updated = 3000"), &c));
        assert!(eval(&parse_expr("version = 2"), &c));
        assert!(eval(&parse_expr("version > 1"), &c));
        assert!(!eval(&parse_expr("version > 2"), &c));
    }

    #[test]
    fn eval_metadata_matches_if_any_value_at_the_cell_satisfies_it() {
        // Metadata is per key, not per cell -- a comparison matches if
        // *any* value set at the cell satisfies it against its own
        // metadata, the same "any match" spirit as EXISTS/Operand::Key.
        let mut c = cell(
            &[0, 0, 0],
            vec![
                ("material", Value::Str("stone".into())),
                ("hardness", Value::I64(7)),
            ],
        );
        c.values[0].2 = CellMeta {
            created_at_ms: 1_000,
            modified_at_ms: 1_000,
            version: 0,
        };
        c.values[1].2 = CellMeta {
            created_at_ms: 1_000,
            modified_at_ms: 5_000,
            version: 3,
        };

        assert!(eval(&parse_expr("version = 0"), &c)); // material's version
        assert!(eval(&parse_expr("version = 3"), &c)); // hardness's version
        assert!(!eval(&parse_expr("version = 9"), &c));
    }

    #[test]
    fn eval_metadata_against_a_non_numeric_literal_is_false_not_an_error() {
        let c = cell(&[0, 0, 0], vec![("material", Value::Str("stone".into()))]);
        assert!(!eval(&parse_expr("version = 'zero'"), &c));
        assert!(!eval(&parse_expr("version = true"), &c));
    }

    // --- Parsing/evaluation: aggregates (count/sum/mean/max/min) ---

    #[test]
    fn parses_count_star() {
        let stmt = parse("SELECT count(*)").unwrap();
        let Statement::Select { columns, .. } = stmt else {
            panic!("expected Select");
        };
        assert_eq!(columns, Columns::Aggregates(vec![Aggregate::Count]));
    }

    #[test]
    fn parses_count_star_with_from_and_where() {
        let stmt =
            parse("SELECT count(*) FROM (0,0,0,0) TO (9,9,9,1) WHERE material = 'stone'").unwrap();
        let Statement::Select {
            columns,
            range,
            where_clause,
        } = stmt
        else {
            panic!("expected Select");
        };
        assert_eq!(columns, Columns::Aggregates(vec![Aggregate::Count]));
        assert!(range.is_some());
        assert!(where_clause.is_some());
    }

    #[test]
    fn parses_sum_mean_max_min_and_is_case_insensitive() {
        let stmt = parse("SELECT SUM(density), Mean(density), max(density), MIN(density)").unwrap();
        let Statement::Select { columns, .. } = stmt else {
            panic!("expected Select");
        };
        assert_eq!(
            columns,
            Columns::Aggregates(vec![
                Aggregate::Sum(AggregateArg::Key("density".to_string())),
                Aggregate::Mean(AggregateArg::Key("density".to_string())),
                Aggregate::Max(AggregateArg::Key("density".to_string())),
                Aggregate::Min(AggregateArg::Key("density".to_string())),
            ])
        );
    }

    #[test]
    fn a_key_named_like_an_aggregate_function_still_works_as_a_plain_column() {
        // No "(" immediately after -- same "the paren disambiguates" rule
        // as `now()` -- so this is just an ordinary named column, not
        // count(*).
        let stmt = parse("SELECT count, material").unwrap();
        let Statement::Select { columns, .. } = stmt else {
            panic!("expected Select");
        };
        assert_eq!(
            columns,
            Columns::Named(vec!["count".to_string(), "material".to_string()])
        );
    }

    #[test]
    fn aggregate_label_echoes_the_function_call() {
        assert_eq!(Aggregate::Count.label(), "count(*)");
        assert_eq!(
            Aggregate::Sum(AggregateArg::Key("density".to_string())).label(),
            "sum(density)"
        );
        assert_eq!(
            Aggregate::Mean(AggregateArg::Key("density".to_string())).label(),
            "mean(density)"
        );
        assert_eq!(
            Aggregate::Max(AggregateArg::Key("density".to_string())).label(),
            "max(density)"
        );
        assert_eq!(
            Aggregate::Min(AggregateArg::Key("density".to_string())).label(),
            "min(density)"
        );
    }

    /// The `columns` of a `SELECT` -- for the aggregate parsing tests.
    fn select_columns(src: &str) -> Columns {
        match parse(src).unwrap() {
            Statement::Select { columns, .. } => columns,
            other => panic!("expected a SELECT, got {other:?}"),
        }
    }

    #[test]
    fn aggregates_parse_metadata_keywords_as_metadata() {
        assert_eq!(
            select_columns(
                "SELECT max(updated), min(created), mean(version), sum(VERSION) FROM (0) TO (1)"
            ),
            Columns::Aggregates(vec![
                Aggregate::Max(AggregateArg::Meta(MetaField::Updated)),
                Aggregate::Min(AggregateArg::Meta(MetaField::Created)),
                Aggregate::Mean(AggregateArg::Meta(MetaField::Version)),
                Aggregate::Sum(AggregateArg::Meta(MetaField::Version)),
            ])
        );
        // Word-bounded, like everywhere else: a key that merely starts
        // with a metadata keyword is still a key.
        assert_eq!(
            select_columns("SELECT max(updatedness) FROM (0) TO (1)"),
            Columns::Aggregates(vec![Aggregate::Max(AggregateArg::Key(
                "updatedness".to_string()
            ))])
        );
    }

    #[test]
    fn metadata_aggregates_are_labelled_lowercase() {
        assert_eq!(
            Aggregate::Max(AggregateArg::Meta(MetaField::Updated)).label(),
            "max(updated)"
        );
        assert_eq!(
            Aggregate::Min(AggregateArg::Meta(MetaField::Created)).label(),
            "min(created)"
        );
        assert_eq!(
            Aggregate::Sum(AggregateArg::Meta(MetaField::Version)).label(),
            "sum(version)"
        );
    }

    #[test]
    fn metadata_aggregates_reduce_every_keys_metadata_across_cells() {
        let meta = |created, updated, version| CellMeta {
            created_at_ms: created,
            modified_at_ms: updated,
            version,
        };
        // Two keys in one cell, each with its own metadata, plus a second
        // cell -- every key's metadata counts.
        let mut a = cell(
            &[0, 0, 0],
            vec![
                ("material", Value::Str("stone".into())),
                ("density", Value::F64(2.6)),
            ],
        );
        a.values[0].2 = meta(1_000, 5_000, 4);
        a.values[1].2 = meta(2_000, 2_000, 0);
        let mut b = cell(&[1, 0, 0], vec![("material", Value::Str("sand".into()))]);
        b.values[0].2 = meta(500, 9_000, 2);
        let empty = cell(&[2, 0, 0], vec![]);

        let results = aggregate(
            &[
                Aggregate::Max(AggregateArg::Meta(MetaField::Updated)),
                Aggregate::Min(AggregateArg::Meta(MetaField::Created)),
                Aggregate::Max(AggregateArg::Meta(MetaField::Version)),
                Aggregate::Sum(AggregateArg::Meta(MetaField::Version)),
                Aggregate::Mean(AggregateArg::Meta(MetaField::Version)),
            ],
            &[&a, &b, &empty],
        );
        let values: Vec<Option<f64>> = results.iter().map(|r| r.value).collect();
        assert_eq!(
            values,
            vec![Some(9_000.0), Some(500.0), Some(4.0), Some(6.0), Some(2.0)]
        );

        // No keys at all: nothing to take a max of.
        let results = aggregate(
            &[Aggregate::Max(AggregateArg::Meta(MetaField::Updated))],
            &[&empty],
        );
        assert_eq!(results[0].value, None);
    }

    #[test]
    fn millisecond_timestamps_survive_metadata_aggregation_exactly() {
        let mut a = cell(&[0, 0, 0], vec![("k", Value::I64(1))]);
        a.values[0].2 = CellMeta {
            created_at_ms: 1_790_999_411_204,
            modified_at_ms: 1_790_999_411_205,
            version: 0,
        };
        let results = aggregate(
            &[Aggregate::Max(AggregateArg::Meta(MetaField::Updated))],
            &[&a],
        );
        assert_eq!(results[0].value.map(|v| v as u64), Some(1_790_999_411_205));
    }

    #[test]
    fn count_counts_cells_regardless_of_any_key() {
        let a = cell(&[0, 0, 0], vec![("material", Value::Str("stone".into()))]);
        let b = cell(&[1, 0, 0], vec![]);
        let results = aggregate(&[Aggregate::Count], &[&a, &b]);
        assert_eq!(
            results,
            vec![AggregateResult {
                label: "count(*)".to_string(),
                value: Some(2.0),
            }]
        );
    }

    #[test]
    fn sum_mean_max_min_reduce_a_keys_numeric_value_across_cells() {
        let a = cell(&[0, 0, 0], vec![("density", Value::F64(2.0))]);
        let b = cell(&[1, 0, 0], vec![("density", Value::I64(5))]);
        let c = cell(&[2, 0, 0], vec![("density", Value::F64(-1.0))]);
        let cells: Vec<&CellEntry> = vec![&a, &b, &c];

        let results = aggregate(
            &[
                Aggregate::Sum(AggregateArg::Key("density".to_string())),
                Aggregate::Mean(AggregateArg::Key("density".to_string())),
                Aggregate::Max(AggregateArg::Key("density".to_string())),
                Aggregate::Min(AggregateArg::Key("density".to_string())),
            ],
            &cells,
        );
        assert_eq!(
            results,
            vec![
                AggregateResult {
                    label: "sum(density)".to_string(),
                    value: Some(6.0),
                },
                AggregateResult {
                    label: "mean(density)".to_string(),
                    value: Some(2.0),
                },
                AggregateResult {
                    label: "max(density)".to_string(),
                    value: Some(5.0),
                },
                AggregateResult {
                    label: "min(density)".to_string(),
                    value: Some(-1.0),
                },
            ]
        );
    }

    #[test]
    fn sum_of_nothing_is_zero_but_mean_max_min_of_nothing_is_none() {
        let a = cell(&[0, 0, 0], vec![("material", Value::Str("stone".into()))]);
        let results = aggregate(
            &[
                Aggregate::Sum(AggregateArg::Key("density".to_string())),
                Aggregate::Mean(AggregateArg::Key("density".to_string())),
                Aggregate::Max(AggregateArg::Key("density".to_string())),
                Aggregate::Min(AggregateArg::Key("density".to_string())),
            ],
            &[&a],
        );
        assert_eq!(results[0].value, Some(0.0)); // sum
        assert_eq!(results[1].value, None); // mean
        assert_eq!(results[2].value, None); // max
        assert_eq!(results[3].value, None); // min
    }

    #[test]
    fn aggregates_skip_cells_where_the_key_is_missing_or_non_numeric() {
        let numeric = cell(&[0, 0, 0], vec![("density", Value::F64(10.0))]);
        let missing = cell(&[1, 0, 0], vec![]);
        let wrong_type = cell(&[2, 0, 0], vec![("density", Value::Str("heavy".into()))]);
        let results = aggregate(
            &[
                Aggregate::Sum(AggregateArg::Key("density".to_string())),
                Aggregate::Count,
            ],
            &[&numeric, &missing, &wrong_type],
        );
        assert_eq!(results[0].value, Some(10.0)); // only `numeric` contributes
        assert_eq!(results[1].value, Some(3.0)); // but count(*) still counts all 3 cells
    }

    // --- Parsing/evaluation: now() ---

    #[test]
    fn now_parses_to_a_current_millisecond_timestamp_literal() {
        let before = now_ms();
        let stmt = parse("SELECT * WHERE updated < now()").unwrap();
        let after = now_ms();
        let Statement::Select {
            where_clause:
                Some(Expr::Compare(Operand::Meta(MetaField::Updated), CompareOp::Lt, Literal::Int(n))),
            ..
        } = stmt
        else {
            panic!("expected Compare(Meta(Updated), Lt, Int), got something else");
        };
        assert!(
            (before..=after).contains(&n),
            "{n} not in [{before}, {after}]"
        );
    }

    #[test]
    fn now_is_case_insensitive_and_requires_parentheses() {
        assert!(parse("SELECT * WHERE created < NOW()").is_ok());
        assert!(parse("SELECT * WHERE created < Now()").is_ok());
        assert!(parse("SELECT * WHERE created < now").is_err());
        assert!(parse("SELECT * WHERE created < now(1)").is_err());
    }

    #[test]
    fn a_key_named_like_now_still_works_as_an_ordinary_identifier() {
        // "nowish" can't match `now_call` -- the "(" right after "NOW" is
        // what distinguishes the function call, not a word-boundary
        // lookahead, so an identifier that happens to start the same way
        // is unaffected.
        let stmt = parse("SELECT * WHERE nowish = true").unwrap();
        let Statement::Select {
            where_clause: Some(expr),
            ..
        } = stmt
        else {
            panic!("expected a WHERE clause");
        };
        assert_eq!(
            expr,
            Expr::Compare(
                Operand::Key("nowish".to_string()),
                CompareOp::Eq,
                Literal::Bool(true)
            )
        );
    }

    #[test]
    fn now_can_also_be_used_in_an_assignment() {
        let before = now_ms();
        let stmt = parse("SET (seen_at = now()) IN (0,0,0) TO (1,1,1)").unwrap();
        let after = now_ms();
        let Statement::Set { assignments, .. } = stmt else {
            panic!("expected Set");
        };
        let [(key, Literal::Int(n))] = assignments.as_slice() else {
            panic!("expected a single int assignment, got {assignments:?}");
        };
        assert_eq!(key, "seen_at");
        assert!(
            (before..=after).contains(n),
            "{n} not in [{before}, {after}]"
        );
    }

    #[test]
    fn now_matches_against_metadata_as_an_ordinary_int_comparison() {
        let mut c = cell(&[0, 0, 0], vec![("material", Value::Str("stone".into()))]);
        c.values[0].2 = CellMeta {
            created_at_ms: 1,
            modified_at_ms: 1,
            version: 0,
        };
        // A cell created/modified at ms=1 is always in the past relative
        // to now() -- this is exactly `load.sh`'s own worked example,
        // `WHERE updated < now()`.
        assert!(eval(&parse_expr("updated < now()"), &c));
        assert!(eval(&parse_expr("created < now()"), &c));
    }

    #[test]
    fn eval_exists_combines_with_not_and_and_or() {
        let with_density = cell(&[0, 0, 0], vec![("density", Value::F64(2.6))]);
        let without_density = cell(&[0, 0, 0], vec![("material", Value::Str("stone".into()))]);

        assert!(eval(&parse_expr("NOT EXISTS(density)"), &without_density));
        assert!(!eval(&parse_expr("NOT EXISTS(density)"), &with_density));

        assert!(eval(
            &parse_expr("EXISTS(density) AND density > 1"),
            &with_density
        ));
        assert!(!eval(
            &parse_expr("EXISTS(density) AND density > 1"),
            &without_density
        ));

        assert!(eval(
            &parse_expr("EXISTS(density) OR material = 'stone'"),
            &without_density
        ));
    }

    /// Test-only helper: parses `SELECT * WHERE <src>` and returns just the
    /// resulting `Expr`, so evaluation tests can write a bare predicate.
    fn parse_expr(src: &str) -> Expr {
        let stmt = parse(&format!("SELECT * WHERE {src}")).unwrap();
        let Statement::Select {
            where_clause: Some(expr),
            ..
        } = stmt
        else {
            panic!("expected a WHERE clause");
        };
        expr
    }
}
