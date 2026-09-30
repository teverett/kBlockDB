//! kBlockDB's query language: a small SQL-like language over a world's
//! populated cells, with three statement kinds --
//!
//! ```text
//! SELECT <columns> [FROM <range>] [WHERE <criteria>]
//! SET (<key>=<value>, ...) [WHERE <criteria>] [IN <range>]
//! DELETE [WHERE <criteria>] [IN <range>]
//! ```
//!
//! `<columns>` is `*` or a comma-separated key list. `<range>` is
//! `(o0,o1,...) TO (e0,e1,...)`, inclusive of `o`/exclusive of `e` on every
//! axis (matching `kblockdblib::Region`'s own origin/extent convention).
//! `<criteria>` is a boolean expression combining comparisons
//! (`key = 'value'`, `x0 >= 10`, ...) with `AND`/`OR`/`NOT` and
//! parentheses; `x<N>` addresses coordinate axis `N`, anything else is a
//! key name.
//!
//! `SELECT` is a read; `SET`/`DELETE` are writes -- see
//! `Statement::is_write`, which `routes.rs`'s query handler uses to reject
//! a `read_only` account's `SET`/`DELETE` the same way the REST API's
//! `PUT`/`DELETE` handlers do, just checked explicitly there instead of by
//! HTTP method (this whole language shares one endpoint and one HTTP
//! method -- see `routes.rs`'s doc comment on why).
//!
//! This module only builds and evaluates the AST against
//! `kblockdblib::CellEntry` values (in memory, no I/O) -- `routes.rs`
//! drives the actual `World::list_cells`/`set`/`remove` calls the parsed
//! statement implies.

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
}

/// An axis-aligned box: `from` inclusive, `to` exclusive on every axis --
/// same convention as `kblockdblib::Region` (origin + extent), just
/// expressed as two corners instead of a corner + a size. `from.len()` and
/// `to.len()` are always equal (the grammar can't produce a mismatch --
/// both come from the same `point` rule applied twice), but may differ
/// from the world's actual axis count, which only `routes.rs` (the one
/// place that knows the target `World`) can check.
#[derive(Debug, Clone, PartialEq)]
pub struct Range {
    pub from: Vec<u32>,
    pub to: Vec<u32>,
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
}

impl Literal {
    /// This literal as a `kblockdblib::Value` -- used by `SET`'s
    /// assignments, where a literal becomes the value actually written.
    pub fn to_value(&self) -> Value {
        match self {
            Literal::Str(s) => Value::Str(s.clone()),
            Literal::Int(n) => Value::I64(*n),
            Literal::Float(f) => Value::F64(*f),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Operand {
    /// `x<N>`: coordinate axis `N`.
    Axis(usize),
    /// Anything else: a key name.
    Key(String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    Not(Box<Expr>),
    Compare(Operand, CompareOp, Literal),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Select {
        columns: Columns,
        range: Option<Range>,
        where_clause: Option<Expr>,
    },
    Set {
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
    /// `SET`/`DELETE` are writes; `SELECT` is a read. See this module's
    /// doc comment on why this -- not HTTP method -- is what gates a
    /// `read_only` account here.
    pub fn is_write(&self) -> bool {
        matches!(self, Statement::Set { .. } | Statement::Delete { .. })
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
        Rule::delete_stmt => build_delete(inner),
        other => unreachable!("statement can only contain select/set/delete_stmt, got {other:?}"),
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
        Some(column_list) => {
            let names = column_list
                .into_inner()
                .map(|ident| ident.as_str().to_string())
                .collect();
            Columns::Named(names)
        }
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

fn build_point(pair: Pair<Rule>) -> Result<Vec<u32>, ParseError> {
    pair.into_inner()
        .map(|uint| {
            uint.as_str().parse().map_err(|_| {
                ParseError(format!(
                    "coordinate '{}' doesn't fit in a u32",
                    uint.as_str()
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
        Rule::comparison => build_comparison(inner),
        other => unreachable!("primary can only contain expr/comparison, got {other:?}"),
    }
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
        Rule::ident => Operand::Key(inner.as_str().to_string()),
        other => unreachable!("operand can only contain axis/ident, got {other:?}"),
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
        other => unreachable!("literal can only contain string/float/int, got {other:?}"),
    }
}

// --- Evaluation ---
//
// Pure functions over an in-memory `CellEntry` -- no I/O, no `World`. See
// this module's doc comment for why `routes.rs` owns everything that
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

fn eval(expr: &Expr, cell: &CellEntry) -> bool {
    match expr {
        Expr::And(a, b) => eval(a, cell) && eval(b, cell),
        Expr::Or(a, b) => eval(a, cell) || eval(b, cell),
        Expr::Not(a) => !eval(a, cell),
        Expr::Compare(operand, op, literal) => eval_compare(operand, *op, literal, cell),
    }
}

fn eval_compare(operand: &Operand, op: CompareOp, literal: &Literal, cell: &CellEntry) -> bool {
    match operand {
        Operand::Axis(i) => match (cell.coord.get(*i), literal) {
            (Some(&c), Literal::Int(n)) => compare_f64(f64::from(c), op, *n as f64),
            (Some(&c), Literal::Float(n)) => compare_f64(f64::from(c), op, *n),
            // A coordinate is never a string, and an axis past the world's
            // own axis count can't match anything -- both are "doesn't
            // match", not an error (see this module's doc comment on
            // mismatched types).
            _ => false,
        },
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
                // A string compared against a number, or vice versa: not
                // an error, just never matches -- same "total, never
                // panics" philosophy as a missing key above.
                _ => false,
            }
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use kblockdblib::CellMeta;

    fn cell(coord: &[u32], values: Vec<(&str, Value)>) -> CellEntry {
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
    fn keywords_are_case_insensitive() {
        assert!(parse("select * where x0 = 1").is_ok());
        assert!(parse("Select * Where x0 = 1 and x1 = 2").is_ok());
    }

    // --- Parsing: SET ---

    #[test]
    fn parses_set_with_a_single_assignment() {
        let stmt = parse("SET (material = 'stone')").unwrap();
        assert_eq!(
            stmt,
            Statement::Set {
                assignments: vec![("material".to_string(), Literal::Str("stone".to_string()))],
                where_clause: None,
                range: None,
            }
        );
    }

    #[test]
    fn parses_set_with_multiple_assignments_and_clauses() {
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
        assert!(range.is_some());
    }

    #[test]
    fn set_is_a_write() {
        let stmt = parse("SET (k = 1)").unwrap();
        assert!(stmt.is_write());
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
    }

    #[test]
    fn eval_compares_axis_coordinates() {
        let c = cell(&[10, 20, 30], vec![]);
        assert!(eval(&parse_expr("x0 = 10"), &c));
        assert!(eval(&parse_expr("x1 > 15"), &c));
        assert!(!eval(&parse_expr("x2 < 30"), &c));
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
