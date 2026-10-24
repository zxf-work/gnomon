//! `match`: the pattern operator's plan executor.
//!
//! # What runs here
//!
//! The operator (`gnomon/temporal/ops_match.py`) compiles its argument to one
//! plan of the plan IR (`gnomon/plan/node.py`) and hands this module that plan as JSON (`plan_json`): a DAG of
//! `NodeScan`, `EdgeScan`, `Expand`, `Filter`, `TypeConstraint`, `Project`,
//! `Join`, `Aggregate`, `Order` and `Limit` nodes, Σ, and the output columns;
//! under the shared-instant rule (one shared valid instant) also `CoValidExpand`, `CoValidJoin`
//! and `Collapse` (module `covalid`).
//! The plan IR's reference evaluator runs the same plan when the handle was opened with
//! `GNOMON_MATCH_KERNEL=python`, so the two executors differ in execution only,
//! and every rule below is the evaluator's rule restated on this
//! representation.
//!
//! # How it runs: range-native
//!
//! * A relation is a **binding table**: per pattern variable a column of
//!   dense `u32` ids and a column of version addresses (segment index, row);
//!   per computed expression a typed column. No row carries a string.
//! * Scans read the believed, window-clipped row ranges of each segment at
//!   `as_of` (the scan planner's ranges for current belief; the version-key
//!   walk for `superseded`/`all`), never a materialised version list.
//! * `Expand` walks the per-segment temporal adjacency index through
//!   `neighbors_batch`, once per hop for the whole distinct frontier, under
//!   the same belief ranges; a closure is a level-synchronous multi-source
//!   breadth-first search keyed by (seed, node).
//! * Node versions under Σ come from identity point reads for a few ids, or
//!   from one pass over the node lane's believed ranges for many.
//! * A property is read through a **typed property column**: each distinct
//!   payload a segment interns is parsed once per key (`props.py`'s type
//!   classes), never once per binding row. Props are never rewritten.
//! * Joins are hash joins on dense ids or encoded keys; aggregates fold with
//!   exact integer sums and a correctly rounded float sum; the order is a
//!   top-k by `select_nth_unstable_by` and a sort of the page alone.
//! * Strings (uids, labels, eids, property text) are built for the returned
//!   page only; `rows_total` is a count.
//!
//! # When it declines
//!
//! `Ok(None)`, and the operator takes the reference route, for: an open write
//! batch (staged rows are not in segments); a node or expression form this
//! executor does not implement; and any row that would make the evaluator
//! raise (division by zero, a non-canonical uid cast, a typed read of an
//! integer outside int64, a comparison across type classes). Declining on
//! the error forms leaves the error itself to the one implementation that
//! defines it, so the two routes can never disagree about which calls fail.

// Column-at-a-time loops index several parallel columns by row position.
#![allow(clippy::needless_range_loop)]

use std::cmp::Ordering;
use std::collections::HashMap;
use std::hash::BuildHasherDefault;
use std::sync::Arc;

use serde_json::Value as J;

use crate::derive::Id96;
use crate::error::{EngineError, Result};
use crate::read::{Belief, VersionFilter};
use crate::row::RowKind;
use crate::scan::{ScanRequest, ScanSet, ScanTarget};
use crate::segment::{MmapSource, Segment};
use crate::store::{segment_id_of, NativeStore, NeighborDir};
use crate::visibility::CloseIndex;
use crate::OPEN_END;

mod covalid;

// --------------------------------------------------------------------------
// hashing
// --------------------------------------------------------------------------

#[derive(Default, Clone, Copy)]
struct Fx(u64);

impl std::hash::Hasher for Fx {
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        for c in bytes.chunks(8) {
            let mut b = [0u8; 8];
            b[..c.len()].copy_from_slice(c);
            self.add(u64::from_le_bytes(b) ^ c.len() as u64);
        }
    }
    #[inline]
    fn write_u32(&mut self, v: u32) {
        self.add(v as u64);
    }
    #[inline]
    fn write_u64(&mut self, v: u64) {
        self.add(v);
    }
    #[inline]
    fn write_usize(&mut self, v: usize) {
        self.add(v as u64);
    }
}

impl Fx {
    #[inline]
    fn add(&mut self, v: u64) {
        self.0 = (self.0.rotate_left(5) ^ v).wrapping_mul(0x51_7c_c1_b7_27_22_0a_95);
    }
}

type FxMap<K, V> = HashMap<K, V, BuildHasherDefault<Fx>>;

// --------------------------------------------------------------------------
// the answer
// --------------------------------------------------------------------------

/// One output value of the returned page.
#[derive(Clone, Debug, PartialEq)]
pub enum MVal {
    Null,
    Int(i64),
    Float(f64),
    Bool(bool),
    Str(String),
    /// JSON text: a whole stored `props`, or one property's value; the
    /// operator parses it, so its Python value is what `json.loads` gives.
    Json(String),
}

/// The page and the size of the whole answer.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct MatchOut {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<MVal>>,
    pub rows_total: u64,
}

/// A weight pattern's answer column-wise (`paths`' pair tables): `src` and
/// `dst` as dense ids, `w` typed (`w_kind`: 0 null, 1 integer in `w_int`, 2
/// float in `w_f64`), in the answer's row order.
pub struct PairsOut {
    pub src: Vec<u32>,
    pub dst: Vec<u32>,
    pub w_kind: Vec<u8>,
    pub w_int: Vec<i64>,
    pub w_f64: Vec<f64>,
}

/// Why the executor handed a call back. Internal: the caller sees `None`.
#[derive(Debug)]
enum Decline {
    /// a form this executor does not implement
    Unsupported(String),
    /// a row the evaluator would refuse; the reference route raises it
    Refuse(String),
    Engine(EngineError),
}

impl From<EngineError> for Decline {
    fn from(e: EngineError) -> Self {
        Decline::Engine(e)
    }
}

type DResult<T> = std::result::Result<T, Decline>;

/// The last reason this process's executor handed a call back, for receipts
/// and for the corpus check that no benchmark row falls back.
static LAST_DECLINE: std::sync::Mutex<String> = std::sync::Mutex::new(String::new());

fn note_decline(d: &Decline) {
    let text = match d {
        Decline::Unsupported(w) => format!("unsupported: {w}"),
        Decline::Refuse(w) => format!("refused: {w}"),
        Decline::Engine(e) => format!("engine: {e}"),
    };
    if let Ok(mut g) = LAST_DECLINE.lock() {
        *g = text;
    }
}

/// Why the executor last returned `None` (empty if it never has).
pub fn last_decline() -> String {
    LAST_DECLINE.lock().map(|g| g.clone()).unwrap_or_default()
}

fn unsupported<T>(what: impl Into<String>) -> DResult<T> {
    Err(Decline::Unsupported(what.into()))
}

fn refuse<T>(what: impl Into<String>) -> DResult<T> {
    Err(Decline::Refuse(what.into()))
}

/// The largest binding table any stage may build. Past it the call refuses
/// rather than truncating (a partial relation would give false absences).
const MAX_ROWS: usize = 400_000_000;

// --------------------------------------------------------------------------
// the plan
// --------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum VtMode {
    Overlap,
    Event,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BeliefMode {
    Current,
    Superseded,
    All,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Hops {
    Exact(u32),
    Bounded(u32, u32),
    Unbounded(u32),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum JoinKind {
    Inner,
    LeftOuter,
    Anti,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AggFn {
    Count,
    CountDistinct,
    SumExact,
    Min,
    Max,
    Mean,
    Collect,
}

#[derive(Clone, Debug)]
struct Agg {
    func: AggFn,
    alias: String,
    of: Option<Expr>,
    /// `collect`: the element's components, the list's order, its bound,
    /// and whether equal elements are kept once
    items: Vec<Expr>,
    order: Vec<SortKey>,
    limit: Option<usize>,
    distinct: bool,
}

#[derive(Clone, Debug)]
struct SortKey {
    key: Expr,
    desc: bool,
    nulls_first: bool,
}

#[derive(Clone, Debug)]
enum Op {
    NodeScan {
        var: String,
        labels: Option<Vec<String>>,
        uids: Option<Vec<String>>,
        belief: BeliefMode,
        vt_mode: VtMode,
    },
    EdgeScan {
        var: String,
        rel_types: Option<Vec<String>>,
        belief: BeliefMode,
        vt_mode: VtMode,
    },
    Expand {
        from: String,
        into: String,
        hops: Hops,
        rel_type: Option<String>,
        dir: NeighborDir,
        edge_var: Option<String>,
    },
    Filter(Expr),
    TypeConstraint { var: String, labels: Vec<String> },
    Project { bindings: Vec<(String, Expr)>, keep_all: bool },
    Join { on: Vec<(String, String)>, kind: JoinKind },
    Aggregate { group_by: Vec<(String, Expr)>, aggs: Vec<Agg> },
    Order(Vec<SortKey>),
    Limit(usize),
    /// the bag union of inputs with one column list, in input order
    Union,
    /// per partition, the first `n` rows under the keys
    TopK { by: Vec<Expr>, keys: Vec<SortKey>, n: usize },
    /// a regular path step: every node a path spelling the expression reaches
    Walk { from: String, into: String, nfa: Nfa },
    /// shortest valid paths over an explicit edge relation (input 1)
    OrderedReach(Box<Reach>),
    /// a version's successor or predecessor along one time axis
    Succ { of: String, into: String, edge: bool, valid_axis: bool, next: bool, belief: BeliefMode, vt_mode: VtMode },
    /// the shared-instant rule: a variable-length step under one shared valid instant
    CoValidExpand(Box<covalid::CoValid>),
    /// the shared-instant rule: an optional (left outer) or not (anti) sub-pattern
    CoValidJoin(Box<covalid::CoJoin>),
    /// the shared-instant rule: one row per version tuple, each target at its least depth
    Collapse { by: Vec<String>, mins: Vec<String> },
}

/// `OrderedReach`'s fields: the edge relation's columns, the hop band, the
/// time order along the traversal, and the per-node truncation.
#[derive(Clone, Debug)]
struct Reach {
    from: String,
    into: String,
    src: String,
    dst: String,
    time: String,
    lo: u32,
    hi: Option<u32>,
    /// +1 increasing, -1 decreasing, 0 unconstrained
    sign: i8,
    strict: bool,
    limit: Option<usize>,
    key: Option<String>,
    key_desc: bool,
}

/// Thompson automaton of a path expression: one start, one accepting
/// state; `moves[q]` are `(relationship type, inverse, q')`.
#[derive(Clone, Debug)]
struct Nfa {
    start: usize,
    accept: usize,
    eps: Vec<Vec<usize>>,
    moves: Vec<Vec<(String, bool, usize)>>,
}

impl Nfa {
    fn parse(v: &J) -> DResult<Nfa> {
        let mut nfa = Nfa { start: 0, accept: 0, eps: Vec::new(), moves: Vec::new() };
        let (s, t) = nfa.build(v, 0)?;
        nfa.start = s;
        nfa.accept = t;
        Ok(nfa)
    }

    fn state(&mut self) -> usize {
        self.eps.push(Vec::new());
        self.moves.push(Vec::new());
        self.eps.len() - 1
    }

    fn build(&mut self, v: &J, depth: usize) -> DResult<(usize, usize)> {
        if depth > 256 {
            return unsupported("a path expression nested too deeply");
        }
        let o = v.as_object().map_or_else(|| unsupported("a path expression part"), Ok)?;
        if let Some(sym) = o.get("sym").and_then(J::as_str) {
            let inv = o.get("inv").and_then(J::as_bool).unwrap_or(false);
            let (s, t) = (self.state(), self.state());
            self.moves[s].push((sym.to_string(), inv, t));
            return Ok((s, t));
        }
        for k in ["seq", "alt"] {
            if let Some(parts) = o.get(k).and_then(J::as_array) {
                if parts.is_empty() {
                    return unsupported("an empty path expression list");
                }
                if k == "seq" {
                    let mut first = None;
                    let mut last = 0;
                    for p in parts {
                        let (s, t) = self.build(p, depth + 1)?;
                        match first {
                            None => first = Some(s),
                            Some(_) => self.eps[last].push(s),
                        }
                        last = t;
                    }
                    return Ok((first.unwrap_or(0), last));
                }
                let (s, t) = (self.state(), self.state());
                // an alternative written twice is one alternative: the same
                // language, so the same walk pairs, with fewer states to search
                for (i, p) in parts.iter().enumerate() {
                    if parts[..i].contains(p) {
                        continue;
                    }
                    let (ps, pt) = self.build(p, depth + 1)?;
                    self.eps[s].push(ps);
                    self.eps[pt].push(t);
                }
                return Ok((s, t));
            }
        }
        for k in ["star", "plus", "opt"] {
            if let Some(inner) = o.get(k) {
                let (s, t) = (self.state(), self.state());
                let (ps, pt) = self.build(inner, depth + 1)?;
                self.eps[s].push(ps);
                self.eps[pt].push(t);
                if k != "plus" {
                    self.eps[s].push(t);
                }
                if k != "opt" {
                    self.eps[pt].push(ps);
                }
                return Ok((s, t));
            }
        }
        unsupported("a path expression part")
    }

    /// The states reachable from `q` by empty moves, `q` included.
    fn closure(&self, q: usize) -> Vec<usize> {
        let mut seen = vec![false; self.eps.len()];
        let mut stack = vec![q];
        seen[q] = true;
        let mut out = Vec::new();
        while let Some(x) = stack.pop() {
            out.push(x);
            for &y in &self.eps[x] {
                if !seen[y] {
                    seen[y] = true;
                    stack.push(y);
                }
            }
        }
        out.sort_unstable();
        out
    }
}

#[derive(Clone, Debug)]
struct PNode {
    op: Op,
    inputs: Vec<usize>,
}

#[derive(Clone, Debug)]
struct Plan {
    nodes: Vec<PNode>,
    root: usize,
    t_a: i64,
    t_b: i64,
    as_of: i64,
    columns: Vec<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Tau {
    Int,
    Float,
    Ts,
    Str,
    Bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ArithOp {
    Add,
    Sub,
    Mul,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MathOp {
    Abs,
    Round,
    Floor,
    Ceil,
    Sqrt,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CalFn {
    Year,
    Month,
    Day,
    MonthDay,
    MonthsBetween,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StrOp {
    Contains,
    Like,
    Substring,
}

#[derive(Clone, Debug)]
enum Lit {
    Null,
    Int(i64),
    Float(f64),
    Bool(bool),
    Str(Arc<str>),
}

#[derive(Clone, Debug)]
enum Expr {
    Lit(Lit),
    Col(String),
    TProp { col: String, key: String, tau: Tau },
    Prop { col: String, key: String },
    Cmp(CmpOp, Box<Expr>, Box<Expr>),
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    Not(Box<Expr>),
    IsNull(Box<Expr>),
    Coalesce(Box<Expr>, Box<Expr>),
    If(Box<Expr>, Box<Expr>, Box<Expr>),
    CastInt(Box<Expr>),
    CastFloat(Box<Expr>),
    StartsWith(Box<Expr>, Box<Expr>),
    Arith(ArithOp, Box<Expr>, Box<Expr>),
    /// `/` under the blessed rule: an all-integer quotient is `q + r/d`,
    /// formed from the floor division; always a float column
    Div(Box<Expr>, Box<Expr>),
    FloorDiv(Box<Expr>, Box<Expr>),
    Math(MathOp, Box<Expr>),
    Calendar(CalFn, Box<Expr>, Option<Box<Expr>>),
    StrFn(StrOp, Vec<Expr>),
}

fn jstr<'a>(v: &'a J, key: &str) -> DResult<&'a str> {
    v.get(key)
        .and_then(J::as_str)
        .map_or_else(|| unsupported(format!("plan: missing string field {key:?}")), Ok)
}

fn jstrs(v: &J, key: &str) -> DResult<Option<Vec<String>>> {
    match v.get(key) {
        None | Some(J::Null) => Ok(None),
        Some(J::Array(a)) => a
            .iter()
            .map(|x| x.as_str().map(str::to_string))
            .collect::<Option<Vec<_>>>()
            .map(Some)
            .map_or_else(|| unsupported("plan: a list of strings"), Ok),
        _ => unsupported("plan: a list of strings"),
    }
}

fn parse_belief(s: &str) -> DResult<BeliefMode> {
    match s {
        "current" => Ok(BeliefMode::Current),
        "superseded" => Ok(BeliefMode::Superseded),
        "all" => Ok(BeliefMode::All),
        _ => unsupported(format!("belief {s:?}")),
    }
}

fn parse_vt_mode(s: &str) -> DResult<VtMode> {
    match s {
        "overlap" => Ok(VtMode::Overlap),
        "event" => Ok(VtMode::Event),
        _ => unsupported(format!("vt_mode {s:?}")),
    }
}

fn parse_tau(s: &str) -> DResult<Tau> {
    Ok(match s.trim_end_matches('?') {
        "int" => Tau::Int,
        "float" => Tau::Float,
        "ts" => Tau::Ts,
        "str" => Tau::Str,
        "bool" => Tau::Bool,
        other => return unsupported(format!("type {other:?}")),
    })
}

fn parse_expr(v: &J) -> DResult<Expr> {
    let o = match v.as_object() {
        Some(o) => o,
        None => return unsupported("an expression is an object"),
    };
    let b = |k: &str| -> DResult<Box<Expr>> {
        o.get(k).map_or_else(|| unsupported(format!("expression field {k}")), |x| parse_expr(x).map(Box::new))
    };
    let pair = |k: &str| -> DResult<(Box<Expr>, Box<Expr>)> {
        match o.get(k).and_then(J::as_array) {
            Some(a) if a.len() == 2 => Ok((Box::new(parse_expr(&a[0])?), Box::new(parse_expr(&a[1])?))),
            _ => unsupported(format!("expression pair {k}")),
        }
    };
    if let Some(l) = o.get("lit") {
        let tau = o.get("tau").and_then(J::as_str).unwrap_or("");
        return Ok(Expr::Lit(match l {
            J::Null => Lit::Null,
            J::Bool(x) => Lit::Bool(*x),
            J::String(s) => Lit::Str(Arc::from(s.as_str())),
            J::Number(n) => {
                if let Some(i) = n.as_i64() {
                    if tau.starts_with("float") {
                        Lit::Float(i as f64)
                    } else {
                        Lit::Int(i)
                    }
                } else if n.is_u64() {
                    return unsupported("an integer literal outside int64");
                } else {
                    Lit::Float(n.as_f64().unwrap_or(f64::NAN))
                }
            }
            _ => return unsupported("a literal"),
        }));
    }
    if let Some(c) = o.get("col") {
        return Ok(Expr::Col(c.as_str().map_or_else(|| unsupported("col"), |s| Ok(s.to_string()))?));
    }
    if let Some(p) = o.get("tprop") {
        let a = p.as_array().filter(|a| a.len() == 2);
        let (Some(c), Some(k)) = (a.and_then(|a| a[0].as_str()), a.and_then(|a| a[1].as_str())) else {
            return unsupported("tprop");
        };
        let tau = parse_tau(o.get("tau").and_then(J::as_str).unwrap_or(""))?;
        return Ok(Expr::TProp { col: c.to_string(), key: k.to_string(), tau });
    }
    if let Some(p) = o.get("prop") {
        let a = p.as_array().filter(|a| a.len() == 2);
        let (Some(c), Some(k)) = (a.and_then(|a| a[0].as_str()), a.and_then(|a| a[1].as_str())) else {
            return unsupported("prop");
        };
        return Ok(Expr::Prop { col: c.to_string(), key: k.to_string() });
    }
    if let Some(op) = o.get("cmp").and_then(J::as_str) {
        let op = match op {
            "=" => CmpOp::Eq,
            "!=" => CmpOp::Ne,
            "<" => CmpOp::Lt,
            "<=" => CmpOp::Le,
            ">" => CmpOp::Gt,
            ">=" => CmpOp::Ge,
            _ => return unsupported("cmp op"),
        };
        return Ok(Expr::Cmp(op, b("l")?, b("r")?));
    }
    if let Some(op) = o.get("bool").and_then(J::as_str) {
        return match op {
            "and" => Ok(Expr::And(b("l")?, b("r")?)),
            "or" => Ok(Expr::Or(b("l")?, b("r")?)),
            _ => unsupported("bool op"),
        };
    }
    if o.contains_key("not") {
        return Ok(Expr::Not(b("not")?));
    }
    if o.contains_key("is_null") {
        return Ok(Expr::IsNull(b("is_null")?));
    }
    if o.contains_key("coalesce") {
        let (l, r) = pair("coalesce")?;
        return Ok(Expr::Coalesce(l, r));
    }
    if o.contains_key("starts_with") {
        let (l, r) = pair("starts_with")?;
        return Ok(Expr::StartsWith(l, r));
    }
    if let Some(a) = o.get("if").and_then(J::as_array) {
        if a.len() == 3 {
            return Ok(Expr::If(
                Box::new(parse_expr(&a[0])?),
                Box::new(parse_expr(&a[1])?),
                Box::new(parse_expr(&a[2])?),
            ));
        }
        return unsupported("if");
    }
    if o.contains_key("cast") {
        return match o.get("to").and_then(J::as_str) {
            Some("int") | Some("ts") => Ok(Expr::CastInt(b("cast")?)),
            Some("float") => Ok(Expr::CastFloat(b("cast")?)),
            _ => unsupported("cast target"),
        };
    }
    if let Some(op) = o.get("arith").and_then(J::as_str) {
        let op = match op {
            "+" => ArithOp::Add,
            "-" => ArithOp::Sub,
            "*" => ArithOp::Mul,
            "/" => return Ok(Expr::Div(b("l")?, b("r")?)),
            "floor_div" => return Ok(Expr::FloorDiv(b("l")?, b("r")?)),
            _ => return unsupported(format!("arithmetic {op}")),
        };
        return Ok(Expr::Arith(op, b("l")?, b("r")?));
    }
    if let Some(f) = o.get("fn").and_then(J::as_str) {
        let op = match f {
            "abs" => MathOp::Abs,
            "round" => MathOp::Round,
            "floor" => MathOp::Floor,
            "ceil" => MathOp::Ceil,
            "sqrt" => MathOp::Sqrt,
            _ => return unsupported(format!("function {f}")),
        };
        return Ok(Expr::Math(op, b("arg")?));
    }
    if let Some(f) = o.get("calendar").and_then(J::as_str) {
        let op = match f {
            "year" => CalFn::Year,
            "month" => CalFn::Month,
            "day" => CalFn::Day,
            "month_day" => CalFn::MonthDay,
            "months_between" => CalFn::MonthsBetween,
            _ => return unsupported(format!("calendar {f}")),
        };
        let other = match o.get("other") {
            None | Some(J::Null) => None,
            Some(x) => Some(Box::new(parse_expr(x)?)),
        };
        if (op == CalFn::MonthsBetween) != other.is_some() {
            return unsupported("calendar operands");
        }
        return Ok(Expr::Calendar(op, b("arg")?, other));
    }
    if let Some(f) = o.get("strfn").and_then(J::as_str) {
        let (op, k) = match f {
            "contains" => (StrOp::Contains, 2),
            "like" => (StrOp::Like, 2),
            "substring" => (StrOp::Substring, 3),
            _ => return unsupported(format!("string function {f}")),
        };
        let args = match o.get("args").and_then(J::as_array) {
            Some(a) if a.len() == k => a.iter().map(parse_expr).collect::<DResult<Vec<_>>>()?,
            _ => return unsupported("string function operands"),
        };
        return Ok(Expr::StrFn(op, args));
    }
    unsupported(format!("expression {:?}", o.keys().collect::<Vec<_>>()))
}

fn parse_keys(v: Option<&J>) -> DResult<Vec<SortKey>> {
    let Some(arr) = v.and_then(J::as_array) else { return Ok(Vec::new()) };
    let mut keys = Vec::with_capacity(arr.len());
    for k in arr {
        keys.push(SortKey {
            key: parse_expr(k.get("key").unwrap_or(&J::Null))?,
            desc: jstr(k, "dir")? == "desc",
            nulls_first: jstr(k, "nulls")? == "nulls_first",
        });
    }
    Ok(keys)
}

fn parse_plan(text: &str) -> DResult<Plan> {
    let doc: J = serde_json::from_str(text).map_err(|e| Decline::Unsupported(format!("plan json: {e}")))?;
    if doc.get("format").and_then(J::as_u64) != Some(1) {
        return unsupported("plan format");
    }
    let sigma = doc.get("sigma").map_or_else(|| unsupported("sigma"), Ok)?;
    let tv = sigma.get("t_v").and_then(J::as_array).map_or_else(|| unsupported("t_v"), Ok)?;
    if tv.len() != 1 {
        return unsupported("more than one valid-time interval");
    }
    let iv = tv[0].as_array().filter(|a| a.len() == 2).map_or_else(|| unsupported("t_v"), Ok)?;
    let (t_a, t_b) = match (iv[0].as_i64(), iv[1].as_i64()) {
        (Some(a), Some(b)) => (a, b),
        _ => return unsupported("t_v bounds"),
    };
    let as_of = sigma.get("t_b").and_then(J::as_i64).map_or_else(|| unsupported("t_b"), Ok)?;
    let columns = jstrs(&doc, "columns")?.unwrap_or_default();
    let mut nodes = Vec::new();
    for n in doc.get("nodes").and_then(J::as_array).map_or_else(|| unsupported("nodes"), Ok)? {
        let inputs: Vec<usize> = n
            .get("inputs")
            .and_then(J::as_array)
            .map(|a| a.iter().filter_map(|x| x.as_u64().map(|v| v as usize)).collect())
            .unwrap_or_default();
        if inputs.iter().any(|&i| i >= nodes.len()) {
            return unsupported("plan inputs must precede their consumer");
        }
        let op = match jstr(n, "op")? {
            "NodeScan" => Op::NodeScan {
                var: jstr(n, "as")?.to_string(),
                labels: jstrs(n, "labels")?,
                uids: jstrs(n, "uids")?,
                belief: parse_belief(jstr(n, "belief")?)?,
                vt_mode: parse_vt_mode(jstr(n, "vt_mode")?)?,
            },
            "EdgeScan" => Op::EdgeScan {
                var: jstr(n, "as")?.to_string(),
                rel_types: jstrs(n, "rel_types")?,
                belief: parse_belief(jstr(n, "belief")?)?,
                vt_mode: parse_vt_mode(jstr(n, "vt_mode")?)?,
            },
            "Expand" => {
                let h = n.get("hops").map_or_else(|| unsupported("hops"), Ok)?;
                let hops = if let Some(k) = h.get("exact").and_then(J::as_u64) {
                    Hops::Exact(k as u32)
                } else if let Some(ab) = h.get("bounded").and_then(J::as_array) {
                    match (ab.first().and_then(J::as_u64), ab.get(1).and_then(J::as_u64)) {
                        (Some(a), Some(b)) => Hops::Bounded(a as u32, b as u32),
                        _ => return unsupported("bounded hops"),
                    }
                } else if let Some(a) = h.get("unbounded").and_then(J::as_u64) {
                    Hops::Unbounded(a as u32)
                } else {
                    return unsupported("hops");
                };
                let dir = match jstr(n, "dir")? {
                    "out" => NeighborDir::Out,
                    "in" => NeighborDir::In,
                    "both" => NeighborDir::Both,
                    _ => return unsupported("dir"),
                };
                Op::Expand {
                    from: jstr(n, "from")?.to_string(),
                    into: jstr(n, "into")?.to_string(),
                    hops,
                    rel_type: n.get("rel_type").and_then(J::as_str).map(str::to_string),
                    dir,
                    edge_var: n.get("edge_var").and_then(J::as_str).map(str::to_string),
                }
            }
            "Filter" => Op::Filter(parse_expr(n.get("pred").unwrap_or(&J::Null))?),
            "TypeConstraint" => Op::TypeConstraint {
                var: jstr(n, "var")?.to_string(),
                labels: jstrs(n, "labels")?.map_or_else(|| unsupported("labels"), Ok)?,
            },
            "Project" => {
                let mut bindings = Vec::new();
                for b in n.get("bindings").and_then(J::as_array).map_or_else(|| unsupported("bindings"), Ok)? {
                    let a = b.as_array().filter(|a| a.len() == 2).map_or_else(|| unsupported("binding"), Ok)?;
                    let name = a[0].as_str().map_or_else(|| unsupported("binding name"), Ok)?;
                    bindings.push((name.to_string(), parse_expr(&a[1])?));
                }
                Op::Project { bindings, keep_all: jstr(n, "keep")? == "all" }
            }
            "Join" => {
                let mut on = Vec::new();
                for p in n.get("on").and_then(J::as_array).map_or_else(|| unsupported("on"), Ok)? {
                    let a = p.as_array().filter(|a| a.len() == 2).map_or_else(|| unsupported("on"), Ok)?;
                    match (a[0].as_str(), a[1].as_str()) {
                        (Some(l), Some(r)) => on.push((l.to_string(), r.to_string())),
                        _ => return unsupported("on"),
                    }
                }
                let kind = match jstr(n, "type")? {
                    "inner" => JoinKind::Inner,
                    "left_outer" => JoinKind::LeftOuter,
                    "anti" => JoinKind::Anti,
                    _ => return unsupported("join type"),
                };
                Op::Join { on, kind }
            }
            "Aggregate" => {
                let mut group_by = Vec::new();
                for b in n.get("group_by").and_then(J::as_array).map_or_else(|| unsupported("group_by"), Ok)? {
                    let a = b.as_array().filter(|a| a.len() == 2).map_or_else(|| unsupported("group key"), Ok)?;
                    let name = a[0].as_str().map_or_else(|| unsupported("group key name"), Ok)?;
                    group_by.push((name.to_string(), parse_expr(&a[1])?));
                }
                let mut aggs = Vec::new();
                for a in n.get("aggregates").and_then(J::as_array).map_or_else(|| unsupported("aggregates"), Ok)? {
                    let func = match jstr(a, "fn")? {
                        "count" => AggFn::Count,
                        "count_distinct" => AggFn::CountDistinct,
                        "sum_exact" => AggFn::SumExact,
                        "min" => AggFn::Min,
                        "max" => AggFn::Max,
                        "mean" => AggFn::Mean,
                        "collect" => AggFn::Collect,
                        other => return unsupported(format!("aggregate {other}")),
                    };
                    let mut items = Vec::new();
                    let of = match (a.get("of"), func) {
                        (Some(e), AggFn::Collect) => {
                            let parts = e.get("tuple").and_then(J::as_array).map_or_else(|| unsupported("collect of"), Ok)?;
                            items = parts.iter().map(parse_expr).collect::<DResult<Vec<_>>>()?;
                            None
                        }
                        (None | Some(J::Null), _) => None,
                        (Some(e), _) => Some(parse_expr(e)?),
                    };
                    aggs.push(Agg {
                        func,
                        alias: jstr(a, "alias")?.to_string(),
                        of,
                        items,
                        order: parse_keys(a.get("order"))?,
                        limit: a.get("limit").and_then(J::as_u64).map(|v| v as usize),
                        distinct: a.get("distinct").and_then(J::as_bool).unwrap_or(false),
                    });
                }
                Op::Aggregate { group_by, aggs }
            }
            "Order" => Op::Order(parse_keys(n.get("keys"))?),
            "Succ" => Op::Succ {
                of: jstr(n, "of")?.to_string(),
                into: jstr(n, "into")?.to_string(),
                edge: jstr(n, "kind")? == "edge",
                valid_axis: jstr(n, "axis")? == "valid",
                next: jstr(n, "dir")? == "next",
                belief: parse_belief(jstr(n, "belief")?)?,
                vt_mode: parse_vt_mode(jstr(n, "vt_mode")?)?,
            },
            "OrderedReach" => {
                if inputs.len() != 2 {
                    return unsupported("OrderedReach takes the seeds and the edges");
                }
                let order = jstr(n, "order")?;
                let (sign, strict) = match order {
                    "none" => (0, false),
                    "strict_increasing" => (1, true),
                    "increasing" => (1, false),
                    "strict_decreasing" => (-1, true),
                    "decreasing" => (-1, false),
                    _ => return unsupported("reach order"),
                };
                Op::OrderedReach(Box::new(Reach {
                    from: jstr(n, "from")?.to_string(),
                    into: jstr(n, "into")?.to_string(),
                    src: jstr(n, "src")?.to_string(),
                    dst: jstr(n, "dst")?.to_string(),
                    time: jstr(n, "time")?.to_string(),
                    lo: n.get("lo").and_then(J::as_u64).unwrap_or(1) as u32,
                    hi: n.get("hi").and_then(J::as_u64).map(|v| v as u32),
                    sign,
                    strict,
                    limit: n.get("limit").and_then(J::as_u64).map(|v| v as usize),
                    key: n.get("key").and_then(J::as_str).map(str::to_string),
                    key_desc: n.get("key_desc").and_then(J::as_bool).unwrap_or(false),
                }))
            }
            "Walk" => Op::Walk {
                from: jstr(n, "from")?.to_string(),
                into: jstr(n, "into")?.to_string(),
                nfa: Nfa::parse(n.get("regex").unwrap_or(&J::Null))?,
            },
            "TopK" => {
                let by = n
                    .get("by")
                    .and_then(J::as_array)
                    .map_or_else(|| unsupported("TopK by"), Ok)?
                    .iter()
                    .map(parse_expr)
                    .collect::<DResult<Vec<_>>>()?;
                Op::TopK {
                    by,
                    keys: parse_keys(n.get("keys"))?,
                    n: n.get("n").and_then(J::as_u64).map_or_else(|| unsupported("TopK n"), Ok)? as usize,
                }
            }
            "Limit" => Op::Limit(n.get("n").and_then(J::as_u64).map_or_else(|| unsupported("limit n"), Ok)? as usize),
            "Union" => {
                if inputs.len() < 2 {
                    return unsupported("a union of fewer than two inputs");
                }
                Op::Union
            }
            other => match covalid::parse_op(other, n, inputs.len())? {
                Some(op) => op,
                None => return unsupported(format!("node {other}")),
            },
        };
        nodes.push(PNode { op, inputs });
    }
    let root = doc.get("root").and_then(J::as_u64).map_or_else(|| unsupported("root"), Ok)? as usize;
    if root >= nodes.len() {
        return unsupported("root");
    }
    Ok(Plan { nodes, root, t_a, t_b, as_of: crate::clamp_tt(as_of), columns })
}

// --------------------------------------------------------------------------
// segments
// --------------------------------------------------------------------------

/// A version address: segment index in the call's table, and row.
type Addr = u64;
const NO_ADDR: Addr = u64::MAX;
const NO_ID: u32 = u32::MAX;

#[inline]
fn addr(seg: usize, row: u32) -> Addr {
    ((seg as u64) << 32) | row as u64
}

#[inline]
fn seg_of(a: Addr) -> usize {
    (a >> 32) as usize
}

#[inline]
fn row_of(a: Addr) -> usize {
    (a & 0xffff_ffff) as usize
}

/// One segment's columns, borrowed once per call.
struct SegCols<'a> {
    seg: &'a Segment<MmapSource>,
    id: u64,
    vt_s: &'a [i64],
    vt_e: Option<&'a [i64]>,
    vid_hi: &'a [u64],
    vid_lo: &'a [u32],
    props: &'a [u32],
    /// node: `uid_id`; edge: `src_id`
    a: &'a [u32],
    /// edge: `dst_id`
    b: &'a [u32],
    /// node: `label_ref`; edge: `disc_ref`
    tag: &'a [u32],
    /// edge: segment-local rel code -> global rel code
    rel: &'a [u16],
    rel_map: Vec<u32>,
    /// node: label string ref -> global label code (lazily)
    tt_view: crate::ttime::TtSView<'a>,
}

impl<'a> SegCols<'a> {
    fn open(seg: &'a Segment<MmapSource>, id: u64, kind: RowKind, rels: &mut Interner) -> Result<Self> {
        let h = seg.header();
        let vt_e = if h.vt_e_elided { None } else { Some(seg.i64_column("vt_e")?) };
        let (a, b, tag, rel) = match kind {
            RowKind::Node => (seg.u32_column("uid_id")?, &[][..], seg.u32_column("label_ref")?, &[][..]),
            RowKind::Edge => (
                seg.u32_column("src_id")?,
                seg.u32_column("dst_id")?,
                seg.u32_column("disc_ref")?,
                seg.u16_column("rel_code")?,
            ),
        };
        let rel_map = h.rel_types.iter().map(|r| rels.code(r)).collect();
        Ok(SegCols {
            seg,
            id,
            vt_s: seg.i64_column("vt_s")?,
            vt_e,
            vid_hi: seg.u64_column("vid64")?,
            vid_lo: seg.u32_column("vid_lo32")?,
            props: seg.u32_column("props_ref")?,
            a,
            b,
            tag,
            rel,
            rel_map,
            tt_view: seg.tt_s_view()?,
        })
    }

    #[inline]
    fn vt_e(&self, row: usize) -> i64 {
        match self.vt_e {
            Some(c) => c[row],
            None => self.vt_s[row] + 1,
        }
    }
}

/// A small string interner (labels, rel names, disc texts, group-key text).
#[derive(Default)]
struct Interner {
    by: HashMap<Arc<str>, u32>,
    all: Vec<Arc<str>>,
}

impl Interner {
    fn code(&mut self, s: &str) -> u32 {
        if let Some(&c) = self.by.get(s) {
            return c;
        }
        let c = self.all.len() as u32;
        let a: Arc<str> = Arc::from(s);
        self.all.push(a.clone());
        self.by.insert(a, c);
        c
    }
    fn get(&self, s: &str) -> Option<u32> {
        self.by.get(s).copied()
    }
    fn text(&self, c: u32) -> &Arc<str> {
        &self.all[c as usize]
    }
}

/// A property value at a declared type, from one parsed payload.
#[derive(Clone, Debug)]
enum TVal {
    Absent,
    Int(i64),
    /// a JSON integer outside int64
    BigInt,
    Float(f64),
    Bool(bool),
    Str(Arc<str>),
    Other,
}

fn tval_of(v: Option<&J>) -> TVal {
    match v {
        None | Some(J::Null) => TVal::Absent,
        Some(J::Bool(b)) => TVal::Bool(*b),
        Some(J::String(s)) => TVal::Str(Arc::from(s.as_str())),
        Some(J::Number(n)) => {
            if let Some(i) = n.as_i64() {
                TVal::Int(i)
            } else if n.is_u64() {
                TVal::BigInt
            } else {
                TVal::Float(n.as_f64().unwrap_or(f64::NAN))
            }
        }
        Some(_) => TVal::Other,
    }
}

/// A cached node version map: its key (generation, as_of, instant) and the
/// map, addressed by index into that generation's node segment list.
pub type NodeMapEntry = ((u64, i64, i64), Arc<Vec<Addr>>);
/// One generation's node versions per label.
pub type LabelCounts = (u64, Arc<Vec<(String, u64)>>);
/// One node segment's rows, by label (labels sorted).
pub type LabelRows = Arc<Vec<(String, Vec<u32>)>>;
/// Per node segment id, its rows by label.
pub type LabelRowsCache = HashMap<u64, LabelRows>;
/// One edge segment's rare relationships with their rows, and the names of
/// the others.
pub type RelRows = Arc<(Vec<(String, Vec<u32>)>, Vec<String>)>;
/// Per edge segment id, its rare relationships' rows.
pub type RelRowsCache = HashMap<u64, RelRows>;
/// A relationship is rare in a segment while it holds at most this fraction
/// of the segment's rows; only rare relationships get row lists.
const RARE_REL_FRACTION: usize = 32;
/// A hop reads a rare relationship's rows instead of the adjacency index
/// from this many frontier nodes on (below it the index's slices are cheap).
const REL_ROWS_FRONTIER_MIN: usize = 64;

/// One (kind, segment, key)'s typed values, by string-table index.
///
/// A table starts **sparse**: the slots read so far, by index. It turns
/// dense -- one slot per string of the segment, read by position -- once a
/// call has filled `1 / PROP_DENSE_FRACTION` of them, which a scan over the
/// segment does in its first rows. A point read (a FinBench account, an SNB person)
/// used to build the dense vector on its first touch of a segment and again
/// whenever the cache had dropped it: one slot written per distinct props
/// string of the segment for one row's value, 4-6 ms a call on FinBench SF1
/// r10's large node segments -- the pilot's TSR1 floor (6.2 ms against
/// 0.7 ms with the table cached). The bytes are kept as the table fills, so
/// the cache's accounting never walks it: a slot is filled once.
#[derive(Clone, Default)]
struct PropTable {
    /// the segment's string count: the dense length, and the index bound
    len: usize,
    dense: Option<Vec<Option<TVal>>>,
    sparse: FxMap<u32, TVal>,
    str_bytes: usize,
}

/// A sparse table turns dense once it holds this share of its slots.
const PROP_DENSE_FRACTION: usize = 16;
/// ...or, as it goes back to the handle's cache, once it holds this many: a
/// warm scan that read them (T1's 4,000 edges) then reads by position, while a
/// point read's table (a FinBench account: a few slots) stays sparse.
const PROP_DENSE_ON_PUT: usize = 1_024;

impl PropTable {
    fn new(len: usize) -> Self {
        PropTable { len, ..Default::default() }
    }

    fn get(&self, r: usize) -> Option<&TVal> {
        match &self.dense {
            Some(d) => d.get(r).and_then(Option::as_ref),
            None => self.sparse.get(&(r as u32)),
        }
    }

    /// Turn dense if the table holds `PROP_DENSE_ON_PUT` slots or more.
    fn settle(&mut self) {
        if self.dense.is_none() && self.sparse.len() >= PROP_DENSE_ON_PUT {
            let mut d: Vec<Option<TVal>> = vec![None; self.len];
            for (i, x) in self.sparse.drain() {
                d[i as usize] = Some(x);
            }
            self.dense = Some(d);
        }
    }

    /// Fill slot `r` (`r < len`), which is empty.
    fn fill(&mut self, r: usize, v: TVal) {
        if let TVal::Str(x) = &v {
            self.str_bytes += x.len();
        }
        if let Some(d) = &mut self.dense {
            d[r] = Some(v);
            return;
        }
        self.sparse.insert(r as u32, v);
        if self.sparse.len().saturating_mul(PROP_DENSE_FRACTION) > self.len {
            let mut d: Vec<Option<TVal>> = vec![None; self.len];
            for (i, x) in self.sparse.drain() {
                d[i as usize] = Some(x);
            }
            self.dense = Some(d);
        }
    }
}

/// The handle's typed property columns, shared by every `match` call:
/// keyed by segment **id** (immutable files), so they survive commits and
/// are dropped with their segment. A table is taken out while a call fills
/// it and put back after; past the byte budget the cache starts over.
#[derive(Default)]
pub struct PropsCache {
    inner: std::sync::Mutex<PropsInner>,
}

#[derive(Default)]
struct PropsInner {
    tables: HashMap<(u8, u64, Arc<str>), PropTable>,
    bytes: u64,
}

/// `GNOMON_MATCH_PROPS_BYTES` (default 1 GiB): the typed columns' budget.
fn props_budget() -> u64 {
    std::env::var("GNOMON_MATCH_PROPS_BYTES")
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(1 << 30)
}

fn table_bytes(t: &PropTable) -> u64 {
    let slots = match &t.dense {
        Some(d) => d.len() * std::mem::size_of::<Option<TVal>>(),
        None => t.sparse.len() * (std::mem::size_of::<(u32, TVal)>() + 8),
    };
    (slots + t.str_bytes) as u64
}

impl PropsCache {
    fn take(&self, key: &(u8, u64, Arc<str>)) -> Option<PropTable> {
        let mut g = self.inner.lock().ok()?;
        let t = g.tables.remove(key)?;
        g.bytes = g.bytes.saturating_sub(table_bytes(&t));
        Some(t)
    }

    fn put(&self, key: (u8, u64, Arc<str>), t: PropTable) {
        let Ok(mut g) = self.inner.lock() else { return };
        let b = table_bytes(&t);
        if g.bytes + b > props_budget() {
            g.tables.clear();
            g.bytes = 0;
        }
        g.bytes += b;
        if let Some(old) = g.tables.insert(key, t) {
            g.bytes = g.bytes.saturating_sub(table_bytes(&old));
        }
    }

    pub(crate) fn retain_segments(&self, keep: &std::collections::HashSet<u64>) {
        if let Ok(mut g) = self.inner.lock() {
            g.tables.retain(|k, _| keep.contains(&k.1));
            let total: u64 = g.tables.values().map(table_bytes).sum();
            g.bytes = total;
        }
    }

    /// `(tables, bytes)` held.
    pub fn stats(&self) -> (usize, u64) {
        self.inner.lock().map(|g| (g.tables.len(), g.bytes)).unwrap_or((0, 0))
    }
}

// --------------------------------------------------------------------------
// values and columns
// --------------------------------------------------------------------------

/// A typed column. Nulls ride in `Col::null`.
#[derive(Clone, Debug)]
enum Vals {
    /// every value null, no type
    Null(usize),
    I64(Vec<i64>),
    F64(Vec<f64>),
    Bool(Vec<bool>),
    Str(Vec<Arc<str>>),
    /// node identities (dense ids)
    Uid(Vec<u32>),
    /// version ids (hi, lo): ordered as their hex text is
    Vid(Vec<(u64, u32)>),
    /// edge identities: (version address of a row carrying it, edge kind)
    Eid(Vec<Addr>),
    /// JSON values: (version address, node? edge?), and the key read, if any
    Json(Vec<Addr>, RowKind, Option<Arc<str>>),
    /// labels and rel names, as interner codes
    Label(Vec<u32>),
    RelType(Vec<u32>),
    /// computed JSON values (a collected list), as text
    JText(Vec<Arc<str>>),
}

#[derive(Clone, Debug)]
struct Col {
    v: Vals,
    null: Option<Vec<bool>>,
}

impl Col {
    fn len(&self) -> usize {
        match &self.v {
            Vals::Null(n) => *n,
            Vals::I64(x) => x.len(),
            Vals::F64(x) => x.len(),
            Vals::Bool(x) => x.len(),
            Vals::Str(x) => x.len(),
            Vals::Uid(x) => x.len(),
            Vals::Vid(x) => x.len(),
            Vals::Eid(x) => x.len(),
            Vals::Json(x, _, _) => x.len(),
            Vals::Label(x) => x.len(),
            Vals::RelType(x) => x.len(),
            Vals::JText(x) => x.len(),
        }
    }
    #[inline]
    fn is_null(&self, i: usize) -> bool {
        matches!(self.v, Vals::Null(_)) || self.null.as_ref().is_some_and(|m| m[i])
    }
    fn take(&self, idx: &[usize]) -> Col {
        fn g<T: Clone + Send + Sync>(v: &[T], idx: &[usize]) -> Vec<T> {
            gather(v, idx)
        }
        // shared text: cloning an Arc on many threads at once contends on its
        // count, so these gather on one thread
        fn g1<T: Clone>(v: &[T], idx: &[usize]) -> Vec<T> {
            idx.iter().map(|&i| v[i].clone()).collect()
        }
        let v = match &self.v {
            Vals::Null(_) => Vals::Null(idx.len()),
            Vals::I64(x) => Vals::I64(g(x, idx)),
            Vals::F64(x) => Vals::F64(g(x, idx)),
            Vals::Bool(x) => Vals::Bool(g(x, idx)),
            Vals::Str(x) => Vals::Str(g1(x, idx)),
            Vals::Uid(x) => Vals::Uid(g(x, idx)),
            Vals::Vid(x) => Vals::Vid(g(x, idx)),
            Vals::Eid(x) => Vals::Eid(g(x, idx)),
            Vals::Json(x, k, key) => Vals::Json(g(x, idx), *k, key.clone()),
            Vals::Label(x) => Vals::Label(g(x, idx)),
            Vals::RelType(x) => Vals::RelType(g(x, idx)),
            Vals::JText(x) => Vals::JText(g1(x, idx)),
        };
        Col { v, null: self.null.as_ref().map(|m| g(m, idx)) }
    }
    /// `take`, with `None` positions null (the outer side of a join).
    fn take_opt(&self, idx: &[Option<usize>]) -> Col {
        let any_none = idx.iter().any(Option::is_none);
        let dense: Vec<usize> = idx.iter().map(|i| i.unwrap_or(0)).collect();
        if self.len() == 0 {
            return Col { v: Vals::Null(idx.len()), null: None };
        }
        let mut out = self.take(&dense);
        if any_none {
            let mut m = out.null.take().unwrap_or_else(|| vec![false; idx.len()]);
            for (j, i) in idx.iter().enumerate() {
                if i.is_none() {
                    m[j] = true;
                }
            }
            out.null = Some(m);
        }
        out
    }
}

/// A pattern variable's binding column.
#[derive(Clone, Debug)]
enum Var {
    /// node: dense ids (`NO_ID` = null) and version addresses (`NO_ADDR` =
    /// no version under Σ); `depth` for a variable-length target
    Node { ids: Vec<u32>, ver: Vec<Addr>, depth: Option<Vec<u32>> },
    /// edge: version addresses (`NO_ADDR` = null)
    Edge { ver: Vec<Addr> },
}

impl Var {
    fn take_opt(&self, idx: &[Option<usize>]) -> Var {
        match self {
            Var::Node { ids, ver, depth } => Var::Node {
                ids: idx.iter().map(|i| i.map_or(NO_ID, |i| ids[i])).collect(),
                ver: idx.iter().map(|i| i.map_or(NO_ADDR, |i| ver[i])).collect(),
                depth: depth.as_ref().map(|d| idx.iter().map(|i| i.map_or(0, |i| d[i])).collect()),
            },
            Var::Edge { ver } => Var::Edge { ver: idx.iter().map(|i| i.map_or(NO_ADDR, |i| ver[i])).collect() },
        }
    }
    fn take(&self, idx: &[usize]) -> Var {
        match self {
            Var::Node { ids, ver, depth } => Var::Node {
                ids: gather(ids, idx),
                ver: gather(ver, idx),
                depth: depth.as_ref().map(|d| gather(d, idx)),
            },
            Var::Edge { ver } => Var::Edge { ver: gather(ver, idx) },
        }
    }
}

/// A relation: variables and computed columns, `n` rows, in order.
#[derive(Clone, Debug, Default)]
struct Rel {
    n: usize,
    vars: Vec<(String, Var)>,
    cols: Vec<(String, Col)>,
}

impl Rel {
    fn take(&self, idx: &[usize]) -> Rel {
        Rel {
            n: idx.len(),
            vars: self.vars.iter().map(|(k, v)| (k.clone(), v.take(idx))).collect(),
            cols: self.cols.iter().map(|(k, c)| (k.clone(), c.take(idx))).collect(),
        }
    }
    fn take_opt(&self, idx: &[Option<usize>]) -> Rel {
        Rel {
            n: idx.len(),
            vars: self.vars.iter().map(|(k, v)| (k.clone(), v.take_opt(idx))).collect(),
            cols: self.cols.iter().map(|(k, c)| (k.clone(), c.take_opt(idx))).collect(),
        }
    }
    fn var(&self, name: &str) -> Option<&Var> {
        self.vars.iter().find(|(k, _)| k == name).map(|(_, v)| v)
    }
    fn col(&self, name: &str) -> Option<&Col> {
        self.cols.iter().find(|(k, _)| k == name).map(|(_, c)| c)
    }
    fn set_col(&mut self, name: &str, c: Col) {
        if let Some(slot) = self.cols.iter_mut().find(|(k, _)| k == name) {
            slot.1 = c;
        } else {
            self.cols.push((name.to_string(), c));
        }
    }
}

thread_local! {
    /// The read kernels' thread count (`Designs::match_threads`) for the
    /// match call running on this thread, so a gather deep in the executor
    /// reads it without a handle; 1 outside a call.
    static MATCH_THREADS: std::cell::Cell<usize> = const { std::cell::Cell::new(1) };
    /// The rows from which that call's row loops split (`Designs::match_par_rows`).
    static MATCH_PAR_ROWS: std::cell::Cell<usize> = const { std::cell::Cell::new(1 << 17) };
}

/// Sets this thread's match thread count and split for one call; restores
/// them on drop.
struct ThreadsGuard(usize, usize);

impl ThreadsGuard {
    fn set(n: usize, rows: usize) -> ThreadsGuard {
        ThreadsGuard(
            MATCH_THREADS.with(|c| c.replace(n.max(1))),
            MATCH_PAR_ROWS.with(|c| c.replace(rows.max(1))),
        )
    }
}

impl Drop for ThreadsGuard {
    fn drop(&mut self) {
        MATCH_THREADS.with(|c| c.set(self.0));
        MATCH_PAR_ROWS.with(|c| c.set(self.1));
    }
}

/// The rows from which a row loop runs in chunks on the pool, for this call.
fn par_min() -> usize {
    MATCH_PAR_ROWS.with(|c| c.get())
}

/// Below this many values a distinct set is sorted, not read off a bitset.
const PAR_ROWS_MIN: usize = 1 << 15;

/// `f` over `0..n` in contiguous chunks on the pool (from `par_min()` rows,
/// at most the call's thread count of them), their outputs
/// concatenated in chunk order: the serial loop's output, in its order,
/// whatever the thread count.
fn par_concat<T: Send>(n: usize, f: impl Fn(std::ops::Range<usize>) -> Vec<T> + Sync) -> Vec<T> {
    par_concat_from(n, par_min(), f)
}

/// `par_concat` over `n` items each worth about `par_min() / from` rows:
/// in chunks on the pool from `from` items on.
fn par_concat_from<T: Send>(n: usize, from: usize, f: impl Fn(std::ops::Range<usize>) -> Vec<T> + Sync) -> Vec<T> {
    let threads = MATCH_THREADS.with(|c| c.get());
    let width = if threads > 1 && n >= from { threads.min(n / (from / 2).max(1)) } else { 1 };
    if width <= 1 {
        return f(0..n);
    }
    let size = n.div_ceil(width);
    let parts = crate::pool::run(width, |w| f((w * size).min(n)..((w + 1) * size).min(n)));
    let mut out = Vec::with_capacity(parts.iter().map(Vec::len).sum());
    for p in parts {
        out.extend(p);
    }
    out
}

/// `par_concat` for three outputs at once, each concatenated in chunk order.
#[allow(clippy::type_complexity)]
fn par_concat3<A: Send, B: Send, C: Send>(
    n: usize,
    f: impl Fn(std::ops::Range<usize>) -> (Vec<A>, Vec<B>, Vec<C>) + Sync,
) -> (Vec<A>, Vec<B>, Vec<C>) {
    let threads = MATCH_THREADS.with(|c| c.get());
    let min = par_min();
    let width = if threads > 1 && n >= min { threads.min(n / (min / 2).max(1)) } else { 1 };
    if width <= 1 {
        return f(0..n);
    }
    let size = n.div_ceil(width);
    let parts = crate::pool::run(width, |w| f((w * size).min(n)..((w + 1) * size).min(n)));
    let total = parts.iter().map(|p| p.0.len()).sum();
    let (mut a, mut b, mut c) = (Vec::with_capacity(total), Vec::with_capacity(total), Vec::with_capacity(total));
    for (x, y, z) in parts {
        a.extend(x);
        b.extend(y);
        c.extend(z);
    }
    (a, b, c)
}

/// `par_concat` for a fallible `f`: the first chunk's error in chunk order,
/// which is the serial loop's first error.
fn par_try_concat<T: Send>(n: usize, f: impl Fn(std::ops::Range<usize>) -> DResult<Vec<T>> + Sync) -> DResult<Vec<T>> {
    let threads = MATCH_THREADS.with(|c| c.get());
    let min = par_min();
    let width = if threads > 1 && n >= min { threads.min(n / (min / 2).max(1)) } else { 1 };
    if width <= 1 {
        return f(0..n);
    }
    let size = n.div_ceil(width);
    let parts = crate::pool::run(width, |w| f((w * size).min(n)..((w + 1) * size).min(n)));
    let mut out = Vec::with_capacity(n);
    for p in parts {
        out.extend(p?);
    }
    Ok(out)
}

/// `v` sorted by `cmp`, a total order (no two positions compare equal): in
/// chunks sorted on the pool and merged pairwise, which under a total order
/// is the one sorted sequence whatever the thread count.
fn par_sort_by(v: &mut Vec<usize>, cmp: &(impl Fn(&usize, &usize) -> Ordering + Sync)) {
    let threads = MATCH_THREADS.with(|c| c.get());
    let n = v.len();
    let min = par_min();
    let width = if threads > 1 && n >= 2 * min { threads.min(n / min) } else { 1 };
    if width <= 1 {
        v.sort_by(cmp);
        return;
    }
    let size = n.div_ceil(width);
    let src: &[usize] = v;
    let mut runs: Vec<Vec<usize>> = crate::pool::run(width, |w| {
        let mut c = src[(w * size).min(n)..((w + 1) * size).min(n)].to_vec();
        c.sort_by(cmp);
        c
    });
    while runs.len() > 1 {
        let rs = &runs;
        runs = crate::pool::run(runs.len().div_ceil(2), |p| match rs.get(2 * p + 1) {
            None => rs[2 * p].clone(),
            Some(b) => {
                let a = &rs[2 * p];
                let mut out = Vec::with_capacity(a.len() + b.len());
                let (mut i, mut j) = (0, 0);
                while i < a.len() && j < b.len() {
                    if cmp(&a[i], &b[j]) == Ordering::Greater {
                        out.push(b[j]);
                        j += 1;
                    } else {
                        out.push(a[i]);
                        i += 1;
                    }
                }
                out.extend_from_slice(&a[i..]);
                out.extend_from_slice(&b[j..]);
                out
            }
        });
    }
    *v = runs.pop().unwrap_or_default();
}

/// `v[i]` for each `i` of `idx`, in order.
fn gather<T: Clone + Send + Sync>(v: &[T], idx: &[usize]) -> Vec<T> {
    par_concat(idx.len(), |r| idx[r].iter().map(|&i| v[i].clone()).collect())
}

/// The distinct values of `v`, ascending: by a bitset over the values'
/// range when it is dense enough, else by sorting.
fn sorted_distinct(mut v: Vec<u32>) -> Vec<u32> {
    let max = v.iter().copied().max().unwrap_or(0) as usize;
    if v.len() < PAR_ROWS_MIN || max / 64 > v.len() {
        v.sort_unstable();
        v.dedup();
        return v;
    }
    let mut bits = vec![0u64; max / 64 + 1];
    for &x in &v {
        bits[x as usize / 64] |= 1 << (x % 64);
    }
    let mut out = Vec::with_capacity(bits.iter().map(|w| w.count_ones() as usize).sum());
    for (w, &word) in bits.iter().enumerate() {
        let mut b = word;
        while b != 0 {
            out.push((w * 64) as u32 + b.trailing_zeros());
            b &= b - 1;
        }
    }
    out
}

/// Rows from which a binding table drops the columns no later operator names.
const DEAD_COLS_MIN: usize = 4096;

/// A believed scan is cut into pieces of this many rows for the pool.
const SCAN_PIECE: usize = 1 << 16;

fn check_rows(n: usize) -> DResult<()> {
    if n > MAX_ROWS {
        return Err(Decline::Engine(EngineError::invariant(format!(
            "match: an intermediate binding table of {n} rows exceeds the executor's \
             {MAX_ROWS}-row budget; it refuses rather than truncate"
        ))));
    }
    Ok(())
}

// --------------------------------------------------------------------------
// the execution context
// --------------------------------------------------------------------------

struct Ctx<'s, 'a> {
    store: &'s NativeStore,
    plan: &'a Plan,
    nodes: Vec<SegCols<'a>>,
    edges: Vec<SegCols<'a>>,
    node_by_id: FxMap<u64, usize>,
    edge_by_id: FxMap<u64, usize>,
    closes: Arc<CloseIndex>,
    rels: Interner,
    labels: Interner,
    /// per node segment: label string ref -> label code (`u32::MAX` unset)
    label_codes: Vec<Vec<u32>>,
    /// (kind, seg, key) -> the typed value of each interned payload, by its
    /// string-table index; filled as rows ask (each payload parsed once)
    props: FxMap<(u8, usize, Arc<str>), PropTable>,
    /// the node version map of Σ (dense id -> address), built on demand
    node_map: Option<Arc<Vec<Addr>>>,
    /// expansion node -> the semi-join a later closing step implies for its
    /// target (computed once per plan, see `semi_hints`)
    semi: Option<FxMap<usize, SemiHint>>,
    /// node -> the text of every operator downstream of it and the answer's
    /// columns (`dead_cols`), built on demand
    downstream: FxMap<usize, String>,
    /// relationship list -> the believed edge rows of the call's scope
    /// (`edge_rows_estimate`), once per call
    edge_est: std::sync::Mutex<FxMap<Option<Vec<String>>, usize>>,
}

/// A later closing step `X -[S, dir]-> T` over the variable `X` an
/// expansion binds, with `T` bound before that expansion: `X` may only take
/// neighbours of `T` (reverse direction, relation `S`). Filtering early drops
/// exactly the rows the closing step would drop, before they are made.
#[derive(Clone, Debug)]
struct SemiHint {
    target: String,
    rel: Option<String>,
    dir: NeighborDir,
}

impl<'s, 'a> Ctx<'s, 'a> {
    fn seg(&self, kind: RowKind, a: Addr) -> &SegCols<'a> {
        match kind {
            RowKind::Node => &self.nodes[seg_of(a)],
            RowKind::Edge => &self.edges[seg_of(a)],
        }
    }

    fn tt_s(&self, kind: RowKind, a: Addr) -> Result<i64> {
        self.seg(kind, a).tt_view.at(row_of(a) as u32)
    }

    fn tt_e_raw(&self, kind: RowKind, a: Addr) -> i64 {
        let s = self.seg(kind, a);
        crate::read::effective_tt_e(&self.closes, &s.seg.sidecar(), s.id, row_of(a) as u32)
    }

    /// `tt_e` as the scans expose it: censored to `OPEN_END` past `as_of`.
    fn tt_e(&self, kind: RowKind, a: Addr) -> i64 {
        let t = self.tt_e_raw(kind, a);
        if t > self.plan.as_of {
            OPEN_END
        } else {
            t
        }
    }

    fn label_code(&mut self, a: Addr) -> Result<u32> {
        let si = seg_of(a);
        let r = self.nodes[si].tag[row_of(a)] as usize;
        if self.label_codes[si].is_empty() {
            self.label_codes[si] = vec![u32::MAX; self.nodes[si].seg.strings()?.count() as usize];
        }
        if let Some(&c) = self.label_codes[si].get(r) {
            if c != u32::MAX {
                return Ok(c);
            }
        }
        let s = self.nodes[si].seg.strings()?.get(r as u32)?.to_string();
        let c = self.labels.code(&s);
        if r < self.label_codes[si].len() {
            self.label_codes[si][r] = c;
        }
        Ok(c)
    }

    fn props_text(&self, kind: RowKind, a: Addr) -> Result<&'a str> {
        let s = self.seg(kind, a);
        let r = s.props[row_of(a)];
        // the strings view borrows the segment, which outlives the call
        let seg: &'a Segment<MmapSource> = s.seg;
        seg.strings()?.get(r)
    }

    /// The typed value of `key` in the props of the version at `a`.
    fn prop(&mut self, kind: RowKind, a: Addr, key: &Arc<str>) -> Result<TVal> {
        let si = seg_of(a);
        let mut table = self.prop_table(kind, si, key)?;
        let v = self.prop_in(&mut table, kind, a, key);
        self.props.insert((kind as u8, si, key.clone()), table);
        v
    }

    /// The typed-value table of one (kind, segment, key), taken out of the
    /// cache for a run of rows of that segment (put it back after).
    fn prop_table(&mut self, kind: RowKind, si: usize, key: &Arc<str>) -> Result<PropTable> {
        if let Some(t) = self.props.remove(&(kind as u8, si, key.clone())) {
            return Ok(t);
        }
        let s = match kind {
            RowKind::Node => &self.nodes[si],
            RowKind::Edge => &self.edges[si],
        };
        if let Some(t) = self.store.match_props.take(&(kind as u8, s.id, key.clone())) {
            return Ok(t);
        }
        Ok(PropTable::new(s.seg.strings()?.count() as usize))
    }

    fn prop_in(&self, table: &mut PropTable, kind: RowKind, a: Addr, key: &Arc<str>) -> Result<TVal> {
        let r = self.seg(kind, a).props[row_of(a)] as usize;
        if let Some(v) = table.get(r) {
            return Ok(v.clone());
        }
        let text = self.props_text(kind, a)?;
        let parsed: J = serde_json::from_str(text).unwrap_or(J::Null);
        let v = tval_of(parsed.get(key.as_ref()));
        if r < table.len {
            table.fill(r, v.clone());
        }
        Ok(v)
    }

    /// The typed values of `key` for many addresses, in order: a segment's
    /// table is taken once per run of rows from that segment.
    fn props_of(&mut self, kind: RowKind, ver: &[Addr], key: &Arc<str>) -> Result<Vec<TVal>> {
        // each segment's table taken once for the whole column
        let nseg = match kind {
            RowKind::Node => self.nodes.len(),
            RowKind::Edge => self.edges.len(),
        };
        let mut tables: Vec<Option<PropTable>> = (0..nseg).map(|_| None).collect();
        for &a in ver {
            if a != NO_ADDR && tables[seg_of(a)].is_none() {
                tables[seg_of(a)] = Some(self.prop_table(kind, seg_of(a), key)?);
            }
        }
        let mut out = Vec::with_capacity(ver.len());
        let mut err = None;
        for &a in ver {
            if a == NO_ADDR {
                out.push(TVal::Absent);
                continue;
            }
            let t = tables[seg_of(a)].as_mut().expect("taken above");
            match self.prop_in(t, kind, a, key) {
                Ok(v) => out.push(v),
                Err(e) => {
                    err = Some(e);
                    break;
                }
            }
        }
        for (si, t) in tables.into_iter().enumerate() {
            if let Some(t) = t {
                self.props.insert((kind as u8, si, key.clone()), t);
            }
        }
        match err {
            Some(e) => Err(e),
            None => Ok(out),
        }
    }

    fn vid(&self, kind: RowKind, a: Addr) -> (u64, u32) {
        let s = self.seg(kind, a);
        (s.vid_hi[row_of(a)], s.vid_lo[row_of(a)])
    }

    fn rel_code(&self, a: Addr) -> u32 {
        let s = &self.edges[seg_of(a)];
        s.rel_map[s.rel[row_of(a)] as usize]
    }

    fn disc(&self, a: Addr) -> Result<&'a str> {
        let s = &self.edges[seg_of(a)];
        let seg: &'a Segment<MmapSource> = s.seg;
        seg.strings()?.get(s.tag[row_of(a)])
    }

    /// Edge identity key: two addresses carry one eid iff these agree.
    fn eid_key(&self, a: Addr) -> Result<(u32, u32, u32, &'a str)> {
        let s = &self.edges[seg_of(a)];
        let r = row_of(a);
        Ok((s.a[r], s.b[r], self.rel_code(a), self.disc(a)?))
    }

    /// Do two edge versions carry one identity? Endpoints and relation
    /// first; the discriminator text only when those agree.
    fn same_eid(&self, x: Addr, y: Addr) -> Result<bool> {
        let (sx, sy) = (&self.edges[seg_of(x)], &self.edges[seg_of(y)]);
        let (rx, ry) = (row_of(x), row_of(y));
        if sx.a[rx] != sy.a[ry] || sx.b[rx] != sy.b[ry] || self.rel_code(x) != self.rel_code(y) {
            return Ok(false);
        }
        Ok(self.disc(x)? == self.disc(y)?)
    }

    fn eid_hex(&self, a: Addr) -> DResult<String> {
        let (src, dst, rel, disc) = self.eid_key(a)?;
        let d = self.store.dict();
        let (Some(s), Some(t)) = (d.uid(src), d.uid(dst)) else {
            return refuse("an edge endpoint outside the dictionary");
        };
        Ok(crate::derive::edge_eid(s, t, self.rels.text(rel), disc).to_hex())
    }

    fn vt_ok(&self, vt_s: i64, vt_e: i64, mode: VtMode) -> bool {
        match mode {
            VtMode::Overlap => vt_s < self.plan.t_b && self.plan.t_a < vt_e,
            VtMode::Event => self.plan.t_a <= vt_s && vt_s < self.plan.t_b,
        }
    }
}

// --------------------------------------------------------------------------
// scans
// --------------------------------------------------------------------------

impl<'s, 'a> Ctx<'s, 'a> {
    /// Addresses of the versions of `kind` a scan under (Σ, belief, vt mode)
    /// returns, in segment order, with a per-row predicate on the address.
    fn scan_addrs(
        &mut self,
        kind: RowKind,
        belief: BeliefMode,
        vt_mode: VtMode,
        rel_types: Option<&[String]>,
    ) -> DResult<Vec<Addr>> {
        let plan = self.plan;
        let mut out: Vec<Addr> = Vec::new();
        if belief == BeliefMode::Current {
            let segs = match kind {
                RowKind::Node => &self.nodes,
                RowKind::Edge => &self.edges,
            };
            let targets: Vec<ScanTarget<'_, MmapSource>> = segs
                .iter()
                .map(|s| ScanTarget { segment: s.seg, lane: s.seg.header().lane, id: s.id })
                .collect();
            let set = ScanSet::new(targets)
                .with_closes(self.closes.clone())
                .with_designs(self.store.designs());
            let req = ScanRequest {
                as_of_tt: plan.as_of,
                vt_min: Some(plan.t_a),
                vt_max: Some(plan.t_b),
                rel_types: rel_types.map(<[String]>::to_vec),
                ..ScanRequest::default()
            };
            let (plans, _) = set.plan_believed(&req)?;
            // the believed ranges in pieces of at most SCAN_PIECE rows, in
            // scan order, read in chunks on the pool and concatenated in order
            let mut pieces: Vec<(usize, usize, usize)> = Vec::new();
            for (k, p) in plans.iter().enumerate() {
                for &(lo, hi) in &p.ranges {
                    let mut a = lo;
                    while a < hi {
                        let b = (a + SCAN_PIECE).min(hi);
                        pieces.push((k, a, b));
                        a = b;
                    }
                }
            }
            let this: &Self = self;
            // on the pool from four splits' worth of rows
            out = par_concat_from(pieces.len(), (4 * par_min()).div_ceil(SCAN_PIECE).max(2), |r| {
                let mut out = Vec::new();
                for &(k, lo, hi) in &pieces[r] {
                    let p = &plans[k];
                    let s = &segs[p.idx];
                    let allowed = p.rel_allowed.as_deref();
                    for row in lo..hi {
                        if let (Some(al), Some(codes)) = (allowed, p.rel_codes) {
                            if !al[codes[row] as usize] {
                                continue;
                            }
                        }
                        if !this.vt_ok(s.vt_s[row], s.vt_e(row), vt_mode) {
                            continue;
                        }
                        out.push(addr(p.idx, row as u32));
                    }
                }
                out
            });
            return Ok(out);
        }
        let f = VersionFilter {
            as_of: plan.as_of,
            t_a: plan.t_a,
            t_b: plan.t_b,
            belief: match belief {
                BeliefMode::Superseded => Belief::Superseded,
                _ => Belief::All,
            },
            rel_types,
        };
        let by_id = match kind {
            RowKind::Node => &self.node_by_id,
            RowKind::Edge => &self.edge_by_id,
        };
        for k in self.store.version_keys(kind, &f)? {
            let Some(&si) = by_id.get(&k.seg_id) else {
                return unsupported("a version outside the call's segments");
            };
            let s = match kind {
                RowKind::Node => &self.nodes[si],
                RowKind::Edge => &self.edges[si],
            };
            let row = k.row as usize;
            if self.vt_ok(s.vt_s[row], s.vt_e(row), vt_mode) {
                out.push(addr(si, k.row));
            }
        }
        Ok(out)
    }

    /// Whether the scan reads fewer rows than a seek of `est` adjacency rows
    /// (`SEEK_ROW_FACTOR`). The estimate is at most the edge segments' stored
    /// rows, so above those the scan wins without it, and far below them
    /// (`SEEK_STORED_FACTOR`) the seek does.
    fn scan_beats_seek(&self, est: usize, rel_types: Option<&[String]>) -> DResult<bool> {
        let stored: usize = self.edges.iter().map(|s| s.seg.rows() as usize).sum();
        let need = est.saturating_mul(SEEK_ROW_FACTOR);
        if need > stored {
            return Ok(true);
        }
        if need.saturating_mul(SEEK_STORED_FACTOR) <= stored {
            return Ok(false);
        }
        Ok(need > self.edge_rows_estimate(rel_types)?)
    }

    /// The rows an EdgeScan of `rel_types` reads (current belief): its
    /// believed, window-clipped row ranges, summed from the scan's plan.
    fn edge_rows_estimate(&self, rel_types: Option<&[String]>) -> DResult<usize> {
        let key = rel_types.map(<[String]>::to_vec);
        if let Some(&n) = self.edge_est.lock().ok().and_then(|g| g.get(&key).copied()).as_ref() {
            return Ok(n);
        }
        let n = self.edge_rows_estimate_uncached(rel_types)?;
        if let Ok(mut g) = self.edge_est.lock() {
            g.insert(key, n);
        }
        Ok(n)
    }

    fn edge_rows_estimate_uncached(&self, rel_types: Option<&[String]>) -> DResult<usize> {
        let plan = self.plan;
        let targets: Vec<ScanTarget<'_, MmapSource>> = self
            .edges
            .iter()
            .map(|s| ScanTarget { segment: s.seg, lane: s.seg.header().lane, id: s.id })
            .collect();
        let set = ScanSet::new(targets).with_closes(self.closes.clone()).with_designs(self.store.designs());
        let req = ScanRequest {
            as_of_tt: plan.as_of,
            vt_min: Some(plan.t_a),
            vt_max: Some(plan.t_b),
            rel_types: rel_types.map(<[String]>::to_vec),
            ..ScanRequest::default()
        };
        let (plans, _) = set.plan_believed(&req)?;
        Ok(plans.iter().map(|p| p.rows).sum())
    }

    /// Versions of the given uids under Σ, by identity point reads.
    fn node_versions_of(&mut self, uids: &[String], belief: BeliefMode, vt_mode: VtMode) -> DResult<Vec<(u32, Addr)>> {
        let mut out = Vec::new();
        for uid in uids {
            let Some(id) = self.store.dict().dense_id(uid) else { continue };
            let mut found: Vec<Addr> = Vec::new();
            for (file, row) in self.store.locate_rows(RowKind::Node, crate::read::uid_key(uid))? {
                let Some(&si) = self.node_by_id.get(&segment_id_of(&file)) else { continue };
                let a = addr(si, row);
                if self.nodes[si].a[row as usize] != id {
                    continue;
                }
                if self.believed(RowKind::Node, a, belief)?
                    && self.vt_ok(self.nodes[si].vt_s[row as usize], self.nodes[si].vt_e(row as usize), vt_mode)
                {
                    found.push(a);
                }
            }
            found.sort_unstable();
            found.dedup();
            out.extend(found.into_iter().map(|a| (id, a)));
        }
        Ok(out)
    }

    /// The versions a NodeScan with `labels` returns, in segment order, read
    /// label first: per segment, the string refs naming a label, the rows
    /// carrying one, then belief and valid time on those rows alone. `None`
    /// when they are more than `1 / LABEL_FIRST_FRACTION` of the node lane
    /// (the believed plan of the whole lane is then the cheaper route). The
    /// same rows as `scan_addrs`: the same belief and valid-time tests.
    fn labelled_node_addrs(&mut self, labels: &[String], belief: BeliefMode, vt_mode: VtMode) -> DResult<Option<Vec<Addr>>> {
        let mut per: Vec<Vec<u32>> = Vec::with_capacity(self.nodes.len());
        let (mut rows, mut hits) = (0usize, 0usize);
        for si in 0..self.nodes.len() {
            let by_label = self.label_rows(si)?;
            let mut mine: Vec<u32> = Vec::new();
            for (l, rs) in by_label.iter() {
                if labels.iter().any(|x| x == l) {
                    mine.extend_from_slice(rs);
                }
            }
            // one label's rows are ascending already (the cache lists them in
            // row order): sorted only when several lists were merged
            if !mine.is_sorted() {
                mine.sort_unstable();
            }
            rows += self.nodes[si].tag.len();
            hits += mine.len();
            per.push(mine);
        }
        if hits.saturating_mul(LABEL_FIRST_FRACTION) > rows {
            return Ok(None);
        }
        let mut out = Vec::with_capacity(hits);
        let this: &Self = self;
        for (si, mine) in per.iter().enumerate() {
            let s = &self.nodes[si];
            if mine.is_empty() || s.seg.tt_s_bounds().0 > self.plan.as_of {
                continue;
            }
            // the segment's label rows in chunks on the pool, in row order
            out.extend(par_try_concat(mine.len(), |rg| {
                let mut part = Vec::new();
                for &r in &mine[rg] {
                    let row = r as usize;
                    if !this.vt_ok(s.vt_s[row], s.vt_e(row), vt_mode) {
                        continue;
                    }
                    let a = addr(si, r);
                    if this.believed(RowKind::Node, a, belief)? {
                        part.push(a);
                    }
                }
                Ok(part)
            })?);
        }
        Ok(Some(out))
    }

    /// One node segment's rows by label, from the handle's cache or by one
    /// pass over its label column (the distinct label refs resolved once).
    fn label_rows(&self, si: usize) -> DResult<LabelRows> {
        let s = &self.nodes[si];
        if let Ok(g) = self.store.match_label_rows.lock() {
            if let Some(v) = g.get(&s.id) {
                return Ok(v.clone());
            }
        }
        let mut by_ref: FxMap<u32, Vec<u32>> = FxMap::default();
        for (row, &t) in s.tag.iter().enumerate() {
            by_ref.entry(t).or_default().push(row as u32);
        }
        let strings = s.seg.strings()?;
        let mut by_label: HashMap<String, Vec<u32>> = HashMap::new();
        for (r, rows) in by_ref {
            let l = strings.get(r)?.to_string();
            by_label.entry(l).or_default().extend(rows);
        }
        let mut v: Vec<(String, Vec<u32>)> = by_label.into_iter().collect();
        for (_, rows) in v.iter_mut() {
            rows.sort_unstable();
        }
        v.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        let v = Arc::new(v);
        if let Ok(mut g) = self.store.match_label_rows.lock() {
            g.insert(s.id, v.clone());
        }
        Ok(v)
    }

    /// Every node version (segment order) whose row passes `pick`, valid in
    /// Σ's window (`vt_mode`) and admitted by `belief` at `as_of`: a row loop,
    /// belief decided per row.
    fn node_rows_where(&self, belief: BeliefMode, vt_mode: VtMode, pick: impl Fn(&SegCols<'a>, usize) -> bool) -> DResult<Vec<Addr>> {
        let mut out = Vec::new();
        for si in 0..self.nodes.len() {
            out.extend(self.node_rows_in(si, belief, vt_mode, &pick)?);
        }
        Ok(out)
    }

    /// `node_rows_where` over one segment: the rows `pick` keeps, in row
    /// order.
    fn node_rows_in(&self, si: usize, belief: BeliefMode, vt_mode: VtMode, pick: impl Fn(&SegCols<'a>, usize) -> bool) -> DResult<Vec<Addr>> {
        let s = &self.nodes[si];
        let mut out = Vec::new();
        if s.seg.tt_s_bounds().0 > self.plan.as_of {
            return Ok(out);
        }
        for row in 0..s.tag.len() {
            if !pick(s, row) || !self.vt_ok(s.vt_s[row], s.vt_e(row), vt_mode) {
                continue;
            }
            let a = addr(si, row as u32);
            if self.believed(RowKind::Node, a, belief)? {
                out.push(a);
            }
        }
        Ok(out)
    }

    fn believed(&self, kind: RowKind, a: Addr, belief: BeliefMode) -> Result<bool> {
        let as_of = self.plan.as_of;
        let tt_s = self.tt_s(kind, a)?;
        if tt_s > as_of {
            return Ok(false);
        }
        let tt_e = self.tt_e_raw(kind, a);
        Ok(match belief {
            BeliefMode::Current => as_of < tt_e,
            BeliefMode::Superseded => tt_e <= as_of,
            BeliefMode::All => true,
        })
    }

    /// The node version map of Σ under current belief: dense id -> the
    /// version valid at the instant (first by `(vt_s, vid)` if several).
    /// Adopt the handle's cached node version map when it is Σ's.
    fn load_cached_map(&mut self) {
        if self.node_map.is_some() {
            return;
        }
        let key = (self.store.generation(), self.plan.as_of, self.plan.t_a);
        if let Ok(g) = self.store.match_node_map.lock() {
            if let Some((k, m)) = g.as_ref() {
                if *k == key {
                    self.node_map = Some(m.clone());
                }
            }
        }
    }

    fn node_map(&mut self) -> DResult<Arc<Vec<Addr>>> {
        let key = (self.store.generation(), self.plan.as_of, self.plan.t_a);
        self.load_cached_map();
        if self.node_map.is_none() {
            let n = self.store.dict().len() as usize;
            let mut map = vec![NO_ADDR; n];
            let addrs = self.scan_addrs(RowKind::Node, BeliefMode::Current, VtMode::Overlap, None)?;
            for a in addrs {
                let s = &self.nodes[seg_of(a)];
                let id = s.a[row_of(a)] as usize;
                if id >= n {
                    continue;
                }
                let cur = map[id];
                if cur == NO_ADDR || self.version_key(a) < self.version_key(cur) {
                    map[id] = a;
                }
            }
            let map = Arc::new(map);
            if let Ok(mut g) = self.store.match_node_map.lock() {
                *g = Some((key, map.clone()));
            }
            self.node_map = Some(map);
        }
        Ok(self.node_map.clone().expect("built above"))
    }

    fn version_key(&self, a: Addr) -> (i64, u64, u32) {
        let s = &self.nodes[seg_of(a)];
        (s.vt_s[row_of(a)], s.vid_hi[row_of(a)], s.vid_lo[row_of(a)])
    }

    /// The Σ-visible version of each id (current belief): point reads for a
    /// few distinct ids, a pass over the node lane for more, the node version
    /// map for many.
    fn versions_for(&mut self, ids: &[u32]) -> DResult<Vec<Addr>> {
        self.load_cached_map();
        let point_max = POINT_READS_MAX.max(self.store.dict().len() as usize / POINT_READS_FRACTION);
        let mut distinct: Vec<u32> = Vec::new();
        if self.node_map.is_none() && ids.len() <= 64 * point_max.max(self.store.dict().len() as usize / BATCH_READS_FRACTION) {
            distinct = ids.iter().copied().filter(|&i| i != NO_ID).collect();
            distinct.sort_unstable();
            distinct.dedup();
        }
        let batch_max = (self.store.dict().len() as usize / BATCH_READS_FRACTION).max(point_max);
        if self.node_map.is_none() && distinct.len() > point_max && distinct.len() <= batch_max {
            let mut found: FxMap<u32, Addr> = FxMap::default();
            let n = self.store.dict().len() as usize;
            let mut want = vec![false; n];
            for &i in &distinct {
                if (i as usize) < n {
                    want[i as usize] = true;
                }
            }
            let rows = self.node_rows_where(BeliefMode::Current, VtMode::Overlap, |s, r| {
                want.get(s.a[r] as usize).copied().unwrap_or(false)
            })?;
            for a in rows {
                let id = self.nodes[seg_of(a)].a[row_of(a)];
                let keep = match found.get(&id) {
                    Some(&cur) => self.version_key(a) < self.version_key(cur),
                    None => true,
                };
                if keep {
                    found.insert(id, a);
                }
            }
            return Ok(ids.iter().map(|i| found.get(i).copied().unwrap_or(NO_ADDR)).collect());
        }
        if self.node_map.is_none() && !distinct.is_empty() && distinct.len() <= point_max {
            let mut found: FxMap<u32, Addr> = FxMap::default();
            let uids: Vec<String> = distinct
                .iter()
                .filter_map(|&i| self.store.dict().uid(i).map(str::to_string))
                .collect();
            for (id, a) in self.node_versions_of(&uids, BeliefMode::Current, VtMode::Overlap)? {
                let keep = match found.get(&id) {
                    Some(&cur) => self.version_key(a) < self.version_key(cur),
                    None => true,
                };
                if keep {
                    found.insert(id, a);
                }
            }
            return Ok(ids.iter().map(|i| found.get(i).copied().unwrap_or(NO_ADDR)).collect());
        }
        let map = self.node_map()?;
        Ok(par_concat(ids.len(), |r| {
            ids[r].iter().map(|&i| if (i as usize) < map.len() { map[i as usize] } else { NO_ADDR }).collect()
        }))
    }
}

/// `GNOMON_MATCH_TRACE=1` (read once per process): one stderr line per plan
/// node with its output rows and time, for diagnosing a slow plan.
fn trace() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| std::env::var("GNOMON_MATCH_TRACE").is_ok_and(|v| v == "1"))
}

/// Time since `t0` when a trace timer is running (`trace()`), else zero: the
/// trace timers cost nothing in an untraced call.
fn lap(t0: Option<std::time::Instant>) -> std::time::Duration {
    t0.map_or(std::time::Duration::ZERO, |t| t.elapsed())
}

fn op_name(op: &Op) -> &'static str {
    match op {
        Op::NodeScan { .. } => "NodeScan",
        Op::EdgeScan { .. } => "EdgeScan",
        Op::Expand { .. } => "Expand",
        Op::Filter(_) => "Filter",
        Op::TypeConstraint { .. } => "TypeConstraint",
        Op::Project { .. } => "Project",
        Op::Join { .. } => "Join",
        Op::Aggregate { .. } => "Aggregate",
        Op::Order(_) => "Order",
        Op::Limit(_) => "Limit",
        Op::Union => "Union",
        Op::TopK { .. } => "TopK",
        Op::Walk { .. } => "Walk",
        Op::OrderedReach(_) => "OrderedReach",
        Op::Succ { .. } => "Succ",
        Op::CoValidExpand(_) => "CoValidExpand",
        Op::CoValidJoin(_) => "CoValidJoin",
        Op::Collapse { .. } => "Collapse",
    }
}

/// A version's identity: a node's dense id, or an edge's (src, dst,
/// relationship, discriminator).
type IdentityKey = (u32, u32, u32, Arc<str>);
/// A version on one axis: (start, version id, address).
type VersionAt = (i64, (u64, u32), Addr);
/// One symbol of a path expression (relationship type, inverse) and the
/// (state, next state) moves on it.
type SymMoves = (String, bool, Vec<(usize, usize)>);
/// A walk's (group, target) pairs, each group's first position, and the
/// states searched.
type WalkFound = (Vec<u64>, Vec<usize>, usize);
/// A distinct projection over a walk: the walk node, its target-only
/// predicates, and the keys as (output name, input column).
type WalkDistinct = (usize, Vec<Op>, Vec<(String, String)>);

/// A semi-join filter: the target column per input row, and per target
/// value the sorted allowed neighbours.
type SemiSets = (Vec<u32>, FxMap<u32, Vec<u32>>);

/// The closure search's answer: per seed, `pairs[offsets[i]..offsets[i + 1]]`
/// of (node, depth), as `(offsets, pairs)`.
type SeedReach = (Vec<usize>, Vec<(u32, u32)>);

/// An expansion: for each output row, the input row it extends; and the
/// newly bound variables.
type Expansion = (Vec<usize>, Vec<(String, Var)>);

/// The candidate targets passing the target predicates (sorted, distinct)
/// and the Σ-visible version of each.
type Passing = (Vec<u32>, Vec<Addr>);

/// Per frontier node, its neighbours with the connecting edge's address, as
/// one CSR: `items[off[p]..off[p + 1]]` is node `p`'s list. One allocation per
/// hop, not one per frontier node (a 2.3M-node frontier at SF1).
#[derive(Clone, Debug, Default)]
struct NeighborLists {
    off: Vec<u32>,
    items: Vec<(u32, Addr)>,
}

impl NeighborLists {
    fn empty(n: usize) -> NeighborLists {
        NeighborLists { off: vec![0; n + 1], items: Vec::new() }
    }

    fn from_vecs(v: Vec<Vec<(u32, Addr)>>) -> NeighborLists {
        let mut out = NeighborLists { off: Vec::with_capacity(v.len() + 1), items: Vec::with_capacity(v.iter().map(Vec::len).sum()) };
        out.off.push(0);
        for l in v {
            out.items.extend(l);
            out.off.push(out.items.len() as u32);
        }
        out
    }

    fn iter(&self) -> impl Iterator<Item = &[(u32, Addr)]> + '_ {
        self.off.windows(2).map(move |w| &self.items[w[0] as usize..w[1] as usize])
    }

    /// Keep the entries `f` accepts, every list in place.
    fn retain(&mut self, mut f: impl FnMut(&(u32, Addr)) -> bool) {
        let mut items = Vec::with_capacity(self.items.len());
        let mut off = Vec::with_capacity(self.off.len());
        off.push(0u32);
        for w in self.off.windows(2) {
            items.extend(self.items[w[0] as usize..w[1] as usize].iter().copied().filter(|x| f(x)));
            off.push(items.len() as u32);
        }
        self.items = items;
        self.off = off;
    }
}

impl std::ops::Index<usize> for NeighborLists {
    type Output = [(u32, Addr)];
    fn index(&self, p: usize) -> &[(u32, Addr)] {
        &self.items[self.off[p] as usize..self.off[p + 1] as usize]
    }
}

/// A sorted distinct frontier's positions: a direct map over the ids for a
/// large frontier, the binary search for a small one.
struct FrontierPos<'f> {
    frontier: &'f [u32],
    map: Option<Vec<u32>>,
}

impl<'f> FrontierPos<'f> {
    fn new(frontier: &'f [u32]) -> FrontierPos<'f> {
        let map = (frontier.len() > 4096).then(|| {
            let mut m = vec![u32::MAX; frontier.last().map_or(0, |&x| x as usize + 1)];
            for (i, &u) in frontier.iter().enumerate() {
                m[u as usize] = i as u32;
            }
            m
        });
        FrontierPos { frontier, map }
    }

    fn get(&self, u: u32) -> Option<usize> {
        match &self.map {
            Some(m) => m.get(u as usize).copied().filter(|&p| p != u32::MAX).map(|p| p as usize),
            None => self.frontier.binary_search(&u).ok(),
        }
    }
}

/// a relationship row met from a frontier node: (valid start, version id,
/// orientation, far node, edge address)
type RelHit = (i64, (u64, u32), u8, u32, Addr);

/// The rows of a join on identity keys: (left row, right row or none), left
/// rows in order and each one's matches in right order, as the general path
/// gives them; through a CSR index of the smaller side (`KeyCsr`).
fn match_ids(lu: &[u64], ru: &[u64], kind: JoinKind) -> DResult<(Vec<usize>, Vec<Option<usize>>)> {
    let mut li: Vec<usize> = Vec::new();
    let mut ri: Vec<Option<usize>> = Vec::new();
    let emit = |i: usize, js: &mut dyn Iterator<Item = usize>, li: &mut Vec<usize>, ri: &mut Vec<Option<usize>>| {
        let mut any = false;
        for j in js {
            any = true;
            if kind != JoinKind::Anti {
                li.push(i);
                ri.push(Some(j));
            }
        }
        match (any, kind) {
            (false, JoinKind::LeftOuter) => {
                li.push(i);
                ri.push(None);
            }
            (false, JoinKind::Anti) => li.push(i),
            _ => {}
        }
    };
    let pooled = MATCH_THREADS.with(|c| c.get()) > 1 && lu.len().max(ru.len()) >= par_min();
    // on the pool: a left side several times smaller is indexed and the right
    // rows find their matches in chunks; otherwise the right side is indexed
    // and the left rows probe it in chunks (measured at SF1: 10k x 2.8M rows
    // 125 -> 77 ms the first way, 1.7M x 2.8M 49 -> 77 ms the second)
    if lu.len().saturating_mul(4) < ru.len() && pooled {
        // the (smaller) left side indexed; the right rows find their left
        // matches in chunks on the pool, in right order, and a stable
        // counting pass groups them by left row: per left row, its matches
        // in right order
        let idx = KeyCsr::build(lu);
        let pairs: Vec<(u32, u32)> = par_concat(ru.len(), |r| {
            let mut v = Vec::new();
            for j in r {
                for &i in idx.rows(ru[j]) {
                    v.push((i, j as u32));
                }
            }
            v
        });
        check_rows(pairs.len())?;
        let mut start = vec![0usize; lu.len() + 1];
        for &(i, _) in &pairs {
            start[i as usize + 1] += 1;
        }
        for i in 0..lu.len() {
            start[i + 1] += start[i];
        }
        let mut fill = vec![0u32; pairs.len()];
        let mut pos = start.clone();
        for &(i, j) in &pairs {
            fill[pos[i as usize]] = j;
            pos[i as usize] += 1;
        }
        drop(pairs);
        for i in 0..lu.len() {
            emit(i, &mut fill[start[i]..start[i + 1]].iter().map(|&j| j as usize), &mut li, &mut ri);
        }
    } else if lu.len() < ru.len() && !pooled {
        // the left side indexed: per left row, its matches in right order
        // (counted, then filled)
        let idx = KeyCsr::build(lu);
        let mut start = vec![0usize; lu.len() + 1];
        for &k in ru {
            for &i in idx.rows(k) {
                start[i as usize + 1] += 1;
            }
        }
        for i in 0..lu.len() {
            start[i + 1] += start[i];
        }
        let total = start[lu.len()];
        check_rows(total)?;
        let mut fill = vec![0u32; total];
        let mut pos = start.clone();
        for (j, &k) in ru.iter().enumerate() {
            for &i in idx.rows(k) {
                fill[pos[i as usize]] = j as u32;
                pos[i as usize] += 1;
            }
        }
        for i in 0..lu.len() {
            emit(i, &mut fill[start[i]..start[i + 1]].iter().map(|&j| j as usize), &mut li, &mut ri);
        }
    } else {
        // the right side indexed: the left rows probe it in chunks on the
        // pool, concatenated in left order
        let idx = KeyCsr::build(ru);
        let n = lu.len();
        let threads = MATCH_THREADS.with(|c| c.get());
        // a few left rows can match many right rows: on the pool, chunks of
        // 64 left rows at least, whichever side is the large one
        let width = if pooled { threads.min(n / 64).max(1) } else { 1 };
        let size = n.div_ceil(width.max(1)).max(1);
        let parts = crate::pool::run(width, |w| {
            let (mut li, mut ri) = (Vec::new(), Vec::new());
            for i in (w * size).min(n)..((w + 1) * size).min(n) {
                emit(i, &mut idx.rows(lu[i]).iter().map(|&j| j as usize), &mut li, &mut ri);
            }
            (li, ri)
        });
        for (a, b) in parts {
            li.extend(a);
            ri.extend(b);
        }
    }
    Ok((li, ri))
}

/// A CSR index of join keys: for each distinct key, its rows in ascending
/// order. Keys below a bound (dense node ids) index a direct array; others
/// (two identities in one u64) an FxMap to a dense slot.
struct KeyCsr {
    slots: Option<FxMap<u64, u32>>,
    offsets: Vec<u32>,
    rows: Vec<u32>,
}

impl KeyCsr {
    fn build(keys: &[u64]) -> KeyCsr {
        let max = keys.iter().copied().max().unwrap_or(0);
        // a direct array only where it is no larger than four slots a key
        let bound = ((keys.len() as u64) * 4).max(64);
        let (slots, gid): (Option<FxMap<u64, u32>>, Vec<u32>) = if max < bound {
            (None, keys.iter().map(|&k| k as u32).collect())
        } else {
            let mut m: FxMap<u64, u32> = FxMap::default();
            let g = keys
                .iter()
                .map(|&k| {
                    let n = m.len() as u32;
                    *m.entry(k).or_insert(n)
                })
                .collect();
            (Some(m), g)
        };
        let groups = match &slots {
            None => max as usize + 1,
            Some(m) => m.len(),
        };
        let mut offsets = vec![0u32; groups + 1];
        for &g in &gid {
            offsets[g as usize + 1] += 1;
        }
        for g in 0..groups {
            offsets[g + 1] += offsets[g];
        }
        let mut pos: Vec<u32> = offsets[..groups].to_vec();
        let mut rows = vec![0u32; keys.len()];
        for (r, &g) in gid.iter().enumerate() {
            rows[pos[g as usize] as usize] = r as u32;
            pos[g as usize] += 1;
        }
        KeyCsr { slots, offsets, rows }
    }

    fn rows(&self, k: u64) -> &[u32] {
        let g = match &self.slots {
            None => {
                if k as usize + 1 >= self.offsets.len() {
                    return &[];
                }
                k as usize
            }
            Some(m) => match m.get(&k) {
                Some(&g) => g as usize,
                None => return &[],
            },
        };
        &self.rows[self.offsets[g] as usize..self.offsets[g + 1] as usize]
    }
}

/// The right side of a join `index_join` reads: edges by adjacency, or nodes
/// by point reads.
#[derive(Clone, Copy, Debug)]
enum Seek {
    Edges { scan: usize, dir: NeighborDir },
    Nodes { scan: usize },
}

/// A join on a whole scan seeks its left keys instead (adjacency or point
/// reads) while they are at most one per this many identities, and
/// `INDEX_JOIN_MIN` at least. Measured at SF1 (3.7M identities, IC14's
/// window weight pattern restricted to n persons, seek / scan): n = 16:
/// 0.16 / 0.90 s, 64: 0.34 / 0.91 s, 256: 1.19 / 1.36 s, 1024: 3.5 / 3.1 s;
/// the seek's keys there reach ~120k at n = 256 and ~1M at n = 4096.
const INDEX_JOIN_FRACTION: usize = 64;
const INDEX_JOIN_MIN: usize = 256;
/// A node seek runs for up to one key per this many identities. Measured at
/// SF1 (BI19's restricted weight pattern, 65k message keys of 3.7M ids): the
/// NodeScan of 2.76M messages and the join on it 0.32 s, the bitset pass
/// and the join on its 65k rows ~0.06 s.
const NODE_SEEK_FRACTION: usize = 4;

/// An edge seek reads its first `SEEK_PROBE` keys' adjacency, and scans
/// instead when the rows extrapolated to all its keys pass 1 /
/// `SEEK_ROW_FACTOR` of the scan's (measured at SF1: ~4 us an adjacency row
/// by seek, ~0.1 us a row by the scan and the CSR join).
const SEEK_PROBE: usize = 1024;
const SEEK_ROW_FACTOR: usize = 40;

/// The scan's own estimate plans every edge segment's believed ranges: 17-20
/// ms at SF1 under an as-of belief, against a 1.5-3 ms probe (BI7's two
/// hops). A seek whose rows, times `SEEK_ROW_FACTOR`, stay within 1 / this of
/// the edge segments' stored rows keeps the seek without it.
const SEEK_STORED_FACTOR: usize = 8;

/// A seek under `belief: all` or `superseded` reads each key's adjacency
/// slices and tests every row; past `SEEK_PROBE` keys it scans instead when
/// those slices hold more than 1 / this of the edge segments' stored rows.
/// Measured at SF1 (BIH-R7's KNOWS join, 10,363 keys): ~29 ns a slice row
/// (2.87M rows, 84 ms), the scan ~2.7 ns a stored row (21.6M, 59 ms).
const SEEK_ANY_ROW_FACTOR: usize = 10;

/// Distinct ids up to which node versions are found by identity point reads
/// rather than by building the node version map of Σ.
const POINT_READS_MAX: usize = 256;

/// The node versions of up to one id per this many node identities are found
/// by point reads too: at SF1 (3.8M identities) a point read costs ~3.5 us an
/// id and the node lane pass ~4 ms plus ~0.3 us an id, so they cross near
/// 1,200 ids (684 ids: 2.4 vs 4.1 ms; 1,103: 3.8 vs 4.3; 3,439: 12 vs 4.9).
const POINT_READS_FRACTION: usize = 3200;

/// `OrderedReach` loads its frontier's out-lists by a pass over the edge
/// columns per level (a few ms at SF1) for at most this many passes, then
/// the whole adjacency (about 0.2 s for SF1's 1.5M transfers).
const REACH_SCANS_MAX: usize = 8;
/// Beyond `POINT_READS_MAX`, while the distinct ids are at most this fraction
/// of the node identities and Σ's node version map is not already built,
/// their versions come from one pass over the node lane's id column against a
/// bitset of the wanted ids, belief and valid time tested on the hits alone:
/// the map holds every identity's version and is rebuilt for every new
/// generation or belief time.
const BATCH_READS_FRACTION: usize = 16;
/// A label NodeScan reads the label column first and tests belief and valid
/// time on the matching rows alone while they are at most this fraction of
/// the node lane's rows; above it, the believed plan of the whole lane.
const LABEL_FIRST_FRACTION: usize = 8;

// --------------------------------------------------------------------------
// operators
// --------------------------------------------------------------------------

impl<'s, 'a> Ctx<'s, 'a> {
    fn run(&mut self, id: usize, memo: &mut FxMap<usize, Rel>) -> DResult<Rel> {
        if let Some(r) = memo.get(&id) {
            return Ok(r.clone());
        }
        let node = self.plan.nodes[id].clone();
        let t_node = std::time::Instant::now();
        let semi_expand = matches!(node.op, Op::Expand { .. }) && self.hints().contains_key(&id);
        let out = if let Some(plan) = self.walk_distinct(id) {
            self.run_walk_distinct(&plan, memo)?
        } else if let Some(chain) = self.filter_chain(id) {
            self.run_filter_chain(&chain, memo)?
        } else if semi_expand {
            let Op::Expand { from, into, hops, rel_type, dir, edge_var } = node.op.clone() else { unreachable!() };
            let input = self.run(node.inputs[0], memo)?;
            let semi = self.semi_sets(id, &input)?;
            let (rows, vars) = self.expansion(
                &input, &from, &into, hops, rel_type.as_deref(), dir, edge_var.as_deref(), None, &[], semi.as_ref(),
            )?;
            let mut out = input.take(&rows);
            out.vars.extend(vars);
            out
        } else if let Some((exp, target)) = self.closing_expand(&node) {
            // `Filter(into.uid = x)` over the `Expand` that bound `into`: the
            // expansion keeps only the neighbours equal to `x` (expand-into),
            // never materialising the rest. The same rows, in the same order.
            let Op::Expand { from, into, hops, rel_type, dir, edge_var } = &self.plan.nodes[exp].op else {
                unreachable!("closing_expand checks the op")
            };
            let (from, into, hops, rel_type, dir, edge_var) =
                (from.clone(), into.clone(), *hops, rel_type.clone(), *dir, edge_var.clone());
            let input = self.run(self.plan.nodes[exp].inputs[0], memo)?;
            let want = self.uid_column(&input, &target)?;
            self.expand_to(input, &from, &into, hops, rel_type.as_deref(), dir, edge_var.as_deref(), Some(&want))?
        } else if let Some((seek, lcol)) = self.seek_plan(&node) {
            self.index_join(&node, seek, &lcol, memo)?
        } else {
            let mut inputs = Vec::with_capacity(node.inputs.len());
            for &i in &node.inputs {
                inputs.push(self.run(i, memo)?);
            }
            self.apply(&node.op, inputs)?
        };
        check_rows(out.n)?;
        let mut out = out;
        self.drop_dead_cols(id, &mut out);
        if trace() {
            eprintln!(
                "match trace: node {id} {} -> {} rows in {:.3} ms (cumulative over inputs)",
                op_name(&node.op),
                out.n,
                t_node.elapsed().as_secs_f64() * 1e3
            );
        }
        // a subtree reached twice (a sub-pattern's key relation) is kept
        let uses = self.plan.nodes.iter().filter(|n| n.inputs.contains(&id)).count();
        if uses > 1 {
            memo.insert(id, out.clone());
        }
        Ok(out)
    }

    fn uses(&self, id: usize) -> usize {
        self.plan.nodes.iter().filter(|n| n.inputs.contains(&id)).count()
    }

    /// Drop the compiler's own columns (named `_...`, never an answer's)
    /// that no operator downstream of `id` names: the interval chains add
    /// two per step and keep the rest, and every later join or filter would
    /// copy them. An operator reads only the columns it names, so a name
    /// absent from the text of every operator downstream (and from the
    /// answer's columns) is never read again; the search is textual, so it
    /// may keep a column, never drop a read one. Not at the root, and only
    /// for tables of `DEAD_COLS_MIN` rows or more (no cost on small calls).
    fn drop_dead_cols(&mut self, id: usize, rel: &mut Rel) {
        if id == self.plan.root || rel.n < DEAD_COLS_MIN.min(par_min()) {
            return;
        }
        if !self.downstream.contains_key(&id) {
            let plan = self.plan;
            let mut text = plan.columns.join(" ");
            let mut seen = vec![false; plan.nodes.len()];
            let mut stack = vec![id];
            while let Some(x) = stack.pop() {
                for (j, n) in plan.nodes.iter().enumerate() {
                    if !seen[j] && n.inputs.contains(&x) {
                        seen[j] = true;
                        text.push(' ');
                        text.push_str(&format!("{:?}", n.op));
                        stack.push(j);
                    }
                }
            }
            self.downstream.insert(id, text);
        }
        let text = &self.downstream[&id];
        rel.cols.retain(|(k, _)| !k.starts_with('_') || text.contains(k.as_str()));
        // a variable is read by name ("x") or through a field ("x.uid"),
        // both quoted in an operator's text: one named by no operator
        // downstream is never read again (a memoised table, read by two
        // operators, is copied for each)
        rel.vars.retain(|(k, _)| text.contains(&format!("\"{k}\"")) || text.contains(&format!("\"{k}.")));
    }

    /// A Join the left keys can seek: its right input a whole scan (a scan
    /// another branch shares is read whole there, if at all), keyed (first
    /// key) on an edge end of an EdgeScan, on the
    /// near end of `dir: both`'s union of two projections of one EdgeScan, or
    /// on the uid of a NodeScan; current belief, overlapping valid time.
    /// Returns the seek and the left key column.
    fn seek_plan(&self, node: &PNode) -> Option<(Seek, String)> {
        let Op::Join { on, .. } = &node.op else { return None };
        let (lcol, rcol) = on.first()?;
        let right = *node.inputs.get(1)?;
        let edge_scan = |id: usize| -> Option<&String> {
            match &self.plan.nodes[id].op {
                Op::EdgeScan { var, belief, vt_mode: VtMode::Overlap, .. }
                    if *belief == BeliefMode::Current || self.store.designs().adjacency_index =>
                {
                    Some(var)
                }
                _ => None,
            }
        };
        match &self.plan.nodes[right].op {
            Op::EdgeScan { .. } => {
                let var = edge_scan(right)?;
                let dir = if *rcol == format!("{var}.src") {
                    NeighborDir::Out
                } else if *rcol == format!("{var}.dst") {
                    NeighborDir::In
                } else {
                    return None;
                };
                Some((Seek::Edges { scan: right, dir }, lcol.clone()))
            }
            Op::NodeScan { var, uids: None, vt_mode: VtMode::Overlap, .. } if *rcol == format!("{var}.uid") =>
            {
                Some((Seek::Nodes { scan: right }, lcol.clone()))
            }
            Op::Union => {
                if self.uses(right) != 1 {
                    return None;
                }
                let ins = &self.plan.nodes[right].inputs;
                if ins.len() != 2 {
                    return None;
                }
                let mut scan = None;
                for &pj in ins {
                    let Op::Project { bindings, keep_all: true } = &self.plan.nodes[pj].op else { return None };
                    if self.uses(pj) != 1 {
                        return None;
                    }
                    let sc = self.plan.nodes[pj].inputs[0];
                    let var = edge_scan(sc)?;
                    let (_, e) = bindings.iter().find(|(n, _)| n == rcol)?;
                    if !matches!(e, Expr::Col(c) if *c == format!("{var}.src") || *c == format!("{var}.dst")) {
                        return None;
                    }
                    if scan.is_some_and(|x| x != sc) {
                        return None;
                    }
                    scan = Some(sc);
                }
                let sc = scan?;
                Some((Seek::Edges { scan: sc, dir: NeighborDir::Both }, lcol.clone()))
            }
            _ => None,
        }
    }

    /// The Join of `seek_plan`: the left side, then the right side read for
    /// the distinct left keys only when they are few (the rows the scan holds
    /// for them, in the scan's order, so the join's rows and their order are
    /// the scan's), else the whole scan.
    fn index_join(&mut self, node: &PNode, seek: Seek, lcol: &str, memo: &mut FxMap<usize, Rel>) -> DResult<Rel> {
        let left = self.run(node.inputs[0], memo)?;
        // a node variable's identities, or a column of identities (an edge end)
        let keys = match self.uid_column(&left, lcol) {
            Ok(ids) => Some(ids),
            Err(_) => {
                let c = self.eval(&Expr::Col(lcol.to_string()), &left)?;
                match &c.v {
                    Vals::Uid(x) if !c.null.as_ref().is_some_and(|m| m.iter().any(|&b| b)) => Some(x.clone()),
                    _ => None,
                }
            }
        };
        let mut f = keys.unwrap_or_default();
        f.sort_unstable();
        f.dedup();
        // a node seek past the point reads is one pass over the node lane's
        // id column against a bitset, never dearer than the scan it replaces
        // and the join over all of the scan's rows
        let n_ids = self.store.dict().len() as usize;
        let most = match seek {
            Seek::Nodes { .. } => (n_ids / NODE_SEEK_FRACTION).max(INDEX_JOIN_MIN),
            Seek::Edges { .. } => (n_ids / INDEX_JOIN_FRACTION).max(INDEX_JOIN_MIN),
        };
        if f.is_empty() || f.len() > most || f.contains(&NO_ID) {
            let right = self.run(node.inputs[1], memo)?;
            return self.apply(&node.op, vec![left, right]);
        }
        let t0 = trace().then(std::time::Instant::now);
        let right = match seek {
            Seek::Edges { scan, dir } if !matches!(self.plan.nodes[scan].op, Op::EdgeScan { belief: BeliefMode::Current, .. }) => {
                let Op::EdgeScan { var, rel_types, belief, .. } = self.plan.nodes[scan].op.clone() else {
                    unreachable!("seek_plan checks the op")
                };
                // many keys: the slices they read, against the scan's stored rows
                if f.len() > SEEK_PROBE {
                    let reads = self.edge_seek_reads(&f, dir)?;
                    let stored: usize = self.edges.iter().map(|s| s.seg.rows() as usize).sum();
                    let scan = reads.saturating_mul(SEEK_ANY_ROW_FACTOR) > stored;
                    if trace() {
                        eprintln!(
                            "match trace:   edge seek any: {} keys, {} slice rows, {} stored: scan {}",
                            f.len(),
                            reads,
                            stored,
                            scan
                        );
                    }
                    if scan {
                        let right = self.run(node.inputs[1], memo)?;
                        return self.apply(&node.op, vec![left, right]);
                    }
                }
                let addrs = self.edge_seek_any(&f, dir, rel_types.as_deref(), belief)?;
                let r = Rel { n: addrs.len(), vars: vec![(var, Var::Edge { ver: addrs })], cols: Vec::new() };
                if scan == node.inputs[1] {
                    r
                } else {
                    let mut only: FxMap<usize, Rel> = FxMap::default();
                    only.insert(scan, r);
                    self.run(node.inputs[1], &mut only)?
                }
            }
            Seek::Edges { scan, dir } => {
                let Op::EdgeScan { var, rel_types, .. } = self.plan.nodes[scan].op.clone() else {
                    unreachable!("seek_plan checks the op")
                };
                let (va, vb, as_of) = (self.plan.t_a, self.plan.t_b, self.plan.as_of);
                let probe = f.len().min(SEEK_PROBE);
                let t0 = std::time::Instant::now();
                let nb = self.store.neighbors_batch(&f[..probe], dir, as_of, Some(va), Some(vb), rel_types.as_deref())?;
                let mut parts = vec![nb];
                if f.len() > probe {
                    // the rows the seek would read, against the scan's
                    let est = parts[0].len() * f.len() / probe;
                    let t_probe = t0.elapsed();
                    let scan = self.scan_beats_seek(est, rel_types.as_deref())?;
                    if trace() {
                        eprintln!(
                            "match trace:   edge seek probe: {} keys, {} rows estimated, scan {}: probe {:.1} ms, decision {:.1} ms",
                            f.len(),
                            est,
                            scan,
                            t_probe.as_secs_f64() * 1e3,
                            (t0.elapsed() - t_probe).as_secs_f64() * 1e3
                        );
                    }
                    if scan {
                        let right = self.run(node.inputs[1], memo)?;
                        return self.apply(&node.op, vec![left, right]);
                    }
                    parts.push(self.store.neighbors_batch(&f[probe..], dir, as_of, Some(va), Some(vb), rel_types.as_deref())?);
                }
                let mut addrs = Vec::with_capacity(parts.iter().map(|nb| nb.len()).sum());
                for nb in &parts {
                    for j in 0..nb.len() {
                        let Some(&si) = self.edge_by_id.get(&nb.seg_id[j]) else {
                            return unsupported("an adjacency row outside the call's segments");
                        };
                        addrs.push(addr(si, nb.seg_row[j]));
                    }
                }
                // the scan's order: segment, then row
                addrs.sort_unstable();
                addrs.dedup();
                let r = Rel { n: addrs.len(), vars: vec![(var, Var::Edge { ver: addrs })], cols: Vec::new() };
                if scan == node.inputs[1] {
                    r
                } else {
                    let mut only: FxMap<usize, Rel> = FxMap::default();
                    only.insert(scan, r);
                    self.run(node.inputs[1], &mut only)?
                }
            }
            Seek::Nodes { scan } => {
                let Op::NodeScan { var, labels, belief, vt_mode, .. } = self.plan.nodes[scan].op.clone() else {
                    unreachable!("seek_plan checks the op")
                };
                let n_ids = self.store.dict().len() as usize;
                let mut pairs = if f.len() <= POINT_READS_MAX.max(n_ids / POINT_READS_FRACTION) {
                    let uids: Vec<String> = f.iter().filter_map(|&i| self.store.dict().uid(i).map(str::to_string)).collect();
                    self.node_versions_of(&uids, belief, vt_mode)?
                } else {
                    // past the point reads' crossover: the id column against a bitset
                    let mut want = vec![false; n_ids];
                    for &i in &f {
                        if (i as usize) < n_ids {
                            want[i as usize] = true;
                        }
                    }
                    self.node_rows_where(belief, vt_mode, |sc, r| want.get(sc.a[r] as usize).copied().unwrap_or(false))?
                        .into_iter()
                        .map(|a| (self.nodes[seg_of(a)].a[row_of(a)], a))
                        .collect()
                };
                let wanted: Option<Vec<u32>> = labels.as_ref().map(|l| l.iter().map(|s| self.labels.code(s)).collect());
                if let Some(w) = &wanted {
                    let mut kept = Vec::with_capacity(pairs.len());
                    for (id, a) in pairs {
                        if w.contains(&self.label_code(a)?) {
                            kept.push((id, a));
                        }
                    }
                    pairs = kept;
                }
                pairs.sort_unstable_by_key(|p| p.1);
                pairs.dedup();
                Rel {
                    n: pairs.len(),
                    vars: vec![(var, Var::Node { ids: pairs.iter().map(|p| p.0).collect(), ver: pairs.iter().map(|p| p.1).collect(), depth: None })],
                    cols: Vec::new(),
                }
            }
        };
        if trace() {
            eprintln!(
                "match trace:   index join ({}): {} keys -> {} right rows in {:.3} ms",
                if matches!(seek, Seek::Nodes { .. }) { "nodes" } else { "edges" },
                f.len(),
                right.n,
                lap(t0).as_secs_f64() * 1e3
            );
        }
        self.apply(&node.op, vec![left, right])
    }

    /// The adjacency-index rows `edge_seek_any` reads for `keys`: the
    /// slices' lengths, before any test.
    fn edge_seek_reads(&self, keys: &[u32], dir: NeighborDir) -> DResult<usize> {
        use crate::tcsr::Direction;
        let files: FxMap<u64, String> = self.store.edge_files().into_iter().map(|(f, id)| (id, f)).collect();
        let dirs: &[Direction] = match dir {
            NeighborDir::Out => &[Direction::Out],
            NeighborDir::In => &[Direction::In],
            NeighborDir::Both => &[Direction::Out, Direction::In],
        };
        let mut n = 0usize;
        for s in &self.edges {
            if s.seg.rows() == 0 || s.seg.tt_s_bounds().0 > self.plan.as_of {
                continue;
            }
            let Some(file) = files.get(&s.id) else { return unsupported("an edge segment without a file") };
            let idx = self.store.tcsr_index(file)?;
            for &d in dirs {
                let side = idx.dir(d);
                n += keys.iter().map(|&u| side.slice(u).len()).sum::<usize>();
            }
        }
        Ok(n)
    }

    /// An EdgeScan under a belief other than current, restricted to the edges
    /// with an end (`dir`) in `keys`: each segment's adjacency-index slices
    /// for the keys, then the scan's own tests on those rows alone
    /// (relationship, valid-time overlap, belief), in the scan's order
    /// (segment, then row). The same rows as the scan's that end in a key.
    fn edge_seek_any(&self, keys: &[u32], dir: NeighborDir, rels: Option<&[String]>, belief: BeliefMode) -> DResult<Vec<Addr>> {
        use crate::tcsr::Direction;
        let files: FxMap<u64, String> = self.store.edge_files().into_iter().map(|(f, id)| (id, f)).collect();
        let codes: Option<Vec<u32>> = rels.map(|r| r.iter().filter_map(|x| self.rels.get(x)).collect());
        let dirs: &[Direction] = match dir {
            NeighborDir::Out => &[Direction::Out],
            NeighborDir::In => &[Direction::In],
            NeighborDir::Both => &[Direction::Out, Direction::In],
        };
        let (va, vb) = (self.plan.t_a, self.plan.t_b);
        let mut out = Vec::new();
        for (si, s) in self.edges.iter().enumerate() {
            if s.seg.rows() == 0 || s.seg.tt_s_bounds().0 > self.plan.as_of {
                continue;
            }
            let Some(file) = files.get(&s.id) else { return unsupported("an edge segment without a file") };
            let idx = self.store.tcsr_index(file)?;
            for &u in keys {
                for &d in dirs {
                    for &r in idx.dir(d).slice(u) {
                        let a = addr(si, r);
                        if let Some(c) = &codes {
                            if !c.contains(&self.rel_code(a)) {
                                continue;
                            }
                        }
                        let row = r as usize;
                        let (vs, ve) = (s.vt_s[row], s.vt_e(row));
                        if !(vs < vb && va < ve) || !self.vt_ok(vs, ve, VtMode::Overlap) {
                            continue;
                        }
                        if self.believed(RowKind::Edge, a, belief)? {
                            out.push(a);
                        }
                    }
                }
            }
        }
        out.sort_unstable();
        out.dedup();
        Ok(out)
    }

    /// A maximal run of `Filter`/`TypeConstraint` nodes ending at `id`, each
    /// below the top used once: `(the predicates bottom-up, the node under
    /// the run)`. `None` for a single predicate over anything but an
    /// expansion (nothing to fuse).
    fn filter_chain(&self, id: usize) -> Option<(Vec<Op>, usize)> {
        let is_pred = |op: &Op| matches!(op, Op::Filter(_) | Op::TypeConstraint { .. });
        if !is_pred(&self.plan.nodes[id].op) {
            return None;
        }
        let mut preds = vec![self.plan.nodes[id].op.clone()];
        let mut cur = self.plan.nodes[id].inputs[0];
        while is_pred(&self.plan.nodes[cur].op) && self.uses(cur) == 1 {
            preds.push(self.plan.nodes[cur].op.clone());
            cur = self.plan.nodes[cur].inputs[0];
        }
        preds.reverse();
        let under_expand = matches!(self.plan.nodes[cur].op, Op::Expand { .. }) && self.uses(cur) == 1;
        (preds.len() > 1 || under_expand).then_some((preds, cur))
    }

    /// Every predicate of the run evaluated against one relation and the
    /// rows cut once. Over an expansion, the predicates see only the new
    /// variables and the input columns they name, and the input is gathered
    /// once, for the surviving rows: the same rows in the same order as
    /// filtering after each step, without copying the binding table per step.
    fn run_filter_chain(&mut self, chain: &(Vec<Op>, usize), memo: &mut FxMap<usize, Rel>) -> DResult<Rel> {
        let (preds, under) = chain;
        let Op::Expand { from, into, hops, rel_type, dir, edge_var } = self.plan.nodes[*under].op.clone() else {
            let input = self.run(*under, memo)?;
            let mut mask = vec![true; input.n];
            for p in preds {
                self.and_mask(p, &input, &mut mask)?;
            }
            let idx: Vec<usize> = (0..input.n).filter(|&i| mask[i]).collect();
            return Ok(if idx.len() == input.n { input } else { input.take(&idx) });
        };
        let input = self.run(self.plan.nodes[*under].inputs[0], memo)?;
        // the closing equality directly above the expansion becomes its target
        let mut rest: &[Op] = preds;
        let mut want: Option<Vec<u32>> = None;
        if let Some(Op::Filter(Expr::Cmp(CmpOp::Eq, l, r))) = preds.first() {
            if let (Expr::Col(lc), Expr::Col(rc)) = (l.as_ref(), r.as_ref()) {
                if lc.strip_suffix(".uid") == Some(into.as_str()) && !rc.starts_with(&format!("{into}.")) {
                    want = Some(self.uid_column(&input, rc)?);
                    rest = &preds[1..];
                }
            }
        }
        // predicates that read the target alone are decided once per distinct
        // target, inside the expansion, before any row is made
        let (local, rest): (Vec<Op>, Vec<Op>) = rest.iter().cloned().partition(|p| target_local(p, &into));
        let rest: &[Op] = &rest;
        let t_exp = std::time::Instant::now();
        let semi = self.semi_sets(*under, &input)?;
        let (rows, vars) = self.expansion(
            &input, &from, &into, hops, rel_type.as_deref(), dir, edge_var.as_deref(), want.as_deref(), &local,
            semi.as_ref(),
        )?;
        if trace() {
            eprintln!("match trace:   expansion -> {} rows in {:.3} ms", rows.len(), t_exp.elapsed().as_secs_f64() * 1e3);
        }
        // a thin relation: the new variables and the input columns named
        let mut names: Vec<String> = Vec::new();
        for p in rest {
            match p {
                Op::Filter(e) => expr_names(e, &mut names),
                Op::TypeConstraint { var, .. } => names.push(var.clone()),
                _ => {}
            }
        }
        let wanted = |k: &str| names.iter().any(|n| n == k || n.split('.').next() == Some(k));
        let mut thin = Rel { n: rows.len(), vars: Vec::new(), cols: Vec::new() };
        for (k, v) in &input.vars {
            if wanted(k) && !vars.iter().any(|(nk, _)| nk == k) {
                thin.vars.push((k.clone(), v.take(&rows)));
            }
        }
        for (k, c) in &input.cols {
            if wanted(k) {
                thin.cols.push((k.clone(), c.take(&rows)));
            }
        }
        thin.vars.extend(vars.iter().cloned());
        // positions (into `rows`) still alive; the thin relation is cut after
        // each predicate, so the next one reads only the surviving rows
        let mut alive: Vec<usize> = (0..thin.n).collect();
        for p in rest {
            let t_p = std::time::Instant::now();
            let mut mask = vec![true; thin.n];
            self.and_mask(p, &thin, &mut mask)?;
            let idx: Vec<usize> = (0..thin.n).filter(|&i| mask[i]).collect();
            if idx.len() != thin.n {
                alive = idx.iter().map(|&i| alive[i]).collect();
                thin = thin.take(&idx);
            }
            if trace() {
                eprintln!("match trace:   predicate {} -> {} in {:.3} ms", op_name(p), alive.len(),
                          t_p.elapsed().as_secs_f64() * 1e3);
            }
        }
        let keep = alive;
        let kept_rows: Vec<usize> = keep.iter().map(|&i| rows[i]).collect();
        let mut out = input.take(&kept_rows);
        for (k, v) in vars {
            out.vars.push((k, v.take(&keep)));
        }
        Ok(out)
    }

    /// The candidate targets (sorted, distinct) that pass every predicate
    /// reading `into` alone, as a sorted list, with their versions.
    fn targets_passing(&mut self, preds: &[Op], into: &str, cand: Vec<u32>) -> DResult<Passing> {
        let ver = self.versions_for(&cand)?;
        let rel = Rel {
            n: cand.len(),
            vars: vec![(into.to_string(), Var::Node { ids: cand.clone(), ver: ver.clone(), depth: None })],
            cols: Vec::new(),
        };
        let mut mask = vec![true; rel.n];
        for p in preds {
            self.and_mask(p, &rel, &mut mask)?;
        }
        Ok(cand.into_iter().zip(ver).zip(mask).filter_map(|(vv, m)| m.then_some(vv)).unzip())
    }

    /// `mask &= predicate` over `rel`.
    fn and_mask(&mut self, p: &Op, rel: &Rel, mask: &mut [bool]) -> DResult<()> {
        match p {
            Op::Filter(e) => {
                let m = self.predicate(e, rel)?;
                for (x, y) in mask.iter_mut().zip(m) {
                    *x &= y;
                }
            }
            Op::TypeConstraint { var, labels } => {
                let wanted: Vec<u32> = labels.iter().map(|s| self.labels.code(s)).collect();
                let ver = match rel.var(var) {
                    Some(Var::Node { ver, .. }) => ver,
                    _ => return unsupported("TypeConstraint on a non-node variable"),
                };
                if ver.len() < par_min() {
                    // the row loop (the cached label codes are one array read)
                    for (i, &a) in ver.iter().enumerate() {
                        if mask[i] && (a == NO_ADDR || !wanted.contains(&self.label_code(a)?)) {
                            mask[i] = false;
                        }
                    }
                    return Ok(());
                }
                // each live row's (segment, label tag), in chunks on the pool;
                // each distinct pair's label resolved once, in row order (the
                // first row to need it, as the row loop met it)
                const NONE: u64 = u64::MAX;
                const GONE: u64 = u64::MAX - 1;
                let this: &Self = self;
                let m: &[bool] = mask;
                let tags: Vec<u64> = par_concat(ver.len(), |r| {
                    r.map(|i| {
                        let a = ver[i];
                        if !m[i] {
                            NONE
                        } else if a == NO_ADDR {
                            GONE
                        } else {
                            ((seg_of(a) as u64) << 32) | this.nodes[seg_of(a)].tag[row_of(a)] as u64
                        }
                    })
                    .collect()
                });
                let mut ok: FxMap<u64, bool> = FxMap::default();
                for (i, &t) in tags.iter().enumerate() {
                    if t < GONE && !ok.contains_key(&t) {
                        let c = self.label_code(ver[i])?;
                        ok.insert(t, wanted.contains(&c));
                    }
                }
                for (slot, &t) in mask.iter_mut().zip(&tags) {
                    if t == GONE || (t != NONE && !ok[&t]) {
                        *slot = false;
                    }
                }
            }
            _ => return unsupported("a predicate node"),
        }
        Ok(())
    }

    /// The plan's semi-join hints, found once: for each closing step
    /// `Filter(c.uid = T)` directly over a one-hop `Expand(X -> c)`, the
    /// expansion that bound `X`, when only row filters and expansions lie
    /// between them and `T` is bound below that expansion.
    fn hints(&mut self) -> &FxMap<usize, SemiHint> {
        if self.semi.is_none() {
            let plan = self.plan;
            let mut out: FxMap<usize, SemiHint> = FxMap::default();
            for node in &plan.nodes {
                let Op::Filter(Expr::Cmp(CmpOp::Eq, l, r)) = &node.op else { continue };
                let (Expr::Col(lc), Expr::Col(tc)) = (l.as_ref(), r.as_ref()) else { continue };
                let Some(&e2) = node.inputs.first() else { continue };
                let Op::Expand { from, into, hops: Hops::Exact(1), rel_type, dir, .. } = &plan.nodes[e2].op else {
                    continue;
                };
                if lc.strip_suffix(".uid") != Some(into.as_str()) {
                    continue;
                }
                let Some(x) = from.strip_suffix(".uid") else { continue };
                // walk down to the expansion that bound X
                let mut cur = plan.nodes[e2].inputs[0];
                let found = loop {
                    match &plan.nodes[cur].op {
                        Op::Expand { into: i, .. } if i == x => break Some(cur),
                        Op::Filter(_) | Op::TypeConstraint { .. } | Op::Expand { .. } => {}
                        Op::Project { keep_all: true, .. } => {}
                        _ => break None,
                    }
                    cur = plan.nodes[cur].inputs[0];
                };
                let Some(e1) = found else { continue };
                // T must be bound below e1
                let tvar = tc.strip_suffix(".uid").unwrap_or(tc);
                let mut below = plan.nodes[e1].inputs.first().copied();
                let mut bound = false;
                while let Some(b) = below {
                    match &plan.nodes[b].op {
                        Op::NodeScan { var, .. } if var == tvar => bound = true,
                        Op::Expand { into: i, .. } if i == tvar => bound = true,
                        _ => {}
                    }
                    if bound {
                        break;
                    }
                    below = plan.nodes[b].inputs.first().copied().filter(|_| plan.nodes[b].inputs.len() == 1);
                }
                if bound {
                    out.insert(e1, SemiHint { target: tc.clone(), rel: rel_type.clone(), dir: *dir });
                }
            }
            self.semi = Some(out);
        }
        self.semi.as_ref().expect("built above")
    }

    /// The semi-join sets of an expansion with a hint: per distinct target
    /// value (at most 1,024 of them), its neighbours in the reverse direction.
    fn semi_sets(&mut self, exp: usize, input: &Rel) -> DResult<Option<SemiSets>> {
        let Some(h) = self.hints().get(&exp).cloned() else { return Ok(None) };
        let tcol = self.uid_column(input, &h.target)?;
        let mut distinct: Vec<u32> = tcol.iter().copied().filter(|&v| v != NO_ID).collect();
        distinct.sort_unstable();
        distinct.dedup();
        if distinct.is_empty() || distinct.len() > 1024 {
            return Ok(None);
        }
        let back = match h.dir {
            NeighborDir::Out => NeighborDir::In,
            NeighborDir::In => NeighborDir::Out,
            NeighborDir::Both => NeighborDir::Both,
        };
        let rels: Option<Vec<String>> = h.rel.map(|r| vec![r]);
        let adj = self.neighbors(&distinct, back, self.plan.t_a, rels.as_deref())?;
        let mut sets: FxMap<u32, Vec<u32>> = FxMap::default();
        for (p, list) in adj.iter().enumerate() {
            let mut v: Vec<u32> = list.iter().map(|&(n, _)| n).collect();
            v.sort_unstable();
            v.dedup();
            sets.insert(distinct[p], v);
        }
        if trace() {
            eprintln!("match trace:   semi-join on {} ({} targets)", h.target, distinct.len());
        }
        Ok(Some((tcol, sets)))
    }

    /// `Some((expand node, target column))` when `node` is the compiler's
    /// cycle-closing filter `into.uid = target` directly over the `Expand`
    /// that bound a fresh `into`, used by nothing else.
    fn closing_expand(&self, node: &PNode) -> Option<(usize, String)> {
        let Op::Filter(Expr::Cmp(CmpOp::Eq, l, r)) = &node.op else { return None };
        let (Expr::Col(lc), Expr::Col(rc)) = (l.as_ref(), r.as_ref()) else { return None };
        let exp = *node.inputs.first()?;
        let Op::Expand { into, .. } = &self.plan.nodes[exp].op else { return None };
        if lc.strip_suffix(".uid") != Some(into.as_str()) || rc.starts_with(&format!("{into}.")) {
            return None;
        }
        let uses = self.plan.nodes.iter().filter(|n| n.inputs.contains(&exp)).count();
        (uses == 1).then(|| (exp, rc.clone()))
    }

    fn apply(&mut self, op: &Op, mut inputs: Vec<Rel>) -> DResult<Rel> {
        match op {
            Op::NodeScan { var, labels, uids, belief, vt_mode } => {
                let by_label = match (uids, labels) {
                    (None, Some(l)) => self.labelled_node_addrs(l, *belief, *vt_mode)?,
                    _ => None,
                };
                // rows read by label carry one of the labels already
                let by_labels = by_label.is_some();
                let pairs: Vec<(u32, Addr)> = match (uids, by_label) {
                    (Some(u), _) => self.node_versions_of(u, *belief, *vt_mode)?,
                    (None, Some(addrs)) => {
                        let this: &Self = self;
                        par_concat(addrs.len(), |r| {
                            addrs[r].iter().map(|&a| (this.nodes[seg_of(a)].a[row_of(a)], a)).collect()
                        })
                    }
                    (None, None) => self
                        .scan_addrs(RowKind::Node, *belief, *vt_mode, None)?
                        .into_iter()
                        .map(|a| (self.nodes[seg_of(a)].a[row_of(a)], a))
                        .collect(),
                };
                let wanted: Option<Vec<u32>> = if by_labels {
                    None
                } else {
                    labels.as_ref().map(|l| l.iter().map(|s| self.labels.code(s)).collect())
                };
                let keep = match &wanted {
                    None => pairs,
                    Some(w) => {
                        let mut keep = Vec::with_capacity(pairs.len());
                        for (id, a) in pairs {
                            if w.contains(&self.label_code(a)?) {
                                keep.push((id, a));
                            }
                        }
                        keep
                    }
                };
                let n = keep.len();
                let ids = par_concat(n, |r| keep[r].iter().map(|p| p.0).collect());
                let ver = par_concat(n, |r| keep[r].iter().map(|p| p.1).collect());
                Ok(Rel {
                    n,
                    vars: vec![(var.clone(), Var::Node { ids, ver, depth: None })],
                    cols: Vec::new(),
                })
            }
            Op::EdgeScan { var, rel_types, belief, vt_mode } => {
                let addrs = self.scan_addrs(RowKind::Edge, *belief, *vt_mode, rel_types.as_deref())?;
                // the version-key route does not test relation names per row
                let addrs: Vec<Addr> = match rel_types {
                    Some(r) => {
                        let codes: Vec<u32> = r.iter().filter_map(|s| self.rels.get(s)).collect();
                        addrs.into_iter().filter(|&a| codes.contains(&self.rel_code(a))).collect()
                    }
                    None => addrs,
                };
                Ok(Rel { n: addrs.len(), vars: vec![(var.clone(), Var::Edge { ver: addrs })], cols: Vec::new() })
            }
            Op::Expand { from, into, hops, rel_type, dir, edge_var } => {
                let input = inputs.pop().expect("one input");
                self.expand(input, from, into, *hops, rel_type.as_deref(), *dir, edge_var.as_deref())
            }
            Op::Filter(pred) => {
                let input = inputs.pop().expect("one input");
                let mask = self.predicate(pred, &input)?;
                let idx: Vec<usize> = (0..input.n).filter(|&i| mask[i]).collect();
                Ok(if idx.len() == input.n { input } else { input.take(&idx) })
            }
            Op::TypeConstraint { var, labels } => {
                let input = inputs.pop().expect("one input");
                let wanted: Vec<u32> = labels.iter().map(|s| self.labels.code(s)).collect();
                let ver = match input.var(var) {
                    Some(Var::Node { ver, .. }) => ver.clone(),
                    _ => return unsupported("TypeConstraint on a non-node variable"),
                };
                let mut idx = Vec::with_capacity(input.n);
                for (i, &a) in ver.iter().enumerate() {
                    if a != NO_ADDR && wanted.contains(&self.label_code(a)?) {
                        idx.push(i);
                    }
                }
                Ok(input.take(&idx))
            }
            Op::Project { bindings, keep_all } => {
                let input = inputs.pop().expect("one input");
                let mut cols = Vec::with_capacity(bindings.len());
                for (name, e) in bindings {
                    cols.push((name.clone(), self.eval(e, &input)?));
                }
                if *keep_all {
                    let mut out = input;
                    for (name, c) in cols {
                        out.set_col(&name, c);
                    }
                    Ok(out)
                } else {
                    Ok(Rel { n: input.n, vars: Vec::new(), cols })
                }
            }
            Op::Join { on, kind } => {
                let right = inputs.pop().expect("two inputs");
                let left = inputs.pop().expect("two inputs");
                self.join(left, right, on, *kind)
            }
            Op::Aggregate { group_by, aggs } => {
                let input = inputs.pop().expect("one input");
                self.aggregate(&input, group_by, aggs)
            }
            Op::Order(keys) => {
                let input = inputs.pop().expect("one input");
                let perm = self.order(&input, keys, None)?;
                Ok(input.take(&perm))
            }
            Op::Limit(n) => {
                let input = inputs.pop().expect("one input");
                let k = (*n).min(input.n);
                Ok(if k == input.n { input } else { input.take(&(0..k).collect::<Vec<_>>()) })
            }
            Op::Union => self.union(inputs),
            Op::Succ { of, into, edge, valid_axis, next, belief, vt_mode } => {
                let input = inputs.pop().expect("one input");
                self.succ(input, of, into, *edge, *valid_axis, *next, *belief, *vt_mode)
            }
            Op::OrderedReach(r) => {
                let edges = inputs.pop().expect("two inputs");
                let input = inputs.pop().expect("two inputs");
                let (rows, vars) = self.ordered_reach(&input, &edges, r)?;
                let mut out = input.take(&rows);
                out.vars.extend(vars);
                Ok(out)
            }
            Op::Walk { from, into, nfa } => {
                let input = inputs.pop().expect("one input");
                let (rows, vars) = self.walk(&input, from, into, nfa)?;
                let mut out = input.take(&rows);
                out.vars.extend(vars);
                Ok(out)
            }
            Op::CoValidExpand(c) => {
                let input = inputs.pop().expect("one input");
                self.covalid_expand(input, c)
            }
            Op::CoValidJoin(j) => {
                let right = inputs.pop().expect("two inputs");
                let left = inputs.pop().expect("two inputs");
                self.covalid_join(left, right, j)
            }
            Op::Collapse { by, mins } => {
                let input = inputs.pop().expect("one input");
                self.collapse(input, by, mins)
            }
            Op::TopK { by, keys, n } => {
                let input = inputs.pop().expect("one input");
                let perm = self.order(&input, keys, None)?;
                let parts: Vec<Vec<KeyVal>> =
                    by.iter().map(|e| self.eval(e, &input).and_then(|c| self.keys_of(&c))).collect::<DResult<_>>()?;
                let mut taken: HashMap<Vec<KeyVal>, usize> = HashMap::new();
                let mut keep = Vec::new();
                for i in perm {
                    let k: Vec<KeyVal> = parts.iter().map(|p| p[i].clone()).collect();
                    let c = taken.entry(k).or_insert(0);
                    if *c < *n {
                        *c += 1;
                        keep.push(i);
                    }
                }
                Ok(input.take(&keep))
            }
        }
    }

    /// The union's inputs are projections: columns only, one list of names.
    fn union(&mut self, inputs: Vec<Rel>) -> DResult<Rel> {
        if inputs.iter().any(|r| !r.vars.is_empty()) {
            return self.union_vars(inputs);
        }
        let names: Vec<String> = inputs[0].cols.iter().map(|(k, _)| k.clone()).collect();
        if inputs.iter().any(|r| !r.vars.is_empty() || r.cols.iter().map(|(k, _)| k).ne(names.iter())) {
            return unsupported("a union of relations with different columns");
        }
        let n: usize = inputs.iter().map(|r| r.n).sum();
        check_rows(n)?;
        let mut cols = Vec::with_capacity(names.len());
        for (j, name) in names.iter().enumerate() {
            let parts: Vec<&Col> = inputs.iter().map(|r| &r.cols[j].1).collect();
            cols.push((name.clone(), self.concat_cols(&parts, n)?));
        }
        Ok(Rel { n, vars: Vec::new(), cols })
    }

    /// Columns stacked in order. One value class each: integers and floats
    /// widen to floats, text classes (strings, ids, labels, relationship
    /// names) to strings; an all-null part takes the others' class.
    fn concat_cols(&self, parts: &[&Col], n: usize) -> DResult<Col> {
        use Vals::*;
        let typed: Vec<&&Col> = parts.iter().filter(|c| !matches!(c.v, Null(_))).collect();
        let mut null = Vec::with_capacity(n);
        for c in parts {
            for i in 0..c.len() {
                null.push(c.is_null(i));
            }
        }
        let any_null = null.iter().any(|&b| b);
        let null = any_null.then_some(null);
        let Some(first) = typed.first() else { return Ok(Col { v: Null(n), null: None }) };
        let same = typed.iter().all(|c| std::mem::discriminant(&c.v) == std::mem::discriminant(&first.v));
        macro_rules! stack {
            ($variant:ident, $fill:expr) => {{
                let mut out = Vec::with_capacity(n);
                for c in parts {
                    match &c.v {
                        $variant(x) => out.extend(x.iter().cloned()),
                        _ => out.extend(std::iter::repeat_n($fill, c.len())),
                    }
                }
                $variant(out)
            }};
        }
        let v = if same {
            match &first.v {
                I64(_) => stack!(I64, 0i64),
                F64(_) => stack!(F64, 0.0f64),
                Bool(_) => stack!(Bool, false),
                Str(_) => stack!(Str, Arc::<str>::from("")),
                Uid(_) => stack!(Uid, NO_ID),
                Vid(_) => stack!(Vid, (0u64, 0u32)),
                Eid(_) => stack!(Eid, NO_ADDR),
                Label(_) => stack!(Label, 0u32),
                RelType(_) => stack!(RelType, 0u32),
                _ => return unsupported("a union column of this class"),
            }
        } else if typed.iter().all(|c| matches!(c.v, I64(_) | F64(_))) {
            let mut out = Vec::with_capacity(n);
            for c in parts {
                match &c.v {
                    I64(x) => out.extend(x.iter().map(|&v| v as f64)),
                    F64(x) => out.extend(x.iter().copied()),
                    _ => out.extend(std::iter::repeat_n(0.0, c.len())),
                }
            }
            F64(out)
        } else if typed.iter().all(|c| matches!(c.v, Str(_) | Uid(_) | Label(_) | RelType(_))) {
            let mut out = Vec::with_capacity(n);
            for c in parts {
                for i in 0..c.len() {
                    out.push(if c.is_null(i) { Arc::from("") } else { self.text_at(c, i)?.unwrap_or_else(|| Arc::from("")) });
                }
            }
            Str(out)
        } else {
            return unsupported("a union column over two value classes");
        };
        Ok(Col { v, null })
    }

    /// The uid column `name` names: a node variable's ids, or a uid-typed
    /// key column.
    fn uid_column(&self, rel: &Rel, name: &str) -> DResult<Vec<u32>> {
        if let Some(var) = name.strip_suffix(".uid") {
            if let Some(Var::Node { ids, .. }) = rel.var(var) {
                return Ok(ids.clone());
            }
        }
        match rel.col(name) {
            Some(Col { v: Vals::Uid(ids), null }) => Ok(match null {
                Some(m) => ids.iter().zip(m).map(|(&i, &n)| if n { NO_ID } else { i }).collect(),
                None => ids.clone(),
            }),
            Some(Col { v: Vals::Null(n), .. }) => Ok(vec![NO_ID; *n]),
            _ => unsupported(format!("not a uid column: {name}")),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn expand(
        &mut self,
        input: Rel,
        from: &str,
        into: &str,
        hops: Hops,
        rel_type: Option<&str>,
        dir: NeighborDir,
        edge_var: Option<&str>,
    ) -> DResult<Rel> {
        self.expand_to(input, from, into, hops, rel_type, dir, edge_var, None)
    }

    /// `Expand`, and with `want` only the rows whose target is `want[row]`.
    #[allow(clippy::too_many_arguments)]
    fn expand_to(
        &mut self,
        input: Rel,
        from: &str,
        into: &str,
        hops: Hops,
        rel_type: Option<&str>,
        dir: NeighborDir,
        edge_var: Option<&str>,
        want: Option<&[u32]>,
    ) -> DResult<Rel> {
        let (rows, vars) = self.expansion(&input, from, into, hops, rel_type, dir, edge_var, want, &[], None)?;
        let mut out = input.take(&rows);
        out.vars.extend(vars);
        Ok(out)
    }

    /// The expansion alone: for each output row, the input row it extends
    /// and the newly bound variables (target, and edge for one hop).
    #[allow(clippy::too_many_arguments)]
    fn expansion(
        &mut self,
        input: &Rel,
        from: &str,
        into: &str,
        hops: Hops,
        rel_type: Option<&str>,
        dir: NeighborDir,
        edge_var: Option<&str>,
        want: Option<&[u32]>,
        target_preds: &[Op],
        semi: Option<&SemiSets>,
    ) -> DResult<Expansion> {
        let seeds = self.uid_column(input, from)?;
        let keep = |row: usize, v: u32| {
            want.is_none_or(|w| w[row] == v && v != NO_ID)
                && semi.is_none_or(|(tcol, sets)| {
                    sets.get(&tcol[row]).is_some_and(|set| set.binary_search(&v).is_ok())
                })
        };
        let rels: Option<Vec<String>> = rel_type.map(|r| vec![r.to_string()]);
        let t = self.plan.t_a;
        // (input row, target id, edge address, depth)
        let mut rows: Vec<usize> = Vec::new();
        let mut targets: Vec<u32> = Vec::new();
        let mut edges: Vec<Addr> = Vec::new();
        let mut depths: Vec<u32> = Vec::new();
        let mut passed: Option<Passing> = None;
        let t0 = trace().then(std::time::Instant::now);
        let (mut t_nb, mut t_pass) = (std::time::Duration::ZERO, std::time::Duration::ZERO);
        match hops {
            Hops::Exact(k) => {
                // walks: (input row, current node, last edge); a one-hop step
                // reads its rows from the seeds themselves
                let one = k == 1;
                let mut cur: Vec<(usize, u32, Addr)> = if one {
                    Vec::new()
                } else {
                    seeds.iter().enumerate().filter(|(_, &s)| s != NO_ID).map(|(i, &s)| (i, s, NO_ADDR)).collect()
                };
                for hop in 0..k {
                    let last = hop + 1 == k;
                    let frontier = if one {
                        sorted_distinct(seeds.iter().copied().filter(|&s| s != NO_ID).collect())
                    } else {
                        sorted_distinct(cur.iter().map(|c| c.1).collect())
                    };
                    let t1 = trace().then(std::time::Instant::now);
                    let mut adj = self.neighbors(&frontier, dir, t, rels.as_deref())?;
                    t_nb += lap(t1);
                    // one semi-join target for every row: filter each adjacency
                    // list once, not once per row that reaches it
                    if last {
                        if let Some((_, sets)) = semi {
                            if sets.len() == 1 {
                                let set = sets.values().next().expect("one set");
                                adj.retain(|(v, _)| set.binary_search(v).is_ok());
                            }
                        }
                    }
                    // the predicates on the target alone, once per distinct target
                    let pass = if last && !target_preds.is_empty() {
                        let cand = sorted_distinct(adj.iter().flatten().map(|&(v, _)| v).collect());
                        Some(self.targets_passing(target_preds, into, cand)?)
                    } else {
                        None
                    };
                    t_pass += lap(t1);
                    let ok = |v: u32| pass.as_ref().is_none_or(|p| p.0.binary_search(&v).is_ok());
                    let fp = FrontierPos::new(&frontier);
                    if last {
                        // the output columns themselves, in chunks of the
                        // source rows, concatenated in order
                        let src_n = if one { seeds.len() } else { cur.len() };
                        let at = |j: usize| -> Option<(usize, u32)> {
                            if one {
                                (seeds[j] != NO_ID).then_some((j, seeds[j]))
                            } else {
                                Some((cur[j].0, cur[j].1))
                            }
                        };
                        let (r3, t3, e3) = par_concat3(src_n, |rg| {
                            let (mut rr, mut tt, mut ee) = (Vec::new(), Vec::new(), Vec::new());
                            for j in rg {
                                let Some((i, u)) = at(j) else { continue };
                                let p = fp.get(u).expect("frontier holds every node");
                                for &(v, e) in &adj[p] {
                                    if keep(i, v) && ok(v) {
                                        rr.push(i);
                                        tt.push(v);
                                        ee.push(e);
                                    }
                                }
                            }
                            (rr, tt, ee)
                        });
                        check_rows(r3.len())?;
                        rows = r3;
                        targets = t3;
                        edges = e3;
                        passed = pass;
                        break;
                    }
                    let next = par_concat(cur.len(), |r| {
                        let mut next = Vec::with_capacity(r.len());
                        for &(i, u, _) in &cur[r] {
                            let p = fp.get(u).expect("frontier holds every node");
                            for &(v, e) in &adj[p] {
                                next.push((i, v, e));
                            }
                        }
                        next
                    });
                    check_rows(next.len())?;
                    cur = next;
                }
            }
            Hops::Bounded(_, _) | Hops::Unbounded(_) => {
                let (lo, hi) = match hops {
                    Hops::Bounded(a, b) => (a, Some(b)),
                    Hops::Unbounded(a) => (a, None),
                    Hops::Exact(_) => unreachable!(),
                };
                // per distinct seed, its shortest distances
                let mut distinct: Vec<u32> = seeds.iter().copied().filter(|&s| s != NO_ID).collect();
                distinct.sort_unstable();
                distinct.dedup();
                let tb = trace().then(std::time::Instant::now);
                let (off, reach) = self.bfs(&distinct, dir, t, rels.as_deref(), lo, hi)?;
                let t_bfs = lap(tb);
                let pass = if target_preds.is_empty() {
                    None
                } else {
                    let mut cand: Vec<u32> = reach.iter().map(|&(v, _)| v).collect();
                    cand.sort_unstable();
                    cand.dedup();
                    Some(self.targets_passing(target_preds, into, cand)?)
                };
                for (i, &s) in seeds.iter().enumerate() {
                    if s == NO_ID {
                        continue;
                    }
                    let p = distinct.binary_search(&s).expect("seed listed");
                    for &(v, d) in &reach[off[p]..off[p + 1]] {
                        if !keep(i, v) || pass.as_ref().is_some_and(|q| q.0.binary_search(&v).is_err()) {
                            continue;
                        }
                        rows.push(i);
                        targets.push(v);
                        edges.push(NO_ADDR);
                        depths.push(d);
                    }
                }
                check_rows(rows.len())?;
                passed = pass;
                if trace() {
                    eprintln!(
                        "match trace:     closure: {} seeds, {} reached, bfs {:.1} ms, then {:.1} ms",
                        distinct.len(),
                        reach.len(),
                        t_bfs.as_secs_f64() * 1e3,
                        (lap(tb) - t_bfs).as_secs_f64() * 1e3
                    );
                }
            }
        }
        let t_rows = lap(t0);
        // the target predicates read the passing targets' versions already
        let ver = match &passed {
            Some((ids, vers)) => par_concat(targets.len(), |r| {
                targets[r].iter().map(|v| vers[ids.binary_search(v).expect("every target passed")]).collect()
            }),
            None => self.versions_for(&targets)?,
        };
        if trace() {
            eprintln!(
                "match trace:     expansion parts: neighbors {:.1} ms, targets passing {:.1} ms, rows {:.1} ms, versions {:.1} ms",
                t_nb.as_secs_f64() * 1e3,
                (t_pass - t_nb).as_secs_f64() * 1e3,
                (t_rows - t_pass).as_secs_f64() * 1e3,
                (lap(t0) - t_rows).as_secs_f64() * 1e3
            );
        }
        let variable = !matches!(hops, Hops::Exact(_));
        let mut vars = vec![(
            into.to_string(),
            Var::Node { ids: targets, ver, depth: if variable { Some(depths) } else { None } },
        )];
        if let Some(e) = edge_var {
            vars.push((e.to_string(), Var::Edge { ver: edges }));
        }
        Ok((rows, vars))
    }

    /// The version after (or before) each row's `of` version among the
    /// versions of its identity the scan admits, by (axis start, version id).
    #[allow(clippy::too_many_arguments)]
    fn succ(
        &mut self,
        input: Rel,
        of: &str,
        into: &str,
        edge: bool,
        valid_axis: bool,
        next: bool,
        belief: BeliefMode,
        vt_mode: VtMode,
    ) -> DResult<Rel> {
        let kind = if edge { RowKind::Edge } else { RowKind::Node };
        let addrs = self.scan_addrs(kind, belief, vt_mode, None)?;
        // per identity, its versions; an edge's identity by its key
        let mut chains: FxMap<IdentityKey, Vec<VersionAt>> = FxMap::default();
        for &a in &addrs {
            let key = if edge {
                let (x, y, r, d) = self.eid_key(a)?;
                (x, y, r, Arc::from(d))
            } else {
                (self.nodes[seg_of(a)].a[row_of(a)], 0, 0, Arc::from(""))
            };
            let start = if valid_axis { self.seg(kind, a).vt_s[row_of(a)] } else { self.tt_s(kind, a)? };
            chains.entry(key).or_default().push((start, self.vid(kind, a), a));
        }
        let mut following: FxMap<Addr, Addr> = FxMap::default();
        for list in chains.values_mut() {
            list.sort_unstable();
            for p in 0..list.len() {
                let q = if next { p + 1 } else { p.wrapping_sub(1) };
                if q < list.len() {
                    following.insert(list[p].2, list[q].2);
                }
            }
        }
        let ver: Vec<Addr> = match input.var(of) {
            Some(Var::Node { ver, .. }) if !edge => ver.clone(),
            Some(Var::Edge { ver }) if edge => ver.clone(),
            _ => return unsupported("succ of a variable of another kind"),
        };
        let mut rows = Vec::new();
        let mut picked = Vec::new();
        for (i, a) in ver.iter().enumerate() {
            if let Some(&b) = following.get(a) {
                rows.push(i);
                picked.push(b);
            }
        }
        let mut out = input.take(&rows);
        let var = if edge {
            Var::Edge { ver: picked }
        } else {
            let ids = picked.iter().map(|&a| self.nodes[seg_of(a)].a[row_of(a)]).collect();
            Var::Node { ids, ver: picked, depth: None }
        };
        out.vars.push((into.to_string(), var));
        Ok(out)
    }

    /// Shortest valid paths from each distinct seed over the edge relation:
    /// level by level, a node re-entering the frontier only when it arrives
    /// strictly better (earlier along an increasing order) than before.
    fn ordered_reach(&mut self, input: &Rel, edges: &Rel, r: &Reach) -> DResult<Expansion> {
        let t0 = trace().then(std::time::Instant::now);
        let src = self.uid_column(edges, &r.src)?;
        let dst = self.uid_column(edges, &r.dst)?;
        let tcol = self.eval(&Expr::Col(r.time.clone()), edges)?;
        let Vals::I64(times) = &tcol.v else { return unsupported("a reach time column of this type") };
        // the truncation key, as an ordered float (an integer column widened)
        let keys: Option<Vec<Option<f64>>> = match &r.key {
            None => None,
            Some(k) => {
                let c = self.eval(&Expr::Col(k.clone()), edges)?;
                let v: Vec<Option<f64>> = match &c.v {
                    Vals::I64(x) => (0..c.len()).map(|i| (!c.is_null(i)).then(|| x[i] as f64)).collect(),
                    Vals::F64(x) => (0..c.len()).map(|i| (!c.is_null(i)).then_some(x[i])).collect(),
                    Vals::Null(n) => vec![None; *n],
                    _ => return unsupported("a reach truncation key of this type"),
                };
                Some(v)
            }
        };
        let t_cols = lap(t0);
        // out-lists are loaded per level for the frontier's vertices not yet
        // loaded, one pass over the edge columns each; after REACH_SCANS_MAX
        // passes, every list at once. A loaded vertex's list is complete, so
        // the search sees the whole relation's adjacency.
        let usable = |i: usize| src[i] != NO_ID && dst[i] != NO_ID && !tcol.is_null(i);
        let entry = |i: usize| (keys.as_ref().and_then(|k| k[i]), times[i], dst[i]);
        let words = src.iter().filter(|&&x| x != NO_ID).max().map_or(0, |&m| m as usize / 64 + 1);
        let mut adj: FxMap<u32, Vec<(Option<f64>, i64, u32)>> = FxMap::default();
        let mut loaded = vec![0u64; words];
        let mut want = vec![0u64; words];
        let mut scans = 0usize;
        let mut full = false;
        // the truncation cuts a vertex's list when the search first expands
        // it: at SF1 a reach from one account expands a few hundred of the
        // million sources, and sorting every list was most of the call
        let mut cut: std::collections::HashSet<u32, BuildHasherDefault<Fx>> = Default::default();
        let cut_list = |list: &mut Vec<(Option<f64>, i64, u32)>, limit: usize| {
                let mut err = None;
                list.sort_by(|a, b| {
                    let o = match (a.0, b.0) {
                        (None, None) => Ordering::Equal,
                        (None, Some(_)) => Ordering::Greater,
                        (Some(_), None) => Ordering::Less,
                        (Some(x), Some(y)) => {
                            let o = x.partial_cmp(&y).unwrap_or(Ordering::Equal);
                            if r.key_desc { o.reverse() } else { o }
                        }
                    };
                    o.then(a.1.cmp(&b.1)).then_with(|| {
                        if a.2 == b.2 {
                            return Ordering::Equal;
                        }
                        match (self.uid_text(a.2), self.uid_text(b.2)) {
                            (Ok(x), Ok(y)) => x.cmp(y),
                            (Err(e), _) | (_, Err(e)) => {
                                err.get_or_insert(e);
                                Ordering::Equal
                            }
                        }
                    })
                });
                list.truncate(limit);
                err
        };
        let t_adj = lap(t0);
        let seeds = self.uid_column(input, &r.from)?;
        let mut distinct: Vec<u32> = seeds.iter().copied().filter(|&s| s != NO_ID).collect();
        distinct.sort_unstable();
        distinct.dedup();
        let sign = r.sign as i64;
        let mut total = 0usize;
        // every seed's search, level by level, so that one pass per level
        // loads the union of their frontiers; a seed's search is its own
        // (arrival times are i128 so that "before every edge" is below any)
        struct Search {
            dist: FxMap<u32, u32>,
            best: FxMap<u32, i128>,
            frontier: Vec<(u32, i128)>,
        }
        let mut searches: Vec<Search> = distinct
            .iter()
            .map(|&s| Search {
                dist: std::iter::once((s, 0)).collect(),
                best: std::iter::once((s, i128::MIN)).collect(),
                frontier: vec![(s, i128::MIN)],
            })
            .collect();
        let mut depth = 0u32;
        while searches.iter().any(|q| !q.frontier.is_empty()) && r.hi.is_none_or(|h| depth < h) {
            depth += 1;
            if !full {
                let mut any = false;
                for &(u, _) in searches.iter().flat_map(|q| q.frontier.iter()) {
                    let (w, b) = (u as usize / 64, 1u64 << (u % 64));
                    if w < words && loaded[w] & b == 0 {
                        want[w] |= b;
                        any = true;
                    }
                }
                if any {
                    scans += 1;
                    if scans > REACH_SCANS_MAX {
                        adj.clear();
                        cut.clear();
                        for i in 0..edges.n {
                            if usable(i) {
                                adj.entry(src[i]).or_default().push(entry(i));
                            }
                        }
                        full = true;
                    } else {
                        for i in 0..edges.n {
                            let u = src[i];
                            if u != NO_ID && want[u as usize / 64] & (1u64 << (u % 64)) != 0 && usable(i) {
                                adj.entry(u).or_default().push(entry(i));
                            }
                        }
                        for (l, w) in loaded.iter_mut().zip(want.iter_mut()) {
                            *l |= *w;
                            *w = 0;
                        }
                    }
                }
            }
            for q in searches.iter_mut() {
                if q.frontier.is_empty() {
                    continue;
                }
                let mut next: FxMap<u32, i128> = FxMap::default();
                for &(u, at) in &q.frontier {
                    let Some(list) = adj.get_mut(&u) else { continue };
                    if let Some(limit) = r.limit {
                        if cut.insert(u) {
                            if let Some(e) = cut_list(list, limit) {
                                return Err(e);
                            }
                        }
                    }
                    for &(_, t, v) in list.iter() {
                        let tt = (sign * t) as i128;
                        if sign != 0 && !(if r.strict { tt > at } else { tt >= at }) {
                            continue;
                        }
                        let arrive = if sign == 0 { i128::MIN } else { tt };
                        let slot = next.entry(v).or_insert(i128::MAX);
                        if arrive < *slot {
                            *slot = arrive;
                        }
                    }
                }
                total += next.len();
                check_rows(total)?;
                let mut nf: Vec<(u32, i128)> = Vec::new();
                for (v, arrive) in next {
                    q.dist.entry(v).or_insert(depth);
                    let b = q.best.entry(v).or_insert(i128::MAX);
                    if arrive < *b {
                        *b = arrive;
                        nf.push((v, arrive));
                    }
                }
                nf.sort_unstable();
                q.frontier = nf;
            }
        }
        let reached: Vec<Vec<(u32, u32)>> = searches
            .into_iter()
            .map(|q| {
                let mut out: Vec<(u32, u32)> = q
                    .dist
                    .into_iter()
                    .filter(|&(_, d)| d >= r.lo && r.hi.is_none_or(|h| d <= h))
                    .map(|(v, d)| (d, v))
                    .collect();
                out.sort_unstable();
                out
            })
            .collect();
        let mut rows = Vec::new();
        let mut targets = Vec::new();
        let mut depths = Vec::new();
        for (i, &s) in seeds.iter().enumerate() {
            if s == NO_ID {
                continue;
            }
            let si = distinct.binary_search(&s).expect("seed listed");
            for &(d, v) in &reached[si] {
                rows.push(i);
                targets.push(v);
                depths.push(d);
            }
        }
        check_rows(rows.len())?;
        let t_search = lap(t0);
        let ver = self.versions_for(&targets)?;
        if trace() {
            let ms = |d: std::time::Duration| d.as_secs_f64() * 1e3;
            eprintln!(
                "match trace:   reach over {} edges: columns {:.1} ms, adjacency {:.1} ms, search {:.1} ms, versions {:.1} ms",
                edges.n,
                ms(t_cols),
                ms(t_adj - t_cols),
                ms(t_search - t_adj),
                ms(lap(t0) - t_search)
            );
        }
        Ok((rows, vec![(r.into.clone(), Var::Node { ids: targets, ver, depth: Some(depths) })]))
    }

    /// A regular path step from every distinct seed at once: a
    /// level-synchronous search over (seed, node, state), one adjacency call
    /// per (relationship type, direction) per level for the union of the
    /// frontiers. A target is a node reached in the accepting state; per
    /// input row, its seed's targets ascending.
    fn walk(&mut self, input: &Rel, from: &str, into: &str, nfa: &Nfa) -> DResult<Expansion> {
        let t0 = std::time::Instant::now();
        let seeds = self.uid_column(input, from)?;
        let mut distinct: Vec<u32> = seeds.iter().copied().filter(|&s| s != NO_ID).collect();
        distinct.sort_unstable();
        distinct.dedup();
        let groups: Vec<Vec<u32>> = distinct.iter().map(|&s| vec![s]).collect();
        let (found, start_of, total) = self.walk_search(&groups, nfa)?;
        let t_bfs = t0.elapsed();
        let mut rows = Vec::new();
        let mut targets = Vec::new();
        for (i, &s) in seeds.iter().enumerate() {
            if s == NO_ID {
                continue;
            }
            let si = distinct.binary_search(&s).expect("seed listed");
            for &k in &found[start_of[si]..start_of[si + 1]] {
                rows.push(i);
                targets.push(k as u32);
            }
        }
        check_rows(rows.len())?;
        let ver = self.versions_for(&targets)?;
        if trace() {
            eprintln!(
                "match trace:   walk {} seeds, {} states searched, {} pairs: search {:.1} ms, bind {:.1} ms",
                distinct.len(),
                total,
                found.len(),
                t_bfs.as_secs_f64() * 1e3,
                (t0.elapsed() - t_bfs).as_secs_f64() * 1e3
            );
        }
        Ok((rows, vec![(into.to_string(), Var::Node { ids: targets, ver, depth: None })]))
    }

    /// The search of a walk from groups of seeds (a group's seeds search as
    /// one source): the (group, target) pairs packed in `u64`, sorted and
    /// once each, with each group's first position, and the states searched.
    fn walk_search(&mut self, groups: &[Vec<u32>], nfa: &Nfa) -> DResult<WalkFound> {
        let t = self.plan.t_a;
        let ns = nfa.eps.len();
        let closure: Vec<Vec<usize>> = (0..ns).map(|q| nfa.closure(q)).collect();
        // the symbols, and per symbol the (state, next state) moves on it
        let mut syms: Vec<SymMoves> = Vec::new();
        for (q, m) in nfa.moves.iter().enumerate() {
            for (r, inv, q2) in m {
                match syms.iter_mut().find(|(a, b, _)| a == r && b == inv) {
                    Some(s) => s.2.push((q, *q2)),
                    None => syms.push((r.clone(), *inv, vec![(q, *q2)])),
                }
            }
        }
        let pack = |si: usize, v: u32| -> u64 { (si as u64) << 32 | v as u64 };
        // per state: the (seed, node) pairs seen, and this level's frontier
        let mut seen: Vec<std::collections::HashSet<u64, BuildHasherDefault<Fx>>> = (0..ns).map(|_| Default::default()).collect();
        let mut frontier: Vec<Vec<u64>> = vec![Vec::new(); ns];
        let mut found: Vec<u64> = Vec::new();
        // the seeds stand in the start's closure without entering `seen`: a
        // seed met again there is recognised from its (sorted) group instead,
        // so a search from many seeds hashes only the nodes it reaches
        let mut in_start = vec![false; ns];
        for &q in &closure[nfa.start] {
            in_start[q] = true;
        }
        for (si, g) in groups.iter().enumerate() {
            debug_assert!(g.windows(2).all(|w| w[0] < w[1]), "a group's seeds are sorted and distinct");
            for &s in g {
                for &q in &closure[nfa.start] {
                    frontier[q].push(pack(si, s));
                }
            }
        }
        let is_seed = |si: usize, v: u32| groups[si].binary_search(&v).is_ok();
        found.extend(frontier[nfa.accept].iter().copied());
        let mut total = 0usize;
        while frontier.iter().any(|f| !f.is_empty()) {
            let mut next: Vec<Vec<u64>> = vec![Vec::new(); ns];
            for (rel, inv, moves) in &syms {
                let mut nodes: Vec<u32> = moves.iter().flat_map(|&(q, _)| frontier[q].iter().map(|&k| k as u32)).collect();
                if nodes.is_empty() {
                    continue;
                }
                nodes.sort_unstable();
                nodes.dedup();
                let dir = if *inv { NeighborDir::In } else { NeighborDir::Out };
                let adj = self.neighbors(&nodes, dir, t, Some(std::slice::from_ref(rel)))?;
                for &(q, q2) in moves {
                    for &k in &frontier[q] {
                        let (si, u) = ((k >> 32) as usize, k as u32);
                        let p = nodes.binary_search(&u).expect("frontier node listed");
                        for &(v, _) in &adj[p] {
                            let key = pack(si, v);
                            let seed = is_seed(si, v);
                            for &q3 in &closure[q2] {
                                if !(seed && in_start[q3]) && seen[q3].insert(key) {
                                    next[q3].push(key);
                                }
                            }
                        }
                    }
                }
            }
            found.extend(next[nfa.accept].iter().copied());
            total += next.iter().map(Vec::len).sum::<usize>();
            check_rows(total)?;
            frontier = next;
        }
        drop(seen);
        // (seed, target) pairs, each once, by seed then target
        found.sort_unstable();
        found.dedup();
        let mut start_of = vec![0usize; groups.len() + 1];
        for &k in &found {
            start_of[(k >> 32) as usize + 1] += 1;
        }
        for i in 0..groups.len() {
            start_of[i + 1] += start_of[i];
        }
        Ok((found, start_of, total))
    }

    /// `Aggregate` of plain columns (a distinct projection) directly over a
    /// `Walk` and target-only predicates, each used once: `(walk node, the
    /// predicates, the keys as (output name, input column))`.
    fn walk_distinct(&self, id: usize) -> Option<WalkDistinct> {
        let Op::Aggregate { group_by, aggs } = &self.plan.nodes[id].op else { return None };
        if !aggs.is_empty() {
            return None;
        }
        let keys: Vec<(String, String)> = group_by
            .iter()
            .map(|(n, e)| match e {
                Expr::Col(c) => Some((n.clone(), c.clone())),
                _ => None,
            })
            .collect::<Option<_>>()?;
        let mut preds = Vec::new();
        let mut cur = self.plan.nodes[id].inputs[0];
        loop {
            if self.uses(cur) != 1 {
                return None;
            }
            match &self.plan.nodes[cur].op {
                Op::Walk { into, .. } => {
                    if !preds.iter().all(|p| target_local(p, into)) {
                        return None;
                    }
                    preds.reverse();
                    return Some((cur, preds, keys));
                }
                op @ (Op::Filter(_) | Op::TypeConstraint { .. }) => {
                    preds.push(op.clone());
                    cur = self.plan.nodes[cur].inputs[0];
                }
                _ => return None,
            }
        }
    }

    /// The distinct projection of a walk without its binding table: per
    /// group of input rows (equal on the keys the input carries), the union
    /// of its seeds' targets that pass the predicates, each once. The rows
    /// are the projection's rows (in another order: an `Order` follows).
    fn run_walk_distinct(
        &mut self,
        plan: &WalkDistinct,
        memo: &mut FxMap<usize, Rel>,
    ) -> DResult<Rel> {
        let (wid, preds, keys) = plan;
        let Op::Walk { from, into, nfa } = self.plan.nodes[*wid].op.clone() else { unreachable!() };
        let input = self.run(self.plan.nodes[*wid].inputs[0], memo)?;
        let t0 = std::time::Instant::now();
        let seeds = self.uid_column(&input, &from)?;
        let target_col = format!("{into}.uid");
        let in_keys: Vec<&(String, String)> = keys.iter().filter(|(_, c)| *c != target_col).collect();
        let has_target = in_keys.len() < keys.len();
        let in_cols: Vec<Col> = in_keys.iter().map(|(_, c)| self.eval(&Expr::Col(c.clone()), &input)).collect::<DResult<_>>()?;
        let in_parts: Vec<Vec<KeyVal>> = in_cols.iter().map(|c| self.keys_of(c)).collect::<DResult<_>>()?;
        // groups of input rows (equal on the input's keys), first-seen, each
        // searching from all its seeds at once
        let mut index: FxMap<Vec<KeyVal>, usize> = FxMap::default();
        let mut first: Vec<usize> = Vec::new();
        let mut gseeds: Vec<Vec<u32>> = Vec::new();
        if in_parts.is_empty() {
            // no input key: every row is one group
            if let Some(i) = seeds.iter().position(|&s| s != NO_ID) {
                first.push(i);
                gseeds.push(seeds.iter().copied().filter(|&s| s != NO_ID).collect());
            }
        }
        for (i, &s) in seeds.iter().enumerate() {
            if s == NO_ID || in_parts.is_empty() {
                continue;
            }
            let k: Vec<KeyVal> = in_parts.iter().map(|p| p[i].clone()).collect();
            let g = *index.entry(k).or_insert_with(|| {
                first.push(i);
                gseeds.push(Vec::new());
                first.len() - 1
            });
            gseeds[g].push(s);
        }
        drop(index);
        for g in gseeds.iter_mut() {
            g.sort_unstable();
            g.dedup();
        }
        let (found, start_of, total) = self.walk_search(&gseeds, &nfa)?;
        let t_search = t0.elapsed();
        let pass = if preds.is_empty() {
            None
        } else {
            let mut cand: Vec<u32> = found.iter().map(|&k| k as u32).collect();
            cand.sort_unstable();
            cand.dedup();
            Some(self.targets_passing(preds, &into, cand)?)
        };
        let ok = |v: u32| pass.as_ref().is_none_or(|p| p.0.binary_search(&v).is_ok());
        let mut rows: Vec<usize> = Vec::new();
        let mut targets: Vec<u32> = Vec::new();
        for g in 0..gseeds.len() {
            let ts = found[start_of[g]..start_of[g + 1]].iter().map(|&k| k as u32).filter(|&v| ok(v));
            if has_target {
                for v in ts {
                    rows.push(first[g]);
                    targets.push(v);
                }
            } else if ts.take(1).count() == 1 {
                rows.push(first[g]);
            }
        }
        check_rows(rows.len())?;
        if trace() {
            eprintln!(
                "match trace:   distinct walk {} groups, {} states searched, {} rows: search {:.1} ms, total {:.1} ms",
                gseeds.len(),
                total,
                rows.len(),
                t_search.as_secs_f64() * 1e3,
                t0.elapsed().as_secs_f64() * 1e3
            );
        }
        let mut out = Rel { n: rows.len(), vars: Vec::new(), cols: Vec::new() };
        let mut j = 0;
        for (name, c) in keys {
            if *c == target_col {
                out.cols.push((name.clone(), Col { v: Vals::Uid(targets.clone()), null: None }));
            } else {
                out.cols.push((name.clone(), in_cols[j].take(&rows)));
                j += 1;
            }
        }
        Ok(out)
    }

    /// One hop for a sorted distinct frontier: per frontier node, its
    /// `(neighbour, edge address)` list under Σ (valid at the instant `t`,
    /// believed at `as_of`), through the per-segment adjacency index.
    fn neighbors(&mut self, frontier: &[u32], dir: NeighborDir, t: i64, rels: Option<&[String]>) -> DResult<NeighborLists> {
        if frontier.is_empty() {
            return Ok(NeighborLists::empty(0));
        }
        if let Some([rel]) = rels {
            let t0 = std::time::Instant::now();
            let got = self.neighbors_by_rel(frontier, dir, t, rel)?;
            if let Some(lists) = got {
                crate::phase::add(crate::phase::Phase::MatchHopNs, t0.elapsed().as_nanos() as u64);
                if trace() {
                    eprintln!("match trace:     hop by the relation's rows: {} nodes -> {} in {:.1} ms", frontier.len(), lists.items.len(), t0.elapsed().as_secs_f64() * 1e3);
                }
                return Ok(lists);
            }
        }
        let t0 = std::time::Instant::now();
        // a large frontier: its first keys by TCSR, and the whole hop by one
        // scan when the rows extrapolated to every key pass 1 /
        // SEEK_ROW_FACTOR of the scan's (the edge seek's rule)
        let mut probed: Option<crate::store::NeighborRows> = None;
        if frontier.len() > SEEK_PROBE && t == self.plan.t_a && self.plan.t_b == t + 1 {
            let probe = self.store.neighbors_batch(&frontier[..SEEK_PROBE], dir, self.plan.as_of, Some(t), Some(t + 1), rels)?;
            let t_probe = t0.elapsed();
            let est = probe.len() * frontier.len() / SEEK_PROBE;
            let scan = self.scan_beats_seek(est, rels)?;
            if trace() {
                eprintln!(
                    "match trace:     hop probe: {} nodes, {} rows estimated, scan {}: probe {:.1} ms, decision {:.1} ms",
                    frontier.len(),
                    est,
                    scan,
                    t_probe.as_secs_f64() * 1e3,
                    (t0.elapsed() - t_probe).as_secs_f64() * 1e3
                );
            }
            if scan {
                let lists = self.neighbors_by_scan(frontier, dir, rels)?;
                crate::phase::add(crate::phase::Phase::MatchHopNs, t0.elapsed().as_nanos() as u64);
                if trace() {
                    eprintln!("match trace:     hop by one scan: {} nodes -> {} in {:.1} ms", frontier.len(), lists.items.len(), t0.elapsed().as_secs_f64() * 1e3);
                }
                return Ok(lists);
            }
            probed = Some(probe);
        }
        // the probe's lists are the first `SEEK_PROBE` nodes' own: only the
        // rest are read, and the two concatenated in frontier order
        let nb = match probed {
            Some(mut first) => {
                let rest =
                    self.store.neighbors_batch(&frontier[SEEK_PROBE..], dir, self.plan.as_of, Some(t), Some(t + 1), rels)?;
                let base = first.nbr.len() as u32;
                first.nbr.extend(rest.nbr);
                first.vt_s.extend(rest.vt_s);
                first.vt_e.extend(rest.vt_e);
                first.seg_id.extend(rest.seg_id);
                first.seg_row.extend(rest.seg_row);
                first.offsets.extend(rest.offsets[1..].iter().map(|&o| o + base));
                first
            }
            None => self.store.neighbors_batch(frontier, dir, self.plan.as_of, Some(t), Some(t + 1), rels)?,
        };
        crate::phase::add(crate::phase::Phase::MatchHopNs, t0.elapsed().as_nanos() as u64);
        if trace() {
            eprintln!("match trace:     hop by the index: {} nodes -> {} in {:.1} ms", frontier.len(), nb.len(), t0.elapsed().as_secs_f64() * 1e3);
        }
        let mut items = Vec::with_capacity(nb.len());
        for j in 0..nb.len() {
            let Some(&si) = self.edge_by_id.get(&nb.seg_id[j]) else {
                return unsupported("an adjacency row outside the call's segments");
            };
            items.push((nb.nbr[j], addr(si, nb.seg_row[j])));
        }
        Ok(NeighborLists { off: nb.offsets.clone(), items })
    }

    /// `neighbors` at the call's instant for a sorted distinct frontier, by
    /// one scan of the relation: each frontier node's (neighbour, edge) list
    /// in `neighbors_batch`'s order -- by (vt_s, version id), an out-edge
    /// before an in-edge on a tie (a self-loop) -- so the expansion's rows
    /// and their order are the TCSR read's.
    fn neighbors_by_scan(&mut self, frontier: &[u32], dir: NeighborDir, rels: Option<&[String]>) -> DResult<NeighborLists> {
        let t0 = trace().then(std::time::Instant::now);
        let addrs = self.scan_addrs(RowKind::Edge, BeliefMode::Current, VtMode::Overlap, rels)?;
        let t_scan = lap(t0);
        let codes: Option<Vec<u32>> = rels.map(|r| r.iter().filter_map(|s| self.rels.get(s)).collect());
        let fp = FrontierPos::new(frontier);
        let at = |u: u32| fp.get(u);
        // (frontier position, far node, edge, orientation)
        type Hit = (u32, u32, Addr, u8);
        let addrs_len = addrs.len();
        let this: &Self = self;
        let hits: Vec<Hit> = par_concat(addrs.len(), |range| {
            let mut hits = Vec::new();
            for &a in &addrs[range] {
                if let Some(c) = &codes {
                    if !c.contains(&this.rel_code(a)) {
                        continue;
                    }
                }
                let s = &this.edges[seg_of(a)];
                let r = row_of(a);
                let (src, dst) = (s.a[r], s.b[r]);
                if matches!(dir, NeighborDir::Out | NeighborDir::Both) {
                    if let Some(p) = at(src) {
                        hits.push((p as u32, dst, a, 0));
                    }
                }
                if matches!(dir, NeighborDir::In | NeighborDir::Both) {
                    if let Some(p) = at(dst) {
                        hits.push((p as u32, src, a, 1));
                    }
                }
            }
            hits
        });
        let t_hits = lap(t0);
        // grouped by frontier position (counted, then filled), each list by key
        let mut off = vec![0u32; frontier.len() + 1];
        for h in &hits {
            off[h.0 as usize + 1] += 1;
        }
        for p in 0..frontier.len() {
            off[p + 1] += off[p];
        }
        // positions into `hits`, grouped; a list of two or more by
        // (vt_s, version id, orientation)
        let mut order = vec![0u32; hits.len()];
        let mut next = off.clone();
        for (j, h) in hits.iter().enumerate() {
            order[next[h.0 as usize] as usize] = j as u32;
            next[h.0 as usize] += 1;
        }
        let key = |j: &u32| {
            let h = &hits[*j as usize];
            let s = &this.edges[seg_of(h.2)];
            let r = row_of(h.2);
            (s.vt_s[r], s.vid_hi[r], s.vid_lo[r], h.3)
        };
        for w in off.windows(2) {
            if w[1] - w[0] > 1 {
                order[w[0] as usize..w[1] as usize].sort_unstable_by_key(key);
            }
        }
        let items = par_concat(order.len(), |r| {
            order[r].iter().map(|&j| (hits[j as usize].1, hits[j as usize].2)).collect()
        });
        let out = NeighborLists { off, items };
        if trace() {
            eprintln!(
                "match trace:     scan hop parts: scan {:.1} ms ({} rows), hits {:.1} ms, group {:.1} ms",
                t_scan.as_secs_f64() * 1e3,
                addrs_len,
                (t_hits - t_scan).as_secs_f64() * 1e3,
                (lap(t0) - t_hits).as_secs_f64() * 1e3
            );
        }
        Ok(out)
    }

    /// One edge segment's rare relationships and their rows, from the
    /// handle's cache or by one pass over its relationship column.
    fn rel_rows(&self, si: usize) -> DResult<RelRows> {
        let s = &self.edges[si];
        if let Ok(g) = self.store.match_rel_rows.lock() {
            if let Some(v) = g.get(&s.id) {
                return Ok(v.clone());
            }
        }
        let names = &s.seg.header().rel_types;
        let mut counts = vec![0usize; names.len()];
        for &c in s.rel {
            if let Some(n) = counts.get_mut(c as usize) {
                *n += 1;
            }
        }
        let rows_n = s.rel.len();
        let rare: Vec<bool> = counts.iter().map(|&n| n.saturating_mul(RARE_REL_FRACTION) <= rows_n).collect();
        let mut lists: Vec<Vec<u32>> = vec![Vec::new(); names.len()];
        for (row, &c) in s.rel.iter().enumerate() {
            if rare.get(c as usize).copied().unwrap_or(false) {
                lists[c as usize].push(row as u32);
            }
        }
        let mut rare_rows = Vec::new();
        let mut common = Vec::new();
        for (i, name) in names.iter().enumerate() {
            if counts[i] == 0 {
                continue;
            }
            if rare[i] {
                rare_rows.push((name.clone(), std::mem::take(&mut lists[i])));
            } else {
                common.push(name.clone());
            }
        }
        let v: RelRows = Arc::new((rare_rows, common));
        if let Ok(mut g) = self.store.match_rel_rows.lock() {
            g.insert(s.id, v.clone());
        }
        Ok(v)
    }

    /// One hop for a sorted frontier over one relationship rare in every
    /// segment holding it: its rows, valid at `t` and believed at `as_of`,
    /// whose bound end is in the frontier. Each node's list in the
    /// adjacency index's order, `(vt_s, vid)` with out before in on a tie.
    /// `None` (the adjacency index) for a small frontier or where the
    /// relationship is common.
    fn neighbors_by_rel(&self, frontier: &[u32], dir: NeighborDir, t: i64, rel: &str) -> DResult<Option<NeighborLists>> {
        if frontier.len() < REL_ROWS_FRONTIER_MIN {
            return Ok(None);
        }
        let mut lists: Vec<Vec<RelHit>> = vec![Vec::new(); frontier.len()];
        for si in 0..self.edges.len() {
            let s = &self.edges[si];
            if !s.seg.header().rel_types.iter().any(|r| r == rel) || s.seg.tt_s_bounds().0 > self.plan.as_of {
                continue;
            }
            let rr = self.rel_rows(si)?;
            if rr.1.iter().any(|c| c == rel) {
                return Ok(None);
            }
            let Some((_, rows)) = rr.0.iter().find(|(n, _)| n == rel) else { continue };
            for &r in rows {
                let row = r as usize;
                let (vs, ve) = (s.vt_s[row], s.vt_e(row));
                if !(vs <= t && t < ve) {
                    continue;
                }
                let (src, dst) = (s.a[row], s.b[row]);
                let mut hit: [Option<(usize, u32, u8)>; 2] = [None, None];
                if matches!(dir, NeighborDir::Out | NeighborDir::Both) {
                    if let Ok(p) = frontier.binary_search(&src) {
                        hit[0] = Some((p, dst, 0));
                    }
                }
                if matches!(dir, NeighborDir::In | NeighborDir::Both) {
                    if let Ok(p) = frontier.binary_search(&dst) {
                        hit[1] = Some((p, src, 1));
                    }
                }
                if hit.iter().all(Option::is_none) {
                    continue;
                }
                let a = addr(si, r);
                if !self.believed(RowKind::Edge, a, BeliefMode::Current)? {
                    continue;
                }
                for (p, nbr, k) in hit.into_iter().flatten() {
                    lists[p].push((vs, (s.vid_hi[row], s.vid_lo[row]), k, nbr, a));
                }
            }
        }
        Ok(Some(NeighborLists::from_vecs(
            lists
                .into_iter()
                .map(|mut l| {
                    l.sort_by_key(|x| (x.0, x.1, x.2));
                    l.into_iter().map(|x| (x.3, x.4)).collect()
                })
                .collect(),
        )))
    }

    /// Shortest distances from each seed, kept iff `lo <= d <= hi`; the seed
    /// itself is at depth 0. Level-synchronous over every seed at once, so a
    /// level costs one adjacency call for the union of the frontiers.
    fn bfs(
        &mut self,
        seeds: &[u32],
        dir: NeighborDir,
        t: i64,
        rels: Option<&[String]>,
        lo: u32,
        hi: Option<u32>,
    ) -> DResult<SeedReach> {
        // One frontier of (seed, node) pairs and one seen-set over the
        // pairs, not a map and a frontier per seed. BI12's REPLY_OF closure
        // seeds every message (1.8M at SNB SF1): the per-seed allocations
        // were ~3 s of a 5.8 s call. Breadth-first by level as before, so a
        // node's depth is the level it is first reached at; the answer per
        // seed comes back as a CSR (`out.1[out.0[i]..out.0[i + 1]]`), in the
        // same (depth, node) order.
        let n = seeds.len();
        let key = |si: u32, v: u32| (u64::from(si) << 32) | u64::from(v);
        let mut seen: std::collections::HashSet<u64, BuildHasherDefault<Fx>> = Default::default();
        seen.reserve(n);
        // (seed index, node, depth) of every node reached
        let mut found: Vec<(u32, u32, u32)> = Vec::with_capacity(n);
        let mut front: Vec<(u32, u32)> = Vec::with_capacity(n);
        for (i, &s0) in seeds.iter().enumerate() {
            let si = i as u32;
            seen.insert(key(si, s0));
            found.push((si, s0, 0));
            front.push((si, s0));
        }
        let mut depth = 0u32;
        let mut total = n;
        loop {
            if hi.is_some_and(|h| depth >= h) || front.is_empty() {
                break;
            }
            depth += 1;
            let mut all: Vec<u32> = front.iter().map(|&(_, u)| u).collect();
            all.sort_unstable();
            all.dedup();
            let adj = self.neighbors(&all, dir, t, rels)?;
            let mut next: Vec<(u32, u32)> = Vec::new();
            for &(si, u) in &front {
                let p = all.binary_search(&u).expect("frontier node listed");
                for &(v, _) in &adj[p] {
                    if seen.insert(key(si, v)) {
                        found.push((si, v, depth));
                        next.push((si, v));
                    }
                }
            }
            total += next.len();
            front = next;
            check_rows(total)?;
        }
        found.retain(|&(_, _, d)| d >= lo);
        found.sort_unstable_by_key(|&(si, v, d)| (si, d, v));
        let mut off = vec![0usize; n + 1];
        for &(si, _, _) in &found {
            off[si as usize + 1] += 1;
        }
        for i in 0..n {
            off[i + 1] += off[i];
        }
        Ok((off, found.into_iter().map(|(_, v, d)| (v, d)).collect()))
    }
}

// --------------------------------------------------------------------------
// keys: joins, groups, distinct sets
// --------------------------------------------------------------------------

/// One value as a hashable, comparable key part. Numbers follow Python's
/// equality (`1 == 1.0`), strings their text.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum KeyVal {
    Null,
    /// an integer, or a float with an integral value in i64 range
    Int(i64),
    /// a non-integral float, by its bits (never NaN: the evaluator refuses NaN)
    Float(u64),
    Bool(bool),
    Text(Arc<str>),
    /// a node identity, a version id, an edge identity
    Uid(u32),
    Vid(u64, u32),
    Eid(u32, u32, u32, Arc<str>),
}

impl<'s, 'a> Ctx<'s, 'a> {
    /// Column `c` as key parts, row by row.
    fn keys_of(&self, c: &Col) -> DResult<Vec<KeyVal>> {
        // text keys clone shared Arcs: on one thread (see `Col::take`)
        if matches!(c.v, Vals::Str(_) | Vals::Label(_) | Vals::RelType(_)) {
            return self.keys_in(c, 0..c.len());
        }
        par_try_concat(c.len(), |r| self.keys_in(c, r))
    }

    fn keys_in(&self, c: &Col, rows: std::ops::Range<usize>) -> DResult<Vec<KeyVal>> {
        let mut out = Vec::with_capacity(rows.len());
        for i in rows {
            if c.is_null(i) {
                out.push(KeyVal::Null);
                continue;
            }
            out.push(match &c.v {
                Vals::Null(_) => KeyVal::Null,
                Vals::I64(x) => KeyVal::Int(x[i]),
                Vals::F64(x) => float_key(x[i]),
                Vals::Bool(x) => KeyVal::Bool(x[i]),
                Vals::Str(x) => KeyVal::Text(x[i].clone()),
                Vals::Uid(x) => KeyVal::Uid(x[i]),
                Vals::Vid(x) => KeyVal::Vid(x[i].0, x[i].1),
                Vals::Eid(x, ..) => {
                    let (s, d, r, disc) = self.eid_key(x[i])?;
                    KeyVal::Eid(s, d, r, Arc::from(disc))
                }
                Vals::Label(x) => KeyVal::Text(self.labels.text(x[i]).clone()),
                Vals::RelType(x) => KeyVal::Text(self.rels.text(x[i]).clone()),
                Vals::Json(..) | Vals::JText(_) => return unsupported("a JSON value as a key"),
            });
        }
        Ok(out)
    }
}

fn float_key(f: f64) -> KeyVal {
    if f.fract() == 0.0 && f.abs() < 9.2e18 {
        KeyVal::Int(f as i64)
    } else {
        KeyVal::Float(f.to_bits())
    }
}

impl<'s, 'a> Ctx<'s, 'a> {
    fn join(&mut self, left: Rel, right: Rel, on: &[(String, String)], kind: JoinKind) -> DResult<Rel> {
        if let (Some(lu), Some(ru)) = (self.id_keys(&left, on, true), self.id_keys(&right, on, false)) {
            return self.join_ids(left, right, &lu, &ru, kind);
        }
        let lk: Vec<Vec<KeyVal>> = on.iter().map(|(l, _)| self.keys_by_name(&left, l)).collect::<DResult<_>>()?;
        let rk: Vec<Vec<KeyVal>> = on.iter().map(|(_, r)| self.keys_by_name(&right, r)).collect::<DResult<_>>()?;
        let key = |ks: &[Vec<KeyVal>], i: usize| -> Vec<KeyVal> { ks.iter().map(|k| k[i].clone()).collect() };
        // a null join key is the evaluator's error, so it is the reference's
        if lk.iter().any(|k| k.contains(&KeyVal::Null)) || rk.iter().any(|k| k.contains(&KeyVal::Null)) {
            return refuse("a null join key");
        }
        let mut table: HashMap<Vec<KeyVal>, Vec<usize>> = HashMap::new();
        for j in 0..right.n {
            table.entry(key(&rk, j)).or_default().push(j);
        }
        let mut li: Vec<usize> = Vec::new();
        let mut ri: Vec<Option<usize>> = Vec::new();
        for i in 0..left.n {
            match (table.get(&key(&lk, i)), kind) {
                (Some(js), JoinKind::Inner | JoinKind::LeftOuter) => {
                    for &j in js {
                        li.push(i);
                        ri.push(Some(j));
                    }
                }
                (None, JoinKind::LeftOuter) => {
                    li.push(i);
                    ri.push(None);
                }
                (None, JoinKind::Anti) => li.push(i),
                _ => {}
            }
            check_rows(li.len())?;
        }
        let mut out = left.take(&li);
        if kind != JoinKind::Anti {
            let r = right.take_opt(&ri);
            out.vars.extend(r.vars);
            out.cols.extend(r.cols);
        }
        Ok(out)
    }

    /// One or two join keys that are node-identity columns with no null, as
    /// one u64 per row (the first key in the high half); None otherwise, and
    /// the general path then decides (a null key is refused there).
    fn id_keys(&mut self, rel: &Rel, on: &[(String, String)], left: bool) -> Option<Vec<u64>> {
        if on.is_empty() || on.len() > 2 {
            return None;
        }
        let mut out: Vec<u64> = Vec::new();
        for (l, r) in on {
            let name = if left { l } else { r };
            let ids = match self.uid_column(rel, name) {
                Ok(ids) if !ids.contains(&NO_ID) => ids,
                Ok(_) => return None,
                Err(_) => {
                    let c = self.eval(&Expr::Col(name.clone()), rel).ok()?;
                    if c.null.as_ref().is_some_and(|m| m.iter().any(|&x| x)) {
                        return None;
                    }
                    match c.v {
                        Vals::Uid(ids) => ids,
                        _ => return None,
                    }
                }
            };
            let prev = std::mem::take(&mut out);
            out = par_concat(ids.len(), |r| {
                r.map(|i| (prev.get(i).copied().unwrap_or(0) << 32) | ids[i] as u64).collect()
            });
        }
        Some(out)
    }

    /// `join` on identity keys: the same rows in the same order as the
    /// general path (left rows in order, each one's matches in right order),
    /// through a CSR index of the smaller side (`KeyCsr`): no allocation per
    /// key or per row. A 1.7M-row join at SF1 spent ~0.8 s building and
    /// probing a HashMap of Vecs, and ~1.3 s with the per-row match Vecs of
    /// the smaller-left path; the copies of the variables, ~10 ms.
    fn join_ids(&mut self, left: Rel, right: Rel, lu: &[u64], ru: &[u64], kind: JoinKind) -> DResult<Rel> {
        let t0 = trace().then(std::time::Instant::now);
        let (li, ri) = match_ids(lu, ru, kind)?;
        check_rows(li.len())?;
        let t_match = t0.map(|t| t.elapsed());
        let mut out = left.take(&li);
        if kind != JoinKind::Anti {
            let r = right.take_opt(&ri);
            out.vars.extend(r.vars);
            out.cols.extend(r.cols);
        }
        if let (Some(t0), Some(t_match)) = (t0, t_match) {
            eprintln!(
                "match trace:   identity join {} x {} -> {} rows: index and match {:.1} ms, gather {:.1} ms",
                lu.len(),
                ru.len(),
                li.len(),
                t_match.as_secs_f64() * 1e3,
                (t0.elapsed() - t_match).as_secs_f64() * 1e3
            );
        }
        Ok(out)
    }

    fn keys_by_name(&mut self, rel: &Rel, name: &str) -> DResult<Vec<KeyVal>> {
        let c = self.eval(&Expr::Col(name.to_string()), rel)?;
        self.keys_of(&c)
    }
}

// --------------------------------------------------------------------------
// aggregation
// --------------------------------------------------------------------------

/// An exact float accumulator: the sum of its inputs, rounded once, which
/// is `math.fsum`'s value whatever the order of the inputs (Shewchuk's
/// partials, as CPython's `fsum` keeps them).
#[derive(Clone, Debug, Default)]
pub(crate) struct ExactSum {
    partials: Vec<f64>,
}

impl ExactSum {
    /// Add every input of `other`: its partials represent its inputs' sum
    /// exactly, so adding them is adding the inputs (the aggregate kernel's
    /// partial states merge this way).
    pub(crate) fn merge(&mut self, other: &ExactSum) {
        for &p in &other.partials {
            self.add(p);
        }
    }

    pub(crate) fn add(&mut self, mut x: f64) {
        let mut i = 0;
        for j in 0..self.partials.len() {
            let mut y = self.partials[j];
            if x.abs() < y.abs() {
                std::mem::swap(&mut x, &mut y);
            }
            let hi = x + y;
            let lo = y - (hi - x);
            if lo != 0.0 {
                self.partials[i] = lo;
                i += 1;
            }
            x = hi;
        }
        self.partials.truncate(i);
        // CPython keeps a zero out of the partials, so a sum of -0.0 is 0.0
        if x != 0.0 {
            self.partials.push(x);
        }
    }

    /// CPython `math.fsum`'s final rounding step, verbatim in structure.
    pub(crate) fn value(&self) -> f64 {
        let p = &self.partials;
        let mut n = p.len();
        if n == 0 {
            return 0.0;
        }
        n -= 1;
        let mut hi = p[n];
        let mut lo = 0.0;
        while n > 0 {
            let x = hi;
            n -= 1;
            let y = p[n];
            hi = x + y;
            let yr = hi - x;
            lo = y - yr;
            if lo != 0.0 {
                break;
            }
        }
        if n > 0 && ((lo < 0.0 && p[n - 1] < 0.0) || (lo > 0.0 && p[n - 1] > 0.0)) {
            let y = lo * 2.0;
            let x = hi + y;
            let yr = x - hi;
            if y == yr {
                hi = x;
            }
        }
        hi
    }
}

/// The blessed integer mean: exact when exact, else one rounding of the
/// exact quotient (`float(q) + r / den` with the division correctly
/// rounded, which `f64` division is for operands below 2^53).
fn blessed_mean(sum: i128, n: i128) -> DResult<f64> {
    let q = sum.div_euclid(n);
    let r = sum.rem_euclid(n);
    if q.unsigned_abs() >= (1u128 << 53) || r.unsigned_abs() >= (1u128 << 53) || n >= (1i128 << 53) {
        return refuse("a mean outside the exactly representable range");
    }
    Ok(q as f64 + (r as f64) / (n as f64))
}

/// A group's distinct values: a short vector until it grows, then a set
/// (most groups of a pattern aggregate hold one or two values).
#[derive(Clone, Debug, Default)]
struct DistinctSet {
    small: Vec<KeyVal>,
    big: Option<std::collections::HashSet<KeyVal>>,
}

impl DistinctSet {
    fn insert(&mut self, v: KeyVal) {
        if let Some(b) = &mut self.big {
            b.insert(v);
        } else if !self.small.contains(&v) {
            self.small.push(v);
            if self.small.len() > 16 {
                self.big = Some(self.small.drain(..).collect());
            }
        }
    }
    fn len(&self) -> usize {
        self.big.as_ref().map_or(self.small.len(), |b| b.len())
    }
}

#[derive(Clone, Debug)]
enum AggState {
    Count(u64),
    Distinct(DistinctSet),
    SumInt(i128, bool),
    SumFloat(ExactSum, bool),
    Min(Option<usize>),
    Max(Option<usize>),
    MeanInt(i128, i128),
    MeanFloat(ExactSum, i128),
}

impl<'s, 'a> Ctx<'s, 'a> {
    fn aggregate(&mut self, input: &Rel, group_by: &[(String, Expr)], aggs: &[Agg]) -> DResult<Rel> {
        let t0 = trace().then(std::time::Instant::now);
        let key_cols: Vec<Col> = group_by.iter().map(|(_, e)| self.eval(e, input)).collect::<DResult<_>>()?;
        // one or two identity key columns without nulls pack each row's key
        // straight from the ids, as `gkey` packs a `KeyVal::Uid`
        let dense = |c: &Col| matches!(c.v, Vals::Uid(_)) && c.null.as_ref().is_none_or(|m| !m.iter().any(|&b| b));
        let packed: Option<Vec<u128>> = if !key_cols.is_empty() && key_cols.len() <= 2 && key_cols.iter().all(dense) {
            let ids: Vec<&[u32]> = key_cols
                .iter()
                .map(|c| match &c.v {
                    Vals::Uid(x) => x.as_slice(),
                    _ => unreachable!("checked above"),
                })
                .collect();
            let part = |u: u32| (1u64 << 62) | u as u64;
            Some(par_concat(input.n, |r| {
                r.map(|i| {
                    let a = part(ids[0][i]) as u128;
                    let b = ids.get(1).map_or(0, |x| part(x[i])) as u128;
                    (a << 64) | b
                })
                .collect()
            }))
        } else {
            None
        };
        let key_parts: Vec<Vec<KeyVal>> = if packed.is_some() {
            Vec::new()
        } else {
            key_cols.iter().map(|c| self.keys_of(c)).collect::<DResult<_>>()?
        };
        let of_cols: Vec<Option<Col>> = aggs
            .iter()
            .map(|a| a.of.as_ref().map(|e| self.eval(e, input)).transpose())
            .collect::<DResult<_>>()?;
        let distinct_keys: Vec<Option<Vec<KeyVal>>> = aggs
            .iter()
            .zip(&of_cols)
            .map(|(a, c)| match (a.func, c) {
                (AggFn::CountDistinct, Some(c)) => self.keys_of(c).map(Some),
                _ => Ok(None),
            })
            .collect::<DResult<_>>()?;
        let job = FoldJob {
            aggs,
            key_parts: &key_parts,
            packed: packed.as_deref(),
            of_cols: &of_cols,
            distinct_keys: &distinct_keys,
        };
        let t_keys = lap(t0);
        // On the pool the rows are partitioned by a hash of their key: each
        // worker folds its partition's groups over their rows in row order
        // (each group's state is the serial fold's), and the groups of all
        // partitions are put in first-seen order. No partial state is ever
        // merged, so the cost does not grow with groups times workers.
        let this: &Self = self;
        let n = input.n;
        let width = if n >= fold_parallel_rows() { crate::scan::scan_threads().clamp(1, 64) } else { 1 };
        let (first, states) = if width <= 1 {
            let f = this.fold_all(&job, n)?;
            (f.first, f.states)
        } else {
            // each chunk of rows split by partition, (row, key) in row order
            let chunk = n.div_ceil(width);
            let split: Vec<Vec<Vec<(usize, GKey)>>> = crate::pool::run(width, |c| {
                let mut by: Vec<Vec<(usize, GKey)>> = (0..width).map(|_| Vec::new()).collect();
                for i in (c * chunk).min(n)..((c + 1) * chunk).min(n) {
                    let k = Self::gkey(&job, i);
                    by[key_part(&k, width) as usize].push((i, k));
                }
                by
            });
            // a partition's rows: its part of each chunk, chunks in order
            let parts: Vec<DResult<Fold>> = crate::pool::run(width, |w| {
                this.fold(&job, split.iter().flat_map(|by| by[w].iter().cloned()))
            });
            let mut groups: Vec<(usize, Vec<AggState>)> = Vec::new();
            for p in parts {
                let p = p?;
                groups.extend(p.first.into_iter().zip(p.states));
            }
            groups.sort_unstable_by_key(|g| g.0);
            groups.into_iter().unzip()
        };
        if trace() {
            eprintln!(
                "match trace:   aggregate: keys {:.1} ms, fold {:.1} ms ({} rows, {} groups, {} workers)",
                t_keys.as_secs_f64() * 1e3,
                (lap(t0) - t_keys).as_secs_f64() * 1e3,
                n,
                first.len(),
                width
            );
        }
        let ng = first.len();
        let groups_of_rows = if aggs.iter().any(|a| a.func == AggFn::Collect) {
            let mut index: FxMap<GKey, usize> = FxMap::default();
            for (g, &i) in first.iter().enumerate() {
                index.insert(Self::gkey(&job, i), g);
            }
            let mut members = vec![Vec::new(); ng];
            for i in 0..n {
                members[index[&Self::gkey(&job, i)]].push(i);
            }
            members
        } else {
            Vec::new()
        };
        let mut out = Rel { n: ng, vars: Vec::new(), cols: Vec::new() };
        for ((name, _), c) in group_by.iter().zip(&key_cols) {
            out.cols.push((name.clone(), c.take(&first)));
        }
        for (j, a) in aggs.iter().enumerate() {
            let col = match aggs[j].func {
                AggFn::Count => Col { v: Vals::I64(states.iter().map(|s| match &s[j] { AggState::Count(n) => *n as i64, _ => 0 }).collect()), null: None },
                AggFn::CountDistinct => Col { v: Vals::I64(states.iter().map(|s| match &s[j] { AggState::Distinct(d) => d.len() as i64, _ => 0 }).collect()), null: None },
                AggFn::SumExact => {
                    let float = matches!(of_cols[j].as_ref().map(|c| &c.v), Some(Vals::F64(_)));
                    let mut null = vec![false; ng];
                    if float {
                        let mut v = Vec::with_capacity(ng);
                        for (g, s) in states.iter().enumerate() {
                            if let AggState::SumFloat(x, any) = &s[j] {
                                null[g] = !any;
                                v.push(x.value());
                            }
                        }
                        Col { v: Vals::F64(v), null: Some(null) }
                    } else {
                        let mut v = Vec::with_capacity(ng);
                        for (g, s) in states.iter().enumerate() {
                            if let AggState::SumInt(x, any) = &s[j] {
                                null[g] = !any;
                                v.push(i64::try_from(*x).map_or_else(|_| refuse("an integer sum outside int64"), Ok)?);
                            }
                        }
                        Col { v: Vals::I64(v), null: Some(null) }
                    }
                }
                AggFn::Mean => {
                    let mut v = Vec::with_capacity(ng);
                    let mut null = vec![false; ng];
                    for (g, s) in states.iter().enumerate() {
                        match &s[j] {
                            AggState::MeanInt(sum, n) => {
                                null[g] = *n == 0;
                                v.push(if *n == 0 { 0.0 } else { blessed_mean(*sum, *n)? });
                            }
                            AggState::MeanFloat(sum, n) => {
                                null[g] = *n == 0;
                                v.push(if *n == 0 { 0.0 } else { sum.value() / *n as f64 });
                            }
                            _ => v.push(0.0),
                        }
                    }
                    Col { v: Vals::F64(v), null: Some(null) }
                }
                AggFn::Min | AggFn::Max => {
                    let src = of_cols[j].as_ref().expect("of");
                    let idx: Vec<Option<usize>> = states
                        .iter()
                        .map(|s| match &s[j] {
                            AggState::Min(b) | AggState::Max(b) => *b,
                            _ => None,
                        })
                        .collect();
                    src.take_opt(&idx)
                }
                AggFn::Collect => self.collect(input, a, &groups_of_rows)?,
            };
            out.cols.push((a.alias.clone(), col));
        }
        Ok(out)
    }
}

impl<'s, 'a> Ctx<'s, 'a> {
    /// `collect` per group (rows in input order): the elements of the rows
    /// whose components are not all null, ordered by the aggregate's keys and
    /// then by the components (ascending, nulls first), equal elements kept
    /// once when `distinct`, cut at `limit`; each group's list as JSON text.
    fn collect(&mut self, input: &Rel, a: &Agg, groups: &[Vec<usize>]) -> DResult<Col> {
        let items: Vec<Col> = a.items.iter().map(|e| self.eval(e, input)).collect::<DResult<_>>()?;
        let keys: Vec<Col> = a.order.iter().map(|k| self.eval(&k.key, input)).collect::<DResult<_>>()?;
        let item_keys: Vec<Vec<KeyVal>> = items.iter().map(|c| self.keys_of(c)).collect::<DResult<_>>()?;
        let mut out = Vec::with_capacity(groups.len());
        for rows in groups {
            let mut live: Vec<usize> = rows.iter().copied().filter(|&r| items.iter().any(|c| !c.is_null(r))).collect();
            let mut err = None;
            let this: &Self = self;
            let one = |c: &Col, x: usize, y: usize, desc: bool, nulls_first: bool| -> DResult<Ordering> {
                Ok(match (c.is_null(x), c.is_null(y)) {
                    (true, true) => Ordering::Equal,
                    (true, false) => if nulls_first { Ordering::Less } else { Ordering::Greater },
                    (false, true) => if nulls_first { Ordering::Greater } else { Ordering::Less },
                    (false, false) => {
                        let o = this.cmp_values(c, x, c, y)?;
                        if desc { o.reverse() } else { o }
                    }
                })
            };
            live.sort_by(|&x, &y| {
                for (k, c) in a.order.iter().zip(&keys) {
                    match one(c, x, y, k.desc, k.nulls_first) {
                        Ok(Ordering::Equal) => {}
                        Ok(o) => return o,
                        Err(e) => {
                            err.get_or_insert(e);
                            return Ordering::Equal;
                        }
                    }
                }
                for c in &items {
                    match one(c, x, y, false, true) {
                        Ok(Ordering::Equal) => {}
                        Ok(o) => return o,
                        Err(e) => {
                            err.get_or_insert(e);
                            return Ordering::Equal;
                        }
                    }
                }
                x.cmp(&y)
            });
            if let Some(e) = err {
                return Err(e);
            }
            let mut seen: Vec<Vec<KeyVal>> = Vec::new();
            let mut elems: Vec<String> = Vec::new();
            for r in live {
                if a.distinct {
                    let k: Vec<KeyVal> = item_keys.iter().map(|ks| ks[r].clone()).collect();
                    if seen.contains(&k) {
                        continue;
                    }
                    seen.push(k);
                }
                let mut parts = Vec::with_capacity(items.len());
                for c in &items {
                    parts.push(json_text(&self.value(c, r)?)?);
                }
                elems.push(if parts.len() == 1 { parts.pop().unwrap_or_default() } else { format!("[{}]", parts.join(",")) });
                if a.limit.is_some_and(|l| elems.len() >= l) {
                    break;
                }
            }
            out.push(Arc::<str>::from(format!("[{}]", elems.join(",")).as_str()));
        }
        Ok(Col { v: Vals::JText(out), null: None })
    }
}

/// One output value as JSON text (the operator parses it back).
fn json_text(v: &MVal) -> DResult<String> {
    Ok(match v {
        MVal::Null => "null".to_string(),
        MVal::Int(x) => x.to_string(),
        MVal::Float(x) => serde_json::to_string(x).map_err(|e| Decline::Refuse(e.to_string()))?,
        MVal::Bool(x) => x.to_string(),
        MVal::Str(x) => serde_json::to_string(x).map_err(|e| Decline::Refuse(e.to_string()))?,
        MVal::Json(x) => x.clone(),
    })
}

/// Rows from which an aggregate folds on the pool rather than serially:
/// 2^17 by default; `GNOMON_MATCH_FOLD_ROWS` (read once per process) moves it,
/// which is how the tests hold the parallel fold to the serial one.
fn fold_parallel_rows() -> usize {
    static ROWS: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *ROWS.get_or_init(|| {
        std::env::var("GNOMON_MATCH_FOLD_ROWS")
            .ok()
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or(1 << 17)
    })
}

/// A group key: the first part inline (no allocation for one key), the rest
/// in a vector.
/// A group key: up to two identities or integers packed in a `u128`
/// (no allocation, one word to hash), anything else in general form.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum GKey {
    Packed(u128),
    General(KeyVal, Vec<KeyVal>),
}

/// A key part as 63 bits plus a tag bit, when it is a dense id or a small
/// integer; `None` otherwise.
fn pack_part(k: &KeyVal) -> Option<u64> {
    match k {
        KeyVal::Null => Some(0),
        KeyVal::Uid(u) => Some((1u64 << 62) | *u as u64),
        KeyVal::Int(i) if (0..(1i64 << 61)).contains(i) => Some((2u64 << 62) | *i as u64),
        _ => None,
    }
}

struct FoldJob<'j> {
    aggs: &'j [Agg],
    key_parts: &'j [Vec<KeyVal>],
    /// one or two dense-id key columns without nulls: each row's packed key
    /// (what `gkey` would pack from `key_parts`), built without them
    packed: Option<&'j [u128]>,
    of_cols: &'j [Option<Col>],
    distinct_keys: &'j [Option<Vec<KeyVal>>],
}

/// A group key's partition among `width` workers.
fn key_part(k: &GKey, width: usize) -> u16 {
    use std::hash::{Hash, Hasher};
    let h = match k {
        GKey::Packed(u) => ((*u as u64) ^ ((*u >> 64) as u64).rotate_left(29)).wrapping_mul(0x9E37_79B9_7F4A_7C15),
        GKey::General(..) => {
            let mut h = Fx::default();
            k.hash(&mut h);
            h.finish().wrapping_mul(0x9E37_79B9_7F4A_7C15)
        }
    };
    ((h >> 32) % width as u64) as u16
}

/// One worker's partial fold: its groups in first-seen order.
#[derive(Default)]
struct Fold {
    index: FxMap<GKey, usize>,
    first: Vec<usize>,
    states: Vec<Vec<AggState>>,
}

impl<'s, 'a> Ctx<'s, 'a> {
    fn gkey(job: &FoldJob<'_>, i: usize) -> GKey {
        if let Some(p) = job.packed {
            return GKey::Packed(p[i]);
        }
        let parts = job.key_parts;
        if parts.len() <= 2 {
            let a = parts.first().map_or(Some(0), |p| pack_part(&p[i]));
            let b = parts.get(1).map_or(Some(0), |p| pack_part(&p[i]));
            if let (Some(a), Some(b)) = (a, b) {
                return GKey::Packed(((a as u128) << 64) | b as u128);
            }
        }
        let mut it = parts.iter();
        let head = it.next().map_or(KeyVal::Null, |p| p[i].clone());
        GKey::General(head, it.map(|p| p[i].clone()).collect())
    }

    fn fresh_states(job: &FoldJob<'_>) -> Vec<AggState> {
        job.aggs
            .iter()
            .zip(job.of_cols)
            .map(|(a, c)| match a.func {
                AggFn::Count => AggState::Count(0),
                AggFn::CountDistinct => AggState::Distinct(Default::default()),
                AggFn::SumExact => match c.as_ref().map(|c| &c.v) {
                    Some(Vals::F64(_)) => AggState::SumFloat(ExactSum::default(), false),
                    _ => AggState::SumInt(0, false),
                },
                AggFn::Min => AggState::Min(None),
                AggFn::Max => AggState::Max(None),
                // folded apart, after the groups are known
                AggFn::Collect => AggState::Count(0),
                AggFn::Mean => match c.as_ref().map(|c| &c.v) {
                    Some(Vals::F64(_)) => AggState::MeanFloat(ExactSum::default(), 0),
                    _ => AggState::MeanInt(0, 0),
                },
            })
            .collect()
    }

    /// The groups of `rows` (row, key), in first-seen order, each state
    /// folded over its rows in the order given.
    fn fold(&self, job: &FoldJob<'_>, rows: impl Iterator<Item = (usize, GKey)>) -> DResult<Fold> {
        let mut f = Fold::default();
        for (i, k) in rows {
            self.fold_row(job, &mut f, i, k)?;
        }
        Ok(f)
    }

    /// `fold` over the rows `0..n` with their own keys (the serial path).
    fn fold_all(&self, job: &FoldJob<'_>, n: usize) -> DResult<Fold> {
        let mut f = Fold::default();
        for i in 0..n {
            self.fold_row(job, &mut f, i, Self::gkey(job, i))?;
        }
        Ok(f)
    }

    /// Row `i`, of key `k`, into its group's states.
    #[inline]
    fn fold_row(&self, job: &FoldJob<'_>, f: &mut Fold, i: usize, k: GKey) -> DResult<()> {
        let g = match f.index.get(&k) {
            Some(&g) => g,
            None => {
                let g = f.first.len();
                f.index.insert(k, g);
                f.first.push(i);
                f.states.push(Self::fresh_states(job));
                g
            }
        };
        for (j, st) in f.states[g].iter_mut().enumerate() {
            let c = job.of_cols[j].as_ref();
            if c.is_some_and(|c| c.is_null(i)) {
                continue; // a null value folds into nothing
            }
            match st {
                AggState::Count(n) => *n += 1,
                AggState::Distinct(set) => {
                    set.insert(job.distinct_keys[j].as_ref().expect("keys")[i].clone());
                }
                AggState::SumInt(s, any) => match &c.expect("of").v {
                    Vals::I64(x) => {
                        *s += x[i] as i128;
                        *any = true;
                    }
                    Vals::Null(_) => {}
                    _ => return unsupported("sum over a non-numeric column"),
                },
                AggState::MeanInt(s, k) => match &c.expect("of").v {
                    Vals::I64(x) => {
                        *s += x[i] as i128;
                        *k += 1;
                    }
                    Vals::Null(_) => {}
                    _ => return unsupported("mean over a non-numeric column"),
                },
                AggState::SumFloat(s, any) => {
                    if let Vals::F64(x) = &c.expect("of").v {
                        s.add(x[i]);
                        *any = true;
                    }
                }
                AggState::MeanFloat(s, k) => {
                    if let Vals::F64(x) = &c.expect("of").v {
                        s.add(x[i]);
                        *k += 1;
                    }
                }
                AggState::Min(best) | AggState::Max(best) => {
                    let is_min = job.aggs[j].func == AggFn::Min;
                    let col = c.expect("of");
                    let take = match best {
                        None => true,
                        Some(b) => {
                            let o = self.cmp_values(col, i, col, *b)?;
                            if is_min {
                                o == Ordering::Less
                            } else {
                                o == Ordering::Greater
                            }
                        }
                    };
                    if take {
                        *best = Some(i);
                    }
                }
            }
        }
        Ok(())
    }
}

// --------------------------------------------------------------------------
// order
// --------------------------------------------------------------------------

impl<'s, 'a> Ctx<'s, 'a> {
    /// The input positions in the order the keys give (stable: ties keep
    /// input order). With `prefix = Some(k)`, only the first `k` positions are
    /// guaranteed sorted (a top-k: select then sort the k).
    fn order(&mut self, input: &Rel, keys: &[SortKey], prefix: Option<usize>) -> DResult<Vec<usize>> {
        let cols: Vec<Col> = keys.iter().map(|k| self.eval(&k.key, input)).collect::<DResult<_>>()?;
        if prefix.is_none() && input.n >= 2 {
            if let Some(idx) = self.order_by_ranks(&cols, keys, input.n) {
                return Ok(idx);
            }
        }
        let mut err: Option<Decline> = None;
        let this: &Self = self;
        let cmp = |a: &usize, b: &usize| -> Ordering {
            for (k, c) in keys.iter().zip(&cols) {
                let (na, nb) = (c.is_null(*a), c.is_null(*b));
                let o = match (na, nb) {
                    (true, true) => Ordering::Equal,
                    (true, false) => {
                        if k.nulls_first {
                            Ordering::Less
                        } else {
                            Ordering::Greater
                        }
                    }
                    (false, true) => {
                        if k.nulls_first {
                            Ordering::Greater
                        } else {
                            Ordering::Less
                        }
                    }
                    (false, false) => match this.cmp_values(c, *a, c, *b) {
                        Ok(o) => {
                            if k.desc {
                                o.reverse()
                            } else {
                                o
                            }
                        }
                        Err(_) => Ordering::Equal,
                    },
                };
                if o != Ordering::Equal {
                    return o;
                }
            }
            a.cmp(b)
        };
        // surface a comparison failure (types the evaluator would refuse)
        if input.n > 1 {
            for (_k, c) in keys.iter().zip(&cols) {
                if let Err(e) = this.cmp_values(c, 0, c, 0) {
                    if !c.is_null(0) {
                        err = Some(e);
                    }
                }
            }
        }
        if let Some(e) = err {
            return Err(e);
        }
        let mut idx: Vec<usize> = (0..input.n).collect();
        match prefix {
            Some(k) if k < idx.len() && k > 0 => {
                idx.select_nth_unstable_by(k - 1, cmp);
                idx.truncate(k);
                idx.sort_by(cmp);
            }
            Some(0) => idx.clear(),
            _ => par_sort_by(&mut idx, &cmp),
        }
        Ok(idx)
    }

    /// `order` for keys that are all integer or identity columns without a
    /// null: each identity column is read as its uid's rank in text order
    /// (computed once per distinct id), so a comparison is a few integer
    /// compares instead of the uid texts'. The same total order (ties by
    /// input position), so the same permutation. None for any other key, or
    /// an id outside the dictionary (the general path decides then).
    fn order_by_ranks(&self, cols: &[Col], keys: &[SortKey], n: usize) -> Option<Vec<usize>> {
        enum K<'c> {
            Int(&'c [i64]),
            Rank(Vec<u32>),
        }
        let mut ks: Vec<(K<'_>, bool)> = Vec::with_capacity(cols.len());
        for (c, k) in cols.iter().zip(keys) {
            if c.null.as_ref().is_some_and(|m| m.iter().any(|&b| b)) {
                return None;
            }
            match &c.v {
                Vals::I64(x) => ks.push((K::Int(x), k.desc)),
                Vals::Uid(x) => {
                    let distinct = sorted_distinct(x.clone());
                    let mut texts: Vec<(&str, u32)> = Vec::with_capacity(distinct.len());
                    for &id in &distinct {
                        texts.push((self.uid_text(id).ok()?, id));
                    }
                    texts.sort_unstable();
                    // ids with one text share a rank (a uid has one id, so none do)
                    let mut rank_of: FxMap<u32, u32> = FxMap::default();
                    let mut r = 0u32;
                    for (j, &(t, id)) in texts.iter().enumerate() {
                        if j > 0 && texts[j - 1].0 != t {
                            r += 1;
                        }
                        rank_of.insert(id, r);
                    }
                    ks.push((K::Rank(par_concat(n, |rg| x[rg].iter().map(|id| rank_of[id]).collect())), k.desc));
                }
                _ => return None,
            }
        }
        let cmp = |a: &usize, b: &usize| -> Ordering {
            for (k, desc) in &ks {
                let o = match k {
                    K::Int(x) => x[*a].cmp(&x[*b]),
                    K::Rank(x) => x[*a].cmp(&x[*b]),
                };
                let o = if *desc { o.reverse() } else { o };
                if o != Ordering::Equal {
                    return o;
                }
            }
            a.cmp(b)
        };
        let mut idx: Vec<usize> = (0..n).collect();
        par_sort_by(&mut idx, &cmp);
        Some(idx)
    }

    /// The input of the answer's `Order`, and its row count. When it is a
    /// `Project` (keep all) of infallible bindings read once, and the page
    /// needs fewer rows than it has, the projection's input is cut first to
    /// the rows that can reach the page under the first key alone (every row
    /// at least as good as the page's last on it, ties included), and the
    /// bindings are computed for those rows only. The full order then runs
    /// on them: the same rows in the same order, without computing every
    /// output column of every row.
    fn order_input(
        &mut self,
        input_id: usize,
        keys: &[SortKey],
        cap: Option<usize>,
        page_end: usize,
        memo: &mut FxMap<usize, Rel>,
    ) -> DResult<(Rel, usize)> {
        let node = &self.plan.nodes[input_id];
        let fast = match (&node.op, keys.first()) {
            (Op::Project { bindings, keep_all: true }, Some(k1)) if self.uses(input_id) == 1 => {
                if bindings.iter().any(|(_, e)| fallible(e)) {
                    None
                } else {
                    // the first key over the projection's input
                    let e1 = match &k1.key {
                        Expr::Col(n) => bindings.iter().find(|(b, _)| b == n).map(|(_, e)| e.clone()),
                        _ => None,
                    };
                    let reads: Vec<String> = e1.as_ref().map_or_else(Vec::new, |e| {
                        let mut v = Vec::new();
                        expr_names(e, &mut v);
                        v
                    });
                    let names_binding = reads.iter().any(|r| bindings.iter().any(|(b, _)| b == r));
                    e1.filter(|_| !names_binding).map(|e| (bindings.clone(), SortKey { key: e, ..k1.clone() }))
                }
            }
            _ => None,
        };
        let Some((bindings, k1)) = fast else {
            let input = self.run(input_id, memo)?;
            let n = input.n;
            return Ok((input, n));
        };
        let inner = self.run(node.inputs[0], memo)?;
        let n = inner.n;
        let want = page_end.min(cap.map_or(n, |c| c.min(n)));
        if want == 0 || want * 4 >= n {
            let out = self.apply(&Op::Project { bindings, keep_all: true }, vec![inner])?;
            return Ok((out, n));
        }
        let top = self.order(&inner, std::slice::from_ref(&k1), Some(want))?;
        let last = *top.last().expect("want > 0 rows");
        let col = self.eval(&k1.key, &inner)?;
        let mut keep = Vec::with_capacity(want * 2);
        for i in 0..n {
            if self.key_order(&col, &k1, i, last)? != Ordering::Greater {
                keep.push(i);
            }
        }
        let cut = inner.take(&keep);
        let out = self.apply(&Op::Project { bindings, keep_all: true }, vec![cut])?;
        Ok((out, n))
    }

    /// Row `a` against row `b` of one key column, as `order` compares them
    /// (nulls first or last, descending reversed).
    fn key_order(&self, c: &Col, k: &SortKey, a: usize, b: usize) -> DResult<Ordering> {
        Ok(match (c.is_null(a), c.is_null(b)) {
            (true, true) => Ordering::Equal,
            (true, false) => {
                if k.nulls_first {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            }
            (false, true) => {
                if k.nulls_first {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }
            (false, false) => {
                let o = self.cmp_values(c, a, c, b)?;
                if k.desc {
                    o.reverse()
                } else {
                    o
                }
            }
        })
    }

    /// Compare two non-null values of two columns of one type (numbers
    /// across int and float as numpy does: the int widened to float).
    fn cmp_values(&self, c1: &Col, i: usize, c2: &Col, j: usize) -> DResult<Ordering> {
        use Vals::*;
        Ok(match (&c1.v, &c2.v) {
            (I64(a), I64(b)) => a[i].cmp(&b[j]),
            (F64(a), F64(b)) => a[i].partial_cmp(&b[j]).unwrap_or(Ordering::Equal),
            (I64(a), F64(b)) => (a[i] as f64).partial_cmp(&b[j]).unwrap_or(Ordering::Equal),
            (F64(a), I64(b)) => a[i].partial_cmp(&(b[j] as f64)).unwrap_or(Ordering::Equal),
            (Bool(a), Bool(b)) => a[i].cmp(&b[j]),
            (Bool(a), I64(b)) => (a[i] as i64).cmp(&b[j]),
            (I64(a), Bool(b)) => a[i].cmp(&(b[j] as i64)),
            (Vid(a), Vid(b)) => a[i].cmp(&b[j]),
            (Uid(a), Uid(b)) => {
                if a[i] == b[j] {
                    Ordering::Equal
                } else {
                    self.uid_text(a[i])?.cmp(self.uid_text(b[j])?)
                }
            }
            (Null(_), _) | (_, Null(_)) => Ordering::Equal,
            (Eid(a), Eid(b)) => self.eid_hex(a[i])?.cmp(&self.eid_hex(b[j])?),
            _ => {
                let (Some(x), Some(y)) = (self.text_at(c1, i)?, self.text_at(c2, j)?) else {
                    return refuse("a comparison across type classes");
                };
                x.as_ref().cmp(y.as_ref())
            }
        })
    }

    fn uid_text(&self, id: u32) -> DResult<&str> {
        self.store.dict().uid(id).map_or_else(|| refuse("a dense id outside the dictionary"), Ok)
    }

    /// The text of a string-like value, or `None` for a non-string column.
    fn text_at(&self, c: &Col, i: usize) -> DResult<Option<Arc<str>>> {
        Ok(match &c.v {
            Vals::Str(x) => Some(x[i].clone()),
            Vals::Uid(x) => Some(Arc::from(self.uid_text(x[i])?)),
            Vals::Label(x) => Some(self.labels.text(x[i]).clone()),
            Vals::RelType(x) => Some(self.rels.text(x[i]).clone()),
            _ => None,
        })
    }
}

// --------------------------------------------------------------------------
// expressions
// --------------------------------------------------------------------------

/// A numeric operand, as the reference's `_numeric` sees it.
enum Num {
    I(Vec<i64>),
    F(Vec<f64>),
}

impl Num {
    fn is_zero(&self, i: usize) -> bool {
        match self {
            Num::I(v) => v[i] == 0,
            Num::F(v) => v[i] == 0.0,
        }
    }
    fn floats(&self) -> Vec<f64> {
        match self {
            Num::I(v) => v.iter().map(|&x| x as f64).collect(),
            Num::F(v) => v.clone(),
        }
    }
}

fn or_nulls(a: &Col, b: &Col, n: usize) -> Option<Vec<bool>> {
    if a.null.is_none() && b.null.is_none() && !matches!(a.v, Vals::Null(_)) && !matches!(b.v, Vals::Null(_)) {
        return None;
    }
    Some((0..n).map(|i| a.is_null(i) || b.is_null(i)).collect())
}

impl<'s, 'a> Ctx<'s, 'a> {
    fn predicate(&mut self, e: &Expr, rel: &Rel) -> DResult<Vec<bool>> {
        let c = self.eval(e, rel)?;
        match &c.v {
            Vals::Bool(b) => Ok((0..rel.n).map(|i| b[i] && !c.is_null(i)).collect()),
            Vals::Null(n) => Ok(vec![false; *n]),
            _ => unsupported("a predicate of a non-boolean type"),
        }
    }

    /// A variable field (`v.uid`, `e.vt_s`, ...) as a column.
    fn field(&mut self, rel: &Rel, var: &str, field: &str) -> DResult<Option<Col>> {
        let Some(v) = rel.var(var) else { return Ok(None) };
        let n = rel.n;
        let (kind, ver): (RowKind, &[Addr]) = match v {
            Var::Node { ids, ver, depth } => {
                match field {
                    "uid" => {
                        let null: Vec<bool> = ids.iter().map(|&i| i == NO_ID).collect();
                        return Ok(Some(Col { v: Vals::Uid(ids.clone()), null: null.iter().any(|&b| b).then_some(null) }));
                    }
                    "depth" => {
                        let Some(d) = depth else { return unsupported("depth of a fixed-length target") };
                        let null: Vec<bool> = ids.iter().map(|&i| i == NO_ID).collect();
                        return Ok(Some(Col {
                            v: Vals::I64(d.iter().map(|&x| x as i64).collect()),
                            null: null.iter().any(|&b| b).then_some(null),
                        }));
                    }
                    _ => {}
                }
                (RowKind::Node, ver.as_slice())
            }
            Var::Edge { ver } => (RowKind::Edge, ver.as_slice()),
        };
        let null: Vec<bool> = ver.iter().map(|&a| a == NO_ADDR).collect();
        let any_null = null.iter().any(|&b| b);
        let null_opt = any_null.then_some(null);
        let live = |i: usize| ver[i] != NO_ADDR;
        // the per-row reads in chunks on the pool, in row order
        let this: &Self = self;
        let vals = match (kind, field) {
            (_, "vt_s") => Vals::I64(par_concat(n, |r| {
                r.map(|i| if live(i) { this.seg(kind, ver[i]).vt_s[row_of(ver[i])] } else { 0 }).collect()
            })),
            (_, "vt_e") => Vals::I64(par_concat(n, |r| {
                r.map(|i| if live(i) { this.seg(kind, ver[i]).vt_e(row_of(ver[i])) } else { 0 }).collect()
            })),
            (_, "tt_s") => Vals::I64(par_try_concat(n, |r| {
                let mut v = Vec::with_capacity(r.len());
                for i in r {
                    v.push(if live(i) { this.tt_s(kind, ver[i])? } else { 0 });
                }
                Ok(v)
            })?),
            (_, "tt_e") => Vals::I64(par_concat(n, |r| {
                r.map(|i| if live(i) { this.tt_e(kind, ver[i]) } else { 0 }).collect()
            })),
            (_, "vid") => Vals::Vid(par_concat(n, |r| {
                r.map(|i| if live(i) { this.vid(kind, ver[i]) } else { (0, 0) }).collect()
            })),
            (_, "props") => Vals::Json(ver.to_vec(), kind, None),
            (RowKind::Node, "label") => {
                let mut v = Vec::with_capacity(n);
                for i in 0..n {
                    v.push(if live(i) { self.label_code(ver[i])? } else { 0 });
                }
                Vals::Label(v)
            }
            (RowKind::Edge, "src") | (RowKind::Edge, "dst") => {
                let src = field == "src";
                Vals::Uid(par_concat(n, |r| {
                    r.map(|i| if live(i) {
                        let s = &this.edges[seg_of(ver[i])];
                        if src { s.a[row_of(ver[i])] } else { s.b[row_of(ver[i])] }
                    } else { NO_ID }).collect()
                }))
            }
            (RowKind::Edge, "rel_type") => Vals::RelType(par_concat(n, |r| {
                r.map(|i| if live(i) { this.rel_code(ver[i]) } else { 0 }).collect()
            })),
            (RowKind::Edge, "disc") => Vals::Str(par_try_concat(n, |r| {
                let mut v = Vec::with_capacity(r.len());
                for i in r {
                    v.push(if live(i) { Arc::from(this.disc(ver[i])?) } else { Arc::from("") });
                }
                Ok(v)
            })?),
            (RowKind::Edge, "eid") => Vals::Eid(ver.to_vec()),
            _ => return unsupported(format!("variable field {var}.{field}")),
        };
        Ok(Some(Col { v: vals, null: null_opt }))
    }

    fn eval(&mut self, e: &Expr, rel: &Rel) -> DResult<Col> {
        let n = rel.n;
        match e {
            Expr::Lit(l) => Ok(match l {
                Lit::Null => Col { v: Vals::Null(n), null: None },
                Lit::Int(x) => Col { v: Vals::I64(vec![*x; n]), null: None },
                Lit::Float(x) => Col { v: Vals::F64(vec![*x; n]), null: None },
                Lit::Bool(x) => Col { v: Vals::Bool(vec![*x; n]), null: None },
                Lit::Str(s) => Col { v: Vals::Str(vec![s.clone(); n]), null: None },
            }),
            Expr::Col(name) => {
                if let Some(c) = rel.col(name) {
                    return Ok(c.clone());
                }
                if let Some((var, field)) = name.split_once('.') {
                    if let Some(c) = self.field(rel, var, field)? {
                        return Ok(c);
                    }
                }
                unsupported(format!("unknown column {name}"))
            }
            Expr::TProp { col, key, tau } => self.typed_prop(rel, col, key, *tau),
            Expr::Prop { col, key } => {
                let Some(var) = col.strip_suffix(".props") else { return unsupported("prop of a non-props column") };
                let (kind, ver) = match rel.var(var) {
                    Some(Var::Node { ver, .. }) => (RowKind::Node, ver.clone()),
                    Some(Var::Edge { ver }) => (RowKind::Edge, ver.clone()),
                    None => return unsupported("prop of an unbound variable"),
                };
                let key: Arc<str> = Arc::from(key.as_str());
                // null where the version is missing or the key is absent
                let mut null = vec![false; n];
                for i in 0..n {
                    null[i] = ver[i] == NO_ADDR || !self.prop_raw_present(kind, ver[i], &key)?;
                }
                Ok(Col { v: Vals::Json(ver, kind, Some(key)), null: Some(null) })
            }
            Expr::Cmp(op, l, r) => {
                if let Some(c) = self.cmp_literal(*op, l, r, rel)? {
                    return Ok(c);
                }
                let a = self.eval(l, rel)?;
                let b = self.eval(r, rel)?;
                let null = or_nulls(&a, &b, n);
                if let (Vals::Eid(x), Vals::Eid(y), CmpOp::Eq | CmpOp::Ne) = (&a.v, &b.v, op) {
                    // edge identity: one address is one identity, and two
                    // addresses carry one identity iff their keys agree; no
                    // eid hash is computed for an (in)equality
                    let eq = *op == CmpOp::Eq;
                    let this: &Self = self;
                    let out = par_try_concat(n, |r| {
                        let mut out = Vec::with_capacity(r.len());
                        for i in r {
                            if a.is_null(i) || b.is_null(i) {
                                out.push(false);
                                continue;
                            }
                            let same = x[i] == y[i] || this.same_eid(x[i], y[i])?;
                            out.push(same == eq);
                        }
                        Ok(out)
                    })?;
                    return Ok(Col { v: Vals::Bool(out), null });
                }
                if let (Vals::I64(x), Vals::I64(y)) = (&a.v, &b.v) {
                    // two integer columns: compared directly
                    let op = *op;
                    let out = par_concat(n, |rg| {
                        rg.map(|i| {
                            if a.is_null(i) || b.is_null(i) {
                                return false;
                            }
                            let o = x[i].cmp(&y[i]);
                            match op {
                                CmpOp::Eq => o == Ordering::Equal,
                                CmpOp::Ne => o != Ordering::Equal,
                                CmpOp::Lt => o == Ordering::Less,
                                CmpOp::Le => o != Ordering::Greater,
                                CmpOp::Gt => o == Ordering::Greater,
                                CmpOp::Ge => o != Ordering::Less,
                            }
                        })
                        .collect()
                    });
                    return Ok(Col { v: Vals::Bool(out), null });
                }
                if let (Vals::Uid(x), Vals::Uid(y), CmpOp::Eq | CmpOp::Ne) = (&a.v, &b.v, op) {
                    // dense ids are one per uid: (in)equality needs no text
                    let eq = *op == CmpOp::Eq;
                    let out = (0..n).map(|i| (x[i] == y[i]) == eq && !(a.is_null(i) || b.is_null(i))).collect();
                    return Ok(Col { v: Vals::Bool(out), null });
                }
                let mut out = vec![false; n];
                for (i, slot) in out.iter_mut().enumerate() {
                    if a.is_null(i) || b.is_null(i) {
                        continue;
                    }
                    let o = self.cmp_values(&a, i, &b, i)?;
                    *slot = match op {
                        CmpOp::Eq => o == Ordering::Equal,
                        CmpOp::Ne => o != Ordering::Equal,
                        CmpOp::Lt => o == Ordering::Less,
                        CmpOp::Le => o != Ordering::Greater,
                        CmpOp::Gt => o == Ordering::Greater,
                        CmpOp::Ge => o != Ordering::Less,
                    };
                }
                Ok(Col { v: Vals::Bool(out), null })
            }
            Expr::And(l, r) | Expr::Or(l, r) => {
                let and = matches!(e, Expr::And(..));
                let a = self.eval(l, rel)?;
                let b = self.eval(r, rel)?;
                let (av, bv) = (self.bools(&a)?, self.bools(&b)?);
                let null = or_nulls(&a, &b, n);
                let out = (0..n).map(|i| if and { av[i] && bv[i] } else { av[i] || bv[i] }).collect();
                Ok(Col { v: Vals::Bool(out), null })
            }
            Expr::Not(x) => {
                let a = self.eval(x, rel)?;
                let av = self.bools(&a)?;
                Ok(Col { v: Vals::Bool(av.iter().map(|b| !b).collect()), null: a.null.clone().or_else(|| matches!(a.v, Vals::Null(_)).then(|| vec![true; n])) })
            }
            Expr::IsNull(x) => {
                let a = self.eval(x, rel)?;
                Ok(Col { v: Vals::Bool((0..n).map(|i| a.is_null(i)).collect()), null: None })
            }
            Expr::Coalesce(l, r) => {
                let a = self.eval(l, rel)?;
                if a.null.is_none() && !matches!(a.v, Vals::Null(_)) {
                    return Ok(a);
                }
                let b = self.eval(r, rel)?;
                let pick: Vec<bool> = (0..n).map(|i| a.is_null(i)).collect();
                merge_cols(&a, &b, &pick, n)
            }
            Expr::If(c, t, f) => {
                // `if(a > b, a, b)` and its kin over two integer columns (the
                // common-interval chain's bounds): one read of each column,
                // and the row's greater or lesser; null where either is
                if let (Expr::Cmp(op, l, r), Expr::Col(tn), Expr::Col(fn_)) = (c.as_ref(), t.as_ref(), f.as_ref()) {
                    if let (Expr::Col(ln), Expr::Col(rn), CmpOp::Gt | CmpOp::Ge | CmpOp::Lt | CmpOp::Le) =
                        (l.as_ref(), r.as_ref(), op)
                    {
                        let same = tn == ln && fn_ == rn;
                        let swapped = tn == rn && fn_ == ln;
                        if same || swapped {
                            let a = self.eval(l, rel)?;
                            let b = self.eval(r, rel)?;
                            if let (Vals::I64(x), Vals::I64(y)) = (&a.v, &b.v) {
                                let null = or_nulls(&a, &b, n);
                                let op = *op;
                                let v = par_concat(n, |rg| {
                                    rg.map(|i| {
                                        let hit = match op {
                                            CmpOp::Gt => x[i] > y[i],
                                            CmpOp::Ge => x[i] >= y[i],
                                            CmpOp::Lt => x[i] < y[i],
                                            _ => x[i] <= y[i],
                                        };
                                        if hit == same { x[i] } else { y[i] }
                                    })
                                    .collect()
                                });
                                return Ok(Col { v: Vals::I64(v), null });
                            }
                        }
                    }
                }
                let cond = self.eval(c, rel)?;
                let cv = self.bools(&cond)?;
                // each fallible branch runs over only the rows that select it
                let sel_then: Vec<bool> = (0..n).map(|i| cv[i] && !cond.is_null(i)).collect();
                let sel_else: Vec<bool> = (0..n).map(|i| !cv[i] && !cond.is_null(i)).collect();
                let a = self.eval_on(t, rel, &sel_then)?;
                let b = self.eval_on(f, rel, &sel_else)?;
                // rows: then where cond true, else where false, null where null
                let pick_else: Vec<bool> = (0..n).map(|i| !cv[i]).collect();
                let mut out = merge_cols(&a, &b, &pick_else, n)?;
                if cond.null.is_some() || matches!(cond.v, Vals::Null(_)) {
                    let mut m = out.null.take().unwrap_or_else(|| vec![false; n]);
                    for (i, slot) in m.iter_mut().enumerate() {
                        if cond.is_null(i) {
                            *slot = true;
                        }
                    }
                    out.null = Some(m);
                }
                Ok(out)
            }
            Expr::CastInt(x) => {
                let a = self.eval(x, rel)?;
                let mut out = vec![0i64; n];
                for (i, slot) in out.iter_mut().enumerate() {
                    if a.is_null(i) {
                        continue;
                    }
                    *slot = match &a.v {
                        Vals::I64(v) => v[i],
                        Vals::Bool(v) => v[i] as i64,
                        Vals::F64(_) => return refuse("cast(float, int)"),
                        _ => {
                            let Some(t) = self.text_at(&a, i)? else { return unsupported("cast of this type") };
                            canonical_decimal(&t).map_or_else(|| refuse("cast of a non-canonical decimal"), Ok)?
                        }
                    };
                }
                Ok(Col { v: Vals::I64(out), null: a.null.clone() })
            }
            Expr::CastFloat(x) => {
                let a = self.eval(x, rel)?;
                let v = match &a.v {
                    Vals::I64(v) => v.iter().map(|&x| x as f64).collect(),
                    Vals::F64(v) => v.clone(),
                    Vals::Bool(v) => v.iter().map(|&x| x as i64 as f64).collect(),
                    _ => return unsupported("cast to float of this type"),
                };
                Ok(Col { v: Vals::F64(v), null: a.null.clone() })
            }
            Expr::StartsWith(x, p) => {
                let a = self.eval(x, rel)?;
                let b = self.eval(p, rel)?;
                let null = or_nulls(&a, &b, n);
                let mut out = vec![false; n];
                for (i, slot) in out.iter_mut().enumerate() {
                    if a.is_null(i) || b.is_null(i) {
                        continue;
                    }
                    let (Vals::Str(xs), Vals::Str(ps)) = (&a.v, &b.v) else {
                        return refuse("starts_with over a non-string value");
                    };
                    *slot = xs[i].starts_with(ps[i].as_ref());
                }
                Ok(Col { v: Vals::Bool(out), null })
            }
            Expr::Div(l, r) => self.eval_div(l, r, rel, false),
            Expr::FloorDiv(l, r) => self.eval_div(l, r, rel, true),
            Expr::Math(op, x) => self.eval_math(*op, x, rel),
            Expr::Calendar(f, a, b) => self.eval_calendar(*f, a, b.as_deref(), rel),
            Expr::StrFn(op, args) => self.eval_strfn(*op, args, rel),
            Expr::Arith(op, l, r) => {
                let a = self.eval(l, rel)?;
                let b = self.eval(r, rel)?;
                let null = or_nulls(&a, &b, n);
                let num = |c: &Col| -> DResult<Option<Vec<i64>>> {
                    Ok(match &c.v {
                        Vals::I64(v) => Some(v.clone()),
                        Vals::Bool(v) => Some(v.iter().map(|&x| x as i64).collect()),
                        Vals::Null(k) => Some(vec![0; *k]),
                        Vals::F64(_) => None,
                        _ => return refuse("arithmetic over a non-numeric column"),
                    })
                };
                match (num(&a)?, num(&b)?) {
                    (Some(x), Some(y)) => {
                        let v = (0..n)
                            .map(|i| match op {
                                ArithOp::Add => x[i].wrapping_add(y[i]),
                                ArithOp::Sub => x[i].wrapping_sub(y[i]),
                                ArithOp::Mul => x[i].wrapping_mul(y[i]),
                            })
                            .collect();
                        Ok(Col { v: Vals::I64(v), null })
                    }
                    _ => {
                        let f = |c: &Col| -> Vec<f64> {
                            match &c.v {
                                Vals::I64(v) => v.iter().map(|&x| x as f64).collect(),
                                Vals::F64(v) => v.clone(),
                                Vals::Bool(v) => v.iter().map(|&x| x as i64 as f64).collect(),
                                _ => vec![0.0; n],
                            }
                        };
                        let (x, y) = (f(&a), f(&b));
                        let v: Vec<f64> = (0..n)
                            .map(|i| match op {
                                ArithOp::Add => x[i] + y[i],
                                ArithOp::Sub => x[i] - y[i],
                                ArithOp::Mul => x[i] * y[i],
                            })
                            .collect();
                        if v.iter().enumerate().any(|(i, x)| !x.is_finite() && !(a.is_null(i) || b.is_null(i))) {
                            return refuse("a non-finite arithmetic result");
                        }
                        Ok(Col { v: Vals::F64(v), null })
                    }
                }
            }
        }
    }

    /// `e` over the rows `sel` selects, the others null; the whole column
    /// when `e` cannot refuse a row (so nothing is gathered).
    fn eval_on(&mut self, e: &Expr, rel: &Rel, sel: &[bool]) -> DResult<Col> {
        if !fallible(e) || sel.iter().all(|&b| b) {
            return self.eval(e, rel);
        }
        let idx: Vec<usize> = (0..rel.n).filter(|&i| sel[i]).collect();
        let sub = self.eval(e, &rel.take(&idx))?;
        let mut pos = vec![None; rel.n];
        for (j, &i) in idx.iter().enumerate() {
            pos[i] = Some(j);
        }
        if sub.len() == 0 {
            return Ok(match sub.v {
                Vals::F64(_) => Col { v: Vals::F64(vec![0.0; rel.n]), null: Some(vec![true; rel.n]) },
                Vals::I64(_) => Col { v: Vals::I64(vec![0; rel.n]), null: Some(vec![true; rel.n]) },
                Vals::Bool(_) => Col { v: Vals::Bool(vec![false; rel.n]), null: Some(vec![true; rel.n]) },
                Vals::Str(_) => Col { v: Vals::Str(vec![Arc::from(""); rel.n]), null: Some(vec![true; rel.n]) },
                _ => Col { v: Vals::Null(rel.n), null: None },
            });
        }
        Ok(sub.take_opt(&pos))
    }

    /// A numeric operand: integers (bools widened), floats, or all null.
    fn numeric(c: &Col) -> DResult<Num> {
        Ok(match &c.v {
            Vals::I64(v) => Num::I(v.clone()),
            Vals::Bool(v) => Num::I(v.iter().map(|&x| x as i64).collect()),
            Vals::Null(k) => Num::I(vec![0; *k]),
            Vals::F64(v) => Num::F(v.clone()),
            _ => return unsupported("arithmetic over a non-numeric column"),
        })
    }

    fn eval_div(&mut self, l: &Expr, r: &Expr, rel: &Rel, floor: bool) -> DResult<Col> {
        let n = rel.n;
        let a = self.eval(l, rel)?;
        let b = self.eval(r, rel)?;
        let null = or_nulls(&a, &b, n);
        let defined = |i: usize| !(a.is_null(i) || b.is_null(i));
        let (x, y) = (Self::numeric(&a)?, Self::numeric(&b)?);
        for i in 0..n {
            if defined(i) && y.is_zero(i) {
                return refuse("division by zero");
            }
        }
        let out = match (&x, &y, floor) {
            (Num::I(x), Num::I(y), true) => {
                let mut v = vec![0i64; n];
                for i in 0..n {
                    if defined(i) {
                        let Some(q) = x[i].checked_div(y[i]) else {
                            return refuse("an integer quotient outside int64");
                        };
                        let r = x[i] - q * y[i];
                        v[i] = if r != 0 && ((r < 0) != (y[i] < 0)) { q - 1 } else { q };
                    }
                }
                return Ok(Col { v: Vals::I64(v), null });
            }
            (Num::I(x), Num::I(y), false) => {
                let mut v = vec![0.0f64; n];
                for i in 0..n {
                    if defined(i) {
                        let Some(q) = blessed_quotient(x[i], y[i]) else {
                            return unsupported("an integer quotient past 2^53");
                        };
                        v[i] = q;
                    }
                }
                v
            }
            _ => {
                let (x, y) = (x.floats(), y.floats());
                (0..n)
                    .map(|i| if !defined(i) { 0.0 } else if floor { py_floor_div(x[i], y[i]) } else { x[i] / y[i] })
                    .collect()
            }
        };
        if (0..n).any(|i| defined(i) && !out[i].is_finite()) {
            return refuse("a non-finite quotient");
        }
        Ok(Col { v: Vals::F64(out), null })
    }

    fn eval_math(&mut self, op: MathOp, x: &Expr, rel: &Rel) -> DResult<Col> {
        let n = rel.n;
        let a = self.eval(x, rel)?;
        let null = a.null.clone().or_else(|| matches!(a.v, Vals::Null(_)).then(|| vec![true; n]));
        let v = Self::numeric(&a)?;
        let defined = |i: usize| !a.is_null(i);
        match (op, &v) {
            (MathOp::Abs, Num::I(v)) => Ok(Col { v: Vals::I64(v.iter().map(|x| x.wrapping_abs()).collect()), null }),
            (MathOp::Abs, Num::F(v)) => Ok(Col { v: Vals::F64(v.iter().map(|x| x.abs()).collect()), null }),
            (MathOp::Sqrt, _) => {
                let f = v.floats();
                if (0..n).any(|i| defined(i) && f[i] < 0.0) {
                    return refuse("sqrt of a negative value");
                }
                let out: Vec<f64> = f.iter().map(|x| x.sqrt()).collect();
                if (0..n).any(|i| defined(i) && !out[i].is_finite()) {
                    return refuse("a non-finite sqrt");
                }
                Ok(Col { v: Vals::F64(out), null })
            }
            _ => {
                let f = v.floats();
                let mut out = vec![0i64; n];
                for i in 0..n {
                    if !defined(i) {
                        continue;
                    }
                    let r = match op {
                        MathOp::Round => f[i].round_ties_even(),
                        MathOp::Floor => f[i].floor(),
                        _ => f[i].ceil(),
                    };
                    // the reference's float64 -> int64 cast is defined only in range
                    if !(-9.223_372_036_854_776e18..9.223_372_036_854_776e18).contains(&r) {
                        return refuse("a rounded value outside int64");
                    }
                    out[i] = r as i64;
                }
                Ok(Col { v: Vals::I64(out), null })
            }
        }
    }

    fn eval_calendar(&mut self, f: CalFn, a: &Expr, b: Option<&Expr>, rel: &Rel) -> DResult<Col> {
        let n = rel.n;
        let x = self.eval(a, rel)?;
        let y = match b {
            Some(b) => Some(self.eval(b, rel)?),
            None => None,
        };
        let ints = |c: &Col| -> DResult<Vec<i64>> {
            match &c.v {
                Vals::I64(v) => Ok(v.clone()),
                Vals::Null(k) => Ok(vec![0; *k]),
                _ => unsupported("a calendar field of a non-integer column"),
            }
        };
        let xv = ints(&x)?;
        let yv = match &y {
            Some(c) => Some(ints(c)?),
            None => None,
        };
        let null = match &y {
            Some(c) => or_nulls(&x, c, n),
            None => x.null.clone().or_else(|| matches!(x.v, Vals::Null(_)).then(|| vec![true; n])),
        };
        let mut out = vec![0i64; n];
        for i in 0..n {
            if x.is_null(i) || y.as_ref().is_some_and(|c| c.is_null(i)) {
                continue;
            }
            let (yy, mm, dd) = civil(xv[i]);
            out[i] = match f {
                CalFn::Year => yy,
                CalFn::Month => mm,
                CalFn::Day => dd,
                CalFn::MonthDay => mm * 100 + dd,
                CalFn::MonthsBetween => {
                    let (y2, m2, _) = civil(yv.as_ref().map_or(0, |v| v[i]));
                    (y2 - yy) * 12 + (m2 - mm)
                }
            };
        }
        Ok(Col { v: Vals::I64(out), null })
    }

    fn eval_strfn(&mut self, op: StrOp, args: &[Expr], rel: &Rel) -> DResult<Col> {
        let n = rel.n;
        let cols = args.iter().map(|a| self.eval(a, rel)).collect::<DResult<Vec<_>>>()?;
        let null: Option<Vec<bool>> = if cols.iter().any(|c| c.null.is_some() || matches!(c.v, Vals::Null(_))) {
            Some((0..n).map(|i| cols.iter().any(|c| c.is_null(i))).collect())
        } else {
            None
        };
        let is_null = |i: usize| null.as_ref().is_some_and(|m| m[i]);
        let text = |me: &Self, c: &Col, i: usize| -> DResult<Arc<str>> {
            me.text_at(c, i)?.map_or_else(|| unsupported("a string function over a non-string column"), Ok)
        };
        match op {
            StrOp::Contains | StrOp::Like => {
                let mut out = vec![false; n];
                for (i, slot) in out.iter_mut().enumerate() {
                    if is_null(i) {
                        continue;
                    }
                    let (s, t) = (text(self, &cols[0], i)?, text(self, &cols[1], i)?);
                    *slot = if op == StrOp::Contains {
                        s.contains(t.as_ref())
                    } else {
                        let (sc, tc): (Vec<char>, Vec<char>) = (s.chars().collect(), t.chars().collect());
                        like(&sc, &tc)
                    };
                }
                Ok(Col { v: Vals::Bool(out), null })
            }
            StrOp::Substring => {
                let int_at = |c: &Col, i: usize| -> DResult<i64> {
                    match &c.v {
                        Vals::I64(v) => Ok(v[i]),
                        _ => unsupported("substring bounds of a non-integer column"),
                    }
                };
                let mut out: Vec<Arc<str>> = Vec::with_capacity(n);
                for i in 0..n {
                    if is_null(i) {
                        out.push(Arc::from(""));
                        continue;
                    }
                    let s = text(self, &cols[0], i)?;
                    let (start, len) = (int_at(&cols[1], i)? as i128, int_at(&cols[2], i)? as i128);
                    let begin = start - 1;
                    let end = begin + len;
                    let b = begin.max(0);
                    out.push(if end <= b {
                        Arc::from("")
                    } else {
                        let k = (end - b).min(usize::MAX as i128) as usize;
                        let sk = b.min(usize::MAX as i128) as usize;
                        Arc::from(s.chars().skip(sk).take(k).collect::<String>().as_str())
                    });
                }
                Ok(Col { v: Vals::Str(out), null })
            }
        }
    }

    /// `column ⋈ literal` (either side) on integers or floats, compared
    /// against the scalar instead of a broadcast column: `None` when the
    /// shape is anything else.
    fn cmp_literal(&mut self, op: CmpOp, l: &Expr, r: &Expr, rel: &Rel) -> DResult<Option<Col>> {
        let (col, lit, flipped) = match (l, r) {
            (e, Expr::Lit(x)) if !matches!(e, Expr::Lit(_)) => (e, x, false),
            (Expr::Lit(x), e) if !matches!(e, Expr::Lit(_)) => (e, x, true),
            _ => return Ok(None),
        };
        let (li, lf) = match lit {
            Lit::Int(v) => (Some(*v), None),
            Lit::Float(v) => (None, Some(*v)),
            _ => return Ok(None),
        };
        let c = self.eval(col, rel)?;
        let ord = |o: Ordering| -> bool {
            let o = if flipped { o.reverse() } else { o };
            match op {
                CmpOp::Eq => o == Ordering::Equal,
                CmpOp::Ne => o != Ordering::Equal,
                CmpOp::Lt => o == Ordering::Less,
                CmpOp::Le => o != Ordering::Greater,
                CmpOp::Gt => o == Ordering::Greater,
                CmpOp::Ge => o != Ordering::Less,
            }
        };
        let out: Vec<bool> = match (&c.v, li, lf) {
            (Vals::I64(x), Some(v), _) => x.iter().map(|&a| ord(a.cmp(&v))).collect(),
            (Vals::I64(x), None, Some(v)) => {
                x.iter().map(|&a| ord((a as f64).partial_cmp(&v).unwrap_or(Ordering::Equal))).collect()
            }
            (Vals::F64(x), Some(v), _) => {
                let v = v as f64;
                x.iter().map(|&a| ord(a.partial_cmp(&v).unwrap_or(Ordering::Equal))).collect()
            }
            (Vals::F64(x), None, Some(v)) => x.iter().map(|&a| ord(a.partial_cmp(&v).unwrap_or(Ordering::Equal))).collect(),
            _ => return Ok(None),
        };
        Ok(Some(Col { v: Vals::Bool(out), null: c.null.clone() }))
    }

    fn bools(&self, c: &Col) -> DResult<Vec<bool>> {
        match &c.v {
            Vals::Bool(b) => Ok(b.clone()),
            Vals::Null(n) => Ok(vec![false; *n]),
            _ => unsupported("a boolean operand of another type"),
        }
    }

    fn prop_raw_present(&mut self, kind: RowKind, a: Addr, key: &Arc<str>) -> DResult<bool> {
        Ok(!matches!(self.prop(kind, a, key)?, TVal::Absent) || {
            // JSON null is present for an untyped read (the evaluator returns
            // it as a value, not as a missing key)
            let text = self.props_text(kind, a)?;
            let parsed: J = serde_json::from_str(text).unwrap_or(J::Null);
            parsed.get(key.as_ref()).is_some()
        })
    }

    fn typed_prop(&mut self, rel: &Rel, col: &str, key: &str, tau: Tau) -> DResult<Col> {
        let n = rel.n;
        let Some(var) = col.strip_suffix(".props") else { return unsupported("tprop of a non-props column") };
        let (kind, ver) = match rel.var(var) {
            Some(Var::Node { ver, .. }) => (RowKind::Node, ver.clone()),
            Some(Var::Edge { ver }) => (RowKind::Edge, ver.clone()),
            None => return unsupported("tprop of an unbound variable"),
        };
        let key: Arc<str> = Arc::from(key);
        let mut null = vec![true; n];
        let tv = self.props_of(kind, &ver, &key)?;
        let v = match tau {
            Tau::Int | Tau::Ts | Tau::Float => {
                let float = tau == Tau::Float;
                let mut iv = vec![0i64; if float { 0 } else { n }];
                let mut fv = vec![0f64; if float { n } else { 0 }];
                for (i, t) in tv.iter().enumerate() {
                    match t {
                        TVal::Int(x) => {
                            null[i] = false;
                            if float {
                                fv[i] = *x as f64;
                            } else {
                                iv[i] = *x;
                            }
                        }
                        TVal::Float(x) if float => {
                            null[i] = false;
                            fv[i] = *x;
                        }
                        TVal::BigInt => return refuse("a typed read of an integer outside int64"),
                        _ => {}
                    }
                }
                if float {
                    Vals::F64(fv)
                } else {
                    Vals::I64(iv)
                }
            }
            Tau::Bool => {
                let mut bv = vec![false; n];
                for (i, t) in tv.iter().enumerate() {
                    if let TVal::Bool(x) = t {
                        null[i] = false;
                        bv[i] = *x;
                    }
                }
                Vals::Bool(bv)
            }
            Tau::Str => {
                let empty: Arc<str> = Arc::from("");
                let mut sv = vec![empty; n];
                for (i, t) in tv.into_iter().enumerate() {
                    if let TVal::Str(x) = t {
                        null[i] = false;
                        sv[i] = x;
                    }
                }
                Vals::Str(sv)
            }
        };
        Ok(Col { v, null: Some(null) })
    }
}

/// Does a predicate read nothing but the target `into` (not its depth,
/// which varies by row)? Such a predicate is a function of the target id.
fn target_local(p: &Op, into: &str) -> bool {
    match p {
        Op::TypeConstraint { var, .. } => var == into,
        Op::Filter(e) => {
            let mut names = Vec::new();
            expr_names(e, &mut names);
            !names.is_empty()
                && names.iter().all(|n| n.split_once('.').is_some_and(|(v, f)| v == into && f != "depth"))
        }
        _ => false,
    }
}

/// Column names an expression reads (`prop`/`tprop` name `<var>.props`).
fn expr_names(e: &Expr, out: &mut Vec<String>) {
    match e {
        Expr::Lit(_) => {}
        Expr::Col(c) | Expr::TProp { col: c, .. } | Expr::Prop { col: c, .. } => out.push(c.clone()),
        Expr::Not(x) | Expr::IsNull(x) | Expr::CastInt(x) | Expr::CastFloat(x) => expr_names(x, out),
        Expr::Cmp(_, a, b)
        | Expr::And(a, b)
        | Expr::Or(a, b)
        | Expr::Coalesce(a, b)
        | Expr::StartsWith(a, b)
        | Expr::Arith(_, a, b) => {
            expr_names(a, out);
            expr_names(b, out);
        }
        Expr::If(a, b, c) => {
            expr_names(a, out);
            expr_names(b, out);
            expr_names(c, out);
        }
        Expr::Div(a, b) | Expr::FloorDiv(a, b) => {
            expr_names(a, out);
            expr_names(b, out);
        }
        Expr::Math(_, x) => expr_names(x, out),
        Expr::Calendar(_, a, b) => {
            expr_names(a, out);
            if let Some(b) = b {
                expr_names(b, out);
            }
        }
        Expr::StrFn(_, args) => {
            for a in args {
                expr_names(a, out);
            }
        }
    }
}

/// Whether evaluating an expression can refuse a row (a zero divisor, a
/// negative square root, a non-canonical cast): `if` then evaluates each
/// branch over only the rows that select it, as the reference does.
fn fallible(e: &Expr) -> bool {
    match e {
        Expr::Div(..) | Expr::FloorDiv(..) | Expr::CastInt(_) => true,
        Expr::Math(MathOp::Sqrt | MathOp::Round | MathOp::Floor | MathOp::Ceil, _) => true,
        Expr::Lit(_) | Expr::Col(_) | Expr::TProp { .. } | Expr::Prop { .. } => false,
        Expr::Not(x) | Expr::IsNull(x) | Expr::CastFloat(x) | Expr::Math(_, x) => fallible(x),
        Expr::Cmp(_, a, b)
        | Expr::And(a, b)
        | Expr::Or(a, b)
        | Expr::Coalesce(a, b)
        | Expr::StartsWith(a, b)
        | Expr::Arith(_, a, b) => fallible(a) || fallible(b),
        Expr::If(a, b, c) => fallible(a) || fallible(b) || fallible(c),
        Expr::Calendar(_, a, b) => fallible(a) || b.as_ref().is_some_and(|b| fallible(b)),
        Expr::StrFn(_, args) => args.iter().any(fallible),
    }
}

/// (year, month, day) of a UTC instant in microseconds, proleptic Gregorian.
fn civil(us: i64) -> (i64, i64, i64) {
    let days = us.div_euclid(86_400_000_000);
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// SQL `LIKE` over code points: `%` any run, `_` one character.
fn like(text: &[char], pat: &[char]) -> bool {
    let (mut t, mut p) = (0usize, 0usize);
    let (mut star, mut mark) = (None::<usize>, 0usize);
    while t < text.len() {
        if p < pat.len() && pat[p] != '%' && (pat[p] == '_' || pat[p] == text[t]) {
            t += 1;
            p += 1;
        } else if p < pat.len() && pat[p] == '%' {
            star = Some(p);
            mark = t;
            p += 1;
        } else if let Some(sp) = star {
            p = sp + 1;
            mark += 1;
            t = mark;
        } else {
            return false;
        }
    }
    while p < pat.len() && pat[p] == '%' {
        p += 1;
    }
    p == pat.len()
}

/// Python's float floor division (`divmod`'s quotient), as the reference's
/// numpy `//` computes it.
fn py_floor_div(a: f64, b: f64) -> f64 {
    let m = a % b;
    let mut div = (a - m) / b;
    if m != 0.0 && ((b < 0.0) != (m < 0.0)) {
        div -= 1.0;
    }
    if div != 0.0 {
        let f = div.floor();
        if div - f > 0.5 {
            f + 1.0
        } else {
            f
        }
    } else {
        0.0f64.copysign(a / b)
    }
}

/// The blessed quotient of two integers, as a float: exact quotients are
/// `float(q)`, the others `float(q) + r / d` with `(q, r)` the floor
/// division. `None` past the range where `r / d` is formed exactly as the
/// reference's integer true division forms it.
fn blessed_quotient(num: i64, den: i64) -> Option<f64> {
    const EXACT: i64 = 1 << 53;
    let (mut q, mut r) = (num.checked_div(den)?, num.checked_rem(den)?);
    if r != 0 && ((r < 0) != (den < 0)) {
        q -= 1;
        r += den;
    }
    if r == 0 {
        return Some(q as f64);
    }
    if r.unsigned_abs() > EXACT as u64 || den.unsigned_abs() > EXACT as u64 {
        return None;
    }
    Some(q as f64 + r as f64 / den as f64)
}

/// `b` where `pick` holds, else `a` (the `np.where` of coalesce and if).
fn merge_cols(a: &Col, b: &Col, pick: &[bool], n: usize) -> DResult<Col> {
    use Vals::*;
    let null = Some((0..n).map(|i| if pick[i] { b.is_null(i) } else { a.is_null(i) }).collect());
    let v = match (&a.v, &b.v) {
        (Null(_), Null(_)) => Null(n),
        (I64(x), I64(y)) => I64((0..n).map(|i| if pick[i] { y[i] } else { x[i] }).collect()),
        (F64(x), F64(y)) => F64((0..n).map(|i| if pick[i] { y[i] } else { x[i] }).collect()),
        (I64(x), F64(y)) => F64((0..n).map(|i| if pick[i] { y[i] } else { x[i] as f64 }).collect()),
        (F64(x), I64(y)) => F64((0..n).map(|i| if pick[i] { y[i] as f64 } else { x[i] }).collect()),
        (Bool(x), Bool(y)) => Bool((0..n).map(|i| if pick[i] { y[i] } else { x[i] }).collect()),
        (Str(x), Str(y)) => Str((0..n).map(|i| if pick[i] { y[i].clone() } else { x[i].clone() }).collect()),
        (Uid(x), Uid(y)) => Uid((0..n).map(|i| if pick[i] { y[i] } else { x[i] }).collect()),
        (Null(_), other) => other.clone(),
        (other, Null(_)) => other.clone(),
        _ => return unsupported("coalesce/if over two different types"),
    };
    Ok(Col { v, null })
}

fn canonical_decimal(t: &str) -> Option<i64> {
    let body = t.strip_prefix('-').unwrap_or(t);
    if body.is_empty() || !body.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    if (body.len() > 1 && body.starts_with('0')) || t == "-0" {
        return None;
    }
    t.parse::<i64>().ok()
}

// --------------------------------------------------------------------------
// the page
// --------------------------------------------------------------------------

impl<'s, 'a> Ctx<'s, 'a> {
    fn value(&mut self, c: &Col, i: usize) -> DResult<MVal> {
        if c.is_null(i) {
            return Ok(MVal::Null);
        }
        Ok(match &c.v {
            Vals::Null(_) => MVal::Null,
            Vals::I64(x) => MVal::Int(x[i]),
            Vals::F64(x) => {
                if !x[i].is_finite() {
                    return refuse("a non-finite value");
                }
                MVal::Float(x[i])
            }
            Vals::Bool(x) => MVal::Bool(x[i]),
            Vals::Str(x) => MVal::Str(x[i].to_string()),
            Vals::Uid(x) => MVal::Str(self.uid_text(x[i])?.to_string()),
            Vals::Vid(x) => MVal::Str(Id96 { hi: x[i].0, lo: x[i].1 }.to_hex()),
            Vals::Eid(x) => MVal::Str(self.eid_hex(x[i])?),
            Vals::Label(x) => MVal::Str(self.labels.text(x[i]).to_string()),
            Vals::RelType(x) => MVal::Str(self.rels.text(x[i]).to_string()),
            Vals::JText(x) => MVal::Json(x[i].to_string()),
            Vals::Json(x, kind, key) => {
                let text = self.props_text(*kind, x[i])?;
                match key {
                    None => MVal::Json(text.to_string()),
                    Some(k) => {
                        let parsed: J = serde_json::from_str(text).unwrap_or(J::Null);
                        match parsed.get(k.as_ref()) {
                            Some(v) => MVal::Json(serde_json::to_string(v).map_err(|e| Decline::Refuse(e.to_string()))?),
                            None => MVal::Null,
                        }
                    }
                }
            }
        })
    }
}

impl NativeStore {
    /// Execute one compiled `match` plan (`plan_json`, format 1) and return
    /// the page `[offset, offset + limit)` of its answer with the answer's
    /// size, or `None` when the operator should take its reference route
    /// (see the module documentation for when).
    pub fn match_plan(&self, plan_json: &str, offset: usize, limit: usize) -> Result<Option<MatchOut>> {
        if self.in_batch() {
            return Ok(None);
        }
        let plan = match parse_plan(plan_json) {
            Ok(p) => p,
            Err(Decline::Engine(e)) => return Err(e),
            Err(d) => {
                note_decline(&d);
                return Ok(None);
            }
        };
        self.assert_full_belief(plan.as_of)?;
        match self.match_inner(&plan, offset, limit) {
            Ok(out) => Ok(Some(out)),
            Err(Decline::Engine(e)) => Err(e),
            Err(d) => {
                note_decline(&d);
                Ok(None)
            }
        }
    }

    /// Node versions written per label, over the generation's node
    /// segments (every version, believed or not: an estimate for the
    /// `match` compiler's step order, which needs only magnitudes). One
    /// pass over each segment's label column; cached per generation.
    pub fn match_label_counts(&self) -> Result<Vec<(String, u64)>> {
        let gen = self.generation();
        if let Ok(g) = self.match_labels.lock() {
            if let Some((k, v)) = g.as_ref() {
                if *k == gen {
                    return Ok(v.as_ref().clone());
                }
            }
        }
        let mut counts: HashMap<String, u64> = HashMap::new();
        for (f, _id) in self.node_files() {
            let seg = self.open_segment(&f)?;
            if seg.rows() == 0 {
                continue;
            }
            let mut per: HashMap<u32, u64> = HashMap::new();
            for &r in seg.u32_column("label_ref")? {
                *per.entry(r).or_default() += 1;
            }
            let strings = seg.strings()?;
            for (r, n) in per {
                *counts.entry(strings.get(r)?.to_string()).or_default() += n;
            }
        }
        let mut out: Vec<(String, u64)> = counts.into_iter().collect();
        out.sort_unstable();
        if let Ok(mut g) = self.match_labels.lock() {
            *g = Some((gen, Arc::new(out.clone())));
        }
        Ok(out)
    }

    /// `match_plan` for a weight pattern, column-wise (`PairsOut`); `None`
    /// sends the caller to the rows.
    pub fn match_plan_pairs(&self, plan_json: &str) -> Result<Option<PairsOut>> {
        if self.in_batch() {
            return Ok(None);
        }
        let plan = match parse_plan(plan_json) {
            Ok(p) => p,
            Err(Decline::Engine(e)) => return Err(e),
            Err(d) => {
                note_decline(&d);
                return Ok(None);
            }
        };
        self.assert_full_belief(plan.as_of)?;
        match self.with_ctx(&plan, |ctx| ctx.answer_pairs()) {
            Ok(out) => Ok(Some(out)),
            Err(Decline::Engine(e)) => Err(e),
            Err(d) => {
                note_decline(&d);
                Ok(None)
            }
        }
    }

    fn match_inner(&self, plan: &Plan, offset: usize, limit: usize) -> DResult<MatchOut> {
        self.with_ctx(plan, |ctx| ctx.answer(offset, limit))
    }

    /// Open the generation's segments once, build the executor's context
    /// over them, and run `f` in it; the typed property columns `f` filled
    /// go back to the handle's cache.
    fn with_ctx<R>(&self, plan: &Plan, f: impl FnOnce(&mut Ctx<'_, '_>) -> DResult<R>) -> DResult<R> {
        let _threads = ThreadsGuard::set(self.designs().match_threads(), self.designs().match_par_rows);
        let node_files = self.node_files();
        let edge_files = self.edge_files();
        let mut node_segs = Vec::with_capacity(node_files.len());
        for (f, id) in &node_files {
            node_segs.push((self.open_segment(f)?, *id));
        }
        let mut edge_segs = Vec::with_capacity(edge_files.len());
        for (f, id) in &edge_files {
            edge_segs.push((self.open_segment(f)?, *id));
        }
        let mut rels = Interner::default();
        let mut nodes = Vec::with_capacity(node_segs.len());
        for (s, id) in &node_segs {
            nodes.push(SegCols::open(s, *id, RowKind::Node, &mut rels)?);
        }
        let mut edges = Vec::with_capacity(edge_segs.len());
        for (s, id) in &edge_segs {
            edges.push(SegCols::open(s, *id, RowKind::Edge, &mut rels)?);
        }
        let n_node_segs = nodes.len();
        let mut ctx = Ctx {
            store: self,
            plan,
            node_by_id: nodes.iter().enumerate().map(|(i, s)| (s.id, i)).collect(),
            edge_by_id: edges.iter().enumerate().map(|(i, s)| (s.id, i)).collect(),
            nodes,
            edges,
            closes: self.close_index()?,
            rels,
            labels: Interner::default(),
            label_codes: vec![Vec::new(); n_node_segs],
            props: FxMap::default(),
            node_map: None,
            semi: None,
            downstream: FxMap::default(),
            edge_est: std::sync::Mutex::new(FxMap::default()),
        };
        let out = f(&mut ctx);
        // the typed columns this call filled go back to the handle's cache
        for ((kind, si, key), mut table) in std::mem::take(&mut ctx.props) {
            let id = if kind == RowKind::Node as u8 { ctx.nodes[si].id } else { ctx.edges[si].id };
            table.settle();
            self.match_props.put((kind, id, key), table);
        }
        out
    }
}

impl<'s, 'a> Ctx<'s, 'a> {
    /// The plan's page and the answer's size.
    /// The whole answer's `src`, `dst` (node identities, no null) and `w`
    /// (integer, float or null) columns; a refusal for any other shape (the
    /// caller reads rows then).
    fn answer_pairs(&mut self) -> DResult<PairsOut> {
        let mut memo: FxMap<usize, Rel> = FxMap::default();
        let rel = self.run(self.plan.root, &mut memo)?;
        let ids = |c: Option<&Col>| -> DResult<Vec<u32>> {
            match c {
                Some(Col { v: Vals::Uid(x), null: None }) => Ok(x.clone()),
                Some(Col { v: Vals::Uid(x), null: Some(m) }) if !m.iter().any(|&b| b) => Ok(x.clone()),
                _ => unsupported("a pair table whose ends are not node identities"),
            }
        };
        let src = ids(rel.col("src"))?;
        let dst = ids(rel.col("dst"))?;
        let Some(w) = rel.col("w") else { return unsupported("a pair table without w") };
        let n = rel.n;
        let (mut kind, mut wi, mut wf) = (vec![0u8; n], vec![0i64; n], vec![0f64; n]);
        for i in 0..n {
            if w.is_null(i) {
                continue;
            }
            match &w.v {
                Vals::I64(x) => {
                    kind[i] = 1;
                    wi[i] = x[i];
                }
                Vals::F64(x) => {
                    if !x[i].is_finite() {
                        return refuse("a non-finite value");
                    }
                    kind[i] = 2;
                    wf[i] = x[i];
                }
                Vals::Null(_) => {}
                _ => return unsupported("a pair weight of this type"),
            }
        }
        Ok(PairsOut { src, dst, w_kind: kind, w_int: wi, w_f64: wf })
    }

    fn answer(&mut self, offset: usize, limit: usize) -> DResult<MatchOut> {
        let plan = self.plan;
        let ctx = self;
        let t0 = std::time::Instant::now();
        // the root chain [Project listed] <- [Limit] <- Order: the order is cut
        // to the page instead of sorting the whole answer
        let mut memo: FxMap<usize, Rel> = FxMap::default();
        let mut chain: Vec<usize> = Vec::new();
        let mut id = plan.root;
        loop {
            match &plan.nodes[id].op {
                Op::Project { keep_all: false, .. } | Op::Limit(_) if chain.len() < 2 => {
                    chain.push(id);
                    id = plan.nodes[id].inputs[0];
                }
                _ => break,
            }
        }
        let (rel, total) = if let Op::Order(keys) = &plan.nodes[id].op {
            let input_id = plan.nodes[id].inputs[0];
            let cap = chain.iter().find_map(|&c| match plan.nodes[c].op {
                Op::Limit(n) => Some(n),
                _ => None,
            });
            let (input, all) = ctx.order_input(input_id, keys, cap, offset.saturating_add(limit), &mut memo)?;
            let total = cap.map_or(all, |n| n.min(all));
            let want = (offset.saturating_add(limit)).min(total);
            let perm = ctx.order(&input, keys, Some(want))?;
            let page: Vec<usize> = perm.into_iter().skip(offset).take(limit).collect();
            let mut rel = input.take(&page);
            // the chain's projection, if any
            for &c in chain.iter().rev() {
                if let Op::Project { bindings, keep_all: false } = &plan.nodes[c].op {
                    rel = ctx.apply(&Op::Project { bindings: bindings.clone(), keep_all: false }, vec![rel])?;
                }
            }
            (rel, total)
        } else {
            let rel = ctx.run(plan.root, &mut memo)?;
            let total = rel.n;
            let page: Vec<usize> = (offset.min(total)..(offset.saturating_add(limit)).min(total)).collect();
            (rel.take(&page), total)
        };
        crate::phase::add(crate::phase::Phase::MatchExecNs, t0.elapsed().as_nanos() as u64);
        let t1 = std::time::Instant::now();
        let mut rows = Vec::with_capacity(rel.n);
        let cols: Vec<Col> = plan
            .columns
            .iter()
            .map(|c| ctx.eval(&Expr::Col(c.clone()), &rel))
            .collect::<DResult<_>>()?;
        for i in 0..rel.n {
            let mut row = Vec::with_capacity(cols.len());
            for c in &cols {
                row.push(ctx.value(c, i)?);
            }
            rows.push(row);
        }
        crate::phase::add(crate::phase::Phase::MatchPageNs, t1.elapsed().as_nanos() as u64);
        crate::phase::add(crate::phase::Phase::MatchCalls, 1);
        Ok(MatchOut { columns: plan.columns.clone(), rows, rows_total: total as u64 })
    }
}

// --------------------------------------------------------------------------
// the seed-driven snapshot (`snapshot_subgraph`)
// --------------------------------------------------------------------------

/// One edge row of a seed-driven snapshot, as `snapshot_subgraph` emits it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapEdge {
    pub eid: String,
    pub vid: String,
    pub src: String,
    pub dst: String,
    pub rel_type: String,
    pub vt_s: i64,
    pub vt_e: i64,
}

/// The reached nodes `(uid, label, hop)` and the induced edges in the
/// instant scan's `(vt_s, vid)` order.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SnapOut {
    pub nodes: Vec<(String, String, u32)>,
    pub edges: Vec<SnapEdge>,
}

impl NativeStore {
    /// `snapshot_subgraph` from its seeds, with the executor's batched
    /// frontier: an undirected breadth-first search over the edges valid at
    /// `t` and believed at `as_of_tt`, one adjacency call per level for the
    /// whole frontier, entering a node only if it has a version valid at `t`
    /// (identity point reads), then the edges with both ends reached. The
    /// answer of the whole-snapshot path (`bfs_node_set` over `edges_at`,
    /// restricted to `nodes_at`), at any reached-set size. `None` inside an
    /// open write batch.
    pub fn snapshot_expand(
        &self,
        seeds: &[u32],
        t: i64,
        as_of_tt: i64,
        hops: u32,
        rel_types: Option<&[String]>,
    ) -> Result<Option<SnapOut>> {
        if self.in_batch() {
            return Ok(None);
        }
        let as_of = crate::clamp_tt(as_of_tt);
        self.assert_full_belief(as_of)?;
        let plan = Plan { nodes: Vec::new(), root: 0, t_a: t, t_b: t + 1, as_of, columns: Vec::new() };
        match self.with_ctx(&plan, |ctx| ctx.snapshot(seeds, hops, rel_types)) {
            Ok(out) => Ok(Some(out)),
            Err(Decline::Engine(e)) => Err(e),
            Err(d) => {
                note_decline(&d);
                Ok(None)
            }
        }
    }
}

impl<'s, 'a> Ctx<'s, 'a> {
    /// The version, valid at the instant, whose label the instant scan
    /// lists for each id (the greatest in `(vt_s, vid)` where several are):
    /// by identity point reads.
    fn valid_versions(&mut self, ids: &[u32]) -> DResult<FxMap<u32, Addr>> {
        let uids: Vec<String> = ids.iter().filter_map(|&i| self.store.dict().uid(i).map(str::to_string)).collect();
        let mut out: FxMap<u32, Addr> = FxMap::default();
        for (id, a) in self.node_versions_of(&uids, BeliefMode::Current, VtMode::Overlap)? {
            let keep = match out.get(&id) {
                Some(&cur) => self.version_key(a) > self.version_key(cur),
                None => true,
            };
            if keep {
                out.insert(id, a);
            }
        }
        Ok(out)
    }

    fn snapshot(&mut self, seeds: &[u32], hops: u32, rels: Option<&[String]>) -> DResult<SnapOut> {
        let t = self.plan.t_a;
        let mut seeds: Vec<u32> = seeds.to_vec();
        seeds.sort_unstable();
        seeds.dedup();
        let mut valid = self.valid_versions(&seeds)?;
        let mut dist: FxMap<u32, u32> = FxMap::default();
        let mut frontier: Vec<u32> = seeds.iter().copied().filter(|i| valid.contains_key(i)).collect();
        for &s in &frontier {
            dist.insert(s, 0);
        }
        for h in 1..=hops {
            if frontier.is_empty() {
                break;
            }
            let adj = self.neighbors(&frontier, NeighborDir::Both, t, rels)?;
            let mut cand: Vec<u32> = adj.iter().flatten().map(|&(v, _)| v).filter(|v| !dist.contains_key(v)).collect();
            cand.sort_unstable();
            cand.dedup();
            let ask: Vec<u32> = cand.iter().copied().filter(|v| !valid.contains_key(v)).collect();
            valid.extend(self.valid_versions(&ask)?);
            frontier = cand.into_iter().filter(|v| valid.contains_key(v)).collect();
            for &v in &frontier {
                dist.insert(v, h);
            }
        }
        let mut reached: Vec<u32> = dist.keys().copied().collect();
        reached.sort_unstable();
        let mut nodes = Vec::with_capacity(reached.len());
        for &id in &reached {
            let a = valid[&id];
            let label = self.label_code(a)?;
            nodes.push((self.uid_text(id)?.to_string(), self.labels.text(label).to_string(), dist[&id]));
        }
        // each induced edge once: from its source, kept when its target is reached
        let adj = self.neighbors(&reached, NeighborDir::Out, t, rels)?;
        let mut rows: Vec<(i64, u64, u32, Addr, u32, u32)> = Vec::new();
        for (p, list) in adj.iter().enumerate() {
            for &(v, e) in list {
                if reached.binary_search(&v).is_ok() {
                    let s = &self.edges[seg_of(e)];
                    let r = row_of(e);
                    rows.push((s.vt_s[r], s.vid_hi[r], s.vid_lo[r], e, reached[p], v));
                }
            }
        }
        rows.sort_unstable_by_key(|x| (x.0, x.1, x.2));
        let mut edges = Vec::with_capacity(rows.len());
        for (vt_s, hi, lo, e, src, dst) in rows {
            edges.push(SnapEdge {
                eid: self.eid_hex(e)?,
                vid: Id96 { hi, lo }.to_hex(),
                src: self.uid_text(src)?.to_string(),
                dst: self.uid_text(dst)?.to_string(),
                rel_type: self.rels.text(self.rel_code(e)).to_string(),
                vt_s,
                vt_e: self.edges[seg_of(e)].vt_e(row_of(e)),
            });
        }
        Ok(SnapOut { nodes, edges })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_sum_matches_fsum_on_cancellation() {
        // values whose naive left fold loses the small terms
        let mut s = ExactSum::default();
        for _ in 0..50 {
            for x in [1e16, 1.0, -1e16, 0.1, 0.2, 0.3] {
                s.add(x);
            }
        }
        // math.fsum([1e16, 1.0, -1e16, 0.1, 0.2, 0.3] * 50) == 80.0; the left
        // fold gives 0.0
        assert_eq!(s.value(), 80.0);
        let mut r = ExactSum::default();
        for x in [0.1f64; 10] {
            r.add(x);
        }
        assert_eq!(r.value(), 1.0);
    }

    #[test]
    fn exact_sum_of_negative_zeros_is_positive_zero() {
        // math.fsum([-0.0]) == 0.0 and math.fsum([-0.0, -0.0]) == 0.0
        let mut s = ExactSum::default();
        s.add(-0.0);
        assert_eq!(s.value().to_bits(), 0.0f64.to_bits());
        s.add(-0.0);
        assert_eq!(s.value().to_bits(), 0.0f64.to_bits());
    }

    /// Step 3: the chunked helpers (the join probe, a gather, a row loop and
    /// its fallible form, the distinct frontier) give the serial output, in
    /// its order, with 1, 2 and 32 threads, on inputs past the split's limit.
    #[test]
    fn the_pool_helpers_give_one_output_for_every_thread_count() {
        let mut x: u64 = 0x2545_F491_4F6C_DD1D;
        let mut next = move || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        let n = PAR_ROWS_MIN * 3 + 17;
        let lu: Vec<u64> = (0..n).map(|_| next() % 50_000).collect();
        let ru: Vec<u64> = (0..n + 999).map(|_| next() % 50_000).collect();
        let wide: Vec<u64> = (0..n).map(|_| (next() % 3000) << 33).collect();
        // a left side several times smaller than the right (indexed on the left)
        let small: Vec<u64> = (0..2000).map(|_| next() % 50_000).collect();
        let idx: Vec<usize> = (0..n * 2).map(|_| (next() % n as u64) as usize).collect();
        let vals: Vec<u32> = (0..n).map(|_| (next() % 200_000) as u32).collect();
        let run = |t: usize| {
            let _g = ThreadsGuard::set(t, PAR_ROWS_MIN);
            let joins: Vec<_> = [JoinKind::Inner, JoinKind::LeftOuter, JoinKind::Anti]
                .into_iter()
                .flat_map(|k| {
                    [
                        match_ids(&lu, &ru, k).expect("budget"),
                        match_ids(&ru, &lu, k).expect("budget"),
                        match_ids(&wide, &lu, k).expect("budget"),
                        match_ids(&small, &ru, k).expect("budget"),
                    ]
                })
                .collect();
            let g = gather(&vals, &idx);
            let rows = par_concat(n, |r| r.filter(|i| vals[*i].is_multiple_of(3)).collect::<Vec<usize>>());
            let failing = par_try_concat(n, |r| {
                let mut out = Vec::new();
                for i in r {
                    if i == n / 2 + 5 || i == n - 3 {
                        return Err(Decline::Refuse(format!("row {i}")));
                    }
                    out.push(i);
                }
                Ok(out)
            });
            let first_err = match failing {
                Err(Decline::Refuse(m)) => m,
                _ => String::from("no error"),
            };
            let mut order: Vec<usize> = (0..n).collect();
            par_sort_by(&mut order, &|a: &usize, b: &usize| vals[*a].cmp(&vals[*b]).then(a.cmp(b)));
            (joins, g, rows, first_err, sorted_distinct(vals.clone()), order)
        };
        let one = run(1);
        let mut sorted = vals.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(one.4, sorted);
        assert_eq!(one.3, format!("row {}", n / 2 + 5));
        for t in [2, 32] {
            let got = run(t);
            assert!(got.0 == one.0, "{t} threads: a join's rows differ");
            assert!(got.1 == one.1 && got.2 == one.2, "{t} threads: a gather or row loop differs");
            assert_eq!(got.3, one.3, "{t} threads: another first error");
            assert_eq!(got.4, one.4);
            assert!(got.5 == one.5, "{t} threads: another order");
        }
        let mut want: Vec<usize> = (0..n).collect();
        want.sort_by_key(|&i| (vals[i], i));
        assert!(one.5 == want);
    }

    #[test]
    fn identity_join_rows_are_the_nested_loops_rows() {
        // a small deterministic generator: keys from a narrow range (many
        // repeats) and from a wide one (the map form), every join kind, the
        // smaller side on either hand
        let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut next = || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        for case in 0..400 {
            let (nl, nr) = ((next() % 40) as usize, (next() % 40) as usize);
            let wide = case % 3 == 0;
            let mut key = || if wide { next() % 3 + (1 << 40) * (next() % 2) } else { next() % 7 };
            let lu: Vec<u64> = (0..nl).map(|_| key()).collect();
            let ru: Vec<u64> = (0..nr).map(|_| key()).collect();
            for kind in [JoinKind::Inner, JoinKind::LeftOuter, JoinKind::Anti] {
                let mut want_l = Vec::new();
                let mut want_r = Vec::new();
                for (i, a) in lu.iter().enumerate() {
                    let js: Vec<usize> = ru.iter().enumerate().filter(|(_, b)| *b == a).map(|(j, _)| j).collect();
                    match kind {
                        JoinKind::Inner => js.iter().for_each(|&j| {
                            want_l.push(i);
                            want_r.push(Some(j));
                        }),
                        JoinKind::LeftOuter if js.is_empty() => {
                            want_l.push(i);
                            want_r.push(None);
                        }
                        JoinKind::LeftOuter => js.iter().for_each(|&j| {
                            want_l.push(i);
                            want_r.push(Some(j));
                        }),
                        JoinKind::Anti => {
                            if js.is_empty() {
                                want_l.push(i);
                            }
                        }
                    }
                }
                let (got_l, got_r) = match_ids(&lu, &ru, kind).expect("within the budget");
                assert_eq!(got_l, want_l, "case {case} {kind:?}");
                if kind != JoinKind::Anti {
                    assert_eq!(got_r, want_r, "case {case} {kind:?}");
                }
            }
        }
    }

    #[test]
    fn exact_sum_is_order_free() {
        let xs: Vec<f64> = (0..1000).map(|i| ((i * 7919) % 1000) as f64 * 0.01 + 1e-3).collect();
        let mut a = ExactSum::default();
        let mut b = ExactSum::default();
        for x in &xs {
            a.add(*x);
        }
        for x in xs.iter().rev() {
            b.add(*x);
        }
        assert_eq!(a.value(), b.value());
    }

    #[test]
    fn blessed_mean_is_exact_or_rounded_once() {
        assert_eq!(blessed_mean(10, 4).unwrap(), 2.5);
        assert_eq!(blessed_mean(9, 3).unwrap(), 3.0);
        // float(q) + r/den: 1/3 is rounded once
        assert_eq!(blessed_mean(1, 3).unwrap(), 1.0f64 / 3.0);
        assert_eq!(blessed_mean(-7, 2).unwrap(), -3.5);
    }

    #[test]
    fn canonical_decimals_only() {
        assert_eq!(canonical_decimal("123"), Some(123));
        assert_eq!(canonical_decimal("-5"), Some(-5));
        assert_eq!(canonical_decimal("0"), Some(0));
        for bad in ["", "-", "007", "-0", "1e3", " 1", "Account:3"] {
            assert_eq!(canonical_decimal(bad), None, "{bad}");
        }
    }

    #[test]
    fn float_keys_follow_python_equality() {
        assert_eq!(float_key(3.0), KeyVal::Int(3));
        assert_ne!(float_key(3.5), KeyVal::Int(3));
    }

    #[test]
    fn plans_outside_the_forms_are_declined_not_refused() {
        for bad in [
            "{}",
            r#"{"format": 2}"#,
            r#"{"format": 1, "sigma": {"t_v": [[0, 10]], "t_b": 5}, "nodes": [{"op": "PatternMatch", "inputs": []}], "root": 0, "columns": []}"#,
            r#"{"format": 1, "sigma": {"t_v": [[0, 10]], "t_b": 5}, "nodes": [{"op": "Filter", "inputs": [], "pred": {"cast": {"lit": 1}, "to": "str"}}], "root": 0, "columns": []}"#,
        ] {
            assert!(matches!(parse_plan(bad), Err(Decline::Unsupported(_))), "{bad}");
        }
    }

    #[test]
    fn civil_dates_match_known_instants() {
        let us = |s: i64| s * 1_000_000;
        assert_eq!(civil(0), (1970, 1, 1));
        assert_eq!(civil(-1), (1969, 12, 31));
        assert_eq!(civil(us(951_782_400)), (2000, 2, 29));
        assert_eq!(civil(us(1_709_164_800)), (2024, 2, 29));
        assert_eq!(civil(us(4_107_542_400) - 1), (2100, 2, 28));
        assert_eq!(civil(us(-62_135_596_800)), (1, 1, 1));
    }

    #[test]
    fn like_matches_sql_wildcards() {
        let m = |t: &str, p: &str| like(&t.chars().collect::<Vec<_>>(), &p.chars().collect::<Vec<_>>());
        assert!(m("Album x", "A%_x"));
        assert!(!m("Wall y", "A%"));
        assert!(m("", "%"));
        assert!(!m("", "_"));
        assert!(m("a.c", "a.c") && !m("abc", "a.c"));
        assert!(m("héllo", "h_llo"));
        assert!(m("aaab", "%a%b") && !m("aaab", "%a%c"));
    }

    #[test]
    fn the_blessed_quotient_forms_q_plus_r_over_d() {
        assert_eq!(blessed_quotient(6, 3), Some(2.0));
        assert_eq!(blessed_quotient(1, -2), Some(-0.5));
        assert_eq!(blessed_quotient(-7, 2), Some(-3.5));
        assert_eq!(blessed_quotient(1, 3), Some(1.0 / 3.0));
        assert_eq!(blessed_quotient(i64::MIN, -1), None);
        assert_eq!(py_floor_div(7.0, -2.0), -4.0);
        assert_eq!(py_floor_div(-7.5, 2.0), -4.0);
        assert!(py_floor_div(0.0, -3.0).is_sign_negative());
    }
}
