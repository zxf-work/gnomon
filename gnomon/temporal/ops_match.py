"""`match` — typed pattern matching with a fused group/order/limit tail.

The operator's argument is a typed pattern (node variables with labels or
uids, steps of one hop, k hops or a bounded/unbounded closure, cross-variable
predicates, optional and negated sub-patterns), a return clause (a projection,
or one or two aggregation stages, then an order and a semantic limit), and a
scope Σ = (valid instant or window, `as_of_tt`, belief mode). The schema below
(`ARGS_SCHEMA`, `validate`) is the contract.

**One logical plan, two executors.** `compile_match` lowers the argument to a
plan of the plan IR (`gnomon/plan/node.py`). The engine's plan executor runs that
plan by default; the IR's reference evaluator (`evaluate_core`) runs the very
same plan when
the handle was opened with `GNOMON_MATCH_KERNEL=python`, on other backends,
inside an open write batch, or when the engine declines the plan. Because both
consume one plan, an answer difference between them can only be an execution
difference.

The operator is registered in `algebra.REGISTRY` for the engine: it runs
through `call_operator` as an opaque leaf like the other operators, and is
also callable as `match(adapter, args)`. It is **not** in the base operator
set (`algebra.BASE_OPERATORS`); it is an engine-level operator
(docs/operators.md).

Semantics the compiler fixes, beyond the plan IR's own rules:

- under a valid instant with current belief, every node variable binds the one
  version valid at the instant and believed at `as_of_tt`; a node with no such
  version does not bind (the snapshot graph `G(t)`);
- under a window, or any belief mode other than `current`, variables bind
  **versions**, joined on identity (non-sequenced);
  belief mode other than `current`), walks and `optional`/`not` sub-patterns
  share one valid instant with the binding: the answer is the union over the
  instants of the instant answers, one row per version tuple;
- no two single-hop edges of one pattern bind the same edge identity; node
  variables are not implicitly distinct;
- `{min: a, max: b}` keeps each target once, at its shortest distance `d`, iff
  `a <= d <= b`;
- the order is completed with every output column and every bound version id,
  so it is total and both executors cut a `limit` identically.
"""

from __future__ import annotations

import json
import os
import re
import weakref
from dataclasses import dataclass, field
from typing import Any

from gnomon.core.errors import InvalidArgError
from gnomon.core.model import OPEN_END
from gnomon.plan.expr import (
    BoolOp, Cmp, Coalesce, Col, Expr, If, IsNull, Lit, Not, TupleExpr,
)
from gnomon.plan.loader import _expr as _plan_expr
from gnomon.plan.node import (
    Agg, Aggregate, Bounded, Collapse, CoValidExpand, CoValidJoin, EdgeScan, Exact, Expand,
    Filter, Join, Limit, Node, NodeScan, Order, OrderedReach, Project, SortKey, Succ, TopK,
    TypeConstraint, Unbounded, Union, Walk,
)
from gnomon.plan import regex as regex_module
from gnomon.plan.types import NODE_COLUMNS, Sigma

MAX_NODE_VARS = 12
MAX_STEPS = 16
#: A node variable's `uids` list. The corpus binds lists computed by an
#: earlier call (a probe: BI11's persons of a country, BI4's top forums), so
#: the bound follows the data rather than a typed literal: SNB SF10's largest
#: country has more than 10,000 persons. Each uid is a seed, read by
#: identity; the bound only stops a runaway argument.
MAX_UIDS = 1_000_000
DEFAULT_LIMIT = 100
#: A page's rows. Every page runs the whole plan again (the cursor is an
#: offset), so a caller reading a large answer pays the plan once a page:
#: TPC-BiH R7's two belief-all branches at SNB SF1 are 265k and 62k rows, 34 pages of
#: ~0.29 s each at 10,000 a page, and at SF10 past the 120 s cap. A page
#: may hold a whole answer of the size an inline pattern may (MAX_PATTERN_ROWS).
MAX_LIMIT = 1_000_000

#: Columns a pattern variable exposes, by kind.
NODE_FIELDS = frozenset({"uid", "vid", "label", "vt_s", "vt_e", "tt_s", "tt_e", "props"})
EDGE_FIELDS = frozenset({"eid", "vid", "src", "dst", "rel_type", "disc", "vt_s",
                         "vt_e", "tt_s", "tt_e", "props"})
AGG_FNS = ("count", "count_distinct", "sum", "min", "max", "mean", "collect")
_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,63}$")

_EXPR = {"type": "object"}
_ORDER_KEYS = {"type": "array", "items": {
    "type": "object", "additionalProperties": False, "required": ["expr"],
    "properties": {"expr": _EXPR, "dir": {"enum": ["asc", "desc"]},
                   "nulls": {"enum": ["first", "last"]}}}}
_SUB = {
    "type": "object", "additionalProperties": False,
    "properties": {
        "nodes": {"type": "array", "items": {"$ref": "#/$defs/node"}},
        "steps": {"type": "array", "items": {"$ref": "#/$defs/step"}},
        "where": {"type": "array", "items": _EXPR},
    },
}
_GROUP = {
    "type": "object", "additionalProperties": False, "required": ["by", "aggs"],
    "properties": {
        "by": {"type": "array", "items": {"$ref": "#/$defs/binding"}},
        "aggs": {"type": "array", "items": {
            "type": "object", "additionalProperties": False, "required": ["as", "fn"],
            "properties": {
                "as": {"type": "string"},
                "fn": {"enum": list(AGG_FNS)},
                "of": {"anyOf": [_EXPR, {"type": "array", "items": _EXPR, "minItems": 1}]},
                "distinct_on": {"type": "array", "items": {"type": "string"},
                                "minItems": 1},
                "where": _EXPR,
                "order_by": _ORDER_KEYS,
                "limit": {"type": "integer", "minimum": 1},
                "distinct": {"type": "boolean"},
            }}},
        "project": {"type": "array", "items": {"$ref": "#/$defs/binding"}},
        "having": _EXPR,
        "then": {"$ref": "#/$defs/group"},
    },
}

#: The argument schema. Defaults are filled by `validate`.
ARGS_SCHEMA: dict[str, Any] = {
    "type": "object", "additionalProperties": False,
    "required": ["return", "scope"],
    "$defs": {
        "node": {"type": "object", "additionalProperties": False, "required": ["var"],
                 "properties": {
                     "var": {"type": "string"},
                     "labels": {"type": "array", "items": {"type": "string"},
                                "minItems": 1},
                     "uids": {"type": "array", "items": {"type": "string"},
                              "minItems": 1, "maxItems": MAX_UIDS}}},
        "step": {"type": "object", "additionalProperties": False,
                 "required": ["from", "to"],
                 "properties": {
                     "var": {"type": "string"},
                     "from": {"type": "string"}, "to": {"type": "string"},
                     "rel_type": {"anyOf": [
                         {"type": ["string", "null"]},
                         {"type": "array", "items": {"type": "string"}, "minItems": 1}]},
                     "dir": {"enum": ["out", "in", "both"]},
                     "hops": {"type": "object"},
                     "regex": {"type": "object"},
                     "order": {"enum": ["strict_increasing", "increasing",
                                        "strict_decreasing", "decreasing"]},
                     "edge_where": {"type": "object", "additionalProperties": False,
                                    "required": ["var", "where"],
                                    "properties": {"var": {"type": "string"},
                                                   "where": {"type": "array", "items": _EXPR}}},
                     "truncate": {"type": "object", "additionalProperties": False,
                                  "required": ["limit", "order_by"],
                                  "properties": {"limit": {"type": "integer", "minimum": 1},
                                                 "order_by": {**_ORDER_KEYS, "minItems": 1}}}}},
        "binding": {"type": "object", "additionalProperties": False,
                    "required": ["as", "expr"],
                    "properties": {"as": {"type": "string"}, "expr": _EXPR}},
        "group": _GROUP,
        "pattern": {
            "type": "object", "additionalProperties": False,
            "required": ["nodes"],
            "properties": {
                "nodes": {"type": "array", "items": {"$ref": "#/$defs/node"},
                          "minItems": 1, "maxItems": MAX_NODE_VARS},
                "steps": {"type": "array", "items": {"$ref": "#/$defs/step"},
                          "maxItems": MAX_STEPS},
                "where": {"type": "array", "items": _EXPR},
                "optional": {"type": "array", "items": _SUB},
                "not": {"type": "array", "items": _SUB},
                "lookups": {"type": "array", "maxItems": 8, "items": {
                    "type": "object", "additionalProperties": False,
                    "required": ["as", "on", "key", "match"],
                    "properties": {
                        "as": {"type": "string"}, "on": _EXPR, "key": {"type": "string"},
                        "join": {"enum": ["left", "inner"]},
                        "match": {"type": "object", "additionalProperties": False,
                                  "required": ["pattern", "group"],
                                  "properties": {"pattern": {"$ref": "#/$defs/pattern"},
                                                 "group": {"$ref": "#/$defs/group"}}}}}},
                "succ": {"type": "array", "items": {
                    "type": "object", "additionalProperties": False,
                    "required": ["var", "of"],
                    "properties": {"var": {"type": "string"}, "of": {"type": "string"},
                                   "axis": {"enum": ["valid", "transaction"]},
                                   "dir": {"enum": ["next", "prev"]}}}},
            }},
    },
    "properties": {
        "pattern": {"$ref": "#/$defs/pattern"},
        "union": {"type": "array", "minItems": 2, "maxItems": 8, "items": {
            "type": "object", "additionalProperties": False,
            "required": ["pattern", "rows"],
            "properties": {
                "pattern": {"$ref": "#/$defs/pattern"},
                "rows": {"type": "array", "items": {"$ref": "#/$defs/binding"},
                         "minItems": 1}}}},
        "return": {
            "type": "object", "additionalProperties": False,
            "properties": {
                "rows": {"type": "array", "items": {"$ref": "#/$defs/binding"},
                         "minItems": 1},
                "group": {"$ref": "#/$defs/group"},
                "order_by": _ORDER_KEYS,
                "limit": {"type": "integer", "minimum": 1},
                "top_per": {"type": "object", "additionalProperties": False,
                            "required": ["by", "order_by", "k"],
                            "properties": {
                                "by": {"type": "array", "items": _EXPR, "minItems": 1},
                                "order_by": {**_ORDER_KEYS, "minItems": 1},
                                "k": {"type": "integer", "minimum": 1}}},
            }},
        "scope": {
            "type": "object", "additionalProperties": False, "required": ["vt"],
            "properties": {
                "vt": {"type": "object"},
                "as_of_tt": {"type": "integer", "minimum": 0, "maximum": OPEN_END},
                "belief": {"enum": ["current", "superseded", "all"]},
                "co_valid": {"type": "array", "items": {"enum": ["valid", "transaction"]},
                             "uniqueItems": True, "minItems": 1},
            }},
        "params": {"type": "object"},
        "limit": {"type": "integer", "minimum": 1, "maximum": MAX_LIMIT},
        "cursor": {"type": ["string", "null"]},
    },
}


# ---------------------------------------------------------------------------
# validation
# ---------------------------------------------------------------------------

def _bind_params(value: Any, params: dict[str, Any]) -> Any:
    """`"$name"` anywhere in the argument becomes `params[name]`."""
    if isinstance(value, str) and value.startswith("$") and value[1:] in params:
        return params[value[1:]]
    if isinstance(value, dict):
        return {k: _bind_params(v, params) for k, v in value.items()}
    if isinstance(value, list):
        return [_bind_params(v, params) for v in value]
    return value


_VALIDATOR: list[Any] = []


def _validator() -> Any:
    """The compiled schema validator, built once: `jsonschema.validate`
    re-checks the schema and builds a validator on every call, which cost
    more than executing a small plan."""
    if not _VALIDATOR:
        import jsonschema

        cls = jsonschema.validators.validator_for(ARGS_SCHEMA)
        cls.check_schema(ARGS_SCHEMA)
        _VALIDATOR.append(cls(ARGS_SCHEMA))
    return _VALIDATOR[0]


#: `validate`'s filled arguments by (the compiled schema, the argument's
#: canonical text): the registry's validator and the operator itself both
#: validate one call, and the schema walk is most of a small call's Python.
#: Validation reads nothing else (the parameters bind from the argument, the
#: defaults are module constants). Only successes are kept: an invalid
#: argument is checked, and fails, every time. Callers get a copy.
_VALIDATED: dict[tuple[int, str], dict[str, Any]] = {}
_VALIDATED_MAX = 4096


def validate(args: dict[str, Any]) -> dict[str, Any]:
    """Schema check, parameter binding, defaults, then the structural rules
    the schema cannot state. Returns the filled argument."""
    try:
        # the compiled validator's identity stands for the schema version: a
        # rebuilt validator (a new schema) never meets an old entry
        key: tuple[int, str] | None = (id(_validator()), json.dumps(args, sort_keys=True))
    except (TypeError, ValueError):
        key = None
    if key is not None:
        hit = _VALIDATED.get(key)
        if hit is not None:
            return json.loads(json.dumps(hit))
    filled = _validate(args)
    if key is not None:
        if len(_VALIDATED) >= _VALIDATED_MAX:
            _VALIDATED.clear()
        _VALIDATED[key] = json.loads(json.dumps(filled))
    return filled


def _validate(args: dict[str, Any]) -> dict[str, Any]:
    import jsonschema

    if not isinstance(args, dict):
        raise InvalidArgError("match: the argument is an object")
    params = args.get("params") or {}
    if not isinstance(params, dict):
        raise InvalidArgError("match: params is an object")
    filled = {k: _bind_params(v, params) for k, v in args.items() if k != "params"}
    error = jsonschema.exceptions.best_match(_validator().iter_errors(filled))
    if error is not None:
        raise InvalidArgError(f"match: {error.message}",
                              path="/".join(str(p) for p in error.absolute_path))
    filled.setdefault("limit", DEFAULT_LIMIT)
    filled.setdefault("cursor", None)
    scope = dict(filled["scope"])
    scope.setdefault("as_of_tt", OPEN_END)
    scope.setdefault("belief", "current")
    filled["scope"] = scope
    if ("pattern" in filled) == ("union" in filled):
        raise InvalidArgError("match: the argument takes exactly one of `pattern` or `union`")
    ret = filled["return"]
    if ("rows" in ret) == ("group" in ret):
        raise InvalidArgError("match: return takes exactly one of `rows` or `group`")
    _sigma_of(scope)                                   # validates the vt shape
    return filled


def _sigma_of(scope: dict[str, Any]) -> tuple[Sigma, str]:
    vt = scope["vt"]
    as_of = int(scope.get("as_of_tt", OPEN_END))
    if set(vt) == {"instant"}:
        t = int(vt["instant"])
        if not 0 <= t < OPEN_END:
            raise InvalidArgError("match: scope.vt.instant out of range")
        return Sigma.at_instant(t, as_of_tt=as_of), "overlap"
    if "window" in vt and set(vt) <= {"window", "mode"}:
        w = vt["window"]
        t_a, t_b = int(w["t_a"]), int(w["t_b"])
        if not 0 <= t_a < t_b <= OPEN_END:
            raise InvalidArgError("match: scope.vt.window needs 0 <= t_a < t_b <= OPEN_END")
        mode = vt.get("mode", "overlap")
        if mode not in ("overlap", "event"):
            raise InvalidArgError("match: window mode is overlap or event")
        return Sigma.in_window(t_a, t_b, as_of_tt=as_of), mode
    raise InvalidArgError("match: scope.vt is {instant: t} or {window: {t_a, t_b}, mode}")


# ---------------------------------------------------------------------------
# compilation
# ---------------------------------------------------------------------------

@dataclass
class Compiled:
    """The lowered plan and what the payload needs around it."""

    root: Node
    columns: list[str]
    sigma: Sigma
    #: the plan as the engine's executor reads it (`plan_json`)
    doc: dict[str, Any] = field(default_factory=dict)


@dataclass
class _Ctx:
    sigma: Sigma
    vt_mode: str
    belief: str
    #: variable -> "node" | "edge" | "target" (variable-length target)
    kinds: dict[str, str] = field(default_factory=dict)
    #: variable -> the uid column it is bound through (a shared variable in a
    #: sub-pattern is bound through its key column)
    uid_col: dict[str, str] = field(default_factory=dict)
    #: version-id columns of every bound variable, in binding order
    vid_cols: list[str] = field(default_factory=list)
    fresh: int = 0
    #: `co_valid` columns available once the common interval is projected
    sequenced: set[str] = field(default_factory=set)
    #: `<lookup>.<output>` columns, once the lookups are joined
    lookup_cols: set[str] = field(default_factory=set)
    #: lookup name -> the pattern variables its `on` reads (a lookup's
    #: outputs are a function of them, its key being unique)
    lookup_on: dict[str, set[str]] = field(default_factory=dict)
    #: the scope's `co_valid` axes
    axes: tuple[str, ...] = ()
    walks: list[tuple[str, str]] = field(default_factory=list)
    depth_cols: list[str] = field(default_factory=list)
    #: shared variables a sub-pattern reads through key columns only
    key_vars: set[str] = field(default_factory=set)
    #: the orientation column of each `dir: both` hop (a self-loop read both ways
    #: is two bindings, as `Expand` counts it)
    orient_cols: list[str] = field(default_factory=list)
    fragmented: bool = False
    seq_iv: tuple[str, str] = ("valid.s", "valid.e")
    #: label and relationship counts for the step order (`plan_order`)
    card: dict[str, Any] | None = None
    #: pattern binds, and what it already covers
    run_iv: tuple[str, str] | None = None
    carried: set[str] = field(default_factory=set)

    def name(self, stem: str) -> str:
        self.fresh += 1
        return f"_{stem}{self.fresh}"

    @property
    def instant(self) -> bool:
        return len(self.sigma.t_v) == 1 and \
            self.sigma.t_v[0].end - self.sigma.t_v[0].start == 1

    @property
    def expand_path(self) -> bool:
        """Instant scope with current belief compiles through `Expand`; every
        other scope binds versions through scans and identity joins."""
        return self.belief == "current" and self.vt_mode == "overlap" and self.instant

    @property
    def shared_instant(self) -> bool:
        """        sub-patterns: a window in overlap mode with `co_valid: [valid]`, or an
        instant with a belief mode other than current."""
        return not self.expand_path and self.vt_mode == "overlap" and \
            "transaction" not in self.axes and (self.instant or "valid" in self.axes)

    def shared_instant_refusal(self, what: str) -> str | None:
        """Why `what` (a walk, a sub-pattern) is refused here, or None."""
        if self.expand_path or self.shared_instant:
            return None
        if self.vt_mode == "event":
            return f"match: {what} is not defined under a window in event mode"
        if "transaction" in self.axes:
            return f"match: {what} under co_valid: transaction is not supported"
        return (f"match: {what} under a window needs scope.co_valid: [\"valid\"] "
                "(the shared-instant rule: every node and edge of the binding, walks included, valid at "
                "one shared instant)")


def _var_of(column: str) -> str:
    return column.split(".", 1)[0]


def _rewrite(spec: Any, ctx: _Ctx | None, allowed: set[str] | None) -> Any:
    """`match` expression JSON -> plan IR expression JSON: `prop`/`tprop` name a
    variable and become `<var>.props`; `col` references are checked against
    the bound variables (or, in a group stage, the stage's columns)."""
    if isinstance(spec, list):
        return [_rewrite(s, ctx, allowed) for s in spec]
    if not isinstance(spec, dict):
        return spec
    out: dict[str, Any] = {}
    for k, v in spec.items():
        if k in ("prop", "tprop"):
            var, key = v
            if ctx is not None and var not in ctx.kinds:
                raise InvalidArgError(f"match: property of an unbound variable {var!r}")
            out[k] = [f"{var}.props", key]
        elif k == "col":
            _check_col(v, ctx, allowed)
            out[k] = (ctx.uid_col.get(_var_of(v), v)
                      if ctx is not None and v.endswith(".uid") else v)
        elif k == "lit" or k == "tau" or k == "to":
            out[k] = v
        else:
            out[k] = _rewrite(v, ctx, allowed)
    return out


#: The common interval of every bound version under `co_valid`, per axis.
SEQUENCED_COLS = {"valid": ("valid.s", "valid.e"), "transaction": ("belief.s", "belief.e")}


def _check_col(name: str, ctx: _Ctx | None, allowed: set[str] | None) -> None:
    if allowed is not None and name in allowed:
        return
    if ctx is not None and ctx.fragmented and \
            name in {c for pair in SEQUENCED_COLS.values() for c in pair}:
        raise InvalidArgError(
            f"match: {name} is not defined when a walk or a sub-pattern can split a "
            "binding's valid set into several intervals (the shared-instant rule)")
    if ctx is not None and (name in ctx.sequenced or name in ctx.lookup_cols):
        return
    if ctx is None or "." not in name:
        if allowed is not None:
            raise InvalidArgError(f"match: unknown column {name!r}", known=sorted(allowed))
        raise InvalidArgError(f"match: column {name!r} names no variable")
    var, fld = name.split(".", 1)
    kind = ctx.kinds.get(var)
    if kind is None:
        raise InvalidArgError(f"match: column of an unbound variable: {name!r}")
    ok = (NODE_FIELDS if kind == "node" else
          EDGE_FIELDS if kind == "edge" else NODE_FIELDS | {"depth"})
    if fld not in ok:
        raise InvalidArgError(f"match: {var!r} has no column {fld!r}", allowed=sorted(ok))


def _expr(spec: Any, ctx: _Ctx | None, allowed: set[str] | None = None) -> Expr:
    return _plan_expr(_rewrite(spec, ctx, allowed))


def _vars_in(spec: Any) -> set[str]:
    """Pattern variables an M expression reads."""
    found: set[str] = set()
    if isinstance(spec, dict):
        for k, v in spec.items():
            if k in ("prop", "tprop"):
                found.add(v[0])
            elif k == "col" and isinstance(v, str) and "." in v:
                found.add(_var_of(v))
            elif k not in ("lit",):
                found |= _vars_in(v)
    elif isinstance(spec, list):
        for v in spec:
            found |= _vars_in(v)
    return found


def _hops(spec: dict[str, Any] | None):
    if spec is None or spec == {}:
        return Exact(1)
    if set(spec) == {"exact"}:
        k = int(spec["exact"])
        if k < 1:
            raise InvalidArgError("match: hops.exact must be >= 1 (use min: 0 for zero-length)")
        return Exact(k)
    if "min" in spec and set(spec) <= {"min", "max"}:
        a, b = int(spec["min"]), spec.get("max")
        if b is None:
            if a not in (0, 1):
                raise InvalidArgError("match: an unbounded step starts at 0 or 1 hops")
            return Unbounded(a)
        return Bounded(a, int(b))
    raise InvalidArgError("match: hops is {exact: k} or {min: a, max: b|null}")


_REVERSE = {"out": "in", "in": "out", "both": "both"}


class _Builder:
    """Lowers one pattern (the main one, or a sub-pattern over a key
    relation) onto a plan IR relation."""

    def __init__(self, ctx: _Ctx, rel: Node | None,
                 keep_only: set[str] | None = None,
                 qual: dict[str, Node] | None = None) -> None:
        self.ctx = ctx
        self.rel = rel
        self.single_edges: list[str] = []
        self.pending: list[tuple[set[str], Any]] = []
        #: set semantics over these variables' identities (see `_project_live`)
        self.keep_only = keep_only
        #: per variable, the nodes it may bind (a semijoin pass, `_tree_reduction`),
        #: joined as soon as it binds
        self.qual = qual or {}
        #: lookups not yet joined: each joins once its `on` is bound
        self.lookups: list[dict[str, Any]] = []
        #: shared variables of a decorrelated `not` sub-pattern (`_decorrelation`):
        #: each is the far end of one single hop and binds as that hop's endpoint
        #: uid only, exactly as a key variable (no node version, no interval)
        self.proxies: set[str] = set()

    def _bind_proxy(self, var: str, uid_col: str) -> None:
        self.ctx.kinds[var] = "node"
        self.ctx.uid_col[var] = uid_col
        self.ctx.key_vars.add(var)

    # -- helpers -----------------------------------------------------------
    def _bound(self, var: str) -> bool:
        return var in self.ctx.kinds

    def _scan(self, node: dict[str, Any]) -> Node:
        c = self.ctx
        return NodeScan(node["var"], labels=tuple(node["labels"]) if node.get("labels") else None,
                        uids=tuple(node["uids"]) if node.get("uids") else None,
                        belief=c.belief, vt_mode=c.vt_mode, sigma_=c.sigma)

    def _bind_node(self, var: str) -> None:
        self.ctx.kinds[var] = "node"
        self.ctx.uid_col.setdefault(var, f"{var}.uid")
        self.ctx.vid_cols.append(f"{var}.vid")

    def _qualify(self, var: str) -> None:
        q = self.qual.pop(var, None)
        if q is not None:
            self.rel = Join(self.rel, q, ((self.ctx.uid_col[var], q.group_by[0][0]),),
                            "inner", sigma_=self.ctx.sigma)

    def _filter(self, pred: Expr) -> None:
        self.rel = Filter(self.rel, pred, sigma_=self.ctx.sigma)

    def _constrain(self, node: dict[str, Any] | None, var: str) -> None:
        """Node constraints on a variable reached by a step."""
        c = self.ctx
        if node and node.get("labels"):
            self.rel = TypeConstraint(self.rel, var, labels=tuple(node["labels"]),
                                      sigma_=c.sigma)
        elif c.expand_path:
            # the snapshot graph: a target without a version under Σ does not bind
            self._filter(Not(IsNull(Col(f"{var}.vid"))))
        if node and node.get("uids"):
            probe = NodeScan(c.name("u"), uids=tuple(node["uids"]), belief=c.belief,
                             vt_mode=c.vt_mode, sigma_=c.sigma)
            if c.expand_path:
                key = Aggregate(probe, group_by=((c.name("k"), Col(f"{probe.as_}.uid")),),
                                sigma_=c.sigma)
                self.rel = Join(self.rel, key,
                                ((f"{var}.uid", key.group_by[0][0]),), "inner",
                                sigma_=c.sigma)
            else:
                self._filter(_uid_in(f"{var}.uid", node["uids"]))

    def _join_lookups(self) -> None:
        """Join every lookup whose `on` reads only bound variables; its name
        then binds, so predicates on its outputs apply at the next flush."""
        c = self.ctx
        left = []
        for lk in self.lookups:
            if _vars_in(lk["on"]) <= {v for v, k in c.kinds.items() if k != "lookup"}:
                self.rel = _lookup(c, self.rel, lk)
                c.kinds[lk["as"]] = "lookup"
                c.lookup_on[lk["as"]] = _vars_in(lk["on"])
            else:
                left.append(lk)
        self.lookups = left

    def _flush(self, join: bool = True) -> None:
        """Apply every pending predicate whose variables are now all bound
        (after joining the lookups that can be, unless `join` is false)."""
        if join and self.lookups:
            self._join_lookups()
        keep = []
        for needs, pred in self.pending:
            if needs <= set(self.ctx.kinds):
                self._filter(_expr(pred, self.ctx))
            else:
                keep.append((needs, pred))
        self.pending = keep

    def _distinct_edges(self, var: str) -> None:
        for other in self.single_edges:
            self._filter(Cmp("!=", Col(f"{other}.eid"), Col(f"{var}.eid")))
        self.single_edges.append(var)

    # -- one step ----------------------------------------------------------
    def step(self, i: int, st: dict[str, Any], nodes: dict[str, dict]) -> None:
        c = self.ctx
        hops = _hops(st.get("hops")) if "regex" not in st else None
        single = isinstance(hops, Exact) and hops.k == 1
        ordered = "regex" not in st and ("order" in st or "edge_where" in st
                                         or ("truncate" in st and not single))
        before = list(c.vid_cols)
        bound_end = st["from"] if self._bound(st["from"]) else st["to"]
        if ordered:
            if not c.expand_path:
                raise InvalidArgError("match: an ordered or filtered variable-length step needs "
                                      "an instant scope with current belief in this version")
            self._step_ordered(st, nodes)
        elif c.expand_path:
            self._step_expand(i, st, nodes)
        else:
            self._step_join(i, st, nodes)
        for v in (st["from"], st["to"]):
            if v in self.qual and self._bound(v):
                self._qualify(v)
        # a truncation cuts after the predicates on the pattern's variables,
        # and before any lookup's
        self._flush(join="truncate" not in st)
        if "truncate" in st:
            if not ordered:
                self._truncate_hop(st, bound_end, before)
            self._flush()
        self._carry()

    def _carry(self) -> None:
        """        interval by every version and walk fragment bound since the last
        step, and drop the rows where it is empty, so no combination that
        cannot share an instant is carried into the next step."""
        c = self.ctx
        if not c.fragmented:
            return
        new = [v for v, k in c.kinds.items()
               if k in ("node", "edge", "target") and not v.startswith(("_c", "_u", "_k"))
               and v not in c.key_vars and v not in c.carried]
        new_walks = [w for w in c.walks if f"{w[0]}|{w[1]}" not in c.carried]
        if not new and not new_walks:
            return
        ts = NODE_COLUMNS[3].tau
        if c.run_iv is None:
            lo_e: Expr = Lit(min(i.start for i in c.sigma.t_v), ts)
            hi_e: Expr = Lit(max(i.end for i in c.sigma.t_v), ts)
        else:
            lo_e, hi_e = Col(c.run_iv[0]), Col(c.run_iv[1])
        items = [(f"{v}.vt_s", f"{v}.vt_e") for v in new] + list(new_walks)
        for a, b in items:
            # one element per projection: the expression stays two levels deep
            lo, hi = c.name("cs"), c.name("ce")
            self.rel = Project(self.rel, bindings=(
                (lo, If(Cmp(">", Col(a), lo_e), Col(a), lo_e)),
                (hi, If(Cmp("<", Col(b), hi_e), Col(b), hi_e))), keep="all", sigma_=c.sigma)
            lo_e, hi_e = Col(lo), Col(hi)
        self._filter(Cmp("<", lo_e, hi_e))
        c.run_iv = (lo, hi)
        c.carried |= set(new) | {f"{a}|{b}" for a, b in new_walks}

    def _truncate_hop(self, st: dict[str, Any], bound_end: str, before: list[str]) -> None:
        """A single hop's truncation: per binding the step started from (its
        bound end's identity and every version bound before it), the first
        `limit` edges under the keys (the edge's version id last), after the
        predicates that read only what is bound."""
        c = self.ctx
        if not st.get("var"):
            raise InvalidArgError("match: a truncated single hop names its edge variable")
        t = st["truncate"]
        # ties fall to the versions this step bound (the edge's, then the
        # far end's, in binding order), so both executors cut the same rows
        keys = _order_keys(t["order_by"], c, None) + [
            SortKey(Col(v), "asc", "nulls_first") for v in c.vid_cols if v not in before]
        part = (Col(c.uid_col[bound_end]),) + tuple(Col(v) for v in before)
        self.rel = TopK(self.rel, part, tuple(keys), int(t["limit"]), sigma_=c.sigma)

    def _step_ordered(self, st: dict[str, Any], nodes: dict[str, dict]) -> None:
        """A variable-length step over the edges of one relationship type that
        pass `edge_where`, with times in `order` along the step (from `from`
        to `to`) and per-node `truncate`: `OrderedReach` over an explicit
        edge relation. Read from the bound end (the order flips with it)."""
        c = self.ctx
        if st.get("var"):
            raise InvalidArgError("match: a variable-length step binds no edge variable")
        rel_type = st.get("rel_type")
        if not isinstance(rel_type, str):
            raise InvalidArgError("match: an ordered step names one relationship type")
        direction = st.get("dir", "out")
        if direction == "both":
            raise InvalidArgError("match: an ordered step is directed (out or in)")
        hops = _hops(st.get("hops"))
        lo, hi = ((hops.k, hops.k) if isinstance(hops, Exact) else
                  (hops.a, hops.b) if isinstance(hops, Bounded) else (hops.a, None))
        order = st.get("order", "none")
        src, dst = st["from"], st["to"]
        if not self._bound(src):
            src, dst = dst, src
            direction = _REVERSE[direction]
            order = {"none": "none", "strict_increasing": "strict_decreasing",
                     "increasing": "decreasing", "strict_decreasing": "strict_increasing",
                     "decreasing": "increasing"}[order]
        ew = st.get("edge_where") or {"var": c.name("t"), "where": []}
        ev = ew["var"]
        if ("edge_where" in st and not _NAME.match(ev)) or ev in c.kinds:
            raise InvalidArgError(f"match: edge_where names a fresh variable, not {ev!r}")
        scan: Node = EdgeScan(ev, rel_types=(rel_type,), belief=c.belief, vt_mode=c.vt_mode,
                              sigma_=c.sigma)
        c.kinds[ev] = "edge"
        try:
            for w in ew["where"]:
                if _vars_in(w) - {ev}:
                    raise InvalidArgError("match: edge_where reads its edge variable only")
                scan = Filter(scan, _expr(w, c), sigma_=c.sigma)
            ks, kd, kt, kk = (c.name("rs"), c.name("rd"), c.name("rt"), c.name("rk"))
            binds = [(ks, Col(f"{ev}.src")), (kd, Col(f"{ev}.dst")), (kt, Col(f"{ev}.vt_s"))]
            t = st.get("truncate")
            key_desc = False
            if t is not None:
                if len(t["order_by"]) != 1:
                    raise InvalidArgError("match: a variable-length truncation takes one key")
                o = t["order_by"][0]
                binds.append((kk, _expr(o["expr"], c)))
                key_desc = o.get("dir", "asc") == "desc"
        finally:
            del c.kinds[ev]
        edges = Project(scan, bindings=tuple(binds), sigma_=c.sigma)
        a_col, b_col = (ks, kd) if direction == "out" else (kd, ks)
        closing = self._bound(dst)
        into = c.name("c") if closing else dst
        self.rel = OrderedReach(self.rel, edges, c.uid_col[src], into, a_col, b_col, kt,
                                lo=lo, hi=hi, order=order,
                                limit=int(t["limit"]) if t is not None else None,
                                key=kk if t is not None else None, key_desc=key_desc,
                                sigma_=c.sigma)
        if closing:
            self._filter(Cmp("=", Col(f"{into}.uid"), Col(c.uid_col[dst])))
        else:
            c.kinds[dst] = "target"
            c.uid_col[dst] = f"{dst}.uid"
            c.vid_cols.append(f"{dst}.vid")
            self._constrain(nodes.get(dst), dst)

    def _step_walk(self, st: dict[str, Any], nodes: dict[str, dict]) -> None:
        """A regular path step: every node a path spelling the expression
        reaches, once (endpoint semantics), read from the bound end."""
        c = self.ctx
        if st.get("var") or st.get("hops") or st.get("rel_type") is not None or \
                st.get("dir", "out") != "out":
            raise InvalidArgError("match: a regex step takes no var, hops, rel_type or dir; "
                                  "the expression carries the direction of each edge")
        spec = regex_module.canonical(st["regex"])
        src, dst = st["from"], st["to"]
        if not self._bound(src):
            src, dst, spec = dst, src, regex_module.reverse(spec)
        closing = self._bound(dst)
        into = c.name("c") if closing else dst
        self.rel = Walk(self.rel, c.uid_col[src], into, regex_module.text(spec),
                        sigma_=c.sigma)
        if closing:
            self._filter(Cmp("=", Col(f"{into}.uid"), Col(c.uid_col[dst])))
        else:
            c.kinds[dst] = "node"
            c.uid_col[dst] = f"{dst}.uid"
            c.vid_cols.append(f"{dst}.vid")
            self._constrain(nodes.get(dst), dst)

    def _step_expand(self, i: int, st: dict[str, Any], nodes: dict[str, dict]) -> None:
        c = self.ctx
        if "regex" in st:
            self._step_walk(st, nodes)
            return
        src, dst = st["from"], st["to"]
        direction = st.get("dir", "out")
        if not self._bound(src):                    # walk it from the bound end
            src, dst, direction = dst, src, _REVERSE[direction]
        hops = _hops(st.get("hops"))
        single = isinstance(hops, Exact) and hops.k == 1
        edge_var = None
        if single:
            edge_var = st.get("var") or c.name("e")
        elif st.get("var"):
            raise InvalidArgError("match: only a single-hop step binds an edge variable")
        closing = self._bound(dst)
        proxy = not closing and dst in self.proxies
        into = c.name("c") if closing or proxy else dst
        rel_type, alternatives = _rel_choice(st, single)
        self.rel = Expand(self.rel, c.uid_col[src], into, hops,
                          rel_type=rel_type, dir=direction,
                          edge_var=edge_var, sigma_=c.sigma)
        if alternatives:
            # one hop over any of several relationship types: the edge's type
            self._filter(_one_of(f"{edge_var}.rel_type", alternatives))
        if closing:
            self._filter(Cmp("=", Col(f"{into}.uid"), Col(c.uid_col[dst])))
        elif proxy:
            # the closing hop's own expansion, its far end left free: the
            # key join after the sub-pattern is the closing test
            self._bind_proxy(dst, f"{into}.uid")
        else:
            c.kinds[dst] = "node" if not hops.variable_length else "target"
            c.uid_col[dst] = f"{dst}.uid"
            c.vid_cols.append(f"{dst}.vid")
            self._constrain(nodes.get(dst), dst)
        if edge_var is not None:
            c.kinds[edge_var] = "edge"
            c.vid_cols.append(f"{edge_var}.vid")
            self._distinct_edges(edge_var)

    def _step_join(self, i: int, st: dict[str, Any], nodes: dict[str, dict]) -> None:
        c = self.ctx
        if "regex" in st:
            raise InvalidArgError("match: a regex step needs an instant scope with current "
                                  "belief in this version")
        hops = _hops(st.get("hops"))
        if not (isinstance(hops, Exact) and hops.k == 1):
            why = c.shared_instant_refusal("a variable-length or multi-hop step")
            if why is not None:
                raise InvalidArgError(why)
            if st.get("var"):
                raise InvalidArgError("match: only a single-hop step binds an edge variable")
            if isinstance(hops, Exact):
                self._step_unrolled(st, nodes, hops.k)
            else:
                self._step_covalid(st, nodes, hops)
            return
        e = st.get("var") or c.name("e")
        self._hop(st["from"], st["to"], st.get("dir", "out"), e, st.get("rel_type"), nodes,
                  distinct=True)

    def _hop(self, src: str, dst: str, direction: str, e: str, rt: Any,
             nodes: dict[str, dict], distinct: bool) -> None:
        """One hop binding edge `e` through an edge scan joined on identity;
        `dir: both` is the union of the two orientations. A closing hop (both
        ends bound) joins on both ends: joined on one end and filtered on the
        other, the binding table passed through every edge of the bound end
        (213M rows for a KNOWS closing step at SF1)."""
        c = self.ctx
        scan = EdgeScan(e, rel_types=(tuple(rt) if isinstance(rt, list) else (rt,)) if rt else None,
                        belief=c.belief, vt_mode=c.vt_mode, sigma_=c.sigma)
        c.kinds[e] = "edge"
        c.vid_cols.append(f"{e}.vid")
        if direction == "both":
            # each edge in both orientations: the union of two projections of
            # the scan, keyed by the end it is read from (`near`) and the end
            # it leads to (`far`); a self-loop is read twice, as `Expand` does
            near, far, way = c.name("nr"), c.name("fr"), c.name("or")
            orient = [Project(scan, bindings=((near, Col(f"{e}.{a}")), (far, Col(f"{e}.{b}")),
                                              (way, Lit(k))),
                              keep="all", sigma_=c.sigma)
                      for k, (a, b) in enumerate((("src", "dst"), ("dst", "src")))]
            both_ways: Node = Union(tuple(orient), sigma_=c.sigma)
            c.orient_cols.append(way)
            bound_end, other_end = (src, dst) if self._bound(src) else (dst, src)
            on: tuple[tuple[str, str], ...] = ((c.uid_col[bound_end], near),)
            if self._bound(other_end):
                on += ((c.uid_col[other_end], far),)
            self.rel = Join(self.rel, both_ways, on, "inner", sigma_=c.sigma)
            other_col = far
        else:
            tail, head = (src, dst) if direction == "out" else (dst, src)
            bound_end, other_end = (tail, head) if self._bound(tail) else (head, tail)
            end_col = f"{e}.src" if bound_end == tail else f"{e}.dst"
            other_col = f"{e}.dst" if bound_end == tail else f"{e}.src"
            on = ((c.uid_col[bound_end], end_col),)
            if self._bound(other_end):
                on += ((c.uid_col[other_end], other_col),)
            self.rel = Join(self.rel, scan, on, "inner", sigma_=c.sigma)
        if not self._bound(other_end) and other_end in self.proxies:
            # the closing hop joined on its bound end only: the key join after
            # the sub-pattern is the other end's test (`_decorrelation`)
            self._bind_proxy(other_end, other_col)
        elif not self._bound(other_end):
            node = nodes.get(other_end, {"var": other_end})
            self.rel = Join(self.rel, self._scan(node), ((other_col, f"{other_end}.uid"),),
                            "inner", sigma_=c.sigma)
            self._bind_node(other_end)
        if distinct:
            self._distinct_edges(e)

    def _step_unrolled(self, st: dict[str, Any], nodes: dict[str, dict], k: int) -> None:
        """        variables, each binding a version (a walk counted once per version
        tuple of its elements), exempt from the distinct-edge rule."""
        c = self.ctx
        a, b = st["from"], st["to"]
        direction = st.get("dir", "out")
        if not self._bound(a):
            a, b, direction = b, a, _REVERSE[direction]
        rt, _alternatives = _rel_choice(st, False)
        cur = a
        for h in range(k):
            nxt = b if h == k - 1 else c.name("n")
            self._hop(cur, nxt, direction, c.name("e"), rt, nodes, distinct=False)
            cur = nxt

    def _step_covalid(self, st: dict[str, Any], nodes: dict[str, dict], hops: Any) -> None:
        """        fragment joins the binding's common interval."""
        c = self.ctx
        src, dst = st["from"], st["to"]
        direction = st.get("dir", "out")
        if not self._bound(src):
            src, dst, direction = dst, src, _REVERSE[direction]
        rel_type, _alternatives = _rel_choice(st, False)
        closing = self._bound(dst)
        into = c.name("c") if closing else dst
        schema = self.rel.out_schema
        if c.run_iv is not None:
            from_iv: tuple[str, str] | None = c.run_iv
        elif c.uid_col.get(src) == f"{src}.uid" and f"{src}.vt_s" in schema:
            from_iv = (f"{src}.vt_s", f"{src}.vt_e")
        else:
            from_iv = None
        self.rel = CoValidExpand(self.rel, c.uid_col[src], into, hops, rel_type=rel_type,
                                 dir=direction, belief=c.belief, from_iv=from_iv,
                                 sigma_=c.sigma)
        c.walks.append((f"{into}.ps", f"{into}.pe"))
        if closing:
            if c.uid_col.get(dst) == f"{dst}.uid" and f"{dst}.vid" in schema:
                self._filter(Cmp("=", Col(f"{into}.vid"), Col(f"{dst}.vid")))
            else:
                self._filter(Cmp("=", Col(f"{into}.uid"), Col(c.uid_col[dst])))
        else:
            c.kinds[dst] = "target"
            c.uid_col[dst] = f"{dst}.uid"
            c.vid_cols.append(f"{dst}.vid")
            c.depth_cols.append(f"{dst}.depth")
            self._constrain(nodes.get(dst), dst)

    # -- a whole pattern -----------------------------------------------------
    def succ(self, entries: list[dict[str, Any]]) -> None:
        """`succ`: a variable bound to the next (or previous) version of
        another's identity along one time axis, among the versions the
        scope's belief and mode admit."""
        c = self.ctx
        for e in entries:
            of, v = e["of"], e["var"]
            kind = c.kinds.get(of)
            if kind not in ("node", "edge", "target"):
                raise InvalidArgError(f"match: succ of an unbound variable {of!r}")
            if not _NAME.match(v) or v in c.kinds:
                raise InvalidArgError(f"match: succ binds a fresh variable, not {v!r}")
            if of in c.uid_col and c.uid_col[of] != f"{of}.uid":
                raise InvalidArgError("match: succ of a variable read through a key column")
            kind = "edge" if kind == "edge" else "node"
            self.rel = Succ(self.rel, of, v, kind, axis=e.get("axis", "valid"),
                            dir=e.get("dir", "next"), belief=c.belief, vt_mode=c.vt_mode,
                            sigma_=c.sigma)
            c.kinds[v] = kind
            if kind == "node":
                c.uid_col[v] = f"{v}.uid"
            c.vid_cols.append(f"{v}.vid")
            self._flush()

    def pattern(self, nodes: list[dict], steps: list[dict], where: list[Any],
                succ: list[dict[str, Any]] | None = None, anchor_var: str | None = None) -> None:
        by_var = {n["var"]: n for n in nodes}
        for v in by_var:
            if not _NAME.match(v):
                raise InvalidArgError(f"match: bad variable name {v!r}")
        self.pending = [(_vars_in(w), w) for w in where]
        if self.rel is None:
            anchor = by_var[anchor_var] if anchor_var is not None else _anchor(nodes)
            self.rel = self._scan(anchor)
            self._bind_node(anchor["var"])
            self._qualify(anchor["var"])
            self._flush()
            self._carry()
        todo = list(enumerate(steps))
        pairs = self._existential_pairs(by_var, steps, where) if self.keep_only is not None else {}
        held = {i for p in pairs.values() for i in p}
        while todo:
            for v, (i, j) in list(pairs.items()):
                a, b = (_other_end(steps[i], v), _other_end(steps[j], v))
                if self._bound(a) and self._bound(b):
                    del pairs[v]
                    todo = [(k, st) for k, st in todo if k not in (i, j)]
                    self._through(v, steps[i], steps[j], by_var.get(v))
                    self.ctx.kinds[v] = "gone"
                    if todo:
                        self._project_live([s for _, s in todo])
            if not todo:
                break
            for pos, (i, st) in enumerate(todo):
                if i in held and any(i in p for p in pairs.values()):
                    continue
                if self._bound(st["from"]) or self._bound(st["to"]):
                    del todo[pos]
                    self.step(i, st, by_var)
                    if todo and self.keep_only is not None:
                        self._project_live([s for _, s in todo])
                    break
            else:
                if pairs:
                    # a held variable's far end is reachable only through it:
                    # evaluate its two steps as any others
                    pairs, held = {}, set()
                    continue
                raise InvalidArgError("match: the pattern is not connected",
                                      unreached=[s for _, s in todo])
        for v, n in by_var.items():
            if not self._bound(v):
                raise InvalidArgError(f"match: node variable {v!r} is not reached by any step")
        if succ:
            self.succ(succ)
        if self.lookups:
            raise InvalidArgError("match: a lookup's `on` names an unbound variable",
                                  lookups=[lk["as"] for lk in self.lookups])
        if self.pending:
            raise InvalidArgError("match: a predicate names an unbound variable",
                                  vars=sorted(set().union(*(n for n, _ in self.pending))))


    def _existential_pairs(self, by_var: dict[str, dict], steps: list[dict[str, Any]],
                           where: list[Any]) -> dict[str, tuple[int, int]]:
        """Under set semantics, the variables no answer, predicate or other
        step reads that sit between exactly two regex steps: `v` with
        `a - v - b`. They are evaluated apart (`_through`), once `a` and `b`
        are bound."""
        need = set(self.keep_only or ()) | {v for w in where for v in _vars_in(w)}
        incident: dict[str, list[int]] = {}
        for i, st in enumerate(steps):
            if st["from"] == st["to"]:
                incident.setdefault(st["from"], []).extend([i, i])
                continue
            for v in (st["from"], st["to"]):
                incident.setdefault(v, []).append(i)
        out = {}
        anchor = _anchor(list(by_var.values()))["var"] if by_var else None
        for v, ks in incident.items():
            n = by_var.get(v, {})
            if v in need or v == anchor or n.get("uids") or len(ks) != 2 or ks[0] == ks[1]:
                continue
            if any(set(p) & set(ks) for p in out.values()):
                continue                               # one at a time along a chain
            out[v] = (ks[0], ks[1])
        return out

    def _through(self, v: str, s1: dict[str, Any], s2: dict[str, Any],
                 node: dict | None) -> None:
        """`a -R1- v -R2- b` with `v` existential, as the distinct pairs
        `(a, b)` some binding of `v` connects: from the distinct bound `a`,
        walk `R1` to `v` (with `v`'s constraints), project to `(a, v)`, walk
        `R2` on to `b`, project to `(a, b)`, and join the pattern on both.
        No binding table ever holds `v` beside the other variables."""
        c = self.ctx
        a, b = _other_end(s1, v), _other_end(s2, v)
        r1 = regex_module.canonical(s1["regex"])
        if s1["to"] != v:
            r1 = regex_module.reverse(r1)                 # read a -> v
        r2 = regex_module.canonical(s2["regex"])
        if s2["from"] != v:
            r2 = regex_module.reverse(r2)                 # read v -> b
        ka, kb, into = c.name("pa"), c.name("pb"), c.name("pv")
        seeds = Aggregate(Project(self.rel, bindings=((ka, Col(c.uid_col[a])),), sigma_=c.sigma),
                          group_by=((ka, Col(ka)),), sigma_=c.sigma)
        rel: Node = Walk(seeds, ka, into, regex_module.text(r1), sigma_=c.sigma)
        saved, self.rel = self.rel, rel
        self._constrain(node, into)
        rel = Aggregate(self.rel, group_by=((ka, Col(ka)), (f"{into}.uid", Col(f"{into}.uid"))),
                        sigma_=c.sigma)
        into_b = c.name("pc")
        rel = Walk(rel, f"{into}.uid", into_b, regex_module.text(r2), sigma_=c.sigma)
        rel = Aggregate(rel, group_by=((ka, Col(ka)), (kb, Col(f"{into_b}.uid"))), sigma_=c.sigma)
        self.rel = Join(saved, rel, ((c.uid_col[a], ka), (c.uid_col[b], kb)), "inner",
                        sigma_=c.sigma)

    def _project_live(self, todo: list[dict[str, Any]]) -> None:
        """Under set semantics (a distinct answer over node identities, see
        `_set_semantics`), the binding table keeps only the identities still
        needed: the answer's, and those later steps or predicates read. A
        variable no later part reads is existential, so its rows collapse to
        the distinct identities of the others: the answer is the same, and a
        chain of regular paths never materialises every combination."""
        c = self.ctx
        needed = set(self.keep_only or ()) | {v for st in todo for v in (st["from"], st["to"])}
        for vs, _ in self.pending:
            needed |= vs
        bound = [v for v, k in c.kinds.items() if k == "node" and not v.startswith("_")]
        live = sorted(v for v in bound if v in needed)
        if len(live) == len(bound):
            return
        self.rel = Aggregate(self.rel, group_by=tuple((f"{v}.uid", Col(c.uid_col[v]))
                                                      for v in live), sigma_=c.sigma)
        for v in bound:
            if v in live:
                c.uid_col[v] = f"{v}.uid"
            else:
                c.kinds[v] = "gone"


def _other_end(st: dict[str, Any], v: str) -> str:
    return st["to"] if st["from"] == v else st["from"]


def _set_semantics(pat: dict[str, Any], group: dict[str, Any] | None,
                   reads: list[Any]) -> set[str] | None:
    """The variables a distinct answer reads, when the call can be evaluated
    under set semantics: every step a regular path, no edge variable, no
    sub-pattern, predicates and outputs reading identities only, and an
    answer that is a distinct projection (a group with no aggregates and no
    later stage). `None` otherwise."""
    if group is not None and (group.get("aggs") or group.get("then") or group.get("having")
                              or group.get("project")):
        return None
    if pat.get("optional") or pat.get("not") or pat.get("lookups") or pat.get("succ"):
        return None
    if any("regex" not in st for st in pat.get("steps", [])) or not pat.get("steps"):
        return None
    out: set[str] = set()
    for e in reads + list(pat.get("where", [])):
        if _props_in(e, "prop") or _props_in(e, "tprop"):
            return None
        for col in _cols_in(e):
            if not col.endswith(".uid"):
                return None
            out.add(_var_of(col))
    return out


def _rel_choice(st: dict[str, Any], single: bool) -> tuple[str | None, list[str]]:
    """A step's relationship type for `Expand`, and the alternatives a list
    names (a single hop only: a longer one is a regular path)."""
    rt = st.get("rel_type")
    if not isinstance(rt, list):
        return rt, []
    if not single:
        raise InvalidArgError("match: a list of relationship types is a single-hop form; "
                              "a longer step over alternatives is a regular path")
    if len(rt) == 1:
        return rt[0], []
    return None, list(rt)


def _one_of(column: str, values: list[str]) -> Expr:
    out: Expr | None = None
    for v in values:
        e = Cmp("=", Col(column), Lit(v))
        out = e if out is None else BoolOp("or", out, e)
    assert out is not None
    return out


def _anchor(nodes: list[dict]) -> dict:
    for n in nodes:
        if n.get("uids"):
            return n
    for n in nodes:
        if n.get("labels"):
            return n
    return nodes[0]


def _uid_in(column: str, uids: list[str]) -> Expr:
    out: Expr | None = None
    for u in uids:
        e = Cmp("=", Col(column), Lit(u))
        out = e if out is None else BoolOp("or", out, e)
    assert out is not None
    return out


def _sub(ctx: _Ctx, left: Node, sub: dict[str, Any], join_type: str,
         main_vars: set[str]) -> Node:
    """`optional` (left-outer) and `not` (anti) over the shared variables. The
    right side is the sub-pattern grown from the distinct keys of the left."""
    nodes = sub.get("nodes", [])
    steps = sub.get("steps", [])
    used = {n["var"] for n in nodes} | {s["from"] for s in steps} | {s["to"] for s in steps}
    shared = sorted(v for v in used if v in main_vars and ctx.kinds.get(v) != "edge")
    if not shared:
        raise InvalidArgError("match: a sub-pattern must share a node variable with the pattern")
    if not ctx.expand_path:
        why = ctx.shared_instant_refusal("an optional or not sub-pattern")
        if why is not None:
            raise InvalidArgError(why)
    sub_ctx = _Ctx(ctx.sigma, ctx.vt_mode, ctx.belief, fresh=ctx.fresh, axes=ctx.axes)
    sub_ctx.fragmented = ctx.fragmented
    every = shared
    anchor = _sub_anchor(sub, shared)
    proxies = list(shared) if anchor is not None else _decorrelation(sub, shared, join_type)
    if proxies:
        shared = [v for v in shared if v not in proxies]
    keys = tuple((sub_ctx.name("k"), Col(ctx.uid_col[v])) for v in shared)
    right: Node | None = None if anchor is not None else \
        Aggregate(left, group_by=keys, sigma_=ctx.sigma)
    for (k, _), v in zip(keys, shared):
        sub_ctx.kinds[v] = "node"
        sub_ctx.uid_col[v] = k
        sub_ctx.key_vars.add(v)
    for w in sub.get("where", []):
        for col in _cols_in(w):
            if _var_of(col) in shared and not col.endswith(".uid"):
                raise InvalidArgError(
                    "match: a sub-pattern predicate may read a shared variable's uid only",
                    column=col)
        for k in ("prop", "tprop"):
            for var in _props_in(w, k):
                if var in shared:
                    raise InvalidArgError(
                        "match: a sub-pattern predicate may not read a shared variable's props")
    b = _Builder(sub_ctx, right)
    b.proxies = set(proxies)
    for n in nodes:
        if n["var"] in every and (n.get("labels") or n.get("uids")):
            raise InvalidArgError("match: constrain a shared variable in the pattern, not in "
                                  "the sub-pattern")
    b.pattern([n for n in nodes if n["var"] not in every] or [{"var": shared[0]}],
              steps, sub.get("where", []), anchor_var=anchor)
    on = tuple((ctx.uid_col[v], k) for (k, _), v in zip(keys, shared)) + \
        tuple((ctx.uid_col[v], sub_ctx.uid_col[v]) for v in proxies)
    right_rel = b.rel
    if ctx.fragmented:
        # then the per-instant join
        b._carry()
        assert sub_ctx.run_iv is not None, "a sub-pattern step binds a version"
        right_iv = sub_ctx.run_iv
        right_rel = b.rel
    ctx.fresh = sub_ctx.fresh
    if join_type == "left_outer":
        for v, kind in sub_ctx.kinds.items():
            if v not in every:
                ctx.kinds[v] = kind
        ctx.vid_cols.extend(c for c in sub_ctx.vid_cols if _var_of(c) not in every)
        ctx.depth_cols.extend(sub_ctx.depth_cols)
        ctx.orient_cols.extend(sub_ctx.orient_cols)
    if ctx.fragmented:
        out_iv = (ctx.name("ws"), ctx.name("we"))
        rel = CoValidJoin(left, right_rel, on, join_type, left_iv=ctx.seq_iv,
                          right_iv=right_iv, out_iv=out_iv, sigma_=ctx.sigma)
        ctx.seq_iv = out_iv
        return rel
    return Join(left, b.rel, on, join_type, sigma_=ctx.sigma)


def _implied_on() -> bool:
    return os.environ.get("GNOMON_MATCH_IMPLIED", "1") != "0"


def _flat(w: Any, op: str) -> list[Any]:
    """The operands of a chain of `bool: op` (one item when `w` is not one)."""
    if isinstance(w, dict) and w.get("bool") == op and "l" in w and "r" in w:
        return _flat(w["l"], op) + _flat(w["r"], op)
    return [w]


def _fold(items: list[Any], op: str) -> Any:
    out = items[0]
    for x in items[1:]:
        out = {"bool": op, "l": out, "r": x}
    return out


def _implied(where: list[Any]) -> list[Any]:
    """The single-variable predicates a disjunction over several
    variables implies. When every disjunct of `a OR b OR ...` holds
    conjuncts reading one variable `v` alone, any binding the disjunction
    keeps passes the disjunction of those conjunct groups (the disjunct it
    passes passes its group), so adding that predicate keeps the same rows,
    NULLs included; the step order may then anchor on `v` (BI16: the tag by
    `name = $tagA OR name = $tagB`, where the pattern walked every person's
    every message: SF10 31.5M messages, 36.8M tags, 57 s, before 5,919
    rows passed). `GNOMON_MATCH_IMPLIED=0` adds none."""
    if not _implied_on():
        return []
    out: list[Any] = []
    for w in where:
        disj = _flat(w, "or")
        if len(disj) < 2 or len(_vars_in(w)) < 2:
            continue
        groups = []
        for d in disj:
            g: dict[str, list[Any]] = {}
            for c in _flat(d, "and"):
                vs = _vars_in(c)
                if len(vs) == 1:
                    g.setdefault(next(iter(vs)), []).append(c)
            groups.append(g)
        for v in sorted(set.intersection(*(set(g) for g in groups))):
            out.append(_fold([_fold(g[v], "and") for g in groups], "or"))
    return out


def _eq_literals(w: Any) -> int:
    """The number of values when `w` is `x = lit OR x = lit ...` on one
    variable's property or column (`0` otherwise)."""
    parts = _flat(w, "or")
    for c in parts:
        if not (isinstance(c, dict) and c.get("cmp") == "=" and
                any(isinstance(c.get(side), dict) and "lit" in c[side] for side in ("l", "r"))):
            return 0
    return len(parts)


def _sub_anchor(sub: dict[str, Any], shared: list[str]) -> str | None:
    """The variable an `optional` or `not` sub-pattern is grown from instead
    of the outer rows' keys: a node of its own with uids or an
    equality predicate reading it alone (BI16: the tag by name), when every
    shared variable is the far end of exactly one plain single hop and read
    by no predicate of the sub-pattern. The shared variables then bind as
    those hops' endpoint uids (key variables, as `_decorrelation`'s), and the
    join reads them all; the sub-pattern's bindings for a key do not depend
    on the outer rows, so the rows are the same. Grown from the keys, BI16
    walked every friend's every message (SF1: 34,918 keys -> 20.7M rows
    before the one-day, one-tag filters). `GNOMON_MATCH_SUB_ANCHOR=0` grows it
    from the keys."""
    if os.environ.get("GNOMON_MATCH_SUB_ANCHOR", "1") == "0" or not shared:
        return None
    steps = sub.get("steps", [])
    where = sub.get("where", [])
    read = set().union(*(_vars_in(w) for w in where)) if where else set()

    def plain(st: dict[str, Any]) -> bool:
        if "regex" in st or {"order", "edge_where", "truncate"} & set(st):
            return False
        h = _hops(st.get("hops"))
        return isinstance(h, Exact) and h.k == 1

    for v in shared:
        if v in read or sum(v in (st["from"], st["to"]) for st in steps) != 1:
            return None
    own = [n for n in sub.get("nodes", []) if n["var"] not in shared]
    cands = [n["var"] for n in own if n.get("uids")] + [
        n["var"] for n in own if not n.get("uids") and any(
            isinstance(w, dict) and w.get("cmp") == "=" and _vars_in(w) == {n["var"]} for w in where)]
    for a in cands:
        bound, todo, ok = {a}, list(steps), True
        while todo and ok:
            pos = next((k for k, st in enumerate(todo) if st["from"] in bound or st["to"] in bound), None)
            if pos is None:
                ok = False
                break
            st = todo.pop(pos)
            far = {st["from"], st["to"]} - bound
            if (far & set(shared) and not plain(st)) or (set(shared) & {st["from"], st["to"]} & bound):
                ok = False
            bound |= {st["from"], st["to"]}
        if ok and set(shared) <= bound:
            return a
    return None


def _decorrelation(sub: dict[str, Any], shared: list[str], join_type: str) -> list[str]:
    """The shared variables a `not` sub-pattern need not be grown from: each is the far end of exactly one plain single hop and is read by
    no predicate of the sub-pattern, so in the correlated form it is only that
    hop's closing test. The sub-pattern is then grown from the distinct keys
    of the other shared variables alone, the hop leaves the variable free (its
    endpoint uid, bound as a key variable: no node version, no interval, as
    the closing test binds none), and the anti-join reads it with the rest.
    The rows the anti-join keeps are the same; the binding table no longer
    repeats the sub-pattern once per value of the variable (IC4: once per
    tag, 695M-1.1B rows at SF10). `[]` keeps the correlated form:
    `GNOMON_MATCH_DECORRELATE=0`, an optional sub-pattern (its rows are the
    answer's), one shared variable, or no seed from which every step is
    reached in the builder's order with each such variable a far end."""
    if join_type != "anti" or len(shared) < 2 or os.environ.get("GNOMON_MATCH_DECORRELATE", "1") == "0":
        return []
    steps = sub.get("steps", [])
    read = set().union(*(_vars_in(w) for w in sub.get("where", []))) if sub.get("where") else set()

    def plain(st: dict[str, Any]) -> bool:
        if "regex" in st or {"order", "edge_where", "truncate"} & set(st):
            return False
        h = _hops(st.get("hops"))
        return isinstance(h, Exact) and h.k == 1

    # the seeds: the shared variables of the step the correlated form expands
    # first (every shared variable bound, the first step in order touching
    # one), and every shared variable that cannot be left free; growing from
    # another one could expand far more (IC4's tag: every post of the tag)
    first = next((st for st in steps if st["from"] in shared or st["to"] in shared), None)
    if first is None:
        return []
    seeds = {v for v in (first["from"], first["to"]) if v in shared}
    for v in shared:
        if v in read or sum(v in (st["from"], st["to"]) for st in steps) != 1:
            seeds.add(v)
    cand = [v for v in shared if v not in seeds]
    if not cand:
        return []
    bound, todo = set(seeds), list(steps)
    while todo:
        pos = next((k for k, st in enumerate(todo) if st["from"] in bound or st["to"] in bound), None)
        if pos is None:
            return []
        st = todo.pop(pos)
        far = {st["from"], st["to"]} - bound
        if far & set(cand) and not plain(st):
            return []
        bound |= {st["from"], st["to"]}
    return cand if set(cand) <= bound else []


def _cols_in(spec: Any) -> list[str]:
    out: list[str] = []
    if isinstance(spec, dict):
        for k, v in spec.items():
            if k == "col" and isinstance(v, str):
                out.append(v)
            elif k != "lit":
                out.extend(_cols_in(v))
    elif isinstance(spec, list):
        for v in spec:
            out.extend(_cols_in(v))
    return out


def _props_in(spec: Any, key: str) -> list[str]:
    out: list[str] = []
    if isinstance(spec, dict):
        for k, v in spec.items():
            if k == key:
                out.append(v[0])
            elif k != "lit":
                out.extend(_props_in(v, key))
    elif isinstance(spec, list):
        for v in spec:
            out.extend(_props_in(v, key))
    return out


def _check_as(name: str, seen: set[str]) -> None:
    if not _NAME.match(name):
        raise InvalidArgError(f"match: output name {name!r} must be a plain identifier")
    if name in seen:
        raise InvalidArgError(f"match: duplicate output name {name!r}")
    seen.add(name)


def _group(ctx: _Ctx, rel: Node, g: dict[str, Any], pattern_stage: bool,
           allowed: set[str] | None) -> tuple[Node, list[str]]:
    """One aggregation stage: plain aggregates in one `Aggregate`; each
    `distinct_on` set folds once per distinct identity tuple (an inner
    `Aggregate` keyed by group keys + identities, then the outer fold), joined
    back on the group keys."""
    sigma = ctx.sigma
    seen: set[str] = set()
    by = []
    for b in g["by"]:
        _check_as(b["as"], seen)
        by.append((b["as"], _expr(b["expr"], ctx if pattern_stage else None, allowed)))
    hidden: list[str] = []
    if not by and any(a.get("distinct_on") for a in g["aggs"]):
        # a whole-stage fold with distinct_on: the per-identity folds join
        # back on a constant key, dropped from the stage's outputs
        hidden.append(ctx.name("g"))
        by.append((hidden[0], Lit(0)))
    plain: list[Agg] = []
    distinct: dict[tuple[str, ...], list[tuple[str, str, Expr | None, Expr | None]]] = {}
    for a in g["aggs"]:
        _check_as(a["as"], seen)
        fn, of = a["fn"], a.get("of")
        if fn == "collect":
            plain.append(_collect(ctx, rel, a, pattern_stage, allowed))
            continue
        if isinstance(of, list) or any(k in a for k in ("order_by", "limit", "distinct")):
            raise InvalidArgError(f"match: a list `of`, order_by, limit and distinct are "
                                  f"collect's, not {fn}'s")
        of_e = _expr(of, ctx if pattern_stage else None, allowed) if of is not None else None
        if fn != "count" and of_e is None:
            raise InvalidArgError(f"match: {fn} needs `of`")
        cond = None
        if a.get("where") is not None:
            # the fold sees only the rows where the predicate holds (null: not)
            cond = Coalesce(_expr(a["where"], ctx if pattern_stage else None, allowed),
                            Lit(False))
            if of_e is not None:
                of_e = If(cond, of_e, Lit(None, of_e.tau(rel.out_schema)))
        if a.get("distinct_on"):
            if not pattern_stage:
                raise InvalidArgError("match: distinct_on names pattern variables; it is "
                                      "only defined on the first stage")
            names = tuple(a["distinct_on"])
            for v in names:
                if ctx.kinds.get(v) not in ("node", "edge", "target"):
                    raise InvalidArgError(f"match: distinct_on names an unbound variable {v!r}")
            # A lookup's outputs are a function of the variables its `on`
            # reads (the lookup's key is unique), so a lookup keyed on named
            # variables reads as one of them
            reads = {v for v in _vars_in(of or {})
                     if not (ctx.kinds.get(v) == "lookup" and ctx.lookup_on.get(v, {v}) <= set(names))}
            if of is not None and not reads <= set(names):
                raise InvalidArgError("match: with distinct_on, `of` may read only the "
                                      "named variables (its value must be a function of them)")
            distinct.setdefault(names, []).append((a["as"], fn, of_e, cond))
            continue
        if fn == "count" and of_e is None and cond is not None:
            plain.append(Agg("sum_exact", a["as"], If(cond, Lit(1), Lit(0))))
            continue
        plain.append(_agg(fn, a["as"], of_e))
    out: Node = Aggregate(rel, group_by=tuple(by), aggregates=tuple(plain), sigma_=sigma)
    for n_set, (names, aggs) in enumerate(sorted(distinct.items())):
        ident = []
        for v in names:
            col = f"{v}.eid" if ctx.kinds[v] == "edge" else f"{v}.uid"
            ident.append((ctx.name("d"), Col(col)))
        inner_aggs = []
        outer_aggs = []
        for as_, fn, of_e, cond in aggs:
            if fn == "count" and cond is not None and of_e is None:
                w = ctx.name("w")
                inner_aggs.append(Agg("max", w, If(cond, Lit(1), Lit(0))))
                outer_aggs.append(Agg("sum_exact", as_, Col(w)))
                continue
            if fn == "count" and of_e is None:
                outer_aggs.append(Agg("count", as_))
                continue
            v = ctx.name("v")
            inner_aggs.append(Agg("min", v, of_e))
            outer_aggs.append(_agg(fn, as_, Col(v)))
        inner = Aggregate(rel, group_by=tuple(by) + tuple(ident),
                          aggregates=tuple(inner_aggs), sigma_=sigma)
        mid = Project(inner, bindings=((ctx.name("p"), Lit(0)),), keep="all", sigma_=sigma)
        keyed = [(ctx.name("j"), Col(name)) for name, _ in by]
        outer = Aggregate(mid, group_by=tuple(keyed), aggregates=tuple(outer_aggs),
                          sigma_=sigma)
        if by:
            out = Join(out, outer, tuple((name, k) for (name, _), (k, _) in zip(by, keyed)),
                       "inner", sigma_=sigma)
        else:
            raise InvalidArgError("match: distinct_on needs at least one group key")
    names = [name for name, _ in by if name not in hidden] + [a["as"] for a in g["aggs"]]
    if g.get("project"):
        out, names = _project(ctx, out, names, g["project"], seen)
    else:
        out = Project(out, bindings=tuple((n, Col(n)) for n in names), sigma_=sigma)
    if g.get("having") is not None:
        out = Filter(out, _expr(g["having"], None, set(names)), sigma_=sigma)
    if g.get("then") is not None:
        return _group(ctx, out, g["then"], False, set(names))
    return out, names


def _order_keys(specs: list[dict[str, Any]], ctx: _Ctx | None,
                allowed: set[str] | None) -> list[SortKey]:
    return [SortKey(_expr(o["expr"], ctx, allowed), o.get("dir", "asc"),
                    "nulls_first" if o.get("nulls", "last") == "first" else "nulls_last")
            for o in specs]


def _collect(ctx: _Ctx, rel: Node, a: dict[str, Any], pattern_stage: bool,
             allowed: set[str] | None) -> Agg:
    """`collect`: the elements (one value, or a list of several) of a
    group's rows, ordered by `order_by` then by the elements, `distinct`
    keeping equal ones once, at most `limit`. Rows whose components are all
    null contribute nothing, so `where` nulls the components it excludes."""
    if a.get("distinct_on"):
        raise InvalidArgError("match: collect takes `distinct`, not distinct_on")
    if a.get("of") is None:
        raise InvalidArgError("match: collect needs `of`")
    sctx = ctx if pattern_stage else None
    specs = a["of"] if isinstance(a["of"], list) else [a["of"]]
    items = [_expr(x, sctx, allowed) for x in specs]
    if a.get("where") is not None:
        cond = Coalesce(_expr(a["where"], sctx, allowed), Lit(False))
        items = [If(cond, e, Lit(None, e.tau(rel.out_schema))) for e in items]
    return Agg("collect", a["as"], TupleExpr(tuple(items)),
               order=tuple(_order_keys(a.get("order_by", []), sctx, allowed)),
               limit=a.get("limit"), distinct=bool(a.get("distinct", False)))


def _totals(spec: Any, found: list[str]) -> Any:
    """`{"total": c}` -> a column read of the total's name, recording `c`."""
    if isinstance(spec, list):
        return [_totals(x, found) for x in spec]
    if not isinstance(spec, dict):
        return spec
    if set(spec) == {"total"}:
        c = spec["total"]
        if not isinstance(c, str):
            raise InvalidArgError("match: total names one stage column")
        if c not in found:
            found.append(c)
        return {"col": f"_total{found.index(c)}"}
    return {k: (v if k == "lit" else _totals(v, found)) for k, v in spec.items()}


def _project(ctx: _Ctx, rel: Node, names: list[str], project: list[dict[str, Any]],
             seen: set[str]) -> tuple[Node, list[str]]:
    """`group.project`: expressions over the stage's outputs, appended to
    them. `{"total": c}` is column `c`'s exact sum over the whole stage: a
    one-row aggregate joined to every group on a constant key."""
    sigma = ctx.sigma
    found: list[str] = []
    specs = [(p["as"], _totals(p["expr"], found)) for p in project]
    # the stage's outputs as plain columns: the plan IR's one-aggregate-column
    # rule speaks of a Project directly over an Aggregate (the translated
    # benchmark plans), and stays as it is
    rel = Project(rel, bindings=tuple((n, Col(n)) for n in names), sigma_=sigma)
    for c in found:
        if c not in names:
            raise InvalidArgError(f"match: total of an unknown column {c!r}", known=names)
        if rel.out_schema.tau_of(c).base not in ("int", "float"):
            raise InvalidArgError(f"match: total of a non-numeric column {c!r}")
    allowed = set(names)
    if found:
        kl, kr = ctx.name("tk"), ctx.name("tk")
        left = Project(rel, bindings=((kl, Lit(0)),), keep="all", sigma_=sigma)
        tot = Aggregate(left, group_by=(), aggregates=tuple(
            Agg("sum_exact", f"_total{i}", Col(c)) for i, c in enumerate(found)), sigma_=sigma)
        right = Project(tot, bindings=((kr, Lit(0)),), keep="all", sigma_=sigma)
        rel = Join(left, right, ((kl, kr),), "inner", sigma_=sigma)
        allowed |= {f"_total{i}" for i in range(len(found))}
    bindings = [(n, Col(n)) for n in names]
    for as_, spec in specs:
        _check_as(as_, seen)
        bindings.append((as_, _expr(spec, None, allowed)))
    out = Project(rel, bindings=tuple(bindings), sigma_=sigma)
    return out, names + [as_ for as_, _ in specs]


#: Up to this many intervals a common interval is one nested-if projection
#: (2^n expression nodes, each a pass over the column); beyond it, a chain of
#: projections, each two levels deep. Eight intervals as one projection took
#: 13 s over 0.9M bindings at SF1, the chain under 1 s.
_NESTED_MAX = 2


def _greatest(cols: list[Expr], least: bool) -> Expr:
    out: Expr = cols[0]
    for c in cols[1:]:
        pick = Cmp("<" if least else ">", c, out)
        out = If(pick, c, out)
    return out


def _sequenced(ctx: _Ctx, rel: Node, axes: list[str],
               names: dict[str, tuple[str, str]] | None = None,
               expose: bool = True) -> Node:
    """Sequenced semantics: every bound version's interval on each named
    axis shares an instant; the common interval is projected (its start is
    the greatest start, its end the least end) and must be non-empty. (Under
    `_Builder._carry`.)"""
    bound = [v for v, k in ctx.kinds.items()
             if not v.startswith(("_c", "_u", "_k")) and k in ("node", "edge", "target")
             and v not in ctx.key_vars]
    for ax in axes:
        lo, hi = ("vt_s", "vt_e") if ax == "valid" else ("tt_s", "tt_e")
        s_name, e_name = (names or SEQUENCED_COLS)[ax]
        starts: list[Expr] = [Col(f"{v}.{lo}") for v in bound]
        ends: list[Expr] = [Col(f"{v}.{hi}") for v in bound]
        if len(starts) > _NESTED_MAX:
            # one interval per projection: each expression two levels deep
            lo_e, hi_e = starts[0], ends[0]
            for a, b in zip(starts[1:], ends[1:]):
                ls, le = ctx.name("qs"), ctx.name("qe")
                rel = Project(rel, bindings=((ls, If(Cmp(">", a, lo_e), a, lo_e)),
                                             (le, If(Cmp("<", b, hi_e), b, hi_e))),
                              keep="all", sigma_=ctx.sigma)
                lo_e, hi_e = Col(ls), Col(le)
            starts, ends = [lo_e], [hi_e]
        rel = Project(rel, bindings=((s_name, _greatest(starts, False)),
                                     (e_name, _greatest(ends, True))),
                      keep="all", sigma_=ctx.sigma)
        rel = Filter(rel, Cmp("<", Col(s_name), Col(e_name)), sigma_=ctx.sigma)
        if expose:
            ctx.sequenced |= {s_name, e_name}
    return rel


def _agg(fn: str, alias: str, of: Expr | None) -> Agg:
    if fn == "count":
        if of is None:
            return Agg("count", alias)
        return Agg("sum_exact", alias, If(IsNull(of), Lit(0), Lit(1)))
    if fn == "sum":
        return Agg("sum_exact", alias, of)
    return Agg(fn, alias, of)


_JSON_TAUS = ("json",)


def _tree_reduction(pat: dict[str, Any], keep: set[str], ctx: _Ctx
                    ) -> tuple[dict[str, Any], dict[str, Node]] | None:
    """Under set semantics, a tree-shaped body is evaluated after a semijoin
    pass (Yannakakis): bottom-up from the leaves, each variable's qualifying
    nodes are those with, for every child, a path spelling the step to a
    qualifying child, found by one reversed walk from all the child's
    qualifying nodes at once (no pairs). Branches that reach no needed
    variable then drop out of the pattern (they are only existence tests),
    and every remaining variable is joined to its qualifying nodes as it
    binds, so no binding table holds a row that does not extend to an
    answer. Returns the remaining pattern and the qualifying sets; `None`
    when the body is not a tree."""
    nodes = {n["var"]: n for n in pat["nodes"]}
    steps = pat["steps"]
    names = list(nodes)
    for st in steps:
        for v in (st["from"], st["to"]):
            if v not in nodes:
                nodes[v] = {"var": v}
                names.append(v)
    if len(steps) != len(names) - 1:
        return None                                   # a cycle, or disconnected
    adj: dict[str, list[tuple[str, dict[str, Any]]]] = {v: [] for v in names}
    for st in steps:
        spec = regex_module.canonical(st["regex"])
        adj[st["from"]].append((st["to"], spec))                      # from -> to
        adj[st["to"]].append((st["from"], regex_module.reverse(spec)))
    need = {v for v in keep if v in nodes} | {v for w in pat.get("where", []) for v in _vars_in(w)}
    root = next((v for v in names if v in need), names[0])
    parent: dict[str, str | None] = {root: None}
    order = [root]
    for v in order:
        for w, _ in adj[v]:
            if w not in parent:
                parent[w] = v
                order.append(w)
    if len(order) != len(names):
        return None
    under = {v: v in need or v == root for v in names}   # does v's subtree hold a needed var
    for v in reversed(order):
        if parent[v] is not None and under[v]:
            under[parent[v]] = True
    keep_vars = {v for v in names if under[v]}
    if len(steps) == 0:
        return None

    def node_scan(v: str) -> Node:
        n = nodes[v]
        return NodeScan(v, labels=tuple(n["labels"]) if n.get("labels") else None,
                        uids=tuple(n["uids"]) if n.get("uids") else None,
                        belief=ctx.belief, vt_mode=ctx.vt_mode, sigma_=ctx.sigma)

    memo: dict[str, Node] = {}

    def qualifying(v: str) -> Node:
        """The nodes `v` may bind given its whole subtree (the semijoin
        pass, bottom-up), as one key column `_qv_<v>`."""
        if v not in memo:
            rel: Node = node_scan(v)
            for w, spec in adj[v]:
                if parent.get(w) == v:
                    rel = Join(rel, reach_into(w, regex_module.reverse(spec)),
                               ((f"{v}.uid", f"_q_{w}"),), "inner", sigma_=ctx.sigma)
            memo[v] = Aggregate(rel, group_by=((f"_qv_{v}", Col(f"{v}.uid")),),
                                sigma_=ctx.sigma)
        return memo[v]

    def reach_into(v: str, back: dict[str, Any]) -> Node:
        """The nodes from which a path spelling the step reaches a qualifying
        `v` (a reversed walk from all of them at once), as `_q_<v>`."""
        into = f"_r_{v}"
        walk = Walk(qualifying(v), f"_qv_{v}", into, regex_module.text(back), sigma_=ctx.sigma)
        return Aggregate(walk, group_by=((f"_q_{v}", Col(f"{into}.uid")),), sigma_=ctx.sigma)

    qual = {v: qualifying(v) for v in keep_vars}
    rest = {"nodes": [n for v, n in nodes.items() if v in keep_vars],
            "steps": [st for st in steps if st["from"] in keep_vars and st["to"] in keep_vars],
            "where": pat.get("where", [])}
    return rest, qual


def _pattern_rel(pat: dict[str, Any], sigma: Sigma, vt_mode: str, belief: str,
                 axes: list[str], keep_only: set[str] | None = None,
                 card: dict[str, Any] | None = None,
                 qual: dict[str, Node] | None = None,
                 ends: dict[str, Any] | None = None) -> tuple[_Ctx, Node]:
    """One pattern (with its sequenced projection and sub-patterns) lowered
    onto a relation, and the context that names its variables. With
    `card`, the steps may be reordered by estimated cardinality
    (`plan_order`)."""
    ctx = _Ctx(sigma, vt_mode, belief, axes=tuple(axes), card=card)
    if keep_only is not None and not axes and ctx.expand_path:
        reduced = _tree_reduction(pat, keep_only, ctx)
        if reduced is not None:
            rest, qual = reduced
            b = _Builder(ctx, None, keep_only, qual)
            b.pattern(rest["nodes"], rest["steps"], rest["where"])
            return ctx, b.rel
    seq_names = {c for ax in axes for c in SEQUENCED_COLS[ax]}
    where = pat.get("where", [])
    implied = [w for w in _implied(where) if not set(_cols_in(w)) & seq_names]
    if implied:
        where = list(where) + implied
        pat = {**pat, "where": where}
    looks = {lk["as"] for lk in pat.get("lookups", [])}
    deferred = [w for w in where if set(_cols_in(w)) & seq_names]
    walks = any("min" in (st.get("hops") or {}) for st in pat.get("steps", [])
                if "regex" not in st)
    if ctx.shared_instant and (walks or pat.get("optional") or pat.get("not")):
        ctx.fragmented = True
    b = _Builder(ctx, None, keep_only if not axes and not deferred and not pat.get("succ")
                 and not looks else None, qual)
    b.lookups = list(pat.get("lookups", []))
    planned = plan_order(pat, card)
    split = None
    if planned is None and ends is not None and b.keep_only is None and not b.lookups \
            and not deferred and ctx.belief == "current" and ctx.vt_mode == "overlap" \
            and set(axes) <= {"valid"}:
        split = second_anchor(pat, ends)
    if split is not None:
        rel = _second_anchor_rel(ctx, pat, [w for w in where if w not in deferred], split)
        b.rel = rel
    else:
        anchor_var, steps = planned if planned is not None else (None, pat.get("steps", []))
        b.pattern(pat["nodes"], steps, [w for w in where if w not in deferred],
                  pat.get("succ"), anchor_var=anchor_var)
        rel = b.rel
    main_vars = set(ctx.kinds)
    if ctx.fragmented:
        # carried as fragments and collapsed to one row per version tuple
        if deferred:
            _check_col(sorted(set(_cols_in(deferred)) & seq_names)[0], ctx, None)
        b._carry()
        assert ctx.run_iv is not None, "the anchor binds a version"
        rel = b.rel
        ctx.seq_iv = ctx.run_iv
    elif axes:
        rel = _sequenced(ctx, rel, axes)
        for w in deferred:
            rel = Filter(rel, _expr(w, ctx), sigma_=ctx.sigma)
    elif deferred:
        raise InvalidArgError("match: valid.s/valid.e (belief.s/belief.e) need scope.co_valid")
    for sub in pat.get("optional", []):
        rel = _sub(ctx, rel, sub, "left_outer", main_vars)
    for sub in pat.get("not", []):
        rel = _sub(ctx, rel, sub, "anti", main_vars)
    if ctx.fragmented:
        schema = rel.out_schema
        rel = Collapse(rel, by=tuple(v for v in ctx.vid_cols + ctx.orient_cols if v in schema),
                       mins=tuple(d for d in ctx.depth_cols if d in schema), sigma_=ctx.sigma)
    return ctx, rel


def _lookup(ctx: _Ctx, rel: Node, lk: dict[str, Any]) -> Node:
    """A decorrelated subquery: a sub-`match` under the same scope whose last
    group stage is keyed by `key` (so keys are unique), joined to the pattern on
    `on = <as>.<key>`, left outer by default; its outputs read as
    `<as>.<name>`."""
    alias = lk["as"]
    if not _NAME.match(alias) or alias in ctx.kinds or alias in ctx.uid_col or any(
            c.startswith(f"{alias}.") for c in ctx.lookup_cols):
        raise InvalidArgError(f"match: lookup name {alias!r} must be fresh")
    sub = lk["match"]
    last = sub["group"]
    while last.get("then"):
        last = last["then"]
    if lk["key"] not in [b["as"] for b in last["by"]]:
        raise InvalidArgError("match: a lookup's key is a group key of its last stage")
    seed = _lookup_seed(lk)
    if seed is not None:
        # only where the sub-match binds the key variable first: joined later,
        # the semijoin prunes nothing and costs a key aggregate and a join
        # (TCR8 on the corpus store: +28 %)
        planned = plan_order(sub["pattern"], ctx.card)
        first = planned[0] if planned is not None else _anchor(sub["pattern"].get("nodes", []))["var"]
        if first != seed:
            seed = None
    qual = None
    if seed is not None:
        # The sub-match's key variable bound to the outer rows' distinct keys
    # only (a semijoin as it binds; the step order stays the match plan's)
        qual = {seed: Aggregate(rel, group_by=((ctx.name("lq"), _expr(lk["on"], ctx)),),
                                sigma_=ctx.sigma)}
    _sctx, srel = _pattern_rel(sub["pattern"], ctx.sigma, ctx.vt_mode, ctx.belief, [],
                               card=ctx.card, qual=qual)
    grouped, cols = _group(_sctx, srel, sub["group"], True, None)
    right = Project(grouped, bindings=tuple((f"{alias}.{c}", Col(c)) for c in cols),
                    sigma_=ctx.sigma)
    k = ctx.name("lk")
    on = _expr(lk["on"], ctx)
    if on.tau(rel.out_schema).nullable:
        # a null join key is an error in the plan IR, never a non-match
        raise InvalidArgError("match: a lookup's `on` may not be null; bind it in the "
                              "pattern (or in a lookup's own pattern), not in an optional")
    left = Project(rel, bindings=((k, on),), keep="all", sigma_=ctx.sigma)
    out = Join(left, right, ((k, f"{alias}.{lk['key']}"),),
               "inner" if lk.get("join") == "inner" else "left_outer", sigma_=ctx.sigma)
    ctx.lookup_cols |= {f"{alias}.{c}" for c in cols}
    return out


def _lookup_seed(lk: dict[str, Any]) -> str | None:
    """The node variable a lookup's sub-match may be restricted to the outer
    rows' keys by, or None. Its group stages must carry the key from a
    first stage keyed by `<v>.uid` (each later stage keyed by the previous
    stage's key column) with nothing that ranks or cuts across keys; then
    the rows of a key are those of the bindings with that `v`, and the
    sub-match binds `v` only to keys the outer rows hold. `_lookup` applies
    it only when the match plan's own anchor of the sub-match is `v` (anchoring
    it there by force made BI8 6x slower; joined after other steps it prunes
    nothing). BI6's `pop` counted the
    likes of every person's every message (SF10: 31.5M messages, 26.8M
    likes, ~30 s) to read 6,825 persons. `GNOMON_MATCH_LOOKUP_SEED=0` reads
    every key."""
    if os.environ.get("GNOMON_MATCH_LOOKUP_SEED", "1") == "0":
        return None
    sub = lk["match"]
    pat = sub.get("pattern") or {}
    stages, g = [], sub.get("group")
    while isinstance(g, dict):
        if set(g) - {"by", "aggs", "project", "having", "then"}:
            return None
        stages.append(g)
        g = g.get("then")
    if not stages:
        return None
    first = [b for b in stages[0].get("by", []) if isinstance(b.get("expr"), dict)
             and set(b["expr"]) == {"col"} and str(b["expr"]["col"]).endswith(".uid")]
    nodes = {n["var"]: n for n in pat.get("nodes", [])}
    for b in first:
        v = b["expr"]["col"][: -len(".uid")]
        if v not in nodes or nodes[v].get("uids"):
            continue
        name, ok = b["as"], True
        for st in stages[1:]:
            nxt = [c for c in st.get("by", []) if c.get("expr") == {"col": name}]
            if not nxt:
                ok = False
                break
            name = nxt[0]["as"]
        if ok and name == lk["key"]:
            return v
    return None


def _union_rel(args: dict[str, Any], sigma: Sigma, vt_mode: str, belief: str,
               axes: list[str], card: dict[str, Any] | None = None) -> tuple[Node, list[str]]:
    """`union`: each branch's pattern projected to the branch's `rows`, the
    branches' rows in order (bag union). Every branch names the same columns;
    the columns are typed (a JSON value has no order to complete one with)."""
    names: list[str] | None = None
    branches: list[Node] = []
    group = args["return"].get("group")
    distinct = group is not None and _set_semantics({"steps": [{"regex": 1}]}, group, []) \
        is not None and not args["return"].get("top_per")
    for i, br in enumerate(args["union"]):
        keep = _set_semantics(br["pattern"], None, [r["expr"] for r in br["rows"]]) \
            if distinct else None
        bctx, rel = _pattern_rel(br["pattern"], sigma, vt_mode, belief, axes, keep, card)
        seen: set[str] = set()
        bindings = []
        for r in br["rows"]:
            _check_as(r["as"], seen)
            bindings.append((r["as"], _expr(r["expr"], bctx)))
        cols = [n for n, _ in bindings]
        if names is None:
            names = cols
        elif cols != names:
            raise InvalidArgError("match: every union branch returns the same columns in "
                                  "the same order", first=names, branch=i, got=cols)
        proj = Project(rel, bindings=tuple(bindings), sigma_=sigma)
        for n in cols:
            if proj.out_schema.tau_of(n).base in _JSON_TAUS:
                raise InvalidArgError(f"match: union column {n!r} must be typed (use tprop)")
        branches.append(proj)
    assert names is not None
    return Union(tuple(branches), sigma_=sigma), names


def compile_match(args: dict[str, Any], card: dict[str, Any] | None = None,
                  ends: dict[str, Any] | None = None) -> Compiled:
    """The filled argument (`validate`) -> its plan. `card` (label and
    relationship counts, `cardinalities`) lets the compiler order a
    pattern's steps by estimated size (`plan_order`). `ends` (the counts
    with each relationship's distinct end identities, `anchor_cardinalities`)
    lets it grow a uid-anchored pattern from a second anchor
    (`second_anchor`)."""
    from gnomon.plan.node import schema_memo

    if card is not None:
        card = {**card, "walks_movable": _reads_no_depth(args)}
    with schema_memo():
        return _compile_match(args, card, ends)


def _reads_no_depth(args: dict[str, Any]) -> bool:
    """Whether no clause of the argument reads a walk target's `depth` (then
    a variable-length step reads the same pairs from either end)."""
    return ".depth" not in json.dumps(args, default=str)


def _compile_match(args: dict[str, Any], card: dict[str, Any] | None = None,
                   ends: dict[str, Any] | None = None) -> Compiled:
    sigma, vt_mode = _sigma_of(args["scope"])
    belief = args["scope"].get("belief", "current")
    axes = args["scope"].get("co_valid") or []
    ret = args["return"]
    if "union" in args:
        rel, union_cols = _union_rel(args, sigma, vt_mode, belief, axes, card)
        ctx = _Ctx(sigma, vt_mode, belief)
        expr_ctx: _Ctx | None = None
        stage: set[str] | None = set(union_cols)
    else:
        group = ret.get("group")
        keep = _set_semantics(args["pattern"], group, [b["expr"] for b in group["by"]]) \
            if group is not None and not ret.get("top_per") else None
        ctx, rel = _pattern_rel(args["pattern"], sigma, vt_mode, belief, axes, keep, card,
                                ends=ends if _order_free(ret) else None)
        expr_ctx, stage = ctx, None

    if "rows" in ret:
        seen: set[str] = set()
        bindings = []
        for r in ret["rows"]:
            _check_as(r["as"], seen)
            bindings.append((r["as"], _expr(r["expr"], expr_ctx, stage)))
        columns = [n for n, _ in bindings]
        rel = Project(rel, bindings=tuple(bindings), keep="all", sigma_=sigma)
        allowed = set(columns)
        completion = [c for c in columns if rel.out_schema.tau_of(c).base not in _JSON_TAUS]
        if expr_ctx is not None:
            completion += [v for v in ctx.vid_cols if v in rel.out_schema]
        order_ctx: _Ctx | None = expr_ctx
    else:
        rel, columns = _group(ctx, rel, ret["group"], expr_ctx is not None, stage)
        allowed = set(columns)
        completion = [c for c in columns if rel.out_schema.tau_of(c).base not in _JSON_TAUS]
        order_ctx = None

    tail = [SortKey(Col(c), "asc", "nulls_first") for c in completion]
    top = ret.get("top_per")
    if top is not None:
        # per partition, the first k rows under its keys (completed like the
        # answer's order, so the cut is the same in both executors)
        rel = TopK(rel, tuple(_expr(e, order_ctx, allowed) for e in top["by"]),
                   tuple(_order_keys(top["order_by"], order_ctx, allowed) + tail),
                   int(top["k"]), sigma_=sigma)
    keys = _order_keys(ret.get("order_by", []), order_ctx, allowed) + tail
    if keys:
        rel = Order(rel, tuple(keys), sigma_=sigma)
    if ret.get("limit"):
        rel = Limit(rel, int(ret["limit"]), sigma_=sigma)
    if "rows" in ret:
        rel = Project(rel, bindings=tuple((c, Col(c)) for c in columns), sigma_=sigma)
    return Compiled(rel, columns, sigma, plan_json(rel, sigma, columns))


# ---------------------------------------------------------------------------
# the step order
# ---------------------------------------------------------------------------

#: Reorder only when the given order is estimated this many times costlier.
REORDER_FACTOR = 8.0
#: Selectivity assumed for a predicate on one variable: equality, other.
_SEL_EQ, _SEL_OTHER = 0.05, 0.5


def _plain_steps(pat: dict[str, Any]) -> bool:
    """Steps whose meaning does not depend on the order they are bound in:
    no regular path, ordered or truncated step, no `succ` or lookup, and no
    variable-length step (its far end, the one bound by it, carries the
    depth the call may read)."""
    if pat.get("succ") or pat.get("lookups"):
        return False
    return all("regex" not in st and not any(k in st for k in ("order", "edge_where", "truncate"))
               and "min" not in (st.get("hops") or {})
               for st in pat.get("steps", []))


#: `plan_order`'s estimates by (pattern, counts, anchor, walks): step
#: positions, or None to keep the given order. Cleared when full.
_PLAN_ORDER_MEMO: dict[str, tuple[str, list[int]] | None] = {}
_PLAN_ORDER_MEMO_MAX = 1024


def plan_order(pat: dict[str, Any], card: dict[str, Any] | None,
               anchor: str | None = None, walks: bool = False
               ) -> tuple[str, list[dict[str, Any]]] | None:
    """A cheaper (anchor, step order) for a pattern, or None to keep the
    given one (a lookup's and a union branch's patterns too). Estimates: a
    node variable's rows are its labels' version
    counts (or every node), an edge step multiplies by the relationship's
    count over the bound end's rows (twice for `dir: both`; a fixed or
    bounded length raises it to that power), a closing step divides by the
    far end's rows, and a predicate on one variable keeps 5 % (equality) or
    50 %. The plan's cost is the sum of its intermediate row counts; the
    best anchor with a greedy order is taken only when the given order is
    estimated `REORDER_FACTOR` times costlier. A pattern with a uid list, no
    step, or any step or clause whose meaning depends on the order is kept
    as written. With `anchor`, the greedy order from that variable, whatever
    the given order costs (a caller restricting the anchor to a few uids:
    `paths` seeding a weight pattern from its search frontier); `walks` then
    lets a variable-length step move too, for a caller whose return reads no
    walk depth (the targets of a walk do not depend on the end it is read
    from)."""
    if card is None:
        return None
    if not _plain_steps(pat):
        # a variable-length step may move when the call reads no walk depth
        # (`card["walks_movable"]`, set by `compile_match`)
        relaxed = {**pat, "steps": [{k: v for k, v in st.items() if k != "hops"}
                                    for st in pat.get("steps", [])]}
        if not ((walks or card.get("walks_movable")) and _plain_steps(relaxed)):
            return None
    nodes = {n["var"]: n for n in pat.get("nodes", [])}
    steps = list(pat.get("steps", []))
    if not steps or (anchor is None and any(n.get("uids") for n in nodes.values())):
        return None
    for st in steps:
        for v in (st["from"], st["to"]):
            nodes.setdefault(v, {"var": v})
    # the estimate is a function of the pattern, the counts and the two
    # options alone: a repeated call reuses it (as step positions, so the
    # steps returned are this pattern's own)
    try:
        key: str | None = json.dumps([pat, card, anchor, walks], sort_keys=True, default=str)
    except (TypeError, ValueError):
        key = None
    if key is not None and key in _PLAN_ORDER_MEMO:
        got = _PLAN_ORDER_MEMO[key]
        return None if got is None else (got[0], [steps[i] for i in got[1]])

    def remember(got: tuple[str, list[int]] | None) -> tuple[str, list[dict[str, Any]]] | None:
        if key is not None:
            if len(_PLAN_ORDER_MEMO) >= _PLAN_ORDER_MEMO_MAX:
                _PLAN_ORDER_MEMO.clear()
            _PLAN_ORDER_MEMO[key] = got
        return None if got is None else (got[0], [steps[i] for i in got[1]])

    labels: dict[str, float] = card.get("labels") or {}
    rels: dict[str, float] = card.get("rels") or {}
    total_n = float(card.get("nodes") or sum(labels.values()) or 1.0)
    total_e = float(sum(rels.values()) or 1.0)

    def n_of(v: str) -> float:
        ls = nodes[v].get("labels")
        return max(1.0, float(sum(labels.get(x, 0) for x in ls)) if ls else total_n)

    sel: dict[str, float] = {v: 1.0 for v in nodes}
    for w in pat.get("where", []):
        vs = _vars_in(w)
        if len(vs) == 1:
            v = next(iter(vs))
            if v in sel:
                k = _eq_literals(w) if _implied_on() else 0
                if k:
                    # A value list on one variable (BI16's tag names):
                    # about one node per value, never above the 50 % rule
                    sel[v] *= min(_SEL_OTHER, k / n_of(v))
                else:
                    sel[v] *= _SEL_EQ if w.get("cmp") == "=" else _SEL_OTHER

    def fan(st: dict[str, Any], src: str) -> float:
        rt = st.get("rel_type")
        e = (sum(rels.get(r, 0) for r in rt) if isinstance(rt, list) else
             rels.get(rt, 0) if rt else total_e)
        f = max(float(e), 1.0) / n_of(src) * (2.0 if st.get("dir") == "both" else 1.0)
        hops = st.get("hops") or {"exact": 1}
        if "exact" in hops:
            return f ** int(hops["exact"])
        lo, hi = int(hops.get("min", 1)), hops.get("max")
        hi = min(3, int(hi) if hi is not None else 3)
        return max(sum(f ** d for d in range(lo, max(hi, lo) + 1)), 1e-9)

    def simulate(anchor: str, given: bool) -> tuple[float, list[int]]:
        rows = n_of(anchor) * sel[anchor]
        bound, cost, order = {anchor}, rows, []
        left = list(range(len(steps)))
        while left:
            best = None
            for i in left:
                st = steps[i]
                a, b = st["from"], st["to"]
                if a not in bound and b not in bound:
                    continue
                src, dst = (a, b) if a in bound else (b, a)
                est = rows * fan(st, src)
                est *= (1.0 / n_of(dst)) if dst in bound else sel[dst]
                if given:
                    best = (est, i)
                    break
                if best is None or est < best[0]:
                    best = (est, i)
            if best is None:
                return float("inf"), []
            rows = max(best[0], 1e-9)
            cost += rows
            order.append(best[1])
            left.remove(best[1])
            st = steps[best[1]]
            bound |= {st["from"], st["to"]}
        return cost, order


    def order_cost(anchor_: str, perm: list[int]) -> float | None:
        """The estimated cost of binding the steps in this order, or None
        when a step has no bound end when its turn comes."""
        rows = n_of(anchor_) * sel[anchor_]
        bound, cost = {anchor_}, rows
        for i in perm:
            st = steps[i]
            a, b = st["from"], st["to"]
            if a not in bound and b not in bound:
                return None
            src, dst = (a, b) if a in bound else (b, a)
            est = rows * fan(st, src)
            est *= (1.0 / n_of(dst)) if dst in bound else sel[dst]
            rows = max(est, 1e-9)
            cost += rows
            bound |= {a, b}
        return cost

    if anchor is not None:
        # every connected order from the anchor (up to six steps), else
        # greedy; "*": from every node variable, the cheapest
        anchors = [n["var"] for n in pat.get("nodes", [])] if anchor == "*" else [anchor]
        best_a: tuple[float, str, list[int]] | None = None
        for a in anchors:
            if len(steps) <= 6:
                from itertools import permutations

                for perm in permutations(range(len(steps))):
                    cost = order_cost(a, list(perm))
                    if cost is not None and (best_a is None or cost < best_a[0]):
                        best_a = (cost, a, list(perm))
            else:
                cost, order = simulate(a, False)
                if order and (best_a is None or cost < best_a[0]):
                    best_a = (cost, a, order)
        return remember((best_a[1], best_a[2]) if best_a else None)
    default_anchor = _anchor(pat.get("nodes", []))["var"]
    given_cost, _ = simulate(default_anchor, True)
    best: tuple[float, str, list[int]] | None = None
    for v in nodes:
        if v not in {n["var"] for n in pat.get("nodes", [])}:
            continue
        cost, order = simulate(v, False)
        if best is None or cost < best[0]:
            best = (cost, v, order)
    if best is None or not given_cost > REORDER_FACTOR * best[0]:
        return remember(None)
    return remember((best[1], best[2]))


# ---------------------------------------------------------------------------
# the second anchor
# ---------------------------------------------------------------------------

#: A pattern is grown from a second anchor only when its given plan is
#: estimated this many times costlier than the two sides and their join
#: parameter sets, three settings, untimed): estimated ratios 3.2-18 ran
#: 1.3-6x faster split, 1.4 and 2.3 ran slower or mixed.
SECOND_ANCHOR_FACTOR = 3.0


def _second_anchor_mode() -> str:
    """`GNOMON_MATCH_SECOND_ANCHOR` (read per call): `0` keeps every pattern's
    given plan; `force` takes the split whenever one is eligible, whatever
    the estimates say (the tests); otherwise the estimates decide."""
    v = os.environ.get("GNOMON_MATCH_SECOND_ANCHOR", "1").strip()
    return "off" if v == "0" else "force" if v == "force" else "on"


def _order_free(ret: dict[str, Any]) -> bool:
    """Whether the answer is the same for any row order of the binding table:
    no `collect` at any group stage (its list follows the rows), so the only
    order is the answer's own, completed by every bound version."""
    g = ret.get("group")
    while g is not None:
        if any(a.get("fn") == "collect" for a in g.get("aggs", [])):
            return False
        g = g.get("then")
    return True


#: the keys of a predicate `second_anchor` moves: comparisons, connectives,
#: null tests, columns, properties, literals
_PLAIN_KEYS = {"cmp", "l", "r", "bool", "not", "is_null", "col", "lit", "tau", "prop", "tprop"}


def _plain_pred(w: Any) -> bool:
    """A predicate of comparisons and boolean connectives over columns,
    properties and literals alone (no arithmetic, cast or function, which
    could raise on a row the given plan never tested)."""
    if isinstance(w, list):
        return all(_plain_pred(x) for x in w)
    if not isinstance(w, dict):
        return True
    if not set(w) <= _PLAIN_KEYS:
        return False
    return all(_plain_pred(v) for k, v in w.items() if k not in ("lit", "tau", "col", "prop", "tprop"))


def _second_anchor_shape(pat: Any) -> bool:
    """The shape `second_anchor` considers, from the pattern alone (cheap):
    one node pinned by uids, every other node free, no sub-pattern, lookup
    or `succ`, at least two steps."""
    if not isinstance(pat, dict) or not isinstance(pat.get("nodes"), list):
        return False
    if pat.get("optional") or pat.get("not") or pat.get("lookups") or pat.get("succ"):
        return False
    if len(pat.get("steps") or []) < 2:
        return False
    return sum(1 for n in pat["nodes"] if n.get("uids")) == 1


def _pinned_free_node(pat: dict[str, Any]) -> bool:
    """Whether a node without uids is pinned to literal values by a predicate
    of its own (`x = lit OR ...`, the implied ones included): the second
    anchor `second_anchor` needs (a cheap test before any count is read)."""
    free = {n["var"] for n in pat["nodes"] if not n.get("uids")}
    where = list(pat.get("where") or [])
    for w in where + _implied(where):
        vs = _vars_in(w)
        if len(vs) == 1 and next(iter(vs)) in free and _eq_literals(w):
            return True
    return False


def anchor_cardinalities(adapter: Any, args: dict[str, Any]) -> dict[str, Any] | None:
    """`cardinalities` plus, per relationship type, its edge versions and the
    distinct identities at its source and target ends (`match_rel_ends`),
    for a call `second_anchor` may split: current belief (at any `as_of_tt`),
    an instant or an overlap window (`co_valid` at most `["valid"]`), over a
    pattern of its shape; `None` otherwise, when the adapter cannot say, or
    under `GNOMON_MATCH_SECOND_ANCHOR=0`."""
    if _second_anchor_mode() == "off" or "union" in args:
        return None
    scope = args.get("scope") or {}
    if scope.get("belief", "current") != "current" or not set(scope.get("co_valid") or []) <= {"valid"} or \
            scope.get("mode", "overlap") != "overlap":
        return None
    if not _second_anchor_shape(args.get("pattern")) or not _order_free(args.get("return") or {}):
        return None
    if not _pinned_free_node(args["pattern"]):
        return None
    if ".depth" in json.dumps(args, default=str):
        return None
    # the store's generation (a property of the native handle)
    gen_of = getattr(adapter, "generation", None)
    gen_v = gen_of() if callable(gen_of) else gen_of
    gen = gen_v if isinstance(gen_v, int) and not isinstance(gen_v, bool) else None
    # the counts are the store's: kept per handle and generation (a call
    # pays a dictionary lookup, not a stats read and a conversion)
    held = _ANCHOR_CARDS.get(adapter) if gen is not None else None
    if held is not None and held[0] == gen:
        out: dict[str, Any] = dict(held[1])
    else:
        base = cardinalities(adapter)
        ends_of = getattr(adapter, "match_rel_ends", None)
        if base is None or ends_of is None:
            return None
        ends = {str(r): (float(c), float(s), float(d)) for r, c, s, d in ends_of()}
        out = {**base, "ends": ends}
        # the counts' text, once: part of `second_anchor`'s memo keys
        out["counts_key"] = json.dumps(out, sort_keys=True, default=str)
        if gen is not None:
            _ANCHOR_CARDS[adapter] = (gen, dict(out))
    if hasattr(adapter, "match_adjacency_rows"):
        out["probe"] = _anchor_probe(adapter, args, out.get("labels") or {}, gen)
        if gen is not None:
            try:
                out["probe_key"] = (id(adapter), gen, json.dumps(scope, sort_keys=True, default=str))
            except (TypeError, ValueError):
                pass
    return out


#: `anchor_cardinalities`' counts per handle: (generation, counts).
_ANCHOR_CARDS: "weakref.WeakKeyDictionary[Any, tuple[int, dict[str, Any]]]" = weakref.WeakKeyDictionary()
#: probe answers by (handle, generation, the anchor's node and predicates, the
#: scope, the direction): estimates only, never an answer; cleared when full
_PROBE_MEMO: dict[tuple[int, int, str, str], float | None] = {}


def _anchor_probe(adapter: Any, args: dict[str, Any], labels: dict[str, Any], gen: int | None = None) -> Any:
    """`second_anchor`'s probe: `(var, direction) -> rows`, the adjacency-index
    rows of the variable's identities in that direction (every relationship
    type and version: an upper bound of its first hop), or None. A variable
    pinned by uids reads them; one pinned by a literal predicate is found by
    a match on its label (under the call's scope; at most 64 identities, on
    a label of at most `PROBE_LABEL_MAX` versions). An answer is kept per
    handle and store generation, by the anchor's node, its predicates, the
    scope and the direction (a repeated call does not probe again)."""
    pat = args["pattern"]
    nodes = {n["var"]: n for n in pat["nodes"]}
    where = pat.get("where", [])

    def probe(var: str, way: str) -> float | None:
        if gen is None:
            return probe_store(var, way)
        try:
            spec = json.dumps([nodes[var], [w for w in where if _vars_in(w) == {var}], args["scope"]],
                              sort_keys=True, default=str)
        except (TypeError, ValueError):
            return probe_store(var, way)
        key = (id(adapter), gen, spec, way)
        if key not in _PROBE_MEMO:
            if len(_PROBE_MEMO) >= _PLAN_ORDER_MEMO_MAX:
                _PROBE_MEMO.clear()
            _PROBE_MEMO[key] = probe_store(var, way)
        return _PROBE_MEMO[key]

    def probe_store(var: str, way: str) -> float | None:
        node = nodes[var]
        uids = list(node.get("uids") or [])
        if not uids:
            ls = node.get("labels")
            if not ls or sum(int(labels.get(x, 0)) for x in ls) > PROBE_LABEL_MAX:
                return None
            preds = [w for w in where if _vars_in(w) == {var}]
            if not any(_eq_literals(w) for w in preds):
                return None
            q = {"pattern": {"nodes": [node], "steps": [], "where": preds},
                 "return": {"rows": [{"as": "u", "expr": {"col": f"{var}.uid"}}]},
                 "scope": args["scope"], "limit": 65}
            got = run(adapter, validate(q))
            if got["rows_total"] > 64:
                return None
            uids = [r["u"] if isinstance(r, dict) else r[0] for r in got["rows"]]
            if not uids:
                return 0.0
        return float(adapter.match_adjacency_rows(uids, way))

    return probe


def second_anchor(pat: dict[str, Any], card: dict[str, Any]) -> dict[str, Any] | None:
    """A uid-anchored pattern grown from two anchors and joined at one
    variable, when the estimates say so; None keeps the given plan.

    The shape: one node `A` pinned by uids (the given plan's anchor, its
    steps bound in the given order), and a node variable `J` whose removal
    cuts `A`'s part of the pattern from the rest. Side A is every step
    touching `A`'s part (bound from `A` in the given order, `J` under a
    fresh name); side B is every other step, each a single hop, grown from a
    node `B` of its own that a predicate pins to literal values (`x = lit
    OR ...`), in the cheapest order of its steps; the two sides join on
    `J`'s identity and version. Every predicate stays exact: one reading a
    side's variables alone is tested in that side, any other after the join
    (only predicates that cannot raise, so the rows they see do not
    matter).

    The estimate: a node variable's rows are its labels' version counts
    times its predicates' selectivity (`plan_order`'s, a literal list by
    its values), and a hop multiplies the rows by the relationship's
    edge versions over its distinct identities at the end it is read from
    (`card["ends"]`, every relationship of the pattern needed: none missing,
    none zero). A plan's cost is the sum of its intermediate rows; the
    split's is both sides' plus the join's rows.

    Averages miss skew (a hub country holds millions of a relationship's
    rows, another a few thousand), so the decision is taken in two passes.
    The first, on the averages alone (memoised by pattern, counts and mode),
    keeps a split only where it is estimated cheaper at all. The second
    replaces, for each anchor, its first hop's rows by a probe of the store
    (`card["probe"]`: the adjacency-index rows of the anchor's identities in
    that hop's direction, every type and version, an upper bound; a
    literal-pinned anchor's identities found by a match on its label, at
    most 64 of them on a label of at most `PROBE_LABEL_MAX` versions), and
    takes the split when the given plan is then estimated
    `SECOND_ANCHOR_FACTOR` times costlier. No probe, a probe that cannot
    answer, or a missing count keeps the given plan (`GNOMON_MATCH_SECOND_ANCHOR`:
    `0` never, `force` whenever the shape allows)."""
    mode = _second_anchor_mode()
    if mode == "off" or not _second_anchor_shape(pat):
        return None
    base = {k: v for k, v in card.items() if k not in ("probe", "probe_key", "counts_key")}
    # the first pass is a function of the pattern, the counts and the mode
    # alone: a repeated call reuses it
    try:
        counts = card.get("counts_key") or json.dumps(base, sort_keys=True, default=str)
        key: str | None = json.dumps([pat, mode], sort_keys=True, default=str) + "|" + counts
    except (TypeError, ValueError):
        key = None
    if key is not None and key in _SECOND_ANCHOR_MEMO:
        got = _SECOND_ANCHOR_MEMO[key]
    else:
        got = _second_anchor(pat, base, mode, None, 1.0)
        if key is not None:
            if len(_SECOND_ANCHOR_MEMO) >= _PLAN_ORDER_MEMO_MAX:
                _SECOND_ANCHOR_MEMO.clear()
            _SECOND_ANCHOR_MEMO[key] = got
    if got is None or mode == "force":
        return None if got is None else dict(got)
    probe = card.get("probe")
    if probe is None:
        return None
    # the second pass is a function of the first's key and the probes' store
    # (handle, generation) and scope: a repeated call reuses it
    key2 = (key, card.get("probe_key")) if key is not None and card.get("probe_key") is not None else None
    if key2 is not None and key2 in _SECOND_ANCHOR_MEMO2:
        got2 = _SECOND_ANCHOR_MEMO2[key2]
        return None if got2 is None else dict(got2)
    seen: dict[tuple[str, str], float | None] = {}

    def first(var: str, st: dict[str, Any]) -> float | None:
        d = st.get("dir", "out")
        way = d if st["from"] == var else _REVERSE[d]
        if (var, way) not in seen:
            try:
                seen[(var, way)] = probe(var, way)
            except Exception:  # noqa: BLE001 - a probe that cannot answer: no estimate
                seen[(var, way)] = None
        return seen[(var, way)]

    got2 = _second_anchor(pat, base, mode, first, SECOND_ANCHOR_FACTOR)
    if key2 is not None:
        if len(_SECOND_ANCHOR_MEMO2) >= _PLAN_ORDER_MEMO_MAX:
            _SECOND_ANCHOR_MEMO2.clear()
        _SECOND_ANCHOR_MEMO2[key2] = got2
    return None if got2 is None else dict(got2)


#: `second_anchor`'s first pass by (pattern, counts, mode). Cleared when full.
_SECOND_ANCHOR_MEMO: dict[str, dict[str, Any] | None] = {}
#: its second pass by (the first's key, the probes' handle, generation and
#: scope). Cleared when full.
_SECOND_ANCHOR_MEMO2: dict[tuple[str, Any], dict[str, Any] | None] = {}

#: A literal-pinned anchor is probed only on a label of at most this many
#: versions (the probe is a match over its label's rows).
PROBE_LABEL_MAX = 1_000_000


def _second_anchor(pat: dict[str, Any], card: dict[str, Any], mode: str,
                   first: Any, factor: float) -> dict[str, Any] | None:
    """`second_anchor`'s search: the cheapest split by the estimates, taken
    when the given plan costs `factor` times more; with `first`, each
    anchor's first hop rows are its probe's (None: no estimate)."""
    nodes = {n["var"]: n for n in pat["nodes"]}
    steps = list(pat["steps"])
    where = list(pat.get("where", []))
    if not all(_plain_pred(w) for w in where):
        return None
    for st in steps:
        if "regex" in st or any(k in st for k in ("order", "edge_where", "truncate")):
            return None
        if st["from"] not in nodes or st["to"] not in nodes:
            return None
        h = _hops(st.get("hops"))
        if isinstance(h, Exact) and h.k > 1:
            return None
        rt = st.get("rel_type")
        if not rt or (isinstance(rt, list) and not all(isinstance(r, str) for r in rt)):
            return None
    a = next(v for v, n in nodes.items() if n.get("uids"))
    labels: dict[str, float] = card.get("labels") or {}
    ends: dict[str, tuple[float, float, float]] = card.get("ends") or {}

    def rels_of(st: dict[str, Any]) -> list[str]:
        rt = st["rel_type"]
        return list(rt) if isinstance(rt, list) else [rt]

    if any(ends.get(r, (0.0, 0.0, 0.0))[1] <= 0 or ends.get(r, (0.0, 0.0, 0.0))[2] <= 0
           for st in steps for r in rels_of(st)):
        return None
    if any(n.get("labels") and not all(labels.get(x, 0) > 0 for x in n["labels"])
           for n in nodes.values()):
        return None
    total_n = float(card.get("nodes") or sum(labels.values()) or 1.0)

    def n_of(v: str) -> float:
        ls = nodes[v].get("labels")
        return max(1.0, float(sum(labels.get(x, 0) for x in ls)) if ls else total_n)

    sel: dict[str, float] = {v: 1.0 for v in nodes}
    pinned: set[str] = set()
    for w in where:
        vs = _vars_in(w)
        if len(vs) == 1 and next(iter(vs)) in sel:
            # (a predicate on an edge variable is not a node's selectivity)
            v = next(iter(vs))
            k = _eq_literals(w)
            if k:
                sel[v] *= min(_SEL_OTHER, k / n_of(v))
                pinned.add(v)
            else:
                sel[v] *= _SEL_EQ if w.get("cmp") == "=" else _SEL_OTHER

    def fan(st: dict[str, Any], src: str) -> float:
        d = st.get("dir", "out")
        f = 0.0
        for r in rels_of(st):
            c, ns, nd = ends[r]
            if d == "both":
                f += c / ns + c / nd
            elif (d == "out") == (src == st["from"]):
                f += c / ns
            else:
                f += c / nd
        hops = st.get("hops") or {"exact": 1}
        if "exact" in hops:
            return f ** int(hops["exact"])
        lo, hi = int(hops.get("min", 1)), hops.get("max")
        hi = min(3, int(hi) if hi is not None else 3)
        return max(sum(f ** k for k in range(lo, max(hi, lo) + 1)), 1e-9)

    def cost_of(anchor: str, order: list[dict[str, Any]]) -> tuple[float, float] | None:
        """The cost and the last rows of binding `order` from `anchor`, each
        step taken as `pattern` takes them (the first with a bound end); with
        `first`, the anchor's first hop reads its probed rows (a longer
        step: those rows, then the average fan for each further hop)."""
        rows = n_of(anchor) * sel[anchor]
        bound, cost, todo = {anchor}, rows, list(order)
        while todo:
            i = next((k for k, st in enumerate(todo) if st["from"] in bound or st["to"] in bound), None)
            if i is None:
                return None
            st = todo.pop(i)
            src, dst = (st["from"], st["to"]) if st["from"] in bound else (st["to"], st["from"])
            if first is not None and bound == {anchor}:
                deg = first(anchor, st)
                if deg is None:
                    return None
                hops = st.get("hops") or {"exact": 1}
                f1 = fan({**st, "hops": {"exact": 1}}, src)
                if "exact" in hops:
                    rows = deg * f1 ** (int(hops["exact"]) - 1)
                else:
                    lo, hi = int(hops.get("min", 1)), hops.get("max")
                    hi = min(3, int(hi) if hi is not None else 3)
                    rows = sum(1.0 if k == 0 else deg * f1 ** (k - 1) for k in range(lo, max(hi, lo) + 1))
            else:
                rows *= fan(st, src)
            rows *= (1.0 / n_of(dst)) if dst in bound else sel[dst]
            rows = max(rows, 1e-9)
            cost += rows
            bound |= {st["from"], st["to"]}
        return cost, rows

    given = cost_of(a, steps)
    if given is None:
        return None
    best: tuple[float, str, str, list[int], list[int]] | None = None
    for j in nodes:
        if j == a:
            continue
        # A's part: the variables reached from A without passing through J
        part, grow = {a}, True
        while grow:
            grow = False
            for st in steps:
                ends_ = {st["from"], st["to"]}
                if ends_ & part and j not in ends_ and not ends_ <= part:
                    part |= ends_
                    grow = True
        side_a = [i for i, st in enumerate(steps) if {st["from"], st["to"]} & part]
        side_b = [i for i in range(len(steps)) if i not in side_a]
        vars_b = {v for i in side_b for v in (steps[i]["from"], steps[i]["to"])}
        if not side_b or j not in vars_b or any(v in part for v in vars_b):
            continue
        if not any(j in {steps[i]["from"], steps[i]["to"]} for i in side_a):
            continue
        if not all(_single_hop(steps[i]) for i in side_b):
            continue
        cost_a = cost_of(a, [steps[i] for i in side_a])
        if cost_a is None:
            continue
        for bv in sorted(vars_b - {j}):
            if bv not in pinned:
                continue
            from itertools import permutations

            # every order of a short side (up to four steps), else as given
            perms = permutations(side_b) if len(side_b) <= 4 else [tuple(side_b)]
            for perm in perms:
                got = cost_of(bv, [steps[i] for i in perm])
                if got is None:
                    continue
                join_rows = max(cost_a[1] * got[1] / n_of(j), 1e-9)
                total = cost_a[0] + got[0] + join_rows
                if best is None or total < best[0]:
                    best = (total, j, bv, side_a, list(perm))
    if best is None:
        return None
    if mode != "force" and not given[0] > factor * best[0]:
        return None
    return {"a": a, "j": best[1], "b": best[2], "side_a": best[3], "side_b": best[4],
            "given_cost": given[0], "cost": best[0]}


def _single_hop(st: dict[str, Any]) -> bool:
    h = _hops(st.get("hops"))
    return isinstance(h, Exact) and h.k == 1


def _second_anchor_rel(ctx: _Ctx, pat: dict[str, Any], where: list[Any],
                       split: dict[str, Any]) -> Node:
    """The plan of `second_anchor`'s split: side B grown from its anchor,
    side A from the uid anchor with `J` under a fresh name, joined on `J`'s
    identity and then its version, the predicates over both sides and the
    distinct-edge tests across them after the join. The bound versions'
    order (`vid_cols`, which completes the answer's order) is the given
    plan's, so ties fall the same way."""
    import copy

    nodes = {n["var"]: n for n in pat["nodes"]}
    j, bv, a = split["j"], split["b"], split["a"]
    # every unnamed single hop named up front, so the given plan's binding
    # order and this plan's name the same edge variables
    steps = []
    for st in pat["steps"]:
        if not st.get("var") and _single_hop(st):
            st = {**st, "var": ctx.name("e")}
        steps.append(st)
    # the given plan's binding order, from a dry run of it
    dry = copy.deepcopy(ctx)
    _Builder(dry, None).pattern(pat["nodes"], steps, where)
    order = list(dry.vid_cols)
    # each side's variables: its steps' ends and edge variables
    vars_b = {v for i in split["side_b"] for v in (steps[i]["from"], steps[i]["to"], steps[i].get("var")) if v}
    vars_a = {v for i in split["side_a"] for v in (steps[i]["from"], steps[i]["to"], steps[i].get("var")) if v} | {a}
    # J's name in side A: a pattern variable's form (a letter first), one no
    # variable of the call uses
    taken = set(nodes) | set(ctx.kinds) | {st.get("var") for st in steps}
    k, jj = 0, f"{j}_j"
    while jj in taken:
        k += 1
        jj = f"{j}_j{k}"
    ren = lambda v: jj if v == j else v  # noqa: E731
    start_iv = ctx.run_iv
    where_b = [w for w in where if _vars_in(w) and _vars_in(w) <= vars_b]
    where_a = [w for w in where if w not in where_b and _vars_in(w) <= vars_a - {j}]
    where_x = [w for w in where if w not in where_b and w not in where_a]
    names = [n["var"] for n in pat["nodes"]]
    bb = _Builder(ctx, None)
    bb.pattern([nodes[v] for v in names if v in vars_b],
               [steps[i] for i in split["side_b"]], where_b, anchor_var=bv)
    iv_b, ctx.run_iv = ctx.run_iv, start_iv
    ba = _Builder(ctx, None)
    a_nodes = [nodes[v] for v in names if v in vars_a and v != j]
    a_nodes.append({**nodes[j], "var": jj})
    ba.pattern(a_nodes, [{**steps[i], "from": ren(steps[i]["from"]), "to": ren(steps[i]["to"])}
                         for i in split["side_a"]], where_a, anchor_var=a)
    on = ((ctx.uid_col[jj], ctx.uid_col[j]),)
    rel: Node
    rel = Join(ba.rel, bb.rel, on, "inner", sigma_=ctx.sigma)
    if ctx.fragmented:
        # overlap of the two sides' (as `_carry` narrows it by a step), and
        # a pair whose intervals share no instant is dropped
        iv_a = ctx.run_iv
        assert iv_a is not None and iv_b is not None, "each side binds a version"
        lo, hi = ctx.name("cs"), ctx.name("ce")
        rel = Project(rel, bindings=(
            (lo, If(Cmp(">", Col(iv_a[0]), Col(iv_b[0])), Col(iv_a[0]), Col(iv_b[0]))),
            (hi, If(Cmp("<", Col(iv_a[1]), Col(iv_b[1])), Col(iv_a[1]), Col(iv_b[1])))),
            keep="all", sigma_=ctx.sigma)
        rel = Filter(rel, Cmp("<", Col(lo), Col(hi)), sigma_=ctx.sigma)
        ctx.run_iv = (lo, hi)
    bj = _Builder(ctx, rel)
    bj._filter(Cmp("=", Col(f"{jj}.vid"), Col(f"{j}.vid")))
    for ea in ba.single_edges:
        for eb in bb.single_edges:
            bj._filter(Cmp("!=", Col(f"{ea}.eid"), Col(f"{eb}.eid")))
    bj.pending = [(_vars_in(w), w) for w in where_x]
    bj._flush(join=False)
    if bj.pending:
        raise InvalidArgError("match: a predicate names an unbound variable",
                              vars=sorted(set().union(*(n for n, _ in bj.pending))))
    ctx.vid_cols[:] = [v for v in order if v in ctx.vid_cols] + [v for v in ctx.vid_cols if v not in order]
    return bj.rel


def _orderable(spec: Any, movable: bool | None = None) -> bool:
    """Whether some pattern of the argument (the main one, a union branch, a
    lookup's) is one `plan_order` may reorder (with walks, when the
    argument reads no depth)."""
    if movable is None:
        movable = _reads_no_depth(spec) if isinstance(spec, dict) else False
    if isinstance(spec, dict):
        if "nodes" in spec and "steps" in spec and len(spec.get("steps") or []) >= 1 and \
                not any(n.get("uids") for n in spec["nodes"]):
            relaxed = {**spec, "steps": [{k: v for k, v in st.items() if k != "hops"}
                                         for st in spec.get("steps", [])]}
            if _plain_steps(spec) or (movable and _plain_steps(relaxed)):
                return True
        return any(_orderable(v, movable) for k, v in spec.items() if k not in ("return", "scope"))
    if isinstance(spec, list):
        return any(_orderable(v, movable) for v in spec)
    return False


def cardinalities(adapter: Any) -> dict[str, Any] | None:
    """The counts `plan_order` reads: node versions per label and edge
    versions per relationship type, from the adapter (`None` when it cannot
    say)."""
    labels = getattr(adapter, "match_label_counts", None)
    stats = getattr(adapter, "stats", None)
    if labels is None or stats is None:
        return None
    s = stats()
    return {"labels": dict(labels()), "rels": dict(s.get("rel_type_counts") or {}),
            "nodes": s.get("n_node_versions")}


# ---------------------------------------------------------------------------
# the plan as the engine reads it
# ---------------------------------------------------------------------------

def _hops_json(h) -> dict[str, Any]:
    if isinstance(h, Exact):
        return {"exact": h.k}
    if isinstance(h, Bounded):
        return {"bounded": [h.a, h.b]}
    return {"unbounded": h.a}


def plan_json(root: Node, sigma: Sigma, columns: list[str]) -> dict[str, Any]:
    """A DAG of nodes by id (a subtree reached twice is one entry), each with
    its op, fields and input ids, plus Σ and the output columns."""
    nodes: list[dict[str, Any]] = []
    ids: dict[str, int] = {}

    def visit(n: Node) -> int:
        key = n.node_digest
        if key in ids:
            return ids[key]
        inputs = [visit(i) for i in n.inputs]
        d: dict[str, Any] = {"op": n.op, "inputs": inputs}
        if isinstance(n, NodeScan):
            d.update({"as": n.as_, "labels": list(n.labels) if n.labels else None,
                      "uids": list(n.uids) if n.uids else None,
                      "belief": n.belief, "vt_mode": n.vt_mode})
        elif isinstance(n, EdgeScan):
            d.update({"as": n.as_, "rel_types": list(n.rel_types) if n.rel_types else None,
                      "belief": n.belief, "vt_mode": n.vt_mode})
        elif isinstance(n, Expand):
            d.update({"from": n.from_column, "into": n.into, "hops": _hops_json(n.hops),
                      "rel_type": n.rel_type, "dir": n.dir, "edge_var": n.edge_var})
        elif isinstance(n, Filter):
            d["pred"] = n.pred.to_json()
        elif isinstance(n, TypeConstraint):
            d.update({"var": n.var, "labels": list(n.labels) if n.labels else None})
        elif isinstance(n, Project):
            d.update({"bindings": [[k, e.to_json()] for k, e in n.bindings],
                      "keep": n.keep})
        elif isinstance(n, Join):
            d.update({"on": [list(p) for p in n.on], "type": n.join_type})
        elif isinstance(n, Aggregate):
            d.update({"group_by": [[k, e.to_json()] for k, e in n.group_by],
                      "aggregates": [a.to_json() for a in n.aggregates]})
        elif isinstance(n, Order):
            d["keys"] = [k.to_json() for k in n.keys]
        elif isinstance(n, Limit):
            d["n"] = n.n
        elif isinstance(n, Union):
            pass                                   # the inputs, in order
        elif isinstance(n, Succ):
            d.update({"of": n.of, "into": n.into, "kind": n.kind, "axis": n.axis,
                      "dir": n.dir, "belief": n.belief, "vt_mode": n.vt_mode})
        elif isinstance(n, OrderedReach):
            d.update({"from": n.from_column, "into": n.into, "src": n.src, "dst": n.dst,
                      "time": n.time, "lo": n.lo, "hi": n.hi, "order": n.order,
                      "limit": n.limit, "key": n.key, "key_desc": n.key_desc})
        elif isinstance(n, Walk):
            d.update({"from": n.from_column, "into": n.into, "regex": json.loads(n.regex)})
        elif isinstance(n, TopK):
            d.update({"by": [e.to_json() for e in n.partition_by],
                      "keys": [k.to_json() for k in n.keys], "n": n.n})
        elif isinstance(n, CoValidExpand):
            d.update({"from": n.from_column, "into": n.into, "hops": _hops_json(n.hops),
                      "rel_type": n.rel_type, "dir": n.dir, "belief": n.belief,
                      "from_iv": list(n.from_iv) if n.from_iv else None})
        elif isinstance(n, CoValidJoin):
            d.update({"on": [list(p) for p in n.on], "type": n.join_type,
                      "left_iv": list(n.left_iv), "right_iv": list(n.right_iv),
                      "out_iv": list(n.out_iv)})
        elif isinstance(n, Collapse):
            d.update({"by": list(n.by), "mins": list(n.mins)})
        else:  # pragma: no cover - the compiler emits only the nodes above
            raise InvalidArgError(f"match: no engine form for {n.op}")
        ids[key] = len(nodes)
        nodes.append(d)
        return ids[key]

    root_id = visit(root)
    return {"format": 1, "nodes": nodes, "root": root_id,
            "sigma": {"t_v": [[i.start, i.end] for i in sigma.t_v], "t_b": sigma.t_b},
            "columns": columns}


# ---------------------------------------------------------------------------
# execution
# ---------------------------------------------------------------------------

#: Process-wide route counters, for receipts: never part of a payload.
ROUTES: dict[str, int] = {"native": 0, "reference": 0}
FALLBACKS: dict[str, int] = {}
#: the engine's reason for the last declined plan, for receipts
LAST_DECLINE: list[str] = [""]


def _match_kernel(adapter: Any) -> bool:
    """The engine's executor is the default wherever it exists and the handle
    was not opened with `GNOMON_MATCH_KERNEL=python`."""
    designs = getattr(adapter, "designs", None)
    if designs is None or not hasattr(adapter, "match_kernel"):
        return False
    return bool(designs().get("match_kernel", False))


def _fallback(reason: str) -> None:
    FALLBACKS[reason] = FALLBACKS.get(reason, 0) + 1


def _reference_rows(c: Compiled, adapter: Any, bypass: str | None) -> list[dict[str, Any]]:
    from gnomon.plan.eval import evaluate_core

    if bypass is not None:
        rel = evaluate_core(c.root, adapter, admit_plan=False, bypass_admission=bypass)
    else:
        rel = evaluate_core(c.root, adapter)
    return rel.rows()


def _page(rows_total: int, rows: list[dict[str, Any]], offset: int, limit: int,
          columns: list[str]) -> dict[str, Any]:
    truncated = offset + len(rows) < rows_total
    return {"columns": columns, "rows": rows, "rows_total": rows_total,
            "truncated": truncated,
            "cursor": str(offset + len(rows)) if truncated else None}


def _offset(cursor: str | None) -> int:
    try:
        offset = int(cursor) if cursor else 0
    except ValueError:
        raise InvalidArgError(f"match: bad cursor {cursor!r}") from None
    if offset < 0:
        raise InvalidArgError("match: bad cursor")
    return offset


def run(adapter: Any, args: dict[str, Any], *, route: str | None = None,
        reference_bypass: str | None = None) -> dict[str, Any]:
    """Execute a filled, validated argument and return the payload. `route`
    forces `"native"` or `"reference"` (tests and the ablation harness);
    `None` takes the native executor wherever the handle allows it."""
    from gnomon.temporal.algebra import _canonicalize_floats

    card = cardinalities(adapter) if _orderable(args) else None
    ends = anchor_cardinalities(adapter, args) if card is None else None
    c = compile_match(args, card, ends)
    offset, limit = _offset(args.get("cursor")), int(args.get("limit", DEFAULT_LIMIT))
    want_native = _match_kernel(adapter) if route is None else route == "native"
    got = None
    if want_native:
        if not hasattr(adapter, "match_kernel"):
            _fallback("backend")
        else:
            got = adapter.match_kernel(json.dumps(c.doc), offset, limit)
            if got is None:
                _fallback("declined")
                why = getattr(adapter, "match_last_decline", None)
                if why is not None:
                    LAST_DECLINE[0] = why()
    if got is not None:
        ROUTES["native"] += 1
        payload = _page(int(got["rows_total"]), got["rows"], offset, limit, c.columns)
    else:
        ROUTES["reference"] += 1
        rows = _reference_rows(c, adapter, reference_bypass)
        payload = _page(len(rows), rows[offset:offset + limit], offset, limit, c.columns)
    return _canonicalize_floats(payload)


def run_pairs(adapter: Any, args: dict[str, Any]) -> tuple | None:
    """A filled argument whose answer is a weight pattern's (`src`, `dst`,
    `w`), in the engine column-wise (`match_pairs`): `(src ids, dst ids,
    w_kind, w_int, w_f64)`, or None (no engine, or a shape it refuses: the
    caller reads rows)."""
    if not _match_kernel(adapter) or not hasattr(adapter, "match_pairs"):
        return None
    card = cardinalities(adapter) if _orderable(args) else None
    c = compile_match(args, card)
    if not {"src", "dst", "w"} <= set(c.columns):
        return None
    got = adapter.match_pairs(json.dumps(c.doc))
    if got is not None:
        ROUTES["native"] += 1
    return got


def match(adapter: Any, args: dict[str, Any], **kw: Any) -> dict[str, Any]:
    """The operator: validate, compile, execute."""
    return run(adapter, validate(args), **kw)


def bound_scope(args: dict[str, Any]) -> dict[str, Any]:
    """The call's scope with its `$name` parameters bound, from the argument
    as the registry fills it (parameters unbound); `{}` when it has none."""
    params = args.get("params") or {}
    scope = args.get("scope")
    if not isinstance(scope, dict):
        return {}
    bound = _bind_params(scope, params if isinstance(params, dict) else {})
    return bound if isinstance(bound, dict) else {}


def scope_sigma(args: dict[str, Any]) -> tuple[Sigma, str]:
    """Σ and the valid-time keying mode of one registered call: an instant
    reads `instant`, a window `overlap` or `event`."""
    scope = bound_scope(args)
    sigma, mode = _sigma_of(scope)
    return sigma, "instant" if set(scope["vt"]) == {"instant"} else mode


# ---------------------------------------------------------------------------
# registration
# ---------------------------------------------------------------------------

#: The schema `algebra.validate_args` checks: the top-level keys only. The
#: `$name` parameters bind before the full check, so a bound value may stand
#: where `ARGS_SCHEMA` wants another type; `validate` (the registered
#: validator, and again inside the kernel) binds them and then checks the
#: full schema with its `$defs`. No defaults here: the echoed arguments are
#: the caller's, and `validate` fills the defaults.
REGISTERED_SCHEMA: dict[str, Any] = {
    "type": "object", "additionalProperties": False,
    "required": list(ARGS_SCHEMA["required"]),
    "properties": {k: {} for k in ARGS_SCHEMA["properties"]},
}

#: The payload's top-level fields.
OUTPUT_FIELDS = ("columns", "rows", "rows_total", "truncated", "cursor")

DESCRIPTION = (
    "Typed pattern matching over the bi-temporal graph: node variables, steps "
    "of one hop, k hops, a bounded closure or a regular path expression, "
    "predicates, optional and negated sub-patterns, lookups and unions, under "
    "a scope (a valid instant or window, as_of_tt and a belief mode), with a "
    "fused group, order and limit. The call compiles to one plan.")


def _register() -> None:
    from gnomon.temporal.algebra import REGISTRY, OperatorSpec

    REGISTRY["match"] = OperatorSpec(
        name="match", fn=match, args_schema=REGISTERED_SCHEMA,
        description=DESCRIPTION, validators=[validate],
        output_fields=OUTPUT_FIELDS)


_register()


__all__ = ["ARGS_SCHEMA", "Compiled", "DESCRIPTION", "FALLBACKS", "OUTPUT_FIELDS",
           "REGISTERED_SCHEMA", "ROUTES", "bound_scope", "compile_match", "match",
           "plan_json", "run", "scope_sigma", "validate"]
