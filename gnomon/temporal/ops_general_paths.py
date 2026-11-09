"""`paths`: shortest, optimal and enumerated paths (engine-level only).

A generalisation of `temporal_paths` and
`temporal_reachability` (whose kernels stay, and answer `metric: "arrival"`
here). It unlocks the path rows of the established benchmarks: LDBC SNB
IC13 (hop distance), IC14 (all shortest paths scored by an interaction
pattern; one cheapest path with pattern weights), BI15, BI19, BI20
(weighted paths whose weights or targets come from a pattern), and
FinBench TCR3 (shortest path over edges inside a time window) and TCR5
(every time-increasing trace of 1..3 transfers).

THE GRAPH. Unless `time_respecting` is set, the graph is the snapshot
`G(t_valid)` under `as_of_tt`: believed edge versions valid at the instant,
restricted by `rel_types`, by `edge_where` (property predicates, the type-fit
rule) and by `edge_window` (`t_a <= vt_s < t_b` on the edge's own
start). A path is a **vertex sequence** and is node-simple: several edges
between the same two vertices are one step, whose weight is the smallest of
theirs. `dir` reads the stored direction (`out`), its reverse (`in`) or
both.

With `time_respecting` (`non_strict` or `strict`) the graph is instead the
believed edge versions with `vt_s` in `window` (and, with `t_valid`, valid
at that instant too: a transfer deleted by then is not a step), and
consecutive steps must
have non-decreasing (strictly increasing) `vt_s`; a vertex sequence is a
path when some choice of its edges is so ordered, which the earliest
feasible edge per step decides. Only `all_paths` and `k_paths` enumerate
those; the arrival metrics remain `temporal_reachability`'s and
`temporal_paths`'.

METRICS. `hops` counts steps. `weight` sums positive step weights: a
property of the edge (`{"prop": key}`), or a pattern (`{"match": spec}`, a
`match` argument whose rows carry `src`, `dst` and `w`; pairs it does not
name are excluded or take `default`). Sums are exact: integers as integers,
floats as rationals, reported once rounded, so ties are ties and an order
never depends on summation order. `arrival` delegates to the two temporal
operators. A `score` (same forms as `weight`) is summed along each returned
path for ordering and output, without deciding optimality (IC14 v1).

WINDOW. With `valid_window` instead of `t_valid` (engine-only), the graph is
the believed edge versions overlapping the window, and a path is valid when
all of its edges and nodes are valid at one instant of it:
`distance` is the least cost
over the instants, `one_path` and `all_optimal` take the paths optimal at
some instant where the minimum is reached, and `all_paths` / `k_paths`
enumerate the paths valid at some instant (with `time_respecting`, through
edges whose order holds and which share that instant).

LOADING. A bounded search from known sources (an enumeration, or a hop
distance) reads the graph frontier-first: per round, the edges incident to
the frontier (`edges_columnar(touching_ids=...)`), at most `max_hops`
rounds, and a hop distance stops once every target is reached. Every vertex
the search expands has all its edges read, so the answer is the
whole-graph one.

TRUNCATION. `truncate: {limit, by, dir}` (engine-only) keeps, per vertex, the
first `limit` of its edges in the traversal direction by `vt_s` or a numeric
property, before `edge_where`, `edge_window` and the time window (FinBench's
truncationLimit / truncationOrder).

RESULTS, per (source, target) pair, or over all pairs with
`pairs: "min_over_pairs"` (the pairs at the global minimum):
`distance`; `one_path` (an optimal path: from the target, repeatedly the
optimal predecessor with the smallest uid); `all_optimal` (every optimal
path, in uid-sequence order, counted exactly); `all_paths` (every simple
path of `min_hops..max_hops` steps); `k_paths` (the first `k` of those per
pair by (cost, hops, vertex sequence)). A source is its own target at
distance 0 only when named among the targets; with no targets every
reached vertex but the source is one.
"""

from __future__ import annotations

import json
import math
import os
from bisect import bisect_left, bisect_right
from collections import defaultdict, deque
from fractions import Fraction
from heapq import heappop, heappush, nsmallest
from itertools import repeat
from typing import Any

import numpy as np

from gnomon.core.errors import CostError, InvalidArgError, LimitError, NotFoundError
from gnomon.core.model import OPEN_END
from gnomon.storage.base import StorageAdapter
from gnomon.temporal.algebra import (
    AS_OF_TT,
    CURSOR,
    LIMIT,
    TIMESTAMP,
    UID_LIST,
    WINDOW,
    check_window,
    operator,
    paginate,
    required,
)
from gnomon.temporal.guardrails import window_fraction
from gnomon.temporal.props import PROP_CMPS, SKIP, is_number, matches, parse_props

MAX_HOPS_ENUM = 6
MAX_K = 100
MAX_EXPANSIONS = 2_000_000
MAX_PATTERN_ROWS = 1_000_000
#: An inline weight or score pattern read column-wise (`run_pairs`: five
#: arrays, about 25 bytes a pair, never a row dict) has its own guard, sized
#: for memory rather than for Python rows (SNB SF10's IC14 and BI19 weight
#: tables pass a million pairs; SF1's has 357,700). A runaway pattern is still
#: stopped, and every pattern read as rows keeps `MAX_PATTERN_ROWS`.
MAX_PAIR_ROWS = 16_000_000

ENDPOINTS = {
    "type": "object",
    "properties": {
        "uids": UID_LIST,
        "match": {"type": "object",
                  "description": "a `match` argument whose rows carry a `uid` column"},
    },
    "additionalProperties": False,
    "description": "{uids: [...]} or {match: spec}",
}
PAIR_VALUES = {
    "type": "object",
    "properties": {
        "prop": {"type": "string", "minLength": 1},
        "match": {"type": "object",
                  "description": "a `match` argument whose rows carry src, dst, w"},
        "missing": {"type": "string", "enum": ["exclude", "default"],
                    "default": "exclude"},
        "default": {"type": "number"},
    },
    "additionalProperties": False,
}
#: engine-only arguments: no default, so a call without them is unchanged
VALID_WINDOW = {
    "type": "object",
    "properties": {"t_a": {"type": "integer"}, "t_b": {"type": "integer"}},
    "required": ["t_a", "t_b"],
    "additionalProperties": False,
    "description": "the window form: a path is valid at one shared instant of [t_a, t_b)",
}
TRUNCATE = {
    "type": "object",
    "properties": {
        "limit": {"type": "integer", "minimum": 1},
        "by": {"anyOf": [{"type": "string", "enum": ["vt_s"]},
                         {"type": "object", "properties": {"prop": {"type": "string",
                                                                    "minLength": 1}},
                          "required": ["prop"], "additionalProperties": False}]},
        "dir": {"type": "string", "enum": ["asc", "desc"]},
    },
    "required": ["limit", "by", "dir"],
    "additionalProperties": False,
    "description": "per vertex, the first `limit` edges by `by`, before the filters",
}
EDGE_WHERE = {
    "type": "array",
    "items": {"type": "object",
              "properties": {"prop": {"type": "string", "minLength": 1},
                             "cmp": {"type": "string", "enum": PROP_CMPS},
                             "value": {}},
              "required": ["prop", "cmp", "value"],
              "additionalProperties": False},
    "maxItems": 8,
}


def _validators(args: dict[str, Any]) -> None:
    for side in ("sources", "targets"):
        spec = args.get(side)
        if spec is not None and len(spec) != 1:
            raise InvalidArgError(f"{side} takes exactly one of uids or match")
    tr, res, metric = args.get("time_respecting"), args["result"], args["metric"]
    if tr is not None:
        if res not in ("all_paths", "k_paths"):
            raise InvalidArgError("time_respecting enumerates: result all_paths or k_paths")
        if args.get("window") is None:
            raise InvalidArgError("time_respecting reads the edges with vt_s in `window`")
        check_window(args)
    elif metric != "arrival" and args.get("t_valid") is None and \
            args.get("valid_window") is None:
        raise InvalidArgError("paths over a snapshot needs `t_valid`")
    vw = args.get("valid_window")
    if vw is not None:
        if args.get("t_valid") is not None:
            raise InvalidArgError("valid_window and t_valid are exclusive: one is the "
                                  "window form, the other the instant form")
        if metric == "arrival":
            raise InvalidArgError("valid_window: metric arrival delegates; it takes window")
        if not vw["t_a"] < vw["t_b"]:
            raise InvalidArgError("valid_window requires t_a < t_b")
        if metric == "weight" and args.get("max_hops") is not None and \
                res in ("distance", "one_path", "all_optimal"):
            raise InvalidArgError("valid_window: a hop bound on weighted optimal paths is "
                                  "not supported")
    if args.get("truncate") is not None and metric == "arrival":
        raise InvalidArgError("truncate cuts the searched graph; metric arrival delegates")
    if metric == "weight" and args.get("weight") is None:
        raise InvalidArgError("metric weight needs `weight`")
    if metric != "weight" and args.get("weight") is not None:
        raise InvalidArgError("`weight` is only read by metric weight")
    for name in ("weight", "score"):
        w = args.get(name)
        if w is not None:
            if ("prop" in w) == ("match" in w):
                raise InvalidArgError(f"{name} takes exactly one of prop or match")
            if w.get("missing", "exclude") == "default" and "default" not in w:
                raise InvalidArgError(f"{name}: missing 'default' needs a default")
    if args.get("score") is not None and "prop" in args["score"]:
        raise InvalidArgError("score is a pattern ({match}): a per-pair value")
    enum = res in ("all_paths", "k_paths") and metric != "arrival"
    mx, mn = args.get("max_hops"), args.get("min_hops")
    if enum:
        if mx is None:
            raise InvalidArgError(f"{res} enumerates: it needs max_hops")
        if mx > MAX_HOPS_ENUM:
            raise LimitError(f"max_hops capped at {MAX_HOPS_ENUM} for {res}")
    elif mn is not None:
        raise InvalidArgError("min_hops bounds an enumeration (all_paths, k_paths)")
    if mn is not None and mx is not None and mn > mx:
        raise InvalidArgError("min_hops > max_hops")
    if metric == "weight" and mx is not None and res in ("one_path", "all_optimal"):
        raise InvalidArgError("a hop bound on weighted optimal paths: result distance only")
    if res == "k_paths" and args.get("k") is None:
        raise InvalidArgError("k_paths needs k")
    if args.get("order") == "score_desc" and (args.get("score") is None or
                                             res not in ("all_optimal", "all_paths", "k_paths")):
        raise InvalidArgError("order score_desc needs a score, over enumerated paths")
    if args["pairs"] == "min_over_pairs" and res not in ("distance", "one_path", "all_optimal"):
        raise InvalidArgError("min_over_pairs selects among optimal results")
    if metric == "arrival":
        if res not in ("distance", "k_paths") or tr is not None:
            raise InvalidArgError("metric arrival: result distance (temporal_reachability) "
                                  "or k_paths (temporal_paths)")
        if args.get("window") is None:
            raise InvalidArgError("metric arrival reads a window")
        check_window(args)
    if args.get("edge_window") is not None:
        w = args["edge_window"]
        if not w["t_a"] < w["t_b"]:
            raise InvalidArgError("edge_window requires t_a < t_b")


# --------------------------------------------------------------------------- #
# inputs                                                                       #
# --------------------------------------------------------------------------- #

def _match_rows(adapter: StorageAdapter, spec: dict[str, Any], args: dict[str, Any],
                need: tuple[str, ...]) -> list[dict[str, Any]]:
    """Every row of an inline `match`, as dicts; its scope defaults to the
    call's own (the snapshot instant and belief time). The pattern runs once:
    validated as the operator's argument, then read in one page of up to
    `MAX_PATTERN_ROWS + 1` rows (a cursor would run it again per page)."""
    from gnomon.temporal.ops_match import run, validate

    q = dict(spec)
    if "scope" not in q:
        scope: dict[str, Any] = {"as_of_tt": args["as_of_tt"]}
        if args.get("t_valid") is not None:
            scope["vt"] = {"instant": args["t_valid"]}
        elif args.get("valid_window") is not None:
            scope["vt"] = {"window": args["valid_window"]}
            scope["co_valid"] = ["valid"]
        elif args.get("window") is not None:
            scope["vt"] = {"window": args["window"], "mode": "event"}
        q["scope"] = scope
    out: list[dict[str, Any]] = []
    q["limit"], q["cursor"] = 10_000, None
    filled = validate(q)
    filled["limit"] = MAX_PATTERN_ROWS + 1
    got = run(adapter, filled)
    cols = got.get("columns")
    for r in got["rows"]:
        out.append(dict(zip(cols, r)) if isinstance(r, list) else r)
    if len(out) > MAX_PATTERN_ROWS:
        raise LimitError(f"an inline pattern returned more than {MAX_PATTERN_ROWS} rows")
    for r in out[:1]:
        missing = [c for c in need if c not in r]
        if missing:
            raise InvalidArgError(f"the inline pattern's rows need columns {missing}")
    return out


def _pair_columns(adapter: StorageAdapter, spec: dict[str, Any], args: dict[str, Any]) -> tuple | None:
    """An inline weight pattern in the engine column-wise (dense ids, typed
    weights), or None for the rows (`_match_rows`); the same scope default."""
    from gnomon.temporal.ops_match import run_pairs, validate

    q = dict(spec)
    if "scope" not in q:
        scope: dict[str, Any] = {"as_of_tt": args["as_of_tt"]}
        if args.get("t_valid") is not None:
            scope["vt"] = {"instant": args["t_valid"]}
        elif args.get("valid_window") is not None:
            scope["vt"] = {"window": args["valid_window"]}
            scope["co_valid"] = ["valid"]
        elif args.get("window") is not None:
            scope["vt"] = {"window": args["window"], "mode": "event"}
        q["scope"] = scope
    q["limit"], q["cursor"] = 10_000, None
    filled = validate(q)
    filled["limit"] = MAX_PAIR_ROWS + 1
    got = run_pairs(adapter, filled)
    if got is not None and len(got[0]) > MAX_PAIR_ROWS:
        raise LimitError(f"an inline pattern returned more than {MAX_PAIR_ROWS} rows")
    return got


def _ids(adapter: StorageAdapter, uids: list[str], strict: bool) -> list[int]:
    """Dense ids; an unknown uid is an error for a listed source and simply
    absent for a target or a pattern row."""
    out = []
    for u in dict.fromkeys(uids):
        try:
            out.append(int(adapter.dense_ids([u])[0]))
        except NotFoundError:
            if strict:
                raise
    return out


def _endpoints(adapter, spec, args, strict) -> list[int] | None:
    if spec is None:
        return None
    if "uids" in spec:
        return _ids(adapter, list(spec["uids"]), strict)
    rows = _match_rows(adapter, spec["match"], args, ("uid",))
    return _ids(adapter, [str(r["uid"]) for r in rows if r.get("uid") is not None], False)


def _exact(v: Any) -> int | Fraction:
    """An exact step value: an int as itself, a float as its exact rational."""
    if isinstance(v, bool) or not isinstance(v, (int, float)):
        raise InvalidArgError(f"a path weight must be a number, got {v!r}")
    if isinstance(v, float):
        if v != v or v in (float("inf"), float("-inf")):
            raise InvalidArgError("a path weight must be finite")
        return Fraction(v)
    return v


def _pair_arrays(adapter, spec, args, both: bool) -> tuple[Any, Any, Any]:
    """`_pair_table` as sorted arrays, for integer weights: `(keys, values,
    None)` with `key = (lo << 32) | hi` per pair (one value each; a pair with
    two values is the same error); else `(None, None, cols)`, the engine's
    columns for `_pair_table` to read (None for the rows route)."""
    cols = _pair_columns(adapter, spec["match"], args)
    if cols is None:
        return None, None, None
    src, dst, kind, wi, _wf = cols
    live = kind != 0
    if (kind[live] != 1).any():
        return None, None, cols
    a, b = src[live].astype(np.int64), dst[live].astype(np.int64)
    if a.size and max(int(a.max()), int(b.max())) >= 1 << 31:
        return None, None, cols
    lo_, hi_ = (np.minimum(a, b), np.maximum(a, b)) if both else (a, b)
    keys = (lo_ << 32) | hi_
    vals = wi[live].astype(np.int64)
    o = np.argsort(keys, kind="stable")
    keys, vals = keys[o], vals[o]
    same = keys[1:] == keys[:-1]
    if (same & (vals[1:] != vals[:-1])).any():
        raise InvalidArgError("the weight pattern gives one pair two values")
    first = np.ones(keys.size, dtype=bool)
    first[1:] = ~same
    return keys[first], vals[first], None


def _pair_table(adapter, spec, args, both: bool, cols: Any = SKIP) -> dict[tuple[int, int], Any]:
    if cols is SKIP:
        cols = _pair_columns(adapter, spec["match"], args)
    if cols is not None:
        src, dst, kind, wi, wf = cols
        table: dict[tuple[int, int], Any] = {}
        a_, b_ = src.tolist(), dst.tolist()
        kl, il, fl = kind.tolist(), wi.tolist(), wf.tolist()
        exact: dict[float, Any] = {}
        for j in range(len(a_)):
            if kl[j] == 0:
                continue
            a, b = a_[j], b_[j]
            key = (min(a, b), max(a, b)) if both else (a, b)
            if kl[j] == 1:
                w = il[j]
            else:
                f = fl[j]
                w = exact.get(f)
                if w is None:
                    w = exact[f] = _exact(f)
            if key in table and table[key] != w:
                raise InvalidArgError("the weight pattern gives one pair two values")
            table[key] = w
        return table
    rows = _match_rows(adapter, spec["match"], args, ("src", "dst", "w"))
    uids = sorted({str(r[c]) for r in rows for c in ("src", "dst") if r[c] is not None})
    known = dict(zip(uids, (_ids(adapter, [u], False) or [None] for u in uids)))
    table: dict[tuple[int, int], Any] = {}
    for r in rows:
        if r["w"] is None:
            continue
        a, b = known.get(str(r["src"]), [None])[0], known.get(str(r["dst"]), [None])[0]
        if a is None or b is None:
            continue
        key = (min(a, b), max(a, b)) if both else (a, b)
        w = _exact(r["w"])
        if key in table and table[key] != w:
            raise InvalidArgError("the weight pattern gives one pair two values")
        table[key] = w
    return table


#: The largest scaled weight `_dyadic_pair_arrays` keeps in int64 (path sums
#: are Python ints, unbounded); past it the exact rationals of the loop.
DYADIC_MAX = 1 << 62


def _dyadic_pair_arrays(cols: tuple, both: bool, wspec: dict[str, Any]) -> tuple | None:
    """A float weight table at one scale: every finite double is
    n / 2^k, so with E the largest k among the weights in use (and the
    default), w * 2^E is an exact integer and sums and comparisons of the
    scaled weights are those of the exact rationals the loop builds
    (`_exact`). `(keys, values, E, scaled default or None)` as
    `_pair_arrays` gives integer tables, or None for the loop: a weight that
    is not a float (the loop's sums would mix int and rational), not finite,
    or too large at the scale."""
    src, dst, kind, _wi, wf = cols
    live = kind != 0
    if not live.any() or (kind[live] != 2).any():
        return None
    f = np.asarray(wf[live], dtype=np.float64)
    if not np.isfinite(f).all():
        return None
    default = None
    if wspec.get("missing") == "default":
        default = wspec.get("default")
        if not isinstance(default, float) or not math.isfinite(default):
            return None
    uniq = np.unique(f)
    exact = [Fraction(float(x)) for x in uniq.tolist()]
    if default is not None:
        exact.append(Fraction(default))
    scale = max(q.denominator.bit_length() - 1 for q in exact)
    scaled = [q.numerator << (scale - (q.denominator.bit_length() - 1)) for q in exact]
    if max(abs(x) for x in scaled) >= DYADIC_MAX:
        return None
    sdefault = scaled.pop() if default is not None else None
    vals_all = np.asarray(scaled, dtype=np.int64)[np.searchsorted(uniq, f)]
    a, b = src[live].astype(np.int64), dst[live].astype(np.int64)
    if a.size and max(int(a.max()), int(b.max())) >= 1 << 31:
        return None
    lo_, hi_ = (np.minimum(a, b), np.maximum(a, b)) if both else (a, b)
    keys = (lo_ << 32) | hi_
    o = np.argsort(keys, kind="stable")
    keys, vals_all = keys[o], vals_all[o]
    same = keys[1:] == keys[:-1]
    if (same & (vals_all[1:] != vals_all[:-1])).any():
        raise InvalidArgError("the weight pattern gives one pair two values")
    first = np.ones(keys.size, dtype=bool)
    first[1:] = ~same
    return keys[first], vals_all[first], scale, sdefault


class _Graph:
    """Pair-level adjacency: `adj[u][v] = (step weight, step score)` for the
    traversal u -> v; `times[u][v]` = sorted edge start times (time mode)."""

    def __init__(self) -> None:
        self.adj: dict[int, dict[int, Any]] = defaultdict(dict)
        self.times: dict[int, dict[int, list[int]]] = defaultdict(lambda: defaultdict(list))
        #: weights held as integers over 2^scale (`_dyadic_pair_arrays`), or None
        self.scale: int | None = None

    def value(self, d: Any) -> Any:
        """A distance in the weights' own terms: an exact rational when the
        graph holds float weights at a common scale (a nonzero distance
        crossed one; the source's own 0 is the int it is on every path)."""
        if self.scale is None or d == 0:
            return d
        return Fraction(d, 1 << self.scale)


def _frontier_rounds(args: dict[str, Any], sources: list[int] | None) -> int | bool | None:
    """How many frontier rounds a search from `sources` needs (`True`: until
    the targets are reached or the frontier is empty), or None to read the
    whole graph."""
    if not sources:
        return None
    if args["result"] in ("all_paths", "k_paths"):
        return int(args["max_hops"])
    if args["metric"] == "hops":
        return int(args["max_hops"]) if args.get("max_hops") is not None else True
    return None


# Frontier-first loading reads the whole relation instead once the next
# round, estimated from the last round's rows per frontier vertex, would bring
# the rows read past 1 / FRONTIER_WHOLE_FRACTION of the relation's edge
# versions (one read is then cheaper than the rounds: a hop distance over
# most of a small graph). The relation's size comes from the native store's
# counts; another backend keeps the rounds.
FRONTIER_WHOLE_FRACTION = 2


def _edge_total(adapter: StorageAdapter, rel_types: list[str] | None) -> int | None:
    """The edge versions of `rel_types` (all types for None) by the native
    store's counts, or None when the backend cannot say cheaply."""
    if not hasattr(adapter, "match_kernel"):
        return None
    counts = adapter.stats().get("rel_type_counts") or {}
    return int(sum(counts.values()) if rel_types is None else
               sum(counts.get(r, 0) for r in rel_types))


def _read_edges(adapter: StorageAdapter, args: dict[str, Any], span: tuple[int, int],
                cols: list[str], sources: list[int] | None, targets: list[int] | None,
                masks: Any) -> dict[str, np.ndarray]:
    """The edge versions the search reads: the whole span, or frontier-first
    (`_frontier_rounds`). A round reads the edges incident to the frontier;
    the next frontier is the far ends the round's kept edges reach (with the
    edge predicates when no truncation applies, so a hop distance can stop
    once every target is reached: every vertex at a smaller depth has then
    been expanded). Edge versions are read once each. A round that would
    bring the rows read past half the relation is replaced by one whole read
    (`FRONTIER_WHOLE_FRACTION`); the answer is the same."""
    def read(touching: list[int] | None) -> dict[str, np.ndarray]:
        kw = {} if touching is None else {"touching_ids": touching}
        return adapter.edges_columnar(as_of_tt=args["as_of_tt"], vt_min=span[0],
                                      vt_max=span[1], rel_types=args.get("rel_types"),
                                      columns=tuple(cols), **kw)

    rounds = _frontier_rounds(args, sources)
    if rounds is None:
        return read(None)
    direction = args["dir"]
    exact = args.get("truncate") is None
    frontier = sorted(set(sources or ()))
    # Stopping once every target has been seen is a breadth-first argument,
    # sound only for a shortest hop distance from ONE source in no time
    # order: a target first seen at depth d is at distance d, and every
    # shortest path to it runs through vertices of smaller depth, whose edges
    # are read. From several sources the frontier is their union, so a target
    # seen from one (or a target that is itself a source) says nothing about
    # its distance from another; an enumeration needs paths longer than the
    # shortest; a time-respecting trace can need a vertex past the depth its
    # target was first seen at.
    want = None
    if (targets is not None and len(frontier) == 1 and not args.get("time_respecting")
            and args["result"] in ("distance", "one_path", "all_optimal")):
        want = set(targets)
    seen_nodes = set(frontier)
    # One source and one target (not the source), no cut: the frontier
    # also grows backward from the target, the smaller side first, and the
    # reading stops once the two meet or either side is exhausted. A meeting
    # vertex bounds the distance L by the rounds f + b taken from each side,
    # and edge i of a shortest path leaves a vertex at depth i - 1 < f or
    # enters one at backward depth L - i < b, so every shortest path's edges
    # are read; an exhausted side means every vertex that side can reach has
    # been expanded. A target the source cannot reach (FinBench TCR3 at a past
    # belief time) no longer reads the source's whole component.
    bidir = (want is not None and exact and len(want) == 1 and not want <= seen_nodes)
    bfront = sorted(want) if bidir else []
    bseen = set(bfront)
    back = {"out": "in", "in": "out", "both": "both"}[direction]
    seen_vids: set[str] = set()
    parts: list[dict[str, np.ndarray]] = []
    done = 0
    total: int | None = None
    rows_read, per_vertex = 0, 0.0
    while frontier and (bfront or not bidir) and (rounds is True or done < rounds):
        done += 1
        backward = bidir and len(bfront) < len(frontier)
        front = bfront if backward else frontier
        if per_vertex and rows_read + per_vertex * len(front) > 1024:
            if total is None:
                total = _edge_total(adapter, args.get("rel_types")) or -1
            if total > 0 and \
                    (rows_read + per_vertex * len(front)) * FRONTIER_WHOLE_FRACTION > total:
                return read(None)
        part = read(front)
        rows_read += len(part["vid"])
        per_vertex = len(part["vid"]) / len(front)
        fresh = np.array([str(v) not in seen_vids for v in part["vid"]], dtype=bool)
        seen_vids.update(str(v) for v in part["vid"])
        part = {k: v[fresh] for k, v in part.items()}
        parts.append(part)
        _, keep = masks(part)
        if exact and args.get("edge_where"):
            for i in np.flatnonzero(keep):
                bag = parse_props(part["props"][i])
                if not all(matches(bag, q["prop"], q["cmp"], q["value"]) is True
                           for q in args["edge_where"]):
                    keep[i] = False
        here = set(front)
        way = back if backward else direction
        nxt: set[int] = set()
        for i in np.flatnonzero(keep):
            a, b = int(part["src_id"][i]), int(part["dst_id"][i])
            if way in ("out", "both") and a in here:
                nxt.add(b)
            if way in ("in", "both") and b in here:
                nxt.add(a)
        if backward:
            nxt -= bseen
            bseen |= nxt
            bfront = sorted(nxt)
        else:
            nxt -= seen_nodes
            seen_nodes |= nxt
            frontier = sorted(nxt)
        if want is not None and exact and want <= seen_nodes:
            break
        if bidir and not seen_nodes.isdisjoint(bseen):
            break
    if not parts:
        return read([])
    return {k: np.concatenate([q[k] for q in parts]) for k in parts[0]}


def _graph(adapter: StorageAdapter, args: dict[str, Any], sources: list[int] | None = None,
           targets: list[int] | None = None) -> _Graph:
    tr = args.get("time_respecting")
    wspec, sspec = args.get("weight"), args.get("score")
    trunc = args.get("truncate")
    t_valid = args.get("t_valid")
    need_props = bool(args.get("edge_where")) or (wspec is not None and "prop" in wspec) or \
        (trunc is not None and isinstance(trunc["by"], dict))
    frontier = _frontier_rounds(args, sources) is not None
    cols = ["src_id", "dst_id", "vt_s", "vt_e"] + (["props"] if need_props else []) + \
        (["vid"] if trunc is not None or frontier else [])
    if tr is None:
        t = int(t_valid)
        span = (t, t + 1)
    elif trunc is not None:
        # the cut is over the vertex's whole believed edge list (valid at
        # t_valid when given), before the window
        span = (0, OPEN_END) if t_valid is None else (int(t_valid), int(t_valid) + 1)
    else:
        span = (args["window"]["t_a"], args["window"]["t_b"])

    def masks(e: dict[str, np.ndarray]) -> tuple[np.ndarray, np.ndarray]:
        """(the rows the truncation reads, the rows the graph keeps)."""
        if tr is None:
            cand = (e["vt_s"] <= t) & (t < e["vt_e"])
            keep = cand.copy()
        else:
            w = args["window"]
            cand = np.ones(len(e["vt_s"]), dtype=bool) if t_valid is None else \
                (e["vt_s"] <= t_valid) & (t_valid < e["vt_e"])
            keep = cand & (e["vt_s"] >= w["t_a"]) & (e["vt_s"] < w["t_b"])
        ew = args.get("edge_window")
        if ew is not None:
            keep &= (e["vt_s"] >= ew["t_a"]) & (e["vt_s"] < ew["t_b"])
        return cand, keep

    e = _read_edges(adapter, args, span, cols, sources, targets, masks)
    cand, keep = masks(e)
    allowed = _truncation(adapter, e, cand, args)
    idx = np.flatnonzero(keep)
    both = args["dir"] == "both"
    if wspec and "match" in wspec and sources and targets is not None and \
            args["result"] in ("distance", "one_path", "all_optimal"):
        # No static walk from a source to a target, nothing to weigh
        su, sv = _steps_sorted(args["dir"], e["src_id"][idx], e["dst_id"][idx])
        tset = set(targets)
        if not any(_reaches(su, sv, s_, tset) for s_ in sources):
            return _Graph()
    columnar = tr is None and allowed is None and not need_props and \
        not args.get("edge_where") and not sspec
    warrays = None
    cols_ = SKIP
    dyadic = None
    if columnar and args["metric"] == "weight" and wspec and "match" in wspec:
        wk, wv, cols_ = _pair_arrays(adapter, wspec, args, both)
        warrays = None if wk is None else (wk, wv)
        if warrays is None and cols_ is not None and \
                args["result"] in ("distance", "one_path", "all_optimal"):
            # float weights: the same table at one power-of-two scale
            dyadic = _dyadic_pair_arrays(cols_, both, wspec)
            if dyadic is not None:
                warrays = dyadic[:2]
    wtab = _pair_table(adapter, wspec, args, both, cols_) if wspec and "match" in wspec and \
        warrays is None else None
    stab = _pair_table(adapter, sspec, args, both) if sspec else None

    def pair_value(spec, table, a, b, bag):
        """The step's exact value, or None when the pair has none (and no
        default applies)."""
        if table is not None:
            v = table.get((min(a, b), max(a, b)) if both else (a, b))
        else:
            raw = bag.get(spec["prop"], SKIP)
            v = _exact(raw) if raw is not SKIP and is_number(raw) else None
        if v is None and spec.get("missing") == "default":
            return _exact(spec["default"])
        return v

    # array operations pay a fixed setup cost: a graph of a few hundred edges
    # (an IC13 hop distance reads ~20) stays with the loop
    if columnar and (idx.size >= GRAPH_COLUMNS_MIN or warrays is not None) and \
            (args["metric"] != "weight" or wtab is not None or warrays is not None):
        fast = _graph_columns(args, e["src_id"][idx], e["dst_id"][idx], wtab, wspec, both, warrays,
                              default=SKIP if dyadic is None else dyadic[3])
        if fast is not None:
            if dyadic is not None:
                fast.scale = dyadic[2]
            return fast
        if warrays is not None:
            # the loop reads the table (the engine evaluates the pattern again)
            wtab = _pair_table(adapter, wspec, args, both)
    if tr is not None and allowed is None and not need_props and not args.get("edge_where") \
            and args["metric"] != "weight" and not sspec and \
            args["result"] in ("all_paths", "k_paths") and \
            os.environ.get("GNOMON_PATHS_TR_COLUMNS", "1") != "0":
        # the enumerations read the graph per vertex only (`get`, `[]`)
        return _LazyTrGraph(args["dir"], e["src_id"][idx], e["dst_id"][idx], e["vt_s"][idx])  # type: ignore[return-value]
    g = _Graph()
    for i in idx:
        a, b = int(e["src_id"][i]), int(e["dst_id"][i])
        bag = parse_props(e["props"][i]) if need_props else {}
        if args.get("edge_where"):
            if not all(matches(bag, p["prop"], p["cmp"], p["value"]) is True
                       for p in args["edge_where"]):
                continue
        if args["metric"] == "weight":
            w = pair_value(wspec, wtab, a, b, bag)
            if w is None:
                continue
            if w <= 0:
                raise InvalidArgError("path weights must be positive")
        else:
            w = 1
        s = (pair_value(sspec, stab, a, b, bag) or 0) if sspec else 0
        steps = [(a, b)] if args["dir"] == "out" else [(b, a)] if args["dir"] == "in" \
            else [(a, b), (b, a)]
        if allowed is not None:
            steps = [(u, v) for u, v, flag in _traversals(args["dir"], a, b)
                     if (int(i), flag) in allowed]
        for u, v in steps:
            if u == v:
                continue
            cur = g.adj[u].get(v)
            if cur is None or w < cur[0]:
                g.adj[u][v] = (w, s)
            if tr is not None:
                g.times[u][v].append(int(e["vt_s"][i]))
    for u in g.times:
        for v in g.times[u]:
            g.times[u][v].sort()
    return g


class _LazyTimes:
    """`times[u][v]`, built per `u` by `_LazyTrGraph`."""

    def __init__(self, g: "_LazyTrGraph") -> None:
        self._g = g

    def __getitem__(self, u: int) -> dict[int, list[int]]:
        return self._g._row(u)[1]

    def get(self, u: int, default: Any = None) -> Any:
        r = self._g._row(u)
        return r[1] if r[1] else default


class _LazyAdj(_LazyTimes):
    """`adj[u]`, built per `u` by `_LazyTrGraph`."""

    def __getitem__(self, u: int) -> dict[int, Any]:
        return self._g._row(u)[0]

    def get(self, u: int, default: Any = None) -> Any:
        r = self._g._row(u)
        return r[0] if r[0] else default

    def __contains__(self, u: int) -> bool:
        return bool(self._g._row(u)[0])


class _LazyTrGraph:
    """The time-respecting hop graph of the per-edge loop, each source
    vertex's lists built when the search first reads them. The traversals
    are held sorted by their source vertex (a stable sort, so each vertex's
    in edge order); a vertex's `adj` (every far end at `(1, 0)`, in first
    traversal order) and `times` (sorted start times) are what the loop
    FinBench SF10 TCR5 on a hub read the whole TRANSFER relation (3-hop
    frontier past half of it) and spent 78 s there; the enumeration reads
    the vertices it reaches only."""

    scale = None

    def __init__(self, direction: str, src: np.ndarray, dst: np.ndarray, vt: np.ndarray) -> None:
        a, b, t = src.astype(np.int64), dst.astype(np.int64), vt.astype(np.int64)
        n = a.size
        if direction == "out":
            u, v, tt = a, b, t
        elif direction == "in":
            u, v, tt = b, a, t
        else:
            # the loop's two steps of edge i: (a, b) then (b, a)
            u = np.empty(2 * n, dtype=np.int64); v = np.empty(2 * n, dtype=np.int64)
            u[0::2], u[1::2], v[0::2], v[1::2] = a, b, b, a
            tt = np.repeat(t, 2)
        keep = u != v
        u, v, tt = u[keep], v[keep], tt[keep]
        o = np.argsort(u, kind="stable")
        self._u, self._v, self._t = u[o], v[o], tt[o]
        self._keys, self._starts = np.unique(self._u, return_index=True)
        self._ends = np.append(self._starts[1:], self._u.size)
        self._rows: dict[int, tuple[dict[int, Any], dict[int, list[int]]]] = {}
        self.adj = _LazyAdj(self)
        self.times = _LazyTimes(self)

    def _row(self, u: int) -> tuple[dict[int, Any], dict[int, list[int]]]:
        got = self._rows.get(u)
        if got is not None:
            return got
        adj: dict[int, Any] = {}
        times: dict[int, list[int]] = {}
        k = int(np.searchsorted(self._keys, u))
        if k < self._keys.size and int(self._keys[k]) == u:
            lo, hi = int(self._starts[k]), int(self._ends[k])
            for v, t in zip(self._v[lo:hi].tolist(), self._t[lo:hi].tolist()):
                if v not in adj:
                    adj[v] = (1, 0)
                    times[v] = []
                times[v].append(t)
            for lst in times.values():
                lst.sort()
        self._rows[u] = (adj, times)
        return self._rows[u]


#: Kept edges from which the instant graph is built by array operations
#: (below, the loop is cheaper: s1000's IC13 hop distance, ~20 edges, took
#: 0.40 ms by the arrays against the loop's 0.37 ms).
GRAPH_COLUMNS_MIN = 512


def _steps_sorted(direction: str, src: np.ndarray, dst: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Every traversal `u -> v` (u != v) of the edges `src -> dst` under
    `direction`, sorted by `u`."""
    a, b = src.astype(np.int64), dst.astype(np.int64)
    if direction == "out":
        u, v = a, b
    elif direction == "in":
        u, v = b, a
    else:
        u, v = np.concatenate([a, b]), np.concatenate([b, a])
    keep = u != v
    u, v = u[keep], v[keep]
    order = np.argsort(u, kind="stable")
    return u[order], v[order]


def _reaches(su: np.ndarray, sv: np.ndarray, s: int, targets: set[int]) -> bool:
    """Whether some walk over the traversals `su[i] -> sv[i]` (sorted by
    `su`) joins `s` to a target: `_joined`'s answer, level by level over
    the arrays."""
    if s in targets:
        return True
    if not targets:
        return False
    tarr = np.fromiter(targets, dtype=np.int64, count=len(targets))
    seen = np.array([s], dtype=np.int64)
    fr = seen
    while fr.size:
        lo = np.searchsorted(su, fr, "left")
        hi = np.searchsorted(su, fr, "right")
        lens = hi - lo
        total = int(lens.sum())
        if total == 0:
            return False
        starts = np.repeat(lo - np.concatenate([[0], np.cumsum(lens)[:-1]]), lens)
        nb = np.unique(sv[starts + np.arange(total)])
        nb = nb[~np.isin(nb, seen, assume_unique=True)]
        if np.isin(nb, tarr).any():
            return True
        seen = np.union1d(seen, nb)
        fr = nb
    return False


def _graph_columns(args: dict[str, Any], src: np.ndarray, dst: np.ndarray,
                   wtab: dict[tuple[int, int], Any] | None, wspec: dict[str, Any] | None,
                   both: bool, warrays: tuple[np.ndarray, np.ndarray] | None = None,
                   default: Any = SKIP) -> _Graph | None:
    """`_graph`'s loop over the kept edges by array operations, for a hop
    metric or integer weights from a pair table, without a score, edge
    predicates, a truncation or time: per traversal `u -> v` the least step
    weight, the loop's graph exactly. None (the loop) for any other case."""
    a, b = src.astype(np.int64), dst.astype(np.int64)
    if a.size and max(int(a.max()), int(b.max())) >= 1 << 31:
        return None
    if args["metric"] == "weight":
        assert wspec is not None and (wtab is not None or warrays is not None)
        if warrays is None and any(type(w) is not int for w in wtab.values()):
            return None
        if default is SKIP:         # else the caller's, at its arrays' scale
            default = None
            if wspec.get("missing") == "default":
                default = _exact(wspec["default"])
                if type(default) is not int:
                    return None
        if warrays is not None:
            keys, vals = warrays
        else:
            keys = np.fromiter(((k[0] << 32) | k[1] for k in wtab), dtype=np.int64, count=len(wtab))
            vals = np.fromiter(wtab.values(), dtype=np.int64, count=len(wtab))
            order = np.argsort(keys)
            keys, vals = keys[order], vals[order]
        lo_, hi_ = (np.minimum(a, b), np.maximum(a, b)) if both else (a, b)
        q = (lo_ << 32) | hi_
        if keys.size:
            pos = np.minimum(np.searchsorted(keys, q), keys.size - 1)
            found = keys[pos] == q
            w = np.where(found, vals[pos], 0)
        else:
            found = np.zeros(q.size, dtype=bool)
            w = np.zeros(q.size, dtype=np.int64)
        if default is not None:
            w = np.where(found, w, default)
        else:
            a, b, w = a[found], b[found], w[found]
        if (w <= 0).any():
            raise InvalidArgError("path weights must be positive")
    else:
        w = np.ones(a.size, dtype=np.int64)
    direction = args["dir"]
    if direction == "out":
        us, vs, ws = a, b, w
    elif direction == "in":
        us, vs, ws = b, a, w
    else:
        us, vs, ws = np.concatenate([a, b]), np.concatenate([b, a]), np.concatenate([w, w])
    keep = us != vs
    us, vs, ws = us[keep], vs[keep], ws[keep]
    # per (u, v), its least weight
    order = np.lexsort((ws, vs, us))
    us, vs, ws = us[order], vs[order], ws[order]
    first = np.ones(us.size, dtype=bool)
    first[1:] = (us[1:] != us[:-1]) | (vs[1:] != vs[:-1])
    g = _Graph()
    adj = g.adj
    for u, v, wt in zip(us[first].tolist(), vs[first].tolist(), ws[first].tolist()):
        adj[u][v] = (wt, 0)
    return g


# --------------------------------------------------------------------------- #
# searches                                                                     #
# --------------------------------------------------------------------------- #

def _dijkstra(g: _Graph, s: int, max_hops: int | None, targets: set[int] | None = None,
              bound: Any = None) -> dict[int, Any]:
    """Exact distances from `s` (hops are weight 1). With `max_hops`, a
    layered relaxation bounds the number of steps.

    Without it the search may stop early, once every vertex the caller
    reads is settled: when all of `targets` are, or when the next vertex is
    farther than `bound` (the least distance found so far under
    `min_over_pairs`, which keeps no pair farther than that). Every vertex
    at or within the stop is settled with its final distance, and so is
    every optimal predecessor of a settled vertex (weights are positive):
    the rows and the paths read from `dist` are those of the whole search.
    The entries past the stop are tentative and no reader keeps them."""
    if max_hops is not None:
        dist = {s: 0}
        frontier = {s: 0}
        for _ in range(max_hops):
            nxt: dict[int, Any] = {}
            for u, du in frontier.items():
                for v, (w, _) in g.adj.get(u, {}).items():
                    nd = du + w
                    if nd < dist.get(v, nd + 1) and nd < nxt.get(v, nd + 1):
                        nxt[v] = nd
            for v, nd in nxt.items():
                if nd < dist.get(v, nd + 1):
                    dist[v] = nd
            frontier = nxt
            if not frontier:
                break
        return dist
    dist = {s: 0}
    heap: list[tuple[Any, int]] = [(0, s)]
    done: set[int] = set()
    left = None if targets is None else set(targets)
    while heap:
        d, u = heappop(heap)
        if u in done:
            continue
        if bound is not None and d > bound:
            break
        done.add(u)
        if left is not None:
            left.discard(u)
            if not left:
                break
        for v, (w, _) in g.adj.get(u, {}).items():
            nd = d + w
            if v not in dist or nd < dist[v]:
                dist[v] = nd
                heappush(heap, (nd, v))
    return dist


def _preds(g: _Graph, dist: dict[int, Any]) -> dict[int, list[int]]:
    """Optimal predecessors: p with dist[p] + w(p, v) == dist[v]."""
    pred: dict[int, list[int]] = defaultdict(list)
    for p, dp in dist.items():
        for v, (w, _) in g.adj.get(p, {}).items():
            if v in dist and dp + w == dist[v]:
                pred[v].append(p)
    return pred


def _score(g: _Graph, path: list[int]) -> Any:
    return sum((g.adj[a][b][1] for a, b in zip(path, path[1:])), 0)


def _enum_simple(g: _Graph, s: int, lo: int, hi: int, strict: bool | None,
                 targets: set[int] | None, budget: list[int]):
    """Every node-simple path from `s` of `lo..hi` steps ending in `targets`
    (any vertex when None). In time mode, steps take the earliest edge whose
    start is after (at or after) the previous step's."""
    out: list[tuple[list[int], Any]] = []

    def rec(u: int, path: list[int], cost: Any, t_prev: int | None) -> None:
        budget[0] += 1
        if budget[0] > MAX_EXPANSIONS:
            raise CostError("paths expansion budget exceeded",
                            estimate={"expansions_est": budget[0]},
                            suggestions=["reduce max_hops", "name fewer sources",
                                         "restrict rel_types"])
        steps = len(path) - 1
        if steps >= lo and steps >= 1 and (targets is None or u in targets):
            out.append((list(path), cost))
        if steps == hi:
            return
        for v in sorted(g.adj.get(u, {})):
            if v in path:
                continue
            t_next = None
            if strict is not None:
                ts = g.times[u][v]
                if t_prev is None:
                    k = 0
                elif strict:
                    k = bisect_right(ts, t_prev)
                else:
                    k = bisect_left(ts, t_prev)
                if k >= len(ts):
                    continue
                t_next = ts[k]
            path.append(v)
            rec(v, path, cost + g.adj[u][v][0], t_next)
            path.pop()

    rec(s, [s], 0, None)
    return out


def _out_num(x: Any) -> Any:
    return float(x) if isinstance(x, Fraction) else x


# --------------------------------------------------------------------------- #
# per-vertex truncation                                                   #
# --------------------------------------------------------------------------- #

def _truncation(adapter: StorageAdapter, e: dict[str, np.ndarray], cand: np.ndarray,
                args: dict[str, Any]) -> set[tuple[int, int]] | None:
    """FinBench's truncation: per
    vertex, among its candidate edges in the traversal direction, the first
    `limit` by the key (`vt_s` or a numeric property; ties by the far end's
    uid, then the edge's version id; an edge without a numeric key last).
    Returns the allowed `(edge row, 0 = src->dst | 1 = dst->src)` traversals,
    or None without a `truncate`. The cut precedes `edge_where`, `edge_window`
    and the time window, as in the reference implementation."""
    trunc = args.get("truncate")
    if trunc is None:
        return None
    by, desc, limit = trunc["by"], trunc["dir"] == "desc", int(trunc["limit"])
    idx = np.flatnonzero(cand)
    ids = sorted({int(e["src_id"][i]) for i in idx} | {int(e["dst_id"][i]) for i in idx})
    uid = dict(zip(ids, adapter.uids_for(ids))) if ids else {}
    per: dict[int, list[tuple[Any, str, str, int, int]]] = defaultdict(list)
    for i in idx:
        a, b = int(e["src_id"][i]), int(e["dst_id"][i])
        if by == "vt_s":
            key: Any = int(e["vt_s"][i])
        else:
            raw = parse_props(e["props"][i]).get(by["prop"], SKIP)
            key = _exact(raw) if raw is not SKIP and is_number(raw) else None
        vid = str(e["vid"][i])
        if args["dir"] in ("out", "both"):
            per[a].append((key, uid[b], vid, int(i), 0))
        if args["dir"] in ("in", "both"):
            per[b].append((key, uid[a], vid, int(i), 1))
    allowed: set[tuple[int, int]] = set()
    for lst in per.values():
        keyed = sorted((x for x in lst if x[0] is not None),
                       key=lambda x: (-x[0] if desc else x[0], x[1], x[2], x[4]))
        rest = sorted((x for x in lst if x[0] is None), key=lambda x: (x[1], x[2], x[4]))
        allowed.update((x[3], x[4]) for x in (keyed + rest)[:limit])
    return allowed


def _traversals(direction: str, a: int, b: int) -> list[tuple[int, int, int]]:
    """`(from, to, flag)` per traversal of an edge `a -> b` under `dir`."""
    if direction == "out":
        return [(a, b, 0)]
    if direction == "in":
        return [(b, a, 1)]
    return [(a, b, 0), (b, a, 1)]


# --------------------------------------------------------------------------- #
# the window form: one shared instant                                     #
# --------------------------------------------------------------------------- #

Ivs = list[tuple[int, int]]


#: Vertices whose weight-pattern rows one restricted evaluation reads at most.
WEIGHT_BATCH = 4096
#: The window graph reads its nodes' existence by point reads while they are
#: at most this fraction of the node versions, else by the node scan.
NODE_POINT_FRACTION = 32
#: Seeded weights evaluate the whole pattern once instead when the vertices
#: weighed would pass this fraction of the vertices with steps, or after
#: `SEEDED_BATCHES_MAX` restricted evaluations. Measured at SF1 on the LDBC BI
#: instances: the whole pattern ~2.9 s (BI19) and ~4.7 s (BI15); a first
#: batch of 1-6 vertices 0.2-0.3 s, a second of 7-61 vertices 1.1-1.6 s,
#: and both searches that needed a second batch (BI19, BI15) went on to
#: the whole pattern anyway, so the second batch was spent for nothing.
SEEDED_WHOLE_FRACTION = 8
SEEDED_BATCHES_MAX = 1


def _seedable(spec: dict[str, Any]) -> list[tuple[dict[str, Any], str, str]] | None:
    """For a weight pattern `paths` may evaluate per frontier: each pattern
    (a union's branches, or the one pattern) with the node variables its
    `src` and `dst` columns read (`{"col": "<var>.uid"}`), when the return
    keeps a row's `src` and `dst` (rows, or a group by exactly them), so the
    rows with `src` (or `dst`) in a vertex set are those of the pattern with
    that variable restricted to the set. None otherwise."""
    def ends(rows: list[dict[str, Any]], nodes: set[str]) -> tuple[str, str] | None:
        got = {}
        for r in rows:
            e = r.get("expr") or {}
            if r.get("as") in ("src", "dst") and set(e) == {"col"} and \
                    str(e["col"]).endswith(".uid") and e["col"][:-4] in nodes:
                got[r["as"]] = e["col"][:-4]
        return (got["src"], got["dst"]) if len(got) == 2 else None

    ret = spec.get("return") or {}
    # a cut of the rows (a limit, a top per group) is not a set of rows per
    # vertex: such a pattern is evaluated whole
    if any(k in ret for k in ("limit", "top_per", "having")):
        return None
    branches = spec.get("union") if spec.get("union") else [spec]
    if spec.get("union"):
        g = ret.get("group")
        if g is None and "rows" not in ret:
            return None
        if g is not None:
            keys = [k.get("expr") for k in g.get("by", [])]
            if sorted(json_key(k) for k in keys) != sorted(json_key({"col": c}) for c in ("src", "dst")):
                return None
    out = []
    for b in branches:
        pat = b.get("pattern")
        if not isinstance(pat, dict) or pat.get("lookups") or pat.get("succ"):
            return None
        if spec.get("union"):
            rows = b.get("rows")
        else:
            # one pattern: its rows, or a group by exactly `src` and `dst`
            r_ = b.get("return") or {}
            rows = r_.get("rows")
            if rows is None and r_.get("group") is not None:
                rows = r_["group"].get("by", [])
                if len(rows) != 2:
                    return None
        if not rows:
            return None
        vs = ends(rows, {n["var"] for n in pat.get("nodes", [])})
        if vs is None or any(n.get("uids") for n in pat.get("nodes", []) if n["var"] in vs):
            return None
        out.append((b, vs[0], vs[1]))
    return out


def json_key(x: Any) -> str:
    import json
    return json.dumps(x, sort_keys=True)


class _ArrayTable:
    """A pair table of integer weights as sorted arrays (`_pair_arrays`):
    `key = (lo << 32) | hi`; read as the dict it replaces (`get`, `items`)."""

    def __init__(self, keys: np.ndarray, vals: np.ndarray) -> None:
        self.keys, self.vals = keys, vals

    def get(self, k: tuple[int, int], default: Any = None) -> Any:
        q = (int(k[0]) << 32) | int(k[1])
        i = int(np.searchsorted(self.keys, q))
        if i < self.keys.size and int(self.keys[i]) == q:
            return int(self.vals[i])
        return default

    def __contains__(self, k: tuple[int, int]) -> bool:
        return self.get(k) is not None

    def __getitem__(self, k: tuple[int, int]) -> Any:
        v = self.get(k)
        if v is None:
            raise KeyError(k)
        return v

    def __len__(self) -> int:
        return int(self.keys.size)

    def items(self):
        return ((((int(k) >> 32), int(k) & 0xFFFFFFFF), int(v)) for k, v in zip(self.keys, self.vals))

    def values(self):
        return (int(v) for v in self.vals)


class _SeededWeights:
    """A window graph whose weight pattern is evaluated per batch of the
    vertices the search expands: `get(u)` weighs `u` and, in the same
    restricted evaluations, the next vertices of the search (the cheapest
    states on its heap; at most as many as are weighed already, 64 at least,
    and `WEIGHT_BATCH`). A vertex's rows are
    the pattern's rows with `src` or `dst` at it, so its steps' weights are
    the whole table's."""

    def __init__(self, adapter, args, spec, seeds, raw, both):
        from gnomon.temporal import ops_match as OM

        self.adapter, self.args, self.spec, self.raw, self.both = adapter, args, spec, raw, both
        self.done: dict[int, list[tuple]] = {}
        self.ring: dict[int, None] = {}
        self.table: dict[tuple[int, int], Any] = {}
        self.calls = 0
        card = OM.cardinalities(adapter)
        # each pattern's steps in the greedy order from either restricted end
        self.forms = []
        for b, vs, vd in seeds:
            orders = {}
            for v in (vs, vd):
                planned = OM.plan_order(b["pattern"], card, anchor=v,
                                        walks=".depth" not in json_key(b))
                orders[v] = [] if planned is None else planned[1]
            self.forms.append((b, vs, vd, orders))

    def _restricted(self, uids: list[str], side: int) -> dict[str, Any]:
        import copy

        q = copy.deepcopy(self.spec)
        branches = q["union"] if q.get("union") else [q]
        for (b0, vs, vd, orders), b in zip(self.forms, branches):
            var = vs if side == 0 else vd
            if orders[var]:
                b["pattern"]["steps"] = copy.deepcopy(orders[var])
            for n in b["pattern"]["nodes"]:
                if n["var"] == var:
                    n["uids"] = list(uids)
        return {"match": q}

    def _weigh(self, batch: list[int]) -> None:
        uids = self.adapter.uids_for(batch)
        for side in (0, 1):
            self.calls += 1
            part = _pair_table(self.adapter, self._restricted(uids, side), self.args, self.both)
            for k, w in part.items():
                if k in self.table and self.table[k] != w:
                    raise InvalidArgError("the weight pattern gives one pair two values")
                self.table[k] = w
        wspec = self.args["weight"]
        for u in batch:
            lst = []
            for v, lo, hi, a, b, bag, sc, ts in self.raw.get(u, ()):
                wt = _value(wspec, self.table, a, b, bag, self.both)
                if wt is None:
                    continue
                if wt <= 0:
                    raise InvalidArgError("path weights must be positive")
                lst.append((v, lo, hi, wt, sc, ts))
                if v not in self.done:
                    self.ring[v] = None
            self.done[u] = lst
            self.ring.pop(u, None)

    def _weigh_all(self) -> None:
        """The whole pattern once, each branch bound in its cheapest order
        (anchor first); every vertex's list from it."""
        import copy

        from gnomon.temporal import ops_match as OM

        self.calls += 1
        q = copy.deepcopy(self.spec)
        card = OM.cardinalities(self.adapter)
        for b in (q["union"] if q.get("union") else [q]):
            planned = OM.plan_order(b["pattern"], card, anchor="*",
                                    walks=".depth" not in json_key(b))
            if planned is not None:
                a, steps = planned
                b["pattern"]["steps"] = steps
                b["pattern"]["nodes"] = sorted(b["pattern"]["nodes"], key=lambda n: n["var"] != a)
        wk, wv, cols = (None, None, SKIP) if getattr(self, "cols", None) is None else \
            _pair_arrays(self.adapter, {"match": q}, self.args, self.both)
        if wk is not None:
            whole: Any = _ArrayTable(wk, wv)
        else:
            whole = _pair_table(self.adapter, {"match": q}, self.args, self.both, cols)
        for k, w in self.table.items():
            if whole.get(k) != w:
                raise InvalidArgError("the weight pattern gives one pair two values")
        self.table = whole
        self.whole = True
        rest = [u for u in self.raw if u not in self.done]
        self.ring.clear()
        lists = self._lists_by_columns()
        for u in rest:
            self.done[u] = lists[u] if lists is not None else self._list(u)

    def _lists_by_columns(self) -> dict[int, list[tuple]] | None:
        """Every start's `_list` from the traversal arrays and the whole
        table (None without the arrays: `_list` per start). Each distinct
        weight is checked once; the kept traversals, with each one's weight
        as an index into the distinct values, stay on `self.kept` for
        `_NativeGraph`."""
        c = getattr(self, "cols", None)
        wspec = self.args["weight"]
        if c is None:
            return None
        A, B = c["A"], c["B"]
        if A.size and max(int(A.max()), int(B.max())) >= 1 << 31:
            return None
        objs: list[Any] = []
        if isinstance(self.table, _ArrayTable):
            # integer weights as arrays: the distinct values, each pair's index
            distinct, inv = np.unique(self.table.vals, return_inverse=True)
            objs = distinct.tolist()
            keys, vals = self.table.keys, inv.astype(np.int64)
        else:
            index: dict[Any, int] = {}
            ki: list[int] = []
            wi: list[int] = []
            for k, w in self.table.items():
                j = index.get(w)
                if j is None or type(objs[j]) is not type(w):
                    j = len(objs)
                    index[w] = j
                    objs.append(w)
                ki.append((k[0] << 32) | k[1])
                wi.append(j)
            keys = np.asarray(ki, dtype=np.int64)
            vals = np.asarray(wi, dtype=np.int64)
            o = np.argsort(keys)
            keys, vals = keys[o], vals[o]
        lo_, hi_ = (np.minimum(A, B), np.maximum(A, B)) if self.both else (A, B)
        q = (lo_ << 32) | hi_
        if keys.size:
            pos = np.minimum(np.searchsorted(keys, q), keys.size - 1)
            found = keys[pos] == q
            W = np.where(found, vals[pos], -1)
        else:
            found = np.zeros(q.size, dtype=bool)
            W = np.full(q.size, -1, dtype=np.int64)
        if wspec.get("missing") == "default":
            objs.append(_exact(wspec["default"]))
            W = np.where(found, W, len(objs) - 1)
            found = np.ones(q.size, dtype=bool)
        W = W[found]
        used = np.unique(W).tolist()
        if any(objs[j] <= 0 for j in used):
            raise InvalidArgError("path weights must be positive")
        U, V, LO, HI, TS = (x[found] for x in (c["U"], c["V"], c["LO"], c["HI"], c["TS"]))
        rows = list(zip(V.tolist(), LO.tolist(), HI.tolist(), [objs[j] for j in W.tolist()],
                        repeat(0), TS.tolist()))
        kept = np.concatenate([[0], np.cumsum(found)]).astype(np.int64)
        out: dict[int, list[tuple]] = {}
        b = c["bounds"]
        for g, u in enumerate(c["starts"]):
            out[u] = rows[int(kept[b[g]]):int(kept[b[g + 1]])]
        self.kept = (U, V, LO, HI, W, objs, used)
        return out

    def _list(self, u: int) -> list[tuple]:
        wspec = self.args["weight"]
        lst = []
        for v, lo, hi, a, b, bag, sc, ts in self.raw.get(u, ()):
            wt = _value(wspec, self.table, a, b, bag, self.both)
            if wt is None:
                continue
            if wt <= 0:
                raise InvalidArgError("path weights must be positive")
            lst.append((v, lo, hi, wt, sc, ts))
        return lst

    def prime(self, sources: list[int]) -> None:
        """Weigh the sources first, together (each search starts at one)."""
        todo = [s_ for s_ in dict.fromkeys(sources) if s_ in self.raw and s_ not in self.done]
        if not todo:
            return
        if len(todo) * SEEDED_WHOLE_FRACTION > len(self.raw):
            self._weigh_all()
            return
        for i in range(0, len(todo), WEIGHT_BATCH):
            self._weigh(todo[i:i + WEIGHT_BATCH])

    def get(self, u: int, default: Any = (), hint: Any = None) -> list[tuple]:
        if u not in self.done:
            if getattr(self, "whole", False):
                self.done[u] = self._list(u)
            else:
                nxt = hint() if hint is not None else list(self.ring)
                batch = [u] + list(dict.fromkeys(
                    v for v in nxt if v != u and v not in self.done and v in self.raw)
                )[:min(WEIGHT_BATCH, max(64, len(self.done))) - 1]
                if (len(self.done) + len(batch)) * SEEDED_WHOLE_FRACTION > len(self.raw) or \
                        self.calls >= 2 * SEEDED_BATCHES_MAX:
                    self._weigh_all()
                else:
                    self._weigh(batch)
        return self.done.get(u, default)

    def items(self):
        return self.done.items()


def _win_graph(adapter: StorageAdapter, args: dict[str, Any], seeded: bool = False,
               extra: list[int] | None = None, arrays: bool = False
               ) -> tuple[dict[int, list[tuple]], dict[int, Ivs], Any, Any, bool]:
    """Per traversal start, its `(to, lo, hi, weight, score)` edge versions
    (window-clipped), the window-clipped existence of every node, and the
    score inputs. With `arrays`, a hop-metric graph without properties, a
    predicate, a truncation or a score comes from the traversal arrays
    (`_HopAdj`), the loop's lists built only for the starts read."""
    from gnomon.plan.eval.covalid import ivs_norm

    w = args["valid_window"]
    va, vb = int(w["t_a"]), int(w["t_b"])
    as_of = args["as_of_tt"]
    wspec, sspec = args.get("weight"), args.get("score")
    trunc = args.get("truncate")
    need_props = bool(args.get("edge_where")) or (wspec is not None and "prop" in wspec) or \
        (trunc is not None and isinstance(trunc["by"], dict))
    cols = ["src_id", "dst_id", "vt_s", "vt_e"] + (["props"] if need_props else []) + \
        (["vid"] if trunc is not None else [])
    e = adapter.edges_columnar(as_of_tt=as_of, vt_min=va, vt_max=vb,
                               rel_types=args.get("rel_types"), columns=tuple(cols))
    cand = (e["vt_s"] < vb) & (va < e["vt_e"])
    allowed = _truncation(adapter, e, cand, args)
    keep = cand.copy()
    ew = args.get("edge_window")
    if ew is not None:
        keep &= (e["vt_s"] >= ew["t_a"]) & (e["vt_s"] < ew["t_b"])
    if args.get("time_respecting") is not None:
        tw = args["window"]
        keep &= (e["vt_s"] >= tw["t_a"]) & (e["vt_s"] < tw["t_b"])
    both = args["dir"] == "both"
    seeds = _seedable(wspec["match"]) if seeded and args["metric"] == "weight" and wspec \
        and "match" in wspec else None
    wtab = _pair_table(adapter, wspec, args, both) if wspec and "match" in wspec and \
        seeds is None else None
    stab = _pair_table(adapter, sspec, args, both) if sspec else None
    adj: dict[int, list[tuple]] = defaultdict(list)
    raw: dict[int, list[tuple]] = defaultdict(list)
    cols = None
    plain = allowed is None and not need_props and not args.get("edge_where") and not sspec
    if seeds is not None and plain:
        cols = _raw_columns(args["dir"], e, keep, va, vb)
        raw = cols.pop("raw")
    elif arrays and plain and args["metric"] == "hops":
        cols = _raw_columns(args["dir"], e, keep, va, vb)
        adj = _HopAdj(cols)
    for i in (np.flatnonzero(keep) if cols is None else ()):
        a, b = int(e["src_id"][i]), int(e["dst_id"][i])
        bag = parse_props(e["props"][i]) if need_props else {}
        if args.get("edge_where"):
            if not all(matches(bag, p["prop"], p["cmp"], p["value"]) is True
                       for p in args["edge_where"]):
                continue
        if seeds is not None:
            wt = None
        elif args["metric"] == "weight":
            wt = _value(wspec, wtab, a, b, bag, both)
            if wt is None:
                continue
            if wt <= 0:
                raise InvalidArgError("path weights must be positive")
        else:
            wt = 1
        sc = (_value(sspec, stab, a, b, bag, both) or 0) if sspec else 0
        lo, hi = max(int(e["vt_s"][i]), va), min(int(e["vt_e"][i]), vb)
        for u, v, flag in _traversals(args["dir"], a, b):
            if u == v or (allowed is not None and (int(i), flag) not in allowed):
                continue
            if seeds is not None:
                raw[u].append((v, lo, hi, a, b, bag, sc, int(e["vt_s"][i])))
            else:
                adj[u].append((v, lo, hi, wt, sc, int(e["vt_s"][i])))
    nraw: dict[int, Ivs] = defaultdict(list)
    # only the nodes a traversal can stand on, and the endpoints: the search
    # reads no other node's existence (every node at SF1 was 3.8M versions);
    # few of them by point reads, many by the scan
    if cols is not None:
        ends = (set(raw) if seeds is not None else set(adj)) | \
            set(np.unique(cols["V"]).tolist()) | set(extra or ())
    else:
        ends = set(adj) | set(raw) | {x[0] for lst in adj.values() for x in lst} | \
            {x[0] for lst in raw.values() for x in lst} | set(extra or ())
    point = getattr(adapter, "node_intervals", None)
    if point is not None and len(ends) * NODE_POINT_FRACTION <= adapter.stats()["n_node_versions"]:
        for u_, ivl in point(sorted(ends), va, vb, as_of).items():
            for lo_, hi_ in ivl:
                lo, hi = max(lo_, va), min(hi_, vb)
                if lo < hi:
                    nraw[u_].append((lo, hi))
    else:
        n = adapter.nodes_columnar(as_of_tt=as_of, vt_min=va, vt_max=vb)
        on = np.isin(n["uid_id"], np.fromiter(ends, dtype=np.int64, count=len(ends)))
        for i in np.flatnonzero(on):
            lo, hi = max(int(n["vt_s"][i]), va), min(int(n["vt_e"][i]), vb)
            if lo < hi:
                nraw[int(n["uid_id"][i])].append((lo, hi))
    exists = {k: ivs_norm(v) for k, v in nraw.items()}
    if seeds is not None:
        sw = _SeededWeights(adapter, args, wspec["match"], seeds, raw, both)
        sw.cols = cols
        return sw, exists, sspec, stab, both
    return adj, exists, sspec, stab, both


class _HopAdj:
    """`_win_graph`'s hop-metric lists from the traversal arrays: the
    same keys in the loop's order and, per start, the same
    `(to, lo, hi, 1, 0, vt_s)` list in edge order, built when read. The
    native search reads the arrays (`_NativeGraph`), so the lists are built
    only when the Python search or a path's predecessors read them; the loop
    built 2.1M tuples for an IC13 window call at SF1."""

    def __init__(self, cols: dict[str, Any]) -> None:
        self.cols = cols
        self.span = cols["raw"].span
        self.built: dict[int, list[tuple]] = {}

    def get(self, u: int, default: Any = ()) -> Any:
        hit = self.built.get(u)
        if hit is not None:
            return hit
        sp = self.span.get(u)
        if sp is None:
            return default
        c, (lo, hi) = self.cols, sp
        lst = list(zip(c["V"][lo:hi].tolist(), c["LO"][lo:hi].tolist(), c["HI"][lo:hi].tolist(),
                       repeat(1), repeat(0), c["TS"][lo:hi].tolist()))
        self.built[u] = lst
        return lst

    def __getitem__(self, u: int) -> list[tuple]:
        got = self.get(u, None)
        if got is None:
            raise KeyError(u)
        return got

    def __contains__(self, u: object) -> bool:
        return u in self.span

    def __len__(self) -> int:
        return len(self.span)

    def __iter__(self):
        return iter(self.span)

    def keys(self):
        return self.span.keys()

    def items(self):
        return ((u, self.get(u)) for u in self.span)

    def values(self):
        return (self.get(u) for u in self.span)


#: The bag of a step read without properties (only read, never written).
_NO_PROPS: dict[str, Any] = {}


def _raw_columns(direction: str, e: dict[str, np.ndarray], keep: np.ndarray, va: int,
                 vb: int) -> dict[str, Any]:
    """`_win_graph`'s seeded loop by array operations, for steps read
    without properties, a predicate, a truncation or a score: per
    traversal start, its `(to, lo, hi, a, b, bag, 0, vt_s)` list in edge
    order, the starts in the order the loop first meets them (`raw`); and
    the traversals as arrays sorted by (start, edge order) with each
    start's run (`U`, `V`, `LO`, `HI`, `A`, `B`, `TS`, `starts`, `bounds`)."""
    idx = np.flatnonzero(keep)
    a = e["src_id"][idx].astype(np.int64)
    b = e["dst_id"][idx].astype(np.int64)
    ts = e["vt_s"][idx].astype(np.int64)
    lo = np.maximum(ts, va)
    hi = np.minimum(e["vt_e"][idx].astype(np.int64), vb)
    seq = np.arange(idx.size, dtype=np.int64) * 2
    if direction == "out":
        U, V, K, A, B, LO, HI, TS = a, b, seq, a, b, lo, hi, ts
    elif direction == "in":
        U, V, K, A, B, LO, HI, TS = b, a, seq + 1, a, b, lo, hi, ts
    else:
        U, V = np.concatenate([a, b]), np.concatenate([b, a])
        K = np.concatenate([seq, seq + 1])
        A, B = np.concatenate([a, a]), np.concatenate([b, b])
        LO, HI, TS = np.concatenate([lo, lo]), np.concatenate([hi, hi]), np.concatenate([ts, ts])
    m = U != V
    order = np.lexsort((K[m], U[m]))
    U, V, K, A, B, LO, HI, TS = (x[m][order] for x in (U, V, K, A, B, LO, HI, TS))
    cut = np.flatnonzero(np.diff(U)) + 1 if U.size else np.zeros(0, dtype=np.int64)
    first = np.concatenate([[0], cut]).astype(np.int64) if U.size else np.zeros(0, dtype=np.int64)
    bounds = np.concatenate([first, [U.size]]).astype(np.int64)
    starts = U[first].tolist()
    bl = bounds.tolist()
    # the loop's key order: by each start's first traversal in edge order
    order = np.argsort(K[first], kind="stable").tolist()
    cols = {"U": U, "V": V, "LO": LO, "HI": HI, "A": A, "B": B, "TS": TS, "starts": starts, "bounds": bl}
    cols["raw"] = _LazyRaw(cols, order)
    return cols


class _LazyRaw:
    """`_win_graph`'s seeded `raw` lists, built per start on demand from the
    sorted traversal arrays: the same keys, in the loop's order, and the same
    lists (`(to, lo, hi, a, b, bag, 0, vt_s)` in edge order)."""

    def __init__(self, cols: dict[str, Any], order: list[int]) -> None:
        self.cols = cols
        starts, bl = cols["starts"], cols["bounds"]
        self.span: dict[int, tuple[int, int]] = {starts[g]: (bl[g], bl[g + 1]) for g in order}
        self.built: dict[int, list[tuple]] = {}

    def get(self, u: int, default: Any = ()) -> Any:
        hit = self.built.get(u)
        if hit is not None:
            return hit
        sp = self.span.get(u)
        if sp is None:
            return default
        c, (lo, hi) = self.cols, sp
        lst = list(zip(c["V"][lo:hi].tolist(), c["LO"][lo:hi].tolist(), c["HI"][lo:hi].tolist(),
                       c["A"][lo:hi].tolist(), c["B"][lo:hi].tolist(), repeat(_NO_PROPS),
                       repeat(0), c["TS"][lo:hi].tolist()))
        self.built[u] = lst
        return lst

    def __getitem__(self, u: int) -> list[tuple]:
        got = self.get(u, None)
        if got is None:
            raise KeyError(u)
        return got

    def __contains__(self, u: object) -> bool:
        return u in self.span

    def __len__(self) -> int:
        return len(self.span)

    def __iter__(self):
        return iter(self.span)

    def keys(self):
        return self.span.keys()

    def values(self):
        return (self[u] for u in self.span)

    def items(self):
        return ((u, self[u]) for u in self.span)


def _value(spec: dict[str, Any], table: dict | None, a: int, b: int, bag: dict,
           both: bool) -> Any:
    """A step's exact weight or score, as `_graph` reads it."""
    if table is not None:
        v = table.get((min(a, b), max(a, b)) if both else (a, b))
    else:
        raw = bag.get(spec["prop"], SKIP)
        v = _exact(raw) if raw is not SKIP and is_number(raw) else None
    if v is None and spec.get("missing") == "default":
        return _exact(spec["default"])
    return v


def _win_search(adj: dict[int, list[tuple]], exists: dict[int, Ivs], s: int, va: int, vb: int,
                max_hops: int | None, targets: set[int] | None = None,
                bound: Any = None, first_only: bool = False,
                abort_on_whole: bool = False) -> dict[int, list[tuple[Ivs, Any]]]:
    """Per node, its `(instants, cost)` pieces: the instant set where its least
    cost from `s` is that cost (label setting over (node, instant set)
    states; at each instant, Dijkstra on the snapshot). With `targets`, it
    stops once every target has been settled and the next cost exceeds the
    largest of their least costs (with `first_only`, the first target's
    cost bounds it), and with `bound` once it exceeds that: a
    state of no larger cost is settled first, so every piece a target's
    least cost, its instants or its optimal predecessors read is there."""
    from gnomon.plan.eval.covalid import ivs_and, ivs_or, ivs_sub

    start = ivs_and([(va, vb)], exists.get(s, []))
    if not start:
        return {}
    settled: dict[int, Ivs] = {}
    pieces: dict[int, list[tuple[Ivs, Any]]] = {}
    heap: list[tuple[Any, int, int, Ivs]] = [(0, 0, s, start)]
    seq = 1
    budget = 0
    first: dict[int, Any] = {}
    while heap:
        if bound is not None and heap[0][0] > bound:
            break
        if targets and len(first) == len(targets) and heap[0][0] > max(first.values()):
            break
        d, _, u, k = heappop(heap)
        k = ivs_sub(k, settled.get(u, []))
        if not k:
            continue
        if targets and u in targets:
            first.setdefault(u, d)
            if first_only and (bound is None or d < bound):
                bound = d
        budget += 1
        if budget > MAX_EXPANSIONS:
            raise CostError("paths expansion budget exceeded",
                            estimate={"expansions_est": budget},
                            suggestions=["narrow valid_window", "name fewer sources"])
        settled[u] = ivs_or(settled.get(u, []), k)
        pieces.setdefault(u, []).append((k, d))
        if max_hops is not None and d >= max_hops:
            continue
        # every step costs more than nothing: from a state at the stop cost
        # no state the answer reads is reached, so its steps are not read
        if bound is not None and d >= bound:
            continue
        if targets and len(first) == len(targets) and d >= max(first.values()):
            continue
        steps = adj.get(u, (), hint=lambda: [x[2] for x in nsmallest(WEIGHT_BATCH, heap)]) \
            if isinstance(adj, _SeededWeights) else adj.get(u, ())
        if abort_on_whole and getattr(adj, "whole", False):
            raise _Whole()
        for v, lo, hi, wt, _sc, _ts in steps:
            k2 = ivs_and(k, [(lo, hi)])
            if not k2:
                continue
            k2 = ivs_sub(ivs_and(k2, exists.get(v, [])), settled.get(v, []))
            if k2:
                heappush(heap, (d + wt, seq, v, k2))
                seq += 1
    return pieces


class _Whole(Exception):
    """Seeded weights went whole mid-search: the search restarts natively."""


def _scaled(x: Any, k: int) -> int | None:
    """An exact cost as an integer at scale 2^k, or None when it is not one
    (a denominator that is not a power of two, or finer than 2^k)."""
    if isinstance(x, Fraction):
        d = x.denominator
        if d & (d - 1) or d.bit_length() - 1 > k:
            return None
        return x.numerator * ((1 << k) // d)
    return int(x) << k


class _NativeGraph:
    """A window graph as `winsearch.rs` reads it, built once for every
    search over it: CSR steps and existence on local indices, weights at a
    common scale 2^k."""

    def __init__(self, adj: Any, exists: dict[int, Ivs], extra: Any) -> None:
        if isinstance(adj, _HopAdj):
            self._from_hops(adj, exists, extra)
            return
        kept = getattr(adj, "kept", None)
        if kept is not None and getattr(adj, "whole", False):
            self._from_arrays(adj, kept, exists, extra)
            return
        items = adj.items() if isinstance(adj, dict) else adj.done.items()
        lists = dict(items)
        k, ok = 0, True
        for lst in lists.values():
            for e in lst:
                if type(e[3]) is Fraction:
                    d = e[3].denominator
                    if d & (d - 1):
                        ok = False
                    k = max(k, d.bit_length() - 1)
        self.ok = ok and k <= 90
        self.k = k
        if not self.ok:
            return
        ids = set(lists) | {e[0] for lst in lists.values() for e in lst} | set(extra)
        self.order = sorted(ids)
        self.local = {g: i for i, g in enumerate(self.order)}
        local = self.local
        off, to, lo, hi, w, fr = [0], [], [], [], [], []
        one = 1 << k
        for g in self.order:
            for e in lists.get(g, ()):
                to.append(local[e[0]])
                lo.append(e[1])
                hi.append(e[2])
                x = e[3]
                if type(x) is Fraction:
                    w.append(x.numerator * (one // x.denominator))
                    fr.append(True)
                else:
                    w.append(int(x) << k)
                    fr.append(False)
            off.append(len(to))
        ex_off, ex_lo, ex_hi = [0], [], []
        for g in self.order:
            for a_, b_ in exists.get(g, ()):
                ex_lo.append(a_)
                ex_hi.append(b_)
            ex_off.append(len(ex_lo))
        self.arrays = (np.asarray(off, dtype=np.uint32), np.asarray(to, dtype=np.uint32),
                       np.asarray(lo, dtype=np.int64), np.asarray(hi, dtype=np.int64), w, fr,
                       np.asarray(ex_off, dtype=np.uint32), np.asarray(ex_lo, dtype=np.int64),
                       np.asarray(ex_hi, dtype=np.int64))

    def _from_hops(self, adj: "_HopAdj", exists: dict[int, Ivs], extra: Any) -> None:
        """The arrays from a hop graph's traversals (sorted by start, then
        edge order: the lists' order), every step weight 1 at scale 1."""
        c = adj.cols
        U, V = c["U"], c["V"]
        self.ok, self.k = True, 0
        ids = set(adj.span) | set(np.unique(V).tolist()) | set(extra)
        self.order = sorted(ids)
        self.local = {g: i for i, g in enumerate(self.order)}
        order = np.asarray(self.order, dtype=np.int64)
        lu = np.searchsorted(order, U)
        off = np.concatenate([[0], np.cumsum(np.bincount(lu, minlength=order.size))])
        ex_off, ex_lo, ex_hi = [0], [], []
        for g in self.order:
            for a_, b_ in exists.get(g, ()):
                ex_lo.append(a_)
                ex_hi.append(b_)
            ex_off.append(len(ex_lo))
        n = int(U.size)
        self.arrays = (off.astype(np.uint32), np.searchsorted(order, V).astype(np.uint32),
                       c["LO"].astype(np.int64), c["HI"].astype(np.int64), [1] * n, [False] * n,
                       np.asarray(ex_off, dtype=np.uint32), np.asarray(ex_lo, dtype=np.int64),
                       np.asarray(ex_hi, dtype=np.int64))

    def _from_arrays(self, adj: Any, kept: tuple, exists: dict[int, Ivs], extra: Any) -> None:
        """The same arrays from a whole seeded graph's kept traversals
        (sorted by start then edge order: each start's list in its `done`
        order, every list of `done` being the whole table's); the scale from
        the distinct weights in use, as the lists give it."""
        U, V, LO, HI, W, objs, used = kept
        k, ok = 0, True
        for j in used:
            x = objs[j]
            if type(x) is Fraction:
                d = x.denominator
                if d & (d - 1):
                    ok = False
                k = max(k, d.bit_length() - 1)
        self.ok = ok and k <= 90
        self.k = k
        if not self.ok:
            return
        one = 1 << k
        scaled = [(x.numerator * (one // x.denominator), True) if type(x) is Fraction
                  else (int(x) << k, False) for x in objs]
        ids = set(adj.done) | set(np.unique(V).tolist()) | set(extra)
        self.order = sorted(ids)
        self.local = {g: i for i, g in enumerate(self.order)}
        order = np.asarray(self.order, dtype=np.int64)
        lu = np.searchsorted(order, U)
        off = np.concatenate([[0], np.cumsum(np.bincount(lu, minlength=order.size))])
        ex_off, ex_lo, ex_hi = [0], [], []
        for g in self.order:
            for a_, b_ in exists.get(g, ()):
                ex_lo.append(a_)
                ex_hi.append(b_)
            ex_off.append(len(ex_lo))
        wl = W.tolist()
        self.arrays = (off.astype(np.uint32), np.searchsorted(order, V).astype(np.uint32),
                       LO.astype(np.int64), HI.astype(np.int64), [scaled[j][0] for j in wl],
                       [scaled[j][1] for j in wl],
                       np.asarray(ex_off, dtype=np.uint32), np.asarray(ex_lo, dtype=np.int64),
                       np.asarray(ex_hi, dtype=np.int64))


def _win_search_native(g: "_NativeGraph", s: int, va: int, vb: int, max_hops: int | None,
                       targets: set[int] | None = None, bound: Any = None,
                       first_only: bool = False) -> dict[int, list[tuple[Ivs, Any]]] | None:
    """`_win_search` in the engine (`winsearch.rs`): the same pieces. None
    when the engine lacks the search or a value is not at the graph's scale
    (the caller searches in Python)."""
    try:
        from gnomon import _engine
    except ImportError:
        return None
    if not hasattr(_engine, "win_search") or not g.ok or s not in g.local:
        return None
    k = g.k
    sb = None if bound is None else _scaled(bound, k)
    if bound is not None and sb is None:
        return None
    if any(t not in g.local for t in (targets or ())):
        return None
    try:
        node, cost, frac, ivs_off, ivs_lo, ivs_hi = _engine.win_search(
            *g.arrays, g.local[s], int(va), int(vb),
            None if max_hops is None else _scaled(max_hops, k),
            [g.local[t] for t in (targets or ())], sb, bool(first_only), MAX_EXPANSIONS)
    except OverflowError as e:
        raise CostError("paths expansion budget exceeded", estimate={"expansions_est": str(e)},
                        suggestions=["narrow valid_window", "name fewer sources"]) from None
    pieces: dict[int, list[tuple[Ivs, Any]]] = {}
    den = 1 << k
    order = g.order
    for j, (u, c, f) in enumerate(zip(node, cost, frac)):
        ivs = list(zip(ivs_lo[ivs_off[j]:ivs_off[j + 1]], ivs_hi[ivs_off[j]:ivs_off[j + 1]]))
        val = Fraction(c, den) if f else c >> k
        pieces.setdefault(order[u], []).append((ivs, val))
    return pieces


def _win_preds(radj: dict[int, list[tuple]], pieces: dict[int, list[tuple[Ivs, Any]]],
               v: int, within: Ivs) -> dict[int, Ivs]:
    """The instants of `within` at which each predecessor `p` of `v` is
    optimal: `cost_t(p) + w = cost_t(v)` over an edge valid at `t`."""
    from gnomon.plan.eval.covalid import ivs_and, ivs_or

    out: dict[int, Ivs] = {}
    for p, lo, hi, wt in radj.get(v, ()):
        for kp, dp in pieces.get(p, ()):
            for kv, dv in pieces.get(v, ()):
                if dp + wt != dv:
                    continue
                x = ivs_and(ivs_and(ivs_and(kp, kv), [(lo, hi)]), within)
                if x:
                    out[p] = ivs_or(out.get(p, []), x)
    return out


def _window_enum_rows(adapter: StorageAdapter, args: dict[str, Any], sources: list[int],
                      targets: list[int] | None, name) -> list[dict[str, Any]]:
    """`all_paths` and `k_paths` under `valid_window`: every node-simple path
    of `min_hops..max_hops` steps whose edges and nodes are valid at one
    shared instant of the window, through edges whose start times follow the
    time order when time-respecting (some choice of the parallel edges must
    do both). A path's cost is its steps' least weights over the window, as
    the instant form's is over its snapshot."""
    from gnomon.plan.eval.covalid import ivs_and

    adj, exists, sspec, stab, both = _win_graph(adapter, args, extra=list(sources) + list(targets or ()))
    w = args["valid_window"]
    va, vb = int(w["t_a"]), int(w["t_b"])
    tr = args.get("time_respecting")
    strict = None if tr is None else tr == "strict"
    steps: dict[int, dict[int, list[tuple[int, int, int]]]] = defaultdict(lambda: defaultdict(list))
    cost: dict[tuple[int, int], Any] = {}
    for u, lst in adj.items():
        for v, lo, hi, wt, _sc, ts in lst:
            steps[u][v].append((ts, lo, hi))
            if (u, v) not in cost or wt < cost[(u, v)]:
                cost[(u, v)] = wt
    for u in steps:
        for v in steps[u]:
            steps[u][v].sort()
    lo_h, hi_h = args.get("min_hops") or 1, int(args["max_hops"])
    tset = None if targets is None else set(targets)
    rows: list[dict[str, Any]] = []
    budget = [0]

    def score(path: list[int]) -> Any:
        return sum(((_value(sspec, stab, a, b, {}, both) or 0) for a, b in zip(path, path[1:])), 0)

    for s in sources:
        found: dict[tuple[int, ...], Any] = {}

        def rec(u: int, path: list[int], c: Any, t_prev: int | None, ivs: list) -> None:
            budget[0] += 1
            if budget[0] > MAX_EXPANSIONS:
                raise CostError("paths expansion budget exceeded",
                                estimate={"expansions_est": budget[0]},
                                suggestions=["reduce max_hops", "narrow valid_window"])
            n = len(path) - 1
            if n >= lo_h and n >= 1 and (tset is None or u in tset):
                found.setdefault(tuple(path), c)
            if n == hi_h:
                return
            for v in sorted(steps.get(u, {})):
                if v in path:
                    continue
                for ts, lo, hi in steps[u][v]:
                    if strict is not None and t_prev is not None and \
                            not (ts > t_prev if strict else ts >= t_prev):
                        continue
                    k = ivs_and(ivs_and(ivs, [(lo, hi)]), exists.get(v, []))
                    if not k:
                        continue
                    path.append(v)
                    rec(v, path, c + cost[(u, v)], ts if strict is not None else None, k)
                    path.pop()

        start = ivs_and([(va, vb)], exists.get(s, []))
        if start:
            rec(s, [s], 0, None, start)
        listed = sorted(found.items())
        if args["result"] == "k_paths":
            by_pair: dict[int, list] = defaultdict(list)
            for p, c in listed:
                by_pair[p[-1]].append((c, len(p) - 1, [name(x) for x in p], list(p)))
            listed = []
            for _, lst in sorted(by_pair.items(), key=lambda kv: name(kv[0])):
                lst.sort(key=lambda x: (x[0], x[1], x[2]))
                listed += [(tuple(x[3]), x[0]) for x in lst[: args["k"]]]
        for p, c in listed:
            row = {"src": name(s), "dst": name(p[-1]), "hops": len(p) - 1,
                   "path": [name(x) for x in p]}
            if args["metric"] == "weight":
                row["cost"] = _out_num(c)
            if sspec is not None:
                row["_sx"] = score(list(p))
                row["score"] = _out_num(row["_sx"])
            rows.append(row)
    return _ordered(rows, args, args["result"] == "k_paths")


def _least_cost(pieces: list[tuple[Ivs, Any]]) -> Any:
    """The least of a node's `(instants, cost)` pieces. Costs are exact:
    an int, or a Fraction once a fractional weight is on the path, and the two
    can be equal (3 + 3 and 2.5 + 3.5), while the answer prints them as 6 and
    6.0. The one reported is the cost of the earliest instant that attains the
    least -- the definition reads the instants in ascending order and replaces
    a node's cost only by a smaller one -- so an exact tie reads the same in
    whatever order the search settled the pieces."""
    d = min(c for _k, c in pieces)
    return min((k[0][0], i, c) for i, (k, c) in enumerate(pieces) if c == d)[2]


def _joined(adj: dict[int, list[tuple]], s: int, targets: set[int]) -> bool:
    """Whether some walk over the traversal lists (any instants, any
    weights) joins `s` to a target: breadth-first from both sides, the
    smaller frontier first; a target with no step at all ends it at once."""
    if s in targets:
        return True
    radj: dict[int, set[int]] | None = None
    fwd, bwd = {s}, set(targets)
    seen_f, seen_b = {s}, set(targets)
    while fwd and bwd:
        if len(fwd) <= len(bwd):
            nxt = {e[0] for u in fwd for e in adj.get(u, ())} - seen_f
            if nxt & seen_b:
                return True
            seen_f |= nxt
            fwd = nxt
        else:
            if radj is None:
                radj = defaultdict(set)
                for u, lst in adj.items():
                    for e in lst:
                        radj[e[0]].add(u)
            nxt = {p for v in bwd for p in radj.get(v, ())} - seen_b
            if nxt & seen_f:
                return True
            seen_b |= nxt
            bwd = nxt
    return False


def _window_rows(adapter: StorageAdapter, args: dict[str, Any], sources: list[int],
                 targets: list[int] | None, name) -> list[dict[str, Any]]:
    """`distance`, `one_path` and `all_optimal` under `valid_window`."""
    from gnomon.plan.eval.covalid import ivs_norm

    adj, exists, sspec, stab, both = _win_graph(adapter, args, seeded=True,
                                                extra=list(sources) + list(targets or ()),
                                                arrays=True)
    w = args["valid_window"]
    va, vb = int(w["t_a"]), int(w["t_b"])
    tset = None if targets is None else set(targets)
    res = args["result"]
    hop_bound = args.get("max_hops") if args["metric"] == "hops" else None

    def score(path: list[int]) -> Any:
        return sum(((_value(sspec, stab, a, b, {}, both) or 0) for a, b in zip(path, path[1:])), 0)

    per_pair: list[tuple[int, int, Any, dict]] = []
    best_pair = None
    native_graph: _NativeGraph | None = None
    static = adj.raw if isinstance(adj, _SeededWeights) else adj
    arrays: list[np.ndarray] = []

    def joined(s_: int, live_: set[int]) -> bool:
        """`_joined` over the static lists, by `_reaches` on their arrays
        (built once a call)."""
        if not arrays:
            cols = getattr(adj, "cols", None) if isinstance(adj, (_SeededWeights, _HopAdj)) \
                else None
            if cols is not None:
                # the seeded graph's traversals, sorted by start already
                arrays.extend([cols["U"], cols["V"]])
            else:
                us = np.fromiter((u for u, lst in static.items() for _ in lst), dtype=np.int64)
                vs = np.fromiter((e[0] for lst in static.values() for e in lst), dtype=np.int64)
                order = np.argsort(us, kind="stable")
                arrays.extend([us[order], vs[order]])
        return _reaches(arrays[0], arrays[1], s_, live_)
    if isinstance(adj, _SeededWeights) and len(sources) > 1:
        adj.prime([s for s in sources if tset is None or joined(s, tset)])
    for s in sources:
        live = tset
        if live is not None and not joined(s, live):
            continue                       # no walk at all: unreachable
        bound = best_pair if args["pairs"] == "min_over_pairs" else None
        first_only = args["pairs"] == "min_over_pairs"
        pieces = None
        if isinstance(adj, _SeededWeights) and not getattr(adj, "whole", False):
            try:
                pieces = _win_search(adj, exists, s, va, vb, hop_bound, targets=live, bound=bound,
                                     first_only=first_only, abort_on_whole=True)
            except _Whole:
                pieces = None
        if pieces is None:
            if native_graph is None:
                native_graph = _NativeGraph(adj, exists, list(sources) + list(tset or ()))
            pieces = _win_search_native(native_graph, s, va, vb, hop_bound, targets=live,
                                        bound=bound, first_only=first_only)
        if pieces is None:
            pieces = _win_search(adj, exists, s, va, vb, hop_bound, targets=live, bound=bound,
                                 first_only=first_only)
        cands = sorted(pieces) if tset is None else sorted(t for t in tset if t in pieces)
        for t in cands:
            if t == s and tset is None:
                continue
            per_pair.append((s, t, _least_cost(pieces[t]), pieces))
            if best_pair is None or per_pair[-1][2] < best_pair:
                best_pair = per_pair[-1][2]
    if args["pairs"] == "min_over_pairs" and per_pair:
        best = min(d for _, _, d, _ in per_pair)
        per_pair = [x for x in per_pair if x[2] == best]
    per_pair.sort(key=lambda x: (name(x[0]), name(x[1])))
    radj: dict[int, list[tuple]] = defaultdict(list)
    if res != "distance":
        for u, lst in adj.items():
            for v, lo, hi, wt, _sc, _ts in lst:
                radj[v].append((u, lo, hi, wt))
    rows: list[dict[str, Any]] = []
    for s, t, d, pieces in per_pair:
        base = {"src": name(s), "dst": name(t), "distance": _out_num(d)}
        if res == "distance":
            rows.append(base)
            continue
        at = ivs_norm([x for k, c in pieces[t] if c == d for x in k])
        if res == "one_path":
            path, cur = [t], at
            while path[-1] != s:
                opts = _win_preds(radj, pieces, path[-1], cur)
                p = min(opts, key=name)
                cur = opts[p]
                path.append(p)
            path.reverse()
            row = {**base, "path": [name(x) for x in path]}
            if sspec is not None:
                row["score"] = _out_num(score(path))
            rows.append(row)
            continue
        found: list[list[int]] = []
        budget = [0]

        def rec(v: int, cur: Ivs, path: list[int]) -> None:
            budget[0] += 1
            if budget[0] > MAX_EXPANSIONS:
                raise CostError("paths: too many optimal paths to enumerate",
                                estimate={"expansions_est": budget[0]},
                                suggestions=["ask for distance or one_path"])
            if v == s:
                found.append(list(reversed(path)))
                return
            opts = _win_preds(radj, pieces, v, cur)
            for p in sorted(opts, key=name):
                path.append(p)
                rec(p, opts[p], path)
                path.pop()

        rec(t, at, [t])
        for path in found:
            row = {**base, "path": [name(x) for x in path]}
            if sspec is not None:
                row["_sx"] = score(path)
                row["score"] = _out_num(row["_sx"])
            rows.append(row)
    if res == "all_optimal":
        rows = _ordered(rows, args, False)
    return rows


# --------------------------------------------------------------------------- #
# admission cost                                                               #
# --------------------------------------------------------------------------- #

#: The admission model's rates, ms per million units, fitted on one
#: measurement host to the calibration calls on FinBench SF1, an SNB SF1 store,
#: a power-law store and K1M/K10M, each from the shapes whose time that component
#: dominates, rounded up.
PATHS_COST_MS_PER_M = {
    # edge versions read by frontier rounds: the read plus the per-row
    # version-id dedup of `_read_edges`
    "frontier_row": 2_000.0,
    # edge versions read whole, the graph built by `_graph_columns`
    "whole_row": 400.0,
    # edge versions the graph is built from by the per-edge loop (time order,
    # properties, a truncation or a score)
    "loop_row": 3_000.0,
    # edge versions over a valid_window: the per-edge loop of `_win_graph`
    # (two traversals per edge for dir both) and the node intervals
    "window_row": 8_000.0,
    # A weighted valid_window graph from an inline pattern's weights is
    # built from the traversal arrays, not by the loop:
    # versions) 150 ms for the edges and 86 ms for `node_intervals`, i.e.
    # 0.64 us an edge version and 0.023 us a node version; priced ~1.55x over
    "window_weight_row": 1_000.0,
    "window_node": 35.0,
    # edges relaxed by the per-source searches over the loaded graph
    "search": 700.0,
    # enumeration expansions (`_enum_simple`, `_window_enum_rows`)
    "enum": 3_600.0,
    # time-respecting enumeration: the time check of each expansion's edges
    "candidate": 1_000.0,
    # result rows: names, the rows, their order
    "out": 5_000.0,
    # relation rows an inline pattern (sources, targets, weight, score) reads
    "pattern": 150.0,
}
#: Fixed costs: a call, a frontier round (one `edges_columnar` read), a pattern.
PATHS_CALL_MS = 2.0
PATHS_ROUND_MS = 5.0
PATTERN_CALL_MS = 30.0
#: An unbounded hop step inside an inline pattern (`hops: {max: null}`) is
#: charged as this many reads of its relation.
PATTERN_UNBOUNDED_HOPS = 4


def _window_weight_priced(args: dict[str, Any]) -> bool:
    """Whether a `valid_window` call is priced at the array-built
    weighted graph's rates: `metric: weight` from an inline pattern, with no
    property weight, `edge_where`, truncation or score (`_win_graph`'s loop
    time at SF1 (2,260 against 262 ms) and refused at SF10 (22 s estimated,
    every other system under 10 s). `GNOMON_PATHS_WINDOW_COST=v1` restores the
    `window_row` price for every valid_window call."""
    if os.environ.get("GNOMON_PATHS_WINDOW_COST", "").strip() == "v1":
        return False
    w = args.get("weight")
    return (args.get("metric") == "weight" and isinstance(w, dict) and "match" in w
            and "prop" not in w and not args.get("edge_where") and args.get("truncate") is None
            and args.get("score") is None)


def _window_hops_priced(args: dict[str, Any]) -> bool:
    """Whether a `valid_window` hop-metric search (`distance`,
    `one_path`, `all_optimal`; not an enumeration) is priced at the
    array-built graph's rates: `_window_rows` builds a hop graph with no
    `edge_where`, truncation, score or time order from the traversal arrays
    versions) was estimated 21.4 s and refused; warm it runs 6.5 s (graph
    3.3 s, search 1.9 s). `GNOMON_PATHS_WINDOW_HOPS_COST=0` (read per call)
    keeps `window_row` for it."""
    if os.environ.get("GNOMON_PATHS_WINDOW_HOPS_COST", "").strip() == "0":
        return False
    return (args.get("metric") == "hops" and not args.get("edge_where") and args.get("truncate") is None
            and args.get("score") is None and args.get("time_respecting") is None)


def _rel_rows(stats: dict[str, Any], rels: Any) -> int:
    """The edge versions of `rels` by the store's counts (all for None)."""
    total = int(stats.get("n_edge_versions") or 0)
    counts = stats.get("rel_type_counts") or {}
    if rels is None or not counts:
        return total
    if isinstance(rels, str):
        rels = [rels]
    return sum(int(counts.get(r, 0)) for r in rels)


def _started_by(t: Any, stats: dict[str, Any]) -> float:
    """The share of the valid-time extent before `t`: the edge versions that
    overlap a window ending at `t` started before it."""
    vt_min, vt_max = stats.get("vt_min"), stats.get("vt_max")
    if vt_min is None or vt_max is None or vt_max <= vt_min:
        return 1.0
    return max(0.0, min(1.0, (int(t) - vt_min) / (vt_max - vt_min)))


def _share(w: dict[str, Any] | None, stats: dict[str, Any]) -> float:
    """The share of the extent a `{t_a, t_b}` window covers (1 for None)."""
    return 1.0 if w is None else window_fraction({"window": w}, stats)


def _pattern_cost(spec: dict[str, Any], stats: dict[str, Any], deg: float) -> tuple[int, float]:
    """(relation rows read, rows produced) of an inline `match` argument: every
    step reads its relation once (an unbounded hop step a few times); a
    pattern anchored by uids produces their count times the mean degree per
    step, an unanchored one at most its smallest step's relation."""
    reads, out = 0, 0.0
    parts = spec.get("union") if isinstance(spec.get("union"), list) else [spec]
    for part in parts:
        pat = part.get("pattern", part) if isinstance(part, dict) else {}
        steps = [s for s in pat.get("steps", []) if isinstance(s, dict)]
        anchors = [len(nd["uids"]) for nd in pat.get("nodes", [])
                   if isinstance(nd, dict) and isinstance(nd.get("uids"), list)]
        rel_rows = []
        for s in steps:
            r = _rel_rows(stats, s.get("rel_type"))
            hops = s.get("hops")
            k = 1
            if isinstance(hops, dict):
                k = PATTERN_UNBOUNDED_HOPS if hops.get("max") is None else max(1, int(hops["max"]))
            reads += r * k
            rel_rows.append(r)
        if anchors:
            out += min(anchors) * max(1.0, deg) ** len(steps)
        else:
            out += min(rel_rows) if rel_rows else int(stats.get("n_entities") or 0)
        for sub in ("optional", "not", "exists"):
            for x in (pat.get(sub) or []) if isinstance(pat.get(sub), list) else []:
                if isinstance(x, dict):
                    reads += _pattern_cost(x, stats, deg)[0]
    return reads, min(out, float(MAX_PATTERN_ROWS))


def _depth(g: float, size: float) -> int:
    """Rounds for a frontier growing by `g` per round to reach `size` vertices."""
    if g <= 1.0:
        return max(1, int(math.ceil(math.log2(max(2.0, size)))))
    return max(1, int(math.ceil(math.log(max(2.0, size)) / math.log(g))))


def _geo(g: float, rounds: int) -> float:
    """1 + g + ... + g^(rounds - 1): the vertices a frontier expands."""
    return float(rounds) if g == 1.0 else (g ** rounds - 1.0) / (g - 1.0)


def _arrival_cost(args: dict[str, Any], stats: dict[str, Any], n_src: float,
                  n_dst: float | None) -> dict[str, int]:
    """`metric: arrival`: one temporal_reachability per source, or one
    temporal_paths per (source, target) pair, at those operators' own prices."""
    from gnomon.temporal.guardrails import add_time_estimate
    from gnomon.temporal.ops_paths import _paths_cost as tp_cost
    from gnomon.temporal.ops_paths import _reach_cost

    if args["result"] == "distance":
        one = add_time_estimate("temporal_reachability", _reach_cost(args, stats))
        times = n_src
    else:
        one = add_time_estimate("temporal_paths", tp_cost(
            {"window": args["window"], "max_hops": args.get("max_hops") or 4}, stats))
        times = n_src * (n_dst or 1)
    return {k: int(v * times) for k, v in one.items()}


def _paths_cost(args: dict[str, Any], stats: dict[str, Any]) -> dict[str, int]:
    """Admission estimate of a `paths` call, from the work the search does.
    The old model was `scan_estimate` -- every edge and node version,
    at the uncalibrated 170 ms/M -- a whole-graph search, which a bounded
    search has not been since frontier-first loading, the bidirectional stop,
    seeding and the fail-fast forms; at FinBench SF10 (54.9M edge versions)
    it estimated 10.4 s for TCR3 and TCR5 calls that run in ~10 ms, and
    refused them.

    THE GRAPH. The relation is `rel_types`' versions by the store's counts.
    A vertex's mean degree is the store's edge versions per entity (a
    relation touching a share of the vertices holds about that share of the
    edges); distinct neighbours are at most n - 1, `dir: both` doubles the
    branching and a truncation caps it at its limit. The statistics count
    versions, not the edges valid at one instant, so the share a read keeps
    is the extent's share before its end, starts taken as spread over the
    extent: a snapshot keeps the versions started by `t_valid`, a
    valid_window those started by its end; a time-respecting `window` and
    an `edge_window` keep the share of the extent they cover.

    THE READ. A bounded search from known sources (an enumeration, or a hop
    distance) reads frontier-first: per round the frontier's incident edges,
    growing by the branching, to `max_hops` rounds, or (no bound) until the
    frontier covers the vertices -- with one source and one target from both
    ends until they meet, about sqrt(n) vertices a side. A round that would
    pass half the relation turns into one whole read. The weighted optimal
    forms read the relation whole, the window forms the window whole, edge
    by edge.

    THE SEARCH. The optimal forms search the loaded graph once per source,
    over the source's ball (the vertices within the hop bound). An
    enumeration expands S * (g + g^2 + ... + g^max_hops) paths, the term at
    depth i divided by i! when time-respecting (a random order of i steps is
    increasing once in i!), every time-ordered expansion checking its
    edges' times. Its time is priced up to the runtime budget
    (`MAX_EXPANSIONS`), where it stops and fails, and the uncounted
    estimate is `expansions_est`: past the unit ceiling (640 times the
    budget) the call is refused at admission.

    Inline patterns (sources, targets, weight, score) read each step's
    relation once; an anchored pattern's rows are its uids times the mean
    degree per step. The estimate is a guide, an upper bound in order of
    magnitude on the stores measured; the runtime budgets stay the exact
    backstop. `GNOMON_TIME_COEFF_SCALE` scales it like the operator table.
    """
    n = max(1, int(stats.get("n_entities") or 1))
    deg = int(stats.get("n_edge_versions") or 0) / n
    rate = PATHS_COST_MS_PER_M
    pattern_reads, n_patterns = 0, 0

    def endpoints(spec: dict[str, Any] | None) -> float | None:
        nonlocal pattern_reads, n_patterns
        if spec is None:
            return None
        if "uids" in spec:
            return float(len(spec["uids"]))
        reads, rows = _pattern_cost(spec.get("match") or {}, stats, deg)
        pattern_reads += reads
        n_patterns += 1
        return rows

    n_src = endpoints(args["sources"]) or 0.0
    n_dst = endpoints(args.get("targets"))
    for side in ("weight", "score"):
        w = args.get(side)
        if w is not None and "match" in w:
            pattern_reads += _pattern_cost(w["match"], stats, deg)[0]
            n_patterns += 1
    pattern_ms = pattern_reads * rate["pattern"] / 1e6 + n_patterns * PATTERN_CALL_MS
    if args["metric"] == "arrival":
        est = _arrival_cost(args, stats, max(1.0, n_src), n_dst)
        est["time_est_ms"] = int(est.get("time_est_ms", 0) + pattern_ms + PATHS_CALL_MS)
        est["rows_scanned_est"] = int(est.get("rows_scanned_est", 0) + pattern_reads)
        return est

    res, tr = args["result"], args.get("time_respecting")
    vw, trunc = args.get("valid_window"), args.get("truncate")
    enum = res in ("all_paths", "k_paths")
    rel = _rel_rows(stats, args.get("rel_types"))
    if vw is not None:
        read_f = keep_f = _started_by(vw["t_b"], stats)
    elif tr is not None:
        read_f = 1.0 if trunc is not None else _started_by(args["window"]["t_b"], stats)
        keep_f = _share(args["window"], stats)
    else:
        # a snapshot holds the versions valid at t_valid: at most those started by it
        read_f = keep_f = _started_by(int(args["t_valid"]) + 1, stats)
    keep_f *= _share(args.get("edge_window"), stats)
    whole = rel * read_f
    both = 2.0 if args["dir"] == "both" else 1.0
    g = deg * keep_f * both
    if trunc is not None:
        g = min(g, trunc["limit"] * both)
    g = min(g, float(n - 1))
    per_vertex = 2.0 * deg * read_f           # a touching read returns both directions
    hops = args.get("max_hops")

    rounds, read_ms = 0, 0.0
    depth = int(hops) if hops else None
    if vw is not None:
        loaded = whole
        if _window_weight_priced(args) or (not enum and _window_hops_priced(args)):
            read_ms = (whole * rate["window_weight_row"]
                       + int(stats.get("n_node_versions") or n) * rate["window_node"]) / 1e6
        else:
            read_ms = whole * rate["window_row"] / 1e6
    elif n_src and (enum or args["metric"] == "hops"):
        bidir = (not enum and tr is None and trunc is None and n_src == 1 and n_dst == 1)
        if bidir:
            side = int(math.ceil(hops / 2)) if hops else _depth(g, math.sqrt(n))
            rows, rounds = 2.0 * per_vertex * min(n, _geo(g, side)), 2 * side
        else:
            depth = depth or _depth(g, n)
            rows, rounds = n_src * per_vertex * min(n, _geo(g, depth)), depth
        if rows * FRONTIER_WHOLE_FRACTION > whole:
            # the relation whole, after the rounds whose next one would have
            # passed half of it: that round is g times the rows read so far
            read_ms = (whole / (FRONTIER_WHOLE_FRACTION * max(g, 1.0)) * rate["frontier_row"]
                       + whole * rate["whole_row"]) / 1e6
            loaded = whole
        else:
            read_ms = rows * rate["frontier_row"] / 1e6
            loaded = rows
    else:
        loaded = whole
        read_ms = whole * rate["whole_row"] / 1e6
    w = args.get("weight")
    looped = (tr is not None or trunc is not None or bool(args.get("edge_where"))
              or args.get("score") is not None or (w is not None and "prop" in w))
    build_ms = loaded * rate["loop_row"] / 1e6 if looped and vw is None else 0.0

    sources = max(1.0, n_src)
    if enum:
        paths_est, level = 0.0, sources
        for i in range(1, int(hops) + 1):
            level *= g / (i if tr is not None else 1)
            paths_est += level
        # the runtime budget stops the enumeration: past it the call fails,
        # having spent the budget and produced nothing
        within = paths_est <= MAX_EXPANSIONS
        run = paths_est if within else float(MAX_EXPANSIONS)
        search_ms = run * rate["enum"] / 1e6 + \
            (run * g * rate["candidate"] / 1e6 if tr is not None else 0.0)
        out = 0.0
        if within:
            out = paths_est if n_dst is None else paths_est * min(1.0, n_dst / n)
            if res == "k_paths" and n_dst is not None:
                out = min(out, sources * n_dst * int(args.get("k") or 1))
        expansions = paths_est
    else:
        # each source's search covers its ball: the vertices within the hop
        # bound (all of them without one), and their edges in the loaded graph
        ball = float(n) if depth is None or vw is not None else min(float(n), _geo(g, depth + 1))
        expansions = sources * min(loaded, ball * max(g, 1.0))
        search_ms = expansions * rate["search"] / 1e6
        out = sources * (min(ball, loaded) if n_dst is None else n_dst)
        if args["pairs"] == "min_over_pairs":
            out = min(out, max(1.0, n_dst or 1.0))
    out_ms = out * rate["out"] / 1e6
    time_ms = (PATHS_CALL_MS + rounds * PATHS_ROUND_MS + read_ms + build_ms + search_ms
               + out_ms + pattern_ms)
    time_ms *= float(os.environ.get("GNOMON_TIME_COEFF_SCALE", "1.0"))
    cap = 2.0 ** 62
    return {"rows_scanned_est": int(min(loaded + pattern_reads, cap)),
            "expansions_est": int(min(expansions, cap)),
            "time_est_ms": int(min(time_ms, cap))}


# --------------------------------------------------------------------------- #
# the operator                                                                 #
# --------------------------------------------------------------------------- #

def _arrival(adapter: StorageAdapter, args: dict[str, Any], sources: list[int],
             targets: list[int] | None) -> list[dict[str, Any]]:
    """`metric: arrival`: the two temporal operators' own kernels, per source."""
    from gnomon.temporal.ops_paths import temporal_paths, temporal_reachability

    rows: list[dict[str, Any]] = []
    s_uids = adapter.uids_for(sources)
    tset = None if targets is None else set(adapter.uids_for(targets))
    for su in s_uids:
        if args["result"] == "distance":
            got = temporal_reachability(adapter, {
                "src": su, "window": args["window"], "as_of_tt": args["as_of_tt"],
                "delta_max_wait": args.get("delta_max_wait"),
                "direction": args["dir"], "limit": 10_000, "cursor": None})
            for r in got["rows"]:
                if tset is None or r["uid"] in tset:
                    rows.append({"src": su, "dst": r["uid"], "arrival": r["earliest_arrival"]})
        else:
            for tu in sorted(tset or ()):
                got = temporal_paths(adapter, {
                    "src": su, "dst": tu, "window": args["window"], "k": args["k"],
                    "max_hops": args.get("max_hops") or 4, "as_of_tt": args["as_of_tt"]})
                for r in got["rows"]:
                    rows.append({"src": su, "dst": tu, "arrival": r["arrival"],
                                 "hops": r["hops"], "edges": r["edges"]})
    return rows


#: The rows of recent multi-page `paths` calls, by (store handle, store
#: generation, the arguments but the page): a later page is read from them
#: 19,560 all_paths rows: ~75 s a 10,000-row page, every page re-running the
#: search). The rows are those the call computes, in its order, so each page
#: is the one the operator would return. `GNOMON_PATHS_PAGE_MEMO=0` re-runs.
_PAGE_MEMO: dict[Any, list[dict[str, Any]]] = {}
PAGE_MEMO_ENTRIES = 4
PAGE_MEMO_MAX_ROWS = 1_000_000


def _page_key(adapter: StorageAdapter, args: dict[str, Any]) -> Any:
    if os.environ.get("GNOMON_PATHS_PAGE_MEMO", "1").strip() == "0":
        return None
    gen = getattr(adapter, "generation", None)
    try:
        gen = gen() if callable(gen) else gen
    except Exception:  # noqa: BLE001 - no generation, no memo
        return None
    if gen is None:
        return None
    try:
        rest = json.dumps({k: v for k, v in args.items() if k not in ("limit", "cursor")},
                          sort_keys=True, default=str)
    except (TypeError, ValueError):
        return None
    return (id(adapter), gen, rest)


@operator(
    "paths",
    {
        "sources": required(ENDPOINTS),
        "targets": {**ENDPOINTS, "type": ["object", "null"], "default": None},
        "t_valid": TIMESTAMP,
        "window": WINDOW,
        "as_of_tt": AS_OF_TT,
        "rel_types": {"type": ["array", "null"], "items": {"type": "string"}, "default": None},
        "dir": {"type": "string", "enum": ["out", "in", "both"], "default": "out"},
        "edge_where": EDGE_WHERE,
        "edge_window": WINDOW,
        "valid_window": VALID_WINDOW,
        "truncate": TRUNCATE,
        "metric": {"type": "string", "enum": ["hops", "weight", "arrival"], "default": "hops"},
        "weight": PAIR_VALUES,
        "score": PAIR_VALUES,
        "time_respecting": {"type": ["string", "null"], "enum": ["non_strict", "strict", None],
                            "default": None},
        "delta_max_wait": {"type": ["integer", "null"], "minimum": 1, "default": None},
        "min_hops": {"type": "integer", "minimum": 1},
        "max_hops": {"type": "integer", "minimum": 1},
        "result": {"type": "string", "default": "distance",
                   "enum": ["distance", "one_path", "all_optimal", "k_paths", "all_paths"]},
        "k": {"type": "integer", "minimum": 1, "maximum": MAX_K},
        "pairs": {"type": "string", "enum": ["each", "min_over_pairs"], "default": "each"},
        "order": {"type": "string", "enum": ["path", "hops_desc", "score_desc"],
                  "default": "path"},
        "limit": LIMIT,
        "cursor": CURSOR,
    },
    "Paths between sources and targets: hop or weight distances, one or all "
    "optimal paths, k best or all simple paths, optionally time-respecting; "
    "sources, targets and weights from uid lists, properties or inline match "
    "patterns. Engine-level only.",
    cost_fn=_paths_cost,
    validators=[_validators],
    output_fields=("rows", "rows_total", "truncated", "cursor"),
)
def paths(adapter: StorageAdapter, args: dict[str, Any]) -> dict[str, Any]:
    key = _page_key(adapter, args)
    rows = _PAGE_MEMO.get(key) if key is not None and args.get("cursor") else None
    if rows is None:
        rows = _paths_rows(adapter, args)
        if key is not None and args["limit"] < len(rows) <= PAGE_MEMO_MAX_ROWS:
            if len(_PAGE_MEMO) >= PAGE_MEMO_ENTRIES:
                _PAGE_MEMO.pop(next(iter(_PAGE_MEMO)))
            _PAGE_MEMO[key] = rows
    return paginate(rows, args["limit"], args["cursor"])


def _paths_rows(adapter: StorageAdapter, args: dict[str, Any]) -> list[dict[str, Any]]:
    """Every row of a `paths` call, in its order (`paths` pages them)."""
    sources = _endpoints(adapter, args["sources"], args, strict=True)
    targets = _endpoints(adapter, args.get("targets"), args, strict=False)
    if args["metric"] == "arrival":
        rows = _arrival(adapter, args, sources, targets)
        return rows
    if args.get("valid_window") is not None:
        names = dict(zip(sources + (targets or []),
                         adapter.uids_for(sources + (targets or []))))

        def wname(i: int) -> str:
            if i not in names:
                names[i] = adapter.uids_for([i])[0]
            return names[i]

        if args["result"] in ("all_paths", "k_paths"):
            rows = _window_enum_rows(adapter, args, sources, targets, wname)
        else:
            rows = _window_rows(adapter, args, sources, targets, wname)
        return rows
    g = _graph(adapter, args, sources, targets)
    tset = None if targets is None else set(targets)
    res = args["result"]
    uid = dict(zip(sources + (targets or []),
                   adapter.uids_for(sources + (targets or []))))

    def name(i: int) -> str:
        if i not in uid:
            uid[i] = adapter.uids_for([i])[0]
        return uid[i]

    rows: list[dict[str, Any]] = []
    if res in ("all_paths", "k_paths"):
        tr = args.get("time_respecting")
        strict = None if tr is None else tr == "strict"
        budget = [0]
        for s in sources:
            found = _enum_simple(g, s, args.get("min_hops") or 1, args["max_hops"], strict,
                                 tset, budget)
            if res == "k_paths":
                by_pair: dict[int, list] = defaultdict(list)
                for p, c in found:
                    by_pair[p[-1]].append((c, len(p) - 1, [name(x) for x in p], p))
                found = []
                for _, lst in sorted(by_pair.items(), key=lambda kv: name(kv[0])):
                    lst.sort(key=lambda x: (x[0], x[1], x[2]))
                    found += [(x[3], x[0]) for x in lst[: args["k"]]]
            for p, c in found:
                row = {"src": name(s), "dst": name(p[-1]), "hops": len(p) - 1,
                       "path": [name(x) for x in p]}
                if args["metric"] == "weight":
                    row["cost"] = _out_num(c)
                if args.get("score") is not None:
                    row["_sx"] = _score(g, p)
                    row["score"] = _out_num(row["_sx"])
                rows.append(row)
        rows = _ordered(rows, args, res == "k_paths")
        return rows

    # optimal results
    per_pair: list[tuple[int, int, Any, dict[int, Any]]] = []
    least = None
    mop = args["pairs"] == "min_over_pairs"
    for s in sources:
        dist = _dijkstra(g, s, args.get("max_hops"), targets=tset, bound=least if mop else None)
        cands = sorted(dist) if tset is None else sorted(t for t in tset if t in dist)
        for t in cands:
            if t == s and tset is None:
                continue
            if mop:
                dt = dist[t]
                if least is not None and dt > least:
                    continue    # tentative past the stop, or farther than a pair already found
                if least is None or dt < least:
                    least = dt
            per_pair.append((s, t, dist[t], dist))
    if args["pairs"] == "min_over_pairs" and per_pair:
        best = min(d for _, _, d, _ in per_pair)
        per_pair = [x for x in per_pair if x[2] == best]
    per_pair.sort(key=lambda x: (name(x[0]), name(x[1])))
    preds_of: dict[int, dict[int, list[int]]] = {}
    unscale = None if g.scale is None else g.value
    for s, t, d, dist in per_pair:
        base = {"src": name(s), "dst": name(t),
                "distance": _out_num(d if unscale is None else unscale(d))}
        if res == "distance":
            rows.append(base)
            continue
        if s not in preds_of:
            preds_of[s] = _preds(g, dist)
        pred = preds_of[s]
        if res == "one_path":
            path = [t]
            while path[-1] != s:
                path.append(min(pred[path[-1]], key=name))
            path.reverse()
            row = {**base, "path": [name(x) for x in path]}
            if args.get("score") is not None:
                row["score"] = _out_num(_score(g, path))
            rows.append(row)
        else:
            for path in _all_optimal(pred, s, t, name):
                row = {**base, "path": [name(x) for x in path]}
                if args.get("score") is not None:
                    row["_sx"] = _score(g, path)
                    row["score"] = _out_num(row["_sx"])
                rows.append(row)
    if res == "all_optimal":
        rows = _ordered(rows, args, False)
    return rows


def _all_optimal(pred: dict[int, list[int]], s: int, t: int, name) -> list[list[int]]:
    """Every optimal path s -> t (the predecessor DAG is acyclic: weights are
    positive), in uid-sequence order, within the expansion budget."""
    succ: dict[int, list[int]] = defaultdict(list)
    seen = {t}
    q = deque([t])
    while q:
        v = q.popleft()
        if v == s:
            continue
        for p in pred.get(v, ()):
            succ[p].append(v)
            if p not in seen:
                seen.add(p)
                q.append(p)
    for p in succ:
        succ[p].sort(key=name)
    out: list[list[int]] = []
    budget = [0]

    def rec(u: int, path: list[int]) -> None:
        budget[0] += 1
        if budget[0] > MAX_EXPANSIONS:
            raise CostError("paths: too many optimal paths to enumerate",
                            estimate={"expansions_est": budget[0]},
                            suggestions=["ask for distance or one_path"])
        if u == t:
            out.append(list(path))
            return
        for v in succ.get(u, ()):
            path.append(v)
            rec(v, path)
            path.pop()

    if s == t:
        return [[s]]
    if s in seen:
        rec(s, [s])
    return out


def _ordered(rows: list[dict[str, Any]], args: dict[str, Any], keep: bool) -> list[dict]:
    """Rows in the requested order; `path` (the default) is (src, dst, hops,
    vertex sequence) unless the rows already carry k_paths' per-pair order.
    `score_desc` orders by the exact score, ties by the path order."""
    order = args.get("order", "path")
    if order == "path":
        out = rows if keep else sorted(
            rows, key=lambda r: (r["src"], r["dst"], len(r["path"]), r["path"]))
    elif order == "hops_desc":
        out = sorted(rows, key=lambda r: (-len(r["path"]), r["src"], r["dst"], r["path"]))
    else:
        out = sorted(rows, key=lambda r: (-r["_sx"], r["src"], r["dst"],
                                          len(r["path"]), r["path"]))
    for r in out:
        r.pop("_sx", None)
    return out


__all__ = ["paths", "OPEN_END"]
