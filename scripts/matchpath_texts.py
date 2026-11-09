"""`match` texts for W1 and W2 queries that Gnomon answers with operators built for them.

The question this file serves: the same query, the same store, the same instance, answered once by the
dedicated operator and once through the general `match` (pattern) path. Each function below takes the
dedicated operator's own arguments and returns the
dedicated operator's payload shape, computed only from `match` calls made through
`call_operator(adapter, "match", ...)` (admission on, as shipped) plus the client steps named in its
docstring. Nothing here changes or bypasses the engine.

Queries covered: the run's core, `snap.hop2` and `b.snap.hop2@now` (operator `snapshot_subgraph`, also
`b.snap.hop2@T`) and `nbr.evolution` (operator `neighborhood_evolution`); the W1 extension, `series.count`,
`series.count.w10` (`graph_metric_timeseries`, edge_event_count) and `agg.rel_bucket`, `agg.rel_bucket.w10`
(`aggregate_events`, the registry's argument shape only). The expressibility of the other W1 and W2 T and B

`whole=True` returns every edge row (the whole-answer check); the timed form returns the dedicated call's
page (its `limit`, default 100) with the exact totals, as the dedicated operator does.
"""

from __future__ import annotations

import json
from typing import Any, Callable

from gnomon.core.model import OPEN_END
from gnomon.temporal.algebra import DEFAULT_LIMIT, call_operator

#: match's own page maximum (ops_match.MAX_LIMIT): a list a later call reads whole is fetched in pages of it
MATCH_PAGE = 1_000_000


# ------------------------------------------------------------------ expression helpers
def _c(name: str) -> dict:
    return {"col": name}


def _i(v: int) -> dict:
    return {"lit": int(v), "tau": "int"}


def _cmp(op: str, l: dict, r: dict) -> dict:
    return {"cmp": op, "l": l, "r": r}


def _and(*xs: dict) -> dict:
    out = xs[0]
    for x in xs[1:]:
        out = {"bool": "and", "l": out, "r": x}
    return out


def _or(a: dict, b: dict) -> dict:
    return {"bool": "or", "l": a, "r": b}


def valid_at(var: str, t: int) -> dict:
    """The version bound to `var` is valid at instant t: vt_s <= t < vt_e."""
    return _and(_cmp("<=", _c(f"{var}.vt_s"), _i(t)), _cmp(">", _c(f"{var}.vt_e"), _i(t)))


def _scope_instant(t: int, as_of: int) -> dict:
    return {"vt": {"instant": int(t)}, "as_of_tt": int(as_of)}


def _scope_window(t_a: int, t_b: int, as_of: int) -> dict:
    return {"vt": {"window": {"t_a": int(t_a), "t_b": int(t_b)}, "mode": "overlap"}, "as_of_tt": int(as_of)}


def _rows(env: dict) -> list[dict]:
    cols = env.get("columns")
    return [r if isinstance(r, dict) else dict(zip(cols, r)) for r in env.get("rows", [])]


def run_match(adapter, text: dict, limit: int, call: Callable = call_operator) -> tuple[list[dict], int]:
    """One `match` text: the rows of its first `limit` (every row when `limit` is None, page by page), and
    its exact `rows_total`."""
    if limit is not None:
        env = call(adapter, "match", {**text, "limit": int(limit)})
        return _rows(env), int(env["rows_total"])
    args = {**text, "limit": MATCH_PAGE}
    env = call(adapter, "match", args)
    rows, total = _rows(env), int(env["rows_total"])
    while env.get("cursor"):
        env = call(adapter, "match", {**args, "cursor": env["cursor"]})
        rows.extend(_rows(env))
    return rows, total


# ------------------------------------------------------------------ snapshot_subgraph (snap.hop2, b.snap.hop2@*)
def snap_text_nodes(args: dict) -> dict:
    """The reached set of G(t_valid) under belief as_of_tt: every node within `hops` undirected hops of a seed,
    at its least distance over the seeds, ordered (hop, uid) as the operator orders `nodes`.

    `{min: 0, max: h}` keeps each target once per seed, at its shortest distance;
    the group takes the least over the seeds, which is the operator's multi-source BFS distance. A node variable
    under an instant binds only a node with a version valid at t (the snapshot graph G(t)); a seed without one
    does not bind, as the operator drops it."""
    step = {"from": "s", "to": "x", "dir": "both", "hops": {"min": 0, "max": int(args.get("hops", 1))}}
    if args.get("rel_types"):
        step["rel_type"] = list(args["rel_types"])
    return {"pattern": {"nodes": [{"var": "s", "uids": list(args["seeds"])}, {"var": "x"}], "steps": [step]},
            "return": {"group": {"by": [{"as": "uid", "expr": _c("x.uid")}, {"as": "label", "expr": _c("x.label")}],
                                 "aggs": [{"as": "hop", "fn": "min", "of": _c("x.depth")}]},
                       "order_by": [{"expr": _c("hop"), "dir": "asc"}, {"expr": _c("uid"), "dir": "asc"}]},
            "scope": _scope_instant(args["t_valid"], args.get("as_of_tt", OPEN_END))}


def snap_text_edges(args: dict, reached: list[str]) -> dict:
    """The induced edges of the reached set in G(t_valid): every believed edge version valid at t whose two
    endpoints are reached, one row per edge (`dir: out` from a to b binds each stored edge once, a self-loop
    included), ordered (vt_s, vid) as the operator's scan orders them."""
    step = {"var": "e", "from": "a", "to": "b", "dir": "out"}
    if args.get("rel_types"):
        step["rel_type"] = list(args["rel_types"])
    cols = ("eid", "vid", "src", "dst", "rel_type", "vt_s", "vt_e")
    return {"pattern": {"nodes": [{"var": "a", "uids": list(reached)}, {"var": "b", "uids": list(reached)}],
                        "steps": [step]},
            "return": {"rows": [{"as": c, "expr": _c(f"e.{c}")} for c in cols],
                       "order_by": [{"expr": _c("vt_s"), "dir": "asc"}, {"expr": _c("vid"), "dir": "asc"}]},
            "scope": _scope_instant(args["t_valid"], args.get("as_of_tt", OPEN_END))}


def snap(adapter, args: dict, whole: bool = False, call: Callable = call_operator) -> dict:
    """`snapshot_subgraph` through `match`: two calls (the reached set, then its induced edges, the second
    binding the first's uids as a later W3 call binds an earlier call's list). Client steps: the node page is
    the first `limit` rows of the first call; an empty reached set has no edge call."""
    limit = int(args.get("limit") or DEFAULT_LIMIT)
    nodes, _ = run_match(adapter, snap_text_nodes(args), None, call)
    node_rows = [{"uid": r["uid"], "label": r["label"], "hop": int(r["hop"])} for r in nodes]
    if node_rows:
        edges, total = run_match(adapter, snap_text_edges(args, [r["uid"] for r in node_rows]),
                                 None if whole else limit, call)
    else:
        edges, total = [], 0
    edges = [{"eid": r["eid"], "vid": r["vid"], "src": r["src"], "dst": r["dst"], "rel_type": r["rel_type"],
              "vt_s": int(r["vt_s"]), "vt_e": int(r["vt_e"])} for r in edges]
    page = edges if whole else edges[:limit]
    truncated = len(page) < total
    out = {"rows": page, "rows_total": total, "truncated": truncated,
           "cursor": str(len(page)) if truncated else None,
           "nodes": node_rows if whole else node_rows[:limit], "nodes_total": len(node_rows),
           "nodes_truncated": len(node_rows) > limit}
    out["truncated"] = out["truncated"] or out["nodes_truncated"]
    return out


# ------------------------------------------------------------------ neighborhood_evolution (nbr.evolution)
def nbr_stride(args: dict) -> tuple[int, list[int]]:
    t1, t2 = int(args["t1"]), int(args["t2"])
    stride = int(args.get("stride") or max(1, (t2 - t1) // 20))
    return stride, list(range(t1, t2, stride))


def nbr_text_changes(args: dict) -> dict:
    """Neighbours gained and lost between G(t1) and G(t2), undirected, in one call: the incident edge versions
    overlapping [t1, t2 + 1) (every version valid at t1 or at t2 overlaps it), grouped by the other endpoint,
    with c1 = how many of its edges are valid at t1 and c2 = how many at t2; a neighbour is gained iff c1 = 0
    and c2 > 0, lost iff c1 > 0 and c2 = 0. A self-loop is no neighbour (x != n), as in the operator. Under a
    window the variables bind versions (non-sequenced), so c1 and c2 count bindings; only their being zero is
    read."""
    t1, t2 = int(args["t1"]), int(args["t2"])
    z = _i(0)
    c1 = {"coalesce": [_c("c1"), z]}
    c2 = {"coalesce": [_c("c2"), z]}
    return {"pattern": {"nodes": [{"var": "n", "uids": [args["uid"]]}, {"var": "x"}],
                        "steps": [{"var": "e", "from": "n", "to": "x", "dir": "both"}],
                        "where": [_cmp("!=", _c("x.uid"), _c("n.uid"))]},
            "return": {"group": {"by": [{"as": "uid", "expr": _c("x.uid")}],
                                 "aggs": [{"as": "c1", "fn": "count", "where": valid_at("e", t1)},
                                          {"as": "c2", "fn": "count", "where": valid_at("e", t2)}],
                                 "having": _or(_and(_cmp("=", c1, z), _cmp(">", c2, z)),
                                               _and(_cmp(">", c1, z), _cmp("=", c2, z)))},
                       "order_by": [{"expr": _c("uid"), "dir": "asc"}]},
            "scope": _scope_window(t1, t2 + 1, args.get("as_of_tt", OPEN_END))}


def nbr_text_series(args: dict) -> dict:
    """The incident active-edge count at each bucket start b in [t1, t2) (stride as the operator's), in one
    call: the incident edge versions overlapping [t1, t2), one conditional aggregate per bucket counting the
    distinct versions valid at b. A self-loop is one version and counts once (count_distinct of the version
    id), as the operator counts it."""
    t1, t2 = int(args["t1"]), int(args["t2"])
    _, starts = nbr_stride(args)
    aggs = [{"as": f"d{k}", "fn": "count_distinct", "of": _c("e.vid"), "where": valid_at("e", b)}
            for k, b in enumerate(starts)]
    return {"pattern": {"nodes": [{"var": "n", "uids": [args["uid"]]}, {"var": "x"}],
                        "steps": [{"var": "e", "from": "n", "to": "x", "dir": "both"}]},
            "return": {"group": {"by": [], "aggs": aggs}},
            "scope": _scope_window(t1, t2, args.get("as_of_tt", OPEN_END))}


def nbr(adapter, args: dict, whole: bool = False, call: Callable = call_operator) -> dict:
    """`neighborhood_evolution` through `match`: two calls (changes, series). Client steps: the change rows are
    split into gained (c1 = 0) and lost (c2 = 0), each cut at `limit`; a bucket the series call returns no
    value for (no incident version at all: no group row) is 0."""
    t1, t2 = int(args["t1"]), int(args["t2"])
    if not t1 < t2:
        raise ValueError("neighborhood_evolution requires t1 < t2")
    limit = int(args.get("limit") or DEFAULT_LIMIT)
    stride, starts = nbr_stride(args)
    rows, _ = run_match(adapter, nbr_text_changes(args), None, call)
    gained = [r["uid"] for r in rows if not (r.get("c1") or 0)]
    lost = [r["uid"] for r in rows if not (r.get("c2") or 0)]
    srows, _ = run_match(adapter, nbr_text_series(args), None, call)
    s = srows[0] if srows else {}
    series = [{"t": int(b), "degree": int(s.get(f"d{k}") or 0)} for k, b in enumerate(starts)]
    cut = (lambda x: x) if whole else (lambda x: x[:limit])
    return {"neighbors_gained": cut(gained), "neighbors_gained_total": len(gained),
            "neighbors_lost": cut(lost), "neighbors_lost_total": len(lost),
            "degree_series": series, "stride": stride,
            "truncated": len(gained) > limit or len(lost) > limit}


# ------------------------------------------------------------------ edge-event buckets (series.count, agg.rel_bucket, .w10)
def _bucket(var: str, t_a: int, stride: int) -> dict:
    """The start of the stride-wide bucket of [t_a, ...) that holds the version's vt_s."""
    off = {"arith": "-", "l": {"cast": _c(f"{var}.vt_s"), "to": "int"}, "r": _i(t_a)}
    return {"arith": "+", "l": _i(t_a),
            "r": {"arith": "*", "l": {"arith": "floor_div", "l": off, "r": _i(stride)}, "r": _i(stride)}}


def _events_pattern(t_a: int | None = None, t_b: int | None = None) -> dict:
    """The edge events t_a <= vt_s < t_b. Under a `mode: event` window every variable, the endpoint nodes too, binds
    only versions that start inside the window (a node valid since before it does not bind: W5's nodes start at 0),
    so the window is `overlap` and the event condition is on the edge alone."""
    p = {"nodes": [{"var": "u"}, {"var": "v"}], "steps": [{"var": "e", "from": "u", "to": "v", "dir": "out"}]}
    if t_a is not None:
        p["where"] = [_cmp(">=", _c("e.vt_s"), _i(t_a)), _cmp("<", _c("e.vt_s"), _i(t_b))]
    return p


def series_text(args: dict) -> dict:
    """graph_metric_timeseries, metric edge_event_count: the believed edge versions with t_a <= vt_s < t_b
    (an `overlap` window and the event condition on the edge, _events_pattern), counted per bucket. count_distinct of the version id: under a window the endpoint variables
    bind node versions, and a node with two versions overlapping the window would repeat an edge's binding."""
    w, st = args["window"], int(args["stride"])
    return {"pattern": _events_pattern(w["t_a"], w["t_b"]),
            "return": {"group": {"by": [{"as": "t_a", "expr": _bucket("e", w["t_a"], st)}],
                                 "aggs": [{"as": "value", "fn": "count_distinct", "of": _c("e.vid")}]},
                       "order_by": [{"expr": _c("t_a"), "dir": "asc"}]},
            "scope": {"vt": {"window": {"t_a": int(w["t_a"]), "t_b": int(w["t_b"])}, "mode": "overlap"},
                      "as_of_tt": int(args.get("as_of_tt", OPEN_END))}}


def _page(rows: list, limit: int, whole: bool) -> dict:
    page = rows if whole else rows[:limit]
    truncated = len(page) < len(rows)
    return {"rows": page, "rows_total": len(rows), "truncated": truncated,
            "cursor": str(len(page)) if truncated else None}


def series(adapter, args: dict, whole: bool = False, call: Callable = call_operator) -> dict:
    """One call; client step: a bucket with no event (no group row) is 0, as the operator reports it."""
    w, st = args["window"], int(args["stride"])
    got = {int(r["t_a"]): int(r["value"]) for r in run_match(adapter, series_text(args), None, call)[0]}
    rows = [{"t_a": t, "t_b": min(t + st, int(w["t_b"])), "value": got.get(t, 0)}
            for t in range(int(w["t_a"]), int(w["t_b"]), st)]
    return {**_page(rows, int(args.get("limit") or DEFAULT_LIMIT), whole), "n_buckets": len(rows)}


def agg_text(args: dict) -> dict:
    """aggregate_events grouped by rel_type and time bucket, count and count_distinct(dst), over the edge events
    (_events_pattern); count is of distinct versions for the reason series_text gives."""
    w, st = args["window"], int(args["stride"])
    return {"pattern": _events_pattern(w["t_a"], w["t_b"]),
            "return": {"group": {"by": [{"as": "rel_type", "expr": _c("e.rel_type")},
                                        {"as": "t_a", "expr": _bucket("e", w["t_a"], st)}],
                                 "aggs": [{"as": "count", "fn": "count_distinct", "of": _c("e.vid")},
                                          {"as": "distinct_dst", "fn": "count_distinct", "of": _c("e.dst")}]},
                       "order_by": [{"expr": _c("rel_type"), "dir": "asc"}, {"expr": _c("t_a"), "dir": "asc"}]},
            "scope": {"vt": {"window": {"t_a": int(w["t_a"]), "t_b": int(w["t_b"])}, "mode": "overlap"},
                      "as_of_tt": int(args.get("as_of_tt", OPEN_END))}}


def agg(adapter, args: dict, whole: bool = False, call: Callable = call_operator) -> dict:
    """One call; non-empty groups only, as the operator. Client step: t_b = the bucket's end, cut at the window's."""
    w, st = args["window"], int(args["stride"])
    rows = [{"rel_type": r["rel_type"], "t_a": int(r["t_a"]), "t_b": min(int(r["t_a"]) + st, int(w["t_b"])),
             "count": int(r["count"]), "distinct_dst": int(r["distinct_dst"])}
            for r in run_match(adapter, agg_text(args), None, call)[0]]
    return _page(rows, int(args.get("limit") or DEFAULT_LIMIT), whole)


def _series_shape(args: dict) -> bool:
    return args.get("metric") == "edge_event_count"


def _agg_shape(args: dict) -> bool:
    return (args.get("group_by") == [{"dim": "rel_type"}, {"dim": "time_bucket"}]
            and args.get("aggregates") == [{"agg": "count"}, {"agg": "count_distinct", "of": "dst"}]
            and not any(args.get(k) for k in ("rel_types", "prop_filter", "endpoint_filter", "pair_mode")))


# ------------------------------------------------------------------ helpers for the second set of forms
def _s(v: str) -> dict:
    return {"lit": v, "tau": "str"}


def _int(expr: dict) -> dict:
    return {"cast": expr, "to": "int"}


def _win(t_a: int, t_b: int, as_of: int = OPEN_END, mode: str = "overlap", belief: str | None = None) -> dict:
    s = {"vt": {"window": {"t_a": int(t_a), "t_b": int(t_b)}, "mode": mode}, "as_of_tt": int(as_of)}
    if belief:
        s["belief"] = belief
    return s


def _distinct(cols: list[tuple[str, dict]]) -> dict:
    """one row per distinct output row: under a window or `belief: all` the node variables bind node versions, and a
    node with several versions would repeat a binding"""
    return {"by": [{"as": a, "expr": e} for a, e in cols], "aggs": []}


# ------------------------------------------------------------------ entity_history (hist.single, hist.asof)
#: version metadata the dedicated operator returns and `match` does not expose; a comparison drops them, and says so
HIST_FIELDS_NOT_IN_MATCH = ("source", "provenance_ref")


def hist_text(args: dict) -> dict:
    """The versions of one node believed at as_of_tt, both clocks: `belief: all` exposes tt_s and tt_e, and a version
    believed at as_of has its tt_e censored to OPEN_END; the operator's optional t_valid / window select the versions
    whose valid interval contains the instant or overlaps the window (the scope's window, overlap)."""
    as_of = int(args.get("as_of_tt", OPEN_END))
    if args.get("t_valid") is not None:
        lo, hi = int(args["t_valid"]), int(args["t_valid"]) + 1
    elif args.get("window") is not None:
        lo, hi = int(args["window"]["t_a"]), int(args["window"]["t_b"])
    else:
        lo, hi = 0, OPEN_END
    cols = ("vid", "uid", "label", "vt_s", "vt_e", "tt_s", "tt_e", "props")
    return {"pattern": {"nodes": [{"var": "x", "uids": [args["uid"]]}], "steps": [],
                        "where": [_cmp("=", _c("x.tt_e"), _i(OPEN_END))]},
            "return": {"rows": [{"as": k, "expr": _c(f"x.{k}")} for k in cols],
                       "order_by": [{"expr": _c("vt_s"), "dir": "asc"}, {"expr": _c("vid"), "dir": "asc"}]},
            "scope": _win(lo, hi, as_of, belief="all")}


def hist(adapter, args: dict, whole: bool = False, call: Callable = call_operator) -> dict:
    """One call. The rows lack `source` and `provenance_ref` (HIST_FIELDS_NOT_IN_MATCH)."""
    if args.get("include_edges"):
        raise ValueError("entity_history with include_edges has no match form here")
    rows = [{k: (int(v) if k in ("vt_s", "vt_e", "tt_s", "tt_e") else v) for k, v in r.items()}
            for r in run_match(adapter, hist_text(args), None, call)[0]]
    return _page(rows, int(args.get("limit") or DEFAULT_LIMIT), whole)


# ------------------------------------------------------------------ diff_belief (diff.belief, b.diff)
def diffb_text(args: dict) -> dict:
    """Every node and edge version whose belief changed in (t1, t2] inside the valid window: a union of four
    `belief: all` arms, added on tt_s, retracted on a tt_e below OPEN_END (a version still believed was never
    retracted), the BIH-R7 form of W3; k orders nodes before edges as the operator does."""
    t1, t2, w = int(args["t1"]), int(args["t2"]), args["window"]

    def arm(kind, change, tcol, edge):
        v = "e" if edge else "x"
        pat = ({"nodes": [{"var": "u"}, {"var": "v"}], "steps": [{"var": "e", "from": "u", "to": "v", "dir": "out"}]}
               if edge else {"nodes": [{"var": "x"}], "steps": []})
        pat["where"] = [_cmp(">", _c(f"{v}.{tcol}"), _i(t1)), _cmp("<=", _c(f"{v}.{tcol}"), _i(t2))]
        if tcol == "tt_e":
            pat["where"].append(_cmp("<", _c(f"{v}.tt_e"), _i(OPEN_END)))
        return {"pattern": pat, "rows": [
            {"as": "k", "expr": _i(1 if edge else 0)}, {"as": "kind", "expr": _s(kind)},
            {"as": "key", "expr": {"cast": _c("e.eid" if edge else "x.uid"), "to": "str"}},
            {"as": "vid", "expr": _c(f"{v}.vid")}, {"as": "tt", "expr": _int(_c(f"{v}.{tcol}"))},
            {"as": "change", "expr": _s(change)}, {"as": "vt_s", "expr": _int(_c(f"{v}.vt_s"))},
            {"as": "vt_e", "expr": _int(_c(f"{v}.vt_e"))}]}
    cols = ("k", "kind", "key", "vid", "tt", "change", "vt_s", "vt_e")
    return {"union": [arm("node", "added", "tt_s", False), arm("node", "retracted", "tt_e", False),
                      arm("edge", "added", "tt_s", True), arm("edge", "retracted", "tt_e", True)],
            "return": {"group": _distinct([(k, _c(k)) for k in cols]),
                       "order_by": [{"expr": _c(k), "dir": "asc"} for k in ("k", "key", "tt", "change", "vid")]},
            "scope": _win(w["t_a"], w["t_b"], belief="all")}


def diffb(adapter, args: dict, whole: bool = False, call: Callable = call_operator) -> dict:
    """One call; the payload in the engine's diff_belief keys (rows, added_total, retracted_total, truncated)."""
    rows = [{k: r[k] for k in ("kind", "key", "vid", "tt", "change", "vt_s", "vt_e")}
            for r in run_match(adapter, diffb_text(args), None, call)[0]]
    limit = int(args.get("limit") or DEFAULT_LIMIT)
    return {"rows": rows if whole else rows[:limit], "added_total": sum(r["change"] == "added" for r in rows),
            "retracted_total": sum(r["change"] == "retracted" for r in rows), "truncated": len(rows) > limit}


# ------------------------------------------------------------------ one edge's versions (b.hist, b.hist@now)
def edge_versions_text(args: dict, believed: bool) -> dict:
    """The edge chosen by its endpoints, rel_type and disc (its eid is a function of the four): every version written
    (`belief: all`) or only the believed ones (tt_e = OPEN_END); one row per distinct version."""
    cols = ["vid", "vt_s", "vt_e", "tt_s"] + ([] if believed else ["tt_e"])
    where = [_cmp("=", _c("e.disc"), _s(args.get("disc") or ""))]
    if believed:
        where.append(_cmp("=", _c("e.tt_e"), _i(OPEN_END)))
    return {"pattern": {"nodes": [{"var": "u", "uids": [args["src"]]}, {"var": "v", "uids": [args["dst"]]}],
                        "steps": [{"var": "e", "from": "u", "to": "v", "dir": "out", "rel_type": args["rel_type"]}],
                        "where": where},
            "return": {"group": _distinct([(k, _c("e.vid") if k == "vid" else _int(_c(f"e.{k}"))) for k in cols]),
                       "order_by": [{"expr": _c("vt_s" if believed else "tt_s"), "dir": "asc"},
                                    {"expr": _c("vid"), "dir": "asc"}]},
            "scope": _win(0, OPEN_END, belief="all")}


def edge_hist(adapter, args: dict, whole: bool = False, call: Callable = call_operator) -> dict:
    rows = run_match(adapter, edge_versions_text(args, False), None, call)[0]
    return {"rows": rows, "rows_total": len(rows)}


def edge_believed(adapter, args: dict, whole: bool = False, call: Callable = call_operator) -> dict:
    rows = run_match(adapter, edge_versions_text(args, True), None, call)[0]
    return {"rows": rows, "rows_total": len(rows)}


# ------------------------------------------------------------------ diff_snapshots (diff.global, b.diff@ctl)
def _diff_aggs(v: str, t1: int, t2: int, extra: bool) -> list[dict]:
    a = [{"as": "c1", "fn": "count", "where": valid_at(v, t1)}, {"as": "c2", "fn": "count", "where": valid_at(v, t2)}]
    if extra:
        a += [{"as": "v1", "fn": "min", "of": _c(f"{v}.vid"), "where": valid_at(v, t1)},
              {"as": "v2", "fn": "min", "of": _c(f"{v}.vid"), "where": valid_at(v, t2)},
              {"as": "p1", "fn": "collect", "of": _c(f"{v}.props"), "where": valid_at(v, t1)},
              {"as": "p2", "fn": "collect", "of": _c(f"{v}.props"), "where": valid_at(v, t2)}]
        if v == "x":
            a += [{"as": "l1", "fn": "min", "of": _c("x.label"), "where": valid_at("x", t1)},
                  {"as": "l2", "fn": "min", "of": _c("x.label"), "where": valid_at("x", t2)}]
    return a


def _z(c: str) -> dict:
    return {"coalesce": [_c(c), _i(0)]}


_HAVING = {"added": lambda: _and(_cmp("=", _z("c1"), _i(0)), _cmp(">", _z("c2"), _i(0))),
           "removed": lambda: _and(_cmp(">", _z("c1"), _i(0)), _cmp("=", _z("c2"), _i(0))),
           # v1 / v2 are null when no version is valid at t1 / t2: compared only where both exist (`if`), so that no
           # operand of the node call's `or` is null (match's `or` with a null operand did not keep a row whose other
           "changed": lambda: {"if": [_and(_cmp(">", _z("c1"), _i(0)), _cmp(">", _z("c2"), _i(0))),
                                      _cmp("!=", _c("v1"), _c("v2")), {"lit": False, "tau": "bool"}]}}


def diffs_text_edges(args: dict, part: str) -> dict:
    """Edge identities present in G(t2) and not G(t1) (added), the reverse (removed), or in both through different
    versions (changed, with both versions' props collected): the edge versions overlapping [t1, t2 + 1), grouped by
    identity, c1 / c2 = versions valid at t1 / t2 (at most one each under one belief time)."""
    t1, t2 = int(args["t1"]), int(args["t2"])
    by = [{"as": k, "expr": _c(f"e.{k}")} for k in ("eid", "src", "dst", "rel_type")]
    return {"pattern": {"nodes": [{"var": "u"}, {"var": "v"}], "steps": [{"var": "e", "from": "u", "to": "v", "dir": "out"}]},
            "return": {"group": {"by": by, "aggs": _diff_aggs("e", t1, t2, part == "changed"), "having": _HAVING[part]()},
                       "order_by": [{"expr": _c("eid"), "dir": "asc"}]},
            "scope": _win(min(t1, t2), max(t1, t2) + 1, args.get("as_of_tt", OPEN_END))}


def diffs_text_nodes(args: dict) -> dict:
    """Node identities added, removed or changed between G(t1) and G(t2), one call (the three parts by `having`)."""
    t1, t2 = int(args["t1"]), int(args["t2"])
    hv = {"bool": "or", "l": {"bool": "or", "l": _HAVING["added"](), "r": _HAVING["removed"]()}, "r": _HAVING["changed"]()}
    return {"pattern": {"nodes": [{"var": "x"}], "steps": []},
            "return": {"group": {"by": [{"as": "uid", "expr": _c("x.uid")}], "aggs": _diff_aggs("x", t1, t2, True),
                                 "having": hv},
                       "order_by": [{"expr": _c("uid"), "dir": "asc"}]},
            "scope": _win(min(t1, t2), max(t1, t2) + 1, args.get("as_of_tt", OPEN_END))}


def _one(js):
    v = json.loads(js) if isinstance(js, str) else js
    return v[0] if isinstance(v, list) and v else None


def diffs(adapter, args: dict, whole: bool = False, call: Callable = call_operator) -> dict:
    """diff_snapshots without `scope`: four calls (nodes; edges added; edges removed; edge candidates for a props
    change). Client steps: split the node rows by c1/c2; cut each list at `limit`; a candidate is a props change iff the
    two versions' props (or, for a node, labels) differ, the comparison of two JSON values made by the client."""
    if args.get("scope") is not None:
        raise ValueError("diff_snapshots with scope has no match form here")
    limit = int(args.get("limit") or DEFAULT_LIMIT)
    nrows, _ = run_match(adapter, diffs_text_nodes(args), None, call)
    n_add = [r["uid"] for r in nrows if not (r.get("c1") or 0) and (r.get("c2") or 0)]
    n_rem = [r["uid"] for r in nrows if (r.get("c1") or 0) and not (r.get("c2") or 0)]
    lim = None if whole else limit
    ea, ea_t = run_match(adapter, diffs_text_edges(args, "added"), lim, call)
    er, er_t = run_match(adapter, diffs_text_edges(args, "removed"), lim, call)
    ec, _ = run_match(adapter, diffs_text_edges(args, "changed"), None, call)
    changed = []
    for r in nrows:
        if (r.get("c1") or 0) and (r.get("c2") or 0) and r.get("v1") != r.get("v2"):
            pa, pb = _one(r["p1"]), _one(r["p2"])
            if pa != pb or r.get("l1") != r.get("l2"):
                changed.append({"kind": "node", "id": r["uid"], "from": {"label": r.get("l1"), "props": pa},
                                "to": {"label": r.get("l2"), "props": pb}})
    for r in ec:
        pa, pb = _one(r["p1"]), _one(r["p2"])
        if pa != pb:
            changed.append({"kind": "edge", "id": r["eid"], "from": {"props": pa}, "to": {"props": pb}})
    desc = lambda rows: [{"eid": r["eid"], "src": r["src"], "dst": r["dst"], "rel_type": r["rel_type"]} for r in rows]
    cut = (lambda x: x) if whole else (lambda x: x[:limit])
    out = {"nodes_added": cut(n_add), "nodes_added_total": len(n_add),
           "nodes_removed": cut(n_rem), "nodes_removed_total": len(n_rem),
           "edges_added": cut(desc(ea)), "edges_added_total": ea_t,
           "edges_removed": cut(desc(er)), "edges_removed_total": er_t,
           "props_changed": cut(changed), "props_changed_total": len(changed)}
    out["truncated"] = any(out[f"{k}_total"] > limit for k in
                           ("nodes_added", "nodes_removed", "edges_added", "edges_removed", "props_changed"))
    return out


# ------------------------------------------------------------------ temporal_paths (paths.k)
def paths_text(args: dict, h: int) -> dict:
    """Every node-simple time-respecting path of exactly h edges src -> dst inside the window (out-edges): explicit edge
    variables e1..eh, traversal times tau_i = max(tau_{i-1}, e_i.vt_s) from tau_0 = t_a with tau_i < e_i.vt_e and
    tau_i < t_b (the operator's definition), every node distinct and dst only at the end; one row per distinct path."""
    w = args["window"]
    t_a, t_b = int(w["t_a"]), int(w["t_b"])
    xs = ["s"] + [f"x{i}" for i in range(1, h)] + ["d"]
    nodes = [{"var": "s", "uids": [args["src"]]}] + [{"var": x} for x in xs[1:-1]] + [{"var": "d", "uids": [args["dst"]]}]
    steps = [{"var": f"e{i}", "from": xs[i - 1], "to": xs[i], "dir": "out"} for i in range(1, h + 1)]
    where, prev = [], _i(t_a)
    for i in range(1, h + 1):
        vs = _int(_c(f"e{i}.vt_s"))
        tau = {"if": [_cmp(">", vs, prev), vs, prev]}
        where += [_cmp("<", tau, _int(_c(f"e{i}.vt_e"))), _cmp("<", tau, _i(t_b))]
        prev = tau
    for a in range(len(xs)):
        for b in range(a + 1, len(xs)):
            where.append(_cmp("!=", _c(f"{xs[a]}.uid"), _c(f"{xs[b]}.uid")))
    cols = [("arrival", prev)] + [(f"e{i}_{k}", _int(_c(f"e{i}.vt_s")) if k == "t" else _c(f"e{i}.{k}"))
                                  for i in range(1, h + 1) for k in ("src", "dst", "rel_type", "eid", "t")]
    return {"pattern": {"nodes": nodes, "steps": steps, "where": where},
            "return": {"group": _distinct(cols)},
            "scope": _win(t_a, t_b, args.get("as_of_tt", OPEN_END))}


def paths(adapter, args: dict, whole: bool = False, call: Callable = call_operator) -> dict:
    """One call per path length 1..max_hops. Client steps: merge the lengths, order by (arrival, hops, the edges'
    (vt_s, eid) sequence) as the operator does, keep the first k (every path when whole)."""
    k, mh = int(args.get("k") or 5), int(args.get("max_hops") or 4)
    found = []
    for h in range(1, mh + 1):
        for r in run_match(adapter, paths_text(args, h), None, call)[0]:
            edges = [{"src": r[f"e{i}_src"], "dst": r[f"e{i}_dst"], "rel_type": r[f"e{i}_rel_type"],
                      "eid": r[f"e{i}_eid"], "t": int(r[f"e{i}_t"])} for i in range(1, h + 1)]
            found.append((int(r["arrival"]), h, tuple((e["t"], e["eid"]) for e in edges), edges))
    found.sort(key=lambda p: (p[0], p[1], p[2]))
    rows = [{"arrival": a, "hops": h, "edges": e} for a, h, _, e in (found if whole else found[:k])]
    return {"rows": rows, "rows_total": len(found), "truncated": len(found) > k, "cursor": None}


#: operator name -> its `match` form, and the argument shapes the form covers
FORMS: dict[str, Callable[..., dict]] = {"snapshot_subgraph": snap, "neighborhood_evolution": nbr,
                                         "graph_metric_timeseries": series, "aggregate_events": agg,
                                         "entity_history": hist, "diff_belief": diffb,
                                         "entity_full_history": edge_hist, "entity_believed": edge_believed,
                                         "diff_snapshots": diffs, "temporal_paths": paths}
SHAPES: dict[str, Callable[[dict], bool]] = {"graph_metric_timeseries": _series_shape, "aggregate_events": _agg_shape,
                                             "entity_history": lambda a: not a.get("include_edges"),
                                             "diff_snapshots": lambda a: a.get("scope") is None}


def fn_for(adapter, op: str, args: dict, whole: bool = False) -> Callable[[], dict] | None:
    """A zero-argument call answering `op(args)` through `match`, or None when this file has no form for it."""
    f = FORMS.get(op)
    if f is None or not SHAPES.get(op, lambda a: True)(args):
        return None
    a = dict(args)
    return lambda: f(adapter, a, whole=whole)


def texts(op: str, args: dict) -> list[dict]:
    """The `match` texts one instance issues (the edge call's uid lists shown as '$reached'), for the record."""
    if op == "snapshot_subgraph":
        return [snap_text_nodes(args), snap_text_edges(args, ["$reached"])]
    if op == "neighborhood_evolution":
        return [nbr_text_changes(args), nbr_text_series(args)]
    if op == "graph_metric_timeseries":
        return [series_text(args)]
    if op == "aggregate_events":
        return [agg_text(args)]
    if op == "entity_history":
        return [hist_text(args)]
    if op == "diff_belief":
        return [diffb_text(args)]
    if op in ("entity_full_history", "entity_believed"):
        return [edge_versions_text(args, op == "entity_believed")]
    if op == "diff_snapshots":
        return [diffs_text_nodes(args)] + [diffs_text_edges(args, p) for p in ("added", "removed", "changed")]
    if op == "temporal_paths":
        return [paths_text(args, h) for h in range(1, int(args.get("max_hops") or 4) + 1)]
    raise KeyError(op)
