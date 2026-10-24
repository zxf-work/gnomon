"""`state_series`: an aggregate over the state at each instant (engine-level only).

A generalisation of
`graph_metric_timeseries`' instant metrics (`node_count` is `state_series`
of nodes with `count`; `active_edge_count` is its edge analogue). It
answers TPC-BiH's temporal aggregation (R3 a/b: "when did we have most
unshipped items", per system-time version or per application-time range)
and its joined variant R6 provisionally.

THE POPULATION is the node or edge versions of `kind`, restricted by
`labels` (nodes) or `rel_types` (edges) and by `where` (property
predicates, the type-fit rule: a predicate that is not well typed
excludes the version). Buckets split `window` as every series operator
does: `[t_a + i*stride, min(t_a + (i+1)*stride, t_b))`, at most 2,000.

`axis: "valid"` (the default). Under `as_of_tt`, bucket `i` aggregates the
versions valid at its start (`at: "bucket_start"`, the instant metrics'
rule) or, with `at: "time_weighted"`, every version overlapping the bucket,
weighted by the share of the bucket it covers: `count` is the mean number
alive, `sum` the mean total, `mean` the time-weighted mean, `min` and
`max` the extremes over the versions that were alive at some point.

`axis: "transaction"`. Bucket `i` reads the state BELIEVED at transaction
time `t_a + i*stride` (versions with `tt_s <= T < tt_e`) that is valid at
the instant `t_valid`: how what we believed about one moment evolved.

`agg` is `count`, or `sum`/`min`/`max`/`mean` of a numeric property
(`of: {"prop": key}`; a value that is not a JSON number does not take part).
`prop_coercion` counts, per key, the versions of the population whose value
for it was unusable, each once: the population is the versions overlapping
the window under `as_of_tt` (valid axis), or believed at some time of the
window and valid at `t_valid` (transaction axis). Sums are exact
(`props.sum_exact`), means the blessed quotient; time-weighted values are
exact rationals, reported once rounded (an integer when integral). Ties
between equal but distinguishable extremes (`1` and `1.0`) go to the first
in version-id order. With `group_by` (`label`, `rel_type` or `{"prop":
key}`, at most two) a row is emitted per non-empty (bucket, group); without
it, one row per bucket, an empty one with count 0 and other values null.
"""

from __future__ import annotations

from fractions import Fraction
from typing import Any

import numpy as np

from gnomon.core.errors import InvalidArgError
from gnomon.core.model import OPEN_END, canonical_json
from gnomon.storage.base import StorageAdapter
from gnomon.temporal.algebra import (
    AS_OF_TT,
    CURSOR,
    LIMIT,
    TIMESTAMP,
    WINDOW,
    check_window,
    operator,
    paginate,
    required,
)
from gnomon.temporal.guardrails import coeff_scale, rel_rows, started_by, window_fraction
from gnomon.temporal.ops_compute import _mean as blessed_mean
from gnomon.temporal.ops_series import _buckets
from gnomon.temporal.props import PROP_CMPS, SKIP, matches, numeric_value, parse_props, sum_exact

PROP_REF = {"type": "object", "properties": {"prop": {"type": "string", "minLength": 1}},
            "required": ["prop"], "additionalProperties": False}
GROUP = {"oneOf": [{"type": "string", "enum": ["label", "rel_type"]}, PROP_REF]}


def _validators(args: dict[str, Any]) -> None:
    check_window(args)
    kind, axis = args["kind"], args["axis"]
    if kind == "node" and args.get("rel_types") is not None:
        raise InvalidArgError("rel_types filters edges; nodes take labels")
    if kind == "edge" and args.get("labels") is not None:
        raise InvalidArgError("labels filter nodes; edges take rel_types")
    for g in args.get("group_by") or ():
        if g == "label" and kind != "node" or g == "rel_type" and kind != "edge":
            raise InvalidArgError(f"group {g!r} does not apply to {kind} versions")
    if len({canonical_json(g) for g in args.get("group_by") or ()}) != \
            len(args.get("group_by") or ()):
        raise InvalidArgError("duplicate group")
    fn = args["agg"]["fn"]
    if fn != "count" and args["agg"].get("of") is None:
        raise InvalidArgError(f"{fn} needs of: {{prop: key}}")
    if fn == "count" and args["agg"].get("of") is not None:
        raise InvalidArgError("count takes no of")
    if axis == "transaction":
        if args.get("t_valid") is None:
            raise InvalidArgError("axis transaction reads the state valid at t_valid")
        if args["at"] != "bucket_start":
            raise InvalidArgError("time_weighted is a valid-time weighting")
        if args.get("as_of_tt", OPEN_END) != OPEN_END:
            raise InvalidArgError("axis transaction: each bucket is its own belief time")
    elif args.get("t_valid") is not None:
        raise InvalidArgError("t_valid is the transaction axis' valid instant")


def _population(adapter: StorageAdapter, args: dict[str, Any]) -> list[dict[str, Any]]:
    """The versions in play, each as {vid, vt_s, vt_e, tt_s, tt_e, label|rel_type,
    props}, sorted by vid; props only when the call reads them."""
    kind, axis = args["kind"], args["axis"]
    keys = ([p["prop"] for p in args.get("where") or ()]
            + ([args["agg"]["of"]["prop"]] if args["agg"].get("of") else [])
            + [g["prop"] for g in args.get("group_by") or () if isinstance(g, dict)])
    t_a, t_b = args["window"]["t_a"], args["window"]["t_b"]
    if axis == "transaction":
        c = adapter.versions_columnar(kind)
        t = args["t_valid"]
        keep = (c["vt_s"] <= t) & (t < c["vt_e"]) & (c["tt_e"] > t_a) & (c["tt_s"] < t_b)
    elif kind == "node":
        c = adapter.nodes_columnar(as_of_tt=args["as_of_tt"], vt_min=t_a, vt_max=t_b)
        keep = np.ones(len(c["vid"]), dtype=bool)
    else:
        c = adapter.edges_columnar(as_of_tt=args["as_of_tt"], vt_min=t_a, vt_max=t_b,
                                   rel_types=args.get("rel_types"),
                                   columns=("vid", "vt_s", "vt_e", "rel_type")
                                   + (("props",) if keys else ()))
        keep = np.ones(len(c["vid"]), dtype=bool)
    tag = "label" if kind == "node" else "rel_type"
    idx = np.flatnonzero(keep)
    rows = []
    for i in idx:
        r = {"vid": c["vid"][i], "vt_s": int(c["vt_s"][i]), "vt_e": int(c["vt_e"][i]),
             tag: c[tag][i]}
        if axis == "transaction":
            r["tt_s"], r["tt_e"] = int(c["tt_s"][i]), int(c["tt_e"][i])
        if keys and kind == "edge" and "props" in c:
            r["props"] = parse_props(c["props"][i])
        rows.append(r)
    if kind == "node" and args.get("labels") is not None:
        rows = [r for r in rows if r["label"] in args["labels"]]
    if kind == "edge" and args.get("rel_types") is not None:
        rows = [r for r in rows if r["rel_type"] in args["rel_types"]]
    if keys and (kind == "node" or axis == "transaction"):
        bags = adapter.props_for_vids(kind, sorted({r["vid"] for r in rows}))
        for r in rows:
            r["props"] = bags.get(r["vid"]) or {}
    rows.sort(key=lambda r: r["vid"])
    return rows


def _stabbing_counts(adapter: StorageAdapter, args: dict[str, Any],
                     starts: np.ndarray) -> np.ndarray | None:
    """The plain count series (`count`, no `group_by`, no `where`, at bucket
    starts), from the version columns alone: per bucket start, the versions
    started by it minus those ended by it. The same population and the same
    arithmetic as the row path below, which builds one dict per version first
    (~4 us each: 8 s for FinBench SF1's two million transfers); None for
    every other call."""
    if (args["agg"]["fn"] != "count" or args.get("group_by") or args.get("where")
            or args["at"] != "bucket_start"):
        return None
    kind, axis = args["kind"], args["axis"]
    t_a, t_b = args["window"]["t_a"], args["window"]["t_b"]
    if axis == "transaction":
        c = adapter.versions_columnar(kind)
        t = args["t_valid"]
        keep = (c["vt_s"] <= t) & (t < c["vt_e"]) & (c["tt_e"] > t_a) & (c["tt_s"] < t_b)
        lo_c, hi_c = "tt_s", "tt_e"
    elif kind == "node":
        c = adapter.nodes_columnar(as_of_tt=args["as_of_tt"], vt_min=t_a, vt_max=t_b)
        keep = np.ones(len(c["vid"]), dtype=bool)
        lo_c, hi_c = "vt_s", "vt_e"
    else:
        # the read filters by rel_types itself: only the two clocks cross
        c = adapter.edges_columnar(as_of_tt=args["as_of_tt"], vt_min=t_a, vt_max=t_b,
                                   rel_types=args.get("rel_types"), columns=("vt_s", "vt_e"))
        keep = np.ones(len(c["vt_s"]), dtype=bool)
        lo_c, hi_c = "vt_s", "vt_e"
    keep = np.asarray(keep, dtype=bool)
    rels = args.get("rel_types") if kind == "edge" and axis == "transaction" else None
    for tag, allowed in (("label", args.get("labels") if kind == "node" else None),
                         ("rel_type", rels)):
        if allowed is not None:
            ok = set(allowed)
            keep &= np.fromiter((v in ok for v in c[tag]), dtype=bool, count=len(keep))
    lo = np.sort(np.asarray(c[lo_c], dtype=np.int64)[keep])
    hi = np.sort(np.asarray(c[hi_c], dtype=np.int64)[keep])
    return (np.searchsorted(lo, starts, side="right")
            - np.searchsorted(hi, starts, side="right"))


#: Per version, measured on K1M, FinBench SF1 and an SNB SF1 store: the
#: stabbing count's column read and sort (edges ~90-150 ms/M; nodes
#: ~800 ms/M, whose read carries uid, vid and label); the row path's dict per
#: version (~4-5 us, the node read included), props parsed when the call reads
#: them; its scan of the kept rows once per bucket (~0.4-0.5 us per row and
#: bucket); the transaction axis' read of every version of the kind
#: (`versions_columnar`, ~10 us per version).
SS_COLUMN_MS_PER_M = {"edge": 300.0, "node": 1_500.0}
SS_ROW_MS_PER_M = 7_000.0
SS_PROPS_MS_PER_M = 6_000.0
SS_BUCKET_MS_PER_M = 600.0
SS_TRANSACTION_MS_PER_M = 12_000.0
SS_CALL_MS = 2.0


def _state_series_cost(args: dict[str, Any], stats: dict[str, Any]) -> dict[str, int]:
    """Admission estimate from the work the call does. `scan_estimate`
    priced the window's share of the edge versions plus every node version at
    the 170 ms/M fallback: the row path below builds a dict per version and
    then scans the kept rows once per bucket, which that undercounted (a plain
    count over FinBench SF1's transfers ran 7 s against 1.2 s estimated), and
    the stabbing count now reads two columns.

    The population is the kind's versions (an edge call's `rel_types'). The
    versions overlapping a window are at most those started by its end and at
    least its share of the extent (all of the first for long-lived versions,
    the second for events): the column read is priced at the upper bound, the
    row path, whose per-bucket scan multiplies the error, at the geometric
    mean of the two. The transaction axis reads every version of the kind."""
    kind, axis = args["kind"], args["axis"]
    n = int(stats.get("n_node_versions", 0)) if kind == "node" else rel_rows(stats, args.get("rel_types"))
    t_a, t_b, stride = args["window"]["t_a"], args["window"]["t_b"], args["stride"]
    buckets = max(1, -(-(t_b - t_a) // max(1, int(stride))))
    plain = (args["agg"]["fn"] == "count" and not args.get("group_by")
             and not args.get("where") and args["at"] == "bucket_start")
    read_ms = 0.0
    if axis == "transaction":
        read_ms = n * SS_TRANSACTION_MS_PER_M / 1e6
        hi = lo = float(n)
    else:
        hi = n * started_by(t_b, stats)
        lo = n * window_fraction(args, stats)
    if plain:
        ms = read_ms + (0.0 if axis == "transaction" else hi * SS_COLUMN_MS_PER_M[kind] / 1e6)
        rows, expansions = hi, 0.0
    else:
        rows = (hi * lo) ** 0.5
        keys = bool(args.get("where") or args["agg"].get("of")
                    or any(isinstance(g, dict) for g in args.get("group_by") or ()))
        ms = read_ms + rows * (SS_ROW_MS_PER_M + (SS_PROPS_MS_PER_M if keys else 0.0)) / 1e6 \
            + rows * buckets * SS_BUCKET_MS_PER_M / 1e6
        expansions = rows * buckets
    ms = (SS_CALL_MS + ms) * coeff_scale()
    return {"rows_scanned_est": int(rows), "expansions_est": int(expansions), "time_est_ms": int(ms)}


def _num(x: Any) -> Any:
    if isinstance(x, Fraction):
        return x.numerator if x.denominator == 1 else float(x)
    return x


def _exact(v: Any) -> Any:
    return Fraction(v) if isinstance(v, float) else v


@operator(
    "state_series",
    {
        "kind": required({"type": "string", "enum": ["node", "edge"]}),
        "labels": {"type": ["array", "null"], "items": {"type": "string"}, "default": None},
        "rel_types": {"type": ["array", "null"], "items": {"type": "string"},
                      "default": None},
        "where": {"type": "array", "maxItems": 8, "items": {
            "type": "object", "properties": {"prop": {"type": "string", "minLength": 1},
                                             "cmp": {"type": "string", "enum": PROP_CMPS},
                                             "value": {}},
            "required": ["prop", "cmp", "value"], "additionalProperties": False}},
        "axis": {"type": "string", "enum": ["valid", "transaction"], "default": "valid"},
        "window": required(WINDOW),
        "stride": required({"type": "integer", "minimum": 1}),
        "at": {"type": "string", "enum": ["bucket_start", "time_weighted"],
               "default": "bucket_start"},
        "agg": required({"type": "object", "properties": {
            "fn": {"type": "string", "enum": ["count", "sum", "min", "max", "mean"]},
            "of": PROP_REF}, "required": ["fn"], "additionalProperties": False}),
        "group_by": {"type": "array", "items": GROUP, "maxItems": 2},
        "as_of_tt": AS_OF_TT,
        "t_valid": TIMESTAMP,
        "limit": LIMIT,
        "cursor": CURSOR,
    },
    "An aggregate over the node or edge versions valid at each bucket of a "
    "valid-time window (at its start, or time-weighted over it), or believed "
    "at each bucket of a transaction-time window about one valid instant. "
    "Engine-level only.",
    cost_fn=_state_series_cost,
    validators=[_validators],
    output_fields=("rows", "rows_total", "truncated", "cursor", "n_buckets",
                   "prop_coercion"),
)
def state_series(adapter: StorageAdapter, args: dict[str, Any]) -> dict[str, Any]:
    starts = _buckets(args)
    n_buckets = len(starts)
    t_b, stride = args["window"]["t_b"], args["stride"]
    counts = _stabbing_counts(adapter, args, starts)
    if counts is not None:
        out = paginate([{"t_a": int(bs), "t_b": int(min(bs + stride, t_b)), "value": int(n)}
                        for bs, n in zip(starts, counts)], args["limit"], args["cursor"])
        out["n_buckets"] = n_buckets
        return out
    pop = _population(adapter, args)
    fn, of = args["agg"]["fn"], (args["agg"].get("of") or {}).get("prop")
    skipped: dict[str, int] = {}

    def note(key: str) -> None:
        skipped[key] = skipped.get(key, 0) + 1

    kept = []
    for r in pop:
        ok = True
        for p in args.get("where") or ():
            v = matches(r["props"], p["prop"], p["cmp"], p["value"])
            if v is SKIP:
                note(p["prop"])
            if v is not True:
                ok = False
                break
        if not ok:
            continue
        if of is not None:
            r["value"] = numeric_value(r["props"], of)
            if r["value"] is SKIP:
                note(of)
        kept.append(r)

    groups = args.get("group_by") or []

    def gkey(r: dict[str, Any]) -> tuple:
        out = []
        for g in groups:
            if isinstance(g, dict):
                v = r["props"].get(g["prop"])
            else:
                v = r[g]
            out.append(v)
        return tuple(out)

    def gname(g: Any) -> str:
        return f"prop_{g['prop']}" if isinstance(g, dict) else g

    def gorder(key: tuple) -> tuple:
        return tuple((0, "") if v is None else (1, canonical_json(v)) for v in key)

    transaction, weighted = args["axis"] == "transaction", args["at"] == "time_weighted"
    rows: list[dict[str, Any]] = []
    if fn == "count" and not groups and not weighted:
        # the instant metrics' stabbing count, vectorised: alive at T is
        # started by T and not yet ended at T
        lo = np.sort(np.asarray([r["tt_s" if transaction else "vt_s"] for r in kept],
                                dtype=np.int64))
        hi = np.sort(np.asarray([r["tt_e" if transaction else "vt_e"] for r in kept],
                                dtype=np.int64))
        counts = (np.searchsorted(lo, starts, side="right")
                  - np.searchsorted(hi, starts, side="right"))
        rows = [{"t_a": int(bs), "t_b": int(min(bs + stride, t_b)), "value": int(n)}
                for bs, n in zip(starts, counts)]
        starts = starts[:0]
    for bs in starts:
        bs = int(bs)
        be = int(min(bs + stride, t_b))
        if transaction:
            alive = [(r, 1) for r in kept if r["tt_s"] <= bs < r["tt_e"]]
        elif weighted:
            alive = [(r, min(r["vt_e"], be) - max(r["vt_s"], bs)) for r in kept
                     if r["vt_s"] < be and bs < r["vt_e"]]
        else:
            alive = [(r, 1) for r in kept if r["vt_s"] <= bs < r["vt_e"]]
        by: dict[tuple, list] = {}
        for r, w in alive:
            by.setdefault(gkey(r), []).append((r, w))
        if not groups and not by:
            by[()] = []
        for key in sorted(by, key=gorder):
            members = by[key]
            vals = [] if of is None else \
                [(r["value"], w) for r, w in members if r["value"] is not SKIP]
            if fn == "count":
                value: Any = Fraction(sum(w for _, w in members), be - bs) if weighted \
                    else len(members)
            elif not vals:
                value = None
            elif fn in ("min", "max"):
                value = (min if fn == "min" else max)(v for v, _ in vals)
            elif not weighted:
                plain = [v for v, _ in vals]
                value = sum_exact(plain) if fn == "sum" else blessed_mean(plain)
            elif fn == "sum":
                value = sum((_exact(v) * w for v, w in vals), Fraction(0)) / (be - bs)
            else:
                value = sum((_exact(v) * w for v, w in vals), Fraction(0)) / \
                    sum(w for _, w in vals)
            row = {"t_a": bs, "t_b": be}
            for g, v in zip(groups, key):
                row[gname(g)] = v
            row["value"] = _num(value)
            rows.append(row)
    out = paginate(rows, args["limit"], args["cursor"])
    out["n_buckets"] = n_buckets
    if of is not None or args.get("where"):
        out["prop_coercion"] = dict(sorted(skipped.items()))
    return out
