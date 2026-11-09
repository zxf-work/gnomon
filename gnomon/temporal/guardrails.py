"""Cost guardrails.

Operators declare a `cost_fn(args, stats) -> {rows_scanned_est, expansions_est}`;
`call_operator` rejects a call with E_COST *before* execution when
estimates exceed
ceilings. The error payload carries the estimate and actionable narrowing
suggestions, so a caller can retry with a narrower request.
"""

from __future__ import annotations

from typing import Any

from gnomon.core.errors import CostError

DEFAULT_CEILINGS = {
    # Unit ceilings are memory-shaped BACKSTOPS: the admission
    # frontier measured the old defaults refusing
    # 18% of finishable calls with the optimum 256x higher, so time is the
    # primary axis now and these sit 256x up from the old values.
    "rows_scanned_est": 5_120_000_000,
    "expansions_est": 1_280_000_000,
    # The primary ceiling. Estimated from per-operator coefficients below.
    "time_est_ms": 10_000,
}

#: Where a kernel runs, the cost functions of
#: temporal_reachability, version_history, aggregate_events, co_active,
#: neighborhood_evolution, diff_belief and state_series supply their own
#: `time_est_ms` from the work that kernel does (`cost_routes`, `ROUTES_KEY`);
#: the coefficients below then price only the portable routes and the callers
#: that pass no route hint (a plan-level estimate, a caller without an
#: adapter), so
#: the plan-level policy, `guardrail-policy-v2`, is unchanged.
#:
#: ms of wall time per MILLION estimated units (rows + expansions), by
#: operator — measured, not guessed: medians from the 90-cell admission
#: frontier receipt. These are
#: HOST-CALIBRATED; a slower deployment scales them uniformly with
#: GNOMON_TIME_COEFF_SCALE. Operators absent here get the conservative
#: maximum, so an uncalibrated operator over-refuses rather than
#: over-admits. count_temporal_motifs is deliberately absent: its
#: coefficient depends on node_filter presence, so _motif_cost supplies its
#: own time_est_ms.
TIME_COEFF_MS_PER_M = {
    "graph_metric_timeseries": 55.0,
    "burst_detection": 55.0,          # same scan shape as the timeseries
    "co_active": 115.0,
    "temporal_reachability": 170.0,
    "temporal_paths": 170.0,          # frontier-bounded; reach coeff is the cap
    "entity_history": 55.0,           # postings read; scan coeff is generous
    "snapshot_subgraph": 115.0,
    "diff_snapshots": 115.0,
    # The documented off-class operator: materializes one Python
    # object per version — measured 153.9 s per 10M rows, i.e. ~15,390 ms/M,
    # 116x the columnar rate. The first draft of this table let it fall to
    # the "conservative" fallback below, which is 90x too cheap for it; the
    # pinned refusal test caught that immediately.
    "version_history": 15_400.0,
    # The PLAN_IR core's unbounded `Expand` — not an operator, which is why the
    # key is a node name. Measured on CollegeMsg (native, replay-built), same
    # store digest 7efd7f4f0ec02cb8 and the same two shapes, on two hosts:
    #
    #   host             ten anchors   hoisted    receipt
    #
    # **343.8 is the shipped value (guardrail-policy-v2).** The dearer of the
    # two *shapes* is taken, as before, so the estimate over-refuses rather than
    # over-admits on the shape it was not measured against — the IR's own
    # instruction for an uncalibrated operator, applied to a calibrated one at
    # its worst measured shape.
    #
    # Why the host, not the shape, decided this. Every other coefficient in
    # policy on that basis and set GNOMON_TIME_COEFF_SCALE as the *host* knob,
    # somewhere else — and 183.3/343.8 means it was 1.88x too cheap on the only
    # host whose numbers this project may publish. A coefficient that is too
    # cheap over-ADMITS, which is the one direction the IR says the guard must
    # not fail in, so the macOS value was not a conservative choice on Linux;
    # it was the unsafe one. Landing 343.8 makes the table host-consistent for
    # the first time.
    #
    # Its result — no row refused by the cost guard at the frozen policy, on
    # disclosure") — stands as a **v1 fact** and is not re-scored here. Nothing
    #
    # Worth noting against `_MAX_COEFF` below: the "conservative" columnar-class
    # fallback (170) is now **2.0x too cheap** for this node (it was 8% too
    # cheap against the macOS value), so an uncalibrated unbounded expansion
    # over-admits by twice as much as the old comment claimed.
    "plan_expand_unbounded": 343.8,
}
#: Fallback for uncalibrated operators: the dearest *columnar-class* rate,
#: not the global max — version_history's 15,400 would refuse any unlisted
#: 1M-row scan outright, which is over-refusal of exactly the kind the
#: frontier measured this refresh out of.
_MAX_COEFF = 170.0


def add_time_estimate(op: str, estimate: dict[str, int]) -> dict[str, int]:
    """Attach `time_est_ms` unless the cost_fn already supplied one."""
    import os

    if "time_est_ms" in estimate:
        return estimate
    units = estimate.get("rows_scanned_est", 0) + estimate.get("expansions_est", 0)
    coeff = TIME_COEFF_MS_PER_M.get(op, _MAX_COEFF)
    scale = float(os.environ.get("GNOMON_TIME_COEFF_SCALE", "1.0"))
    return {**estimate, "time_est_ms": int(units * coeff * scale / 1_000_000)}


def pure_cost(fn: Any = None, *, constants: Any = None) -> Any:
    """Mark a cost function whose estimate is a function of its two
    arguments (the filled arguments, the priced stats with their routes),
    `GNOMON_TIME_COEFF_SCALE` and the module constants `constants()` returns,
    read through nothing else (no store, no thread count, no other setting):
    the repeat-call path then keeps an admitted decision
    keyed by exactly those inputs, plus the ceilings and the time coefficient
    `add_time_estimate` and `enforce_cost` read. Unmarked functions price
    every call."""
    def mark(f: Any) -> Any:
        f._gnomon_pure_cost = constants or _NO_CONSTANTS
        return f
    return mark if fn is None else mark(fn)


def _no_constants() -> tuple[Any, ...]:
    return ()


_NO_CONSTANTS = _no_constants


def admission_constants(op: str) -> tuple[Any, ...]:
    """What `add_time_estimate` and `enforce_cost` read besides the
    estimate, the ceilings and the scale: the default ceilings and `op`'s
    time coefficient."""
    return (tuple(DEFAULT_CEILINGS.items()), TIME_COEFF_MS_PER_M.get(op, _MAX_COEFF))


@pure_cost
def point_read_estimate(args, stats):
    """Cost of reading one identity's versions through the postings.

    The pre-refresh model (`scan_estimate`) priced this as a full-store
    scan — 1,015,199 estimated rows for a 0.13 ms read at 1M, seven orders
    of magnitude of error, and the first thing any tightened ceiling would
    wrongly refuse. What the operator actually returns is one identity's
    version list, whose expected size is the store's mean.
    """
    n = stats.get("n_node_versions", 0)
    entities = max(stats.get("n_entities", 1), 1)
    return {"rows_scanned_est": max(1, -(-n // entities)), "expansions_est": 0}

#: The key under which `call_operator` hands a cost_fn the routes this
#: adapter's kernels take (`cost_routes`). A cost_fn that prices a kernel reads
#: it; a caller without an adapter (a plan-level estimate, for one)
#: leaves it out, and the cost_fn then prices the portable route, the dearer
#: one: an unknown route over-refuses, it never over-admits.
ROUTES_KEY = "_routes"


def cost_routes(adapter: Any) -> dict[str, bool]:
    """Which engine kernels this handle runs, for the cost functions.

    * `reach_kernel`: temporal_reachability's earliest-arrival search in the
      engine (`designs()["reach_kernel"]`, off under `GNOMON_REACH_KERNEL=python`);
    * `version_page`: the backend's own bounded `versions_page`, not the
      base's default, which materializes the whole population;
    * `aggregate_kernel` / `aggregate_ext`: aggregate_events' columnar fold
      and its property extension (`AGGREGATE_EXT`).

    The designs are fixed when the handle opens, so the answer is remembered
    on the adapter."""
    got = getattr(adapter, "_cost_routes", None)
    if got is None:
        from gnomon.storage.base import StorageAdapter

        designs = adapter.designs() if hasattr(adapter, "designs") else {}
        got = {
            "reach_kernel": bool(designs.get("reach_kernel", False))
            and hasattr(adapter, "reach_earliest"),
            "version_page": getattr(type(adapter), "versions_page", None)
            is not StorageAdapter.versions_page,
            "aggregate_kernel": hasattr(adapter, "aggregate_events_columnar"),
            "aggregate_ext": bool(getattr(adapter, "AGGREGATE_EXT", False)),
            # the engine routes the query-aware estimates price
            "seed_expansion": bool(designs.get("seed_expansion", True))
            and hasattr(adapter, "nodes_valid_at")
            and hasattr(adapter, "snapshot_scan_page"),
            "diff_kernel": bool(designs.get("diff_kernel", False))
            and hasattr(adapter, "diff_snapshots_kernel"),
            "resolve_kernel": hasattr(adapter, "resolve_entities_page"),
        }
        try:
            adapter._cost_routes = got
        except AttributeError:
            pass
    return got


def started_by(t: Any, stats: dict[str, Any]) -> float:
    """The share of the valid-time extent before `t`: an upper bound on the
    share of the versions that overlap a window ending at `t` (starts taken
    as spread over the extent; a long-lived version overlaps every later
    window, which `window_fraction` does not see)."""
    vt_min, vt_max = stats.get("vt_min"), stats.get("vt_max")
    if vt_min is None or vt_max is None or vt_max <= vt_min:
        return 1.0
    return max(0.0, min(1.0, (int(t) - vt_min) / (vt_max - vt_min)))


def rel_rows(stats: dict[str, Any], rels: Any) -> int:
    """The edge versions of `rels` by the store's counts (all for None)."""
    total = int(stats.get("n_edge_versions") or 0)
    counts = stats.get("rel_type_counts") or {}
    if rels is None or not counts:
        return total
    if isinstance(rels, str):
        rels = [rels]
    return sum(int(counts.get(r, 0)) for r in rels)


#: One read of the edge versions touching one vertex (`touching_ids`), the
#: form a point-anchored operator takes: a fixed part (the segment probes) and
#: the vertex's incident versions, at most twice the largest out-degree.
#: Measured on the five calibration stores: 0.3-11 ms.
TOUCHING_READ_MS = 5.0
TOUCHING_ROW_MS_PER_M = 1_000.0


def touching_read_ms(stats: dict[str, Any]) -> float:
    """The price of one touching read (see `TOUCHING_READ_MS`)."""
    return TOUCHING_READ_MS + 2 * int(stats.get("max_out_degree") or 0) * TOUCHING_ROW_MS_PER_M / 1e6


# --------------------------------------------------------------------------- #
# Query-aware admission for the native kernels                                #
# --------------------------------------------------------------------------- #
#
# The whole-store price of snapshot_subgraph, diff_snapshots and
# resolve_entities is a scan of every edge and node version (`scan_estimate`),
# and that of the point-anchored reads of neighborhood_evolution and co_active
# is 1 us per incident version of the largest out-degree. On large stores that
# price refuses calls without a window that run in milliseconds. Where the
# engine route runs, the estimate is the lower of the whole-store price and an
# estimate from what the call can touch:
#
# * the rows left after zone-map pruning: the manifest's per-segment `vt_min`
#   (the least `vt_s` of the segment), so a read at instant `t` touches at most
#   the rows of the segments with `vt_min <= t`, plus every row the manifest's
#   segments do not hold (`cost_zones`, `zone_rows`);
# * for a seeded expansion, the seeds, the hops and the largest out-degree
#   (each BFS level reads at most twice the largest out-degree per frontier
#   vertex, and all levels together read each version at most twice);
# * per-operator rates (ms per million rows) that are the upper envelope of a
#   calibration sweep (840 calls on the uniform and power-law graphs at 10M
#   edge versions, hub seeds, hops 0-3, five instants), not a median: every
#   timed call of the sweep is at or under its estimate.
#
# Without the zone summary (a caller with no adapter, another backend) or the
# engine route, the whole-store price is used unchanged, so this estimate never
# refuses a call the whole-store price admits. The same `GNOMON_TIME_COEFF_SCALE`
# host knob scales both.
ZONES_KEY = "_zones"

#: ms per call, and ms per million rows bounded as above (the sweep's envelope).
C5_FIXED_MS = 5.0
C5_SNAPSHOT_MS_PER_M = 80.0      # envelope 75.1 (power-law hub, hops 3, whole snapshot)
C5_DIFF_MS_PER_M = 14.0          # envelope 13.4
C5_RESOLVE_MS_PER_M = 75.0       # envelope 73.1 (a query matching every uid)
C5_TOUCH_EVOLUTION_MS_PER_M = 250.0  # envelope 246.3 per incident version (power-law hub)
C5_TOUCH_COACTIVE_MS_PER_M = 600.0   # envelope 597.6 per side row (power-law hub self-join)


def c5_constants() -> tuple[Any, ...]:
    """The module constants the query-aware estimates read (the kept admission's key)."""
    return (C5_FIXED_MS, C5_SNAPSHOT_MS_PER_M, C5_DIFF_MS_PER_M, C5_RESOLVE_MS_PER_M,
            C5_TOUCH_EVOLUTION_MS_PER_M, C5_TOUCH_COACTIVE_MS_PER_M, TOUCHING_READ_MS,
            TOUCHING_ROW_MS_PER_M, _c4_scan_coeffs())


def _c4_scan_coeffs() -> tuple[Any, ...]:
    """The whole-store coefficients the query-aware cost functions read to
    rebuild the whole-store `time_est_ms` (they supply it themselves, so `add_time_estimate` does not)."""
    return tuple((k, TIME_COEFF_MS_PER_M.get(k, _MAX_COEFF)) for k in
                 ("snapshot_subgraph", "diff_snapshots", "resolve_entities"))


def cost_zones(adapter: Any) -> tuple[Any, ...] | None:
    """The zone-map summary of the adapter's current generation, or `None`.

    `(generation, digest, edge_starts, edge_cum, node_starts, node_cum)`:
    the segments' `vt_min` in order and the cumulative rows, edge lanes and
    the node lane apart, from `segment_layout()` (the manifest; opens
    nothing). Kept on the adapter per generation."""
    gen = getattr(adapter, "generation", None)
    layout = getattr(adapter, "segment_layout", None)
    if gen is None or not callable(layout):
        return None
    try:
        g = int(gen() if callable(gen) else gen)
        got = getattr(adapter, "_cost_zones", None)
        if got is not None and got[0] == g:
            return got
        segs = layout()
    except Exception:
        return None

    def cum(lanes: Any) -> tuple[tuple[int, ...], tuple[int, ...]]:
        xs = sorted((int(d["vt_min"]), int(d["rows"])) for d in segs if lanes(d["lane"]))
        starts, total, out = [], 0, []
        for s, r in xs:
            total += r
            starts.append(s)
            out.append(total)
        return tuple(starts), tuple(out)

    es, ec = cum(lambda lane: lane != "node")
    ns, nc = cum(lambda lane: lane == "node")
    got = (g, hash((es, ec, ns, nc)), es, ec, ns, nc)
    try:
        adapter._cost_zones = got
    except AttributeError:
        pass
    return got


def zones_key(zones: Any) -> Any:
    """What of the zone summary an admission memo key needs."""
    return None if zones is None else f"{zones[0]}:{zones[1]}"


def zone_rows(stats: dict[str, Any], t: Any, kind: str = "edge") -> int | None:
    """Rows a read at valid instant `t` can touch after zone-map pruning:
    the rows of the segments whose least `vt_s` is at or before `t`, plus the
    rows no segment of the manifest holds. `None` without a zone summary."""
    import bisect

    z = stats.get(ZONES_KEY)
    if not z:
        return None
    starts, cum = (z[2], z[3]) if kind == "edge" else (z[4], z[5])
    total = int(stats.get("n_edge_versions" if kind == "edge" else "n_node_versions") or 0)
    laid = cum[-1] if cum else 0
    i = bisect.bisect_right(starts, int(t))
    return (cum[i - 1] if i else 0) + max(0, total - laid)


def expansion_rows(seeds: int, hops: int, stats: dict[str, Any], zone: int) -> int:
    """Edge versions a seeded expansion of `hops` levels (plus the level that
    closes the induced edges) can read: each level at most twice the largest
    out-degree per frontier vertex and at most the zone's rows, the frontier
    at most every entity, and all levels together each version at most twice
    (once from each endpoint)."""
    d2 = 2 * int(stats.get("max_out_degree") or 0)
    n = int(stats.get("n_entities") or 0)
    frontier, rows = max(1, seeds), 0
    for _ in range(hops + 1):
        r = min(zone, frontier * d2)
        rows += r
        frontier = min(n, frontier + r)
    return min(rows, 2 * zone)


def c4_scan_ms(op: str, args: dict[str, Any], stats: dict[str, Any]) -> tuple[dict[str, int], int]:
    """`scan_estimate` and the `time_est_ms` `add_time_estimate` gives it."""
    est = scan_estimate(args, stats)
    units = est["rows_scanned_est"] + est["expansions_est"]
    return est, int(units * TIME_COEFF_MS_PER_M.get(op, _MAX_COEFF) * coeff_scale() / 1_000_000)


def c5_ms(fixed: float, rows: int, ms_per_m: float) -> int:
    """A query-aware estimate in ms, host-scaled."""
    return int((fixed + rows * ms_per_m / 1e6) * coeff_scale()) + 1


def coeff_scale() -> float:
    """`GNOMON_TIME_COEFF_SCALE`, for cost functions that supply their own
    `time_est_ms`."""
    import os

    return float(os.environ.get("GNOMON_TIME_COEFF_SCALE", "1.0"))

SUGGESTIONS = [
    "narrow the valid-time window",
    "add a node_filter / seed set",
    "restrict rel_types",
    "reduce hops / max_hops",
]


def enforce_cost(op: str, estimate: dict[str, int],
                 ceilings: dict[str, int] | None = None) -> None:
    limits = {**DEFAULT_CEILINGS, **(ceilings or {})}
    over = {k: (estimate.get(k, 0), limits[k]) for k in limits
            if estimate.get(k, 0) > limits[k]}
    if over:
        raise CostError(
            f"estimated cost for {op} exceeds ceilings",
            estimate=estimate,
            ceilings={k: v for k, (_, v) in over.items()},
            suggestions=SUGGESTIONS,
        )


def window_fraction(args: dict[str, Any], stats: dict[str, Any]) -> float:
    """Fraction of the dataset's valid-time extent covered by args['window']."""
    w = args.get("window")
    vt_min, vt_max = stats.get("vt_min"), stats.get("vt_max")
    if not w or vt_min is None or vt_max is None or vt_max <= vt_min:
        return 1.0
    lo = max(w["t_a"], vt_min)
    hi = min(w["t_b"], vt_max)
    return max(0.0, min(1.0, (hi - lo) / (vt_max - vt_min)))


@pure_cost
def scan_estimate(args: dict[str, Any], stats: dict[str, Any]) -> dict[str, int]:
    """Default cost model: one interval-pruned scan over edge versions."""
    rows = int(stats.get("n_edge_versions", 0) * window_fraction(args, stats)) + \
        stats.get("n_node_versions", 0)
    return {"rows_scanned_est": rows, "expansions_est": 0}
