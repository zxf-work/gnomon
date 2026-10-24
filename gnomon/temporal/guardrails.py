"""Cost guardrails.

Operators declare a `cost_fn(args, stats) -> {rows_scanned_est, expansions_est}`;
the router rejects with E_COST *before* execution when estimates exceed
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


def scan_estimate(args: dict[str, Any], stats: dict[str, Any]) -> dict[str, int]:
    """Default cost model: one interval-pruned scan over edge versions."""
    rows = int(stats.get("n_edge_versions", 0) * window_fraction(args, stats)) + \
        stats.get("n_node_versions", 0)
    return {"rows_scanned_est": rows, "expansions_est": 0}
