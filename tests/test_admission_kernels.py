"""Admission prices the kernel that runs.

With admission on in every timed run, an estimate that prices a route the call
does not take refuses benchmark queries that finish in milliseconds. These
estimates priced the portable routes:

- `temporal_reachability` over a whole span: the NumPy fixpoint's nine units per
  edge version at 170 ms/M, 20 to 60 times the engine's search (SNB SF1: 0.7 s run,
  33 s estimated, refused);
- `version_history` without `ref`: the base rate of 15,400 ms/M for one Python object per
  version, 800 to 49,000 times the engine's bounded page;
- `aggregate_events`, `co_active`, `neighborhood_evolution`: a scan of the store
  for a columnar fold or a read of one vertex's edges;
- `state_series`: too LOW, the row path's dict per version and per-bucket scan
  priced as a columnar scan (FinBench SF1: 7 s run, 1.2 s estimated).

`call_operator` now hands the cost functions the routes the adapter's kernels
take (`ROUTES_KEY`); a caller that does not (a plan-level estimate, a caller
without an adapter) gets the portable price. These tests pin the decisions at measured and
scaled store statistics: the benchmark shapes admitted, genuinely huge calls
refused, the portable routes priced as before.
"""

from __future__ import annotations

import pytest

from gnomon.core.errors import CostError
from gnomon.core.model import OPEN_END
from gnomon.temporal import ops_state_series as SS
from gnomon.temporal.algebra import call_operator, ensure_all_registered, validate_args, REGISTRY
from gnomon.temporal.guardrails import (
    DEFAULT_CEILINGS,
    ROUTES_KEY,
    add_time_estimate,
    cost_routes,
    enforce_cost,
)

from .conftest import fresh_adapter

ensure_all_registered()

NATIVE = {"reach_kernel": True, "version_page": True, "aggregate_kernel": True, "aggregate_ext": True}

#: The statistics of an LDBC SNB SF1 store (a replay of the SF1 update stream).
SNB_SF1 = {
    "n_entities": 3_727_426, "n_node_versions": 3_817_828, "n_edge_versions": 21_631_486,
    "max_out_degree": 5_871, "vt_min": 0, "vt_max": 1_356_998_399_940_001,
    "rel_type_counts": {"KNOWS": 235_180, "HAS_CREATOR": 3_663_166, "REPLY_OF": 2_452_078,
                        "IS_LOCATED_IN": 3_681_818, "HAS_TAG": 4_108_292, "LIKES": 2_588_997,
                        "HAS_MEMBER": 3_284_216, "CONTAINER_OF": 1_211_088},
}
#: FinBench SF1 (Gnomon w4/sf1-r0).
FINBENCH_SF1 = {
    "n_entities": 643_241, "n_node_versions": 654_775, "n_edge_versions": 6_452_165,
    "max_out_degree": 1_723, "vt_min": 1_577_963_915_963_000, "vt_max": 1_672_531_198_487_001,
    "rel_type_counts": {"DEPOSIT": 533_915, "OWN": 275_609, "TRANSFER": 1_993_721,
                        "INVEST": 260_156, "REPAY": 517_527, "APPLY": 159_166,
                        "SIGN_IN": 471_741, "WITHDRAW": 2_168_525, "GUARANTEE": 71_805},
}
#: The synthetic store at 10M events (K10M).
K10M = {
    "n_entities": 100_000, "n_node_versions": 100_003, "n_edge_versions": 10_000_269,
    "max_out_degree": 147, "vt_min": 0, "vt_max": 10_499_999,
    "rel_type_counts": {"R": 6_666_846, "S": 3_333_423},
}


def scaled(stats: dict, k: float) -> dict:
    out = dict(stats)
    for key in ("n_entities", "n_node_versions", "n_edge_versions", "max_out_degree"):
        out[key] = int(stats[key] * k)
    out["rel_type_counts"] = {r: int(v * k) for r, v in stats["rel_type_counts"].items()}
    return out


def estimate(op: str, args: dict, stats: dict, routes: dict | None = NATIVE) -> dict:
    filled = validate_args(op, dict(args))
    st = stats if routes is None else {**stats, ROUTES_KEY: routes}
    return add_time_estimate(op, REGISTRY[op].cost_fn(filled, st))


def admitted(op, args, stats, routes=NATIVE):
    est = estimate(op, args, stats, routes)
    enforce_cost(op, est)
    return est


def refused(op, args, stats, routes=NATIVE):
    est = estimate(op, args, stats, routes)
    with pytest.raises(CostError, match=f"estimated cost for {op} exceeds ceilings"):
        enforce_cost(op, est)
    return est


def span(stats, lo=0.0, hi=1.0):
    a, b = stats["vt_min"], stats["vt_max"]
    return {"t_a": int(a + lo * (b - a)), "t_b": int(a + hi * (b - a))}


# --------------------------------------------------------------------------- #
# temporal_reachability                                                        #
# --------------------------------------------------------------------------- #

def test_whole_span_reachability_is_admitted_on_the_engine_search():
    """SNB SF1: 0.7 s measured (1.3 s out, 2.6 s both estimated) where the old
    price was 33 s and refused; K10M: 0.3 s measured, 15.3 s before."""
    for stats in (SNB_SF1, K10M, FINBENCH_SF1):
        for d in ("out", "in", "both"):
            est = admitted("temporal_reachability",
                           {"src": "n1", "window": span(stats), "direction": d}, stats)
            assert est["time_est_ms"] < 3_000
    # K10M at nine times its statistics still answers
    admitted("temporal_reachability", {"src": "n1", "window": span(K10M)}, scaled(K10M, 9))


def test_a_window_late_in_the_extent_is_priced_by_the_versions_started_by_its_end():
    """A long-lived version overlaps every later window: a last-tenth window on
    the SNB store reads (and measured) as much as the whole span."""
    late = estimate("temporal_reachability", {"src": "n1", "window": span(SNB_SF1, 0.9, 1.0)}, SNB_SF1)
    whole = estimate("temporal_reachability", {"src": "n1", "window": span(SNB_SF1)}, SNB_SF1)
    early = estimate("temporal_reachability", {"src": "n1", "window": span(SNB_SF1, 0.0, 0.1)}, SNB_SF1)
    assert late["time_est_ms"] == whole["time_est_ms"]
    assert early["time_est_ms"] < whole["time_est_ms"] / 5


def test_the_portable_routes_keep_the_fixpoint_price():
    """No route hint (a plan-level estimate, a caller without an adapter), the NumPy
    fixpoint (`GNOMON_REACH_KERNEL=python`, another backend) and the multi-label
    search under `delta_max_wait` are priced as before, and refused as before."""
    args = {"src": "n1", "window": span(SNB_SF1)}
    old = {"rows_scanned_est": 21_631_486, "expansions_est": 8 * 21_631_486}
    for routes in (None, {**NATIVE, "reach_kernel": False}):
        est = refused("temporal_reachability", args, SNB_SF1, routes)
        assert {k: est[k] for k in old} == old
    est = refused("temporal_reachability", {**args, "delta_max_wait": 60}, SNB_SF1)
    assert est["expansions_est"] == old["expansions_est"]


def test_a_genuinely_huge_search_is_still_refused():
    refused("temporal_reachability", {"src": "n1", "window": span(SNB_SF1), "direction": "both"},
            scaled(SNB_SF1, 10))
    refused("temporal_reachability", {"src": "n1", "window": span(K10M)}, scaled(K10M, 100))


# --------------------------------------------------------------------------- #
# version_history                                                              #
# --------------------------------------------------------------------------- #

def test_the_whole_log_is_admitted_on_the_bounded_page():
    """K10M edges: 1.0 s measured (1.9 s estimated), 311 s before; FinBench SF1
    every edge version: 1.0 s measured (1.8 s estimated)."""
    for stats, rel in ((K10M, "R"), (FINBENCH_SF1, "TRANSFER"), (SNB_SF1, "KNOWS")):
        for kind in ("node", "edge"):
            a = {"kind": kind, "window": span(stats), "belief": "all", "limit": 10_000}
            if kind == "edge":
                a["rel_types"] = [rel]
            est = admitted("version_history", a, stats)
            assert est["time_est_ms"] < 2_500
    admitted("version_history", {"kind": "edge", "window": span(FINBENCH_SF1)}, FINBENCH_SF1)


def test_the_page_is_priced_by_the_kind_and_relation_it_sweeps():
    w = span(FINBENCH_SF1)
    one = estimate("version_history", {"kind": "edge", "window": w, "rel_types": ["GUARANTEE"]},
                   FINBENCH_SF1)
    every = estimate("version_history", {"kind": "edge", "window": w}, FINBENCH_SF1)
    assert one["rows_scanned_est"] == 71_805 and every["rows_scanned_est"] == 6_452_165
    assert one["time_est_ms"] < every["time_est_ms"] / 5
    # the window does not make it cheaper: it filters during the sweep
    narrow = estimate("version_history", {"kind": "edge", "window": span(FINBENCH_SF1, 0.99, 1.0)},
                      FINBENCH_SF1)
    assert narrow == every


def test_without_the_bounded_page_the_d058_price_stands():
    """The base adapter's default materializes one object per version."""
    a = {"kind": "edge", "window": span(K10M)}
    est = refused("version_history", a, K10M, routes=None)
    assert est["expansions_est"] == K10M["n_edge_versions"] + K10M["n_node_versions"]
    refused("version_history", a, K10M, routes={**NATIVE, "version_page": False})


def test_a_store_too_large_to_sort_is_refused_on_the_page_too():
    refused("version_history", {"kind": "edge", "window": span(K10M)}, scaled(K10M, 10))
    refused("version_history", {"kind": "edge", "window": span(SNB_SF1)}, scaled(SNB_SF1, 10))


# --------------------------------------------------------------------------- #
# state_series                                                                 #
# --------------------------------------------------------------------------- #

def _ss(stats, rel, w, buckets, **extra):
    return {"kind": "edge", "rel_types": [rel], "window": w,
            "stride": max(1, (w["t_b"] - w["t_a"]) // buckets), "agg": {"fn": "count"}, **extra}


def test_the_plain_count_reads_columns():
    """FinBench SF1, the transfers over the whole span: 0.19 s now (7.0 s on
    the row path, 1.2 s estimated before); the TPC-BiH R3 shape at ten times SF1."""
    est = admitted("state_series", _ss(FINBENCH_SF1, "TRANSFER", span(FINBENCH_SF1), 30), FINBENCH_SF1)
    assert est["time_est_ms"] < 1_000
    admitted("state_series", _ss(SNB_SF1, "KNOWS", span(SNB_SF1, 0.99, 1.0), 30), scaled(SNB_SF1, 10))
    admitted("state_series", _ss(FINBENCH_SF1, "TRANSFER", span(FINBENCH_SF1), 1_900),
             scaled(FINBENCH_SF1, 9))


def test_the_row_path_is_priced_per_row_and_bucket():
    """The row path (a group, a time weighting, a where or a property) builds a
    dict per version and scans the kept rows once per bucket: K1M at 300 buckets
    over its last tenth ran 10.8 s, and FinBench SF1's transfers over the whole
    span 7 s even at 30."""
    w = span(FINBENCH_SF1)
    plain = estimate("state_series", _ss(FINBENCH_SF1, "TRANSFER", w, 30), FINBENCH_SF1)
    grouped = estimate("state_series", _ss(FINBENCH_SF1, "TRANSFER", w, 30, group_by=["rel_type"]),
                       FINBENCH_SF1)
    assert grouped["time_est_ms"] > 7_000 > plain["time_est_ms"]
    many = estimate("state_series", _ss(FINBENCH_SF1, "TRANSFER", w, 300, at="time_weighted"),
                    FINBENCH_SF1)
    assert many["time_est_ms"] > 5 * grouped["time_est_ms"]
    refused("state_series", _ss(K10M, "R", span(K10M), 1_900, group_by=["rel_type"]), K10M)


def test_the_transaction_axis_reads_every_version_of_the_kind():
    a = {"kind": "edge", "rel_types": ["R"], "axis": "transaction", "t_valid": 5,
         "window": {"t_a": 0, "t_b": 1 << 40}, "stride": 1 << 35, "agg": {"fn": "count"}}
    est = refused("state_series", a, K10M)
    assert est["rows_scanned_est"] == K10M["rel_type_counts"]["R"]


@pytest.mark.parametrize("seed", range(6))
def test_the_stabbing_count_answers_as_the_row_path(monkeypatch, seed):
    """The vectorised count is the row path's arithmetic over the same
    population: the corrected and retracted random stores of the state_series
    suite, both backends, every shape the count takes, both routes compared."""
    from .test_state_series import T_MAX, stores

    nat, duck, _ = stores(seed)
    tt = len(_ops_of(seed)) // 2
    shapes = [
        {"kind": "edge", "window": {"t_a": 0, "t_b": T_MAX}, "stride": 3, "agg": {"fn": "count"}},
        {"kind": "edge", "rel_types": ["R"], "window": {"t_a": 5, "t_b": 40}, "stride": 4,
         "agg": {"fn": "count"}, "as_of_tt": tt},
        {"kind": "node", "window": {"t_a": 0, "t_b": T_MAX}, "stride": 5, "agg": {"fn": "count"}},
        {"kind": "node", "labels": ["A"], "window": {"t_a": 0, "t_b": T_MAX}, "stride": 2,
         "agg": {"fn": "count"}, "as_of_tt": tt},
        {"kind": "edge", "rel_types": ["S"], "axis": "transaction", "t_valid": 20,
         "window": {"t_a": 0, "t_b": 45}, "stride": 5, "agg": {"fn": "count"}},
        {"kind": "node", "labels": ["B"], "axis": "transaction", "t_valid": 30,
         "window": {"t_a": 1, "t_b": 45}, "stride": 7, "agg": {"fn": "count"}},
    ]
    for a in (nat, duck):
        for args in shapes:
            fast = call_operator(a, "state_series", dict(args))
            with monkeypatch.context() as m:
                m.setattr(SS, "_stabbing_counts", lambda *x: None)
                rows = call_operator(a, "state_series", dict(args))
            assert fast == rows, args


def _ops_of(seed):
    from .test_state_series import _ops

    return _ops(seed)


# --------------------------------------------------------------------------- #
# aggregate_events, co_active, neighborhood_evolution, diff_belief             #
# --------------------------------------------------------------------------- #

def test_the_columnar_fold_is_priced_as_one():
    """K10M whole span: 15-18 ms measured, 1.7 s estimated before (15.5 s at
    nine times, refused); now about 0.2 s. A sequence aggregate, a calendar unit
    or a pair mode takes the portable path and keeps the scan price."""
    a = {"group_by": [{"dim": "rel_type"}, {"dim": "time_bucket"}], "aggregates": [{"agg": "count"}],
         "window": span(K10M), "stride": 105_000}
    cal = {**a, "group_by": [{"dim": "calendar_unit", "unit": "hour_of_day"}], "stride": None}
    est = admitted("aggregate_events", a, scaled(K10M, 9))
    assert est["time_est_ms"] < 2_500
    portable = estimate("aggregate_events", a, K10M, routes=None)
    assert portable["time_est_ms"] == 1_717
    assert estimate("aggregate_events", cal, K10M)["time_est_ms"] == 1_717


def test_point_anchored_reads_are_priced_as_touching_reads():
    w2pl = {"n_entities": 10_000, "n_node_versions": 10_003, "n_edge_versions": 1_000_269,
            "max_out_degree": 34_443, "vt_min": 0, "vt_max": 1_049_999,
            "rel_type_counts": {"R": 666_846, "S": 333_423}}
    for stats in (FINBENCH_SF1, scaled(FINBENCH_SF1, 9), SNB_SF1, scaled(SNB_SF1, 10), w2pl):
        e = admitted("neighborhood_evolution", {"uid": "n1", "t1": stats["vt_min"], "t2": stats["vt_max"]},
                     stats)
        assert e["time_est_ms"] < 1_000
        e = admitted("co_active", {"a_spec": {"src": "n1"}, "b_spec": {"src": "n2"},
                                   "allen_relation": {"relation": "overlaps"}}, stats)
        assert e["time_est_ms"] < 1_000
    # a whole-relation side reads the relation: K10M ran 3.2 s, estimated 1.2 s before
    whole = estimate("co_active", {"a_spec": {"rel_type": "R"}, "b_spec": {"src": "n2"},
                                   "allen_relation": {"relation": "overlaps"}}, K10M)
    assert whole["time_est_ms"] > 3_200
    refused("co_active", {"a_spec": {"rel_type": None}, "b_spec": {"rel_type": None},
                          "allen_relation": {"relation": "overlaps"}}, scaled(K10M, 2))


def test_a_belief_diff_over_an_empty_interval_reads_nothing():
    est = admitted("diff_belief", {"t1": 7, "t2": 7, "window": span(SNB_SF1)}, scaled(SNB_SF1, 100))
    assert est["time_est_ms"] <= 1
    late = estimate("diff_belief", {"t1": 0, "t2": OPEN_END, "window": span(SNB_SF1, 0.9, 1.0)}, SNB_SF1)
    whole = estimate("diff_belief", {"t1": 0, "t2": OPEN_END, "window": span(SNB_SF1)}, SNB_SF1)
    assert late == whole       # SNB SF1: 3.4 s for the last tenth, 3.6 s for all of it


# --------------------------------------------------------------------------- #
# end to end                                                                   #
# --------------------------------------------------------------------------- #

def test_the_adapter_reports_its_kernels():
    import os

    a = fresh_adapter()
    try:
        r = cost_routes(a)
        native = os.environ.get("GNOMON_TEST_BACKEND", "native") == "native"
        assert r["version_page"] is native
        assert r["aggregate_kernel"] is native
        assert r["reach_kernel"] is (native and os.environ.get("GNOMON_REACH_KERNEL") != "python")
        assert cost_routes(a) is r          # read once per handle
    finally:
        a.close()


def test_call_operator_prices_with_the_routes_and_still_refuses():
    a = fresh_adapter()
    a.begin()
    a.apply_ops([{"op": "assert_edge", "src": "a", "dst": "b", "rel_type": "R", "props": {},
                  "vt_s": 1, "vt_e": OPEN_END}], 5)
    a.commit()
    try:
        out = call_operator(a, "temporal_reachability", {"src": "a", "window": {"t_a": 0, "t_b": 10}})
        assert [r["uid"] for r in out["rows"]] == ["b"]
        with pytest.raises(CostError):
            call_operator(a, "temporal_reachability", {"src": "a", "window": {"t_a": 0, "t_b": 10}},
                          cost_ceilings={"time_est_ms": 0, "rows_scanned_est": 0})
    finally:
        a.close()


def test_the_ceilings_are_unchanged():
    assert DEFAULT_CEILINGS == {"rows_scanned_est": 5_120_000_000, "expansions_est": 1_280_000_000,
                                "time_est_ms": 10_000}
