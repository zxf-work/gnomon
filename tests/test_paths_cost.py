"""The `paths` admission estimate: the work the search does, not a whole-store scan.

The old estimate was `scan_estimate` (every edge and node version at the uncalibrated
170 ms/M). At FinBench SF10's statistics -- 6.16M node and 54.9M edge versions -- that is
10.4 s, past the 10 s ceiling, so every TCR3 and the TCR5 calls over the whole span were
refused at admission, though they run in 7 to 35 ms at SF1. These tests pin the new
estimate's admission decisions on the benchmark shapes at SF10-like statistics (admitted)
and on genuinely large calls (refused), and one refusal end to end.
"""

from __future__ import annotations

import pytest

from gnomon.core.errors import CostError
from gnomon.temporal import ops_general_paths as P
from gnomon.temporal.algebra import call_operator, ensure_all_registered, validate_args
from gnomon.temporal.guardrails import (
    DEFAULT_CEILINGS,
    add_time_estimate,
    enforce_cost,
    scan_estimate,
)

from .conftest import fresh_adapter

ensure_all_registered()

NOW = 1672531200001000
START, END = 1669690368000000 - 30 * 86_400_000_000, 1669690368000000
FINBENCH_SF1_COUNTS = {"APPLY": 159166, "SIGN_IN": 471741, "WITHDRAW": 2168525, "OWN": 275609,
                       "DEPOSIT": 533915, "TRANSFER": 1993721, "REPAY": 517527, "INVEST": 260156,
                       "GUARANTEE": 71805}
SF10 = 54_900_000 / 6_452_165
#: FinBench SF10 as admission sees it: the SF1 store's statistics (Gnomon, w4/sf1-r0)
#: scaled to 6.16M node versions and 54.9M edge versions.
FINBENCH_SF10 = {
    "n_entities": 6_050_000, "n_node_versions": 6_160_000, "n_edge_versions": 54_900_000,
    "vt_min": 1577963915963000, "vt_max": 1672531198487001, "max_out_degree": 15_500,
    "rel_type_counts": {k: int(v * SF10) for k, v in FINBENCH_SF1_COUNTS.items()},
}
#: The statistics of an LDBC SNB SF1 store, scaled 9x.
SNB_SF10 = {
    "n_entities": 33_546_834, "n_node_versions": 34_360_452, "n_edge_versions": 194_683_374,
    "vt_min": 0, "vt_max": 1356998399940001, "max_out_degree": 52_839,
    "rel_type_counts": {"KNOWS": 2_116_620, "HAS_CREATOR": 32_968_494, "REPLY_OF": 22_068_702,
                        "IS_LOCATED_IN": 33_136_362},
}
#: A power-law store (1M edge versions over 10k vertices, hubs of 34k).
POWER_LAW = {
    "n_entities": 10_000, "n_node_versions": 10_003, "n_edge_versions": 1_000_269,
    "vt_min": 0, "vt_max": 1_049_999, "max_out_degree": 34_443,
    "rel_type_counts": {"R": 666_846, "S": 333_423},
}


def _estimate(args, stats):
    filled = validate_args("paths", args)
    return add_time_estimate("paths", P._paths_cost(filled, stats))


def _admitted(args, stats):
    est = _estimate(args, stats)
    enforce_cost("paths", est)
    return est


def _refused(args, stats):
    est = _estimate(args, stats)
    with pytest.raises(CostError, match="estimated cost for paths exceeds ceilings"):
        enforce_cost("paths", est)
    return est


def tcr3(edge_window):
    return {"sources": {"uids": ["Account:1"]}, "targets": {"uids": ["Account:2"]},
            "t_valid": NOW, "rel_types": ["TRANSFER"], "dir": "out",
            "edge_window": edge_window, "metric": "hops", "result": "distance"}


def tcr5(window):
    owned = {"match": {"pattern": {"nodes": [{"var": "p", "labels": ["Person"], "uids": ["$id"]},
                                             {"var": "a", "labels": ["Account"]}],
                                   "steps": [{"from": "p", "to": "a", "rel_type": "OWN",
                                              "dir": "out"}]},
                       "return": {"rows": [{"as": "uid", "expr": {"col": "a.uid"}}]},
                       "params": {"id": "Person:1", "now": NOW}}}
    return {"sources": owned, "t_valid": NOW, "rel_types": ["TRANSFER"], "dir": "out",
            "edge_window": window, "time_respecting": "strict", "window": window,
            "min_hops": 1, "max_hops": 3, "result": "all_paths", "limit": 10000}


WHOLE = {"t_a": 1577963915963001, "t_b": 1672531198487001}
DAYS30 = {"t_a": START + 1, "t_b": END}


def test_the_old_estimate_refused_finbench_sf10():
    """The defect itself: the scan estimate of a TCR3 call at SF10 is past the time ceiling."""
    old = add_time_estimate("paths", scan_estimate(validate_args("paths", tcr3(WHOLE)),
                                                   FINBENCH_SF10))
    assert old["time_est_ms"] > DEFAULT_CEILINGS["time_est_ms"]


@pytest.mark.parametrize("window", [WHOLE, DAYS30])
def test_tcr3_is_admitted_at_sf10(window):
    """One source and one target: read from both ends, a few rounds of ~20 rows a vertex."""
    est = _admitted(tcr3(window), FINBENCH_SF10)
    assert est["time_est_ms"] < 1_000
    assert est["rows_scanned_est"] < 1_000_000


@pytest.mark.parametrize("window", [WHOLE, DAYS30])
def test_tcr5_is_admitted_at_sf10(window):
    """The owned accounts, three time-increasing transfers: a few thousand traces."""
    est = _admitted(tcr5(window), FINBENCH_SF10)
    assert est["time_est_ms"] < 1_000
    assert est["expansions_est"] < P.MAX_EXPANSIONS


def test_ic13_hop_distance_is_admitted_at_snb_sf10():
    est = _admitted({"sources": {"uids": ["3171"]}, "targets": {"uids": ["35184372149443"]},
                     "t_valid": 1356998400000000, "rel_types": ["KNOWS"], "dir": "both",
                     "metric": "hops", "result": "distance"}, SNB_SF10)
    assert est["time_est_ms"] < 1_000


def test_a_bounded_enumeration_from_one_source_is_admitted():
    _admitted({"sources": {"uids": ["Account:1"]}, "t_valid": NOW, "rel_types": ["TRANSFER"],
               "result": "all_paths", "max_hops": 3}, FINBENCH_SF10)


def test_an_enumeration_from_every_account_is_refused():
    """Unanchored sources (every account) and six hops both ways: far past the
    enumeration's runtime budget, refused at admission."""
    every = {"match": {"pattern": {"nodes": [{"var": "a", "labels": ["Account"]},
                                             {"var": "b", "labels": ["Account"]}],
                                   "steps": [{"from": "a", "to": "b", "rel_type": "TRANSFER",
                                              "dir": "out"}]},
                       "return": {"rows": [{"as": "uid", "expr": {"col": "a.uid"}}]}}}
    est = _refused({"sources": every, "t_valid": NOW, "rel_types": ["TRANSFER"], "dir": "both",
                    "result": "all_paths", "max_hops": 6}, FINBENCH_SF10)
    assert est["expansions_est"] > DEFAULT_CEILINGS["expansions_est"]


def test_a_deep_enumeration_over_a_hub_graph_is_refused():
    est = _refused({"sources": {"uids": ["n0"]}, "t_valid": 500_000, "dir": "both",
                    "result": "all_paths", "max_hops": 6}, POWER_LAW)
    assert est["expansions_est"] > DEFAULT_CEILINGS["expansions_est"]


def test_a_shallow_enumeration_over_a_hub_graph_is_admitted():
    _admitted({"sources": {"uids": ["n1"]}, "t_valid": 500_000, "dir": "out",
               "result": "all_paths", "max_hops": 3}, POWER_LAW)


def test_a_window_hop_distance_over_the_whole_relation_is_refused_at_sf10():
    """The window form reads every version overlapping the window, per edge in Python
    (8 s at SF1 on FinBench for a 30-day window): at SF10 it is refused."""
    args = {"sources": {"uids": ["Account:1"]}, "targets": {"uids": ["Account:2"]},
            "valid_window": DAYS30, "rel_types": ["TRANSFER"], "dir": "both",
            "metric": "hops", "result": "distance"}
    est = _refused(args, FINBENCH_SF10)
    assert est["time_est_ms"] > DEFAULT_CEILINGS["time_est_ms"]


def test_many_sources_over_the_whole_graph_are_refused_at_sf10():
    """Sixteen sources, no hop bound: a whole-graph search per source (15.5 s at SF1)."""
    args = {"sources": {"uids": [f"Account:{i}" for i in range(16)]},
            "targets": {"uids": [f"Account:{i}" for i in range(16, 32)]},
            "t_valid": NOW, "rel_types": ["TRANSFER"], "metric": "hops", "result": "distance"}
    _refused(args, FINBENCH_SF10)


def test_the_estimate_follows_the_time_coefficient_scale(monkeypatch):
    base = _estimate(tcr3(WHOLE), FINBENCH_SF10)["time_est_ms"]
    monkeypatch.setenv("GNOMON_TIME_COEFF_SCALE", "4.0")
    assert _estimate(tcr3(WHOLE), FINBENCH_SF10)["time_est_ms"] >= 4 * base - 4


def _dense_store():
    """25 vertices, 2,000 edges valid over [0, 4,000,000): a mean degree of 80 (24
    distinct neighbours at most), so 25 sources and six hops both ways estimate
    25 * (24 + 24^2 + ... + 24^6) enumerated paths, past the unit ceiling."""
    a = fresh_adapter()
    events = [{"src": f"u{i % 25}", "dst": f"u{(i * 7 + 3) % 25}", "rel_type": "R",
               "vt_s": 0, "vt_e": 4_000_000} for i in range(2000)]
    a.begin()
    a.apply_ops([{"op": "ingest_events", "events": events, "offset": 0,
                  "node_label": "Node"}], 1)
    a.commit()
    return a


def test_a_refused_enumeration_never_runs(monkeypatch):
    a = _dense_store()

    def ran(*_a, **_k):
        raise AssertionError("the operator ran: the refusal was not at admission")

    monkeypatch.setattr(P, "_graph", ran)
    monkeypatch.setattr(P, "_enum_simple", ran)
    args = {"sources": {"uids": [f"u{i}" for i in range(25)]}, "t_valid": 3_999_999, "dir": "both",
            "result": "all_paths", "max_hops": 6}
    with pytest.raises(CostError, match="estimated cost for paths exceeds ceilings"):
        call_operator(a, "paths", args)


def test_an_admitted_call_on_the_same_store_answers():
    a = _dense_store()
    out = call_operator(a, "paths", {"sources": {"uids": ["u1"]}, "targets": {"uids": ["u2"]},
                                     "t_valid": 10, "metric": "hops", "result": "distance"})
    assert out["rows_total"] >= 0 and "rows" in out   # an answer, not a refusal
