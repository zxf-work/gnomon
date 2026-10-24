"""`state_series` (engine-level): an aggregate over the state at each instant.

An aggregate over the versions alive at each bucket: valid at its start or
time-weighted over it under one belief time, or believed at each bucket of
a transaction-time window about one valid instant. Checked against the
oracle's definition on random bi-temporal stores (both backends), against
`graph_metric_timeseries`' instant metrics it generalises, and on a
TPC-BiH R3-shaped history ("when did we have most open items").
"""

from __future__ import annotations

import os
import random
import tempfile
from typing import Any

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from gnomon.core.errors import InvalidArgError
from gnomon.core.model import OPEN_END, canonical_json
from gnomon.storage.duckdb_adapter import DuckDBAdapter
from gnomon.temporal.algebra import (
    BASE_OPERATORS,
    REGISTRY,
    _canonicalize_floats,
    call_operator,
    ensure_all_registered,
    validate_args,
)
from gnomon.temporal.oracle import Oracle

from .conftest import ENVELOPE_META_KEYS

ensure_all_registered()

N_EXAMPLES = int(os.environ.get("GNOMON_HYP_EXAMPLES", "25"))
SETTINGS = settings(max_examples=N_EXAMPLES * 4, deadline=None,
                    suppress_health_check=[HealthCheck.too_slow,
                                           HealthCheck.data_too_large])
UIDS = [f"n{i}" for i in range(6)]
T_MAX = 60


def _native():
    from gnomon.storage.native import NativeAdapter

    tmp = tempfile.TemporaryDirectory(prefix="gnomon-test-state-")
    a = NativeAdapter(tmp.name)
    a._test_tmpdir = tmp
    return a


def _commit(a, ops, tt):
    a.begin()
    try:
        a.apply_ops(ops, tt)
    except Exception:
        a.rollback()
        raise
    a.commit()


def _ops(seed: int):
    rng = random.Random(seed)
    pool = [0, 1, 2, 5, -3, 0.5, 2.25, "x", True, None]
    out = []
    for i in range(40):
        u, v = rng.choice(UIDS), rng.choice(UIDS)
        s = rng.randrange(0, T_MAX - 1)
        e = OPEN_END if rng.random() < 0.3 else s + rng.randrange(1, T_MAX - s)
        bag = {k: rng.choice(pool) for k in ("q", "st") if rng.random() < 0.85}
        k = rng.choice(["n", "n", "e", "e", "cn", "ce", "rn", "re"])
        ref_e = {"kind": "edge", "src": u, "dst": v, "rel_type": "R", "disc": ""}
        out.append({
            "n": {"op": "assert_node", "uid": u, "label": rng.choice(["A", "B"]),
                  "props": bag, "vt_s": s, "vt_e": e},
            "e": {"op": "assert_edge", "src": u, "dst": v, "rel_type": rng.choice(["R", "S"]),
                  "props": bag, "vt_s": s, "vt_e": e, "disc": ""},
            "cn": {"op": "correct", "ref": {"kind": "node", "uid": u}, "props": bag,
                   "vt_s": s, "vt_e": e},
            "ce": {"op": "correct", "ref": ref_e, "props": bag, "vt_s": s, "vt_e": e},
            "rn": {"op": "retract", "ref": {"kind": "node", "uid": u},
                   "t": rng.randrange(0, T_MAX)},
            "re": {"op": "retract", "ref": ref_e, "t": rng.randrange(0, T_MAX)},
        }[k])
    return out


_stores: dict[int, Any] = {}


def stores(seed: int):
    if seed not in _stores:
        nat, duck = _native(), DuckDBAdapter(":memory:")
        for tt, op in enumerate(_ops(seed), 1):
            for a in (nat, duck):
                try:
                    _commit(a, [op], tt)
                except Exception:  # noqa: BLE001 - an op the store refuses
                    pass
        _stores[seed] = (nat, duck, Oracle(list(nat.all_node_versions()),
                                           list(nat.all_edge_versions())))
    return _stores[seed]


def _payload(a, args):
    env = call_operator(a, "state_series", dict(args))
    return canonical_json({k: v for k, v in env.items() if k not in ENVELOPE_META_KEYS})


@st.composite
def calls(draw) -> dict[str, Any]:
    kind = draw(st.sampled_from(["node", "edge"]))
    axis = draw(st.sampled_from(["valid", "valid", "transaction"]))
    fn = draw(st.sampled_from(["count", "sum", "min", "max", "mean"]))
    t_a = draw(st.integers(0, T_MAX - 1))
    span = draw(st.integers(1, 50))
    args: dict[str, Any] = {"kind": kind, "axis": axis, "agg": {"fn": fn},
                            "window": {"t_a": t_a, "t_b": t_a + span},
                            "stride": draw(st.integers(1, 15)),
                            "limit": draw(st.sampled_from([100, 4]))}
    if fn != "count":
        args["agg"]["of"] = {"prop": draw(st.sampled_from(["q", "st"]))}
    if axis == "transaction":
        args["t_valid"] = draw(st.integers(0, T_MAX))
    else:
        args["at"] = draw(st.sampled_from(["bucket_start", "time_weighted"]))
        args["as_of_tt"] = draw(st.one_of(st.integers(1, 42), st.just(OPEN_END)))
    if draw(st.booleans()):
        args["group_by"] = draw(st.sampled_from(
            [["label"] if kind == "node" else ["rel_type"], [{"prop": "st"}],
             [{"prop": "st"}, "label" if kind == "node" else "rel_type"]]))
    if draw(st.booleans()):
        args["where"] = [{"prop": "q", "cmp": draw(st.sampled_from(["gt", "le", "eq"])),
                          "value": draw(st.sampled_from([0, 1, 2.25]))}]
    if draw(st.booleans()):
        if kind == "node":
            args["labels"] = ["A"]
        else:
            args["rel_types"] = ["R"]
    return args


@SETTINGS
@given(args=calls(), seed=st.integers(0, 3))
def test_state_series_equals_the_oracle_on_both_backends(args, seed):
    nat, duck, oracle = stores(seed)
    want = canonical_json(_canonicalize_floats(
        oracle.state_series(validate_args("state_series", dict(args)))))
    assert _payload(nat, args) == want, args
    assert _payload(duck, args) == want, args


@SETTINGS
@given(seed=st.integers(0, 3), t_a=st.integers(0, 50), span=st.integers(1, 60),
       stride=st.integers(1, 20), as_of=st.one_of(st.integers(1, 42), st.just(OPEN_END)),
       kind=st.sampled_from(["node", "edge"]))
def test_count_at_bucket_start_is_the_instant_metric(seed, t_a, span, stride, as_of, kind):
    nat, duck, _ = stores(seed)
    w = {"t_a": t_a, "t_b": t_a + span}
    for a in (nat, duck):
        ss = call_operator(a, "state_series", {"kind": kind, "window": w, "stride": stride,
                                               "as_of_tt": as_of, "agg": {"fn": "count"}})
        gm = call_operator(a, "graph_metric_timeseries", {
            "metric": "node_count" if kind == "node" else "active_edge_count",
            "window": w, "stride": stride, "as_of_tt": as_of})
        assert ss["rows"] == gm["rows"] and ss["n_buckets"] == gm["n_buckets"]


def test_r3_most_open_items_per_application_time_and_per_system_time():
    nat = _native()
    # line items: open (status O) over an application-time span, then shipped
    # by a later correction that closes the open belief
    items = [("i1", 0, 30, 2), ("i2", 10, 40, 3), ("i3", 20, 25, 4), ("i4", 5, 50, 5)]
    tt = 0
    for uid, s, e, qty in items:
        tt += 1
        _commit(nat, [{"op": "assert_node", "uid": uid, "label": "Lineitem",
                       "props": {"status": "O", "qty": qty}, "vt_s": s, "vt_e": e}], tt)
    tt += 1
    _commit(nat, [{"op": "correct", "ref": {"kind": "node", "uid": "i2"},
                   "props": {"status": "F", "qty": 3}, "vt_s": 10, "vt_e": 40}], tt)
    open_items = {"kind": "node", "labels": ["Lineitem"],
                  "where": [{"prop": "status", "cmp": "eq", "value": "O"}]}
    by_app = call_operator(nat, "state_series", {
        **open_items, "window": {"t_a": 0, "t_b": 50}, "stride": 10,
        "agg": {"fn": "sum", "of": {"prop": "qty"}}})
    assert [r["value"] for r in by_app["rows"]] == [2, 2 + 5, 2 + 4 + 5, 5, 5]
    by_sys = call_operator(nat, "state_series", {
        **open_items, "axis": "transaction", "t_valid": 22,
        "window": {"t_a": 1, "t_b": 7}, "stride": 1, "agg": {"fn": "count"}})
    # believed open at valid time 22, as the belief grew and then i2 shipped
    assert [r["value"] for r in by_sys["rows"]] == [1, 2, 3, 4, 3, 3]
    weighted = call_operator(nat, "state_series", {
        **open_items, "window": {"t_a": 0, "t_b": 40}, "stride": 40,
        "at": "time_weighted", "agg": {"fn": "count"}})
    # i1 30/40, i3 5/40, i4 35/40 of the bucket
    assert weighted["rows"][0]["value"] == pytest.approx(70 / 40)


def test_arguments_are_checked_and_the_operator_is_engine_level():
    with pytest.raises(InvalidArgError):
        validate_args("state_series", {"kind": "node", "window": {"t_a": 0, "t_b": 9},
                                       "stride": 1, "axis": "transaction",
                                       "agg": {"fn": "count"}})
    with pytest.raises(InvalidArgError):
        validate_args("state_series", {"kind": "node", "window": {"t_a": 0, "t_b": 9},
                                       "stride": 1, "agg": {"fn": "sum"}})
    assert "state_series" in REGISTRY and "state_series" not in BASE_OPERATORS
