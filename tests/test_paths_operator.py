"""`paths` (engine-level): general shortest, optimal and enumerated paths.

Hop and weight distances, one and all optimal paths, k best and all simple
paths (optionally time-respecting), sources, targets and weights from uid
lists, properties or inline `match` patterns, and `metric: "arrival"`
answered by the two temporal operators' kernels.

Every answer is checked against `Oracle.paths`, which enumerates every
node-simple path of a small store and reads each result off that list,
sharing nothing with the operator's searches; on the native engine and on
DuckDB; on random bi-temporal stores and on the benchmark shapes the
operator exists for (IC13, IC14, BI19, TCR3, TCR5).
"""

from __future__ import annotations

import os
import random
import tempfile
from typing import Any

import pytest
from hypothesis import HealthCheck, example, given, settings
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
UIDS = [f"p{i}" for i in range(7)]
T_MAX = 40


def _native():
    from gnomon.storage.native import NativeAdapter

    tmp = tempfile.TemporaryDirectory(prefix="gnomon-test-paths-")
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


def _build(ops_by_tt):
    nat, duck = _native(), DuckDBAdapter(":memory:")
    for tt, ops in enumerate(ops_by_tt, 1):
        for a in (nat, duck):
            try:
                _commit(a, ops, tt)
            except Exception:  # noqa: BLE001 - an op the store refuses
                pass
    return nat, duck, Oracle(list(nat.all_node_versions()), list(nat.all_edge_versions()))


def _random_ops(seed: int):
    rng = random.Random(seed)
    batches = [[{"op": "assert_node", "uid": u, "label": "Person", "props": {},
                 "vt_s": 0, "vt_e": OPEN_END} for u in UIDS]]
    for i in range(30):
        u, v = rng.sample(UIDS, 2)
        s = rng.randrange(0, T_MAX - 2)
        e = OPEN_END if rng.random() < 0.6 else s + rng.randrange(1, T_MAX - s)
        w = rng.choice([1, 2, 3, 0.5, 1.25, 2.5, 7])
        bag = {"w": w} if rng.random() < 0.85 else {"w": "x"}
        batches.append([{"op": "assert_edge", "src": u, "dst": v,
                         "rel_type": rng.choice(["KNOWS", "KNOWS", "OTHER"]),
                         "props": bag, "vt_s": s, "vt_e": e, "disc": str(i % 2)}])
        if rng.random() < 0.15:
            batches.append([{"op": "retract", "ref": {"kind": "edge", "src": u, "dst": v,
                                                      "rel_type": "KNOWS", "disc": str(i % 2)},
                             "t": rng.randrange(0, T_MAX)}])
    return batches


_stores: dict[int, Any] = {}


def stores(seed: int):
    if seed not in _stores:
        _stores[seed] = _build(_random_ops(seed))
    return _stores[seed]


#: Weight distances compare as numbers.
#: When an integer and a fractional optimal sum tie (3 + 3 and 2.5 + 3.5), the
#: printed type (6 or 6.0) is unspecified; the values must be equal. Every
#: other field keeps its exact comparison.
WEIGHT_FIELDS = ("distance", "cost")


def _as_numbers(payload: dict[str, Any], args: dict[str, Any]) -> dict[str, Any]:
    if args.get("metric") != "weight":
        return payload
    def num(k: str, v: Any) -> Any:
        if k in WEIGHT_FIELDS and isinstance(v, (int, float)) and not isinstance(v, bool):
            return float(v)
        return v
    return {**payload, "rows": [{k: num(k, v) for k, v in r.items()}
                                for r in payload.get("rows", [])]}


def _agree(nat, duck, oracle, args) -> None:
    filled = validate_args("paths", dict(args))
    want = canonical_json(_as_numbers(_canonicalize_floats(oracle.paths(filled)), filled))
    for a in (nat, duck):
        env = call_operator(a, "paths", dict(args))
        got = canonical_json(_as_numbers(
            {k: v for k, v in env.items() if k not in ENVELOPE_META_KEYS}, filled))
        assert got == want, (args, got[:400], want[:400])


@pytest.mark.parametrize("seed", [0, 1])
def test_one_source_one_target_reads_from_both_ends(seed):
    """A hop distance from one source to one target reads frontier-first
    from both ends and stops when they meet or a side is exhausted. Every
    ordered pair, reachable or not, at two instants and two belief times, in
    every direction and optimal result, equals the oracle."""
    nat, duck, oracle = stores(seed)
    for s_ in UIDS:
        for t_ in UIDS:
            if s_ == t_:
                continue
            for t_valid in (10, 30):
                for as_of in (OPEN_END, 20):
                    for d in ("out", "in", "both"):
                        for res in ("distance", "one_path", "all_optimal"):
                            _agree(nat, duck, oracle, {
                                "sources": {"uids": [s_]}, "targets": {"uids": [t_]},
                                "t_valid": t_valid, "as_of_tt": as_of, "dir": d,
                                "metric": "hops", "result": res})


@st.composite
def snapshot_calls(draw) -> dict[str, Any]:
    res = draw(st.sampled_from(["distance", "one_path", "all_optimal", "all_paths",
                                "k_paths"]))
    metric = draw(st.sampled_from(["hops", "weight"]))
    args: dict[str, Any] = {
        "sources": {"uids": draw(st.lists(st.sampled_from(UIDS), min_size=1, max_size=2,
                                          unique=True))},
        "t_valid": draw(st.integers(0, T_MAX)), "metric": metric, "result": res,
        "dir": draw(st.sampled_from(["out", "in", "both"])),
        "as_of_tt": draw(st.one_of(st.integers(1, 45), st.just(OPEN_END))),
        "limit": draw(st.sampled_from([100, 3])),
    }
    if draw(st.booleans()):
        args["targets"] = {"uids": draw(st.lists(st.sampled_from(UIDS), min_size=1,
                                                 max_size=3, unique=True))}
    if draw(st.booleans()):
        args["rel_types"] = ["KNOWS"]
    if metric == "weight":
        args["weight"] = {"prop": "w"}
        if draw(st.booleans()):
            args["weight"].update({"missing": "default", "default": 4})
    if res in ("all_paths", "k_paths"):
        args["max_hops"] = draw(st.integers(1, 4))
        if draw(st.booleans()):
            args["min_hops"] = draw(st.integers(1, args["max_hops"]))
        if res == "k_paths":
            args["k"] = draw(st.integers(1, 3))
        args["order"] = draw(st.sampled_from(["path", "hops_desc"]))
    elif metric == "hops" and draw(st.booleans()):
        args["max_hops"] = draw(st.integers(1, 3))
    elif metric == "weight" and res == "distance" and draw(st.booleans()):
        args["max_hops"] = draw(st.integers(1, 3))
    if res in ("distance", "one_path", "all_optimal") and draw(st.booleans()):
        args["pairs"] = "min_over_pairs"
    if draw(st.booleans()):
        args["edge_where"] = [{"prop": "w", "cmp": draw(st.sampled_from(["gt", "le"])),
                               "value": draw(st.sampled_from([1, 2]))}]
    if draw(st.booleans()):
        t = draw(st.integers(0, T_MAX - 5))
        args["edge_window"] = {"t_a": t, "t_b": t + draw(st.integers(1, 30))}
    return args


@SETTINGS
@given(args=snapshot_calls(), seed=st.integers(0, 3))
# frontier-first loading stopped once every target was seen, from the union
# of the sources: p0 is a target and a source, so p1 -> p0 (3 hops) was lost
@example(args={"sources": {"uids": ["p0", "p1"]}, "t_valid": 15, "metric": "hops",
               "result": "distance", "dir": "out", "as_of_tt": OPEN_END, "limit": 100,
               "targets": {"uids": ["p0"]}}, seed=0)
def test_snapshot_paths_equal_the_oracle(args, seed):
    _agree(*stores(seed), args)


@SETTINGS
@given(seed=st.integers(0, 3), src=st.sampled_from(UIDS),
       strict=st.sampled_from(["strict", "non_strict"]), t=st.integers(0, 20),
       span=st.integers(5, 30), hi=st.integers(1, 3), res=st.sampled_from(["all_paths",
                                                                          "k_paths"]),
       weighted=st.booleans(), as_of=st.one_of(st.integers(1, 45), st.just(OPEN_END)))
def test_time_respecting_traces_equal_the_oracle(seed, src, strict, t, span, hi, res,
                                                 weighted, as_of):
    args: dict[str, Any] = {"sources": {"uids": [src]}, "window": {"t_a": t, "t_b": t + span},
                            "time_respecting": strict, "result": res, "max_hops": hi,
                            "as_of_tt": as_of, "order": "hops_desc"}
    if res == "k_paths":
        args["k"] = 2
        args["order"] = "path"
    if weighted:
        args["metric"] = "weight"
        args["weight"] = {"prop": "w", "missing": "default", "default": 1}
    _agree(*stores(seed), args)


@SETTINGS
@given(seed=st.integers(0, 3), src=st.sampled_from(UIDS), t=st.integers(0, 20),
       span=st.integers(5, 30), as_of=st.one_of(st.integers(1, 45), st.just(OPEN_END)),
       res=st.sampled_from(["distance", "k_paths"]), d=st.sampled_from(["out", "in"]))
def test_arrival_is_the_temporal_operators(seed, src, t, span, as_of, res, d):
    args: dict[str, Any] = {"sources": {"uids": [src]}, "window": {"t_a": t, "t_b": t + span},
                            "metric": "arrival", "result": res, "as_of_tt": as_of, "dir": d}
    if res == "k_paths":
        args.update({"k": 3, "targets": {"uids": UIDS[:3]}, "dir": "out"})
    _agree(*stores(seed), args)


# ---- the benchmark shapes ---------------------------------------------------- #

KNOWS = [("a", "b"), ("b", "c"), ("a", "d"), ("d", "c"), ("c", "e"), ("b", "e")]


def _snb():
    ops = [[{"op": "assert_node", "uid": u, "label": "Person", "props": {}, "vt_s": 0,
             "vt_e": OPEN_END} for u in "abcdef"]]
    ops.append([{"op": "assert_edge", "src": s, "dst": d, "rel_type": "KNOWS", "props": {},
                 "vt_s": 1, "vt_e": OPEN_END, "disc": ""} for s, d in KNOWS])
    # replies between persons: the interaction pattern IC14 scores paths by
    ops.append([{"op": "assert_edge", "src": s, "dst": d, "rel_type": "REPLIED", "props": {},
                 "vt_s": 2, "vt_e": OPEN_END, "disc": str(i)}
                for i, (s, d) in enumerate([("a", "b"), ("b", "a"), ("b", "c"), ("d", "c"),
                                            ("a", "d"), ("a", "d"), ("c", "e")])])
    return _build(ops)


REPLIES = {"pattern": {"nodes": [{"var": "x"}, {"var": "y"}],
                       "steps": [{"from": "x", "to": "y", "rel_type": "REPLIED", "var": "r"}]},
           "return": {"group": {"by": [{"as": "src", "expr": {"col": "x.uid"}},
                                       {"as": "dst", "expr": {"col": "y.uid"}}],
                                "aggs": [{"as": "w", "fn": "count"}]}}}


def test_ic13_hop_distance_and_its_absence():
    nat, duck, oracle = _snb()
    for a in (nat, duck):
        got = call_operator(a, "paths", {"sources": {"uids": ["a"]},
                                         "targets": {"uids": ["e", "a", "f"]},
                                         "t_valid": 5, "dir": "both", "rel_types": ["KNOWS"]})
        assert [(r["dst"], r["distance"]) for r in got["rows"]] == [("a", 0), ("e", 2)]
    _agree(nat, duck, oracle, {"sources": {"uids": ["a"]}, "targets": {"uids": ["e"]},
                               "t_valid": 5, "dir": "both", "rel_types": ["KNOWS"],
                               "result": "all_optimal"})


def test_ic14_all_shortest_paths_scored_by_a_pattern():
    nat, duck, oracle = _snb()
    args = {"sources": {"uids": ["a"]}, "targets": {"uids": ["c"]}, "t_valid": 5,
            "dir": "both", "rel_types": ["KNOWS"], "result": "all_optimal",
            "score": {"match": REPLIES, "missing": "default", "default": 0},
            "order": "score_desc"}
    _agree(nat, duck, oracle, args)
    rows = call_operator(nat, "paths", args)["rows"]
    # one value per pair under dir both: a-d replied twice, every other pair once
    assert [(r["path"], r["score"]) for r in rows] == [(["a", "d", "c"], 3),
                                                        (["a", "b", "c"], 2)]


def test_weights_from_a_pattern_one_cheapest_path_and_min_over_pairs():
    nat, duck, oracle = _snb()
    for res in ("one_path", "distance", "all_optimal"):
        args = {"sources": {"uids": ["a", "b"]}, "targets": {"uids": ["c", "e"]},
                "t_valid": 5, "dir": "both", "rel_types": ["KNOWS"], "metric": "weight",
                "weight": {"match": REPLIES, "missing": "default", "default": 10},
                "result": res, "pairs": "min_over_pairs"}
        _agree(nat, duck, oracle, args)



def _replies_store(seed: int):
    """Persons p0..p6, random KNOWS (some ending) and REPLIED edges: the
    IC14 shape on a random store."""
    rng = random.Random(100 + seed)
    ops = [[{"op": "assert_node", "uid": u, "label": "Person", "props": {}, "vt_s": 0,
             "vt_e": OPEN_END} for u in UIDS]]
    for i in range(24):
        u, v = rng.sample(UIDS, 2)
        s = rng.randrange(0, T_MAX - 2)
        e = OPEN_END if rng.random() < 0.7 else s + rng.randrange(1, T_MAX - s)
        ops.append([{"op": "assert_edge", "src": u, "dst": v, "rel_type": "KNOWS", "props": {},
                     "vt_s": s, "vt_e": e, "disc": str(i % 2)}])
    for i in range(30):
        # one direction per pair: under `dir: both` a pair has one weight
        u, v = sorted(rng.sample(UIDS, 2))
        ops.append([{"op": "assert_edge", "src": u, "dst": v, "rel_type": "REPLIED",
                     "props": {}, "vt_s": rng.randrange(0, 10), "vt_e": OPEN_END,
                     "disc": str(i)}])
    return _build(ops)


@pytest.mark.parametrize("seed", [0, 1, 2])
def test_instant_graph_by_arrays_equals_the_loop(seed, monkeypatch):
    """The instant graph from integer pattern weights or hops is built by
    array operations (`_graph_columns`): the same graph as the edge loop's,
    and the oracle's answers, for every direction, `missing` and result."""
    from gnomon.temporal import ops_general_paths as GP

    # these graphs are small: build them by arrays whatever their size
    monkeypatch.setattr(GP, "GRAPH_COLUMNS_MIN", 0)
    nat, duck, oracle = _replies_store(seed)
    for t in (3, 15, 30):
        for d in ("out", "in", "both"):
            for weight in (None, {"match": REPLIES}, {"match": REPLIES, "missing": "default",
                                                       "default": 3}):
                for res in ("distance", "one_path", "all_optimal"):
                    args = {"sources": {"uids": ["p0", "p3"]}, "targets": {"uids": ["p1", "p5", "p6"]},
                            "t_valid": t, "dir": d, "rel_types": ["KNOWS"], "result": res}
                    if weight is not None:
                        args.update(metric="weight", weight=weight)
                    _agree(nat, duck, oracle, args)
                filled = validate_args("paths", {**args, "result": "distance"})
                src = GP._endpoints(nat, filled["sources"], filled, strict=True)
                tgt = GP._endpoints(nat, filled["targets"], filled, strict=False)
                fast = GP._graph(nat, filled, src, tgt)
                with monkeypatch.context() as m:
                    m.setattr(GP, "_graph_columns", lambda *a, **k: None)
                    slow = GP._graph(nat, filled, src, tgt)
                assert {u: dict(v) for u, v in fast.adj.items()} == \
                    {u: dict(v) for u, v in slow.adj.items()}, (t, d, weight)



def test_reaches_by_arrays_is_the_set_search():
    """`_reaches` (level by level over sorted step arrays) answers what
    `_joined` (the two-sided set search) does, on random step lists."""
    import numpy as np

    from gnomon.temporal import ops_general_paths as GP

    rng = random.Random(7)
    for case in range(300):
        n = rng.randrange(1, 30)
        adj: dict[int, list[tuple]] = {}
        for _ in range(rng.randrange(0, 60)):
            u, v = rng.randrange(n), rng.randrange(n)
            adj.setdefault(u, []).append((v,))
        us = np.array([u for u, lst in adj.items() for _ in lst], dtype=np.int64)
        vs = np.array([e[0] for lst in adj.values() for e in lst], dtype=np.int64)
        order = np.argsort(us, kind="stable")
        s0 = rng.randrange(n)
        targets = set(rng.sample(range(n), rng.randrange(0, min(n, 4) + 1)))
        assert GP._reaches(us[order], vs[order], s0, targets) == GP._joined(adj, s0, targets), case


def test_tcr5_time_increasing_traces_from_a_pattern():
    ops = [[{"op": "assert_node", "uid": u, "label": "Account", "props": {}, "vt_s": 0,
             "vt_e": OPEN_END} for u in "uvwxyz"]]
    for i, (s, d, t) in enumerate([("u", "v", 3), ("v", "w", 5), ("v", "w", 2), ("w", "x", 5),
                                   ("w", "x", 6), ("u", "w", 9), ("x", "y", 7), ("v", "y", 1)]):
        ops.append([{"op": "assert_edge", "src": s, "dst": d, "rel_type": "TRANSFER",
                     "props": {"amount": i + 1}, "vt_s": t, "vt_e": OPEN_END,
                     "disc": str(i)}])
    nat, duck, oracle = _build(ops)
    for strict in ("strict", "non_strict"):
        args = {"sources": {"uids": ["u"]}, "window": {"t_a": 0, "t_b": 100},
                "rel_types": ["TRANSFER"], "time_respecting": strict,
                "result": "all_paths", "max_hops": 3, "order": "hops_desc"}
        _agree(nat, duck, oracle, args)
    got = call_operator(nat, "paths", args)["rows"]
    # v->y happened before u->v, and w->x after u->w never did
    assert [r["path"] for r in got] == [["u", "v", "w", "x"], ["u", "v", "w"],
                                        ["u", "v"], ["u", "w"]]


def test_tcr3_unbounded_distance_inside_an_edge_window():
    nat, duck, oracle = stores(1)
    _agree(nat, duck, oracle, {"sources": {"uids": ["p0"]}, "t_valid": 30,
                               "edge_window": {"t_a": 1, "t_b": 25}, "result": "distance"})


def test_arguments_are_checked_and_the_operator_is_engine_level():
    with pytest.raises(InvalidArgError):
        validate_args("paths", {"sources": {"uids": ["a"]}, "t_valid": 1,
                                "result": "all_paths"})
    with pytest.raises(InvalidArgError):
        validate_args("paths", {"sources": {"uids": ["a"]}, "window": {"t_a": 0, "t_b": 9},
                                "time_respecting": "strict"})
    with pytest.raises(InvalidArgError):
        validate_args("paths", {"sources": {"uids": ["a"]}, "t_valid": 1, "metric": "weight"})
    assert "paths" in REGISTRY and "paths" not in BASE_OPERATORS
