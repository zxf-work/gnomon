"""when all of its edges and nodes are valid at one shared instant) and with
FinBench's per-vertex truncation (`truncate`).

Every answer is checked against `Oracle.paths`, which for a window evaluates
the snapshot at every elementary instant and takes the least distance and the
paths reaching it, on the native engine and on DuckDB.
"""

from __future__ import annotations

import os
import random
from typing import Any

import pytest

from gnomon.core.errors import InvalidArgError
from gnomon.core.model import OPEN_END
from gnomon.temporal.algebra import call_operator, ensure_all_registered, validate_args

from .test_paths_operator import _agree, _build

ensure_all_registered()

EXAMPLES = int(os.environ.get("GNOMON_HYP_EXAMPLES", "25"))
UIDS = [f"p{i}" for i in range(7)]
T_MAX = 40


def _node(u: str, lo: int = 0, hi: int = OPEN_END) -> dict:
    return {"op": "assert_node", "uid": u, "label": "P", "props": {}, "vt_s": lo, "vt_e": hi}


def _edge(s: str, d: str, lo: int, hi: int, rel: str = "KNOWS", disc: str = "", **props) -> dict:
    return {"op": "assert_edge", "src": s, "dst": d, "rel_type": rel, "props": props,
            "vt_s": lo, "vt_e": hi, "disc": disc}


@pytest.fixture(scope="module")
def timeline():
    """a, b, c, d valid throughout, x on [0, 12); KNOWS a-b [0,10), b-c [5,20),
    a-c [15,30), c-d [0,40), a-x [0,40), x-d [0,40), with weights."""
    ops = [[_node(u) for u in "abcd"] + [_node("x", 0, 12)]]
    for s, d, lo, hi, w in (("a", "b", 0, 10, 1), ("b", "c", 5, 20, 1), ("a", "c", 15, 30, 5),
                            ("c", "d", 0, 40, 1), ("a", "x", 0, 40, 2), ("x", "d", 0, 40, 2)):
        ops.append([_edge(s, d, lo, hi, w=w)])
    return _build(ops)


def _dist(nat, args) -> dict[str, Any]:
    return {r["dst"]: r["distance"] for r in call_operator(nat, "paths", args)["rows"]}


def test_hop_distance_is_the_least_over_the_instants(timeline):
    nat, duck, oracle = timeline
    args = {"sources": {"uids": ["a"]}, "valid_window": {"t_a": 0, "t_b": 30}, "dir": "both"}
    _agree(nat, duck, oracle, args)
    assert _dist(nat, args) == {"b": 1, "c": 1, "d": 2, "x": 1}
    # [10, 15): a-b is over, a-c not yet valid, x exists only until 12
    narrow = {**args, "valid_window": {"t_a": 10, "t_b": 15}}
    _agree(nat, duck, oracle, narrow)
    assert _dist(nat, narrow) == {"x": 1, "d": 2, "c": 3, "b": 4}


def test_weighted_optimal_paths_share_one_instant(timeline):
    nat, duck, oracle = timeline
    base = {"sources": {"uids": ["a"]}, "targets": {"uids": ["d"]}, "dir": "both",
            "valid_window": {"t_a": 0, "t_b": 30}, "metric": "weight", "weight": {"prop": "w"}}
    for res in ("distance", "one_path", "all_optimal"):
        _agree(nat, duck, oracle, {**base, "result": res})
    rows = call_operator(nat, "paths", {**base, "result": "all_optimal"})["rows"]
    # a-b-c-d costs 3 on [5,10); a-x-d 4; a-c-d 6: only the first is optimal
    assert [r["path"] for r in rows] == [["a", "b", "c", "d"]]
    assert rows[0]["distance"] == 3
    late = {**base, "valid_window": {"t_a": 10, "t_b": 30}, "result": "all_optimal"}
    _agree(nat, duck, oracle, late)
    assert [r["path"] for r in call_operator(nat, "paths", late)["rows"]] == [["a", "x", "d"]]


def test_a_width_one_window_is_the_instant(timeline):
    nat, duck, oracle = timeline
    for t in (3, 7, 16, 25):
        for res in ("distance", "all_optimal"):
            inst = {"sources": {"uids": ["a"]}, "dir": "both", "t_valid": t, "result": res}
            win = {**{k: v for k, v in inst.items() if k != "t_valid"},
                   "valid_window": {"t_a": t, "t_b": t + 1}}
            if t >= 12:
                continue                      # x is gone: the instant form does not test nodes
            assert call_operator(nat, "paths", inst)["rows"] == \
                call_operator(nat, "paths", win)["rows"]


def _random_ops(seed: int):
    rng = random.Random(seed)
    batches = [[_node(u) for u in UIDS[:5]] +
               [_node(u, rng.randrange(0, 20), rng.randrange(20, T_MAX)) for u in UIDS[5:]]]
    for i in range(30):
        u, v = rng.sample(UIDS, 2)
        s = rng.randrange(0, T_MAX - 2)
        e = OPEN_END if rng.random() < 0.5 else s + rng.randrange(1, T_MAX - s)
        w = rng.choice([1, 2, 3, 0.5, 1.25, 2.5, 7])
        bag = {"w": w, "amount": rng.choice([1, 5, 9, 2.5])} if rng.random() < 0.85 else {"w": "x"}
        batches.append([_edge(u, v, s, e, rng.choice(["KNOWS", "KNOWS", "OTHER"]),
                              str(i % 2), **bag)])
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


def _random_window_args(rng: random.Random) -> dict[str, Any]:
    res = rng.choice(["distance", "one_path", "all_optimal"])
    metric = rng.choice(["hops", "weight"])
    t_a = rng.randrange(0, T_MAX - 1)
    args: dict[str, Any] = {
        "sources": {"uids": rng.sample(UIDS, rng.randint(1, 2))},
        "valid_window": {"t_a": t_a, "t_b": rng.choice([t_a + rng.randrange(1, 20), OPEN_END])},
        "metric": metric, "result": res, "dir": rng.choice(["out", "in", "both"]),
        "as_of_tt": rng.choice([OPEN_END, rng.randrange(1, 45)]),
        "limit": rng.choice([100, 3]),
    }
    if rng.random() < 0.5:
        args["targets"] = {"uids": rng.sample(UIDS, rng.randint(1, 3))}
    if rng.random() < 0.5:
        args["rel_types"] = ["KNOWS"]
    if metric == "weight":
        args["weight"] = {"prop": "w"}
        if rng.random() < 0.5:
            args["weight"].update({"missing": "default", "default": 4})
    elif rng.random() < 0.4:
        args["max_hops"] = rng.randint(1, 3)
    if rng.random() < 0.3:
        args["pairs"] = "min_over_pairs"
    if rng.random() < 0.3:
        args["edge_where"] = [{"prop": "w", "cmp": rng.choice(["gt", "le"]),
                               "value": rng.choice([1, 2])}]
    if rng.random() < 0.3:
        args["truncate"] = {"limit": rng.randint(1, 3),
                            "by": rng.choice(["vt_s", {"prop": "amount"}]),
                            "dir": rng.choice(["asc", "desc"])}
    return args


#: At GNOMON_HYP_EXAMPLES=2000 these differed from the oracle only in a
#: number's type -- a least cost equal as an integer sum at one instant and as
#: a fractional sum at another (6 and 6.0); the answer reads the earliest
#: instant's (cases 635, 715, 1267 and 1619 of the random test below).
W = {"metric": "weight", "weight": {"prop": "w"}, "as_of_tt": OPEN_END}
G10_TIES = [
    (3, {**W, "sources": {"uids": ["p3", "p2"]}, "valid_window": {"t_a": 24, "t_b": OPEN_END},
         "result": "all_optimal", "dir": "in", "limit": 3}),
    (3, {**W, "sources": {"uids": ["p2", "p3"]}, "valid_window": {"t_a": 31, "t_b": OPEN_END},
         "result": "distance", "dir": "in", "limit": 100}),
    (3, {**W, "sources": {"uids": ["p2"]}, "valid_window": {"t_a": 24, "t_b": 42},
         "result": "distance", "dir": "in", "limit": 100}),
    (3, {**W, "sources": {"uids": ["p0", "p4"]}, "valid_window": {"t_a": 24, "t_b": OPEN_END},
         "result": "all_optimal", "dir": "out", "limit": 100}),
]


@pytest.mark.parametrize("seed,args", G10_TIES)
def test_an_exact_tie_of_costs_reads_the_earliest_instant(seed, args):
    _agree(*stores(seed), dict(args))


#: An integer and a fractional optimal sum tie at the same instant (case
#: 1241: 1 against 0.5 + 0.5; case 1269: 3 against 1.5 + 1.5), where the
#: printed type is unspecified -- they agree as numbers (`_agree`) -- and the
#: engine answers the same call the same way every time.
G10_SAME_INSTANT = [
    (1, {**W, "sources": {"uids": ["p6", "p3"]}, "valid_window": {"t_a": 25, "t_b": OPEN_END},
         "result": "distance", "dir": "both", "limit": 100,
         "edge_where": [{"prop": "w", "cmp": "le", "value": 1}]}),
    (1, {"sources": {"uids": ["p2"]}, "valid_window": {"t_a": 25, "t_b": 39},
         "metric": "weight", "result": "all_optimal", "dir": "both", "as_of_tt": 13,
         "limit": 100, "targets": {"uids": ["p2", "p3"]},
         "weight": {"prop": "w", "missing": "default", "default": 4},
         "truncate": {"limit": 3, "by": "vt_s", "dir": "asc"}}),
]


@pytest.mark.parametrize("seed,args", G10_SAME_INSTANT)
def test_a_same_instant_tie_of_costs_agrees_as_numbers(seed, args):
    nat, duck, oracle = stores(seed)
    _agree(nat, duck, oracle, dict(args))
    for a in (nat, duck):
        first = call_operator(a, "paths", dict(args))["rows"]
        assert call_operator(a, "paths", dict(args))["rows"] == first


@pytest.mark.parametrize("case", range(EXAMPLES))
def test_random_window_paths_equal_the_oracle(case):
    rng = random.Random(300 + case)
    for _ in range(6):
        _agree(*stores(case % 4), _random_window_args(rng))


@pytest.mark.parametrize("case", range(EXAMPLES))
def test_random_truncated_paths_equal_the_oracle(case):
    """The cut on the instant form and on time-respecting traces."""
    rng = random.Random(600 + case)
    trunc = {"limit": rng.randint(1, 3), "by": rng.choice(["vt_s", {"prop": "amount"}]),
             "dir": rng.choice(["asc", "desc"])}
    src = {"uids": [rng.choice(UIDS)]}
    d = rng.choice(["out", "in", "both"])
    _agree(*stores(case % 4), {"sources": src, "t_valid": rng.randrange(0, T_MAX),
                               "dir": d, "truncate": trunc,
                               "result": rng.choice(["distance", "all_optimal"])})
    t = rng.randrange(0, 20)
    _agree(*stores(case % 4), {"sources": src, "window": {"t_a": t, "t_b": t + 25},
                               "time_respecting": rng.choice(["strict", "non_strict"]),
                               "result": "all_paths", "max_hops": 3, "truncate": trunc,
                               "order": "hops_desc"})


def test_tcr5_with_truncation():
    """TCR5 (FinBench): every strictly time-increasing transfer trace of at
    most three steps, each vertex expanding only its first `limit` transfers
    by time, cut before the window and the order are tested."""
    ops = [[_node(u) for u in "uvwxyz"]]
    for i, (s, d, t) in enumerate([("u", "v", 3), ("v", "w", 5), ("v", "w", 2), ("w", "x", 5),
                                   ("w", "x", 6), ("u", "w", 9), ("x", "y", 7), ("v", "y", 1)]):
        ops.append([_edge(s, d, t, OPEN_END, "TRANSFER", str(i), amount=i + 1)])
    nat, duck, oracle = _build(ops)
    base = {"sources": {"uids": ["u"]}, "window": {"t_a": 0, "t_b": 100},
            "rel_types": ["TRANSFER"], "time_respecting": "strict",
            "result": "all_paths", "max_hops": 3, "order": "hops_desc"}
    want = {
        # the latest transfer per account: u->w@9, then w->x@6 is too early
        (1, "desc"): [["u", "w"]],
        # the earliest: u->v@3, then v->y@1 is too early
        (1, "asc"): [["u", "v"]],
        (2, "desc"): [["u", "v", "w", "x"], ["u", "v", "w"], ["u", "v"], ["u", "w"]],
    }
    for (limit, d), paths in want.items():
        args = {**base, "truncate": {"limit": limit, "by": "vt_s", "dir": d}}
        _agree(nat, duck, oracle, args)
        assert [r["path"] for r in call_operator(nat, "paths", args)["rows"]] == paths
    # by amount: u keeps u->w (amount 6) over u->v (1)
    args = {**base, "truncate": {"limit": 1, "by": {"prop": "amount"}, "dir": "desc"}}
    _agree(nat, duck, oracle, args)
    assert [r["path"] for r in call_operator(nat, "paths", args)["rows"]] == [["u", "w"]]


def test_window_arguments_are_checked():
    base = {"sources": {"uids": ["a"]}, "valid_window": {"t_a": 0, "t_b": 9}}
    for bad in ({**base, "t_valid": 1}, {**base, "metric": "arrival", "window": {"t_a": 0, "t_b": 9}},
                {**base, "valid_window": {"t_a": 9, "t_b": 9}},
                {**base, "metric": "weight", "weight": {"prop": "w"}, "max_hops": 2},
                {"sources": {"uids": ["a"]}, "window": {"t_a": 0, "t_b": 9},
                 "metric": "arrival", "truncate": {"limit": 1, "by": "vt_s", "dir": "asc"}}):
        with pytest.raises(InvalidArgError):
            validate_args("paths", bad)
    # the instant form is unchanged and still needs t_valid
    with pytest.raises(InvalidArgError):
        validate_args("paths", {"sources": {"uids": ["a"]}})


# --------------------------------------------------------------------------- #
# Time-respecting traces at t_valid, and under a window                    #
# --------------------------------------------------------------------------- #

def test_a_trace_at_t_valid_skips_an_edge_no_longer_valid():
    """A -> B -> C with increasing times, B -> C ended before t_valid: at
    t_valid the strict trace A -> B -> C is not a path (FinBench reads are as
    of now; DuckDB and PostgreSQL exclude it), without t_valid it is."""
    ops = [[_node(u) for u in "ABC"],
           [_edge("A", "B", 10, OPEN_END, "TRANSFER", "0")],
           [_edge("B", "C", 20, 40, "TRANSFER", "1")]]
    nat, duck, oracle = _build(ops)
    base = {"sources": {"uids": ["A"]}, "window": {"t_a": 0, "t_b": 100},
            "rel_types": ["TRANSFER"], "time_respecting": "strict",
            "result": "all_paths", "max_hops": 3}
    for args in (base, {**base, "t_valid": 50}, {**base, "t_valid": 30}):
        _agree(nat, duck, oracle, args)
    paths = lambda a: [r["path"] for r in call_operator(nat, "paths", a)["rows"]]  # noqa: E731
    assert paths({**base, "t_valid": 50}) == [["A", "B"]]
    assert paths({**base, "t_valid": 30}) == [["A", "B"], ["A", "B", "C"]]
    assert paths(base) == [["A", "B"], ["A", "B", "C"]]
    # under a window the trace needs one shared instant: [45, 60) has none for B -> C
    win = {k: v for k, v in base.items()}
    _agree(nat, duck, oracle, {**win, "valid_window": {"t_a": 45, "t_b": 60}})
    assert paths({**win, "valid_window": {"t_a": 45, "t_b": 60}}) == [["A", "B"]]
    _agree(nat, duck, oracle, {**win, "valid_window": {"t_a": 25, "t_b": 60}})
    assert paths({**win, "valid_window": {"t_a": 25, "t_b": 60}}) == [["A", "B"], ["A", "B", "C"]]


@pytest.mark.parametrize("case", range(EXAMPLES))
def test_random_traces_at_t_valid_and_in_a_window_equal_the_oracle(case):
    rng = random.Random(900 + case)
    t = rng.randrange(0, 20)
    trace = {"sources": {"uids": rng.sample(UIDS, rng.randint(1, 2))},
             "window": {"t_a": t, "t_b": t + rng.randrange(5, 30)},
             "time_respecting": rng.choice(["strict", "non_strict"]),
             "result": rng.choice(["all_paths", "k_paths"]), "max_hops": rng.randint(1, 3),
             "dir": rng.choice(["out", "in", "both"]),
             "as_of_tt": rng.choice([OPEN_END, rng.randrange(1, 45)])}
    if trace["result"] == "k_paths":
        trace["k"] = rng.randint(1, 3)
    if rng.random() < 0.4:
        trace["metric"] = "weight"
        trace["weight"] = {"prop": "w", "missing": "default", "default": 1}
    if rng.random() < 0.3:
        trace["truncate"] = {"limit": rng.randint(1, 3), "by": "vt_s",
                             "dir": rng.choice(["asc", "desc"])}
    _agree(*stores(case % 4), {**trace, "t_valid": rng.randrange(0, T_MAX)})
    va = rng.randrange(0, T_MAX - 1)
    _agree(*stores(case % 4), {**trace, "valid_window": {"t_a": va, "t_b": va + rng.randrange(1, 20)}})
    plain = {k: v for k, v in trace.items() if k not in ("time_respecting", "window")}
    _agree(*stores(case % 4), {**plain, "valid_window": {"t_a": va, "t_b": va + rng.randrange(1, 20)}})


# --------------------------------------------------------------------------- #
# Weights from a pattern under valid_window, evaluated for the vertices  #
# the search expands (restricted to them), or whole                          #
# --------------------------------------------------------------------------- #

#: one value per pair: the OTHER edges between the two, either way
PAIRS = {"pattern": {"nodes": [{"var": "x"}, {"var": "y"}],
                     "steps": [{"from": "x", "to": "y", "rel_type": "OTHER", "dir": "both"}]},
         "return": {"group": {"by": [{"as": "src", "expr": {"col": "x.uid"}},
                                     {"as": "dst", "expr": {"col": "y.uid"}}],
                              "aggs": [{"as": "w", "fn": "count"}]}}}


def _branch(a: str, b: str) -> dict[str, Any]:
    return {"pattern": {"nodes": [{"var": "x"}, {"var": "y"}],
                        "steps": [{"from": "x", "to": "y", "rel_type": "OTHER", "dir": "out"},
                                  {"from": "x", "to": "y", "rel_type": "KNOWS", "dir": "both"}]},
            "rows": [{"as": "src", "expr": {"col": f"{a}.uid"}},
                     {"as": "dst", "expr": {"col": f"{b}.uid"}}]}


#: IC14's shape: a union of the two orientations, counted per pair
UNION = {"union": [_branch("x", "y"), _branch("y", "x")],
         "return": {"group": {"by": [{"as": "src", "expr": {"col": "src"}},
                                     {"as": "dst", "expr": {"col": "dst"}}],
                              "aggs": [{"as": "n", "fn": "count"}],
                              "project": [{"as": "w", "expr": {"arith": "+", "l": {"col": "n"},
                                                                "r": {"lit": 1}}}]}}}


def _pattern_weight_args(rng: random.Random) -> dict[str, Any]:
    args = _random_window_args(rng)
    args.pop("truncate", None)
    args.pop("edge_where", None)
    args.pop("max_hops", None)
    args["metric"] = "weight"
    args["dir"] = "both"
    args["weight"] = {"match": rng.choice([PAIRS, UNION])}
    if rng.random() < 0.5:
        args["weight"].update({"missing": "default", "default": rng.choice([1, 3])})
    return args


@pytest.mark.parametrize("case", range(EXAMPLES))
@pytest.mark.parametrize("mode", ["seeded", "whole"])
def test_random_pattern_weights_seeded_or_whole_equal_the_oracle(case, mode, monkeypatch):
    """A weight pattern evaluated per batch of the vertices the search
    expands (one vertex a batch, never the whole), or whole at once, gives
    the oracle's answer (which evaluates the pattern whole)."""
    from gnomon.temporal import ops_general_paths as GP

    if mode == "seeded":
        monkeypatch.setattr(GP, "WEIGHT_BATCH", 1)
        monkeypatch.setattr(GP, "SEEDED_WHOLE_FRACTION", 0)
        monkeypatch.setattr(GP, "SEEDED_BATCHES_MAX", 10 ** 9)
    else:
        monkeypatch.setattr(GP, "SEEDED_BATCHES_MAX", 0)
    rng = random.Random(900 + case)
    for _ in range(4):
        _agree(*stores(case % 4), _pattern_weight_args(rng))


def test_an_unreachable_target_reads_no_weights(monkeypatch):
    """A target no walk reaches (here a node with no edge at all) is
    answered without evaluating the weight pattern or searching."""
    from gnomon.temporal import ops_general_paths as GP

    ops = [[_node(u) for u in "abcz"]]
    for s_, d_, lo, hi in (("a", "b", 0, 30), ("b", "c", 5, 20)):
        ops.append([_edge(s_, d_, lo, hi), _edge(s_, d_, lo, hi, rel="OTHER", disc="o")])
    nat, duck, oracle = _build(ops)
    calls = []
    real = GP._pair_table
    monkeypatch.setattr(GP, "_pair_table", lambda *a, **k: calls.append(1) or real(*a, **k))
    args = {"sources": {"uids": ["a"]}, "targets": {"uids": ["z"]}, "dir": "both",
            "valid_window": {"t_a": 0, "t_b": 30}, "metric": "weight",
            "weight": {"match": UNION}}
    _agree(nat, duck, oracle, args)
    assert calls == []
    reach = {**args, "targets": {"uids": ["c", "z"]}}
    _agree(nat, duck, oracle, reach)
    assert [r["dst"] for r in call_operator(nat, "paths", reach)["rows"]] == ["c"]


def test_node_intervals_are_the_node_scan_s():
    """The native point read of node intervals says what the node scan
    says about the same ids."""
    nat, _duck, _oracle = stores(0)
    if not hasattr(nat, "node_intervals"):
        pytest.skip("native only")
    for va, vb, as_of in ((0, OPEN_END, OPEN_END), (5, 25, OPEN_END), (10, 11, 20)):
        n = nat.nodes_columnar(as_of_tt=as_of, vt_min=va, vt_max=vb)
        want: dict[int, list] = {}
        for i, s_, e_ in zip(n["uid_id"], n["vt_s"], n["vt_e"]):
            if s_ < vb and va < e_:
                want.setdefault(int(i), []).append((int(s_), int(e_)))
        ids = sorted({int(i) for i in nat.dense_ids(UIDS)})
        got = nat.node_intervals(ids, va, vb, as_of)
        assert {k: sorted(v) for k, v in got.items()} == \
            {k: sorted(v) for k, v in want.items() if k in ids}


# --------------------------------------------------------------------------- #
# The instant form -- weights from a pattern with a walk (bound in the    #
# cheapest order), and an unreachable target answered without weights        #
# --------------------------------------------------------------------------- #

#: one value per pair: the OTHER walks of one or two steps between the two
WALKS = {"pattern": {"nodes": [{"var": "x"}, {"var": "y"}],
                     "steps": [{"from": "x", "to": "y", "rel_type": "OTHER", "dir": "both",
                                "hops": {"min": 1, "max": 2}},
                               {"from": "x", "to": "y", "rel_type": "KNOWS", "dir": "both"}]},
         "return": {"group": {"by": [{"as": "src", "expr": {"col": "x.uid"}},
                                     {"as": "dst", "expr": {"col": "y.uid"}}],
                              "aggs": [{"as": "w", "fn": "count"}]}}}


@pytest.mark.parametrize("case", range(EXAMPLES))
def test_random_instant_pattern_weights_equal_the_oracle(case):
    """Instant-form weights from a pattern (with a walk the compiler may
    move, its depth unread) give the oracle's answer."""
    rng = random.Random(1200 + case)
    for _ in range(4):
        args = {"sources": {"uids": rng.sample(UIDS, rng.randint(1, 2))},
                "t_valid": rng.randrange(0, T_MAX), "dir": "both", "metric": "weight",
                "weight": {"match": rng.choice([WALKS, PAIRS, UNION]),
                           **({"missing": "default", "default": 2} if rng.random() < 0.5 else {})},
                "result": rng.choice(["distance", "one_path", "all_optimal"]),
                "as_of_tt": rng.choice([OPEN_END, rng.randrange(1, 45)])}
        if rng.random() < 0.7:
            args["targets"] = {"uids": rng.sample(UIDS, rng.randint(1, 3))}
        if rng.random() < 0.3:
            args["pairs"] = "min_over_pairs"
        _agree(*stores(case % 4), args)


def test_an_unreachable_instant_target_reads_no_weights(monkeypatch):
    from gnomon.temporal import ops_general_paths as GP

    ops = [[_node(u) for u in "abcz"]]
    for s_, d_, lo, hi in (("a", "b", 0, 30), ("b", "c", 5, 20)):
        ops.append([_edge(s_, d_, lo, hi), _edge(s_, d_, lo, hi, rel="OTHER", disc="o")])
    nat, duck, oracle = _build(ops)
    calls = []
    real = GP._pair_table
    monkeypatch.setattr(GP, "_pair_table", lambda *a, **k: calls.append(1) or real(*a, **k))
    args = {"sources": {"uids": ["a"]}, "targets": {"uids": ["z"]}, "dir": "both",
            "t_valid": 10, "metric": "weight", "weight": {"match": UNION}}
    _agree(nat, duck, oracle, args)
    assert calls == []
    _agree(nat, duck, oracle, {**args, "targets": {"uids": ["c", "z"]}})


# --------------------------------------------------------------------------- #
# The window search in the engine gives the Python search's pieces       #
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("case", range(EXAMPLES * 4))
def test_native_window_search_equals_the_python_search(case):
    """`winsearch.rs` against `_win_search`, piece for piece and in
    settling order, on random window graphs (dyadic and integer weights,
    split existence, targets, a bound, min-over-pairs' first target, a hop
    bound)."""
    from fractions import Fraction

    from gnomon.temporal import ops_general_paths as GP

    rng = random.Random(4000 + case)
    n = rng.randint(2, 12)
    va, vb = 0, 40
    adj: dict[int, list[tuple]] = {}
    for _ in range(rng.randint(0, 40)):
        u, v = rng.randrange(n), rng.randrange(n)
        if u == v:
            continue
        lo = rng.randrange(0, 39)
        hi = rng.choice([40, lo + rng.randrange(1, 40 - lo + 1)])
        w = rng.choice([1, 2, 3, Fraction(1, 2), Fraction(5, 4), Fraction(5, 2), 7])
        adj.setdefault(u, []).append((v, lo, min(hi, 40), w, 0, lo))
    exists = {}
    for u in range(n):
        if rng.random() < 0.8:
            exists[u] = [(0, 40)]
        else:
            a = rng.randrange(0, 30)
            exists[u] = [(a, a + rng.randrange(1, 10))]
    hops = rng.random() < 0.3
    if hops:
        adj = {u: [(v, lo, hi, 1, sc, ts) for v, lo, hi, _w, sc, ts in lst] for u, lst in adj.items()}
    targets = set(rng.sample(range(n), rng.randint(1, min(3, n)))) if rng.random() < 0.6 else None
    bound = rng.choice([None, None, 2, Fraction(7, 2), 5]) if targets else None
    first_only = bool(targets) and rng.random() < 0.4
    max_hops = rng.choice([None, 1, 2]) if hops else None
    src = rng.randrange(n)
    want = GP._win_search(adj, exists, src, va, vb, max_hops, targets=targets, bound=bound,
                          first_only=first_only)
    graph = GP._NativeGraph(adj, exists, [src] + sorted(targets or ()))
    got = GP._win_search_native(graph, src, va, vb, max_hops, targets=targets,
                                bound=bound, first_only=first_only)
    if got is None:
        # the engine declines a bound finer than the weights' scale only
        assert bound is not None and GP._scaled(bound, graph.k) is None
        return
    def norm(pc):
        return {u: [(list(map(tuple, k)), (type(c) is Fraction, c)) for k, c in lst]
                for u, lst in pc.items()}

    assert norm(got) == norm(want)



@pytest.mark.parametrize("case", range(EXAMPLES))
def test_pair_tables_column_wise_equal_the_rows(case, monkeypatch):
    """A weight pattern read column-wise from the engine (dense ids,
    typed weights) gives the pair table its rows give, at an instant and in
    a window, for both pattern shapes."""
    from gnomon.temporal import ops_general_paths as GP
    from gnomon.temporal import ops_match as OM

    nat, _duck, _oracle = stores(case % 4)
    if not hasattr(nat, "match_pairs"):
        pytest.skip("native only")
    rng = random.Random(1500 + case)
    for _ in range(3):
        t = rng.randrange(0, T_MAX)
        args = {"as_of_tt": rng.choice([OPEN_END, rng.randrange(1, 45)]),
                **({"t_valid": t} if rng.random() < 0.5 else
                   {"valid_window": {"t_a": t, "t_b": t + rng.randrange(1, 20)}})}
        spec = {"match": rng.choice([PAIRS, UNION, WALKS])}
        both = rng.random() < 0.5
        cols = GP._pair_table(nat, spec, args, both)
        monkeypatch.setattr(OM, "run_pairs", lambda *a, **k: None)
        rows = GP._pair_table(nat, spec, args, both)
        monkeypatch.undo()
        assert cols == rows
        assert {type(v) for v in cols.values()} == {type(v) for v in rows.values()}
