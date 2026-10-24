"""The composition overhead: three exact rewrites inside `paths`.

On an SNB SF1 store the weighted and windowed path rows spent up to half of
their time in Python around the engine calls:

- float weights at an instant (BI15): the loop graph and a Dijkstra over exact
  rationals; now one power-of-two scale, the integer array graph and integer
  sums (`_dyadic_pair_arrays`), divided back at the output;
- the window form's hop graph (IC13 window): a tuple per traversal, then a
  second loop into the native search's arrays; now the traversal arrays
  (`_HopAdj`, `_NativeGraph._from_hops`);
- the instant Dijkstra (BI19's eight sources): it stops once every target is
  settled, or past the least distance under `min_over_pairs`.

Each is checked here against the path it replaces, payload for payload with
exact types, on the random stores of the paths suites (both backends where the
route exists), and against the oracle as the suites compare.
"""

from __future__ import annotations

import json
import random

import pytest

from gnomon.core.model import OPEN_END
from gnomon.temporal import ops_general_paths as GP
from gnomon.temporal.algebra import call_operator, ensure_all_registered

from . import test_paths_operator as PO
from . import test_paths_window as PW

ensure_all_registered()

STRIP = ("plan", "freshness", "args_echo", "elapsed_ms", "timing")


def _payload(a, args):
    env = call_operator(a, "paths", json.loads(json.dumps(args)))
    return json.dumps({k: v for k, v in env.items() if k not in STRIP}, sort_keys=True)


#: weights from a pattern, as floats (BI15's 10 / (s + 10)): one per pair
FLOATS = {"pattern": {"nodes": [{"var": "x"}, {"var": "y"}],
                      "steps": [{"from": "x", "to": "y", "rel_type": "KNOWS", "var": "k"}]},
          "return": {"group": {"by": [{"as": "src", "expr": {"col": "x.uid"}},
                                      {"as": "dst", "expr": {"col": "y.uid"}}],
                               "aggs": [{"as": "n", "fn": "count"}],
                               "project": [{"as": "w", "expr": {
                                   "arith": "/", "l": {"lit": 10.0},
                                   "r": {"arith": "+", "l": {"col": "n"}, "r": {"lit": 3.0}}}}]}}}


def _float_args(rng):
    a = {"sources": {"uids": rng.sample(PO.UIDS, rng.randint(1, 3))}, "t_valid": rng.randrange(0, 40),
         "dir": rng.choice(["out", "both"]), "rel_types": ["KNOWS"], "metric": "weight",
         "weight": {"match": FLOATS, "missing": rng.choice(["exclude", "default"]), "default": 1.0},
         "result": rng.choice(["distance", "one_path", "all_optimal"]),
         "pairs": rng.choice(["each", "min_over_pairs"]), "as_of_tt": rng.choice([OPEN_END, 20])}
    if a["weight"]["missing"] == "exclude":
        del a["weight"]["default"]
    if rng.random() < 0.7:
        a["targets"] = {"uids": rng.sample(PO.UIDS, rng.randint(1, 4))}
    return a


@pytest.mark.parametrize("case", range(40))
def test_float_weights_at_one_scale_equal_the_rationals(case, monkeypatch):
    rng = random.Random(7_000 + case)
    nat, duck, oracle = PO.stores(case % 8)
    args = _float_args(rng)
    new = _payload(nat, args)
    monkeypatch.setattr(GP, "_dyadic_pair_arrays", lambda *a: None)
    assert new == _payload(nat, args), args
    PO._agree(nat, duck, oracle, args)


def test_the_random_float_cases_take_the_scaled_route(monkeypatch):
    """Most of the cases above read their weights at one scale (the rest have
    an empty table or no walk to weigh), so the comparison is not vacuous."""
    real, hits = GP._dyadic_pair_arrays, []
    monkeypatch.setattr(GP, "_dyadic_pair_arrays", lambda *a: hits.append(real(*a) is not None) or real(*a))
    taken = 0
    for case in range(40):
        rng = random.Random(7_000 + case)
        nat, _, _ = PO.stores(case % 8)
        hits.clear()
        _payload(nat, _float_args(rng))
        taken += any(hits)
    assert taken >= 25


def test_the_scale_is_taken_on_the_float_tables():
    """The engine's float table takes the scaled route; an int default, a
    mixed table or a non-finite weight keeps the rationals."""
    import numpy as np

    src = np.array([1, 2, 3], dtype=np.uint32)
    dst = np.array([2, 3, 4], dtype=np.uint32)
    kind = np.array([2, 2, 2], dtype=np.uint8)
    wi = np.zeros(3, dtype=np.int64)
    wf = np.array([0.5, 10 / 13, 1.0])
    got = GP._dyadic_pair_arrays((src, dst, kind, wi, wf), True, {"missing": "default", "default": 1.0})
    assert got is not None
    keys, vals, scale, default = got
    from fractions import Fraction
    assert [Fraction(int(v), 1 << scale) for v in vals] == [Fraction(x) for x in wf]
    assert Fraction(default, 1 << scale) == 1
    assert GP._dyadic_pair_arrays((src, dst, kind, wi, wf), True, {"missing": "default", "default": 1}) is None
    assert GP._dyadic_pair_arrays((src, dst, np.array([2, 1, 2], dtype=np.uint8), wi, wf), True, {}) is None
    assert GP._dyadic_pair_arrays((src, dst, kind, wi, np.array([0.5, np.inf, 1.0])), True, {}) is None


@pytest.mark.parametrize("case", range(60))
def test_the_window_hop_graph_from_arrays_equals_the_loop(case, monkeypatch):
    rng = random.Random(9_000 + case)
    nat, duck, oracle = PW.stores(case % 8)
    args = PW._random_window_args(rng)
    args["metric"] = "hops"
    args.pop("weight", None)
    real = GP._win_graph
    for a in (nat, duck):
        new = _payload(a, args)
        monkeypatch.setattr(GP, "_win_graph", lambda *x, **k: real(*x, **{**k, "arrays": False}))
        old = _payload(a, args)
        monkeypatch.setattr(GP, "_win_graph", real)
        assert new == old, args
    PW._agree(nat, duck, oracle, args)


@pytest.mark.parametrize("case", range(60))
def test_the_dijkstra_stop_keeps_every_row(case, monkeypatch):
    rng = random.Random(11_000 + case)
    nat, duck, oracle = PO.stores(case % 8)
    args = {"sources": {"uids": rng.sample(PO.UIDS, rng.randint(1, 4))}, "t_valid": rng.randrange(0, 40),
            "dir": rng.choice(["out", "in", "both"]), "rel_types": ["KNOWS"],
            "metric": rng.choice(["hops", "weight"]),
            "result": rng.choice(["distance", "one_path", "all_optimal"]),
            "pairs": rng.choice(["each", "min_over_pairs"])}
    if args["metric"] == "weight":
        args["weight"] = {"prop": "w", "missing": "default", "default": 2}
    if rng.random() < 0.8:
        args["targets"] = {"uids": rng.sample(PO.UIDS, rng.randint(1, 4))}
    real = GP._dijkstra
    for a in (nat, duck):
        new = _payload(a, args)
        monkeypatch.setattr(GP, "_dijkstra", lambda g, s, h, targets=None, bound=None: real(g, s, h))
        old = _payload(a, args)
        monkeypatch.setattr(GP, "_dijkstra", real)
        assert new == old, args
    PO._agree(nat, duck, oracle, args)
