"""The matcher's typed-property cache and closures, and match's page bound.

- A typed-property table starts sparse and turns dense as a scan fills it,
  its bytes kept as it fills: a point read no longer writes one slot per props
  string of the segment, nor does the cache walk the table to account for it
  (FinBench SF1 r10, TSR1 5.7 -> 0.7 ms). Answers: the reference route, at the
  shipped budget and at one that drops the cache on every call.
- The multi-seed closure search runs over one frontier and one seen-set
  (BI12, 1.8M seeds: -1 s of 4.1 s), and a page may hold a million rows, so a
  large answer is not recomputed once per 10,000 rows (BIH-R7, 34 pages a cell
  at SF1). `_canonicalize_floats` copies rows of plain scalars without a call per
  value. Answers: the reference route; pages concatenated; the old function.
"""

from __future__ import annotations

import json
import math
import random
import tempfile

import numpy as np
import pytest

from gnomon.core.errors import GnomonError
from gnomon.core.model import OPEN_END
from gnomon.temporal import algebra as AL
from gnomon.temporal import ops_match as OM
from gnomon.temporal.algebra import call_operator, ensure_all_registered

from .test_state_series import T_MAX as SS_T_MAX
from .test_state_series import stores as ss_stores

ensure_all_registered()


def _both(adapter, args):
    """(native rows, reference rows) or the error each raised, as comparable values."""
    out = []
    for route in ("native", "reference"):
        try:
            got = OM.run(adapter, OM.validate(json.loads(json.dumps(args))), route=route)
            out.append(("ok", got["rows_total"], json.dumps(got["rows"], sort_keys=True, default=str)))
        except GnomonError as e:
            out.append(("err", type(e).__name__))
    return out


def _prop_calls(rng):
    calls = []
    for _ in range(12):
        tau = rng.choice(["str", "int", "float", "bool"])
        key = rng.choice(["q", "st"])
        node = {"var": "x", "labels": [rng.choice(["A", "B"])]}
        if rng.random() < 0.5:
            node["uids"] = rng.sample([f"n{i}" for i in range(6)], rng.randint(1, 2))
        calls.append({"pattern": {"nodes": [node], "steps": []},
                      "return": {"rows": [{"as": "u", "expr": {"col": "x.uid"}},
                                          {"as": "v", "expr": {"tprop": ["x", key], "tau": tau}},
                                          {"as": "w", "expr": {"tprop": ["x", "st" if key == "q" else "q"],
                                                               "tau": "str"}}],
                                 "order_by": [{"expr": {"col": "u"}, "dir": "asc"}]},
                      "scope": {"vt": {"instant": rng.randrange(0, SS_T_MAX)},
                                "as_of_tt": rng.choice([OPEN_END - 1, rng.randrange(1, 40)])},
                      "limit": 1000})
    return calls


@pytest.mark.parametrize("seed", range(6))
def test_typed_props_at_any_cache_budget_equal_the_reference(seed, monkeypatch):
    nat, _, _ = ss_stores(seed)
    rng = random.Random(500 + seed)
    calls = _prop_calls(rng)
    want = [_both(nat, c) for c in calls]
    for w in want:
        assert w[0] == w[1], w
    # the cache dropped on every put: every call rebuilds its tables
    monkeypatch.setenv("GNOMON_MATCH_PROPS_BYTES", "1")
    assert [_both(nat, c) for c in calls] == want
    monkeypatch.delenv("GNOMON_MATCH_PROPS_BYTES")
    # and twice more at the shipped budget: the tables filled above, read back
    assert [_both(nat, c) for c in calls] == want
    assert [_both(nat, c) for c in calls] == want


def _graph_store(seed: int, n: int = 60):
    from gnomon.storage.native import NativeAdapter

    tmp = tempfile.TemporaryDirectory(prefix="gnomon-test-bfs-")
    a = NativeAdapter(tmp.name)
    rng = random.Random(seed)
    ops = [{"op": "assert_node", "uid": f"v{i}", "label": "V", "props": {}, "vt_s": 0, "vt_e": OPEN_END}
           for i in range(n)]
    for k in range(3 * n):
        u, v = rng.randrange(n), rng.randrange(n)
        ops.append({"op": "assert_edge", "src": f"v{u}", "dst": f"v{v}", "rel_type": rng.choice(["R", "R", "S"]),
                    "props": {}, "vt_s": rng.randrange(0, 5), "vt_e": rng.choice([OPEN_END, rng.randrange(6, 20)]),
                    "disc": str(k)})
    a.begin()
    a.apply_ops(ops, 1)
    a.commit()
    a._test_tmpdir = tmp
    return a


@pytest.mark.parametrize("seed", range(6))
def test_many_seed_closures_equal_the_reference(seed):
    a = _graph_store(seed)
    rng = random.Random(900 + seed)
    try:
        for _ in range(10):
            lo = rng.choice([0, 1, 2])
            hi = rng.choice([None, lo + rng.randint(0, 3)])
            if hi is None:
                lo = min(lo, 1)         # an unbounded step starts at 0 or 1 hops
            hops = {"min": lo, "max": hi}
            args = {"pattern": {"nodes": [{"var": "x", "labels": ["V"]}, {"var": "y", "labels": ["V"]}],
                                "steps": [{"from": "x", "to": "y", "rel_type": "R",
                                           "dir": rng.choice(["out", "in", "both"]), "hops": hops}]},
                    "return": {"group": {"by": [{"as": "x", "expr": {"col": "x.uid"}},
                                                {"as": "y", "expr": {"col": "y.uid"}}],
                                         "aggs": [{"as": "n", "fn": "count"}]},
                               "order_by": [{"expr": {"col": "x"}, "dir": "asc"},
                                            {"expr": {"col": "y"}, "dir": "asc"}]},
                    "scope": {"vt": {"instant": rng.randrange(0, 10)}}, "limit": 10_000}
            got = _both(a, args)
            assert got[0] == got[1], args
            assert got[0][0] == "ok" and got[0][1] > 0, args
    finally:
        a.close()


def test_a_page_may_hold_more_than_ten_thousand_rows():
    from gnomon.storage.native import NativeAdapter

    tmp = tempfile.TemporaryDirectory(prefix="gnomon-test-page-")
    a = NativeAdapter(tmp.name)
    ops = [{"op": "assert_node", "uid": u, "label": "N", "props": {}, "vt_s": 0, "vt_e": OPEN_END}
           for u in [f"s{i}" for i in range(150)] + [f"d{i}" for i in range(97)]]
    ops += [{"op": "assert_edge", "src": f"s{i % 150}", "dst": f"d{i % 97}", "rel_type": "R", "props": {},
             "vt_s": 0, "vt_e": OPEN_END, "disc": str(i)} for i in range(14_000)]
    a.begin()
    a.apply_ops(ops, 1)
    a.commit()
    q = {"pattern": {"nodes": [{"var": "x"}, {"var": "y"}],
                     "steps": [{"var": "e", "from": "x", "to": "y", "rel_type": "R"}]},
         "return": {"rows": [{"as": "x", "expr": {"col": "x.uid"}}, {"as": "d", "expr": {"col": "e.disc"}}],
                    "order_by": [{"expr": {"col": "d"}, "dir": "asc"}]},
         "scope": {"vt": {"instant": 5}}}
    try:
        one = call_operator(a, "match", {**q, "limit": 20_000})
        assert one["rows_total"] == 14_000 and len(one["rows"]) == 14_000 and not one["truncated"]
        pages, cursor = [], None
        while True:
            env = call_operator(a, "match", {**q, "limit": 10_000, "cursor": cursor})
            pages.extend(env["rows"])
            cursor = env["cursor"]
            if not cursor:
                break
        assert pages == one["rows"]
        assert OM.MAX_LIMIT == 1_000_000
    finally:
        a.close()
        tmp.cleanup()


def _old_canonicalize(obj):
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            raise ValueError("non-finite")
        return round(obj, 9)
    if isinstance(obj, dict):
        return {k: _old_canonicalize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_old_canonicalize(v) for v in obj]
    return obj


def _rand_value(rng, depth=0):
    kinds = ["int", "str", "bool", "none", "float", "npf", "npi", "tuple"] + (["list", "dict"] if depth < 3 else [])
    k = rng.choice(kinds)
    if k == "int":
        return rng.randrange(-10**12, 10**12)
    if k == "str":
        return rng.choice(["a", "", "x" * 5])
    if k == "bool":
        return rng.random() < 0.5
    if k == "none":
        return None
    if k == "float":
        return rng.uniform(-1e6, 1e6)
    if k == "npf":
        return np.float64(rng.uniform(-10, 10))
    if k == "npi":
        return np.int64(rng.randrange(100))
    if k == "tuple":
        return (1, 2.5, "t")
    if k == "list":
        return [_rand_value(rng, depth + 1) for _ in range(rng.randint(0, 5))]
    return {f"k{i}": _rand_value(rng, depth + 1) for i in range(rng.randint(0, 5))}


def test_the_canonicalisation_fast_path_equals_the_old_function():
    rng = random.Random(3)
    for _ in range(2000):
        v = _rand_value(rng)
        new, old = AL._canonicalize_floats(v), _old_canonicalize(v)
        assert repr(new) == repr(old)
        assert type(new) is type(old)
        if isinstance(v, (list, dict)):
            assert new is not v           # a copy, as before
    rows = [{"a": "x", "b": 1, "c": None, "d": True}] * 3
    out = AL._canonicalize_floats({"rows": rows})
    assert out["rows"] == rows and all(r is not s for r, s in zip(out["rows"], rows))
