"""The per-call envelope: each new path returns the
whole envelope of the path it replaces, which stays behind its switch.

- a repeat call takes the kept plan of its arguments (GNOMON_CALL_FAST)
"""

from __future__ import annotations

import json
import random
import tempfile

import pytest

from gnomon.core.errors import CostError
from gnomon.core.model import OPEN_END
from gnomon.temporal import algebra as A
from gnomon.temporal.algebra import REGISTRY, call_operator, ensure_all_registered

ensure_all_registered()


def _native():
    from gnomon.storage.native import NativeAdapter

    tmp = tempfile.TemporaryDirectory(prefix="gnomon-test-c4fix51-")
    a = NativeAdapter(tmp.name)
    a._test_tmpdir = tmp
    return a


def _duck():
    from gnomon.storage.duckdb_adapter import DuckDBAdapter

    return DuckDBAdapter(":memory:")


def _commit(a, ops, tt):
    a.begin()
    try:
        a.apply_ops(ops, tt)
    except Exception:
        a.rollback()
        raise
    a.commit()


def _node(uid, label="N", vt_s=0, vt_e=OPEN_END):
    return {"op": "assert_node", "uid": uid, "label": label, "props": {"name": uid}, "vt_s": vt_s,
            "vt_e": vt_e}


def _edge(s, d, rt="R", vt_s=0, vt_e=OPEN_END, disc="0", w=None):
    return {"op": "assert_edge", "src": s, "dst": d, "rel_type": rt,
            "props": {} if w is None else {"w": w}, "vt_s": vt_s, "vt_e": vt_e, "disc": disc}


def _retract(src, dst, rel, disc, t):
    return {"op": "retract", "ref": {"kind": "edge", "src": src, "dst": dst, "rel_type": rel,
                                     "disc": disc}, "t": t}


def _hubby(make, seed: int):
    """A hub `h`, its neighbours and a second ring, edges sharing a `vt_s`,
    float props, and a later belief that closes some hub edges."""
    rng = random.Random(seed)
    a = make()
    nodes = ["h"] + [f"n{i}" for i in range(40)]
    _commit(a, [_node(u, rng.choice(["A", "B"]), 0 if u != "n7" else 50) for u in nodes], 1)
    ops = []
    for i in range(30):
        ops.append(_edge("h", f"n{i}", rng.choice(["R", "S"]), 10 * rng.randrange(0, 3),
                         disc=str(i), w=rng.random()))
    for i in range(60):
        u, v = rng.sample(nodes[1:], 2)
        ops.append(_edge(u, v, "R", rng.randrange(0, 30), disc=str(100 + i), w=rng.random() * 3))
    _commit(a, ops, 2)
    _commit(a, [_edge("h", f"n{i}", "R", 0, 25, disc=str(i)) for i in range(0, 30, 5)], 3)
    return a


def _eid(src, dst, rel="R", disc="0"):
    from gnomon.core.model import edge_eid

    return edge_eid(src, dst, rel, disc)


W = {"t_a": 0, "t_b": 40}

#: one or more calls per operator, a refused one, invalid ones, pinned ones
CALLS = [
    ("entity_history", {"uid": "n1", "limit": 50}),
    ("entity_history", {"uid": "h", "include_edges": True, "limit": 5}),
    ("entity_history", {"uid": "n1", "limit": 50, "as_of_tt": 2}),
    ("version_history", {"kind": "edge", "window": W, "limit": 7}),
    ("version_history", {"kind": "edge", "window": W, "limit": 7, "cursor": "7"}),
    ("version_history", {"kind": "edge", "ref": {"eid": _eid("h", "n0", "R", "0")},
                         "belief": "all", "window": {"t_a": 0, "t_b": OPEN_END},
                         "limit": 10000, "cursor": None}),
    ("version_history", {"kind": "node", "ref": {"uid": "h"}, "belief": "superseded",
                         "window": W}),
    ("snapshot_subgraph", {"seeds": ["h"], "t_valid": 10, "hops": 2, "limit": 9}),
    ("snapshot_subgraph", {"seeds": ["n1"], "t_valid": 26, "hops": 1, "limit": 10000,
                           "as_of_tt": 2}),
    ("diff_snapshots", {"t1": 5, "t2": 30, "limit": 5}),
    ("neighborhood_evolution", {"uid": "h", "t1": 0, "t2": 30, "limit": 50}),
    ("neighborhood_evolution", {"uid": "h", "t1": 30, "t2": 0}),
    ("resolve_entities", {"query": "n1", "limit": 20}),
    ("aggregate_events", {"group_by": [{"dim": "time_bucket"}], "aggregates": [{"agg": "count"}],
                          "window": W, "stride": 5, "limit": 50}),
    ("aggregate_events", {"group_by": [{"dim": "endpoint", "role": "src"}],
                          "aggregates": [{"agg": "count"}, {"agg": "mean", "of": "vt_s"}],
                          "window": W, "limit": 50}),
    ("graph_metric_timeseries", {"metric": "edge_event_count", "window": W, "stride": 5,
                                 "limit": 50}),
    ("burst_detection", {"target": {"kind": "edge_event_rate"}, "window": W, "stride": 5,
                         "limit": 50}),
    ("count_temporal_motifs", {"motif": "M_2node_pingpong", "delta": 10, "window": W}),
    ("find_temporal_motif_instances", {"motif": "M_2node_pingpong", "delta": 10, "window": W,
                                       "limit": 20}),
    ("temporal_reachability", {"src": "h", "window": W, "limit": 50}),
    ("temporal_paths", {"src": "h", "dst": "n2", "window": W, "k": 3, "max_hops": 2}),
    ("co_active", {"a_spec": {"src": "h"}, "b_spec": {"src": "n2"},
                   "allen_relation": {"relation": "overlaps"}, "limit": 20}),
    ("compute", {"fn": "count", "input": [{"x": 1}, {"x": 2.5}, {"x": 3}]}),
    ("entity_history", {"uid": "h", "limit": 0}),
    ("snapshot_subgraph", {"seeds": ["h"], "t_valid": 10, "hops": 2, "nope": 1}),
]

KWS = [{}, {"agent": True}, {"skip_cost_check": True},
       {"cost_ceilings": {"rows_scanned_est": 1}}, {"cost_ceilings": {"time_est_ms": 10**9}}]


def _env(a, name, args, monkeypatch, fast, **kw):
    monkeypatch.setenv("GNOMON_CALL_FAST", "1" if fast else "0")
    try:
        out = call_operator(a, name, json.loads(json.dumps(args)), **kw)
        return json.dumps(out)       # the whole envelope, key order included
    except Exception as e:  # noqa: BLE001 - the error is part of the answer
        return (type(e).__name__, str(e), json.dumps(getattr(e, "details", None), default=str))
    finally:
        monkeypatch.setenv("GNOMON_CALL_FAST", "1")


def _check_all(a, monkeypatch, hits, step, kws=KWS, tt_source=None):
    for name, args in CALLS:
        for kw in kws:
            if tt_source is not None:
                kw = {**kw, "tt_source": tt_source}
            old = _env(a, name, args, monkeypatch, False, **kw)
            first = _env(a, name, args, monkeypatch, True, **kw)
            before = hits[0]
            again = _env(a, name, args, monkeypatch, True, **kw)
            assert first == old, (step, name, kw)
            assert again == old, (step, name, kw, "the repeat path")
            if not isinstance(old, tuple):
                assert hits[0] == before + 1, (step, name, kw, "repeat did not take the plan")


@pytest.fixture
def hits(monkeypatch):
    n = [0]
    real = A._call_fast

    def counting(*x):
        out = real(*x)
        if out is not None:
            n[0] += 1
        return out

    monkeypatch.setattr(A, "_call_fast", counting)
    return n


@pytest.mark.parametrize("backend", ["native", "duckdb"])
def test_the_repeat_path_is_the_full_envelope_across_writes(backend, monkeypatch, hits):
    a = _hubby(_native if backend == "native" else _duck, 5)
    frontier = [3]
    steps = [
        ("start", None),
        ("commit", lambda: _commit(a, [_edge("h", "n35", "S", 5, disc="new", w=0.25)], 4)),
        ("correction", lambda: _commit(a, [_edge("h", "n0", "R", 3, 9, disc="0", w=1.5)], 5)),
        ("retract", lambda: _commit(a, [_retract("h", "n35", "S", "new", 12)], 6)),
    ]
    if backend == "native":
        steps += [("compact", lambda: a.compact()), ("settle", lambda: a.settle()),
                  ("publish", lambda: a.publish_pending()), ("gc", lambda: a.gc())]
    for step, act in steps:
        if act is not None:
            act()
        _check_all(a, monkeypatch, hits, step)
        _check_all(a, monkeypatch, hits, step, kws=[{}], tt_source=lambda: frontier[0])
        frontier[0] += 1
    assert hits[0] > 0


def test_admission_is_decided_per_call(monkeypatch, hits):
    a = _hubby(_native, 7)
    args = {"src": "h", "dst": "n2", "window": W, "k": 3, "max_hops": 2}
    call_operator(a, "temporal_paths", dict(args))
    before = hits[0]
    call_operator(a, "temporal_paths", dict(args))
    assert hits[0] == before + 1
    with pytest.raises(CostError):
        call_operator(a, "temporal_paths", dict(args), cost_ceilings={"rows_scanned_est": 0})
    monkeypatch.setenv("GNOMON_TIME_COEFF_SCALE", "1e12")
    for fast in ("1", "0"):
        monkeypatch.setenv("GNOMON_CALL_FAST", fast)
        with pytest.raises(CostError):
            call_operator(a, "temporal_paths", dict(args))
    monkeypatch.delenv("GNOMON_TIME_COEFF_SCALE")
    monkeypatch.setenv("GNOMON_CALL_FAST", "1")
    calls = []
    spec = REGISTRY["temporal_paths"]
    real = spec.cost_fn
    monkeypatch.setattr(spec, "cost_fn", lambda f, s: calls.append(1) or real(f, s))
    call_operator(a, "temporal_paths", dict(args))
    assert calls, "the repeat path skipped the admission estimate"


def test_the_repeat_path_hands_out_copies(monkeypatch, hits):
    a = _hubby(_native, 6)
    args = {"uid": "h", "limit": 5, "include_edges": True}
    call_operator(a, "entity_history", dict(args))
    x = call_operator(a, "entity_history", dict(args))
    assert hits[0] >= 1
    before = hits[0]
    ref = json.dumps(x)
    x["dependency"]["terms"].clear()
    x["plan_ir"].clear()
    x["args_echo"]["uid"] = "zz"
    x["tt_q"] = -1
    y = call_operator(a, "entity_history", dict(args))
    assert json.dumps(y) == ref and hits[0] == before + 1


@pytest.mark.parametrize("env", [("GNOMON_CALL_FAST", "0"), ("GNOMON_CALL_MEMO", "0"),
                                 ("GNOMON_PLAN_PATH", "off")])
def test_the_repeat_path_steps_aside(env, monkeypatch, hits):
    a = _hubby(_native, 8)
    args = {"uid": "n1", "limit": 50}
    call_operator(a, "entity_history", dict(args))
    before = hits[0]
    monkeypatch.setenv(*env)
    call_operator(a, "entity_history", dict(args))
    assert hits[0] == before


def test_a_kernel_change_reaches_the_repeat_path(monkeypatch, hits):
    a = _hubby(_native, 9)
    args = {"uid": "n1", "limit": 50}
    call_operator(a, "entity_history", dict(args))
    before = hits[0]
    spec = REGISTRY["entity_history"]
    monkeypatch.setattr(spec, "fn", lambda ad, ar: {"rows": [], "rows_total": 0,
                                                    "truncated": False, "cursor": None})
    out = call_operator(a, "entity_history", dict(args))
    assert hits[0] == before + 1 and out["rows"] == []


# --------------------------------------------------------------------------- #
# --------------------------------------------------------------------------- #

CHARS = ["a", "Z", "\"", "\\", "\n", "\t", "\x00", "\x1f", "é", "€", "😀", " ", "{"]


def _value(rng, depth=0):
    import enum

    import numpy as np

    class S(str):
        pass

    class D(dict):
        pass

    class L(list):
        pass

    class Num(enum.IntEnum):
        one = 1

    leaves = [lambda: None, lambda: True, lambda: False, lambda: 1, lambda: 0, lambda: 1.0, lambda: 0.0,
              lambda: -0.0, lambda: rng.randrange(-10, 10), lambda: rng.randrange(-2 ** 80, 2 ** 80),
              lambda: 2 ** 63, lambda: -2 ** 63, lambda: 2 ** 63 - 1,
              lambda: rng.random() * 10 ** rng.randrange(-20, 20), lambda: 0.1 + 0.2, lambda: 1e16,
              lambda: float(rng.randrange(10 ** 6)), lambda: float("inf"), lambda: -float("inf"),
              lambda: "".join(rng.choice(CHARS) for _ in range(rng.randrange(0, 5))), lambda: "1",
              lambda: S("a"), lambda: np.int64(3), lambda: np.float64(0.5), lambda: Num.one,
              lambda: "\ud800"]
    if depth > 3 or rng.random() < 0.4:
        return rng.choice(leaves)()
    kids = [_value(rng, depth + 1) for _ in range(rng.randrange(0, 4))]
    kind = rng.randrange(7)
    if kind == 0:
        return {rng.choice(["a", "b", "1", "é"]) + str(i): v for i, v in enumerate(kids)}
    if kind == 1:
        return {str(i): v for i, v in reversed(list(enumerate(kids)))}
    if kind == 2:
        return list(kids)
    if kind == 3:
        return tuple(kids)
    if kind == 4:
        return D({f"k{i}": v for i, v in enumerate(kids)})
    if kind == 5:
        return {1: kids[0]} if kids else {}
    return L(kids)


def test_engine_args_key_is_args_key():
    from gnomon import _engine as E
    from gnomon.temporal.algebra import args_key

    rng = random.Random(21)
    vals = [_value(rng) for _ in range(4000)]
    vals += [float("nan"), {"a": float("nan")}, [1, True], [1, 1.0], [True], {"a": 1, "b": 2},
             {"b": 2, "a": 1}, [0.0], [-0.0], [2 ** 64], [-(2 ** 64)], ["a"], [b"a"]]
    keyed = []
    for v in vals:
        k, b = args_key(v), E.args_key_bytes(v)
        if k is None:
            assert b is None, v
        elif b is None:
            assert "\ud800" in repr(v) or "\\ud800" in repr(v), v   # only a str UTF-8 cannot carry
        else:
            keyed.append((k, b))
    assert len(keyed) > 1000
    by_key, by_bytes = {}, {}
    for k, b in keyed:
        assert by_key.setdefault(k, b) == b
        assert by_bytes.setdefault(b, k) == k


def test_one_pass_canonical_payload_is_the_two_walks(monkeypatch):
    from gnomon import _engine as E
    from gnomon.core.errors import InvalidArgError
    from gnomon.temporal import algebra as AL

    def same(x, y):
        if type(x) is not type(y):
            return False
        if isinstance(x, dict):
            return list(x) == list(y) and all(same(x[k], y[k]) for k in x)
        if isinstance(x, (list, tuple)):
            return len(x) == len(y) and all(same(a, b) for a, b in zip(x, y))
        if isinstance(x, float):
            return repr(x) == repr(y)
        return x == y

    def two(obj):
        monkeypatch.setenv("GNOMON_CANON_ONEPASS", "0")
        try:
            return AL._canonical_payload(obj)
        except Exception as e:  # noqa: BLE001 - the error is part of the answer
            return (type(e).__name__, str(e))
        finally:
            monkeypatch.setenv("GNOMON_CANON_ONEPASS", "1")

    rng = random.Random(22)
    took = 0
    for i in range(6000):
        obj = {"rows": [_value(rng) for _ in range(rng.randrange(0, 4))], "x": _value(rng),
               "truncated": False} if i % 2 else _value(rng)
        got = E.canonicalize_json(obj, round)
        ref = two(obj)
        try:
            out = AL._canonical_payload(obj)
        except Exception as e:  # noqa: BLE001
            out = (type(e).__name__, str(e))
        if isinstance(ref, tuple) and isinstance(ref[0], str) and len(ref) == 2 and ref[0].endswith("Error"):
            assert out == ref, obj
            continue
        assert same(out[0], ref[0]) and out[1] == ref[1], obj
        if got is not None:
            took += 1
            assert same(got[0], ref[0]), obj
            if type(obj) in (dict, list):
                assert got[0] is not obj
    assert took > 1000
    shared = {"a": 1}
    out, _ = AL._canonical_payload({"x": shared, "y": [shared, 2.5]})
    assert out["x"] is not shared and out["x"] == shared
    for bad in (float("nan"), [1.0, float("inf")], {"a": {"b": -float("inf")}}):
        with pytest.raises(InvalidArgError, match="non-finite"):
            AL._canonical_payload(bad)
    monkeypatch.setenv("GNOMON_CANON_ONEPASS", "0")
    monkeypatch.setitem(AL._ENGINE_FNS, "canonicalize_json", lambda *a: pytest.fail("one pass under the switch"))
    AL._canonical_payload({"a": [1.5]})


@pytest.mark.parametrize("env", [("GNOMON_ARGS_KEY_RUST", "0"), ("GNOMON_CANON_ONEPASS", "0"),
                                 ("GNOMON_CANON_RUST", "0"), ("GNOMON_DIGEST_RUST", "0")])
def test_the_engine_helpers_switch_off_to_the_same_envelopes(env, monkeypatch):
    a = _hubby(_native, 11)
    for name, args in CALLS[:12]:
        x = _env(a, name, args, monkeypatch, True)
        x = _env(a, name, args, monkeypatch, True)
        monkeypatch.setenv(*env)
        y = _env(a, name, args, monkeypatch, True)
        y = _env(a, name, args, monkeypatch, True)
        z = _env(a, name, args, monkeypatch, False)
        monkeypatch.delenv(env[0])
        assert x == y == z, (env, name)


# --------------------------------------------------------------------------- #
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("seed", [1, 2, 3, 4])
def test_one_call_evolution_is_the_numpy_form(seed, monkeypatch):
    a = _hubby(_native, seed)
    _commit(a, [_edge("h", "h", "R", 0, disc="loop"), _edge("n3", "h", "S", 12, 27, disc="in"),
                _edge("h", "n38", "R", 25, 26, disc="blip")], 4)
    _commit(a, [_retract("h", "n38", "R", "blip", 25)], 5)
    calls = []
    real = a.neighborhood_evolution_parts
    monkeypatch.setattr(a, "neighborhood_evolution_parts", lambda *x: calls.append(1) or real(*x),
                        raising=False)
    n = 0
    for uid in ("h", "n3", "n7", "n39", "nope"):
        for t1, t2 in ((0, 10), (0, 25), (10, 11), (24, 25), (25, 30), (0, 1000), (9, 20), (26, 27)):
            for as_of in (None, 2, 3, 4):
                for stride in (None, 1, 3, 7, 10 ** 6):
                    args = {"uid": uid, "t1": t1, "t2": t2, "limit": 1 + n % 3}
                    if as_of is not None:
                        args["as_of_tt"] = as_of
                    if stride is not None:
                        args["stride"] = stride
                    n += 1
                    monkeypatch.setenv("GNOMON_NBR_EVOLUTION_KERNEL", "1")
                    calls.clear()
                    x = _env(a, "neighborhood_evolution", args, monkeypatch, False)
                    assert len(calls) == (0 if uid == "nope" else 1), (uid, t1, t2, as_of, stride)
                    monkeypatch.setenv("GNOMON_NBR_EVOLUTION_KERNEL", "0")
                    calls.clear()
                    y = _env(a, "neighborhood_evolution", args, monkeypatch, False)
                    assert not calls
                    assert x == y, (uid, t1, t2, as_of, stride)
    monkeypatch.delenv("GNOMON_NBR_EVOLUTION_KERNEL")


# --------------------------------------------------------------------------- #
# --------------------------------------------------------------------------- #

def test_kept_admission_follows_stats_ceilings_and_scale(monkeypatch, hits):
    from gnomon.temporal import guardrails as G

    a = _hubby(_native, 12)
    priced = []
    real = G.add_time_estimate
    monkeypatch.setattr(G, "add_time_estimate", lambda op, e: priced.append(op) or real(op, e))
    args = {"kind": "edge", "window": W, "limit": 7}
    n = a.stats()["n_edge_versions"]
    ceil = {"rows_scanned_est": n}

    def both(**kw):
        out = []
        for memo in ("1", "0"):
            monkeypatch.setenv("GNOMON_ADMIT_MEMO", memo)
            out.append(_env(a, "version_history", args, monkeypatch, True, **kw))
        monkeypatch.delenv("GNOMON_ADMIT_MEMO")
        assert out[0] == out[1], kw
        return out[0]

    for _ in range(2):
        call_operator(a, "version_history", dict(args), cost_ceilings=ceil)
    priced.clear()
    call_operator(a, "version_history", dict(args), cost_ceilings=ceil)
    assert not priced, "a kept admission was priced again"
    assert not isinstance(both(cost_ceilings=ceil), tuple)
    # a write moves the stats past the ceiling: refused on both paths
    _commit(a, [_edge("n1", "n2", "R", 3, disc="more")], 4)
    assert both(cost_ceilings=ceil)[0] == "CostError"
    # a lower ceiling, and a scale that prices past the default time ceiling
    assert both(cost_ceilings={"rows_scanned_est": 1})[0] == "CostError"
    ev = {"uid": "h", "t1": 0, "t2": 30}
    for _ in range(2):
        call_operator(a, "neighborhood_evolution", dict(ev))
    monkeypatch.setenv("GNOMON_TIME_COEFF_SCALE", "1e9")
    for memo in ("1", "0"):
        monkeypatch.setenv("GNOMON_ADMIT_MEMO", memo)
        with pytest.raises(CostError):
            call_operator(a, "neighborhood_evolution", dict(ev))
    monkeypatch.delenv("GNOMON_TIME_COEFF_SCALE")
    monkeypatch.setenv("GNOMON_ADMIT_MEMO", "0")
    priced.clear()
    call_operator(a, "neighborhood_evolution", dict(ev))
    assert priced, "the switch did not price the call"
    monkeypatch.delenv("GNOMON_ADMIT_MEMO")
    # a refusal is never kept: raising the ceiling admits the call again
    n2 = a.stats()["n_edge_versions"]
    assert not isinstance(both(cost_ceilings={"rows_scanned_est": n2}), tuple)
    assert both(cost_ceilings={"rows_scanned_est": n2 - 1})[0] == "CostError"
    # the policy constants the decision reads are in the key
    for _ in range(2):
        call_operator(a, "neighborhood_evolution", dict(ev))
    monkeypatch.setitem(G.DEFAULT_CEILINGS, "time_est_ms", 1)
    for memo in ("1", "0"):
        monkeypatch.setenv("GNOMON_ADMIT_MEMO", memo)
        with pytest.raises(CostError):
            call_operator(a, "neighborhood_evolution", dict(ev))
    monkeypatch.undo()
    for _ in range(2):
        call_operator(a, "neighborhood_evolution", dict(ev))
    monkeypatch.setattr(G, "TOUCHING_READ_MS", 1e9)
    with pytest.raises(CostError):
        call_operator(a, "neighborhood_evolution", dict(ev))
    from gnomon.temporal import ops_versions as OV

    for _ in range(2):
        call_operator(a, "version_history", dict(args))
    monkeypatch.setattr(OV, "VERSION_PAGE_CALL_MS", 1e9)
    with pytest.raises(CostError):
        call_operator(a, "version_history", dict(args))
