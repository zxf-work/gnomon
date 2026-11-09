"""it replaces, which stays behind its switch.

- `snapshot_subgraph`'s engine frontier returns the page only (GNOMON_SNAPSHOT_PAGE)
- `neighborhood_evolution` reads the incident edges once (GNOMON_NBR_EVOLUTION_ONE_READ)
"""

from __future__ import annotations

import random
import tempfile

import pytest

from gnomon.core.model import OPEN_END
from gnomon.temporal import ops_snapshot as OS
from gnomon.temporal.algebra import call_operator, ensure_all_registered

ensure_all_registered()


def _native():
    from gnomon.storage.native import NativeAdapter

    tmp = tempfile.TemporaryDirectory(prefix="gnomon-test-c4fix48-")
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


def _node(uid, label="N", vt_s=0, vt_e=OPEN_END):
    return {"op": "assert_node", "uid": uid, "label": label, "props": {}, "vt_s": vt_s, "vt_e": vt_e}


def _edge(s, d, rt="R", vt_s=0, vt_e=OPEN_END, disc="0"):
    return {"op": "assert_edge", "src": s, "dst": d, "rel_type": rt, "props": {},
            "vt_s": vt_s, "vt_e": vt_e, "disc": disc}


def _hubby(seed: int):
    """A hub `h` and its neighbours, a second ring around them, and many
    edges sharing one `vt_s` (runs of equal keys for a page to cut), some
    later versions and some retracted, so the instant and belief both bite."""
    rng = random.Random(seed)
    a = _native()
    nodes = ["h"] + [f"n{i}" for i in range(60)]
    _commit(a, [_node(u, rng.choice(["A", "B"]), 0 if u != "n7" else 50) for u in nodes], 1)
    ops = []
    for i in range(40):
        ops.append(_edge("h", f"n{i}", rng.choice(["R", "S"]), 10 * rng.randrange(0, 3), disc=str(i)))
    for i in range(80):
        u, v = rng.sample(nodes[1:], 2)
        ops.append(_edge(u, v, "R", 10 * rng.randrange(0, 3), disc=str(100 + i)))
    ops.append(_edge("h", "h", "R", 0, disc="loop"))
    _commit(a, ops, 2)
    ops = []
    for i in range(0, 40, 5):     # a later belief closes some hub edges at 25
        ops.append(_edge("h", f"n{i}", "R", 0, 25, disc=str(i)))
    _commit(a, ops, 3)
    return a


def _call(a, op, args, monkeypatch, env, on):
    monkeypatch.setenv(env, "1" if on else "0")
    try:
        return call_operator(a, op, args)
    except Exception as e:  # noqa: BLE001 - the error is part of the answer
        return (type(e).__name__, str(e))


@pytest.mark.parametrize("seed", [1, 2, 3])
def test_the_snapshot_page_is_the_full_paths_slice(seed, monkeypatch):
    a = _hubby(seed)
    # every reached set takes the engine frontier (the hub path)
    monkeypatch.setattr(OS, "SEED_EXPANSION_MAX_IDS", 1)
    called = []
    real = a.snapshot_kernel_page
    monkeypatch.setattr(a, "snapshot_kernel_page", lambda *x: called.append(1) or real(*x), raising=False)
    cut_in_run = 0
    for seeds in (["h"], ["n3", "n9"], ["n7"]):
        for t in (0, 10, 24, 25, 30):
            for as_of in (None, 2):
                base = {"seeds": seeds, "hops": 2, "t_valid": t}
                if as_of is not None:
                    base["as_of_tt"] = as_of
                full = _call(a, "snapshot_subgraph", {**base, "limit": 10000}, monkeypatch, "GNOMON_SNAPSHOT_PAGE", False)
                rows = full["rows"]
                for lim in (1, 2, 3, 5, 10000):
                    for cur in (None, "0", "1", "4", " 2", "007", str(len(rows)), str(len(rows) + 3), "-2", "x"):
                        args = {**base, "limit": lim, "cursor": cur}
                        x = _call(a, "snapshot_subgraph", args, monkeypatch, "GNOMON_SNAPSHOT_PAGE", True)
                        y = _call(a, "snapshot_subgraph", args, monkeypatch, "GNOMON_SNAPSHOT_PAGE", False)
                        assert x == y, (seeds, t, as_of, lim, cur)
                        off = int(cur) if cur and cur.strip().lstrip("-").isdigit() else 0
                        if 0 < off < len(rows) and rows[off]["vt_s"] == rows[off - 1]["vt_s"]:
                            cut_in_run += 1
    assert called, "the page kernel never ran"
    assert cut_in_run, "no page boundary fell inside a run of equal vt_s"


def test_the_snapshot_page_switch_keeps_the_full_path(monkeypatch):
    a = _hubby(4)
    monkeypatch.setattr(OS, "SEED_EXPANSION_MAX_IDS", 1)
    monkeypatch.setenv("GNOMON_SNAPSHOT_PAGE", "0")
    monkeypatch.setattr(a, "snapshot_kernel_page", lambda *x: pytest.fail("page kernel under the switch"),
                        raising=False)
    call_operator(a, "snapshot_subgraph", {"seeds": ["h"], "hops": 2, "t_valid": 10, "limit": 3})


@pytest.mark.parametrize("seed", [1, 2, 3, 4])
def test_one_read_evolution_answers_as_three_reads(seed, monkeypatch):
    # tests/test_envelope_fixes.py compares that call with this form)
    monkeypatch.setenv("GNOMON_NBR_EVOLUTION_KERNEL", "0")
    a = _hubby(seed)
    calls = []
    real = a.edges_columnar
    monkeypatch.setattr(a, "edges_columnar", lambda **k: calls.append(1) or real(**k), raising=False)
    for uid in ("h", "n3", "n7", "n59"):
        for t1, t2 in ((0, 10), (0, 25), (10, 11), (24, 25), (25, 30), (0, 1000), (9, 20)):
            for as_of in (None, 2):
                args = {"uid": uid, "t1": t1, "t2": t2}
                if as_of is not None:
                    args["as_of_tt"] = as_of
                for lim in (1, 100):
                    args["limit"] = lim
                    calls.clear()
                    x = _call(a, "neighborhood_evolution", args, monkeypatch, "GNOMON_NBR_EVOLUTION_ONE_READ", True)
                    assert len(calls) == 1
                    calls.clear()
                    y = _call(a, "neighborhood_evolution", args, monkeypatch, "GNOMON_NBR_EVOLUTION_ONE_READ", False)
                    assert len(calls) == 3
                    assert x == y, (uid, t1, t2, as_of, lim)


# --------------------------------------------------------------------------- #
# --------------------------------------------------------------------------- #

def _eid(a, src, dst, rel="R", disc="0"):
    from gnomon.core.model import edge_eid

    return edge_eid(src, dst, rel, disc)


def _retract(src, dst, rel, disc, t):
    return {"op": "retract", "ref": {"kind": "edge", "src": src, "dst": dst, "rel_type": rel, "disc": disc}, "t": t}


def _calls(a, eid):
    return [
        ("version_history", {"kind": "edge", "ref": {"eid": eid}, "belief": "all",
                             "window": {"t_a": 0, "t_b": OPEN_END}, "limit": 10000}),
        ("version_history", {"kind": "edge", "ref": {"eid": eid}, "belief": "current",
                             "window": {"t_a": 0, "t_b": OPEN_END}, "limit": 2, "as_of_tt": 3}),
        ("version_history", {"kind": "node", "ref": {"uid": "h"}, "belief": "superseded",
                             "window": {"t_a": 0, "t_b": 40}}),
        ("entity_history", {"uid": "h"}),
        ("neighborhood_evolution", {"uid": "h", "t1": 0, "t2": 30}),
        ("snapshot_subgraph", {"seeds": ["h"], "hops": 1, "t_valid": 10}),
    ]


def _both(a, monkeypatch, name, args, **kw):
    monkeypatch.setenv("GNOMON_CALL_MEMO", "1")
    x = call_operator(a, name, args, **kw)
    monkeypatch.setenv("GNOMON_CALL_MEMO", "0")
    y = call_operator(a, name, args, **kw)
    monkeypatch.setenv("GNOMON_CALL_MEMO", "1")
    return x, y


def test_the_envelope_memo_follows_every_write(monkeypatch):
    from gnomon.plan import ttq

    a = _hubby(5)
    eid = _eid(a, "h", "n0")
    computed = []
    real = ttq._envelope_from
    monkeypatch.setattr(ttq, "_envelope_from", lambda *x: computed.append(1) or real(*x))
    frontier = [7]
    steps = [
        ("start", None),
        ("commit", lambda: _commit(a, [_edge("h", "n50", "S", 5, disc="new")], 4)),
        ("correction", lambda: _commit(a, [_edge("h", "n0", "R", 3, 9, disc="0")], 5)),
        ("retract", lambda: _commit(a, [_retract("h", "n50", "S", "new", 12)], 6)),
        ("compact", lambda: a.compact()),
        ("settle", lambda: a.settle()),
        ("publish", lambda: a.publish_pending()),
        ("gc", lambda: a.gc()),
        ("frontier override", lambda: frontier.__setitem__(0, 3)),
    ]
    prev = None
    for step, act in steps:
        if act is not None:
            act()
        for name, args in _calls(a, eid):
            for kw in ({}, {"tt_source": lambda: frontier[0]}):
                x, y = _both(a, monkeypatch, name, args, **kw)
                assert x == y, (step, name, kw)
                computed.clear()
                z = call_operator(a, name, args, **kw)
                assert z == x and not computed, (step, name, "memo did not answer a repeat call")
        tt = call_operator(a, "version_history", _calls(a, eid)[0][1])["tt_q"]
        if step in ("commit", "correction", "retract"):
            assert tt > prev, (step, "freshness did not move with the write")
        prev = tt


def test_the_envelope_memo_hands_out_copies(monkeypatch):
    a = _hubby(6)
    args = {"uid": "h"}
    x = call_operator(a, "entity_history", args)
    x["dependency"]["terms"].clear()
    x["tt_q"] = -1
    y, z = _both(a, monkeypatch, "entity_history", args)
    assert y == z and y["tt_q"] != -1 and y["dependency"]["terms"]


@pytest.mark.parametrize("seed", [1, 2, 3])
def test_the_identity_log_by_columns_is_the_object_path(seed, monkeypatch):
    a = _hubby(seed)
    _commit(a, [_edge("h", "n0", "R", 3, 9, disc="0")], 4)
    _commit(a, [_edge("h", "n55", "R", 0, disc="x")], 5)
    _commit(a, [_retract("h", "n55", "R", "x", 7)], 6)
    keys = [("edge", {"eid": _eid(a, "h", f"n{i}", r, str(i))}) for i in range(6) for r in ("R", "S")]
    keys += [("edge", {"eid": _eid(a, "h", "n55", "R", "x")})]
    keys += [("node", {"uid": u}) for u in ("h", "n7", "n59", "nope")]
    keys += [("edge", {"eid": "zz" * 12}), ("edge", {"eid": "ab"})]
    for kind, ref in keys:
        for belief in ("all", "current", "superseded"):
            for as_of in (OPEN_END, 2, 3, 4, 5, 6):
                for window in ({"t_a": 0, "t_b": OPEN_END}, {"t_a": 4, "t_b": 9}, {"t_a": 25, "t_b": 26}):
                    for extra in ({}, {"rel_types": ["S"]} if kind == "edge" else {}, {"include_props": True},
                                  {"limit": 1, "cursor": "1"}):
                        args = {"kind": kind, "ref": ref, "belief": belief, "window": window,
                                "as_of_tt": as_of, **extra}
                        x = _call(a, "version_history", args, monkeypatch, "GNOMON_IDENTITY_LOG_COLS", True)
                        y = _call(a, "version_history", args, monkeypatch, "GNOMON_IDENTITY_LOG_COLS", False)
                        assert x == y, (kind, ref, belief, as_of, window, extra)


# --------------------------------------------------------------------------- #
# --------------------------------------------------------------------------- #

def _shape(rng, depth=0):
    import enum

    import numpy as np

    class Sub(dict):
        pass

    class SubL(list):
        pass

    class Num(enum.IntEnum):
        one = 1

    leaves = [lambda: rng.random() * 10 ** rng.randrange(-12, 12), lambda: rng.randrange(-5, 5),
              lambda: rng.choice([True, False, None]), lambda: f"s{rng.randrange(9)}", lambda: 0.1 + 0.2,
              lambda: -0.0, lambda: np.float64(rng.random()), lambda: np.int64(3), lambda: Num.one,
              lambda: 1e-10, lambda: 123456789.123456789]
    if depth > 3 or rng.random() < 0.4:
        return rng.choice(leaves)()
    kind = rng.randrange(5)
    kids = [_shape(rng, depth + 1) for _ in range(rng.randrange(0, 5))]
    if kind == 0:
        return {f"k{i}": v for i, v in enumerate(kids)}
    if kind == 1:
        return list(kids)
    if kind == 2:
        return tuple(kids)
    if kind == 3:
        return Sub({f"k{i}": v for i, v in enumerate(kids)})
    return SubL(kids)


def _same(x, y):
    """Equal values of the same types, all the way down."""
    if type(x) is not type(y):
        return False
    if isinstance(x, dict):
        return list(x) == list(y) and all(_same(x[k], y[k]) for k in x)
    if isinstance(x, (list, tuple)):
        return len(x) == len(y) and all(_same(a, b) for a, b in zip(x, y))
    if isinstance(x, float):
        return repr(x) == repr(y)
    return x == y


def test_engine_canonicalisation_is_the_python_walk(monkeypatch):
    from gnomon.core.errors import InvalidArgError
    from gnomon.temporal import algebra as AL

    rng = random.Random(7)
    for _ in range(3000):
        obj = _shape(rng)
        monkeypatch.setenv("GNOMON_CANON_RUST", "1")
        x = AL._canonicalize_floats(obj)
        assert _same(x, AL._canonicalize_floats_py(obj))
        # the containers are copies, as the Python walk makes them
        if type(obj) in (dict, list):
            assert x is not obj
    shared = {"a": 1}
    out = AL._canonicalize_floats({"x": shared, "y": [shared, 2.5]})
    assert out["x"] is not shared and out["x"] == shared
    for bad in (float("nan"), [1.0, float("inf")], {"a": {"b": -float("inf")}}):
        with pytest.raises(InvalidArgError, match="non-finite"):
            AL._canonicalize_floats(bad)
    monkeypatch.setenv("GNOMON_CANON_RUST", "0")
    monkeypatch.setattr(AL, "_CANON_FAST", [lambda *a: pytest.fail("engine walk under the switch")])
    AL._canonicalize_floats({"a": [1.5]})


@pytest.mark.parametrize("seed", [1, 2, 3])
def test_batched_node_validity_is_the_per_id_loop(seed, monkeypatch):
    a = _hubby(seed)
    _commit(a, [_node("n3", "C", 5, 20), _node("n4", "C", 0, 5)], 4)   # later versions, labels change
    ids = [int(i) for i in a.dense_ids(["h"] + [f"n{i}" for i in range(60)])]
    for t in (0, 4, 5, 10, 19, 20, 49, 50, 10 ** 9):
        for as_of in (OPEN_END, 2, 3, 4):
            monkeypatch.setenv("GNOMON_NODES_VALID_BATCH", "1")
            x = a.nodes_valid_at(ids, t, as_of)
            monkeypatch.setenv("GNOMON_NODES_VALID_BATCH", "0")
            y = a.nodes_valid_at(ids, t, as_of)
            assert x == y and list(x) == list(y), (t, as_of)


# --------------------------------------------------------------------------- #
# --------------------------------------------------------------------------- #

def _jsonish(rng, depth=0):
    chars = ["a", "Z", "\"", "\\", "\n", "\r", "\t", "\b", "\f", "\x00", "\x1f", "\x7f", "é", "€", " ",
             "😀", "/", " ", "<", "&", "'", "{", "\u0019"]
    leaves = [lambda: None, lambda: True, lambda: False, lambda: rng.randrange(-10, 10),
              lambda: rng.randrange(-2 ** 80, 2 ** 80), lambda: rng.random() * 10 ** rng.randrange(-20, 20),
              lambda: -0.0, lambda: 1e16, lambda: 1e-7, lambda: 0.1, lambda: float(rng.randrange(10 ** 6)),
              lambda: "".join(rng.choice(chars) for _ in range(rng.randrange(0, 6)))]
    if depth > 3 or rng.random() < 0.35:
        return rng.choice(leaves)()
    kids = [_jsonish(rng, depth + 1) for _ in range(rng.randrange(0, 5))]
    k = rng.randrange(3)
    if k == 0:
        return {"".join(rng.choice(chars) for _ in range(rng.randrange(0, 4))) + str(i): v for i, v in enumerate(kids)}
    return list(kids) if k == 1 else tuple(kids)


def test_engine_canonical_json_is_canonical_json():
    import enum

    from gnomon import _engine as E
    from gnomon.core.model import canonical_json

    rng = random.Random(11)
    for _ in range(5000):
        obj = _jsonish(rng)
        assert E.canonical_json_bytes(obj) == canonical_json(obj).encode("utf-8"), obj

    class Num(enum.IntEnum):
        one = 1

    class D(dict):
        pass

    for odd in ({1: 2}, {"a": float("nan")}, [float("inf")], D(a=1), [Num.one], {"a": "\ud800"}, {None: 1},
                [1.5, object()], (1, {2: 3})):
        assert E.canonical_json_bytes(odd) is None, odd


def test_payload_digest_takes_digest_where_the_engine_declines(monkeypatch):
    from gnomon.core.model import digest
    from gnomon.temporal import algebra as AL

    rng = random.Random(12)
    for _ in range(500):
        obj = {"rows": [_jsonish(rng) for _ in range(3)], "x": _jsonish(rng)}
        assert AL._payload_digest(obj) == digest(obj)
    assert AL._payload_digest({"a": float("nan")}) == digest({"a": float("nan")})
    monkeypatch.setenv("GNOMON_DIGEST_RUST", "0")
    monkeypatch.setitem(AL._ENGINE_FNS, "canonical_json_bytes", lambda *a: pytest.fail("engine under the switch"))
    AL._payload_digest({"a": 1})


def test_engine_json_copy_is_the_python_copy(monkeypatch):
    from gnomon.temporal import algebra as AL

    rng = random.Random(13)
    for _ in range(3000):
        obj = _shape(rng)
        x, y = AL.json_copy(obj), AL._json_copy_py(obj)
        assert _same(x, y)
        if type(obj) in (dict, list, tuple) and obj:
            assert (x is not obj) or type(obj) is tuple and not obj
    shared = {"a": [1]}
    c = AL.json_copy({"x": shared, "y": (shared,)})
    assert c["x"] is not shared and c["y"][0] is not shared and c["x"]["a"] is not shared["a"]
    monkeypatch.setenv("GNOMON_JSON_COPY_RUST", "0")
    monkeypatch.setitem(AL._ENGINE_FNS, "json_copy", lambda *a: pytest.fail("engine under the switch"))
    AL.json_copy({"a": 1})


# --------------------------------------------------------------------------- #
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("seed", [1, 2, 3])
@pytest.mark.parametrize("cap", [1024, 12, 3])
def test_one_call_expansion_is_the_python_expansion(seed, cap, monkeypatch):
    a = _hubby(seed)
    _commit(a, [_node("n3", "C", 5, 20), _edge("n40", "n41", "S", 0, disc="z")], 4)
    monkeypatch.setattr(OS, "SEED_EXPANSION_MAX_IDS", cap)
    called = []
    real = a.snapshot_scan_page
    monkeypatch.setattr(a, "snapshot_scan_page", lambda *x: called.append(1) or real(*x), raising=False)
    for seeds in (["h"], ["n3", "n9", "n3"], ["n7"], ["n40"], ["n12", "n13"]):
        for t in (0, 10, 24, 25, 30, 60):
            for as_of in (None, 2, 3):
                for hops in (0, 1, 2, 3):
                    base = {"seeds": seeds, "hops": hops, "t_valid": t}
                    if as_of is not None:
                        base["as_of_tt"] = as_of
                    for lim in (1, 3, 10000):
                        for cur in (None, "2", "007", "-1", "x", "999"):
                            args = {**base, "limit": lim, "cursor": cur}
                            x = _call(a, "snapshot_subgraph", args, monkeypatch, "GNOMON_SNAPSHOT_ONECALL", True)
                            y = _call(a, "snapshot_subgraph", args, monkeypatch, "GNOMON_SNAPSHOT_ONECALL", False)
                            assert x == y, (seeds, t, as_of, hops, lim, cur, cap)
    assert called
