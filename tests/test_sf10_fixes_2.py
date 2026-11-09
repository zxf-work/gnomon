"""
- `neighbors_batch` splits a frontier over the pool by work, from 64
  vertices in small chunks the threads claim (GNOMON_NEIGHBORS_SPLIT=keys:
  from 4,096 vertices, one chunk a thread)
- each vertex's slot in a sparse adjacency direction is searched forward
  from the previous vertex's (GNOMON_NEIGHBORS_SEEK=bisect: over the whole
  active list)

Both switches are read per call, so one read-only handle answers under each.
"""

from __future__ import annotations

import random
import tempfile

import numpy as np
import pytest

from gnomon.core.model import OPEN_END

pytest.importorskip("gnomon._engine", reason="native engine extension not built")

N = 3_000
SWITCHES = ("GNOMON_NEIGHBORS_SPLIT", "GNOMON_NEIGHBORS_SEEK")


def _native():
    from gnomon.storage.native import NativeAdapter

    tmp = tempfile.TemporaryDirectory(prefix="gnomon-test-c4fix50-")
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


def _edge(s, d, rt, vt_s, disc):
    return {"op": "assert_edge", "src": s, "dst": d, "rel_type": rt, "props": {},
            "vt_s": vt_s, "vt_e": OPEN_END, "disc": disc}


def _store():
    """Several commits (several edge segments), hubs and leaves, two
    relationship types, a self-loop and edge closes at a later transaction
    time."""
    rng = random.Random(46)
    a = _native()
    _commit(a, [{"op": "assert_node", "uid": f"u{i}", "label": "N", "props": {}, "vt_s": 0, "vt_e": OPEN_END}
                for i in range(N)], 1)
    hubs = [f"u{i}" for i in range(0, N, 97)]
    k = 0
    made = []
    for part in range(4):
        ops = []
        for i in range(part, N, 4):
            for _ in range(rng.randrange(0, 3)):
                ops.append(_edge(f"u{i}", f"u{rng.randrange(N)}", "K" if rng.random() < 0.7 else "L",
                                 rng.randrange(0, 50), f"{k}"))
                k += 1
        for h in hubs:
            for _ in range(rng.randrange(5, 40)):
                ops.append(_edge(f"u{rng.randrange(N)}", h, "K", rng.randrange(0, 50), f"{k}"))
                k += 1
        if part == 0:
            ops.append(_edge("u5", "u5", "K", 3, "loop"))
        made += ops
        _commit(a, ops, 2 + part)
    closes = [{"op": "retract", "ref": {"kind": "edge", "src": e["src"], "dst": e["dst"], "rel_type": e["rel_type"],
                                        "disc": e["disc"]}, "t": 60} for e in rng.sample(made, len(made) // 9)]
    _commit(a, closes, 10)
    return a


def _batches(ad, us, as_of):
    out = []
    for direction in ("out", "in", "both"):
        for rels in (None, ["K"], ["L", "K"]):
            for vt in ((None, None), (10, 30)):
                d = ad._store.neighbors_batch(us, direction, int(as_of), vt[0], vt[1], rels)
                out.append({k: list(v) for k, v in d.items()})
    return out


def test_the_split_and_the_forward_seek_keep_the_rows(monkeypatch):
    for k in SWITCHES:
        monkeypatch.delenv(k, raising=False)
    ad = _store()
    rng = random.Random(7)
    ids = np.array(sorted(ad.dense_ids([f"u{i}" for i in range(N)])), dtype=np.int64)
    frontiers = [np.ascontiguousarray(f, dtype=np.int64) for f in
                 (ids[:1], ids[:63], ids[:64], ids[::7], ids, ids[::-1],
                  np.array(rng.sample(list(ids), 900), dtype=np.int64),
                  np.concatenate([ids[:200], ids[100:300]]))]
    for as_of in (OPEN_END, 3, 6):
        want = None
        for split in (None, "keys", "eager"):
            for seek in (None, "bisect"):
                for name, val in zip(SWITCHES, (split, seek)):
                    if val is None:
                        monkeypatch.delenv(name, raising=False)
                    else:
                        monkeypatch.setenv(name, val)
                got = [_batches(ad, f, as_of) for f in frontiers]
                if want is None:
                    want = got
                    assert any(len(v) > 100 for g in got for b in g for v in b.values())
                assert got == want, (split, seek, as_of)


# ---- through `match` ---------------------------------------------------------

from gnomon.core.model import canonical_json  # noqa: E402
from gnomon.temporal.algebra import call_operator, ensure_all_registered  # noqa: E402

from .conftest import ENVELOPE_META_KEYS  # noqa: E402
from .test_sf10_fixes_1 import SCOPES, _friends_posts, _ic9_like  # noqa: E402

ensure_all_registered()

#: switch -> (new path value, old path value)
VALUES = {
    "GNOMON_NEIGHBORS_SPLIT": ("work", "keys"),
    "GNOMON_NEIGHBORS_SEEK": ("forward", "bisect"),
    "GNOMON_MATCH_HOP_MEMO": ("1", "0"),
    "GNOMON_MATCH_SCAN_VT": ("1", "0"),
    "GNOMON_MATCH_KEYCSR_WIDE": ("1", "0"),
    "GNOMON_MATCH_CAST_DISTINCT": ("1", "0"),
    "GNOMON_MATCH_TYPE_PASS": ("par", "serial"),
}


def _answer(a, args):
    env = call_operator(a, "match", dict(args))
    return canonical_json({k: v for k, v in env.items() if k not in ENVELOPE_META_KEYS})


def _same_either_way(a, args, switch, monkeypatch):
    got = {}
    for v in VALUES[switch]:
        monkeypatch.setenv(switch, v)
        got[v] = _answer(a, args)
    assert len(set(got.values())) == 1, (switch, args)
    return got[VALUES[switch][0]]


def _twice(scope):
    """IC10's read: one hop (friends -> their posts) under two sub-patterns,
    here two posts of the same friend, counted per friend."""
    return {"pattern": {
        "nodes": [{"var": "p", "labels": ["Person"], "uids": ["p"]}, {"var": "f", "labels": ["Person"]},
                  {"var": "m", "labels": ["Post"]}, {"var": "m2", "labels": ["Post"]}],
        "steps": [{"from": "p", "to": "f", "rel_type": "KNOWS", "dir": "both"},
                  {"from": "m", "to": "f", "rel_type": "HAS_CREATOR", "dir": "out"},
                  {"from": "m2", "to": "f", "rel_type": "HAS_CREATOR", "dir": "out"}]},
        "return": {"group": {"by": [{"as": "f", "expr": {"col": "f.uid"}}],
                             "aggs": [{"as": "n", "fn": "count"}]},
                   "order_by": [{"expr": {"col": "n"}, "dir": "desc"}, {"expr": {"col": "f"}, "dir": "asc"}],
                   "limit": 50},
        "scope": scope}


def _dated(where, scope):
    """BI1's scan: every post, its creation date filtered, grouped."""
    return {"pattern": {"nodes": [{"var": "m", "labels": ["Post"]}], "steps": [], "where": where},
            "return": {"group": {"by": [{"as": "d", "expr": {"col": "m.vt_s"}}],
                                 "aggs": [{"as": "n", "fn": "count"}]},
                       "order_by": [{"expr": {"col": "d"}, "dir": "asc"}]},
            "scope": scope}


def _cmp(op, x, flip=False):
    col, lit = {"col": "m.vt_s"}, {"lit": x, "tau": "ts"}
    return {"cmp": op, "l": lit, "r": col} if flip else {"cmp": op, "l": col, "r": lit}


@pytest.mark.parametrize("setting", sorted(SCOPES))
def test_the_index_hop_changes_keep_the_rows_of_a_match(setting, monkeypatch):
    # 1,300 friends: past the 1,024-key probe (the memo's floor and the split)
    a = _friends_posts(5, 1300, 2)
    for switch in ("GNOMON_NEIGHBORS_SPLIT", "GNOMON_NEIGHBORS_SEEK", "GNOMON_MATCH_HOP_MEMO"):
        _same_either_way(a, _twice(SCOPES[setting]), switch, monkeypatch)
        _same_either_way(a, _ic9_like("date", 10_000, SCOPES[setting]), switch, monkeypatch)


@pytest.mark.parametrize("setting", sorted(SCOPES))
def test_a_scan_bounded_by_its_filter_keeps_the_rows(setting, monkeypatch):
    a = _friends_posts(9, 60, 3)
    wheres = [[_cmp("<", 3)], [_cmp("<=", 3)], [_cmp(">", 2)], [_cmp(">=", 2)], [_cmp("=", 4)],
              [_cmp("<", 3, flip=True)], [_cmp(">=", 1), _cmp("<", 4)],
              [{"bool": "and", "l": _cmp(">", 0), "r": _cmp("<=", 4)}],
              [{"bool": "or", "l": _cmp("<", 1), "r": _cmp(">", 4)}], [_cmp("!=", 2)], [_cmp("<", -5)]]
    for where in wheres:
        _same_either_way(a, _dated(where, SCOPES[setting]), "GNOMON_MATCH_SCAN_VT", monkeypatch)


def _triangles(scope):
    """BI11's shape: KNOWS triangles a < b < c."""
    k = lambda a, b: {"from": a, "to": b, "rel_type": "KNOWS", "dir": "both"}
    # (the fixture's person ids are not decimals: BI11's integer casts are IC9's here)
    lt = lambda a, b: {"cmp": "<", "l": {"col": f"{a}.uid"}, "r": {"col": f"{b}.uid"}}
    return {"pattern": {
        "nodes": [{"var": "a", "labels": ["Person"]}, {"var": "b", "labels": ["Person"]}, {"var": "c", "labels": ["Person"]}],
        "steps": [k("a", "b"), k("b", "c"), k("c", "a")], "where": [lt("a", "b"), lt("b", "c")]},
        "return": {"group": {"by": [], "aggs": [{"as": "n", "fn": "count"}]}},
        "scope": scope}


def _posts_by_creator(scope):
    """BIH-R5's join: every post with its creator, counted per creator (a
    join of two large sides on node identities), the label constraint over
    the expansion."""
    return {"pattern": {
        "nodes": [{"var": "p", "labels": ["Person"]}, {"var": "m", "labels": ["Post"]}],
        "steps": [{"from": "m", "to": "p", "rel_type": "HAS_CREATOR", "dir": "out"}]},
        "return": {"group": {"by": [{"as": "id", "expr": {"col": "p.uid"}}], "aggs": [{"as": "n", "fn": "count"}]},
                   "order_by": [{"expr": {"col": "n"}, "dir": "desc"}, {"expr": {"col": "id"}, "dir": "asc"}],
                   "limit": 20},
        "scope": scope}


@pytest.mark.parametrize("setting", sorted(SCOPES))
def test_the_join_key_index_the_cast_and_the_label_pass_keep_the_rows(setting, monkeypatch):
    # every row loop split from 1 row: the pool paths at any size
    monkeypatch.setenv("GNOMON_MATCH_PAR_ROWS", "1")
    # 1,300 friends x 4 posts: past the cast's 4,096-row floor (IC9's order casts every message id)
    a = _friends_posts(13, 1300, 4)
    for switch in ("GNOMON_MATCH_KEYCSR_WIDE", "GNOMON_MATCH_CAST_DISTINCT", "GNOMON_MATCH_TYPE_PASS"):
        _same_either_way(a, _triangles(SCOPES[setting]), switch, monkeypatch)
        _same_either_way(a, _posts_by_creator(SCOPES[setting]), switch, monkeypatch)
        _same_either_way(a, _twice(SCOPES[setting]), switch, monkeypatch)
        _same_either_way(a, _ic9_like("date", 10_000, SCOPES[setting]), switch, monkeypatch)


def _tagged(seed: int, n_friends: int = 400, posts_each: int = 4, n_tags: int = 40):
    """A person, friends, their posts (past the 1,024-key probe), each with one
    to three of `n_tags` tags (some re-asserted later, one retracted)."""
    rng = random.Random(seed)
    a = _native()
    node = lambda u, lab, t=0, **p: {"op": "assert_node", "uid": u, "label": lab, "props": p, "vt_s": t, "vt_e": OPEN_END}
    friends = [f"f{i}" for i in range(n_friends)]
    _commit(a, [node("p", "Person", name="p")] + [node(f, "Person", name=f) for f in friends]
            + [node(f"t{j}", "Tag", name=f"tag{j}") for j in range(n_tags)], 1)
    _commit(a, [_edge("p", f, "KNOWS", 0, str(i)) for i, f in enumerate(friends)], 2)
    ops, k = [], 0
    for f in friends:
        for _ in range(posts_each):
            t0 = rng.randrange(0, 6)
            ops.append(node(f"m{k}", "Post", t0, content=f"c{k}"))
            ops.append(_edge(f"m{k}", f, "HAS_CREATOR", t0, "0"))
            for j in rng.sample(range(n_tags), rng.randrange(1, 4)):
                ops.append(_edge(f"m{k}", f"t{j}", "HAS_TAG", t0, "0"))
            k += 1
    _commit(a, ops, 3)
    _commit(a, [node(f"t{j}", "Tag", 2, name=f"tag{j}") for j in rng.sample(range(n_tags), 5)], 4)
    try:
        _commit(a, [{"op": "retract", "ref": {"kind": "node", "uid": "t1"}, "t": 3}], 5)
    except Exception:  # noqa: BLE001 - an op the store refuses
        pass
    return a


def _ic6_like(tag: str, scope: dict) -> dict:
    """IC6's shape: the friends' posts with a tag, the other tags of those posts, counted."""
    return {"pattern": {
        "nodes": [{"var": "p", "labels": ["Person"], "uids": ["p"]}, {"var": "f", "labels": ["Person"]},
                  {"var": "m", "labels": ["Post"]}, {"var": "t", "labels": ["Tag"]}, {"var": "t2", "labels": ["Tag"]}],
        "steps": [{"from": "p", "to": "f", "rel_type": "KNOWS", "dir": "both"},
                  {"from": "m", "to": "f", "rel_type": "HAS_CREATOR", "dir": "out"},
                  {"from": "m", "to": "t", "rel_type": "HAS_TAG", "dir": "out"},
                  {"from": "m", "to": "t2", "rel_type": "HAS_TAG", "dir": "out"}],
        "where": [{"cmp": "=", "l": {"tprop": ["t", "name"], "tau": "str"}, "r": {"lit": tag}},
                  {"cmp": "!=", "l": {"tprop": ["t2", "name"], "tau": "str"}, "r": {"lit": tag}}]},
        "return": {"group": {"by": [{"as": "tagName", "expr": {"tprop": ["t2", "name"], "tau": "str"}}],
                             "aggs": [{"as": "postCount", "fn": "count_distinct", "of": {"col": "m.uid"}}]},
                   "order_by": [{"expr": {"col": "postCount"}, "dir": "desc"}, {"expr": {"col": "tagName"}, "dir": "asc"}],
                   "limit": 10},
        "scope": scope}


@pytest.mark.parametrize("setting", sorted(SCOPES))
def test_a_last_hop_read_from_its_few_targets_keeps_the_rows(setting, monkeypatch):
    VALUES["GNOMON_MATCH_TARGET_REVERSE"] = ("1", "0")
    a = _tagged(17)
    for tag in ("tag0", "tag1", "tag7", "none"):
        _same_either_way(a, _ic6_like(tag, SCOPES[setting]), "GNOMON_MATCH_TARGET_REVERSE", monkeypatch)


def _bi13_like(scope):
    """BI13's shape: each friend's months since creation (from its own vt_s)
    against its message count, an optional sub-pattern."""
    months = {"arith": "+", "l": {"arith": "*", "l": {"lit": 12}, "r": {"arith": "-",
              "l": {"calendar": "year", "arg": {"lit": 40, "tau": "ts"}}, "r": {"calendar": "year", "arg": {"col": "f.vt_s"}}}},
              "r": {"calendar": "month", "arg": {"col": "f.vt_s"}}}
    return {"pattern": {
        "nodes": [{"var": "p", "labels": ["Person"], "uids": ["p"]}, {"var": "f", "labels": ["Person"]}],
        "steps": [{"from": "p", "to": "f", "rel_type": "KNOWS", "dir": "both"}],
        "optional": [{"nodes": [{"var": "m", "labels": ["Post"]}],
                      "steps": [{"from": "m", "to": "f", "rel_type": "HAS_CREATOR", "dir": "out"}]}]},
        "return": {"group": {"by": [{"as": "id", "expr": {"col": "f.uid"}}, {"as": "months", "expr": months},
                                    {"as": "odd", "expr": {"if": [{"cmp": "=", "l": {"tprop": ["f", "name"], "tau": "str"},
                                                                   "r": {"lit": "f3"}}, {"lit": 1}, {"lit": 0}]}}],
                             "aggs": [{"as": "n", "fn": "count", "of": {"col": "m.uid"}}]},
                   "order_by": [{"expr": {"col": "n"}, "dir": "desc"}, {"expr": {"col": "id"}, "dir": "asc"}]},
        "scope": scope}


@pytest.mark.parametrize("setting", sorted(SCOPES))
def test_an_expression_of_one_variable_by_its_versions_keeps_the_rows(setting, monkeypatch):
    VALUES["GNOMON_MATCH_VERSION_EVAL"] = ("1", "0")
    monkeypatch.setenv("GNOMON_MATCH_VERSION_EVAL_ROWS", "8")
    a = _friends_posts(21, 60, 6)
    for args in (_bi13_like(SCOPES[setting]), _ic9_like("score", 10_000, SCOPES[setting]),
                 _posts_by_creator(SCOPES[setting]), _twice(SCOPES[setting])):
        _same_either_way(a, args, "GNOMON_MATCH_VERSION_EVAL", monkeypatch)
