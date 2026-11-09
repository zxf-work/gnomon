"""replaces, which stays behind its switch (read per call).

- an answer's order and limit cut the one-hop expansion under them to the
  targets that can reach the page (GNOMON_MATCH_TOPK_HOP)
- a pre-probe sample that leans to the scan decides it (GNOMON_MATCH_PREPROBE_LEAN)
"""

from __future__ import annotations

import random
import tempfile

import pytest

from gnomon.core.model import OPEN_END, canonical_json
from gnomon.temporal.algebra import call_operator, ensure_all_registered

from .conftest import ENVELOPE_META_KEYS

ensure_all_registered()


def _native():
    from gnomon.storage.native import NativeAdapter

    tmp = tempfile.TemporaryDirectory(prefix="gnomon-test-c4fix49-")
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


def _node(uid, label, vt_s=0, vt_e=OPEN_END, **props):
    return {"op": "assert_node", "uid": uid, "label": label, "props": props, "vt_s": vt_s, "vt_e": vt_e}


def _edge(s, d, rt, vt_s=0, vt_e=OPEN_END, disc="0"):
    return {"op": "assert_edge", "src": s, "dst": d, "rel_type": rt, "props": {},
            "vt_s": vt_s, "vt_e": vt_e, "disc": disc}


def _friends_posts(seed: int, n_friends: int, posts_each: int = 3):
    """One person who knows `n_friends` persons (some twice, by two edges: a
    friend's posts then make two rows each), each friend with posts whose
    creation times fall in a few values (ties), some without a score (nulls),
    some re-asserted later (a second version), one retracted."""
    rng = random.Random(seed)
    a = _native()
    friends = [f"f{i}" for i in range(n_friends)]
    _commit(a, [_node("p", "Person", name="p")] + [_node(f, "Person", name=f) for f in friends], 1)
    ops = [_edge("p", f, "KNOWS", 0, disc=str(i)) for i, f in enumerate(friends)]
    ops += [_edge(f, "p", "KNOWS", 0, disc="x") for f in rng.sample(friends, max(1, n_friends // 7))]
    _commit(a, ops, 2)
    ops = []
    k = 0
    for f in friends:
        for _ in range(posts_each):
            t0 = rng.randrange(0, 6)
            props = {"content": f"m{k}"}
            if rng.random() < 0.8:
                props["score"] = rng.randrange(0, 4)
            ops.append(_node(f"{k}", "Post", t0, **props))
            ops.append(_edge(f"{k}", f, "HAS_CREATOR", t0))
            k += 1
    _commit(a, ops, 3)
    _commit(a, [_node(f"{j}", "Post", rng.randrange(0, 6), content="later") for j in rng.sample(range(k), k // 10)], 4)
    try:
        _commit(a, [{"op": "retract", "ref": {"kind": "node", "uid": "1"}, "t": 2}], 5)
    except Exception:  # noqa: BLE001 - an op the store refuses
        pass
    return a


SCOPES = {
    "current": {"vt": {"instant": 70}},
    "asof": {"vt": {"instant": 70}, "as_of_tt": 3},
    "window": {"vt": {"window": {"t_a": 1, "t_b": 4}, "mode": "overlap"}, "co_valid": ["valid"]},
}


def _ic9_like(first_key: str, limit: int, scope: dict, max_date: int = 5) -> dict:
    """IC9's shape: a person's friends' posts before a date, newest first, by
    id on a tie, the first `limit` rows."""
    key = {"col": "creationDate"} if first_key == "date" else {"col": "score"}
    return {"pattern": {
        "nodes": [{"var": "p", "labels": ["Person"], "uids": ["p"]}, {"var": "f", "labels": ["Person"]},
                  {"var": "m", "labels": ["Post"]}],
        "steps": [{"from": "p", "to": "f", "rel_type": "KNOWS", "dir": "both"},
                  {"from": "m", "to": "f", "rel_type": "HAS_CREATOR", "dir": "out"}],
        "where": [{"cmp": "<", "l": {"col": "m.vt_s"}, "r": {"lit": max_date, "tau": "ts"}}]},
        "return": {"rows": [{"as": "personId", "expr": {"col": "f.uid"}},
                            {"as": "messageId", "expr": {"col": "m.uid"}},
                            {"as": "content", "expr": {"tprop": ["m", "content"], "tau": "str"}},
                            {"as": "score", "expr": {"tprop": ["m", "score"], "tau": "int"}},
                            {"as": "creationDate", "expr": {"col": "m.vt_s"}}],
                   "order_by": [{"expr": key, "dir": "desc"},
                                {"expr": {"cast": {"col": "messageId"}, "to": "int"}, "dir": "asc"},
                                {"expr": {"col": "personId"}, "dir": "asc"}],
                   "limit": limit},
        "scope": scope}


def _same_either_way(a, args: dict, switch: str, monkeypatch) -> str:
    got = {}
    for flag in ("1", "0"):
        monkeypatch.setenv(switch, flag if switch != "GNOMON_MATCH_NODE_PASS" else ("par" if flag == "1" else "serial"))
        env = call_operator(a, "match", dict(args))
        got[flag] = canonical_json({k: v for k, v in env.items() if k not in ENVELOPE_META_KEYS})
    assert got["1"] == got["0"], (switch, args)
    return got["1"]


# 30 friends: the hop by the index (the expansion's cut); 1,300: past the
# 1,024-key probe, the hop by one scan (the cut before grouping)
@pytest.mark.parametrize("n_friends", [30, 1300])
@pytest.mark.parametrize("setting", sorted(SCOPES))
def test_ic9_shaped_top_rows_are_the_same_either_way(n_friends, setting, monkeypatch):
    a = _friends_posts(n_friends, n_friends)
    for first_key in ("date", "score"):
        for limit in (1, 2, 3, 5, 19, 20, 21, 64, 10_000):
            args = _ic9_like(first_key, limit, SCOPES[setting])
            _same_either_way(a, args, "GNOMON_MATCH_TOPK_HOP", monkeypatch)
            # pages inside the limit: the order's need is the page's end
            for cursor, page in (("0", 1), ("1", 2), ("3", 4)):
                _same_either_way(a, {**args, "cursor": cursor, "limit": page}, "GNOMON_MATCH_TOPK_HOP", monkeypatch)


@pytest.mark.parametrize("setting", sorted(SCOPES))
def test_the_lean_pre_probe_keeps_the_rows(setting, monkeypatch):
    a = _friends_posts(7, 1300)
    for limit in (5, 10_000):
        _same_either_way(a, _ic9_like("date", limit, SCOPES[setting]), "GNOMON_MATCH_PREPROBE_LEAN", monkeypatch)


def _chain_like(rels: tuple[str, str], scope: dict) -> dict:
    """Two one-hop steps: of two relationship types (BI4's chain, the
    distinct-edge filter passes every row) or of one (KNOWS twice, the filter
    drops the walks back along the same edge)."""
    second = ({"from": "m", "to": "f", "rel_type": "HAS_CREATOR", "dir": "out"} if rels[1] == "HAS_CREATOR"
              else {"from": "f", "to": "m", "rel_type": "KNOWS", "dir": "both"})
    return {"pattern": {
        "nodes": [{"var": "p", "labels": ["Person"]}, {"var": "f", "labels": ["Person"]}, {"var": "m"}],
        "steps": [{"from": "p", "to": "f", "rel_type": rels[0], "dir": "both"}, second]},
        "return": {"group": {"by": [{"as": "p", "expr": {"col": "p.uid"}}, {"as": "m", "expr": {"col": "m.uid"}}],
                             "aggs": [{"as": "n", "fn": "count"}]},
                   "order_by": [{"expr": {"col": "p"}, "dir": "asc"}, {"expr": {"col": "m"}, "dir": "asc"}]},
        "scope": scope}


@pytest.mark.parametrize("setting", sorted(SCOPES))
@pytest.mark.parametrize("rels", [("KNOWS", "HAS_CREATOR"), ("KNOWS", "KNOWS")])
def test_the_distinct_edge_filter_of_two_types_passes_every_row(setting, rels, monkeypatch):
    a = _friends_posts(3, 40, 2)
    _same_either_way(a, _chain_like(rels, SCOPES[setting]), "GNOMON_MATCH_EDGE_DISTINCT", monkeypatch)
    # and the top-k cut behind it (IC2's shape: the filter no longer stops it)
    _same_either_way(a, _ic9_like("date", 3, SCOPES[setting]), "GNOMON_MATCH_EDGE_DISTINCT", monkeypatch)


@pytest.mark.parametrize("setting", sorted(SCOPES))
def test_the_direct_chain_rows_and_gathers_keep_the_rows(setting, monkeypatch):
    # every row loop split from 1 row (the parallel gathers at any size)
    monkeypatch.setenv("GNOMON_MATCH_PAR_ROWS", "1")
    a = _friends_posts(11, 60, 3)
    for rels in (("KNOWS", "HAS_CREATOR"), ("KNOWS", "KNOWS")):
        _same_either_way(a, _chain_like(rels, SCOPES[setting]), "GNOMON_MATCH_ROWS_FAST", monkeypatch)
    _same_either_way(a, _ic9_like("score", 10_000, SCOPES[setting]), "GNOMON_MATCH_ROWS_FAST", monkeypatch)
    # the version reads of many targets: one pass over the node rows
    for limit in (3, 10_000):
        _same_either_way(a, _ic9_like("date", limit, SCOPES[setting]), "GNOMON_MATCH_NODE_PASS", monkeypatch)


def _bi4_like(scope: dict) -> dict:
    """BI4's first call: distinct members per (forum, country), here (person,
    tag-of-post) distinct posts and distinct creators, then the max per person."""
    return {"pattern": {
        "nodes": [{"var": "p", "labels": ["Person"]}, {"var": "f", "labels": ["Person"]}, {"var": "m", "labels": ["Post"]}],
        "steps": [{"from": "p", "to": "f", "rel_type": "KNOWS", "dir": "both"},
                  {"from": "m", "to": "f", "rel_type": "HAS_CREATOR", "dir": "out"}]},
        "return": {"group": {"by": [{"as": "a", "expr": {"col": "p.uid"}}, {"as": "b", "expr": {"col": "f.uid"}}],
                             "aggs": [{"as": "n", "fn": "count_distinct", "of": {"col": "m.uid"}}],
                             "then": {"by": [{"as": "a", "expr": {"col": "a"}}],
                                      "aggs": [{"as": "top", "fn": "max", "of": {"col": "n"}}]}},
                   "order_by": [{"expr": {"col": "top"}, "dir": "desc"}, {"expr": {"col": "a"}, "dir": "asc"}]},
        "scope": scope}


def _distinct_one_key(scope: dict) -> dict:
    args = _bi4_like(scope)
    args["return"] = {"group": {"by": [{"as": "a", "expr": {"col": "f.uid"}}],
                                "aggs": [{"as": "n", "fn": "count_distinct", "of": {"col": "p.uid"}}]},
                      "order_by": [{"expr": {"col": "n"}, "dir": "desc"}, {"expr": {"col": "a"}, "dir": "asc"}]}
    return args


@pytest.mark.parametrize("setting", sorted(SCOPES))
def test_distinct_counts_by_sort_are_the_folds(setting, monkeypatch):
    a = _friends_posts(5, 50, 3)
    for args in (_bi4_like(SCOPES[setting]), _distinct_one_key(SCOPES[setting])):
        _same_either_way(a, args, "GNOMON_MATCH_DISTINCT_SORT", monkeypatch)


def _with_forums(seed: int, n_friends: int = 1300):
    """`_friends_posts`, and forums whose members are some friends and whose
    posts are many friends' posts (most by non-members)."""
    rng = random.Random(seed)
    a = _friends_posts(seed, n_friends, 3)
    n_posts = 3 * n_friends
    ops = [_node(f"fo{j}", "Forum", 0, title=f"forum {j}") for j in range(4)]
    for j in range(4):
        for k, f in enumerate(rng.sample(range(n_friends), 20)):
            ops.append(_edge(f"fo{j}", f"f{f}", "HAS_MEMBER", rng.randrange(0, 4), disc=str(k)))
        for m in rng.sample(range(n_posts), n_posts // 3):
            ops.append(_edge(f"fo{j}", f"{m}", "CONTAINER_OF", 0))
    _commit(a, ops, 6)
    return a


def _ic5_like(scope: dict) -> dict:
    """IC5's shape: friends' forums, and per forum the posts its friend
    members created there (an optional sub-pattern closing on the friend)."""
    return {"pattern": {
        "nodes": [{"var": "p", "labels": ["Person"], "uids": ["p"]}, {"var": "f", "labels": ["Person"]},
                  {"var": "fo", "labels": ["Forum"]}],
        "steps": [{"from": "p", "to": "f", "rel_type": "KNOWS", "dir": "both"},
                  {"from": "fo", "to": "f", "rel_type": "HAS_MEMBER", "dir": "out", "var": "hm"}],
        "where": [{"cmp": ">", "l": {"col": "hm.vt_s"}, "r": {"lit": 0, "tau": "ts"}}],
        "optional": [{"nodes": [{"var": "po", "labels": ["Post"]}],
                      "steps": [{"from": "fo", "to": "po", "rel_type": "CONTAINER_OF", "dir": "out"},
                                {"from": "po", "to": "f", "rel_type": "HAS_CREATOR", "dir": "out"}]}]},
        "return": {"group": {"by": [{"as": "forumId", "expr": {"col": "fo.uid"}},
                                    {"as": "member", "expr": {"col": "f.uid"}}],
                             "aggs": [{"as": "postCount", "fn": "count_distinct", "of": {"col": "po.uid"}},
                                      {"as": "rows", "fn": "count"}]},
                   "order_by": [{"expr": {"col": "postCount"}, "dir": "desc"}, {"expr": {"col": "forumId"}, "dir": "asc"},
                                {"expr": {"col": "member"}, "dir": "asc"}]},
        "scope": scope}


@pytest.mark.parametrize("setting", sorted(SCOPES))
def test_a_closing_step_read_from_its_few_targets_keeps_the_rows(setting, monkeypatch):
    a = _with_forums(9)
    _same_either_way(a, _ic5_like(SCOPES[setting]), "GNOMON_MATCH_CLOSE_REVERSE", monkeypatch)
