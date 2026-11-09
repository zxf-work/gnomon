"""replaces, which stays behind its switch (read per call).

  variables) filters the right side of the join below it that first yields
  its near end (GNOMON_MATCH_WINDOW_SEMI=0: no filter; the tests set
  GNOMON_MATCH_WINDOW_SEMI_ROWS so the filter runs at these stores' sizes)
  a version id, an integer or an identity (GNOMON_MATCH_COLLAPSE_WORDS=0: as
  key values)
  (GNOMON_MATCH_COJOIN_IDS=0: the row loop over key values)
  (GNOMON_MATCH_COVALID_PAR=0: one thread)
  the target's own fields once per found version, inside the expansion
  (GNOMON_MATCH_COVALID_LOCAL=0: the expansion's rows are tested)
- a valid_window hop-metric `paths` search is priced at the array-built
  graph's rates (GNOMON_PATHS_WINDOW_HOPS_COST=0: the per-edge loop's)
"""

from __future__ import annotations

import json
import os
import random
import subprocess
import sys
import tempfile

import pytest

from gnomon.core.model import OPEN_END, canonical_json
from gnomon.temporal.algebra import call_operator, ensure_all_registered

from .conftest import ENVELOPE_META_KEYS

pytest.importorskip("gnomon._engine", reason="native engine extension not built")

ensure_all_registered()


def _native(path: str | None = None):
    from gnomon.storage.native import NativeAdapter

    if path is not None:
        return NativeAdapter(path)
    tmp = tempfile.TemporaryDirectory(prefix="gnomon-test-c4fix55-")
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


def _retract_edge(s, d, rt, disc, t):
    return {"op": "retract", "ref": {"kind": "edge", "src": s, "dst": d, "rel_type": rt, "disc": disc}, "t": t}


def _world(seed: int, path: str | None = None, n_people: int = 90):
    """People who know each other, tags they are interested in and messages
    they created with tags, at several valid times. In belief time: some
    interests and message tags retracted (tt 5), some asserted late (tt 6),
    some interests moved to another valid time (tt 7)."""
    rng = random.Random(seed)
    a = _native(path)
    people = [f"q{i}" for i in range(n_people)]
    tags = [f"t{i}" for i in range(6)]
    ops = [_node(t, "Tag", name=t) for t in tags]
    ops += [_node(x, "Person", firstName=x) for x in people]
    _commit(a, ops, 1)
    ops = []
    for i, x in enumerate(people):
        for y in rng.sample(people, 5):
            if y != x:
                ops.append(_edge(x, y, "KNOWS", rng.randrange(0, 6), disc=f"{i}"))
    interests = []
    for x in people:
        for t in rng.sample(tags, rng.randrange(0, 3)):
            v0 = rng.randrange(0, 8)
            v1 = v0 + rng.randrange(1, 6) if rng.random() < 0.4 else OPEN_END
            ops.append(_edge(x, t, "HAS_INTEREST", v0, v1))
            interests.append((x, t, v0))
    tagged = []
    for k in range(400):
        m, t0 = f"m{k}", rng.randrange(0, 8)
        ops.append(_node(m, "Comment" if k % 3 else "Post", t0, content=f"c{k}"))
        ops.append(_edge(m, rng.choice(people), "HAS_CREATOR", t0))
        for t in rng.sample(tags, rng.randrange(1, 3)):
            ops.append(_edge(m, t, "HAS_TAG", t0))
            tagged.append((m, t, t0))
    _commit(a, ops, 2)
    gone_i = rng.sample(interests, len(interests) // 6)
    gone_t = rng.sample(tagged, len(tagged) // 8)
    _commit(a, [_retract_edge(x, t, "HAS_INTEREST", "0", v0) for x, t, v0 in gone_i]
            + [_retract_edge(m, t, "HAS_TAG", "0", t0) for m, t, t0 in gone_t], 5)
    late = []
    for x in rng.sample(people, 15):
        t = rng.choice(tags)
        late.append(_edge(x, t, "HAS_INTEREST", rng.randrange(0, 8), disc="1"))
    _commit(a, late, 6)
    keep = [x for x in interests if x not in gone_i]
    moved = rng.sample(keep, len(keep) // 8)
    ops = [_retract_edge(x, t, "HAS_INTEREST", "0", v0) for x, t, v0 in moved]
    ops += [_edge(x, t, "HAS_INTEREST", rng.randrange(0, 8), disc="2") for x, t, _ in moved]
    _commit(a, ops, 7)
    return a


def _mutual(scope: dict, tag: str = "t1") -> dict:
    """BI18's shape: pairs of people interested in a tag who share a friend
    (`dir: both` hops), counted per pair."""
    pat = {"nodes": [{"var": "t", "labels": ["Tag"]}, {"var": "p1", "labels": ["Person"]},
                     {"var": "mu", "labels": ["Person"]}, {"var": "p2", "labels": ["Person"]}],
           "steps": [{"from": "p1", "to": "t", "rel_type": "HAS_INTEREST", "dir": "out"},
                     {"from": "p1", "to": "mu", "rel_type": "KNOWS", "dir": "both"},
                     {"from": "mu", "to": "p2", "rel_type": "KNOWS", "dir": "both"},
                     {"from": "p2", "to": "t", "rel_type": "HAS_INTEREST", "dir": "out"}],
           "where": [{"cmp": "=", "l": {"tprop": ["t", "name"], "tau": "str"}, "r": {"lit": tag}},
                     {"cmp": "!=", "l": {"col": "p1.uid"}, "r": {"col": "p2.uid"}}]}
    ret = {"group": {"by": [{"as": "a", "expr": {"col": "p1.uid"}}, {"as": "b", "expr": {"col": "p2.uid"}}],
                     "aggs": [{"as": "n", "fn": "count"}]},
           "order_by": [{"expr": {"col": "n"}, "dir": "desc"}, {"expr": {"col": "a"}, "dir": "asc"},
                        {"expr": {"col": "b"}, "dir": "asc"}],
           "limit": 40}
    return {"pattern": pat, "return": ret, "scope": scope}


def _same_tag(scope: dict, tag: str = "t2") -> dict:
    """BI17's closing hop: a message with a tag, its creator, the creator's
    comments that carry the same tag, as rows."""
    pat = {"nodes": [{"var": "tag", "labels": ["Tag"]}, {"var": "m1", "labels": ["Post", "Comment"]},
                     {"var": "p", "labels": ["Person"]}, {"var": "cm", "labels": ["Comment"]}],
           "steps": [{"from": "m1", "to": "tag", "rel_type": "HAS_TAG", "dir": "out"},
                     {"from": "m1", "to": "p", "rel_type": "HAS_CREATOR", "dir": "out"},
                     {"from": "cm", "to": "p", "rel_type": "HAS_CREATOR", "dir": "out"},
                     {"from": "cm", "to": "tag", "rel_type": "HAS_TAG", "dir": "out"}],
           "where": [{"cmp": "=", "l": {"tprop": ["tag", "name"], "tau": "str"}, "r": {"lit": tag}},
                     {"cmp": "!=", "l": {"col": "m1.uid"}, "r": {"col": "cm.uid"}}]}
    ret = {"rows": [{"as": "m", "expr": {"col": "m1.uid"}}, {"as": "c", "expr": {"col": "cm.uid"}},
                    {"as": "p", "expr": {"col": "p.uid"}}],
           "order_by": [{"expr": {"col": "m"}, "dir": "asc"}, {"expr": {"col": "c"}, "dir": "asc"}],
           "limit": 200}
    return {"pattern": pat, "return": ret, "scope": scope}


def _triangles(scope: dict) -> dict:
    """A closing hop back to the first variable: friends of friends who know
    the person back, counted per person."""
    return {"pattern": {"nodes": [{"var": "a", "labels": ["Person"]}, {"var": "b", "labels": ["Person"]},
                                  {"var": "c", "labels": ["Person"]}],
                        "steps": [{"from": "a", "to": "b", "rel_type": "KNOWS", "dir": "out"},
                                  {"from": "b", "to": "c", "rel_type": "KNOWS", "dir": "out"},
                                  {"from": "c", "to": "a", "rel_type": "KNOWS", "dir": "out"}],
                        "where": [{"cmp": "=", "l": {"col": "a.uid"}, "r": {"lit": "q3"}}]},
            "return": {"group": {"by": [{"as": "b", "expr": {"col": "b.uid"}}], "aggs": [{"as": "n", "fn": "count"}]},
                       "order_by": [{"expr": {"col": "n"}, "dir": "desc"}, {"expr": {"col": "b"}, "dir": "asc"}],
                       "limit": 50},
            "scope": scope}


SCOPES = {
    "current": {"vt": {"instant": 70}},
    "asof3": {"vt": {"instant": 3}, "as_of_tt": 3},
    "asof6": {"vt": {"instant": 4}, "as_of_tt": 6},
    "window": {"vt": {"window": {"t_a": 1, "t_b": 4}, "mode": "overlap"}, "co_valid": ["valid"]},
    "window_wide": {"vt": {"window": {"t_a": 0, "t_b": 70}, "mode": "overlap"}, "co_valid": ["valid"]},
    "window_asof5": {"vt": {"window": {"t_a": 0, "t_b": 70}, "mode": "overlap"}, "co_valid": ["valid"],
                     "as_of_tt": 5},
    "window_asof6": {"vt": {"window": {"t_a": 2, "t_b": 6}, "mode": "overlap"}, "co_valid": ["valid"],
                     "as_of_tt": 6},
}


def _answer(a, args):
    env = call_operator(a, "match", dict(args))
    return canonical_json({k: v for k, v in env.items() if k not in ENVELOPE_META_KEYS})


def _semi_same(a, args, monkeypatch):
    got = {}
    for v in ("0", "1"):
        monkeypatch.setenv("GNOMON_MATCH_WINDOW_SEMI", v)
        got[v] = _answer(a, args)
    assert got["0"] == got["1"], (args["scope"], args["pattern"]["steps"][0], got["0"][:200], got["1"][:200])
    return got["1"]


@pytest.mark.parametrize("setting", sorted(SCOPES))
def test_the_closing_join_semi_keeps_the_rows(setting, monkeypatch):
    monkeypatch.setenv("GNOMON_MATCH_WINDOW_SEMI_ROWS", "1")
    monkeypatch.setenv("GNOMON_MATCH_PAR_ROWS", "64")
    a = _world(31)
    sc = SCOPES[setting]
    for args in (_mutual(sc), _mutual(sc, "t4"), _same_tag(sc), _same_tag(sc, "t0"), _triangles(sc)):
        _semi_same(a, args, monkeypatch)


def test_the_closing_join_semi_keeps_rows_at_the_default_bound(monkeypatch):
    """Below the row bound nothing is filtered; the answers match either way."""
    monkeypatch.delenv("GNOMON_MATCH_WINDOW_SEMI_ROWS", raising=False)
    a = _world(37)
    for setting in ("window", "window_asof6"):
        _semi_same(a, _mutual(SCOPES[setting]), monkeypatch)


_TRACE_RUN = r"""
import json, sys
from gnomon.storage.native import NativeAdapter
from gnomon.temporal.algebra import call_operator, ensure_all_registered
ensure_all_registered()
a = NativeAdapter(sys.argv[1])
for args in json.loads(sys.argv[2]):
    call_operator(a, "match", args)
"""


def test_the_window_closing_joins_take_the_semi(tmp_path):
    """Under the window the closing joins of BI18's and BI17's shapes filter
    the join that first yields their near end (the trace's line)."""
    _world(41, str(tmp_path / "s"))
    args = [_mutual(SCOPES["window_wide"]), _same_tag(SCOPES["window_wide"])]
    env = {**os.environ, "GNOMON_MATCH_TRACE": "1", "GNOMON_MATCH_WINDOW_SEMI_ROWS": "1"}
    env.pop("GNOMON_MATCH_WINDOW_SEMI", None)
    r = subprocess.run([sys.executable, "-c", _TRACE_RUN, str(tmp_path / "s"), json.dumps(args)],
                       env=env, capture_output=True, text=True, timeout=300)
    assert r.returncode == 0, r.stderr[-2000:]
    lines = [x for x in r.stderr.splitlines() if "window semi-join on" in x and "->" in x]
    assert any("t.uid -> _fr" in x for x in lines), r.stderr[-3000:]
    assert any("tag.uid -> _e" in x for x in lines), r.stderr[-3000:]
    env["GNOMON_MATCH_WINDOW_SEMI"] = "0"
    r = subprocess.run([sys.executable, "-c", _TRACE_RUN, str(tmp_path / "s"), json.dumps(args)],
                       env=env, capture_output=True, text=True, timeout=300)
    assert r.returncode == 0, r.stderr[-2000:]
    assert "window semi-join on" not in r.stderr


# ---- Collapse's keys as words (GNOMON_MATCH_COLLAPSE_WORDS)


def _friends_messages(scope: dict, before: int = 6) -> dict:
    """IC9's shape: the messages of friends and friends of friends (a
    variable-length `dir: both` step: one row per path, collapsed to the
    least depth), created before a time, newest first."""
    pat = {"nodes": [{"var": "p", "labels": ["Person"], "uids": ["q1"]}, {"var": "f", "labels": ["Person"]},
                     {"var": "m", "labels": ["Post", "Comment"]}],
           "steps": [{"from": "p", "to": "f", "rel_type": "KNOWS", "dir": "both", "hops": {"min": 1, "max": 2}},
                     {"from": "m", "to": "f", "rel_type": "HAS_CREATOR", "dir": "out"}],
           "where": [{"cmp": "<", "l": {"col": "m.vt_s"}, "r": {"lit": before, "tau": "ts"}},
                     {"cmp": "!=", "l": {"col": "f.uid"}, "r": {"col": "p.uid"}}]}
    ret = {"rows": [{"as": "f", "expr": {"col": "f.uid"}}, {"as": "m", "expr": {"col": "m.uid"}},
                    {"as": "d", "expr": {"col": "m.vt_s"}}],
           "order_by": [{"expr": {"col": "d"}, "dir": "desc"}, {"expr": {"col": "m"}, "dir": "asc"}],
           "limit": 30}
    return {"pattern": pat, "return": ret, "scope": scope}


@pytest.mark.parametrize("setting", sorted(s for s in SCOPES if s.startswith("window")))
def test_the_collapse_words_keep_the_rows(setting, monkeypatch):
    monkeypatch.setenv("GNOMON_MATCH_PAR_ROWS", "64")
    a = _world(43)
    sc = SCOPES[setting]
    for args in (_friends_messages(sc), _friends_messages(sc, 3), _mutual(sc), _same_tag(sc), _triangles(sc)):
        got = {}
        for v in ("0", "1"):
            monkeypatch.setenv("GNOMON_MATCH_COLLAPSE_WORDS", v)
            got[v] = _answer(a, args)
        assert got["0"] == got["1"], (setting, args["pattern"]["steps"][0])


# ---- CoValidJoin on identity keys (GNOMON_MATCH_COJOIN_IDS)


def _not_friends(scope: dict) -> dict:
    """BI18's anti-join: pairs sharing an interest and a friend, at the
    instants they do not know each other."""
    args = _mutual(scope)
    args["pattern"]["not"] = [{"steps": [{"from": "p1", "to": "p2", "rel_type": "KNOWS", "dir": "both"}]}]
    return args


def _maybe_interest(scope: dict) -> dict:
    """An optional sub-pattern on one key: each person's interests, and the
    remainder of the window with none (nulls)."""
    return {"pattern": {"nodes": [{"var": "p", "labels": ["Person"]}],
                        "optional": [{"nodes": [{"var": "t"}],
                                      "steps": [{"from": "p", "to": "t", "rel_type": "HAS_INTEREST"}]}]},
            "return": {"rows": [{"as": "p", "expr": {"col": "p.uid"}}, {"as": "t", "expr": {"col": "t.uid"}}],
                       "order_by": [{"expr": {"col": "p"}, "dir": "asc"}, {"expr": {"col": "t"}, "dir": "asc"}],
                       "limit": 300},
            "scope": scope}


def _no_comment(scope: dict) -> dict:
    """A `not` sub-pattern on two keys: tagged messages whose creator has no
    other message with that tag."""
    args = _same_tag(scope)
    args["pattern"]["nodes"] = [n for n in args["pattern"]["nodes"] if n["var"] != "cm"]
    args["pattern"]["steps"] = args["pattern"]["steps"][:2]
    args["pattern"]["where"] = args["pattern"]["where"][:1]
    args["pattern"]["not"] = [{"nodes": [{"var": "o", "labels": ["Comment"]}],
                               "steps": [{"from": "o", "to": "p", "rel_type": "HAS_CREATOR"},
                                         {"from": "o", "to": "tag", "rel_type": "HAS_TAG"}]}]
    args["return"] = {"rows": [{"as": "m", "expr": {"col": "m1.uid"}}, {"as": "p", "expr": {"col": "p.uid"}}],
                      "order_by": [{"expr": {"col": "m"}, "dir": "asc"}], "limit": 200}
    return args


@pytest.mark.parametrize("setting", sorted(s for s in SCOPES if s.startswith("window")))
def test_the_cojoin_ids_keep_the_rows(setting, monkeypatch):
    monkeypatch.setenv("GNOMON_MATCH_PAR_ROWS", "64")
    a = _world(47)
    sc = SCOPES[setting]
    for args in (_not_friends(sc), _maybe_interest(sc), _no_comment(sc), _no_comment({**sc, "vt": {**sc["vt"]}})):
        got = {}
        for v in ("0", "1"):
            monkeypatch.setenv("GNOMON_MATCH_COJOIN_IDS", v)
            got[v] = _answer(a, args)
        assert got["0"] == got["1"], (setting, args["pattern"].get("not") or args["pattern"].get("optional"))
        assert "error" not in got["1"][:200].lower(), got["1"][:300]


# ---- CoValidExpand's searches on the pool (GNOMON_MATCH_COVALID_PAR)


def _reach_from_all(scope: dict, lo: int, hi: int | None, dir_: str = "out", rows: bool = True) -> dict:
    """BI12's shape on KNOWS: a variable-length step from every person (one
    search per seed and start interval), its targets kept or counted."""
    hops = {"min": lo, "max": hi}
    pat = {"nodes": [{"var": "p", "labels": ["Person"]}, {"var": "f", "labels": ["Person"]}],
           "steps": [{"from": "p", "to": "f", "rel_type": "KNOWS", "dir": dir_, "hops": hops}]}
    if rows:
        ret = {"rows": [{"as": "p", "expr": {"col": "p.uid"}}, {"as": "f", "expr": {"col": "f.uid"}},
                        {"as": "d", "expr": {"col": "f.depth"}}],
               "order_by": [{"expr": {"col": "p"}, "dir": "asc"}, {"expr": {"col": "f"}, "dir": "asc"}], "limit": 500}
    else:
        ret = {"group": {"by": [{"as": "p", "expr": {"col": "p.uid"}}], "aggs": [{"as": "n", "fn": "count"}]},
               "order_by": [{"expr": {"col": "n"}, "dir": "desc"}, {"expr": {"col": "p"}, "dir": "asc"}], "limit": 90}
    return {"pattern": pat, "return": ret, "scope": scope}


# ---- target predicates inside CoValidExpand (GNOMON_MATCH_COVALID_LOCAL)


def _named_friends(scope: dict, names: tuple, depth: bool = False, rows: bool = True) -> dict:
    """BI12's shape on KNOWS: a variable-length step whose target is tested on
    its label and a property (and, with `depth`, on its depth too, which is
    not decided inside the search)."""
    where = [{"bool": "or", "l": {"cmp": "=", "l": {"tprop": ["f", "firstName"], "tau": "str"}, "r": {"lit": names[0]}},
              "r": {"cmp": "=", "l": {"tprop": ["f", "firstName"], "tau": "str"}, "r": {"lit": names[1]}}}]
    if depth:
        where.append({"cmp": "=", "l": {"col": "f.depth"}, "r": {"lit": 2}})
    pat = {"nodes": [{"var": "p", "labels": ["Person"]}, {"var": "f", "labels": ["Person"]}],
           "steps": [{"from": "p", "to": "f", "rel_type": "KNOWS", "dir": "out", "hops": {"min": 1, "max": 3}}],
           "where": where}
    if rows:
        ret = {"rows": [{"as": "p", "expr": {"col": "p.uid"}}, {"as": "f", "expr": {"col": "f.uid"}}],
               "order_by": [{"expr": {"col": "p"}, "dir": "asc"}, {"expr": {"col": "f"}, "dir": "asc"}], "limit": 400}
    else:
        ret = {"group": {"by": [{"as": "f", "expr": {"col": "f.uid"}}], "aggs": [{"as": "n", "fn": "count"}]},
               "order_by": [{"expr": {"col": "n"}, "dir": "desc"}, {"expr": {"col": "f"}, "dir": "asc"}], "limit": 50}
    return {"pattern": pat, "return": ret, "scope": scope}


@pytest.mark.parametrize("setting", sorted(s for s in SCOPES if s.startswith("window")))
def test_the_covalid_searches_on_the_pool_keep_the_rows(setting, monkeypatch):
    monkeypatch.setenv("GNOMON_MATCH_PAR_ROWS", "64")
    a = _world(59)
    sc = SCOPES[setting]
    for args in (_reach_from_all(sc, 1, 3), _reach_from_all(sc, 0, None, rows=False), _reach_from_all(sc, 2, 2, "both"),
                 _friends_messages(sc), _not_friends(sc)):
        got = {}
        for v in ("0", "1"):
            monkeypatch.setenv("GNOMON_MATCH_COVALID_PAR", v)
            got[v] = _answer(a, args)
        assert got["0"] == got["1"], (setting, args["pattern"]["steps"][0])


@pytest.mark.parametrize("setting", sorted(s for s in SCOPES if s.startswith("window")))
def test_the_covalid_target_predicates_keep_the_rows(setting, monkeypatch):
    monkeypatch.setenv("GNOMON_MATCH_PAR_ROWS", "64")
    a = _world(53)
    sc = SCOPES[setting]
    for args in (_named_friends(sc, ("q3", "q7")), _named_friends(sc, ("q1", "q40"), depth=True),
                 _named_friends(sc, ("q5", "q9"), rows=False), _friends_messages(sc)):
        got = {}
        for v in ("0", "1"):
            monkeypatch.setenv("GNOMON_MATCH_COVALID_LOCAL", v)
            got[v] = _answer(a, args)
        assert got["0"] == got["1"], (setting, args["pattern"]["where"])


# ---- window hop-metric paths pricing (GNOMON_PATHS_WINDOW_HOPS_COST)


def _hops(result: str = "distance", **extra) -> dict:
    """IC13's shape: the hop distance between two people over KNOWS, both
    directions, under a valid window."""
    return {"sources": {"uids": ["q1"]}, "targets": {"uids": ["q40"]}, "rel_types": ["KNOWS"], "dir": "both",
            "metric": "hops", "result": result, "valid_window": {"t_a": 1, "t_b": 6}, **extra}


def test_window_hops_are_priced_at_the_array_rates(monkeypatch):
    import gnomon.temporal.guardrails as GR

    a = _world(67)
    seen = []
    orig = GR.enforce_cost

    def wrap(op, estimate, ceilings=None):
        if op == "paths":
            seen.append(estimate["time_est_ms"])
        return orig(op, estimate, ceilings)

    monkeypatch.setattr(GR, "enforce_cost", wrap)
    import sys
    for m in list(sys.modules.values()):
        if getattr(m, "enforce_cost", None) is orig and m is not GR:
            monkeypatch.setattr(m, "enforce_cost", wrap)
    got = {}
    for v in ("0", "1"):
        monkeypatch.setenv("GNOMON_PATHS_WINDOW_HOPS_COST", v)
        seen.clear()
        env = call_operator(a, "paths", _hops())
        got[v] = (canonical_json({k: x for k, x in env.items() if k not in ENVELOPE_META_KEYS}), list(seen))
    assert got["0"][0] == got["1"][0]
    assert got["1"][1] and got["0"][1] and got["1"][1][0] < got["0"][1][0], (got["0"][1], got["1"][1])
    # an enumeration and an edge predicate keep the per-edge loop's price
    for args in (_hops("all_paths", max_hops=3), _hops(edge_where=[{"prop": "x", "cmp": "=", "value": 1}])):
        est = {}
        for v in ("0", "1"):
            monkeypatch.setenv("GNOMON_PATHS_WINDOW_HOPS_COST", v)
            seen.clear()
            try:
                call_operator(a, "paths", args)
            except Exception:
                pass
            est[v] = list(seen)
        assert est["0"] == est["1"], (args.get("result"), est)
