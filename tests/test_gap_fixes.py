"""replaces, which stays behind its switch (read per call).

- a uid-anchored pattern is grown from a second anchor pinned by literal
  values and joined at one variable, when the estimates say the given plan
  costs more (GNOMON_MATCH_SECOND_ANCHOR=0: the given plan; =force: the split
  whenever the shape allows, for these tests)
- a current-belief edge scan does each row's work in its own chunks
  (GNOMON_MATCH_SCAN_FUSE=0: the address list first)
- an inner identity join on distinct right keys looks each left row's match
  up and keeps the left side when every row matches (GNOMON_MATCH_UNIQUE_JOIN=0);
  a node seek's label test runs on the pool (GNOMON_MATCH_TYPE_PASS=serial)
- a filter over the common-interval chain is computed in one pass
  (GNOMON_MATCH_INTERVAL_FUSE=0)
- a join key index's map takes its keys mixed (GNOMON_MATCH_KEY_MIX=0)
"""

from __future__ import annotations

import random
import tempfile

import pytest

from gnomon.core.model import OPEN_END, canonical_json
from gnomon.temporal import ops_match as OM
from gnomon.temporal.algebra import call_operator, ensure_all_registered

from .conftest import ENVELOPE_META_KEYS

pytest.importorskip("gnomon._engine", reason="native engine extension not built")

ensure_all_registered()


def _native():
    from gnomon.storage.native import NativeAdapter

    tmp = tempfile.TemporaryDirectory(prefix="gnomon-test-c4fix54-")
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


COUNTRIES = ["X", "Y", "Z", "W"]


def _world(seed: int, n_people: int = 120):
    """A person `p`, people who know each other, cities in four countries,
    and messages located in a country, by a creator, at a few times (ties).
    In belief time: some message locations retracted (tt 5), some messages
    corrected to another valid time (tt 6), some moved to another country
    (the old edge retracted, a new one asserted, tt 7)."""
    rng = random.Random(seed)
    a = _native()
    people = ["p"] + [f"q{i}" for i in range(n_people)]
    ops = [_node(c, "Country", name=c) for c in COUNTRIES]
    ops += [_node(f"c{i}", "City", name=f"c{i}") for i in range(8)]
    ops += [_node(x, "Person", firstName=x) for x in people]
    _commit(a, ops, 1)
    ops = [_edge(f"c{i}", COUNTRIES[i % 4], "IS_PART_OF") for i in range(8)]
    ops += [_edge(x, f"c{rng.randrange(8)}", "IS_LOCATED_IN") for x in people]
    for i, x in enumerate(people):
        for y in rng.sample(people, 4):
            if y != x:
                ops.append(_edge(x, y, "KNOWS", disc=f"{i}"))
    _commit(a, ops, 2)
    ops, located = [], []
    for k in range(900):
        m, t0 = f"m{k}", rng.randrange(0, 8)
        where = rng.choice(COUNTRIES[:2] * 3 + COUNTRIES[2:])
        ops.append(_node(m, "Post", t0, content=f"t{k}"))
        ops.append(_edge(m, rng.choice(people), "HAS_CREATOR", t0))
        ops.append(_edge(m, where, "IS_LOCATED_IN", t0, disc="0"))
        located.append((m, where, t0))
    _commit(a, ops, 3)
    gone = rng.sample(located, 60)
    _commit(a, [_retract_edge(m, c, "IS_LOCATED_IN", "0", t0) for m, c, t0 in gone], 5)
    keep = [x for x in located if x not in gone]
    _commit(a, [_node(m, "Post", rng.randrange(0, 8), content="corrected") for m, _, _ in rng.sample(keep, 50)], 6)
    moved = rng.sample(keep, 40)
    ops = [_retract_edge(m, c, "IS_LOCATED_IN", "0", t0) for m, c, t0 in moved]
    ops += [_edge(m, rng.choice(COUNTRIES), "IS_LOCATED_IN", t0, disc="1") for m, _, t0 in moved]
    _commit(a, ops, 7)
    return a


def _ic3_like(scope: dict, start: int = 2, end: int = 6, rows: bool = False) -> dict:
    """IC3's shape: a person's friends and friends of friends outside two
    countries, with their messages located in either country in a window,
    counted per friend (or as rows, newest first, cut at a page)."""
    pat = {"nodes": [{"var": "p", "labels": ["Person"], "uids": ["p"]}, {"var": "f", "labels": ["Person"]},
                     {"var": "ci", "labels": ["City"]}, {"var": "cn", "labels": ["Country"]},
                     {"var": "m", "labels": ["Post"]}, {"var": "mc", "labels": ["Country"]}],
           "steps": [{"from": "p", "to": "f", "rel_type": "KNOWS", "dir": "both", "hops": {"min": 1, "max": 2}},
                     {"from": "f", "to": "ci", "rel_type": "IS_LOCATED_IN", "dir": "out"},
                     {"from": "ci", "to": "cn", "rel_type": "IS_PART_OF", "dir": "out"},
                     {"from": "m", "to": "f", "rel_type": "HAS_CREATOR", "dir": "out"},
                     {"from": "m", "to": "mc", "rel_type": "IS_LOCATED_IN", "dir": "out"}],
           "where": [{"cmp": "!=", "l": {"tprop": ["cn", "name"], "tau": "str"}, "r": {"lit": "X"}},
                     {"cmp": "!=", "l": {"tprop": ["cn", "name"], "tau": "str"}, "r": {"lit": "Y"}},
                     {"bool": "and", "l": {"cmp": ">=", "l": {"col": "m.vt_s"}, "r": {"lit": start, "tau": "ts"}},
                      "r": {"cmp": "<", "l": {"col": "m.vt_s"}, "r": {"lit": end, "tau": "ts"}}},
                     {"bool": "or", "l": {"cmp": "=", "l": {"tprop": ["mc", "name"], "tau": "str"}, "r": {"lit": "X"}},
                      "r": {"cmp": "=", "l": {"tprop": ["mc", "name"], "tau": "str"}, "r": {"lit": "Y"}}}]}
    if rows:
        ret = {"rows": [{"as": "personId", "expr": {"col": "f.uid"}}, {"as": "messageId", "expr": {"col": "m.uid"}},
                        {"as": "country", "expr": {"tprop": ["mc", "name"], "tau": "str"}},
                        {"as": "creationDate", "expr": {"col": "m.vt_s"}}],
               "order_by": [{"expr": {"col": "creationDate"}, "dir": "desc"}], "limit": 25}
    else:
        x = {"cmp": "=", "l": {"tprop": ["mc", "name"], "tau": "str"}, "r": {"lit": "X"}}
        y = {"cmp": "=", "l": {"tprop": ["mc", "name"], "tau": "str"}, "r": {"lit": "Y"}}
        ret = {"group": {"by": [{"as": "personId", "expr": {"col": "f.uid"}},
                                {"as": "firstName", "expr": {"tprop": ["f", "firstName"], "tau": "str"}}],
                         "aggs": [{"as": "xCount", "fn": "count", "where": x},
                                  {"as": "yCount", "fn": "count", "where": y}]},
               "order_by": [{"expr": {"col": "xCount"}, "dir": "desc"}, {"expr": {"col": "personId"}, "dir": "asc"}],
               "limit": 20}
    return {"pattern": pat, "return": ret, "scope": scope}


SCOPES = {
    "current": {"vt": {"instant": 70}},
    "asof3": {"vt": {"instant": 70}, "as_of_tt": 3},
    "asof5": {"vt": {"instant": 70}, "as_of_tt": 5},
    "asof6": {"vt": {"instant": 4}, "as_of_tt": 6},
    "asof7": {"vt": {"instant": 70}, "as_of_tt": 7},
    "window": {"vt": {"window": {"t_a": 1, "t_b": 4}, "mode": "overlap"}, "co_valid": ["valid"]},
    "window_asof5": {"vt": {"window": {"t_a": 0, "t_b": 70}, "mode": "overlap"}, "co_valid": ["valid"],
                     "as_of_tt": 5},
}


def _answer(a, args):
    env = call_operator(a, "match", dict(args))
    return canonical_json({k: v for k, v in env.items() if k not in ENVELOPE_META_KEYS})


def _plans(a, args, monkeypatch):
    """The compiled plan's operators under each value of the switch."""
    out = {}
    filled = OM.validate(dict(args))
    for v in ("0", "1", "force"):
        monkeypatch.setenv("GNOMON_MATCH_SECOND_ANCHOR", v)
        ends = OM.anchor_cardinalities(a, filled)
        out[v] = [n["op"] for n in OM.compile_match(filled, None, ends).doc["nodes"]]
    return out


@pytest.mark.parametrize("rows", [False, True])
@pytest.mark.parametrize("setting", sorted(SCOPES))
def test_the_second_anchor_keeps_the_answer(setting, rows, monkeypatch):
    a = _world(54)
    for start, end in ((2, 6), (0, 8), (5, 6)):
        args = _ic3_like(SCOPES[setting], start, end, rows)
        got = {}
        for v in ("0", "1", "force"):
            monkeypatch.setenv("GNOMON_MATCH_SECOND_ANCHOR", v)
            got[v] = _answer(a, args)
        assert got["0"] == got["1"] == got["force"], (setting, rows, start, end)
        plans = _plans(a, args, monkeypatch)
        # the split joins two sides: one more Join than the given plan
        assert plans["force"].count("Join") == plans["0"].count("Join") + 1, plans


def test_the_answer_sees_retracted_and_corrected_anchor_edges(monkeypatch):
    """The second anchor's own edges (a country's located messages) under
    belief time: retracted at tt 5, moved at tt 7, corrected at tt 6; the
    split's answers follow them exactly as the given plan's."""
    a = _world(7)
    seen = set()
    for setting in ("asof3", "asof5", "asof6", "asof7", "current", "window", "window_asof5"):
        args = _ic3_like(SCOPES[setting], 0, 8, rows=True)
        monkeypatch.setenv("GNOMON_MATCH_SECOND_ANCHOR", "0")
        want = _answer(a, args)
        monkeypatch.setenv("GNOMON_MATCH_SECOND_ANCHOR", "force")
        assert _answer(a, args) == want, setting
        seen.add(want)
    # the belief times give different answers, so the test can tell them apart
    assert len(seen) >= 3


def test_rel_ends_are_the_distinct_end_identities():
    a = _world(3, 40)
    got = {r: (c, s, d) for r, c, s, d in a.match_rel_ends()}
    assert set(got) == {"IS_PART_OF", "IS_LOCATED_IN", "KNOWS", "HAS_CREATOR"}
    # IS_PART_OF: 8 versions, 8 cities, 4 countries
    assert got["IS_PART_OF"] == (8, 8, 4)
    # HAS_CREATOR: one per message (900 sources)
    assert got["HAS_CREATOR"][0] == 900 and got["HAS_CREATOR"][1] == 900
    # IS_LOCATED_IN: people and messages at the source, cities and countries at the target
    c, s, d = got["IS_LOCATED_IN"]
    assert s == 41 + 900 and d <= 8 + 4 and c >= 41 + 900 + 40
    # cached per generation: a commit is seen
    _commit(a, [_edge("c0", "X", "IS_PART_OF", disc="9")], 8)
    assert {r: (c, s, d) for r, c, s, d in a.match_rel_ends()}["IS_PART_OF"] == (9, 8, 4)


def _jobs(seed: int):
    """People who know each other, companies located in four countries, and
    people working at companies since a year (an edge property); some jobs
    retracted at tt 5."""
    rng = random.Random(seed)
    a = _native()
    people = ["p"] + [f"q{i}" for i in range(150)]
    ops = [_node(c, "Country", name=c) for c in COUNTRIES]
    ops += [_node(f"co{i}", "Company", name=f"co{i}") for i in range(30)]
    ops += [_node(x, "Person", firstName=x) for x in people]
    _commit(a, ops, 1)
    ops = [_edge(f"co{i}", COUNTRIES[i % 4], "IS_LOCATED_IN") for i in range(30)]
    for i, x in enumerate(people):
        for y in rng.sample(people, 4):
            if y != x:
                ops.append(_edge(x, y, "KNOWS", disc=f"{i}"))
    jobs = []
    for i, x in enumerate(people):
        for k in range(rng.randrange(0, 3)):
            c = f"co{rng.randrange(30)}"
            e = _edge(x, c, "WORK_AT", disc=f"{k}")
            e["props"] = {"workFrom": rng.randrange(2000, 2012)}
            ops.append(e)
            jobs.append((x, c, f"{k}"))
    _commit(a, ops, 2)
    _commit(a, [_retract_edge(x, c, "WORK_AT", k, 0) for x, c, k in rng.sample(jobs, len(jobs) // 8)], 5)
    return a


def _ic11_like(scope: dict, country: str = "X", year: int = 2008) -> dict:
    """IC11's shape: friends and friends of friends working at a company in
    a country since before a year (a predicate on the step's edge
    variable), as rows."""
    return {"pattern": {
        "nodes": [{"var": "p", "labels": ["Person"], "uids": ["p"]}, {"var": "f", "labels": ["Person"]},
                  {"var": "co", "labels": ["Company"]}, {"var": "cn", "labels": ["Country"]}],
        "steps": [{"from": "p", "to": "f", "rel_type": "KNOWS", "dir": "both", "hops": {"min": 1, "max": 2}},
                  {"from": "f", "to": "co", "rel_type": "WORK_AT", "dir": "out", "var": "w"},
                  {"from": "co", "to": "cn", "rel_type": "IS_LOCATED_IN", "dir": "out"}],
        "where": [{"cmp": "=", "l": {"tprop": ["cn", "name"], "tau": "str"}, "r": {"lit": country}},
                  {"cmp": "<", "l": {"tprop": ["w", "workFrom"], "tau": "int"}, "r": {"lit": year}}]},
        "return": {"rows": [{"as": "personId", "expr": {"col": "f.uid"}},
                            {"as": "organizationName", "expr": {"tprop": ["co", "name"], "tau": "str"}},
                            {"as": "workFrom", "expr": {"tprop": ["w", "workFrom"], "tau": "int"}}],
                   "order_by": [{"expr": {"col": "workFrom"}, "dir": "asc"},
                                {"expr": {"col": "personId"}, "dir": "asc"}], "limit": 10},
        "scope": scope}


@pytest.mark.parametrize("setting", sorted(SCOPES))
def test_the_second_anchor_with_an_edge_variable_predicate(setting, monkeypatch):
    """A predicate on a step's edge variable goes with that step's side."""
    a = _jobs(11)
    for country, year in (("X", 2008), ("Y", 2012), ("W", 2001)):
        args = _ic11_like(SCOPES[setting], country, year)
        got = {}
        for v in ("0", "1", "force"):
            monkeypatch.setenv("GNOMON_MATCH_SECOND_ANCHOR", v)
            got[v] = _answer(a, args)
        assert got["0"] == got["1"] == got["force"], (setting, country)
        plans = _plans(a, args, monkeypatch)
        assert plans["force"].count("Join") == plans["0"].count("Join") + 1, plans


def _card(given_fan: float, ends_ok: bool = True, deg: dict | None = None) -> dict:
    labels = {"Person": 1000, "City": 50, "Country": 10, "Post": 100_000}
    ends = {"KNOWS": (20_000.0, 1000.0, 1000.0), "IS_LOCATED_IN": (101_000.0, 101_000.0, 60.0),
            "IS_PART_OF": (50.0, 50.0, 10.0), "HAS_CREATOR": (given_fan * 1000, given_fan * 1000, 1000.0)}
    if not ends_ok:
        ends.pop("IS_PART_OF")
    card = {"labels": labels, "rels": {k: v[0] for k, v in ends.items()}, "nodes": 101_060, "ends": ends}
    if deg is not None:
        # the probe: each anchor's first-hop rows
        card["probe"] = lambda var, way: deg.get(var)
    return card


def test_the_split_is_taken_only_where_its_estimate_wins(monkeypatch):
    monkeypatch.setenv("GNOMON_MATCH_SECOND_ANCHOR", "1")
    pat = _ic3_like(SCOPES["current"])["pattern"]
    pat = {**pat, "where": pat["where"] + OM._implied(pat["where"])}
    even = {"p": 20.0, "mc": 500.0}
    # many messages per friend: the given plan reads them all
    got = OM.second_anchor(pat, _card(100.0, deg=even))
    assert got is not None and (got["j"], got["b"]) == ("f", "mc"), got
    assert got["given_cost"] > OM.SECOND_ANCHOR_FACTOR * got["cost"]
    # the averages alone never decide: without a probe, the given plan
    assert OM.second_anchor(pat, _card(100.0)) is None
    # a probe that cannot answer for an anchor: the given plan
    assert OM.second_anchor(pat, _card(100.0, deg={"p": 20.0})) is None
    # a hub anchor (its first hop far above the average): the given plan
    assert OM.second_anchor(pat, _card(100.0, deg={"p": 20.0, "mc": 5_000_000.0})) is None
    # one message per friend: the given plan is cheap
    assert OM.second_anchor(pat, _card(1.0, deg=even)) is None
    # a relationship without its end counts: no estimate, the given plan
    assert OM.second_anchor(pat, _card(100.0, ends_ok=False, deg=even)) is None
    # no node pinned by literal values on the far side: the given plan
    unpinned = {**pat, "where": [w for w in pat["where"] if "mc" not in OM._vars_in(w)]}
    assert OM.second_anchor(unpinned, _card(100.0, deg=even)) is None
    # a predicate that can raise is never moved
    raising = {**pat, "where": pat["where"] + [{"cmp": ">", "l": {"arith": "/", "l": {"col": "m.vt_s"},
                                                                    "r": {"lit": 0}}, "r": {"lit": 1}}]}
    assert OM.second_anchor(raising, _card(100.0, deg=even)) is None
    monkeypatch.setenv("GNOMON_MATCH_SECOND_ANCHOR", "0")
    assert OM.second_anchor(pat, _card(100.0, deg=even)) is None


def test_the_probe_reads_the_adjacency_rows():
    a = _world(5, 30)
    # a city: one IS_PART_OF out-edge
    assert a.match_adjacency_rows(["c0"], "out") == 1
    # a country: every message ever located in it (versions retracted or
    # moved stay rows of the index) and its cities, at most every row
    xs = a.match_adjacency_rows(["X"], "in")
    assert xs >= 2 and xs <= a.match_adjacency_rows(["X", "Y", "Z", "W"], "in")
    assert a.match_adjacency_rows(["X"], "both") == xs + a.match_adjacency_rows(["X"], "out")
    assert a.match_adjacency_rows(["no-such-uid"], "both") == 0


# ---- the fused edge scan (GNOMON_MATCH_SCAN_FUSE) ------------------------------

from .test_sf10_fixes_1 import SCOPES as W49_SCOPES, _friends_posts, _ic9_like  # noqa: E402


def _created(scope: dict) -> dict:
    """BIH-R5's shape: per person with a name, the messages they created that
    share an instant with them, counted (an edge scan joined to the persons,
    then to the messages, under a window)."""
    return {"pattern": {"nodes": [{"var": "p", "labels": ["Person"]}, {"var": "m", "labels": ["Post"]}],
                        "steps": [{"from": "m", "to": "p", "rel_type": "HAS_CREATOR", "dir": "out"}],
                        "where": [{"cmp": "!=", "l": {"tprop": ["p", "name"], "tau": "str"}, "r": {"lit": "f3"}}]},
            "return": {"group": {"by": [{"as": "id", "expr": {"col": "p.uid"}}], "aggs": [{"as": "n", "fn": "count"}]},
                       "order_by": [{"expr": {"col": "n"}, "dir": "desc"}, {"expr": {"col": "id"}, "dir": "asc"}],
                       "limit": 50},
            "scope": scope}


def _fuse_same(a, args, monkeypatch):
    got = {}
    for v in ("0", "1"):
        monkeypatch.setenv("GNOMON_MATCH_SCAN_FUSE", v)
        got[v] = _answer(a, args)
    assert got["0"] == got["1"], args
    return got["1"]


@pytest.mark.parametrize("setting", sorted(W49_SCOPES))
def test_the_fused_edge_scan_keeps_the_rows(setting, monkeypatch):
    # 1,300 friends: past the 1,024-key probe, the hop by one scan and the
    # join that chose the scan
    a = _friends_posts(11, 1300, 3)
    sc = W49_SCOPES[setting]
    _fuse_same(a, _ic9_like("date", 10_000, sc), monkeypatch)
    _fuse_same(a, _ic9_like("score", 25, sc), monkeypatch)
    _fuse_same(a, _created(sc), monkeypatch)
    _fuse_same(a, {**_created(sc), "pattern": {**_created(sc)["pattern"], "where": []}}, monkeypatch)


@pytest.mark.parametrize("setting", sorted(SCOPES))
def test_the_fused_edge_scan_keeps_the_rows_under_belief_time(setting, monkeypatch):
    a = _world(13)
    for second in ("0", "1"):
        monkeypatch.setenv("GNOMON_MATCH_SECOND_ANCHOR", second)
        _fuse_same(a, _ic3_like(SCOPES[setting], 0, 8, rows=True), monkeypatch)


# ---- the unique-key join, the one-pass interval chain, the node seek's label pass

#: switch -> its old-path value
OLD = {"GNOMON_MATCH_UNIQUE_JOIN": "0", "GNOMON_MATCH_INTERVAL_FUSE": "0", "GNOMON_MATCH_TYPE_PASS": "serial"}


def _old_paths_same(a, args, monkeypatch):
    got = {}
    for name in [None, *OLD, "all"]:
        for k in OLD:
            monkeypatch.delenv(k, raising=False)
        if name == "all":
            for k, v in OLD.items():
                monkeypatch.setenv(k, v)
        elif name is not None:
            monkeypatch.setenv(name, OLD[name])
        got[name] = _answer(a, args)
    assert len(set(got.values())) == 1, (args, {k: v[:120] for k, v in got.items()})
    return got[None]


@pytest.mark.parametrize("setting", sorted(W49_SCOPES))
def test_the_unique_join_and_the_interval_pass_keep_the_rows(setting, monkeypatch):
    # every row loop on the pool from 64 rows (read when the handle opens),
    # so the joins and passes past `par_min()` run at this store's size
    monkeypatch.setenv("GNOMON_MATCH_PAR_ROWS", "64")
    a = _friends_posts(17, 1300, 3)
    sc = W49_SCOPES[setting]
    _old_paths_same(a, _created(sc), monkeypatch)
    _old_paths_same(a, {**_created(sc), "pattern": {**_created(sc)["pattern"], "where": []}}, monkeypatch)
    _old_paths_same(a, _ic9_like("date", 10_000, sc), monkeypatch)


@pytest.mark.parametrize("setting", sorted(SCOPES))
def test_the_unique_join_and_the_interval_pass_keep_the_rows_under_belief_time(setting, monkeypatch):
    monkeypatch.setenv("GNOMON_MATCH_PAR_ROWS", "64")
    a = _world(23)
    for second in ("0", "force"):
        monkeypatch.setenv("GNOMON_MATCH_SECOND_ANCHOR", second)
        _old_paths_same(a, _ic3_like(SCOPES[setting], 0, 8, rows=True), monkeypatch)
        _old_paths_same(a, _ic3_like(SCOPES[setting], 2, 6), monkeypatch)


# ---- the join key index's mixed keys (GNOMON_MATCH_KEY_MIX)


def _triangles(scope: dict) -> dict:
    """A closing hop under a window: joined on both of its ends (two identity
    keys packed in one), friends of friends who know the person back."""
    return {"pattern": {"nodes": [{"var": "a", "labels": ["Person"]}, {"var": "b", "labels": ["Person"]},
                                  {"var": "c", "labels": ["Person"]}],
                        "steps": [{"from": "a", "to": "b", "rel_type": "KNOWS", "dir": "out"},
                                  {"from": "b", "to": "c", "rel_type": "KNOWS", "dir": "out"},
                                  {"from": "c", "to": "a", "rel_type": "KNOWS", "dir": "out"}]},
            "return": {"group": {"by": [{"as": "a", "expr": {"col": "a.uid"}}], "aggs": [{"as": "n", "fn": "count"}]},
                       "order_by": [{"expr": {"col": "n"}, "dir": "desc"}, {"expr": {"col": "a"}, "dir": "asc"}],
                       "limit": 50},
            "scope": scope}


@pytest.mark.parametrize("setting", sorted(SCOPES))
def test_the_mixed_join_keys_keep_the_rows(setting, monkeypatch):
    a = _world(29, 200)
    for args in (_triangles(SCOPES[setting]), _ic3_like(SCOPES[setting], 0, 8, rows=True)):
        got = {}
        for v in ("0", "1"):
            monkeypatch.setenv("GNOMON_MATCH_KEY_MIX", v)
            got[v] = _answer(a, args)
        assert got["0"] == got["1"], (setting, args["pattern"]["steps"][0])
