"""`optional`/`not` sub-patterns under a valid-time window (with
`co_valid: [valid]`) or under a non-current belief mode.

* a hand-built store whose answers are stated by hand (instant sets that
  split, a node that stops existing mid-walk, a remainder with nulls);
* a window of width one answers what the instant does;
  reference route against the oracle (which evaluates the instant semantics
  at every elementary instant and takes the union), and the engine's
  executor against the reference route;
* the forms that stay refused.
"""

from __future__ import annotations

import itertools
import os
import random
from typing import Any

import pytest

from gnomon.core.errors import InvalidArgError
from gnomon.core.model import OPEN_END, canonical_json
from gnomon.temporal import ops_match as OM
from gnomon.temporal.algebra import ensure_all_registered
from gnomon.temporal.ops_match import compile_match, run, validate
from gnomon.temporal.oracle import Oracle

from . import test_match_oracle as T
from .conftest import fresh_adapter

EXAMPLES = int(os.environ.get("GNOMON_HYP_EXAMPLES", "25"))
NATIVE = os.environ.get("GNOMON_TEST_BACKEND", "native") == "native"


def reference(adapter, args):
    return run(adapter, validate(args), route="reference", reference_bypass="test store")


# ---------------------------------------------------------------------------
# a hand-built store
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def timeline():
    """Persons a, b, c, d valid throughout and x valid on [0, 12); KNOWS
    a-b [0,10), b-c [5,20), a-c [15,30), c-d [0,40), a-x [0,40), x-d [0,40)."""
    ensure_all_registered()
    a = fresh_adapter()
    T._apply(a, [T.node(u, "P", {"w": i}) for i, u in enumerate("abcd")]
             + [T.node("x", "P", {"w": 9}, 0, 12)], 1)
    T._apply(a, [T.edge(s, d, "KNOWS", vt_s=lo, vt_e=hi) for s, d, lo, hi in (
        ("a", "b", 0, 10), ("b", "c", 5, 20), ("a", "c", 15, 30), ("c", "d", 0, 40),
        ("a", "x", 0, 40), ("x", "d", 0, 40))], 2)
    oracle = Oracle(list(a.all_node_versions()), list(a.all_edge_versions()))
    yield a, oracle
    a.close()


def W(t_a: int, t_b: int) -> dict:
    return {"vt": {"window": {"t_a": t_a, "t_b": t_b}}, "co_valid": ["valid"]}


def friends(hops: dict, scope: dict) -> dict:
    return {"pattern": {"nodes": [{"var": "p", "uids": ["a"]}, {"var": "f", "labels": ["P"]}],
                        "steps": [{"from": "p", "to": "f", "rel_type": "KNOWS", "dir": "both",
                                   "hops": hops}]},
            "return": {"rows": [{"as": "f", "expr": {"col": "f.uid"}},
                                {"as": "d", "expr": {"col": "f.depth"}}]},
            "scope": scope}


HAND: dict[str, tuple[dict[str, Any], list[dict[str, Any]]]] = {
    # c is at distance 2 on [5,10) and 1 on [15,30): the least is reported;
    # d is reached through x only while x exists, and through c later
    "friends within two, the least distance": (
        friends({"min": 1, "max": 2}, W(0, 30)),
        [{"f": "b", "d": 1}, {"f": "c", "d": 1}, {"f": "d", "d": 2}, {"f": "x", "d": 1}]),
    # the band is tested per instant: b and c are at distance 2 at some
    # instant, x never is
    "exactly two away at some instant": (
        friends({"min": 2, "max": 2}, W(0, 30)),
        [{"f": "b", "d": 2}, {"f": "c", "d": 2}, {"f": "d", "d": 2}]),
    "a narrower window keeps only its instants": (
        friends({"min": 1, "max": 2}, W(0, 12)),
        [{"f": "b", "d": 1}, {"f": "c", "d": 2}, {"f": "d", "d": 2}, {"f": "x", "d": 1}]),
    # walks of two edges, one per version tuple; a walk may reuse an edge
    "two-hop walks with multiplicity": ({
        "pattern": {"nodes": [{"var": "p", "uids": ["a"]}, {"var": "f"}],
                    "steps": [{"from": "p", "to": "f", "rel_type": "KNOWS", "dir": "both",
                               "hops": {"exact": 2}}]},
        "return": {"group": {"by": [{"as": "f", "expr": {"col": "f.uid"}}],
                             "aggs": [{"as": "n", "fn": "count"}]}},
        "scope": W(0, 30)},
        [{"f": "a", "n": 3}, {"f": "b", "n": 1}, {"f": "c", "n": 1}, {"f": "d", "n": 2}]),
    # b has no outgoing KNOWS on [0,5) and [20,30), a none on [12,15) (x is
    # gone, a-c not yet valid); x's edge covers all of x
    "not: some instant without an extension": ({
        "pattern": {"nodes": [{"var": "p", "labels": ["P"]}],
                    "not": [{"steps": [{"from": "p", "to": "q", "rel_type": "KNOWS"}]}]},
        "return": {"rows": [{"as": "p", "expr": {"col": "p.uid"}}]},
        "scope": W(0, 30)},
        [{"p": "a"}, {"p": "b"}, {"p": "d"}]),
    # a's extensions cover [0,12) and [15,30): the remainder [12,15) keeps nulls
    "optional: extensions, then the uncovered remainder with nulls": ({
        "pattern": {"nodes": [{"var": "p", "uids": ["a"]}],
                    "optional": [{"steps": [{"from": "p", "to": "q", "rel_type": "KNOWS"}]}]},
        "return": {"rows": [{"as": "q", "expr": {"col": "q.uid"}}]},
        "scope": W(0, 30)},
        [{"q": None}, {"q": "b"}, {"q": "c"}, {"q": "x"}]),
    "optional: no remainder, no null row": ({
        "pattern": {"nodes": [{"var": "p", "uids": ["a"]}],
                    "optional": [{"steps": [{"from": "p", "to": "q", "rel_type": "KNOWS"}]}]},
        "return": {"rows": [{"as": "q", "expr": {"col": "q.uid"}}]},
        "scope": W(0, 12)},
        [{"q": "b"}, {"q": "x"}]),
    "a closure step whose far end is bound": ({
        "pattern": {"nodes": [{"var": "p", "uids": ["a"]}, {"var": "q", "uids": ["d"]}],
                    "steps": [{"from": "p", "to": "q", "rel_type": "KNOWS",
                               "hops": {"min": 1, "max": None}}]},
        "return": {"rows": [{"as": "q", "expr": {"col": "q.uid"}}]},
        "scope": W(12, 15)},
        []),
}


@pytest.mark.parametrize("name", sorted(HAND))
def test_hand_corpus(timeline, name):
    a, oracle = timeline
    args, want = HAND[name]
    got = reference(a, args)
    assert got["rows"] == want
    assert canonical_json(oracle.match(validate(args))) == canonical_json(got)
    if NATIVE:
        before = OM.ROUTES["native"]
        nat = run(a, validate(args), route="native")
        assert OM.ROUTES["native"] > before, OM.LAST_DECLINE[0]
        assert canonical_json(nat) == canonical_json(got)


# ---------------------------------------------------------------------------
# a window of width one is the instant
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def social():
    a, oracle = T.build_social()
    yield a, oracle
    a.close()


def test_a_width_one_window_answers_as_the_instant(social):
    a, _ = social
    calls = [
        {"pattern": {"nodes": [{"var": "p", "uids": ["p4"]}, {"var": "f"}],
                     "steps": [{"from": "p", "to": "f", "rel_type": "KNOWS", "dir": "both",
                                "hops": {"min": 1, "max": 3}}]},
         "return": {"rows": [{"as": "f", "expr": {"col": "f.uid"}},
                             {"as": "d", "expr": {"col": "f.depth"}}]}},
        {"pattern": {"nodes": [{"var": "m", "uids": ["c202"]}, {"var": "r", "labels": ["Post"]}],
                     "steps": [{"from": "m", "to": "r", "rel_type": "REPLY_OF",
                                "hops": {"min": 0, "max": None}}]},
         "return": {"rows": [{"as": "r", "expr": {"col": "r.uid"}}]}},
        {"pattern": {"nodes": [{"var": "p", "labels": ["Person"]}],
                     "optional": [{"nodes": [{"var": "m"}],
                                   "steps": [{"from": "m", "to": "p", "rel_type": "HAS_CREATOR"}]}]},
         "return": {"rows": [{"as": "p", "expr": {"col": "p.uid"}},
                             {"as": "m", "expr": {"col": "m.uid"}}]}},
        {"pattern": {"nodes": [{"var": "p", "labels": ["Person"]}],
                     "not": [{"nodes": [{"var": "m"}],
                              "steps": [{"from": "m", "to": "p", "rel_type": "HAS_CREATOR"}]}]},
         "return": {"rows": [{"as": "p", "expr": {"col": "p.uid"}}]}},
        {"pattern": {"nodes": [{"var": "p", "uids": ["p1"]}, {"var": "f"}],
                     "steps": [{"from": "p", "to": "f", "rel_type": "KNOWS", "dir": "both",
                                "hops": {"exact": 2}}]},
         "return": {"group": {"by": [{"as": "f", "expr": {"col": "f.uid"}}],
                              "aggs": [{"as": "n", "fn": "count"}]}}},
    ]
    for t in (21, 25, 40):
        for q in calls:
            at = reference(a, {**q, "scope": {"vt": {"instant": t}}})
            win = reference(a, {**q, "scope": W(t, t + 1)})
            assert win["rows"] == at["rows"], (t, q)


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------

HOPS = [{"exact": 1}, {"exact": 1}, {"exact": 1}, {"exact": 2}, {"min": 0, "max": 2},
        {"min": 1, "max": None}, {"min": 2, "max": 3}]


def shared_instant_args(rng: random.Random, max_tt: int) -> dict[str, Any]:
    as_of = rng.choice([OPEN_END, rng.randrange(1, max_tt + 1)])
    k = rng.random()
    if k < 0.55:
        t_a = rng.choice([0, rng.randrange(0, 40)])
        scope: dict[str, Any] = {"vt": {"window": {"t_a": t_a,
                                                   "t_b": rng.choice([OPEN_END, t_a + rng.randrange(1, 30)])}},
                                 "co_valid": ["valid"], "as_of_tt": as_of,
                                 "belief": rng.choice(["current", "current", "all", "superseded"])}
    elif k < 0.8:
        scope = {"vt": {"instant": rng.randrange(0, T.T_MAX)}, "as_of_tt": as_of,
                 "belief": rng.choice(["all", "superseded"])}
    else:
        t = rng.randrange(0, T.T_MAX)
        scope = {"vt": {"window": {"t_a": t, "t_b": t + 1}}, "co_valid": ["valid"],
                 "as_of_tt": as_of}
    n_vars = rng.randint(1, 4)
    vs = [f"v{i}" for i in range(n_vars)]
    nodes = []
    for v in vs:
        nd: dict[str, Any] = {"var": v}
        if rng.random() < 0.35:
            nd["labels"] = rng.sample(T.LABELS, rng.randint(1, 2))
        if rng.random() < 0.2:
            nd["uids"] = rng.sample(T.UIDS, rng.randint(1, 3))
        nodes.append(nd)
    steps = []
    targets: list[str] = []
    for i in range(1, n_vars):
        other = vs[rng.randrange(i)]
        a, b = (other, vs[i]) if rng.random() < 0.5 else (vs[i], other)
        hops = rng.choice(HOPS)
        st: dict[str, Any] = {"from": a, "to": b, "rel_type": rng.choice(T.RELS + [None]),
                              "dir": rng.choice(["out", "in", "both"]), "hops": hops}
        if hops == {"exact": 1} and rng.random() < 0.5:
            st["var"] = f"e{i}"
        if "min" in hops:
            targets.append(vs[i])
        steps.append(st)
    if n_vars >= 3 and rng.random() < 0.3:                 # a closing step
        steps.append({"from": vs[-1], "to": vs[0], "rel_type": rng.choice(T.RELS),
                      "dir": rng.choice(["out", "both"]),
                      "hops": rng.choice([{"exact": 1}, {"min": 1, "max": 2}])})
    where = []
    if rng.random() < 0.4:
        v = rng.choice(vs)
        where.append({"cmp": rng.choice(["<", ">=", "=", "!="]),
                      "l": {"tprop": [v, "w"], "tau": "int"}, "r": {"lit": rng.randrange(5)}})
    if rng.random() < 0.2:
        where.append({"cmp": "<=", "l": {"col": f"{rng.choice(vs)}.vt_s"},
                      "r": {"lit": rng.randrange(T.T_MAX)}})
    pattern: dict[str, Any] = {"nodes": nodes, "steps": steps, "where": where}
    sub_vars: list[str] = []
    if rng.random() < 0.45:
        key = "optional" if rng.random() < 0.55 else "not"
        sh = rng.choice(vs)
        sub_hops = rng.choice([{"exact": 1}, {"exact": 1}, {"min": 1, "max": 2}])
        sst: dict[str, Any] = {"from": sh, "to": "x9", "rel_type": rng.choice(T.RELS),
                               "dir": rng.choice(["out", "in", "both"]), "hops": sub_hops}
        if sub_hops == {"exact": 1} and rng.random() < 0.5:
            sst["var"] = "o1"
        sub: dict[str, Any] = {"steps": [sst], "where": []}
        if rng.random() < 0.3:
            sub["where"].append({"cmp": ">", "l": {"tprop": ["x9", "w"], "tau": "int"},
                                 "r": {"lit": rng.randrange(4)}})
        pattern[key] = [sub]
        if key == "optional":
            sub_vars.append("x9")
            if "min" in sub_hops:
                targets.append("x9")
    edge_vars = [s["var"] for s in steps if s.get("var")]
    cols = [f"{v}.uid" for v in vs + sub_vars] + [f"{e}.eid" for e in edge_vars] + \
        [f"{v}.vt_s" for v in vs] + [f"{v}.depth" for v in targets]
    if rng.random() < 0.6:
        picked = rng.sample(cols, rng.randint(1, min(4, len(cols))))
        rows = [{"as": f"c{i}", "expr": {"col": c}} for i, c in enumerate(picked)]
        if rng.random() < 0.3:
            rows.append({"as": "w", "expr": {"tprop": [rng.choice(vs), "w"], "tau": "int"}})
        ret: dict[str, Any] = {"rows": rows}
        if rng.random() < 0.4:
            ret["order_by"] = [{"expr": {"col": rows[0]["as"]},
                                "dir": rng.choice(["asc", "desc"])}]
    else:
        by = [{"as": "g", "expr": {"col": f"{rng.choice(vs)}.uid"}}] if rng.random() < 0.7 else []
        v = rng.choice(vs)
        aggs = [{"as": "n", "fn": "count"},
                {"as": "d", "fn": "count_distinct", "of": {"col": f"{rng.choice(vs)}.uid"}},
                {"as": "s", "fn": "sum", "of": {"tprop": [v, "w"], "tau": "int"}}]
        if targets:
            aggs.append({"as": "md", "fn": "min", "of": {"col": f"{rng.choice(targets)}.depth"}})
        ret = {"group": {"by": by, "aggs": aggs}}
    if rng.random() < 0.3:
        ret["limit"] = rng.randint(1, 4)
    return {"pattern": pattern, "return": ret, "scope": scope,
            "limit": rng.choice([100, 2]), "cursor": rng.choice([None, None, "1"])}


#: compared pairs per test, for the lane report (`pytest -s` prints them)
COUNTS: dict[str, int] = {"oracle": 0, "native": 0, "refused": 0}


def _compiles(args: dict[str, Any]) -> bool:
    try:
        compile_match(validate(args))
    except InvalidArgError:
        COUNTS["refused"] += 1
        return False
    return True


@pytest.mark.parametrize("case", range(EXAMPLES))
def test_random_shared_instant_arguments_agree_with_the_oracle(case):
    ensure_all_registered()
    a, oracle, max_tt = T.random_store(case % 5)
    rng = random.Random(7000 + case)
    compared = 0
    for _ in range(16):
        args = shared_instant_args(rng, max_tt)
        if not _compiles(args):
            continue
        try:
            got = reference(a, args)
        except InvalidArgError:
            with pytest.raises(InvalidArgError):
                oracle.match(validate(args))
            continue
        want = oracle.match(validate(args))
        assert canonical_json(got) == canonical_json(want), args
        compared += 1
    COUNTS["oracle"] += compared
    assert compared > 0


@pytest.mark.skipif(not NATIVE, reason="the engine executor is the native backend's")
@pytest.mark.parametrize("case", range(EXAMPLES))
def test_random_shared_instant_arguments_native_equals_reference(case):
    ensure_all_registered()
    a, _oracle, max_tt = T.random_store(case % 5)
    rng = random.Random(9000 + case)
    compared = 0
    for _ in range(16):
        args = shared_instant_args(rng, max_tt)
        if not _compiles(args):
            continue
        filled = validate(args)
        try:
            ref = run(a, filled, route="reference", reference_bypass="test store")
        except InvalidArgError:
            continue
        before = OM.ROUTES["native"]
        got = run(a, filled, route="native")
        assert OM.ROUTES["native"] > before, (OM.LAST_DECLINE[0], args)
        assert canonical_json(got) == canonical_json(ref), args
        compared += 1
    COUNTS["native"] += compared
    assert compared > 0


def test_report_counts():
    """Runs last in the module: the compared totals, for the lane report."""
    print(f"\nW1 random comparisons: {COUNTS}")


# ---------------------------------------------------------------------------
# what stays refused
# ---------------------------------------------------------------------------

def _walk(scope: dict) -> dict:
    return friends({"min": 1, "max": 2}, scope)


@pytest.mark.parametrize("scope,why", [
    ({"vt": {"window": {"t_a": 0, "t_b": 30}}}, "co_valid"),
    ({"vt": {"window": {"t_a": 0, "t_b": 30}, "mode": "event"}, "co_valid": ["valid"]},
     "event mode"),
    ({"vt": {"window": {"t_a": 0, "t_b": 30}}, "belief": "all",
      "co_valid": ["valid", "transaction"]}, "transaction"),
])
def test_walks_outside_shared_instant_refuse(scope, why):
    with pytest.raises(InvalidArgError, match=why):
        compile_match(validate(_walk(scope)))


def test_sub_patterns_outside_shared_instant_refuse():
    args = {"pattern": {"nodes": [{"var": "p"}],
                        "not": [{"steps": [{"from": "p", "to": "q", "rel_type": "KNOWS"}]}]},
            "return": {"rows": [{"as": "p", "expr": {"col": "p.uid"}}]},
            "scope": {"vt": {"window": {"t_a": 0, "t_b": 30}}}}
    with pytest.raises(InvalidArgError, match="co_valid"):
        compile_match(validate(args))


def test_the_common_interval_is_not_defined_with_a_walk():
    args = friends({"min": 1, "max": 2}, W(0, 30))
    args["return"]["rows"].append({"as": "s", "expr": {"col": "valid.s"}})
    with pytest.raises(InvalidArgError, match="valid.s"):
        compile_match(validate(args))


def test_dir_both_on_one_hop_is_accepted_under_any_window(timeline):
    a, oracle = timeline
    args = {"pattern": {"nodes": [{"var": "p", "uids": ["c"]}, {"var": "q"}],
                        "steps": [{"from": "p", "to": "q", "rel_type": "KNOWS", "dir": "both"}]},
            "return": {"rows": [{"as": "q", "expr": {"col": "q.uid"}}]},
            "scope": {"vt": {"window": {"t_a": 0, "t_b": 30}}}}
    got = reference(a, args)
    assert got["rows"] == [{"q": "a"}, {"q": "b"}, {"q": "d"}]
    assert canonical_json(oracle.match(validate(args))) == canonical_json(got)


# --------------------------------------------------------------------------- #
# crossovers: the engine against the reference route                          #
# --------------------------------------------------------------------------- #

@pytest.fixture(scope="module")
def wide():
    """20000 persons (some ending mid-window) and 6000 KNOWS / LIKES edges
    with random intervals: past 1/64 of the identities (the index join's
    limit) and past 256 ids (the point reads' limit) with a few anchors
    under both."""
    ensure_all_registered()
    rng = random.Random(29)
    a = fresh_adapter()
    nodes = [T.node(f"n{i}", "P", {"w": i % 7}, 0, OPEN_END if i % 5 else rng.randrange(5, 40))
             for i in range(20000)]
    T._apply(a, nodes, 1)
    edges = []
    for i in range(6000):
        u, v = rng.randrange(0, 3000), rng.randrange(0, 3000)
        lo = rng.randrange(0, 35)
        edges.append(T.edge(f"n{u}", f"n{v}", rng.choice(["KNOWS", "LIKES"]),
                            vt_s=lo, vt_e=rng.choice([OPEN_END, lo + rng.randrange(1, 20)]),
                            disc=str(i)))
    T._apply(a, edges, 2)
    yield a
    a.close()


WIDE = [
    # closing steps and dir both, unanchored (every join on many keys)
    {"pattern": {"nodes": [{"var": "x", "labels": ["P"]}, {"var": "y", "labels": ["P"]},
                           {"var": "z", "labels": ["P"]}],
                 "steps": [{"from": "x", "to": "y", "rel_type": "KNOWS", "dir": "both"},
                           {"from": "y", "to": "z", "rel_type": "LIKES"},
                           {"from": "z", "to": "x", "rel_type": "KNOWS", "dir": "both"}]},
     "return": {"group": {"by": [{"as": "x", "expr": {"col": "x.uid"}}],
                          "aggs": [{"as": "n", "fn": "count"}]},
                "order_by": [{"expr": {"col": "n"}, "dir": "desc"}, {"expr": {"col": "x"}}]}},
    # anchored on a few: the joins seek
    {"pattern": {"nodes": [{"var": "x", "labels": ["P"], "uids": ["n1", "n2", "n3", "n40"]},
                           {"var": "y", "labels": ["P"]}, {"var": "z", "labels": ["P"]}],
                 "steps": [{"from": "x", "to": "y", "rel_type": "KNOWS", "dir": "both", "var": "k"},
                           {"from": "z", "to": "y", "rel_type": "LIKES", "var": "l"}]},
     "return": {"rows": [{"as": "x", "expr": {"col": "x.uid"}}, {"as": "y", "expr": {"col": "y.uid"}},
                         {"as": "z", "expr": {"col": "z.uid"}}, {"as": "ks", "expr": {"col": "k.vt_s"}}]}},
    {"pattern": {"nodes": [{"var": "x", "labels": ["P"]}, {"var": "y", "labels": ["P"]}],
                 "steps": [{"from": "x", "to": "y", "rel_type": "KNOWS", "dir": "both",
                            "hops": {"min": 1, "max": 3}}]},
     "return": {"group": {"by": [{"as": "d", "expr": {"col": "y.depth"}}],
                          "aggs": [{"as": "n", "fn": "count"}]},
                "order_by": [{"expr": {"col": "d"}}]}},
    # an optional and a not over the same relations
    {"pattern": {"nodes": [{"var": "x", "labels": ["P"]}, {"var": "y", "labels": ["P"]}],
                 "steps": [{"from": "x", "to": "y", "rel_type": "LIKES"}],
                 "optional": [{"steps": [{"from": "y", "to": "x", "rel_type": "KNOWS", "dir": "both",
                                          "var": "kk"}]}],
                 "not": [{"nodes": [{"var": "z", "labels": ["P"]}],
                          "steps": [{"from": "y", "to": "z", "rel_type": "LIKES"}]}]},
     "return": {"group": {"by": [{"as": "x", "expr": {"col": "x.uid"}}],
                          "aggs": [{"as": "n", "fn": "count"}, {"as": "f", "fn": "count", "of": {"col": "kk.eid"}}]},
                "order_by": [{"expr": {"col": "x"}}]}},
]


@pytest.mark.parametrize("i", range(len(WIDE)))
@pytest.mark.parametrize("scope", [W(0, 40), W(10, 25), {**W(0, 40), "as_of_tt": 1},
                                   {"vt": {"instant": 12}}, {"vt": {"instant": 30}}])
def test_index_join_and_wide_walks_equal_the_reference(wide, i, scope):
    """A join on a whole scan seeks its left keys while they are
    few (one per 64 identities, 256 at least) and reads the scan otherwise;
    bitset pass or the node lane; every one gives the reference's rows, in
    its order."""
    args = {**WIDE[i], "scope": scope, "limit": 10000}
    want = reference(wide, args)
    got = OM.match(wide, args)
    assert canonical_json(got["rows"]) == canonical_json(want["rows"])



WIDE_SCOPES = [W(0, 40), W(10, 25), {**W(0, 40), "as_of_tt": 1},
               {"vt": {"instant": 12}}, {"vt": {"instant": 30}}]


@pytest.mark.skipif(not NATIVE, reason="the thread count is the native engine's")
def test_every_thread_count_gives_the_same_rows_in_the_same_order(wide):
    """Step 3: the engine splits a large frontier's adjacency read, a join
    and a projection over `GNOMON_MATCH_THREADS` threads (row loops from
    `GNOMON_MATCH_PAR_ROWS` rows), read when the handle opens and reported by
    `designs()`. A handle on the wide store opened
    with 1, 2 and 32 threads answers every wide pattern, in every scope,
    with the same rows in the same order (the frontier from every person is
    past the split's limit)."""
    from gnomon.storage.native import NativeAdapter

    got: dict[int, list[str]] = {}
    for t in (1, 2, 32):
        # the row loops split from 64 rows (GNOMON_MATCH_PAR_ROWS), so every
        # one of them runs on the pool at this store's size
        env = {"GNOMON_MATCH_THREADS": str(t), "GNOMON_MATCH_PAR_ROWS": "64"}
        old = {k: os.environ.get(k) for k in env}
        os.environ.update(env)
        try:
            h = NativeAdapter(wide.path, read_only=True)
        finally:
            for k, v in old.items():
                if v is None:
                    del os.environ[k]
                else:
                    os.environ[k] = v
        try:
            assert h.designs()["match_threads"] == t
            assert h.designs()["match_par_rows"] == 64
            got[t] = [canonical_json(OM.match(h, {**args, "scope": scope, "limit": 10000})["rows"])
                      for args in WIDE for scope in WIDE_SCOPES]
        finally:
            h.close()
    assert sum(r != "[]" for r in got[1]) > len(got[1]) // 2
    for t in (2, 32):
        diff = [i for i, (a, b) in enumerate(zip(got[1], got[t])) if a != b]
        assert not diff, f"{t} threads answered cases {diff} differently from one"



@pytest.fixture(scope="module")
def corrected():
    """3000 persons asserted at three belief times (three versions each) and
    three out-edges each, asserted at two: a store where `belief: all` and
    `superseded` bind several versions of every node and edge."""
    ensure_all_registered()
    rng = random.Random(32)
    a = fresh_adapter()
    n = 3000
    for tt in (10, 20, 30):
        T._apply(a, [T.node(f"p{i}", "P", {"v": tt}) for i in range(n)], tt)
    edges = [(i, rng.randrange(n), k) for i in range(n) for k in range(3)]
    for tt in (40, 50):
        T._apply(a, [T.edge(f"p{u}", f"p{v}", "KNOWS", {"x": tt}, vt_s=rng.randrange(0, 30), disc=str(k))
                     for u, v, k in edges], tt)
    yield a
    a.close()


def test_a_join_seeks_under_every_belief(corrected):
    """A join on a whole scan under `belief: all` or `superseded` seeks
    its left keys too (the edges through the adjacency index's slices, the
    nodes by point reads or the id bitset), with the scan's own tests on the
    rows found; with the diagonal `where` on the transaction times, every
    direction, window and belief time it gives the reference's rows, in its
    order."""
    rng = random.Random(9)
    for case in range(60):
        lo = rng.randrange(0, 40)
        args = {"pattern": {"nodes": [{"var": "n", "uids": [f"p{rng.randrange(3000)}"]}, {"var": "m"}],
                            "steps": [{"from": "n", "to": "m", "rel_type": "KNOWS",
                                       "dir": rng.choice(["out", "in", "both"]), "var": "e"}],
                            "where": [{"cmp": "<", "l": {"col": "e.tt_s"}, "r": {"col": "e.tt_e"}},
                                      {"cmp": "<", "l": {"col": "e.tt_s"}, "r": {"lit": rng.randrange(35, 60)}},
                                      {"cmp": ">", "l": {"col": "e.tt_e"}, "r": {"lit": rng.randrange(30, 55)}},
                                      {"cmp": "<", "l": {"col": "e.vt_s"}, "r": {"col": "e.tt_e"}}]},
                "return": {"rows": [{"as": "m", "expr": {"col": "m.uid"}},
                                    {"as": "mv", "expr": {"col": "m.vid"}},
                                    {"as": "s", "expr": {"col": "e.tt_s"}}]},
                "scope": {"vt": {"window": {"t_a": lo, "t_b": lo + rng.randrange(1, 40)}},
                          "belief": rng.choice(["all", "superseded"])},
                "limit": 10000}
        if case % 2:
            args["scope"]["as_of_tt"] = rng.choice([15, 25, 45, 55])
        if case % 3 == 0:
            args["pattern"]["where"] = []
        want = reference(corrected, args)
        got = OM.match(corrected, args)
        assert canonical_json(got["rows"]) == canonical_json(want["rows"]), (case, args)



@pytest.fixture(scope="module")
def lopsided():
    """1100 persons, one in twenty with a KNOWS out-edge and one in five with
    a MET one, and 1100 items with ten LIKES out-edges each, among 75000
    other nodes (a join seeks up to one key per 64 identities); every person,
    item and edge asserted at two belief times. A seek from every person
    reads a sliver of the stored edge rows, one from every item most of them."""
    ensure_all_registered()
    rng = random.Random(41)
    a = fresh_adapter()
    T._apply(a, [T.node(f"z{i}", "Z", {"v": 0}) for i in range(75000)], 5)
    edges = ([(f"p{i}", f"p{rng.randrange(1100)}", "KNOWS", "") for i in range(0, 1100, 20)]
             + [(f"p{i}", f"p{rng.randrange(1100)}", "MET", "") for i in range(0, 1100, 5)]
             + [(f"q{i % 1100}", f"q{rng.randrange(1100)}", "LIKES", str(i)) for i in range(11000)])
    starts = [rng.randrange(0, 10) for _ in edges]
    for tt in (10, 20):
        T._apply(a, [T.node(f"p{i}", "P", {"v": tt}) for i in range(1100)]
                 + [T.node(f"q{i}", "Q", {"v": tt}) for i in range(1100)], tt)
    for tt in (30, 40):
        T._apply(a, [T.edge(u, v, r, {"x": tt}, vt_s=t0, disc=k) for (u, v, r, k), t0 in zip(edges, starts)], tt)
    yield a
    a.close()


@pytest.mark.skipif(not NATIVE, reason="the seek decisions are the native executor's")
@pytest.mark.parametrize("rel,label", [("KNOWS", "P"), ("MET", "P"), ("LIKES", "Q")])
@pytest.mark.parametrize("belief", ["current", "all", "superseded"])
def test_many_keys_seek_or_scan(lopsided, rel, label, belief):
    """Thousands of keys against a sliver or most of the stored edge rows:
    whichever way the join's seek and the hop decide (the probe against the
    stored rows and the scan's estimate, or under a non-current belief the
    keys' slice rows against the stored rows), a join and a two-hop walk in
    every direction, at an instant and over a window, give the reference's
    rows, in its order."""
    for d, hops, vt in itertools.product(("out", "in", "both"), (1, 2),
                                         ({"instant": 5}, {"window": {"t_a": 3, "t_b": 8}})):
        # the dense items' two hops once (their frontier passes the hop's probe)
        if rel != "LIKES" or hops == 1 or d == "out":
            nodes = [{"var": "n", "labels": [label]}, {"var": "m"}]
            steps = [{"from": "n", "to": "m", "rel_type": rel, "dir": d, "var": "e"}]
            if hops == 2:
                nodes.append({"var": "o"})
                steps.append({"from": "m", "to": "o", "rel_type": rel, "dir": d})
            args = {"pattern": {"nodes": nodes, "steps": steps},
                    "return": {"rows": [{"as": "n", "expr": {"col": "n.uid"}},
                                        {"as": "m", "expr": {"col": "m.uid"}},
                                        {"as": "s", "expr": {"col": "e.tt_s"}}]},
                    "scope": {"vt": vt, "belief": belief},
                    "limit": 10000}
            want = reference(lopsided, args)
            got = OM.match(lopsided, args)
            assert canonical_json(got["rows"]) == canonical_json(want["rows"]), (rel, belief, d, hops, vt)
            assert got["rows"] or rel == "KNOWS"


RPQ = {"alt": [{"seq": [{"sym": "KNOWS"}, {"sym": "LIKES", "inv": True}]},
               {"seq": [{"sym": "KNOWS"}, {"sym": "LIKES", "inv": True}]},
               {"seq": [{"sym": "LIKES"}, {"sym": "KNOWS"}, {"sym": "KNOWS", "inv": True}]},
               {"seq": [{"sym": "KNOWS"}, {"sym": "LIKES", "inv": True}]}]}


@pytest.mark.parametrize("ret", ["pairs", "distinct", "exists"])
def test_a_path_expression_from_every_person(wide, ret):
    """A regular path from every one of 20000 persons (most with no
    first step), with an alternative written three times: the engine's walk
    (the seeds recognised from their groups, a repeated alternative read
    once) gives the reference's rows, in its order, as pairs, as a distinct
    projection and as an existence count; and the same rows as the
    expression with each alternative once."""
    once = {"alt": [RPQ["alt"][0], RPQ["alt"][2]]}
    out = []
    for rx in (RPQ, once):
        pat = {"nodes": [{"var": "x", "labels": ["P"]}, {"var": "y"}],
               "steps": [{"from": "x", "to": "y", "regex": rx}]}
        if ret == "pairs":
            r = {"rows": [{"as": "x", "expr": {"col": "x.uid"}}, {"as": "y", "expr": {"col": "y.uid"}}],
                 "order_by": [{"expr": {"col": "x"}}, {"expr": {"col": "y"}}]}
        elif ret == "distinct":
            r = {"group": {"by": [{"as": "y", "expr": {"col": "y.uid"}}], "aggs": []},
                 "order_by": [{"expr": {"col": "y"}}]}
        else:
            r = {"group": {"by": [{"as": "one", "expr": {"lit": 1}}], "aggs": []}}
        args = {"pattern": pat, "return": r, "scope": {"vt": {"instant": 12}}, "limit": 10000}
        want = reference(wide, args)
        got = OM.match(wide, args)
        assert canonical_json(got["rows"]) == canonical_json(want["rows"]), (ret, rx is RPQ)
        out.append(canonical_json(got["rows"]))
    assert out[0] == out[1]


def test_a_walk_moves_when_no_depth_is_read(wide):
    """With no `depth` read, plan_order may bind a variable-length step
    from its other end (here: counts under which the LIKES end is the cheap
    anchor); the plan so ordered gives the reference's rows (compiled with
    the store's own counts), at an instant and in two windows; a call that
    reads the depth keeps its order."""
    pat = {"nodes": [{"var": "x", "labels": ["P"]}, {"var": "y", "labels": ["P"]},
                     {"var": "z", "labels": ["P"]}],
           "steps": [{"from": "x", "to": "y", "rel_type": "KNOWS", "dir": "both",
                      "hops": {"min": 1, "max": 2}},
                     {"from": "z", "to": "y", "rel_type": "LIKES"}]}
    card = {"labels": {"P": 20000.0}, "rels": {"KNOWS": 100000.0, "LIKES": 2.0}, "nodes": 20000.0}
    moved = OM.plan_order(pat, {**card, "walks_movable": True})
    assert moved is not None and moved[0] != "x"
    assert OM.plan_order(pat, {**card, "walks_movable": False}) is None
    import json as _json

    for scope in ({"vt": {"instant": 12}}, W(0, 40), W(5, 20)):
        args = {"pattern": pat, "scope": scope,
                "return": {"rows": [{"as": "x", "expr": {"col": "x.uid"}},
                                    {"as": "y", "expr": {"col": "y.uid"}},
                                    {"as": "z", "expr": {"col": "z.uid"}}],
                           "order_by": [{"expr": {"col": "x"}}, {"expr": {"col": "y"}},
                                        {"expr": {"col": "z"}}]},
                "limit": 10000}
        filled = validate(args)
        c = compile_match(filled, {**card})
        assert _json.dumps(c.doc) != _json.dumps(compile_match(filled, None).doc)
        if hasattr(wide, "match_kernel"):
            got = wide.match_kernel(_json.dumps(c.doc), 0, 10000)
            rows = [dict(zip(c.columns, r)) if isinstance(r, list) else r for r in got["rows"]]
        else:
            from gnomon.temporal.ops_match import _reference_rows
            rows = _reference_rows(c, wide, "test store")
        want = reference(wide, args)["rows"]
        assert canonical_json(rows) == canonical_json(want), scope
