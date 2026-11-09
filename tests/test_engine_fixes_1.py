"""path it replaces, which stays behind its switch.

- `not` sub-patterns grown from their anchor keys only (GNOMON_MATCH_DECORRELATE)
- `paths` later pages read from the rows of the first (GNOMON_PATHS_PAGE_MEMO)
- the weighted valid_window graph priced at its own rates (GNOMON_PATHS_WINDOW_COST)
"""

from __future__ import annotations

import random
import tempfile

import pytest

from gnomon.core.model import OPEN_END, canonical_json
from gnomon.temporal import ops_general_paths as GP
from gnomon.temporal import ops_match as OM
from gnomon.temporal.algebra import call_operator, ensure_all_registered, validate_args
from gnomon.temporal.guardrails import add_time_estimate

from .conftest import ENVELOPE_META_KEYS

ensure_all_registered()


def _native():
    from gnomon.storage.native import NativeAdapter

    tmp = tempfile.TemporaryDirectory(prefix="gnomon-test-c4fix47-")
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


def _social(seed: int):
    """Persons who know each other, posts with creators and tags, some of it
    re-asserted later (versions) and some retracted (belief)."""
    rng = random.Random(seed)
    a = _native()
    people = [f"p{i}" for i in range(8)]
    tags = [f"t{i}" for i in range(6)]
    posts = [f"m{i}" for i in range(30)]
    _commit(a, [_node(u, "Person", name=u) for u in people] + [_node(t, "Tag", name=t) for t in tags], 1)
    ops = []
    for i in range(14):
        u, v = rng.sample(people, 2)
        ops.append(_edge(u, v, "KNOWS", rng.randrange(0, 20), disc=str(i)))
    _commit(a, ops, 2)
    ops = []
    for i, m in enumerate(posts):
        t0 = rng.randrange(0, 60)
        ops.append(_node(m, "Post", t0, content=m))
        ops.append(_edge(m, rng.choice(people), "HAS_CREATOR", t0))
        for t in rng.sample(tags, rng.randrange(1, 4)):
            ops.append(_edge(m, t, "HAS_TAG", t0, disc=str(i)))
    _commit(a, ops, 3)
    # later beliefs: some tags re-asserted with a closed interval, a node version
    ops = []
    for i, m in enumerate(posts[:8]):
        ops.append(_node(m, "Post", rng.randrange(0, 60), content=m + "'"))
    _commit(a, ops, 4)
    for m in posts[8:11]:
        try:
            _commit(a, [{"op": "retract", "ref": {"kind": "node", "uid": m}, "t": 50}], 5)
        except Exception:  # noqa: BLE001 - an op the store refuses
            pass
    return a


def _ic4_like(person: str, start: int, end: int, scope: dict) -> dict:
    return {"pattern": {
        "nodes": [{"var": "p", "labels": ["Person"], "uids": [person]}, {"var": "f", "labels": ["Person"]},
                  {"var": "po", "labels": ["Post"]}, {"var": "t", "labels": ["Tag"]}],
        "steps": [{"from": "p", "to": "f", "rel_type": "KNOWS", "dir": "both"},
                  {"from": "po", "to": "f", "rel_type": "HAS_CREATOR", "dir": "out"},
                  {"from": "po", "to": "t", "rel_type": "HAS_TAG", "dir": "out"}],
        "where": [{"cmp": ">=", "l": {"col": "po.vt_s"}, "r": {"lit": start, "tau": "ts"}},
                  {"cmp": "<", "l": {"col": "po.vt_s"}, "r": {"lit": end, "tau": "ts"}}],
        "not": [{"nodes": [{"var": "f2", "labels": ["Person"]}, {"var": "po2", "labels": ["Post"]}],
                 "steps": [{"from": "p", "to": "f2", "rel_type": "KNOWS", "dir": "both"},
                           {"from": "po2", "to": "f2", "rel_type": "HAS_CREATOR", "dir": "out"},
                           {"from": "po2", "to": "t", "rel_type": "HAS_TAG", "dir": "out"}],
                 "where": [{"cmp": "<", "l": {"col": "po2.vt_s"}, "r": {"lit": start, "tau": "ts"}}]}]},
        "return": {"group": {"by": [{"as": "tag", "expr": {"col": "t.uid"}}],
                             "aggs": [{"as": "n", "fn": "count"}]},
                   "order_by": [{"expr": {"col": "n"}, "dir": "desc"}, {"expr": {"col": "tag"}, "dir": "asc"}]},
        "scope": scope}


SCOPES = {
    "current": {"vt": {"instant": 70}},
    "asof": {"vt": {"instant": 70}, "as_of_tt": 3},
    "window": {"vt": {"window": {"t_a": 10, "t_b": 55}, "mode": "overlap"}, "co_valid": ["valid"]},
}


def test_the_closing_tag_of_ic4s_not_pattern_is_decorrelated():
    sub = _ic4_like("p0", 20, 40, SCOPES["current"])["pattern"]["not"][0]
    assert OM._decorrelation(sub, ["p", "t"], "anti") == ["t"]
    assert OM._decorrelation(sub, ["p", "t"], "left_outer") == []
    assert OM._decorrelation(sub, ["p"], "anti") == []
    walk = {**sub, "steps": sub["steps"][:2] + [{**sub["steps"][2], "hops": {"min": 1, "max": 2}}]}
    # a walk never leaves its far end free, and the seed is the first step's
    assert OM._decorrelation(walk, ["p", "t"], "anti") == []
    read = {**sub, "where": sub["where"] + [{"cmp": "!=", "l": {"col": "t.uid"}, "r": {"lit": "t0"}}]}
    assert OM._decorrelation(read, ["p", "t"], "anti") == []


@pytest.mark.parametrize("seed", [0, 1, 2])
@pytest.mark.parametrize("setting", sorted(SCOPES))
def test_decorrelated_not_answers_as_the_correlated_form(seed, setting, monkeypatch):
    a = _social(seed)
    compared = 0
    for person in ("p0", "p1", "p2", "p3"):
        for start, end in ((10, 30), (25, 45), (0, 60)):
            args = _ic4_like(person, start, end, SCOPES[setting])
            got = {}
            for flag in ("1", "0"):
                monkeypatch.setenv("GNOMON_MATCH_DECORRELATE", flag)
                env = call_operator(a, "match", dict(args))
                got[flag] = canonical_json({k: v for k, v in env.items() if k not in ENVELOPE_META_KEYS})
            assert got["1"] == got["0"], (seed, setting, person, start, end)
            compared += 1
    assert compared == 12


def _clique(n: int):
    a = _native()
    us = [f"v{i}" for i in range(n)]
    _commit(a, [_node(u, "Account") for u in us], 1)
    _commit(a, [_edge(u, v, "TRANSFER", disc=f"{i}{j}") for i, u in enumerate(us)
                for j, v in enumerate(us) if u != v], 2)
    return a


def test_paths_pages_read_from_the_first_and_equal_one_page(monkeypatch):
    a = _clique(7)
    base = {"sources": {"uids": ["v0"]}, "t_valid": 5, "rel_types": ["TRANSFER"],
            "result": "all_paths", "min_hops": 1, "max_hops": 3}
    whole = call_operator(a, "paths", {**base, "limit": 10_000})
    assert not whole["truncated"] and len(whole["rows"]) > 50
    calls = []
    real = GP._paths_rows
    monkeypatch.setattr(GP, "_paths_rows", lambda ad, x: calls.append(1) or real(ad, x))
    for memo in ("1", "0"):
        monkeypatch.setenv("GNOMON_PATHS_PAGE_MEMO", memo)
        GP._PAGE_MEMO.clear()
        calls.clear()
        rows, cursor, pages = [], None, 0
        while True:
            env = call_operator(a, "paths", {**base, "limit": 17, "cursor": cursor})
            rows += env["rows"]
            pages += 1
            cursor = env["cursor"]
            if not cursor:
                break
        assert canonical_json(rows) == canonical_json(whole["rows"]), memo
        assert len(calls) == (1 if memo == "1" else pages), (memo, len(calls), pages)
    # a write moves the generation: the memo is not read across it
    monkeypatch.setenv("GNOMON_PATHS_PAGE_MEMO", "1")
    first = call_operator(a, "paths", {**base, "limit": 17})
    _commit(a, [_node("v9", "Account")], 3)
    calls.clear()
    call_operator(a, "paths", {**base, "limit": 17, "cursor": first["cursor"]})
    assert len(calls) == 1


def _stats():
    return {"n_entities": 3_727_426, "n_node_versions": 3_817_828, "n_edge_versions": 21_631_486,
            "max_out_degree": 5871, "vt_min": 0, "vt_max": 1_356_998_399_940_001,
            "rel_type_counts": {"KNOWS": 235_180, "STUDY_AT": 8625, "TRANSFER": 1_993_721}}


def _weighted_window():
    w = {"pattern": {"nodes": [{"var": "a", "labels": ["Person"]}, {"var": "b", "labels": ["Person"]}],
                     "steps": [{"from": "a", "to": "b", "rel_type": "KNOWS", "dir": "both"}]},
         "return": {"rows": [{"as": "src", "expr": {"col": "a.uid"}}, {"as": "dst", "expr": {"col": "b.uid"}},
                             {"as": "w", "expr": {"lit": 1}}]}}
    return {"sources": {"uids": ["x"]}, "targets": {"uids": ["y"]}, "rel_types": ["KNOWS"], "dir": "both",
            "metric": "weight", "weight": {"match": w, "missing": "exclude"}, "result": "distance",
            "valid_window": {"t_a": 1_351_987_200_000_000, "t_b": 1_354_579_200_000_000}}


def test_the_weighted_window_form_alone_takes_the_array_rates(monkeypatch):
    st = _stats()
    weighted = validate_args("paths", _weighted_window())
    hops = validate_args("paths", {**{k: v for k, v in _weighted_window().items() if k not in ("metric", "weight")},
                                   "metric": "hops"})
    assert GP._window_weight_priced(weighted) and not GP._window_weight_priced(hops)
    new = add_time_estimate("paths", GP._paths_cost(weighted, st))["time_est_ms"]
    same = add_time_estimate("paths", GP._paths_cost(hops, st))["time_est_ms"]
    monkeypatch.setenv("GNOMON_PATHS_WINDOW_COST", "v1")
    assert not GP._window_weight_priced(weighted)
    old = add_time_estimate("paths", GP._paths_cost(weighted, st))["time_est_ms"]
    assert add_time_estimate("paths", GP._paths_cost(hops, st))["time_est_ms"] == same
    # 235,180 edge versions at 8 us (old) against 1 us plus 3.8M node versions at 35 ns
    assert old - new == pytest.approx(235_180 * 0.998 * 7e-3 - 3_817_828 * 35e-6, rel=0.05)


def _same_either_way(a, args: dict, switch: str, monkeypatch) -> str:
    got = {}
    for flag in ("1", "0"):
        monkeypatch.setenv(switch, flag)
        env = call_operator(a, "match", dict(args))
        got[flag] = canonical_json({k: v for k, v in env.items() if k not in ENVELOPE_META_KEYS})
    assert got["1"] == got["0"], (switch, args)
    return got["1"]


def _tag_eq(var: str, tag: str) -> dict:
    return {"cmp": "=", "l": {"tprop": [var, "name"], "tau": "str"}, "r": {"lit": tag}}


def _bi16_like(ta: str, tb: str, scope: dict) -> dict:
    """BI16's shape: a disjunction over the tag and the message, and two
    optional sub-patterns grown from the person, each with a tag by name."""
    def day(var: str, lo: int) -> dict:
        return {"bool": "and", "l": {"cmp": ">=", "l": {"col": f"{var}.vt_s"}, "r": {"lit": lo, "tau": "ts"}},
                "r": {"cmp": "<", "l": {"col": f"{var}.vt_s"}, "r": {"lit": lo + 30, "tau": "ts"}}}

    def opt(q: str, m: str, t: str, tag: str) -> dict:
        return {"nodes": [{"var": q, "labels": ["Person"]}, {"var": m, "labels": ["Post"]}, {"var": t, "labels": ["Tag"]}],
                "steps": [{"from": "p", "to": q, "rel_type": "KNOWS", "dir": "both"},
                          {"from": m, "to": q, "rel_type": "HAS_CREATOR", "dir": "out"},
                          {"from": m, "to": t, "rel_type": "HAS_TAG", "dir": "out"}],
                "where": [_tag_eq(t, tag), day(m, 10)]}
    return {"pattern": {
        "nodes": [{"var": "p", "labels": ["Person"]}, {"var": "m", "labels": ["Post"]}, {"var": "t", "labels": ["Tag"]}],
        "steps": [{"from": "m", "to": "p", "rel_type": "HAS_CREATOR", "dir": "out"},
                  {"from": "m", "to": "t", "rel_type": "HAS_TAG", "dir": "out"}],
        "where": [{"bool": "or", "l": {"bool": "and", "l": _tag_eq("t", ta), "r": day("m", 0)},
                   "r": {"bool": "and", "l": _tag_eq("t", tb), "r": day("m", 20)}}],
        "optional": [opt("qa", "ma", "xa", ta), opt("qb", "mb", "xb", tb)]},
        "return": {"group": {"by": [{"as": "person", "expr": {"col": "p.uid"}}],
                             "aggs": [{"as": "a", "fn": "count", "of": {"col": "ma.uid"}},
                                      {"as": "b", "fn": "count", "of": {"col": "mb.uid"}}]},
                   "order_by": [{"expr": {"col": "person"}, "dir": "asc"}]},
        "scope": scope}


def test_the_implied_predicates_and_their_price():
    w = _bi16_like("t0", "t1", SCOPES["current"])["pattern"]["where"]
    got = OM._implied(w)
    assert len(got) == 2 and {next(iter(OM._vars_in(x))) for x in got} == {"m", "t"}
    assert [OM._eq_literals(x) for x in got if OM._vars_in(x) == {"t"}] == [2]
    assert OM._implied([_tag_eq("t", "t0")]) == []          # one variable already
    sub = _bi16_like("t0", "t1", SCOPES["current"])["pattern"]["optional"][0]
    assert OM._sub_anchor(sub, ["p"]) == "xa"
    assert OM._sub_anchor({**sub, "where": sub["where"] + [{"cmp": "!=", "l": {"col": "p.uid"}, "r": {"lit": "x"}}]},
                          ["p"]) is None


@pytest.mark.parametrize("seed", [0, 1, 2])
@pytest.mark.parametrize("setting", sorted(SCOPES))
@pytest.mark.parametrize("switch", ["GNOMON_MATCH_IMPLIED", "GNOMON_MATCH_SUB_ANCHOR"])
def test_bi16_shaped_rows_are_the_same_either_way(seed, setting, switch, monkeypatch):
    a = _social(seed)
    for ta, tb in (("t0", "t1"), ("t2", "t5"), ("t3", "t3")):
        _same_either_way(a, _bi16_like(ta, tb, SCOPES[setting]), switch, monkeypatch)


def _bi6_like(tag: str, scope: dict) -> dict:
    """BI6's shape: a lookup keyed by a person's uid (the person's post count),
    read for the creators of one tag's posts."""
    pop = {"pattern": {"nodes": [{"var": "q", "labels": ["Person"]}, {"var": "m2", "labels": ["Post"]}],
                       "steps": [{"from": "m2", "to": "q", "rel_type": "HAS_CREATOR", "dir": "out"}]},
           "group": {"by": [{"as": "k", "expr": {"col": "q.uid"}}], "aggs": [{"as": "n", "fn": "count"}]}}
    return {"pattern": {
        "nodes": [{"var": "t", "labels": ["Tag"]}, {"var": "m", "labels": ["Post"]}, {"var": "p", "labels": ["Person"]}],
        "steps": [{"from": "m", "to": "t", "rel_type": "HAS_TAG", "dir": "out"},
                  {"from": "m", "to": "p", "rel_type": "HAS_CREATOR", "dir": "out"}],
        "where": [_tag_eq("t", tag)],
        "lookups": [{"as": "pop", "on": {"col": "p.uid"}, "key": "k", "match": pop}]},
        "return": {"group": {"by": [{"as": "person", "expr": {"col": "p.uid"}}],
                             "aggs": [{"as": "n", "fn": "max", "of": {"coalesce": [{"col": "pop.n"}, {"lit": 0}]}}]},
                   "order_by": [{"expr": {"col": "person"}, "dir": "asc"}]},
        "scope": scope}


def test_the_lookup_seed_rule():
    lk = _bi6_like("t0", SCOPES["current"])["pattern"]["lookups"][0]
    assert OM._lookup_seed(lk) == "q"
    ranked = {**lk, "match": {**lk["match"], "group": {**lk["match"]["group"], "limit": 3}}}
    assert OM._lookup_seed(ranked) is None
    other = {**lk, "match": {**lk["match"], "group": {**lk["match"]["group"],
                                                      "by": [{"as": "k", "expr": {"col": "m2.uid"}}]}}}
    assert OM._lookup_seed(other) == "m2"
    two = {**lk, "match": {**lk["match"], "group": {"by": [{"as": "k", "expr": {"col": "q.uid"}},
                                                           {"as": "z", "expr": {"col": "m2.uid"}}],
                                                    "aggs": [{"as": "n", "fn": "count"}],
                                                    "then": {"by": [{"as": "k", "expr": {"col": "k"}}],
                                                             "aggs": [{"as": "s", "fn": "sum", "of": {"col": "n"}}]}}}}
    assert OM._lookup_seed(two) == "q"


@pytest.mark.parametrize("seed", [0, 1, 2])
@pytest.mark.parametrize("setting", sorted(SCOPES))
def test_bi6_shaped_rows_are_the_same_either_way(seed, setting, monkeypatch):
    a = _social(seed)
    for tag in ("t0", "t3", "t5"):
        _same_either_way(a, _bi6_like(tag, SCOPES[setting]), "GNOMON_MATCH_LOOKUP_SEED", monkeypatch)


def _transfers(seed: int, n: int = 9, m: int = 60):
    rng = random.Random(seed)
    a = _native()
    accts = [f"a{i}" for i in range(n)]
    _commit(a, [_node(u, "Account") for u in accts], 1)
    _commit(a, [_edge(rng.choice(accts), rng.choice(accts), "TRANSFER", rng.randrange(0, 40), disc=str(i))
                for i in range(m)], 2)
    return a, accts


@pytest.mark.parametrize("seed", [0, 1, 2, 3])
@pytest.mark.parametrize("direction", ["out", "in", "both"])
def test_the_lazy_time_respecting_graph_is_the_loops(seed, direction, monkeypatch):
    """Per vertex, the lazily built adjacency and start times (their
    order included) are the per-edge loop's, and every page of an
    all_paths enumeration is the same."""
    a, accts = _transfers(seed)
    args = validate_args("paths", {"sources": {"uids": accts[:3]}, "rel_types": ["TRANSFER"], "dir": direction,
                                   "window": {"t_a": 0, "t_b": 100}, "time_respecting": "strict",
                                   "result": "all_paths", "min_hops": 1, "max_hops": 3})
    srcs = GP._endpoints(a, args["sources"], args, strict=True)
    monkeypatch.setenv("GNOMON_PATHS_TR_COLUMNS", "1")
    lazy = GP._graph(a, args, srcs, None)
    monkeypatch.setenv("GNOMON_PATHS_TR_COLUMNS", "0")
    loop = GP._graph(a, args, srcs, None)
    assert isinstance(lazy, GP._LazyTrGraph) and not isinstance(loop, GP._LazyTrGraph)
    ids = sorted(set(loop.adj) | set(range(0, 64)))
    for u in ids:
        assert list(lazy.adj.get(u, {}).items()) == list(loop.adj.get(u, {}).items()), u
        assert list((lazy.times.get(u) or {}).items()) == list((loop.times.get(u) or {}).items()), u
    rows = {}
    for flag in ("1", "0"):
        monkeypatch.setenv("GNOMON_PATHS_TR_COLUMNS", flag)
        GP._PAGE_MEMO.clear()
        out, cursor = [], None
        while True:
            env = call_operator(a, "paths", {**{k: v for k, v in args.items() if k not in ("limit", "cursor")},
                                             "limit": 7, "cursor": cursor})
            out += env["rows"]; cursor = env["cursor"]
            if not cursor:
                break
        rows[flag] = canonical_json(out)
    assert rows["1"] == rows["0"]
