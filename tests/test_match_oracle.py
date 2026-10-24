"""`match`: the compiled plan on the reference route against `Oracle.match`.

The oracle reads the operator's argument directly and shares nothing with the
compiler, so agreement here checks the compiler (Σ, the snapshot rule, edge
identity, shortest-distance bands, sub-patterns, `distinct_on`, the completed
order) and not only an executor. The engine's own executor is held to the
reference route separately (`test_match_native.py`).

Two halves:

* a fixed corpus over a hand-built store whose answers are also stated by
  hand, so a defect shared by the oracle and the compiler still fails;
* random bi-temporal stores (asserts, retracts, corrections, edges to nodes
  without versions) and random arguments from the operator's grammar, under
  every scope and belief mode. `GNOMON_HYP_EXAMPLES` sets the number of cases.
"""

from __future__ import annotations

import os
import random
from typing import Any

import pytest

from gnomon.core.errors import InvalidArgError, NotFoundError
from gnomon.core.model import OPEN_END, canonical_json
from gnomon.temporal.algebra import ensure_all_registered
from gnomon.temporal.ops_match import compile_match, run, validate
from gnomon.temporal.oracle import Oracle

from .conftest import fresh_adapter

EXAMPLES = int(os.environ.get("GNOMON_HYP_EXAMPLES", "25"))


def _apply(adapter, ops, tt):
    adapter.begin()
    try:
        adapter.apply_ops(ops, tt)
    except Exception:
        adapter.rollback()
        raise
    adapter.commit()


def node(uid, label, props, vt_s=0, vt_e=OPEN_END):
    return {"op": "assert_node", "uid": uid, "label": label, "props": props,
            "vt_s": vt_s, "vt_e": vt_e, "source": "t", "provenance_ref": None}


def edge(src, dst, rel, props=None, vt_s=0, vt_e=OPEN_END, disc=""):
    return {"op": "assert_edge", "src": src, "dst": dst, "rel_type": rel,
            "props": props or {}, "vt_s": vt_s, "vt_e": vt_e, "disc": disc,
            "source": "t", "provenance_ref": None}


def both(adapter, oracle, args):
    """(reference payload, oracle payload), or None when the argument is
    outside the operator's forms (both sides must then agree it is)."""
    filled = validate(args)
    try:
        compile_match(filled)
    except InvalidArgError:
        return None
    got = run(adapter, filled, route="reference",
              reference_bypass="test store: a few hundred rows")
    want = oracle.match(filled)
    return got, want


# ---------------------------------------------------------------------------
# the fixed corpus
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def social():
    a, oracle = build_social()
    yield a, oracle
    a.close()


def build_social():
    """Six persons; KNOWS once per friendship (a triangle 1-2-3 and a chain
    3-4-5-6, plus 6 knowing a node with no version); posts and a reply chain;
    person 5 retracted at 50; post 101's `w` corrected at tt 3."""
    ensure_all_registered()
    a = fresh_adapter()
    people = [node(f"p{i}", "Person", {"w": i, "name": f"P{i}"}) for i in range(1, 7)]
    posts = [node("m101", "Post", {"len": 10, "lang": "en"}),
             node("m102", "Post", {"len": 25, "lang": "de"}),
             node("c201", "Comment", {"len": 3}),
             node("c202", "Comment", {"len": 4}),
             node("f1", "Forum", {"title": "Album one"}),
             node("f2", "Forum", {"title": "Wall two"})]
    _apply(a, people + posts, 1)
    knows = [("p1", "p2", 5), ("p2", "p3", 6), ("p1", "p3", 7), ("p3", "p4", 8),
             ("p4", "p5", 9), ("p5", "p6", 10), ("p6", "ghost", 11)]
    es = [edge(s, d, "KNOWS", vt_s=t) for s, d, t in knows]
    es += [edge("m101", "p1", "HAS_CREATOR", vt_s=20), edge("m102", "p2", "HAS_CREATOR", vt_s=21),
           edge("c201", "p3", "HAS_CREATOR", vt_s=22), edge("c202", "p1", "HAS_CREATOR", vt_s=23),
           edge("c201", "m101", "REPLY_OF", vt_s=22), edge("c202", "c201", "REPLY_OF", vt_s=23),
           edge("f1", "m101", "CONTAINER_OF", vt_s=20), edge("f2", "m102", "CONTAINER_OF", vt_s=21),
           edge("f1", "p1", "HAS_MODERATOR", vt_s=1), edge("f2", "p2", "HAS_MODERATOR", vt_s=1),
           edge("p2", "m101", "LIKES", {"x": 1}, vt_s=30), edge("p3", "m101", "LIKES", {"x": 2}, vt_s=31),
           edge("p3", "m101", "LIKES", {"x": 3}, vt_s=32, disc="1")]
    _apply(a, es, 2)
    _apply(a, [{"op": "correct", "ref": {"kind": "node", "uid": "m101"},
                "props": {"len": 11, "lang": "en"}, "vt_s": 0, "vt_e": OPEN_END}], 3)
    _apply(a, [{"op": "retract", "ref": {"kind": "node", "uid": "p5"}, "t": 50}], 4)
    oracle = Oracle(list(a.all_node_versions()), list(a.all_edge_versions()))
    return a, oracle


NOW = {"vt": {"instant": 100}}
CORPUS: dict[str, tuple[dict[str, Any], list[dict[str, Any]] | None]] = {
    "friends both ways": ({
        "pattern": {"nodes": [{"var": "p", "uids": ["p3"]}, {"var": "f", "labels": ["Person"]}],
                    "steps": [{"var": "k", "from": "p", "to": "f", "rel_type": "KNOWS",
                               "dir": "both"}]},
        "return": {"rows": [{"as": "f", "expr": {"col": "f.uid"}},
                            {"as": "since", "expr": {"col": "k.vt_s"}}],
                   "order_by": [{"expr": {"col": "since"}, "dir": "desc"}]},
        "scope": NOW},
        [{"f": "p4", "since": 8}, {"f": "p1", "since": 7}, {"f": "p2", "since": 6}]),
    "a retracted person and a versionless node do not bind": ({
        "pattern": {"nodes": [{"var": "p", "uids": ["p4"]}, {"var": "f"}],
                    "steps": [{"from": "p", "to": "f", "rel_type": "KNOWS", "dir": "both",
                               "hops": {"min": 1, "max": 3}}]},
        "return": {"rows": [{"as": "f", "expr": {"col": "f.uid"}},
                            {"as": "d", "expr": {"col": "f.depth"}}]},
        "scope": NOW},
        # p5 is retracted at 50 but its KNOWS edges are not: the walk passes
        # through it (edges only) to p6, while p5 itself does not bind
        [{"f": "p1", "d": 2}, {"f": "p2", "d": 2}, {"f": "p3", "d": 1}, {"f": "p6", "d": 2}]),
    "the same at an instant before the retraction": ({
        "pattern": {"nodes": [{"var": "p", "uids": ["p4"]}, {"var": "f"}],
                    "steps": [{"from": "p", "to": "f", "rel_type": "KNOWS", "dir": "both",
                               "hops": {"min": 1, "max": 1}}]},
        "return": {"rows": [{"as": "f", "expr": {"col": "f.uid"}}]},
        "scope": {"vt": {"instant": 40}}},
        [{"f": "p3"}, {"f": "p5"}]),
    "triangle, each once": ({
        "pattern": {"nodes": [{"var": "a", "labels": ["Person"]}, {"var": "b"}, {"var": "c"}],
                    "steps": [{"from": "a", "to": "b", "rel_type": "KNOWS", "dir": "both"},
                              {"from": "b", "to": "c", "rel_type": "KNOWS", "dir": "both"},
                              {"from": "c", "to": "a", "rel_type": "KNOWS", "dir": "both"}],
                    "where": [{"cmp": "<", "l": {"col": "a.uid"}, "r": {"col": "b.uid"}},
                              {"cmp": "<", "l": {"col": "b.uid"}, "r": {"col": "c.uid"}}]},
        "return": {"group": {"by": [], "aggs": [{"as": "n", "fn": "count"}]}},
        "scope": NOW},
        [{"n": 1}]),
    "reply closure to the root post": ({
        "pattern": {"nodes": [{"var": "m", "uids": ["c202"]}, {"var": "root", "labels": ["Post"]},
                              {"var": "a", "labels": ["Person"]}],
                    "steps": [{"from": "m", "to": "root", "rel_type": "REPLY_OF",
                               "hops": {"min": 0, "max": None}},
                              {"from": "root", "to": "a", "rel_type": "HAS_CREATOR"}]},
        "return": {"rows": [{"as": "root", "expr": {"col": "root.uid"}},
                            {"as": "author", "expr": {"col": "a.uid"}},
                            {"as": "len", "expr": {"tprop": ["root", "len"], "tau": "int"}}]},
        "scope": NOW},
        [{"root": "m101", "author": "p1", "len": 11}]),
    "the root post's length as first believed": ({
        "pattern": {"nodes": [{"var": "m", "uids": ["m101"]}]},
        "return": {"rows": [{"as": "len", "expr": {"tprop": ["m", "len"], "tau": "int"}},
                            {"as": "lang", "expr": {"tprop": ["m", "lang"], "tau": "int"}}]},
        "scope": {"vt": {"instant": 100}, "as_of_tt": 2}},
        # a typed read of a string as int is null, never an error
        [{"len": 10, "lang": None}]),
    "reply flag through an optional sub-pattern": ({
        "pattern": {"nodes": [{"var": "m", "uids": ["m101"]}, {"var": "r"}, {"var": "ra"},
                              {"var": "ma"}],
                    "steps": [{"from": "r", "to": "m", "rel_type": "REPLY_OF"},
                              {"from": "r", "to": "ra", "rel_type": "HAS_CREATOR"},
                              {"from": "m", "to": "ma", "rel_type": "HAS_CREATOR"}],
                    "optional": [{"steps": [{"var": "kk", "from": "ra", "to": "ma",
                                             "rel_type": "KNOWS", "dir": "both"}]}]},
        "return": {"rows": [{"as": "r", "expr": {"col": "r.uid"}},
                            {"as": "knows", "expr": {"not": {"is_null": {"col": "kk.eid"}}}}]},
        "scope": NOW},
        [{"r": "c201", "knows": True}]),
    "persons who created nothing": ({
        "pattern": {"nodes": [{"var": "p", "labels": ["Person"]}],
                    "not": [{"steps": [{"from": "m", "to": "p", "rel_type": "HAS_CREATOR"}]}]},
        "return": {"rows": [{"as": "p", "expr": {"col": "p.uid"}}]},
        "scope": NOW},
        [{"p": "p4"}, {"p": "p6"}]),
    "likes per message: multi-edges, distinct likers": ({
        "pattern": {"nodes": [{"var": "m", "labels": ["Post"]}, {"var": "p"}],
                    "steps": [{"var": "l", "from": "p", "to": "m", "rel_type": "LIKES"}]},
        "return": {"group": {"by": [{"as": "m", "expr": {"col": "m.uid"}}],
                             "aggs": [{"as": "likes", "fn": "count"},
                                      {"as": "likers", "fn": "count_distinct",
                                       "of": {"col": "p.uid"}},
                                      {"as": "x", "fn": "sum",
                                       "of": {"tprop": ["l", "x"], "tau": "int"}},
                                      {"as": "w", "fn": "sum",
                                       "of": {"tprop": ["p", "w"], "tau": "int"},
                                       "distinct_on": ["p"]}]}},
        "scope": NOW},
        [{"m": "m101", "likes": 3, "likers": 2, "x": 6, "w": 5}]),
    "forum titles by prefix": ({
        "pattern": {"nodes": [{"var": "f", "labels": ["Forum"]}],
                    "where": [{"starts_with": [{"tprop": ["f", "title"], "tau": "str"},
                                               {"lit": "Album "}]}]},
        "return": {"rows": [{"as": "f", "expr": {"col": "f.uid"}}]},
        "scope": NOW},
        [{"f": "f1"}]),
    "count of counts": ({
        "pattern": {"nodes": [{"var": "p", "labels": ["Person"]}],
                    "optional": [{"steps": [{"from": "m", "to": "p", "rel_type": "HAS_CREATOR"}]}]},
        "return": {"group": {"by": [{"as": "p", "expr": {"col": "p.uid"}}],
                             "aggs": [{"as": "n", "fn": "count", "of": {"col": "m.uid"}}],
                             "then": {"by": [{"as": "n", "expr": {"col": "n"}}],
                                      "aggs": [{"as": "persons", "fn": "count"}]}},
                   "order_by": [{"expr": {"col": "persons"}, "dir": "desc"}]},
        "scope": NOW},
        [{"n": 0, "persons": 2}, {"n": 1, "persons": 2}, {"n": 2, "persons": 1}]),
    "all believed versions of a corrected node": ({
        "pattern": {"nodes": [{"var": "m", "uids": ["m101"]}]},
        "return": {"rows": [{"as": "len", "expr": {"tprop": ["m", "len"], "tau": "int"}},
                            {"as": "tt_e", "expr": {"col": "m.tt_e"}}]},
        "scope": {"vt": {"window": {"t_a": 0, "t_b": OPEN_END}}, "belief": "all"}},
        [{"len": 10, "tt_e": 3}, {"len": 11, "tt_e": OPEN_END}]),
    "sequenced: a like while both endpoints and the like are valid": ({
        "pattern": {"nodes": [{"var": "p", "labels": ["Person"]}, {"var": "m", "labels": ["Post"]}],
                    "steps": [{"var": "l", "from": "p", "to": "m", "rel_type": "LIKES"}]},
        "return": {"rows": [{"as": "p", "expr": {"col": "p.uid"}},
                            {"as": "s", "expr": {"col": "valid.s"}},
                            {"as": "e", "expr": {"col": "valid.e"}}]},
        "scope": {"vt": {"window": {"t_a": 0, "t_b": OPEN_END}}, "co_valid": ["valid"]}},
        [{"p": "p2", "s": 30, "e": OPEN_END}, {"p": "p3", "s": 31, "e": OPEN_END},
         {"p": "p3", "s": 32, "e": OPEN_END}]),
    "conditional counts, a ratio and a percent of the total": ({
        "pattern": {"nodes": [{"var": "m", "labels": ["Post", "Comment"]},
                              {"var": "p", "labels": ["Person"]}],
                    "steps": [{"from": "m", "to": "p", "rel_type": "HAS_CREATOR"}]},
        "return": {"group": {
            "by": [{"as": "p", "expr": {"col": "p.uid"}}],
            "aggs": [{"as": "n", "fn": "count"},
                     {"as": "long", "fn": "count",
                      "where": {"cmp": ">=", "l": {"tprop": ["m", "len"], "tau": "int"},
                                "r": {"lit": 10}}}],
            "project": [{"as": "pct", "expr": {"arith": "/", "l": {"arith": "*", "l": {"lit": 100},
                                                                   "r": {"col": "n"}},
                                               "r": {"total": "n"}}},
                        {"as": "ratio", "expr": {"arith": "/", "l": {"col": "long"},
                                                 "r": {"col": "n"}}}]}},
        "scope": NOW},
        [{"p": "p1", "n": 2, "long": 1, "pct": 50.0, "ratio": 0.5},
         {"p": "p2", "n": 1, "long": 1, "pct": 25.0, "ratio": 1.0},
         {"p": "p3", "n": 1, "long": 0, "pct": 25.0, "ratio": 0.0}]),
    "conditional aggregates over distinct identities": ({
        "pattern": {"nodes": [{"var": "m", "labels": ["Post"]}, {"var": "p", "labels": ["Person"]}],
                    "steps": [{"var": "l", "from": "p", "to": "m", "rel_type": "LIKES"}]},
        "return": {"group": {
            "by": [{"as": "m", "expr": {"col": "m.uid"}}],
            "aggs": [{"as": "n", "fn": "count", "where": {
                         "cmp": ">=", "l": {"tprop": ["l", "x"], "tau": "int"}, "r": {"lit": 2}}},
                     {"as": "likers", "fn": "count", "distinct_on": ["p"], "where": {
                         "cmp": ">=", "l": {"tprop": ["l", "x"], "tau": "int"}, "r": {"lit": 2}}},
                     {"as": "w", "fn": "sum", "of": {"tprop": ["p", "w"], "tau": "int"}, "distinct_on": ["p"], "where": {
                         "cmp": ">=", "l": {"tprop": ["l", "x"], "tau": "int"}, "r": {"lit": 2}}},
                     {"as": "none", "fn": "max", "of": {"tprop": ["p", "w"], "tau": "int"}, "where": {"lit": False}}]}},
        "scope": NOW},
        [{"m": "m101", "n": 2, "likers": 1, "w": 3, "none": None}]),
    "guarded division, rounding half to even, floor division": ({
        "pattern": {"nodes": [{"var": "p", "labels": ["Person"]}]},
        "return": {"rows": [
            {"as": "p", "expr": {"col": "p.uid"}},
            {"as": "q", "expr": {"if": [{"cmp": "!=", "l": {"tprop": ["p", "w"], "tau": "int"}, "r": {"lit": 3}},
                                        {"arith": "/", "l": {"tprop": ["p", "w"], "tau": "int"},
                                         "r": {"arith": "-", "l": {"tprop": ["p", "w"], "tau": "int"}, "r": {"lit": 3}}},
                                        {"lit": 0.0}]}},
            {"as": "r", "expr": {"fn": "round", "arg": {"arith": "/", "l": {"tprop": ["p", "w"], "tau": "int"}, "r": {"lit": 2}}}},
            {"as": "fd", "expr": {"arith": "floor_div", "l": {"tprop": ["p", "w"], "tau": "int"}, "r": {"lit": -4}}},
            {"as": "ab", "expr": {"fn": "abs", "arg": {"arith": "-", "l": {"tprop": ["p", "w"], "tau": "int"}, "r": {"lit": 4}}}},
            {"as": "sq", "expr": {"fn": "sqrt", "arg": {"arith": "*", "l": {"tprop": ["p", "w"], "tau": "int"}, "r": {"tprop": ["p", "w"], "tau": "int"}}}}]},
        "scope": NOW},
        [{"p": "p1", "q": -0.5, "r": 0, "fd": -1, "ab": 3, "sq": 1.0},
         {"p": "p2", "q": -2.0, "r": 1, "fd": -1, "ab": 2, "sq": 2.0},
         {"p": "p3", "q": 0.0, "r": 2, "fd": -1, "ab": 1, "sq": 3.0},
         {"p": "p4", "q": 4.0, "r": 2, "fd": -1, "ab": 0, "sq": 4.0},
         {"p": "p6", "q": 2.0, "r": 3, "fd": -2, "ab": 2, "sq": 6.0}]),
    "calendar fields and string functions": ({
        "pattern": {"nodes": [{"var": "f", "labels": ["Forum"]}],
                    "where": [{"strfn": "like", "args": [{"tprop": ["f", "title"], "tau": "str"},
                                                         {"lit": "%l%"}]}]},
        "return": {"rows": [
            {"as": "f", "expr": {"col": "f.uid"}},
            {"as": "sub", "expr": {"strfn": "substring", "args": [
                {"tprop": ["f", "title"], "tau": "str"}, {"lit": -1}, {"lit": 4}]}},
            {"as": "one", "expr": {"strfn": "like", "args": [
                {"tprop": ["f", "title"], "tau": "str"}, {"lit": "%o_e"}]}},
            {"as": "has", "expr": {"strfn": "contains", "args": [
                {"tprop": ["f", "title"], "tau": "str"}, {"lit": "l t"}]}},
            {"as": "yr", "expr": {"calendar": "year", "arg": {"col": "f.vt_s"}}},
            {"as": "md", "expr": {"calendar": "month_day", "arg": {"lit": 1699963200000000}}},
            {"as": "mb", "expr": {"calendar": "months_between", "arg": {"lit": 1580428800000000},
                                  "other": {"lit": 1614556800000000}}}]},
        "scope": NOW},
        [{"f": "f1", "sub": "Al", "one": True, "has": False, "yr": 1970, "md": 1114, "mb": 14},
         {"f": "f2", "sub": "Wa", "one": False, "has": True, "yr": 1970, "md": 1114, "mb": 14}]),
    "a union of two branches, then a group over its columns": ({
        "union": [
            {"pattern": {"nodes": [{"var": "f", "labels": ["Forum"]}, {"var": "p"}],
                         "steps": [{"from": "f", "to": "p", "rel_type": "HAS_MODERATOR"}]},
             "rows": [{"as": "p", "expr": {"col": "p.uid"}}, {"as": "w", "expr": {"lit": 100}}]},
            {"pattern": {"nodes": [{"var": "m", "labels": ["Post", "Comment"]}, {"var": "p"}],
                         "steps": [{"from": "m", "to": "p", "rel_type": "HAS_CREATOR"}]},
             "rows": [{"as": "p", "expr": {"col": "p.uid"}}, {"as": "w", "expr": {"lit": 1}}]}],
        "return": {"group": {"by": [{"as": "p", "expr": {"col": "p"}}],
                             "aggs": [{"as": "score", "fn": "sum", "of": {"col": "w"}}]},
                   "order_by": [{"expr": {"col": "score"}, "dir": "desc"}]},
        "scope": NOW},
        [{"p": "p1", "score": 102}, {"p": "p2", "score": 101}, {"p": "p3", "score": 1}]),
    "one hop over either of two relationship types": ({
        "pattern": {"nodes": [{"var": "p", "uids": ["p1"]}, {"var": "x"}],
                    "steps": [{"var": "e", "from": "p", "to": "x", "dir": "both",
                               "rel_type": ["KNOWS", "HAS_MODERATOR"]}]},
        "return": {"rows": [{"as": "x", "expr": {"col": "x.uid"}},
                            {"as": "rel", "expr": {"col": "e.rel_type"}}]},
        "scope": NOW},
        [{"x": "f1", "rel": "HAS_MODERATOR"}, {"x": "p2", "rel": "KNOWS"},
         {"x": "p3", "rel": "KNOWS"}]),
    "either of two relationship types, binding versions": ({
        "pattern": {"nodes": [{"var": "p", "uids": ["p1"]}, {"var": "x"}],
                    "steps": [{"var": "e", "from": "x", "to": "p",
                               "rel_type": ["HAS_CREATOR", "HAS_MODERATOR"]}]},
        "return": {"rows": [{"as": "x", "expr": {"col": "x.uid"}},
                            {"as": "rel", "expr": {"col": "e.rel_type"}}]},
        "scope": {"vt": {"window": {"t_a": 0, "t_b": OPEN_END}}}},
        None),
    "the latest like per liker": ({
        "pattern": {"nodes": [{"var": "m", "labels": ["Post"]}, {"var": "p", "labels": ["Person"]}],
                    "steps": [{"var": "l", "from": "p", "to": "m", "rel_type": "LIKES"}]},
        "return": {"rows": [{"as": "liker", "expr": {"col": "p.uid"}},
                            {"as": "d", "expr": {"col": "l.vt_s"}}],
                   "top_per": {"by": [{"col": "liker"}],
                               "order_by": [{"expr": {"col": "d"}, "dir": "desc"}], "k": 1},
                   "order_by": [{"expr": {"col": "liker"}}]},
        "scope": NOW},
        [{"liker": "p2", "d": 30}, {"liker": "p3", "d": 32}]),
    "the first group per creator after an aggregate": ({
        "pattern": {"nodes": [{"var": "m", "labels": ["Post", "Comment"]}, {"var": "p"}],
                    "steps": [{"from": "m", "to": "p", "rel_type": "HAS_CREATOR"}]},
        "return": {"group": {"by": [{"as": "p", "expr": {"col": "p.uid"}},
                                    {"as": "label", "expr": {"col": "m.label"}}],
                             "aggs": [{"as": "n", "fn": "count"}]},
                   "top_per": {"by": [{"col": "p"}],
                               "order_by": [{"expr": {"col": "n"}, "dir": "desc"}], "k": 1}},
        "scope": NOW},
        [{"p": "p1", "label": "Comment", "n": 1}, {"p": "p2", "label": "Post", "n": 1},
         {"p": "p3", "label": "Comment", "n": 1}]),
    "collected lists: ordered, bounded, distinct, tuples": ({
        "pattern": {"nodes": [{"var": "p", "uids": ["p1", "p3"]}, {"var": "f", "labels": ["Person"]}],
                    "steps": [{"from": "p", "to": "f", "rel_type": "KNOWS", "dir": "both"}]},
        "return": {"group": {"by": [{"as": "p", "expr": {"col": "p.uid"}}],
                             "aggs": [{"as": "names", "fn": "collect",
                                       "of": {"tprop": ["f", "name"], "tau": "str"},
                                       "order_by": [{"expr": {"tprop": ["f", "name"], "tau": "str"},
                                                     "dir": "desc"}], "limit": 2},
                                      {"as": "all", "fn": "collect", "distinct": True,
                                       "of": [{"col": "f.uid"}, {"tprop": ["f", "w"], "tau": "int"}]}]}},
        "scope": NOW},
        [{"p": "p1", "names": ["P3", "P2"], "all": [["p2", 2], ["p3", 3]]},
         {"p": "p3", "names": ["P4", "P2"], "all": [["p1", 1], ["p2", 2], ["p4", 4]]}]),
    "collected lists with a condition, distinct, and empty": ({
        "pattern": {"nodes": [{"var": "m", "labels": ["Post"]}, {"var": "p", "labels": ["Person"]}],
                    "steps": [{"var": "l", "from": "p", "to": "m", "rel_type": "LIKES"}]},
        "return": {"group": {"by": [{"as": "m", "expr": {"col": "m.uid"}}],
                             "aggs": [{"as": "likers", "fn": "collect", "of": {"col": "p.uid"},
                                       "distinct": True},
                                      {"as": "big", "fn": "collect",
                                       "of": {"tprop": ["l", "x"], "tau": "int"},
                                       "where": {"cmp": ">=", "l": {"tprop": ["l", "x"], "tau": "int"},
                                                 "r": {"lit": 3}}},
                                      {"as": "none", "fn": "collect", "of": {"col": "p.uid"},
                                       "where": {"lit": False}}]}},
        "scope": NOW},
        [{"m": "m101", "likers": ["p2", "p3"], "big": [3], "none": []}]),
    "a reply chain as a regular path": ({
        "pattern": {"nodes": [{"var": "c", "uids": ["c202"]}, {"var": "x"}],
                    "steps": [{"from": "c", "to": "x", "regex": {"plus": {"sym": "REPLY_OF"}}}]},
        "return": {"rows": [{"as": "x", "expr": {"col": "x.uid"}}]},
        "scope": NOW},
        [{"x": "c201"}, {"x": "m101"}]),
    "inverse edges under a closure, read from the bound end": ({
        "pattern": {"nodes": [{"var": "x"}, {"var": "p", "uids": ["p1"]}],
                    "steps": [{"from": "x", "to": "p", "regex": {"seq": [
                        {"star": {"sym": "REPLY_OF"}}, {"sym": "HAS_CREATOR"}]}}]},
        "return": {"rows": [{"as": "x", "expr": {"col": "x.uid"}}]},
        "scope": NOW},
        [{"x": "c201"}, {"x": "c202"}, {"x": "m101"}]),
    "a closing regular path (a triangle back to the anchor)": ({
        "pattern": {"nodes": [{"var": "a", "uids": ["p1"]}, {"var": "b"}],
                    "steps": [{"var": "e", "from": "a", "to": "b", "rel_type": "KNOWS"},
                              {"from": "b", "to": "a", "regex": {"alt": [
                                  {"sym": "KNOWS"}, {"sym": "KNOWS", "inv": True}]}}]},
        "return": {"rows": [{"as": "b", "expr": {"col": "b.uid"}}]},
        "scope": NOW},
        [{"b": "p2"}, {"b": "p3"}]),
    "a time-increasing reach, shortest valid distance": ({
        "pattern": {"nodes": [{"var": "p", "uids": ["p1"]}, {"var": "x"}],
                    "steps": [{"from": "p", "to": "x", "rel_type": "KNOWS",
                               "hops": {"min": 1, "max": 5}, "order": "strict_increasing"}]},
        "return": {"rows": [{"as": "x", "expr": {"col": "x.uid"}},
                            {"as": "d", "expr": {"col": "x.depth"}}]},
        "scope": NOW},
        [{"x": "p2", "d": 1}, {"x": "p3", "d": 1}, {"x": "p4", "d": 2}, {"x": "p6", "d": 4}]),
    "an increasing path read from its end": ({
        "pattern": {"nodes": [{"var": "x"}, {"var": "p", "uids": ["p4"]}],
                    "steps": [{"from": "x", "to": "p", "rel_type": "KNOWS",
                               "hops": {"min": 1, "max": 3}, "order": "strict_increasing"}]},
        "return": {"rows": [{"as": "x", "expr": {"col": "x.uid"}},
                            {"as": "d", "expr": {"col": "x.depth"}}]},
        "scope": NOW},
        [{"x": "p1", "d": 2}, {"x": "p2", "d": 2}, {"x": "p3", "d": 1}]),
    "a closure over the edges a predicate admits, truncated per node": ({
        "pattern": {"nodes": [{"var": "p", "uids": ["p1"]}, {"var": "x"}],
                    "steps": [{"from": "p", "to": "x", "rel_type": "KNOWS",
                               "hops": {"min": 1, "max": 3},
                               "edge_where": {"var": "t", "where": [
                                   {"cmp": ">=", "l": {"col": "t.vt_s"}, "r": {"lit": 5}}]},
                               "truncate": {"limit": 1, "order_by": [
                                   {"expr": {"col": "t.vt_s"}, "dir": "desc"}]}}]},
        "return": {"rows": [{"as": "x", "expr": {"col": "x.uid"}},
                            {"as": "d", "expr": {"col": "x.depth"}}]},
        "scope": NOW},
        [{"x": "p3", "d": 1}, {"x": "p4", "d": 2}]),
    "a single hop truncated per binding": ({
        "pattern": {"nodes": [{"var": "p", "uids": ["p1", "p3"]}, {"var": "f", "labels": ["Person"]}],
                    "steps": [{"var": "k", "from": "p", "to": "f", "rel_type": "KNOWS",
                               "dir": "both", "truncate": {"limit": 1, "order_by": [
                                   {"expr": {"col": "k.vt_s"}, "dir": "desc"}]}}]},
        "return": {"rows": [{"as": "p", "expr": {"col": "p.uid"}},
                            {"as": "f", "expr": {"col": "f.uid"}}]},
        "scope": NOW},
        [{"p": "p1", "f": "p3"}, {"p": "p3", "f": "p4"}]),
    "the version a correction replaced, and its successor": ({
        "pattern": {"nodes": [{"var": "m", "uids": ["m101"]}],
                    "succ": [{"var": "n", "of": "m", "axis": "transaction", "dir": "next"}]},
        "return": {"rows": [{"as": "before", "expr": {"tprop": ["m", "len"], "tau": "int"}},
                            {"as": "after", "expr": {"tprop": ["n", "len"], "tau": "int"}}]},
        "scope": {"vt": {"window": {"t_a": 0, "t_b": OPEN_END}}, "belief": "all"}},
        [{"before": 10, "after": 11}]),
    "versions of an edge identity in valid-time order, backwards": ({
        "pattern": {"nodes": [{"var": "p", "uids": ["p3"]}, {"var": "m", "labels": ["Post"]}],
                    "steps": [{"var": "l", "from": "p", "to": "m", "rel_type": "LIKES"}],
                    "succ": [{"var": "k", "of": "l", "axis": "valid", "dir": "prev"}]},
        "return": {"rows": [{"as": "s", "expr": {"col": "l.vt_s"}},
                            {"as": "prev", "expr": {"col": "k.vt_s"}}]},
        "scope": {"vt": {"window": {"t_a": 0, "t_b": OPEN_END}}, "belief": "all"}},
        None),
    "a decorrelated count per person, left outer": ({
        "pattern": {"nodes": [{"var": "p", "labels": ["Person"]}],
                    "lookups": [{"as": "msgs", "on": {"col": "p.uid"}, "key": "q", "match": {
                        "pattern": {"nodes": [{"var": "m", "labels": ["Post", "Comment"]},
                                              {"var": "q", "labels": ["Person"]}],
                                    "steps": [{"from": "m", "to": "q", "rel_type": "HAS_CREATOR"}]},
                        "group": {"by": [{"as": "q", "expr": {"col": "q.uid"}}],
                                  "aggs": [{"as": "n", "fn": "count"}]}}}]},
        "return": {"rows": [{"as": "p", "expr": {"col": "p.uid"}},
                            {"as": "n", "expr": {"coalesce": [{"col": "msgs.n"}, {"lit": 0}]}}]},
        "scope": NOW},
        [{"p": "p1", "n": 2}, {"p": "p2", "n": 1}, {"p": "p3", "n": 1}, {"p": "p4", "n": 0},
         {"p": "p6", "n": 0}]),
    # A lookup keyed on p is a function of p, so distinct_on [p] may read it
    "distinct_on reads a lookup keyed on its variable": ({
        "pattern": {"nodes": [{"var": "m", "labels": ["Post"]}, {"var": "p", "labels": ["Person"]}],
                    "steps": [{"from": "p", "to": "m", "rel_type": "LIKES"}],
                    "lookups": [{"as": "msgs", "on": {"col": "p.uid"}, "key": "q", "match": {
                        "pattern": {"nodes": [{"var": "c", "labels": ["Post", "Comment"]},
                                              {"var": "q", "labels": ["Person"]}],
                                    "steps": [{"from": "c", "to": "q", "rel_type": "HAS_CREATOR"}]},
                        "group": {"by": [{"as": "q", "expr": {"col": "q.uid"}}],
                                  "aggs": [{"as": "n", "fn": "count"}]}}}]},
        "return": {"group": {"by": [{"as": "m", "expr": {"col": "m.uid"}}],
                             "aggs": [{"as": "likes", "fn": "count"},
                                      {"as": "msgs", "fn": "sum", "distinct_on": ["p"],
                                       "of": {"coalesce": [{"col": "msgs.n"}, {"lit": 0}]}},
                                      {"as": "msgs_all", "fn": "sum",
                                       "of": {"coalesce": [{"col": "msgs.n"}, {"lit": 0}]}}]}},
        "scope": NOW},
        [{"m": "m101", "likes": 3, "msgs": 2, "msgs_all": 3}]),
    "an inner lookup of a whole-stage mean, read by a predicate": ({
        "pattern": {"nodes": [{"var": "p", "labels": ["Person"]}],
                    "where": [{"cmp": ">", "l": {"tprop": ["p", "w"], "tau": "int"},
                               "r": {"col": "avg.mw"}}],
                    "lookups": [{"as": "avg", "on": {"lit": 1}, "key": "k", "join": "inner",
                                 "match": {
                        "pattern": {"nodes": [{"var": "q", "labels": ["Person"]}]},
                        "group": {"by": [{"as": "k", "expr": {"lit": 1}}],
                                  "aggs": [{"as": "mw", "fn": "mean",
                                            "of": {"tprop": ["q", "w"], "tau": "int"}}]}}}]},
        "return": {"rows": [{"as": "p", "expr": {"col": "p.uid"}}]},
        "scope": NOW},
        [{"p": "p4"}, {"p": "p6"}]),
    "top-k with a semantic limit and a page": ({
        "pattern": {"nodes": [{"var": "p", "labels": ["Person"]}]},
        "return": {"rows": [{"as": "p", "expr": {"col": "p.uid"}},
                            {"as": "w", "expr": {"tprop": ["p", "w"], "tau": "int"}}],
                   "order_by": [{"expr": {"col": "w"}, "dir": "desc"}], "limit": 3},
        "scope": NOW, "limit": 2},
        [{"p": "p6", "w": 6}, {"p": "p4", "w": 4}]),
}


def test_distinct_on_reads_only_lookups_keyed_on_its_variables():
    """A lookup keyed on a distinct_on variable reads as that variable; one
    keyed on another variable does not (its value is not a function of the
    named identities)."""
    def args(on: str) -> dict[str, Any]:
        return {"pattern": {
            "nodes": [{"var": "m", "labels": ["Post"]}, {"var": "p", "labels": ["Person"]}],
            "steps": [{"from": "p", "to": "m", "rel_type": "LIKES"}],
            "lookups": [{"as": "lk", "on": {"col": f"{on}.uid"}, "key": "q", "match": {
                "pattern": {"nodes": [{"var": "q"}]},
                "group": {"by": [{"as": "q", "expr": {"col": "q.uid"}}],
                          "aggs": [{"as": "n", "fn": "count"}]}}}]},
            "return": {"group": {"by": [{"as": "m", "expr": {"col": "m.uid"}}],
                                 "aggs": [{"as": "s", "fn": "sum", "distinct_on": ["p"],
                                           "of": {"col": "lk.n"}}]}},
            "scope": NOW}
    compile_match(validate(args("p")))
    with pytest.raises(InvalidArgError, match="may read only the named variables"):
        compile_match(validate(args("m")))


@pytest.mark.parametrize("name", sorted(CORPUS))
def test_corpus(social, name):
    a, oracle = social
    args, expected = CORPUS[name]
    pair = both(a, oracle, args)
    assert pair is not None, "the corpus argument must compile"
    got, want = pair
    assert canonical_json(got) == canonical_json(want)
    if expected is not None:
        assert got["rows"] == expected, got


def test_paging_reports_the_whole_answer(social):
    a, _ = social
    args, _ = CORPUS["top-k with a semantic limit and a page"]
    got = run(a, validate(args), route="reference", reference_bypass="test")
    assert got["rows_total"] == 3 and got["truncated"] and got["cursor"] == "2"
    rest = run(a, validate({**args, "cursor": "2"}), route="reference",
               reference_bypass="test")
    assert rest["rows"] == [{"p": "p3", "w": 3}] and not rest["truncated"]


def test_unsupported_shapes_refuse_loudly(social):
    a, _ = social
    window = {"vt": {"window": {"t_a": 0, "t_b": 100}}}
    for args in (
        # a closure under a non-sequenced window (the shared-instant rule needs co_valid: [valid])
        {"pattern": {"nodes": [{"var": "p"}, {"var": "q"}],
                     "steps": [{"from": "p", "to": "q", "hops": {"min": 1, "max": 2}}]},
         "return": {"rows": [{"as": "q", "expr": {"col": "q.uid"}}]}, "scope": window},
        {"pattern": {"nodes": [{"var": "p"}, {"var": "q"}]},
         "return": {"rows": [{"as": "q", "expr": {"col": "q.uid"}}]}, "scope": NOW},
        {"pattern": {"nodes": [{"var": "p"}]},
         "return": {"rows": [{"as": "x", "expr": {"col": "p.nope"}}]}, "scope": NOW},
    ):
        with pytest.raises(InvalidArgError):
            compile_match(validate(args))


# ---------------------------------------------------------------------------
# random stores, random arguments
# ---------------------------------------------------------------------------

UIDS = [f"n{i}" for i in range(9)]
LABELS = ["A", "B"]
RELS = ["R", "S"]
T_MAX = 60
_stores: dict[int, tuple[Any, Oracle, int]] = {}


def random_store(seed: int):
    if seed in _stores:
        return _stores[seed]
    rng = random.Random(seed)
    a = fresh_adapter()
    # a dense base: most nodes believed and valid throughout, a dozen edges,
    # so that random patterns mostly match something; then random history
    _apply(a, [node(u, rng.choice(LABELS),
                    {"w": rng.randrange(5), "s": rng.choice(["x", "xy", "y"]),
                     "f": rng.choice([0.5, 1.25, 2.0]), "z": rng.choice([1, "1", True, None])})
               for u in UIDS[:-2]], 1)
    _apply(a, [edge(rng.choice(UIDS), rng.choice(UIDS), rng.choice(RELS),
                    {"w": rng.randrange(4)}, rng.randrange(0, 20), OPEN_END, disc=str(i))
               for i in range(14)], 2)
    tt = 2
    for _ in range(45):
        tt += 1
        kind = rng.choice(["an", "an", "ae", "ae", "ae", "ae", "rn", "re", "co", "ce"])
        u, v = rng.choice(UIDS), rng.choice(UIDS)
        s = rng.randrange(0, T_MAX - 1)
        e = rng.choice([OPEN_END, s + rng.randrange(1, T_MAX - s + 1)])
        try:
            if kind == "an":
                _apply(a, [node(u, rng.choice(LABELS),
                                {"w": rng.randrange(5), "s": rng.choice(["x", "xy", "y"]),
                                 "f": rng.choice([0.5, 1.25, 2.0]),
                                 "z": rng.choice([1, "1", True, None])}, s, e)], tt)
            elif kind == "ae":
                _apply(a, [edge(u, v, rng.choice(RELS), {"w": rng.randrange(4)}, s, e,
                                disc=rng.choice(["", "", "1"]))], tt)
            elif kind == "rn":
                _apply(a, [{"op": "retract", "ref": {"kind": "node", "uid": u},
                            "t": rng.randrange(0, T_MAX)}], tt)
            elif kind == "re":
                _apply(a, [{"op": "retract",
                            "ref": {"kind": "edge", "src": u, "dst": v,
                                    "rel_type": rng.choice(RELS), "disc": ""},
                            "t": rng.randrange(0, T_MAX)}], tt)
            elif kind == "co":
                _apply(a, [{"op": "correct", "ref": {"kind": "node", "uid": u},
                            "props": {"w": rng.randrange(5, 9), "s": "z"},
                            "vt_s": s, "vt_e": e}], tt)
            else:
                _apply(a, [{"op": "correct",
                            "ref": {"kind": "edge", "src": u, "dst": v,
                                    "rel_type": rng.choice(RELS), "disc": ""},
                            "props": {"w": 9}, "vt_s": s, "vt_e": e}], tt)
        except (NotFoundError, InvalidArgError):
            pass
    oracle = Oracle(list(a.all_node_versions()), list(a.all_edge_versions()))
    _stores[seed] = (a, oracle, tt)
    return _stores[seed]


def random_args(rng: random.Random, max_tt: int) -> dict[str, Any]:
    snapshot = rng.random() < 0.7
    if snapshot:
        scope = {"vt": {"instant": rng.randrange(0, T_MAX)},
                 "as_of_tt": rng.choice([OPEN_END, rng.randrange(1, max_tt + 1)])}
    else:
        vt = rng.choice([{"window": {"t_a": 0, "t_b": OPEN_END}},
                         {"window": {"t_a": rng.randrange(0, 30), "t_b": rng.randrange(30, 61)},
                          "mode": rng.choice(["overlap", "event"])},
                         {"instant": rng.randrange(0, T_MAX)}])
        scope = {"vt": vt, "as_of_tt": rng.choice([OPEN_END, rng.randrange(1, max_tt + 1)]),
                 "belief": rng.choice(["current", "superseded", "all"])
                 if "instant" not in vt else rng.choice(["superseded", "all"])}
    n_vars = rng.randint(1, 4)
    vs = [f"v{i}" for i in range(n_vars)]
    nodes = []
    for v in vs:
        nd: dict[str, Any] = {"var": v}
        if rng.random() < 0.4:
            nd["labels"] = rng.sample(LABELS, rng.randint(1, 2))
        if rng.random() < 0.2:
            nd["uids"] = rng.sample(UIDS, rng.randint(1, 3))
        nodes.append(nd)
    steps = []
    for i in range(1, n_vars):
        other = vs[rng.randrange(i)]
        a, b = (other, vs[i]) if rng.random() < 0.5 else (vs[i], other)
        st: dict[str, Any] = {"from": a, "to": b, "rel_type": rng.choice(RELS + [None])}
        if snapshot:
            st["dir"] = rng.choice(["out", "in", "both"])
            st["hops"] = rng.choice([{"exact": 1}, {"exact": 1}, {"exact": 2},
                                     {"min": 0, "max": 2}, {"min": 1, "max": None}])
        if rng.random() < 0.5 and st.get("hops", {"exact": 1}) == {"exact": 1}:
            st["var"] = f"e{i}"
        steps.append(st)
    if n_vars >= 3 and rng.random() < 0.3:                 # a closing step
        steps.append({"from": vs[-1], "to": vs[0], "rel_type": rng.choice(RELS),
                      **({"dir": rng.choice(["out", "both"])} if snapshot else {})})
    where = []
    if rng.random() < 0.5:
        v = rng.choice(vs)
        where.append({"cmp": rng.choice(["<", ">=", "=", "!="]),
                      "l": {"tprop": [v, "w"], "tau": "int"}, "r": {"lit": rng.randrange(5)}})
    if rng.random() < 0.3:
        where.append({"cmp": "<=", "l": {"col": f"{rng.choice(vs)}.vt_s"},
                      "r": {"lit": rng.randrange(T_MAX)}})
    pattern: dict[str, Any] = {"nodes": nodes, "steps": steps, "where": where}
    if snapshot and rng.random() < 0.3:
        key = "optional" if rng.random() < 0.6 else "not"
        pattern[key] = [{"steps": [{"var": "o1", "from": rng.choice(vs), "to": "x9",
                                    "rel_type": rng.choice(RELS), "dir": rng.choice(["out", "in"])}],
                         "where": []}]
    edge_vars = [s["var"] for s in steps if s.get("var")]
    cols = [f"{v}.uid" for v in vs] + [f"{e}.eid" for e in edge_vars] + \
        [f"{v}.vt_s" for v in vs]
    if rng.random() < 0.55:
        picked = rng.sample(cols, rng.randint(1, min(3, len(cols))))
        rows = [{"as": f"c{i}", "expr": {"col": c}} for i, c in enumerate(picked)]
        if rng.random() < 0.4:
            rows.append({"as": "w", "expr": {"tprop": [rng.choice(vs), "w"], "tau": "int"}})
        if rng.random() < 0.2:
            rows.append({"as": "pr", "expr": {"prop": [rng.choice(vs), "z"]}})
        if "optional" in pattern:
            rows.append({"as": "ox", "expr": {"col": "x9.uid"}})
        ret: dict[str, Any] = {"rows": rows}
        if rng.random() < 0.5:
            ret["order_by"] = [{"expr": {"col": rows[0]["as"]},
                                "dir": rng.choice(["asc", "desc"]),
                                "nulls": rng.choice(["first", "last"])}]
    else:
        by = [{"as": "g", "expr": {"col": f"{rng.choice(vs)}.uid"}}] if rng.random() < 0.7 else []
        v = rng.choice(vs)
        aggs = [{"as": "n", "fn": "count"},
                {"as": "d", "fn": "count_distinct", "of": {"col": f"{rng.choice(vs)}.uid"}},
                {"as": "s", "fn": "sum", "of": {"tprop": [v, "w"], "tau": "int"}},
                {"as": "mx", "fn": rng.choice(["min", "max"]),
                 "of": {"tprop": [v, "f"], "tau": "float"}},
                {"as": "mn", "fn": "mean", "of": {"tprop": [v, "w"], "tau": "int"}}]
        if by and rng.random() < 0.4:
            aggs.append({"as": "sd", "fn": "sum", "of": {"tprop": [v, "f"], "tau": "float"},
                         "distinct_on": [v]})
        group: dict[str, Any] = {"by": by, "aggs": aggs}
        if by and rng.random() < 0.3:
            group["then"] = {"by": [{"as": "n", "expr": {"col": "n"}}],
                             "aggs": [{"as": "k", "fn": "count"}]}
        ret = {"group": group}
    if rng.random() < 0.4:
        ret["limit"] = rng.randint(1, 4)
    if rng.random() < 0.3:
        axes = rng.choice([["valid"], ["transaction"], ["valid", "transaction"]])
        scope["co_valid"] = axes
        if "rows" in ret:
            for ax in axes:
                s_, e_ = ("valid.s", "valid.e") if ax == "valid" else ("belief.s", "belief.e")
                ret["rows"] += [{"as": f"cs_{ax[0]}", "expr": {"col": s_}},
                                {"as": f"ce_{ax[0]}", "expr": {"col": e_}}]
            if rng.random() < 0.5:
                pattern["where"].append({"cmp": ">", "l": {"col": "valid.e" if "valid" in axes
                                                              else "belief.e"},
                                         "r": {"lit": rng.randrange(T_MAX)}})
    _expression_family(rng, vs, pattern, ret)
    _lists_and_top_per(rng, vs, ret)
    if snapshot and scope.get("belief", "current") == "current" and rng.random() < 0.3:
        a = rng.choice(vs)
        b = rng.choice(vs) if rng.random() < 0.3 else "w9"
        pattern["steps"].append({"from": a, "to": b, "regex": _random_regex(rng, 2)})
    _ordered_and_truncated(rng, vs, pattern, snapshot and scope.get("belief", "current")
                           == "current")
    if rng.random() < 0.2:
        on = rng.choice(vs)
        pattern["lookups"] = [{"as": "lk", "on": {"col": f"{on}.uid"}, "key": "q",
                               "join": rng.choice(["left", "inner"]), "match": {
            "pattern": {"nodes": [{"var": "lq"}, {"var": "lr"}],
                        "steps": [{"from": "lq", "to": "lr", "rel_type": rng.choice(RELS)}]},
            "group": {"by": [{"as": "q", "expr": {"col": "lq.uid"}}],
                      "aggs": [{"as": "n", "fn": "count"},
                               {"as": "mw", "fn": "max",
                                "of": {"tprop": ["lr", "w"], "tau": "int"}}]}}}]
        if rng.random() < 0.4:
            pattern["where"].append({"cmp": ">=", "l": {"coalesce": [{"col": "lk.n"}, {"lit": 0}]},
                                     "r": {"lit": rng.randint(0, 2)}})
        if "rows" in ret:
            ret["rows"].append({"as": "lkn", "expr": {"col": "lk.n"}})
    if rng.random() < 0.25:
        edge_vars = [s["var"] for s in pattern["steps"] if s.get("var") and "regex" not in s
                     and s.get("hops", {"exact": 1}) == {"exact": 1}]
        of = rng.choice(vs + edge_vars)
        pattern["succ"] = [{"var": "s9", "of": of,
                            "axis": rng.choice(["valid", "transaction"]),
                            "dir": rng.choice(["next", "prev"])}]
        if "rows" in ret and rng.random() < 0.7:
            ret["rows"].append({"as": "sv", "expr": {"col": "s9.vt_s"}})
    args = {"pattern": pattern, "return": ret, "scope": scope,
            "limit": rng.choice([100, 2]), "cursor": rng.choice([None, None, "1"])}
    if rng.random() < 0.15 and "rows" in ret and \
            not any("prop" in r["expr"] for r in ret["rows"]):
        # the same rows from two branches: the pattern, and it without its where
        other = {**pattern, "where": []}
        args["union"] = [{"pattern": pattern, "rows": ret["rows"]},
                         {"pattern": other, "rows": ret["rows"]}]
        del args["pattern"]
        args["return"] = {**{k: v for k, v in ret.items() if k != "rows"},
                          "rows": [{"as": r["as"], "expr": {"col": r["as"]}}
                                   for r in ret["rows"]]}
    return args


def _ordered_and_truncated(rng: random.Random, vs: list[str], pattern: dict,
                           instant: bool) -> None:
    """Ordered or filtered variable-length steps (instant scopes), and a
    truncation on a single hop that binds its edge."""
    if instant and rng.random() < 0.3:
        st: dict[str, Any] = {"from": rng.choice(vs), "to": "w8", "rel_type": rng.choice(RELS),
                              "dir": rng.choice(["out", "in"]),
                              "hops": rng.choice([{"min": 1, "max": 3}, {"min": 0, "max": 2},
                                                  {"min": 1, "max": None}, {"exact": 2}])}
        if rng.random() < 0.7:
            st["order"] = rng.choice(["strict_increasing", "increasing", "strict_decreasing",
                                      "decreasing"])
        st["edge_where"] = {"var": "tq", "where": [{"cmp": "<", "l": {"col": "tq.vt_s"},
                                                     "r": {"lit": rng.randrange(5, T_MAX)}}]
                            if rng.random() < 0.5 else []}
        if rng.random() < 0.4:
            st["truncate"] = {"limit": rng.randint(1, 2), "order_by": [
                {"expr": rng.choice([{"col": "tq.vt_s"}, {"tprop": ["tq", "w"], "tau": "int"}]),
                 "dir": rng.choice(["asc", "desc"])}]}
        if rng.random() < 0.5:
            st["from"], st["to"] = "w8", st["from"]            # read from its end
        pattern["steps"].append(st)
    singles = [s for s in pattern["steps"] if s.get("var") and "regex" not in s
               and s.get("hops", {"exact": 1}) == {"exact": 1}]
    if singles and rng.random() < 0.25:
        one = rng.choice(singles)
        one["truncate"] = {"limit": rng.randint(1, 2), "order_by": [
            {"expr": {"tprop": [one["var"], "w"], "tau": "int"},
             "dir": rng.choice(["asc", "desc"])}]}


def _random_regex(rng: random.Random, depth: int) -> dict:
    k = rng.choice(["sym", "sym", "seq", "alt", "star", "plus", "opt"]) if depth else "sym"
    if k == "sym":
        return {"sym": rng.choice(RELS), **({"inv": True} if rng.random() < 0.4 else {})}
    if k in ("seq", "alt"):
        return {k: [_random_regex(rng, depth - 1) for _ in range(rng.randint(1, 3))]}
    return {k: _random_regex(rng, depth - 1)}


def _lists_and_top_per(rng: random.Random, vs: list[str], ret: dict) -> None:
    """Collected lists and a per-partition top-k (drawn after the others)."""
    if "group" in ret:
        g = ret["group"]
        if rng.random() < 0.35:
            v = rng.choice(vs)
            g["aggs"].append({"as": "cl", "fn": "collect",
                              "of": rng.choice([{"col": f"{v}.uid"},
                                                [{"col": f"{v}.uid"},
                                                 {"tprop": [v, "w"], "tau": "int"}]]),
                              "order_by": [{"expr": {"tprop": [v, "s"], "tau": "str"},
                                            "dir": rng.choice(["asc", "desc"]),
                                            "nulls": rng.choice(["first", "last"])}],
                              "limit": rng.randint(1, 3), "distinct": rng.random() < 0.5})
        if g["by"] and rng.random() < 0.25 and "then" not in g:
            ret["top_per"] = {"by": [{"col": g["by"][0]["as"]}],
                              "order_by": [{"expr": {"col": "s"}, "dir": "desc"}],
                              "k": rng.randint(1, 2)}
        return
    rows = [r for r in ret["rows"] if "prop" not in r["expr"]]    # typed columns
    if rng.random() < 0.25:
        ret["top_per"] = {"by": [{"col": rows[0]["as"]}],
                          "order_by": [{"expr": {"col": rows[-1]["as"]},
                                        "dir": rng.choice(["asc", "desc"])}],
                          "k": rng.randint(1, 2)}


def _expression_family(rng: random.Random, vs: list[str], pattern: dict, ret: dict) -> None:
    """Conditional aggregates, stage expressions with a total, and the
    arithmetic, calendar and string forms (drawn after every other choice,
    so the earlier draws of each case are unchanged)."""
    v = rng.choice(vs)
    w = {"tprop": [v, "w"], "tau": "int"}
    s = {"tprop": [v, "s"], "tau": "str"}
    if rng.random() < 0.2:
        pattern["where"].append({"strfn": rng.choice(["like", "contains"]),
                                 "args": [s, {"lit": rng.choice(["x%", "%y", "_", "x"])}]})
    if "group" in ret:
        g = ret["group"]
        if rng.random() < 0.5:
            pick = rng.choice([a for a in g["aggs"] if not a.get("distinct_on")] or g["aggs"])
            pick["where"] = {"cmp": rng.choice(["<", ">="]), "l": w, "r": {"lit": rng.randrange(5)}}
        if rng.random() < 0.4:
            g["project"] = [{"as": "pc", "expr": {"arith": "/", "l": {"col": "n"},
                                                  "r": {"total": "n"}}},
                            {"as": "ad", "expr": {"fn": "abs", "arg": {
                                "arith": "-", "l": {"col": "s"}, "r": {"lit": 3}}}}]
        return
    if rng.random() < 0.5:
        f = {"tprop": [v, "f"], "tau": "float"}
        ret["rows"] += [
            {"as": "x1", "expr": {"if": [{"cmp": "!=", "l": w, "r": {"lit": 2}},
                                         {"arith": "/", "l": {"lit": 7},
                                          "r": {"arith": "-", "l": w, "r": {"lit": 2}}},
                                         {"lit": -1.0}]}},
            {"as": "x2", "expr": {"fn": rng.choice(["round", "floor", "ceil"]),
                                  "arg": {"arith": "*", "l": f, "r": {"lit": 2.0}}}},
            {"as": "x3", "expr": {"arith": "floor_div", "l": w, "r": {"lit": rng.choice([-2, 3])}}},
            {"as": "x4", "expr": {"strfn": "substring", "args": [
                s, {"lit": rng.randrange(-1, 3)}, {"lit": rng.randrange(0, 3)}]}},
            {"as": "x5", "expr": {"calendar": rng.choice(["year", "month", "day", "month_day"]),
                                  "arg": {"arith": "*", "l": {"col": f"{v}.vt_s"},
                                          "r": {"lit": 17 * 86_400_000_000}}}},
            {"as": "x6", "expr": {"calendar": "months_between",
                                  "arg": {"arith": "*", "l": {"col": f"{v}.vt_s"},
                                          "r": {"lit": 29 * 86_400_000_000}},
                                  "other": {"lit": 1_614_556_800_000_000}}},
        ]


@pytest.mark.parametrize("case", range(EXAMPLES))
def test_random_arguments_agree_with_the_oracle(case):
    ensure_all_registered()
    a, oracle, max_tt = random_store(case % 5)
    rng = random.Random(1000 + case)
    compared = 0
    for _ in range(12):
        args = random_args(rng, max_tt)
        try:
            pair = both(a, oracle, args)
        except InvalidArgError as e:
            # a data-dependent refusal (a null join key) must be the oracle's too
            with pytest.raises(InvalidArgError):
                oracle.match(validate(args))
            del e
            continue
        if pair is None:
            continue
        got, want = pair
        assert canonical_json(got) == canonical_json(want), args
        compared += 1
    assert compared > 0


def random_rpq_args(rng: random.Random, max_tt: int) -> dict[str, Any]:
    """A conjunctive regular path query: regex steps only, a distinct
    projection of some identities (set semantics, existential variables
    projected away), sometimes as a two-body union."""
    def body() -> dict[str, Any]:
        n = rng.randint(2, 5)
        vs = [f"v{i}" for i in range(n)]
        steps = []
        for i in range(1, n):
            a = vs[rng.randrange(i)]
            steps.append({"from": a, "to": vs[i], "regex": _random_regex(rng, 2)}
                         if rng.random() < 0.5 else
                         {"from": vs[i], "to": a, "regex": _random_regex(rng, 2)})
        if rng.random() < 0.3:
            steps.append({"from": vs[-1], "to": vs[0], "regex": _random_regex(rng, 1)})
        nodes = [{"var": v, **({"labels": [rng.choice(LABELS)]} if rng.random() < 0.2 else {})}
                 for v in vs]
        return {"nodes": nodes, "steps": steps}
    head = [f"v{i}" for i in sorted(rng.sample(range(2), rng.randint(0, 2)))]
    scope = {"vt": {"instant": rng.randrange(0, T_MAX)},
             "as_of_tt": rng.choice([OPEN_END, rng.randrange(1, max_tt + 1)])}
    by = [{"as": v, "expr": {"col": f"{v}.uid"}} for v in head] or \
        [{"as": "one", "expr": {"lit": 1}}]
    if rng.random() < 0.3:
        rows = [{"as": v, "expr": {"col": f"{v}.uid"}} for v in head] or \
            [{"as": "one", "expr": {"lit": 1}}]
        return {"union": [{"pattern": body(), "rows": rows}, {"pattern": body(), "rows": rows}],
                "return": {"group": {"by": [{"as": r["as"], "expr": {"col": r["as"]}}
                                            for r in rows], "aggs": []}},
                "scope": scope, "limit": 100}
    return {"pattern": body(), "return": {"group": {"by": by, "aggs": []}},
            "scope": scope, "limit": 100}


@pytest.mark.parametrize("case", range(EXAMPLES))
def test_random_regular_path_queries_agree_with_the_oracle(case):
    ensure_all_registered()
    a, oracle, max_tt = random_store(case % 5)
    rng = random.Random(7000 + case)
    for _ in range(8):
        args = random_rpq_args(rng, max_tt)
        pair = both(a, oracle, args)
        assert pair is not None, args
        got, want = pair
        assert canonical_json(got) == canonical_json(want), args
