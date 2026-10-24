"""`match` orders a pattern's steps by estimated cardinality: the IC14 /
BI19 / BI15 weight pattern (friends and the replies between them) is seeded
from the replies, not from the friendships, and every reordered plan answers
what the oracle does (the random suites of `test_match_oracle` run with the
adapter's counts too)."""

from __future__ import annotations

import os
import random

import pytest

from gnomon.core.model import OPEN_END, canonical_json
from gnomon.temporal import ops_match as OM
from gnomon.temporal.algebra import ensure_all_registered
from gnomon.temporal.ops_match import compile_match, run, validate
from gnomon.temporal.oracle import Oracle

from . import test_match_oracle as T
from .conftest import fresh_adapter

REPLIES = {
    "pattern": {
        "nodes": [{"var": "pa", "labels": ["Person"]}, {"var": "pb", "labels": ["Person"]},
                  {"var": "msg", "labels": ["Post", "Comment"]},
                  {"var": "rep", "labels": ["Comment"]}],
        "steps": [{"from": "pa", "to": "pb", "rel_type": "KNOWS", "dir": "both"},
                  {"from": "msg", "to": "pa", "rel_type": "HAS_CREATOR", "dir": "out"},
                  {"from": "rep", "to": "pb", "rel_type": "HAS_CREATOR", "dir": "out"},
                  {"from": "rep", "to": "msg", "rel_type": "REPLY_OF", "dir": "out"}],
        "where": [{"cmp": "!=", "l": {"col": "pa.uid"}, "r": {"col": "pb.uid"}}]},
    "return": {"group": {"by": [{"as": "src", "expr": {"col": "pa.uid"}},
                                {"as": "dst", "expr": {"col": "pb.uid"}}],
                         "aggs": [{"as": "n", "fn": "count"}]}},
    "limit": 1000,
}
#: SF1-like magnitudes: 10k persons, KNOWS degree 40, 80 messages per person
SF1 = {"labels": {"Person": 10_000, "Post": 320_000, "Comment": 480_000},
       "rels": {"KNOWS": 200_000, "HAS_CREATOR": 800_000, "REPLY_OF": 480_000},
       "nodes": 810_000}


def test_the_reply_pattern_is_seeded_from_the_replies():
    anchor, steps = OM.plan_order(REPLIES["pattern"], SF1)
    assert anchor == "rep"
    assert steps[0]["rel_type"] == "REPLY_OF"
    assert steps[-1]["rel_type"] == "KNOWS"           # the friendship closes the pattern


def test_written_orders_that_are_fine_and_unsafe_shapes_are_kept():
    # a uid anchor, too few steps, or an order-dependent step: unchanged
    with_uid = {**REPLIES["pattern"], "nodes": [{"var": "pa", "uids": ["p1"]}]
                + REPLIES["pattern"]["nodes"][1:]}
    assert OM.plan_order(with_uid, SF1) is None
    assert OM.plan_order({**REPLIES["pattern"], "steps": REPLIES["pattern"]["steps"][:1]},
                         SF1) is None
    truncated = [dict(s) for s in REPLIES["pattern"]["steps"]]
    truncated[1]["truncate"] = {"limit": 1, "order_by": [{"expr": {"col": "msg.vt_s"}}]}
    assert OM.plan_order({**REPLIES["pattern"], "steps": truncated}, SF1) is None
    good = {**REPLIES["pattern"], "steps": OM.plan_order(REPLIES["pattern"], SF1)[1]}
    good["nodes"] = [n for n in REPLIES["pattern"]["nodes"] if n["var"] == "rep"] + \
        [n for n in REPLIES["pattern"]["nodes"] if n["var"] != "rep"]
    assert OM.plan_order(good, SF1) is None             # already the cheap order
    assert OM.plan_order(REPLIES["pattern"], None) is None


def test_the_counts_reorder_the_plan_and_not_the_answer():
    plain = compile_match(validate({**REPLIES, "scope": {"vt": {"instant": 5}}}))
    planned = compile_match(validate({**REPLIES, "scope": {"vt": {"instant": 5}}}), SF1)
    assert plain.doc != planned.doc and plain.columns == planned.columns


@pytest.fixture(scope="module")
def social():
    """30 persons, KNOWS stored once, posts and comments replying to friends',
    some corrected and some ending inside the window."""
    ensure_all_registered()
    rng = random.Random(4)
    a = fresh_adapter()
    people = [f"p{i}" for i in range(30)]
    T._apply(a, [T.node(p, "Person", {"w": i}) for i, p in enumerate(people)], 1)
    ks, seen = [], set()
    for p in people:
        for q in rng.sample(people, 4):
            if q != p and (q, p) not in seen:
                seen.add((p, q))
                ks.append(T.edge(p, q, "KNOWS", vt_s=rng.randrange(0, 20)))
    T._apply(a, ks, 2)
    msgs, ops = [], []
    for i in range(240):
        m, who = f"m{i}", rng.choice(people)
        lo = rng.randrange(0, 40)
        hi = OPEN_END if rng.random() < 0.8 else lo + rng.randrange(1, 30)
        if msgs and rng.random() < 0.6:
            ops.append(T.node(m, "Comment", {"len": i}, lo, hi))
            ops.append(T.edge(m, rng.choice(msgs), "REPLY_OF", vt_s=lo, vt_e=hi))
        else:
            ops.append(T.node(m, "Post", {"len": i}, lo, hi))
        ops.append(T.edge(m, who, "HAS_CREATOR", vt_s=lo, vt_e=hi))
        msgs.append(m)
    T._apply(a, ops, 3)
    T._apply(a, [{"op": "retract", "ref": {"kind": "node", "uid": "p3"}, "t": 25}], 4)
    oracle = Oracle(list(a.all_node_versions()), list(a.all_edge_versions()))
    yield a, oracle
    a.close()


@pytest.mark.parametrize("scope", [
    {"vt": {"instant": 30}},
    {"vt": {"instant": 60}, "as_of_tt": 3},
    {"vt": {"window": {"t_a": 10, "t_b": 50}}, "co_valid": ["valid"]},
    {"vt": {"window": {"t_a": 0, "t_b": OPEN_END}}},
])
def test_the_reply_pattern_answers_as_the_oracle(social, scope):
    a, oracle = social
    args = {**REPLIES, "scope": scope}
    card = OM.cardinalities(a)
    assert card is not None and OM.plan_order(REPLIES["pattern"], card) is not None
    got = run(a, validate(args), route="reference", reference_bypass="test store")
    assert got["rows_total"] > 0
    assert canonical_json(got) == canonical_json(oracle.match(validate(args)))
    if os.environ.get("GNOMON_TEST_BACKEND") == "native":
        before = OM.ROUTES["native"]
        nat = run(a, validate(args), route="native")
        assert OM.ROUTES["native"] > before, OM.LAST_DECLINE[0]
        assert canonical_json(nat) == canonical_json(got)
