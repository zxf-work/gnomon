"""`match` in the registry: the engine runs it as an opaque leaf through
`call_operator`, and no base-schema surface lists or dispatches it."""

from __future__ import annotations

import json

import pytest

from gnomon.core.model import OPEN_END
from gnomon.temporal import ops_match
from gnomon.temporal.algebra import (
    BASE_OPERATORS, REGISTRY, call_operator, ensure_all_registered,
)

from .conftest import fresh_adapter

#: Every operator registered for the engine and off the base-schema
#: surfaces; a new one joins here in its registration commit.
ENGINE_ONLY = {"match", "diff_belief", "paths", "state_series"}

ARGS = {"pattern": {"nodes": [{"var": "a"}, {"var": "b"}],
                    "steps": [{"from": "a", "to": "b", "rel_type": "R"}]},
        "return": {"rows": [{"as": "src", "expr": {"col": "a.uid"}},
                            {"as": "dst", "expr": {"col": "b.uid"}}],
                   "order_by": [{"expr": {"col": "src"}}, {"expr": {"col": "dst"}}]},
        "scope": {"vt": {"instant": "$t"}, "as_of_tt": "$b"},
        "params": {"t": 20, "b": 2}}


@pytest.fixture()
def adapter():
    ensure_all_registered()
    a = fresh_adapter()
    a.begin()
    a.apply_ops([
        {"op": "assert_node", "uid": u, "label": "N", "props": {}, "vt_s": 0,
         "vt_e": 100, "source": "i", "provenance_ref": None} for u in ("u1", "u2", "u3")
    ] + [
        {"op": "assert_edge", "src": "u1", "dst": "u2", "rel_type": "R", "props": {},
         "vt_s": 0, "vt_e": 100, "disc": "", "source": "i", "provenance_ref": None},
    ], 1)
    a.commit()
    a.begin()
    a.apply_ops([
        {"op": "assert_edge", "src": "u2", "dst": "u3", "rel_type": "R", "props": {},
         "vt_s": 0, "vt_e": 100, "disc": "", "source": "i", "provenance_ref": None},
    ], 3)
    a.commit()
    yield a
    a.close()


def test_match_is_registered_and_kept_off_the_agent_surfaces():
    ensure_all_registered()
    assert "match" in REGISTRY
    assert "match" not in BASE_OPERATORS
    assert set(REGISTRY) - BASE_OPERATORS == ENGINE_ONLY
    assert len(BASE_OPERATORS) == 15


def test_call_operator_runs_match_through_its_leaf(adapter):
    env = call_operator(adapter, "match", json.loads(json.dumps(ARGS)))
    direct = ops_match.match(adapter, json.loads(json.dumps(ARGS)))
    assert env["rows"] == direct["rows"] == [{"src": "u1", "dst": "u2"}]
    assert env["columns"] == ["src", "dst"]
    # the echo is the caller's argument; Σ and the pin are its bound scope's
    assert env["args_echo"]["scope"] == {"vt": {"instant": "$t"}, "as_of_tt": "$b"}
    assert env["pinned"] is True and env["tt_q"] == 2
    now = call_operator(adapter, "match", {**ARGS, "params": {"t": 20, "b": OPEN_END}})
    assert now["rows"] == [{"src": "u1", "dst": "u2"}, {"src": "u2", "dst": "u3"}]


def test_a_malformed_match_is_refused_before_the_leaf(adapter):
    from gnomon.core.errors import GnomonError

    with pytest.raises(GnomonError):
        call_operator(adapter, "match", {"return": {"rows": []}, "scope": {"vt": {}}})



def test_validate_remembers_successes_only():
    """`validate` keeps the filled argument of a valid call (the registry
    and the operator both validate one call) and never an invalid one: an
    invalid argument fails on every call, and a caller's change to a
    returned argument never reaches the next call."""
    import copy

    import pytest

    from gnomon.core.errors import InvalidArgError
    from gnomon.temporal import ops_match as OM

    good = {"pattern": {"nodes": [{"var": "n", "uids": ["a"]}]},
            "return": {"rows": [{"as": "n", "expr": {"col": "n.uid"}}]},
            "scope": {"vt": {"instant": 5}}}
    first = OM.validate(copy.deepcopy(good))
    first["limit"] = -1
    first["scope"]["belief"] = "all"
    again = OM.validate(copy.deepcopy(good))
    assert again["limit"] == OM.DEFAULT_LIMIT and again["scope"]["belief"] == "current"
    bad = {**copy.deepcopy(good), "scope": {"vt": {"window": {"t_a": 9, "t_b": 3}}}}
    for _ in range(3):
        with pytest.raises(InvalidArgError):
            OM.validate(copy.deepcopy(bad))
    worse = {**copy.deepcopy(good), "return": {"rows": [], "group": {"by": [], "aggs": []}}}
    for _ in range(3):
        with pytest.raises(InvalidArgError):
            OM.validate(copy.deepcopy(worse))
