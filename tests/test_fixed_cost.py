"""The fixed per-call cost of `call_operator` is memoized, and no memo can
change an answer, an envelope byte, or a validation verdict.

* The native `stats()` serves a per-generation summary. It must equal the
  statistics a DuckDB adapter computes for the same writes after every
  commit, which is the cross-backend agreement `estimate_cost` relies on.
* `validate_args` memoizes filled arguments. The caller must get a copy it
  can mutate, an invalid call must fail after a valid one that is equal as
  a value but not as Python types, and `args_echo` keeps the caller's key
  order.
"""

from __future__ import annotations

import pytest

from gnomon.core.errors import SchemaError
from gnomon.storage.duckdb_adapter import DuckDBAdapter
from gnomon.temporal.algebra import (
    REGISTRY, call_operator, ensure_all_registered, validate_args,
)

ensure_all_registered()


def _native(tmp):
    native = pytest.importorskip("gnomon.storage.native")
    a = native.NativeAdapter(tmp)
    a.paranoid = False
    return a


def _batches():
    """Six batches of nodes and edges with a skewed out-degree, so the
    maximum moves between commits."""
    out = []
    for b in range(6):
        ops = [{"op": "assert_node", "uid": f"n{i}", "label": "N",
                "props": {"name": f"node-{i}"}, "vt_s": 10 * b + i, "vt_e": 10 * b + i + 5}
               for i in range(b, b + 3)]
        for j in range(b + 1):
            ops.append({"op": "assert_edge", "src": f"n{b % 3}", "dst": f"n{j}",
                        "rel_type": "R" if j % 2 else "S", "props": {"w": j},
                        "vt_s": 10 * b + j, "vt_e": 10 * b + j + 3, "disc": f"{b}-{j}"})
        out.append(ops)
    return out


def test_native_stats_track_every_commit(tmp_path):
    nat, ref = _native(tmp_path / "n"), DuckDBAdapter(":memory:")
    keys = ("n_node_versions", "n_edge_versions", "vt_min", "vt_max",
            "rel_type_counts", "max_out_degree")
    for i, ops in enumerate(_batches()):
        for a in (nat, ref):
            a.begin()
            a.apply_ops(ops, 1000 + i)
            a.commit()
        got, want = nat.stats(), ref.stats()
        assert {k: got[k] for k in keys} == {k: want[k] for k in keys}, i
        # a repeated read (served from the summary) is the same value
        assert nat.stats() == got
    nat.compact()
    assert {k: nat.stats()[k] for k in keys} == {k: ref.stats()[k] for k in keys}


def test_validate_args_hands_out_copies():
    def args():     # a fresh value each time: the caller's own objects are its own
        return {"group_by": [{"dim": "rel_type"}], "aggregates": [{"agg": "count"}],
                "window": {"t_a": 0, "t_b": 100}}
    validate_args("aggregate_events", args())
    first = validate_args("aggregate_events", args())      # a memo hit
    first["window"]["t_a"] = 99
    first["group_by"].append({"dim": "bogus"})
    first["limit"] = -1
    again = validate_args("aggregate_events", args())
    assert again["window"] == {"t_a": 0, "t_b": 100}
    assert again["group_by"] == [{"dim": "rel_type"}]
    assert again["limit"] == REGISTRY["aggregate_events"].args_schema["properties"]["limit"]["default"]
    assert again is not validate_args("aggregate_events", args())


def test_an_invalid_call_still_fails_after_an_equal_valid_one():
    assert validate_args("resolve_entities", {"query": "a", "limit": 1})["limit"] == 1
    # True == 1 in Python, but a boolean is not a JSON Schema integer
    with pytest.raises(SchemaError):
        validate_args("resolve_entities", {"query": "a", "limit": True})
    with pytest.raises(SchemaError):
        validate_args("resolve_entities", {"query": "a", "limit": 0})
    # and the valid one is still served after the failures
    assert validate_args("resolve_entities", {"query": "a", "limit": 1})["limit"] == 1


def test_args_echo_keeps_the_callers_key_order(tmp_path):
    a = _native(tmp_path / "n")
    for i, ops in enumerate(_batches()):
        a.begin()
        a.apply_ops(ops, 1000 + i)
        a.commit()
    x = call_operator(a, "resolve_entities", {"query": "node", "limit": 2})
    y = call_operator(a, "resolve_entities", {"limit": 2, "query": "node"})
    assert list(x["args_echo"])[:2] == ["query", "limit"]
    assert list(y["args_echo"])[:2] == ["limit", "query"]
