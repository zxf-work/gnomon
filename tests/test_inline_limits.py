"""Argument and inline-pattern bounds sized for SF10.

At SNB SF10 the weight tables of IC14 and BI19 (one row per friendship
with replies; 357,700 at SF1) pass the inline-pattern bound of a million rows,
and the calls failed. A weight pattern read column-wise is five arrays, not a
dict per row, so it has its own, memory-shaped bound; every pattern read as
rows keeps the million.

BI11 binds a node variable to the persons of a country, computed by an
earlier call; at SF10 the largest country has more than 10,000 and `match`
refused the argument (the message cut inside the id list).
"""

from __future__ import annotations

import pytest

from gnomon.core.errors import InvalidArgError, LimitError
from gnomon.temporal import ops_general_paths as GP
from gnomon.temporal import ops_match as OM
from gnomon.temporal.algebra import call_operator, ensure_all_registered

from .test_paths_operator import REPLIES, _snb

ensure_all_registered()

ARGS = {"sources": {"uids": ["a", "b"]}, "targets": {"uids": ["c", "e"]}, "t_valid": 5,
        "dir": "both", "rel_types": ["KNOWS"], "metric": "weight",
        "weight": {"match": REPLIES, "missing": "default", "default": 10},
        "result": "distance", "pairs": "min_over_pairs"}


def test_a_column_wise_weight_table_is_not_held_to_the_row_bound(monkeypatch):
    nat, duck, _ = _snb()
    want = call_operator(nat, "paths", dict(ARGS))["rows"]
    assert want == call_operator(duck, "paths", dict(ARGS))["rows"]
    monkeypatch.setattr(GP, "MAX_PATTERN_ROWS", 2)
    # the engine reads the six pairs column-wise: the row bound does not apply
    assert call_operator(nat, "paths", dict(ARGS))["rows"] == want
    # read as rows, the same pattern is held to it
    with pytest.raises(LimitError, match="more than 2 rows"):
        call_operator(duck, "paths", dict(ARGS))


def test_the_column_wise_bound_still_stops_a_runaway_pattern(monkeypatch):
    nat, _, _ = _snb()
    monkeypatch.setattr(GP, "MAX_PAIR_ROWS", 2)
    with pytest.raises(LimitError, match="more than 2 rows"):
        call_operator(nat, "paths", dict(ARGS))
    assert GP.MAX_PAIR_ROWS == 2 and GP.MAX_PATTERN_ROWS == 1_000_000


def test_the_pair_bound_is_sized_for_sf10():
    assert GP.MAX_PAIR_ROWS >= 10 * 1_000_000 > GP.MAX_PATTERN_ROWS


def test_a_probe_sized_uid_list_is_an_argument():
    """20,000 uids (BI11 at SF10 binds more than 10,000): the known ones answer."""
    nat, duck, _ = _snb()
    uids = ["a", "c"] + [f"absent{i}" for i in range(20_000)]
    q = {"pattern": {"nodes": [{"var": "x", "uids": uids}], "steps": []},
         "return": {"rows": [{"as": "uid", "expr": {"col": "x.uid"}}]},
         "scope": {"vt": {"instant": 5}}, "limit": 100}
    for a in (nat, duck):
        got = call_operator(a, "match", q)
        assert sorted(r["uid"] if isinstance(r, dict) else r[0] for r in got["rows"]) == ["a", "c"]


def test_the_uid_bound_still_refuses_a_runaway_list(monkeypatch):
    """The compiled validator reads the schema it was built from: a lowered
    bound refuses a longer list, as the shipped one refuses past a million."""
    assert OM.MAX_UIDS == 1_000_000
    uids = OM.ARGS_SCHEMA["$defs"]["node"]["properties"]["uids"]
    assert uids["maxItems"] == OM.MAX_UIDS
    monkeypatch.setitem(uids, "maxItems", 3)
    with pytest.raises(InvalidArgError):
        OM.validate({"pattern": {"nodes": [{"var": "x", "uids": ["a", "b", "c", "d"]}], "steps": []},
                     "return": {"rows": [{"as": "uid", "expr": {"col": "x.uid"}}]},
                     "scope": {"vt": {"instant": 5}}})
