"""`sum` in `aggregate_events`, and the engine's extended fold.

Two additions, one contract.

* **`sum`** over `vt_s`, `duration` or a numeric property, registered for
  engine-level callers only: the base schema is unchanged. It is exact: an integer total over integers, and
  `math.fsum`'s correctly rounded total once a float takes part (`match`'s
  `sum_exact` rule, `props.sum_exact`).
* **`valid_at`**, also engine-level: keep only the events whose version is
  valid at an instant, the window's events that the state at that instant
  still holds (FinBench's TSR rows read "as of now").
* **The extended fold**: calls that read properties (`of: "prop"`,
  `prop_filter`) or restrict to a cohort (`endpoint_filter`) used to take the
  portable path on every backend. On the native engine they now take a fold
  that reads the typed values itself, and declines any call whose portable
  answer it cannot reproduce exactly (an integer outside int64, a non-finite
  float, an extreme with an equal but distinguishable rival).

Every answer below is checked four ways: the engine's fold, the portable
path on the same native store (`GNOMON_AGG_EXT=python`), the portable path on
DuckDB, and the brute-force oracle.
"""

from __future__ import annotations

import math
import os
import random
from contextlib import contextmanager
from typing import Any

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from gnomon.core.errors import SchemaError
from gnomon.core.model import OPEN_END, canonical_json
from gnomon.storage.duckdb_adapter import DuckDBAdapter
from gnomon.temporal import ops_aggregate
from gnomon.temporal.algebra import (
    REGISTRY,
    _canonicalize_floats,
    call_operator,
    ensure_all_registered,
    validate_args,
)
from gnomon.temporal.oracle import Oracle

from .conftest import ENVELOPE_META_KEYS

ensure_all_registered()

N_EXAMPLES = int(os.environ.get("GNOMON_HYP_EXAMPLES", "25"))
SETTINGS = settings(max_examples=N_EXAMPLES * 4, deadline=None,
                    suppress_health_check=[HealthCheck.too_slow,
                                           HealthCheck.data_too_large])

T_MAX = 60
UIDS = [f"u{i}" for i in range(8)]

#: Property values a real bag holds, plus the ones the engine must decline.
PLAIN = [0, 1, -1, 2, 7, 13, 2**53 + 1, -(2**60), 0.5, 2.25, -3.5, 0.1,
         0.30000000000000004, 123.45, 1e16 + 0.5, "3", "abc", True, False,
         None, [1], {"x": 1}]
#: Equal-but-distinguishable rivals (1 and 1.0, 0.0 and -0.0) and values
#: no int64 or finite float holds: the engine declines what they touch.
EXOTIC = [1.0, 0.0, -0.0, 2**63, -(2**64), 2**53]


def _native_adapter():
    import tempfile

    from gnomon.storage.native import NativeAdapter

    tmp = tempfile.TemporaryDirectory(prefix="gnomon-test-aggext-")
    a = NativeAdapter(tmp.name)
    a._test_tmpdir = tmp
    return a


def _apply(a, ops, tt):
    a.begin()
    try:
        a.apply_ops(ops, tt)
    except Exception:
        a.rollback()
        raise
    a.commit()


def _ops(seed: int, exotic: bool) -> list[list[dict[str, Any]]]:
    rng = random.Random(seed)
    pool = PLAIN + (EXOTIC if exotic else [])
    batches = []
    for _ in range(40):
        kind = rng.choice(["an", "ae", "ae", "ae", "rt", "co"])
        u, v = rng.choice(UIDS), rng.choice(UIDS)
        s = rng.randrange(0, T_MAX - 1)
        e = s + rng.randrange(1, T_MAX - s)
        bag = {k: rng.choice(pool) for k in ("amt", "w") if rng.random() < 0.8}
        rel = rng.choice(["R", "S"])
        disc = rng.choice(["", "1"])
        if kind == "an":
            op = {"op": "assert_node", "uid": u, "label": rng.choice(["A", "B"]),
                  "props": {}, "vt_s": s, "vt_e": e}
        elif kind == "ae":
            op = {"op": "assert_edge", "src": u, "dst": v, "rel_type": rel,
                  "props": bag, "vt_s": s,
                  "vt_e": OPEN_END if rng.random() < 0.2 else e, "disc": disc}
        elif kind == "rt":
            op = {"op": "retract", "ref": {"kind": "edge", "src": u, "dst": v,
                                           "rel_type": rel, "disc": disc},
                  "t": rng.randrange(0, T_MAX)}
        else:
            op = {"op": "correct", "ref": {"kind": "edge", "src": u, "dst": v,
                                           "rel_type": rel, "disc": disc},
                  "props": bag, "vt_s": s, "vt_e": e}
        batches.append([op])
    return batches


_stores: dict[tuple[int, bool], Any] = {}


def stores(seed: int, exotic: bool):
    key = (seed, exotic)
    if key not in _stores:
        nat, duck = _native_adapter(), DuckDBAdapter(":memory:")
        tt = 0
        for batch in _ops(seed, exotic):
            tt += 1
            for a in (nat, duck):
                try:
                    _apply(a, batch, tt)
                except Exception:  # noqa: BLE001 - an op the store refuses
                    pass
        oracle = Oracle(list(nat.all_node_versions()),
                        list(nat.all_edge_versions()))
        _stores[key] = (nat, duck, oracle)
    return _stores[key]


@contextmanager
def portable_only():
    old = os.environ.get(ops_aggregate.AGG_EXT_ENV)
    os.environ[ops_aggregate.AGG_EXT_ENV] = "python"
    try:
        yield
    finally:
        if old is None:
            del os.environ[ops_aggregate.AGG_EXT_ENV]
        else:
            os.environ[ops_aggregate.AGG_EXT_ENV] = old


def _payload(env: dict[str, Any]) -> str:
    return canonical_json({k: v for k, v in env.items()
                           if k not in ENVELOPE_META_KEYS})


def _call(adapter, args):
    try:
        return _payload(call_operator(adapter, "aggregate_events", dict(args)))
    except Exception as e:  # noqa: BLE001 - the routes must fail alike too
        return f"{type(e).__name__}"


# ---- strategies ------------------------------------------------------------ #

DIMS = [{"dim": "time_bucket"}, {"dim": "rel_type"},
        {"dim": "endpoint", "role": "src"}, {"dim": "endpoint", "role": "dst"},
        {"dim": "label", "role": "dst"}]
AGGS = ([{"agg": "count"}, {"agg": "count_distinct", "of": "dst"}]
        + [{"agg": f, "of": o} for f in ("min", "max", "mean", "sum")
           for o in ("vt_s", "duration")]
        + [{"agg": f, "of": "prop", "prop": p}
           for f in ("min", "max", "mean", "sum") for p in ("amt", "w")])
LITERALS = [0, 1, 2, 0.5, -1, "3", "abc", True, None, 2**70, 1e300]


@st.composite
def calls(draw) -> dict[str, Any]:
    dims = draw(st.lists(st.sampled_from(DIMS), max_size=2,
                         unique_by=lambda d: (d["dim"], d.get("role"))))
    aggs = draw(st.lists(st.sampled_from(AGGS), min_size=1, max_size=4,
                         unique_by=ops_aggregate._agg_field))
    t_a = draw(st.integers(0, T_MAX - 1))
    t_b = draw(st.integers(t_a + 1, T_MAX + 5))
    args: dict[str, Any] = {"group_by": dims, "aggregates": aggs,
                            "window": {"t_a": t_a, "t_b": t_b},
                            "as_of_tt": draw(st.one_of(st.integers(1, 40),
                                                       st.just(OPEN_END))),
                            "limit": draw(st.sampled_from([100, 3]))}
    if any(d["dim"] == "time_bucket" for d in dims):
        args["stride"] = draw(st.integers(1, 20))
    if draw(st.booleans()):
        args["prop_filter"] = {"prop": draw(st.sampled_from(["amt", "w", "zz"])),
                               "cmp": draw(st.sampled_from(
                                   ["eq", "ne", "lt", "le", "gt", "ge"])),
                               "value": draw(st.sampled_from(LITERALS))}
    if draw(st.booleans()):
        args["endpoint_filter"] = {
            "role": draw(st.sampled_from(["src", "dst", "either"])),
            "uids": draw(st.lists(st.sampled_from(UIDS + ["nobody"]),
                                  max_size=4))}
    if draw(st.booleans()):
        args["rel_types"] = draw(st.sampled_from([["R"], ["S"], ["R", "S"]]))
    if draw(st.booleans()):
        args["valid_at"] = draw(st.integers(0, T_MAX + 5))
    return args


# ---- answer identity ------------------------------------------------------- #

@SETTINGS
@given(args=calls(), seed=st.integers(0, 3), exotic=st.booleans())
def test_engine_fold_equals_portable_duckdb_and_oracle(args, seed, exotic):
    nat, duck, oracle = stores(seed, exotic)
    engine = _call(nat, args)
    with portable_only():
        portable = _call(nat, args)
    assert engine == portable, f"engine != portable for {args}"
    assert _call(duck, args) == portable, f"duckdb != native portable for {args}"
    if engine.startswith("{"):
        filled = validate_args("aggregate_events", dict(args))
        want = _canonicalize_floats(oracle.aggregate_events(filled))
        got = {k: v for k, v in call_operator(nat, "aggregate_events",
                                              dict(args)).items()
               if k not in ENVELOPE_META_KEYS}
        if "prop_filter" in args and "endpoint_filter" in args:
            # the oracle counts filter coercions over the whole window and
            # the operator over the cohort (it filters by cohort first); the
            # rows agree, the counts are each side's own definition
            got.pop("prop_coercion", None)
            want.pop("prop_coercion", None)
        assert canonical_json(got) == canonical_json(want), \
            f"engine != oracle for {args}"


@SETTINGS
@given(args=calls(), seed=st.integers(0, 3))
def test_engine_answers_plain_stores_without_declining(args, seed):
    """On values every route holds exactly, the engine never declines: the
    fold is the answer, not a detour back to the portable path."""
    nat, _, _ = stores(seed, False)
    pf = args.get("prop_filter")
    if pf is not None and (pf["value"] == 2**70):
        return                       # a literal outside int64 declines
    filled = validate_args("aggregate_events", dict(args))
    if not (ops_aggregate._prop_keys(filled) or filled.get("endpoint_filter")
            or filled.get("valid_at") is not None):
        return
    try:
        got = ops_aggregate._native(nat, filled)
    except Exception:  # noqa: BLE001 - group cap, as the portable path
        return
    assert got is not None, f"engine declined {args}"


def test_exact_sums_by_hand():
    nat = _native_adapter()
    duck = DuckDBAdapter(":memory:")
    big = 2**62
    rows = [({"a": big}, 1), ({"a": big}, 2), ({"a": 3}, 3),
            ({"b": 0.1}, 4), ({"b": 0.2}, 5), ({"b": 0.30000000000000004}, 6)]
    for a in (nat, duck):
        for tt, (bag, vt) in enumerate(rows, 1):
            _apply(a, [{"op": "assert_edge", "src": "x", "dst": f"y{tt}",
                        "rel_type": "T", "props": bag, "vt_s": vt,
                        "vt_e": vt + 10, "disc": ""}], tt)
    args = {"group_by": [], "window": {"t_a": 0, "t_b": 100},
            "aggregates": [{"agg": "sum", "of": "prop", "prop": "a"},
                           {"agg": "sum", "of": "prop", "prop": "b"},
                           {"agg": "sum", "of": "vt_s"},
                           {"agg": "sum", "of": "duration"}]}
    for a in (nat, duck):
        row = call_operator(a, "aggregate_events", dict(args))["rows"][0]
        assert row["sum_prop_a"] == 2 * big + 3          # past int64, exact
        assert isinstance(row["sum_prop_a"], int)
        # one rounding of the exact total, then the envelope's 9 decimals
        assert row["sum_prop_b"] == round(math.fsum([0.1, 0.2, 0.30000000000000004]), 9)
        assert row["sum_vt_s"] == 21
        assert row["sum_duration"] == 60


def test_sum_over_an_empty_group_is_null_and_count_is_zero():
    nat = _native_adapter()
    _apply(nat, [{"op": "assert_edge", "src": "x", "dst": "y", "rel_type": "T",
                  "props": {}, "vt_s": 5, "vt_e": 6, "disc": ""}], 1)
    row = call_operator(nat, "aggregate_events", {
        "group_by": [], "window": {"t_a": 50, "t_b": 60},
        "aggregates": [{"agg": "count"}, {"agg": "sum", "of": "vt_s"}]})["rows"][0]
    assert row == {"count": 0, "sum_vt_s": None}


# ---- engine-level only ----------------------------------------------------- #

SUM_CALL = {"group_by": [], "window": {"t_a": 0, "t_b": 10},
            "aggregates": [{"agg": "sum", "of": "vt_s"}]}


def test_sum_is_not_in_the_base_schema():
    spec = REGISTRY["aggregate_events"]
    base_enum = spec.args_schema["properties"]["aggregates"]["items"][
        "properties"]["agg"]["enum"]
    engine_enum = spec.engine_schema["properties"]["aggregates"]["items"][
        "properties"]["agg"]["enum"]
    assert "sum" not in base_enum
    assert engine_enum == [*base_enum[:5], "sum", *base_enum[5:]]


def test_valid_at_is_not_in_the_base_schema():
    spec = REGISTRY["aggregate_events"]
    assert "valid_at" not in spec.args_schema["properties"]
    assert "default" not in spec.engine_schema["properties"]["valid_at"]
    with pytest.raises(SchemaError):
        validate_args("aggregate_events", {**SUM_CALL, "aggregates": [
            {"agg": "count"}], "valid_at": 3}, base_schema=True)


def test_base_schema_callers_are_refused_sum_and_engine_callers_are_not():
    with pytest.raises(SchemaError):
        validate_args("aggregate_events", dict(SUM_CALL), base_schema=True)
    assert validate_args("aggregate_events", dict(SUM_CALL))["aggregates"][0][
        "agg"] == "sum"

