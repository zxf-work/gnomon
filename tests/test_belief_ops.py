"""
* `diff_belief` (engine-level, registered beside `match`): every version
  whose belief state changed between two transaction times inside a
  valid-time window. The native engine answers through `beliefdiff.rs`; the
  other backends by the definition over their version columns.
* `version_history(ref, include_props)` (engine-level arguments): the belief
  log of one node or edge, every version ever written, with both clocks and
  optionally the props. Versions closed by a bare retraction are in it,
  could miss.

Both are checked against the oracle on random bi-temporal stores, on the
native engine and on DuckDB, and both stay off every base-schema surface.
"""

from __future__ import annotations

import os
import random
import tempfile
from typing import Any

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from gnomon.core.errors import InvalidArgError, SchemaError
from gnomon.core.model import OPEN_END, canonical_json, edge_eid
from gnomon.storage.duckdb_adapter import DuckDBAdapter
from gnomon.temporal.algebra import (
    BASE_OPERATORS,
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

T_MAX = 50
UIDS = [f"u{i}" for i in range(6)]
RELS = ["R", "S"]


def _native():
    from gnomon.storage.native import NativeAdapter

    tmp = tempfile.TemporaryDirectory(prefix="gnomon-test-belief-")
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


def _ops(seed: int) -> list[dict[str, Any]]:
    """Asserts, corrections and bare retractions over few identities, so
    each identity's log is long and every belief shape occurs."""
    rng = random.Random(seed)
    out = []
    for _ in range(45):
        k = rng.choice(["an", "an", "ae", "ae", "ae", "cn", "ce", "rn", "re"])
        u, v = rng.choice(UIDS), rng.choice(UIDS)
        rel, disc = rng.choice(RELS), rng.choice(["", "1"])
        s = rng.randrange(0, T_MAX - 1)
        e = s + rng.randrange(1, T_MAX - s)
        p = {"w": rng.randrange(5)}
        ref_e = {"kind": "edge", "src": u, "dst": v, "rel_type": rel, "disc": disc}
        out.append({
            "an": {"op": "assert_node", "uid": u, "label": rng.choice(["A", "B"]),
                   "props": p, "vt_s": s, "vt_e": e},
            "ae": {"op": "assert_edge", "src": u, "dst": v, "rel_type": rel,
                   "props": p, "vt_s": s, "vt_e": e, "disc": disc},
            "cn": {"op": "correct", "ref": {"kind": "node", "uid": u}, "props": p,
                   "vt_s": s, "vt_e": e},
            "ce": {"op": "correct", "ref": ref_e, "props": p, "vt_s": s, "vt_e": e},
            "rn": {"op": "retract", "ref": {"kind": "node", "uid": u},
                   "t": rng.randrange(0, T_MAX)},
            "re": {"op": "retract", "ref": ref_e, "t": rng.randrange(0, T_MAX)},
        }[k])
    return out


_stores: dict[int, Any] = {}


def stores(seed: int):
    if seed not in _stores:
        nat, duck = _native(), DuckDBAdapter(":memory:")
        for tt, op in enumerate(_ops(seed), 1):
            for a in (nat, duck):
                try:
                    _apply(a, [op], tt)
                except Exception:  # noqa: BLE001 - an op the store refuses
                    pass
        oracle = Oracle(list(nat.all_node_versions()), list(nat.all_edge_versions()))
        _stores[seed] = (nat, duck, oracle)
    return _stores[seed]


def _payload(adapter, op, args) -> str:
    env = call_operator(adapter, op, dict(args))
    return canonical_json({k: v for k, v in env.items() if k not in ENVELOPE_META_KEYS})


def _expected(oracle, op, args) -> str:
    filled = validate_args(op, dict(args))
    return canonical_json(_canonicalize_floats(getattr(oracle, op)(filled)))


tts = st.one_of(st.integers(0, 47), st.just(OPEN_END))


@st.composite
def refs(draw) -> tuple[str, dict[str, Any]]:
    if draw(st.booleans()):
        return "node", {"uid": draw(st.sampled_from(UIDS + ["nobody"]))}
    src, dst = draw(st.sampled_from(UIDS)), draw(st.sampled_from(UIDS))
    rel, disc = draw(st.sampled_from(RELS)), draw(st.sampled_from(["", "1"]))
    if draw(st.booleans()):
        return "edge", {"eid": edge_eid(src, dst, rel, disc)}
    ref = {"src": src, "dst": dst, "rel_type": rel}
    if disc or draw(st.booleans()):
        ref["disc"] = disc
    return "edge", ref


@SETTINGS
@given(seed=st.integers(0, 3), r=refs(), as_of=tts,
       belief=st.sampled_from(["current", "superseded", "all"]),
       ta=st.integers(0, T_MAX), span=st.integers(1, T_MAX + 5),
       props=st.booleans(), limit=st.sampled_from([100, 2]),
       page=st.integers(0, 2), rel=st.sampled_from([None, ["R"], ["S"]]))
def test_version_history_of_one_identity_matches_the_oracle(seed, r, as_of, belief, ta,
                                                            span, props, limit, page, rel):
    kind, ref = r
    args: dict[str, Any] = {"kind": kind, "ref": ref, "belief": belief, "as_of_tt": as_of,
                            "window": {"t_a": ta, "t_b": ta + span}, "limit": limit,
                            "cursor": str(page * limit) if page else None}
    if props:
        args["include_props"] = True
    if kind == "edge" and rel is not None:
        args["rel_types"] = rel
    nat, duck, oracle = stores(seed)
    want = _expected(oracle, "version_history", args)
    assert _payload(nat, "version_history", args) == want, args
    assert _payload(duck, "version_history", args) == want, args


@SETTINGS
@given(seed=st.integers(0, 3), t1=st.integers(0, 47), d=st.integers(0, 50),
       ta=st.integers(0, T_MAX), span=st.integers(1, T_MAX + 5),
       limit=st.sampled_from([100, 3]), page=st.integers(0, 2), now=st.booleans())
def test_diff_belief_matches_the_oracle_on_every_backend(seed, t1, d, ta, span, limit,
                                                         page, now):
    args = {"t1": t1, "t2": OPEN_END if now else t1 + d,
            "window": {"t_a": ta, "t_b": ta + span}, "limit": limit,
            "cursor": str(page * limit) if page else None}
    nat, duck, oracle = stores(seed)
    want = _expected(oracle, "diff_belief", args)
    assert _payload(nat, "diff_belief", args) == want, args
    assert _payload(duck, "diff_belief", args) == want, args


def test_the_whole_log_includes_versions_ended_by_a_bare_retraction():
    """A composition of adapter calls walks believed states backwards and can
    skip a version closed by a retraction with an older open sibling; the
    log is read, so it cannot."""
    for a in (_native(), DuckDBAdapter(":memory:")):
        e = {"src": "x", "dst": "y", "rel_type": "R", "disc": ""}
        _apply(a, [{"op": "assert_edge", **e, "props": {}, "vt_s": 0, "vt_e": 10}], 1)
        _apply(a, [{"op": "assert_edge", **e, "props": {"k": 1}, "vt_s": 20, "vt_e": 30}], 2)
        _apply(a, [{"op": "retract", "ref": {"kind": "edge", **e}, "t": 25}], 3)
        got = call_operator(a, "version_history", {
            "kind": "edge", "ref": e, "belief": "all", "include_props": True,
            "window": {"t_a": 0, "t_b": OPEN_END}})
        assert [(r["vt_s"], r["vt_e"], r["tt_s"], r["props"]) for r in got["rows"]] == [
            (0, 10, 1, {}), (20, 30, 2, {"k": 1}), (20, 25, 3, {"k": 1})]
        assert got["rows"][1]["tt_e"] == 3


def test_ref_arguments_are_validated():
    base = {"window": {"t_a": 0, "t_b": 10}}
    with pytest.raises(InvalidArgError):
        validate_args("version_history", {**base, "kind": "node", "ref": {"eid": "ab"}})
    with pytest.raises(InvalidArgError):
        validate_args("version_history", {**base, "kind": "edge", "ref": {"uid": "u"}})
    with pytest.raises(InvalidArgError):
        validate_args("version_history", {**base, "kind": "node", "include_props": True})
    with pytest.raises(InvalidArgError):
        validate_args("diff_belief", {"t1": 5, "t2": 4, "window": {"t_a": 0, "t_b": 1}})


# ---- engine-level only ----------------------------------------------------- #


def test_ref_and_include_props_are_not_in_the_agent_schema():
    spec = REGISTRY["version_history"]
    for k in ("ref", "include_props"):
        assert k not in spec.args_schema["properties"]
        assert "default" not in spec.engine_schema["properties"][k]
    with pytest.raises(SchemaError):
        validate_args("version_history", {"kind": "node", "ref": {"uid": "u1"},
                                          "window": {"t_a": 0, "t_b": 10}}, agent=True)
