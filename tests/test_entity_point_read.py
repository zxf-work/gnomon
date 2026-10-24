"""The point-in-time entity read.

`entity_history(uid, t_valid=t)` (or `window={t_a, t_b}`), engine-level
arguments: the believed versions of one node whose valid interval contains
the instant (overlaps the window), as of `as_of_tt`, read by the engine's
windowed point read instead of by materialising the whole believed history.
The answer must be the full history filtered the same way, page for page,
with or without incident edges, at every belief time: checked against the
unfiltered call on the same store, against the oracle, on the native engine
and on DuckDB, on histories of both shapes the T-mgBench workload produces
(valid-time updates, and transaction-time-only rewrites of the whole line).
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
from gnomon.core.model import OPEN_END, canonical_json
from gnomon.storage.duckdb_adapter import DuckDBAdapter
from gnomon.temporal.algebra import (
    REGISTRY,
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
UIDS = ["hot", "warm", "cold"]
T_MAX = 400


def _native():
    from gnomon.storage.native import NativeAdapter

    tmp = tempfile.TemporaryDirectory(prefix="gnomon-test-pointread-")
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


def _history(seed: int, shape: str):
    """`now`: each update starts at its own valid time (the old version keeps
    the earlier part); `zero`: each write replaces the whole valid line, as a
    transaction-time-only workload does. Skewed: `hot` takes most writes.
    A few edge writes and retractions, and incremental compaction, so rows
    sit in several segments with folded closes."""
    rng = random.Random(seed)
    nat, duck = _native(), DuckDBAdapter(":memory:")
    for tt in range(1, 160):
        u = rng.choices(UIDS, weights=[8, 2, 1])[0]
        t = 0 if shape == "zero" else min(T_MAX - 2, tt * 2 + rng.randrange(3))
        r = rng.random()
        if r < 0.75:
            op = {"op": "assert_node", "uid": u, "label": "P", "props": {"p": tt},
                  "vt_s": t, "vt_e": OPEN_END}
        elif r < 0.9:
            op = {"op": "assert_edge", "src": u, "dst": rng.choice(UIDS), "rel_type": "K",
                  "props": {}, "vt_s": t, "vt_e": OPEN_END, "disc": str(tt % 3)}
        else:
            op = {"op": "retract", "ref": {"kind": "node", "uid": u},
                  "t": t + rng.randrange(1, 20)}
        for a in (nat, duck):
            try:
                _commit(a, [op], tt)
            except Exception:  # noqa: BLE001 - an op the store refuses
                pass
        if tt % 25 == 0 and nat.needs_compaction():
            nat.compact_incremental()
    oracle = Oracle(list(nat.all_node_versions()), list(nat.all_edge_versions()))
    return nat, duck, oracle


_stores: dict[tuple[int, str], Any] = {}


def stores(seed: int, shape: str):
    if (seed, shape) not in _stores:
        _stores[(seed, shape)] = _history(seed, shape)
    return _stores[(seed, shape)]


def _payload(adapter, args) -> dict[str, Any]:
    env = call_operator(adapter, "entity_history", dict(args))
    return {k: v for k, v in env.items() if k not in ENVELOPE_META_KEYS}


def _full_filtered(adapter, args, lo, hi) -> dict[str, Any]:
    """The unfiltered call, every page, filtered to [lo, hi) and re-paged."""
    from gnomon.temporal.algebra import paginate

    base = {k: v for k, v in args.items() if k not in ("t_valid", "window", "cursor")}
    base["limit"] = 10_000
    full = _payload(adapter, base)
    rows = [r for r in full["rows"] if r["vt_s"] < hi and lo < r["vt_e"]]
    out = paginate(rows, args.get("limit", 100), args.get("cursor"))
    if args.get("include_edges"):
        es = [e for e in full["edges"] if e["vt_s"] < hi and lo < e["vt_e"]]
        out["edges"] = es[: args.get("limit", 100)]
        out["edges_truncated"] = len(es) > args.get("limit", 100)
    return out


@SETTINGS
@given(seed=st.integers(0, 2), shape=st.sampled_from(["now", "zero"]),
       uid=st.sampled_from(UIDS), as_of=st.one_of(st.integers(0, 170), st.just(OPEN_END)),
       instant=st.booleans(), t=st.integers(0, T_MAX), span=st.integers(1, 120),
       limit=st.sampled_from([100, 1, 3]), page=st.integers(0, 2),
       edges=st.booleans())
def test_the_point_read_is_the_full_history_filtered(seed, shape, uid, as_of, instant, t,
                                                     span, limit, page, edges):
    nat, duck, oracle = stores(seed, shape)
    args: dict[str, Any] = {"uid": uid, "as_of_tt": as_of, "limit": limit,
                            "cursor": str(page * limit) if page else None,
                            "include_edges": edges}
    if instant:
        args["t_valid"] = t
        lo, hi = t, t + 1
    else:
        args["window"] = {"t_a": t, "t_b": t + span}
        lo, hi = t, t + span
    try:
        want_oracle = oracle.entity_history(validate_args("entity_history", dict(args)))
    except Exception:  # noqa: BLE001 - an identity this store never wrote
        return
    for a in (nat, duck):
        got = _payload(a, args)
        assert canonical_json(got) == canonical_json(_full_filtered(a, args, lo, hi)), args
        assert canonical_json(got) == canonical_json(want_oracle), args


def test_a_hot_key_at_a_past_belief_time_returns_its_one_version():
    """The transaction-time-only shape: every write replaces the whole line,
    so as of any belief time exactly one version is believed, however many
    were written."""
    nat = _native()
    for tt in range(1, 400):
        _commit(nat, [{"op": "assert_node", "uid": "u", "label": "P", "props": {"p": tt},
                       "vt_s": 0, "vt_e": OPEN_END}], tt)
        if tt % 50 == 0 and nat.needs_compaction():
            nat.compact_incremental()
    for as_of in (1, 57, 200, 398, OPEN_END):
        got = call_operator(nat, "entity_history",
                            {"uid": "u", "t_valid": 5, "as_of_tt": as_of, "limit": 1})
        assert got["rows_total"] == 1
        want = min(as_of, 399)
        assert got["rows"][0]["props"] == {"p": want}


def test_t_valid_and_window_are_exclusive_and_engine_level():
    with pytest.raises(InvalidArgError):
        validate_args("entity_history", {"uid": "u", "t_valid": 1,
                                         "window": {"t_a": 0, "t_b": 2}})
    spec = REGISTRY["entity_history"]
    for k in ("t_valid", "window"):
        assert k not in spec.args_schema["properties"]
        assert "default" not in spec.engine_schema["properties"][k]
    with pytest.raises(SchemaError):
        validate_args("entity_history", {"uid": "u", "t_valid": 1}, base_schema=True)
