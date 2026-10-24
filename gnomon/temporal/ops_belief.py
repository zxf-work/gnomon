"""`diff_belief`: the belief diff as an operator (engine-level only).

Proposal D1 of the operator-coverage study. `diff_snapshots` takes one belief
time and two valid-time instants; this is its dual on the other clock: two
belief times `t1 <= t2` and one valid-time window `[t_a, t_b)`. A version is
tagged `"added"` when its `tt_s` falls in `(t1, t2]` and `"retracted"` when
its effective `tt_e` does, independently, so a version written and revised
inside the same belief window changed state twice and is reported twice
(`StorageAdapter.diff_belief` states the contract; `beliefdiff.rs` is the
native kernel; `Oracle.diff_belief` is the reference).

It is registered as its own operator rather than as `diff_snapshots(axis=
"belief")`, for one reason: its answer has a different shape (`rows` tagged
by change, `added_total`, `retracted_total`), and an operator's output fields
are part of its registered contract. Declaring them on `diff_snapshots` would
change the contract of a base operator; a separate engine-level operator
changes none of them.

Rows are ordered by `(kind, key, tt, change, vid)` and paginated with the
usual offset cursor; the totals count the whole population.

Every backend answers: the native engine through its kernel, the others by
the definition over their version columns, in the same order.
"""

from __future__ import annotations

from typing import Any

from gnomon.core.errors import InvalidArgError
from gnomon.core.model import OPEN_END, clamp_tt
from gnomon.storage.base import StorageAdapter
from gnomon.temporal.algebra import (
    CURSOR,
    LIMIT,
    TIMESTAMP,
    WINDOW,
    check_window,
    operator,
    required,
)
from gnomon.temporal.guardrails import started_by


def _diff_belief_cost(args: dict[str, Any], stats: dict[str, Any]) -> dict[str, int]:
    """Every node version and the edge versions that may overlap the window:
    those started by its end. `scan_estimate` took the window's share
    of the extent, which a long-lived version overlaps whatever the window:
    on FinBench and SNB SF1 a window over the last tenth ran 3.3 to 3.9 times
    the estimate. Over an empty interval (t1 = t2 after the clamp) no version
    can change and neither route reads one."""
    if clamp_tt(args["t2"]) <= clamp_tt(args["t1"]):
        return {"rows_scanned_est": 0, "expansions_est": 0, "time_est_ms": 1}
    rows = int(int(stats.get("n_edge_versions", 0)) * started_by(args["window"]["t_b"], stats)) + \
        int(stats.get("n_node_versions", 0))
    return {"rows_scanned_est": rows, "expansions_est": 0}


def _validators(args: dict[str, Any]) -> None:
    check_window(args)
    if args["t1"] > args["t2"]:
        raise InvalidArgError(
            f"diff_belief requires t1 <= t2, got t1={args['t1']} t2={args['t2']}")


def _offset(cursor: str | None) -> int:
    try:
        return int(cursor) if cursor else 0
    except ValueError:
        raise InvalidArgError(f"bad cursor: {cursor!r}") from None


def _native_kernel(adapter: StorageAdapter) -> bool:
    """The adapter overrides the base's `unsupported` diff_belief."""
    return type(adapter).diff_belief is not StorageAdapter.diff_belief


def _by_definition(adapter: StorageAdapter, t1: int, t2: int, t_a: int,
                   t_b: int) -> list[dict[str, Any]]:
    """Every change in `(t1, t2]` inside `[t_a, t_b)`, over the backend's
    version columns: one predicate per version, as the oracle states it."""
    rows: list[dict[str, Any]] = []
    if not (t1 < t2 and t_a < t_b):
        return rows
    for kind, key in (("node", "uid"), ("edge", "eid")):
        c = adapter.versions_columnar(kind)
        for i in range(len(c["vid"])):
            vs, ve = int(c["vt_s"][i]), int(c["vt_e"][i])
            if not (vs < t_b and ve > t_a):
                continue
            ts, te = int(c["tt_s"][i]), int(c["tt_e"][i])
            for tt, change in ((ts, "added"), (te, "retracted")):
                if change == "retracted" and te == OPEN_END:
                    continue
                if t1 < tt <= t2:
                    rows.append({"kind": kind, "key": c[key][i], "vid": c["vid"][i],
                                 "tt": tt, "change": change, "vt_s": vs, "vt_e": ve})
    rows.sort(key=lambda r: (0 if r["kind"] == "node" else 1, r["key"], r["tt"],
                             r["change"], r["vid"]))
    return rows


@operator(
    "diff_belief",
    {
        "t1": required(TIMESTAMP),
        "t2": required(TIMESTAMP),
        "window": required(WINDOW),
        "limit": LIMIT,
        "cursor": CURSOR,
    },
    "Belief diff: every node and edge version whose belief state changed "
    "between transaction times t1 and t2 (t1 <= t2; OPEN_END = now), inside "
    "the valid-time window. change = 'added' when tt_s is in (t1, t2], "
    "'retracted' when the effective tt_e is; a version written and revised "
    "inside the window is reported twice. Rows {kind, key, vid, tt, change, "
    "vt_s, vt_e} ordered by (kind, key, tt, change, vid); added_total and "
    "retracted_total count the whole population. Engine-level only.",
    cost_fn=_diff_belief_cost,
    validators=[_validators],
    output_fields=("rows", "rows_total", "added_total", "retracted_total",
                   "truncated", "cursor"),
)
def diff_belief(adapter: StorageAdapter, args: dict[str, Any]) -> dict[str, Any]:
    t1, t2 = clamp_tt(args["t1"]), clamp_tt(args["t2"])
    t_a, t_b = args["window"]["t_a"], args["window"]["t_b"]
    offset, limit = _offset(args["cursor"]), args["limit"]
    if _native_kernel(adapter):
        # the kernel orders and caps; a later page asks for the rows up to
        # its end and keeps its own slice of them
        got = adapter.diff_belief(t1, t2, t_a, t_b, offset + limit)
        rows = got["rows"][offset:offset + limit]
        added, retracted = int(got["added_total"]), int(got["retracted_total"])
    else:
        everything = _by_definition(adapter, t1, t2, t_a, t_b)
        rows = everything[offset:offset + limit]
        added = sum(1 for r in everything if r["change"] == "added")
        retracted = len(everything) - added
    total = added + retracted
    truncated = offset + len(rows) < total
    return {"rows": rows, "rows_total": total, "added_total": added,
            "retracted_total": retracted, "truncated": truncated,
            "cursor": str(offset + len(rows)) if truncated else None}
