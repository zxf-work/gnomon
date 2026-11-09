"""Building the `OpaqueLeaf` for one operator call.

The registered operators become **opaque algorithmic PLAN_IR operators** —
plan nodes that carry the full result-metadata tuple while delegating to the
exact kernel that runs today. This module is the constructor: it derives Σ from
the bound arguments, reads the output schema off `OperatorSpec`, and
carries the `∅` classification that decides whether the kernel may see a live
storage adapter.

**Σ is per call, never per operator**, which is the whole reason the leaf is a
plan node rather than a registry annotation: `as_of_tt`, `window`, `t_valid`,
`t1`/`t2` all move per call, and so does the output schema (`entity_history`'s
`edges` list exists only when `include_edges`).
"""

from __future__ import annotations

from typing import Any

from gnomon.core.model import OPEN_END, Interval
from gnomon.plan.node import OpaqueLeaf
from gnomon.plan.types import Sigma

#: The IR's table, transcribed. `T_v` is derived per operator from the bound
#: args; `vt_mode` is the keying mode the kernel reads its intervals under,
#: which is a *scan* parameter rather than part of Σ.
#:
#: `overlap`  — `x.vt_s < v_b ∧ v_a < x.vt_e`; the general window reading.
#: `instant`  — `x.vt_s ≤ t < x.vt_e`; all snapshot semantics.
#: `event`    — `v_a ≤ x.vt_s < v_b`; "an edge version *is* an event at `vt_s`".
LEAF_VT_MODE: dict[str, str] = {
    "entity_history": "overlap",
    "resolve_entities": "overlap",
    "co_active": "overlap",
    "version_history": "overlap",
    "aggregate_events": "event",
    "graph_metric_timeseries": "event",
    "burst_detection": "event",
    "count_temporal_motifs": "event",
    "find_temporal_motif_instances": "event",
    "temporal_reachability": "event",
    "temporal_paths": "event",
    "snapshot_subgraph": "instant",
    "diff_snapshots": "instant",
    "neighborhood_evolution": "instant",
    "compute": "overlap",
    # the table's entry; a call's own mode follows its scope (`vt_mode_for`):
    # an instant reads `instant`, a window `overlap` or `event`
    "match": "overlap",
    # engine-level: versions whose interval overlaps the window
    "diff_belief": "overlap",
    # engine-level: the snapshot at `t_valid`; a call's own mode follows
    # its arguments (`vt_mode_for`): a window of events when it reads one
    "paths": "instant",
    # engine-level: the state at each bucket start (`time_weighted` reads
    # the buckets as windows, `vt_mode_for`)
    "state_series": "instant",
}

#: Operators whose `T_v` is the whole valid-time extent because they take no
_UNWINDOWED = frozenset({"entity_history", "resolve_entities", "co_active"})

#: The default scope: `T_v = [0, OPEN_END)`, matching every operator's own
#: default and the fourteen store readers' `as_of_tt` default.
FULL_EXTENT = Interval(0, OPEN_END)


def sigma_for(op: str, args: dict[str, Any]) -> Sigma:
    """Σ = `(T_v, T_b)` for one bound call.

    `T_b = as_of_tt` for all fourteen store-reading operators; `compute` has
    none — its Σ is *inherited*, and a standalone call inherits the
    default.
    """
    t_b = args.get("as_of_tt")
    t_b = OPEN_END if t_b is None else int(t_b)

    if op == "entity_history" and args.get("t_valid") is not None:
        # engine-level point-in-time read
        return Sigma.at_instant(int(args["t_valid"]), as_of_tt=t_b)
    if op == "entity_history" and isinstance(args.get("window"), dict):
        return Sigma.in_window(int(args["window"]["t_a"]), int(args["window"]["t_b"]),
                               as_of_tt=t_b)
    if op in _UNWINDOWED or op == "compute":
        return Sigma((FULL_EXTENT,), t_b)
    if op == "match":
        # Σ sits in the argument's `scope`, with its parameters still unbound
        from gnomon.temporal.ops_match import scope_sigma

        return scope_sigma(args)[0]
    if op == "snapshot_subgraph":
        return Sigma.at_instant(int(args["t_valid"]), as_of_tt=t_b)
    if op == "diff_snapshots":
        # the one leaf whose Σ is a *pair* of instants — and, with
        # carve arm, because an instant Σ is what makes the `vt` narrowing worth
        # something to lose
        return Sigma.at_instants(int(args["t1"]), int(args["t2"]), as_of_tt=t_b)
    if op == "neighborhood_evolution":
        return Sigma.in_window(int(args["t1"]), int(args["t2"]) + 1, as_of_tt=t_b)
    if op == "paths" and args.get("t_valid") is not None and \
            args.get("time_respecting") is None and args.get("metric") != "arrival":
        return Sigma.at_instant(int(args["t_valid"]), as_of_tt=t_b)
    if op == "state_series" and args.get("axis") == "transaction":
        # one valid instant, read at every belief time of the window
        return Sigma.at_instant(int(args["t_valid"]), as_of_tt=t_b)
    if op == "diff_belief":
        # two belief times: the later one is the belief basis the diff reads
        w = args["window"]
        return Sigma.in_window(int(w["t_a"]), int(w["t_b"]), as_of_tt=int(args["t2"]))
    window = args.get("window")
    if isinstance(window, dict) and "t_a" in window and "t_b" in window:
        return Sigma.in_window(int(window["t_a"]), int(window["t_b"]), as_of_tt=t_b)
    # an operator whose window is optional and absent reads the whole extent
    return Sigma((FULL_EXTENT,), t_b)


def build_leaf(op: str, bound_args: dict[str, Any],
               out_fields: tuple[str, ...]) -> OpaqueLeaf:
    """The plan node for one call, from post-`validate_args` arguments.

    `withhold_adapter` is derived from the `∅` classification and cannot be
    talked out of it: constructing `compute` with the adapter *not* withheld is
    refused by `OpaqueLeaf` itself.
    """
    return OpaqueLeaf.build(op, bound_args, out_fields,
                            sigma=sigma_for(op, bound_args),
                            vt_mode=vt_mode_for(op, bound_args))


def vt_mode_for(op: str, args: dict[str, Any]) -> str:
    """The keying mode of one call: the operator's, except `match`, whose
    mode is its scope's."""
    if op == "match":
        from gnomon.temporal.ops_match import scope_sigma

        return scope_sigma(args)[1]
    if op == "entity_history" and args.get("t_valid") is not None:
        return "instant"
    if op == "paths" and (args.get("time_respecting") is not None
                          or args.get("metric") == "arrival"):
        return "event"
    if op == "state_series" and args.get("at") == "time_weighted":
        return "overlap"
    return LEAF_VT_MODE.get(op, "overlap")


__all__ = ["FULL_EXTENT", "LEAF_VT_MODE", "build_leaf", "sigma_for", "vt_mode_for"]
