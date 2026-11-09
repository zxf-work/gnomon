"""Rollout control for the PLAN_IR path.

Two switches live here, and the difference between them is the point:

  direct call: `call_operator` invokes `REGISTRY[op].fn(adapter, args)` itself
  instead of building and evaluating a single-leaf plan. It is a *structural*
  rollback, not a semantic one — both paths call the same kernel with the same
  arguments and produce the same payload and the same `result_digest` — so
  flipping it can only ever remove the wrapping, never change an answer. The
  plan removes this flag once the phase has been green for one full CI cycle.

- **`COMPILE_MODE`** — which operators evaluate as a
  *compiled core expansion* rather than as an opaque leaf. That one **is**
  rather than an environment variable: an env var is global, unversioned and
  unreviewable, and would let a stray shell export change what a caller gets
  `shadow`, never toward `compiled`.

The asymmetry is deliberate. `GNOMON_PLAN_PATH` is allowed to be an env var
precisely because it cannot change an answer.
"""

from __future__ import annotations

import os
from typing import Literal

#: The environment variable that disables the plan path.
PLAN_PATH_ENV = "GNOMON_PLAN_PATH"

#: Values that turn the wrapping off. Anything else — unset included — leaves
#: it on, so the escape hatch has to be taken deliberately.
_OFF = frozenset({"off", "0", "false", "no"})


def plan_path_enabled() -> bool:
    """True unless `GNOMON_PLAN_PATH` names one of the off values.

    Read per call rather than at import: a rollback should not need a process
    restart, and the cost is one `os.environ` lookup against an operator call
    that touches a storage backend.
    """
    return os.environ.get(PLAN_PATH_ENV, "on").strip().lower() not in _OFF


Mode = Literal["leaf", "shadow", "compiled"]

#: The per-operator rollout table. **Every operator is `leaf`**, which is
#: the core is expressive enough, not to ship it.
#:
#: `entity_history` and `version_history` were compiled. `snapshot_subgraph`,
#: `diff_snapshots` and the `aggregate_events` fragment **stay `leaf`
#: permanently** — the decision, and its reason:
#:
#: - They deliver **no forecast row**. The IR's five COMPILE rows were about proving
#:   `version_history`, `bfs_node_set`, `compute join` and bo33's nine-edge
#:   motif on real data, so a further three compilations would re-prove a
#:   settled point at an estimated 7–9 days.
#: - the IR's **safety valve** blesses the outcome in as many words: falling
#:   back to the leaf is "a *recorded deviation with a D-entry*, never a silent
#:   substitution". This comment plus the D-entry is that record.
#: - Each carries a specific cost the valve exists for: `diff_snapshots`
#:   compiled is **carve-reachable while its leaf is not**, so compiling
#:   it would *lose freshness precision* to gain nothing; `snapshot_subgraph`
#:   needs a per-hop `Join{inner}` against `NodeScan @ instant($t)` plus a
#:   two-root reassembly of two independently-capped lists with one merged
#:   truncation signal; `aggregate_events` has a five-item opaque residue
#:   whose default-deny allowlist is a semantics switch where a mis-route costs
#:   a wrong answer.
#:
#: Promoting an entry to `shadow` or `compiled` requires an equivalence receipt
#: **plus** a D-entry **plus** a non-gating scope-diff receipt.
COMPILE_MODE: dict[str, Mode] = {
    "entity_history": "leaf",
    "version_history": "leaf",
}


def compile_mode(op: str) -> Mode:
    return COMPILE_MODE.get(op, "leaf")


__all__ = ["COMPILE_MODE", "Mode", "PLAN_PATH_ENV", "compile_mode", "plan_path_enabled"]
