"""`tt_q` — the belief-time frontier a read was actually served from
, and the read basis the
`DependencyScope` on every envelope is stamped with.

`tt_q` relates to the execution basis but **is not** it: `as_of_tt` is what was
*asked for*, `tt_q` is what was *served*. Two rules govern every line here:

  frontier. A `tt_q` above what the read actually saw makes `check` skip the
  suffix that would have invalidated the answer, which is false freshness — the
  one class of error this contract exists to forbid. Everything below that
  cannot be established exactly is therefore rounded *down*, or marked.
  the frontier is not a pin: it is clamped, and reports `pinned = false,
  clamped = true`. The IR's own second non-claim says the same thing from the
  stability side — writes landing between the frontier and an above-frontier
  `T_b` legitimately change the result.

The frontier itself comes from the adapter's **applied** frontier
(`StorageAdapter.frontier_tt`), never from `Store.clock.last_tt`: the log is
fsynced *before* the batch is applied, so the clock over-reports what a reader
"""

from __future__ import annotations

import os
from dataclasses import dataclass, replace
from typing import Any, Callable

from gnomon.core.model import OPEN_END
from gnomon.plan.depscope import (
    FULL_SCAN_CHECKPOINTS, TOP_TERM, UNANCHORED, Checkpoint, DependencyScope,
)
from gnomon.plan.node import EMPTY_SCOPE_OPS
from gnomon.plan.scope_of import ScopeBasis


@dataclass(frozen=True, slots=True)
class Frontier:
    """What the store can say about its own applied transaction-time frontier.

    `tt is None` means *unavailable* — the fourth row of the IR's clamp table.
    `verified` is false when the value could not be established against the
    applied prefix: a **legacy cursorless store**, where the backend keeps no
    event cursor and the log's tail is therefore an upper bound on what was
    applied rather than a statement about it.
    """

    tt: int | None
    verified: bool = True


@dataclass(frozen=True, slots=True)
class TtQ:
    """The `(tt_q, pinned, clamped)` triple, plus the verification flag."""

    tt_q: int
    pinned: bool = False
    clamped: bool = False
    verified: bool = True
    #: handed, or `None` when this read earns no exemption. Not part of
    #: `to_envelope()`: it does not ride flat on the envelope, it rides on the
    #: `DependencyScope`.
    as_of_tt: int | None = None

    def to_envelope(self) -> dict[str, Any]:
        """The flat envelope keys. `verified` is **not**
        flat — a fifth flat key would slip past every comparator's exclusion
        tuple; it rides inside the `DependencyScope` instead."""
        return {"tt_q": self.tt_q, "pinned": self.pinned, "clamped": self.clamped}


def clamp(as_of_tt: int, frontier: Frontier) -> TtQ:
    """
    |---|---|---|---|---|
    | `OPEN_END` (the default) | frontier | false | false | *absent* |
    | `<= frontier` | `as_of_tt` | **true** | false | `as_of_tt` (= `tt_q`) |
    | `> frontier`, `< OPEN_END` | frontier | **false** | **true** | `as_of_tt` |
    | frontier unavailable | `0` | false | **true** | *absent* |

    The third row is the one an implementer gets wrong on `pinned`: an
    above-frontier pin is `pinned = false`, because `pinned` describes the
    basis *requested* and that basis was not served. It is nonetheless the
    exemption — `α = as_of_tt > tt_q` there, unlike row two where the field
    would be redundant with `tt_q`.

    **Row four is this function's one genuinely ambiguous corner, resolved
    fail-safe.** `frontier.tt is None` means the frontier could not be
    established at all — not merely that it is small — so there is no basis
    on which to trust *any* requested `as_of_tt` as the predicate the read
    actually applied; T1's per-batch proof needs exactly that trust. Emitting
    the caller's requested value there would be sound only if the read truly
    served it, which this function cannot know, so this entry treats the row
    spell out this row; the memo's derivation covers only rows two and three
    explicitly. This is the chosen fail-safe reading, flagged here rather
    than assumed.)
    """
    if frontier.tt is None:
        return TtQ(0, False, True, frontier.verified, None)
    if as_of_tt >= OPEN_END:
        return TtQ(frontier.tt, False, False, frontier.verified, None)
    if as_of_tt <= frontier.tt:
        return TtQ(as_of_tt, True, False, frontier.verified, as_of_tt)
    return TtQ(frontier.tt, False, True, frontier.verified, as_of_tt)


def frontier_of(adapter: Any, tt_source: Any = None) -> Frontier:
    """The frontier for this read.

    `tt_source` is the authoritative override — a `Store`, or any object with
    `frontier_tt()` (and optionally `frontier_verified`), or a plain callable
    returning an int. With none, the adapter's own applied frontier is used,
    which every backend maintains through the shared `apply_ops`.
    """
    if tt_source is not None:
        verified = bool(getattr(tt_source, "frontier_verified", True))
        getter = (tt_source if callable(tt_source)
                  else getattr(tt_source, "frontier_tt", None))
        if getter is not None:
            tt = getter()
            return Frontier(None if tt is None else int(tt), verified)
    getter = getattr(adapter, "frontier_tt", None)
    if getter is None:
        # An adapter predating the ABC method. Unavailable, not zero-by-luck.
        return Frontier(None, True)
    tt = getter()
    return Frontier(None if tt is None else int(tt), True)


def checkpoints_of(adapter: Any, tt_source: Any = None) -> tuple[Checkpoint, ...]:
    """
    `event_cursor()` exists on `NativeAdapter` only; it is not on the ABC, and
    neither DuckDB nor Kuzu has one. Their scopes therefore
    carry `[[0, SEED_CHAIN]]` — a full-log scan, "widening and therefore sound.
    Slow, never wrong." The same fallback covers a legacy store, whose cursor
    reports chain `""` and means *no cursor*, never *nothing applied*.
    """
    for holder in (tt_source, adapter):
        cursor = getattr(holder, "event_cursor", None) if holder is not None else None
        if cursor is None:
            continue
        try:
            offset, chain = cursor()
        except Exception:  # pragma: no cover - a backend that cannot answer
            continue
        if chain:
            return (Checkpoint(int(offset), chain),)
    return FULL_SCAN_CHECKPOINTS


def store_identity_of(adapter: Any, tt_source: Any = None) -> str:
    """    oracle-family stores (`DuckDBAdapter(":memory:")`) have no event log at
    all, and no identity can be invented for them."""
    for holder in (tt_source, adapter):
        ident = getattr(holder, "store_identity", None) if holder is not None else None
        if isinstance(ident, str) and ident:
            return ident
    return UNANCHORED


def basis_of(adapter: Any, as_of_tt: int, tt_source: Any = None) -> ScopeBasis:
    """Assemble the read basis: store identity, the clamped `tt_q` triple and
    the log cursor. Capture happens **before** the read is issued, which is how"""
    triple = clamp(as_of_tt, frontier_of(adapter, tt_source))
    return ScopeBasis(
        store=store_identity_of(adapter, tt_source),
        tt_q=triple.tt_q, pinned=triple.pinned, clamped=triple.clamped,
        checkpoints=checkpoints_of(adapter, tt_source),
        tt_q_verified=triple.verified,
    )


def dependency_of(op: str, basis: ScopeBasis, args: dict[str, Any] | None = None,
                  sigma: Any = None) -> DependencyScope:
    """The scope for one call: `leaves.terms_for` where a derivation exists,
    the coarse all-`"*"` term where it does not.

    `"*"` stays explicitly legal for any operator whose derivation is not yet
    strict improvement rather than a compatibility event.

    `compute` is the exception **from day one**: `terms: []`, the empty scope
    ∅, "the correct, non-degenerate value for a `compute` node over literal
    inputs".
    """
    from gnomon.plan.leaf import sigma_for
    from gnomon.plan.leaves import terms_for

    if op in EMPTY_SCOPE_OPS:
        return basis.empty_scope()
    if args is None:
        return basis.scope(TOP_TERM)
    return basis.scope(*terms_for(op, args, sigma or sigma_for(op, args)))


#: `envelope_metadata` by everything it reads -- the
#: operator, the caller's argument key, the frontier, the log checkpoints and
#: the store identity. The last three are read on every call (cheap getters)
#: and are the key, so a write, a correction or anything else that moves
#: what the uncached path would report gives a new key; nothing is keyed on
#: a generation counter. A full memo is simply cleared.
_ENVELOPE_MEMO: dict[Any, dict[str, Any]] = {}
_ENVELOPE_MEMO_ENTRIES = 4096


def _call_memo() -> bool:
    """`GNOMON_CALL_MEMO=0` computes the envelope metadata on every call."""
    return os.environ.get("GNOMON_CALL_MEMO", "1") != "0"


def _json_copy(obj: Any) -> Any:
    from gnomon.temporal.algebra import json_copy  # local: the cycle

    return json_copy(obj)


def _json_copy_py(obj: Any) -> Any:
    t = type(obj)
    if t is dict:
        return {k: _json_copy(v) for k, v in obj.items()}
    if t is list:
        return [_json_copy(v) for v in obj]
    if t is tuple:
        return tuple(_json_copy(v) for v in obj)
    return obj


def _envelope_from(op: str, args: dict[str, Any] | None, frontier: Frontier,
                   checkpoints: Any, store: str) -> dict[str, Any]:
    """`envelope_metadata`'s value from one captured frontier, checkpoint
    tuple and store identity: `basis_of`, `clamp` and `dependency_of` as the
    uncached path runs them."""
    triple = clamp(as_of_tt_of(args or {}, op), frontier)
    basis = ScopeBasis(store=store, tt_q=triple.tt_q, pinned=triple.pinned,
                       clamped=triple.clamped, checkpoints=checkpoints,
                       tt_q_verified=triple.verified)
    scope = dependency_of(op, basis, args)
    if triple.as_of_tt is not None:
        scope = replace(scope, as_of_tt=triple.as_of_tt)
    return {**triple.to_envelope(), "dependency": scope.to_json()}


def envelope_metadata(adapter: Any, op: str, args: dict[str, Any] | None = None,
                      tt_source: Any = None, args_key: Any = None) -> dict[str, Any]:
    """The four flat envelope keys plus the dependency object, for one call.

    Every one of them lands on the **envelope**, never in the kernel's
    `payload`, so digest exclusion is structural rather than a denylist
.

    `args` are the *filled* arguments the leaf was built from; with none — a
    step that never ran, and so has none — the scope falls back to `"*"`, which
    is the widening a failed step's mandatory contribution should be.

    built.** `ScopeBasis` (`scope_of.py`) is not this entry's file to widen, so
    the emission value is derived a second time, from the same `clamp` this
    function's own `basis_of` call already ran — `clamp` is pure, so the two
    calls agree by construction — and applied with `dataclasses.replace`
    after the scope object exists. Absent (the unpinned and frontier-
    unavailable rows of `clamp`'s table) leaves the scope exactly as `check`
    saw it before this entry.

    `args_key`: the caller's argument key (`args_key` of the
    arguments `args` were filled from). With it the value comes from
    `_ENVELOPE_MEMO`, keyed by the frontier, checkpoints and store identity
    read for this call; the caller gets a fresh copy.
    """
    if args_key is not None and _call_memo():
        frontier = frontier_of(adapter, tt_source)
        checkpoints = checkpoints_of(adapter, tt_source)
        store = store_identity_of(adapter, tt_source)
        mkey = (op, args_key, frontier, checkpoints, store)
        hit = _ENVELOPE_MEMO.get(mkey)
        if hit is None:
            hit = _envelope_from(op, args, frontier, checkpoints, store)
            if len(_ENVELOPE_MEMO) >= _ENVELOPE_MEMO_ENTRIES:
                _ENVELOPE_MEMO.clear()
            _ENVELOPE_MEMO[mkey] = hit
        return _json_copy(hit)
    basis = basis_of(adapter, as_of_tt_of(args or {}, op), tt_source)
    triple = clamp(as_of_tt_of(args or {}, op), frontier_of(adapter, tt_source))
    scope = dependency_of(op, basis, args)
    if triple.as_of_tt is not None:
        scope = replace(scope, as_of_tt=triple.as_of_tt)
    return {**triple.to_envelope(), "dependency": scope.to_json()}


def as_of_tt_of(args: dict[str, Any], op: str | None = None) -> int:
    """The requested belief basis of a filled argument set. Fourteen of the
    fifteen base-schema operators take `as_of_tt`; `compute` takes none and
    is unpinned. `match` carries it in `scope`, after parameter binding."""
    if op == "match":
        from gnomon.temporal.ops_match import bound_scope

        value = bound_scope(args).get("as_of_tt", OPEN_END)
        return OPEN_END if value is None else int(value)
    if op == "diff_belief":
        # its later belief time is the basis it reads (engine-level)
        return int(args.get("t2", OPEN_END))
    value = args.get("as_of_tt", OPEN_END)
    return OPEN_END if value is None else int(value)


#: A `tt_source` is anything `frontier_of` accepts; named for the signatures
#: that pass one through.
TtSource = Any
FrontierFn = Callable[[], int]

__all__ = [
    "Frontier", "FrontierFn", "TtQ", "TtSource", "as_of_tt_of", "basis_of",
    "checkpoints_of", "clamp", "dependency_of", "envelope_metadata",
    "frontier_of", "store_identity_of",
]
