"""`Order` and `Limit`.

`Order` is one of the four node kinds that ever sort. `Limit` sorts
nothing: it is a cut at the plan's output boundary, and its two uses produce
different *metadata* rather than different rows.
"""

from __future__ import annotations

import numpy as np

from gnomon.plan.eval.expr_eval import eval_expr
from gnomon.plan.node import Limit, Order
from gnomon.plan.relation import Relation


def eval_order(node: Order, rel: Relation) -> Relation:
    """`Order`, including the part that is easy to skip: **a total order**.

    "The declared keys are extended with the input's own canonical order as a
    final tiebreak, so the output order is total and the canonical result hash
    is well defined." A stable sort over the declared keys does exactly that —
    ties keep their input positions, which *is* the input's canonical order —
    so the tiebreak needs no extra key and no row-position column.

    Strings compare **by code point** (`COLLATE "C"`), which is what numpy's
    ordering on an object array of `str` gives.

    `nulls_first` / `nulls_last` are honoured per key; a null sorts to the
    declared end regardless of what its storage slot happens to hold, since
    values under a null are unspecified.
    """
    if rel.n == 0:
        return rel
    order = np.arange(rel.n)
    # apply keys in reverse: the last key is the least significant, and each
    # pass is stable, so the first key ends up dominant with the input's own
    # order surviving underneath as the final tiebreak
    for key in reversed(node.keys):
        values, nulls = eval_expr(key.key, rel)
        ranks = _rank(values[order])
        if key.direction == "desc":
            ranks = -ranks
        if nulls is not None:
            # **After** the direction, never before: a null's placement is
            # declared in output terms ("first" / "last"), so negating a
            # null rank for `desc` would silently swap the two.
            null_here = nulls[order]
            span = ranks.max() - ranks.min() + 1 if ranks.size else 1
            ranks = np.where(null_here,
                             ranks.min() - span if key.nulls == "nulls_first"
                             else ranks.max() + span, ranks)
        order = order[np.argsort(ranks, kind="stable")]
    return rel.take(order)


def _rank(values: np.ndarray) -> np.ndarray:
    """Sortable integer ranks for a column of any storage type.

    Ranking rather than sorting the values directly is what lets one code path
    handle int64, float and object-array strings, and what makes `desc` a
    negation rather than a reversed sort — a reversal would flip the *input*
    order underneath equal keys and lose the IR's tiebreak.
    """
    if values.dtype.kind in "iuf":
        order = np.argsort(values, kind="stable")
    else:
        # A slot holding `None` gets a placeholder of its column's own class.
        # Its rank is replaced by the null placement whenever the slot is
        # null, so the placeholder only has to be comparable: "" beside
        # strings (as always), -inf beside numbers — a nullable numeric
        # column (an aggregate over a group with no values, a left-outer
        # side) used to raise `TypeError` here, comparing a float with "".
        numeric = any(isinstance(v, (int, float)) and not isinstance(v, bool)
                      for v in values)
        filler: object = float("-inf") if numeric else ""
        order = np.argsort(np.array([filler if v is None else v for v in values],
                                    dtype=object), kind="stable")
    ranks = np.empty(len(values), dtype=np.int64)
    ranks[order] = np.arange(len(values))
    # equal values must share a rank, or `desc` would reverse their input order
    sorted_values = values[order]
    group = np.zeros(len(values), dtype=np.int64)
    if len(values):
        same = np.array([sorted_values[i] == sorted_values[i - 1]
                         for i in range(1, len(values))], dtype=bool)
        group[order] = np.concatenate([[0], np.cumsum(~same)])
    return group


def _key(value: object) -> object:
    return "" if value is None else value


def eval_limit(node: Limit, rel: Relation) -> Relation:
    """Two uses, one implementation:

    - directly above an `Order` it is **top-k**: the declared domain narrows to
      "the `n` greatest rows under the recorded ranking key";
    - otherwise it is a **page cut**: delivery is incomplete, execution is not.

    The rows are the same either way — `is_top_k` is a syntactic property of the
    plan, and what differs is the metadata and therefore what a caller
    may certify. The offset stays plaintext decimal, which is the fixed cursor
    semantics.
    """
    start = node.offset or 0
    if node.cursor is not None:
        start = int(node.cursor)
    return rel.take(np.arange(start, min(start + node.n, rel.n)))


def limit_truncated(node: Limit, rel: Relation) -> bool:
    """Whether a page cut left rows behind — `rows_total` is counted before the
    cut, exactly as `algebra.paginate` does it."""
    start = (node.offset or 0) if node.cursor is None else int(node.cursor)
    return start + node.n < rel.n


__all__ = ["eval_limit", "eval_order", "limit_truncated"]
