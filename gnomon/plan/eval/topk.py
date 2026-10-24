"""`TopK`: per partition, the first n rows under the keys."""

from __future__ import annotations

import numpy as np

from gnomon.plan.eval.expr_eval import eval_expr
from gnomon.plan.eval.order import _rank
from gnomon.plan.node import TopK
from gnomon.plan.relation import Relation, _py


def eval_topk(node: TopK, rel: Relation) -> Relation:
    """The ranking is `Order`'s (stable passes over the keys, least
    significant first, nulls placed after the direction); each row is kept
    while its partition has fewer than `n` rows kept."""
    if rel.n == 0:
        return rel
    order = np.arange(rel.n)
    for key in reversed(node.keys):
        values, nulls = eval_expr(key.key, rel)
        ranks = _rank(values[order])
        if key.direction == "desc":
            ranks = -ranks
        if nulls is not None:
            null_here = nulls[order]
            span = ranks.max() - ranks.min() + 1 if ranks.size else 1
            ranks = np.where(null_here,
                             ranks.min() - span if key.nulls == "nulls_first"
                             else ranks.max() + span, ranks)
        order = order[np.argsort(ranks, kind="stable")]
    parts = [eval_expr(e, rel) for e in node.partition_by]
    taken: dict[tuple, int] = {}
    keep = []
    for i in order:
        key = tuple(None if (nulls is not None and nulls[i]) else _py(values[i])
                    for values, nulls in parts)
        k = taken.get(key, 0)
        if k < node.n:
            taken[key] = k + 1
            keep.append(i)
    return rel.take(np.array(keep, dtype=np.int64))


__all__ = ["eval_topk"]
