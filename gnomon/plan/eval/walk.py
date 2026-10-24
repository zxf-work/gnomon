"""`Walk`: a regular path step, as a breadth-first search over the
product of the graph under Σ and the expression's automaton."""

from __future__ import annotations

import json
from typing import Any

import numpy as np

from gnomon.plan.eval.adjacency import AdjacencyCache
from gnomon.plan.eval.expand import _charge, _dense_map, _with_into
from gnomon.plan.node import Walk
from gnomon.plan.regex import nfa, symbols
from gnomon.plan.relation import Relation


def eval_walk(node: Walk, rel: Relation, adapter: Any, cache: AdjacencyCache,
              live: frozenset[str] | None = None, budget: Any = None,
              scans: Any = None) -> Relation:
    """Per input row, the targets reached in the accepting state, ascending
    by dense id; one search per distinct seed."""
    spec = json.loads(node.regex)
    a = nfa(spec)
    closure = [frozenset(a.closure({q})) for q in range(a.n)]
    adj = {key: cache.get(key[0], node.sigma) for key in symbols(spec)}
    from_uids = rel.column(node.from_column)
    dense = _dense_map(adapter, from_uids)
    memo: dict[int, list[int]] = {}
    rows: list[int] = []
    targets: list[int] = []
    for i in range(rel.n):
        seed = dense.get(from_uids[i])
        if seed is None:
            continue
        seed = int(seed)
        if seed not in memo:
            seen = {(seed, q) for q in closure[a.start]}
            found = {seed} if a.accept in closure[a.start] else set()
            frontier = list(seen)
            while frontier:
                nxt = []
                for u, q in frontier:
                    for rel_type, inverse, q2 in a.moves[q]:
                        nbr, _row = adj[(rel_type, inverse)].step(u, "in" if inverse else "out")
                        _charge(budget, len(nbr))
                        for v in nbr.tolist():
                            for q3 in closure[q2]:
                                if (v, q3) not in seen:
                                    seen.add((v, q3))
                                    nxt.append((v, q3))
                                    if q3 == a.accept:
                                        found.add(v)
                frontier = nxt
            memo[seed] = sorted(found)
        for v in memo[seed]:
            rows.append(i)
            targets.append(v)
    out = rel.take(np.asarray(rows, dtype=np.int64))
    return _with_into(out, node, targets, adapter, live, scans)


__all__ = ["eval_walk"]
