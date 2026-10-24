"""`OrderedReach`: shortest valid paths over an explicit edge relation,
with time order along the traversal and per-node truncation."""

from __future__ import annotations

import functools
from typing import Any

import numpy as np

from gnomon.plan.eval.expand import _charge, _dense_map, _with_into
from gnomon.plan.node import OrderedReach
from gnomon.plan.relation import Relation, _py
from gnomon.plan.types import Column, Schema, T_INT


def adjacency(node: OrderedReach, edges: Relation) -> dict[str, list[tuple[str, int]]]:
    """Per source uid, its `(target uid, time)` edges, cut to `limit` under
    (key, time, target)."""
    def col(name: str) -> list[Any]:
        values, mask = edges.column(name), edges.null_mask(name)
        return [None if (mask is not None and mask[i]) else _py(values[i]) for i in range(edges.n)]

    src, dst, tim = col(node.src), col(node.dst), col(node.time)
    key = col(node.key) if node.key is not None else [None] * edges.n
    adj: dict[str, list[tuple[Any, int, str]]] = {}
    for i in range(edges.n):
        if src[i] is None or dst[i] is None or tim[i] is None:
            continue
        adj.setdefault(src[i], []).append((key[i], int(tim[i]), dst[i]))
    out: dict[str, list[tuple[str, int]]] = {}
    for u, lst in adj.items():
        if node.limit is not None:
            def cmp(a: tuple, b: tuple) -> int:
                ka, kb = a[0], b[0]
                if ka is None or kb is None:
                    if ka is None and kb is None:
                        c = 0
                    else:
                        c = 1 if ka is None else -1          # a null key last
                elif ka != kb:
                    c = (-1 if ka < kb else 1) * (-1 if node.key_desc else 1)
                else:
                    c = 0
                if c:
                    return c
                if a[1] != b[1]:
                    return -1 if a[1] < b[1] else 1
                return (a[2] > b[2]) - (a[2] < b[2])
            lst = sorted(lst, key=functools.cmp_to_key(cmp))[: node.limit]
        out[u] = [(v, t) for _k, t, v in lst]
    return out


def reach(node: OrderedReach, adj: dict[str, list[tuple[str, int]]], seed: str,
          budget: Any = None) -> dict[str, int]:
    """Shortest valid-path length per reached node (the seed at 0)."""
    sign = -1 if node.order in ("strict_decreasing", "decreasing") else 1
    strict = node.order.startswith("strict")
    ordered = node.order != "none"
    dist = {seed: 0}
    best: dict[str, float] = {seed: float("-inf")}
    frontier: dict[str, float] = {seed: float("-inf")}
    depth = 0
    while frontier and (node.hi is None or depth < node.hi):
        depth += 1
        nxt: dict[str, float] = {}
        for u, at in frontier.items():
            lst = adj.get(u, [])
            _charge(budget, len(lst))
            for v, t in lst:
                tt = sign * t
                if ordered and not (tt > at if strict else tt >= at):
                    continue
                arrive = tt if ordered else float("-inf")
                if arrive < nxt.get(v, float("inf")):
                    nxt[v] = arrive
        frontier = {}
        for v, arrive in nxt.items():
            if v not in dist:
                dist[v] = depth
            if v not in best or arrive < best[v]:
                best[v] = arrive
                frontier[v] = arrive
    return dist


def eval_reach(node: OrderedReach, rel: Relation, edges: Relation, adapter: Any,
               live: frozenset[str] | None = None, budget: Any = None,
               scans: Any = None) -> Relation:
    adj = adjacency(node, edges)
    from_uids = rel.column(node.from_column)
    from_nulls = rel.null_mask(node.from_column)
    memo: dict[str, list[tuple[int, str]]] = {}
    rows: list[int] = []
    targets: list[str] = []
    depths: list[int] = []
    for i in range(rel.n):
        if from_nulls is not None and from_nulls[i]:
            continue
        seed = str(from_uids[i])
        if seed not in memo:
            d = reach(node, adj, seed, budget)
            memo[seed] = sorted((at, v) for v, at in d.items()
                                if at >= node.lo and (node.hi is None or at <= node.hi))
        for at, v in memo[seed]:
            rows.append(i)
            targets.append(v)
            depths.append(at)
    dense = _dense_map(adapter, np.array(targets, dtype=object))
    out = rel.take(np.asarray(rows, dtype=np.int64))
    out = _with_into(out, node, [int(dense[v]) for v in targets], adapter, live, scans)
    return out.with_columns(Schema.of(Column(f"{node.into}.depth", T_INT)),
                            {f"{node.into}.depth": np.asarray(depths, dtype=np.int64)})


__all__ = ["adjacency", "eval_reach", "reach"]
