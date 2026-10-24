"""The the shared-instant rule nodes: a
variable-length step, an `optional`/`not` join and the per-version-tuple
collapse, under one shared valid instant. Reference evaluators over the
window scans, with instant sets held as sorted lists of disjoint half-open
intervals."""

from __future__ import annotations

from collections import defaultdict
from typing import Any

import numpy as np

from gnomon.plan.eval.join import _hashable, _keys, _null_column
from gnomon.plan.eval.scan import scan_edges, scan_nodes
from gnomon.plan.eval.select import check_join_keys
from gnomon.plan.node import Bounded, Collapse, CoValidExpand, CoValidJoin, EdgeScan, NodeScan
from gnomon.plan.relation import Relation, _py
from gnomon.plan.types import Column, Schema, T_INT

Ivs = list[tuple[int, int]]


# ---------------------------------------------------------------------------
# instant sets
# ---------------------------------------------------------------------------

def ivs_norm(ivs: Ivs) -> Ivs:
    """Sorted, merged (touching intervals joined), empty ones dropped."""
    out: Ivs = []
    for s, e in sorted(x for x in ivs if x[0] < x[1]):
        if out and s <= out[-1][1]:
            if e > out[-1][1]:
                out[-1] = (out[-1][0], e)
        else:
            out.append((s, e))
    return out


def ivs_and(a: Ivs, b: Ivs) -> Ivs:
    out: Ivs = []
    i = j = 0
    while i < len(a) and j < len(b):
        s, e = max(a[i][0], b[j][0]), min(a[i][1], b[j][1])
        if s < e:
            out.append((s, e))
        if a[i][1] < b[j][1]:
            i += 1
        else:
            j += 1
    return out


def ivs_sub(a: Ivs, b: Ivs) -> Ivs:
    out: Ivs = []
    j = 0
    for s, e in a:
        cur = s
        while j < len(b) and b[j][1] <= cur:
            j += 1
        k = j
        while k < len(b) and b[k][0] < e:
            if b[k][0] > cur:
                out.append((cur, b[k][0]))
            cur = max(cur, b[k][1])
            if cur >= e:
                break
            k += 1
        if cur < e:
            out.append((cur, e))
    return out


def ivs_or(a: Ivs, b: Ivs) -> Ivs:
    return ivs_norm(a + b)


def _window(node: Any) -> tuple[int, int]:
    t_v = node.sigma.t_v
    return min(i.start for i in t_v), max(i.end for i in t_v)


# ---------------------------------------------------------------------------
# CoValidExpand
# ---------------------------------------------------------------------------

def _bounds(node: CoValidExpand) -> tuple[int, int | None]:
    if isinstance(node.hops, Bounded):
        return node.hops.a, node.hops.b
    return node.hops.a, None                          # Unbounded


def covalid_search(adj: dict[Any, list[tuple[Any, int, int]]], exists: dict[Any, Ivs],
                   seed: Any, start: Ivs, hi: int | None,
                   budget: Any = None) -> dict[Any, list[tuple[Ivs, int]]]:
    """Per reached node, its `(instants, d_t)` pieces: the instant set where
    its shortest co-valid walk distance from `seed` is `d_t` (disjoint)."""
    from gnomon.plan.eval.expand import _charge

    first = ivs_and(start, exists.get(seed, []))
    if not first:
        return {}
    covered: dict[Any, Ivs] = {seed: first}
    reach: dict[Any, list[tuple[Ivs, int]]] = {seed: [(first, 0)]}
    frontier: dict[Any, Ivs] = {seed: first}
    depth = 0
    while frontier and (hi is None or depth < hi):
        depth += 1
        nxt: dict[Any, Ivs] = {}
        for u in sorted(frontier, key=str):
            got = frontier[u]
            lst = adj.get(u, [])
            _charge(budget, len(lst))
            for v, s, e in lst:
                k = ivs_and(got, [(s, e)])
                if not k:
                    continue
                k = ivs_sub(ivs_and(k, exists.get(v, [])), covered.get(v, []))
                if not k:
                    continue
                covered[v] = ivs_or(covered.get(v, []), k)
                nxt[v] = ivs_or(nxt.get(v, []), k)
        for v, k in nxt.items():
            reach.setdefault(v, []).append((k, depth))
        frontier = nxt
    return reach


def eval_covalid_expand(node: CoValidExpand, rel: Relation, adapter: Any,
                        budget: Any = None, scans: Any = None) -> Relation:
    va, vb = _window(node)
    lo, hi = _bounds(node)
    edges = scan_edges(EdgeScan("_cve", rel_types=(node.rel_type,) if node.rel_type else None,
                                belief=node.belief, vt_mode="overlap", sigma_=node.sigma),
                       adapter, None)
    nodes = scan_nodes(NodeScan(node.into, belief=node.belief, vt_mode="overlap",
                                sigma_=node.sigma), adapter, None, scans)
    adj: dict[Any, list[tuple[Any, int, int]]] = defaultdict(list)
    src, dst = edges.column("_cve.src"), edges.column("_cve.dst")
    es, ee = edges.column("_cve.vt_s"), edges.column("_cve.vt_e")
    for i in range(edges.n):
        a, b = _py(src[i]), _py(dst[i])
        s, e = max(int(es[i]), va), min(int(ee[i]), vb)
        if s >= e:
            continue
        if node.dir in ("out", "both"):
            adj[a].append((b, s, e))
        if node.dir in ("in", "both"):
            adj[b].append((a, s, e))
    uid, ns, ne = (nodes.column(f"{node.into}.{c}") for c in ("uid", "vt_s", "vt_e"))
    vids = nodes.column(f"{node.into}.vid")
    raw: dict[Any, Ivs] = defaultdict(list)
    versions: dict[Any, list[int]] = defaultdict(list)
    for j in range(nodes.n):
        u = _py(uid[j])
        s, e = max(int(ns[j]), va), min(int(ne[j]), vb)
        if s < e:
            raw[u].append((s, e))
            versions[u].append(j)
    exists = {u: ivs_norm(x) for u, x in raw.items()}
    for u in versions:
        versions[u].sort(key=lambda j: (int(ns[j]), str(vids[j])))

    seeds = rel.column(node.from_column)
    seed_nulls = rel.null_mask(node.from_column)
    if node.from_iv is not None:
        fs, fe = rel.column(node.from_iv[0]), rel.column(node.from_iv[1])
        fnull = [rel.null_mask(c) for c in node.from_iv]
    memo: dict[tuple, list[tuple[int, int, int, int]]] = {}
    rows: list[int] = []
    picked: list[int] = []
    depths: list[int] = []
    p_s: list[int] = []
    p_e: list[int] = []
    for i in range(rel.n):
        if seed_nulls is not None and seed_nulls[i]:
            continue
        seed = _py(seeds[i])
        if node.from_iv is not None:
            if any(m is not None and m[i] for m in fnull):
                continue
            start = [(max(va, int(fs[i])), min(vb, int(fe[i])))]
        else:
            start = [(va, vb)]
        start = [x for x in start if x[0] < x[1]]
        key = (seed, tuple(start))
        if key not in memo:
            reach = covalid_search(adj, exists, seed, start, hi, budget)
            found: list[tuple[int, str, int, int, int, int]] = []
            for v, pieces in reach.items():
                for k, d in pieces:
                    if d < lo:
                        continue
                    for j in versions.get(v, []):
                        mine = ivs_and(k, [(max(int(ns[j]), va), min(int(ne[j]), vb))])
                        for s, e in mine:
                            found.append((d, str(v), int(ns[j]), j, s, e))
            found.sort(key=lambda x: (x[0], x[1], x[2], str(vids[x[3]]), x[4]))
            memo[key] = [(j, d, s, e) for d, _v, _t, j, s, e in found]
        for j, d, s, e in memo[key]:
            rows.append(i)
            picked.append(j)
            depths.append(d)
            p_s.append(s)
            p_e.append(e)
    out = rel.take(np.asarray(rows, dtype=np.int64))
    tgt = nodes.take(np.asarray(picked, dtype=np.int64))
    out = out.with_columns(tgt.schema, dict(tgt.cols), dict(tgt.nulls))
    ts = node.out_schema.tau_of(f"{node.into}.ps")
    return out.with_columns(
        Schema.of(Column(f"{node.into}.depth", T_INT), Column(f"{node.into}.ps", ts),
                  Column(f"{node.into}.pe", ts)),
        {f"{node.into}.depth": np.asarray(depths, dtype=np.int64),
         f"{node.into}.ps": np.asarray(p_s, dtype=np.int64),
         f"{node.into}.pe": np.asarray(p_e, dtype=np.int64)})


# ---------------------------------------------------------------------------
# CoValidJoin
# ---------------------------------------------------------------------------

def eval_covalid_join(node: CoValidJoin, left: Relation, right: Relation) -> Relation:
    left_keys = tuple(p[0] for p in node.on)
    right_keys = tuple(p[1] for p in node.on)
    check_join_keys(left, left_keys, "left")
    check_join_keys(right, right_keys, "right")
    index: dict[Any, list[int]] = defaultdict(list)
    for j, key in enumerate(_keys(right, right_keys)):
        index[key].append(j)
    ls, le = (left.column(c) for c in node.left_iv)
    rs, re_ = (right.column(c) for c in node.right_iv)
    outer = node.join_type == "left_outer"
    l_idx: list[int] = []
    r_idx: list[int] = []                 # -1: the null extension
    o_s: list[int] = []
    o_e: list[int] = []
    for i, key in enumerate(_keys(left, left_keys)):
        piece = [(int(ls[i]), int(le[i]))]
        if piece[0][0] >= piece[0][1]:
            continue
        cover: Ivs = []
        for j in index.get(key, ()):
            got = ivs_and(piece, [(int(rs[j]), int(re_[j]))])
            if not got:
                continue
            cover.append(got[0])
            if outer:
                l_idx.append(i)
                r_idx.append(j)
                o_s.append(got[0][0])
                o_e.append(got[0][1])
        for s, e in ivs_sub(piece, ivs_norm(cover)):
            l_idx.append(i)
            r_idx.append(-1)
            o_s.append(s)
            o_e.append(e)
    out = left.take(np.asarray(l_idx, dtype=np.int64))
    if outer:
        nullable = right.nullable_copy()
        rows = np.asarray([max(j, 0) for j in r_idx], dtype=np.int64)
        missing = np.asarray([j < 0 for j in r_idx], dtype=bool)
        cols, nulls = {}, {}
        for c in nullable.schema:
            if right.n:
                vals = nullable.cols[c.name][rows] if len(rows) else nullable.cols[c.name][:0]
            else:
                vals = _null_column(nullable, c.name, len(rows))
            cols[c.name] = vals
            m = nullable.nulls.get(c.name)
            m = (m[rows] if (m is not None and right.n and len(rows)) else
                 np.zeros(len(rows), dtype=bool))
            nulls[c.name] = m | missing
        out = out.with_columns(nullable.schema, cols, nulls)
    ts = node.out_schema.tau_of(node.out_iv[0])
    return out.with_columns(Schema.of(Column(node.out_iv[0], ts), Column(node.out_iv[1], ts)),
                            {node.out_iv[0]: np.asarray(o_s, dtype=np.int64),
                             node.out_iv[1]: np.asarray(o_e, dtype=np.int64)})


# ---------------------------------------------------------------------------
# Collapse
# ---------------------------------------------------------------------------

def eval_collapse(node: Collapse, rel: Relation) -> Relation:
    cols = [(rel.column(c), rel.null_mask(c)) for c in node.by]
    first: dict[tuple, int] = {}
    order: list[int] = []
    mins: dict[str, dict[tuple, int]] = {c: {} for c in node.mins}
    for i in range(rel.n):
        key = tuple(None if (m is not None and m[i]) else _hashable(v[i]) for v, m in cols)
        if key not in first:
            first[key] = i
            order.append(i)
        for c in node.mins:
            m = rel.null_mask(c)
            if m is not None and m[i]:
                continue
            x = int(rel.column(c)[i])
            cur = mins[c].get(key)
            if cur is None or x < cur:
                mins[c][key] = x
    out = rel.take(np.asarray(order, dtype=np.int64))
    if not node.mins:
        return out
    keys = list(first)
    new_cols = dict(out.cols)
    new_nulls = dict(out.nulls)
    for c in node.mins:
        vals = [mins[c].get(k) for k in keys]
        new_cols[c] = np.asarray([0 if v is None else v for v in vals], dtype=np.int64)
        if any(v is None for v in vals):
            new_nulls[c] = np.asarray([v is None for v in vals], dtype=bool)
        else:
            new_nulls.pop(c, None)
    return Relation(out.schema, new_cols, out.n, new_nulls)


__all__ = ["covalid_search", "eval_collapse", "eval_covalid_expand", "eval_covalid_join",
           "ivs_and", "ivs_norm", "ivs_or", "ivs_sub"]
