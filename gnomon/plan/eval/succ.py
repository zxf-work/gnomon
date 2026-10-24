"""`Succ`: a version's successor or predecessor along one time axis."""

from __future__ import annotations

from typing import Any

import numpy as np

from gnomon.plan.eval.scan import scan_edges, scan_nodes
from gnomon.plan.node import EdgeScan, NodeScan, Succ
from gnomon.plan.relation import Relation, _py


def eval_succ(node: Succ, rel: Relation, adapter: Any, scans: Any = None) -> Relation:
    v = node.into
    if node.kind == "node":
        versions = scan_nodes(NodeScan(v, belief=node.belief, vt_mode=node.vt_mode,
                                       sigma_=node.sigma), adapter, None, scans)
        ident = f"{v}.uid"
    else:
        versions = scan_edges(EdgeScan(v, belief=node.belief, vt_mode=node.vt_mode,
                                       sigma_=node.sigma), adapter, None)
        ident = f"{v}.eid"
    start = f"{v}.vt_s" if node.axis == "valid" else f"{v}.tt_s"
    ids = [_py(x) for x in versions.column(ident)]
    starts = [int(x) for x in versions.column(start)]
    vids = [str(x) for x in versions.column(f"{v}.vid")]
    chains: dict[Any, list[int]] = {}
    for i, k in enumerate(ids):
        chains.setdefault(k, []).append(i)
    step = 1 if node.dir == "next" else -1
    following: dict[str, int] = {}
    for rows in chains.values():
        rows.sort(key=lambda i: (starts[i], vids[i]))
        for pos, i in enumerate(rows):
            j = pos + step
            if 0 <= j < len(rows):
                following[vids[i]] = rows[j]
    of_vid = rel.column(f"{node.of}.vid")
    of_nulls = rel.null_mask(f"{node.of}.vid")
    keep, picked = [], []
    for i in range(rel.n):
        if of_nulls is not None and of_nulls[i]:
            continue
        j = following.get(str(of_vid[i]))
        if j is not None:
            keep.append(i)
            picked.append(j)
    out = rel.take(np.asarray(keep, dtype=np.int64))
    succ = versions.take(np.asarray(picked, dtype=np.int64))
    return out.with_columns(succ.schema, dict(succ.cols), dict(succ.nulls))


__all__ = ["eval_succ"]
