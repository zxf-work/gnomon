"""Raphtory (valid-time temporal graph, no transaction time) for the external
comparison: the three registry shapes its Python API expresses, answered on a
`PersistentGraph` built from the currently-believed rows only
(`eval_harness._raphtory_graph`).

Raphtory reproduces neither Gnomon version ids nor edge identity beyond
`(src, dst, layer)`, so each answer is a *projection* of the native payload —
the fields Raphtory can produce — and `project_native` derives the same
projection from the native engine's own answer for the verdict:

* `snap.hop2`: the reached nodes with their hop distance (first 100 by
  `(hop, uid)`, as native pages them) and the total.
* `nbr.evolution`: neighbours gained and lost (first 100 each) and totals.
  The degree series is not projected: native counts edge versions, Raphtory's
  degree counts distinct neighbours.
* `reach.window`: reachable uids with earliest arrival (first 100 by
  `(arrival, uid)`) and the total. Raphtory's built-in
  `temporally_reachable_nodes` follows *event* times with strictly increasing
  `t` along a path; Gnomon's operator lets a path wait inside an edge's validity
  interval (`tau = max(arrival, vt_s) < vt_e`). Different semantics, so this
  cell is expected to differ and is reported as such, not tuned to agree.

Everything else, and every belief-time question, is not expressible: there is
no transaction-time axis to load a superseded version into.
"""

from __future__ import annotations

from typing import Any

LIMIT = 100


def snapshot_subgraph(g, *, seeds, hops, t_valid, **_):
    snap = g.snapshot_at(int(t_valid))
    seen = {}
    frontier = []
    for s in seeds:
        if snap.node(str(s)) is not None:
            seen[str(s)] = 0
            frontier.append(str(s))
    for h in range(1, int(hops) + 1):
        nxt = set()
        for uid in frontier:
            node = snap.node(uid)
            if node is None:
                continue
            nxt.update(n.name for n in node.neighbours)
        new = sorted(u for u in nxt if u not in seen)
        for u in new:
            seen[u] = h
        frontier = new
        if not frontier:
            break
    nodes = sorted(([hh, u] for u, hh in seen.items()))
    return {"nodes": nodes[:LIMIT], "nodes_total": len(nodes)}


def neighborhood_evolution(g, *, uid, t1, t2, **_):
    def nbrs(t):
        node = g.snapshot_at(int(t)).node(str(uid))
        return {n.name for n in node.neighbours if n.name != uid} if node is not None else set()
    a, b = nbrs(t1), nbrs(t2)
    gained, lost = sorted(b - a), sorted(a - b)
    return {"gained": gained[:LIMIT], "gained_total": len(gained),
            "lost": lost[:LIMIT], "lost_total": len(lost)}


def temporal_reachability(g, *, src, window, **_):
    from raphtory import algorithms
    t_a, t_b = int(window["t_a"]), int(window["t_b"])
    w = g.window(t_a, t_b)
    state = algorithms.temporally_reachable_nodes(w, w.count_nodes(), t_a, [str(src)])
    rows = []
    for n, hist in state.items():
        if n.name == src:
            continue
        # each entry is {"0": time, "1": the node it was reached from}
        times = [int(e["0"]) for e in hist.get("reachable_nodes", [])]
        if times:
            rows.append([min(times), n.name])
    rows.sort()
    return {"rows": rows[:LIMIT], "rows_total": len(rows)}


QUERIES = {
    "snap.hop2": snapshot_subgraph,
    "nbr.evolution": neighborhood_evolution,
    "reach.window": temporal_reachability,
    # correction-density study: the current-belief controls only; every
    # belief-time id is not expressible (no transaction-time axis)
    "b.snap.hop2@now": snapshot_subgraph,
    "b.reach@now": temporal_reachability,
}


# ---- the fuller forms: the same output shape as Gnomon's call, by Raphtory's documented API, no tuning. The forms above stay as they are.
# snap: nodes with hops, and the induced edges with a total, each paged at the call's own limit, as Gnomon's snapshot_subgraph pages (b.snap.hop2 asks for 10,000). An edge is (src, dst, layer): this graph has no
# eid, vid, disc or version interval, since the loader coalesces intervals per (src, dst, rel). nbr: gained and lost at the call's limit, and the degree series Gnomon returns: incident (edge, layer) pairs at each bucket start.

def snapshot_with_edges(g, *, seeds, hops, t_valid, limit=LIMIT, **_):
    snap = g.snapshot_at(int(t_valid))
    seen = {}
    frontier = []
    for s in seeds:
        if snap.node(str(s)) is not None:
            seen[str(s)] = 0
            frontier.append(str(s))
    for h in range(1, int(hops) + 1):
        nxt = set()
        for uid in frontier:
            node = snap.node(uid)
            if node is None:
                continue
            nxt.update(n.name for n in node.neighbours)
        new = sorted(u for u in nxt if u not in seen)
        for u in new:
            seen[u] = h
        frontier = new
        if not frontier:
            break
    nodes = sorted(([hh, u] for u, hh in seen.items()))
    edges = snap.subgraph(list(seen)).edges.explode_layers()
    total = edges.count()
    rows = []
    for e in edges:
        if len(rows) >= limit:
            break
        rows.append([e.src.name, e.dst.name, e.layer_name])
    return {"nodes": nodes[:limit], "nodes_total": len(nodes), "rows": rows, "rows_total": total}


def evolution_with_series(g, *, uid, t1, t2, stride=None, limit=LIMIT, **_):
    t1, t2 = int(t1), int(t2)
    stride = stride or max(1, (t2 - t1) // 20)

    def nbrs(t):
        node = g.snapshot_at(int(t)).node(str(uid))
        return {n.name for n in node.neighbours if n.name != uid} if node is not None else set()
    a, b = nbrs(t1), nbrs(t2)
    gained, lost = sorted(b - a), sorted(a - b)
    series = []
    for t in range(t1, t2, stride):
        node = g.snapshot_at(t).node(str(uid))
        series.append({"t": t, "degree": node.edges.explode_layers().count() if node is not None else 0})
    return {"gained": gained[:limit], "gained_total": len(gained), "lost": lost[:limit], "lost_total": len(lost), "degree_series": series, "stride": stride}


QUERIES_EQ = {"snap.hop2": snapshot_with_edges, "b.snap.hop2@now": snapshot_with_edges, "nbr.evolution": evolution_with_series}


def project_native(qid: str, p: dict[str, Any]) -> dict[str, Any] | None:
    """The same projection, from the native engine's payload."""
    qid = {"b.snap.hop2@now": "snap.hop2", "b.reach@now": "reach.window",
           "b.snap.hop2@T": "snap.hop2", "b.reach@T": "reach.window"}.get(qid, qid)
    if qid == "snap.hop2":
        nodes = [[n["hop"], n["uid"]] for n in p["nodes"]]
        return {"nodes": nodes[:LIMIT], "nodes_total": p["nodes_total"]}
    if qid == "nbr.evolution":
        return {"gained": p["neighbors_gained"][:LIMIT], "gained_total": p["neighbors_gained_total"],
                "lost": p["neighbors_lost"][:LIMIT], "lost_total": p["neighbors_lost_total"]}
    if qid == "reach.window":
        rows = [[r["earliest_arrival"], r["uid"]] for r in p["rows"]]
        return {"rows": rows[:LIMIT], "rows_total": p["rows_total"]}
    return None
