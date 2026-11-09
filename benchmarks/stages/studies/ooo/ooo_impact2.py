#!/usr/bin/env python3
"""Impact scan, second form: the valid intervals that correct, retract and later assert ops write, in log order, against the endpoints' node versions at that belief. No gnomon import. ooo_impact2.py <log> <out.json>

ooo_impact.py compared ingest and assert_edge edges with each node's final smallest start. It fed a correct's vt_s into its endpoint counts and into the node's start, but did not test the interval at its belief, and did not test node retractions at all. This form replays the rule of gnomon/storage/base.py, in order, batch by batch:
  node versions: `nodes` records, bare versions, assert_node (carves), correct on a node (carves: remainders kept, the stated interval written even past the old bounds), retract on a node (the version valid at t ends at t; gone if it started at t).
  edges: ingest and assert_edge insert; correct on an edge carves and writes the stated interval; retract on an edge ends the version valid at t.
Reported separately:
  correct on an edge: the new interval starting before an endpoint's version start at that belief, per side; ending past the endpoint's last version end at that belief.
  later edge inserts (assert_edge, ingest in a later batch than the node's first version) below the start at that belief.
  correct on a node, assert_node, retract on a node: at the end of the batch, does that node have a believed edge version outside its believed node versions? Counted per op kind, and how many edge versions.
  at the end of the log: nodes with believed edge versions outside their versions, and those edge versions.
Per-edge state is kept only for nodes the log can shrink or bound: a correct or retract on a node, or a version with a finite end. Others have one open version from their start, so the start test covers them. An op with no version to hit (the engine would refuse the batch) is counted, not applied."""
import json
import sys
import time
from collections import Counter

try:
    import orjson
    loads = orjson.loads
except ImportError:
    loads = json.loads

OPEN = 4611686018427387904


def carve(vs, a, b):
    """versions after writing [a, b): remainders of the overlapping ones, plus [a, b); returns (list, n overlapped)"""
    out, hit = [], 0
    for s, e in vs:
        if s < b and a < e:
            hit += 1
            if s < a:
                out.append((s, a))
            if b < e:
                out.append((b, e))
        else:
            out.append((s, e))
    out.append((a, b))
    return out, hit


def retract_at(vs, t):
    out, hit = [], 0
    for s, e in vs:
        if s <= t < e:
            hit += 1
            if s < t:
                out.append((s, t))
        else:
            out.append((s, e))
    return out, hit


def outside(edge_vs, node_vs):
    """edge intervals not covered by the union of node versions"""
    if not edge_vs:
        return 0
    cov = []
    for s, e in sorted(node_vs):
        if cov and s <= cov[-1][1]:
            if e > cov[-1][1]:
                cov[-1] = (cov[-1][0], e)
        else:
            cov.append((s, e))
    n = 0
    for s, e in edge_vs:
        if not any(cs <= s and e <= ce for cs, ce in cov):
            n += 1
    return n


def main():
    path, out = sys.argv[1], sys.argv[2]
    t0 = time.time()
    U = set()
    with open(path, "rb") as f:     # the nodes that need per-edge state
        f.readline()
        for line in f:
            for op in loads(line)["ops"]:
                k = op["op"]
                if k in ("correct", "retract"):
                    r = op.get("ref") or {}
                    if r.get("kind") == "node":
                        U.add(r["uid"])
                elif k == "assert_node":
                    if op.get("vt_e", OPEN) != OPEN:
                        U.add(op["uid"])
                elif k == "ingest_events":
                    for r in op.get("nodes") or []:
                        if r.get("vt_e", OPEN) != OPEN:
                            U.add(r["uid"])
    t1 = time.time()
    c = Counter()
    start, end = {}, {}           # every uid: smallest start, largest end at this belief, as written so far (for U: recomputed from versions)
    created = {}
    nv = {}                       # uid in U -> node versions
    ev = {}                       # uid in U -> {edge key: versions}

    def add_version(u, a, b, carves):
        if u in U:
            cur = nv.get(u, [])
            cur, _ = carve(cur, a, b) if carves else (cur + [(a, b)], 0)
            nv[u] = cur
            start[u] = min(x[0] for x in cur)
            end[u] = max(x[1] for x in cur)
        else:
            s = start.get(u)
            start[u] = a if s is None else min(s, a)
            end[u] = max(end.get(u, b), b)

    def insert_edge(key, a, b, carves=False):
        for u in (key[0], key[1]):
            if u in U:
                d = ev.setdefault(u, {})
                if carves:
                    d[key], _ = carve(d.get(key, []), a, b)
                else:
                    d.setdefault(key, []).append((a, b))

    bn = 0
    with open(path, "rb") as f:
        f.readline()
        for line in f:
            bn += 1
            rec = loads(line)
            touched = []
            for op in rec["ops"]:
                k = op["op"]
                if k == "ingest_events":
                    offset = op.get("offset", 0)
                    evs = op.get("events") or []
                    expl = set()
                    for r in op.get("nodes") or []:
                        add_version(r["uid"], r["vt_s"], r.get("vt_e", OPEN), False)
                        created.setdefault(r["uid"], bn)
                        expl.add(r["uid"])
                    first = {}
                    for e in evs:
                        v = e["vt_s"]
                        for u in (e["src"], e["dst"]):
                            m = first.get(u)
                            if m is None or v < m:
                                first[u] = v
                    for u, m in first.items():
                        if u not in start and u not in expl:
                            add_version(u, m, OPEN, False)
                            created.setdefault(u, bn)
                    for i, e in enumerate(evs):
                        s, d, v = e["src"], e["dst"], e["vt_s"]
                        ve = e.get("vt_e") or v + 1
                        for side, u in (("src", s), ("dst", d)):
                            if created.get(u, bn) < bn and v < start[u]:
                                c["later ingest edge below the start at that belief, " + side] += 1
                        if s in U or d in U:
                            insert_edge((s, d, e["rel_type"], e.get("disc", f"#{offset + i}")), v, ve)
                elif k == "assert_node":
                    u = op["uid"]
                    add_version(u, op["vt_s"], op.get("vt_e", OPEN), True)
                    created.setdefault(u, bn)
                    if u in U:
                        touched.append(("assert_node", u))
                elif k == "assert_edge":
                    s, d, v, ve = op["src"], op["dst"], op["vt_s"], op.get("vt_e", OPEN)
                    c["assert_edge"] += 1
                    for side, u in (("src", s), ("dst", d)):
                        if u in start and v < start[u]:
                            c["assert_edge below the start at that belief, " + side] += 1
                        elif u not in start:
                            c["assert_edge with no version of the endpoint yet, " + side] += 1
                    if s in U or d in U:
                        insert_edge((s, d, op["rel_type"], op.get("disc", "")), v, ve, True)
                elif k == "correct":
                    r = op["ref"]
                    v, ve = op["vt_s"], op.get("vt_e", OPEN)
                    if r["kind"] == "node":
                        u = r["uid"]
                        c["correct on a node"] += 1
                        s0, e0 = start.get(u), end.get(u)
                        cur, hit = carve(nv.get(u, []), v, ve)
                        if not hit:
                            c["correct on a node, no version to hit"] += 1
                            continue
                        nv[u] = cur
                        start[u] = min(x[0] for x in cur)
                        end[u] = max(x[1] for x in cur)
                        if start[u] < s0:
                            c["correct on a node moved its start earlier"] += 1
                        if end[u] > e0:
                            c["correct on a node moved its end later"] += 1
                        touched.append(("correct", u))
                    else:
                        s, d = r["src"], r["dst"]
                        c["correct on an edge"] += 1
                        for side, u in (("src", s), ("dst", d)):
                            if u not in start:
                                c["correct on an edge, endpoint with no version, " + side] += 1
                                continue
                            if v < start[u]:
                                c["correct on an edge, new interval starts before the start at that belief, " + side] += 1
                            if ve > end[u]:
                                c["correct on an edge, new interval ends past the end at that belief, " + side] += 1
                        if s in U or d in U:
                            insert_edge((s, d, r["rel_type"], r.get("disc", "")), v, ve, True)
                elif k == "retract":
                    r = op["ref"]
                    t = op["t"]
                    if r["kind"] == "node":
                        u = r["uid"]
                        c["retract on a node"] += 1
                        cur, hit = retract_at(nv.get(u, []), t)
                        if not hit:
                            c["retract on a node, no version to hit"] += 1
                            continue
                        nv[u] = cur
                        if cur:
                            start[u] = min(x[0] for x in cur)
                            end[u] = max(x[1] for x in cur)
                        else:
                            c["retract on a node left it with no version"] += 1
                        touched.append(("retract", u))
                    else:
                        c["retract on an edge"] += 1
                        key = (r["src"], r["dst"], r["rel_type"], r.get("disc", ""))
                        for u in (key[0], key[1]):
                            if u in U and key in ev.get(u, {}):
                                ev[u][key], _ = retract_at(ev[u][key], t)
            for kind, u in touched:      # at the end of the batch: the batch is one belief
                n = sum(outside(vs, nv.get(u, [])) for vs in ev.get(u, {}).values())
                if n:
                    c[f"{kind} on a node leaves believed edge versions outside, ops"] += 1
                    c[f"{kind} on a node leaves believed edge versions outside, edge versions"] += n
    nodes_out = edges_out = 0
    for u, d in ev.items():
        n = sum(outside(vs, nv.get(u, [])) for vs in d.values())
        if n:
            nodes_out += 1
            edges_out += n
    res = {"log": path, "batches": bn, "uids_with_a_version": len(start), "nodes_with_per_edge_state": len(U), "counts": dict(c),
           "end_of_log_nodes_with_believed_edges_outside": nodes_out, "end_of_log_edge_versions_outside": edges_out,
           "seconds_prepass": round(t1 - t0, 1), "seconds": round(time.time() - t0, 1)}
    with open(out, "w") as f:
        json.dump(res, f, indent=1)
    print(json.dumps(res), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
