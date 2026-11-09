#!/usr/bin/env python3
"""Impact scan: on a frozen log, what the engine's node rule does, without importing gnomon. ooo_impact.py <log> <out.json> [--edges out.jsonl.gz]

The rule, from gnomon/storage/base.py: _ingest_events and crates/.../ingest_raw.rs. In an ingest_events op, a uid in `nodes` gets that version. An endpoint with no believed version and not in `nodes`
gets a bare version starting at the smallest vt_s of that uid in this op. A known node is never extended. assert_edge and correct create no node version; assert_node writes the version it states.
A node's version start here is the smallest vt_s over the versions the log creates for it. A node version closed by a retraction is treated as still known (the log's node retractions are counted, so the size of this simplification is on the record).

Reported: where nodes come from; nodes for which a later batch holds an event with vt_s below the node's version start at that moment; edge versions whose vt_s is below an endpoint's final version start; edges with an endpoint that never has a version."""
import gzip
import json
import sys
import time
from collections import Counter

try:
    import orjson
    loads = orjson.loads
except ImportError:
    loads = json.loads


def main():
    path, out = sys.argv[1], sys.argv[2]
    edges_out = sys.argv[4] if len(sys.argv) > 4 and sys.argv[3] == "--edges" else None
    t0 = time.time()
    start, min_edge, c = {}, {}, Counter()
    late = set()      # nodes with a later batch's event below the start at that moment
    created_in = {}   # uid -> batch number of its first version

    def edge(u, vs, b):
        m = min_edge.get(u)
        if m is None or vs < m:
            min_edge[u] = vs
        s = start.get(u)
        if s is not None and vs < s and created_in.get(u, b) < b:
            late.add(u)

    def version(u, vs, b):
        s = start.get(u)
        if s is None:
            start[u] = vs
            created_in[u] = b
        elif vs < s:
            start[u] = vs
    b = 0
    with open(path, "rb") as f:
        f.readline()
        for line in f:
            b += 1
            rec = loads(line)
            for op in rec["ops"]:
                k = op["op"]
                c["op " + k] += 1
                if k == "ingest_events":
                    evs = op.get("events") or []
                    explicit = op.get("nodes") or []
                    for r in explicit:
                        version(r["uid"], r["vt_s"], b)
                        c["nodes record"] += 1
                    expl = {r["uid"] for r in explicit} if explicit else ()
                    first = {}
                    for e in evs:
                        vs = e["vt_s"]
                        for u in (e["src"], e["dst"]):
                            m = first.get(u)
                            if m is None or vs < m:
                                first[u] = vs
                    c["edge versions, ingest"] += len(evs)
                    for u, m in first.items():
                        if u not in start and u not in expl:
                            version(u, m, b)
                            c["bare versions auto-created"] += 1
                    for e in evs:
                        vs = e["vt_s"]
                        edge(e["src"], vs, b)
                        edge(e["dst"], vs, b)
                elif k == "assert_node":
                    version(op["uid"], op["vt_s"], b)
                elif k == "assert_edge":
                    c["edge versions, assert_edge"] += 1
                    edge(op["src"], op["vt_s"], b)
                    edge(op["dst"], op["vt_s"], b)
                elif k in ("correct", "retract"):
                    ref = op.get("ref") or {}
                    kind = ref.get("kind")
                    c[f"op {k} on {kind}"] += 1
                    if k == "correct" and kind == "node":
                        version(ref.get("uid") or op.get("uid"), op["vt_s"], b)
                    if k == "correct" and kind == "edge" and "vt_s" in op:
                        edge(ref["src"], op["vt_s"], b)
                        edge(ref["dst"], op["vt_s"], b)
    both = [u for u, m in min_edge.items() if u not in start or m < start[u]]
    aff = {u for u in both if u in start}
    novers = {u for u in both if u not in start}
    res = {"log": path, "batches": b, "ops": dict(c), "uids_with_a_version": len(start), "uids_with_edges": len(min_edge),
           "nodes_later_batch_event_below_start": len(late), "endpoints_with_an_edge_below_final_start": len(aff), "endpoints_never_with_a_version": len(novers)}
    n = Counter()
    if both:
        zout = gzip.open(edges_out, "wt") if edges_out else None
        written = 0
        with open(path, "rb") as f:
            f.readline()
            for line in f:
                rec = loads(line)
                for op in rec["ops"]:
                    k = op["op"]
                    if k == "ingest_events":
                        es = op.get("events") or []
                    elif k == "assert_edge":
                        es = [op]
                    else:
                        continue
                    for e in es:
                        s, d, vs = e["src"], e["dst"], e["vt_s"]
                        hit = False
                        for side, u in (("src", s), ("dst", d)):
                            if u in aff and vs < start[u]:
                                n["edges below start, " + side] += 1
                                hit = True
                            elif u in novers:
                                n["edges with an endpoint never a version"] += 1
                        if hit:
                            n["edges below an endpoint's start"] += 1
                            if zout and written < 3_000_000:
                                zout.write(json.dumps({"src": s, "dst": d, "rel": e.get("rel_type"), "vt_s": vs, "vt_e": e.get("vt_e"), "tt": rec["tt"], "s_start": start.get(s), "d_start": start.get(d)}) + "\n")
                                written += 1
        if zout:
            zout.close()
    res["edges"] = dict(n)
    res["edge_versions_total"] = c["edge versions, ingest"] + c["edge versions, assert_edge"]
    res["seconds"] = round(time.time() - t0, 1)
    with open(out, "w") as f:
        json.dump(res, f, indent=1)
    print(json.dumps({k: res[k] for k in ("log", "uids_with_a_version", "nodes_later_batch_event_below_start", "endpoints_with_an_edge_below_final_start", "endpoints_never_with_a_version", "edges", "edge_versions_total", "seconds")}), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
