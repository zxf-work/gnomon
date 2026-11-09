#!/usr/bin/env python3
"""Hand-made log for the node branch of ooo_impact2.py. Each node is one case; the expected counts are written here by hand, and the scan's must equal them exactly.
impact2_handtest.py <tool> <workdir>

By the rule in gnomon/storage/base.py::_correct, a correct on a node keeps the remainders of the versions it overlaps, so it cannot narrow a node: stating a narrow interval (A) changes nothing outside it. A correct leaves a believed edge outside only where the edge was outside already (F, a finite version). retract is the op that shrinks (B)."""
import json
import subprocess
import sys

OPEN = 4611686018427387904
tool, work = sys.argv[1], sys.argv[2]
nodes = [("A", 0, OPEN), ("B", 0, OPEN), ("C", 0, OPEN), ("D", 0, OPEN), ("E", 30, OPEN), ("F", 0, 50), ("G", 0, 50), ("I", 0, 50), ("J", 40, OPEN), ("K", 60, OPEN), ("Z", 0, OPEN)]


def edge(u, a, b):
    return {"op": "assert_edge", "src": u, "dst": "Z", "rel_type": "R", "disc": "", "vt_s": a, "vt_e": b}


def ref_e(u):
    return {"kind": "edge", "src": u, "dst": "Z", "rel_type": "R", "disc": ""}


def corr_n(u, a, b):
    return {"op": "correct", "ref": {"kind": "node", "uid": u}, "props": {"x": 1}, "vt_s": a, "vt_e": b}


def ret_n(u, t):
    return {"op": "retract", "ref": {"kind": "node", "uid": u}, "t": t}


batches = [
    [{"op": "ingest_events", "events": [], "node_label": "N", "nodes": [{"uid": u, "label": "N", "vt_s": a, "vt_e": b} for u, a, b in nodes]}],
    [edge("A", 10, 20), edge("B", 10, 80), edge("C", 10, 40), edge("D", 10, 80), edge("E", 35, 40), edge("F", 10, 80), edge("G", 10, 30), edge("I", 10, 30), edge("J", 41, 42),
     edge("K", 10, 20)],                              # K: an edge below its node's start (bad)
    [corr_n("A", 5, 15)],                             # a narrow correct: remainders kept, the edge stays inside (clean)
    [ret_n("B", 50)],                                 # retract shrinks B under its edge [10, 80) (bad)
    [ret_n("C", 50)],                                 # retract, the edge [10, 40) stays inside (clean)
    [ret_n("D", 50), {"op": "retract", "ref": ref_e("D"), "t": 50}],    # the node and its edge in one batch: one belief (clean)
    [corr_n("F", 10, 20)],                            # F's edge [10, 80) is outside F's [0, 50) already; the correct leaves it outside (bad)
    [corr_n("G", 10, 20)],                            # edge inside (clean)
    [corr_n("J", 30, 45)],                            # a correct moving a node's start earlier
    [{"op": "correct", "ref": ref_e("E"), "props": {"x": 1}, "vt_s": 20, "vt_e": 40}],   # new interval before E's start 30 (bad, src)
    [{"op": "correct", "ref": ref_e("I"), "props": {"x": 1}, "vt_s": 10, "vt_e": 70}],   # new interval past I's end 50 (bad, src)
]
log = work + "/hand.eventlog.jsonl"
with open(log, "w") as f:
    f.write(json.dumps({"format": "gnomon-events", "version": 1}) + "\n")
    for i, ops in enumerate(batches):
        f.write(json.dumps({"batch_id": "x", "tt": i + 1, "ops": ops}) + "\n")
out = work + "/hand-result.json"
subprocess.check_call([sys.executable, tool, log, out])
got = json.load(open(out))
expect = {
    "assert_edge": 10,
    "assert_edge below the start at that belief, src": 1,                                  # K
    "correct on a node": 4,                                                                 # A, F, G, J
    "correct on a node moved its start earlier": 1,                                         # J
    "correct on a node leaves believed edge versions outside, ops": 1,                      # F
    "correct on a node leaves believed edge versions outside, edge versions": 1,
    "retract on a node": 3,                                                                 # B, C, D
    "retract on a node leaves believed edge versions outside, ops": 1,                      # B
    "retract on a node leaves believed edge versions outside, edge versions": 1,
    "retract on an edge": 1,                                                                # D
    "correct on an edge": 2,                                                                # E, I
    "correct on an edge, new interval starts before the start at that belief, src": 1,      # E
    "correct on an edge, new interval ends past the end at that belief, src": 1,            # I
}
bad = []
for k in sorted(set(expect) | set(got["counts"])):
    e, g = expect.get(k, 0), got["counts"].get(k, 0)
    print(f"{'ok ' if e == g else 'DIFF'} {k}: expected {e}, got {g}")
    if e != g:
        bad.append(k)
for k, e in (("end_of_log_nodes_with_believed_edges_outside", 3), ("end_of_log_edge_versions_outside", 3)):      # B, F, I
    print(f"{'ok ' if got[k] == e else 'DIFF'} {k}: expected {e}, got {got[k]}")
    if got[k] != e:
        bad.append(k)
print("HAND TEST", "FAILED: " + str(bad) if bad else "PASS")
sys.exit(1 if bad else 0)
