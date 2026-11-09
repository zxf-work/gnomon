#!/usr/bin/env python3
"""
    kf_pilot_params.py --in params-K10M.json --out DIR/params-K10M.json --queries a,b,c [--n 3] [--first-out F]

Per query, up to --n instances, strata preferred in the order p50, hub, p90, p99, p10,
correction-touched p50, correction-free p50, then file order. --first-out writes the
file with only the first chosen instance per query (the overhead-control pass).
"""
import argparse
import json

ap = argparse.ArgumentParser()
ap.add_argument("--in", dest="src", required=True)
ap.add_argument("--out", required=True)
ap.add_argument("--first-out")
ap.add_argument("--queries", required=True)
ap.add_argument("--n", type=int, default=3)
a = ap.parse_args()
PREF = ["p50", "hub", "p90", "p99", "p10", "correction-touched p50", "correction-free p50",
        "correction-touched p90", "correction-free p90"]
doc = json.load(open(a.src))
want = a.queries.split(",")
by = {}
for i in doc["instances"]:
    by.setdefault(i["query"], []).append(i)
chosen = {}
for q in want:
    insts = by.get(q, [])
    pick = []
    for s in PREF:
        for i in insts:
            if i.get("stratum") == s and i not in pick:
                pick.append(i); break
        if len(pick) >= a.n:
            break
    for i in insts:
        if len(pick) >= a.n:
            break
        if i not in pick:
            pick.append(i)
    chosen[q] = pick
out = dict(doc)
out["instances"] = [i for q in want for i in chosen[q]]
out["pilot"] = {"from": a.src, "queries": want, "n": a.n}
json.dump(out, open(a.out, "w"), indent=1)
if a.first_out:
    f = dict(out); f["instances"] = [chosen[q][0] for q in want if chosen[q]]
    json.dump(f, open(a.first_out, "w"), indent=1)
print({q: [(i["instance"], i.get("stratum")) for i in chosen[q]] for q in want})
