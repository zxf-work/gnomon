#!/usr/bin/env python3
"""fs_table.py <results dir>: per query, point and system: instances in scope, compared, equal, rows only on each side, first-page order differences, prefix-only, not checked, not reached. No ratios."""
import glob
import json
import sys
from collections import defaultdict

t = defaultdict(lambda: defaultdict(int))
last = {}
for dirn in sys.argv[1:]:          # earlier directories first; a later record of an instance replaces an earlier one
    for p in sorted(glob.glob(dirn + "/cells-*.jsonl")):
        for x in open(p):
            r = json.loads(x)
            last[(r["point"], r["query"], r["instance"])] = r
for r in last.values():
    if True:
        for s in ("duckdbsql", "postgres"):
            k = (r["point"], r["query"], s)
            d = t[k]
            d["scope"] += 1
            if "systems" not in r:
                d[r["state"]] += 1
                continue
            c = r["systems"][s]
            if c["state"] != "compared":
                d[c["state"]] += 1
                continue
            d["compared"] += 1
            d["equal whole"] += bool(c["equal"])
            d["prefix only"] += bool(c["prefix_only_fields"])
            d["only system"] += c["only_system"]
            d["only gnomon"] += c["only_gnomon"]
            d["exact only system"] += c["exact_only_system"]
            d["exact only gnomon"] += c["exact_only_gnomon"]
            d["first page order"] += c["first_page_order_differs"]
            d["totals differ"] += bool(c["totals_differ"])
print("# Whole answers against Gnomon's, instances past the hashed page\n\nUntimed, no ratios. The other systems' rows come from a Gnomon store on W1, W2 and W5, so this checks query evaluation and not ingest. Rows compared as multisets under the hash's projection; exact-field counts beside them. 'prefix only' = the operator cannot return the whole list (snapshot nodes above 10,000, paths above k = 20): the returned part was compared and the instance is not counted as equal whole.\n")
print("| point | query | system | in scope | compared | equal whole | rows only in system | rows only in Gnomon | exact: only system, only Gnomon | first-page order differs | totals differ | prefix only | not checked: too large | not checked: over the limit | not checked: other | not reached | fraction compared |")
print("|---|---|---|---:|---:|---:|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|")
for (pt, q, s), d in sorted(t.items()):
    other = d["not checked: Gnomon refused or failed"] + d["not checked: failed"] + d["not checked: Gnomon over the limit"]
    print(f"| {pt} | {q} | {s} | {d['scope']} | {d['compared']} | {d['equal whole']} | {d['only system']} | {d['only gnomon']} | {d['exact only system']}, {d['exact only gnomon']} | {d['first page order']} | {d['totals differ']} | {d['prefix only']} | {d['not checked: too large']} | {d['not checked: over the limit']} | {other} | {d['not reached']} | {d['compared'] / d['scope']:.2f} |")
