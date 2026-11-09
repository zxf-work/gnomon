#!/usr/bin/env python3
"""fsx_table.py RESDIR CLOSUREDIR: the closure table from cells-<point>.jsonl (untimed, no ratios). Per point, query and system: instances
selected, compared whole and equal, differing, still a prefix, over the limit, failed, Gnomon not answered, not reached; and the Gnomon
whole-answer method with its dedicated-vs-whole check. Then the instances listed too large (not run) with their recorded totals."""
import glob
import json
import sys
from collections import defaultdict
from pathlib import Path

R, C = Path(sys.argv[1]), Path(sys.argv[2])
rows = defaultdict(lambda: defaultdict(int))
meth = defaultdict(set)
dvw = defaultdict(lambda: [0, 0])
for f in sorted(R.glob("cells-*.jsonl")):
    last = {}
    for x in open(f):
        r = json.loads(x)
        last[r["key"]] = r
    for r in last.values():
        g = r["gnomon"]
        if g.get("state") == "returned":
            meth[(r["point"], r["query"])].add(g["method"].split(", k =")[0])
            dvw[(r["point"], r["query"])][0] += 1
            dvw[(r["point"], r["query"])][1] += bool(g["dedicated_vs_whole"]["differs"])
        for s in r["need"]:
            v = r["systems"].get(s, {})
            k = (r["point"], r["query"], s)
            rows[k]["selected"] += 1
            if v.get("compared"):
                if v.get("equal"):
                    rows[k]["equal whole"] += 1
                elif v.get("prefix_only_fields") and not (v["only_system"] or v["only_gnomon"] or v["first_page_order_differs"] or v["totals_differ"]):
                    rows[k]["prefix only"] += 1
                else:
                    rows[k]["DIFFER"] += 1
            elif g.get("state") != "returned":
                rows[k]["Gnomon: " + g.get("state", "?").replace("not checked: Gnomon ", "")] += 1
            else:
                rows[k][v.get("state", "not reached").replace("not checked: ", "")] += 1
cols = ["selected", "equal whole", "DIFFER", "prefix only", "over the limit", "failed", "not reached"]
extra = sorted({c for v in rows.values() for c in v} - set(cols))
print("| point | query | system | " + " | ".join(cols + extra) + " | Gnomon whole answer | Gnomon dedicated vs whole: checked, differ |")
print("|---|---|---|" + "---:|" * len(cols + extra) + "---|---|")
for k in sorted(rows):
    v = rows[k]
    print(f"| {k[0]} | {k[1]} | {k[2]} | " + " | ".join(str(v.get(c, 0)) for c in cols + extra) +
          f" | {'; '.join(sorted(meth[k[:2]]))} | {dvw[k[:2]][0]}, {dvw[k[:2]][1]} |")
print()
print("Not run, recorded total above the row limit (20,000,000):")
for f in sorted(C.glob("closure-*.json")):
    d = json.load(open(f))
    for t in d["too_large_not_run"]:
        print(f"- {d['point']} {t['query']}#{t['instance']}: {t['recorded_total']:,} rows")
