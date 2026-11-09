#!/usr/bin/env python3
"""rl_spread.py <timed dir> [<timed dir> ...]: rotation spread, max(p50 by rotation) / min(p50 by rotation) per cell, per point and system, and the largest gap between the rotL rotations per form. A later directory's point replaces an earlier one's."""
import glob
import json
import statistics as st
import sys
from collections import defaultdict

pts = {}
for d in sys.argv[1:]:
    for p in glob.glob(d + "/*/*/"):
        p = p.rstrip("/")
        pts["/".join(p.split("/")[-2:])] = p
L = ["## Rotation spread: max / min of a cell's two rotation p50s", "", "| point | system | cells with both rotations | median spread | largest spread | largest at | cells with one rotation only |", "|---|---|---:|---:|---:|---|---:|"]
lab = defaultdict(list)
for pt, p in sorted(pts.items()):
    for s in ("native", "raphtory", "raphtoryeq"):
        c = defaultdict(dict)
        for f in glob.glob(f"{p}/rot[12]/*-{s}.json"):
            r = f.split("/")[-2]
            for x in json.load(open(f)).get("results", []):
                if x.get("ok"):
                    c[f"{x['query']}#{x.get('instance')}"][r] = x["p50_ms"]
        sp = {k: max(v.values()) / min(v.values()) for k, v in c.items() if len(v) == 2}
        one = sum(1 for v in c.values() if len(v) == 1)
        if sp or one:
            kmax = max(sp, key=sp.get) if sp else ""
            L.append(f"| {pt} | {s} | {len(sp)} | {round(st.median(sp.values()), 3) if sp else ''} | {round(max(sp.values()), 3) if sp else ''} | {kmax} | {one} |")
        c2 = defaultdict(dict)
        for f in glob.glob(f"{p}/rotL[12]/*-{s}.json"):
            r = f.split("/")[-2]
            for x in json.load(open(f)).get("results", []):
                if x.get("ok"):
                    c2[f"{x['query']}#{x.get('instance')}"][r] = x["p50_ms"]
        for k, v in c2.items():
            if len(v) == 2:
                lab[s].append((max(v.values()) / min(v.values()), pt, k, v))
L += ["", "## Label \"different answer\": largest gap between the two rotations, per form", "", "| form | cells with both rotations | largest gap | at |", "|---|---:|---:|---|"]
for s, v in lab.items():
    m = max(v, key=lambda x: x[0])
    L.append(f"| {s} | {len(v)} | {round(m[0], 3)} | {m[1]} {m[2]} {m[3]} |")
print("\n".join(L))
