#!/usr/bin/env python3
"""rl_table.py --res R --out table.md: Gnomon against Raphtory's two forms, per point, query, class and stratum. Median of the rotations' p50s; ratio = Gnomon / the form, on one instance; descriptive. A differing cell has its own record (rotL<r>) and no ratio."""
import argparse
import glob
import json
import statistics as st
from pathlib import Path


def cells(p, s):
    out = {}
    for f in sorted(glob.glob(f"{p}/rot[12]/*-{s}.json")):
        r = f.split("/")[-2]
        for c in json.load(open(f)).get("results", []):
            k = f"{c['query']}#{c.get('instance')}"
            o = out.setdefault(k, {"query": c["query"], "stratum": c.get("stratum"), "p50": {}, "state": {}})
            o["p50"][r] = c.get("p50_ms") if c.get("ok") else None
            o["state"][r] = "ok" if c.get("ok") else (c.get("error") or "")[:60]
    return out


def med(o):
    v = [x for x in o["p50"].values() if x is not None] if o else []
    return st.median(v) if v else None


def summ(g, r, ks):
    both = [(med(g[k]), med(r[k])) for k in ks if k in g and k in r and med(g[k]) and med(r[k])]
    if not both:
        return f"0 | | | | | 0 | {sum(1 for k in ks if k in r and med(r[k]) is None)}"
    rat = [x / y for x, y in both]
    return f"{len(both)} | {round(st.median(x for x, _ in both), 3)} | {round(st.median(y for _, y in both), 3)} | {round(st.median(rat), 3)} | {round(min(rat), 3)}, {round(max(rat), 3)} | {sum(1 for x in rat if x < 1)} | {sum(1 for k in ks if k in r and med(r[k]) is None)}"


def main():
    ap = argparse.ArgumentParser()
    for k in ("res", "out"):
        ap.add_argument("--" + k, required=True)
    a = ap.parse_args()
    L = ["# Gnomon and Raphtory's two forms, one job", "", "raphtory = the first form (nodes and hops at a page of 100; gained and lost). raphtoryeq = the fuller form (nodes, hops and induced edges at the call's own limit; gained, lost and the degree series). Ratio = Gnomon / the form. Times in ms. Edge rows are (src, dst, layer): Raphtory's graph has no eid, vid or version interval. Figures are per stratum; a differing cell has no time here.", ""]
    for pt in sorted(glob.glob(f"{a.res}/*/*/")):
        pt = pt.rstrip("/")
        wl, tag = pt.split("/")[-2:]
        if not glob.glob(pt + "/rot1"):
            continue
        g, r1, r2 = cells(pt, "native"), cells(pt, "raphtory"), cells(pt, "raphtoryeq")
        L += [f"## {wl} {tag}", "", "| query | stratum | form | both answered | Gnomon median | form median | median ratio | ratio min, max | Gnomon lower | form cells without a time |", "|---|---|---|---:|---:|---:|---:|---|---:|---:|"]
        groups = {}
        for k, o in g.items():
            groups.setdefault((o["query"], o["stratum"]), []).append(k)
        for (q, s), ks in sorted(groups.items(), key=lambda x: (x[0][0], str(x[0][1]))):
            for nm, rr in (("raphtory", r1), ("raphtoryeq", r2)):
                L.append(f"| {q} | {s} | {nm} | {summ(g, rr, ks)} |")
        lab = {}
        for s_ in ("raphtory", "raphtoryeq"):
            for f in sorted(glob.glob(f"{pt}/rotL[12]/*-{s_}.json")):
                for c in json.load(open(f)).get("results", []):
                    if not c.get("excluded"):
                        lab.setdefault((s_, f"{c['query']}#{c.get('instance')}", c.get("stratum")), {})[f.split("/")[-2]] = c.get("p50_ms") if c.get("ok") else (c.get("error") or "")[:40]
        if lab:
            L += ["", "Label \"different answer\": no ratio, count or summary reads these.", "", "| form | cell | stratum | p50 by rotation |", "|---|---|---|---|"]
            L += [f"| {a_} | {b} | {c} | {v} |" for (a_, b, c), v in sorted(lab.items(), key=str)]
        L.append("")
    Path(a.out).write_text("\n".join(L) + "\n")
    print("\n".join(L)[:6000])


if __name__ == "__main__":
    main()
