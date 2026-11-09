#!/usr/bin/env python3
"""rl_w2stat.py <timed dir> [...]: the W2 per-query statistic, computed as w2_summary.py computes the printed tables. Instance value = mean of the two rotations' p50s. A cell censored in either rotation, or excluded at the answer check (above 10 s, or a differing answer), has no value and is counted. Cell value = median over the instances the system answered. Printed ratio form = Gnomon's cell / the form's cell (unpaired). Also the paired count: instances both answered, and Gnomon lower on how many. Gnomon's figure printed in the side-by-side record is passed by --printed as JSON {point|query: value}."""
import argparse
import glob
import json
import statistics as st
from collections import Counter
from pathlib import Path


def read(p, s):
    v, why = {}, Counter()
    rows = {}
    for r in ("rot1", "rot2"):
        for f in glob.glob(f"{p}/{r}/*-{s}.json"):
            for c in json.load(open(f)).get("results", []):
                rows.setdefault((c["query"], c.get("instance")), {})[r] = c
    for k, d in rows.items():
        ok = [c for c in d.values() if c.get("ok")]
        if len(d) == 2 and len(ok) == 2:
            v[k] = sum(c["p50_ms"] for c in ok) / 2
        else:
            e = next((c.get("error") or "" for c in d.values() if not c.get("ok")), "one rotation missing")
            why[(k[0], "excluded: " + e.split("excluded: ")[-1][:30] if "excluded" in e else "censored in a rotation" if (">" in e or "ceil" in e.lower()) else e[:40])] += 1
    return v, why


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("dirs", nargs="+")
    ap.add_argument("--printed")
    a = ap.parse_args()
    pr = json.loads(Path(a.printed).read_text()) if a.printed else {}
    L = ["# W2 statistic, as the printed tables compute it (w2_summary.py rules)", "", "| point | query | n | Gnomon, this job | Gnomon, printed | first: answered, median, Gnomon / first | fuller: answered, median, Gnomon / fuller | first paired: both, Gnomon lower | fuller paired: both, Gnomon lower |", "|---|---|---:|---:|---:|---|---|---|---|"]
    W = []
    for d in a.dirs:
        for pt in sorted(glob.glob(d + "/w2/*/")):
            pt = pt.rstrip("/")
            tag = pt.split("/")[-1]
            g, gw = read(pt, "native")
            r, rw = read(pt, "raphtory")
            e, ew = read(pt, "raphtoryeq")
            for q in sorted({k[0] for k in g}):
                gq = {k: v for k, v in g.items() if k[0] == q}
                rq = {k: v for k, v in r.items() if k[0] == q}
                eq = {k: v for k, v in e.items() if k[0] == q}
                gm = st.median(gq.values())
                def col(m):
                    return f"{len(m)}, {round(st.median(m.values()), 3)}, {round(gm / st.median(m.values()), 3)}" if m else "0"
                def pair(m):
                    b = [k for k in gq if k in m]
                    return f"{len(b)}, {sum(gq[k] < m[k] for k in b)}"
                n = len(gq) + sum(v for k, v in gw.items() if k[0] == q)
                L.append(f"| {tag} | {q} | {n} | {round(gm, 3)} | {pr.get(tag + '|' + q, '')} | {col(rq)} | {col(eq)} | {pair(rq)} | {pair(eq)} |")
            for nm, w in (("Gnomon", gw), ("first", rw), ("fuller", ew)):
                for (q, why), c in sorted(w.items()):
                    W.append(f"- {tag} {q} {nm}: {c} without a value, {why}")
    L += ["", "Cells without a value:"] + (W or ["- none"])
    print("\n".join(L))


if __name__ == "__main__":
    main()
