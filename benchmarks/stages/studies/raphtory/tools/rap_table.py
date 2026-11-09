#!/usr/bin/env python3
"""rap_table.py --res DIR --ref E --out table.md: Gnomon and Raphtory per point and query, as recorded. A cell's figure is the median of the rotations' p50s; a ratio is Gnomon's over Raphtory's on the same
instance. Descriptive: counts and medians, no verdict words."""
import argparse
import glob
import json
import statistics as st
from pathlib import Path


def cells(p, sysname):
    out = {}
    for f in sorted(glob.glob(f"{p}/rot[12]/*-{sysname}.json")):
        r = f.split("/")[-2]
        for c in json.load(open(f)).get("results", []):
            k = f"{c['query']}#{c.get('instance')}"
            o = out.setdefault(k, {"query": c["query"], "instance": c.get("instance"), "stratum": c.get("stratum"), "p50": {}, "state": {}})
            o["p50"][r] = c.get("p50_ms") if c.get("ok") else None
            o["state"][r] = "ok" if c.get("ok") else (c.get("error") or "not ok")[:70]
    return out


def med(o):
    v = [x for x in o["p50"].values() if x is not None]
    return st.median(v) if v else None


def main():
    ap = argparse.ArgumentParser()
    for k in ("res", "ref", "out"):
        ap.add_argument("--" + k, required=True)
    a = ap.parse_args()
    L = ["# Raphtory and Gnomon, one job", "", "Times in ms: median of the two rotations' p50s, the parameter-file protocol (window prime, 2 warm-ups + 10, 1 + 3 above 1 s, 10 s ceiling). Ratio = Gnomon / Raphtory. A cell the answer check excluded has no time.", ""]
    for wl in ("w1", "w2"):
        for pt in sorted(glob.glob(f"{a.res}/{wl}/K*")):
            tag = pt.split("/")[-1]
            g, r = cells(pt, "native"), cells(pt, "raphtory")
            L += [f"## {wl} {tag}", ""]
            byq = {}
            for k in sorted(set(g) | set(r)):
                byq.setdefault((g.get(k) or r.get(k))["query"], []).append(k)
            if wl == "w1":
                L += ["| query | Gnomon p50 by rotation | Gnomon | Raphtory p50 by rotation | Raphtory | ratio | Raphtory state |", "|---|---|---:|---|---:|---:|---|"]
                for q, ks in byq.items():
                    for k in ks:
                        a1, b1 = g.get(k), r.get(k)
                        ga, ra = med(a1) if a1 else None, med(b1) if b1 else None
                        L.append(f"| {q} | {a1['p50'] if a1 else ''} | {ga} | {b1['p50'] if b1 else ''} | {ra} | {round(ga / ra, 3) if ga and ra else ''} | {sorted(set(b1['state'].values())) if b1 else ''} |")
            else:
                L += ["| query | class | instances | both answered | Gnomon median | Raphtory median | median ratio | ratio min, max | Gnomon lower | Raphtory cells without a time |", "|---|---|---:|---:|---:|---:|---:|---|---:|---:|"]
                for q, ks in byq.items():
                    for cl in ("hub", "other"):
                        sub = [k for k in ks if ((g.get(k) or r.get(k))["stratum"] == "hub") == (cl == "hub")]
                        if not sub:
                            continue
                        both = [(med(g[k]), med(r[k])) for k in sub if k in g and k in r and med(g[k]) and med(r[k])]
                        rat = [x / y for x, y in both]
                        L.append(f"| {q} | {cl} | {len(sub)} | {len(both)} | {round(st.median(x for x, _ in both), 3) if both else ''} | {round(st.median(y for _, y in both), 3) if both else ''} | {round(st.median(rat), 3) if rat else ''} | {(round(min(rat), 3), round(max(rat), 3)) if rat else ''} | {sum(1 for x in rat if x < 1)} | {sum(1 for k in sub if k in r and med(r[k]) is None)} |")
            L.append("")
            lab = {}
            for f in sorted(glob.glob(f"{pt}/rotL[12]/*-raphtory.json")):
                rr = f.split("/")[-2]
                for c in json.load(open(f)).get("results", []):
                    if not c.get("excluded"):
                        lab.setdefault(f"{c['query']}#{c.get('instance')}", {})[rr] = (c.get("p50_ms") if c.get("ok") else (c.get("error") or "not ok")[:50])
            if lab:
                L += [f"Cells timed under the label \"different answer\" at {tag}: Raphtory's answer differs from Gnomon's, so these two columns are not the same question. No ratio, win count or summary reads them.", "", "| cell | stratum | Raphtory p50 by rotation, different answer | Gnomon p50 by rotation |", "|---|---|---|---|"]
                L += [f"| {k} | {(g.get(k) or {}).get('stratum')} | {v} | {(g.get(k) or {}).get('p50')} |" for k, v in sorted(lab.items())]
                L.append("")
            mism = 0
            for sysn, m in (("native", g), ("raphtory", r)):
                vf = Path(pt, f"verify-{sysn}.json")
                if vf.exists():
                    vh = {f"{c['query']}#{c.get('instance')}": c.get("hash") for c in json.loads(vf.read_text()).get("results", [])}
                    for f in glob.glob(f"{pt}/rot[12]/*-{sysn}.json"):
                        for c in json.load(open(f)).get("results", []):
                            k = f"{c['query']}#{c.get('instance')}"
                            if c.get("ok") and vh.get(k) and vh[k] != c.get("hash"):
                                mism += 1
            L += [f"Timed answers whose hash differs from the same system's answer check: {mism}.", ""]
    Path(a.out).write_text("\n".join(L) + "\n")
    print("\n".join(L))


if __name__ == "__main__":
    main()
