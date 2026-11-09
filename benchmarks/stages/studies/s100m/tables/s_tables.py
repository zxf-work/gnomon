#!/usr/bin/env python3
"""s_tables.py --tag TAG --timed DIR [DIR ...] [--out FILE]: per-family and per-class Gnomon / DuckDB (SQL) 64 and Gnomon / PostgreSQL, from timed records only.
Values as x_summary.py: per instance the mean over rotations of each rotation's median timed call (p50, end-to-end); "> 10 s" when censored in either rotation
or excluded at the answer check above the ceiling; a Gnomon refusal has no time and enters no ratio. Several DIRs: their instances are
joined (different instance ids, other jobs on other nodes; labelled by the caller). Per family and per class: median of the per-instance ratios,
[instances with a ratio; other > 10 s where Gnomon answered], Gnomon faster on k of the instances with a ratio. Descriptive, no verdict words."""
import argparse
import statistics as st
import sys
from collections import defaultdict
from pathlib import Path

sys.path[:0] = [__import__("os").environ.get("GNOMON_BENCH_ROOT", ".") + "/studies/s100m-a/src/benchmarks/stages/studies/t10/tools",
                __import__("os").environ.get("GNOMON_BENCH_ROOT", ".") + "/studies/s100m-a/src/scripts", __import__("os").environ.get("GNOMON_BENCH_ROOT", ".") + "/studies/s100m-a/src"]
import x_summary as XS          # noqa: E402
import extcmp as X              # noqa: E402

FAM = {}
for f, qs in X.FAMILIES.items():
    for q in qs:
        FAM[q] = f
for f, qs in X.NATIVE_ONLY_BY_FAMILY.items():
    for q in qs:
        FAM.setdefault(q, f)
OTH = (("duckdbsql", "DuckDB (SQL) 64"), ("postgres", "PostgreSQL"))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tag", required=True); ap.add_argument("--timed", nargs="+", type=Path, required=True); ap.add_argument("--out", type=Path)
    a = ap.parse_args()
    S = defaultdict(dict)
    for d in a.timed:
        for s, m in XS.read_root(d / "reg" / a.tag).items():
            for k, e in m.items():
                S[s][k] = e
    nat = S.get("native", {})
    groups = defaultdict(lambda: defaultdict(lambda: {"r": [], "oc": 0, "gc": 0, "ref": 0, "n": 0}))
    perq = defaultdict(lambda: defaultdict(lambda: {"r": [], "oc": 0, "gc": 0, "ref": 0, "n": 0}))
    for k, e in nat.items():
        q = e["query"]; g = XS.value(e)
        for s, _ in OTH:
            o = S.get(s, {}).get(k)
            ov = XS.value(o) if o else None
            for key, tab in ((("class", XS.REG.get(q, "?")), groups), (("family", FAM.get(q, "?")), groups), (q, perq)):
                c = tab[key][s]; c["n"] += 1
                if g == XS.REF:
                    c["ref"] += 1
                elif XS.num(g) and XS.num(ov):
                    c["r"].append(g / ov)
                elif XS.num(g) and ov == XS.CENS:
                    c["oc"] += 1
                elif g == XS.CENS and XS.num(ov):
                    c["gc"] += 1

    def cell(c):
        if not c["n"]:
            return "-"
        m = f"{st.median(c['r']):.3g}" if c["r"] else "-"
        f = sum(1 for x in c["r"] if x < 1)
        x = f"{m} [{len(c['r'])}; {c['oc']}]"
        if c["r"]:
            x += f", Gnomon faster on {f} of {len(c['r'])}"
        if c["gc"]:
            x += f"; Gnomon > 10 s, other answers {c['gc']}"
        if c["ref"]:
            x += f"; refused by Gnomon {c['ref']}"
        return x
    L = [f"# {a.tag}: Gnomon / DuckDB (SQL) 64 and Gnomon / PostgreSQL", "", "Records: " + ", ".join(str(d) for d in a.timed), "",
         "Cell: median of per-instance ratios [instances with a ratio; other > 10 s where Gnomon answered]. End-to-end p50, mean of 2 rotations.", ""]
    for kind, order in (("class", "TGB"), ("family", "PTSWDNRMB")):
        L += [f"## Per {kind}", "", f"| {kind} | ÷ DuckDB (SQL) 64 | ÷ PostgreSQL |", "|---|---|---|"]
        for x in order:
            if (kind, x) in groups:
                L.append(f"| {x} | " + " | ".join(cell(groups[(kind, x)][s]) for s, _ in OTH) + " |")
        L.append("")
    L += ["## Per query", "", "| class | family | query | ÷ DuckDB (SQL) 64 | ÷ PostgreSQL |", "|---|---|---|---|---|"]
    for q in sorted(perq, key=lambda q: (XS.REG.get(q, "?"), FAM.get(q, "?"), q)):
        L.append(f"| {XS.REG.get(q, '?')} | {FAM.get(q, '?')} | {q} | " + " | ".join(cell(perq[q][s]) for s, _ in OTH) + " |")
    txt = "\n".join(L) + "\n"
    if a.out:
        a.out.write_text(txt)
    print(txt)
    return 0


if __name__ == "__main__":
    sys.exit(main())
