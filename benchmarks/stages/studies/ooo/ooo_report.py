#!/usr/bin/env python3
"""Out-of-order logs: the tables. ooo_report.py --res <results dir> --tags base,tag,... --out report.md (works from the records; re-runnable on the login node).
For each instance and log: the median over rotations of the window p50 with block checks on (the paired unset window) and off; off / on is the median of the per-pair ratios.
A/A is the same with the unset engine on both sides. A cell at the ceiling or refused has no time and enters no ratio. Descriptive: no verdict."""
import argparse
import glob
import json
import os
import statistics as st
import sys


def windows(a, tag, pair):
    out = {}
    for f in sorted(glob.glob(os.path.join(a, tag, "rot*", "*", f"{pair}.json"))):
        rot = f.split(os.sep)[-3]
        for c in json.load(open(f)).get("results", []):
            out.setdefault((c["query"], c.get("instance"), c.get("stratum")), {})[rot] = c
    return out


def p50(c):
    return c.get("p50_ms") if c and c.get("ok") else None


def fmt(x, n=3):
    return "-" if x is None else f"{x:.{n}g}" if x < 1000 else f"{x:.0f}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--res", required=True)
    ap.add_argument("--tags", required=True)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    A = os.path.join(a.res, "ooo")
    tags = a.tags.split(",")
    base = tags[0]
    W = {t: {"on": windows(A, t, "unset~range_visibility_off"), "off": windows(A, t, "range_visibility_off"),
             "a": windows(A, t, "unset~aa"), "b": windows(A, t, "aa")} for t in tags}
    keys = sorted({k for t in tags for k in W[t]["on"]}, key=lambda k: (str(k[2]), k[0], str(k[1])))
    def verify(t):
        out = {}
        for f in glob.glob(os.path.join(a.res, t, "verify-native*.json")):
            for c in json.load(open(f)).get("results", []):
                out[(c["query"], c.get("instance"))] = c
        return out
    V = {t: verify(t) for t in tags}
    L = ["# Out-of-order logs on C4", "", "Median of window p50 in ms over rotations. off / on: median of paired ratios. log / original: same configuration. A/A: unset against unset, same log. "
         "`c` is a censored or refused cell: no time and no ratio. Where the answer differs from the original log's (vid and belief times left out) a log / original ratio compares different answers. Descriptive, no verdict.", ""]
    row = {}
    for t in tags:
        L += [f"## {t}", "", "| query | stratum | rows | on ms | off ms | off / on | A/A | on, log / original | off, log / original | answer as on the original log | not answered |", "|---|---|---:|---:|---:|---:|---:|---:|---:|---|---|"]
        for k in keys:
            on, off = W[t]["on"].get(k, {}), W[t]["off"].get(k, {})
            ao, bo = W[t]["a"].get(k, {}), W[t]["b"].get(k, {})
            med = lambda d: (st.median([v for v in map(p50, d.values()) if v is not None]) if any(p50(c) is not None for c in d.values()) else None)
            mo, mf = med(on), med(off)
            prs = [p50(off[r]) / p50(on[r]) for r in on if r in off and p50(on[r]) and p50(off[r]) is not None]
            aa = [p50(bo[r]) / p50(ao[r]) for r in ao if r in bo and p50(ao[r]) and p50(bo[r]) is not None]
            bon, boff = W[base]["on"].get(k, {}), W[base]["off"].get(k, {})
            bmo, bmf = med(bon), med(boff)
            missing = sum(1 for d in (on, off) for c in d.values() if not c.get("ok"))
            rows = next((c.get("rows") for c in on.values() if c.get("ok")), None)
            vb, vt = V[base].get((k[0], k[1]), {}), V[t].get((k[0], k[1]), {})
            same = "yes" if vb.get("proj_hash") and vb.get("proj_hash") == vt.get("proj_hash") else "no"
            r = {"on_ms": mo, "off_ms": mf, "off_on": st.median(prs) if prs else None, "aa": st.median(aa) if aa else None,
                 "on_vs_orig": mo / bmo if mo and bmo else None, "off_vs_orig": mf / bmf if mf and bmf else None, "not_answered": missing, "rows": rows, "same_answer_as_original": same}
            row[f"{t}|{k[0]}|{k[1]}"] = r
            L.append(f"| {k[0]} | {'T_mid' if k[2] and 'inside' in k[2] else 'registry'} | {rows} | {fmt(mo)} | {fmt(mf)} | {fmt(r['off_on'])} | {fmt(r['aa'])} | {fmt(r['on_vs_orig'])} | {fmt(r['off_vs_orig'])} | {same} | {'c' * missing} |")
        L.append("")
    with open(a.out, "w") as f:
        f.write("\n".join(L) + "\n")
    with open(a.out.rsplit(".", 1)[0] + ".json", "w") as f:
        json.dump(row, f, indent=1)
    print(f"{a.out}: {len(keys)} instances x {len(tags)} logs")
    return 0


if __name__ == "__main__":
    sys.exit(main())
