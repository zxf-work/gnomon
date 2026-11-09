#!/usr/bin/env python3
"""W3 component study. w3abl_report.py pair --dir D --arm A [--ref DIR]: exit 7 if a cell both windows answered has a different hash, or an unset answer differs from C4's recorded timed block; also checks the block's designs say the switch took effect.
w3abl_report.py table --res R --out report.md: per query and setting, the configuration / unset ratio per instance, median over instances; censored or refused instances listed, no ratio. Descriptive."""
import argparse
import glob
import json
import os
import statistics as st
import sys

KEYS = {"range_visibility_off": ("range_visibility", False)}


def cells(p):
    if not os.path.exists(p):
        return {}
    return {(c["query"], c.get("setting"), c.get("instance")): c for c in json.load(open(p)).get("results", [])}


def pair(a):
    un, ar = cells(os.path.join(a.dir, "unset.json")), cells(os.path.join(a.dir, a.arm + ".json"))
    bad = [k for k, c in un.items() if c.get("ok") and ar.get(k, {}).get("ok") and c.get("hash") != ar[k].get("hash")]
    if a.ref:
        rec = {}
        for f in glob.glob(os.path.join(a.ref, "**", "*-gnomon.json"), recursive=True):
            rec.update({k: c for k, c in cells(f).items() if c.get("ok")})
        bad += [("recorded", k) for k, c in un.items() if c.get("ok") and k in rec and rec[k].get("hash") != c.get("hash")]
    for b in bad[:8]:
        print("ANSWER DIFFERENCE", a.dir, b)
    return 7 if bad else 0


def table(a):
    L = ["# W3 component study on C4, Gnomon only", "", "Ratio = configuration / its back-to-back unset window on the same store, end-to-end p50, per instance; the row is the median over instances both answered. Descriptive: no verdict. "
         "A censored or refused instance has no ratio and is counted.", "",
         "| configuration | query | setting | instances | both answered | median ratio | range | unset median ms | configuration median ms | censored, configuration | censored, unset | refused or error | designs |",
         "|---|---|---|---:|---:|---:|---|---:|---:|---:|---:|---:|---|"]
    rows = {}
    for arm in sorted(os.listdir(a.res)):
        ad = os.path.join(a.res, arm)
        if not os.path.isdir(ad):
            continue
        for qd in sorted(os.listdir(ad)):
            U, A = cells(os.path.join(ad, qd, "unset.json")), cells(os.path.join(ad, qd, arm + ".json"))
            des = ""
            p = os.path.join(ad, qd, arm + ".json")
            if os.path.exists(p):
                d = json.load(open(p)).get("settings", {}).get("designs")
                u = json.load(open(os.path.join(ad, qd, "unset.json"))).get("settings", {}).get("designs") if os.path.exists(os.path.join(ad, qd, "unset.json")) else None
                if isinstance(d, dict) and isinstance(u, dict):
                    des = ",".join(k for k in sorted(d) if d[k] != u.get(k)) or "none differ"
            for s in sorted({k[1] for k in U} | {k[1] for k in A}):
                ks = sorted({k for k in U if k[1] == s} | {k for k in A if k[1] == s}, key=str)
                rat, un, ar = [], [], []
                cA = cU = oth = 0
                for k in ks:
                    x, y = U.get(k, {}), A.get(k, {})
                    if x.get("ok"):
                        un.append(x["p50_ms"])
                    if y.get("ok"):
                        ar.append(y["p50_ms"])
                    if x.get("ok") and y.get("ok") and x["p50_ms"]:
                        rat.append(y["p50_ms"] / x["p50_ms"])
                    for side, c in (("A", y), ("U", x)):
                        if c and not c.get("ok"):
                            if c.get("censored") or c.get("aborted"):
                                cA += side == "A"
                                cU += side == "U"
                            else:
                                oth += 1
                f = lambda v: "-" if not v else f"{st.median(v):.3g}"
                r = {"n": len(ks), "both": len(rat), "median": st.median(rat) if rat else None, "range": [min(rat), max(rat)] if rat else None, "unset_ms": st.median(un) if un else None, "arm_ms": st.median(ar) if ar else None, "censored_arm": cA, "censored_unset": cU, "other": oth, "designs": des}
                rows[f"{arm}|{qd}|{s}"] = r
                L.append(f"| {arm} | {qd} | {s} | {len(ks)} | {len(rat)} | {f(rat)} | {'-' if not rat else f'{min(rat):.3g}-{max(rat):.3g}'} | {f(un)} | {f(ar)} | {cA} | {cU} | {oth} | {des} |")
    with open(a.out, "w") as f:
        f.write("\n".join(L) + "\n")
    json.dump(rows, open(a.out.rsplit(".", 1)[0] + ".json", "w"), indent=1)
    return 0


def pairs(res):
    """{(arm, query, setting): {instance: (ratio or None, unset ms, arm ms, unset ok, arm ok)}} from one job's records"""
    out = {}
    for arm in sorted(os.listdir(res)):
        ad = os.path.join(res, arm)
        if not os.path.isdir(ad):
            continue
        for qd in sorted(os.listdir(ad)):
            U, A = cells(os.path.join(ad, qd, "unset.json")), cells(os.path.join(ad, qd, arm + ".json"))
            for k in set(U) | set(A):
                x, y = U.get(k, {}), A.get(k, {})
                ratio = y["p50_ms"] / x["p50_ms"] if x.get("ok") and y.get("ok") and x["p50_ms"] else None
                out.setdefault((arm, qd, k[1]), {})[k[2]] = (ratio, x.get("p50_ms") if x.get("ok") else None, y.get("p50_ms") if y.get("ok") else None, bool(x.get("ok")), bool(y.get("ok")))
    return out


def pct(v, p):
    v = sorted(v)
    i = (len(v) - 1) * p / 100.0
    lo = int(i)
    hi = min(lo + 1, len(v) - 1)
    return v[lo] + (v[hi] - v[lo]) * (i - lo)


def combine(a):
    C = {}
    for r in a.res:
        for k, d in pairs(r).items():
            C.setdefault(k, {}).update(d)
    aa = [x[0] for k, d in C.items() if k[0] == "aa" for x in d.values() if x[0] is not None]
    lo, hi = (pct(aa, 5), pct(aa, 95)) if aa else (None, None)
    f = lambda x: "-" if x is None else f"{x:.3g}"
    cell = {(k[1], k[2]): [x[0] for x in d.values() if x[0] is not None] for k, d in C.items() if k[0] == "aa"}
    L = ["# W3 SF10 component study on C4, combined: " + ", ".join(os.path.basename(x.rstrip("/")) for x in a.res), "",
         f"A/A, unset against unset, same pairing, instances 0 to 3: n = {len(aa)} instance ratios, 5th to 95th percentile {f(lo)} to {f(hi)}. Ratios outside that band are marked `out`; the cell's own A/A range is beside it. Descriptive, no verdict. A censored instance has no ratio.", "",
         "| configuration | query | setting | instances | both answered | median ratio | range | A/A range, this query and setting | median against the overall A/A band | median against this cell's A/A range | censored, configuration | censored, unset |",
         "|---|---|---|---:|---:|---:|---|---|---|---|---:|---:|"]
    rows = {}
    for k in sorted(C, key=lambda k: (k[0] != "aa", k)):
        d = C[k]
        v = [x[0] for x in d.values() if x[0] is not None]
        med = st.median(v) if v else None
        ca = cell.get((k[1], k[2]), [])
        out_all = "-" if med is None or lo is None else ("out" if med < lo or med > hi else "in")
        out_cell = "-" if med is None or not ca else ("out" if med < min(ca) or med > max(ca) else "in")
        cA = sum(1 for x in d.values() if not x[4])
        cU = sum(1 for x in d.values() if not x[3])
        rows["|".join(k)] = {"n": len(d), "both": len(v), "median": med, "out_overall_band": out_all, "out_cell_range": out_cell, "censored_arm": cA, "censored_unset": cU}
        L.append(f"| {k[0]} | {k[1]} | {k[2]} | {len(d)} | {len(v)} | {f(med)} | {'-' if not v else f(min(v)) + '-' + f(max(v))} | {'-' if not ca else f(min(ca)) + '-' + f(max(ca))} | {out_all} | {out_cell} | {cA} | {cU} |")
    L.append("")
    for arm in sorted({k[0] for k in C}):
        v = [r["median"] for k, r in rows.items() if k.startswith(arm + "|") and r["median"] is not None]
        o = [k for k, r in rows.items() if k.startswith(arm + "|") and r["out_overall_band"] == "out"]
        L.append(f"- {arm}: {len(v)} cells with a ratio, median {f(st.median(v) if v else None)}, {len(o)} outside the overall A/A band; below it {sum(1 for k in o if rows[k]['median'] < lo)}, above it {sum(1 for k in o if rows[k]['median'] > hi)}; instances censored with it on {sum(r['censored_arm'] for k, r in rows.items() if k.startswith(arm + '|'))}, unset {sum(r['censored_unset'] for k, r in rows.items() if k.startswith(arm + '|'))}.")
    with open(a.out, "w") as fh:
        fh.write("\n".join(L) + "\n")
    json.dump({"aa_band_5_95": [lo, hi], "aa_n": len(aa), "rows": rows}, open(a.out.rsplit(".", 1)[0] + ".json", "w"), indent=1)
    return 0


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("pair")
    p.add_argument("--dir", required=True)
    p.add_argument("--arm", required=True)
    p.add_argument("--ref", default="")
    t = sub.add_parser("table")
    t.add_argument("--res", required=True)
    t.add_argument("--out", required=True)
    k = sub.add_parser("combine")
    k.add_argument("--res", action="append", required=True)
    k.add_argument("--out", required=True)
    a = ap.parse_args()
    return {"pair": pair, "table": table, "combine": combine}[a.cmd](a)


if __name__ == "__main__":
    sys.exit(main())
