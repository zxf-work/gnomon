#!/usr/bin/env python3
"""mp_table.py --res R --out table.md [--duckdb-w1 D1] [--duckdb-w2 D2] [--json out.json]: dedicated operator (native) against
the match path (nativematch), one job, one node; descriptive, no bands, no verdicts.

Rules (the W1 and W2 records' own, w2_summary.py read_root/value): instance value = mean over the two rotations of each
rotation's median timed call (p50, end-to-end, ms); a cell censored (> 10 s), refused or excluded in either rotation has
no value and is counted. Per query:
  * dedicated, match = median over the instances that system answered;
  * match / dedicated per instance = the two instance values' ratio, on instances both answered (same node, same job);
    the query's figure is the median of those ratios, with the smallest and largest beside it;
  * ratio of medians = match median / dedicated median (the printed W2 statistic's kind of figure: per-query medians
    over instances, compared as medians);
  * DuckDB (SQL) from its record (another job and another node): instance value by the same rule, the better of 64 and
    32 threads per instance (w1_summary/w2_summary), query value = median over the instances it answered; match / DuckDB
    as ratio of medians and as the median of per-instance ratios;
  * engine time from the engine-time pass (rotation E, 2 warm-ups + 3 timed calls): per instance the median engine
    time (receive -> result ready: front = validation, admission, plan; exec = native engine calls), delivery = e2e -
    engine; per query the median over instances;
  * rotation spread: per instance rotation 2 / rotation 1 of the p50s; per query the median and the extremes.
"""
from __future__ import annotations

import argparse
import json
import statistics as st
import sys
from pathlib import Path

HERE = Path(__file__).resolve()
sys.path.insert(0, str(HERE.parents[3] / "w2" / "tools"))
import w2_summary as W  # noqa: E402  (read_root, value: the W2 records' rules)

QUERIES = ("snap.hop2", "nbr.evolution", "b.snap.hop2@now", "b.snap.hop2@T", "series.count", "series.count.w10",
           "agg.rel_bucket", "agg.rel_bucket.w10", "hist.single", "hist.asof", "paths.k", "diff.global", "b.diff@ctl",
           "diff.belief", "b.diff", "b.hist", "b.hist@now")
#: compared without these fields of the dedicated answer (match cannot return them): their timed hashes differ by construction
FIELDS_EXCLUDED = {"hist.single": "source, provenance_ref", "hist.asof": "source, provenance_ref"}
ROOTS = (("p1", "t32p1"), ("sp", "t32sp"), ("reg", "t32reg"))
NAME = {"native": "dedicated", "nativematch": "match"}


def f3(v):
    if v is None:
        return "–"
    if isinstance(v, str):
        return v
    return f"{v:.3g}" if abs(v) < 100 else f"{v:,.0f}"


def med(xs):
    xs = [x for x in xs if x is not None]
    return st.median(xs) if xs else None


def duck_values(root: Path | None, tag: str, keys: set) -> dict:
    """{(query, instance): value} for DuckDB (SQL), better of 64 (p1|sp) and 32 threads (t32p1|t32sp) per instance,
    over every timed-* job directory of a stage's results root."""
    out = {}
    roots = [r for r in (root if isinstance(root, list) else [root]) if r is not None and r.is_dir()]
    # a root is one timed job's records (it holds p1/, sp/ or reg/), or a stage's results dir whose timed-* jobs are read
    jobs = [r for r in roots if any((r / m).is_dir() for m, _ in ROOTS)] + \
           [p for r in roots if not any((r / m).is_dir() for m, _ in ROOTS) for p in sorted(r.glob("timed-*")) if p.is_dir()]
    for td in jobs:
        for main_n, t32_n in ROOTS:
            d = td / main_n / tag
            if not d.is_dir():
                continue
            main, _ = W.read_root(d)
            t32, _ = W.read_root(td / t32_n / tag)
            for k in keys:
                a = W.value(main.get("duckdbsql", {}).get(k))
                b = W.value(t32.get("duckdbsql", {}).get(k))
                cand = [x for x in (a, b) if x[0] == W.OK]
                if cand:
                    out[k] = (W.OK, min(v for _, v in cand), str(td.name) + "/" + main_n)
                elif k not in out and (a[0] or b[0]):
                    out[k] = (a[0] or b[0], None, str(td.name) + "/" + main_n)
    return out


def timed_hashes(dirp: Path) -> dict:
    """{system: {(query, instance): set of answer hashes over the timed rotations 1, 2 and E}}"""
    out: dict = {}
    for f in sorted(dirp.glob("rot[12E]/*.json")):
        rec = json.loads(f.read_text())
        s = rec.get("system")
        for c in rec.get("results", []) if s else []:
            if c.get("ok") and c.get("hash"):
                out.setdefault(s, {}).setdefault((c["query"], c.get("instance")), set()).add(c["hash"])
    return out


def point(dirp: Path, wl: str, duck_root: Path | None, js: dict) -> list[str]:
    main, _ = W.read_root(dirp)
    th = timed_hashes(dirp)
    eng, _ = W.read_root(dirp, rots=("E",))
    per_rot = {r: W.read_root(dirp, rots=(r,))[0] for r in ("1", "2")}
    params = json.loads((dirp / "params-used.json").read_text())["instances"]
    excl = json.loads((dirp / "excluded.json").read_text()).get("nativematch", {}) if (dirp / "excluded.json").exists() else {}
    tag = dirp.name
    lines = [f"## {wl.upper()} {tag}", ""]
    head = ["query", "n", "dedicated ms (answered)", "match ms (answered)", "match / dedicated, per instance: median (min, max)",
            "ratio of medians", "match faster on", "DuckDB (SQL) ms, its record (answered)", "match / DuckDB: ratio of medians",
            "match / DuckDB: median per instance (n)", "dedicated / DuckDB: ratio of medians",
            "censored d / m", "refused d / m", "excluded m", "other d / m", "timed answer hashes equal (instances)"]
    rows, erows, srows = [], [], []
    for q in QUERIES:
        ks = [(i["query"], i["instance"]) for i in params if i["query"] == q]
        if not ks:
            continue
        # DuckDB's registry records are at K10M (the same log as K10M-d0: one sha256), its belief records at K10M-d0
        dtag = tag if q.startswith("b.") else tag.replace("-d0", "")
        dk = duck_values(duck_root, dtag, set(ks))
        vals = {s: {k: W.value(main.get(s, {}).get(k)) for k in ks} for s in ("native", "nativematch")}
        cnt = {s: {} for s in vals}
        for s in vals:
            for k, (cl, v) in vals[s].items():
                if cl != W.OK:
                    xl = excl.get(f"{k[0]}#{k[1]}", "") if s == "nativematch" else ""
                    lab = "excluded" if xl and not xl.startswith("excluded: > 10 s") else cl
                    cnt[s][lab] = cnt[s].get(lab, 0) + 1
        d_ok = {k: v for k, (cl, v) in vals["native"].items() if cl == W.OK}
        m_ok = {k: v for k, (cl, v) in vals["nativematch"].items() if cl == W.OK}
        both = [k for k in ks if k in d_ok and k in m_ok]
        r_inst = [m_ok[k] / d_ok[k] for k in both]
        md, mm = med(d_ok.values()), med(m_ok.values())
        dd = {k: v for k, (cl, v, _) in dk.items() if cl == W.OK}
        mdk = med(dd.values())
        mb = [m_ok[k] / dd[k] for k in ks if k in m_ok and k in dd]
        other = lambda s: sum(v for c, v in cnt[s].items() if c not in ("cens", "refused", "excluded"))
        hk = [k for k in ks if th.get("native", {}).get(k) and th.get("nativematch", {}).get(k)]
        heq = sum(1 for k in hk if th["native"][k] == th["nativematch"][k] and len(th["native"][k]) == 1)
        rows.append([q, len(ks), f"{f3(md)} ({len(d_ok)})", f"{f3(mm)} ({len(m_ok)})",
                     f"{f3(med(r_inst))} ({f3(min(r_inst) if r_inst else None)}, {f3(max(r_inst) if r_inst else None)})",
                     f3(mm / md if md and mm else None), f"{sum(1 for x in r_inst if x < 1)} of {len(r_inst)}",
                     f"{f3(mdk)} ({len(dd)})", f3(mm / mdk if mm and mdk else None),
                     f"{f3(med(mb))} ({len(mb)})", f3(md / mdk if md and mdk else None),
                     f"{cnt['native'].get('cens', 0)} / {cnt['nativematch'].get('cens', 0)}",
                     f"{cnt['native'].get('refused', 0)} / {cnt['nativematch'].get('refused', 0)}",
                     cnt["nativematch"].get("excluded", 0), f"{other('native')} / {other('nativematch')}",
                     f"{heq} of {len(hk)}" + (f" (differ by construction: compared without {FIELDS_EXCLUDED[q]})" if q in FIELDS_EXCLUDED else "")])
        # engine time (rotation E)
        er = []
        for s in ("native", "nativematch"):
            e_, f_, x_, dl, e2 = [], [], [], [], []
            for k in ks:
                e = eng.get(s, {}).get(k)
                if not e or (e["cls"] is not None and e["cls"] != W.NA):
                    continue
                for x in e["et"]:
                    if x.get("engine_ms_p50") is None:
                        continue
                    e_.append(x["engine_ms_p50"]); e2.append(x["e2e_p50"]); dl.append(x["e2e_p50"] - x["engine_ms_p50"])
                    if x.get("front_ms_p50") is not None:
                        f_.append(x["front_ms_p50"])
                    if x.get("exec_ms_p50") is not None:
                        x_.append(x["exec_ms_p50"])
            er.append((s, len(e_), med(e2), med(e_), med(f_), med(x_), med(dl)))
        for s, n, e2, e_, f_, x_, dl in er:
            erows.append([q, NAME[s], n, f3(e2), f3(e_), f3(f_), f3(x_), f3(dl)])
        # rotation spread
        for s in ("native", "nativematch"):
            rr = []
            for k in ks:
                a, b = per_rot["1"].get(s, {}).get(k), per_rot["2"].get(s, {}).get(k)
                if a and b and a["vals"] and b["vals"] and a["cls"] is None and b["cls"] is None:
                    rr.append(b["vals"][0] / a["vals"][0])
            srows.append([q, NAME[s], len(rr), f3(med(rr)), f3(min(rr) if rr else None), f3(max(rr) if rr else None)])
        js.setdefault(wl, {})[q] = {
            "n": len(ks), "dedicated_median_ms": md, "match_median_ms": mm, "dedicated_answered": len(d_ok),
            "match_answered": len(m_ok), "ratio_per_instance_median": med(r_inst),
            "ratio_per_instance_min": min(r_inst) if r_inst else None, "ratio_per_instance_max": max(r_inst) if r_inst else None,
            "ratio_of_medians": mm / md if md and mm else None, "match_faster_on": sum(1 for x in r_inst if x < 1),
            "duckdb_median_ms": mdk, "duckdb_answered": len(dd), "match_over_duckdb_ratio_of_medians": mm / mdk if mm and mdk else None,
            "match_over_duckdb_per_instance_median": med(mb), "dedicated_over_duckdb_ratio_of_medians": md / mdk if md and mdk else None,
            "timed_hashes_equal": heq, "timed_hashes_compared": len(hk), "counts": cnt, "engine": [dict(zip(("system", "n", "e2e_ms", "engine_ms", "front_ms", "exec_ms", "delivery_ms"), r)) for r in er],
            "per_instance": {f"{k[0]}#{k[1]}": {"dedicated": vals["native"][k], "match": vals["nativematch"][k],
                                                "duckdb": dk.get(k)} for k in ks}}
    lines += W.md_table(head, rows) + [""]
    lines += ["Engine time (engine-time pass, rotation E; medians over instances of each instance's median):", ""]
    lines += W.md_table(["query", "path", "instances", "e2e ms", "engine ms", "front ms", "exec ms", "delivery ms"], erows) + [""]
    lines += ["Rotation spread (per instance rotation 2 p50 / rotation 1 p50):", ""]
    lines += W.md_table(["query", "path", "instances", "median", "min", "max"], srows) + [""]
    return lines


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--res", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--json")
    ap.add_argument("--duckdb-w1")
    ap.add_argument("--duckdb-w2")
    ap.add_argument("--duckdb-w5")
    ap.add_argument("--duckdb-w2d", help="records of the K10M-d10 registry instances (the T-at-10% stage); the B ones are read from --duckdb-w2")
    a = ap.parse_args()
    res = Path(a.res)
    duck = {"w1": Path(a.duckdb_w1) if a.duckdb_w1 else None, "w2": Path(a.duckdb_w2) if a.duckdb_w2 else None}
    duck["w1x"] = duck["w1y"] = duck["w1"]
    duck["w2y"] = duck["w2"]
    duck["w5"] = [Path(x) for x in a.duckdb_w5.split(",")] if a.duckdb_w5 else None
    duck["w2d"] = ([Path(x) for x in a.duckdb_w2d.split(",")] if a.duckdb_w2d else []) + ([duck["w2"]] if duck["w2"] else [])
    lines = ["# Dedicated operator against the match path, same store, same instances, same job (descriptive)", "",
             "End-to-end ms (primary). DuckDB (SQL) values come from its own W1/W2 timed records: another job, another node.", ""]
    js: dict = {}
    for wl in ("w1", "w2", "w1x", "w1y", "w2y", "w5", "w2d"):
        for dirp in sorted((res / wl).glob("K*")):
            if (dirp / "params-used.json").exists():
                lines += point(dirp, wl, duck[wl], js)
    Path(a.out).write_text("\n".join(lines) + "\n")
    Path(a.json or (str(a.out) + ".json")).write_text(json.dumps(js, indent=1, default=str) + "\n")
    print("\n".join(lines))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
