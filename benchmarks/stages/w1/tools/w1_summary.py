#!/usr/bin/env python3
"""W1 timed stage: summaries of the timed records (e2e primary; engine/delivery split from the engine pass).

    w1_summary.py --timed <timed results dir> [--timed ...] --out <dir>

Each timed results dir holds result roots sp/, p1/, t32sp/, t32p1/ with <tag>/rot{1,2,E}/<fam>-<system>.json.
Rules:
  * instance value = mean over the two rotations of each rotation's median timed call (p50_ms, e2e);
  * a cell censored in either rotation, or excluded at the answer check as "> 10 s", is "> 10 s";
    a Gnomon refusal is "refused (estimated > 10 s)"; "not written"/"unsupported" is n/a;
  * DuckDB (SQL) = the better of 64 threads (sp/p1) and 32 threads (t32sp/t32p1) per instance; both reported;
  * reused systems (Neo4j, Memgraph, XTDB, LadybugDB, AeonG, Raphtory) at the single-parameter instances come from
    the first campaign's records ($PROJECT/extcmp/campaign/results), under that campaign's protocol.
Writes summary.json, tables.md, and forecast scoring.
"""
from __future__ import annotations

import argparse
import json
import math
import statistics as st
from collections import defaultdict
from pathlib import Path

E = Path(__import__("os").environ.get("GNOMON_BENCH_ROOT", ".") + "")
FIRST = E / "campaign/results"
FAM = {"P": ("hist.single", "hist.asof"), "T": ("snap.hop2", "reach.window", "paths.k"),
       "S": ("series.count", "burst.zscore", "agg.rel_bucket"),
       "W": ("series.count.w10", "burst.zscore.w10", "agg.rel_bucket.w10"), "D": ("diff.global",),
       "N": ("nbr.evolution", "coactive.narrow"), "R": ("resolve.substr",), "M": ("motif.filtered",),
       "B": ("b.snap.hop2@T", "b.snap.hop2@now", "b.reach@T", "b.reach@now", "b.diff", "b.diff@ctl", "b.hist",
             "b.hist@now")}
FAM_OF = {q: f for f, qs in FAM.items() for q in qs}
FAM_OF.update({"diff.belief": "D", "resolve.exact": "R", "resolve.prefix": "R"})
NATIVE_ONLY = {"diff.belief", "resolve.exact", "resolve.prefix"}
RERUN = ("native", "duckdb", "duckdbsql", "clickhouse", "postgres")
REUSED = ("neo4j", "memgraph", "xtdb", "ladybug", "aeong", "raphtory")
NAME = {"native": "Gnomon", "duckdb": "Gnomon on DuckDB storage", "duckdbsql": "DuckDB (SQL)",
        "duckdbsql64": "DuckDB (SQL) 64 thr", "duckdbsql32": "DuckDB (SQL) 32 thr", "clickhouse": "ClickHouse",
        "postgres": "PostgreSQL", "neo4j": "Neo4j", "memgraph": "Memgraph", "xtdb": "XTDB", "ladybug": "LadybugDB",
        "aeong": "AeonG", "raphtory": "Raphtory"}
TAGS = ("K1M", "K10M", "K10M-d0", "K10M-d1", "K10M-d10")
CENS, REF, NA = "> 10 s", "refused (estimated > 10 s)", "n/a"
ADD1 = {"P": (30, 150), "T": (15, 80), "S": (200, 1500), "W": (80, 400), "D": (70, 350), "N": (12, 80),
        "R": (30, 200), "M": (2, 8), "B": (35, 200)}
P1F = {"hist.single": (0.75, 1.2, 2.5), "hist.asof": (0.80, 1.2, 2.5), "snap.hop2": (1.0, 30, 150),
       "reach.window": (0.90, 3, 20), "paths.k": (0.90, 1.3, 4), "nbr.evolution": (1.0, 4, 30),
       "coactive.narrow": (1.0, 3, 8), "resolve.substr": (1.0, 5, 15), "agg.rel_bucket": (0.80, 1.3, 4),
       "series.count": (1.0, 6, 40), "burst.zscore": (1.0, 4, 15), "series.count.w10": (0.60, 0.9, 3),
       "burst.zscore.w10": (0.40, 0.7, 1.6), "agg.rel_bucket.w10": (0.40, 0.7, 1.6), "diff.global": (1.0, 8, 30),
       "motif.filtered": (0.90, 1.3, 6), "b.snap.hop2@now": (0.20, 0.2, 0.6), "b.snap.hop2@T": (0.20, 0.2, 0.6),
       "b.reach@now": (0.70, 3, 10), "b.reach@T": (0.75, 3, 10), "b.diff@ctl": (1.0, 6, 20),
       "b.hist": (0.90, 1.5, 6), "b.hist@now": (1.0, 4, 12)}
P1F_DIFF = {"K10M-d0": (1.0, 10, 200), "K10M-d10": (0.75, 1.5, 200)}


def key(c):
    return f"{c['query']}#{c['instance']}" if c.get("instance") else c["query"]


def status(c):
    if c.get("ok"):
        return None
    e = str(c.get("error", ""))
    if e.startswith(("unsupported", "NotImplemented", "not written")) or "not expressible" in e:
        return NA
    if c.get("refused") or "E_COST" in e:
        return REF
    if c.get("aborted") or c.get("censored") or e.startswith((">", "excluded: >")) or "client-side limit" in e \
            or "> 10 s at answer check" in e:
        return CENS
    if "excluded" in e:
        return e[:60]
    return "error: " + e[:80]


def read_root(d: Path, rots=("1", "2")):
    """{system: {key: {"query", "stratum", "class", "vals": [p50 per rotation], "st": status or None}}}"""
    out: dict = defaultdict(dict)
    for r in rots:
        rd = d / f"rot{r}"
        if not rd.is_dir():
            continue
        for f in sorted(rd.glob("*.json")):
            if f.name.endswith("-hung.json") or "-" not in f.stem:
                continue
            rec = json.loads(f.read_text())
            s = rec.get("system")
            if not s or "results" not in rec:
                continue
            for c in rec["results"]:
                k = key(c)
                e = out[s].setdefault(k, {"query": c["query"], "stratum": c.get("stratum"), "vals": [], "st": None,
                                          "et": []})
                stt = status(c)
                if stt is None:
                    e["vals"].append(c["p50_ms"])
                    if c.get("engine_time", {}).get("summary"):
                        e["et"].append(dict(c["engine_time"]["summary"], e2e_p50=c["p50_ms"]))
                elif e["st"] is None or e["st"] == NA:
                    e["st"] = stt
    return out


def value(e):
    if e["st"] is not None and e["st"] != NA:
        return e["st"]
    if not e["vals"]:
        return e["st"] or NA
    return sum(e["vals"]) / len(e["vals"])


def num(v):
    return isinstance(v, (int, float))


def fmt(v):
    if not num(v):
        return str(v)
    return f"{v:.3g}" if v < 1000 else f"{v:,.0f}"


def gm(xs):
    xs = [x for x in xs if x > 0]
    return math.exp(sum(map(math.log, xs)) / len(xs)) if xs else None


def collect(timed: list[Path]):
    """{mode: {tag: {system: {key: entry}}}} plus engine-pass entries."""
    T: dict = {"sp": {}, "p1": {}}
    EP: dict = {"sp": {}, "p1": {}}
    for td in timed:
        for mode in ("sp", "p1"):
            for tag in TAGS:
                d = td / mode / tag
                if not d.is_dir():
                    continue
                main = read_root(d)
                t32 = read_root(td / f"t32{mode}" / tag)
                sysd = {s: v for s, v in main.items()}
                if "duckdbsql" in sysd:
                    sysd["duckdbsql64"] = sysd.pop("duckdbsql")
                if "duckdbsql" in t32:
                    sysd["duckdbsql32"] = t32["duckdbsql"]
                # better of the two per instance
                best = {}
                for k in set(sysd.get("duckdbsql64", {})) | set(sysd.get("duckdbsql32", {})):
                    a, b = sysd.get("duckdbsql64", {}).get(k), sysd.get("duckdbsql32", {}).get(k)
                    va, vb = (value(a) if a else NA), (value(b) if b else NA)
                    cand = [(x, y) for x, y in ((va, a), (vb, b)) if num(x)]
                    best[k] = min(cand, key=lambda t: t[0])[1] if cand else (a or b)
                sysd["duckdbsql"] = best
                T[mode][tag] = sysd
                EP[mode][tag] = read_root(d, rots=("E",))
    return T, EP


def reused(tag):
    out = {}
    d = FIRST / tag
    for s in REUSED:
        out[s] = {}
        for r in ("1", "2"):
            for f in sorted((d / f"rot{r}").glob(f"*-{s}.json")) if (d / f"rot{r}").is_dir() else []:
                rec = json.loads(f.read_text())
                for c in rec.get("results", []):
                    e = out[s].setdefault(c["query"], {"query": c["query"], "vals": [], "st": None, "et": []})
                    if c.get("ok"):
                        e["vals"].append(c["p50_ms"])
                    elif e["st"] is None:
                        err = str(c.get("error", ""))
                        e["st"] = "excluded: DIFFERS" if "DIFFERS" in err else (
                            "> 3,600 s (aborted)" if c.get("aborted") or ">3600" in err else
                            ("n/a" if "not written" in err or "unsupported" in err or "not expressible" in err
                             else err[:50]))
    return out


def faster(g, o):
    """Gnomon faster than o on this cell? None when not comparable."""
    if not num(g):
        return None
    if num(o):
        return g < o
    if o == CENS or (isinstance(o, str) and o.startswith(">")):
        return True
    return None


def point_summary(sysd, systems, extra=None):
    rows = []
    gn = sysd.get("native", {})
    for s in systems:
        cs = (extra or {}).get(s) if extra and s in extra else sysd.get(s, {})
        if not cs:
            continue
        vals = {k: value(e) for k, e in cs.items()}
        if extra and s in extra:          # reused: keyed by query id; Gnomon's single-parameter key is query#inst
            gv = {e["query"]: value(e) for e in gn.values()}
        else:
            gv = {k: value(e) for k, e in gn.items()}
        ans = [v for v in vals.values() if num(v)]
        cmp_ = [faster(gv.get(k), v) for k, v in vals.items() if s != "native"]
        cmp_ = [c for c in cmp_ if c is not None]
        rows.append({"system": s, "cells": len(vals), "answered": len(ans),
                     "censored": sum(1 for v in vals.values() if v == CENS or (isinstance(v, str) and v.startswith(">"))),
                     "refused": sum(1 for v in vals.values() if v == REF),
                     "na": sum(1 for v in vals.values() if v == NA),
                     "other": sum(1 for v in vals.values() if isinstance(v, str) and v not in (CENS, REF, NA)
                                  and not v.startswith(">")),
                     "median_e2e_ms": st.median(ans) if ans else None,
                     "gnomon_faster": sum(cmp_) if s != "native" else None, "comparable": len(cmp_) if s != "native" else None})
    return rows


def engine_split(EP_tag, T_tag):
    out = []
    for s in ("native", "duckdbsql", "postgres"):
        es = EP_tag.get(s, {})
        sh, dl, fr, ub, cost = [], [], [], [], []
        main = T_tag.get("duckdbsql64" if s == "duckdbsql" else s, {})
        for k, e in es.items():
            for x in e["et"]:
                e2e = x.get("e2e_p50")
                eng = x.get("engine_ms_p50")
                if e2e and eng is not None:
                    sh.append(eng / e2e)
                    dl.append(e2e - eng)
                    if x.get("front_ms_p50") is not None and eng:
                        fr.append(x["front_ms_p50"] / eng)
                    if x.get("engine_ub_ms_p50"):
                        ub.append(x["engine_ub_ms_p50"] / e2e)
                    mv = value(main[k]) if k in main else None
                    if num(mv) and mv > 0:
                        cost.append(e2e / mv)
        if sh:
            out.append({"system": s, "cells": len(sh), "engine_share_median": st.median(sh),
                        "engine_share_p10": sorted(sh)[len(sh) // 10], "delivery_ms_median": st.median(dl),
                        "front_share_of_engine_median": st.median(fr) if fr else None,
                        "engine_ub_share_median": st.median(ub) if ub else None,
                        "engine_pass_over_timed_e2e_median": st.median(cost) if cost else None})
    # ClickHouse: probes on in the timed rotations
    ch = T_tag.get("clickhouse", {})
    sh, dl = [], []
    for e in ch.values():
        for x in e["et"]:
            if x.get("e2e_p50") and x.get("engine_ms_p50") is not None:
                sh.append(x["engine_ms_p50"] / x["e2e_p50"]); dl.append(x["e2e_p50"] - x["engine_ms_p50"])
    if sh:
        out.append({"system": "clickhouse", "cells": len(sh), "engine_share_median": st.median(sh),
                    "engine_share_p10": sorted(sh)[len(sh) // 10], "delivery_ms_median": st.median(dl),
                    "source": "timed rotations (probes on)"})
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--timed", type=Path, action="append", required=True)
    ap.add_argument("--out", type=Path, required=True)
    a = ap.parse_args()
    a.out.mkdir(parents=True, exist_ok=True)
    T, EP = collect(a.timed)
    S = {"single": {}, "curated": {}, "engine": {}, "per_query_single": {}}
    md = ["# W1 timed stage: summaries (e2e primary)", ""]
    for tag in TAGS:
        if tag not in T["sp"]:
            continue
        ru = reused(tag)
        allsys = ["native", "duckdb", "duckdbsql", "duckdbsql64", "duckdbsql32", "clickhouse", "postgres"] + list(REUSED)
        rows = point_summary(T["sp"][tag], allsys, extra=ru)
        S["single"][tag] = rows
        md += [f"## {tag}: single-parameter instances (re-run systems: this stage; reused: first campaign)", "",
               "| system | cells | answered | > 10 s | refused | n/a | median e2e ms | Gnomon faster |",
               "|---|---:|---:|---:|---:|---:|---:|---|"]
        for r in rows:
            gf = "" if r["gnomon_faster"] is None else f"{r['gnomon_faster']} of {r['comparable']}"
            md.append(f"| {NAME.get(r['system'], r['system'])}{' (reused)' if r['system'] in REUSED else ''} | {r['cells']} | "
                      f"{r['answered']} | {r['censored']} | {r['refused']} | {r['na']} | {fmt(r['median_e2e_ms']) if r['median_e2e_ms'] is not None else '–'} | {gf} |")
        # per query
        pq = {}
        ids = sorted({e["query"] for e in T["sp"][tag].get("native", {}).values()}, key=lambda q: (FAM_OF.get(q, "Z"), q))
        md += ["", f"Per query ({tag}, ms; mean of two rotations' medians; reused systems: first campaign):", "",
               "| query | " + " | ".join(NAME[s] for s in ["native", "duckdb", "duckdbsql", "clickhouse", "postgres"] + list(REUSED)) + " |",
               "|---|" + "---:|" * 11]
        for q in ids:
            cells = []
            for s in ["native", "duckdb", "duckdbsql", "clickhouse", "postgres"]:
                e = next((x for x in T["sp"][tag].get(s, {}).values() if x["query"] == q), None)
                cells.append(value(e) if e else "–")
            for s in REUSED:
                e = ru.get(s, {}).get(q)
                cells.append(value(e) if e else "–")
            pq[q] = dict(zip(["native", "duckdb", "duckdbsql", "clickhouse", "postgres"] + list(REUSED), cells))
            md.append(f"| {q} | " + " | ".join(fmt(c) for c in cells) + " |")
        S["per_query_single"][tag] = pq
        if tag in T["p1"]:
            rows = point_summary(T["p1"][tag], ["native", "duckdb", "duckdbsql", "duckdbsql64", "duckdbsql32", "clickhouse", "postgres"])
            S["curated"][tag] = rows
            md += ["", f"## {tag}: curated P1 instances", "",
                   "| system | cells | answered | > 10 s | refused | n/a | median e2e ms | Gnomon faster |",
                   "|---|---:|---:|---:|---:|---:|---:|---|"]
            for r in rows:
                gf = "" if r["gnomon_faster"] is None else f"{r['gnomon_faster']} of {r['comparable']}"
                md.append(f"| {NAME.get(r['system'], r['system'])} | {r['cells']} | {r['answered']} | {r['censored']} | "
                          f"{r['refused']} | {r['na']} | {fmt(r['median_e2e_ms']) if r['median_e2e_ms'] is not None else '–'} | {gf} |")
        for mode in ("sp", "p1"):
            if tag in EP[mode]:
                es = engine_split(EP[mode][tag], T[mode][tag])
                S["engine"][f"{mode} {tag}"] = es
                md += ["", f"Engine/delivery split, {tag} {'single-parameter' if mode == 'sp' else 'curated'} "
                           f"(engine pass; ClickHouse from the timed rotations):", "",
                       "| system | cells | engine/e2e median (p10) | delivery ms median | front/engine median | engine_ub/e2e median | engine-pass e2e / timed e2e |",
                       "|---|---:|---|---:|---:|---:|---:|"]
                for r in es:
                    md.append(f"| {NAME[r['system']]} | {r['cells']} | {r['engine_share_median']:.2f} ({r['engine_share_p10']:.2f}) | "
                              f"{fmt(r['delivery_ms_median'])} | {fmt(r.get('front_share_of_engine_median')) if r.get('front_share_of_engine_median') is not None else '–'} | "
                              f"{fmt(r.get('engine_ub_share_median')) if r.get('engine_ub_share_median') else '–'} | "
                              f"{fmt(r.get('engine_pass_over_timed_e2e_median')) if r.get('engine_pass_over_timed_e2e_median') else '–'} |")
        md.append("")
    (a.out / "summary.json").write_text(json.dumps(S, indent=1, default=str) + "\n")
    (a.out / "tables.md").write_text("\n".join(md) + "\n")
    print(f"wrote {a.out}/summary.json and tables.md")


if __name__ == "__main__":
    main()
