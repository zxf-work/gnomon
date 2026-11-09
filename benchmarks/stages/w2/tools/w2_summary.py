#!/usr/bin/env python3
"""W2 timed stage: summaries and scoring of the timed records (end-to-end time primary).

    w2_summary.py --timed <timed results dir> [--timed ...] --params <params dir> --out <dir>

Each timed results dir holds result roots p1/ and t32p1/ with <tag>/rot{1,2,E}/<fam>-<system>.json and <tag>/verdicts.json.
Rules:
  * instance value = mean over the two rotations of each rotation's median timed call (p50_ms, e2e);
  * a cell censored in either rotation, or excluded at the answer check because the call took > 10 s, is "> 10 s";
    a (system, query) pair skipped by the R-10M rule is "> 10 s (R-10M)";
  * a cell the answer-check window never reached is "unreached" (nothing measured; neither compared nor timed);
  * DuckDB (SQL) = the better of 64 threads (p1) and 32 threads (t32p1) per instance;
  * cell value (query, point, system) = median over the instances the system answered;
  * best competitor on a cell/instance = the fastest of DuckDB (SQL), ClickHouse, PostgreSQL, Neo4j, Memgraph, LadybugDB
    that answered (Gnomon on DuckDB storage is Gnomon's own storage-engine configuration and is not a competitor);
  * ratio = Gnomon / other, below 1 means Gnomon is faster; row centre = geometric mean of the row's cell ratios.
Writes summary.json, tables.md and scoring.md.
"""
from __future__ import annotations

import argparse
import json
import math
import statistics as st
from collections import Counter, defaultdict
from pathlib import Path

FAM = {"P": ("hist.single", "hist.asof"), "T": ("snap.hop2", "reach.window", "paths.k"),
       "S": ("series.count", "burst.zscore", "agg.rel_bucket"),
       "W": ("series.count.w10", "burst.zscore.w10", "agg.rel_bucket.w10"), "D": ("diff.global",),
       "N": ("nbr.evolution", "coactive.narrow"), "R": ("resolve.substr",), "M": ("motif.filtered",),
       "B": ("b.snap.hop2@T", "b.snap.hop2@now", "b.reach@T", "b.reach@now", "b.diff", "b.diff@ctl", "b.hist",
             "b.hist@now")}
FAM_OF = {q: f for f, qs in FAM.items() for q in qs}
SYSTEMS = ["native", "duckdb", "duckdbsql", "clickhouse", "postgres", "neo4j", "memgraph", "ladybug"]
COMP = ["duckdbsql", "clickhouse", "postgres", "neo4j", "memgraph", "ladybug"]
NAME = {"native": "Gnomon", "duckdb": "Gnomon on DuckDB storage", "duckdbsql": "DuckDB (SQL)",
        "duckdbsql64": "DuckDB (SQL) 64 thr", "duckdbsql32": "DuckDB (SQL) 32 thr", "clickhouse": "ClickHouse",
        "postgres": "PostgreSQL", "neo4j": "Neo4j", "memgraph": "Memgraph", "ladybug": "LadybugDB"}
REG = ("K1M", "K10M")
BPTS = ("K1M-d0", "K1M-d1", "K1M-d10", "K10M-d0", "K10M-d1", "K10M-d10")
TAGS = REG + BPTS
W2IDS = ["snap.hop2", "reach.window", "paths.k", "nbr.evolution", "coactive.narrow", "hist.single", "diff.global",
         "agg.rel_bucket", "series.count.w10", "motif.filtered"]
OK, CENS, R10, REF, UNR, NA, ERR = "ok", "cens", "r10m", "refused", "unreached", "na", "error"


def num(v):
    return isinstance(v, (int, float))


def fmt(v):
    if v is None:
        return "–"
    if not num(v):
        return str(v)
    if v < 1:
        return f"{v:.3g}"
    return f"{v:.3g}" if v < 100 else f"{v:,.0f}"


def gm(xs):
    xs = [x for x in xs if x and x > 0]
    return math.exp(sum(map(math.log, xs)) / len(xs)) if xs else None


def sig2(x):
    if x is None or x <= 0:
        return None
    d = math.floor(math.log10(x))
    return round(x, -d + 1)


def key(c):
    return (c["query"], c.get("instance"))


def classify(c, verd):
    """Class of a non-ok cell."""
    e = str(c.get("error", ""))
    if c.get("refused") or "E_COST" in e or "CostError" in e:
        return REF
    if e.startswith("excluded: > 10") or e.startswith(">") or c.get("aborted") or c.get("censored") \
            or "client-side limit" in e:
        return CENS
    if e.startswith(("unsupported", "NotImplemented", "not written")) or "not expressible" in e:
        return NA
    if e.startswith("unreached") or c.get("unreached"):
        return UNR
    if "excluded" in e:
        v = verd.get(key(c)) or {}
        d = str(v.get("detail", ""))
        if "unreached" in d:
            return UNR
        if d.startswith(">10.0 s") or "timeout" in d or "client-side limit" in d or "> 10 s" in d:
            return CENS
        if "CostError" in d or "E_COST" in d or v.get("verdict") == "refused" and "refus" in d.lower():
            return REF
        if "NotImplemented" in d or "not expressible" in d:
            return NA
        return ERR
    return ERR


def read_root(d: Path, rots=("1", "2")):
    """{system: {key: {"query","stratum","vals":[...],"cls":None|class,"et":[...]}}}, {system: skipped queries}"""
    verd = {}
    vf = d / "verdicts.json"
    if vf.exists():
        for s, rows in json.loads(vf.read_text()).items():
            verd[s] = {(r["query"], r.get("instance")): r for r in rows}
    out: dict = defaultdict(dict)
    skip: dict = defaultdict(set)
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
            for q in (rec.get("protocol_p1", {}).get("r10m", {}) or {}).get("skipped", []) or []:
                skip[s].add(q)
            for c in rec["results"]:
                k = key(c)
                e = out[s].setdefault(k, {"query": c["query"], "stratum": c.get("stratum"), "vals": [], "cls": None,
                                          "et": []})
                if c.get("ok"):
                    e["vals"].append(c["p50_ms"])
                    es = (c.get("engine_time") or {}).get("summary")
                    if es:
                        e["et"].append(dict(es, e2e_p50=c["p50_ms"]))
                else:
                    cl = classify(c, verd.get(s, {}))
                    if e["cls"] is None or e["cls"] == NA or (cl == CENS):
                        e["cls"] = cl
    return out, skip


def value(e):
    """(class, value)"""
    if e is None:
        return (None, None)
    if e["cls"] is not None and e["cls"] != NA:
        return (e["cls"], None)
    if not e["vals"]:
        return (e["cls"] or NA, None)
    return (OK, sum(e["vals"]) / len(e["vals"]))


def collect(timed, pdir):
    """{tag: {system: {key: (class, value, stratum)}}}, engine-pass entries, params instances"""
    T, EP, INST = {}, {}, {}
    for tag in TAGS:
        pf = pdir / f"params-{tag}.json"
        inst = json.loads(pf.read_text())["instances"]
        INST[tag] = {(i["query"], i["instance"]): i.get("stratum") for i in inst}
    for td in timed:
        for tag in TAGS:
            d = td / "p1" / tag
            if not d.is_dir():
                continue
            main, skip = read_root(d)
            t32, _ = read_root(td / "t32p1" / tag)
            res = {}
            for s in SYSTEMS:
                cells = {}
                src = main.get(s, {})
                for k, strat in INST[tag].items():
                    if s == "duckdbsql":
                        a, b = value(main.get(s, {}).get(k)), value(t32.get(s, {}).get(k))
                        cand = [x for x in (a, b) if x[0] == OK]
                        if cand:
                            cl, v = min(cand, key=lambda t: t[1])
                        else:
                            cl, v = a if a[0] is not None else b
                    else:
                        cl, v = value(src.get(k))
                    if cl is None:
                        cl = R10 if k[0] in skip.get(s, set()) else "missing"
                    cells[k] = (cl, v, strat)
                res[s] = cells
            T[tag] = res
            es = {}
            for s in ("native", "duckdbsql", "postgres"):
                es[s] = read_root(d, rots=("E",))[0].get(s, {})
            for s in ("clickhouse", "neo4j", "memgraph"):
                es[s] = main.get(s, {})
            EP[tag] = es
    return T, EP, INST


def tally(cells):
    c = Counter(cl for cl, _, _ in cells.values())
    return c


def point_rows(T, tag):
    g = T[tag]["native"]
    rows = []
    for s in SYSTEMS:
        cs = T[tag][s]
        t = tally(cs)
        ans = [v for cl, v, _ in cs.values() if cl == OK]
        fast = comp = 0
        ratios = []
        for k, (cl, v, _) in cs.items():
            gcl, gv, _ = g[k]
            if s == "native" or gcl != OK:
                continue
            if cl == OK:
                comp += 1
                fast += gv < v
                ratios.append(gv / v)
            elif cl in (CENS, R10):
                comp += 1
                fast += 1
        rows.append({"system": s, "cells": len(cs), "answered": t[OK], "censored": t[CENS], "r10m": t[R10],
                     "refused": t[REF], "unreached": t[UNR], "error": t[ERR], "na": t[NA] + t["missing"],
                     "median_e2e_ms": st.median(ans) if ans else None,
                     "faster": None if s == "native" else fast, "comparable": None if s == "native" else comp,
                     "median_ratio_both_ok": st.median(ratios) if ratios else None})
    return rows


def cell_median(T, tag, s, q):
    vs = [v for (qq, _), (cl, v, _) in T[tag][s].items() if qq == q and cl == OK]
    return (st.median(vs), len(vs)) if vs else (None, 0)


def best_cell(T, tag, q, half=False):
    """(median, system, answered): the fastest competitor cell; half=True keeps only competitors that answered at least
    half of the query's instances (sensitivity variant)."""
    n_all = sum(1 for k in T[tag]["native"] if k[0] == q)
    cands = []
    for s in COMP:
        m, n = cell_median(T, tag, s, q)
        if m is not None and (not half or 2 * n >= n_all):
            cands.append((m, s, n))
    return min(cands) if cands else None


def inst_ratios(T, tag, qs):
    """[(ratio, stratum, query, best system)] for instances with a Gnomon value and >=1 competitor answer;
    and wins/losses incl. no-answer instances."""
    out, noans = [], 0
    for k, (cl, gv, strat) in T[tag]["native"].items():
        if k[0] not in qs or cl != OK:
            continue
        cands = [(T[tag][s][k][1], s) for s in COMP if T[tag][s][k][0] == OK]
        if not cands:
            noans += 1
            out.append((None, strat, k[0], None))
            continue
        bv, bs = min(cands)
        out.append((gv / bv, strat, k[0], bs))
    return out


def engine_split(EP, T, tag):
    rows = []
    for s in ("native", "duckdbsql", "postgres", "clickhouse", "neo4j", "memgraph"):
        sh, dl, fr, ex, e2 = [], [], [], [], []
        for k, e in EP[tag].get(s, {}).items():
            if e["cls"] is not None and e["cls"] != NA:
                continue
            for x in e["et"]:
                e2e, eng = x.get("e2e_p50"), x.get("engine_ms_p50")
                if e2e and eng is not None:
                    sh.append(eng / e2e)
                    dl.append(e2e - eng)
                    e2.append(e2e)
                    if x.get("front_ms_p50") is not None:
                        fr.append(x["front_ms_p50"])
                        ex.append(eng - x["front_ms_p50"])
        if sh:
            rows.append({"system": s, "cells": len(sh), "engine_share": st.median(sh),
                         "engine_share_p10": sorted(sh)[len(sh) // 10], "delivery_ms": st.median(dl),
                         "e2e_ms": st.median(e2), "front_ms": st.median(fr) if fr else None,
                         "exec_ms": st.median(ex) if ex else None,
                         "src": "engine pass" if s in ("native", "duckdbsql", "postgres") else "timed rotations"})
    return rows


def md_table(head, rows):
    return ["| " + " | ".join(head) + " |", "|" + "---|" * len(head)] + ["| " + " | ".join(map(str, r)) + " |" for r in rows]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--timed", type=Path, action="append", required=True)
    ap.add_argument("--params", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    a = ap.parse_args()
    a.out.mkdir(parents=True, exist_ok=True)
    T, EP, INST = collect(a.timed, a.params)
    S = {"points": {}, "engine": {}, "gate": {}}
    # headline tables: median e2e (Gnomon faster x of y) per system and point; cell classes totalled over the other systems
    head = ["# W2 timed stage: summaries (e2e primary)", "", "## Headline: median e2e ms (cells where Gnomon is faster, of comparable cells)", ""]
    hb = []
    for s in SYSTEMS:
        row = [NAME[s]]
        for tag in TAGS:
            r = next(x for x in point_rows(T, tag) if x["system"] == s)
            row.append(f"{fmt(r['median_e2e_ms'])}" + ("" if r["faster"] is None else f" ({r['faster']}/{r['comparable']})"))
        hb.append(row)
    head += md_table(["system"] + list(TAGS), hb)
    head += ["", "## Cell classes totalled over the seven other systems (instances x systems)", ""]
    cb = []
    for tag in TAGS:
        tot = Counter()
        for s in SYSTEMS[1:]:
            tot.update(tally(T[tag][s]))
        cb.append([tag, tot[OK], tot[CENS], tot[R10], tot[REF], tot[UNR], tot[ERR], tot[NA] + tot["missing"]])
    head += md_table(["point", "answered", "> 10 s", "R-10M skipped", "refused (Gnomon on DuckDB storage CostError)", "unreached", "error", "n/a or missing"], cb)
    head.append("")
    md = head + [""]
    md += ["",
          "Instance value = mean of the two rotations' medians (ms). Gnomon faster = cells where Gnomon's value is below the "
          "other system's, out of comparable cells (a censored or R-10M-skipped cell counts as Gnomon faster; unreached, "
          "error and not-expressible cells are not comparable). Median e2e is over the cells the system answered.", ""]
    for tag in TAGS:
        if tag not in T:
            continue
        rows = point_rows(T, tag)
        S["points"][tag] = rows
        kind = "registry instances" if tag in REG else "belief-time (B family) instances"
        md += [f"## {tag}: {kind} ({len(INST[tag])} per system)", ""]
        md += md_table(["system", "cells", "answered", "> 10 s", "R-10M skipped", "refused", "unreached", "error", "n/a",
                        "median e2e ms", "Gnomon faster", "median ratio (both answered)"],
                       [[NAME[r["system"]], r["cells"], r["answered"], r["censored"], r["r10m"], r["refused"], r["unreached"],
                         r["error"], r["na"], fmt(r["median_e2e_ms"]),
                         "" if r["faster"] is None else f"{r['faster']} of {r['comparable']}",
                         fmt(r["median_ratio_both_ok"])] for r in rows])
        md.append("")
        qs = sorted({k[0] for k in INST[tag]}, key=lambda q: (FAM_OF.get(q, "Z"), q))
        head = ["query", "n"] + [NAME[s] for s in SYSTEMS] + ["best competitor", "Gnomon ÷ best"]
        body = []
        for q in qs:
            n = sum(1 for k in INST[tag] if k[0] == q)
            cells = []
            for s in SYSTEMS:
                m, c = cell_median(T, tag, s, q)
                cells.append("–" if m is None else (fmt(m) if c == n else f"{fmt(m)} ({c})"))
            b = best_cell(T, tag, q)
            g, _ = cell_median(T, tag, "native", q)
            body.append([q, n] + cells + [f"{NAME[b[1]]}" if b else "none", fmt(g / b[0]) if b and g else "–"])
        md += [f"Per query, {tag}: median over answered instances, ms (the count in brackets is the number answered, when below n):", ""]
        md += md_table(head, body)
        md.append("")
        es = engine_split(EP, T, tag)
        S["engine"][tag] = es
        md += [f"Engine, front-end and delivery split, {tag} (median over cells of each instance's median call; Gnomon, "
               "DuckDB (SQL) and PostgreSQL from the engine pass, the others from the timed rotations with probes on):", ""]
        md += md_table(["system", "cells", "e2e ms", "engine share (p10)", "front ms", "execution ms", "delivery ms"],
                       [[NAME[r["system"]], r["cells"], fmt(r["e2e_ms"]), f"{r['engine_share']:.2f} ({r['engine_share_p10']:.2f})",
                         fmt(r["front_ms"]), fmt(r["exec_ms"]), fmt(r["delivery_ms"])] for r in es])
        md.append("")
    # gate table: how many instances each system answered/excluded (class of the timed cells)
    md += ["## Answer-gate outcome per system and point (class of each instance's timed cell)", ""]
    for tag in TAGS:
        if tag in T:
            S["gate"][tag] = {s: dict(tally(T[tag][s])) for s in SYSTEMS}
    md += md_table(["point", "system", "answered", "> 10 s", "R-10M", "refused", "unreached", "error", "n/a or missing"],
                   [[tag, NAME[s], t.get(OK, 0), t.get(CENS, 0), t.get(R10, 0), t.get(REF, 0), t.get(UNR, 0), t.get(ERR, 0),
                     t.get(NA, 0) + t.get("missing", 0)] for tag in TAGS if tag in T for s, t in S["gate"][tag].items()])
    (a.out / "summary.json").write_text(json.dumps(S, indent=1, default=str) + "\n")
    (a.out / "tables.md").write_text("\n".join(md) + "\n")

    print(f"wrote {a.out}")


if __name__ == "__main__":
    main()
