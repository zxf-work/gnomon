#!/usr/bin/env python3
"""Thread-matched control: tables from the timed records (end-to-end time primary).

    thr_summary.py --timed <results/timed-<job>> --out <dir>

<timed>/<wl>/cfg-<c>/<tag>/rot{1,2,E}/<family>-<system>.json and <tag>/excluded.json, wl in {w1, w2}.
Rules (the W1/W2 summaries'):
  * instance value = mean over the two rotations of each rotation's median timed call (p50_ms, end-to-end);
  * a cell censored in either rotation, or excluded at the answer check because it took > 10 s, is "> 10 s"; refusals are
    "refused (estimated > 10 s)"; other exclusions (error, unreached, no reference) and "not expressible" are listed, not valued;
  * configurations: g<n> Gnomon threads, d<n> DuckDB (SQL) threads, p<n> PostgreSQL max_parallel_workers_per_gather (p0 = no
    parallel workers = 1 process, p4 = 5 processes, p32 = 33 processes at most; the planner may plan fewer workers);
  * matched pairs: 1 vs 1 (g1, d1, p0), 4 vs 4 (g4, d4, p4), 32 vs 32 (g32, d32, p32);
  * ratio = Gnomon / other, below 1 means Gnomon is faster. Over the cells both answered: median and geometric mean. Count
    "Gnomon faster": cells where both answered and Gnomon is lower, plus cells where only the other is "> 10 s" (a lower bound);
    cells where Gnomon is "> 10 s" and the other answered count against Gnomon; cells both "> 10 s" are not compared.
Writes summary.json and tables.md.
"""
from __future__ import annotations

import argparse
import json
import math
import statistics as st
from collections import defaultdict
from pathlib import Path

CFGS = ["g1", "g4", "g32", "d1", "d4", "d32", "d64", "p0", "p4", "p32"]
SYS = {"g": "native", "d": "duckdbsql", "p": "postgres"}
NAME = {"g": "Gnomon", "d": "DuckDB (SQL)", "p": "PostgreSQL"}
PAIRS = {1: ("g1", "d1", "p0"), 4: ("g4", "d4", "p4"), 32: ("g32", "d32", "p32")}
FAM = {"P": ("hist.single", "hist.asof"), "T": ("snap.hop2", "reach.window", "paths.k"),
       "S": ("series.count", "burst.zscore", "agg.rel_bucket"), "W": ("series.count.w10", "burst.zscore.w10", "agg.rel_bucket.w10"),
       "D": ("diff.global",), "N": ("nbr.evolution", "coactive.narrow"), "R": ("resolve.substr",), "M": ("motif.filtered",),
       "B": ("b.snap.hop2@T", "b.snap.hop2@now", "b.reach@T", "b.reach@now", "b.diff", "b.diff@ctl", "b.hist", "b.hist@now")}
FAM_OF = {q: f for f, qs in FAM.items() for q in qs}
OK, CENS, REF, EXC, NX, MISS = "ok", "> 10 s", "refused", "excluded", "not expressible", "missing"


def gm(xs):
    xs = [x for x in xs if x and x > 0]
    return math.exp(sum(map(math.log, xs)) / len(xs)) if xs else None


def fmt(v):
    if v is None:
        return "-"
    if isinstance(v, str):
        return v
    return f"{v:.3g}" if v < 100 else f"{v:,.0f}"


def key(c):
    return f"{c['query']}#{c.get('instance')}"


def load_cells(d: Path, rot: str, sysname: str) -> dict:
    out = {}
    for f in sorted((d / f"rot{rot}").glob(f"*-{sysname}.json")):
        for c in json.loads(f.read_text()).get("results", []):
            out[key(c)] = c
    return out


def cell_class(c):
    if c is None:
        return MISS, None
    if c.get("ok"):
        return OK, c.get("p50_ms")
    err = str(c.get("error", ""))
    if c.get("excluded"):
        if "refused" in err:
            return REF, None
        if "> 10 s" in err:
            return CENS, None
        return EXC, None
    if c.get("aborted") or err.startswith(">") or "client-side limit" in err:
        return CENS, None
    if err.startswith(("unsupported", "NotImplemented", "not written")):
        return NX, None
    if c.get("refused") or "E_COST" in err:
        return REF, None
    return EXC, None


def instance_value(cfg: str, d: Path, rots=("1", "2")):
    """{key: (class, value ms or None, stratum, query)} for one configuration of one input."""
    sysname = SYS[cfg[0]]
    recs = {r: load_cells(d, r, sysname) for r in rots}
    keys = set().union(*[set(x) for x in recs.values()])
    out = {}
    for k in keys:
        cs = [cell_class(recs[r].get(k)) for r in rots]
        cl = [c for c, _ in cs]
        any_c = next((recs[r][k] for r in rots if k in recs[r]), {})
        stratum, q = any_c.get("stratum"), any_c.get("query")
        if all(c == OK for c in cl):
            out[k] = (OK, sum(v for _, v in cs) / len(cs), stratum, q)
        else:
            for want in (CENS, REF, NX, EXC):
                if want in cl:
                    out[k] = (want, None, stratum, q)
                    break
            else:
                ok = [v for c, v in cs if c == OK]
                out[k] = (OK, sum(ok) / len(ok), stratum, q) if ok else (MISS, None, stratum, q)
    return out


def engine_rows(cfg: str, d: Path):
    out = {}
    for k, c in load_cells(d, "E", SYS[cfg[0]]).items():
        es = ((c.get("engine_time") or {}).get("summary")) if c.get("ok") else None
        if es and c.get("p50_ms"):
            out[k] = dict(e2e=c["p50_ms"], **{x: es.get(x) for x in ("engine_ms_p50", "front_ms_p50", "exec_ms_p50", "delivery_ms_p50")})
    return out


def compare(g, o):
    """Class of the pair (Gnomon cell g, other cell o) -> (ratio or None, verdict)."""
    gc, gv = g[0], g[1]
    oc, ov = o[0], o[1]
    if gc == OK and oc == OK:
        return gv / ov, ("faster" if gv < ov else "slower")
    if gc == OK and oc in (CENS, REF):
        return None, "faster"           # the other is above the ceiling: a lower bound of Gnomon's lead
    if gc in (CENS, REF) and oc == OK:
        return None, "slower"
    return None, None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--timed", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    a = ap.parse_args()
    a.out.mkdir(parents=True, exist_ok=True)
    md = ["# Thread-matched control: tables", "", "Ratios are Gnomon / other (below 1: Gnomon faster). Times are end-to-end, ms.", ""]
    js: dict = {}
    for wl in ("w1", "w2"):
        wdir = a.timed / wl
        if not wdir.is_dir():
            continue
        tags = sorted({p.name for c in wdir.glob("cfg-*") for p in c.iterdir() if p.is_dir()})
        for tag in tags:
            val = {c: instance_value(c, wdir / f"cfg-{c}" / tag) for c in CFGS if (wdir / f"cfg-{c}" / tag).is_dir()}
            if not val:
                continue
            allk = sorted(set().union(*[set(v) for v in val.values()]))
            title = f"{wl} {tag}"
            md += [f"## {title}: {len(allk)} instances", "",
                   "| config | answered | > 10 s | refused | other excluded | not expressible | median e2e over answered | geomean over cells every config of its system answered |",
                   "|---|---:|---:|---:|---:|---:|---:|---:|"]
            common = {}
            for s in "gdp":
                cs = [c for c in CFGS if c[0] == s and c in val]
                common[s] = [k for k in allk if cs and all(val[c].get(k, (MISS,))[0] == OK for c in cs)]
            for c in CFGS:
                if c not in val:
                    continue
                v = val[c]
                oks = [x[1] for x in v.values() if x[0] == OK]
                cnt = lambda w: sum(1 for x in v.values() if x[0] == w)
                md.append(f"| {c} | {len(oks)} | {cnt(CENS)} | {cnt(REF)} | {cnt(EXC) + cnt(MISS)} | {cnt(NX)} | {fmt(st.median(oks) if oks else None)} | "
                          f"{fmt(gm([v[k][1] for k in common[c[0]]]))} (n={len(common[c[0]])}) |")
            md.append("")
            # thread scaling of each system on its common cells
            md += ["Thread scaling on the cells every configuration of the system answered (geometric mean of the per-cell ratio of times):", ""]
            for s, base, lows in (("g", "g32", ("g1", "g4")), ("d", "d32", ("d1", "d4", "d64")), ("p", "p4", ("p0", "p32"))):
                for c in lows:
                    if c in val and base in val:
                        r = [val[c][k][1] / val[base][k][1] for k in common[s]]
                        md.append(f"- {NAME[s]} {c} / {base}: {fmt(gm(r))} over {len(r)} cells")
            md.append("")
            # matched pairs
            for strata_name, sel in (("all instances", lambda s: True), ("hub", lambda s: s == "hub"), ("non-hub", lambda s: s != "hub")):
                if strata_name != "all instances" and wl != "w2":
                    continue
                md += [f"### {title}: matched pairs, {strata_name}", "",
                       "| level | other | cells both answered | median ratio | geomean ratio | Gnomon faster | Gnomon slower | not compared |", "|---|---|---:|---:|---:|---:|---:|---:|"]
                for lvl, (g, d_, p_) in PAIRS.items():
                    for oth in (d_, p_):
                        if g not in val or oth not in val:
                            continue
                        rat, fast, slow, nc = [], 0, 0, 0
                        for k in allk:
                            gk, ok_ = val[g].get(k), val[oth].get(k)
                            if gk is None or ok_ is None or not sel(gk[2]):
                                continue
                            r, vd = compare(gk, ok_)
                            if r is not None:
                                rat.append(r)
                            fast += vd == "faster"
                            slow += vd == "slower"
                            nc += vd is None
                        js.setdefault(title, {}).setdefault(strata_name, {})[f"{g}/{oth}"] = dict(ratios=len(rat), faster=fast, slower=slow)
                        md.append(f"| {lvl} vs {lvl} | {NAME[oth[0]]} {oth} | {len(rat)} | {fmt(st.median(rat) if rat else None)} | {fmt(gm(rat))} | {fast} | {slow} | {nc} |")
                md.append("")
            # per family, matched pairs
            md += [f"### {title}: geometric-mean ratio per family", "", "| family | " + " | ".join(f"{g} / {o}" for lv, (g, d_, p_) in PAIRS.items() for o in (d_, p_)) + " |",
                   "|---|" + "---:|" * 6]
            fams = sorted({FAM_OF.get(x[3], "?") for x in val[next(iter(val))].values()})
            for fam in fams:
                row = []
                for lvl, (g, d_, p_) in PAIRS.items():
                    for oth in (d_, p_):
                        r = []
                        if g in val and oth in val:
                            for k in allk:
                                gk, ok_ = val[g].get(k), val[oth].get(k)
                                if gk and ok_ and FAM_OF.get(gk[3]) == fam:
                                    x, _ = compare(gk, ok_)
                                    if x is not None:
                                        r.append(x)
                        row.append(f"{fmt(gm(r))} (n={len(r)})")
                md.append(f"| {fam} | " + " | ".join(row) + " |")
            md.append("")
            # unmatched comparison: the W1/W2 configurations
            if all(c in val for c in ("g32", "d32", "d64", "p4")):
                rb, rp = [], []
                for k in allk:
                    g = val["g32"].get(k)
                    ds = [val[c][k] for c in ("d32", "d64") if k in val[c] and val[c][k][0] == OK]
                    if g and g[0] == OK and ds:
                        rb.append(g[1] / min(x[1] for x in ds))
                    p = val["p4"].get(k)
                    if g and p and g[0] == OK and p[0] == OK:
                        rp.append(g[1] / p[1])
                md += [f"### {title}: the W1/W2 comparison (Gnomon 32 threads; DuckDB (SQL) the better of 32 and 64 per instance; PostgreSQL 4 workers)", "",
                       f"- Gnomon / DuckDB (SQL) better-of: median {fmt(st.median(rb) if rb else None)}, geomean {fmt(gm(rb))} over {len(rb)} cells",
                       f"- Gnomon / PostgreSQL p4: median {fmt(st.median(rp) if rp else None)}, geomean {fmt(gm(rp))} over {len(rp)} cells", ""]
            # engine-time breakdown
            md += [f"### {title}: engine-time pass (median over the cells every configuration of the system answered)", "",
                   "| config | cells | e2e ms | engine ms | front ms | exec ms | delivery ms | engine / e2e |", "|---|---:|---:|---:|---:|---:|---:|---:|"]
            eng = {c: engine_rows(c, wdir / f"cfg-{c}" / tag) for c in CFGS if (wdir / f"cfg-{c}" / tag).is_dir()}
            for s in "gdp":
                cs = [c for c in CFGS if c[0] == s and c in eng]
                ks = sorted(set.intersection(*[set(eng[c]) for c in cs])) if cs else []
                for c in cs:
                    rows = [eng[c][k] for k in ks]
                    med = lambda f: st.median([r[f] for r in rows if r.get(f) is not None]) if any(r.get(f) is not None for r in rows) else None
                    sh = [r["engine_ms_p50"] / r["e2e"] for r in rows if r.get("engine_ms_p50") is not None and r["e2e"]]
                    md.append(f"| {c} | {len(rows)} | {fmt(med('e2e'))} | {fmt(med('engine_ms_p50'))} | {fmt(med('front_ms_p50'))} | {fmt(med('exec_ms_p50'))} | "
                              f"{fmt(med('delivery_ms_p50'))} | {fmt(st.median(sh) if sh else None)} |")
            md.append("")
    (a.out / "tables.md").write_text("\n".join(md) + "\n")
    (a.out / "summary.json").write_text(json.dumps(js, indent=1, sort_keys=True) + "\n")
    print(f"wrote {a.out}/tables.md ({len(md)} lines)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
