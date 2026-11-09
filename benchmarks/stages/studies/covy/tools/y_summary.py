#!/usr/bin/env python3
"""counts, answer comparisons, drift.

    y_summary.py --timed <timed results dir> --tags T,T --out <dir>

An instance's value is the mean over the rotations of each rotation's median timed call (p50_ms, end-to-end); a cell censored in either rotation, or
excluded at the answer check as above the ceiling, is "> 10 s"; a refusal has no time and enters no ratio. Ratio = Gnomon's value / the named system's
value for the same instance in this job (Neo4j, Memgraph, DuckDB (SQL) 64 separately); per query the median over the instances with a ratio, with counts
of the instances that have none. Drift: this job's value over the system's earlier value of the same instance on the same log bytes: families P..M
against the October K10M records (store K10M, without the belief-axis indexes), family B against the October K10M-d0 / K10M-d10 records (same store archive).
"""
from __future__ import annotations

import argparse
import json
import statistics as st
from collections import Counter, defaultdict
from pathlib import Path

E = Path(__import__("os").environ.get("GNOMON_BENCH_ROOT", ".") + "")
EARLIER = {"others": {"reg": E / "final/w2/results/timed-J24/p1/K10M", "B": E / "final/w2/results/timed-J25/p1"},
           "native": {"reg": E / "confirm/w2/results/timed-J46/p1/K10M", "B": E / "confirm/w2/results/timed-J46/p1"}}
REG = {}
for q in ("hist.single snap.hop2 reach.window paths.k series.count series.count.w10 burst.zscore burst.zscore.w10 agg.rel_bucket agg.rel_bucket.w10 diff.global nbr.evolution coactive.narrow motif.filtered").split():
    REG[q] = "T"
for q in ("resolve.substr resolve.exact resolve.prefix").split():
    REG[q] = "G"
for q in ("hist.asof diff.belief b.snap.hop2@T b.snap.hop2@now b.reach@T b.reach@now b.diff b.diff@ctl b.hist b.hist@now").split():
    REG[q] = "B"
BFAM = {"b.snap.hop2@T", "b.snap.hop2@now", "b.reach@T", "b.reach@now", "b.diff", "b.diff@ctl", "b.hist", "b.hist@now"}
CENS, REF, NA = "> 10 s", "refused", "n/a"
NAME = {"native": "Gnomon", "duckdbsql": "DuckDB (SQL) 64", "neo4j": "Neo4j", "memgraph": "Memgraph"}
SYS = ("native", "duckdbsql", "neo4j", "memgraph")
OTHERS = ("neo4j", "memgraph", "duckdbsql")


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
    if c.get("aborted") or c.get("censored") or e.startswith((">", "excluded: >")) or "client-side limit" in e or "> 10 s at answer check" in e:
        return CENS
    if "excluded" in e:
        return e[:60]
    return "error: " + e[:80]


def read_root(d: Path, rots=("1", "2")):
    out: dict = defaultdict(dict)
    for r in rots:
        rd = d / f"rot{r}"
        if not rd.is_dir():
            continue
        for f in sorted(rd.glob("*.json")):
            if "-" not in f.stem:
                continue
            rec = json.loads(f.read_text())
            s = rec.get("system")
            if not s or "results" not in rec:
                continue
            for c in rec["results"]:
                e = out[s].setdefault(key(c), {"query": c["query"], "vals": [], "st": None, "et": [], "hash": set(), "r10m": False})
                x = status(c)
                if c.get("r10m"):
                    e["r10m"] = True
                if x is None:
                    e["vals"].append(c["p50_ms"])
                    if c.get("hash"):
                        e["hash"].add((c.get("proj_hash"), c.get("rows")))
                    if c.get("engine_time", {}).get("summary"):
                        e["et"].append(dict(c["engine_time"]["summary"], e2e_p50=c["p50_ms"]))
                elif e["st"] is None or e["st"] == NA:
                    e["st"] = x
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
    if v is None:
        return "-"
    if not num(v):
        return str(v)
    return f"{v:.3g}" if v < 1000 else f"{v:,.0f}"


def table(head, rows):
    out = ["| " + " | ".join(head) + " |", "|" + "---|" * len(head)]
    return out + ["| " + " | ".join(str(x) for x in r) + " |" for r in rows]


def rss(path: Path):
    peak = {}
    for f in sorted(path.glob("rss-*.jsonl")):
        for l in f.read_text().splitlines():
            try:
                d = json.loads(l)
            except ValueError:
                continue
            for c, v in d["by"].items():
                p = peak.setdefault(c, {"rss_kb": 0, "hwm_kb": 0, "pss_kb": 0})
                for k in p:
                    p[k] = max(p[k], v.get(k, 0))
    return peak


def drift(S, tag, label):
    rows, dj = [], {}
    for s in SYS:
        grp = "native" if s == "native" else "others"
        old_reg = read_root(EARLIER[grp]["reg"]).get(s, {}) if tag == "K10M-d0" else {}
        old_b = read_root(EARLIER[grp]["B"] / tag).get(s, {})
        rs, both, cen_now, cen_old, n_b, n_reg = [], 0, 0, 0, 0, 0
        for k, e in S.get(s, {}).items():
            isb = e["query"] in BFAM
            o = (old_b if isb else old_reg).get(k)
            if o is None:
                continue
            vn, vo = value(e), value(o)
            if num(vn) and num(vo):
                rs.append(vn / vo); n_b += isb; n_reg += not isb
            elif num(vo):
                cen_now += 1
            elif num(vn):
                cen_old += 1
            else:
                both += 1
        if rs:
            rs.sort()
            qt = lambda p: rs[min(len(rs) - 1, int(p * len(rs)))]
            dj[s] = {"n": len(rs), "n_P..M": n_reg, "n_B": n_b, "median": st.median(rs), "p10": qt(0.1), "p90": qt(0.9), "min": rs[0], "max": rs[-1],
                     "censored_now_only": cen_now, "censored_then_only": cen_old, "both_censored": both, "ratios": rs}
            rows.append([NAME[s], len(rs), n_reg, n_b, f"{dj[s]['median']:.3f}", f"{dj[s]['p10']:.3f}", f"{dj[s]['p90']:.3f}", f"{rs[0]:.3f}", f"{rs[-1]:.3f}", cen_now, cen_old, both])
        else:
            rows.append([NAME[s], 0, 0, 0, "-", "-", "-", "-", "-", cen_now, cen_old, both])
    md = [f"## Drift at {tag} against the earlier records (now ÷ then, same instance){label}", "",
          "Earlier: families P..M the October K10M records (store K10M, same log bytes; K10M-d0 carries the belief-axis indexes), family B the October "
          f"{tag} records (same store archive). Gnomon's earlier records are the C4 confirmation (confirm, timed-J46).", ""]
    md += table(["configuration", "instances", "of which P..M", "of which B", "median", "p10", "p90", "min", "max", "> 10 s now only", "> 10 s then only", "> 10 s both"], rows) + [""]
    return md, dj


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--timed", type=Path, required=True); ap.add_argument("--tags", required=True); ap.add_argument("--out", type=Path, required=True)
    a = ap.parse_args()
    a.out.mkdir(parents=True, exist_ok=True)
    tags = a.tags.split(",")
    md, J = [f"# Summary {a.timed}", "", "Descriptive. End-to-end time is primary. Times in ms. Deterministic 10 % sample (params-w2s, tools/y_params.py).", ""], {}
    for tag in tags:
        d = a.timed / "reg" / tag
        if not d.is_dir():
            md += [f"## {tag}", "", "no records", ""]; continue
        S = {s: dict(v) for s, v in read_root(d).items()}
        rows = []
        for s in SYS:
            c = Counter()
            for e in S.get(s, {}).values():
                v = value(e)
                c["ok" if num(v) else str(v)] += 1
                if e["r10m"]:
                    c["of which R-10M"] += 1
            rows.append([NAME[s], c.get("ok", 0), c.get(CENS, 0), c.get(REF, 0), c.get(NA, 0), c.get("of which R-10M", 0), {k: v for k, v in c.items() if k not in ("ok", CENS, REF, NA, "of which R-10M")} or ""])
        md += [f"## {tag}", "", "### Cells", ""] + table(["configuration", "ok", "> 10 s", "refused", "n/a", "of which R-10M", "other"], rows) + [""]
        for f in sorted((d / "rot1").glob("*-duckdbsql.json"))[:1]:
            st_ = json.loads(f.read_text()).get("settings", {})
            md += [f"DuckDB (SQL) settings: memory_limit {st_.get('memory_limit')}, threads {st_.get('threads')}, version {st_.get('duckdb')}.", ""]
        qs = defaultdict(list)
        for s in SYS:
            for k, e in S.get(s, {}).items():
                if k not in qs[e["query"]]:
                    qs[e["query"]].append(k)
        Q, rows = {}, []
        for q in sorted(qs, key=lambda x: (REG.get(x, "?"), x)):
            ent = {"class": REG.get(q, "?"), "instances": len(qs[q])}
            r = [ent["class"], q, len(qs[q])]
            for s in SYS:
                vs = [value(S[s][k]) for k in qs[q] if k in S.get(s, {})]
                nv = [v for v in vs if num(v)]
                ent[f"{s}_median_ms"] = st.median(nv) if nv else None
                ent[f"{s}_counts"] = dict(Counter("ok" if num(v) else str(v) for v in vs))
                r.append(f"{fmt(ent[f'{s}_median_ms'])} ({len(nv)}/{len(vs)})")
            for o in OTHERS:
                rat, gc, oc, bc = [], 0, 0, 0
                for k in qs[q]:
                    g = value(S["native"][k]) if k in S.get("native", {}) else None
                    x = value(S[o][k]) if k in S.get(o, {}) else None
                    if num(g) and num(x):
                        rat.append(g / x)
                    elif num(g) and x == CENS:
                        oc += 1
                    elif g == CENS and num(x):
                        gc += 1
                    elif g == CENS and x == CENS:
                        bc += 1
                ent[f"ratio_{o}"] = {"median": st.median(rat) if rat else None, "n": len(rat), "min": min(rat) if rat else None, "max": max(rat) if rat else None,
                                     "other_censored_gnomon_ok": oc, "gnomon_censored_other_ok": gc, "both_censored": bc}
                r.append((f"{st.median(rat):.3g}" if rat else "-") + f" [{len(rat)}; {oc}/{gc}/{bc}]")
            Q[q] = ent; rows.append(r)
        md += ["### Values and Gnomon ÷ other per query", "",
               "Value cells: median over instances of the instance value (answered/instances). Ratio cells: median [instances with a ratio; other > 10 s with Gnomon answered / Gnomon > 10 s with other answered / both > 10 s].", ""]
        md += table(["class", "query", "instances", "Gnomon", "DuckDB (SQL) 64", "Neo4j", "Memgraph", "÷ Neo4j", "÷ Memgraph", "÷ DuckDB (SQL) 64"], rows) + [""]
        # per class
        rows = []
        for cl in ("T", "B", "G"):
            r = [cl]
            for o in OTHERS:
                rat = []
                for q, ent in Q.items():
                    if ent["class"] != cl:
                        continue
                    for k in qs[q]:
                        g = value(S["native"][k]) if k in S.get("native", {}) else None
                        x = value(S[o][k]) if k in S.get(o, {}) else None
                        if num(g) and num(x):
                            rat.append(g / x)
                r.append(f"{st.median(rat):.3g} (n {len(rat)}, Gnomon faster on {sum(1 for x in rat if x < 1)})" if rat else "-")
            rows.append(r)
        md += ["### Per class, instance ratios Gnomon ÷ other", ""] + table(["class", "÷ Neo4j", "÷ Memgraph", "÷ DuckDB (SQL) 64"], rows) + [""]
        J[tag] = {"queries": Q}
        cmp_, bad = Counter(), []
        for s in OTHERS:
            for k, e in S.get(s, {}).items():
                g = S.get("native", {}).get(k)
                if not g or not g["hash"] or not e["hash"]:
                    cmp_[f"{s} not compared"] += 1; continue
                if {x[0] for x in e["hash"]} == {x[0] for x in g["hash"]}:
                    cmp_[f"{s} equal"] += 1
                else:
                    cmp_[f"{s} DIFFERS"] += 1; bad.append((s, k, sorted(map(str, e["hash"])), sorted(map(str, g["hash"]))))
        md += ["### Timed answers against Gnomon's in this job (projected hash)", "", str(dict(cmp_)), ""]
        if bad:
            md += ["ANSWERS DIFFER IN THE TIMED RECORDS:", str(bad[:20]), ""]
        J[tag]["timed_answers_differ"] = bad
        EP = read_root(d, rots=("E",))
        agg = defaultdict(lambda: defaultdict(list))
        for s, m in EP.items():
            for k, e in m.items():
                for x in e["et"]:
                    agg[e["query"]][s].append((x.get("e2e_p50"), x.get("engine_ms_p50")))
        for s in ("neo4j", "memgraph"):
            for k, e in S.get(s, {}).items():
                for x in e["et"]:
                    agg[e["query"]][s].append((x.get("e2e_p50"), x.get("engine_ms_p50")))
        rows = []
        for q in sorted(agg, key=lambda x: (REG.get(x, "?"), x)):
            r = [q]
            for s in SYS:
                v = [x for x in agg[q].get(s, []) if x[0] is not None and x[1] is not None]
                r.append(f"{fmt(st.median(x[0] for x in v))} / {fmt(st.median(x[1] for x in v))}" if v else "-")
            rows.append(r)
        md += ["### End-to-end / engine time, median per query (Gnomon and DuckDB (SQL) from the engine pass; Neo4j and Memgraph from their Bolt summaries in the timed rotations)", ""]
        md += table(["query", "Gnomon", "DuckDB (SQL) 64", "Neo4j", "Memgraph"], rows) + [""]
        if tag == "K10M-d0":
            m, dj = drift(S, tag, ""); md += m; J["drift_d0"] = dj
        else:
            m, dj = drift(S, tag, " (family B only)"); md += m; J[f"drift_{tag}"] = dj
    peak = rss(a.timed)
    md += ["## Peak memory of this job, whole node, by process category (GB)", ""] + table(["category", "sum RSS", "largest VmHWM", "sum PSS"], [[c, f"{v['rss_kb'] / 1e6:.1f}", f"{v['hwm_kb'] / 1e6:.1f}", f"{v['pss_kb'] / 1e6:.1f}"] for c, v in sorted(peak.items())]) + [""]
    J["peak_memory_kb"] = peak
    (a.out / "summary.md").write_text("\n".join(md) + "\n")
    (a.out / "summary.json").write_text(json.dumps(J, indent=1, sort_keys=True, default=list) + "\n")
    print(f"summary {a.out / 'summary.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
