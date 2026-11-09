#!/usr/bin/env python3
"""A plain summary of the t10 and k100m timed records. No forecasts, no verdict words: measured ratios, censored and refused counts, answer comparisons.

    x_summary.py --set w1|w2|k100m --timed <timed results dir> --tags T,T --out <dir>

Rules as the earlier W1 and W2 summaries: an instance's value is the mean over the rotations of each rotation's median timed call (p50_ms, end-to-end); a cell censored
in either rotation, or excluded at the answer check as above the ceiling, is "> 10 s"; a refusal has no time and enters no ratio; DuckDB (SQL) is the better of 64 and 32
threads per instance, both reported. Ratio = Gnomon's value / the smallest value among DuckDB (SQL), ClickHouse, PostgreSQL for the same instance; per query the median over
Drift: this job's value at K10M-d0 over the system's earlier value of the same instance on the same log bytes (store K10M, without the belief-axis indexes).
"""
from __future__ import annotations

import argparse
import json
import statistics as st
from collections import Counter, defaultdict
from pathlib import Path

E = Path(__import__("os").environ.get("GNOMON_BENCH_ROOT", ".") + "")
EARLIER = {"w1": {"others": (E / "final/w1/results/timed-J20/sp/K10M", E / "final/w1/results/timed-J20/t32sp/K10M"), "native": E / "confirm/w1/results/timed-J43/sp/K10M"},
           "w2": {"others": (E / "final/w2/results/timed-J24/p1/K10M", E / "final/w2/results/timed-J24/t32p1/K10M"), "native": E / "confirm/w2/results/timed-J46/p1/K10M"}}
REG = {}
for q in ("hist.single snap.hop2 reach.window paths.k series.count series.count.w10 burst.zscore burst.zscore.w10 agg.rel_bucket agg.rel_bucket.w10 diff.global nbr.evolution coactive.narrow motif.filtered").split():
    REG[q] = "T"
for q in ("resolve.substr resolve.exact resolve.prefix").split():
    REG[q] = "G"
for q in ("hist.asof diff.belief b.snap.hop2@T b.snap.hop2@now b.reach@T b.reach@now b.diff b.diff@ctl b.hist b.hist@now").split():
    REG[q] = "B"
CENS, REF, NA = "> 10 s", "refused by Gnomon as shipped", "n/a"
NAME = {"native": "Gnomon", "duckdbsql": "DuckDB (SQL)", "d64": "DuckDB (SQL) 64", "d32": "DuckDB (SQL) 32", "clickhouse": "ClickHouse", "postgres": "PostgreSQL"}
OTHERS = ("duckdbsql", "clickhouse", "postgres")


def key(c):
    return f"{c['query']}#{c['instance']}" if c.get("instance") else c["query"]


def status(c):
    if c.get("ok"):
        return None
    e = str(c.get("error", ""))
    if e.startswith(("unsupported", "NotImplemented", "not written")) or "not expressible" in e:
        return NA
    if c.get("refused") or "E_COST" in e or "CostError: estimated cost" in e:     # the kit's raw records say refused: false for this string; they stay as they are
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


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--set", required=True); ap.add_argument("--timed", type=Path, required=True); ap.add_argument("--tags", required=True); ap.add_argument("--out", type=Path, required=True)
    a = ap.parse_args()
    a.out.mkdir(parents=True, exist_ok=True)
    tags = a.tags.split(",")
    md, J = [f"# Summary {a.timed}", "", "Descriptive. End-to-end time is primary. Times in ms.", ""], {}
    per_tag = {}
    for tag in tags:
        d = a.timed / "reg" / tag
        if not d.is_dir():
            md += [f"## {tag}", "", "no records", ""]; continue
        main_ = read_root(d)
        t32 = read_root(a.timed / "t32reg" / tag)
        S = {s: dict(v) for s, v in main_.items()}
        S["d64"] = S.pop("duckdbsql", {})
        S["d32"] = t32.get("duckdbsql", {})
        best = {}
        for k in set(S["d64"]) | set(S["d32"]):
            x, y = S["d64"].get(k), S["d32"].get(k)
            cand = [(value(e), e) for e in (x, y) if e and num(value(e))]
            best[k] = min(cand, key=lambda t: t[0])[1] if cand else (x or y)
        S["duckdbsql"] = best
        per_tag[tag] = S
        # counts
        rows = []
        for s in ("native", "d64", "d32", "clickhouse", "postgres"):
            c = Counter()
            for e in S.get(s, {}).values():
                v = value(e)
                c["ok" if num(v) else str(v)] += 1
                if e["r10m"]:
                    c["of which R-10M"] += 1
            rows.append([NAME[s], c.get("ok", 0), c.get(CENS, 0), c.get(REF, 0), c.get(NA, 0), c.get("of which R-10M", 0), {k: v for k, v in c.items() if k not in ("ok", CENS, REF, NA, "of which R-10M")} or ""])
        md += [f"## {tag}", "", "### Cells", ""] + table(["configuration", "ok", "> 10 s", "refused", "n/a", "of which R-10M", "other"], rows) + [""]
        # settings
        for f in sorted((d / "rot1").glob("*-duckdbsql.json"))[:1]:
            st_ = json.loads(f.read_text()).get("settings", {})
            md += [f"DuckDB (SQL) settings: memory_limit {st_.get('memory_limit')}, threads {st_.get('threads')}, version {st_.get('duckdb')}.", ""]
        # ratios per query
        qs = defaultdict(list)
        for k, e in S.get("native", {}).items():
            qs[e["query"]].append(k)
        Q = {}
        rows = []
        for q in sorted(qs, key=lambda x: (REG.get(x, "?"), x)):
            ratio, g_c, g_r, o_c, both_c, gv, ov, who = [], 0, 0, 0, 0, [], [], Counter()
            for k in qs[q]:
                g = value(S["native"][k])
                vals = {s: value(S[s][k]) for s in OTHERS if k in S.get(s, {})}
                nv = {s: v for s, v in vals.items() if num(v)}
                if g == REF:
                    g_r += 1
                if num(g):
                    gv.append(g)
                if num(g) and nv:
                    b = min(nv, key=nv.get)
                    ratio.append(g / nv[b]); who[b] += 1; ov.append(nv[b])
                elif num(g) and any(v == CENS for v in vals.values()):
                    o_c += 1
                elif g == CENS and nv:
                    g_c += 1; ov.append(min(nv.values()))
                elif g == CENS:
                    both_c += 1
            Q[q] = {"class": REG.get(q, "?"), "instances": len(qs[q]), "ratio_median": st.median(ratio) if ratio else None, "ratio_n": len(ratio), "gnomon_median_ms": st.median(gv) if gv else None,
                    "best_other_median_ms": st.median(ov) if ov else None, "best_other": dict(who), "gnomon_censored_other_ok": g_c, "other_all_censored_gnomon_ok": o_c, "both_censored": both_c, "gnomon_refused": g_r}
            rows.append([Q[q]["class"], q, len(qs[q]), fmt(Q[q]["gnomon_median_ms"]), fmt(Q[q]["best_other_median_ms"]), who.most_common(1)[0][0] if who else "-", (f"{Q[q]['ratio_median']:.3g}" if ratio else "-"), len(ratio), o_c, g_c, both_c, g_r])
        refused_rows = []
        for q in sorted(qs, key=lambda x: (REG.get(x, "?"), x)):
            for k in qs[q]:
                if value(S["native"][k]) != REF:
                    continue
                gc = {}
                gdir = a.timed / "reg" / tag
                for s in OTHERS:
                    for f in sorted(gdir.glob(f"verify-{s}-*.json")):
                        for c in json.loads(f.read_text()).get("results", []):
                            if key(c) == k:
                                gc[s] = c
                gc32 = {}
                for f in sorted((a.timed / "t32reg" / tag).glob("verify-duckdbsql-*.json")):
                    for c in json.loads(f.read_text()).get("results", []):
                        if key(c) == k:
                            gc32 = c
                def chk(c):
                    if not c:
                        return "no check"
                    if c.get("ok"):
                        return f"{fmt(c.get('wall_ms'))}" + (" (above 10 s)" if c.get("wall_ms", 0) > 10_000 else "")
                    return "client limit" if "client-side limit" in str(c.get("error")) else "> 10 s"
                ph = {gc[s].get("proj_hash") for s in gc if gc[s].get("ok")} | ({gc32.get("proj_hash")} if gc32.get("ok") else set())
                def tv(s):
                    v = value(S[s][k]) if k in S.get(s, {}) else None
                    return fmt(v)
                refused_rows.append([REG.get(q, "?"), k, tv("d64"), tv("d32"), tv("clickhouse"), tv("postgres"), chk(gc.get("duckdbsql")), chk(gc32), chk(gc.get("clickhouse")), chk(gc.get("postgres")), f"{sum(1 for s in gc if gc[s].get('ok')) + (1 if gc32.get('ok') else 0)} answers, {len(ph)} distinct"])
        if refused_rows:
            md += ["### Refused by Gnomon as shipped (a loss to Gnomon on that query; no Gnomon time, no ratio)", "", "Times of the other systems on these calls: no Gnomon answer. Check columns: the answer check's wall, untimed.", ""] + table(["class", "call", "DuckDB (SQL) 64", "DuckDB (SQL) 32", "ClickHouse", "PostgreSQL", "check DuckDB 64", "check DuckDB 32", "check ClickHouse", "check PostgreSQL", "others' mutual agreement at the gate"], refused_rows) + [""]
        md += ["### Gnomon ÷ best other, per query", ""] + table(["class", "query", "instances", "Gnomon median", "best other median", "mostly", "ratio median", "with ratio", "others all > 10 s", "Gnomon > 10 s, other answers", "all > 10 s", "Gnomon refused"], rows) + [""]
        J[tag] = {"queries": Q}
        # answers in the timed records against Gnomon's: the projected hash (the kit's verdict without vid, tt_s, tt_e)
        cmp_ = Counter(); bad = []
        for s in ("d64", "d32", "clickhouse", "postgres"):
            for k, e in S.get(s, {}).items():
                g = S.get("native", {}).get(k)
                if not g or not g["hash"] or not e["hash"]:
                    cmp_[f"{s} not compared"] += 1; continue
                if {x[0] for x in e["hash"]} == {x[0] for x in g["hash"]}:
                    cmp_[f"{s} equal"] += 1
                else:
                    cmp_[f"{s} DIFFERS"] += 1; bad.append((s, k, sorted(map(str, e["hash"])), sorted(map(str, g["hash"]))))
        md += ["### Timed answers against Gnomon's in this job", "", str(dict(cmp_)), ""]
        if bad:
            md += ["ANSWERS DIFFER IN THE TIMED RECORDS:", str(bad[:20]), ""]
        J[tag]["timed_answers_differ"] = bad
        # engine pass
        EP = read_root(d, rots=("E",))
        agg = defaultdict(lambda: defaultdict(list))
        for s, m in EP.items():
            for k, e in m.items():
                for x in e["et"]:
                    agg[e["query"]][s].append((x.get("e2e_p50"), x.get("engine_ms_p50")))
        for k, e in S.get("clickhouse", {}).items():
            for x in e["et"]:
                agg[e["query"]]["clickhouse"].append((x.get("e2e_p50"), x.get("engine_ms_p50")))
        rows = []
        for q in sorted(agg):
            r = [q]
            for s in ("native", "duckdbsql", "clickhouse", "postgres"):
                v = [x for x in agg[q].get(s, []) if x[0] is not None and x[1] is not None]
                r.append(f"{fmt(st.median(x[0] for x in v))} / {fmt(st.median(x[1] for x in v))}" if v else "-")
            rows.append(r)
        md += ["### End-to-end / engine time, median per query (engine pass; ClickHouse from its probes in the timed rotations)", ""] + table(["query", "Gnomon", "DuckDB (SQL)", "ClickHouse", "PostgreSQL"], rows) + [""]
    # d0 to d10
    if "K10M-d0" in J and "K10M-d10" in J:
        rows = []
        for q in sorted(J["K10M-d10"]["queries"], key=lambda x: (REG.get(x, "?"), x)):
            a0, a1 = J["K10M-d0"]["queries"].get(q, {}), J["K10M-d10"]["queries"][q]
            g0, g1 = a0.get("gnomon_median_ms"), a1.get("gnomon_median_ms")
            rows.append([a1["class"], q, fmt(a0.get("ratio_median")), fmt(a1.get("ratio_median")), fmt(g0), fmt(g1), (f"{g1 / g0:.3g}" if g0 and g1 else "-"), fmt(a0.get("best_other_median_ms")), fmt(a1.get("best_other_median_ms"))])
        md += ["## d0 and d10 side by side", ""] + table(["class", "query", "ratio d0", "ratio d10", "Gnomon d0", "Gnomon d10", "Gnomon d10 ÷ d0", "best other d0", "best other d10"], rows) + [""]
    # drift
    if a.set in EARLIER and "K10M-d0" in per_tag:
        S = per_tag["K10M-d0"]
        old_main = read_root(EARLIER[a.set]["others"][0]); old_t32 = read_root(EARLIER[a.set]["others"][1]); old_nat = read_root(EARLIER[a.set]["native"])
        rows, dj = [], {}
        for s, old in (("clickhouse", old_main.get("clickhouse", {})), ("postgres", old_main.get("postgres", {})), ("d64", old_main.get("duckdbsql", {})), ("d32", old_t32.get("duckdbsql", {})), ("native", old_nat.get("native", {}))):
            rs, both, cen_now, cen_old = [], 0, 0, 0
            for k, e in S.get(s, {}).items():
                o = old.get(k)
                if o is None:
                    continue
                vn, vo = value(e), value(o)
                if num(vn) and num(vo):
                    rs.append(vn / vo)
                elif num(vo):
                    cen_now += 1
                elif num(vn):
                    cen_old += 1
                else:
                    both += 1
            if rs:
                rs.sort()
                qt = lambda p: rs[min(len(rs) - 1, int(p * len(rs)))]
                dj[s] = {"n": len(rs), "median": st.median(rs), "p10": qt(0.1), "p90": qt(0.9), "min": rs[0], "max": rs[-1], "censored_now_only": cen_now, "censored_then_only": cen_old, "both_censored": both}
                rows.append([NAME[s], len(rs), f"{dj[s]['median']:.3f}", f"{dj[s]['p10']:.3f}", f"{dj[s]['p90']:.3f}", f"{rs[0]:.3f}", f"{rs[-1]:.3f}", cen_now, cen_old, both])
            else:
                rows.append([NAME[s], 0, "-", "-", "-", "-", "-", cen_now, cen_old, both])
        md += ["## Drift at K10M-d0 against the earlier records (now ÷ then, same instance)", "", "Earlier: store K10M, this job: store K10M-d0, same log bytes; K10M-d0 carries the belief-axis indexes. Gnomon's earlier record is the C4 confirmation; the others' are the October records.", ""] + table(["configuration", "instances", "median", "p10", "p90", "min", "max", "> 10 s now only", "> 10 s then only", "> 10 s both"], rows) + [""]
        J["drift"] = dj
    peak = rss(a.timed)
    md += ["## Peak memory of this job, whole node, by process category (GB)", "", "ClickHouse is in category other.", ""] + table(["category", "sum RSS", "largest VmHWM", "sum PSS"], [[c, f"{v['rss_kb'] / 1e6:.1f}", f"{v['hwm_kb'] / 1e6:.1f}", f"{v['pss_kb'] / 1e6:.1f}"] for c, v in sorted(peak.items())]) + [""]
    J["peak_memory_kb"] = peak
    (a.out / "summary.md").write_text("\n".join(md) + "\n")
    (a.out / "summary.json").write_text(json.dumps(J, indent=1, sort_keys=True, default=list) + "\n")
    print(f"summary {a.out / 'summary.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
