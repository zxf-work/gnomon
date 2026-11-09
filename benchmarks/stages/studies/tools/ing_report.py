#!/usr/bin/env python3
""""""
import glob, json, os, statistics as st, sys
R = sys.argv[1] if len(sys.argv) > 1 else "."
recs = {os.path.basename(p)[:-5]: json.load(open(p)) for p in sorted(glob.glob(R + "/*.json")) if not p.endswith("-digest.json") and "report" not in p}
dg = {os.path.basename(p)[:-12]: json.load(open(p)) for p in glob.glob(R + "/*-digest.json")}
ss = [json.loads(l) for l in open(R + "/rss-samples.jsonl") if l.strip().endswith("}")]
GB = 1048576
def p12(d):
    p1 = d.get("phase1_s", d.get("bulk_s")); p2 = d.get("phase2_s", d.get("transactions_s"))
    return p1, p2
def peak(d, part):
    m = d["marks"]; a, b = (m["start"], m.get("bulk_end", m["start"])) if part == 1 else (m.get("bulk_end", m["start"]), m["end"])
    w = [s for s in ss if a <= s["t"] <= b]
    if not w: return None
    rss = max(sum(c["rss_kb"] for c in s["by"].values()) for s in w) / GB
    pw = [sum(c["pss_kb"] for c in s["by"].values()) for s in w if s.get("pss")]
    cats = {}
    for s in w:
        for k, c in s["by"].items():
            cats[k] = max(cats.get(k, 0), c["rss_kb"])
    hwm = {k: max((s["by"][k]["hwm_kb"] for s in w if k in s["by"]), default=0) / GB for k in cats}
    return {"rss_gb": round(rss, 2), "pss_gb": round(max(pw) / GB, 2) if pw else None, "by_rss_gb": {k: round(v / GB, 2) for k, v in sorted(cats.items()) if v > GB / 10}, "hwm_gb": {k: round(v, 2) for k, v in sorted(hwm.items()) if v > 0.1}}
L = ["# Timed ingest study on C4: job J51 (protocol sha256 f27a7772...)", "", "## Phases (seconds) and size", "",
     "| run | phase 1 | phase 2 | load wall | day s, items | corrections s, items | bytes on disk | verified |", "|---|---:|---:|---:|---|---|---:|---|"]
for n, d in recs.items():
    p1, p2 = p12(d); bk = d.get("by_kind", {})
    f = lambda k: f"{bk[k]['s']}, {bk[k]['items']}" if k in bk else ""
    L.append(f"| {n} | {p1} | {p2} | {d.get('load_s', d.get('wall_incl_log_read_s'))} | {f('day')} | {f('corrections')} | {d.get('bytes_on_disk')} | {d.get('verified')} |")
L += ["", "## Gnomon, by part (seconds)", "", "| run | kind | event log | apply | commit | compaction | total | items/s | high-water GB |", "|---|---|---:|---:|---:|---:|---:|---:|---:|"]
for n, d in recs.items():
    if d.get("system") != "gnomon": continue
    for k, v in d["by_kind"].items():
        hw = max((b["hwm_compact_kb"] for b in d["per_batch"] if b["kind"] == k), default=0) / GB
        L.append(f"| {n} | {k} | {v['log_s']} | {v['apply_s']} | {v['commit_s']} | {v['compact_s']} | {v['s']} | {v['items_per_s']} | {hw:.2f} |")
    lc = d["log_check"]
    L.append(f"| {n} | log check | records {lc['written_records']} of {lc['input_records']}, items {lc['written_items']} of {lc['input_items']}, equal {lc['equal']}, bytes equal {lc['bytes_equal']} | | | | | | |")
L += ["", "## Other storage engines, phase detail", ""]
for n, d in recs.items():
    if d.get("system") == "gnomon": continue
    extra = {k: d[k] for k in ("prepare_s", "bulk_import_s", "start_index_s", "snapshot_s", "inserted", "closed", "transactions") if k in d}
    L.append(f"- {n}: {extra}")
L += ["", "## Peak memory (GB), over each run's own phases", "", "| run | phase 1 RSS sum | phase 1 PSS | phase 2 RSS sum | phase 2 PSS | RSS by process kind, phase 2 | VmHWM by kind |", "|---|---:|---:|---:|---:|---|---|"]
for n, d in recs.items():
    a, b = peak(d, 1), peak(d, 2)
    g = lambda x, k: x[k] if x else None
    L.append(f"| {n} | {g(a,'rss_gb')} | {g(a,'pss_gb')} | {g(b,'rss_gb')} | {g(b,'pss_gb')} | {g(b,'by_rss_gb')} | {g(b,'hwm_gb')} |")
med = lambda names, i: st.median(p12(recs[n])[i] for n in names)
gP = ["P1-gnomon-SF1-h1-r1", "P2-gnomon-SF1-h1-r1"]
g1, g2 = med(gP, 0), med(gP, 1)
d64 = (med(["P1-duckdb64", "P2-duckdb64"], 0), med(["P1-duckdb64", "P2-duckdb64"], 1)); d32 = (med(["P1b-duckdb32", "P2b-duckdb32"], 0), med(["P1b-duckdb32", "P2b-duckdb32"], 1))
pg = (med(["P1-postgres", "P2-postgres"], 0), med(["P1-postgres", "P2-postgres"], 1))
mg = (med(["P1-memgraph", "P2-memgraph"], 0), med(["P1-memgraph", "P2-memgraph"], 1)); nj = p12(recs["P1-neo4j"])
rate = lambda n, k: recs[n]["by_kind"][k]["s"] / recs[n]["by_kind"][k]["items"]
fi1 = st.median(rate(n, "corrections") / rate(n, "day") for n in gP)
fg2 = p12(recs["F-gnomon-SF1-h1-r1"])[1]
r0, r10 = recs["L-gnomon-SF1-h1-r0"], recs["L-gnomon-SF1-h1-r10"]
w = lambda d: p12(d)[0] + p12(d)[1]
S = {"F-I-1": {"ratio": round(fi1, 3), "note": "corrections s per item / day s per item, prefix, median of 2; ratio only"},
     "F-I-2": {"gnomon_phase2_s": g2, "duckdb64_s": d64[1], "duckdb32_s": d32[1], "better": min(d64[1], d32[1]), "ratio_gnomon_over_duckdb": round(g2 / min(d64[1], d32[1]), 3), "postgres_s": pg[1], "ratio_over_postgres": round(g2 / pg[1], 3)},
     "F-I-3": {"gnomon_phase2_s": g2, "memgraph_s": mg[1], "neo4j_s": nj[1], "ratio_over_better": round(g2 / min(mg[1], nj[1]), 3)},
     "F-I-4": {"gnomon_phase1_s": g1, "duckdb_better_s": min(d64[0], d32[0]), "postgres_s": pg[0], "ratio_over_duckdb": round(g1 / min(d64[0], d32[0]), 3), "ratio_over_postgres": round(g1 / pg[0], 3)},
     "F-I-5": {"full_phase2_s": fg2, "prefix_phase2_s": g2, "ratio": round(fg2 / (g2 * 6.2), 3), "note": "ratio only"},
     "F-W3-5": {"r0_load_s": round(w(r0), 1), "r10_load_s": round(w(r10), 1), "ratio_phases": round(w(r10) / w(r0), 3), "r0_wall_incl_log_read_s": r0["wall_incl_log_read_s"], "r10_wall_incl_log_read_s": r10["wall_incl_log_read_s"], "ratio_wall": round(r10["wall_incl_log_read_s"] / r0["wall_incl_log_read_s"], 3)}}
vd = {"F-I-2": "named loss holds" if g2 > min(d64[1], d32[1]) else "MISS: Gnomon faster than DuckDB (reported)",
      "F-I-3": "faster holds" if g2 < min(mg[1], nj[1]) else "FALSIFIED: Gnomon slower than the best graph system",
      "F-I-4": "named loss holds against both" if g1 > min(d64[0], d32[0]) and g1 > pg[0] else "MISS: Gnomon faster than " + ",".join(x for x, y in (("DuckDB", min(d64[0], d32[0])), ("PostgreSQL", pg[0])) if g1 <= y),
      "F-W3-5": ("inside <= 3x" if w(r10) / w(r0) <= 3 else "outside <= 3x") + "; falsifier > 10x " + ("not met" if w(r10) / w(r0) <= 10 else "MET"), "F-I-1": "no verdict", "F-I-5": "no verdict"}
L += ["", "## Scored as section 6 says", "", "| row | values | score |", "|---|---|---|"]
for k, v in S.items():
    L.append(f"| {k} | {json.dumps(v)} | {vd[k]} |")
# Section 3: Neo4j's and Memgraph's full-stream times, extrapolated from the prefix per kind
SQLP, SQLF = recs["P1-duckdb64"]["by_kind"], recs["F-duckdb64"]["by_kind"]
sc = {k: SQLF[k]["items"] / SQLP[k]["items"] for k in ("day", "corrections")}
L += ["", "## Neo4j and Memgraph full-stream phase 2: extrapolated, not measured", "",
      "Method (protocol section 3): per kind, the prefix's seconds per item × the full stream's items. A graph run counts version and relationship copies, which the full stream's records do not hold for these two systems, so the "
      "full stream's items are the prefix's graph items × the history tables' full ÷ prefix row ratio for that kind, read from the DuckDB (SQL) records: "
      f"day {sc['day']:.3f}, corrections {sc['corrections']:.3f}. This assumes the prefix's copies per row hold over the whole stream. It reduces to prefix seconds × that ratio. Memgraph is the median of 2 repetitions, Neo4j 1.", "",
      "| storage engine | kind | prefix s | prefix items | full-stream items (scaled) | full-stream s |", "|---|---|---:|---:|---:|---|"]
X = {}
for sys_, names in (("Memgraph", ["P1-memgraph", "P2-memgraph"]), ("Neo4j", ["P1-neo4j"])):
    tot = 0.0
    for k in ("day", "corrections"):
        ps = st.median(recs[n]["by_kind"][k]["s"] for n in names); it = recs[names[0]]["by_kind"][k]["items"]
        fi = it * sc[k]; fs = ps / it * fi; tot += fs
        L.append(f"| {sys_} | {k} | {ps} | {it} | {fi:.0f} | {fs:.0f} (extrapolated, not measured) |")
    L.append(f"| {sys_} | phase 2 total | | | | {tot:.0f} (extrapolated, not measured) |")
    X[sys_] = round(tot)
dst = st.median(recs[n]["load_s"] - recs[n]["transactions_s"] for n in ("P1-duckdb64", "P2-duckdb64", "P1b-duckdb32", "P2b-duckdb32"))
L += ["", "## DuckDB (SQL) phase 1", "", f"Two figures: {min(d64[0], d32[0])} s is its INSERT only, which is what phase 1 and F-I-4 above show; about {dst:.0f} s includes Parquet staging (load wall - phase 2), which falls before the phase mark. "
      f"Gnomon's phase 1 against them: {g1 / min(d64[0], d32[0]):.0f}× and {g1 / dst:.0f}×. The score is the same on either."]
S["F-I-4"]["duckdb_including_staging_s"] = round(dst, 1); S["F-I-4"]["ratio_over_duckdb_including_staging"] = round(g1 / dst, 1)
S["extrapolated_not_measured_full_stream_phase2_s"] = X
L += ["", "## Verification", "", "Baselines: `verified` above. Gnomon digests against the replayed stores:", ""] + [f"- {k}: equal {v['equal']} ({v['this_run'][:16]}, reference {os.path.basename(v['reference_path'])})" for k, v in sorted(dg.items())]
open(R + "/ingest-report.md", "w").write("\n".join(L) + "\n")
json.dump({"scores": S, "verdicts": vd}, open(R + "/ingest-report.json", "w"), indent=1)
print("\n".join(L))
