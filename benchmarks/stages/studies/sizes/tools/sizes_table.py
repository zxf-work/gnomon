#!/usr/bin/env python3
"""sizes_table.py sizes.jsonl raphtory-results-dir: du -sb per system and store, GB (1e9 bytes), and the stores that do not exist with the reason. Raphtory's sizes are read from its stage's gate job."""
import glob
import json
import sys

recs = [json.loads(x) for x in open(sys.argv[1]) if x.strip()]
rap = {}
for p in glob.glob(sys.argv[2] + "/gate-*/raphtory-du.jsonl"):
    for x in open(p):
        r = json.loads(x)
        rap[(r["wl"].upper(), r["tag"])] = r["du_sb"]
for (wl, tag), v in rap.items():
    recs.append({"workload": wl, "point": tag, "system": "raphtory", "du_sb": v, "du_blocks": None, "note": "Raphtory stage gate job", "source": "save_to_file"})
SYS = ["gnomon", "gnomon-c4-rebuild", "duckdbsql", "duckdb", "postgres", "clickhouse", "neo4j", "memgraph", "ladybug", "aeong", "xtdb", "raphtory"]
NAME = {"gnomon": "Gnomon (no event log, see below)", "gnomon-c4-rebuild": "Gnomon rebuilt on C4 (no event log)", "duckdbsql": "DuckDB (SQL)", "duckdb": "Gnomon on DuckDB storage (configuration)", "postgres": "PostgreSQL data directory", "clickhouse": "ClickHouse", "neo4j": "Neo4j", "memgraph": "Memgraph", "ladybug": "LadybugDB", "aeong": "AeonG", "xtdb": "XTDB", "raphtory": "Raphtory"}
NOSTORE = {
    "W1": {"neo4j": "first campaign, loaded on $LOCAL, store not kept", "memgraph": "first campaign, loaded on $LOCAL, store not kept", "ladybug": "first campaign, loaded on $LOCAL, store not kept", "xtdb": "first campaign, loaded on $LOCAL, store not kept", "aeong": "first campaign, loaded on $LOCAL, store not kept", "raphtory": "not measured at this point"},
    "W2": {"xtdb": "not run", "aeong": "not run", "raphtory": "not run at this point"},
    "W3": {"clickhouse": "not run (out of W3 by the stage design)", "xtdb": "not run", "raphtory": "not run", "duckdb": "not run", "aeong": "SF1 only, loaded in its job, store not kept"},
    "W4": {"clickhouse": "not run", "xtdb": "not run", "raphtory": "not run", "duckdb": "not run", "aeong": "not run"},
    "W5": {"clickhouse": "not run", "memgraph": "not run", "ladybug": "not run", "xtdb": "not run", "raphtory": "not run", "duckdb": "not run", "aeong": "not run"},
}
pts = []
for r in recs:
    k = (r["workload"], r["point"])
    if k not in pts:
        pts.append(k)
print("# Store size, du -sb after restore (GB = 1e9 bytes). Each row is that system's whole restored directory. Gnomon's stores on disk hold a stub event log; with and without it are printed separately below.\n")
for wl, pt in sorted(pts):
    print(f"## {wl} {pt}\n\n| system | du -sb bytes | GB | disk blocks GB | source note |\n|---|---:|---:|---:|---|")
    have = {r["system"]: r for r in recs if (r["workload"], r["point"]) == (wl, pt)}
    for s in SYS:
        if s in have:
            r = have[s]
            print(f"| {NAME[s]} | {r['du_sb']} | {r['du_sb'] / 1e9:.3f} | {r['du_blocks'] / 1e9 if r.get('du_blocks') else 0:.3f} | {r.get('note')} |")
    for s in SYS:
        if s not in have and s in NOSTORE.get(wl, {}):
            print(f"| {NAME[s]} | no store | | | {NOSTORE[wl][s]} |")
    print()
print("## Gnomon with and without its event log\n\n| store | without event log, bytes | stub event log in the store, bytes | frozen log, bytes | with the frozen log, bytes | without GB | with GB |\n|---|---:|---:|---:|---:|---:|---:|")
for r in recs:
    if r["system"].startswith("gnomon"):
        print(f"| {r['workload']} {r['point']} {r.get('note')} | {r['du_sb_without_eventlog']} | {r['eventlog_in_store_bytes']} | {r.get('frozen_log_bytes')} | {r.get('du_sb_with_frozen_log')} | {r['du_sb_without_eventlog'] / 1e9:.3f} | {(r.get('du_sb_with_frozen_log') or 0) / 1e9:.3f} |")
print("\n## Entries inside each restored directory\n")
for r in recs:
    if r.get("entries"):
        print(f"- {r['workload']} {r['point']} {r['system']} ({r.get('data_root')}): {json.dumps(r['entries'])}")
