#!/usr/bin/env python3
"""mem_cells.py <out dir> [--smoke]: the memory pass's cells, written before submission from the recorded timed runs. A cell a system answered in its record (W3: instances 0, 1, 2 of a (query, setting) every instance of which answered, the engine pass's rule) is kept;
a cell that was censored in the record stays out, so a query that ran into the ceiling is not in this pass. W3, W4: keep lists. W2, W5: the kit's exclusion files. --smoke: a few cells per system."""
import glob
import hashlib
import json
import sys
from pathlib import Path

E = Path(__import__("os").environ.get("GNOMON_BENCH_ROOT", ".") + "")


def key(c):
    return f"{c['query']}|{c['instance']}|{c['setting']}"


def kit_key(c):
    return f"{c['query']}#{c['instance']}"


def main():
    out = Path(sys.argv[1])
    out.mkdir(parents=True, exist_ok=True)
    smoke = "--smoke" in sys.argv
    srcs = {}
    w3 = {"gnomon": E / "confirm/w3/results/timed-J47/rot1/ALL-gnomon.json", "duckdb": E / "final/w3/results/timed-J40/rot1/ALL-duckdb64.json", "postgres": E / "final/w3/results/timed-J40/rot1/ALL-postgres.json", "memgraph": E / "final/w3/results/timed-J40/rot1/ALL-memgraph.json"}
    for s, p in w3.items():
        rs = json.loads(p.read_text())["results"]
        bad = {(c["query"], c["setting"]) for c in rs if not c.get("ok")}
        keep = sorted(key(c) for c in rs if (c["query"], c["setting"]) not in bad and int(c["instance"]) in (0, 1, 2))
        if smoke:
            keep = keep[:3]
        (out / f"w3-keep-{s}.json").write_text(json.dumps(keep) + "\n")
        srcs[f"w3-{s}"] = {"record": str(p), "sha256": hashlib.sha256(p.read_bytes()).hexdigest(), "cells": len(keep), "of": len(rs)}
    wd = E / "studies/w4/results/sf10-r10-J48/rot1"
    for t, f in {"gnomon": "gnomon", "duckdbsql": "duckdbsql64", "postgres": "postgres", "neo4j": "neo4j", "memgraph": "memgraph"}.items():
        p = wd / f"{f}.json"
        rs = json.loads(p.read_text())["results"]
        keep = sorted(key(c) for c in rs if c.get("ok"))
        if smoke:
            keep = keep[:3]
        (out / f"w4-keep-{t}.json").write_text(json.dumps(keep) + "\n")
        srcs[f"w4-{t}"] = {"record": str(p), "sha256": hashlib.sha256(p.read_bytes()).hexdigest(), "cells": len(keep), "of": len(rs)}
    # W2, W5: per system {query#instance: label} for the file's instances that were not answered in the record
    points = {"w2-K10M": ([str(E / "final/w2/results/timed-J24/p1/K10M/rot1")], E / "studies/abl/params-w2/params-K10M.json", "w2", "K10M"),
              "w2-K10M-d10": ([str(E / "final/w2/results/timed-J25/p1/K10M-d10/rot1")], E / "studies/abl/params-w2/params-K10M-d10.json", "w2", "K10M-d10"),
              "w5-reg": ([str(E / "studies/w5/results/W5-wikidata-v2-J49/p1/W5-wikidata-v2/rot1")], E / "w5-wikidata/params-c3/params-W5-wikidata-v2-reg.json", "w5", "W5-wikidata-v2"),
              "w5-bel": ([str(E / "studies/w5/results/W5-wikidata-v2-J49/p1/W5-wikidata-v2/rot1")], E / "w5-wikidata/params-c3/params-W5-wikidata-v2-bel.json", "w5", "W5-wikidata-v2")}
    merged = {}
    for name, (dirs, pf, wl, tag) in points.items():
        doc = json.loads(pf.read_text())
        keys = {f"{i['query']}#{i['instance']}" for i in doc["instances"]}
        ex = merged.setdefault((wl, tag), {})
        for d in dirs:
            for p in glob.glob(d + "/*-*.json"):
                sysn = Path(p).stem.split("-", 1)[1]
                if sysn not in ("native", "duckdbsql", "postgres", "neo4j", "memgraph"):
                    continue
                r = json.loads(Path(p).read_text())
                for c in r.get("results", []):
                    k = kit_key(c)
                    if k in keys and not c.get("ok"):
                        ex.setdefault(sysn, {})[k] = "excluded: not answered in the recorded run"
        srcs[name] = {"record": dirs, "params": str(pf), "sha256": hashlib.sha256(pf.read_bytes()).hexdigest(), "instances": len(keys)}
    for (wl, tag), ex in merged.items():
        (out / f"{wl}-exclude-{tag}.json").write_text(json.dumps(ex, indent=0, sort_keys=True) + "\n")
        print(wl, tag, {s: len(v) for s, v in ex.items()})
    if smoke:
        src = json.loads((E / "final/w2/stores/K1M/load-native.json").read_text()) if False else None
        doc = json.loads((E / "confirm/w2/src/benchmarks/stages/w2/params/params-K1M.json").read_text())
        pick, n = [], {}
        for i in doc["instances"]:
            if i["query"] in ("snap.hop2", "reach.window", "nbr.evolution"):
                n[i["query"]] = n.get(i["query"], 0) + 1
                if n[i["query"]] <= 2:
                    pick.append(i)
        doc["instances"] = pick
        (out / "w2-params-K1M.json").write_text(json.dumps(doc) + "\n")
        (out / "w2-exclude-K1M.json").write_text("{}\n")
    (out / "sources.json").write_text(json.dumps(srcs, indent=1, default=str) + "\n")
    sums = []
    for p in sorted(out.iterdir()):
        if p.name != "SHA256SUMS":
            sums.append(f"{hashlib.sha256(p.read_bytes()).hexdigest()}  {p.name}")
    (out / "SHA256SUMS").write_text("\n".join(sums) + "\n")
    print(json.dumps(srcs, default=str)[:1500])


if __name__ == "__main__":
    main()
