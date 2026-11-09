#!/usr/bin/env python3
"""fsx_select.py OUTDIR: the instances of the whole-answer closure (coverage refresh row 3, gaps N1 and N2), written before submission.

From the fullsets stage: its manifests (SHA256SUMS checked) and its records, the last record of each
state there was a prefix comparison ("prefix only") or "not checked: over the limit". Instances "not checked: too large" are listed
with their recorded totals and not run. From the matchpath stage: the W2 K10M-d0 instances
whose dedicated answer was compared with `match` on a prefix only, with the args of matchpath's parameter file (checked equal to the
fullsets manifest's args for the same instance id); they run at the K10M-d0 point, on matchpath's store."""
import glob
import hashlib
import json
import subprocess
import sys
from pathlib import Path

E = __import__("os").environ.get("GNOMON_BENCH_ROOT", ".") + "/studies"
FS = f"{E}/fullsets"
JOBS = ["fullsets-J54", "fullsets-J57", "fullsets-J58"]
MP = f"{E}/matchpath/results/timed-J59/w2/K10M-d0"
MP_PARAMS = f"{E}/matchpath/params/w2/params-K10M-d0.json"
POINTS = ["w2-K10M-d0", "w2-K10M-d10", "w1-K10M", "w2-K10M"]
SYSTEMS = ("duckdbsql", "postgres")


def main():
    out = Path(sys.argv[1])
    out.mkdir(parents=True, exist_ok=True)
    subprocess.run(["sha256sum", "-c", "--quiet", "SHA256SUMS"], cwd=f"{FS}/manifest", check=True)
    last = {}
    for j in JOBS:
        for f in sorted(glob.glob(f"{FS}/results/{j}/cells-*.jsonl")):
            for x in open(f):
                r = json.loads(x)
                last[(r["point"], r["query"], r["instance"])] = r
    mp_part = set()
    for f in glob.glob(f"{MP}/whole-cells.jsonl.*"):
        for x in open(f):
            r = json.loads(x)
            if r.get("whole") is False or r.get("prefix_fields"):
                mp_part.add((r["query"], r["instance"]))
    mp_args = {(i["query"], i["instance"]): i for i in json.load(open(MP_PARAMS))["instances"]}
    man = {}
    for p in POINTS:
        for c in json.load(open(f"{FS}/manifest/manifest-{p}.json"))["cells"]:
            man[(p, c["query"], c["instance"])] = c
    summary = {}
    for p in POINTS:
        cells, too_large = [], []
        for (pt, q, i), c in sorted(man.items()):
            if pt != p:
                continue
            r = last.get((pt, q, i))
            if r is None:
                continue
            if r.get("state") == "not checked: too large":
                too_large.append({"query": q, "instance": i, "recorded_total": c["recorded_total"]})
                continue
            need, why = [], {}
            for s in SYSTEMS:
                v = (r.get("systems") or {}).get(s) or {}
                st = "prefix only" if v.get("prefix_only_fields") else v.get("state")
                if st in ("prefix only", "not checked: over the limit"):
                    need.append(s)
                    why[s] = st
            if need:
                cells.append({**{k: c[k] for k in ("point", "query", "instance", "key", "args", "stratum", "recorded_total")},
                              "need": need, "why": why, "source": "fullsets"})
        if p == "w2-K10M-d0":
            have = {(c["query"], c["instance"]) for c in cells}
            for q, i in sorted(mp_part):
                a = mp_args[(q, i)]["args"]
                src = man.get(("w2-K10M", q, i)) or man.get(("w2-K10M-d0", q, i))
                if src is None or src["args"] != a:
                    raise SystemExit(f"matchpath instance {q}#{i}: args differ from the fullsets manifest or it is absent")
                if (q, i) in have:
                    for c in cells:
                        if (c["query"], c["instance"]) == (q, i):
                            c["source"] = "fullsets+matchpath"
                    continue
                cells.append({"point": p, "query": q, "instance": i, "key": f"{q}#{i}", "args": a, "stratum": src.get("stratum"),
                              "recorded_total": src["recorded_total"], "need": list(SYSTEMS), "why": {"matchpath": "dedicated prefix only"},
                              "source": "matchpath"})
        doc = {"point": p, "fullsets_jobs": JOBS, "matchpath_cells": MP, "cells": cells, "too_large_not_run": too_large}
        (out / f"closure-{p}.json").write_text(json.dumps(doc, sort_keys=True, indent=0) + "\n")
        summary[p] = {"cells": len(cells), "duckdbsql": sum("duckdbsql" in c["need"] for c in cells),
                      "postgres": sum("postgres" in c["need"] for c in cells), "too_large": len(too_large),
                      "by_query": {q: sum(c["query"] == q for c in cells) for q in sorted({c["query"] for c in cells})},
                      "matchpath": sum("matchpath" in c["source"] for c in cells)}
        print(p, json.dumps(summary[p]))
    sums = [f"{hashlib.sha256(f.read_bytes()).hexdigest()}  {f.name}" for f in sorted(out.glob("closure-*.json"))]
    (out / "SHA256SUMS").write_text("\n".join(sums) + "\n")
    (out / "summary.json").write_text(json.dumps(summary, indent=1) + "\n")


if __name__ == "__main__":
    main()
