#!/usr/bin/env python3
"""answer in its recorded timed run stays out).
"""
import glob
import hashlib
import json
import shutil
import sys
from pathlib import Path

E = Path(__import__("os").environ.get("GNOMON_BENCH_ROOT", ".") + "")
out = Path(sys.argv[1]); out.mkdir(parents=True, exist_ok=True)
src = {}
pf = E / "final/w1/params-single/params-K10M.json"
keys = {f"{i['query']}#{i['instance']}" for i in json.loads(pf.read_text())["instances"]}
ex = {}
for d, systems in ((E / "confirm/w1/results/timed-J43/sp/K10M/rot1", ("native",)),
                   (E / "final/w1/results/timed-J20/sp/K10M/rot1", ("duckdbsql", "duckdb", "clickhouse", "postgres"))):
    for p in sorted(d.glob("*-*.json")):
        s = p.stem.split("-", 1)[1]
        if s not in systems:
            continue
        for c in json.loads(p.read_text()).get("results", []):
            k = f"{c['query']}#{c['instance']}"
            if k in keys and not c.get("ok"):
                ex.setdefault(s, {})[k] = "excluded: not answered in the recorded run"
    src[f"w1 {systems}"] = str(d)
(out / "w1-exclude-K10M.json").write_text(json.dumps(ex, indent=0, sort_keys=True) + "\n")
print("w1 K10M", len(keys), "instances; excluded", {s: len(v) for s, v in ex.items()})
shutil.copy2(E / "studies/mem/cells/w2-exclude-K10M.json", out / "w2-exclude-K10M.json")
d10 = json.loads((E / "studies/mem/cells/w2-exclude-K10M-d10.json").read_text())
rec = E / "studies/mem/results/w2-J53/K10M-d10/rotM/B-postgres.json"
done = [f"{c['query']}#{c['instance']}" for c in json.loads(rec.read_text())["results"] if c.get("ok")]
for k in done:
    d10.setdefault("postgres", {})[k] = "excluded: measured in mem J53"
(out / "w2-exclude-K10M-d10.json").write_text(json.dumps(d10, indent=0, sort_keys=True) + "\n")
print("w2 K10M-d10 PostgreSQL already measured", len(done))
src["w2"] = "studies/mem/cells (verbatim for K10M); d10 plus " + str(rec)
(out / "sources.json").write_text(json.dumps(src, indent=1) + "\n")
(out / "SHA256SUMS").write_text("".join(f"{hashlib.sha256(p.read_bytes()).hexdigest()}  {p.name}\n" for p in sorted(out.iterdir()) if p.name != "SHA256SUMS"))
