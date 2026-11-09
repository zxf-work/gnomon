#!/usr/bin/env python3
"""k100m stage: after every timed window, the window's answers against the gate's. Exit 1 (the job stops, exit 7) on any difference.

    x_anscheck.py <block record> <point dir with verify-*.json>

An answered cell's projected hash must equal the Gnomon answer at the gate; on a call Gnomon as shipped refused, it must equal the other systems' answer at the gate. A call with no
answer at the gate has nothing to compare with and is listed.
"""
import json
import sys
from pathlib import Path

rec, point = Path(sys.argv[1]), Path(sys.argv[2])
if not rec.exists():
    print(f"ANSCHECK: no record {rec} (window failed?)")
    sys.exit(0)


def key(c):
    return f"{c['query']}#{c['instance']}" if c.get("instance") else c["query"]


ref, others = {}, {}
for f in point.glob("verify-*.json"):
    d = json.loads(f.read_text())
    for c in d.get("results", []):
        if c.get("ok"):
            (ref if d.get("system") == "native" else others).setdefault(key(c), set()).add(c.get("proj_hash"))
bad = 0
n = 0
for c in json.loads(rec.read_text()).get("results", []):
    if not c.get("ok"):
        continue
    n += 1
    k = key(c)
    want = ref.get(k) or others.get(k)
    if not want:
        print(f"ANSCHECK note: {k}: no answer at the gate to compare with")
    elif c.get("proj_hash") not in want or len(want) != 1:
        print(f"ANSCHECK DIFFERS: {rec.name} {k}: {c.get('proj_hash')} rows {c.get('rows')} against {sorted(map(str, want))}")
        bad += 1
print(f"ANSCHECK {rec.name}: {n} answers, {bad} differ")
sys.exit(1 if bad else 0)
