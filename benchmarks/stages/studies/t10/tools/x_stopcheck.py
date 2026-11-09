#!/usr/bin/env python3
"""W2 timed stage: the Gnomon stop rule after each timed Gnomon window (same rule as the W1 stage's w1_stopcheck.py).

    w2_stopcheck.py <block record rotX/<fam>-native.json> <point dir with verify-*.json>

Exit 1 (stop the job) when a Gnomon cell that was not excluded at the answer check is an error, or a refusal of
a call that another system answered inside 10 s at the answer check. A Gnomon call above the ceiling, or skipped by the
R-10M rule, is recorded "> 10 s" and does not stop the job (it is listed). Exit 0 otherwise.
"""
import json
import sys
from pathlib import Path

rec, point = Path(sys.argv[1]), Path(sys.argv[2])
# X_KNOWN_REFUSED: a file listing exactly the (query, instance) pairs Gnomon as shipped refused at this job's
# gate. Such a pair with the same CostError is listed instead of stopping. Any other Gnomon error, or any new refusal, stops as before.
import os
_kr = os.environ.get("X_KNOWN_REFUSED")
KNOWN = set(json.loads(Path(_kr).read_text())["refused"]) if _kr else set()
if not rec.exists():
    print(f"STOPCHECK: no record {rec} (window failed?)")
    sys.exit(1)
cells = json.loads(rec.read_text()).get("results", [])
walls = {}
for p in point.glob("verify-*.json"):
    d = json.loads(p.read_text())
    if d.get("system") == "native":
        continue
    for c in d.get("results", []):
        if c.get("ok"):
            k = f"{c['query']}#{c.get('instance')}" if c.get("instance") else c["query"]
            walls.setdefault(k, []).append((d.get("system"), c.get("wall_ms", 1e18)))
stop = False
for c in cells:
    if c.get("ok") or c.get("excluded"):
        continue
    k = f"{c['query']}#{c.get('instance')}" if c.get("instance") else c["query"]
    err = str(c.get("error", ""))
    if k in KNOWN and "CostError: estimated cost" in err:
        print(f"STOPCHECK listed: Gnomon as shipped refused {k} ({err[:120]}), a pair of the gate's list")
        continue
    fast = [s for s, w in walls.get(k, []) if w <= 10_000]
    if c.get("aborted") or err.startswith(">") or "client-side limit" in err:
        print(f"STOPCHECK note: Gnomon > 10 s on {k} ({err[:120]}); others inside 10 s at the answer check: {fast or 'none'}")
    elif c.get("refused") or "E_COST" in err or "guardrail" in err.lower():
        if fast:
            print(f"STOPCHECK STOP: Gnomon refused {k} ({err[:160]}); answered inside 10 s by {fast}")
            stop = True
        else:
            print(f"STOPCHECK note: Gnomon refused {k} (estimated > 10 s); no other system inside 10 s")
    else:
        print(f"STOPCHECK STOP: Gnomon error on {k}: {err[:200]}")
        stop = True
print(f"STOPCHECK {rec}: {len(cells)} cells, {'STOP' if stop else 'ok'}")
sys.exit(1 if stop else 0)
