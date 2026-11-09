#!/usr/bin/env python3
"""Thread-matched control: the Gnomon stop rule after each timed Gnomon window.

    thr_stopcheck.py <block record rotX/<fam>-native.json> <cfg>

At the configuration g32 (the W1/W2 rule, whose answer check each cell already passed): exit 1 (stop the job) when a
Gnomon cell that was not excluded is an error or an admission refusal (it was answered at the gate), or when the window left no
record. A call above the ceiling is recorded "> 10 s" and does not stop the job. At g1 and g4 (a deliberately slower
Gnomon) nothing stops the job: errors, refusals and calls above the ceiling are listed and counted in the report.
"""
import json
import sys
from pathlib import Path

rec, cfg = Path(sys.argv[1]), sys.argv[2]
pinned = cfg == "g32"
if not rec.exists():
    print(f"STOPCHECK: no record {rec} (window failed?) {'STOP' if pinned else 'noted'}")
    sys.exit(1 if pinned else 0)
cells = json.loads(rec.read_text()).get("results", [])
stop = False
for c in cells:
    if c.get("ok") or c.get("excluded"):
        continue
    k = f"{c['query']}#{c.get('instance')}" if c.get("instance") else c["query"]
    err = str(c.get("error", ""))
    if c.get("aborted") or err.startswith(">") or "client-side limit" in err:
        print(f"STOPCHECK note ({cfg}): Gnomon > 10 s on {k} ({err[:120]})")
    elif c.get("refused") or "E_COST" in err or "guardrail" in err.lower():
        print(f"STOPCHECK {'STOP' if pinned else 'note'} ({cfg}): Gnomon refused {k} ({err[:160]})")
        stop = stop or pinned
    else:
        print(f"STOPCHECK {'STOP' if pinned else 'note'} ({cfg}): Gnomon error on {k}: {err[:200]}")
        stop = stop or pinned
print(f"STOPCHECK {rec} ({cfg}): {len(cells)} cells, {'STOP' if stop else 'ok'}")
sys.exit(1 if stop else 0)
