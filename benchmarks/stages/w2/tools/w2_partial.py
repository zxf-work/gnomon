#!/usr/bin/env python3
"""W2 timed stage: records of the answer-check windows that the window limit cut.

    w2_partial.py <point results dir> <params-<tag>.json>

A verify window (one system, one family) that outlives W2_VERIFY_WINDOW_S is killed; the kit leaves its finished cells in
<record>.tmp.cells.jsonl and no <record>.json. For every such window this writes <record>.json from the finished cells
and adds, for every instance of the family the window did not reach, a cell {ok: false, error: "unreached: ..."}.
The kit gate then excludes those instances from timing (verdict "error"), and tools/w2_gate.py counts them. They are
NOT recorded "> 10 s": nothing was measured for them. Windows with a record are untouched.
"""
import json
import re
import sys
from pathlib import Path

import extcmp as X          # the pinned kit (PYTHONPATH = $REPO:$REPO/scripts)

d, params = Path(sys.argv[1]), json.loads(Path(sys.argv[2]).read_text())
fam_of = {q: f for f, qs in X.FAMILIES.items() for q in qs}
n_rec = 0
for cf in sorted(d.glob("verify-*.json.tmp.cells.jsonl")):
    rec = Path(str(cf)[: -len(".tmp.cells.jsonl")])
    if rec.exists():
        continue
    m = re.match(r"verify-([a-z0-9]+)(?:-([A-Z]))?\.json$", rec.name)
    if not m:
        continue
    fam = m.group(2) or "B"
    cells = [json.loads(line) for line in cf.read_text().splitlines() if line.strip()]
    system = (cells[0].get("system") if cells else None) or m.group(1)
    have = {(c["query"], c.get("instance")) for c in cells}
    n_unr = 0
    for i in params["instances"]:
        if (i["query"], i["instance"]) in have or fam_of.get(i["query"]) != fam:
            continue
        cells.append({"query": i["query"], "instance": i["instance"], "stratum": i.get("stratum"), "ok": False,
                      "unreached": True, "error": "unreached: answer-check window limit (not compared, not timed)"})
        n_unr += 1
    rec.write_text(json.dumps({"system": system, "compare": "exact", "partial": True, "results": cells}, indent=1) + "\n")
    n_rec += 1
    print(f"partial record {rec.name}: {len(cells) - n_unr} finished cells, {n_unr} unreached")
print(f"w2_partial: {n_rec} partial records in {d}")
