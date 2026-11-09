#!/usr/bin/env python3
"""Thread-matched control: records of the answer-check windows that the window limit cut (as the W2 stage's w2_partial.py).

    thr_partial.py <point results dir> <params-<tag>.json>

A verify window (one configuration, one family) that outlives THR_VERIFY_WINDOW_S is killed; the kit leaves its finished
cells in <record>.tmp.cells.jsonl and no <record>.json. For every such window this writes <record>.json from the finished cells
and adds, for every instance of the family the window did not reach, a cell {ok: false, unreached: true}. Such instances are
neither compared nor timed and are counted in the report; they are NOT recorded "> 10 s" (nothing was measured).
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
    rec.write_text(json.dumps({"system": system, "compare": "exact", "partial": True,
                               "settings": {}, "results": cells}, indent=1) + "\n")
    n_rec += 1
    print(f"partial record {rec.name}: {len(cells) - n_unr} finished cells, {n_unr} unreached")
print(f"thr_partial: {n_rec} partial records in {d}")
