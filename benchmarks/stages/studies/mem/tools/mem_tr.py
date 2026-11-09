#!/usr/bin/env python3
"""mem_tr.py --keep CELLS.json <timed_runner arguments>: the kit's timed runner with its cell list narrowed to the named cells (CELLS.json: ["query|instance|setting", ...]), for the untimed memory pass. W3 keeps its stage's Cypher overrides."""
import json
import os
import sys
from pathlib import Path

repo = Path(os.environ["EXTCMP_REPO"])
for p in (repo, repo / "scripts", repo / "benchmarks/stages/w3/tools"):
    sys.path.insert(0, str(p))
argv = sys.argv[1:]
i = argv.index("--keep")
keep = set(json.load(open(argv[i + 1])))
del argv[i:i + 2]
wl = argv[0]
import timed_runner as TR  # noqa: E402
import tr_catalogs as TC  # noqa: E402

if wl == "w3":
    import w3_timed
    w3_timed.apply_override()
cells0, open0 = TC.CATALOGS[wl]


def cells(a):
    allc = cells0(a)
    k = [c for c in allc if f"{c[0]}|{c[2]}|{c[1]}" in keep]
    print(f"[mem_tr] {len(k)} of {len(allc)} cells", flush=True)
    return k


TC.CATALOGS[wl] = (cells, open0)
sys.argv = ["timed_runner.py"] + argv
raise SystemExit(TR.main())
