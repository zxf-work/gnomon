#!/usr/bin/env python3
"""One R-10M skip list from two r10m.py lists of the same system, as the October W2 records applied the rule: families P..M from the K1M list,
family B from the K1M-dX list (K10M-dX <- K1M-dX).

    y_r10m_merge.py <K1M list> <K1M-dX list> <out>
"""
import json
import sys
from pathlib import Path

import extcmp as X          # the pinned kit (PYTHONPATH = $REPO:$REPO/scripts)

B = set(X.FAMILIES["B"])
reg, bl = (json.loads(Path(p).read_text()) for p in sys.argv[1:3])
assert reg.get("system") == bl.get("system"), (reg.get("system"), bl.get("system"))
out = dict(reg)
out["skip"] = {q: v for q, v in reg["skip"].items() if q not in B} | {q: v for q, v in bl["skip"].items() if q in B}
out["run"] = {q: v for q, v in reg["run"].items() if q not in B} | {q: v for q, v in bl["run"].items() if q in B}
out["source"] = {"P..M": reg["source"], "B": bl["source"]}
out["point_1m"] = {"P..M": reg["point_1m"], "B": bl["point_1m"]}
out["files"] = reg["files"] + bl["files"]
Path(sys.argv[3]).write_text(json.dumps(out, indent=1) + "\n")
print(f"R-10M merged {out['system']}: skip {sorted(out['skip'])}")
