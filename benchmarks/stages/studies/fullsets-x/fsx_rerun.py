#!/usr/bin/env python3
"""fsx_rerun.py FIRST_CELLS CLOSURE OUT [--pg-over-only]: a closure list for a second run of one point, from the first run's records:
for each instance, the systems whose state was not "compared" (not reached, failed, over the limit); Gnomon runs again on every listed
instance (its whole answers were not kept). With --pg-over-only... (not used). Records which first-run state each system had."""
import json
import sys

first, closure, out = sys.argv[1:4]
recs = {}
for x in open(first):
    r = json.loads(x)
    recs[r["key"]] = r
doc = json.load(open(closure))
cells = []
for c in doc["cells"]:
    r = recs.get(c["key"])
    need, prev = [], {}
    for s in c["need"]:
        v = (r or {}).get("systems", {}).get(s, {})
        if not v.get("compared"):
            need.append(s)
            prev[s] = v.get("state", "no record")
    if need:
        cells.append({**c, "need": need, "first_run": prev})
doc = {**doc, "cells": cells, "first_run_records": first, "too_large_not_run": doc["too_large_not_run"]}
open(out, "w").write(json.dumps(doc, sort_keys=True, indent=0) + "\n")
from collections import Counter
print(len(cells), Counter((s, c["query"], c["first_run"][s]) for c in cells for s in c["need"]))
