#!/usr/bin/env python3
"""
    answer_check.py <new results dir> <old results dir> <tag>...

designs(): the on/off design switches are compared (every one in force); the
numeric settings it also reports (match_threads, match_par_rows: thread count
and row threshold of the parallel executor, NUMERIC_SETTINGS) are recorded in
the output but are not switches, so they are left out of that comparison.

Per tag, every query of verify-native.json: the answer hash (and row count)
of the new engine must equal the campaign's. Also prints the
designs() report the verify record carries. Exit 1 on any difference.
"""
import json, sys
from pathlib import Path

import os
NEW_SWITCHES = {"agg_range_count", "reach_kernel", "diff_kernel", "ingest_raw"}
#: designs() entries that are settings with a value, not on/off switches
NUMERIC_SETTINGS = {"match_threads", "match_par_rows"}
EXPECT = int(os.environ.get("EXPECT_DESIGNS", "0"))
if EXPECT >= 13:
    NEW_SWITCHES |= {"staircase"}
new, old, tags = Path(sys.argv[1]), Path(sys.argv[2]), sys.argv[3:]
bad = 0
for tag in tags:
    n = json.loads((new / tag / "verify-native.json").read_text())
    o = json.loads((old / tag / "verify-native.json").read_text())
    nr = {r["query"]: r for r in n["results"]}
    orr = {r["query"]: r for r in o["results"]}
    des = (n.get("settings") or {}).get("designs")
    eq = 0
    print(f"## {tag}: {len(nr)} queries new, {len(orr)} campaign")
    if isinstance(des, dict):
        num = {k: des[k] for k in sorted(des) if k in NUMERIC_SETTINGS or (isinstance(des[k], (int, float)) and not isinstance(des[k], bool))}
        if num:
            print(f"   designs() numeric settings (recorded, not compared): {num}")
        des = {k: v for k, v in des.items() if k not in num}
        off = sorted(k for k, v in des.items() if v is not True)
        missing = sorted(NEW_SWITCHES - set(des))
        print(f"   designs(): {len(des)} switches, all in force (unset/default): {not off}; "
              f"off: {off or 'none'}; the 4 new present: {not missing}")
        if EXPECT and len(des) != EXPECT:  # switches only (numeric settings excluded above)
            print(f"   designs(): expected {EXPECT} switches, got {len(des)}"); bad += 1
        if off or missing:
            bad += 1
    else:
        print(f"   designs(): {des!r}"); bad += 1
    for q in sorted(set(nr) | set(orr)):
        a, b = nr.get(q), orr.get(q)
        if not a or not b or not a.get("ok") or a.get("hash") != b.get("hash"):
            bad += 1
            print(f"   DIFFERS {q}: new={a and (a.get('ok'), a.get('hash'), a.get('rows'))} "
                  f"campaign={b and (b.get('hash'), b.get('rows'))}")
        else:
            eq += 1
            print(f"   equal   {q:22s} {a['hash']} rows={a.get('rows')} "
                  f"wall_ms new={a.get('wall_ms')} campaign={b.get('wall_ms')}")
    print(f"   {eq}/{len(set(nr) | set(orr))} equal")
print("ANSWER CHECK", "PASS" if bad == 0 else f"FAIL ({bad})")
sys.exit(1 if bad else 0)
