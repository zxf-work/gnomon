#!/usr/bin/env python3
"""Bounded AeonG time-map check (replaces `w3_snb_systems.py aeong-timemap-check` for this stage).
The kit's check bisects the created-version count for EVERY distinct T of the parameter file (hundreds), each bisection about 50
and ran into the job limit. This runs the same check (AeonG.timemap_check, unchanged) on a sample of T values: the first, the
last, evenly spread ones (--max-t) and OPEN-1, stops starting new T values after --deadline-s, logs the duration of every count,
and exits 0 only if at least 3 values were checked and none mismatched (4 on a mismatch, 5 if fewer than 3 were checked).
It is a check of the load, not a measured query."""
import argparse
import json
import os
import sys
import time
from pathlib import Path

repo = Path(os.environ["EXTCMP_REPO"])
sys.path[:0] = [str(repo), str(repo / "scripts")]
import w3_snb_systems as Y  # noqa: E402

ap = argparse.ArgumentParser()
ap.add_argument("--hist", required=True)
ap.add_argument("--uri", required=True)
ap.add_argument("--params", required=True)
ap.add_argument("--out", required=True)
ap.add_argument("--max-t", type=int, default=5)
ap.add_argument("--deadline-s", type=float, default=900)
a = ap.parse_args()
A = Y.AeonG(a.uri, "aeong", a.hist)
if not A.group_ts:
    sys.exit("no load-time map (group_ts) in aeong-load.json")
Ts = sorted({json.loads(x)["T"] for x in open(a.params) if x.strip()})
k = max(2, a.max_t)
pick = sorted({Ts[round(j * (len(Ts) - 1) / (k - 1))] for j in range(k)}) + [Y.OPEN - 1]
orig = A._count


def timed_count(x):
    t = time.time()
    v = orig(x)
    print(f"  count x={x} -> {v} in {time.time() - t:.2f} s", flush=True)
    return v


A._count = timed_count
t0, rows, bad = time.time(), [], []
print(f"{len(Ts)} distinct T in the parameter file; checking {len(pick)}: {pick}", flush=True)
for T in pick:
    if time.time() - t0 > a.deadline_s and len(rows) >= 3:
        print(f"deadline {a.deadline_s:g} s reached after {len(rows)} values", flush=True)
        break
    t1 = time.time()
    r = A.timemap_check([T])
    rows += r["rows"]
    bad += r["mismatches"]
    print(f"T={T} checked in {time.time() - t1:.1f} s, mismatches so far {len(bad)}", flush=True)
rec = {"T_checked": len(rows), "T_in_file": len(Ts), "sampled": True, "mismatches": bad, "rows": rows, "elapsed_s": round(time.time() - t0, 1)}
Path(a.out).write_text(json.dumps(rec, indent=1) + "\n")
print(json.dumps({k2: v for k2, v in rec.items() if k2 != "rows"}))
sys.exit(4 if bad else (0 if len(rows) >= 3 else 5))
