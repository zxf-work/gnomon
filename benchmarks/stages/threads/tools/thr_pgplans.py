#!/usr/bin/env python3
"""Thread-matched control: how many parallel workers PostgreSQL planned per statement, per configuration.

    thr_pgplans.py --res <workload results dir> --wl <w1|w2>

max_parallel_workers_per_gather is a CAP: the planner chooses the number of workers of a Gather node from the size of the
scanned relation (log3 of its size over min_parallel_table_scan_size) up to the cap. The answer-check windows of the gate job
log every statement's plan (auto_explain, log_min_duration 0, no analyze); this lists, per PostgreSQL configuration, the
statements, those with a Gather node, and the distribution of the largest "Workers Planned" per statement. Untimed.
"""
import argparse
import re
import statistics as st
from collections import Counter
from pathlib import Path

ap = argparse.ArgumentParser()
ap.add_argument("--res", type=Path, required=True)
ap.add_argument("--wl", default="")
a = ap.parse_args()
print(f"# PostgreSQL workers planned per statement ({a.wl})\n")
print("| input | config | statements | with a Gather node | planned workers per statement (min / median / max) | at the cap | cap |")
print("|---|---|---:|---:|---|---:|---:|")
found = False
for cdir in sorted(a.res.glob("cfg-p*")):
    cap = int(cdir.name[len("cfg-p"):])
    for tdir in sorted(p for p in cdir.iterdir() if p.is_dir()):
        planned = []
        n = 0
        for f in sorted(tdir.glob("pgplan-*.log")):
            text = f.read_text(errors="replace")
            for chunk in re.split(r"LOG:\s+duration:", text)[1:]:
                if "plan:" not in chunk.split("\n", 1)[0] + chunk[:200]:
                    continue
                n += 1
                w = [int(x) for x in re.findall(r"Workers Planned:\s*(\d+)", chunk)]
                planned.append(max(w) if w else 0)
        if not n:
            continue
        found = True
        par = [p for p in planned if p > 0]
        dist = f"{min(planned)} / {st.median(planned):g} / {max(planned)}"
        print(f"| {tdir.name} | p{cap} | {n} | {len(par)} | {dist} | {sum(1 for p in planned if cap and p >= cap)} | {cap} |")
if not found:
    print("| (no plan logs found: auto_explain unavailable or no PostgreSQL window ran) | | | | | | |")
