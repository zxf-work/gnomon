#!/usr/bin/env python3
"""Plan of the final Gnomon-only SF10 gate extension (the 67 cells still unreached after the first extension): the cells in tasks
of one query group and one setting (at most 16 cells each), balanced over N streams by the measured per-cell rate. There is no
per-task window: a stream runs its tasks one after the other under one whole-job budget (tools: jobs/w3_gate_ext2_main.sh).
Usage: w3_gate_ext2_plan.py UNREACHED.json PARAMS OUTDIR N   (stream-K.txt lines: name|setting)"""
import collections
import json
import re
import sys
from pathlib import Path

RATE = {"BIa": 16.4, "BIb": 128.6, "BIc": 33.3, "BId": 180.0, "BIH": 13.4, "ICa": 9.1, "ICb": 5.4, "IS": 5.4, "TL": 6.2}


def grp(q):
    if q.startswith("TLDBC"):
        return "TL"
    if q.startswith("BIH"):
        return "BIH"
    m = int(re.sub(r"\D", "", q))
    if q.startswith("IS"):
        return "IS"
    if q.startswith("IC"):
        return "ICa" if m <= 7 else "ICb"
    return "BIa" if m <= 5 else "BIb" if m <= 10 else "BIc" if m <= 15 else "BId"


un = json.load(open(sys.argv[1]))
rows = {}
for line in open(sys.argv[2]):
    r = json.loads(line)
    rows[(r["qid"], r["i"])] = line
out, n = Path(sys.argv[3]), int(sys.argv[4])
out.mkdir(parents=True, exist_ok=True)
by = collections.defaultdict(list)
for q, i, s in un:
    by[(grp(q), s)].append((q, i))
tasks = []
for (g, s), cells in sorted(by.items()):
    cells.sort()
    for k in range(0, len(cells), 16):
        ch = cells[k:k + 16]
        name = f"{g}-{s}-{k // 16}"
        (out / f"params-{name}.jsonl").write_text("".join(rows[c] for c in ch))
        tasks.append((len(ch) * RATE[g], name, s, len(ch)))
tasks.sort(reverse=True)
load, streams = [0.0] * n, [[] for _ in range(n)]
for t in tasks:
    k = load.index(min(load))
    load[k] += t[0]
    streams[k].append(t)
for k, st in enumerate(streams, 1):
    (out / f"stream-{k}.txt").write_text("".join(f"{nm}|{s}\n" for _, nm, s, _c in st))
(out / "params.jsonl").write_text("".join(rows[c] for c in sorted({(q, i) for q, i, _ in un})))
(out / "settings.txt").write_text("current,asof,window\n")
print(f"{len(un)} cells, {len(tasks)} tasks, estimated seconds per stream {[round(sum(t[0] for t in s)) for s in streams]}")
