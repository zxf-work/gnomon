#!/usr/bin/env python3
"""writer_table.py <results dir> <E>: phases of each writer-path run, as recorded, in seconds, with the replay of the same log beside it. Wall includes reading and parsing the log; wall minus the four phases is that read."""
import glob
import json
import os
import sys

res, E = sys.argv[1:3]
print("| run | wall s | log append s | apply s | commit s | compaction s | read and other s | final gc s | commits | compactions | peak RSS GB | store du -sb | of which log | log equal to input | digest equal to replay | replay load s (no log write) |")
print("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---:|")
for p in sorted(glob.glob(res + "/run-*.json")):
    d = json.load(open(p))
    i = os.path.basename(p)[4:-5]
    wl, tag = i.split("-", 1)
    k = next(iter(d["by_kind"].values()))
    four = k["log_s"] + k["apply_s"] + k["commit_s"] + k["compact_s"]
    dg = {}
    if os.path.exists(f"{res}/digest-{i}.json"):
        dg = json.load(open(f"{res}/digest-{i}.json"))
    try:
        rep = json.load(open(f"{E}/confirm/{wl}/stores/{tag}/load-native.json"))["load"]["load_s"]
    except (OSError, KeyError):
        rep = ""
    print(f"| {i} | {d['wall_incl_log_read_s']} | {k['log_s']} | {k['apply_s']} | {k['commit_s']} | {k['compact_s']} | {round(d['wall_incl_log_read_s'] - four, 3)} | {d['final_gc_s']} | {k['batches']} | {d['compactions']} | {round(d['peak_rss_kb'] / 1048576, 2)} | {d.get('du_sb_store')} | {d.get('eventlog_bytes')} | {d['log_check']['equal']} (bytes {d['log_check']['bytes_equal']}) | {dg.get('equal')} | {rep} |")
