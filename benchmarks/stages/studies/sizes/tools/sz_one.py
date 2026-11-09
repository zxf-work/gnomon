#!/usr/bin/env python3
"""sz_one.py: one store's size record. du -sb of the unpacked directory (apparent bytes), du --block-size=1 -s (disk blocks), du -sb of each entry one and two levels down. For Gnomon: the store as it is on disk holds a stub event log; the
record gives the size with that stub, without it, and with the frozen log's bytes added (what a store written through the writer path would keep)."""
import argparse
import json
import os
import subprocess
from pathlib import Path


def du(p, blocks=False):
    a = ["du", "-s", "--block-size=1", p] if blocks else ["du", "-sb", p]
    return int(subprocess.run(a, capture_output=True, text=True, check=True).stdout.split()[0])


def main():
    ap = argparse.ArgumentParser()
    for k in ("dir", "workload", "point", "system", "source", "log", "note", "sha-checked", "out"):
        ap.add_argument("--" + k, required=True)
    a = ap.parse_args()
    d = Path(a.dir)
    ents = sorted(d.iterdir())
    top = ents[0] if len(ents) == 1 else d
    rec = {"workload": a.workload, "point": a.point, "system": a.system, "source": a.source, "note": a.note, "sha_checked": a.sha_checked == "1", "du_sb": du(top), "du_blocks": du(top, True), "data_root": top.name,
           "entries": {e.name: du(e) for e in sorted(top.iterdir())} if top.is_dir() else {}}
    if a.system.startswith("gnomon"):
        logs = list(top.rglob("eventlog.jsonl"))
        stub = sum(p.stat().st_size for p in logs)
        rec.update(eventlog_in_store_bytes=stub, eventlog_paths=[str(p.relative_to(top)) for p in logs], du_sb_without_eventlog=rec["du_sb"] - stub)
        if a.log != "-":
            n = os.stat(a.log).st_size
            rec.update(frozen_log=a.log, frozen_log_bytes=n, du_sb_with_frozen_log=rec["du_sb"] - stub + n)
    with open(a.out, "a") as f:
        f.write(json.dumps(rec, sort_keys=True) + "\n")
    print(json.dumps({k: rec[k] for k in ("workload", "point", "system", "du_sb", "du_blocks", "data_root")}), flush=True)


if __name__ == "__main__":
    main()
