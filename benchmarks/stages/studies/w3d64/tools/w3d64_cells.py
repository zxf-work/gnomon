#!/usr/bin/env python3
"""w3d64_cells.py <out dir> [--smoke]: the cells of the 64 GB run, chosen before submission from the recorded W3 SF10 timed run, DuckDB (SQL) at 64 and 32 threads.
selected: every cell with p50 above 1 s, or not answered, at either thread setting. killed: a cell whose prime was killed at 60 s or interrupted at its 58 s cap. control: the first max(2, ceil(10 %)) selected cells by md5(salt|cell).
The exclude files are the runner's gate-exclusion format, naming every other cell. --smoke: 3 cells."""
import hashlib
import json
import math
import sys
from pathlib import Path

R = Path(__import__("os").environ.get("GNOMON_BENCH_ROOT", ".") + "/final/w3/results/timed-J40/rot1")
SALT = "gnomon-c4-w3d64-2026-11-07"


def key(c):
    return f"{c['query']}|{c['instance']}|{c['setting']}"


def main():
    out = Path(sys.argv[1])
    out.mkdir(parents=True, exist_ok=True)
    smoke = "--smoke" in sys.argv
    sel, killed, allc = {}, {}, set()
    srcs = {}
    for s in ("duckdb64", "duckdb32"):
        p = R / f"ALL-{s}.json"
        srcs[s] = hashlib.sha256(p.read_bytes()).hexdigest()
        for c in json.loads(p.read_text())["results"]:
            k = key(c)
            allc.add(k)
            if c.get("ok"):
                if c["p50_ms"] > 1000:
                    sel.setdefault(k, []).append(f"{s}: above 1 s")
            else:
                sel.setdefault(k, []).append(f"{s}: not answered")
                if "killed at 60" in c.get("error", "") or (c.get("prime_ms") or 0) > 50000:
                    killed.setdefault(k, []).append(s)
    order = sorted(sel, key=lambda k: hashlib.md5(f"{SALT}|{k}".encode()).hexdigest())
    n = max(2, math.ceil(0.10 * len(order)))
    control = order[:n]
    if smoke:
        kk = next(k for k in order if k in killed)
        ok = [k for k in order if k not in killed][:2]
        order = [kk] + ok
        control = ok[:1]
    (out / "selected.json").write_text(json.dumps({"selected": {k: sel[k] for k in sorted(order)}, "killed_at_prime": {k: killed[k] for k in sorted(killed) if k in order}, "control": sorted(control), "salt": SALT, "source_sha256": srcs, "n_all_cells": len(allc)}, indent=1) + "\n")
    (out / "exclude-selected.json").write_text(json.dumps({"excluded": {"duckdb": sorted(allc - set(order))}}) + "\n")
    (out / "exclude-control.json").write_text(json.dumps({"excluded": {"duckdb": sorted(allc - set(control))}}) + "\n")
    print("selected", len(order), "killed at prime", sum(1 for k in order if k in killed), "control", len(control), "of", len(allc))


if __name__ == "__main__":
    main()
