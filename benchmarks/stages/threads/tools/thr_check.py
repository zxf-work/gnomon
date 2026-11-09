#!/usr/bin/env python3
"""Thread-matched control: did the configuration reach the system? Judged from the record's own reported settings.

    thr_check.py applied <record.json> <cfg>        one window's record
    thr_check.py applied-dir <point dir> <cfg>      every answer-check record of the configuration's system in a point directory

cfg = g<n> (Gnomon: settings.designs.match_threads and GNOMON_MATCH_THREADS = n), d<n> (DuckDB (SQL): settings.threads = n),
p<n> (PostgreSQL: the session's max_parallel_workers_per_gather = n, and the server holding at least 33 background-worker
slots and 33 parallel workers when n > 0). Exit 1 when a record says otherwise. A missing record is reported and is not an error here
(the gate counts it; the Gnomon stop rule stops a g32 window without one).
"""
import json
import re
import sys
from pathlib import Path

SYS = {"g": "native", "d": "duckdbsql", "p": "postgres"}


def problem(rec: dict, cfg: str):
    """None when the record's settings match the configuration, else a one-line reason."""
    kind, n = cfg[0], int(cfg[1:])
    if rec.get("system") != SYS[kind]:
        return f"record is of system {rec.get('system')!r}, configuration {cfg} is {SYS[kind]!r}"
    s = rec.get("settings") or {}
    if kind == "g":
        got, env = (s.get("designs") or {}).get("match_threads"), (s.get("gnomon_env") or {}).get("GNOMON_MATCH_THREADS")
        if got != n or str(env) != str(n):
            return f"Gnomon match_threads {got!r} / GNOMON_MATCH_THREADS {env!r}, wanted {n}"
    elif kind == "d":
        got = s.get("threads")
        if str(got) != str(n):
            return f"DuckDB (SQL) threads {got!r}, wanted {n}"
    else:
        got = s.get("max_parallel_workers_per_gather")
        if str(got) != str(n):
            return f"PostgreSQL max_parallel_workers_per_gather {got!r}, wanted {n}"
        if n > 0:
            try:
                mwp, mpw = int(s.get("max_worker_processes", 0)), int(s.get("max_parallel_workers", 0))
            except ValueError:
                return f"PostgreSQL max_worker_processes/max_parallel_workers unreadable: {s.get('max_worker_processes')!r}, {s.get('max_parallel_workers')!r}"
            if mwp < n + 1 or mpw < n + 1:
                return f"PostgreSQL max_worker_processes {mwp}, max_parallel_workers {mpw}: too few for {n} workers"
    return None


def main() -> int:
    mode, target, cfg = sys.argv[1], Path(sys.argv[2]), sys.argv[3]
    if mode == "applied":
        files = [target]
    elif mode == "applied-dir":
        pat = re.compile(rf"verify-{SYS[cfg[0]]}(?:-[A-Z])?\.json")
        files = [f for f in sorted(target.glob("verify-*.json")) if pat.fullmatch(f.name)]
    else:
        print(__doc__)
        return 2
    bad = 0
    for f in files:
        if not f.exists():
            print(f"THR CHECK {cfg}: record {f} is missing (window failed?)")
            continue
        rec = json.loads(f.read_text())
        if rec.get("partial"):
            print(f"THR CHECK {cfg}: {f.name}: partial record (window cut), settings not recorded; not judged")
            continue
        why = problem(rec, cfg)
        print(f"THR CHECK {cfg}: {f.name}: {'APPLIED' if why is None else 'NOT APPLIED: ' + why}")
        bad += why is not None
    if mode == "applied-dir" and not files:
        print(f"THR CHECK {cfg}: no answer-check record in {target}")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
