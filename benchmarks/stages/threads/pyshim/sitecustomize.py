"""Thread-matched control: the PostgreSQL parallel-worker setting, applied at the connection.

The kit's PostgreSQL client (scripts/pg_baseline.py) sets max_parallel_workers_per_gather = 4 at the start of every session
(SESSION_TUNING, applied by apply_tuning), which overrides the server's command-line value, and the kit has no switch for
it. This module (put first on PYTHONPATH by the stage's jobs; Python imports `sitecustomize` at start-up) changes that one
session setting, and only when EXTCMP_THR_PG_WORKERS is set, without editing the kit:

  EXTCMP_THR_PG_WORKERS=<n>   max_parallel_workers_per_gather = n for every session of the process
  EXTCMP_THR_PG_PLANS=1       (answer-check windows only) every session also loads auto_explain with log_min_duration = 0
                              and log_analyze off, so the server log holds each statement's plan with "Workers Planned"

It also adds the settings that bound or price parallelism to the settings the records report, so
every record of a PostgreSQL window carries the values the server actually had. Unset, nothing happens and nothing is
imported. The patch is applied when `pg_baseline` is first imported (a lazy import hook), and it fails loudly (the process
exits) if the setting cannot be applied: a silent no-op would void the control.
"""
import importlib.machinery
import os
import sys

_W = os.environ.get("EXTCMP_THR_PG_WORKERS", "").strip()
_PLANS = os.environ.get("EXTCMP_THR_PG_PLANS", "") == "1"
_REPORT = ("max_worker_processes", "max_parallel_workers", "max_parallel_workers_per_gather", "parallel_setup_cost",
           "parallel_tuple_cost", "min_parallel_table_scan_size", "min_parallel_index_scan_size",
           "parallel_leader_participation", "jit_above_cost")


def _patch(mod):
    n = int(_W)
    if not 0 <= n <= 1024:
        raise SystemExit(f"EXTCMP_THR_PG_WORKERS={_W}: out of range")
    mod.SESSION_TUNING["max_parallel_workers_per_gather"] = str(n)
    for k in _REPORT:
        if k not in mod.SERVER_TUNING_REPORTED:
            mod.SERVER_TUNING_REPORTED.append(k)
    orig = mod.apply_tuning

    def apply_tuning(conn):
        out = orig(conn)
        if _PLANS:
            try:
                conn.execute("LOAD 'auto_explain'")
                conn.execute("SET auto_explain.log_min_duration = 0")
                conn.execute("SET auto_explain.log_analyze = off")
                conn.execute("SET auto_explain.log_format = 'text'")
                out["auto_explain"] = "loaded: log_min_duration 0, log_analyze off"
            except Exception as e:                          # noqa: BLE001 - recorded, not fatal (untimed answer check only)
                out["auto_explain"] = f"unavailable: {type(e).__name__}: {e}"[:200]
        got = out.get("max_parallel_workers_per_gather")
        if got != str(n):
            raise SystemExit(f"THR OVERRIDE FAILED: session has max_parallel_workers_per_gather={got}, wanted {n}")
        return out

    mod.apply_tuning = apply_tuning
    mod.THR_PG_WORKERS = n


class _Finder:
    busy = False

    def find_spec(self, name, path=None, target=None):
        if name != "pg_baseline" or _Finder.busy:
            return None
        _Finder.busy = True
        try:
            spec = importlib.machinery.PathFinder.find_spec(name, path)
        finally:
            _Finder.busy = False
        if spec is not None and spec.loader is not None:
            orig = spec.loader.exec_module

            def exec_module(module, _orig=orig):
                _orig(module)
                _patch(module)

            spec.loader.exec_module = exec_module
        return spec


if _W:
    sys.meta_path.insert(0, _Finder())
