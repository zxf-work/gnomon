#!/usr/bin/env python3
"""The shared timed runner for W3, W4 and W6.

    timed_runner.py <w3|w4|w6tmg|w6gm> --system S [catalog args] --rotation R --out BLOCK.json
                    [--jsonl CALLS.jsonl] [--engine-pass] [--label L] [--protocol W,R,SW,SR]

One run = one block: every cell of one system in one rotation (the job's shell starts and stops the
servers and orders the systems per rotation, as the kit's windows do). Per-workload query catalogs

1 warm-up + 3 timed): per (system, query, setting) one priming call, recorded, not timed; per instance
W warm-ups + R timed calls, and SW warm-ups + SR timed calls when the first warm-up is above
TR_SLOW_S (1 s). The client ceiling is TR_CEILING_S (10 s): a call above it is censored ("> 10 s"),
the instance stops there and the rest of its calls are not run.

Every system runs in a worker process (fork; the system is opened there):
  - the protocol loop runs inside the worker, calls back to back; e2e is measured there around fn()
    (the call and its fetch), engine time (EXTCMP_ENGINE_TIME=1) with the in-process probe;
  - the parent receives one result per instance; its extra wall time is the harness hop
    (`harness_hop_ms`, outside e2e);
  - abort: the worker writes each call's start time into shared memory; the parent kills the worker
    when a call has run past the ceiling + TR_GRACE_S. A kill, a worker death, or any cell that ended in
    a timeout / interrupt / ceiling restarts the worker (a labelled `worker_events` entry, cold cache), and
    the cell is primed again before its next instance.

Records (the fields extcmp's records carry, so one reporting code reads every workload):
  BLOCK.json  {system, workload, label, rotation, pass, manifest {engine_commit, so_sha256, kit_head},
               settings {designs, gnomon_env, threads, ...}, protocol, host, results: [cell]}
  cell        {query, setting, instance, ok, p50_ms, p95_ms, n, protocol, capped, censored, aborted,
               timings_ms, warmup_ms, hash, rows, error, harness_hop_ms, worker_events,
               engine_time {source, warmup, timed, summary}}
  CALLS.jsonl one line per call (phase prime|warm|timed, e2e_ms, engine record, status)
"""
from __future__ import annotations

import argparse
import hashlib
import json
import multiprocessing as mp
import os
import re
import socket
import statistics as st
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import engine_time as ET  # noqa: E402
import tr_catalogs as TC  # noqa: E402
import cov_mark as CM  # noqa: E402

CEIL_S = float(os.environ.get("TR_CEILING_S", "10"))
GRACE_S = float(os.environ.get("TR_GRACE_S", "5"))
SLOW_MS = float(os.environ.get("TR_SLOW_S", "1")) * 1e3
#: the priming call is outside the ceiling: it may answer after CEIL_S without
#: censoring the cell, and the parent kills it only at TR_PRIME_KILL_S. After every worker restart each (query,
#: setting) is primed again, so a fresh worker's cold state is never charged to a cell's ceiling.
import prime_cap as PC  # noqa: E402
PRIME_KILL_S = PC.PRIME_KILL_S
TIMEOUT_RE = re.compile(r"TransactionTimedOut|statement timeout|QueryCanceled|execution timeout|timed out|timeout|"
                        r"Interrupt|TransientError|ceiling|cancel", re.I)


def utc() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


# --------------------------------------------------------------------------- #
# worker
# --------------------------------------------------------------------------- #
def _one(B, payload_job, watch, wlim=None, kill_s=None):
    """One call: (status, e2e ms, engine record, hash, rows, error). `wlim` holds the parent's kill threshold
    for this call (the ceiling + grace, or PRIME_KILL_S for a priming call)."""
    q, s, payload = payload_job
    if wlim is not None:
        wlim.value = kill_s if kill_s is not None else CEIL_S + GRACE_S
    try:
        fn, hasher = B.make(q, s, payload)
    except NotImplementedError as e:
        return ("not-expressible", None, None, None, None, str(e)[:300])
    if B.probe is not None:
        B.probe.begin()
    watch.value = time.time()
    t = time.perf_counter()
    try:
        res = fn()
        ms = (time.perf_counter() - t) * 1e3
    except NotImplementedError as e:
        watch.value = 0.0
        return ("not-expressible", None, None, None, None, str(e)[:300])
    except Exception as e:                                       # noqa: BLE001
        ms = (time.perf_counter() - t) * 1e3
        watch.value = 0.0
        msg = f"{type(e).__name__}: {e}"[:600]
        rec = B.probe.end(ms) if B.probe is not None else None
        return ("censored" if (TIMEOUT_RE.search(msg) or ms > CEIL_S * 1e3) else "error", ms, rec, None, None, msg)
    watch.value = 0.0
    rec = B.probe.end(ms) if B.probe is not None else None
    try:
        h, n = hasher(res)
    except Exception as e:                                       # noqa: BLE001
        h, n = f"hash-error {type(e).__name__}: {e}"[:200], None
    return ("censored" if ms > CEIL_S * 1e3 else "ok", ms, rec, h, n, None)


def _instance(B, job, proto, watch, wlim=None):
    """The protocol of one instance (prime first when asked): every call's result, in order."""
    q, s, payload, prime = job
    wu, reps, swu, sreps = proto
    calls = []
    if prime:
        CM.mark("prime_start")
        with PC.priming():                   # the systems' own caps at PRIME_KILL_S - 2 s, reset after it
            r = _one(B, (q, s, payload), watch, wlim, max(PRIME_KILL_S, CEIL_S + GRACE_S))
        CM.mark("prime_done")
        calls.append(("prime",) + r)
        # a prime that answered after the ceiling is not a censored cell: the prime is outside the ceiling
        if not (r[0] == "ok" or (r[0] == "censored" and r[5] is None)):
            return calls
    r = _one(B, (q, s, payload), watch, wlim)
    calls.append(("warm",) + r)
    if r[0] != "ok":
        return calls
    slow = r[1] > SLOW_MS
    for _ in range((swu if slow else wu) - 1):
        r = _one(B, (q, s, payload), watch, wlim)
        calls.append(("warm",) + r)
        if r[0] != "ok":
            return calls
    for _ in range(sreps if slow else reps):
        r = _one(B, (q, s, payload), watch, wlim)
        calls.append(("timed",) + r)
        if r[0] != "ok":
            return calls
    return calls


def _serve(conn, a, watch, wlim=None):
    open_ = TC.CATALOGS[a.wl][1]
    try:
        CM.mark("open_start")
        B = open_(a)
        CM.mark("open_done")
        ET.CURRENT = B.probe if B.probe is not None else ET.NULL
        conn.send(("ready", B.settings, B.probe.source if B.probe is not None else "none"))
    except Exception as e:                                       # noqa: BLE001
        conn.send(("setup-error", f"{type(e).__name__}: {e}"[:600], None))
        return
    while True:
        job = conn.recv()
        if job is None:
            return
        (qsp, prime), proto = job
        t0 = time.perf_counter()
        calls = _instance(B, (*qsp, prime), proto, watch, wlim)
        conn.send(("done", calls, time.perf_counter() - t0))


class Worker:
    def __init__(self, a):
        self.a = a
        self.p = self.c = None
        self.events: list = []
        self.start()

    def start(self):
        ctx = mp.get_context("fork")
        self.c, child = ctx.Pipe()
        self.watch = ctx.Value("d", 0.0, lock=False)
        self.wlim = ctx.Value("d", CEIL_S + GRACE_S, lock=False)
        self.p = ctx.Process(target=_serve, args=(child, self.a, self.watch, self.wlim), daemon=True)
        self.p.start()
        child.close()
        if not self.c.poll(3600):
            self.kill()
            raise SystemExit("worker did not open the system within 3600 s")
        kind, settings, source = self.c.recv()
        if kind != "ready":
            raise SystemExit(f"worker setup failed: {settings}")
        self.settings, self.source = settings, source

    def kill(self):
        if self.p is not None and self.p.is_alive():
            self.p.kill()
            self.p.join(30)
        self.p = None

    def restart(self, why, query):
        self.kill()
        self.events.append({"utc": utc(), "event": why, "query": query, "cold_cache": True})
        self.start()

    def run(self, job, proto):
        """(calls, hop s) for one instance; a kill or death yields a censored / error last call."""
        if self.p is None or not self.p.is_alive():
            self.restart("worker not alive before the call: fresh worker", job[0])
        t0 = time.perf_counter()
        self.watch.value = 0.0
        self.c.send((job, proto))
        while not self.c.poll(0.1):
            stv = self.watch.value
            lim = self.wlim.value
            if stv and time.time() - stv > lim:
                self.restart(f"a call ran past its kill threshold ({lim:g} s): killed", job[0])
                return [("timed", "censored", lim * 1e3, None, None, None,
                         f"> {CEIL_S:g} s: killed at {lim:g} s (worker restarted, cold cache)")], None
            if not self.p.is_alive():
                break
        try:
            kind, calls, wt = self.c.recv()
        except EOFError:
            self.p.join(10)
            code = self.p.exitcode
            why = ", SIGKILL: likely out of memory" if code == -9 else ""
            self.restart(f"the worker died inside the instance (exit code {code}{why})", job[0])
            return [("timed", "error", None, None, None, None, f"worker died (exit code {code}{why})")], None
        return calls, max(0.0, time.perf_counter() - t0 - wt)

    def close(self):
        if self.p is not None and self.p.is_alive():
            try:
                self.c.send(None)
                self.p.join(30)
            except Exception:                                    # noqa: BLE001
                pass
        self.kill()


# --------------------------------------------------------------------------- #
# parent
# --------------------------------------------------------------------------- #
def manifest() -> dict:
    repo = Path(os.environ.get("EXTCMP_REPO", HERE.parent))
    so = sorted(repo.glob("gnomon/_engine*.so"))
    out = {"engine_commit": os.environ.get("EXTCMP_ENGINE_COMMIT"),
           "so": str(so[0]) if so else None,
           "so_sha256": hashlib.sha256(so[0].read_bytes()).hexdigest() if so else None}
    try:
        out["kit_head"] = subprocess.run(["git", "-C", str(repo), "rev-parse", "HEAD"], capture_output=True,
                                         text=True, timeout=30).stdout.strip()
        out["kit_dirty"] = bool(subprocess.run(["git", "-C", str(repo), "status", "--porcelain", "--untracked-files=no"],
                                               capture_output=True, text=True, timeout=30).stdout.strip())
    except Exception:                                            # noqa: BLE001
        pass
    return out


def p50_p95(v):
    v = sorted(v)
    if not v:
        return None, None
    return round(st.median(v), 4), round(v[min(len(v) - 1, int(0.95 * len(v)))], 4)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("wl", choices=sorted(TC.CATALOGS))
    ap.add_argument("--system", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--jsonl")
    ap.add_argument("--rotation", default="1")
    ap.add_argument("--label", default="")
    ap.add_argument("--engine-pass", action="store_true")
    ap.add_argument("--protocol", help="W,R,SW,SR (overrides the workload's)")
    for k in ("target", "params", "cells", "hist-db", "store", "db", "queries", "work", "uri", "load", "workload",
              "derived", "instances", "n-instances"):
        ap.add_argument("--" + k)
    ap.add_argument("--settings", default="current,asof,window")
    ap.add_argument("--beliefs", default="current,historic")
    ap.add_argument("--asof", default="load,now")
    ap.add_argument("--gran", default="op")
    ap.add_argument("--threads", type=int, default=64)
    ap.add_argument("--memory-limit", default="64GB")
    a = ap.parse_args()
    if a.engine_pass and not ET.ENABLED:
        raise SystemExit("--engine-pass needs EXTCMP_ENGINE_TIME=1")
    proto = tuple(int(x) for x in a.protocol.split(",")) if a.protocol else \
        (TC.ENGINE_PASS if a.engine_pass else TC.PROTOCOLS[a.wl])
    cells = TC.CATALOGS[a.wl][0](a)
    t_start = utc()
    W = Worker(a)
    jl = open(a.jsonl, "a") if a.jsonl else None
    results = []
    primed: set = set()
    for (q, s, inst, payload) in cells:
        key = (q, s)
        if W.p is None or not W.p.is_alive():
            primed.clear()                    # the next run() starts a fresh worker: every cell is primed again
        prime = key not in primed
        ev0 = len(W.events)
        calls, hop = W.run(((q, s, payload), prime), proto)
        primed.add(key)
        warm = [c for c in calls if c[0] == "warm"]
        timed = [c for c in calls if c[0] == "timed"]
        last = calls[-1] if calls else None
        status = last[1] if last else "error"
        ok = status == "ok" and timed and all(c[1] == "ok" for c in timed)
        slow = bool(warm) and warm[0][2] is not None and warm[0][2] > SLOW_MS
        wu, reps, swu, sreps = proto
        cell = {"query": q, "setting": s, "instance": inst, "ok": bool(ok),
                "protocol": (f"slow: {swu} warmup + {sreps} reps (first warmup > {SLOW_MS / 1e3:g} s)" if slow
                             else f"standard: {wu} warmups + {reps} reps") + ("; primed" if prime else ""),
                "timings_ms": [round(c[2], 4) for c in timed if c[1] == "ok"],
                "warmup_ms": [round(c[2], 4) for c in warm if c[2] is not None],
                "capped": slow, "harness_hop_ms": None if hop is None else round(hop * 1e3, 3)}
        if prime and calls and calls[0][0] == "prime":
            cell["prime_ms"] = None if calls[0][2] is None else round(calls[0][2], 4)
        if ok:
            cell["p50_ms"], cell["p95_ms"] = p50_p95(cell["timings_ms"])
            cell["n"] = len(cell["timings_ms"])
            cell["hash"], cell["rows"] = timed[-1][4], timed[-1][5]
        else:
            cell["error"] = last[6] if last else "no call"
            cell["censored"] = status == "censored"
            cell["aborted"] = status == "censored"
            cell["not_expressible"] = status == "not-expressible"
            if status == "censored":
                cell["error"] = f">{CEIL_S:g} s, aborted ({cell['error']})"
        if ET.ENABLED:
            recs_t = [c[3] for c in timed if c[1] == "ok"]
            cell["engine_time"] = {"source": W.source, "warmup": [c[3] for c in warm], "timed": recs_t,
                                   "summary": ET.summarize(recs_t)}
        if len(W.events) > ev0:
            cell["worker_events"] = W.events[ev0:]
        # after an interrupted / timed-out / killed call: a fresh worker, and the cell is primed again
        if status in ("censored", "error") and (len(W.events) == ev0):
            W.restart(f"after a {status} call ({str(cell.get('error'))[:80]}): fresh worker", q)
            cell.setdefault("worker_events", []).extend(W.events[ev0:])
        if len(W.events) > ev0:
            primed.clear()                    # a fresh worker: every (query, setting) is primed again

        results.append(cell)
        if jl:
            for c in calls:
                jl.write(json.dumps({"wl": a.wl, "system": a.system, "rotation": a.rotation, "label": a.label,
                                     "pass": "engine" if a.engine_pass else "timed", "query": q, "setting": s,
                                     "instance": inst, "phase": c[0], "status": c[1],
                                     "e2e_ms": None if c[2] is None else round(c[2], 4), "engine": c[3],
                                     "hash": c[4], "rows": c[5], "error": c[6]}, default=str) + "\n")
            jl.flush()
        shown = cell.get("p50_ms", cell.get("error"))
        print(f"  {a.system:<14} {q:<10} {s:<9} {str(inst):<10} ok={cell['ok']} {str(shown)[:90]}", flush=True)
    W.close()
    block = {"system": a.system, "workload": a.wl, "label": a.label, "rotation": a.rotation,
             "pass": "engine" if a.engine_pass else "timed", "manifest": manifest(), "settings": W.settings,
             "protocol": {"warmups": proto[0], "reps": proto[1], "slow_warmups": proto[2], "slow_reps": proto[3],
                          "slow_s": SLOW_MS / 1e3, "ceiling_s": CEIL_S, "grace_s": GRACE_S,
                          "prime": "per (query, setting) and again after every worker restart; outside the ceiling "
                                   f"(killed at {max(PRIME_KILL_S, CEIL_S + GRACE_S):g} s)",
                          "worker": "every call in a worker process; e2e measured in the worker"},
             "engine_time_probe": W.source if ET.ENABLED else None,
             "host": socket.gethostname(), "job": os.environ.get("SLURM_JOB_ID"), "start": t_start, "end": utc(),
             "worker_events": W.events, "results": results}
    Path(a.out).write_text(json.dumps(block, indent=1, default=str) + "\n")
    n_ok = sum(1 for c in results if c["ok"])
    print(f"done {a.wl} {a.system} rotation {a.rotation}: {n_ok}/{len(results)} cells ok, "
          f"{len(W.events)} worker restarts -> {a.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
