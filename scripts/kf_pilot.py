#!/usr/bin/env python3
"""timed runner (W3, W4, W6 T-mgBench, W6 T-gMark). W2 and W5 use extcmp.py (P1 protocol).

    kf_pilot.py w3    --system S --target T --params P --cells CELLS --instances 0,1,2 --hist-db DB --out X.jsonl
    kf_pilot.py w4    --system S [--store ST | --db DB] --params P --queries Q,.. --beliefs current,historic --out X.jsonl
    kf_pilot.py w6tmg --system S --work W [--store ST] [--uri U] --cells Q1-TT,.. --out X.jsonl
    kf_pilot.py w5match --system gnomon --store ST --params match-<tag>.json --n-instances 3 --out X.jsonl
    kf_pilot.py w6gm  --system S [--store ST] [--uri U --load L.json] --workload W.xml --queries 2,3 --asof load,now --out X.jsonl

CELLS (w3): "IS1@current+asof+window,BI15@current+asof,IC13@window".

Protocol (the timed shape): per cell one priming call (instance 0, recorded, not timed), then per
instance 2 warm-ups + 5 timed calls; when the first warm-up takes more than 1 s, 1 warm-up + 3 timed.
The 10 s ceiling is on the client call (PILOT_CEILING_S): a call above it is censored ("> 10 s") and
the instance stops there. Every call runs in one worker process per system (forked; the system is
opened there); the parent kills and restarts the worker when a call outlives the ceiling by 5 s.
The e2e timer and the engine-time probe (EXTCMP_ENGINE_TIME=1) are both inside the worker, around
the same fn() the workload's check driver calls; the answer is hashed after the timer.
"""
from __future__ import annotations

import argparse
import json
import multiprocessing as mp
import os
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import engine_time as ET  # noqa: E402

CEIL = float(os.environ.get("PILOT_CEILING_S", "10"))
SLOW_MS = 1000.0


# --------------------------------------------------------------------------- #
# per workload: setup(a) -> (calls: list[(cell, setting, inst_id, payload)], make(a) -> runner)
# runner(payload) -> (fn, hasher); fn is the timed call, hasher(result) -> (hash, n)
# --------------------------------------------------------------------------- #
def w3_calls(a):
    want = {}
    for c in a.cells.split(","):
        q, s = c.split("@")
        want[q] = s.split("+")
    insts = [int(x) for x in a.instances.split(",")]
    by = {}
    for line in open(a.params):
        r = json.loads(line)
        if r["qid"] in want and r["i"] in insts:
            by[(r["qid"], r["i"])] = r
    out = []
    for q, sets in want.items():
        for s in sets:
            for i in insts:
                if (q, i) in by:
                    out.append((q, s, i, by[(q, i)]))
    return out


def w3_runner(a):
    import w3_snb_run as R
    S = R.make_system(a.system, a.target)
    pr = ET.probe("native" if a.system == "gnomon" else a.system.split("-")[0])
    if ET.ENABLED and a.system == "duckdb":
        pr = ET.DuckProbe()
        S.con = pr.wrap(S.con)
    elif ET.ENABLED and a.system == "postgres":
        pr = ET.PgProbe(a.target)

    def make(payload):
        q, s, inst = payload

        def fn():
            return S.run(q, inst, s)

        def hasher(res):
            cols, rows = res
            rows = R.canon_rows(rows)
            if q in R.COMPARE_LAST:
                rows = [r[-1:] for r in rows]
            return R.hrows(rows), len(rows)
        return fn, hasher
    return make, pr


def w4_calls(a):
    doc = json.loads(Path(a.params).read_text())
    want = set(a.queries.split(","))
    bel = set(a.beliefs.split(","))
    n = int(a.n_instances)
    seen: dict = {}
    out = []
    for i in doc["instances"]:
        if i["query"] not in want or i["belief"] not in bel:
            continue
        k = (i["query"], i["belief"])
        seen[k] = seen.get(k, 0) + 1
        if seen[k] <= n:
            out.append((i["query"], i["belief"], i["instance"], i))
    return out


def w4_runner(a):
    import w4_check as C
    import w4_queries as Q
    state: dict = {}
    pr = ET.probe("native" if a.system == "gnomon" else a.system)
    wrap = None
    if ET.ENABLED and a.system == "duckdbsql":
        pr = ET.DuckProbe()
        wrap = pr.wrap
    ex = C.executor(a.system, Path(a.store) if a.store else None, Path(a.db) if a.db else None,
                    int(CEIL), state, wrap_con=wrap)

    def make(payload):
        q, _b, inst = payload
        p = inst["params"]

        def fn():
            return ex(q, p)

        def hasher(res):
            rows = Q.canonical(q, res)
            return C.h(rows), len(rows)
        return fn, hasher
    return make, pr


def w6tmg_calls(a):
    import w6_tmgbench as T
    params, _oracle = T.params_for(Path(a.work), "op", None)
    cells = a.cells.split(",")
    out = []
    for c in cells:
        qid, form = c.split("-")
        ps = params[qid]
        pick = []
        for cls in ("hot", "warm", "cold"):          # one instance per class: 3 instances
            for n, p in enumerate(ps):
                if p["class"] == cls:
                    pick.append((n, p)); break
        for n, p in pick:
            out.append((qid, form, n, p))
    return out


def w6tmg_runner(a):
    import w6_tmgbench as T
    ns = argparse.Namespace(system=a.system, store=a.store, uri=a.uri, db=a.db, threads=a.threads,
                            memory_limit=a.memory_limit)
    S = T.make_system(ns, Path(a.work))
    if a.system.startswith("aeong-native"):
        S.ensure_marks()
        ps, _ = T.params_for(Path(a.work), "op", None)
        needed = {p["marker"] for v in ps.values() for p in v} | \
                 {p["marker2"] for v in ps.values() for p in v if "marker2" in p}
        S.map_markers(needed)
    pr = ET.probe("native" if a.system.startswith("gnomon") else
                  ("aeong" if a.system.startswith("aeong") else a.system))
    if ET.ENABLED and a.system == "duckdbsql":
        pr = ET.DuckProbe()
        S.con = pr.wrap(S.con)

    def make(payload):
        qid, form, p = payload
        f = S.fn(qid, form, p)

        def hasher(raw):
            ans = S.project(qid, form, raw)
            return T.h(ans), (len(ans) if hasattr(ans, "__len__") else None)
        return f, hasher
    return make, pr


def w6gm_calls(a):
    import tgmark as TG
    qs = {q["id"]: q for q in TG.parse_workload(Path(a.workload))}
    out = []
    for qid in [int(x) for x in a.queries.split(",")]:
        for at in a.asof.split(","):
            out.append((f"q{qid}", at, 0, qs[qid]))
    return out


def w6gm_runner(a):
    import tgmark as TG
    pr = ET.probe("native" if a.system == "gnomon" else "aeong")
    if a.system == "gnomon":
        import gnomon
        import gnomon.temporal.ops_match as OM
        from gnomon.temporal.algebra import ensure_all_registered
        ensure_all_registered()
        store = gnomon.open(Path(a.store), backend="native", read_only=True)
        tts = sorted({int(t) for t in store.adapter.versions_columnar("edge")["tt_s"]})

        def make(payload):
            _c, at, q = payload
            as_of = (tts[-2] if len(tts) > 1 else tts[-1]) if at == "load" else None
            args = TG.match_args(q, as_of)

            def fn():
                return OM.match(store.adapter, args, route="native")

            def hasher(payload_):
                rows = TG._payload_rows(q, payload_)
                return TG.digest_rows(q["head"], rows), len(rows)
            fn._keep = store
            return fn, hasher
        return make, pr
    import w6_tgmark as WG
    form = a.system.split("-", 1)[1]                # aeong-native | aeong-property
    x = int(json.loads(Path(a.load).read_text())["marks"]["0"])
    d = WG.bolt(a.uri)

    def make(payload):
        _c, at, q = payload
        cy = WG.text(q, form, at, x, "as_written")

        def fn():
            with d.session() as s:
                return [tuple(r.values()) for r in s.run(cy)]

        def hasher(rows):
            head = q["head"]
            uniq = sorted(set(rows)) if head else ([()] if rows else [])
            return TG.digest_rows(head, uniq[:TG.PAGE]), len(uniq)
        return fn, hasher
    return make, pr


def w5match_calls(a):
    doc = json.loads(Path(a.params).read_text())
    n = int(a.n_instances)
    seen: dict = {}
    out = []
    for p in doc["patterns"]:
        c = p["class"].split(" ")[0]                     # the pattern kind (holders, ...)
        seen[c] = seen.get(c, 0) + 1
        if seen[c] <= n:
            out.append((c, "match", p["id"], p))
    return out


def w5match_runner(a):
    import gnomon
    from gnomon.temporal.algebra import ensure_all_registered
    from gnomon.temporal.ops_match import match
    from extcmp import answer_hash
    ensure_all_registered()
    st = gnomon.open(a.store, backend="native", read_only=True)
    pr = ET.probe("native")

    def make(payload):
        _c, _s, p = payload
        args = dict(p["args"]); args.setdefault("limit", 10000)

        def fn():
            return match(st.adapter, args, route="native")

        def hasher(res):
            return answer_hash(res), len(res.get("rows", []))
        fn._keep = st
        return fn, hasher
    return make, pr


WL = {"w3": (w3_calls, w3_runner), "w4": (w4_calls, w4_runner),
      "w6tmg": (w6tmg_calls, w6tmg_runner), "w6gm": (w6gm_calls, w6gm_runner),
      "w5match": (w5match_calls, w5match_runner)}


# --------------------------------------------------------------------------- #
# worker: opens the system once, answers one call per request
# --------------------------------------------------------------------------- #
def worker(conn, a):
    try:
        make, pr = WL[a.wl][1](a)
    except Exception as e:                                       # noqa: BLE001
        conn.send(("setup-error", f"{type(e).__name__}: {e}"[:600]))
        return
    conn.send(("ready", pr.source))
    while True:
        job = conn.recv()
        if job is None:
            return
        try:
            fn, hasher = make(job)
        except NotImplementedError as e:
            conn.send(("not-expressible", str(e)[:300], None, None, None)); continue
        except Exception as e:                                   # noqa: BLE001
            conn.send(("error", f"{type(e).__name__}: {e}"[:600], None, None, None)); continue
        pr.begin()
        t = time.perf_counter()
        try:
            res = fn()
            ms = (time.perf_counter() - t) * 1e3
        except NotImplementedError as e:
            conn.send(("not-expressible", str(e)[:300], None, None, None)); continue
        except Exception as e:                                   # noqa: BLE001
            ms = (time.perf_counter() - t) * 1e3
            conn.send(("error", f"{type(e).__name__}: {e}"[:600], ms, pr.end(ms), None)); continue
        rec = pr.end(ms)
        try:
            hh = hasher(res)
        except Exception as e:                                   # noqa: BLE001
            hh = (f"hash-error {type(e).__name__}: {e}"[:200], None)
        conn.send(("ok", None, ms, rec, hh))


class Worker:
    def __init__(self, a):
        self.a, self.p, self.c = a, None, None

    def start(self):
        ctx = mp.get_context("fork")
        self.c, child = ctx.Pipe()
        self.p = ctx.Process(target=worker, args=(child, self.a), daemon=True)
        self.p.start()
        if not self.c.poll(3600):
            raise SystemExit("worker did not open the system within 3600 s")
        kind, msg = self.c.recv()
        if kind != "ready":
            raise SystemExit(f"worker setup failed: {msg}")
        self.source = msg

    def call(self, payload):
        if self.p is None or not self.p.is_alive():
            self.start()
        self.c.send(payload)
        if not self.c.poll(CEIL + 5):
            self.p.kill(); self.p.join(); self.p = None
            return ("timeout", f"> {CEIL:g} s: no answer within the ceiling + 5 s (worker killed)", None, None, None)
        return self.c.recv()

    def close(self):
        if self.p is not None and self.p.is_alive():
            self.c.send(None); self.p.join(10)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("wl", choices=sorted(WL))
    ap.add_argument("--system", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--label", default="")
    for k in ("target", "params", "cells", "hist-db", "store", "db", "queries", "work", "uri", "load", "workload",
              "asof", "memory_limit"):
        ap.add_argument("--" + k)
    ap.add_argument("--instances", default="0,1,2")
    ap.add_argument("--n-instances", default="3")
    ap.add_argument("--beliefs", default="current,historic")
    ap.add_argument("--threads", type=int, default=64)
    ap.add_argument("--first-only", action="store_true",
                    help="only the first instance of each cell (the uninstrumented overhead control)")
    a = ap.parse_args()
    a.wl = a.wl
    if a.memory_limit is None:
        a.memory_limit = "64GB"
    calls = WL[a.wl][0](a)
    if a.first_only:
        seen, keep = set(), []
        for c in calls:
            if (c[0], c[1]) not in seen:
                seen.add((c[0], c[1])); keep.append(c)
        calls = keep
    W = Worker(a)
    W.start()
    out = open(a.out, "a")

    def emit(cell, setting, inst, phase, r):
        kind, err, ms, rec, hh = r
        d = {"wl": a.wl, "label": a.label, "engine_time": ET.ENABLED, "system": a.system, "cell": cell, "setting": setting, "instance": inst,
             "phase": phase, "status": kind if not (kind == "ok" and ms is not None and ms > CEIL * 1e3) else "censored",
             "e2e_ms": None if ms is None else round(ms, 4), "engine": rec,
             "hash": hh[0] if hh else None, "n": hh[1] if hh else None, "error": err,
             "job": os.environ.get("SLURM_JOB_ID"), "host": os.uname().nodename}
        out.write(json.dumps(d, default=str) + "\n"); out.flush()
        return d

    primed = set()
    t_all = time.time()
    for cell, setting, inst, payload in calls:
        job = (cell, setting, payload)
        if (cell, setting) not in primed:
            primed.add((cell, setting))
            emit(cell, setting, inst, "prime", W.call(job))
        d = emit(cell, setting, inst, "warm", W.call(job))
        if d["status"] != "ok":
            print(f"{a.system:<14} {cell:<10} {setting:<9} i={inst} {d['status']} {str(d['error'])[:90]}", flush=True)
            continue
        slow = d["e2e_ms"] > SLOW_MS
        if not slow:
            d = emit(cell, setting, inst, "warm", W.call(job))
        e2e = []
        for _ in range(3 if slow else 5):
            d = emit(cell, setting, inst, "timed", W.call(job))
            if d["status"] != "ok":
                break
            e2e.append(d["e2e_ms"])
        e2e.sort()
        med = e2e[len(e2e) // 2] if e2e else None
        eng = (d.get("engine") or {}).get("engine_ms")
        print(f"{a.system:<14} {cell:<10} {setting:<9} i={inst} e2e_p50={med} last_engine={eng} "
              f"status={d['status']} n={d['n']}", flush=True)
    W.close()
    print(f"done {a.wl} {a.system}: {len(calls)} instances in {time.time() - t_all:.0f} s; probe: {W.source}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
