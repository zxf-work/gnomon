#!/usr/bin/env python3
"""C4 causes (coverage audit item 12): where the time of one query instance goes, on one system, from what the system itself exposes.
    causes_probe.py w3 --system gnomon --target STORE --params P --cells IC13@current,... --instances 0,3 --out D
    causes_probe.py w4 --system postgres --params P --queries TSR1,... --beliefs historic --n-instances 2 [--store S --db D] --out D
    causes_probe.py w5 --system native|duckdbsql|postgres|neo4j --log L --ref S --workdir W --size-tag T --params F --params2 F2 --cases FILE --out D

For every case: one prime, 2 warm-ups, 3 timed calls (end-to-end, engine time with EXTCMP_ENGINE_TIME=1). Then:
  Gnomon: GNOMON_MATCH_TRACE's plan-node lines of the last call (file trace-<case>.txt), the engine's phase counters across the last call, the operator calls it made.
  DuckDB, PostgreSQL: every SELECT the last call issued, under EXPLAIN ANALYZE (PostgreSQL with BUFFERS).
  Neo4j, Memgraph: every read statement of the last call under PROFILE.
A diagnostic run, not a timed result: it enters no table of times. No engine change; the system classes are the kit's. One JSON line per case in <out>/<system>.jsonl."""
import argparse
import json
import os
import re
import statistics as st
import sys
import time
from pathlib import Path

CAP = []
READ = ("select", "with", "match", "optional", "call", "return", "unwind")


def patch_all():
    try:
        import psycopg
        orig = psycopg.Cursor.execute

        def ex(self, query, params=None, **kw):
            CAP.append(("pg", query if isinstance(query, str) else query.as_string(self.connection), params, self.connection))
            return orig(self, query, params, **kw)
        psycopg.Cursor.execute = ex
    except ImportError:
        pass
    try:
        import neo4j
        o1 = neo4j.Session.run

        def srun(self, query, parameters=None, **kw):
            t = query.text if isinstance(query, neo4j.Query) else query
            CAP.append(("bolt", t, {**(parameters or {}), **kw}, None))
            return o1(self, query, parameters, **kw)
        neo4j.Session.run = srun
        o2 = neo4j.Transaction.run

        def trun(self, query, parameters=None, **kw):
            t = query.text if isinstance(query, neo4j.Query) else query
            CAP.append(("bolt", t, {**(parameters or {}), **kw}, None))
            return o2(self, query, parameters, **kw)
        neo4j.Transaction.run = trun
    except ImportError:
        pass
    import engine_time as ET
    owrap = ET.DuckProbe.wrap

    class CapCon:
        def __init__(self, c):
            self._c = c

        def execute(self, q, *a, **k):
            CAP.append(("duck", q, a[0] if a else None, self._c))
            return self._c.execute(q, *a, **k)

        def __getattr__(self, n):
            return getattr(self._c, n)

    ET.DuckProbe.wrap = lambda self, con: CapCon(owrap(self, con))


def explain(limit=6):
    out, seen = [], set()
    for kind, text, params, con in list(CAP):
        t = text.strip()
        low = re.sub(r"^(\s*(--[^\n]*\n|//[^\n]*\n|/\*.*?\*/))+", "", t, flags=re.S).lower().lstrip("( \n\t")
        if not low.startswith(READ) or low.startswith(("select pg_", "select current_setting", "call db.await")) or "pg_stat_statements" in low or t in seen:
            continue
        seen.add(t)
        if len(out) >= limit:
            out.append({"more": True})
            break
        rec = {"kind": kind, "text": t[:6000]}
        try:
            if kind == "duck":
                # the engine-time probe narrows the profiler's metrics; operator timings and row counts for this call only
                import engine_time as ET
                raw = getattr(con, "_con", con)
                raw.execute("PRAGMA custom_profiling_settings = '" + json.dumps({"OPERATOR_TIMING": "true", "OPERATOR_CARDINALITY": "true"}) + "'")
                try:
                    rows = raw.execute("EXPLAIN ANALYZE " + t, params) if params else raw.execute("EXPLAIN ANALYZE " + t)
                    rec["plan"] = "\n".join(str(r[-1]) for r in rows.fetchall())
                finally:
                    raw.execute("PRAGMA custom_profiling_settings = '" + json.dumps(ET._DUCK_SETTINGS) + "'")
            elif kind == "pg":
                rows = con.execute("EXPLAIN (ANALYZE, BUFFERS) " + t, params).fetchall()
                rec["plan"] = "\n".join(r[0] for r in rows)
            else:
                import neo4j
                sysname = os.environ["CAUSES_BOLT"]
                uri = os.environ["GNOMON_NEO4J_URI" if sysname == "neo4j" else "GNOMON_MEMGRAPH_URI"]
                with neo4j.GraphDatabase.driver(uri, auth=("neo4j", "neo4j") if sysname == "neo4j" else ("", "")) as d, d.session() as s:
                    res = s.run("PROFILE " + t, params or {})
                    rows = [dict(r) for r in res]
                    summ = res.consume()
                    rec["profile"] = summ.profile or summ.plan or None
                    if sysname == "memgraph":
                        rec["table"] = rows
                    rec["t_first_ms"], rec["t_last_ms"] = summ.result_available_after, summ.result_consumed_after
        except Exception as e:      # noqa: BLE001
            rec["error"] = f"{type(e).__name__}: {e}"[:400]
        out.append(rec)
    return out


class Trace:
    """fd 2 to one file for one call (the engine's eprintln), else to /dev/null."""

    def __init__(self):
        self.saved = os.dup(2)

    def to(self, path):
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
        os.dup2(fd, 2)
        os.close(fd)

    def restore(self):
        os.dup2(self.saved, 2)


def call(B, fn, hasher):
    if B.probe is not None:
        B.probe.begin()
    t = time.perf_counter()
    try:
        res = fn()
        ms = (time.perf_counter() - t) * 1e3
        err = None
    except Exception as e:      # noqa: BLE001
        ms, res, err = (time.perf_counter() - t) * 1e3, None, f"{type(e).__name__}: {e}"[:400]
    rec = B.probe.end(ms) if B.probe is not None else None
    h = n = None
    if err is None:
        try:
            h, n = hasher(res)
        except Exception as e:      # noqa: BLE001
            err = f"hash: {type(e).__name__}: {e}"[:200]
    return ms, rec, h, n, err


def one(B, a, q, s, payload, label, adapter, tr, cap_calls):
    fn, hasher = B.make(q, s, payload)
    out = {"case": label, "system": a.system, "utc": time.strftime("%FT%TZ", time.gmtime())}
    os.makedirs(a.out, exist_ok=True)
    safe = label.replace("/", "_").replace("@", "_at_").replace("|", "_")
    tr.to("/dev/null")
    calls = []
    h = n = None
    for ph in ("prime", "warm", "warm", "timed", "timed"):
        ms, rec, h, n, err = call(B, fn, hasher)
        calls.append({"phase": ph, "e2e_ms": round(ms, 3), "engine": rec, "err": err})
        if err:
            break
    if not calls[-1]["err"]:
        CAP.clear()
        if adapter is not None:
            adapter.reset_phase_stats()
        if cap_calls is not None:
            cap_calls.obj.capture = []
        tr.to(os.path.join(a.out, f"trace-{a.system}-{safe}.txt"))
        ms, rec, h, n, err = call(B, fn, hasher)
        tr.to("/dev/null")
        calls.append({"phase": "timed", "e2e_ms": round(ms, 3), "engine": rec, "err": err})
        if adapter is not None:
            out["phases"] = {k: v for k, v in adapter.phase_stats().items() if v}
        if cap_calls is not None:
            out["operator_calls"] = [{"op": c["op"], "args": json.dumps(c["args"], default=str)[:1500]} for c in cap_calls.obj.capture]
            cap_calls.obj.capture = None
        if a.system not in ("gnomon", "native"):
            out["statements"] = explain()
    tr.restore()
    timed = [c for c in calls if c["phase"] == "timed" and not c["err"]]
    out["calls"] = calls
    out["e2e_ms_median"] = st.median(c["e2e_ms"] for c in timed) if timed else None
    out["hash"], out["rows"] = h, n
    out["error"] = next((c["err"] for c in calls if c["err"]), None)
    print(json.dumps({k: out[k] for k in ("case", "e2e_ms_median", "hash", "rows", "error")}, default=str), flush=True)
    return out


class CapRef:
    def __init__(self, obj):
        self.obj = obj


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("wl", choices=["w3", "w4", "w5"])
    ap.add_argument("--system", required=True)
    ap.add_argument("--out", required=True)
    for k in ("target", "params", "cells", "hist-db", "store", "db", "queries", "instances", "n-instances", "log", "ref", "workdir", "size-tag", "cases"):
        ap.add_argument("--" + k)
    ap.add_argument("--params2")
    ap.add_argument("--settings", default="current")
    ap.add_argument("--beliefs", default="historic")
    a = ap.parse_args()
    if os.environ.get("EXTCMP_ENGINE_TIME") != "1":
        raise SystemExit("EXTCMP_ENGINE_TIME=1 is needed")
    patch_all()
    if a.system in ("neo4j", "memgraph"):
        os.environ["CAUSES_BOLT"] = a.system
    import tr_catalogs as TC
    tr = Trace()
    adapter = cap = None
    cases = []
    if a.wl in ("w3", "w4"):
        if a.wl == "w3":
            sys.path.insert(0, os.path.join(os.environ["EXTCMP_REPO"], "benchmarks/stages/w3/tools"))
            import w3_timed     # the stage's Cypher text overrides, as the timed runner applies them
            w3_timed.apply_override()
        cells = TC.CATALOGS[a.wl][0](a)
        B = TC.CATALOGS[a.wl][1](a)
        cases = [(q, s, p, f"{q}@{s}#{i}") for (q, s, i, p) in cells]
        if a.system == "gnomon":
            if a.wl == "w3":
                adapter, cap = B.keep.ad, CapRef(B.keep)
            else:
                adapter = B.keep["store"].adapter
    else:
        import engine_time as ET
        import eval_harness as H
        import extcmp as X
        import extcmp_p1 as P1
        data = X.dataset(Path(a.log))
        wanted = json.load(open(a.cases))
        ids = sorted({c["query"] for c in wanted})
        qs = X.queries(data, ids)
        docs = {"instances": [i for f in (a.params, a.params2) if f for i in json.load(open(f))["instances"]]}
        qs, insts = P1.expand(qs, docs)
        want = {(c["query"], c["instance"]) for c in wanted}
        cls = X._system_class(a.system)
        ref = Path(a.ref) if a.ref else None
        so = cls(ref, Path(a.workdir), a.size_tag)
        so.log = Path(a.log)
        so.open()
        probe = ET.attach(a.system, so)

        class BB:
            pass
        B = BB()
        B.probe = probe

        def make(q, s, inst):
            f = so.fn_for(q)

            def hasher(p):
                return X.answer_hash(p) + ":" + X.projected_hash(p), int(H._answer_size(p))
            return f, hasher
        B.make = make
        cases = [(q, "registry", i, f"{q.id}#{i['instance']}") for q, i in zip(qs, insts) if (q.id, i["instance"]) in want]
        if a.system == "native":
            adapter = so.adapter
    if not cases:
        raise SystemExit("no case selected")
    os.makedirs(a.out, exist_ok=True)
    with open(os.path.join(a.out, f"{a.system}.jsonl"), "a") as f:
        for (q, s, p, label) in cases:
            f.write(json.dumps(one(B, a, q, s, p, label, adapter, tr, cap), default=str) + "\n")
            f.flush()
    print(f"{a.system}: {len(cases)} cases", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
