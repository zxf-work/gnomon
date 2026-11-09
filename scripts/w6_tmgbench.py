#!/usr/bin/env python3
"""W6 T-mgBench (AeonG's own workload, head to head): loaders in both load modes
for every system, the query texts, and the untimed answer checks.

The frozen inputs are made by `extcmp_p6_aeong.py gen/build` (P6's driver, with
`--day-every` for the per-day log); this driver reads them and never writes them.

    gnomon-load  --work W --gran op|batch|day --path replay|live --store S --out F
        replay: `eval_harness.load_store` (the campaign's bulk replay; each batch is one
                engine commit at its logged tt, fsynced; no event-log record)
        live:   `Store._write` per batch (the writer's path: event-log record fsynced
                before apply, then the engine commit; store-assigned tt), with the
                compaction trigger consulted as replay does. GNOMON_SMALL_COMMIT=1 in the
                environment selects the small-commit path.
    load   --system S --work W --gran G [--mode maint|bulk] --uri/--db ... --out F
        aeong-native (the generator's statements; op = one auto-commit each, batch/day =
        one explicit transaction per 100 / 10,000 operations with its marker),
        aeong-property / neo4j (four-timestamp property form, Cypher maintenance
        statements per operation, or bulk from a Gnomon store's version rows),
        duckdbsql (the same model in SQL tables).
    check  --system S --work W --gran G [--store/--uri/--db] --out F
        every instance of Q1-Q4 in TT and VT form against the oracle; per cell
        verdicts, differing instances with their answers and the query text.

Systems answer the same 21 instances per query (P6's curated parameters,
`params.json`; the per-day stores answer `params-day.json`). Answers are compared as
content (per vertex the collapsed state sequence; per one-hop query the multiset of
out-neighbour state sequences), exactly as P6.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import statistics
import sys
import threading
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Callable

sys.path.insert(0, str(Path(__file__).resolve().parent))
import extcmp_p6_aeong as P6  # noqa: E402
from extcmp_p6_aeong import OPEN, cj, h, proj_nodes_seq, sha_file, state_of, utc  # noqa: E402

QIDS, FORMS = P6.QIDS, P6.FORMS
GRAN = {"op": ("eventlog.jsonl", 1), "batch": ("eventlog-grouped.jsonl", 100),
        "day": ("eventlog-day.jsonl", 10_000)}
#: rows per base-graph statement; halved on an AeonG ceiling abort down to 50 (W3 D-W3-9)
CHUNK = 5000


def read_batches(path: Path):
    with open(path, encoding="utf-8") as f:
        f.readline()                                   # the header
        for line in f:
            r = json.loads(line)
            yield r["tt"], r["ops"]


def params_for(work: Path, gran: str, derived: Path | None = None):
    """the parameters a store of this granularity answers, and their oracle: the frozen
    one for per-operation stores, `oracle-{batch,day}.json` from `oracle` (TT answers of the
    coarser commit) when `derived` is given"""
    pf = "params-day.json" if gran == "day" else "params.json"
    params = json.loads((work / pf).read_text())["instances"]
    if gran != "op" and derived is not None:
        return params, json.loads((derived / f"oracle-{gran}.json").read_text())
    return params, json.loads((work / ("oracle-day.json" if gran == "day" else "oracle.json")).read_text())


def _pct(lat: list) -> dict:
    """Latency percentiles in ms of a list of seconds (audit #7, commit cost)."""
    if not lat:
        return {}
    x = sorted(lat)
    return {"median": round(1e3 * x[len(x) // 2], 4), "p95": round(1e3 * x[int(0.95 * len(x))], 4), "p99": round(1e3 * x[int(0.99 * len(x))], 4), "n": len(x)}


def du(p: Path) -> int:
    return sum(f.stat().st_size for f in Path(p).rglob("*") if f.is_file())


# --------------------------------------------------------------------------- #
# Gnomon                                                                       #
# --------------------------------------------------------------------------- #

def cmd_gnomon_load(a) -> int:
    import gnomon
    work, store = Path(a.work), Path(a.store)
    log = work / GRAN[a.gran][0]
    if store.exists():
        raise SystemExit(f"{store} exists")
    rec: dict[str, Any] = {"system": "gnomon", "gran": a.gran, "path": a.path, "log": str(log),
                           "log_sha256": sha_file(log), "small_commit_env": {
                               k: os.environ.get(k) for k in ("GNOMON_SMALL_COMMIT", "GNOMON_SMALL_COMMIT_N",
                                                              "GNOMON_SMALL_COMMIT_MS")},
                           "host": os.uname().nodename, "job": os.environ.get("SLURM_JOB_ID"), "start": utc()}
    t0 = time.perf_counter()
    if a.path == "replay":
        sys.path.insert(0, str(Path(a.repo) / "scripts"))
        import eval_harness as H
        H.load_store(store, "native", log)
        rec["durability"] = ("each batch one engine commit at its logged tt: segments, dictionary and "
                             "manifest fsynced before the generation is published; no event-log record")
    else:
        from gnomon.storage.eventlog import REPLAY_GC_EVERY_COMPACTIONS
        st = gnomon.open(store, backend="native")
        ad = st.adapter
        sc = ad.small_commit() if hasattr(ad, "small_commit") else {}
        every = int(sc.get("n", 1) or 1) if sc.get("on") else 1
        n = comp = 0
        lat = []
        for _tt, ops in read_batches(log):
            t = time.perf_counter()
            st._write(ops)                              # noqa: SLF001 - the writer's own path
            lat.append(time.perf_counter() - t)
            n += 1
            if n % every == 0 and ad.needs_compaction():
                ad.compact_incremental()
                comp += 1
                if comp % REPLAY_GC_EVERY_COMPACTIONS == 0:
                    ad.gc(keep_last=2)
        if comp:
            ad.gc(keep_last=2)
        st.close()
        lat.sort()
        rec |= {"transactions": n, "compactions": comp, "small_commit": sc, "compaction_check_every": every,
                "commit_ms": {"median": round(1e3 * lat[len(lat) // 2], 4),
                              "p95": round(1e3 * lat[int(0.95 * len(lat))], 4),
                              "p99": round(1e3 * lat[int(0.99 * len(lat))], 4)},
                "durability": ("Store._write: the event-log record is appended and fsynced before apply "
                               "(acknowledged then); the engine commit follows (small-commit: published "
                               "every n transactions or ms)")}
    rec["load_s"] = round(time.perf_counter() - t0, 3)
    st = gnomon.open(store, backend="native", read_only=True)
    rec["stats"] = st.stats()
    if a.digest:
        t = time.perf_counter()
        rec["content_digest"] = st.adapter.content_digest_streaming() if hasattr(
            st.adapter, "content_digest_streaming") else st.adapter.content_digest()
        rec["digest_s"] = round(time.perf_counter() - t, 1)
    try:
        rec["designs"] = st.adapter.designs()
    except Exception as e:                              # noqa: BLE001
        rec["designs"] = str(e)[:200]
    st.close()
    log_b = (store / "eventlog.jsonl").stat().st_size if (store / "eventlog.jsonl").exists() else 0
    rec |= {"store_bytes": du(store), "store_eventlog_bytes": log_b, "store_bytes_without_log": du(store) - log_b,
            "end": utc()}
    Path(a.out).write_text(json.dumps(rec, indent=1, default=str))
    print(json.dumps(rec, default=str))
    return 0


def _cmp(op, l, r):
    return {"cmp": op, "l": l, "r": r}


def _col(c):
    return {"col": c}


def _lit(v):
    return {"lit": v}


def diag(v: str, t: int, t2: int) -> list:
    """Recorded as current at some x in [t, t2): [max(tt_s, vt_s), min(tt_e, vt_e)) meets
    [t, t2). The scope's valid window already gives vt_s < t2 and vt_e > t."""
    return [_cmp("<", _col(f"{v}.tt_s"), _col(f"{v}.tt_e")),
            _cmp("<", _col(f"{v}.tt_s"), _lit(t2)), _cmp(">", _col(f"{v}.tt_e"), _lit(t)),
            _cmp("<", _col(f"{v}.tt_s"), _col(f"{v}.vt_e")), _cmp("<", _col(f"{v}.vt_s"), _col(f"{v}.tt_e"))]


class GnomonMatch:
    """Gnomon through `match`, one pattern per AeonG query: the vertex (Q1, Q2), and for
    Q3/Q4 a second call for its out-edges and their targets. TT form: belief at the
    as-of point (Q1, Q3) or every belief recorded as current inside the range (Q2, Q4:
    belief `all` with the diagonal predicate, AeonG's `TT FROM`); VT form: current
    belief, valid at t or overlapping [t, t2)."""

    name = "gnomon"

    def __init__(self, store: Path):
        import gnomon
        from gnomon.temporal import ops_match as OM
        from gnomon.temporal.algebra import call_operator, ensure_all_registered
        ensure_all_registered()
        self.OM, self.call = OM, call_operator
        self.store = gnomon.open(store, backend="native", read_only=True)
        self.a = self.store.adapter
        self.routes: dict[str, int] = defaultdict(int)

    def settings(self):
        try:
            return {"designs": self.a.designs(), "operator": "match"}
        except Exception as e:                          # noqa: BLE001
            return {"designs": str(e)[:200]}

    @staticmethod
    def expressible(qid, form, density):
        return True

    def _run(self, args) -> list:
        rows, cur = [], None
        while True:
            before = self.OM.ROUTES["native"]
            p = self.call(self.a, "match", {**args, "cursor": cur})
            self.routes["native" if self.OM.ROUTES["native"] > before else "reference"] += 1
            rows += p["rows"]
            cur = p.get("cursor")
            if not p.get("truncated") or not cur:
                return rows

    @staticmethod
    def scope(qid, form, p):
        t, t2 = p["t"], p.get("t2")
        if qid in ("Q1", "Q3"):
            return {"vt": {"instant": t}, **({"as_of_tt": t} if form == "TT" else {})}
        sc = {"vt": {"window": {"t_a": t, "t_b": t2}, "mode": "overlap"}}
        return {**sc, "belief": "all"} if form == "TT" else sc

    def args(self, qid, form, p) -> list[dict]:
        uid, t, t2 = p["uid"], p["t"], p.get("t2")
        sc = self.scope(qid, form, p)
        rng = qid in ("Q2", "Q4") and form == "TT"
        node = {"pattern": {"nodes": [{"var": "n", "uids": [uid]}], **({"where": diag("n", t, t2)} if rng else {})},
                "return": {"rows": [{"as": c, "expr": _col(f"n.{c}")} for c in ("vid", "props", "vt_s", "tt_s")]},
                "scope": sc, "limit": 10_000}
        if qid in ("Q1", "Q2"):
            return [node]
        where = (diag("n", t, t2) + diag("e", t, t2) + diag("m", t, t2)) if rng else []
        out = {"pattern": {"nodes": [{"var": "n", "uids": [uid]}, {"var": "m"}],
                           "steps": [{"from": "n", "to": "m", "dir": "out", "var": "e"}],
                           **({"where": where} if where else {})},
               "return": {"rows": [{"as": "eid", "expr": _col("e.eid")}, {"as": "vid", "expr": _col("m.vid")},
                                   {"as": "props", "expr": _col("m.props")}, {"as": "vt_s", "expr": _col("m.vt_s")},
                                   {"as": "tt_s", "expr": _col("m.tt_s")}]},
               "scope": sc, "limit": 10_000}
        return [node, out]

    def text(self, qid, form, p):
        return json.dumps(self.args(qid, form, p))

    def fn(self, qid, form, p):
        calls = self.args(qid, form, p)

        def f():
            return [self._run(c) for c in calls]
        return f

    @staticmethod
    def project(qid, form, res):
        def key(r):
            return r["vt_s"] if form == "VT" else max(r["vt_s"], r["tt_s"])

        def seq(rows):
            uniq = {r["vid"]: r for r in rows}
            return proj_nodes_seq([(key(r), r["props"]) for r in uniq.values()])
        ans = {"n": seq(res[0])}
        if qid in ("Q3", "Q4"):
            per = defaultdict(list)
            for r in res[1]:
                per[r["eid"]].append(r)
            ans["out"] = sorted((seq(v) for v in per.values()), key=cj)
        return ans

    def close(self):
        self.store.close()


class GnomonEH(P6.Gnomon):
    """P6's Gnomon form (`entity_history` compositions), kept for continuity."""

    name = "gnomon-eh"

    def __init__(self, store: Path):
        import gnomon
        from gnomon.temporal.algebra import call_operator, ensure_all_registered
        ensure_all_registered()
        self.call = call_operator
        self.store = gnomon.open(store, backend="native", read_only=True)
        self.a = self.store.adapter

    def text(self, qid, form, p):
        return f"P6 entity_history composition ({qid}-{form})"

    def project(self, qid, form, raw):
        return P6.Gnomon.project(qid, raw)

    def fn(self, qid, form, p):
        return P6.Gnomon.fn(self, qid, form, p)


# --------------------------------------------------------------------------- #
# Bolt helpers: chunked writes under AeonG's ceiling (W3 D-W3-9)               #
# --------------------------------------------------------------------------- #

def bolt(uri):
    import neo4j
    return neo4j.GraphDatabase.driver(uri, auth=("neo4j", "neo4j"))


class TxRunner:
    """One writer session. `tx(stmts)` commits a list of (cypher, params) in one explicit
    transaction; AeonG's "conflicting transactions" retries the whole transaction;
    a one-statement UNWIND aborted at the ceiling is split in halves down to 50 rows (W3
    per-transaction timeout lifts its server default (the 10 s ceiling) for loads."""

    def __init__(self, uri, system):
        self.drv = bolt(uri)
        self.system = system
        self.s = self.drv.session()
        self.commits = 0
        self.retries: list = []
        self.splits: list = []
        self.slowest = [0.0, None]
        self.lat: list = []

    def _begin(self):
        if self.system == "neo4j":
            return self.s.begin_transaction(timeout=3600)
        return self.s.begin_transaction()

    def tx(self, stmts, tag=None):
        for attempt in range(6):
            t = time.time()
            try:
                tx = self._begin()
                for cy, kw in stmts:
                    tx.run(cy, **kw).consume()
                tx.commit()
                self.lat.append(time.time() - t)
                self.commits += 1
                if time.time() - t > self.slowest[0]:
                    self.slowest = [round(time.time() - t, 3), tag]
                return
            except Exception as e:                      # noqa: BLE001 - conflict and ceiling handled
                msg = str(e)
                try:
                    self.s.close()
                except Exception:                       # noqa: BLE001
                    pass
                self.s = self.drv.session()
                rows = stmts[0][1].get("rows") if len(stmts) == 1 else None
                if "asked to abort" in msg and rows is not None and len(rows) > 50:
                    half = len(rows) // 2
                    self.splits.append([tag, len(rows), round(time.time() - t, 1)])
                    cy, kw = stmts[0]
                    self.tx([(cy, {**kw, "rows": rows[:half]})], tag)
                    self.tx([(cy, {**kw, "rows": rows[half:]})], tag)
                    return
                if "conflicting transactions" not in msg or attempt == 5:
                    raise
                self.retries.append([tag, attempt + 1])
                time.sleep(2)

    def auto(self, cy, **kw):
        """one auto-commit statement (AeonG's own load: the authors' client)"""
        for attempt in range(6):
            try:
                t = time.time()
                self.s.run(cy, **kw).consume()
                self.lat.append(time.time() - t)
                self.commits += 1
                return
            except Exception as e:                      # noqa: BLE001
                if "conflicting transactions" not in str(e) or attempt == 5:
                    raise
                self.retries.append(["auto", attempt + 1])
                time.sleep(2)

    def record(self):
        return {"commits": self.commits, "retries": self.retries, "splits": self.splits,
                "slowest_commit": self.slowest, "tx_ms": _pct(self.lat)}

    def close(self):
        self.s.close()
        self.drv.close()


def ceiling_check(uri, ceiling_s, heavy):
    """W3 P13: a query far past the
    ceiling must be aborted by the server at about the ceiling."""
    d = bolt(uri)
    out: dict = {}

    def go():
        t = time.time()
        try:
            with d.session() as s:
                s.run(heavy).consume()
            out.update(aborted=False, s=round(time.time() - t, 2))
        except Exception as e:                          # noqa: BLE001
            out.update(aborted=True, s=round(time.time() - t, 2), error=str(e)[:200])
    th = threading.Thread(target=go, daemon=True)
    th.start()
    th.join(ceiling_s * 3 + 30)
    if th.is_alive():
        out.update(aborted=False, s=None, note="still running at 3 x ceiling + 30 s")
    why = out.get("error", "").lower()
    out["enforced"] = (bool(out.get("aborted")) and ("timeout" in why or "abort" in why or "terminat" in why)
                       and out.get("s") is not None and out["s"] <= ceiling_s * 1.5 + 2)
    out["ceiling_s"] = ceiling_s
    out["verdict"] = "ceiling enforced" if out["enforced"] else "ceiling not enforceable"
    d.close()
    return out


# --------------------------------------------------------------------------- #
# AeonG native (the generator's statements, AeonG's own TT texts)              #
# --------------------------------------------------------------------------- #

class AeonGNativeW6(P6.AeonGNative):
    """P6's AeonG native path with the commit granularity as a parameter. op: one
    auto-commit transaction per statement and one per marker (the authors' load, from
    one session in operation order); batch/day: one explicit transaction per 100 /
    10,000 operations, the base statements grouped alike, the group's marker its last
    statement (so the marker's commit time is the group's)."""

    def __init__(self, work: Path, uri: str):
        self.work = work
        self.uri = uri
        self.drv = bolt(uri)
        self.x: dict[int, int] = {}

    def load_w6(self, index_file: Path, gran: str) -> dict:
        G = GRAN[gran][1]
        if gran == "day":
            G = int(json.loads((self.work / "build-meta.json").read_text()).get("day_every", G))
        R = TxRunner(self.uri, "aeong")
        t0 = time.perf_counter()
        for line in open(index_file):
            if line.strip():
                R.auto(line.strip())
        R.auto("CREATE INDEX ON :TTMark(k)")
        t1 = time.perf_counter()
        n = defaultdict(int)
        tb = None
        buf: list = []
        ops_in_group = 0
        for line in open(self.work / "aeong_native.jsonl", encoding="utf-8"):
            kind, q = json.loads(line)
            if kind != "base" and tb is None:
                if buf:
                    R.tx(buf, "base")
                    buf = []
                tb = time.perf_counter()
                R.lat.clear()
            n[kind] += 1
            if G == 1:
                R.auto(q)
                continue
            if kind == "mark":
                k = int(q.split("k: ")[1].split("}")[0])
                if (k + 1) % G == 0:
                    buf.append((q, {}))
                    R.tx(buf, f"group k={k}")
                    buf, ops_in_group = [], 0
                continue
            buf.append((q, {}))
            if kind == "base" and len(buf) >= G:
                R.tx(buf, "base")
                buf = []
        if buf:
            R.tx(buf, "tail")
        t2 = time.perf_counter()
        marks = {}
        with self.drv.session() as s:
            for r in s.run("MATCH (m:TTMark) RETURN m"):
                marks[int(r["m"]["k"])] = int(dict(r["m"].items())["transaction_ts"])
        self.marks_ts = marks
        return {"load_s": round(t2 - t0, 3), "gran": gran, "statements_per_group": G,
                "phases": {"indexes_s": round(t1 - t0, 3), "base_s": round(tb - t1, 3),
                           "history_s": round(t2 - tb, 3), "statements": dict(n)},
                "markers": len(marks), **R.record()}

    def ensure_marks(self):
        if not getattr(self, "marks_ts", None):
            with self.drv.session() as s:
                self.marks_ts = {int(r["m"]["k"]): int(dict(r["m"].items())["transaction_ts"])
                                 for r in s.run("MATCH (m:TTMark) RETURN m")}

    def text(self, qid, form, p=None):
        return P6.AeonGNative.text(self, qid, form if p is None else p)

    def fn(self, qid, form, p):
        q = P6.AeonGNative.text(self, qid, p)
        drv = self.drv

        def f():
            with drv.session() as s:
                return list(s.run(q))
        return f

    def project(self, qid, form, raw):
        return P6.AeonGNative.project(qid, raw)


# --------------------------------------------------------------------------- #
# the four-timestamp property form (AeonG property, Neo4j)                     #
# --------------------------------------------------------------------------- #

#: per-operation maintenance, the standard bitemporal update: an assertion over valid
#: [a, b) recorded at tt closes every open version it overlaps (tt_e = tt) and re-asserts
#: the parts outside [a, b); then the new version. Edges: create; retract at t closes the
#: open version and re-asserts [vt_s, t).
#: (AeonG's Memgraph 2.x base has no list comprehension with WHERE, so the two
#: remainders are their own statements, before the close, as in the SQL form)
_PU_COPY = ("uid: v.uid, id: v.id, completion_percentage: v.completion_percentage, gender: v.gender, "
            "age: v.age")
CY_LEFT = ("MATCH (e:PE {uid: $uid})-[:HAS_V]->(v:PU) WHERE v.tt_e = $open AND v.vt_s < $a "
           "AND v.vt_e > $a CREATE (e)-[:HAS_V]->(:PU {" + _PU_COPY + ", vt_s: v.vt_s, vt_e: $a, tt_s: $tt, "
           "tt_e: $open})")
CY_RIGHT = ("MATCH (e:PE {uid: $uid})-[:HAS_V]->(v:PU) WHERE v.tt_e = $open AND v.vt_s < $b "
            "AND v.vt_e > $b CREATE (e)-[:HAS_V]->(:PU {" + _PU_COPY + ", vt_s: $b, vt_e: v.vt_e, tt_s: $tt, "
            "tt_e: $open})")
CY_CLOSE = ("MATCH (e:PE {uid: $uid})-[:HAS_V]->(v:PU) WHERE v.tt_e = $open AND v.vt_s < $b "
            "AND v.vt_e > $a SET v.tt_e = $tt")
CY_NEW = ("MERGE (e:PE {uid: $uid}) ON CREATE SET e.id = $id "
          "CREATE (e)-[:HAS_V]->(:PU {uid: $uid, id: $id, completion_percentage: $cp, gender: $g, age: $age, "
          "vt_s: $a, vt_e: $b, tt_s: $tt, tt_e: $open})")
CY_EDGE = ("MATCH (a:PE {uid: $s}), (b:PE {uid: $d}) "
           "CREATE (a)-[:F {disc: $disc, vt_s: $a, vt_e: $b, tt_s: $tt, tt_e: $open}]->(b)")
CY_RETRACT = ("MATCH (a:PE {uid: $s})-[f:F {disc: $disc}]->(b:PE {uid: $d}) WHERE f.tt_e = $open AND f.vt_e > $t "
              "SET f.tt_e = $tt WITH a, b, f WHERE f.vt_s < $t "
              "CREATE (a)-[:F {disc: f.disc, vt_s: f.vt_s, vt_e: $t, tt_s: $tt, tt_e: $open}]->(b)")
CY_BASE_NODES = ("UNWIND $rows AS r CREATE (e:PE {uid: r.uid, id: r.id}) "
                 "CREATE (e)-[:HAS_V]->(:PU {uid: r.uid, id: r.id, completion_percentage: r.cp, gender: r.g, "
                 "age: r.age, vt_s: r.a, vt_e: r.b, tt_s: r.tt, tt_e: $open})")
CY_BASE_EDGES = ("UNWIND $rows AS r MATCH (a:PE {uid: r.s}), (b:PE {uid: r.d}) "
                 "CREATE (a)-[:F {disc: r.disc, vt_s: r.a, vt_e: r.b, tt_s: r.tt, tt_e: $open}]->(b)")


def node_kw(op, tt):
    pr = op["props"]
    return {"uid": op["uid"] if "uid" in op else op["ref"]["uid"], "id": pr.get("id"),
            "cp": pr.get("completion_percentage"), "g": pr.get("gender"), "age": pr.get("age"),
            "a": op["vt_s"], "b": op["vt_e"], "tt": tt, "open": OPEN}


def is_base(tt):
    """the base graph's batches (tt 1..7, before the workload's first operation at T0)"""
    return tt < P6.T0


def pnames(sql):
    return set(re.findall(r"\$([A-Za-z_][A-Za-z_0-9]*)", sql))


def cypher_stmts(tt, ops) -> list:
    """The maintenance statements of one logged batch, in order."""
    out = []
    for o in ops:
        if o["op"] in ("assert_node", "correct"):
            kw = node_kw(o, tt)
            for cy in (CY_LEFT, CY_RIGHT, CY_CLOSE, CY_NEW):
                out.append((cy, {k: kw[k] for k in pnames(cy)}))
        elif o["op"] == "assert_edge":
            out.append((CY_EDGE, {"s": o["src"], "d": o["dst"], "disc": o["disc"], "a": o["vt_s"], "b": o["vt_e"],
                                  "tt": tt, "open": OPEN}))
        elif o["op"] == "retract":
            r = o["ref"]
            out.append((CY_RETRACT, {"s": r["src"], "d": r["dst"], "disc": r["disc"], "t": o["t"], "tt": tt,
                                     "open": OPEN}))
        else:
            raise SystemExit(f"unexpected op {o['op']}")
    return out


def base_rows(tt, ops):
    nodes = [{"uid": o["uid"], "id": o["props"].get("id"), "cp": o["props"].get("completion_percentage"),
              "g": o["props"].get("gender"), "age": o["props"].get("age"), "a": o["vt_s"], "b": o["vt_e"], "tt": tt}
             for o in ops if o["op"] == "assert_node"]
    edges = [{"s": o["src"], "d": o["dst"], "disc": o["disc"], "a": o["vt_s"], "b": o["vt_e"], "tt": tt}
             for o in ops if o["op"] == "assert_edge"]
    return nodes, edges


class CypherProperty:
    """The kit's property form with all four timestamps as properties, every time
    predicate written in Cypher: (:PE entity)-[:HAS_V]->(:PU node version),
    (:PE)-[:F edge version]->(:PE). AeonG ("aeong-property") and Neo4j ("neo4j")."""

    def __init__(self, system, uri):
        self.name = system
        self.uri = uri
        self.drv = bolt(uri)

    IDX = {"aeong": ["CREATE INDEX ON :PE(uid)", "CREATE INDEX ON :PE(id)", "CREATE INDEX ON :PU(id)",
                     "CREATE INDEX ON :PU(uid)"],
           "neo4j": ["CREATE INDEX pe_uid IF NOT EXISTS FOR (n:PE) ON (n.uid)",
                     "CREATE INDEX pe_id IF NOT EXISTS FOR (n:PE) ON (n.id)",
                     "CREATE INDEX pu_id IF NOT EXISTS FOR (n:PU) ON (n.id)",
                     "CREATE INDEX pu_uid IF NOT EXISTS FOR (n:PU) ON (n.uid)"]}

    def kind(self):
        return "neo4j" if self.name == "neo4j" else "aeong"

    def settings(self):
        return {"schema": "(:PE {uid, id})-[:HAS_V]->(:PU {uid, id, props, vt_s, vt_e, tt_s, tt_e}); "
                          "(:PE)-[:F {disc, vt_s, vt_e, tt_s, tt_e}]->(:PE)", "indexes": self.IDX[self.kind()]}

    def load(self, work: Path, gran: str, mode: str, ref_store: Path | None = None) -> dict:
        R = TxRunner(self.uri, self.kind())
        t0 = time.perf_counter()
        for s in self.IDX[self.kind()]:
            R.auto(s)
        if self.kind() == "neo4j":
            R.auto("CALL db.awaitIndexes(600)")
        t1 = time.perf_counter()
        n_tx = n_stmt = 0
        tb = None
        if mode == "bulk":
            rec = self._bulk(R, ref_store)
        else:
            for tt, ops in read_batches(work / GRAN[gran][0]):
                if is_base(tt):
                    nodes, edges = base_rows(tt, ops)
                    for rows, cy in ((nodes, CY_BASE_NODES), (edges, CY_BASE_EDGES)):
                        for j in range(0, len(rows), CHUNK):
                            R.tx([(cy, {"rows": rows[j:j + CHUNK], "open": OPEN})], f"base tt={tt}")
                    continue
                if tb is None:
                    tb = time.perf_counter()
                    R.lat.clear()
                st = cypher_stmts(tt, ops)
                R.tx(st, f"tt={tt}")
                n_tx += 1
                n_stmt += len(st)
            rec = {}
        t2 = time.perf_counter()
        with self.drv.session() as s:
            got = {"PU": s.run("MATCH (v:PU) RETURN count(v) AS n").single()["n"],
                   "F": s.run("MATCH ()-[f:F]->() RETURN count(f) AS n").single()["n"],
                   "PE": s.run("MATCH (e:PE) RETURN count(e) AS n").single()["n"]}
        R.close()
        return {"load_s": round(t2 - t0, 3), "gran": gran, "mode": mode,
                "phases": {"indexes_s": round(t1 - t0, 3),
                           "base_s": round((tb or t2) - t1, 3), "history_s": round(t2 - (tb or t2), 3)},
                "history_transactions": n_tx, "history_statements": n_stmt, "counts": got,
                **R.record(), **rec}

    def _bulk(self, R, ref_store):
        """P6's bulk form: the Gnomon store's version rows (`AeonGProperty.load`), in
        chunks under the ceiling."""
        import gnomon
        st = gnomon.open(ref_store, backend="native", read_only=True)
        ad = st.adapter
        nv = list(ad.all_node_versions())
        ev = list(ad.all_edge_versions())
        st.close()
        ents = {}
        for v in nv:
            ents.setdefault(v.uid, v.props.get("id"))
        er = [{"uid": u, "id": i} for u, i in ents.items()]
        for j in range(0, len(er), CHUNK):
            R.tx([("UNWIND $rows AS r CREATE (:PE {uid: r.uid, id: r.id})", {"rows": er[j:j + CHUNK]})], "PE")
        rows = [{"uid": v.uid, "vt_s": v.vt_s, "vt_e": v.vt_e, "tt_s": v.tt_s, "tt_e": v.tt_e,
                 "id": v.props.get("id"), "cp": v.props.get("completion_percentage"),
                 "g": v.props.get("gender"), "age": v.props.get("age")} for v in nv]
        for j in range(0, len(rows), CHUNK):
            R.tx([("UNWIND $rows AS r MATCH (a:PE {uid: r.uid}) CREATE (a)-[:HAS_V]->(:PU {uid: r.uid, "
                   "vt_s: r.vt_s, vt_e: r.vt_e, tt_s: r.tt_s, tt_e: r.tt_e, id: r.id, completion_percentage: r.cp, "
                   "gender: r.g, age: r.age})", {"rows": rows[j:j + CHUNK]})], "PU")
        erows = [{"s": v.src, "d": v.dst, "disc": v.disc, "vt_s": v.vt_s, "vt_e": v.vt_e, "tt_s": v.tt_s,
                  "tt_e": v.tt_e} for v in ev]
        for j in range(0, len(erows), CHUNK):
            R.tx([("UNWIND $rows AS r MATCH (a:PE {uid: r.s}), (b:PE {uid: r.d}) CREATE (a)-[:F {disc: r.disc, "
                   "vt_s: r.vt_s, vt_e: r.vt_e, tt_s: r.tt_s, tt_e: r.tt_e}]->(b)", {"rows": erows[j:j + CHUNK]})],
                 "F")
        return {"bulk_rows": {"PE": len(er), "PU": len(rows), "F": len(erows)}}

    @staticmethod
    def expressible(qid, form, density):
        return True

    @staticmethod
    def _pred(var, form, point):
        """P6's predicates (`AeonGProperty._pred`), with a non-empty belief interval
        required in the TT range form (a version closed by a later operation of the same
        transaction has tt_s = tt_e)"""
        p = P6.AeonGProperty._pred(None, var, form, point)
        return f"{var}.tt_s < {var}.tt_e AND {p}" if (form == "TT" and not point) else p

    def text(self, qid, form, p=None):
        point = qid in ("Q1", "Q3")
        eid = "elementId(f)" if self.kind() == "neo4j" else "id(f)"
        if qid in ("Q1", "Q2"):
            return f"MATCH (v:PU {{id: $id}}) WHERE {self._pred('v', form, point)} RETURN v"
        return (f"MATCH (a:PE {{id: $id}}) OPTIONAL MATCH (a)-[:HAS_V]->(v:PU) WHERE {self._pred('v', form, point)} "
                f"WITH a, collect(v) AS ns "
                f"OPTIONAL MATCH (a)-[f:F]->(b:PE) WHERE {self._pred('f', form, point)} "
                f"OPTIONAL MATCH (b)-[:HAS_V]->(r:PU) WHERE {self._pred('r', form, point)} "
                f"RETURN ns, {eid} AS fvid, r")

    def fn(self, qid, form, p):
        q = self.text(qid, form)
        args = {"id": p["id"], "t": p["t"], "t2": p.get("t2", 0), "open": OPEN}
        drv = self.drv

        def f():
            with drv.session() as s:
                import prime_cap as PC
                if self.name == "neo4j" and PC.active():   # the prime's transaction timeout (prime_cap)
                    from neo4j import Query
                    return [dict(r) for r in s.run(Query(q, timeout=PC.active()), **args)]
                return [dict(r) for r in s.run(q, **args)]
        return f

    @staticmethod
    def project(qid, form, recs):
        return P6.AeonGProperty.project(qid, recs, form)

    def close(self):
        self.drv.close()


# --------------------------------------------------------------------------- #
# DuckDB (SQL): the same model in two tables                                   #
# --------------------------------------------------------------------------- #

SQL_SCHEMA = [
    "CREATE TABLE nv (uid VARCHAR, id BIGINT, cp BIGINT, gender VARCHAR, age BIGINT, "
    "vt_s BIGINT, vt_e BIGINT, tt_s BIGINT, tt_e BIGINT)",
    "CREATE TABLE ev (src VARCHAR, dst VARCHAR, disc VARCHAR, vt_s BIGINT, vt_e BIGINT, tt_s BIGINT, tt_e BIGINT)",
]
SQL_INDEX = ["CREATE INDEX nv_uid ON nv (uid)", "CREATE INDEX nv_id ON nv (id)", "CREATE INDEX ev_src ON ev (src)"]
#: maintenance of one node assertion over [a, b) at tt: the two remainders, the close,
#: the new version. A remainder never overlaps [a, b), so the close never reaches it; an
#: earlier operation of the same transaction (same tt) is closed with tt_e = tt, a version
#: never believed (zero belief interval), which no read admits.
SQL_NODE = [
    ("INSERT INTO nv SELECT uid, id, cp, gender, age, vt_s, $a, $tt, $open FROM nv "
     "WHERE uid = $uid AND tt_e = $open AND vt_s < $a AND vt_e > $a"),
    ("INSERT INTO nv SELECT uid, id, cp, gender, age, $b, vt_e, $tt, $open FROM nv "
     "WHERE uid = $uid AND tt_e = $open AND vt_s < $b AND vt_e > $b"),
    ("UPDATE nv SET tt_e = $tt WHERE uid = $uid AND tt_e = $open AND vt_s < $b AND vt_e > $a"),
    "INSERT INTO nv VALUES ($uid, $id, $cp, $g, $age, $a, $b, $tt, $open)",
]
SQL_EDGE = "INSERT INTO ev VALUES ($s, $d, $disc, $a, $b, $tt, $open)"
SQL_RETRACT = [
    ("INSERT INTO ev SELECT src, dst, disc, vt_s, $t, $tt, $open FROM ev "
     "WHERE src = $s AND dst = $d AND disc = $disc AND tt_e = $open AND vt_s < $t AND vt_e > $t"),
    ("UPDATE ev SET tt_e = $tt WHERE src = $s AND dst = $d AND disc = $disc AND tt_e = $open "
     "AND vt_e > $t"),
]


def sql_pred(v, form, point):
    if form == "TT":
        if point:
            return f"{v}.tt_s <= $t AND $t < {v}.tt_e AND {v}.vt_s <= $t AND $t < {v}.vt_e"
        return (f"{v}.tt_s < {v}.tt_e AND {v}.tt_s < {v}.vt_e AND {v}.vt_s < {v}.tt_e AND {v}.tt_s < $t2 AND "
                f"{v}.vt_s < $t2 AND {v}.tt_e > $t AND {v}.vt_e > $t")
    if point:
        return f"{v}.tt_e = $open AND {v}.vt_s <= $t AND $t < {v}.vt_e"
    return f"{v}.tt_e = $open AND {v}.vt_s < $t2 AND {v}.vt_e > $t"


class DuckSQL:
    """DuckDB (SQL): node versions `nv` and edge versions `ev` with four timestamps,
    ART indexes on nv(uid), nv(id), ev(src); every time predicate in SQL. One file
    database (WAL on; a commit writes and syncs the WAL)."""

    name = "duckdbsql"

    def __init__(self, db: Path, threads: int, memory_limit: str):
        import duckdb
        self.duckdb = duckdb
        self.db = db
        self.con = duckdb.connect(str(db))
        self.con.execute(f"SET threads = {threads}")
        self.con.execute(f"SET memory_limit = '{memory_limit}'")
        self.cfg = {"threads": threads, "memory_limit": memory_limit, "duckdb": duckdb.__version__,
                    "database": str(db)}

    def settings(self):
        r = self.con.execute("SELECT name, value FROM duckdb_settings() WHERE name IN "
                             "('threads', 'memory_limit', 'checkpoint_threshold', 'wal_autocheckpoint', "
                             "'enable_fsync_on_commit')").fetchall()
        return {**self.cfg, "settings": dict(r), "indexes": SQL_INDEX}

    def load(self, work: Path, gran: str, mode: str, ref_store: Path | None = None) -> dict:
        import pandas as pd
        c = self.con
        t0 = time.perf_counter()
        for s in SQL_SCHEMA:
            c.execute(s)
        n_tx = n_stmt = 0
        tb = None
        lat: list = []
        if mode == "bulk":
            import gnomon
            st = gnomon.open(ref_store, backend="native", read_only=True)
            nv = pd.DataFrame([{"uid": v.uid, "id": v.props.get("id"), "cp": v.props.get("completion_percentage"),
                                "gender": v.props.get("gender"), "age": v.props.get("age"), "vt_s": v.vt_s,
                                "vt_e": v.vt_e, "tt_s": v.tt_s, "tt_e": v.tt_e} for v in st.adapter.all_node_versions()])
            ev = pd.DataFrame([{"src": v.src, "dst": v.dst, "disc": v.disc, "vt_s": v.vt_s, "vt_e": v.vt_e,
                                "tt_s": v.tt_s, "tt_e": v.tt_e} for v in st.adapter.all_edge_versions()])
            st.close()
            c.register("nv_df", nv)
            c.register("ev_df", ev)
            c.execute("INSERT INTO nv SELECT uid, id, cp, gender, age, vt_s, vt_e, tt_s, tt_e FROM nv_df ORDER BY id, tt_s")
            c.execute("INSERT INTO ev SELECT src, dst, disc, vt_s, vt_e, tt_s, tt_e FROM ev_df ORDER BY src, tt_s")
            for s in SQL_INDEX:
                c.execute(s)
            c.execute("CHECKPOINT")
        else:
            idx_made = False
            for tt, ops in read_batches(work / GRAN[gran][0]):
                if is_base(tt):
                    nodes, edges = base_rows(tt, ops)
                    if nodes:
                        df = pd.DataFrame(nodes)
                        c.register("b_df", df)
                        c.execute(f"INSERT INTO nv SELECT uid, id, cp, g, age, a, b, tt, {OPEN} FROM b_df")
                        c.unregister("b_df")
                    if edges:
                        df = pd.DataFrame(edges)
                        c.register("b_df", df)
                        c.execute(f"INSERT INTO ev SELECT s, d, disc, a, b, tt, {OPEN} FROM b_df")
                        c.unregister("b_df")
                    continue
                if not idx_made:
                    for s in SQL_INDEX:
                        c.execute(s)
                    idx_made = True
                if tb is None:
                    tb = time.perf_counter()
                tq = time.perf_counter()
                c.execute("BEGIN TRANSACTION")
                for o in ops:
                    if o["op"] in ("assert_node", "correct"):
                        kw = node_kw(o, tt)
                        for s in SQL_NODE:
                            c.execute(s, {k: kw[k] for k in pnames(s)})
                            n_stmt += 1
                    elif o["op"] == "assert_edge":
                        c.execute(SQL_EDGE, {"s": o["src"], "d": o["dst"], "disc": o["disc"], "a": o["vt_s"],
                                             "b": o["vt_e"], "tt": tt, "open": OPEN})
                        n_stmt += 1
                    elif o["op"] == "retract":
                        r = o["ref"]
                        kw = {"s": r["src"], "d": r["dst"], "disc": r["disc"], "t": o["t"], "tt": tt, "open": OPEN}
                        for s in SQL_RETRACT:
                            c.execute(s, kw)
                            n_stmt += 1
                c.execute("COMMIT")
                lat.append(time.perf_counter() - tq)
                n_tx += 1
        t2 = time.perf_counter()
        got = {"nv": c.execute("SELECT count(*) FROM nv").fetchone()[0],
               "ev": c.execute("SELECT count(*) FROM ev").fetchone()[0]}
        c.execute("CHECKPOINT")
        return {"load_s": round(t2 - t0, 3), "gran": gran, "mode": mode,
                "phases": {"base_s": round((tb or t2) - t0, 3), "history_s": round(t2 - (tb or t2), 3)},
                "history_transactions": n_tx, "history_statements": n_stmt, "counts": got, "tx_ms": _pct(lat),
                "db_bytes": self.db.stat().st_size if self.db.exists() else None}

    @staticmethod
    def expressible(qid, form, density):
        return True

    def text(self, qid, form, p=None):
        point = qid in ("Q1", "Q3")
        if qid in ("Q1", "Q2"):
            return (f"SELECT v.id, v.cp, v.gender, v.age, v.vt_s, v.tt_s FROM nv v WHERE v.id = $id AND "
                    f"{sql_pred('v', form, point)}")
        return (f"WITH a AS (SELECT DISTINCT uid FROM nv WHERE id = $id), "
                f"n AS (SELECT v.* FROM nv v WHERE v.id = $id AND {sql_pred('v', form, point)}), "
                f"f AS (SELECT e.* FROM ev e JOIN a ON e.src = a.uid WHERE {sql_pred('e', form, point)}), "
                f"r AS (SELECT f.dst, f.disc, f.vt_s AS fvs, v.* FROM f JOIN nv v ON v.uid = f.dst "
                f"WHERE {sql_pred('v', form, point)}) "
                f"SELECT 'n' AS k, NULL AS fkey, n.id, n.cp, n.gender, n.age, n.vt_s, n.tt_s FROM n "
                f"UNION ALL SELECT 'r', r.dst || '|' || r.disc || '|' || r.fvs, r.id, r.cp, r.gender, r.age, "
                f"r.vt_s, r.tt_s FROM r")

    def fn(self, qid, form, p):
        q = self.text(qid, form)
        allv = {"id": p["id"], "t": p["t"], "t2": p.get("t2", 0), "open": OPEN}
        args = {k: allv[k] for k in pnames(q)}
        con = self.con

        def f():
            return con.execute(q, args).fetchall()
        return f

    @staticmethod
    def project(qid, form, rows):
        def st(r):
            return {"id": r[0], "completion_percentage": r[1], "gender": r[2], "age": r[3]}

        def key(vt_s, tt_s):
            return vt_s if form == "VT" else max(vt_s, tt_s)
        if qid in ("Q1", "Q2"):
            return {"n": proj_nodes_seq([(key(r[4], r[5]), st(r)) for r in rows])}
        ns = [(key(r[6], r[7]), st(r[2:])) for r in rows if r[0] == "n"]
        per = defaultdict(list)
        for r in rows:
            if r[0] == "r":
                per[r[1]].append((key(r[6], r[7]), st(r[2:])))
        return {"n": proj_nodes_seq(ns), "out": sorted((proj_nodes_seq(v) for v in per.values()), key=cj)}

    def close(self):
        self.con.close()


# --------------------------------------------------------------------------- #
# answer check                                                                 #
# --------------------------------------------------------------------------- #

def answers(S, params, oracle, density, repeat=1) -> dict:
    """P6's answer check, generic over the systems above; `repeat` > 1 calls each
    instance again and records whether the answer is stable (P15-style)."""
    out = {}
    for qid in QIDS:
        for form in FORMS:
            cell = f"{qid}-{form}"
            ex = S.expressible(qid, form, density)
            res = []
            for n, p in enumerate(params[qid]):
                try:
                    hs = []
                    for _ in range(repeat):
                        t = time.perf_counter()
                        raw = S.fn(qid, form, p)()
                        ms = (time.perf_counter() - t) * 1e3
                        ans = S.project(qid, form, raw)
                        hs.append(h(ans))
                    o = oracle[f"{cell}#{n}"]
                    ok = all(x == o["hash"] for x in hs)
                    extra: dict = {}
                    if len(set(hs)) > 1:
                        extra["unstable"] = hs
                    if not ok:
                        extra |= {"answer": ans, "oracle_answer": o["answer"],
                                  "why": P6._safe_classify(qid, form, {**p, "_n": n}, ans, oracle)}
                        try:
                            extra["query_text"] = S.text(qid, form, p)
                        except Exception:              # noqa: BLE001
                            pass
                    res.append({"n": n, "class": p["class"], "id": p["id"], "hash": hs[-1], "oracle": o["hash"],
                                "agree": ok, "ms": round(ms, 3), **extra})
                except Exception as e:                  # noqa: BLE001
                    res.append({"n": n, "class": p["class"], "id": p["id"], "agree": False,
                                "error": f"{type(e).__name__}: {e}"[:400]})
            n_ok = sum(1 for r in res if r.get("agree"))
            whys: dict = defaultdict(int)
            for r_ in res:
                if not r_.get("agree"):
                    whys[r_.get("why") or r_.get("error", "error")[:80]] += 1
            out[cell] = {"expressible": ex, "agree": n_ok, "of": len(res), "why_counts": dict(whys),
                         "verdict": ("not expressible" if not ex else
                                     "agrees" if n_ok == len(res) else f"differs {len(res) - n_ok}/{len(res)}"),
                         "instances": res}
    return out


def make_system(a, work: Path):
    s = a.system
    if s == "gnomon":
        return GnomonMatch(Path(a.store))
    if s == "gnomon-eh":
        return GnomonEH(Path(a.store))
    if s.startswith("aeong-native"):
        return AeonGNativeW6(work, a.uri)
    if s in ("aeong-property", "neo4j"):
        return CypherProperty(s, a.uri)
    if s == "duckdbsql":
        return DuckSQL(Path(a.db), a.threads, a.memory_limit)
    raise SystemExit(f"unknown system {s}")


def cmd_load(a) -> int:
    work = Path(a.work)
    # budget control for running jobs: a flag file skips one (system, granularity) load
    flag = Path(os.environ.get("W6", "/nonexistent")) / f"skip-load-{work.name}-{a.system}-{a.gran}"
    if flag.exists():
        Path(a.out).write_text(json.dumps({"system": a.system, "gran": a.gran, "skipped": flag.read_text().strip(),
                                           "utc": utc()}) + "\n")
        print(f"skipped by {flag}: {flag.read_text().strip()}")
        return 4
    S = make_system(a, work)
    rec: dict[str, Any] = {"system": a.system, "work": str(work), "gran": a.gran, "mode": a.mode,
                           "host": os.uname().nodename, "job": os.environ.get("SLURM_JOB_ID"), "start": utc(),
                           "server_flags": os.environ.get("W6_SERVER_FLAGS"),
                           "durability": os.environ.get("W6_DURABILITY")}
    if a.system.startswith("aeong-native"):
        rec["load"] = S.load_w6(Path(a.index), a.gran)
    else:
        rec["load"] = S.load(work, a.gran, a.mode, Path(a.store) if a.store else None)
    rec["settings"] = S.settings() if hasattr(S, "settings") else {}
    rec["end"] = utc()
    Path(a.out).write_text(json.dumps(rec, indent=1, default=str))
    print(json.dumps({k: v for k, v in rec.items()}, default=str)[:3000])
    S.close()
    return 0


def cmd_check(a) -> int:
    work = Path(a.work)
    if a.gran != "op" and not a.derived:
        raise SystemExit("a batch/day store is checked against oracle-<gran>.json: pass --derived")
    params, oracle = params_for(work, a.gran, Path(a.derived) if a.derived else None)
    meta = json.loads((work / "build-meta.json").read_text())
    S = make_system(a, work)
    rec: dict[str, Any] = {"system": a.system, "tag": meta["tag"], "gran": a.gran, "start": utc(),
                           "host": os.uname().nodename, "job": os.environ.get("SLURM_JOB_ID"),
                           "params_sha256": sha_file(work / ("params-day.json" if a.gran == "day" else "params.json")),
                           "engine_so_sha256": os.environ.get("W6_ENGINE_SO_SHA")}
    if a.system.startswith("aeong-native"):
        S.ensure_marks()
        needed = {p["marker"] for v in params.values() for p in v} | \
                 {p["marker2"] for v in params.values() for p in v if "marker2" in p}
        rec["marker_map"] = S.map_markers(needed)
        if a.gc_wait:
            time.sleep(a.gc_wait)
            rec["gc_wait_s"] = a.gc_wait
    rec["settings"] = S.settings() if hasattr(S, "settings") else {}
    rec["answers"] = answers(S, params, oracle, meta["density_pct"], a.repeat)
    if isinstance(S, GnomonMatch):
        rec["routes"] = dict(S.routes)
    rec["summary"] = {c: d["verdict"] for c, d in rec["answers"].items()}
    rec["end"] = utc()
    Path(a.out).write_text(json.dumps(rec, indent=1, default=str))
    print(json.dumps({"system": a.system, "tag": meta["tag"], "gran": a.gran, "summary": rec["summary"],
                      "routes": rec.get("routes")}))
    S.close()
    return 0


def cmd_oracle(a) -> int:
    """The transaction-time answers of a store committed per batch or per day.

    A coarser commit records fewer states as current: within one transaction only the
    last write of an entity is ever believed, at the transaction's tt. The valid-time
    answers do not depend on the granularity, so they are copied from the frozen oracle;
    the TT answers are recomputed from the per-operation log with every tt moved to its
    transaction's commit tt (T0 + 4 k_last + 1, as the build writes the grouped logs).
    Self-check first: with groups of one operation the recomputed TT answers must equal
    the frozen `oracle.json` on every TT instance, or nothing is written."""
    import bisect
    work, out = Path(a.work), Path(a.out)
    meta = json.loads((work / "build-meta.json").read_text())
    n_ops, M, D = meta["operations"], meta["marker_every"], meta.get("day_every", 0)
    T0 = P6.T0
    tts: dict[str, list] = defaultdict(list)            # uid -> [(tt, state)] TT changes, log order
    out_e: dict[str, list] = defaultdict(list)          # src -> [[dst, c, d]]
    eidx: dict[tuple, list] = {}
    for tt, ops in read_batches(work / "eventlog.jsonl"):
        for o in ops:
            if o["op"] == "assert_node":
                tts[o["uid"]].append((tt, state_of(o["props"])))
            elif o["op"] == "correct" and o["vt_e"] == OPEN:
                tts[o["ref"]["uid"]].append((tt, state_of(o["props"])))
            elif o["op"] == "assert_edge":
                e = [o["dst"], tt, OPEN]
                out_e[o["src"]].append(e)
                eidx[(o["src"], o["dst"], o["disc"])] = e
            elif o["op"] == "retract":
                r = o["ref"]
                eidx[(r["src"], r["dst"], r["disc"])][2] = tt

    def mk(G):
        def gt(tt):
            if tt < T0 or G == 1 or tt >= OPEN:
                return tt
            k = (tt - T0) // 4
            return T0 + 4 * min((k // G + 1) * G - 1, n_ops - 1) + 1
        H = {}
        for u, ch in tts.items():
            g: list = []
            for t, st in ch:
                t = gt(t)
                if g and g[-1][0] == t:
                    g[-1] = (t, st)
                else:
                    g.append((t, st))
            H[u] = g
        E = {u: [[d, gt(c), gt(x)] for d, c, x in es] for u, es in out_e.items()}

        def at(u, t):
            ch = H.get(u, [])
            i = bisect.bisect_right([c[0] for c in ch], t) - 1
            return ch[i][1] if i >= 0 else None

        def seq(u, t1, t2):
            ch = H.get(u, [])
            f = at(u, t1)
            return P6.collapse(([f] if f is not None else []) + [st for (t, st) in ch if t1 < t < t2])

        def answers_tt(params):
            orc = {}
            for qid, inst in params.items():
                for n, p in enumerate(inst):
                    u, t, t2 = p["uid"], p["t"], p.get("t2")
                    if qid == "Q1":
                        s_ = at(u, t)
                        ans = {"n": [s_] if s_ else []}
                    elif qid == "Q2":
                        ans = {"n": seq(u, t, t2)}
                    elif qid == "Q3":
                        s_ = at(u, t)
                        ans = {"n": [s_] if s_ else [],
                               "out": sorted(([at(d, t)] for d, c, x in E.get(u, []) if c <= t < x), key=cj)}
                    else:
                        ans = {"n": seq(u, t, t2),
                               "out": sorted((seq(d, t, t2) for d, c, x in E.get(u, []) if c < t2 and x > t), key=cj)}
                    orc[f"{qid}-TT#{n}"] = {"hash": h(ans), "answer": ans}
            return orc
        return answers_tt

    params, frozen = params_for(work, "op")
    chk = mk(1)(params)
    bad = [k for k, v in chk.items() if v["hash"] != frozen[k]["hash"]]
    rec = {"work": str(work), "self_check_op": {"tt_instances": len(chk), "differ": len(bad), "examples": bad[:5]}}
    if bad:
        print(json.dumps(rec))
        raise SystemExit("self-check failed: the recomputed per-operation TT answers differ from oracle.json")
    out.mkdir(parents=True, exist_ok=True)
    for gran, G, pf in (("batch", M, "op"), ("day", D, "day")):
        if not G:
            continue
        prm, base = params_for(work, pf)
        o = {k: v for k, v in base.items() if "-VT#" in k or k == "_hist"}
        o |= mk(G)(prm)
        f = out / f"oracle-{gran}.json"
        f.write_text(json.dumps(o))
        changed = sum(1 for k, v in o.items() if "-TT#" in k and v["hash"] != base[k]["hash"])
        rec[gran] = {"group_ops": G, "file": str(f), "sha256": sha_file(f), "tt_instances_changed_vs_per_operation": changed}
    (out / "oracle-gran.json").write_text(json.dumps(rec, indent=1))
    print(json.dumps(rec))
    return 0


def cmd_ceiling(a) -> int:
    heavy = {"aeong": "MATCH (a:PU), (b:PU), (c:PU) RETURN count(*) AS n",
             "neo4j": "MATCH (a:PU), (b:PU), (c:PU) RETURN count(*) AS n"}[a.kind]
    rec = ceiling_check(a.uri, a.ceiling, heavy)
    print(json.dumps(rec))
    if a.out:
        Path(a.out).write_text(json.dumps(rec) + "\n")
    return 0 if rec["enforced"] else 3


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sp = ap.add_subparsers(dest="cmd", required=True)
    g = sp.add_parser("gnomon-load")
    g.add_argument("--repo", required=True)
    g.add_argument("--work", required=True)
    g.add_argument("--gran", choices=list(GRAN), default="op")
    g.add_argument("--path", choices=["replay", "live"], default="replay")
    g.add_argument("--store", required=True)
    g.add_argument("--out", required=True)
    g.add_argument("--digest", action="store_true")
    for name in ("load", "check"):
        c = sp.add_parser(name)
        c.add_argument("--system", required=True,
                       choices=["gnomon", "gnomon-eh", "aeong-native", "aeong-native-mem", "aeong-property",
                                "neo4j", "duckdbsql"])
        c.add_argument("--work", required=True)
        c.add_argument("--gran", choices=list(GRAN), default="op")
        c.add_argument("--mode", choices=["maint", "bulk"], default="maint")
        c.add_argument("--store")
        c.add_argument("--uri", default="bolt://127.0.0.1:17689")
        c.add_argument("--db")
        c.add_argument("--index")
        c.add_argument("--threads", type=int, default=16)
        c.add_argument("--memory-limit", default="24GB")
        c.add_argument("--out", required=True)
        c.add_argument("--gc-wait", type=int, default=0)
        c.add_argument("--repeat", type=int, default=1)
        c.add_argument("--derived", help="dir with oracle-batch.json / oracle-day.json (cmd oracle)")
    o = sp.add_parser("oracle")
    o.add_argument("--work", required=True)
    o.add_argument("--out", required=True)
    k = sp.add_parser("ceiling")
    k.add_argument("--uri", required=True)
    k.add_argument("--kind", choices=["aeong", "neo4j"], default="aeong")
    k.add_argument("--ceiling", type=float, default=10.0)
    k.add_argument("--out")
    a = ap.parse_args()
    return {"gnomon-load": cmd_gnomon_load, "load": cmd_load, "check": cmd_check, "ceiling": cmd_ceiling,
            "oracle": cmd_oracle}[a.cmd](a)


if __name__ == "__main__":
    raise SystemExit(main())
