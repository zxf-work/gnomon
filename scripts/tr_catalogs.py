"""Query catalogs of the shared timed runner.

check driver makes, so the timed runner times exactly what the answer checks verified:

    cells(a)  -> [(query, setting, instance id, payload)]          (in the parent; no system opened)
    open_(a)  -> Bound                                              (in the worker; opens the system)

`Bound.make(query, setting, payload) -> (fn, hasher)`: fn() is the timed call, hasher(result) ->
(answer hash, rows) runs after the timer. `Bound.probe` is the engine-time probe (or NULL),
`Bound.settings` what the record states about the system (designs(), threads, version).

W3  w3_snb_run.make_system (gnomon | duckdb | postgres | neo4j | memgraph | aeong | aeong-prop | ladybug), run(qid, inst, setting)
W4  w4_check.executor (gnomon | duckdbsql | postgres | neo4j | memgraph | ladybug), ex(query, params)
W6  w6_tmgbench.make_system (gnomon | gnomon-eh | aeong-native[-mem] | aeong-property | neo4j | duckdbsql), fn(qid, form, p)
    tgmark (gnomon: gnomon match on the T-gMark store) and w6_tgmark (aeong-native | aeong-property)
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

import engine_time as ET


class Bound:
    def __init__(self, make, probe, settings: dict[str, Any], keep: Any = None) -> None:
        self.make, self.probe, self.settings, self.keep = make, probe, settings, keep


def _gnomon_settings(adapter) -> dict[str, Any]:
    out: dict[str, Any] = {"gnomon_env": {k: v for k, v in sorted(os.environ.items()) if k.startswith("GNOMON_")}}
    try:
        out["designs"] = adapter.designs()
    except Exception as e:                                       # noqa: BLE001
        out["designs"] = f"unavailable: {type(e).__name__}: {e}"[:200]
    out["threads"] = {"GNOMON_MATCH_THREADS": os.environ.get("GNOMON_MATCH_THREADS")}
    return out


def _duck_settings(con) -> dict[str, Any]:
    try:
        import duckdb
        r = dict(con.execute("SELECT name, value FROM duckdb_settings() WHERE name IN "
                             "('threads', 'memory_limit', 'enable_fsync_on_commit')").fetchall())
        return {"duckdb": duckdb.__version__, "threads": {"duckdb_threads": r.get("threads")}, "settings": r}
    except Exception as e:                                       # noqa: BLE001
        return {"duckdb_settings": f"unavailable: {e}"[:200]}


def _probe_for(system: str):
    s = system.split("-")[0]
    return ET.probe("native" if s in ("gnomon", "native") else s)


# ---------------------------------------------------------------- W3 ------- #
def w3_cells(a):
    want: dict[str, list[str] | None] = {}
    if a.cells:
        for c in a.cells.split(","):
            q, _, s = c.partition("@")
            want[q] = s.split("+") if s else None
    settings = a.settings.split(",")
    insts = None if not a.instances else {int(x) for x in a.instances.split(",")}
    out = []
    rows = [json.loads(line) for line in open(a.params) if line.strip()]
    order: dict[str, list] = {}
    for r in rows:
        if want and r["qid"] not in want:
            continue
        if insts is not None and r["i"] not in insts:
            continue
        order.setdefault(r["qid"], []).append(r)
    for q, rs in order.items():
        for s in (want.get(q) if want and want.get(q) else settings):
            for r in rs:
                out.append((q, s, r["i"], r))
    return out


def w3_open(a) -> Bound:
    # the W3 server-side caps (DuckDB interrupt, PostgreSQL statement_timeout, LadybugDB query timeout) read W3_CAP_S
    # at import: in the timed runner they are the ceiling, so every cap sits TR_GRACE_S below the parent's kill
    os.environ.setdefault("W3_CAP_S", str(int(float(os.environ.get("TR_CEILING_S", "10")))))
    import w3_snb_run as R
    S = R.make_system(a.system, a.target)
    pr = _probe_for(a.system)
    settings: dict[str, Any] = {"system_class": type(S).__name__, "server_side_limit_s": R.CAP_S}
    if a.system == "gnomon":
        ad = getattr(S, "ad", None) or getattr(S, "adapter", None)
        if ad is not None:
            settings.update(_gnomon_settings(ad))
    if a.system == "duckdb":
        settings.update(_duck_settings(S.con))
        if ET.ENABLED:
            pr = ET.DuckProbe()
            S.con = pr.wrap(S.con)
    if a.system == "postgres" and ET.ENABLED:
        pr = ET.PgProbe(a.target)
    if a.system == "ladybug":
        settings.update(S.settings())

    def make(q, s, inst):
        def fn():
            return S.run(q, inst, s)

        def hasher(res):
            cols, rows = res
            rows = R.canon_rows(rows)
            if q in R.COMPARE_LAST:
                rows = [r[-1:] for r in rows]
            return R.hrows(rows), len(rows)
        return fn, hasher
    return Bound(make, pr, settings, S)


# ---------------------------------------------------------------- W4 ------- #
def w4_cells(a):
    doc = json.loads(Path(a.params).read_text())
    want = set(a.queries.split(",")) if a.queries else None
    bel = set(a.beliefs.split(","))
    n = int(a.n_instances) if a.n_instances else None
    seen: dict = {}
    out = []
    for i in doc["instances"]:
        if (want and i["query"] not in want) or i["belief"] not in bel:
            continue
        k = (i["query"], i["belief"])
        seen[k] = seen.get(k, 0) + 1
        if n is None or seen[k] <= n:
            out.append((i["query"], i["belief"], i["instance"], i))
    return out


def w4_open(a) -> Bound:
    import w4_check as C
    import w4_queries as Q
    state: dict = {}
    pr = _probe_for(a.system)
    wrap = None
    if ET.ENABLED and a.system == "duckdbsql":
        pr = ET.DuckProbe()
        wrap = pr.wrap
    ceil = int(float(os.environ.get("TR_CEILING_S", "10")))
    ex = C.executor(a.system, Path(a.store) if a.store else None, Path(a.db) if a.db else None, ceil, state,
                    wrap_con=wrap)
    settings: dict[str, Any] = {"server_side_limit_s": ceil}
    if a.system == "duckdbsql":
        settings["threads"] = {"W4_DUCKDB_THREADS": os.environ.get("W4_DUCKDB_THREADS")}
    if a.system == "ladybug" and "ladybug_db" in state:
        import w3_ladybug as L3
        settings.update(L3.db_settings(state["ladybug_db"]), threads={"W4_LADYBUG_THREADS": state.get("threads")},
                        server_side_limit_s=f"{ceil} (LadybugDB query timeout)")
    if a.system == "gnomon" and "store" in state:
        settings.update(_gnomon_settings(state["store"].adapter))

    def make(q, _b, inst):
        p = inst["params"]

        def fn():
            return ex(q, p)

        def hasher(res):
            rows = Q.canonical(q, res)
            return C.h(rows), len(rows)
        return fn, hasher
    return Bound(make, pr, settings, state)


# ---------------------------------------------------------------- W6 ------- #
def w6tmg_cells(a):
    import w6_tmgbench as T
    params, _ = T.params_for(Path(a.work), a.gran, Path(a.derived) if a.derived else None)
    cells = a.cells.split(",") if a.cells else [f"{q}-{f}" for q in T.QIDS for f in T.FORMS]
    out = []
    for c in cells:
        qid, form = c.split("-")
        for n, p in enumerate(params[qid]):
            out.append((qid, form, n, p))
    return out


def w6tmg_open(a) -> Bound:
    import w6_tmgbench as T
    ns = argparse.Namespace(system=a.system, store=a.store, uri=a.uri, db=a.db, threads=a.threads,
                            memory_limit=a.memory_limit)
    work = Path(a.work)
    S = T.make_system(ns, work)
    meta = json.loads((work / "build-meta.json").read_text())
    density = meta["density_pct"]
    if a.system.startswith("aeong-native"):
        S.ensure_marks()
        ps, _ = T.params_for(work, a.gran, Path(a.derived) if a.derived else None)
        needed = {p["marker"] for v in ps.values() for p in v} | \
                 {p["marker2"] for v in ps.values() for p in v if "marker2" in p}
        S.map_markers(needed)
    pr = _probe_for("aeong" if a.system.startswith("aeong") else a.system)
    settings: dict[str, Any] = {"system_settings": S.settings() if hasattr(S, "settings") else {},
                                "density_pct": density, "gran": a.gran}
    if a.system.startswith("gnomon"):
        st = getattr(S, "store", None) or getattr(S, "st", None)
        ad = getattr(st, "adapter", None) if st is not None else getattr(S, "adapter", None)
        if ad is not None:
            settings.update(_gnomon_settings(ad))
    if a.system == "duckdbsql":
        settings.update(_duck_settings(S.con))
        if ET.ENABLED:
            pr = ET.DuckProbe()
            S.con = pr.wrap(S.con)

    def make(qid, form, p):
        if not S.expressible(qid, form, density):
            raise NotImplementedError(f"{qid}-{form} not expressible on {a.system}")
        f = S.fn(qid, form, p)

        def hasher(raw):
            ans = S.project(qid, form, raw)
            return T.h(ans), (len(ans) if hasattr(ans, "__len__") else None)
        return f, hasher
    return Bound(make, pr, settings, S)


def w6gm_cells(a):
    import tgmark as TG
    qs = {q["id"]: q for q in TG.parse_workload(Path(a.workload))}
    want = [int(x) for x in a.queries.split(",")] if a.queries else sorted(qs)
    out = []
    for qid in want:
        for at in a.asof.split(","):
            out.append((f"q{qid}", at, 0, qs[qid]))
    return out


def w6gm_open(a) -> Bound:
    import tgmark as TG
    pr = _probe_for("native" if a.system == "gnomon" else "aeong")
    if a.system == "gnomon":
        import gnomon
        import gnomon.temporal.ops_match as OM
        from gnomon.temporal.algebra import ensure_all_registered
        ensure_all_registered()
        store = gnomon.open(Path(a.store), backend="native", read_only=True)
        tts = sorted({int(t) for t in store.adapter.versions_columnar("edge")["tt_s"]})

        def make(_c, at, q):
            as_of = (tts[-2] if len(tts) > 1 else tts[-1]) if at == "load" else None
            args = TG.match_args(q, as_of)

            def fn():
                # every page: answers are compared whole
                a2 = {**args, "limit": max(int(args.get("limit") or 1), int(getattr(OM, "MAX_LIMIT", TG.PAGE)))}
                rows = []
                while True:
                    pl = OM.match(store.adapter, a2, route="native")
                    rows += pl["rows"]
                    if not (pl.get("truncated") and pl.get("cursor")):
                        return {**pl, "rows": rows, "truncated": False, "cursor": None}
                    a2["cursor"] = pl["cursor"]

            def hasher(payload_):
                rows = TG._payload_rows(q, payload_)
                return TG.digest_rows(q["head"], rows), len(rows)
            return fn, hasher
        return Bound(make, pr, _gnomon_settings(store.adapter), store)
    import w6_tgmark as WG
    form = a.system.split("-", 1)[1]
    x = int(json.loads(Path(a.load).read_text())["marks"]["0"])
    d = WG.bolt(a.uri)

    def make(_c, at, q):
        cy = WG.text(q, form, at, x, "as_written")

        def fn():
            with d.session() as s:
                return [tuple(r.values()) for r in s.run(cy)]

        def hasher(rows):
            head = q["head"]
            uniq = sorted(set(rows)) if head else ([()] if rows else [])
            return TG.digest_rows(head, uniq), len(uniq)
        return fn, hasher
    return Bound(make, pr, {"form": form}, d)


CATALOGS = {"w3": (w3_cells, w3_open), "w4": (w4_cells, w4_open),
            "w6tmg": (w6tmg_cells, w6tmg_open), "w6gm": (w6gm_cells, w6gm_open)}

PROTOCOLS = {"w3": (1, 5, 1, 3), "w4": (2, 10, 1, 3), "w6tmg": (2, 10, 1, 3), "w6gm": (2, 10, 1, 3)}
#: the engine-time pass: 1 warm-up + 3 timed for every workload
ENGINE_PASS = (1, 3, 1, 3)
