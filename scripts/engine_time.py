"""Engine time per call, the same definition for every system.

**Engine time** is the time inside the system from receiving the query until
the result is ready: parsing, validation and admission, compilation and
planning, and execution. It excludes the client driver, network transfer, and
the conversion of the result into Python rows or payloads. Per timed call the
kit records

    e2e_ms       the existing client-side timer around fn() (unchanged)
    engine_ms    the system's own time for the call (summed over the statements
                 the call issues), with front_ms (parse/validate/plan) and
                 exec_ms where the system reports the split
    delivery_ms  e2e_ms - engine_ms

Off unless EXTCMP_ENGINE_TIME=1: with it unset no hook is installed, `probe()`
returns the null probe, and every timed path is the one the campaign ran.

Use, in a timing loop (begin/end sit OUTSIDE the e2e timer):

    pr = engine_time.probe("postgres", handle)      # once per system/handle
    pr.begin(); t = perf_counter(); r = fn(); e2e = (perf_counter() - t) * 1e3
    rec = pr.end(e2e)                                 # dict or None

Sources:
  gnomon (native)  kit-level wrappers on stable engine entry points (no engine
                   change): front = argument validation (call_operator's and
                   match's own), admission (stats, cost estimate, cost check),
                   tt_q/freshness metadata, the plan leaf, match cardinalities
                   and compile; exec = every call into the native engine
                   (NativeAdapter._read_op and the module-level kernels of
                   gnomon._engine). The Rust call returns Python objects built by
                   pyo3 inside the call, so that construction is inside exec;
                   the kernels' Python shaping of those objects into the
                   payload, float canonicalisation, the digest and the envelope
                   are delivery. kernel_ms (the whole kernel incl. its Python)
                   gives engine_ub_ms = front + kernel, an upper bound.
  duckdbsql        DuckDB's profiler (latency, planner metrics) per statement,
                   read right after each execute() through a connection proxy.
  postgres         pg_stat_statements (total_plan_time + total_exec_time) as
                   deltas around the call, from a separate monitor session;
                   needs the server started with EXTCMP_PG_STATS=1.
  neo4j            Bolt summary: result_available_after + result_consumed_after
                   (t_first + t_last), integer ms.
  memgraph, aeong  Bolt summary: parsing_time + planning_time +
                   plan_execution_time (seconds, float).
  clickhouse       the server-reported elapsed (X-ClickHouse-Summary
                   elapsed_ns) of every query of the call.
  ladybug          LadybugDB's own QueryResult timers per statement:
                   get_compiling_time (front) + get_execution_time (exec).
                   The Python client prepares a parameterised statement once
                   and reuses it (its implicit prepared-statement cache); a
                   reused statement reports its first compile time again, so
                   front counts only when the statement was prepared in this
                   call (the cache grew) or has no parameters (compiled every
                   time). Rows are materialised by execute(); fetching them
                   into Python is delivery.
"""
from __future__ import annotations

import json
import os
import time
from typing import Any

ENABLED = os.environ.get("EXTCMP_ENGINE_TIME") == "1"
_pc = time.perf_counter_ns


class Probe:
    source = "none"

    def begin(self) -> None:
        pass

    def end(self, e2e_ms: float | None = None) -> dict[str, Any] | None:
        return None


NULL = Probe()


def _rec(source: str, e2e_ms: float | None, engine_ms: float | None,
         front_ms: float | None = None, exec_ms: float | None = None, **extra: Any) -> dict[str, Any]:
    r: dict[str, Any] = {"src": source, "engine_ms": None if engine_ms is None else round(engine_ms, 4),
                         "front_ms": None if front_ms is None else round(front_ms, 4),
                         "exec_ms": None if exec_ms is None else round(exec_ms, 4)}
    if e2e_ms is not None and engine_ms is not None:
        r["delivery_ms"] = round(e2e_ms - engine_ms, 4)
    r.update({k: (round(v, 4) if isinstance(v, float) else v) for k, v in extra.items()})
    return r


# --------------------------------------------------------------------------- #
# Gnomon: wrappers on the engine's stable entry points (module attributes and
# class attributes are looked up at call time, so a wrapper installed here is
# what call_operator and the kernels call; nothing under gnomon/ changes)
# --------------------------------------------------------------------------- #
class _G:
    front_ns = 0
    native_ns = 0
    kernel_ns = 0
    front_in_kernel_ns = 0
    native_calls = 0
    front_depth = 0
    native_depth = 0
    kernel_depth = 0
    installed = False
    hooks: list[str] = []


def _wrap_front(fn):
    def w(*a, **k):
        if _G.front_depth or _G.native_depth:
            return fn(*a, **k)
        _G.front_depth += 1
        t = _pc()
        try:
            return fn(*a, **k)
        finally:
            d = _pc() - t
            _G.front_ns += d
            if _G.kernel_depth:
                _G.front_in_kernel_ns += d
            _G.front_depth -= 1
    w.__wrapped__ = fn
    w.__name__ = getattr(fn, "__name__", "front")
    return w


def _wrap_native(fn):
    def w(*a, **k):
        if _G.native_depth or _G.front_depth:
            return fn(*a, **k)
        _G.native_depth += 1
        t = _pc()
        try:
            return fn(*a, **k)
        finally:
            _G.native_ns += _pc() - t
            _G.native_calls += 1
            _G.native_depth -= 1
    w.__wrapped__ = fn
    w.__name__ = getattr(fn, "__name__", "native")
    return w


def _wrap_kernel(fn):
    def w(*a, **k):
        if _G.kernel_depth:
            return fn(*a, **k)
        _G.kernel_depth += 1
        t = _pc()
        try:
            return fn(*a, **k)
        finally:
            _G.kernel_ns += _pc() - t
            _G.kernel_depth -= 1
    w.__wrapped__ = fn
    w.__name__ = getattr(fn, "__name__", "kernel")
    return w


def _patch(mod: Any, name: str, wrapper) -> None:
    f = getattr(mod, name, None)
    if f is None or getattr(f, "__wrapped__", None) is not None:
        return
    setattr(mod, name, wrapper(f))
    _G.hooks.append(f"{getattr(mod, '__name__', mod)}.{name}")


def install_gnomon() -> list[str]:
    if _G.installed:
        return _G.hooks
    import gnomon._engine as E
    import gnomon.temporal.algebra as A
    import gnomon.temporal.guardrails as GR
    import gnomon.temporal.ops_match as M
    import gnomon.plan.evaluate as EV
    import gnomon.plan.leaf as LF
    import gnomon.plan.ttq as TQ
    from gnomon.storage.native.adapter import NativeAdapter
    # front: validation, admission, tt_q metadata, the plan leaf, match planning
    _patch(A, "_validate_args_keyed", _wrap_front)
    _patch(GR, "add_time_estimate", _wrap_front)
    _patch(GR, "enforce_cost", _wrap_front)
    _patch(TQ, "envelope_metadata", _wrap_front)
    _patch(LF, "build_leaf", _wrap_front)
    _patch(M, "validate", _wrap_front)
    _patch(M, "cardinalities", _wrap_front)
    _patch(M, "compile_match", _wrap_front)
    _patch(NativeAdapter, "stats", _wrap_front)         # call_operator's admission input
    # exec: every call into the native engine
    _patch(NativeAdapter, "_read_op", _wrap_native)
    for n in ("motif_match", "interval_pairs", "interval_page", "win_search"):
        _patch(E, n, _wrap_native)
    # the kernel as a whole (upper bound)
    _patch(EV, "evaluate_leaf", _wrap_kernel)
    _G.installed = True
    return _G.hooks


class GnomonProbe(Probe):
    source = "gnomon: kit wrappers (front = validate+admission+tt_q+leaf+match plan; exec = native engine calls)"

    def __init__(self) -> None:
        install_gnomon()

    def begin(self) -> None:
        _G.front_ns = _G.native_ns = _G.kernel_ns = _G.front_in_kernel_ns = _G.native_calls = 0

    def end(self, e2e_ms=None):
        f, n, k = _G.front_ns / 1e6, _G.native_ns / 1e6, _G.kernel_ns / 1e6
        fk = _G.front_in_kernel_ns / 1e6
        # upper bound: the front outside the kernel + the whole kernel (its Python included);
        # a call that runs no kernel (an adapter method called directly) bounds at front + native
        ub = (f - fk) + k if k > 0 else f + n
        return _rec("gnomon", e2e_ms, f + n, f, n, kernel_ms=k, engine_ub_ms=max(ub, f + n),
                    native_calls=_G.native_calls)


# --------------------------------------------------------------------------- #
# Bolt (Neo4j, Memgraph, AeonG): the summary metadata of every result that
# finished between begin() and end(), captured where the driver finishes a
# stream (Result._handle_warnings runs once per result after the final
# SUCCESS, for PULL and DISCARD alike)
# --------------------------------------------------------------------------- #
class _B:
    got: list[dict] = []
    installed = False


def install_bolt() -> None:
    if _B.installed:
        return
    from neo4j._sync.work import result as R
    orig = R.Result._handle_warnings

    def hw(self, *a, **k):
        md = self._metadata
        _B.got.append({k2: md.get(k2) for k2 in ("t_first", "t_last", "parsing_time", "planning_time",
                                                 "plan_execution_time", "cost_estimate", "type")
                       if k2 in md})
        return orig(self, *a, **k)
    hw.__wrapped__ = orig
    R.Result._handle_warnings = hw
    _B.installed = True


class BoltProbe(Probe):
    def __init__(self, flavour: str) -> None:
        self.flavour = flavour        # "neo4j" | "memgraph"
        self.source = ("neo4j: summary result_available_after + result_consumed_after"
                       if flavour == "neo4j" else
                       f"{flavour}: summary parsing_time + planning_time + plan_execution_time")
        install_bolt()

    def begin(self) -> None:
        _B.got.clear()

    def end(self, e2e_ms=None):
        got = list(_B.got)
        if not got:
            return _rec(self.flavour, e2e_ms, None, n_results=0)
        if self.flavour == "neo4j":
            av = sum(float(g.get("t_first") or 0) for g in got)
            co = sum(float(g.get("t_last") or 0) for g in got)
            return _rec(self.flavour, e2e_ms, av + co, None, None, available_ms=av, consumed_ms=co,
                        n_results=len(got), missing=sum(1 for g in got if "t_last" not in g))
        pa = sum(float(g.get("parsing_time") or 0) for g in got) * 1e3
        pl = sum(float(g.get("planning_time") or 0) for g in got) * 1e3
        ex = sum(float(g.get("plan_execution_time") or 0) for g in got) * 1e3
        return _rec(self.flavour, e2e_ms, pa + pl + ex, pa + pl, ex, parse_ms=pa, plan_ms=pl,
                    n_results=len(got), missing=sum(1 for g in got if "plan_execution_time" not in g))


# --------------------------------------------------------------------------- #
# PostgreSQL: pg_stat_statements deltas from a separate monitor session
# --------------------------------------------------------------------------- #
# every database of the server (the monitor may sit in another database than the client); the
# probe's own statements are left out by their text. One client per server in the kit's windows.
_PG_SNAP = ("SELECT coalesce(sum(calls), 0), coalesce(sum(total_plan_time), 0), "
            "coalesce(sum(total_exec_time), 0) FROM pg_stat_statements "
            "WHERE query NOT LIKE '%pg_stat_statements%'")


class PgProbe(Probe):
    source = "postgres: pg_stat_statements total_plan_time + total_exec_time (delta per call)"

    def __init__(self, dsn: str | None = None) -> None:
        import psycopg
        self.dsn = dsn or os.environ["EXTCMP_PG_DSN"]
        self.mon = psycopg.connect(self.dsn, autocommit=True)
        self.mon.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
        tp = self.mon.execute("SELECT current_setting('pg_stat_statements.track_planning', true)").fetchone()[0]
        self.track_planning = tp
        self.mon.execute("SELECT pg_stat_statements_reset()")
        self.s0 = (0, 0.0, 0.0)

    def _snap(self):
        c, p, e = self.mon.execute(_PG_SNAP).fetchone()
        return int(c), float(p), float(e)

    def begin(self) -> None:
        self.s0 = self._snap()

    def end(self, e2e_ms=None):
        c, p, e = self._snap()
        dc, dp, de = c - self.s0[0], p - self.s0[1], e - self.s0[2]
        return _rec("postgres", e2e_ms, dp + de, dp, de, n_statements=dc)


# --------------------------------------------------------------------------- #
# DuckDB (SQL): the profiler, per statement, through a connection proxy
# --------------------------------------------------------------------------- #
# the metrics read (DuckDB 1.5.4; PLANNER_BINDING / ROWS_RETURNED in this set made
_DUCK_SETTINGS = {"LATENCY": "true", "PLANNER": "true", "PHYSICAL_PLANNER": "true", "ALL_OPTIMIZERS": "true"}


class DuckProxy:
    """Stands in for a duckdb connection in a timed fn().

    DuckDB's Python client streams a result: the statement (and its profile's
    latency clock) ends only when the stream is drained, so a caller that reads
    one row with fetchone() and then post-processes in Python would have that
    Python time inside DuckDB's latency. The proxy therefore drains the result
    in execute() (fetchall: the rows the caller fetches anyway for every query
    of the kit, which reads whole results or a single row) and serves
    fetchone/fetchmany/fetchall/description from those rows. The statement is
    complete there; its profile is read at the next statement or at the probe's
    end() (outside the timer): a call of one statement reads nothing inside the
    timer, a call of k statements reads k - 1 profiles inside it. A statement
    without a result (SET ...) has nothing to drain. execute() returns the
    proxy (the connection's execute returns the connection)."""

    def __init__(self, con: Any, sink: list) -> None:
        for k, v in (("_con", con), ("_sink", sink), ("_pending", False), ("_rows", None),
                     ("_pos", 0), ("_desc", None)):
            object.__setattr__(self, k, v)

    def _take(self) -> None:
        if not self._pending:
            return
        object.__setattr__(self, "_pending", False)
        try:
            self._sink.append(json.loads(self._con.get_profiling_information(format="json")))
        except Exception as e:                                   # noqa: BLE001
            self._sink.append({"error": f"{type(e).__name__}: {e}"[:200]})

    def execute(self, *a, **k):
        self._take()
        self._con.execute(*a, **k)
        desc = self._con.description
        try:
            rows = self._con.fetchall() if desc is not None else []
        except Exception:                                        # noqa: BLE001 - no result set
            rows = []
        for k_, v in (("_rows", rows), ("_pos", 0), ("_desc", desc), ("_pending", True)):
            object.__setattr__(self, k_, v)
        return self

    @property
    def description(self):
        return self._desc

    def fetchall(self):
        r = self._rows[self._pos:] if self._rows is not None else []
        object.__setattr__(self, "_pos", len(self._rows or []))
        return r

    def fetchone(self):
        if self._rows is None or self._pos >= len(self._rows):
            return None
        r = self._rows[self._pos]
        object.__setattr__(self, "_pos", self._pos + 1)
        return r

    def fetchmany(self, size: int = 1):
        r = (self._rows or [])[self._pos:self._pos + size]
        object.__setattr__(self, "_pos", self._pos + len(r))
        return r

    def flush(self) -> None:
        self._take()

    def __getattr__(self, n):
        return getattr(self._con, n)

    def __setattr__(self, n, v):
        setattr(self._con, n, v)


def duck_enable(con: Any) -> None:
    con.execute("PRAGMA enable_profiling = 'no_output'")
    con.execute(f"PRAGMA custom_profiling_settings = '{json.dumps(_DUCK_SETTINGS)}'")


class DuckProbe(Probe):
    source = "duckdbsql: DuckDB profiler latency (planner metrics as front), per statement"

    def __init__(self) -> None:
        self.sink: list = []
        self.proxies: list = []

    def wrap(self, con: Any) -> DuckProxy:
        duck_enable(con)
        p = DuckProxy(con, self.sink)
        self.proxies.append(p)
        return p

    def begin(self) -> None:
        for p in self.proxies:
            p.flush()
        self.sink.clear()

    def end(self, e2e_ms=None):
        for p in self.proxies:
            p.flush()
        prof = [p for p in self.sink if "error" not in p]
        if not prof:
            return _rec("duckdbsql", e2e_ms, None, n_statements=len(self.sink),
                        error=(self.sink[0].get("error") if self.sink else "no statement profiled"))
        lat = sum(float(p.get("latency") or 0) for p in prof) * 1e3
        front = sum(float(p.get("planner") or 0) + float(p.get("physical_planner") or 0)
                    + float(p.get("all_optimizers") or 0) for p in prof) * 1e3
        return _rec("duckdbsql", e2e_ms, lat, front, max(0.0, lat - front), n_statements=len(prof))


class WorkerProbe(Probe):
    """An in-process system run in a worker (sysworker.py, EXTCMP_INPROC_WORKER=1): the worker
    measures engine time with the same probe as in-process and returns the record with the
    payload; the parent only hands it on. Delivery stays worker-side (e2e in the worker)."""

    def __init__(self, system: str) -> None:
        self.source = f"{system} (in a worker process): the in-process probe, measured in the worker"

    def begin(self) -> None:
        import sysworker
        sysworker._Last.rec = None

    def end(self, e2e_ms=None):
        import sysworker
        r = getattr(sysworker._Last, "rec", None)
        if r is None:
            return None
        r = dict(r)
        r["worker"] = True
        return r


# --------------------------------------------------------------------------- #
# ClickHouse: the server's elapsed of every query of the call
# --------------------------------------------------------------------------- #
class _C:
    got: list = []
    installed = False


def install_clickhouse(client: Any) -> None:
    cls = type(client)
    if getattr(cls, "_et_installed", False):
        return
    for meth in ("query", "command", "raw_query"):
        orig = getattr(cls, meth, None)
        if orig is None:
            continue

        def mk(orig):
            def w(self, *a, **k):
                r = orig(self, *a, **k)
                s = getattr(r, "summary", None)
                _C.got.append(s if isinstance(s, dict) else {"_type": type(r).__name__})
                return r
            w.__wrapped__ = orig
            return w
        setattr(cls, meth, mk(orig))
    cls._et_installed = True


class ClickHouseProbe(Probe):
    source = "clickhouse: X-ClickHouse-Summary elapsed_ns per query"

    def __init__(self, client: Any) -> None:
        install_clickhouse(client)

    def begin(self) -> None:
        _C.got.clear()

    def end(self, e2e_ms=None):
        el = [g.get("elapsed_ns") for g in _C.got]
        ok = [float(x) for x in el if x not in (None, "")]
        if not ok:
            return _rec("clickhouse", e2e_ms, None, n_queries=len(el), error="no elapsed_ns in summary")
        return _rec("clickhouse", e2e_ms, sum(ok) / 1e6, None, None, n_queries=len(el),
                    missing=len(el) - len(ok))


# --------------------------------------------------------------------------- #
# LadybugDB (in-process): the QueryResult's compiling and execution times
# --------------------------------------------------------------------------- #
class _LB:
    recs: list = []
    installed = False


def install_ladybug() -> None:
    if _LB.installed:
        return
    import ladybug.connection as LC
    cls = LC.Connection
    orig = cls.execute

    def w(self, query, parameters=None):
        cache = getattr(self, "_pybind_implicit_prepared_cache", None)
        n0 = len(cache) if cache is not None else None
        r = orig(self, query, parameters)
        try:
            res = r[-1] if isinstance(r, list) else r
            comp, ex = float(res.get_compiling_time()), float(res.get_execution_time())
            fresh = (not parameters) or n0 is None or len(cache) != n0
            _LB.recs.append((comp if fresh else 0.0, ex, fresh))
        except Exception:                                        # noqa: BLE001
            _LB.recs.append(None)
        return r
    w.__wrapped__ = orig
    cls.execute = w
    _LB.installed = True


class LadybugProbe(Probe):
    source = ("ladybug: QueryResult get_compiling_time (front, only when prepared in the call) + "
              "get_execution_time (exec), summed over the call's statements")

    def __init__(self) -> None:
        install_ladybug()

    def begin(self) -> None:
        _LB.recs.clear()

    def end(self, e2e_ms=None):
        rs = [r for r in _LB.recs if r is not None]
        if not rs:
            return _rec("ladybug", e2e_ms, None, error="no statement timing")
        f, e = sum(r[0] for r in rs), sum(r[1] for r in rs)
        return _rec("ladybug", e2e_ms, f + e, f, e, n_statements=len(rs), prepared=sum(1 for r in rs if r[2]),
                    missing=len(_LB.recs) - len(rs))


def probe(system: str, handle: Any = None) -> Probe:
    """The probe for a kit system name (NULL unless EXTCMP_ENGINE_TIME=1)."""
    if not ENABLED:
        return NULL
    s = system.lower()
    if s in ("native", "gnomon", "nativematch"):     # nativematch: Gnomon through match
        return GnomonProbe()
    if s == "neo4j":
        return BoltProbe("neo4j")
    if s in ("memgraph", "aeong"):
        return BoltProbe(s)
    if s in ("postgres", "pg"):
        return PgProbe()
    if s == "clickhouse":
        return ClickHouseProbe(handle)
    if s in ("duckdbsql", "duckdb-sql"):
        return DuckProbe()
    if s == "ladybug":
        return LadybugProbe()
    return NULL


def summarize(recs: list[dict | None]) -> dict[str, Any] | None:
    """Medians over the timed calls of a cell (engine, front, exec, delivery)."""
    rs = [r for r in recs if r and r.get("engine_ms") is not None]
    if not rs:
        return None

    def med(k):
        v = sorted(r[k] for r in rs if r.get(k) is not None)
        return None if not v else (v[len(v) // 2] if len(v) % 2 else (v[len(v) // 2 - 1] + v[len(v) // 2]) / 2)
    out = {"src": rs[0]["src"], "n": len(rs)}
    for k in ("engine_ms", "front_ms", "exec_ms", "delivery_ms", "engine_ub_ms", "kernel_ms"):
        m = med(k)
        if m is not None:
            out[k + "_p50"] = round(m, 4)
    return out


# --------------------------------------------------------------------------- #
# the timing loops' side (extcmp.measure_capped, extcmp_p1.measure, and the
# workload runners): one current probe per process, one record per call
# --------------------------------------------------------------------------- #
CURRENT: Probe = NULL
CALLS: list = []


def note(e2e_ms: float) -> None:
    CALLS.append(CURRENT.end(e2e_ms))


def attach(system: str, sysobj: Any = None) -> Probe:
    """Set CURRENT for an extcmp system object (after its open()); the DuckDB (SQL)
    connection is replaced by its profiling proxy."""
    global CURRENT
    if not ENABLED:
        CURRENT = NULL
        return CURRENT
    if sysobj is not None and type(sysobj).__name__ == "WorkerSystem":
        CURRENT = WorkerProbe(system)          # EXTCMP_INPROC_WORKER=1: measured in the worker
        return CURRENT
    if system == "duckdbsql" and sysobj is not None:
        CURRENT = DuckProbe()
        sysobj.con = CURRENT.wrap(sysobj.con)
        return CURRENT
    CURRENT = probe(system, getattr(sysobj, "handle", None))
    return CURRENT
