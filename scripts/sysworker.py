"""In-process systems (Gnomon, DuckDB (SQL)) in a worker process, with e2e measured inside it.

aborted at the ceiling by ending its process (a DuckDB statement after interrupt() can hold the
connection, the W2-K1 hang; a native engine call cannot be interrupted at all). So, with
EXTCMP_INPROC_WORKER=1, every call of `native`, `duckdb` (Gnomon on DuckDB storage) and `duckdbsql`
runs in a forked worker that opened the system itself:

- **The timing loop itself runs inside the worker** (`WorkerSystem.measure`): the worker runs the same
  protocol function the in-process path runs (extcmp.measure_capped or extcmp_p1.measure) around `fn()`,
  so warm-ups and timed calls are back to back exactly as in-process, and e2e is measured where the
  in-process path measures it. (A request per call made Gnomon's sub-ms calls 10-20 us slower: the worker
  for the whole cell (request, pickle, pipe) is the **harness hop**, recorded outside e2e
  (`harness_hop_ms`). Single calls (the answer check) still go one request per call (`call`), with the
  worker's own e2e. Server systems keep their client-side e2e.
- **Ceiling watchdog**: the worker writes the start time of every call into shared memory (a ctypes
  double, no lock, no syscall: about 0.2 us inside the timer); the parent polls it every 0.1 s and kills the
  worker when a call has run longer than the client ceiling + grace (a native engine call cannot be
  interrupted by the worker's own alarm).
- Engine time, when on, is measured in the worker too (the same probe as in-process).
- **Abort**: the caller's client alarm interrupts the parent's wait; `cancel()` SIGKILLs the worker and
  starts a fresh one. A backstop wait (EXTCMP_CALL_CEILING_S + EXTCMP_INPROC_WORKER_GRACE_S) kills a
  worker that never answers; a worker that dies inside a call (the OOM killer) is recorded with its
  exit code. A call that never came back marks the worker busy: the next call gets a fresh worker.
- **A restart is a labelled event**: `events` holds {utc, event, query, cold_cache: true}; the
  restarted worker reopens the store with a cold cache, and the cell that follows says so.

Unset, nothing here is imported or used.
"""
from __future__ import annotations

import os
import time
from typing import Any, Callable

ENABLED = os.environ.get("EXTCMP_INPROC_WORKER") == "1" or os.environ.get("EXTCMP_DUCKDB_WORKER") == "1"
SYSTEMS = ("native", "duckdb", "duckdbsql", "ladybug", "raphtory", "raphtoryeq", "nativematch")


class _Last:
    """The last worker call of this process: (e2e_ms measured in the worker, hop_ms, engine record)."""
    call: tuple | None = None


def take(parent_ms: float) -> tuple[float, float | None]:
    """In a timing loop, right after fn(): the e2e to record and the harness hop. A call that went
    through a worker returns the worker's e2e; any other call returns the parent's own timer."""
    c, _Last.call = _Last.call, None
    if c is None:
        return parent_ms, None
    e2e, _hop, _rec = c
    return e2e, max(0.0, parent_ms - e2e)


#: the harness hop of every worker call since the timing loop last cleared it
HOPS: list[float] = []


def take_into(parent_ms: float) -> float:
    """take() for the timing loops: returns the e2e to record and appends the hop to HOPS."""
    ms, hop = take(parent_ms)
    if hop is not None:
        HOPS.append(hop)
    return ms


class WorkerTimeout(Exception):
    pass


def _protocols():
    import sys
    main = sys.modules.get("__main__")
    X = main if hasattr(main, "measure_capped") else __import__("extcmp")
    import extcmp_p1 as P1
    return X, P1


def _serve(conn, make: Callable[[], Any], system: str, watch) -> None:
    import signal
    X, P1 = _protocols()
    signal.signal(signal.SIGALRM, X._alarm)          # the client alarm, as cmd_query installs it in-process
    try:
        sysobj = make()
        sysobj.open()
        pr = None
        try:
            import engine_time as ET
            if ET.ENABLED:
                pr = ET.attach(system, sysobj)
        except Exception:                                        # noqa: BLE001
            pr = None
        settings = sysobj.settings() if hasattr(sysobj, "settings") else {}
        conn.send(("ready", settings, None, None))
    except Exception as e:                                       # noqa: BLE001
        conn.send(("setup-error", f"{type(e).__name__}: {e}"[:600], None, None))
        return
    import eval_harness as H
    while True:
        job = conn.recv()
        if job is None:
            try:
                sysobj.close()
            except Exception:                                    # noqa: BLE001
                pass
            return
        kind, qf = job[0], job[1]
        q = H.Query(*qf)
        if kind == "measure":
            conn.send(_measure(sysobj, q, job[2], X, P1, watch)); continue
        try:
            fn = sysobj.fn_for(q)
        except Exception as e:                                   # noqa: BLE001
            conn.send(("error", f"{type(e).__name__}: {e}"[:1000], None, None)); continue
        if kind == "prime":                 # the priming rule (prime_cap): unmeasured, the systems' caps raised
            import prime_cap as PC
            if fn is None:
                conn.send(("none", None, None, None)); continue
            t = time.perf_counter()
            try:
                with PC.priming():
                    fn()
                conn.send(("ok", None, (time.perf_counter() - t) * 1e3, None))
            except Exception as e:                               # noqa: BLE001
                conn.send(("error", f"{type(e).__name__}: {e}"[:1000], (time.perf_counter() - t) * 1e3, None))
            continue
        if kind == "has":
            conn.send(("ok", fn is not None, None, None)); continue
        if fn is None:
            conn.send(("none", None, None, None)); continue
        if pr is not None:
            pr.begin()
        t = time.perf_counter()
        try:
            payload = fn()
        except Exception as e:                                   # noqa: BLE001
            ms = (time.perf_counter() - t) * 1e3
            conn.send(("error", f"{type(e).__name__}: {e}"[:1000], ms, None)); continue
        ms = (time.perf_counter() - t) * 1e3
        rec = pr.end(ms) if pr is not None else None
        conn.send(("ok", payload, ms, rec))


def _measure(sysobj, q, proto: str, X, P1, watch) -> tuple:
    """Run the protocol for one cell inside the worker: ('ok', (payload, timings, warm, protocol),
    engine-time records, worker wall s) or ('raise', exception type name, message, None)."""
    import engine_time as ET
    fn = sysobj.fn_for(q)
    if fn is None:
        return ("none", None, None, None)

    def timed():
        watch.value = time.time()                       # the watchdog's start mark (inside the timer: ~0.2 us)
        try:
            return fn()
        finally:
            watch.value = 0.0
    ET.CALLS.clear()
    t0 = time.perf_counter()
    try:
        if proto == "p1":
            res = P1.measure(timed, X._Timeout, lambda: (P1.recover(sysobj), sysobj.fn_for(q))[1])
        else:
            res = X.measure_capped(timed)
    except Exception as e:                               # noqa: BLE001 - re-raised in the parent by name
        return ("raise", type(e).__name__, str(e)[:1000], list(ET.CALLS))
    return ("ok", res, list(ET.CALLS), time.perf_counter() - t0)


class SysWorker:
    def __init__(self, make: Callable[[], Any], system: str) -> None:
        self.make, self.system = make, system
        self.p = self.c = None
        self.busy = False
        self.events: list[dict[str, Any]] = []
        self.settings: dict[str, Any] = {}
        self.reprime = None          # the window's priming query: replayed in every fresh worker (restart)
        self.start()

    def start(self) -> None:
        import multiprocessing as mp
        ctx = mp.get_context("fork")
        self.c, child = ctx.Pipe()
        self.watch = ctx.Value("d", 0.0, lock=False)
        self.p = ctx.Process(target=_serve, args=(child, self.make, self.system, self.watch), daemon=True)
        self.p.start()
        child.close()
        if not self.c.poll(3600):
            self._kill()
            raise RuntimeError(f"{self.system} worker did not open its store within 3600 s")
        kind, settings, _, _ = self.c.recv()
        if kind != "ready":
            raise RuntimeError(f"{self.system} worker: {settings}")
        self.settings = {**(settings or {}), "inproc_worker": "EXTCMP_INPROC_WORKER=1: each call in a worker "
                         "process, e2e measured in the worker, the hop recorded outside e2e"}
        self.busy = False

    def _kill(self) -> None:
        if self.p is not None and self.p.is_alive():
            self.p.kill()
            self.p.join(30)
        self.p = None

    def restart(self, why: str, query: str | None = None) -> None:
        self._kill()
        self.events.append({"utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "event": why,
                            "query": query, "cold_cache": True})
        self.start()
        self._reprime(query)

    def _reprime(self, query: str | None) -> None:
        """        priming call both run in the fresh worker before the next cell, so neither is charged to its ceiling."""
        q = self.reprime
        if q is None:
            return
        import prime_cap as PC
        backstop = PC.PRIME_KILL_S
        t = time.perf_counter()
        self.c.send(("prime", (q.id, q.op, q.args, q.note, q.requires)))
        kind = "no answer within the backstop"
        if self.c.poll(backstop):
            try:
                kind = self.c.recv()[0]
            except EOFError:
                kind = "worker died"
        ev = {"utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "query": query,
              "event": f"re-primed the fresh worker with {q.id}: {kind}", "ms": round((time.perf_counter() - t) * 1e3, 3)}
        if kind not in ("ok", "none", "error"):
            self._kill()
            self.start()
            ev["event"] += "; a fresh worker without the prime"
        self.events.append(ev)

    def _ask(self, kind: str, q) -> tuple:
        if self.busy or self.p is None or not self.p.is_alive():
            self.restart("the previous call did not return (alarm or death): fresh worker", q.id)
        self.busy = True
        self.c.send((kind, (q.id, q.op, q.args, q.note, q.requires)))
        backstop = float(os.environ.get("EXTCMP_CALL_CEILING_S", "3600")) + \
            float(os.environ.get("EXTCMP_INPROC_WORKER_GRACE_S", "5"))
        if not self.c.poll(backstop):
            self.restart(f"no answer within the backstop {backstop:g} s: killed", q.id)
            raise WorkerTimeout(f"{self.system} worker gave no answer within {backstop:g} s: killed, restarted (cold cache)")
        try:
            out = self.c.recv()
        except EOFError:
            self.p.join(10)
            code = self.p.exitcode
            why = ", SIGKILL: likely out of memory" if code == -9 else ""
            self.restart(f"the worker died inside the call (exit code {code}{why})", q.id)
            raise RuntimeError(f"{self.system} worker died during the call (exit code {code}{why}); restarted (cold cache)")
        self.busy = False
        return out

    def measure(self, q, proto: str, exc: dict[str, type]):
        """The cell's protocol in the worker. Returns (payload, timings, warm, protocol, engine CALLS,
        hop s); raises the protocol's exception (mapped by name through `exc`) or a timeout after a
        watchdog kill."""
        if self.busy or self.p is None or not self.p.is_alive():
            self.restart("the previous call did not return (alarm or death): fresh worker", q.id)
        self.busy = True
        ceiling = max(float(os.environ.get("EXTCMP_CALL_CEILING_S", "3600")),
                      float(os.environ.get("EXTCMP_P1_CEILING_S", "0") or 0)) + \
            float(os.environ.get("EXTCMP_INPROC_WORKER_GRACE_S", "5"))
        t0 = time.perf_counter()
        self.watch.value = 0.0
        self.c.send(("measure", (q.id, q.op, q.args, q.note, q.requires), proto))
        while not self.c.poll(0.1):
            st = self.watch.value
            if st and time.time() - st > ceiling:
                self.restart(f"a call ran past the client ceiling + grace ({ceiling:g} s): killed", q.id)
                raise exc.get("_Timeout", TimeoutError)(f"call ran past {ceiling:g} s in the {self.system} worker: killed, restarted (cold cache)")
            if not self.p.is_alive():
                break
        try:
            kind, a, b, c = self.c.recv()
        except EOFError:
            self.p.join(10)
            code = self.p.exitcode
            why = ", SIGKILL: likely out of memory" if code == -9 else ""
            self.restart(f"the worker died inside the cell (exit code {code}{why})", q.id)
            raise RuntimeError(f"{self.system} worker died during the call (exit code {code}{why}); restarted (cold cache)")
        self.busy = False
        wall = time.perf_counter() - t0
        if kind == "none":
            raise NotImplementedError(f"not written for {self.system}")
        if kind == "raise":
            import re
            import engine_time as ET
            ET.CALLS[:] = c or []
            if a in ("_Timeout", "CeilingReached") or re.search(r"interrupt|timeout|timed out|ceiling|cancel", b, re.I):
                # after an interrupted or timed-out call a DuckDB connection can hold the next call (W2-K1,
                self.restart(f"after an interrupted or timed-out call ({a}): fresh worker", q.id)
            E = exc.get(a)
            if E is not None:
                raise E(b)
            raise RuntimeError(f"{a}: {b}")
        payload, timings, warm, protocol = a
        return payload, timings, warm, protocol, b, max(0.0, wall - c)

    def has(self, q) -> bool:
        kind, val, _, _ = self._ask("has", q)
        return bool(val) if kind == "ok" else False

    def call(self, q, kind: str = "call"):
        t = time.perf_counter()
        kind, payload, ms, rec = self._ask(kind, q)
        parent = (time.perf_counter() - t) * 1e3
        if kind == "ok":
            _Last.call = (ms, parent - ms, rec)
            _Last.rec = rec
            return payload
        _Last.call = None
        if kind == "none":
            raise NotImplementedError(f"not written for {self.system}")
        raise RuntimeError(payload)

    def close(self) -> None:
        if self.p is not None and self.p.is_alive():
            try:
                self.c.send(None)
                self.p.join(30)
            except Exception:                                    # noqa: BLE001
                pass
        self._kill()


class WorkerSystem:
    """Wraps an extcmp System object: open() starts the worker (the system is opened only there),
    fn_for() returns a call into it, cancel() kills and restarts it."""

    def __init__(self, inner: Any, make: Callable[[], Any]) -> None:
        self.inner, self.make = inner, make
        self.name = inner.name
        self.compare = getattr(inner, "compare", "exact")
        self.worker: SysWorker | None = None
        self._prime = None

    def __getattr__(self, n):
        return getattr(self.inner, n)

    def open(self) -> None:
        if self.worker is not None:
            self.worker.close()
        self.worker = SysWorker(self.make, self.inner.name)
        self.worker.reprime = self._prime

    def set_prime(self, q) -> None:
        """The window's priming query (extcmp.py, after P1.prime): replayed after every worker restart."""
        self._prime = q
        if self.worker is not None:
            self.worker.reprime = q

    def settings(self) -> dict[str, Any]:
        return self.worker.settings if self.worker else {}

    def fn_for(self, q):
        if not self.worker.has(q):
            return None
        return lambda: self.worker.call(q)

    def prime_fn(self, q):
        """The priming call in the worker, under prime_cap.priming() there (extcmp_p1.prime)."""
        if not self.worker.has(q):
            return None
        return lambda: self.worker.call(q, kind="prime")

    def measure(self, q, proto: str, exc: dict[str, type]):
        return self.worker.measure(q, proto, exc)

    def cancel(self) -> None:
        self.worker.restart("cancel after the client alarm (ceiling): killed", None)

    def close(self) -> None:
        if self.worker is not None:
            self.worker.close()

    def take_events(self) -> list[dict[str, Any]]:
        ev, self.worker.events = (self.worker.events, []) if self.worker else ([], [])
        return ev
