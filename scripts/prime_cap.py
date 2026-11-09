"""The priming rule, the same for every system: the priming call is unmeasured, outside
the ceiling, killed at 60 s (PRIME_KILL_S). While it runs, a system's own server-side cap is PRIME_KILL_S - 2 s
(the kit's cap rule: server cap = client kill - 2 s); the cap is reset to the cell's value before the warm-ups.

A system registers a hook at open (`hook(fn)`, fn(seconds) raises, fn(None) resets) where its cap is a session or
database setting (PostgreSQL statement_timeout, LadybugDB query timeout, Memgraph query.timeout), or reads `cap()`
per call where its cap is per call (a DuckDB interrupt timer, a Neo4j transaction timeout, a ClickHouse request's
max_execution_time). The harness wraps the priming call in `priming()` in the process that holds the system.
"""
from __future__ import annotations

import os
import sys
from contextlib import contextmanager
from typing import Any, Callable

PRIME_KILL_S = float(os.environ.get("TR_PRIME_KILL_S") or os.environ.get("EXTCMP_PRIME_KILL_S") or 60)
PRIME_CAP_S = PRIME_KILL_S - 2
RULE = f"priming call unmeasured, outside the ceiling, killed at {PRIME_KILL_S:g} s (server cap {PRIME_CAP_S:g} s)"
_ACTIVE: float | None = None
_HOOKS: list[Callable[[float | None], Any]] = []
EVENTS: list[str] = []


def active() -> float | None:
    """The server cap in force for the current call while priming, else None."""
    return _ACTIVE


def cap(default: float) -> float:
    return _ACTIVE if _ACTIVE is not None else default


def hook(fn: Callable[[float | None], Any]) -> None:
    _HOOKS.append(fn)


@contextmanager
def priming():
    global _ACTIVE
    _ACTIVE = PRIME_CAP_S
    for h in list(_HOOKS):
        try:
            h(PRIME_CAP_S)
        except Exception as e:                                   # noqa: BLE001
            EVENTS.append(f"prime cap raise failed: {type(e).__name__}: {e}"[:300])
    try:
        yield
    finally:
        _ACTIVE = None
        for h in list(_HOOKS):                                   # always reset before the warm-ups
            try:
                h(None)
            except Exception as e:                               # noqa: BLE001
                msg = f"prime cap reset failed: {type(e).__name__}: {e}"[:300]
                EVENTS.append(msg)
                print(msg, file=sys.stderr, flush=True)


def memgraph_hook(drv) -> None:
    """Memgraph's server cap is the database setting query.timeout (runtime; per-transaction timeouts are"""
    with drv.session() as s:
        rows = [list(r.values()) for r in s.run("SHOW DATABASE SETTINGS")]
    orig = next((str(r[1]) for r in rows if r and r[0] == "query.timeout"), None)
    if orig is None:
        return

    def h(sec):
        with drv.session() as s:
            s.run(f"SET DATABASE SETTING 'query.timeout' TO '{int(sec) if sec else orig}'").consume()
    hook(h)
