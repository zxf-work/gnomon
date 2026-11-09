"""P1 parameter-sensitivity study: curated parameter instances.

Until P1 every registry query was timed with one parameter set (for example
reachability from n1 over the first 10 % of the time range). P1 asks each
parameterisable query over a curated set of *instances* — the same registry
id with different arguments — chosen LDBC-style:

1. `factors`: factor tables computed on the frozen log's native store (degree,
   2-hop size, reachable-set size per window, version counts, match counts,
   events per window), with the native engine's own operators where the
   factor is an answer size;
2. `select`: instances picked around stated percentiles of each query's cost
   proxy (or on a fixed grid where the parameter is itself the design), then
   shuffled by MD5 hash and written as one versioned parameter file per log;
3. every system reads the identical file: `extcmp.py query --params FILE`
   (parameter-file mode) asks every selected registry id once per instance of
   the file with that instance's complete argument dict; the registry id stays
   fixed and the cell carries `instance` (and `stratum`). With the mode off
   nothing changes (tests/test_extcmp_p1.py checks the records are
   byte-identical);
4. `report`: per query, the median over instances of the per-instance median,
   the p10-p90 spread, Gnomon's win rate and the instance classes where it
   loses, the worst instance, the per-stratum medians, rotation agreement.

The timing protocol of the parameter-file mode is `measure` below (see
warmup exceeds EXTCMP_P1_SLOW_S switches to 1 warmup + 3 repetitions; a call
reaching EXTCMP_CALL_CEILING_S is aborted and the cell recorded as above the
ceiling (the kit's ceiling mechanism, with the P1 value set by the job).
"""

from __future__ import annotations

import engine_time as ET
import sysworker as SW
import argparse
from gnomon.core.errors import CostError
import hashlib
import json
import os
import signal
import statistics
import sys
import time
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

import eval_harness as H  # noqa: E402

FORMAT = "extcmp-p1-params/1"
VERSION = "p1-v1"

# --------------------------------------------------------------------------- #
# parameter files (used by extcmp.py in parameter-file mode)                    #
# --------------------------------------------------------------------------- #


def load(path: Path) -> dict[str, Any]:
    doc = json.loads(Path(path).read_text())
    if doc.get("format") != FORMAT:
        raise SystemExit(f"{path}: not a P1 parameter file (format {doc.get('format')!r})")
    seen = set()
    for inst in doc["instances"]:
        k = (inst["query"], inst["instance"])
        if k in seen:
            raise SystemExit(f"{path}: duplicate instance {k}")
        seen.add(k)
    return doc


def file_facts(path: Path) -> dict[str, Any]:
    raw = Path(path).read_bytes()
    doc = json.loads(raw)
    return {"path": str(path), "sha256": hashlib.sha256(raw).hexdigest(),
            "version": doc.get("version"), "log_sha256": doc.get("log", {}).get("sha256"),
            "n_instances": len(doc.get("instances", []))}


def expand(qs: list[H.Query], doc: dict[str, Any]) -> tuple[list[H.Query], list[dict[str, Any]]]:
    """Each registry query once per instance of the file (file order, which is
    the MD5 shuffle), with the instance's complete argument dict. Ids with no
    instance in the file are not asked. The argument names must be exactly the
    registry's own, so an adapter receives the call it always received."""
    by: dict[str, list[dict[str, Any]]] = {}
    for inst in doc["instances"]:
        by.setdefault(inst["query"], []).append(inst)
    out_q: list[H.Query] = []
    out_i: list[dict[str, Any]] = []
    for q in qs:
        for inst in by.get(q.id, []):
            if set(inst["args"]) != set(q.args):
                raise SystemExit(f"P1 instance {q.id}#{inst['instance']}: arguments "
                                 f"{sorted(inst['args'])} differ from the registry's {sorted(q.args)}")
            out_q.append(H.Query(q.id, q.op, dict(inst["args"]), q.note, q.requires))
            out_i.append(inst)
    return out_q, out_i


# --------------------------------------------------------------------------- #
# timing protocol                                                               #
# --------------------------------------------------------------------------- #

WARMUP = 2
# EXTCMP_P1_REPS; unset, the 10
REPS = int(os.environ.get("EXTCMP_P1_REPS", "10"))
SLOW_REPS = 3


def _slow_s() -> float:
    return float(os.environ.get("EXTCMP_P1_SLOW_S", "1.0"))


def _alarm_s() -> int:
    return int(os.environ.get("EXTCMP_CALL_CEILING_S", "3600"))


def ceiling_s() -> float:
    """The P1 ceiling: a call taking longer is recorded as above it. The
    server-side limit (EXTCMP_SERVER_LIMIT_S) is set to it for PostgreSQL and
    ClickHouse; the client alarm (EXTCMP_CALL_CEILING_S) is the backstop and
    sits above it, so the server cancels first."""
    return float(os.environ.get("EXTCMP_P1_CEILING_S", _alarm_s()))


class CeilingReached(Exception):
    pass


def protocol_facts() -> dict[str, Any]:
    return {"warmup": WARMUP, "reps": REPS, "slow_rule": {"first_warmup_over_s": _slow_s(),
            "then": f"1 warmup + {SLOW_REPS} reps"}, "ceiling_s": ceiling_s(),
            "server_limit_s": os.environ.get("EXTCMP_SERVER_LIMIT_S"), "client_alarm_s": _alarm_s()}


def measure(fn, timeout_exc=None, on_alarm=None) -> tuple[Any, list[float], list[float], str]:
    """P1 protocol. Returns (last payload, timed ms, warmup ms, protocol label).
    A call reaching the ceiling raises extcmp's _Timeout (its SIGALRM handler
    is installed by cmd_query)."""
    alarm, ceiling = _alarm_s(), ceiling_s()

    def call():
        if ET.ENABLED:
            ET.CURRENT.begin()
        signal.alarm(alarm)
        t = time.perf_counter()
        try:
            payload = fn()
        finally:
            signal.alarm(0)
        ms = (time.perf_counter() - t) * 1e3
        if SW.ENABLED:
            ms = SW.take_into(ms)           # e2e measured in the worker; the hop is recorded outside e2e
        if ET.ENABLED:
            ET.note(ms)
        if ms > ceiling * 1e3:
            raise CeilingReached(f"call took {ms:.0f} ms (client-side limit {ceiling:g} s)")
        return payload, ms

    try:
        payload, w0 = call()
    except Exception as e:                                  # noqa: BLE001
        # the first call of a cell may build per-window state (a cold
        # build): above the ceiling, cancelled by the server-side limit, or
        # stopped by the client alarm (the session is then recovered), it is
        # retried once; the retry decides
        alarm_hit = timeout_exc is not None and isinstance(e, timeout_exc)
        if not (alarm_hit or isinstance(e, CeilingReached) or is_server_timeout(f"{type(e).__name__}: {e}")):
            raise
        if alarm_hit and on_alarm is not None:
            fresh = on_alarm()           # recovers the session; may return the call rebuilt on it
            if callable(fresh):
                fn = fresh
        payload, w0 = call()
    warm = [w0]
    if w0 > _slow_s() * 1e3:
        protocol, reps = f"slow: 1 warmup + {SLOW_REPS} reps (first warmup > {_slow_s():g} s)", SLOW_REPS
    else:
        for _ in range(WARMUP - 1):
            payload, w = call()
            warm.append(w)
        protocol, reps = f"standard: {WARMUP} warmups + {REPS} reps", REPS
    timings = []
    for _ in range(reps):
        payload, t = call()
        timings.append(t)
    return payload, timings, warm, protocol


def verify_call(fn, alarm_s: int, timeout_exc, sysobj, refn=None) -> tuple[Any, float, dict[str, Any]]:
    """The answer-check call of one instance. A first call above the ceiling,
    cancelled by the server-side limit, or stopped by the client alarm is
    retried once (after recovering the session); the retry decides. Returns
    (payload, wall ms, notes); raises the retry's exception."""
    notes: dict[str, Any] = {}
    for attempt in (1, 2):
        signal.alarm(alarm_s)
        t = time.perf_counter()
        try:
            payload = fn()
        except timeout_exc as e:
            signal.alarm(0)
            if attempt == 2:
                raise
            notes["first_call"] = f"client alarm ({e})"
            notes["recover"] = recover(sysobj)
            if refn is not None:
                fn = refn()                  # the call rebuilt on the new session
            continue
        except Exception as e:                              # noqa: BLE001
            signal.alarm(0)
            if attempt == 2 or not is_server_timeout(f"{type(e).__name__}: {e}"):
                raise
            notes["first_call"] = f"{type(e).__name__}: {e}"[:200]
            continue
        finally:
            signal.alarm(0)
        wall = (time.perf_counter() - t) * 1e3
        if SW.ENABLED:
            wall = SW.take_into(wall)
        if attempt == 1 and wall > ceiling_s() * 1e3:
            notes["first_call_ms"] = round(wall, 3)
            continue
        return payload, wall, notes
    raise AssertionError("unreachable")


def _alarm_off(timeout_exc) -> bool:
    """Turn the client alarm off; True if it fired while doing so (its exception is swallowed)."""
    fired = False
    while True:
        try:
            signal.alarm(0)
            return fired
        except timeout_exc:
            fired = True


def prime(sysobj, q, alarm_s: int, timeout_exc) -> dict[str, Any]:
    """One untimed call before a window's first instance, so the store's
    open-time state is not charged to whichever instance comes first."""
    import prime_cap as PC
    # the priming rule (prime_cap): unmeasured, outside the ceiling, killed at PC.PRIME_KILL_S; the system's own
    # cap raised to PC.PRIME_CAP_S for it and reset after it (in the worker process when there is one)
    alarm_s = int(PC.PRIME_KILL_S)
    inworker = hasattr(sysobj, "prime_fn")
    fn = sysobj.prime_fn(q) if inworker else sysobj.fn_for(q)
    if fn is None:
        return {"primed": False, "reason": "not written"}
    t = time.perf_counter()
    signal.alarm(alarm_s)
    try:
        if inworker:
            fn()
        else:
            with PC.priming():
                fn()
        out = {"primed": True, "rule": PC.RULE}
    except timeout_exc as e:
        _alarm_off(timeout_exc)
        out = {"primed": False, "reason": f"client alarm ({e})", "recover": None}
        out["recover"] = recover(sysobj)
    except Exception as e:                                  # noqa: BLE001
        # a system's own timeout that surfaces as the alarm fires (an equal cap, or an interrupt honoured late):
        # the pending alarm runs at the handler's first bytecode, so it is caught here, not left to end the window
        #
        late = _alarm_off(timeout_exc)
        out = {"primed": False, "reason": f"{type(e).__name__}: {e}"[:200] + (" (client alarm too)" if late else "")}
    finally:
        signal.alarm(0)
    out["ms"] = round((time.perf_counter() - t) * 1e3, 3)
    return out


def recover(sysobj) -> str:
    """After a client-side alarm: cancel what the server still runs and open
    a fresh session (PostgreSQL, ClickHouse, DuckDB (SQL)); returns what was done."""
    done = []
    cancel = getattr(sysobj, "cancel", None)
    if cancel is not None:
        try:
            cancel()
            done.append("cancel sent")
        except Exception as e:                              # noqa: BLE001
            done.append(f"cancel failed: {e}"[:120])
    if getattr(sysobj, "name", "") in ("postgres", "clickhouse", "duckdbsql"):
        try:
            sysobj.close()
        except Exception:                                   # noqa: BLE001
            pass
        try:
            sysobj.open()
            done.append("reconnected")
        except Exception as e:                              # noqa: BLE001
            done.append(f"reconnect failed: {e}"[:120])
    return "; ".join(done) or "nothing to recover"


_TIMEOUT_MARKS = ("statement timeout", "canceling statement due to", "TIMEOUT_EXCEEDED",
                  "Timeout exceeded", "client-side limit", "INTERRUPT", "Interrupted")


def is_server_timeout(msg: str) -> bool:
    return any(m in msg for m in _TIMEOUT_MARKS)


# --------------------------------------------------------------------------- #
# curation helpers                                                              #
# --------------------------------------------------------------------------- #

def md5(*parts: Any) -> str:
    return hashlib.md5(":".join(str(p) for p in parts).encode()).hexdigest()


def quantile(xs: list[float], p: float) -> float:
    s = sorted(xs)
    if not s:
        return float("nan")
    i = (len(s) - 1) * p / 100.0
    lo = int(i)
    hi = min(lo + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (i - lo)


def pick_percentiles(cands: list[dict[str, Any]], pcts: tuple[float, ...], k: int,
                     salt: str, used: set | None = None) -> list[dict[str, Any]]:
    """k candidates nearest to each percentile of `value` (ties by MD5 order),
    without reuse; each pick carries stratum 'p<pct>' and the target."""
    used = set() if used is None else used
    vals = [c["value"] for c in cands]
    out = []
    for p in pcts:
        target = quantile(vals, p)
        pool = sorted((c for c in cands if c["id"] not in used),
                      key=lambda c: (abs(c["value"] - target), md5(salt, c["id"])))
        for c in pool[:k]:
            used.add(c["id"])
            out.append(dict(c, stratum=f"p{p:g}", target=target))
    return out


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


# --------------------------------------------------------------------------- #
# the design: windows, instants and grids (one place; select reads the same)   #
# --------------------------------------------------------------------------- #

def design(t0: int, t1: int) -> dict[str, Any]:
    span = t1 - t0
    mid = t0 + span // 2

    def placed(w: int) -> dict[str, dict[str, int]]:
        return {"early": {"t_a": t0, "t_b": t0 + w},
                "middle": {"t_a": mid - w // 2, "t_b": mid - w // 2 + w},
                "late": {"t_a": t1 - w, "t_b": t1}}
    reach = {f"w{pct}-{pos}": win for pct in (1, 10, 25)
             for pos, win in placed(span * pct // 100).items()}
    agg = {}
    for pct in (25, 50, 100):
        for pos, win in (placed(span * pct // 100).items() if pct < 100 else [("whole", {"t_a": t0, "t_b": t1})]):
            agg[f"w{pct}-{pos}"] = win
    return {
        "span": span, "mid": mid,
        "snap_instants": {"q1": t0 + span // 4, "mid": mid, "q3": t0 + 3 * span // 4},
        "reach_windows": reach,
        "paths_windows": {"early": {"t_a": t0, "t_b": t0 + span // 4},
                          "middle": {"t_a": mid - span // 8, "t_b": mid + span // 8}},
        "nbr_intervals": {f"w{pct}": (mid, mid + span * pct // 100) for pct in (1, 10, 25)},
        "agg_windows": agg, "agg_buckets": (50, 100),
        "motif_sizes": (50, 100, 200, 300, 400), "motif_delta_pct": (1, 2, 5),
        "diff_gaps_permille": (5, 25, 100), "diff_positions_pct": (10, 30, 50, 70, 85),
        "w10_width": H.W10_WIDTH,
    }


PCTS = (10, 50, 90, 99)          # the stated percentiles of the cost proxy
LOW_TAIL = 1                     # added where the factor table shows a low tail (see select)


# --------------------------------------------------------------------------- #
# factor tables                                                                 #
# --------------------------------------------------------------------------- #

def _open(store: Path):
    import gnomon
    from gnomon.temporal.algebra import ensure_all_registered
    ensure_all_registered()
    from extcmp import open_existing_store         # P-7: never create the store empty
    return open_existing_store(store, "native")


def _call(a, op: str, args: dict[str, Any]) -> dict[str, Any]:
    from gnomon.temporal.algebra import call_operator
    return call_operator(a, op, dict(args))


def factors_registry(log: Path, store: Path, out: Path, n_snap: int, n_reach: int,
                     n_paths: int) -> dict[str, Any]:
    import numpy as np
    data = H.dataset_from_log(log)
    D = design(data.t0, data.t1)
    st = _open(store)
    a = st.adapter
    t_start = time.time()
    E = a.edges_columnar(columns=["src_id", "dst_id", "vt_s", "vt_e"])
    src, dst, vs, ve = (np.asarray(E[k]) for k in ("src_id", "dst_id", "vt_s", "vt_e"))
    N = int(max(src.max(), dst.max())) + 1
    uids = a.uids_for(list(range(N)))
    order = sorted(range(N), key=lambda i: md5("node", uids[i]))     # MD5 order of the nodes
    F: dict[str, Any] = {"log": str(log), "log_sha256": sha256_file(log), "store": str(store),
                         "scale": data.scale, "t0": data.t0, "t1": data.t1,
                         "tt_epoch1": data.tt_epoch1, "n_nodes": N, "n_edges_believed": int(len(src)),
                         "design": D}
    # versions per entity
    from collections import Counter
    vc = Counter(v.uid for v in a.all_node_versions())
    F["node_versions"] = {"distribution": sorted(Counter(vc.values()).items()),
                          "multi_version": sorted([u, c] for u, c in vc.items() if c > 1)}
    # degree (all time, current belief) and per-window out-degree
    outdeg = np.bincount(src, minlength=N)
    indeg = np.bincount(dst, minlength=N)
    F["degree_all_time"] = {"out": _dist(outdeg), "in": _dist(indeg), "total": _dist(outdeg + indeg)}

    def outdeg_in(win):
        m = (vs < win["t_b"]) & (ve > win["t_a"])
        return np.bincount(src[m], minlength=N)

    def incident_in(lo, hi):
        m = (vs < hi) & (ve > lo)
        return np.bincount(src[m], minlength=N) + np.bincount(dst[m], minlength=N)
    # snap.hop2: 2-hop size from the native operator
    snap = {}
    for name, t in D["snap_instants"].items():
        rows = []
        for i in order[:n_snap]:
            p = _call(a, "snapshot_subgraph", {"seeds": [uids[i]], "hops": 2, "t_valid": t})
            rows.append({"id": uids[i], "value": int(p["nodes_total"]), "edges": int(p["rows_total"])})
        snap[name] = rows
    F["snap_2hop"] = snap
    print(f"  snap done {time.time() - t_start:.0f}s", flush=True)
    # reach.window: reachable-set size; MD5 sample plus the lowest out-degree
    # nodes of the window (where the low tail lives)
    reach = {}
    for name, win in D["reach_windows"].items():
        od = outdeg_in(win)
        low = sorted(range(N), key=lambda i: (int(od[i]), md5("low", uids[i])))[: n_reach // 3]
        ids = list(dict.fromkeys(order[:n_reach] + low))
        rows = []
        for i in ids:
            p = _call(a, "temporal_reachability", {"src": uids[i], "window": win})
            rows.append({"id": uids[i], "value": int(p["rows_total"]), "outdeg_in_window": int(od[i]),
                         "sample": "md5" if i in order[:n_reach] else "low-outdegree"})
        reach[name] = {"rows": rows, "outdeg_in_window": _dist(od)}
        print(f"  reach {name} done {time.time() - t_start:.0f}s", flush=True)
    F["reach_size"] = reach
    # paths.k: (src, dst) pairs classified by the minimum hop count of the
    # native answer (k=3, max_hops=3): 1, 2, 3, or none within 3 hops
    paths = {}
    for name, win in D["paths_windows"].items():
        m = (vs < win["t_b"]) & (ve > win["t_a"])
        s_w, d_w = src[m], dst[m]
        idx = np.argsort(s_w, kind="stable")
        s_sorted, d_sorted = s_w[idx], d_w[idx]
        starts = np.searchsorted(s_sorted, np.arange(N + 1))

        def nbrs(i):
            return d_sorted[starts[i]:starts[i + 1]]
        vs_sorted, ve_sorted = vs[m][idx], ve[m][idx]

        def _bfs_hops(i, j, win, max_hops=3):
            """Minimum hops of a time-respecting path i -> j inside win (None beyond max_hops)."""
            arr = {int(i): int(win["t_a"])}
            for hop in range(1, max_hops + 1):
                nxt: dict[int, int] = {}
                for u, a_u in arr.items():
                    lo, hi = starts[u], starts[u + 1]
                    for k in range(lo, hi):
                        tau = max(a_u, int(vs_sorted[k]))
                        if tau < int(ve_sorted[k]) and tau < win["t_b"]:
                            v = int(d_sorted[k])
                            if v not in nxt or tau < nxt[v]:
                                nxt[v] = tau
                if int(j) in nxt:
                    return hop
                arr = nxt
            return None
        rows = []
        for i in order[:n_paths]:
            n1 = nbrs(i)
            if len(n1) == 0:
                continue
            h = int(md5("pairs", uids[i])[:8], 16)
            c1 = int(n1[h % len(n1)])
            n2 = nbrs(c1)
            c2 = int(n2[h % len(n2)]) if len(n2) else None
            n3 = nbrs(c2) if c2 is not None else []
            c3 = int(n3[h % len(n3)]) if len(n3) else None
            cr = order[(order.index(i) + 1 + h % (N - 1)) % N]
            for kind, j in (("direct", c1), ("two-step", c2), ("three-step", c3), ("random", cr)):
                if j is None or j == i:
                    continue
                try:
                    p = _call(a, "temporal_paths", {"src": uids[i], "dst": uids[j], "window": win,
                                                    "k": 3, "max_hops": 3})
                except CostError:
                    # W2: Gnomon's expansion budget refuses
                    # the hop class comes from an independent layered earliest-arrival search
                    # (same step rule as temporal_paths: tau = max(arrival, vt_s) < min(vt_e, t_b),
                    # node-simple by minimality) and the refusal is kept as the recorded outcome
                    rows.append({"id": f"{uids[i]}>{uids[j]}", "src": uids[i], "dst": uids[j],
                                 "candidate": kind, "hops": _bfs_hops(i, j, win), "rows_total": -1,
                                 "outcome": "budget", "class_source": "earliest-arrival search"})
                    continue
                hops = min((r["hops"] for r in p["rows"]), default=None)
                rows.append({"id": f"{uids[i]}>{uids[j]}", "src": uids[i], "dst": uids[j],
                             "candidate": kind, "hops": hops, "rows_total": int(p["rows_total"])})
        paths[name] = rows
        print(f"  paths {name} done {time.time() - t_start:.0f}s", flush=True)
    F["paths_hops"] = paths
    # nbr.evolution: incident edges overlapping the interval
    F["nbr_degree"] = {name: [{"id": uids[i], "value": int(v)} for i, v in
                              ((i, incident_in(lo, hi)[i]) for i in order[:5000])]
                       for name, (lo, hi) in D["nbr_intervals"].items()}
    # coactive.narrow: |A| x |B| of the two sources' edge sets (all time)
    pairs = []
    for k in range(3000):
        i, j = order[(2 * k) % N], order[(2 * k + 1) % N]
        pairs.append({"id": f"{uids[i]}|{uids[j]}", "a": uids[i], "b": uids[j],
                      "edges_a": int(outdeg[i]), "edges_b": int(outdeg[j]),
                      "value": int(outdeg[i]) * int(outdeg[j])})
    F["coactive_pairs"] = pairs
    # resolve.substr: match count from the native operator over candidate strings
    cand = [str(d) for d in range(10)] + [f"{d:02d}" for d in range(100)] + [f"{d:03d}" for d in range(1000)]
    cand += [f"{d:04d}" for d in sorted(range(10000), key=lambda d: md5("s4", d))[:1500]]
    cand += [f"{d:05d}" for d in sorted(range(100000), key=lambda d: md5("s5", d))[:500]]
    cand += [f"n{d}" for d in sorted(range(10000), key=lambda d: md5("sn", d))[:800]]
    cand += [f"n{d}" for d in range(10)] + ["alpha", "alp", "corrected", "n"]
    subs = []
    for s in dict.fromkeys(cand):
        p = _call(a, "resolve_entities", {"query": s})
        subs.append({"id": s, "value": int(p["rows_total"])})
    F["substr_matches"] = subs
    print(f"  substr done {time.time() - t_start:.0f}s", flush=True)
    # grids: motif, windowed aggregates, w10 positions, diff.global
    flt = sorted({uids[i] for i in set(src.tolist())})      # every source, lexicographic (registry rule)
    F["motif_grid"] = []
    for size in D["motif_sizes"]:
        for dp in D["motif_delta_pct"]:
            args = {"motif": "M_triangle_cyclic", "delta": D["span"] * dp // 100,
                    "window": {"t_a": data.t0, "t_b": data.t1}, "node_filter": flt[:size]}
            t = time.perf_counter()
            try:
                p = _call(a, "count_temporal_motifs", args)
                F["motif_grid"].append({"size": size, "delta_pct": dp, "count": p["count"],
                                        "events": p["n_events_in_window"],
                                        "ms": round((time.perf_counter() - t) * 1e3, 1)})
            except Exception as e:                          # noqa: BLE001
                F["motif_grid"].append({"size": size, "delta_pct": dp, "error": f"{type(e).__name__}: {e}"[:200]})
    F["agg_events"] = {name: int(((vs >= w["t_a"]) & (vs < w["t_b"])).sum())
                       for name, w in D["agg_windows"].items()}
    F["w10_events_per_tick"] = {"note": "one event per tick except the burst band [scale/2, scale/2 + scale/200) at 10 per tick",
                                "burst_band": [data.scale // 2, data.scale // 2 + max(1, data.scale // 20) // 10]}
    diffs = []
    for g in D["diff_gaps_permille"]:
        for pos in D["diff_positions_pct"]:
            ta = data.t0 + D["span"] * pos // 100
            p = _call(a, "diff_snapshots", {"t1": ta, "t2": ta + D["span"] * g // 1000})
            diffs.append({"gap_permille": g, "pos_pct": pos,
                          "edges_added": p["edges_added_total"], "edges_removed": p["edges_removed_total"]})
    F["diff_grid"] = diffs
    F["elapsed_s"] = round(time.time() - t_start, 1)
    st.close()
    out.write_text(json.dumps(F, indent=1, default=str) + "\n")
    return F


def _dist(arr) -> dict[str, float]:
    xs = [float(x) for x in arr]
    return {f"p{p:g}": quantile(xs, p) for p in (0, 1, 10, 50, 90, 99, 100)}


def factors_belief(log: Path, store: Path, out: Path, n_reach: int) -> dict[str, Any]:
    """Factor tables for the belief-time family B on one density point's log."""
    import numpy as np
    import extcmp_beliefq as B
    from gnomon.core.model import OPEN_END
    data = H.dataset_from_log(log)
    p = B.derive_params(log, data.scale, data.t0, data.t1)
    st = _open(store)
    a = st.adapter
    t_start = time.time()
    F: dict[str, Any] = {"log": str(log), "log_sha256": sha256_file(log), "store": str(store),
                         "scale": data.scale, "t0": data.t0, "t1": data.t1, "beliefq_params": p}
    # the store's correction history: every batch that ingests no events
    corr = []
    with open(log) as f:
        for line in f:
            rec = json.loads(line)
            if "tt" in rec and not any(op["op"] == "ingest_events" for op in rec["ops"]):
                corr.append(rec["tt"])
    F["correction_batches"] = len(corr)
    F["T_points"] = {f"q{q}": corr[min(len(corr) - 1, int(len(corr) * q / 100))] for q in (10, 30, 50, 70, 90)}
    E = a.edges_columnar(columns=["src_id", "dst_id", "vt_s", "vt_e"])
    src, dst, vs, ve = (np.asarray(E[k]) for k in ("src_id", "dst_id", "vt_s", "vt_e"))
    N = int(max(src.max(), dst.max())) + 1
    uids = a.uids_for(list(range(N)))
    order = sorted(range(N), key=lambda i: md5("node", uids[i]))
    # seed sets of 20: contiguous id blocks (one community, overlapping
    # neighbourhoods) and MD5-random sets (spread out); union 2-hop size @now
    sets = []
    for k in range(100):
        s0 = int(md5("block", k)[:8], 16) % (N - 20)
        sets.append({"id": f"block{s0}", "seeds": [f"n{s0 + j}" for j in range(20)], "kind": "contiguous"})
    for k in range(100):
        sets.append({"id": f"rand{k}", "seeds": [uids[order[(20 * k + j) % N]] for j in range(20)], "kind": "random"})
    for s in sets:
        r = _call(a, "snapshot_subgraph", {"seeds": s["seeds"], "hops": 2, "t_valid": p["t_star"], "limit": 10_000})
        s["value"] = int(r["nodes_total"])
        s["edges"] = int(r["rows_total"])
    F["seed_sets"] = sets
    # reach sources over the B reach window
    win = p["reach_window"]
    m = (vs < win["t_b"]) & (ve > win["t_a"])
    od = np.bincount(src[m], minlength=N)
    low = sorted(range(N), key=lambda i: (int(od[i]), md5("low", uids[i])))[: n_reach // 3]
    rows = []
    for i in list(dict.fromkeys(order[:n_reach] + low)):
        r = _call(a, "temporal_reachability", {"src": uids[i], "window": win})
        rows.append({"id": uids[i], "value": int(r["rows_total"]), "outdeg_in_window": int(od[i])})
    F["reach_size"] = rows
    print(f"  B reach done {time.time() - t_start:.0f}s", flush=True)
    # belief-diff windows: the study's window shifted by multiples of L/2
    L = p["L"]
    dw = p["diff_window"]
    wins = {f"s{k:+d}": {"t_a": dw["t_a"] + k * L // 2, "t_b": dw["t_b"] + k * L // 2} for k in (-2, -1, 0, 1)}
    F["diff_windows"] = wins
    diffs = []
    for wn, w in wins.items():
        for tq, T in [("base", p["T_base"])] + list(F["T_points"].items()):
            r = H._diff_belief_dispatch(a, {"t1": T, "t2": OPEN_END, "window": w, "limit": 1000})
            diffs.append({"window": wn, "t1": tq, "rows": int(r["added_total"]) + int(r["retracted_total"])})
    F["belief_diff"] = diffs
    # edge version counts (every version ever written)
    from collections import Counter, defaultdict
    vcount: Counter = Counter()
    first: dict[str, Any] = {}
    for v in a.all_edge_versions():
        vcount[v.eid] += 1
        if v.eid not in first:
            first[v.eid] = {"eid": v.eid, "src": v.src, "dst": v.dst, "rel_type": v.rel_type,
                            "disc": getattr(v, "disc", None)}
    F["edge_versions"] = {"distribution": sorted(Counter(min(c, 9) for c in vcount.values()).items())}
    by = defaultdict(list)
    for e, c in vcount.items():
        by[min(c, 3)].append(e)
    F["edge_candidates"] = {str(k): [dict(first[e], versions=vcount[e]) for e in
                                     sorted(v, key=lambda e: md5("edge", e))[:200]] for k, v in by.items()}
    F["elapsed_s"] = round(time.time() - t_start, 1)
    st.close()
    out.write_text(json.dumps(F, indent=1, default=str) + "\n")
    return F


# --------------------------------------------------------------------------- #
# selection                                                                     #
# --------------------------------------------------------------------------- #

def _inst(query: str, args: dict[str, Any], stratum: str, cls: str, factors: dict[str, Any]) -> dict[str, Any]:
    iid = md5(VERSION, query, json.dumps(args, sort_keys=True))[:8]
    return {"query": query, "instance": iid, "stratum": stratum, "class": cls,
            "factors": factors, "args": args}


def _with(template: dict[str, Any], **kw) -> dict[str, Any]:
    a = json.loads(json.dumps(template))
    a.update(kw)
    return a


def _write(out: Path, log_facts: dict[str, Any], instances: list[dict[str, Any]], notes: list[str]) -> None:
    instances = sorted(instances, key=lambda i: md5(VERSION, "order", i["query"], i["instance"]))
    keys = [(i["query"], i["instance"]) for i in instances]
    dup = sorted({k for k in keys if keys.count(k) > 1})
    if dup:
        # W2: two strata can pick the same seed set; the later copy is
        # dropped and named in the notes (P1's files had none, so they are unchanged)
        seen: set = set()
        kept = []
        for i in instances:
            k = (i["query"], i["instance"])
            if k not in seen:
                seen.add(k)
                kept.append(i)
        instances = kept
        notes = list(notes) + [f"dropped duplicate instances (identical arguments): {dup}"]
    by_q: dict[str, int] = {}
    for i in instances:
        by_q[i["query"]] = by_q.get(i["query"], 0) + 1
    doc = {"format": FORMAT, "version": VERSION, "log": log_facts,
           "selection": {"percentiles": list(PCTS), "shuffle": "md5(version, 'order', query, instance)",
                         "instance_id": "md5(version, query, canonical args)[:8]", "notes": notes},
           "counts": by_q, "instances": instances}
    out.write_text(json.dumps(doc, indent=1, sort_keys=False) + "\n")
    print(f"{out}: {len(instances)} instances", json.dumps(by_q))


def select_registry(F: dict[str, Any], Fd0: dict[str, Any], out: Path, reach_k: int) -> None:
    D = F["design"]
    t0, t1, span = F["t0"], F["t1"], D["span"]
    reg = {q.id: q.args for q in H.registry(t0, t1, F["tt_epoch1"], ())}
    ins: list[dict[str, Any]] = []
    notes: list[str] = []
    # hist.single / hist.asof: version count is degenerate on this log (every
    # node has one version except n1): n1 is its own stratum, the rest are
    # MD5-random one-version nodes
    multi = [u for u, c in F["node_versions"]["multi_version"]]
    one = sorted((u for u in (r["id"] for r in F["snap_2hop"]["mid"]) if u not in multi),
                 key=lambda u: md5("hist", u))[: 20 - len(multi)]
    ents = [(u, f"{dict(F['node_versions']['multi_version'])[u]} versions") for u in multi] + [(u, "1 version") for u in one]
    notes.append(f"hist: version-count distribution {F['node_versions']['distribution']} (degenerate): "
                 f"multi-version nodes {multi} are one stratum, 19 MD5-random one-version nodes the other")
    bp = Fd0["beliefq_params"]
    asof = {"epoch1": F["tt_epoch1"], "mid-corrections": Fd0["T_points"]["q50"], "now": bp["now"]}
    for u, st in ents:
        ins.append(_inst("hist.single", _with(reg["hist.single"], uid=u), st, st, {"versions": st}))
        for an, T in asof.items():
            ins.append(_inst("hist.asof", _with(reg["hist.asof"], uid=u, as_of_tt=T), st, f"as-of {an}",
                             {"versions": st, "as_of": an}))
    # snap.hop2: 2-hop size per instant
    for name, t in D["snap_instants"].items():
        for c in pick_percentiles(F["snap_2hop"][name], PCTS, 5, f"snap{name}"):
            ins.append(_inst("snap.hop2", _with(reg["snap.hop2"], seeds=[c["id"]], t_valid=t), c["stratum"],
                             f"instant {name}", {"two_hop_nodes": c["value"], "instant": name}))
    # reach.window: reachable-set size per window
    # The factor table decides the design here: in every window but the
    # first 1 % the reachable set takes two values, empty (no out-edge in the
    # window) or >= 99.7 % of the nodes, so percentiles of the MD5 sample all
    # land on "full" and 20 sources per window would repeat one instance 20
    # times. Windows whose sizes vary (p10 < p90 in the MD5 sample) get the
    # stated percentiles (3 sources each); the others get 2 "full" sources and
    # 2 "tiny" ones (reachable set < 1 % of the nodes, from the lowest
    # out-degree candidates), or 4 "full" where no tiny source exists.
    n_nodes = F["n_nodes"]
    for name, win in D["reach_windows"].items():
        rows = F["reach_size"][name]["rows"]
        md = [r for r in rows if r["sample"] == "md5"]
        vals = [r["value"] for r in md]
        if quantile(vals, 10) < quantile(vals, 90):
            picks = pick_percentiles(md, PCTS, 3, f"reach{name}")
            how = "percentiles"
        else:
            tiny = sorted((r for r in rows if r["value"] < n_nodes // 100), key=lambda r: md5("tiny", name, r["id"]))
            full = sorted((r for r in md if r["value"] >= n_nodes // 100), key=lambda r: md5("full", name, r["id"]))
            nt = min(2, len({r["id"] for r in tiny}))
            tiny = list({r["id"]: r for r in tiny}.values())[:nt]
            picks = [dict(r, stratum="tiny (< 1 % of nodes)") for r in tiny] + \
                    [dict(r, stratum="full (>= 99 % of nodes)") for r in full[: 4 - nt]]
            how = f"two-valued: {nt} tiny + {4 - nt} full"
        notes.append(f"reach.window {name}: {how}; MD5-sample sizes p10={quantile(vals, 10):.0f} "
                     f"p50={quantile(vals, 50):.0f} p90={quantile(vals, 90):.0f}")
        for c in picks:
            ins.append(_inst("reach.window", _with(reg["reach.window"], src=c["id"], window=win), c["stratum"],
                             name, {"reachable": c["value"], "window": name, "outdeg_in_window": c["outdeg_in_window"]}))
    # paths.k: hop class from the native answer
    for name, win in D["paths_windows"].items():
        rows = F["paths_hops"][name]
        for cls in (1, 2, 3, None):
            pool = sorted((r for r in rows if r["hops"] == cls), key=lambda r: md5("paths", name, r["id"]))
            pool = list({r["id"]: r for r in pool}.values())[:5]
            if len(pool) < 5:
                notes.append(f"paths.k {name}: only {len(pool)} pairs at hops={cls}")
            lab = f"{cls} hops" if cls else "unreachable within 3 hops"
            for r in pool:
                ins.append(_inst("paths.k", _with(reg["paths.k"], src=r["src"], dst=r["dst"], window=win), lab,
                                 f"window {name}", {"hops": cls, "paths": r["rows_total"], "window": name}))
    # nbr.evolution: incident edges in the interval
    for name, (lo, hi) in D["nbr_intervals"].items():
        for c in pick_percentiles(F["nbr_degree"][name], PCTS, 5, f"nbr{name}"):
            ins.append(_inst("nbr.evolution", _with(reg["nbr.evolution"], uid=c["id"], t1=lo, t2=hi), c["stratum"],
                             f"interval {name}", {"incident_edges": c["value"], "interval": name}))
    # coactive.narrow: |A| x |B|
    for c in pick_percentiles(F["coactive_pairs"], PCTS, 5, "coactive"):
        ins.append(_inst("coactive.narrow", _with(reg["coactive.narrow"], a_spec={"src": c["a"]}, b_spec={"src": c["b"]}),
                         c["stratum"], "pair", {"edges_a": c["edges_a"], "edges_b": c["edges_b"]}))
    # resolve.substr: match counts 1, 10, 100, 1k, 10k
    import math
    used: set = set()
    for target in (1, 10, 100, 1000, 10000):
        pool = sorted((c for c in F["substr_matches"] if c["value"] > 0 and c["id"] not in used),
                      key=lambda c: (abs(math.log(c["value"] / target)), md5("substr", c["id"])))[:4]
        for c in pool:
            used.add(c["id"])
            ins.append(_inst("resolve.substr", _with(reg["resolve.substr"], query=c["id"]), f"~{target} matches",
                             f"~{target} matches", {"matches": c["value"]}))
    # motif.filtered: grid (filter size x delta), the registry's filter rule
    # every node is a source on this log, so the registry's rule (sources in
    # lexicographic order, first 200) extends to the first k uids in that order
    import extcmp as X
    flt_all = sorted(f"n{i}" for i in range(F["n_nodes"]))
    reg_flt = list(X.dataset(Path(F["log"])).filter_uids)
    assert flt_all[: len(reg_flt)] == reg_flt, "motif filter rule does not extend the registry's"
    for g in F["motif_grid"]:
        if "error" in g:
            notes.append(f"motif.filtered size {g['size']} delta {g['delta_pct']} %: native {g['error']} — not selected")
            continue
        ins.append(_inst("motif.filtered", _with(reg["motif.filtered"], delta=span * g["delta_pct"] // 100,
                                                 node_filter=flt_all[: g["size"]]),
                         f"filter {g['size']}", f"delta {g['delta_pct']} %", {"events": g["events"], "count": g["count"]}))
    # whole-window aggregates: width x position x stride
    for q in ("series.count", "burst.zscore", "agg.rel_bucket"):
        for name, w in D["agg_windows"].items():
            for nb in D["agg_buckets"]:
                ins.append(_inst(q, _with(reg[q], window=w, stride=max(1, (w["t_b"] - w["t_a"]) // nb)),
                                 name.split("-")[0], f"{name}, {nb} buckets",
                                 {"events": F["agg_events"][name], "buckets": nb}))
    notes.append("series/burst/agg: the 100 % window has one position, so 7 windows x 2 strides = 14 per query (not 18)")
    # *.w10: 20 positions, 16 on an even grid, 4 inside the burst band
    b0, b1 = F["w10_events_per_tick"]["burst_band"]
    pos = [t0 + H.W10_OFFSET + j * ((span - 2 * H.W10_OFFSET) // 16) for j in range(16)]
    pos = [(p_, "1 event per tick") for p_ in pos] + [(b0 + (2 * j + 1) * ((b1 - b0) // 8), "burst band, 10 per tick") for j in range(4)]
    for q in ("series.count.w10", "burst.zscore.w10", "agg.rel_bucket.w10"):
        for ta, cls in pos:
            ins.append(_inst(q, _with(reg[q], window={"t_a": ta, "t_b": ta + H.W10_WIDTH}), cls, cls,
                             {"t_a": ta}))
    # diff.global: gap x position
    for g in F["diff_grid"]:
        ta = t0 + span * g["pos_pct"] // 100
        ins.append(_inst("diff.global", _with(reg["diff.global"], t1=ta, t2=ta + span * g["gap_permille"] // 1000),
                         f"gap {g['gap_permille'] / 10:g} %", f"at {g['pos_pct']} %",
                         {"edges_added": g["edges_added"], "edges_removed": g["edges_removed"]}))
    _write(out, {"path": Path(F["log"]).name, "sha256": F["log_sha256"]}, ins, notes)


def select_belief(F: dict[str, Any], out: Path, tag: str) -> None:
    import extcmp_beliefq as B
    from gnomon.core.model import OPEN_END
    p = F["beliefq_params"]
    data = H.Dataset(log=Path(F["log"]), scale=F["scale"], tt_epoch1=0, t0=F["t0"], t1=F["t1"])
    tmpl = {q.id: q.args for q in B.belief_queries(data, p)}
    Ts = F["T_points"]
    ins: list[dict[str, Any]] = []
    notes = [f"T points: the batches at quantiles 10/30/50/70/90 % of the store's correction history "
             f"({F['correction_batches']} batches that ingest no events)"]
    sets = pick_percentiles(F["seed_sets"], PCTS, 5, f"seeds{tag}")
    for c in sets:
        ins.append(_inst("b.snap.hop2@now", _with(tmpl["b.snap.hop2@now"], seeds=c["seeds"]), c["stratum"],
                         c["kind"], {"two_hop_nodes": c["value"], "set": c["id"]}))
    four = [next(c for c in sets if c["stratum"] == f"p{q:g}") for q in PCTS]
    for c in four:
        for tq, T in Ts.items():
            ins.append(_inst("b.snap.hop2@T", _with(tmpl["b.snap.hop2@T"], seeds=c["seeds"], as_of_tt=T),
                             c["stratum"], f"as-of {tq}", {"two_hop_nodes": c["value"], "set": c["id"], "T": tq}))
    rows = F["reach_size"]
    vals = [r["value"] for r in rows[:300]]
    n_nodes = max(r["value"] for r in rows) + 1
    if quantile(vals, 10) < quantile(vals, 90):
        srcs = pick_percentiles(rows, PCTS, 5, f"breach{tag}")
        four = [next(c for c in srcs if c["stratum"] == f"p{q:g}") for q in PCTS]
        notes.append("b.reach: percentiles of the reachable-set size")
    else:
        # two-valued, as reach.window: tiny (< 1 % of nodes) and full sources
        tiny = sorted({r["id"]: r for r in rows if r["value"] < n_nodes // 100}.values(), key=lambda r: md5("btiny", tag, r["id"]))[:6]
        full = sorted((r for r in rows[:300] if r["value"] >= n_nodes // 100), key=lambda r: md5("bfull", tag, r["id"]))
        srcs = [dict(r, stratum="tiny (< 1 % of nodes)") for r in tiny] + \
               [dict(r, stratum="full (>= 99 % of nodes)") for r in full[: 20 - len(tiny)]]
        four = srcs[:1] + [c for c in srcs if c["stratum"].startswith("full")][:3]
        notes.append(f"b.reach: reachable-set size two-valued (MD5 sample p10={quantile(vals, 10):.0f}, "
                     f"p90={quantile(vals, 90):.0f}): {len(tiny)} tiny + {20 - len(tiny)} full sources; @T: 1 tiny + 3 full")
    for c in srcs:
        ins.append(_inst("b.reach@now", _with(tmpl["b.reach@now"], src=c["id"]), c["stratum"], "source",
                         {"reachable": c["value"]}))
    for c in four:
        for tq, T in Ts.items():
            ins.append(_inst("b.reach@T", _with(tmpl["b.reach@T"], src=c["id"], as_of_tt=T), c["stratum"],
                             f"as-of {tq}", {"reachable": c["value"], "T": tq}))
    rows = {(r["window"], r["t1"]): r["rows"] for r in F["belief_diff"]}
    for wn, w in F["diff_windows"].items():
        for tq, T in Ts.items():
            ins.append(_inst("b.diff", _with(tmpl["b.diff"], t1=T, t2=OPEN_END, window=w), f"from {tq}",
                             f"window {wn}", {"rows": rows.get((wn, tq)), "T": tq}))
    L = p["L"]
    dw = p["diff_window"]
    for k in range(-4, 4):
        w = {"t_a": dw["t_a"] + k * L // 2, "t_b": dw["t_b"] + k * L // 2}
        ins.append(_inst("b.diff@ctl", _with(tmpl["b.diff@ctl"], t1=w["t_a"], t2=w["t_b"]), f"shift {k:+d}",
                         f"window shift {k:+d} L/2", {"window": w}))
    for vc, k in (("1", 7), ("2", 7), ("3", 6)):
        pool = F["edge_candidates"].get(vc, [])[:k]
        if len(pool) < k:
            notes.append(f"b.hist {tag}: only {len(pool)} edges with {vc} versions")
        lab = {"1": "0 corrections", "2": "1 correction", "3": "several corrections"}[vc]
        for e in pool:
            ent = {kk: e[kk] for kk in ("eid", "src", "dst", "rel_type", "disc")}
            for q in ("b.hist", "b.hist@now"):
                ins.append(_inst(q, _with(tmpl[q], **ent), lab, lab, {"versions": e["versions"]}))
    _write(out, {"path": Path(F["log"]).name, "sha256": F["log_sha256"]}, ins, notes)


# --------------------------------------------------------------------------- #
# per-instance answer-check gate                                                #
# --------------------------------------------------------------------------- #

def _records(d: Path, prefix: str) -> dict[str, list[dict[str, Any]]]:
    """{system: cells} from every `<prefix>*.json` record in d (one per family)."""
    out: dict[str, list[dict[str, Any]]] = {}
    for f in sorted(d.glob(f"{prefix}*.json")):
        rec = json.loads(f.read_text())
        if "results" not in rec or "system" not in rec:
            continue
        for c in rec["results"]:
            out.setdefault(rec["system"], []).append(dict(c, _record=f.name))
    return out


def gate(d: Path, ceiling: int, out: Path) -> int:
    import extcmp as X
    cells = _records(d, "verify-")
    nat = cells.get("native", [])
    bad_ref = [X.cell_key(c) for c in nat if not c.get("ok")]
    verdicts: dict[str, list[dict[str, Any]]] = {}
    excl: dict[str, dict[str, str]] = {}
    counts: dict[str, dict[str, int]] = {}
    for sysname, cs in sorted(cells.items()):
        if sysname == "native":
            continue
        rows = X.verdict({"results": nat}, {"system": sysname, "compare": "exact", "results": cs})
        verdicts[sysname] = rows
        walls = {X.cell_key(c): c.get("wall_ms", 0) for c in cs}
        for r in rows:
            k = r["query"] if r.get("instance") is None else f"{r['query']}#{r['instance']}"
            r["wall_ms"] = walls.get(k, 0)
            v = r["verdict"]
            det = r.get("detail", "")
            if v in ("error", "refused") and (is_server_timeout(det) or det.startswith("timeout")):
                v = f"> {ceiling:g} s at answer check"
            elif v == "equivalent" and r.get("wall_ms", 0) > ceiling * 1e3:
                v = f"> {ceiling:g} s at answer check"
            if v not in ("equivalent", "not written", "not expressible"):
                excl.setdefault(sysname, {})[k] = f"excluded: {v}"
            counts.setdefault(sysname, {}).setdefault(v, 0)
            counts[sysname][v] += 1
    (d / "verdicts.json").write_text(json.dumps(verdicts, indent=1, sort_keys=True) + "\n")
    out.write_text(json.dumps(excl, indent=1, sort_keys=True) + "\n")
    print(f"native reference: {len(nat)} cells, {len(bad_ref)} not ok {bad_ref[:20]}")
    for sysname, c in counts.items():
        print(f"{sysname}: " + ", ".join(f"{k} {v}" for k, v in sorted(c.items())))
    differs = {s: [k for k, v in e.items() if "DIFFERS" in v] for s, e in excl.items()}
    print("DIFFERS:", {s: v for s, v in differs.items() if v} or "none")
    return 1 if bad_ref or not nat else 0


# --------------------------------------------------------------------------- #
# cost estimate from the answer check (one call per instance and system)        #
# --------------------------------------------------------------------------- #

def estimate(d: Path, ceiling: float, slow_s: float, scale: float, rotations: int,
             window_overhead_s: float) -> dict[str, Any]:
    """Projected timed seconds per system and query: per instance, the answer
    check's single call x `scale` (host speed ratio), times the protocol's call
    count (2 + 10, or 1 + 3 above slow_s), zero for a cell excluded at the
    answer check (it is not timed), per rotation; plus one window overhead per
    (family, system, rotation)."""
    cells = _records(d, "verify-")
    tot: dict[str, float] = {}
    by_q: dict[tuple, float] = {}
    n_cens: dict[str, int] = {}
    fams: dict[str, set] = {}
    import extcmp as X
    fam_of = {q: f for f, qs in X.FAMILIES.items() for q in qs}
    for sysname, cs in cells.items():
        for c in cs:
            if c.get("instance") is None:
                continue
            fams.setdefault(sysname, set()).add(fam_of.get(c["query"], "?"))
            if not c.get("ok"):
                n_cens[sysname] = n_cens.get(sysname, 0) + 1
                continue
            t = c["wall_ms"] / 1e3 * scale
            if t > ceiling:
                n_cens[sysname] = n_cens.get(sysname, 0) + 1
                continue
            calls = 4 if t > slow_s else 12
            sec = t * calls * rotations
            tot[sysname] = tot.get(sysname, 0) + sec
            by_q[(sysname, c["query"])] = by_q.get((sysname, c["query"]), 0) + sec
    over = {s: len(f) * rotations * window_overhead_s for s, f in fams.items()}
    verify_s = {s: sum(min(c.get("wall_ms", ceiling * 1e3) / 1e3 * scale, ceiling) if c.get("ok") else ceiling
                       for c in cs if c.get("instance") is not None) for s, cs in cells.items()}
    out = {"timed_s": tot, "window_overhead_s": over, "verify_s": verify_s, "censored_cells": n_cens,
           "top": sorted(((round(v), s, q) for (s, q), v in by_q.items()), reverse=True)[:25],
           "total_s": sum(tot.values()) + sum(over.values()) + sum(verify_s.values())}
    return out


# --------------------------------------------------------------------------- #
# report                                                                        #
# --------------------------------------------------------------------------- #

SYSTEM_NAMES = {"native": "Gnomon", "duckdbsql": "DuckDB (SQL)", "duckdbsql_t32": "DuckDB (SQL) 32 threads",
                "clickhouse": "ClickHouse",
                "postgres": "PostgreSQL", "duckdb": "Gnomon on DuckDB storage"}


def _fmt(x: float | None) -> str:
    if x is None:
        return "—"
    if x == float("inf"):
        return "> ceiling"
    return f"{x:.3f}" if x < 1 else f"{x:.3g}" if x < 1000 else f"{x:,.0f}"


def report(d: Path, rotations: int, out: Path, extra: list[tuple[Path, str, str]] = ()) -> dict[str, Any]:
    """`extra`: (directory, system, name) — another job's cells of one system
    on the same parameter file (DuckDB (SQL) at 32 threads), added as a
    further competitor under `name`."""
    doc = json.loads((d / "params-used.json").read_text())
    insts = {(i["query"], i["instance"]): i for i in doc["instances"]}
    excl = json.loads((d / "excluded.json").read_text()) if (d / "excluded.json").exists() else {}
    per: dict[tuple, dict[str, list]] = {}       # (q, inst) -> system -> [p50 per rotation]
    status: dict[tuple, dict[str, str]] = {}
    # a cell excluded as "error" because its answer-check retry reused a
    # session that the client-alarm recovery had closed (registry job
    # the 30 s client alarm, so it is above the 10 s ceiling — censored, and
    # listed in the report
    lost_session: dict[tuple, str] = {}
    vpath = d / "verdicts.json"
    if vpath.exists():
        for sysname, rows in json.loads(vpath.read_text()).items():
            for r in rows:
                if r.get("instance") is not None and "the connection is closed" in r.get("detail", ""):
                    lost_session[(r["query"], r["instance"], sysname)] = r["detail"]
    for src_dir, only, rename in [(d, None, None)] + list(extra):
      if src_dir != d:
        other = json.loads((src_dir / "params-used.json").read_text())
        if other["instances"] != doc["instances"]:
            raise SystemExit(f"{src_dir}: a different parameter file")
      for r in range(1, rotations + 1):
        rd = src_dir / f"rot{r}"
        if not rd.is_dir():
            continue
        for sysname, cs in _records(rd, "").items():
            if only is not None:
                if sysname != only:
                    continue
                sysname = rename
            for c in cs:
                if c.get("instance") is None:
                    continue
                k = (c["query"], c["instance"])
                lst = per.setdefault(k, {}).setdefault(sysname, [None] * rotations)
                if c.get("ok"):
                    lst[r - 1] = c["p50_ms"]
                elif c.get("aborted") or "> " in str(c.get("error", "")) and "answer check" in str(c.get("error", "")):
                    lst[r - 1] = float("inf")
                elif src_dir == d and (c["query"], c["instance"], sysname) in lost_session:
                    lst[r - 1] = float("inf")
                else:
                    status.setdefault(k, {})[sysname] = str(c.get("error", ""))[:80]
    order = ["native", "duckdbsql", "clickhouse", "postgres", "duckdb"] + [e[2] for e in extra]
    systems = [s for s in order if any(s in v for v in per.values())]
    rot_agree: dict[str, list[int]] = {}
    val: dict[tuple, dict[str, float]] = {}
    for k, m in per.items():
        for s, lst in m.items():
            xs = [x for x in lst if x is not None]
            if len(xs) == rotations and all(x != float("inf") for x in xs) and rotations >= 2:
                ok = 0.9 <= xs[1] / xs[0] <= 1.1 if xs[0] > 0 else True
                rot_agree.setdefault(s, [0, 0])
                rot_agree[s][0] += ok
                rot_agree[s][1] += 1
            if xs:
                val.setdefault(k, {})[s] = (float("inf") if any(x == float("inf") for x in xs)
                                            else statistics.mean(xs))
    queries = sorted({q for q, _ in val}, key=lambda q: list(dict.fromkeys(i["query"] for i in doc["instances"])).index(q))
    res: dict[str, Any] = {"point": d.name, "params": doc.get("version"), "queries": {},
                           "rotation_agreement": {s: f"{a}/{n}" for s, (a, n) in rot_agree.items()}}
    lines = [f"# P1 sensitivity — {d.name} ({doc.get('version')})", "",
             "Per instance: mean over rotations of the per-rotation median (ms). Per query: median over",
             "instances, p10–p90 over instances; win = Gnomon fastest among the systems with a valid cell;",
             "ratio = best competitor / Gnomon per instance (> 1: Gnomon faster).", ""]
    for q in queries:
        ks = [k for k in val if k[0] == q]
        row: dict[str, Any] = {"n_instances": len(ks), "systems": {}}
        for s in systems:
            xs = [val[k][s] for k in ks if s in val[k]]
            if xs:
                row["systems"][s] = {"median": quantile(xs, 50), "p10": quantile(xs, 10),
                                     "p90": quantile(xs, 90), "n": len(xs),
                                     "n_censored": sum(1 for x in xs if x == float("inf"))}
        wins, ratios, losses = 0, [], []
        for k in ks:
            v = val[k]
            if "native" not in v:
                continue
            comp = {s: x for s, x in v.items() if s != "native"}
            if not comp:
                continue
            best_s = min(comp, key=comp.get)
            ratio = comp[best_s] / v["native"] if v["native"] > 0 else float("inf")
            ratios.append((ratio, k, best_s))
            if v["native"] < comp[best_s]:
                wins += 1
            else:
                losses.append({"instance": k[1], "stratum": insts[k].get("stratum"),
                               "class": insts[k].get("class"), "winner": best_s,
                               "ratio": ratio, "native_ms": v["native"], "winner_ms": comp[best_s]})
        rs = [r for r, _, _ in ratios]
        worst = min(ratios, default=None)
        row["win_rate"] = f"{wins}/{len(ratios)}"
        row["win_share"] = wins / len(ratios) if ratios else None
        row["ratio_median"] = quantile(rs, 50) if rs else None
        row["ratio_p10"] = quantile(rs, 10) if rs else None
        row["ratio_p90"] = quantile(rs, 90) if rs else None
        row["losses"] = losses
        if worst:
            wk = worst[1]
            row["worst"] = {"instance": wk[1], "stratum": insts[wk].get("stratum"), "class": insts[wk].get("class"),
                            "ratio": worst[0], "best_competitor": worst[2],
                            "native_ms": val[wk]["native"], "args": insts[wk]["args"]}
        strata: dict[str, dict[str, float]] = {}
        for st in dict.fromkeys(insts[k].get("stratum") for k in ks):
            sk = [k for k in ks if insts[k].get("stratum") == st]
            strata[str(st)] = {s: quantile([val[k][s] for k in sk if s in val[k]], 50)
                               for s in systems if any(s in val[k] for k in sk)}
        row["strata"] = strata
        res["queries"][q] = row
        lines += [f"## {q} — {len(ks)} instances", "",
                  "| system | median ms | p10–p90 ms | censored |", "|---|---:|---:|---:|"]
        for s, x in row["systems"].items():
            lines.append(f"| {SYSTEM_NAMES.get(s, s)} | {_fmt(x['median'])} | {_fmt(x['p10'])}–{_fmt(x['p90'])} | {x['n_censored']} |")
        lines += ["", f"Gnomon win rate {row['win_rate']}; best competitor / Gnomon median "
                  f"{_fmt(row['ratio_median'])} (p10–p90 {_fmt(row['ratio_p10'])}–{_fmt(row['ratio_p90'])})."]
        if worst:
            w = row["worst"]
            lines.append(f"Worst instance: {w['instance']} ({w['stratum']}, {w['class']}): ratio {_fmt(w['ratio'])} "
                         f"vs {SYSTEM_NAMES.get(w['best_competitor'], w['best_competitor'])}, Gnomon {_fmt(w['native_ms'])} ms.")
        if losses:
            lines.append("Losses: " + "; ".join(f"{x['instance']} ({x['stratum']}, {x['class']}) to "
                                                  f"{SYSTEM_NAMES.get(x['winner'], x['winner'])} ratio {_fmt(x['ratio'])}"
                                                  for x in losses))
        lines += ["", "| stratum | " + " | ".join(SYSTEM_NAMES.get(s, s) for s in systems) + " |",
                  "|---|" + "---:|" * len(systems)]
        for st, m in strata.items():
            lines.append(f"| {st} | " + " | ".join(_fmt(m.get(s)) for s in systems) + " |")
        lines.append("")
    lines += ["## Rotation agreement (rot2 / rot1 within ±10 %)", ""]
    lines += [f"- {SYSTEM_NAMES.get(s, s)}: {v}" for s, v in res["rotation_agreement"].items()]
    if lost_session:
        lines += ["", f"Censored after a lost session at the answer check (first call past the 30 s client "
                  f"alarm; the retry reused the closed session): {sorted(f'{q}#{i} ({s})' for q, i, s in lost_session)}"]
    excl_n = {s: len(v) for s, v in excl.items()}
    lines += ["", f"Excluded at the answer check (per system, instance cells): {excl_n or 'none'}"]
    if status:
        lines += ["", f"Cells with other errors: {sum(len(v) for v in status.values())}"]
    out.with_suffix(".json").write_text(json.dumps(res, indent=1, default=str) + "\n")
    print("\n".join(lines))
    return res


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("factors")
    p.add_argument("--kind", choices=("registry", "belief"), required=True)
    p.add_argument("--log", type=Path, required=True)
    p.add_argument("--store", type=Path, required=True)
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--n-snap", type=int, default=2000)
    p.add_argument("--n-reach", type=int, default=300)
    p.add_argument("--n-paths", type=int, default=150)
    p = sub.add_parser("select")
    p.add_argument("--kind", choices=("registry", "belief"), required=True)
    p.add_argument("--factors", type=Path, required=True)
    p.add_argument("--factors-d0", type=Path, help="registry: the d0 belief factors (as-of points)")
    p.add_argument("--tag", default="")
    p.add_argument("--reach-k", type=int, default=4)
    p.add_argument("--out", type=Path, required=True)
    p = sub.add_parser("estimate")
    p.add_argument("--dir", type=Path, required=True)
    p.add_argument("--ceiling", type=float, default=10)
    p.add_argument("--slow-s", type=float, default=1.0)
    p.add_argument("--scale", type=float, default=1.0)
    p.add_argument("--rotations", type=int, default=2)
    p.add_argument("--window-overhead-s", type=float, default=30)
    p = sub.add_parser("gate")
    p.add_argument("--dir", type=Path, required=True)
    p.add_argument("--ceiling", type=float, required=True)
    p.add_argument("--out", type=Path, required=True)
    p = sub.add_parser("report")
    p.add_argument("--dir", type=Path, required=True)
    p.add_argument("--rotations", type=int, default=2)
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--extra", action="append", default=[],
                   help="DIR:SYSTEM:NAME, another job's cells of SYSTEM as competitor NAME")
    args = ap.parse_args()
    if args.cmd == "select":
        F = json.loads(args.factors.read_text())
        if args.kind == "registry":
            select_registry(F, json.loads(args.factors_d0.read_text()), args.out, args.reach_k)
        else:
            select_belief(F, args.out, args.tag)
        return 0
    if args.cmd == "estimate":
        print(json.dumps(estimate(args.dir, args.ceiling, args.slow_s, args.scale, args.rotations,
                                  args.window_overhead_s), indent=1))
        return 0
    if args.cmd == "gate":
        return gate(args.dir, args.ceiling, args.out)
    if args.cmd == "report":
        report(args.dir, args.rotations, args.out,
               [(Path(e.split(":")[0]), e.split(":")[1], e.split(":")[2]) for e in args.extra])
        return 0
    if args.cmd == "factors":
        if args.kind == "registry":
            factors_registry(args.log, args.store, args.out, args.n_snap, args.n_reach, args.n_paths)
        else:
            factors_belief(args.log, args.store, args.out, args.n_reach)
    return 0


if __name__ == "__main__":
    sys.exit(main())
