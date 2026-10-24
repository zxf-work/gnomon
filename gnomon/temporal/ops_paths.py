"""Time-respecting path operators: temporal_reachability, temporal_paths.

Semantics (shared by engine and oracle — the operational definition):

An edge version e = (u -> v, [vt_s, vt_e)) is *traversable* from u at arrival
time a iff tau = max(a, vt_s) satisfies tau < vt_e and tau < t_b; the arrival
at v is tau. Consecutive edges therefore have non-decreasing traversal times
(t_{i+1} >= t_i). Instantaneous events (vt_e = vt_s + 1) reduce to the classic
Wu et al. (2014) earliest-arrival semantics.

`delta_max_wait`: along a path, the wait tau_i - tau_{i-1} at every
intermediate node must be <= delta (no constraint at the source). Semantics
are path-based and exact: a node is reachable iff some time-respecting path
satisfies all wait constraints; earliest_arrival is the minimum over such
paths. Without delta, prefix-optimality holds and a single-label vectorized
fixpoint is exact; with delta, smaller arrivals can *disable* later edges, so
the kernel switches to an exact multi-label search over (node, arrival)
states (a candidate for a Rust kernel).
"""

from __future__ import annotations

from typing import Any

import numpy as np

from gnomon.core.errors import CostError, InvalidArgError, LimitError
from gnomon.core.model import OPEN_END
from gnomon.storage.base import StorageAdapter
from gnomon.temporal.algebra import (
    AS_OF_TT,
    CURSOR,
    LIMIT,
    UID,
    WINDOW,
    check_window,
    operator,
    paginate,
    required,
)
from gnomon.temporal.guardrails import ROUTES_KEY, coeff_scale, started_by, window_fraction

INF = OPEN_END
MAX_PATH_HOPS = 6
MAX_K = 20
MAX_EXPANSIONS = 500_000
#: The engine's `temporal_paths` search examines only edges that can
#: still lie on an answer -- prefixes past their node's latest start are cut,
#: last edges are counted by binary search, never walked -- a subset of the
#: edges the enumeration below examines. It gets a larger budget: every query
#: the enumeration answers, it answers too, and the hub queries the
#: enumeration's budget refused it answers as well.
KERNEL_MAX_EXPANSIONS = 64 * MAX_EXPANSIONS


#: The engine's earliest-arrival search (`_engine_reach`): one planned
#: scan of the window's believed edge versions into a by-source CSR, then a
#: label-setting search. Per edge version the scan may return, measured on the
#: five calibration stores; `both` reads and searches twice.
REACH_KERNEL_MS_PER_M = 60.0
REACH_CALL_MS = 2.0


def _reach_cost(args: dict[str, Any], stats: dict[str, Any]) -> dict[str, int]:
    """Admission estimate of one earliest-arrival search from one source.

    With the engine's search (single label: no `delta_max_wait`), the work is
    the scan of the window's edge versions and a search over them: the
    versions that may overlap the window are those started by its end (a
    long-lived version overlaps every later window, so the window's share of
    the extent undercounts it), priced per version at the engine's measured
    rate. The old price -- the window's share, nine units each at the
    170 ms/M columnar rate, the NumPy fixpoint's bound -- was 20 to 60 times
    the engine's time and refused whole-span searches that run in 0.3 to
    0.7 s. The NumPy fixpoint and the multi-label search under a wait
    cap keep that price, and so does any caller that does not say which
    route the call takes (`ROUTES_KEY`)."""
    routes = stats.get(ROUTES_KEY) or {}
    if routes.get("reach_kernel") and args.get("delta_max_wait") is None:
        rows = int(stats.get("n_edge_versions", 0) * started_by(args["window"]["t_b"], stats))
        both = 2.0 if args.get("direction", args.get("dir")) == "both" else 1.0
        ms = (REACH_CALL_MS + rows * both * REACH_KERNEL_MS_PER_M / 1e6) * coeff_scale()
        return {"rows_scanned_est": rows, "expansions_est": 0, "time_est_ms": int(ms)}
    rows = int(stats.get("n_edge_versions", 0) * window_fraction(args, stats))
    # fixpoint rounds are bounded by path length; budget a small multiple
    return {"rows_scanned_est": rows, "expansions_est": rows * 8}


def _paths_cost(args: dict[str, Any], stats: dict[str, Any]) -> dict[str, int]:
    """Charge the branching-bounded DFS frontier, not the windowed scan.

    temporal_paths explores from a single source, node-simply, to at most
    `max_hops` levels, so its work is the frontier sum
    b + b^2 + ... + b^max_hops with branching b = the mean windowed
    out-degree — NOT a multiple of the windowed event count. The old
    `rows * 8` form charged the reachability fixpoint's shape (every
    windowed edge relaxed per round) and refused a query at 10M events
    that a SQL baseline answered in 37 ms:
    a quarter-span window priced 20M "expansions" where a 3-hop frontier
    over mean degree 25 explores ~16k.

    Mean degree, not max — the same lesson as the motif estimate: one heavy sender
    does not sit on every hop of every path, and max-degree skew is
    exactly what produced that operator's false positive. The estimate
    stays an over-count of live traversal because time-respecting
    pruning (tau < vt_e, tau < t_b) and node-simplicity cut far below
    the full b^h product; the runtime MAX_EXPANSIONS budget remains the
    exact backstop for stores whose local structure beats the average.
    """
    n_ev = int(stats.get("n_edge_versions", 0))
    wf = window_fraction(args, stats)
    e_w = int(n_ev * wf)
    branching = max(1.0, e_w / max(1, int(stats.get("n_entities", 1))))
    expansions, frontier = 0.0, 1.0
    for _ in range(int(args.get("max_hops") or MAX_PATH_HOPS)):
        frontier *= branching
        expansions += frontier
        if expansions >= 2**40:
            break
    return {"rows_scanned_est": e_w,
            "expansions_est": min(int(expansions), 2**40)}


@operator(
    "temporal_reachability",
    {
        "src": required(UID),
        "window": required(WINDOW),
        "as_of_tt": AS_OF_TT,
        "delta_max_wait": {"type": ["integer", "null"], "minimum": 1, "default": None},
        "direction": {"type": "string", "enum": ["out", "in", "both"], "default": "out"},
        "limit": LIMIT,
        "cursor": CURSOR,
    },
    "Earliest-arrival time per node reachable from `src` via time-respecting "
    "paths inside `window` (source excluded). direction='in' answers "
    "\"who could reach src\".",
    cost_fn=_reach_cost,
    validators=[check_window],
)
def temporal_reachability(adapter: StorageAdapter, args: dict[str, Any]) -> dict[str, Any]:
    t_a, t_b = args["window"]["t_a"], args["window"]["t_b"]
    as_of = args["as_of_tt"]
    delta = args["delta_max_wait"]
    sid = int(adapter.dense_ids([args["src"]])[0])

    if delta is None:
        page = _engine_reach(adapter, sid, t_a, t_b, as_of, args)
        if page is not None:
            return page

    e = adapter.edges_columnar(as_of_tt=as_of, vt_min=t_a, vt_max=t_b,
                               columns=("src_id", "dst_id", "vt_s", "vt_e"))
    src, dst, vt_s, vt_e = e["src_id"], e["dst_id"], e["vt_s"], e["vt_e"]
    if args["direction"] == "in":
        src, dst = dst, src
    elif args["direction"] == "both":
        src, dst = np.concatenate([src, dst]), np.concatenate([dst, src])
        vt_s, vt_e = np.concatenate([vt_s, vt_s]), np.concatenate([vt_e, vt_e])

    n = adapter.num_entities()
    if delta is None:
        arr = np.full(n, INF, dtype=np.int64)
        arr[sid] = t_a
        # vectorized label-correcting fixpoint: each round relaxes every edge
        # once; exact because smaller arrivals are always at least as good
        while True:
            a_u = arr[src]
            tau = np.maximum(a_u, vt_s)
            ok = (tau < vt_e) & (tau < t_b) & (a_u < INF)
            new = arr.copy()
            np.minimum.at(new, dst[ok], tau[ok])
            if np.array_equal(new, arr):
                break
            arr = new
    else:
        arr = _multilabel_arrivals(n, sid, t_a, t_b, delta, src, dst, vt_s, vt_e)

    reached = np.flatnonzero((arr < INF) & (np.arange(n) != sid))
    uids = adapter.uids_for(reached)
    rows = [{"uid": u, "earliest_arrival": int(a)}
            for u, a in zip(uids, arr[reached])]
    rows.sort(key=lambda r: (r["earliest_arrival"], r["uid"]))
    return paginate(rows, args["limit"], args["cursor"])


def _reach_kernel(adapter: StorageAdapter) -> bool:
    """Does this adapter carry the engine's earliest-arrival search?

    Only the native engine, and only with `GNOMON_REACH_KERNEL` not set to
    `python` -- read when the handle opens and reported by `designs()`, like
    every other design switch. The single-label case only: under
    `delta_max_wait` a smaller arrival can disable a later edge, so the
    multi-label search below stays the one implementation of that case.
    """
    designs = getattr(adapter, "designs", None)
    if designs is None or not hasattr(adapter, "reach_earliest"):
        return False
    return bool(designs().get("reach_kernel", False))


def _engine_reach(adapter: StorageAdapter, sid: int, t_a: int, t_b: int,
                  as_of: int, args: dict[str, Any]) -> dict[str, Any] | None:
    """The kernel's answer in `paginate`'s exact shape, or `None` to run the
    NumPy path. The engine orders the reached nodes by `(arrival, uid)` --
    uid by code point, which is UTF-8 byte order -- selects the requested
    page and sorts only that, so rows (and uid strings) are built for the
    page alone; the total comes back beside it."""
    if not _reach_kernel(adapter):
        return None
    cursor, limit = args["cursor"], args["limit"]
    try:
        offset = int(cursor) if cursor else 0
    except ValueError:
        raise InvalidArgError(f"bad cursor: {cursor!r}") from None
    if offset < 0:
        return None     # Python slice semantics for a negative offset: leave them to paginate
    got = adapter.reach_earliest(sid, t_a, t_b, as_of, args["direction"], offset, limit)
    if got is None:
        return None
    rows = [{"uid": u, "earliest_arrival": int(a)}
            for u, a in zip(got["uids"], got["arrival"])]
    total = int(got["total"])
    truncated = offset + len(rows) < total
    return {
        "rows": rows,
        "rows_total": total,
        "truncated": truncated,
        "cursor": str(offset + len(rows)) if truncated else None,
    }


def _multilabel_arrivals(n: int, sid: int, t_a: int, t_b: int, delta: int,
                         src: np.ndarray, dst: np.ndarray,
                         vt_s: np.ndarray, vt_e: np.ndarray) -> np.ndarray:
    """Exact earliest arrivals under a wait cap: BFS over (node, arrival)
    states. All distinct arrival labels per node are kept — under delta,
    dominance pruning is unsound (a smaller arrival can disable an edge)."""
    from collections import deque

    order = np.argsort(src, kind="stable")
    s_sorted = src[order]
    starts = np.searchsorted(s_sorted, np.arange(n), side="left")
    stops = np.searchsorted(s_sorted, np.arange(n), side="right")

    labels: list[set[int]] = [set() for _ in range(n)]
    labels[sid].add(t_a)
    queue = deque([(sid, t_a)])
    expansions = 0
    while queue:
        u, a = queue.popleft()
        for k in range(int(starts[u]), int(stops[u])):
            expansions += 1
            if expansions > MAX_EXPANSIONS:
                raise CostError("temporal_reachability expansion budget exceeded",
                                estimate={"expansions_est": expansions},
                                suggestions=["narrow the window",
                                             "drop or loosen delta_max_wait"])
            i = int(order[k])
            tau = max(a, int(vt_s[i]))
            if tau >= int(vt_e[i]) or tau >= t_b:
                continue
            # the wait cap is waived only for the source *start* label
            if not (u == sid and a == t_a) and tau - a > delta:
                continue
            v = int(dst[i])
            if tau not in labels[v]:
                labels[v].add(tau)
                queue.append((v, tau))
    arr = np.full(n, INF, dtype=np.int64)
    for v in range(n):
        if labels[v]:
            arr[v] = min(labels[v])
    arr[sid] = t_a
    return arr


def _paths_validators(args: dict[str, Any]) -> None:
    check_window(args)
    if args["max_hops"] > MAX_PATH_HOPS:
        raise LimitError(f"max_hops capped at {MAX_PATH_HOPS}")
    if args["k"] > MAX_K:
        raise LimitError(f"k capped at {MAX_K}")


@operator(
    "temporal_paths",
    {
        "src": required(UID),
        "dst": required(UID),
        "window": required(WINDOW),
        "k": {"type": "integer", "minimum": 1, "maximum": MAX_K, "default": 5},
        "max_hops": {"type": "integer", "minimum": 1, "maximum": MAX_PATH_HOPS,
                     "default": 4},
        "as_of_tt": AS_OF_TT,
    },
    "Up to k node-simple time-respecting paths src -> dst inside `window`, "
    "ordered by (arrival, hops, edge sequence).",
    cost_fn=_paths_cost,
    validators=[_paths_validators],
)
def temporal_paths(adapter: StorageAdapter, args: dict[str, Any]) -> dict[str, Any]:
    t_a, t_b = args["window"]["t_a"], args["window"]["t_b"]
    as_of = args["as_of_tt"]
    sid, did = (int(x) for x in adapter.dense_ids([args["src"], args["dst"]]))

    page = _engine_paths(adapter, sid, did, t_a, t_b, as_of, args)
    if page is not None:
        return page

    csr, cols = _csr_for(adapter, as_of, t_a, t_b)
    # traversal over the TCSR: per-node slices ordered by (vt_s, vid); the
    # deterministic enumeration order is fixed after the walk, by (vt_s, eid)
    found: list[tuple[int, int, list[int]]] = []
    expansions = 0

    def dfs(node: int, arrival: int, hops: int, visited: set[int], trail: list[int]) -> None:
        nonlocal expansions
        if node == did and trail:
            found.append((arrival, hops, list(trail)))
            return  # node-simple: dst terminates the path
        if hops == args["max_hops"]:
            return
        nbr, vt_s, vt_e, row = csr.neighbors(node, "out", t_max=t_b)
        for j in range(len(nbr)):
            expansions += 1
            if expansions > MAX_EXPANSIONS:
                raise CostError("temporal_paths expansion budget exceeded",
                                estimate={"expansions_est": expansions},
                                suggestions=["narrow the window",
                                             "reduce max_hops"])
            v = int(nbr[j])
            if v in visited:
                continue
            tau = max(arrival, int(vt_s[j]))
            if tau >= int(vt_e[j]) or tau >= t_b:
                continue
            visited.add(v)
            trail.append(int(row[j]))
            dfs(v, tau, hops + 1, visited, trail)
            trail.pop()
            visited.remove(v)

    dfs(sid, t_a, 0, {sid}, [])
    # Identity fetch. Portable backends carry eid/rel_type in the scan; the
    # native TCSR path deliberately does not (a sha256 per scanned row), so
    # its scan returns row addresses instead and the engine derives the two
    # fields for exactly the rows on found paths.
    if "eid" in cols:
        eid_of, rel_of = cols["eid"], cols["rel_type"]
    else:
        need = sorted({r for _, _, trail in found for r in trail})
        eids, rels = (adapter.edge_idents_at(cols["seg_id"][need],
                                             cols["seg_row"][need])
                      if need else ((), ()))
        eid_of = dict(zip(need, eids))
        rel_of = dict(zip(need, rels))
    paths = [(arrival, hops,
              tuple((int(cols["vt_s"][r]), eid_of[r]) for r in trail), trail)
             for arrival, hops, trail in found]
    paths.sort(key=lambda p: (p[0], p[1], p[2]))
    rows = []
    for arrival, hops, _, trail in paths[: args["k"]]:
        s_uids = adapter.uids_for([int(cols["src_id"][r]) for r in trail])
        d_uids = adapter.uids_for([int(cols["dst_id"][r]) for r in trail])
        rows.append({
            "arrival": arrival,
            "hops": hops,
            "edges": [{"src": s, "dst": d, "rel_type": rel_of[r],
                       "eid": eid_of[r], "t": int(cols["vt_s"][r])}
                      for r, s, d in zip(trail, s_uids, d_uids)],
        })
    return {"rows": rows, "rows_total": len(paths),
            "truncated": len(paths) > args["k"], "cursor": None}


def _paths_kernel(adapter: StorageAdapter) -> bool:
    """Does this adapter carry the engine's paths search? Only the
    native engine, and only with `GNOMON_PATHS_KERNEL` not set to `python` --
    read when the handle opens and reported by `designs()`."""
    designs = getattr(adapter, "designs", None)
    if designs is None or not hasattr(adapter, "paths_topk"):
        return False
    return bool(designs().get("paths_kernel", False))


def _engine_paths(adapter: StorageAdapter, sid: int, did: int, t_a: int, t_b: int,
                  as_of: int, args: dict[str, Any]) -> dict[str, Any] | None:
    """The engine's answer in the enumeration's exact shape, or `None` to run
    the enumeration. Strings are built for the `k` returned paths only."""
    if not _paths_kernel(adapter):
        return None
    got = adapter.paths_topk(sid, did, t_a, t_b, as_of, args["k"], args["max_hops"],
                             KERNEL_MAX_EXPANSIONS)
    if got is None:
        return None
    if got["exceeded"]:
        raise CostError("temporal_paths expansion budget exceeded",
                        estimate={"expansions_est": int(got["expansions"])},
                        suggestions=["narrow the window", "reduce max_hops"])
    ids = sorted(set(got["src_id"]) | set(got["dst_id"]))
    uid = dict(zip(ids, adapter.uids_for(ids))) if ids else {}
    rows, at = [], 0
    for arrival, hops in zip(got["arrival"], got["hops"]):
        rows.append({
            "arrival": int(arrival),
            "hops": int(hops),
            "edges": [{"src": uid[got["src_id"][i]], "dst": uid[got["dst_id"][i]],
                       "rel_type": got["rel_type"][i], "eid": got["eid"][i],
                       "t": int(got["vt_s"][i])}
                      for i in range(at, at + int(hops))],
        })
        at += int(hops)
    total = int(got["total"])
    return {"rows": rows, "rows_total": total, "truncated": total > args["k"],
            "cursor": None}


def _csr_for(adapter: StorageAdapter, as_of: int, t_a: int, t_b: int):
    """Cached current-belief TCSR when possible; windowed per-call build
    otherwise. Traversal constraints (tau < vt_e, tau < t_b, tau >= t_a via
    the source arrival) make the unwindowed index return identical results."""
    from gnomon.core.model import clamp_tt
    from gnomon.storage.tcsr import TemporalCSR

    if clamp_tt(as_of) == clamp_tt(OPEN_END):
        return adapter.tcsr()
    cols = adapter.edges_columnar(as_of_tt=as_of, vt_min=t_a, vt_max=t_b,
                                  columns=adapter.TCSR_COLS)
    return TemporalCSR.build(cols, adapter.num_entities()), cols
