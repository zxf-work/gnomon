"""Brute-force reference implementations.

Correct-by-inspection Python over full version lists; O(n!)-ish acceptable —
used only on graphs <= 5k edge events. Every function takes the same filled
args dict as the engine operator and returns the same payload structure, so
property tests compare canonical JSON directly.

Python loops are explicitly allowed here (spec 7.1).
"""

from __future__ import annotations

import math
from collections import deque
from datetime import datetime, timedelta, timezone
from typing import Any

from gnomon.core.errors import InvalidArgError
from gnomon.core.model import (
    OPEN_END, EdgeVersion, NodeVersion, canonical_json, clamp_tt, edge_eid,
)
from gnomon.temporal.algebra import paginate
from gnomon.temporal.ops_compute import _mean as blessed_mean
from gnomon.temporal.props import SKIP, matches, numeric_value, prop_keys, sum_exact

#: The sequence aggregates. Spelled out here rather than imported from
#: `ops_aggregate` on purpose — the blessed `mean` is shared because the
#: float sequence has to be bit-identical, but a *set of names* is exactly
#: the kind of thing ground truth should restate independently.
SEQ_AGGS = ("max_gap", "max_in_window", "max_session_span")

#: The calendar units, computed here with `datetime` while the engine
#: computes them with integer civil arithmetic. That the two agree is the
#: evidence; sharing an implementation would make the test vacuous.
DAY_NAMES = ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday",
             "Saturday", "Sunday")
MONTH_NAMES = ("January", "February", "March", "April", "May", "June",
               "July", "August", "September", "October", "November",
               "December")


def calendar_unit(t: int, unit: str, offset_minutes: int) -> Any:
    """The calendar unit of one epoch-microsecond instant, at a fixed offset.

    A fixed offset and not a timezone. `datetime` here, and only
    UTC plus explicit arithmetic, so nothing consults a tz database.
    """
    local = datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(
        microseconds=int(t) + offset_minutes * 60_000_000)
    if unit == "hour_of_day":
        return local.hour
    if unit == "day_of_week":
        return DAY_NAMES[local.weekday()]
    if unit == "month_of_year":
        return MONTH_NAMES[local.month - 1]
    raise ValueError(f"unknown calendar unit {unit!r}")


class Oracle:
    def __init__(self, node_versions: list[NodeVersion],
                 edge_versions: list[EdgeVersion]) -> None:
        self.nv = list(node_versions)
        self.ev = list(edge_versions)

    # ------------------------------------------------------------------ #
    # primitive filters                                                   #
    # ------------------------------------------------------------------ #

    def believed_nodes(self, as_of: int) -> list[NodeVersion]:
        return [v for v in self.nv if v.believed_at(as_of)]

    def believed_edges(self, as_of: int) -> list[EdgeVersion]:
        return [v for v in self.ev if v.believed_at(as_of)]

    def nodes_at(self, t: int, as_of: int) -> dict[str, NodeVersion]:
        return {v.uid: v for v in self.believed_nodes(as_of) if v.valid_at(t)}

    def edges_at(self, t: int, as_of: int,
                 rel_types: list[str] | None = None) -> dict[str, EdgeVersion]:
        return {v.eid: v for v in self.believed_edges(as_of)
                if v.valid_at(t) and (rel_types is None or v.rel_type in rel_types)}

    def events_in(self, t_a: int, t_b: int, as_of: int) -> list[EdgeVersion]:
        """Edge versions as events at vt_s, ordered by the (t, eid) total order."""
        evs = [v for v in self.believed_edges(as_of) if t_a <= v.vt_s < t_b]
        return sorted(evs, key=lambda v: (v.vt_s, v.eid))

    @staticmethod
    def _edge_row(v: EdgeVersion) -> dict[str, Any]:
        return {"eid": v.eid, "vid": v.vid, "src": v.src, "dst": v.dst,
                "rel_type": v.rel_type, "vt_s": v.vt_s, "vt_e": v.vt_e}

    def _bfs(self, edges: dict[str, EdgeVersion], seeds: list[str],
             valid_uids: set[str], hops: int) -> dict[str, int]:
        dist = {u: 0 for u in seeds if u in valid_uids}
        frontier = sorted(dist)
        for h in range(1, hops + 1):
            nxt = set()
            for v in edges.values():
                if v.src in frontier and v.dst not in dist and v.dst in valid_uids:
                    nxt.add(v.dst)
                if v.dst in frontier and v.src not in dist and v.src in valid_uids:
                    nxt.add(v.src)
            for u in nxt:
                dist[u] = h
            frontier = sorted(nxt)
        return dist

    # ------------------------------------------------------------------ #
    # version_history                                                  #
    # ------------------------------------------------------------------ #

    def version_history(self, args: dict[str, Any]) -> dict[str, Any]:
        as_of = clamp_tt(args["as_of_tt"])
        t_a, t_b = args["window"]["t_a"], args["window"]["t_b"]
        belief, kind = args["belief"], args["kind"]
        versions = self.nv if kind == "node" else self.ev
        ref = args.get("ref")
        if ref is not None:
            # engine-level: one identity's log, named by uid, eid or the
            # edge's (src, dst, rel_type, disc)
            if kind == "node":
                versions = [v for v in versions if v.uid == ref["uid"]]
            else:
                key = ref.get("eid") or edge_eid(ref["src"], ref["dst"],
                                                 ref["rel_type"], ref.get("disc", ""))
                versions = [v for v in versions if v.eid == key]

        rows = []
        for v in versions:
            # nothing written after `as_of` exists yet, in any belief mode:
            # that is what makes a pinned result immutable under later writes
            if v.tt_s > as_of:
                continue
            superseded = v.tt_e <= as_of
            if belief == "current" and superseded:
                continue
            if belief == "superseded" and not superseded:
                continue
            if not (v.vt_s < t_b and t_a < v.vt_e):   # interval overlap
                continue
            if kind == "edge" and args["rel_types"] is not None and \
                    v.rel_type not in args["rel_types"]:
                continue
            r = v.to_json()
            props = r["props"]
            del r["props"], r["source"], r["provenance_ref"]
            # a belief that ends after `as_of` has not ended yet, as far as
            # `as_of` can know — the same censoring entity_history applies
            if not superseded:
                r["tt_e"] = OPEN_END
            if args.get("include_props"):
                r["props"] = props
            rows.append(r)

        rows.sort(key=lambda r: (r["tt_s"], r["vid"]))
        return paginate(rows, args["limit"], args["cursor"])

    # ------------------------------------------------------------------ #
    # entity_history                                                    #
    # ------------------------------------------------------------------ #

    def entity_history(self, args: dict[str, Any]) -> dict[str, Any]:
        as_of = args["as_of_tt"]
        versions = sorted((v for v in self.believed_nodes(as_of) if v.uid == args["uid"]),
                          key=lambda v: (v.vt_s, v.vid))
        # engine-level: the versions valid at `t_valid`, or overlapping
        # `window`, out of the same history
        lo = hi = None
        if args.get("t_valid") is not None:
            lo, hi = args["t_valid"], args["t_valid"] + 1
        elif args.get("window") is not None:
            lo, hi = args["window"]["t_a"], args["window"]["t_b"]
        if lo is not None:
            versions = [v for v in versions if v.vt_s < hi and lo < v.vt_e]
        rows = [v.to_json() for v in versions]
        for r in rows:  # censor post-as_of belief closure, like the engine
            if r["tt_e"] > as_of:
                r["tt_e"] = OPEN_END
        out = paginate(rows, args["limit"], args["cursor"])
        if args["include_edges"]:
            inc = sorted((v for v in self.believed_edges(as_of)
                          if args["uid"] in (v.src, v.dst)
                          and (lo is None or (v.vt_s < hi and lo < v.vt_e))),
                         key=lambda v: (v.vt_s, v.vid))
            out["edges"] = [self._edge_row(v) for v in inc[: args["limit"]]]
            out["edges_truncated"] = len(inc) > args["limit"]
        return out

    # ------------------------------------------------------------------ #
    # snapshot_subgraph                                                 #
    # ------------------------------------------------------------------ #

    def snapshot_subgraph(self, args: dict[str, Any]) -> dict[str, Any]:
        t, as_of = args["t_valid"], args["as_of_tt"]
        edges = self.edges_at(t, as_of, args["rel_types"])
        nodes = self.nodes_at(t, as_of)
        dist = self._bfs(edges, args["seeds"], set(nodes), args["hops"])
        induced = sorted((v for v in edges.values()
                          if v.src in dist and v.dst in dist),
                         key=lambda v: (v.vt_s, v.vid))
        node_rows = sorted(({"uid": u, "label": nodes[u].label, "hop": h}
                            for u, h in dist.items()),
                           key=lambda r: (r["hop"], r["uid"]))
        out = paginate([self._edge_row(v) for v in induced],
                       args["limit"], args["cursor"])
        out["nodes"] = node_rows[: args["limit"]]
        out["nodes_total"] = len(node_rows)
        out["nodes_truncated"] = len(node_rows) > args["limit"]
        out["truncated"] = out["truncated"] or out["nodes_truncated"]
        return out

    # ------------------------------------------------------------------ #
    # diff_snapshots                                                    #
    # ------------------------------------------------------------------ #

    def diff_snapshots(self, args: dict[str, Any]) -> dict[str, Any]:
        t1, t2, as_of, limit = args["t1"], args["t2"], args["as_of_tt"], args["limit"]
        n1, n2 = self.nodes_at(t1, as_of), self.nodes_at(t2, as_of)
        e1, e2 = self.edges_at(t1, as_of), self.edges_at(t2, as_of)

        allowed: set[str] | None = None
        if args["scope"] is not None:
            seeds, hops = args["scope"]["seeds"], args["scope"]["hops"]
            d1 = self._bfs(e1, seeds, set(n1), hops)
            d2 = self._bfs(e2, seeds, set(n2), hops)
            allowed = set(d1) | set(d2)

        def nok(u: str) -> bool:
            return allowed is None or u in allowed

        def eok(v: EdgeVersion) -> bool:
            return allowed is None or (v.src in allowed and v.dst in allowed)

        nodes_added = sorted(u for u in n2 if u not in n1 and nok(u))
        nodes_removed = sorted(u for u in n1 if u not in n2 and nok(u))
        edges_added = sorted(e for e in e2 if e not in e1 and eok(e2[e]))
        edges_removed = sorted(e for e in e1 if e not in e2 and eok(e1[e]))

        changed = []
        for u in sorted(u for u in n1 if u in n2 and nok(u)):
            a, b = n1[u], n2[u]
            if a.vid != b.vid and (a.props != b.props or a.label != b.label):
                changed.append({"kind": "node", "id": u,
                                "from": {"label": a.label, "props": a.props},
                                "to": {"label": b.label, "props": b.props}})
        for e in sorted(e for e in e1 if e in e2 and eok(e1[e])):
            a, b = e1[e], e2[e]
            if a.vid != b.vid and a.props != b.props:
                changed.append({"kind": "edge", "id": e,
                                "from": {"props": a.props},
                                "to": {"props": b.props}})

        def desc(v: EdgeVersion) -> dict[str, Any]:
            return {"eid": v.eid, "src": v.src, "dst": v.dst, "rel_type": v.rel_type}

        out = {
            "nodes_added": nodes_added[:limit], "nodes_added_total": len(nodes_added),
            "nodes_removed": nodes_removed[:limit],
            "nodes_removed_total": len(nodes_removed),
            "edges_added": [desc(e2[e]) for e in edges_added[:limit]],
            "edges_added_total": len(edges_added),
            "edges_removed": [desc(e1[e]) for e in edges_removed[:limit]],
            "edges_removed_total": len(edges_removed),
            "props_changed": changed[:limit], "props_changed_total": len(changed),
        }
        out["truncated"] = any(
            out[f"{k}_total"] > limit
            for k in ("nodes_added", "nodes_removed", "edges_added",
                      "edges_removed", "props_changed"))
        return out

    # ------------------------------------------------------------------ #
    # temporal_reachability                                             #
    # ------------------------------------------------------------------ #

    def temporal_reachability(self, args: dict[str, Any]) -> dict[str, Any]:
        t_a, t_b = args["window"]["t_a"], args["window"]["t_b"]
        delta = args["delta_max_wait"]
        src_uid = args["src"]
        edges = [v for v in self.believed_edges(args["as_of_tt"])
                 if v.vt_e > t_a and v.vt_s < t_b]
        hops: list[tuple[str, str, int, int]] = []
        for v in edges:
            hops.append((v.src, v.dst, v.vt_s, v.vt_e))
            if args["direction"] == "both":
                hops.append((v.dst, v.src, v.vt_s, v.vt_e))
        if args["direction"] == "in":
            hops = [(d, s, a, b) for s, d, a, b in hops]

        # exhaustive state search over (node, arrival)
        labels: dict[str, set[int]] = {src_uid: {t_a}}
        queue = deque([(src_uid, t_a)])
        while queue:
            u, a = queue.popleft()
            for s, d, vs, ve in hops:
                if s != u:
                    continue
                tau = max(a, vs)
                if tau >= ve or tau >= t_b:
                    continue
                if delta is not None and not (u == src_uid and a == t_a) \
                        and tau - a > delta:
                    continue
                if tau not in labels.setdefault(d, set()):
                    labels[d].add(tau)
                    queue.append((d, tau))
        rows = [{"uid": u, "earliest_arrival": min(ls)}
                for u, ls in labels.items() if u != src_uid]
        rows.sort(key=lambda r: (r["earliest_arrival"], r["uid"]))
        return paginate(rows, args["limit"], args["cursor"])

    # ------------------------------------------------------------------ #
    # temporal_paths                                                    #
    # ------------------------------------------------------------------ #

    def temporal_paths(self, args: dict[str, Any]) -> dict[str, Any]:
        t_a, t_b = args["window"]["t_a"], args["window"]["t_b"]
        edges = sorted((v for v in self.believed_edges(args["as_of_tt"])
                        if v.vt_e > t_a and v.vt_s < t_b),
                       key=lambda v: (v.vt_s, v.eid))
        found: list[tuple[int, int, tuple, list[EdgeVersion]]] = []

        def dfs(node: str, arrival: int, visited: set[str],
                trail: list[EdgeVersion]) -> None:
            if node == args["dst"] and trail:
                key = tuple((v.vt_s, v.eid) for v in trail)
                found.append((arrival, len(trail), key, list(trail)))
                return
            if len(trail) == args["max_hops"]:
                return
            for v in edges:
                if v.src != node or v.dst in visited:
                    continue
                tau = max(arrival, v.vt_s)
                if tau >= v.vt_e or tau >= t_b:
                    continue
                visited.add(v.dst)
                trail.append(v)
                dfs(v.dst, tau, visited, trail)
                trail.pop()
                visited.remove(v.dst)

        dfs(args["src"], t_a, {args["src"]}, [])
        found.sort(key=lambda p: (p[0], p[1], p[2]))
        rows = [{"arrival": a, "hops": h,
                 "edges": [{"src": v.src, "dst": v.dst, "rel_type": v.rel_type,
                            "eid": v.eid, "t": v.vt_s} for v in trail]}
                for a, h, _, trail in found[: args["k"]]]
        return {"rows": rows, "rows_total": len(found),
                "truncated": len(found) > args["k"], "cursor": None}

    # ------------------------------------------------------------------ #
    # motifs                                                       #
    # ------------------------------------------------------------------ #

    _MOTIF_CHECK = {
        "M_triangle_cyclic": lambda a, b, c: (
            b.src == a.dst and c.src == b.dst and c.dst == a.src
            and len({a.src, a.dst, b.dst}) == 3),
        "M_triangle_acyclic_1": lambda a, b, c: (
            b.src == a.src and c.src == a.dst and c.dst == b.dst
            and len({a.src, a.dst, b.dst}) == 3),
        "M_2node_pingpong": lambda a, b, c: (
            b.src == a.dst and b.dst == a.src and c.src == a.src
            and c.dst == a.dst and a.src != a.dst),
        "M_star_out_3": lambda a, b, c: (
            b.src == a.src and c.src == a.src
            and len({a.src, a.dst, b.dst, c.dst}) == 4),
        "M_path_3": lambda a, b, c: (
            b.src == a.dst and c.src == b.dst
            and len({a.src, a.dst, b.dst, c.dst}) == 4),
    }

    def _motif_instances(self, args: dict[str, Any]) -> list[tuple]:
        t_a, t_b = args["window"]["t_a"], args["window"]["t_b"]
        events = self.events_in(t_a, t_b, args["as_of_tt"])
        if args["node_filter"] is not None:
            f = set(args["node_filter"])
            events = [v for v in events if v.src in f and v.dst in f]
        check = self._MOTIF_CHECK[args["motif"]]
        delta = args["delta"]
        out = []
        n = len(events)
        for i in range(n):
            for j in range(i + 1, n):
                if events[j].vt_s - events[i].vt_s > delta:
                    break
                for k in range(j + 1, n):
                    if events[k].vt_s - events[i].vt_s > delta:
                        break
                    a, b, c = events[i], events[j], events[k]
                    if check(a, b, c):
                        out.append((a, b, c))
        out.sort(key=lambda t: tuple((v.vt_s, v.eid) for v in t))
        return out

    def count_temporal_motifs(self, args: dict[str, Any]) -> dict[str, Any]:
        t_a, t_b = args["window"]["t_a"], args["window"]["t_b"]
        events = self.events_in(t_a, t_b, args["as_of_tt"])
        if args["node_filter"] is not None:
            f = set(args["node_filter"])
            events = [v for v in events if v.src in f and v.dst in f]
        return {"count": len(self._motif_instances(args)),
                "n_events_in_window": len(events), "truncated": False}

    def find_temporal_motif_instances(self, args: dict[str, Any]) -> dict[str, Any]:
        rows = [{"edges": [{"src": v.src, "dst": v.dst, "t": v.vt_s,
                            "eid": v.eid, "rel_type": v.rel_type}
                           for v in inst]}
                for inst in self._motif_instances(args)]
        return paginate(rows, args["limit"], args["cursor"])

    # ------------------------------------------------------------------ #
    # graph_metric_timeseries                                           #
    # ------------------------------------------------------------------ #

    def _bucket_bounds(self, args: dict[str, Any]) -> list[tuple[int, int]]:
        t_a, t_b, stride = args["window"]["t_a"], args["window"]["t_b"], args["stride"]
        return [(bs, min(bs + stride, t_b)) for bs in range(t_a, t_b, stride)]

    def graph_metric_timeseries(self, args: dict[str, Any]) -> dict[str, Any]:
        as_of = args["as_of_tt"]
        metric = args["metric"]
        nodes = self.believed_nodes(as_of)
        edges = self.believed_edges(as_of)
        rows = []
        for bs, be in self._bucket_bounds(args):
            if metric == "node_count":
                val = sum(1 for v in nodes if v.valid_at(bs))
            elif metric == "edge_event_count":
                val = sum(1 for v in edges if bs <= v.vt_s < be)
            elif metric == "active_edge_count":
                val = sum(1 for v in edges if v.valid_at(bs))
            elif metric == "mean_out_degree":
                act = sum(1 for v in edges if v.valid_at(bs))
                nod = sum(1 for v in nodes if v.valid_at(bs))
                val = act / nod if nod > 0 else 0.0
            elif metric == "new_node_rate":
                births: dict[str, int] = {}
                for v in nodes:
                    births[v.uid] = min(births.get(v.uid, OPEN_END), v.vt_s)
                val = sum(1 for t in births.values() if bs <= t < be)
            else:  # reciprocity
                pairs = {(v.src, v.dst) for v in edges if bs <= v.vt_s < be}
                val = (sum(1 for (s, d) in pairs if (d, s) in pairs) / len(pairs)
                       if pairs else 0.0)
            rows.append({"t_a": bs, "t_b": be,
                         "value": float(val) if isinstance(val, float) else int(val)})
        out = paginate(rows, args["limit"], args["cursor"])
        out["n_buckets"] = len(rows)
        return out

    # ------------------------------------------------------------------ #
    # burst_detection                                                   #
    # ------------------------------------------------------------------ #

    def burst_detection(self, args: dict[str, Any]) -> dict[str, Any]:
        tgt = args["target"]
        edges = self.believed_edges(args["as_of_tt"])
        if tgt.get("rel_type"):
            edges = [v for v in edges if v.rel_type == tgt["rel_type"]]
        if tgt["kind"] == "node_activity":
            edges = [v for v in edges if tgt["uid"] in (v.src, v.dst)]
        bounds = self._bucket_bounds(args)
        series = [float(sum(1 for v in edges if bs <= v.vt_s < be))
                  for bs, be in bounds]
        w = args["params"].get("w", 10)
        flagged = []
        for i, ((bs, be), x) in enumerate(zip(bounds, series)):
            hist = series[max(0, i - w): i]
            if not hist:
                continue
            if args["method"] == "zscore":
                mean = sum(hist) / len(hist)
                var = sum((h - mean) ** 2 for h in hist) / len(hist)
                std = var ** 0.5
                score = abs(x - mean) / std if std > 0 else (0.0 if x == mean else 1e9)
                score = round(score, 9)  # quantized threshold, same as engine
                flag = score >= args["params"].get("z", 3.0)
            else:
                srt = sorted(hist)
                m = len(srt)
                med = srt[m // 2] if m % 2 else (srt[m // 2 - 1] + srt[m // 2]) / 2
                score = x / med if med > 0 else (1e9 if x > 0 else 0.0)
                score = round(score, 9)
                flag = score >= args["params"].get("r", 3.0)
            if flag:
                flagged.append({"t_a": bs, "t_b": be, "value": x,
                                "score": float(score)})
        out = paginate(flagged, args["limit"], args["cursor"])
        out["n_buckets"] = len(series)
        return out

    # ------------------------------------------------------------------ #
    # diff_belief                                                          #
    # ------------------------------------------------------------------ #

    def diff_belief(self, args: dict[str, Any]) -> dict[str, Any]:
        """Ground truth for the belief diff, one predicate per version.

        Deliberately *not* the engine's shape. `beliefdiff.rs` never looks at
        a version: it works in the range domain, deriving the `added` set from
        the difference of two `tt_s`-run verdict lists and the `retracted` set
        from a segment's folded close slice, and it is precisely that
        derivation this has to be able to contradict. So this restates the
        definition instead — for every version in the population, is its
        `tt_s` in `(t1, t2]`, and is its `tt_e` — and sorts the result by the
        operator's own `(kind, key, tt, change, vid)` order.

        A version written *and* superseded inside the same belief window
        satisfies both predicates and is reported twice, which is the case a
        set difference of `believed(t1)` and `believed(t2)` cannot express
        (it is in neither population). Getting that wrong in one place and
        right in the other is the disagreement this reference exists to find.
        """
        if "window" in args:
            # the registered operator's arguments: a window, and an offset
            # cursor over the same order
            offset = int(args.get("cursor") or 0)
            got = self.diff_belief({"t1": args["t1"], "t2": args["t2"],
                                    "t_a": args["window"]["t_a"],
                                    "t_b": args["window"]["t_b"], "limit": 2**62})
            total = got["added_total"] + got["retracted_total"]
            rows = got["rows"][offset:offset + args["limit"]]
            trunc = offset + len(rows) < total
            return {"rows": rows, "rows_total": total,
                    "added_total": got["added_total"],
                    "retracted_total": got["retracted_total"], "truncated": trunc,
                    "cursor": str(offset + len(rows)) if trunc else None}
        t1, t2 = clamp_tt(args["t1"]), clamp_tt(args["t2"])
        t_a, t_b, limit = args["t_a"], args["t_b"], args["limit"]
        rows: list[dict[str, Any]] = []
        if t1 < t2 and t_a < t_b:
            for kind, versions, key_of in (
                ("node", self.nv, lambda v: v.uid),
                ("edge", self.ev, lambda v: v.eid),
            ):
                for v in versions:
                    if not (v.vt_s < t_b and v.vt_e > t_a):
                        continue
                    for tt, change in ((v.tt_s, "added"), (v.tt_e, "retracted")):
                        if change == "retracted" and v.tt_e == OPEN_END:
                            continue
                        if t1 < tt <= t2:
                            rows.append({"kind": kind, "key": key_of(v),
                                         "vid": v.vid, "tt": tt,
                                         "change": change,
                                         "vt_s": v.vt_s, "vt_e": v.vt_e})
        added_total = sum(1 for r in rows if r["change"] == "added")
        rows.sort(key=lambda r: (0 if r["kind"] == "node" else 1, r["key"],
                                 r["tt"], r["change"], r["vid"]))
        return {
            "rows": rows[:limit],
            "added_total": added_total,
            "retracted_total": len(rows) - added_total,
            "truncated": len(rows) > limit,
        }

    # ------------------------------------------------------------------ #
    # neighborhood_evolution                                           #
    # ------------------------------------------------------------------ #

    def neighborhood_evolution(self, args: dict[str, Any]) -> dict[str, Any]:
        uid, t1, t2, as_of = args["uid"], args["t1"], args["t2"], args["as_of_tt"]
        stride = args["stride"] or max(1, (t2 - t1) // 20)

        def neighbors(t: int) -> set[str]:
            out = set()
            for v in self.edges_at(t, as_of).values():
                if v.src == uid and v.dst != uid:
                    out.add(v.dst)
                if v.dst == uid and v.src != uid:
                    out.add(v.src)
            return out

        n1, n2 = neighbors(t1), neighbors(t2)
        gained, lost = sorted(n2 - n1), sorted(n1 - n2)
        incident = [v for v in self.believed_edges(as_of) if uid in (v.src, v.dst)]
        series = [{"t": bs,
                   "degree": sum(1 for v in incident if v.valid_at(bs))}
                  for bs in range(t1, t2, stride)]
        limit = args["limit"]
        return {
            "neighbors_gained": gained[:limit], "neighbors_gained_total": len(gained),
            "neighbors_lost": lost[:limit], "neighbors_lost_total": len(lost),
            "degree_series": series, "stride": stride,
            "truncated": len(gained) > limit or len(lost) > limit,
        }

    # ------------------------------------------------------------------ #
    # co_active                                                        #
    # ------------------------------------------------------------------ #

    def co_active(self, args: dict[str, Any]) -> dict[str, Any]:
        as_of = args["as_of_tt"]

        def select(spec: dict[str, Any]) -> list[EdgeVersion]:
            return [v for v in self.believed_edges(as_of)
                    if (not spec.get("src") or v.src == spec["src"])
                    and (not spec.get("dst") or v.dst == spec["dst"])
                    and (not spec.get("rel_type") or v.rel_type == spec["rel_type"])]

        rel = args["allen_relation"]["relation"]
        gap = args["allen_relation"].get("gap")

        def ok(a: EdgeVersion, b: EdgeVersion) -> bool:
            if rel == "overlaps":
                return a.vt_s < b.vt_s < a.vt_e < b.vt_e
            if rel == "during":
                return b.vt_s < a.vt_s and a.vt_e < b.vt_e
            if rel == "meets":
                return a.vt_e == b.vt_s
            return 0 < b.vt_s - a.vt_e <= gap  # before

        def desc(v: EdgeVersion) -> dict[str, Any]:
            return {"eid": v.eid, "vid": v.vid, "src": v.src, "dst": v.dst,
                    "rel_type": v.rel_type, "vt_s": v.vt_s, "vt_e": v.vt_e}

        rows = [{"a": desc(a), "b": desc(b)}
                for a in select(args["a_spec"]) for b in select(args["b_spec"])
                if a.vid != b.vid and ok(a, b)]
        rows.sort(key=lambda r: (r["a"]["vt_s"], r["a"]["vid"],
                                 r["b"]["vt_s"], r["b"]["vid"]))
        return paginate(rows, args["limit"], args["cursor"])

    # ------------------------------------------------------------------ #
    # aggregate_events                                                 #
    # ------------------------------------------------------------------ #

    def aggregate_events(self, args: dict[str, Any]) -> dict[str, Any]:
        t_a, t_b = args["window"]["t_a"], args["window"]["t_b"]
        as_of, stride = args["as_of_tt"], args.get("stride")
        dims, aggs = args["group_by"], args["aggregates"]
        events = [v for v in self.believed_edges(as_of)
                  if t_a <= v.vt_s < t_b
                  and (args["rel_types"] is None
                       or v.rel_type in args["rel_types"])]
        if args.get("valid_at") is not None:
            # engine-level: the events the state at `valid_at` still holds
            events = [v for v in events if v.valid_at(args["valid_at"])]

        # property typing, by the same rule and its own loop: a row
        # whose value is absent or of the wrong JSON type leaves the
        # population and is counted, per property.
        pkeys = prop_keys(args)
        skipped: dict[str, int] = {k: 0 for k in pkeys}
        pf = args.get("prop_filter")
        if pf is not None:
            kept = []
            for v in events:
                verdict = matches(v.props, pf["prop"], pf["cmp"], pf["value"])
                if verdict is SKIP:
                    skipped[pf["prop"]] += 1
                elif verdict:
                    kept.append(v)
            events = kept

        # cohort pre-filter and pair modes, each in its own loop
        ef = args.get("endpoint_filter")
        if ef is not None:
            cohort = set(ef["uids"])
            events = [v for v in events
                      if (v.src in cohort if ef["role"] == "src" else
                          v.dst in cohort if ef["role"] == "dst" else
                          v.src in cohort or v.dst in cohort)]
        pair_mode = args.get("pair_mode")
        if pair_mode == "reciprocal":
            directed = {(v.src, v.dst) for v in events}
            events = [v for v in events
                      if (v.src, v.dst) in directed
                      and (v.dst, v.src) in directed]

        def canon(v: EdgeVersion) -> tuple[str, str]:
            """Endpoints as the grouping sees them: folded onto the canonical
            pair under either pair mode, untouched otherwise."""
            if pair_mode is None:
                return v.src, v.dst
            return (v.src, v.dst) if v.src <= v.dst else (v.dst, v.src)

        def label_at(uid: str, t: int) -> str | None:
            # believed valid intervals of one identity are disjoint, so at
            # most one version matches
            for v in self.believed_nodes(as_of):
                if v.uid == uid and v.valid_at(t):
                    return v.label
            return None

        def dim_value(d: dict[str, Any], v: EdgeVersion) -> Any:
            if d["dim"] == "time_bucket":
                return (v.vt_s - t_a) // stride
            if d["dim"] == "calendar_unit":
                return calendar_unit(v.vt_s, d["unit"],
                                     d.get("tz_offset_minutes") or 0)
            if d["dim"] == "rel_type":
                return v.rel_type
            cs, cd = canon(v)
            ep = cs if d["role"] == "src" else cd
            if d["dim"] == "endpoint":
                return ep
            return label_at(ep, v.vt_s)

        groups: dict[tuple, list[EdgeVersion]] = {}
        if dims:
            for v in events:
                groups.setdefault(tuple(dim_value(d, v) for d in dims),
                                  []).append(v)
        else:
            groups[()] = list(events)  # exactly one row, even with 0 events

        def sort_key(key: tuple) -> tuple:
            # numeric dims in numeric order, strings in code-point order,
            # null labels first — except a named calendar unit, which sorts
            # in CALENDAR order. Code-point order would put Friday first and
            # April before January, which is not an ordering of anything.
            out = []
            for d, kv in zip(dims, key):
                if kv is None:
                    out.append((0, ""))
                elif d["dim"] == "calendar_unit" and \
                        d["unit"] in ("day_of_week", "month_of_year"):
                    names = DAY_NAMES if d["unit"] == "day_of_week" \
                        else MONTH_NAMES
                    out.append((1, names.index(kv)))
                else:
                    out.append((1, kv))
            return tuple(out)

        def mean(vals: list[int]) -> float:
            q, r = divmod(sum(vals), len(vals))
            return float(q) + r / len(vals)

        def agg_field(a: dict[str, Any]) -> str:
            if a["agg"] == "count":
                return "count"
            if a["agg"] in SEQ_AGGS:
                return a["agg"]
            if a["agg"] == "count_distinct":
                return f"distinct_{a['of']}"
            if a.get("of") == "prop":
                return f"{a['agg']}_prop_{a['prop']}"
            return f"{a['agg']}_{a['of']}"

        # The three sequence aggregates, each written as the
        # definition reads rather than as it would be computed: sort the
        # group's event times, then walk them.
        def max_gap(ts: list[int]) -> int | None:
            # fewer than two events is no gap at all, which is null and not
            # zero — zero is what "two simultaneous events" means
            return None if len(ts) < 2 else max(b - a for a, b in
                                                zip(ts, ts[1:]))

        def max_in_window(ts: list[int], span: int) -> int:
            # a window that maximizes the count can always be slid until it
            # starts on an event, so the event times are the only starts
            # worth testing. Half-open [t, t + span), as every window in this
            # system is.
            return max((sum(1 for u in ts if t <= u < t + span) for t in ts),
                       default=0)

        def max_session_span(ts: list[int], gap: int) -> int | None:
            if not ts:
                return None          # no run exists, so it has no span
            best, start = 0, ts[0]
            for prev, cur in zip(ts, ts[1:]):
                if cur - prev > gap:
                    best, start = max(best, prev - start), cur
            return max(best, ts[-1] - start)

        rows = []
        for key in sorted(groups, key=sort_key):
            evs = groups[key]
            row: dict[str, Any] = {}
            for d, kv in zip(dims, key):
                if d["dim"] == "time_bucket":
                    row["t_a"] = t_a + kv * stride
                    row["t_b"] = min(t_a + (kv + 1) * stride, t_b)
                elif d["dim"] == "calendar_unit":
                    row[d["unit"]] = kv
                elif d["dim"] == "rel_type":
                    row["rel_type"] = kv
                elif d["dim"] == "endpoint":
                    row[d["role"]] = kv
                else:
                    row[f"{d['role']}_label"] = kv
            for a in aggs:
                if a["agg"] == "count":
                    val: Any = len(evs)
                elif a["agg"] in SEQ_AGGS:
                    ts = sorted(v.vt_s for v in evs)
                    if a["agg"] == "max_gap":
                        val = max_gap(ts)
                    elif a["agg"] == "max_in_window":
                        val = max_in_window(ts, a["span"])
                    else:
                        val = max_session_span(ts, a["gap"])
                elif a["agg"] == "count_distinct":
                    val = len({v.src if a["of"] == "src" else v.dst
                               for v in evs})
                elif a.get("of") == "prop":
                    vals = []
                    for v in evs:
                        pv = numeric_value(v.props, a["prop"])
                        if pv is SKIP:
                            skipped[a["prop"]] += 1
                        else:
                            vals.append(pv)
                    # exact property arithmetic, not the typed-column `mean` above
                    val = None if not vals else \
                        {"min": min, "max": max, "mean": blessed_mean,
                         "sum": sum_exact}[a["agg"]](vals)
                else:
                    if a["of"] == "vt_s":
                        vals = [v.vt_s for v in evs]
                    else:  # duration: open-ended rows contribute nothing
                        vals = [v.vt_e - v.vt_s for v in evs
                                if v.vt_e < OPEN_END]
                    if not vals:
                        val = None
                    elif a["agg"] == "min":
                        val = min(vals)
                    elif a["agg"] == "max":
                        val = max(vals)
                    elif a["agg"] == "sum":
                        val = sum(vals)
                    else:
                        val = mean(vals)
                row[agg_field(a)] = val
            rows.append(row)
        out = paginate(rows, args["limit"], args["cursor"])
        if pkeys:
            out["prop_coercion"] = {k: skipped[k] for k in pkeys}
        return out

    # ------------------------------------------------------------------ #
    # resolve_entities                                                 #
    # ------------------------------------------------------------------ #

    def resolve_entities(self, args: dict[str, Any]) -> dict[str, Any]:
        q, ql, as_of = args["query"], args["query"].lower(), args["as_of_tt"]
        latest: dict[str, NodeVersion] = {}
        matched: dict[str, int] = {}
        for v in self.believed_nodes(as_of):
            cur = latest.get(v.uid)
            if cur is None or v.vt_s > cur.vt_s:
                latest[v.uid] = v
            # Only JSON *string* names participate in matching. The
            # former str() coercion matched text the store never held —
            # "None" for a JSON null, "42" for a number.
            name = v.props.get("name")
            if v.uid == q:
                score = 0
            elif ql in v.uid.lower():
                score = 1
            elif isinstance(name, str) and name and ql in name.lower():
                score = 2
            else:
                continue
            matched[v.uid] = min(matched.get(v.uid, 9), score)
        rows = []
        for uid, score in matched.items():
            v = latest[uid]
            if args["label"] is not None and v.label != args["label"]:
                continue
            rows.append({"uid": uid, "label": v.label,
                         "name": v.props.get("name"), "match": score})
        rows.sort(key=lambda r: (r["match"], r["uid"]))
        return paginate(rows, args["limit"], args["cursor"])


    def _paths_window(self, args: dict[str, Any], sources: list[str],
                      targets: list[str] | None, versions: list[tuple]) -> dict[str, Any]:
        """`paths` under `valid_window`, by instants: at every elementary
        instant of the window, the snapshot's node-simple paths (edges valid
        then, every node of the path with a version valid then) and their
        costs; the distance is the least over the instants, and the optimal
        paths those reaching it at some instant."""
        from fractions import Fraction

        vw = args["valid_window"]
        ta, tb = vw["t_a"], vw["t_b"]
        nodes = [v for v in self.believed_nodes(args["as_of_tt"])
                 if v.vt_s < tb and ta < v.vt_e]
        points = {ta}
        for x in [(e[4], e[5]) for e in versions] + [(v.vt_s, v.vt_e) for v in nodes]:
            for y in x:
                if ta < y < tb:
                    points.add(y)
        res, mx = args["result"], args.get("max_hops") if args["metric"] == "hops" else None
        if res in ("all_paths", "k_paths"):
            return self._paths_window_enum(args, sources, targets, versions, nodes, points)
        best: dict[tuple[str, str], Any] = {}
        opt: dict[tuple[str, str], set[tuple[str, ...]]] = {}
        score: dict[tuple[str, str], Any] = {}
        for t in sorted(points):
            alive = {v.uid for v in nodes if v.vt_s <= t < v.vt_e}
            step: dict[tuple[str, str], Any] = {}
            for a, b, w, sc, lo, hi in versions:
                if lo <= t < hi and a in alive and b in alive:
                    if (a, b) not in step or w < step[(a, b)]:
                        step[(a, b)] = w
                        score[(a, b)] = sc
            nbrs: dict[str, list[str]] = {}
            for a, b in step:
                nbrs.setdefault(a, []).append(b)
            for s in sources:
                if s not in alive:
                    continue
                found: list[list[str]] = []

                def rec(path: list[str]) -> None:
                    found.append(list(path))
                    if mx is not None and len(path) - 1 >= mx:
                        return
                    for b in nbrs.get(path[-1], ()):
                        if b not in path:
                            path.append(b)
                            rec(path)
                            path.pop()

                rec([s])
                at: dict[str, Any] = {}
                for path in found:
                    c = sum((step[(a, b)] for a, b in zip(path, path[1:])), 0)
                    key = (s, path[-1])
                    if key not in best or c < best[key]:
                        best[key] = c
                        opt[key] = set()
                    if c == best[key]:
                        opt[key].add(tuple(path))
                    at[path[-1]] = c
        rows: list[dict[str, Any]] = []

        def num(x):
            return float(x) if isinstance(x, Fraction) else x

        def sc_of(path):
            return sum((score[(a, b)] for a, b in zip(path, path[1:])), 0)

        pairs = []
        for (s, t), d in best.items():
            if targets is None and t == s:
                continue
            if targets is not None and t not in targets:
                continue
            pairs.append((s, t, d))
        if args["pairs"] == "min_over_pairs" and pairs:
            low = min(d for _, _, d in pairs)
            pairs = [x for x in pairs if x[2] == low]
        pairs.sort(key=lambda x: (x[0], x[1]))
        for s, t, d in pairs:
            base = {"src": s, "dst": t, "distance": num(d)}
            paths = sorted(list(p) for p in opt[(s, t)])
            if res == "distance":
                rows.append(base)
            elif res == "one_path":
                path = min(paths, key=lambda q: list(reversed(q)))
                row = {**base, "path": path}
                if args.get("score"):
                    row["score"] = num(sc_of(path))
                rows.append(row)
            else:
                for path in paths:
                    row = {**base, "path": path}
                    if args.get("score"):
                        row["score"], row["_sx"] = num(sc_of(path)), sc_of(path)
                    rows.append(row)
        if res == "all_optimal":
            order = args.get("order", "path")
            if order == "path":
                rows.sort(key=lambda r: (r["src"], r["dst"], len(r["path"]), r["path"]))
            elif order == "hops_desc":
                rows.sort(key=lambda r: (-len(r["path"]), r["src"], r["dst"], r["path"]))
            else:
                rows.sort(key=lambda r: (-r["_sx"], r["src"], r["dst"], len(r["path"]),
                                         r["path"]))
        for r in rows:
            r.pop("_sx", None)
        return paginate(rows, args["limit"], args["cursor"])

    def _paths_window_enum(self, args: dict[str, Any], sources: list[str],
                           targets: list[str] | None, versions: list[tuple],
                           nodes: list, points: set[int]) -> dict[str, Any]:
        """`all_paths` / `k_paths` under `valid_window`: at every elementary
        instant, the snapshot's node-simple paths over nodes valid then
        (time-respecting: some choice of the edges valid then follows the
        order), the union over the instants; a path's cost is its steps'
        least weights over the window's versions."""
        from fractions import Fraction

        tr = args.get("time_respecting")
        lo, hi = args.get("min_hops") or 1, args["max_hops"]
        cost: dict[tuple[str, str], Any] = {}
        score: dict[tuple[str, str], Any] = {}
        for a, b, w, sc, _s, _e in versions:
            if (a, b) not in cost or w < cost[(a, b)]:
                cost[(a, b)] = w
            score[(a, b)] = sc
        found: set[tuple[str, ...]] = set()
        for t in sorted(points):
            alive = {v.uid for v in nodes if v.vt_s <= t < v.vt_e}
            times: dict[tuple[str, str], list[int]] = {}
            for a, b, _w, _sc, s_, e_ in versions:
                if s_ <= t < e_ and a in alive and b in alive:
                    times.setdefault((a, b), []).append(s_)
            nbrs: dict[str, list[str]] = {}
            for a, b in times:
                nbrs.setdefault(a, []).append(b)

            def feasible(path: list[str]) -> bool:
                if tr is None:
                    return True
                t_prev = None
                for a, b in zip(path, path[1:]):
                    ok = [x for x in times[(a, b)] if t_prev is None or
                          (x > t_prev if tr == "strict" else x >= t_prev)]
                    if not ok:
                        return False
                    t_prev = min(ok)
                return True

            def rec(path: list[str]) -> None:
                n = len(path) - 1
                if n >= lo and n >= 1 and (targets is None or path[-1] in targets) and \
                        feasible(path):
                    found.add(tuple(path))
                if n == hi:
                    return
                for b in nbrs.get(path[-1], ()):
                    if b not in path:
                        path.append(b)
                        rec(path)
                        path.pop()

            for s in sources:
                if s in alive:
                    rec([s])

        def num(x):
            return float(x) if isinstance(x, Fraction) else x

        def c_of(path):
            return sum((cost[(a, b)] for a, b in zip(path, path[1:])), 0)

        def sc_of(path):
            return sum((score[(a, b)] for a, b in zip(path, path[1:])), 0)

        sel = sorted(list(x) for x in found)
        if args["result"] == "k_paths":
            kept = []
            for s in sources:
                for t in sorted({p[-1] for p in sel if p[0] == s}):
                    lst = sorted((p for p in sel if p[0] == s and p[-1] == t),
                                 key=lambda p: (c_of(p), len(p), p))
                    kept += lst[: args["k"]]
            sel = kept
        rows = []
        for path in sel:
            row = {"src": path[0], "dst": path[-1], "hops": len(path) - 1, "path": path}
            if args["metric"] == "weight":
                row["cost"] = num(c_of(path))
            if args.get("score"):
                row["score"], row["_sx"] = num(sc_of(path)), sc_of(path)
            rows.append(row)
        order = args.get("order", "path")
        if order == "path" and args["result"] == "all_paths":
            rows.sort(key=lambda r: (r["src"], r["dst"], len(r["path"]), r["path"]))
        elif order == "hops_desc":
            rows.sort(key=lambda r: (-len(r["path"]), r["src"], r["dst"], r["path"]))
        elif order == "score_desc":
            rows.sort(key=lambda r: (-r["_sx"], r["src"], r["dst"], len(r["path"]), r["path"]))
        for r in rows:
            r.pop("_sx", None)
        return paginate(rows, args["limit"], args["cursor"])

    # ------------------------------------------------------------------ #
    # match (the pattern operator)                                         #
    # ------------------------------------------------------------------ #

    def match(self, args: dict[str, Any]) -> dict[str, Any]:
        """A nested-loop matcher over the version lists, straight from the
        filled argument (the operator's contract), sharing nothing with its
        compiler or either executor: bindings by backtracking, closures by
        breadth-first search, sub-patterns per row, then group, order, limit."""
        return _MatchOracle(self, args).run()

    # ------------------------------------------------------------------ #
    # engine-level `state_series`                                          #
    # ------------------------------------------------------------------ #

    def state_series(self, args: dict[str, Any]) -> dict[str, Any]:
        """Per bucket, the definition: which versions are alive (valid at the
        bucket start under `as_of_tt`, overlapping it with their share, or
        believed at the bucket's transaction time and valid at `t_valid`),
        then the aggregate over them, written as it reads."""
        from fractions import Fraction

        from gnomon.temporal.props import sum_exact

        kind, axis, at = args["kind"], args["axis"], args["at"]
        t_a, t_b, stride = args["window"]["t_a"], args["window"]["t_b"], args["stride"]
        fn, of = args["agg"]["fn"], (args["agg"].get("of") or {}).get("prop")
        groups = args.get("group_by") or []
        if axis == "transaction":
            versions = list(self.nv if kind == "node" else self.ev)
        else:
            versions = list(self.believed_nodes(args["as_of_tt"]) if kind == "node"
                            else self.believed_edges(args["as_of_tt"]))
        skipped: dict[str, int] = {}
        pop = []
        for v in sorted(versions, key=lambda v: v.vid):
            if kind == "node" and args.get("labels") is not None and \
                    v.label not in args["labels"]:
                continue
            if kind == "edge" and args.get("rel_types") is not None and \
                    v.rel_type not in args["rel_types"]:
                continue
            if axis == "transaction":
                # believed at some time of the window, about the valid instant
                if not v.valid_at(args["t_valid"]) or not (v.tt_s < t_b and t_a < v.tt_e):
                    continue
            elif not (v.vt_s < t_b and t_a < v.vt_e):
                continue
            ok = True
            for p in args.get("where") or ():
                verdict = matches(v.props, p["prop"], p["cmp"], p["value"])
                if verdict is SKIP:
                    skipped[p["prop"]] = skipped.get(p["prop"], 0) + 1
                if verdict is not True:
                    ok = False
                    break
            if not ok:
                continue
            val = numeric_value(v.props, of) if of is not None else None
            if of is not None and val is SKIP:
                skipped[of] = skipped.get(of, 0) + 1
            pop.append((v, val))

        def num(x):
            if isinstance(x, Fraction):
                return x.numerator if x.denominator == 1 else float(x)
            return x

        def key_of(v):
            return tuple(v.props.get(g["prop"]) if isinstance(g, dict) else getattr(v, g)
                         for g in groups)

        rows = []
        for bs in range(t_a, t_b, stride):
            be = min(bs + stride, t_b)
            alive = []
            for v, val in pop:
                if axis == "transaction":
                    if v.tt_s <= bs < v.tt_e:
                        alive.append((v, val, 1))
                elif at == "time_weighted":
                    if v.vt_s < be and bs < v.vt_e:
                        alive.append((v, val, min(v.vt_e, be) - max(v.vt_s, bs)))
                elif v.vt_s <= bs < v.vt_e:
                    alive.append((v, val, 1))
            by: dict[tuple, list] = {}
            for v, val, w in alive:
                by.setdefault(key_of(v), []).append((val, w))
            if not groups and not by:
                by[()] = []
            for key in sorted(by, key=lambda k: tuple((0, "") if x is None else
                                                      (1, canonical_json(x)) for x in k)):
                ms = by[key]
                vals = [(x, w) for x, w in ms if of is None or x is not SKIP]
                if fn == "count":
                    value = Fraction(sum(w for _, w in ms), be - bs) \
                        if at == "time_weighted" else len(ms)
                elif not vals:
                    value = None
                elif fn == "min":
                    value = min(x for x, _ in vals)
                elif fn == "max":
                    value = max(x for x, _ in vals)
                elif at != "time_weighted":
                    plain = [x for x, _ in vals]
                    value = sum_exact(plain) if fn == "sum" else blessed_mean(plain)
                else:
                    ex = [(Fraction(x) if isinstance(x, float) else x, w) for x, w in vals]
                    tot = sum((x * w for x, w in ex), Fraction(0))
                    value = tot / (be - bs) if fn == "sum" else tot / sum(w for _, w in ex)
                row = {"t_a": bs, "t_b": be}
                for g, x in zip(groups, key):
                    row[f"prop_{g['prop']}" if isinstance(g, dict) else g] = x
                row["value"] = num(value)
                rows.append(row)
        out = paginate(rows, args["limit"], args["cursor"])
        out["n_buckets"] = len(range(t_a, t_b, stride))
        if of is not None or args.get("where"):
            out["prop_coercion"] = dict(sorted(skipped.items()))
        return out

    # ------------------------------------------------------------------ #
    # engine-level `paths`                                                 #
    # ------------------------------------------------------------------ #

    def paths(self, args: dict[str, Any]) -> dict[str, Any]:
        """Every result kind from the definition: enumerate every node-simple
        path (small stores only), cost each one exactly, and read distances,
        optimal paths and enumerations off that list. Shares nothing with the
        operator's searches."""
        from fractions import Fraction

        from gnomon.temporal.ops_match import validate as match_validate

        def match_rows(spec: dict[str, Any]) -> list[dict[str, Any]]:
            q = dict(spec)
            if "scope" not in q:
                scope: dict[str, Any] = {"as_of_tt": args["as_of_tt"]}
                if args.get("t_valid") is not None:
                    scope["vt"] = {"instant": args["t_valid"]}
                elif args.get("valid_window") is not None:
                    scope["vt"] = {"window": args["valid_window"]}
                    scope["co_valid"] = ["valid"]
                elif args.get("window") is not None:
                    scope["vt"] = {"window": args["window"], "mode": "event"}
                q["scope"] = scope
            q["limit"] = 10_000
            got = self.match(match_validate(q))
            return [dict(zip(got["columns"], r)) if isinstance(r, list) else r
                    for r in got["rows"]]

        def ends(spec: dict[str, Any] | None) -> list[str] | None:
            if spec is None:
                return None
            if "uids" in spec:
                return list(dict.fromkeys(spec["uids"]))
            return list(dict.fromkeys(str(r["uid"]) for r in match_rows(spec["match"])
                                      if r.get("uid") is not None))

        def exact(v: Any) -> Any:
            return Fraction(v) if isinstance(v, float) else v

        known = {v.uid for v in self.nv}
        sources = [u for u in ends(args["sources"]) if u in known]
        if len(sources) != len(ends(args["sources"])):
            raise InvalidArgError("oracle: unknown source")
        targets = ends(args.get("targets"))
        res, metric = args["result"], args["metric"]

        if metric == "arrival":
            rows: list[dict[str, Any]] = []
            for s in sources:
                if res == "distance":
                    got = self.temporal_reachability({
                        "src": s, "window": args["window"], "as_of_tt": args["as_of_tt"],
                        "delta_max_wait": args.get("delta_max_wait"),
                        "direction": args["dir"], "limit": 10_000, "cursor": None})
                    rows += [{"src": s, "dst": r["uid"], "arrival": r["earliest_arrival"]}
                             for r in got["rows"] if targets is None or r["uid"] in targets]
                else:
                    for t in sorted(set(targets or ())):
                        got = self.temporal_paths({
                            "src": s, "dst": t, "window": args["window"], "k": args["k"],
                            "max_hops": args.get("max_hops") or 4,
                            "as_of_tt": args["as_of_tt"]})
                        rows += [{"src": s, "dst": t, "arrival": r["arrival"],
                                  "hops": r["hops"], "edges": r["edges"]} for r in got["rows"]]
            return paginate(rows, args["limit"], args["cursor"])

        tr = args.get("time_respecting")
        both = args["dir"] == "both"

        def pair_table(spec):
            out: dict[tuple, Any] = {}
            for r in match_rows(spec["match"]):
                if r["w"] is None or r["src"] not in known or r["dst"] not in known:
                    continue
                key = tuple(sorted((r["src"], r["dst"]))) if both else (r["src"], r["dst"])
                out[key] = exact(r["w"])
            return out

        wspec, sspec = args.get("weight"), args.get("score")
        wtab = pair_table(wspec) if wspec and "match" in wspec else None
        stab = pair_table(sspec) if sspec else None

        def value(spec, table, v):
            if table is not None:
                x = table.get(tuple(sorted((v.src, v.dst))) if both else (v.src, v.dst))
            else:
                raw = v.props.get(spec["prop"])
                x = exact(raw) if isinstance(raw, (int, float)) and \
                    not isinstance(raw, bool) else None
            if x is None and spec.get("missing") == "default":
                x = exact(spec["default"])
            return x

        vw = args.get("valid_window")
        trunc = args.get("truncate")
        # the traversals each candidate edge offers: (version, from, to, flag)
        cands = []
        for v in self.believed_edges(args["as_of_tt"]):
            if args.get("rel_types") is not None and v.rel_type not in args["rel_types"]:
                continue
            if vw is not None:
                if not (v.vt_s < vw["t_b"] and vw["t_a"] < v.vt_e):
                    continue
            elif tr is None:
                if not v.valid_at(args["t_valid"]):
                    continue
            elif trunc is None and not args["window"]["t_a"] <= v.vt_s < args["window"]["t_b"]:
                continue
            # A time-respecting trace at t_valid steps only through edges
            # valid then (a transfer deleted by then does not count)
            if tr is not None and args.get("t_valid") is not None and \
                    not v.valid_at(args["t_valid"]):
                continue
            dirs = [(v.src, v.dst, 0)] if args["dir"] == "out" else \
                [(v.dst, v.src, 1)] if args["dir"] == "in" else \
                [(v.src, v.dst, 0), (v.dst, v.src, 1)]
            cands += [(v, a, b, flag) for a, b, flag in dirs]
        if trunc is not None:
            # FinBench's cut: per vertex, the first `limit` edges by the key,
            # before every other filter
            per: dict[str, list] = {}
            for x in cands:
                v = x[0]
                if trunc["by"] == "vt_s":
                    key = v.vt_s
                else:
                    raw = v.props.get(trunc["by"]["prop"])
                    key = exact(raw) if isinstance(raw, (int, float)) and \
                        not isinstance(raw, bool) else None
                per.setdefault(x[1], []).append((key, x[2], v.vid, x[3], x))
            desc = trunc["dir"] == "desc"
            cut = []
            for lst in per.values():
                keyed = sorted((y for y in lst if y[0] is not None),
                               key=lambda y: (-y[0] if desc else y[0], y[1], y[2], y[3]))
                rest = sorted((y for y in lst if y[0] is None), key=lambda y: (y[1], y[2], y[3]))
                cut += [y[4] for y in (keyed + rest)[: trunc["limit"]]]
            cands = cut
        step: dict[tuple[str, str], Any] = {}
        score: dict[tuple[str, str], Any] = {}
        times: dict[tuple[str, str], list[int]] = {}
        versions: list[tuple[str, str, Any, Any, int, int]] = []
        for v, a, b, _flag in cands:
            if tr is not None and not args["window"]["t_a"] <= v.vt_s < args["window"]["t_b"]:
                continue
            ew = args.get("edge_window")
            if ew is not None and not ew["t_a"] <= v.vt_s < ew["t_b"]:
                continue
            if any(matches(v.props, p["prop"], p["cmp"], p["value"]) is not True
                   for p in args.get("edge_where") or ()):
                continue
            w = value(wspec, wtab, v) if metric == "weight" else 1
            if w is None:
                continue
            sc = (value(sspec, stab, v) or 0) if sspec else 0
            if a == b:
                continue
            if vw is not None:
                versions.append((a, b, w, sc, v.vt_s, v.vt_e))
                continue
            if (a, b) not in step or w < step[(a, b)]:
                step[(a, b)], score[(a, b)] = w, sc
            times.setdefault((a, b), []).append(v.vt_s)
        if vw is not None:
            return self._paths_window(args, sources, targets, versions)

        nbrs: dict[str, list[str]] = {}
        for a, b in step:
            nbrs.setdefault(a, []).append(b)

        def feasible(p: list[str]) -> bool:
            if tr is None:
                return True
            t_prev = None
            for a, b in zip(p, p[1:]):
                ok = [t for t in times[(a, b)] if t_prev is None or
                      (t > t_prev if tr == "strict" else t >= t_prev)]
                if not ok:
                    return False
                t_prev = min(ok)
            return True

        allp: list[list[str]] = []

        def rec(p: list[str]) -> None:
            allp.append(list(p))
            for b in nbrs.get(p[-1], ()):
                if b not in p:
                    p.append(b)
                    rec(p)
                    p.pop()

        for s in sources:
            rec([s])

        def cost(p):
            return sum((step[(a, b)] for a, b in zip(p, p[1:])), 0)

        def sc_of(p):
            return sum((score[(a, b)] for a, b in zip(p, p[1:])), 0)

        def num(x):
            return float(x) if isinstance(x, Fraction) else x

        def tgt_ok(s, t):
            return (targets is None and t != s) or (targets is not None and t in targets)

        rows = []
        if res in ("all_paths", "k_paths"):
            lo, hi = args.get("min_hops") or 1, args["max_hops"]
            sel = [p for p in allp if lo <= len(p) - 1 <= hi and len(p) > 1
                   and (targets is None or p[-1] in targets) and feasible(p)]
            if res == "k_paths":
                kept = []
                for s in sources:
                    for t in sorted({p[-1] for p in sel if p[0] == s}):
                        lst = sorted((p for p in sel if p[0] == s and p[-1] == t),
                                     key=lambda p: (cost(p), len(p), p))
                        kept += lst[: args["k"]]
                sel = kept
            for p in sel:
                row = {"src": p[0], "dst": p[-1], "hops": len(p) - 1, "path": p}
                if metric == "weight":
                    row["cost"] = num(cost(p))
                if sspec:
                    row["score"], row["_sx"] = num(sc_of(p)), sc_of(p)
                rows.append(row)
            if args.get("order", "path") == "path" and res == "all_paths":
                rows.sort(key=lambda r: (r["src"], r["dst"], len(r["path"]), r["path"]))
        else:
            mx = args.get("max_hops")
            pairs = []
            for s in sources:
                cands = sorted(({p[-1] for p in allp if p[0] == s} | {s})
                               if targets is None else set(targets))
                for t in cands:
                    if not tgt_ok(s, t) and not (targets is not None and t == s):
                        continue
                    ps = [p for p in allp if p[0] == s and p[-1] == t
                          and (mx is None or len(p) - 1 <= mx)]
                    if not ps:
                        continue
                    pairs.append((s, t, min(cost(p) for p in ps), ps))
            if args["pairs"] == "min_over_pairs" and pairs:
                best = min(d for _, _, d, _ in pairs)
                pairs = [x for x in pairs if x[2] == best]
            pairs.sort(key=lambda x: (x[0], x[1]))
            for s, t, d, ps in pairs:
                base = {"src": s, "dst": t, "distance": num(d)}
                opt = sorted(p for p in ps if cost(p) == d)
                if res == "distance":
                    rows.append(base)
                elif res == "one_path":
                    p = min(opt, key=lambda q: list(reversed(q)))
                    row = {**base, "path": p}
                    if sspec:
                        row["score"] = num(sc_of(p))
                    rows.append(row)
                else:
                    for p in opt:
                        row = {**base, "path": p}
                        if sspec:
                            row["score"], row["_sx"] = num(sc_of(p)), sc_of(p)
                        rows.append(row)
            if res == "all_optimal" and args.get("order", "path") == "path":
                rows.sort(key=lambda r: (r["src"], r["dst"], len(r["path"]), r["path"]))
        order = args.get("order", "path")
        if order == "hops_desc":
            rows.sort(key=lambda r: (-len(r["path"]), r["src"], r["dst"], r["path"]))
        elif order == "score_desc":
            rows.sort(key=lambda r: (-r["_sx"], r["src"], r["dst"], len(r["path"]),
                                     r["path"]))
        for r in rows:
            r.pop("_sx", None)
        return paginate(rows, args["limit"], args["cursor"])


def _expr_cols(e: Any) -> list[str]:
    """Column names an expression reads."""
    if isinstance(e, dict):
        out = [e["col"]] if isinstance(e.get("col"), str) else []
        return out + [c for k, v in e.items() if k not in ("lit", "col") for c in _expr_cols(v)]
    if isinstance(e, list):
        return [c for v in e for c in _expr_cols(v)]
    return []


def _expr_vars(e: Any) -> set[str]:
    """Pattern variables an expression reads."""
    out: set[str] = set()
    if isinstance(e, dict):
        for k, v in e.items():
            if k in ("prop", "tprop"):
                out.add(v[0])
            elif k == "col" and isinstance(v, str) and "." in v:
                out.add(v.split(".", 1)[0])
            elif k != "lit":
                out |= _expr_vars(v)
    elif isinstance(e, list):
        for v in e:
            out |= _expr_vars(v)
    return out


def _rx_reverse(spec: dict) -> dict:
    if "sym" in spec:
        return {"sym": spec["sym"], "inv": not spec.get("inv", False)}
    if "seq" in spec:
        return {"seq": [_rx_reverse(p) for p in spec["seq"][::-1]]}
    if "alt" in spec:
        return {"alt": [_rx_reverse(p) for p in spec["alt"]]}
    return {k: _rx_reverse(v) for k, v in spec.items()}


def _rel_ok(e: EdgeVersion, rel: str | list[str] | None) -> bool:
    if rel is None:
        return True
    return e.rel_type in rel if isinstance(rel, list) else e.rel_type == rel


def _totals_in(e: Any) -> list[str]:
    if isinstance(e, dict):
        if set(e) == {"total"}:
            return [e["total"]]
        return [c for k, v in e.items() if k != "lit" for c in _totals_in(v)]
    if isinstance(e, list):
        return [c for v in e for c in _totals_in(v)]
    return []


def _utc_ymd(us: int) -> tuple[int, int, int]:
    d = datetime(1970, 1, 1) + timedelta(microseconds=us)
    return d.year, d.month, d.day


def _like(text: str, pattern: str) -> bool:
    """SQL LIKE by dynamic programming over (text, pattern) positions."""
    n, m = len(text), len(pattern)
    ok = [[False] * (m + 1) for _ in range(n + 1)]
    ok[0][0] = True
    for j in range(1, m + 1):
        ok[0][j] = ok[0][j - 1] and pattern[j - 1] == "%"
    for i in range(1, n + 1):
        for j in range(1, m + 1):
            p = pattern[j - 1]
            if p == "%":
                ok[i][j] = ok[i][j - 1] or ok[i - 1][j]
            else:
                ok[i][j] = ok[i - 1][j - 1] and (p == "_" or p == text[i - 1])
    return ok[n][m]


class _MatchOracle:
    """One `match` evaluation. A binding row maps each pattern variable to
    `(kind, version, depth)`; outputs live under the reserved key `__out`."""

    def __init__(self, oracle: Oracle, args: dict[str, Any]) -> None:
        self.args = args
        scope = args["scope"]
        self.T = clamp_tt(scope["as_of_tt"])
        belief = scope["belief"]
        vt = scope["vt"]
        if "instant" in vt:
            t = vt["instant"]
            self.snapshot = belief == "current"

            def vt_ok(v):
                return v.vt_s <= t < v.vt_e
        else:
            ta, tb = vt["window"]["t_a"], vt["window"]["t_b"]
            event = vt.get("mode", "overlap") == "event"
            # a window of width one in overlap mode is the instant
            self.snapshot = belief == "current" and not event and tb - ta == 1

            def vt_ok(v):
                return ta <= v.vt_s < tb if event else (v.vt_s < tb and ta < v.vt_e)
        T = self.T

        def bel_ok(v):
            if belief == "current":
                return v.tt_s <= T < v.tt_e
            if belief == "superseded":
                return v.tt_s <= T and v.tt_e <= T
            return v.tt_s <= T
        self.nodes = [v for v in oracle.nv if bel_ok(v) and vt_ok(v)]
        self.edges = [v for v in oracle.ev if bel_ok(v) and vt_ok(v)]
        self.node_of: dict[str, NodeVersion] = {}
        for v in sorted(self.nodes, key=lambda v: (v.vt_s, v.vid)):
            self.node_of.setdefault(v.uid, v)
        self.versions_of: dict[str, list[NodeVersion]] = {}
        for v in self.nodes:
            self.versions_of.setdefault(v.uid, []).append(v)
        self.fresh = 0

    # -- the domain ---------------------------------------------------------
    def _incident(self, uid: str, direction: str, rel: str | None) -> list[tuple]:
        """`(edge, other end)` as an expansion reads them: out, then in."""
        out = []
        if direction in ("out", "both"):
            out += [(e, e.dst) for e in self.edges if e.src == uid and _rel_ok(e, rel)]
        if direction in ("in", "both"):
            out += [(e, e.src) for e in self.edges if e.dst == uid and _rel_ok(e, rel)]
        return out

    @staticmethod
    def _fits(v: NodeVersion | None, node: dict[str, Any] | None) -> bool:
        """A node constraint: a version exists, and its labels and uids fit."""
        if v is None:
            return False
        if node and node.get("labels") and v.label not in node["labels"]:
            return False
        if node and node.get("uids") and v.uid not in node["uids"]:
            return False
        return True

    @staticmethod
    def _uid(b: Any) -> str | None:
        return None if b is None or b[1] is None else b[1].uid

    @staticmethod
    def _same_edge(r: dict, singles: list[str], e: EdgeVersion) -> bool:
        return any(r.get(s) is not None and r[s][1] is not None and r[s][1].eid == e.eid
                   for s in singles)

    # -- one pattern --------------------------------------------------------
    def _pattern(self, rows: list[dict], order: list[str], nodes: list[dict],
                 steps: list[dict], where: list[Any], shared: set[str],
                 succ: list[dict] | None = None) -> list[dict]:
        by_var = {n["var"]: n for n in nodes}
        bound = set(shared)
        if not shared:
            anchor = next((n for n in nodes if n.get("uids")),
                          next((n for n in nodes if n.get("labels")), nodes[0]))
            pool = list(self.node_of.values()) if self.snapshot else self.nodes
            rows = [{anchor["var"]: ("node", v, None)} for v in pool if self._fits(v, anchor)]
            bound.add(anchor["var"])
            order.append(anchor["var"])
        singles: list[str] = []
        todo = list(steps)
        while todo:
            st = next(s for s in todo if s["from"] in bound or s["to"] in bound)
            todo.remove(st)
            pre_order, pre_bound = list(order), set(bound)
            hops = st.get("hops") or {"exact": 1}
            single = hops == {"exact": 1}
            ev = st.get("var")
            if single and ev is None:
                self.fresh += 1
                ev = f"_e{self.fresh}"
            ordered = "regex" not in st and ("order" in st or "edge_where" in st or
                                             ("truncate" in st and not single))
            if self.snapshot and ordered:
                src, dst = st["from"], st["to"]
                direction, tord = st.get("dir", "out"), st.get("order", "none")
                if src not in bound:
                    src, dst = dst, src
                    direction = {"out": "in", "in": "out"}[direction]
                    tord = {"none": "none", "strict_increasing": "strict_decreasing",
                            "increasing": "decreasing", "strict_decreasing": "strict_increasing",
                            "decreasing": "increasing"}[tord]
                closing = dst in bound
                rows = [r2 for r in rows for r2 in self._ordered(r, st, src, dst, direction,
                                                                  tord, closing, by_var.get(dst))]
                if not closing:
                    bound.add(dst)
                    order.append(dst)
                continue
            if self.snapshot and "regex" in st:
                src, dst, spec = st["from"], st["to"], st["regex"]
                if src not in bound:
                    src, dst, spec = dst, src, _rx_reverse(spec)
                closing = dst in bound
                rows = [r2 for r in rows for r2 in self._walk(r, src, dst, spec, closing,
                                                               by_var.get(dst))]
                if not closing:
                    bound.add(dst)
                    order.append(dst)
                continue
            if self.snapshot:
                src, dst, direction = st["from"], st["to"], st.get("dir", "out")
                if src not in bound:
                    src, dst = dst, src
                    direction = {"out": "in", "in": "out", "both": "both"}[direction]
                closing = dst in bound
                rows = [r2 for r in rows for r2 in self._expand(
                    r, src, dst, direction, st.get("rel_type"), hops,
                    ev if single else None, closing, by_var.get(dst), singles)]
                if not closing:
                    bound.add(dst)
                    order.append(dst)
                if single:
                    order.append(ev)
            else:
                rows = [r2 for r in rows for r2 in self._join_step(r, st, ev, bound, by_var,
                                                                   singles)]
                order.append(ev)
                for v in (st["from"], st["to"]):
                    if v not in bound:
                        bound.add(v)
                        order.append(v)
            if single:
                singles.append(ev)
                bound.add(ev)
            if single and "truncate" in st:
                rows = self._truncate(rows, st, ev, bound, pre_order, pre_bound, where, order)
        for e in succ or []:
            rows = [r2 for r in rows for r2 in self._succ(r, e)]
            order.append(e["var"])
            bound.add(e["var"])
        return [r for r in rows if all(self._eval(w, r) is True for w in where)]

    def _succ(self, r: dict, e: dict):
        """The next (or previous) visible version of the same identity, by
        (axis start, version id)."""
        b = r.get(e["of"])
        if b is None or b[1] is None:
            return
        edge = b[0] == "edge"
        ver = b[1]
        ident = ver.eid if edge else ver.uid
        pool = self.edges if edge else self.nodes
        valid = e.get("axis", "valid") == "valid"
        chain = sorted((x for x in pool if (x.eid if edge else x.uid) == ident),
                       key=lambda x: (x.vt_s if valid else x.tt_s, x.vid))
        pos = next((i for i, x in enumerate(chain) if x.vid == ver.vid), None)
        if pos is None:
            return
        j = pos + (1 if e.get("dir", "next") == "next" else -1)
        if 0 <= j < len(chain):
            yield {**r, e["var"]: ("edge" if edge else "node", chain[j], None)}

    def _truncate(self, rows: list[dict], st: dict, ev: str, bound: set[str],
                  before: list[str], pre_bound: set[str], where: list[Any],
                  order_now: list[str]) -> list[dict]:
        """A single hop's truncation: rows grouped by the step's bound end and
        the versions bound before it, the first `limit` under the keys (the
        edge's version id last), after every predicate already decidable."""
        rows = [r for r in rows if all(self._eval(w, r) is True for w in where
                                       if _expr_vars(w) <= bound)]
        start = st["from"] if st["from"] in pre_bound else st["to"]
        t = st["truncate"]
        keys = [(o["expr"], o.get("dir", "asc"), o.get("nulls", "last")) for o in t["order_by"]]
        new = [v for v in order_now if v not in before]
        keys += [({"col": f"{v}.vid"}, "asc", "first") for v in new]
        kept, count = [], {}
        for r in self._sort(rows, keys):
            part = (self._uid(r[start]),) + tuple(
                None if r.get(v) is None or r[v][1] is None else r[v][1].vid for v in before)
            if count.get(part, 0) < t["limit"]:
                count[part] = count.get(part, 0) + 1
                kept.append(r)
        return kept

    def _ordered(self, r: dict, st: dict, src: str, dst: str, direction: str, tord: str,
                 closing: bool, node: dict | None):
        """An ordered variable-length step by exhaustive search over (node,
        last time) states, level by level: a node's distance is the first
        level it appears at."""
        u = self._uid(r[src])
        if u is None:
            return
        ew = st.get("edge_where") or {"var": "_t", "where": []}
        trunc = st.get("truncate")
        adj: dict[str, list[tuple[str, int]]] = {}
        for e in self.edges:
            if e.rel_type != st["rel_type"]:
                continue
            probe = {ew["var"]: ("edge", e, None)}
            if not all(self._eval(w, probe) is True for w in ew["where"]):
                continue
            a, b = (e.src, e.dst) if direction == "out" else (e.dst, e.src)
            key = self._eval(trunc["order_by"][0]["expr"], probe) if trunc else None
            adj.setdefault(a, []).append((key, e.vt_s, b))
        if trunc:
            desc = trunc["order_by"][0].get("dir", "asc") == "desc"
            for a, lst in adj.items():
                with_key = sorted((x for x in lst if x[0] is not None),
                                  key=lambda x: (-x[0] if desc else x[0], x[1], x[2]))
                no_key = sorted((x for x in lst if x[0] is None), key=lambda x: (x[1], x[2]))
                adj[a] = (with_key + no_key)[: trunc["limit"]]
        hops = st.get("hops") or {"exact": 1}
        lo, hi = ((hops["exact"], hops["exact"]) if "exact" in hops
                  else (hops["min"], hops.get("max")))
        dec = tord in ("strict_decreasing", "decreasing")
        strict = tord.startswith("strict")
        dist = {u: 0}
        level = {(u, None)}
        seen = set(level)
        depth = 0
        while level and (hi is None or depth < hi):
            depth += 1
            nxt = set()
            for x, last in level:
                for _k, t, y in adj.get(x, []):
                    if tord != "none" and last is not None:
                        if dec and not (t < last if strict else t <= last):
                            continue
                        if not dec and not (t > last if strict else t >= last):
                            continue
                    state = (y, t if tord != "none" else None)
                    if state not in seen:
                        seen.add(state)
                        nxt.add(state)
                        dist.setdefault(y, depth)
            level = nxt
        for o, d in sorted(dist.items()):
            if d < lo or (hi is not None and d > hi):
                continue
            if closing:
                if o == self._uid(r[dst]):
                    yield dict(r)
                continue
            v = self.node_of.get(o)
            if self._fits(v, node):
                yield {**r, dst: ("target", v, d)}

    def _expand(self, r: dict, src: str, dst: str, direction: str, rel: str | None,
                hops: dict, ev: str | None, closing: bool, node: dict | None,
                singles: list[str]):
        u = self._uid(r[src])
        if u is None:
            return
        if ev is not None:                                   # one hop, edge bound
            for e, other in self._incident(u, direction, rel):
                if self._same_edge(r, singles, e):
                    continue
                if closing:
                    if other == self._uid(r[dst]):
                        yield {**r, ev: ("edge", e, None)}
                    continue
                v = self.node_of.get(other)
                if self._fits(v, node):
                    yield {**r, dst: ("node", v, None), ev: ("edge", e, None)}
            return
        if "exact" in hops:                                  # k-walks, with multiplicity
            ends = [u]
            for _ in range(hops["exact"]):
                ends = [o for x in ends for _e, o in self._incident(x, direction, rel)]
            targets = [(o, None) for o in ends]
            kind = "node"
        else:                                                # shortest distance band
            lo, hi = hops["min"], hops.get("max")
            dist = {u: 0}
            frontier = [u]
            depth = 0
            while frontier and (hi is None or depth < hi):
                depth += 1
                nxt = []
                for x in frontier:
                    for _e, o in self._incident(x, direction, rel):
                        if o not in dist:
                            dist[o] = depth
                            nxt.append(o)
                frontier = nxt
            targets = [(o, d) for o, d in dist.items() if d >= lo]
            kind = "target"
        for o, d in targets:
            if closing:
                if o == self._uid(r[dst]):
                    yield dict(r)
                continue
            v = self.node_of.get(o)
            if self._fits(v, node):
                yield {**r, dst: (kind, v, d)}

    def _reach(self, spec: dict, start: set[str]) -> set[str]:
        """The nodes a path spelling `spec` reaches from `start`, by the
        expression's set semantics (compositions, unions, closures)."""
        if "sym" in spec:
            direction = "in" if spec.get("inv") else "out"
            return {o for x in start for _e, o in self._incident(x, direction, spec["sym"])}
        if "seq" in spec:
            cur = set(start)
            for p in spec["seq"]:
                cur = self._reach(p, cur)
            return cur
        if "alt" in spec:
            return set().union(*(self._reach(p, start) for p in spec["alt"]))
        if "opt" in spec:
            return set(start) | self._reach(spec["opt"], start)
        inner = spec.get("star") or spec.get("plus")
        base = set(start) if "star" in spec else self._reach(inner, start)
        seen, frontier = set(base), set(base)
        while frontier:
            frontier = self._reach(inner, frontier) - seen
            seen |= frontier
        return seen

    def _walk(self, r: dict, src: str, dst: str, spec: dict, closing: bool,
              node: dict | None):
        u = self._uid(r[src])
        if u is None:
            return
        for o in sorted(self._reach(spec, {u})):
            if closing:
                if o == self._uid(r[dst]):
                    yield dict(r)
                continue
            v = self.node_of.get(o)
            if self._fits(v, node):
                yield {**r, dst: ("node", v, None)}

    def _join_step(self, r: dict, st: dict, ev: str, bound: set[str], by_var: dict,
                   singles: list[str]):
        direction = st.get("dir", "out")
        rel = st.get("rel_type")
        for d in (("out", "in") if direction == "both" else (direction,)):
            tail, head = ((st["from"], st["to"]) if d == "out" else (st["to"], st["from"]))
            for e in self.edges:
                if not _rel_ok(e, rel) or self._same_edge(r, singles, e):
                    continue
                ends = {tail: e.src, head: e.dst}
                if any(var in bound and self._uid(r[var]) != uid for var, uid in ends.items()):
                    continue
                base = {**r, ev: ("edge", e, None)}
                free = [v for v in (tail, head) if v not in bound]
                if not free:
                    yield base
                    continue
                for v in self.versions_of.get(ends[free[0]], []):
                    if self._fits(v, by_var.get(free[0])):
                        yield {**base, free[0]: ("node", v, None)}

    # -- expressions --------------------------------------------------------
    def _column(self, r: dict, name: str) -> Any:
        if "." not in name:
            return r["__out"][name]
        alias, _, fld = name.partition(".")
        if alias in r.get("__lk", {}):
            return r["__lk"][alias].get(fld)
        if name in r.get("__seq", {}):
            return r["__seq"][name]
        var, fld = name.split(".", 1)
        b = r.get(var)
        if b is None or b[1] is None:
            return None
        _kind, v, depth = b
        if fld == "depth":
            return depth
        if fld == "tt_e":
            return v.tt_e if v.tt_e <= self.T else OPEN_END
        return getattr(v, fld)

    def _eval(self, e: Any, r: dict) -> Any:
        if "lit" in e:
            return e["lit"]
        if "col" in e:
            return self._column(r, e["col"])
        if "tprop" in e or "prop" in e:
            var, key = e["tprop"] if "tprop" in e else e["prop"]
            b = r.get(var)
            if b is None or b[1] is None or key not in b[1].props:
                return None
            value = b[1].props[key]
            if "prop" in e:
                return value
            is_int = isinstance(value, int) and not isinstance(value, bool)
            tau = e["tau"]
            if tau in ("int", "ts"):
                if is_int and not -(2 ** 63) <= value < 2 ** 63:
                    raise InvalidArgError("typed read of an integer outside int64")
                return value if is_int else None
            if tau == "float":
                return float(value) if (is_int or isinstance(value, float)) else None
            if tau == "str":
                return value if isinstance(value, str) else None
            return value if isinstance(value, bool) else None
        if "cmp" in e:
            a, b = self._eval(e["l"], r), self._eval(e["r"], r)
            if a is None or b is None:
                return None
            return {"=": a == b, "!=": a != b, "<": a < b, "<=": a <= b,
                    ">": a > b, ">=": a >= b}[e["cmp"]]
        if "bool" in e:
            a, b = self._eval(e["l"], r), self._eval(e["r"], r)
            if a is None or b is None:
                return None
            return bool(a and b) if e["bool"] == "and" else bool(a or b)
        if "not" in e:
            a = self._eval(e["not"], r)
            return None if a is None else not a
        if "is_null" in e:
            return self._eval(e["is_null"], r) is None
        if "coalesce" in e:
            a = self._eval(e["coalesce"][0], r)
            return a if a is not None else self._eval(e["coalesce"][1], r)
        if "if" in e:
            c = self._eval(e["if"][0], r)
            if c is None:
                return None
            return self._eval(e["if"][1] if c else e["if"][2], r)
        if "starts_with" in e:
            a, b = (self._eval(x, r) for x in e["starts_with"])
            if a is None or b is None:
                return None
            if not isinstance(a, str) or not isinstance(b, str):
                raise InvalidArgError("starts_with over a non-string value")
            return a.startswith(b)
        if "cast" in e:
            a = self._eval(e["cast"], r)
            if a is None:
                return None
            to = e["to"]
            if to == "str":
                return str(a)
            if to == "float":
                return float(a)
            if isinstance(a, (bool, float)):
                raise InvalidArgError("cast of a non-integer to int")
            if isinstance(a, int):
                return a
            text = str(a)
            body = text[1:] if text.startswith("-") else text
            if not body.isdigit() or (len(body) > 1 and body[0] == "0") or text == "-0":
                raise InvalidArgError(f"cast of {text!r} to int")
            return int(text)
        if "arith" in e:
            a, b = self._eval(e["l"], r), self._eval(e["r"], r)
            if a is None or b is None:
                return None
            if e["arith"] == "+":
                return a + b
            if e["arith"] == "-":
                return a - b
            if e["arith"] == "*":
                return a * b
            if e["arith"] in ("/", "floor_div"):
                if b == 0:
                    raise InvalidArgError("division by zero")
                if e["arith"] == "floor_div":
                    out = a // b
                elif isinstance(a, int) and isinstance(b, int):
                    q, rem = divmod(a, b)          # the blessed rule, always a float
                    out = float(q) if rem == 0 else float(q) + rem / b
                else:
                    out = a / b
                if isinstance(out, float) and not math.isfinite(out):
                    raise InvalidArgError("a non-finite quotient")
                return out
            raise InvalidArgError(f"oracle: arithmetic {e['arith']!r} is not a match form")
        if "fn" in e:
            a = self._eval(e["arg"], r)
            if a is None:
                return None
            fn = e["fn"]
            if fn == "abs":
                return abs(a)
            if fn == "sqrt":
                if a < 0:
                    raise InvalidArgError("sqrt of a negative value")
                return math.sqrt(a)
            x = float(a)
            return {"round": round, "floor": math.floor, "ceil": math.ceil}[fn](x)
        if "calendar" in e:
            a = self._eval(e["arg"], r)
            b = self._eval(e["other"], r) if "other" in e else 0
            if a is None or b is None:
                return None
            ya, ma, da = _utc_ymd(a)
            fn = e["calendar"]
            if fn == "months_between":
                yb, mb, _ = _utc_ymd(b)
                return (yb - ya) * 12 + (mb - ma)
            return {"year": ya, "month": ma, "day": da, "month_day": ma * 100 + da}[fn]
        if "strfn" in e:
            vals = [self._eval(x, r) for x in e["args"]]
            if any(v is None for v in vals):
                return None
            if not isinstance(vals[0], str):
                raise InvalidArgError("a string function over a non-string value")
            fn = e["strfn"]
            if fn == "contains":
                return vals[1] in vals[0]
            if fn == "like":
                return _like(vals[0], vals[1])
            first = max(vals[1], 1)                 # SQL: positions start..start+len-1
            last = vals[1] + vals[2] - 1
            return "".join(ch for pos, ch in enumerate(vals[0], 1) if first <= pos <= last)
        if "total" in e:
            return r["__tot"][e["total"]]
        raise InvalidArgError(f"oracle: unknown expression {sorted(e)}")

    @staticmethod
    def _is_json(e: Any) -> bool:
        """Whether an output expression is statically `json` (it is then left
        out of the order's completion, as the compiler leaves it out)."""
        if "prop" in e:
            return True
        if "lit" in e:
            return e["lit"] is None and e.get("tau") is None
        if "coalesce" in e:
            return any(_MatchOracle._is_json(x) for x in e["coalesce"])
        if "if" in e:
            return any(_MatchOracle._is_json(x) for x in e["if"][1:])
        return False

    def _shared_instant(self, pat: dict[str, Any], axes: list[str]) -> bool:
        """        by instants. Fixed single hops keep the version-join route above."""
        scope = self.args["scope"]
        vt = scope["vt"]
        if "transaction" in axes:
            return False
        width_one = "window" in vt and vt.get("mode", "overlap") == "overlap" and \
            vt["window"]["t_b"] - vt["window"]["t_a"] == 1
        if "instant" in vt or width_one:
            if scope["belief"] == "current":
                return False
        elif vt.get("mode", "overlap") != "overlap" or "valid" not in axes:
            return False
        multi = any((st.get("hops") or {"exact": 1}) != {"exact": 1}
                    for st in pat.get("steps", []) if "regex" not in st)
        return multi or bool(pat.get("optional") or pat.get("not"))

    def _shared_instant_rows(self, pat: dict[str, Any], where: list[Any], deferred: list[Any],
                 axes: list[str], order: list[str]) -> list[dict]:
        """The union over the elementary instants of the window (between
        consecutive version endpoints nothing changes) of the instant
        answers, one row per version tuple, each walk target at its least
        depth over the instants."""
        vt = self.args["scope"]["vt"]
        if "instant" in vt:
            va, vb = vt["instant"], vt["instant"] + 1
        else:
            va, vb = vt["window"]["t_a"], vt["window"]["t_b"]
        points = {va}
        for v in list(self.nodes) + list(self.edges):
            for x in (v.vt_s, v.vt_e):
                if va < x < vb:
                    points.add(x)
        merged: dict[tuple, dict] = {}
        for t in sorted(points):
            self._t = t
            self._at_nodes: dict[str, list] = {}
            for v in sorted(self.nodes, key=lambda v: v.vid):
                if v.vt_s <= t < v.vt_e:
                    self._at_nodes.setdefault(v.uid, []).append(v)
            self._at_edges = [e for e in self.edges if e.vt_s <= t < e.vt_e]
            rows = self._shared_instant_match({}, pat.get("nodes", []), pat.get("steps", []), where, set(), "m")
            for j, sub in enumerate(pat.get("optional", [])):
                rows = self._shared_instant_sub(rows, sub, True, f"o{j}")
            for j, sub in enumerate(pat.get("not", [])):
                rows = self._shared_instant_sub(rows, sub, False, f"x{j}")
            for r in rows:
                key = tuple(sorted((k, b[1].vid if b is not None and b[1] is not None else None)
                                   for k, b in r.items() if not k.startswith("__"))) + \
                    tuple(sorted(r.get("__o", {}).items()))
                have = merged.get(key)
                if have is None:
                    merged[key] = r
                    continue
                for k, b in r.items():
                    if k.startswith("__"):
                        continue
                    if b is not None and b[0] == "target" and b[2] < have[k][2]:
                        have[k] = b
        out = list(merged.values())
        names = [k for k in (out[0] if out else {}) if not k.startswith("__")]
        order.extend(sorted(names))
        frag = bool(pat.get("optional") or pat.get("not")) or any(
            "min" in (st.get("hops") or {}) for st in pat.get("steps", []))
        if axes and not frag:
            kept = []
            for r in out:
                vs = [b[1] for k, b in r.items()
                      if not k.startswith("__") and b is not None and b[1] is not None]
                r["__seq"] = {"valid.s": max(v.vt_s for v in vs),
                              "valid.e": min(v.vt_e for v in vs)}
                if all(self._eval(w, r) is True for w in deferred):
                    kept.append(r)
            out = kept
        return out

    def _shared_instant_versions(self, uid: str, node: dict | None) -> list:
        return [v for v in self._at_nodes.get(uid, []) if self._fits(v, node)]

    def _shared_instant_match(self, seed: dict, nodes: list[dict], steps: list[dict], where: list[Any],
                  shared: set[str], tag: str) -> list[dict]:
        """The instant answer at `self._t` of one pattern: every admitted
        version valid at the instant, walks through nodes that exist then."""
        by_var = {n["var"]: n for n in nodes}
        bound = set(seed)
        if not seed:
            anchor = next((n for n in nodes if n.get("uids")),
                          next((n for n in nodes if n.get("labels")), nodes[0]))
            rows = [{anchor["var"]: ("node", v, None)}
                    for vs in self._at_nodes.values() for v in vs if self._fits(v, anchor)]
            bound.add(anchor["var"])
        else:
            rows = [dict(seed)]
        singles: list[str] = []
        todo = list(enumerate(steps))
        while todo:
            pos = next(k for k, (_i, st) in enumerate(todo)
                       if st["from"] in bound or st["to"] in bound)
            i, st = todo.pop(pos)
            hops = st.get("hops") or {"exact": 1}
            a, b, direction = st["from"], st["to"], st.get("dir", "out")
            if a not in bound:
                a, b = b, a
                direction = {"out": "in", "in": "out", "both": "both"}[direction]
            closing = b in bound
            rel = st.get("rel_type")
            if hops == {"exact": 1}:
                ev = st.get("var") or f"_{tag}e{i}"
                rows = [x for r in rows for x in self._shared_instant_hop(r, a, b, direction, rel, ev,
                                                                closing, by_var.get(b),
                                                                singles)]
                singles.append(ev)
                bound.add(ev)
            elif "exact" in hops:
                for h in range(hops["exact"]):
                    last = h == hops["exact"] - 1
                    nxt = b if last else f"_{tag}n{i}_{h}"
                    ev = f"_{tag}w{i}_{h}"
                    rows = [x for r in rows for x in self._shared_instant_hop(
                        r, a, nxt, direction, rel, ev, closing and last,
                        by_var.get(nxt) if last else None, [])]
                    bound |= {nxt, ev}
                    a = nxt
            else:
                rows = [x for r in rows for x in self._shared_instant_walk(r, a, b, direction, rel, hops,
                                                                 closing, by_var.get(b))]
            bound.add(b)
        return [r for r in rows if all(self._eval(w, r) is True for w in where)]

    def _shared_instant_hop(self, r: dict, a: str, b: str, direction: str, rel: Any, ev: str,
                closing: bool, node: dict | None, singles: list[str]):
        u = self._uid(r[a])
        for d in (("out", "in") if direction == "both" else (direction,)):
            for e in self._at_edges:
                if not _rel_ok(e, rel) or self._same_edge(r, singles, e):
                    continue
                near, far = (e.src, e.dst) if d == "out" else (e.dst, e.src)
                if near != u:
                    continue
                o = {**r.get("__o", {}), ev: d} if direction == "both" else r.get("__o", {})
                if closing:
                    if far == self._uid(r[b]):
                        yield {**r, ev: ("edge", e, None), "__o": o}
                    continue
                for v in self._shared_instant_versions(far, node):
                    yield {**r, ev: ("edge", e, None), b: ("node", v, None), "__o": o}

    def _shared_instant_walk(self, r: dict, a: str, b: str, direction: str, rel: Any, hops: dict,
                 closing: bool, node: dict | None):
        u = self._uid(r[a])
        if u not in self._at_nodes:
            return
        lo, hi = hops["min"], hops.get("max")
        dist = {u: 0}
        frontier = [u]
        depth = 0
        while frontier and (hi is None or depth < hi):
            depth += 1
            nxt = []
            for x in frontier:
                for e in self._at_edges:
                    if not _rel_ok(e, rel):
                        continue
                    for d in (("out", "in") if direction == "both" else (direction,)):
                        near, far = (e.src, e.dst) if d == "out" else (e.dst, e.src)
                        if near == x and far not in dist and far in self._at_nodes:
                            dist[far] = depth
                            nxt.append(far)
            frontier = nxt
        for o, dd in dist.items():
            if dd < lo:
                continue
            if closing:
                if o == self._uid(r[b]):
                    yield dict(r)
                continue
            for v in self._shared_instant_versions(o, node):
                yield {**r, b: ("target", v, dd)}

    def _shared_instant_sub(self, rows: list[dict], sub: dict, keep: bool, tag: str) -> list[dict]:
        nodes, steps = sub.get("nodes", []), sub.get("steps", [])
        used = ({n["var"] for n in nodes} | {s["from"] for s in steps}
                | {s["to"] for s in steps})
        out: list[dict] = []
        for r in rows:
            shared = {v for v in used if v in r and r[v] is not None and r[v][0] != "edge"}
            got = self._shared_instant_match({v: r[v] for v in shared},
                                 [n for n in nodes if n["var"] not in shared], steps,
                                 sub.get("where", []), shared, tag)
            if not keep:
                if not got:
                    out.append(r)
            elif got:
                out += [{**r, **{k: v for k, v in g.items() if k not in shared},
                         "__o": {**r.get("__o", {}), **g.get("__o", {})}} for g in got]
            else:
                out.append(dict(r))
        return out

    # -- sub-patterns -------------------------------------------------------
    def _sub(self, rows: list[dict], order: list[str], sub: dict, keep: bool) -> list[dict]:
        nodes, steps = sub.get("nodes", []), sub.get("steps", [])
        used = ({n["var"] for n in nodes} | {s["from"] for s in steps}
                | {s["to"] for s in steps})
        out: list[dict] = []
        sub_order: list[str] = []
        for r in rows:
            shared = {v for v in used if v in r and r[v][0] != "edge"}
            seed = {v: r[v] for v in shared}
            sub_order = []
            got = self._pattern([seed], sub_order,
                                [n for n in nodes if n["var"] not in shared], steps,
                                sub.get("where", []), shared)
            if not keep:
                if not got:
                    out.append(r)
            elif got:
                out += [{**r, **{k: v for k, v in g.items() if k not in shared}} for g in got]
            else:
                out.append(dict(r))
        if keep:
            order.extend(sub_order)
        return out

    # -- the tail -----------------------------------------------------------
    def _sort(self, rows: list[dict], keys: list[tuple[Any, str, str]]) -> list[dict]:
        import functools

        def cmp(a: dict, b: dict) -> int:
            for expr, direction, nulls in keys:
                x, y = self._eval(expr, a), self._eval(expr, b)
                if x is None and y is None:
                    continue
                if x is None or y is None:
                    first = nulls == "first"
                    return (-1 if first else 1) if x is None else (1 if first else -1)
                if x == y:
                    continue
                c = -1 if x < y else 1
                return -c if direction == "desc" else c
            return 0

        return sorted(rows, key=functools.cmp_to_key(cmp))

    @staticmethod
    def _fold(fn: str, vals: list[Any]) -> Any:
        import math

        if fn == "count_distinct":
            return len(set(vals))            # an empty group of values counts 0
        if not vals:
            return None
        ints = all(isinstance(v, int) and not isinstance(v, bool) for v in vals)
        if fn == "sum":
            return sum(vals) if ints else math.fsum(vals)
        if fn == "min":
            return min(vals)
        if fn == "max":
            return max(vals)
        if fn == "mean":
            return float(blessed_mean(vals)) if ints else math.fsum(vals) / len(vals)
        raise InvalidArgError(f"oracle: aggregate {fn!r}")

    def _collect(self, a: dict, pool: list[dict]) -> list[Any]:
        """The group's elements: rows with a non-null component, sorted by
        the order keys then the components (nulls first), once each when
        distinct, at most `limit`."""
        specs = a["of"] if isinstance(a["of"], list) else [a["of"]]
        elems = []
        for m in pool:
            comps = [self._eval(x, m) for x in specs]
            if all(c is None for c in comps):
                continue
            keys = [self._eval(o["expr"], m) for o in a.get("order_by", [])]
            elems.append((keys, comps))

        def one(x: Any, y: Any, desc: bool, nulls_first: bool) -> int:
            if x is None or y is None:
                if x is None and y is None:
                    return 0
                return -1 if (x is None) == nulls_first else 1
            if x == y:
                return 0
            return (1 if x < y else -1) if desc else (-1 if x < y else 1)

        def cmp(p: tuple, q: tuple) -> int:
            for o, x, y in zip(a.get("order_by", []), p[0], q[0]):
                c = one(x, y, o.get("dir", "asc") == "desc", o.get("nulls", "last") == "first")
                if c:
                    return c
            for x, y in zip(p[1], q[1]):
                c = one(x, y, False, True)
                if c:
                    return c
            return 0

        import functools

        out: list[Any] = []
        for _keys, comps in sorted(elems, key=functools.cmp_to_key(cmp)):
            e = comps[0] if len(comps) == 1 else comps
            if a.get("distinct") and e in out:
                continue
            out.append(e)
            if a.get("limit") and len(out) >= a["limit"]:
                break
        return out

    def _aggregate(self, rows: list[dict], g: dict) -> tuple[list[dict], list[str], set]:
        groups: dict[tuple, list[dict]] = {}
        for r in rows:
            key = tuple(self._eval(b["expr"], r) for b in g["by"])
            groups.setdefault(key, []).append(r)
        names = [b["as"] for b in g["by"]] + [a["as"] for a in g["aggs"]]
        json_cols = {b["as"] for b in g["by"] if self._is_json(b["expr"])}
        json_cols |= {a["as"] for a in g["aggs"]
                      if a["fn"] in ("min", "max") and self._is_json(a["of"])}
        json_cols |= {a["as"] for a in g["aggs"] if a["fn"] == "collect"}
        if any(a.get("distinct_on") for a in g["aggs"]) and \
                any(v is None for key in groups for v in key):
            raise InvalidArgError("a null group key cannot be joined back")
        out = []
        for key, members in groups.items():
            row = {b["as"]: k for b, k in zip(g["by"], key)}
            for a in g["aggs"]:
                fn, of = a["fn"], a.get("of")
                if a.get("distinct_on"):
                    seen: dict[tuple, Any] = {}
                    for m in members:
                        ident = tuple(
                            None if m.get(v) is None or m[v][1] is None
                            else (m[v][1].eid if m[v][0] == "edge" else m[v][1].uid)
                            for v in a["distinct_on"])
                        if a.get("where") is not None and self._eval(a["where"], m) is not True:
                            continue
                        val = None if of is None else self._eval(of, m)
                        # one value per identity: the least non-null one, as the
                        # compiled plan folds it (versions of one identity may differ)
                        if ident not in seen or seen[ident] is None or \
                                (val is not None and val < seen[ident]):
                            seen[ident] = val if ident not in seen or val is not None \
                                else seen[ident]
                    vals = [v for v in seen.values() if v is not None]
                    row[a["as"]] = len(seen) if fn == "count" else self._fold(fn, vals)
                    continue
                pool = members if a.get("where") is None else \
                    [m for m in members if self._eval(a["where"], m) is True]
                if fn == "collect":
                    row[a["as"]] = self._collect(a, pool)
                    continue
                if fn == "count" and of is None:
                    row[a["as"]] = len(pool)
                    continue
                vals = [v for v in (self._eval(of, m) for m in pool) if v is not None]
                row[a["as"]] = len(vals) if fn == "count" else self._fold(fn, vals)
            out.append({"__out": row})
        if g.get("project"):
            tot: dict[str, Any] = {}
            for c in {c for p in g["project"] for c in _totals_in(p["expr"])}:
                vals = [o["__out"][c] for o in out if o["__out"][c] is not None]
                tot[c] = self._fold("sum", vals)
            for o in out:
                o["__tot"] = tot
                for p in g["project"]:
                    o["__out"][p["as"]] = self._eval(p["expr"], o)
            names += [p["as"] for p in g["project"]]
        if g.get("having") is not None:
            out = [r for r in out if self._eval(g["having"], r) is True]
        if g.get("then") is not None:
            return self._aggregate(out, g["then"])
        return out, names, json_cols

    def run(self) -> dict[str, Any]:
        from gnomon.temporal.algebra import _canonicalize_floats

        args = self.args
        if "union" in args:
            rows = []
            for br in args["union"]:
                for r in self._rows(br["pattern"])[0]:
                    rows.append({"__out": {b["as"]: self._eval(b["expr"], r)
                                           for b in br["rows"]}})
            order: list[str] = []
            from_union = True
        else:
            rows, order = self._rows(args["pattern"])
            from_union = False
        ret = args["return"]
        if "rows" in ret:
            columns = [b["as"] for b in ret["rows"]]
            for r in rows:
                r["__out"] = {b["as"]: self._eval(b["expr"], r) for b in ret["rows"]}
            json_cols = {b["as"] for b in ret["rows"] if self._is_json(b["expr"])}
            vids = [] if from_union else [f"{v}.vid" for v in order]
        else:
            rows, columns, json_cols = self._aggregate(rows, ret["group"])
            vids = []
        top = ret.get("top_per")
        if top is not None:
            keys = [(o["expr"], o.get("dir", "asc"), o.get("nulls", "last"))
                    for o in top["order_by"]]
            keys += [({"col": c}, "asc", "first") for c in columns if c not in json_cols]
            keys += [({"col": v}, "asc", "first") for v in vids]
            taken: dict[tuple, int] = {}
            kept = []
            for r in self._sort(rows, keys):
                part = tuple(self._eval(e, r) for e in top["by"])
                if taken.get(part, 0) < top["k"]:
                    taken[part] = taken.get(part, 0) + 1
                    kept.append(r)
            rows = kept
        return self._tail(rows, columns, json_cols, vids, _canonicalize_floats)

    def _rows(self, pat: dict[str, Any], axes: list[str] | None = None
              ) -> tuple[list[dict], list[str]]:
        """One pattern's binding rows (sequenced projection, sub-patterns and
        lookups applied), and its variables in binding order."""
        args = self.args
        order: list[str] = []
        if axes is None:
            axes = args["scope"].get("co_valid") or []
        seq = {"valid": ("valid.s", "valid.e"), "transaction": ("belief.s", "belief.e")}
        seq_names = {c for ax in axes for c in seq[ax]}

        def reads_seq(e: Any) -> bool:
            if isinstance(e, dict):
                return (e.get("col") in seq_names) or any(reads_seq(v) for k, v in e.items()
                                                          if k != "lit")
            if isinstance(e, list):
                return any(reads_seq(v) for v in e)
            return False
        where = pat.get("where", [])
        looks = {lk["as"] for lk in pat.get("lookups", [])}
        late = [w for w in where if any(c.split(".", 1)[0] in looks
                                        for c in _expr_cols(w) if "." in c)]
        where = [w for w in where if w not in late]
        deferred = [w for w in where if reads_seq(w)]
        if self._shared_instant(pat, axes):
            rows = self._shared_instant_rows(pat, [w for w in where if w not in deferred], deferred,
                                 axes, order)
            for lk in pat.get("lookups", []):
                rows = self._lookup(rows, lk)
            rows = [r for r in rows if all(self._eval(w, r) is True for w in late)]
            return rows, order
        rows = self._pattern([], order, pat["nodes"], pat.get("steps", []),
                             [w for w in where if w not in deferred], set(), pat.get("succ"))
        if axes:
            kept = []
            for r in rows:
                cv: dict[str, Any] = {}
                ok = True
                for ax in axes:
                    starts, ends = [], []
                    for b in r.values():
                        v = b[1]
                        if v is None:
                            continue
                        if ax == "valid":
                            starts.append(v.vt_s)
                            ends.append(v.vt_e)
                        else:
                            starts.append(v.tt_s)
                            ends.append(v.tt_e if v.tt_e <= self.T else OPEN_END)
                    s_, e_ = max(starts), min(ends)
                    cv[seq[ax][0]], cv[seq[ax][1]] = s_, e_
                    ok = ok and s_ < e_
                if ok:
                    r["__seq"] = cv
                    kept.append(r)
            rows = [r for r in kept if all(self._eval(w, r) is True for w in deferred)]
        for sub in pat.get("optional", []):
            rows = self._sub(rows, order, sub, True)
        for sub in pat.get("not", []):
            rows = self._sub(rows, order, sub, False)
        for lk in pat.get("lookups", []):
            rows = self._lookup(rows, lk)
        rows = [r for r in rows if all(self._eval(w, r) is True for w in late)]
        return rows, order

    def _lookup(self, rows: list[dict], lk: dict) -> list[dict]:
        """The sub-match's groups by key; each row takes the group its `on`
        value names (none: nulls, or dropped for an inner lookup)."""
        sub_rows, _ = self._rows(lk["match"]["pattern"], axes=[])
        groups, names, _json = self._aggregate(sub_rows, lk["match"]["group"])
        table = {g["__out"][lk["key"]]: g["__out"] for g in groups
                 if g["__out"][lk["key"]] is not None}
        empty = {n: None for n in names}
        out = []
        for r in rows:
            val = self._eval(lk["on"], r)
            hit = table.get(val) if val is not None else None
            if hit is None and lk.get("join") == "inner":
                continue
            out.append({**r, "__lk": {**r.get("__lk", {}), lk["as"]: hit or empty}})
        return out

    def _tail(self, rows: list[dict], columns: list[str], json_cols: set, vids: list[str],
              _canonicalize_floats: Any) -> dict[str, Any]:
        args = self.args
        ret = args["return"]
        keys = [(o["expr"], o.get("dir", "asc"), o.get("nulls", "last"))
                for o in ret.get("order_by", [])]
        keys += [({"col": c}, "asc", "first") for c in columns if c not in json_cols]
        keys += [({"col": v}, "asc", "first") for v in vids]
        rows = self._sort(rows, keys)
        if ret.get("limit"):
            rows = rows[:ret["limit"]]
        out_rows = [{c: r["__out"][c] for c in columns} for r in rows]
        offset = int(args["cursor"]) if args.get("cursor") else 0
        page = out_rows[offset:offset + args["limit"]]
        truncated = offset + len(page) < len(out_rows)
        return _canonicalize_floats({
            "columns": columns, "rows": page, "rows_total": len(out_rows),
            "truncated": truncated,
            "cursor": str(offset + len(page)) if truncated else None})
