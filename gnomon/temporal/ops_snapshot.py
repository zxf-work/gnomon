"""Snapshot-family operators: entity_history, snapshot_subgraph,
diff_snapshots, neighborhood_evolution, resolve_entities.

Kernels operate on columnar buffers (struct-of-arrays) — no per-edge Python
loops (exception documented on `resolve_entities`).
"""

from __future__ import annotations

import json

from typing import Any

import numpy as np

from gnomon.core.errors import InvalidArgError, LimitError
from gnomon.core.model import OPEN_END
from gnomon.storage.base import StorageAdapter
from gnomon.temporal.algebra import (
    AS_OF_TT,
    CURSOR,
    LIMIT,
    TIMESTAMP,
    UID,
    UID_LIST,
    WINDOW,
    check_window,
    operator,
    paginate,
    required,
)
from gnomon.temporal.guardrails import (
    coeff_scale,
    point_read_estimate,
    scan_estimate,
    touching_read_ms,
)

MAX_HOPS = 3

#: The largest reached-node set the seed-driven snapshot
#: expands; past it `snapshot_subgraph` takes the whole-snapshot path. It is
#: the engine's cap on an incidence filter answered from adjacency postings
#: (`ADJ_FILTER_MAX_IDS`), so every filtered scan the expansion makes can
#: take them; a frontier that large is no longer "a few nodes", and one
#: instant scan is the cheaper plan.
SEED_EXPANSION_MAX_IDS = 1024


# --------------------------------------------------------------------------- #
# shared snapshot helpers                                                      #
# --------------------------------------------------------------------------- #

def edges_at(adapter: StorageAdapter, t: int, as_of_tt: int,
             rel_types: list[str] | None = None,
             columns: tuple[str, ...] | None = None) -> dict[str, np.ndarray]:
    """Believed edge versions valid at instant t (vt_s <= t < vt_e)."""
    return adapter.edges_columnar(as_of_tt=as_of_tt, vt_min=t, vt_max=t + 1,
                                  rel_types=rel_types, columns=columns)


def nodes_at(adapter: StorageAdapter, t: int, as_of_tt: int) -> dict[str, np.ndarray]:
    return adapter.nodes_columnar(as_of_tt=as_of_tt, vt_min=t, vt_max=t + 1)


def bfs_node_set(edges: dict[str, np.ndarray], seed_ids: np.ndarray,
                 valid_ids: np.ndarray, hops: int) -> dict[int, int]:
    """Undirected BFS over a snapshot edge set, restricted to valid nodes.
    Returns {dense_id: hop_distance} for nodes within `hops` of the seeds."""
    src, dst = edges["src_id"], edges["dst_id"]
    valid = set(valid_ids.tolist())
    dist: dict[int, int] = {int(i): 0 for i in seed_ids if int(i) in valid}
    frontier = np.asarray(sorted(dist), dtype=np.int64)
    for h in range(1, hops + 1):
        if frontier.size == 0:
            break
        m = np.isin(src, frontier) | np.isin(dst, frontier)
        touched = np.unique(np.concatenate([src[m], dst[m]]))
        new = [int(i) for i in touched if int(i) not in dist and int(i) in valid]
        for i in new:
            dist[i] = h
        frontier = np.asarray(new, dtype=np.int64)
    return dist


def _seed_expansion(adapter: StorageAdapter) -> bool:
    """Can this adapter answer a snapshot by expanding from its seeds?

    Only the native engine: its filtered scans walk the per-segment
    adjacency index (fix B), and it reads a node's validity at an instant by
    identity (`nodes_valid_at`). The switch `GNOMON_SEED_EXPANSION=0` is read
    when the handle opens and reported by `designs()`. Not inside an open
    write batch: point reads see staged versions, which scans do not.
    """
    designs = getattr(adapter, "designs", None)
    if designs is None or not hasattr(adapter, "nodes_valid_at"):
        return False
    if not designs().get("seed_expansion", True):
        return False
    in_batch = getattr(getattr(adapter, "_store", None), "in_batch", None)
    return not (in_batch is not None and in_batch())


def _expand_from_seeds(adapter: StorageAdapter, seed_ids: np.ndarray, t: int,
                       as_of: int, hops: int, rel_types: list[str] | None):
    """`bfs_node_set` over G(t) and the induced edges, from
    the seeds outwards rather than over the whole instant.

    Each hop is one scan filtered to the frontier (`touching_ids`), which
    the engine answers from each segment's adjacency postings, with the same
    belief, valid-time and relation predicate as the instant scan; a reached
    node joins only if it has a node version valid at `t` (identity point
    reads, `nodes_valid_at`). The induced edges are one scan with both
    endpoints in the reached set (`touching_both`), every column, in the
    scan's `(vt_s, vid)` order -- the rows the whole-instant scan would keep.

    Returns `(dist, edges, node_info)`, or `None` when the reached set
    outgrows `SEED_EXPANSION_MAX_IDS`; the caller then runs the
    whole-snapshot path.
    """
    cols = ("src_id", "dst_id")
    seeds = sorted({int(i) for i in seed_ids})
    # The first call is a scan, as on the whole-snapshot path, so a store
    # that cannot answer this belief time refuses it the same way.
    e = adapter.edges_columnar(as_of_tt=as_of, vt_min=t, vt_max=t + 1,
                               rel_types=rel_types, columns=cols,
                               touching_ids=seeds)
    info = adapter.nodes_valid_at(seeds, t, as_of)
    checked = set(seeds)
    dist: dict[int, int] = {int(i): 0 for i in seed_ids if int(i) in info}
    frontier = np.asarray(sorted(dist), dtype=np.int64)
    for h in range(1, hops + 1):
        if frontier.size == 0:
            break
        if h > 1:
            e = adapter.edges_columnar(as_of_tt=as_of, vt_min=t, vt_max=t + 1,
                                       rel_types=rel_types, columns=cols,
                                       touching_ids=frontier.tolist())
        src, dst = e["src_id"], e["dst_id"]
        m = np.isin(src, frontier) | np.isin(dst, frontier)
        touched = np.unique(np.concatenate([src[m], dst[m]]))
        cand = [int(i) for i in touched if int(i) not in dist]
        if len(dist) + len(cand) > SEED_EXPANSION_MAX_IDS:
            return None
        ask = [i for i in cand if i not in checked]
        checked.update(ask)
        info.update(adapter.nodes_valid_at(ask, t, as_of))
        new = [i for i in cand if i in info]
        for i in new:
            dist[i] = h
        frontier = np.asarray(new, dtype=np.int64)
    edges = adapter.edges_columnar(as_of_tt=as_of, vt_min=t, vt_max=t + 1,
                                   rel_types=rel_types, touching_ids=sorted(dist),
                                   touching_both=True)
    return dist, edges, info


def _edge_rows(adapter: StorageAdapter, edges: dict[str, np.ndarray],
               idx: np.ndarray) -> list[dict[str, Any]]:
    src_uids = adapter.uids_for(edges["src_id"][idx])
    dst_uids = adapter.uids_for(edges["dst_id"][idx])
    return [
        {"eid": edges["eid"][i], "vid": edges["vid"][i], "src": s, "dst": d,
         "rel_type": edges["rel_type"][i],
         "vt_s": int(edges["vt_s"][i]), "vt_e": int(edges["vt_e"][i])}
        for i, s, d in zip(idx.tolist(), src_uids, dst_uids)
    ]


# --------------------------------------------------------------------------- #
# entity_history                                                            #
# --------------------------------------------------------------------------- #

def _engine_entity_schema(schema: dict[str, Any]) -> dict[str, Any]:
    """Engine-level arguments (the point-in-time read): `t_valid`, an
    instant, or `window`, a valid-time window. The answer is then the
    believed versions whose valid interval contains the instant or overlaps
    the window, found by the engine's windowed point read
    (`believed_*_versions_overlapping`, `pointread.rs`) instead of by
    materialising the whole believed history. Neither has a default, and
    neither is in the base schema (algebra.OperatorSpec)."""
    schema["properties"]["t_valid"] = {
        **TIMESTAMP, "description": "only the versions valid at this instant"}
    schema["properties"]["window"] = {
        **WINDOW, "description": "only the versions whose valid interval "
                                "overlaps this half-open window"}
    return schema


def _entity_window(args: dict[str, Any]) -> tuple[int, int] | None:
    """`[lo, hi)` the engine-level `t_valid`/`window` select, or `None`."""
    if args.get("t_valid") is not None:
        return int(args["t_valid"]), int(args["t_valid"]) + 1
    if args.get("window") is not None:
        return int(args["window"]["t_a"]), int(args["window"]["t_b"])
    return None


def _entity_validators(args: dict[str, Any]) -> None:
    if args.get("t_valid") is not None and args.get("window") is not None:
        raise InvalidArgError("entity_history takes t_valid or window, not both")
    check_window(args)


@operator(
    "entity_history",
    {
        "uid": required(UID),
        "as_of_tt": AS_OF_TT,
        "include_edges": {"type": "boolean", "default": False},
        "limit": LIMIT,
        "cursor": CURSOR,
    },
    "Ordered version list of a node (optionally with incident edge versions): "
    "all versions of `uid` believed at `as_of_tt`, ordered by vt_s.",
    cost_fn=point_read_estimate,
    validators=[_entity_validators],
    output_fields=("rows", "rows_total", "truncated", "cursor",
                   "edges", "edges_truncated"),
    engine_schema=_engine_entity_schema,
)
def entity_history(adapter: StorageAdapter, args: dict[str, Any]) -> dict[str, Any]:
    uid = args["uid"]
    as_of = args["as_of_tt"]
    adapter.dense_ids([uid])  # raises E_NOT_FOUND for unknown uid
    span = _entity_window(args)
    if span is None:
        versions = adapter.believed_node_versions(uid, as_of_tt=as_of)
    else:
        # the full history filtered to the span, without materialising it:
        # the same rows, in the same (vt_s, vid) order
        versions = adapter.believed_node_versions_overlapping(
            uid, span[0], span[1], as_of_tt=as_of)
    rows = [v.to_json() for v in sorted(versions, key=lambda v: (v.vt_s, v.vid))]
    for r in rows:
        # bi-temporal immutability: a version believed at as_of_tt has its
        # belief end in the future of that tt — report it as open, never
        # leak post-as_of knowledge into a pinned result
        if r["tt_e"] > as_of:
            r["tt_e"] = OPEN_END
    out = paginate(rows, args["limit"], args["cursor"])
    if args["include_edges"]:
        uid_id = int(adapter.dense_ids([uid])[0])
        if span is None:
            edges = adapter.edges_columnar(as_of_tt=as_of, touching_ids=[uid_id])
        else:
            # incident edge versions overlapping the same span
            edges = adapter.edges_columnar(as_of_tt=as_of, touching_ids=[uid_id],
                                           vt_min=span[0], vt_max=span[1])
        n_incident = len(edges["src_id"])
        idx = np.arange(min(n_incident, args["limit"]))
        out["edges"] = _edge_rows(adapter, edges, idx)
        out["edges_truncated"] = n_incident > args["limit"]
    return out


# --------------------------------------------------------------------------- #
# snapshot_subgraph                                                         #
# --------------------------------------------------------------------------- #

def _check_hops(args: dict[str, Any]) -> None:
    if args.get("hops", 1) > MAX_HOPS:
        raise LimitError(f"hops capped at {MAX_HOPS}")


@operator(
    "snapshot_subgraph",
    {
        "seeds": required(UID_LIST),
        "hops": {"type": "integer", "minimum": 0, "maximum": MAX_HOPS, "default": 1},
        "t_valid": required(TIMESTAMP),
        "as_of_tt": AS_OF_TT,
        "rel_types": {"type": ["array", "null"], "items": {"type": "string"}, "default": None},
        "limit": LIMIT,
        "cursor": CURSOR,
    },
    "k-hop subgraph (undirected expansion, induced edges) of the snapshot graph "
    "G(t_valid) under belief state as_of_tt. `rows` are the induced edges, "
    "paginated; `nodes` carry hop distance.",
    cost_fn=scan_estimate,
    validators=[_check_hops],
    output_fields=("rows", "rows_total", "truncated", "cursor",
                   "nodes", "nodes_total", "nodes_truncated"),
)
def snapshot_subgraph(adapter: StorageAdapter, args: dict[str, Any]) -> dict[str, Any]:
    t, as_of = args["t_valid"], args["as_of_tt"]
    seed_ids = adapter.dense_ids(args["seeds"])
    expanded = None
    if _seed_expansion(adapter):
        expanded = _expand_from_seeds(adapter, seed_ids, t, as_of, args["hops"],
                                      args["rel_types"])
    if expanded is None and _seed_expansion(adapter) and \
            hasattr(adapter, "snapshot_kernel") and \
            adapter.designs().get("match_kernel", False):
        # a reached set past `SEED_EXPANSION_MAX_IDS`: the engine's batched
        # frontier (the pattern executor's) rather than the whole instant.
        # Below the cap the scan expansion stays first, since on a store with
        # retractions its filtered scans are the cheaper per-vertex read.
        # `GNOMON_MATCH_KERNEL=python` keeps the whole-snapshot fallback.
        got = adapter.snapshot_kernel(seed_ids, t, as_of, args["hops"], args["rel_types"])
        if got is not None:
            node_rows = sorted(({"uid": u, "label": lab, "hop": h} for u, lab, h in got["nodes"]),
                               key=lambda r: (r["hop"], r["uid"]))
            out = paginate(got["edges"], args["limit"], args["cursor"])
            out["nodes"] = node_rows[: args["limit"]]
            out["nodes_total"] = len(node_rows)
            out["nodes_truncated"] = len(node_rows) > args["limit"]
            out["truncated"] = out["truncated"] or out["nodes_truncated"]
            return out
    if expanded is not None:
        # the induced edges are exactly the scan's rows
        dist, edges, info = expanded
        idx = np.arange(len(edges["src_id"]))
        node_rows = sorted(
            ({"uid": info[i][0], "label": info[i][1], "hop": h} for i, h in dist.items()),
            key=lambda r: (r["hop"], r["uid"]))
    else:
        # the whole snapshot at t: other backends, `GNOMON_SEED_EXPANSION=0`,
        # and a reached set past `SEED_EXPANSION_MAX_IDS` with no engine
        # frontier to take it
        edges = edges_at(adapter, t, as_of, args["rel_types"])
        nodes = nodes_at(adapter, t, as_of)
        dist = bfs_node_set(edges, seed_ids, nodes["uid_id"], args["hops"])
        in_set = np.isin(edges["src_id"], list(dist)) & np.isin(edges["dst_id"], list(dist))
        idx = np.flatnonzero(in_set)  # already ordered by (vt_s, vid)
        node_pos = {int(i): k for k, i in enumerate(nodes["uid_id"].tolist())}
        node_rows = sorted(
            ({"uid": nodes["uid"][node_pos[i]], "label": nodes["label"][node_pos[i]],
              "hop": h} for i, h in dist.items()),
            key=lambda r: (r["hop"], r["uid"]))
    out = paginate(_edge_rows(adapter, edges, idx), args["limit"], args["cursor"])
    out["nodes"] = node_rows[: args["limit"]]
    out["nodes_total"] = len(node_rows)
    out["nodes_truncated"] = len(node_rows) > args["limit"]
    out["truncated"] = out["truncated"] or out["nodes_truncated"]
    return out


# --------------------------------------------------------------------------- #
# diff_snapshots                                                            #
# --------------------------------------------------------------------------- #

SCOPE = {
    "type": ["object", "null"],
    "default": None,
    "properties": {"seeds": UID_LIST,
                   "hops": {"type": "integer", "minimum": 0, "maximum": MAX_HOPS}},
    "required": ["seeds", "hops"],
    "additionalProperties": False,
    "description": "restrict the diff to nodes within `hops` of `seeds` in "
                   "either snapshot (edges need both endpoints in scope)",
}


#: Everything `diff_snapshots` reads off an edge: `eid` is the logical
#: identity the difference is taken over, `vid` decides which identities are
#: props_changed candidates, and the rest fill the output rows. Notably absent
#: are `vt_s` and `vt_e` — the snapshot is an instant, so both are implied.
#: `snapshot_subgraph` does need them, which is why the projection lives here
#: rather than in `edges_at`.
_DIFF_EDGE_COLS = ("eid", "vid", "src_id", "dst_id", "rel_type")


#: With the rows' physical addresses, where the adapter can read props by
#: address: the changed-props check then reads the few candidate
#: rows directly instead of sweeping every edge row for their vids.
_DIFF_EDGE_ADDR_COLS = _DIFF_EDGE_COLS + ("seg_id", "seg_row")


def _point_state(adapter: StorageAdapter, t: int, as_of: int,
                 edge_columns: tuple[str, ...] = _DIFF_EDGE_COLS):
    """(node columnar, edge columnar) valid at t, plus identity->index maps."""
    nodes = nodes_at(adapter, t, as_of)
    edges = edges_at(adapter, t, as_of, columns=edge_columns)
    node_map = {u: i for i, u in enumerate(nodes["uid"].tolist())}
    edge_map = {e: i for i, e in enumerate(edges["eid"].tolist())}
    return nodes, edges, node_map, edge_map


def _edge_props_by_address(adapter: StorageAdapter, cand_e, e1, emap1, e2, emap2):
    """`{vid: props}` for the candidate edge versions, read at the addresses
    the two point-state scans returned — the map `props_for_vids`
    builds by sweeping every edge row, built from the candidate rows alone.
    `None` (take the sweep) when an address is not a committed segment row:
    a row staged in an open batch has no segment to read it from."""
    vids: list[str] = []
    seg_ids: list[int] = []
    seg_rows: list[int] = []
    seen: set[str] = set()
    for e, va, vb in cand_e:
        for vid, edges, i in ((va, e1, emap1[e]), (vb, e2, emap2[e])):
            if vid in seen:
                continue
            seen.add(vid)
            sid, row = int(edges["seg_id"][i]), int(edges["seg_row"][i])
            if sid < 0 or row < 0:
                return None
            vids.append(vid)
            seg_ids.append(sid)
            seg_rows.append(row)
    return dict(zip(vids, adapter.props_at("edge", seg_ids, seg_rows)))


@operator(
    "diff_snapshots",
    {
        "t1": required(TIMESTAMP),
        "t2": required(TIMESTAMP),
        "as_of_tt": AS_OF_TT,
        "scope": SCOPE,
        "limit": LIMIT,
    },
    "Delta between snapshots G(t1) and G(t2) under one belief state: "
    "nodes/edges added & removed (by logical identity) and props_changed. "
    "Each list is independently capped at `limit` with *_total counts.",
    cost_fn=scan_estimate,
    output_fields=("nodes_added", "nodes_added_total", "nodes_removed",
                   "nodes_removed_total", "edges_added", "edges_added_total",
                   "edges_removed", "edges_removed_total", "props_changed",
                   "props_changed_total", "truncated"),
)
def diff_snapshots(adapter: StorageAdapter, args: dict[str, Any]) -> dict[str, Any]:
    t1, t2, as_of, limit = args["t1"], args["t2"], args["as_of_tt"], args["limit"]
    if args["scope"] is None and _diff_kernel(adapter):
        got = adapter.diff_snapshots_kernel(t1, t2, as_of, limit)
        if got is not None:
            return _diff_from_kernel(got, limit)
    by_address = callable(getattr(adapter, "props_at", None))
    ecols = _DIFF_EDGE_ADDR_COLS if by_address else _DIFF_EDGE_COLS
    n1, e1, nmap1, emap1 = _point_state(adapter, t1, as_of, ecols)
    n2, e2, nmap2, emap2 = _point_state(adapter, t2, as_of, ecols)

    allowed: set[str] | None = None
    if args["scope"] is not None:
        seed_ids = adapter.dense_ids(args["scope"]["seeds"])
        hops = args["scope"]["hops"]
        d1 = bfs_node_set(e1, seed_ids, n1["uid_id"], hops)
        d2 = bfs_node_set(e2, seed_ids, n2["uid_id"], hops)
        allowed = set(adapter.uids_for(sorted(set(d1) | set(d2))))

    def node_ok(uid: str) -> bool:
        return allowed is None or uid in allowed

    def edge_ok(edges, i) -> bool:
        if allowed is None:
            return True
        s, d = adapter.uids_for([int(edges["src_id"][i]), int(edges["dst_id"][i])])
        return s in allowed and d in allowed

    nodes_added = sorted(u for u in nmap2 if u not in nmap1 and node_ok(u))
    nodes_removed = sorted(u for u in nmap1 if u not in nmap2 and node_ok(u))
    edges_added = sorted(e for e in emap2 if e not in emap1 and edge_ok(e2, emap2[e]))
    edges_removed = sorted(e for e in emap1 if e not in emap2 and edge_ok(e1, emap1[e]))

    # props_changed: same identity present at both instants via different
    # version rows with different content
    changed: list[dict[str, Any]] = []
    common_nodes = [u for u in nmap1 if u in nmap2 and node_ok(u)]
    cand = [(u, n1["vid"][nmap1[u]], n2["vid"][nmap2[u]]) for u in common_nodes
            if n1["vid"][nmap1[u]] != n2["vid"][nmap2[u]]]
    props = adapter.props_for_vids(
        "node", sorted({v for _, a, b in cand for v in (a, b)}))
    for u, va, vb in sorted(cand):
        la, lb = n1["label"][nmap1[u]], n2["label"][nmap2[u]]
        if props.get(va) != props.get(vb) or la != lb:
            changed.append({"kind": "node", "id": u,
                            "from": {"label": la, "props": props.get(va)},
                            "to": {"label": lb, "props": props.get(vb)}})
    common_edges = [e for e in emap1 if e in emap2 and edge_ok(e1, emap1[e])]
    cand_e = [(e, e1["vid"][emap1[e]], e2["vid"][emap2[e]]) for e in common_edges
              if e1["vid"][emap1[e]] != e2["vid"][emap2[e]]]
    props_e = None
    if by_address and cand_e:
        props_e = _edge_props_by_address(adapter, cand_e, e1, emap1, e2, emap2)
    if props_e is None:
        props_e = adapter.props_for_vids(
            "edge", sorted({v for _, a, b in cand_e for v in (a, b)}))
    for e, va, vb in sorted(cand_e):
        if props_e.get(va) != props_e.get(vb):
            changed.append({"kind": "edge", "id": e,
                            "from": {"props": props_e.get(va)},
                            "to": {"props": props_e.get(vb)}})

    def edge_desc(edges, emap, eid):
        i = emap[eid]
        s, d = adapter.uids_for([int(edges["src_id"][i]), int(edges["dst_id"][i])])
        return {"eid": eid, "src": s, "dst": d, "rel_type": edges["rel_type"][i]}

    out = {
        "nodes_added": nodes_added[:limit], "nodes_added_total": len(nodes_added),
        "nodes_removed": nodes_removed[:limit], "nodes_removed_total": len(nodes_removed),
        "edges_added": [edge_desc(e2, emap2, e) for e in edges_added[:limit]],
        "edges_added_total": len(edges_added),
        "edges_removed": [edge_desc(e1, emap1, e) for e in edges_removed[:limit]],
        "edges_removed_total": len(edges_removed),
        "props_changed": changed[:limit], "props_changed_total": len(changed),
    }
    out["truncated"] = any(
        out[f"{k}_total"] > limit
        for k in ("nodes_added", "nodes_removed", "edges_added", "edges_removed",
                  "props_changed"))
    return out


def _diff_kernel(adapter: StorageAdapter) -> bool:
    """Does this adapter carry the engine's snapshot diff? Only the
    native engine, with `GNOMON_DIFF_KERNEL` not set to `python` (read at open,
    reported by `designs()`). `scope` keeps the path below."""
    designs = getattr(adapter, "designs", None)
    if designs is None or not hasattr(adapter, "diff_snapshots_kernel"):
        return False
    return bool(designs().get("diff_kernel", False))


def _diff_from_kernel(got: dict[str, Any], limit: int) -> dict[str, Any]:
    """The operator's answer from the engine's: the four identity lists come
    back paged and ordered; the content candidates come back whole, with the
    stored text of both versions, and are decided here exactly as the path
    below decides them -- `props_for_vids` parses the same stored text with
    `json.loads`, so a candidate is a change iff the parsed values (or, for
    a node, the labels) differ."""
    changed: list[dict[str, Any]] = []
    for uid, _va, _vb, la, lb, pa_text, pb_text in got["node_cands"]:
        pa, pb = json.loads(pa_text), json.loads(pb_text)
        if pa != pb or la != lb:
            changed.append({"kind": "node", "id": uid,
                            "from": {"label": la, "props": pa},
                            "to": {"label": lb, "props": pb}})
    for eid, _va, _vb, pa_text, pb_text in got["edge_cands"]:
        pa, pb = json.loads(pa_text), json.loads(pb_text)
        if pa != pb:
            changed.append({"kind": "edge", "id": eid,
                            "from": {"props": pa},
                            "to": {"props": pb}})
    out = {
        "nodes_added": got["nodes_added"], "nodes_added_total": got["nodes_added_total"],
        "nodes_removed": got["nodes_removed"], "nodes_removed_total": got["nodes_removed_total"],
        "edges_added": got["edges_added"], "edges_added_total": got["edges_added_total"],
        "edges_removed": got["edges_removed"], "edges_removed_total": got["edges_removed_total"],
        "props_changed": changed[:limit], "props_changed_total": len(changed),
    }
    out["truncated"] = any(
        out[f"{k}_total"] > limit
        for k in ("nodes_added", "nodes_removed", "edges_added", "edges_removed",
                  "props_changed"))
    return out


# --------------------------------------------------------------------------- #
# neighborhood_evolution                                                   #
# --------------------------------------------------------------------------- #

MAX_BUCKETS = 2_000


def _check_t1_t2(args: dict[str, Any]) -> None:
    if not (args["t1"] < args["t2"]):
        raise InvalidArgError("neighborhood_evolution requires t1 < t2")


def _evolution_cost(args: dict[str, Any], stats: dict[str, Any]) -> dict[str, int]:
    """Three reads of the edge versions touching `uid` (the neighbourhoods at
    t1 and at t2, the degree series), not a scan of the store: priced as
    touching reads (`scan_estimate` was 560 to 4,250 times the time)."""
    rows = 3 * 2 * int(stats.get("max_out_degree") or 0)
    return {"rows_scanned_est": rows, "expansions_est": 0,
            "time_est_ms": int(3 * touching_read_ms(stats) * coeff_scale()) + 1}


@operator(
    "neighborhood_evolution",
    {
        "uid": required(UID),
        "t1": required(TIMESTAMP),
        "t2": required(TIMESTAMP),
        "as_of_tt": AS_OF_TT,
        "stride": {"type": ["integer", "null"], "minimum": 1, "default": None,
                   "description": "degree-series bucket width; default (t2-t1)/20"},
        "limit": LIMIT,
    },
    "Neighbors of `uid` gained/lost between G(t1) and G(t2) (undirected), plus "
    "an incident-active-edge-count series sampled at bucket starts in [t1, t2).",
    cost_fn=_evolution_cost,
    validators=[_check_t1_t2],
    output_fields=("neighbors_gained", "neighbors_gained_total",
                   "neighbors_lost", "neighbors_lost_total",
                   "degree_series", "stride", "truncated"),
)
def neighborhood_evolution(adapter: StorageAdapter, args: dict[str, Any]) -> dict[str, Any]:
    uid, t1, t2, as_of = args["uid"], args["t1"], args["t2"], args["as_of_tt"]
    uid_id = int(adapter.dense_ids([uid])[0])
    stride = args["stride"] or max(1, (t2 - t1) // 20)
    n_buckets = -(-(t2 - t1) // stride)
    if n_buckets > MAX_BUCKETS:
        raise LimitError(f"bucket count {n_buckets} exceeds cap {MAX_BUCKETS}; "
                         "increase stride")

    def neighbors(t: int) -> set[str]:
        e = adapter.edges_columnar(as_of_tt=as_of, vt_min=t, vt_max=t + 1,
                                   columns=("src_id", "dst_id"),
                                   touching_ids=[uid_id])
        m_out = e["src_id"] == uid_id
        m_in = e["dst_id"] == uid_id
        ids = np.unique(np.concatenate([e["dst_id"][m_out], e["src_id"][m_in]]))
        return set(adapter.uids_for([i for i in ids.tolist() if i != uid_id]))

    n_t1, n_t2 = neighbors(t1), neighbors(t2)
    gained, lost = sorted(n_t2 - n_t1), sorted(n_t1 - n_t2)

    # incident active-edge count at each bucket start, one pushed-down scan
    e = adapter.edges_columnar(as_of_tt=as_of, vt_min=t1, vt_max=t2,
                               columns=("vt_s", "vt_e"), touching_ids=[uid_id])
    starts = np.sort(e["vt_s"])
    ends = np.sort(e["vt_e"])
    bucket_starts = np.arange(t1, t2, stride, dtype=np.int64)
    deg = (np.searchsorted(starts, bucket_starts, side="right")
           - np.searchsorted(ends, bucket_starts, side="right"))
    series = [{"t": int(t), "degree": int(d)} for t, d in zip(bucket_starts, deg)]

    limit = args["limit"]
    return {
        "neighbors_gained": gained[:limit], "neighbors_gained_total": len(gained),
        "neighbors_lost": lost[:limit], "neighbors_lost_total": len(lost),
        "degree_series": series, "stride": stride,
        "truncated": len(gained) > limit or len(lost) > limit,
    }


# --------------------------------------------------------------------------- #
# resolve_entities                                                         #
# --------------------------------------------------------------------------- #

@operator(
    "resolve_entities",
    {
        "query": required({"type": "string", "minLength": 1}),
        "label": {"type": ["string", "null"], "default": None},
        "as_of_tt": AS_OF_TT,
        "limit": LIMIT,
        "cursor": CURSOR,
    },
    "Name/uid lookup returning uids — exact uid match first, then "
    "case-insensitive substring on uid and on the `name` prop of any believed "
    "version. Callers obtain uids from this operator.",
    cost_fn=scan_estimate,
)
def resolve_entities(adapter: StorageAdapter, args: dict[str, Any]) -> dict[str, Any]:
    q = args["query"]
    page_kernel = getattr(adapter, "resolve_entities_page", None)
    if page_kernel is not None:
        # engine-side, paged: the label filter, the (match, uid)
        # order and the slice happen in the engine, so props are parsed for
        # the page's rows only. The cursor is read exactly as `paginate`
        # reads it; a negative offset (Python slicing from the end) keeps the
        # row-list path below rather than being re-derived here.
        cursor = args["cursor"]
        try:
            offset = int(cursor) if cursor else 0
        except ValueError:
            raise InvalidArgError(f"bad cursor: {cursor!r}") from None
        if 0 <= offset < 1 << 62:
            total, page = page_kernel(q, args["as_of_tt"], args["label"],
                                      offset, args["limit"])
            rows = [{"uid": uid, "label": label,
                     "name": json.loads(props_text).get("name"),
                     "match": int(score)}
                    for uid, score, label, props_text in page]
            truncated = offset + len(rows) < total
            return {
                "rows": rows,
                "rows_total": total,
                "truncated": truncated,
                "cursor": str(offset + len(rows)) if truncated else None,
            }
    kernel = getattr(adapter, "resolve_entities", None)
    if kernel is not None:
        # engine-side: matches over the promoted name column, no JSON per row
        rows = []
        for uid, score, label, props_text in kernel(q, args["as_of_tt"]):
            if args["label"] is not None and label != args["label"]:
                continue
            rows.append({"uid": uid, "label": label,
                         "name": json.loads(props_text).get("name"),
                         "match": int(score)})
        return paginate(rows, args["limit"], args["cursor"])

    # portable fallback: a row loop over every believed node version
    ql = q.lower()
    as_of = args["as_of_tt"]
    latest: dict[str, Any] = {}
    matched: dict[str, int] = {}
    for v in adapter.all_node_versions():
        if not v.believed_at(as_of):
            continue
        cur = latest.get(v.uid)
        if cur is None or v.vt_s > cur.vt_s:
            latest[v.uid] = v
        # Only JSON string names participate in matching
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
