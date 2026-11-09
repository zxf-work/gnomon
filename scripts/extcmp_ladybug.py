"""LadybugDB (the maintained successor of Kùzu) for the external comparison.

Embedded, `pip install ladybug`, `import ladybug`. Same data model as the
Neo4j/Memgraph baselines (`(:Entity {dense_id, uid})`, `(:NodeVersion)`,
edge versions as relationships `E` carrying all four timestamps as INT64
properties and `props` as canonical-JSON text), declared with LadybugDB's
schema DDL and bulk-loaded with `COPY FROM` Parquet — the vendor's
recommended load path.

The Cypher is ported from `neo4j_queries.py` / `memgraph_queries.py`. What
changes, and why:

* **Per-hop predicates.** `snap.hop2` is one recursive pattern,
  `-[r:E*1..h (e, n | WHERE <valid at t> AND <believed> AND n valid)]-`, the
  LadybugDB idiom for a filtered variable-length traversal — no
  Python-driven rounds. Time-respecting traversals (`reach.window`,
  `paths.k`) cannot be written that way: the predicate on hop k depends on
  the arrival time reached at hop k-1, which a per-hop predicate does not
  see. They keep the Neo4j baseline's round structure, one Cypher statement
  per round.
* **Dialect.** `lower` for `toLower`; node inequality on `dense_id`;
  `collect(...)[0]`-style picks done after the query; the scoped
  `CALL (x) {}` degree series uses Memgraph's correlated UNWIND form.
* **Path state.** The Neo4j `paths.k` passes whole path states (with nested
  lists) as parameters; here a round passes `(i, node, arr)` only and the
  `seen` check runs on the returned candidates. Same answer, smaller
  parameter.

Settings (recorded in the receipt): `buffer_pool_size` LadybugDB's own default share, 80 % of the memory the
process may use, or
EXTCMP_LADYBUG_BUFFER_POOL (bytes) when set; W1 used 16 GiB on a shared 122 GB host,
threads left at the default (all cores), compression on.
"""

from __future__ import annotations

import json
import os
import shutil
import time
from pathlib import Path
from typing import Any

import extcmp as X
from neo4j_queries import BIG_SCORE, OPEN_END, _page, belief

from gnomon.core.model import canonical_json



def _mem_limit() -> int:
    """The memory this process may use: host RAM, or the job's cgroup limit if smaller (Slurm on a shared node)."""
    lim = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
    try:
        for line in open("/proc/self/cgroup"):
            _, ctl, path = line.strip().split(":", 2)
            if ctl in ("memory", ""):
                p = Path("/sys/fs/cgroup") / ("memory" if ctl else "") / path.lstrip("/")
                while True:            # the job's limit sits on a parent of the step's cgroup
                    for f in ("memory.limit_in_bytes", "memory.max"):
                        try:
                            v = (p / f).read_text().strip()
                            if v.isdigit():
                                lim = min(lim, int(v))
                        except OSError:
                            pass
                    if p == p.parent or p.name in ("memory", "cgroup"):
                        break
                    p = p.parent
    except OSError:
        pass
    return lim


def auto_buffer_pool(env_value: str | None) -> tuple[int, str]:
    """The buffer pool a competent user sets: LadybugDB's own default share, 80 % of the
    memory, of what this process may use (host RAM or the job's cgroup limit, whichever is smaller; LadybugDB itself
    reads host RAM, which on a shared node is more than the job may use). An explicit size in GiB overrides it."""
    if env_value and env_value not in ("auto", "default"):
        return int(float(env_value) * (1 << 30)), f"explicit {env_value} GiB"
    m = _mem_limit()
    return int(0.8 * m), f"auto: 0.8 x {m / (1 << 30):.1f} GiB (min of host RAM and the job's memory limit)"


_bp = os.environ.get("EXTCMP_LADYBUG_BUFFER_POOL")        # bytes (older runs) or auto
BUFFER_POOL, BUFFER_POOL_RULE = ((int(_bp), f"explicit {int(_bp)} bytes") if _bp and _bp.isdigit()
                                 else auto_buffer_pool(_bp))
#: 5 s below the client alarm: LadybugDB honours an interrupt at operator boundaries, seconds late on a long scan
#: (W2 K10M snap.hop2: the "Interrupted." of a 28 s timeout arrived with the 30 s alarm)
#: and never above the P1 server-side limit (EXTCMP_SERVER_LIMIT_S, the ceiling), as PostgreSQL's statement_timeout
QUERY_TIMEOUT_MS = max(1000, min(int(X.SERVER_LIMIT_S) * 1000, int(X.VERIFY_CAP_S) * 1000 - 5000))
#: threads: LadybugDB's default (0: all cores the process sees) or EXTCMP_LADYBUG_THREADS
THREADS = int(os.environ.get("EXTCMP_LADYBUG_THREADS", "0") or 0)
#: LB-4 (prep-ladybug PROBLEMS): the client's implicit prepared-statement cache keeps buffer-pool memory per
#: statement; at most this many statements stay cached (the oldest are closed first)
CACHE_MAX = int(os.environ.get("EXTCMP_LADYBUG_CACHE_MAX", "32"))


def _trim_cache(conn, current: str) -> None:
    cache = getattr(conn, "_pybind_implicit_prepared_cache", None)
    if not cache or len(cache) <= CACHE_MAX:
        return
    for k in list(cache)[: len(cache) - CACHE_MAX]:
        if k[0] == current:
            continue
        p = cache.pop(k)
        close = getattr(p, "close", None)
        if callable(close):
            try:
                close()
            except Exception:                                    # noqa: BLE001
                pass

SCHEMA = [
    "CREATE NODE TABLE Entity(dense_id INT64, uid STRING, PRIMARY KEY (dense_id))",
    "CREATE NODE TABLE NodeVersion(vid STRING, uid STRING, label STRING, vt_s INT64, "
    "vt_e INT64, tt_s INT64, tt_e INT64, props STRING, source STRING, prov STRING, "
    "name STRING, PRIMARY KEY (vid))",
    "CREATE REL TABLE E(FROM Entity TO Entity, eid STRING, vid STRING, rel_type STRING, "
    "disc STRING, vt_s INT64, vt_e INT64, tt_s INT64, tt_e INT64, props STRING, "
    "source STRING, prov STRING)",
]


def _rows(conn, q: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    _trim_cache(conn, q)
    r = conn.execute(q, params or {})
    cols = r.get_column_names()
    out = []
    while r.has_next():
        out.append(dict(zip(cols, r.get_next())))
    r.close()
    return out


def _one(conn, q, params=None):
    rows = _rows(conn, q, params)
    return next(iter(rows[0].values()))


# --------------------------------------------------------------------------- #
# registry queries                                                              #
# --------------------------------------------------------------------------- #

def entity_history(conn, *, uid, as_of_tt=OPEN_END, limit=100):
    b = belief(as_of_tt, "n")
    total = _one(conn, f"MATCH (n:NodeVersion) WHERE n.uid = $uid AND {b} RETURN count(n)",
                 {"uid": uid})
    recs = _rows(conn, f"MATCH (n:NodeVersion) WHERE n.uid = $uid AND {b} "
                       f"RETURN n.vid AS vid, n.uid AS uid, n.label AS label, n.vt_s AS vt_s, "
                       f"n.vt_e AS vt_e, n.tt_s AS tt_s, n.props AS props, n.source AS source, "
                       f"n.prov AS prov ORDER BY n.vt_s, n.vid LIMIT {int(limit)}", {"uid": uid})
    out = [{"vid": n["vid"], "uid": n["uid"], "label": n["label"], "vt_s": n["vt_s"],
            "vt_e": n["vt_e"], "tt_s": n["tt_s"], "tt_e": OPEN_END,
            "props": json.loads(n["props"]), "source": n["source"] or "ingest",
            "provenance_ref": n["prov"]} for n in recs]
    return {"rows": out, **_page(total, len(out))}


def _bucket_counts(conn, as_of_tt, t_a, t_b, stride):
    return {r["i"]: r["c"] for r in _rows(
        conn, f"MATCH ()-[r:E]->() WHERE {belief(as_of_tt, 'r')} "
              f"AND r.vt_s >= $ta AND r.vt_s < $tb "
              f"RETURN (r.vt_s - $ta) / $stride AS i, count(r) AS c",
        {"ta": t_a, "tb": t_b, "stride": stride})}


def graph_metric_timeseries(conn, *, metric, window, stride, as_of_tt=OPEN_END, limit=100):
    if metric != "edge_event_count":
        raise NotImplementedError(metric)
    t_a, t_b = window["t_a"], window["t_b"]
    n = -(-(t_b - t_a) // stride)
    got = _bucket_counts(conn, as_of_tt, t_a, t_b, stride)
    rows = [{"t_a": t_a + i * stride, "t_b": min(t_a + (i + 1) * stride, t_b),
             "value": int(got.get(i, 0))} for i in range(min(n, limit))]
    return {"rows": rows, "rows_total": n, "truncated": n > len(rows), "n_buckets": n}


def aggregate_events(conn, *, group_by, aggregates, window, stride=None, rel_types=None,
                     as_of_tt=OPEN_END, limit=100, cursor=None):
    if [d["dim"] for d in group_by] != ["rel_type", "time_bucket"] \
            or aggregates != [{"agg": "count"}, {"agg": "count_distinct", "of": "dst"}] \
            or rel_types is not None or cursor is not None:
        raise NotImplementedError("only the agg.rel_bucket shape is written")
    t_a, t_b = window["t_a"], window["t_b"]
    recs = _rows(conn, f"MATCH ()-[r:E]->(b:Entity) WHERE {belief(as_of_tt, 'r')} "
                       f"AND r.vt_s >= $ta AND r.vt_s < $tb "
                       f"RETURN r.rel_type AS rel, (r.vt_s - $ta) / $stride AS i, "
                       f"count(r) AS c, count(DISTINCT b.dense_id) AS d ORDER BY rel, i",
                 {"ta": t_a, "tb": t_b, "stride": stride})
    out = [{"rel_type": r["rel"], "t_a": t_a + int(r["i"]) * stride,
            "t_b": min(t_a + (int(r["i"]) + 1) * stride, t_b),
            "count": int(r["c"]), "distinct_dst": int(r["d"])} for r in recs]
    page = out[:limit]
    return {"rows": page, **_page(len(out), len(page))}


def burst_detection(conn, *, target, window, stride, method="zscore", params=None,
                    as_of_tt=OPEN_END, limit=100):
    if method != "zscore" or target.get("kind") != "edge_event_rate":
        raise NotImplementedError(f"{method}/{target.get('kind')}")
    if target.get("rel_type") or target.get("uid"):
        raise NotImplementedError("filtered burst target")
    params = params or {}
    t_a, t_b = window["t_a"], window["t_b"]
    n = -(-(t_b - t_a) // stride)
    got = _bucket_counts(conn, as_of_tt, t_a, t_b, stride)
    series = [float(got.get(i, 0)) for i in range(n)]
    w, z = params.get("w", 10), params.get("z", 3.0)
    flagged = []
    for b in range(n):
        hist = series[max(0, b - w):b]
        if not hist:
            continue
        x = series[b]
        mean = sum(hist) / len(hist)
        std = (sum((h - mean) ** 2 for h in hist) / len(hist)) ** 0.5
        score = abs(x - mean) / std if std > 0 else (0.0 if x == mean else BIG_SCORE)
        score = round(score, 9)
        if score >= z:
            flagged.append({"t_a": t_a + b * stride, "t_b": min(t_a + (b + 1) * stride, t_b),
                            "value": float(x), "score": float(score)})
    page = flagged[:limit]
    return {"rows": page, **_page(len(flagged), len(page)), "n_buckets": n}


def _valid_nodes(conn, t, as_of_tt):
    rows = _rows(conn, f"MATCH (n:NodeVersion) WHERE {belief(as_of_tt, 'n')} "
                       f"AND n.vt_s <= $t AND $t < n.vt_e "
                       f"RETURN n.uid AS uid, n.label AS label, n.vt_s AS vs, n.vid AS vid",
                 {"t": t})
    best: dict[str, tuple] = {}
    for r in rows:   # canonical version per uid: max (vt_s, vid), as collect()[0] DESC did
        cur = best.get(r["uid"])
        if cur is None or (r["vs"], r["vid"]) > (cur[0], cur[1]):
            best[r["uid"]] = (r["vs"], r["vid"], r["label"])
    ids = {r["uid"]: r["id"] for r in _rows(
        conn, "MATCH (e:Entity) WHERE e.uid IN $u RETURN e.uid AS uid, e.dense_id AS id",
        {"u": sorted(best)})}
    return {int(ids[u]): (u, v[2]) for u, v in best.items() if u in ids}


def snapshot_subgraph(conn, *, seeds, hops, t_valid, as_of_tt=OPEN_END, limit=100):
    valid = _valid_nodes(conn, t_valid, as_of_tt)
    seed_ids = sorted(i for i, (u, _) in valid.items() if u in set(seeds))
    vlist = sorted(valid)
    eb = belief(as_of_tt, "e")
    dist: dict[int, int] = {i: 0 for i in seed_ids}
    if seed_ids and hops > 0:
        for r in _rows(conn,
                       f"MATCH (s:Entity)-[r:E*1..{int(hops)} (e, n | WHERE e.vt_s <= $t "
                       f"AND $t < e.vt_e AND {eb} AND n.dense_id IN $valid)]-(b:Entity) "
                       f"WHERE s.dense_id IN $seeds AND b.dense_id IN $valid "
                       f"RETURN b.dense_id AS id, min(length(r)) AS h",
                       {"t": t_valid, "valid": vlist, "seeds": seed_ids}):
            i = int(r["id"])
            if i not in dist:
                dist[i] = int(r["h"])
    reached = sorted(dist)
    rb = belief(as_of_tt, "r")
    base = (f"MATCH (a:Entity)-[r:E]->(b:Entity) WHERE a.dense_id IN $d AND b.dense_id IN $d "
            f"AND {rb} AND r.vt_s <= $t AND $t < r.vt_e ")
    total = _one(conn, base + "RETURN count(r)", {"d": reached, "t": t_valid})
    erows = _rows(conn, base + f"RETURN r.eid AS eid, r.vid AS vid, a.uid AS src, b.uid AS dst, "
                               f"r.rel_type AS rel, r.vt_s AS vs, r.vt_e AS ve "
                               f"ORDER BY r.vt_s, r.vid LIMIT {int(limit)}",
                  {"d": reached, "t": t_valid})
    rows = [{"eid": r["eid"], "vid": r["vid"], "src": r["src"], "dst": r["dst"],
             "rel_type": r["rel"], "vt_s": r["vs"], "vt_e": r["ve"]} for r in erows]
    nodes = sorted(({"uid": valid[i][0], "label": valid[i][1], "hop": hh}
                    for i, hh in dist.items()), key=lambda r: (r["hop"], r["uid"]))
    nodes_truncated = len(nodes) > limit
    page = _page(total, len(rows))
    return {"rows": rows, "rows_total": page["rows_total"], "nodes": nodes[:limit],
            "nodes_total": len(nodes), "nodes_truncated": nodes_truncated,
            "truncated": page["truncated"] or nodes_truncated}


def neighborhood_evolution(conn, *, uid, t1, t2, stride=None, as_of_tt=OPEN_END, limit=100):
    stride = stride or max(1, (t2 - t1) // 20)
    rb = belief(as_of_tt, "r")

    def nbrs(t):
        return {r["u"] for r in _rows(
            conn, f"MATCH (e:Entity)-[r:E]-(m:Entity) WHERE e.uid = $uid AND {rb} "
                  f"AND r.vt_s <= $t AND $t < r.vt_e AND m.uid <> $uid RETURN DISTINCT m.uid AS u",
            {"uid": uid, "t": t})}

    m1, m2 = nbrs(t1), nbrs(t2)
    got = {int(r["bs"]): int(r["deg"]) for r in _rows(
        conn, f"UNWIND range($t1, $t2 - 1, $stride) AS bs "
              f"MATCH (e:Entity)-[r:E]-() WHERE e.uid = $uid AND {rb} "
              f"AND r.vt_s <= bs AND r.vt_e > bs RETURN bs, count(DISTINCT r.vid) AS deg",
        {"uid": uid, "t1": t1, "t2": t2, "stride": stride})}
    gained, lost = sorted(m2 - m1), sorted(m1 - m2)
    return {"neighbors_gained": gained[:limit], "neighbors_gained_total": len(gained),
            "neighbors_lost": lost[:limit], "neighbors_lost_total": len(lost),
            "degree_series": [{"t": bs, "degree": got.get(bs, 0)} for bs in range(t1, t2, stride)],
            "stride": stride, "truncated": len(gained) > limit or len(lost) > limit}


def temporal_reachability(conn, *, src, window, direction="out", delta_max_wait=None,
                          as_of_tt=OPEN_END, limit=100):
    if direction != "out" or delta_max_wait is not None:
        raise NotImplementedError(f"direction={direction} delta={delta_max_wait}")
    t_a, t_b = window["t_a"], window["t_b"]
    rb = belief(as_of_tt, "r")
    sid = int(_one(conn, "MATCH (e:Entity) WHERE e.uid = $u RETURN e.dense_id", {"u": src}))
    arr = {sid: t_a}
    frontier = [{"id": sid, "arr": t_a}]
    while frontier:
        relaxed = _rows(conn,
                        f"UNWIND $front AS f MATCH (a:Entity)-[r:E]->(b:Entity) "
                        f"WHERE a.dense_id = CAST(f.id AS INT64) AND {rb} AND r.vt_e > $ta AND r.vt_s < $tb "
                        f"WITH b, r, CASE WHEN CAST(f.arr AS INT64) > r.vt_s "
                        f"THEN CAST(f.arr AS INT64) ELSE r.vt_s END AS tau "
                        f"WHERE tau < r.vt_e AND tau < $tb "
                        f"RETURN b.dense_id AS id, min(tau) AS arr",
                        {"front": frontier, "ta": t_a, "tb": t_b})
        frontier = []
        for r in relaxed:
            i, a = int(r["id"]), int(r["arr"])
            if i not in arr or a < arr[i]:
                arr[i] = a
                frontier.append({"id": i, "arr": a})
    ids = sorted(i for i in arr if i != sid)
    uids = {int(r["id"]): r["uid"] for r in _rows(
        conn, "MATCH (e:Entity) WHERE e.dense_id IN $ids RETURN e.dense_id AS id, e.uid AS uid",
        {"ids": ids})} if ids else {}
    rows = sorted(({"uid": uids[i], "earliest_arrival": arr[i]} for i in ids),
                  key=lambda r: (r["earliest_arrival"], r["uid"]))
    out = rows[:limit]
    return {"rows": out, **_page(len(rows), len(out))}


def temporal_paths(conn, *, src, dst, window, k=5, max_hops=4, as_of_tt=OPEN_END):
    t_a, t_b = window["t_a"], window["t_b"]
    rb = belief(as_of_tt, "r")
    ids = {r["uid"]: int(r["id"]) for r in _rows(
        conn, "MATCH (e:Entity) WHERE e.uid IN $u RETURN e.uid AS uid, e.dense_id AS id",
        {"u": [src, dst]})}
    sid, did = ids[src], ids[dst]
    states = [{"node": sid, "arr": t_a, "seen": [sid], "key": "", "edges": []}]
    done = []
    for _ in range(max_hops):
        live = [st for st in states if st["node"] != did]
        if not live:
            break
        expanded = _rows(conn,
                         f"UNWIND $states AS st MATCH (a:Entity)-[r:E]->(b:Entity) "
                         f"WHERE a.dense_id = CAST(st.node AS INT64) AND {rb} "
                         f"AND r.vt_e > $ta AND r.vt_s < $tb "
                         f"WITH st, r, a, b, CASE WHEN CAST(st.arr AS INT64) > r.vt_s "
                         f"THEN CAST(st.arr AS INT64) ELSE r.vt_s END AS tau "
                         f"WHERE tau < r.vt_e AND tau < $tb "
                         f"RETURN st.i AS i, b.dense_id AS nid, tau, r.eid AS eid, "
                         f"a.uid AS asrc, b.uid AS bdst, r.rel_type AS rel, r.vt_s AS t",
                         {"states": [{"i": j, "node": st["node"], "arr": st["arr"]}
                                     for j, st in enumerate(live)],
                          "ta": t_a, "tb": t_b})
        states = []
        for row in expanded:
            st = live[int(row["i"])]
            nid = int(row["nid"])
            if nid in st["seen"]:
                continue
            nst = {"node": nid, "arr": int(row["tau"]), "seen": st["seen"] + [nid],
                   "key": st["key"] + f"{int(row['t']):019d}" + row["eid"],
                   "edges": st["edges"] + [{"src": row["asrc"], "dst": row["bdst"],
                                            "rel_type": row["rel"], "eid": row["eid"],
                                            "t": int(row["t"])}]}
            (done if nid == did else states).append(nst)
    done.sort(key=lambda p: (p["arr"], len(p["edges"]), p["key"]))
    out = [{"arrival": p["arr"], "hops": len(p["edges"]), "edges": p["edges"]} for p in done[:k]]
    return {"rows": out, "rows_total": len(done), "truncated": len(done) > k}


def diff_snapshots(conn, *, t1, t2, as_of_tt=OPEN_END, scope=None, limit=100):
    if scope is not None:
        raise NotImplementedError("scoped diff")
    rb, nb = belief(as_of_tt, "r"), belief(as_of_tt, "n")

    def edge_state(t):
        st: dict[str, tuple] = {}
        for r in _rows(conn, f"MATCH (a:Entity)-[r:E]->(b:Entity) WHERE {rb} "
                             f"AND r.vt_s <= $t AND $t < r.vt_e "
                             f"RETURN r.eid AS eid, a.uid AS src, b.uid AS dst, r.rel_type AS rel, "
                             f"r.vid AS vid, r.vt_s AS vs, r.props AS props", {"t": t}):
            cur = st.get(r["eid"])
            if cur is None or (r["vs"], r["vid"]) > (cur[4], cur[3]):
                st[r["eid"]] = (r["src"], r["dst"], r["rel"], r["vid"], r["vs"], r["props"])
        return st

    def node_state(t):
        st: dict[str, tuple] = {}
        for r in _rows(conn, f"MATCH (n:NodeVersion) WHERE {nb} AND n.vt_s <= $t AND $t < n.vt_e "
                             f"RETURN n.uid AS uid, n.label AS label, n.vid AS vid, n.vt_s AS vs, "
                             f"n.props AS props", {"t": t}):
            cur = st.get(r["uid"])
            if cur is None or (r["vs"], r["vid"]) > (cur[3], cur[1]):
                st[r["uid"]] = (r["label"], r["vid"], r["props"], r["vs"])
        return st

    n1, n2 = node_state(t1), node_state(t2)
    e1, e2 = edge_state(t1), edge_state(t2)
    nodes_added = sorted(u for u in n2 if u not in n1)
    nodes_removed = sorted(u for u in n1 if u not in n2)

    def edesc(eid, st):
        return {"eid": eid, "src": st[eid][0], "dst": st[eid][1], "rel_type": st[eid][2]}

    edges_added = [edesc(e, e2) for e in sorted(e for e in e2 if e not in e1)]
    edges_removed = [edesc(e, e1) for e in sorted(e for e in e1 if e not in e2)]
    changed: list[dict[str, Any]] = []
    for u in sorted(u for u in n1 if u in n2 and n1[u][1] != n2[u][1]):
        (la, _va, pa, _), (lb, _vb, pb, _) = n1[u], n2[u]
        if json.loads(pa) != json.loads(pb) or la != lb:
            changed.append({"kind": "node", "id": u,
                            "from": {"label": la, "props": json.loads(pa)},
                            "to": {"label": lb, "props": json.loads(pb)}})
    for e in sorted(e for e in e1 if e in e2 and e1[e][3] != e2[e][3]):
        pa, pb = e1[e][5], e2[e][5]
        if json.loads(pa) != json.loads(pb):
            changed.append({"kind": "edge", "id": e, "from": {"props": json.loads(pa)},
                            "to": {"props": json.loads(pb)}})
    lists = {"nodes_added": nodes_added, "nodes_removed": nodes_removed,
             "edges_added": edges_added, "edges_removed": edges_removed,
             "props_changed": changed}
    out: dict[str, Any] = {}
    for kk, v in lists.items():
        out[kk], out[f"{kk}_total"] = v[:limit], len(v)
    out["truncated"] = any(len(v) > limit for v in lists.values())
    return out


def _spec_where(spec, rel, src, dst, params, tag):
    parts = []
    for key, expr in (("rel_type", f"{rel}.rel_type"), ("src", f"{src}.uid"), ("dst", f"{dst}.uid")):
        if spec.get(key):
            params[f"{tag}_{key}"] = spec[key]
            parts.append(f"{expr} = ${tag}_{key}")
    return (" AND " + " AND ".join(parts)) if parts else ""


def co_active(conn, *, a_spec, b_spec, allen_relation, as_of_tt=OPEN_END, limit=100):
    if allen_relation.get("relation") != "overlaps":
        raise NotImplementedError(allen_relation.get("relation"))
    params: dict[str, Any] = {}
    where = (f"{belief(as_of_tt, 'ra')} AND {belief(as_of_tt, 'rb')} "
             f"AND ra.vt_s < rb.vt_s AND rb.vt_s < ra.vt_e AND ra.vt_e < rb.vt_e "
             f"AND ra.vid <> rb.vid"
             f"{_spec_where(a_spec, 'ra', 'sa', 'da', params, 'a')}"
             f"{_spec_where(b_spec, 'rb', 'sb', 'db', params, 'b')}")
    pat = "MATCH (sa:Entity)-[ra:E]->(da:Entity), (sb:Entity)-[rb:E]->(db:Entity) "
    total = _one(conn, f"{pat} WHERE {where} RETURN count(*)", params)
    rows = _rows(conn, f"{pat} WHERE {where} "
                       f"RETURN ra.eid AS ae, ra.vid AS av, sa.uid AS asrc, da.uid AS adst, "
                       f"ra.rel_type AS ar, ra.vt_s AS avs, ra.vt_e AS ave, "
                       f"rb.eid AS be, rb.vid AS bv, sb.uid AS bsrc, db.uid AS bdst, "
                       f"rb.rel_type AS br, rb.vt_s AS bvs, rb.vt_e AS bve "
                       f"ORDER BY avs, av, bvs, bv LIMIT {int(limit)}", params)
    out = [{"a": {"eid": r["ae"], "vid": r["av"], "src": r["asrc"], "dst": r["adst"],
                  "rel_type": r["ar"], "vt_s": r["avs"], "vt_e": r["ave"]},
            "b": {"eid": r["be"], "vid": r["bv"], "src": r["bsrc"], "dst": r["bdst"],
                  "rel_type": r["br"], "vt_s": r["bvs"], "vt_e": r["bve"]}} for r in rows]
    return {"rows": out, **_page(total, len(out))}


def resolve_entities(conn, *, query, label=None, as_of_tt=OPEN_END, limit=100):
    nb = belief(as_of_tt, "n")
    ql = query.lower()
    scored = {r["uid"]: int(r["m"]) for r in _rows(
        conn, f"MATCH (n:NodeVersion) WHERE {nb} "
              f"WITH n, CASE WHEN n.uid = $q THEN 0 "
              f"WHEN contains(lower(n.uid), $ql) THEN 1 "
              f"WHEN n.name IS NOT NULL AND n.name <> '' AND contains(lower(n.name), $ql) THEN 2 "
              f"ELSE 9 END AS sc "
              f"WITH n.uid AS uid, min(sc) AS m WHERE m < 9 RETURN uid, m",
        {"q": query, "ql": ql})}
    canon: dict[str, tuple] = {}
    if scored:
        best: dict[str, tuple] = {}
        for r in _rows(conn, f"MATCH (n:NodeVersion) WHERE {nb} AND n.uid IN $uids "
                             f"RETURN n.uid AS uid, n.vt_s AS vs, n.vid AS vid, n.label AS label, "
                             f"n.props AS props", {"uids": sorted(scored)}):
            cur = best.get(r["uid"])
            if cur is None or (r["vs"], r["vid"]) > (cur[0], cur[1]):
                best[r["uid"]] = (r["vs"], r["vid"], r["label"], r["props"])
        canon = {u: (v[2], v[3]) for u, v in best.items()}
    rows = [{"uid": u, "label": canon[u][0], "name": json.loads(canon[u][1]).get("name"),
             "match": m} for u, m in scored.items() if label is None or canon[u][0] == label]
    rows.sort(key=lambda r: (r["match"], r["uid"]))
    out = rows[:limit]
    return {"rows": out, **_page(len(rows), len(out))}


def count_temporal_motifs(conn, *, motif, delta, window, node_filter=None, as_of_tt=OPEN_END):
    if motif != "M_triangle_cyclic":
        raise NotImplementedError(motif)
    t_a, t_b = window["t_a"], window["t_b"]
    params: dict[str, Any] = {"ta": t_a, "tb": t_b, "d": delta}
    nf_ev = ""
    if node_filter is not None:
        params["nf"] = sorted(set(node_filter))
        nf_ev = " AND x.uid IN $nf AND y.uid IN $nf"
    n_events = _one(conn, f"MATCH (x:Entity)-[r:E]->(y:Entity) WHERE {belief(as_of_tt, 'r')} "
                          f"AND r.vt_s >= $ta AND r.vt_s < $tb{nf_ev} RETURN count(r)",
                    {k: v for k, v in params.items() if k != "d"})
    # Staged MATCH, not the single cyclic pattern: on this store the one-pattern
    # form `(x)-[a]->(y)-[b]->(z)-[c]->(x)` returned 167 against the 236 every
    # other system returns at 200k (parallel edge versions between one node
    # pair are under-counted when the cycle is closed in one pattern); the
    # staged form returns 236. Recorded in the Phase A report.
    nf_x = " AND x.uid IN $nf AND y.uid IN $nf" if node_filter is not None else ""
    nf_z = " AND z.uid IN $nf" if node_filter is not None else ""
    cnt = _one(conn,
               f"MATCH (x:Entity)-[a:E]->(y:Entity) WHERE {belief(as_of_tt, 'a')}{nf_x} "
               f"AND a.vt_s >= $ta AND a.vt_s < $tb "
               f"WITH x, y, a MATCH (y)-[b:E]->(z:Entity) WHERE {belief(as_of_tt, 'b')}{nf_z} "
               f"AND b.vt_s >= $ta AND b.vt_s < $tb "
               f"WITH x, y, z, a, b MATCH (z)-[c:E]->(x) WHERE {belief(as_of_tt, 'c')} "
               f"AND c.vt_s >= $ta AND c.vt_s < $tb "
               f"AND (b.vt_s > a.vt_s OR (b.vt_s = a.vt_s AND b.eid > a.eid)) "
               f"AND (c.vt_s > b.vt_s OR (c.vt_s = b.vt_s AND c.eid > b.eid)) "
               f"AND c.vt_s - a.vt_s <= $d AND b.vt_s - a.vt_s <= $d "
               f"AND x.dense_id <> y.dense_id AND y.dense_id <> z.dense_id "
               f"AND z.dense_id <> x.dense_id RETURN count(*)", params)
    return {"count": int(cnt), "n_events_in_window": int(n_events), "truncated": False}


QUERIES = {
    "hist.single": entity_history, "hist.asof": entity_history,
    "snap.hop2": snapshot_subgraph, "reach.window": temporal_reachability,
    "paths.k": temporal_paths, "series.count": graph_metric_timeseries,
    "burst.zscore": burst_detection, "agg.rel_bucket": aggregate_events,
    "nbr.evolution": neighborhood_evolution, "diff.global": diff_snapshots,
    "coactive.narrow": co_active, "resolve.substr": resolve_entities,
    "motif.filtered": count_temporal_motifs,
}


# --------------------------------------------------------------------------- #
# system                                                                        #
# --------------------------------------------------------------------------- #

class Ladybug(X._QueryModule):
    name = "ladybug"

    @property
    def path(self) -> Path:
        return self.workdir / "stores" / f"ladybug-{self.size_tag}"

    def load(self, log):
        import ladybug as lb
        import pyarrow as pa
        import pyarrow.parquet as pq
        if self.path.exists():
            raise SystemExit(f"{self.path} exists; refusing to reload over it")
        stage = self.workdir / "scratch" / f"ladybug-stage-{self.size_tag}"
        stage.mkdir(parents=True, exist_ok=True)
        adapter = self.ref_adapter()
        t0 = time.perf_counter()
        uids = adapter.uids_for(list(range(adapter.num_entities())))
        dense = {u: i for i, u in enumerate(uids)}
        pq.write_table(pa.table({"dense_id": pa.array(range(len(uids)), pa.int64()),
                                 "uid": pa.array(uids, pa.string())}), stage / "entity.parquet")
        cols: dict[str, list] = {k: [] for k in ("vid", "uid", "label", "vt_s", "vt_e", "tt_s",
                                                  "tt_e", "props", "source", "prov", "name")}
        for v in adapter.all_node_versions():
            nm = v.props.get("name")
            for k, val in (("vid", v.vid), ("uid", v.uid), ("label", v.label), ("vt_s", v.vt_s),
                           ("vt_e", v.vt_e), ("tt_s", v.tt_s), ("tt_e", v.tt_e),
                           ("props", canonical_json(v.props)), ("source", v.source),
                           ("prov", v.provenance_ref),
                           ("name", nm if isinstance(nm, str) else None)):
                cols[k].append(val)
        pq.write_table(pa.table({k: pa.array(v, pa.int64() if k in ("vt_s", "vt_e", "tt_s", "tt_e")
                                             else pa.string()) for k, v in cols.items()}),
                       stage / "nodeversion.parquet")
        n_nv = len(cols["vid"])
        ecols = ("from", "to", "eid", "vid", "rel_type", "disc", "vt_s", "vt_e", "tt_s", "tt_e",
                 "props", "source", "prov")
        writer = None
        buf: dict[str, list] = {k: [] for k in ecols}
        n_e = 0

        def flush():
            nonlocal writer
            tbl = pa.table({k: pa.array(v, pa.int64() if k in ("from", "to", "vt_s", "vt_e", "tt_s", "tt_e")
                                        else pa.string()) for k, v in buf.items()})
            if writer is None:
                writer = pq.ParquetWriter(stage / "e.parquet", tbl.schema)
            writer.write_table(tbl)
            for v in buf.values():
                v.clear()

        for v in adapter.all_edge_versions():
            for k, val in (("from", dense[v.src]), ("to", dense[v.dst]), ("eid", v.eid),
                           ("vid", v.vid), ("rel_type", v.rel_type), ("disc", v.disc),
                           ("vt_s", v.vt_s), ("vt_e", v.vt_e), ("tt_s", v.tt_s), ("tt_e", v.tt_e),
                           ("props", canonical_json(v.props)), ("source", v.source),
                           ("prov", v.provenance_ref)):
                buf[k].append(val)
            n_e += 1
            if len(buf["eid"]) >= 500_000:
                flush()
        if buf["eid"]:
            flush()
        writer.close()
        t1 = time.perf_counter()
        db = lb.Database(str(self.path), buffer_pool_size=BUFFER_POOL)
        conn = lb.Connection(db)
        for stmt in SCHEMA:
            conn.execute(stmt)
        conn.execute(f"COPY Entity FROM '{stage / 'entity.parquet'}'")
        conn.execute(f"COPY NodeVersion FROM '{stage / 'nodeversion.parquet'}'")
        conn.execute(f"COPY E FROM '{stage / 'e.parquet'}'")
        conn.execute("CHECKPOINT")
        t2 = time.perf_counter()
        got = {"entities": _one(conn, "MATCH (e:Entity) RETURN count(e)"),
               "node_versions": _one(conn, "MATCH (n:NodeVersion) RETURN count(n)"),
               "edge_versions": _one(conn, "MATCH ()-[r:E]->() RETURN count(r)")}
        want = {"entities": len(uids), "node_versions": n_nv, "edge_versions": n_e}
        conn.close()
        db.close()
        if got != want:
            raise SystemExit(f"ladybug load: sent {want}, landed {got}")
        stage_bytes = X._dir_bytes(stage)
        shutil.rmtree(stage)
        return {"load_s": round(t2 - t0, 3),
                "phases": {"export_parquet_s": round(t1 - t0, 3), "copy_from_s": round(t2 - t1, 3)},
                "counts": got, "bytes": X._dir_bytes(self.path) if self.path.is_dir()
                else self.path.stat().st_size, "staging_parquet_bytes": stage_bytes}

    def open(self):
        import ladybug as lb
        self.module = __import__(__name__)
        kw = {"max_num_threads": THREADS} if THREADS else {}
        self.db = lb.Database(str(self.path), buffer_pool_size=BUFFER_POOL, read_only=True, **kw)
        self.handle = lb.Connection(self.db, num_threads=THREADS)
        # LadybugDB's own timeout 5 s below the harness's client alarm (both were VERIFY_CAP_S): when they fired
        # together, the alarm landed inside the handler of LadybugDB's "Interrupted." and ended the window
        #
        self.handle.set_query_timeout(QUERY_TIMEOUT_MS)
        import prime_cap as PC
        PC.hook(lambda sec: self.handle.set_query_timeout(int(sec * 1000) if sec else QUERY_TIMEOUT_MS))

    def settings(self):
        import importlib.metadata as md
        return {"ladybug": md.version("ladybug"),
                "buffer_pool_size": BUFFER_POOL, "buffer_pool_rule": BUFFER_POOL_RULE,
                "threads": THREADS or "default (all cores the process sees)",
                "cpus_available": len(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else os.cpu_count(),
                "prepared_cache_max": CACHE_MAX,
                "read_only": True, "query_timeout_ms": QUERY_TIMEOUT_MS}

    def cancel(self):
        self.handle.interrupt()

    def close(self):
        self.handle.close()
        self.db.close()
