"""XTDB 2 for the external comparison: the registry in XTDB SQL.

Loading is XTDB's own idiom, unchanged from the August evaluation
(`xtdb_baseline.replay`): the frozen event log's *ops* are replayed, one XTDB
transaction per batch at `SYSTEM_TIME = map(tt)`, and XTDB performs its own
SQL:2011 portion supersession. Nothing pre-resolved crosses: belief time is
XTDB's system time, valid time is XTDB's valid time, both native.

Queries use the temporal table clauses (`FOR VALID_TIME AS OF`, `FOR ALL
VALID_TIME`, `FOR SYSTEM_TIME AS OF` for as-of belief; current belief is
XTDB's default). Times cross as microsecond integers through
`(t - TIMESTAMP '1970-01-01Z') / INTERVAL 'PT0.000001S'`, which XTDB evaluates
to an integer; buckets are the same quotient against the bucket width.

Version ids are not stored anywhere in XTDB. Gnomon defines a version id as
`sha256("{identity}:{tt_s}:{vt_s}")[:24]` (`derive.rs::version_vid`), a pure
function of fields XTDB does return (`_id`, `_system_from`, `_valid_from`),
so the adapter computes it client-side to order and page rows the way the
operators do. Where XTDB's fragments carry a different `_system_from` than
Gnomon's versions (a portion update keeps the untouched fragment's original
system time, where Gnomon writes a fresh version), ids differ and the verdict
falls back to the projected comparison (ids and belief stamps removed).

XTDB has no recursive SQL (the reference says `WITH RECURSIVE` is not yet
supported) and no path primitive, so `snap.hop2`, `reach.window` and `paths.k`
run as client-driven rounds, one SQL statement per round — the same shape the
ClickHouse baseline uses for the same reason. The motif is a fixed-length
pattern and is one three-way self-join.

`resolve.substr` matches inside `props`, which the replay stores as canonical
JSON text: the SQL selects the candidate versions (`LOWER(props) LIKE`, a
superset), the name-field test runs on that candidate set after parsing.
"""

from __future__ import annotations

import hashlib
import json
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import extcmp as X
from neo4j_queries import BIG_SCORE, OPEN_END, _page

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
E = "TIMESTAMP '1970-01-01T00:00:00Z'"
US = "INTERVAL 'PT0.000001S'"


def tsl(t: int) -> str:
    d = EPOCH + timedelta(microseconds=int(t))
    return "TIMESTAMP '" + d.strftime("%Y-%m-%dT%H:%M:%S.%fZ") + "'"


def us(col: str) -> str:
    return f"(({col} - {E}) / {US})"


def width(stride: int) -> str:
    s, u = divmod(int(stride), 1_000_000)
    return f"INTERVAL 'PT{s}.{u:06d}S'"


def sys_clause(as_of_tt: int) -> str:
    if as_of_tt >= OPEN_END:
        return ""                      # XTDB's default: current belief
    return f" FOR SYSTEM_TIME AS OF {tsl(min(as_of_tt, OPEN_END - 1))}"


def vid(identity: str, tt_s: int, vt_s: int) -> str:
    return hashlib.sha256(f"{identity}:{tt_s}:{vt_s}".encode()).hexdigest()[:24]


def _q(cur, sql: str, params=None) -> list[tuple]:
    cur.execute(sql, params)
    return cur.fetchall()


def _ve(x) -> int:
    return OPEN_END if x is None else int(x)


def _inlist(xs) -> str:
    return "(" + ", ".join("'" + str(x).replace("'", "''") + "'" for x in xs) + ")"


V_COLS = f"{us('_valid_from')}, {us('_valid_to')}, {us('_system_from')}"


# --------------------------------------------------------------------------- #

def entity_history(cur, *, uid, as_of_tt=OPEN_END, limit=100):
    rows = _q(cur, f"SELECT label, props, source, provenance_ref, {V_COLS} "
                   f"FROM nodes FOR ALL VALID_TIME{sys_clause(as_of_tt)} WHERE _id = %s", (uid,))
    out = [{"vid": vid(uid, int(ss), int(vs)), "uid": uid, "label": lab, "vt_s": int(vs),
            "vt_e": _ve(ve), "tt_s": int(ss), "tt_e": OPEN_END, "props": json.loads(props),
            "source": src or "ingest", "provenance_ref": prov}
           for lab, props, src, prov, vs, ve, ss in rows]
    out.sort(key=lambda r: (r["vt_s"], r["vid"]))
    page = out[:limit]
    return {"rows": page, **_page(len(out), len(page))}


def _valid_nodes(cur, t, as_of_tt):
    best: dict[str, tuple] = {}
    for uid, lab, vs, ss in _q(cur, f"SELECT _id, label, {us('_valid_from')}, {us('_system_from')} "
                                    f"FROM nodes FOR VALID_TIME AS OF {tsl(t)}{sys_clause(as_of_tt)}"):
        key = (int(vs), vid(uid, int(ss), int(vs)))
        if uid not in best or key > best[uid][0]:
            best[uid] = (key, lab)
    return {u: v[1] for u, v in best.items()}


def snapshot_subgraph(cur, *, seeds, hops, t_valid, as_of_tt=OPEN_END, limit=100):
    valid = _valid_nodes(cur, t_valid, as_of_tt)
    sc = sys_clause(as_of_tt)
    dist = {s: 0 for s in seeds if s in valid}
    frontier = sorted(dist)
    for h in range(1, int(hops) + 1):
        if not frontier:
            break
        il = _inlist(frontier)
        nb = set()
        for a, b in _q(cur, f"SELECT src, dst FROM edges FOR VALID_TIME AS OF {tsl(t_valid)}{sc} "
                            f"WHERE src IN {il} OR dst IN {il}"):
            if a in frontier:
                nb.add(b)
            if b in frontier:
                nb.add(a)
        new = sorted(u for u in nb if u not in dist and u in valid)
        for u in new:
            dist[u] = h
        frontier = new
    reached = sorted(dist)
    erows = []
    if reached:
        il = _inlist(reached)
        for eid, s, d, rel, vs, ve, ss in _q(
                cur, f"SELECT _id, src, dst, rel_type, {V_COLS} FROM edges "
                     f"FOR VALID_TIME AS OF {tsl(t_valid)}{sc} WHERE src IN {il} AND dst IN {il}"):
            erows.append({"eid": eid, "vid": vid(eid, int(ss), int(vs)), "src": s, "dst": d,
                          "rel_type": rel, "vt_s": int(vs), "vt_e": _ve(ve)})
    erows.sort(key=lambda r: (r["vt_s"], r["vid"]))
    nodes = sorted(({"uid": u, "label": valid[u], "hop": hh} for u, hh in dist.items()),
                   key=lambda r: (r["hop"], r["uid"]))
    rows = erows[:limit]
    nodes_truncated = len(nodes) > limit
    page = _page(len(erows), len(rows))
    return {"rows": rows, "rows_total": page["rows_total"], "nodes": nodes[:limit],
            "nodes_total": len(nodes), "nodes_truncated": nodes_truncated,
            "truncated": page["truncated"] or nodes_truncated}


def _buckets(cur, as_of_tt, t_a, t_b, stride):
    # XTDB's grammar takes column references, not expressions, in GROUP BY:
    # the bucket index is computed in a derived table and grouped outside it.
    return _q(cur, f"SELECT s.i, count(*) FROM (SELECT (_valid_from - {tsl(t_a)}) / {width(stride)} AS i "
                   f"FROM edges FOR ALL VALID_TIME{sys_clause(as_of_tt)} "
                   f"WHERE _valid_from >= {tsl(t_a)} AND _valid_from < {tsl(t_b)}) AS s GROUP BY s.i")


def graph_metric_timeseries(cur, *, metric, window, stride, as_of_tt=OPEN_END, limit=100):
    if metric != "edge_event_count":
        raise NotImplementedError(metric)
    t_a, t_b = window["t_a"], window["t_b"]
    n = -(-(t_b - t_a) // stride)
    got = {int(i): int(c) for i, c in _buckets(cur, as_of_tt, t_a, t_b, stride)}
    rows = [{"t_a": t_a + i * stride, "t_b": min(t_a + (i + 1) * stride, t_b),
             "value": got.get(i, 0)} for i in range(min(n, limit))]
    return {"rows": rows, "rows_total": n, "truncated": n > len(rows), "n_buckets": n}


def aggregate_events(cur, *, group_by, aggregates, window, stride=None, rel_types=None,
                     as_of_tt=OPEN_END, limit=100, cursor=None):
    if [d["dim"] for d in group_by] != ["rel_type", "time_bucket"] \
            or aggregates != [{"agg": "count"}, {"agg": "count_distinct", "of": "dst"}] \
            or rel_types is not None or cursor is not None:
        raise NotImplementedError("only the agg.rel_bucket shape is written")
    t_a, t_b = window["t_a"], window["t_b"]
    recs = _q(cur, f"SELECT s.rel_type, s.i, count(*), count(DISTINCT s.dst) FROM "
                   f"(SELECT rel_type, dst, (_valid_from - {tsl(t_a)}) / {width(stride)} AS i "
                   f"FROM edges FOR ALL VALID_TIME{sys_clause(as_of_tt)} "
                   f"WHERE _valid_from >= {tsl(t_a)} AND _valid_from < {tsl(t_b)}) AS s "
                   f"GROUP BY s.rel_type, s.i")
    out = [{"rel_type": rel, "t_a": t_a + int(i) * stride,
            "t_b": min(t_a + (int(i) + 1) * stride, t_b), "count": int(c), "distinct_dst": int(d)}
           for rel, i, c, d in recs]
    out.sort(key=lambda r: (r["rel_type"], r["t_a"]))
    page = out[:limit]
    return {"rows": page, **_page(len(out), len(page))}


def burst_detection(cur, *, target, window, stride, method="zscore", params=None,
                    as_of_tt=OPEN_END, limit=100):
    if method != "zscore" or target.get("kind") != "edge_event_rate":
        raise NotImplementedError(f"{method}/{target.get('kind')}")
    if target.get("rel_type") or target.get("uid"):
        raise NotImplementedError("filtered burst target")
    params = params or {}
    t_a, t_b = window["t_a"], window["t_b"]
    n = -(-(t_b - t_a) // stride)
    got = {int(i): int(c) for i, c in _buckets(cur, as_of_tt, t_a, t_b, stride)}
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


def neighborhood_evolution(cur, *, uid, t1, t2, stride=None, as_of_tt=OPEN_END, limit=100):
    stride = stride or max(1, (t2 - t1) // 20)
    sc = sys_clause(as_of_tt)

    def nbrs(t):
        out = set()
        for a, b in _q(cur, f"SELECT src, dst FROM edges FOR VALID_TIME AS OF {tsl(t)}{sc} "
                            f"WHERE src = %s OR dst = %s", (uid, uid)):
            out.add(b if a == uid else a)
        out.discard(uid)
        return out

    m1, m2 = nbrs(t1), nbrs(t2)
    got = {int(bs): int(c) for bs, c in _q(
        cur, f"SELECT g.bs, count(e._id) FROM generate_series({int(t1)}, {int(t2)}, {int(stride)}) "
             f"AS g(bs) JOIN edges FOR ALL VALID_TIME{sc} AS e "
             f"ON (e.src = %s OR e.dst = %s) AND {us('e._valid_from')} <= g.bs "
             f"AND g.bs < COALESCE({us('e._valid_to')}, {OPEN_END}) GROUP BY g.bs", (uid, uid))}
    gained, lost = sorted(m2 - m1), sorted(m1 - m2)
    return {"neighbors_gained": gained[:limit], "neighbors_gained_total": len(gained),
            "neighbors_lost": lost[:limit], "neighbors_lost_total": len(lost),
            "degree_series": [{"t": bs, "degree": got.get(bs, 0)} for bs in range(t1, t2, stride)],
            "stride": stride, "truncated": len(gained) > limit or len(lost) > limit}


def _values(rows: list[tuple]) -> str:
    return ", ".join("(" + ", ".join(("'" + v.replace("'", "''") + "'") if isinstance(v, str)
                                     else str(int(v)) for v in r) + ")" for r in rows)


def temporal_reachability(cur, *, src, window, direction="out", delta_max_wait=None,
                          as_of_tt=OPEN_END, limit=100):
    if direction != "out" or delta_max_wait is not None:
        raise NotImplementedError(f"direction={direction} delta={delta_max_wait}")
    t_a, t_b = window["t_a"], window["t_b"]
    sc = sys_clause(as_of_tt)
    vf, vt = us("e._valid_from"), f"COALESCE({us('e._valid_to')}, {OPEN_END})"
    arr = {src: t_a}
    frontier = [(src, t_a)]
    while frontier:
        relaxed = _q(cur, f"SELECT e.dst, MIN(GREATEST(f.a, {vf})) FROM "
                          f"(VALUES {_values(frontier)}) AS f(u, a) "
                          f"JOIN edges FOR ALL VALID_TIME{sc} AS e ON e.src = f.u "
                          f"WHERE {vt} > {int(t_a)} AND {vf} < {int(t_b)} "
                          f"AND GREATEST(f.a, {vf}) < {vt} AND GREATEST(f.a, {vf}) < {int(t_b)} "
                          f"GROUP BY e.dst")
        frontier = []
        for d, a in relaxed:
            a = int(a)
            if d not in arr or a < arr[d]:
                arr[d] = a
                frontier.append((d, a))
    rows = sorted(({"uid": u, "earliest_arrival": a} for u, a in arr.items() if u != src),
                  key=lambda r: (r["earliest_arrival"], r["uid"]))
    out = rows[:limit]
    return {"rows": out, **_page(len(rows), len(out))}


def temporal_paths(cur, *, src, dst, window, k=5, max_hops=4, as_of_tt=OPEN_END):
    t_a, t_b = window["t_a"], window["t_b"]
    sc = sys_clause(as_of_tt)
    vf, vt = us("e._valid_from"), f"COALESCE({us('e._valid_to')}, {OPEN_END})"
    states = [{"node": src, "arr": t_a, "seen": [src], "key": "", "edges": []}]
    done = []
    for _ in range(max_hops):
        live = [st for st in states if st["node"] != dst]
        if not live:
            break
        expanded = _q(cur, f"SELECT f.i, e.dst, GREATEST(f.a, {vf}), e._id, e.src, e.rel_type, {vf} "
                           f"FROM (VALUES {_values([(j, st['node'], st['arr']) for j, st in enumerate(live)])}) "
                           f"AS f(i, u, a) JOIN edges FOR ALL VALID_TIME{sc} AS e ON e.src = f.u "
                           f"WHERE {vt} > {int(t_a)} AND {vf} < {int(t_b)} "
                           f"AND GREATEST(f.a, {vf}) < {vt} AND GREATEST(f.a, {vf}) < {int(t_b)}")
        states = []
        for i, nid, tau, eid, s, rel, t in expanded:
            st = live[int(i)]
            if nid in st["seen"]:
                continue
            nst = {"node": nid, "arr": int(tau), "seen": st["seen"] + [nid],
                   "key": st["key"] + f"{int(t):019d}" + eid,
                   "edges": st["edges"] + [{"src": s, "dst": nid, "rel_type": rel, "eid": eid,
                                            "t": int(t)}]}
            (done if nid == dst else states).append(nst)
    done.sort(key=lambda p: (p["arr"], len(p["edges"]), p["key"]))
    out = [{"arrival": p["arr"], "hops": len(p["edges"]), "edges": p["edges"]} for p in done[:k]]
    return {"rows": out, "rows_total": len(done), "truncated": len(done) > k}


def diff_snapshots(cur, *, t1, t2, as_of_tt=OPEN_END, scope=None, limit=100):
    if scope is not None:
        raise NotImplementedError("scoped diff")
    sc = sys_clause(as_of_tt)

    def edge_state(t):
        st: dict[str, tuple] = {}
        for eid, s, d, rel, props, vs, ss in _q(
                cur, f"SELECT _id, src, dst, rel_type, props, {us('_valid_from')}, {us('_system_from')} "
                     f"FROM edges FOR VALID_TIME AS OF {tsl(t)}{sc}"):
            v = vid(eid, int(ss), int(vs))
            cur_ = st.get(eid)
            if cur_ is None or (int(vs), v) > (cur_[4], cur_[3]):
                st[eid] = (s, d, rel, v, int(vs), props)
        return st

    def node_state(t):
        st: dict[str, tuple] = {}
        for uid, lab, props, vs, ss in _q(
                cur, f"SELECT _id, label, props, {us('_valid_from')}, {us('_system_from')} "
                     f"FROM nodes FOR VALID_TIME AS OF {tsl(t)}{sc}"):
            v = vid(uid, int(ss), int(vs))
            cur_ = st.get(uid)
            if cur_ is None or (int(vs), v) > (cur_[3], cur_[1]):
                st[uid] = (lab, v, props, int(vs))
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
            changed.append({"kind": "node", "id": u, "from": {"label": la, "props": json.loads(pa)},
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


def co_active(cur, *, a_spec, b_spec, allen_relation, as_of_tt=OPEN_END, limit=100):
    """Interval join between the two edge sets.

    Written as a join of two derived tables, each filtered by its own spec:
    the first spelling (one self-join with the spec predicates in ON) ran
    > 10 min at 200k without finishing — XTDB 2.1's planner did not push the
    per-side equality filters below the non-equi join.
    """
    if allen_relation.get("relation") != "overlaps":
        raise NotImplementedError(allen_relation.get("relation"))
    sc = sys_clause(as_of_tt)
    sides, params = [], []
    for spec in (a_spec, b_spec):
        conds = []
        for key in ("rel_type", "src", "dst"):
            if spec.get(key):
                conds.append(f"{key} = %s")
                params.append(spec[key])
        where = (" WHERE " + " AND ".join(conds)) if conds else ""
        sides.append(f"(SELECT _id AS eid, src, dst, rel_type, {us('_valid_from')} AS vs, "
                     f"COALESCE({us('_valid_to')}, {OPEN_END}) AS ve, {us('_system_from')} AS ss "
                     f"FROM edges FOR ALL VALID_TIME{sc}{where})")
    # every projected column aliased: XTDB rejects a projection that names the
    # same column twice, even when qualified by different tables
    recs = _q(cur, f"SELECT a.eid AS ae, a.src AS asrc, a.dst AS adst, a.rel_type AS ar, "
                   f"a.vs AS avs, a.ve AS ave, a.ss AS ass, "
                   f"b.eid AS be, b.src AS bsrc, b.dst AS bdst, b.rel_type AS br, "
                   f"b.vs AS bvs, b.ve AS bve, b.ss AS bss "
                   f"FROM {sides[0]} AS a JOIN {sides[1]} AS b "
                   f"ON a.vs < b.vs AND b.vs < a.ve AND a.ve < b.ve", tuple(params))
    out = []
    for r in recs:
        a = {"eid": r[0], "vid": vid(r[0], int(r[6]), int(r[4])), "src": r[1], "dst": r[2],
             "rel_type": r[3], "vt_s": int(r[4]), "vt_e": int(r[5])}
        b = {"eid": r[7], "vid": vid(r[7], int(r[13]), int(r[11])), "src": r[8], "dst": r[9],
             "rel_type": r[10], "vt_s": int(r[11]), "vt_e": int(r[12])}
        if a["vid"] != b["vid"]:
            out.append({"a": a, "b": b})
    out.sort(key=lambda p: (p["a"]["vt_s"], p["a"]["vid"], p["b"]["vt_s"], p["b"]["vid"]))
    page = out[:limit]
    return {"rows": page, **_page(len(out), len(page))}


def resolve_entities(cur, *, query, label=None, as_of_tt=OPEN_END, limit=100):
    sc = sys_clause(as_of_tt)
    ql = query.lower()
    like = "%" + ql.replace("%", r"\%").replace("_", r"\_") + "%"
    scored: dict[str, int] = {}
    for uid, props in _q(cur, f"SELECT _id, props FROM nodes FOR ALL VALID_TIME{sc} "
                              f"WHERE LOWER(_id) LIKE %s OR LOWER(props) LIKE %s", (like, like)):
        nm = json.loads(props).get("name")
        s = (0 if uid == query else 1 if ql in uid.lower()
             else 2 if isinstance(nm, str) and nm != "" and ql in nm.lower() else 9)
        if s < 9:
            scored[uid] = min(s, scored.get(uid, 9))
    canon: dict[str, tuple] = {}
    if scored:
        best: dict[str, tuple] = {}
        for uid, lab, props, vs, ss in _q(
                cur, f"SELECT _id, label, props, {us('_valid_from')}, {us('_system_from')} "
                     f"FROM nodes FOR ALL VALID_TIME{sc} WHERE _id IN {_inlist(sorted(scored))}"):
            key = (int(vs), vid(uid, int(ss), int(vs)))
            if uid not in best or key > best[uid][0]:
                best[uid] = (key, lab, props)
        canon = {u: (v[1], v[2]) for u, v in best.items()}
    rows = [{"uid": u, "label": canon[u][0], "name": json.loads(canon[u][1]).get("name"),
             "match": m} for u, m in scored.items() if label is None or canon[u][0] == label]
    rows.sort(key=lambda r: (r["match"], r["uid"]))
    out = rows[:limit]
    return {"rows": out, **_page(len(rows), len(out))}


def count_temporal_motifs(cur, *, motif, delta, window, node_filter=None, as_of_tt=OPEN_END):
    if motif != "M_triangle_cyclic":
        raise NotImplementedError(motif)
    t_a, t_b = window["t_a"], window["t_b"]
    sc = sys_clause(as_of_tt)
    win = lambda x: f"{x}._valid_from >= {tsl(t_a)} AND {x}._valid_from < {tsl(t_b)}"  # noqa: E731
    nf = nf3 = ""
    if node_filter is not None:
        il = _inlist(sorted(set(node_filter)))
        nf = f" AND r.src IN {il} AND r.dst IN {il}"
        nf3 = f" AND a.src IN {il} AND a.dst IN {il} AND b.dst IN {il}"
    n_events = _q(cur, f"SELECT count(*) FROM edges FOR ALL VALID_TIME{sc} AS r "
                       f"WHERE {win('r')}{nf}")[0][0]
    av, bv, cv = us("a._valid_from"), us("b._valid_from"), us("c._valid_from")
    cnt = _q(cur, f"SELECT count(*) FROM edges FOR ALL VALID_TIME{sc} AS a "
                  f"JOIN edges FOR ALL VALID_TIME{sc} AS b ON b.src = a.dst "
                  f"JOIN edges FOR ALL VALID_TIME{sc} AS c ON c.src = b.dst AND c.dst = a.src "
                  f"WHERE {win('a')} AND {win('b')} AND {win('c')}{nf3} "
                  f"AND ({bv} > {av} OR ({bv} = {av} AND b._id > a._id)) "
                  f"AND ({cv} > {bv} OR ({cv} = {bv} AND c._id > b._id)) "
                  f"AND {cv} - {av} <= {int(delta)} AND {bv} - {av} <= {int(delta)} "
                  f"AND a.src <> a.dst AND a.dst <> b.dst AND b.dst <> a.src")[0][0]
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

def connect():
    import os

    import psycopg
    from psycopg.types.string import StrDumper
    port = os.environ.get("GNOMON_XTDB_PORT", "15433")
    conn = psycopg.connect(f"host=127.0.0.1 port={port} dbname=xtdb user=xtdb", autocommit=True)
    conn.adapters.register_dumper(str, StrDumper)
    return conn


class XTDB(X._QueryModule):
    name, compare = "xtdb", "exact-or-projected"

    def load(self, log):
        import xtdb_baseline
        conn = connect()
        t0 = time.perf_counter()
        rep = xtdb_baseline.replay(conn, log)
        t1 = time.perf_counter()
        cur = conn.cursor()
        last, stable = None, 0
        while stable < 2:     # indexing caught up: two equal polls 2 s apart
            now = _q(cur, "SELECT (SELECT count(*) FROM edges FOR ALL VALID_TIME), "
                          "(SELECT count(*) FROM nodes FOR ALL VALID_TIME)")[0]
            stable = stable + 1 if now == last else 0
            last = now
            time.sleep(2)
        t2 = time.perf_counter()
        conn.close()
        return {"load_s": round(t2 - t0, 3),
                "phases": {"op_replay_s": round(t1 - t0, 3), "index_settle_s": round(t2 - t1, 3)},
                "replay": rep, "current_rows_edges_nodes_all_valid_time": list(last)}

    def open(self):
        """Wait until the node has indexed the whole log before answering.

        directory, XTDB 2.1 accepts queries as soon as pgwire is up while it is
        still replaying the log tail; a verification started 10 s after start
        read `n1` as `alpha` (pre-correction) and 8 fewer `props_changed`, i.e.
        the last batches were not yet visible. So: poll `xt.txs` until it holds
        one committed transaction per log batch, then two further polls 2 s
        apart with unchanged row counts. The wait is recorded in `settings`."""
        self.module = __import__(__name__)
        conn = connect()
        self.handle = conn.cursor()
        want = None
        log = getattr(self, "log", None)
        if log is not None:
            with open(log) as f:
                want = sum(1 for line in f if '"tt"' in line)
        t0 = time.perf_counter()
        polls = []
        last, stable = None, 0
        while True:
            try:
                ntx = _q(self.handle, "SELECT count(*) FROM xt.txs WHERE committed = true")[0][0]
            except Exception as e:                          # noqa: BLE001
                ntx = f"error: {str(e)[:120]}"
            rows = _q(self.handle, "SELECT (SELECT count(*) FROM edges FOR ALL VALID_TIME), "
                                   "(SELECT count(*) FROM nodes FOR ALL VALID_TIME)")[0]
            polls.append([round(time.perf_counter() - t0, 1), ntx, list(rows)])
            caught_up = want is None or not isinstance(ntx, int) or ntx >= want
            stable = stable + 1 if (rows == last and caught_up) else 0
            last = rows
            if stable >= 2:
                break
            if time.perf_counter() - t0 > 7200:
                raise RuntimeError(f"xtdb did not catch up with the log in 2 h: {polls[-3:]}")
            time.sleep(2)
        self.catchup = {"wait_s": round(time.perf_counter() - t0, 1), "want_txs": want,
                        "polls": polls[:5] + polls[-3:]}

    def settings(self):
        return {"catchup_before_queries": getattr(self, "catchup", None),
                "image": "ghcr.io/xtdb/xtdb:2.1.0 (amd64 config sha256:6dc8aa2c...a061)",
                "config": "image local_config.yaml, ports moved to loopback 15433/18080",
                "jvm": "image defaults", "commit": "sync (default)",
                "wire": "pgwire + psycopg (StrDumper)"}

    def cancel(self):
        self.handle.connection.cancel_safe()

    def close(self):
        self.handle.connection.close()
