"""AeonG for the external comparison: Memgraph v2.3 + a RocksDB history store,
transaction time only (`TT AS <t>` / `TT FROM <t1> TO <t2>` on MATCH).

**What AeonG's transaction time keeps, measured on this build.** Property
updates are reconstructable: a property set at AeonG time 19 reads its old
value at `TT AS 12` and its new value at `TT AS 20`. Deletions are not: a
vertex or an edge deleted at AeonG time t is absent from `TT AS x` for every
x, including times before the delete, immediately and after a full GC cycle
. A
version history therefore cannot be carried by create + delete.

**Loading.** The replayed native store's version history is played into
AeonG as one AeonG transaction per distinct Gnomon transaction time `tt`, in
`tt` order: versions with `tt_s = tt` are created with `tt_e = OPEN_END`,
versions with `tt_e = tt` get `SET tt_e = tt`. AeonG's own clock therefore
advances with Gnomon's belief clock, and `TT AS x` returns each version with
the properties it had at the AeonG time `x` that corresponds to a Gnomon time.

**Queries.** Current belief: the Memgraph baseline's Cypher unchanged
(`memgraph_queries`, predicate `tt_e = OPEN_END` on the property).
As-of belief (`hist.asof`, the registry's one as-of id): AeonG's native
transaction time, `MATCH ... WHERE tt_e = OPEN_END TT AS x`, with `x` the
AeonG time found at load for `tt_epoch1` (bisection on the believed
NodeVersion count). Valid time has no model in AeonG: `vt_s`/`vt_e`
properties, as on Neo4j and Memgraph.

AeonG's base is Memgraph v2.3 (2022): node label-property indexes exist, edge
property indexes do not.
"""

from __future__ import annotations

import json
import os
import time
from collections import defaultdict
from typing import Any

import extcmp as X
from neo4j_queries import OPEN_END, _page

from gnomon.core.model import canonical_json

URI = os.environ.get("GNOMON_AEONG_URI", "bolt://127.0.0.1:17689")

#: AeonG time for the registry's `tt_epoch1` belief point, found at load.
STATE_FILE = "aeong-ttmap-{size}.json"


def connect():
    import neo4j
    return neo4j.GraphDatabase.driver(URI, auth=("neo4j", "neo4j"))


def entity_history_tt(drv, *, uid, as_of_tt=OPEN_END, limit=100, _tt=None):
    """`entity_history` at an AeonG transaction time (native TT)."""
    tt = "" if _tt is None else f" TT AS {int(_tt)}"
    with drv.session() as s:
        recs = s.run(f"MATCH (n:NodeVersion) WHERE n.uid = $uid AND n.tt_e = {OPEN_END}{tt} "
                     f"RETURN n.vid AS vid, n.uid AS uid, n.label AS label, n.vt_s AS vt_s, "
                     f"n.vt_e AS vt_e, n.tt_s AS tt_s, n.props AS props, n.source AS source, "
                     f"n.prov AS prov", uid=uid).data()
    out = [{"vid": n["vid"], "uid": n["uid"], "label": n["label"], "vt_s": n["vt_s"],
            "vt_e": n["vt_e"], "tt_s": n["tt_s"], "tt_e": OPEN_END,
            "props": json.loads(n["props"]), "source": n["source"] or "ingest",
            "provenance_ref": n["prov"]} for n in recs]
    out.sort(key=lambda r: (r["vt_s"], r["vid"]))
    page = out[:limit]
    return {"rows": page, **_page(len(out), len(page))}


def aggregate_events(drv, *, group_by, aggregates, window, stride=None, rel_types=None,
                     as_of_tt=OPEN_END, limit=100, cursor=None):
    """`agg.rel_bucket` without `count(DISTINCT …)`, which AeonG's Memgraph v2.3
    base rejects ("Not yet implemented: DISTINCT function call"): group by
    (rel, bucket, dst) first, then count the groups — one Cypher statement."""
    if [d["dim"] for d in group_by] != ["rel_type", "time_bucket"] \
            or aggregates != [{"agg": "count"}, {"agg": "count_distinct", "of": "dst"}] \
            or rel_types is not None or cursor is not None:
        raise NotImplementedError("only the agg.rel_bucket shape is written")
    if as_of_tt < OPEN_END:
        raise NotImplementedError("as-of goes through TT AS")
    t_a, t_b = window["t_a"], window["t_b"]
    with drv.session() as s:
        recs = s.run(
            f"MATCH ()-[r:E]->(b:Entity) WHERE r.tt_e = {OPEN_END} "
            f"AND r.vt_s >= $ta AND r.vt_s < $tb "
            f"WITH r.rel_type AS rel, (r.vt_s - $ta) / $stride AS i, b.dense_id AS d, count(r) AS c "
            f"RETURN rel, i, sum(c) AS c, count(d) AS d ORDER BY rel, i",
            ta=t_a, tb=t_b, stride=stride).data()
    out = [{"rel_type": r["rel"], "t_a": t_a + int(r["i"]) * stride,
            "t_b": min(t_a + (int(r["i"]) + 1) * stride, t_b),
            "count": int(r["c"]), "distinct_dst": int(r["d"])} for r in recs]
    page = out[:limit]
    return {"rows": page, **_page(len(out), len(page))}


def resolve_entities(drv, *, query: str, label: str | None = None,
                     as_of_tt: int = OPEN_END, limit: int = 100) -> dict[str, Any]:
    """`resolve.substr`: the shared Cypher (`neo4j_queries.resolve_entities`)
    with its second statement's `n.uid IN $uids` filter written as
    `UNWIND $uids AS u MATCH (n:NodeVersion {uid: u})`. With the `IN $uids`
    form (11,111 uids at 10M) AeonG's Memgraph v2.3 base requested a 16.5 GiB
    query-memory chunk and failed against its 40 GiB limit (campaign K10M,
    the UNWIND form answers in ~1.2 s with the native hash.
    Likely cause, not confirmed by a plan: the IN-list filter evaluated per
    scanned vertex instead of an index lookup per uid. Same rows, same order."""
    from neo4j_queries import belief
    nbel = belief(as_of_tt, "n")
    ql = query.lower()
    with drv.session() as s:
        scored = {r["uid"]: int(r["m"]) for r in s.run(
            f"MATCH (n:NodeVersion) WHERE {nbel} "
            f"WITH n, CASE WHEN n.uid = $q THEN 0 "
            f"WHEN toLower(n.uid) CONTAINS $ql THEN 1 "
            f"WHEN n.name IS NOT NULL AND n.name <> '' "
            f"AND toLower(n.name) CONTAINS $ql THEN 2 ELSE 9 END AS sc "
            f"WITH n.uid AS uid, min(sc) AS m WHERE m < 9 "
            f"RETURN uid, m", q=query, ql=ql).data()}
        canon = {}
        if scored:
            for r in s.run(
                    f"UNWIND $uids AS u MATCH (n:NodeVersion {{uid: u}}) WHERE {nbel} "
                    f"WITH n ORDER BY n.vt_s DESC, n.vid DESC "
                    f"WITH n.uid AS uid, collect(n)[0] AS c "
                    f"RETURN uid, c.label AS label, c.props AS props",
                    uids=sorted(scored)).data():
                canon[r["uid"]] = (r["label"], r["props"])
    rows = [{"uid": u, "label": canon[u][0],
             "name": json.loads(canon[u][1]).get("name"), "match": m}
            for u, m in scored.items()
            if label is None or canon[u][0] == label]
    rows.sort(key=lambda r: (r["match"], r["uid"]))
    out = rows[:limit]
    return {"rows": out, **_page(len(rows), len(out))}


class AeonG(X._QueryModule):
    name = "aeong"

    def _state_path(self):
        return self.workdir / "stores" / STATE_FILE.format(size=self.size_tag)

    def load(self, log):
        adapter = self.ref_adapter()
        drv = connect()
        t0 = time.perf_counter()
        with drv.session() as s:
            s.run("MATCH (n) DETACH DELETE n").consume()
            for stmt in ("CREATE INDEX ON :Entity(uid)", "CREATE INDEX ON :Entity(dense_id)",
                         "CREATE INDEX ON :NodeVersion(uid)", "CREATE INDEX ON :NodeVersion(vid)"):
                s.run(stmt).consume()
        uids = adapter.uids_for(list(range(adapter.num_entities())))
        dense = {u: i for i, u in enumerate(uids)}
        creates: dict[int, list] = defaultdict(list)
        deletes: dict[int, list] = defaultdict(list)
        n_nv = n_ev = 0
        for v in adapter.all_node_versions():
            nm = v.props.get("name")
            row = {"kind": "n", "vid": v.vid, "uid": v.uid, "label": v.label, "vt_s": v.vt_s,
                   "vt_e": v.vt_e, "tt_s": v.tt_s, "props": canonical_json(v.props),
                   "source": v.source, "prov": v.provenance_ref,
                   "name": nm if isinstance(nm, str) else None}
            creates[v.tt_s].append(row)
            if v.tt_e < OPEN_END:
                deletes[v.tt_e].append({"kind": "n", "vid": v.vid})
            n_nv += 1
        for v in adapter.all_edge_versions():
            row = {"kind": "e", "s": dense[v.src], "d": dense[v.dst], "eid": v.eid, "vid": v.vid,
                   "rel": v.rel_type, "disc": v.disc, "vt_s": v.vt_s, "vt_e": v.vt_e,
                   "tt_s": v.tt_s, "props": canonical_json(v.props), "source": v.source,
                   "prov": v.provenance_ref}
            creates[v.tt_s].append(row)
            if v.tt_e < OPEN_END:
                deletes[v.tt_e].append({"kind": "e", "s": dense[v.src], "d": dense[v.dst],
                                        "vid": v.vid})
            n_ev += 1
        t1 = time.perf_counter()
        with drv.session() as s:
            for i in range(0, len(uids), 20_000):
                s.run("UNWIND $rows AS r CREATE (:Entity {uid: r.uid, dense_id: r.id})",
                      rows=[{"uid": u, "id": j} for j, u in enumerate(uids[i:i + 20_000], i)]).consume()
        tts = sorted(set(creates) | set(deletes))
        batch_counts = []
        with drv.session() as s:
            for tt in tts:
                tx = s.begin_transaction()
                dn = [r["vid"] for r in deletes.get(tt, []) if r["kind"] == "n"]
                de = [r for r in deletes.get(tt, []) if r["kind"] == "e"]
                if dn:
                    tx.run("UNWIND $v AS vid MATCH (n:NodeVersion {vid: vid}) SET n.tt_e = $tt",
                           v=dn, tt=tt).consume()
                if de:
                    tx.run("UNWIND $rows AS r MATCH (a:Entity {dense_id: r.s})-[x:E]->"
                           "(b:Entity {dense_id: r.d}) WHERE x.vid = r.vid SET x.tt_e = $tt",
                           rows=de, tt=tt).consume()
                cn = [r for r in creates.get(tt, []) if r["kind"] == "n"]
                ce = [r for r in creates.get(tt, []) if r["kind"] == "e"]
                for j in range(0, len(cn), 10_000):
                    tx.run("UNWIND $rows AS r CREATE (:NodeVersion {vid: r.vid, uid: r.uid, "
                           "label: r.label, vt_s: r.vt_s, vt_e: r.vt_e, tt_s: r.tt_s, tt_e: $open, "
                           "props: r.props, source: r.source, prov: r.prov, name: r.name})",
                           rows=cn[j:j + 10_000], open=OPEN_END).consume()
                for j in range(0, len(ce), 10_000):
                    tx.run("UNWIND $rows AS r MATCH (a:Entity {dense_id: r.s}), "
                           "(b:Entity {dense_id: r.d}) CREATE (a)-[:E {eid: r.eid, vid: r.vid, "
                           "rel_type: r.rel, disc: r.disc, vt_s: r.vt_s, vt_e: r.vt_e, "
                           "tt_s: r.tt_s, tt_e: $open, props: r.props, source: r.source, prov: r.prov}]->(b)",
                           rows=ce[j:j + 10_000], open=OPEN_END).consume()
                # marker as the transaction's last statement: the first AeonG
                # time at which it is visible shows this batch completely
                tx.run("CREATE (:TTMark {tt: $tt})", tt=tt).consume()
                tx.commit()
                batch_counts.append([tt, len(cn), len(ce), len(dn), len(de)])
        t2 = time.perf_counter()
        with drv.session() as s:
            got = {"node_versions_current": s.run(f"MATCH (n:NodeVersion) WHERE n.tt_e = {OPEN_END} "
                                                  f"RETURN count(n)").single()[0],
                   "edge_versions_current": s.run(f"MATCH ()-[r:E]->() WHERE r.tt_e = {OPEN_END} "
                                                  f"RETURN count(r)").single()[0],
                   "entities": s.run("MATCH (n:Entity) RETURN count(n)").single()[0]}
            storage = {r["storage info"]: r["value"] for r in s.run("SHOW STORAGE INFO").data()}
        want_n = sum(1 for v in adapter.all_node_versions() if v.tt_e == OPEN_END)
        want_e = sum(1 for v in adapter.all_edge_versions() if v.tt_e == OPEN_END)
        if (got["node_versions_current"], got["edge_versions_current"]) != (want_n, want_e):
            raise SystemExit(f"aeong load: current sets differ: got {got}, want {want_n}/{want_e}")
        # map the registry's tt_epoch1 (first batch's tt) onto AeonG time
        data = X.dataset(log)
        t3 = time.perf_counter()
        self.history_mismatch = []
        x_epoch1, probes = self._find_tt(drv, data.tt_epoch1, creates, deletes, tts)
        t4 = time.perf_counter()
        self._state_path().write_text(json.dumps({"tt_epoch1": data.tt_epoch1,
                                                  "aeong_tt_epoch1": x_epoch1,
                                                  "probes": probes,
                                                  "history_mismatch": self.history_mismatch}))
        drv.close()
        return {"load_s": round(t4 - t0, 3),
                "phases": {"read_versions_s": round(t1 - t0, 3),
                           "tt_ordered_transactions_s": round(t2 - t1, 3),
                           "count_check_s": round(t3 - t2, 3), "tt_map_search_s": round(t4 - t3, 3)},
                "transactions": len(tts), "versions": {"node": n_nv, "edge": n_ev},
                "current": got, "storage_info": storage, "aeong_tt_epoch1": x_epoch1,
                "history_mismatch": self.history_mismatch}

    def _find_tt(self, drv, tt_epoch1, creates, deletes, tts):
        """The AeonG time that shows the store exactly as believed at Gnomon time
        `tt_epoch1`: the smallest time at which that batch's `TTMark` vertex is
        visible (it is the batch transaction's last statement), checked to show
        no later batch's marker and exactly the epoch-1 NodeVersion set.

        Replaces a bisection on the believed NodeVersion count (Phase A): that
        count is not monotone once versions are closed, and at 1M on the pinned
        engine's store it matched the epoch-1 count at a later AeonG time (50),
        which showed a later version of `n1` (Phase B rotation 1, recorded)."""
        believed = set()
        for tt in tts:
            if tt > tt_epoch1:
                break
            for r in deletes.get(tt, []):
                if r["kind"] == "n":
                    believed.discard(r["vid"])
            for r in creates.get(tt, []):
                if r["kind"] == "n":
                    believed.add(r["vid"])
        probes = []
        with drv.session() as s:
            def marks_at(x):
                r = s.run(f"MATCH (m:TTMark) TT AS {int(x)} "
                          f"RETURN count(m) AS n, max(m.tt) AS mx").single()
                probes.append([int(x), int(r["n"]), r["mx"]])
                return int(r["n"]), r["mx"]

            def seen(x):
                n, mx = marks_at(x)
                return n > 0 and mx is not None and mx >= tt_epoch1
            lo, hi = 0, 1
            while not seen(hi):
                hi *= 2
                if hi > 1 << 62:
                    raise SystemExit("aeong: could not bracket tt_epoch1")
            while lo < hi:
                mid = (lo + hi) // 2
                if seen(mid):
                    hi = mid
                else:
                    lo = mid + 1
            n, mx = marks_at(lo)
            vids = {r["v"] for r in s.run(
                f"MATCH (n:NodeVersion) WHERE n.tt_e = {OPEN_END} TT AS {int(lo)} "
                f"RETURN n.vid AS v").data()}
            if mx != tt_epoch1:
                raise SystemExit(f"aeong: AeonG time {lo} does not show the epoch-1 belief "
                                 f"(latest marker {mx} vs {tt_epoch1})")
            if vids != believed:
                # The marker mapping is right (the epoch-1 batch is the latest
                # visible), but AeonG shows some versions with a property value
                # and at 10M: one NodeVersion created in the epoch-1 batch reads
                # its later tt_e at TT AS x). That is AeonG's history, not the
                # mapping: record it with the load and let the answer checks
                # show what it changes, instead of stopping the window.
                life = {}
                for tt_, rows_ in creates.items():
                    for r_ in rows_:
                        if r_["kind"] == "n":
                            life.setdefault(r_["vid"], {})["tt_s"] = tt_
                for tt_, rows_ in deletes.items():
                    for r_ in rows_:
                        if r_["kind"] == "n":
                            life.setdefault(r_["vid"], {})["tt_e"] = tt_
                for v_ in sorted(vids ^ believed)[:20]:
                    at = s.run(f"MATCH (n:NodeVersion {{vid: $v}}) TT AS {int(lo)} "
                               f"RETURN n.tt_s AS s, n.tt_e AS e", v=v_).data()
                    now = s.run("MATCH (n:NodeVersion {vid: $v}) RETURN n.tt_s AS s, n.tt_e AS e",
                                v=v_).data()
                    self.history_mismatch.append({
                        "vid": v_, "side": "aeong only" if v_ in vids else "believed only",
                        "log": life.get(v_), "at_x": at, "now": now})
                print(f"aeong: AeonG time {lo} shows the epoch-1 marker but "
                      f"{len(vids ^ believed)} NodeVersions differ from the epoch-1 belief "
                      f"(recorded in the load record as history_mismatch)", flush=True)
        return lo, probes

    def open(self):
        import memgraph_queries
        self.module = memgraph_queries
        self.handle = connect()
        st = json.loads(self._state_path().read_text())
        self.x_epoch1 = st["aeong_tt_epoch1"]
        self.tt_epoch1 = st["tt_epoch1"]

    def fn_for(self, q):
        if q.id == "hist.asof":
            if q.args.get("as_of_tt") != self.tt_epoch1:
                return None
            h, x = self.handle, self.x_epoch1
            return lambda: entity_history_tt(h, **{k: v for k, v in q.args.items()
                                                   if k != "as_of_tt"}, _tt=x)
        # The two as-of graph ids default to the property form, AeonG's answer.
        # Its native TT AS forms return wrong answers (defect 1: undirected
        # patterns; defect 2: edges updated after x disappear; reproduction
        # aeong-tt-undirected-repro/) and are never timed: "tt" (two directed
        # matches) and "tt_undirected" exist only for the untimed answer checks
        # that record the defects.
        mode = os.environ.get("EXTCMP_AEONG_ASOF", "property")
        if q.id in ("b.snap.hop2@T", "b.reach@T") and mode in ("tt", "tt_undirected"):
            h = self.handle
            x = aeong_time(h, q.args["as_of_tt"])
            if q.id == "b.snap.hop2@T":
                und = mode == "tt_undirected"
                return lambda: snapshot_tt(h, x, undirected=und, **q.args)
            return lambda: reach_tt(h, x, **q.args)
        if q.id in ("agg.rel_bucket", "agg.rel_bucket.w10"):
            h = self.handle
            return lambda: aggregate_events(h, **q.args)
        if q.id == "resolve.substr":
            h = self.handle
            return lambda: resolve_entities(h, **q.args)
        return super().fn_for(q)

    def settings(self):
        try:
            with self.handle.session() as ss:
                storage = {r["storage info"]: r["value"] for r in ss.run("SHOW STORAGE INFO").data()}
        except Exception as e:                              # noqa: BLE001
            storage = {"error": str(e)[:200]}
        return {"storage_info": storage,
                "storage_note": "in-memory engine; history moves to RocksDB (history_deltas) "
                                "only when GC runs, so data-dir bytes do not measure the store",
                "tt_mapping": {"tt_epoch1": self.tt_epoch1, "aeong_time": self.x_epoch1},
                "native_tt_queries": ["hist.asof"],
                "current_belief": "tt_e = OPEN_END property predicate (Memgraph Cypher)",
                "valid_time": "emulated with vt_s/vt_e properties"}

    def close(self):
        self.handle.close()


def queries_note() -> dict[str, Any]:
    return {"native transaction time": ["hist.asof"],
            "current graph, no time predicate on belief": "all other ids"}


# --------------------------------------------------------------------------- #
# correction-density study: the as-of graph queries on AeonG's own TT axis     #
# --------------------------------------------------------------------------- #

def aeong_time(drv, T: int) -> int:
    """The AeonG time at which the store shows exactly what was believed at
    Gnomon time T: the smallest time at which the marker of the last batch with
    tt <= T is visible (markers are written as each batch transaction's last
    statement)."""
    with drv.session() as s:
        target = s.run("MATCH (m:TTMark) WHERE m.tt <= $T RETURN max(m.tt) AS t", T=T).single()["t"]
        if target is None:
            return 0

        def seen(x):
            return s.run(f"MATCH (m:TTMark) WHERE m.tt = $t TT AS {int(x)} RETURN count(m) AS n",
                         t=target).single()["n"] > 0
        lo, hi = 0, 1
        while not seen(hi):
            hi *= 2
        while lo < hi:
            mid = (lo + hi) // 2
            if seen(mid):
                hi = mid
            else:
                lo = mid + 1
        return lo


def snapshot_tt(drv, x, *, seeds, hops, t_valid, as_of_tt=None, limit=100, undirected=False):
    """`snapshot_subgraph` with every version-reading MATCH evaluated
    `TT AS x` and current belief inside that snapshot (tt_e still open).

    Correctness workaround for an AeonG defect: under `TT AS x` an undirected
    pattern `(a)-[r]-(b)` binds b to a itself for every edge that points into
    a.
    The hop expansion therefore runs the outgoing and the incoming match and
    takes the union; `undirected=True` runs the single undirected match, kept
    only to record the defect."""
    tt = f" TT AS {int(x)}"
    cur = f"r.tt_e = {OPEN_END}"
    with drv.session() as s:
        nrows = s.run(
            f"MATCH (n:NodeVersion) WHERE n.tt_e = {OPEN_END} AND n.vt_s <= $t AND $t < n.vt_e{tt} "
            f"RETURN n.uid AS uid, n.label AS label, n.vt_s AS vs, n.vid AS vid", t=t_valid).data()
        best = {}
        for r in nrows:
            k = (r["vs"], r["vid"])
            if r["uid"] not in best or k > best[r["uid"]][0]:
                best[r["uid"]] = (k, r["label"])
        ids = {r["uid"]: int(r["id"]) for r in s.run(
            "MATCH (e:Entity) WHERE e.uid IN $u RETURN e.uid AS uid, e.dense_id AS id",
            u=sorted(best)).data()}
        valid = {ids[u]: (u, v[1]) for u, v in best.items() if u in ids}
        dist = {i: 0 for i, (u, _) in valid.items() if u in set(seeds)}
        frontier = sorted(dist)
        for h in range(1, hops + 1):
            if not frontier:
                break
            pats = ["(a:Entity)-[r:E]-(b:Entity)"] if undirected else \
                ["(a:Entity)-[r:E]->(b:Entity)", "(a:Entity)<-[r:E]-(b:Entity)"]
            touched = []
            for pat in pats:
                touched += s.run(
                    f"MATCH {pat} WHERE a.dense_id IN $front AND {cur} "
                    f"AND r.vt_s <= $t AND $t < r.vt_e{tt} RETURN DISTINCT b.dense_id AS id",
                    front=frontier, t=t_valid).data()
            new = sorted({int(r["id"]) for r in touched if int(r["id"]) not in dist and int(r["id"]) in valid})
            for i in new:
                dist[i] = h
            frontier = new
        reached = sorted(dist)
        erows = s.run(
            f"MATCH (a:Entity)-[r:E]->(b:Entity) WHERE a.dense_id IN $d AND b.dense_id IN $d "
            f"AND {cur} AND r.vt_s <= $t AND $t < r.vt_e{tt} "
            f"RETURN r.eid AS eid, r.vid AS vid, a.uid AS src, b.uid AS dst, "
            f"r.rel_type AS rel, r.vt_s AS vs, r.vt_e AS ve", d=reached, t=t_valid).data()
    erows.sort(key=lambda r: (r["vs"], r["vid"]))
    rows = [{"eid": r["eid"], "vid": r["vid"], "src": r["src"], "dst": r["dst"],
             "rel_type": r["rel"], "vt_s": r["vs"], "vt_e": r["ve"]} for r in erows[:limit]]
    nodes = sorted(({"uid": valid[i][0], "label": valid[i][1], "hop": hh} for i, hh in dist.items()),
                   key=lambda r: (r["hop"], r["uid"]))
    nt = len(nodes) > limit
    return {"rows": rows, "rows_total": len(erows), "nodes": nodes[:limit], "nodes_total": len(nodes),
            "nodes_truncated": nt, "truncated": len(erows) > len(rows) or nt}


def reach_tt(drv, x, *, src, window, as_of_tt=None, direction="out", delta_max_wait=None, limit=100):
    tt = f" TT AS {int(x)}"
    t_a, t_b = window["t_a"], window["t_b"]
    with drv.session() as s:
        sid = int(s.run("MATCH (e:Entity {uid: $u}) RETURN e.dense_id", u=src).single()[0])
        arr = {sid: t_a}
        frontier = [{"id": sid, "arr": t_a}]
        while frontier:
            relaxed = s.run(
                f"UNWIND $front AS f MATCH (a:Entity {{dense_id: f.id}})-[r:E]->(b:Entity) "
                f"WHERE r.tt_e = {OPEN_END} AND r.vt_e > $ta AND r.vt_s < $tb{tt} "
                f"WITH b, r, CASE WHEN f.arr > r.vt_s THEN f.arr ELSE r.vt_s END AS tau "
                f"WHERE tau < r.vt_e AND tau < $tb RETURN b.dense_id AS id, min(tau) AS arr",
                front=frontier, ta=t_a, tb=t_b).data()
            frontier = []
            for r in relaxed:
                i, a_ = int(r["id"]), int(r["arr"])
                if i not in arr or a_ < arr[i]:
                    arr[i] = a_
                    frontier.append({"id": i, "arr": a_})
        ids = sorted(i for i in arr if i != sid)
        uids = {int(r["id"]): r["uid"] for r in s.run(
            "MATCH (e:Entity) WHERE e.dense_id IN $ids RETURN e.dense_id AS id, e.uid AS uid",
            ids=ids).data()} if ids else {}
    rows = sorted(({"uid": uids[i], "earliest_arrival": arr[i]} for i in ids),
                  key=lambda r: (r["earliest_arrival"], r["uid"]))
    out = rows[:limit]
    return {"rows": out, **_page(len(rows), len(out))}
