"""Belief-time graph queries for the correction-density study.

The shared registry asks one belief-time question (`hist.asof`, a point
read). The study needs the belief axis composed with adjacency and with
history, each next to a current-belief control of the same shape:

| id | question | control |
|---|---|---|
| `b.snap.hop2@T` | 2-hop neighbourhood of n1..n20 at instant t*, as believed at T (whole answer, no page) | `b.snap.hop2@now` |
| `b.reach@T` | time-respecting reachability from n1 in [t* - L, t*), as believed at T | `b.reach@now` |
| `b.diff` | every version whose belief state changed in (T_base, now] inside [t* - L/2, t* + L/2) | `b.diff@ctl` (valid-time diff of the same window, current belief) |
| `b.hist` | the full belief history of one edge e*: every version ever written, both clocks | `b.hist@now` (e*'s versions believed now) |

**Parameters, and how the as-of time is chosen so the answer moves with
density.** Everything derives from the frozen event log of one density point
(`derive_params`, cached beside the log):

* `t*` = t0 + 3/4 of the extent; `L` = the synthetic generator's edge lifetime.
* The injector (`scripts/inject_corrections.py`) walks the store in
  valid-time order and commits corrections in batches, so its batches are
  ordered by the valid time of what they correct. `T` is the transaction time
  of the **first injected batch whose corrected edges reach valid time
  t* - L/2**. As believed at `T`, the selected edges that started before
  t* - L/2 and are still valid at t* carry their corrected versions; those
  that started after it do not yet. The as-of answer therefore differs from
  the uncorrected answer (density 0), from the current answer, and between 1 %
  and 10 % (the selected sets are nested: `_selected` is a threshold on one
  hash, so every edge selected at 1 % is selected at 10 %).
* `T_base` is the last batch before the first injected one (the end of the
  base build); `b.diff` covers `(T_base, now]`, so its row count is the number
  of corrected versions in the window: zero at density 0, growing with it.
* `e*` is the first base edge with vt_s in [t* - L/2, t*) that no base
  correction or retraction touched and that the injector selects at 1 %
  (seed 0): one version at density 0, a closed original plus its correction
  at 1 % and at 10 %.
* A density point's frozen log is the base log followed by the injector's
  batches (the injector writes through the store's write-ahead log; a store
  built by replay holds only those batches in its own log).
* At density 0 there are no injected batches: `T = T_base` = the last batch,
  and every as-of id equals its control by construction.

**Reachability is a cost probe, not an answer probe.** The injector rewrites
props over the same interval, so the topology — all that reachability reads —
is identical at every density. `b.reach@T` answers the same at 0, 1 and 10 %;
what changes is how many versions an emulation must filter per hop. That is
stated in the study memo rather than disguised.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import eval_harness as H

from gnomon.core.model import OPEN_END, clamp_tt, edge_eid

BELIEF_IDS = ("b.snap.hop2@T", "b.snap.hop2@now", "b.reach@T", "b.reach@now",
              "b.diff", "b.diff@ctl", "b.hist", "b.hist@now")
#: ids whose answer is produced by an existing registry function with as_of args
ALIAS = {"b.snap.hop2@T": "snap.hop2", "b.snap.hop2@now": "snap.hop2",
         "b.reach@T": "reach.window", "b.reach@now": "reach.window",
         "b.diff@ctl": "diff.global"}
SEED = 0


def selected(eid: str, seed: int, density_pct: float) -> bool:
    """`inject_corrections._selected`, verbatim (kept in step by the check)."""
    if density_pct <= 0:
        return False
    if density_pct >= 100:
        return True
    h = hashlib.sha256(f"{eid}:{seed}".encode()).digest()
    return int.from_bytes(h[:8], "big") / 2.0 ** 64 < density_pct / 100.0


def _ref_eid(ref: dict[str, Any]) -> str | None:
    if ref.get("kind") != "edge":
        return None
    return edge_eid(ref["src"], ref["dst"], ref["rel_type"], ref.get("disc", ""))


def derive_params(log: Path, scale: int, t0: int, t1: int) -> dict[str, Any]:
    side = log.with_suffix(".beliefq-params.json")
    st = log.stat()
    if side.exists():
        d = json.loads(side.read_text())
        if d.get("size") == st.st_size and d.get("mtime_ns") == st.st_mtime_ns:
            return d
    span = t1 - t0
    t_star = t0 + 3 * span // 4
    L = H.edge_life(scale)
    batches = []                       # (tt, injected?, max corrected vt_s)
    touched: set[str] = set()          # eids referenced by base correct/retract
    candidates: list[tuple[str, dict]] = []
    with open(log) as f:
        for line in f:
            rec = json.loads(line)
            if "tt" not in rec:
                continue
            injected, max_vt = False, None
            for op in rec["ops"]:
                kind = op["op"]
                if kind == "ingest_events":
                    off = op.get("offset", 0)
                    for i, ev in enumerate(op["events"]):
                        vs = ev["vt_s"]
                        if t_star - L // 2 <= vs < t_star and len(candidates) < 50_000:
                            disc = ev.get("disc", f"#{off + i}")
                            e = edge_eid(ev["src"], ev["dst"], ev["rel_type"], disc)
                            candidates.append((e, {"eid": e, "src": ev["src"], "dst": ev["dst"],
                                                   "rel_type": ev["rel_type"], "disc": disc,
                                                   "vt_s": vs, "vt_e": ev.get("vt_e") or vs + 1}))
                elif kind in ("correct", "retract"):
                    if "w5_correction_seed" in (op.get("props") or {}):
                        injected = True
                        max_vt = max(max_vt if max_vt is not None else op["vt_s"], op["vt_s"])
                    else:
                        e = _ref_eid(op["ref"])
                        if e:
                            touched.add(e)
            batches.append((rec["tt"], injected, max_vt))
    first_inj = next((i for i, b in enumerate(batches) if b[1]), None)
    if first_inj is None:
        T_base = batches[-1][0]
        T = T_base
    else:
        T_base = batches[first_inj - 1][0]
        T = next((b[0] for b in batches[first_inj:] if b[1] and b[2] is not None
                  and b[2] >= t_star - L // 2), batches[-1][0])
    e_star = next((c for e, c in candidates if e not in touched and selected(e, SEED, 1.0)), None)
    if e_star is None:
        raise SystemExit("beliefq: no edge in [t* - L/2, t*) is selected at 1 %")
    d = {"size": st.st_size, "mtime_ns": st.st_mtime_ns, "t_star": t_star, "L": L,
         "T": T, "T_base": T_base, "now": batches[-1][0],
         "injected_batches": sum(1 for b in batches if b[1]),
         "injected_corrections_note": "count in the injector's receipt",
         "seed_node": "n1", "snap_seeds": [f"n{i}" for i in range(1, 21)], "e_star": e_star,
         "reach_window": {"t_a": t_star - L, "t_b": t_star},
         "diff_window": {"t_a": t_star - L // 2, "t_b": t_star + L // 2}}
    side.write_text(json.dumps(d, indent=1))
    return d


def belief_queries(data: H.Dataset, p: dict[str, Any]) -> list[H.Query]:
    # 20 seeds and a page covering the whole answer: with one seed a 2-hop
    # neighbourhood holds ~10^2 edge versions, so at 1 % the as-of answer
    # would often contain no corrected edge at all, and the default page of
    # 100 rows would hide the ones it did contain
    snap = {"seeds": p["snap_seeds"], "hops": 2, "t_valid": p["t_star"], "limit": 10_000}
    reach = {"src": p["seed_node"], "window": p["reach_window"]}
    e = p["e_star"]
    ent = {k: e[k] for k in ("eid", "src", "dst", "rel_type", "disc")}
    return [
        H.Query("b.snap.hop2@T", "snapshot_subgraph", {**snap, "as_of_tt": p["T"]},
                "2-hop neighbourhood at t*, as believed at T", requires=("bitemporal",)),
        H.Query("b.snap.hop2@now", "snapshot_subgraph", dict(snap), "control: current belief"),
        H.Query("b.reach@T", "temporal_reachability", {**reach, "as_of_tt": p["T"]},
                "time-respecting reachability, as believed at T", requires=("bitemporal",)),
        H.Query("b.reach@now", "temporal_reachability", dict(reach), "control: current belief"),
        H.Query("b.diff", "diff_belief", {"t1": p["T_base"], "t2": OPEN_END,
                                          "window": p["diff_window"], "limit": 1000},
                "belief diff (T_base, now] over a window", requires=("bitemporal",)),
        H.Query("b.diff@ctl", "diff_snapshots", {"t1": p["diff_window"]["t_a"],
                                                 "t2": p["diff_window"]["t_b"]},
                "control: valid-time diff of the same window, current belief"),
        H.Query("b.hist", "entity_full_history", ent,
                "every version of e* ever written, both clocks", requires=("bitemporal",)),
        H.Query("b.hist@now", "entity_believed", ent, "control: e*'s versions believed now"),
    ]


# --------------------------------------------------------------------------- #
# reference semantics over version rows (the oracle every system is held to) #
# --------------------------------------------------------------------------- #

def _hist_row(v) -> dict[str, Any]:
    return {"vid": v.vid, "vt_s": int(v.vt_s), "vt_e": int(v.vt_e),
            "tt_s": int(v.tt_s), "tt_e": int(v.tt_e)}


def shape_hist(rows: list[dict[str, Any]]) -> dict[str, Any]:
    rows = sorted(rows, key=lambda r: (r["tt_s"], r["vid"]))
    return {"rows": rows, "rows_total": len(rows)}


def shape_believed(rows: list[dict[str, Any]]) -> dict[str, Any]:
    rows = sorted(({k: r[k] for k in ("vid", "vt_s", "vt_e", "tt_s")} for r in rows),
                  key=lambda r: (r["vt_s"], r["vid"]))
    return {"rows": rows, "rows_total": len(rows)}


def shape_diff(cands: list[tuple], t1: int, t2: int, limit: int) -> dict[str, Any]:
    """cands: (kind 'node'|'edge', key, vid, vt_s, vt_e, tt_s, tt_e)."""
    a, b = clamp_tt(t1), clamp_tt(t2)
    rows = []
    for kind, key, vid, vs, ve, ts, te in cands:
        if a < ts <= b:
            rows.append({"kind": kind, "key": key, "vid": vid, "tt": int(ts), "change": "added",
                         "vt_s": int(vs), "vt_e": int(ve)})
        if te < OPEN_END and a < te <= b:
            rows.append({"kind": kind, "key": key, "vid": vid, "tt": int(te), "change": "retracted",
                         "vt_s": int(vs), "vt_e": int(ve)})
    rows.sort(key=lambda r: (0 if r["kind"] == "node" else 1, r["key"], r["tt"], r["change"], r["vid"]))
    added = sum(1 for r in rows if r["change"] == "added")
    return {"rows": rows[:limit], "added_total": added, "retracted_total": len(rows) - added,
            "truncated": len(rows) > limit}


def reference(q: H.Query, nodes: list, edges: list) -> dict[str, Any] | None:
    a = q.args
    if q.op == "entity_full_history":
        return shape_hist([_hist_row(v) for v in edges if v.eid == a["eid"]])
    if q.op == "entity_believed":
        return shape_believed([_hist_row(v) for v in edges if v.eid == a["eid"] and v.tt_e == OPEN_END])
    if q.op == "diff_belief":
        w = a["window"]
        c = [("node", v.uid, v.vid, v.vt_s, v.vt_e, v.tt_s, v.tt_e) for v in nodes
             if v.vt_s < w["t_b"] and v.vt_e > w["t_a"]]
        c += [("edge", v.eid, v.vid, v.vt_s, v.vt_e, v.tt_s, v.tt_e) for v in edges
              if v.vt_s < w["t_b"] and v.vt_e > w["t_a"]]
        return shape_diff(c, a["t1"], a["t2"], a["limit"])
    return None


# --------------------------------------------------------------------------- #
# engine (native and the DuckDB backend): operator calls and compositions      #
# --------------------------------------------------------------------------- #

def full_history_walk(adapter, eid: str) -> dict[str, Any]:
    """Composition: the engine has no per-identity full-history call, only
    `believed(identity, as_of)` through its identity postings. Walk back from
    current belief: at each step ask what was believed just before the latest
    write seen so far. Visits each distinct write time of the identity once.
    Complete for an identity whose every closed version has a successor (a
    correction); a version ended by a bare retraction with an older open
    sibling would be skipped — e* is chosen without base retractions, and the
    check compares this against the version-table reference."""
    seen: dict[str, dict[str, Any]] = {}
    t = OPEN_END
    while True:
        vs = adapter.believed_edge_versions(eid, t)
        if not vs:
            break
        for v in vs:
            seen.setdefault(v.vid, _hist_row(v))
        t = max(int(v.tt_s) for v in vs) - 1
        if t < 0:
            break
    return shape_hist(list(seen.values()))


def version_history_log(adapter, eid: str) -> dict[str, Any]:
    """b.hist through the engine's `version_history` with a `ref` (an
    engine-level argument): every stored version of the identity, read through
    its postings, so a belief gap -- a removal, then a rollback -- does not
    hide the versions before it, as it does from `full_history_walk`."""
    from gnomon.temporal.algebra import call_operator

    rows, cursor = [], None
    while True:
        env = call_operator(adapter, "version_history", {
            "kind": "edge", "ref": {"eid": eid}, "belief": "all",
            "window": {"t_a": 0, "t_b": OPEN_END}, "limit": 10_000, "cursor": cursor})
        rows += [{k: r[k] for k in ("vid", "vt_s", "vt_e", "tt_s", "tt_e")} for r in env["rows"]]
        cursor = env["cursor"]
        if cursor is None:
            return shape_hist(rows)


def _has_identity_log() -> bool:
    """Does this engine take `version_history(ref=...)`? Engines before it
    keep the walk, which is complete only without belief gaps."""
    from gnomon.temporal.algebra import REGISTRY, ensure_all_registered

    ensure_all_registered()
    spec = REGISTRY.get("version_history")
    schema = getattr(spec, "engine_schema", None) or {}
    return "ref" in schema.get("properties", {})


def engine_fn(adapter, q: H.Query):
    if q.op == "entity_full_history":
        if _has_identity_log():
            return lambda: version_history_log(adapter, q.args["eid"])
        return lambda: full_history_walk(adapter, q.args["eid"])
    if q.op == "entity_believed":
        return lambda: shape_believed([_hist_row(v) for v in adapter.believed_edge_versions(q.args["eid"])])
    return None


# --------------------------------------------------------------------------- #
# four-timestamp emulations                                                     #
# --------------------------------------------------------------------------- #

def pg_diff(conn, *, t1, t2, window, limit):
    a, b = clamp_tt(t1), clamp_tt(t2)
    p = {"t1": a, "t2": b, "ta": window["t_a"], "tb": window["t_b"], "lim": limit}
    sel = ("vt_s < %(tb)s AND vt_e > %(ta)s AND ((tt_s > %(t1)s AND tt_s <= %(t2)s) "
           "OR (tt_e > %(t1)s AND tt_e <= %(t2)s))")
    c = (f"WITH v AS (SELECT 0 AS k, uid AS key, vid, vt_s, vt_e, tt_s, tt_e FROM node_versions WHERE {sel} "
         f"UNION ALL SELECT 1, eid, vid, vt_s, vt_e, tt_s, tt_e FROM edge_versions WHERE {sel}), "
         f"c AS (SELECT k, key, vid, tt_s AS tt, 'added' AS change, vt_s, vt_e FROM v "
         f"WHERE tt_s > %(t1)s AND tt_s <= %(t2)s UNION ALL "
         f"SELECT k, key, vid, tt_e, 'retracted', vt_s, vt_e FROM v WHERE tt_e > %(t1)s AND tt_e <= %(t2)s) ")
    tot = conn.execute(c + "SELECT count(*) FILTER (WHERE change = 'added'), "
                           "count(*) FILTER (WHERE change = 'retracted') FROM c", p).fetchone()
    rows = conn.execute(c + 'SELECT k, key, vid, tt, change, vt_s, vt_e FROM c ORDER BY k, '
                            'key COLLATE "C", tt, change COLLATE "C", vid COLLATE "C" LIMIT %(lim)s',
                        p).fetchall()
    out = [{"kind": "node" if r[0] == 0 else "edge", "key": r[1], "vid": r[2], "tt": int(r[3]),
            "change": r[4], "vt_s": int(r[5]), "vt_e": int(r[6])} for r in rows]
    return {"rows": out, "added_total": int(tot[0]), "retracted_total": int(tot[1]),
            "truncated": int(tot[0]) + int(tot[1]) > len(out)}


def pg_hist(conn, *, eid, **_):
    rows = conn.execute('SELECT vid, vt_s, vt_e, tt_s, tt_e FROM edge_versions WHERE eid = %s',
                        (eid,)).fetchall()
    return shape_hist([{"vid": r[0], "vt_s": r[1], "vt_e": r[2], "tt_s": r[3], "tt_e": r[4]} for r in rows])


def pg_believed(conn, *, eid, **_):
    rows = conn.execute(f'SELECT vid, vt_s, vt_e, tt_s, tt_e FROM edge_versions WHERE eid = %s '
                        f'AND tt_e = {OPEN_END}', (eid,)).fetchall()
    return shape_believed([{"vid": r[0], "vt_s": r[1], "vt_e": r[2], "tt_s": r[3], "tt_e": r[4]}
                           for r in rows])


PG_INDEXES = ("CREATE INDEX IF NOT EXISTS ev_tte ON edge_versions (tt_e, tt_s)",
              "CREATE INDEX IF NOT EXISTS nv_tte ON node_versions (tt_e, tt_s)")


def ch_diff(client, *, t1, t2, window, limit):
    a, b = clamp_tt(t1), clamp_tt(t2)
    sel = (f"vt_s < {int(window['t_b'])} AND vt_e > {int(window['t_a'])} AND "
           f"((tt_s > {a} AND tt_s <= {b}) OR (tt_e > {a} AND tt_e <= {b}))")
    c = (f"SELECT * FROM (SELECT k, key, vid, tt_s AS tt, 'added' AS change, vt_s, vt_e FROM v "
         f"WHERE tt_s > {a} AND tt_s <= {b} UNION ALL "
         f"SELECT k, key, vid, tt_e AS tt, 'retracted' AS change, vt_s, vt_e FROM v "
         f"WHERE tt_e > {a} AND tt_e <= {b})")
    v = (f"WITH v AS (SELECT 0 AS k, uid AS key, vid, vt_s, vt_e, tt_s, tt_e FROM gnomon_eval.node_versions "
         f"WHERE {sel} UNION ALL SELECT 1 AS k, eid AS key, vid, vt_s, vt_e, tt_s, tt_e "
         f"FROM gnomon_eval.edge_versions WHERE {sel}) ")
    tot = client.query(v + f"SELECT countIf(change = 'added'), countIf(change = 'retracted') FROM ({c})").result_rows[0]
    rows = client.query(v + f"{c} ORDER BY k, key, tt, change, vid LIMIT {int(limit)}").result_rows
    out = [{"kind": "node" if r[0] == 0 else "edge", "key": r[1], "vid": r[2], "tt": int(r[3]),
            "change": r[4], "vt_s": int(r[5]), "vt_e": int(r[6])} for r in rows]
    return {"rows": out, "added_total": int(tot[0]), "retracted_total": int(tot[1]),
            "truncated": int(tot[0]) + int(tot[1]) > len(out)}


def ch_hist(client, *, eid, **_):
    rows = client.query("SELECT vid, vt_s, vt_e, tt_s, tt_e FROM gnomon_eval.edge_versions WHERE eid = %(e)s",
                        parameters={"e": eid}).result_rows
    return shape_hist([{"vid": r[0], "vt_s": r[1], "vt_e": r[2], "tt_s": r[3], "tt_e": r[4]} for r in rows])


def ch_believed(client, *, eid, **_):
    rows = client.query(f"SELECT vid, vt_s, vt_e, tt_s, tt_e FROM gnomon_eval.edge_versions "
                        f"WHERE eid = %(e)s AND tt_e = {OPEN_END}", parameters={"e": eid}).result_rows
    return shape_believed([{"vid": r[0], "vt_s": r[1], "vt_e": r[2], "tt_s": r[3], "tt_e": r[4]}
                           for r in rows])


# Cypher (Neo4j, Memgraph, and AeonG's property emulation share it; LadybugDB
# runs the same text through its own connection)

def _cy_rows(run, q, **params):
    return run(q, params)


def cypher_diff(run, *, t1, t2, window, limit, rel="()-[r:E]->()"):
    a, b = clamp_tt(t1), clamp_tt(t2)
    p = {"ta": window["t_a"], "tb": window["t_b"], "a": a, "b": b}
    pred = ("{x}.vt_s < $tb AND {x}.vt_e > $ta AND "
            "(({x}.tt_s > $a AND {x}.tt_s <= $b) OR ({x}.tt_e > $a AND {x}.tt_e <= $b))")
    cands = [("node", r["k"], r["vid"], r["vs"], r["ve"], r["ts"], r["te"]) for r in run(
        "MATCH (n:NodeVersion) WHERE " + pred.format(x="n") +
        " RETURN n.uid AS k, n.vid AS vid, n.vt_s AS vs, n.vt_e AS ve, n.tt_s AS ts, n.tt_e AS te", p)]
    cands += [("edge", r["k"], r["vid"], r["vs"], r["ve"], r["ts"], r["te"]) for r in run(
        f"MATCH {rel} WHERE " + pred.format(x="r") +
        " RETURN r.eid AS k, r.vid AS vid, r.vt_s AS vs, r.vt_e AS ve, r.tt_s AS ts, r.tt_e AS te", p)]
    return shape_diff(cands, t1, t2, limit)


def cypher_hist(run, *, eid, rel="()-[r:E]->()", **_):
    return shape_hist([{"vid": r["vid"], "vt_s": r["vs"], "vt_e": r["ve"], "tt_s": r["ts"],
                        "tt_e": r["te"]} for r in run(
        f"MATCH {rel} WHERE r.eid = $eid RETURN r.vid AS vid, r.vt_s AS vs, r.vt_e AS ve, "
        f"r.tt_s AS ts, r.tt_e AS te", {"eid": eid})])


def cypher_believed(run, *, eid, rel="()-[r:E]->()", **_):
    return shape_believed([{"vid": r["vid"], "vt_s": r["vs"], "vt_e": r["ve"], "tt_s": r["ts"],
                            "tt_e": r["te"]} for r in run(
        f"MATCH {rel} WHERE r.eid = $eid AND r.tt_e = {OPEN_END} RETURN r.vid AS vid, "
        f"r.vt_s AS vs, r.vt_e AS ve, r.tt_s AS ts, r.tt_e AS te", {"eid": eid})])


def bolt_runner(drv):
    def run(q, p):
        with drv.session() as s:
            return s.run(q, **p).data()
    return run


NEO4J_INDEXES = ("CREATE INDEX e_eid IF NOT EXISTS FOR ()-[r:E]-() ON (r.eid)",
                 "CREATE INDEX e_tts IF NOT EXISTS FOR ()-[r:E]-() ON (r.tt_s)")
MEMGRAPH_INDEXES = ("CREATE EDGE INDEX ON :E(eid)", "CREATE EDGE INDEX ON :E(tt_s)")


# XTDB: both clocks native; rows are XTDB's own bitemporal fragments, so the
# version id is recomputed from (_id, _system_from, _valid_from) and agreement
# is judged on the projected answer (ids and belief stamps removed).

def xtdb_diff(cur, *, t1, t2, window, limit):
    import extcmp_xtdb as XT
    a, b = clamp_tt(t1), clamp_tt(t2)
    vf, vt = XT.us("_valid_from"), f"COALESCE({XT.us('_valid_to')}, {OPEN_END})"
    sf, st = XT.us("_system_from"), f"COALESCE({XT.us('_system_to')}, {OPEN_END})"
    cands = []
    for table, kind in (("nodes", "node"), ("edges", "edge")):
        for _id, vs, ve, ts, te in XT._q(
                cur, f"SELECT _id, {vf}, {vt}, {sf}, {st} FROM {table} "
                     f"FOR ALL SYSTEM_TIME FOR ALL VALID_TIME "
                     f"WHERE {vf} < {int(window['t_b'])} AND {vt} > {int(window['t_a'])} "
                     f"AND (({sf} > {a} AND {sf} <= {b}) OR ({st} > {a} AND {st} <= {b}))"):
            cands.append((kind, _id, XT.vid(_id, int(ts), int(vs)), vs, ve, ts, te))
    return shape_diff(cands, t1, t2, limit)


def xtdb_hist(cur, *, eid, **_):
    import extcmp_xtdb as XT
    rows = XT._q(cur, f"SELECT {XT.us('_valid_from')}, COALESCE({XT.us('_valid_to')}, {OPEN_END}), "
                      f"{XT.us('_system_from')}, COALESCE({XT.us('_system_to')}, {OPEN_END}) "
                      f"FROM edges FOR ALL SYSTEM_TIME FOR ALL VALID_TIME WHERE _id = %s", (eid,))
    return shape_hist([{"vid": XT.vid(eid, int(ts), int(vs)), "vt_s": int(vs), "vt_e": int(ve),
                        "tt_s": int(ts), "tt_e": int(te)} for vs, ve, ts, te in rows])


def xtdb_believed(cur, *, eid, **_):
    import extcmp_xtdb as XT
    rows = XT._q(cur, f"SELECT {XT.us('_valid_from')}, COALESCE({XT.us('_valid_to')}, {OPEN_END}), "
                      f"{XT.us('_system_from')} FROM edges FOR ALL VALID_TIME WHERE _id = %s", (eid,))
    return shape_believed([{"vid": XT.vid(eid, int(ts), int(vs)), "vt_s": int(vs), "vt_e": int(ve),
                            "tt_s": int(ts)} for vs, ve, ts in rows])


def ladybug_runner(conn):
    import extcmp_ladybug as LB
    return lambda q, p: LB._rows(conn, q, p)


def system_fn(system: str, handle, q: H.Query):
    """The b.* ids that are not plain aliases of registry functions.

    Cypher systems share one text: Neo4j and Memgraph over bolt, AeonG over
    bolt as a property emulation (its tt_s/tt_e properties: tt_e is updated
    when a version is closed), LadybugDB through its own connection."""
    a = dict(q.args)
    run = None
    if system in ("neo4j", "memgraph", "aeong"):
        run = bolt_runner(handle)
    elif system == "ladybug":
        run = ladybug_runner(handle)
    if q.id == "b.diff":
        a.pop("limit", None)
        lim = q.args["limit"]
        if system == "postgres":
            return lambda: pg_diff(handle, limit=lim, **a)
        if system == "clickhouse":
            return lambda: ch_diff(handle, limit=lim, **a)
        if run is not None:
            return lambda: cypher_diff(run, limit=lim, **a)
        if system == "xtdb":
            return lambda: xtdb_diff(handle, limit=lim, **a)
    if q.id in ("b.hist", "b.hist@now"):
        fn = {"postgres": (pg_hist, pg_believed), "clickhouse": (ch_hist, ch_believed),
              "xtdb": (xtdb_hist, xtdb_believed)}.get(system)
        if fn:
            f = fn[0] if q.id == "b.hist" else fn[1]
            return lambda: f(handle, **a)
        if run is not None:
            f = cypher_hist if q.id == "b.hist" else cypher_believed
            return lambda: f(run, **a)
    return None
