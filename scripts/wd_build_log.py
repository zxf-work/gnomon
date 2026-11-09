"""W5 stage B: stage-A change records -> one frozen bi-temporal Gnomon event log + statistics.

    wd_build_log.py --name smallpedia --stagea DIR --subset NODES.q --tgb EDGELIST.csv --out DIR

Transaction time (tt) is the revision timestamp in epoch microseconds; revisions
committed in the same second are ordered by revision id and spaced 1 us apart.
One batch per revision that changes at least one temporal edge statement.
Valid time is microseconds since 0000-01-01 (see wd_extract.py).

Mapping of one statement (GUID g on subject q) whose reduced value goes b -> a:
    b absent            assert_edge(a)                                  'add'
    a absent            retract(ref(b), t = b.vs)   (whole interval)    'remove'
    same edge, b open, a closed at the same start
                        retract(ref(b), t = a.ve)                       'close'
    otherwise           retract(ref(b), b.vs) + assert_edge(a)          'edge' (object changed) / 'date'
Edge identity: src = q, dst = object, rel_type = property, disc = "" for the
first GUID of (q, p, o), "#k" for the k-th further one.

A change is a *correction* when its revision is a revert (rollback, undo,
restore, manual identity revert; wd_extract.classify). Its lag is the revert's
tt minus the tt of the statement's previous change. Every op carries
provenance_ref "wd:<revid>[:<revert kind>]".
"""
from __future__ import annotations

import argparse
import collections
import hashlib
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from gnomon.core.model import canonical_json, sha256_hex  # noqa: E402

OPEN = 2 ** 62
HEADER = '{"format":"gnomon-events","version":1}\n'
BOT = re.compile(r"bot\b|bot$|^QuickStatementsBot|^KrBot|^Reinheitsgebot", re.I)
YEAR_US = 365.2425 * 86_400_000_000


def ts_us(ts: str) -> int:
    return int(datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).timestamp()) * 1_000_000


def pct(xs, ps=(0.5, 0.9, 0.99)):
    if not xs:
        return {}
    xs = sorted(xs)
    return {f"p{int(p*100)}": xs[min(len(xs) - 1, int(p * len(xs)))] for p in ps} | {"n": len(xs), "max": xs[-1]}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--name", required=True)
    ap.add_argument("--stagea", required=True)
    ap.add_argument("--subset", required=True)
    ap.add_argument("--tgb", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--threads", type=int, default=16)
    a = ap.parse_args()
    t0 = time.time()
    out = Path(a.out); out.mkdir(parents=True, exist_ok=True)
    db = out / f"W5-{a.name}.stage.duckdb"
    if db.exists():
        db.unlink()
    con = duckdb.connect(str(db))
    con.execute(f"SET threads={a.threads}")
    cols = ("{'t':'VARCHAR','q':'BIGINT','id':'BIGINT','p':'BIGINT','ts':'VARCHAR','u':'VARCHAR','c':'VARCHAR',"
            "'sha1':'VARCHAR','rv':'VARCHAR','rv_to':'BIGINT','n':'INTEGER','text':'VARCHAR','redirect':'VARCHAR',"
            "'g':'VARCHAR','b':'JSON','a':'JSON','lag_from':'VARCHAR'}")
    con.execute(f"CREATE TABLE raw AS SELECT * FROM read_ndjson('{a.stagea}/*.jsonl.gz', columns={cols})")
    con.execute("CREATE TABLE rev AS SELECT q,id,p,ts,u,c,rv,rv_to,n,text,redirect FROM raw WHERE t='rev'")
    con.execute("CREATE TABLE chg AS SELECT q,id,ts,g,b,a,lag_from FROM raw WHERE t='chg'")
    con.execute("DROP TABLE raw")
    con.execute("""CREATE TABLE revtt AS SELECT id, ts,
        (epoch(strptime(ts, '%Y-%m-%dT%H:%M:%SZ'))::BIGINT * 1000000
         + dense_rank() OVER (PARTITION BY ts ORDER BY id) - 1) AS tt
        FROM (SELECT DISTINCT id, ts FROM chg)""")
    st: dict = {"name": a.name}
    st["revisions"] = con.execute("""SELECT count(*), count(DISTINCT q), sum((rv IS NOT NULL)::INT),
        sum((text IS NOT NULL)::INT), min(ts), max(ts) FROM rev""").fetchone()
    st["revisions"] = dict(zip(["n", "pages", "reverts", "text_missing_or_bad", "ts_min", "ts_max"], st["revisions"]))
    # revision-level revert statistics, human vs bot by user-name heuristic
    # user class in SQL (a Python pass over every revision does not scale to the wikidata range);
    # same rule as BOT: anonymous = ip:, bot = name matching bot\b|bot$ (case-insensitive) or a named bot
    hb = collections.Counter({(c, k): n for c, k, n in con.execute(r"""
        SELECT CASE WHEN u LIKE 'ip:%' THEN 'anon'
                    WHEN regexp_matches(coalesce(u,''), '(?i)(bot\b|bot$|^QuickStatementsBot|^KrBot|^Reinheitsgebot)') THEN 'bot'
                    ELSE 'user' END AS cls, coalesce(rv, '-') AS k, count(*) FROM rev GROUP BY ALL""").fetchall()})
    st["revisions_by_class_and_kind"] = {f"{c}|{k}": v for (c, k), v in sorted(hb.items())}
    human = sum(v for (c, k), v in hb.items() if c != "bot")
    human_rv = sum(v for (c, k), v in hb.items() if c != "bot" and k != "-")
    st["revert_rate_all"] = st["revisions"]["reverts"] / max(1, st["revisions"]["n"])
    st["revert_rate_human"] = human_rv / max(1, human)
    # revision-level lag: revert ts - ts of the first reverted revision
    lag_rows = con.execute(r"""
        WITH r AS (SELECT q, id, ts, rv, rv_to,
                     TRY_CAST(regexp_extract(c, '/\* undo:\d+\|\|(\d+)\|', 1) AS BIGINT) AS undone FROM rev WHERE rv IS NOT NULL),
             first_rev AS (SELECT r.id AS rid, min(x.id) AS fid FROM r JOIN rev x ON x.q=r.q AND x.p=r.rv_to
                           WHERE r.rv_to IS NOT NULL AND x.id<r.id GROUP BY r.id)
        SELECT r.rv, epoch(strptime(r.ts,'%Y-%m-%dT%H:%M:%SZ')) - epoch(strptime(y.ts,'%Y-%m-%dT%H:%M:%SZ'))
        FROM r LEFT JOIN first_rev f ON f.rid=r.id
        JOIN rev y ON y.id = coalesce(r.undone, f.fid)""").fetchall()
    by = collections.defaultdict(list)
    for k, s in lag_rows:
        by[k].append(int(s)); by["all"].append(int(s))
    st["revert_lag_s_revision_level"] = {k: pct(v) for k, v in by.items()}

    # ---------------------------------------------------------------- the log
    subset = sorted(int(x) for x in open(a.subset) if x.strip())
    logp = out / f"W5-{a.name}.eventlog.jsonl"
    truthp = out / f"W5-{a.name}.corrections.jsonl"
    f = open(str(logp) + ".tmp", "w", encoding="utf-8"); f.write(HEADER)
    tf = open(str(truthp) + ".tmp", "w", encoding="utf-8")
    nrec = 0
    def rec(tt, ops):
        nonlocal nrec
        bid = sha256_hex(canonical_json({"tt": tt, "ops": ops}))[:16]
        f.write(canonical_json({"batch_id": bid, "tt": tt, "ops": ops}) + "\n"); nrec += 1
    tt_first = con.execute("SELECT min(tt) FROM revtt").fetchone()[0]
    CH = 50_000
    nb = (len(subset) + CH - 1) // CH
    for i in range(nb):
        nodes = [{"uid": str(q), "label": "Item", "props": {}, "vt_s": 0, "vt_e": OPEN} for q in subset[i*CH:(i+1)*CH]]
        rec(tt_first - nb + i, [{"op": "ingest_events", "events": [], "node_label": "Item", "source": "ingest",
                                  "provenance_ref": None, "nodes": nodes}])
    rvkind = dict(con.execute("SELECT id, rv FROM rev WHERE rv IS NOT NULL").fetchall())
    cur = con.execute("""SELECT t.tt, c.id, c.q, c.g, c.b, c.a, c.lag_from FROM chg c JOIN revtt t USING (id)
                         ORDER BY t.tt, c.q, c.g""")
    disc_of: dict = {}; nd = collections.Counter(); state: dict = {}
    cnt = collections.Counter(); lags = collections.defaultdict(list); inconsistent = 0
    edges_ever = set(); touched_by_corr = set()
    def ident(q, g, v):
        key = (q, v[0], v[1])
        dm = disc_of.setdefault(key, {})
        if g not in dm:
            dm[g] = "" if not dm else f"#{len(dm)}"
        return {"src": str(q), "dst": str(v[1]), "rel_type": v[0], "disc": dm[g]}
    def ref(q, g, v):
        return {"kind": "edge", **ident(q, g, v)}
    def assert_op(q, g, v, pr):
        edges_ever.add((q, v[0], v[1], disc_of.get((q, v[0], v[1]), {}).get(g)))
        return {"op": "assert_edge", **ident(q, g, v), "props": {}, "vt_s": v[2], "vt_e": v[3],
                "source": "ingest", "provenance_ref": pr}
    def retract_op(q, g, v, t, pr):
        return {"op": "retract", "ref": ref(q, g, v), "t": t, "source": "ingest", "provenance_ref": pr}
    ops = []; adds = []; cur_tt = None; cur_id = None; add_pr = None
    def flush():
        # adds of new statements ride one ingest_events op (pure append: their identities hold no
        # believed version, so the bulk path's disjointness holds); kit dataset_from_log reads them
        if adds:
            ops.append({"op": "ingest_events", "events": list(adds), "node_label": "Item", "source": "ingest",
                        "provenance_ref": add_pr})
        if ops:
            rec(cur_tt, ops)
    while True:
        rows = cur.fetchmany(100_000)
        if not rows:
            break
        for tt, rid, q, g, b, av, lag_from in rows:
            if tt != cur_tt:
                flush()
                ops = []; adds = []; cur_tt = tt; cur_id = rid; add_pr = None
            b = json.loads(b) if b is not None else None
            av = json.loads(av) if av is not None else None
            k = (q, g)
            if state.get(k) != b:
                inconsistent += 1
                b = state.get(k)
            if b == av:
                continue
            kind = rvkind.get(rid)
            pr = f"wd:{rid}" + (f":{kind}" if kind else "")
            if b is None:
                cls = "add"; e = assert_op(q, g, av, pr); add_pr = pr
                adds.append({k: e[k] for k in ("src", "dst", "rel_type", "disc", "props", "vt_s", "vt_e")})
            elif av is None:
                cls = "remove"; ops.append(retract_op(q, g, b, b[2], pr))
            elif b[:2] == av[:2] and b[2] == av[2] and b[3] == OPEN and av[3] < OPEN:
                cls = "close"; ops.append(retract_op(q, g, b, av[3], pr))
            else:
                cls = "edge" if b[:2] != av[:2] else "date"
                ops.append(retract_op(q, g, b, b[2], pr)); ops.append(assert_op(q, g, av, pr))
            if av is None:
                state.pop(k, None)
            else:
                state[k] = av
            cnt[(kind or "-", cls)] += 1
            if kind:
                lag = (tt - ts_us(lag_from)) / 1e6 if lag_from else None
                if lag is not None:
                    lags[kind].append(lag); lags[cls].append(lag); lags["all"].append(lag)
                touched_by_corr.add(k)
                tf.write(json.dumps({"tt": tt, "rev": rid, "kind": kind, "class": cls, "q": q, "g": g,
                                     "before": b, "after": av, "lag_s": lag}, separators=(",", ":")) + "\n")
    flush()
    f.close(); tf.close()
    Path(str(logp) + ".tmp").replace(logp); Path(str(truthp) + ".tmp").replace(truthp)
    st["log"] = {"records": nrec, "tt_first": tt_first, "tt_last": cur_tt, "inconsistent_b": inconsistent}
    st["changes_by_kind_class"] = {f"{k}|{c}": v for (k, c), v in sorted(cnt.items())}
    tot = sum(cnt.values()); corr = sum(v for (k, c), v in cnt.items() if k != "-")
    adds = sum(v for (k, c), v in cnt.items() if c == "add")
    st["correction_density"] = {"corrections": corr, "statement_events": tot, "per_event": corr / max(1, tot),
                                "per_assertion": corr / max(1, adds),
                                "statements_ever": len({k for k in edges_ever}),
                                "statements_touched_by_correction": len(touched_by_corr)}
    st["correction_lag_s"] = {k: pct(v) for k, v in lags.items()}
    # graph characterisation at the end of history
    rel = collections.Counter(); outd = collections.Counter(); ind = collections.Counter(); openn = 0
    for (q, g), v in state.items():
        rel[v[0]] += 1; outd[q] += 1; ind[v[1]] += 1; openn += v[3] == OPEN
    degs = [outd.get(n, 0) + ind.get(n, 0) for n in subset]
    st["graph_final"] = {"nodes": len(subset), "edges": len(state), "open_ended": openn, "rel_types": len(rel),
                         "top_rel": rel.most_common(25), "degree": pct(degs, (0.5, 0.9, 0.99, 0.999)),
                         "isolated_nodes": sum(1 for d in degs if d == 0)}
    # overlap with the TGB 2.0 edge list (triples; year containment for matches)
    tgb = collections.defaultdict(set)
    with open(a.tgb) as g:
        next(g)
        for line in g:
            ts, h, t, r = line.rstrip("\n").split(",")
            tgb[(int(h[1:]), r, int(t[1:]))].add(int(ts))
    fin = collections.defaultdict(list)
    for (q, g), v in state.items():
        fin[(q, v[0], v[1])].append((v[2], v[3]))
    ever = {(q, p, o) for (q, p, o, _) in edges_ever}
    import math
    def inside(y, s, e):
        ys = math.floor(s / YEAR_US + 0.01)
        return ys <= y and (e >= OPEN or y < math.ceil(e / YEAR_US - 0.01))
    m_final = [k for k in tgb if k in fin]
    yrs_in = sum(1 for k in m_final for y in tgb[k] if any(inside(y, s, e) for s, e in fin[k]))
    st["tgb_overlap"] = {"tgb_triples": len(tgb), "tgb_quadruples": sum(len(v) for v in tgb.values()),
                         "in_final_state": len(m_final), "in_ever_believed": sum(1 for k in tgb if k in ever),
                         "final_triples": len(fin), "final_not_in_tgb": sum(1 for k in fin if k not in tgb),
                         "matched_quadruples_year_inside": yrs_in,
                         "matched_quadruples": sum(len(tgb[k]) for k in m_final)}
    st["wall_s"] = round(time.time() - t0, 1)
    for p in (logp, truthp):
        h = hashlib.sha256()
        with open(p, "rb") as g:
            for blk in iter(lambda: g.read(1 << 24), b""):
                h.update(blk)
        st.setdefault("sha256", {})[p.name] = h.hexdigest()
    (out / f"W5-{a.name}.eventlog.jsonl.meta.json").write_text(json.dumps(st, indent=1, default=str) + "\n")
    print(json.dumps(st, indent=1, default=str))
    con.close(); db.unlink()
    return 0


if __name__ == "__main__":
    sys.exit(main())
