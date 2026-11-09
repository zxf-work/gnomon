"""W4 (FinBench-derived) on LadybugDB: loader, read-back verification, digest and query texts.

**FinBench-derived queries, not LDBC FinBench queries or results.**

    w4_ladybug.py load   --dir EXPORT --db DB --stage DIR --out REC     COPY FROM of the exported version rows
    w4_ladybug.py digest --db DB --out DIGEST.json                       w4_check.py digest's Cypher form

**Model.** The Neo4j / Memgraph W4 model (w4_check.load), as LadybugDB tables: one node per entity (Account,
Person, Company, Loan, Medium; key uid; the entity-level properties of the Neo4j load: max(loanAmount),
max(balance), bool_or(isBlocked), any_value(mediumType)), one AState node per account version -[:OF]-> its
account, one relationship per edge version (disc, vt_s, vt_e, tt_s, tt_e, amount). LadybugDB adds one table the
Neo4j model lacks: NState, one node per version of the other entities -[:NOF]-> its entity, so that the texts the
the SQL texts do) can be written. The digest has the Neo4j parts (entity:*, AState, edge:*) and must equal the
Neo4j and Memgraph digests; NState is checked by read-back against node_v.

**Texts** (DEVIATIONS D-W4-L1 ...): the Neo4j texts of w4_queries.CYPHER, in LadybugDB's dialect:
  - the belief and window values are written into the text as literals (LB-1: a parameter inside a quantifier or
    lambda fails an assertion);
  - Neo4j's per-node truncation, a CALL {} subquery with ORDER BY ... LIMIT $lim, is a rank test: an edge is kept
    iff fewer than $lim qualifying edges of the same node and step come before it in the order (vt_s DESC, other
    endpoint uid, disc), a correlated COUNT {} (LadybugDB has no CALL {} subquery);
  - sum() over no rows is NULL in LadybugDB and 0 in Neo4j: coalesce(sum(x), 0.0) where the text reads it;
  - shortestPath with a predicate on every edge is `* SHORTEST 1..15` with the predicate in the step;
  - TCR1, TCR2, TCR8 and TCR11 (time-ordered, amount-ordered or truncated multi-hop walks) run in rounds, one
    statement per hop, the frontier passed as a list (as the W1 LadybugDB adapter's time-respecting traversals;
    the one-statement TCR8 with OPTIONAL MATCH steps loses third-hop rows, LB-3); TCR5 is one statement;
  - every MATCH is cut into one MATCH per hop with `WITH *` between (w3_ladybug.stage_hops, the LB-3 workaround).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import sys
import time
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))
import w3_ladybug as L3  # noqa: E402  (open_db, rows, db_settings, bound_cache, defer_rel_filters)

ENTITIES = ("Account", "Person", "Company", "Loan", "Medium")
ENT_COLS = [("uid", "STRING"), ("loanAmount", "DOUBLE"), ("balance", "DOUBLE"), ("isBlocked", "BOOLEAN"),
            ("mediumType", "STRING")]
ASTATE_COLS = [("gid", "INT64"), ("vt_s", "INT64"), ("vt_e", "INT64"), ("tt_s", "INT64"), ("tt_e", "INT64"),
               ("isBlocked", "BOOLEAN"), ("accountType", "STRING"), ("createTime", "INT64")]
NSTATE_COLS = [("gid", "INT64"), ("uid", "STRING"), ("label", "STRING"), ("vt_s", "INT64"), ("vt_e", "INT64"),
               ("tt_s", "INT64"), ("tt_e", "INT64"), ("isBlocked", "BOOLEAN"), ("mediumType", "STRING"),
               ("loanAmount", "DOUBLE"), ("balance", "DOUBLE")]
EDGE_COLS = [("disc", "STRING"), ("vt_s", "INT64"), ("vt_e", "INT64"), ("tt_s", "INT64"), ("tt_e", "INT64"),
             ("amount", "DOUBLE")]
OPEN_V = 2 ** 62 - 2


# ----------------------------------------------------------------------------------------------------- load ---

def cmd_load(a) -> int:
    import duckdb
    t0 = time.time()
    d, st, dbp = Path(a.dir), Path(a.stage), Path(a.db)
    if dbp.exists():
        raise SystemExit(f"{dbp} exists; refusing to load over it")
    dbp.parent.mkdir(parents=True, exist_ok=True)
    st.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"SET threads={a.threads}; SET memory_limit='{a.duck_mem}'; SET temp_directory='{st}/tmp'")
    con.execute(f"CREATE VIEW node_v AS SELECT * FROM '{d}/node_v.parquet'")
    con.execute(f"CREATE VIEW edge_v AS SELECT * FROM '{d}/edge_v.parquet'")
    files: dict[str, Any] = {}
    # entities: exactly the Neo4j load's per-uid aggregate (w4_check.load)
    con.execute("CREATE TABLE ent AS SELECT uid, any_value(label) AS label, max(loanAmount) AS loanAmount, "
                "max(balance) AS balance, bool_or(isBlocked) AS isBlocked, any_value(mediumType) AS mediumType "
                "FROM node_v GROUP BY uid")
    for lab in ENTITIES:
        f = st / f"n_{lab}.parquet"
        con.execute(f"COPY (SELECT uid, loanAmount, balance, isBlocked, mediumType FROM ent WHERE label = '{lab}') "
                    f"TO '{f}' (FORMAT parquet)")
        files[lab] = f
    con.execute(f"COPY (SELECT row_number() OVER (ORDER BY uid, tt_s, vt_s, tt_e, vt_e) AS gid, vt_s, vt_e, tt_s, tt_e, "
                f"isBlocked, accountType, createTime, uid FROM node_v WHERE label = 'Account') TO '{st}/astate_all.parquet' (FORMAT parquet)")
    con.execute(f"COPY (SELECT gid, vt_s, vt_e, tt_s, tt_e, isBlocked, accountType, createTime FROM '{st}/astate_all.parquet') "
                f"TO '{st}/n_AState.parquet' (FORMAT parquet)")
    con.execute(f"COPY (SELECT gid AS \"from\", uid AS \"to\" FROM '{st}/astate_all.parquet') TO '{st}/r_OF.parquet' (FORMAT parquet)")
    con.execute(f"COPY (SELECT row_number() OVER (ORDER BY uid, tt_s, vt_s, tt_e, vt_e) AS gid, uid, label, vt_s, vt_e, tt_s, "
                f"tt_e, isBlocked, mediumType, loanAmount, balance FROM node_v WHERE label <> 'Account') "
                f"TO '{st}/nstate_all.parquet' (FORMAT parquet)")
    con.execute(f"COPY (SELECT {', '.join(c for c, _ in NSTATE_COLS)} FROM '{st}/nstate_all.parquet') TO '{st}/n_NState.parquet' (FORMAT parquet)")
    nof = con.execute(f"SELECT DISTINCT label FROM '{st}/nstate_all.parquet' ORDER BY 1").fetchall()
    for (lab,) in nof:
        con.execute(f"COPY (SELECT gid AS \"from\", uid AS \"to\" FROM '{st}/nstate_all.parquet' WHERE label = '{lab}') "
                    f"TO '{st}/r_NOF__{lab}.parquet' (FORMAT parquet)")
    pairs = con.execute("SELECT e.rel, s.label, t.label, count(*) FROM edge_v e JOIN ent s ON s.uid = e.src "
                        "JOIN ent t ON t.uid = e.dst GROUP BY 1, 2, 3 ORDER BY 1, 2, 3").fetchall()
    n_edges = con.execute("SELECT count(*) FROM edge_v").fetchone()[0]
    if sum(p[3] for p in pairs) != n_edges:
        raise SystemExit(f"edges with an endpoint outside node_v: {n_edges - sum(p[3] for p in pairs)}")
    for rel, sl, tl, _ in pairs:
        con.execute(f"COPY (SELECT e.src AS \"from\", e.dst AS \"to\", disc, vt_s, vt_e, tt_s, tt_e, amount FROM edge_v e "
                    f"JOIN ent s ON s.uid = e.src AND s.label = '{sl}' JOIN ent t ON t.uid = e.dst AND t.label = '{tl}' "
                    f"WHERE e.rel = '{rel}') TO '{st}/r_{rel}__{sl}__{tl}.parquet' (FORMAT parquet)")
    t1 = time.time()
    db, conn = L3.open_db(str(dbp), read_only=False, threads=a.threads)
    rec: dict[str, Any] = {"system": "ladybug", "export": str(d), "stage_s": round(t1 - t0, 1),
                           "settings": L3.db_settings(db), "method": "bulk: COPY FROM Parquet (staged from the export)"}
    ddl = [f"CREATE NODE TABLE {lab}(" + ", ".join(f"{c} {t}" for c, t in ENT_COLS) + ", PRIMARY KEY (uid))"
           for lab in ENTITIES]
    ddl.append("CREATE NODE TABLE AState(" + ", ".join(f"{c} {t}" for c, t in ASTATE_COLS) + ", PRIMARY KEY (gid))")
    ddl.append("CREATE NODE TABLE NState(" + ", ".join(f"{c} {t}" for c, t in NSTATE_COLS) + ", PRIMARY KEY (gid))")
    ddl.append("CREATE REL TABLE OF(FROM AState TO Account)")
    ddl.append("CREATE REL TABLE NOF(" + ", ".join(f"FROM NState TO {lab}" for (lab,) in nof) + ")")
    by: dict[str, list] = {}
    for rel, sl, tl, n in pairs:
        by.setdefault(rel, []).append((sl, tl, n))
    for rel, ps in by.items():
        ddl.append(f"CREATE REL TABLE {rel}(" + ", ".join(f"FROM {s} TO {t}" for s, t, _ in ps) + ", "
                   + ", ".join(f"{c} {t}" for c, t in EDGE_COLS) + ")")
    for s in ddl:
        conn.execute(s)
    copy = {}
    for lab in ENTITIES + ("AState", "NState"):
        tc = time.time()
        conn.execute(f"COPY {lab} FROM '{st}/n_{lab}.parquet'")
        copy[lab] = round(time.time() - tc, 2)
    conn.execute(f"COPY OF FROM '{st}/r_OF.parquet'")
    for (lab,) in nof:
        conn.execute(f"COPY NOF FROM '{st}/r_NOF__{lab}.parquet' (from='NState', to='{lab}')")
    for rel, sl, tl, _ in pairs:
        tc = time.time()
        conn.execute(f"COPY {rel} FROM '{st}/r_{rel}__{sl}__{tl}.parquet' (from='{sl}', to='{tl}')")
        copy[f"{rel}:{sl}->{tl}"] = round(time.time() - tc, 2)
    conn.execute("CHECKPOINT")
    t2 = time.time()
    rec.update(copy_from_s=round(t2 - t1, 1), load_s=round(t2 - t1, 1), copy=copy,
               rel_pairs={r: [[s, t, n] for s, t, n in ps] for r, ps in by.items()})
    rec["verify"] = verify(conn, con)
    rec["verified"] = rec["verify"]["verified"]
    conn.close()
    db.close()
    rec["bytes"] = sum(p.stat().st_size for p in dbp.parent.glob(dbp.name + "*"))
    if not a.keep_stage:
        shutil.rmtree(st, ignore_errors=True)
    Path(a.out).write_text(json.dumps(rec, indent=1, default=str) + "\n")
    print(json.dumps({k: v for k, v in rec.items() if k not in ("copy", "verify", "rel_pairs")}, default=str))
    return 0 if rec["verified"] else 1


def verify(conn, con) -> dict[str, Any]:
    """w4_check.verify's parts, read back from LadybugDB and computed from the export: per edge type and per
    version table [count, sum(vt_s s), sum(tt_s s), closed sum(vt_e s), closed sum(tt_e s), open vt_e, open tt_e]
    (whole seconds), and the entity counts."""
    agg = (f"count(*), sum(vt_s // 1000000), sum(tt_s // 1000000), "
           f"sum(CASE WHEN vt_e >= {OPEN_V} THEN 0 ELSE vt_e // 1000000 END), "
           f"sum(CASE WHEN tt_e >= {OPEN_V} THEN 0 ELSE tt_e // 1000000 END), "
           f"sum(CASE WHEN vt_e >= {OPEN_V} THEN 1 ELSE 0 END), sum(CASE WHEN tt_e >= {OPEN_V} THEN 1 ELSE 0 END)")
    cagg = lambda v: (f"count({v}), sum({v}.vt_s / 1000000), sum({v}.tt_s / 1000000), "  # noqa: E731
                      f"sum(CASE WHEN {v}.vt_e >= {OPEN_V} THEN 0 ELSE {v}.vt_e / 1000000 END), "
                      f"sum(CASE WHEN {v}.tt_e >= {OPEN_V} THEN 0 ELSE {v}.tt_e / 1000000 END), "
                      f"sum(CASE WHEN {v}.vt_e >= {OPEN_V} THEN 1 ELSE 0 END), sum(CASE WHEN {v}.tt_e >= {OPEN_V} THEN 1 ELSE 0 END)")
    parts = {}
    for rel, *v in con.execute(f"SELECT rel, {agg} FROM edge_v GROUP BY rel ORDER BY rel").fetchall():
        got = L3.rows(conn, f"MATCH ()-[x:{rel}]->() RETURN {cagg('x')}")[1][0]
        parts["edge:" + rel] = {"store": [int(x or 0) for x in got], "export": [int(x or 0) for x in v]}
    for lab, *v in con.execute(f"SELECT label, {agg} FROM node_v GROUP BY label ORDER BY label").fetchall():
        q = (f"MATCH (x:AState)-[:OF]->(:Account) RETURN {cagg('x')}" if lab == "Account"
             else f"MATCH (x:NState)-[:NOF]->(:{lab}) RETURN {cagg('x')}")
        got = L3.rows(conn, q)[1][0]
        parts["node_v:" + lab] = {"store": [int(x or 0) for x in got], "export": [int(x or 0) for x in v]}
    for lab, n in con.execute("SELECT label, count(DISTINCT uid) FROM node_v GROUP BY label ORDER BY label").fetchall():
        got = L3.rows(conn, f"MATCH (n:{lab}) RETURN count(n)")[1][0][0]
        parts["entity:" + lab] = {"store": [int(got)], "export": [int(n)]}
    return {"verified": all(p["store"] == p["export"] for p in parts.values()), "parts": parts}


def _hv(*xs) -> int:
    return int(hashlib.sha256(repr(xs).encode()).hexdigest()[:16], 16)


def _digest_job(job):
    db_path, kind, name, extra = job
    db, conn = L3.open_db(db_path, read_only=True, threads=4)
    M = 1 << 64
    acc: dict[str, list] = {}

    def add(key, h):
        c = acc.setdefault(key, [0, 0]); c[0] += 1; c[1] = (c[1] + h) % M
    if kind == "entity":
        names = [c for c, _ in ENT_COLS]
        r = conn.execute(f"MATCH (n:{name}) RETURN " + ", ".join(f"n.{c}" for c in names))
        while r.has_next():
            x = r.get_next()
            add("entity:" + name, _hv(name, sorted((k, v) for k, v in zip(names, x) if v is not None)))
    elif kind == "astate":
        names = [c for c, _ in ASTATE_COLS if c != "gid"]
        r = conn.execute("MATCH (x:AState)-[:OF]->(a:Account) RETURN a.uid, " + ", ".join(f"x.{c}" for c in names))
        while r.has_next():
            x = r.get_next()
            add("AState", _hv(x[0], sorted((k, v) for k, v in zip(names, x[1:]) if v is not None)))
    else:
        names = [c for c, _ in EDGE_COLS]
        s, t = extra
        r = conn.execute(f"MATCH (a:{s})-[x:{name}]->(b:{t}) RETURN a.uid, b.uid, " + ", ".join(f"x.{c}" for c in names))
        while r.has_next():
            x = r.get_next()
            add("edge:" + name, _hv(name, x[0], x[1], sorted((k, v) for k, v in zip(names, x[2:]) if v is not None)))
    conn.close()
    db.close()
    return acc


def cmd_digest(a) -> int:
    """w4_check.py digest's Cypher form: entity labels and properties (non-null), AState
    properties with its account's uid, relationship type, endpoint uids and properties. NState is not a part (the
    Neo4j model has no such nodes)."""
    import multiprocessing as mp
    t0 = time.time()
    db, conn = L3.open_db(a.db, read_only=True)
    jobs = [(a.db, "entity", lab, None) for lab in ENTITIES] + [(a.db, "astate", "AState", None)]
    for name, typ in L3.rows(conn, "CALL show_tables() RETURN name, type")[1]:
        if typ == "REL" and name not in ("OF", "NOF"):
            for c in L3.rows(conn, f"CALL show_connection('{name}') RETURN *")[1]:
                jobs.append((a.db, "edge", name, (c[0], c[1])))
    conn.close()
    db.close()
    M = 1 << 64
    out: dict[str, list] = {}
    with mp.get_context("spawn").Pool(a.procs) as pool:
        for part in pool.imap_unordered(_digest_job, jobs):
            for k, (n, h) in part.items():
                c = out.setdefault(k, [0, 0]); c[0] += n; c[1] = (c[1] + h) % M
    Path(a.out).write_text(json.dumps(out, indent=1, sort_keys=True) + "\n")
    print(json.dumps({"system": "ladybug", "parts": len(out), "rows": sum(v[0] for v in out.values()),
                      "digest_s": round(time.time() - t0, 1)}))
    return 0


# ---------------------------------------------------------------------------------------------------- texts ---

def _rank(e: str, node: str, other: str, rel: str, side: str, extra: str = "", olab: str = ":Account") -> str:
    """Neo4j's truncation (the $lim most recent qualifying edges of `node` in this step; ties: other endpoint uid,
    disc) as a rank test on edge `e`: fewer than $lim qualifying edges of `node` come before it."""
    pat = (f"({node})-[_e2:{rel}]->(_x2{olab})" if side == "out" else f"(_x2{olab})-[_e2:{rel}]->({node})")
    return (f"COUNT {{ MATCH {pat} WHERE {{B:_e2}} AND {{W:_e2}}{extra.replace('$E', '_e2')} AND "
            f"(_e2.vt_s > {e}.vt_s OR (_e2.vt_s = {e}.vt_s AND (_x2.uid < {other}.uid OR "
            f"(_x2.uid = {other}.uid AND _e2.disc < {e}.disc)))) }} < $lim")


LB: dict[str, str] = {
    "TSR1": """MATCH (a:Account {uid: $id})<-[:OF]-(s:AState) WHERE {B:s}
RETURN s.createTime, s.isBlocked, s.accountType""",
    "TSR2": """MATCH (a:Account {uid: $id})
OPTIONAL MATCH (a)-[e1:TRANSFER]->(:Account) WHERE {B:e1} AND {W:e1}
WITH a, coalesce(sum(e1.amount), 0.0) AS s1, max(e1.amount) AS m1, count(e1) AS n1
OPTIONAL MATCH (a)<-[e2:TRANSFER]-(:Account) WHERE {B:e2} AND {W:e2}
RETURN s1, coalesce(m1, -1.0), n1, coalesce(sum(e2.amount), 0.0), coalesce(max(e2.amount), -1.0), count(e2)""",
    "TSR3": """MATCH (src:Account)-[e:TRANSFER]->(a:Account {uid: $id}) WHERE {B:e} AND {W:e} AND e.amount > $threshold
MATCH (src)<-[:OF]-(s:AState) WHERE {B:s}
WITH count(e) AS n, sum(CASE WHEN s.isBlocked THEN 1 ELSE 0 END) AS b
RETURN CASE WHEN n = 0 THEN -1.0 ELSE 1.0 * CAST(b AS DOUBLE) / n END""",
    "TSR4": """MATCH (a:Account {uid: $id})-[e:TRANSFER]->(d:Account) WHERE {B:e} AND {W:e} AND e.amount > $threshold
RETURN d.uid, count(e), sum(e.amount)""",
    "TSR5": """MATCH (a:Account {uid: $id})<-[e:TRANSFER]-(s:Account) WHERE {B:e} AND {W:e} AND e.amount > $threshold
RETURN s.uid, count(e), sum(e.amount)""",
    "TSR6": """MATCH (src:Account {uid: $id})<-[e1:TRANSFER]-(mid:Account)-[e2:TRANSFER]->(dst:Account)
WHERE dst.uid <> src.uid AND {B:e1} AND {W:e1} AND {B:e2} AND {W:e2}
MATCH (dst)<-[:OF]-(s:AState) WHERE {B:s} AND s.isBlocked
RETURN DISTINCT dst.uid""",
    "TCR3": """MATCH (s:Account {uid: $id1}), (d:Account {uid: $id2})
OPTIONAL MATCH (s)-[es:TRANSFER* SHORTEST 1..15 (_r, _n | WHERE {B:_r} AND {W:_r})]->(d)
RETURN CASE WHEN es IS NULL THEN -1 ELSE length(es) END""",
    "TCR4": """MATCH (s:Account {uid: $id1})-[e1:TRANSFER]->(d:Account {uid: $id2}) WHERE {B:e1} AND {W:e1}
WITH s, d LIMIT 1
MATCH (d)-[e3:TRANSFER]->(o:Account)-[e2:TRANSFER]->(s) WHERE {B:e2} AND {W:e2} AND {B:e3} AND {W:e3}
WITH DISTINCT s, o, d
MATCH (o)-[e2:TRANSFER]->(s) WHERE {B:e2} AND {W:e2}
WITH s, o, d, count(e2) AS n2, sum(e2.amount) AS s2, max(e2.amount) AS m2
MATCH (d)-[e3:TRANSFER]->(o) WHERE {B:e3} AND {W:e3}
RETURN o.uid, n2, s2, m2, count(e3), sum(e3.amount), max(e3.amount)""",
    "TCR6": """MATCH (mid:Account)-[w:WITHDRAW]->(card:Account {uid: $id})
WHERE {B:w} AND {W:w} AND w.amount > $threshold2 AND """ + _rank("w", "card", "mid", "WITHDRAW", "in", " AND $E.amount > $threshold2") + """
WITH mid, sum(w.amount) AS s2
MATCH (src:Account)-[t:TRANSFER]->(mid)
WHERE {B:t} AND {W:t} AND t.amount > $threshold1 AND """ + _rank("t", "mid", "src", "TRANSFER", "in", " AND $E.amount > $threshold1") + """
WITH mid, s2, count(DISTINCT src) AS ns, sum(t.amount) AS s1
WHERE ns > 3
RETURN mid.uid, s1, s2""",
    "TCR7": """MATCH (a:Account {uid: $id})
OPTIONAL MATCH (x:Account)-[e:TRANSFER]->(a)
WHERE {B:e} AND {W:e} AND e.amount > $threshold AND """ + _rank("e", "a", "x", "TRANSFER", "in", " AND $E.amount > $threshold") + """
WITH a, count(DISTINCT x) AS ns, sum(e.amount) AS si
OPTIONAL MATCH (a)-[e:TRANSFER]->(x:Account)
WHERE {B:e} AND {W:e} AND e.amount > $threshold AND """ + _rank("e", "a", "x", "TRANSFER", "out", " AND $E.amount > $threshold") + """
WITH ns, si, count(DISTINCT x) AS nd, sum(e.amount) AS so, count(e) AS ne
RETURN ns, nd, CASE WHEN ne = 0 THEN -1.0 ELSE coalesce(si, 0.0) / so END""",
    "TCR9": """MATCH (a:Account {uid: $id})
OPTIONAL MATCH (:Loan)-[d:DEPOSIT]->(a) WHERE {B:d} AND {W:d} AND d.amount > $threshold
WITH a, coalesce(sum(d.amount), 0.0) AS dep
OPTIONAL MATCH (a)-[r:REPAY]->(:Loan) WHERE {B:r} AND {W:r} AND r.amount > $threshold
WITH a, dep, coalesce(sum(r.amount), 0.0) AS rep
OPTIONAL MATCH (:Account)-[i:TRANSFER]->(a) WHERE {B:i} AND {W:i} AND i.amount > $threshold
WITH a, dep, rep, coalesce(sum(i.amount), 0.0) AS tin
OPTIONAL MATCH (a)-[o:TRANSFER]->(:Account) WHERE {B:o} AND {W:o} AND o.amount > $threshold
WITH dep, rep, tin, coalesce(sum(o.amount), 0.0) AS tout
RETURN CASE WHEN rep = 0 THEN -1.0 ELSE 1.0 * dep / rep END, CASE WHEN tout = 0 THEN -1.0 ELSE 1.0 * dep / tout END,
       CASE WHEN tout = 0 THEN -1.0 ELSE 1.0 * tin / tout END""",
    "TCR10": """MATCH (p:Person)-[e:INVEST]->(c:Company) WHERE p.uid IN [$pid1, $pid2] AND {B:e} AND {W:e}
WITH c, count(DISTINCT p) AS k
WITH count(c) AS u, sum(CASE WHEN k = 2 THEN 1 ELSE 0 END) AS i
RETURN CASE WHEN u = 0 THEN 0.0 ELSE 1.0 * CAST(i AS DOUBLE) / u END""",
    "TCR12": """MATCH (p:Person {uid: $id})-[o:OWN]->(acc:Account) WHERE {B:o}
MATCH (acc)-[e:TRANSFER]->(c:Account) WHERE {B:e} AND {W:e} AND """ + _rank("e", "acc", "c", "TRANSFER", "out") + """
MATCH (co:Company)-[o2:OWN]->(c) WHERE {B:o2}
WITH DISTINCT c, e
RETURN c.uid, sum(e.amount)""",
    # one statement: the time-ordered walks of 1..3 hops (strictly increasing edge starts, distinct nodes), as the
    # SQL text's p1 / p2 / p3 (no truncation; w4_queries: paths has no truncation argument)
    "TCR5": """MATCH (p:Person {uid: $id})-[o:OWN]->(a0:Account) WHERE {B:o}
MATCH (a0)-[t1:TRANSFER]->(a1:Account) WHERE {B:t1} AND {W:t1} AND a0.uid <> a1.uid
RETURN DISTINCT [a0.uid, a1.uid] AS path
UNION
MATCH (p:Person {uid: $id})-[o:OWN]->(a0:Account) WHERE {B:o}
MATCH (a0)-[t1:TRANSFER]->(a1:Account)-[t2:TRANSFER]->(a2:Account)
WHERE {B:t1} AND {W:t1} AND {B:t2} AND {W:t2} AND t2.vt_s > t1.vt_s AND a0.uid <> a1.uid AND a0.uid <> a2.uid AND a1.uid <> a2.uid
RETURN DISTINCT [a0.uid, a1.uid, a2.uid] AS path
UNION
MATCH (p:Person {uid: $id})-[o:OWN]->(a0:Account) WHERE {B:o}
MATCH (a0)-[t1:TRANSFER]->(a1:Account)-[t2:TRANSFER]->(a2:Account)-[t3:TRANSFER]->(a3:Account)
WHERE {B:t1} AND {W:t1} AND {B:t2} AND {W:t2} AND {B:t3} AND {W:t3} AND t2.vt_s > t1.vt_s AND t3.vt_s > t2.vt_s
  AND a0.uid <> a1.uid AND a0.uid <> a2.uid AND a0.uid <> a3.uid AND a1.uid <> a2.uid AND a1.uid <> a3.uid AND a2.uid <> a3.uid
RETURN DISTINCT [a0.uid, a1.uid, a2.uid, a3.uid] AS path""",
}
#: rounds (one statement per hop, the frontier as a list): TCR1, TCR2, TCR8, TCR11
ROUNDS = ("TCR1", "TCR2", "TCR8", "TCR11")
EXPRESSIBLE = sorted(set(LB) | set(ROUNDS))


def subst(s: str, p: dict) -> str:
    """{B:x} / {W:x} with the belief and window values as literals (LB-1), as w4_queries.cypher's macros."""
    s = re.sub(r"\{B:(\w+)\}", lambda m: (f"{m[1]}.vt_s <= {int(p['now'])} AND {int(p['now'])} < {m[1]}.vt_e AND "
                                         f"{m[1]}.tt_s <= {int(p['T'])} AND {int(p['T'])} < {m[1]}.tt_e"), s)
    return re.sub(r"\{W:(\w+)\}", lambda m: f"{int(p['start'])} < {m[1]}.vt_s AND {m[1]}.vt_s < {int(p['end'])}", s)


def _params(stmt: str, p: dict) -> dict:
    return {k: v for k, v in p.items() if re.search(rf"\${k}\b", stmt)}


R_TCR1 = ("UNWIND $F AS f MATCH (a:Account) WHERE a.uid = f.u MATCH (a)-[t:TRANSFER]->(n:Account) "
          "WHERE {B:t} AND {W:t} AND t.vt_s > f.ts AND " + _rank("t", "a", "n", "TRANSFER", "out")
          + " RETURN n.uid AS u, min(t.vt_s) AS ts")
R_TCR1_END = ("MATCH (m:Medium)-[si:SIGN_IN]->(a:Account) WHERE a.uid IN $us AND {B:si} AND {W:si} "
              "MATCH (m)<-[:NOF]-(mv:NState) WHERE {B:mv} AND mv.isBlocked RETURN DISTINCT a.uid, m.uid, mv.mediumType")
R_TCR2_START = "MATCH (p:Person {uid: $id})-[o:OWN]->(a:Account) WHERE {B:o} RETURN DISTINCT a.uid"
R_TCR2 = ("UNWIND $F AS f MATCH (a:Account) WHERE a.uid = f.u MATCH (x:Account)-[t:TRANSFER]->(a) "
          "WHERE {B:t} AND {W:t} AND t.vt_s < f.ts AND " + _rank("t", "a", "x", "TRANSFER", "in")
          + " RETURN x.uid AS u, max(t.vt_s) AS ts")
R_TCR2_END = ("MATCH (loan:Loan)-[dep:DEPOSIT]->(o:Account) WHERE o.uid IN $os AND {B:dep} AND {W:dep} "
              "WITH DISTINCT o, loan MATCH (loan)<-[:NOF]-(n:NState) WHERE {B:n} "
              "RETURN o.uid, sum(n.loanAmount), sum(n.balance)")
R_TCR11 = ("MATCH (a:Person:Company)-[g:GUARANTEE]->(b:Person:Company) WHERE a.uid IN $F AND {B:g} AND {W:g} AND "
           + _rank("g", "a", "b", "GUARANTEE", "out", "", ":Person:Company") + " RETURN DISTINCT b.uid")
R_TCR11_END = ("MATCH (x:Person:Company)-[ap:APPLY]->(l:Loan) WHERE x.uid IN $ns AND {B:ap} WITH DISTINCT l "
               "MATCH (l)<-[:NOF]-(n:NState) WHERE {B:n} RETURN coalesce(sum(n.loanAmount), 0.0), count(*)")
R_TCR8_START = "MATCH (l:Loan {uid: $id})-[dp:DEPOSIT]->(a0:Account) WHERE {B:dp} AND {W:dp} RETURN DISTINCT a0.uid"
R_TCR8 = ("UNWIND $F AS f MATCH (a:Account) WHERE a.uid = f.u MATCH (a)-[h:TRANSFER|WITHDRAW]->(x:Account) "
          "WHERE {B:h} AND {W:h} AND h.amount > f.a * $threshold "
          "RETURN x.uid, concat(a.uid, '|', x.uid, '|', label(h), '|', h.disc), h.amount")
R_TCR8_LOAN = "MATCH (l:Loan {uid: $id})<-[:NOF]-(n:NState) WHERE {B:n} RETURN n.loanAmount"
LOW = -(2 ** 62)


def statements(q: str, p: dict) -> list[str]:
    """The texts one call runs (for the cache bound and the record)."""
    if q == "TCR1":
        return [L3.lb3(subst(x, p)) for x in (R_TCR1, R_TCR1_END)]
    if q == "TCR2":
        return [L3.lb3(subst(x, p)) for x in (R_TCR2_START, R_TCR2, R_TCR2_END)]
    if q == "TCR11":
        return [L3.lb3(subst(x, p)) for x in (R_TCR11, R_TCR11_END)]
    if q == "TCR8":
        return [L3.lb3(subst(x, p)) for x in (R_TCR8_START, R_TCR8, R_TCR8_LOAN)]
    return [L3.lb3(subst(LB[q], p))]


_STMTS: dict = {}


def run_query(conn, q: str, p: dict) -> list[list[Any]]:
    """Rows in w4_queries.RESULT column order (before canonical())."""
    if q not in LB and q not in ROUNDS:
        raise NotImplementedError(f"{q}: not expressible on LadybugDB")
    key = (q, json.dumps(p, sort_keys=True, default=str))
    st = _STMTS.get(key)
    if st is None:                    # text expansion once per instance (harness work, outside the query's own work)
        if len(_STMTS) > 256:
            _STMTS.clear()
        st = _STMTS[key] = statements(q, p)
    L3.bound_cache(conn, st)
    run = lambda s, **kw: L3.rows(conn, s, _params(s, {**p, **kw}))[1]   # noqa: E731
    if q == "TCR1":
        best: dict[str, int] = {}
        F = [{"u": p["id"], "ts": LOW}]
        for d in (1, 2, 3):
            got = run(st[0], F=F)
            for u, _ in got:
                best.setdefault(u, d)
            F = [{"u": u, "ts": ts} for u, ts in got]          # per node its smallest arrival time
            if not F:
                break
        if not best:
            return []
        return [[u, best[u], m, mt] for u, m, mt in run(st[1], us=sorted(best))]
    if q == "TCR2":
        accs = [r[0] for r in run(st[0])]
        seen: set[str] = set()
        F = [{"u": u, "ts": 2 ** 62} for u in accs]
        for _ in (1, 2, 3):
            if not F:
                break
            got = run(st[1], F=F)
            seen.update(u for u, _ in got)
            F = [{"u": u, "ts": ts} for u, ts in got]          # per node its latest departure time
        if not seen:
            return []
        return [list(r) for r in run(st[2], os=sorted(seen))]
    if q == "TCR8":
        # the SQL text's p1 / p2 / p3 by hop: a step's amount must exceed the previous step's x threshold, so per
        # node the frontier keeps the smallest amount that reached it; e = per (destination, last edge) the largest
        # amount and the smallest depth (2, 3, 4); per destination sum / loan amount and the smallest depth
        F = [{"u": u, "a": -1.0} for (u,) in run(st[0])]
        e: dict[tuple, list] = {}
        for depth in (2, 3, 4):
            if not F:
                break
            got = run(st[1], F=F)
            nxt: dict[str, float] = {}
            for dst, le, amt in got:
                cur = e.get((dst, le))
                e[(dst, le)] = [amt, depth] if cur is None else [max(cur[0], amt), min(cur[1], depth)]
                nxt[dst] = amt if dst not in nxt else min(nxt[dst], amt)
            F = [{"u": u, "a": a} for u, a in nxt.items()]
        la = run(st[2])
        if not e or not la:
            return []
        per: dict[str, list] = {}
        for (dst, _), (amt, depth) in e.items():
            c = per.setdefault(dst, [0.0, depth]); c[0] += amt; c[1] = min(c[1], depth)
        return [[dst, s_ / la[0][0], d_] for dst, (s_, d_) in per.items()]
    if q == "TCR11":
        seen = set()
        F = [p["id"]]
        for _ in range(5):
            got = [r[0] for r in run(st[0], F=F)]
            new = sorted(set(got) - seen)
            seen.update(got)
            F = new
            if not F:
                break
        if not seen:
            return [[0.0, 0]]
        r = run(st[1], ns=sorted(seen))[0]
        return [[r[0], r[1]]]
    return [list(r) for r in run(st[0])]


def executor(db: Path, alarm: int, state: dict):
    """w4_check.executor's call for LadybugDB (in-process): ex(query, params) -> rows. Threads: LadybugDB's
    default unless W4_LADYBUG_THREADS is set; the cap is LadybugDB's query timeout (the alarm)."""
    threads = int(os.environ.get("W4_LADYBUG_THREADS", "0") or 0)
    dbh, conn = L3.open_db(str(db), read_only=True, threads=threads)
    conn.set_query_timeout(int(alarm * 1000))
    import prime_cap as PC
    PC.hook(lambda sec: conn.set_query_timeout(int((sec if sec else alarm) * 1000)))
    state.update(ladybug_db=dbh, ladybug_conn=conn, threads=threads or "default")

    def ex(q, p):
        return run_query(conn, q, p)
    return ex


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    lo = sub.add_parser("load"); lo.add_argument("--dir", required=True); lo.add_argument("--db", required=True)
    lo.add_argument("--stage", required=True); lo.add_argument("--out", required=True)
    lo.add_argument("--threads", type=int, default=int(os.environ.get("SLURM_NTASKS_PER_NODE", "8")))
    lo.add_argument("--duck-mem", default="16GB"); lo.add_argument("--keep-stage", action="store_true")
    dg = sub.add_parser("digest"); dg.add_argument("--db", required=True); dg.add_argument("--out", required=True)
    dg.add_argument("--procs", type=int, default=8)
    a = ap.parse_args()
    return {"load": cmd_load, "digest": cmd_digest}[a.cmd](a)


if __name__ == "__main__":
    sys.exit(main())
