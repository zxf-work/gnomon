#!/usr/bin/env python3
"""W4 (FinBench-derived): export, load, curated parameters and the untimed
answer check on every system.

    w4_check.py export  --log L --store S --out DIR     # replay + typed version tables (parquet)
    w4_check.py params  --dir DIR --log-meta M --out P.json [--per 5]
    w4_check.py load    --system duckdbsql|postgres|neo4j|memgraph --dir DIR [--db F]
    w4_check.py run     --system gnomon|duckdbsql|postgres|neo4j|memgraph|ladybug --params P.json
                        [--store S | --db F] --out answers.jsonl [--queries TCR1,...] [--alarm 60]
    w4_check.py compare --base answers-gnomon.jsonl answers-*.jsonl --json verdicts.json

Every system reads the same version rows: the Gnomon store replayed from the
frozen log, exported with its own transaction times (as the registry
baselines do, scripts/pg_baseline.py::load). An answer is the canonical row
list of w4_queries.canonical() (ids stripped, floats to three decimals, rows
in the specification's order); its hash is compared with Gnomon's.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import signal
import sys
import time
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))
import w4_queries as Q  # noqa: E402
import prime_cap as PC  # noqa: E402

NODE_COLS = ["uid", "label", "vt_s", "vt_e", "tt_s", "tt_e", "isBlocked", "accountType", "createTime",
             "mediumType", "loanAmount", "balance"]
EDGE_COLS = ["src", "dst", "rel", "disc", "vt_s", "vt_e", "tt_s", "tt_e", "amount"]
DDL_NODE = ("CREATE TABLE node_v (uid VARCHAR, label VARCHAR, vt_s BIGINT, vt_e BIGINT, tt_s BIGINT, tt_e BIGINT, "
            "isBlocked BOOLEAN, accountType VARCHAR, createTime BIGINT, mediumType VARCHAR, loanAmount DOUBLE, "
            "balance DOUBLE)")
DDL_EDGE = ("CREATE TABLE edge_v (src VARCHAR, dst VARCHAR, rel VARCHAR, disc VARCHAR, vt_s BIGINT, vt_e BIGINT, "
            "tt_s BIGINT, tt_e BIGINT, amount DOUBLE)")
SQL_INDEXES = ["CREATE INDEX ev_src ON edge_v (src, rel, vt_s)", "CREATE INDEX ev_dst ON edge_v (dst, rel, vt_s)",
               "CREATE INDEX nv_uid ON node_v (uid)"]


def h(rows: Any) -> str:
    return hashlib.sha256(json.dumps(rows, sort_keys=True, default=str).encode()).hexdigest()[:24]


# ---------------------------------------------------------------- export ----

def export(log: Path, store: Path, out: Path) -> None:
    import duckdb
    import gnomon
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import eval_harness as H
    out.mkdir(parents=True, exist_ok=True)
    t0 = time.time()
    if not (store / "eventlog.jsonl").exists():
        H.load_store(store, "native", log)
    rec = {"replay_s": round(time.time() - t0, 1)}
    s = gnomon.open(store)
    a = s.adapter
    con = duckdb.connect()
    con.execute(DDL_NODE)
    con.execute(DDL_EDGE)

    import pandas as pd

    def flush(table, rows):
        # a DataFrame insert: executemany is row-at-a-time (40+ min for SF1's 6M edges)
        if rows:
            df = pd.DataFrame(rows, columns=NODE_COLS if table == "node_v" else EDGE_COLS, dtype=object)  # noqa: F841
            con.execute(f"INSERT INTO {table} SELECT * FROM df")
            rows.clear()
    rows: list = []
    n = 0
    for v in a.all_node_versions():
        p = v.props
        rows.append((v.uid, v.label, v.vt_s, v.vt_e, v.tt_s, v.tt_e, p.get("isBlocked"), p.get("accountType"),
                     p.get("createTime"), p.get("mediumType"), p.get("loanAmount"), p.get("balance")))
        n += 1
        if len(rows) >= 500_000:
            flush("node_v", rows)
    flush("node_v", rows)
    m = 0
    for v in a.all_edge_versions():
        rows.append((v.src, v.dst, v.rel_type, v.disc, v.vt_s, v.vt_e, v.tt_s, v.tt_e, v.props.get("amount")))
        m += 1
        if len(rows) >= 500_000:
            flush("edge_v", rows)
    flush("edge_v", rows)
    s.close()
    con.execute(f"COPY node_v TO '{out}/node_v.parquet' (FORMAT parquet)")
    con.execute(f"COPY edge_v TO '{out}/edge_v.parquet' (FORMAT parquet)")
    tts = [r[0] for r in con.execute("SELECT DISTINCT tt_s FROM edge_v UNION SELECT DISTINCT tt_s FROM node_v "
                                     "ORDER BY 1").fetchall()]
    rec.update(node_versions=n, edge_versions=m, export_s=round(time.time() - t0, 1), commit_tts=len(tts),
               tt_first=tts[0], tt_last=tts[-1])
    (out / "export.json").write_text(json.dumps(rec, indent=1))
    (out / "commit_tts.json").write_text(json.dumps(tts))
    print(json.dumps(rec))


# ---------------------------------------------------------------- params ----

def params(d: Path, meta: Path, out: Path, per: int) -> None:
    """Curated instances: seeds at percentiles of their window activity (p10/p50/p90/p99 + the
    busiest), two windows (the whole history; the 30 days before the bulk-load threshold), two
    belief times (current; the commit at the middle of the incremental stream)."""
    import duckdb
    m = json.loads(meta.read_text())
    t0 = m["t0_ms"] * 1000
    tts = json.loads((d / "commit_tts.json").read_text())
    inc = [t for t in tts if t > t0 + 10_000]
    T_hist = inc[len(inc) // 2] if inc else tts[-1]
    now = (m["end_ms"] + 1) * 1000
    con = duckdb.connect()
    con.execute(f"CREATE VIEW edge_v AS SELECT * FROM '{d}/edge_v.parquet'")
    con.execute(f"CREATE VIEW node_v AS SELECT * FROM '{d}/node_v.parquet'")
    wins = {"all": (1577836800000 * 1000, m["end_ms"] * 1000), "30d": (t0 - 30 * 86_400_000_000, t0)}
    cur = f"vt_e > {now} AND tt_e > {2 ** 62 - 2}"

    def pick(sql: str) -> list[tuple[str, int]]:
        rows = con.execute(sql).fetchall()
        if not rows:
            return []
        rows.sort(key=lambda r: (r[1], r[0]))
        idx = [int(q * (len(rows) - 1)) for q in (0.10, 0.50, 0.90, 0.99)] + [len(rows) - 1]
        seen, outp = set(), []
        for i in idx:
            if rows[i][0] not in seen:
                seen.add(rows[i][0])
                outp.append((rows[i][0], rows[i][1]))
        return outp[:per]
    seeds: dict[str, dict[str, list]] = {}
    for wn, (a, b) in wins.items():
        W = f"vt_s > {a} AND vt_s < {b} AND {cur}"
        seeds[wn] = {
            "account": pick(f"SELECT u, count(*) FROM (SELECT src u FROM edge_v WHERE rel='TRANSFER' AND {W} UNION ALL "
                            f"SELECT dst FROM edge_v WHERE rel='TRANSFER' AND {W}) GROUP BY u"),
            "card": pick(f"SELECT dst, count(*) FROM edge_v WHERE rel='WITHDRAW' AND {W} GROUP BY dst"),
            "person": pick(f"SELECT o.src, count(*) FROM edge_v o JOIN edge_v t ON t.src = o.dst AND t.rel='TRANSFER' "
                           f"AND t.vt_s > {a} AND t.vt_s < {b} AND t.vt_e > {now} AND t.tt_e > {2 ** 62 - 2} "
                           f"WHERE o.rel='OWN' AND o.src LIKE 'Person:%' AND o.vt_e > {now} AND o.tt_e > {2 ** 62 - 2} "
                           f"GROUP BY o.src"),
            "loan": pick(f"SELECT src, count(*) FROM edge_v WHERE rel='DEPOSIT' AND {W} GROUP BY src"),
            "guarantor": pick(f"SELECT src, count(*) FROM edge_v WHERE rel='GUARANTEE' AND src LIKE 'Person:%' AND {W} "
                              f"GROUP BY src"),   # TCR11 seeds a Person (companies also guarantee)
            "investor": pick(f"SELECT src, count(*) FROM edge_v WHERE rel='INVEST' AND src LIKE 'Person:%' AND {W} "
                             f"GROUP BY src"),
            "pair": [r for r in con.execute(f"SELECT src, dst FROM edge_v WHERE rel='TRANSFER' AND {W} AND src <> dst "
                                            f"ORDER BY md5(src || dst) LIMIT {per}").fetchall()],
            "cycle": [r for r in con.execute(
                f"WITH t AS (SELECT src, dst FROM edge_v WHERE rel='TRANSFER' AND {W}) SELECT DISTINCT a.src, a.dst "
                f"FROM t a JOIN t b ON b.src = a.dst JOIN t c ON c.src = b.dst AND c.dst = a.src "
                f"WHERE a.src <> a.dst ORDER BY md5(a.src || a.dst) LIMIT {per}").fetchall()],
        }
    ins = []
    for wn, (a, b) in wins.items():
        S = seeds[wn]
        for T, tn in ((Q.CURRENT, "current"), (T_hist, "historic")):
            base = {"start": a, "end": b, "now": now, "T": T, "lim": 500, "threshold": 0.0,
                    "threshold1": 0.0, "threshold2": 0.0}

            def add(q, stratum, **kw):
                p = dict(base, **kw)
                ins.append({"query": q, "instance": h([q, p])[:8], "window": wn, "belief": tn,
                            "stratum": stratum, "params": p})
            for i, (u, c) in enumerate(S["account"]):
                st = f"activity rank {i} ({c} transfers)"
                for q in ("TSR1", "TSR2", "TSR3", "TSR4", "TSR5", "TSR6", "TCR1", "TCR7", "TCR9"):
                    add(q, st, id=u)
            for i, (u, c) in enumerate(S["card"]):
                add("TCR6", f"withdraw-in rank {i} ({c})", id=u)
            for i, (u, c) in enumerate(S["person"]):
                for q in ("TCR2", "TCR5", "TCR12"):
                    add(q, f"owner rank {i} ({c})", id=u)
            for i, (u, c) in enumerate(S["loan"]):
                add("TCR8", f"deposit rank {i} ({c})", id=u, threshold=0.5)
            for i, (u, c) in enumerate(S["guarantor"]):
                add("TCR11", f"guarantor rank {i} ({c})", id=u)
            inv = S["investor"]
            for i in range(len(inv) - 1):
                add("TCR10", f"investor ranks {i},{i + 1}", pid1=inv[i][0], pid2=inv[i + 1][0])
            for i, (x, y) in enumerate(S["pair"]):
                add("TCR3", f"transfer pair {i}", id1=x, id2=y)
            for i, (x, y) in enumerate(S["cycle"]):
                add("TCR4", f"cycle pair {i}", id1=x, id2=y)
    doc = {"format": "w4-params", "version": "w4-v1", "meta": str(meta), "T_historic": T_hist, "now": now,
           "windows": wins, "counts": {}, "instances": ins}
    for i in ins:
        doc["counts"][i["query"]] = doc["counts"].get(i["query"], 0) + 1
    out.write_text(json.dumps(doc, indent=1))
    print(json.dumps(doc["counts"]), len(ins))


# ------------------------------------------------------------------ load ----

def _cypher_driver(system: str):
    from neo4j import GraphDatabase
    uri = os.environ["GNOMON_NEO4J_URI" if system == "neo4j" else "GNOMON_MEMGRAPH_URI"]
    for _ in range(120):
        try:
            d = GraphDatabase.driver(uri, auth=None)
            with d.session() as s:
                s.run("RETURN 1").consume()
            return d
        except Exception:  # noqa: BLE001
            time.sleep(2)
    raise SystemExit(f"{system}: no connection at {uri}")


def _pg():
    import psycopg
    for _ in range(120):
        try:
            return psycopg.connect(host=os.environ.get("PGHOST", "127.0.0.1"), port=int(os.environ.get("PGPORT", 15432)),
                                   user=os.environ.get("PGUSER", "postgres"), dbname="postgres", autocommit=True)
        except Exception:  # noqa: BLE001
            time.sleep(2)
    raise SystemExit("postgres: no connection")


def load(system: str, d: Path, db: Path | None) -> None:
    import duckdb
    t0 = time.time()
    rec: dict[str, Any] = {"system": system}
    if system == "duckdbsql":
        con = duckdb.connect(str(db))
        con.execute(f"CREATE TABLE node_v AS SELECT * FROM '{d}/node_v.parquet'")
        con.execute(f"CREATE TABLE edge_v AS SELECT * FROM '{d}/edge_v.parquet'")
        for s in SQL_INDEXES:
            con.execute(s)
        con.execute("CHECKPOINT")
        con.close()
    elif system == "postgres":
        src = duckdb.connect()
        pg = _pg()
        pg.execute("DROP TABLE IF EXISTS node_v; DROP TABLE IF EXISTS edge_v")
        pg.execute(DDL_NODE.replace("VARCHAR", "TEXT").replace("DOUBLE", "DOUBLE PRECISION"))
        pg.execute(DDL_EDGE.replace("VARCHAR", "TEXT").replace("DOUBLE", "DOUBLE PRECISION"))
        for t in ("node_v", "edge_v"):
            cur = src.execute(f"SELECT * FROM '{d}/{t}.parquet'")
            with pg.cursor() as c, c.copy(f"COPY {t} FROM STDIN") as cp:
                while rows := cur.fetchmany(100_000):
                    for r in rows:
                        cp.write_row(r)
        for s in SQL_INDEXES:
            pg.execute(s)
        pg.execute("ANALYZE")
    else:
        src = duckdb.connect()
        drv = _cypher_driver(system)
        idx = (["CREATE INDEX IF NOT EXISTS FOR (n:{L}) ON (n.uid)"] if system == "neo4j"
               else ["CREATE INDEX ON :{L}(uid)"])
        with drv.session() as s:
            s.run("MATCH (n) DETACH DELETE n").consume()
            for L in ("Account", "Person", "Company", "Loan", "Medium", "AState"):
                for st in idx:
                    s.run(st.replace("{L}", L)).consume()
            ents = src.execute(f"""SELECT uid, any_value(label), max(loanAmount), max(balance), bool_or(isBlocked),
                     any_value(mediumType) FROM '{d}/node_v.parquet' GROUP BY uid""").fetchall()
            by: dict[str, list] = {}
            for u, lab, la, bal, blk, mt in ents:
                by.setdefault(lab, []).append({"uid": u, "la": la, "bal": bal, "blk": blk, "mt": mt})
            for lab, rows in by.items():
                for i in range(0, len(rows), 20_000):
                    s.run(f"UNWIND $rows AS r CREATE (:{lab} {{uid: r.uid, loanAmount: r.la, balance: r.bal, "
                          f"isBlocked: r.blk, mediumType: r.mt}})", rows=rows[i:i + 20_000]).consume()
            av = src.execute(f"""SELECT uid, vt_s, vt_e, tt_s, tt_e, isBlocked, accountType, createTime
                   FROM '{d}/node_v.parquet' WHERE label = 'Account'""").fetchall()
            for i in range(0, len(av), 20_000):
                s.run("UNWIND $rows AS r MATCH (a:Account {uid: r[0]}) CREATE (:AState {vt_s: r[1], vt_e: r[2], "
                      "tt_s: r[3], tt_e: r[4], isBlocked: r[5], accountType: r[6], createTime: r[7]})-[:OF]->(a)",
                      rows=[list(x) for x in av[i:i + 20_000]]).consume()
            lab_of = {r[0]: r[1] for r in ents}
            cur = src.execute(f"SELECT * FROM '{d}/edge_v.parquet' ORDER BY rel, src")
            n = 0
            while rows := cur.fetchmany(20_000):
                groups: dict[tuple, list] = {}
                for r in rows:
                    groups.setdefault((r[2], lab_of.get(r[0], "Account"), lab_of.get(r[1], "Account")), []).append(list(r))
                for (rel, sl, dl), rs in groups.items():
                    s.run(f"UNWIND $rows AS r MATCH (a:{sl} {{uid: r[0]}}), (b:{dl} {{uid: r[1]}}) "
                          f"CREATE (a)-[:{rel} {{disc: r[3], vt_s: r[4], vt_e: r[5], tt_s: r[6], tt_e: r[7], "
                          f"amount: r[8]}}]->(b)", rows=rs).consume()
                    n += len(rs)
            rec["edges"] = n
        drv.close()
    rec["load_s"] = round(time.time() - t0, 1)
    print(json.dumps(rec))


# ------------------------------------------------- verify and digest (stores) ----
# version rows and an order-independent digest. SQL stores hold node_v / edge_v as exported (DuckDB (SQL) and
# PostgreSQL digests must be equal); the Cypher stores hold one node per entity, one AState node per account
# version (-[:OF]-> its account) and one relationship per edge version (Neo4j and Memgraph digests must be equal).

OPEN_V = 2 ** 62 - 2      # vt_e / tt_e at or above it: open


def _expected(d: Path) -> dict:
    """From the export: per part, [count, sum(vt_s // 1e6), sum(tt_s // 1e6), closed sum(vt_e // 1e6),
    closed sum(tt_e // 1e6), open vt_e, open tt_e] (whole seconds, so no Cypher sum overflows 64 bits)."""
    import duckdb
    con = duckdb.connect()
    agg = (f"count(*), sum(vt_s // 1000000), sum(tt_s // 1000000), "
           f"sum(CASE WHEN vt_e >= {OPEN_V} THEN 0 ELSE vt_e // 1000000 END), "
           f"sum(CASE WHEN tt_e >= {OPEN_V} THEN 0 ELSE tt_e // 1000000 END), "
           f"sum(CASE WHEN vt_e >= {OPEN_V} THEN 1 ELSE 0 END), sum(CASE WHEN tt_e >= {OPEN_V} THEN 1 ELSE 0 END)")
    out = {}
    for rel, *v in con.execute(f"SELECT rel, {agg} FROM '{d}/edge_v.parquet' GROUP BY rel ORDER BY rel").fetchall():
        out["edge:" + rel] = [int(x or 0) for x in v]
    for lab, *v in con.execute(f"SELECT label, {agg} FROM '{d}/node_v.parquet' GROUP BY label ORDER BY label").fetchall():
        out["node_v:" + lab] = [int(x or 0) for x in v]
    for lab, n in con.execute(f"SELECT label, count(DISTINCT uid) FROM '{d}/node_v.parquet' GROUP BY label ORDER BY label").fetchall():
        out["entity:" + lab] = [int(n)]
    return out


def verify(system: str, d: Path, db: Path | None, out: Path) -> int:
    t0 = time.time()
    exp = _expected(d)
    got: dict[str, list] = {}
    sec = lambda c: (f"count(*), sum({c}vt_s / 1000000), sum({c}tt_s / 1000000), "  # noqa: E731
                     f"sum(CASE WHEN {c}vt_e >= {OPEN_V} THEN 0 ELSE {c}vt_e / 1000000 END), "
                     f"sum(CASE WHEN {c}tt_e >= {OPEN_V} THEN 0 ELSE {c}tt_e / 1000000 END), "
                     f"sum(CASE WHEN {c}vt_e >= {OPEN_V} THEN 1 ELSE 0 END), sum(CASE WHEN {c}tt_e >= {OPEN_V} THEN 1 ELSE 0 END)")
    if system in ("duckdbsql", "postgres"):
        if system == "duckdbsql":
            import duckdb
            con = duckdb.connect(str(db), read_only=True)
            q = lambda sql: con.execute(sql).fetchall()  # noqa: E731
            idiv = lambda x: x.replace(" / 1000000", " // 1000000")  # noqa: E731
        else:
            pg = _pg()
            q = lambda sql: pg.execute(sql).fetchall()  # noqa: E731
            idiv = lambda x: x                                           # bigint / bigint is integer division
        for rel, *v in q(idiv(f"SELECT rel, {sec('')} FROM edge_v GROUP BY rel ORDER BY rel")):
            got["edge:" + rel] = [int(x or 0) for x in v]
        for lab, *v in q(idiv(f"SELECT label, {sec('')} FROM node_v GROUP BY label ORDER BY label")):
            got["node_v:" + lab] = [int(x or 0) for x in v]
        for lab, n in q("SELECT label, count(DISTINCT uid) FROM node_v GROUP BY label ORDER BY label"):
            got["entity:" + lab] = [int(n)]
    else:
        drv = _cypher_driver(system)
        csec = lambda v: sec(v + ".").replace("count(*)", f"count({v})")  # noqa: E731
        with drv.session() as s:
            for k in exp:
                kind, name = k.split(":", 1)
                if kind == "edge":
                    r = s.run(f"MATCH ()-[x:{name}]->() RETURN {csec('x')}").single()
                elif kind == "entity":
                    r = s.run(f"MATCH (n:{name}) RETURN count(n)").single()
                elif name == "Account":     # the account versions are the AState nodes
                    r = s.run(f"MATCH (x:AState)-[:OF]->(:Account) RETURN {csec('x')}").single()
                else:                       # other entities keep no version rows in the Cypher model
                    continue
                got[k] = [int(x or 0) for x in r.values()]
        drv.close()
    rows = {k: {"store": got.get(k), "export": v} for k, v in exp.items() if not (system in ("neo4j", "memgraph")
            and k.startswith("node_v:") and k != "node_v:Account")}
    extra = sorted(set(got) - set(exp))
    ok = all(r["store"] == r["export"] for r in rows.values()) and not extra
    rec = {"system": system, "export": str(d), "verified": ok, "parts": rows, "extra_parts": extra,
           "verify_s": round(time.time() - t0, 1)}
    out.write_text(json.dumps(rec, indent=1) + "\n")
    print(json.dumps({"system": system, "verified": ok, "parts": len(rows), "bad": [k for k, r in rows.items() if r["store"] != r["export"]][:10],
                      "extra": extra}))
    return 0 if ok else 1


def _hv(*xs) -> int:
    return int(hashlib.sha256(repr(xs).encode()).hexdigest()[:16], 16)


def _digest_part(args) -> dict:
    system, db, K, k = args
    M = 1 << 64
    acc: dict = {}

    def add(key, h_):
        c = acc.setdefault(key, [0, 0]); c[0] += 1; c[1] = (c[1] + h_) % M
    if system in ("duckdbsql", "postgres"):
        if system == "duckdbsql":
            import duckdb
            con = duckdb.connect(str(db), read_only=True)
            cur = lambda sql: con.execute(sql)  # noqa: E731
        else:
            pg = _pg()
            cur = lambda sql: pg.execute(sql)  # noqa: E731
        for t, cols in (("node_v", NODE_COLS), ("edge_v", EDGE_COLS)):
            c = cur(f"SELECT {', '.join(cols)} FROM {t}")
            while rows := c.fetchmany(100_000):
                for r in rows:
                    add(t, _hv(*r))
        return acc
    drv = _cypher_driver(system)
    f = (lambda v: f" WHERE id({v}) % {K} = {k}") if K > 1 else (lambda v: "")
    with drv.session() as s:
        for r in s.run(f"MATCH (n){f('n')} WHERE NOT n:AState RETURN labels(n) AS l, properties(n) AS p"
                       if K <= 1 else f"MATCH (n){f('n')} AND NOT n:AState RETURN labels(n) AS l, properties(n) AS p"):
            lab = ":".join(sorted(r["l"]))
            add("entity:" + lab, _hv(lab, sorted(r["p"].items())))
        for r in s.run(f"MATCH (x:AState)-[:OF]->(a){f('x')} RETURN a.uid AS u, properties(x) AS p"):
            add("AState", _hv(r["u"], sorted(r["p"].items())))
        for r in s.run(f"MATCH (a)-[x]->(b){f('a')} RETURN type(x) AS t, a.uid AS a, b.uid AS b, properties(x) AS p"):
            if r["t"] == "OF":
                continue
            add("edge:" + r["t"], _hv(r["t"], r["a"], r["b"], sorted(r["p"].items())))
    drv.close()
    return acc


def digest(system: str, db: Path | None, out: Path, parts: int, workers: int) -> int:
    """Order-independent: per part, the count and the sum mod 2^64 of a hash of every row (SQL: node_v, edge_v
    columns in the export's order) or element (Cypher: entity labels and properties; AState properties and its
    account's uid; relationship type, endpoint uids, properties). Cypher part k of K: internal id % K = k of the
    node (the source for relationships); every element is read once, so the result is that of one pass."""
    t0 = time.time()
    M = 1 << 64
    K = 1 if system in ("duckdbsql", "postgres") else max(1, parts)
    jobs = [(system, db, K, k) for k in range(K)]
    W = max(1, min(workers or K, K))
    if W == 1:
        res = [_digest_part(j) for j in jobs]
    else:
        import multiprocessing as mp
        with mp.get_context("spawn").Pool(W) as pool:
            res = pool.map(_digest_part, jobs, chunksize=1)
    acc: dict = {}
    for part in res:
        for key, (n, hh) in part.items():
            c = acc.setdefault(key, [0, 0]); c[0] += n; c[1] = (c[1] + hh) % M
    out.write_text(json.dumps(acc, indent=1, sort_keys=True) + "\n")
    print(json.dumps({"system": system, "parts": len(acc), "rows": sum(v[0] for v in acc.values()), "k": K, "workers": W,
                      "digest_s": round(time.time() - t0, 1)}))
    return 0


def snapshot(system: str) -> None:
    """Memgraph: a snapshot of the loaded store before it is stopped and packed (its WAL alone replays slowly)."""
    if system != "memgraph":
        return
    drv = _cypher_driver(system)
    t0 = time.time()
    with drv.session() as s:
        s.run("CREATE SNAPSHOT").consume()
    drv.close()
    print(json.dumps({"system": system, "snapshot_s": round(time.time() - t0, 1)}))


# ------------------------------------------------------------------- run ----

class Alarm(Exception):
    pass


#: a server's own cap message (PostgreSQL statement_timeout, DuckDB interrupt, Neo4j transaction timeout, Memgraph
#: query execution timeout): the cell is "timeout", as a client alarm would record it
import re  # noqa: E402
TIMEOUT_RE = re.compile(r"statement timeout|QueryCanceled|INTERRUPT|Interrupted|TransactionTimedOut|"
                        r"transaction has been terminated|asked to abort|execution timeout|transaction timeout", re.I)


def executor(system: str, store: Path | None, db: Path | None, alarm: int, state: dict[str, Any],
             wrap_con=None):
    """    the DuckDB connection by a profiling proxy."""
    if system == "gnomon":
        import gnomon
        from gnomon.temporal.algebra import call_operator
        from gnomon.temporal.ops_match import match
        from gnomon.temporal.algebra import ensure_all_registered
        ensure_all_registered()   # the engine-level operators (paths on the preview)
        st = gnomon.open(store)
        state["store"] = st      # kept open as long as the caller holds state
        a = st.adapter

        from gnomon.temporal import ops_match as _OM
        match_page = int(getattr(_OM, "MAX_LIMIT", 10_000))

        def every_page(op, args):
            # every page of the call: the W4 executor took one page, so an
            # operator answer above the page was cut.
            # match: one page of the loaded engine's own maximum (as the W3 driver); other operators: 10,000.
            args["limit"] = match_page if op == "match" else 10_000
            args.pop("cursor", None)
            call = (lambda x: match(a, x)) if op == "match" else (lambda x: call_operator(a, op, x))
            env = call(args)
            rows = list(env.get("rows", []))
            while env.get("truncated") and env.get("cursor"):
                args["cursor"] = env["cursor"]
                env = call(args)
                rows.extend(env.get("rows", []))
            return {**env, "rows": rows, "truncated": False, "cursor": None}

        def ex(q, p):
            pays = [every_page(op, json.loads(json.dumps(args))) for op, args in Q.gnomon(q, p)]
            return Q.gnomon_rows(q, pays)
    elif system in ("duckdbsql", "postgres"):
        if system == "duckdbsql":
            import duckdb
            import threading
            con = duckdb.connect(str(db), read_only=True)
            if os.environ.get("W4_DUCKDB_THREADS"):
                con.execute(f"SET threads={int(os.environ['W4_DUCKDB_THREADS'])}")
            if wrap_con is not None:
                con = wrap_con(con)

            def run_sql(s):
                timer = threading.Timer(PC.cap(alarm), con.interrupt)
                timer.start()
                try:
                    return con.execute(s).fetchall()
                finally:
                    timer.cancel()
        else:
            def connect():
                state["pg"] = _pg()
                state["pg"].execute(f"SET statement_timeout = '{alarm}s'")
            connect()
            state["reconnect"] = connect
            PC.hook(lambda sec: state["pg"].execute(f"SET statement_timeout = '{int(sec) if sec else alarm}s'"))
            run_sql = lambda s: state["pg"].execute(s).fetchall()  # noqa: E731

        def ex(q, p):
            return Q.sql_rows(q, run_sql(Q.sql(q, p)))
    elif system == "ladybug":                     # in-process
        import w4_ladybug as WL
        ex = WL.executor(db, alarm, state)
    else:
        state["drv"] = _cypher_driver(system)

        def reconnect():
            try:
                state["drv"].close()
            except Exception:  # noqa: BLE001
                pass
            state["drv"] = _cypher_driver(system)
        state["reconnect"] = reconnect
        if system == "memgraph":
            PC.memgraph_hook(state["drv"])

        def ex(q, p):
            if q in Q.CYPHER_GAPS:
                raise NotImplementedError("no Cypher text (CYPHER_GAPS)")
            text, prm = Q.cypher(q, p, system)
            with state["drv"].session() as s:
                if system == "neo4j" and PC.active():     # the prime's transaction timeout (prime_cap)
                    from neo4j import Query
                    text = Query(text, timeout=PC.active())
                return [list(r.values()) for r in s.run(text, prm)]
    return ex


def run(system: str, pfile: Path, store: Path | None, db: Path | None, out: Path, queries: set[str],
        alarm: int, label: str) -> None:
    doc = json.loads(pfile.read_text())
    ins = [i for i in doc["instances"] if not queries or i["query"] in queries]
    signal.signal(signal.SIGALRM, lambda *_: (_ for _ in ()).throw(Alarm()))
    # The cap is enforced by the server (PostgreSQL statement_timeout, a DuckDB interrupt, the Neo4j and Memgraph
    # transaction / query timeout the kit's server start sets); the client alarm is a backstop BACKSTOP_S above it.
    # A client alarm alone does not stop the statement on the server, and the connection it ran on is not usable
    # for the next call until it ends.
    backstop = alarm + int(os.environ.get("W4_ALARM_BACKSTOP_S", "30"))
    state: dict[str, Any] = {}
    ex = executor(system, store, db, alarm, state)
    with open(out, "w") as f:
        for i in ins:
            q, p = i["query"], i["params"]
            t0 = time.perf_counter()
            rec = {"system": system, "label": label, "query": q, "instance": i["instance"],
                   "stratum": i["stratum"], "window": i["window"], "belief": i["belief"]}
            try:
                signal.alarm(backstop if system != "gnomon" else alarm)
                rows = Q.canonical(q, ex(q, p))
                signal.alarm(0)
                rec.update(status="ok", hash=h(rows), n=len(rows), head=rows[:3])
            except Alarm:
                rec.update(status="timeout", alarm_s=alarm, note="client backstop alarm")
                if "reconnect" in state:
                    state["reconnect"]()
            except NotImplementedError as e:
                signal.alarm(0)
                rec.update(status="not expressed", error=str(e))
            except Exception as e:  # noqa: BLE001
                signal.alarm(0)
                msg = f"{type(e).__name__}: {str(e)[:300]}"
                if TIMEOUT_RE.search(msg):
                    rec.update(status="timeout", alarm_s=alarm, error=msg)
                else:
                    rec.update(status="error", error=msg)
            rec["ms"] = round((time.perf_counter() - t0) * 1e3, 2)
            f.write(json.dumps(rec, default=str) + "\n")
            f.flush()


def compare(base: Path, others: list[Path], out: Path) -> None:
    B = {(r["query"], r["instance"]): r for r in map(json.loads, open(base))}
    res: dict[str, Any] = {}
    for o in others:
        recs = list(map(json.loads, open(o)))
        if not recs:
            continue
        sysname = recs[0]["system"] + (f" [{recs[0]['label']}]" if recs[0].get("label") else "")
        per: dict[str, dict[str, int]] = {}
        diffs = []
        for r in recs:
            b = B.get((r["query"], r["instance"]))
            if b is None:
                continue
            if b["status"] != "ok":
                v = "no reference (" + b["status"] + ")"
            elif r["status"] != "ok":
                v = r["status"]
            else:
                v = "equivalent" if r["hash"] == b["hash"] else "DIFFERS"
            per.setdefault(r["query"], {}).setdefault(v, 0)
            per[r["query"]][v] += 1
            if v == "DIFFERS":
                diffs.append({"query": r["query"], "instance": r["instance"], "n": r.get("n"), "base_n": b.get("n"),
                              "head": r.get("head"), "base_head": b.get("head")})
        res[sysname] = {"per_query": per, "differs": diffs[:40]}
    out.write_text(json.dumps(res, indent=1, default=str))
    for s, v in res.items():
        print(s, json.dumps(v["per_query"]))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    e = sub.add_parser("export"); e.add_argument("--log", type=Path, required=True)
    e.add_argument("--store", type=Path, required=True); e.add_argument("--out", type=Path, required=True)
    p = sub.add_parser("params"); p.add_argument("--dir", type=Path, required=True)
    p.add_argument("--log-meta", type=Path, required=True); p.add_argument("--out", type=Path, required=True)
    p.add_argument("--per", type=int, default=5)
    lo = sub.add_parser("load"); lo.add_argument("--system", required=True); lo.add_argument("--dir", type=Path, required=True)
    lo.add_argument("--db", type=Path)
    r = sub.add_parser("run"); r.add_argument("--system", required=True); r.add_argument("--params", type=Path, required=True)
    r.add_argument("--store", type=Path); r.add_argument("--db", type=Path); r.add_argument("--out", type=Path, required=True)
    r.add_argument("--queries", default=""); r.add_argument("--alarm", type=int, default=60)
    r.add_argument("--label", default="")
    v = sub.add_parser("verify"); v.add_argument("--system", required=True); v.add_argument("--dir", type=Path, required=True)
    v.add_argument("--db", type=Path); v.add_argument("--out", type=Path, required=True)
    g = sub.add_parser("digest"); g.add_argument("--system", required=True); g.add_argument("--db", type=Path)
    g.add_argument("--out", type=Path, required=True); g.add_argument("--parts", type=int, default=1)
    g.add_argument("--workers", type=int, default=0)
    sn = sub.add_parser("snapshot"); sn.add_argument("--system", required=True)
    c = sub.add_parser("compare"); c.add_argument("--base", type=Path, required=True)
    c.add_argument("others", type=Path, nargs="+"); c.add_argument("--json", type=Path, required=True)
    a = ap.parse_args()
    if a.cmd == "export":
        export(a.log, a.store, a.out)
    elif a.cmd == "params":
        params(a.dir, a.log_meta, a.out, a.per)
    elif a.cmd == "load":
        load(a.system, a.dir, a.db)
    elif a.cmd == "run":
        run(a.system, a.params, a.store, a.db, a.out, set(filter(None, a.queries.split(","))), a.alarm, a.label)
    elif a.cmd == "verify":
        return verify(a.system, a.dir, a.db, a.out)
    elif a.cmd == "digest":
        return digest(a.system, a.db, a.out, a.parts, a.workers)
    elif a.cmd == "snapshot":
        snapshot(a.system)
    else:
        compare(a.base, a.others, a.json)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
