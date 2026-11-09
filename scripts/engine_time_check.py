#!/usr/bin/env python3
"""
    engine_time_check.py <system> --out FILE.jsonl [--mode on|off] [--reps 7]
        system: duckdbsql | postgres | neo4j | memgraph | aeong | clickhouse
        (Gnomon and the SQL systems are also checked on the W1 registry through extcmp.py);
        the servers are started by the job.

Per query (a light and a heavy one, and one with many result rows): a prime
call, 2 warm-ups and --reps timed calls; each timed call records e2e_ms and,
with --mode on (EXTCMP_ENGINE_TIME=1 set by the caller), the engine-time record.
--mode off runs the same calls with no instrumentation (the overhead control:
the job alternates off/on processes). One JSON line per query.
"""
from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import engine_time as ET  # noqa: E402

ap = argparse.ArgumentParser()
ap.add_argument("system")
ap.add_argument("--out", required=True)
ap.add_argument("--mode", default="on")
ap.add_argument("--reps", type=int, default=7)
ap.add_argument("--setup", action="store_true", help="create the synthetic data and exit")
a = ap.parse_args()
assert (a.mode == "on") == ET.ENABLED, "EXTCMP_ENGINE_TIME must be 1 exactly for --mode on"


def run(name: str, fn, pr: ET.Probe, note: str = "") -> None:
    fn()
    for _ in range(2):
        fn()
    e2e, recs = [], []
    for _ in range(a.reps):
        pr.begin()
        t = time.perf_counter()
        r = fn()
        d = (time.perf_counter() - t) * 1e3
        recs.append(pr.end(d))
        e2e.append(d)
    rows = len(r) if hasattr(r, "__len__") else None
    out = {"system": a.system, "mode": a.mode, "query": name, "note": note, "rows": rows,
           "e2e_ms": [round(x, 4) for x in e2e], "e2e_p50": round(statistics.median(e2e), 4),
           "engine": recs, "summary": ET.summarize(recs)}
    bad = [i for i, r in enumerate(recs) if r and r.get("engine_ms") is not None and r["engine_ms"] > e2e[i]]
    out["engine_gt_e2e"] = bad
    with open(a.out, "a") as f:
        f.write(json.dumps(out, default=str) + "\n")
    s = out["summary"] or {}
    print(f"{a.system:<10} {a.mode:<3} {name:<14} rows={rows} e2e={out['e2e_p50']:.3f} "
          f"engine={s.get('engine_ms_p50')} front={s.get('front_ms_p50')} exec={s.get('exec_ms_p50')} "
          f"delivery={s.get('delivery_ms_p50')} engine>e2e={bad}", flush=True)


S = a.system
if S == "duckdbsql":
    import duckdb
    con = duckdb.connect()
    con.execute("CREATE TABLE t AS SELECT range AS id, range % 1000 AS g, hash(range) % 100000 AS v "
                "FROM range(20000000)")
    pr = ET.probe("duckdbsql")
    h = pr.wrap(con) if a.mode == "on" else con
    run("point", lambda: h.execute("SELECT * FROM t WHERE id = 123456").fetchall(), pr)
    run("agg-heavy", lambda: h.execute("SELECT g, count(*), avg(v) FROM t GROUP BY g ORDER BY g").fetchall(), pr)
    run("join-heavy", lambda: h.execute("SELECT count(*) FROM t a JOIN t b ON a.v = b.id "
                                        "WHERE a.g < 50").fetchall(), pr)
    run("rows-100k", lambda: h.execute("SELECT * FROM t WHERE g = 7 LIMIT 100000").fetchall(), pr)
    run("two-stmts", lambda: (h.execute("SELECT id FROM t WHERE id = 5").fetchall(),
                              h.execute("SELECT count(*) FROM t WHERE g = 3").fetchall()), pr)
elif S == "postgres":
    import psycopg
    dsn = os.environ["EXTCMP_PG_DSN"]
    if a.setup:
        c = psycopg.connect(dsn, autocommit=True)
        c.execute("DROP TABLE IF EXISTS t")
        c.execute("CREATE TABLE t AS SELECT g AS id, g % 1000 AS grp, (g::bigint * 7919) % 100000 AS v "
                  "FROM generate_series(1, 5000000) g")
        c.execute("CREATE INDEX ON t (id)")
        c.execute("ANALYZE t")
        sys.exit(0)
    pr = ET.probe("postgres")
    c = psycopg.connect(dsn, autocommit=True)
    run("point", lambda: c.execute("SELECT * FROM t WHERE id = %s", (123456,)).fetchall(), pr)
    run("agg-heavy", lambda: c.execute("SELECT grp, count(*), avg(v) FROM t GROUP BY grp ORDER BY grp").fetchall(), pr)
    run("join-heavy", lambda: c.execute("SELECT count(*) FROM t a JOIN t b ON a.v = b.id WHERE a.grp < 100").fetchall(), pr)
    run("rows-100k", lambda: c.execute("SELECT * FROM t WHERE id <= 100000").fetchall(), pr)
    run("two-stmts", lambda: (c.execute("SELECT 1").fetchall(),
                              c.execute("SELECT count(*) FROM t WHERE grp = 3").fetchall()), pr)
elif S in ("neo4j", "memgraph", "aeong"):
    import neo4j
    port = {"neo4j": "EXTCMP_NEO4J_PORT", "memgraph": "EXTCMP_MG_PORT", "aeong": "EXTCMP_AEONG_PORT"}[S]
    drv = neo4j.GraphDatabase.driver(f"bolt://127.0.0.1:{os.environ[port]}",
                                     auth=("neo4j", "neo4j") if S == "neo4j" else None)
    if a.setup:
        with drv.session() as s:
            s.run("MATCH (n) DETACH DELETE n").consume()
            for b in range(20):
                s.run("UNWIND range($a, $b) AS i CREATE (:N {id: i, g: i % 100})", a=b * 10000, b=b * 10000 + 9999).consume()
            if S == "neo4j":
                s.run("CREATE INDEX n_id IF NOT EXISTS FOR (n:N) ON (n.id)").consume()
                s.run("CALL db.awaitIndexes(600)").consume()
            else:
                s.run("CREATE INDEX ON :N(id)").consume()
            # edges by a bulk UNWIND over the id index (an index lookup per row, never a nested loop)
            for b in range(10):
                s.run("UNWIND range($a, $b) AS i MATCH (x:N {id: i}), (y:N {id: (i * 7) % 200000}) CREATE (x)-[:R]->(y)",
                      a=b * 10000, b=b * 10000 + 9999).consume()
        sys.exit(0)
    pr = ET.probe(S)
    sess = drv.session()
    run("point", lambda: sess.run("MATCH (n:N {id: $i}) RETURN n.id, n.g", i=123456).data(), pr)
    run("agg-heavy", lambda: sess.run("MATCH (n:N) RETURN n.g AS g, count(*) AS c ORDER BY g").data(), pr)
    run("expand-heavy", lambda: sess.run("MATCH (a:N)-[:R]->(b:N)-[:R]->(c:N) RETURN count(*) AS c").data(), pr)
    run("rows-50k", lambda: sess.run("MATCH (n:N) WHERE n.id < 50000 RETURN n.id, n.g").data(), pr)
    run("two-stmts", lambda: (sess.run("RETURN 1 AS x").data(),
                              sess.run("MATCH (n:N) WHERE n.g = 3 RETURN count(*) AS c").data()), pr)
elif S == "clickhouse":
    import clickhouse_connect
    cl = clickhouse_connect.get_client(host="127.0.0.1", port=int(os.environ["EXTCMP_CH_HTTP_PORT"]))
    pr = ET.probe("clickhouse", cl)
    run("point", lambda: cl.query("SELECT number FROM numbers(100000000) WHERE number = 123456").result_rows, pr)
    run("agg-heavy", lambda: cl.query("SELECT number % 1000 AS g, count(), avg(number) FROM numbers(300000000) "
                                      "GROUP BY g ORDER BY g").result_rows, pr)
    run("rows-100k", lambda: cl.query("SELECT number, number * 2 FROM numbers(100000)").result_rows, pr)
    run("two-stmts", lambda: (cl.query("SELECT 1").result_rows,
                              cl.query("SELECT count() FROM numbers(100000000) WHERE number % 7 = 3").result_rows), pr)
print("done", S, a.mode)
