"""DuckDB (SQL) baseline: every benchmark query as SQL inside DuckDB (system id `duckdbsql`).

Not to be confused with the kit system `duckdb`, which is Gnomon's own Python
operators over the DuckDB *storage backend* (`gnomon/storage/duckdb_adapter.py`:
DuckDB serves filtered scans, the traversal and aggregation run in our Python).
This system is a baseline in the sense of `pg_baseline` / `ch_baseline`:
transaction times and every id derived from them arrive byte-for-byte) into the
same four-timestamp version tables the PostgreSQL and ClickHouse baselines use,
and each registry query is one SQL statement (two for nothing; see below)
executed by DuckDB in-process. Python only binds the call's arguments into the
text and shapes DuckDB's result rows into the operator payload (JSON-parsing
`props`, building dicts); it never filters, joins, sorts or aggregates — with
one exception inherited from PostgreSQL: `burst.zscore`'s final score rounding
(see `burst_detection`).

Physical design (what a competent DuckDB user would do; recorded per load):

* `edge_versions` is inserted in `(vt_s, vid)` order. Almost every query is a
  valid-time window or instant on edges, and DuckDB's per-row-group zone maps
  (min/max) then prune by `vt_s` — and by `vt_e` too, because `vt_e - vt_s` is
  bounded by the edge lifetime, so `vt_e` is near-sorted in the same order.
* `node_versions` is inserted in `(uid, vt_s, vid)` order (ClickHouse's
  `ORDER BY`): entity history is a prefix read.
* ART indexes only where DuckDB's optimizer turns an equality predicate into
  an index scan and the predicate is selective: `node_versions(uid)` (the
  entity point reads `hist.single`, `hist.asof`) and `edge_versions(eid)` (the
  belief-history point reads `b.hist`, `b.hist@now`), `edge_versions(src)`
  (the per-source edge sets of `coactive.narrow`), plus the `entities`
  primary key / unique key (uid <-> dense id). No ART for range or OR
  predicates (DuckDB does not use an ART for them; zone maps do that work),
  no primary key on `vid` (no query looks a version up by vid, and the PK's
  ART would only slow the bulk load). The load records the plan of each
  point read so the index use is checked, not assumed.
* `props` is VARCHAR holding the canonical JSON text, as in PostgreSQL (TEXT)
  and ClickHouse (String): the bytes the store digest covers.

Strings compare bytewise in DuckDB (default collation binary), which is the
operators' code-point order for UTF-8, so PostgreSQL's `COLLATE "C"` has no
counterpart here. Integer arguments are inlined into the SQL text (as
`pg_queries.belief` does) so the optimizer sees constants and can push them
into the zone-map filters; string arguments are inlined as escaped literals.

Settings (`settings_report`, recorded in every query record and load record):
`threads` = the CPUs this process may run on (`os.sched_getaffinity`, i.e. the
NUMA node-0 cores under the kit's client pinning; DuckDB's own default is
recorded beside it) unless EXTCMP_DUCKDBSQL_THREADS sets it, `memory_limit` (EXTCMP_DUCKDBSQL_MEMORY_LIMIT, default
32GB = ClickHouse's max_server_memory_usage in the kit), `temp_directory` under
the kit's TMPDIR.
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any

import eval_harness as H

from gnomon.core.model import OPEN_END, canonical_json

W10_ALIAS = {"series.count.w10": "series.count",
             "agg.rel_bucket.w10": "agg.rel_bucket",
             "burst.zscore.w10": "burst.zscore"}

MEMORY_LIMIT = os.environ.get("EXTCMP_DUCKDBSQL_MEMORY_LIMIT", "32GB")
LOAD_ORDER = {"edge_versions": "vt_s, vid", "node_versions": "uid, vt_s, vid",
              "entities": "dense_id"}

SCHEMA = """
CREATE TABLE entities (
    dense_id BIGINT PRIMARY KEY,
    uid      VARCHAR NOT NULL UNIQUE,
    label    VARCHAR
);
CREATE TABLE node_versions (
    vid            VARCHAR NOT NULL,
    uid            VARCHAR NOT NULL,
    label          VARCHAR,
    vt_s           BIGINT  NOT NULL,
    vt_e           BIGINT  NOT NULL,
    tt_s           BIGINT  NOT NULL,
    tt_e           BIGINT  NOT NULL,
    props          VARCHAR NOT NULL,
    source         VARCHAR,
    provenance_ref VARCHAR
);
CREATE TABLE edge_versions (
    vid            VARCHAR NOT NULL,
    eid            VARCHAR NOT NULL,
    src            VARCHAR NOT NULL,
    dst            VARCHAR NOT NULL,
    src_id         BIGINT  NOT NULL,
    dst_id         BIGINT  NOT NULL,
    rel_type       VARCHAR NOT NULL,
    disc           VARCHAR,
    vt_s           BIGINT  NOT NULL,
    vt_e           BIGINT  NOT NULL,
    tt_s           BIGINT  NOT NULL,
    tt_e           BIGINT  NOT NULL,
    props          VARCHAR NOT NULL,
    source         VARCHAR,
    provenance_ref VARCHAR
);
"""

INDEXES = (
    "CREATE INDEX nv_uid ON node_versions (uid)",
    "CREATE INDEX ev_eid ON edge_versions (eid)",
    "CREATE INDEX ev_src ON edge_versions (src)",
)

NODE_COLS = ["vid", "uid", "label", "vt_s", "vt_e", "tt_s", "tt_e", "props",
             "source", "provenance_ref"]
EDGE_COLS = ["vid", "eid", "src", "dst", "src_id", "dst_id", "rel_type", "disc",
             "vt_s", "vt_e", "tt_s", "tt_e", "props", "source", "provenance_ref"]
INT_COLS = {"vt_s", "vt_e", "tt_s", "tt_e", "src_id", "dst_id", "dense_id"}


# --------------------------------------------------------------------------- #
# connection, settings                                                          #
# --------------------------------------------------------------------------- #

def _threads() -> int:
    # EXTCMP_DUCKDBSQL_THREADS (a recorded setting) overrides the default, the
    env = os.environ.get("EXTCMP_DUCKDBSQL_THREADS")
    if env:
        return int(env)
    try:
        return len(os.sched_getaffinity(0))
    except AttributeError:                                  # not Linux
        return os.cpu_count() or 1


def _temp_dir() -> str:
    base = os.environ.get("TMPDIR") or "/tmp"
    d = Path(base) / "duckdbsql-spill"
    d.mkdir(parents=True, exist_ok=True)
    return str(d)


def config() -> dict[str, Any]:
    return {"threads": _threads(), "memory_limit": MEMORY_LIMIT,
            "temp_directory": _temp_dir()}


def connect(path: Path, read_only: bool):
    import duckdb
    return duckdb.connect(str(path), read_only=read_only, config=config())


REPORTED = ("threads", "memory_limit", "temp_directory", "max_temp_directory_size",
            "preserve_insertion_order", "default_collation", "index_scan_percentage",
            "index_scan_max_count", "enable_object_cache", "access_mode")


def settings_report(con) -> dict[str, Any]:
    import duckdb
    out: dict[str, Any] = {"duckdb": duckdb.__version__,
                           "system": "duckdbsql (DuckDB SQL baseline)",
                           "threads_setting": (f"EXTCMP_DUCKDBSQL_THREADS={os.environ['EXTCMP_DUCKDBSQL_THREADS']}"
                                               if os.environ.get("EXTCMP_DUCKDBSQL_THREADS")
                                               else "default: CPU affinity count")}
    for k in REPORTED:
        try:
            out[k] = con.execute(f"SELECT current_setting('{k}')").fetchone()[0]
        except Exception as e:                              # noqa: BLE001
            out[k] = f"unavailable: {type(e).__name__}"
    try:
        probe = duckdb.connect(":memory:")
        out["threads_duckdb_default"] = probe.execute(
            "SELECT current_setting('threads')").fetchone()[0]
        probe.close()
    except Exception as e:                                  # noqa: BLE001
        out["threads_duckdb_default"] = f"unavailable: {e}"
    try:
        aff = sorted(os.sched_getaffinity(0))
        out["cpu_affinity_count"] = len(aff)
        out["cpu_affinity"] = _ranges(aff)
    except AttributeError:
        pass
    if os.environ.get("EXTCMP_NUMA_CLIENT"):
        out["numa_client"] = os.environ["EXTCMP_NUMA_CLIENT"]
    elif os.environ.get("EXTCMP_NUMA_CLIENT_DROPPED"):
        out["numa_client"] = "dropped (not satisfiable): " + os.environ["EXTCMP_NUMA_CLIENT_DROPPED"]
    else:
        out["numa_client"] = "unset"
    out["load_order"] = LOAD_ORDER
    out["indexes"] = [r[0] for r in con.execute(
        "SELECT sql FROM duckdb_indexes() ORDER BY index_name").fetchall()]
    out["constraints"] = [f"{r[0]}: {r[1]}" for r in con.execute(
        "SELECT table_name, constraint_text FROM duckdb_constraints() "
        "WHERE constraint_type IN ('PRIMARY KEY', 'UNIQUE') ORDER BY 1, 2").fetchall()]
    return out


def _ranges(xs: list[int]) -> str:
    out, i = [], 0
    while i < len(xs):
        j = i
        while j + 1 < len(xs) and xs[j + 1] == xs[j] + 1:
            j += 1
        out.append(f"{xs[i]}-{xs[j]}" if j > i else str(xs[i]))
        i = j + 1
    return ",".join(out)


# --------------------------------------------------------------------------- #
# loader                                                                        #
# --------------------------------------------------------------------------- #

def _insert_chunks(con, table: str, cols: list[str], rows, timing: dict[str, float],
                   chunk: int = 500_000) -> int:
    """Rows -> Arrow record batches -> INSERT ... SELECT (DuckDB scans the Arrow
    table registered under a name). `timing` accumulates the time spent
    producing rows (the store's iterator and canonical_json), building Arrow
    arrays, and inside DuckDB's INSERT."""
    import pyarrow as pa
    n = 0
    buf: list[tuple] = []

    def flush():
        t = time.perf_counter()
        cols_data = list(zip(*buf))
        arrays = [pa.array(c, type=pa.int64() if name in INT_COLS else pa.string())
                  for name, c in zip(cols, cols_data)]
        tbl = pa.Table.from_arrays(arrays, names=cols)
        t2 = time.perf_counter()
        con.register("_chunk", tbl)
        con.execute(f"INSERT INTO {table} ({', '.join(cols)}) SELECT {', '.join(cols)} FROM _chunk")
        con.unregister("_chunk")
        t3 = time.perf_counter()
        timing["arrow_s"] = timing.get("arrow_s", 0.0) + (t2 - t)
        timing["duckdb_insert_s"] = timing.get("duckdb_insert_s", 0.0) + (t3 - t2)
        buf.clear()

    t0 = time.perf_counter()
    for r in rows:
        buf.append(r)
        n += 1
        if len(buf) >= chunk:
            timing["rows_s"] = timing.get("rows_s", 0.0) + (time.perf_counter() - t0)
            flush()
            t0 = time.perf_counter()
    timing["rows_s"] = timing.get("rows_s", 0.0) + (time.perf_counter() - t0)
    if buf:
        flush()
    return n


def load(con, adapter) -> dict[str, Any]:
    """Copy the canonical version rows out of a Gnomon store (as `pg_baseline.load`),
    then write them in the load order with the indexes. Phases are timed by the
    caller; this returns counts."""
    t0 = time.perf_counter()
    con.execute(SCHEMA)
    con.execute("CREATE TEMP TABLE _nv_stage AS SELECT * FROM node_versions LIMIT 0")
    con.execute("CREATE TEMP TABLE _ev_stage AS SELECT * FROM edge_versions LIMIT 0")
    counts: dict[str, int] = {}
    timing: dict[str, float] = {}
    n_ent = adapter.num_entities()
    uids = adapter.uids_for(list(range(n_ent)))
    dense = {u: i for i, u in enumerate(uids)}
    counts["entities"] = _insert_chunks(con, "entities", ["dense_id", "uid", "label"],
                                        ((i, u, "") for i, u in enumerate(uids)), timing)
    counts["node_versions"] = _insert_chunks(
        con, "_nv_stage", NODE_COLS,
        ((v.vid, v.uid, v.label, v.vt_s, v.vt_e, v.tt_s, v.tt_e,
          canonical_json(v.props), v.source, v.provenance_ref)
         for v in adapter.all_node_versions()), timing)
    counts["edge_versions"] = _insert_chunks(
        con, "_ev_stage", EDGE_COLS,
        ((v.vid, v.eid, v.src, v.dst, dense[v.src], dense[v.dst], v.rel_type, v.disc,
          v.vt_s, v.vt_e, v.tt_s, v.tt_e, canonical_json(v.props), v.source,
          v.provenance_ref)
         for v in adapter.all_edge_versions()), timing)
    t1 = time.perf_counter()
    con.execute(f"INSERT INTO node_versions SELECT * FROM _nv_stage ORDER BY {LOAD_ORDER['node_versions']}")
    con.execute(f"INSERT INTO edge_versions SELECT * FROM _ev_stage ORDER BY {LOAD_ORDER['edge_versions']}")
    con.execute("DROP TABLE _nv_stage")
    con.execute("DROP TABLE _ev_stage")
    t2 = time.perf_counter()
    for stmt in INDEXES:
        con.execute(stmt)
    t3 = time.perf_counter()
    con.execute("ANALYZE")
    con.execute("CHECKPOINT")
    t4 = time.perf_counter()
    # the loader is a transcription: check that every copied row landed
    for table, n in counts.items():
        got = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        if got != n:
            raise SystemExit(f"{table}: copied {n} rows, {got} landed")
    return {"counts": counts,
            "extract_breakdown": {k: round(v, 3) for k, v in timing.items()},
            "phases": {"extract_insert_s": round(t1 - t0, 3),
                       "sorted_write_s": round(t2 - t1, 3),
                       "index_build_s": round(t3 - t2, 3),
                       "analyze_checkpoint_s": round(t4 - t3, 3)}}


def index_plans(con) -> dict[str, str]:
    """Which operator each point read gets (INDEX_SCAN vs a zone-map scan)."""
    # EXPLAIN ANALYZE, not EXPLAIN: DuckDB decides index scan vs sequential
    # scan when the scan starts, so the plain EXPLAIN always prints a
    # sequential scan
    eid = con.execute("SELECT eid FROM edge_versions LIMIT 1").fetchone()[0]
    probes = {
        "hist.single as written (uid ART, belief above a MATERIALIZED CTE)":
            f"WITH h AS MATERIALIZED (SELECT * FROM node_versions WHERE uid = 'n1') "
            f"SELECT vid FROM h WHERE tt_e = {OPEN_END}",
        "hist.single with the belief conjunct in the same scan (not used)":
            f"SELECT vid FROM node_versions WHERE uid = 'n1' AND tt_e = {OPEN_END}",
        "b.hist (eid ART)":
            f"SELECT vid FROM edge_versions WHERE eid = {lit(eid)}",
        "coactive.narrow as written (src ART, belief above a MATERIALIZED CTE)":
            f"WITH h AS MATERIALIZED (SELECT * FROM edge_versions WHERE src = 'n1') "
            f"SELECT vid FROM h WHERE tt_e = {OPEN_END}",
        "b.hist@now as written (eid ART, belief above a MATERIALIZED CTE)":
            f"WITH h AS MATERIALIZED (SELECT * FROM edge_versions WHERE eid = {lit(eid)}) "
            f"SELECT vid FROM h WHERE tt_e = {OPEN_END}",
    }
    out = {}
    for k, sql in probes.items():
        plan = "\n".join(str(r[-1]) for r in con.execute("EXPLAIN ANALYZE " + sql).fetchall())
        out[k] = ("index scan" if "INDEX SCAN" in plan.upper() or "INDEX_SCAN" in plan.upper()
                  else "sequential scan" if "SEQUENTIAL SCAN" in plan.upper() else plan[:2000])
    return out


# --------------------------------------------------------------------------- #
# SQL helpers                                                                   #
# --------------------------------------------------------------------------- #

def clamp(as_of_tt: int) -> int:
    return min(as_of_tt, OPEN_END - 1)


def belief(as_of_tt: int, alias: str = "") -> str:
    """`believed at as_of_tt`, spelled as in `pg_queries.belief` (current belief
    as `tt_e = OPEN_END`; the range form only for an as-of instant)."""
    p = f"{alias}." if alias else ""
    if as_of_tt >= OPEN_END:
        return f"{p}tt_e = {OPEN_END}"
    a = clamp(int(as_of_tt))
    return f"{p}tt_s <= {a} AND {a} < {p}tt_e"


def lit(s: str) -> str:
    return "'" + str(s).replace("'", "''") + "'"


def ilit(x: int) -> str:
    return str(int(x))


def _page(total: int, returned: int) -> dict[str, Any]:
    return {"rows_total": total, "truncated": total > returned}


def _props(raw: str) -> dict[str, Any]:
    return json.loads(raw)


def _dense(con, uid: str) -> int:
    return con.execute(f"SELECT dense_id FROM entities WHERE uid = {lit(uid)}").fetchone()[0]


# --------------------------------------------------------------------------- #
# entity_history                                                            #
# --------------------------------------------------------------------------- #

def entity_history(con, *, uid: str, as_of_tt: int = OPEN_END, limit: int = 100) -> dict[str, Any]:
    # The uid lookup sits alone in a MATERIALIZED CTE and the belief filter is
    # applied above it: DuckDB 1.5.4 answers a scan with an ART index scan only
    # when the equality on the indexed column is the scan's *only* filter; with
    # the belief conjunct pushed into the same scan it falls back to a
    # sequential scan (measured: 10M-row table, 0.4 ms vs 279 ms single-thread).
    # The CTE keeps the conjunct out of the scan. (pg_queries makes the same
    # kind of choice: spelling current belief so the partial index is reachable.)
    # count and page in one statement: the window count is taken before LIMIT
    rows = con.execute(
        f"WITH h AS MATERIALIZED (SELECT * FROM node_versions WHERE uid = {lit(uid)}) "
        f"SELECT vid, uid, label, vt_s, vt_e, tt_s, props, source, provenance_ref, "
        f"       count(*) OVER () AS total "
        f"FROM h WHERE {belief(as_of_tt)} "
        f"ORDER BY vt_s, vid LIMIT {ilit(limit)}").fetchall()
    total = rows[0][9] if rows else 0
    # tt_e is OPEN_END in this operator's output whatever the as-of (pg_queries)
    out = [{"vid": r[0], "uid": r[1], "label": r[2], "vt_s": r[3], "vt_e": r[4],
            "tt_s": r[5], "tt_e": OPEN_END, "props": _props(r[6]),
            "source": r[7] or "ingest", "provenance_ref": r[8]} for r in rows]
    return {"rows": out, **_page(total, len(out))}


# --------------------------------------------------------------------------- #
# snapshot_subgraph                                                         #
# --------------------------------------------------------------------------- #

#: BFS as a recursive CTE with USING KEY (DuckDB >= 1.3): one row per node,
#: the frontier is the last iteration's rows, and `recurring.bfs` (every node
#: reached so far) keeps a node at its first (= minimum) hop. Equivalent to
#: PostgreSQL's `UNION` over (id, hop) followed by min(hop), without
#: enumerating the dominated (id, hop) pairs.
_SNAP = """
WITH RECURSIVE
nodes_at AS MATERIALIZED (
    SELECT DISTINCT ON (nv.uid) nv.uid, nv.label, en.dense_id AS id
    FROM node_versions nv JOIN entities en ON en.uid = nv.uid
    WHERE {nbel} AND nv.vt_s <= {t} AND {t} < nv.vt_e
    ORDER BY nv.uid, nv.vt_s DESC, nv.vid DESC
),
edges_at AS MATERIALIZED (
    SELECT eid, vid, src, dst, rel_type, vt_s, vt_e, src_id, dst_id
    FROM edge_versions
    WHERE {ebel} AND vt_s <= {t} AND {t} < vt_e
),
adj AS MATERIALIZED (
    SELECT src_id AS a, dst_id AS b FROM edges_at
    UNION ALL
    SELECT dst_id, src_id FROM edges_at
),
bfs(id, hop) USING KEY (id) AS (
    SELECT id, 0 FROM nodes_at WHERE uid IN ({seeds})
    UNION
    SELECT DISTINCT adj.b, bfs.hop + 1
    FROM bfs JOIN adj ON adj.a = bfs.id
    WHERE bfs.hop < {hops}
      AND adj.b IN (SELECT id FROM nodes_at)
      AND adj.b NOT IN (SELECT id FROM recurring.bfs)
),
reached AS MATERIALIZED (SELECT id, hop FROM bfs),
ind AS MATERIALIZED (
    SELECT e.* FROM edges_at e
    WHERE e.src_id IN (SELECT id FROM reached) AND e.dst_id IN (SELECT id FROM reached)
)
SELECT
    (SELECT count(*) FROM ind) AS total,
    (SELECT list({{'eid': eid, 'vid': vid, 'src': src, 'dst': dst, 'rel_type': rel_type,
                   'vt_s': vt_s, 'vt_e': vt_e}} ORDER BY vt_s, vid)
     FROM (SELECT * FROM ind ORDER BY vt_s, vid LIMIT {limit})) AS erows,
    (SELECT list({{'uid': n.uid, 'label': COALESCE(n.label, ''), 'hop': r.hop}} ORDER BY r.hop, n.uid)
     FROM reached r JOIN nodes_at n ON n.id = r.id) AS nrows
"""


def snapshot_subgraph(con, *, seeds: list[str], hops: int, t_valid: int,
                      as_of_tt: int = OPEN_END, limit: int = 100) -> dict[str, Any]:
    bel = belief(as_of_tt)
    total, erows, nrows = con.execute(_SNAP.format(
        nbel=belief(as_of_tt, "nv"), ebel=bel, t=ilit(t_valid),
        seeds=", ".join(lit(s) for s in seeds) or "NULL", hops=ilit(hops),
        limit=ilit(limit))).fetchone()
    nodes = [{"uid": r["uid"], "label": r["label"], "hop": int(r["hop"])} for r in (nrows or [])]
    rows = [dict(r) for r in (erows or [])]
    nodes_truncated = len(nodes) > limit
    page = _page(int(total), len(rows))
    return {"rows": rows, "rows_total": page["rows_total"],
            "nodes": nodes[:limit], "nodes_total": len(nodes),
            "nodes_truncated": nodes_truncated,
            "truncated": page["truncated"] or nodes_truncated}


# --------------------------------------------------------------------------- #
# diff_snapshots                                                            #
# --------------------------------------------------------------------------- #

#: The whole diff in one statement: the two node states and the two edge
#: states (DISTINCT ON, first row in (vt_s, vid) order, as PostgreSQL), the
#: anti-joins for added/removed and the join for props_changed, each list
#: counted and paged in SQL. PostgreSQL fetched the four states and compared
#: them in Python dicts; here the comparison is DuckDB's. props are compared
#: as canonical JSON text, which is equality of the parsed values for the
#: canonical form (sorted keys, compact separators).
_DIFF = """
WITH
n1 AS MATERIALIZED (SELECT DISTINCT ON (uid) uid, label, vid, props FROM node_versions
                    WHERE {bel} AND vt_s <= {t1} AND {t1} < vt_e ORDER BY uid, vt_s, vid),
n2 AS MATERIALIZED (SELECT DISTINCT ON (uid) uid, label, vid, props FROM node_versions
                    WHERE {bel} AND vt_s <= {t2} AND {t2} < vt_e ORDER BY uid, vt_s, vid),
e1 AS MATERIALIZED (SELECT DISTINCT ON (eid) eid, src, dst, rel_type, vid, props FROM edge_versions
                    WHERE {bel} AND vt_s <= {t1} AND {t1} < vt_e ORDER BY eid, vt_s, vid),
e2 AS MATERIALIZED (SELECT DISTINCT ON (eid) eid, src, dst, rel_type, vid, props FROM edge_versions
                    WHERE {bel} AND vt_s <= {t2} AND {t2} < vt_e ORDER BY eid, vt_s, vid),
na AS (SELECT uid FROM n2 WHERE NOT EXISTS (SELECT 1 FROM n1 WHERE n1.uid = n2.uid)),
nr AS (SELECT uid FROM n1 WHERE NOT EXISTS (SELECT 1 FROM n2 WHERE n2.uid = n1.uid)),
ea AS (SELECT eid, src, dst, rel_type FROM e2 WHERE NOT EXISTS (SELECT 1 FROM e1 WHERE e1.eid = e2.eid)),
er AS (SELECT eid, src, dst, rel_type FROM e1 WHERE NOT EXISTS (SELECT 1 FROM e2 WHERE e2.eid = e1.eid)),
pc AS (
    SELECT 0 AS k, a.uid AS id, a.label AS la, a.props AS pa, b.label AS lb, b.props AS pb
    FROM n1 a JOIN n2 b ON a.uid = b.uid
    WHERE a.vid <> b.vid AND (a.props <> b.props OR a.label IS DISTINCT FROM b.label)
    UNION ALL
    SELECT 1, a.eid, NULL, a.props, NULL, b.props
    FROM e1 a JOIN e2 b ON a.eid = b.eid
    WHERE a.vid <> b.vid AND a.props <> b.props
)
SELECT
    (SELECT count(*) FROM na), (SELECT list(uid ORDER BY uid) FROM (SELECT uid FROM na ORDER BY uid LIMIT {lim})),
    (SELECT count(*) FROM nr), (SELECT list(uid ORDER BY uid) FROM (SELECT uid FROM nr ORDER BY uid LIMIT {lim})),
    (SELECT count(*) FROM ea), (SELECT list({{'eid': eid, 'src': src, 'dst': dst, 'rel_type': rel_type}} ORDER BY eid)
                                FROM (SELECT * FROM ea ORDER BY eid LIMIT {lim})),
    (SELECT count(*) FROM er), (SELECT list({{'eid': eid, 'src': src, 'dst': dst, 'rel_type': rel_type}} ORDER BY eid)
                                FROM (SELECT * FROM er ORDER BY eid LIMIT {lim})),
    (SELECT count(*) FROM pc), (SELECT list({{'k': k, 'id': id, 'la': la, 'pa': pa, 'lb': lb, 'pb': pb}} ORDER BY k, id)
                                FROM (SELECT * FROM pc ORDER BY k, id LIMIT {lim}))
"""


def diff_snapshots(con, *, t1: int, t2: int, as_of_tt: int = OPEN_END,
                   scope: dict | None = None, limit: int = 100) -> dict[str, Any]:
    if scope is not None:
        raise NotImplementedError("scoped diff")
    r = con.execute(_DIFF.format(bel=belief(as_of_tt), t1=ilit(t1), t2=ilit(t2),
                                 lim=ilit(limit))).fetchone()
    changed = []
    for c in (r[9] or []):
        if c["k"] == 0:
            changed.append({"kind": "node", "id": c["id"],
                            "from": {"label": c["la"], "props": _props(c["pa"])},
                            "to": {"label": c["lb"], "props": _props(c["pb"])}})
        else:
            changed.append({"kind": "edge", "id": c["id"],
                            "from": {"props": _props(c["pa"])},
                            "to": {"props": _props(c["pb"])}})
    lists = {"nodes_added": (list(r[1] or []), r[0]),
             "nodes_removed": (list(r[3] or []), r[2]),
             "edges_added": ([dict(x) for x in (r[5] or [])], r[4]),
             "edges_removed": ([dict(x) for x in (r[7] or [])], r[6]),
             "props_changed": (changed, r[8])}
    out: dict[str, Any] = {}
    for k, (v, n) in lists.items():
        out[k], out[f"{k}_total"] = v, int(n)
    out["truncated"] = any(int(n) > limit for _v, n in lists.values())
    return out


# --------------------------------------------------------------------------- #
# graph_metric_timeseries (edge_event_count)                                #
# --------------------------------------------------------------------------- #

def graph_metric_timeseries(con, *, metric: str, window: dict, stride: int,
                            as_of_tt: int = OPEN_END, limit: int = 100) -> dict[str, Any]:
    if metric != "edge_event_count":
        raise NotImplementedError(metric)
    t_a, t_b = int(window["t_a"]), int(window["t_b"])
    n = -(-(t_b - t_a) // stride)
    rows = con.execute(f"""
        WITH b AS (
            SELECT g AS i, {t_a} + g * {stride} AS ba, least({t_a} + (g + 1) * {stride}, {t_b}) AS bb
            FROM range(0, {n}) t(g)
        ),
        c AS (
            SELECT (vt_s - {t_a}) // {stride} AS i, count(*) AS n
            FROM edge_versions
            WHERE {belief(as_of_tt)} AND vt_s >= {t_a} AND vt_s < {t_b}
            GROUP BY 1
        )
        SELECT b.ba, b.bb, COALESCE(c.n, 0) FROM b LEFT JOIN c ON c.i = b.i
        ORDER BY b.ba LIMIT {ilit(limit)}""").fetchall()
    out = [{"t_a": int(r[0]), "t_b": int(r[1]), "value": int(r[2])} for r in rows]
    return {"rows": out, "rows_total": n, "truncated": n > len(out), "n_buckets": n}


# --------------------------------------------------------------------------- #
# aggregate_events (rel_type x time_bucket; count + distinct dst)           #
# --------------------------------------------------------------------------- #

def aggregate_events(con, *, group_by: list, aggregates: list, window: dict,
                     stride: int | None = None, rel_types: list | None = None,
                     as_of_tt: int = OPEN_END, limit: int = 100,
                     cursor: str | None = None) -> dict[str, Any]:
    if [d["dim"] for d in group_by] != ["rel_type", "time_bucket"] \
            or aggregates != [{"agg": "count"}, {"agg": "count_distinct", "of": "dst"}] \
            or rel_types is not None or cursor is not None:
        raise NotImplementedError("only the agg.rel_bucket shape is written")
    t_a, t_b = int(window["t_a"]), int(window["t_b"])
    rows = con.execute(f"""
        SELECT rel_type, (vt_s - {t_a}) // {stride} AS b,
               count(*) AS c, count(DISTINCT dst_id) AS d, count(*) OVER () AS total
        FROM edge_versions
        WHERE {belief(as_of_tt)} AND vt_s >= {t_a} AND vt_s < {t_b}
        GROUP BY rel_type, b
        ORDER BY rel_type, b LIMIT {ilit(limit)}""").fetchall()
    total = rows[0][4] if rows else 0
    out = [{"rel_type": r[0], "t_a": t_a + int(r[1]) * stride,
            "t_b": min(t_a + (int(r[1]) + 1) * stride, t_b),
            "count": int(r[2]), "distinct_dst": int(r[3])} for r in rows]
    return {"rows": out, **_page(int(total), len(out))}


# --------------------------------------------------------------------------- #
# temporal_reachability                                                     #
# --------------------------------------------------------------------------- #

#: Time-respecting reachability (non-decreasing rule: tau = max(arrival,
#: vt_s), admissible iff tau < vt_e and tau < t_b) as Bellman-Ford in one
#: recursive CTE with USING KEY: the key-value state holds one arrival per
#: node; each iteration relaxes the out-edges of the nodes improved in the
#: previous one, keeps the minimum per destination, and emits only real
#: improvements over `recurring.reach`; it stops when a round improves
#: nothing. This is exactly the pruning PostgreSQL's recursive CTE cannot
#: express (pg_queries: the plain recursive form took 278 s at 200k), which
#: is why PostgreSQL relaxes round by round against a temp table from Python.
_REACH = """
WITH RECURSIVE
ev AS MATERIALIZED (
    SELECT src_id, dst_id, vt_s, vt_e FROM edge_versions
    WHERE {bel} AND vt_e > {t_a} AND vt_s < {t_b}
),
reach(id, arr) USING KEY (id) AS (
    SELECT {sid}::BIGINT, {t_a}::BIGINT
    UNION
    SELECT c.id, c.arr FROM (
        SELECT e.dst_id AS id, min(greatest(r.arr, e.vt_s)) AS arr
        FROM reach r JOIN ev e ON e.src_id = r.id
        WHERE greatest(r.arr, e.vt_s) < e.vt_e AND greatest(r.arr, e.vt_s) < {t_b}
        GROUP BY e.dst_id
    ) c LEFT JOIN recurring.reach cur ON cur.id = c.id
    WHERE cur.arr IS NULL OR c.arr < cur.arr
)
SELECT en.uid, r.arr, count(*) OVER () AS total
FROM reach r JOIN entities en ON en.dense_id = r.id
WHERE r.id <> {sid}
ORDER BY r.arr, en.uid LIMIT {limit}
"""


def temporal_reachability(con, *, src: str, window: dict, direction: str = "out",
                          delta_max_wait: int | None = None, as_of_tt: int = OPEN_END,
                          limit: int = 100) -> dict[str, Any]:
    if direction != "out" or delta_max_wait is not None:
        raise NotImplementedError(f"direction={direction} delta={delta_max_wait}")
    rows = con.execute(_REACH.format(
        bel=belief(as_of_tt), t_a=ilit(window["t_a"]), t_b=ilit(window["t_b"]),
        sid=f"(SELECT dense_id FROM entities WHERE uid = {lit(src)})",
        limit=ilit(limit))).fetchall()
    total = rows[0][2] if rows else 0
    out = [{"uid": r[0], "earliest_arrival": int(r[1])} for r in rows]
    return {"rows": out, **_page(int(total), len(out))}


# --------------------------------------------------------------------------- #
# temporal_paths                                                            #
# --------------------------------------------------------------------------- #

#: Node-simple k-shortest time-respecting paths. Ranking (arrival, hops, then
#: the sequence of (vt_s, eid) compared lexicographically) is DuckDB's own
#: list ordering on a LIST of STRUCT(t, eid, ...): lists compare element-wise
#: and structs field by field, and `eid` determines src/dst/rel_type, so the
#: trailing fields never decide. PostgreSQL needed a fixed-width text key.
_PATHS = """
WITH RECURSIVE
ev AS MATERIALIZED (
    SELECT src_id, dst_id, eid, rel_type, src, dst, vt_s, vt_e FROM edge_versions
    WHERE {bel} AND vt_e > {t_a} AND vt_s < {t_b}
),
ids AS MATERIALIZED (
    SELECT (SELECT dense_id FROM entities WHERE uid = {src}) AS s,
           (SELECT dense_id FROM entities WHERE uid = {dst}) AS d
),
p(node, arr, hops, seen, edges) AS (
    SELECT s, {t_a}::BIGINT, 0, [s],
           []::STRUCT(t BIGINT, eid VARCHAR, src VARCHAR, dst VARCHAR, rel_type VARCHAR)[]
    FROM ids
    UNION ALL
    SELECT e.dst_id, greatest(p.arr, e.vt_s), p.hops + 1, list_append(p.seen, e.dst_id),
           list_append(p.edges, {{'t': e.vt_s, 'eid': e.eid, 'src': e.src, 'dst': e.dst,
                                  'rel_type': e.rel_type}})
    FROM p JOIN ev e ON e.src_id = p.node
    WHERE p.hops < {max_hops}
      AND p.node <> (SELECT d FROM ids)
      AND greatest(p.arr, e.vt_s) < e.vt_e
      AND greatest(p.arr, e.vt_s) < {t_b}
      AND NOT list_contains(p.seen, e.dst_id)
)
SELECT arr, hops, edges, count(*) OVER () AS total
FROM p WHERE node = (SELECT d FROM ids) AND hops > 0
ORDER BY arr, hops, edges LIMIT {k}
"""


def temporal_paths(con, *, src: str, dst: str, window: dict, k: int = 5,
                   max_hops: int = 4, as_of_tt: int = OPEN_END) -> dict[str, Any]:
    rows = con.execute(_PATHS.format(
        bel=belief(as_of_tt), t_a=ilit(window["t_a"]), t_b=ilit(window["t_b"]),
        src=lit(src), dst=lit(dst), max_hops=ilit(max_hops), k=ilit(k))).fetchall()
    total = int(rows[0][3]) if rows else 0
    out = [{"arrival": int(r[0]), "hops": int(r[1]),
            "edges": [{"src": e["src"], "dst": e["dst"], "rel_type": e["rel_type"],
                       "eid": e["eid"], "t": int(e["t"])} for e in r[2]]}
           for r in rows]
    return {"rows": out, "rows_total": total, "truncated": total > k}


# --------------------------------------------------------------------------- #
# burst_detection (zscore)                                                  #
# --------------------------------------------------------------------------- #

_BURST = """
WITH b AS (
    SELECT g AS i, {t_a} + g * {stride} AS ba, least({t_a} + (g + 1) * {stride}, {t_b}) AS bb
    FROM range(0, {n}) t(g)
),
c AS (
    SELECT (vt_s - {t_a}) // {stride} AS i, count(*) AS n
    FROM edge_versions
    WHERE {bel} AND vt_s >= {t_a} AND vt_s < {t_b}
    GROUP BY 1
),
s AS (SELECT b.i, b.ba, b.bb, COALESCE(c.n, 0)::DOUBLE AS v FROM b LEFT JOIN c ON c.i = b.i)
SELECT i, ba, bb, v,
       avg(v)        OVER w,
       stddev_pop(v) OVER w,
       count(*)      OVER w
FROM s
WINDOW w AS (ORDER BY i ROWS BETWEEN {w} PRECEDING AND 1 PRECEDING)
ORDER BY i
"""

BIG_SCORE = 1e9


def burst_detection(con, *, target: dict, window: dict, stride: int,
                    method: str = "zscore", params: dict | None = None,
                    as_of_tt: int = OPEN_END, limit: int = 100) -> dict[str, Any]:
    if method != "zscore" or target.get("kind") != "edge_event_rate":
        raise NotImplementedError(f"{method}/{target.get('kind')}")
    if target.get("rel_type") or target.get("uid"):
        raise NotImplementedError("filtered burst target")
    params = params or {}
    t_a, t_b = int(window["t_a"]), int(window["t_b"])
    n = -(-(t_b - t_a) // stride)
    rows = con.execute(_BURST.format(bel=belief(as_of_tt), t_a=t_a, t_b=t_b,
                                     stride=int(stride), n=n,
                                     w=int(params.get("w", 10)))).fetchall()
    # As in pg_queries: the final |x - mean| / std and its round(.., 9) stay in
    # Python, because the reference thresholds on the *rounded* score with
    # Python's half-to-even on the binary double; the scan, bucketing and the
    # windowed mean/stddev are DuckDB's. At most a few thousand buckets.
    z = params.get("z", 3.0)
    flagged = []
    for _i, ba, bb, v, mean, std, hist_n in rows:
        if not hist_n:
            continue
        std = float(std or 0.0)
        score = abs(v - float(mean)) / std if std > 0 else (0.0 if v == float(mean) else BIG_SCORE)
        score = round(score, 9)
        if score >= z:
            flagged.append({"t_a": int(ba), "t_b": int(bb), "value": float(v), "score": float(score)})
    page = flagged[:limit]
    return {"rows": page, **_page(len(flagged), len(page)), "n_buckets": n}


# --------------------------------------------------------------------------- #
# neighborhood_evolution                                                    #
# --------------------------------------------------------------------------- #

_NBR = """
WITH
inc AS MATERIALIZED (
    SELECT CASE WHEN ev.src_id = {id} THEN ev.dst_id ELSE ev.src_id END AS other,
           ev.vt_s, ev.vt_e
    FROM edge_versions ev
    WHERE {bel} AND ev.vt_e > {t1} AND ev.vt_s <= {t2}
      AND (ev.src_id = {id} OR ev.dst_id = {id})
),
a1 AS (SELECT DISTINCT other FROM inc WHERE vt_s <= {t1} AND {t1} < vt_e AND other <> {id}),
a2 AS (SELECT DISTINCT other FROM inc WHERE vt_s <= {t2} AND {t2} < vt_e AND other <> {id}),
g AS (SELECT en.uid FROM (SELECT other FROM a2 EXCEPT SELECT other FROM a1) x
      JOIN entities en ON en.dense_id = x.other),
l AS (SELECT en.uid FROM (SELECT other FROM a1 EXCEPT SELECT other FROM a2) x
      JOIN entities en ON en.dense_id = x.other),
bs AS (SELECT t FROM range({t1}, {t2}, {stride}) r(t))
SELECT (SELECT count(*) FROM g), (SELECT list(uid ORDER BY uid) FROM (SELECT uid FROM g ORDER BY uid LIMIT {lim})),
       (SELECT count(*) FROM l), (SELECT list(uid ORDER BY uid) FROM (SELECT uid FROM l ORDER BY uid LIMIT {lim})),
       (SELECT list({{'t': bs.t, 'degree': (SELECT count(*) FROM inc WHERE inc.vt_s <= bs.t AND inc.vt_e > bs.t)}}
                    ORDER BY bs.t) FROM bs)
"""


def neighborhood_evolution(con, *, uid: str, t1: int, t2: int, stride: int | None = None,
                           as_of_tt: int = OPEN_END, limit: int = 100) -> dict[str, Any]:
    stride = stride or max(1, (t2 - t1) // 20)
    # the dense id first, as PostgreSQL does: inlined as a constant, the
    # incidence predicate is a plain filter on the scan
    r = con.execute(_NBR.format(id=ilit(_dense(con, uid)), bel=belief(as_of_tt, "ev"), t1=ilit(t1),
                                t2=ilit(t2), stride=ilit(stride), lim=ilit(limit))).fetchone()
    ng, gained, nl, lost, series = int(r[0]), list(r[1] or []), int(r[2]), list(r[3] or []), r[4] or []
    return {
        "neighbors_gained": gained, "neighbors_gained_total": ng,
        "neighbors_lost": lost, "neighbors_lost_total": nl,
        "degree_series": [{"t": int(x["t"]), "degree": int(x["degree"])} for x in series],
        "stride": stride,
        "truncated": ng > limit or nl > limit,
    }


# --------------------------------------------------------------------------- #
# co_active (Allen `overlaps`)                                              #
# --------------------------------------------------------------------------- #

def _spec_rel(spec: dict, bel: str) -> str:
    """One side of the interval join as a relation. With a `src` in the spec
    the src lookup sits alone in a MATERIALIZED CTE so the `ev_src` ART serves
    it (see entity_history); the other conjuncts and belief apply above it."""
    parts = [bel]
    if spec.get("rel_type"):
        parts.append(f"rel_type = {lit(spec['rel_type'])}")
    if spec.get("dst"):
        parts.append(f"dst = {lit(spec['dst'])}")
    if spec.get("src"):
        return (f"(WITH s AS MATERIALIZED (SELECT * FROM edge_versions WHERE src = {lit(spec['src'])}) "
                f"SELECT * FROM s WHERE {' AND '.join(parts)})")
    return f"(SELECT * FROM edge_versions WHERE {' AND '.join(parts)})"


def co_active(con, *, a_spec: dict, b_spec: dict, allen_relation: dict,
              as_of_tt: int = OPEN_END, limit: int = 100) -> dict[str, Any]:
    rel = allen_relation.get("relation")
    if rel != "overlaps":
        raise NotImplementedError(rel)
    rows = con.execute(f"""
        WITH a AS {_spec_rel(a_spec, belief(as_of_tt))},
             b AS {_spec_rel(b_spec, belief(as_of_tt))}
        SELECT a.eid, a.vid, a.src, a.dst, a.rel_type, a.vt_s, a.vt_e,
               b.eid, b.vid, b.src, b.dst, b.rel_type, b.vt_s, b.vt_e, count(*) OVER () AS total
        FROM a JOIN b ON a.vt_s < b.vt_s AND b.vt_s < a.vt_e AND a.vt_e < b.vt_e AND a.vid <> b.vid
        ORDER BY a.vt_s, a.vid, b.vt_s, b.vid LIMIT {ilit(limit)}""").fetchall()
    total = int(rows[0][14]) if rows else 0
    out = [{"a": {"eid": r[0], "vid": r[1], "src": r[2], "dst": r[3], "rel_type": r[4],
                  "vt_s": r[5], "vt_e": r[6]},
            "b": {"eid": r[7], "vid": r[8], "src": r[9], "dst": r[10], "rel_type": r[11],
                  "vt_s": r[12], "vt_e": r[13]}} for r in rows]
    return {"rows": out, **_page(total, len(out))}


# --------------------------------------------------------------------------- #
# resolve_entities                                                          #
# --------------------------------------------------------------------------- #

_RESOLVE = """
WITH v AS MATERIALIZED (
    SELECT uid, label, props, vt_s, vid, lower(uid) AS luid,
           -- only JSON string names participate in matching
           lower(COALESCE(CASE WHEN json_type(props, '$.name') = 'VARCHAR'
                               THEN props ->> '$.name' END, '')) AS lname
    FROM node_versions WHERE {bel}
),
scored AS (
    SELECT uid, min(CASE WHEN uid = {q} THEN 0
                         WHEN strpos(luid, {ql}) > 0 THEN 1
                         WHEN lname <> '' AND strpos(lname, {ql}) > 0 THEN 2
                    END) AS match
    FROM v GROUP BY uid
),
latest AS (
    SELECT DISTINCT ON (uid) uid, label, props FROM v ORDER BY uid, vt_s DESC, vid DESC
)
SELECT s.uid, l.label, l.props, s.match, count(*) OVER () AS total
FROM scored s JOIN latest l ON l.uid = s.uid
WHERE s.match IS NOT NULL {label_f}
ORDER BY s.match, s.uid LIMIT {limit}
"""


def resolve_entities(con, *, query: str, label: str | None = None,
                     as_of_tt: int = OPEN_END, limit: int = 100) -> dict[str, Any]:
    rows = con.execute(_RESOLVE.format(
        bel=belief(as_of_tt), q=lit(query), ql=lit(query.lower()),
        label_f=f"AND l.label = {lit(label)}" if label else "", limit=ilit(limit))).fetchall()
    total = int(rows[0][4]) if rows else 0
    # `name` keeps its JSON type (a number stays a number): read from the props
    out = [{"uid": r[0], "label": r[1], "name": _props(r[2]).get("name"),
            "match": int(r[3])} for r in rows]
    return {"rows": out, **_page(total, len(out))}


# --------------------------------------------------------------------------- #
# count_temporal_motifs (M_triangle_cyclic)                                 #
# --------------------------------------------------------------------------- #

_MOTIF = """
WITH mev AS MATERIALIZED (
    SELECT eid, src, dst, vt_s AS t FROM edge_versions
    WHERE {bel} AND vt_s >= {t_a} AND vt_s < {t_b} {nf}
)
SELECT (SELECT count(*) FROM mev),
       (SELECT count(*)
        FROM mev a
        JOIN mev b ON b.src = a.dst
                  AND b.t >= a.t AND b.t <= a.t + {delta}
                  AND (b.t > a.t OR (b.t = a.t AND b.eid > a.eid))
        JOIN mev c ON c.src = b.dst AND c.dst = a.src
                  AND c.t >= b.t AND c.t <= a.t + {delta}
                  AND (c.t > b.t OR (c.t = b.t AND c.eid > b.eid))
        WHERE a.src <> a.dst AND b.dst <> a.dst AND b.dst <> a.src)
"""


def count_temporal_motifs(con, *, motif: str, delta: int, window: dict,
                          node_filter: list[str] | None = None,
                          as_of_tt: int = OPEN_END) -> dict[str, Any]:
    if motif != "M_triangle_cyclic":
        raise NotImplementedError(motif)
    nf = ""
    if node_filter is not None:
        vals = ", ".join(lit(u) for u in sorted(set(node_filter))) or "NULL"
        nf = f"AND src IN ({vals}) AND dst IN ({vals})"
    r = con.execute(_MOTIF.format(bel=belief(as_of_tt), t_a=ilit(window["t_a"]),
                                  t_b=ilit(window["t_b"]), delta=ilit(delta), nf=nf)).fetchone()
    return {"count": int(r[1]), "n_events_in_window": int(r[0]), "truncated": False}


# --------------------------------------------------------------------------- #
# belief-time family B (extcmp_beliefq): the ids that are not registry aliases #
# --------------------------------------------------------------------------- #

def belief_diff(con, *, t1, t2, window, limit):
    """`extcmp_beliefq.pg_diff` in one statement (totals and page together)."""
    a, b = clamp(int(t1)), clamp(int(t2))
    sel = (f"vt_s < {int(window['t_b'])} AND vt_e > {int(window['t_a'])} AND "
           f"((tt_s > {a} AND tt_s <= {b}) OR (tt_e > {a} AND tt_e <= {b}))")
    r = con.execute(f"""
        WITH v AS MATERIALIZED (
            SELECT 0 AS k, uid AS key, vid, vt_s, vt_e, tt_s, tt_e FROM node_versions WHERE {sel}
            UNION ALL
            SELECT 1, eid, vid, vt_s, vt_e, tt_s, tt_e FROM edge_versions WHERE {sel}
        ),
        c AS MATERIALIZED (
            SELECT k, key, vid, tt_s AS tt, 'added' AS change, vt_s, vt_e FROM v
            WHERE tt_s > {a} AND tt_s <= {b}
            UNION ALL
            SELECT k, key, vid, tt_e, 'retracted', vt_s, vt_e FROM v
            WHERE tt_e > {a} AND tt_e <= {b}
        )
        SELECT (SELECT count(*) FILTER (WHERE change = 'added') FROM c),
               (SELECT count(*) FILTER (WHERE change = 'retracted') FROM c),
               (SELECT list({{'k': k, 'key': key, 'vid': vid, 'tt': tt, 'change': change,
                              'vt_s': vt_s, 'vt_e': vt_e}} ORDER BY k, key, tt, change, vid)
                FROM (SELECT * FROM c ORDER BY k, key, tt, change, vid LIMIT {int(limit)}))
        """).fetchone()
    out = [{"kind": "node" if x["k"] == 0 else "edge", "key": x["key"], "vid": x["vid"],
            "tt": int(x["tt"]), "change": x["change"], "vt_s": int(x["vt_s"]),
            "vt_e": int(x["vt_e"])} for x in (r[2] or [])]
    return {"rows": out, "added_total": int(r[0]), "retracted_total": int(r[1]),
            "truncated": int(r[0]) + int(r[1]) > len(out)}


def belief_hist(con, *, eid, **_):
    import extcmp_beliefq as B
    rows = con.execute(f"SELECT vid, vt_s, vt_e, tt_s, tt_e FROM edge_versions "
                       f"WHERE eid = {lit(eid)}").fetchall()
    return B.shape_hist([{"vid": r[0], "vt_s": r[1], "vt_e": r[2], "tt_s": r[3], "tt_e": r[4]}
                         for r in rows])


def belief_believed(con, *, eid, **_):
    import extcmp_beliefq as B
    # the eid lookup alone in a MATERIALIZED CTE, so the ART serves it (see
    # entity_history); the belief filter above it
    rows = con.execute(f"WITH h AS MATERIALIZED (SELECT vid, vt_s, vt_e, tt_s, tt_e FROM edge_versions "
                       f"WHERE eid = {lit(eid)}) SELECT * FROM h WHERE tt_e = {OPEN_END}").fetchall()
    return B.shape_believed([{"vid": r[0], "vt_s": r[1], "vt_e": r[2], "tt_s": r[3], "tt_e": r[4]}
                             for r in rows])


QUERIES = {
    "hist.single": entity_history,
    "hist.asof": entity_history,
    "snap.hop2": snapshot_subgraph,
    "diff.global": diff_snapshots,
    "reach.window": temporal_reachability,
    "paths.k": temporal_paths,
    "series.count": graph_metric_timeseries,
    "burst.zscore": burst_detection,
    "agg.rel_bucket": aggregate_events,
    "nbr.evolution": neighborhood_evolution,
    "coactive.narrow": co_active,
    "resolve.substr": resolve_entities,
    "motif.filtered": count_temporal_motifs,
}


# --------------------------------------------------------------------------- #
# the kit system (extcmp.py `_system_class("duckdbsql")`)                       #
# --------------------------------------------------------------------------- #

class DuckDBSQL:
    """In-process like `native` and `duckdb`: no server; the window pins it with
    EXTCMP_NUMA_CLIENT like every client process."""

    name, compare = "duckdbsql", "exact"

    def __init__(self, ref: Path | None, workdir: Path, size_tag: str):
        self.ref, self.workdir, self.size_tag = ref, workdir, size_tag

    @property
    def dir(self) -> Path:
        return self.workdir / "stores" / f"duckdbsql-{self.size_tag}"

    @property
    def db(self) -> Path:
        return self.dir / "gnomon.duckdb"

    def ref_adapter(self):
        from extcmp import open_existing_store     # P-7: never create the reference empty
        return open_existing_store(self.ref, "native").adapter

    def load(self, log: Path) -> dict[str, Any]:
        if self.dir.exists():
            raise SystemExit(f"{self.dir} exists; refusing to reload over it")
        self.dir.mkdir(parents=True)
        adapter = self.ref_adapter()
        t0 = time.perf_counter()
        con = connect(self.db, read_only=False)
        rec = load(con, adapter)
        wall = time.perf_counter() - t0
        rec["index_plans"] = index_plans(con)
        rec["settings"] = settings_report(con)
        rec["table_sizes"] = [list(r) for r in con.execute(
            "SELECT table_name, estimated_size, column_count FROM duckdb_tables() ORDER BY 1").fetchall()]
        rec["database_size"] = [str(x) for x in con.execute("PRAGMA database_size").fetchone()]
        con.close()
        rec["load_s"] = round(wall, 3)
        rec["bytes"] = sum(p.stat().st_size for p in self.dir.iterdir() if p.is_file())
        rec["files"] = {p.name: p.stat().st_size for p in self.dir.iterdir() if p.is_file()}
        return rec

    def open(self) -> None:
        self.con = connect(self.db, read_only=True)
        self.report = settings_report(self.con)

    def cancel(self) -> None:
        self.con.interrupt()

    def settings(self) -> dict[str, Any]:
        return getattr(self, "report", {})

    def fn_for(self, q: H.Query):
        con, a = self.con, dict(q.args)
        qid = W10_ALIAS.get(q.id, q.id)
        if q.id.startswith("b."):
            import extcmp_beliefq as B
            if q.id == "b.diff":
                lim = a.pop("limit")
                return lambda: belief_diff(con, limit=lim, **a)
            if q.id == "b.hist":
                return lambda: belief_hist(con, **a)
            if q.id == "b.hist@now":
                return lambda: belief_believed(con, **a)
            qid = B.ALIAS.get(q.id, q.id)
        fn = QUERIES.get(qid)
        if fn is None:
            return None
        return lambda: fn(con, **a)

    def close(self) -> None:
        self.con.close()
