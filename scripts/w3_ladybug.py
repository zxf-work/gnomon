"""W3 on LadybugDB (the community continuation of Kùzu): loaders, digest and the query adapter.

**This is not an LDBC Benchmark, and nothing produced here is an LDBC Benchmark Result.**

    w3_ladybug.py prep-csv --hist HIST --out CSVDIR             the graph CSVs, as the Neo4j and Memgraph loads write them
    w3_ladybug.py load     --csv CSVDIR --db DB --stage DIR --out REC      bulk COPY FROM of every version (query stores)
    w3_ladybug.py replay   --hist HIST --base-meta M --db DB --stage DIR --out REC [--prefix-days N]
                           bulk snapshot (tt <= T0), then one LadybugDB transaction per W3 transaction time (ingest study)
    w3_ladybug.py digest   --db DB --out DIGEST.json                 the order-independent digest of w3_snb_ingest.py
    w3_ladybug.py cmp      DIGEST_A DIGEST_B

**Model (D-W3-4, the version-node model of the Neo4j and Memgraph texts).** Every node version is a node and every
(edge version x source version x target version) with intersecting rectangles is a relationship carrying the
intersection rectangle, plus the edge version's own rectangle (evs, eve, ets, ete). The input is the graph CSVs of
`w3_snb_ingest.write_csvs` (the files neo4j-admin and Memgraph's LOAD CSV read; SF10: `graph-csv.tar.zst`; SF0.1
and SF1: rewritten from the same history tables by the same function, `prep-csv`).

LadybugDB is schema-first: one node table per leaf label, and Neo4j's label hierarchy becomes a label union in the
texts (`:Message` = `:Post:Comment`, `:Place` = `:City:Country:Continent`, `:Organisation` =
`:Company:University`, `:V` = every table; D-W3-13). Tables of one family share their columns. A relationship type
is one REL table with one FROM/TO pair per (source table, target table) that occurs in the data. KNOWS has one more
column, `w3w` (DOUBLE, NULL in the store), where the weighted-path texts set their weights inside a transaction that
is rolled back (as Memgraph's texts do, D-W3-4).

**Load.** DuckDB stages each CSV into Parquet with LadybugDB's column types (the CSV's typed header; an empty field
is NULL, as for neo4j-admin and Memgraph's `NULLIF ''`), and splits each relationship file by its endpoint tables
(the gid -> table map of the node files). LadybugDB then loads each file with `COPY ... FROM` (the vendor's bulk
path), and `CHECKPOINT`s. Read-back: per label and type the count and the sums of tt_s, closed tt_e and vt_s in
whole seconds, against the same sums over the input (`verified`), and the digest, which must equal the Neo4j and
Memgraph digests of the same log.

**Durability.** LadybugDB has a write-ahead log, always on; a COMMIT (auto-commit or explicit) is written to the
WAL before it returns, and a CHECKPOINT folds the WAL into the database file. The replay loader commits every W3
transaction time as one explicit transaction, and records the database's checkpoint settings (`auto_checkpoint`,
`checkpoint_threshold`) in its record.
"""

from __future__ import annotations

import argparse
import re
import datetime as dt
import json
import os
import shutil
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

OPEN = 2 ** 62
#: Neo4j label set (as w3_snb_systems.NODE_SPEC writes it) -> LadybugDB node table
NODE_TABLE = {"Person": "Person", "Forum": "Forum", "Message:Post": "Post", "Message:Comment": "Comment",
              "Place:City": "City", "Place:Country": "Country", "Place:Continent": "Continent",
              "Organisation:Company": "Company", "Organisation:University": "University", "Tag": "Tag",
              "TagClass": "TagClass"}
LABEL_OF = {t: l for l, t in NODE_TABLE.items()}
FAMILY = {"Post": "Message", "Comment": "Message", "City": "Place", "Country": "Place", "Continent": "Place",
          "Company": "Organisation", "University": "Organisation"}
#: the CSV header's types (neo4j-admin names) -> LadybugDB and DuckDB types
LB_T = {"string": "STRING", "long": "INT64", "int": "INT64", "double": "DOUBLE", "boolean": "BOOLEAN",
        "localdatetime": "TIMESTAMP", "date": "DATE"}
DK_T = {"STRING": "VARCHAR", "INT64": "BIGINT", "DOUBLE": "DOUBLE", "BOOLEAN": "BOOLEAN", "TIMESTAMP": "TIMESTAMP",
        "DATE": "DATE"}
REL_COLS = [("rid", "INT64"), ("vt_s", "INT64"), ("vt_e", "INT64"), ("tt_s", "INT64"), ("tt_e", "INT64"),
            ("creationDate", "TIMESTAMP"), ("classYear", "INT64"), ("workFrom", "INT64"), ("evs", "INT64"),
            ("eve", "INT64"), ("ets", "INT64"), ("ete", "INT64")]
EXTRA_REL_COLS = {"KNOWS": [("w3w", "DOUBLE")]}


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


BUFFER_POOL, BUFFER_POOL_RULE = auto_buffer_pool(os.environ.get("W3_LADYBUG_BUFFER_POOL_GB"))


def _lb_version() -> str:
    try:
        import importlib.metadata as md
        return md.version("ladybug")
    except Exception:                                            # noqa: BLE001
        return "?"


def open_db(path: str, read_only: bool, threads: int = 0, buffer_pool: int | None = None):
    import ladybug as lb
    kw: dict[str, Any] = {"buffer_pool_size": buffer_pool or BUFFER_POOL, "read_only": read_only}
    if threads:
        kw["max_num_threads"] = threads
    db = lb.Database(path, **kw)
    return db, lb.Connection(db, num_threads=threads or 0)


def rows(conn, q: str, params: dict | None = None) -> tuple[list[str], list[list]]:
    r = conn.execute(q, params or {})
    cols = r.get_column_names()
    out = []
    while r.has_next():
        out.append(r.get_next())
    r.close()
    return cols, out


# ------------------------------------------------------------------------------------------------- CSV files ---

def cmd_prep_csv(a) -> int:
    """The graph CSVs of the full version table (t0 = OPEN_END), with the Neo4j load's own function."""
    import w3_snb_ingest as I
    import w3_snb_systems as Y
    t0 = time.time()
    duck = Y.graph_tables(a.hist)
    files = I.write_csvs(duck, OPEN, Path(a.out))
    rec = {"hist": a.hist, "t0": OPEN, "files": files, "prepare_s": round(time.time() - t0, 1),
           "by": "w3_snb_ingest.write_csvs (the Neo4j / Memgraph bulk input)"}
    Path(a.out, "graph-csv.prepared.json").write_text(json.dumps(rec, indent=1) + "\n")
    print(json.dumps({k: v for k, v in rec.items() if k != "files"}))
    return 0


def _header(path: Path) -> list[str]:
    import csv
    with open(path, newline="") as fh:
        return next(csv.reader(fh))


def _csv(path: Path) -> str:
    return (f"read_csv('{path}', header=true, all_varchar=true, delim=',', quote='\"', escape='\"', "
            f"max_line_size=100000000, parallel=false)")


def node_schema(csvdir: Path) -> dict[str, list[tuple[str, str]]]:
    """table -> [(column, LadybugDB type)], gid first; a family's tables share one column list."""
    per: dict[str, dict[str, str]] = {}
    for f in sorted(csvdir.glob("n_*.csv")):
        lab = f.stem[2:].replace("_", ":")
        t = NODE_TABLE[lab]
        cols = per.setdefault(FAMILY.get(t, t), {})
        for h in _header(f)[2:]:
            k, _, ty = h.partition(":")
            cols.setdefault(k, LB_T[ty])
    out = {}
    for t in NODE_TABLE.values():
        fam = per.get(FAMILY.get(t, t), {})
        out[t] = [("gid", "INT64")] + sorted((k, v) for k, v in fam.items())
    return out


def stage(csvdir: Path, st: Path, threads: int, mem: str) -> dict[str, Any]:
    """CSV -> Parquet with LadybugDB's types; relationship files split by their endpoint tables."""
    import duckdb
    st.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(st / "stage.duckdb"))
    con.execute(f"SET threads={threads}; SET memory_limit='{mem}'; SET preserve_insertion_order=false; "
                f"SET temp_directory='{st}/tmp'")
    schema = node_schema(csvdir)
    plan: dict[str, Any] = {"nodes": {}, "rels": {}}
    con.execute("CREATE TABLE gl (gid BIGINT, tab VARCHAR)")
    for f in sorted(csvdir.glob("n_*.csv")):
        t = NODE_TABLE[f.stem[2:].replace("_", ":")]
        have = {h.partition(":")[0]: h for h in _header(f)[2:]}
        sel = ['CAST("gid:ID(V)" AS BIGINT) AS gid']
        for k, ty in schema[t][1:]:
            src = f'"{have[k]}"' if k in have else "NULL"
            sel.append(f"CAST({src} AS {DK_T[ty]}) AS \"{k}\"")
        out = st / f"n_{t}.parquet"
        con.execute(f"COPY (SELECT {', '.join(sel)} FROM {_csv(f)}) TO '{out}' (FORMAT parquet)")
        n = con.execute(f"SELECT count(*) FROM '{out}'").fetchone()[0]
        con.execute(f"INSERT INTO gl SELECT gid, '{t}' FROM '{out}'")
        plan["nodes"][t] = {"file": out.name, "rows": n, "csv": f.name}
    for f in sorted(csvdir.glob("r_*.csv")):
        rel = f.stem[2:]
        hdr = _header(f)
        have = {h.partition(":")[0]: h for h in hdr[3:]}
        cols = REL_COLS + EXTRA_REL_COLS.get(rel, [])
        sel = []
        for k, ty in cols:
            src = f'"{have[k]}"' if k in have else "NULL"
            sel.append(f"CAST({src} AS {DK_T[ty]}) AS \"{k}\"")
        tmp = st / f"r_{rel}.all.parquet"
        con.execute(f"COPY (SELECT CAST(\"{hdr[0]}\" AS BIGINT) AS \"from\", CAST(\"{hdr[1]}\" AS BIGINT) AS \"to\", "
                    f"{', '.join(sel)} FROM {_csv(f)}) TO '{tmp}' (FORMAT parquet)")
        pairs = con.execute(f"SELECT s.tab, d.tab, count(*) FROM '{tmp}' r JOIN gl s ON s.gid = r.\"from\" "
                            f"JOIN gl d ON d.gid = r.\"to\" GROUP BY 1, 2 ORDER BY 1, 2").fetchall()
        total = con.execute(f"SELECT count(*) FROM '{tmp}'").fetchone()[0]
        if sum(p[2] for p in pairs) != total:
            raise SystemExit(f"{rel}: {total} rows but {sum(p[2] for p in pairs)} with both endpoints known")
        plan["rels"][rel] = {"csv": f.name, "rows": total, "pairs": []}
        names = ", ".join(['r."from"', 'r."to"'] + [f'r."{k}"' for k, _ in cols])
        for s, d, n in pairs:
            out = st / f"r_{rel}__{s}__{d}.parquet"
            con.execute(f"COPY (SELECT {names} FROM '{tmp}' r JOIN gl s ON s.gid = r.\"from\" AND s.tab = '{s}' "
                        f"JOIN gl d ON d.gid = r.\"to\" AND d.tab = '{d}') TO '{out}' (FORMAT parquet)")
            plan["rels"][rel]["pairs"].append([s, d, n, out.name])
        tmp.unlink()
    # the read-back's expected sums, per Neo4j label and type, over the staged input
    exp = {}
    sums = ("count(*), sum(tt_s // 1000000), sum(CASE WHEN tt_e = {o} THEN 0 ELSE tt_e // 1000000 END), "
            "sum(vt_s // 1000000)").format(o=OPEN)
    for t, v in plan["nodes"].items():
        exp[t] = [int(x or 0) for x in con.execute(f"SELECT {sums} FROM '{st / v['file']}'").fetchone()]
    for rel, v in plan["rels"].items():
        acc = [0, 0, 0, 0]
        for _, _, _, fn in v["pairs"]:
            r = con.execute(f"SELECT {sums} FROM '{st / fn}'").fetchone()
            acc = [x + int(y or 0) for x, y in zip(acc, r)]
        exp[rel] = acc
    con.close()
    (st / "stage.duckdb").unlink(missing_ok=True)
    shutil.rmtree(st / "tmp", ignore_errors=True)
    plan["schema"] = schema
    plan["expect"] = exp
    return plan


def ddl(plan: dict) -> list[str]:
    out = []
    for t, cols in plan["schema"].items():
        out.append(f"CREATE NODE TABLE {t}(" + ", ".join(f"{k} {ty}" for k, ty in cols) + ", PRIMARY KEY (gid))")
    for rel, v in plan["rels"].items():
        pairs = ", ".join(f"FROM {s} TO {d}" for s, d, _, _ in v["pairs"])
        cols = REL_COLS + EXTRA_REL_COLS.get(rel, [])
        out.append(f"CREATE REL TABLE {rel}({pairs}, " + ", ".join(f"{k} {ty}" for k, ty in cols) + ")")
    return out


def readback(conn, plan: dict) -> dict[str, Any]:
    sec = lambda v: f"{v} / 1000000"   # noqa: E731  (INT64 division truncates, as DuckDB's //)
    agg = lambda v: (f"count({v}) AS c, sum({sec(v + '.tt_s')}) AS a, "
                     f"sum(CASE WHEN {v}.tt_e = {OPEN} THEN 0 ELSE {sec(v + '.tt_e')} END) AS b, "
                     f"sum({sec(v + '.vt_s')}) AS d")
    out = {}
    for t in plan["nodes"]:
        _, r = rows(conn, f"MATCH (n:{t}) RETURN " + agg("n"))
        out[t] = {"db": [int(x or 0) for x in r[0]], "input": plan["expect"][t]}
    for rel in plan["rels"]:
        _, r = rows(conn, f"MATCH ()-[x:{rel}]->() RETURN " + agg("x"))
        out[rel] = {"db": [int(x or 0) for x in r[0]], "input": plan["expect"][rel]}
    return out


def db_settings(db) -> dict[str, Any]:
    out = {"ladybug": _lb_version(), "buffer_pool_rule": BUFFER_POOL_RULE}
    for k in ("buffer_pool_size", "max_num_threads", "compression", "auto_checkpoint", "checkpoint_threshold",
              "enable_checksums", "read_only"):
        try:
            out[k] = getattr(db, k)
        except Exception as e:                                    # noqa: BLE001
            out[k] = f"? {e}"[:80]
    return out


def cmd_load(a) -> int:
    t0 = time.time()
    db_path = Path(a.db)
    if db_path.exists():
        raise SystemExit(f"{db_path} exists; refusing to load over it")
    db_path.parent.mkdir(parents=True, exist_ok=True)
    st = Path(a.stage)
    plan = stage(Path(a.csv), st, a.threads, a.duck_mem)
    t1 = time.time()
    db, conn = open_db(str(db_path), read_only=False, threads=a.threads)
    rec: dict[str, Any] = {"system": "ladybug", "method": "bulk: COPY FROM Parquet (staged from the graph CSVs)",
                           "csv": a.csv, "stage_s": round(t1 - t0, 1), "settings": db_settings(db), "copy": {}}
    for s in ddl(plan):
        conn.execute(s)
    for t, v in plan["nodes"].items():
        tc = time.time()
        conn.execute(f"COPY {t} FROM '{st / v['file']}'")
        rec["copy"][t] = round(time.time() - tc, 2)
    for rel, v in plan["rels"].items():
        for s, d, _, fn in v["pairs"]:
            tc = time.time()
            conn.execute(f"COPY {rel} FROM '{st / fn}' (from='{s}', to='{d}')")
            rec["copy"][f"{rel}:{s}->{d}"] = round(time.time() - tc, 2)
    conn.execute("CHECKPOINT")
    t2 = time.time()
    rec["copy_from_s"] = round(t2 - t1, 1)
    rec["counts"] = readback(conn, plan)
    rec["verified"] = all(v["db"] == v["input"] for v in rec["counts"].values())
    rec["readback_s"] = round(time.time() - t2, 1)
    conn.close()
    db.close()
    rec["bytes"] = sum(p.stat().st_size for p in db_path.parent.glob(db_path.name + "*"))
    rec["load_s"] = round(t2 - t1, 1)
    rec["schema"] = plan["schema"]
    rec["rel_pairs"] = {r: [p[:3] for p in v["pairs"]] for r, v in plan["rels"].items()}
    if not a.keep_stage:
        shutil.rmtree(st, ignore_errors=True)
    Path(a.out).write_text(json.dumps(rec, indent=1, default=str) + "\n")
    print(json.dumps({k: v for k, v in rec.items() if k not in ("counts", "schema", "copy", "rel_pairs")}, default=str))
    return 0 if rec["verified"] else 1


# ------------------------------------------------------------------------------------------- replay (ingest) ---

def _arrow(rows_: list[dict], cols: list[tuple[str, str]]):
    import pyarrow as pa
    pt = {"STRING": pa.string(), "INT64": pa.int64(), "DOUBLE": pa.float64(), "BOOLEAN": pa.bool_(),
          "TIMESTAMP": pa.timestamp("us"), "DATE": pa.date32()}
    return pa.table({k: pa.array([r.get(k) for r in rows_], pt[ty]) for k, ty in cols})


def cmd_replay(a) -> int:
    """Ingest study: the snapshot (tt <= T0) through COPY FROM, then one LadybugDB transaction per transaction time
    above T0 (creates of the versions and copies with tt_s = tt, closes of those with tt_e = tt), as
    w3_snb_ingest.replay_graph does for Neo4j and Memgraph. `--prefix-days N` stops at the Nth day's corrections."""
    import w3_snb_ingest as I
    import w3_snb_systems as Y
    w0 = time.time()
    t0 = I.t0_of(a.base_meta, False)
    L, ends = I.prefix_of(a.base_meta, a.prefix_days)
    duck = Y.graph_tables(a.hist)
    tts = [r[0] for r in duck.execute(
        "SELECT DISTINCT tt FROM (SELECT tt_s AS tt FROM nodes UNION SELECT tt_e FROM nodes WHERE tt_e < "
        f"{OPEN} UNION SELECT tt_s FROM rels UNION SELECT tt_e FROM rels WHERE tt_e < {OPEN}) WHERE tt > {t0} "
        f"AND tt <= {L} ORDER BY 1").fetchall()]
    csvdir = Path(a.stage) / "csv"
    tc = time.time()
    I.write_csvs(duck, t0, csvdir)
    prepare_s = round(time.time() - tc, 1)
    db_path = Path(a.db)
    if db_path.exists():
        raise SystemExit(f"{db_path} exists")
    tb = time.time()
    plan = stage(csvdir, Path(a.stage) / "pq", a.threads, a.duck_mem)
    # every table and pair the whole log uses must exist before the stream (the snapshot may lack some pairs)
    gtab = {r[0]: NODE_TABLE[r[1]] for r in duck.execute("SELECT gid, lab FROM nodes").fetchall()}
    allpairs: dict[str, set] = {}
    for rel, ga, gb in duck.execute("SELECT DISTINCT rel, a, b FROM rels").fetchall():
        allpairs.setdefault(rel, set()).add((gtab[ga], gtab[gb]))
    for rel, ps in allpairs.items():
        have = {(s, d) for s, d, _, _ in plan["rels"].get(rel, {"pairs": []})["pairs"]}
        plan["rels"].setdefault(rel, {"pairs": []})
        for s, d in sorted(ps - have):
            plan["rels"][rel]["pairs"].append([s, d, 0, None])
    staged_s = round(time.time() - tb, 1)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    db, conn = open_db(str(db_path), read_only=False, threads=a.threads)
    rec: dict[str, Any] = {"system": "ladybug", "t0": t0, "prefix_days": a.prefix_days, "until_tt": L,
                           "transactions": len(tts), "prepare_s": prepare_s, "stage_s": staged_s,
                           "settings": db_settings(db),
                           "method": "bulk: COPY FROM Parquet (staged from the graph CSVs); transactions: one "
                                     "explicit LadybugDB transaction per tt (LOAD FROM Arrow + CREATE / MATCH SET)"}
    tl = time.time()
    for s in ddl(plan):
        conn.execute(s)
    st = Path(a.stage) / "pq"
    for t, v in plan["nodes"].items():
        conn.execute(f"COPY {t} FROM '{st / v['file']}'")
    for rel, v in plan["rels"].items():
        for s, d, n, fn in v["pairs"]:
            if fn:
                conn.execute(f"COPY {rel} FROM '{st / fn}' (from='{s}', to='{d}')")
    conn.execute("CHECKPOINT")
    rec["bulk_s"] = round(time.time() - tl, 1)
    rec["bulk_rows"] = {"nodes": sum(v["rows"] for v in plan["nodes"].values()),
                        "rels": sum(n for v in plan["rels"].values() for _, _, n, _ in v["pairs"])}
    schema = plan["schema"]
    rcols = {rel: [("src_g", "INT64"), ("dst_g", "INT64")] + REL_COLS + EXTRA_REL_COLS.get(rel, []) for rel in plan["rels"]}
    tx0 = time.time()
    ins = clo = 0
    per_tx = []
    for tt in tts:
        q0, i0, c0 = time.time(), ins, clo
        conn.execute("BEGIN TRANSACTION")
        try:
            for lab in [r[0] for r in duck.execute(f"SELECT DISTINCT lab FROM nodes WHERE tt_s = {tt} ORDER BY 1").fetchall()]:
                t = NODE_TABLE[lab]
                got = duck.execute(f"SELECT gid, lab, id, vt_s, vt_e, tt_s, {OPEN}, props FROM nodes "
                                   f"WHERE tt_s = {tt} AND lab = '{lab}' ORDER BY gid").fetchall()
                tbl = _arrow([Y.node_dict(*x) for x in got], schema[t])   # noqa: F841 (scanned by name below)
                conn.execute(f"LOAD FROM tbl CREATE (n:{t} {{" + ", ".join(f"{k}: {k}" for k, _ in schema[t]) + "})")
                ins += len(got)
            for rel in sorted(plan["rels"]):
                got = duck.execute(f"SELECT {', '.join(I.RCOLS)} FROM rels WHERE tt_s = {tt} AND rel = '{rel}'").fetchall()
                if not got:
                    continue
                by: dict[tuple, list] = {}
                for x in got:
                    r = Y.rel_dict(dict(zip(I.RCOLS, x)))
                    r["tt_e"] = OPEN
                    r["creationDate"] = r.pop("cd", None)
                    r["src_g"], r["dst_g"] = r.pop("a"), r.pop("b")
                    by.setdefault((gtab[r["src_g"]], gtab[r["dst_g"]]), []).append(r)
                for (s, d), rs in by.items():
                    tbl = _arrow(rs, rcols[rel])   # noqa: F841
                    props = ", ".join(f"{k}: {k}" for k, _ in REL_COLS + EXTRA_REL_COLS.get(rel, []))
                    # one MATCH per endpoint: `MATCH (a), (b) WHERE a.gid = .. AND b.gid = ..` is planned as a cross
                    # product of the two node tables; separate clauses give primary-key lookups
                    conn.execute(f"LOAD FROM tbl WITH * MATCH (a:{s}) WHERE a.gid = src_g WITH * MATCH (b:{d}) "
                                 f"WHERE b.gid = dst_g CREATE (a)-[:{rel} {{{props}}}]->(b)")
                ins += len(got)
            for rel in sorted(plan["rels"]):
                got = duck.execute(f"SELECT a, b, rid FROM rels WHERE tt_e = {tt} AND rel = '{rel}'").fetchall()
                by = {}
                for ga, gb, rid in got:
                    by.setdefault((gtab[ga], gtab[gb]), []).append([ga, rid])
                for (s, d), rs in by.items():
                    # source by primary key, then its edges (the planner still scans the rel table once per statement)
                    conn.execute(f"UNWIND $rows AS r MATCH (a:{s}) WHERE a.gid = r[1] WITH a, r "
                                 f"MATCH (a)-[x:{rel}]->(b:{d}) WHERE x.rid = r[2] "
                                 "SET x.tt_e = $tt", {"rows": rs, "tt": int(tt)})
                clo += len(got)
            for t in NODE_TABLE.values():
                lab = LABEL_OF[t]
                gids = [r[0] for r in duck.execute(f"SELECT gid FROM nodes WHERE tt_e = {tt} AND lab = '{lab}'").fetchall()]
                if gids:
                    conn.execute(f"UNWIND $g AS g MATCH (n:{t}) WHERE n.gid = g SET n.tt_e = $tt", {"g": gids, "tt": int(tt)})
                clo += len(gids)
            conn.execute("COMMIT")
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except RuntimeError:        # the engine already ended the transaction (e.g. on a buffer-pool error)
                pass
            raise
        # LB-4: the client's implicit prepared-statement cache holds buffer-pool memory; the statements above are
        # parameter-stable (a handful of texts), and the cache is cleared after each transaction regardless
        getattr(conn, "_pybind_implicit_prepared_cache", {}).clear()
        per_tx.append([tt, I._txkind(tt, ends), ins - i0, clo - c0, round(time.time() - q0, 3)])
    rec.update(transactions_s=round(time.time() - tx0, 1), inserted=ins, closed=clo, per_tx=per_tx,
               by_kind=I._by_kind(per_tx))
    tk = time.time()
    conn.execute("CHECKPOINT")
    rec["final_checkpoint_s"] = round(time.time() - tk, 1)
    # verification against the expected state at L (w3_snb_ingest.graph_counts' sums, per LadybugDB table)
    exp = {}
    hagg = (f"count(*), sum(tt_s // 1000000), sum(CASE WHEN tt_e = {OPEN} THEN 0 ELSE tt_e // 1000000 END), "
            f"sum(vt_s // 1000000)")
    for lab, t in NODE_TABLE.items():
        exp[t] = [int(x or 0) for x in duck.execute(
            f"SELECT {hagg} FROM " + I._expect(f"(SELECT * FROM nodes WHERE lab = '{lab}')", L)).fetchone()]
    for rel in plan["rels"]:
        exp[rel] = [int(x or 0) for x in duck.execute(
            f"SELECT {hagg} FROM " + I._expect(f"(SELECT * FROM rels WHERE rel = '{rel}')", L)).fetchone()]
    plan["expect"] = exp
    rec["counts"] = readback(conn, plan)
    rec["verified"] = all(v["db"] == v["input"] for v in rec["counts"].values())
    conn.close()
    db.close()
    rec["bytes"] = sum(p.stat().st_size for p in db_path.parent.glob(db_path.name + "*"))
    rec["load_s"] = round(time.time() - w0 - prepare_s, 1)
    if not a.keep_stage:
        shutil.rmtree(a.stage, ignore_errors=True)
    Path(a.out).write_text(json.dumps(rec, indent=1, default=str) + "\n")
    print(json.dumps({k: v for k, v in rec.items() if k not in ("counts", "per_tx")}, default=str))
    return 0 if rec["verified"] else 1


# ---------------------------------------------------------------------------------------------------- digest ---

def _norm(v):
    """w3_snb_ingest._norm on the values the Neo4j driver returns: LocalDateTime and Date go through
    neo4j.time's iso_format, exactly as in the Neo4j and Memgraph digests."""
    import neo4j.time as nt
    if isinstance(v, dt.datetime):
        return nt.DateTime.from_native(v).iso_format()
    if isinstance(v, dt.date):
        return nt.Date.from_native(v).iso_format()
    return v


def _digest_part(job) -> tuple[str, list[int]]:
    import w3_snb_ingest as I
    path, kind, tab, extra, lo, hi = job
    # each worker opens its own read-only database: a small pool per worker keeps procs x pool inside the job's
    # memory
    bp = int(float(os.environ.get("W3_LADYBUG_DIGEST_BP_GB", "2")) * (1 << 30))
    db, conn = open_db(path, read_only=True, threads=2, buffer_pool=bp)
    M = 1 << 64
    n = acc = 0
    if kind == "node":
        label = ":".join(sorted(["V"] + LABEL_OF[tab].split(":")))
        names = [k for k, _ in extra]
        r = conn.execute(f"MATCH (n:{tab}) WHERE n.gid >= {lo} AND n.gid < {hi} RETURN "
                         + ", ".join(f"n.{k}" for k in names))
        while r.has_next():
            x = r.get_next()
            props = sorted((k, _norm(v)) for k, v in zip(names, x) if v is not None)
            acc = (acc + I._h(label, props)) % M
            n += 1
        key = label
    else:
        rel, s, d, names = tab, extra[0], extra[1], extra[2]
        r = conn.execute(f"MATCH (a:{s})-[x:{rel}]->(b:{d}) WHERE a.gid >= {lo} AND a.gid < {hi} "
                         f"RETURN a.gid, b.gid, " + ", ".join(f"x.{k}" for k in names))
        while r.has_next():
            x = r.get_next()
            props = sorted((k, _norm(v)) for k, v in zip(names, x[2:]) if v is not None and k != "rid")
            acc = (acc + I._h(rel, x[0], x[1], props)) % M
            n += 1
        key = rel
    conn.close()
    db.close()
    return key, [n, acc]


def cmd_digest(a) -> int:
    """w3_snb_ingest.py digest's Cypher form: per label set (sorted, `:`-joined, with V) and per relationship type,
    the count and the sum mod 2^64 of a hash of every element (labels or type, endpoint gids, every non-null
    property but the loader's row key rid). Equal to the Neo4j and Memgraph digests when the stores hold the same
    versions."""
    import multiprocessing as mp
    from concurrent.futures import ProcessPoolExecutor
    t0 = time.time()
    db, conn = open_db(a.db, read_only=True)
    _, tabs = rows(conn, "CALL show_tables() RETURN name, type")
    jobs = []
    # a query result is materialised in memory, so each part is cut into source-gid ranges of about CHUNK rows
    chunk = int(os.environ.get("W3_LADYBUG_DIGEST_CHUNK", "4000000"))

    def ranges(n, g0, g1):
        k = max(1, -(-int(n) // chunk))
        step = -(-(int(g1) + 1 - int(g0)) // k)
        return [(int(g0) + i * step, int(g0) + (i + 1) * step) for i in range(k)]
    for name, typ in tabs:
        _, info = rows(conn, f"CALL table_info('{name}') RETURN name, type")
        if typ == "NODE":
            n, g0, g1 = rows(conn, f"MATCH (n:{name}) RETURN count(*), min(n.gid), max(n.gid)")[1][0]
            if n:
                jobs += [(a.db, "node", name, info, lo, hi) for lo, hi in ranges(n, g0, g1)]
        elif typ == "REL":
            _, conns = rows(conn, f"CALL show_connection('{name}') RETURN *")
            names = [k for k, _ in info if k not in [c for c, _ in EXTRA_REL_COLS.get(name, [])]]
            for c in conns:
                n, g0, g1 = rows(conn, f"MATCH (a:{c[0]})-[x:{name}]->(b:{c[1]}) RETURN count(*), min(a.gid), "
                                       "max(a.gid)")[1][0]
                if n:
                    jobs += [(a.db, "rel", name, (c[0], c[1], names), lo, hi) for lo, hi in ranges(n, g0, g1)]
    conn.close()
    db.close()
    M = 1 << 64
    out: dict[str, list[int]] = {}
    # ProcessPoolExecutor, not Pool: a worker that dies raises BrokenProcessPool instead of hanging the digest
    with ProcessPoolExecutor(a.procs, mp_context=mp.get_context("spawn")) as pool:
        for key, (n, h) in pool.map(_digest_part, jobs):
            c = out.setdefault(key, [0, 0])
            c[0] += n
            c[1] = (c[1] + h) % M
    Path(a.out).write_text(json.dumps(out, indent=1, sort_keys=True) + "\n")
    print(json.dumps({"system": "ladybug", "parts": len(out), "rows": sum(v[0] for v in out.values()),
                      "digest_s": round(time.time() - t0, 1)}))
    return 0


def cmd_cmp(a) -> int:
    x, y = json.loads(Path(a.a).read_text()), json.loads(Path(a.b).read_text())
    diff = sorted(k for k in set(x) | set(y) if x.get(k) != y.get(k))
    print(json.dumps({"a": a.a, "b": a.b, "parts": len(set(x) | set(y)), "equal": not diff,
                      "differ": [[k, x.get(k), y.get(k)] for k in diff[:20]]}))
    return 0 if not diff else 1


# ----------------------------------------------------------------------------------------------- query texts ---

W3CY = ROOT / "benchmarks/workloads/snb/cypher"
#: the belief values are written into the text as INT64 literals, not sent as parameters: LadybugDB 0.21.2 fails an
#: assertion on a parameter inside a lambda or a list quantifier (all/any/none/list_filter; PROBLEMS LB-1), and
#: the window macros put them there
INLINE = ("T1", "T", "VA", "VB", "V", "A", "B")


def _one(x: str) -> str:
    return f"({x}.tt_s <= $T AND $T < {x}.tt_e AND {x}.vt_s <= $V AND $V < {x}.vt_e)"


def _at(t: str, x: str) -> str:
    t = t.strip()
    return f"({x}.tt_s <= $T AND $T < {x}.tt_e AND {x}.vt_s <= {t} AND {t} < {x}.vt_e)"


def _parts(args: list[str]) -> tuple[list[str], list[str]]:
    """W_* arguments `a, b | nodes(w) + relationships(w)` -> (['a', 'b'], ['nodes(w)', 'rels(w)'])."""
    from w3_snb_systems import _top_split
    joined = ", ".join(args)
    vs, _, extra = joined.partition("|")
    names = [v.strip() for v in vs.split(",") if v.strip()]
    lists = []
    if extra.strip():
        depth, cur = 0, ""
        for ch in extra:
            depth += ch in "([{"
            depth -= ch in ")]}"
            if ch == "+" and depth == 0:
                lists.append(cur.strip()); cur = ""
            else:
                cur += ch
        lists.append(cur.strip())
    lists = [l.replace("relationships(", "rels(") for l in lists if l]
    for l in [l for l in lists if l.startswith("ends(")]:
        names += [x.strip() for x in l[5:-1].split(",")]
    lists = [l for l in lists if not l.startswith("ends(")]
    return names, lists


def _vals(col: str, init: str, names: list[str], lists: list[str]) -> str:
    out = "[" + ", ".join([f"CAST({init} AS INT64)"] + [f"{v}.{col}" for v in names]) + "]"
    for l in lists:
        out = f"list_concat({out}, properties({l}, '{col}'))"
    return out


def _bound(which: str, init: str, names: list[str], lists: list[str]) -> str:
    if which == "lo":
        return f"list_reduce({_vals('vt_s', init, names, lists)}, (_a, _b) -> CASE WHEN _b > _a THEN _b ELSE _a END)"
    return f"list_reduce({_vals('vt_e', init, names, lists)}, (_a, _b) -> CASE WHEN _b < _a THEN _b ELSE _a END)"


def _believed(names: list[str], lists: list[str]) -> str:
    out = [f"({v}.tt_s <= $T AND $T < {v}.tt_e)" for v in names]
    out += [f"all(_x IN {l} WHERE _x.tt_s <= $T AND $T < _x.tt_e)" for l in lists]
    return " AND ".join(out) if out else "true"


def _ok(args: list[str], lo0: str = "$VA", hi0: str = "$VB") -> str:
    names, lists = _parts(args)
    if not names and not lists:
        return "true"
    return f"({_believed(names, lists)} AND {_bound('lo', lo0, names, lists)} < {_bound('hi', hi0, names, lists)})"


#: the end of an open interval in W_EXT's [lo, hi] pairs: 2^61, not OPEN (2^62), so the sweep's 2e + 1 fits INT64
#: (every window bound is far below it)
EXT_CAP = 1 << 61


def _uncovered(lo: str, hi: str, ext: str) -> str:
    """W1's NOT (SEMANTICS rule 5): some instant of [lo, hi) is covered by no [s, e) of `ext`. The list gets one more
    interval, [0, lo), so no instant before lo can be a gap; then a sweep over the sorted end points (start = 2s,
    end = 2e + 1: a start sorts before an end at the same instant): the list_reduce returns the running count of
    open intervals, or -g once the count first falls to 0 at g. Uncovered iff g is below hi (g >= lo always).
    Written around LadybugDB 0.21.2 defects (PROBLEMS P18): no lambda reads lo or hi (LB-10: a lambda reading an
    outer variable in a grouped pipeline reads it wrongly; list_filter too, W3 IC4 window); ext is not coalesced
    (LB-12: list_filter over coalesce(<NULL list>, []) returns other rows' elements); the end points are taken out
    as flat lists before any arithmetic, and no lambda compares with a literal (LB-13: arithmetic on an element of
    a collected list of lists, and a CASE on a list element against a large literal, give wrong values, W3 BI18 and
    IC7 windows). (LadybugDB sorts no list of lists, LB-2, so the candidate-instant form of the Neo4j text is not
    available.) Elements need not overlap [lo, hi): one before lo merges into the sentinel, one after hi lies past
    the first gap. Ends must be <= EXT_CAP (W_EXT caps them; walk intervals lie inside the window)."""
    full = f"(CASE WHEN ({ext}) IS NULL THEN [[CAST(0 AS INT64), ({lo})]] ELSE list_concat([[CAST(0 AS INT64), ({lo})]], {ext}) END)"
    st = f"list_transform({full}, _e -> _e[1])"
    en = f"list_transform({full}, _e -> _e[2])"
    ev = f"list_sort(list_concat(list_transform({st}, _x -> 2 * _x), list_transform({en}, _x -> 2 * _x + 1)))"
    gap = (f"list_reduce(list_concat([CAST(0 AS INT64)], {ev}), (_a, _x) -> CASE WHEN _a < 0 THEN _a "
           f"WHEN _x % 2 = 0 THEN _a + 1 WHEN _a = 1 THEN -(_x / 2) ELSE _a - 1 END)")
    return f"(-({gap}) < ({hi}))"


#: the LB-3 workaround applies to a MATCH with at least this many relationship patterns (env LADYBUG_DEFER_MIN)
DEFER_MIN = int(os.environ.get("LADYBUG_DEFER_MIN", "3"))
#: the LB-3 workaround in use: "hops" (every MATCH cut into one MATCH per hop; default) or "defer"
STAGING = os.environ.get("LADYBUG_STAGING", "hops")
OPT_BARRIER = os.environ.get("LADYBUG_OPT_BARRIER", "1") == "1"
QUANT_REWRITE = os.environ.get("LADYBUG_QUANT_REWRITE", "1") == "1"
_KW = ("OPTIONAL MATCH", "MATCH", "WHERE", "WITH", "RETURN", "UNWIND", "ORDER BY", "LIMIT", "SKIP", "SET", "UNION",
       "CALL")


def _top_keywords(text: str) -> list[tuple[int, str]]:
    """(position, keyword) of every clause keyword at bracket depth 0 (outside strings)."""
    import re
    out, depth, i, quote = [], 0, 0, None
    pat = re.compile(r"\b(" + "|".join(k.replace(" ", r"\s+") for k in _KW) + r")\b", re.I)
    while i < len(text):
        ch = text[i]
        if quote:
            if ch == quote:
                quote = None
            i += 1
            continue
        if ch in "'\"`":
            quote = ch
        elif ch in "([{":
            depth += 1
        elif ch in ")]}":
            depth -= 1
        elif depth == 0 and (i == 0 or not (text[i - 1].isalnum() or text[i - 1] == "_")):
            m = pat.match(text, i)
            if m:
                out.append((i, re.sub(r"\s+", " ", m.group(1).upper())))
                i = m.end()
                continue
        i += 1
    return out


def _conjuncts(expr: str) -> list[str]:
    import re
    out, depth, cur, i = [], 0, "", 0
    while i < len(expr):
        ch = expr[i]
        if ch in "([{":
            depth += 1
        elif ch in ")]}":
            depth -= 1
        if depth == 0 and re.match(r"\s+AND\s+", expr[i:], re.I) and not cur.upper().rstrip().endswith("BETWEEN"):
            m = re.match(r"\s+AND\s+", expr[i:], re.I)
            out.append(cur.strip()); cur = ""; i += m.end()
            continue
        cur += ch
        i += 1
    if cur.strip():
        out.append(cur.strip())
    return out


def defer_rel_filters(text: str) -> str:
    """LB-3 workaround: in a MATCH (not OPTIONAL MATCH), a predicate on a relationship that has
    relationships on both sides in the chain is evaluated wrongly by LadybugDB 0.21.2 (matches lost or added).
    Every predicate that reads a relationship variable of the MATCH moves to a following `WITH * WHERE`, which
    LadybugDB evaluates after the join (node predicates stay where they are). Same rows, same meaning."""
    import re
    kws = _top_keywords(text)
    out, last = [], 0
    for n, (pos, kw) in enumerate(kws):
        if kw != "MATCH" or n + 1 >= len(kws) or kws[n + 1][1] != "WHERE":
            continue
        pat_s, wpos = pos + len("MATCH"), kws[n + 1][0]
        wend = kws[n + 2][0] if n + 2 < len(kws) else len(text)
        pattern = text[pat_s:wpos]
        cnt = [0]

        def _name(m, kind):
            cnt[0] += 1
            return f"{m.group(1)}_{kind}{n}x{cnt[0]}:"
        named = re.sub(r"(\[)\s*:", lambda m: _name(m, "r"), pattern)
        named = re.sub(r"(\()\s*:", lambda m: _name(m, "a"), named)
        rels = set(re.findall(r"\[\s*(\w+)\s*:", named))
        if len(re.findall(r"\[", re.sub(r"\([^()]*\|", "(", named))) < DEFER_MIN:
            continue        # the defect needs a relationship with relationships on both sides
        where = text[wpos + len("WHERE"):wend]
        stay, move = [], []
        for c in _conjuncts(where):
            m = re.fullmatch(r"VIS\((.*)\)", c, re.S)
            if m and "(" not in m.group(1):
                args = [x.strip() for x in m.group(1).split(",")]
                a_r = [x for x in args if x in rels]
                a_n = [x for x in args if x not in rels]
                if a_n:
                    stay.append(f"VIS({', '.join(a_n)})")
                if a_r:
                    move.append(f"VIS({', '.join(a_r)})")
            elif any(re.search(rf"\b{re.escape(r)}\b", c) for r in rels):
                move.append(c)
            else:
                stay.append(c)
        if not move:
            continue
        new = (f"WHERE {' AND '.join(stay)}\n" if stay else "") + f"WITH * WHERE {' AND '.join(move)}\n"
        out.append(text[last:pat_s])
        out.append(named)
        out.append(new)
        last = wend
    out.append(text[last:])
    return "".join(out)



def _split_top(text: str, sep: str = ",") -> list[str]:
    out, depth, cur, quote = [], 0, "", None
    for ch in text:
        if quote:
            cur += ch
            if ch == quote:
                quote = None
            continue
        if ch in "'\"`":
            quote = ch
        elif ch in "([{":
            depth += 1
        elif ch in ")]}":
            depth -= 1
        if ch == sep and depth == 0:
            out.append(cur); cur = ""
        else:
            cur += ch
    out.append(cur)
    return out


def _path_elems(path: str) -> list[str] | None:
    """A linear path pattern -> [node, rel, node, rel, node ...] (each a text), or None if it is not one."""
    out, i, n = [], 0, len(path)

    def take(open_ch, close_ch, j):
        depth, quote = 0, None
        for k in range(j, n):
            ch = path[k]
            if quote:
                if ch == quote:
                    quote = None
                continue
            if ch in "'\"`":
                quote = ch
            elif ch in "([{":
                depth += 1
            elif ch in ")]}":
                depth -= 1
                if depth == 0:
                    return k + 1
        return -1
    while i < n:
        if path[i].isspace():
            i += 1
            continue
        if path[i] == "(":
            j = take("(", ")", i)
            if j < 0:
                return None
            out.append(path[i:j].strip()); i = j
        elif path[i] in "<-":
            k = path.find("[", i)
            if k < 0 or path[i:k].strip() not in ("-", "<-"):
                return None
            j = take("[", "]", k)
            if j < 0:
                return None
            m = re.match(r"\s*(->|-)", path[j:])
            if not m:
                return None
            out.append(path[i:j + m.end()].strip()); i = j + m.end()
        else:
            return None
    if len(out) % 2 == 0 or not out:
        return None
    return out


def unname_paths(text: str) -> str:
    """LadybugDB cannot carry a path variable through WITH ("Expression in WITH must be aliased"), and the LB-3
    workaround needs WITH. A named variable-length path `w = (A)-[:T* ...]-(B)` becomes `(A)-[_pw:T* ...]-(B)`:
    nodes(w) is the step's intermediate nodes plus its endpoints (`nodes(_pw) + ends(A, B)` in a macro list),
    relationships(w) is rels(_pw), length(w) is length(_pw). Same elements, same meaning (D-W3-13)."""
    out = text
    for m in list(re.finditer(r"\b(\w+)\s*=\s*(?=\()", text))[::-1]:
        name, start = m.group(1), m.end()
        rest = text[start:]
        kw = _top_keywords(rest)
        seg = _split_top(rest[: kw[0][0]] if kw else rest)[0]
        el = _path_elems(seg.strip()) if seg.strip() else None
        if el is None or len(el) != 3 or "*" not in el[1]:
            continue
        a = re.match(r"\(\s*(\w+)", el[0]); b = re.match(r"\(\s*(\w+)", el[2])
        if not a or not b:
            continue
        rv = re.match(r"(<?-\[)\s*(\w*)", el[1])
        var = rv.group(2) or f"_p{name}"
        rel = el[1] if rv.group(2) else el[1][:rv.end(1)] + var + el[1][rv.end(1):]
        new_seg = f"{el[0]}{rel}{el[2]}" + seg[len(seg.rstrip()):]
        seg_end = start + len(seg)
        body = text[:m.start()] + new_seg + text[seg_end:]
        ends = f"ends({a.group(1)}, {b.group(1)})"
        body = re.sub(rf"\bnodes\(\s*{name}\s*\)", f"nodes({var}) + {ends}", body)
        body = re.sub(rf"\b(relationships|rels)\(\s*{name}\s*\)", f"rels({var})", body)
        body = re.sub(rf"\blength\(\s*{name}\s*\)", f"length({var})", body)
        body = re.sub(rf"(?<![\w.$]){name}\b(?!\s*[(.:=])", var, body)      # the path carried by a WITH
        text = out = body
    return out


def stage_hops(text: str) -> str:
    """LB-3 workaround: LadybugDB 0.21.2 loses or adds matches when a relationship filtered in a MATCH
    is followed by further extensions in the same pipeline (consecutive MATCH clauses are one pipeline). Every
    MATCH (not OPTIONAL MATCH) is cut into one MATCH per hop, each followed by `WITH *` and the hop's own predicates:
    predicates on its nodes in the hop's WHERE, predicates that read a relationship in `WITH * WHERE` (evaluated
    after the hop, before the next one). A named path (w = ...) stays one hop. Same rows, same meaning."""
    kws = _top_keywords(text)
    out, last, anon = [], 0, [0]
    for n, (pos, kw) in enumerate(kws):
        if kw == "OPTIONAL MATCH":
            # a barrier after an OPTIONAL MATCH (and its WHERE) that another MATCH follows
            j = n + 1 + (1 if n + 1 < len(kws) and kws[n + 1][1] == "WHERE" else 0)
            if j < len(kws) and kws[j][1] in ("MATCH", "OPTIONAL MATCH") and OPT_BARRIER:
                out.append(text[last:kws[j][0]])
                out.append("WITH *\n")
                last = kws[j][0]
            continue
        if kw != "MATCH":
            continue
        pat_s = pos + len("MATCH")
        nxt = kws[n + 1][0] if n + 1 < len(kws) else len(text)
        has_where = n + 1 < len(kws) and kws[n + 1][1] == "WHERE"
        wend = (kws[n + 2][0] if n + 2 < len(kws) else len(text)) if has_where else nxt
        pattern = text[pat_s:nxt]
        where = text[nxt + len("WHERE"):wend] if has_where else ""
        hops, bound_order = [], []
        ok = True
        rel_vars: set = set()
        for part in _split_top(pattern):
            part = part.strip()
            if not part:
                continue
            if re.match(r"\w+\s*=", part):                     # a named path: one hop
                hops.append(part)
                bound_order.append(set(re.findall(r"[(\[]\s*(\w+)", part)))
                rel_vars |= set(re.findall(r"\[\s*(\w+)", part)) | {part.split("=")[0].strip()}
                continue
            el = _path_elems(part)
            if el is None:
                ok = False
                break
            names = []
            for k, e in enumerate(el):
                m = re.match(r"(\(|<?-\[)\s*(\w*)", e)
                v = m.group(2)
                if not v or v[0] == ":":
                    anon[0] += 1
                    v = f"_{'a' if k % 2 == 0 else 'r'}{anon[0]}"
                    e = e[:m.end(1)] + v + e[m.end(1):]
                    el[k] = e
                names.append(v)
                if k % 2:
                    rel_vars.add(v)
            if len(el) == 1:
                hops.append(el[0]); bound_order.append({names[0]})
                continue
            seen = set(x for b in bound_order for x in b)
            for k in range(1, len(el), 2):
                a = el[k - 1] if names[k - 1] not in seen else f"({names[k - 1]})"
                b = el[k + 1] if names[k + 1] not in seen else f"({names[k + 1]})"
                seen |= {names[k - 1], names[k + 1]}
                hops.append(a + el[k] + b)
                bound_order.append({names[k - 1], names[k], names[k + 1]})
        if not ok or len(hops) <= 1 and not rel_vars:
            continue
        allv = set(x for b in bound_order for x in b)
        conj = [c for c in _conjuncts(where)] if where.strip() else []
        per_hop: list[tuple[list, list]] = [([], []) for _ in hops]
        for c in conj:
            m = re.fullmatch(r"VIS\(([^()]*)\)", c.strip())
            pieces = [f"VIS({x.strip()})" for x in m.group(1).split(",")] if m else [c]
            for pc in pieces:
                refs = {v for v in allv if re.search(rf"(?<![\w.]){re.escape(v)}\b", pc)}
                acc, idx = set(), 0
                for idx, b in enumerate(bound_order):
                    acc |= b
                    if refs <= acc:
                        break
                (per_hop[idx][1] if refs & rel_vars else per_hop[idx][0]).append(pc)
        new = []
        for hop, (node_c, rel_c) in zip(hops, per_hop):
            if "*" in re.sub(r"\([^()]*\|.*?\)", "", hop):
                # a recursive step: its own predicate filters it while it expands; no barrier after it (a WITH
                # cannot carry a weighted path's relationship variable: "Cannot evaluate expression with type VARIABLE")
                cs = node_c + rel_c
                new.append(f"MATCH {hop}" + (f" WHERE {' AND '.join(cs)}" if cs else ""))
                continue
            new.append(f"MATCH {hop}" + (f" WHERE {' AND '.join(node_c)}" if node_c else ""))
            new.append("WITH *" + (f" WHERE {' AND '.join(rel_c)}" if rel_c else ""))
        out.append(text[last:pos])
        out.append("\n".join(new) + "\n")
        last = wend
    out.append(text[last:])
    return "".join(out)


def lb3(text: str) -> str:
    """The LB-3 workaround in use (LADYBUG_STAGING: hops | defer | none), after unname_paths."""
    text = unname_paths(text)
    return stage_hops(text) if STAGING == "hops" else (defer_rel_filters(text) if STAGING == "defer" else text)


def _quantifier(kind: str, args: list[str]) -> str:
    """LB-10 workaround: LadybugDB's any() / all() / none() over a list give wrong results when the predicate reads
    a variable of the enclosing query (list_filter with the same lambda is right). The quantifier becomes a size test
    on list_filter: any -> size(...) > 0, none -> size(...) = 0, all -> size(list_filter(NOT ...)) = 0."""
    body = ", ".join(args)
    m = re.match(r"\s*(\w+)\s+IN\s+(.*)$", body, re.S)
    if not m:
        return f"{kind}({body})"
    var, rest = m.group(1), m.group(2)
    kws = [k for k in _top_keywords(rest) if k[1] == "WHERE"]
    if not kws:
        return f"{kind}({body})"
    lst, pred = rest[: kws[0][0]].strip(), rest[kws[0][0] + 5:].strip()
    if kind == "all":
        return f"(size(list_filter({lst}, {var} -> NOT ({pred}))) = 0)"
    if kind == "none":
        return f"(size(list_filter({lst}, {var} -> {pred})) = 0)"
    return f"(size(list_filter({lst}, {var} -> {pred})) > 0)"


def expand(text: str) -> str:
    """The W3 Cypher macros in LadybugDB's form."""
    from w3_snb_systems import macro
    text = macro(text, "VISALL", lambda a: f"all(_x IN {a[0]} WHERE {_one('_x')})")
    text = macro(text, "VISATALL", lambda a: (lambda t, l: f"all(_x IN {l} WHERE {_at(t, '_x')})")(*", ".join(a).split(";", 1)))
    text = macro(text, "VISAT", lambda a: (lambda t, vs: " AND ".join(_at(t, v.strip()) for v in vs.split(",")))(*", ".join(a).split(";", 1)))
    text = macro(text, "VIS", lambda a: " AND ".join(_one(x) for x in a))
    text = macro(text, "WPRUNE", lambda a: " AND ".join(
        f"({x}.tt_s <= $T AND $T < {x}.tt_e AND {x}.vt_s < $VB AND $VA < {x}.vt_e)" for x in a))
    text = macro(text, "W_OK", _ok)
    text = macro(text, "W_LOV", lambda a: _bound("lo", "$VA", *_parts(a)))
    text = macro(text, "W_HIV", lambda a: _bound("hi", "$VB", *_parts(a)))
    text = macro(text, "W_INV", lambda a: _ok(a[2:], a[0], a[1]))
    text = macro(text, "W_EXT", lambda a: "[" + _bound("lo", "-1", *_parts(a)) + ", " + _bound("hi", str(EXT_CAP), *_parts(a)) + "]")
    text = macro(text, "W_UNCOVERED", lambda a: _uncovered(a[0], a[1], a[2]))
    if QUANT_REWRITE:
        for kind in ("any", "all", "none"):
            for _ in range(3):                        # nested quantifiers, inside out after a few passes
                text = macro(text, kind, lambda a, k=kind: _quantifier(k, a))
    return text


def text_for(qid: str, setting: str) -> Path:
    q = qid.replace("TLDBC-", "")
    base = W3CY / "window" / "ladybug" if setting == "window" and not qid.startswith("BIH-") else W3CY / "ladybug"
    return base / f"{q}.cypher"


def bind(qid: str, inst: dict, setting: str) -> tuple[list[str], dict[str, Any]]:
    """The statements of one cell (a text's `// ---` separates statements that run in one transaction, rolled
    back) with the belief values inlined, and the remaining parameters."""
    import re
    from w3_snb_run import query_params, setting_values
    import w3_snb_systems as Y
    f = text_for(qid, setting)
    if not f.exists():
        raise NotImplementedError(f"no LadybugDB text for {qid} ({setting})")
    src = f.read_text()
    if src.startswith("// NOT EXPRESSIBLE"):
        raise NotImplementedError(src.splitlines()[0][3:].strip())
    sv = setting_values(inst, setting)
    vals: dict[str, Any] = {"T": sv["T"], "V": sv["V"], "VA": sv["VA"], "VB": sv["VB"]}
    if qid.startswith("BIH-"):
        vals.update({k: v for k, (_, v) in query_params(qid, inst, setting).items() if k in ("T", "V", "A", "B", "T1")})
    params: dict[str, Any] = {}
    for k, (ty, v) in inst["params"].items():
        params[k] = Y._ldt(v) if ty in ("DATETIME", "DATE") else v
    if "startDate" in params and "durationDays" in params and "endDate" not in params:
        params["endDate"] = params["startDate"] + dt.timedelta(days=int(params["durationDays"]))
    stmts = []
    pieces = re.split(r"\n// ---(?: ARG (\w+))?[^\n]*\n", src)
    args: list[str | None] = [None] + pieces[1::2]
    for part, arg in zip(pieces[0::2], args):
        body = "\n".join(l for l in part.splitlines() if not l.lstrip().startswith("//")).strip()
        if not body:
            continue
        if arg:
            body = f"// ARG {arg}\n" + body
        if "// NO-DEFER" not in part:
            head, sep, rest = body.partition("\n") if body.startswith("// ARG") else ("", "", body)
            body = head + sep + lb3(rest)
        body = expand(body)
        for k in INLINE:
            if k in vals:
                body = re.sub(rf"\${k}\b", str(int(vals[k])), body)
        stmts.append(body)
    used = {k: v for k, v in params.items() if any(re.search(rf"\${k}\b", s) for s in stmts)}
    return stmts, used


def bound_cache(conn, keep: list[str]) -> int:
    """LB-4 workaround: the Python client's implicit prepared-statement cache keeps buffer-pool
    memory for every statement it has prepared, until the pool is full and every later query fails ("Unable to
    allocate memory! The buffer pool is full and no memory could be freed!"). Before a cell runs, every cached
    statement of another cell is closed and dropped, so the cache holds only the current cell's statements: the
    repeated calls of one cell still reuse their prepared statement (as LadybugDB's client does). Returns the
    number dropped."""
    cache = getattr(conn, "_pybind_implicit_prepared_cache", None)
    if not cache:
        return 0
    keep = [x.split("\n", 1)[1] if x.startswith("// ARG ") else x for x in keep]
    lock = getattr(conn, "_prepared_cache_lock", None)
    drop = 0
    with lock if lock is not None else _Null():
        for k in [k for k in cache if k[0] not in keep]:
            p = cache.pop(k)
            close = getattr(p, "close", None)
            if callable(close):
                try:
                    close()
                except Exception:                                # noqa: BLE001
                    pass
            drop += 1
    return drop


class _Null:
    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def run_stmts(conn, stmts: list[str], params: dict[str, Any]):
    """One cell: a single statement; several in one transaction that is rolled back (weights); or rounds, where a
    statement starting `// ARG name` receives the previous statement's first column (every row, as a list) as the
    parameter $name (D-W3-13: LadybugDB starts a recursive pattern from every node of the label unless
    its source is filtered in the same MATCH, so a source set computed by an earlier part is passed as a list)."""
    if len(stmts) == 1:
        return rows(conn, stmts[0], _used(stmts[0], params))
    if any(s.startswith("// ARG ") for s in stmts[1:]):
        p = dict(params)
        res = None
        for s in stmts:
            if s.startswith("// ARG "):
                name = s.split("\n", 1)[0].split()[2]
                p[name] = [r[0] for r in res[1]] if res else []
                s = s.split("\n", 1)[1]
            res = rows(conn, s, _used(s, p))
        return res
    conn.execute("BEGIN TRANSACTION")
    try:
        for s in stmts[:-1]:
            conn.execute(s, _used(s, params))
        return rows(conn, stmts[-1], _used(stmts[-1], params))
    finally:
        try:
            conn.execute("ROLLBACK")
        except Exception:                                        # noqa: BLE001 - an error already rolled it back
            pass


def _used(stmt: str, params: dict[str, Any]) -> dict[str, Any]:
    import re
    return {k: v for k, v in params.items() if re.search(rf"\${k}\b", stmt)}


class LadybugW3:
    """W3 on a LadybugDB store (in-process). The store is opened read-write: the weighted-path texts set their
    weights inside a transaction that is rolled back (nothing is ever committed). Threads: LadybugDB's default
    (W3_LADYBUG_THREADS unset or 0) or the given count."""
    name = "ladybug"

    def __init__(self, target: str):
        from w3_snb_run import CAP_S
        self.threads = int(os.environ.get("W3_LADYBUG_THREADS", "0") or 0)
        self.db, self.conn = open_db(target, read_only=False, threads=self.threads)
        self.conn.set_query_timeout(int(CAP_S * 1000))
        import prime_cap as PC
        PC.hook(lambda sec: self.conn.set_query_timeout(int((sec if sec else CAP_S) * 1000)))
        self.target = target
        self._bound: dict = {}

    def settings(self) -> dict[str, Any]:
        out = db_settings(self.db)
        out.update(threads={"W3_LADYBUG_THREADS": self.threads or "default",
                            "connection_threads": self.conn.num_threads if not callable(getattr(self.conn, "num_threads", None)) else None,
                            "cpus_available": len(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else os.cpu_count()},
                   read_write="weights in rolled-back transactions; nothing committed", belief_values="inlined")
        return out

    def run(self, qid: str, inst: dict, setting: str):
        # the bound text of a cell is computed once (text expansion is harness work, not query work: a timed call's
        # e2e holds LadybugDB's execute and fetch, as for the other systems)
        key = (qid, setting, json.dumps(inst.get("params"), sort_keys=True), inst.get("T"), inst.get("VA"), inst.get("VB"))
        got = self._bound.get(key)
        if got is None:
            if len(self._bound) > 64:
                self._bound.clear()
            got = self._bound[key] = bind(qid, inst, setting)
        stmts, params = got
        bound_cache(self.conn, stmts)
        return run_stmts(self.conn, stmts, params)

    def close(self):
        self.conn.close()
        self.db.close()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("prep-csv"); p.add_argument("--hist", required=True); p.add_argument("--out", required=True)
    for name in ("load", "replay"):
        x = sub.add_parser(name)
        x.add_argument("--db", required=True); x.add_argument("--stage", required=True); x.add_argument("--out", required=True)
        x.add_argument("--threads", type=int, default=int(os.environ.get("SLURM_NTASKS_PER_NODE", "8")))
        x.add_argument("--duck-mem", default=os.environ.get("W3_LADYBUG_DUCK_MEM", "16GB"))
        x.add_argument("--keep-stage", action="store_true")
        if name == "load":
            x.add_argument("--csv", required=True)
        else:
            x.add_argument("--hist", required=True); x.add_argument("--base-meta", required=True)
            x.add_argument("--prefix-days", type=int)
    d = sub.add_parser("digest"); d.add_argument("--db", required=True); d.add_argument("--out", required=True)
    d.add_argument("--procs", type=int, default=8)
    c = sub.add_parser("cmp"); c.add_argument("a"); c.add_argument("b")
    a = ap.parse_args()
    return {"prep-csv": cmd_prep_csv, "load": cmd_load, "replay": cmd_replay, "digest": cmd_digest,
            "cmp": cmd_cmp}[a.cmd](a)


if __name__ == "__main__":
    sys.exit(main())
