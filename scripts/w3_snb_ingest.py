"""W3 ingest, load method v2: a bulk snapshot load, then the stream's transactions through each
system's normal write path.

**This is not an LDBC Benchmark, and nothing produced here is an LDBC Benchmark Result.**

    w3_snb_ingest.py sql   --system duckdb|postgres --hist H --base-meta M (--db FILE | --dsn DSN) --tmp DIR --out REC
    w3_snb_ingest.py graph --system neo4j|memgraph --hist H --base-meta M --srv DIR --uri URI --out REC
                           [--neo4j-sif SIF] [--servers SERVERS_SH]
    (either with --full: every version, closed ones included, through the bulk path; no transactions)
    w3_snb_ingest.py digest --system S --target DB|DSN|URI --out DIGEST.json
    w3_snb_ingest.py cmp DIGEST_A DIGEST_B

**Input.** The history tables of one frozen log (`w3_snb_sql.py hist`, from the replay of that log into Gnomon)
and the log's base meta. T0 is the snapshot's last commit (`meta.snapshot.tt_last`): the snapshot records are
the bulk part of every W3 log, and every later record (a day's transaction, a day's corrections) commits at
a transaction time above T0.

**Phase 1 (bulk).** Every row with tt_s <= T0 is written with its tt_e if tt_e <= T0, and OPEN_END otherwise.
It goes through the system's bulk path:
- DuckDB: INSERT ... SELECT from Parquet;
- PostgreSQL: `COPY ... FROM STDIN`, with the indexes built afterwards;
- Neo4j: `neo4j-admin database import full`, offline, before the server starts;
- Memgraph: `LOAD CSV` into the running server.

**Phase 2 (transactions).** For each distinct transaction time tt > T0, in order, one database transaction:
- it inserts the rows with tt_s = tt, with tt_e = OPEN_END;
- it closes the rows with tt_e = tt (`UPDATE ... SET tt_e`, or Cypher `SET x.tt_e`).

That is the same change set the log's record makes at tt. The final state equals the history tables row for
row. The record holds per-table row counts and checksums, read back from the database and compared with the
history tables (`verified`).

Both phases are timed separately.

**`--full` (the query-stage stores).** The complete version table goes through the bulk path: every node and
edge version with all four timestamps, closed ones included. No transaction follows. The store content is the
replayed store's. `digest` and `cmp` check this at SF0.1 and SF1: order-independent per-table digests of the
full and the replayed store must be equal, and the answers must be equivalent.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import w3_snb_sql as S  # noqa: E402
import w3_snb_systems as Y  # noqa: E402

OPEN = 2 ** 62
B = 20_000


SEC = 1_000_000


def prefix_of(meta: str, days: int | None) -> tuple[int, set]:
    """The last transaction time of the stream prefix 'the first N days plus their corrections' (a day commits at its
    day end, its corrections 1 s later), and the set of day ends (to label each transaction day / corrections)."""
    ends = [d["day_end_us"] for d in json.loads(Path(meta).read_text())["days"]]
    return (ends[days - 1] + SEC if days else OPEN), set(ends)


def _txkind(tt: int, ends: set) -> str:
    return "day" if tt in ends else ("corrections" if tt - SEC in ends else "other")


def _expect(src: str, L: int) -> str:
    """The rows a store replayed up to L holds: tt_s <= L, with tt_e kept if <= L and OPEN_END otherwise."""
    return f"(SELECT tt_s, CASE WHEN tt_e <= {L} THEN tt_e ELSE {OPEN} END AS tt_e, vt_s FROM {src} WHERE tt_s <= {L})"


def t0_of(meta: str, full: bool = False) -> int:
    """T0, the snapshot's last commit; with --full, OPEN_END: every version, closed ones with their real tt_e,
    goes through the bulk path and no transaction follows (the query-stage stores, D-W3-9)."""
    return OPEN if full else int(json.loads(Path(meta).read_text())["snapshot"]["tt_last"])


# --------------------------------------------------------------------------------------------- SQL systems ---

def _limits(con) -> None:
    """W3_SQL_DUCK_MEM: the staging databases' memory_limit (a shared node's allocation is below DuckDB's default of
    80 % of the node's memory); W3_STAGE_DIR: their spill directory."""
    if os.environ.get("W3_SQL_DUCK_MEM"):
        con.execute(f"SET memory_limit = '{os.environ['W3_SQL_DUCK_MEM']}'")
    if os.environ.get("W3_STAGE_DIR"):
        con.execute(f"SET temp_directory = '{os.environ['W3_STAGE_DIR']}/duck.tmp'")


def _stage_db(name: str) -> str:
    """W3_STAGE_DIR set (SF10): the staging tables live in a file database there instead of in memory; the tables
    are the same."""
    d = os.environ.get("W3_STAGE_DIR")
    if not d:
        return ":memory:"
    f = os.path.join(d, name + ".duckdb")
    for x in (f, f + ".wal"):
        if os.path.exists(x):
            os.remove(x)
    return f


def _stage(hist: str):
    """A DuckDB (in memory, or a file under W3_STAGE_DIR) with one table per history table, plus a row key `_rk`
    (the table's row order)."""
    import duckdb
    st = duckdb.connect(_stage_db("stage"))
    st.execute("SET TimeZone='UTC'")
    _limits(st)
    for t in S.HIST_TABLES:
        st.execute(f"CREATE TABLE {t} AS SELECT *, row_number() OVER () AS _rk FROM read_parquet('{hist}/{t}.parquet')")
    return st


def _tts(st, t0: int) -> list[int]:
    parts = []
    for t in S.HIST_TABLES:
        parts += [f"SELECT tt_s AS tt FROM {t} WHERE tt_s > {t0}", f"SELECT tt_e FROM {t} WHERE tt_e > {t0} AND tt_e < {OPEN}"]
    return [r[0] for r in st.execute("SELECT DISTINCT tt FROM (" + " UNION ".join(parts) + ") ORDER BY 1").fetchall()]


def _sums(con, t: str, num=None) -> list:
    num = num or (lambda x: x)
    r = con.execute(f"SELECT count(*), sum({num('tt_s')}), sum({num(f'CASE WHEN tt_e = {OPEN} THEN 0 ELSE tt_e END')}), "
                    f"sum({num('vt_s')}), sum(CASE WHEN tt_e = {OPEN} THEN 1 ELSE 0 END) FROM {t}").fetchone()
    return [int(x or 0) for x in r]


def cmd_sql(a) -> int:
    t0 = t0_of(a.base_meta, a.full)
    st = _stage(a.hist)
    L, ends = prefix_of(a.base_meta, a.prefix_days)
    tts = [t for t in _tts(st, t0) if t <= L]
    per_tx = []
    rec = {"system": a.system, "t0": t0, "prefix_days": a.prefix_days, "until_tt": L, "transactions": len(tts), "tables": {}, "method": {
        "duckdb": "bulk: INSERT ... SELECT from Parquet; transactions: INSERT + UPDATE per tt",
        "postgres": "bulk: COPY FROM STDIN, then indexes; transactions: COPY FROM STDIN + UPDATE per tt"}[a.system]}
    cols = {t: [d[0] for d in st.execute(f"SELECT * FROM {t} LIMIT 0").description] for t in S.HIST_TABLES}
    clip = lambda t: ", ".join(f"CASE WHEN tt_e <= {t0} THEN tt_e ELSE {OPEN} END AS tt_e" if c == "tt_e" else c for c in cols[t])
    w0 = time.time()
    if a.system == "duckdb":
        import duckdb
        if os.path.exists(a.db):
            os.remove(a.db)
        con = duckdb.connect(a.db)
        con.execute("SET TimeZone='UTC'")
        _limits(con)
        if os.environ.get("W3_INGEST_DUCK_THREADS"):
            con.execute(f"SET threads = {int(os.environ['W3_INGEST_DUCK_THREADS'])}")
        st.close()
        st = None
        con.execute(f"ATTACH '{_stage_db('stg')}' AS stg")
        for t in S.HIST_TABLES:
            con.execute(f"CREATE TABLE stg.{t} AS SELECT *, row_number() OVER () AS _rk FROM read_parquet('{a.hist}/{t}.parquet')")
        tb = time.time()
        for t in S.HIST_TABLES:
            con.execute(f"CREATE TABLE {t} AS SELECT {clip(t)} FROM stg.{t} WHERE tt_s <= {t0} ORDER BY tt_s, vt_s")
        con.execute("CHECKPOINT")
        rec["bulk_s"] = round(time.time() - tb, 1); rec["marks"] = {"bulk_end": time.time()}
        tx0 = time.time()
        ins = clo = 0
        for tt in tts:
            q0, i0, c0 = time.time(), ins, clo
            con.execute("BEGIN")
            for t in S.HIST_TABLES:
                c = ", ".join(f"{OPEN} AS tt_e" if x == "tt_e" else x for x in cols[t])
                ins += con.execute(f"INSERT INTO {t} SELECT {c} FROM stg.{t} WHERE tt_s = {tt}").fetchone()[0]
                clo += con.execute(f"UPDATE {t} SET tt_e = {tt} WHERE _rk IN (SELECT _rk FROM stg.{t} WHERE tt_e = {tt})").fetchone()[0]
            con.execute("COMMIT")
            per_tx.append([tt, _txkind(tt, ends), ins - i0, clo - c0, round(time.time() - q0, 3)])
        rec.update(transactions_s=round(time.time() - tx0, 1), inserted=ins, closed=clo)
        for mode in ("instant", "window"):
            for fam in ("bi", "iv2"):
                for s in S.view_ddl("duckdb", mode, fam):
                    con.execute(s)
        con.execute("CHECKPOINT")
        for t in S.HIST_TABLES:
            rec["tables"][t] = {"db": _sums(con, t), "hist": _sums(con, _expect(f"stg.{t}", L))}
        con.execute("DETACH stg")
        con.close()
        rec["bytes"] = os.path.getsize(a.db)
    else:
        import psycopg
        con = psycopg.connect(a.dsn, autocommit=True)
        rec["settings"] = {k: con.execute(f"SHOW {k}").fetchone()[0] for k in ("fsync", "synchronous_commit", "wal_level")}
        tb = time.time()
        for t in S.HIST_TABLES:
            types = dict(st.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{t}'").fetchall())
            con.execute(f"DROP TABLE IF EXISTS {t} CASCADE")
            con.execute(f"CREATE TABLE {t} (" + ", ".join(f"{c} {Y.PGT.get(types[c], 'text')}" for c in cols[t]) + ")")
            f = f"{a.tmp}/{t}.bulk.csv"
            st.execute(f"COPY (SELECT {clip(t)} FROM {t} WHERE tt_s <= {t0}) TO '{f}' (FORMAT csv, HEADER false)")
            with con.cursor() as cur, cur.copy(f"COPY {t} FROM STDIN (FORMAT csv)") as cp, open(f, "rb") as fh:
                while b := fh.read(1 << 22):
                    cp.write(b)
            os.remove(f)
        for t in S.HIST_TABLES:
            con.execute(f"CREATE UNIQUE INDEX ON {t} (_rk)")
            for c in Y.PG_INDEXES.get(t, []):
                con.execute(f"CREATE INDEX ON {t} ({c}, tt_s)")
            con.execute(f"ANALYZE {t}")
        rec["bulk_s"] = round(time.time() - tb, 1); rec["marks"] = {"bulk_end": time.time()}
        tx0 = time.time()
        ins = clo = 0
        con.autocommit = False
        for tt in tts:
            q0, i0, c0 = time.time(), ins, clo
            with con.transaction():
                for t in S.HIST_TABLES:
                    rows = st.execute(f"SELECT {', '.join(str(OPEN) if x == 'tt_e' else x for x in cols[t])} FROM {t} WHERE tt_s = {tt}").fetchall()
                    if rows:
                        with con.cursor() as cur, cur.copy(f"COPY {t} ({', '.join(cols[t])}) FROM STDIN") as cp:
                            for r in rows:
                                cp.write_row(r)
                        ins += len(rows)
                    rk = [r[0] for r in st.execute(f"SELECT _rk FROM {t} WHERE tt_e = {tt}").fetchall()]
                    if rk:
                        clo += con.execute(f"UPDATE {t} SET tt_e = %s WHERE _rk = ANY(%s)", (tt, rk)).rowcount
            per_tx.append([tt, _txkind(tt, ends), ins - i0, clo - c0, round(time.time() - q0, 3)])
        con.autocommit = True
        rec.update(transactions_s=round(time.time() - tx0, 1), inserted=ins, closed=clo)
        for mode in ("instant", "window"):
            for fam in ("bi", "iv2"):
                for s in S.view_ddl("postgres", mode, fam):
                    con.execute(s)
        for t in S.HIST_TABLES:
            con.execute(f"ANALYZE {t}")
        con.execute("CHECKPOINT")
        num = lambda x: f"({x})::numeric"
        for t in S.HIST_TABLES:
            rec["tables"][t] = {"db": _sums(con, t, num), "hist": _sums(st, _expect(t, L))}
        rec["bytes"] = con.execute("SELECT pg_database_size(current_database())").fetchone()[0]
        con.close()
    rec["load_s"] = round(time.time() - w0, 1)
    rec.setdefault("marks", {}).update(start=w0, end=time.time())
    rec["per_tx"] = per_tx
    rec["by_kind"] = _by_kind(per_tx)
    rec["verified"] = all(v["db"] == v["hist"] for v in rec["tables"].values())
    Path(a.out).write_text(json.dumps(rec, indent=1) + "\n")
    print(json.dumps({k: v for k, v in rec.items() if k not in ("tables", "per_tx")}))
    return 0 if rec["verified"] else 1


def _by_kind(per_tx: list) -> dict:
    """Per transaction kind: transactions, items (inserted + closed), seconds, items/s."""
    out = {}
    for _, k, i, c, sec in per_tx:
        o = out.setdefault(k, {"transactions": 0, "items": 0, "s": 0.0})
        o["transactions"] += 1; o["items"] += i + c; o["s"] += sec
    for o in out.values():
        o["s"] = round(o["s"], 1); o["items_per_s"] = round(o["items"] / o["s"], 1) if o["s"] else None
    return out


# ------------------------------------------------------------------------------------------- graph systems ---

def _kind(v) -> str:
    if isinstance(v, bool):
        return "boolean"
    if isinstance(v, int):
        return "long"
    if isinstance(v, float):
        return "double"
    if isinstance(v, dt.datetime):
        return "localdatetime"
    if isinstance(v, dt.date):
        return "date"
    return "string"


def _val(v):
    if v is None:
        return ""
    if isinstance(v, (dt.datetime, dt.date)):
        return v.isoformat()
    if isinstance(v, bool):
        return "true" if v else "false"
    return v


RCOLS = ["rid", "a", "b", "cd", "classYear", "workFrom", "vt_s", "vt_e", "tt_s", "tt_e", "evs", "eve", "ets", "ete"]
RTYPES = {"rid": "long", "vt_s": "long", "vt_e": "long", "tt_s": "long", "tt_e": "long", "creationDate": "localdatetime",
          "classYear": "int", "workFrom": "int", "evs": "long", "eve": "long", "ets": "long", "ete": "long"}


def write_csvs(duck, t0: int, out: Path) -> dict:
    """Bulk CSVs: one file per node label and one per relationship type, with typed headers (neo4j-admin form)."""
    out.mkdir(parents=True, exist_ok=True)
    files = {"nodes": {}, "rels": {}}
    for lab in [r[0] for r in duck.execute("SELECT DISTINCT lab FROM nodes ORDER BY 1").fetchall()]:
        q = (f"SELECT gid, lab, id, vt_s, vt_e, tt_s, CASE WHEN tt_e <= {t0} THEN tt_e ELSE {OPEN} END, props "
             f"FROM nodes WHERE lab = '{lab}' AND tt_s <= {t0} ORDER BY gid")
        # two streamed passes (the header's keys and types, then the rows), so a label's node dicts are never all
        # in memory at once (SF10: 25 M Comment versions); the header and the rows are those of one pass
        keys, kinds = [], {}
        cur = duck.execute(q)
        while rows := cur.fetchmany(B):
            for x in rows:
                for k, v in Y.node_dict(*x).items():
                    if k not in kinds:
                        keys.append(k); kinds[k] = _kind(v)
        f = out / f"n_{lab.replace(':', '_')}.csv"
        n = 0
        with open(f, "w", newline="") as fh:
            w = csv.writer(fh)
            w.writerow(["gid:ID(V)", ":LABEL"] + [f"{k}:{kinds[k]}" for k in keys if k != "gid"])
            labs = "V;" + lab.replace(":", ";")
            cur = duck.execute(q)
            while rows := cur.fetchmany(B):
                for x in rows:
                    d = Y.node_dict(*x)
                    w.writerow([d["gid"], labs] + [_val(d.get(k)) for k in keys if k != "gid"])
                n += len(rows)
        files["nodes"][lab] = {"file": f.name, "rows": n, "cols": {k: kinds[k] for k in keys}}
    for rel in [r[0] for r in duck.execute("SELECT DISTINCT rel FROM rels ORDER BY 1").fetchall()]:
        cur = duck.execute(f"SELECT {', '.join(RCOLS[:9])}, CASE WHEN tt_e <= {t0} THEN tt_e ELSE {OPEN} END, evs, eve, ets, ete "
                           f"FROM rels WHERE rel = '{rel}' AND tt_s <= {t0}")
        f = out / f"r_{rel}.csv"
        n = 0
        with open(f, "w", newline="") as fh:
            w = csv.writer(fh)
            names = ["rid", "vt_s", "vt_e", "tt_s", "tt_e", "creationDate", "classYear", "workFrom", "evs", "eve", "ets", "ete"]
            w.writerow([":START_ID(V)", ":END_ID(V)", ":TYPE"] + [f"{k}:{RTYPES[k]}" for k in names])
            while rows := cur.fetchmany(B):
                for x in rows:
                    r = dict(zip(RCOLS, x))
                    cd = Y._ldt(r["cd"]) if r["cd"] is not None else None
                    w.writerow([r["a"], r["b"], rel, r["rid"], r["vt_s"], r["vt_e"], r["tt_s"], r["tt_e"], _val(cd),
                                _val(r["classYear"]), _val(r["workFrom"]), r["evs"], r["eve"], r["ets"], r["ete"]])
                n += len(rows)
        files["rels"][rel] = {"file": f.name, "rows": n}
    return files


def _split_csv(args) -> list[str]:
    """One CSV file as parts of at most `rows` records, each with the header (csv module: quoted multi-line fields
    stay whole). The parts are written in the same directory; their names, in order, are returned."""
    path, rows = args
    path = Path(path)
    out, k = [], 0
    with open(path, newline="") as fh:
        r = csv.reader(fh)
        header = next(r)
        w = g = None
        n = rows
        for rec in r:
            if n >= rows:
                if g:
                    g.close()
                name = f"{path.stem}.p{k:04d}.csv"; k += 1; out.append(name)
                g = open(path.parent / name, "w", newline=""); w = csv.writer(g); w.writerow(header); n = 0
            w.writerow(rec); n += 1
        if g:
            g.close()
    return out


def mg_load_csv(run, files: dict, base: str, host_dir: Path | None = None, rows: int = 0) -> dict:
    """Memgraph LOAD CSV, one statement per file, with the typed conversions of the header. With `rows` (env
    W3_MG_LOAD_ROWS), each file is split into parts of at most that many records and loaded one LOAD CSV statement
    (one transaction) per part: the same statement text, the same rows. One transaction per SF10 file exceeds what a
    the evaluation host node lets Memgraph map."""
    conv = {"long": "toInteger({})", "int": "toInteger({})", "double": "toFloat({})", "boolean": "toBoolean({})",
            "localdatetime": "localDateTime({})", "date": "date({})", "string": "{}"}
    parts: dict[str, list[str]] = {}
    if rows and host_dir is not None:
        import multiprocessing as mp
        names = [f["file"] for k in ("nodes", "rels") for f in files[k].values()]
        with mp.get_context("spawn").Pool(min(len(names), os.cpu_count() or 8)) as pool:
            for name, ps in zip(names, pool.map(_split_csv, [(host_dir / n, rows) for n in names])):
                parts[name] = ps
    of = lambda name: parts.get(name, [name])  # noqa: E731
    for lab, f in files["nodes"].items():
        props = ", ".join(f"{k}: " + ("toInteger(row.`gid:ID(V)`)" if k == "gid" else conv[t].format(f"row.`{k}:{t}`"))
                          for k, t in f["cols"].items())
        for part in of(f["file"]):
            run(f"LOAD CSV FROM '{base}/{part}' WITH HEADER NULLIF '' AS row CREATE (n:V:{lab} {{{props}}})")
    for rel, f in files["rels"].items():
        props = ", ".join(f"{k}: " + conv[t].format(f"row.`{k}:{t}`") for k, t in RTYPES.items())
        for part in of(f["file"]):
            run(f"LOAD CSV FROM '{base}/{part}' WITH HEADER NULLIF '' AS row "
                f"MATCH (a:V {{gid: toInteger(row.`:START_ID(V)`)}}), (b:V {{gid: toInteger(row.`:END_ID(V)`)}}) "
                f"CREATE (a)-[x:{rel} {{{props}}}]->(b)")
    return {"rows_per_statement": rows or None, "statements": sum(len(of(f["file"])) for k in ("nodes", "rels")
                                                                for f in files[k].values())}


def replay_graph(drv, duck, t0: int, tts: list[int], ends: set) -> tuple[int, int, list]:
    """One Bolt transaction per transaction time above T0: create the versions and copies with tt_s = tt
    (tt_e = OPEN_END), then close those with tt_e = tt."""
    rels = [r[0] for r in duck.execute("SELECT DISTINCT rel FROM rels").fetchall()]
    ins = clo = 0
    per_tx = []
    t_first = time.time()
    for tt in tts:
        q0, i0, c0 = time.time(), ins, clo
        with drv.session() as s:
            tx = s.begin_transaction()
            for lab in [r[0] for r in duck.execute(f"SELECT DISTINCT lab FROM nodes WHERE tt_s = {tt}").fetchall()]:
                rows = duck.execute(f"SELECT gid, lab, id, vt_s, vt_e, tt_s, {OPEN}, props FROM nodes WHERE tt_s = {tt} AND lab = '{lab}'").fetchall()
                for i in range(0, len(rows), B):
                    tx.run(f"UNWIND $rows AS r CREATE (n:V:{lab}) SET n = r", rows=[Y.node_dict(*x) for x in rows[i:i + B]]).consume()
                ins += len(rows)
            for rel in rels:
                rows = duck.execute(f"SELECT {', '.join(RCOLS)} FROM rels WHERE tt_s = {tt} AND rel = '{rel}'").fetchall()
                for i in range(0, len(rows), B):
                    batch = [Y.rel_dict(dict(zip(RCOLS, x))) for x in rows[i:i + B]]
                    for r in batch:
                        r["tt_e"] = OPEN
                    tx.run(f"UNWIND $rows AS r MATCH (a:V {{gid: r.a}}), (b:V {{gid: r.b}}) CREATE (a)-[x:{rel}]->(b) " + Y.REL_SET,
                           rows=batch).consume()
                ins += len(rows)
            for rel in rels:
                rows = duck.execute(f"SELECT a, b, rid FROM rels WHERE tt_e = {tt} AND rel = '{rel}'").fetchall()
                for i in range(0, len(rows), B):
                    tx.run(f"UNWIND $rows AS r MATCH (a:V {{gid: r[0]}})-[x:{rel}]->(b:V {{gid: r[1]}}) WHERE x.rid = r[2] "
                           f"SET x.tt_e = {tt}", rows=[list(x) for x in rows[i:i + B]]).consume()
                clo += len(rows)
            gids = [r[0] for r in duck.execute(f"SELECT gid FROM nodes WHERE tt_e = {tt}").fetchall()]
            for i in range(0, len(gids), B):
                tx.run(f"UNWIND $g AS g MATCH (n:V {{gid: g}}) SET n.tt_e = {tt}", g=gids[i:i + B]).consume()
            clo += len(gids)
            tx.commit()
        per_tx.append([tt, _txkind(tt, ends), ins - i0, clo - c0, round(time.time() - q0, 3)])
        if os.environ.get("W3_INGEST_PROGRESS"):     # one line per committed transaction (a run stopped at its limit keeps how far it got)
            with open(os.environ["W3_INGEST_PROGRESS"], "a") as f:
                f.write(json.dumps({"n": len(per_tx), "of": len(tts), "tx": per_tx[-1], "since_first_tx_s": round(time.time() - t_first, 1), "utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}) + "\n")
    return ins, clo, per_tx


def graph_expected(duck, L: int = OPEN) -> dict:
    """The history side of graph_counts: per node label ("n:<label>") and relationship type ("r:<type>"), the
    count and the sums of tt_s, closed tt_e and vt_s in whole seconds, from the graph tables."""
    hagg = (f"count(*), sum(tt_s // 1000000), sum(CASE WHEN tt_e = {OPEN} THEN 0 ELSE tt_e // 1000000 END), "
            f"sum(vt_s // 1000000)")
    out = {}
    for lab in [r[0] for r in duck.execute("SELECT DISTINCT lab FROM nodes ORDER BY 1").fetchall()]:
        h = duck.execute(f"SELECT {hagg} FROM " + _expect(f"(SELECT * FROM nodes WHERE lab = '{lab}')", L)).fetchone()
        out["n:" + lab] = [int(x or 0) for x in h]
    for rel in [r[0] for r in duck.execute("SELECT DISTINCT rel FROM rels ORDER BY 1").fetchall()]:
        h = duck.execute(f"SELECT {hagg} FROM " + _expect(f"(SELECT * FROM rels WHERE rel = '{rel}')", L)).fetchone()
        out["r:" + rel] = [int(x or 0) for x in h]
    return out


def graph_counts(run_q, duck, L: int = OPEN, expected: dict | None = None) -> dict:
    """Per label and type: the count and the sums of tt_s, closed tt_e and vt_s in whole seconds (so no sum
    overflows 64 bits), read back from the database and from the tables (the verification of the loaded store).
    `expected`: graph_expected's output computed earlier (a prepared load, `prepare`), instead of the tables."""
    out = {}
    sec = lambda v: f"{v} / 1000000"
    agg = lambda v: (f"count({v}) AS c, sum({sec(v + '.tt_s')}) AS a, "
                     f"sum(CASE WHEN {v}.tt_e = {OPEN} THEN 0 ELSE {sec(v + '.tt_e')} END) AS b, sum({sec(v + '.vt_s')}) AS d")
    exp = expected if expected is not None else graph_expected(duck, L)
    for key, h in exp.items():
        kind, name = key.split(":", 1)
        r = run_q(f"MATCH (n:V:{name}) RETURN " + agg("n")) if kind == "n" else run_q(f"MATCH ()-[x:{name}]->() RETURN " + agg("x"))
        out[name] = {"db": [int(x or 0) for x in r], "hist": h}
    return out


def _wait_bolt(uri: str, system: str, tries: int = 600) -> None:
    for _ in range(tries):
        try:
            d = Y._driver(uri, system)
            d.verify_connectivity(); d.close()
            return
        except Exception:      # noqa: BLE001 - not up yet
            time.sleep(2)
    raise RuntimeError(f"{system} at {uri} did not come up")


def cmd_prepare(a) -> int:
    """The bulk CSVs of a full (--full) graph load, written once for both graph systems, with the history side of
    the read-back counts (graph_expected) and the CSV writing time, in DIR/prepared.json. `graph --prepared DIR`
    then loads from them; the files are those cmd_graph would write itself (write_csvs, t0 = OPEN_END)."""
    out = Path(a.out)
    tc = time.time()
    duck = Y.graph_tables(a.hist, db=a.duck_db)
    tg = round(time.time() - tc, 1)
    files = write_csvs(duck, OPEN, out)
    tw = round(time.time() - tc - tg, 1)
    exp = graph_expected(duck, OPEN)
    duck.close()
    if a.duck_db:
        for f in (a.duck_db, a.duck_db + ".wal"):
            if os.path.exists(f):
                os.remove(f)
    rec = {"hist": a.hist, "t0": OPEN, "files": files, "expected": exp, "graph_tables_s": tg, "csv_s": tw,
           "prepare_s": round(time.time() - tc, 1),
           "bytes": sum((out / f["file"]).stat().st_size for k in ("nodes", "rels") for f in files[k].values()),
           "bulk_rows": {"nodes": sum(f["rows"] for f in files["nodes"].values()),
                         "rels": sum(f["rows"] for f in files["rels"].values())}}
    (out / "prepared.json").write_text(json.dumps(rec, indent=1) + "\n")
    print(json.dumps({k: v for k, v in rec.items() if k not in ("files", "expected")}))
    return 0


def _place(prep_dir: Path, files: dict, imp: Path) -> dict:
    """A prepared load: the prepared CSVs into the server's import directory (hard links when on one file system)."""
    imp.mkdir(parents=True, exist_ok=True)
    for k in ("nodes", "rels"):
        for f in files[k].values():
            src, dst = prep_dir / f["file"], imp / f["file"]
            if dst.exists():
                dst.unlink()
            try:
                os.link(src, dst)
            except OSError:
                import shutil
                shutil.copyfile(src, dst)
    return files


def cmd_graph(a) -> int:
    t0 = t0_of(a.base_meta, a.full)
    w0 = time.time()
    prep = json.loads((Path(a.prepared) / "prepared.json").read_text()) if a.prepared else None
    if prep is not None:
        if not a.full or prep["t0"] != OPEN:
            raise SystemExit("--prepared is the full (--full) load only")
        duck, tts = None, []
    else:
        duck = Y.graph_tables(a.hist)
        tts = [r[0] for r in duck.execute(
            "SELECT DISTINCT tt FROM (SELECT tt_s AS tt FROM nodes UNION SELECT tt_e FROM nodes WHERE tt_e < "
            f"{OPEN} UNION SELECT tt_s FROM rels UNION SELECT tt_e FROM rels WHERE tt_e < {OPEN}) WHERE tt > {t0} ORDER BY 1").fetchall()]
    L, ends = prefix_of(a.base_meta, a.prefix_days)
    tts = [t for t in tts if t <= L]
    d = Path(a.srv) / f"{a.system}-w3"
    rec = {"system": a.system, "t0": t0, "prefix_days": a.prefix_days, "until_tt": L, "transactions": len(tts), "prepare_s": None}
    env = dict(os.environ)
    # a bulk statement or one stream transaction can run far longer than a query: the server's per-query timeout
    # (the kit's SERVER_TIMEOUT_S, the answer-check cap otherwise) is lifted for the load
    env["EXTCMP_SERVER_TIMEOUT_S"] = str(max(int(env.get("EXTCMP_SERVER_TIMEOUT_S", "0") or 0), 14400))
    if a.system == "neo4j":
        rec["method"] = "bulk: neo4j-admin database import full (offline); transactions: Bolt, one per tt"
        imp = Path(a.prepared) if prep else Path(a.srv) / "neo4j-import"
        tc = time.time()
        files = prep["files"] if prep else write_csvs(duck, t0, imp)
        rec["prepare_s"] = prep["prepare_s"] if prep else round(time.time() - tc, 1)   # CSV writing: input preparation, not the load
        subprocess.run(["bash", a.servers, "wipe", "neo4j", "w3"], check=False, env=env)
        (d / "data").mkdir(parents=True, exist_ok=True); (d / "logs").mkdir(parents=True, exist_ok=True)
        args = ["apptainer", "exec", "--cleanenv", "--writable-tmpfs", "--bind", f"{d}/data:/data", "--bind", f"{d}/logs:/logs",
                "--bind", f"{imp}:/import-w3", "--env", "NEO4J_AUTH=none", "--env", "NEO4J_ACCEPT_LICENSE_AGREEMENT=yes",
                a.neo4j_sif, "/startup/docker-entrypoint.sh", "neo4j-admin", "database", "import", "full", "neo4j",
                "--overwrite-destination=true", "--multiline-fields=true", "--id-type=INTEGER", "--report-file=/import-w3/import.report",
                "--threads=" + os.environ.get("W3_NEO4J_IMPORT_THREADS", str(os.cpu_count() or 16))]
        args += os.environ.get("W3_NEO4J_IMPORT_EXTRA", "").split()     # e.g. --max-off-heap-memory=24g on a shared node
        args += [f"--nodes=/import-w3/{f['file']}" for f in files["nodes"].values()]
        args += [f"--relationships=/import-w3/{f['file']}" for f in files["rels"].values()]
        tb = time.time()
        p = subprocess.run(args, capture_output=True, text=True)
        rec["import_log_tail"] = (p.stdout + p.stderr)[-3000:]
        if p.returncode != 0:
            rec["error"] = f"neo4j-admin import failed rc={p.returncode}"
            Path(a.out).write_text(json.dumps(rec, indent=1) + "\n")
            print(rec["import_log_tail"]); return 2
        rec["bulk_import_s"] = round(time.time() - tb, 1)
        ts = time.time()
        subprocess.run(["bash", a.servers, "start", "neo4j", "w3"], check=True, env=env)
        _wait_bolt(a.uri, "neo4j")
        drv = Y._driver(a.uri, "neo4j")

        def run(cy, **kw):
            with drv.session() as s:
                s.run(cy, **kw).consume()
        Y._indexes(run, "neo4j")
        run("CALL db.awaitIndexes(3600)")
        rec["start_index_s"] = round(time.time() - ts, 1)
        rec["bulk_s"] = round(rec["bulk_import_s"] + rec["start_index_s"], 1)
        rec["marks"] = {"bulk_end": time.time()}
    else:
        rec["method"] = "bulk: LOAD CSV (running server); transactions: Bolt, one per tt"
        imp = d / "lib" / "import"
        subprocess.run(["bash", a.servers, "wipe", "memgraph", "w3"], check=False, env=env)
        tc = time.time()
        files = _place(Path(a.prepared), prep["files"], imp) if prep else write_csvs(duck, t0, imp)
        rec["prepare_s"] = prep["prepare_s"] if prep else round(time.time() - tc, 1)
        tb = time.time()
        subprocess.run(["bash", a.servers, "start", "memgraph", "w3"], check=True, env=env)
        _wait_bolt(a.uri, "memgraph")
        drv = Y._driver(a.uri, "memgraph")

        def run(cy, **kw):
            with drv.session() as s:
                s.run(cy, **kw).consume()
        Y._indexes(run, "memgraph")
        rec["load_statements"] = mg_load_csv(run, files, "/var/lib/memgraph/import", host_dir=imp,
                                             rows=int(os.environ.get("W3_MG_LOAD_ROWS", "0") or 0))
        rec["bulk_s"] = round(time.time() - tb, 1); rec["marks"] = {"bulk_end": time.time()}
    rec["bulk_rows"] = {"nodes": sum(f["rows"] for f in files["nodes"].values()), "rels": sum(f["rows"] for f in files["rels"].values())}
    tx0 = time.time()
    ins, clo, per_tx = replay_graph(drv, duck, t0, tts, ends) if tts else (0, 0, [])
    rec.update(transactions_s=round(time.time() - tx0, 1), inserted=ins, closed=clo, per_tx=per_tx, by_kind=_by_kind(per_tx))
    if a.system == "memgraph":
        tsn = time.time()
        run("CREATE SNAPSHOT")
        rec["snapshot_s"] = round(time.time() - tsn, 1)
        try:      # the server's own memory figures after the load (peak_memory_res covers the LOAD CSV transactions)
            with drv.session() as s:
                rec["storage_info"] = {r["storage info"]: str(r["value"]) for r in s.run("SHOW STORAGE INFO")}
        except Exception as e:  # noqa: BLE001 - a record field, not a load step
            rec["storage_info"] = {"error": str(e)[:200]}

    def run_q(q):
        with drv.session() as s:
            return list(s.run(q).single().values())
    rec["counts"] = graph_counts(run_q, duck, L, expected=prep["expected"] if prep else None)
    if prep:
        rec["prepared"] = str(a.prepared)
    rec["verified"] = all(v["db"] == v["hist"] for v in rec["counts"].values())
    drv.close()
    rec["load_s"] = round(time.time() - w0 - (0 if prep else (rec["prepare_s"] or 0)), 1)
    rec.setdefault("marks", {}).update(start=w0, end=time.time())
    if not (prep and a.system == "neo4j"):        # a prepared Neo4j load reads the prepared directory itself
        subprocess.run(["rm", "-rf", str(imp)], check=False)
    Path(a.out).write_text(json.dumps(rec, indent=1) + "\n")
    print(json.dumps({k: v for k, v in rec.items() if k not in ("counts", "import_log_tail", "per_tx")}))
    return 0 if rec["verified"] else 1


# ----------------------------------------------------------------------------------------------- digests ---

def _h(*xs) -> int:
    import hashlib
    return int(hashlib.sha256(repr(xs).encode()).hexdigest()[:16], 16)


def _norm(v):
    if isinstance(v, (dt.datetime, dt.date)):
        return v.isoformat()
    if hasattr(v, "iso_format"):          # neo4j temporal types
        return v.iso_format()
    if hasattr(v, "to_native"):
        return _norm(v.to_native())
    return v


def _digest_sql_part(args) -> dict:
    """One worker of the SQL digest: the [count, hash sum] of each of its tables."""
    system, target, tables = args
    M = 1 << 64
    if system == "duckdb":
        import duckdb
        con = duckdb.connect(target, read_only=True)
    else:
        import psycopg
        con = psycopg.connect(target)
    out = {}
    for t in tables:
        # PostgreSQL: a server-side (named) cursor streams the table; a client-side cursor holds the whole result in
        # memory
        cur = (con.cursor(name=f"digest_{t.lower()}") if system == "postgres" else con)
        if system == "postgres":
            cur.itersize = 100_000
            cur.execute(f"SELECT * FROM {t}")
        else:
            cur = con.execute(f"SELECT * FROM {t}")
        names = [d[0].lower() for d in cur.description]
        keep = [i for i, n in enumerate(names) if n != "_rk"]
        n = acc = 0
        while rows := cur.fetchmany(100_000):
            for r in rows:
                acc = (acc + _h(*[_norm(r[i]) for i in keep])) % M; n += 1
        out[t] = [n, acc]
    con.close()
    return out


def _digest_graph_part(args) -> dict:
    """One worker of the graph digest: the elements whose node (or relationship source) has gid % K = k."""
    system, target, K, k = args
    M = 1 << 64
    flt = (lambda v: f" WHERE {v}.gid % {K} = {k}") if K > 1 else (lambda v: "")
    drv = Y._driver(target, system)
    acc = {}
    with drv.session() as s:
        for rec in s.run(f"MATCH (n:V){flt('n')} RETURN labels(n) AS l, properties(n) AS p"):
            lab = ":".join(sorted(rec["l"]))
            h = _h(lab, sorted((k_, _norm(v)) for k_, v in rec["p"].items()))
            c = acc.setdefault(lab, [0, 0]); c[0] += 1; c[1] = (c[1] + h) % M
        for rec in s.run(f"MATCH (a:V)-[x]->(b:V){flt('a')} RETURN type(x) AS t, a.gid AS a, b.gid AS b, properties(x) AS p"):
            # rid is the loader's row key of a copy, not data: left out (its numbering is not stable across builds)
            h = _h(rec["t"], rec["a"], rec["b"], sorted((k_, _norm(v)) for k_, v in rec["p"].items() if k_ != "rid"))
            c = acc.setdefault(rec["t"], [0, 0]); c[0] += 1; c[1] = (c[1] + h) % M
    drv.close()
    return acc


def cmd_digest(a) -> int:
    """An order-independent digest of a loaded store: per table (SQL) or label / relationship type (Cypher), the
    count and the sum mod 2^64 of a hash of every row (all columns but the row key) or element (labels or type,
    endpoint gids, every property). Two stores with equal digests hold the same multiset of versions.

    `--parts K` (SF10), on `--workers W` processes (default K). SQL: the tables are dealt out to K parts. Cypher:
    part k reads the nodes with gid % K = k and the relationships whose source node has gid % K = k, so each query
    stays short (below a server's per-query timeout). Every element is read by exactly one part, and counts and
    sums mod 2^64 add, so the digest is the one of a single pass."""
    M = 1 << 64
    K = max(1, a.parts)
    if a.system in ("duckdb", "postgres"):
        jobs = [(a.system, a.target, list(S.HIST_TABLES[i::K])) for i in range(min(K, len(S.HIST_TABLES)))]
        worker = _digest_sql_part
    else:
        jobs = [(a.system, a.target, K, k) for k in range(K)]
        worker = _digest_graph_part
    W = max(1, min(a.workers or len(jobs), len(jobs)))
    if W == 1:
        parts = [worker(j) for j in jobs]
    else:
        import multiprocessing as mp
        with mp.get_context("spawn").Pool(W) as pool:
            parts = pool.map(worker, jobs, chunksize=1)
    acc: dict = {}
    for part in parts:
        for key, (n, h) in part.items():
            c = acc.setdefault(key, [0, 0]); c[0] += n; c[1] = (c[1] + h) % M
    out = {t: acc[t] for t in S.HIST_TABLES if t in acc} if a.system in ("duckdb", "postgres") else acc
    Path(a.out).write_text(json.dumps(out, indent=1, sort_keys=True) + "\n")
    print(json.dumps({"system": a.system, "parts": len(out), "rows": sum(v[0] for v in out.values()), "parts_k": K, "workers": W}))
    return 0


def cmd_cmp(a) -> int:
    x, y = json.loads(Path(a.a).read_text()), json.loads(Path(a.b).read_text())
    diff = sorted(k for k in set(x) | set(y) if x.get(k) != y.get(k))
    print(json.dumps({"a": a.a, "b": a.b, "parts": len(set(x) | set(y)), "equal": not diff, "differ": diff[:20]}))
    return 0 if not diff else 1


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("sql")
    s.add_argument("--system", choices=["duckdb", "postgres"], required=True)
    s.add_argument("--hist", required=True); s.add_argument("--base-meta", required=True)
    s.add_argument("--db"); s.add_argument("--dsn"); s.add_argument("--tmp", default="/tmp")
    s.add_argument("--out", required=True)
    s.add_argument("--full", action="store_true", help="every version through the bulk path, no transactions")
    s.add_argument("--prefix-days", type=int, help="replay only the first N days and their corrections (ingest study)")
    g = sub.add_parser("graph")
    g.add_argument("--system", choices=["neo4j", "memgraph"], required=True)
    g.add_argument("--hist", required=True); g.add_argument("--base-meta", required=True)
    g.add_argument("--srv", required=True); g.add_argument("--uri", required=True)
    g.add_argument("--neo4j-sif"); g.add_argument("--servers", required=True)
    g.add_argument("--out", required=True)
    g.add_argument("--full", action="store_true", help="every version through the bulk path, no transactions")
    g.add_argument("--prefix-days", type=int, help="replay only the first N days and their corrections (ingest study)")
    g.add_argument("--prepared", help="a `prepare` directory: its CSVs and expected counts (with --full only)")
    pr = sub.add_parser("prepare")
    pr.add_argument("--hist", required=True); pr.add_argument("--out", required=True)
    pr.add_argument("--duck-db", help="a file-backed working DuckDB for the graph tables (SF10)")
    d = sub.add_parser("digest")
    d.add_argument("--system", required=True); d.add_argument("--target", required=True); d.add_argument("--out", required=True)
    d.add_argument("--parts", type=int, default=1, help="partitions (the digest is the same for every K)")
    d.add_argument("--workers", type=int, default=0, help="worker processes (default: one per part)")
    c = sub.add_parser("cmp")
    c.add_argument("a"); c.add_argument("b")
    a = ap.parse_args()
    return {"sql": cmd_sql, "graph": cmd_graph, "prepare": cmd_prepare, "digest": cmd_digest, "cmp": cmd_cmp}[a.cmd](a)


if __name__ == "__main__":
    raise SystemExit(main())
