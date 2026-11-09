"""W3 loaders and query adapters for PostgreSQL, ClickHouse, Neo4j and Memgraph.

**This is not an LDBC Benchmark, and nothing produced here is an LDBC Benchmark Result.**

    w3_snb_systems.py pg-load     --hist HIST --dsn DSN
    w3_snb_systems.py ch-load     --hist HIST [--port 18123]
    w3_snb_systems.py cypher-load --hist HIST --uri bolt://... [--system neo4j|memgraph]

Every system loads the same history tables (`w3_snb_sql.py hist`), and those come from the replay of one frozen log.

**SQL systems** get the tables as they are, plus the belief views of `w3_snb_sql.view_ddl`.

**Graph systems** get one node per *version* and one relationship per (edge version, source version, target version) whose three rectangles intersect.
- The LDBC labels, relationship types and properties are kept.
- Each node and relationship also carries `vt_s, vt_e, tt_s, tt_e`. On a relationship these are the intersection rectangle, so a visible relationship always joins visible versions.
- At one belief setting (V, T), each identity has at most one visible version, so the reference Cypher plus a visibility predicate on every bound variable returns the reference answer. That predicate is the `VIS(...)` macro of the W3 Cypher texts, documented per query.

**Durability is on for every system.** No server setting relaxes durability:
- PostgreSQL keeps `fsync` and `synchronous_commit` at their defaults (on);
- ClickHouse tables are created with `fsync_after_insert = 1, fsync_part_directory = 1`;
- Neo4j keeps its transaction log on;
- Memgraph and AeonG run with WAL enabled and a flush every transaction (the kit's `EXTCMP_DURABLE=1` start mode).
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

import w3_snb_sql as S  # noqa: E402

OPEN = 2 ** 62
PG_INDEXES = {
    "Person_v": ["id"], "Person_h": ["id"], "Forum_h": ["id", "ModeratorPersonId"],
    "Message_h": ["MessageId", "CreatorPersonId", "ParentMessageId", "ContainerForumId", "RootPostId"],
    "Person_knows_Person_h": ["Person1Id", "Person2Id"], "Person_likes_Message_h": ["PersonId", "MessageId"],
    "Message_hasTag_Tag_h": ["MessageId", "TagId"], "Forum_hasMember_Person_h": ["ForumId", "PersonId"],
    "Forum_hasTag_Tag_h": ["ForumId", "TagId"], "Person_hasInterest_Tag_h": ["PersonId", "TagId"],
    "Person_studyAt_University_h": ["PersonId", "UniversityId"], "Person_workAt_Company_h": ["PersonId", "CompanyId"],
    "Place_h": ["id", "name"], "Organisation_h": ["id"], "Tag_h": ["id", "name"], "TagClass_h": ["id", "name"],
}


def duck_types(con, table: str) -> list[tuple[str, str]]:
    return [(r[0], r[1]) for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{table}')").fetchall()]


PGT = {"BIGINT": "bigint", "INTEGER": "integer", "VARCHAR": "text", "DATE": "date", "BOOLEAN": "boolean",
       "DOUBLE": "double precision", "HUGEINT": "numeric"}
CHT = {"BIGINT": "Int64", "INTEGER": "Int32", "VARCHAR": "String", "DATE": "Date32", "BOOLEAN": "Bool",
       "DOUBLE": "Float64"}


def cmd_pg_load(a) -> int:
    import duckdb
    import psycopg
    t0 = time.time()
    duck = duckdb.connect()
    con = psycopg.connect(a.dsn, autocommit=True)
    rec = {"tables": {}, "settings": {k: con.execute(f"SHOW {k}").fetchone()[0]
                                      for k in ("fsync", "synchronous_commit", "wal_level", "full_page_writes")}}
    for t in S.HIST_TABLES:
        p = f"{a.hist}/{t}.parquet"
        cols = duck_types(duck, p)
        nulls = {"ModeratorPersonId", "PartOfPlaceId", "SubclassOfTagClassId", "ParentMessageId", "ContainerForumId",
                 "imageFile", "content", "RootPostLanguage", "language", "browserUsed", "locationIP"}
        con.execute(f"DROP TABLE IF EXISTS {t} CASCADE")
        con.execute(f"CREATE TABLE {t} (" + ", ".join(f"{c} {PGT.get(ty, 'text')}" for c, ty in cols) + ")")
        csv = f"{a.tmp}/{t}.csv"
        duck.execute(f"COPY (SELECT * FROM read_parquet('{p}')) TO '{csv}' (FORMAT csv, HEADER false)")
        t1 = time.time()
        with con.cursor() as cur, cur.copy(f"COPY {t} FROM STDIN (FORMAT csv)") as cp, open(csv, "rb") as f:
            while b := f.read(1 << 22):
                cp.write(b)
        os.remove(csv)
        for c in PG_INDEXES.get(t, []):
            con.execute(f"CREATE INDEX ON {t} ({c}, tt_s)")
        con.execute(f"ANALYZE {t}")
        rec["tables"][t] = {"rows": con.execute(f"SELECT count(*) FROM {t}").fetchone()[0], "copy_s": round(time.time() - t1, 1)}
    for mode in ("instant", "window"):
        for fam in ("bi", "iv2"):
            for s in S.view_ddl("postgres", mode, fam):
                con.execute(s)
    con.execute("CHECKPOINT")
    rec["load_s"] = round(time.time() - t0, 1)
    rec["bytes"] = con.execute("SELECT pg_database_size(current_database())").fetchone()[0]
    print(json.dumps(rec))
    return 0


def cmd_ch_load(a) -> int:
    import duckdb
    import urllib.request
    t0 = time.time()
    duck = duckdb.connect()
    url = f"http://127.0.0.1:{a.port}/"

    # the belief settings must exist when a view that reads them is created
    dflt = "custom_w3_T=0&custom_w3_V=0&custom_w3_VA=0&custom_w3_VB=0"

    def q(sql: str, data: bytes | None = None) -> str:
        req = urllib.request.Request(url + "?query=" + urllib.parse.quote(sql) + "&" + dflt if data else url + "?" + dflt,
                                     data=data if data is not None else sql.encode())
        try:
            return urllib.request.urlopen(req).read().decode()
        except urllib.error.HTTPError as e:
            raise RuntimeError(f"ClickHouse refused: {sql[:200]!r}: {e.read().decode()[:600]}")
    import urllib.parse
    rec = {"tables": {}}
    for t in S.HIST_TABLES:
        p = f"{a.hist}/{t}.parquet"
        cols = duck_types(duck, p)
        key = PG_INDEXES.get(t, ["tt_s"])[0]
        q(f"DROP TABLE IF EXISTS {t}")
        q(f"CREATE TABLE {t} (" + ", ".join(f"{c} Nullable({CHT.get(ty, 'String')})" if c not in (key, "tt_s")
                                           else f"{c} {CHT.get(ty, 'String')}" for c, ty in cols)
          + f") ENGINE = MergeTree ORDER BY ({key}, tt_s) SETTINGS fsync_after_insert = 1, fsync_part_directory = 1")
        with open(p, "rb") as f:
            q(f"INSERT INTO {t} FORMAT Parquet", f.read())
        rec["tables"][t] = int(q(f"SELECT count() FROM {t}").strip())
    for mode in ("instant", "window"):
        for fam in ("bi", "iv2"):
            for s in S.view_ddl("clickhouse", mode, fam):
                q(s.rstrip(";"))
    rec["load_s"] = round(time.time() - t0, 1)
    print(json.dumps(rec))
    return 0


# --------------------------------------------------------------------------- #
# graph systems                                                                 #
# --------------------------------------------------------------------------- #

NODE_SPEC = {   # table -> (label expression, id column, property columns)
    "Person_h": ("'Person'", "id", "firstName, lastName, gender, birthday, locationIP, browserUsed, speaks, email"),
    "Forum_h": ("'Forum'", "id", "title"),
    "Message_h": ("CASE WHEN ParentMessageId IS NULL THEN 'Message:Post' ELSE 'Message:Comment' END", "MessageId",
                  "content, imageFile, locationIP, browserUsed, length, language"),
    "Place_h": ("'Place:' || upper(type[1]) || type[2:]", "id", "name, url, type"),
    "Organisation_h": ("'Organisation:' || upper(type[1]) || type[2:]", "id", "name, url, type"),
    "Tag_h": ("'Tag'", "id", "name, url"),
    "TagClass_h": ("'TagClass'", "id", "name, url"),
}
#: relationship -> (source table, source id col, target table, target id col, edge table or None (merged FK of the source row), props)
REL_SPEC = [
    ("IS_LOCATED_IN", "Person_h", "LocationCityId", "Place_h", None, ""),
    ("IS_LOCATED_IN", "Message_h", "LocationCountryId", "Place_h", None, ""),
    ("IS_LOCATED_IN", "Organisation_h", "LocationPlaceId", "Place_h", None, ""),
    ("IS_PART_OF", "Place_h", "PartOfPlaceId", "Place_h", None, ""),
    ("HAS_TYPE", "Tag_h", "TypeTagClassId", "TagClass_h", None, ""),
    ("IS_SUBCLASS_OF", "TagClass_h", "SubclassOfTagClassId", "TagClass_h", None, ""),
    ("HAS_CREATOR", "Message_h", "CreatorPersonId", "Person_h", None, ""),
    ("REPLY_OF", "Message_h", "ParentMessageId", "Message_h", None, ""),
    ("HAS_MODERATOR", "Forum_h", "ModeratorPersonId", "Person_h", None, ""),
    ("CONTAINER_OF", "Forum_h", "id", "Message_h", "post_container", ""),
    ("KNOWS", "Person_h", "Person1Id", "Person_h", "Person_knows_Person_h|Person2Id", ""),
    ("LIKES", "Person_h", "PersonId", "Message_h", "Person_likes_Message_h|MessageId", ""),
    ("HAS_TAG", "Message_h", "MessageId", "Tag_h", "Message_hasTag_Tag_h|TagId", ""),
    ("HAS_TAG", "Forum_h", "ForumId", "Tag_h", "Forum_hasTag_Tag_h|TagId", ""),
    ("HAS_MEMBER", "Forum_h", "ForumId", "Person_h", "Forum_hasMember_Person_h|PersonId", ""),
    ("HAS_INTEREST", "Person_h", "PersonId", "Tag_h", "Person_hasInterest_Tag_h|TagId", ""),
    ("STUDY_AT", "Person_h", "PersonId", "Organisation_h", "Person_studyAt_University_h|UniversityId", ", e.classYear"),
    ("WORK_AT", "Person_h", "PersonId", "Organisation_h", "Person_workAt_Company_h|CompanyId", ", e.workFrom"),
]


def _ldt(us: int):
    return dt.datetime(1970, 1, 1) + dt.timedelta(microseconds=int(us))


def _idcol(t: str) -> str:
    return NODE_SPEC[t][1]


def graph_tables(hist: str, db: str | None = None):
    """DuckDB tables `nodes` (one row per version, with a load key gid) and `rels` (one row per
    edge version x source version x target version whose rectangles intersect, rectangle = intersection; the
    edge version's own rectangle is kept as evs/eve/ets/ete, which identifies the edge version for the BiH texts).

    `db` (or W3_GRAPH_DUCK_DB): a file-backed working database instead of an in-memory one. At SF10 the `rels`
    table (about 300 M rows) does not fit in a shared node's memory; the file database spills to disk under
    W3_GRAPH_DUCK_MEM (memory_limit). The tables, and so every row written from them, are the same."""
    import duckdb
    db = db or os.environ.get("W3_GRAPH_DUCK_DB")
    if db and os.path.exists(db):
        os.remove(db)
    duck = duckdb.connect(db) if db else duckdb.connect()
    duck.execute("SET TimeZone='UTC'")
    if os.environ.get("W3_GRAPH_DUCK_MEM"):
        duck.execute(f"SET memory_limit = '{os.environ['W3_GRAPH_DUCK_MEM']}'")
    if db:
        duck.execute(f"SET temp_directory = '{db}.tmp'")
        duck.execute("SET preserve_insertion_order = false")
    for t in S.HIST_TABLES:
        duck.execute(f"CREATE VIEW {t} AS SELECT * FROM read_parquet('{hist}/{t}.parquet')")
    parts = []
    for t, (lab, idc, props) in NODE_SPEC.items():
        parts.append(f"SELECT '{t}' AS t, {idc} AS id, {lab} AS lab, vt_s, vt_e, tt_s, tt_e, "
                     f"to_json(struct_pack({', '.join(p.strip() + ' := ' + p.strip() for p in props.split(','))})) AS props FROM {t}")
    duck.execute("CREATE TABLE nodes AS SELECT row_number() OVER (ORDER BY t, id, tt_s, vt_s, tt_e, vt_e) AS gid, * FROM ("
                 + " UNION ALL ".join(parts) + ")")
    # a Person node is a piece of a Person version (Person_h splits versions by the merged location FK); it also
    # carries its whole version's own rectangle (pvs, pve, pts, pte, from Person_v) for the entity-history text BIH-K
    duck.execute("""UPDATE nodes SET props = json_merge_patch(props, json_object('pvs', v.vt_s, 'pve', v.vt_e, 'pts', v.tt_s, 'pte', v.tt_e))
        FROM Person_v v WHERE nodes.t = 'Person_h' AND v.id = nodes.id AND v.vt_s <= nodes.vt_s AND nodes.vt_e <= v.vt_e
        AND v.tt_s <= nodes.tt_s AND nodes.tt_e <= v.tt_e""")

    def rect(*xs):
        g = lambda c, f: f"{f}(" + ", ".join(f"{x}.{c}" for x in xs) + ")"
        return (f"{g('vt_s', 'greatest')} AS vt_s, {g('vt_e', 'least')} AS vt_e, "
                f"{g('tt_s', 'greatest')} AS tt_s, {g('tt_e', 'least')} AS tt_e")
    sel = []
    for rel, st, scol, tt, et, extra in REL_SPEC:
        sk, tk = f"'{st}'", f"'{tt}'"
        cy = "e.classYear" if "classYear" in extra else "NULL::INT"
        wf = "e.workFrom" if "workFrom" in extra else "NULL::INT"
        if et in (None, "post_container"):
            fk, cond, tab = (("ContainerForumId", "h.ParentMessageId IS NULL", "Message_h") if et else
                             (scol, f"h.{scol} IS NOT NULL", st))
            src = (f"(SELECT n.gid, n.vt_s, n.vt_e, n.tt_s, n.tt_e, h.{fk} AS fk FROM nodes n JOIN {tab} h ON n.t = '{tab}' "
                   f"AND h.{_idcol(tab)} = n.id AND h.vt_s = n.vt_s AND h.tt_s = n.tt_s AND h.vt_e = n.vt_e AND h.tt_e = n.tt_e WHERE {cond})")
            other = "'Forum_h'" if et else tk
            a, b = ("d.gid", "s.gid") if et else ("s.gid", "d.gid")
            sel.append(f"SELECT '{rel}' AS rel, {a} AS a, {b} AS b, s.vt_s AS cd, NULL::INT AS classYear, NULL::INT AS workFrom, "
                       f"{rect('s', 'd')}, s.vt_s AS evs, s.vt_e AS eve, s.tt_s AS ets, s.tt_e AS ete "
                       f"FROM {src} s JOIN nodes d ON d.t = {other} AND d.id = s.fk "
                       f"AND d.vt_s < s.vt_e AND s.vt_s < d.vt_e AND d.tt_s < s.tt_e AND s.tt_s < d.tt_e")
        else:
            etab, dcol = et.split("|")
            sel.append(f"SELECT '{rel}', s.gid, d.gid, e.vt_s, {cy}, {wf}, {rect('e', 's', 'd')}, "
                       f"e.vt_s, e.vt_e, e.tt_s, e.tt_e FROM {etab} e "
                       f"JOIN nodes s ON s.t = {sk} AND s.id = e.{scol} JOIN nodes d ON d.t = {tk} AND d.id = e.{dcol} "
                       f"WHERE greatest(e.vt_s, s.vt_s, d.vt_s) < least(e.vt_e, s.vt_e, d.vt_e) "
                       f"AND greatest(e.tt_s, s.tt_s, d.tt_s) < least(e.tt_e, s.tt_e, d.tt_e)")
    duck.execute("CREATE TABLE rels AS SELECT row_number() OVER (ORDER BY rel, a, b, tt_s, vt_s, tt_e, vt_e) AS rid, * FROM ("
                 + " UNION ALL ".join(sel) + ")")
    return duck


def node_dict(gid, lab, i, vs, ve, ts, te, pr) -> dict:
    d = {k: v for k, v in json.loads(pr).items() if v is not None}
    d.update(gid=gid, id=i, vt_s=vs, vt_e=ve, tt_s=ts, tt_e=te)
    if lab.startswith(("Person", "Forum", "Message")):
        d["creationDate"] = _ldt(vs)          # LocalDateTime (UTC wall clock), as the driver sends it
    if "birthday" in d:
        d["birthday"] = dt.date.fromisoformat(d["birthday"])
    return d


def rel_dict(r: dict) -> dict:
    r = dict(r)
    r["cd"] = _ldt(r["cd"]) if r["cd"] is not None else None
    return {k: v for k, v in r.items() if v is not None or k == "cd"}


REL_SET = ("SET x.rid = r.rid, x.vt_s = r.vt_s, x.vt_e = r.vt_e, x.tt_s = r.tt_s, x.tt_e = r.tt_e, "
           "x.creationDate = r.cd, x.classYear = r.classYear, x.workFrom = r.workFrom, "
           "x.evs = r.evs, x.eve = r.eve, x.ets = r.ets, x.ete = r.ete")


def _indexes(run, system: str) -> None:
    keys = (("Person", "id"), ("Message", "id"), ("Forum", "id"), ("Tag", "name"), ("Place", "name"),
            ("TagClass", "name"), ("Organisation", "id"), ("Place", "id"), ("Tag", "id"))
    if system == "neo4j":
        run("CREATE CONSTRAINT v_gid IF NOT EXISTS FOR (n:V) REQUIRE n.gid IS UNIQUE")
        for lab, key in keys:
            run(f"CREATE INDEX IF NOT EXISTS FOR (n:{lab}) ON (n.{key})")
    else:
        run("CREATE INDEX ON :V(gid)")
        for lab, key in keys:
            run(f"CREATE INDEX ON :{lab}({key})")


def _driver(uri: str, system: str):
    import neo4j
    return neo4j.GraphDatabase.driver(uri, auth=("neo4j", "neo4j") if system == "neo4j" else ("", ""))


def cmd_cypher_load(a) -> int:
    t0 = time.time()
    duck = graph_tables(a.hist)
    drv = _driver(a.uri, a.system)
    B = 20_000

    def run(cy, **kw):
        with drv.session() as s:
            s.run(cy, **kw).consume()
    _indexes(run, a.system)
    counts = {}
    for lab in [r[0] for r in duck.execute("SELECT DISTINCT lab FROM nodes ORDER BY 1").fetchall()]:
        cur = duck.execute(f"SELECT gid, lab, id, vt_s, vt_e, tt_s, tt_e, props FROM nodes WHERE lab = '{lab}'")
        n = 0
        while rows := cur.fetchmany(B):
            run(f"UNWIND $rows AS r CREATE (n:V:{lab}) SET n = r", rows=[node_dict(*x) for x in rows])
            n += len(rows)
        counts[lab] = n
    for rel in [r[0] for r in duck.execute("SELECT DISTINCT rel FROM rels ORDER BY 1").fetchall()]:
        cur = duck.execute(f"SELECT rid, a, b, cd, classYear, workFrom, vt_s, vt_e, tt_s, tt_e, evs, eve, ets, ete "
                           f"FROM rels WHERE rel = '{rel}'")
        cols = [x[0] for x in cur.description]
        n = 0
        while rows := cur.fetchmany(B):
            run(f"UNWIND $rows AS r MATCH (a:V {{gid: r.a}}), (b:V {{gid: r.b}}) CREATE (a)-[x:{rel}]->(b) " + REL_SET,
                rows=[rel_dict(dict(zip(cols, x))) for x in rows])
            n += len(rows)
        counts[rel] = n
    drv.close()
    rec = {"system": a.system, "counts": counts, "load_s": round(time.time() - t0, 1)}
    print(json.dumps(rec))
    Path(a.hist, f"{a.system}-load.json").write_text(json.dumps(rec, indent=1) + "\n")
    return 0


def cmd_aeong_load(a) -> int:
    """AeonG, native transaction time: the W3 transaction times in order, each one a *group* of AeonG transactions.
    Version nodes and relationship copies are created at their tt_s with tt_e = OPEN_END and closed by
    `SET tt_e` at their tt_e (AeonG loses deleted objects at every TT AS, so nothing is ever deleted).

    Split load: AeonG aborts a query only by its start-time
    `--query-execution-timeout-sec`, which bounds the load's queries as well. So each group is split into AeonG
    transactions of at most `--chunk` items (one query each); a query the server aborts at the ceiling is split in
    halves and retried. Each group ends with one more transaction that creates its marker node `(:W3Mark {tt})`.
    The W3 -> AeonG time map is then the bisection on the marker count: the AeonG time of T is the first AeonG time at which every group with tt <= T is complete. `--chunk 0` keeps one transaction per group
    (the earlier form, no markers)."""
    t0 = time.time()
    duck = graph_tables(a.hist)
    drv = _driver(a.uri, "neo4j")
    chunk = a.chunk or None

    def run(cy, **kw):
        with drv.session() as s:
            s.run(cy, **kw).consume()
    _indexes(run, "memgraph")
    if chunk:
        run("CREATE INDEX ON :W3Mark(tt)")
    tts = [r[0] for r in duck.execute("SELECT DISTINCT tt FROM (SELECT tt_s AS tt FROM nodes UNION SELECT tt_e FROM nodes WHERE tt_e < "
                                      f"{OPEN} UNION SELECT tt_s FROM rels UNION SELECT tt_e FROM rels WHERE tt_e < {OPEN}) ORDER BY 1").fetchall()]
    rels = [r[0] for r in duck.execute("SELECT DISTINCT rel FROM rels").fetchall()]
    rcols = ["rid", "a", "b", "cd", "classYear", "workFrom", "vt_s", "vt_e", "tt_s", "tt_e"]
    retries, commits, slowest = [], 0, [0.0, None]
    size = chunk or 20_000

    def ops(tt):
        """the group's queries in order, each (cypher, parameter name, rows) with at most `size` rows"""
        for lab in [r[0] for r in duck.execute(f"SELECT DISTINCT lab FROM nodes WHERE tt_s = {tt} ORDER BY 1").fetchall()]:
            rows = duck.execute(f"SELECT gid, lab, id, vt_s, vt_e, {OPEN}, {OPEN}, props FROM nodes WHERE tt_s = {tt} AND lab = '{lab}' ORDER BY gid").fetchall()
            for i in range(0, len(rows), size):
                yield (f"UNWIND $rows AS r CREATE (n:V:{lab}) SET n = r, n.tt_s = {tt}", "rows",
                       [node_dict(*x) for x in rows[i:i + size]])
        for rel in rels:
            rows = duck.execute(f"SELECT {', '.join(rcols)} FROM rels WHERE tt_s = {tt} AND rel = '{rel}' ORDER BY rid").fetchall()
            for i in range(0, len(rows), size):
                batch = [rel_dict(dict(zip(rcols, x))) for x in rows[i:i + size]]
                for r in batch:
                    r["tt_e"] = OPEN
                yield (f"UNWIND $rows AS r MATCH (a:V {{gid: r.a}}), (b:V {{gid: r.b}}) CREATE (a)-[x:{rel}]->(b) " + REL_SET,
                       "rows", batch)
        for rel in rels:
            rows = duck.execute(f"SELECT a, b, rid FROM rels WHERE tt_e = {tt} AND rel = '{rel}' ORDER BY rid").fetchall()
            for i in range(0, len(rows), size):
                # the close expands from the source version (a), whose fan-out is small; matching both ends can let the
                # planner start from the target (a Place or Tag with thousands of version copies), past the ceiling
                yield (f"UNWIND $rows AS r MATCH (a:V {{gid: r[0]}}) WITH a, r MATCH (a)-[x:{rel}]->() WHERE x.rid = r[2] SET x.tt_e = {tt}",
                       "rows", [list(x) for x in rows[i:i + size]])
        gids = [r[0] for r in duck.execute(f"SELECT gid FROM nodes WHERE tt_e = {tt} ORDER BY gid").fetchall()]
        for i in range(0, len(gids), size):
            yield (f"UNWIND $rows AS g MATCH (n:V {{gid: g}}) SET n.tt_e = {tt}", "rows", gids[i:i + size])

    splits = []

    def commit(tt, qs):
        """one AeonG transaction; the whole transaction is retried on AeonG's "conflicting transactions". A query
        aborted by the server timeout (the ceiling, P13) rolls the transaction back; a one-query transaction is then
        split in halves and committed as two (recorded in `splits`), down to 50 rows."""
        nonlocal commits
        for attempt in range(6):
            try:
                t = time.time()
                with drv.session() as s:
                    tx = s.begin_transaction()
                    for cy, k, rows in qs:
                        tx.run(cy, **{k: rows}).consume()
                    tx.commit()
                commits += 1
                if time.time() - t > slowest[0]:
                    slowest[:] = [round(time.time() - t, 2), tt, sum(len(r) if isinstance(r, list) else 1 for _, _, r in qs)]
                return
            except Exception as e:      # noqa: BLE001 - only the conflict and the ceiling are handled; anything else re-raises
                msg = str(e)
                if "asked to abort" in msg and len(qs) == 1 and isinstance(qs[0][2], list) and len(qs[0][2]) > 50:
                    cy, k, rows = qs[0]
                    h = len(rows) // 2
                    splits.append([tt, len(rows), round(time.time() - t, 1)])
                    print(f"split tt={tt} rows={len(rows)} after {time.time() - t:.1f} s: {cy[:60]}", file=sys.stderr, flush=True)
                    commit(tt, [(cy, k, rows[:h])])
                    commit(tt, [(cy, k, rows[h:])])
                    return
                if "conflicting transactions" not in msg or attempt == 5:
                    raise
                retries.append([tt, attempt + 1])
                time.sleep(5)

    mark = ("CREATE (m:W3Mark {tt: $rows})", "rows")
    group_ts = []

    def marker_ts(tt):
        """the marker's own AeonG transaction timestamp (`transaction_ts`, its commit time; 0 inside its own
        transaction, so read after the commit): the AeonG time at which the whole group is visible"""
        with drv.session() as s:
            r = s.run("MATCH (m:W3Mark {tt: $tt}) RETURN m", tt=tt).single()
        return int(dict(r["m"].items())["transaction_ts"])
    for n, tt in enumerate(tts):
        qs = list(ops(tt))
        if not chunk:
            commit(tt, qs)
            continue
        for q in qs:
            commit(tt, [q])
        commit(tt, [(mark[0], mark[1], tt)])      # the group's last transaction: its marker
        group_ts.append([tt, marker_ts(tt)])
        if n % 20 == 0:
            print(f"group {n + 1}/{len(tts)} tt={tt} commits={commits} splits={len(splits)} {time.time() - t0:.0f} s", file=sys.stderr, flush=True)
    # created-version counts per W3 transaction time (the earlier time map, kept for --chunk 0)
    counts = duck.execute("SELECT tt_s, count(*) FROM nodes GROUP BY 1 ORDER BY 1").fetchall()
    cum, acc = [], 0
    for tt, c in counts:
        acc += c; cum.append([tt, acc])
    drv.close()
    rec = {"system": "aeong", "transactions": len(tts), "aeong_commits": commits, "chunk": chunk,
           "markers": bool(chunk), "group_tts": tts if chunk else None, "group_ts": group_ts or None, "slowest_commit_s": slowest,
           "load_s": round(time.time() - t0, 1), "retries": retries, "splits": splits, "created_by_tt": cum}
    Path(a.hist, "aeong-load.json").write_text(json.dumps(rec) + "\n")
    print(json.dumps({k: v for k, v in rec.items() if k not in ("created_by_tt", "group_tts", "group_ts")}))
    return 0


# --------------------------------------------------------------------------- #
# Cypher query adapter                                                          #
# --------------------------------------------------------------------------- #

W3CY = ROOT / "benchmarks/workloads/snb/cypher"


def macro(text: str, name: str, fn) -> str:
    """Replace every NAME(args) by fn(list of top-level args), matching parentheses (args may nest)."""
    out, i = [], 0
    pat = re.compile(rf"\b{name}\(")
    while (m := pat.search(text, i)):
        out.append(text[i:m.start()])
        depth, j, args, start = 1, m.end(), [], m.end()
        while depth:
            ch = text[j]
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
            elif ch == "," and depth == 1:
                args.append(text[start:j].strip()); start = j + 1
            j += 1
        args.append(text[start:j - 1].strip())
        out.append(fn(args))
        i = j
    out.append(text[i:])
    return "".join(out)


def vis_expand(text: str) -> str:
    """VIS(a, b, ...) -> the instant-visibility predicate on each variable;
    VISALL(xs) -> the same for every element of a list (path nodes / relationships);
    W_OK(a, b | L) -> the window rule W1 (belief at T for every element, one shared valid instant in [VA, VB))."""
    one = lambda x: f"({x}.tt_s <= $T AND $T < {x}.tt_e AND {x}.vt_s <= $V AND $V < {x}.vt_e)"
    text = macro(text, "VISALL", lambda a: f"all(_x IN {a[0]} WHERE {one('_x')})")
    text = macro(text, "VIS", lambda a: " AND ".join(one(x) for x in a))
    text = macro(text, "VISATALL", lambda a: (lambda t, l: f"all(_x IN {l} WHERE {at(t, '_x')})")(*", ".join(a).split(";", 1)))
    text = macro(text, "VISAT", lambda a: (lambda t, vs: " AND ".join(at(t, v.strip()) for v in vs.split(",")))(*", ".join(a).split(";", 1)))
    text = macro(text, "W_OK", cw_ok)
    text = macro(text, "W_LOV", lambda a: cw_bound(a, "lo"))
    text = macro(text, "W_HIV", lambda a: cw_bound(a, "hi"))
    text = macro(text, "W_INV", lambda a: cw_ok(a[2:], a[0], a[1]))
    text = macro(text, "W_EXT", lambda a: "[" + cw_bound(a, "lo", "-1", None) + ", " + cw_bound(a, "hi", None, None) + "]")
    return macro(text, "W_UNCOVERED", lambda a: (
        f"any(_p IN [{a[0]}] + [_e IN {a[2]} WHERE _e[1] > {a[0]} AND _e[1] < {a[1]} | _e[1]] "
        f"WHERE none(_e IN {a[2]} WHERE _e[0] <= _p AND _p < _e[1]))"))


def at(t: str, x: str) -> str:
    t = t.strip()
    return f"({x}.tt_s <= $T AND $T < {x}.tt_e AND {x}.vt_s <= {t} AND {t} < {x}.vt_e)"


def _lst(args: list[str]) -> str:
    joined = ", ".join(args)
    vars_, _, extra = joined.partition("|")
    return "[" + vars_.strip() + "]" + (" + " + extra.strip() if extra.strip() else "")


def cw_bound(args: list[str], which: str, lo0: str | None = "$VA", hi0: str | None = "$VB") -> str:
    lst = _lst(args)
    if which == "lo":
        init = lo0 if lo0 is not None else "-1"
        return f"reduce(_m = {init}, _x IN {lst} | CASE WHEN _x.vt_s > _m THEN _x.vt_s ELSE _m END)"
    init = hi0 if hi0 is not None else str(OPEN)
    return f"reduce(_m = {init}, _x IN {lst} | CASE WHEN _x.vt_e < _m THEN _x.vt_e ELSE _m END)"


def cw_ok(args: list[str], lo0: str = "$VA", hi0: str = "$VB") -> str:
    """W1 in Cypher: every element believed at T, and one shared valid instant in [lo0, hi0)."""
    lst = _lst(args)
    if lst == "[]":
        return "true"
    lo = f"reduce(_m = {lo0}, _x IN {lst} | CASE WHEN _x.vt_s > _m THEN _x.vt_s ELSE _m END)"
    hi = f"reduce(_m = {hi0}, _x IN {lst} | CASE WHEN _x.vt_e < _m THEN _x.vt_e ELSE _m END)"
    return f"(all(_x IN {lst} WHERE _x.tt_s <= $T AND $T < _x.tt_e) AND {lo} < {hi})"


def _top_split(s: str) -> list[str]:
    out, depth, cur = [], 0, ""
    for ch in s:
        depth += ch in "([{"
        depth -= ch in ")]}"
        if ch == "," and depth == 0:
            out.append(cur); cur = ""
        else:
            cur += ch
    return out + [cur]


def memgraph_dialect(text: str) -> str:
    """The Memgraph-family dialect differences, applied to every text Memgraph and AeonG run (DEVIATIONS D-W3-4):
    - duration maps take singular keys (`{day: n}`, `{hour: n}`);
    - `duration.inSeconds(a, b).seconds` becomes whole seconds from `timestamp()` microseconds;
    - after an aggregating RETURN, ORDER BY may name only returned columns: an ORDER BY key that is a returned
      expression is replaced by that item's alias (one is added where the item has none)."""
    for k in ("day", "hour", "minute", "second"):
        text = re.sub(r"duration\(\{\s*" + k + r"s\s*:", "duration({" + k + ":", text)
    text = re.sub(r"duration\.inSeconds\(([^,()]+),\s*([^,()]+)\)\.seconds",
                  r"((timestamp(\2) - timestamp(\1)) / 1000000)", text)
    i = text.rfind("RETURN")
    j = text.find("ORDER BY", i) if i >= 0 else -1
    if j < 0:
        return text
    m = re.search(r"\b(LIMIT|SKIP)\b", text[j:])
    k = j + m.start() if m else len(text)
    head = text[i + 6:j]
    distinct = head.lstrip().upper().startswith("DISTINCT")
    if distinct:
        head = head.lstrip()[8:]
    items = []
    for it in _top_split(head):
        mm = re.match(r"(?is)\s*(.*?)\s+AS\s+(\w+)\s*$", it)
        items.append([mm.group(1).strip(), mm.group(2)] if mm else [it.strip(), None])
    keys = []
    for n, key in enumerate(_top_split(text[j + 8:k])):
        mm = re.match(r"(?is)\s*(.*?)\s*(ASC|DESC)?\s*$", key)
        e, d = mm.group(1), mm.group(2) or ""
        for it in items:
            if it[0] == e:
                if it[1] is None:
                    it[1] = f"_o{n}"
                e = it[1]
                break
        keys.append(f"{e} {d}".strip())
    ret = "RETURN " + ("DISTINCT " if distinct else "") + ", ".join(f"{a} AS {b}" if b else a for a, b in items)
    return text[:i] + ret + "\nORDER BY " + ", ".join(keys) + "\n" + text[k:]


class Cypher:
    def __init__(self, uri: str, system: str):
        import neo4j
        self.name = system
        self.drv = neo4j.GraphDatabase.driver(uri, auth=("neo4j", "neo4j") if system == "neo4j" else ("", ""))
        if system == "memgraph":
            import prime_cap as PC
            PC.memgraph_hook(self.drv)

    def run(self, qid: str, inst: dict, setting: str):
        from w3_snb_run import setting_values
        base = W3CY / "window" if setting == "window" and not qid.startswith("BIH-") else W3CY
        name = f"{qid.replace('TLDBC-', '')}.cypher"
        f = base / self.name / name if (base / self.name / name).exists() else base / name
        if not f.exists():
            raise NotImplementedError(f"no W3 Cypher text for {qid} yet")
        sv = setting_values(inst, setting)
        params = {"T": sv["T"], "V": sv["V"], "VA": sv["VA"], "VB": sv["VB"]}
        if qid.startswith("BIH-"):      # the TPC-BiH-style texts read T, V, A, B, T1 (as the SQL texts do)
            from w3_snb_run import query_params
            params.update({k: v for k, (_, v) in query_params(qid, inst, setting).items() if k in ("T", "V", "A", "B", "T1")})
        for k, (ty, v) in inst["params"].items():
            # dates compare with LocalDateTime creationDate properties: both DATE and DATETIME go as LocalDateTime
            params[k] = _ldt(v) if ty in ("DATETIME", "DATE") else v
        if "startDate" in params and "durationDays" in params and "endDate" not in params:
            params["endDate"] = params["startDate"] + dt.timedelta(days=int(params["durationDays"]))
        text = vis_expand(f.read_text())
        if self.name in ("memgraph", "aeong", "aeong-prop"):
            text = memgraph_dialect(text)
        import uuid
        params["gname"] = "w3_" + uuid.uuid4().hex
        stmts = [x for x in text.split("\n// ---\n") if x.strip()]
        try:
            import neo4j as _n4
            from w3_snb_run import CAP_S
            with self.drv.session() as s:
                if len(stmts) == 1:
                    import prime_cap as PC
                    res = s.run(_n4.Query(text, timeout=PC.cap(CAP_S)), **params)
                    cols = list(res.keys())
                    rows = [list(r.values()) for r in res]
                else:       # weights set inside the query's own transaction, then rolled back
                    tx = s.begin_transaction()
                    try:
                        for st in stmts[:-1]:
                            tx.run(st, **params).consume()
                        res = tx.run(stmts[-1], **params)
                        cols = list(res.keys())
                        rows = [list(r.values()) for r in res]
                    finally:
                        tx.rollback()
        finally:
            if "gds.graph.project" in text:
                with self.drv.session() as s:
                    s.run("CALL gds.graph.drop($g, false) YIELD graphName RETURN graphName", g=params["gname"]).consume()
        return cols, [[_cy(v) for v in r] for r in rows]


class ClickHouse:
    """The BI texts (and the IS/IC texts) on ClickHouse over HTTP, belief values as custom settings.
    A text ClickHouse's dialect rejects is recorded as an error cell (the BI subset is what runs)."""
    name = "clickhouse"

    def __init__(self, port: str):
        self.url = f"http://127.0.0.1:{port}/"

    def run(self, qid: str, inst: dict, setting: str):
        import urllib.parse
        import urllib.request
        from w3_snb_run import CATALOG, bind_sql, qtext, query_params, setting_values
        fam, path = CATALOG[qid]
        sv = setting_values(inst, setting)
        mode = "window" if setting == "window" else "instant"
        text = bind_sql(qtext(qid, path, setting), query_params(qid, inst, setting)).strip().rstrip(";")
        q = {"database": f"{fam}_{mode}", "default_format": "JSONCompact", "output_format_json_quote_64bit_integers": "0",
             "session_timezone": "UTC", **{f"custom_w3_{k}": str(int(x)) for k, x in sv.items()},
             **({"max_execution_time": str(int(__import__("prime_cap").active()))} if __import__("prime_cap").active() else {})}
        req = urllib.request.Request(self.url + "?" + urllib.parse.urlencode(q), data=text.encode())
        try:
            body = urllib.request.urlopen(req, timeout=3700).read()
        except urllib.error.HTTPError as e:
            raise RuntimeError(e.read().decode()[:500])
        d = json.loads(body)
        cols = [m["name"] for m in d["meta"]]
        types = [m["type"] for m in d["meta"]]
        rows = [[_chv(v, t) for v, t in zip(r, types)] for r in d["data"]]
        return cols, rows


def _chv(v, t: str):
    if v is None:
        return None
    if "DateTime" in t:
        return dt.datetime.fromisoformat(str(v).replace(" ", "T"))
    if t.startswith(("Date", "Nullable(Date")) and "Time" not in t:
        return dt.date.fromisoformat(str(v))
    return v


class AeonG(Cypher):
    """AeonG in two forms: `aeong` = native TT (`TT AS x`, x the AeonG time of the W3 commit T, found by
    bisection on the created-version count); `aeong-prop` = the Neo4j texts with all four timestamps as properties."""

    def __init__(self, uri: str, system: str, hist: str):
        super().__init__(uri, "neo4j")      # AeonG (Memgraph v2.3) wants an auth token, as the kit sends
        self.name, self.native = system, system == "aeong"
        rec = json.loads(Path(hist, "aeong-load.json").read_text())
        self.created = rec["created_by_tt"]
        # split load: the time map counts the marker nodes, one per completed tt group
        self.markers = rec.get("markers", False)
        if self.markers:
            self.created = [[tt, k + 1] for k, tt in enumerate(rec["group_tts"])]
        # the load-time map (the marker's transaction_ts per tt group); W3_AEONG_TIMEMAP=bisect forces the bisection
        self.group_ts = rec.get("group_ts") if os.environ.get("W3_AEONG_TIMEMAP", "ts") == "ts" else None
        self.xmap: dict[int, int] = {}

    def _count(self, x: int) -> int:
        lab = "W3Mark" if self.markers else "V"
        with self.drv.session() as s:
            return s.run(f"MATCH (n:{lab}) TT AS {x} RETURN count(n) AS c").single()["c"]

    def confirm_ceiling(self, ceiling_s: float) -> dict:
        """P13: the ceiling is the server's start-time timeout. One query known to run far longer than the ceiling
        must be aborted by the server at about the ceiling; otherwise AeonG's cells are "ceiling not enforceable"."""
        import threading
        heavy = "MATCH (a:V), (b:V), (c:V) RETURN count(*) AS n"
        out: dict = {}

        def go():
            t = time.time()
            try:
                with self.drv.session() as s:
                    s.run(heavy).consume()
                out.update(aborted=False, s=round(time.time() - t, 2))
            except Exception as e:  # noqa: BLE001 - the server's abort arrives as an error
                out.update(aborted=True, s=round(time.time() - t, 2), error=str(e)[:200])
        th = threading.Thread(target=go, daemon=True)
        th.start()
        th.join(ceiling_s * 3 + 30)
        if th.is_alive():
            out.update(aborted=False, s=None, note="still running at 3 x ceiling + 30 s")
        why = out.get("error", "").lower()
        out["enforced"] = (bool(out.get("aborted")) and ("timeout" in why or "abort" in why)
                           and out.get("s") is not None and out["s"] <= ceiling_s * 1.5 + 2)
        out["ceiling_s"] = ceiling_s
        return out

    def aeong_time(self, T: int) -> int:
        if T in self.xmap:
            return self.xmap[T]
        if self.group_ts:            # the load-time map: the last group with tt <= T, by its marker's commit time
            x = max((ts for tt, ts in self.group_ts if tt <= T), default=0)
            self.xmap[T] = x
            return x
        return self.bisect_time(T)

    def bisect_time(self, T: int) -> int:
        want = 0
        for tt, c in self.created:
            if tt <= T:
                want = c
        lo, hi = 0, 1
        while self._count(hi) < want:
            lo, hi = hi, hi * 2
        while lo + 1 < hi:          # smallest x with count(x) >= want
            mid = (lo + hi) // 2
            if self._count(mid) >= want:
                hi = mid
            else:
                lo = mid
        if self._count(hi) != want:
            raise RuntimeError(f"AeonG time map: no x with {want} created versions (got {self._count(hi)})")
        self.xmap[T] = hi
        return hi

    def timemap_check(self, Ts: list[int]) -> dict:
        """the load-time map against the bisection for each T, and the marker boundary: the last group's marker is
        visible at x and not at x - 1"""
        out, bad = [], []
        for T in sorted(set(Ts)):
            T = min(T, self.created[-1][0])
            g = max(tt for tt, _ in self.group_ts if tt <= T)
            x_ts = max(ts for tt, ts in self.group_ts if tt <= T)
            self.xmap.pop(T, None)
            x_b = self.bisect_time(T)
            with self.drv.session() as s:
                vis = s.run(f"MATCH (m:W3Mark {{tt: {g}}}) TT AS {x_ts} RETURN count(m) AS c").single()["c"]
                pre = s.run(f"MATCH (m:W3Mark {{tt: {g}}}) TT AS {x_ts - 1} RETURN count(m) AS c").single()["c"]
            row = {"T": T, "group": g, "x_ts": x_ts, "x_bisect": x_b, "visible_at_x": vis, "visible_at_x_minus_1": pre}
            out.append(row)
            if x_ts != x_b or vis != 1 or pre != 0:
                bad.append(row)
        return {"T_checked": len(out), "mismatches": bad, "rows": out}

    def run(self, qid: str, inst: dict, setting: str):
        if not self.native:
            return super().run(qid, inst, setting)
        from w3_snb_run import setting_values
        if setting == "window":
            raise NotImplementedError("AeonG: no sequenced valid-time form")
        f = W3CY / "aeong" / f"{qid.replace('TLDBC-', '')}.cypher"
        alt = os.environ.get("W3_AEONG_TEXT_VARIANT")      # a test form, e.g. "collect" -> IS7.collect.cypher
        if alt and f.with_suffix(f".{alt}.cypher").exists():
            f = f.with_suffix(f".{alt}.cypher")
        if not f.exists():
            raise NotImplementedError(f"no AeonG text for {qid}")
        sv = setting_values(inst, setting)
        x = self.aeong_time(min(sv["T"], self.created[-1][0]))
        one = lambda v: f"({v}.tt_e = {OPEN} AND {v}.vt_s <= $V AND $V < {v}.vt_e)"
        text = f.read_text().replace("$AT", str(x))
        text = macro(text, "VISTTALL", lambda a: f"all(_x IN {a[0]} WHERE {one('_x')})")
        text = macro(text, "VISTT", lambda a: " AND ".join(one(y) for y in a))
        params = {"V": sv["V"]}
        for k, (ty, v) in inst["params"].items():
            params[k] = _ldt(v) if ty == "DATETIME" else (_ldt(v).date() if ty == "DATE" else v)
        with self.drv.session() as s:
            res = s.run(text, **params)
            cols = list(res.keys())
            rows = [list(r.values()) for r in res]
        return cols, [[_cy(v) for v in r] for r in rows]


def _cy(v):
    try:
        import neo4j.time as nt
        if isinstance(v, nt.DateTime):
            return v.to_native()
        if isinstance(v, nt.Date):
            return v.to_native()
    except Exception:  # noqa: BLE001
        pass
    return v


def cmd_aeong_timemap_check(a) -> int:
    A = AeonG(a.uri, "aeong", a.hist)
    if not A.group_ts:
        raise SystemExit("no load-time map (group_ts) in aeong-load.json: load with --chunk > 0")
    Ts = [x["T"] for x in map(json.loads, open(a.params))] + [OPEN - 1]
    rec = A.timemap_check(Ts)
    Path(a.out).write_text(json.dumps(rec, indent=1) + "\n")
    print(json.dumps({k: v for k, v in rec.items() if k != "rows"}))
    return 4 if rec["mismatches"] else 0


def cmd_aeong_ceiling(a) -> int:
    rec = AeonG(a.uri, "aeong", a.hist).confirm_ceiling(a.ceiling)
    rec["verdict"] = "ceiling enforced" if rec["enforced"] else "ceiling not enforceable"
    print(json.dumps(rec))
    if a.out:
        Path(a.out).write_text(json.dumps(rec) + "\n")
    return 0 if rec["enforced"] else 3


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sp = p.add_subparsers(dest="cmd", required=True)
    g = sp.add_parser("pg-load"); g.add_argument("--hist", required=True); g.add_argument("--dsn", required=True)
    g.add_argument("--tmp", default=os.environ.get("LOCAL", "/tmp"))
    c = sp.add_parser("ch-load"); c.add_argument("--hist", required=True); c.add_argument("--port", default=os.environ.get("EXTCMP_CH_HTTP_PORT", "18123"))
    y = sp.add_parser("cypher-load"); y.add_argument("--hist", required=True); y.add_argument("--uri", required=True)
    y.add_argument("--system", default="neo4j", choices=["neo4j", "memgraph"])
    e = sp.add_parser("aeong-load"); e.add_argument("--hist", required=True); e.add_argument("--uri", required=True)
    e.add_argument("--chunk", type=int, default=5000, help="items per AeonG transaction; 0 = one transaction per tt group")
    m = sp.add_parser("aeong-timemap-check", help="the load-time AeonG time map against the bisection, for every T of the parameter files; exit 4 on a mismatch")
    m.add_argument("--hist", required=True); m.add_argument("--uri", required=True); m.add_argument("--params", required=True)
    m.add_argument("--out", required=True)
    k = sp.add_parser("aeong-ceiling", help="confirm that the server aborts a query at the ceiling; exit 3 if not")
    k.add_argument("--hist", required=True); k.add_argument("--uri", required=True)
    k.add_argument("--ceiling", type=float, default=10.0); k.add_argument("--out")
    a = p.parse_args()
    return {"pg-load": cmd_pg_load, "ch-load": cmd_ch_load, "cypher-load": cmd_cypher_load,
            "aeong-load": cmd_aeong_load, "aeong-ceiling": cmd_aeong_ceiling, "aeong-timemap-check": cmd_aeong_timemap_check}[a.cmd](a)


if __name__ == "__main__":
    sys.exit(main())
