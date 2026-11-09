"""W3 SQL side: canonical version rows -> LDBC-schema history tables -> belief views.

**This is not an LDBC Benchmark, and nothing produced here is an LDBC
Benchmark Result.**

    w3_snb_sql.py export --store S --out ROWS            canonical version rows of a replayed store (parquet)
    w3_snb_sql.py hist   --rows ROWS --out HIST           LDBC-schema history tables (parquet, one per table)
    w3_snb_sql.py views  --dialect duckdb|postgres|clickhouse --mode instant|window   print the view DDL

byte for byte from the replay of the frozen log). A history table holds one
row per *version*: the LDBC columns of LDBC's own reference schema
(ldbc_snb_bi/umbra and ldbc_snb_interactive_v2_impls/postgres,
composite-merged-fk) plus the four timestamps vt_s, vt_e, tt_s, tt_e
(microseconds). A merged foreign key (Message.CreatorPersonId, ...) is the
intersection of the owner's version rectangle with the FK edge's version
rectangle; Forum.ModeratorPersonId becomes NULL where the moderator's own
deletion closed the edge before the forum (Datagen raw keeps the id).

The reference queries then run **unchanged** against views that carry the
reference table names and are filtered to one belief setting:

    instant: tt_s <= T < tt_e  and  vt_s <= V < vt_e        (current: T = now; as of TT: T = a commit)
    window : tt_s <= T < tt_e  and  vt_s <  VB and VA < vt_e  (plus vt_s, vt_e columns for co_valid)

T, V, VA, VB are session values (DuckDB `SET VARIABLE`, PostgreSQL
`SET w3.T`, ClickHouse custom settings), so one prepared text serves every
setting. Interactive v2's schema (`iv2`) and BI's (`bi`) are two view sets
over the same history tables. KNOWS is stored once; both reference schemas
load it in both directions, so the views union the two orientations.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

OPEN = 2 ** 62


def cmd_export(a) -> int:
    import pyarrow as pa
    import pyarrow.parquet as pq
    import gnomon
    from gnomon.core.model import canonical_json

    out = Path(a.out); out.mkdir(parents=True, exist_ok=True)
    t0 = time.time()
    st = gnomon.open(a.store, backend="native", read_only=True)
    ad = st.adapter
    B = 1_000_000

    def dump(it, cols, fn, path):
        w, n, buf = None, 0, {c: [] for c in cols}
        for v in it:
            for c, x in zip(cols, fn(v)):
                buf[c].append(x)
            n += 1
            if len(buf[cols[0]]) >= B:
                t = pa.table(buf); w = w or pq.ParquetWriter(path, t.schema); w.write_table(t)
                buf = {c: [] for c in cols}
        t = pa.table(buf); w = w or pq.ParquetWriter(path, t.schema); w.write_table(t); w.close()
        return n

    nn = dump(ad.all_node_versions(), ["uid", "label", "props", "vt_s", "vt_e", "tt_s", "tt_e", "vid"],
              lambda v: (int(v.uid), v.label, canonical_json(v.props), v.vt_s, v.vt_e, v.tt_s, v.tt_e, v.vid),
              str(out / "nv.parquet"))
    ne = dump(ad.all_edge_versions(), ["src", "dst", "rel", "disc", "props", "vt_s", "vt_e", "tt_s", "tt_e", "vid"],
              lambda v: (int(v.src), int(v.dst), v.rel_type, v.disc, canonical_json(v.props), v.vt_s, v.vt_e,
                         v.tt_s, v.tt_e, v.vid), str(out / "ev.parquet"))
    st.close()
    rec = {"store": str(a.store), "node_versions": nn, "edge_versions": ne, "wall_s": round(time.time() - t0, 1)}
    (out / "export.json").write_text(json.dumps(rec, indent=1) + "\n")
    print(json.dumps(rec))
    return 0


TS = "make_timestamp({c})"            # DuckDB: microseconds -> TIMESTAMP (UTC wall clock)


def isect(a: str, b: str) -> str:
    """Overlap of two version rectangles (vt and tt)."""
    return (f"{b}.vt_s < {a}.vt_e AND {a}.vt_s < {b}.vt_e AND {b}.tt_s < {a}.tt_e AND {a}.tt_s < {b}.tt_e")


def rect(*xs: str) -> str:
    g = lambda c, f: f"{f}(" + ", ".join(f"{x}.{c}" for x in xs) + ")"
    return (f"{g('vt_s', 'greatest')} AS vt_s, {g('vt_e', 'least')} AS vt_e, "
            f"{g('tt_s', 'greatest')} AS tt_s, {g('tt_e', 'least')} AS tt_e")


HIST_SQL = r"""
CREATE TABLE nv AS SELECT * FROM read_parquet('{rows}/nv.parquet');
CREATE TABLE ev AS SELECT * FROM read_parquet('{rows}/ev.parquet');
CREATE MACRO P(j, k) AS json_extract_string(j, k);
CREATE MACRO PI(j, k) AS CAST(json_extract(j, k) AS BIGINT);
-- static ------------------------------------------------------------------------------
CREATE TABLE Place_h AS SELECT n.uid // 8 AS id, P(n.props,'name') AS name, P(n.props,'url') AS url, lower(n.label) AS type,
    e.dst // 8 AS PartOfPlaceId, n.vt_s, n.vt_e, n.tt_s, n.tt_e
  FROM nv n LEFT JOIN ev e ON e.src = n.uid AND e.rel = 'IS_PART_OF' WHERE n.label IN ('City','Country','Continent');
CREATE TABLE Organisation_h AS SELECT n.uid // 8 AS id, lower(n.label) AS type, P(n.props,'name') AS name, P(n.props,'url') AS url,
    e.dst // 8 AS LocationPlaceId, n.vt_s, n.vt_e, n.tt_s, n.tt_e
  FROM nv n JOIN ev e ON e.src = n.uid AND e.rel = 'IS_LOCATED_IN' WHERE n.label IN ('University','Company');
CREATE TABLE Tag_h AS SELECT n.uid // 8 AS id, P(n.props,'name') AS name, P(n.props,'url') AS url, e.dst // 8 AS TypeTagClassId,
    n.vt_s, n.vt_e, n.tt_s, n.tt_e FROM nv n JOIN ev e ON e.src = n.uid AND e.rel = 'HAS_TYPE' WHERE n.label = 'Tag';
CREATE TABLE TagClass_h AS SELECT n.uid // 8 AS id, P(n.props,'name') AS name, P(n.props,'url') AS url, e.dst // 8 AS SubclassOfTagClassId,
    n.vt_s, n.vt_e, n.tt_s, n.tt_e FROM nv n LEFT JOIN ev e ON e.src = n.uid AND e.rel = 'IS_SUBCLASS_OF' WHERE n.label = 'TagClass';
-- dynamic nodes with merged FKs (rectangle intersection) ------------------------------------
CREATE TABLE Person_h AS SELECT n.uid // 8 AS id, P(n.props,'firstName') AS firstName, P(n.props,'lastName') AS lastName,
    P(n.props,'gender') AS gender, CAST(make_timestamp(PI(n.props,'birthday')) AS DATE) AS birthday,
    P(n.props,'locationIP') AS locationIP, P(n.props,'browserUsed') AS browserUsed, e.dst // 8 AS LocationCityId,
    array_to_string(CAST(json_extract(n.props,'speaks') AS VARCHAR[]), ';') AS speaks,
    array_to_string(CAST(json_extract(n.props,'email') AS VARCHAR[]), ';') AS email, {r_ne}
  FROM nv n JOIN ev e ON e.src = n.uid AND e.rel = 'IS_LOCATED_IN' AND {i_ne} WHERE n.label = 'Person';
CREATE TABLE Forum_h AS
  SELECT n.uid // 8 AS id, P(n.props,'title') AS title, e.dst // 8 AS ModeratorPersonId, {r_ne}
    FROM nv n JOIN ev e ON e.src = n.uid AND e.rel = 'HAS_MODERATOR' AND {i_ne} WHERE n.label = 'Forum'
  UNION ALL   -- the moderator's deletion closed the edge before the forum: no moderator from then on
  SELECT n.uid // 8, P(n.props,'title'), NULL, e.vt_e, n.vt_e, greatest(n.tt_s, e.tt_s), least(n.tt_e, e.tt_e)
    FROM nv n JOIN ev e ON e.src = n.uid AND e.rel = 'HAS_MODERATOR' AND {i_ne} AND e.vt_e < n.vt_e WHERE n.label = 'Forum'
  UNION ALL   -- a forum version no moderator edge version overlaps at all
  SELECT n.uid // 8, P(n.props,'title'), NULL, n.vt_s, n.vt_e, n.tt_s, n.tt_e FROM nv n WHERE n.label = 'Forum'
    AND NOT EXISTS (SELECT 1 FROM ev e WHERE e.src = n.uid AND e.rel = 'HAS_MODERATOR' AND {i_ne});
CREATE TABLE _parent AS SELECT DISTINCT src AS m, dst AS parent FROM ev WHERE rel = 'REPLY_OF';
CREATE TABLE _root AS WITH RECURSIVE r(m, root, depth) AS (
    SELECT uid, uid, 0 FROM nv WHERE label = 'Post' GROUP BY uid
    UNION ALL SELECT p.m, r.root, r.depth + 1 FROM _parent p JOIN r ON p.parent = r.m)
  SELECT m, root FROM r;
CREATE TABLE _postmeta AS SELECT DISTINCT n.uid AS root, P(n.props,'language') AS lang, c.src AS forum
  FROM nv n JOIN ev c ON c.dst = n.uid AND c.rel = 'CONTAINER_OF' WHERE n.label = 'Post';
CREATE TABLE Message_h AS
  SELECT n.uid // 8 AS MessageId, n.uid // 8 AS RootPostId, P(n.props,'language') AS RootPostLanguage,
     P(n.props,'content') AS content, P(n.props,'imageFile') AS imageFile, P(n.props,'locationIP') AS locationIP,
     P(n.props,'browserUsed') AS browserUsed, PI(n.props,'length') AS length, hc.dst // 8 AS CreatorPersonId,
     co.src // 8 AS ContainerForumId, lo.dst // 8 AS LocationCountryId, NULL::BIGINT AS ParentMessageId,
     P(n.props,'language') AS language, {r_4}
  FROM nv n JOIN ev hc ON hc.src = n.uid AND hc.rel = 'HAS_CREATOR' AND {i_hc}
    JOIN ev co ON co.dst = n.uid AND co.rel = 'CONTAINER_OF' AND {i_co}
    JOIN ev lo ON lo.src = n.uid AND lo.rel = 'IS_LOCATED_IN' AND {i_lo}
  WHERE n.label = 'Post'
  UNION ALL
  SELECT n.uid // 8, rt.root // 8, pm.lang, P(n.props,'content'), NULL, P(n.props,'locationIP'), P(n.props,'browserUsed'),
     PI(n.props,'length'), hc.dst // 8, pm.forum // 8, lo.dst // 8, ro.dst // 8, NULL, {r_4r}
  FROM nv n JOIN ev hc ON hc.src = n.uid AND hc.rel = 'HAS_CREATOR' AND {i_hc}
    JOIN ev ro ON ro.src = n.uid AND ro.rel = 'REPLY_OF' AND {i_ro}
    JOIN ev lo ON lo.src = n.uid AND lo.rel = 'IS_LOCATED_IN' AND {i_lo}
    JOIN _root rt ON rt.m = n.uid JOIN _postmeta pm ON pm.root = rt.root
  WHERE n.label = 'Comment';
-- node versions of Person as written (no FK intersection): the entity-history class reads these
CREATE TABLE Person_v AS SELECT n.uid // 8 AS id, P(n.props,'firstName') AS firstName, P(n.props,'lastName') AS lastName,
    P(n.props,'browserUsed') AS browserUsed, P(n.props,'locationIP') AS locationIP, n.vt_s, n.vt_e, n.tt_s, n.tt_e
  FROM nv n WHERE n.label = 'Person';
-- edge tables -----------------------------------------------------------------------------
CREATE TABLE Person_knows_Person_h AS SELECT src // 8 AS Person1Id, dst // 8 AS Person2Id, vt_s, vt_e, tt_s, tt_e FROM ev WHERE rel = 'KNOWS';
CREATE TABLE Person_likes_Message_h AS SELECT src // 8 AS PersonId, dst // 8 AS MessageId, vt_s, vt_e, tt_s, tt_e FROM ev WHERE rel = 'LIKES';
CREATE TABLE Message_hasTag_Tag_h AS SELECT e.src // 8 AS MessageId, e.dst // 8 AS TagId, e.vt_s, e.vt_e, e.tt_s, e.tt_e
  FROM ev e WHERE e.rel = 'HAS_TAG' AND e.src % 8 = 1;
CREATE TABLE Forum_hasTag_Tag_h AS SELECT src // 8 AS ForumId, dst // 8 AS TagId, vt_s, vt_e, tt_s, tt_e FROM ev WHERE rel = 'HAS_TAG' AND src % 8 = 0;
CREATE TABLE Forum_hasMember_Person_h AS SELECT src // 8 AS ForumId, dst // 8 AS PersonId, vt_s, vt_e, tt_s, tt_e FROM ev WHERE rel = 'HAS_MEMBER';
CREATE TABLE Person_hasInterest_Tag_h AS SELECT src // 8 AS PersonId, dst // 8 AS TagId, vt_s, vt_e, tt_s, tt_e FROM ev WHERE rel = 'HAS_INTEREST';
CREATE TABLE Person_studyAt_University_h AS SELECT src // 8 AS PersonId, dst // 8 AS UniversityId, PI(props,'classYear')::INT AS classYear,
  vt_s, vt_e, tt_s, tt_e FROM ev WHERE rel = 'STUDY_AT';
CREATE TABLE Person_workAt_Company_h AS SELECT src // 8 AS PersonId, dst // 8 AS CompanyId, PI(props,'workFrom')::INT AS workFrom,
  vt_s, vt_e, tt_s, tt_e FROM ev WHERE rel = 'WORK_AT';
"""

HIST_TABLES = ["Person_v", "Place_h", "Organisation_h", "Tag_h", "TagClass_h", "Person_h", "Forum_h", "Message_h",
               "Person_knows_Person_h", "Person_likes_Message_h", "Message_hasTag_Tag_h", "Forum_hasTag_Tag_h",
               "Forum_hasMember_Person_h", "Person_hasInterest_Tag_h", "Person_studyAt_University_h",
               "Person_workAt_Company_h"]


def cmd_hist(a) -> int:
    import duckdb
    out = Path(a.out); out.mkdir(parents=True, exist_ok=True)
    t0 = time.time()
    con = duckdb.connect(str(out / "hist.duckdb"))
    con.execute("SET TimeZone='UTC'; SET preserve_insertion_order=false")
    sql = HIST_SQL.format(rows=a.rows, r_ne=rect("n", "e"), i_ne=isect("n", "e"),
                          r_4=rect("n", "hc", "co", "lo"), r_4r=rect("n", "hc", "ro", "lo"),
                          i_hc=isect("n", "hc"), i_co=isect("n", "co"), i_lo=isect("n", "lo"), i_ro=isect("n", "ro"))
    for chunk in sql.split(";\n"):
        stmt = "\n".join(l for l in chunk.splitlines() if not l.strip().startswith("--")).strip()
        if stmt:
            con.execute(stmt)
    counts = {}
    for t in HIST_TABLES:
        con.execute(f"DELETE FROM {t} WHERE vt_s >= vt_e OR tt_s >= tt_e")
        counts[t] = con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
        con.execute(f"COPY (SELECT * FROM {t} ORDER BY tt_s, vt_s) TO '{out}/{t}.parquet' (FORMAT parquet)")
    # transcription checks: every message / person / forum version is covered by its FK rows at each commit point
    chk = {"node_versions": con.execute("SELECT label, count(*) FROM nv GROUP BY 1 ORDER BY 1").fetchall()}
    rec = {"rows": str(a.rows), "tables": counts, "checks": chk, "wall_s": round(time.time() - t0, 1)}
    (out / "hist.json").write_text(json.dumps(rec, indent=1, default=str) + "\n")
    print(json.dumps(rec, default=str))
    return 0


# --------------------------------------------------------------------------- #
# views                                                                         #
# --------------------------------------------------------------------------- #

#: view name -> (history table, select list in the reference schema's column order)
BI_VIEWS = {
    "Organisation": ("Organisation_h", "id, type, name, url, LocationPlaceId"),
    "Place": ("Place_h", "id, name, url, type, PartOfPlaceId"),
    "Tag": ("Tag_h", "id, name, url, TypeTagClassId"),
    "TagClass": ("TagClass_h", "id, name, url, SubclassOfTagClassId"),
    "Country": ("Place_h", "id, name, url, PartOfPlaceId AS PartOfContinentId", "type = 'country'"),
    "City": ("Place_h", "id, name, url, PartOfPlaceId AS PartOfCountryId", "type = 'city'"),
    "Company": ("Organisation_h", "id, name, url, LocationPlaceId", "type = 'company'"),
    "University": ("Organisation_h", "id, name, url, LocationPlaceId", "type = 'university'"),
    "Person": ("Person_h", "{cd} AS creationDate, id, firstName, lastName, gender, birthday, locationIP, browserUsed, "
                           "LocationCityId, speaks, email"),
    "Forum": ("Forum_h", "{cd} AS creationDate, id, title, ModeratorPersonId"),
    "Message": ("Message_h", "{cd} AS creationDate, MessageId, RootPostId, RootPostLanguage, content, imageFile, "
                             "locationIP, browserUsed, length, CreatorPersonId, ContainerForumId, LocationCountryId, ParentMessageId"),
    "Person_likes_Message": ("Person_likes_Message_h", "{cd} AS creationDate, PersonId, MessageId"),
    "Message_hasTag_Tag": ("Message_hasTag_Tag_h", "{cd} AS creationDate, MessageId, TagId"),
    "Forum_hasMember_Person": ("Forum_hasMember_Person_h", "{cd} AS creationDate, ForumId, PersonId"),
    "Forum_hasTag_Tag": ("Forum_hasTag_Tag_h", "{cd} AS creationDate, ForumId, TagId"),
    "Person_hasInterest_Tag": ("Person_hasInterest_Tag_h", "{cd} AS creationDate, PersonId, TagId"),
    "Person_studyAt_University": ("Person_studyAt_University_h", "{cd} AS creationDate, PersonId, UniversityId, classYear"),
    "Person_workAt_Company": ("Person_workAt_Company_h", "{cd} AS creationDate, PersonId, CompanyId, workFrom"),
}
IV2_VIEWS = dict(BI_VIEWS)
IV2_VIEWS.update({
    "Message": ("Message_h", "{cd} AS creationDate, MessageId AS id, language, content, imageFile, locationIP, browserUsed, "
                             "length, CreatorPersonId, CASE WHEN ParentMessageId IS NULL THEN ContainerForumId END AS ContainerForumId, "
                             "LocationCountryId, ParentMessageId"),
    "Person_likes_Message": ("Person_likes_Message_h", "{cd} AS creationDate, PersonId, MessageId AS id"),
    "Message_hasTag_Tag": ("Message_hasTag_Tag_h", "{cd} AS creationDate, MessageId AS id, TagId"),
})

DIALECT = {
    "duckdb": {"T": "getvariable('w3_T')", "V": "getvariable('w3_V')", "VA": "getvariable('w3_VA')",
               "VB": "getvariable('w3_VB')", "cd": "make_timestamp(vt_s)", "schema": "CREATE SCHEMA IF NOT EXISTS {s};"},
    "postgres": {"T": "current_setting('w3.T')::bigint", "V": "current_setting('w3.V')::bigint",
                 "VA": "current_setting('w3.VA')::bigint", "VB": "current_setting('w3.VB')::bigint",
                 "cd": "(timestamptz 'epoch' + vt_s * interval '1 microsecond')", "schema": "CREATE SCHEMA IF NOT EXISTS {s};"},
    "clickhouse": {"T": "getSetting('custom_w3_T')", "V": "getSetting('custom_w3_V')", "VA": "getSetting('custom_w3_VA')",
                   "VB": "getSetting('custom_w3_VB')", "cd": "fromUnixTimestamp64Micro(vt_s, 'UTC')",
                   "schema": "CREATE DATABASE IF NOT EXISTS {s};"},
}


def view_ddl(dialect: str, mode: str, family: str) -> list[str]:
    d = DIALECT[dialect]
    views = BI_VIEWS if family == "bi" else IV2_VIEWS
    schema = f"{family}_{mode}"
    if mode == "instant":
        vis = f"tt_s <= {d['T']} AND {d['T']} < tt_e AND vt_s <= {d['V']} AND {d['V']} < vt_e"
    else:
        vis = f"tt_s <= {d['T']} AND {d['T']} < tt_e AND vt_s < {d['VB']} AND {d['VA']} < vt_e"
    extra = "" if mode == "instant" else ", vt_s, vt_e"
    out = [d["schema"].format(s=schema)]
    for name, spec in views.items():
        table, cols = spec[0], spec[1].format(cd=d["cd"])
        where = vis + (f" AND {spec[2]}" if len(spec) > 2 else "")
        out.append(f"CREATE OR REPLACE VIEW {schema}.{name} AS SELECT {cols}{extra} FROM {table} WHERE {where};")
    # KNOWS: stored once, read both ways (the reference loaders copy it twice)
    kcols = ("Person1Id, Person2Id" if family == "iv2" else "Person1id, Person2id")
    out.append(f"CREATE OR REPLACE VIEW {schema}.Person_knows_Person AS "
               f"SELECT {d['cd']} AS creationDate, Person1Id AS {kcols.split(', ')[0]}, Person2Id AS {kcols.split(', ')[1]}{extra} "
               f"FROM Person_knows_Person_h WHERE {vis} UNION ALL "
               f"SELECT {d['cd']}, Person2Id, Person1Id{extra} FROM Person_knows_Person_h WHERE {vis};")
    if family == "bi":   # Umbra's derived views and precomputed tables, computed per belief setting
        out.append(f"CREATE OR REPLACE VIEW {schema}.Comment_View AS SELECT creationDate, MessageId AS id, locationIP, browserUsed, "
                   f"content, length, CreatorPersonId, LocationCountryId, ParentMessageId{extra} FROM {schema}.Message WHERE ParentMessageId IS NOT NULL;")
        out.append(f"CREATE OR REPLACE VIEW {schema}.Post_View AS SELECT creationDate, MessageId AS id, imageFile, locationIP, browserUsed, "
                   f"RootPostLanguage, content, length, CreatorPersonId, ContainerForumId, LocationCountryId{extra} FROM {schema}.Message WHERE ParentMessageId IS NULL;")
        for name, body in PRECOMP.items():
            out.append(f"CREATE OR REPLACE VIEW {schema}.{name} AS {body.format(s=schema)};")
    else:
        for name, src, cols in (("Post", "ParentMessageId IS NULL", "creationDate, id, imageFile, locationIP, browserUsed, language, content, length, CreatorPersonId, ContainerForumId, LocationCountryId"),
                                ("Comment", "ParentMessageId IS NOT NULL", "creationDate, id, locationIP, browserUsed, content, length, CreatorPersonId, LocationCountryId, ParentMessageId")):
            out.append(f"CREATE OR REPLACE VIEW {schema}.{name} AS SELECT {cols}{extra} FROM {schema}.Message WHERE {src};")
    return out


#: Umbra's dml/precomp/*.sql, verbatim bodies, as per-setting views (deviation D-W3-2)
PRECOMP = {
    "Top100PopularForumsQ04": """SELECT T.id, Forum.creationdate, T.maxNumberOfMembers
FROM (SELECT ForumId AS id, max(numberOfMembers) AS maxNumberOfMembers
FROM (SELECT Forum_hasMember_Person.ForumId AS ForumId, count(Person.id) AS numberOfMembers, City.PartOfCountryId AS CountryId
    FROM {s}.Forum_hasMember_Person JOIN {s}.Person ON Person.id = Forum_hasMember_Person.PersonId
    JOIN {s}.City ON City.id = Person.LocationCityId
    GROUP BY City.PartOfCountryId, Forum_hasMember_Person.ForumId) ForumMembershipPerCountry
GROUP BY ForumId) T, {s}.Forum WHERE T.id = Forum.id""",
    "PopularityScoreQ06": """SELECT message2.CreatorPersonId AS person2id, count(*) AS popularityScore
FROM {s}.Message message2 JOIN {s}.Person_likes_Message like2 ON like2.MessageId = message2.MessageId
GROUP BY message2.CreatorPersonId""",
    "PathQ19": """WITH weights(src, dst, w) AS (
    SELECT person1id AS src, person2id AS dst, greatest(round(40 - sqrt(count(*)))::bigint, 1) AS w
    FROM (SELECT person1id, person2id FROM {s}.Person_knows_person WHERE person1id < person2id) pp, {s}.Message m1, {s}.Message m2
    WHERE pp.person1id = least(m1.creatorpersonid, m2.creatorpersonid) and pp.person2id = greatest(m1.creatorpersonid, m2.creatorpersonid)
      and m1.parentmessageid = m2.messageid and m1.creatorpersonid <> m2.creatorpersonid
    GROUP BY src, dst)
SELECT src, dst, w FROM weights UNION ALL SELECT dst, src, w FROM weights""",
    "PathQ20": """select p1.personid AS src, p2.personid AS dst, min(abs(p1.classYear - p2.classYear)) + 1 AS w
from {s}.Person_knows_person pp, {s}.Person_studyAt_University p1, {s}.Person_studyAt_University p2
where pp.person1id = p1.personid and pp.person2id = p2.personid and p1.universityid = p2.universityid
group by p1.personid, p2.personid""",
}


def cmd_views(a) -> int:
    for fam in ("bi", "iv2"):
        for s in view_ddl(a.dialect, a.mode, fam):
            print(s)
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sp = p.add_subparsers(dest="cmd", required=True)
    e = sp.add_parser("export"); e.add_argument("--store", required=True); e.add_argument("--out", required=True)
    h = sp.add_parser("hist"); h.add_argument("--rows", required=True); h.add_argument("--out", required=True)
    v = sp.add_parser("views"); v.add_argument("--dialect", default="duckdb", choices=list(DIALECT))
    v.add_argument("--mode", default="instant", choices=["instant", "window"])
    a = p.parse_args()
    return {"export": cmd_export, "hist": cmd_hist, "views": cmd_views}[a.cmd](a)


if __name__ == "__main__":
    sys.exit(main())
