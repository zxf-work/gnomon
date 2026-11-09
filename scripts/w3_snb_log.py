"""W3: LDBC SNB Datagen raw output -> frozen bi-temporal Gnomon event logs.

**This is not an LDBC Benchmark, and nothing produced here is an LDBC
Benchmark Result.** LDBC SNB Datagen output is used under its licence.

Two steps:

    w3_snb_log.py base   --raw DIR --out BASE [--history-mult H]
    w3_snb_log.py inject --base BASE --rho R --lag mix|same|months|years --seed S --out LOG

`base` reads Datagen 0.5.1 *raw mode* (composite-merged-fk, parquet), maps it to
id*8+hierarchy tag; KNOWS once with Person1Id < Person2Id; FK columns become
edges owned by the entity; valid time [creationDate, deletionDate) in
microseconds, deletionDate >= 2013-01-01 -> OPEN_END), splits it at LDBC's
bulk-load threshold (RawToInteractiveTransform.calculateBulkLoadThreshold) and
writes the base event-log records: the initial snapshot at TT0 = threshold,
then one transaction per simulation day (inserts, then deletes as `retract` at
deletionDate) committed at the day's end. `--history-mult H` moves the
threshold earlier so the update stream holds H times the default stream's
elements (TPC-BiH's history-length axis at a fixed final graph).

`inject` adds corrections after each day's commit (1 s later) at density
rho = corrections / base elements, with a stated lag mix, and writes one
self-contained frozen log plus `.meta.json` (counts, realised lags) and
`.corrections.parquet` (the ground truth of every correction).

Correction kinds (each respects the SNB lifespan rules: 10 s minimum spacing
between dependent creations, an edge inside both endpoints' lifespans,
membership before posting, deletion >= creation + 10 s):
  CS  shifted creation date of a Person/Forum/Post/Comment (owned FK edges move with it)
  DS  shifted deletion date of a deleted KNOWS/LIKES/HAS_MEMBER edge
  RD  retroactive deletion of a live KNOWS/LIKES edge at its true end
  UD  un-deletion of a deleted KNOWS/LIKES/HAS_MEMBER edge (endpoints alive)
  PA  person-property amendment (`correct` over the version's interval)
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from pathlib import Path

import duckdb
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from gnomon.core.model import canonical_json, sha256_hex  # noqa: E402

OPEN = 2 ** 62
SIM_START_MS = 1262304000000          # 2010-01-01 (Datagen simulation start)
SIM_END_MS = 1356998400000            # 2013-01-01 (Datagen simulation end)
NOW_US = SIM_END_MS * 1000            # D6: reads at valid instant "now"
DAY_US = 86_400_000_000
SEC_US = 1_000_000
GAP_US = 10 * SEC_US                  # SNB minimum spacing
CHUNK = 50_000
TAG = {"Forum": 0, "Message": 1, "Organisation": 2, "Person": 3, "Place": 4,
       "Tag": 5, "TagClass": 6}
HEADER = '{"format":"gnomon-events","version":1}\n'
# element classes (injector pools)
C_STATIC, C_PERSON, C_FORUM, C_POST, C_COMMENT, C_KNOWS, C_LIKES, C_MEMBER, C_OWNED, C_OTHER = range(10)


def uid(h: str, col: str) -> str:
    return f"(CAST({col} AS BIGINT) * 8 + {TAG[h]})"


def us(col: str) -> str:
    return f"(CAST({col} AS BIGINT) * 1000)"


def ve(col: str) -> str:
    return f"(CASE WHEN {col} >= {SIM_END_MS} THEN {OPEN} ELSE CAST({col} AS BIGINT) * 1000 END)"


# --------------------------------------------------------------------------- #
# base                                                                          #
# --------------------------------------------------------------------------- #

def build_elements(con: duckdb.DuckDBPyConnection, raw: Path) -> dict:
    R = raw / "graphs/parquet/raw/composite-merged-fk"
    for kind in ("static", "dynamic"):
        for e in sorted(os.listdir(R / kind)):
            if e[0] in "_.":
                continue
            con.execute(f"CREATE VIEW r_{e} AS SELECT * FROM read_parquet('{R}/{kind}/{e}/*.parquet')")
    J = "json_object"
    con.execute(f"""
    CREATE TABLE n AS
    SELECT {uid('Place','id')} AS uid, upper(type[1]) || type[2:] AS label,
           {J}('name', name, 'url', url, 'id', id)::VARCHAR AS props, 0::BIGINT AS vs, {OPEN}::BIGINT AS ve, {C_STATIC} AS cls FROM r_Place
    UNION ALL SELECT {uid('Organisation','id')}, upper(type[1]) || type[2:], {J}('name', name, 'url', url, 'id', id)::VARCHAR, 0, {OPEN}, {C_STATIC} FROM r_Organisation
    UNION ALL SELECT {uid('Tag','id')}, 'Tag', {J}('name', name, 'url', url, 'id', id)::VARCHAR, 0, {OPEN}, {C_STATIC} FROM r_Tag
    UNION ALL SELECT {uid('TagClass','id')}, 'TagClass', {J}('name', name, 'url', url, 'id', id)::VARCHAR, 0, {OPEN}, {C_STATIC} FROM r_TagClass
    UNION ALL SELECT {uid('Person','id')}, 'Person', {J}('firstName', firstName, 'lastName', lastName, 'gender', gender,
           'birthday', CAST(birthday AS BIGINT) * 1000, 'locationIP', locationIP, 'browserUsed', browserUsed,
           'speaks', string_split(language, ';'), 'email', string_split(email, ';'), 'id', id)::VARCHAR,
           {us('creationDate')}, {ve('deletionDate')}, {C_PERSON} FROM r_Person
    UNION ALL SELECT {uid('Forum','id')}, 'Forum', {J}('title', title, 'id', id)::VARCHAR,
           {us('creationDate')}, {ve('deletionDate')}, {C_FORUM} FROM r_Forum
    UNION ALL SELECT {uid('Message','id')}, 'Post', {J}('imageFile', imageFile, 'locationIP', locationIP,
           'browserUsed', browserUsed, 'language', language, 'content', content, 'length', length, 'id', id)::VARCHAR,
           {us('creationDate')}, {ve('deletionDate')}, {C_POST} FROM r_Post
    UNION ALL SELECT {uid('Message','id')}, 'Comment', {J}('locationIP', locationIP, 'browserUsed', browserUsed,
           'content', content, 'length', length, 'id', id)::VARCHAR,
           {us('creationDate')}, {ve('deletionDate')}, {C_COMMENT} FROM r_Comment
    """)
    zero_nodes = con.execute("SELECT count(*) FROM n WHERE vs >= ve").fetchone()[0]
    con.execute("DELETE FROM n WHERE vs >= ve")
    # edges: (src, dst, rel, props, vs, ve, cls, owner)  owner = node uid whose lifespan the edge shares
    con.execute(f"""
    CREATE TABLE e0 AS
    SELECT {uid('Organisation','id')} AS src, {uid('Place','LocationPlaceId')} AS dst, 'IS_LOCATED_IN' AS rel, '{{}}' AS props, 0::BIGINT AS vs, {OPEN}::BIGINT AS ve, {C_STATIC} AS cls, NULL::BIGINT AS owner FROM r_Organisation
    UNION ALL SELECT {uid('Place','id')}, {uid('Place','PartOfPlaceId')}, 'IS_PART_OF', '{{}}', 0, {OPEN}, {C_STATIC}, NULL FROM r_Place WHERE PartOfPlaceId IS NOT NULL
    UNION ALL SELECT {uid('Tag','id')}, {uid('TagClass','TypeTagClassId')}, 'HAS_TYPE', '{{}}', 0, {OPEN}, {C_STATIC}, NULL FROM r_Tag
    UNION ALL SELECT {uid('TagClass','id')}, {uid('TagClass','SubclassOfTagClassId')}, 'IS_SUBCLASS_OF', '{{}}', 0, {OPEN}, {C_STATIC}, NULL FROM r_TagClass WHERE SubclassOfTagClassId IS NOT NULL
    UNION ALL SELECT {uid('Person','id')}, {uid('Place','LocationCityId')}, 'IS_LOCATED_IN', '{{}}', {us('creationDate')}, {ve('deletionDate')}, {C_OWNED}, {uid('Person','id')} FROM r_Person
    UNION ALL SELECT {uid('Forum','id')}, {uid('Person','ModeratorPersonId')}, 'HAS_MODERATOR', '{{}}', {us('creationDate')}, {ve('deletionDate')}, {C_OWNED}, {uid('Forum','id')} FROM r_Forum WHERE ModeratorPersonId IS NOT NULL
    UNION ALL SELECT {uid('Message','id')}, {uid('Person','CreatorPersonId')}, 'HAS_CREATOR', '{{}}', {us('creationDate')}, {ve('deletionDate')}, {C_OWNED}, {uid('Message','id')} FROM r_Post
    UNION ALL SELECT {uid('Forum','ContainerForumId')}, {uid('Message','id')}, 'CONTAINER_OF', '{{}}', {us('creationDate')}, {ve('deletionDate')}, {C_OWNED}, {uid('Message','id')} FROM r_Post
    UNION ALL SELECT {uid('Message','id')}, {uid('Place','LocationCountryId')}, 'IS_LOCATED_IN', '{{}}', {us('creationDate')}, {ve('deletionDate')}, {C_OWNED}, {uid('Message','id')} FROM r_Post
    UNION ALL SELECT {uid('Message','id')}, {uid('Person','CreatorPersonId')}, 'HAS_CREATOR', '{{}}', {us('creationDate')}, {ve('deletionDate')}, {C_OWNED}, {uid('Message','id')} FROM r_Comment
    UNION ALL SELECT {uid('Message','id')}, {uid('Place','LocationCountryId')}, 'IS_LOCATED_IN', '{{}}', {us('creationDate')}, {ve('deletionDate')}, {C_OWNED}, {uid('Message','id')} FROM r_Comment
    UNION ALL SELECT {uid('Message','id')}, {uid('Message','coalesce(ParentPostId, ParentCommentId)')}, 'REPLY_OF', '{{}}', {us('creationDate')}, {ve('deletionDate')}, {C_OWNED}, {uid('Message','id')} FROM r_Comment
    UNION ALL SELECT {uid('Forum','ForumId')}, {uid('Person','PersonId')}, 'HAS_MEMBER', '{{}}', {us('creationDate')}, {ve('deletionDate')}, {C_MEMBER}, NULL FROM r_Forum_hasMember_Person
    UNION ALL SELECT {uid('Forum','ForumId')}, {uid('Tag','TagId')}, 'HAS_TAG', '{{}}', {us('creationDate')}, {ve('deletionDate')}, {C_OWNED}, {uid('Forum','ForumId')} FROM r_Forum_hasTag_Tag
    UNION ALL SELECT {uid('Person','PersonId')}, {uid('Tag','TagId')}, 'HAS_INTEREST', '{{}}', {us('creationDate')}, {ve('deletionDate')}, {C_OWNED}, {uid('Person','PersonId')} FROM r_Person_hasInterest_Tag
    UNION ALL SELECT {uid('Person','Person1Id')}, {uid('Person','Person2Id')}, 'KNOWS', '{{}}', {us('creationDate')}, {ve('deletionDate')}, {C_KNOWS}, NULL FROM r_Person_knows_Person
    UNION ALL SELECT {uid('Person','PersonId')}, {uid('Message','CommentId')}, 'LIKES', '{{}}', {us('creationDate')}, {ve('deletionDate')}, {C_LIKES}, NULL FROM r_Person_likes_Comment
    UNION ALL SELECT {uid('Person','PersonId')}, {uid('Message','PostId')}, 'LIKES', '{{}}', {us('creationDate')}, {ve('deletionDate')}, {C_LIKES}, NULL FROM r_Person_likes_Post
    UNION ALL SELECT {uid('Person','PersonId')}, {uid('Organisation','UniversityId')}, 'STUDY_AT', json_object('classYear', classYear)::VARCHAR, {us('creationDate')}, {ve('deletionDate')}, {C_OWNED}, {uid('Person','PersonId')} FROM r_Person_studyAt_University
    UNION ALL SELECT {uid('Person','PersonId')}, {uid('Organisation','CompanyId')}, 'WORK_AT', json_object('workFrom', workFrom)::VARCHAR, {us('creationDate')}, {ve('deletionDate')}, {C_OWNED}, {uid('Person','PersonId')} FROM r_Person_workAt_Company
    UNION ALL SELECT {uid('Message','PostId')}, {uid('Tag','TagId')}, 'HAS_TAG', '{{}}', {us('creationDate')}, {ve('deletionDate')}, {C_OWNED}, {uid('Message','PostId')} FROM r_Post_hasTag_Tag
    UNION ALL SELECT {uid('Message','CommentId')}, {uid('Tag','TagId')}, 'HAS_TAG', '{{}}', {us('creationDate')}, {ve('deletionDate')}, {C_OWNED}, {uid('Message','CommentId')} FROM r_Comment_hasTag_Tag
    """)
    # clamp every edge inside both endpoints' lifespans; drop empty / dangling
    con.execute("""
    CREATE TABLE e1 AS SELECT e0.*, s.vs AS svs, s.ve AS sve, d.vs AS dvs, d.ve AS dve,
        greatest(e0.vs, s.vs, d.vs) AS cvs, least(e0.ve, s.ve, d.ve) AS cve
    FROM e0 LEFT JOIN n s ON s.uid = e0.src LEFT JOIN n d ON d.uid = e0.dst""")
    q = con.execute("""SELECT count(*) FILTER (WHERE svs IS NULL OR dvs IS NULL),
        count(*) FILTER (WHERE svs IS NOT NULL AND dvs IS NOT NULL AND cvs >= cve),
        count(*) FILTER (WHERE svs IS NOT NULL AND dvs IS NOT NULL AND cvs < cve AND (cvs <> vs OR cve <> ve)),
        count(*) FILTER (WHERE rel='HAS_MODERATOR' AND cve <> ve) FROM e1""").fetchone()
    con.execute("""CREATE TABLE e AS SELECT src, dst, rel, props, cvs AS vs, cve AS ve, cls, owner,
        row_number() OVER (PARTITION BY src, dst, rel ORDER BY cvs) - 1 AS k
        FROM e1 WHERE svs IS NOT NULL AND dvs IS NOT NULL AND cvs < cve""")
    dups = con.execute("SELECT count(*) FROM e WHERE k > 0").fetchone()[0]
    return {"zero_life_nodes_dropped": zero_nodes, "edges_dangling_dropped": q[0],
            "edges_empty_after_clamp_dropped": q[1], "edges_clamped": q[2],
            "has_moderator_clamped_to_moderator_life": q[3], "edge_identity_duplicates": dups}


def threshold_ms(portion: float) -> int:
    return SIM_END_MS - int((SIM_END_MS - SIM_START_MS) * (1 - portion))


def stream_size(con, thr_us: int) -> int:
    return con.execute(f"""SELECT (SELECT count(*) FROM n WHERE vs >= {thr_us}) + (SELECT count(*) FROM e WHERE vs >= {thr_us})
        + (SELECT count(*) FROM n WHERE ve >= {thr_us} AND ve < {OPEN}) + (SELECT count(*) FROM e WHERE ve >= {thr_us} AND ve < {OPEN})""").fetchone()[0]


class LogWriter:
    def __init__(self, path: Path):
        self.f = open(path, "w", encoding="utf-8")
        self.n = 0
        self.ops = 0

    def record(self, tt: int, ops: list[dict]) -> None:
        bid = sha256_hex(canonical_json({"tt": tt, "ops": ops}))[:16]
        self.f.write(canonical_json({"batch_id": bid, "tt": tt, "ops": ops}) + "\n")
        self.n += 1
        self.ops += sum(len(o.get("events", ())) + len(o.get("nodes", ())) if o["op"] == "ingest_events" else 1
                        for o in ops)

    def close(self) -> None:
        self.f.close()


def _props(s: str) -> dict:
    return {k: v for k, v in json.loads(s).items() if v is not None}


def ingest_node_op(rows) -> dict:
    return {"op": "ingest_events", "events": [], "node_label": "Node", "source": "ingest", "provenance_ref": None,
            "nodes": [{"uid": str(u), "label": l, "props": _props(p), "vt_s": int(a), "vt_e": int(b)}
                      for u, l, p, a, b in rows]}


def ingest_edge_op(rows, offset: int) -> dict:
    return {"op": "ingest_events", "offset": offset, "node_label": "Node", "source": "ingest", "provenance_ref": None,
            "events": [{"src": str(s), "dst": str(d), "rel_type": r, "disc": disc(k), "props": json.loads(p),
                        "vt_s": int(a), "vt_e": int(b)} for s, d, r, p, a, b, k in rows]}


def disc(k) -> str:
    return "" if not k else f"#{int(k)}"


def retract_op(ref: dict, t: int) -> dict:
    return {"op": "retract", "ref": ref, "t": int(t), "source": "ingest", "provenance_ref": None}


def node_ref(u) -> dict:
    return {"kind": "node", "uid": str(u)}


def edge_ref(s, d, r, k) -> dict:
    return {"kind": "edge", "src": str(s), "dst": str(d), "rel_type": r, "disc": disc(k)}


def write_chunks(w: LogWriter, tt: int, cur, mk, *extra) -> int:
    while True:
        rows = cur.fetchmany(CHUNK)
        if not rows:
            return tt
        w.record(tt, [mk(rows, *extra)] if extra else [mk(rows)])
        tt += 1


def cmd_base(a) -> int:
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    t0 = time.time()
    con = duckdb.connect(str(out / "base.duckdb"))
    con.execute(f"SET threads={os.cpu_count()}; SET preserve_insertion_order=false")
    meta: dict = {"raw": str(a.raw), "tool": "w3_snb_log.py base", "open_end": OPEN, "now_us": NOW_US}
    meta["mapping"] = build_elements(con, Path(a.raw))
    thr97 = threshold_ms(0.97) * 1000
    s97 = stream_size(con, thr97)
    meta["stream_elements_at_0.97"] = s97
    if a.history_mult == 1:
        thr, portion = thr97, 0.97
    else:   # bisection on the bulk-load portion
        lo, hi = 0.0, 0.97
        for _ in range(40):
            mid = (lo + hi) / 2
            if stream_size(con, threshold_ms(mid) * 1000) > a.history_mult * s97:
                lo = mid
            else:
                hi = mid
        portion = hi
        thr = threshold_ms(portion) * 1000
    meta.update(history_mult=a.history_mult, bulkload_portion=portion, threshold_us=thr,
                stream_elements=stream_size(con, thr))
    meta["n_nodes"] = con.execute("SELECT count(*) FROM n").fetchone()[0]
    meta["n_edges"] = con.execute("SELECT count(*) FROM e").fetchone()[0]
    meta["n_base_elements"] = meta["n_nodes"] + meta["n_edges"]
    # element index: nodes then edges, deterministic order
    con.execute("CREATE TABLE ni AS SELECT row_number() OVER (ORDER BY uid) - 1 AS idx, * FROM n")
    nn = meta["n_nodes"]
    con.execute(f"""CREATE TABLE ei AS SELECT {nn} + row_number() OVER (ORDER BY src, dst, rel, k) - 1 AS idx, e.*,
        s.idx AS sidx, d.idx AS didx, o.idx AS oidx
        FROM e JOIN ni s ON s.uid = e.src JOIN ni d ON d.uid = e.dst LEFT JOIN ni o ON o.uid = e.owner""")
    # day of each event: insert day (vs >= thr), delete day (thr <= ve < OPEN); day end = next UTC midnight
    day = lambda c: f"(({c} // {DAY_US}) + 1) * {DAY_US}"
    # ---- snapshot records ----
    w = LogWriter(out / "snapshot.part")
    tt = thr
    tt = write_chunks(w, tt, con.execute(f"SELECT uid, label, props, vs, CASE WHEN ve < {thr} THEN ve ELSE {OPEN} END FROM ni WHERE vs < {thr} ORDER BY idx"), ingest_node_op)
    off = 0
    cur = con.execute(f"SELECT src, dst, rel, props, vs, CASE WHEN ve < {thr} THEN ve ELSE {OPEN} END, k FROM ei WHERE vs < {thr} ORDER BY idx")
    while True:
        rows = cur.fetchmany(CHUNK)
        if not rows:
            break
        w.record(tt, [ingest_edge_op(rows, off)]); tt += 1; off += len(rows)
    w.close()
    meta["snapshot"] = {"records": w.n, "ops_items": w.ops, "tt_first": thr, "tt_last": tt - 1}
    # ---- daily records ----
    days = [r[0] for r in con.execute(f"""SELECT DISTINCT d FROM (
        SELECT {day('vs')} d FROM ni WHERE vs >= {thr} UNION SELECT {day('vs')} FROM ei WHERE vs >= {thr}
        UNION SELECT {day('ve')} FROM ni WHERE ve >= {thr} AND ve < {OPEN} UNION SELECT {day('ve')} FROM ei WHERE ve >= {thr} AND ve < {OPEN}) ORDER BY d""").fetchall()]
    (out / "days").mkdir(exist_ok=True)
    per_day = []
    for dend in days:
        dstart = max(dend - DAY_US, thr)
        # one transaction per simulation day: inserts (nodes, then edges), then
        # deletes (edges, then nodes) as retract at deletionDate, all in one record
        w = LogWriter(out / "days" / f"{dend}.part")
        ops = []
        cur = con.execute(f"SELECT uid, label, props, vs, {OPEN} FROM ni WHERE vs >= {dstart} AND vs < {dend} ORDER BY vs, idx")
        while rows := cur.fetchmany(CHUNK):
            ops.append(ingest_node_op(rows))
        cur = con.execute(f"SELECT src, dst, rel, props, vs, {OPEN}, k FROM ei WHERE vs >= {dstart} AND vs < {dend} ORDER BY vs, idx")
        while rows := cur.fetchmany(CHUNK):
            ops.append(ingest_edge_op(rows, off)); off += len(rows)
        dl = [retract_op(edge_ref(s, d, r, k), t) for s, d, r, k, t in con.execute(
            f"SELECT src, dst, rel, k, ve FROM ei WHERE ve >= {dstart} AND ve < {dend} AND ve < {OPEN} ORDER BY ve, idx").fetchall()]
        dl += [retract_op(node_ref(u), t) for u, t in con.execute(
            f"SELECT uid, ve FROM ni WHERE ve >= {dstart} AND ve < {dend} AND ve < {OPEN} ORDER BY ve, idx").fetchall()]
        ops += dl
        w.record(dend, ops)
        w.close()
        per_day.append({"day_end_us": dend, "records": w.n, "items": w.ops, "deletes": len(dl)})
    meta["days"] = per_day
    # ---- injector inputs ----
    con.execute(f"COPY (SELECT idx, uid, label, props, vs, ve, cls FROM ni ORDER BY idx) TO '{out}/nodes.parquet' (FORMAT parquet)")
    con.execute(f"COPY (SELECT idx, src, dst, rel, k, props, vs, ve, cls, sidx, didx, oidx FROM ei ORDER BY idx) TO '{out}/edges.parquet' (FORMAT parquet)")
    # prerequisites: (pre, dep): dep.vs >= pre.vs + 10 s
    con.execute(f"""COPY (
        SELECT e.didx AS pre, e.sidx AS dep FROM ei e WHERE e.rel = 'HAS_CREATOR'                       -- message after creator
        UNION ALL SELECT e.sidx, e.didx FROM ei e WHERE e.rel = 'CONTAINER_OF'                          -- post after forum
        UNION ALL SELECT e.didx, e.sidx FROM ei e WHERE e.rel = 'REPLY_OF'                              -- comment after parent
        UNION ALL SELECT e.didx, e.sidx FROM ei e WHERE e.rel = 'HAS_MODERATOR'                         -- forum after moderator
        UNION ALL SELECT m.idx, c.didx FROM ei c JOIN ei h ON h.sidx = c.didx AND h.rel = 'HAS_CREATOR'
             JOIN ei m ON m.rel = 'HAS_MEMBER' AND m.sidx = c.sidx AND m.didx = h.didx WHERE c.rel = 'CONTAINER_OF'  -- membership before posting
        UNION ALL SELECT sidx, idx FROM ei WHERE cls IN ({C_KNOWS},{C_LIKES},{C_MEMBER})               -- edge after endpoints
        UNION ALL SELECT didx, idx FROM ei WHERE cls IN ({C_KNOWS},{C_LIKES},{C_MEMBER})
        ) TO '{out}/prereq.parquet' (FORMAT parquet)""")
    meta["wall_s"] = round(time.time() - t0, 1)
    (out / "meta.json").write_text(json.dumps(meta, indent=1) + "\n")
    print(json.dumps({k: v for k, v in meta.items() if k != "days"}, indent=1))
    return 0


# --------------------------------------------------------------------------- #
# inject                                                                        #
# --------------------------------------------------------------------------- #

LAGS = {"same": (0, 0), "months": (30 * DAY_US, 90 * DAY_US), "years": (365 * DAY_US, 1095 * DAY_US)}
LAG_MIX = {"mix": {"same": 0.50, "months": 0.35, "years": 0.15},
           "same": {"same": 1.0}, "months": {"months": 1.0}, "years": {"years": 1.0}}
KIND_MIX = {"CS": 0.35, "RD": 0.30, "DS": 0.15, "UD": 0.10, "PA": 0.10}
FALLBACK = {"UD": "RD", "DS": "RD", "RD": "CS", "CS": "RD", "PA": "CS"}
NAME_FIELDS = ("firstName", "lastName", "browserUsed", "locationIP")


def csr(keys: np.ndarray, vals: np.ndarray, n: int):
    o = np.argsort(keys, kind="stable")
    ptr = np.zeros(n + 1, dtype=np.int64)
    np.add.at(ptr, keys + 1, 1)
    return np.cumsum(ptr), vals[o]


class _Codes:
    """A dictionary-encoded string column, indexed like a list."""

    def __init__(self, arr):
        self.codes = arr.indices.to_numpy()
        self.words = arr.dictionary.to_pylist()

    def __getitem__(self, i):
        return self.words[self.codes[i]]


def protected_uid(uid, frac: float) -> bool:
    """The protected (correction-free) anchors: Person and Message nodes whose MD5-derived hash is below `frac`.
    `w3_snb_params.py` uses the same rule for the free stratum."""
    return int(hashlib.md5(str(uid).encode()).hexdigest()[:8], 16) / 2 ** 32 < frac


class Injector:
    def __init__(self, base: Path, seed: int, protect: float = 0.0):
        import pyarrow.parquet as pq
        self.rng = np.random.default_rng(seed)
        nt = pq.read_table(base / "nodes.parquet")
        et = pq.read_table(base / "edges.parquet")
        self.nn = nt.num_rows
        nn = self.nn
        self.N = self.nn + et.num_rows
        col = lambda t, c, dt: t.column(c).to_numpy().astype(dt)
        self.vs = np.concatenate([col(nt, "vs", np.int64), col(et, "vs", np.int64)])
        self.ve = np.concatenate([col(nt, "ve", np.int64), col(et, "ve", np.int64)])
        self.cls = np.concatenate([col(nt, "cls", np.int8), col(et, "cls", np.int8)])
        import pyarrow as pa
        import pyarrow.compute as pc
        big = lambda c: c.cast(pa.large_string()).combine_chunks()     # SF10: > 2 GiB of string data per column
        enc = lambda c: pc.dictionary_encode(big(c))
        self.uid = nt.column("uid").to_numpy()
        lab = enc(nt.column("label")); self.label = _Codes(lab)
        self.nprops = big(nt.column("props"))
        self.src = et.column("src").to_numpy(); self.dst = et.column("dst").to_numpy()
        rel = enc(et.column("rel")); self.rel = _Codes(rel); self.k = et.column("k").to_numpy()
        self.eprops = big(et.column("props"))
        self.sidx = et.column("sidx").to_numpy(); self.didx = et.column("didx").to_numpy()
        oidx = et.column("oidx").to_numpy(zero_copy_only=False)
        oidx = np.where(np.isnan(oidx.astype(float)), -1, oidx).astype(np.int64)
        has = np.nonzero(oidx >= 0)[0]
        self.own_ptr, self.own = csr(oidx[has], has + nn, nn)
        pre = pq.read_table(base / "prereq.parquet")
        p, d = pre.column("pre").to_numpy().astype(np.int64), pre.column("dep").to_numpy().astype(np.int64)
        self.pre_ptr, self.pre = csr(d, p, self.N)      # prerequisites of x
        self.dep_ptr, self.dep = csr(p, d, self.N)      # dependents of x
        self.used = np.zeros(self.N, dtype=bool)
        # protected anchors: no correction touches them, their incident edges, or the entities that own an edge to them
        self.prot = np.zeros(self.N, dtype=bool)
        if protect > 0:
            cls_n = self.cls[: self.nn]
            for i in np.nonzero(np.isin(cls_n, [C_PERSON, C_POST, C_COMMENT]))[0]:
                if protected_uid(self.uid[i], protect):
                    self.prot[i] = True
        # subset safety: no creation shift touches a prerequisite or dependent of
        # another creation shift, and no person is both shifted and amended, so
        # every subset of the generated corrections is itself a valid history
        self.cs_lock = np.zeros(self.N, dtype=bool)
        self.person_props: dict[int, dict] = {}
        pools = {}
        isnode = np.arange(self.N) < nn
        dyn_node = isnode & np.isin(self.cls, [C_PERSON, C_FORUM, C_POST, C_COMMENT])
        pools["CS"] = np.nonzero(dyn_node)[0]
        pools["PA"] = np.nonzero(self.cls == C_PERSON)[0]
        pools["RD"] = np.nonzero(~isnode & np.isin(self.cls, [C_KNOWS, C_LIKES]))[0]
        pools["DS"] = np.nonzero(~isnode & np.isin(self.cls, [C_KNOWS, C_LIKES, C_MEMBER]) & (self.ve < OPEN))[0]
        pools["UD"] = pools["DS"].copy()
        self.pool = {}
        for kname, idx in pools.items():
            anchor = self.vs[idx] if kname in ("CS", "PA", "RD") else self.ve[idx]
            o = np.argsort(anchor, kind="stable")
            self.pool[kname] = (anchor[o], idx[o])
        names = {f: set() for f in NAME_FIELDS}
        for i in pools["PA"][: 50_000]:
            pr = json.loads(self.nprops[int(i)].as_py())
            for f in NAME_FIELDS:
                names[f].add(pr[f])
        self.names = {f: sorted(v) for f, v in names.items()}

    # ------------------------------------------------------------------ refs
    def ref(self, i: int) -> dict:
        if i < self.nn:
            return node_ref(self.uid[i])
        j = i - self.nn
        return edge_ref(self.src[j], self.dst[j], self.rel[j], self.k[j])

    def assert_op(self, i: int, vs: int, ve: int, props: dict | None = None) -> dict:
        if i < self.nn:
            return {"op": "assert_node", "uid": str(self.uid[i]), "label": self.label[i],
                    "props": props if props is not None else _props(self.nprops[i].as_py()),
                    "vt_s": int(vs), "vt_e": int(ve), "source": "ingest", "provenance_ref": None}
        j = i - self.nn
        return {"op": "assert_edge", "src": str(self.src[j]), "dst": str(self.dst[j]), "rel_type": self.rel[j],
                "props": json.loads(self.eprops[j].as_py()), "vt_s": int(vs), "vt_e": int(ve), "disc": disc(self.k[j]),
                "source": "ingest", "provenance_ref": None}

    def pprops(self, i: int) -> dict:
        if i not in self.person_props:
            self.person_props[i] = _props(self.nprops[i].as_py())
        return self.person_props[i]

    # ------------------------------------------------------------- candidates
    def nearest(self, kname: str, a: int, tt: int, ok) -> int | None:
        A, I = self.pool[kname]
        pos = int(np.searchsorted(A, a))
        for step in range(400):
            for p in ((pos + step // 2) if step % 2 == 0 else (pos - 1 - step // 2),):
                if 0 <= p < len(A):
                    i = int(I[p])
                    if (kname == "PA" or not self.used[i]) and ok(i):
                        return i
        return None

    def bve(self, i: int, tt: int) -> int:
        """The valid-time end believed at the correction's commit: a base deletion
        is known only from its own day's commit on (the day ends before tt)."""
        e = int(self.ve[i])
        return e if e < tt - SEC_US else OPEN

    def try_kind(self, kname: str, a: int, tt: int):
        vs, ve = self.vs, self.ve
        if kname == "CS":
            def ok(i):
                if vs[i] > tt - GAP_US or self.cs_lock[i] or self.prot[i]:
                    return False
                ow0 = self.own[self.own_ptr[i]:self.own_ptr[i + 1]] - self.nn
                if len(ow0) and (self.prot[self.sidx[ow0]].any() or self.prot[self.didx[ow0]].any()):
                    return False
                lo = SIM_START_MS * 1000
                pr = self.pre[self.pre_ptr[i]:self.pre_ptr[i + 1]]
                if len(pr):
                    lo = max(lo, int(vs[pr].max()) + GAP_US)
                hi = min(int(ve[i]) - GAP_US, tt - GAP_US)
                ow = self.own[self.own_ptr[i]:self.own_ptr[i + 1]]
                if len(ow):
                    hi = min(hi, int(ve[ow].min()) - GAP_US)
                dp = self.dep[self.dep_ptr[i]:self.dep_ptr[i + 1]]
                if len(dp):
                    hi = min(hi, int(vs[dp].min()) - GAP_US)
                self._win = (lo, hi)
                return lo < hi and (lo < vs[i] - GAP_US or hi > vs[i] + GAP_US)
            i = self.nearest("CS", a, tt, ok)
            if i is None:
                return None
            lo, hi = self._win
            old = int(vs[i])
            for _ in range(8):
                d = int(np.exp(self.rng.uniform(np.log(GAP_US), np.log(30 * DAY_US))))
                new = old + d if self.rng.random() < 0.5 else old - d
                new = min(max(new, lo), hi)
                if abs(new - old) >= GAP_US:
                    break
            else:
                return None
            own = self.own[self.own_ptr[i]:self.own_ptr[i + 1]]
            ops = [retract_op(self.ref(int(j)), int(vs[j])) for j in own]
            ops.append(retract_op(self.ref(i), old))
            ops.append(self.assert_op(i, new, self.bve(i, tt), self.pprops(i) if self.cls[i] == C_PERSON else None))
            for j in own:
                ops.append(self.assert_op(int(j), new, self.bve(int(j), tt)))
                vs[j] = new; self.used[j] = True
            vs[i] = new; self.used[i] = True
            self.cs_lock[self.pre[self.pre_ptr[i]:self.pre_ptr[i + 1]]] = True
            self.cs_lock[self.dep[self.dep_ptr[i]:self.dep_ptr[i + 1]]] = True
            self.cs_lock[i] = True
            return ops, i, old, new, old, "vs"
        if kname == "PA":
            i = self.nearest("PA", a, tt, lambda i: vs[i] <= tt - GAP_US and not self.used[i] and not self.prot[i])
            if i is None:
                return None
            pr = dict(self.pprops(i))      # base props: an amendment is never cumulative (subset safety)
            f = NAME_FIELDS[int(self.rng.integers(len(NAME_FIELDS)))]
            if f == "locationIP":
                v = ".".join(str(int(x)) for x in self.rng.integers(1, 255, 4))
            else:
                v = self.names[f][int(self.rng.integers(len(self.names[f])))]
            if v == pr.get(f):
                return None
            pr[f] = v
            self.cs_lock[i] = True
            op = {"op": "correct", "ref": self.ref(i), "props": pr, "vt_s": int(vs[i]), "vt_e": self.bve(i, tt),
                  "source": "ingest", "provenance_ref": None}
            return [op], i, int(vs[i]), int(vs[i]), int(vs[i]), f
        if kname == "RD":
            A, I = self.pool["RD"]
            hi = int(np.searchsorted(A, a - GAP_US))
            if hi == 0:
                return None
            for _ in range(64):
                i = int(I[int(self.rng.integers(hi))])
                j = i - self.nn
                if self.prot[self.sidx[j]] or self.prot[self.didx[j]]:
                    continue
                if not self.used[i] and ve[i] == OPEN and vs[i] + GAP_US <= a:
                    self.used[i] = True
                    ve[i] = a
                    return [retract_op(self.ref(i), a)], i, OPEN, a, a, "ve"
            return None
        if kname in ("DS", "UD"):
            j_of = lambda i: i - self.nn
            def ok(i):
                if ve[i] >= OPEN or ve[i] > tt - GAP_US:
                    return False
                s, d = self.sidx[j_of(i)], self.didx[j_of(i)]
                if self.prot[s] or self.prot[d]:
                    return False
                if kname == "UD":
                    return ve[s] == OPEN and ve[d] == OPEN
                return True
            i = self.nearest(kname, a, tt, ok)
            if i is None:
                return None
            old = int(ve[i])
            s, d = self.sidx[i - self.nn], self.didx[i - self.nn]
            if kname == "UD":
                new = OPEN
                ops = [self.assert_op(i, int(vs[i]), OPEN)]
            else:
                lo = int(vs[i]) + GAP_US
                hi = min(int(ve[s]), int(ve[d]), tt - GAP_US)
                dd = int(np.exp(self.rng.uniform(np.log(GAP_US), np.log(30 * DAY_US))))
                new = old + dd if self.rng.random() < 0.5 else old - dd
                new = min(max(new, lo), hi)
                if abs(new - old) < GAP_US:
                    return None
                ops = [retract_op(self.ref(i), new)] if new < old else [self.assert_op(i, int(vs[i]), new)]
            ve[i] = new; self.used[i] = True
            return ops, i, old, new, old, "ve"
        raise ValueError(kname)


def cmd_inject(a) -> int:
    """Generate the corrections once at `--rho-gen`, then write one log per
    requested density as the subset of corrections with u < rho / rho_gen
    (u uniform per correction): the densities are nested, so a parameter's
    correction-free stratum is free at every density."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    base = Path(a.base)
    meta = json.loads((base / "meta.json").read_text())
    rhos = [float(x) for x in a.rhos.split(",")]
    rho_gen = a.rho_gen or max(rhos)
    t0 = time.time()
    inj = Injector(base, a.seed, a.protect)
    days = meta["days"]
    C = int(round(rho_gen * meta["n_base_elements"]))
    per = np.full(len(days), C // len(days), dtype=np.int64)
    per[: C % len(days)] += 1
    lagmix = LAG_MIX[a.lag]
    lag_names, lag_p = list(lagmix), np.array(list(lagmix.values()))
    kinds, kind_p = list(KIND_MIX), np.array(list(KIND_MIX.values()))
    frags: list[list[str]] = [[] for _ in days]          # per day: per correction, its ops as one JSON fragment
    truth = {k: [] for k in ("day", "tt", "kind", "kind_asked", "idx", "old", "new", "anchor", "lag_us", "lag_bucket", "field", "u")}
    n_skip = 0
    for di, day in enumerate(days):
        dend = day["day_end_us"]
        dstart = max(dend - DAY_US, meta["threshold_us"])
        tt = dend + SEC_US
        for c in range(int(per[di])):
            lb = lag_names[int(inj.rng.choice(len(lag_names), p=lag_p))]
            if lb == "same":
                anchor = int(inj.rng.integers(dstart, dend))
            else:
                lo, hi = LAGS[lb]
                anchor = tt - int(inj.rng.integers(lo, hi))
            asked = kinds[int(inj.rng.choice(len(kinds), p=kind_p))]
            kname, res = asked, None
            for _ in range(3):
                res = inj.try_kind(kname, anchor, tt)
                if res is not None:
                    break
                kname = FALLBACK[kname]
            u = float(inj.rng.random())
            if res is None:
                n_skip += 1
                continue
            cops, i, old, new, anc, field = res
            frags[di].append(",".join(canonical_json(o) for o in cops))
            for kk, v in (("day", di), ("tt", tt), ("kind", kname), ("kind_asked", asked), ("idx", i), ("old", old),
                          ("new", new), ("anchor", anc), ("lag_us", tt - anc), ("lag_bucket", lb), ("field", field),
                          ("u", u)):
                truth[kk].append(v)
    gen_s = round(time.time() - t0, 1)
    idx_all = np.array(truth["idx"], dtype=np.int64)
    keys = [str(inj.uid[i]) if i < inj.nn else f"{inj.src[i-inj.nn]}|{inj.dst[i-inj.nn]}|{inj.rel[i-inj.nn]}|{disc(inj.k[i-inj.nn])}"
            for i in idx_all]
    u_all = np.array(truth["u"])
    day_all = np.array(truth["day"], dtype=np.int64)
    for rho in rhos:
        out = Path(a.out_pattern.format(r=fmt_rho(rho)))
        t1 = time.time()
        keep = u_all < (rho / rho_gen) if rho_gen > 0 else np.zeros(len(u_all), bool)
        tmp = out.with_suffix(".tmp")
        n_rec = 0
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(HEADER)
            copy_into(f, base / "snapshot.part")
            pos = 0
            for di, day in enumerate(days):
                copy_into(f, base / "days" / f"{day['day_end_us']}.part")
                n = len(frags[di])
                sel = [frags[di][j] for j in range(n) if keep[pos + j]]
                pos += n
                if sel:
                    tt = day["day_end_us"] + SEC_US
                    line = frag_line(tt, sel)
                    if n_rec == 0:      # the fragment assembly is byte-identical to canonical_json
                        assert line == rec_line(tt, json.loads("[" + ",".join(sel) + "]")), "fragment encoding drift"
                    f.write(line); n_rec += 1
        os.replace(tmp, out)
        m = summarize(truth, keep, keys, out, base, meta, a, rho, rho_gen, C, n_skip, n_rec, gen_s, t1)
        print(json.dumps({k: m[k] for k in ("log", "sha256", "rho", "corrections", "by_kind", "by_lag_bucket", "wall_s")}))
    return 0


def fmt_rho(rho: float) -> str:
    return ("%g" % (rho * 100)).replace(".", "p")


def copy_into(f, part: Path) -> None:
    with open(part, "r", encoding="utf-8") as g:
        while b := g.read(1 << 24):
            f.write(b)


def frag_line(tt: int, frags: list[str]) -> str:
    ops = "[" + ",".join(frags) + "]"
    bid = sha256_hex('{"ops":' + ops + ',"tt":' + str(tt) + '}')[:16]
    return '{"batch_id":"' + bid + '","ops":' + ops + ',"tt":' + str(tt) + '}\n'


def summarize(truth, keep, keys, out, base, meta, a, rho, rho_gen, C, n_skip, n_rec, gen_s, t1) -> dict:
    import pyarrow as pa
    import pyarrow.parquet as pq
    sel = np.nonzero(keep)[0]
    cols = {k: [v[i] for i in sel] for k, v in truth.items()}
    cols["key"] = [keys[i] for i in sel]
    pq.write_table(pa.table(cols), str(out) + ".corrections.parquet")
    h = hashlib.sha256()
    with open(out, "rb") as g:
        for b in iter(lambda: g.read(1 << 24), b""):
            h.update(b)
    lag = np.array(cols["lag_us"], dtype=np.int64)
    kinds_arr, lb_arr = np.array(cols["kind"]), np.array(cols["lag_bucket"])
    q = lambda x, qs: {str(k): round(float(np.quantile(x, k)) / DAY_US, 4) for k in qs} if len(x) else {}
    m = {"log": str(out), "sha256": h.hexdigest(), "bytes": out.stat().st_size, "base": str(base),
         "base_meta": {k: meta[k] for k in ("history_mult", "bulkload_portion", "threshold_us", "n_base_elements",
                                            "stream_elements")},
         "rho": rho, "rho_gen": rho_gen, "lag": a.lag, "seed": a.seed, "corrections_target_at_rho_gen": C,
         "corrections": int(len(sel)), "skipped_infeasible_at_rho_gen": n_skip, "correction_records": n_rec,
         "by_kind": {k: int((kinds_arr == k).sum()) for k in KIND_MIX},
         "by_kind_asked": {k: int((np.array(cols["kind_asked"]) == k).sum()) for k in KIND_MIX},
         "by_lag_bucket": {k: int((lb_arr == k).sum()) for k in LAGS},
         "lag_days_quantiles": q(lag, (0, .1, .25, .5, .75, .9, 1)),
         "lag_days_quantiles_by_bucket": {b: q(lag[lb_arr == b], (0, .5, 1)) for b in LAGS if (lb_arr == b).any()},
         "touched_elements": int(len(set(cols["key"]))), "generate_s": gen_s, "wall_s": round(time.time() - t1, 1),
         "kind_mix": KIND_MIX, "lag_mix": LAG_MIX[a.lag], "protect_frac": a.protect}
    Path(str(out) + ".meta.json").write_text(json.dumps(m, indent=1) + "\n")
    return m


def rec_line(tt: int, ops: list) -> str:
    bid = sha256_hex(canonical_json({"tt": tt, "ops": ops}))[:16]
    return canonical_json({"batch_id": bid, "tt": tt, "ops": ops}) + "\n"


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sp = p.add_subparsers(dest="cmd", required=True)
    b = sp.add_parser("base"); b.add_argument("--raw", required=True); b.add_argument("--out", required=True)
    b.add_argument("--history-mult", type=float, default=1)
    i = sp.add_parser("inject"); i.add_argument("--base", required=True)
    i.add_argument("--out-pattern", required=True, help="log path with {r} for the density in percent, e.g. .../W3-SF1-h1-r{r}.eventlog.jsonl")
    i.add_argument("--rhos", required=True, help="comma list, e.g. 0,0.0002,0.003,0.01,0.1")
    i.add_argument("--rho-gen", type=float, default=0.0); i.add_argument("--lag", default="mix", choices=list(LAG_MIX))
    i.add_argument("--seed", type=int, default=0)
    i.add_argument("--protect", type=float, default=0.3,
                   help="fraction of Person/Message anchors no correction touches (the free stratum)")
    a = p.parse_args()
    return {"base": cmd_base, "inject": cmd_inject}[a.cmd](a)


if __name__ == "__main__":
    sys.exit(main())
