"""W3 query runner and answer check (LDBC SNB-derived bi-temporal workload).

**This is not an LDBC Benchmark, and nothing produced here is an LDBC
Benchmark Result.**

    w3_snb_run.py duckdb-load --hist HIST --db DB
    w3_snb_run.py run --system duckdb|postgres|gnomon --target DB|DSN|STORE --params P.jsonl
                      [--queries IS1,BI1] [--settings current,asof,window] --out R.jsonl
    w3_snb_run.py compare --base R_gnomon.jsonl R_other.jsonl ... [--json out.json]

Belief settings (D6: reads at the valid instant "now" = 2013-01-01T00:00Z):
  current  T = now (OPEN_END - 1), V = now
  asof     T = the instance's transaction time (a day's commit), V = now
  window   T = the instance's transaction time, VT window [VA, VB), sequenced (co_valid on valid time)

SQL systems run LDBC's reference texts unchanged against the belief views of
`w3_snb_sql.py` (schema bi_<mode> for BI = ldbc_snb_bi/umbra, iv2_<mode> for
Interactive v2 = ldbc_snb_interactive_v2_impls/postgres). Parameters are
substituted exactly as LDBC's `cast_parameter_to_driver_input` does.
Gnomon runs the operator calls of `w3_snb_gnomon.py`.

An answer is canonicalised (timestamps -> epoch microseconds, dates -> ISO,
floats rounded to 6 places and integral floats -> int, ids from uids) and
hashed in LDBC's order. A pair that differs only in row order is reported as
`same-rows-other-order` (a tie the specification leaves open).
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import re
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

OPEN = 2 ** 62
NOW_US = 1356998400000000
EXT = Path(os.environ.get("W3_EXT", __import__("os").environ.get("GNOMON_BENCH_ROOT", ".") + "/w3-snb/ext"))
IV2Q = EXT / "ldbc_snb_interactive_v2_impls/postgres/queries"
BIQ = EXT / "ldbc_snb_bi/umbra/queries"
W3Q = ROOT / "benchmarks/workloads/snb/queries"     # our TPC-BiH-style texts

#: qid -> (family, reference SQL file). The T-LDBC rows are the IS texts at "asof".
CATALOG: dict[str, tuple[str, Path]] = {}
for i in range(1, 8):
    CATALOG[f"IS{i}"] = ("iv2", IV2Q / f"interactive-short-{i}.sql")
for i in range(1, 15):
    CATALOG[f"IC{i}"] = ("iv2", IV2Q / f"interactive-complex-{i}.sql")
for i in range(1, 21):
    CATALOG[f"BI{i}"] = ("bi", BIQ / f"bi-{i}.sql")
for i in range(1, 7):
    CATALOG[f"TLDBC-IS{i}"] = ("iv2", IV2Q / f"interactive-short-{i}.sql")
for q in ("K", "VER", "R7", "R3", "R5"):
    CATALOG[f"BIH-{q}"] = ("bi", W3Q / f"bih-{q}.sql")

SETTINGS = ("current", "asof", "window")
COMPARE_LAST = {"IC14"}
#: answer-check cap per call (a call past it is recorded as an error cell, "timeout")
CAP_S = int(os.environ.get("W3_CAP_S", "900"))
STARTUP_S = int(os.environ.get("W3_STARTUP_S", "3600"))   # a capped worker's store open (outside the cap)
PRIME_S = int(os.environ.get("W3_PRIME_S", "600"))         # a fresh worker's priming call (outside the cap)


# --------------------------------------------------------------------------- #
# parameters                                                                    #
# --------------------------------------------------------------------------- #

def iso_dt(us: int) -> str:
    return dt.datetime.fromtimestamp(us / 1e6, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "+00:00"


def cast_param(value, ptype: str) -> str:
    """LDBC's `cast_parameter_to_driver_input` (ldbc_snb_bi/umbra/queries.py), our value encoding:
    DATETIME and DATE values are epoch microseconds."""
    if ptype in ("INT", "INT32"):
        return str(int(value))
    if ptype in ("ID", "INT64"):
        return f"{int(value)}::bigint"
    if ptype == "STRING[]":
        return "(" + ", ".join("'" + e.replace("'", "''") + "'" for e in value) + ")"
    if ptype == "STRING":
        return "'" + str(value).replace("'", "''") + "'"
    if ptype == "DATETIME":
        return "'" + dt.datetime.fromtimestamp(value / 1e6, dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f") + "'::timestamp"
    if ptype == "DATE":
        return "'" + dt.datetime.fromtimestamp(value / 1e6, dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S") + "'::date"
    raise ValueError(ptype)


def bind_sql(text: str, params: dict) -> str:
    for k in sorted(params, key=len, reverse=True):
        ptype, v = params[k]
        text = re.sub(rf":{k}\b", cast_param(v, ptype), text)
    return text


def query_params(qid: str, inst: dict, setting: str) -> dict:
    """The instance's LDBC parameters plus the belief-setting values a text reads
    (window texts read :VA/:VB/:T; the TPC-BiH-style texts read :T, :T1, :V, :A, :B)."""
    p = dict(inst["params"])
    sv = setting_values(inst, setting)
    if setting == "window":
        p.update(VA=("INT64", sv["VA"]), VB=("INT64", sv["VB"]), T=("INT64", sv["T"]))
    if qid.startswith("BIH-"):
        A, B = (sv["VA"], sv["VB"]) if setting == "window" else (inst.get("A", 0), inst.get("B", OPEN))
        T1 = inst["T"] if setting == "current" else inst.get("T1", 0)
        p.update(T=("INT64", sv["T"]), V=("INT64", NOW_US), A=("INT64", A), B=("INT64", B), T1=("INT64", T1))
    return p


def setting_values(inst: dict, setting: str) -> dict:
    if setting == "current":
        return {"T": OPEN - 1, "V": NOW_US, "VA": NOW_US, "VB": NOW_US + 1}
    if setting == "asof":
        return {"T": inst["T"], "V": NOW_US, "VA": NOW_US, "VB": NOW_US + 1}
    return {"T": inst["T"], "V": NOW_US, "VA": inst["VA"], "VB": inst["VB"]}


# --------------------------------------------------------------------------- #
# canonical answers                                                             #
# --------------------------------------------------------------------------- #

def canon_value(v, col: str = ""):
    import decimal
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, dt.datetime):
        if v.tzinfo is None:
            v = v.replace(tzinfo=dt.timezone.utc)
        return int(round(v.timestamp() * 1e6))
    if isinstance(v, dt.date):
        return v.isoformat()
    if isinstance(v, decimal.Decimal):
        v = float(v)
    if isinstance(v, float):
        r = round(v, 6)
        return int(r) if r.is_integer() else r
    if isinstance(v, (list, tuple)):     # LDBC's collected lists are sets: compared sorted
        if len(v) == 0:                  # an empty collection: SQL's array_agg gives NULL
            return None
        items = [canon_value(x) for x in v]
        if all(not isinstance(x, (list, dict)) for x in items):
            # a tuple of scalars ([name, year, place]): the two references order and type it differently
            return sorted(("" if x is None else str(x)) for x in items)
        return sorted(items, key=lambda x: json.dumps(x, default=str))
    if isinstance(v, dict):
        return {k: canon_value(x) for k, x in v.items()}
    return v


def canon_rows(rows) -> list:
    out = []
    for r in rows:
        r = [canon_value(v) for v in r]
        for i, x in enumerate(r):        # a list that arrives as JSON text (match `collect`)
            if isinstance(x, str) and x[:1] == "[" and x[-1:] == "]":
                try:
                    r[i] = canon_value(json.loads(x))
                except ValueError:
                    pass
        out.append(r)
    return out


def hrows(rows: list) -> str:
    return hashlib.sha256(json.dumps(rows, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()[:24]


# --------------------------------------------------------------------------- #
# systems                                                                       #
# --------------------------------------------------------------------------- #

#: DuckDB dialect fixes to reference texts: PostgreSQL's integer `/` is DuckDB's `//`
DUCKDB_FIX = {"IC7": [("AS INTEGER) / 60", "AS INTEGER) // 60")]}
#: fixes for every SQL system: the reference IC1 groups by Person.id and relies on its primary key for the
#: other Person columns, which a view does not have (PostgreSQL and DuckDB both refuse); email and speaks
#: are added to the GROUP BY (functionally dependent, no change to the answer)
SQL_FIX = {"IC1": [("GROUP BY Person.id, lastName, birthday, creationDate, gender, browserUsed, locationIP, City.name",
                    "GROUP BY Person.id, lastName, birthday, creationDate, gender, browserUsed, locationIP, City.name, Person.email, Person.speaks")]}


class DuckDB:
    name = "duckdb"

    def __init__(self, db: str, threads: int | None = None):
        import duckdb
        self.con = duckdb.connect(db, read_only=True)
        self.con.execute("SET TimeZone='UTC'")
        self.con.execute(f"SET memory_limit='{os.environ.get('W3_DUCKDB_MEM', '20GB')}'")
        if threads:
            self.con.execute(f"SET threads={threads}")

    def run(self, qid: str, inst: dict, setting: str):
        fam, path = CATALOG[qid]
        sv = setting_values(inst, setting)
        for k, v in sv.items():
            self.con.execute(f"SET VARIABLE w3_{k} = {int(v)}::BIGINT")
        mode = "window" if setting == "window" else "instant"
        self.con.execute(f"SET search_path = '{fam}_{mode},main'")
        text = bind_sql(qtext(qid, path, setting), query_params(qid, inst, setting))
        for a_, b_ in DUCKDB_FIX.get(qid, []) + SQL_FIX.get(qid, []):
            text = text.replace(a_, b_)
        import threading
        import prime_cap as PC
        timer = threading.Timer(PC.cap(CAP_S), self.con.interrupt)
        timer.start()
        try:
            cur = self.con.execute(text)
            return [d[0] for d in cur.description], cur.fetchall()
        finally:
            timer.cancel()


class Postgres:
    name = "postgres"

    def __init__(self, dsn: str):
        import psycopg
        self.con = psycopg.connect(dsn, autocommit=True)
        self.con.execute("SET TimeZone='UTC'")
        self.con.execute(f"SET statement_timeout = '{CAP_S}s'")
        import prime_cap as PC
        PC.hook(lambda sec: self.con.execute(f"SET statement_timeout = '{int(sec) if sec else CAP_S}s'"))

    def run(self, qid: str, inst: dict, setting: str):
        fam, path = CATALOG[qid]
        sv = setting_values(inst, setting)
        for k, v in sv.items():
            self.con.execute(f"SET w3.{k} = '{int(v)}'")
        mode = "window" if setting == "window" else "instant"
        self.con.execute(f"SET search_path = {fam}_{mode}, public")
        text = bind_sql(qtext(qid, path, setting), query_params(qid, inst, setting))
        for a_, b_ in SQL_FIX.get(qid, []):
            text = text.replace(a_, b_)
        cur = self.con.execute(text)
        return [d.name for d in cur.description], cur.fetchall()


_WN = [0]


def _wend(args, col, fn, bound):
    parts = [(a[1:] + (".lo" if col == "vt_s" else ".hi")) if a.startswith("^") else
             (a if a.startswith(("greatest(", "least(", ":")) or "(" in a else f"{a}.{col}") for a in args]
    return f"{fn}(" + ", ".join(parts + [bound]) + ")"


def _hop(p0: str, f: str, k: int, lo: str, hi: str) -> str:
    """EXISTS a KNOWS walk of exactly k hops p0 -> f through Person versions, all co-valid with [lo, hi)."""
    _WN[0] += 1
    n = _WN[0]
    al = [f"w{n}_p0"] + [x for i in range(1, k + 1) for x in (f"w{n}_k{i}", f"w{n}_m{i}")][:-1]
    al.append(f"w{n}_pf")          # the end person's own version is part of the walk
    frm = ([f"Person w{n}_p0"] + [x for i in range(1, k + 1) for x in (f"Person_knows_Person w{n}_k{i}", f"Person w{n}_m{i}")][:-1]
           + [f"Person w{n}_pf"])
    cond = [f"w{n}_p0.id = {p0}", f"w{n}_k1.Person1Id = {p0}"]
    for i in range(1, k):
        cond += [f"w{n}_m{i}.id = w{n}_k{i}.Person2Id", f"w{n}_k{i + 1}.Person1Id = w{n}_m{i}.id"]
    cond.append(f"w{n}_k{k}.Person2Id = {f}")
    cond.append(f"w{n}_pf.id = {f}")
    cond.append("greatest(" + ", ".join(f"{a}.vt_s" for a in al) + f", {lo}) < least(" + ", ".join(f"{a}.vt_e" for a in al) + f", {hi})")
    return f"EXISTS (SELECT 1 FROM {', '.join(frm)} WHERE {' AND '.join(cond)})"


def _walks(p0: str, kmin: int, kmax: int) -> str:
    """The co-valid KNOWS walks of kmin..kmax hops from p0 as a relation (f, lo, hi): f the end person, [lo, hi)
    the walk's own common interval (every person version on it, the end person's included, and every friendship)
    within the window. Used where the walk's interval must be carried into the binding (G11 verdict 1)."""
    arms = []
    for k in range(kmin, kmax + 1):
        _WN[0] += 1
        n = _WN[0]
        al = [f"v{n}_p0"] + [x for i in range(1, k + 1) for x in (f"v{n}_k{i}", f"v{n}_m{i}")][:-1] + [f"v{n}_pf"]
        frm = ([f"Person v{n}_p0"] + [x for i in range(1, k + 1) for x in (f"Person_knows_Person v{n}_k{i}", f"Person v{n}_m{i}")][:-1]
               + [f"Person v{n}_pf"])
        cond = [f"v{n}_p0.id = {p0}", f"v{n}_k1.Person1Id = {p0}"]
        for i in range(1, k):
            cond += [f"v{n}_m{i}.id = v{n}_k{i}.Person2Id", f"v{n}_k{i + 1}.Person1Id = v{n}_m{i}.id"]
        cond += [f"v{n}_pf.id = v{n}_k{k}.Person2Id"]
        lo = "greatest(" + ", ".join(f"{a}.vt_s" for a in al) + ", :VA)"
        hi = "least(" + ", ".join(f"{a}.vt_e" for a in al) + ", :VB)"
        arms.append(f"SELECT DISTINCT v{n}_pf.id AS f, {lo} AS lo, {hi} AS hi, {k} AS hops FROM {', '.join(frm)} "
                    f"WHERE {' AND '.join(cond)} AND {lo} < {hi}")
    return "(" + " UNION ALL ".join(arms) + ")"


def _band(p0: str, f: str, kmax: int, amin: str, amax: str, lo: str, hi: str) -> str:
    """W1's band test on the least walk depth, per instant (G11 verdict 2): some instant t of [lo, hi) has a co-valid
    walk p0 -> f of k hops, a <= k <= b, and no shorter co-valid walk at t. Candidate instants are the start of each
    k-walk's intersection with [lo, hi) and every shorter walk's end inside it. The band [amin, amax] may be
    parameters; walks of up to kmax hops are enumerated."""
    arms = []
    for k in range(1, kmax + 1):
        _WN[0] += 1
        n = _WN[0]
        L, H = f"greatest({lo}, b{n}.lo)", f"least({hi}, b{n}.hi)"
        cond = f"b{n}.f = {f} AND b{n}.hops = {k} AND {L} < {H}"
        inband = f"({k} BETWEEN {amin} AND {amax})"
        if k == 1:
            arms.append(f"({inband} AND EXISTS (SELECT 1 FROM {_walks(p0, 1, 1)} b{n} WHERE {cond}))")
            continue
        sh = f"FROM {_walks(p0, 1, k - 1)} s{n} WHERE s{n}.f = {f}"
        arms.append(f"({inband} AND EXISTS (SELECT 1 FROM {_walks(p0, k, k)} b{n} WHERE {cond} AND {_not(L, H, sh, '(^s' + str(n) + ')')}))")
    return "(" + " OR ".join(arms) + ")"


def _search(name: str, seeds: str, edges: str, rounds: int) -> str:
    """W1-paths: a bounded Bellman-Ford over co-valid walks. Round i keeps, per (source, node), the Pareto front
    of (lo, hi, cost) over walks of i hops whose persons and friendships share [lo, hi): a state is dropped
    when another state of the same (source, node) has a wider-or-equal interval and a lower-or-equal cost.
    `seeds` yields (f, node, cost, lo, hi); `edges` has (src, dst, w, lo, hi). The CTE `name` holds every
    round's states (min over it is exact for walks of at most `rounds` hops)."""
    seeds = seeds.strip()
    if seeds.startswith("(") and seeds.endswith(")"):
        seeds = seeds[1:-1]
    prune = lambda src: (f"SELECT * FROM {src} x WHERE NOT EXISTS (SELECT 1 FROM {src} y WHERE y.f = x.f AND y.node = x.node "
                         f"AND y.lo <= x.lo AND y.hi >= x.hi AND y.cost <= x.cost AND (y.lo <> x.lo OR y.hi <> x.hi OR y.cost <> x.cost))")
    parts = [f"{name}_0(f, node, cost, lo, hi) AS MATERIALIZED ({seeds})"]
    for i in range(1, rounds + 1):
        parts.append(f"{name}_{i}r(f, node, cost, lo, hi) AS MATERIALIZED (SELECT r.f, e.dst, min(r.cost + e.w), greatest(e.lo, r.lo), least(e.hi, r.hi) "
                     f"FROM {name}_{i - 1} r JOIN {edges} e ON e.src = r.node WHERE greatest(e.lo, r.lo) < least(e.hi, r.hi) "
                     f"GROUP BY r.f, e.dst, greatest(e.lo, r.lo), least(e.hi, r.hi))")
        parts.append(f"{name}_{i}(f, node, cost, lo, hi) AS MATERIALIZED ({prune(name + '_' + str(i) + 'r')})")
    parts.append(f"{name}(f, node, cost, lo, hi) AS (" + " UNION ALL ".join(f"SELECT * FROM {name}_{i}" for i in range(rounds + 1)) + ")")
    return ",\n".join(parts)


def _strip(x: str) -> str:
    x = x.strip()
    return x[1:-1].strip() if x.startswith("(") and x.endswith(")") else x


def _not(lo: str, hi: str, from_where: str, aliases: str) -> str:
    """W1 NOT EXISTS: the binding [lo, hi) survives iff some instant of it is covered by no extension. The
    extension rows are `from_where` (FROM ... WHERE ...), their interval the intersection of `aliases`'
    versions; the candidate instants are lo and every extension end inside (lo, hi)."""
    _WN[0] += 1
    n = _WN[0]
    return _notn(lo, hi, [(from_where, aliases)], n)


def _notn(lo: str, hi: str, exts: list, n: int | None = None) -> str:
    """W1 NOT EXISTS over several extension sets at one instant: the binding [lo, hi) survives iff some instant of
    it is covered by none of them. Candidate instants: lo and every extension's end inside (lo, hi)."""
    if n is None:
        _WN[0] += 1
        n = _WN[0]
    end = lambda x, c: (f"{x[1:]}.{'lo' if c == 'vt_s' else 'hi'}" if x.startswith("^") else f"{x}.{c}")
    pts, nots = [f"SELECT {lo} AS p"], []
    for fw, aliases in exts:
        al = [x.strip() for x in _strip(aliases).split(",")]
        elo = "greatest(" + ", ".join(end(x, "vt_s") for x in al) + ")"
        ehi = "least(" + ", ".join(end(x, "vt_e") for x in al) + ")"
        fw = _strip(fw)
        pts.append(f"SELECT {ehi} {fw} AND {ehi} > {lo} AND {ehi} < {hi}")
        nots.append(f"NOT EXISTS (SELECT 1 {fw} AND {elo} <= wn{n}.p AND wn{n}.p < {ehi})")
    return f"EXISTS (SELECT 1 FROM ({' UNION ALL '.join(pts)}) wn{n} WHERE {' AND '.join(nots)})"


def expand_w(text: str) -> str:
    """The window rule W1's shared SQL macros: W_LO, W_HI, W_OK, W_IN, W_NOT, W_NOTN, W_HOP, W_REACH,
    W_WALKS (walks as a relation with their own interval), W_BAND (per-instant least-depth band), W_DIST, W_SEARCH."""
    from w3_snb_systems import macro
    for _ in range(4):        # nested uses expand from the outside in; arguments are re-expanded
        text = macro(text, "W_LO", lambda a: _wend(a, "vt_s", "greatest", ":VA"))
        text = macro(text, "W_HI", lambda a: _wend(a, "vt_e", "least", ":VB"))
        text = macro(text, "W_OK", lambda a: f"(W_LO({', '.join(a)}) < W_HI({', '.join(a)}))")
        text = macro(text, "W_IN", lambda a: "(greatest(" + ", ".join([a[0]] + [x[1:] + ".lo" if x.startswith("^") else f"{x}.vt_s" for x in a[2:]])
                     + ") < least(" + ", ".join([a[1]] + [x[1:] + ".hi" if x.startswith("^") else f"{x}.vt_e" for x in a[2:]]) + "))")
        text = macro(text, "W_NOTN", lambda a: _notn(a[0], a[1], list(zip(a[2::2], a[3::2]))))
        text = macro(text, "W_NOT", lambda a: _not(*a))
        text = macro(text, "W_WALKS", lambda a: _walks(a[0], int(a[1]), int(a[2])))
        text = macro(text, "W_BAND", lambda a: _band(a[0], a[1], int(a[2]), a[3], a[4], a[5], a[6]))
        text = macro(text, "W_HOP", lambda a: _hop(a[0], a[1], int(a[2]), a[3], a[4]))
        text = macro(text, "W_REACH", lambda a: "(" + " OR ".join(_hop(a[0], a[1], k, a[3], a[4]) for k in range(1, int(a[2]) + 1)) + ")")
        text = macro(text, "W_SEARCH", lambda a: _search(a[0], a[1], a[2], int(a[3])))
        text = macro(text, "W_DIST", lambda a: "CASE " + " ".join(f"WHEN {_hop(a[0], a[1], k, a[3], a[4])} THEN {k}" for k in range(1, int(a[2]) + 1)) + " END")
    return text


def qtext(qid: str, path: Path, setting: str) -> str:
    """The reference text; for the window setting a `.window.sql` sibling (documented deviation) if one exists."""
    if setting == "window" and not qid.startswith("BIH-"):     # window texts follow rule W1
        w = W3Q / "window" / (path.stem + ".window.sql")
        if w.exists():
            return expand_w(w.read_text())
        raise NotImplementedError("no sequenced (window) text for " + path.name)
    return path.read_text()


def make_system(name: str, target: str):
    if name == "duckdb":
        return DuckDB(target, int(os.environ.get("W3_DUCKDB_THREADS", "0")) or None)
    if name == "postgres":
        return Postgres(target)
    if name == "clickhouse":
        import w3_snb_systems as Y
        return Y.ClickHouse(target)
    if name in ("neo4j", "memgraph"):
        import w3_snb_systems as Y
        return Y.Cypher(target, name)
    if name in ("aeong", "aeong-prop"):
        import w3_snb_systems as Y
        uri, hist = target.split(",")
        return Y.AeonG(uri, name, hist)
    if name == "ladybug":                      # in-process
        import w3_ladybug as L
        return L.LadybugW3(target)
    if name == "gnomon":
        import w3_snb_gnomon as G
        return G.Gnomon(target)
    raise SystemExit(f"unknown system {name}")


# --------------------------------------------------------------------------- #
# commands                                                                      #
# --------------------------------------------------------------------------- #

def cmd_duckdb_load(a) -> int:
    import duckdb
    import w3_snb_sql as S
    t0 = time.time()
    con = duckdb.connect(a.db)
    con.execute("SET TimeZone='UTC'")
    counts = {}
    for t in S.HIST_TABLES:
        con.execute(f"CREATE OR REPLACE TABLE {t} AS SELECT * FROM read_parquet('{a.hist}/{t}.parquet') ORDER BY tt_s, vt_s")
        counts[t] = con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
    for mode in ("instant", "window"):
        for fam in ("bi", "iv2"):
            for s in S.view_ddl("duckdb", mode, fam):
                con.execute(s)
    con.execute("CHECKPOINT")
    con.close()
    rec = {"db": a.db, "tables": counts, "load_s": round(time.time() - t0, 1), "bytes": os.path.getsize(a.db)}
    print(json.dumps(rec))
    Path(a.db + ".load.json").write_text(json.dumps(rec, indent=1) + "\n")
    return 0


#: anchor parameters and the history table that must hold the id (P-7: no vacuous instance)
ANCHORS = {"personId": "Person_v", "person1Id": "Person_v", "person2Id": "Person_v", "messageId": "Message_h"}


def check_inputs(sysobj, params_path: str, hist_db: str | None) -> dict:
    """Refuse an empty target and count the instances whose anchors do not exist (P-7)."""
    import duckdb
    rec = {}
    if hist_db and os.path.exists(hist_db):
        con = duckdb.connect(hist_db, read_only=True)
        n = con.execute("SELECT count(*) FROM Person_v").fetchone()[0]
        if n == 0:
            raise SystemExit(f"{hist_db}: no Person versions: refusing a vacuous answer check")
        missing = 0
        for line in open(params_path):
            inst = json.loads(line)
            for k, t in ANCHORS.items():
                if k in inst["params"]:
                    col = "MessageId" if t == "Message_h" else "id"
                    if not con.execute(f"SELECT 1 FROM {t} WHERE {col} = ? LIMIT 1", [inst["params"][k][1]]).fetchone():
                        missing += 1
        rec = {"person_versions": n, "instances_with_missing_anchor": missing}
        if missing:
            raise SystemExit(f"{missing} parameter instances name an anchor that does not exist: refusing (P-7)")
    return rec


class CappedGnomon:
    """Gnomon under the per-cell check cap (CAP_S), as the baselines are capped: the store lives in a worker
    process; a cell that does not answer within the cap is recorded "> cap", and the worker is killed (the engine
    call cannot be interrupted in-process) and started again for the next cell."""
    name = "gnomon"

    def __init__(self, target: str):
        self.target, self.proc, self.conn = target, None, None
        self.startups: list[float] = []      # seconds per worker start (store open), outside every cell's cap
        self.primes: list[float] = []        # seconds per priming call of a fresh worker, outside every cell's cap

    def _start(self):
        import multiprocessing as mp
        ctx = mp.get_context("fork")
        self.conn, child = ctx.Pipe()

        def worker(c, target):
            g = make_system("gnomon", target)
            c.send(("ready", None))          # the store is open: the parent starts a cell's cap only after this
            while True:
                job = c.recv()
                if job is None:
                    return
                try:
                    c.send(("ok", g.run(*job)))
                except NotImplementedError as e:
                    c.send(("ni", str(e)))
                except Exception as e:      # noqa: BLE001 - passed back to the parent, recorded per cell
                    c.send(("err", f"{type(e).__name__}: {e}"))
        self.proc = ctx.Process(target=worker, args=(child, self.target), daemon=True)
        self.proc.start()
        # store open (cold, >= 10 s at SF1) ran inside the next cell's cap after every kill, so at a 10 s cap one
        # timeout made the following cells time out too.
        t0 = time.perf_counter()
        if not self.conn.poll(STARTUP_S):
            self.proc.kill(); self.proc.join(); self.proc = None
            raise RuntimeError(f"gnomon worker did not open the store within {STARTUP_S} s")
        kind, _ = self.conn.recv()
        if kind != "ready":
            raise RuntimeError(f"gnomon worker start: {kind}")
        self.startups.append(round(time.perf_counter() - t0, 3))

    def run(self, qid: str, inst: dict, setting: str):
        if self.proc is None or not self.proc.is_alive():
            self._start()
            # a fresh worker's first query is cold, so after the
            # store open the cell's own call runs once unmeasured, outside the cap (the priming rule), and the
            # cell is then answered under the cap. Without it, one timeout cut every following cell.
            t0 = time.perf_counter()
            self.conn.send((qid, inst, setting))
            if not self.conn.poll(PRIME_S):
                self.proc.kill(); self.proc.join(); self.proc = None
                raise TimeoutError(f"> cap: the priming call gave no answer within {PRIME_S} s (worker killed)")
            self.conn.recv()
            self.primes.append(round(time.perf_counter() - t0, 3))
        self.conn.send((qid, inst, setting))
        if not self.conn.poll(CAP_S):
            self.proc.kill(); self.proc.join(); self.proc = None
            raise TimeoutError(f"> cap: no answer within {CAP_S} s (cell aborted; worker killed)")
        kind, val = self.conn.recv()
        if kind == "ok":
            return val
        if kind == "ni":
            raise NotImplementedError(val)
        raise RuntimeError(val)


def cmd_run(a) -> int:
    capped = a.system == "gnomon" and os.environ.get("W3_GNOMON_CAP", "1") == "1"
    sysobj = CappedGnomon(a.target) if capped else make_system(a.system, a.target)
    print(json.dumps({"inputs": check_inputs(sysobj, a.params, a.hist_db)}))
    want = set(a.queries.split(",")) if a.queries else None
    settings = a.settings.split(",")
    out = open(a.out, "a")
    n = 0
    for line in open(a.params):
        inst = json.loads(line)
        if want and inst["qid"] not in want:
            continue
        for s in settings:
            rec = {"system": a.system, "qid": inst["qid"], "i": inst["i"], "setting": s}
            t0 = time.perf_counter()
            try:
                cols, rows = sysobj.run(inst["qid"], inst, s)
                rows = canon_rows(rows)
                if inst["qid"] in COMPARE_LAST:      # the answer is the weight; the path is one of possibly several
                    cols, rows = cols[-1:], [r[-1:] for r in rows]
                rec.update(ok=True, n=len(rows), hash=hrows(rows), sorted_hash=hrows(sorted(rows, key=json.dumps)),
                           cols=cols, rows=rows[: a.keep])
            except NotImplementedError as e:
                rec.update(ok=False, kind="not-expressible", err=str(e)[:300])
            except TimeoutError as e:
                rec.update(ok=False, kind="timeout", err=str(e)[:300])
            except Exception as e:          # noqa: BLE001 - recorded per cell
                rec.update(ok=False, kind="error", err=f"{type(e).__name__}: {e}"[:600])
                if a.system == "postgres":
                    pass
            rec["ms"] = round((time.perf_counter() - t0) * 1e3, 3)
            out.write(json.dumps(rec, default=str) + "\n"); out.flush()
            n += 1
    st, pm = getattr(sysobj, "startups", None), getattr(sysobj, "primes", None)
    print(f"{a.system}: {n} cells -> {a.out}" + (f"; {len(st)} worker starts (store open, outside the cap): max {max(st)} s" if st else "")
          + (f"; {len(pm)} priming calls (outside the cap): max {max(pm)} s" if pm else ""))
    return 0


def load_results(p: str) -> dict:
    """Results; where a cell's rows were kept in full, its hashes are recomputed with the current canonical
    form (so a canonicalisation fix needs no re-run)."""
    r = {}
    for line in open(p):
        x = json.loads(line)
        if x.get("ok") and x.get("rows") is not None and len(x["rows"]) == x["n"]:
            rows = canon_rows(x["rows"])
            x["hash"], x["sorted_hash"] = hrows(rows), hrows(sorted(rows, key=json.dumps))
        r[(x["qid"], x["i"], x["setting"])] = x
    return r


def cmd_compare(a) -> int:
    base = load_results(a.base)
    others = {p: load_results(p) for p in a.others}
    table: dict = {}
    for key, b in sorted(base.items()):
        for p, res in others.items():
            o = res.get(key)
            sysname = o["system"] if o else Path(p).stem
            if o is None:
                v = "missing"
            elif not b.get("ok"):
                v = "base-" + b.get("kind", "error")
            elif not o.get("ok"):
                v = o.get("kind", "error")
            elif o["hash"] == b["hash"]:
                v = "equivalent"
            elif o["sorted_hash"] == b["sorted_hash"]:
                v = "same-rows-other-order"
            else:
                v = "DIFFERS"
            table.setdefault((key[0], key[2], sysname), {}).setdefault(v, 0)
            table[(key[0], key[2], sysname)][v] += 1
    empty = {}
    for key, b in base.items():
        if b.get("ok"):
            empty.setdefault((key[0], key[2]), []).append(b["n"] == 0)
    for (q, st), xs in empty.items():
        if xs and all(xs):
            table.setdefault((q, st, "ALL-EMPTY(base)"), {"every instance empty": len(xs)})
    lines = ["| query | setting | system | verdicts |", "|---|---|---|---|"]
    for (q, s, sysname), vs in sorted(table.items()):
        lines.append(f"| {q} | {s} | {sysname} | " + ", ".join(f"{k} {n}" for k, n in sorted(vs.items())) + " |")
    print("\n".join(lines))
    if a.json:
        Path(a.json).write_text(json.dumps({"|".join(k): v for k, v in table.items()}, indent=1) + "\n")
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sp = p.add_subparsers(dest="cmd", required=True)
    d = sp.add_parser("duckdb-load"); d.add_argument("--hist", required=True); d.add_argument("--db", required=True)
    r = sp.add_parser("run"); r.add_argument("--system", required=True); r.add_argument("--target", required=True)
    r.add_argument("--params", required=True); r.add_argument("--queries"); r.add_argument("--settings", default="current,asof")
    r.add_argument("--out", required=True); r.add_argument("--keep", type=int, default=20)
    r.add_argument("--hist-db", help="the DuckDB history database, for the non-vacuity checks (P-7)")
    c = sp.add_parser("compare"); c.add_argument("--base", required=True); c.add_argument("others", nargs="+")
    c.add_argument("--json")
    a = p.parse_args()
    return {"duckdb-load": cmd_duckdb_load, "run": cmd_run, "compare": cmd_compare}[a.cmd](a)


if __name__ == "__main__":
    sys.exit(main())
