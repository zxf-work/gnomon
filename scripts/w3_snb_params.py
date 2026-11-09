"""W3 parameter curation: LDBC's curated parameters, extended for belief time.

**This is not an LDBC Benchmark, and nothing produced here is an LDBC Benchmark Result.**

    w3_snb_params.py --ldbc PARAMS_DIR --base BASE --corrections LOG.corrections.parquet --n 10 --out P.jsonl

**Input.** The candidate lists are LDBC's own, produced by the unmodified paramgens on our raw-mode factor tables:
- `ldbc_snb_bi/paramgen` -> `bi/bi-<q>.csv`, which carries a typed header;
- `ldbc_snb_interactive_v2_driver/paramgen` -> `iv2/interactive-<q>.parquet`.

Those paramgens already apply LDBC's curation: factor tables, percentile anchors, MD5 shuffles, per-day buckets and path-length bounds. We keep LDBC's order and add two strata on top.

**Transaction-time buckets.** The update stream's commit days are split into four quartile buckets. Instance i draws a day from bucket i mod 4, picked by an MD5 hash of (query, i). From that day:
- T = the day's commit point (2 s after the day end, after the day's base and correction transactions);
- T1 = the previous commit point;
- the sequenced window is [day end - 30 d, day end).

**Correction strata.** An anchored query (one with a person or message parameter) is labelled `touched` or `free`:
- free: the anchor is one of the injector's protected anchors (`--protect`, an MD5 rule): no correction at any density touches it, its incident edges, or an entity that owns an edge to it;
- touched: the anchor, or an element incident to it, is corrected already in the lowest positive density's log (pass that log's corrections), hence at every density, since the densities are nested.
Other anchors are not used.

Instances alternate touched / free, which gives a fixed 1:1 ratio. The densities are nested (`w3_snb_log.py inject` writes each rho as a subset), so a free anchor is free at every density. The same parameter file therefore serves every density, and the density sweep changes the work, not the chosen sources. Queries with no anchor (most BI) are labelled `global`.

**Message candidates (IS4-IS7, TLDBC-IS4-6).** LDBC's messageId file holds about 36 messages per SF, too few for
a touched stratum (PROBLEMS P9). They are replaced by the driver's own source, the messages of the curated
persons and their friends (`driver_messages`, DEVIATIONS D-W3-8), taken from the frozen log's base tables.

**Non-vacuity re-selection (`--nonempty`).** For the listed queries a candidate is used only if the DuckDB reference answer is non-empty at both the current and the asof setting. LDBC's order is kept; the empty candidates are skipped, and the report counts the candidates tested and the non-empty ones found. When fewer than n are found within the scan limit, the remaining instances are LDBC's next candidates, as without re-selection (they come after the non-empty ones). This is applied to queries whose first candidates gave an empty answer at every instance.

**Reachability (`--nopath`, IC13 and IC14; D-W3-8).** LDBC's Interactive v2 paramgen writes two variants of each: `13a`/`14a` are pairs "guaranteed that no path exists" (in different connected components), and `13b`/`14b` pairs "guaranteed that there is a path" (people4Hops). The kit reads both:
- n - 1 instances come from the `b` file, in LDBC's order, split (n - 1) // 2 touched and the rest free. Each must be reachable in the DuckDB reference at the current and the asof setting (IC13 not -1, IC14 non-empty), because a pair connected at the end of the log need not be connected at the instance's T.
- Exactly one instance, labelled stratum "no-path", is the first `a` pair in LDBC's order that has no path at either setting. An unreachable pair is a legitimate LDBC case.
- If too few reachable `b` pairs are found within the scan limit, the remaining instances are the next `b` candidates without the test.

**Output.** Every system reads the same file. Each line holds `{qid, i, stratum, params: {name: [TYPE, value]}, T, T1, VA, VB, A, B}`. DATETIME and DATE values are epoch microseconds.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import json
from pathlib import Path

import duckdb

from w3_snb_log import protected_uid

OPEN = 2 ** 62
DAY = 86_400_000_000
SEC = 1_000_000
NOW_US = 1356998400000000   # valid instant 'now' of every instant setting

BI_FILE = {f"BI{q}": f"bi-{q}{'a' if q in (2, 8, 10, 14, 15, 16, 19, 20) else ''}.csv" for q in range(1, 21)}
IV2_FILE = {**{f"IC{q}": f"interactive-{q}{'a' if q in (3, 13, 14) else ''}.parquet" for q in range(1, 15)},
            **{f"IS{q}": "interactive-personId.parquet" for q in (1, 2, 3)},
            **{f"IS{q}": "interactive-messageId.parquet" for q in (4, 5, 6, 7)}}
#: iv2 parameter types by name (the parquet files carry no LDBC type tag)
IV2_TYPES = {"personId": "ID", "person1Id": "ID", "person2Id": "ID", "messageId": "ID", "firstName": "STRING",
             "countryXName": "STRING", "countryYName": "STRING", "tagName": "STRING", "countryName": "STRING",
             "tagClassName": "STRING", "maxDate": "DATETIME", "minDate": "DATETIME", "startDate": "DATE",
             "durationDays": "INT", "month": "INT", "workFromYear": "INT"}
ANCHOR = {"personId": "person", "person1Id": "person", "messageId": "message"}
BROWSERS = ["Chrome", "Firefox", "Internet Explorer", "Opera", "Safari"]


def md5i(*xs) -> int:
    return int(hashlib.md5("|".join(map(str, xs)).encode()).hexdigest(), 16)


def to_us(v, ptype: str):
    if v is None:
        return None
    if isinstance(v, dt.datetime):
        return int((v.replace(tzinfo=dt.timezone.utc) if v.tzinfo is None else v).timestamp() * 1e6)
    if isinstance(v, dt.date):
        return int(dt.datetime(v.year, v.month, v.day, tzinfo=dt.timezone.utc).timestamp() * 1e6)
    if ptype == "DATETIME":
        return int(dt.datetime.strptime(v, "%Y-%m-%dT%H:%M:%S.%f+00:00").replace(tzinfo=dt.timezone.utc).timestamp() * 1e6)
    if ptype == "DATE":
        return int(dt.datetime.strptime(v[:10], "%Y-%m-%d").replace(tzinfo=dt.timezone.utc).timestamp() * 1e6)
    if ptype in ("ID", "INT", "INT64", "INT32"):
        return int(v)
    if ptype == "STRING[]":
        return v.split(";") if isinstance(v, str) else list(v)
    return v


def read_bi(path: Path) -> list[dict]:
    with open(path) as f:
        r = csv.reader(f, delimiter="|")
        head = next(r)
        names = [h.split(":") for h in head]
        return [{n: [t, to_us(x, t)] for (n, t), x in zip(names, row)} for row in r]


def read_iv2(con, path: Path) -> list[dict]:
    rel = con.execute(f"SELECT * FROM read_parquet('{path}')")
    cols = [d[0] for d in rel.description]
    out = []
    for row in rel.fetchall():
        d = {}
        for c, x in zip(cols, row):
            if c in ("useFrom", "useUntil"):
                continue
            if c not in IV2_TYPES and c[:1].lower() + c[1:] in IV2_TYPES:
                c = c[:1].lower() + c[1:]       # iv2's messageId file names its column "MessageId"
            t = IV2_TYPES.get(c, "INT" if isinstance(x, int) else "STRING")
            d[c] = [t, to_us(x, t)]
        out.append(d)
    return out


def touched_sets(con, base: Path, corr: Path) -> tuple[set, set]:
    con.execute(f"CREATE TABLE c AS SELECT key FROM read_parquet('{corr}')")
    con.execute(f"CREATE TABLE e AS SELECT src, dst, rel FROM read_parquet('{base}/edges.parquet') WHERE rel IN ('HAS_CREATOR','REPLY_OF')")
    con.execute("CREATE TABLE ce AS SELECT split_part(key,'|',1)::BIGINT s, split_part(key,'|',2)::BIGINT d, split_part(key,'|',3) r FROM c WHERE key LIKE '%|%'")
    con.execute("CREATE TABLE cn AS SELECT key::BIGINT u FROM c WHERE key NOT LIKE '%|%'")
    persons = {r[0] for r in con.execute("""
        SELECT u FROM cn WHERE u % 8 = 3
        UNION SELECT s FROM ce WHERE r IN ('KNOWS','LIKES') UNION SELECT d FROM ce WHERE r IN ('KNOWS','HAS_MEMBER')
        UNION SELECT e.dst FROM cn JOIN e ON e.src = cn.u AND e.rel = 'HAS_CREATOR'""").fetchall()}
    messages = {r[0] for r in con.execute("""
        SELECT u FROM cn WHERE u % 8 = 1 UNION SELECT d FROM ce WHERE r = 'LIKES'
        UNION SELECT e.dst FROM cn JOIN e ON e.src = cn.u AND e.rel = 'REPLY_OF'""").fetchall()}
    return persons, messages


def driver_messages(con, base: Path, person_ids: list[int], thr: int) -> list[dict]:
    """Message candidates for IS4-IS7 the way LDBC's Interactive driver gets them (its short reads take their
    messageIds from complex-read results): the messages created by the curated persons or their friends, which
    is IC2's and IC9's result set, before the bulk-load threshold, and so before every instance's T. They are
    alive at the end. Computed from the frozen log's base tables, not from any system's answer; MD5 order
    (DEVIATIONS D-W3-8)."""
    con.execute("CREATE OR REPLACE TEMP TABLE dm_p AS SELECT unnest(?::BIGINT[]) * 8 + 3 AS u", [person_ids])
    con.execute(f"""CREATE OR REPLACE TEMP TABLE dm_f AS SELECT u FROM dm_p UNION
        SELECT CASE WHEN e.src = p.u THEN e.dst ELSE e.src END FROM read_parquet('{base}/edges.parquet') e
        JOIN dm_p p ON (e.src = p.u OR e.dst = p.u) WHERE e.rel = 'KNOWS' AND e.vs < {thr} AND e.ve > {thr}""")
    rows = con.execute(f"""SELECT DISTINCT n.uid FROM read_parquet('{base}/edges.parquet') e JOIN dm_f f ON e.dst = f.u
        JOIN read_parquet('{base}/nodes.parquet') n ON n.uid = e.src
        WHERE e.rel = 'HAS_CREATOR' AND n.label IN ('Post', 'Comment') AND n.vs < {thr} AND n.ve = {OPEN}
        ORDER BY md5(n.uid::VARCHAR)""").fetchall()
    return [{"messageId": ["ID", r[0] // 8]} for r in rows]


def fallback_iv2(con, base: Path) -> dict:
    """Stand-in Interactive v2 parameters for an SF LDBC's paramgen refuses: persons and messages alive at the
    end, with at least one friend / one reply, in MD5 order; the other parameters from the data's own names."""
    con.execute(f"CREATE TABLE fbn AS SELECT uid, label, props, ve FROM read_parquet('{base}/nodes.parquet')")
    con.execute(f"CREATE TABLE fbe AS SELECT src, dst, rel FROM read_parquet('{base}/edges.parquet') WHERE rel IN ('KNOWS','REPLY_OF')")
    P = [r[0] // 8 for r in con.execute(f"""SELECT DISTINCT n.uid FROM fbn n JOIN fbe e ON e.rel = 'KNOWS' AND (e.src = n.uid OR e.dst = n.uid)
                    WHERE n.label = 'Person' AND n.ve = {OPEN} ORDER BY md5(n.uid::VARCHAR) LIMIT 400""").fetchall()]
    M = [r[0] // 8 for r in con.execute(f"""SELECT DISTINCT n.uid FROM fbn n JOIN fbe e ON e.rel = 'REPLY_OF' AND e.dst = n.uid
                    WHERE n.label IN ('Post','Comment') AND n.ve = {OPEN} ORDER BY md5(n.uid::VARCHAR) LIMIT 400""").fetchall()]
    names = lambda lab, key: [r[0] for r in con.execute(
        f"SELECT DISTINCT json_extract_string(props, '$.{key}') FROM fbn WHERE label = '{lab}' ORDER BY 1").fetchall()]
    fn, ctry, tags, tcls = names("Person", "firstName"), names("Country", "name"), names("Tag", "name"), names("TagClass", "name")
    d = lambda s: to_us(s, "DATE")
    out = {}
    pid = lambda i: ["ID", P[i % len(P)]]
    out["IS1"] = [{"personId": pid(i)} for i in range(len(P))]
    out["IS2"] = out["IS3"] = out["IS1"]
    out["IS4"] = [{"messageId": ["ID", m]} for m in M]
    out["IS5"] = out["IS6"] = out["IS7"] = out["IS4"]
    R = range(len(P))
    out["IC1"] = [{"personId": pid(i), "firstName": ["STRING", fn[md5i(i) % len(fn)]]} for i in R]
    out["IC2"] = [{"personId": pid(i), "maxDate": ["DATETIME", d("2012-09-01")]} for i in R]
    out["IC3"] = [{"personId": pid(i), "countryXName": ["STRING", ctry[md5i(i, 1) % len(ctry)]],
                   "countryYName": ["STRING", ctry[md5i(i, 2) % len(ctry)]], "startDate": ["DATE", d("2011-06-01")],
                   "durationDays": ["INT", 120]} for i in R]
    out["IC4"] = [{"personId": pid(i), "startDate": ["DATE", d("2011-06-01")], "durationDays": ["INT", 120]} for i in R]
    out["IC5"] = [{"personId": pid(i), "minDate": ["DATETIME", d("2011-01-01")]} for i in R]
    out["IC6"] = [{"personId": pid(i), "tagName": ["STRING", tags[md5i(i, 3) % len(tags)]]} for i in R]
    out["IC7"] = out["IC8"] = [{"personId": pid(i)} for i in R]
    out["IC9"] = [{"personId": pid(i), "maxDate": ["DATETIME", d("2012-09-01")]} for i in R]
    out["IC10"] = [{"personId": pid(i), "month": ["INT", 1 + md5i(i, 4) % 12]} for i in R]
    out["IC11"] = [{"personId": pid(i), "countryName": ["STRING", ctry[md5i(i, 5) % len(ctry)]], "workFromYear": ["INT", 2010]} for i in R]
    out["IC12"] = [{"personId": pid(i), "tagClassName": ["STRING", tcls[md5i(i, 6) % len(tcls)]]} for i in R]
    out["IC13"] = out["IC14"] = [{"person1Id": pid(i), "person2Id": pid(i + 7)} for i in R]
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--ldbc", required=True); ap.add_argument("--base", required=True)
    ap.add_argument("--corrections", required=True); ap.add_argument("--n", type=int, default=10)
    ap.add_argument("--out", required=True)
    ap.add_argument("--protect", type=float, default=0.3, help="the injector's protected fraction (free stratum)")
    ap.add_argument("--nonempty", default="", help="queries whose candidates must give a non-empty reference answer")
    ap.add_argument("--nonempty-db", help="the DuckDB history database the reference answer is computed on")
    ap.add_argument("--nonempty-scan", type=int, default=300, help="at most this many candidates tested per query")
    ap.add_argument("--nopath", default="", help="IC13/IC14: n - 1 reachable pairs from LDBC's b file, one no-path pair from its a file")
    a = ap.parse_args()
    nonempty = set(filter(None, a.nonempty.split(",")))
    nopath = set(filter(None, a.nopath.split(",")))
    ref = None
    if nonempty or nopath:
        from w3_snb_run import DuckDB
        ref = DuckDB(a.nonempty_db)
    scanned, found = {}, {}
    ldbc, base = Path(a.ldbc), Path(a.base)
    meta = json.loads((base / "meta.json").read_text())
    days = [d["day_end_us"] for d in meta["days"]]
    q4 = [days[i * len(days) // 4:(i + 1) * len(days) // 4] for i in range(4)]
    con = duckdb.connect()
    # anchors alive at the end of the log (D-W3-8): vt_e = OPEN_END in the base
    alive_p = {r[0] for r in duckdb.connect().execute(f"SELECT uid FROM read_parquet('{base}/nodes.parquet') "
                                         f"WHERE label = 'Person' AND ve = {OPEN}").fetchall()}
    adb = ref.con if ref is not None else (duckdb.connect(a.nonempty_db, read_only=True) if a.nonempty_db else None)
    dropped = {}

    def anchors_ok(qid, c, T) -> bool:
        """Every anchor (personId, person1Id, person2Id, messageId) is alive at the end of the log and, where the
        check database is given, believed alive at the instance's T at valid time now (the as-of state)."""
        for k in ("personId", "person1Id", "person2Id", "messageId"):
            if k not in c:
                continue
            i = c[k][1]
            if k != "messageId" and i * 8 + 3 not in alive_p:
                dropped[qid] = dropped.get(qid, 0) + 1; return False
            if adb is not None:
                tab, col = ("Message_h", "MessageId") if k == "messageId" else ("Person_v", "id")
                ok = adb.execute(f"SELECT count(*) FROM main.{tab} WHERE {col} = {int(i)} AND tt_s <= {T} AND {T} < tt_e "
                                 f"AND vt_s <= {NOW_US} AND {NOW_US} < vt_e").fetchone()[0]
                if not ok:
                    dropped[qid] = dropped.get(qid, 0) + 1; return False
        return True
    tp, tm = touched_sets(con, base, Path(a.corrections))
    report = {}
    out = open(a.out, "w")

    def make_rec(qid, i, st, c):
        b = q4[i % 4] or days
        di = days.index(b[md5i(qid, i) % len(b)])
        dend = days[di]
        rec = {"qid": qid, "i": i, "stratum": st, "params": c, "T": dend + 2 * SEC,
               "T1": (days[di - 1] if di else meta["threshold_us"]) + 2 * SEC,
               "VA": dend - 30 * DAY, "VB": dend}
        if qid == "BIH-R3":
            rec.update(A=dend - 30 * DAY, B=dend)
        elif qid == "BIH-R5":
            rec.update(A=dend - 365 * DAY, B=dend)
        return rec

    def answers(qid, rec) -> bool:
        """Non-vacuity re-selection: the reference answer is non-empty at the current and the asof setting."""
        scanned[qid] = scanned.get(qid, 0) + 1
        if scanned[qid] > a.nonempty_scan:
            return False
        try:
            return all(len(ref.run(qid, rec, s)[1]) > 0 for s in ("current", "asof"))
        except Exception:           # noqa: BLE001 - a candidate the reference cannot run is not used
            return False

    def path_class(qid, rec) -> str:
        """--nopath: 'path' (a path at current and asof), 'nopath' (none at either), else 'mixed'"""
        scanned[qid] = scanned.get(qid, 0) + 1
        if scanned[qid] > a.nonempty_scan:
            return "mixed"
        try:
            has = [(lambda r: len(r) > 0 and list(map(list, r)) != [[-1]])(ref.run(qid, rec, s)[1]) for s in ("current", "asof")]
        except Exception:           # noqa: BLE001 - a candidate the reference cannot run is not used
            return "mixed"
        return "path" if all(has) else ("nopath" if not any(has) else "mixed")

    def stratum(c):
        anc = next((k for k in c if k in ANCHOR), None)
        if anc is None:
            return "global"
        uid = c[anc][1] * 8 + (3 if ANCHOR[anc] == "person" else 1)
        if protected_uid(uid, a.protect):
            return "free"
        return "touched" if uid in (tp if ANCHOR[anc] == "person" else tm) else None

    def emit_np(qid, cands_b, cands_a):
        """--nopath: n - 1 reachable pairs from the b file, then one no-path pair from the a file (D-W3-8)"""
        chosen, want = [], {"touched": (a.n - 1) // 2, "free": a.n - 1 - (a.n - 1) // 2}

        def fill(test: bool):
            seen = {json.dumps(c, sort_keys=True) for _, c in chosen}
            for c in cands_b:
                if len(chosen) >= a.n - 1:
                    return
                st = stratum(c)
                if st is None or json.dumps(c, sort_keys=True) in seen or (st != "global" and want[st] <= 0):
                    continue
                rec = make_rec(qid, len(chosen), st, c)
                if not anchors_ok(qid, c, rec["T"]) or (test and path_class(qid, rec) != "path"):
                    continue
                if st != "global":
                    want[st] -= 1
                chosen.append((st, c))
        fill(True)
        found[qid] = len(chosen)
        fill(False)
        for c in cands_a:
            if stratum(c) is None:
                continue
            rec = make_rec(qid, len(chosen), "no-path", c)
            if anchors_ok(qid, c, rec["T"]) and path_class(qid, rec) == "nopath":
                chosen.append(("no-path", c))
                break
        for i, (st, c) in enumerate(chosen):
            out.write(json.dumps(make_rec(qid, i, st, c)) + "\n")
        report[qid] = {s: sum(1 for x, _ in chosen if x == s) for s in ("touched", "free", "global", "no-path")}

    def emit(qid, cands):
        chosen, want = [], {"touched": a.n // 2, "free": a.n - a.n // 2}

        def take(test: bool):
            seen = {json.dumps(c, sort_keys=True) for _, c in chosen}
            for c in cands:
                if len(chosen) >= a.n:
                    return
                if json.dumps(c, sort_keys=True) in seen:
                    continue
                anc = next((k for k in c if k in ANCHOR), None)
                if anc is None:
                    st = "global"
                else:
                    uid = c[anc][1] * 8 + (3 if ANCHOR[anc] == "person" else 1)
                    if protected_uid(uid, a.protect):
                        st = "free"            # protected: no correction at any density touches it
                    elif uid in (tp if ANCHOR[anc] == "person" else tm):
                        st = "touched"         # touched already at the lowest density, hence at every density
                    else:
                        continue
                if st != "global" and want[st] <= 0:
                    continue
                if not anchors_ok(qid, c, make_rec(qid, len(chosen), st, c)["T"]):
                    continue
                if test and not answers(qid, make_rec(qid, len(chosen), st, c)):
                    continue
                if st != "global":
                    want[st] -= 1
                chosen.append((st, c))

        take(qid in nonempty)
        if qid in nonempty:
            found[qid] = len(chosen)
            take(False)       # too few non-empty candidates: the rest in LDBC's order, as without re-selection
        for i, (st, c) in enumerate(chosen):
            out.write(json.dumps(make_rec(qid, i, st, c)) + "\n")
        report[qid] = {s: sum(1 for x, _ in chosen if x == s) for s in ("touched", "free", "global")}

    iv2_fallback = not (ldbc / "iv2" / "interactive-personId.parquet").exists()
    if iv2_fallback:      # LDBC's Interactive v2 paramgen refuses small SFs (PROBLEMS P5): sampled stand-ins
        fb = fallback_iv2(con, base)
    persons = fb["IS1"] if iv2_fallback else read_iv2(con, ldbc / "iv2" / "interactive-personId.parquet")
    messages = driver_messages(con, base, [c["personId"][1] for c in persons], meta["threshold_us"])
    for qid, f in IV2_FILE.items():
        p = ldbc / "iv2" / f
        if qid in ("IS4", "IS5", "IS6", "IS7"):
            emit(qid, messages)
        elif qid in nopath and p.exists():
            emit_np(qid, read_iv2(con, ldbc / "iv2" / f.replace("a.parquet", "b.parquet")), read_iv2(con, p))
        elif p.exists():
            emit(qid, read_iv2(con, p))
        elif iv2_fallback:
            emit(qid, fb[qid])
    for qid, f in BI_FILE.items():
        p = ldbc / "bi" / f
        if p.exists():
            emit(qid, read_bi(p))
    for i in range(1, 7):
        emit(f"TLDBC-IS{i}", persons if i <= 3 else messages)
    for q in ("K", "VER", "R7", "R3"):
        emit(f"BIH-{q}", persons)
    emit("BIH-R5", [{"browser": ["STRING", BROWSERS[k % len(BROWSERS)]]} for k in range(a.n)])
    out.close()
    Path(a.out + ".report.json").write_text(json.dumps({"strata": report, "nonempty_scanned": scanned, "nonempty_found": found, "dead_anchor_skipped": dropped,
                                                         "touched_persons": len(tp),
                                                         "touched_messages": len(tm)}, indent=1) + "\n")
    print(json.dumps(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
