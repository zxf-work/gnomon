#!/usr/bin/env python3
"""W4: LDBC FinBench datagen output -> a frozen Gnomon event log, with
synthetic corrections at density rho.

**FinBench-derived, not an LDBC FinBench workload or result.** Input: the raw
CSV output of `ldbc_finbench_datagen` 0.1.0 (`<sf>/raw/<entity>/*.csv`,
'|'-separated, millisecond timestamps, deleteTime 2030-01-01 = never).

History (valid time = FinBench time, microseconds; transaction time = the
replay's commit time, also in FinBench microseconds):

* split at the datagen's own bulk-load threshold T0 (transformation/
  convert_data.py: 2020-01-01 + 97 % of three years);
* bulk load at tt = T0, T0+1, ... in chunks of 100,000 ops: every node and
  edge created at or before T0, nodes first; an entity deleted at or before T0
  is asserted with vt_e = deleteTime;
* the incremental stream after T0 in commit-time order, grouped into commit
  windows of `--commit-window-ms` (default one hour; a batch's tt is the end of
  its window): node and edge inserts at createTime, deletes (retract at
  deleteTime) at deleteTime;
* nodes valid [createTime, deleteTime or OPEN_END); edges [ts, OPEN_END) until
  a delete, with disc = the edge's ordinal, in time order, among the
  occurrences of the same (src, dst, rel_type) for the multi-edge relations
  TRANSFER (transfer + loantransfer), WITHDRAW, DEPOSIT, REPAY, SIGN_IN; "" for
  OWN, INVEST, APPLY, GUARANTEE. Schema and property names follow the
  FinBench-shaped generator (gnomon/data/finbench_shaped.py), uid "<Type>:<id>".
  Dropped as long free text: Company.description, transfer comment. Account carries
  createTime (ms) as a property, for TSR1 (a retroactive block carves the version start).

Corrections (rho > 0): an edge of TRANSFER or WITHDRAW is selected when
md5(seed:key) maps below rho (nested: every edge selected at rho is selected
at every larger rho); its kind, commit time and amount factor come from the
same digest. Commit time ct is uniform in (max(ts, T0) + 1 ms, end of
stream); kinds, one quarter each:

* chargeback: `retract` at t = ts (the transfer never happened);
* amount fix: `correct` of the version believed at ct, amount x U(0.5, 1.5);
* late posting: the edge is withheld from its original batch and asserted at
  ct with its original valid time (valid time far before transaction time);
* retroactive blocking: `assert_node` of the source account with
  isBlocked = true over [ts, its end), carving the earlier version.

Measured counts and lags (batch tt - ts) per kind go to the meta file.

    python scripts/w4_finbench_log.py --raw data/sf1 --rho 0.3 --seed 0 \
        --out w4-sf1-r0.3.eventlog.jsonl
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from gnomon.core.model import canonical_json, sha256_hex  # noqa: E402

OPEN_END = 2 ** 62
NEVER_MS = 1893196800000                       # 2030-01-01: the datagen's "no delete"
START = datetime(2020, 1, 1, tzinfo=timezone.utc).timestamp()
END = datetime(2023, 1, 1, tzinfo=timezone.utc).timestamp()
T0_MS = int((START + (END - START) * 0.97) * 1000)   # convert_data.py's threshold
END_MS = int(END * 1000)
CHUNK = 100_000
MULTI = ("TRANSFER", "WITHDRAW", "DEPOSIT", "REPAY", "SIGN_IN")

# (label, raw dir, uid prefix, props sql)
NODES = [
    ("Person", "person", "struct_pack(name := name, isBlocked := isBlocked, gender := gender, "
     "birthday := birthday, country := country, city := city)", "NULL"),
    ("Company", "company", "struct_pack(name := name, isBlocked := isBlocked, country := country, "
     "city := city, business := business, url := url)", "NULL"),
    ("Account", "account", "struct_pack(isBlocked := isBlocked, accountType := type, nickname := nickname, "
     "phonenum := phonenum, email := email, freqLoginType := freqLoginType, lastLoginTime := lastLoginTime, "
     "accountLevel := accountLevel, owner := Owner, createTime := CAST(createTime AS BIGINT))", "deleteTime"),
    ("Loan", "loan", "struct_pack(loanAmount := CAST(loanAmount AS DOUBLE), balance := CAST(balance AS DOUBLE), "
     "loanUsage := usage, interestRate := CAST(interestRate AS DOUBLE))", "NULL"),
    ("Medium", "medium", "struct_pack(mediumType := type, isBlocked := isBlocked, lastLoginTime := lastLogin, "
     "riskLevel := riskLevel)", "NULL"),
]
# (rel, raw dir, src label, src col, dst label, dst col, props sql, delete col, order col)
EDGES = [
    ("TRANSFER", "transfer", "Account", "fromId", "Account", "toId",
     "struct_pack(amount := CAST(amount AS DOUBLE), orderNum := CAST(orderNum AS VARCHAR), payType := payType, "
     "goodsType := goodsType)", "deleteTime", "multiplicityId"),
    ("TRANSFER", "loantransfer", "Account", "fromId", "Account", "toId",
     "struct_pack(amount := CAST(amount AS DOUBLE), orderNum := CAST(orderNum AS VARCHAR), payType := payType, "
     "goodsType := goodsType)", "deleteTime", "multiplicityId"),
    ("WITHDRAW", "withdraw", "Account", "fromId", "Account", "toId",
     "struct_pack(amount := CAST(amount AS DOUBLE))", "deleteTime", "multiplicityId"),
    ("DEPOSIT", "deposit", "Loan", "loanId", "Account", "accountId",
     "struct_pack(amount := CAST(amount AS DOUBLE))", "deleteTime", "0"),
    ("REPAY", "repay", "Account", "accountId", "Loan", "loanId",
     "struct_pack(amount := CAST(amount AS DOUBLE))", "deleteTime", "0"),
    ("SIGN_IN", "signIn", "Medium", "mediumId", "Account", "accountId",
     "struct_pack(location := location)", "deleteTime", "multiplicityId"),
    ("OWN", "personOwnAccount", "Person", "personId", "Account", "accountId", "struct_pack(x := NULL)",
     "deleteTime", "0"),
    ("OWN", "companyOwnAccount", "Company", "companyId", "Account", "accountId", "struct_pack(x := NULL)",
     "deleteTime", "0"),
    ("INVEST", "personInvest", "Person", "investorId", "Company", "companyId",
     "struct_pack(ratio := CAST(ratio AS DOUBLE))", "NULL", "0"),
    ("INVEST", "companyInvest", "Company", "investorId", "Company", "companyId",
     "struct_pack(ratio := CAST(ratio AS DOUBLE))", "NULL", "0"),
    ("APPLY", "personApplyLoan", "Person", "personId", "Loan", "loanId", "struct_pack(org := org)", "NULL", "0"),
    ("APPLY", "companyApplyLoan", "Company", "companyId", "Loan", "loanId", "struct_pack(org := org)", "NULL", "0"),
    ("GUARANTEE", "personGuarantee", "Person", "fromId", "Person", "toId",
     "struct_pack(relation := relation)", "NULL", "0"),
    ("GUARANTEE", "companyGuarantee", "Company", "fromId", "Company", "toId",
     "struct_pack(relation := relation)", "NULL", "0"),
]


def csv(raw: Path, d: str) -> str:
    return f"read_csv('{raw}/raw/{d}/*.csv', delim='|', header=true, auto_detect=true)"


def load(con, raw: Path) -> None:
    nq = []
    for label, d, props, dele in NODES:
        nq.append(f"SELECT '{label}:' || CAST(id AS VARCHAR) AS uid, '{label}' AS label, "
                  f"CAST(createTime AS BIGINT) AS ts, CAST({dele} AS BIGINT) AS del, "
                  f"to_json({props}) AS props FROM {csv(raw, d)}")
    con.execute("CREATE TABLE nodes AS " + " UNION ALL ".join(nq))
    eq = []
    for k, (rel, d, sl, sc, dl, dc, props, dele, ordc) in enumerate(EDGES):
        eq.append(f"SELECT '{rel}' AS rel, '{sl}:' || CAST({sc} AS VARCHAR) AS src, "
                  f"'{dl}:' || CAST({dc} AS VARCHAR) AS dst, CAST(createTime AS BIGINT) AS ts, "
                  f"CAST({dele} AS BIGINT) AS del, to_json({props}) AS props, {k} AS part, "
                  f"CAST({ordc} AS BIGINT) AS mult FROM {csv(raw, d)}")
    con.execute("CREATE TABLE e0 AS " + " UNION ALL ".join(eq))
    multi = ",".join(f"'{r}'" for r in MULTI)
    con.execute(f"""CREATE TABLE edges AS SELECT rel, src, dst, ts,
        CASE WHEN del IS NULL OR del >= {NEVER_MS} THEN NULL ELSE del END AS del, props,
        CASE WHEN rel IN ({multi}) THEN CAST(row_number() OVER (PARTITION BY src, dst, rel
             ORDER BY ts, part, mult) - 1 AS VARCHAR) ELSE '' END AS disc
        FROM e0""")
    con.execute("DROP TABLE e0")
    con.execute(f"UPDATE nodes SET del = NULL WHERE del >= {NEVER_MS}")


def select_corrections(con, rho: float, seed: int) -> None:
    """kind 0 chargeback, 1 amount fix, 2 late posting, 3 retroactive block."""
    thr = int(rho / 100.0 * 2 ** 32)
    con.execute(f"""CREATE TABLE corr AS
      WITH h AS (SELECT rel, src, dst, disc, ts, del, props,
                   md5('{seed}:' || rel || '|' || src || '|' || dst || '|' || disc) AS d
                 FROM edges WHERE rel IN ('TRANSFER', 'WITHDRAW')),
      g AS (SELECT *, CAST(('0x' || substr(d, 1, 8)) AS UBIGINT) AS u1,
                   CAST(('0x' || substr(d, 9, 8)) AS UBIGINT) % 4 AS kind,
                   CAST(('0x' || substr(d, 17, 8)) AS UBIGINT) / 4294967296.0 AS u2,
                   CAST(('0x' || substr(d, 25, 8)) AS UBIGINT) / 4294967296.0 AS u3,
                   greatest(ts, {T0_MS}) + 1 AS lo FROM h)
      SELECT rel, src, dst, disc, ts, del, props, kind, u3,
             lo + CAST(floor(u2 * ({END_MS} - lo)) AS BIGINT) AS ct
      FROM g WHERE u1 < {thr} AND lo < {END_MS}""")


def ms_us(x: int) -> int:
    return int(x) * 1000


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--raw", type=Path, required=True)
    ap.add_argument("--rho", type=float, default=0.0, help="correction share in percent")
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--commit-window-ms", type=int, default=3_600_000)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--threads", type=int, default=16)
    a = ap.parse_args()
    t_start = time.time()
    con = duckdb.connect()
    con.execute(f"SET threads={a.threads}")
    load(con, a.raw)
    counts = {"nodes": con.execute("SELECT count(*) FROM nodes").fetchone()[0],
              "edges": dict(con.execute("SELECT rel, count(*) FROM edges GROUP BY rel ORDER BY rel").fetchall())}
    corr_on = a.rho > 0
    if corr_on:
        select_corrections(con, a.rho, a.seed)
    else:
        con.execute("CREATE TABLE corr AS SELECT * FROM (SELECT ''::VARCHAR rel, ''::VARCHAR src, ''::VARCHAR dst, "
                    "''::VARCHAR disc, 0::BIGINT ts, 0::BIGINT del, ''::VARCHAR props, 0::BIGINT kind, "
                    "0.0::DOUBLE u3, 0::BIGINT ct) WHERE false")
    # late-posted edges leave their original position; a chargeback or a late posting
    # committed after the edge's delete folds the delete into it (no retract of a
    # version that is gone or not yet written)
    con.execute("""CREATE TABLE late AS SELECT rel, src, dst, disc FROM corr WHERE kind = 2""")
    con.execute("""CREATE TABLE nodel AS SELECT rel, src, dst, disc FROM corr
                   WHERE (kind = 2 AND del IS NOT NULL AND del < ct)
                      OR (kind = 0 AND (del IS NULL OR del > ct))""")
    W = a.commit_window_ms
    out = a.out
    out.parent.mkdir(parents=True, exist_ok=True)
    f = open(out, "w", encoding="utf-8")
    f.write(canonical_json({"format": "gnomon-events", "version": 1}) + "\n")
    nb = 0

    def emit(tt: int, ops: list) -> None:
        nonlocal nb
        bid = sha256_hex(canonical_json({"tt": tt, "ops": ops}))[:16]
        f.write(canonical_json({"batch_id": bid, "tt": tt, "ops": ops}) + "\n")
        nb += 1

    def props_of(p: str) -> dict:
        d = json.loads(p)
        return {k: v for k, v in d.items() if v is not None and k != "x"}

    # ---- bulk ----------------------------------------------------------------
    tt = ms_us(T0_MS)
    batch: list = []
    q_nodes = con.execute(f"""SELECT uid, label, ts, del, props FROM nodes WHERE ts <= {T0_MS}
                              ORDER BY ts, uid""")
    while rows := q_nodes.fetchmany(50_000):
        for uid, label, ts, dl, p in rows:
            batch.append({"op": "assert_node", "uid": uid, "label": label, "props": props_of(p),
                          "vt_s": ms_us(ts), "vt_e": ms_us(dl) if dl is not None and dl <= T0_MS else OPEN_END,
                          "source": "ingest", "provenance_ref": None})
            if len(batch) >= CHUNK:
                emit(tt, batch); tt += 1; batch = []
    q_edges = con.execute(f"""SELECT e.rel, e.src, e.dst, e.disc, e.ts, e.del, e.props FROM edges e
        ANTI JOIN late l USING (rel, src, dst, disc) WHERE e.ts <= {T0_MS} ORDER BY e.ts, e.src, e.dst, e.rel, e.disc""")
    while rows := q_edges.fetchmany(50_000):
        for rel, s, d, disc, ts, dl, p in rows:
            batch.append({"op": "assert_edge", "src": s, "dst": d, "rel_type": rel, "disc": disc,
                          "props": props_of(p), "vt_s": ms_us(ts),
                          "vt_e": ms_us(dl) if dl is not None and dl <= T0_MS else OPEN_END,
                          "source": "ingest", "provenance_ref": None})
            if len(batch) >= CHUNK:
                emit(tt, batch); tt += 1; batch = []
    if batch:
        emit(tt, batch); tt += 1; batch = []
    bulk_batches = nb
    # ---- incremental stream + corrections ------------------------------------
    # pri: node assert 0, edge assert 1, correction 2, edge retract 3, node retract 4
    con.execute(f"""CREATE TABLE inc AS
      SELECT ts AS ct, 0 AS pri, 'an' AS k, uid AS a, label AS b, '' AS c, '' AS disc, ts, del, props, 0 AS kind, 0.0 AS u3
        FROM nodes WHERE ts > {T0_MS}
      UNION ALL
      SELECT e.ts, 1, 'ae', e.src, e.dst, e.rel, e.disc, e.ts, e.del, e.props, 0, 0.0
        FROM edges e ANTI JOIN late l USING (rel, src, dst, disc) WHERE e.ts > {T0_MS}
      UNION ALL
      SELECT del, 4, 'rn', uid, label, '', '', ts, del, NULL, 0, 0.0 FROM nodes
        WHERE del IS NOT NULL AND del > {T0_MS} AND del < {END_MS}
      UNION ALL
      SELECT e.del, 3, 're', e.src, e.dst, e.rel, e.disc, e.ts, e.del, NULL, 0, 0.0
        FROM edges e ANTI JOIN nodel n USING (rel, src, dst, disc)
        WHERE e.del IS NOT NULL AND e.del > {T0_MS} AND e.del < {END_MS}
      UNION ALL
      SELECT ct, 2, 'c', src, dst, rel, disc, ts, del, props, kind, u3 FROM corr""")
    acct = {}
    if corr_on:
        for uid, ts, dl, p in con.execute("""SELECT n.uid, n.ts, n.del, n.props FROM nodes n
                SEMI JOIN (SELECT src FROM corr WHERE kind = 3) c ON n.uid = c.src""").fetchall():
            acct[uid] = (ts, dl, props_of(p))
    lags: dict[str, list[int]] = {"chargeback": [], "amount_fix": [], "late_posting": [], "retro_block": []}
    names = ["chargeback", "amount_fix", "late_posting", "retro_block"]
    first_w = None
    cur_w = None
    q = con.execute(f"SELECT * FROM inc WHERE ct < {END_MS} ORDER BY ct, pri, a, b, c, disc")
    while rows := q.fetchmany(50_000):
        for ct, pri, k, x, y, rel, disc, ts, dl, p, kind, u3 in rows:
            w = (ct - T0_MS - 1) // W
            if cur_w is not None and w != cur_w and batch:
                emit(max(tt, ms_us(T0_MS + (cur_w + 1) * W)), batch); tt = max(tt, ms_us(T0_MS + (cur_w + 1) * W)) + 1
                batch = []
            cur_w = w
            btt = max(tt, ms_us(T0_MS + (w + 1) * W))
            if k == "an":
                batch.append({"op": "assert_node", "uid": x, "label": y, "props": props_of(p), "vt_s": ms_us(ts),
                              "vt_e": OPEN_END, "source": "ingest", "provenance_ref": None})
            elif k == "ae":
                batch.append({"op": "assert_edge", "src": x, "dst": y, "rel_type": rel, "disc": disc,
                              "props": props_of(p), "vt_s": ms_us(ts), "vt_e": OPEN_END,
                              "source": "ingest", "provenance_ref": None})
            elif k == "rn":
                batch.append({"op": "retract", "ref": {"kind": "node", "uid": x}, "t": ms_us(dl),
                              "source": "ingest", "provenance_ref": None})
            elif k == "re":
                batch.append({"op": "retract", "ref": {"kind": "edge", "src": x, "dst": y, "rel_type": rel,
                                                       "disc": disc}, "t": ms_us(dl),
                              "source": "ingest", "provenance_ref": None})
            else:
                ref = {"kind": "edge", "src": x, "dst": y, "rel_type": rel, "disc": disc}
                ve = ms_us(dl) if dl is not None and dl <= ct else OPEN_END
                if kind == 0:
                    op = {"op": "retract", "ref": ref, "t": ms_us(ts)}
                elif kind == 1:
                    pr = props_of(p)
                    pr["amount"] = round(pr["amount"] * (0.5 + u3), 2)
                    op = {"op": "correct", "ref": ref, "props": pr, "vt_s": ms_us(ts), "vt_e": ve}
                elif kind == 2:
                    op = {"op": "assert_edge", "src": x, "dst": y, "rel_type": rel, "disc": disc,
                          "props": props_of(p), "vt_s": ms_us(ts), "vt_e": ve}
                else:
                    ats, adl, apr = acct[x]
                    apr = dict(apr, isBlocked=True)
                    aend = ms_us(adl) if adl is not None and adl <= ct else OPEN_END
                    op = {"op": "assert_node", "uid": x, "label": "Account", "props": apr,
                          "vt_s": ms_us(ts), "vt_e": aend}
                op.update(source="ingest", provenance_ref=None)
                batch.append(op)
                lags[names[kind]].append(btt - ms_us(ts))
    if batch:
        emit(max(tt, ms_us(T0_MS + (cur_w + 1) * W)), batch)
    f.close()

    def dist(xs):
        if not xs:
            return {"n": 0}
        xs = sorted(xs)
        day = 86_400_000_000
        q = lambda p: round(xs[min(len(xs) - 1, int(p * len(xs)))] / day, 2)  # noqa: E731
        return {"n": len(xs), "lag_days_p10": q(.1), "lag_days_p50": q(.5), "lag_days_p90": q(.9),
                "lag_days_max": round(xs[-1] / day, 2)}

    elig = con.execute("SELECT count(*) FROM edges WHERE rel IN ('TRANSFER','WITHDRAW')").fetchone()[0]
    ncorr = sum(len(v) for v in lags.values())
    h = hashlib.sha256()
    with open(out, "rb") as g:
        for chunk in iter(lambda: g.read(1 << 22), b""):
            h.update(chunk)
    meta = {"generator": "scripts/w4_finbench_log.py (FinBench-derived)", "raw": str(a.raw),
            "rho_pct": a.rho, "seed": a.seed, "commit_window_ms": W, "t0_ms": T0_MS, "end_ms": END_MS,
            "counts": counts, "bulk_batches": bulk_batches, "batches": nb,
            "corrections": {"eligible_transfer_withdraw": elig, "applied": ncorr,
                            "measured_pct": round(100.0 * ncorr / elig, 4) if elig else 0.0,
                            "by_kind": {k: dist(v) for k, v in lags.items()}},
            "sha256": h.hexdigest(), "bytes": out.stat().st_size, "wall_s": round(time.time() - t_start, 1)}
    Path(str(out).replace(".eventlog.jsonl", ".eventlog.meta.json").replace(".jsonl.tmp", ".meta.json")
         ).write_text(json.dumps(meta, indent=1))
    print(json.dumps(meta, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
