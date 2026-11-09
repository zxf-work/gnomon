#!/usr/bin/env python3
"""W4: the read-write transactions' parameter streams (TRW1-TRW3) from the
datagen's raw output.

The datagen's own transformation (`transformation/readwrites.sql`, duckdb
0.7.1) runs out of memory at SF10 on a 128 GB node, so the streams are derived
here from `raw/` with the same definitions:

* the transfer inserts after the bulk-load threshold T0 (transfer +
  loantransfer, `createTime > T0`), dependencyTime = the later endpoint
  creation; 20 % of them are read-write transfers, split half and half into
  TRW1 and TRW2; the rest stay plain inserts (TW12);
* the person-guarantee inserts after T0; 20 % are TRW3;
* startTime / endTime = the min / max createTime of each stream;
  truncation 500 / TIMESTAMP_DESCENDING; TRW2 amount threshold 1000, ratio
  threshold 1.0; TRW3 amount threshold 10000 (convert_data.py's constants).

**Deviation:** the datagen samples with DuckDB's `USING SAMPLE 20 PERCENT
(bernoulli, 23)`, whose draw depends on the DuckDB version and thread
schedule; here membership is md5(seed:createTime|fromId|toId) below 20 % (and
the TRW1/TRW2 split another md5 bit), reproducible on any version. The
streams have the datagen's columns. `trw-edges.csv` lists the read-write
transfers and guarantees, which the ingest study removes from the replayed
incremental stream (they are written by the transactions).

    python scripts/w4_trw_params.py --raw data/sf10 --out data/sf10/trw
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
from w4_finbench_log import NEVER_MS, T0_MS, csv  # noqa: E402,F401


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--raw", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--seed", type=int, default=23)
    ap.add_argument("--threads", type=int, default=16)
    a = ap.parse_args()
    a.out.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"SET threads={a.threads}")
    s = a.seed
    con.execute(f"""CREATE TABLE acc AS SELECT id, createTime FROM {csv(a.raw, 'account')}""")
    con.execute(f"""CREATE TABLE per AS SELECT id, createTime FROM {csv(a.raw, 'person')}""")
    tq = " UNION ALL ".join(
        f"""SELECT t.createTime, greatest(a1.createTime, a2.createTime) AS dependencyTime, t.fromId, t.toId,
                   t.amount, t.orderNum, t.comment, t.payType, t.goodsType
            FROM {csv(a.raw, d)} t JOIN acc a1 ON a1.id = t.fromId JOIN acc a2 ON a2.id = t.toId
            WHERE t.createTime > {T0_MS}""" for d in ("transfer", "loantransfer"))
    con.execute(f"""CREATE TABLE tr AS SELECT *, md5('{s}:' || createTime || '|' || fromId || '|' || toId) AS h
                    FROM ({tq})""")
    con.execute(f"""CREATE TABLE pg AS SELECT g.createTime, greatest(p1.createTime, p2.createTime) AS dependencyTime,
                    g.fromId, g.toId, g.relation, md5('{s}:' || g.createTime || '|' || g.fromId || '|' || g.toId) AS h
                    FROM {csv(a.raw, 'personGuarantee')} g JOIN per p1 ON p1.id = g.fromId JOIN per p2 ON p2.id = g.toId
                    WHERE g.createTime > {T0_MS}""")
    sel = "CAST(('0x' || substr(h, 1, 8)) AS UBIGINT) < 858993459"      # 20 % of 2^32
    half = "CAST(('0x' || substr(h, 9, 1)) AS UBIGINT) % 2"
    cols = "createTime, dependencyTime, fromId, toId, amount, orderNum, comment, payType, goodsType"
    rec = {}
    for name, where, extra in (
            ("AddAccountTransferAccountReadWrite1.csv", f"{sel} AND {half} = 0", ""),
            ("AddAccountTransferAccountReadWrite2.csv", f"{sel} AND {half} = 1",
             ", 500 AS truncation_limit, 'TIMESTAMP_DESCENDING' AS truncation_order, 1000 AS amount_threshold, "
             "1.0 AS ratio_threshold")):
        con.execute(f"""COPY (WITH x AS (SELECT * FROM tr WHERE {where})
            SELECT {cols}, (SELECT min(createTime) FROM x) AS startTime, (SELECT max(createTime) FROM x) AS endTime
                   {extra} FROM x ORDER BY createTime) TO '{a.out}/{name}' (DELIMITER '|', HEADER)""")
        rec[name] = con.execute(f"SELECT count(*) FROM tr WHERE {where}").fetchone()[0]
    con.execute(f"""COPY (WITH x AS (SELECT * FROM pg WHERE {sel})
        SELECT createTime, dependencyTime, fromId, toId, relation, (SELECT min(createTime) FROM x) AS startTime,
               (SELECT max(createTime) FROM x) AS endTime, 500 AS truncation_limit,
               'TIMESTAMP_DESCENDING' AS truncation_order, 10000 AS amount_threshold FROM x ORDER BY createTime)
        TO '{a.out}/AddPersonGuaranteePersonReadWrite3.csv' (DELIMITER '|', HEADER)""")
    rec["AddPersonGuaranteePersonReadWrite3.csv"] = con.execute(f"SELECT count(*) FROM pg WHERE {sel}").fetchone()[0]
    con.execute(f"""COPY (SELECT 'TRANSFER' AS rel, 'Account:' || fromId AS src, 'Account:' || toId AS dst, createTime
                    FROM tr WHERE {sel} UNION ALL SELECT 'GUARANTEE', 'Person:' || fromId, 'Person:' || toId, createTime
                    FROM pg WHERE {sel} ORDER BY createTime) TO '{a.out}/trw-edges.csv' (DELIMITER '|', HEADER)""")
    rec["transfer_inserts_after_t0"] = con.execute("SELECT count(*) FROM tr").fetchone()[0]
    rec["guarantee_inserts_after_t0"] = con.execute("SELECT count(*) FROM pg").fetchone()[0]
    rec.update(t0_ms=T0_MS, seed=s, sampling="md5 20 % (datagen: bernoulli 20 %, seed 23)")
    (a.out / "trw-params.json").write_text(json.dumps(rec, indent=1))
    print(json.dumps(rec))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
