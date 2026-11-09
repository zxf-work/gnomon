"""Offline per-row classification of AeonG's asof differences, from the records alone (no AeonG run).

    w3_aeong_classify.py --hist HIST --params P --aeong aeong.jsonl --ref duckdb.jsonl --out classify.json

For each AeonG asof cell of IS1, IS3, IS5 and IS7 (and the T-LDBC IS1/3/5 forms) that differs from the reference,
the reference answer is recomputed in full by DuckDB (the result files keep only the first 20 rows), and each
T and was closed after T (T < tt_e < OPEN): a node or relationship that a later transaction SET, which AeonG's TT AS
read loses (C), or shows with its later tt_e so that the visibility predicate drops it (B). The prediction is the
every row AeonG kept is in the prediction (all of them when n <= 20). Cells that fit are labelled P14; the others are
reported."""
import argparse, collections, json
import duckdb

OPEN = 2 ** 62
NOW = 1356998400000000
ap = argparse.ArgumentParser()
for k in ("--hist", "--params", "--aeong", "--ref", "--out", "--hist-db"):
    ap.add_argument(k, required=True)
a = ap.parse_args()
con = duckdb.connect()
for t in ("Person_h", "Message_h", "Person_knows_Person_h", "Place_h"):
    con.execute(f"CREATE VIEW {t} AS SELECT * FROM read_parquet('{a.hist}/{t}.parquet')")
inst = {(x["qid"], x["i"]): x for x in map(json.loads, open(a.params))}
L = lambda f: {(x["qid"], x["i"], x["setting"]): x for x in map(json.loads, open(f))}
ae, ref = L(a.aeong), L(a.ref)


def versions(table, idcol, ids, T):
    """the versions of these entities visible at (T, NOW), with whether each was closed after T"""
    if not ids:
        return []
    rows = con.execute(f"SELECT {idcol}, tt_s, tt_e FROM {table} WHERE {idcol} IN ({','.join(str(int(i)) for i in ids)}) "
                       f"AND tt_s <= {T} AND {T} < tt_e AND vt_s <= {NOW} AND {NOW} < vt_e").fetchall()
    return [{"table": table, "id": r[0], "tt_e": r[2], "closed_after_T": r[2] < OPEN} for r in rows]


def knows(p, q, T):
    rows = con.execute("SELECT Person1Id, Person2Id, tt_s, tt_e FROM Person_knows_Person_h WHERE "
                       f"((Person1Id = {int(p)} AND Person2Id = {int(q)}) OR (Person1Id = {int(q)} AND Person2Id = {int(p)})) "
                       f"AND tt_s <= {T} AND {T} < tt_e AND vt_s <= {NOW} AND {NOW} < vt_e").fetchall()
    return [{"table": "Person_knows_Person_h", "id": [r[0], r[1]], "tt_e": r[3], "closed_after_T": r[3] < OPEN} for r in rows]


def involved(q, x, row):
    T, P = x["T"], x["params"]
    base = q.replace("TLDBC-", "")
    if base == "IS1":
        pv = versions("Person_h", "id", [P["personId"][1]], T)
        city = con.execute(f"SELECT LocationCityId FROM Person_h WHERE id = {int(P['personId'][1])} AND tt_s <= {T} AND {T} < tt_e "
                           f"AND vt_s <= {NOW} AND {NOW} < vt_e").fetchall()
        return pv + versions("Place_h", "id", [c[0] for c in city], T)
    if base == "IS3":
        return versions("Person_h", "id", [P["personId"][1], row[0]], T) + knows(P["personId"][1], row[0], T)
    if base == "IS5":
        return versions("Message_h", "MessageId", [P["messageId"][1]], T) + versions("Person_h", "id", [row[0]], T)
    if base == "IS7":
        M = P["messageId"][1]
        au = [r[0] for r in con.execute(f"SELECT DISTINCT CreatorPersonId FROM Message_h WHERE MessageId = {int(M)} AND tt_s <= {T} AND {T} < tt_e").fetchall()]
        v = versions("Message_h", "MessageId", [M, row[0]], T) + versions("Person_h", "id", [row[3]] + au, T)
        for x_ in au:
            v += knows(x_, row[3], T)
        return v
    return None


import sys
sys.path.insert(0, "scripts")
import w3_snb_run as R
D = R.DuckDB(a.hist_db)
out, summary = [], collections.Counter()
for k, x in sorted(ae.items()):
    q, i, s = k
    if s != "asof" or q.replace("TLDBC-", "") not in ("IS1", "IS3", "IS5", "IS7"):
        continue
    r = ref.get(k)
    if not (x.get("ok") and r and r.get("ok")) or x["sorted_hash"] == r["sorted_hash"]:
        continue
    _, full = D.run(q, inst[(q, i)], s)
    full = R.canon_rows(full)
    sig = []
    for row in full:
        inv = involved(q, inst[(q, i)], row)
        sig.append([e for e in inv if e["closed_after_T"]])
    pred = [row for row, s_ in zip(full, sig) if not s_]
    kept = x["rows"]
    pc = collections.Counter(json.dumps(z) for z in pred); kc = collections.Counter(json.dumps(z) for z in kept)
    subset = not (kc - pc)
    exact = (pc == kc) if x["n"] <= 20 else None
    fits = (len(pred) == x["n"]) and subset and (exact is not False)
    summary[(q, "P14 (B/C signature)" if fits else "DOES NOT FIT")] += 1
    out.append({"qid": q, "i": i, "T": inst[(q, i)]["T"], "ref_n": len(full), "aeong_n": x["n"], "predicted_n": len(pred),
                "aeong_rows_in_prediction": subset, "exact": exact, "fits": fits,
                "lost_rows": [{"row": row[:6], "closed_after_T": s_} for row, s_ in zip(full, sig) if s_]})
json.dump({"cells": out, "summary": {f"{a_} | {b_}": c for (a_, b_), c in sorted(summary.items())}}, open(a.out, "w"), indent=1, default=str)
for o in out:
    print(o["qid"], o["i"], "ref", o["ref_n"], "aeong", o["aeong_n"], "predicted", o["predicted_n"], "subset", o["aeong_rows_in_prediction"],
          "exact", o["exact"], "FITS" if o["fits"] else "DOES NOT FIT")
for (q_, lab), c in sorted(summary.items()):
    print(q_, lab, c, "cells")
