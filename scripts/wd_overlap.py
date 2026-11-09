"""W5: split the TGB 2.0 triples the frozen log does not reproduce, by cause.

    wd_overlap.py --stagea DIR --log LOG --tgb EDGELIST.csv --out JSON

existed(q) = item q has at least one revision in the dump (stage-A rev records).
Classes of a TGB (head, rel, tail) triple, first match wins:
  head_after_dump   the subject was created after the dump (no revision of it in the dump)
  tail_after_dump   the object was created after the dump
  in_final          believed at the end of the history
  removed_by_2019   believed at some time, no longer at the end of 2018
  not_in_history    both items existed, the statement was never a temporal edge in the history
                    (added 2019-2024, or not temporal / object not item-valued then)
"""
import argparse, collections, json, sys
from pathlib import Path
import duckdb
sys.path.insert(0, str(Path(__file__).resolve().parent))
from wd_match import final_edges

ap = argparse.ArgumentParser()
for k in ("--stagea", "--log", "--tgb", "--out"):
    ap.add_argument(k, required=True)
a = ap.parse_args()
con = duckdb.connect()
existed = {r[0] for r in con.execute(
    f"SELECT DISTINCT q FROM read_ndjson('{a.stagea}/*.jsonl.gz', columns={{'t':'VARCHAR','q':'BIGINT'}}) WHERE t='rev'").fetchall()}
cur = final_edges(Path(a.log))
final = {(int(s), r, int(d)) for (s, d, r, _) in cur}
ever = set()
with open(a.log) as f:
    f.readline()
    for line in f:
        for op in json.loads(line)["ops"]:
            for e in op.get("events", []) + ([op] if op["op"] == "assert_edge" else []):
                ever.add((int(e["src"]), e["rel_type"], int(e["dst"])))
tri = collections.Counter()
with open(a.tgb) as f:
    next(f)
    for line in f:
        _, h, t, r = line.rstrip("\n").split(",")
        tri[(int(h[1:]), r, int(t[1:]))] += 1
cls = collections.Counter(); quad = collections.Counter()
for (h, r, t), n in tri.items():
    k = ("head_after_dump" if h not in existed else "tail_after_dump" if t not in existed else
         "in_final" if (h, r, t) in final else "removed_by_2019" if (h, r, t) in ever else "not_in_history")
    cls[k] += 1; quad[k] += n
out = {"existed_items": len(existed), "tgb_triples": len(tri), "tgb_quadruples": sum(tri.values()),
       "triples": dict(cls), "quadruples": dict(quad),
       "triples_pct": {k: round(100 * v / len(tri), 2) for k, v in cls.items()},
       "quadruples_pct": {k: round(100 * v / sum(tri.values()), 2) for k, v in quad.items()}}
Path(a.out).write_text(json.dumps(out, indent=1) + "\n"); print(json.dumps(out, indent=1))
