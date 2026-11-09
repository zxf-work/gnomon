"""W4 transfer pack: exact Gnomon call arguments for one instance of each named query, with the
expected answer (canonical rows) from DuckDB (SQL) over the exported version rows."""
import json, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import duckdb
import w4_queries as Q
exp, pfile, out, queries = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4].split(",")
doc = json.load(open(pfile))
con = duckdb.connect()
con.execute(f"CREATE VIEW node_v AS SELECT * FROM '{exp}/node_v.parquet'")
con.execute(f"CREATE VIEW edge_v AS SELECT * FROM '{exp}/edge_v.parquet'")
res = []
for q in queries:
    i = next(x for x in doc["instances"] if x["query"] == q and x["belief"] == "current" and x["window"] == "30d")
    rows = Q.canonical(q, Q.sql_rows(q, con.execute(Q.sql(q, i["params"])).fetchall()))
    res.append({"query": q, "instance": i["instance"], "stratum": i["stratum"], "params": i["params"],
                "gnomon_calls": [{"operator": op, "args": args} for op, args in Q.gnomon(q, i["params"])],
                "note": "match calls take limit 10000; paths needs algebra.ensure_all_registered(); "
                        "TCR5 untruncated and differs by G7",
                "expected_duckdbsql": {"rows": rows, "n": len(rows), "sql": Q.sql(q, i["params"])}})
Path(out).write_text(json.dumps(res, indent=1, default=str))
print(out, [(r["query"], r["instance"], r["expected_duckdbsql"]["n"]) for r in res])
