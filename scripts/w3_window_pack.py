"""Adjudication pack for window-setting differences: per instance, the exact Gnomon calls (op + args JSON) and
Gnomon's rows, and the expanded DuckDB SQL text (with its session variables) and DuckDB's rows.

    w3_window_pack.py --store S --duckdb DB --params P --cases IC10:0,IC5:1,... --out DIR [--setting window]
"""
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import w3_snb_run as W  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--store", required=True); ap.add_argument("--duckdb", required=True)
    ap.add_argument("--params", required=True); ap.add_argument("--cases", required=True)
    ap.add_argument("--out", required=True); ap.add_argument("--setting", default="window")
    ap.add_argument("--capture-only", action="store_true", help="record Gnomon's calls without executing them")
    a = ap.parse_args()
    out = Path(a.out); out.mkdir(parents=True, exist_ok=True)
    files = {}

    def inst_of(q, i, f):
        if f not in files:
            files[f] = {(x["qid"], x["i"]): x for x in map(json.loads, open(f))}
        return files[f][(q, i)]
    g = W.make_system("gnomon", a.store)
    d = W.make_system("duckdb", a.duckdb)
    index = []
    for case in a.cases.split(","):
        case, _, pf = case.partition("@")
        q, i = case.split(":"); i = int(i)
        inst = inst_of(q, i, pf or a.params)
        tag = f"{q}-i{i}-{a.setting}" + (f"-{Path(pf).name}" if pf else "")
        g.capture, g.capture_only = [], a.capture_only
        try:
            gcols, grows = g.run(q, inst, a.setting)
        except Exception:                   # capture-only: the post-processing of an empty answer may fail
            if not a.capture_only:
                raise
            gcols, grows = [], []
        (out / f"{tag}.gnomon-calls.json").write_text(json.dumps(g.capture, indent=1, default=str) + "\n")
        (out / f"{tag}.gnomon-rows.json").write_text(json.dumps({"executed": False} if a.capture_only else
                                                              {"cols": gcols, "rows": W.canon_rows(grows)}, indent=1, default=str) + "\n")
        fam, path = W.CATALOG[q]
        sv = W.setting_values(inst, a.setting)
        text = W.bind_sql(W.qtext(q, path, a.setting), W.query_params(q, inst, a.setting))
        for x, y in W.DUCKDB_FIX.get(q, []) + W.SQL_FIX.get(q, []):
            text = text.replace(x, y)
        mode = "window" if a.setting == "window" else "instant"
        pre = "".join(f"SET VARIABLE w3_{k} = {int(v)}::BIGINT;\n" for k, v in sv.items()) + f"SET search_path = '{fam}_{mode},main';\n"
        (out / f"{tag}.duckdb.sql").write_text(pre + text + "\n")
        import time
        t0 = time.time()
        dcols, drows = d.run(q, inst, a.setting)
        dsec = round(time.time() - t0, 3)
        (out / f"{tag}.duckdb-rows.json").write_text(json.dumps({"cols": dcols, "rows": W.canon_rows(drows), "seconds": dsec}, indent=1, default=str) + "\n")
        gs, ds = set(map(json.dumps, W.canon_rows(grows))), set(map(json.dumps, W.canon_rows(drows)))
        index.append({"case": tag, "instance": inst, "gnomon_executed": not a.capture_only, "gnomon_rows": len(grows),
                      "duckdb_rows": len(drows), "duckdb_seconds": dsec,
                      "only_gnomon": sorted(gs - ds)[:20], "only_duckdb": sorted(ds - gs)[:20]})
        print(tag, len(grows), len(drows), len(gs - ds), len(ds - gs), flush=True)
    (out / "INDEX.json").write_text(json.dumps(index, indent=1) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
