#!/usr/bin/env python3
"""rap_fullsets.py --cause cause-<wl>-<tag>.json --wl W --tag T --native STORE --root EXTCMP_ROOT --out OUT: untimed, on the cause check's cells: the whole reachable set with arrivals from DuckDB (SQL) and PostgreSQL, their own reachability code called with a limit above the node count, against Gnomon's operator read page by page. Counts: nodes in each direction, arrivals that differ, rows in the same order. The answer check's hash covers only the first page and the totals, so this is the check of everything past the page, on these cells only."""
import argparse
import hashlib
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import extcmp as X  # noqa: E402
import extcmp_duckdbsql as D  # noqa: E402
import pg_queries as PQ  # noqa: E402
from rap_cause import full_gnomon  # noqa: E402

LIM = 10_000_000


def as_map(rows):
    return {r["uid"]: int(r["earliest_arrival"]) for r in rows}


def order_key(rows):
    return hashlib.sha256(json.dumps([[r["earliest_arrival"], r["uid"]] for r in rows]).encode()).hexdigest()[:24]


def main():
    ap = argparse.ArgumentParser()
    for k in ("cause", "wl", "tag", "native", "root", "out"):
        ap.add_argument("--" + k, required=True)
    a = ap.parse_args()
    cells = json.loads(Path(a.cause).read_text())
    st = X.open_existing_store(Path(a.native), "native")
    X.ensure_all_registered()
    duck = X._system_class("duckdbsql")(Path(a.native), Path(a.root), a.tag)
    duck.open()
    pg = X._system_class("postgres")(Path(a.native), Path(a.root), a.tag)
    pg.open()
    out = []
    for c in cells:
        src, win = c["src"], c["window"]
        t0 = time.time()
        G, tot = full_gnomon(st.adapter, src, win)
        gord = sorted(G.items(), key=lambda x: (x[1], x[0]))
        g_key = hashlib.sha256(json.dumps([[v, u] for u, v in gord]).encode()).hexdigest()[:24]
        rec = {"key": c["key"], "query": c["query"], "stratum": c.get("stratum"), "src": src, "window": win, "gnomon_n": len(G), "gnomon_total": tot, "gnomon_s": round(time.time() - t0, 2), "systems": {}}
        for name, fn, h in (("duckdbsql", D.temporal_reachability, duck.con), ("postgres", PQ.temporal_reachability, pg.handle)):
            t0 = time.time()
            try:
                p = fn(h, src=src, window=dict(win), limit=LIM)
            except Exception as e:  # noqa: BLE001
                rec["systems"][name] = {"error": f"{type(e).__name__}: {e}"[:300], "s": round(time.time() - t0, 2)}
                continue
            rows = p["rows"]
            M = as_map(rows)
            rec["systems"][name] = {"n": len(M), "rows_total": p.get("rows_total"), "truncated": p.get("truncated"), "only_system": len(set(M) - set(G)), "only_gnomon": len(set(G) - set(M)), "arrival_differs": sum(1 for u in set(M) & set(G) if M[u] != G[u]),
                                    "sets_equal": M == G, "same_order_of_rows": order_key(rows) == g_key, "s": round(time.time() - t0, 2)}
        out.append(rec)
        print(json.dumps({k: rec[k] for k in ("key", "gnomon_n", "systems")}), flush=True)
    Path(a.out).write_text(json.dumps(out, indent=1, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
