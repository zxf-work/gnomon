#!/usr/bin/env python3
"""W3 untimed answer check on LadybugDB with a budget rule.

    w3_ladybug_check.py --target DB --params P.jsonl --settings current,asof[,window] --out R.jsonl
                        [--queries IS1,BI1] [--skip-after 2]

The cells of w3_snb_run.py run (same records, same canonical hashes, the W3_CAP_S check cap), with one budget rule:
after `--skip-after` cells of one (query, setting) ended at the cap, its remaining instances are not run and are
recorded `{"ok": false, "kind": "skipped", "err": "after N cells > cap"}` (labelled, never counted as answered).
A "buffer pool is full" error counts toward the same rule (it repeats on every instance of a query at a given pool).
After a cell that raised, the store is reopened (a fresh connection), so one failure cannot leak into the next cell.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import w3_snb_run as R  # noqa: E402
import w3_ladybug as L  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--target", required=True); ap.add_argument("--params", required=True)
    ap.add_argument("--settings", default="current,asof"); ap.add_argument("--queries")
    ap.add_argument("--out", required=True); ap.add_argument("--keep", type=int, default=1000)
    ap.add_argument("--skip-after", type=int, default=int(os.environ.get("W3_SKIP_AFTER", "2")))
    a = ap.parse_args()
    want = set(a.queries.split(",")) if a.queries else None
    sysobj = L.LadybugW3(a.target)
    out = open(a.out, "a")
    capped: dict = {}
    n = 0
    print(json.dumps({"settings": sysobj.settings(), "cap_s": R.CAP_S, "skip_after": a.skip_after}, default=str), flush=True)
    for line in open(a.params):
        inst = json.loads(line)
        if want and inst["qid"] not in want:
            continue
        for s in a.settings.split(","):
            key = (inst["qid"], s)
            rec = {"system": "ladybug", "qid": inst["qid"], "i": inst["i"], "setting": s}
            if capped.get(key, 0) >= a.skip_after:
                rec.update(ok=False, kind="skipped", err=f"not run: {capped[key]} earlier cells of this query and setting > cap ({R.CAP_S} s) or buffer pool full", ms=None)
                out.write(json.dumps(rec) + "\n"); out.flush(); n += 1
                continue
            t0 = time.perf_counter()
            try:
                cols, rows = sysobj.run(inst["qid"], inst, s)
                rows = R.canon_rows(rows)
                if inst["qid"] in R.COMPARE_LAST:
                    cols, rows = cols[-1:], [r[-1:] for r in rows]
                rec.update(ok=True, n=len(rows), hash=R.hrows(rows), sorted_hash=R.hrows(sorted(rows, key=json.dumps)),
                           cols=cols, rows=rows[: a.keep])
            except NotImplementedError as e:
                rec.update(ok=False, kind="not-expressible", err=str(e)[:300])
            except Exception as e:                                # noqa: BLE001
                msg = f"{type(e).__name__}: {e}"
                timeout = "nterrupt" in msg or "timeout" in msg.lower() or (time.perf_counter() - t0) > R.CAP_S
                rec.update(ok=False, kind="timeout" if timeout else "error", err=msg[:600])
                if timeout or "buffer pool is full" in msg:
                    capped[key] = capped.get(key, 0) + 1
                try:
                    sysobj.close()
                except Exception:                                 # noqa: BLE001
                    pass
                sysobj = L.LadybugW3(a.target)
            rec["ms"] = round((time.perf_counter() - t0) * 1e3, 3)
            out.write(json.dumps(rec, default=str) + "\n"); out.flush()
            n += 1
            print(f"{rec['qid']:<10} {rec['i']:<3} {s:<8} {'ok' if rec['ok'] else rec['kind']:<16} {rec['ms']}", flush=True)
    print(f"ladybug: {n} cells -> {a.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
