#!/usr/bin/env python3
"""
    x_readcopy.py --gate <gate job results dir> --tag K100M-d0 --out <read dir> --known <known-refused.json>

The gate job's records are copied, never changed. In the copy, for each call Gnomon as shipped refused with a CostError (its estimated cost):
a system's "no native reference" exclusion is lifted when its answer-check cell answered inside 10 s, and becomes "> 10 s at answer check"
when the cell answered above 10 s or hit a limit; any other exclusion stays. known-refused.json lists exactly those calls (X_KNOWN_REFUSED of
the timed job: x_stopcheck.py lists them instead of stopping; any other Gnomon error or a new refusal stops the job). Printed: per refused
call, the others' answers at the gate and how many distinct projected hashes they have.
"""
from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path


def key(c):
    return f"{c['query']}#{c['instance']}" if c.get("instance") else c["query"]


def cells(d: Path, s: str) -> dict:
    out = {}
    for f in sorted(d.glob(f"verify-{s}-*.json")):
        for c in json.loads(f.read_text()).get("results", []):
            out[key(c)] = c
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--gate", type=Path, required=True); ap.add_argument("--tag", required=True)
    ap.add_argument("--out", type=Path, required=True); ap.add_argument("--known", type=Path, required=True)
    a = ap.parse_args()
    refused_all = set()
    for root in ("reg", "t32reg"):
        src, dst = a.gate / root / a.tag, a.out / root / a.tag
        if root == "t32reg" and not src.is_dir():      # s100m: no DuckDB (SQL) 32-thread root
            print(f"{root}: no records (not run in this stage)"); continue
        if dst.exists():
            sys.exit(f"{dst} exists: refusing to overwrite")
        shutil.copytree(src, dst)
        nat = cells(src, "native")
        refused = sorted(k for k, c in nat.items() if not c.get("ok") and "CostError: estimated cost" in str(c.get("error", "")))
        other_err = sorted(k for k, c in nat.items() if not c.get("ok") and k not in refused)
        refused_all |= set(refused)
        ex = json.loads((src / "excluded.json").read_text())
        for s, m in ex.items():
            sc = cells(src, s)
            for k in refused:
                if m.get(k) != "excluded: no native reference":
                    continue
                c = sc.get(k, {})
                if c.get("ok") and c.get("wall_ms", 1e18) <= 10_000:
                    del m[k]
                else:
                    m[k] = "excluded: > 10 s at answer check"
        (dst / "excluded.json").write_text(json.dumps(ex, indent=1, sort_keys=True) + "\n")
        print(f"{root}: Gnomon refused (CostError) {len(refused)}: {refused}; other Gnomon errors: {other_err}")
        for k in refused:
            ans = {s: cells(src, s).get(k, {}) for s in ("duckdbsql", "clickhouse", "postgres") if (src / f"verify-{s}-P.json").exists() or True}
            ok = {s: c for s, c in ans.items() if c.get("ok")}
            print(f"  {k}: answered by {sorted(ok)} walls {[round(c.get('wall_ms', 0)) for c in ok.values()]}; distinct projected hashes {len({c.get('proj_hash') for c in ok.values()})}")
    a.known.write_text(json.dumps({"source": f"gate {a.gate.name}, the k100m option-A decision applied to this stage's gate", "refused": sorted(refused_all)}) + "\n")
    print(f"known refused: {len(refused_all)} -> {a.known}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
