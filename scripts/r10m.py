#!/usr/bin/env python3
"""R-10M.

A (system, query family) pair is not run at 10M if every instance of that family was censored (> 10 s)
for that system in the W2 1M timed run. The skipped cells are recorded as
"> 10 s (R-10M: all instances censored at 1M)". The rule applies to every system alike, Gnomon included.

A query family is a registry id with its curated instances (snap.hop2, paths.k, b.reach@T, ...). The 1M
timed records are the kit's block records `<1M results>/<1M point>/rot<r>/<letter>-<system>.json`, every
rotation; an instance counts as censored only when its cell is aborted at the ceiling (> 10 s, a client
alarm, or a server-side limit). A family with any answered instance, any error that is not the ceiling,
or no 1M record at all is run.

    r10m.py build --from <1M results dir> --point K1M[-dX] --system S --out SKIP.json

SKIP.json = {"rule": ..., "source": ..., "point_1m": ..., "system": ..., "skip": {query: {"instances": n,
"censored": n}}, "run": {query: why}}. `extcmp.py query` reads it from EXTCMP_R10M_SKIP.
"""
from __future__ import annotations

import argparse
import glob
import json
import re
import sys
from collections import defaultdict
from pathlib import Path

RULE = ("R-10M: a (system, query family) pair is not run at 10M if every instance of that family "
        "was censored (> 10 s) for that system in the W2 1M timed run")
LABEL = "> 10 s (R-10M: all instances censored at 1M)"
_CEIL = re.compile(r">\s?10(\.0)? s|client-side limit|statement timeout|canceling statement|Interrupted|INTERRUPT|"
                   r"timeout|TIMEOUT_EXCEEDED|TransactionTimedOut|execution timeout|ceiling", re.I)


def censored(cell: dict) -> bool:
    if cell.get("ok"):
        return False
    if cell.get("aborted") or cell.get("censored"):
        return True
    return bool(_CEIL.search(str(cell.get("error", ""))))


def build(results: Path, point: str, system: str) -> dict:
    per: dict[str, list[bool]] = defaultdict(list)
    files = sorted(glob.glob(str(results / point / "rot*" / f"*-{system}.json")))
    for f in files:
        try:
            doc = json.loads(Path(f).read_text())
        except Exception:                                        # noqa: BLE001
            continue
        for c in doc.get("results", []):
            if c.get("excluded"):
                continue
            per[c["query"]].append(censored(c))
    skip, run = {}, {}
    for q, cs in sorted(per.items()):
        if cs and all(cs):
            skip[q] = {"instances": len(cs), "censored": len(cs)}
        else:
            run[q] = f"{sum(cs)} of {len(cs)} cells censored at 1M"
    return {"rule": RULE, "label": LABEL, "source": str(results / point), "point_1m": point, "system": system,
            "files": files, "skip": skip, "run": run}


def main() -> int:
    ap = argparse.ArgumentParser()
    sp = ap.add_subparsers(dest="cmd", required=True)
    b = sp.add_parser("build")
    b.add_argument("--from", dest="src", required=True, type=Path)
    b.add_argument("--point", required=True)
    b.add_argument("--system", required=True)
    b.add_argument("--out", required=True, type=Path)
    a = ap.parse_args()
    d = build(a.src, a.point, a.system)
    if not d["files"]:
        print(f"R-10M: no 1M timed record for {a.system} at {a.src / a.point}: nothing skipped", file=sys.stderr)
    a.out.write_text(json.dumps(d, indent=1) + "\n")
    print(f"R-10M {a.system} {a.point}: skip {sorted(d['skip'])} (run {len(d['run'])})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
