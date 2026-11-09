#!/usr/bin/env python3
"""Baseline-vs-baseline answer check over the kit's verify records.

    ps_crosscheck.py --dir RESULTS/<point> --base duckdbsql --ceiling 10 --out cross.json

The kit's gate (`extcmp_p1.py gate`) compares every system with Gnomon's native cells. Here no Gnomon cell exists
(no Gnomon-side check before the engine pin), so every system is compared with every other one, with the kit's
own cell identity (`extcmp.cell_key`) and verdict rule (`extcmp.verdict`: exact hash, else the projected hash
without vid/tt_s/tt_e, else DIFFERS), and the gate's labels for a cell past the ceiling. Output:
- `vs_base`: per system, verdict counts against the base system (the gate's table with the base in native's
  place);
- `pairs`: per pair of systems, the instances both answered, how many agree, and the first disagreements;
- `consensus`: per instance answered by two or more systems, whether all answers agree; the instances where they
  do not, with each system's hash.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import extcmp as X  # noqa: E402
from extcmp_p1 import _records, is_server_timeout  # noqa: E402


def label(cell: dict, ceiling: float) -> str:
    if not cell.get("ok"):
        det = str(cell.get("error", ""))
        if is_server_timeout(det) or det.startswith("timeout"):
            return f"> {ceiling:g} s at answer check"
        rows: list = []
        X._verdict_row(rows, cell, None, cell["query"], {})
        return rows[0]["verdict"]
    return "ok"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dir", type=Path, required=True)
    ap.add_argument("--base", default="duckdbsql")
    ap.add_argument("--ceiling", type=float, default=10.0)
    ap.add_argument("--out", type=Path, required=True)
    a = ap.parse_args()
    cells = _records(a.dir, "verify-")
    # a verify window cut by EXTCMP_WINDOW_TIMEOUT_S leaves no record, only its per-cell log
    # (<record>.tmp.cells.jsonl, extcmp.py): those cells are added, and the system is listed as partial
    partial: dict = {}
    for f in sorted(a.dir.glob("verify-*.cells.jsonl")):
        full = a.dir / f.name.replace(".tmp.cells.jsonl", "").replace(".cells.jsonl", "")
        if full.exists():
            continue
        got = [json.loads(l) for l in open(f) if l.strip()]
        for c in got:
            cells.setdefault(c["system"], []).append(dict(c, _record=f.name))
        if got:
            partial.setdefault(got[0]["system"], []).append({"log": f.name, "cells": len(got)})
    systems = sorted(cells)
    by = {s: {X.cell_key(c): c for c in cs} for s, cs in cells.items()}
    vs_base: dict = {}
    if a.base in cells:
        for s in systems:
            if s == a.base:
                continue
            cnt: dict = {}
            for r in X.verdict({"results": cells[a.base]}, {"system": s, "compare": "exact", "results": cells[s]}):
                v = r["verdict"]
                k = r["query"] if r.get("instance") is None else f"{r['query']}#{r['instance']}"
                c = by[s].get(k, {})
                if v in ("error", "refused") and (is_server_timeout(r.get("detail", "")) or r.get("detail", "").startswith("timeout")):
                    v = f"> {a.ceiling:g} s at answer check"
                elif v == "equivalent" and c.get("wall_ms", 0) > a.ceiling * 1e3:
                    v = f"equivalent, > {a.ceiling:g} s at answer check"
                elif v == "no native reference":
                    v = f"no {a.base} answer ({label(by[a.base].get(k, {}), a.ceiling)})" if k in by[a.base] else f"no {a.base} cell"
                cnt[v] = cnt.get(v, 0) + 1
            vs_base[s] = dict(sorted(cnt.items()))
    pairs: dict = {}
    for i, s in enumerate(systems):
        for t in systems[i + 1:]:
            both = agree = 0
            dis = []
            for k, c in by[s].items():
                d = by[t].get(k)
                if not (c.get("ok") and d and d.get("ok")):
                    continue
                both += 1
                if c["hash"] == d["hash"] or c.get("proj_hash") == d.get("proj_hash"):
                    agree += 1
                elif len(dis) < 20:
                    dis.append(k)
            pairs[f"{s}|{t}"] = {"both_answered": both, "agree": agree, "differ": both - agree, "first_differing": dis}
    keys = sorted({k for s in systems for k in by[s]})
    cons = {"instances": len(keys), "answered_by_2plus": 0, "all_agree": 0, "disagree": []}
    cover: dict = {s: {"answered": 0, "cells": len(by[s])} for s in systems}
    for k in keys:
        ans = {s: by[s][k] for s in systems if k in by[s] and by[s][k].get("ok")}
        for s in ans:
            cover[s]["answered"] += 1
        if len(ans) < 2:
            continue
        cons["answered_by_2plus"] += 1
        hs = {s: (c["hash"], c.get("proj_hash")) for s, c in ans.items()}
        first = next(iter(hs.values()))
        if all(h[0] == first[0] or h[1] == first[1] for h in hs.values()):
            cons["all_agree"] += 1
        else:
            cons["disagree"].append({"cell": k, "hashes": {s: h[0] for s, h in hs.items()}})
    labels: dict = {}
    for s in systems:
        for c in cells[s]:
            v = label(c, a.ceiling)
            if v == "ok" and c.get("wall_ms", 0) > a.ceiling * 1e3:
                v = f"ok, > {a.ceiling:g} s"
            labels.setdefault(s, {}).setdefault(v, 0)
            labels[s][v] += 1
    out = {"dir": str(a.dir), "base": a.base, "ceiling_s": a.ceiling, "systems": systems, "partial": partial,
           "coverage": cover,
           "cell_labels": labels, "vs_base": vs_base, "pairs": pairs, "consensus": cons}
    a.out.write_text(json.dumps(out, indent=1) + "\n")
    print(f"{a.dir.name}: systems {systems}")
    for s in systems:
        print(f"  {s}: {cover[s]['answered']}/{cover[s]['cells']} answered; {labels.get(s)}"
              + (f" PARTIAL (window cut: {partial[s]})" if s in partial else ""))
    for s, v in vs_base.items():
        print(f"  vs {a.base}: {s}: {v}")
    print(f"  consensus: {cons['answered_by_2plus']} instances answered by 2+ systems, {cons['all_agree']} agree, "
          f"{len(cons['disagree'])} disagree {[d['cell'] for d in cons['disagree'][:10]]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
