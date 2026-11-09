#!/usr/bin/env python3
"""Thread-matched control: the answer gate. Every thread configuration's answers must equal the reference answers.

    thr_gate.py --res <workload results dir> --params <params dir of the workload> --tags K10M[,K10M-d1] \
                --cfgs "g32 d64 ..." --ref K10M=<W1/W2 gate record dir> [--ref ...] --md <report.md>

Reads <res>/cfg-<c>/<tag>/verify-<system>[-<family>].json (the answer-check records of the gate job) and the reference records
of the W1 / W2 stages' gates (Gnomon at 32 threads, DuckDB (SQL) at 64 threads, PostgreSQL at 4 workers, whose answers those
stages' gates validated against each other). Per cell (instance) of the workload's parameter file:
  equal            the answer hash equals the same system's reference hash (exact), or else the Gnomon-t32 reference
                   (exact hash or the kit's projected hash: the kit's own "equivalent" rule)
  DIFFERS          the cell answered and equals neither: FAIL
  over ceiling     the call took > 10 s or hit the server limit / client alarm at the answer check: not timed, labelled
  refused          Gnomon's admission refused it (estimated > 10 s), or any system refused it: not timed, labelled
  error            any other exception: not timed; FAIL for Gnomon, and for another system when its reference cell was ok
  not expressible  the system has no form of the id: recorded, nothing to time
  unreached        the answer-check window limit cut before this instance: not compared, not timed, counted
  no reference     answered, but neither reference cell answered: not timed (cannot be validated)
Writes <res>/cfg-<c>/<tag>/excluded.json in the kit's format ({system: {cell key: "excluded: <why>"}}), which the timed job
passes as EXTCMP_EXCLUDE, the per-cell statuses to <res>/gate-cells.json, and the report. Also FAIL: a missing record, a
missing cell, and a configuration that did not reach the system (thr_check). Exit 0 only without a FAIL.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import extcmp as X          # the pinned kit (PYTHONPATH = $REPO:$REPO/scripts)
import extcmp_p1 as P1
import thr_check

SYS = {"g": "native", "d": "duckdbsql", "p": "postgres"}
CEIL_MS = 10_000.0
FAILS: list[str] = []


def records(d: Path, system: str) -> tuple[dict, list[dict]]:
    pat = re.compile(rf"verify-{system}(?:-[A-Z])?\.json")
    cells, recs = {}, []
    for f in sorted(d.glob("verify-*.json")):
        if pat.fullmatch(f.name):
            r = json.loads(f.read_text())
            recs.append(dict(r, _file=f.name))
            for c in r.get("results", []):
                cells[X.cell_key(c)] = c
    return cells, recs


def classify(c: dict | None, ref_same: dict | None, ref_nat: dict | None) -> tuple[str, str | None]:
    """(status, exclusion label or None)."""
    if c is None:
        return "missing", "excluded: no answer-check cell"
    err = str(c.get("error", ""))
    if not c.get("ok"):
        if c.get("unreached"):
            return "unreached", "excluded: unreached (answer-check window limit)"
        if err.startswith(("unsupported", "NotImplemented", "not written")):
            return "not expressible", None
        if err.startswith("timeout") or P1.is_server_timeout(err):
            return "over ceiling", "excluded: > 10 s at answer check"
        if c.get("refused") or "E_COST" in err or "guardrail" in err.lower():
            return "refused", "excluded: refused (estimated > 10 s)"
        return "error", "excluded: error"
    h = c.get("hash")
    rs, rn = (ref_same if ref_same and ref_same.get("ok") else None), (ref_nat if ref_nat and ref_nat.get("ok") else None)
    if rs is not None and rs.get("hash") == h:
        st = "equal"
    elif rn is not None and (rn.get("hash") == h or rn.get("proj_hash") == c.get("proj_hash")):
        st = "equal"
    elif rs is None and rn is None:
        return "no reference", "excluded: no reference answer"
    else:
        return "DIFFERS", "excluded: DIFFERS"
    if c.get("wall_ms", 0) > CEIL_MS:
        return "over ceiling", "excluded: > 10 s at answer check"
    return st, None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--res", type=Path, required=True)
    ap.add_argument("--params", type=Path, required=True)
    ap.add_argument("--tags", required=True)
    ap.add_argument("--cfgs", required=True)
    ap.add_argument("--ref", action="append", default=[], help="TAG=<reference record dir>")
    ap.add_argument("--md", type=Path, required=True)
    a = ap.parse_args()
    refdirs = dict(r.split("=", 1) for r in a.ref)
    cfgs, tags = a.cfgs.split(), a.tags.split(",")
    lines = [f"# Thread-matched control: answer gate of {a.res}", ""]
    allcells: dict = {}
    for tag in tags:
        params = json.loads((a.params / f"params-{tag}.json").read_text())
        keys = [f"{i['query']}#{i['instance']}" for i in params["instances"]]
        ref = {s: records(Path(refdirs[tag]), s)[0] for s in SYS.values()}
        lines += [f"## {tag}: {len(keys)} instances", "",
                  "| config | equal | over ceiling | refused | error | not expressible | unreached | no reference | DIFFERS | missing | override |",
                  "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|"]
        for c in cfgs:
            sysname = SYS[c[0]]
            d = a.res / f"cfg-{c}" / tag
            cells, recs = records(d, sysname)
            cnt, excl, per = Counter(), {}, {}
            if not recs:
                FAILS.append(f"{tag} {c}: no answer-check record in {d}")
            applied = "ok"
            for r in recs:
                if r.get("partial"):
                    applied = "partial"
                    continue
                why = thr_check.problem(r, c)
                if why:
                    applied = "NOT APPLIED"
                    FAILS.append(f"{tag} {c}: {r['_file']}: {why}")
            for k in keys:
                st, label = classify(cells.get(k), ref[sysname].get(k), ref["native"].get(k))
                cnt[st] += 1
                per[k] = {"status": st, "wall_ms": (cells.get(k) or {}).get("wall_ms"), "label": label}
                if label:
                    excl[k] = label
                if st == "DIFFERS":
                    FAILS.append(f"{tag} {c}: {k} DIFFERS from the reference (hash {cells[k].get('hash')}; "
                                 f"same-system reference {ref[sysname].get(k, {}).get('hash')}, Gnomon t32 {ref['native'].get(k, {}).get('hash')})")
                elif st == "missing":
                    FAILS.append(f"{tag} {c}: {k} has no answer-check cell")
                elif st == "error":
                    rs = ref[sysname].get(k)
                    if c[0] == "g" or (rs and rs.get("ok")):
                        FAILS.append(f"{tag} {c}: {k} unexpected error: {str(cells[k].get('error'))[:200]}")
            (d).mkdir(parents=True, exist_ok=True)
            (d / "excluded.json").write_text(json.dumps({sysname: excl}, indent=1, sort_keys=True) + "\n")
            (d / "verdicts.txt").write_text("\n".join(f"{k}: {v['status']}" + (f" ({v['label']})" if v["label"] else "")
                                                       for k, v in per.items()) + "\n")
            allcells.setdefault(c, {})[tag] = per
            lines.append(f"| {c} | " + " | ".join(str(cnt[s]) for s in (
                "equal", "over ceiling", "refused", "error", "not expressible", "unreached", "no reference", "DIFFERS", "missing")) + f" | {applied} |")
        lines.append("")
    (a.res / "gate-cells.json").write_text(json.dumps(allcells, indent=1, sort_keys=True) + "\n")
    lines += ["## Result", "", "FAIL" if FAILS else "PASS: every answered cell equals its reference, every configuration reached its system", ""]
    lines += [f"- {f}" for f in FAILS]
    a.md.parent.mkdir(parents=True, exist_ok=True)
    a.md.write_text("\n".join(lines) + "\n")
    print("\n".join(lines))
    return 1 if FAILS else 0


if __name__ == "__main__":
    sys.exit(main())
