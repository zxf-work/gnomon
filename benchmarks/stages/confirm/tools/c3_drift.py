#!/usr/bin/env python3
"""C3 confirmation: the DuckDB (SQL) drift sample.

records stand; a 10 % DuckDB (SQL) drift sample is re-timed in the same jobs. This tool picks the sample before the run,
writes the exclusion files the timed jobs read, and scores the sample after the run.

    c3_drift.py sample --stage W1|W2|W3 --old DIR [DIR ...] --out-dir OUT
    c3_drift.py rule

THE RULE (also printed by `rule`):
  universe  every DuckDB (SQL) cell at its thread setting (64 threads; the 32-thread runs are not sampled) that has
            an ok timed record in the recorded stage results;
            a cell is (point, query, instance) for W1/W2 (W1 also has the result root, sp or p1) and
            (point, query, setting, instance) for W3;
  stratum   the input size of the cell: its point (W1/W2: K1M, K10M, K1M-d0 ... K10M-d10, each size and density point of
  key       "<salt>|<stage>|<point>|<query>|<instance key>", instance key = "<root>/<point>/<instance>",
            "<point>/<instance>", "<point>/<setting>/<instance>"; salt = SALT below;
  pick      within each point the cells are ordered by md5(key) (hex, ascending) and the first max(2, ceil(0.10 x N))
            are the sample (all of them if N < 2), N = the number of that point's cells in the universe.
            Deterministic, independent of file order, and fixed before the run.
  score     per sampled cell: old = median of every recorded timed call (all rotations), new = the same over the
            confirmation run; ratio = new / old. A cell censored or not ok on either side has no ratio (counted).
            The stage statistic is the median of the ratios. It must lie in [0.9, 1.1]; outside, the stage is FLAGGED
            (reported beside every Gnomon ratio of that stage; it does not stop or void the stage).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import statistics
import sys
from pathlib import Path

SALT = "gnomon-c3-drift-2026-11-03"
FRACTION = 0.10
MIN_PER_SIZE = 2
BAND = (0.9, 1.1)
DUCK = {"W1": "duckdbsql", "W2": "duckdbsql", "W3": "duckdb64"}
LABEL = "excluded: not in the C3 drift sample"


def rule_text() -> str:
    return __doc__.split("THE RULE (also printed by `rule`):", 1)[1].strip()


def md5(s: str) -> str:
    return hashlib.md5(s.encode()).hexdigest()


def kit_key(c: dict) -> str:
    """The kit's per-cell exclusion key (scripts/extcmp.py): query#instance, or the query alone."""
    return f"{c['query']}#{c['instance']}" if c.get("instance") not in (None, "") else c["query"]


def calls_of(c: dict) -> list[float]:
    return [float(x) for x in (c.get("timings_ms") or [])]


# ---------------------------------------------------------------- universe
def w12_cells(stage: str, dirs: list[Path]) -> dict:
    """{cell id: {meta, calls}} of DuckDB (SQL) 64-thread timed cells in W1/W2 timed result dirs.
    Layout: <job dir>/<root>/<point>/rot<r>/<fam>-duckdbsql.json; roots p1 and sp; t32* roots are skipped."""
    out: dict = {}
    for d in dirs:
        for rec in sorted(d.glob("*/*/rot[0-9]*/*-duckdbsql.json")):
            point, root = rec.parent.parent.name, rec.parent.parent.parent.name
            if root.startswith("t32"):
                continue
            fam = rec.stem.split("-")[0]
            for c in json.loads(rec.read_text()).get("results", []):
                inst = c.get("instance") or ""
                ik = f"{root}/{point}/{inst}" if stage == "W1" else f"{point}/{inst}"
                cid = f"{c['query']}|{ik}"
                e = out.setdefault(cid, {"stage": stage, "root": root, "point": point, "family": fam,
                                         "query": c["query"], "instance": inst, "inst_key": ik,
                                         "kit_key": kit_key(c), "calls": [], "ok": True, "seen": 0})
                e["seen"] += 1
                if c.get("ok") and calls_of(c):
                    e["calls"] += calls_of(c)
                else:
                    e["ok"] = False
    return out


def w3_cells(dirs: list[Path]) -> dict:
    """W3: <job dir>/rot1/<block>-duckdb64.json (the point is read from the record's label 'w3-final <PT> rot..').
    Rotation 1 only: the W3 result is rotation 1."""
    out: dict = {}
    for d in dirs:
        for rec in sorted(d.glob("rot1/*-duckdb64.json")):
            doc = json.loads(rec.read_text())
            pt = str(doc.get("label", "")).split()[1] if len(str(doc.get("label", "")).split()) > 1 else "?"
            for c in doc.get("results", []):
                ik = f"{pt}/{c['setting']}/{c['instance']}"
                cid = f"{c['query']}|{ik}"
                e = out.setdefault(cid, {"stage": "W3", "point": pt, "query": c["query"], "setting": c["setting"],
                                         "instance": c["instance"], "inst_key": ik, "calls": [], "ok": True, "seen": 0,
                                         "block": rec.stem.split("-")[0]})
                e["seen"] += 1
                if c.get("ok") and calls_of(c):
                    e["calls"] += calls_of(c)
                else:
                    e["ok"] = False
    return out


def universe(stage: str, dirs: list[Path]) -> dict:
    return w3_cells(dirs) if stage == "W3" else w12_cells(stage, dirs)


def key_of(stage: str, e: dict) -> str:
    return f"{SALT}|{stage}|{e['point']}|{e['query']}|{e['inst_key']}"


def pick(stage: str, cells: dict) -> list[str]:
    """The sample, per point (input size): ok cells ordered by md5(salt|stage|point|query|instance key), the first
    max(2, ceil(10 %)) of each point."""
    by: dict = {}
    for cid, e in cells.items():
        if e["ok"] and e["calls"]:
            by.setdefault(e["point"], []).append(cid)
    out: list[str] = []
    for pt in sorted(by):
        ids = sorted(by[pt], key=lambda cid: (md5(key_of(stage, cells[cid])), cid))
        out += ids[:min(len(ids), max(MIN_PER_SIZE, math.ceil(FRACTION * len(ids))))]
    return out


# ---------------------------------------------------------------- sample
def cmd_sample(a) -> int:
    cells = universe(a.stage, a.old)
    if not cells:
        print(f"no DuckDB (SQL) timed records under {a.old}")
        return 2
    chosen = pick(a.stage, cells)
    a.out_dir.mkdir(parents=True, exist_ok=True)
    doc = {"format": "c3-drift-sample/2", "stage": a.stage, "salt": SALT, "fraction": FRACTION,
           "min_per_size": MIN_PER_SIZE, "stratum": "point (input size)", "band": BAND,
           "rule": rule_text(), "old": [str(d) for d in a.old], "universe": sum(1 for e in cells.values() if e["ok"]),
           "cells_seen": len(cells), "sample": []}
    uni: dict = {}
    for e in cells.values():
        if e["ok"] and e["calls"]:
            uni[e["point"]] = uni.get(e["point"], 0) + 1
    doc["universe_per_point"] = dict(sorted(uni.items()))
    for cid in chosen:
        e = cells[cid]
        doc["sample"].append({k: v for k, v in e.items() if k not in ("calls", "seen", "ok")} |
                             {"id": cid, "old_median_ms": statistics.median(e["calls"]), "old_calls": len(e["calls"])})
    (a.out_dir / f"drift-{a.stage}.json").write_text(json.dumps(doc, indent=1) + "\n")
    files = [f"drift-{a.stage}.json"]
    keep = set(chosen)
    if a.stage == "W3":
        # one file per point: the timed job's duckdb exclusion list (query|instance|setting) for every non-sampled cell
        by: dict = {}
        for cid, e in cells.items():
            if cid not in keep:
                by.setdefault(e["point"], []).append(f"{e['query']}|{e['instance']}|{e['setting']}")
        # merged into the recorded gate's exclusions of that point (--gate-excluded: exclusions-<point>.json, every
        # system's list kept as it was; Gnomon's is the one the confirmation runs under)
        base = {}
        for g in a.gate_excluded or []:
            d = json.loads(g.read_text())
            base[d["point"]] = d
        for pt, ex in sorted(by.items()):
            xd = json.loads(json.dumps(base.get(pt, {"point": pt, "excluded": {}, "reasons": {}})))
            if pt not in base:
                print(f"note: no recorded gate exclusions given for {pt}")
            cur = set(xd["excluded"].get("duckdb", []))
            xd["excluded"]["duckdb"] = sorted(cur | set(ex))
            xd.setdefault("reasons", {}).setdefault("duckdb", {})[LABEL] = len(set(ex) - cur)
            xd["c3_drift"] = {"salt": SALT, "fraction": FRACTION, "sampled": sum(1 for c in chosen if cells[c]["point"] == pt)}
            n = f"drift-exclude-W3-{pt}.json"
            (a.out_dir / n).write_text(json.dumps(xd, indent=1) + "\n")
            files.append(n)
    else:
        # one EXTCMP_EXCLUDE file per (root, point): every non-sampled DuckDB (SQL) cell, plus the recorded gate's
        # exclusions of DuckDB (SQL) at that point (--gate-excluded), which keep their label
        by: dict = {}
        for cid, e in cells.items():
            by.setdefault((e["root"], e["point"]), {})
            if cid not in keep:
                by[(e["root"], e["point"])][e["kit_key"]] = LABEL
        for (root, pt), ex in sorted(by.items()):
            for g in a.gate_excluded or []:
                p = g / root / pt / "excluded.json"
                if p.exists():
                    for k, v in json.loads(p.read_text()).get("duckdbsql", {}).items():
                        ex[k] = v
            n = f"drift-exclude-{a.stage}-{root}-{pt}.json"
            (a.out_dir / n).write_text(json.dumps({"duckdbsql": dict(sorted(ex.items()))}, indent=1) + "\n")
            files.append(n)
    with open(a.out_dir / "SHA256SUMS", "a") as f:
        for n in files:
            f.write(f"{hashlib.sha256((a.out_dir / n).read_bytes()).hexdigest()}  {n}\n")
    per = {}
    for cid in chosen:
        k = cells[cid]["point"]
        per[k] = per.get(k, 0) + 1
    print(f"{a.stage}: universe {doc['universe']} ok DuckDB (SQL) cells ({len(cells)} seen), sample {len(chosen)} "
          f"(per point, sampled/universe: {', '.join(f'{k} {per.get(k, 0)}/{v}' for k, v in sorted(uni.items()))}); "
          f"files {files} in {a.out_dir}")
    return 0


# ---------------------------------------------------------------- report
def cmd_report(a) -> int:
    s = json.loads(a.sample.read_text())
    stage = s["stage"]
    new = universe(stage, a.new)
    rows, ratios, nor = [], [], {"not in the new run": 0, "not ok in the new run": 0}
    for c in s["sample"]:
        e = new.get(c["id"])
        if e is None:
            nor["not in the new run"] += 1
            rows.append((c["id"], c["old_median_ms"], None, None))
            continue
        if not (e["ok"] and e["calls"]):
            nor["not ok in the new run"] += 1
            rows.append((c["id"], c["old_median_ms"], None, None))
            continue
        nm = statistics.median(e["calls"])
        r = nm / c["old_median_ms"] if c["old_median_ms"] > 0 else None
        if r is not None:
            ratios.append(r)
        rows.append((c["id"], c["old_median_ms"], nm, r))
    med = statistics.median(ratios) if ratios else None
    flag = med is None or not (BAND[0] <= med <= BAND[1])
    L = [f"# C3 drift sample, {stage}", "",
         f"Sample {len(s['sample'])} of {s['universe']} DuckDB (SQL) cells (salt {s['salt']}, fraction {s['fraction']}); "
         f"cells with a ratio {len(ratios)}; without: " + ", ".join(f"{v} {k}" for k, v in nor.items()), "",
         f"**Median ratio new / old: {med:.4f}**" if med is not None else "**Median ratio: none (no cell with a ratio)**",
         f"Band {BAND[0]}-{BAND[1]}: **{'FLAGGED' if flag else 'within'}**", "",
         "| cell | old median ms | new median ms | new / old |", "|---|---:|---:|---:|"]
    for cid, o, n, r in rows:
        L.append(f"| {cid} | {o:.3f} | {'' if n is None else f'{n:.3f}'} | {'' if r is None else f'{r:.4f}'} |")
    a.out.write_text("\n".join(L) + "\n")
    print(f"DRIFT {stage}: median {med if med is None else round(med, 4)} over {len(ratios)} cells: "
          f"{'FLAGGED' if flag else 'within'} [{BAND[0]}, {BAND[1]}]; report {a.out}")
    return 1 if flag else 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("sample")
    p.add_argument("--stage", choices=("W1", "W2", "W3"), required=True)
    p.add_argument("--old", type=Path, nargs="+", required=True)
    p.add_argument("--gate-excluded", type=Path, nargs="*", help="W1/W2: the recorded gate result dirs (their <root>/<point>/excluded.json); "
                   "W3: the recorded gate's exclusions-<point>.json files")
    p.add_argument("--out-dir", type=Path, required=True)
    r = sub.add_parser("report")
    r.add_argument("--sample", type=Path, required=True)
    r.add_argument("--new", type=Path, nargs="+", required=True)
    r.add_argument("--out", type=Path, required=True)
    sub.add_parser("rule")
    a = ap.parse_args(argv)
    if a.cmd == "rule":
        print(rule_text())
        return 0
    return {"sample": cmd_sample, "report": cmd_report}[a.cmd](a)


if __name__ == "__main__":
    sys.exit(main())
