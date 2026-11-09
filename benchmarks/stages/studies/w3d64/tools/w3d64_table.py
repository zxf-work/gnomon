#!/usr/bin/env python3
"""w3d64_table.py --res R --old OLD --cells selected.json --out report.md [--hashes-only]: the 64 GB run against the recorded run. Descriptive. Exit 1 if an answered cell's hash differs from the recorded hash of the same cell at either thread setting, or, where DuckDB
had none, from Gnomon's or PostgreSQL's recorded hash. Cells that change state; ratios old limit / 64 GB on p50, per thread setting and on the better of the two; the control cells (recorded limit in this job) against the recorded run and against 64 GB in this job."""
import argparse
import json
import statistics as st
from pathlib import Path


def rec(p):
    if not Path(p).exists():
        return {}
    return {f"{c['query']}|{c['instance']}|{c['setting']}": c for c in json.loads(Path(p).read_text()).get("results", [])}


def state(c):
    if c is None:
        return "not run"
    if c.get("ok"):
        return f"{c['p50_ms']:.1f} ms"
    return "censored: " + ("prime killed" if "killed at 60" in (c.get("error") or "") else (c.get("error") or "")[:40])


def best(a, b):
    v = [x["p50_ms"] for x in (a, b) if x and x.get("ok")]
    return min(v) if v else None


def main():
    ap = argparse.ArgumentParser()
    for k in ("res", "old", "cells", "out"):
        ap.add_argument("--" + k, required=True)
    ap.add_argument("--hashes-only", action="store_true")
    a = ap.parse_args()
    sel = json.loads(Path(a.cells).read_text())
    o64, o32, g, pg = (rec(f"{a.old}/ALL-{s}.json") for s in ("duckdb64", "duckdb32", "gnomon", "postgres"))
    f64, f32, c64, c32 = (rec(f"{a.res}/{n}/ALL-duckdb.json") for n in ("F64", "F32", "C64", "C32"))
    diff, newans = [], []
    for name, m in (("F64", f64), ("F32", f32), ("C64", c64), ("C32", c32)):
        for k, c in m.items():
            if not c.get("ok"):
                continue
            refs = [(n, r[k]["hash"]) for n, r in (("recorded duckdb64", o64), ("recorded duckdb32", o32)) if k in r and r[k].get("ok")]
            if not refs:
                refs = [(n, r[k]["hash"]) for n, r in (("recorded gnomon", g), ("recorded postgres", pg)) if k in r and r[k].get("ok")]
                newans.append((name, k, [n for n, _ in refs] or ["no other system's answer"]))
            for n, h in refs:
                if h != c.get("hash"):
                    diff.append((name, k, n, c.get("hash"), h))
    L = ["# DuckDB (SQL), W3 SF10, memory_limit 64 GB against the recorded 18.6 GiB (20GB)", "", f"Cells selected {len(sel['selected'])}; prime killed or interrupted in the record {len(sel['killed_at_prime'])}; control {len(sel['control'])}. Answers differing from a recorded answer: {len(diff)}."]
    L += [f"- DIFFERS {x}" for x in diff]
    if a.hashes_only:
        Path(a.out).write_text("\n".join(L) + "\n")
        return 1 if diff else 0
    changed, rows, r64, r32, rb = [], [], [], [], []
    for k in sorted(sel["selected"]):
        s = [state(o64.get(k)), state(f64.get(k)), state(o32.get(k)), state(f32.get(k))]
        rows.append(f"| {k} | {s[0]} | {s[1]} | {s[2]} | {s[3]} |")
        for lab, x, y, acc in (("64 threads", o64.get(k), f64.get(k), r64), ("32 threads", o32.get(k), f32.get(k), r32)):
            ox, ny = bool(x and x.get("ok")), bool(y and y.get("ok"))
            if ox != ny:
                changed.append(f"| {k} | {lab} | {state(x)} | {state(y)} |")
            if ox and ny:
                acc.append(x["p50_ms"] / y["p50_ms"])
        bo, bn = best(o64.get(k), o32.get(k)), best(f64.get(k), f32.get(k))
        if bo and bn:
            rb.append(bo / bn)
    q = lambda v: f"n {len(v)}, median {st.median(v):.3f}, min {min(v):.3f}, max {max(v):.3f}" if v else "n 0"
    L += ["", "## Ratio recorded limit / 64 GB, p50", "", f"- 64 threads: {q(r64)}", f"- 32 threads: {q(r32)}", f"- better of the two threads on each side: {q(rb)}", "", "## Cells that change answered or censored state", "", "| cell | threads | recorded | 64 GB |", "|---|---|---|---|"] + changed
    L += ["", f"Cells answered in this run where DuckDB had no recorded answer, and what they were compared with: {len(newans)}"] + [f"- {x}" for x in newans]
    L += ["", "## Control: recorded limit in this job", "", "| cell | threads | recorded run | this job 20GB | this job 64 GB | recorded / this job 20GB | this job 20GB / this job 64 GB |", "|---|---|---|---|---|---:|---:|"]
    for k in sel["control"]:
        for lab, o, c, f in (("64", o64, c64, f64), ("32", o32, c32, f32)):
            x, y, z = o.get(k), c.get(k), f.get(k)
            r1 = f"{x['p50_ms'] / y['p50_ms']:.3f}" if x and y and x.get("ok") and y.get("ok") else ""
            r2 = f"{y['p50_ms'] / z['p50_ms']:.3f}" if y and z and y.get("ok") and z.get("ok") else ""
            L.append(f"| {k} | {lab} | {state(x)} | {state(y)} | {state(z)} | {r1} | {r2} |")
    L += ["", "## All cells", "", "| cell | recorded 64 threads | 64 GB 64 threads | recorded 32 threads | 64 GB 32 threads |", "|---|---|---|---|---|"] + rows
    Path(a.out).write_text("\n".join(L) + "\n")
    return 1 if diff else 0


if __name__ == "__main__":
    raise SystemExit(main())
