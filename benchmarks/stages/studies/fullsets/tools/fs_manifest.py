#!/usr/bin/env python3
""""""
import glob
import hashlib
import json
import os
import sys
from collections import Counter
from pathlib import Path

E = __import__("os").environ.get("GNOMON_BENCH_ROOT", ".") + ""
SEED = "gnomon-c4-fullsets-2026-11-07"
Q = ["reach.window", "b.reach@now", "b.reach@T", "snap.hop2", "b.snap.hop2@now", "b.snap.hop2@T", "nbr.evolution", "coactive.narrow", "paths.k", "resolve.substr", "series.count", "agg.rel_bucket", "b.diff"]
DROP = {"op", "args_echo", "dataset_extent", "tt_q", "pinned", "clamped", "dependency", "plan_ir", "result_digest", "cursor"}
P1W1 = f"{E}/confirm/w1/src/benchmarks/compare/p1"
PTS = {"w1-K10M": ([f"{E}/confirm/w1/results/gate-J42/sp/K10M/answers-native*", f"{E}/confirm/w1/results/gate-J42/p1/K10M/answers-native*"], [f"{E}/confirm/w1/params-single/params-K10M.json", f"{P1W1}/params-K10M.json"]),
       "w1-K10M-d0": ([f"{E}/confirm/w1/results/gate-J42/sp/K10M-d0/answers-native*", f"{E}/confirm/w1/results/gate-J42/p1/K10M-d0/answers-native*"], [f"{E}/confirm/w1/params-single/params-K10M-d0.json", f"{P1W1}/params-K10M-d0.json"]),
       "w1-K10M-d10": ([f"{E}/confirm/w1/results/gate-J42/sp/K10M-d10/answers-native*", f"{E}/confirm/w1/results/gate-J42/p1/K10M-d10/answers-native*"], [f"{E}/confirm/w1/params-single/params-K10M-d10.json", f"{P1W1}/params-K10M-d10.json"])}
for t in ("K10M", "K10M-d0", "K10M-d10"):
    PTS[f"w2-{t}"] = ([f"{E}/confirm/w2/results/gate-5027813[67]/p1/{t}/answers-native*"], [f"{E}/confirm/w2/src/benchmarks/stages/w2/params/params-{t}.json"])
TAG = "W5-wikidata-v2"
PTS[f"w5-{TAG}"] = ([f"{E}/studies/w5/results/W5-wikidata-v2-J49/p1/{TAG}/answers-native-*"], [f"{E}/w5-wikidata/params-c3/params-{TAG}-reg.json", f"{E}/w5-wikidata/params-c3/params-{TAG}-bel.json"])


def short(p):
    tot = 0
    sh = bool(p.get("truncated"))
    for k, v in p.items():
        if isinstance(v, list) and k not in DROP:
            t = p.get(k + "_total")
            if isinstance(t, int):
                tot = max(tot, t)
                sh |= t > len(v)
    for k in ("added_total", "retracted_total"):
        if isinstance(p.get(k), int):
            tot = max(tot, p.get("added_total", 0) + p.get("retracted_total", 0))
    return sh, tot


def main():
    out = Path(sys.argv[1])
    out.mkdir(parents=True, exist_ok=True)
    allm = {}
    for pt, (ans, params) in PTS.items():
        args = {}
        for f in params:
            for i in json.load(open(f))["instances"]:
                args[(i["query"], i["instance"])] = i
        cells, seen = [], set()
        c = Counter()
        for pat in ans:
            for d in sorted(glob.glob(pat)):
                for f in glob.glob(d + "/*.json"):
                    k = os.path.basename(f)[:-5]
                    if "#" not in k or k in seen:
                        continue
                    q, inst = k.split("#", 1)
                    if q not in Q:
                        continue
                    seen.add(k)
                    try:
                        p = json.load(open(f))
                    except ValueError:
                        c[(q, "unreadable answer")] += 1
                        continue
                    sh, tot = short(p)
                    c[(q, "answers")] += 1
                    if not sh:
                        continue
                    if (q, inst) not in args:
                        c[(q, "no parameter entry")] += 1
                        continue
                    cells.append({"point": pt, "query": q, "instance": inst, "key": k, "args": args[(q, inst)]["args"], "stratum": args[(q, inst)].get("stratum"), "recorded_total": tot,
                                  "recorded_lists": {x: len(v) for x, v in p.items() if isinstance(v, list) and x not in DROP}, "order": hashlib.md5(f"{SEED}|{pt}|{q}|{inst}".encode()).hexdigest()})
        cells.sort(key=lambda r: (r["query"], r["order"]))
        allm[pt] = {"seed": SEED, "answers": {f"{a}|{b}": n for (a, b), n in c.items()}, "params": params, "cells": cells}
        (out / f"manifest-{pt}.json").write_text(json.dumps(allm[pt], sort_keys=True) + "\n")
        print(pt, len(cells), dict(Counter(r["query"] for r in cells)), "total>20M", sum(r["recorded_total"] > 20_000_000 for r in cells), "sum totals", sum(min(r["recorded_total"], 20_000_000) for r in cells))
    sums = [f"{hashlib.sha256(p.read_bytes()).hexdigest()}  {p.name}" for p in sorted(out.glob("manifest-*.json"))]
    (out / "SHA256SUMS").write_text("\n".join(sums) + "\n")


if __name__ == "__main__":
    main()
