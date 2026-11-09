#!/usr/bin/env python3
"""W5 on C4. w5_gate.py --new <point dir> --out md: Gnomon's verify-native-{reg,bel}.json against the baselines' recorded verify records
in the same directory. Exit 7 on a differing answer (hash or projected hash), on a Gnomon cell not answered that a baseline answered inside
10 s. --timed: every rotation's timed hashes against Gnomon's verify hashes; exit 8 on a difference."""
import argparse
import glob
import json
import os
from collections import Counter


def load(p):
    with open(p) as f:
        return {(r["query"], r.get("instance")): r for r in json.load(f).get("results", [])}


def same(a, b):
    return a.get("hash") == b.get("hash") or (bool(b.get("proj_hash")) and b.get("proj_hash") == a.get("proj_hash"))


def gate(d):
    fail, cnt = [], Counter()
    for v in ("reg", "bel"):
        N = load(os.path.join(d, f"verify-native-{v}.json"))
        if not N:
            fail.append(f"no Gnomon cell in {v}")
        for s in ("duckdbsql", "postgres", "neo4j"):
            for k, r in load(os.path.join(d, f"verify-{s}-{v}.json")).items():
                n = N.get(k)
                if not r.get("ok"):
                    cnt["baseline not ok"] += 1
                elif n is None or not n.get("ok"):
                    if r.get("wall_ms", 1e18) <= 10000:
                        fail.append(f"{v} {s} {k}: Gnomon {(n or {}).get('error', 'no answer')!s:.120}")
                    else:
                        cnt["no Gnomon answer, baseline above 10 s"] += 1
                elif same(n, r):
                    cnt["equivalent"] += 1
                else:
                    fail.append(f"{v} {s} {k}: DIFFERS")
    return fail, cnt


def timed(d):
    ref = {}
    for v in ("reg", "bel"):
        ref.update({k: r for k, r in load(os.path.join(d, f"verify-native-{v}.json")).items() if r.get("ok")})
    diffs, n = [], 0
    roots = [d, d.replace("/results-p1/", "/results-t32p1/")]
    for root in roots:
        for p in glob.glob(os.path.join(root, "rot[0-9]*", "*-*.json")):
            with open(p) as f:
                cells = json.load(f).get("results", [])
            for c in cells:
                k = (c.get("query"), c.get("instance"))
                if c.get("ok") and k in ref:
                    n += 1
                    if not same(c, ref[k]):
                        diffs.append(f"{os.path.relpath(p, d)} {k}")
    return diffs, n


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--new", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--timed", action="store_true")
    a = ap.parse_args()
    if a.timed:
        diffs, n = timed(a.new)
        head = f"W5 timed answers: {n} compared, {len(diffs)} differences"
        open(a.out, "w").write(head + "\n" + "\n".join(diffs[:200]) + "\n")
        print(head)
        return 8 if diffs else 0
    fail, cnt = gate(a.new)
    head = f"W5 gate: {dict(cnt)}; {len(fail)} failures"
    open(a.out, "w").write(head + "\n" + "\n".join(fail[:200]) + "\n")
    print(head)
    for x in fail[:10]:
        print("FAIL", x)
    return 7 if fail else 0


if __name__ == "__main__":
    raise SystemExit(main())
