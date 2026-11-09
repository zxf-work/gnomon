#!/usr/bin/env python3
"""Deterministic 10 % sample of the W2 K10M-d0 and K10M-d10 instances for Neo4j and Memgraph.


Sources per point (tag K10M-d0, K10M-d10):
  T10 set  = <t10>/params-<tag>.json, the 234 instances of the t10 stage (families P T S W D N R M; classes T, B (hist.asof), G).
  B family = <bfam>/<tag>/params-used.json, the 163 belief-time instances of the October W2 density records.
Rule (written before any timing): for each source and each query, order its instances by md5
as a hex string, ascending, and keep the first k = max(1, floor(n / 10 + 1/2)) of the n instances. The two sources' samples are written into one
parameter file per point with the T10 file's log block; the B file's log sha256 must equal it. Nothing else of an instance changes.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from collections import defaultdict
from pathlib import Path

SALT = "covy-t10s/2026-11-07"


def pick(insts: list) -> list:
    byq = defaultdict(list)
    for i in insts:
        byq[i["query"]].append(i)
    out = []
    for q in sorted(byq):
        xs = sorted(byq[q], key=lambda i: hashlib.md5(f"{SALT}|{q}|{i['instance']}".encode()).hexdigest())
        k = max(1, int(len(xs) / 10 + 0.5))
        out += xs[:k]
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--t10", type=Path, required=True); ap.add_argument("--bfam", type=Path, required=True); ap.add_argument("--out", type=Path, required=True)
    a = ap.parse_args()
    a.out.mkdir(parents=True, exist_ok=True)
    for tag in ("K10M-d0", "K10M-d10"):
        t = json.loads((a.t10 / f"params-{tag}.json").read_text())
        b = json.loads((a.bfam / tag / "params-used.json").read_text())
        if t["log"]["sha256"] != b["log"]["sha256"]:
            raise SystemExit(f"{tag}: log sha256 differs between the sources: {t['log']} {b['log']}")
        for src in (t, b):
            for i in src["instances"]:
                assert {"query", "instance", "args"} <= set(i), i
        st, sb = pick(t["instances"]), pick(b["instances"])
        ids = [i["instance"] for i in st + sb]
        assert len(ids) == len(set(ids)), f"{tag}: instance ids repeat"
        counts = defaultdict(int)
        for i in st + sb:
            counts[i["query"]] += 1
        sel = dict(t["selection"])
        sel["cov_y_t10s"] = {"rule": __doc__.split("Rule (written before any timing): ")[1].split("\n\"\"\"")[0].replace("\n", " ").strip(),
                             "salt": SALT, "sources": {"t10": str(a.t10 / f"params-{tag}.json"), "b_family": str(a.bfam / tag / "params-used.json")},
                             "from": {"t10": len(t["instances"]), "b_family": len(b["instances"])}, "kept": {"t10": len(st), "b_family": len(sb)}}
        out = {"format": t["format"], "version": t["version"], "log": t["log"], "selection": sel, "counts": dict(sorted(counts.items())), "instances": st + sb}
        (a.out / f"params-{tag}.json").write_text(json.dumps(out, indent=1) + "\n")
        print(tag, "t10", len(t["instances"]), "->", len(st), "| B", len(b["instances"]), "->", len(sb), "|", dict(sorted(counts.items())))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
