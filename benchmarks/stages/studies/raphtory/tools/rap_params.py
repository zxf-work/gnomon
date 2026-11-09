#!/usr/bin/env python3
"""rap_params.py <out dir> [--smoke]: the parameter files of the Raphtory stage, written before submission: the recorded files filtered to the five queries, same instance ids and arguments.
--smoke: at most 2 hub and 1 other instance per query. Each output carries its source file and sha256."""
import hashlib
import json
import sys
from pathlib import Path

E = Path(__import__("os").environ.get("GNOMON_BENCH_ROOT", ".") + "")
Q = {"snap.hop2", "nbr.evolution", "reach.window", "b.snap.hop2@now", "b.reach@now"}
SRC = {"w1": (E / "confirm/w1/params-single", ["K1M", "K10M", "K10M-d0", "K10M-d10"]), "w2": (E / "studies/abl/params-w2", ["K10M", "K10M-d0", "K10M-d10"])}


def main():
    out = Path(sys.argv[1])
    smoke = "--smoke" in sys.argv
    sums = []
    for wl, (d, tags) in SRC.items():
        (out / wl).mkdir(parents=True, exist_ok=True)
        for t in tags:
            src = d / f"params-{t}.json"
            raw = src.read_bytes()
            doc = json.loads(raw)
            keep = [i for i in doc["instances"] if i["query"] in Q]
            if smoke:
                pick, n = [], {}
                for i in keep:
                    k = (i["query"], "hub" in i.get("factors", {}))
                    n[k] = n.get(k, 0) + 1
                    if n[k] <= (2 if k[1] else 1):
                        pick.append(i)
                keep = pick
            counts = {}
            for i in keep:
                counts[i["query"]] = counts.get(i["query"], 0) + 1
            doc.update(instances=keep, counts=counts, filtered_from={"path": str(src), "sha256": hashlib.sha256(raw).hexdigest(), "queries": sorted(Q), "smoke": smoke})
            p = out / wl / f"params-{t}.json"
            p.write_text(json.dumps(doc, indent=0, sort_keys=True) + "\n")
            sums.append(f"{hashlib.sha256(p.read_bytes()).hexdigest()}  {wl}/{p.name}")
            print(wl, t, counts)
    (out / "SHA256SUMS").write_text("\n".join(sums) + "\n")


if __name__ == "__main__":
    main()
