#!/usr/bin/env python3
"""rl_params.py <out dir> [--smoke]: the fuller-form stage's parameter files, written before submission, filtered from the recorded files with the same ids and arguments. Part 1: snap.hop2, nbr.evolution, b.snap.hop2@now at W1 K1M, K10M, K10M-d0, K10M-d10, W2 K10M, K10M-d0. Part 2: W2 K10M-d10, the B family's b.snap.hop2@now and b.reach@now. Part 3: W5, all five queries."""
import hashlib
import json
import sys
from pathlib import Path

E = Path(__import__("os").environ.get("GNOMON_BENCH_ROOT", ".") + "")
P1 = {"snap.hop2", "nbr.evolution", "b.snap.hop2@now"}
ALL = P1 | {"reach.window", "b.reach@now"}
W5 = "W5-wikidata-v2"
SRC = [("w1", t, E / f"confirm/w1/params-single/params-{t}.json", P1) for t in ("K1M", "K10M", "K10M-d0", "K10M-d10")]
SRC += [("w2", t, E / f"studies/abl/params-w2/params-{t}.json", P1) for t in ("K10M", "K10M-d0")]
SRC += [("w2", "K10M-d10", E / "studies/abl/params-w2/params-K10M-d10.json", ALL)]
SRC += [("w5reg", W5, E / f"w5-wikidata/params-c3/params-{W5}-reg.json", ALL), ("w5bel", W5, E / f"w5-wikidata/params-c3/params-{W5}-bel.json", ALL)]


def main():
    out = Path(sys.argv[1])
    smoke = "--smoke" in sys.argv
    sums = []
    for wl, t, src, q in SRC:
        (out / wl).mkdir(parents=True, exist_ok=True)
        raw = src.read_bytes()
        doc = json.loads(raw)
        keep = [i for i in doc["instances"] if i["query"] in q]
        if smoke:
            pick, n = [], {}
            for i in keep:
                k = (i["query"], "hub" in i.get("factors", {}))
                n[k] = n.get(k, 0) + 1
                if n[k] <= (2 if k[1] else 1):
                    pick.append(i)
            keep = pick
        c = {}
        for i in keep:
            c[i["query"]] = c.get(i["query"], 0) + 1
        doc.update(instances=keep, counts=c, filtered_from={"path": str(src), "sha256": hashlib.sha256(raw).hexdigest(), "queries": sorted(q), "smoke": smoke})
        p = out / wl / f"params-{t}.json"
        p.write_text(json.dumps(doc, indent=0, sort_keys=True) + "\n")
        sums.append(f"{hashlib.sha256(p.read_bytes()).hexdigest()}  {wl}/{p.name}")
        print(wl, t, c)
    (out / "SHA256SUMS").write_text("\n".join(sums) + "\n")


if __name__ == "__main__":
    main()
