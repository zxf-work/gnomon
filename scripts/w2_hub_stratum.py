#!/usr/bin/env python3
"""W2: add the hub stratum to a P1-format parameter file.

`extcmp_p1.py select` picks seeds at percentiles of an MD5 sample of the
candidates, so on a power-law graph the few hubs are almost never chosen.
W2's design puts hub seeds in a stratum of their own: for every seeded query
and every class already in the file (instant, window, interval, as-of point),
the first instance of the class is copied with its seed replaced by each of
the top-k hubs (cumulative degree, from `w2_characterise.py`). Instance ids
follow `extcmp_p1._inst` (md5 of version, query and canonical arguments), and
the file is rewritten through `extcmp_p1._write` (same shuffle, same
duplicate check), stratum "hub".

    python scripts/w2_hub_stratum.py --params params-K1M.json \
        --characterisation powerlaw-1000000.json --out params-K1M.json
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import extcmp_p1 as P  # noqa: E402

#: query -> (how the seed is set, hubs per class)
SEEDED = {
    "snap.hop2": (lambda a, h: a.update(seeds=[h[0]]), 5),
    "reach.window": (lambda a, h: a.update(src=h[0]), 3),
    "paths.k": (lambda a, h: a.update(src=h[0]), 3),
    "nbr.evolution": (lambda a, h: a.update(uid=h[0]), 5),
    "coactive.narrow": (lambda a, h: a.update(a_spec={"src": h[0]}, b_spec={"src": h[1]}), 5),
    "b.reach@now": (lambda a, h: a.update(src=h[0]), 5),
    "b.reach@T": (lambda a, h: a.update(src=h[0]), 1),
    "b.snap.hop2@now": (lambda a, h: a.update(seeds=list(h[:20])), 1),
    "b.snap.hop2@T": (lambda a, h: a.update(seeds=list(h[:20])), 1),
}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--params", type=Path, required=True)
    ap.add_argument("--characterisation", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    a = ap.parse_args()
    doc = json.loads(a.params.read_text())
    ch = json.loads(a.characterisation.read_text())
    hubs = [h["uid"] for h in ch["hubs_top20"]]
    ins = [i for i in doc["instances"] if i.get("stratum") != "hub"]
    added = []
    keys = {(i["query"], i["instance"]) for i in ins}
    for q, (setter, k) in SEEDED.items():
        seen = set()
        for t in ins:
            if t["query"] != q or t["class"] in seen:
                continue
            seen.add(t["class"])
            for j in range(k):
                if q.startswith("b.snap"):
                    hs = hubs
                elif q == "coactive.narrow":
                    hs = [hubs[j], hubs[(j + 1) % len(hubs)]]
                else:
                    hs = [hubs[j]]
                args = json.loads(json.dumps(t["args"]))
                setter(args, hs)
                f = {"hub_rank": j + 1, "hub": hs[0], "template": t["instance"]}
                new = P._inst(q, args, "hub", t["class"], f)
                if (q, new["instance"]) not in keys:     # a hub already sampled
                    keys.add((q, new["instance"]))
                    added.append(new)
    notes = list(doc["selection"]["notes"]) + [
        f"hub stratum (w2_hub_stratum.py): top hubs by cumulative degree {hubs[:5]}; "
        f"per seeded query and class, the first instance of the class with its seed replaced "
        f"by each of the top-k hubs (k per query: { {q: k for q, (_, k) in SEEDED.items()} })"]
    P._write(a.out, doc["log"], ins + added, notes)
    print(f"hub instances added: {len(added)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
