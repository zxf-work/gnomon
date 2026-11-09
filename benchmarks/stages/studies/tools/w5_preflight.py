#!/usr/bin/env python3
"""registry, belief-time and match files equal the values, and the cell count is the 230."""
import argparse
import hashlib
import json
import os
import sys

PINNED = {"params-W5-wikidata-v2-reg.json": "428b37e087fdc360efe2", "params-W5-wikidata-v2-bel.json": "26f268d65ba676cf475b",
          "match-W5-wikidata-v2.json": "c52c517dcb39890663b8"}
PINNED_CELLS = 230


def sha(p):
    with open(p, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()


def check(pd, tag, base, pinned):
    bad, cells = [], set()
    if pinned:
        for name, want in PINNED.items():
            got = sha(os.path.join(pd, name))
            if not got.startswith(want):
                bad.append(f"{name}: sha256 {got[:20]}, pinned {want}")
    for v in ("reg", "bel"):
        with open(os.path.join(pd, f"params-{tag}-{v}.json")) as f:
            have = {(i["query"], i["instance"]) for i in json.load(f)["instances"]}
        cells |= have
        for s in ("duckdbsql", "postgres", "neo4j"):
            with open(os.path.join(base, f"verify-{s}-{v}.json")) as f:
                rec = {(c["query"], c.get("instance")) for c in json.load(f)["results"]}
            print(f"{v} {s}: {len(rec & have)}/{len(rec)} recorded cells in the file; file has {len(have)}")
            if rec - have:
                bad.append(f"{v} {s}: {len(rec - have)} recorded cells not in the file")
            if have - rec:
                bad.append(f"{v} {s}: {len(have - rec)} file cells not recorded")
    if pinned and len(cells) != PINNED_CELLS:
        bad.append(f"{len(cells)} cells, pinned {PINNED_CELLS}")
    return bad, len(cells)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--params-dir", required=True)
    ap.add_argument("--tag", required=True)
    ap.add_argument("--base", required=True)
    ap.add_argument("--pinned", action="store_true")
    a = ap.parse_args()
    try:
        bad, n = check(a.params_dir, a.tag, a.base, a.pinned)
    except (OSError, KeyError, ValueError) as e:
        print(f"W5 PREFLIGHT FAILED: {e}")
        return 2
    for b in bad:
        print("W5 PREFLIGHT:", b)
    print(f"W5 PREFLIGHT {'FAILED' if bad else 'PASS'}: {n} cells")
    return 2 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
