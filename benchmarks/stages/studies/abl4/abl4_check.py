#!/usr/bin/env python3
"""an answer hash that differs between a configuration and its paired unset run stops the run). Refused calls have no hash."""
import json
import sys
from collections import defaultdict


def diffs(paths):
    hs = defaultdict(set)
    for p in paths:
        with open(p) as f:
            for line in f:
                r = json.loads(line)
                if not r.get("refused"):
                    hs[(r["store"], r["query"])].add(r["hash"])
    return sorted(k for k, v in hs.items() if len(v) > 1)


if __name__ == "__main__":
    bad = diffs(sys.argv[1:])
    for k in bad:
        print("ANSWER MISMATCH", k)
    sys.exit(7 if bad else 0)
