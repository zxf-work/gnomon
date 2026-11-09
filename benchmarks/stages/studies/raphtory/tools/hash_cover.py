#!/usr/bin/env python3
"""hash_cover.py: what the answer hash of the parameter-file path covers, per query, measured from the answers Gnomon wrote at the answer checks. For each answer file: the fields left after the hash's drop list, each list field with its length and its <name>_total, and truncated. Read-only."""
import collections
import glob
import json
import os
import re
import sys
E = __import__("os").environ.get("GNOMON_BENCH_ROOT", ".") + ""
SRC = {"W1": [f"{E}/confirm/w1/results/gate-J42/sp/*/answers-native*", f"{E}/confirm/w1/results/gate-J42/p1/*/answers-native*"],
       "W2": [f"{E}/confirm/w2/results/gate-5027813[67]/p1/*/answers-native*"],
       "W5": [f"{E}/studies/w5/results/W5-wikidata-v2-J49/p1/*/answers-native*"]}
DROP = {"op", "args_echo", "dataset_extent", "tt_q", "pinned", "clamped", "dependency", "plan_ir", "result_digest", "cursor"}
out = []
for wl, pats in SRC.items():
    t = collections.defaultdict(lambda: {"n": 0, "trunc": 0, "over": 0, "keys": set(), "maxpage": 0, "maxtotal": 0})
    for pat in pats:
        for d in glob.glob(pat):
            for f in glob.glob(d + "/*.json"):
                q = os.path.basename(f)[:-5].split("#")[0]
                try:
                    p = json.load(open(f))
                except ValueError:
                    continue
                if not isinstance(p, dict):
                    continue
                r = t[q]
                r["n"] += 1
                r["keys"] |= set(p) - DROP
                r["trunc"] += bool(p.get("truncated"))
                over = False
                for k, v in p.items():
                    if isinstance(v, list) and k not in DROP:
                        tot = p.get(k + "_total")
                        r["maxpage"] = max(r["maxpage"], len(v))
                        if isinstance(tot, int):
                            r["maxtotal"] = max(r["maxtotal"], tot)
                            over |= tot > len(v)
                r["over"] += over
    out.append(f"\n## {wl}\n\n| query | answers read | fields in the hash | answers with a list shorter than its total | truncated true | longest list hashed | largest total |\n|---|---:|---|---:|---:|---:|---:|")
    for q, r in sorted(t.items()):
        out.append(f"| {q} | {r['n']} | {', '.join(sorted(r['keys']))} | {r['over']} | {r['trunc']} | {r['maxpage']} | {r['maxtotal']} |")
print("\n".join(out))
