#!/usr/bin/env python3
"""
    kf_jsonl_compare.py --new A.jsonl --old B.jsonl [--old B2.jsonl --override-query TCR11]
                        --key qid,i,setting [--hash hash] [--label W3-gnomon]

Cells are keyed by --key; a cell is answered when it has the hash field and no
error (`ok` false, `status` not ok, `err`/`error`). Per query: equal / DIFFERS /
now answered / no longer answered / both unanswered / missing. Exit 1 on DIFFERS,
no longer answered, or missing in the new file.
"""
import argparse
import json
import sys
from collections import Counter, defaultdict

ap = argparse.ArgumentParser()
ap.add_argument("--new", required=True)
ap.add_argument("--old", required=True, action="append")
ap.add_argument("--override-query", default="", help="queries taken from the LAST --old file only")
ap.add_argument("--key", required=True)
ap.add_argument("--hash", default="hash")
ap.add_argument("--alt-hash", default="sorted_hash")
ap.add_argument("--label", default="")
ap.add_argument("--system", default="", help="keep only records whose system field equals this")
a = ap.parse_args()
K = a.key.split(",")


def load(p):
    out = {}
    for line in open(p):
        line = line.strip()
        if not line:
            continue
        r = json.loads(line)
        if a.system and r.get("system") not in (None, a.system):
            continue
        out[tuple(str(r.get(k)) for k in K)] = r
    return out


def answered(r):
    if r is None:
        return False
    if r.get("ok") is False or r.get("err") or r.get("error"):
        return False
    st = r.get("status")
    if st is not None and st not in ("ok", "OK"):
        return False
    return r.get(a.hash) is not None


new = load(a.new)
old = load(a.old[0])
ov = set(filter(None, a.override_query.split(",")))
if len(a.old) > 1:
    last = load(a.old[-1])
    old = {k: v for k, v in old.items() if k[0] not in ov}
    old.update({k: v for k, v in last.items() if k[0] in ov})
per = defaultdict(Counter)
notes = []
bad = 0
for k in sorted(set(new) | set(old)):
    n, o = new.get(k), old.get(k)
    q = k[0]
    if n is None:
        per[q]["missing in new"] += 1; bad += 1; notes.append(f"missing in new {k}"); continue
    if o is None:
        per[q]["not recorded"] += 1; continue
    an, ao = answered(n), answered(o)
    if an and ao:
        if n[a.hash] == o[a.hash]:
            per[q]["equal"] += 1
        elif a.alt_hash and n.get(a.alt_hash) is not None and n.get(a.alt_hash) == o.get(a.alt_hash):
            per[q]["same rows, other order"] += 1
        else:
            per[q]["DIFFERS"] += 1; bad += 1
            notes.append(f"DIFFERS {k}: new {n[a.hash]} n={n.get('n')} recorded {o[a.hash]} n={o.get('n')}")
    elif an:
        per[q]["now answered"] += 1
        notes.append(f"now answered {k} (recorded: {str(o.get('err') or o.get('error') or o.get('status') or o.get('kind'))[:80]})")
    elif ao:
        per[q]["no longer answered"] += 1; bad += 1
        notes.append(f"no longer answered {k}: {str(n.get('err') or n.get('error') or n.get('status') or n.get('kind'))[:120]}")
    else:
        per[q]["both unanswered"] += 1
tot = Counter()
for q in sorted(per):
    tot.update(per[q])
    print(f"  {q:<16} " + ", ".join(f"{v} {c}" for c, v in sorted(per[q].items())))
print(f"{a.label} TOTAL " + ", ".join(f"{v} {c}" for c, v in sorted(tot.items())))
for x in notes[:60]:
    print("   " + x)
print(f"{a.label} RESULT", "PASS" if bad == 0 else f"FAIL ({bad})")
sys.exit(1 if bad else 0)
