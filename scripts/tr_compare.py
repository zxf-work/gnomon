#!/usr/bin/env python3
"""Answers of a timed_runner block against an answer record.

    tr_compare.py --block BLOCK.json --ref REF --format w3|w4|w6check|w6oracle|w6gm [--alt REF2] [--label L]

  w3        JSON lines {qid, i, setting, ok, hash, sorted_hash}          key (query, instance, setting)
  w4        JSON lines {query, instance, status, hash}                   key (query, instance)
  w6check   a w6_tmgbench check JSON (answers[cell].instances[n].hash)   key (query-setting, instance)
  w6oracle  the same file, compared with each instance's oracle hash
  w6gm      JSON lines {query, asof, variant, hash}                      key (q<query>, asof)
Per query: equal / same rows other order (w3) / DIFFERS / runner-only answered / record-only answered /
neither. Exit 1 on DIFFERS or on a cell the record answered and the runner did not.
"""
import argparse
import json
import sys
from collections import Counter, defaultdict

ap = argparse.ArgumentParser()
ap.add_argument("--block", required=True, nargs="+")
ap.add_argument("--ref", required=True)
ap.add_argument("--format", required=True, choices=["w3", "w4", "w6check", "w6oracle", "w6gm"])
ap.add_argument("--label", default="")
ap.add_argument("--allow-lost", default="", help="queries whose recorded answers the runner may not reproduce (censored)")
a = ap.parse_args()

run = {}
for b in a.block:
    for c in json.load(open(b))["results"]:
        if a.format == "w3":
            k = (c["query"], str(c["instance"]), c["setting"])
        elif a.format == "w4":
            k = (c["query"], str(c["instance"]))
        elif a.format in ("w6check", "w6oracle"):
            k = (f"{c['query']}-{c['setting']}", str(c["instance"]))
        else:
            k = (c["query"], c["setting"])
        run[k] = c

ref, alt = {}, {}
if a.format in ("w6check", "w6oracle"):
    d = json.load(open(a.ref))
    for cell, v in d["answers"].items():
        for i in v.get("instances", []):
            h = i.get("oracle") if a.format == "w6oracle" else (i.get("hash") if "error" not in i else None)
            if v.get("expressible") is False and a.format == "w6check":
                h = None                     # the record ran a cell the system cannot express: nothing to compare
            ref[(cell, str(i["n"]))] = {"ok": h is not None, "hash": h}
else:
    for line in open(a.ref):
        if not line.strip():
            continue
        r = json.loads(line)
        if a.format == "w3":
            k = (r["qid"], str(r["i"]), r["setting"])
            ref[k] = {"ok": bool(r.get("ok")) and r.get("hash") is not None, "hash": r.get("hash"),
                      "sorted_hash": r.get("sorted_hash")}
        elif a.format == "w4":
            k = (r["query"], str(r["instance"]))
            ref[k] = {"ok": r.get("status") == "ok", "hash": r.get("hash")}
        else:
            if r.get("variant", "as_written") != "as_written":
                continue
            k = (f"q{r['query']}", r["asof"])
            ref[k] = {"ok": r.get("hash") is not None, "hash": r.get("hash")}

per = defaultdict(Counter)
bad = 0
notes = []
allow = set(filter(None, a.allow_lost.split(",")))
for k in sorted(set(run) & set(ref), key=str):
    c, r = run[k], ref[k]
    q = k[0]
    if c.get("ok") and r["ok"]:
        if c.get("hash") == r["hash"]:
            per[q]["equal"] += 1
        elif a.format == "w3" and r.get("sorted_hash") and c.get("hash") == r.get("sorted_hash"):
            per[q]["same rows, other order"] += 1
        else:
            per[q]["DIFFERS"] += 1; bad += 1; notes.append(f"DIFFERS {k}: runner {c.get('hash')} record {r['hash']}")
    elif c.get("ok"):
        per[q]["runner-only answered"] += 1
    elif r["ok"]:
        tag = "censored" if c.get("censored") else ("not expressible" if c.get("not_expressible") else "error")
        per[q][f"record-only answered ({tag})"] += 1
        if q not in allow:
            bad += 1
        notes.append(f"record-only {k}: {str(c.get('error'))[:120]}")
    else:
        per[q]["neither"] += 1
only_run = len(set(run) - set(ref))
tot = Counter()
for q in sorted(per):
    tot.update(per[q])
    print(f"  {q:<14} " + ", ".join(f"{v} {k}" for k, v in sorted(per[q].items())))
print(f"{a.label} TOTAL " + ", ".join(f"{v} {k}" for k, v in sorted(tot.items())) + f"; {only_run} runner cells not in the record")
for x in notes[:40]:
    print("   " + x)
print(f"{a.label} RESULT", "PASS" if bad == 0 else f"FAIL ({bad})")
sys.exit(1 if bad else 0)
