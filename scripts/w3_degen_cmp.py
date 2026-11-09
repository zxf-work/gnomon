"""Compare degenerate-window answers (window [now, now+1us)) with the as-of answers at the same T.
    w3_degen_cmp.py ASOF.jsonl DEGEN.jsonl"""
import json, sys, collections
a = {(x["qid"], x["i"]): x for x in map(json.loads, open(sys.argv[1])) if x["setting"] == "asof"}
v = collections.Counter(); ex = {}
for x in map(json.loads, open(sys.argv[2])):
    y = a.get((x["qid"], x["i"]))
    if not y:
        continue
    if not x["ok"]:
        k = "error"
    elif not y["ok"]:
        k = "asof-error"
    elif x["hash"] == y["hash"]:
        k = "same"
    elif x["sorted_hash"] == y["sorted_hash"]:
        k = "same-rows-other-order"
    else:
        k = "DIFFERS"
    v[(x["qid"], k)] += 1
    if k not in ("same", "same-rows-other-order") and x["qid"] not in ex:
        ex[x["qid"]] = x.get("err") or y.get("err") or (x["n"], y["n"], x["rows"][:2], y["rows"][:2])
for k, n in sorted(v.items()):
    print(k, n)
for k, e in ex.items():
    print("DETAIL", k, str(e)[:400])
