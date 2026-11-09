"""Guarded parameter regeneration: only the records of the allowed queries may change. Usage: guard.py OLD NEW Q1:Q2:..."""
import json, sys, collections
a = {(x["qid"], x["i"]): x for x in map(json.loads, open(sys.argv[1]))}
b = {(x["qid"], x["i"]): x for x in map(json.loads, open(sys.argv[2]))}
ch = collections.Counter(k[0] for k in set(a) | set(b) if a.get(k) != b.get(k))
bad = [q for q in ch if q not in sys.argv[3].split(":")]
print(sys.argv[1], "changed records per query:", dict(ch), "REFUSED: outside the allowed set" if bad else "")
sys.exit(1 if bad else 0)
