"""aa.py <jsonl>...: A/A ratios from the two unset blocks of each window (later U block median /
earlier U block median), per store over all queries and both rotations: 5th-95th percentile band,
and the same over the arms' two blocks (arm consistency). Excludes the direct nb rows unless --nb."""
import json, statistics as S, sys
from collections import defaultdict
nb = "--nb" in sys.argv
files = [f for f in sys.argv[1:] if not f.startswith("--")]
blk = defaultdict(list)
for f in files:
    for l in open(f):
        r = json.loads(l)
        if r.get("refused") or ((r["query"][:3] in ("nb5", "nb8")) != nb):
            continue
        blk[(r["store"], r["query"], r["rot"], r["arm"])].append((r["block"], S.median(r["ms"])))
by = defaultdict(list)
for (st, q, rot, arm), v in blk.items():
    if arm == "U" and len(v) == 2:
        v.sort(); by[st].append(v[1][1] / v[0][1])
def pct(x, p):
    x = sorted(x); i = (len(x) - 1) * p; lo = int(i); hi = min(lo + 1, len(x) - 1)
    return x[lo] + (x[hi] - x[lo]) * (i - lo)
allr = []
for st, r in by.items():
    allr += r
    print(f"{st:20s} n={len(r):3d} A/A p5 {pct(r, .05):.3f} p95 {pct(r, .95):.3f} min {min(r):.3f} max {max(r):.3f}")
print(f"{'all':20s} n={len(allr):3d} A/A p5 {pct(allr, .05):.3f} p95 {pct(allr, .95):.3f}")
