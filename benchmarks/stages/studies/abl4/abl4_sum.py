"""sum.py <jsonl>...: per store x query: median of all timed calls per arm (U, P, T), the ratios P/U and
T/U, the per-rotation ratios (median over the rotation's blocks), refusals, and hash agreement
(over the arms that answered)."""
import json, statistics as S, sys
from collections import defaultdict
recs = [json.loads(l) for f in sys.argv[1:] for l in open(f)]
ms = defaultdict(list); rot = defaultdict(list); hs = defaultdict(set); ref = defaultdict(int); keys = []
for r in recs:
    k = (r["store"], r["query"])
    if k not in keys:
        keys.append(k)
    if r.get("refused"):
        ref[k + (r["arm"],)] += 1
        continue
    ms[k + (r["arm"],)] += r["ms"]
    rot[k + (r["arm"], r["rot"])] += r["ms"]
    hs[k].add(r["hash"])
rots = sorted({r["rot"] for r in recs})
med = lambda x: S.median(x) if x else float("nan")
print(f"{'store':18s} {'query':20s} {'U ms':>9s} {'P ms':>9s} {'T ms':>9s} {'P/U':>6s} {'T/U':>6s} " +
      " ".join(f"P/U.r{x} T/U.r{x}" for x in rots) + "  n  hash refusedP")
for k in keys:
    u, p, t = (med(ms[k + (a,)]) for a in "UPT")
    rr = []
    for x in rots:
        ux = med(rot[k + ("U", x)])
        rr += [med(rot[k + ("P", x)]) / ux if ux == ux else float("nan"), med(rot[k + ("T", x)]) / ux if ux == ux else float("nan")]
    print(f"{k[0]:18s} {k[1]:20s} {u:9.3f} {p:9.3f} {t:9.3f} {p / u:6.2f} {t / u:6.2f} " +
          " ".join(f"{v:7.2f}" for v in rr) + f" {len(ms[k + ('U',)]):3d} {'same' if len(hs[k]) == 1 else 'DIFF' + str(len(hs[k]))} {ref[k + ('P',)]}")
