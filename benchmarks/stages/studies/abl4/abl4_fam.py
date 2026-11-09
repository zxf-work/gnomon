"""rotation median U); family ratio = geomean over the family's queries (paths.k excluded from T for P,
reported alone). A/A: per rotation, later U block / earlier U block; band = p5-p95 over all registry
queries and rotations, per density point (tiered + settled pooled). Flags query ratios outside it."""
import json, math, statistics as S, sys
from collections import defaultdict
FAM = {"P": "hist.single hist.asof", "T": "snap.hop2 reach.window paths.k", "S": "series.count burst.zscore agg.rel_bucket",
       "W": "series.count.w10 agg.rel_bucket.w10 burst.zscore.w10", "D": "diff.global diff.belief", "N": "nbr.evolution coactive.narrow",
       "R": "resolve.substr resolve.exact resolve.prefix", "M": "motif.filtered",
       "B": "b.snap.hop2@T b.snap.hop2@now b.reach@T b.reach@now b.diff b.diff@ctl b.hist b.hist@now"}
FAM = {k: v.split() for k, v in FAM.items()}
rot = defaultdict(list); blk = defaultdict(list)
for f in sys.argv[1:]:
    for l in open(f):
        r = json.loads(l)
        if r.get("refused") or r["query"][:3] in ("nb5", "nb8"):
            continue
        rot[(r["store"], r["query"], r["arm"], r["rot"])] += r["ms"]
        blk[(r["store"], r["query"], r["arm"], r["rot"])].append((r["block"], S.median(r["ms"])))
dens = lambda s: "d10" if "d10" in s else ("d1" if "d1" in s else "d0")
stores = sorted({k[0] for k in rot if k[0] != "native-K1M"})
aa = defaultdict(list)
for (st, q, arm, ro), v in blk.items():
    if arm == "U" and st != "native-K1M":
        v.sort(); aa[dens(st)].append(v[1][1] / v[0][1])
def pct(x, p):
    x = sorted(x); i = (len(x) - 1) * p; lo = int(i); hi = min(lo + 1, len(x) - 1)
    return x[lo] + (x[hi] - x[lo]) * (i - lo)
band = {d: (pct(v, .05), pct(v, .95)) for d, v in aa.items()}
for d in ("d0", "d1", "d10"):
    print(f"A/A band {d}: {band[d][0]:.3f}-{band[d][1]:.3f} (n={len(aa[d])})")
def qr(st, q, arm):
    rs = [S.median(rot[(st, q, arm, ro)]) / S.median(rot[(st, q, "U", ro)]) for ro in (1, 2) if (st, q, arm, ro) in rot]
    if not rs: raise SystemExit(f"missing {st} {q} {arm}")
    return S.median(rs)
for arm in "PT":
    print(f"\narm {arm}: family ratio (geomean of query ratios; P arm's T family without paths.k)")
    print(f"{'store':20s} " + " ".join(f"{f:>5s}" for f in FAM))
    for st in stores:
        row = []
        for f, qs in FAM.items():
            qs = [q for q in qs if not (arm == "P" and q == "paths.k")]
            row.append(math.exp(sum(math.log(qr(st, q, arm)) for q in qs) / len(qs)))
        lo, hi = band[dens(st)]
        print(f"{st:20s} " + " ".join(f"{x:5.2f}{'*' if not lo <= x <= hi else ' '}" for x in row))
    out = []
    for st in stores:
        lo, hi = band[dens(st)]
        for qs in FAM.values():
            for q in qs:
                if arm == "P" and q == "paths.k":
                    continue
                x = qr(st, q, arm)
                if not lo <= x <= hi:
                    out.append(f"{st}:{q}={x:.2f}")
    print(f"query ratios outside the A/A band ({len(out)}):", " ".join(out))
