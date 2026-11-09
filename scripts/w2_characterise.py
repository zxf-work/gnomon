#!/usr/bin/env python3
"""W2: characterise a frozen synthetic event log's degree distribution.

Reads the edge events of the log (the ingested edges; corrections and the
retraction do not add edges) and reports, for the cumulative multigraph
(every edge version ever written) and for the instant snapshot at the middle
of the valid-time range (edges with vt_s <= t < vt_e):

* total / out / in degree: max, mean, p50/p90/p99/p99.9;
* the discrete power-law MLE exponent (Clauset et al. 2009, eq. 3.7
  approximation) for k_min in {5, 10, 20, 50};
* hubs: the top 20 nodes by cumulative degree, the share of edge endpoints on
  the top 0.1 % / 1 % / 10 % of nodes, distinct neighbours of the top hubs at t;
* the degree histogram (log2 bins) for plotting.

    python -I scripts/w2_characterise.py --log X.eventlog.jsonl --json out.json
"""
from __future__ import annotations

import argparse
import json
import math
from collections import Counter, defaultdict


def pct(sorted_vals, q):
    if not sorted_vals:
        return 0
    return sorted_vals[min(len(sorted_vals) - 1, int(q * len(sorted_vals)))]


def mle(degs, kmin):
    xs = [d for d in degs if d >= kmin]
    if len(xs) < 10:
        return None
    s = sum(math.log(d / (kmin - 0.5)) for d in xs)
    return {"k_min": kmin, "n_tail": len(xs), "gamma": round(1 + len(xs) / s, 3)}


def summary(cnt: Counter, n_nodes: int):
    degs = sorted(list(cnt.values()) + [0] * (n_nodes - len(cnt)))
    tot = sum(degs) or 1
    desc = sorted(degs, reverse=True)
    share = {}
    for f in (0.001, 0.01, 0.1):
        k = max(1, int(f * n_nodes))
        share[f"top_{f*100:g}pct"] = round(sum(desc[:k]) / tot, 4)
    hist = Counter(0 if d == 0 else int(math.log2(d)) + 1 for d in degs)
    return {"max": desc[0], "mean": round(tot / n_nodes, 3),
            "p50": pct(degs, .5), "p90": pct(degs, .9), "p99": pct(degs, .99),
            "p999": pct(degs, .999), "zero_degree_nodes": degs.count(0),
            "endpoint_share": share,
            "mle": [m for m in (mle(degs, k) for k in (5, 10, 20, 50)) if m],
            "log2_hist": {("0" if b == 0 else f"[{2**(b-1)},{2**b})"): hist[b]
                          for b in sorted(hist)}}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--log", required=True)
    ap.add_argument("--json", required=True)
    ap.add_argument("--n-nodes", type=int, default=0,
                    help="|V| (default scale/100, the generator's rule)")
    a = ap.parse_args()
    tot, out, inn = Counter(), Counter(), Counter()
    edges = []
    lo, hi = None, None
    with open(a.log, encoding="utf-8") as f:
        f.readline()
        for line in f:
            rec = json.loads(line)
            for op in rec.get("ops", []):
                for ev in op.get("events", []):
                    s, d = ev["src"], ev["dst"]
                    out[s] += 1; inn[d] += 1; tot[s] += 1; tot[d] += 1
                    edges.append((s, d, ev["vt_s"], ev["vt_e"]))
                    lo = ev["vt_s"] if lo is None else min(lo, ev["vt_s"])
                    hi = ev["vt_s"] if hi is None else max(hi, ev["vt_s"])
    m = len(edges)
    n = a.n_nodes or max(200, m // 100)
    t = (lo + hi) // 2
    itot, nb = Counter(), defaultdict(set)
    for s, d, vs, ve in edges:
        if vs <= t < ve:
            itot[s] += 1; itot[d] += 1
            nb[s].add(d); nb[d].add(s)
    hubs = [{"uid": u, "degree": k, "out": out[u], "in": inn[u],
             "instant_degree": itot[u], "instant_distinct_neighbours": len(nb[u])}
            for u, k in tot.most_common(20)]
    res = {"log": a.log, "edge_events": m, "n_nodes": n, "t_mid": t,
           "instant_live_edges": sum(itot.values()) // 2,
           "cumulative": {"total": summary(tot, n), "out": summary(out, n),
                          "in": summary(inn, n)},
           "instant": {"total": summary(itot, n)},
           "hubs_top20": hubs}
    with open(a.json, "w") as f:
        json.dump(res, f, indent=1)
    c, i = res["cumulative"]["total"], res["instant"]["total"]
    print(json.dumps({"edges": m, "n": n, "cum_max": c["max"], "cum_mean": c["mean"],
                      "cum_mle": c["mle"], "inst_max": i["max"], "inst_mean": i["mean"],
                      "inst_mle": i["mle"], "share": c["endpoint_share"],
                      "top3": hubs[:3]}, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
