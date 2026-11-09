#!/usr/bin/env python3
"""classcount_4points.py: per-query won/lost counts by class (T, B, G) at the four 100M points, cz_classcount.py's rule, from records only.
Rule (cz_classcount.py, unchanged): per query, ratio median = median over instances of Gnomon / best other (instances where both answered);
won = median < 1, or no ratio and some instance where Gnomon answered and every other system was > 10 s, with no Gnomon refusal;
otherwise lost (Gnomon refused with no ratio, a median >= 1, or Gnomon > 10 s where another answered); queries with no comparison
(no ratio, no censoring either way, no refusal) are not counted. Values as x_summary.py (mean over 2 rotations of each rotation's p50;
"> 10 s" if censored in either rotation; a refusal has no time)."""
import json
import statistics as st
import sys
from collections import defaultdict
from pathlib import Path

sys.path[:0] = [__import__("os").environ.get("GNOMON_BENCH_ROOT", ".") + "/studies/s100m-a/src/benchmarks/stages/studies/t10/tools"]
import x_summary as XS      # noqa: E402
import extcmp as X          # noqa: E402

N4 = __import__("os").environ.get("GNOMON_BENCH_ROOT", ".") + "/studies"
POINTS = [("W1 K100M-d0", "K100M-d0", [f"{N4}/k100m-logsrc/results/merged4-J60"]),
          ("W1 K100M-d10", "K100M-d10", [f"{N4}/s100m-a/results/timed-s100m-J62", f"{N4}/s100m-a/results/timed-s100m-J64"]),
          ("W2-100M-d0", "W2-100M-d0", [f"{N4}/s100m-a/results/timed-s100m-J61", f"{N4}/s100m-a/results/timed-s100m-J65"]),
          ("W2-100M-d10", "W2-100M-d10", [f"{N4}/s100m-a/results/timed-s100m-J63", f"{N4}/s100m-a/results/timed-s100m-J66"])]
NATIVE_ONLY = sorted({q for qs in X.NATIVE_ONLY_BY_FAMILY.values() for q in qs})


def per_query(tag, dirs):
    S = defaultdict(dict)
    for d in dirs:
        for s, m in XS.read_root(Path(d) / "reg" / tag).items():
            S[s].update(m)
    Q = defaultdict(lambda: {"r": [], "oc": 0, "gc": 0, "ref": 0, "n": 0})
    for k, e in S["native"].items():
        q = e["query"]; c = Q[q]; c["n"] += 1
        g = XS.value(e)
        vals = {s: XS.value(S[s][k]) for s in ("duckdbsql", "postgres") if k in S.get(s, {})}
        nv = [v for v in vals.values() if XS.num(v)]
        if g == XS.REF:
            c["ref"] += 1
        elif XS.num(g) and nv:
            c["r"].append(g / min(nv))
        elif XS.num(g) and any(v == XS.CENS for v in vals.values()):
            c["oc"] += 1
        elif g == XS.CENS and nv:
            c["gc"] += 1
    return Q


def classify(x):
    """x: ratio_median (or None), oc, gc, ref -> (counted, won, reason)"""
    r, oc, gc, ref = x
    if r is None and not oc and ref == 0 and not gc:
        return False, None, "no comparison"
    won = (r is not None and r < 1) or (r is None and oc > 0 and ref == 0)
    if won:
        return True, True, ""
    if r is None and ref:
        return True, False, "refused"
    if r is None and gc:
        return True, False, "Gnomon > 10 s, another system answered"
    return True, False, "median ratio"


def count(rows):
    out = {}
    for cl in "TBG":
        rs = [x for x in rows if x[0] == cl and x[2]]
        out[cl] = (sum(1 for x in rs if x[3]), len(rs))
    return out


L = ["# Per-query won/lost counts by class at the four 100M points", "",
     "Rule: tables/classcount_4points.py docstring (cz_classcount.py's rule, unchanged). Best other: the faster of DuckDB (SQL) 64 and PostgreSQL",
     "only, at every point. Records only, no job run. W1 K100M-d0: k100m-logsrc/results/merged4-J60 (base instance from k100m timed J56,",
     "3 instances from covz timed J60, two jobs on two nodes). The other three: s100m-a phase-1 + phase-2 timed records (two jobs, two nodes",
     "each): K100M-d10 J62 + J64, W2-100M-d0 J61 + J65, W2-100M-d10 J63 + J66. 4 instances per query at every point.", ""]
summary_rows, lost_rows = [], []
for name, tag, dirs in POINTS:
    Q = per_query(tag, dirs)
    rows = []
    for q, c in Q.items():
        r = st.median(c["r"]) if c["r"] else None
        counted, won, why = classify((r, c["oc"], c["gc"], c["ref"]))
        rows.append((XS.REG.get(q, "?"), q, counted, won, why, r, len(c["r"]), c["n"], c["ref"], c["oc"], c["gc"]))
        if counted and not won:
            det = f"refused on {c['ref']} of {c['n']} instances" if why == "refused" else (
                f"median {r:.3g} over {len(c['r'])} instances with a ratio (of {c['n']}; refused {c['ref']})" if why == "median ratio" else why)
            lost_rows.append(f"| {name} | {XS.REG.get(q, '?')} | {q} | {det} |")
    cnt = count(rows)
    summary_rows.append((name, cnt))
L += ["## Counts (Gnomon won n of N counted queries; best other = DuckDB (SQL) 64 or PostgreSQL)", "",
      "| point | T | B | G |", "|---|---|---|---|"]
for name, cnt in summary_rows:
    L.append(f"| {name} | " + " | ".join(f"{cnt[c][0]} of {cnt[c][1]}" for c in "TBG") + " |")
# K100M-d0 with all its systems: the same rule over the summary.json of the record (best other there = DuckDB (SQL) best of 64 and 32 threads, ClickHouse, PostgreSQL)
def from_summary(path, tag):
    J = json.load(open(path))[tag]["queries"]
    rows = []
    for q, x in J.items():
        counted, won, why = classify((x["ratio_median"], x["other_all_censored_gnomon_ok"], x["gnomon_censored_other_ok"], x["gnomon_refused"]))
        rows.append((x["class"], q, counted, won, why, x["ratio_median"], x["ratio_n"], x["instances"], x["gnomon_refused"]))
    return rows
all4 = from_summary(f"{N4}/k100m-logsrc/results/merged4-J60/summary/summary.json", "K100M-d0")
all1 = from_summary(f"{N4}/k100m/results/timed-k100m-J56/summary/summary.json", "K100M-d0")
c4, c1 = count(all4), count(all1)
L += ["", "W1 K100M-d0 with all its systems (best other = DuckDB (SQL) better of 64 and 32 threads, ClickHouse, PostgreSQL), same rule:", "",
      "| record | T | B | G |", "|---|---|---|---|",
      f"| 4 instances, merged4-J60/summary/summary.json | " + " | ".join(f"{c4[c][0]} of {c4[c][1]}" for c in "TBG") + " |",
      f"| 1 instance, k100m timed J56 (the recorded 12 of 14 T, 6 of 9 B) | " + " | ".join(f"{c1[c][0]} of {c1[c][1]}" for c in "TBG") + " |", ""]
diffq = []
rk = {r[1]: r for r in all4}
for name, tag, dirs in POINTS[:1]:
    Q = per_query(tag, dirs)
    for q, c in Q.items():
        r = st.median(c["r"]) if c["r"] else None
        a = classify((r, c["oc"], c["gc"], c["ref"]))
        b = rk.get(q)
        if b and (a[0], a[1]) != (b[2], b[3]):
            diffq.append(f"- {q}: restricted {'won' if a[1] else 'lost' if a[0] else 'not counted'} (median {r if r is None else round(r, 3)}), all systems {'won' if b[3] else 'lost' if b[2] else 'not counted'} (median {b[5] if b[5] is None else round(b[5], 3)})")
L += ["Queries whose outcome at K100M-d0 changes with the restriction:", ""] + (diffq or ["- none"]) + [""]
L += ["## Lost queries and the reason", "", "| point | class | query | reason |", "|---|---|---|---|"] + lost_rows + [""]
lost_all = [f"- {q} ({cl}): " + ("refused" if why == "refused" else f"median {r:.3g} over {rn} instances") for cl, q, counted, won, why, r, rn, n, ref in all4 if counted and not won]
L += ["W1 K100M-d0 with all its systems, lost: " if lost_all else "", *lost_all, ""]
L += ["## Queries with no cells to compare at 100M", "",
      f"{', '.join(NATIVE_ONLY)}: the kit's Gnomon-only ids (extcmp.NATIVE_ONLY_BY_FAMILY), timed for Gnomon only; DuckDB (SQL), PostgreSQL (and ClickHouse)",
      "do not run them, so they have no ratio, no censoring and no refusal (\"- [0; 0]\") and are not counted. Gnomon answered them on every instance at all four points.", ""]
chk = []
for name, tag, dirs in POINTS:
    Q = per_query(tag, dirs)
    nc = sorted(q for q, c in Q.items() if not c["r"] and not c["oc"] and not c["gc"] and not c["ref"])
    chk.append(f"- {name}: not counted {nc}")
L += chk
print("\n".join(L))
