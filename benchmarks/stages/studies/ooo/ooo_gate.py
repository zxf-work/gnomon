#!/usr/bin/env python3
"""Out-of-order logs: the answer gate. ooo_gate.py --res <results dir> --tags base,tag,... --base-tag t --out gate.md [--recorded a b ...]
Runs after extcmp_p1.py gate wrote <res>/<tag>/verdicts.json. Exit 7 on any differing answer between Gnomon and DuckDB (SQL) on one log, on a Gnomon
error that is neither a refusal nor a ceiling, or on an original-log answer that differs from the W1 stage's record. Listed, never failed: cells at the ceiling, refusals,
and how each re-ordered log's answers compare with the original's (they may differ where belief time or a node's first arrival matter; DuckDB's rows come
out of the Gnomon store, so the same-rows comparison with the original log is the check that does not rest on the store itself)."""
import argparse
import glob
import json
import os
import sys
from collections import Counter


def cells(d, sysname):
    out = {}
    for f in sorted(glob.glob(os.path.join(d, f"verify-{sysname}*.json"))):
        rec = json.load(open(f))
        for c in rec.get("results", []):
            out[(c["query"], c.get("instance"))] = c
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--res", required=True)
    ap.add_argument("--tags", required=True)
    ap.add_argument("--base-tag", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--recorded", nargs="*", default=[])
    ap.add_argument("--recorded-tag", default="")
    a = ap.parse_args()
    tags = a.tags.split(",")
    L, fail = ["# Out-of-order logs: answer gate", ""], []
    N = {t: cells(os.path.join(a.res, t), "native") for t in tags}
    for t in tags:
        d = os.path.join(a.res, t)
        duck = cells(d, "duckdbsql")
        ver = {}
        vf = os.path.join(d, "verdicts.json")
        if not os.path.exists(vf):
            fail.append(f"{t}: no verdicts.json")
        else:
            for sysname, rows in json.load(open(vf)).items():
                for r in rows:
                    ver[(sysname, r["query"], r.get("instance"))] = r
        c = Counter()
        for k, n in N[t].items():
            if not n.get("ok"):
                e = str(n.get("error", ""))
                if n.get("refused") or "refused" in e or "E_COST" in e:
                    c["Gnomon refused"] += 1
                    L.append(f"- {t} {k}: Gnomon refused: {e[:120]}")
                elif "timeout" in e or " s, aborted" in e:
                    c["Gnomon at the ceiling"] += 1
                    L.append(f"- {t} {k}: Gnomon at the answer-check ceiling: {e[:120]}")
                else:
                    fail.append(f"{t} {k}: Gnomon error: {e[:200]}")
                continue
            c["Gnomon answered"] += 1
            d_ = duck.get(k)
            if d_ is None:
                c["native only"] += 1
                continue
            if not d_.get("ok"):
                c["DuckDB not answered: " + str(d_.get("error", ""))[:40]] += 1
                L.append(f"- {t} {k}: DuckDB (SQL) did not answer: {str(d_.get('error', ''))[:120]}")
                continue
            if d_.get("hash") == n.get("hash") and d_.get("rows") == n.get("rows"):
                c["same answer as DuckDB (SQL)"] += 1
            else:
                fail.append(f"{t} {k}: Gnomon and DuckDB (SQL) differ: hash {n.get('hash')} rows {n.get('rows')} against hash {d_.get('hash')} rows {d_.get('rows')}")
        for (s, q, i), r in ver.items():
            if "DIFFERS" in str(r.get("verdict")):
                fail.append(f"{t} {q}#{i}: verdict {r.get('verdict')} for {s}")
        L += ["", f"## {t}", "", ", ".join(f"{k} {v}" for k, v in sorted(c.items())), ""]
    # the original log's answers against the W1 stage's record, by query and args-independent hash where the instance is a registry one
    rec = {}
    for f in a.recorded:
        for c in json.load(open(f)).get("results", []):
            if c.get("ok"):
                rec[c["query"]] = c
    base = N[a.recorded_tag or a.base_tag]
    n_same = 0
    if rec:
        for (q, i), c in base.items():
            if q in rec and c.get("ok") and c.get("stratum") == "registry instance":
                if c.get("hash") == rec[q].get("hash"):
                    n_same += 1
                else:
                    fail.append(f"{a.recorded_tag or a.base_tag} {q}: hash {c.get('hash')} differs from the W1 record {rec[q].get('hash')}")
        L += [f"Original log against the W1 record: {n_same} registry instances identical.", ""]
    base = N[a.base_tag]
    L += ["## Re-ordered logs against the original (information, no stop)", "", "| log | query | instance | rows original | rows here | same rows and values (vid and belief times left out) |", "|---|---|---|---:|---:|---|"]
    for t in tags:
        if t == a.base_tag:
            continue
        for k, c in sorted(N[t].items(), key=lambda x: (x[0][0], str(x[0][1]))):
            b = base.get(k)
            if not b or not b.get("ok") or not c.get("ok"):
                continue
            L.append(f"| {t} | {k[0]} | {k[1]} | {b.get('rows')} | {c.get('rows')} | {'yes' if b.get('proj_hash') == c.get('proj_hash') else 'no'} |")
    # the test of the node rule: an instance whose belief cut is inside the ingest may differ by construction; any other that differs is unexplained
    def inside(q, c):
        return c.get("stratum") == "belief cut inside the ingest" or q in ("hist.asof", "diff.belief")
    L += ["", "## Against the base log, by expectation", "", "| log | instances | same | differ, belief cut inside the ingest (expected to be able to differ) | differ, UNEXPLAINED |", "|---|---:|---:|---:|---|"]
    unexpl = {}
    for t in tags:
        if t == a.base_tag:
            continue
        same = ins = 0
        un = []
        tot = 0
        for k, c in sorted(N[t].items(), key=lambda x: (x[0][0], str(x[0][1]))):
            b = base.get(k)
            if not b or not b.get("ok") or not c.get("ok"):
                un.append(f"{k[0]}#{k[1]} (not answered)")
                tot += 1
                continue
            tot += 1
            if b.get("proj_hash") == c.get("proj_hash"):
                same += 1
            elif inside(k[0], c):
                ins += 1
            else:
                un.append(f"{k[0]}#{k[1]} rows {b.get('rows')} against {c.get('rows')}")
        unexpl[t] = un
        L.append(f"| {t} | {tot} | {same} | {ins} | {len(un)}: {'; '.join(un) or 'none'} |")
    L += ["", "## Verdict", "", f"FAIL {len(fail)}" if fail else "PASS", ""] + [f"- **FAIL** {x}" for x in fail]
    with open(a.out, "w") as f:
        f.write("\n".join(L) + "\n")
    print("\n".join(L[-1 - len(fail):]))
    return 7 if fail else 0


if __name__ == "__main__":
    sys.exit(main())
