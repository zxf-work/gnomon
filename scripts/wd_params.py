"""W5: curated parameter instances for the registry (16 shared) and belief-time (8) queries on a
frozen Wikidata log, in P1's parameter-file format (extcmp-p1-params/1, read by
`extcmp.py query --params`).

    wd_params.py --log W5-x.eventlog.jsonl --out DIR [--tag SUFFIX]

Curation v2. Selection reads the frozen log only; no system is asked
anything, so no choice depends on Gnomon's (or any system's) answers.
1. Factor tables from the log: per node its out-edges (final belief, and every version with the
   transaction time it was first asserted), degree, correction-touched or -free; cost proxies
   computed from the edge lists (2-hop edges valid at the instant, window out-reach, degree).
2. Strata: correction-touched seeds (an incident edge changed by a revert) and correction-free
   seeds. Within each stratum, picks are nearest the 10/50/90/99th percentiles of the proxy,
   ties and samples by MD5 (P1's `pick_percentiles`).
3. Instants come from the seed's own active span (LDBC style): the seed's out-edge start times,
   sorted, at the anchor percentile (25/50/75); windows and nbr intervals are placed around that
   start; belief instants T are the 10/50/90 % quantiles of the correction batches, and an @T seed
   must hold an edge first asserted before T.
Relation types enter through the seeds (MD5 samples of the subjects of the 5 most frequent
relation types); each instance's class names its relation type and anchor.
"""
from __future__ import annotations

import argparse
import bisect
import collections
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import eval_harness as H  # noqa: E402
import extcmp_p1 as P  # noqa: E402
from gnomon.core.model import OPEN_END, edge_eid  # noqa: E402
from wd_extract import us_of  # noqa: E402

DAY = 86_400_000_000
YEAR = int(365.2425 * DAY)
PCTS = P.PCTS
ANCHORS = (25, 50, 75)


def read_log(log: Path):
    cur, vers, corr, first_tt = {}, collections.Counter(), collections.Counter(), {}
    ctts = []
    with open(log) as f:
        f.readline()
        for line in f:
            r = json.loads(line); tt = r["tt"]; is_c = False
            for op in r["ops"]:
                c = (op.get("provenance_ref") or "").count(":") >= 2
                is_c |= c
                if op["op"] == "ingest_events":
                    evs = op["events"]
                elif op["op"] == "assert_edge":
                    evs = [op]
                else:
                    evs = []
                for e in evs:
                    k = (e["src"], e["dst"], e["rel_type"], e["disc"])
                    cur[k] = (e["vt_s"], e["vt_e"]); vers[k] += 1; corr[k] += c; first_tt.setdefault(k, tt)
                if op["op"] == "retract":
                    rf = op["ref"]; k = (rf["src"], rf["dst"], rf["rel_type"], rf["disc"])
                    if k in cur:
                        if op["t"] <= cur[k][0]:
                            del cur[k]
                        else:
                            cur[k] = (cur[k][0], op["t"])
                    vers[k] += 1; corr[k] += c
            if is_c:
                ctts.append(tt)
    return cur, vers, corr, first_tt, ctts


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--log", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--n-seed", type=int, default=600, help="MD5 seed sample per stratum")
    a = ap.parse_args()
    a.out.mkdir(parents=True, exist_ok=True)
    data = H.dataset_from_log(a.log)
    reg = {q.id: q.args for q in H.registry(data.t0, data.t1, data.tt_epoch1, data.filter_uids)}
    cur, vers, corr, first_tt, ctts = read_log(a.log)
    out_e = collections.defaultdict(list)          # node -> [(vs, ve, dst, rel)] final belief
    in_n = collections.defaultdict(set)
    rel = collections.Counter(); subj_by_rel = collections.defaultdict(set)
    for (s, d, r, _), (vs, ve) in cur.items():
        out_e[s].append((vs, ve, d, r)); in_n[d].add(s); rel[r] += 1; subj_by_rel[r].add(s)
    for s in out_e:
        out_e[s].sort()
    nodes = sorted(set(out_e) | set(in_n))
    deg = {u: len(out_e.get(u, ())) + len(in_n.get(u, ())) for u in nodes}
    touched = {k[0] for k, c in corr.items() if c} | {k[1] for k, c in corr.items() if c}
    first_out = collections.defaultdict(lambda: OPEN_END)    # node -> earliest tt of an out-edge assertion
    for k, t in first_tt.items():
        first_out[k[0]] = min(first_out[k[0]], t)
    top_rel = [r for r, _ in rel.most_common(5)]
    rel_of = {}
    for r in top_rel:
        for u in sorted(subj_by_rel[r], key=lambda u: P.md5("relseed", r, u)):
            rel_of.setdefault(u, r)
    strata = {"correction-touched": sorted((u for u in rel_of if u in touched), key=lambda u: P.md5("node", u))[: a.n_seed],
              "correction-free": sorted((u for u in rel_of if u not in touched), key=lambda u: P.md5("node", u))[: a.n_seed]}
    ctts_s = sorted(ctts)
    T = {f"q{p}": ctts_s[min(len(ctts_s) - 1, int(len(ctts_s) * p / 100))] for p in (10, 30, 50, 70, 90)}

    def anchor(u, p):                                         # start of u's out-edge at percentile p of its starts
        st = [e[0] for e in out_e[u]]
        return st[min(len(st) - 1, int(round((len(st) - 1) * p / 100)))]

    def valid_out(u, t):
        return [e for e in out_e.get(u, ()) if e[0] <= t < e[1]]

    def hop2(u, t):
        one = valid_out(u, t)
        return len(one) + sum(len(valid_out(e[2], t)) for e in one)

    def reach_proxy(u, w):
        fr, seen = [u], {u}
        while fr and len(seen) < 5000:
            nx = []
            for x in fr:
                for vs, ve, d, _ in out_e.get(x, ()):
                    if ve > w["t_a"] and vs < w["t_b"] and d not in seen:
                        seen.add(d); nx.append(d)
            fr = nx
        return len(seen) - 1

    ins, notes = [], []
    lab = lambda st, p, extra="": f"{st} anchor p{p}{extra}"
    # ---- seed queries: per stratum, per anchor, percentile picks of the proxy at the seed's own instant
    for sname, pool in strata.items():
        for p in ANCHORS:
            c = [{"id": u, "value": hop2(u, anchor(u, p)), "t": anchor(u, p), "rel": rel_of[u]} for u in pool]
            for x in P.pick_percentiles(c, PCTS, 1, f"snap{sname}{p}"):
                ins.append(P._inst("snap.hop2", P._with(reg["snap.hop2"], seeds=[x["id"]], t_valid=x["t"]),
                                   f"{sname} {x['stratum']}", lab(x["rel"], p), {"hop2_proxy": x["value"]}))
        for gap in (1, 10):
            c = [{"id": u, "value": deg[u], "t": anchor(u, 50), "rel": rel_of[u]} for u in pool]
            for x in P.pick_percentiles(c, PCTS, 1, f"nbr{sname}{gap}"):
                t1 = max(0, x["t"] - gap * YEAR // 2)
                ins.append(P._inst("nbr.evolution", P._with(reg["nbr.evolution"], uid=x["id"], t1=t1, t2=t1 + gap * YEAR),
                                   f"{sname} {x['stratum']}", lab(x["rel"], 50, f" {gap}y"), {"degree": x["value"]}))
        dc = [{"id": u, "value": deg[u], "rel": rel_of[u]} for u in pool]
        for x in P.pick_percentiles(dc, PCTS, 2, "hist" + sname):
            ins.append(P._inst("hist.single", P._with(reg["hist.single"], uid=x["id"]), f"{sname} {x['stratum']}", x["rel"], {"degree": x["value"]}))
        for tk in ("q10", "q50", "q90"):
            dct = [x for x in dc if first_out[x["id"]] <= T[tk]]
            for x in P.pick_percentiles(dct, (50, 99), 1, "histasof" + sname + tk):
                ins.append(P._inst("hist.asof", P._with(reg["hist.asof"], uid=x["id"], as_of_tt=T[tk]), f"{sname} {x['stratum']}", f"as-of {tk}", {"degree": x["value"]}))
        for wn, wy in (("decade", 10), ("century", 100)):
            c = []
            for u in pool[:300]:
                t = anchor(u, 50); w = {"t_a": max(0, t - wy * YEAR // 2), "t_b": t + wy * YEAR // 2}
                c.append({"id": u, "value": reach_proxy(u, w), "w": w, "rel": rel_of[u]})
            for x in P.pick_percentiles(c, PCTS, 1, f"reach{sname}{wn}"):
                ins.append(P._inst("reach.window", P._with(reg["reach.window"], src=x["id"], window=x["w"]),
                                   f"{sname} {x['stratum']}", lab(x["rel"], 50, f" {wn}"), {"reach_proxy": x["value"]}))
        # paths: u -e1-> v -e2-> w with e2 valid after e1 starts (time-respecting), window spans both starts
        pairs = []
        for u in pool:
            for vs1, ve1, v, _ in out_e[u]:
                nxt = [e for e in out_e.get(v, ()) if e[2] != u and e[1] > vs1 and e[0] < ve1 + 50 * YEAR]
                if nxt:
                    vs2, ve2, w, _ = sorted(nxt, key=lambda e: P.md5("p2", u, e[2]))[0]
                    lo = min(vs1, vs2); hi = max(vs1, vs2) + YEAR
                    pairs.append({"id": f"{u}|{w}", "value": deg[u] + deg[w], "w": {"t_a": lo, "t_b": hi}})
                    break
        for x in P.pick_percentiles(pairs, PCTS, 1, "paths" + sname):
            u, w = x["id"].split("|")
            ins.append(P._inst("paths.k", P._with(reg["paths.k"], src=u, dst=w, window=x["w"]), f"{sname} {x['stratum']}", "time-respecting 2-hop pair", {"deg_sum": x["value"]}))
    # ---- coactive: co-subjects of one object whose edges to it stand in Allen's strict "overlaps"
    #      (a starts first, b starts inside a, a ends inside b), a_spec the earlier one
    pairs = []
    by_obj = collections.defaultdict(list)
    for (s, d, r, _), (vs, ve) in cur.items():
        by_obj[d].append((vs, ve, s))
    for v in sorted(by_obj, key=lambda v: P.md5("co", v))[:20000]:
        es = sorted(by_obj[v])
        hit = next(((es[i], es[j]) for i in range(len(es)) for j in range(i + 1, min(len(es), i + 50))
                    if es[i][0] < es[j][0] < es[i][1] < es[j][1] and es[i][2] != es[j][2]), None)
        if hit:
                x, y = hit[0][2], hit[1][2]
                pairs.append({"id": f"{x}|{y}", "value": len(out_e[x]) * len(out_e[y]),
                              "s": "correction-touched" if (x in touched or y in touched) else "correction-free"})
    pairs = list({c["id"]: c for c in pairs}.values())
    for sname in strata:
        for x in P.pick_percentiles([c for c in pairs if c["s"] == sname], PCTS, 2, "co" + sname):
            u, w = x["id"].split("|")
            ins.append(P._inst("coactive.narrow", P._with(reg["coactive.narrow"], a_spec={"src": u}, b_spec={"src": w}),
                               f"{sname} {x['stratum']}", "co-subjects, overlapping", {"deg_product": x["value"]}))
    # ---- whole-graph queries: a grid (the parameter is the design); instants from the edge starts
    starts = sorted(iv[0] for iv in cur.values() if iv[0] > 0)
    yrs = {p: int(round(P.quantile(starts, p) / YEAR)) for p in (25, 50, 75)}
    y_hi = int(round(P.quantile(starts, 99) / YEAR))
    jan1 = {p: us_of(y, 1, 1) for p, y in yrs.items()}       # exact 1 January (year-precision dates start there)
    wins = {"modern": {"t_a": us_of(1900, 1, 1), "t_b": us_of(y_hi + 1, 1, 1)},
            "century": {"t_a": us_of(yrs[50] - 50, 1, 1), "t_b": us_of(yrs[50] + 50, 1, 1)}}
    for wn, w in wins.items():
        for st in (YEAR, 10 * YEAR):
            l = f"{wn} stride {st // YEAR}y"
            for q in ("series.count", "burst.zscore"):
                ins.append(P._inst(q, P._with(reg[q], window=w, stride=st), "grid", l, {}))
            ins.append(P._inst("agg.rel_bucket", P._with(reg["agg.rel_bucket"], window=w, stride=st, limit=10000), "grid", l, {}))
    for p, t in jan1.items():
        w = {"t_a": t - 2, "t_b": t - 2 + H.W10_WIDTH}
        for q in ("series.count.w10", "agg.rel_bucket.w10", "burst.zscore.w10"):
            ins.append(P._inst(q, P._with(reg[q], window=w), "grid", f"1 Jan {yrs[p]}", {}))
        for gap in (1, 5):
            ins.append(P._inst("diff.global", P._with(reg["diff.global"], t1=t, t2=t + gap * YEAR), "grid", f"{yrs[p]}+{gap}y", {}))
    uids = [str(u) for u in nodes]
    sc = []
    for L in (2, 3, 4, 5):
        for d in sorted(range(10 ** L), key=lambda d: P.md5("s", L, d))[:150]:
            q = str(d).zfill(L)
            sc.append({"id": q, "value": sum(q in x for x in uids)})
    for x in P.pick_percentiles(sc, PCTS, 3, "substr"):
        ins.append(P._inst("resolve.substr", P._with(reg["resolve.substr"], query=x["id"]), x["stratum"], f"len {len(x['id'])}", {"matches": x["value"]}))
    # motif: blocks grown around nodes on a directed 3-cycle when the graph has any (else degree seeds)
    succ = {u: {e[2] for e in es} for u, es in out_e.items()}
    cyc = sorted({x for x in succ for y in succ[x] for z in succ.get(y, ()) if x in succ.get(z, ())},
                 key=lambda u: P.md5("cyc", u))
    notes.append(f"motif: {len(cyc)} nodes lie on a directed 3-cycle in the final graph")
    def block(seed, n):
        seen, fr = [seed], [seed]
        while fr and len(seen) < n:
            nx = []
            for u in fr:
                for v in sorted(succ.get(u, set()) | in_n.get(u, set())):
                    if v not in seen and len(seen) < n:
                        seen.append(v); nx.append(v)
            fr = nx
        return sorted(seen)
    seeds = cyc[:4] if len(cyc) >= 4 else sorted(nodes, key=lambda u: (-deg[u], P.md5("motif", u)))[:4]
    for i, (n, dy) in enumerate(((50, 1), (50, 10), (200, 1), (200, 10))):
        ins.append(P._inst("motif.filtered", P._with(reg["motif.filtered"], node_filter=block(seeds[i], n), delta=dy * YEAR, window=wins["modern"]),
                           "grid", f"block {n} delta {dy}y" + (" (3-cycle seed)" if cyc else ""), {}))
    # ---- belief-time family: tt buckets x strata, seeds active (asserted) before T, instants from their spans
    ed = sorted(vers, key=lambda k: P.md5("edge", *k))
    estr = {"0 corrections": [k for k in ed if corr[k] == 0 and k in cur][:10],
            "1 correction": [k for k in ed if corr[k] == 1][:5],
            "several corrections": [k for k in ed if corr[k] >= 2][:5]}
    for l, ks in estr.items():
        for k in ks:
            ent = {"eid": edge_eid(*k), "src": k[0], "dst": k[1], "rel_type": k[2], "disc": k[3]}
            ins.append(P._inst("b.hist", ent, l, l, {"versions": vers[k]}))
            ins.append(P._inst("b.hist@now", dict(ent), l, l, {"versions": vers[k]}))
    for sname, pool in strata.items():
        for tk in ("q10", "q50", "q90"):
            act = [u for u in pool if first_out[u] <= T[tk]]
            seeds = act[:20]
            if not seeds:
                notes.append(f"b.*@T {sname} {tk}: no seed asserted before T"); continue
            t = sorted(anchor(u, 50) for u in seeds)[len(seeds) // 2]
            ins.append(P._inst("b.snap.hop2@T", {"seeds": seeds, "hops": 2, "t_valid": t, "limit": 10000, "as_of_tt": T[tk]},
                               sname, f"as-of {tk}", {"T": tk}))
            for u in act[20:22]:
                t = anchor(u, 50)
                ins.append(P._inst("b.reach@T", {"src": u, "window": {"t_a": max(0, t - 50 * YEAR), "t_b": t + 50 * YEAR}, "as_of_tt": T[tk]},
                                   sname, f"as-of {tk}", {"T": tk}))
        seeds = pool[:20]; t = sorted(anchor(u, 50) for u in seeds)[len(seeds) // 2]
        ins.append(P._inst("b.snap.hop2@now", {"seeds": seeds, "hops": 2, "t_valid": t, "limit": 10000}, sname, "now", {}))
        for u in pool[22:25]:
            t = anchor(u, 50)
            ins.append(P._inst("b.reach@now", {"src": u, "window": {"t_a": max(0, t - 50 * YEAR), "t_b": t + 50 * YEAR}}, sname, "now", {}))
    dwin = {"decade": {"t_a": jan1[50], "t_b": us_of(yrs[50] + 10, 1, 1)}, "modern": wins["modern"]}
    for tk in T:
        for wn, w in dwin.items():
            ins.append(P._inst("b.diff", {"t1": T[tk], "t2": OPEN_END, "window": w, "limit": 1000}, f"tt {tk}", wn, {"T": tk}))
    for wn, w in list(dwin.items()) + [("century", wins["century"])]:
        ins.append(P._inst("b.diff@ctl", {"t1": w["t_a"], "t2": w["t_b"]}, "grid", wn, {}))
    F = {"curation": "w5-v2 (per-seed instants; log-only selection)", "nodes": len(nodes), "edges_final": len(cur),
         "edges_ever": len(vers), "rel_top": rel.most_common(25), "years": {f"y{p}": y for p, y in yrs.items()}, "y_hi": y_hi,
         "T_points": T, "correction_batches": len(ctts), "touched_nodes": len(touched),
         "strata_sizes": {k: len(v) for k, v in strata.items()}, "cycle_nodes": len(cyc)}
    seen, uniq = set(), []
    for i in ins:                         # a seed with one out-edge has one instant at every anchor
        k = (i["query"], i["instance"])
        if k not in seen:
            seen.add(k); uniq.append(i)
    notes.append(f"{len(ins) - len(uniq)} duplicate instances (identical arguments) dropped")
    ins = uniq
    tag = a.log.name.split(".")[0]
    (a.out / f"factors-{tag}.json").write_text(json.dumps(F, indent=1) + "\n")
    facts = {"path": a.log.name, "sha256": P.sha256_file(a.log)}
    nts = notes + ["curation w5-v2: selection reads the frozen log only (no system is queried); instants from each seed's "
                   "own out-edge starts at anchors p25/p50/p75; strata correction-touched / correction-free; "
                   f"seeds: MD5 samples of the subjects of the top relation types {top_rel}"]
    P._write(a.out / f"params-{tag}-reg.json", facts, [i for i in ins if not i["query"].startswith("b.")], nts)
    P._write(a.out / f"params-{tag}-bel.json", facts, [i for i in ins if i["query"].startswith("b.")], nts)
    print(json.dumps(F, indent=1)[:2500])
    return 0


if __name__ == "__main__":
    sys.exit(main())
