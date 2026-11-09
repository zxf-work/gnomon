#!/usr/bin/env python3
"""rap_cause.py: untimed check of why Raphtory's reach answers differ from Gnomon's, on the differing cells of one point. Whole node sets, not totals. Nothing here asserts a cause beyond what the printed counts show.
For each chosen cell: Gnomon's full set (the operator, every page) and Raphtory's full set (the algorithm's state, uncut); counts in each direction. A re-computation of Gnomon's rule over the store's currently believed edge versions (tau = max(a, vt_s) < vt_e, tau < t_b), checked against the operator's arrivals node by node.
Eight event-only re-computations over the runs Raphtory's loader writes (one run per coalesced interval of (src, dst, rel)): events at starts or at starts and ends; edges alive at t_a counted as an event at t_a or not; strict or non-strict increase. Each is compared with Raphtory's set.
About ten nodes only in Gnomon's set (md5 order): the path, hop by hop, each hop's interval found again in the frozen log, whether the hop is traversed inside the interval (tau > vt_s), and whether the node has any path traversing every hop at its start. About ten nodes only in Raphtory's set: Raphtory's own chain, with what the store and the log hold for each hop.
DuckDB (SQL) and PostgreSQL: the recorded answer-check cells of the same instance, hash against Gnomon's (the hash covers the first page and the total, not the whole set)."""
import argparse
import glob
import hashlib
import json
from collections import defaultdict
from pathlib import Path

import numpy as np

import extcmp as X
from gnomon.core.model import OPEN_END

INF = np.iinfo(np.int64).max // 2
SALT = "gnomon-c4-raphtory-cause-2026-11-07"


def full_gnomon(ad, src, window):
    rows, cur = [], None
    while True:
        args = {"src": src, "window": dict(window), "limit": 10000}
        if cur:
            args["cursor"] = cur
        r = X.call_operator(ad, "temporal_reachability", args)
        rows += r["rows"]
        if not r.get("truncated") or not r.get("cursor"):
            return {x["uid"]: int(x["earliest_arrival"]) for x in rows}, r.get("rows_total")
        cur = r["cursor"]


def full_raphtory(g, src, window):
    from raphtory import algorithms
    t_a, t_b = int(window["t_a"]), int(window["t_b"])
    w = g.window(t_a, t_b)
    st = algorithms.temporally_reachable_nodes(w, w.count_nodes(), t_a, [str(src)])
    out = {}
    for n, h in st.items():
        ent = [(int(e["0"]), e["1"]) for e in h.get("reachable_nodes", [])]
        if ent and n.name != src:
            out[n.name] = ent
    return out


def fix_wait(src, dst, s, e, n, sid, t_a, t_b):
    arr = np.full(n, INF, dtype=np.int64)
    arr[sid] = t_a
    while True:
        au = arr[src]
        tau = np.maximum(au, s)
        ok = (tau < e) & (tau < t_b) & (au < INF)
        new = arr.copy()
        np.minimum.at(new, dst[ok], tau[ok])
        if np.array_equal(new, arr):
            return arr
        arr = new


def fix_events(es, ed, et, n, sid, start, strict):
    arr = np.full(n, INF, dtype=np.int64)
    arr[sid] = start
    while True:
        au = arr[es]
        ok = (et > au) if strict else (et >= au)
        ok &= au < INF
        new = arr.copy()
        np.minimum.at(new, ed[ok], et[ok])
        if np.array_equal(new, arr):
            return arr
        arr = new


def scan_log(path, keys):
    """every record of the log naming an edge in keys {(src, dst, rel)}: list of raw entries"""
    out = defaultdict(list)
    with open(path) as f:
        f.readline()
        for line in f:
            r = json.loads(line)
            for op in r.get("ops", ()):
                for ev in op.get("events", ()) or ():
                    k = (ev.get("src"), ev.get("dst"), ev.get("rel_type"))
                    if k in keys:
                        out[k].append({"tt": r.get("tt"), "kind": "ingest", "vt_s": ev.get("vt_s"), "vt_e": ev.get("vt_e")})
                ref = op.get("ref")
                if isinstance(ref, dict) and ref.get("kind") == "edge":
                    k = (ref.get("src"), ref.get("dst"), ref.get("rel_type"))
                    if k in keys:
                        out[k].append({"tt": r.get("tt"), "kind": op.get("op"), "disc": ref.get("disc"), "vt_s": op.get("vt_s"), "vt_e": op.get("vt_e"), "t": op.get("t")})
                if op.get("op") == "assert_edge":
                    k = (op.get("src"), op.get("dst"), op.get("rel_type"))
                    if k in keys:
                        out[k].append({"tt": r.get("tt"), "kind": "assert_edge", "disc": op.get("disc"), "vt_s": op.get("vt_s"), "vt_e": op.get("vt_e")})
    return out


def recorded(E, wl, tag, q, inst, native_hash):
    rows = []
    for s in ("duckdbsql", "postgres"):
        found = []
        root = f"{E}/studies/w5/results/W5-wikidata-v2-J49/p1/{tag}" if wl.startswith("w5") else None
        for p in sorted(glob.glob(f"{root}/verify-{s}*.json") if root else glob.glob(f"{E}/final/{wl}/results/*/*/{tag}/verify-{s}*.json")):
            if "smoke" in p and found:
                continue
            for c in json.loads(Path(p).read_text()).get("results", []):
                if c.get("query") == q and c.get("instance") == inst:
                    found.append({"record": p, "ok": c.get("ok"), "hash_equal_gnomon": c.get("hash") == native_hash, "rows": c.get("rows"), "error": (c.get("error") or "")[:100]})
        rows.append({"system": s, "cells": found})
    return rows


def main():
    ap = argparse.ArgumentParser()
    for k in ("wl", "tag", "store", "raphtory", "log", "params", "gate", "E", "out"):
        ap.add_argument("--" + k, required=True)
    ap.add_argument("--hub", type=int, default=2)
    ap.add_argument("--other", type=int, default=1)
    a = ap.parse_args()
    v = json.loads(Path(a.gate, "verdicts-raphtory.json").read_text())
    dif = [r for r in v if r["verdict"] == "DIFFERS"]
    pick, n = [], {}
    for r in sorted(dif, key=lambda r: r["key"]):
        cls = "hub" if r.get("stratum") == "hub" else "other"
        n[(r["query"], cls)] = n.get((r["query"], cls), 0) + 1
        if n[(r["query"], cls)] <= (a.hub if cls == "hub" else a.other):
            pick.append(r)
    print(f"{a.wl} {a.tag}: {len(dif)} differing cells, checking {len(pick)}", flush=True)
    params = {i["instance"]: i for i in json.loads(Path(a.params).read_text())["instances"]}
    nat_hash = {c.get("instance"): c.get("hash") for c in json.loads(Path(a.gate, "verify-native.json").read_text())["results"]}
    st = X.open_existing_store(Path(a.store), "native")
    X.ensure_all_registered()
    ad = st.adapter
    rows = [(e.src, e.dst, e.rel_type, e.disc, int(e.vt_s), int(e.vt_e)) for e in ad.all_edge_versions() if e.tt_e == OPEN_END]
    uids = {}
    idx = lambda u: uids.setdefault(u, len(uids))
    rs = np.fromiter((idx(r[0]) for r in rows), dtype=np.int64, count=len(rows))
    rd = np.fromiter((idx(r[1]) for r in rows), dtype=np.int64, count=len(rows))
    vs = np.fromiter((r[4] for r in rows), dtype=np.int64, count=len(rows))
    ve = np.fromiter((r[5] for r in rows), dtype=np.int64, count=len(rows))
    print(f"{len(rows)} believed edge versions", flush=True)
    # the runs Raphtory's loader writes (extcmp.Raphtory._graph)
    byk = defaultdict(list)
    for r in rows:
        byk[r[:3]].append((r[4], r[5]))
    run = []
    runs_by_key = {}

    def merged_runs(ivs):
        o, cs, ce = [], ivs[0][0], ivs[0][1]
        for s_, e_ in ivs[1:]:
            if s_ <= ce:
                ce = max(ce, e_)
            else:
                o.append((cs, ce))
                cs, ce = s_, e_
        o.append((cs, ce))
        return o
    for k, ivs in byk.items():
        ivs.sort()
        cs, ce = ivs[0]
        for s_, e_ in ivs[1:]:
            if s_ <= ce:
                ce = max(ce, e_)
            else:
                run.append((idx(k[0]), idx(k[1]), cs, ce))
                cs, ce = s_, e_
        run.append((idx(k[0]), idx(k[1]), cs, ce))
        runs_by_key[k] = merged_runs(ivs)
    run = np.array(run, dtype=np.int64)
    del byk
    import raphtory
    g = raphtory.PersistentGraph.load_from_file(a.raphtory)
    cells, want = [], set()
    for r in pick:
        i = params[r["instance"]]
        src, win = i["args"]["src"], i["args"]["window"]
        t_a, t_b = int(win["t_a"]), int(win["t_b"])
        G, total = full_gnomon(ad, src, win)
        R = full_raphtory(g, src, win)
        Rset, Gset = set(R), set(G)
        for u in list(Gset | Rset) + [src]:
            idx(u)
        N = len(uids)
        rev = {v: k for k, v in uids.items()}
        rs2, rd2, vs2, ve2 = rs, rd, vs, ve
        m = (vs2 < t_b) & (ve2 > t_a)
        sid = uids[src]
        arr = fix_wait(rs2[m], rd2[m], vs2[m], ve2[m], N, sid, t_a, t_b)
        W = {rev[i_]: int(arr[i_]) for i_ in np.flatnonzero(arr < INF) if i_ != sid}
        sim_ok = W == G
        rm = (run[:, 2] < t_b) & (run[:, 3] > t_a)
        Ru = run[rm]
        base_s = Ru[:, 2] >= t_a
        var = {}
        for ends in (False, True):
            for alive in (False, True):
                for strict in (True, False):
                    es, ed, et = [Ru[base_s, 0]], [Ru[base_s, 1]], [Ru[base_s, 2]]
                    if ends:
                        me = (Ru[:, 3] >= t_a) & (Ru[:, 3] < t_b)
                        es.append(Ru[me, 0]); ed.append(Ru[me, 1]); et.append(Ru[me, 3])
                    if alive:
                        ma = Ru[:, 2] < t_a
                        es.append(Ru[ma, 0]); ed.append(Ru[ma, 1]); et.append(np.full(int(ma.sum()), t_a, dtype=np.int64))
                    es, ed, et = np.concatenate(es), np.concatenate(ed), np.concatenate(et)
                    for start_name, start in (("start t_a", t_a), ("start t_a - 1", t_a - 1)):

                        ar = fix_events(es, ed, et, N, sid, start, strict)
                        S = {rev[i_] for i_ in np.flatnonzero(ar < INF) if i_ != sid}
                        same_arr = sum(1 for u in S & Rset if int(ar[uids[u]]) == min(x[0] for x in R[u]))
                        var[f"events {'starts+ends' if ends else 'starts'}; alive at t_a {'yes' if alive else 'no'}; {'strict' if strict else 'non-strict'}; {start_name}"] = {
                            "set": S, "n": len(S), "only_variant": len(S - Rset), "only_raphtory": len(Rset - S), "equal_to_raphtory": S == Rset, "arrival_equal_on_common": same_arr}
        n0 = var["events starts; alive at t_a no; non-strict; start t_a"]["set"]
        only_g = sorted(Gset - Rset, key=lambda u: hashlib.md5(f"{SALT}|{r['key']}|{u}".encode()).hexdigest())
        only_r = sorted(Rset - Gset, key=lambda u: hashlib.md5(f"{SALT}|{r['key']}|{u}".encode()).hexdigest())
        # parents over tight edges: breadth first from the source
        au = arr[rs2[m]]
        tau = np.maximum(au, vs2[m])
        ok = (tau < ve2[m]) & (tau < t_b) & (au < INF) & (tau == arr[rd2[m]])
        ids = np.flatnonzero(m)[ok]
        out_e = defaultdict(list)
        for x in ids:
            out_e[int(rs[x])].append(int(x))
        par, fr = {sid: None}, [sid]
        while fr:
            nx = []
            for u in fr:
                for x in out_e.get(u, ()):
                    w_ = int(rd[x])
                    if w_ not in par:
                        par[w_] = x
                        nx.append(w_)
            fr = nx
        samples = []
        for u in only_g[:10]:
            hops, cur = [], uids[u]
            while par.get(cur) is not None:
                x = par[cur]
                row = rows[x]
                rr_ = next((q_ for q_ in runs_by_key.get(row[:3], []) if q_[0] <= int(arr[cur]) < q_[1]), None)
                hops.append({"src": row[0], "dst": row[1], "rel": row[2], "disc": row[3], "vt_s": row[4], "vt_e": row[5], "arrival_at_src": int(arr[rs[x]]), "tau": int(arr[cur]), "inside_interval": int(arr[cur]) > row[4], "run": rr_, "after_run_start": bool(rr_ and int(arr[cur]) > rr_[0])})
                want.add(row[:3])
                cur = int(rs[x])
            hops.reverse()
            samples.append({"node": u, "arrival": G[u], "hops": hops, "hops_inside_interval": sum(h["inside_interval"] for h in hops), "hops_after_run_start": sum(h["after_run_start"] for h in hops), "path_at_starts_exists": u in n0})
        rs_samples = []
        for u in only_r[:10]:
            chain, cur, seen = [], u, set()
            while cur != src and cur in R and cur not in seen:
                seen.add(cur)
                t, frm = min(R[cur])
                ri = [(int(a_), int(b_)) for s_, d_, a_, b_ in run[(run[:, 0] == uids.get(frm, -1)) & (run[:, 1] == uids[cur])]]
                chain.append({"from": frm, "to": cur, "t": t, "runs": ri, "t_at_start": any(t == x[0] for x in ri), "t_at_end": any(t == x[1] for x in ri), "t_inside": any(x[0] < t < x[1] for x in ri), "gnomon_arrival_from": G.get(frm), "gnomon_arrival_to": G.get(cur)})


                cur = frm
            chain.reverse()
            rs_samples.append({"node": u, "raphtory_chain": chain})
        cells.append({"key": r["key"], "query": r["query"], "stratum": r.get("stratum"), "src": src, "window": win, "gnomon_total_op": total, "gnomon_n": len(Gset), "raphtory_n": len(Rset), "both": len(Gset & Rset), "only_gnomon": len(only_g), "only_raphtory": len(only_r), "raphtory_subset_of_gnomon": Rset <= Gset,
                      "rule_recomputation_equals_operator": sim_ok, "variants": {k: {x: y for x, y in d.items() if x != "set"} for k, d in var.items()}, "samples_only_gnomon": samples, "samples_only_raphtory": rs_samples,
                      "recorded_other_systems": recorded(a.E, a.wl, a.tag, r["query"], r["instance"], nat_hash.get(r["instance"]))})
        print(json.dumps({k: cells[-1][k] for k in ("key", "gnomon_n", "raphtory_n", "both", "only_gnomon", "only_raphtory", "rule_recomputation_equals_operator")}), flush=True)
    ev = scan_log(a.log, want) if want else {}
    for c in cells:
        for s in c["samples_only_gnomon"]:
            for h in s["hops"]:
                es = ev.get((h["src"], h["dst"], h["rel"]), [])
                h["log_entries"] = es[:20]
                # an entry's own interval, or its start with a retract's t as the end (a retract closes the version at t), or a later entry's own bounds
                ends = {e.get("t") for e in es if e.get("kind") == "retract"}
                starts = {e.get("t") for e in es if e.get("kind") == "retract"}
                h["interval_in_log"] = any(e.get("vt_s") == h["vt_s"] and (e.get("vt_e") == h["vt_e"] or h["vt_e"] in ends) for e in es if e.get("vt_s") is not None) or (h["vt_s"] in starts and any(e.get("vt_e") == h["vt_e"] for e in es))
    Path(a.out).write_text(json.dumps(cells, indent=1, sort_keys=True, default=str) + "\n")
    st.close()


if __name__ == "__main__":
    main()
