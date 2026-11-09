#!/usr/bin/env python3
"""rl_eq_check.py --dir POINT --store NATIVE --raphtory R --params P --ceiling S --label L --md M: the fuller form's answer check, untimed. Whole answers where the operator allows: Gnomon's edges read page by page, projected to (src, dst, rel_type); Raphtory's every (src, dst, layer) of the same snapshot's induced subgraph; nodes with hops, whole up to 10,000 (the operator does not page nodes above that, so the first 10,000 by (hop, uid) and the total); gained and lost, and the degree series. A cell not equal, or above the ceiling at the check, is written to the exclusion list under raphtoryeq; the labelled block times a differing cell. Prints every difference with counts. Gnomon's rows carry eid, vid, vt_s, vt_e, and Raphtory's do not: this is the projected comparison, and parallel = rows_total less distinct (src, dst, rel)."""
import argparse
import json
import sys
from collections import Counter
from pathlib import Path

import extcmp as X
import extcmp_raphtory as R


def gn_snap(ad, a):
    p = X.call_operator(ad, "snapshot_subgraph", {**a, "limit": 10000})
    rows = list(p["rows"])
    cur = p.get("cursor")
    while cur:
        n = X.call_operator(ad, "snapshot_subgraph", {**a, "limit": 10000, "cursor": cur})
        rows += n["rows"]
        cur = n.get("cursor")
    return p, rows


def rp_snap(g, a):
    snap = g.snapshot_at(int(a["t_valid"]))
    seen, fr = {}, []
    for s in a["seeds"]:
        if snap.node(str(s)) is not None:
            seen[str(s)] = 0
            fr.append(str(s))
    for h in range(1, int(a["hops"]) + 1):
        nx = set()
        for u in fr:
            nd = snap.node(u)
            if nd is not None:
                nx.update(n.name for n in nd.neighbours)
        new = sorted(u for u in nx if u not in seen)
        for u in new:
            seen[u] = h
        fr = new
        if not fr:
            break
    e = [(x.src.name, x.dst.name, x.layer_name) for x in snap.subgraph(list(seen)).edges.explode_layers()]
    return seen, e


def main():
    ap = argparse.ArgumentParser()
    for k in ("dir", "store", "raphtory", "params", "label", "md"):
        ap.add_argument("--" + k, required=True)
    ap.add_argument("--ceiling", type=float, default=10)
    a = ap.parse_args()
    d = Path(a.dir)
    import raphtory
    st = X.open_existing_store(Path(a.store), "native")
    X.ensure_all_registered()
    g = raphtory.PersistentGraph.load_from_file(a.raphtory)
    ver = {X.cell_key(c): c for c in json.loads((d / "verify-raphtoryeq.json").read_text())["results"]}
    nat = {X.cell_key(c): c for c in json.loads((d / "verify-native.json").read_text())["results"]}
    rows, excl, cnt = [], {}, Counter()
    for inst in json.loads(Path(a.params).read_text())["instances"]:
        q = inst["query"]
        if q not in R.QUERIES_EQ:
            continue
        k = f"{q}#{inst['instance']}"
        args = dict(inst["args"])
        r = {"key": k, "query": q, "instance": inst["instance"], "stratum": inst.get("stratum")}
        c = ver.get(k, {})
        r["raphtoryeq_wall_ms"] = c.get("wall_ms")
        r["native_wall_ms"] = nat.get(k, {}).get("wall_ms")
        diffs = []
        if q == "nbr.evolution":
            gp = X.call_operator(st.adapter, "neighborhood_evolution", {**args, "limit": 10000})
            rp = R.evolution_with_series(g, **args, limit=10000)
            r["gained_gnomon"], r["gained_raphtory"], r["lost_gnomon"], r["lost_raphtory"] = gp["neighbors_gained_total"], rp["gained_total"], gp["neighbors_lost_total"], rp["lost_total"]
            if gp["neighbors_gained"] != rp["gained"] or gp["neighbors_lost"] != rp["lost"]:
                diffs.append("gained or lost lists")
            sd = [(x["t"], x["degree"], y["degree"]) for x, y in zip(gp["degree_series"], rp["degree_series"]) if x["degree"] != y["degree"]]
            r["series_buckets"] = len(gp["degree_series"])
            r["series_buckets_differ"] = len(sd)
            r["series_first_differ"] = sd[:3]
            if len(gp["degree_series"]) != len(rp["degree_series"]) or sd:
                diffs.append("degree series")
            # what the buckets that differ count: Gnomon's rows at the bucket start, and the distinct (src, dst, rel) among them, beside Raphtory's figure
            if sd:
                uid_id = int(st.adapter.dense_ids([args["uid"]])[0])
                eq, sample = 0, []
                for t, gd, rd in sd:
                    e = st.adapter.edges_columnar(as_of_tt=X.OPEN_END if hasattr(X, "OPEN_END") else 4611686018427387904, vt_min=t, vt_max=t + 1, columns=("src_id", "dst_id", "rel_type", "vt_s", "vt_e"), touching_ids=[uid_id])
                    m = (e["vt_s"] <= t) & (e["vt_e"] > t) & ((e["src_id"] == uid_id) | (e["dst_id"] == uid_id))
                    tr = {(int(a_), int(b_), str(c_)) for a_, b_, c_ in zip(e["src_id"][m], e["dst_id"][m], e["rel_type"][m])}
                    eq += len(tr) == rd
                    sample.append([t, gd, int(m.sum()), len(tr), rd])
                r["series_differ_equal_to_distinct_triples"] = eq
                r["series_t_gnomon_rows_distinct_raphtory"] = sample[:3]
        else:
            gp, gr = gn_snap(st.adapter, args)
            seen, re_ = rp_snap(g, args)
            gset, rset = {(x["src"], x["dst"], x["rel_type"]) for x in gr}, set(re_)
            nt = gp["nodes_total"]
            gn = [[x["hop"], x["uid"]] for x in gp["nodes"]]
            rn = sorted([h, u] for u, h in seen.items())
            r.update(nodes_gnomon=nt, nodes_raphtory=len(rn), nodes_whole=nt <= 10000, rows_total_gnomon=gp["rows_total"], rows_read=len(gr), distinct_edges_gnomon=len(gset), edges_raphtory=len(rset), edges_only_gnomon=len(gset - rset), edges_only_raphtory=len(rset - gset), parallel=gp["rows_total"] - len(gset), raphtory_rows_repeat=len(re_) - len(rset))
            if nt != len(rn) or gn != rn[:len(gn)]:
                diffs.append("nodes or hops")
            if gset != rset:
                diffs.append("edges")
        v = "equivalent" if not diffs else "DIFFERS"
        if not c.get("ok"):
            v = f"> {a.ceiling:g} s at answer check" if any(x in (c.get("error") or "") for x in ("imeout", "eiling", "client-side")) else "error"
        elif v == "equivalent" and max(c.get("wall_ms") or 0, nat.get(k, {}).get("wall_ms") or 0) > a.ceiling * 1e3:
            v = f"> {a.ceiling:g} s at answer check"
        r["verdict"], r["diffs"] = v, diffs
        if v != "equivalent":
            excl[k] = f"excluded: {v}"
        cnt[(q, v)] += 1
        rows.append(r)
        print(json.dumps(r)[:400], flush=True)
    st.close()
    (d / "verdicts-raphtoryeq.json").write_text(json.dumps(rows, indent=1, sort_keys=True) + "\n")
    ex = json.loads((d / "excluded.json").read_text()) if (d / "excluded.json").exists() else {}
    ex["raphtoryeq"] = {**excl, **{f"{i['query']}#{i['instance']}": "excluded: this form does not have this query" for i in json.loads(Path(a.params).read_text())["instances"] if i["query"] not in R.QUERIES_EQ}}
    (d / "excluded.json").write_text(json.dumps(ex, indent=1, sort_keys=True) + "\n")
    lab = json.loads((d / "label-only.json").read_text()) if (d / "label-only.json").exists() else {}
    lab["raphtoryeq"] = {r["key"]: "excluded: not a differing cell" for r in rows if r["verdict"] != "DIFFERS"}
    (d / "label-only.json").write_text(json.dumps(lab, indent=1, sort_keys=True) + "\n")
    L = [f"# Fuller form's answer check, {a.label}", "", "| query | verdict | cells |", "|---|---|---:|"] + [f"| {q} | {v} | {n} |" for (q, v), n in sorted(cnt.items())]
    L += ["", "DIFFERS (not passed):"] + [f"- {r['key']} [{r['stratum']}]: {r['diffs']} " + json.dumps({x: r.get(x) for x in ("nodes_gnomon", "nodes_raphtory", "distinct_edges_gnomon", "edges_raphtory", "edges_only_gnomon", "edges_only_raphtory", "parallel", "series_buckets_differ", "series_first_differ", "series_differ_equal_to_distinct_triples", "series_t_gnomon_rows_distinct_raphtory") if x in r}) for r in rows if r["verdict"] == "DIFFERS"]
    Path(a.md).write_text("\n".join(L) + "\n")
    print("\n".join(L[:40]), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
