#!/usr/bin/env python3
"""
    x_extra.py --log LOCAL_LOG --side DIR --base params-K100M-d0.json --sums FILE --out DIR [--tag K100M-d0]

The single-parameter file (x_params.py k100m) holds one instance per id: the registry's own arguments, derived from the log by
dataset_from_log and derive_params. Those arguments fix a few anchors by convention: the node pair (n1, n2), the seed block n1..n20,
the motif filter (the first 200 sources in lexicographic order), the belief edge e* (the first edge of [t* - L/2, t*) in log order that
no base correction touches and that is selected at 1 %), and, where no node is named, an instant or window of the extent.
Instance j (j = 1, 2, 3) is the same template on the same log with each anchor moved to its j-th next value under the same convention:
  N  node pair (n1, n2) -> (n{2j+1}, n{2j+2}); a single node is the pair's first; resolve.substr / resolve.exact query "n1" -> "n{2j+1}"
  S  seed block n1..n20 -> n{20j+1}..n{20j+20}
  F  motif filter block 0 -> block j of 200 in the same lexicographic order of sources (block 0 is asserted equal to the registry's)
  E  e* -> the (j+1)-th edge meeting e*'s condition, in log order (the first is asserted equal to the side file's e*)
  P  resolve.prefix "alp" (3 characters of n1's name, the only named node) -> the first 3 + j characters of that name
  T  no node in the arguments, a positioned instant or window (diff.global t1 at the extent's middle; the 10-tick windows of the .w10
     ids and diff.belief at t0 + 1000; b.diff / b.diff@ctl around t* = t0 + 3/4 of the extent): the anchor moved by j eighths of the
     extent toward the far end (left from an anchor at or past the middle, right otherwise); widths, gaps and strides unchanged
  W  whole-extent windows (series.count, burst.zscore, agg.rel_bucket): no anchor to move; the extent's halves early, middle, late
     (the kit design's 50 % windows) at the registry's bucket count (stride = width // 100)
Times other than the anchor (t_valid, as_of_tt, T, T_base, the reach and diff windows of node-anchored ids) stay the registry's.
Instance id = md5("w1-single-v1", query, canonical args)[:8], as in the base file, so an equal call has an equal id.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

import extcmp as X
import extcmp_p1 as P1
import eval_harness as H
import extcmp_beliefq as B
from gnomon.core.model import edge_eid

VERSION = "w1-single-v1"
NODE_IDS = {"hist.single", "hist.asof", "snap.hop2", "reach.window", "paths.k", "nbr.evolution", "coactive.narrow",
            "b.reach@T", "b.reach@now"}


def nn(i: int) -> str:
    return f"n{i}"


def scan(log: Path, t_star: int, L: int, want: int):
    """One pass over the log: e*'s candidates in log order (derive_params' rule, the first `want` that qualify) and the sources."""
    cands, touched, srcs = [], set(), set()
    with open(log) as f:
        for line in f:
            rec = json.loads(line)
            for op in rec.get("ops", []):
                kind = op.get("op")
                for ev in op.get("events", []):      # dataset_from_log: the sources of every op's events
                    srcs.add(ev["src"])
                if kind == "ingest_events":
                    off = op.get("offset", 0)
                    for i, ev in enumerate(op["events"]):
                        vs = ev["vt_s"]
                        if "tt" in rec and t_star - L // 2 <= vs < t_star and len(cands) < 50_000:
                            disc = ev.get("disc", f"#{off + i}")
                            e = edge_eid(ev["src"], ev["dst"], ev["rel_type"], disc)
                            cands.append((e, {"eid": e, "src": ev["src"], "dst": ev["dst"], "rel_type": ev["rel_type"], "disc": disc,
                                              "vt_s": vs, "vt_e": ev.get("vt_e") or vs + 1}))
                elif kind in ("correct", "retract") and "tt" in rec:
                    if "w5_correction_seed" not in (op.get("props") or {}):
                        e = B._ref_eid(op["ref"])
                        if e:
                            touched.add(e)
    q = [c for e, c in cands if e not in touched and B.selected(e, B.SEED, 1.0)][:want]
    return q, sorted(srcs)


def shift(anchor: int, j: int, t0: int, span: int, mid: int) -> int:
    step = j * span // 8
    return anchor - step if anchor >= mid else anchor + step


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--log", type=Path, required=True); ap.add_argument("--base", type=Path, required=True)
    ap.add_argument("--sums", type=Path, required=True); ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--tag", default="K100M-d0")
    a = ap.parse_args()
    base = json.loads(a.base.read_text())
    data = X.dataset(a.log)                      # the side files beside the node-local log (copied from the base stage's params/side)
    bp = B.derive_params(a.log, data.scale, data.t0, data.t1)
    t0, t1 = data.t0, data.t1
    span, mid = t1 - t0, t0 + (t1 - t0) // 2
    ds = base["selection"]["dataset"]
    assert (ds["t0"], ds["t1"], ds["tt_epoch1"]) == (t0, t1, data.tt_epoch1), "side file differs from the base file's dataset"
    reg = {i["query"]: i["args"] for i in base["instances"]}
    estars, srcs = scan(a.log, bp["t_star"], bp["L"], 4)
    assert estars and estars[0]["eid"] == bp["e_star"]["eid"], f"e* rule does not reproduce the side file's e*: {estars[:1]} vs {bp['e_star']}"
    assert len(estars) == 4, f"only {len(estars)} edges meet e*'s condition"
    assert tuple(srcs[:200]) == tuple(data.filter_uids), "motif filter rule does not reproduce the registry's block"
    assert len(srcs) >= 800, "fewer than 4 blocks of 200 sources"
    insts, notes = [], []
    for q, r in reg.items():
        for j in (1, 2, 3):
            x = json.loads(json.dumps(r))
            if q in NODE_IDS:
                p, s2 = nn(2 * j + 1), nn(2 * j + 2)
                for k in ("uid", "src"):
                    if k in x:
                        x[k] = p
                if "dst" in x:
                    x["dst"] = s2
                if "seeds" in x:
                    x["seeds"] = [p]
                if "a_spec" in x:
                    x["a_spec"] = {"src": p}; x["b_spec"] = {"src": s2}
                rule = "N"
            elif q in ("resolve.substr", "resolve.exact"):
                assert x["query"] == "n1"; x["query"] = nn(2 * j + 1); rule = "N"
            elif q == "resolve.prefix":
                name = "alpha-corrected"
                assert name.startswith(x["query"]) and len(x["query"]) == 3
                x["query"] = name[: 3 + j]; rule = "P"
            elif q in ("b.snap.hop2@T", "b.snap.hop2@now"):
                assert x["seeds"] == [nn(i) for i in range(1, 21)]
                x["seeds"] = [nn(20 * j + i) for i in range(1, 21)]; rule = "S"
            elif q == "motif.filtered":
                assert x["node_filter"] == list(data.filter_uids)
                x["node_filter"] = srcs[200 * j: 200 * j + 200]; rule = "F"
            elif q in ("b.hist", "b.hist@now"):
                e = estars[j]
                for k in ("eid", "src", "dst", "rel_type", "disc"):
                    x[k] = e[k]
                rule = "E"
            elif q == "diff.global":
                assert x["t1"] == mid
                gap = x["t2"] - x["t1"]; x["t1"] = shift(x["t1"], j, t0, span, mid); x["t2"] = x["t1"] + gap; rule = "T"
            elif q in ("series.count.w10", "burst.zscore.w10", "agg.rel_bucket.w10", "diff.belief", "b.diff"):
                w = x["window"]; ta = shift(w["t_a"], j, t0, span, mid) if q != "b.diff" else None
                if q == "b.diff":       # centred on t*: the centre moves
                    c = (w["t_a"] + w["t_b"]) // 2; c2 = shift(c, j, t0, span, mid); ta = w["t_a"] + (c2 - c)
                x["window"] = {"t_a": ta, "t_b": ta + (w["t_b"] - w["t_a"])}; rule = "T"
            elif q == "b.diff@ctl":
                c = (x["t1"] + x["t2"]) // 2; c2 = shift(c, j, t0, span, mid); d = c2 - c
                x["t1"] += d; x["t2"] += d; rule = "T"
            elif q in ("series.count", "burst.zscore", "agg.rel_bucket"):
                assert x["window"] == {"t_a": t0, "t_b": t1}
                ta = t0 + (j - 1) * span // 4; w = span // 2
                x["window"] = {"t_a": ta, "t_b": ta + w}; x["stride"] = max(1, w // 100); rule = "W"
            else:
                sys.exit(f"no rule for {q}")
            for k in ("window",):
                if k in x:
                    assert t0 <= x[k]["t_a"] < x[k]["t_b"] <= t1, (q, j, x[k])
            args = json.loads(json.dumps(x, sort_keys=True, default=list))
            iid = P1.md5(VERSION, q, json.dumps(args, sort_keys=True))[:8]
            insts.append({"query": q, "instance": iid, "stratum": f"extra {j}", "class": f"single-parameter rule continued, rule {rule}, j={j}",
                          "factors": {}, "args": args})
    ids = [(i["query"], i["instance"]) for i in insts]
    assert len(set(ids)) == len(ids), "duplicate instances"
    assert not set(ids) & {(i["query"], i["instance"]) for i in base["instances"]}, "an extra instance equals a base instance"
    doc = {"format": base["format"], "version": VERSION, "log": base["log"],
           "selection": {"notes": [__doc__.split("\n\n", 2)[2].strip()], "dataset": ds, "base_file_sha256": hashlib.sha256(a.base.read_bytes()).hexdigest(),
                         "e_star_sequence": [e["eid"] for e in estars], "n_sources": len(srcs)},
           "counts": {q: 3 for q in reg}, "instances": insts}
    name = f"params-{a.tag}.json"
    a.out.mkdir(parents=True, exist_ok=True)
    (a.out / name).write_text(json.dumps(doc, indent=1) + "\n")
    (a.out / "SHA256SUMS").write_text(f"{hashlib.sha256((a.out / name).read_bytes()).hexdigest()}  {name}\n")
    print(f"{a.out / name}: {len(insts)} instances; sources {len(srcs)}; e* sequence {[e['eid'] for e in estars]}")
    for i in insts:
        print(i["query"], i["instance"], i["class"], json.dumps(i["args"])[:160])
    return 0


if __name__ == "__main__":
    sys.exit(main())
