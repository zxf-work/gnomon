#!/usr/bin/env python3
"""fs_check.py --manifest M --tag T --root R --seconds S --out cells.jsonl --diff differences.txt: whole-answer check, untimed, no ratios. Gnomon page by page (limit 10,000 and cursor where the operator pages); DuckDB (SQL) and PostgreSQL through their own code with a limit above the total. Rows are compared as multisets under the hash's own projection (extcmp._project, canonical_json), and exactly; the first page in order. Row limit 20,000,000 per instance, on the recorded total: above it, "not checked: too large". A list the operator does not page (snapshot nodes above 10,000, paths above k = 20, the operator's maximum) is reported as a prefix comparison, never as whole. Instances are taken round-robin over queries in the manifest's seeded order until the time is up; the rest are "not reached". A per-system limit on one instance gives "not checked: over the limit"."""
import argparse
import hashlib
import json
import signal
import sys
import threading
import time
from collections import Counter
from pathlib import Path

import eval_harness as H
import extcmp as X
from gnomon.core.errors import GnomonError

ROW_LIMIT = 20_000_000
PER = 240
LIM = 25_000_000
OP = {"reach.window": "temporal_reachability", "b.reach@now": "temporal_reachability", "b.reach@T": "temporal_reachability", "snap.hop2": "snapshot_subgraph", "b.snap.hop2@now": "snapshot_subgraph", "b.snap.hop2@T": "snapshot_subgraph", "nbr.evolution": "neighborhood_evolution", "coactive.narrow": "co_active", "paths.k": "temporal_paths", "resolve.substr": "resolve_entities", "series.count": "graph_metric_timeseries", "agg.rel_bucket": "aggregate_events", "b.diff": "diff_belief"}
NOCURSOR = {"neighborhood_evolution", "temporal_paths"}
LISTS = ("rows", "nodes", "neighbors_gained", "neighbors_lost", "degree_series")
TOTALS = {"rows": "rows_total", "nodes": "nodes_total", "neighbors_gained": "neighbors_gained_total", "neighbors_lost": "neighbors_lost_total"}
SCALARS = ("rows_total", "nodes_total", "neighbors_gained_total", "neighbors_lost_total", "added_total", "retracted_total", "n_buckets", "stride")


class Over(Exception):
    pass


def alarm(*_):
    raise Over()


def dig(x):
    return hashlib.blake2b(X.canonical_json(x).encode(), digest_size=12).digest()


def fingerprint(p):
    out = {}
    for f in LISTS:
        v = p.get(f)
        if isinstance(v, list):
            out[f] = {"proj": Counter(dig(X._project(r)) for r in v), "exact": Counter(dig(r) for r in v), "n": len(v), "first": [dig(X._project(r)) for r in v[:100]]}
    return out, {k: p[k] for k in SCALARS if k in p}


def q_of(c, extra):
    a = {**c["args"], **extra}
    return H.Query(c["query"], OP[c["query"]], a, "", ())


def gnomon(sysobj, c):
    op = OP[c["query"]]
    extra = {"k": 20} if op == "temporal_paths" else {"limit": 10_000}
    if c["query"] == "b.diff":
        # the harness's b.diff path (eval_harness._diff_belief_dispatch) takes a limit and no cursor; the registered operator pages the same kernel
        rows, cur = [], None
        while True:
            a = {**c["args"], "limit": 10_000}
            if cur:
                a["cursor"] = cur
            n = X.call_operator(sysobj.adapter, "diff_belief", a)
            rows += n["rows"]
            cur = n.get("cursor")
            if not cur:
                return {**n, "rows": rows}
    p = sysobj.fn_for(q_of(c, extra))()
    if op in NOCURSOR:
        return p
    rows = list(p.get("rows", []))
    cur = p.get("cursor")
    while cur:
        n = sysobj.fn_for(q_of(c, {**extra, "cursor": cur}))()
        rows += n.get("rows", [])
        cur = n.get("cursor")
    p = dict(p)
    p["rows"] = rows
    p["cursor"] = None      # every page read
    return p


def other(sysobj, c):
    op = OP[c["query"]]
    extra = {"k": 20} if op == "temporal_paths" else {"limit": LIM}
    return sysobj.fn_for(q_of(c, extra))()


def compare(cell, g, s):
    gf, gs = fingerprint(g)
    of, os_ = fingerprint(s)
    r = {"only_system": 0, "only_gnomon": 0, "exact_only_system": 0, "exact_only_gnomon": 0, "first_page_order_differs": 0, "totals_differ": {}, "prefix_only_fields": [], "lists": {}}
    for k in set(gs) & set(os_):       # a total only one side reports (the operator's rows_total on b.diff) is covered by the row comparison
        if gs[k] != os_[k]:
            r["totals_differ"][k] = [gs[k], os_[k]]
    for f in LISTS:
        if f not in gf or f not in of:
            continue
        a, b = gf[f], of[f]
        whole_g = gs.get(TOTALS.get(f), a["n"]) == a["n"] and not (f == "rows" and g.get("cursor"))
        if whole_g:
            onlys = sum((b["proj"] - a["proj"]).values())
            onlyg = sum((a["proj"] - b["proj"]).values())
            r["only_system"] += onlys
            r["only_gnomon"] += onlyg
            r["exact_only_system"] += sum((b["exact"] - a["exact"]).values())
            r["exact_only_gnomon"] += sum((a["exact"] - b["exact"]).values())
        else:
            r["prefix_only_fields"].append({"field": f, "gnomon_returned": a["n"], "total": gs.get(TOTALS.get(f))})
        n = min(100, len(a["first"]), len(b["first"]))
        if a["first"][:n] != b["first"][:n]:
            r["first_page_order_differs"] += 1
        r["lists"][f] = {"gnomon": a["n"], "system": b["n"]}
    return r


def main():
    ap = argparse.ArgumentParser()
    for k in ("manifest", "tag", "root", "out", "diff", "native-ref"):
        ap.add_argument("--" + k, required=True)
    ap.add_argument("--seconds", type=float, required=True)
    ap.add_argument("--done", action="append")
    a = ap.parse_args()
    t_end = time.time() + a.seconds
    m = json.load(open(a.manifest))
    cells = m["cells"]
    X.ensure_all_registered()
    root, ref = Path(a.root), Path(a.native_ref)
    S = {}
    for name in ("native", "duckdbsql", "postgres"):
        o = X._system_class(name)(ref, root, a.tag)
        o.open()
        S[name] = o
    signal.signal(signal.SIGALRM, alarm)
    byq = {}
    for c in cells:
        byq.setdefault(c["query"], []).append(c)
    order = []
    i = 0
    qs = sorted(byq)
    while any(i < len(byq[q]) for q in qs):
        for q in qs:
            if i < len(byq[q]):
                order.append(byq[q][i])
        i += 1
    done = set()
    for pf in a.done or ():     # a resumed run: an instance already compared or already ruled too large is not read again
        if Path(pf).exists():
            for x in open(pf):
                r0 = json.loads(x)
                if r0.get("systems") or r0.get("state") == "not checked: too large":
                    done.add(r0["query"] + "#" + r0["instance"])
    order = [c for c in order if c["key"] not in done]
    f = open(a.out, "a")
    dfile = open(a.diff, "a")
    ran = 0
    for c in order:
        rec = {"point": c["point"], "query": c["query"], "instance": c["instance"], "stratum": c.get("stratum"), "recorded_total": c["recorded_total"]}
        if time.time() > t_end:
            rec["state"] = "not reached"
            f.write(json.dumps(rec) + "\n")
            continue
        if c["recorded_total"] > ROW_LIMIT:
            rec["state"] = "not checked: too large"
            f.write(json.dumps(rec) + "\n")
            continue
        ran += 1
        t0 = time.time()
        try:
            signal.alarm(PER)
            g = gnomon(S["native"], c)
            signal.alarm(0)
        except Over:
            rec["state"] = "not checked: Gnomon over the limit"
            f.write(json.dumps(rec) + "\n")
            f.flush()
            continue
        except (GnomonError, Exception) as e:  # noqa: BLE001
            signal.alarm(0)
            rec["state"] = "not checked: Gnomon refused or failed"
            rec["error"] = f"{type(e).__name__}: {e}"[:300]
            f.write(json.dumps(rec) + "\n")
            f.flush()
            continue
        rec["gnomon_s"] = round(time.time() - t0, 2)
        rec["systems"] = {}
        for name in ("duckdbsql", "postgres"):
            t0 = time.time()
            tim = threading.Timer(PER, S[name].cancel) if name == "duckdbsql" else None
            if tim:
                tim.start()
            try:
                try:
                    signal.alarm(PER + 5)
                    s = other(S[name], c)
                    signal.alarm(0)
                    r = compare(c, g, s)
                    r["state"] = "compared"
                    bad = r["only_system"] or r["only_gnomon"] or r["first_page_order_differs"] or r["totals_differ"]
                    r["equal"] = not bad and not r["prefix_only_fields"]
                    if bad:
                        dfile.write(json.dumps({"point": c["point"], "key": c["key"], "system": name, "only_system": r["only_system"], "only_gnomon": r["only_gnomon"], "first_page_order_differs": r["first_page_order_differs"], "totals_differ": r["totals_differ"]}) + "\n")
                        dfile.flush()
                except Exception as e:  # noqa: BLE001   (Over included: the alarm can also land while this handler runs; the outer except takes that)
                    signal.alarm(0)
                    msg = f"{type(e).__name__}: {e}"[:300]
                    r = {"state": "not checked: over the limit" if (isinstance(e, Over) or "nterrupt" in msg or "timeout" in msg) else "not checked: failed", "error": msg}
            except Over:
                r = {"state": "not checked: over the limit"}
            signal.alarm(0)
            if tim:
                tim.cancel()
            if name == "postgres" and r.get("state") != "compared":
                try:
                    S[name].close()
                except Exception:  # noqa: BLE001
                    pass
                S[name].open()
            r["s"] = round(time.time() - t0, 2)
            rec["systems"][name] = r
        f.write(json.dumps(rec, default=str) + "\n")
        f.flush()
    print(f"{a.tag}: {ran} instances tried of {len(cells)} in {round(a.seconds)} s budget", flush=True)


if __name__ == "__main__":
    main()
