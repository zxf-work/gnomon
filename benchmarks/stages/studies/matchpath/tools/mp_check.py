#!/usr/bin/env python3
"""mp_check.py --store S --params P [--params P2 ...] --out cells.jsonl --md table.md [--queries a,b] [--seconds N]

Whole-answer check, dedicated operator against its `match` form (scripts/matchpath_texts.py), on one store, untimed,
no ratios. For every instance of the named queries in the parameter files:

* page form: the dedicated call exactly as the timed run issues it (the instance's args) and the `match` form's
  page; their answer hashes (extcmp.answer_hash, the timed records' hash) must be equal;
* whole form: the dedicated operator read to its end (limit 10,000 and the cursor where it pages; a list it does
  not page is whole only when its total fits in 10,000), against the `match` form's whole answer. Every list is
  compared in order (the operators define the order of every list here) and as a multiset; every total and
  scalar is compared. A list the dedicated operator cannot return whole (snapshot nodes or neighbour lists above
  10,000) is compared on the prefix it returns, in order, and the instance is counted "part", never "whole".

Any difference is written out row by row (first 20 per list) to the .jsonl; nothing is loosened.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import signal
import sys
import time
from collections import Counter
from pathlib import Path

import extcmp as X
import matchpath_texts as MT
from gnomon.temporal.algebra import call_operator, ensure_all_registered

OP = {"snap.hop2": "snapshot_subgraph", "b.snap.hop2@now": "snapshot_subgraph", "b.snap.hop2@T": "snapshot_subgraph",
      "nbr.evolution": "neighborhood_evolution", "series.count": "graph_metric_timeseries",
      "series.count.w10": "graph_metric_timeseries", "agg.rel_bucket": "aggregate_events",
      "agg.rel_bucket.w10": "aggregate_events", "hist.single": "entity_history", "hist.asof": "entity_history",
      "diff.belief": "diff_belief", "b.diff": "diff_belief", "b.hist": "entity_full_history",
      "b.hist@now": "entity_believed", "diff.global": "diff_snapshots", "b.diff@ctl": "diff_snapshots",
      "paths.k": "temporal_paths"}
NOCURSOR = {"neighborhood_evolution", "diff_snapshots", "temporal_paths", "entity_full_history", "entity_believed"}
DIFF_LISTS = ("nodes_added", "nodes_removed", "edges_added", "edges_removed", "props_changed")
LISTS = ("rows", "nodes", "neighbors_gained", "neighbors_lost", "degree_series") + DIFF_LISTS
TOTALS = {"rows": "rows_total", "nodes": "nodes_total", "neighbors_gained": "neighbors_gained_total",
          "neighbors_lost": "neighbors_lost_total", **{k: f"{k}_total" for k in DIFF_LISTS}}
SCALARS = ("rows_total", "nodes_total", "neighbors_gained_total", "neighbors_lost_total", "stride", "n_buckets",
           "added_total", "retracted_total") + tuple(f"{k}_total" for k in DIFF_LISTS)
#: fields of the dedicated answer that `match` cannot return (matchpath_texts.HIST_FIELDS_NOT_IN_MATCH); the
#: comparison of these operators drops them from the dedicated rows, and every cell records it
EXCLUDED_FIELDS = {"entity_history": MT.HIST_FIELDS_NOT_IN_MATCH}


def strip(op: str, p: dict) -> dict:
    drop = EXCLUDED_FIELDS.get(op)
    if not drop or not isinstance(p.get("rows"), list):
        return p
    return {**p, "rows": [{k: v for k, v in r.items() if k not in drop} for r in p["rows"]]}


def dedicated_page(ad, op: str, args: dict) -> dict:
    """The dedicated call as the timed run makes it (extcmp.Native.fn_for)."""
    if op in ("entity_full_history", "entity_believed"):
        import eval_harness as H
        import extcmp_beliefq as B
        return B.engine_fn(ad, H.Query("q", op, dict(args), "", ()))()
    if op == "diff_belief":
        import eval_harness as H
        return H._diff_belief_dispatch(ad, dict(args))
    return call_operator(ad, op, dict(args))
DED_MAX = 10_000


class Over(Exception):
    pass


def _alarm(*_):
    raise Over()


def dig(x) -> str:
    return hashlib.blake2b(X.canonical_json(x).encode(), digest_size=12).hexdigest()


def dedicated_whole(ad, op: str, args: dict) -> dict:
    if op in ("entity_full_history", "entity_believed"):
        return dedicated_page(ad, op, args)
    if op == "temporal_paths":            # k at the operator's maximum (MAX_K 20): a prefix past it
        return dict(call_operator(ad, op, {**args, "k": 20}))
    a = {**args, "limit": DED_MAX}
    a.pop("cursor", None)
    p = dict(call_operator(ad, op, a))
    if op in NOCURSOR:
        return p
    rows = list(p.get("rows", []))
    cur = p.get("cursor")
    while cur:
        n = call_operator(ad, op, {**a, "cursor": cur})
        rows += n.get("rows", [])
        cur = n.get("cursor")
    p["rows"], p["cursor"] = rows, None
    return p


def compare(d: dict, m: dict) -> dict:
    """d: the dedicated whole answer; m: match's whole answer."""
    r = {"whole": True, "differences": [], "lists": {}, "prefix_fields": []}
    for k in SCALARS:
        if k in d and k in m:
            if d.get(k) != m.get(k):
                r["differences"].append({"scalar": k, "dedicated": d.get(k), "match": m.get(k)})
        elif k in d or k in m:      # a total only one side's call reports (the registered diff_belief's rows_total):
            r.setdefault("one_sided_totals", []).append(k)       # its rows are compared whole; the page hash covers keys
    for f in LISTS:
        if f not in d and f not in m:
            continue
        a, b = list(d.get(f) or []), list(m.get(f) or [])
        tot = d.get(TOTALS[f]) if f in TOTALS else len(a)
        whole = tot is None or tot == len(a)
        info = {"dedicated": len(a), "match": len(b), "total": tot, "whole": whole}
        if not whole:
            r["whole"] = False
            r["prefix_fields"].append(f)
            b = b[:len(a)]
        if a != b:
            ca, cb = Counter(dig(x) for x in a), Counter(dig(x) for x in b)
            only_d, only_m = ca - cb, cb - ca
            ex_d = [x for x in a if dig(x) in only_d][:20]
            ex_m = [x for x in b if dig(x) in only_m][:20]
            first = next((i for i, (x, y) in enumerate(zip(a, b)) if x != y), min(len(a), len(b)))
            r["differences"].append({"list": f, "only_dedicated": sum(only_d.values()), "only_match": sum(only_m.values()),
                                     "order_only": not only_d and not only_m, "first_index": first,
                                     "examples_dedicated": ex_d, "examples_match": ex_m})
        r["lists"][f] = info
    return r


def instances(paths: list[str], queries: set[str] | None) -> list[dict]:
    out = []
    for p in paths:
        d = json.load(open(p))
        for i in d["instances"]:
            if i["query"] in OP and (not queries or i["query"] in queries):
                out.append({**i, "params_file": Path(p).name})
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--store", required=True)
    ap.add_argument("--params", action="append", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--md", required=True)
    ap.add_argument("--label", default="")
    ap.add_argument("--queries", default="")
    ap.add_argument("--only", default="", help="comma-separated instance ids")
    ap.add_argument("--seconds", type=float, default=0, help="stop starting new instances after this many seconds")
    ap.add_argument("--per", type=int, default=600, help="seconds per instance (both forms)")
    ap.add_argument("--workers", type=int, default=1, help="processes, each with its own handle, instances dealt round-robin")
    ap.add_argument("--shard", type=int, default=None)
    ap.add_argument("--of", type=int, default=None)
    a = ap.parse_args()
    if a.workers > 1 and a.shard is None:
        return run_workers(a)
    ensure_all_registered()
    st = X.open_existing_store(Path(a.store), "native")
    ad = st.adapter
    insts = instances(a.params, set(a.queries.split(",")) if a.queries else None)
    if a.only:
        keep = set(a.only.split(","))
        insts = [i for i in insts if i["instance"] in keep]
    if a.shard is not None:
        insts = [i for n, i in enumerate(insts) if n % a.of == a.shard]
    t_end = time.time() + a.seconds if a.seconds else None
    signal.signal(signal.SIGALRM, _alarm)
    cells = []
    with open(a.out, "w") as fo:
        for n, inst in enumerate(insts):
            q, args = inst["query"], inst["args"]
            op = OP[q]
            cell = {"query": q, "instance": inst["instance"], "stratum": inst.get("stratum"), "label": a.label,
                    "params_file": inst["params_file"]}
            if t_end and time.time() > t_end:
                cell["status"] = "not reached"
                cells.append(cell); fo.write(json.dumps(cell) + "\n"); fo.flush()
                continue
            signal.alarm(a.per)
            t0 = time.perf_counter()
            try:
                pd = strip(op, dedicated_page(ad, op, args))
                pm = MT.fn_for(ad, op, args)()
                if op in EXCLUDED_FIELDS:
                    cell["fields_excluded_from_comparison"] = list(EXCLUDED_FIELDS[op])
                cell["page_hash_dedicated"], cell["page_hash_match"] = X.answer_hash(pd), X.answer_hash(pm)
                cell["page_equal"] = cell["page_hash_dedicated"] == cell["page_hash_match"]
                wd = strip(op, dedicated_whole(ad, op, args))
                wm = MT.fn_for(ad, op, args, whole=True)()
                c = compare(wd, wm)
                cell.update(status="compared", whole=c["whole"], equal=not c["differences"],
                            lists=c["lists"], prefix_fields=c["prefix_fields"], differences=c["differences"])
            except Over:
                cell["status"] = f"not checked: over {a.per} s"
            except Exception as e:                       # noqa: BLE001
                cell["status"] = "error"
                cell["error"] = f"{type(e).__name__}: {e}"[:800]
            finally:
                signal.alarm(0)
            cell["seconds"] = round(time.perf_counter() - t0, 3)
            cells.append(cell)
            fo.write(json.dumps(cell, default=str) + "\n"); fo.flush()
            print(f"{n + 1}/{len(insts)} {q}#{inst['instance']} {cell['status']} page_equal={cell.get('page_equal')} "
                  f"equal={cell.get('equal')} whole={cell.get('whole')} {cell['seconds']} s", flush=True)
    st.close()
    if a.shard is not None:
        return 0
    return summarize(cells, a)


def run_workers(a) -> int:
    """`--workers N`: N copies of this tool, each on every N-th instance, then one table over all cells."""
    import subprocess
    procs = []
    for i in range(a.workers):
        cmd = [sys.executable, __file__, "--store", a.store, "--out", f"{a.out}.{i}", "--md", f"{a.md}.{i}",
               "--label", a.label, "--per", str(a.per), "--shard", str(i), "--of", str(a.workers)]
        for p in a.params:
            cmd += ["--params", p]
        for k in ("queries", "only"):
            if getattr(a, k):
                cmd += [f"--{k}", getattr(a, k)]
        if a.seconds:
            cmd += ["--seconds", str(a.seconds)]
        procs.append(subprocess.Popen(cmd))
    rcs = [p.wait() for p in procs]
    order = {(i["query"], i["instance"]): n for n, i in enumerate(instances(a.params, set(a.queries.split(",")) if a.queries else None))}
    cells = []
    for i in range(a.workers):
        f = Path(f"{a.out}.{i}")
        if f.exists():
            cells += [json.loads(x) for x in f.read_text().splitlines() if x.strip()]
    # an instance a worker did not finish (the worker ended: an allocation past its address-space cap aborts the
    # process) is run again alone, one process each; if that process ends too, the instance is "not checked"
    have = {(c["query"], c["instance"]) for c in cells}
    allq = instances(a.params, set(a.queries.split(",")) if a.queries else None)
    if a.only:
        allq = [i for i in allq if i["instance"] in set(a.only.split(","))]
    for i in [i for i in allq if (i["query"], i["instance"]) not in have]:
        f = Path(f"{a.out}.alone.{i['instance']}")
        cmd = [sys.executable, __file__, "--store", a.store, "--out", str(f), "--md", f"{f}.md", "--label", a.label,
               "--per", str(a.per), "--shard", "0", "--of", "1", "--only", i["instance"], "--queries", i["query"]]
        for p in a.params:
            cmd += ["--params", p]
        rc = subprocess.call(cmd)
        got = [json.loads(x) for x in f.read_text().splitlines() if x.strip()] if f.exists() else []
        if got:
            cells += got
        else:
            cells.append({"query": i["query"], "instance": i["instance"], "stratum": i.get("stratum"), "label": a.label,
                          "params_file": i["params_file"], "status": f"not checked: check process ended (rc {rc})"})
    cells.sort(key=lambda c: order.get((c["query"], c["instance"]), 1 << 30))
    Path(a.out).write_text("".join(json.dumps(c, default=str) + "\n" for c in cells))
    print(f"workers rc {rcs}", flush=True)
    rc = summarize(cells, a)
    return rc if all(r == 0 for r in rcs) else (rc or 8)


def summarize(cells: list, a) -> int:
    by = {}
    for c in cells:
        b = by.setdefault(c["query"], Counter())
        b["instances"] += 1
        if c["status"] == "compared":
            b["compared"] += 1
            b["page hash equal"] += bool(c["page_equal"])
            b["equal whole"] += bool(c["equal"] and c["whole"])
            b["equal on the prefix (part)"] += bool(c["equal"] and not c["whole"])
            b["differ"] += not c["equal"]
        else:
            b[c["status"]] += 1
    cols = ["instances", "compared", "page hash equal", "equal whole", "equal on the prefix (part)", "differ"]
    extra = sorted({k for b in by.values() for k in b} - set(cols))
    lines = [f"# Whole answers, dedicated operator against its match form {a.label}", "",
             f"store {a.store}; parameter files {', '.join(Path(p).name for p in a.params)}", "",
             "| query | " + " | ".join(cols + extra) + " |", "|---|" + "---:|" * len(cols + extra)]
    for q in sorted(by):
        lines.append(f"| {q} | " + " | ".join(str(by[q].get(k, 0)) for k in cols + extra) + " |")
    diff = [c for c in cells if c.get("status") == "compared" and not c["equal"]]
    lines += ["", f"Instances whose answers differ: {len(diff)}"]
    for c in diff[:50]:
        lines.append(f"- {c['query']}#{c['instance']}: " + "; ".join(
            (f"{x['list']}: only dedicated {x['only_dedicated']}, only match {x['only_match']}, order only {x['order_only']}, first index {x['first_index']}"
             if "list" in x else f"{x['scalar']}: {x['dedicated']} vs {x['match']}") for x in c["differences"]))
    Path(a.md).write_text("\n".join(lines) + "\n")
    print("\n".join(lines))
    bad = [c for c in cells if c.get("status") == "error" or (c.get("status") == "compared" and not (c["equal"] and c["page_equal"]))]
    return 7 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
