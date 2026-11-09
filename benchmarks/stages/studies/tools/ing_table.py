#!/usr/bin/env python3
"""ing_table.py <results dir>: phases of every run of the ingest study, as recorded; no scoring. Times in seconds: phase 1 (bulk), phase 2 (transactions), by kind. Peak memory per phase from
rss-samples.jsonl over the run's own phase marks: RSS sum (all categories), and PSS sum (one in ten samples, shared pages counted once), in GB."""
import glob
import json
import os
import sys


def row(p):
    d = json.load(open(p))
    bk = d.get("by_kind", {})
    p1 = d.get("phase1_s", d.get("bulk_s"))
    p2 = d.get("phase2_s", d.get("transactions_s"))
    if p2 is None:      # the baselines' records: the transactions' seconds by kind
        p2 = round(sum(v.get("s", 0) for v in bk.values()), 1)
    return d, [os.path.basename(p)[:-5], p1, p2, d.get("load_s", d.get("wall_incl_log_read_s")),
               json.dumps({k: {"items": v.get("items"), "s": v.get("s"), "items_per_s": v.get("items_per_s")} for k, v in bk.items()}), d.get("verified"), d.get("bytes_on_disk")]


def samples(path):
    out = []
    if os.path.exists(path):
        with open(path) as f:
            for line in f:
                try:
                    out.append(json.loads(line))
                except ValueError:
                    pass
    return out


def peak(ss, a, b, field):
    vals = [sum(c[field] for c in s["by"].values()) for s in ss if a <= s["t"] <= b and (field != "pss_kb" or s.get("pss"))]
    return round(max(vals) / 1048576, 2) if vals else None


def phase_peaks(d, ss):
    m = d.get("marks") or {}
    if "start" not in m or "end" not in m:
        return {}
    b = m.get("bulk_end", m["start"])
    return {"phase1": (peak(ss, m["start"], b, "rss_kb"), peak(ss, m["start"], b, "pss_kb")), "phase2": (peak(ss, b, m["end"], "rss_kb"), peak(ss, b, m["end"], "pss_kb"))}


def main():
    ss = samples(os.path.join(sys.argv[1], "rss-samples.jsonl"))
    files = [p for p in sorted(glob.glob(os.path.join(sys.argv[1], "*.json"))) if not p.endswith("-digest.json")]
    print("| run | phase 1 s | phase 2 s | load wall s | by kind | verified | bytes | peak GB phase 1 (RSS, PSS) | peak GB phase 2 (RSS, PSS) |\n|---|---:|---:|---:|---|---|---:|---|---|")
    for p in files:
        d, r = row(p)
        pk = phase_peaks(d, ss)
        print("| " + " | ".join(str(x) for x in r + [pk.get("phase1"), pk.get("phase2")]) + " |")
    return 0 if files else 1


if __name__ == "__main__":
    raise SystemExit(main())
