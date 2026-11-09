#!/usr/bin/env python3
"""rap_cause_table.py <dir of cause-*.json> <out.md>: the check's counts per cell. Says what the sample shows and nothing broader; if the sets are not nested, or a sampled node does not fit, it prints cause not determined for that cell."""
import glob
import json
import sys
from pathlib import Path

L = ["# Raphtory's reach answers against Gnomon's: untimed check", "", "Node sets compared in full on the cells listed. A sampled node 'fits' if its path has a hop traversed inside an edge's interval, every hop's interval is in the frozen log, and it has no path traversing every hop at the hop's start time. A statement about a cell covers the cell's sample and counts, not other cells.", ""]
bad = []
for p in sorted(glob.glob(sys.argv[1] + "/cause-*.json")):
    pt = Path(p).stem[6:]
    for c in json.load(open(p)):
        sg = c["samples_only_gnomon"]
        fit = [s for s in sg if (s["hops_inside_interval"] > 0 or s.get("hops_after_run_start", 0) > 0) and not s["path_at_starts_exists"] and all(h["interval_in_log"] for h in s["hops"])]
        nested = c["raphtory_subset_of_gnomon"]
        ok = nested and len(fit) == len(sg) and c["rule_recomputation_equals_operator"]
        L += [f"## {pt} {c['key']} [{c['stratum']}], src {c['src']}, window {c['window']}", "",
              f"Gnomon {c['gnomon_n']} nodes, Raphtory {c['raphtory_n']}, both {c['both']}, only Gnomon {c['only_gnomon']}, only Raphtory {c['only_raphtory']}. Raphtory's set is a subset of Gnomon's: {nested}. Rule re-computed over the store equals the operator node by node, arrivals included: {c['rule_recomputation_equals_operator']}.",
              f"Sampled nodes only in Gnomon: {len(sg)}; fit: {len(fit)}.", ""]
        eq = [k for k, d in c["variants"].items() if d["equal_to_raphtory"]]
        L.append(f"Event-only re-computations equal to Raphtory's set: {eq or 'none'}.")
        near = sorted(c["variants"].items(), key=lambda kv: kv[1]["only_variant"] + kv[1]["only_raphtory"])[:3]
        L += [f"- closest: {k}: only in it {d['only_variant']}, only in Raphtory {d['only_raphtory']}, arrivals equal on common {d['arrival_equal_on_common']}" for k, d in near]
        L += ["", "| node | hops | hops inside a version's interval | hops after the start of Raphtory's run | a path at run starts exists | every interval in the log |", "|---|---:|---:|---:|---|---|"]
        L += [f"| {s['node']} | {len(s['hops'])} | {s['hops_inside_interval']} | {s.get('hops_after_run_start', '')} | {s['path_at_starts_exists']} | {all(h['interval_in_log'] for h in s['hops'])} |" for s in sg]
        if c["samples_only_raphtory"]:
            L += ["", "Nodes only in Raphtory, its own chain:"] + [f"- {s['node']}: {json.dumps(s['raphtory_chain'])[:900]}" for s in c["samples_only_raphtory"]]
        L += ["", "Recorded answers of other systems on this instance, hash against Gnomon's (first page and total):"]
        for r in c["recorded_other_systems"]:
            L.append(f"- {r['system']}: " + ("; ".join(f"ok {x['ok']}, equal {x['hash_equal_gnomon']}, {x['error']} ({x['record'].split('/results/')[-1]})" for x in r["cells"]) or "no recorded cell"))
        if ok:
            L += ["", f"On this cell: Raphtory's set is inside Gnomon's and the {len(sg)} sampled nodes fit. This is what the sample shows."]
        else:
            L += ["", "On this cell: cause not determined."]
            bad.append(f"{pt} {c['key']}")
        L.append("")
L.insert(3, f"Cells with cause not determined: {bad or 'none'}.\n")
Path(sys.argv[2]).write_text("\n".join(L) + "\n")
print("\n".join(L[:6]))
