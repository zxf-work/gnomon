#!/usr/bin/env python3
"""
    kf_answer_compare.py --new <results dir> --old <results dir> --tags K1M,K10M
                         [--old-tag-map K1M=K1M] [--systems native,postgres]

For every tag and every `verify-<system>[-<family>].json` of the new run, each
cell (query, instance) is compared with the recorded cell of the same file:
  equal       both ok, same exact answer hash (and row count)
  DIFFERS     both ok, different hash                      -> exit 1
  now-ok      recorded cell not ok (refused, > cap, error), new cell ok: its
              hash is compared with every recorded system's ok cell of the same
              key in the same tag (`= <system>` or `no recorded answer`; a
              recorded answer that differs -> DIFFERS)
  both-not-ok neither answered (reported with the new error)
  new-not-ok  recorded ok, new not ok                      -> exit 1
  missing     the cell is in only one of the two files     -> exit 1 (new side missing)
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter
from pathlib import Path

ap = argparse.ArgumentParser()
ap.add_argument("--new", required=True, type=Path)
ap.add_argument("--old", required=True, type=Path)
ap.add_argument("--tags", required=True)
ap.add_argument("--systems", default="")
ap.add_argument("--vs", default="", help="compare each new file with the RECORDED files of these systems (same family suffix) instead of its own record")
a = ap.parse_args()
want = set(filter(None, a.systems.split(",")))
FILE = re.compile(r"verify-(?P<sys>[a-z0-9]+)(?:-(?P<fam>[A-Z]|reg|bel))?\.json$")


def nx(c: dict) -> bool:
    """not expressible: the system has no driver for the query (the kit's own rule, extcmp.py report): e.g.
    b.diff on Gnomon-on-DuckDB-storage, "DuckDBAdapter has no diff_belief driver"."""
    return not c.get("ok") and str(c.get("error", "")).startswith(("unsupported", "NotImplemented"))


def cells(p: Path) -> dict:
    d = json.loads(p.read_text())
    return {(r["query"], r.get("instance") or ""): r for r in d.get("results", [])}


bad = 0
for tag in a.tags.split(","):
    nd, od = a.new / tag, a.old / tag
    old_all: dict = {}
    for p in sorted(od.glob("verify-*.json")):
        m = FILE.search(p.name)
        if m:
            old_all[p.name] = (m["sys"], cells(p))
    for p in sorted(nd.glob("verify-*.json")):
        m = FILE.search(p.name)
        if not m or (want and m["sys"] not in want):
            continue
        new = cells(p)
        if a.vs:
            for vs in a.vs.split(","):
                on = p.name.replace(f"verify-{m['sys']}", f"verify-{vs}", 1)
                if on not in old_all:
                    print(f"## {tag} {p.name} vs recorded {vs}: no recorded file"); continue
                o2 = old_all[on][1]; t2 = Counter(); n2 = []
                for k, n in new.items():
                    o = o2.get(k)
                    if nx(n):
                        t2["not expressible"] += 1; continue
                    if o is None or not o.get("ok"):
                        t2["recorded not ok/absent"] += 1; continue
                    if not n.get("ok"):
                        t2["new not ok"] += 1; n2.append(f"new not ok {k}: {str(n.get('error'))[:100]}"); continue
                    if n.get("hash") == o.get("hash") or (n.get("proj_hash") and n.get("proj_hash") == o.get("proj_hash")):
                        t2["equal"] += 1
                    else:
                        t2["DIFFERS"] += 1; bad += 1; n2.append(f"DIFFERS {k}: new {n.get('hash')} recorded-{vs} {o.get('hash')}")
                print(f"## {tag} {p.name} vs recorded {on}: " + ", ".join(f"{v} {c}" for c, v in sorted(t2.items())))
                for x in n2[:30]:
                    print("   " + x)
            continue
        if p.name not in old_all:
            print(f"## {tag} {p.name}: no recorded file"); bad += 1; continue
        old = old_all[p.name][1]
        tally, notes = Counter(), []
        for k in sorted(set(new) | set(old)):
            n, o = new.get(k), old.get(k)
            key = f"{k[0]}#{k[1]}" if k[1] else k[0]
            if n is None:
                tally["missing (new)"] += 1; notes.append(f"missing in new: {key}"); bad += 1; continue
            if o is None:
                tally["missing (recorded)"] += 1; notes.append(f"not recorded: {key}"); continue
            if nx(n) and not o.get("ok"):
                tally["not expressible" + ("" if nx(o) else " (recorded: other error)")] += 1; continue
            if n.get("ok") and o.get("ok"):
                if n.get("hash") == o.get("hash") and n.get("rows") == o.get("rows"):
                    tally["equal"] += 1
                else:
                    tally["DIFFERS"] += 1; bad += 1
                    notes.append(f"DIFFERS {key}: new {n.get('hash')} rows={n.get('rows')} recorded {o.get('hash')} rows={o.get('rows')}")
            elif n.get("ok"):
                same = sorted(s for f, (s, c) in old_all.items()
                              if f != p.name and k in c and c[k].get("ok") and c[k].get("hash") == n.get("hash"))
                other = sorted(s for f, (s, c) in old_all.items()
                               if f != p.name and k in c and c[k].get("ok") and c[k].get("hash") != n.get("hash"))
                if other and not same:
                    tally["DIFFERS (now-ok)"] += 1; bad += 1
                    notes.append(f"DIFFERS now-ok {key}: new {n.get('hash')} vs recorded {other}")
                else:
                    tally["now-ok, = " + ("+".join(sorted(set(same))) if same else "no recorded answer")] += 1
                    notes.append(f"now-ok {key} (recorded: {str(o.get('error'))[:80]}) = {sorted(set(same)) or '-'}")
            elif o.get("ok"):
                tally["new-not-ok"] += 1; bad += 1
                notes.append(f"new-not-ok {key}: {str(n.get('error'))[:160]}")
            else:
                tally["both-not-ok"] += 1
                notes.append(f"both-not-ok {key}: new {str(n.get('error'))[:100]} | recorded {str(o.get('error'))[:60]}")
        print(f"## {tag} {p.name}: " + ", ".join(f"{v} {k}" for k, v in sorted(tally.items())))
        for x in notes:
            if not x.startswith("now-ok") or "DIFFERS" in x:
                print("   " + x)
        nowok = [x for x in notes if x.startswith("now-ok")]
        if nowok:
            print(f"   ({len(nowok)} now-ok cells, first: {nowok[0]})")
print("RESULT", "PASS" if bad == 0 else f"FAIL ({bad})")
sys.exit(1 if bad else 0)
