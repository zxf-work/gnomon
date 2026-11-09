#!/usr/bin/env python3
"""The answer gate of the t10 and k100m stages (before any timing). Descriptive: it says whether answers agree, nothing about times.

    x_gate.py --res <gate job results dir> --set w1|w2|k100m --tags T,T --fams P,T,... --native-from D --others-from D --out report.md

Result roots reg/<tag>/ and t32reg/<tag>/ hold verify-<sys>-<fam>.json at the timed protocol's caps and the kit gate's verdicts.json, excluded.json.
Any FAIL line makes the exit status 1:
  L. k100m: every system's load complete. Otherwise the provenance of each restored store is printed.
  R. every (system, family) window left a record; a window cut at the window limit leaves a partial record whose unfinished instances are "unreached": listed.
  D. every answer against Gnomon's in the same job (the kit's verdicts): any DIFFERS is a FAIL. Errors and cells above the ceiling are counted, not compared.
  E. Gnomon: an error, or a refusal of a call another system answers inside 10 s, is a FAIL. Gnomon above the ceiling is listed.
  A. at K10M-d0: each system's answers against its own earlier record of the same call on the same log bytes (K10M): a different hash with a different projection
     hash or row count is a FAIL; a different order only is listed.
Also printed: which Gnomon answers changed from d0 to d10, and an estimate of the timed job from the walls of the answer check.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

E = Path(__import__("os").environ.get("GNOMON_BENCH_ROOT", ".") + "")
EARLIER = {"w1": {"native": E / "confirm/w1/results/gate-J42/sp/K10M", "others": E / "final/w1/results/gate-J19/sp/K10M"},
           "w2": {"native": E / "confirm/w2/results/gate-J44/p1/K10M", "others": E / "final/w2/results/gate-J21/p1/K10M"}}
TIMEOUT = ("statement timeout", "canceling statement due to", "TIMEOUT_EXCEEDED", "Timeout exceeded", "client-side limit", "INTERRUPT", "Interrupted", "timeout", "> 10 s", ">10 s", "aborted")
SYS = os.environ.get("X_GATE_SYSTEMS", "native duckdbsql clickhouse postgres").split()     # s100m: native duckdbsql postgres
ROOTS = [("reg", SYS)] + ([("t32reg", ["native", "duckdbsql"])] if os.environ.get("X_T32", "1") == "1" else [])


def cells(d: Path, s: str) -> dict:
    out = {}
    for p in sorted(d.glob(f"verify-{s}*.json")):
        r = json.loads(p.read_text())
        if r.get("system") not in (s, None):
            continue
        for c in r.get("results", []):
            out[(c["query"], c.get("instance") or "")] = c
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    for x in ("--res", "--set", "--tags", "--fams", "--native-from", "--others-from", "--out"):
        ap.add_argument(x, required=True)
    a = ap.parse_args()
    res, tags, fams = Path(a.res), a.tags.split(","), a.fams.split(",")
    L, fail = [f"# Answer gate {res}", ""], []
    native_by = {}
    for tag in tags:
        L += [f"## {tag}", ""]
        for root, syss in ROOTS:
            d = res / root / tag
            if not d.is_dir():
                fail.append(f"{root} {tag}: no records"); continue
            L += [f"### {root}", ""]
            unreached = Counter()
            for s in syss:
                for f in fams:
                    p = d / f"verify-{s}-{f}.json"
                    if not p.exists():
                        fail.append(f"R {root} {tag}: no record {p.name}"); continue
                    r = json.loads(p.read_text())
                    if r.get("partial"):
                        unreached[s] += sum(1 for c in r["results"] if c.get("unreached"))
            if unreached:
                L.append(f"- R unreached at the window limit (not compared, not timed): {dict(unreached)}")
            vf = d / "verdicts.json"
            if not vf.exists():
                fail.append(f"D {root} {tag}: no verdicts.json"); continue
            V = json.loads(vf.read_text())
            for s, rows in sorted(V.items()):
                cnt = Counter(str(r["verdict"]) for r in rows)
                L.append(f"- D {s} against Gnomon: " + ", ".join(f"{k} {v}" for k, v in sorted(cnt.items())))
                for r in rows:
                    if "DIFFERS" in str(r["verdict"]):
                        fail.append(f"D {root} {tag} {s} {r['query']}#{r.get('instance')}: {str(r.get('detail'))[:300]}")
            nat = cells(d, "native")
            others_fast = defaultdict(list)
            for s in syss:
                if s == "native":
                    continue
                for k, c in cells(d, s).items():
                    if c.get("ok") and c.get("wall_ms", 1e18) <= 10_000:
                        others_fast[k].append(s)
            nb = Counter()
            for k, c in nat.items():
                if c.get("ok"):
                    nb["ok"] += 1
                    if c.get("wall_ms", 0) > 10_000:
                        L.append(f"- E Gnomon above the ceiling at the answer check: {k[0]}#{k[1]} {c['wall_ms']:.0f} ms")
                    continue
                err = str(c.get("error", ""))
                if c.get("unreached"):
                    nb["unreached"] += 1
                elif c.get("refused") or "E_COST" in err or "guardrail" in err.lower():
                    nb["refused"] += 1
                    if others_fast.get(k):
                        fail.append(f"E {root} {tag}: Gnomon refused {k[0]}#{k[1]}, answered inside 10 s by {others_fast[k]}: {err[:200]}")
                    else:
                        L.append(f"- E Gnomon refused {k[0]}#{k[1]}; no other system inside 10 s")
                elif c.get("aborted") or any(t in err for t in TIMEOUT):
                    nb["above ceiling"] += 1
                    L.append(f"- E Gnomon above the ceiling {k[0]}#{k[1]}: {err[:120]}")
                else:
                    nb["error"] += 1
                    fail.append(f"E {root} {tag}: Gnomon error {k[0]}#{k[1]}: {err[:300]}")
            L.append(f"- E Gnomon cells: {dict(nb)}")
            if root == "reg":
                native_by[tag] = nat
                # estimate of the timed job from one answer-check call per cell, for the plan only
                excl = {}
                ef = d / "excluded.json"
                if ef.exists():
                    excl = json.loads(ef.read_text())
                tot = 0.0
                rows = []
                for s in SYS:
                    cs = cells(d, s)
                    t, n, slow, ex = 0.0, 0, 0, 0
                    tE = 0.0
                    for k, c in cs.items():
                        key = k[0] if not k[1] else f"{k[0]}#{k[1]}"
                        if not c.get("ok") or key in excl.get(s, {}):
                            ex += 1; continue
                        w = c.get("wall_ms", 0) / 1e3
                        n += 1; slow += w > 1
                        t += w * (12 if w <= 1 else 4)
                        tE += w * (5 if w <= 1 else 4)
                    win = len(fams) * (25 if s in ("clickhouse", "postgres") else 12)
                    cost = 2 * (t + win) + (2 * (t + win) if s == "duckdbsql" else 0) + ((tE + win) if s != "clickhouse" else 0)
                    tot += cost
                    rows.append(f"{s}: {n} cells timed ({slow} above 1 s), {ex} not timed, about {cost / 60:.0f} min")
                L.append(f"- estimate of the timed point: " + "; ".join(rows) + f"; about {tot / 60:.0f} min in all, before the cost of censored cells")
            if tag == "K10M-d0" and root == "reg" and a.set in EARLIER:
                for s in SYS:
                    old = cells(EARLIER[a.set]["native" if s == "native" else "others"], s)
                    now = cells(d, s)
                    eq = diff = order = absent = notcmp = 0
                    for k, c in now.items():
                        o = old.get(k)
                        if o is None:
                            absent += 1; continue
                        if not (c.get("ok") and o.get("ok")):
                            notcmp += 1; continue
                        if c.get("hash") == o.get("hash") and c.get("rows") == o.get("rows"):
                            eq += 1
                        elif c.get("proj_hash") == o.get("proj_hash") and c.get("rows") == o.get("rows") and c.get("proj_hash"):
                            order += 1
                        else:
                            diff += 1
                            fail.append(f"A {tag} {s} {k[0]}#{k[1]}: hash {c.get('hash')} rows {c.get('rows')} against the earlier record's {o.get('hash')} rows {o.get('rows')}")
                    L.append(f"- A {s} against its earlier record: {eq} equal, {order} different order only, {diff} different, {notcmp} not answered on one side, {absent} no earlier cell")
        if a.set not in ("k100m", "s100m") or os.environ.get("X_LOAD") == "0":      # restored stores: the provenance of each
            for s, frm in [(s, a.native_from if s == "native" else a.others_from) for s in SYS]:
                p = Path(frm) / tag
                lf = p / f"load-{s}.json"
                try:
                    r = json.loads(lf.read_text())
                    m = r.get("manifest", {})
                    sha = (p / f"{s}-{tag}.tar.zst.sha256").read_text().split()[0]
                    L.append(f"- store {s}: {p}/{s}-{tag}.tar.zst sha256 {sha[:16]}..., load record commit {m.get('commit')}, host {m.get('hostname')}, finished {r.get('host_after', {}).get('utc')}")
                except Exception as e:      # noqa: BLE001
                    fail.append(f"L {tag} {s}: cannot read the store's load record: {e}")
        else:
            for s in SYS:
                lf = res / "reg" / tag / f"load-{s}.json"
                if not lf.exists():
                    fail.append(f"L {tag} {s}: no load record"); continue
                r = json.loads(lf.read_text())
                if "load" not in r or r.get("load exceeded budget"):
                    fail.append(f"L {tag} {s}: load not complete: {str(r)[:200]}")
                else:
                    L.append(f"- L {s}: load {r['load'].get('load_s')} s, built on this job's engine")
        L.append("")
    if "K10M-d0" in native_by and "K10M-d10" in native_by:
        same, chg = Counter(), Counter()
        for k, c in native_by["K10M-d10"].items():
            o = native_by["K10M-d0"].get(k)
            if o and o.get("ok") and c.get("ok"):
                (same if o.get("hash") == c.get("hash") else chg)[k[0]] += 1
        L += ["## Gnomon answers, d0 against d10 (same call)", "", f"- changed: {dict(chg)}", f"- same: {dict(same)}", ""]
    verdict = "FAIL" if fail else "PASS"
    head = [f"**Verdict: {verdict}** ({len(fail)} failing checks)", ""] + [f"- **FAIL** {x}" for x in fail] + [""]
    Path(a.out).write_text("\n".join(L[:2] + head + L[2:]) + "\n")
    print("\n".join(head))
    return 1 if fail else 0


if __name__ == "__main__":
    sys.exit(main())
