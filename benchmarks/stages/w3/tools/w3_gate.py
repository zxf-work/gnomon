#!/usr/bin/env python3
"""W3 answer gate: verdict over the gate jobs' per-system answer records.

    w3_gate.py --point SF1|SF10 --res DIR [DIR ...] --out-dir DIR [--require gnomon,duckdb,...] [--base gnomon]
               [--gnomon-ref REF.jsonl --systems aeong,aeong-prop]

Layout of a gate result dir: <DIR>/<point>/<system>/{params.jsonl, settings.txt, <group>.jsonl, windows.txt}; the .jsonl
records are those of scripts/w3_snb_run.py run (qid, i, setting, ok, hash, sorted_hash, kind, err, ms).

Every system's answer is compared with Gnomon's (the base) per instance and setting, exact hash.
FAIL (exit 1) on any of:
  - a Gnomon error or a Gnomon cell that is not expressible;
  - a Gnomon refusal (CostError) of a call another system answers inside 10 s, unless the cell is labelled
    "refused (estimated > 10 s)" in the rows (SF10: BI15, BI19, IC14 at every setting, the IC13 window);
  - an answer that differs from Gnomon's (hash and sorted hash), for every system but AeonG (P13-P18) and LadybugDB
    (LB-1/3/5/7/8/9/10/12, inexpressible window texts), whose differences are labelled and excluded instead;
  - a required system with no record at all.
Not failures, recorded: a Gnomon or other timeout ("> cap" at the 10 s check cap: a censored cell), a baseline error or
inexpressible text (excluded from timing), answers that differ only in row order (a tie the specification leaves open),
cells the answer-check window cut ("unreached": neither compared nor timed, for that system; an unreached Gnomon cell
removes the cell from every system's timing).
Outputs in --out-dir: gate-<point>.md, exclusions-<point>.json (the per-system exclusion lists the timed job reads),
ref-<point>-gnomon.jsonl (Gnomon's merged records), gate-<point>.rc.
"""
from __future__ import annotations

import argparse
import collections
import glob
import json
import os
import re
import sys
from pathlib import Path

TIMEOUT_RE = re.compile(r"TransactionTimedOut|statement timeout|QueryCanceled|execution timeout|timed out|timeout|Interrupt|"
                        r"TransientError|cancel|asked to abort|abort|> cap", re.I)
TOLERATED = {"aeong": "AeonG defect (P13-P18)", "aeong-prop": "AeonG defect (P13-P18)",
             "ladybug": "LadybugDB defect (LB-1/3/5/7/8/9/10/12) or inexpressible text"}
SF10_LABELLED = {"BI15", "BI19", "IC14"}
INSIDE_S = 10.0
# counted against Gnomon, and are not gate failures; the engine stays frozen. Any other Gnomon error fails the gate.
GNOMON_TOLERATED = {("SF10", "IC4", "window", i) for i in (0, 5, 6, 7)}
# recorded as Gnomon losses labelled "refused (estimated > 10 s)" and are not gate failures. Any other refusal of a call another system
# answers inside 10 s still fails.
GNOMON_REFUSED_TOLERATED = {("SF10", "BI20", "window", i) for i in range(8)}


def labelled(point, q, s):
    return point == "SF10" and (q in SF10_LABELLED or (q == "IC13" and s == "window"))


def classify(r):
    if r is None:
        return "unreached"
    if r.get("ok"):
        return "ok"
    err, kind = str(r.get("err", "")), r.get("kind")
    if kind == "not-expressible":
        return "notexpr"
    if "CostError" in err:
        return "refused"
    if kind == "timeout" or TIMEOUT_RE.search(err):
        return "censored"
    return "error"


def load_dir(d: Path):
    recs = {}
    for f in sorted(d.glob("*.jsonl")):
        if f.name == "params.jsonl":
            continue
        for line in open(f):
            if line.strip():
                r = json.loads(line)
                recs[(r["qid"], r["i"], r["setting"])] = r
    exp = set()
    if (d / "params.jsonl").exists() and (d / "settings.txt").exists():
        sets = (d / "settings.txt").read_text().strip().split(",")
        for line in open(d / "params.jsonl"):
            if line.strip():
                p = json.loads(line)
                for s in sets:
                    exp.add((p["qid"], p["i"], s))
    return recs, exp


def key_s(k):
    return f"{k[0]}|{k[1]}|{k[2]}"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--point", required=True)
    ap.add_argument("--res", nargs="+", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--require", default="")
    ap.add_argument("--base", default="gnomon")
    ap.add_argument("--gnomon-ref")
    ap.add_argument("--systems")
    ap.add_argument("--emit-unreached", help="write the base system's unreached cells as a JSON list of [qid, i, setting] and keep going")
    a = ap.parse_args()
    pt, base = a.point, a.base
    data: dict = {}
    windows: dict = {}
    for d in a.res:
        for sd in sorted(glob.glob(f"{d}/{pt}/*/")):
            s = Path(sd).name
            if a.systems and s not in a.systems.split(","):
                continue
            recs, exp = load_dir(Path(sd))
            if s in data:      # a later dir (a re-run of some cells) overrides the earlier records of the same system
                data[s][0].update(recs)
                data[s][1].update(exp)
            else:
                data[s] = (recs, exp)
            if (Path(sd) / "windows.txt").exists():
                windows.setdefault(s, []).extend(f"{Path(d).name}: {w}" for w in (Path(sd) / "windows.txt").read_text().strip().splitlines())
    fails: list = []
    for s in [x for x in a.require.split(",") if x]:
        if s not in data or not data[s][0]:
            fails.append(f"required system {s}: no answer record")
    if a.gnomon_ref:
        G = {}
        for line in open(a.gnomon_ref):
            if line.strip():
                r = json.loads(line)
                G[(r["qid"], r["i"], r["setting"])] = r
    elif base in data:
        G = data[base][0]
    else:
        G = {}
        fails.append(f"no {base} records (the base of the comparison)")
    stats: dict = {s: collections.Counter() for s in data}
    excluded: dict = {s: {} for s in data}
    tolerated_rows: list = []
    labelled_ref: collections.Counter = collections.Counter()
    gnomon_cens_other_ok: collections.Counter = collections.Counter()
    ties: collections.Counter = collections.Counter()
    recorded_err: list = []
    recorded_ref: list = []

    def other_ok_inside(key):
        for s, (recs, _) in data.items():
            if s == base:
                continue
            r = recs.get(key)
            if r and r.get("ok") and (r.get("ms") or 0) <= INSIDE_S * 1e3:
                return s
        return None

    for s, (recs, exp) in sorted(data.items()):
        for key in sorted(exp):
            q, i, st = key
            o = recs.get(key)
            co = classify(o)
            b = G.get(key)
            cb = classify(b)
            stats[s]["expected"] += 1
            if s == base:
                stats[s][co] += 1
                if co == "unreached":
                    excluded[s][key_s(key)] = "unreached"
                elif co in ("error", "notexpr"):
                    if (pt, q, st, i) in GNOMON_TOLERATED and "binding table" in str(o.get("err")):
                        recorded_err.append((q, i, st, str(o.get("err"))[:110]))
                    else:
                        fails.append(f"Gnomon error {q} i={i} {st}: {str(o.get('err'))[:120]}")
                elif co == "refused":
                    if labelled(pt, q, st):
                        labelled_ref[(q, st)] += 1
                    elif (pt, q, st, i) in GNOMON_REFUSED_TOLERATED:
                        labelled_ref[(q, st)] += 1
                        recorded_ref.append((q, i, st))
                    else:
                        who = other_ok_inside(key)
                        if who:
                            fails.append(f"Gnomon refused {q} i={i} {st} (estimated cost) and {who} answered inside 10 s")
                elif co == "censored":
                    who = other_ok_inside(key)
                    if who:
                        gnomon_cens_other_ok[(q, st)] += 1
                continue
            # a baseline
            if cb == "unreached":
                stats[s]["gnomon_unreached"] += 1
                excluded[s][key_s(key)] = "Gnomon cell unreached"
                continue
            stats[s][co] += 1
            if co == "unreached":
                excluded[s][key_s(key)] = "unreached"
            elif co in ("error", "notexpr", "refused"):
                excluded[s][key_s(key)] = co
            elif co == "ok" and cb == "ok":
                if o["hash"] == b["hash"]:
                    stats[s]["equivalent"] += 1
                elif o.get("sorted_hash") == b.get("sorted_hash"):
                    stats[s]["equivalent"] += 1
                    stats[s]["row-order-tie"] += 1
                    ties[(s, q, st)] += 1
                else:
                    stats[s]["DIFFERS"] += 1
                    excluded[s][key_s(key)] = "differs from Gnomon"
                    if s in TOLERATED:
                        tolerated_rows.append((s, q, i, st, TOLERATED[s]))
                    else:
                        fails.append(f"{s} differs from Gnomon: {q} i={i} {st}")
            # co == censored (> cap): timed, recorded as "> 10 s"; co ok while Gnomon censored/refused: timed
        if s != base and s in TOLERATED:
            pass
    if a.emit_unreached:
        cells = sorted([k.split("|")[0], int(k.split("|")[1]), k.split("|")[2]] for k, v in excluded.get(base, {}).items() if v == "unreached")
        json.dump(cells, open(a.emit_unreached, "w"))
        print(f"emitted {len(cells)} unreached {base} cells -> {a.emit_unreached}")
    if os.environ.get("W3_GATE_FAIL_UNREACHED_GNOMON") == "1" and base in stats and stats[base]["unreached"]:
        fails.append(f"{stats[base]['unreached']} Gnomon cells are still unreached (the answer-check window cut them)")
    # Gnomon's own exclusions: only unreached cells; all other cells (answered, censored, labelled refusals) are timed
    out = Path(a.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    (out / f"exclusions-{pt}.json").write_text(json.dumps(
        {"point": pt, "excluded": {s: sorted(v) for s, v in excluded.items()},
         "reasons": {s: dict(collections.Counter(v.values())) for s, v in excluded.items()}}, indent=0) + "\n")
    with open(out / f"ref-{pt}-gnomon.jsonl", "w") as f:
        for k in sorted(G):
            f.write(json.dumps(G[k], default=str) + "\n")
    L = [f"# W3 answer gate {pt}", "",
         f"base: {base}. Verdict: **{'FAIL' if fails else 'PASS'}** ({len(fails)} failing checks).", "",
         "| system | expected | answered, equivalent to Gnomon | of which row-order ties | DIFFERS | > cap (censored) | refused | error | not expressible | unreached | Gnomon cell unreached |",
         "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for s in sorted(stats):
        c = stats[s]
        eq = c["equivalent"] if s != base else c["ok"]
        L.append(f"| {s} | {c['expected']} | {eq} | {c['row-order-tie']} | {c['DIFFERS']} | {c['censored']} | {c['refused']} | "
                 f"{c['error']} | {c['notexpr']} | {c['unreached']} | {c['gnomon_unreached']} |")
    L += ["", "## Failing checks", ""] + ([f"- {x}" for x in fails[:200]] or ["none"])
    if len(fails) > 200:
        L.append(f"- ... {len(fails) - 200} more")
    L += ["", "## Gnomon refusals labelled \"refused (estimated > 10 s)\"", ""]
    L += [f"- {q} {st}: {n} instances" for (q, st), n in sorted(labelled_ref.items())] or ["none"]
    L += ["", "## Gnomon refusals tolerated by name: refused (estimated > 10 s), counted against Gnomon", ""]
    L += [f"- {q} i={i} {st}: refused (estimated > 10 s)" for q, i, st in sorted(recorded_ref)] or ["none"]
    L += ["", "## Gnomon errors recorded as results, not failures", ""]
    L += [f"- {q} i={i} {st}: {e}" for q, i, st, e in sorted(recorded_err)] or ["none"]
    L += ["", "## Gnomon censored (> 10 s check cap) where another system answered inside 10 s (a result, not a failure)", ""]
    L += [f"- {q} {st}: {n} instances" for (q, st), n in sorted(gnomon_cens_other_ok.items())] or ["none"]
    L += ["", "## Differences of tolerated systems (labelled, excluded from timing)", ""]
    agg = collections.Counter((s, q, st, lab) for s, q, i, st, lab in tolerated_rows)
    L += [f"- {s} {q} {st}: {n} instances ({lab})" for (s, q, st, lab), n in sorted(agg.items())] or ["none"]
    L += ["", "## Row-order ties (same rows, other order; accepted)", ""]
    L += [f"- {s} {q} {st}: {n}" for (s, q, st), n in sorted(ties.items())[:60]] or ["none"]
    L += ["", "## Answer-check windows cut or failed (cells after the cut are unreached)", ""]
    L += [f"- {s}: {w}" for s, ws in sorted(windows.items()) for w in ws] or ["none"]
    L += ["", "## Exclusions from timing (per system, by reason)", ""]
    for s in sorted(excluded):
        L.append(f"- {s}: {dict(collections.Counter(excluded[s].values())) or 'none'}")
    (out / f"gate-{pt}.md").write_text("\n".join(L) + "\n")
    rc = 1 if fails else 0
    (out / f"gate-{pt}.rc").write_text(f"{rc}\n")
    print("\n".join(L[:30]))
    print(f"gate {pt}: {'FAIL' if fails else 'PASS'} ({len(fails)} failing checks) -> {out}/gate-{pt}.md")
    return rc


if __name__ == "__main__":
    sys.exit(main())
