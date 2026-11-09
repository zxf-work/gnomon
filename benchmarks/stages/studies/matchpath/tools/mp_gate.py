#!/usr/bin/env python3
"""mp_gate.py --dir D --out excluded.json --md verdict.md: the answer check of one point, dedicated (native) against
match (nativematch). Reads the kit's verify records (verify-<system>-reg.json, verify-<system>-bel.json: one untimed
call per instance, the page form, its answer hash) and mp_check.py's whole-answer cells (whole-cells.jsonl).

An instance is excluded from nativematch's timing, with its label, when its page hash differs from native's, when
either verify call failed, or when its whole answer differs or could not be compared because of an error. Nothing is
loosened: every such instance is listed in verdict.md. native is never excluded here."""
import argparse
import json
from collections import Counter
from pathlib import Path


def cells(d: Path, s: str) -> dict:
    out = {}
    for tagv in ("reg", "bel"):
        f = d / f"verify-{s}-{tagv}.json"
        if not f.exists():
            continue
        for c in json.loads(f.read_text()).get("results", []):
            out[f"{c['query']}#{c.get('instance')}"] = c
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--md", required=True)
    a = ap.parse_args()
    d = Path(a.dir)
    nat, mat = cells(d, "native"), cells(d, "nativematch")
    whole = {}
    wf = d / "whole-cells.jsonl"
    if wf.exists():
        for line in wf.read_text().splitlines():
            c = json.loads(line)
            whole[f"{c['query']}#{c['instance']}"] = c
    params = json.loads((d / "params-used.json").read_text())["instances"]
    keys = [f"{i['query']}#{i['instance']}" for i in params]
    excl, rows, tally = {}, [], {}
    for k in keys:
        q = k.split("#")[0]
        t = tally.setdefault(q, Counter())
        t["instances"] += 1
        n, m, w = nat.get(k), mat.get(k), whole.get(k)
        why = []
        if n is None or not n.get("ok"):
            why.append(f"native verify: {(n or {}).get('error', 'missing')}"[:200])
        m_slow = m is not None and not m.get("ok") and ("timeout" in str(m.get("error", "")) or str(m.get("error", "")).startswith(">"))
        if m_slow and w is not None and w.get("status") == "compared" and w.get("equal") and w.get("page_equal"):
            # the verify call stopped at its cap (EXTCMP_VERIFY_CAP_S); mp_check compared the page and the whole
            # answer without a cap: the instance is timed, where the 10 s ceiling decides
            t["nativematch verify over its cap, answers compared by mp_check"] += 1
        elif m is None and w is not None and w.get("status") == "compared":
            pass        # no kit verify for nativematch at this point (MP_VERIFY_SYSTEMS): mp_check's page check below decides
        elif m is None or not m.get("ok"):
            why.append(f"nativematch verify: {(m or {}).get('error', 'missing')}"[:200])
        excl_f = (w or {}).get("fields_excluded_from_comparison")
        if n and m and n.get("ok") and m.get("ok"):
            if n.get("hash") == m.get("hash"):
                t["page hash equal"] += 1
            elif excl_f and w.get("status") == "compared" and w.get("page_equal"):
                # the dedicated answer carries fields match cannot return: the kit hash differs by construction;
                # mp_check compared the page and the whole answer without exactly those fields
                t[f"page equal without {', '.join(excl_f)}"] += 1
            else:
                why.append("page hash differs")
        if w is None:
            t["whole: not run"] += 1
            why.append("no whole-answer check")
        elif w.get("status") != "compared":
            t[f"whole: {w.get('status')}"] += 1
            why.append(f"whole check {w.get('status')}: {w.get('error', '')}"[:200])
        elif w.get("equal") and w.get("page_equal"):
            t["whole: equal whole" if w.get("whole") else "whole: equal on the prefix (part)"] += 1
            if m is None:
                t["page equal (mp_check)"] += 1
        else:
            why.append("whole answer differs" if not w.get("equal") else "whole check: page hash differs")
            t["whole: differs"] += 1
        if why:
            t["excluded (nativematch)"] += 1
            excl[k] = "excluded: " + "; ".join(why)
            rows.append(f"- {k}: " + "; ".join(why))
    Path(a.out).write_text(json.dumps({"nativematch": excl}, indent=1, sort_keys=True) + "\n")
    cols = sorted({c for t in tally.values() for c in t}, key=lambda c: (c != "instances", c))
    lines = [f"# Answer check, dedicated against match: {d}", "",
             "| query | " + " | ".join(cols) + " |", "|---|" + "---:|" * len(cols)]
    lines += [f"| {q} | " + " | ".join(str(tally[q].get(c, 0)) for c in cols) + " |" for q in sorted(tally)]
    lines += ["", f"Excluded from nativematch's timing: {len(excl)}"] + rows
    Path(a.md).write_text("\n".join(lines) + "\n")
    print("\n".join(lines))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
