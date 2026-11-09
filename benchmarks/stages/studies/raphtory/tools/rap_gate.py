#!/usr/bin/env python3
"""rap_gate.py --dir POINT --ceiling S --label L --out excluded.json --md verdict.md: Raphtory's answers against Gnomon's on the parameter-file path. The kit's verdict row (extcmp._verdict_row, projected
compare: the fields Raphtory can produce, extcmp_raphtory.project_native) per instance; extcmp_p1.gate hard-codes the exact compare, so it does not serve here. A cell not equivalent, or above the ceiling at the
answer check, goes to the exclusion list and is not timed. Prints every DIFFERS with both totals. Exit 0 when Gnomon's reference is whole, 1 otherwise; a differing answer does not change the exit code: read verdict.md."""
import argparse
import json
from collections import Counter
from pathlib import Path

import extcmp as X
import extcmp_raphtory as R


def total(payload):
    if not isinstance(payload, dict):
        return None
    for k in ("nodes_total", "rows_total"):
        if k in payload:
            return {k: payload[k]}
    if "gained_total" in payload:
        return {"gained": payload["gained_total"], "lost": payload["lost_total"]}
    if "neighbors_gained_total" in payload:
        return {"gained": payload["neighbors_gained_total"], "lost": payload["neighbors_lost_total"]}
    return None


def main():
    ap = argparse.ArgumentParser()
    for k in ("dir", "label", "out", "md"):
        ap.add_argument("--" + k, required=True)
    ap.add_argument("--ceiling", type=float, default=10)
    a = ap.parse_args()
    d = Path(a.dir)
    nat = json.loads((d / "verify-native.json").read_text())["results"]
    rap = json.loads((d / "verify-raphtory.json").read_text())["results"]
    b = {}
    for c in nat:
        c = dict(c)
        k = X.cell_key(c)
        f = d / "answers-native" / f"{k}.json"
        proj = R.project_native(c["query"], json.loads(f.read_text())) if f.exists() and c.get("ok") else None
        c["hash"] = c["proj_hash"] = X.sha256_hex(X.canonical_json(proj))[:32] if proj is not None else None
        b[k] = c
    rows, excl, counts = [], {}, Counter()
    other = {"compare": "projected-raphtory"}
    for r in rap:
        k = X.cell_key(r)
        tmp = []
        X._verdict_row(tmp, r, b.get(k), r["query"], other)
        v = tmp[0]["verdict"]
        br = b.get(k) or {}
        row = {"key": k, "query": r["query"], "instance": r.get("instance"), "stratum": r.get("stratum"), "verdict": v, "detail": tmp[0].get("detail", tmp[0].get("by", "")),
               "native_ok": br.get("ok"), "native_wall_ms": br.get("wall_ms"), "raphtory_wall_ms": r.get("wall_ms"), "native_error": (br.get("error") or "")[:160]}
        if v == "DIFFERS":
            fa, fb = d / "answers-native" / f"{k}.json", d / "answers-raphtory" / f"{k}.json"
            row["native_total"] = total(json.loads(fa.read_text())) if fa.exists() else None
            row["raphtory_total"] = total(json.loads(fb.read_text())) if fb.exists() else None
        if v == "equivalent" and max(r.get("wall_ms") or 0, br.get("wall_ms") or 0) > a.ceiling * 1e3:
            v = row["verdict"] = f"> {a.ceiling:g} s at answer check"
        if v in ("error", "refused") and ("timeout" in (r.get("error") or "") or "Timeout" in (r.get("error") or "") or "ceiling" in (r.get("error") or "")):
            v = row["verdict"] = f"> {a.ceiling:g} s at answer check"
        if v != "equivalent":
            excl.setdefault("raphtory", {})[k] = f"excluded: {v}"
        counts[(r["query"], v)] += 1
        rows.append(row)
    bad = [k for k, c in b.items() if not c.get("ok")]
    Path(a.out).write_text(json.dumps(excl, indent=1, sort_keys=True) + "\n")
    (d / "label-only.json").write_text(json.dumps({"raphtory": {r["key"]: "excluded: not a differing cell" for r in rows if r["verdict"] != "DIFFERS"}}, indent=1, sort_keys=True) + "\n")
    (d / "verdicts-raphtory.json").write_text(json.dumps(rows, indent=1, sort_keys=True) + "\n")
    L = [f"# Raphtory answer check, {a.label}", "", f"Gnomon cells {len(b)}, not ok {len(bad)} {bad[:10]}. Raphtory cells {len(rap)}.", "", "| query | verdict | cells |", "|---|---|---:|"]
    L += [f"| {q} | {v} | {n} |" for (q, v), n in sorted(counts.items())]
    dif = [r for r in rows if r["verdict"] == "DIFFERS"]
    if dif:
        L += ["", "DIFFERS, not passed; timed only in rotL* under the label \"different answer\" (Gnomon's total, Raphtory's total):"] + [f"- {r['key']} [{r['stratum']}]: {r['native_total']} , {r['raphtory_total']}" for r in dif]
    Path(a.md).write_text("\n".join(L) + "\n")
    print("\n".join(L), flush=True)
    return 1 if bad or not b else 0


if __name__ == "__main__":
    raise SystemExit(main())
