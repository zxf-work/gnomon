#!/usr/bin/env python3
"""Ingest series summary: one row per build (wall, rows/s, peak memory, store
bytes) and the per-phase breakdown (build_info.ingest_phase_totals), with the
local growth exponent of wall time between consecutive sizes.

    series_report.py <ingest-series dir>     (prints markdown; writes summary.json)
"""
import json
import math
import sys
from pathlib import Path


def phase_table(pt):
    """{phase or group: seconds} from ingest_phase_totals, whatever its nesting."""
    out = {}
    if not isinstance(pt, dict):
        return out
    src = pt.get("groups") or pt.get("phases") or pt
    for k, v in src.items():
        if isinstance(v, (int, float)):
            out[k] = float(v)
        elif isinstance(v, dict):
            for kk in ("s", "seconds", "wall_s", "total_s"):
                if isinstance(v.get(kk), (int, float)):
                    out[k] = float(v[kk])
                    break
    return out


def main() -> int:
    S = Path(sys.argv[1])
    rows = []
    for rec in sorted(S.glob("*.build-record.json")):
        d = json.loads(rec.read_text())
        name = rec.name.replace(".build-record.json", "")
        if d.get("failed"):
            rows.append({"name": name, "failed": True, "rc": d.get("rc")})
            continue
        bi = d["build_info"]
        mem = S / f"{name}.mem.jsonl"
        hwm = 0
        if mem.exists():
            for line in mem.read_text().splitlines():
                try:
                    hwm = max(hwm, json.loads(line).get("vmhwm_kb", 0))
                except ValueError:
                    pass
        sb = S / f"{name}.store-bytes"
        rows.append({"name": name, "n": bi.get("n_entities"), "wall_s": bi.get("wall_s"),
                     "rows_per_s": (bi.get("n_entities") or 0) / bi["wall_s"] if bi.get("wall_s") else None,
                     "peak_rss_gb": (((bi.get("peak_rss") or {}).get("maxrss_kb") or hwm) / 1e6) or None,
                     "store_gb": int(sb.read_text()) / 1e9 if sb.exists() else None,
                     "rows_rewritten": bi.get("compaction_rows_rewritten"),
                     "phases": phase_table(bi.get("ingest_phase_totals")),
                     "ingest_s": (bi.get("ingest_phase_totals") or {}).get("ingest_s"),
                     "commit": d.get("git_commit")})
    ok = sorted([r for r in rows if not r.get("failed")], key=lambda r: r["n"] or 0)
    for a, b in zip(ok, ok[1:]):
        if a["wall_s"] and b["wall_s"] and a["n"] and b["n"] and b["n"] != a["n"]:
            b["local_exponent"] = math.log(b["wall_s"] / a["wall_s"]) / math.log(b["n"] / a["n"])
    (S / "summary.json").write_text(json.dumps(rows, indent=1, sort_keys=True) + "\n")
    print("# Ingest series\n")
    print("| build | events | wall s | rows/s | local exponent | peak RSS GB | store GB | rows rewritten |")
    print("|---|---:|---:|---:|---:|---:|---:|---:|")
    for r in ok:
        f = lambda v, p=1: "—" if v is None else f"{v:,.{p}f}"
        print(f"| {r['name']} | {r['n']:,} | {f(r['wall_s'])} | {f(r['rows_per_s'], 0)} | "
              f"{f(r.get('local_exponent'), 2)} | {f(r['peak_rss_gb'])} | {f(r['store_gb'])} | "
              f"{f(r['rows_rewritten'], 0)} |")
    for r in rows:
        if r.get("failed"):
            print(f"| {r['name']} | failed (rc {r.get('rc')}) | | | | | | |")
    phases = sorted({p for r in ok for p in r["phases"]})
    if phases:
        print("\n## Share of ingest time by phase\n")
        print("| build | " + " | ".join(phases) + " |")
        print("|---|" + "---:|" * len(phases))
        for r in ok:
            tot = r.get("ingest_s") or r["wall_s"] or 1
            print(f"| {r['name']} | " + " | ".join(
                f"{100 * r['phases'].get(p, 0) / tot:.1f} %" for p in phases) + " |")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
