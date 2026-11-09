#!/usr/bin/env python3
"""abl_postcheck.py <label dir>: exit 8 unless the ablation is complete: report.json exists, no answer mismatch, no
missing pair, and run_ablation wrote no failed window (window-failed.txt)."""
import json
import sys
from pathlib import Path


def problems(a: Path) -> list:
    out = []
    rp = a / "report.json"
    if not rp.exists():
        return [f"no report.json in {a}"]
    pts = json.loads(rp.read_text()).get("points", {})
    if not pts:
        out.append("report.json has no point")
    for name, p in pts.items():
        if p.get("answer_mismatches"):
            out.append(f"{name}: {len(p['answer_mismatches'])} answer mismatches, first {p['answer_mismatches'][0]}")
        if p.get("missing_pairs"):
            out.append(f"{name}: {len(p['missing_pairs'])} missing pairs, first {p['missing_pairs'][0]}")
    wf = a / "window-failed.txt"
    if wf.exists() and wf.read_text().strip():
        lines = wf.read_text().strip().splitlines()
        out.append(f"{len(lines)} failed windows, first: {lines[0]}")
    return out


def main() -> int:
    bad = problems(Path(sys.argv[1]))
    for b in bad:
        print("POSTCHECK:", b)
    print("POSTCHECK", "FAIL" if bad else "PASS")
    return 8 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
