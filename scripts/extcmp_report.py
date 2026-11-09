"""Assemble the Phase A tables (per system: load, bytes, per-query verdict)
from the extcmp records directory.

    extcmp_report.py <phase-a dir> [--sizes 200000,1000000] > phase-a-tables.md
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

SYSTEMS = ("native", "duckdb", "postgres", "clickhouse", "neo4j", "memgraph", "xtdb",
           "ladybug", "aeong", "raphtory")
SHORT = {"equivalent": "eq", "DIFFERS": "DIFF", "not written": "n/w",
         "not expressible": "n/e", "refused": "ref", "error": "err"}


def _load(p: Path):
    try:
        return json.loads(p.read_text())
    except Exception:                                       # noqa: BLE001
        return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("root", type=Path)
    ap.add_argument("--sizes", default="200000,1000000")
    args = ap.parse_args()
    for size in args.sizes.split(","):
        d = args.root / size
        if not d.is_dir():
            continue
        print(f"\n### input size {int(size):,}\n")
        print("| system | load s (phases) | store bytes | verdicts |")
        print("|---|---|---:|---|")
        verdicts = {}
        for s in SYSTEMS:
            ld = _load(d / f"load-{s}.json")
            vr = _load(d / f"verdict-{s}.json")
            by = _load(d / f"bytes-{s}.json")
            if ld is None and vr is None:
                continue
            L = (ld or {}).get("load", {})
            ph = ", ".join(f"{k} {v}" for k, v in (L.get("phases") or {}).items()
                           if isinstance(v, (int, float)))
            b = (by or {}).get("data_dir_bytes_after_stop") or L.get("bytes") \
                or L.get("pg_database_size") or L.get("bytes_on_disk_active_parts")
            rows = (vr or {}).get(s, [])
            verdicts[s] = {r["query"]: r for r in rows}
            tally = {}
            for r in rows:
                tally[r["verdict"]] = tally.get(r["verdict"], 0) + 1
            t = ", ".join(f"{k} {v}" for k, v in sorted(tally.items())) or (
                "reference" if s == "native" else "-")
            print(f"| {s} | {L.get('load_s', '-')} ({ph}) | {b if b is not None else '-'} | {t} |")
        qids = []
        for s, m in verdicts.items():
            for q in m:
                if q not in qids:
                    qids.append(q)
        cols = [s for s in SYSTEMS if s in verdicts]
        print("\n| query | " + " | ".join(cols) + " |")
        print("|---|" + "---|" * len(cols))
        for q in qids:
            cells = []
            for s in cols:
                r = verdicts[s].get(q)
                if r is None:
                    cells.append("-")
                    continue
                v = SHORT.get(r["verdict"], r["verdict"])
                if r["verdict"] == "equivalent" and "projected" in r.get("by", ""):
                    v = "eq(p)"
                cells.append(v)
            print(f"| {q} | " + " | ".join(cells) + " |")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
