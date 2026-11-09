#!/usr/bin/env python3
"""mp_combined.py --job DIR [--job DIR ...] --out combined.md: one table over every timed matchpath job, the same rules as
mp_table.py (it calls mp_table.point on each job's records), one row per (job, point, query). Descriptive."""
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import mp_table as T  # noqa: E402

E = Path(__import__("os").environ.get("GNOMON_BENCH_ROOT", ".") + "")
DUCK = {"w1": [E / "final/w1/results"], "w1x": [E / "final/w1/results"], "w1y": [E / "final/w1/results"],
        "w2": [E / "final/w2/results"], "w2y": [E / "final/w2/results"],
        "w2d": [E / "studies/t10/results/timed-w2-J55", E / "final/w2/results"],
        "w5": [E / "studies/w5/results/W5-wikidata-v2-J49"]}
NAME = {"w1": "W1", "w1x": "W1", "w1y": "W1", "w2": "W2", "w2y": "W2", "w2d": "W2", "w5": "W5"}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--job", action="append", required=True)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    rows = []
    for j in a.job:
        jd = Path(j)
        for wl in ("w1", "w1x", "w1y", "w2", "w2y", "w2d", "w5"):
            for dirp in sorted((jd / wl).glob("*")):
                if not (dirp / "params-used.json").exists() or not (dirp / "rot1").is_dir():
                    continue        # a point the job did not reach its timing in (its limit): not in the table
                js = {}
                T.point(dirp, wl, DUCK[wl], js)
                for q, r in js.get(wl, {}).items():
                    rows.append((jd.name, NAME[wl], dirp.name, q, r))
    f = T.f3
    head = ["job", "workload", "point", "query", "n", "dedicated ms (answered)", "match ms (answered)",
            "match / dedicated per instance: median (min, max)", "ratio of medians", "match faster on",
            "DuckDB (SQL) ms, its record (answered)", "match / DuckDB, ratio of medians", "censored d / m",
            "refused d / m", "excluded m", "engine ms d / m", "timed answer hashes equal"]
    out = []
    for job, w, pt, q, r in rows:
        c = r["counts"]
        eng = {e["system"]: e for e in r["engine"]}
        ed = (eng.get("native") or {}).get("engine_ms")
        em = (eng.get("nativematch") or {}).get("engine_ms")
        note = f" ({T.FIELDS_EXCLUDED[q]} excluded from the comparison)" if q in T.FIELDS_EXCLUDED else ""
        out.append([job, w, pt, q + note, r["n"], f"{f(r['dedicated_median_ms'])} ({r['dedicated_answered']})",
                    f"{f(r['match_median_ms'])} ({r['match_answered']})",
                    f"{f(r['ratio_per_instance_median'])} ({f(r['ratio_per_instance_min'])}, {f(r['ratio_per_instance_max'])})",
                    f(r["ratio_of_medians"]), f"{r['match_faster_on']}",
                    f"{f(r['duckdb_median_ms'])} ({r['duckdb_answered']})", f(r["match_over_duckdb_ratio_of_medians"]),
                    f"{c['native'].get('cens', 0)} / {c['nativematch'].get('cens', 0)}",
                    f"{c['native'].get('refused', 0)} / {c['nativematch'].get('refused', 0)}",
                    c["nativematch"].get("excluded", 0), f"{f(ed)} / {f(em)}",
                    f"{r['timed_hashes_equal']} of {r['timed_hashes_compared']}"])
    lines = ["# Dedicated operator against the match path, every timed matchpath job (descriptive)", "",
             "End-to-end ms, instance value = mean of the two rotations' p50s, query value = median over answered instances; "
             "ratios on instances both answered, same job and node. DuckDB (SQL) from its own records (another job and node). "
             "Engine ms: the engine pass. Censored = > 10 s.", ""]
    lines += T.W.md_table(head, out)
    Path(a.out).write_text("\n".join(lines) + "\n")
    Path(a.out + ".json").write_text(json.dumps([{"job": j, "workload": w, "point": p, "query": q, **r}
                                                 for j, w, p, q, r in rows], indent=1, default=str) + "\n")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
