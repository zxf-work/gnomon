#!/usr/bin/env python3
"""Ablation report: per density point, arm and query, the slowdown of the arm
against its paired unset window, read against the A/A pair's noise.

    abl_report.py <label dir>          (prints markdown; writes report.json)

Pairing: window `X` and window `unset~X` of the same rotation and family.
Ratio = p50(X) / p50(unset~X). Per (point, arm, query) the reported ratio is
the median over rotations; per (point, arm, family) the geometric mean over
the family's queries. The A/A band of a point is the 5th..95th percentile of
the A/A ratios over all its queries and rotations; an arm's family ratio
outside the band is marked `*`. Every pair is also an answer check: the arm's
hash must equal its paired unset hash (the switches keep answers identical);
a mismatch is listed and its ratio is not used. Ingest: wall seconds per
build, median over repetitions, ratio to the unset builds, A/A from the
second unset arm.
"""
import json
import math
import statistics as st
import sys
from collections import defaultdict
from pathlib import Path

FAM_ORDER = ["P", "T", "S", "W", "D", "N", "R", "M", "B"]


def pct(xs, q):
    xs = sorted(xs)
    if not xs:
        return None
    k = (len(xs) - 1) * q
    lo, hi = math.floor(k), math.ceil(k)
    return xs[lo] + (xs[hi] - xs[lo]) * (k - lo)


def gmean(xs):
    xs = [x for x in xs if x and x > 0]
    return math.exp(sum(math.log(x) for x in xs) / len(xs)) if xs else None


def cells(p):
    d = json.loads(p.read_text())
    return {c["query"] + (f"#{c['instance']}" if c.get("instance") else ""): c for c in d.get("results", [])}


def main() -> int:
    A = Path(sys.argv[1])
    report = {"points": {}, "ingest": {}}
    lines = [f"# Ablation report ({A.name})", ""]
    prov = A / "provenance.json"
    if prov.exists():
        lines += [f"Provenance: `{prov.read_text().strip()}`", ""]
    for point in sorted(p for p in A.iterdir() if p.is_dir() and p.name.startswith("K")):
        ratios = defaultdict(list)          # (arm, fam, query) -> [ratio per rotation]
        p50s = defaultdict(list)            # (window, fam, query) -> [p50 ms per rotation]
        mismatch, missing = [], []
        for rot in sorted(point.glob("rot*")):
            for famdir in sorted(rot.iterdir()):
                if not famdir.is_dir():
                    continue
                for rec in famdir.glob("*.json"):
                    arm = rec.stem
                    if arm.startswith("unset~"):
                        continue
                    base = famdir / f"unset~{arm}.json"
                    if not base.exists():
                        missing.append(f"{rot.name}/{famdir.name}/unset~{arm}")
                        continue
                    a, b = cells(rec), cells(base)
                    for q, ca in a.items():
                        cb = b.get(q)
                        if not cb or not ca.get("ok") or not cb.get("ok"):
                            continue
                        if ca.get("hash") != cb.get("hash"):
                            mismatch.append(f"{rot.name} {famdir.name} {arm} {q}")
                            continue
                        if cb.get("p50_ms"):
                            ratios[(arm, famdir.name, q)].append(ca["p50_ms"] / cb["p50_ms"])
                            p50s[(arm, famdir.name, q)].append(ca["p50_ms"])
                            p50s[("unset~" + arm, famdir.name, q)].append(cb["p50_ms"])
        aa = [r for (arm, _f, _q), rs in ratios.items() if arm == "aa" for r in rs]
        band = (pct(aa, 0.05), pct(aa, 0.95)) if aa else (None, None)
        arms = sorted({k[0] for k in ratios})
        fams = [f for f in FAM_ORDER if any(k[1] == f for k in ratios)]
        fam_ratio = {}
        for arm in arms:
            for fam in fams:
                qs = [st.median(rs) for (a_, f_, _q), rs in ratios.items() if a_ == arm and f_ == fam]
                fam_ratio[(arm, fam)] = gmean(qs)
        report["points"][point.name] = {
            "aa_band": band, "answer_mismatches": mismatch, "missing_pairs": missing,
            "query_ratios": {f"{a}|{f}|{q}": st.median(rs) for (a, f, q), rs in ratios.items()},
            "family_ratios": {f"{a}|{f}": v for (a, f), v in fam_ratio.items()}}
        lines += [f"## {point.name}", "",
                  f"A/A band (5th..95th percentile of unset/unset ratios): "
                  f"{band[0]:.3f}..{band[1]:.3f}" if band[0] else "A/A band: no A/A pairs yet", ""]
        lines.append("| arm | " + " | ".join(fams) + " |")
        lines.append("|---|" + "---|" * len(fams))
        for arm in arms:
            row = []
            for fam in fams:
                v = fam_ratio.get((arm, fam))
                if v is None:
                    row.append("—")
                else:
                    out = band[0] is not None and not (band[0] <= v <= band[1])
                    row.append(f"{v:.2f}{' *' if out else ''}")
            lines.append(f"| {arm} | " + " | ".join(row) + " |")
        lines.append("")
        lines.append("Family ratio: geometric mean over the family's queries of the median "
                     "(over rotations) of p50(arm) / p50(paired unset); `*` outside the A/A band.")
        three = ("duckdb_storage", "exec_python", "unset~exec_python")
        if all(any(k[0] == w for k in p50s) for w in three):
            lines += ["", "Three-way split (geometric mean over the family's queries answered by all three "
                      "of the median p50 over rotations, ms): (1) DuckDB storage + Python operators "
                      "(`duckdb_storage`), (2) Gnomon storage + Python operators (`exec_python`), "
                      "(3) Gnomon storage + engine operators (unset, paired with exec_python). "
                      "storage = (1)/(2); execution = (2)/(3).", "",
                      "| family | (1) duckdb_storage | (2) exec_python | (3) default | storage (1)/(2) | execution (2)/(3) |",
                      "|---|---:|---:|---:|---:|---:|"]
            split = {}
            for fam in fams:
                qs = [q for (w, f_, q) in p50s if w == three[0] and f_ == fam
                      and all((w2, fam, q) in p50s for w2 in three[1:])]
                g = [gmean([st.median(p50s[(w, fam, q)]) for q in qs]) for w in three]
                if not qs or None in g:
                    continue
                split[fam] = {"duckdb_storage": g[0], "exec_python": g[1], "default": g[2],
                              "storage": g[0] / g[1], "execution": g[1] / g[2], "queries": qs}
                lines.append(f"| {fam} | {g[0]:.3f} | {g[1]:.3f} | {g[2]:.3f} | {g[0] / g[1]:.2f} | {g[1] / g[2]:.2f} |")
            report["points"][point.name]["three_way"] = split
        if mismatch:
            lines += ["", f"**Answer mismatches ({len(mismatch)}):** " + "; ".join(mismatch[:20])]
        if missing:
            lines += ["", f"Missing pairs ({len(missing)}): " + "; ".join(missing[:20])]
        lines.append("")
    ing = A / "ingest"
    if ing.exists():
        walls = defaultdict(lambda: defaultdict(list))   # size -> arm -> [wall]
        for rec in ing.glob("*.build-record.json"):
            d = json.loads(rec.read_text())
            if d.get("failed"):
                continue
            arm, size, _rep = rec.name.replace(".build-record.json", "").rsplit("-", 2)
            walls[size][arm].append(d["build_info"]["wall_s"])
        lines += ["## Ingest", "", "| size | arm | builds | median wall s | ratio to unset |", "|---|---|---:|---:|---:|"]
        for size, arms in sorted(walls.items()):
            base = st.median(arms["unset"]) if arms.get("unset") else None
            for arm, ws in sorted(arms.items()):
                m = st.median(ws)
                lines.append(f"| {size} | {arm} | {len(ws)} | {m:.1f} | "
                             f"{(m / base) if base else float('nan'):.3f} |")
                report["ingest"].setdefault(size, {})[arm] = {"walls_s": ws, "median_s": m}
        lines.append("")
    rp = A / "ingest-replay"
    if rp.exists():
        walls = defaultdict(lambda: defaultdict(list))   # size -> arm -> [load_s]
        for rec in rp.glob("*.json"):
            d = json.loads(rec.read_text())
            arm, size, _rep = rec.stem.rsplit("-", 2)
            walls[size][arm].append(d["load"]["load_s"])
        lines += ["## Replay ingest (native load of the base log)", "",
                  "| size | arm | replays | median load s | ratio to unset |", "|---|---|---:|---:|---:|"]
        for size, arms in sorted(walls.items()):
            base = st.median(arms["unset"]) if arms.get("unset") else None
            for arm, ws in sorted(arms.items()):
                m = st.median(ws)
                lines.append(f"| {size} | {arm} | {len(ws)} | {m:.1f} | "
                             f"{(m / base) if base else float('nan'):.3f} |")
                report.setdefault("ingest_replay", {}).setdefault(size, {})[arm] = {"loads_s": ws, "median_s": m}
        lines.append("")
    stores = sorted(A.glob("K*/stores/*.json"))
    if stores:
        lines += ["## Store variants (untimed builds, one per point)", "",
                  "| point | variant | load s | bytes |", "|---|---|---:|---:|"]
        for p in stores:
            d = json.loads(p.read_text())
            ld = d.get("load") or {}
            lines.append(f"| {p.parent.parent.name} | {p.stem} | {ld.get('load_s', '—')} | "
                         f"{ld.get('bytes', d.get('bytes', '—'))} |")
        lines.append("")
    (A / "report.json").write_text(json.dumps(report, indent=1, sort_keys=True, default=str) + "\n")
    print("\n".join(lines))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
