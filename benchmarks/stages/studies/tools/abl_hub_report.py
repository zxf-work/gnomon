#!/usr/bin/env python3
"""abl_hub_report.py <label dir> --out PREFIX: W2 ablation, per (point, configuration, query, hub | non-hub).

For each pair (window X, window unset~X; same rotation, family) and each instance answered by the unset window: ratio =
p50(X) / p50(unset~X). A cell X censored at the ceiling has no ratio: it counts as a lower bound, ceiling / p50(unset~X), and
n_censored says how many; a group with a censored cell is printed with ">=". Per instance the value is the median over
rotations; per group the median over instances. The A/A row of a group is its noise. A differing answer hash is listed and not
used; an unset window that did not answer leaves the instance out, counted in n_unset_failed."""
import argparse
import json
import statistics as st
from collections import defaultdict
from pathlib import Path


def is_censored(c: dict) -> bool:
    return bool(c.get("censored")) or (not c.get("ok") and str(c.get("error", "")).startswith(">"))


def ceiling_ms(c: dict) -> float:
    e = str(c.get("error", ""))
    try:
        return float(e[1:].split()[0]) * 1000
    except (ValueError, IndexError):
        return 10000.0


def group(c: dict) -> str:
    return "hub" if c.get("stratum") == "hub" else "non-hub"


def collect(a: Path):
    vals = defaultdict(lambda: defaultdict(list))   # (point, arm, query, group) -> instance -> [(ratio, censored)]
    mism = []
    unset_failed = defaultdict(int)
    for point in sorted(p for p in a.iterdir() if p.is_dir() and p.name.startswith("K")):
        for rot in sorted(point.glob("rot*")):
            for fam in sorted(x for x in rot.iterdir() if x.is_dir()):
                for rec in fam.glob("*.json"):
                    arm = rec.stem
                    base = fam / f"unset~{arm}.json"
                    if arm.startswith("unset~") or not base.exists():
                        continue
                    A = {(c["query"], c.get("instance")): c for c in json.loads(rec.read_text()).get("results", [])}
                    B = {(c["query"], c.get("instance")): c for c in json.loads(base.read_text()).get("results", [])}
                    for k, ca in A.items():
                        cb = B.get(k)
                        g = group(ca)
                        if not cb or not cb.get("ok") or not cb.get("p50_ms"):
                            unset_failed[(point.name, arm, k[0], g)] += 1
                            continue
                        if ca.get("ok") and ca.get("hash") != cb.get("hash"):
                            mism.append(f"{point.name} {rot.name} {fam.name} {arm} {k[0]}#{k[1]}")
                            continue
                        if ca.get("ok") and ca.get("p50_ms"):
                            vals[(point.name, arm, k[0], g)][k[1]].append((ca["p50_ms"] / cb["p50_ms"], False))
                        elif is_censored(ca):
                            vals[(point.name, arm, k[0], g)][k[1]].append((ceiling_ms(ca) / cb["p50_ms"], True))
    return vals, mism, unset_failed


def confirmation(root: Path, points: dict) -> dict:
    """DuckDB storage from the confirmation's W2 blocks: Gnomon on DuckDB storage / Gnomon, per instance, median over rotations. Not back to
    back with its unset run, and only the instances the ablation's parameter files keep."""
    vals = defaultdict(lambda: defaultdict(list))
    for point, src in points.items():
        for rot in sorted((root / src).glob("rot[0-9]*")):
            for f in rot.glob("*-duckdb.json"):
                base = f.with_name(f.name.replace("-duckdb.json", "-native.json"))
                if not base.exists():
                    continue
                A = {(c["query"], c.get("instance")): c for c in json.loads(f.read_text()).get("results", [])}
                B = {(c["query"], c.get("instance")): c for c in json.loads(base.read_text()).get("results", [])}
                for k, ca in A.items():
                    cb = B.get(k)
                    if not cb or not cb.get("ok") or not cb.get("p50_ms"):
                        continue
                    g = group(ca)
                    if ca.get("ok") and ca.get("p50_ms"):
                        vals[(point, "duckdb_storage (confirmation)", k[0], g)][k[1]].append((ca["p50_ms"] / cb["p50_ms"], False))
                    elif is_censored(ca):
                        vals[(point, "duckdb_storage (confirmation)", k[0], g)][k[1]].append((ceiling_ms(ca) / cb["p50_ms"], True))
    return vals


def summarize(a: Path, confirm: Path = None, keep: set = None) -> dict:
    vals, mism, unset_failed = collect(a)
    if confirm:
        pts = sorted(p.name for p in a.iterdir() if p.is_dir() and p.name.startswith("K"))
        srcs = {p: (p.split("-d")[0] if not p.endswith("-d10") else p) for p in pts}
        for k, v in confirmation(confirm, srcs).items():
            vals[k] = {i: r for i, r in v.items() if keep is None or i in keep}
        for p in pts:                     # the B family of the first density point
            if p.endswith("-d0"):
                for k, v in confirmation(confirm, {p: p}).items():
                    vals[k].update({i: r for i, r in v.items() if keep is None or i in keep})
    vals = {k: v for k, v in vals.items() if v}
    rows = []
    for (pt, arm, q, g), inst in sorted(vals.items()):
        per = [st.median(r for r, _ in v) for v in inst.values()]
        cens = sum(1 for v in inst.values() if any(c for _, c in v))
        rows.append({"point": pt, "arm": arm, "query": q, "group": g, "n": len(per), "n_censored": cens,
                     "median_ratio": st.median(per), "min": min(per), "max": max(per),
                     "n_unset_failed": unset_failed.get((pt, arm, q, g), 0)})
    return {"rows": rows, "answer_mismatches": mism}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("label", type=Path)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--confirm", type=Path, help="the confirmation's W2 p1 result root: adds DuckDB storage, not back to back")
    ap.add_argument("--params", type=Path, help="the ablation's parameter files: the instances --confirm keeps")
    a = ap.parse_args()
    keep = None
    if a.params:
        keep = set()
        for p in a.params.glob("params-*.json"):
            keep.update(i["instance"] for i in json.loads(p.read_text())["instances"])
    s = summarize(a.label, a.confirm, keep)
    Path(str(a.out) + ".json").write_text(json.dumps(s, indent=1))
    lines = [f"# W2 ablation by hub and non-hub instances ({a.label.name})", "",
             "| point | configuration | query | group | n | censored | median ratio | min | max |", "|---|---|---|---|---:|---:|---:|---:|---:|"]
    for r in s["rows"]:
        sign = ">=" if r["n_censored"] else ""
        lines.append(f"| {r['point']} | {r['arm']} | {r['query']} | {r['group']} | {r['n']} | {r['n_censored']} | {sign}{r['median_ratio']:.3f} | {r['min']:.3f} | {r['max']:.3f} |")
    if s["answer_mismatches"]:
        lines += ["", f"**Answer mismatches ({len(s['answer_mismatches'])}):** " + "; ".join(s["answer_mismatches"][:20])]
    Path(str(a.out) + ".md").write_text("\n".join(lines) + "\n")
    print(f"hub report: {len(s['rows'])} rows, {len(s['answer_mismatches'])} mismatches -> {a.out}.md")
    if not s["rows"]:
        print("hub report: no row (no paired window with an instance)")
        return 8
    return 7 if s["answer_mismatches"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
