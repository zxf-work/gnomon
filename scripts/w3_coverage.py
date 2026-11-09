"""W3 coverage table: per query x setting x system, the answer-check verdict against Gnomon.


A Gnomon window cell refused by the operator is marked with its gap:
- G1: a variable-length step or dir=both under a window;
- G2: optional/not under a window;
- G3: `paths` has one valid instant.

"""
from __future__ import annotations
import argparse
import collections
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

GAPS = (("single directed hop", "blocked: G1"), ("optional/not sub-patterns", "blocked: G2"),
        ("one valid instant per call", "blocked: G3"), ("blocked: S21", "blocked: S21"))
KNOWN = {}   # the P6 corpus rows are patched; add a row here only with its documented cause
#: answers empty at every instance in every system for a documented reason (PROBLEMS P11): the check is vacuous
VACUOUS = {"BI20": "vacuous (Datagen)"}
SYSTEMS = ["duckdb", "postgres", "neo4j", "memgraph", "aeong", "aeong-prop"]   # ClickHouse is not in W3 (trim order)
#: AeonG is in W3 for the T-LDBC stage only
AEONG_SCOPE = lambda q: q.startswith(("IS", "TLDBC-IS"))


def load(p: Path) -> dict:
    from w3_snb_run import load_results
    return load_results(str(p)) if p.exists() else {}


def verdict(a: dict | None, b: dict | None) -> str:
    if a is None or b is None:
        return "missing"
    if not a.get("ok"):
        return "ref-" + a.get("kind", "error")
    if not b.get("ok"):
        return b.get("kind", "error")
    if a["hash"] == b["hash"]:
        return "eq"
    if a["sorted_hash"] == b["sorted_hash"]:
        return "eq-order"
    return "DIFFERS"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", required=True)
    ap.add_argument("--out")
    ap.add_argument("--gnomon", default="gnomon.jsonl", help="the Gnomon result file in --dir (e.g. gnomon-<engine>.jsonl)")
    a = ap.parse_args()
    d = Path(a.dir)
    res = {s: load(d / f"{s}.jsonl") for s in SYSTEMS}
    res["gnomon"] = load(d / a.gnomon)
    qs = sorted({(k[0], k[2]) for r in res.values() for k in r}, key=lambda t: (t[0].rstrip("0123456789"), int("".join(c for c in t[0] if c.isdigit()) or 0), t[0], t[1]))
    lines = ["| query | setting | Gnomon | " + " | ".join(SYSTEMS) + " |", "|---|---|---|" + "---|" * len(SYSTEMS)]
    for q, st in qs:
        keys = sorted(k for k in res["gnomon"] if k[0] == q and k[2] == st) or \
            sorted({k for s in SYSTEMS for k in res[s] if k[0] == q and k[2] == st})
        g = [res["gnomon"].get(k) for k in keys]
        gcell = "ok" if all(x and x.get("ok") for x in g) else None
        if gcell is None:
            errs = [x.get("err", "") for x in g if x and not x.get("ok")]
            gcell = next((lab for pat, lab in GAPS if any(pat in e for e in errs)), None) or \
                ("not mapped" if any(x and x.get("kind") == "not-expressible" for x in g) else "error")
        if gcell == "ok" and q in VACUOUS and all(x and x.get("n") == 0 for x in g):
            gcell = "ok, " + VACUOUS[q]
        row = [q, st, gcell]
        for s in SYSTEMS:
            if s.startswith("aeong") and not AEONG_SCOPE(q):
                row.append("not in scope")
                continue
            if not res[s]:
                row.append("—")
                continue
            if gcell.startswith("ok"):
                base = res["gnomon"]
            else:      # cross-check against an independent baseline text: SQL vs Cypher
                other = "neo4j" if s in ("duckdb", "postgres", "clickhouse") else "duckdb"
                base = res[other]
            c = collections.Counter(verdict(base.get(k), res[s].get(k)) for k in keys)
            txt = ", ".join(f"{v} {n}" for v, n in sorted(c.items()))
            if "DIFFERS" in c and q.replace("TLDBC-", "") in KNOWN and gcell.startswith("ok"):
                txt += f" ({KNOWN[q.replace('TLDBC-', '')]})"
            if not gcell.startswith("ok"):
                txt = "vs " + ("neo4j" if s in ("duckdb", "postgres", "clickhouse") else "duckdb") + ": " + txt
            row.append(txt)
        lines.append("| " + " | ".join(row) + " |")
    text = "\n".join(lines) + "\n"
    if a.out:
        Path(a.out).write_text(text)
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
