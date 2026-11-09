#!/usr/bin/env python3
"""W2 timed stage: the answer gate (before any timing).

    w2_gate.py --res <gate job results dir> --tags K1M,K1M-d0,... --out <report.md>

<res> holds one sub-directory per result root of the gate job:
  p1/<tag>/verify-<sys>[-<fam>].json   the answer check of every instance of the W2 parameter file, every system (8), at the
                                       timed protocol's caps (10 s ceiling); load-<sys>.json: the loads
  t32p1/<tag>/                         DuckDB (SQL) at 32 threads with its own Gnomon reference

Checks (any FAIL line makes the exit status 1; nothing is timed then):
  L. every system's load of every point is complete (a record without "load exceeded budget").
  R. every (system, family) verify window left a record (a window cut at the window limit leaves a partial record whose
     unfinished instances are "unreached": counted, not failed; a window with no record at all is a FAIL).
     records; Gnomon's own records of the final-engine tree at 10M): same hash and row count. A difference is a FAIL. A
     recorded cell the new run does not answer is listed (a FAIL only for Gnomon).
  D. the kit's per-instance gate (verdicts.json: every system against Gnomon in the same job): any DIFFERS is a FAIL, except
     the known defects: LadybugDB motif.filtered (the triangle under-count LB-3; its instances are excluded from timing
     by the kit gate). Errors are listed, not failed. Gnomon on DuckDB storage's b.diff NotImplementedError is the known
     exclusion (also in v3 and W1): listed.
  E. Gnomon (native): any error, or a refusal of a call another system answers inside 10 s: FAIL (stop rule).
     Gnomon censored (> 10 s) is listed.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter
from pathlib import Path

E = Path(__import__("os").environ.get("GNOMON_BENCH_ROOT", ".") + "")
W2 = E / "w2-powerlaw"
OLD = {"K1M": [W2 / "check1m/campaign/results"],
       "K10M": [W2 / "check10m/campaign/results", W2 / "check10m/campaign/results-gnomon"]}
SYSTEMS = ["native", "duckdbsql", "clickhouse", "postgres", "duckdb", "neo4j", "memgraph", "ladybug"]
CEIL_MS = 10_000.0
TIMEOUT = ("statement timeout", "canceling statement due to", "TIMEOUT_EXCEEDED", "Timeout exceeded", "client-side limit",
           "INTERRUPT", "Interrupted", "timeout", "TransactionTimedOut", "> 10 s", ">10 s", "aborted")
FILE = re.compile(r"verify-(?P<sys>[a-z0-9]+)(?:-(?P<fam>[A-Z]))?\.json$")
KNOWN_LB_DIFFERS = {"motif.filtered"}            # LB-3 (the triangle under-count, W1 repro; PROBLEMS P18)


def cells(p: Path) -> dict:
    d = json.loads(p.read_text())
    return {(r["query"], r.get("instance") or ""): r for r in d.get("results", [])}


def nx(c: dict) -> bool:
    return not c.get("ok") and str(c.get("error", "")).startswith(("unsupported", "NotImplemented", "not written"))


def unreached(c: dict) -> bool:
    return bool(c.get("unreached"))


def is_timeout(c: dict) -> bool:
    e = str(c.get("error", ""))
    return bool(c.get("aborted")) or any(m in e for m in TIMEOUT)


def same(n: dict, o: dict) -> bool:
    return n.get("hash") == o.get("hash") and n.get("rows") == o.get("rows")


class Rep:
    def __init__(self):
        self.lines: list[str] = []
        self.fail: list[str] = []

    def h(self, s: str):
        self.lines += ["", f"## {s}", ""]

    def p(self, s: str):
        self.lines.append(s)

    def F(self, s: str):
        self.fail.append(s)
        self.lines.append(f"- **FAIL** {s}")


def files(d: Path) -> dict[str, Path]:
    return {p.name: p for p in sorted(d.glob("verify-*.json")) if FILE.search(p.name)}


def old_file(tag: str, name: str) -> Path | None:
    for d in OLD["K1M" if tag.startswith("K1M") else "K10M"]:
        p = d / tag / name
        if p.exists():
            return p
    return None


def check_loads(R: Rep, res: Path, tag: str):
    for s in SYSTEMS:
        p = res / "p1" / tag / f"load-{s}.json"
        if not p.exists():
            R.F(f"L {tag}: no load record for {s}")
            continue
        d = json.loads(p.read_text())
        if "load" not in d or d.get("load exceeded budget"):
            R.F(f"L {tag}: load of {s} not complete ({'exceeded its budget' if d.get('load exceeded budget') else 'no load entry'})")


def check_records(R: Rep, d: Path, tag: str, label: str, systems):
    fams = ["B"] if "-d" in tag else list("PTSWDNRM")
    miss, part = [], Counter()
    for s in systems:
        for f in fams:
            name = f"verify-{s}.json" if f == "B" else f"verify-{s}-{f}.json"
            p = d / name
            if not p.exists():
                miss.append(name)
                continue
            doc = json.loads(p.read_text())
            if doc.get("partial"):
                part[s] += sum(1 for c in doc["results"] if unreached(c))
    for m in miss:
        R.F(f"R {label} {tag}: no verify record {m} (window failed to start or was killed before its first cell)")
    R.p(f"- R {label} {tag}: unreached instances (answer-check window limit): " + (", ".join(f"{s} {n}" for s, n in sorted(part.items())) or "none"))


def compare_old(R: Rep, label: str, new: Path, tag: str, systems=None):
    t_all = Counter()
    for name, p in files(new).items():
        sysn = FILE.search(name)["sys"]
        if systems and sysn not in systems:
            continue
        op = old_file(tag, name)
        if op is None:
            R.p(f"- {label} {name}: no recorded W2 check file; compared with Gnomon in this job only (check D)")
            continue
        n_c, o_c = cells(p), cells(op)
        t = Counter()
        for k in sorted(set(n_c) & set(o_c)):
            n, o = n_c[k], o_c[k]
            key = f"{k[0]}#{k[1]}" if k[1] else k[0]
            if n.get("ok") and o.get("ok"):
                if same(n, o):
                    t["equal"] += 1
                else:
                    t["DIFFERS"] += 1
                    R.F(f"{label} {name}: {key} DIFFERS: new {n.get('hash')} rows {n.get('rows')}, recorded {o.get('hash')} rows {o.get('rows')} ({op.parent.name})")
            elif n.get("ok"):
                t["now ok (recorded not ok)"] += 1
            elif o.get("ok"):
                if unreached(n):
                    t["unreached now"] += 1
                elif is_timeout(n) and sysn != "native":
                    t["censored now (recorded ok)"] += 1
                elif sysn == "native":
                    t["NEW NOT OK"] += 1
                    R.F(f"{label} {name}: {key} recorded ok, Gnomon now not ok: {str(n.get('error'))[:200]}")
                else:
                    t["not ok now (recorded ok)"] += 1
                    R.p(f"  - {label} {name}: {key} recorded ok, now not ok: {str(n.get('error'))[:120]}")
            else:
                t["not expressible" if nx(n) else "both not ok"] += 1
        t_all.update(t)
        R.p(f"- {label} {name}: " + ", ".join(f"{v} {c}" for c, v in sorted(t.items())))
    R.p(f"- **{label} total**: " + ", ".join(f"{v} {c}" for c, v in sorted(t_all.items())))


def kit_verdicts(R: Rep, d: Path, label: str):
    vj = d / "verdicts.json"
    if not vj.exists():
        R.F(f"{label}: no verdicts.json (kit gate not run) in {d}")
        return
    v = json.loads(vj.read_text())
    tally = {s: Counter((("unreached" if "unreached" in str(r.get("detail", "")) else r["verdict"])) for r in rows) for s, rows in v.items()}
    for s, rows in v.items():
        for r in rows:
            if r["verdict"] == "DIFFERS":
                k = f"{r['query']}#{r.get('instance')}"
                if s == "ladybug" and r["query"] in KNOWN_LB_DIFFERS:
                    R.p(f"  - {label}: LadybugDB {k} DIFFERS from Gnomon: the known LB-3 defect (excluded from timing)")
                else:
                    R.F(f"{label} kit gate: {s} {k} DIFFERS from Gnomon")
            elif s == "duckdb" and r["verdict"] in ("error", "not expressible") and r["query"].startswith("b.diff"):
                tally[s]["known: b.diff NotImplementedError"] += 1
    ex = json.loads((d / "excluded.json").read_text()) if (d / "excluded.json").exists() else {}
    R.p(f"- {label} kit gate: " + "; ".join(f"{s}: " + ", ".join(f"{k} {n}" for k, n in sorted(c.items())) for s, c in sorted(tally.items()))
        + "; excluded from timing: " + (", ".join(f"{s} {len(e)}" for s, e in sorted(ex.items())) or "none"))


def native_rule(R: Rep, d: Path, label: str):
    """Stop rule: Gnomon errors, or refuses a call another system answers inside 10 s."""
    by = {}
    for name, p in files(d).items():
        by.setdefault(FILE.search(name)["sys"], {}).update(cells(p))
    nat = by.get("native", {})
    if not nat:
        R.F(f"{label}: no Gnomon cells")
        return
    bad = 0
    for k, c in nat.items():
        if c.get("ok"):
            continue
        key = f"{k[0]}#{k[1]}" if k[1] else k[0]
        others = sorted(s for s, cs in by.items() if s != "native" and cs.get(k, {}).get("ok") and cs[k].get("wall_ms", 1e18) <= CEIL_MS)
        err = str(c.get("error", ""))
        if c.get("refused") or "E_COST" in err or "guardrail" in err.lower():
            if others:
                R.F(f"{label}: Gnomon REFUSED {key} ({err[:160]}); answered inside 10 s by {others}")
            else:
                R.p(f"  - {label}: Gnomon refused {key} (estimated > 10 s); no other system inside 10 s")
        elif is_timeout(c):
            R.p(f"  - {label}: Gnomon > 10 s on {key}; inside 10 s: {others or 'none'}")
        else:
            R.F(f"{label}: Gnomon ERROR {key}: {err[:200]}")
        bad += 1
    R.p(f"- {label}: Gnomon cells {len(nat)}, not ok {bad}")


def main() -> int:
    global SYSTEMS
    ap = argparse.ArgumentParser()
    ap.add_argument("--res", type=Path, required=True)
    ap.add_argument("--tags", required=True)
    ap.add_argument("--out", type=Path, required=True)
    # the Gnomon-only mode: the recorded gate jobs' result dirs; only Gnomon and Gnomon on
    # DuckDB storage ran (loads, records and the t32p1 root are checked for those only) and check G
    # compares their answers with the recorded gate. Without it: unchanged.
    ap.add_argument("--ref-gate", type=Path, nargs="+")
    a = ap.parse_args()
    G = None
    if a.ref_gate:
        sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "confirm" / "tools"))
        import c3_refgate as G   # noqa: E402
        SYSTEMS = ["native", "duckdb"]
    R = Rep()
    R.p(f"# W2 answer gate: {a.res}" + (f" (Gnomon-only mode; recorded gate {', '.join(map(str, a.ref_gate))})" if G else ""))
    for tag in a.tags.split(","):
        R.h(tag)
        p1, t32 = a.res / "p1" / tag, a.res / "t32p1" / tag
        R.p("### L. loads")
        check_loads(R, a.res, tag)
        R.p("### R. verify records")
        check_records(R, p1, tag, "p1", SYSTEMS)
        if not G:
            check_records(R, t32, tag, "t32p1", ["native", "duckdbsql"])
        R.p("### A. against the preparation stage's W2 checks")
        compare_old(R, f"A {tag}", p1, tag)
        if not G:
            compare_old(R, f"A {tag} 32 threads", t32, tag, systems={"duckdbsql"})
        R.p("### D/E. the kit's per-instance gate and the Gnomon stop rule")
        kit_verdicts(R, p1, f"p1 {tag}")
        native_rule(R, p1, f"p1 {tag}")
        if not G:
            kit_verdicts(R, t32, f"t32p1 {tag}")
            native_rule(R, t32, f"t32p1 {tag}")
        else:
            R.p("### G/E2. Gnomon and Gnomon on DuckDB storage against the recorded gate (Gnomon-only mode)")
            G.check_root(R, f"p1 {tag}", p1, a.ref_gate, "p1", tag)
    R.lines.insert(1, "")
    R.lines.insert(2, f"**Verdict: {'FAIL' if R.fail else 'PASS'}** ({len(R.fail)} failing checks)")
    a.out.write_text("\n".join(R.lines) + "\n")
    print(f"GATE {'FAIL' if R.fail else 'PASS'}: {len(R.fail)} failing checks; report {a.out}")
    for f in R.fail[:60]:
        print("  FAIL", f)
    return 1 if R.fail else 0


if __name__ == "__main__":
    sys.exit(main())
