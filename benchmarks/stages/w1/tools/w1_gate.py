#!/usr/bin/env python3
"""W1 timed stage: the answer gate (before any timing).

    w1_gate.py --res <gate job results dir> --tags K1M,K10M,... --out <report.md>

<res> holds one sub-directory per result root of the gate job:
  v3gate/<tag>/verify-<sys>.json      the answer check exactly as v3 ran it (no parameter file, v3's caps)
  v3gate-t32/<tag>/verify-duckdbsql.json   the same for DuckDB (SQL) at 32 threads
  sp/<tag>/verify-<sys>.json          the single-parameter instances through the parameter-file path (10 s ceiling)
  p1/<tag>/verify-<sys>[-<fam>].json  the curated P1 instances (run_p1.sh)
  t32sp/, t32p1/                      DuckDB (SQL) at 32 threads, same two paths (with their own native reference)

Checks (any FAIL line makes the exit status 1; nothing is timed then):
  A. v3gate against v3 (v3/sa, v3/sb; v3/sat32 for 32 threads): every id, size and density, every system:
     same answer hash and row count. A recorded answer the new run does not give, or a missing cell: FAIL.
  B. sp against v3gate in this job: the parameter-file instance gives the same hash as the registry call
     (so the timed instance is the v3 instance). A cell above the 10 s ceiling is "censored", not compared.
  C. p1 against the P1 study's records: same hash per instance.
     A cell censored now that answered then (or the reverse) is listed, not failed, unless it is Gnomon's.
  D. The kit's own per-instance gate (verdicts.json of sp, p1, t32sp, t32p1: every system against Gnomon
     in the same job): any DIFFERS is a FAIL.
  E. Gnomon (native): any error, or a refusal of a call another system answers inside 10 s: FAIL (stop rule).
     Gnomon censored (> 10 s) is listed.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

E = Path(__import__("os").environ.get("GNOMON_BENCH_ROOT", ".") + "")
V3 = {"reg": E / "v3/sa/campaign/results", "bel": E / "v3/sb/campaign/results", "t32": E / "v3/sat32/campaign/results"}
P1OLD = {"K10M": E / "p1-sens/reg/campaign/results", "K10M-d0": E / "p1-sens/bel2/campaign/results",
         "K10M-d10": E / "p1-sens/bel2/campaign/results"}
P1OLD_T32 = E / "p1-sens/t32/campaign/results"
CEIL_MS = 10_000.0
TIMEOUT = ("statement timeout", "canceling statement due to", "TIMEOUT_EXCEEDED", "Timeout exceeded",
           "client-side limit", "INTERRUPT", "Interrupted", "timeout", "> 10 s", ">10 s", "aborted")
FILE = re.compile(r"verify-(?P<sys>[a-z0-9]+)(?:-(?P<fam>[A-Z]))?\.json$")


def cells(p: Path) -> dict:
    d = json.loads(p.read_text())
    return {(r["query"], r.get("instance") or ""): r for r in d.get("results", [])}


def nx(c: dict) -> bool:
    return not c.get("ok") and str(c.get("error", "")).startswith(("unsupported", "NotImplemented", "not written"))


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


def compare_dirs(R: Rep, label: str, new: Path, old: Path, *, strict_new_not_ok: bool, systems=None,
                 old_name=lambda n: n):
    """Each new verify file against the recorded file of the same name."""
    tally_all = Counter()
    nf = files(new)
    if not nf:
        R.F(f"{label}: no verify records in {new}")
        return
    for name, p in nf.items():
        sysn = FILE.search(name)["sys"]
        if systems and sysn not in systems:
            continue
        op = old / old_name(name)
        if not op.exists():
            R.p(f"- {label} {name}: no recorded file ({op}); compared with Gnomon in this job only (check D)")
            continue
        n_c, o_c = cells(p), cells(op)
        t = Counter()
        for k in sorted(set(n_c) | set(o_c)):
            n, o = n_c.get(k), o_c.get(k)
            key = f"{k[0]}#{k[1]}" if k[1] else k[0]
            if n is None:
                t["missing (new)"] += 1
                R.F(f"{label} {name}: {key} missing in the new run")
                continue
            if o is None:
                t["not recorded"] += 1
                continue
            if n.get("ok") and o.get("ok"):
                if same(n, o):
                    t["equal"] += 1
                else:
                    t["DIFFERS"] += 1
                    R.F(f"{label} {name}: {key} DIFFERS: new {n.get('hash')} rows {n.get('rows')}, "
                        f"recorded {o.get('hash')} rows {o.get('rows')}")
            elif n.get("ok"):
                t["now ok (recorded not ok)"] += 1
                R.p(f"  - {label} {name}: {key} now answers (recorded: {str(o.get('error'))[:90]})")
            elif o.get("ok"):
                if is_timeout(n) and not strict_new_not_ok and sysn != "native":
                    t["censored now (recorded ok)"] += 1
                    R.p(f"  - {label} {name}: {key} censored now, recorded ok at {o.get('wall_ms')} ms "
                        f"({str(n.get('error'))[:90]})")
                else:
                    t["NEW NOT OK"] += 1
                    R.F(f"{label} {name}: {key} recorded ok ({o.get('wall_ms')} ms), new not ok: "
                        f"{str(n.get('error'))[:200]}")
            else:
                t["not expressible" if nx(n) else "both not ok"] += 1
        tally_all.update(t)
        R.p(f"- {label} {name}: " + ", ".join(f"{v} {c}" for c, v in sorted(t.items())))
    R.p(f"- **{label} total**: " + ", ".join(f"{v} {c}" for c, v in sorted(tally_all.items())))


def sp_vs_v3gate(R: Rep, sp: Path, v3g: Path):
    """The parameter-file instance and the registry call: same answer on the same store."""
    g = {}
    for name, p in files(v3g).items():
        g[FILE.search(name)["sys"]] = {k[0]: c for k, c in cells(p).items()}
    t = Counter()
    for name, p in files(sp).items():
        sysn = FILE.search(name)["sys"]
        for (q, inst), c in cells(p).items():
            ref = g.get(sysn, {}).get(q)
            if ref is None:
                t["no v3gate cell"] += 1
                R.F(f"sp {sp.name} {sysn} {q}: no registry-call cell in v3gate")
                continue
            if c.get("ok") and ref.get("ok"):
                if same(c, ref):
                    t["equal"] += 1
                else:
                    t["DIFFERS"] += 1
                    R.F(f"sp {sp.name} {sysn} {q}#{inst}: parameter-file hash {c.get('hash')} != registry call {ref.get('hash')}")
            elif not c.get("ok") and is_timeout(c):
                t["censored at 10 s"] += 1
                R.p(f"  - sp {sp.name} {sysn} {q}: > 10 s at the answer check ({str(c.get('error'))[:80]}); "
                    f"registry call {ref.get('wall_ms')} ms")
            elif nx(c) and not ref.get("ok"):
                t["not expressible"] += 1
            else:
                t["NOT OK"] += 1
                R.F(f"sp {sp.name} {sysn} {q}: parameter-file cell not ok: {str(c.get('error'))[:200]}; "
                    f"registry call ok={ref.get('ok')}")
    R.p(f"- sp vs v3gate {sp.name}: " + ", ".join(f"{v} {c}" for c, v in sorted(t.items())))


def kit_verdicts(R: Rep, d: Path, label: str):
    vj = d / "verdicts.json"
    if not vj.exists():
        R.F(f"{label}: no verdicts.json (kit gate not run) in {d}")
        return
    v = json.loads(vj.read_text())
    tally = {s: Counter(r["verdict"] for r in rows) for s, rows in v.items()}
    for s, rows in v.items():
        for r in rows:
            if r["verdict"] == "DIFFERS":
                R.F(f"{label} kit gate: {s} {r['query']}#{r.get('instance')} DIFFERS from Gnomon")
    ex = json.loads((d / "excluded.json").read_text()) if (d / "excluded.json").exists() else {}
    R.p(f"- {label} kit gate: " + "; ".join(f"{s}: " + ", ".join(f"{k} {n}" for k, n in sorted(c.items()))
                                            for s, c in sorted(tally.items()))
        + f"; excluded from timing: " + (", ".join(f"{s} {len(e)}" for s, e in sorted(ex.items())) or "none"))


def native_rule(R: Rep, d: Path, label: str):
    """Stop rule: Gnomon errors, or refuses a call another system answers inside 10 s."""
    by = {}
    for name, p in files(d).items():
        by.setdefault(FILE.search(name)["sys"], {}).update(cells(p))
    nat = by.get("native", {})
    if not nat:
        return
    bad = 0
    for k, c in nat.items():
        if c.get("ok"):
            continue
        key = f"{k[0]}#{k[1]}" if k[1] else k[0]
        others = sorted(s for s, cs in by.items() if s != "native" and cs.get(k, {}).get("ok")
                        and cs[k].get("wall_ms", 1e18) <= CEIL_MS)
        err = str(c.get("error", ""))
        if c.get("refused") or "E_COST" in err or "guardrail" in err.lower():
            if others:
                R.F(f"{label}: Gnomon REFUSED {key} ({err[:160]}); answered inside 10 s by {others}")
            else:
                R.p(f"  - {label}: Gnomon refused {key} (refused (estimated > 10 s)); no other system inside 10 s")
        elif is_timeout(c):
            R.p(f"  - {label}: Gnomon > 10 s on {key}; inside 10 s: {others or 'none'}")
        else:
            R.F(f"{label}: Gnomon ERROR {key}: {err[:200]}")
        bad += 1
    R.p(f"- {label}: Gnomon cells {len(nat)}, not ok {bad}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--res", type=Path, required=True)
    ap.add_argument("--tags", required=True)
    ap.add_argument("--out", type=Path, required=True)
    # the Gnomon-only mode: the recorded gate jobs' result dirs; only Gnomon and Gnomon on
    # DuckDB storage ran, so the 32-thread roots are not checked and check G
    # compares their answers with the recorded gate. Without it: the stage's original gate, unchanged.
    ap.add_argument("--ref-gate", type=Path, nargs="+")
    a = ap.parse_args()
    G = None
    if a.ref_gate:
        sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "confirm" / "tools"))
        import c3_refgate as G   # noqa: E402
    R = Rep()
    R.p(f"# W1 answer gate: {a.res}" + (f" (Gnomon-only mode; recorded gate {', '.join(map(str, a.ref_gate))})" if G else ""))
    for tag in a.tags.split(","):
        bel = "-d" in tag
        R.h(f"{tag}")
        R.p("### A. the v3 answer check, re-run, against v3")
        compare_dirs(R, f"A {tag}", a.res / "v3gate" / tag, V3["bel" if bel else "reg"] / tag, strict_new_not_ok=True)
        if not G:
            compare_dirs(R, f"A {tag} 32 threads", a.res / "v3gate-t32" / tag, V3["t32"] / tag, strict_new_not_ok=True,
                         systems={"duckdbsql"})
        R.p("### B. single-parameter instances (parameter-file path) against the registry call")
        sp_vs_v3gate(R, a.res / "sp" / tag, a.res / "v3gate" / tag)
        R.p("### D/E. the kit's per-instance gate and the Gnomon stop rule")
        kit_verdicts(R, a.res / "sp" / tag, f"sp {tag}")
        native_rule(R, a.res / "sp" / tag, f"sp {tag}")
        native_rule(R, a.res / "v3gate" / tag, f"v3gate {tag}")
        if not G:
            kit_verdicts(R, a.res / "t32sp" / tag, f"t32sp {tag}")
        if G:
            R.p("### G/E2. Gnomon and Gnomon on DuckDB storage against the recorded gate (Gnomon-only mode)")
            G.check_root(R, f"v3gate {tag}", a.res / "v3gate" / tag, a.ref_gate, "v3gate", tag)
            G.check_root(R, f"sp {tag}", a.res / "sp" / tag, a.ref_gate, "sp", tag)
        if tag in P1OLD:
            R.p("### C. curated P1 instances against the P1 study (engine 0cf027dd)")
            compare_dirs(R, f"C {tag}", a.res / "p1" / tag, P1OLD[tag] / tag, strict_new_not_ok=False)
            if not G:
                compare_dirs(R, f"C {tag} 32 threads", a.res / "t32p1" / tag, P1OLD_T32 / tag, strict_new_not_ok=False,
                             systems={"duckdbsql"})
            kit_verdicts(R, a.res / "p1" / tag, f"p1 {tag}")
            native_rule(R, a.res / "p1" / tag, f"p1 {tag}")
            if not G:
                kit_verdicts(R, a.res / "t32p1" / tag, f"t32p1 {tag}")
            else:
                G.check_root(R, f"p1 {tag}", a.res / "p1" / tag, a.ref_gate, "p1", tag)
    R.lines.insert(1, "")
    R.lines.insert(2, f"**Verdict: {'FAIL' if R.fail else 'PASS'}** ({len(R.fail)} failing checks)")
    a.out.write_text("\n".join(R.lines) + "\n")
    print(f"GATE {'FAIL' if R.fail else 'PASS'}: {len(R.fail)} failing checks; report {a.out}")
    for f in R.fail[:40]:
        print("  FAIL", f)
    return 1 if R.fail else 0


if __name__ == "__main__":
    sys.exit(main())
