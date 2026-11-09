"""C3 confirmation: the Gnomon-only answer gate's comparison with the recorded gate.

Used by benchmarks/stages/w1/tools/w1_gate.py and benchmarks/stages/w2/tools/w2_gate.py when they are given
--ref-gate (the Gnomon-only mode; without it they behave as before). In the Gnomon-only gate only Gnomon (native) and
Gnomon on DuckDB storage (duckdb) run; the competitors' records stand. The check that took "every system against Gnomon in

  G. each new Gnomon / Gnomon-on-DuckDB-storage answer against the same system's recorded answer of the same root, point
     and file (verify-<sys>[-<fam>].json): equal hash and rows passes. A different answer passes only if it equals an ok
     recorded answer of another system for the same cell (listed); otherwise FAIL. A cell answered now and not then is
     compared with the recorded systems the same way (FAIL on a difference, listed when no system answered it). A cell
     answered then and not now: FAIL for native unless it is a timeout (listed), as before (strict for Gnomon).
  E2. the stop rule with the recorded competitors: a new Gnomon refusal of a call a recorded system answered inside
     10 s at the recorded gate is a FAIL; a Gnomon error is a FAIL (already in E).
"""
from __future__ import annotations

import json
import re
from collections import Counter
from pathlib import Path

FILE = re.compile(r"verify-(?P<sys>[a-z0-9]+)(?:-(?P<fam>[A-Z]))?\.json$")
GNOMON = ("native", "duckdb")
TIMEOUT = ("statement timeout", "canceling statement due to", "TIMEOUT_EXCEEDED", "Timeout exceeded",
           "client-side limit", "INTERRUPT", "Interrupted", "timeout", "> 10 s", ">10 s", "aborted")
CEIL_MS = 10_000.0


def _cells(p: Path) -> dict:
    return {(r["query"], r.get("instance") or ""): r for r in json.loads(p.read_text()).get("results", [])}


def _timeout(c: dict) -> bool:
    e = str(c.get("error", ""))
    return bool(c.get("aborted")) or any(m in e for m in TIMEOUT)


def _key(k) -> str:
    return f"{k[0]}#{k[1]}" if k[1] else k[0]


def recorded_dir(ref_gates: list[Path], root: str, tag: str) -> Path | None:
    for g in ref_gates:
        d = g / root / tag
        if d.is_dir() and any(d.glob("verify-*.json")):
            return d
    return None


def others_by_cell(old: Path, fam: str | None) -> dict:
    """{cell: {hash: system}} and {cell: [(system, wall_ms)]} of the recorded competitors' ok cells (same family file)."""
    hashes, walls = {}, {}
    for p in sorted(old.glob("verify-*.json")):
        m = FILE.search(p.name)
        if not m or m["sys"] in GNOMON or (m["fam"] or None) != fam:
            continue
        for k, c in _cells(p).items():
            if c.get("ok"):
                hashes.setdefault(k, {})[(c.get("hash"), c.get("rows"))] = m["sys"]
                walls.setdefault(k, []).append((m["sys"], c.get("wall_ms", 1e18)))
    return {"hash": hashes, "wall": walls}


def check_root(R, label: str, new: Path, ref_gates: list[Path], root: str, tag: str):
    """Checks G and E2 for one result root of one point (new = <gate res>/<root>/<tag>)."""
    old = recorded_dir(ref_gates, root, tag)
    if old is None:
        R.F(f"G {label}: no recorded gate records for {root}/{tag} in {', '.join(map(str, ref_gates))}")
        return
    if not new.is_dir():
        R.F(f"G {label}: no new records ({new})")
        return
    tot = Counter()
    for p in sorted(new.glob("verify-*.json")):
        m = FILE.search(p.name)
        if not m or m["sys"] not in GNOMON:
            continue
        sysn, fam = m["sys"], m["fam"] or None
        op = old / p.name
        o_c = _cells(op) if op.exists() else {}
        oth = others_by_cell(old, fam)
        t = Counter()
        for k, n in sorted(_cells(p).items()):
            o = o_c.get(k)
            alts = oth["hash"].get(k, {})
            if n.get("ok"):
                if o and o.get("ok") and (n.get("hash"), n.get("rows")) == (o.get("hash"), o.get("rows")):
                    t["equal to the recorded answer"] += 1
                elif (n.get("hash"), n.get("rows")) in alts:
                    t["equal to a recorded competitor"] += 1
                    R.p(f"  - G {label} {p.name}: {_key(k)} equals the recorded {alts[(n.get('hash'), n.get('rows'))]} answer"
                        f" ({'recorded ' + sysn + ' differs' if o and o.get('ok') else 'not answered by ' + sysn + ' then'})")
                elif (o and o.get("ok")) or alts:
                    t["DIFFERS"] += 1
                    R.F(f"G {label} {p.name}: {_key(k)} DIFFERS: new {n.get('hash')} rows {n.get('rows')}; recorded "
                        f"{(o or {}).get('hash')} rows {(o or {}).get('rows')}; competitors {sorted(set(alts.values()))}")
                else:
                    t["unverified (no recorded answer)"] += 1
                    R.p(f"  - G {label} {p.name}: {_key(k)} answered; no recorded system answered it")
            else:
                if o and o.get("ok"):
                    if _timeout(n):
                        t["censored now (recorded ok)"] += 1
                        R.p(f"  - G {label} {p.name}: {_key(k)} censored now, recorded ok at {o.get('wall_ms')} ms")
                    elif sysn == "native":
                        t["NEW NOT OK"] += 1
                        R.F(f"G {label} {p.name}: {_key(k)} recorded ok, new not ok: {str(n.get('error'))[:200]}")
                    else:
                        t["not ok now (recorded ok)"] += 1
                        R.p(f"  - G {label} {p.name}: {_key(k)} not ok now: {str(n.get('error'))[:120]}")
                else:
                    t["both not ok"] += 1
                if sysn == "native" and not _timeout(n):
                    err = str(n.get("error", ""))
                    if n.get("refused") or "E_COST" in err or "guardrail" in err.lower():
                        fast = sorted(s for s, w in oth["wall"].get(k, []) if w <= CEIL_MS)
                        if fast:
                            R.F(f"E2 {label}: Gnomon REFUSED {_key(k)} ({err[:120]}); recorded inside 10 s by {fast}")
        for k in sorted(set(o_c) - set(_cells(p))):
            t["MISSING (new)"] += 1
            R.F(f"G {label} {p.name}: {_key(k)} recorded, missing in the new run")
        tot.update(t)
        R.p(f"- G {label} {p.name}: " + ", ".join(f"{v} {c}" for c, v in sorted(t.items())))
    R.p(f"- **G {label} total** (against {old}): " + ", ".join(f"{v} {c}" for c, v in sorted(tot.items())))
