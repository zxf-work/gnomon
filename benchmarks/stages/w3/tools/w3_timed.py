#!/usr/bin/env python3
"""W3 timed-runner glue.

    w3_timed.py run <timed_runner.py w3 arguments> [--exclude EXC.json --excl-system S]
    w3_timed.py subset --params P.jsonl --n N --out Q.jsonl
    w3_timed.py answered BLOCK.json [BLOCK.json ...]      (prints the engine-pass cell list "Q@s1+s2,...")
    w3_timed.py audit CALLS.jsonl [...]                   (censor-once audit; exit 1 on a violation)
    w3_timed.py audit-dir DIR                             (every *-calls.jsonl under DIR)

`run` is the kit's shared timed runner (scripts/timed_runner.py, unchanged) with one addition: the gate's per-instance
exclusions (EXC.json["excluded"][system] = ["query|instance|setting", ...]) are removed from the cell list. The kit's
code is not touched (the job's guard refuses a changed kit).

CENSOR-ONCE: an instance whose first timed call reaches the 10 s ceiling is recorded "> 10 s"
and its remaining timed repetitions are skipped, for every system alike. The kit's W3 runner already does exactly this:
timed_runner._instance returns at the first call (warm-up or timed) whose status is not ok, so no later call of that
instance runs; a warm-up at the ceiling is the instance's first call and is recorded the same way. This glue does not
change that behaviour; it audits it: after every block it checks, from the per-call log, that no call of an instance
follows a censored or failed call, and writes the result into the block record ("censor_once").
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

STAGE = Path(__file__).resolve().parents[1]


def apply_override():
    """Stage text overrides for the kit's Cypher texts (W3_CYPHER_OVERRIDE, default <stage>/cypher-override): a merged copy of the
    kit's cypher/ directory with the override files laid over it; w3_snb_systems.W3CY is pointed at the copy, so kit code and
    kit texts stay untouched. Used by the answer-check re-run (tools/w3_run.py) and by the timed runner (cmd_run)."""
    ov = Path(os.environ.get("W3_CYPHER_OVERRIDE", STAGE / "cypher-override"))
    if not ov.is_dir():
        return None
    import shutil
    import tempfile
    repo = Path(os.environ["EXTCMP_REPO"])
    for p in (repo, repo / "scripts"):
        if str(p) not in sys.path:
            sys.path.insert(0, str(p))
    import w3_snb_systems as Y   # noqa: E402
    old = Path(Y.W3CY)
    new = Path(tempfile.mkdtemp(prefix="w3cy.")) / "cypher"
    shutil.copytree(old, new)
    shutil.copytree(ov, new, dirs_exist_ok=True)
    for m in list(sys.modules.values()):
        if getattr(m, "W3CY", None) == old:
            m.W3CY = new
    print(f"[w3_timed] Cypher text overrides from {ov}: {sorted(str(f.relative_to(ov)) for f in ov.rglob('*.cypher'))}", flush=True)
    return new


RULE = ("censor-once: an instance whose first timed call reaches the ceiling is recorded '> ceiling'; "
        "its remaining warm-up and timed calls are skipped, uniformly for every system")


def audit_calls(path):
    """(instances, censored instances, instances stopped by an error, violations[]) from a per-call log."""
    seqs: dict = {}
    for line in open(path):
        if not line.strip():
            continue
        r = json.loads(line)
        seqs.setdefault((r["query"], r["setting"], r["instance"]), []).append(r)
    cens = err = 0
    viol = []
    for k, cs in seqs.items():
        body = [c for c in cs if c["phase"] != "prime"]      # a priming call is outside the ceiling
        bad = [i for i, c in enumerate(body) if c["status"] != "ok"]
        if not bad:
            continue
        if bad[0] != len(body) - 1:
            viol.append(list(k))
        if body[bad[0]]["status"] == "censored":
            cens += 1
        else:
            err += 1
    return {"instances": len(seqs), "censored_instances": cens, "error_instances": err, "violations": viol}


def cmd_run(argv: list[str]) -> int:
    def pop(flag):
        if flag in argv:
            i = argv.index(flag)
            v = argv[i + 1]
            del argv[i:i + 2]
            return v
        return None
    exclude, esys = pop("--exclude"), pop("--excl-system")
    out, jsonl = (argv[argv.index("--out") + 1] if "--out" in argv else None,
                  argv[argv.index("--jsonl") + 1] if "--jsonl" in argv else None)
    repo = Path(os.environ["EXTCMP_REPO"])
    sys.path.insert(0, str(repo))
    sys.path.insert(0, str(repo / "scripts"))
    import timed_runner as TR   # noqa: E402
    import tr_catalogs as TC    # noqa: E402
    apply_override()
    ex: set = set()
    if exclude and Path(exclude).exists():
        ex = set(json.load(open(exclude)).get("excluded", {}).get(esys, []))
    cells0, open0 = TC.CATALOGS["w3"]

    def cells(a):
        allc = cells0(a)
        keep = [c for c in allc if f"{c[0]}|{c[2]}|{c[1]}" not in ex]
        print(f"[w3_timed] {len(keep)} of {len(allc)} instance cells for {esys} ({len(allc) - len(keep)} excluded by the gate)",
              flush=True)
        return keep
    TC.CATALOGS["w3"] = (cells, open0)
    sys.argv = ["timed_runner.py", "w3"] + argv
    rc = TR.main()
    if out and Path(out).exists():
        b = json.loads(Path(out).read_text())
        b["censor_once"] = {"rule": RULE, "enforced_by": "kit scripts/timed_runner.py _instance (unchanged)",
                            "gate_exclusions": len(ex), "system": esys}
        if jsonl and Path(jsonl).exists():
            b["censor_once"]["audit"] = audit_calls(jsonl)
        Path(out).write_text(json.dumps(b, indent=1, default=str) + "\n")
    return rc


def cmd_subset(argv: list[str]) -> int:
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--params", required=True)
    ap.add_argument("--n", type=int, required=True)
    ap.add_argument("--out", required=True)
    a = ap.parse_args(argv)
    k = 0
    with open(a.out, "w") as o:
        for line in open(a.params):
            if line.strip() and json.loads(line)["i"] < a.n:
                o.write(line if line.endswith("\n") else line + "\n")
                k += 1
    print(f"{k} instances (i < {a.n}) -> {a.out}")
    return 0


def cmd_answered(argv: list[str]) -> int:
    by: dict = {}
    for p in argv:
        for c in json.load(open(p))["results"]:
            by.setdefault((c["query"], c["setting"]), []).append(bool(c["ok"]))
    cells: dict = {}
    for (q, s), v in by.items():
        if v and all(v):
            cells.setdefault(q, []).append(s)
    print(",".join(f"{q}@{'+'.join(ss)}" for q, ss in cells.items()))
    return 0


def cmd_audit(argv: list[str]) -> int:
    bad = 0
    for p in argv:
        a = audit_calls(p)
        print(f"{p}: {a['instances']} instances, {a['censored_instances']} censored (stopped at the first call at the ceiling), "
              f"{a['error_instances']} stopped by an error, {len(a['violations'])} violations")
        bad += len(a["violations"])
    return 1 if bad else 0


def main() -> int:
    if len(sys.argv) < 2 or sys.argv[1] not in ("run", "subset", "answered", "audit", "audit-dir"):
        print(__doc__)
        return 2
    c, rest = sys.argv[1], sys.argv[2:]
    if c == "audit-dir":
        return cmd_audit([str(p) for p in sorted(Path(rest[0]).rglob("*-calls.jsonl"))])
    return {"run": cmd_run, "subset": cmd_subset, "answered": cmd_answered, "audit": cmd_audit}[c](rest)


if __name__ == "__main__":
    sys.exit(main())
