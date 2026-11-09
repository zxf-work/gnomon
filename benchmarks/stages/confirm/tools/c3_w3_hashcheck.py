#!/usr/bin/env python3
"""C3 confirmation, W3: the timed run's answer hashes are the answer check.

    c3_w3_hashcheck.py --point SF1|SF10 --ref REF-gnomon.jsonl --old-timed DIR [DIR ...] --new BLOCK.json [...]

Every Gnomon cell of the confirmation's timed blocks (--new: <block>-gnomon.json records of tools/w3_timed.py run) that
answered is compared with the recorded reference:
     equivalent to it at that gate): equal when the new hash is the reference's hash or sorted hash;
  2. otherwise, every other system's ok cell of the recorded timed run (--old-timed: <job>/rot1/*-<system>.json, Gnomon's
     own excluded): equal when the new hash is one of theirs (listed, with the system).
A cell equal to neither while a reference exists is DIFFERS: a FAIL (exit 1). A cell no recorded system answered has no
reference: listed ("unverified"), not a failure. A cell that did not answer (censored, refused, error) is left to the stop
rule (tools/w3_stopcheck.py), which runs before this check; censored cells are counted here.
--stop: the per-block call in the timed job (exit 1 on the first DIFFERS, so the job stops as a failed gate would have).
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path


def load_ref(p: Path) -> dict:
    ref = {}
    for line in open(p):
        if line.strip():
            r = json.loads(line)
            ref[(r["qid"], r["setting"], int(r["i"]))] = r
    return ref


def load_old(dirs: list[Path]) -> dict:
    alt: dict = {}
    for d in dirs:
        for rec in sorted(d.glob("rot1/*.json")):
            if rec.stem.endswith("-gnomon") or rec.name.endswith("-calls.jsonl"):
                continue
            doc = json.loads(rec.read_text())
            sysn = doc.get("system") or rec.stem.split("-", 1)[1]
            for c in doc.get("results", []):
                if c.get("ok") and c.get("hash"):
                    alt.setdefault((c["query"], c["setting"], int(c["instance"])), {})[c["hash"]] = rec.stem.split("-", 1)[1]
    return alt


def check(point: str, ref: dict, alt: dict, new: list[Path]):
    t, lines, fails = Counter(), [], []
    for rec in new:
        for c in json.loads(rec.read_text()).get("results", []):
            k = (c["query"], c["setting"], int(c["instance"]))
            key = f"{k[0]} {k[1]} i={k[2]}"
            if not c.get("ok"):
                t["censored" if c.get("capped") or "> " in str(c.get("error", "")) else "not ok (stop rule)"] += 1
                continue
            h = c.get("hash")
            r = ref.get(k)
            if r and r.get("ok") and h in (r.get("hash"), r.get("sorted_hash")):
                t["equal to the recorded Gnomon reference"] += 1
                continue
            others = alt.get(k, {})
            if h in others:
                t["equal to a recorded system"] += 1
                lines.append(f"- {key}: equal to the recorded {others[h]} answer"
                             + (" (recorded Gnomon reference differs)" if r and r.get("ok") else
                                " (no recorded Gnomon answer)"))
                continue
            if (r and r.get("ok")) or others:
                t["DIFFERS"] += 1
                fails.append(f"{point} {key}: DIFFERS: new {h} rows {c.get('rows')}; recorded Gnomon "
                             f"{(r or {}).get('hash')} ({'ok' if r and r.get('ok') else 'not ok'}), others {sorted(set(others.values()))}")
            else:
                t["unverified (no recorded answer)"] += 1
                lines.append(f"- {key}: answered ({h}, {c.get('rows')} rows); no recorded system answered it")
    return t, lines, fails


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--point", required=True)
    ap.add_argument("--ref", type=Path, required=True)
    ap.add_argument("--old-timed", type=Path, nargs="+", required=True)
    ap.add_argument("--new", type=Path, nargs="+", required=True)
    ap.add_argument("--out", type=Path)
    ap.add_argument("--stop", action="store_true")
    a = ap.parse_args(argv)
    t, lines, fails = check(a.point, load_ref(a.ref), load_old(a.old_timed), [p for p in a.new if p.exists()])
    verdict = "FAIL" if fails else "PASS"
    head = f"HASHCHECK {a.point} {verdict}: " + ", ".join(f"{v} {k}" for k, v in sorted(t.items()))
    if a.out:
        a.out.write_text("\n".join([f"# W3 C3 answer check from the timed run's hashes, {a.point}", "", f"**{head}**", "",
                                    "## Failures", ""] + [f"- **FAIL** {f}" for f in fails] +
                                   ["", "## Listed", ""] + lines) + "\n")
    print(head)
    for f in fails[:40]:
        print("  FAIL", f)
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
