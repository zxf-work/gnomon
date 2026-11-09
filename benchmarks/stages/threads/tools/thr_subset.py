#!/usr/bin/env python3
"""Thread-matched control: the three parameter-file families, derived from the files W1 and W2 already timed.

    thr_subset.py --w1 <w1 params-single dir> --w2 <w2 stage params dir> --out <params dir>

Writes (nothing is generated afresh: every instance, with its instance id and its arguments, is one the W1 or W2
timed stage ran):
  w1/params-K10M.json     the 16 comparable registry ids of W1, one single-parameter instance each (the three
                          Gnomon-only ids diff.belief, resolve.exact, resolve.prefix are dropped);
  w1/params-K10M-d1.json  the eight belief-time ids of W1's B point, one instance each (copied unchanged);
  w2/params-K10M.json     a fixed subset of W2's curated set at K10M: per query id, ALL hub-stratum instances plus
                          up to NONHUB (5) non-hub instances.
W2 subset rule (deterministic, no random generator): the non-hub instances of an id are grouped by stratum; strata are
taken in alphabetical order, and within a stratum instances in ascending md5("thr-subset-v1", query, instance); the picks
go round-robin over the strata (first of each stratum, then second of each, ...) until NONHUB are chosen or none is left.
The subset keeps the W2 file's order (its md5 shuffle) and its log facts; "subset_of" records the source file's sha256.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

VERSION = "thr-subset-v1"
NATIVE_ONLY = ("diff.belief", "resolve.exact", "resolve.prefix")
NONHUB = 5


def md5(*parts) -> str:
    return hashlib.md5("\x1f".join(str(p) for p in parts).encode()).hexdigest()


def sha256(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


def dump(out: Path, doc: dict) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(doc, indent=1, sort_keys=False) + "\n")
    print(f"{out}: {len(doc['instances'])} instances {json.dumps(doc['counts'])}")


def counts(instances: list[dict]) -> dict:
    c: dict[str, int] = {}
    for i in instances:
        c[i["query"]] = c.get(i["query"], 0) + 1
    return c


def derive(src: Path, keep, note: str) -> dict:
    doc = json.loads(src.read_text())
    inst = [i for i in doc["instances"] if keep(i)]
    sel = dict(doc.get("selection", {}))
    sel["notes"] = list(sel.get("notes", [])) + [note]
    return {"format": doc["format"], "version": VERSION, "log": doc["log"], "selection": sel, "counts": counts(inst),
            "subset_of": {"file": src.name, "sha256": sha256(src), "version": doc.get("version")}, "instances": inst}


def w2_pick(inst: list[dict]) -> set[tuple[str, str]]:
    keep: set[tuple[str, str]] = set()
    by_q: dict[str, list[dict]] = {}
    for i in inst:
        by_q.setdefault(i["query"], []).append(i)
    for q, xs in by_q.items():
        keep.update((q, i["instance"]) for i in xs if i.get("stratum") == "hub")
        strata: dict[str, list[dict]] = {}
        for i in xs:
            if i.get("stratum") != "hub":
                strata.setdefault(str(i.get("stratum")), []).append(i)
        queues = [sorted(strata[s], key=lambda i: md5(VERSION, i["query"], i["instance"])) for s in sorted(strata)]
        picked = 0
        depth = 0
        while picked < NONHUB and any(len(qq) > depth for qq in queues):
            for qq in queues:
                if len(qq) > depth and picked < NONHUB:
                    keep.add((q, qq[depth]["instance"]))
                    picked += 1
            depth += 1
    return keep


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--w1", type=Path, required=True)
    ap.add_argument("--w2", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    a = ap.parse_args()
    dump(a.out / "w1/params-K10M.json",
         derive(a.w1 / "params-K10M.json", lambda i: i["query"] not in NATIVE_ONLY,
                f"thr: the 16 comparable ids of W1 (dropped Gnomon-only {', '.join(NATIVE_ONLY)})"))
    dump(a.out / "w1/params-K10M-d1.json",
         derive(a.w1 / "params-K10M-d1.json", lambda i: True, "thr: W1's B point, all eight ids, unchanged"))
    src = a.w2 / "params-K10M.json"
    doc = json.loads(src.read_text())
    keep = w2_pick(doc["instances"])
    dump(a.out / "w2/params-K10M.json",
         derive(src, lambda i: (i["query"], i["instance"]) in keep,
                f"thr: all hub-stratum instances plus up to {NONHUB} non-hub per id (round-robin over strata in "
                f"alphabetical order, ascending md5('{VERSION}', query, instance) within a stratum)"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
