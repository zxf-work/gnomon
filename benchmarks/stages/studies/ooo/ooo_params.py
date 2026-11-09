#!/usr/bin/env python3
"""Out-of-order logs: one parameter file per log, the same instances on each.
    ooo_params.py --base <original log> --out <dir> [--frozen-reg F --frozen-bel F] TAG=<log> ...

Instances: the registry's own arguments on the original log for the 24 temporal and belief queries (families P T S W D N M + B + diff.belief;
family R, the three name lookups, reads no time and is left out). Plus a belief cut inside the ingest: T_mid = the tt of the batch that
finishes 3/4 of the ingest batches, on the five queries that take a belief time (hist.asof, diff.belief, b.snap.hop2@T, b.reach@T, b.diff).
Why T_mid: on this log every registry belief cut lies at the end of the log or in its first batch, where a chunk is wholly below or above the cut and no block is
read, so those instances cannot show what time order does to the block checks. At density 0, @T equals its control by construction.
The B family's e* is the original log's; its discriminator is kept by the shuffler. Files are P1 format, written per log with that log's sha256."""
import argparse
import hashlib
import json
import sys
from pathlib import Path

import extcmp as X
import extcmp_p1 as P1


def sha256(p):
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def t_mid(log):
    tts = []
    with open(log) as f:
        f.readline()
        for line in f:
            rec = json.loads(line)
            if any(op.get("op") == "ingest_events" for op in rec["ops"]):
                tts.append(rec["tt"])
    return tts[len(tts) * 3 // 4 - 1], len(tts)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--frozen-reg", type=Path)
    ap.add_argument("--frozen-bel", type=Path)
    ap.add_argument("tags", nargs="+")
    a = ap.parse_args()
    a.out.mkdir(parents=True, exist_ok=True)
    fams = ["P", "T", "S", "W", "D", "N", "M", "B"]
    ids = [q for f in fams for q in X.FAMILIES[f]] + ["diff.belief"]
    data = X.dataset(a.base)
    qs = X.queries(data, ids)
    args = {q.id: q.args for q in qs}
    if len(ids) != 24 or len(qs) != 24:
        raise SystemExit(f"expected 24 queries, have {len(ids)}, {len(qs)}")
    # at 10M the arguments are checked against the frozen single-parameter files the W1 stage timed
    for f in (a.frozen_reg, a.frozen_bel):
        if f:
            for i in json.loads(f.read_text())["instances"]:
                if i["query"] in args and json.dumps(i["args"], sort_keys=True) != json.dumps(args[i["query"]], sort_keys=True):
                    raise SystemExit(f"arguments of {i['query']} differ from the frozen file {f}")
    tm, nb = t_mid(a.base)
    inst = [P1._inst(q, json.loads(json.dumps(args[q])), "registry instance", "registry arguments", {}) for q in ids]
    for q, key in (("hist.asof", "as_of_tt"), ("diff.belief", "t1"), ("b.snap.hop2@T", "as_of_tt"), ("b.reach@T", "as_of_tt"), ("b.diff", "t1")):
        inst.append(P1._inst(q, P1._with(args[q], **{key: tm}), "belief cut inside the ingest", "T_mid", {"T_mid": tm}))
    sums = []
    for spec in a.tags:
        tag, log = spec.split("=", 1)
        facts = {"name": Path(log).name, "sha256": sha256(log)}
        doc = {"format": P1.FORMAT, "version": "ooo-v1", "log": facts, "T_mid": tm, "ingest_batches": nb,
               "selection": {"notes": ["24 queries with the registry's arguments on the original log, plus T_mid"]},
               "counts": {}, "instances": inst}
        for name in (f"params-{tag}.json", f"params-{tag.split('-d')[0]}.json"):
            (a.out / name).write_text(json.dumps(doc, indent=1) + "\n")
            sums.append(name)
    with open(a.out / "SHA256SUMS", "w") as f:
        for name in sums:
            f.write(f"{sha256(a.out / name)}  {name}\n")
    print(f"T_mid {tm} ({nb} ingest batches), {len(inst)} instances, files {sums}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
