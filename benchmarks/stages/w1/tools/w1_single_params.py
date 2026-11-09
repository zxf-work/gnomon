#!/usr/bin/env python3
"""W1 timed stage: the single-parameter instances as P1-format parameter files.

    w1_single_params.py --logs <eventlog dir> --out <dir> TAG...

For each tag (K1M, K10M: the registry ids of families P T S W D N R M plus the native-only ids;
K10M-d0/d1/d10: the eight belief-time ids of family B) the file holds ONE instance per id whose
arguments are exactly the registry's own (extcmp.queries on the tag's frozen log, i.e. the
arguments v3 timed). So the kit's parameter-file path (extcmp.py query --params: the window prime,
2 warm-ups + 10 timed, 1 + 3 above 1 s, the 10 s ceiling, the retry-once rule) times the v3
single-parameter instances under the current protocol. No kit code changes; the files are inputs,
written once before the gate jobs (login node, from the logs' side files), checked by sha256 (SHA256SUMS) in every job.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

import extcmp as X          # the pinned kit (PYTHONPATH = $REPO:$REPO/scripts)
import extcmp_p1 as P1

VERSION = "w1-single-v1"


def sha256(p: Path) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--logs", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--sha-list", type=Path, help="the frozen logs' SHA256SUMS (v3/eventlogs): take each log's sha256 from it "
                    "instead of hashing the log here")
    ap.add_argument("tags", nargs="+")
    a = ap.parse_args()
    a.out.mkdir(parents=True, exist_ok=True)
    size = {"K1M": 1_000_000, "K10M": 10_000_000}
    for tag in a.tags:
        if "-d" in tag:
            log = a.logs / f"{tag}.eventlog.jsonl"
            ids = list(X.FAMILIES["B"])
        else:
            log = a.logs / f"synth-{size[tag]}.eventlog.jsonl"
            ids = [q for f in "PTSWDNRM" for q in X.FAMILIES[f]]
            ids += [q for f in "PTSWDNRM" for q in X.NATIVE_ONLY_BY_FAMILY.get(f, ())]
        data = X.dataset(log)
        qs = X.queries(data, ids)
        insts = []
        for q in qs:
            args = json.loads(json.dumps(q.args, sort_keys=True, default=list))
            iid = P1.md5(VERSION, q.id, json.dumps(args, sort_keys=True))[:8]
            insts.append({"query": q.id, "instance": iid, "stratum": "registry instance",
                          "class": "single parameter (v3)", "factors": {}, "args": args})
        if a.sha_list:
            known = dict(reversed(l.split()) for l in a.sha_list.read_text().splitlines() if l.strip())
            lsha = known[log.name]
        else:
            lsha = sha256(log)
        doc = {"format": P1.FORMAT, "version": VERSION,
               "log": {"name": log.name, "sha256": lsha},
               "selection": {"notes": ["one instance per registry id: the registry's own arguments "
                                       "(extcmp.queries on this log), the instances v3 timed"]},
               "counts": {q.id: 1 for q in qs}, "instances": insts}
        out = a.out / f"params-{tag}.json"
        out.write_text(json.dumps(doc, indent=1) + "\n")
        print(f"{out}: {len(insts)} instances: {', '.join(i['query'] for i in insts)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
