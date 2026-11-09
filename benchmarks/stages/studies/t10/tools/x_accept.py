#!/usr/bin/env python3
"""Smoke only: the whole parameter files of the t10 stage through the kit's own checks on the C4 Gnomon stores: P1.load, P1.expand (argument names equal the
registry's) and extcmp.check_inputs (every node an instance names is in the store). Nothing is timed or compared here.

    x_accept.py --work DIR --stage NEXT_STAGE_ROOT
"""
import argparse
import json
import shutil
import subprocess
import sys
from pathlib import Path

import extcmp as X
import extcmp_p1 as P1

E = Path(__import__("os").environ.get("GNOMON_BENCH_ROOT", ".") + "")
SETS = {"w1": (E / "v3/eventlogs", E / "confirm/w1/stores"), "w2": (E / "w2-powerlaw/check1m/campaign/eventlogs", E / "confirm/w2/stores")}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--work", type=Path, required=True)
    ap.add_argument("--stage", type=Path, required=True)
    a = ap.parse_args()
    a.work.mkdir(parents=True, exist_ok=True)
    bad = 0
    for s, (logs, stores) in SETS.items():
        for tag in ("K10M-d0", "K10M-d10"):
            pf = a.stage / f"params-{s}" / f"params-{tag}.json"
            log = a.work / f"{s}-{tag}.eventlog.jsonl"
            shutil.copy2(logs / f"{tag}.eventlog.jsonl", log)
            d = a.work / f"{s}-{tag}"
            d.mkdir(exist_ok=True)
            subprocess.run(f"zstd -q -d -c {stores}/{tag}/native-{tag}.tar.zst | tar -C {d} -xf -", shell=True, check=True)
            doc = P1.load(pf)
            ids = sorted({i["query"] for i in doc["instances"]})
            data = X.dataset(log)
            qs, ins = P1.expand(X.queries(data, ids), doc)
            m = {id(q): i for q, i in zip(qs, ins)}
            rs = X.open_existing_store(d / f"native-{tag}", "native")
            try:
                X.check_inputs(rs, qs, lambda q: f"{q.id}#{m[id(q)]['instance']}")
                print(f"accepted {s} {tag}: {len(qs)} of {len(doc['instances'])} instances, dataset t0 {data.t0} t1 {data.t1} tt_epoch1 {data.tt_epoch1}")
            except SystemExit as e:
                print(f"NOT ACCEPTED {s} {tag}: {e}")
                bad += 1
            rs.close()
            shutil.rmtree(d)
            log.unlink()
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
