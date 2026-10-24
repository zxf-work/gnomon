#!/usr/bin/env python3
"""Materialize one synthetic event log to a stable path, once.

`eval_harness.py::build_dataset` mints transaction times from the wall clock
at write time, so two runs of `--scale N` produce logs that differ in every
`tt` and therefore in every version id derived from one. Four of the
registry's thirteen queries embed a `tt` in their answer (`hist.single`,
`hist.asof`, `snap.hop2`, `coactive.narrow`), so their result hashes differ
between two freshly generated datasets by construction.

A comparison whose conclusion is "the answers did not move" therefore needs
both sides to replay one frozen log. This script runs `build_dataset` exactly
once and copies the log it produced to a stable path; every side of the
comparison then runs `eval_harness.py --log <that file>`.

The dataset parameters the registry derives (`t0`, `t1`, `tt_epoch1`,
`filter_uids`) are *not* copied: `dataset_from_log` recomputes all four from
the log itself. The sidecar JSON written next to the log records what they
came out as, so a later reader can check that assumption.

    python scripts/freeze_synth_log.py --scale 1000000 \\
        --out runs/frozen/synth-1m.eventlog.jsonl
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import eval_harness as H  # noqa: E402


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scale", type=int, required=True)
    ap.add_argument("--out", type=Path, required=True,
                    help="destination path for the frozen .eventlog.jsonl")
    ap.add_argument("--force", action="store_true",
                    help="overwrite an existing frozen log (the whole point "
                         "of one is that it does not move, so this is off by "
                         "default)")
    args = ap.parse_args()

    if args.out.exists() and not args.force:
        meta = args.out.with_suffix(".meta.json")
        print(f"{args.out} exists; not regenerating (--force to override)")
        if meta.exists():
            print(meta.read_text())
        return 0

    args.out.parent.mkdir(parents=True, exist_ok=True)
    t0 = time.perf_counter()
    data = H.build_dataset(args.scale)
    gen_s = time.perf_counter() - t0
    shutil.copyfile(data.log, args.out)

    # Round-trip through the reader both configurations will use, and record what it
    # derived. If these four ever disagree with the values `build_dataset`
    # held, the freeze is not equivalent to the generated run and the
    # discrepancy belongs in the receipt, not in a silent pass.
    reread = H.dataset_from_log(args.out)
    meta = {
        "scale_requested": args.scale,
        "scale_in_log": reread.scale,
        "generated_s": round(gen_s, 1),
        "sha256": sha256(args.out),
        "bytes": args.out.stat().st_size,
        "derived": {"t0": reread.t0, "t1": reread.t1,
                    "tt_epoch1": reread.tt_epoch1,
                    "filter_uids": list(reread.filter_uids)},
        "generated_dataset": {"t0": data.t0, "t1": data.t1,
                              "tt_epoch1": data.tt_epoch1,
                              "filter_uids": list(data.filter_uids),
                              "scale": data.scale},
    }
    same = (reread.t0 == data.t0 and reread.t1 == data.t1
            and reread.tt_epoch1 == data.tt_epoch1
            and tuple(reread.filter_uids) == tuple(data.filter_uids))
    meta["reread_matches_generated"] = same
    args.out.with_suffix(".meta.json").write_text(json.dumps(meta, indent=1))
    print(json.dumps(meta, indent=1))
    if not same:
        print("WARNING: replayed parameters differ from the generated "
              "dataset's; see the meta file", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
