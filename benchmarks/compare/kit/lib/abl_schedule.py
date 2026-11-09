#!/usr/bin/env python3
"""Ablation schedule for one density point and one rotation.

    abl_schedule.py <rotation> <families, e.g. P,T,S,W,D,N,R,M,B> [arms, comma list]

One line per window: `name family variant env`, where `variant` names the
native store (stores/native-<tag>-<variant>; `duckdb`: the kit system duckdb's
store stores/duckdb-<tag>) and `env` is the switch setting (`-` for none;
several settings comma-separated). Every arm X is paired with a window of the all-unset engine on
the unset store (or on the store its third field names), named `unset~X`, and the pair runs back to back inside the
family block; pair orientation alternates with the block, the arm index and
the rotation (ABBA), so neither side is always first. The arm `aa` is the
A/A control: the unset engine against itself, the noise floor for every
other ratio. Families rotate between rotations; so do the arms inside a block.
"""

import sys

import json
from pathlib import Path

_CFG = json.loads((Path(__file__).resolve().parent / "ablation_configs.json").read_text())
# configuration -> (store variant, environment[, store of the paired unset window])
ARMS = {k: tuple(v) for k, v in _CFG["configurations"].items()}
# the default list (every configuration but those added for C4)
DEFAULT_ARMS = list(_CFG["default"])
# the configurations added for the evaluated engine
NEW_ARMS6 = {k: tuple(v) for k, v in _CFG["c4"].items()}
ARMS.update(NEW_ARMS6)
# store variants and how each is built (run_ablation.sh, step `stores`)
VARIANTS = dict(_CFG["variants"])

def main() -> int:
    if sys.argv[1] == "--gated":
        return 0 if sys.argv[2] in NEW_ARMS6 else 1
    if sys.argv[1] == "--default":    # the default arm list
        print(",".join(DEFAULT_ARMS))
        return 0
    if sys.argv[1] == "--new":
        print(" ".join(NEW_ARMS6))
        return 0
    rot = int(sys.argv[1])
    fams = sys.argv[2].split(",")
    arms = sys.argv[3].split(",") if len(sys.argv) > 3 and sys.argv[3] else list(DEFAULT_ARMS)
    for a in arms:
        if a not in ARMS:
            raise SystemExit(f"unknown arm {a}; known: {', '.join(ARMS)}")
    k = (rot - 1) % len(fams)
    out = []
    for b, fam in enumerate(fams[k:] + fams[:k]):
        j = (b + rot - 1) % len(arms)
        for i, arm in enumerate(arms[j:] + arms[:j]):
            variant, env = ARMS[arm][:2]
            basev = ARMS[arm][2] if len(ARMS[arm]) > 2 else "u"
            base = f"unset~{arm} {fam} {basev} -"
            this = f"{arm} {fam} {variant} {env}"
            out += [base, this] if (b + i + rot) % 2 == 0 else [this, base]
    print("\n".join(out))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
