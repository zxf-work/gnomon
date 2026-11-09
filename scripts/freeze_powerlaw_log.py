#!/usr/bin/env python3
"""W2: freeze one power-law synthetic event log, once.

The uniform generator (`eval_harness.py::_event`) is kept in every respect but
the endpoint choice: |V| = scale/100, edge lifetime = scale/20, the burst band,
the relation-type mix, communities of 50 contiguous ids with 70 % of the edges
inside one, the corrections and retraction of `build_dataset`. Endpoints are
drawn from a Chung-Lu style rank weight w(r) = (r+1)^(-1/(gamma-1)), which gives
a degree distribution with tail exponent gamma (default 2.1):

* src: a power-law draw over all nodes;
* dst, intra-community (70 %): a power-law draw over the 50 members of src's
  community, members ordered by their global rank, so hubs are hubs inside
  their community too;
* dst, inter-community (30 %): a power-law draw over all nodes.

Ranks map to node ids through a seed-derived multiplicative permutation
(id = rank * A mod |V|, A coprime to |V|), so hubs are spread over the
communities instead of all sitting in community 0. Every draw is a pure
function of (event index, seed) - splitmix64 on the index xor a seed salt -
so `_edge_ref(i)` still recomputes event i, which `build_dataset` needs to
name the edges it corrects. The generator is installed by replacing
`eval_harness._event` before `build_dataset` runs; eval_harness itself is not
edited (the uniform W1 logs stay byte-for-byte reproducible).

As with W1, transaction times come from the wall clock at write time, so the
log is frozen once and checksummed; it is not regenerable byte for byte (the
graph structure and valid times are).

    python scripts/freeze_powerlaw_log.py --scale 1000000 --gamma 2.1 --seed 2101 \
        --out eventlogs/powerlaw-1000000.eventlog.jsonl
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import shutil
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import eval_harness as H  # noqa: E402

_U53 = float(1 << 53)


class PowerLaw:
    def __init__(self, scale: int, gamma: float, seed: int):
        self.scale, self.gamma, self.seed = scale, gamma, seed
        self.n = H.n_nodes(scale)
        self.alpha = 1.0 / (gamma - 1.0)          # rank-weight exponent
        self.salt = H._mix(0x5EED0000 + seed)
        a = H._mix(self.salt + 1) % self.n
        a = max(a, 2)
        while math.gcd(a, self.n) != 1:
            a += 1
        self.a = a
        self.a_inv = pow(a, -1, self.n)
        self.c = H.COMMUNITY
        # members of each community ordered by global rank (hub first)
        self.members: list[list[int]] = []
        for base in range(0, self.n, self.c):
            ids = list(range(base, min(base + self.c, self.n)))
            ids.sort(key=self.rank_of)
            self.members.append(ids)

    def rank_of(self, node: int) -> int:
        return (node * self.a_inv) % self.n

    def node_of(self, rank: int) -> int:
        return (rank * self.a) % self.n

    def draw(self, r: int, m: int) -> int:
        """Rank in [0, m) with weight (rank+1)^-alpha, from 64-bit word r."""
        u = (r >> 11) / _U53
        one = 1.0 - self.alpha
        x = ((((m + 1) ** one) - 1.0) * u + 1.0) ** (1.0 / one)
        k = int(x) - 1
        return min(max(k, 0), m - 1)

    def event(self, i: int, scale: int) -> dict:
        assert scale == self.scale
        t = H._vt_s(i, scale)
        r1 = H._mix((2 * i) ^ self.salt)
        r2 = H._mix((2 * i + 1) ^ self.salt)
        src = self.node_of(self.draw(r1, self.n))
        if r2 % 100 < H.INTRA_PCT:
            mem = self.members[src // self.c]
            dst = mem[self.draw(H._mix(r2), len(mem))]
        else:
            dst = self.node_of(self.draw(H._mix(r2), self.n))
        return {"src": f"n{src}", "dst": f"n{dst}",
                "rel_type": "R" if i % 3 else "S",
                "vt_s": t, "vt_e": t + H.edge_life(scale)}


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
    ap.add_argument("--gamma", type=float, default=2.1)
    ap.add_argument("--seed", type=int, required=True)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()
    if args.out.exists():
        print(f"{args.out} exists; not regenerating")
        return 0
    gen = PowerLaw(args.scale, args.gamma, args.seed)
    H._event = gen.event                      # build_dataset/_edge_ref look it up
    args.out.parent.mkdir(parents=True, exist_ok=True)
    t0 = time.perf_counter()
    data = H.build_dataset(args.scale)
    gen_s = time.perf_counter() - t0
    shutil.copyfile(data.log, args.out)
    reread = H.dataset_from_log(args.out)
    meta = {
        "generator": "powerlaw (scripts/freeze_powerlaw_log.py)",
        "gamma": args.gamma, "rank_weight_exponent": gen.alpha,
        "seed": args.seed, "salt": gen.salt, "permutation_a": gen.a,
        "n_nodes": gen.n, "community": H.COMMUNITY, "intra_pct": H.INTRA_PCT,
        "edge_life": H.edge_life(args.scale),
        "scale_requested": args.scale, "scale_in_log": reread.scale,
        "generated_s": round(gen_s, 1),
        "sha256": sha256(args.out), "bytes": args.out.stat().st_size,
        "derived": {"t0": reread.t0, "t1": reread.t1,
                    "tt_epoch1": reread.tt_epoch1,
                    "filter_uids": list(reread.filter_uids)},
        "reread_matches_generated": (reread.t0 == data.t0 and reread.t1 == data.t1
                                     and reread.tt_epoch1 == data.tt_epoch1),
    }
    Path(str(args.out).replace(".jsonl", ".meta.json")).write_text(json.dumps(meta, indent=1))
    print(json.dumps({k: v for k, v in meta.items() if k != "derived"}, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
