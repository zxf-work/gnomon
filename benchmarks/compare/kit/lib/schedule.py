#!/usr/bin/env python3
"""Campaign schedule for one input size: `kind size tag rot family system` per line.

    schedule.py <size> <tag> loads|verify|<rotation number> [families, e.g. B]

Rotations: families P T S W D N R M; inside block b of rotation r the system
order is ROTATION rotated by (b + 4*(r-1)); Raphtory only in T and N; AeonG
as one contiguous window (load + all families), first in odd rotations and
last in even ones (it cannot keep its history across a server restart).
"""

import sys

ROTATION = ["native", "duckdb", "postgres", "clickhouse", "neo4j", "memgraph", "xtdb",
            "ladybug", "raphtory"]
FAMILIES = ["P", "T", "S", "W", "D", "N", "R", "M"]
RAPHTORY_FAMILIES = {"T", "N"}
# EXTCMP_SYSTEMS (comma list, e.g. native,duckdb,postgres): run only these
# systems, in ROTATION's order; unset, every system (AeonG included)
_ONLY = [s for s in __import__("os").environ.get("EXTCMP_SYSTEMS", "").split(",") if s]
if _ONLY:
    ROTATION = [s for s in ROTATION if s in _ONLY]
AEONG = not _ONLY or "aeong" in _ONLY


def main() -> int:
    size, tag, what = sys.argv[1], sys.argv[2], sys.argv[3]
    families = sys.argv[4].split(",") if len(sys.argv) > 4 else FAMILIES
    out = []
    if what == "loads":
        out += [f"load {size} {tag} - - {s}" for s in ROTATION]
    elif what == "verify":
        out += [f"verify {size} {tag} - - {s}" for s in ROTATION]
        if AEONG:
            out.append(f"aeongverify {size} {tag} - - aeong")
    else:
        r = int(what)
        if r % 2 == 1 and AEONG:
            out.append(f"aeong {size} {tag} {r} - aeong")
        for b, fam in enumerate(families):
            k = (b + 4 * (r - 1)) % len(ROTATION)
            for s in ROTATION[k:] + ROTATION[:k]:
                if s == "raphtory" and fam not in RAPHTORY_FAMILIES | {"B"}:
                    continue
                out.append(f"block {size} {tag} {r} {fam} {s}")
        if r % 2 == 0 and AEONG:
            out.append(f"aeong {size} {tag} {r} - aeong")
    print("\n".join(out))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
