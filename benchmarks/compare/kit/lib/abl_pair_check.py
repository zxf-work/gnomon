#!/usr/bin/env python3
"""abl_pair_check.py <family dir> <window name> [--tamper]: after a window, compare its answers with its pair's, when the pair
has run. Exit 7 if a cell both answered has a different hash. --tamper is a smoke's test of this stop: refused unless the path
holds a smoke label. System python3 (3.6) on the evaluation host."""
import json
import os
import sys


def cells(p):
    with open(p) as f:
        d = json.load(f)
    return {(c.get("query"), c.get("instance")): c for c in d.get("results", [])}


def main():
    fam, name = sys.argv[1], sys.argv[2]
    tamper = "--tamper" in sys.argv or os.environ.get("ABL_SMOKE_TAMPER") == "1"
    if tamper and "smoke-" not in fam:
        print("abl_pair_check: --tamper outside a smoke label: refused")
        return 7
    a = name[len("unset~"):] if name.startswith("unset~") else name
    pa, pb = os.path.join(fam, a + ".json"), os.path.join(fam, "unset~" + a + ".json")
    if not (os.path.exists(pa) and os.path.exists(pb)):
        return 0
    A, B = cells(pa), cells(pb)
    bad = []
    for k, ca in A.items():
        cb = B.get(k)
        if not cb or not ca.get("ok") or not cb.get("ok"):
            continue
        ha = ca.get("hash")
        if tamper:
            ha = "tampered"
        if ha != cb.get("hash"):
            bad.append(k)
    for k in bad[:5]:
        print("ANSWER MISMATCH", fam, a, k)
    return 7 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
