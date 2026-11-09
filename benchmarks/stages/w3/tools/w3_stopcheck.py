#!/usr/bin/env python3
"""W3 stop rule, applied after every Gnomon timed block:  w3_stopcheck.py BLOCK.json POINT   (exit 4 = stop the job).

Stops on a Gnomon cell that is neither answered nor censored (> 10 s), except a refusal that the rows label
the four SF10 cells IC4 window i = 0, 5, 6, 7 when they fail with "an intermediate binding table ... exceeds the executor's limit"
i = 0..7 refused by admission (slow case S44: 'refused (estimated > 10 s)', counted against Gnomon). A censored Gnomon cell does not stop the job (it is a result:
"> 10 s"). The answer gate already failed the stage on any other Gnomon error or refusal, so a stop here means the timed run
disagrees with the gate.
"""
import json
import sys

SF10_LABELLED = {"BI15", "BI19", "IC14"}
TOLERATED = {("SF10", "IC4", "window", i) for i in (0, 5, 6, 7)}
REFUSED_TOLERATED = {("SF10", "BI20", "window", i) for i in range(8)}


def labelled(point, q, s):
    return point == "SF10" and (q in SF10_LABELLED or (q == "IC13" and s == "window"))


def main() -> int:
    block, point = sys.argv[1], sys.argv[2]
    bad, recorded = [], 0
    for c in json.load(open(block))["results"]:
        if c.get("ok") or c.get("censored"):
            continue
        err = str(c.get("error", ""))
        if "CostError" in err and labelled(point, c["query"], c["setting"]):
            continue
        if "CostError" in err and (point, c["query"], c["setting"], c["instance"]) in REFUSED_TOLERATED:
            continue
        if (point, c["query"], c["setting"], c["instance"]) in TOLERATED and "binding table" in err:
            recorded += 1
            continue
        bad.append((c["query"], c["setting"], c["instance"], err[:120]))
    for b in bad[:20]:
        print("STOP RULE: Gnomon cell not answered:", *b)
    print(f"stopcheck {block}: {len(bad)} Gnomon cells neither answered nor censored ({recorded} recorded binding-table errors tolerated)")
    return 4 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
