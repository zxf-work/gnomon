#!/usr/bin/env python3
"""W4 on C4. Three checks:
  w4_gate.py gate --new answers-gnomon-c4.jsonl --check-dir check-sf10/r0 --out gate.md
      Gnomon's answers on C4 against the answers recorded for every other system on the same store and parameter file. Exit 7 on a
      differing hash, on a Gnomon error, or on a refusal of a call another system answered inside 10 s. A cell another system did not
      answer has no reference and is listed.
  w4_gate.py stop --block rot1/gnomon.json --check-dir ...
      The stop rule after a timed Gnomon block: exit 4 on an error, or on a refusal of a call another system answered inside 10 s. A
      censored cell does not stop.
  w4_gate.py timed-check --res DIR --out md
      Every system's timed hashes against Gnomon's in the same job. Exit 8 on a difference."""
import argparse
import glob
import json
import os
from collections import Counter, defaultdict


def jl(p):
    with open(p) as f:
        return [json.loads(x) for x in f if x.strip()]


def recorded(check_dir):
    out = defaultdict(dict)
    for p in sorted(glob.glob(os.path.join(check_dir, "answers-*.jsonl"))):
        name = os.path.basename(p)[len("answers-"):-len(".jsonl")]
        if name.startswith("gnomon"):
            continue
        for r in jl(p):
            out[name][(r["query"], r["instance"])] = r
    return out


def gate(new, check_dir):
    rec = recorded(check_dir)
    if not rec:
        raise SystemExit(f"no recorded answers in {check_dir}")
    fail, unref = [], []
    cnt = Counter()
    for r in new:
        k = (r["query"], r["instance"])
        others = {s: v[k] for s, v in rec.items() if k in v and v[k].get("status") == "ok"}
        if r.get("status") != "ok":
            fast = [s for s, o in others.items() if o.get("ms", 1e18) <= 10000]
            if r.get("status") in ("refused", "cost") and not fast:
                cnt["refused, no other inside 10 s"] += 1
            else:
                fail.append(f"{k[0]}#{k[1]} Gnomon {r.get('status')} (others inside 10 s: {fast}) {str(r.get('error'))[:120]}")
            continue
        if not others:
            unref.append(f"{k[0]}#{k[1]}")
            cnt["no reference"] += 1
            continue
        bad = [s for s, o in others.items() if o.get("hash") != r.get("hash")]
        if bad:
            fail.append(f"{k[0]}#{k[1]} differs from {bad}")
        else:
            cnt["equivalent"] += 1
    return fail, unref, cnt


def stop(cells, rec):
    out = []
    for c in cells:
        if c.get("ok") or c.get("censored") or c.get("aborted"):
            continue
        k = (c.get("query"), c.get("instance"))
        fast = [s for s, v in rec.items() if k in v and v[k].get("status") == "ok" and v[k].get("ms", 1e18) <= 10000]
        err = str(c.get("error", ""))
        if "refus" in err.lower() or "E_COST" in err or "estimated" in err:
            if fast:
                out.append(f"{k} refused, answered inside 10 s by {fast}")
        else:
            out.append(f"{k} error: {err[:160]}")
    return out


def timed_check(res):
    ref, diffs, n = {}, [], Counter()
    blocks = sorted(glob.glob(os.path.join(res, "rot*", "*.json")))
    blocks.sort(key=lambda p: 0 if os.path.basename(p).startswith("gnomon") else 1)
    for p in blocks:
        with open(p) as f:
            d = json.load(f)
        name = os.path.basename(os.path.dirname(p)) + "/" + os.path.basename(p)[:-5]
        for c in d.get("results", []):
            if not c.get("ok") or not c.get("hash"):
                continue
            k = (c.get("query"), c.get("setting"), c.get("instance"))
            if os.path.basename(p).startswith("gnomon"):
                if ref.setdefault(k, c["hash"]) != c["hash"]:
                    diffs.append(f"{k} Gnomon rotations differ")
            elif k in ref:
                n["compared"] += 1
                if ref[k] != c["hash"]:
                    diffs.append(f"{k} {name} differs from Gnomon")
    return diffs, n


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    g = sub.add_parser("gate"); g.add_argument("--new"); g.add_argument("--check-dir"); g.add_argument("--out")
    s = sub.add_parser("stop"); s.add_argument("--block"); s.add_argument("--check-dir")
    t = sub.add_parser("timed-check"); t.add_argument("--res"); t.add_argument("--out")
    a = ap.parse_args()
    if a.cmd == "gate":
        fail, unref, cnt = gate(jl(a.new), a.check_dir)
        lines = [f"W4 gate: {dict(cnt)}; {len(fail)} failures; {len(unref)} cells without a reference"] + [f"FAIL {x}" for x in fail[:60]] + [f"no reference {x}" for x in unref[:60]]
        open(a.out, "w").write("\n".join(lines) + "\n")
        print(lines[0])
        for x in fail[:10]:
            print("FAIL", x)
        return 7 if fail else 0
    if a.cmd == "stop":
        if not os.path.exists(a.block):
            print("STOPCHECK: no record", a.block)
            return 4
        with open(a.block) as f:
            cells = json.load(f).get("results", [])
        bad = stop(cells, recorded(a.check_dir))
        for x in bad[:20]:
            print("STOPCHECK STOP", x)
        print(f"STOPCHECK {a.block}: {len(cells)} cells, {'STOP' if bad else 'ok'}")
        return 4 if bad else 0
    diffs, n = timed_check(a.res)
    open(a.out, "w").write(f"W4 timed answers: {dict(n)}; {len(diffs)} differences\n" + "\n".join(diffs[:200]) + "\n")
    print(f"W4 timed answers: {dict(n)}; {len(diffs)} differences")
    return 8 if diffs else 0


if __name__ == "__main__":
    raise SystemExit(main())
