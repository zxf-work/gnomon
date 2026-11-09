#!/usr/bin/env python3
"""Parameter files of the t10 and k100m stages. Written once before the gate jobs, checked by sha256 (SHA256SUMS) in every job.

    x_params.py w1 --logs DIR --sums FILE --base params-K10M.json --out DIR K10M-d0 K10M-d10
        W1's single-parameter rule (benchmarks/stages/w1/tools/w1_single_params.py): one instance per registry id of families P T S W D N R M plus the
        native-only ids, whose arguments are exactly the registry's own, extcmp.queries on the tag's frozen log. Reads the logs' side files only and refuses a
        stale one (it must not scan or write in the frozen directory). Reports whether each file's instances equal the K10M file's.
    x_params.py w2 --src params-K10M.json --sums FILE --out DIR
        W2's 234-instance registry subset at K10M-d0 and K10M-d10: the instances verbatim, so d0 and d10 ask the same
        calls and d0 joins the earlier W2 K10M record by instance id; only the file's log block names the log it runs on.
    x_params.py k100m --log LOCAL --sums FILE --tag TAG --out DIR
        The same single-parameter rule on a node-local copy of the log, with the B family added (the rule's density-point form), and the log's side files
        copied to DIR/side, so later jobs do not scan the log again.
    x_params.py subset --src FILE --queries a,b,c --out DIR --name params-TAG.json
        A smoke's file: the first non-hub instance of each named query. Not results.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
from pathlib import Path

import extcmp as X          # the pinned kit (PYTHONPATH = $REPO:$REPO/scripts)
import extcmp_p1 as P1

VERSION = "w1-single-v1"    # the rule's own version string: an instance id is md5(version, query, arguments), so equal calls have equal ids across sizes and densities
W2_NAMES = {"K10M-d0": "powerlaw-10000000", "K10M-d10": "W2-10M-d10"}


def sums(path: Path) -> dict:
    return dict(reversed(l.split()) for l in path.read_text().splitlines() if l.strip())


def write(out: Path, name: str, doc: dict) -> None:
    out.mkdir(parents=True, exist_ok=True)
    (out / name).write_text(json.dumps(doc, indent=1) + "\n")
    lines = {}
    sf = out / "SHA256SUMS"
    if sf.exists():
        lines = {l.split()[1]: l.split()[0] for l in sf.read_text().splitlines() if l.strip()}
    lines[name] = hashlib.sha256((out / name).read_bytes()).hexdigest()
    sf.write_text("".join(f"{v}  {k}\n" for k, v in sorted(lines.items())))
    print(f"{out / name}: {len(doc['instances'])} instances, sha256 {lines[name]}")


def single(log: Path, ids: list, lsha: str, notes: list) -> dict:
    data = X.dataset(log)
    insts = []
    for q in X.queries(data, ids):
        args = json.loads(json.dumps(q.args, sort_keys=True, default=list))
        iid = P1.md5(VERSION, q.id, json.dumps(args, sort_keys=True))[:8]
        insts.append({"query": q.id, "instance": iid, "stratum": "registry instance", "class": "single parameter (v3)", "factors": {}, "args": args})
    return {"format": P1.FORMAT, "version": VERSION, "log": {"name": log.name, "sha256": lsha},
            "selection": {"notes": notes, "dataset": {"t0": data.t0, "t1": data.t1, "tt_epoch1": data.tt_epoch1, "scale": data.scale, "n_filter_uids": len(data.filter_uids)}},
            "counts": {q["query"]: 1 for q in insts}, "instances": insts}


REG = [q for f in "PTSWDNRM" for q in X.FAMILIES[f]] + [q for f in "PTSWDNRM" for q in X.NATIVE_ONLY_BY_FAMILY.get(f, ())]
RULE = "one instance per registry id: the registry's own arguments (extcmp.queries on this log), by the rule of w1_single_params.py"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["w1", "w2", "k100m", "subset"])
    ap.add_argument("--logs", type=Path); ap.add_argument("--log", type=Path); ap.add_argument("--sums", type=Path); ap.add_argument("--base", type=Path)
    ap.add_argument("--src", type=Path); ap.add_argument("--out", type=Path, required=True); ap.add_argument("--tag"); ap.add_argument("--queries"); ap.add_argument("--name")
    ap.add_argument("tags", nargs="*")
    a = ap.parse_args()
    if a.mode == "w1":
        known, base = sums(a.sums), json.loads(a.base.read_text())
        for tag in a.tags:
            log = a.logs / f"{tag}.eventlog.jsonl"
            side, st = log.with_suffix(".extcmp-params.json"), log.stat()
            d = json.loads(side.read_text())
            if d.get("size") != st.st_size or d.get("mtime_ns") != st.st_mtime_ns:
                sys.exit(f"{side}: stale, refusing to scan in the frozen directory")
            doc = single(log, REG, known[log.name], [RULE])
            same = doc["instances"] == base["instances"]
            doc["selection"]["same_instances_as_K10M_file"] = same
            print(f"{tag}: instances equal the K10M file's: {same}")
            write(a.out, f"params-{tag}.json", doc)
    elif a.mode == "w2":
        known = sums(a.sums)
        for tag, n in W2_NAMES.items():
            doc = json.loads(a.src.read_text())
            doc["selection"].setdefault("notes", []).append(f"t10: the registry subset's instances verbatim at {tag}; only this log block changed (the source file names the 10M base log)")
            doc["log"] = {"name": f"{tag}.eventlog.jsonl", "frozen_as": f"{n}.eventlog.jsonl", "sha256": known[f"{n}.eventlog.jsonl"]}
            write(a.out, f"params-{tag}.json", doc)
    elif a.mode == "k100m":
        known = sums(a.sums)
        doc = single(a.log, REG + list(X.FAMILIES["B"]), known[a.log.name], [RULE, "with the eight belief-time ids of family B, as the rule's density-point form"])
        write(a.out, f"params-{a.tag}.json", doc)
        (a.out / "side").mkdir(parents=True, exist_ok=True)
        for suf in (".extcmp-params.json", ".beliefq-params.json"):
            shutil.copy2(a.log.with_suffix(suf), a.out / "side" / a.log.with_suffix(suf).name)
        print("dataset", doc["selection"]["dataset"])
    else:
        doc = json.loads(a.src.read_text())
        want, keep = a.queries.split(","), []
        for q in want:
            c = [i for i in doc["instances"] if i["query"] == q and i.get("stratum") != "hub"]
            if not c:
                sys.exit(f"no non-hub instance of {q}")
            keep.append(c[0])
        doc["instances"], doc["counts"] = keep, {q: 1 for q in want}
        doc["selection"].setdefault("notes", []).append("smoke subset, not results")
        write(a.out, a.name, doc)
    return 0


if __name__ == "__main__":
    sys.exit(main())
