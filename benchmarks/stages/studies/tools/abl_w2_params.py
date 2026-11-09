#!/usr/bin/env python3
"""abl_w2_params.py --in DIR --out DIR --tags K10M,K10M-d0,K10M-d10 [--non-hub 3] [--hub-per-query N]

The W2 ablation's instance list, drawn from the W2 parameter files before any ablation result exists. It keeps every
hub instance (stratum "hub"; --hub-per-query caps them, for a smoke only) and, per (query, class), the first --non-hub
non-hub instances by md5(salt|tag|query|class|instance). Output keeps the P1 format; SHA256SUMS is written beside it."""
import argparse
import hashlib
import json
from collections import defaultdict
from pathlib import Path

SALT = "gnomon-c4-ablation-w2-2026-11-05"


def is_hub(i: dict) -> bool:
    return i.get("stratum") == "hub"


def pick(doc: dict, tag: str, non_hub: int, hub_per_query: int = 0) -> list:
    groups = defaultdict(list)
    hubs = defaultdict(list)
    for i in doc["instances"]:
        (hubs[i["query"]] if is_hub(i) else groups[(i["query"], str(i.get("class")))]).append(i)
    key = lambda i: hashlib.md5(f"{SALT}|{tag}|{i['query']}|{i.get('class')}|{i['instance']}".encode()).hexdigest()
    keep = set()
    for g in groups.values():
        keep.update(id(x) for x in sorted(g, key=key)[:non_hub])
    for g in hubs.values():
        keep.update(id(x) for x in (sorted(g, key=key)[:hub_per_query] if hub_per_query else g))
    return [i for i in doc["instances"] if id(i) in keep]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="src", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--tags", required=True)
    ap.add_argument("--non-hub", type=int, default=3)
    ap.add_argument("--hub-per-query", type=int, default=0)
    a = ap.parse_args()
    a.out.mkdir(parents=True, exist_ok=True)
    sums = []
    for tag in a.tags.split(","):
        src = a.src / f"params-{tag}.json"
        doc = json.loads(src.read_text())
        ins = pick(doc, tag, a.non_hub, a.hub_per_query)
        if not ins:
            raise SystemExit(f"{src}: no instance kept")
        counts = defaultdict(int)
        for i in ins:
            counts[i["query"]] += 1
        doc["instances"] = ins
        doc["counts"] = dict(counts)
        doc.setdefault("selection", {})["ablation_w2"] = {"salt": SALT, "non_hub_per_class": a.non_hub, "hub_per_query": a.hub_per_query or "all",
                                                         "source_sha256": hashlib.sha256(src.read_bytes()).hexdigest()}
        out = a.out / src.name
        out.write_text(json.dumps(doc, indent=1, sort_keys=True) + "\n")
        sums.append(f"{hashlib.sha256(out.read_bytes()).hexdigest()}  {src.name}")
        print(f"{tag}: {len(ins)} instances, {sum(1 for i in ins if is_hub(i))} hub")
    (a.out / "SHA256SUMS").write_text("\n".join(sums) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
