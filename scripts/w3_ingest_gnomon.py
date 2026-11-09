#!/usr/bin/env python3
"""The timed ingest study's Gnomon run: one frozen W3 log through the writer's path, the steps of `Store._write_locked` at the log's own transaction
time: event-log append with fsync, apply, commit, then the engine's compaction trigger as `eventlog.replay` uses it. `w3_replay_check.py` omits the append, so it omits a write the
durability statement includes.

    w3_ingest_gnomon.py --log L --meta M --store S --out REC.json [--prefix-days N]

Every batch is timed by part. Exit 1 on a refused batch (a frozen log has none)."""
import argparse
import hashlib
import json
import os
import resource
import subprocess
import time
from pathlib import Path

import gnomon
from gnomon.core.errors import GnomonError
from gnomon.storage.eventlog import REPLAY_GC_EVERY_COMPACTIONS, EventLog, extend_chain

SEC = 1_000_000


def kind_of(tt, t0, ends):
    if tt <= t0:
        return "snapshot"
    return "day" if tt in ends else ("corrections" if tt - SEC in ends else "other")


def items_of(ops):
    return sum(len(o.get("events", ())) + len(o.get("nodes", ())) if o["op"] == "ingest_events" else 1 for o in ops)


def log_check(input_path, written_path, until):
    """The log this run wrote against its input, records with tt <= until (all if None): record count, item count, and a checksum of (tt, ops) in canonical JSON. Headers and fields a writer adds
    are outside it. Whether the bytes are also equal is recorded, not required."""
    def walk(path):
        h, hb, n, items = hashlib.sha256(), hashlib.sha256(), 0, 0
        with open(path) as f:
            f.readline()
            for line in f:
                r = json.loads(line)
                if until is not None and r["tt"] > until:
                    break
                n += 1
                items += items_of(r["ops"])
                h.update(json.dumps([r["tt"], r["ops"]], sort_keys=True, separators=(",", ":")).encode())
                hb.update(line.encode())
        return n, items, h.hexdigest(), hb.hexdigest()
    a, b = walk(input_path), walk(written_path)
    return {"input_records": a[0], "written_records": b[0], "input_items": a[1], "written_items": b[1], "input_sha256": a[2], "written_sha256": b[2], "bytes_equal": a[3] == b[3], "equal": a[:3] == b[:3]}


def summarize(per):
    out = {}
    for b in per:
        o = out.setdefault(b["kind"], {"batches": 0, "items": 0, "log_s": 0.0, "apply_s": 0.0, "commit_s": 0.0, "compact_s": 0.0})
        o["batches"] += 1
        o["items"] += b["items"]
        for k in ("log_s", "apply_s", "commit_s", "compact_s"):
            o[k] += b[k]
    for o in out.values():
        o["s"] = o["log_s"] + o["apply_s"] + o["commit_s"] + o["compact_s"]
        o["items_per_s"] = round(o["items"] / o["s"], 1) if o["s"] else None
        for k in ("log_s", "apply_s", "commit_s", "compact_s", "s"):
            o[k] = round(o[k], 3)
    return out


def main():
    ap = argparse.ArgumentParser()
    for k in ("log", "meta", "store", "out"):
        ap.add_argument("--" + k, required=True)
    ap.add_argument("--prefix-days", type=int)
    a = ap.parse_args()
    if os.environ.get("GNOMON_SMALL_COMMIT"):
        raise SystemExit("GNOMON_SMALL_COMMIT is set: this study's Gnomon runs with it off")
    meta = json.loads(Path(a.meta).read_text())
    ends = [d["day_end_us"] for d in meta["days"]]
    t0 = meta["snapshot"]["tt_last"]
    L = ends[a.prefix_days - 1] + SEC if a.prefix_days else None
    ends = set(ends)
    st = gnomon.open(a.store, backend="native")
    ad = st.adapter
    note_cursor = getattr(ad, "note_event_cursor", None)
    pc = time.perf_counter
    per, refused, compactions = [], 0, 0
    hwm = lambda: resource.getrusage(resource.RUSAGE_SELF).ru_maxrss     # KB, this process's high-water mark so far
    w0 = pc()
    marks = {"start": time.time()}
    for batch, _end, _raw in EventLog(a.log).batches_from(0):
        tt, ops = batch["tt"], batch["ops"]
        if L is not None and tt > L:
            break
        if tt > t0 and "bulk_end" not in marks:
            marks["bulk_end"] = time.time()
        t1 = pc()
        _id, end_offset, record = st.eventlog.append(tt, ops)
        if note_cursor is not None:
            if st._chain is None:
                st._chain = st.eventlog.chain_of_prefix(end_offset - len(record))
            st._chain = extend_chain(st._chain, record)
        t2 = pc()
        h_log = hwm()
        ad.begin()
        try:
            ad.apply_ops(ops, tt, raw=record) if getattr(ad, "accepts_raw_record", False) else ad.apply_ops(ops, tt)
        except GnomonError as e:
            ad.rollback()
            refused += 1
            print(json.dumps({"refused_batch": tt, "error": f"{type(e).__name__}: {e}"[:500]}), flush=True)
            continue
        if note_cursor is not None:
            note_cursor(end_offset, st._chain)
        t3 = pc()
        h_apply = hwm()
        ad.commit()
        t4 = pc()
        h_commit = hwm()
        if ad.needs_compaction():
            ad.compact_incremental()
            compactions += 1
            if compactions % REPLAY_GC_EVERY_COMPACTIONS == 0:
                ad.gc(keep_last=2)
        t5 = pc()
        per.append({"tt": tt, "kind": kind_of(tt, t0, ends), "items": items_of(ops), "log_s": t2 - t1, "apply_s": t3 - t2, "commit_s": t4 - t3, "compact_s": t5 - t4,
                    "hwm_log_kb": h_log, "hwm_apply_kb": h_apply, "hwm_commit_kb": h_commit, "hwm_compact_kb": hwm()})
    wall = pc() - w0
    marks["end"] = time.time()
    tg = pc()
    if compactions:
        ad.gc(keep_last=2)
    gc_s = pc() - tg
    stats = st.stats()
    st.close()
    lc = log_check(a.log, str(Path(a.store) / "eventlog.jsonl"), L)     # after the timed part
    bk = summarize(per)
    phase2 = sum(v["s"] for k, v in bk.items() if k != "snapshot")
    rec = {"system": "gnomon", "log": a.log, "prefix_days": a.prefix_days, "until_tt": L, "t0": t0, "wall_incl_log_read_s": round(wall, 3), "phase1_s": bk.get("snapshot", {}).get("s"),
           "phase2_s": round(phase2, 3), "final_gc_s": round(gc_s, 3), "compactions": compactions, "refused": refused, "marks": marks, "peak_rss_kb": hwm(), "log_check": lc, "by_kind": bk, "per_batch": [{k: (round(v, 4) if isinstance(v, float) else v) for k, v in b.items()} for b in per],
           "stats": {k: stats.get(k) for k in ("n_entities", "n_node_versions", "n_edge_versions")}, "verified": refused == 0 and lc["equal"],
           "bytes_on_disk": int(subprocess.run(["du", "-sb", a.store], capture_output=True, text=True).stdout.split()[0]), "small_commit": False}
    Path(a.out).write_text(json.dumps(rec, indent=1) + "\n")
    print(json.dumps({k: v for k, v in rec.items() if k != "per_batch"}))
    return 0 if rec["verified"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
