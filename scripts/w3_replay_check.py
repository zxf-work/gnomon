"""Replay a frozen log into a fresh native store and report every batch the engine refuses.

    w3_replay_check.py --log L --store S [--max-fail 5]

`gnomon.storage.eventlog.replay` skips a refused batch silently (a batch that failed on the live path fails identically on replay). A frozen benchmark log must have none, so this replays batch by batch and prints the refusal of each one.
"""
import argparse, json, sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import gnomon
from gnomon.core.errors import GnomonError
from gnomon.storage.eventlog import EventLog

a = argparse.ArgumentParser(); a.add_argument("--log", required=True); a.add_argument("--store", required=True)
a.add_argument("--max-fail", type=int, default=5); a = a.parse_args()
st = gnomon.open(a.store, backend="native"); ad = st.adapter
n = fail = 0; t0 = time.time(); prof = []
for batch, end, raw in EventLog(a.log).batches_from(0):
    tt = batch["tt"]; ad.begin(); tb = time.perf_counter()
    try:
        ad.apply_ops(batch["ops"], tt, raw=raw) if getattr(ad, "accepts_raw_record", False) else ad.apply_ops(batch["ops"], tt)
    except GnomonError as e:
        ad.rollback(); fail += 1
        kinds = {}
        for o in batch["ops"]:
            kinds[o["op"]] = kinds.get(o["op"], 0) + 1
        print(json.dumps({"refused_batch": n, "tt": tt, "ops": kinds, "error": f"{type(e).__name__}: {e}"[:1500]}), flush=True)
        if fail >= a.max_fail:
            break
        n += 1; continue
    ad.commit(); n += 1; ta = time.perf_counter()
    comp = 0.0
    if ad.needs_compaction():
        ad.compact_incremental(); comp = time.perf_counter() - ta
    kinds = {}
    for o in batch["ops"]:
        kinds[o["op"]] = kinds.get(o["op"], 0) + (len(o.get("events", ())) + len(o.get("nodes", ())) if o["op"] == "ingest_events" else 1)
    prof.append({"batch": n - 1, "tt": tt, "apply_commit_s": round(ta - tb, 3), "compact_s": round(comp, 3), "items": kinds})
if prof:
    Path(a.store + ".profile.jsonl").write_text("".join(json.dumps(x) + "\n" for x in prof))
    snap = [x for x in prof if set(x["items"]) == {"ingest_events"} and x["tt"] < prof[0]["tt"] + 10**6]
    corr = [x for x in prof if "ingest_events" not in x["items"]]
    day = [x for x in prof if x not in snap and x not in corr]
    for name, xs in (("snapshot", snap), ("day", day), ("correction", corr)):
        items = sum(sum(x["items"].values()) for x in xs)
        sec = sum(x["apply_commit_s"] for x in xs); cs = sum(x["compact_s"] for x in xs)
        print(json.dumps({"class": name, "batches": len(xs), "items": items, "apply_commit_s": round(sec, 1),
                          "compact_s": round(cs, 1), "items_per_s": round(items / sec) if sec else None}))
print(json.dumps({"batches": n, "refused": fail, "wall_s": round(time.time() - t0, 1), "stats": st.stats()}, default=str))
st.close()
