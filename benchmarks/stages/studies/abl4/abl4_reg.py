"""reg.py --store NAME --tag T --log L --ids a,b --arm A --block B --rot R --json OUT [--nb]
One fresh process = one (store, arm) block. Opens <EXTCMP_ROOT>/stores/<NAME> with the extcmp
Native system and, per registry id: 5 warmups, then
30 timed calls (10 if the first timed call takes >= 1 s). A call raising CostError is recorded
as refused (no timing). --nb adds direct neighbors_batch (50 and 800 vertices, both directions,
at the B family's belief time T and at now), as s16_ab.py."""
from __future__ import annotations
import argparse, hashlib, json, os, random, sys, time
from pathlib import Path
import numpy as np
sys.path.insert(0, os.environ["EXTCMP_REPO"] + "/scripts")
import extcmp as X  # noqa: E402
from gnomon.core.errors import CostError  # noqa: E402

pc = time.perf_counter
ap = argparse.ArgumentParser()
for k in ("store", "tag", "log", "ids", "arm", "json"):
    ap.add_argument("--" + k, required=True)
ap.add_argument("--block", type=int, default=0)
ap.add_argument("--rot", type=int, default=0)
ap.add_argument("--reps", type=int, default=30)
ap.add_argument("--warm", type=int, default=5)
ap.add_argument("--nb", action="store_true")
a = ap.parse_args()
root = Path(os.environ["EXTCMP_ROOT"])
data = X.dataset(Path(a.log))
ids = [i for i in a.ids.split(",") if i]
qs = X.queries(data, ids) if ids else []
need_T = a.nb and not any(q.id == "b.snap.hop2@T" for q in qs)
if need_T:
    qT = X.queries(data, ["b.snap.hop2@T"])[0]
nat = X.Native(None, root, a.tag)
store = root / "stores" / a.store
nat.__class__ = type("N", (X.Native,), {"path": property(lambda s: store)})
t0 = pc(); nat.open(); open_ms = (pc() - t0) * 1e3
ad = nat.adapter
designs_off = sorted(k for k, v in ad.designs().items() if not v)
env = {k: v for k, v in os.environ.items() if k.startswith("GNOMON_") and k not in
       ("GNOMON_VOL", "GNOMON_STORES", "GNOMON_BUNDLES", "GNOMON_RECORDS", "GNOMON_REPO")}
common = {"arm": a.arm, "block": a.block, "rot": a.rot, "store": a.store, "designs_off": designs_off,
          "env": env, "engine": __import__("gnomon._engine").__dict__["_engine"].__file__}
out = open(a.json, "a")
print(f"{a.arm} b{a.block} r{a.rot} {a.store} open {open_ms:.0f} ms designs_off={designs_off} "
      f"load={open('/proc/loadavg').read().split()[:3]}", flush=True)


def rec(name, ms, h, extra=None):
    r = dict(common, query=name, ms=ms, hash=h, load=open("/proc/loadavg").read().split()[:3])
    r.update(extra or {})
    out.write(json.dumps(r) + "\n"); out.flush()
    if ms:
        s = sorted(ms)
        print(f"{a.arm:8s} b{a.block} {a.store:16s} {name:22s} med {s[len(s) // 2]:9.3f} ms n={len(ms)} {h[:12]}", flush=True)
    else:
        print(f"{a.arm:8s} b{a.block} {a.store:16s} {name:22s} {extra}", flush=True)


T = int(qT.args["as_of_tt"]) if need_T else None
for q in qs:
    fn = nat.fn_for(q)
    try:
        for _ in range(a.warm):
            p = fn()
        t = pc(); p = fn(); first = (pc() - t) * 1e3
        reps = 10 if first >= 1000 else a.reps
        ms = []
        for _ in range(reps):
            t = pc(); p = fn(); ms.append((pc() - t) * 1e3)
    except CostError as e:
        rec(q.id, [], "refused", {"refused": True, "error": f"{type(e).__name__}: {e}"[:300]})
        continue
    rec(q.id, ms, X.answer_hash(p), {"first_ms": first, "args": q.args})
    if q.id == "b.snap.hop2@T":
        T = int(q.args["as_of_tt"])
if a.nb:
    st = ad._store
    n = int(st.num_entities())
    rng = random.Random(16)
    for size in (50, 800):
        us = np.array(sorted(rng.sample(range(n), size)), dtype=np.int64)
        for label, as_of in (("T", T), ("now", 2 ** 62 - 1)):
            for _ in range(a.warm):
                st.neighbors_batch(us, "both", int(as_of), None, None, None)
            ms = []
            for _ in range(a.reps):
                t = pc()
                got = st.neighbors_batch(us, "both", int(as_of), None, None, None)
                ms.append((pc() - t) * 1e3)
            h = hashlib.sha256()
            for k in sorted(got):
                h.update(k.encode()); h.update(np.ascontiguousarray(got[k]).tobytes())
            rec(f"nb{size}@{label}", ms, h.hexdigest()[:24], {"rows": len(got["nbr"])})
    stats = getattr(st, "tt_e_column_stats", None)
    if stats:
        print(a.arm, a.store, "tt_e_column_stats", json.dumps(stats()), flush=True)
nat.close()
