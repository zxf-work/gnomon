"""Answer-agreement check for the correction-density study's belief-time
queries, at a small input size, on the embedded systems.

    extcmp_beliefq_check.py --scale 20000 --work <scratch dir>

Per density point (0, 1, 10 %): freeze the synthetic log once, replay it into
a native store, inject corrections with the existing batched injector
(`scripts/inject_corrections.py`, seed 0, batch 100), take the store's own
event log as that density's frozen log, then compare, for every b.* id:

* the native engine against the version-table reference
  (`extcmp_beliefq.reference`) and, for the two as-of graph ids, against the
  brute-force Oracle (`gnomon.temporal.oracle`);
* the DuckDB backend (replayed from the same log) against native;
* LadybugDB (loaded from the native store) against native.

With `--server-systems`, also each server-hosted system (PostgreSQL,
ClickHouse, Neo4j, Memgraph, XTDB, AeonG): its server is started on its own
data directory (`--servers-sh`), loaded with the belief-axis indexes, answered,
compared with native (XTDB on the projected answer), stopped and wiped. Run
this under the host's shared lock (untimed).

Also asserts the design properties the study relies on: every as-of id equals
its control at density 0; `b.snap.hop2@T` differs between densities; `b.diff`
is empty at 0 and grows with density; `b.hist` has one version at 0 and more at
1 % and 10 %; `b.reach@T` is identical across densities (the cost probe).
Exits non-zero on any failure. Server-hosted systems are checked in the
host windows of the study, not here.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import eval_harness as H  # noqa: E402
import extcmp as X  # noqa: E402
import extcmp_beliefq as B  # noqa: E402
import inject_corrections as IC  # noqa: E402

import gnomon  # noqa: E402
from gnomon.temporal.algebra import ensure_all_registered, validate_args  # noqa: E402
from gnomon.temporal.oracle import Oracle  # noqa: E402

ensure_all_registered()

FAIL: list[str] = []


def check(cond: bool, msg: str) -> None:
    print(("  ok    " if cond else "  FAIL  ") + msg, flush=True)
    if not cond:
        FAIL.append(msg)


def answers_for(system: str, store_dir: Path, log: Path, ref: Path, tag: str, qs) -> dict:
    cls = X._system_class(system)
    so = cls(ref, store_dir, tag)
    so.log = log
    so.open()
    out = {}
    for q in qs:
        fn = so.fn_for(q)
        if fn is None:
            out[q.id] = ("not written", None)
            continue
        try:
            p = fn()
            out[q.id] = (X.answer_hash(p), p)
        except Exception as e:                              # noqa: BLE001
            out[q.id] = (f"error: {type(e).__name__}: {e}"[:200], None)
    so.close()
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--scale", type=int, default=20000)
    ap.add_argument("--work", type=Path, required=True)
    ap.add_argument("--densities", default="0,1,10")
    ap.add_argument("--skip-ladybug", action="store_true")
    ap.add_argument("--server-systems", default="",
                    help="comma list of postgres,clickhouse,neo4j,memgraph,xtdb,aeong")
    ap.add_argument("--servers-sh", type=Path,
                    default=Path(__file__).resolve().parents[1] /
                    "benchmarks/compare/kit/lib/servers.sh")
    ap.add_argument("--base-log", type=Path, help="use this frozen base log instead of generating one")
    ap.add_argument("--batch", type=int, default=0,
                    help="injector batch size (the study uses 1000 at 10M/100M; small here "
                         "so a 20k log still has many correction batches)")
    args = ap.parse_args()
    w = args.work
    if w.exists():
        shutil.rmtree(w)
    (w / "stores").mkdir(parents=True)
    base = w / "base.eventlog.jsonl"
    if args.base_log:
        shutil.copyfile(args.base_log, base)
    else:
        data0 = H.build_dataset(args.scale)
        shutil.copyfile(data0.log, base)
        shutil.rmtree(data0.log.parent.parent, ignore_errors=True)
    servers = [x for x in args.server_systems.split(",") if x]
    if servers:
        os.environ.update({"PGHOST": "127.0.0.1", "PGPORT": "15432", "PGUSER": "postgres",
                           "GNOMON_NEO4J_URI": "bolt://127.0.0.1:17687",
                           "GNOMON_MEMGRAPH_URI": "bolt://127.0.0.1:17688",
                           "GNOMON_AEONG_URI": "bolt://127.0.0.1:17689",
                           "GNOMON_CH_HTTP_PORT": "18123", "GNOMON_XTDB_PORT": "15433",
                           "EXTCMP_BELIEFQ_INDEXES": "1"})
    per_density = {}
    for d in [float(x) for x in args.densities.split(",")]:
        tag = f"d{d:g}"
        print(f"== density {d:g} %", flush=True)
        nat = w / "stores" / f"native-{tag}"
        H.load_store(nat, "native", base)
        if d > 0:
            # one batch spans at most L/5 ticks of valid time, so the as-of
            # time T can fall inside the edge lifetime around t* (the study's
            # batch of 1000 at 10M spans L/5 at 1 % and L/50 at 10 %)
            batch = args.batch or max(1, int(H.edge_life(args.scale) * d / 500))
            rec = IC.inject(nat, d, 0, 50_000, 0, False, batch=batch)
            print(f"  injected {rec['edges_corrected']} corrections in {rec['commits']} commits "
                  f"({rec['density_pct_actual']:.2f} %)", flush=True)
        # The density point's frozen log: the base log, then the injector's
        # batches (a replayed store's own log holds only what was written
        # through it after the replay, i.e. exactly the injected batches).
        log = w / f"synth-{tag}.eventlog.jsonl"
        with open(log, "w") as out, open(base) as b:
            out.write(b.read())
            with open(nat / "eventlog.jsonl") as inj:
                for line in inj:
                    if '"tt"' in line:
                        out.write(line if line.endswith("\n") else line + "\n")
        data = H.dataset_from_log(log)
        p = B.derive_params(log, args.scale, data.t0, data.t1)
        print(f"  params: t*={p['t_star']} L={p['L']} T_base={p['T_base']} T={p['T']} "
              f"injected_batches={p['injected_batches']} e*={p['e_star']['eid']}", flush=True)
        qs = B.belief_queries(data, p)
        # native vs reference / oracle
        a = gnomon.open(nat, backend="native").adapter
        nodes, edges = list(a.all_node_versions()), list(a.all_edge_versions())
        native = answers_for("native", w, log, nat, tag, qs)
        orc = Oracle(nodes, edges)
        for q in qs:
            ref = B.reference(q, nodes, edges)
            if ref is None and args.scale <= 100_000 and \
                    q.op in ("snapshot_subgraph", "temporal_reachability", "diff_snapshots"):
                ref = getattr(orc, q.op)(validate_args(q.op, dict(q.args)))
            if ref is None:
                continue
            check(native[q.id][0] == X.answer_hash(ref), f"{tag} native == reference: {q.id}")
        # duckdb backend
        ddir = w / "stores" / f"duckdb-{tag}"
        H.load_store(ddir, "duckdb", log)
        duck = answers_for("duckdb", w, log, nat, tag, qs)
        for q in qs:
            if q.op == "diff_belief":
                check(duck[q.id][0].startswith("error") and "unsupported" in duck[q.id][0],
                      f"{tag} duckdb reports b.diff unsupported ({duck[q.id][0][:60]})")
            else:
                check(duck[q.id][0] == native[q.id][0], f"{tag} duckdb == native: {q.id}")
        # ladybug
        if not args.skip_ladybug:
            lb = X._system_class("ladybug")(nat, w, tag)
            lb.load(log)
            lad = answers_for("ladybug", w, log, nat, tag, qs)
            for q in qs:
                check(lad[q.id][0] == native[q.id][0], f"{tag} ladybug == native: {q.id} ({lad[q.id][0][:40]})")
        # raphtory: the current-belief controls only, on the fields it produces
        import extcmp_raphtory as R
        from gnomon.core.model import canonical_json, sha256_hex
        rp = X._system_class("raphtory")(nat, w, tag)
        rp.load(log)
        rap = answers_for("raphtory", w, log, nat, tag, qs)
        for q in qs:
            proj = R.project_native(q.id, native[q.id][1]) if native[q.id][1] else None
            if q.id in ("b.snap.hop2@now", "b.snap.hop2@T"):
                check(rap[q.id][1] is not None and sha256_hex(canonical_json(rap[q.id][1])) ==
                      sha256_hex(canonical_json(proj)), f"{tag} raphtory == native (projection): {q.id}")
            elif q.id in ("b.reach@now", "b.reach@T"):
                check(rap[q.id][1] is not None, f"{tag} raphtory answers {q.id} (verdict: differs by semantics)")
            else:
                check(rap[q.id][0] == "not written", f"{tag} raphtory: {q.id} not expressible")
        for sysname in servers:
            import subprocess
            import time as _t
            stag = f"chk{args.scale}-{tag}"
            sh = str(args.servers_sh)
            subprocess.run([sh, "start", sysname, stag], check=True)
            t0 = _t.time()
            while True:
                try:
                    X._probe(sysname)
                    break
                except Exception:                               # noqa: BLE001
                    if _t.time() - t0 > 900:
                        raise
                    _t.sleep(2)
            try:
                so = X._system_class(sysname)(nat, w, stag)
                so.load(log)
                got = answers_for(sysname, w, log, nat, stag, qs)
                if sysname == "aeong":
                    # the two as-of graph ids: `got` holds the property form
                    # (extcmp_aeong's default, AeonG's answer), checked below
                    # with the other ids. The native TT AS forms return wrong
                    # answers (defects 1 and 2, aeong-tt-undirected-repro/) and
                    # are recorded here, never timed.
                    two = [q for q in qs if q.id in ("b.snap.hop2@T", "b.reach@T")]
                    os.environ["EXTCMP_AEONG_ASOF"] = "tt"
                    ttd = answers_for(sysname, w, log, nat, stag, two)
                    os.environ["EXTCMP_AEONG_ASOF"] = "tt_undirected"
                    und = answers_for(sysname, w, log, nat, stag,
                                      [q for q in two if q.id == "b.snap.hop2@T"])
                    os.environ.pop("EXTCMP_AEONG_ASOF", None)
                    for qid, v in und.items():
                        print(f"  info  {tag} aeong (TT form, single undirected match; defect 1) "
                              f"{'agrees' if v[0] == native[qid][0] else 'DIFFERS'}: {qid}")
                    for qid, v in ttd.items():
                        form = ("two directed matches; defect 2" if qid == "b.snap.hop2@T" else
                                "directed rounds; differs also at density 0, cause not isolated")
                        print(f"  info  {tag} aeong (TT form, {form}) "
                              f"{'agrees' if v[0] == native[qid][0] else 'DIFFERS'}: {qid}")
                for q in [q for q in qs if q.id in got]:
                    if sysname == "xtdb":
                        okk = got[q.id][1] is not None and X.projected_hash(got[q.id][1]) == \
                            X.projected_hash(native[q.id][1])
                        how = "projected"
                    else:
                        okk = got[q.id][0] == native[q.id][0]
                        how = "exact"
                    check(okk, f"{tag} {sysname} == native ({how}): {q.id} ({str(got[q.id][0])[:60]})")
            finally:
                subprocess.run([sh, "stop", sysname, stag])
                subprocess.run([sh, "wipe", sysname, stag])
        per_density[d] = {q.id: native[q.id] for q in qs}
    # design properties (they compare against density 0; a run without it,
    # e.g. one density re-checked on its own, has none to compare)
    ds = sorted(per_density)
    if ds[0] != 0:
        print("\ndesign properties need density 0 in the run; not checked")
        print(f"\n{len(FAIL)} failure(s)")
        return 1 if FAIL else 0
    z = per_density[ds[0]]
    check(z["b.snap.hop2@T"][0] == z["b.snap.hop2@now"][0], "density 0: b.snap.hop2@T == control")
    check(z["b.reach@T"][0] == z["b.reach@now"][0], "density 0: b.reach@T == control")
    check(z["b.diff"][1]["added_total"] + z["b.diff"][1]["retracted_total"] == 0, "density 0: b.diff empty")
    check(z["b.hist"][1]["rows_total"] == 1, "density 0: e* has one version")
    for d in ds[1:]:
        a_ = per_density[d]
        check(a_["b.snap.hop2@T"][0] != z["b.snap.hop2@T"][0], f"{d:g} %: b.snap.hop2@T differs from density 0")
        check(a_["b.snap.hop2@T"][0] != a_["b.snap.hop2@now"][0], f"{d:g} %: b.snap.hop2@T differs from current")
        check(a_["b.reach@T"][0] == z["b.reach@T"][0], f"{d:g} %: b.reach@T equals density 0 (cost probe)")
        check(a_["b.hist"][1]["rows_total"] >= 2, f"{d:g} %: e* has >= 2 versions")
    sizes = [per_density[d]["b.diff"][1]["added_total"] for d in ds]
    check(all(x < y for x, y in zip(sizes, sizes[1:])), f"b.diff added_total grows with density: {sizes}")
    if len(ds) >= 3:
        check(per_density[ds[1]]["b.snap.hop2@T"][0] != per_density[ds[2]]["b.snap.hop2@T"][0],
              "b.snap.hop2@T differs between the two non-zero densities")
    (w / "summary.json").write_text(json.dumps(
        {"failures": FAIL, "diff_added": sizes,
         "hist_versions": [per_density[d]["b.hist"][1]["rows_total"] for d in ds]}, indent=1))
    print(f"\n{len(FAIL)} failure(s)")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
