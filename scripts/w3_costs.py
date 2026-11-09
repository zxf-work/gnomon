"""W3 cost model v2: node-hours and SU per timed stage, for a design that is passed in.

    w3_costs.py --pilot-sf1 SF1_CHECKDIR --pilot-small SF0.1_CHECKDIR --loads loads.json [--design design.json]
                [--json out.json]

**Per call.** t(system, query, setting, SF) is a per-call median from the answer-check pilots. These are single
calls, not the protocol.
- DuckDB: its SF1 pilot.
- Gnomon: 2 x DuckDB's SF1 per-call time. This is an *assumption*: the timed stages run on the engine with S23
  fixed, and the preview engine's pilot (S23 floor of about 5 s per call) is not used.
- Other systems: their SF0.1 pilot x a growth factor to SF1. SQL systems use DuckDB's measured SF0.1 -> SF1 growth
  for that query and setting, clamped to [1, 20]. Graph systems use 10x, a data-linear guess.
- A setting a system's pilot did not run: its current-setting time x DuckDB's SF1 ratio for that query and setting.
- SF10: the SF1 time x `sf10_factor` (12 by default; 10.8x the elements plus superlinear growth).

**Per instance and point** (protocol, ceiling C = 10 s):
- t <= 1 s: 1 warm-up + 5 timed calls.
- 1 s < t <= C: 1 warm-up + 3 timed calls.
- t > C: censored, recorded as "> C". It costs the warm-up plus one call, each aborted at C.
- On top of that, 1 priming call per (system, query, setting, point), capped at C.
- query node-seconds = sum over queries and settings of [priming + n x per-instance] x rotations.

**Loads: once per (system, log), then reused.** Each (system, log) pair is loaded once on an exclusive node,
which is also its W3-L record. The load is the bulk snapshot load, then the daily and correction transactions
through the normal write path. The loaded store or database directory is persisted to $PROJECT. Every later
stage and rotation that reads that log copies it to $LOCAL (`copy_gbps`, size from `loads.json`) and does not
load again.

Load times come from `loads.json`, seconds per system per SF, each with its basis (measured or estimated). Where
only SF1 is given, SF10 = 11 x SF1.

**Rotations.** Rotation 2 re-runs every query series after a server restart (`restart_s`), on the store loaded
once.

**Untimed builds.** A stage with `timed_load: false` builds the Gnomon store and the DuckDB file on a shared node
(`build_cores`), and records no W3-L load time for those points. Server systems always load on the timed node.

**SU.** Node-hours x 128 (an exclusive RM node), x 1.15 for scheduling, server start and stop, and sync. An
a shared node step is charged at cores x hours.
"""
from __future__ import annotations

import argparse
import collections
import json
import statistics
from pathlib import Path

NODE = 128
OVER = 1.15
SQL = {"duckdb", "postgres", "clickhouse"}
GFACT = [2.0]     # Gnomon per-call = GFACT x DuckDB (design key "gnomon_factor")

#: the default v2 design; every key is a lever
DESIGN = {
    "ceiling_s": 10.0,
    "rotations": 2, "restart_s": 300,       # rotation 2 re-runs the queries on the loaded store after a restart
    "fast_s": 1.0, "reps_fast": 5, "reps_slow": 3, "warmups": 1, "priming": 1,
    "sf10_factor": 12.0,
    "export_s": 0.0, "export_cores": 16,     # v1 loader only (history tables from the Gnomon store)
    "copy_gbps": 0.5,
    "build_cores": 16,                      # an untimed store build (no W3-L record) runs on a shared node
    "q_sf1": {"systems": ["gnomon", "duckdb", "postgres", "neo4j", "memgraph", "aeong", "aeong-prop"],
              "settings": ["current", "asof", "window"], "n": 20, "logs": 1},
    "q_sf10": {"systems": ["gnomon", "duckdb", "postgres", "memgraph"],
               "settings": ["current", "asof", "window"], "n": 8, "logs": 1},
    # the sweeps: a representative subset on 3 systems, at the points beyond the Q stage's (rho 1 %, lag mix, h 1x)
    "subset": ["IS1", "IS2", "IC2", "IC6", "IC9", "IC13", "BI2", "BI8", "BI14", "BIH-VER", "BIH-R3", "TLDBC-IS3"],
    "d_sf1": {"points": 4, "settings": ["asof", "window"], "n": 10,       # rho 0, 0.02, 0.3, 10 %
              "systems": ["gnomon", "duckdb", "memgraph"], "timed_load": True},
    "g_sf1": {"points": 3, "settings": ["asof", "window"], "n": 10,       # lag same, months, years
              "systems": ["gnomon", "duckdb", "memgraph"], "timed_load": True},
    "h_sf1": {"points": 4, "settings": ["current", "asof"], "n": 10,      # (3x, max) x (rho 0, 1 %)
              "hmult": [3.0, 3.0, 5.26, 5.26], "systems": ["gnomon", "duckdb", "memgraph"], "timed_load": True},
}


def pilot(d: Path, only=None) -> dict:
    out = collections.defaultdict(dict)
    for f in d.glob("*.jsonl"):
        if any(x in f.name for x in ("degen", "rerun")) or (only and f.stem not in only):
            continue
        by = collections.defaultdict(list)
        for line in open(f):
            x = json.loads(line)
            if x.get("ok"):
                by[(x["qid"], x["setting"])].append(x["ms"] / 1e3)
        for k, v in by.items():
            out[f.stem][k] = statistics.median(v)
    return out


def per_call_sf1(p1: dict, p01: dict) -> dict:
    est: dict = {}
    duck1, duck01 = p1.get("duckdb", {}), p01.get("duckdb", {})
    est["duckdb"] = dict(duck1)
    est["gnomon"] = {k: GFACT[0] * v for k, v in duck1.items()}   # assumption: S23 fixed, Gnomon ~ 2x DuckDB
    for s, cells in p01.items():
        if s in ("duckdb", "gnomon"):
            continue
        est[s] = {}
        for k, v in cells.items():
            if s in SQL:
                g = duck1.get(k, 0) / max(duck01.get(k, 0), 1e-6) if k in duck01 else 10.0
                g = min(max(g, 1.0), 20.0)
            else:
                g = 10.0
            est[s][k] = v * g
        # a setting the pilot did not run: the current-setting time x DuckDB's ratio for that query and setting
        for (q, st0), v in list(est[s].items()):
            if st0 != "current":
                continue
            for st in ("asof", "window"):
                if (q, st) not in est[s] and (q, st) in duck1 and duck1.get((q, "current")):
                    est[s][(q, st)] = v * duck1[(q, st)] / duck1[(q, "current")]
    return est


def inst_cost(t: float, D: dict) -> float:
    c = D["ceiling_s"]
    if t > c:
        return (D["warmups"] + 1) * c
    reps = D["reps_fast"] if t <= D["fast_s"] else D["reps_slow"]
    return (D["warmups"] + reps) * t


def query_s(cells: dict, settings, n: int, D: dict, scale: float = 1.0, only=None, rot=None) -> tuple[float, int, int]:
    tot, cens, ncell = 0.0, 0, 0
    for (q, st), t in cells.items():
        if st not in settings or (only is not None and q not in only):
            continue
        t = t * scale
        ncell += 1
        cens += t > D["ceiling_s"]
        tot += D["priming"] * min(t, D["ceiling_s"]) + n * inst_cost(t, D)
    return tot * (rot or D["rotations"]), cens, ncell


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--pilot-sf1", required=True)
    ap.add_argument("--pilot-small", required=True)
    ap.add_argument("--loads", required=True)
    ap.add_argument("--design")
    ap.add_argument("--json")
    a = ap.parse_args()
    D = json.loads(json.dumps(DESIGN))
    if a.design:
        D.update(json.loads(Path(a.design).read_text()))
    GFACT[0] = D.get("gnomon_factor", 2.0)
    p1, p01 = pilot(Path(a.pilot_sf1), {"duckdb"}), pilot(Path(a.pilot_small))
    t1 = per_call_sf1(p1, p01)
    L = json.loads(Path(a.loads).read_text())

    def load_s(s: str, sf: str) -> float:
        x = L.get(s, {})
        if sf == "sf1":
            return x.get("sf1", 10 * x.get("sf0.1", 0))
        return x.get("sf10", 11 * load_s(s, "sf1"))

    def size_gb(s: str, sf: str) -> float:
        g = L.get(s, {}).get("gb_sf1", 2.0)
        return g if sf == "sf1" else 11 * g

    rows = []

    def stage(name, sf, spec, systems, only=None, hmult=None):
        pts = spec.get("points", spec.get("logs", 1))
        scale = 1.0 if sf == "sf1" else D["sf10_factor"]
        for s in systems:
            if s not in t1:
                continue
            rot = spec.get("rotations", D["rotations"])
            n = spec.get("n_by_system", {}).get(s, spec["n"])
            qs, cens, ncell = query_s(t1[s], spec["settings"], n, D, scale, only, rot)
            mult = sum(hmult) if hmult else pts
            timed = spec.get("timed_load", True) or s not in ("gnomon", "duckdb")   # servers load on the node
            ld = load_s(s, sf) * pts
            restart = (rot - 1) * D["restart_s"] * pts
            copy = size_gb(s, sf) / D["copy_gbps"] * pts
            rows.append({"stage": name, "sf": sf, "system": s, "points": pts, "n": n, "cells": ncell, "censored": cens,
                         "query_node_h": round((qs * mult + restart + copy) / 3600, 2),
                         "load_node_h": round(ld / 3600, 2) if timed else 0.0,
                         "build_su": 0 if timed else round(ld / 3600 * D["build_cores"])})

    stage("Q", "sf1", D["q_sf1"], D["q_sf1"]["systems"])
    if D["q_sf10"]["systems"]:
        stage("Q", "sf10", D["q_sf10"], D["q_sf10"]["systems"])
    sub = set(D["subset"])
    stage("D", "sf1", D["d_sf1"], D["d_sf1"]["systems"], sub)
    stage("G", "sf1", D["g_sf1"], D["g_sf1"]["systems"], sub)
    stage("H", "sf1", D["h_sf1"], D["h_sf1"]["systems"], sub, D["h_sf1"]["hmult"])
    logs = {"sf1": D["q_sf1"]["logs"] + D["d_sf1"]["points"] + D["g_sf1"]["points"] + D["h_sf1"]["points"],
            "sf10": D["q_sf10"]["logs"] if D["q_sf10"]["systems"] else 0}
    export_su = (logs["sf1"] * D["export_s"] + logs["sf10"] * 11 * D["export_s"]) * D["export_cores"] / 3600
    node_h = sum(r["query_node_h"] + r["load_node_h"] for r in rows)
    build_su = sum(r["build_su"] for r in rows)
    by_stage = collections.defaultdict(float)
    for r in rows:
        by_stage[f"{r['stage']}-{r['sf']}"] += (r["query_node_h"] + r["load_node_h"]) * NODE * OVER
    rec = {"design": D, "rows": rows, "logs": logs, "export_su": round(export_su), "build_su": build_su,
           "node_hours": round(node_h, 1), "su": round(node_h * NODE * OVER + export_su + build_su),
           "su_by_stage": {k: round(v) for k, v in by_stage.items()},
           "per_call_sf1_median_s": {s: round(statistics.median(v.values()), 3) for s, v in t1.items() if v},
           "loads_s": {s: {"sf1": round(load_s(s, "sf1")), "sf10": round(load_s(s, "sf10"))} for s in t1}}
    print(json.dumps(rec, indent=1))
    if a.json:
        Path(a.json).write_text(json.dumps(rec, indent=1) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
