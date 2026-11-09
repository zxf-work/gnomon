#!/usr/bin/env python3
"""fsx_check.py: the whole-answer closure (coverage refresh row 3), untimed, no ratios. One store point per call.

  gnomon  --closure C --tag T --root R --ref NATIVE --out D --per S --until EPOCH
  system  --system duckdbsql|postgres --worker N (same arguments)
  compare --closure C --out D --cells cells.jsonl --diff differences.txt

Gnomon's whole answer:
    matchpath stage's texts (two `match` calls, admission on, as shipped);
  * temporal_paths (paths.k): the engine's own paths search with k = the recorded total: ops_paths._engine_paths, the function the
    operator calls, given k above the operator's validator cap (20); if the total it returns is larger, called again with k = that total;
  * any other operator (reach.window): the dedicated operator paged to its end, as fs_check.gnomon reads it.
Beside it, the dedicated call exactly as the fullsets stage read it (fs_check.gnomon). Each list the dedicated call returns whole must
equal the whole answer as a multiset (and its order is recorded); each list it returns in part must equal the whole answer's first rows
in order; every total must be equal. Anything else is a difference ("dedicated_vs_whole").
The other systems: fs_check.other's call with k = the recorded total for temporal_paths (limit 25,000,000 otherwise). Workers take
instances from a shared list, largest recorded total first unless --order asc (an O_EXCL claim file per instance); each worker
process has its own TMPDIR (DuckDB spill directory), set by the job.
Comparison: fs_check.compare, unchanged (multisets under the answer hash's projection and exactly, the first 100 rows in order, every
total). A difference is written with up to 20 example rows per side. Per-instance limit --per seconds on every system."""
import argparse
import json
import os
import pickle
import signal
import sys
import threading
import time
from collections import Counter
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent.parent / "fullsets" / "tools"))
import fs_check as FS  # noqa: E402
import extcmp as X  # noqa: E402
import matchpath_texts as MT  # noqa: E402
from gnomon.core.model import OPEN_END  # noqa: E402
from gnomon.temporal import ops_paths as OPP  # noqa: E402


class Over(Exception):
    pass


def _alarm(*_):
    raise Over()


def kpaths(c):
    return max(int(c["recorded_total"]), 1)


def gnomon_whole(S, c):
    op = FS.OP[c["query"]]
    ad = S.adapter
    if op == "snapshot_subgraph":
        return "match: matchpath_texts.snap whole", MT.snap(ad, dict(c["args"]), whole=True)
    if op == "temporal_paths":
        a = {"k": 5, "max_hops": 4, "as_of_tt": OPEN_END, **c["args"]}
        a["k"] = kpaths(c)
        sid, did = (int(x) for x in ad.dense_ids([a["src"], a["dst"]]))
        for _ in range(2):
            p = OPP._engine_paths(ad, sid, did, a["window"]["t_a"], a["window"]["t_b"], a["as_of_tt"], a)
            if p is None:
                raise RuntimeError("the engine paths search is not available on this handle")
            if p["rows_total"] <= a["k"]:
                break
            a["k"] = int(p["rows_total"])
        return f"engine paths search, k = {a['k']}", p
    return "dedicated operator, paged", FS.gnomon(S, c)


def digs(lst):
    return [FS.dig(X._project(r)) for r in lst]


def ded_vs_whole(ded, whole):
    out = {"lists": {}, "totals_differ": {}, "differs": False}
    for k in FS.SCALARS:
        if k in ded and k in whole and ded[k] != whole[k]:
            out["totals_differ"][k] = [ded[k], whole[k]]
            out["differs"] = True
    for f in FS.LISTS:
        if not isinstance(ded.get(f), list):
            continue
        w = whole.get(f)
        if not isinstance(w, list):
            out["lists"][f] = "absent from the whole answer"
            out["differs"] = True
            continue
        a, b = digs(ded[f]), digs(w)
        t = FS.TOTALS.get(f)
        ded_whole = (ded.get(t, len(a)) if t else len(a)) == len(a) and not (f == "rows" and ded.get("cursor"))
        if ded_whole:
            same = Counter(a) == Counter(b)
            info = {"dedicated": len(a), "whole": len(b), "dedicated_whole": True, "multiset_equal": same, "order_equal": a == b}
        else:
            same = a == b[:len(a)]
            info = {"dedicated": len(a), "whole": len(b), "dedicated_whole": False, "prefix_equal_in_order": same}
        out["lists"][f] = info
        out["differs"] |= not same
    return out


def dump(path, obj):
    tmp = Path(str(path) + ".tmp")
    with open(tmp, "wb") as fh:
        pickle.dump(obj, fh, protocol=5)
    tmp.rename(path)


def wjson(path, obj):
    tmp = Path(str(path) + ".tmp")
    tmp.write_text(json.dumps(obj, default=str) + "\n")
    tmp.rename(path)


def open_sys(name, a):
    X.ensure_all_registered()
    o = X._system_class(name)(Path(a.ref), Path(a.root), a.tag)
    o.open()
    return o


def run_gnomon(a, cells):
    d = Path(a.out) / "gnomon"
    d.mkdir(parents=True, exist_ok=True)
    S = open_sys("native", a)
    signal.signal(signal.SIGALRM, _alarm)
    for c in sorted(cells, key=lambda c: c["recorded_total"]):
        rec = {"key": c["key"], "query": c["query"]}
        per = min(a.per, a.until - time.time())
        if per < 30:
            rec["state"] = "not reached"
            wjson(d / f"{c['key']}.json", rec)
            continue
        t0 = time.time()
        try:
            signal.alarm(int(per))
            ded = FS.gnomon(S, c)
            t1 = time.time()
            method, whole = gnomon_whole(S, c)
            signal.alarm(0)
            rec.update(state="returned", method=method, dedicated_s=round(t1 - t0, 2), whole_s=round(time.time() - t1, 2),
                       totals={k: whole.get(k) for k in FS.SCALARS if k in whole},
                       lists={f: len(whole[f]) for f in FS.LISTS if isinstance(whole.get(f), list)},
                       dedicated_vs_whole=ded_vs_whole(ded, whole))
            dump(d / f"{c['key']}.pkl", whole)
        except Over:
            rec["state"] = "not checked: Gnomon over the limit"
        except Exception as e:  # noqa: BLE001
            signal.alarm(0)
            rec.update(state="not checked: Gnomon refused or failed", error=f"{type(e).__name__}: {e}"[:300])
        rec["s"] = round(time.time() - t0, 2)
        rec["limit_s"] = int(per)
        wjson(d / f"{c['key']}.json", rec)
        print(c["key"], rec["state"], rec["s"], flush=True)


def run_system(a, cells):
    d = Path(a.out) / a.system
    d.mkdir(parents=True, exist_ok=True)
    S = open_sys(a.system, a)
    signal.signal(signal.SIGALRM, _alarm)
    for c in sorted(cells, key=lambda c: c["recorded_total"] * (1 if a.order == "asc" else -1)):
        if a.system not in c["need"]:
            continue
        try:
            os.close(os.open(d / f"{c['key']}.claim", os.O_CREAT | os.O_EXCL | os.O_WRONLY))
        except FileExistsError:
            continue
        rec = {"key": c["key"], "system": a.system, "worker": a.worker}
        per = min(a.per_sys.get(a.system, a.per), a.until - time.time())
        if per < 30:
            rec["state"] = "not reached"
            wjson(d / f"{c['key']}.json", rec)
            continue
        op = FS.OP[c["query"]]
        extra = {"k": kpaths(c)} if op == "temporal_paths" else {"limit": FS.LIM}
        t0 = time.time()
        tim = threading.Timer(per, S.cancel) if a.system == "duckdbsql" else None
        try:
            try:
                if a.system == "postgres":
                    S.handle.execute(f"SET statement_timeout = '{int(per)}s'")
                if tim:
                    tim.start()
                signal.alarm(int(per) + 120)     # the systems' own limits act at per; the alarm is the backstop
                p = S.fn_for(FS.q_of(c, extra))()
                signal.alarm(0)
                rec.update(state="returned", totals={k: p.get(k) for k in FS.SCALARS if k in p},
                           lists={f: len(p[f]) for f in FS.LISTS if isinstance(p.get(f), list)})
                dump(d / f"{c['key']}.pkl", p)
            except Exception as e:  # noqa: BLE001  (Over included)
                signal.alarm(0)
                msg = f"{type(e).__name__}: {e}"[:300]
                over = isinstance(e, Over) or "nterrupt" in msg or "timeout" in msg or "cancel" in msg
                rec.update(state="not checked: over the limit" if over else "not checked: failed", error=msg)
        except Over:      # the backstop alarm landing inside the handler above
            rec.update(state="not checked: over the limit", error="alarm")
        finally:
            signal.alarm(0)
            if tim:
                tim.cancel()
        if a.system == "postgres" and rec["state"] != "returned":
            try:
                S.close()
            except Exception:  # noqa: BLE001
                pass
            S.open()
        rec["s"] = round(time.time() - t0, 2)
        rec["limit_s"] = int(per)
        wjson(d / f"{c['key']}.json", rec)
        print(c["key"], rec["state"], rec["s"], flush=True)


def examples(c, g, s):
    out = {}
    for f in FS.LISTS:
        if not (isinstance(g.get(f), list) and isinstance(s.get(f), list)):
            continue
        ga, sa = Counter(digs(g[f])), Counter(digs(s[f]))
        og, os_ = ga - sa, sa - ga
        if og or os_:
            out[f] = {"only_gnomon": [r for r in g[f] if FS.dig(X._project(r)) in og][:20],
                      "only_system": [r for r in s[f] if FS.dig(X._project(r)) in os_][:20]}
    return out


def run_compare(a, cells):
    D = Path(a.out)
    cf, df = open(a.cells, "a"), open(a.diff, "a")
    for c in cells:
        rec = {k: c[k] for k in ("point", "query", "instance", "key", "recorded_total", "need", "why", "source")}
        gj = D / "gnomon" / f"{c['key']}.json"
        gr = json.loads(gj.read_text()) if gj.exists() else {"state": "not reached"}
        rec["gnomon"] = gr
        rec["systems"] = {}
        g = pickle.load(open(D / "gnomon" / f"{c['key']}.pkl", "rb")) if gr.get("state") == "returned" else None
        if gr.get("dedicated_vs_whole", {}).get("differs"):
            df.write(json.dumps({"point": c["point"], "key": c["key"], "system": "gnomon dedicated vs whole", "detail": gr["dedicated_vs_whole"]}) + "\n")
        for name in c["need"]:
            sj = D / name / f"{c['key']}.json"
            sr = json.loads(sj.read_text()) if sj.exists() else {"state": "not reached"}
            if g is None or sr.get("state") != "returned":
                sr["compared"] = False
                rec["systems"][name] = sr
                continue
            s = pickle.load(open(D / name / f"{c['key']}.pkl", "rb"))
            r = FS.compare(c, g, s)
            bad = r["only_system"] or r["only_gnomon"] or r["first_page_order_differs"] or r["totals_differ"]
            r.update(compared=True, equal=bool(not bad and not r["prefix_only_fields"]), s=sr.get("s"), sys_totals=sr.get("totals"))
            if bad:
                df.write(json.dumps({"point": c["point"], "key": c["key"], "system": name, "only_system": r["only_system"],
                                     "only_gnomon": r["only_gnomon"], "first_page_order_differs": r["first_page_order_differs"],
                                     "totals_differ": r["totals_differ"], "examples": examples(c, g, s)}, default=str) + "\n")
                df.flush()
            rec["systems"][name] = r
        cf.write(json.dumps(rec, default=str) + "\n")
        cf.flush()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=("gnomon", "system", "compare"))
    ap.add_argument("--closure", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--tag")
    ap.add_argument("--root")
    ap.add_argument("--ref")
    ap.add_argument("--per", type=float, default=1800)
    ap.add_argument("--until", type=float, default=0)
    ap.add_argument("--system")
    ap.add_argument("--worker", type=int, default=0)
    ap.add_argument("--only", default="", help="comma-separated keys (smoke)")
    ap.add_argument("--per-sys", default="", help="system=seconds,... overriding --per for that system")
    ap.add_argument("--order", choices=("asc", "desc"), default="desc", help="system workers: recorded total ascending or descending")
    ap.add_argument("--cells")
    ap.add_argument("--diff")
    a = ap.parse_args()
    a.per_sys = {k: float(v) for k, v in (x.split("=") for x in a.per_sys.split(",") if x)}
    cells = json.load(open(a.closure))["cells"]
    if a.only:
        keep = set(a.only.split(","))
        cells = [c for c in cells if c["key"] in keep]
    if not a.until:
        a.until = time.time() + 10 ** 7
    {"gnomon": run_gnomon, "system": run_system, "compare": run_compare}[a.mode](a, cells)


if __name__ == "__main__":
    main()
