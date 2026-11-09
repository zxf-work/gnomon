"""W5: knowledge-graph `match` patterns on a frozen Wikidata log, checked native vs reference.

    wd_match.py emit --factors FACTORS.json --log LOG --out PATTERNS.json
    wd_match.py run  --patterns PATTERNS.json --store NATIVE_STORE --json OUT.jsonl

Patterns (relation types by frequency in the final believed graph, objects by in-degree):
  holders   "who held <rel> -> X at valid instant t, as believed at T"  (P39 first when present)
  path2     typed 2-hop path  x -r1-> y -r2-> z  (the most frequent composable pair), from a seed
  path3     typed 3-hop path  (the most frequent composable triple), counted
  star      a centre with two typed out-edges r1 and r2, grouped by the r1 object
Each at (instant y50, now), (instant y50, as_of T q50) and, for holders, (instant y75, T q90).
The SQL/Cypher baselines have no `match` translation in this kit, so
the answer check here is the operator's two executors against each other (native vs reference).
"""
from __future__ import annotations

import argparse
import collections
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import match_corpus as MC  # noqa: E402
from gnomon.core.model import OPEN_END  # noqa: E402

YEAR = int(365.2425 * 86_400_000_000)


def final_edges(log: Path):
    cur = {}
    with open(log) as f:
        f.readline()
        for line in f:
            r = json.loads(line)
            for op in r["ops"]:
                if op["op"] == "ingest_events":
                    for e in op["events"]:
                        cur[(e["src"], e["dst"], e["rel_type"], e["disc"])] = (e["vt_s"], e["vt_e"])
                elif op["op"] == "assert_edge":
                    cur[(op["src"], op["dst"], op["rel_type"], op["disc"])] = (op["vt_s"], op["vt_e"])
                elif op["op"] == "retract":
                    rf = op["ref"]; k = (rf["src"], rf["dst"], rf["rel_type"], rf["disc"])
                    if k in cur:
                        if op["t"] <= cur[k][0]:
                            del cur[k]
                        else:
                            cur[k] = (cur[k][0], op["t"])
    return cur


def cmd_emit(a) -> int:
    F = json.loads(Path(a.factors).read_text())
    cur = final_edges(Path(a.log))
    rel = collections.Counter(k[2] for k in cur)
    obj_in = collections.Counter((k[2], k[1]) for k in cur)
    out_rel = collections.defaultdict(collections.Counter)      # node -> rel -> count
    for (s, d, r, _) in cur:
        out_rel[s][r] += 1
    pair = collections.Counter(); tri = collections.Counter()
    by_src = collections.defaultdict(list)
    for (s, d, r, _) in cur:
        by_src[s].append((r, d))
    for (s, d, r1, _) in cur:
        for r2, n in out_rel.get(d, {}).items():
            pair[(r1, r2)] += n
    (r1, r2), _ = pair.most_common(1)[0]
    for (s, d, ra, _) in cur:
        if ra != r1:
            continue
        for rb, e in by_src.get(d, ()):
            if rb == r2:
                for rc, n in out_rel.get(e, {}).items():
                    tri[(ra, rb, rc)] += n
    t3 = tri.most_common(1)[0][0] if tri else (r1, r2, r2)
    star = collections.Counter()
    for s, rs in out_rel.items():
        ks = sorted(rs)
        for i in range(len(ks)):
            for j in range(i + 1, len(ks)):
                star[(ks[i], ks[j])] += 1
    s1, s2 = star.most_common(1)[0][0]
    hold_rels = (["P39"] if "P39" in rel else []) + [r for r, _ in rel.most_common(3) if r != "P39"][:2]
    y = {k: int(v * YEAR) for k, v in ((k, v) for k, v in F["years"].items())}
    T = F["T_points"]
    scopes = {"now@y50": {"vt": {"instant": y["y50"]}},
              "Tq50@y50": {"vt": {"instant": y["y50"]}, "as_of_tt": T.get("q50", OPEN_END - 1)},
              "Tq90@y75": {"vt": {"instant": y["y75"]}, "as_of_tt": T.get("q90", OPEN_END - 1)}}
    N, S, R, ORD, col, M = MC.N, MC.S, MC.R, MC.ORD, MC.col, MC.M
    pats = []
    for r in hold_rels:
        x = max(((o, n) for (rr, o), n in obj_in.items() if rr == r), key=lambda t: (t[1], t[0]))[0]
        for sn, sc in scopes.items():
            pats.append({"id": f"holders.{r}.{sn}", "class": f"holders {r} -> Q{x}", "args": M(
                [N("p", uids=None), N("x", uids=[x])], [S("p", "x", r, var="e")],
                {"rows": R(holder=col("p.uid"), start=col("e.vt_s"), end=col("e.vt_e")),
                 "order_by": ORD((col("holder"), "asc"), (col("start"), "asc"))}, scope=sc)})
    seed = max(by_src, key=lambda s: (sum(1 for rr, _ in by_src[s] if rr == r1), s))
    for sn, sc in list(scopes.items())[:2]:
        pats.append({"id": f"path2.{r1}.{r2}.{sn}", "class": f"path {r1}/{r2} from Q{seed}", "args": M(
            [N("x", uids=[seed]), N("y"), N("z")], [S("x", "y", r1), S("y", "z", r2)],
            {"rows": R(y=col("y.uid"), z=col("z.uid")), "order_by": ORD((col("y"), "asc"), (col("z"), "asc")),
             "limit": 1000}, scope=sc)})
        pats.append({"id": f"path3.{'.'.join(t3)}.{sn}", "class": f"path {'/'.join(t3)} counted", "args": M(
            [N("x"), N("y"), N("z"), N("w")], [S("x", "y", t3[0]), S("y", "z", t3[1]), S("z", "w", t3[2])],
            {"group": {"by": [], "aggs": [{"as": "n", "fn": "count"}]}}, scope=sc)})
        pats.append({"id": f"star.{s1}.{s2}.{sn}", "class": f"star {s1}+{s2}", "args": M(
            [N("c"), N("a"), N("b")], [S("c", "a", s1), S("c", "b", s2)],
            {"group": {"by": [{"as": "a", "expr": col("a.uid")}], "aggs": [{"as": "n", "fn": "count"}]},
             "order_by": ORD((col("n"), "desc"), (col("a"), "asc")), "limit": 100}, scope=sc)})
    Path(a.out).write_text(json.dumps({"patterns": pats, "choice": {"hold_rels": hold_rels, "pair": [r1, r2],
                                       "triple": list(t3), "star": [s1, s2]}}, indent=1) + "\n")
    print(len(pats), "patterns", {"pair": (r1, r2), "triple": t3, "star": (s1, s2), "holders": hold_rels})
    return 0


def cmd_run(a) -> int:
    import gnomon
    from gnomon.temporal.algebra import ensure_all_registered
    from gnomon.temporal.ops_match import match
    from extcmp import answer_hash
    ensure_all_registered()
    doc = json.loads(Path(a.patterns).read_text())
    s = gnomon.open(a.store, backend="native", read_only=True)
    out = open(a.json, "w")
    agree = 0
    for p in doc["patterns"]:
        rec = {"id": p["id"], "class": p["class"]}
        for route in ("native", "reference"):
            t = time.time()
            try:
                args = dict(p["args"]); args.setdefault("limit", 10000)
                res = match(s.adapter, args, route=route, reference_bypass=None if route == "native" else "w5 answer check")
                rec[route] = {"hash": answer_hash(res), "rows": len(res.get("rows", [])), "s": round(time.time() - t, 3)}
            except Exception as e:
                rec[route] = {"error": f"{type(e).__name__}: {str(e)[:300]}", "s": round(time.time() - t, 3)}
        rec["agree"] = rec["native"].get("hash") is not None and rec["native"].get("hash") == rec["reference"].get("hash")
        agree += rec["agree"]
        out.write(json.dumps(rec) + "\n"); print(json.dumps(rec))
    print(f"agree {agree}/{len(doc['patterns'])}")
    s.close()
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    e = sub.add_parser("emit"); e.add_argument("--factors", required=True); e.add_argument("--log", required=True); e.add_argument("--out", required=True)
    r = sub.add_parser("run"); r.add_argument("--patterns", required=True); r.add_argument("--store", required=True); r.add_argument("--json", required=True)
    a = ap.parse_args()
    return cmd_emit(a) if a.cmd == "emit" else cmd_run(a)


if __name__ == "__main__":
    sys.exit(main())
