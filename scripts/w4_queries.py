"""W4 (FinBench-derived) query texts for every system.

**FinBench-derived queries, not LDBC FinBench queries or results.** One entry
per read: TCR1-TCR12, TSR1-TSR6 and the read parts of TRW1-TRW3. Each has

* `RESULT[q]`: the result columns (specification order) and sort keys;
* `gnomon(q, p)`: the Gnomon calls (operator, args) - `match` from the merged
* `SQL[q]`: one text for DuckDB (SQL) and PostgreSQL over the exported
  four-timestamp tables `node_v`, `edge_v` (w4_check.py export);
* `CYPHER[q]`: one text for Neo4j and Memgraph over the exported graph
  (entity nodes with FinBench labels, `AState` version nodes for accounts,
  one relationship per edge version).

Semantics shared by every system:

* every read is at valid instant `now` (after the last write) under a belief
  time `T` (current belief: T = 2^62 - 1; historic belief: a commit time of
  the replayed stream). An edge or node version takes part iff
  `vt_s <= now < vt_e` and `tt_s <= T < tt_e`;
* windows are open on both ends: `start < ts < end` on an edge's valid start;
* truncation (`lim`): per node and step, the `lim` most recent window-
  qualifying edges of the step's relation and direction (ties: other endpoint
  uid, disc);
* time-ordered multi-hop steps: strictly increasing edge starts along the path;
* the result is canonicalised in Python (floats to three decimals, half up;
  rows sorted by the specification's keys, then by the whole row), so the
  texts below need no ORDER BY.

Parameters (FinBench milliseconds converted to microseconds): id, id1, id2,
pid1, pid2, start, end, threshold, threshold1, threshold2, lim, T, now.
"""
from __future__ import annotations

import math
import re
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))
import match_corpus as MC  # noqa: E402
from match_corpus import (AND, IF, N, ORD, OR, R, S, agg, arith, by, cmp, col, lit,  # noqa: E402,F401
                          ratio, round3, tp)

CURRENT = 2 ** 62 - 1
TRUNC_ORDER_DESC = "TIMESTAMP_DESCENDING"

RESULT: dict[str, dict[str, Any]] = {
    "TCR1": {"cols": ["otherId", "accountDistance", "mediumId", "mediumType"],
             "sort": [("accountDistance", 1), ("otherId", 1), ("mediumId", 1)]},
    "TCR2": {"cols": ["otherId", "sumLoanAmount", "sumLoanBalance"],
             "sort": [("sumLoanAmount", -1), ("otherId", 1)]},
    "TCR3": {"cols": ["shortestPathLength"], "sort": []},
    "TCR4": {"cols": ["otherId", "numEdge2", "sumEdge2Amount", "maxEdge2Amount",
                      "numEdge3", "sumEdge3Amount", "maxEdge3Amount"],
             "sort": [("sumEdge2Amount", -1), ("sumEdge3Amount", -1), ("otherId", 1)]},
    "TCR5": {"cols": ["path"], "sort": [("pathLength", -1), ("path", 1)]},
    "TCR6": {"cols": ["midId", "sumEdge1Amount", "sumEdge2Amount"],
             "sort": [("sumEdge2Amount", -1), ("midId", 1)]},
    "TCR7": {"cols": ["numSrc", "numDst", "inOutRatio"], "sort": []},
    "TCR8": {"cols": ["dstId", "ratio", "minDistanceFromLoan"],
             "sort": [("minDistanceFromLoan", -1), ("ratio", -1), ("dstId", 1)]},
    "TCR9": {"cols": ["ratioRepay", "ratioDeposit", "ratioTransfer"], "sort": []},
    "TCR10": {"cols": ["jaccardSimilarity"], "sort": []},
    "TCR11": {"cols": ["sumLoanAmount", "numLoans"], "sort": []},
    "TCR12": {"cols": ["compAccountId", "sumEdge2Amount"],
              "sort": [("sumEdge2Amount", -1), ("compAccountId", 1)]},
    "TSR1": {"cols": ["createTime", "isBlocked", "type"], "sort": []},
    "TSR2": {"cols": ["sumEdge1Amount", "maxEdge1Amount", "numEdge1",
                      "sumEdge2Amount", "maxEdge2Amount", "numEdge2"], "sort": []},
    "TSR3": {"cols": ["blockRatio"], "sort": []},
    "TSR4": {"cols": ["dstId", "numEdges", "sumAmount"], "sort": [("sumAmount", -1), ("dstId", 1)]},
    "TSR5": {"cols": ["srcId", "numEdges", "sumAmount"], "sort": [("sumAmount", -1), ("srcId", 1)]},
    "TSR6": {"cols": ["dstId"], "sort": [("dstId", 1)]},
}
#: the read-write transactions' read parts (the ingest study times the whole transaction)
RW_READS = {"TRW1": "TCR4", "TRW2": "TCR7", "TRW3": "TCR11"}
QUERIES = list(RESULT)


def _id(u: Any) -> Any:
    """'Account:123' -> 123 (every system returns uids)."""
    if isinstance(u, str) and ":" in u:
        t = u.split(":", 1)[1]
        return int(t) if t.lstrip("-").isdigit() else t
    return u


def _r3(x: Any) -> Any:
    """Every number a system returns (int, float, Decimal) -> a float to three decimals,
    half up; ids come from uid strings (_id) and stay exact integers."""
    if isinstance(x, bool) or x is None:
        return x
    if isinstance(x, str):
        return _id(x)
    try:
        return math.floor(float(x) * 1000 + 0.5) / 1000
    except (TypeError, ValueError):
        return x


def canonical(q: str, rows: list[list[Any]]) -> list[list[Any]]:
    """Rows (in RESULT column order) -> ids stripped, floats rounded, sorted."""
    cols = RESULT[q]["cols"]
    out = []
    for r in rows:
        r = [([_id(v) for v in x] if isinstance(x, list) else _r3(x)) for x in r]
        out.append(r)
    keys = RESULT[q]["sort"]

    def k(r):
        d = dict(zip(cols, r))
        if q == "TCR5":
            d["pathLength"] = len(d["path"]) - 1
        ks = []
        for c, sgn in keys:
            v = d[c]
            ks.append(-v if sgn < 0 and isinstance(v, (int, float)) else v)
        return (ks, [str(x) for x in r])
    return sorted(out, key=k)


# ---------------------------------------------------------------------------
# Gnomon
# ---------------------------------------------------------------------------

def _scope(p: dict) -> dict:
    s: dict[str, Any] = {"vt": {"instant": "$now"}}
    if p["T"] != CURRENT:
        s["as_of_tt"] = p["T"]
    return s


def _win(v: str) -> list[dict]:
    return [cmp(">", col(f"{v}.vt_s"), lit("$start", "ts")), cmp("<", col(f"{v}.vt_s"), lit("$end", "ts"))]


def _trunc(v: str, lim: int) -> dict:
    return {"limit": lim, "order_by": [{"expr": col(f"{v}.vt_s"), "dir": "desc"}]}


def _M(nodes, steps, ret, p, where=None) -> dict:
    a = MC.M(nodes, steps, ret, where=where, scope=_scope(p))
    a["params"] = {k: v for k, v in p.items() if k not in ("T",)}
    return a


AMT = tp("e", "amount", "float")
TW = lambda v: {"var": v, "where": _win(v)}  # noqa: E731


def gnomon(q: str, p: dict) -> list[tuple[str, dict]]:
    """The calls for query q at parameters p ($-names resolved by the operator)."""
    A = lambda v="acc", par="id": N(v, "Account", uids=f"${par}")  # noqa: E731
    lim = p.get("lim", 500)
    if q == "TCR1":
        return [("match", _M([A(), N("other", "Account"), N("med", "Medium")],
                 [{**S("acc", "other", "TRANSFER", hops={"min": 1, "max": 3}), "order": "strict_increasing",
                   "edge_where": TW("t"), "truncate": _trunc("t", lim)},
                  S("med", "other", "SIGN_IN", var="si")],
                 {"group": {"by": by(otherId=col("other.uid"), accountDistance=col("other.depth"),
                                     mediumId=col("med.uid"), mediumType=tp("med", "mediumType")), "aggs": []}},
                 p, where=[cmp("=", tp("med", "isBlocked", "bool"), lit(True))] + _win("si")))]
    if q == "TCR2":
        return [("match", _M([N("p", "Person", uids="$id"), N("acc", "Account"), N("other", "Account"),
                              N("loan", "Loan")],
                 [S("p", "acc", "OWN"),
                  {**S("other", "acc", "TRANSFER", hops={"min": 1, "max": 3}), "order": "strict_increasing",
                   "edge_where": TW("t"), "truncate": _trunc("t", lim)},
                  S("loan", "other", "DEPOSIT", var="dep")],
                 {"group": {"by": by(otherId=col("other.uid")),
                            "aggs": [agg("sumLoanAmount", "sum", tp("loan", "loanAmount", "float"), distinct_on=["loan"]),
                                     agg("sumLoanBalance", "sum", tp("loan", "balance", "float"), distinct_on=["loan"])]}},
                 p, where=_win("dep")))]
    if q == "TCR3":
        return [("paths", {"sources": {"uids": [p["id1"]]}, "targets": {"uids": [p["id2"]]},
                           "t_valid": p["now"], **({"as_of_tt": p["T"]} if p["T"] != CURRENT else {}),
                           "rel_types": ["TRANSFER"], "dir": "out",
                           "edge_window": {"t_a": p["start"] + 1, "t_b": p["end"]},
                           "metric": "hops", "result": "distance"})]
    if q == "TCR4":
        return [("match", _M([N("s", "Account", uids="$id1"), N("d", "Account", uids="$id2"), N("o", "Account")],
                 [S("s", "d", "TRANSFER", var="e1"), S("d", "o", "TRANSFER", var="e3"),
                  S("o", "s", "TRANSFER", var="e2")],
                 {"group": {"by": by(otherId=col("o.uid")),
                            "aggs": [agg("numEdge2", "count", distinct_on=["e2"]),
                                     agg("sumEdge2Amount", "sum", tp("e2", "amount", "float"), distinct_on=["e2"]),
                                     agg("maxEdge2Amount", "max", tp("e2", "amount", "float")),
                                     agg("numEdge3", "count", distinct_on=["e3"]),
                                     agg("sumEdge3Amount", "sum", tp("e3", "amount", "float"), distinct_on=["e3"]),
                                     agg("maxEdge3Amount", "max", tp("e3", "amount", "float"))]}},
                 p, where=_win("e1") + _win("e2") + _win("e3")))]
    if q == "TCR5":
        # paths has no per-node truncation argument (reported): lim is not applied here. "limit" is the page:
        # w4_check.executor reads every page (cursor), so the answer is the whole path set
        src = {"match": MC.M([N("p", "Person", uids="$id"), N("a", "Account")], [S("p", "a", "OWN")],
                             {"rows": R(uid=col("a.uid"))}, scope=_scope(p))}
        src["match"]["params"] = {"id": p["id"], "now": p["now"]}
        return [("paths", {"sources": src, "t_valid": p["now"],
                           **({"as_of_tt": p["T"]} if p["T"] != CURRENT else {}),
                           "rel_types": ["TRANSFER"], "dir": "out",
                           "edge_window": {"t_a": p["start"] + 1, "t_b": p["end"]},
                           "time_respecting": "strict", "window": {"t_a": p["start"] + 1, "t_b": p["end"]},
                           "min_hops": 1, "max_hops": 3, "result": "all_paths", "limit": 10000})]
    if q == "TCR6":
        return [("match", _M([A("card"), N("mid", "Account"), N("src", "Account")],
                 [{**S("mid", "card", "WITHDRAW", var="e2"), "truncate": _trunc("e2", lim)},
                  {**S("src", "mid", "TRANSFER", var="e1"), "truncate": _trunc("e1", lim)}],
                 {"group": {"by": by(midId=col("mid.uid")),
                            "aggs": [agg("numSrc", "count_distinct", col("src.uid")),
                                     agg("sumEdge1Amount", "sum", tp("e1", "amount", "float"), distinct_on=["e1"]),
                                     agg("sumEdge2Amount", "sum", tp("e2", "amount", "float"), distinct_on=["e2"])],
                            "having": cmp(">", col("numSrc"), lit(3))}},
                 p, where=[cmp(">", tp("e1", "amount", "float"), lit("$threshold1")),
                           cmp(">", tp("e2", "amount", "float"), lit("$threshold2"))] + _win("e1") + _win("e2")))]
    if q == "TCR7":
        # one call per direction: FinBench truncates each step (500 newest per direction)
        def side(d):
            a_, b_ = ("x", "acc") if d == "in" else ("acc", "x")
            return ("match", _M([A(), N("x", "Account")],
                    [{**S(a_, b_, "TRANSFER", var="e"), "truncate": _trunc("e", lim)}],
                    {"group": {"by": [], "aggs": [agg("n", "count_distinct", col("x.uid")),
                                                  agg("s", "sum", AMT), agg("c", "count")]}},
                    p, where=_win("e") + [cmp(">", AMT, lit("$threshold"))]))
        return [side("in"), side("out")]
    if q == "TCR8":
        def branch(k: int) -> dict:
            names = [f"a{i}" for i in range(k + 1)]
            nodes = [N("loan", "Loan", uids="$id")] + [N(v, "Account") for v in names]
            steps = [S("loan", "a0", "DEPOSIT", var="dep")]
            where = _win("dep")
            for i in range(k):
                e = f"h{i}"
                steps.append(S(names[i], names[i + 1], ["TRANSFER", "WITHDRAW"], var=e))
                where += _win(e)
                if i:
                    where.append(cmp(">", tp(e, "amount", "float"),
                                     arith("*", tp(f"h{i - 1}", "amount", "float"), lit("$threshold"))))
            last = f"h{k - 1}"
            return {"pattern": {"nodes": nodes, "steps": steps, "where": where},
                    "rows": R(dstId=col(f"{names[-1]}.uid"), distance=lit(k + 1), lsrc=col(f"{last}.src"),
                              lrel=col(f"{last}.rel_type"), lts=col(f"{last}.vt_s"),
                              amount=tp(last, "amount", "float"), loanAmount=tp("loan", "loanAmount", "float"))}
        return [("match", {"union": [branch(k) for k in (1, 2, 3)],
                           "return": {"group": {"by": by(dstId=col("dstId"), lsrc=col("lsrc"), lrel=col("lrel"), lts=col("lts"),
                                                       lamt=col("amount")),
                                                "aggs": [agg("d", "min", col("distance")),
                                                         agg("amount", "max", col("amount")),
                                                         agg("loanAmount", "max", col("loanAmount"))],
                                                "then": {"by": by(dstId=col("dstId")),
                                                         "aggs": [agg("minDistanceFromLoan", "min", col("d")),
                                                                  agg("inflow", "sum", col("amount")),
                                                                  agg("loanAmount", "max", col("loanAmount"))]}}},
                           "scope": _scope(p), "params": {k: v for k, v in p.items() if k != "T"}})]
    if q == "TCR9":
        REL = lambda r: cmp("=", col("e.rel_type"), lit(r))  # noqa: E731
        INC, OUT = cmp("=", col("e.dst"), col("acc.uid")), cmp("=", col("e.src"), col("acc.uid"))
        return [("match", _M([A(), N("x")], [S("acc", "x", None, var="e", dir="both")],
                 {"group": {"by": [], "aggs": [agg("dep", "sum", AMT, where=REL("DEPOSIT")),
                                               agg("rep", "sum", AMT, where=REL("REPAY")),
                                               agg("tin", "sum", AMT, where=AND(REL("TRANSFER"), INC)),
                                               agg("tout", "sum", AMT, where=AND(REL("TRANSFER"), OUT))]}},
                 p, where=_win("e") + [cmp(">", AMT, lit("$threshold")),
                                       OR(OR(REL("DEPOSIT"), REL("REPAY")), REL("TRANSFER"))]))]
    if q == "TCR10":
        return [("match", _M([N("x", "Person", uids=["$pid1", "$pid2"]), N("c", "Company")],
                 [S("x", "c", "INVEST", var="e")],
                 {"group": {"by": by(companyId=col("c.uid")),
                            "aggs": [agg("by1", "count", where=cmp("=", col("x.uid"), lit("$pid1"))),
                                     agg("by2", "count", where=cmp("=", col("x.uid"), lit("$pid2")))]}},
                 p, where=_win("e")))]
    if q == "TCR11":
        return [("match", _M([N("p", "Person", uids="$id"), N("q", "Person"), N("l", "Loan")],
                 [{**S("p", "q", "GUARANTEE", hops={"min": 1, "max": 5}), "edge_where": TW("t"),
                   "truncate": _trunc("t", lim)}, S("q", "l", "APPLY")],
                 {"group": {"by": [], "aggs": [agg("sumLoanAmount", "sum", tp("l", "loanAmount", "float"),
                                                   distinct_on=["l"]),
                                               agg("numLoans", "count_distinct", col("l.uid"))]}}, p))]
    if q == "TCR12":
        return [("match", _M([N("p", "Person", uids="$id"), N("acc", "Account"), N("cacc", "Account"),
                              N("co", "Company")],
                 [S("p", "acc", "OWN"), {**S("acc", "cacc", "TRANSFER", var="e"), "truncate": _trunc("e", lim)},
                  S("co", "cacc", "OWN")],
                 {"group": {"by": by(compAccountId=col("cacc.uid")),
                            "aggs": [agg("sumEdge2Amount", "sum", AMT, distinct_on=["e"])]}},
                 p, where=_win("e")))]
    if q == "TSR1":
        return [("match", _M([A()], [], {"rows": R(createTime=tp("acc", "createTime", "int"),
                                                   isBlocked=tp("acc", "isBlocked", "bool"),
                                                   type=tp("acc", "accountType"))}, p))]
    if q == "TSR2":
        def star(d):
            a, b = ("acc", "x") if d == "out" else ("x", "acc")
            return ("match", _M([A(), N("x", "Account")], [S(a, b, "TRANSFER", var="e")],
                    {"group": {"by": [], "aggs": [agg("s", "sum", AMT), agg("m", "max", AMT),
                                                  agg("n", "count")]}}, p, where=_win("e")))
        return [star("out"), star("in")]
    if q == "TSR3":
        return [("match", _M([A(), N("src", "Account")], [S("src", "acc", "TRANSFER", var="e")],
                 {"group": {"by": [], "aggs": [agg("blocked", "count",
                                                   where=cmp("=", tp("src", "isBlocked", "bool"), lit(True))),
                                               agg("all", "count")]}},
                 p, where=_win("e") + [cmp(">", AMT, lit("$threshold"))]))]
    if q in ("TSR4", "TSR5"):
        a, b, o = ("acc", "x", "dstId") if q == "TSR4" else ("x", "acc", "srcId")
        return [("match", _M([A(), N("x", "Account")], [S(a, b, "TRANSFER", var="e")],
                 {"group": {"by": by(**{o: col("x.uid")}),
                            "aggs": [agg("numEdges", "count"), agg("sumAmount", "sum", AMT)]}},
                 p, where=_win("e") + [cmp(">", AMT, lit("$threshold"))]))]
    if q == "TSR6":
        return [("match", _M([A("src"), N("mid", "Account"), N("dst", "Account")],
                 [S("mid", "src", "TRANSFER", var="e"), S("mid", "dst", "TRANSFER", var="e2")],
                 {"group": {"by": by(dstId=col("dst.uid")), "aggs": []}},
                 p, where=_win("e") + _win("e2") + [cmp("=", tp("dst", "isBlocked", "bool"), lit(True)),
                                                    cmp("!=", col("dst.uid"), col("src.uid"))]))]
    raise KeyError(q)


def _rows(payload: dict) -> list[dict]:
    return payload.get("rows", [])


def gnomon_rows(q: str, payloads: list[dict]) -> list[list[Any]]:
    """Gnomon payloads -> rows in RESULT column order (before canonical())."""
    P = payloads
    cols = RESULT[q]["cols"]
    if q == "TCR3":
        rows = _rows(P[0])
        d = min((r.get("distance", r.get("hops")) for r in rows), default=None)
        return [[-1 if d is None else d]]
    if q == "TCR5":
        out = []
        for r in _rows(P[0]):
            path = r.get("path") or r.get("nodes") or r.get("uids")
            out.append([list(path)])
        uniq = {tuple(x[0]): x for x in out}
        return list(uniq.values())
    if q == "TCR7":
        i = (_rows(P[0]) or [{}])[0]
        o = (_rows(P[1]) or [{}])[0]
        rat = -1.0 if not o.get("c") else (i.get("s") or 0.0) / o["s"]
        return [[i.get("n") or 0, o.get("n") or 0, rat]]
    if q == "TCR8":
        return [[r["dstId"], r["inflow"] / r["loanAmount"], r["minDistanceFromLoan"]] for r in _rows(P[0])]
    if q == "TCR9":
        r = (_rows(P[0]) or [{}])[0]
        f = lambda n, d: -1.0 if not d else (n or 0.0) / d  # noqa: E731
        return [[f(r.get("dep"), r.get("rep")), f(r.get("dep"), r.get("tout")), f(r.get("tin"), r.get("tout"))]]
    if q == "TCR10":
        rows = _rows(P[0])
        u = len(rows)
        i = sum(1 for r in rows if r["by1"] > 0 and r["by2"] > 0)
        return [[0.0 if u == 0 else i / u]]
    if q == "TCR11":
        r = (_rows(P[0]) or [{}])[0]
        return [[r.get("sumLoanAmount") or 0.0, r.get("numLoans") or 0]]
    if q == "TSR2":
        o = (_rows(P[0]) or [{}])[0]
        i = (_rows(P[1]) or [{}])[0]
        mx = lambda v: -1 if v is None else v  # noqa: E731
        return [[o.get("s") or 0.0, mx(o.get("m")), o.get("n") or 0, i.get("s") or 0.0, mx(i.get("m")), i.get("n") or 0]]
    if q == "TSR3":
        r = (_rows(P[0]) or [{}])[0]
        a = r.get("all") or 0
        return [[-1 if a == 0 else (r.get("blocked") or 0) / a]]
    return [[r.get(c) for c in cols] for r in _rows(P[0])]


# ---------------------------------------------------------------------------
# SQL (DuckDB (SQL) and PostgreSQL): node_v, edge_v (w4_check.py export)
# ---------------------------------------------------------------------------

def _tr(alias: str, rel: str, part: str, extra: str = "") -> str:
    """The truncated, window-qualifying, believed edges of `rel` (part: src = out-steps, dst = in-steps)."""
    other = "dst" if part == "src" else "src"
    return (f"{alias} AS (SELECT * FROM (SELECT e.*, row_number() OVER (PARTITION BY e.{part} "
            f"ORDER BY e.vt_s DESC, e.{other}, e.disc) AS rn FROM edge_v e WHERE e.rel = '{rel}' AND {{B:e}} "
            f"AND {{W:e}}{extra}) z WHERE rn <= {{lim}})")


SQL: dict[str, str] = {
    "TCR1": "WITH RECURSIVE " + _tr("t", "TRANSFER", "src") + """,
 r(node, ts, d) AS (SELECT t.dst, t.vt_s, 1 FROM t WHERE t.src = {id}
   UNION SELECT t.dst, t.vt_s, r.d + 1 FROM r JOIN t ON t.src = r.node AND t.vt_s > r.ts WHERE r.d < 3),
 o AS (SELECT node, min(d) AS d FROM r GROUP BY node)
SELECT DISTINCT o.node, o.d, si.src, m.mediumType FROM o
 JOIN edge_v si ON si.dst = o.node AND si.rel = 'SIGN_IN' AND {B:si} AND {W:si}
 JOIN node_v m ON m.uid = si.src AND {B:m} AND m.isBlocked""",
    "TCR2": "WITH RECURSIVE " + _tr("t", "TRANSFER", "dst") + """,
 acc AS (SELECT o.dst AS uid FROM edge_v o WHERE o.rel = 'OWN' AND o.src = {id} AND {B:o}),
 r(node, ts, d) AS (SELECT t.src, t.vt_s, 1 FROM t JOIN acc ON t.dst = acc.uid
   UNION SELECT t.src, t.vt_s, r.d + 1 FROM r JOIN t ON t.dst = r.node AND t.vt_s < r.ts WHERE r.d < 3),
 o AS (SELECT DISTINCT node FROM r),
 l AS (SELECT DISTINCT o.node, dep.src AS loan FROM o JOIN edge_v dep ON dep.dst = o.node
        AND dep.rel = 'DEPOSIT' AND {B:dep} AND {W:dep})
SELECT l.node, sum(n.loanAmount), sum(n.balance) FROM l JOIN node_v n ON n.uid = l.loan AND {B:n}
 GROUP BY l.node""",
    "TCR3": """WITH RECURSIVE t AS (SELECT src, dst FROM edge_v e WHERE e.rel = 'TRANSFER' AND {B:e} AND {W:e}),
 r(node, d) AS (SELECT {id1}, 0 UNION SELECT t.dst, r.d + 1 FROM r JOIN t ON t.src = r.node WHERE r.d < 15)
SELECT coalesce(min(d), -1) FROM r WHERE node = {id2} AND d > 0""",
    "TCR4": """WITH t AS (SELECT * FROM edge_v e WHERE e.rel = 'TRANSFER' AND {B:e} AND {W:e}),
 ok AS (SELECT 1 AS x FROM t WHERE t.src = {id1} AND t.dst = {id2} LIMIT 1),
 others AS (SELECT DISTINCT e3.dst AS o FROM t e3 JOIN t e2 ON e2.src = e3.dst AND e2.dst = {id1}
            WHERE e3.src = {id2} AND EXISTS (SELECT 1 FROM ok)),
 a2 AS (SELECT e2.src AS o, count(*) AS n, sum(e2.amount) AS s, max(e2.amount) AS m FROM t e2
        WHERE e2.dst = {id1} GROUP BY e2.src),
 a3 AS (SELECT e3.dst AS o, count(*) AS n, sum(e3.amount) AS s, max(e3.amount) AS m FROM t e3
        WHERE e3.src = {id2} GROUP BY e3.dst)
SELECT others.o, a2.n, a2.s, a2.m, a3.n, a3.s, a3.m FROM others JOIN a2 ON a2.o = others.o
 JOIN a3 ON a3.o = others.o""",
    "TCR5": """WITH t AS (SELECT * FROM edge_v e WHERE e.rel = 'TRANSFER' AND {B:e} AND {W:e}),
 acc AS (SELECT o.dst AS uid FROM edge_v o WHERE o.rel = 'OWN' AND o.src = {id} AND {B:o}),
 p1 AS (SELECT DISTINCT acc.uid AS n0, t1.dst AS n1, t1.vt_s AS ts1 FROM acc JOIN t t1 ON t1.src = acc.uid),
 p2 AS (SELECT DISTINCT p1.n0, p1.n1, t2.dst AS n2, t2.vt_s AS ts2 FROM p1 JOIN t t2 ON t2.src = p1.n1
        AND t2.vt_s > p1.ts1),
 p3 AS (SELECT DISTINCT p2.n0, p2.n1, p2.n2, t3.dst AS n3 FROM p2 JOIN t t3 ON t3.src = p2.n2 AND t3.vt_s > p2.ts2)
SELECT DISTINCT n0, n1, NULL, NULL FROM p1 WHERE n0 <> n1
UNION SELECT DISTINCT n0, n1, n2, NULL FROM p2 WHERE n0 <> n1 AND n0 <> n2 AND n1 <> n2
UNION SELECT DISTINCT n0, n1, n2, n3 FROM p3 WHERE n0 <> n1 AND n0 <> n2 AND n0 <> n3 AND n1 <> n2
  AND n1 <> n3 AND n2 <> n3""",
    "TCR6": "WITH " + _tr("w", "WITHDRAW", "dst", " AND e.amount > {threshold2}") + ", "
            + _tr("t", "TRANSFER", "dst", " AND e.amount > {threshold1}") + """,
 m AS (SELECT w.src AS mid, sum(w.amount) AS s2 FROM w WHERE w.dst = {id} GROUP BY w.src),
 i AS (SELECT t.dst AS mid, count(DISTINCT t.src) AS ns, sum(t.amount) AS s1 FROM t JOIN m ON t.dst = m.mid
       GROUP BY t.dst)
SELECT m.mid, i.s1, m.s2 FROM m JOIN i ON i.mid = m.mid WHERE i.ns > 3""",
    "TCR7": "WITH " + _tr("ti", "TRANSFER", "dst", " AND e.amount > {threshold}") + ", "
            + _tr("to_", "TRANSFER", "src", " AND e.amount > {threshold}") + """
SELECT (SELECT count(DISTINCT src) FROM ti WHERE dst = {id}), (SELECT count(DISTINCT dst) FROM to_ WHERE src = {id}),
 CASE WHEN (SELECT count(*) FROM to_ WHERE src = {id}) = 0 THEN -1.0
      ELSE coalesce((SELECT sum(amount) FROM ti WHERE dst = {id}), 0) / (SELECT sum(amount) FROM to_ WHERE src = {id}) END""",
    "TCR8": """WITH t AS (SELECT * FROM edge_v e WHERE e.rel IN ('TRANSFER', 'WITHDRAW') AND {B:e} AND {W:e}),
 s AS (SELECT DISTINCT d.dst AS a0 FROM edge_v d WHERE d.rel = 'DEPOSIT' AND d.src = {id} AND {B:d} AND {W:d}),
 l AS (SELECT n.loanAmount AS la FROM node_v n WHERE n.uid = {id} AND {B:n}),
 p1 AS (SELECT h0.dst AS dst, h0.amount AS amt, h0.src || '|' || h0.dst || '|' || h0.rel || '|' || h0.disc AS le, 2 AS d
        FROM s JOIN t h0 ON h0.src = s.a0),
 p2 AS (SELECT h1.dst, h1.amount, h1.src || '|' || h1.dst || '|' || h1.rel || '|' || h1.disc, 3
        FROM s JOIN t h0 ON h0.src = s.a0 JOIN t h1 ON h1.src = h0.dst AND h1.amount > h0.amount * {threshold}),
 p3 AS (SELECT h2.dst, h2.amount, h2.src || '|' || h2.dst || '|' || h2.rel || '|' || h2.disc, 4
        FROM s JOIN t h0 ON h0.src = s.a0 JOIN t h1 ON h1.src = h0.dst AND h1.amount > h0.amount * {threshold}
        JOIN t h2 ON h2.src = h1.dst AND h2.amount > h1.amount * {threshold}),
 u AS (SELECT * FROM p1 UNION ALL SELECT * FROM p2 UNION ALL SELECT * FROM p3),
 e AS (SELECT dst, le, max(amt) AS amt, min(d) AS d FROM u GROUP BY dst, le)
SELECT e.dst, sum(e.amt) / (SELECT la FROM l), min(e.d) FROM e GROUP BY e.dst""",
    "TCR9": """WITH x AS (SELECT * FROM edge_v e WHERE {B:e} AND {W:e} AND e.amount > {threshold}),
 v AS (SELECT (SELECT sum(amount) FROM x WHERE rel = 'DEPOSIT' AND dst = {id}) AS dep,
              (SELECT sum(amount) FROM x WHERE rel = 'REPAY' AND src = {id}) AS rep,
              (SELECT sum(amount) FROM x WHERE rel = 'TRANSFER' AND dst = {id}) AS tin,
              (SELECT sum(amount) FROM x WHERE rel = 'TRANSFER' AND src = {id}) AS tout)
SELECT CASE WHEN coalesce(rep, 0) = 0 THEN -1.0 ELSE coalesce(dep, 0) / rep END,
       CASE WHEN coalesce(tout, 0) = 0 THEN -1.0 ELSE coalesce(dep, 0) / tout END,
       CASE WHEN coalesce(tout, 0) = 0 THEN -1.0 ELSE coalesce(tin, 0) / tout END FROM v""",
    "TCR10": """WITH i AS (SELECT DISTINCT src, dst FROM edge_v e WHERE e.rel = 'INVEST' AND e.src IN ({pid1}, {pid2})
                AND {B:e} AND {W:e}),
 c AS (SELECT dst, count(DISTINCT src) AS k FROM i GROUP BY dst)
SELECT CASE WHEN count(*) = 0 THEN 0.0 ELSE 1.0 * sum(CASE WHEN k = 2 THEN 1 ELSE 0 END) / count(*) END FROM c""",
    "TCR11": "WITH RECURSIVE " + _tr("g", "GUARANTEE", "src") + """,
 r(node, d) AS (SELECT g.dst, 1 FROM g WHERE g.src = {id} UNION SELECT g.dst, r.d + 1 FROM r JOIN g ON g.src = r.node
   WHERE r.d < 5),
 l AS (SELECT DISTINCT a.dst AS loan FROM (SELECT DISTINCT node FROM r) q JOIN edge_v a ON a.src = q.node
       AND a.rel = 'APPLY' AND {B:a})
SELECT coalesce(sum(n.loanAmount), 0.0), count(*) FROM l JOIN node_v n ON n.uid = l.loan AND {B:n}""",
    "TCR12": "WITH " + _tr("t", "TRANSFER", "src") + """,
 acc AS (SELECT o.dst AS uid FROM edge_v o WHERE o.rel = 'OWN' AND o.src = {id} AND {B:o}),
 ca AS (SELECT DISTINCT o.dst AS uid FROM edge_v o JOIN node_v c ON c.uid = o.src AND c.label = 'Company' AND {B:c}
        WHERE o.rel = 'OWN' AND {B:o})
SELECT t.dst, sum(t.amount) FROM acc JOIN t ON t.src = acc.uid JOIN ca ON ca.uid = t.dst GROUP BY t.dst""",
    "TSR1": """SELECT n.createTime, n.isBlocked, n.accountType FROM node_v n WHERE n.uid = {id} AND {B:n}""",
    "TSR2": """WITH t AS (SELECT * FROM edge_v e WHERE e.rel = 'TRANSFER' AND {B:e} AND {W:e} AND (e.src = {id} OR e.dst = {id}))
SELECT coalesce((SELECT sum(amount) FROM t WHERE src = {id}), 0.0), coalesce((SELECT max(amount) FROM t WHERE src = {id}), -1),
 (SELECT count(*) FROM t WHERE src = {id}),
 coalesce((SELECT sum(amount) FROM t WHERE dst = {id}), 0.0), coalesce((SELECT max(amount) FROM t WHERE dst = {id}), -1),
 (SELECT count(*) FROM t WHERE dst = {id})""",
    "TSR3": """WITH t AS (SELECT e.src FROM edge_v e WHERE e.rel = 'TRANSFER' AND e.dst = {id} AND {B:e} AND {W:e}
               AND e.amount > {threshold})
SELECT CASE WHEN count(*) = 0 THEN -1.0 ELSE 1.0 * sum(CASE WHEN n.isBlocked THEN 1 ELSE 0 END) / count(*) END
 FROM t JOIN node_v n ON n.uid = t.src AND {B:n}""",
    "TSR4": """SELECT e.dst, count(*), sum(e.amount) FROM edge_v e WHERE e.rel = 'TRANSFER' AND e.src = {id} AND {B:e}
 AND {W:e} AND e.amount > {threshold} GROUP BY e.dst""",
    "TSR5": """SELECT e.src, count(*), sum(e.amount) FROM edge_v e WHERE e.rel = 'TRANSFER' AND e.dst = {id} AND {B:e}
 AND {W:e} AND e.amount > {threshold} GROUP BY e.src""",
    "TSR6": """SELECT DISTINCT e2.dst FROM edge_v e JOIN edge_v e2 ON e2.src = e.src AND e2.rel = 'TRANSFER' AND {B:e2} AND {W:e2}
 JOIN node_v n ON n.uid = e2.dst AND {B:n} AND n.isBlocked
 WHERE e.rel = 'TRANSFER' AND e.dst = {id} AND {B:e} AND {W:e} AND e2.dst <> {id}""",
}


def _lit(v: Any) -> str:
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, (int, float)):
        return repr(v)
    return "'" + str(v).replace("'", "''") + "'"


def sql(q: str, p: dict) -> str:
    s = SQL[q]
    s = re.sub(r"\{B:(\w+)\}", lambda m: (f"{m[1]}.vt_s <= {p['now']} AND {p['now']} < {m[1]}.vt_e AND "
                                         f"{m[1]}.tt_s <= {p['T']} AND {p['T']} < {m[1]}.tt_e"), s)
    s = re.sub(r"\{W:(\w+)\}", lambda m: f"{p['start']} < {m[1]}.vt_s AND {m[1]}.vt_s < {p['end']}", s)
    return re.sub(r"\{(\w+)\}", lambda m: _lit(p[m[1]]), s)


def sql_rows(q: str, rows: list[tuple]) -> list[list[Any]]:
    if q == "TCR5":
        return [[[x for x in r if x is not None]] for r in rows]
    return [list(r) for r in rows]


# ---------------------------------------------------------------------------
# Cypher (Neo4j and Memgraph): adapted from the Galaxybase FinBench texts
# (ldbc_finbench_transaction_impls 0.1.0, galaxybase-cypher/queries): label and
# relation names mapped, EXPANDCONFIG truncation and apoc removed, the belief
# predicate added to every version, truncation by a per-node subquery.
# ---------------------------------------------------------------------------

CYPHER: dict[str, str] = {
    "TSR1": """MATCH (a:Account {uid: $id})<-[:OF]-(s:AState) WHERE {B:s}
RETURN s.createTime, s.isBlocked, s.accountType""",
    "TSR2": """MATCH (a:Account {uid: $id})
OPTIONAL MATCH (a)-[e1:TRANSFER]->(:Account) WHERE {B:e1} AND {W:e1}
WITH a, sum(e1.amount) AS s1, max(e1.amount) AS m1, count(e1) AS n1
OPTIONAL MATCH (a)<-[e2:TRANSFER]-(:Account) WHERE {B:e2} AND {W:e2}
RETURN s1, coalesce(m1, -1), n1, sum(e2.amount), coalesce(max(e2.amount), -1), count(e2)""",
    "TSR3": """MATCH (src:Account)-[e:TRANSFER]->(a:Account {uid: $id}) WHERE {B:e} AND {W:e} AND e.amount > $threshold
MATCH (src)<-[:OF]-(s:AState) WHERE {B:s}
WITH count(e) AS n, sum(CASE WHEN s.isBlocked THEN 1 ELSE 0 END) AS b
RETURN CASE WHEN n = 0 THEN -1.0 ELSE 1.0 * b / n END""",
    "TSR4": """MATCH (a:Account {uid: $id})-[e:TRANSFER]->(d:Account) WHERE {B:e} AND {W:e} AND e.amount > $threshold
RETURN d.uid, count(e), sum(e.amount)""",
    "TSR5": """MATCH (a:Account {uid: $id})<-[e:TRANSFER]-(s:Account) WHERE {B:e} AND {W:e} AND e.amount > $threshold
RETURN s.uid, count(e), sum(e.amount)""",
    "TSR6": """MATCH (src:Account {uid: $id})<-[e1:TRANSFER]-(mid:Account)-[e2:TRANSFER]->(dst:Account)
WHERE dst.uid <> src.uid AND {B:e1} AND {W:e1} AND {B:e2} AND {W:e2}
MATCH (dst)<-[:OF]-(s:AState) WHERE {B:s} AND s.isBlocked
RETURN DISTINCT dst.uid""",
    "TCR3": """MATCH (s:Account {uid: $id1}), (d:Account {uid: $id2})
OPTIONAL MATCH p = shortestPath((s)-[es:TRANSFER*1..15]->(d)) WHERE all(e IN es WHERE {B:e} AND {W:e})
RETURN CASE WHEN p IS NULL THEN -1 ELSE length(p) END""",
    "TCR4": """MATCH (s:Account {uid: $id1})-[e1:TRANSFER]->(d:Account {uid: $id2}) WHERE {B:e1} AND {W:e1}
WITH s, d LIMIT 1
MATCH (d)-[e3:TRANSFER]->(o:Account)-[e2:TRANSFER]->(s) WHERE {B:e2} AND {W:e2} AND {B:e3} AND {W:e3}
WITH DISTINCT s, o, d
MATCH (o)-[e2:TRANSFER]->(s) WHERE {B:e2} AND {W:e2}
WITH s, o, d, count(e2) AS n2, sum(e2.amount) AS s2, max(e2.amount) AS m2
MATCH (d)-[e3:TRANSFER]->(o) WHERE {B:e3} AND {W:e3}
RETURN o.uid, n2, s2, m2, count(e3), sum(e3.amount), max(e3.amount)""",
    "TCR7": """MATCH (a:Account {uid: $id})
CALL { WITH a OPTIONAL MATCH (x:Account)-[e:TRANSFER]->(a) WHERE {B:e} AND {W:e} AND e.amount > $threshold
       WITH x, e ORDER BY e.vt_s DESC, x.uid, e.disc LIMIT $lim RETURN count(DISTINCT x) AS ns, sum(e.amount) AS si }
CALL { WITH a OPTIONAL MATCH (a)-[e:TRANSFER]->(x:Account) WHERE {B:e} AND {W:e} AND e.amount > $threshold
       WITH x, e ORDER BY e.vt_s DESC, x.uid, e.disc LIMIT $lim
       RETURN count(DISTINCT x) AS nd, sum(e.amount) AS so, count(e) AS ne }
RETURN ns, nd, CASE WHEN ne = 0 THEN -1.0 ELSE coalesce(si, 0.0) / so END""",
    "TCR9": """MATCH (a:Account {uid: $id})
OPTIONAL MATCH (:Loan)-[d:DEPOSIT]->(a) WHERE {B:d} AND {W:d} AND d.amount > $threshold
WITH a, sum(d.amount) AS dep
OPTIONAL MATCH (a)-[r:REPAY]->(:Loan) WHERE {B:r} AND {W:r} AND r.amount > $threshold
WITH a, dep, sum(r.amount) AS rep
OPTIONAL MATCH (:Account)-[i:TRANSFER]->(a) WHERE {B:i} AND {W:i} AND i.amount > $threshold
WITH a, dep, rep, sum(i.amount) AS tin
OPTIONAL MATCH (a)-[o:TRANSFER]->(:Account) WHERE {B:o} AND {W:o} AND o.amount > $threshold
WITH dep, rep, tin, sum(o.amount) AS tout
RETURN CASE WHEN rep = 0 THEN -1.0 ELSE 1.0 * dep / rep END, CASE WHEN tout = 0 THEN -1.0 ELSE 1.0 * dep / tout END,
       CASE WHEN tout = 0 THEN -1.0 ELSE 1.0 * tin / tout END""",
    "TCR10": """MATCH (p:Person)-[e:INVEST]->(c:Company) WHERE p.uid IN [$pid1, $pid2] AND {B:e} AND {W:e}
WITH c, count(DISTINCT p) AS k
WITH count(c) AS u, sum(CASE WHEN k = 2 THEN 1 ELSE 0 END) AS i
RETURN CASE WHEN u = 0 THEN 0.0 ELSE 1.0 * i / u END""",
    "TCR12": """MATCH (p:Person {uid: $id})-[o:OWN]->(acc:Account) WHERE {B:o}
CALL { WITH acc MATCH (acc)-[e:TRANSFER]->(c:Account) WHERE {B:e} AND {W:e}
       WITH e, c ORDER BY e.vt_s DESC, c.uid, e.disc LIMIT $lim RETURN e, c }
MATCH (co:Company)-[o2:OWN]->(c) WHERE {B:o2}
WITH DISTINCT c, e
RETURN c.uid, sum(e.amount)""",
    "TCR6": """MATCH (card:Account {uid: $id})
CALL { WITH card MATCH (mid:Account)-[w:WITHDRAW]->(card) WHERE {B:w} AND {W:w} AND w.amount > $threshold2
       WITH mid, w ORDER BY w.vt_s DESC, mid.uid, w.disc LIMIT $lim RETURN mid, w }
WITH mid, sum(w.amount) AS s2
CALL { WITH mid MATCH (src:Account)-[t:TRANSFER]->(mid) WHERE {B:t} AND {W:t} AND t.amount > $threshold1
       WITH src, t ORDER BY t.vt_s DESC, src.uid, t.disc LIMIT $lim RETURN src, t }
WITH mid, s2, count(DISTINCT src) AS ns, sum(t.amount) AS s1 WHERE ns > 3
RETURN mid.uid, s1, s2""",
}
#: not written for Cypher (multi-hop truncated / time-ordered walks): TCR1, TCR2, TCR5, TCR8, TCR11
CYPHER_GAPS = ["TCR1", "TCR2", "TCR5", "TCR8", "TCR11"]


#: Memgraph's dialect where it differs: weighted/BFS expansion instead of shortestPath()
CYPHER_MEMGRAPH: dict[str, str] = {
    "TCR3": """MATCH (s:Account {uid: $id1}), (d:Account {uid: $id2})
OPTIONAL MATCH p = (s)-[es:TRANSFER *BFS 1..15 (e, n | {B:e} AND {W:e})]->(d)
RETURN CASE WHEN p IS NULL THEN -1 ELSE size(es) END""",
}


def cypher(q: str, p: dict, system: str = "neo4j") -> tuple[str, dict]:
    s = CYPHER_MEMGRAPH.get(q, CYPHER[q]) if system == "memgraph" else CYPHER[q]
    s = re.sub(r"\{B:(\w+)\}", lambda m: (f"{m[1]}.vt_s <= $now AND $now < {m[1]}.vt_e AND "
                                         f"{m[1]}.tt_s <= $T AND $T < {m[1]}.tt_e"), s)
    s = re.sub(r"\{W:(\w+)\}", lambda m: f"$start < {m[1]}.vt_s AND {m[1]}.vt_s < $end", s)
    return s, dict(p)
