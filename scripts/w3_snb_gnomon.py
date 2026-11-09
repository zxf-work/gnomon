"""W3: the Gnomon side. Every query is a call of the merged operator set.

**This is not an LDBC Benchmark, and nothing produced here is an LDBC Benchmark Result.**

The operator set is `match` and the engine-level operators. That means the generalised `paths`, `state_series`, `diff_belief`, and `version_history(ref)`.

D6 mapping rules:
- reads run at the valid instant "now";
- date parameters become `vt_s` predicates;
- KNOWS is stored once and read with `dir: both`;
- deletes are `retract`.

The pattern arguments reuse the match corpus (`benchmarks/workloads/snb-patterns/queries/<row>.json`, built by `scripts/match_corpus.py`). Only their parameters and their scope are replaced:
- current: valid instant now;
- asof: valid instant now, `as_of_tt = T`;
- window: an overlap window over [VA, VB) with `co_valid: ["valid"]`, as of T.

Rows missing from the corpus are built here: IS1, IS4, IS5, IC13, IC14, BI15, BI19, BI20 and the TPC-BiH-style classes.

Answers are returned as (columns, rows), with ids converted from uid (= id*8 + hierarchy tag) to LDBC ids.
"""

from __future__ import annotations

import copy
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CORPUS = ROOT / "benchmarks/workloads/snb-patterns/queries"
OPEN = 2 ** 62
NOW_US = 1356998400000000
DAY = 86_400_000_000
TAG = {"Forum": 0, "Message": 1, "Organisation": 2, "Person": 3, "Place": 4, "Tag": 5, "TagClass": 6}


def U(h: str, i) -> str:
    return str(int(i) * 8 + TAG[h])


def scope(setting: str, sv: dict) -> dict:
    if setting == "current":
        return {"vt": {"instant": NOW_US}}
    if setting == "asof":
        return {"vt": {"instant": NOW_US}, "as_of_tt": sv["T"]}
    return {"vt": {"window": {"t_a": sv["VA"], "t_b": sv["VB"]}, "mode": "overlap"},
            "co_valid": ["valid"], "as_of_tt": sv["T"]}


def v(params: dict, k: str):
    return params[k][1]


#: LDBC parameter name -> corpus parameter, per row (values converted)
def corpus_params(qid: str, p: dict) -> dict:
    q = qid.replace("TLDBC-", "")
    if q in ("IS2", "IS3"):
        return {"person": U("Person", v(p, "personId"))}
    if q in ("IS6", "IS7"):
        return {"message": U("Message", v(p, "messageId"))}
    out = {}
    if "personId" in p:
        out["person"] = U("Person", v(p, "personId"))
    m = {"firstName": "firstName", "maxDate": "maxDate", "minDate": "minDate", "countryXName": "countryX",
         "countryYName": "countryY", "tagName": "tag", "countryName": "country", "tagClassName": "tagClass",
         "workFromYear": "year", "month": "month"}
    for k, ck in m.items():
        if k in p:
            out[ck] = v(p, k)
    if "startDate" in p and "durationDays" in p:
        out["start"] = v(p, "startDate"); out["end"] = v(p, "startDate") + v(p, "durationDays") * DAY
    if q == "IC10":
        out["nextMonth"] = v(p, "month") % 12 + 1
    if q.startswith("BI"):
        out = {}
        same = ("datetime", "tagClass", "country", "date", "tag", "endDate", "country1", "country2", "maxKnowsLimit",
                "lengthThreshold", "startDate", "minPathDistance", "maxPathDistance")
        for k in same:
            if k in p:
                out[k] = v(p, k)
        if "personId" in p:
            out["person"] = U("Person", v(p, "personId"))
        if q == "BI2":
            out.update(d1=v(p, "date"), d2=v(p, "date") + 100 * DAY, d3=v(p, "date") + 200 * DAY)
        if q in ("BI9", "BI11"):
            out.update(start=v(p, "startDate"), end=v(p, "endDate"))
        if q == "BI12":
            langs = v(p, "languages")
            if not 1 <= len(langs) <= 3:
                raise NotImplementedError(f"corpus BI12 has three language slots, LDBC gave {len(langs)}")
            langs = list(langs) + [langs[-1]] * (3 - len(langs))       # a shorter list repeats its last entry
            out.update(lang1=langs[0], lang2=langs[1], lang3=langs[2])
        if q == "BI16":
            out.update(tagA=v(p, "tagA"), tagB=v(p, "tagB"), dayA=v(p, "dateA"), dayB=v(p, "dateB"))
        if q == "BI17":
            out["delta"] = v(p, "delta") * 3_600_000_000      # LDBC: hours
    return out


def from_corpus(row: str, params: dict, sc: dict, run_match) -> list[dict]:
    """The corpus calls with our parameters and scope. A parameter the corpus
    computes from an earlier call (`probe`, e.g. BI4's top forums, BI13's
    zombies) is evaluated with the same parameters and scope."""
    d = json.loads((CORPUS / f"{row}.json").read_text())
    calls = []
    for c in d["calls"]:
        calls.append(bind_call(c, params, sc, run_match, row))
    return calls


def bind_call(c: dict, params: dict, sc: dict, run_match, row: str) -> dict:
    c = copy.deepcopy(c)
    ps = {}
    for k, x in c.get("params", {}).items():
        if k in params:
            ps[k] = params[k]
        elif not isinstance(x, dict):
            ps[k] = x
        elif "probe" in x and set(x["probe"].get("params", {})) <= set(params) | {"now"}:
            probe = bind_call(x["probe"], params, sc, run_match, row)
            rows = run_match(probe)
            ps[k] = [r[x["col"]] for r in rows] if x.get("all") else \
                (rows[min(x.get("i", 0), len(rows) - 1)][x["col"]] if rows else None)
        else:
            raise NotImplementedError(f"corpus row {row} needs parameter {k!r} not mapped from LDBC's")
    ps["now"] = NOW_US
    c["params"] = ps
    c["scope"] = {**sc}
    return c


def node_scope_rows(sc: dict) -> dict:
    return sc


def own_calls(qid: str, p: dict, sc: dict) -> list[tuple[str, dict]] | None:
    """Rows the corpus does not hold, as (operator, args)."""
    q = qid.replace("TLDBC-", "")
    tp = lambda var, key, tau="str": {"tprop": [var, key], "tau": tau}
    col = lambda c: {"col": c}
    if q == "IS1":
        return [("match", {"pattern": {"nodes": [{"var": "p", "labels": ["Person"], "uids": [U("Person", v(p, "personId"))]},
                                                 {"var": "c", "labels": ["City"]}],
                                       "steps": [{"from": "p", "to": "c", "rel_type": "IS_LOCATED_IN", "dir": "out"}]},
                           "return": {"rows": [{"as": "firstName", "expr": tp("p", "firstName")},
                                               {"as": "lastName", "expr": tp("p", "lastName")},
                                               {"as": "birthday", "expr": tp("p", "birthday", "int")},
                                               {"as": "locationIP", "expr": tp("p", "locationIP")},
                                               {"as": "browserUsed", "expr": tp("p", "browserUsed")},
                                               {"as": "cityId", "expr": col("c.uid")},
                                               {"as": "gender", "expr": tp("p", "gender")},
                                               {"as": "creationDate", "expr": col("p.vt_s")}]},
                           "scope": sc})]
    if q == "IS2":
        return is2_union(p, sc)
    if q == "IS4":     # the reference SQL's column order and coalesce(imageFile, content, '')
        return [("match", {"pattern": {"nodes": [{"var": "m", "labels": ["Post", "Comment"],
                                                  "uids": [U("Message", v(p, "messageId"))]}], "steps": []},
                           "return": {"rows": [{"as": "content", "expr": _coalesce(tp("m", "imageFile"), tp("m", "content"), {"lit": ""})},
                                               {"as": "creationDate", "expr": col("m.vt_s")}]},
                           "scope": sc})]
    if q == "IS5":
        return [("match", {"pattern": {"nodes": [{"var": "m", "labels": ["Post", "Comment"],
                                                  "uids": [U("Message", v(p, "messageId"))]},
                                                 {"var": "a", "labels": ["Person"]}],
                                       "steps": [{"from": "m", "to": "a", "rel_type": "HAS_CREATOR", "dir": "out"}]},
                           "return": {"rows": [{"as": "personId", "expr": col("a.uid")},
                                               {"as": "firstName", "expr": tp("a", "firstName")},
                                               {"as": "lastName", "expr": tp("a", "lastName")}]},
                           "scope": sc})]
    if q.startswith("BIH-"):
        return bih_calls(q, p)
    if q in ("IC14", "BI15", "BI19", "BI20"):
        return weighted_paths(q, p, sc)
    if q == "IC13":
        a = {"sources": {"uids": [U("Person", v(p, "person1Id"))]}, "targets": {"uids": [U("Person", v(p, "person2Id"))]},
             "rel_types": ["KNOWS"], "dir": "both", "metric": "hops", "result": "distance"}
        a.update(_paths_scope(sc))
        return [("paths", a)]
    return None


def weighted_paths(q: str, p: dict, sc: dict) -> list[tuple[str, dict]]:
    """IC14 (v2), BI15, BI19, BI20: `paths` with metric=weight, the weights derived by an inline `match` (rows src, dst,
    w; one value per unordered friendship). The reference formulas: IC14/BI19 max(round(40 - sqrt(n)), 1) over the
    reply interactions; BI15 10/(s + 10), s = 10 per reply to a Post and 5 per reply to a Comment inside forums
    created in [startDate, endDate] (the SQL reference's reading: both messages in such a forum; every friendship
    takes part, s = 0 when none); BI20 min |classYear difference| + 1 over shared universities."""
    sc_i = _paths_scope(sc)
    msc = {k: x for k, x in sc.items()}
    if q in ("IC14", "BI19"):
        wmatch = {"union": _reply_pairs(where=[{"cmp": "!=", "l": _col("pa.uid"), "r": _col("pb.uid")}]),
                  "return": {"group": {"by": [{"as": "src", "expr": _col("src")}, {"as": "dst", "expr": _col("dst")}],
                                       "aggs": [{"as": "n", "fn": "count"}],
                                       "project": [{"as": "w", "expr": {"if": [
                                           {"cmp": ">", "l": _f("round", _ar("-", _L(40), _f("sqrt", _col("n")))), "r": _L(1)},
                                           _f("round", _ar("-", _L(40), _f("sqrt", _col("n")))), _L(1)]}}]}},
                  "scope": msc}
        weight = {"match": wmatch, "missing": "exclude"}
    elif q == "BI15":
        A, B = v(p, "startDate"), v(p, "endDate")
        extra_n = [{"var": "root", "labels": ["Post"]}, {"var": "fo", "labels": ["Forum"]}]
        extra_s = [{"from": "msg", "to": "root", "rel_type": "REPLY_OF", "dir": "out", "hops": {"min": 0, "max": None}},
                   {"from": "fo", "to": "root", "rel_type": "CONTAINER_OF", "dir": "out"}]
        where = [{"cmp": ">=", "l": _col("fo.vt_s"), "r": {"lit": A, "tau": "ts"}},
                 {"cmp": "<=", "l": _col("fo.vt_s"), "r": {"lit": B, "tau": "ts"}}]
        val = {"if": [{"cmp": "=", "l": _col("msg.label"), "r": _L("Post")}, _L(10), _L(5)]}
        wmatch = {"union": _reply_pairs(extra_n, extra_s, where, val),
                  "return": {"group": {"by": [{"as": "src", "expr": _col("src")}, {"as": "dst", "expr": _col("dst")}],
                                       "aggs": [{"as": "s", "fn": "sum", "of": _col("val")}],
                                       "project": [{"as": "w", "expr": _ar("/", _L(10.0), _ar("+", _col("s"), _L(10.0)))}]}},
                  "scope": msc}
        weight = {"match": wmatch, "missing": "default", "default": 1.0}
    else:   # BI20
        wmatch = {"pattern": {"nodes": [{"var": "pa", "labels": ["Person"]}, {"var": "pb", "labels": ["Person"]},
                                        {"var": "u", "labels": ["University"]}],
                              "steps": [{"from": "pa", "to": "pb", "rel_type": "KNOWS", "dir": "both"},
                                        {"var": "sa", "from": "pa", "to": "u", "rel_type": "STUDY_AT", "dir": "out"},
                                        {"var": "sb", "from": "pb", "to": "u", "rel_type": "STUDY_AT", "dir": "out"}]},
                  "return": {"group": {"by": [{"as": "src", "expr": _col("pa.uid")}, {"as": "dst", "expr": _col("pb.uid")}],
                                       "aggs": [{"as": "m", "fn": "min", "of": _f("abs", _ar("-", {"tprop": ["sa", "classYear"], "tau": "int"},
                                                                                        {"tprop": ["sb", "classYear"], "tau": "int"}))}],
                                       "project": [{"as": "w", "expr": _ar("+", _col("m"), _L(1))}]}},
                  "scope": msc}
        weight = {"match": wmatch, "missing": "exclude"}
    a = {"rel_types": ["KNOWS"], "dir": "both", "metric": "weight", "weight": weight, "result": "distance", **sc_i}
    if q in ("IC14", "BI15"):
        a.update(sources={"uids": [U("Person", v(p, "person1Id"))]}, targets={"uids": [U("Person", v(p, "person2Id"))]})
    elif q == "BI19":
        city = lambda k: {"match": {"pattern": {"nodes": [{"var": "x", "labels": ["Person"]},
                                                          {"var": "c", "labels": ["City"], "uids": [U("Place", v(p, k))]}],
                                                "steps": [{"from": "x", "to": "c", "rel_type": "IS_LOCATED_IN", "dir": "out"}]},
                                    "return": {"rows": [{"as": "uid", "expr": _col("x.uid")}]}, "scope": msc}}
        a.update(sources=city("city1Id"), targets=city("city2Id"), pairs="min_over_pairs")
    else:
        a.update(sources={"uids": [U("Person", v(p, "person2Id"))]}, pairs="min_over_pairs",
                 targets={"match": {"pattern": {"nodes": [{"var": "x", "labels": ["Person"]},
                                                          {"var": "c", "labels": ["Company"]}],
                                                "steps": [{"from": "x", "to": "c", "rel_type": "WORK_AT", "dir": "out"}],
                                                "where": [{"cmp": "=", "l": {"tprop": ["c", "name"], "tau": "str"}, "r": _L(v(p, "company"))}]},
                                    "return": {"rows": [{"as": "uid", "expr": _col("x.uid")}]}, "scope": msc}})
    return [("paths", a), ("_shape", {"q": q})]


def bih_calls(q: str, p: dict) -> list[tuple[str, dict]]:
    """The TPC-BiH-style classes; `p` already holds T, T1, V, A, B (w3_snb_run.query_params)."""
    T, T1, V, A, B = (v(p, k) for k in ("T", "T1", "V", "A", "B"))
    col = lambda c: {"col": c}
    if q == "BIH-K":
        return [("version_history", {"kind": "node", "ref": {"uid": U("Person", v(p, "personId"))},
                                     "include_props": True, "belief": "all", "as_of_tt": T,
                                     "window": {"t_a": A, "t_b": B}, "_window": [A, B]})]
    if q == "BIH-VER":     # KNOWS is stored once: two directed arms (dir both is refused under belief=all)
        me = U("Person", v(p, "personId"))
        arm = lambda a, b: {"pattern": {"nodes": [{"var": "p", "labels": ["Person"], "uids": [me]}, {"var": "f", "labels": ["Person"]}],
                                        "steps": [{"var": "k", "from": a, "to": b, "rel_type": "KNOWS", "dir": "out"}]},
                            "rows": [{"as": "friendId", "expr": col("f.uid")}, {"as": "tt_s", "expr": col("k.tt_s")},
                                     {"as": "tt_e", "expr": col("k.tt_e")}]}
        return [("match", {"union": [arm("p", "f"), arm("f", "p")],
                           "return": {"group": {"by": [{"as": c, "expr": col(c)} for c in ("friendId", "tt_s", "tt_e")],
                                                "aggs": []},
                                      "order_by": [{"expr": {"cast": col("friendId"), "to": "int"}, "dir": "asc"},
                                                   {"expr": col("tt_s"), "dir": "asc"}]},
                           "scope": {"vt": {"instant": V}, "as_of_tt": T, "belief": "all"}})]
    if q == "BIH-R7":
        def arm(change, tcol, lo_cmp):
            return {"nodes": [{"var": "a", "labels": ["Person"]}, {"var": "b", "labels": ["Person"]}],
                    "steps": [{"var": "k", "from": "a", "to": "b", "rel_type": "KNOWS", "dir": "out"}],
                    "where": [{"cmp": ">", "l": col(tcol), "r": {"lit": T1, "tau": "int"}},
                              {"cmp": "<=", "l": col(tcol), "r": {"lit": T, "tau": "int"}}]}
        ret = lambda ch, tcol: [{"as": "Person1Id", "expr": col("a.uid")}, {"as": "Person2Id", "expr": col("b.uid")},
                                {"as": "change", "expr": {"lit": ch}}, {"as": "tt", "expr": col(tcol)},
                                {"as": "vt_s", "expr": col("k.vt_s")}, {"as": "vt_e", "expr": col("k.vt_e")}]
        return [("_r7", {"arms": [(arm("added", "k.tt_s", ">"), ret("added", "k.tt_s")),
                                  (arm("retracted", "k.tt_e", ">"), ret("retracted", "k.tt_e"))],
                         "scope": {"vt": {"window": {"t_a": A, "t_b": B}, "mode": "overlap"}, "as_of_tt": T, "belief": "all"}})]
    if q == "BIH-R3":
        return [("state_series", {"kind": "edge", "rel_types": ["KNOWS"], "axis": "valid", "window": {"t_a": A, "t_b": B},
                                  "stride": DAY, "at": "bucket_start", "agg": {"fn": "count"}, "as_of_tt": T})]
    if q == "BIH-R5":
        return [("match", {"pattern": {"nodes": [{"var": "p", "labels": ["Person"]}, {"var": "m", "labels": ["Post", "Comment"]}],
                                       "steps": [{"from": "m", "to": "p", "rel_type": "HAS_CREATOR", "dir": "out"}],
                                       "where": [{"cmp": "=", "l": {"tprop": ["p", "browserUsed"], "tau": "str"},
                                                  "r": {"lit": v(p, "browser")}}]},
                           "return": {"group": {"by": [{"as": "id", "expr": col("p.uid")}],
                                                "aggs": [{"as": "n", "fn": "count"}]},
                                      "order_by": [{"expr": col("n"), "dir": "desc"},
                                                   {"expr": {"cast": col("id"), "to": "int"}, "dir": "asc"}],
                                      "limit": 20},
                           "scope": {"vt": {"window": {"t_a": A, "t_b": B}, "mode": "overlap"}, "co_valid": ["valid"],
                                     "as_of_tt": T}})]
    raise NotImplementedError(q)


def _f(name, x):
    return {"fn": name, "arg": x}


def _ar(op, a, b):
    return {"arith": op, "l": a, "r": b}


def _L(v):
    return {"lit": v}


def _reply_pairs(extra_nodes=(), extra_steps=(), where=None, val=None) -> list:
    """Union arms over a reply between two friends' messages, in both orientations, rows (src, dst, val):
    the weight of an undirected friendship is symmetric (the reference counts per unordered pair)."""
    arms = []
    for a, b in (("pa", "pb"), ("pb", "pa")):
        nodes = [{"var": "pa", "labels": ["Person"]}, {"var": "pb", "labels": ["Person"]},
                 {"var": "msg", "labels": ["Post", "Comment"]}, {"var": "rep", "labels": ["Comment"]}, *extra_nodes]
        steps = [{"from": "pa", "to": "pb", "rel_type": "KNOWS", "dir": "both"},
                 {"from": "msg", "to": "pa", "rel_type": "HAS_CREATOR", "dir": "out"},
                 {"from": "rep", "to": "pb", "rel_type": "HAS_CREATOR", "dir": "out"},
                 {"from": "rep", "to": "msg", "rel_type": "REPLY_OF", "dir": "out"}, *extra_steps]
        pat = {"nodes": nodes, "steps": steps}
        if where:
            pat["where"] = where
        arms.append({"pattern": pat, "rows": [{"as": "src", "expr": _col(a + ".uid")}, {"as": "dst", "expr": _col(b + ".uid")},
                                              {"as": "val", "expr": val or _L(1)}]})
    return arms


def _paths_scope(sc: dict) -> dict:
    out = {}
    if "instant" in sc["vt"]:
        out["t_valid"] = sc["vt"]["instant"]
    elif "window" in sc["vt"]:
        # the window form: a path is valid when all its elements share one instant of W;
        # distance is the least over the instants, rule W1's reading of paths
        w = sc["vt"]["window"]
        out["valid_window"] = {"t_a": w["t_a"], "t_b": w["t_b"]}
    else:
        raise NotImplementedError("paths: no valid-time form for this scope")
    if "as_of_tt" in sc:
        out["as_of_tt"] = sc["as_of_tt"]
    return out


def _tp(var, key, tau="str"):
    return {"tprop": [var, key], "tau": tau}


def _col(c):
    return {"col": c}


def _coalesce(*xs):
    out = xs[-1]
    for x in reversed(xs[:-1]):
        out = {"coalesce": [x, out]}
    return out


def is2_union(p: dict, sc: dict) -> list:
    """IS2 as a union: a Comment walks REPLY_OF{1..} to its root Post; a Post is its own root. The corpus
    form binds HAS_CREATOR twice, which for a Post is the same edge and is excluded by match's
    edge-identity rule, while LDBC's two MATCH clauses allow it."""
    me = U("Person", v(p, "personId"))
    content = _coalesce(_tp("m", "imageFile"), _tp("m", "content"), {"lit": ""})
    rows = lambda root, author: [{"as": "messageId", "expr": _col("m.uid")}, {"as": "content", "expr": content},
                                 {"as": "creationDate", "expr": _col("m.vt_s")}, {"as": "postId", "expr": _col(root + ".uid")},
                                 {"as": "authorId", "expr": _col(author + ".uid")},
                                 {"as": "firstName", "expr": _tp(author, "firstName")}, {"as": "lastName", "expr": _tp(author, "lastName")}]
    comments = {"nodes": [{"var": "p", "labels": ["Person"], "uids": [me]}, {"var": "m", "labels": ["Comment"]},
                          {"var": "r", "labels": ["Post"]}, {"var": "a", "labels": ["Person"]}],
                "steps": [{"from": "m", "to": "p", "rel_type": "HAS_CREATOR", "dir": "out"},
                          {"from": "m", "to": "r", "rel_type": "REPLY_OF", "dir": "out", "hops": {"min": 1, "max": None}},
                          {"from": "r", "to": "a", "rel_type": "HAS_CREATOR", "dir": "out"}]}
    posts = {"nodes": [{"var": "p", "labels": ["Person"], "uids": [me]}, {"var": "m", "labels": ["Post"]}],
             "steps": [{"from": "m", "to": "p", "rel_type": "HAS_CREATOR", "dir": "out"}]}
    cols = ["messageId", "content", "creationDate", "postId", "authorId", "firstName", "lastName"]
    return [("match", {"union": [{"pattern": comments, "rows": rows("r", "a")}, {"pattern": posts, "rows": rows("m", "p")}],
                       "return": {"group": {"by": [{"as": c, "expr": _col(c)} for c in cols], "aggs": []},
                                  "order_by": [{"expr": _col("creationDate"), "dir": "desc"},
                                               {"expr": {"cast": _col("messageId"), "to": "int"}, "dir": "desc"}],
                                  "limit": 10},
                       "scope": sc})]


def _by_insert(call, after: str, items):
    by = call["return"]["group"]["by"]
    i = next(k for k, b in enumerate(by) if b["as"] == after) + 1
    by[i:i] = [{"as": a, "expr": e} for a, e in items]


def _patch(qid: str, calls: list) -> list:
    """W3 changes to corpus rows so that the answer has LDBC's reference columns and filters."""
    q = qid.replace("TLDBC-", "")
    last = calls[-1]
    if q == "IC1":     # LDBC's output columns: the friend's city and profile (presentation; the lists stay the corpus's)
        last["pattern"]["nodes"].append({"var": "fc", "labels": ["City"]})
        last["pattern"]["steps"].append({"from": "f", "to": "fc", "rel_type": "IS_LOCATED_IN", "dir": "out"})
        _by_insert(last, "birthday", [("creationDate", _col("f.vt_s")), ("gender", _tp("f", "gender")),
                                      ("browserUsed", _tp("f", "browserUsed")), ("locationIP", _tp("f", "locationIP")),
                                      ("cityName", _tp("fc", "name"))])
    elif q == "BI3":
        _by_insert(last, "title", [("creationDate", _col("fo.vt_s"))])
    elif q == "BI4":
        _by_insert(last, "firstName", [("lastName", _tp("p", "lastName")), ("creationDate", _col("p.vt_s"))])
    elif q == "BI9":
        _by_insert(last, "firstName", [("lastName", _tp("p", "lastName"))])
    elif q == "IC10":
        _by_insert(last, "lastName", [("gender", _tp("f", "gender"))])
    elif q == "IC7":
        rows = last["return"]["rows"]
        i = next(k for k, r in enumerate(rows) if r["as"] == "messageId") + 1
        rows.insert(i, {"as": "content", "expr": _coalesce(_tp("m", "imageFile"), _tp("m", "content"))})
        for r in rows:
            if r["as"] == "isNew":          # the reference returns 1 / 0
                r["expr"] = {"if": [r["expr"], {"lit": 1}, {"lit": 0}]}
    return calls


#: output columns kept, in the reference order (presentation only)
SELECT = {"BI1": ["year", "isComment", "lengthCategory", "messageCount", "averageMessageLength", "sumMessageLength",
                  "percentageOfMessages"],
          "BI8": ["personId", "score", "friendsScore"],
          "BI16": ["personId", "messageCountA", "messageCountB"],
          "IC4": ["tagName", "postCount"], "IC5": ["title", "postCount"],
          "IC10": ["personId", "firstName", "lastName", "commonInterestScore", "gender", "cityName"],
          "BIH-R3": ["t_a", "value"]}


CORPUS_ROW = {**{f"IS{i}": f"IS{i}" for i in (2, 3, 6, 7)}, **{f"IC{i}": f"IC{i}" for i in range(1, 13)},
              **{f"BI{i}": f"BI{i}" for i in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16, 17, 18)}}


def ids(cols: list[str], rows: list[list]) -> list[list]:
    """uid strings -> LDBC ids in id-valued columns; a birthday (epoch microseconds) -> ISO date."""
    import datetime as _dt
    idc = [i for i, c in enumerate(cols) if c.lower().endswith("id") or c.lower().endswith("ids")]
    bd = [i for i, c in enumerate(cols) if c == "birthday"]
    out = []
    for r in rows:
        r = list(r)
        for i in bd:
            if isinstance(r[i], int):
                r[i] = (_dt.datetime(1970, 1, 1) + _dt.timedelta(microseconds=r[i])).date().isoformat()
        for i in idc:
            x = r[i]
            if isinstance(x, str) and x.isdigit():
                r[i] = int(x) // 8
            elif isinstance(x, list):
                r[i] = [int(y) // 8 if isinstance(y, str) and y.isdigit() else y for y in x]
        out.append(r)
    return out


class Gnomon:
    name = "gnomon"

    def __init__(self, store: str):
        from extcmp import open_existing_store     # P-7: never open (= create) a missing or empty store
        from gnomon.temporal.algebra import ensure_all_registered
        ensure_all_registered()
        self.st = open_existing_store(Path(store), "native")
        self.ad = self.st.adapter

    def call(self, op: str, args: dict) -> dict:
        """One operator call, every page. With `self.capture` a list, every call is also recorded (op, args)."""
        from gnomon.temporal.algebra import call_operator
        if getattr(self, "capture", None) is not None:
            self.capture.append({"op": op, "args": json.loads(json.dumps(args, default=str))})
            if getattr(self, "capture_only", False):     # record the call without executing it (adjudication packs)
                return {"rows": [], "columns": [], "rows_total": 0}
        args = dict(args)
        # one page for match.
        # The page is the loaded engine's own match maximum, so the driver stays valid on an engine whose
        # MAX_LIMIT is still 10,000, where the w45 diff's literal 1,000,000 is refused by validation.
        from gnomon.temporal import ops_match as _OM
        args["limit"] = int(getattr(_OM, "MAX_LIMIT", 10_000)) if op == "match" else 10_000
        # admission is on: no skip_cost_check
        env = call_operator(self.ad, op, args)
        rows = list(env.get("rows", []))
        while env.get("cursor"):
            args["cursor"] = env["cursor"]
            env = call_operator(self.ad, op, args)
            rows.extend(env.get("rows", []))
        env["rows"] = rows
        return env

    def run_match(self, args: dict) -> list:
        return self.call("match", args)["rows"]

    def run(self, qid: str, inst: dict, setting: str):
        import os
        if qid in os.environ.get("W3_GNOMON_BLOCKED", "").split(","):
            raise NotImplementedError(f"blocked: {os.environ.get('W3_GNOMON_BLOCKED_WHY', 'reported Gnomon problem')}")
        from w3_snb_run import setting_values, query_params
        sv = setting_values(inst, setting)
        sc = scope(setting, sv)
        p = query_params(qid, inst, setting)
        calls = own_calls(qid, p, sc)
        # W3_GNOMON_PROBE_REUSE=0: a corpus call that repeats a probe the
        # composition already ran (BI4: its first call is the top-forum probe
        # of the second) is issued again; by default its answer is reused
        reuse = os.environ.get("W3_GNOMON_PROBE_REUSE", "1") != "0"
        probed: dict = {}

        def probe_match(args: dict) -> list:
            env = self.call("match", args)
            if reuse:
                probed[json.dumps(args, sort_keys=True, default=str)] = copy.deepcopy(env)
            return env["rows"]

        if calls is None:
            row = CORPUS_ROW.get(qid.replace("TLDBC-", ""))
            if row is None:
                raise NotImplementedError(f"no operator mapping for {qid} yet")
            calls = [("match", c) for c in _patch(qid, from_corpus(row, corpus_params(qid, p), sc, probe_match))]
        res = None
        for op, args in calls:
            if op == "_r7":       # a union of two belief-all arms, distinct rows (node versions multiply bindings)
                rows, cols = set(), None
                for pat, ret in args["arms"]:
                    r = self.call("match", {"pattern": pat, "return": {"rows": ret}, "scope": args["scope"]})
                    cols = r["columns"]
                    rows |= {tuple(x[c] for c in cols) if isinstance(x, dict) else tuple(x) for x in r["rows"]}
                rows = sorted(rows, key=lambda t: (t[3], int(t[0]), int(t[1]), t[2], t[4]))
                return cols, ids(cols, [list(t) for t in rows])
            if op == "_shape":     # LDBC's output columns for the weighted-path rows
                cols0 = res.get("columns") or (list(res["rows"][0]) if res.get("rows") else [])
                rows0 = [[r.get(c) for c in cols0] if isinstance(r, dict) else r for r in res.get("rows", [])]
                d = {c: [r[cols0.index(c)] for r in rows0] for c in ("src", "dst", "distance") if c in cols0}
                qq = args["q"]
                if qq == "IC14":
                    return ["pathWeight"], [[x] for x in d.get("distance", [])]
                if qq == "BI15":
                    return ["totalCost"], [[min(d["distance"]) if d.get("distance") else -1]]
                if qq == "BI19":
                    out = sorted(zip(d.get("src", []), d.get("dst", []), d.get("distance", [])), key=lambda t: (int(t[0]), int(t[1])))
                    return ["f", "t", "w"], [[int(a_) // 8, int(b_) // 8, w_] for a_, b_, w_ in out]
                out = sorted(zip(d.get("dst", []), d.get("distance", [])), key=lambda t: int(t[0]))[:20]
                return ["t", "w"], [[int(a_) // 8, w_] for a_, w_ in out]
            extra = {k: args.pop(k) for k in [k for k in args if k.startswith("_")]}
            key = json.dumps(args, sort_keys=True, default=str) if reuse and op == "match" else None
            res = copy.deepcopy(probed[key]) if key in probed else self.call(op, args)
            if op == "version_history":
                A, B = extra["_window"]
                keep = res["rows"]
                cols = ["id", "firstName", "lastName", "browserUsed", "locationIP", "vt_s", "vt_e", "tt_s", "tt_e"]
                out = [[int(r["uid"]) // 8] + [(r.get("props") or {}).get(c) for c in cols[1:5]]
                       + [r["vt_s"], r["vt_e"], r["tt_s"], r["tt_e"]] for r in keep]
                return cols, sorted(out, key=lambda x: (x[7], x[5]))
            if "_drop" in extra:
                res["columns"] = [c for c in res["columns"] if c not in extra["_drop"]]
        cols = res.get("columns") or (list(res["rows"][0]) if res.get("rows") else [])
        rows = [[r.get(c) for c in cols] if isinstance(r, dict) else r for r in res.get("rows", [])]
        if qid == "IC13":     # LDBC: the length, -1 when unreachable (0 for the same person)
            d = [r[cols.index("distance")] for r in rows] if "distance" in cols else []
            return ["shortestPathLength"], [[d[0] if d else -1]]
        if qid == "IC1" and rows:      # emails and languages are lists: one lookup of the result's persons
            uids = [r[cols.index("personId")] for r in rows]
            lk = self.call("match", {"pattern": {"nodes": [{"var": "x", "labels": ["Person"], "uids": uids}], "steps": []},
                                     "return": {"rows": [{"as": "uid", "expr": _col("x.uid")}, {"as": "email", "expr": {"prop": ["x", "email"]}},
                                                         {"as": "speaks", "expr": {"prop": ["x", "speaks"]}}]}, "scope": sc})
            em = {r["uid"]: (r["email"], r["speaks"]) for r in lk["rows"]}
            order = ["personId", "lastName", "distanceFromPerson", "birthday", "creationDate", "gender", "browserUsed",
                     "locationIP", "emails", "languages", "cityName", "universities", "companies"]
            ext = []
            for r in rows:
                d = dict(zip(cols, r))
                d["emails"], d["languages"] = em.get(d["personId"], (None, None))
                ext.append([d.get(c) for c in order])
            cols, rows = order, ext
        if qid in SELECT:
            idx = [cols.index(c) for c in SELECT[qid]]
            cols, rows = SELECT[qid], [[r[i] for i in idx] for r in rows]
        return cols, ids(cols, rows)
