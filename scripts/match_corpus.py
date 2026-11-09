"""The pattern operator's stage-one query corpus: one `match` argument (or a
short sequence of them) per benchmark row, over the shaped generators' stores.

**These are not LDBC, FinBench, AeonG or TPC-BiH benchmark queries or
results.** Each entry is the operator-coverage study's reading of one
benchmark row written as a `match`
argument against a synthetic store with that benchmark's schema. They exist to
hold the operator's two executors to one another, and later to the comparison
systems; the specification tie-breaks are written as explicit order keys.

    python scripts/match_corpus.py build --out DIR --scale 100 [--corrections 10]
    python scripts/match_corpus.py emit --out benchmarks/workloads/snb-patterns/queries
    python scripts/match_corpus.py run --stores DIR --route native|reference|oracle \\
        [--rows IS2,BI3] [--json out.jsonl] [--reps 1]

`run --window-days N` runs each SNB-shaped call whose scope is an instant with
current belief under rule W1 instead: the window of the N days up to that
instant (inclusive), with `co_valid: [valid]`
; a call the operator refuses there
is reported as refused.

`run` prints one line per call with the answer hash, `rows_total` and time;
`--json` appends the same as JSON lines. Parameters are resolved per store:
`{"pick": [list, i]}` takes the i-th entry of the generator's `names()`,
`{"now": 0}` its NOW, `{"date": "YYYY-MM-DD"}` a UTC midnight, `{"tt": f}`
the belief time at fraction f of the store's write history, and `{"probe":
args, "col": c, "i": i | "all": true}` the value(s) a reference evaluation
of a helper argument returns (a parameter the data must make non-trivial).
"""

from __future__ import annotations

import argparse
import calendar
import hashlib
import importlib
import json
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

DAY = 86_400_000_000


def _date(s: str) -> int:
    y, m, d = (int(x) for x in s.split("-"))
    return calendar.timegm((y, m, d, 0, 0, 0, 0, 0, 0)) * 1_000_000


# ---------------------------------------------------------------------------
# argument builders
# ---------------------------------------------------------------------------

def col(c: str) -> dict:
    return {"col": c}


def tp(var: str, key: str, tau: str = "str") -> dict:
    return {"tprop": [var, key], "tau": tau}


def lit(v: Any, tau: str | None = None) -> dict:
    return {"lit": v, **({"tau": tau} if tau else {})}


def cmp(op: str, a: dict, b: dict) -> dict:
    return {"cmp": op, "l": a, "r": b}


def AND(a: dict, b: dict) -> dict:
    return {"bool": "and", "l": a, "r": b}


def OR(a: dict, b: dict) -> dict:
    return {"bool": "or", "l": a, "r": b}


def asint(e: dict) -> dict:
    return {"cast": e, "to": "int"}


def name_is(var: str, param: str) -> dict:
    return cmp("=", tp(var, "name"), lit(param))


def N(var: str, *labels: str, uids: Any = None, uid_list: str | None = None) -> dict:
    """A node variable; `uids` is one uid (or parameter), `uid_list` a
    parameter bound to a whole list."""
    d: dict[str, Any] = {"var": var}
    if labels:
        d["labels"] = list(labels)
    if uids is not None:
        d["uids"] = uids if isinstance(uids, list) else [uids]
    if uid_list is not None:
        d["uids"] = uid_list
    return d


def S(a: str, b: str, rel: str, *, var: str | None = None, dir: str = "out",
      hops: dict | None = None) -> dict:
    d: dict[str, Any] = {"from": a, "to": b, "rel_type": rel, "dir": dir}
    if var:
        d["var"] = var
    if hops:
        d["hops"] = hops
    return d


def R(**cols: dict) -> list[dict]:
    return [{"as": k, "expr": v} for k, v in cols.items()]


def ORD(*keys: tuple[dict, str]) -> list[dict]:
    return [{"expr": e, "dir": d} for e, d in keys]


def arith(op: str, a: dict, b: dict) -> dict:
    return {"arith": op, "l": a, "r": b}


def cal(fn: str, a: dict, b: dict | None = None) -> dict:
    return {"calendar": fn, "arg": a, **({"other": b} if b is not None else {})}


def fn(name: str, a: dict) -> dict:
    return {"fn": name, "arg": a}


def IF(c: dict, a: dict, b: dict) -> dict:
    return {"if": [c, a, b]}


def round3(x: dict) -> dict:
    """FinBench's rounding: three decimals, half up."""
    return arith("/", fn("floor", arith("+", arith("*", x, lit(1000)), lit(0.5))), lit(1000))


def ratio(num: dict, den: dict) -> dict:
    """`num / den` rounded, -1 when the denominator is null or zero."""
    return IF(cmp("=", {"coalesce": [den, lit(0)]}, lit(0)), lit(-1.0),
              round3(arith("/", {"coalesce": [num, lit(0)]}, den)))


def between(c: dict, a: str, b: str) -> dict:
    return AND(cmp(">=", c, lit(a, "ts")), cmp("<", c, lit(b, "ts")))


def on_day(c: dict, day: str) -> dict:
    return AND(cmp(">=", c, lit(day, "ts")), cmp("<", c, arith("+", lit(day, "ts"), lit(DAY))))


def agg(as_: str, fn_: str, of: dict | None = None, where: dict | None = None,
        **kw: Any) -> dict:
    d: dict[str, Any] = {"as": as_, "fn": fn_}
    if of is not None:
        d["of"] = of
    if where is not None:
        d["where"] = where
    d.update(kw)
    return d


def by(**cols: dict) -> list[dict]:
    return [{"as": k, "expr": v} for k, v in cols.items()]


CLOSURE = {"min": 0, "max": None}
NOW_SCOPE = {"vt": {"instant": "$now"}}
MESSAGE = ("Post", "Comment")


def M(nodes, steps, ret, *, where=None, optional=None, not_=None, scope=None,
      params=None) -> dict:
    pattern: dict[str, Any] = {"nodes": nodes, "steps": steps}
    if where:
        pattern["where"] = where
    if optional:
        pattern["optional"] = optional
    if not_:
        pattern["not"] = not_
    return {"pattern": pattern, "return": ret, "scope": scope or NOW_SCOPE,
            "params": params or {}}


# ---------------------------------------------------------------------------
# SNB-shaped rows
# ---------------------------------------------------------------------------

PERSON = {"pick": ["persons", 0]}
POST_WITH_REPLIES = {"probe": M(
    [N("m", "Post"), N("c", "Comment")], [S("c", "m", "REPLY_OF")],
    {"group": {"by": [{"as": "m", "expr": col("m.uid")}],
               "aggs": [{"as": "n", "fn": "count"}]},
     "order_by": ORD((col("n"), "desc")), "limit": 1}), "col": "m", "i": 0}
COMMENT_WITH_REPLIES = {"probe": M(
    [N("m", "Comment"), N("c", "Comment")], [S("c", "m", "REPLY_OF")],
    {"group": {"by": [{"as": "m", "expr": col("m.uid")}],
               "aggs": [{"as": "n", "fn": "count"}]},
     "order_by": ORD((col("n"), "desc")), "limit": 1}), "col": "m", "i": 0}
BUSY_PERSON = {"probe": M(
    [N("p", "Person"), N("f", "Person")], [S("p", "f", "KNOWS", dir="both")],
    {"group": {"by": [{"as": "p", "expr": col("p.uid")}],
               "aggs": [{"as": "n", "fn": "count"}]},
     "order_by": ORD((col("n"), "desc")), "limit": 1}), "col": "p", "i": 0}
MODERATOR = {"probe": M(
    [N("f", "Forum"), N("p", "Person"), N("m", "Post")],
    [S("f", "p", "HAS_MODERATOR"), S("f", "m", "CONTAINER_OF")],
    {"group": {"by": [{"as": "p", "expr": col("p.uid")}],
               "aggs": [{"as": "n", "fn": "count"}]},
     "order_by": ORD((col("n"), "desc")), "limit": 1}), "col": "p", "i": 0}
BUSY_FORUM = {"probe": M(
    [N("f", "Forum"), N("m", "Post")], [S("f", "m", "CONTAINER_OF")],
    {"group": {"by": [{"as": "f", "expr": col("f.uid")}],
               "aggs": [{"as": "n", "fn": "count"}]},
     "order_by": ORD((col("n"), "desc")), "limit": 1}), "col": "f", "i": 0}


def is2(scope=None) -> dict:
    return M([N("p", "Person", uids="$person"), N("m", *MESSAGE), N("root", "Post"),
              N("a", "Person")],
             [S("m", "p", "HAS_CREATOR"), S("m", "root", "REPLY_OF", hops=CLOSURE),
              S("root", "a", "HAS_CREATOR")],
             {"rows": R(messageId=col("m.uid"),
                        content={"coalesce": [tp("m", "content"), tp("m", "imageFile")]},
                        creationDate=col("m.vt_s"), postId=col("root.uid"),
                        authorId=col("a.uid"), firstName=tp("a", "firstName"),
                        lastName=tp("a", "lastName")),
              "order_by": ORD((col("creationDate"), "desc"),
                            (asint(col("messageId")), "desc")),
              "limit": 10},
             scope=scope, params={"person": BUSY_PERSON})


def is3(scope=None) -> dict:
    return M([N("p", "Person", uids="$person"), N("f", "Person")],
             [S("p", "f", "KNOWS", var="k", dir="both")],
             {"rows": R(personId=col("f.uid"), firstName=tp("f", "firstName"),
                        lastName=tp("f", "lastName"), since=col("k.vt_s")),
              "order_by": ORD((col("since"), "desc"), (asint(col("personId")), "asc"))},
             scope=scope, params={"person": BUSY_PERSON})


def is6(scope=None) -> dict:
    return M([N("m", uids="$message"), N("root", "Post"), N("f", "Forum"),
              N("mod", "Person")],
             [S("m", "root", "REPLY_OF", hops=CLOSURE), S("f", "root", "CONTAINER_OF"),
              S("f", "mod", "HAS_MODERATOR")],
             {"rows": R(forumId=col("f.uid"), title=tp("f", "title"),
                        moderatorId=col("mod.uid"), firstName=tp("mod", "firstName"),
                        lastName=tp("mod", "lastName"))},
             scope=scope, params={"message": COMMENT_WITH_REPLIES})


def is7(scope=None) -> dict:
    return M([N("m", uids="$message"), N("c", *MESSAGE), N("ca", "Person"),
              N("ma", "Person")],
             [S("c", "m", "REPLY_OF"), S("c", "ca", "HAS_CREATOR"),
              S("m", "ma", "HAS_CREATOR")],
             {"rows": R(commentId=col("c.uid"), content=tp("c", "content"),
                        creationDate=col("c.vt_s"), authorId=col("ca.uid"),
                        firstName=tp("ca", "firstName"), lastName=tp("ca", "lastName"),
                        knows={"not": {"is_null": col("kk.eid")}}),
              "order_by": ORD((col("creationDate"), "desc"), (asint(col("authorId")), "asc"))},
             optional=[{"steps": [S("ca", "ma", "KNOWS", var="kk", dir="both")]}],
             scope=scope, params={"message": POST_WITH_REPLIES})


def friends(hops) -> list[dict]:
    return [N("p", "Person", uids="$person"), N("f", "Person")], \
        S("p", "f", "KNOWS", dir="both", hops=hops)


SNB: dict[str, list[dict]] = {}
SNB["IS2"] = [is2()]
SNB["IS3"] = [is3()]
SNB["IS6"] = [is6()]
SNB["IS7"] = [is7()]

SNB["IC2"] = [M(
    [N("p", "Person", uids="$person"), N("f", "Person"), N("m", *MESSAGE)],
    [S("p", "f", "KNOWS", dir="both"), S("m", "f", "HAS_CREATOR")],
    {"rows": R(personId=col("f.uid"), firstName=tp("f", "firstName"),
               lastName=tp("f", "lastName"), messageId=col("m.uid"),
               content={"coalesce": [tp("m", "content"), tp("m", "imageFile")]},
               creationDate=col("m.vt_s")),
     "order_by": ORD((col("creationDate"), "desc"), (asint(col("messageId")), "asc")),
     "limit": 20},
    where=[cmp("<=", col("m.vt_s"), lit("$maxDate", "ts"))],
    params={"person": BUSY_PERSON, "maxDate": {"date": "2011-12-01"}})]

SNB["IC5"] = [M(
    [N("p", "Person", uids="$person"), N("f", "Person"), N("fo", "Forum")],
    [S("p", "f", "KNOWS", dir="both", hops={"min": 1, "max": 2}),
     S("fo", "f", "HAS_MEMBER", var="hm")],
    {"group": {"by": [{"as": "forumId", "expr": col("fo.uid")},
                      {"as": "title", "expr": tp("fo", "title")}],
               "aggs": [{"as": "postCount", "fn": "count_distinct", "of": col("po.uid")}]},
     "order_by": ORD((col("postCount"), "desc"), (asint(col("forumId")), "asc")),
     "limit": 20},
    where=[cmp(">", col("hm.vt_s"), lit("$minDate", "ts"))],
    optional=[{"nodes": [N("po", "Post")],
               "steps": [S("fo", "po", "CONTAINER_OF"), S("po", "f", "HAS_CREATOR")]}],
    params={"person": BUSY_PERSON, "minDate": {"date": "2011-01-01"}})]

SNB["IC6"] = [M(
    [N("p", "Person", uids="$person"), N("f", "Person"), N("po", "Post"),
     N("t", "Tag"), N("t2", "Tag")],
    [S("p", "f", "KNOWS", dir="both", hops={"min": 1, "max": 2}),
     S("po", "f", "HAS_CREATOR"), S("po", "t", "HAS_TAG"), S("po", "t2", "HAS_TAG")],
    {"group": {"by": [{"as": "tagName", "expr": tp("t2", "name")}],
               "aggs": [{"as": "postCount", "fn": "count_distinct", "of": col("po.uid")}]},
     "order_by": ORD((col("postCount"), "desc"), (col("tagName"), "asc")), "limit": 10},
    where=[name_is("t", "$tag"), cmp("!=", tp("t2", "name"), lit("$tag"))],
    params={"person": BUSY_PERSON, "tag": {"pick": ["tags", 0]}})]

SNB["IC8"] = [M(
    [N("p", "Person", uids="$person"), N("m", *MESSAGE), N("c", "Comment"),
     N("ca", "Person")],
    [S("m", "p", "HAS_CREATOR"), S("c", "m", "REPLY_OF"), S("c", "ca", "HAS_CREATOR")],
    {"rows": R(personId=col("ca.uid"), firstName=tp("ca", "firstName"),
               lastName=tp("ca", "lastName"), creationDate=col("c.vt_s"),
               commentId=col("c.uid"), content=tp("c", "content")),
     "order_by": ORD((col("creationDate"), "desc"), (asint(col("commentId")), "asc")),
     "limit": 20},
    params={"person": BUSY_PERSON})]

SNB["IC9"] = [M(
    [N("p", "Person", uids="$person"), N("f", "Person"), N("m", *MESSAGE)],
    [S("p", "f", "KNOWS", dir="both", hops={"min": 1, "max": 2}), S("m", "f", "HAS_CREATOR")],
    {"rows": R(personId=col("f.uid"), firstName=tp("f", "firstName"),
               lastName=tp("f", "lastName"), messageId=col("m.uid"),
               content={"coalesce": [tp("m", "content"), tp("m", "imageFile")]},
               creationDate=col("m.vt_s")),
     "order_by": ORD((col("creationDate"), "desc"), (asint(col("messageId")), "asc")),
     "limit": 20},
    where=[cmp("<", col("m.vt_s"), lit("$maxDate", "ts"))],
    params={"person": BUSY_PERSON, "maxDate": {"date": "2012-01-01"}})]

SNB["IC11"] = [M(
    [N("p", "Person", uids="$person"), N("f", "Person"), N("co", "Company"),
     N("cn", "Country")],
    [S("p", "f", "KNOWS", dir="both", hops={"min": 1, "max": 2}),
     S("f", "co", "WORK_AT", var="w"), S("co", "cn", "IS_LOCATED_IN")],
    {"rows": R(personId=col("f.uid"), firstName=tp("f", "firstName"),
               lastName=tp("f", "lastName"), organizationName=tp("co", "name"),
               workFrom=tp("w", "workFrom", "int")),
     "order_by": ORD((col("workFrom"), "asc"), (asint(col("personId")), "asc"),
                   (col("organizationName"), "desc")),
     "limit": 10},
    where=[name_is("cn", "$country"), cmp("<", tp("w", "workFrom", "int"), lit("$year"))],
    params={"person": BUSY_PERSON, "country": {"pick": ["countries", 0]}, "year": 2011})]

SNB["BI3"] = [M(
    [N("cn", "Country"), N("ci", "City"), N("mod", "Person"), N("fo", "Forum"),
     N("po", "Post"), N("m", *MESSAGE), N("t", "Tag"), N("tc", "TagClass")],
    [S("ci", "cn", "IS_PART_OF"), S("mod", "ci", "IS_LOCATED_IN"),
     S("fo", "mod", "HAS_MODERATOR"), S("fo", "po", "CONTAINER_OF"),
     S("m", "po", "REPLY_OF", hops=CLOSURE), S("m", "t", "HAS_TAG"),
     S("t", "tc", "HAS_TYPE")],
    {"group": {"by": [{"as": "forumId", "expr": col("fo.uid")},
                      {"as": "title", "expr": tp("fo", "title")},
                      {"as": "moderatorId", "expr": col("mod.uid")}],
               "aggs": [{"as": "messageCount", "fn": "count_distinct", "of": col("m.uid")}]},
     "order_by": ORD((col("messageCount"), "desc"), (asint(col("forumId")), "asc")),
     "limit": 20},
    where=[name_is("cn", "$country"), name_is("tc", "$tagClass")],
    params={"country": {"pick": ["countries", 0]}, "tagClass": {"pick": ["tagclasses", 0]}})]

BI4_TOP = M(
    [N("fo", "Forum"), N("p", "Person"), N("ci", "City"), N("cn", "Country")],
    [S("fo", "p", "HAS_MEMBER"), S("p", "ci", "IS_LOCATED_IN"), S("ci", "cn", "IS_PART_OF")],
    {"group": {"by": [{"as": "forumId", "expr": col("fo.uid")},
                      {"as": "country", "expr": col("cn.uid")}],
               "aggs": [{"as": "members", "fn": "count_distinct", "of": col("p.uid")}],
               "then": {"by": [{"as": "forumId", "expr": col("forumId")}],
                        "aggs": [{"as": "maxMembers", "fn": "max", "of": col("members")}]}},
     "order_by": ORD((col("maxMembers"), "desc"), (asint(col("forumId")), "asc")),
     "limit": 100},
    where=[cmp(">", col("fo.vt_s"), lit("$date", "ts"))],
    params={"date": {"date": "2010-06-01"}})
SNB["BI4"] = [BI4_TOP, M(
    [N("fo", "Forum", uid_list="$top"), N("p", "Person")],
    [S("fo", "p", "HAS_MEMBER")],
    {"group": {"by": [{"as": "personId", "expr": col("p.uid")},
                      {"as": "firstName", "expr": tp("p", "firstName")}],
               "aggs": [{"as": "messageCount", "fn": "count_distinct", "of": col("m.uid")}]},
     "order_by": ORD((col("messageCount"), "desc"), (asint(col("personId")), "asc")),
     "limit": 100},
    optional=[{"nodes": [N("m", *MESSAGE), N("po", "Post"), N("fo2", "Forum", uid_list="$top")],
               "steps": [S("m", "p", "HAS_CREATOR"), S("m", "po", "REPLY_OF", hops=CLOSURE),
                         S("fo2", "po", "CONTAINER_OF")]}],
    params={"top": {"probe": BI4_TOP, "col": "forumId", "all": True}})]

# LDBC BI6: the authority score sums PopularityScoreQ06 (likes received by a person's
# messages) over the distinct persons who liked person1's messages with the tag. Each
# stage is its own lookup, as in the reference; one row per (person1, liker), then the
# sum per person1 (`then`).
BI6_POP = {"as": "pop", "on": col("p2.uid"), "key": "p", "match": {
    "pattern": {"nodes": [N("q", "Person"), N("m2", *MESSAGE), N("p3", "Person")],
                "steps": [S("m2", "q", "HAS_CREATOR"), S("p3", "m2", "LIKES")]},
    "group": {"by": by(p=col("q.uid")), "aggs": [agg("n", "count")]}}}
BI6_AUTH = {"as": "auth", "on": col("p1.uid"), "key": "p", "match": {
    "pattern": {"nodes": [N("t", "Tag"), N("m1", *MESSAGE), N("pp", "Person"), N("p2", "Person")],
                "steps": [S("m1", "t", "HAS_TAG"), S("m1", "pp", "HAS_CREATOR"),
                          S("p2", "m1", "LIKES")],
                "where": [name_is("t", "$tag")], "lookups": [BI6_POP]},
    "group": {"by": by(p=col("pp.uid"), q=col("p2.uid")),
              "aggs": [agg("n", "max", {"coalesce": [col("pop.n"), lit(0)]})],
              "then": {"by": by(p=col("p")), "aggs": [agg("s", "sum", col("n"))]}}}}
SNB["BI6"] = [M(
    [N("t", "Tag"), N("m1", *MESSAGE), N("p1", "Person")],
    [S("m1", "t", "HAS_TAG"), S("m1", "p1", "HAS_CREATOR")],
    {"group": {"by": [{"as": "personId", "expr": col("p1.uid")}],
               "aggs": [{"as": "authorityScore", "fn": "max",
                         "of": {"coalesce": [col("auth.s"), lit(0)]}}]},
     "order_by": ORD((col("authorityScore"), "desc"), (asint(col("personId")), "asc")),
     "limit": 100},
    where=[name_is("t", "$tag")],
    params={"tag": {"pick": ["tags", 0]}})]
SNB["BI6"][0]["pattern"]["lookups"] = [BI6_AUTH]

SNB["BI7"] = [M(
    [N("t", "Tag"), N("m", *MESSAGE), N("c", "Comment"), N("rt", "Tag")],
    [S("m", "t", "HAS_TAG"), S("c", "m", "REPLY_OF"), S("c", "rt", "HAS_TAG")],
    {"group": {"by": [{"as": "relatedTag", "expr": tp("rt", "name")}],
               "aggs": [{"as": "count", "fn": "count_distinct", "of": col("c.uid")}]},
     "order_by": ORD((col("count"), "desc"), (col("relatedTag"), "asc")), "limit": 100},
    where=[name_is("t", "$tag")],
    not_=[{"steps": [S("c", "t", "HAS_TAG")]}],
    params={"tag": {"pick": ["tags", 0]}})]

SNB["BI9"] = [M(
    [N("po", "Post"), N("m", *MESSAGE), N("p", "Person")],
    [S("m", "po", "REPLY_OF", hops=CLOSURE), S("po", "p", "HAS_CREATOR")],
    {"group": {"by": [{"as": "personId", "expr": col("p.uid")},
                      {"as": "firstName", "expr": tp("p", "firstName")}],
               "aggs": [{"as": "threadCount", "fn": "count_distinct", "of": col("po.uid")},
                        {"as": "messageCount", "fn": "count_distinct", "of": col("m.uid")}]},
     "order_by": ORD((col("messageCount"), "desc"), (asint(col("personId")), "asc")),
     "limit": 100},
    where=[cmp(">=", col("po.vt_s"), lit("$start", "ts")),
           cmp("<", col("po.vt_s"), lit("$end", "ts")),
           cmp(">=", col("m.vt_s"), lit("$start", "ts")),
           cmp("<", col("m.vt_s"), lit("$end", "ts"))],
    params={"start": {"date": "2011-01-01"}, "end": {"date": "2011-07-01"}})]

# LDBC BI10: experts at a shortest KNOWS distance in [minPathDistance, maxPathDistance],
# located in the country, with a message carrying a tag of the class; every tag of those
# messages is counted. The class tag and the message's other tags are two HAS_TAG edges,
# so under the edge-identity rule they are two arms of a union: the class tag itself, and
# every other tag t2.
def _bi10_arm(other_tag: bool) -> dict:
    nodes = [N("p", "Person", uids="$person"), N("e", "Person"), N("ci", "City"), N("cn", "Country"),
             N("m", *MESSAGE), N("t", "Tag"), N("tc", "TagClass")]
    steps = [S("p", "e", "KNOWS", dir="both",
               hops={"min": "$minPathDistance", "max": "$maxPathDistance"}),
             S("e", "ci", "IS_LOCATED_IN"), S("ci", "cn", "IS_PART_OF"),
             S("m", "e", "HAS_CREATOR"), S("m", "t", "HAS_TAG"), S("t", "tc", "HAS_TYPE")]
    if other_tag:
        nodes.append(N("t2", "Tag"))
        steps.append(S("m", "t2", "HAS_TAG"))
    return {"pattern": {"nodes": nodes, "steps": steps,
                        "where": [name_is("cn", "$country"), name_is("tc", "$tagClass")]},
            "rows": R(expertId=col("e.uid"), tagName=tp("t2" if other_tag else "t", "name"),
                      messageId=col("m.uid"))}


SNB["BI10"] = [{
    "return": {"group": {"by": by(expertId=col("expertId"), tagName=col("tagName")),
                         "aggs": [agg("messageCount", "count_distinct", col("messageId"))]},
               "order_by": ORD((col("messageCount"), "desc"), (col("tagName"), "asc"),
                               (asint(col("expertId")), "asc")),
               "limit": 100},
    "scope": NOW_SCOPE,
    "params": {"person": BUSY_PERSON, "country": {"pick": ["countries", 0]},
               "tagClass": {"pick": ["tagclasses", 0]}, "minPathDistance": 3, "maxPathDistance": 4},
    "union": [_bi10_arm(False), _bi10_arm(True)]}]

# LDBC BI11: triangles of persons all located in the country, with every KNOWS created in
# [startDate, endDate]. The persons come as one uid list: three City-IS_PART_OF-Country
# steps would bind one edge twice when two persons share a city, which the edge-identity
# rule refuses.
BI11_PERSONS = M(
    [N("x", "Person"), N("ci", "City"), N("cn", "Country")],
    [S("x", "ci", "IS_LOCATED_IN"), S("ci", "cn", "IS_PART_OF")],
    {"rows": R(uid=col("x.uid"))},
    where=[name_is("cn", "$country")],
    params={"country": {"pick": ["countries", 0]}})
SNB["BI11"] = [M(
    [N("a", "Person", uid_list="$persons"), N("b", "Person", uid_list="$persons"),
     N("c", "Person", uid_list="$persons")],
    [S("a", "b", "KNOWS", var="k1", dir="both"), S("b", "c", "KNOWS", var="k2", dir="both"),
     S("c", "a", "KNOWS", var="k3", dir="both")],
    {"group": {"by": [], "aggs": [{"as": "count", "fn": "count"}]}},
    where=[cmp("<", asint(col("a.uid")), asint(col("b.uid"))),
           cmp("<", asint(col("b.uid")), asint(col("c.uid")))]
    + [AND(cmp(">=", col(f"{k}.vt_s"), lit("$start", "ts")),
           cmp("<=", col(f"{k}.vt_s"), lit("$end", "ts"))) for k in ("k1", "k2", "k3")],
    params={"country": {"pick": ["countries", 0]}, "start": {"date": "2010-01-01"},
            "end": {"date": "2012-07-01"},
            "persons": {"probe": BI11_PERSONS, "col": "uid", "all": True}})]

# LDBC BI12: the root Post's language is in the `languages` list. `match` has no list
# membership test, so the list takes three slots (LDBC's generated lists have three
# entries); a shorter list repeats its last entry.
SNB["BI12"] = [M(
    [N("p", "Person")], [],
    {"group": {"by": [{"as": "personId", "expr": col("p.uid")}],
               "aggs": [{"as": "messageCount", "fn": "count", "of": col("m.uid")}],
               "then": {"by": [{"as": "messageCount", "expr": col("messageCount")}],
                        "aggs": [{"as": "personCount", "fn": "count"}]}},
     "order_by": ORD((col("personCount"), "desc"), (col("messageCount"), "desc"))},
    optional=[{"nodes": [N("m", *MESSAGE), N("po", "Post")],
               "steps": [S("m", "p", "HAS_CREATOR"), S("m", "po", "REPLY_OF", hops=CLOSURE)],
               "where": [{"not": {"is_null": tp("m", "content")}},
                         cmp("<", tp("m", "length", "int"), lit("$lengthThreshold")),
                         cmp(">", col("m.vt_s"), lit("$startDate", "ts")),
                         OR(OR(cmp("=", tp("po", "language"), lit("$lang1")),
                               cmp("=", tp("po", "language"), lit("$lang2"))),
                            cmp("=", tp("po", "language"), lit("$lang3")))]}],
    params={"lengthThreshold": 60, "startDate": {"date": "2011-01-01"},
            "lang1": "en", "lang2": "de", "lang3": "de"})]

SNB["BI17"] = [M(
    [N("tag", "Tag"), N("m1", *MESSAGE), N("p1", "Person"), N("post1", "Post"),
     N("f1", "Forum"), N("p2", "Person"), N("cm", "Comment"), N("p3", "Person"),
     N("m2", *MESSAGE), N("post2", "Post"), N("f2", "Forum")],
    [S("m1", "tag", "HAS_TAG"), S("m1", "p1", "HAS_CREATOR"),
     S("m1", "post1", "REPLY_OF", hops=CLOSURE), S("f1", "post1", "CONTAINER_OF"),
     S("f1", "p2", "HAS_MEMBER"), S("cm", "p2", "HAS_CREATOR"), S("cm", "tag", "HAS_TAG"),
     S("cm", "m2", "REPLY_OF"), S("m2", "p3", "HAS_CREATOR"), S("f1", "p3", "HAS_MEMBER"),
     S("m2", "post2", "REPLY_OF", hops=CLOSURE), S("f2", "post2", "CONTAINER_OF"),
     S("m2", "tag", "HAS_TAG")],
    {"group": {"by": [{"as": "personId", "expr": col("p1.uid")}],
               "aggs": [{"as": "messageCount", "fn": "count_distinct", "of": col("m2.uid")}]},
     "order_by": ORD((col("messageCount"), "desc"), (asint(col("personId")), "asc")),
     "limit": 10},
    where=[name_is("tag", "$tag"), cmp("!=", col("f1.uid"), col("f2.uid")),
           cmp(">", col("m2.vt_s"), {"arith": "+", "l": col("m1.vt_s"),
                                      "r": lit("$delta", "int")})],
    not_=[{"steps": [S("f2", "p1", "HAS_MEMBER")]}],
    params={"tag": {"pick": ["tags", 0]}, "delta": 4 * 3_600_000_000})]

SNB["BI18"] = [M(
    [N("t", "Tag"), N("p1", "Person"), N("mu", "Person"), N("p2", "Person")],
    [S("p1", "t", "HAS_INTEREST"), S("p1", "mu", "KNOWS", dir="both"),
     S("mu", "p2", "KNOWS", dir="both"), S("p2", "t", "HAS_INTEREST")],
    {"group": {"by": [{"as": "person1Id", "expr": col("p1.uid")},
                      {"as": "person2Id", "expr": col("p2.uid")}],
               "aggs": [{"as": "mutualFriendCount", "fn": "count_distinct",
                         "of": col("mu.uid")}]},
     "order_by": ORD((col("mutualFriendCount"), "desc"), (asint(col("person1Id")), "asc"),
                   (asint(col("person2Id")), "asc")),
     "limit": 20},
    where=[name_is("t", "$tag"), cmp("!=", col("p1.uid"), col("p2.uid"))],
    not_=[{"steps": [S("p1", "p2", "KNOWS", dir="both")]}],
    params={"tag": {"pick": ["tags", 0]}})]


SNB["IC3"] = [M(
    [N("p", "Person", uids="$person"), N("f", "Person"), N("ci", "City"), N("cn", "Country"),
     N("m", *MESSAGE), N("mc", "Country")],
    [S("p", "f", "KNOWS", dir="both", hops={"min": 1, "max": 2}), S("f", "ci", "IS_LOCATED_IN"),
     S("ci", "cn", "IS_PART_OF"), S("m", "f", "HAS_CREATOR"), S("m", "mc", "IS_LOCATED_IN")],
    {"group": {"by": by(personId=col("f.uid"), firstName=tp("f", "firstName"),
                        lastName=tp("f", "lastName")),
               "aggs": [agg("xCount", "count", where=name_is("mc", "$countryX")),
                        agg("yCount", "count", where=name_is("mc", "$countryY"))],
               "project": by(count=arith("+", col("xCount"), col("yCount"))),
               "having": AND(cmp(">", col("xCount"), lit(0)), cmp(">", col("yCount"), lit(0)))},
     "order_by": ORD((col("xCount"), "desc"), (asint(col("personId")), "asc")), "limit": 20},
    where=[cmp("!=", tp("cn", "name"), lit("$countryX")),
           cmp("!=", tp("cn", "name"), lit("$countryY")),
           between(col("m.vt_s"), "$start", "$end"),
           OR(name_is("mc", "$countryX"), name_is("mc", "$countryY"))],
    params={"person": BUSY_PERSON, "countryX": {"pick": ["countries", 0]},
            "countryY": {"pick": ["countries", 1]}, "start": {"date": "2010-06-01"},
            "end": {"date": "2012-06-01"}})]

# LDBC IC4: tags of the friends' posts created in [start, end) that no friend's post created before start carries.
# "Not used before" is a `not` sub-pattern, so under a window it is tested per instant against the recent binding
#: a window-wide count of earlier posts dropped a tag whose earlier post never shares an instant with the
# recent one (G12 verdict 1).
SNB["IC4"] = [M(
    [N("p", "Person", uids="$person"), N("f", "Person"), N("po", "Post"), N("t", "Tag")],
    [S("p", "f", "KNOWS", dir="both"), S("po", "f", "HAS_CREATOR"), S("po", "t", "HAS_TAG")],
    {"group": {"by": by(tagName=tp("t", "name")), "aggs": [agg("postCount", "count")]},
     "order_by": ORD((col("postCount"), "desc"), (col("tagName"), "asc")), "limit": 10},
    where=[cmp(">=", col("po.vt_s"), lit("$start", "ts")), cmp("<", col("po.vt_s"), lit("$end", "ts"))],
    not_=[{"nodes": [N("f2", "Person"), N("po2", "Post")],
           "steps": [S("p", "f2", "KNOWS", dir="both"), S("po2", "f2", "HAS_CREATOR"), S("po2", "t", "HAS_TAG")],
           "where": [cmp("<", col("po2.vt_s"), lit("$start", "ts"))]}],
    params={"person": BUSY_PERSON, "start": {"date": "2012-01-01"},
            "end": {"date": "2012-04-01"}})]

BDAY = tp("f", "birthday", "ts")
SNB["IC10"] = [M(
    [N("p", "Person", uids="$person"), N("f", "Person"), N("ci", "City")],
    [S("p", "f", "KNOWS", dir="both", hops={"min": 2, "max": 2}), S("f", "ci", "IS_LOCATED_IN")],
    {"group": {"by": by(personId=col("f.uid"), firstName=tp("f", "firstName"),
                        lastName=tp("f", "lastName"), cityName=tp("ci", "name")),
               "aggs": [agg("posts", "count_distinct", col("po.uid")),
                        agg("common", "count_distinct", col("pc.uid"))],
               "project": by(commonInterestScore=arith(
                   "-", arith("*", lit(2), col("common")), col("posts")))},
     "order_by": ORD((col("commonInterestScore"), "desc"), (asint(col("personId")), "asc")),
     "limit": 10},
    where=[OR(AND(cmp("=", cal("month", BDAY), lit("$month")),
                  cmp(">=", cal("day", BDAY), lit(21))),
              AND(cmp("=", cal("month", BDAY), lit("$nextMonth")),
                  cmp("<", cal("day", BDAY), lit(22))))],
    optional=[{"nodes": [N("po", "Post")], "steps": [S("po", "f", "HAS_CREATOR")]},
              {"nodes": [N("pc", "Post"), N("tc", "Tag")],
               "steps": [S("pc", "f", "HAS_CREATOR"), S("pc", "tc", "HAS_TAG"),
                         S("p", "tc", "HAS_INTEREST")]}],
    params={"person": BUSY_PERSON, "month": 5, "nextMonth": 6})]

LEN = tp("m", "length", "int")
HAS_CONTENT = {"not": {"is_null": tp("m", "content")}}   # LDBC BI1: content IS NOT NULL in the groups
SNB["BI1"] = [M(
    [N("m", *MESSAGE)], [],
    {"group": {"by": by(year=cal("year", col("m.vt_s")),
                        isComment=cmp("=", col("m.label"), lit("Comment")),
                        lengthCategory=IF(cmp("<", LEN, lit(40)), lit(0),
                                          IF(cmp("<", LEN, lit(80)), lit(1),
                                             IF(cmp("<", LEN, lit(160)), lit(2), lit(3))))),
               "aggs": [agg("allMessages", "count"),
                        agg("messageCount", "count", where=HAS_CONTENT),
                        agg("averageMessageLength", "mean", LEN, where=HAS_CONTENT),
                        agg("sumMessageLength", "sum", LEN, where=HAS_CONTENT)],
               "project": by(percentageOfMessages=arith(
                   "/", col("messageCount"), {"total": "allMessages"})),
               "having": cmp(">", col("messageCount"), lit(0))},
     "order_by": ORD((col("year"), "desc"), (col("isComment"), "asc"),
                   (col("lengthCategory"), "asc"))},
    where=[cmp("<", col("m.vt_s"), lit("$datetime", "ts"))],
    params={"datetime": {"date": "2012-01-01"}})]

BI2_D1 = _date("2011-03-01")
SNB["BI2"] = [M(
    [N("tc", "TagClass"), N("t", "Tag")], [S("t", "tc", "HAS_TYPE")],
    {"group": {"by": by(tagName=tp("t", "name")),
               "aggs": [agg("countWindow1", "count", col("m.uid"),
                            where=between(col("m.vt_s"), "$d1", "$d2")),
                        agg("countWindow2", "count", col("m.uid"),
                            where=between(col("m.vt_s"), "$d2", "$d3"))],
               "project": by(diff=fn("abs", arith("-", col("countWindow1"),
                                                   col("countWindow2"))))},
     "order_by": ORD((col("diff"), "desc"), (col("tagName"), "asc")), "limit": 100},
    where=[name_is("tc", "$tagClass")],
    optional=[{"nodes": [N("m", *MESSAGE)], "steps": [S("m", "t", "HAS_TAG")],
               "where": [between(col("m.vt_s"), "$d1", "$d3")]}],
    params={"tagClass": {"pick": ["tagclasses", 0]}, "d1": BI2_D1,
            "d2": BI2_D1 + 100 * DAY, "d3": BI2_D1 + 200 * DAY})]

SNB["BI5"] = [M(
    [N("t", "Tag"), N("m", *MESSAGE), N("p", "Person")],
    [S("m", "t", "HAS_TAG"), S("m", "p", "HAS_CREATOR")],
    {"group": {"by": by(personId=col("p.uid")),
               "aggs": [agg("replyCount", "count_distinct", col("c.uid")),
                        agg("likeCount", "count_distinct", col("lk.eid")),
                        agg("messageCount", "count_distinct", col("m.uid"))],
               "project": by(score=arith("+", arith("+", col("messageCount"),
                                                    arith("*", lit(2), col("replyCount"))),
                                         arith("*", lit(10), col("likeCount"))))},
     "order_by": ORD((col("score"), "desc"), (asint(col("personId")), "asc")), "limit": 100},
    where=[name_is("t", "$tag")],
    optional=[{"nodes": [N("c", "Comment")], "steps": [S("c", "m", "REPLY_OF")]},
              {"nodes": [N("liker", "Person")], "steps": [S("liker", "m", "LIKES", var="lk")]}],
    params={"tag": {"pick": ["tags", 0]}})]

# LDBC BI13: months = 12*(year(endDate) - year(creation)) + (month(endDate) - month(creation)) + 1;
# messages created by endDate; likes counted from persons (zombies, for the zombie count)
# created before endDate.
BI13_MONTHS = arith("+", arith("+", arith("*", lit(12), arith("-", cal("year", lit("$endDate", "ts")),
                                                              cal("year", col("p.vt_s")))),
                               arith("-", cal("month", lit("$endDate", "ts")), cal("month", col("p.vt_s")))),
                    lit(1))
BI13_ZOMBIES = M(
    [N("p", "Person"), N("ci", "City"), N("cn", "Country")],
    [S("p", "ci", "IS_LOCATED_IN"), S("ci", "cn", "IS_PART_OF")],
    {"group": {"by": by(zombieId=col("p.uid"),
                        months=BI13_MONTHS),
               "aggs": [agg("messageCount", "count", col("m.uid"))],
               "having": cmp("<", col("messageCount"), col("months"))}},
    where=[name_is("cn", "$country"), cmp("<", col("p.vt_s"), lit("$endDate", "ts"))],
    optional=[{"nodes": [N("m", *MESSAGE)], "steps": [S("m", "p", "HAS_CREATOR")],
               "where": [cmp("<=", col("m.vt_s"), lit("$endDate", "ts"))]}],
    params={"country": {"pick": ["countries", 0]}, "endDate": {"date": "2012-06-01"}})
SNB["BI13"] = [BI13_ZOMBIES, M(
    [N("z", "Person", uid_list="$zombies")], [],
    {"group": {"by": by(zombieId=col("z.uid")),
               "aggs": [agg("zombieLikeCount", "count_distinct", col("lz.eid")),
                        agg("totalLikeCount", "count_distinct", col("la.eid"))],
               "project": by(zombieScore=IF(cmp("=", col("totalLikeCount"), lit(0)), lit(0.0),
                                            arith("/", col("zombieLikeCount"),
                                                  col("totalLikeCount"))))},
     "order_by": ORD((col("zombieScore"), "desc"), (asint(col("zombieId")), "asc")),
     "limit": 100},
    optional=[{"nodes": [N("m", *MESSAGE), N("liker", "Person")],
               "steps": [S("m", "z", "HAS_CREATOR"), S("liker", "m", "LIKES", var="la")],
               "where": [cmp("<", col("liker.vt_s"), lit("$endDate", "ts"))]},
              {"nodes": [N("m2", *MESSAGE), N("zl", "Person", uid_list="$zombies")],
               "steps": [S("m2", "z", "HAS_CREATOR"), S("zl", "m2", "LIKES", var="lz")],
               "where": [cmp("<", col("zl.vt_s"), lit("$endDate", "ts"))]}],
    params={"zombies": {"probe": BI13_ZOMBIES, "col": "zombieId", "all": True},
            "endDate": {"date": "2012-06-01"}})]


def busiest_day(i: int) -> dict:
    """The day (UTC midnight) with the most messages carrying tag `i`."""
    return {"probe": M(
        [N("m", *MESSAGE), N("t", "Tag")], [S("m", "t", "HAS_TAG")],
        {"group": {"by": by(day=arith("*", arith("floor_div", col("m.vt_s"), lit(DAY)),
                                      lit(DAY))),
                   "aggs": [agg("n", "count")]},
         "order_by": ORD((col("n"), "desc")), "limit": 1},
        where=[name_is("t", "$tag")], params={"tag": {"pick": ["tags", i]}}),
        "col": "day", "i": 0}


def _bi16_side(k: str) -> dict:
    q, mq, tq = f"q{k}", f"m{k}", f"t{k}"
    return {"nodes": [N(q, "Person"), N(mq, *MESSAGE), N(tq, "Tag")],
            "steps": [S("p", q, "KNOWS", dir="both"), S(mq, q, "HAS_CREATOR"),
                      S(mq, tq, "HAS_TAG")],
            "where": [name_is(tq, f"$tag{k.upper()}"), on_day(col(f"{mq}.vt_s"), f"$day{k.upper()}")]}


SNB["BI16"] = [M(
    [N("p", "Person"), N("m", *MESSAGE), N("t", "Tag")],
    [S("m", "p", "HAS_CREATOR"), S("m", "t", "HAS_TAG")],
    {"group": {"by": by(personId=col("p.uid")),
               "aggs": [agg("messageCountA", "count_distinct", col("m.uid"),
                            where=AND(name_is("t", "$tagA"), on_day(col("m.vt_s"), "$dayA"))),
                        agg("messageCountB", "count_distinct", col("m.uid"),
                            where=AND(name_is("t", "$tagB"), on_day(col("m.vt_s"), "$dayB"))),
                        agg("friendsA", "count_distinct", col("qa.uid")),
                        agg("friendsB", "count_distinct", col("qb.uid"))],
               "project": by(total=arith("+", col("messageCountA"), col("messageCountB"))),
               "having": AND(AND(cmp(">", col("messageCountA"), lit(0)),
                                 cmp(">", col("messageCountB"), lit(0))),
                             AND(cmp("<=", col("friendsA"), lit("$maxKnowsLimit")),
                                 cmp("<=", col("friendsB"), lit("$maxKnowsLimit"))))},
     "order_by": ORD((col("total"), "desc"), (asint(col("personId")), "asc")), "limit": 20},
    where=[OR(AND(name_is("t", "$tagA"), on_day(col("m.vt_s"), "$dayA")),
              AND(name_is("t", "$tagB"), on_day(col("m.vt_s"), "$dayB")))],
    optional=[_bi16_side("a"), _bi16_side("b")],
    params={"tagA": {"pick": ["tags", 0]}, "tagB": {"pick": ["tags", 1]},
            "dayA": busiest_day(0), "dayB": busiest_day(1), "maxKnowsLimit": 5})]


# LDBC IC7: per liker, the latest like (ties by message id) is chosen first, as its own stage; isNew then
# attaches as the optional KNOWS (the window reading can give a liker both values, so the optional must not
# take part in the per-liker choice). The 20 likers come from a probe stage; the latest like per liker from two
# lookups (its time, then the least message id at that time).
def _ic7_likes(q: str, m: str, k: str, l: str) -> dict:
    return {"nodes": [N(q, "Person", uids="$person"), N(m, *MESSAGE), N(k, "Person")],
            "steps": [S(m, q, "HAS_CREATOR"), S(k, m, "LIKES", var=l)]}


IC7_LATEST = lambda on, name: {"as": name, "on": col(on), "key": "p", "match": {
    "pattern": _ic7_likes("q2", "m2", "k2", "l2"),
    "group": {"by": by(p=col("k2.uid")), "aggs": [agg("t", "max", col("l2.vt_s"))]}}}
IC7_LATEST_MSG = {"as": "lm", "on": col("liker.uid"), "key": "p", "match": {
    "pattern": {**_ic7_likes("q3", "m3", "k3", "l3"), "where": [cmp("=", col("l3.vt_s"), col("lt3.t"))],
                "lookups": [IC7_LATEST("k3.uid", "lt3")]},
    "group": {"by": by(p=col("k3.uid")), "aggs": [agg("mid", "min", asint(col("m3.uid")))]}}}
IC7_LIKERS = {"probe": M(
    [N("q", "Person", uids="$person"), N("m", *MESSAGE), N("k", "Person")],
    [S("m", "q", "HAS_CREATOR"), S("k", "m", "LIKES", var="l")],
    {"group": {"by": by(liker=col("k.uid")), "aggs": [agg("t", "max", col("l.vt_s"))]},
     "order_by": ORD((col("t"), "desc"), (asint(col("liker")), "asc")), "limit": 20},
    params={"person": BUSY_PERSON}), "col": "liker", "all": True}
SNB["IC7"] = [M(
    [N("p", "Person", uids="$person"), N("m", *MESSAGE), N("liker", "Person", uid_list="$likers")],
    [S("m", "p", "HAS_CREATOR"), S("liker", "m", "LIKES", var="l")],
    {"rows": R(personId=col("liker.uid"), firstName=tp("liker", "firstName"),
               lastName=tp("liker", "lastName"), likeCreationDate=col("l.vt_s"),
               messageId=col("m.uid"),
               minutesLatency=arith("floor_div", arith("-", col("l.vt_s"), col("m.vt_s")),
                                    lit(60_000_000)),
               isNew={"is_null": col("kk.eid")}),
     "order_by": ORD((col("likeCreationDate"), "desc"), (asint(col("personId")), "asc"))},
    where=[cmp("=", col("l.vt_s"), col("lt.t")), cmp("=", asint(col("m.uid")), col("lm.mid"))],
    optional=[{"steps": [S("liker", "p", "KNOWS", var="kk", dir="both")]}],
    params={"person": BUSY_PERSON, "likers": IC7_LIKERS})]
SNB["IC7"][0]["pattern"]["lookups"] = [IC7_LATEST("liker.uid", "lt"), IC7_LATEST_MSG]

FIRST_NAME_NEAR = {"probe": M(
    [N("p", "Person", uids="$person"), N("f", "Person")],
    [S("p", "f", "KNOWS", dir="both", hops={"min": 1, "max": 3})],
    {"group": {"by": by(firstName=tp("f", "firstName")), "aggs": [agg("n", "count")]},
     "order_by": ORD((col("n"), "desc")), "limit": 1},
    params={"person": BUSY_PERSON}), "col": "firstName", "i": 0}
SNB["IC1"] = [M(
    [N("p", "Person", uids="$person"), N("f", "Person")],
    [S("p", "f", "KNOWS", dir="both", hops={"min": 1, "max": 3})],
    {"group": {"by": by(personId=col("f.uid"), distanceFromPerson=col("f.depth"),
                        lastName=tp("f", "lastName"), birthday=tp("f", "birthday", "ts")),
               "aggs": [agg("universities", "collect",
                            [tp("u", "name"), tp("sa", "classYear", "int"), tp("uc", "name")],
                            distinct=True),
                        agg("companies", "collect",
                            [tp("co", "name"), tp("wa", "workFrom", "int"), tp("cc", "name")],
                            distinct=True)]},
     "order_by": ORD((col("distanceFromPerson"), "asc"), (col("lastName"), "asc"),
                   (asint(col("personId")), "asc")),
     "limit": 20},
    where=[cmp("=", tp("f", "firstName"), lit("$firstName"))],
    optional=[{"nodes": [N("u", "University"), N("uc", "City")],
               "steps": [S("f", "u", "STUDY_AT", var="sa"), S("u", "uc", "IS_LOCATED_IN")]},
              {"nodes": [N("co", "Company"), N("cc", "Country")],
               "steps": [S("f", "co", "WORK_AT", var="wa"), S("co", "cc", "IS_LOCATED_IN")]}],
    params={"person": BUSY_PERSON, "firstName": FIRST_NAME_NEAR})]

SNB["IC12"] = [M(
    [N("p", "Person", uids="$person"), N("f", "Person"), N("c", "Comment"), N("po", "Post"),
     N("t", "Tag"), N("tc", "TagClass"), N("bc", "TagClass")],
    [S("p", "f", "KNOWS", dir="both"), S("c", "f", "HAS_CREATOR"), S("c", "po", "REPLY_OF"),
     S("po", "t", "HAS_TAG"), S("t", "tc", "HAS_TYPE"),
     S("tc", "bc", "IS_SUBCLASS_OF", hops=CLOSURE)],
    {"group": {"by": by(personId=col("f.uid"), firstName=tp("f", "firstName"),
                        lastName=tp("f", "lastName")),
               "aggs": [agg("tagNames", "collect", tp("t", "name"), distinct=True),
                        agg("replyCount", "count_distinct", col("c.uid"))]},
     "order_by": ORD((col("replyCount"), "desc"), (asint(col("personId")), "asc")),
     "limit": 20},
    where=[name_is("bc", "$tagClass")],
    params={"person": BUSY_PERSON, "tagClass": {"pick": ["tagclasses", 0]}})]


def _bi14_branch(kind: int, points: int, steps: list[dict], nodes: list[dict]) -> dict:
    base_nodes = [N("cn1", "Country"), N("ci1", "City"), N("p1", "Person"), N("p2", "Person"),
                  N("ci2", "City"), N("cn2", "Country")]
    base_steps = [S("ci1", "cn1", "IS_PART_OF"), S("p1", "ci1", "IS_LOCATED_IN"),
                  S("p1", "p2", "KNOWS", dir="both"), S("p2", "ci2", "IS_LOCATED_IN"),
                  S("ci2", "cn2", "IS_PART_OF")]
    return {"pattern": {"nodes": base_nodes + nodes, "steps": base_steps + steps,
                        "where": [name_is("cn1", "$country1"), name_is("cn2", "$country2")]},
            "rows": R(person1Id=col("p1.uid"), person2Id=col("p2.uid"),
                      city1Name=tp("ci1", "name"), kind=lit(kind), points=lit(points))}


SNB["BI14"] = [{
    "union": [
        _bi14_branch(0, 0, [], []),
        _bi14_branch(1, 4, [S("c", "p1", "HAS_CREATOR"), S("c", "m", "REPLY_OF"),
                            S("m", "p2", "HAS_CREATOR")],
                     [N("c", "Comment"), N("m", *MESSAGE)]),
        _bi14_branch(2, 1, [S("c", "p2", "HAS_CREATOR"), S("c", "m", "REPLY_OF"),
                            S("m", "p1", "HAS_CREATOR")],
                     [N("c", "Comment"), N("m", *MESSAGE)]),
        _bi14_branch(3, 10, [S("p1", "m", "LIKES"), S("m", "p2", "HAS_CREATOR")],
                     [N("m", *MESSAGE)]),
        _bi14_branch(4, 1, [S("p2", "m", "LIKES"), S("m", "p1", "HAS_CREATOR")],
                     [N("m", *MESSAGE)])],
    "return": {
        "group": {"by": by(person1Id=col("person1Id"), person2Id=col("person2Id"),
                           city1Name=col("city1Name"), kind=col("kind")),
                  "aggs": [agg("points", "max", col("points"))],
                  "then": {"by": by(person1Id=col("person1Id"), person2Id=col("person2Id"),
                                    city1Name=col("city1Name")),
                           "aggs": [agg("score", "sum", col("points"))]}},
        "top_per": {"by": [col("city1Name")],
                    "order_by": ORD((col("score"), "desc"), (asint(col("person1Id")), "asc"),
                                    (asint(col("person2Id")), "asc")), "k": 1},
        "order_by": ORD((col("score"), "desc"), (asint(col("person1Id")), "asc"),
                        (asint(col("person2Id")), "asc")),
        "limit": 100},                                  # LDBC BI14: the top 100 pairs
    "scope": NOW_SCOPE,
    "params": {"country1": {"pick": ["countries", 0]}, "country2": {"pick": ["countries", 1]}}}]


def _bi8_lookups(var: str, tag: str) -> list[dict]:
    """A person's interest in the tag (0/1) and messages with it created in (startDate, endDate) (LDBC BI8)."""
    return [
        {"as": f"{tag}i", "on": col(f"{var}.uid"), "key": "p", "match": {
            "pattern": {"nodes": [N("q", "Person"), N("t", "Tag")],
                        "steps": [S("q", "t", "HAS_INTEREST")], "where": [name_is("t", "$tag")]},
            "group": {"by": by(p=col("q.uid")), "aggs": [agg("n", "count")]}}},
        {"as": f"{tag}m", "on": col(f"{var}.uid"), "key": "p", "match": {
            "pattern": {"nodes": [N("q", "Person"), N("m", *MESSAGE), N("t", "Tag")],
                        "steps": [S("m", "q", "HAS_CREATOR"), S("m", "t", "HAS_TAG")],
                        "where": [name_is("t", "$tag"),
                                  cmp(">", col("m.vt_s"), lit("$startDate", "ts")),
                                  cmp("<", col("m.vt_s"), lit("$endDate", "ts"))]},
            "group": {"by": by(p=col("q.uid")), "aggs": [agg("n", "count")]}}}]


def _bi8_score(tag: str) -> dict:
    return arith("+", arith("*", lit(100), {"coalesce": [col(f"{tag}i.n"), lit(0)]}),
                 {"coalesce": [col(f"{tag}m.n"), lit(0)]})


BI8_PARAMS = {"tag": {"pick": ["tags", 0]}, "startDate": {"date": "2011-06-01"},
              "endDate": {"date": "2012-06-01"}}
# LDBC BI8: friendsScore is computed only for the persons with a score from the tag. The scored
# persons come first as one uid list (a probe call), and they seed both the main pattern and the
# friends' lookup, so no friend score is computed for a person who cannot be in the answer.
BI8_SCORED = {"probe": {
    "pattern": {"nodes": [N("p", "Person")], "steps": [],
                "where": [cmp(">", _bi8_score("own"), lit(0))],
                "lookups": _bi8_lookups("p", "own")},
    "return": {"rows": R(uid=col("p.uid"))},
    "scope": NOW_SCOPE, "params": BI8_PARAMS}, "col": "uid", "all": True}
SNB["BI8"] = [{
    "pattern": {"nodes": [N("p", "Person", uid_list="$scored")], "steps": [],
                "where": [cmp(">", _bi8_score("own"), lit(0))],
                "lookups": _bi8_lookups("p", "own") + [
                    {"as": "fs", "on": col("p.uid"), "key": "p", "match": {
                        "pattern": {"nodes": [N("p2", "Person", uid_list="$scored"), N("f", "Person")],
                                    "steps": [S("p2", "f", "KNOWS", dir="both")],
                                    "lookups": _bi8_lookups("f", "fr")},
                        "group": {"by": by(p=col("p2.uid")),
                                  "aggs": [agg("total", "sum", _bi8_score("fr"))]}}}]},
    "return": {"rows": R(personId=col("p.uid"), score=_bi8_score("own"),
                         friendsScore={"coalesce": [col("fs.total"), lit(0)]},
                         total=arith("+", _bi8_score("own"),
                                     {"coalesce": [col("fs.total"), lit(0)]})),
               "order_by": ORD((col("total"), "desc"), (asint(col("personId")), "asc")),
               "limit": 100},
    "scope": NOW_SCOPE,
    "params": {**BI8_PARAMS, "scored": BI8_SCORED}}]


def cascade(seed_var_nodes, steps, where=None, params=None) -> dict:
    return M(seed_var_nodes, steps,
             {"group": {"by": [{"as": "uid", "expr": col("c.uid")}], "aggs": []}},
             where=where, params=params)


SNB["DEL1"] = [
    cascade([N("p", "Person", uids="$person"), N("m", *MESSAGE), N("c", *MESSAGE)],
            [S("m", "p", "HAS_CREATOR"), S("c", "m", "REPLY_OF", hops=CLOSURE)],
            params={"person": MODERATOR}),
    cascade([N("p", "Person", uids="$person"), N("fo", "Forum"), N("po", "Post"),
             N("c", *MESSAGE)],
            [S("fo", "p", "HAS_MODERATOR"), S("fo", "po", "CONTAINER_OF"),
             S("c", "po", "REPLY_OF", hops=CLOSURE)],
            where=[OR({"starts_with": [tp("fo", "title"), lit("Album ")]},
                      {"starts_with": [tp("fo", "title"), lit("Wall ")]})],
            params={"person": MODERATOR}),
]
SNB["DEL1.u"] = [{
    "union": [{"pattern": q["pattern"], "rows": [{"as": "uid", "expr": col("c.uid")}]}
              for q in SNB["DEL1"]],
    "return": {"group": {"by": [{"as": "uid", "expr": col("uid")}], "aggs": []}},
    "scope": NOW_SCOPE, "params": {"person": MODERATOR}}]
SNB["DEL4"] = [cascade([N("fo", "Forum", uids="$forum"), N("po", "Post"), N("c", *MESSAGE)],
                       [S("fo", "po", "CONTAINER_OF"), S("c", "po", "REPLY_OF", hops=CLOSURE)],
                       params={"forum": BUSY_FORUM})]
SNB["DEL6"] = [cascade([N("po", "Post", uids="$post"), N("c", *MESSAGE)],
                       [S("c", "po", "REPLY_OF", hops=CLOSURE)],
                       params={"post": POST_WITH_REPLIES})]
SNB["DEL7"] = [cascade([N("cm", "Comment", uids="$comment"), N("c", *MESSAGE)],
                       [S("c", "cm", "REPLY_OF", hops=CLOSURE)],
                       params={"comment": COMMENT_WITH_REPLIES})]

#: AeonG's temporal workloads: the same shapes as of a past belief time.
PAST = {"vt": {"instant": "$now"}, "as_of_tt": "$asof"}
AEONG: dict[str, list[dict]] = {}
for _name, _fn in (("T-LDBC-IS2", is2), ("T-LDBC-IS3", is3), ("T-LDBC-IS6", is6),
                   ("T-LDBC-IS7", is7)):
    _q = _fn(PAST)
    _q["params"]["asof"] = {"tt": 0.9}
    AEONG[_name] = [_q]
AEONG["T-mgBench-Q3"] = [M(
    [N("n", "Person", uids="$person"), N("m")], [S("n", "m", None, var="e")],
    {"rows": R(nProps=col("n.props"), relType=col("e.rel_type"), eProps=col("e.props"),
               mId=col("m.uid"), mLabel=col("m.label"), mProps=col("m.props"))},
    scope=PAST, params={"person": BUSY_PERSON, "asof": {"tt": 0.9}})]

AEONG["T-mgBench-Q4"] = [M(
    [N("n", "Person", uids="$person"), N("m")], [S("n", "m", None, var="e")],
    {"rows": R(relType=col("e.rel_type"), eProps=col("e.props"), mId=col("m.uid"),
               mLabel=col("m.label"), tt_s=col("belief.s"), tt_e=col("belief.e"))},
    where=[cmp(">", col("belief.e"), lit("$t1"))],
    scope={"vt": {"instant": "$now"}, "as_of_tt": "$t2", "belief": "all",
           "co_valid": ["transaction"]},
    params={"person": BUSY_PERSON, "t1": {"tt": 0.3}, "t2": {"tt": 0.9}})]

# ---------------------------------------------------------------------------
# FinBench-shaped rows
# ---------------------------------------------------------------------------

WINDOW = [cmp(">", col("e.vt_s"), lit("$start", "ts")), cmp("<", col("e.vt_s"), lit("$end", "ts"))]
FIN_WINDOW = {"start": {"date": "2022-03-01"}, "end": {"date": "2022-11-01"}}
BUSY_ACCOUNT = {"probe": M(
    [N("a", "Account"), N("b", "Account")], [S("a", "b", "TRANSFER")],
    {"group": {"by": [{"as": "a", "expr": col("a.uid")}], "aggs": [{"as": "n", "fn": "count"}]},
     "order_by": ORD((col("n"), "desc")), "limit": 1}), "col": "a", "i": 0}
def star(direction: str) -> dict:
    a, b = ("acc", "x") if direction == "out" else ("x", "acc")
    return M([N("acc", "Account", uids="$account"), N("x", "Account")],
             [S(a, b, "TRANSFER", var="e")],
             {"group": {"by": [],
                        "aggs": [{"as": "sumAmount", "fn": "sum", "of": tp("e", "amount", "float")},
                                 {"as": "maxAmount", "fn": "max", "of": tp("e", "amount", "float")},
                                 {"as": "count", "fn": "count"}]}},
             where=WINDOW, params={"account": BUSY_ACCOUNT, **FIN_WINDOW})


def grouped(direction: str) -> dict:
    a, b = ("acc", "x") if direction == "out" else ("x", "acc")
    return M([N("acc", "Account", uids="$account"), N("x", "Account")],
             [S(a, b, "TRANSFER", var="e")],
             {"group": {"by": [{"as": "otherId", "expr": col("x.uid")}],
                        "aggs": [{"as": "numEdges", "fn": "count"},
                                 {"as": "sumAmount", "fn": "sum",
                                  "of": tp("e", "amount", "float")}]},
              "order_by": ORD((col("sumAmount"), "desc"), (col("otherId"), "asc"))},
             where=WINDOW + [cmp(">", tp("e", "amount", "float"), lit("$threshold"))],
             params={"account": BUSY_ACCOUNT, "threshold": 100.0, **FIN_WINDOW})


def tcr4(count_only: bool = False) -> dict:
    by = [] if count_only else [{"as": "otherId", "expr": col("o.uid")}]
    aggs = [{"as": "n", "fn": "count"}] if count_only else [
        {"as": "numEdge2", "fn": "count", "distinct_on": ["e2"]},
        {"as": "sumEdge2", "fn": "sum", "of": tp("e2", "amount", "float"), "distinct_on": ["e2"]},
        {"as": "maxEdge2", "fn": "max", "of": tp("e2", "amount", "float")},
        {"as": "numEdge3", "fn": "count", "distinct_on": ["e3"]},
        {"as": "sumEdge3", "fn": "sum", "of": tp("e3", "amount", "float"), "distinct_on": ["e3"]},
        {"as": "maxEdge3", "fn": "max", "of": tp("e3", "amount", "float")}]
    ret: dict[str, Any] = {"group": {"by": by, "aggs": aggs}}
    if not count_only:
        ret["order_by"] = ORD((col("sumEdge2"), "desc"), (col("sumEdge3"), "desc"),
                            (col("otherId"), "asc"))
    return M([N("s", "Account", uids="$src"), N("d", "Account", uids="$dst"), N("o", "Account")],
             [S("s", "d", "TRANSFER", var="e1"), S("d", "o", "TRANSFER", var="e2"),
              S("o", "s", "TRANSFER", var="e3")],
             ret,
             where=[cmp(">", col("e1.vt_s"), lit("$start", "ts")),
                    cmp("<", col("e1.vt_s"), lit("$end", "ts"))],
             params={"src": {"pick": ["cycle_src", 0]},
                     "dst": {"pick": ["cycle_dst", 0]}, **FIN_WINDOW})


AMT = tp("e", "amount", "float")
LOANED_ACCOUNT = {"probe": M(
    [N("l", "Loan"), N("a", "Account")], [S("l", "a", "DEPOSIT")],
    {"group": {"by": [{"as": "a", "expr": col("a.uid")}], "aggs": [{"as": "n", "fn": "count"}]},
     "order_by": ORD((col("n"), "desc")), "limit": 1}), "col": "a", "i": 0}
FED_BY_BLOCKED = {"probe": M(
    [N("b", "Account"), N("a", "Account")], [S("b", "a", "TRANSFER")],
    {"group": {"by": [{"as": "a", "expr": col("a.uid")}], "aggs": [{"as": "n", "fn": "count"}]},
     "order_by": ORD((col("n"), "desc")), "limit": 1},
    where=[cmp("=", tp("b", "isBlocked", "bool"), lit(True))]), "col": "a", "i": 0}


def investor(i: int) -> dict:
    return {"probe": M(
        [N("x"), N("c", "Company")], [S("x", "c", "INVEST")],
        {"group": {"by": [{"as": "x", "expr": col("x.uid")}],
                   "aggs": [{"as": "n", "fn": "count"}]},
         "order_by": ORD((col("n"), "desc")), "limit": 2}), "col": "x", "i": i}


INCOMING = cmp("=", col("e.dst"), col("acc.uid"))
OUTGOING = cmp("=", col("e.src"), col("acc.uid"))
REL = lambda r: cmp("=", col("e.rel_type"), lit(r))   # noqa: E731

#: FinBench's truncation: per node and hop, the first edges by time
TRUNCATION = {"limit": 100, "order_by": [{"expr": col("t.vt_s"), "dir": "desc"}]}
T_WINDOW = {"var": "t", "where": [cmp(">", col("t.vt_s"), lit("$start", "ts")),
                                  cmp("<", col("t.vt_s"), lit("$end", "ts"))]}
PERSON_WITH_ACCOUNTS = {"probe": M(
    [N("p", "Person"), N("a", "Account"), N("x", "Account")],
    [S("p", "a", "OWN"), S("x", "a", "TRANSFER")],
    {"group": {"by": [{"as": "p", "expr": col("p.uid")}], "aggs": [{"as": "n", "fn": "count"}]},
     "order_by": ORD((col("n"), "desc")), "limit": 1}), "col": "p", "i": 0}
GUARANTOR = {"probe": M(
    [N("p", "Person"), N("q", "Person")], [S("p", "q", "GUARANTEE")],
    {"group": {"by": [{"as": "p", "expr": col("p.uid")}], "aggs": [{"as": "n", "fn": "count"}]},
     "order_by": ORD((col("n"), "desc")), "limit": 1}), "col": "p", "i": 0}
WITHDRAWN_TO = {"probe": M(
    [N("m", "Account"), N("c", "Account")], [S("m", "c", "WITHDRAW")],
    {"group": {"by": [{"as": "c", "expr": col("c.uid")}], "aggs": [{"as": "n", "fn": "count"}]},
     "order_by": ORD((col("n"), "desc")), "limit": 1}), "col": "c", "i": 0}


def tcr7(account: Any) -> dict:
    return M([N("acc", "Account", uids="$account"), N("x", "Account")],
             [S("acc", "x", "TRANSFER", var="e", dir="both")],
             {"group": {"by": [],
                        "aggs": [agg("numSrc", "count_distinct", col("x.uid"), where=INCOMING),
                                 agg("numDst", "count_distinct", col("x.uid"), where=OUTGOING),
                                 agg("sumIn", "sum", AMT, where=INCOMING),
                                 agg("sumOut", "sum", AMT, where=OUTGOING)],
                        "project": by(inOutRatio=IF({"is_null": col("sumOut")}, lit(-1.0),
                                                    round3(arith("/", {"coalesce": [
                                                        col("sumIn"), lit(0.0)]},
                                                        col("sumOut")))))}},
             where=WINDOW + [cmp(">", AMT, lit("$threshold"))],
             params={"account": account, "threshold": 100.0, **FIN_WINDOW})


def _inflow(var: str, alias: str) -> dict:
    """An account's TRANSFER inflow in the window, as a lookup."""
    return {"as": alias, "on": col(f"{var}.uid"), "key": "a", "match": {
        "pattern": {"nodes": [N("x", "Account"), N("a", "Account")],
                    "steps": [S("x", "a", "TRANSFER", var="t")],
                    "where": [cmp(">", col("t.vt_s"), lit("$start", "ts")),
                              cmp("<", col("t.vt_s"), lit("$end", "ts"))]},
        "group": {"by": by(a=col("a.uid")),
                  "aggs": [agg("total", "sum", tp("t", "amount", "float"))]}}}


def _tcr8_branch(k: int) -> dict:
    """Loan -DEPOSIT-> a0 then `k` hops over TRANSFER or WITHDRAW, each hop's
    amount above the threshold's share of its source's inflow."""
    names = [f"a{i}" for i in range(k + 1)]
    nodes = [N("loan", "Loan", uids="$loan")] + [N(v, "Account") for v in names]
    steps = [S("loan", "a0", "DEPOSIT", var="dep")]
    where = [cmp(">", col("dep.vt_s"), lit("$start", "ts")),
             cmp("<", col("dep.vt_s"), lit("$end", "ts"))]
    lookups = []
    for i in range(k):
        e = f"h{i}"
        steps.append(S(names[i], names[i + 1], ["TRANSFER", "WITHDRAW"], var=e))
        where += [cmp(">", col(f"{e}.vt_s"), lit("$start", "ts")),
                  cmp("<", col(f"{e}.vt_s"), lit("$end", "ts")),
                  cmp(">", tp(e, "amount", "float"),
                      arith("*", {"coalesce": [col(f"in{i}.total"), lit(0.0)]},
                            lit("$threshold")))]
        lookups.append(_inflow(names[i], f"in{i}"))
    return {"pattern": {"nodes": nodes, "steps": steps, "where": where, "lookups": lookups},
            "rows": R(dstId=col(f"{names[-1]}.uid"), distance=lit(k),
                      amount=tp(f"h{k - 1}", "amount", "float"),
                      loanAmount=tp("loan", "loanAmount", "float"))}


def tcr11() -> dict:
    return M([N("p", "Person", uids="$person"), N("q", "Person"), N("l", "Loan")],
             [{**S("p", "q", "GUARANTEE", hops={"min": 1, "max": None}),
               "edge_where": T_WINDOW}, S("q", "l", "APPLY")],
             {"group": {"by": [],
                        "aggs": [agg("sumLoanAmount", "sum", tp("l", "loanAmount", "float"),
                                     distinct_on=["l"]),
                                 agg("numLoans", "count_distinct", col("l.uid"))]}},
             params={"person": GUARANTOR, "start": {"date": "2022-01-01"},
                     "end": {"date": "2022-12-31"}})


FIN: dict[str, list[dict]] = {
    "TCR1": [M([N("acc", "Account", uids="$account"), N("other", "Account"),
                N("med", "Medium")],
               [{**S("acc", "other", "TRANSFER", hops={"min": 1, "max": 3}),
                 "order": "strict_increasing", "edge_where": T_WINDOW, "truncate": TRUNCATION},
                S("med", "other", "SIGN_IN", var="si")],
               {"group": {"by": by(otherId=col("other.uid"),
                                   accountDistance=col("other.depth"),
                                   mediumId=col("med.uid"),
                                   mediumType=tp("med", "mediumType")), "aggs": []},
                "order_by": ORD((col("accountDistance"), "asc"), (col("otherId"), "asc"),
                                (col("mediumId"), "asc"))},
               where=[cmp("=", tp("med", "isBlocked", "bool"), lit(True)),
                      cmp(">", col("si.vt_s"), lit("$start", "ts")),
                      cmp("<", col("si.vt_s"), lit("$end", "ts"))],
               params={"account": BUSY_ACCOUNT, **FIN_WINDOW})],
    "TCR2": [M([N("p", "Person", uids="$person"), N("acc", "Account"), N("other", "Account"),
                N("loan", "Loan")],
               [S("p", "acc", "OWN"),
                {**S("other", "acc", "TRANSFER", hops={"min": 1, "max": 3}),
                 "order": "strict_increasing", "edge_where": T_WINDOW, "truncate": TRUNCATION},
                S("loan", "other", "DEPOSIT", var="dep")],
               {"group": {"by": by(otherId=col("other.uid")),
                          "aggs": [agg("sumLoanAmount", "sum", tp("loan", "loanAmount", "float"),
                                       distinct_on=["loan"]),
                                   agg("sumLoanBalance", "sum", tp("loan", "balance", "float"),
                                       distinct_on=["loan"])],
                          "project": by(sumAmount=round3(col("sumLoanAmount")),
                                        sumBalance=round3(col("sumLoanBalance")))},
                "order_by": ORD((col("sumLoanAmount"), "desc"), (col("otherId"), "asc"))},
               where=[cmp(">", col("dep.vt_s"), lit("$start", "ts")),
                      cmp("<", col("dep.vt_s"), lit("$end", "ts"))],
               params={"person": PERSON_WITH_ACCOUNTS, **FIN_WINDOW})],
    "TCR6": [M([N("card", "Account", uids="$card"), N("mid", "Account"), N("src", "Account")],
               [{**S("mid", "card", "WITHDRAW", var="e2"),
                 "truncate": {"limit": 100, "order_by": [{"expr": col("e2.vt_s"), "dir": "desc"}]}},
                {**S("src", "mid", "TRANSFER", var="e1"),
                 "truncate": {"limit": 100, "order_by": [{"expr": col("e1.vt_s"), "dir": "desc"}]}}],
               {"group": {"by": by(midId=col("mid.uid")),
                          "aggs": [agg("numSrc", "count_distinct", col("src.uid")),
                                   agg("sumEdge1Amount", "sum", tp("e1", "amount", "float"),
                                       distinct_on=["e1"]),
                                   agg("sumEdge2Amount", "sum", tp("e2", "amount", "float"),
                                       distinct_on=["e2"])],
                          "having": cmp(">", col("numSrc"), lit(3))},
                "order_by": ORD((col("sumEdge2Amount"), "desc"), (col("midId"), "asc"))},
               where=[cmp(">", tp("e1", "amount", "float"), lit("$threshold1")),
                      cmp(">", tp("e2", "amount", "float"), lit("$threshold2")),
                      cmp(">", col("e1.vt_s"), lit("$start", "ts")),
                      cmp("<", col("e1.vt_s"), lit("$end", "ts")),
                      cmp(">", col("e2.vt_s"), lit("$start", "ts")),
                      cmp("<", col("e2.vt_s"), lit("$end", "ts"))],
               params={"card": WITHDRAWN_TO, "threshold1": 100.0, "threshold2": 50.0,
                       **FIN_WINDOW})],
    "TCR8": [{
        "union": [_tcr8_branch(k) for k in (1, 2, 3)],
        "return": {"group": {"by": by(dstId=col("dstId")),
                             "aggs": [agg("minDistanceFromLoan", "min", col("distance")),
                                      agg("inflow", "sum", col("amount")),
                                      agg("loanAmount", "max", col("loanAmount"))],
                             "project": by(ratio=round3(arith("/", col("inflow"),
                                                              col("loanAmount"))))},
                   "order_by": ORD((col("minDistanceFromLoan"), "desc"), (col("ratio"), "desc"),
                                   (col("dstId"), "asc"))},
        "scope": NOW_SCOPE,
        "params": {"loan": {"probe": M(
            [N("l", "Loan"), N("a", "Account")], [S("l", "a", "DEPOSIT")],
            {"group": {"by": [{"as": "l", "expr": col("l.uid")}],
                       "aggs": [{"as": "n", "fn": "count"}]},
             "order_by": ORD((col("n"), "desc")), "limit": 1}), "col": "l", "i": 0},
            "threshold": 0.05, **FIN_WINDOW}}],
    "TCR11": [tcr11()],
    "TCR12": [M([N("p", "Person", uids="$person"), N("acc", "Account"), N("cacc", "Account"),
                 N("co", "Company")],
                [S("p", "acc", "OWN"),
                 {**S("acc", "cacc", "TRANSFER", var="e"),
                  "truncate": {"limit": 100, "order_by": [{"expr": col("e.vt_s"), "dir": "desc"}]}},
                 S("co", "cacc", "OWN")],
                {"group": {"by": by(compAccountId=col("cacc.uid")),
                           "aggs": [agg("sumEdge2Amount", "sum", AMT)],
                           "project": by(sumAmount=round3(col("sumEdge2Amount")))},
                 "order_by": ORD((col("sumEdge2Amount"), "desc"), (col("compAccountId"), "asc"))},
                where=WINDOW,
                params={"person": PERSON_WITH_ACCOUNTS, **FIN_WINDOW})],
    "TRW2": [tcr7({"probe": BUSY_ACCOUNT["probe"], "col": "a", "i": 0}),
             tcr7({"probe": M([N("a", "Account"), N("b", "Account")], [S("a", "b", "TRANSFER")],
                              {"group": {"by": [{"as": "b", "expr": col("b.uid")}],
                                         "aggs": [{"as": "n", "fn": "count"}]},
                               "order_by": ORD((col("n"), "desc")), "limit": 1}),
                   "col": "b", "i": 0})],
    "TRW3": [tcr11()],
    "TCR7": [M([N("acc", "Account", uids="$account"), N("x", "Account")],
               [S("acc", "x", "TRANSFER", var="e", dir="both")],
               {"group": {"by": [],
                          "aggs": [agg("numSrc", "count_distinct", col("x.uid"), where=INCOMING),
                                   agg("numDst", "count_distinct", col("x.uid"), where=OUTGOING),
                                   agg("sumIn", "sum", AMT, where=INCOMING),
                                   agg("sumOut", "sum", AMT, where=OUTGOING)],
                          "project": by(inOutRatio=IF({"is_null": col("sumOut")}, lit(-1.0),
                                                      round3(arith("/", {"coalesce": [
                                                          col("sumIn"), lit(0.0)]},
                                                          col("sumOut")))))}},
               where=WINDOW + [cmp(">", AMT, lit("$threshold"))],
               params={"account": BUSY_ACCOUNT, "threshold": 100.0, **FIN_WINDOW})],
    "TCR9": [M([N("acc", "Account", uids="$account"), N("x")],
               [S("acc", "x", None, var="e", dir="both")],
               {"group": {"by": [],
                          "aggs": [agg("sumLoanDeposit", "sum", AMT, where=REL("DEPOSIT")),
                                   agg("sumLoanRepay", "sum", AMT, where=REL("REPAY")),
                                   agg("sumTransferIn", "sum", AMT,
                                       where=AND(REL("TRANSFER"), INCOMING)),
                                   agg("sumTransferOut", "sum", AMT,
                                       where=AND(REL("TRANSFER"), OUTGOING))],
                          "project": by(
                              ratioRepay=ratio(col("sumLoanRepay"), col("sumLoanDeposit")),
                              ratioDeposit=ratio(col("sumLoanDeposit"), col("sumTransferOut")),
                              ratioTransfer=ratio(col("sumTransferIn"), col("sumTransferOut")))}},
               where=WINDOW + [cmp(">", AMT, lit("$threshold")),
                               OR(OR(REL("DEPOSIT"), REL("REPAY")), REL("TRANSFER"))],
               params={"account": LOANED_ACCOUNT, "threshold": 100.0, **FIN_WINDOW})],
    "TCR10": [M([N("x", uids=["$pid1", "$pid2"]), N("c", "Company")],
                [S("x", "c", "INVEST", var="e")],
                {"group": {"by": by(companyId=col("c.uid")),
                           "aggs": [agg("by1", "count", where=cmp("=", col("x.uid"), lit("$pid1"))),
                                    agg("by2", "count", where=cmp("=", col("x.uid"), lit("$pid2")))],
                           "then": {"by": [],
                                    "aggs": [agg("unionCount", "count"),
                                             agg("interCount", "count", where=AND(
                                                 cmp(">", col("by1"), lit(0)),
                                                 cmp(">", col("by2"), lit(0))))],
                                    "project": by(jaccardSimilarity=IF(
                                        cmp("=", col("unionCount"), lit(0)), lit(0.0),
                                        round3(arith("/", col("interCount"),
                                                     col("unionCount")))))}}},
                where=WINDOW,
                params={"pid1": investor(0), "pid2": investor(1),
                        "start": {"date": "2022-01-15"}, "end": {"date": "2022-03-15"}})],
    "TSR3": [M([N("acc", "Account", uids="$account"), N("src", "Account")],
               [S("src", "acc", "TRANSFER", var="e")],
               {"group": {"by": [],
                          "aggs": [agg("blocked", "count",
                                       where=cmp("=", tp("src", "isBlocked", "bool"), lit(True))),
                                   agg("all", "count")],
                          "project": by(blockRatio=IF(cmp("=", col("all"), lit(0)), lit(-1.0),
                                                      round3(arith("/", col("blocked"),
                                                                   col("all")))))}},
               where=WINDOW + [cmp(">", AMT, lit("$threshold"))],
               params={"account": FED_BY_BLOCKED, "threshold": 100.0, **FIN_WINDOW})],
    "TSR2": [star("out"), star("in")],
    "TSR2.u": [{
        "union": [{"pattern": q["pattern"],
                   "rows": [{"as": "direction", "expr": lit(d)},
                            {"as": "amount", "expr": tp("e", "amount", "float")}]}
                  for d, q in (("out", star("out")), ("in", star("in")))],
        "return": {"group": {"by": [{"as": "direction", "expr": col("direction")}],
                             "aggs": [{"as": "sumAmount", "fn": "sum", "of": col("amount")},
                                      {"as": "maxAmount", "fn": "max", "of": col("amount")},
                                      {"as": "count", "fn": "count"}]}},
        "scope": NOW_SCOPE, "params": {"account": BUSY_ACCOUNT, **FIN_WINDOW}}],
    "TSR4": [grouped("out")],
    "TSR5": [grouped("in")],
    "TSR6": [M([N("src", "Account", uids="$account"), N("mid", "Account"), N("dst", "Account")],
               [S("mid", "src", "TRANSFER", var="e"), S("mid", "dst", "TRANSFER", var="e2")],
               {"group": {"by": [{"as": "dstId", "expr": col("dst.uid")}], "aggs": []}},
               where=WINDOW + [cmp(">", col("e2.vt_s"), lit("$start", "ts")),
                               cmp("<", col("e2.vt_s"), lit("$end", "ts")),
                               cmp("=", tp("dst", "isBlocked", "bool"), lit(True)),
                               cmp("!=", col("dst.uid"), col("src.uid"))],
               params={"account": BUSY_ACCOUNT, **FIN_WINDOW})],
    "TCR4": [tcr4()],
    "TRW1": [tcr4(count_only=True)],
}

# ---------------------------------------------------------------------------
# TPC-BiH-shaped rows
# ---------------------------------------------------------------------------

APP = {"vt": {"instant": "$app"}, "as_of_tt": "$sys"}
BIH_P = {"app": {"date": "2021-06-01"}, "sys": {"tt": 0.5}}
ALL = {"vt": {"window": {"t_a": 0, "t_b": 2 ** 62}}, "belief": "all"}


def t1(app="$app", sys="$sys") -> dict:
    return M([N("s", "Supplier"), N("p", "Part")], [S("s", "p", "SUPPLIES", var="e")],
             {"group": {"by": [], "aggs": [
                 {"as": "avgCost", "fn": "mean", "of": tp("e", "supplycost", "int")},
                 {"as": "count", "fn": "count"}]}},
             scope={"vt": {"instant": app}, "as_of_tt": sys}, params=dict(BIH_P))


def t2(scope=None) -> dict:
    return M([N("o", "Orders")], [],
             {"group": {"by": [], "aggs": [
                 {"as": "total", "fn": "sum", "of": tp("o", "totalprice", "int")}]}},
             where=[cmp("=", tp("o", "orderstatus"), lit("O"))],
             scope=scope or APP, params=dict(BIH_P))


def b3(scope: dict, params: dict) -> dict:
    return M([N("p55", "Part", uids="$part"), N("s", "Supplier"), N("p", "Part")],
             [S("s", "p55", "SUPPLIES", var="e1"), S("s", "p", "SUPPLIES", var="e2")],
             {"group": {"by": [{"as": "partId", "expr": col("p.uid")}], "aggs": []}},
             where=[cmp("!=", col("p.uid"), col("p55.uid"))],
             scope=scope, params={"part": "Part:55", **params})


def b3seq(scope: dict, params: dict) -> dict:
    """B3.1's pattern under sequenced semantics: each answer row carries the
    common interval on each named axis over which both supplies co-hold."""
    axes = scope["co_valid"]
    out = dict(partId=col("p.uid"), supplierId=col("s.uid"))
    if "valid" in axes:
        out.update(vs=col("valid.s"), ve=col("valid.e"))
    if "transaction" in axes:
        out.update(ts=col("belief.s"), te=col("belief.e"))
    return M([N("p55", "Part", uids="$part"), N("s", "Supplier"), N("p", "Part")],
             [S("s", "p55", "SUPPLIES", var="e1"), S("s", "p", "SUPPLIES", var="e2")],
             {"rows": R(**out)},
             where=[cmp("!=", col("p.uid"), col("p55.uid"))],
             scope=scope, params={"part": "Part:55", **params})


ORDER_ROWS = R(orderId=col("o.uid"), status=tp("o", "orderstatus"),
               total=tp("o", "totalprice", "int"), vt_s=col("o.vt_s"), vt_e=col("o.vt_e"),
               tt_s=col("o.tt_s"), tt_e=col("o.tt_e"))
BIH: dict[str, list[dict]] = {
    "T1": [t1()],
    "T2": [t2()],
    "T3": [t1("$app1"), t1("$app2"), t1("$app3")],
    "T4": [M([N("o", "Orders")], [],
             {"rows": R(orderId=col("o.uid"), total=tp("o", "totalprice", "int")),
              "order_by": ORD((col("total"), "desc")), "limit": 10},
             where=[cmp("=", tp("o", "orderstatus"), lit("O"))], scope=APP, params=dict(BIH_P))],
    "T5": [M([N("o", "Orders")], [], {"rows": ORDER_ROWS}, scope=ALL)],
    "T6": [M([N("o", "Orders")], [], {"rows": ORDER_ROWS},
             scope={"vt": {"instant": "$app"}, "belief": "all"}, params=dict(BIH_P)),
           M([N("o", "Orders")], [], {"rows": ORDER_ROWS},
             scope={"vt": {"window": {"t_a": 0, "t_b": 2 ** 62}}, "as_of_tt": "$sys"},
             params=dict(BIH_P))],
    "T7": [t2({"vt": {"instant": "$app"}}), t2({"vt": {"instant": "$app"}, "as_of_tt": "$now_tt"})],
    "T8": [M([N("o", "Orders")], [],
             {"group": {"by": [], "aggs": [{"as": "count", "fn": "count"}]}},
             where=[cmp("<=", tp("o", "receivable_s", "int"), lit("$app")),
                    cmp(">", tp("o", "receivable_e", "int"), lit("$app"))],
             scope=APP, params=dict(BIH_P))],
    "T9": [M([N("o", "Orders")], [],
             {"group": {"by": [{"as": "status", "expr": tp("o", "orderstatus")}],
                        "aggs": [{"as": "count", "fn": "count"}]}},
             where=[cmp("<", tp("o", "receivable_s", "int"), lit("$t2")),
                    cmp(">", tp("o", "receivable_e", "int"), lit("$t1"))],
             scope=APP, params={**BIH_P, "t1": {"date": "2021-01-01"},
                                "t2": {"date": "2021-07-01"}})],
    "K6": [M([N("c", "Customer")], [],
             {"rows": R(customerId=col("c.uid"), acctbal=tp("c", "acctbal", "int"),
                        vt_s=col("c.vt_s"), vt_e=col("c.vt_e"), tt_s=col("c.tt_s"),
                        tt_e=col("c.tt_e"))},
             where=[cmp(">", tp("c", "acctbal", "int"), lit("$x"))], scope=ALL,
             params={"x": 500_000})],
    "R2": [M([N("o", "Orders")], [],
             {"group": {"by": [{"as": "orderId", "expr": col("o.uid")}],
                        "aggs": [{"as": "total", "fn": "sum",
                                  "of": {"arith": "-", "l": col("o.vt_e"), "r": col("o.vt_s")}},
                                 {"as": "longest", "fn": "max",
                                  "of": {"arith": "-", "l": col("o.vt_e"), "r": col("o.vt_s")}}]},
              "order_by": ORD((col("total"), "asc"), (col("orderId"), "asc")), "limit": 50},
             where=[cmp("=", tp("o", "orderstatus"), lit("O"))],
             scope={"vt": {"window": {"t_a": 0, "t_b": 2 ** 62}}})],
    "B3.6": [b3({"vt": {"window": {"t_a": 0, "t_b": 2 ** 62}}}, {})],
    "B3.7": [b3({"vt": {"window": {"t_a": 0, "t_b": 2 ** 62}}, "as_of_tt": "$sys"},
                {"sys": {"tt": 0.5}})],
    "B3.9": [b3({"vt": {"instant": "$app"}, "belief": "all"}, {"app": {"date": "2021-06-01"}})],
    "B3.11": [b3(ALL, {})],
    "R4": [M([N("s", "Supplier"), N("p", "Part")], [S("s", "p", "SUPPLIES", var="e")],
             {"group": {"by": by(supplierId=col("s.uid"), partId=col("p.uid")),
                        "aggs": [agg("maxQty", "max", tp("e", "availqty", "int")),
                                 agg("minQty", "min", tp("e", "availqty", "int"))],
                        "project": by(diff=arith("-", col("maxQty"), col("minQty")))},
              "order_by": ORD((col("diff"), "asc"), (col("supplierId"), "asc"),
                            (col("partId"), "asc")), "limit": 20},
             scope=ALL)],
    "R5": [M([N("c", "Customer"), N("o", "Orders")], [S("o", "c", "PLACED_BY", var="e")],
             {"group": {"by": by(customerId=col("c.uid")), "aggs": [agg("count", "count")]},
              "order_by": ORD((col("count"), "desc"), (col("customerId"), "asc")), "limit": 20},
             where=[cmp("<", tp("c", "acctbal", "int"), lit(500_000)),
                    cmp(">", tp("o", "totalprice", "int"), lit(1_000))],
             scope={"vt": {"window": {"t_a": 0, "t_b": 2 ** 62}}, "as_of_tt": "$sys",
                    "co_valid": ["valid"]},
             params={"sys": {"tt": 0.5}})],
    "R1": [M([N("o", "Orders")], [],
             {"rows": R(orderId=col("o.uid"), since=col("o.vt_s"), changedAt=col("o2.vt_s")),
              "order_by": ORD((col("changedAt"), "asc"), (col("orderId"), "asc")), "limit": 50},
             where=[cmp("=", tp("o", "orderstatus"), lit("O")),
                    cmp("=", tp("o2", "orderstatus"), lit("F")),
                    cmp("=", col("o2.vt_s"), col("o.vt_e"))],
             scope={"vt": {"window": {"t_a": 0, "t_b": 2 ** 62}}})
           | {"pattern": {"nodes": [N("o", "Orders")], "steps": [],
                          "succ": [{"var": "o2", "of": "o", "axis": "valid", "dir": "next"}],
                          "where": [cmp("=", tp("o", "orderstatus"), lit("O")),
                                    cmp("=", tp("o2", "orderstatus"), lit("F")),
                                    cmp("=", col("o2.vt_s"), col("o.vt_e"))]}}],
    "R7": [{"pattern": {"nodes": [N("s", "Supplier"), N("p", "Part")],
                        "steps": [S("s", "p", "SUPPLIES", var="e1")],
                        "succ": [{"var": "e2", "of": "e1", "axis": "transaction", "dir": "next"}],
                        "where": [cmp(">", arith("*", tp("e2", "supplycost", "int"), lit(1000)),
                                      arith("*", tp("e1", "supplycost", "int"), lit(1075)))]},
            "return": {"group": {"by": by(supplierId=col("s.uid")),
                                 "aggs": [agg("raises", "count")]},
                       "order_by": ORD((col("raises"), "desc"), (col("supplierId"), "asc"))},
            "scope": ALL, "params": {}}],
    "K5": [{"pattern": {"nodes": [N("c", "Customer", uids="$customer")], "steps": [],
                        "succ": [{"var": "c0", "of": "c", "axis": "transaction", "dir": "prev"}]},
            "return": {"rows": R(tt_s=col("c.tt_s"), acctbal=tp("c", "acctbal", "int"),
                                 prevTt_s=col("c0.tt_s"), prevAcctbal=tp("c0", "acctbal", "int"))},
            "scope": ALL,
            "params": {"customer": {"probe": M(
                [N("c", "Customer")], [],
                {"group": {"by": by(c=col("c.uid")), "aggs": [agg("n", "count")]},
                 "order_by": ORD((col("n"), "desc")), "limit": 1}, scope=ALL),
                "col": "c", "i": 0}}}],
    "B3.3": [b3seq({"vt": {"window": {"t_a": 0, "t_b": 2 ** 62}}, "co_valid": ["valid"]}, {})],
    "B3.4": [b3seq({"vt": {"instant": "$app"}, "belief": "all", "co_valid": ["transaction"]},
                   {"app": {"date": "2021-06-01"}})],
    "B3.5": [b3seq({**ALL, "co_valid": ["valid", "transaction"]}, {})],
    "B3.8": [b3seq({**ALL, "co_valid": ["transaction"]}, {})],
    "B3.10": [b3seq({**ALL, "co_valid": ["valid"]}, {})],
}
# TPC-H's 22 queries, shaped to the generator's schema at one application
# point and one system point (TPC-BiH's time travel). The generator has no
# ship, commit or receipt dates, comments, tax, ship modes or part types:
# a line's valid-time start stands for its ship date, `linestatus` for the
# return flag, the order status for the priority, part name words for the
# type, a negative supplier balance for a complaint, and a slice of the
# customer name for the phone's country code.
H_P = dict(BIH_P)
LQTY, LPRICE, LDISC = tp("l", "quantity", "int"), tp("l", "extendedprice", "int"), \
    tp("l", "discount", "int")
#: revenue in cents x 100: extendedprice * (100 - discount)
REV = arith("*", LPRICE, arith("-", lit(100), LDISC))
LINE = [N("l", "Lineitem"), N("o", "Orders")]
LINE_O = S("l", "o", "IN_ORDER")


def H(nodes, steps, ret, where=None, lookups=None, not_=None, optional=None, extra=None):
    q = M(nodes, steps, ret, where=where, not_=not_, optional=optional, scope=APP,
          params={**H_P, **(extra or {})})
    if lookups:
        q["pattern"]["lookups"] = lookups
    return q


def in_year(c: dict, y: int) -> dict:
    return AND(cmp(">=", c, lit(_date(f"{y}-01-01"))), cmp("<", c, lit(_date(f"{y + 1}-01-01"))))


def in_range(c: dict, a: str, b: str) -> dict:
    return AND(cmp(">=", c, lit(_date(a))), cmp("<", c, lit(_date(b))))


def nation_of(var: str, nvar: str, rvar: str | None = None) -> tuple[list, list]:
    nodes = [N(nvar, "Nation")] + ([N(rvar, "Region")] if rvar else [])
    steps = [S(var, nvar, "IN_NATION")] + ([S(nvar, rvar, "IN_REGION")] if rvar else [])
    return nodes, steps


EU_N, EU_S = nation_of("s", "n", "r")
HQ: dict[str, list[dict]] = {}
HQ["H1"] = [H(LINE[:1], [],
              {"group": {"by": by(linestatus=tp("l", "linestatus")),
                         "aggs": [agg("sum_qty", "sum", LQTY), agg("sum_base_price", "sum", LPRICE),
                                  agg("sum_disc_price", "sum", REV), agg("avg_qty", "mean", LQTY),
                                  agg("avg_price", "mean", LPRICE), agg("avg_disc", "mean", LDISC),
                                  agg("count_order", "count")]},
               "order_by": ORD((col("linestatus"), "asc"))},
              where=[cmp("<=", col("l.vt_s"), lit(_date("2021-03-01")))])]
HQ["H2"] = [H([N("p", "Part"), N("s", "Supplier")] + EU_N,
              [S("s", "p", "SUPPLIES", var="e")] + EU_S,
              {"rows": R(s_acctbal=tp("s", "acctbal", "int"), s_name=tp("s", "name"),
                         n_name=tp("n", "name"), p_partkey=col("p.uid"),
                         p_brand=tp("p", "brand"), s_phone=tp("s", "phone")),
               "order_by": ORD((col("s_acctbal"), "desc"), (col("n_name"), "asc"),
                               (col("s_name"), "asc"), (col("p_partkey"), "asc")),
               "limit": 100},
              where=[cmp("<=", tp("p", "size", "int"), lit(25)),
                     {"strfn": "like", "args": [tp("p", "name"), lit("%green%")]},
                     name_is("r", "$region"),
                     cmp("=", tp("e", "supplycost", "int"), col("mc.cost"))],
              lookups=[{"as": "mc", "on": col("p.uid"), "key": "part", "match": {
                  "pattern": {"nodes": [N("p2", "Part"), N("s2", "Supplier"), N("n2", "Nation"),
                                        N("r2", "Region")],
                              "steps": [S("s2", "p2", "SUPPLIES", var="e2"),
                                        S("s2", "n2", "IN_NATION"), S("n2", "r2", "IN_REGION")],
                              "where": [name_is("r2", "$region")]},
                  "group": {"by": by(part=col("p2.uid")),
                            "aggs": [agg("cost", "min", tp("e2", "supplycost", "int"))]}}}],
              extra={"region": "EUROPE"})]
HQ["H3"] = [H(LINE + [N("c", "Customer")], [LINE_O, S("o", "c", "PLACED_BY")],
              {"group": {"by": by(l_orderkey=col("o.uid"), o_orderdate=col("o.vt_s")),
                         "aggs": [agg("revenue", "sum", REV)]},
               "order_by": ORD((col("revenue"), "desc"), (col("o_orderdate"), "asc")),
               "limit": 10},
              where=[cmp("=", tp("c", "mktsegment"), lit("BUILDING")),
                     cmp("<", col("o.vt_s"), lit(_date("2021-03-15"))),
                     cmp("=", tp("l", "linestatus"), lit("O"))])]
HQ["H4"] = [H([N("o", "Orders")], [],
              {"group": {"by": by(o_orderpriority=tp("o", "orderstatus")),
                         "aggs": [agg("order_count", "count")]},
               "order_by": ORD((col("o_orderpriority"), "asc"))},
              where=[in_range(col("o.vt_s"), "2021-01-01", "2021-04-01")],
              lookups=[{"as": "late", "on": col("o.uid"), "key": "o", "join": "inner", "match": {
                  "pattern": {"nodes": [N("l2", "Lineitem"), N("o2", "Orders")],
                              "steps": [S("l2", "o2", "IN_ORDER")],
                              "where": [cmp("=", tp("l2", "linestatus"), lit("F"))]},
                  "group": {"by": by(o=col("o2.uid")), "aggs": []}}}])]
H5_N, H5_S = nation_of("s", "n", "r")
HQ["H5"] = [H(LINE + [N("c", "Customer"), N("s", "Supplier"), N("cn", "Nation")] + H5_N,
              [LINE_O, S("o", "c", "PLACED_BY"), S("l", "s", "FROM_SUPPLIER"),
               S("c", "cn", "IN_NATION")] + H5_S,
              {"group": {"by": by(n_name=tp("n", "name")), "aggs": [agg("revenue", "sum", REV)]},
               "order_by": ORD((col("revenue"), "desc"))},
              where=[cmp("=", col("cn.uid"), col("n.uid")), name_is("r", "$region"),
                     in_year(col("o.vt_s"), 2021)],
              extra={"region": "ASIA"})]
HQ["H6"] = [H(LINE[:1], [],
              {"group": {"by": [], "aggs": [agg("revenue", "sum", arith("*", LPRICE, LDISC))]}},
              where=[in_year(col("l.vt_s"), 2021), cmp(">=", LDISC, lit(5)),
                     cmp("<=", LDISC, lit(7)), cmp("<", LQTY, lit(24))])]
HQ["H7"] = [H(LINE + [N("c", "Customer"), N("s", "Supplier"), N("n1", "Nation"),
                      N("n2", "Nation")],
              [LINE_O, S("o", "c", "PLACED_BY"), S("l", "s", "FROM_SUPPLIER"),
               S("s", "n1", "IN_NATION"), S("c", "n2", "IN_NATION")],
              {"group": {"by": by(supp_nation=tp("n1", "name"), cust_nation=tp("n2", "name"),
                                  l_year=cal("year", col("l.vt_s"))),
                         "aggs": [agg("revenue", "sum", REV)]},
               "order_by": ORD((col("supp_nation"), "asc"), (col("cust_nation"), "asc"),
                               (col("l_year"), "asc"))},
              where=[OR(AND(name_is("n1", "$nation1"), name_is("n2", "$nation2")),
                        AND(name_is("n1", "$nation2"), name_is("n2", "$nation1"))),
                     in_range(col("l.vt_s"), "2020-01-01", "2022-01-01")],
              extra={"nation1": "FRANCE", "nation2": "GERMANY"})]
HQ["H8"] = [H(LINE + [N("c", "Customer"), N("p", "Part"), N("s", "Supplier"),
                      N("cn", "Nation"), N("r", "Region"), N("sn", "Nation")],
              [LINE_O, S("o", "c", "PLACED_BY"), S("l", "p", "OF_PART"),
               S("l", "s", "FROM_SUPPLIER"), S("c", "cn", "IN_NATION"), S("cn", "r", "IN_REGION"),
               S("s", "sn", "IN_NATION")],
              {"group": {"by": by(o_year=cal("year", col("o.vt_s"))),
                         "aggs": [agg("nation_volume", "sum", REV,
                                      where=name_is("sn", "$nation")),
                                  agg("volume", "sum", REV)],
                         "project": by(mkt_share=arith("/", {"coalesce": [col("nation_volume"),
                                                                          lit(0)]},
                                                       col("volume")))},
               "order_by": ORD((col("o_year"), "asc"))},
              where=[name_is("r", "$region"), in_range(col("o.vt_s"), "2020-01-01", "2022-01-01"),
                     {"strfn": "like", "args": [tp("p", "name"), lit("%brown%")]}],
              extra={"region": "AMERICA", "nation": "BRAZIL"})]
HQ["H9"] = [H(LINE + [N("p", "Part"), N("s", "Supplier"), N("n", "Nation")],
              [LINE_O, S("l", "p", "OF_PART"), S("l", "s", "FROM_SUPPLIER"),
               S("s", "p", "SUPPLIES", var="ps"), S("s", "n", "IN_NATION")],
              {"group": {"by": by(nation=tp("n", "name"), o_year=cal("year", col("o.vt_s"))),
                         "aggs": [agg("sum_profit", "sum", arith(
                             "-", REV, arith("*", arith("*", tp("ps", "supplycost", "int"), LQTY),
                                             lit(100))))]},
               "order_by": ORD((col("nation"), "asc"), (col("o_year"), "desc"))},
              where=[{"strfn": "like", "args": [tp("p", "name"), lit("%green%")]}])]
HQ["H10"] = [H(LINE + [N("c", "Customer"), N("n", "Nation")],
               [LINE_O, S("o", "c", "PLACED_BY"), S("c", "n", "IN_NATION")],
               {"group": {"by": by(c_custkey=col("c.uid"), c_name=tp("c", "name"),
                                   c_acctbal=tp("c", "acctbal", "int"), n_name=tp("n", "name")),
                          "aggs": [agg("revenue", "sum", REV)]},
                "order_by": ORD((col("revenue"), "desc"), (col("c_custkey"), "asc")), "limit": 20},
               where=[in_range(col("o.vt_s"), "2020-10-01", "2021-07-01"),
                      cmp("=", tp("l", "linestatus"), lit("F"))])]
DE_N, DE_S = nation_of("s", "n")
HQ["H11"] = [H([N("p", "Part"), N("s", "Supplier")] + DE_N,
               [S("s", "p", "SUPPLIES", var="ps")] + DE_S,
               {"group": {"by": by(ps_partkey=col("p.uid")),
                          "aggs": [agg("value", "sum", arith("*", tp("ps", "supplycost", "int"),
                                                            tp("ps", "availqty", "int")))],
                          "project": by(share=arith("/", col("value"), {"total": "value"})),
                          "having": cmp(">", col("share"), lit(0.0001))},
                "order_by": ORD((col("value"), "desc"))},
               where=[name_is("n", "$nation")], extra={"nation": "GERMANY"})]
HQ["H12"] = [H(LINE, [LINE_O],
               {"group": {"by": by(l_shipmode=tp("l", "linestatus")),
                          "aggs": [agg("high_line_count", "count",
                                       where=cmp("=", tp("o", "orderstatus"), lit("O"))),
                                   agg("low_line_count", "count",
                                       where=cmp("!=", tp("o", "orderstatus"), lit("O")))]},
                "order_by": ORD((col("l_shipmode"), "asc"))},
               where=[in_year(col("l.vt_s"), 2021)])]
HQ["H13"] = [H([N("c", "Customer")], [],
               {"group": {"by": by(c_custkey=col("c.uid")),
                          "aggs": [agg("c_count", "count", col("o.uid"))],
                          "then": {"by": by(c_count=col("c_count")),
                                   "aggs": [agg("custdist", "count")]}},
                "order_by": ORD((col("custdist"), "desc"), (col("c_count"), "desc"))},
               optional=[{"nodes": [N("o", "Orders")], "steps": [S("o", "c", "PLACED_BY")],
                          "where": [cmp("!=", tp("o", "orderstatus"), lit("P"))]}])]
HQ["H14"] = [H(LINE[:1] + [N("p", "Part")], [S("l", "p", "OF_PART")],
               {"group": {"by": [],
                          "aggs": [agg("promo", "sum", REV, where={"strfn": "like", "args": [
                              tp("p", "name"), lit("b%")]}),
                                   agg("all", "sum", REV)],
                          "project": by(promo_revenue=arith("/", arith(
                              "*", lit(100.0), {"coalesce": [col("promo"), lit(0)]}),
                              col("all")))}},
               where=[in_range(col("l.vt_s"), "2021-03-01", "2021-04-01")])]
REV_BY_SUPPLIER = {"pattern": {"nodes": [N("l2", "Lineitem"), N("s2", "Supplier")],
                               "steps": [S("l2", "s2", "FROM_SUPPLIER")],
                               "where": [in_range(col("l2.vt_s"), "2021-01-01", "2021-04-01")]},
                   "group": {"by": by(supplier=col("s2.uid")),
                             "aggs": [agg("total_revenue", "sum", arith(
                                 "*", tp("l2", "extendedprice", "int"),
                                 arith("-", lit(100), tp("l2", "discount", "int"))))]}}
HQ["H15"] = [H([N("s", "Supplier")], [],
               {"rows": R(s_suppkey=col("s.uid"), s_name=tp("s", "name"),
                          s_phone=tp("s", "phone"), total_revenue=col("rev.total_revenue")),
                "order_by": ORD((col("s_suppkey"), "asc"))},
               where=[cmp("=", col("rev.total_revenue"), col("top.max_revenue"))],
               lookups=[{"as": "rev", "on": col("s.uid"), "key": "supplier", "join": "inner",
                         "match": REV_BY_SUPPLIER},
                        {"as": "top", "on": lit(1), "key": "k", "join": "inner", "match": {
                            "pattern": REV_BY_SUPPLIER["pattern"],
                            "group": {**REV_BY_SUPPLIER["group"], "then": {
                                "by": by(k=lit(1)),
                                "aggs": [agg("max_revenue", "max", col("total_revenue"))]}}}}])]
HQ["H16"] = [H([N("p", "Part"), N("s", "Supplier")], [S("s", "p", "SUPPLIES")],
               {"group": {"by": by(p_brand=tp("p", "brand"), p_size=tp("p", "size", "int")),
                          "aggs": [agg("supplier_cnt", "count_distinct", col("s.uid"))]},
                "order_by": ORD((col("supplier_cnt"), "desc"), (col("p_brand"), "asc"),
                                (col("p_size"), "asc"))},
               where=[cmp("!=", tp("p", "brand"), lit("Brand#45")),
                      {"not": {"strfn": "like", "args": [tp("p", "name"), lit("almond%")]}},
                      OR(OR(cmp("=", tp("p", "size", "int"), lit(49)),
                            cmp("=", tp("p", "size", "int"), lit(14))),
                         OR(cmp("=", tp("p", "size", "int"), lit(23)),
                            cmp("<", tp("p", "size", "int"), lit(10))))],
               not_=[{"nodes": [N("bad", "Supplier")],
                      "steps": [S("bad", "p", "SUPPLIES")],
                      "where": [cmp("<", tp("bad", "acctbal", "int"), lit(-90_000))]}])]
HQ["H17"] = [H(LINE[:1] + [N("p", "Part")], [S("l", "p", "OF_PART")],
               {"group": {"by": [], "aggs": [agg("total", "sum", LPRICE)],
                          "project": by(avg_yearly=arith("/", col("total"), lit(7.0)))}},
               where=[{"strfn": "like", "args": [tp("p", "brand"), lit("Brand#2%")]},
                      cmp("<", arith("*", LQTY, lit(5)), col("avgq.q"))],
               lookups=[{"as": "avgq", "on": col("p.uid"), "key": "part", "join": "inner",
                         "match": {"pattern": {"nodes": [N("l2", "Lineitem"), N("p2", "Part")],
                                               "steps": [S("l2", "p2", "OF_PART")]},
                                   "group": {"by": by(part=col("p2.uid")),
                                             "aggs": [agg("q", "mean",
                                                          tp("l2", "quantity", "int"))]}}}])]
HQ["H18"] = [H([N("o", "Orders"), N("c", "Customer")], [S("o", "c", "PLACED_BY")],
               {"rows": R(c_name=tp("c", "name"), c_custkey=col("c.uid"), o_orderkey=col("o.uid"),
                          o_orderdate=col("o.vt_s"), o_totalprice=tp("o", "totalprice", "int"),
                          sum_qty=col("big.qty")),
                "order_by": ORD((col("o_totalprice"), "desc"), (col("o_orderdate"), "asc")),
                "limit": 100},
               where=[cmp(">", col("big.qty"), lit(150))],
               lookups=[{"as": "big", "on": col("o.uid"), "key": "order", "join": "inner",
                         "match": {"pattern": {"nodes": [N("l2", "Lineitem"), N("o2", "Orders")],
                                               "steps": [S("l2", "o2", "IN_ORDER")]},
                                   "group": {"by": by(order=col("o2.uid")),
                                             "aggs": [agg("qty", "sum",
                                                          tp("l2", "quantity", "int"))]}}}])]
PBRAND, PSIZE = tp("p", "brand"), tp("p", "size", "int")
HQ["H19"] = [H(LINE[:1] + [N("p", "Part")], [S("l", "p", "OF_PART")],
               {"group": {"by": [], "aggs": [agg("revenue", "sum", REV)]}},
               where=[OR(OR(AND(AND(cmp("=", PBRAND, lit("Brand#12")), cmp("<=", PSIZE, lit(5))),
                                AND(cmp(">=", LQTY, lit(1)), cmp("<=", LQTY, lit(11)))),
                            AND(AND(cmp("=", PBRAND, lit("Brand#23")), cmp("<=", PSIZE, lit(10))),
                                AND(cmp(">=", LQTY, lit(10)), cmp("<=", LQTY, lit(20))))),
                         AND(AND(cmp("=", PBRAND, lit("Brand#34")), cmp("<=", PSIZE, lit(15))),
                             AND(cmp(">=", LQTY, lit(20)), cmp("<=", LQTY, lit(30)))))])]
HQ["H20"] = [H([N("s", "Supplier"), N("p", "Part"), N("n", "Nation")],
               [S("s", "p", "SUPPLIES", var="ps"), S("s", "n", "IN_NATION")],
               {"group": {"by": by(s_name=tp("s", "name"), s_suppkey=col("s.uid")), "aggs": []},
                "order_by": ORD((col("s_name"), "asc"))},
               where=[name_is("n", "$nation"),
                      {"strfn": "like", "args": [tp("p", "name"), lit("%forest%")]},
                      cmp(">", arith("*", tp("ps", "availqty", "int"), lit(2)),
                          {"coalesce": [col("shipped.qty"), lit(0)]})],
               lookups=[{"as": "shipped", "on": col("ps.eid"), "key": "pair", "match": {
                   "pattern": {"nodes": [N("l2", "Lineitem"), N("p2", "Part"), N("s2", "Supplier")],
                               "steps": [S("l2", "p2", "OF_PART"), S("l2", "s2", "FROM_SUPPLIER"),
                                         S("s2", "p2", "SUPPLIES", var="ps2")],
                               "where": [in_year(col("l2.vt_s"), 2021)]},
                   "group": {"by": by(pair=col("ps2.eid")),
                             "aggs": [agg("qty", "sum", tp("l2", "quantity", "int"))]}}}],
               extra={"nation": "CANADA"})]
HQ["H21"] = [H(LINE + [N("s", "Supplier"), N("n", "Nation")],
               [LINE_O, S("l", "s", "FROM_SUPPLIER"), S("s", "n", "IN_NATION")],
               {"group": {"by": by(s_name=tp("s", "name")), "aggs": [agg("numwait", "count")]},
                "order_by": ORD((col("numwait"), "desc"), (col("s_name"), "asc")), "limit": 100},
               where=[name_is("n", "$nation"), cmp("=", tp("o", "orderstatus"), lit("F")),
                      cmp(">", col("multi.suppliers"), lit(1)),
                      cmp("=", {"coalesce": [col("open.suppliers"), lit(0)]}, lit(0))],
               lookups=[{"as": "multi", "on": col("o.uid"), "key": "order", "join": "inner",
                         "match": {"pattern": {"nodes": [N("l2", "Lineitem"), N("o2", "Orders"),
                                                         N("s2", "Supplier")],
                                               "steps": [S("l2", "o2", "IN_ORDER"),
                                                         S("l2", "s2", "FROM_SUPPLIER")]},
                                   "group": {"by": by(order=col("o2.uid")),
                                             "aggs": [agg("suppliers", "count_distinct",
                                                          col("s2.uid"))]}}},
                        {"as": "open", "on": col("o.uid"), "key": "order", "match": {
                            "pattern": {"nodes": [N("l3", "Lineitem"), N("o3", "Orders"),
                                                  N("s3", "Supplier")],
                                        "steps": [S("l3", "o3", "IN_ORDER"),
                                                  S("l3", "s3", "FROM_SUPPLIER")],
                                        "where": [cmp("=", tp("l3", "linestatus"), lit("O"))]},
                            "group": {"by": by(order=col("o3.uid")),
                                      "aggs": [agg("suppliers", "count_distinct",
                                                   col("s3.uid"))]}}}],
               extra={"nation": "SAUDI ARABIA"})]
CNTRY = {"strfn": "substring", "args": [tp("c", "name"), lit(17), lit(2)]}
HQ["H22"] = [H([N("c", "Customer")], [],
               {"group": {"by": by(cntrycode=CNTRY),
                          "aggs": [agg("numcust", "count"),
                                   agg("totacctbal", "sum", tp("c", "acctbal", "int"))]},
                "order_by": ORD((col("cntrycode"), "asc"))},
               where=[cmp(">", tp("c", "acctbal", "int"), col("avgbal.bal")),
                      {"strfn": "contains", "args": [lit("13 31 23 29 30 18 17"), CNTRY]}],
               not_=[{"nodes": [N("o", "Orders")], "steps": [S("o", "c", "PLACED_BY")]}],
               lookups=[{"as": "avgbal", "on": lit(1), "key": "k", "join": "inner", "match": {
                   "pattern": {"nodes": [N("c2", "Customer")],
                               "where": [cmp(">", tp("c2", "acctbal", "int"), lit(0))]},
                   "group": {"by": by(k=lit(1)),
                             "aggs": [agg("bal", "mean", tp("c2", "acctbal", "int"))]}}}])]
BIH.update(HQ)

BIH["T3"] = [dict(q, params={**q["params"], "app1": {"date": "2020-06-01"},
                             "app2": {"date": "2021-06-01"}, "app3": {"date": "2022-03-01"}})
             for q in BIH["T3"]]

#: row -> (store family, calls)
CORPUS: dict[str, tuple[str, list[dict]]] = {}
for _rows, _fam in ((SNB, "snb"), (AEONG, "snb"), (FIN, "fin"), (BIH, "bih")):
    for _row, _calls in _rows.items():
        CORPUS[_row] = (_fam, _calls)

GENERATORS = {"snb": "snb_shaped", "fin": "finbench_shaped", "bih": "tpcbih_shaped"}


# ---------------------------------------------------------------------------
# parameters and execution
# ---------------------------------------------------------------------------

class Ctx:
    """One store family's parameter sources."""

    def __init__(self, fam: str, adapter: Any, scale: int) -> None:
        self.fam, self.adapter = fam, adapter
        self.gen = importlib.import_module(f"gnomon.data.{GENERATORS[fam]}")
        self.names = self.gen.names(scale)
        self._tts: list[int] | None = None
        self._probes: dict[str, Any] = {}

    def tts(self) -> list[int]:
        if self._tts is None:
            cols = self.adapter.versions_columnar("node")
            self._tts = sorted({int(t) for t in cols["tt_s"]})
        return self._tts

    def resolve(self, spec: Any) -> Any:
        from gnomon.temporal.ops_match import match

        if not isinstance(spec, dict):
            return spec
        if "pick" in spec:
            which, i = spec["pick"]
            return self.names[which][i]
        if "now" in spec:
            return self.gen.NOW
        if "date" in spec:
            return _date(spec["date"])
        if "tt" in spec:
            tts = self.tts()
            return tts[min(len(tts) - 1, int(spec["tt"] * len(tts)))]
        if "probe" in spec:
            key = json.dumps(spec["probe"], sort_keys=True)
            if key not in self._probes:
                args = bind(spec["probe"], self)
                args["limit"] = 10_000
                self._probes[key] = match(self.adapter, args,
                                          reference_bypass="corpus parameter probe")["rows"]
            rows = self._probes[key]
            if spec.get("all"):
                return [r[spec["col"]] for r in rows]
            if not rows:
                raise LookupError(f"probe for {spec['col']!r} found nothing on this store")
            # a small store may hold fewer candidates than the index asks for
            return rows[min(spec.get("i", 0), len(rows) - 1)][spec["col"]]
        return spec


def bind(args: dict, ctx: Ctx) -> dict:
    out = json.loads(json.dumps(args))
    params = {k: ctx.resolve(v) for k, v in out.get("params", {}).items()}
    params.setdefault("now", ctx.gen.NOW)
    params.setdefault("now_tt", 2 ** 62)
    out["params"] = params
    return out


def windowed(args: dict, days: int) -> dict:
    """The call under rule W1: an instant scope with current belief becomes
    the window `[t - days, t + 1)` with `co_valid: [valid]`; any other scope
    is left as it is."""
    scope = args.get("scope", {})
    vt = scope.get("vt", {})
    if set(vt) != {"instant"} or scope.get("belief", "current") != "current":
        return args
    t = vt["instant"]
    if isinstance(t, str) and t.startswith("$"):
        t = args["params"][t[1:]]
    out = dict(args)
    out["scope"] = {**scope, "vt": {"window": {"t_a": max(0, int(t) - days * DAY),
                                               "t_b": int(t) + 1}},
                    "co_valid": sorted(set(scope.get("co_valid", [])) | {"valid"})}
    return out


def answer_hash(payload: dict) -> str:
    from gnomon.core.model import canonical_json

    keep = {k: payload[k] for k in ("columns", "rows", "rows_total")}
    return hashlib.sha256(canonical_json(keep).encode()).hexdigest()[:24]


def run_row(row: str, ctx: Ctx, route: str, page: int = 100_000,
            window_days: int = 0) -> list[dict]:
    from gnomon.core.errors import InvalidArgError
    from gnomon.temporal.ops_match import compile_match, match
    from gnomon.temporal.oracle import Oracle
    from gnomon.temporal.ops_match import validate

    fam, calls = CORPUS[row]
    out = []
    for i, q in enumerate(calls):
        args = bind(q, ctx)
        args["limit"] = min(page, 10_000)
        if window_days and fam == "snb":
            args = windowed(args, window_days)
            try:
                compile_match(validate(args))
            except InvalidArgError as e:
                out.append({"row": row, "call": i, "route": "refused", "hash": "-",
                            "rows_total": 0, "ms": 0.0, "declined": str(e)[:160]})
                continue
        from gnomon.temporal import ops_match as OM

        before = dict(OM.ROUTES)
        t0 = time.perf_counter()
        if route == "oracle":
            oracle = Oracle(list(ctx.adapter.all_node_versions()),
                            list(ctx.adapter.all_edge_versions()))
            payload = oracle.match(validate(args))
        else:
            payload = match(ctx.adapter, args, route=None if route == "auto" else route,
                            reference_bypass="corpus run" if route != "native" else None)
        ms = (time.perf_counter() - t0) * 1e3
        taken = route if route == "oracle" else \
            ("native" if OM.ROUTES["native"] > before["native"] else "reference")
        if route == "auto" and taken != "native" and ctx.adapter.designs().get("match_kernel"):
            rec_declined = OM.LAST_DECLINE[0]
        else:
            rec_declined = ""
        rec = {"row": row, "call": i, "route": taken, "hash": answer_hash(payload),
               "rows_total": payload["rows_total"], "ms": round(ms, 3)}
        if route == "native" and taken != "native":
            rec["declined"] = OM.LAST_DECLINE[0]
        elif rec_declined:
            rec["declined"] = rec_declined
        out.append(rec)
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("build")
    b.add_argument("--out", required=True)
    b.add_argument("--scale", type=int, default=100)
    b.add_argument("--corrections", type=float, default=0.0)
    b.add_argument("--families", default="snb,fin,bih")
    e = sub.add_parser("emit")
    e.add_argument("--out", required=True)
    r = sub.add_parser("run")
    r.add_argument("--stores", required=True)
    r.add_argument("--scale", type=int, default=100)
    r.add_argument("--route", default="reference")
    r.add_argument("--rows", default="")
    r.add_argument("--json", default="")
    r.add_argument("--reps", type=int, default=1)
    r.add_argument("--warm", type=int, default=0, help="untimed runs per row first")
    r.add_argument("--arm", default="", help="label recorded with each call")
    r.add_argument("--block", type=int, default=0, help="ABBA block index, recorded")
    r.add_argument("--window-days", type=int, default=0,
                   help="SNB calls at an instant run under rule W1 over this many days")
    a = ap.parse_args()
    if a.cmd == "build":
        for fam in a.families.split(","):
            gen = importlib.import_module(f"gnomon.data.{GENERATORS[fam]}")
            gen.build(Path(a.out) / fam, a.scale, seed=0, corrections_pct=a.corrections)
            print(f"built {fam} at {Path(a.out) / fam}")
        return 0
    if a.cmd == "emit":
        out = Path(a.out)
        out.mkdir(parents=True, exist_ok=True)
        for row, (fam, calls) in CORPUS.items():
            (out / f"{row}.json").write_text(json.dumps(
                {"row": row, "store": GENERATORS[fam], "calls": calls}, indent=1) + "\n")
        print(f"{len(CORPUS)} rows")
        return 0
    import gnomon
    from gnomon.temporal.algebra import ensure_all_registered

    ensure_all_registered()
    rows = a.rows.split(",") if a.rows else list(CORPUS)
    stores: dict[str, Any] = {}
    ctxs: dict[str, Ctx] = {}
    sink = open(a.json, "a") if a.json else None
    try:
        for row in rows:
            fam = CORPUS[row][0]
            if fam not in ctxs:
                stores[fam] = gnomon.open(Path(a.stores) / fam, backend="native", read_only=True)
                ctxs[fam] = Ctx(fam, stores[fam].adapter, a.scale)
            for _ in range(a.warm):
                run_row(row, ctxs[fam], a.route, window_days=a.window_days)
            for _ in range(a.reps):
                for rec in run_row(row, ctxs[fam], a.route, window_days=a.window_days):
                    rec.update({"arm": a.arm, "block": a.block,
                                "designs": ctxs[fam].adapter.designs().get("match_kernel")})
                    print(f"{rec['row']:14s} {rec['call']} {rec['route']:9s} {rec['hash']} "
                          f"{rec['rows_total']:8d} {rec['ms']:10.2f} ms "
                          f"{rec.get('declined', '')}", flush=True)
                    if sink:
                        sink.write(json.dumps(rec) + "\n")
    finally:
        for s in stores.values():
            s.close()
        if sink:
            sink.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
