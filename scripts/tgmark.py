"""`match` arguments with regular path steps, run them, and check every answer
against an independent evaluator over the graph file itself.

**These are not AeonG results.** The graphs and workloads are gMark's
(`gbagan/gmark`), generated with AeonG's use-case configurations; the
translation is this script's, from gMark's own XML (the UCRPQ form: a
conjunct's disjunction of paths under an optional outermost Kleene star),
not from AeonG's Cypher translation.

    python scripts/tgmark.py build --graph G.txt --out DIR [--retract-pct 5]
    python scripts/tgmark.py emit --workload W.xml
    python scripts/tgmark.py run --store DIR --workload W.xml --route native|reference \\
        [--asof load|now] [--json out.jsonl]
    python scripts/tgmark.py check --graph G.txt --workload W.xml [--asof load|now] \\
        [--retract-pct 5]

`run` and `check` take `--variant`: `as_written` (the default), or one of the
two count-only variants for an answer too large to materialise:
`count_rows`, the size of the distinct answer, and `count_col`, the size of
the distinct projection on the first head variable. `count_rows` is written
for a single star-shaped body (every conjunct leaves the head variable it
is rooted at) and a three-conjunct chain whose variables are all in the
head; the evaluator counts the same shapes without materialising them.

The store holds the graph in two belief states: the load (every triple,
valid from 0), then one batch that retracts a deterministic `--retract-pct`
of the edges at valid time `T_RETRACT`. A query reads the instant `NOW`
either as of the load (`--asof load`: the whole graph) or as of now (the
retracted edges gone): the as-of-transaction-time point AeonG's T-gMark
uses. `check` evaluates the same queries over the triples directly (sets of
(source, target) pairs per conjunct, joined), with no Gnomon code.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import random
import sys
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

T_RETRACT = 1_000_000
NOW = 2_000_000
CHUNK = 100_000
#: answers are compared on their first page (ordered by the head columns) and size
PAGE = 10_000
# Answers are compared whole: the run reads every page of the match call
# (cursor) and every side hashes all rows. PAGE was also the compared prefix: the hash covered the first 10,000
# sorted rows and only the count covered the rest (four W6 cells were larger: social q4, uniprot q0). An answer
# of at most PAGE rows hashes as before, so the earlier records stay comparable for those.


# ---------------------------------------------------------------------------
# the graph
# ---------------------------------------------------------------------------

def triples(path: Path) -> list[tuple[str, str, str]]:
    """The distinct `subject predicate object` lines, in file order."""
    seen: set[tuple[str, str, str]] = set()
    out = []
    for line in open(path):
        parts = line.split()
        if len(parts) != 3:
            continue
        t = (parts[0], parts[1], parts[2])
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out


def label_of(node: str) -> str:
    """gMark names a node `<type>_<n>`; its type is the label."""
    head, _, tail = node.rpartition("_")
    return head if head and tail.isdigit() else node


def retracted(ts: list[tuple[str, str, str]], pct: float, seed: int = 0) -> list[int]:
    """The deterministic sample of edge positions the second batch retracts."""
    rng = random.Random(seed)
    k = int(len(ts) * pct / 100)
    return sorted(rng.sample(range(len(ts)), k))


def build(graph: Path, out: Path, pct: float) -> None:
    import shutil

    import gnomon

    ts = triples(graph)
    nodes: dict[str, str] = {}
    for s, _p, o in ts:
        nodes.setdefault(s, label_of(s))
        nodes.setdefault(o, label_of(o))
    ops: list[dict[str, Any]] = [
        {"op": "assert_node", "uid": u, "label": lab, "props": {}, "vt_s": 0,
         "vt_e": 2 ** 62, "source": "gmark", "provenance_ref": None}
        for u, lab in nodes.items()]
    ops += [{"op": "assert_edge", "src": s, "dst": o, "rel_type": p, "props": {},
             "vt_s": 0, "vt_e": 2 ** 62, "disc": "", "source": "gmark",
             "provenance_ref": None} for s, p, o in ts]
    if out.exists():
        shutil.rmtree(out)
    store = gnomon.open(out, backend="native")
    try:
        for i in range(0, len(ops), CHUNK):
            store._write(ops[i:i + CHUNK])          # noqa: SLF001 - a builder writes
        gone = [{"op": "retract", "ref": {"kind": "edge", "src": ts[i][0], "dst": ts[i][2],
                                          "rel_type": ts[i][1], "disc": ""},
                 "t": T_RETRACT, "source": "gmark", "provenance_ref": None}
                for i in retracted(ts, pct)]
        if gone:
            store._write(gone)                      # noqa: SLF001
    finally:
        store.close()
    print(f"built {out}: {len(nodes)} nodes, {len(ts)} edges, {len(gone)} retracted")


# ---------------------------------------------------------------------------
# the workload
# ---------------------------------------------------------------------------

def parse_workload(path: Path) -> list[dict[str, Any]]:
    """gMark's queries: `{id, head, bodies}`, a body a list of conjuncts
    `{src, trg, star, paths}` and a path a list of `(symbol, inverse)`."""
    out = []
    for q in ET.parse(path).getroot().findall("query"):
        # gMark writes `<id>0</id>` on every query: a query is its position
        qid = len(out)
        head = [v.text.strip().lstrip("?") for v in q.find("head").findall("var")]
        bodies = []
        dropped = 0
        for body in q.find("bodies").findall("body"):
            conjuncts = []
            for c in body.findall("conjunct"):
                star = c.find("star")
                disj = (star if star is not None else c).find("disj")
                paths = [[(s.text.strip(), s.get("inverse") == "true")
                          for s in concat.findall("symbol")]
                         for concat in disj.findall("concat")]
                conjuncts.append({"src": c.get("src").lstrip("?"),
                                  "trg": c.get("trg").lstrip("?"),
                                  "star": star is not None, "paths": paths})
            # gMark leaves a body empty when it could not fill it, and a
            # conjunct with no path matches nothing: neither adds an answer
            if not conjuncts or any(not c["paths"] for c in conjuncts):
                dropped += 1
                continue
            bodies.append(conjuncts)
        out.append({"id": qid, "head": head, "bodies": bodies, "dropped": dropped})
    return out


def regex_of(c: dict[str, Any]) -> dict[str, Any]:
    alts = []
    for path in c["paths"]:
        syms = [{"sym": s, **({"inv": True} if inv else {})} for s, inv in path]
        alts.append(syms[0] if len(syms) == 1 else {"seq": syms})
    e = alts[0] if len(alts) == 1 else {"alt": alts}
    return {"star": e} if c["star"] else e


def pattern_of(body: list[dict[str, Any]]) -> dict[str, Any]:
    names: list[str] = []
    for c in body:
        for v in (c["src"], c["trg"]):
            if v not in names:
                names.append(v)
    return {"nodes": [{"var": v} for v in names],
            "steps": [{"from": c["src"], "to": c["trg"], "regex": regex_of(c)} for c in body]}


def match_args(q: dict[str, Any], as_of: int | None) -> dict[str, Any] | None:
    """A union of conjunctive RPQs as one call: the distinct head bindings,
    or, for a boolean query, the count of matches (true iff a row returns)."""
    head = q["head"]
    if head:
        ret = {"group": {"by": [{"as": v, "expr": {"col": v}} for v in head], "aggs": []}}
        rows = [{"as": v, "expr": {"col": f"{v}.uid"}} for v in head]
    else:
        # a boolean query: a constant projection, one row iff a match exists
        ret = {"group": {"by": [{"as": "one", "expr": {"col": "one"}}], "aggs": []}}
        rows = [{"as": "one", "expr": {"lit": 1}}]
    scope: dict[str, Any] = {"vt": {"instant": NOW}}
    if as_of is not None:
        scope["as_of_tt"] = as_of
    if not q["bodies"]:
        return None                       # no body survives: the answer is empty
    if len(q["bodies"]) == 1:
        by = [{"as": v, "expr": {"col": f"{v}.uid"}} for v in head] or \
            [{"as": "one", "expr": {"lit": 1}}]
        args: dict[str, Any] = {"pattern": pattern_of(q["bodies"][0]),
                                "return": {"group": {**ret["group"], "by": by}}}
    else:
        args = {"union": [{"pattern": pattern_of(b), "rows": rows} for b in q["bodies"]],
                "return": ret}
    args["scope"] = scope
    args["limit"] = PAGE
    return args


# ---------------------------------------------------------------------------
# the count-only variants
# ---------------------------------------------------------------------------

VARIANTS = ("as_written", "count_rows", "count_col")


def first_column(q: dict[str, Any]) -> dict[str, Any]:
    """`count_col`: the query projected on its first head variable."""
    return {**q, "head": q["head"][:1]}


def count_shape(q: dict[str, Any]) -> tuple[str, list[dict[str, Any]]] | None:
    """`("star", conjuncts)` for one body whose conjuncts all leave one head
    variable for distinct other variables, `("chain", [c1, c2, c3])` for one
    body that is a three-conjunct chain of distinct head variables; `None`
    for any other shape."""
    if len(q["bodies"]) != 1:
        return None
    body, head = q["bodies"][0], set(q["head"])
    roots = {c["src"] for c in body}
    trgs = [c["trg"] for c in body]
    if len(roots) == 1 and next(iter(roots)) in head and len(set(trgs)) == len(trgs) \
            and not roots & set(trgs) and head <= roots | set(trgs):
        return "star", body
    if len(body) == 3:
        a, b, c = body
        names = [a["src"], a["trg"], b["trg"], c["trg"]]
        if a["trg"] == b["src"] and b["trg"] == c["src"] and \
                len(set(names)) == 4 and set(names) == head:
            return "chain", body
    return None


def _count_lookup(name: str, on: str, c: dict[str, Any], key_at_src: bool) -> dict[str, Any]:
    """The distinct partners of `on` across conjunct `c`, as a lookup keyed on
    `on`'s end of it (its source when `key_at_src`, else its target)."""
    key, other = ("p", "q") if key_at_src else ("q", "p")
    pat = pattern_of([{**c, "src": "p", "trg": "q"}])
    return {"as": name, "on": {"col": f"{on}.uid"}, "key": "k", "join": "inner",
            "match": {"pattern": pat, "group": {
                "by": [{"as": "k", "expr": {"col": f"{key}.uid"}}],
                "aggs": [{"as": "n", "fn": "count_distinct", "of": {"col": f"{other}.uid"}}]}}}


def _product(cols: list[str]) -> dict[str, Any]:
    e: dict[str, Any] = {"col": cols[0]}
    for c in cols[1:]:
        e = {"arith": "*", "l": e, "r": {"col": c}}
    return e


def count_args(q: dict[str, Any], as_of: int | None) -> dict[str, Any] | None:
    """`count_rows`: the size of the distinct answer as one `match` call that
    never binds a whole answer row. A star counts, per root, the product of
    its head arms' distinct targets (an arm outside the head is an inner
    lookup, so it only qualifies the root); a chain `a -> b -> c -> d` sums,
    over the distinct `(b, c)` pairs, `|a's into b| * |d's out of c|`."""
    shape = count_shape(q)
    if shape is None:
        return None
    kind, body = shape
    scope: dict[str, Any] = {"vt": {"instant": NOW}}
    if as_of is not None:
        scope["as_of_tt"] = as_of
    if kind == "star":
        root = body[0]["src"]
        lookups = [_count_lookup(f"a{i}", root, c, True) for i, c in enumerate(body)]
        heads = [f"a{i}.n" for i, c in enumerate(body) if c["trg"] in q["head"]]
        pattern = {"nodes": [{"var": root}], "steps": [], "lookups": lookups}
    else:
        a, b, c = body
        pattern = {**pattern_of([b]),
                   "lookups": [_count_lookup("l", b["src"], a, False),
                               _count_lookup("r", b["trg"], c, True)]}
        heads = ["l.n", "r.n"]
    return {"pattern": pattern, "scope": scope, "limit": 1,
            "return": {"group": {"by": [], "aggs": [
                {"as": "count", "fn": "sum", "of": _product(heads)}]}}}


def count_rows(g: "Graph", q: dict[str, Any]) -> int:
    """The evaluator's `count_rows`, over the same two shapes."""
    kind, body = count_shape(q) or (None, None)
    if kind == "star":
        total = 0
        for x in g.nodes:
            n = 1
            for c in body:
                k = len(g.conjunct(c, x))
                n *= k if c["trg"] in q["head"] else (1 if k else 0)
                if not n:
                    break
            total += n
        return total
    if kind == "chain":
        a, b, c = body
        into: dict[str, int] = {}
        for x in g.nodes:
            for y in g.conjunct(a, x):
                into[y] = into.get(y, 0) + 1
        out: dict[str, int] = {}
        total = 0
        for y, n_in in into.items():
            for z in g.conjunct(b, y):
                if z not in out:
                    out[z] = len(g.conjunct(c, z))
                total += n_in * out[z]
        return total
    raise ValueError(f"query {q['id']}: no count_rows form for this shape")


# ---------------------------------------------------------------------------
# the independent evaluator
# ---------------------------------------------------------------------------

class Graph:
    """The edges believed at the read, over every node the load asserted
    (a node whose edges are all retracted is still a node)."""

    def __init__(self, ts: list[tuple[str, str, str]], nodes: set[str]) -> None:
        self.out: dict[tuple[str, str], list[str]] = {}
        self.inn: dict[tuple[str, str], list[str]] = {}
        self.nodes: set[str] = set(nodes)
        for s, p, o in ts:
            self.out.setdefault((s, p), []).append(o)
            self.inn.setdefault((o, p), []).append(s)

    def step(self, frontier: set[str], sym: str, inv: bool) -> set[str]:
        index = self.inn if inv else self.out
        return {y for x in frontier for y in index.get((x, sym), ())}

    def conjunct(self, c: dict[str, Any], src: str) -> set[str]:
        """Targets from one source: the union of the paths, or its closure;
        a chain of conjuncts (an eliminated variable) composes them."""
        if "chain" in c:
            cur = {src}
            for part in c["chain"]:
                cur = {w for u in cur for w in self.conjunct(part, u)}
            return cur
        def once(start: set[str]) -> set[str]:
            got: set[str] = set()
            for path in c["paths"]:
                cur = start
                for sym, inv in path:
                    cur = self.step(cur, sym, inv)
                    if not cur:
                        break
                got |= cur
            return got
        if not c["star"]:
            return once({src})
        seen = {src}
        frontier = {src}
        while frontier:
            frontier = once(frontier) - seen
            seen |= frontier
        return seen


def _flip(c: dict[str, Any]) -> dict[str, Any]:
    if "chain" in c:
        return {"src": c["trg"], "trg": c["src"], "chain": [_flip(x) for x in reversed(c["chain"])]}
    return {**c, "src": c["trg"], "trg": c["src"],
            "paths": [[(s, not inv) for s, inv in reversed(p)] for p in c["paths"]]}


def _eliminate(body: list[dict[str, Any]], need: set[str]) -> list[dict[str, Any]]:
    """An existential variable on exactly two conjuncts is composed away:
    `a -c1- v -c2- b` becomes one chained conjunct `a -> b` (every gMark node
    is a node of the graph, so `v` needs nothing beyond the paths)."""
    body = list(body)
    changed = True
    while changed:
        changed = False
        names = {v for c in body for v in (c["src"], c["trg"])}
        for v in sorted(names - need):
            inc = [c for c in body if v in (c["src"], c["trg"])]
            if len(inc) != 2 or any(c["src"] == c["trg"] for c in inc):
                continue
            c1, c2 = inc
            c1 = c1 if c1["trg"] == v else _flip(c1)        # a -> v
            c2 = c2 if c2["src"] == v else _flip(c2)        # v -> b
            body = [c for c in body if c not in inc] + [
                {"src": c1["src"], "trg": c2["trg"], "chain": [c1, c2]}]
            changed = True
            break
    return body


def _existence(g: Graph, body: list[dict[str, Any]], need: set[str]):
    """For a tree-shaped body, the conjuncts that lead only to existential
    variables, as a memoised test per (variable, node): `ok(v, n)` holds iff
    every such branch below `v` has a path from `n` to a node passing the
    branch's own test. Returns (the branch conjuncts by variable, ok), or
    None when the body has a cycle."""
    names = sorted({v for c in body for v in (c["src"], c["trg"])})
    if len(body) != len(names) - 1:
        return None
    root = next((v for c in body for v in (c["src"], c["trg"]) if v in need), body[0]["src"])
    edges: dict[str, list[dict[str, Any]]] = {v: [] for v in names}
    for c in body:
        edges[c["src"]].append(c)
        edges[c["trg"]].append(_flip(c))
    parent = {root: None}
    stack = [root]
    while stack:
        v = stack.pop()
        for c in edges[v]:
            if c["trg"] not in parent:
                parent[c["trg"]] = v
                stack.append(c["trg"])
    holds: dict[str, bool] = {}

    def has_need(v: str) -> bool:
        if v not in holds:
            holds[v] = v in need or v == root or any(
                has_need(c["trg"]) for c in edges[v] if parent.get(c["trg"]) == v)
        return holds[v]

    branches = {v: [c for c in edges[v] if parent.get(c["trg"]) == v and not has_need(c["trg"])]
                for v in names}
    memo: dict[tuple[str, str], bool] = {}

    def ok(v: str, n: str) -> bool:
        key = (v, n)
        if key not in memo:
            memo[key] = all(any(ok(c["trg"], w) for w in g.conjunct(c, n))
                            for c in branches[v])
        return memo[key]

    kept = [c for c in body if has_need(c["src"]) and has_need(c["trg"])]
    return kept, ok, root


def evaluate(g: Graph, q: dict[str, Any]) -> list[tuple[str, ...]]:
    """Distinct head tuples over the union of bodies. A body's bindings are
    built conjunct by conjunct (a bound target checked, a free one bound),
    as a set over the variables still needed: the head's and those of the
    conjuncts left (the others are existential). In a tree-shaped body, a
    branch reaching no needed variable is a memoised existence test on the
    variable it hangs from, applied as that variable binds."""
    answers: set[tuple[str, ...]] = set()
    head = q["head"]
    sys.setrecursionlimit(100_000)
    for body in q["bodies"]:
        if not body:
            continue
        tree = _existence(g, body, set(head))
        if tree is None:
            # a cycle: compose its existential variables of degree two away
            body = _eliminate(body, set(head))
            tree = _existence(g, body, set(head))
        def ok(v: str, n: str) -> bool:
            return True
        root = None
        if tree is not None:
            body, ok, root = tree
        if not body:
            # a single needed variable (the tree's root): the nodes passing its tests
            v = root
            root_ok = [n for n in g.nodes if ok(v, n)]
            if root_ok:
                answers.update((n,) if head else () for n in root_ok)
            continue
        todo = list(body)
        first = todo[0]["src"]
        names = [first]
        bindings: set[tuple[str, ...]] = {(n,) for n in g.nodes if ok(first, n)}
        while todo and bindings:
            c = next(x for x in todo if x["src"] in names or x["trg"] in names)
            todo.remove(c)
            src, trg, cc = c["src"], c["trg"], c
            if src not in names:
                src, trg, cc = trg, src, _flip(c)
            need = set(head) | {v for x in todo for v in (x["src"], x["trg"])}
            out_names = [v for v in names if v in need]
            if trg not in names and trg in need:
                out_names.append(trg)
            si = names.index(src)
            ti = names.index(trg) if trg in names else None
            keep = [names.index(v) for v in out_names if v in names]
            memo: dict[str, set[str]] = {}
            nxt: set[tuple[str, ...]] = set()
            for b in bindings:
                u = b[si]
                if u not in memo:
                    memo[u] = {w for w in g.conjunct(cc, u) if ok(trg, w)}
                base = tuple(b[i] for i in keep)
                if ti is not None:
                    if b[ti] in memo[u]:
                        nxt.add(base)
                elif trg in need:
                    nxt.update(base + (v,) for v in memo[u])
                elif memo[u]:
                    nxt.add(base)
            bindings, names = nxt, out_names
        for b in bindings:
            answers.add(tuple(b[names.index(v)] for v in head))
    return sorted(answers)


def digest_rows(head: list[str], rows: list[tuple]) -> str:
    text = json.dumps([head, [list(r) for r in rows]], separators=(",", ":"))
    return hashlib.sha256(text.encode()).hexdigest()[:24]


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def _payload_rows(q: dict[str, Any], payload: dict[str, Any]) -> list[tuple]:
    if not q["head"]:
        return [()] if payload["rows"] else []
    return sorted(tuple(r[v] for v in q["head"]) for r in payload["rows"])


def main() -> int:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("build")
    b.add_argument("--graph", required=True)
    b.add_argument("--out", required=True)
    b.add_argument("--retract-pct", type=float, default=5.0)
    e = sub.add_parser("emit")
    e.add_argument("--workload", required=True)
    r = sub.add_parser("run")
    r.add_argument("--store", required=True)
    r.add_argument("--workload", required=True)
    r.add_argument("--route", default="native")
    r.add_argument("--asof", default="now", choices=["load", "now"])
    r.add_argument("--queries", default="")
    r.add_argument("--json", default="")
    r.add_argument("--variant", default="as_written", choices=VARIANTS)
    c = sub.add_parser("check")
    c.add_argument("--graph", required=True)
    c.add_argument("--workload", required=True)
    c.add_argument("--asof", default="now", choices=["load", "now"])
    c.add_argument("--retract-pct", type=float, default=5.0)
    c.add_argument("--queries", default="")
    c.add_argument("--variant", default="as_written", choices=VARIANTS)
    a = ap.parse_args()

    if a.cmd == "build":
        build(Path(a.graph), Path(a.out), a.retract_pct)
        return 0
    qs = parse_workload(Path(a.workload))
    want = {int(x) for x in a.queries.split(",") if x} if getattr(a, "queries", "") else None
    if a.cmd == "emit":
        print(json.dumps([{"id": q["id"], "dropped": q["dropped"], "args": match_args(q, None)}
                          for q in qs], indent=1))
        return 0
    if a.cmd == "check":
        ts = triples(Path(a.graph))
        nodes = {x for s_, _p, o in ts for x in (s_, o)}
        if a.asof == "now":
            gone = set(retracted(ts, a.retract_pct))
            ts = [t for i, t in enumerate(ts) if i not in gone]
        g = Graph(ts, nodes)
        for q in qs:
            if want is not None and q["id"] not in want:
                continue
            t0 = time.perf_counter()
            if a.variant == "count_rows":
                n = count_rows(g, q)
                rec = {"hash": digest_rows(["count"], [(n,)]), "rows": 1, "rows_total": n}
            else:
                qv = first_column(q) if a.variant == "count_col" else q
                rows = evaluate(g, qv)
                rec = {"hash": digest_rows(qv["head"], rows), "rows": len(rows), "rows_total": len(rows)}
            print(json.dumps({"query": q["id"], "asof": a.asof, "variant": a.variant,
                              "route": "independent", **rec,
                              "ms": round((time.perf_counter() - t0) * 1e3, 1)}), flush=True)
        return 0

    import gnomon
    from gnomon.temporal import ops_match as OM
    from gnomon.temporal.algebra import ensure_all_registered

    ensure_all_registered()
    store = gnomon.open(Path(a.store), backend="native", read_only=True)
    sink = open(a.json, "a") if a.json else None
    try:
        # the load's last batch: the newest belief time before the retract
        # batch, which is the newest of all (the retract writes edge versions)
        tts = sorted({int(t) for t in store.adapter.versions_columnar("edge")["tt_s"]})
        as_of = (tts[-2] if len(tts) > 1 else tts[-1]) if a.asof == "load" else None
        for q in qs:
            if want is not None and q["id"] not in want:
                continue
            if a.variant == "count_col":
                q = first_column(q)
            args = count_args(q, as_of) if a.variant == "count_rows" else match_args(q, as_of)
            before = OM.ROUTES["native"]
            t0 = time.perf_counter()
            pages = 0
            if args is None:
                payload = {"rows": [], "rows_total": 0, "truncated": False}
            else:
                # every page; the page is the loaded engine's own match maximum (as the W3 driver)
                args["limit"] = max(int(args.get("limit") or 1), int(getattr(OM, "MAX_LIMIT", PAGE))) \
                    if a.variant != "count_rows" else args["limit"]
                rows_all: list = []
                while True:
                    payload = OM.match(store.adapter, args, route=a.route,
                                       reference_bypass="tgmark run" if a.route != "native" else None)
                    pages += 1
                    rows_all += payload["rows"]
                    if not (payload.get("truncated") and payload.get("cursor")):
                        break
                    args["cursor"] = payload["cursor"]
                payload = {**payload, "rows": rows_all, "truncated": False, "cursor": None}
            ms = (time.perf_counter() - t0) * 1e3
            if a.variant == "count_rows":
                n = int((payload["rows"][0]["count"] if payload["rows"] else 0) or 0)
                payload = {**payload, "rows_total": n}
                rows, head = [(n,)], ["count"]
            else:
                rows, head = _payload_rows(q, payload), q["head"]
            rec = {"query": q["id"], "asof": a.asof, "variant": a.variant,
                   "bodies": len(q["bodies"]),
                   "dropped": q["dropped"], "route":
                   ("native" if OM.ROUTES["native"] > before or args is None else "reference"),
                   "hash": digest_rows(head, rows), "rows": len(rows),
                   "rows_total": payload["rows_total"], "truncated": payload["truncated"], "pages": pages,
                   "ms": round(ms, 1)}
            if a.route == "native" and rec["route"] != "native":
                rec["declined"] = OM.LAST_DECLINE[0]
            print(json.dumps(rec), flush=True)
            if sink:
                sink.write(json.dumps(rec) + "\n")
    finally:
        store.close()
        if sink:
            sink.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
