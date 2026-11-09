"""Write the W3 LadybugDB texts from the Neo4j texts by the mechanical dialect rules of D-W3-13.

    w3_ladybug_texts.py [--check]

For every Neo4j text `cypher/<Q>.cypher` (instant settings and the TPC-BiH-style rows) and `cypher/window/<Q>.cypher`
(window setting) this writes `cypher/ladybug/<Q>.cypher` and `cypher/window/ladybug/<Q>.cypher`, unless that file
starts with `// HAND` (a hand-written LadybugDB text; its header states its own changes). Each written file starts
with the rules it applied. The rules:

  R1  label unions: `:Message` -> `:Post:Comment`, `:Place` -> `:City:Country:Continent`, `:Organisation` ->
      `:Company:University` (LadybugDB has one table per node label and no label hierarchy);
  R2  functions: toInteger/toString/toFloat -> CAST(... AS INT64 / STRING / DOUBLE), split -> string_split,
      one-argument round(x) -> round(x, 0), head(l) -> list_extract(l, 1), relationships(p) -> rels(p),
      duration({days|hours: n}) -> to_days / to_hours(n), x.year / x.month / x.day -> date_part('year', x), ...;
  R3  chained comparisons `$a > x >= $b` -> `x < $a AND x >= $b`;
  R4  list comprehensions -> list_transform / list_filter;
  R5  variable-length steps: Neo4j's relationship uniqueness is LadybugDB's TRAIL semantics (`* TRAIL a..b`); the
      VIS / belief test on the walk's elements moves into the step's own predicate `(_r, _n | WHERE ...)` (the
      endpoints keep their own test), so the walk is filtered while it is expanded;
  R6  relationship uniqueness inside one MATCH clause (Neo4j's rule; LadybugDB binds fixed-length patterns with walk
      semantics): two relationships of one type in one clause get `a.rid <> b.rid` (rid is the loader's key of a
      relationship copy).
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
CY = ROOT / "benchmarks/workloads/snb/cypher"

KW = r"(?:OPTIONAL\s+MATCH|MATCH|WHERE|WITH|RETURN|UNWIND|CALL|ORDER\s+BY|UNION|LIMIT|SKIP|SET)"


def _macro(text: str, name: str, fn) -> str:
    from w3_snb_systems import macro
    return macro(text, name, fn)


def r1_labels(t: str) -> str:
    t = re.sub(r"(\(\s*\w*\s*):Message\b", r"\1:Post:Comment", t)
    t = re.sub(r"(\(\s*\w*\s*):Place\b", r"\1:City:Country:Continent", t)
    t = re.sub(r"(\(\s*\w*\s*):Organisation\b", r"\1:Company:University", t)
    return t


def r2_functions(t: str) -> str:
    t = _macro(t, "toInteger", lambda a: f"CAST({a[0]} AS INT64)")
    t = _macro(t, "toString", lambda a: f"CAST({a[0]} AS STRING)")
    t = _macro(t, "toFloat", lambda a: f"CAST({a[0]} AS DOUBLE)")
    t = _macro(t, "round", lambda a: f"round({a[0]}, 0)" if len(a) == 1 else f"round({', '.join(a)})")
    t = _macro(t, "head", lambda a: f"list_extract({a[0]}, 1)")
    t = re.sub(r"\bsplit\(", "string_split(", t)
    t = re.sub(r"\b(\w+):(Post|Comment)\s+AS\b", r"label(\1) = '\2' AS", t)
    t = re.sub(r"\brelationships\(", "rels(", t)
    t = re.sub(r"duration\(\{\s*days\s*:\s*([^}]+)\}\)", r"to_days(\1)", t)
    t = re.sub(r"duration\(\{\s*hours\s*:\s*([^}]+)\}\)", r"to_hours(\1)", t)
    t = re.sub(r"(\$\w+|\b[a-z]\w*\.\w+)\.(year|month|day)\b", r"date_part('\2', \1)", t)
    return t


def r3_chained(t: str) -> str:
    return re.sub(r"(\$\w+)\s*>\s*([\w.]+)\s*>=\s*(\$\w+)", r"\2 < \1 AND \2 >= \3", t)


def r4_comprehensions(t: str) -> str:
    def one(m):
        v, lst, rest = m.group(1), m.group(2), m.group(3)
        where, _, proj = rest.partition("|")
        where = where.strip()
        if where.upper().startswith("WHERE"):
            where = where[5:].strip()
        out = lst.strip()
        if where:
            out = f"list_filter({out}, {v} -> {where})"
        if proj.strip():
            out = f"list_transform({out}, {v} -> {proj.strip()})"
        return out
    pat = re.compile(r"\[\s*(\w+)\s+IN\s+((?:[^\[\]|]|\[[^\[\]]*\])+?)\s+((?:WHERE\s+[^\[\]|]+)?\|?[^\[\]]*)\]")
    prev = None
    while prev != t:
        prev = t
        t = pat.sub(lambda m: one(m) if (" WHERE " in " " + m.group(3) or "|" in m.group(3)) else m.group(0), t)
    return t


def _clauses(t: str) -> list[tuple[int, int]]:
    """(start, end) of each MATCH / OPTIONAL MATCH clause's pattern part (up to its WHERE or next clause)."""
    out = []
    for m in re.finditer(r"\b(OPTIONAL\s+MATCH|MATCH)\b", t):
        nxt = re.compile(r"\b" + KW + r"\b").search(t, m.end())
        out.append((m.end(), nxt.start() if nxt else len(t)))
    return out


def _endpoint_vars(path_src: str) -> list[str]:
    return [v for v in re.findall(r"\(\s*(\w+)", path_src)]


def r5_walks(t: str, window: bool) -> str:
    """`w = (A)-[:T*r]-(B)` with VISALL(nodes(w)) AND VISALL(rels(w)) -> `w = (A)-[:T* TRAIL r (_r, _n | WHERE VIS(_r, _n))]-(B)`
    and VIS(A, B); in a window text the step gets the belief + overlap pruning WPRUNE(_r, _n), the exact co-valid test
    on the walk stays in W_OK / W_INV."""
    pat = re.compile(r"(\b\w+)\s*=\s*(\([^()]*\))\s*(<?-)\[\s*(\w*)\s*:\s*([\w|:]+)\s*\*\s*(\d*\s*\.\.\s*\d*|\d+)\s*\](->?)\s*(\([^()]*\))")
    for m in list(pat.finditer(t))[::-1]:
        w, a, l, var, types, rng, r, b = m.groups()
        filt = "WPRUNE(_r, _n)" if window else "VIS(_r, _n)"
        rng = rng.replace(" ", "")
        if rng.endswith(".."):
            rng += "30"
        new = f"{w} = {a}{l}[{var}:{types}* TRAIL {rng} (_r, _n | WHERE {filt})]{r}{b}"
        t = t[:m.start()] + new + t[m.end():]
        if not window:
            ends = _endpoint_vars(a) + _endpoint_vars(b)
            t = re.sub(rf"VISALL\(nodes\({w}\)\)\s+AND\s+VISALL\(rels\({w}\)\)", f"VIS({', '.join(ends)})", t)
            t = re.sub(rf"VISALL\(rels\({w}\)\)\s+AND\s+VISALL\(nodes\({w}\)\)", f"VIS({', '.join(ends)})", t)
    return t


def r6_unique(t: str) -> tuple[str, list[str]]:
    notes = []
    for s, e in _clauses(t)[::-1]:
        clause = t[s:e]
        rels = re.findall(r"\[\s*(\w+)\s*:\s*(\w+)\s*(?:\{[^}]*\})?\s*\]", clause)
        walks = re.findall(r"(\b\w+)\s*=\s*\([^()]*\)\s*<?-\[\s*\w*\s*:\s*([\w|]+)\s*\*", clause)
        conds = []
        by: dict[str, list[str]] = {}
        for v, ty in rels:
            by.setdefault(ty, []).append(v)
        for ty, vs in by.items():
            for i in range(len(vs)):
                for j in range(i + 1, len(vs)):
                    conds.append(f"{vs[i]}.rid <> {vs[j]}.rid")
        for w, tys in walks:
            for ty in tys.split("|"):
                for v in by.get(ty, []):
                    conds.append(f"NOT list_contains(properties(rels({w}), 'rid'), {v}.rid)")
        if not conds:
            continue
        notes.append(", ".join(conds))
        add = " AND ".join(conds)
        m = re.match(r"\s*WHERE\b", t[e:])
        if m:
            t = t[:e + m.end()] + f" {add} AND" + t[e + m.end():]
        else:
            t = t[:e].rstrip() + f"\nWHERE {add}\n" + t[e:]
    return t, notes


def translate(src: str, window: bool) -> tuple[str, list[str]]:
    body = "\n".join(l for l in src.splitlines() if not l.lstrip().startswith("//"))
    body = re.sub(r"\s+//[^\n'\"]*$", "", body, flags=re.M)
    applied = []
    for name, fn in (("R1", r1_labels), ("R2", r2_functions), ("R3", r3_chained), ("R4", r4_comprehensions)):
        new = fn(body)
        if new != body:
            applied.append(name)
        body = new
    new = r5_walks(body, window)
    if new != body:
        applied.append("R5")
    body = new
    body, notes = r6_unique(body)
    if notes:
        applied.append("R6 (" + "; ".join(notes) + ")")
    return body, applied


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--check", action="store_true", help="report what would change; write nothing")
    a = ap.parse_args()
    jobs = [(p, CY / "ladybug" / p.name, False) for p in sorted(CY.glob("*.cypher"))]
    jobs += [(p, CY / "window" / "ladybug" / p.name, True) for p in sorted((CY / "window").glob("*.cypher"))]
    for src, dst, window in jobs:
        if dst.exists() and dst.read_text().startswith(("// HAND", "// NOT EXPRESSIBLE")):
            print(f"{dst.relative_to(CY)}: hand-written, kept")
            continue
        body, applied = translate(src.read_text(), window)
        head = (f"// {src.stem} for LadybugDB ({'window' if window else 'instant settings'}): written by "
                f"scripts/w3_ladybug_texts.py from {src.relative_to(CY)}.\n// Mechanical rules (DEVIATIONS D-W3-13): "
                f"{', '.join(applied) if applied else 'none'}.\n")
        out = head + body.strip() + "\n"
        if a.check:
            print(f"{dst.relative_to(CY)}: {applied}")
            continue
        dst.parent.mkdir(parents=True, exist_ok=True)
        dst.write_text(out)
        print(f"{dst.relative_to(CY)}: {applied}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
