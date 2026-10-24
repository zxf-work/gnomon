"""Regular path expressions over typed edges, for the pattern operator's
`Walk` node.

An expression is JSON:

- `{"sym": r}` one edge of relationship type `r` traversed forward, and
  `{"sym": r, "inv": true}` one traversed backward;
- `{"seq": [e, ...]}` concatenation, `{"alt": [e, ...]}` alternation;
- `{"star": e}` zero or more, `{"plus": e}` one or more, `{"opt": e}` zero or
  one.

`canonical` validates one and returns its canonical form (and the text a
node digest is taken over). `nfa` builds a Thompson automaton: the
evaluators search the product of the graph and its states breadth-first,
and a target is any node reached in the accepting state.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from gnomon.core.errors import InvalidArgError

MAX_NODES = 256
_KEYS = {"sym", "seq", "alt", "star", "plus", "opt"}


def canonical(spec: Any) -> dict[str, Any]:
    """The expression, validated, in canonical form (one key per node,
    `inv` present only when true)."""
    count = [0]

    def walk(e: Any) -> dict[str, Any]:
        count[0] += 1
        if count[0] > MAX_NODES:
            raise InvalidArgError(f"a path expression has at most {MAX_NODES} parts")
        if not isinstance(e, dict):
            raise InvalidArgError("a path expression part is an object")
        keys = set(e) & _KEYS
        if len(keys) != 1:
            raise InvalidArgError("a path expression part has exactly one of "
                                  f"{sorted(_KEYS)}", got=sorted(e))
        k = keys.pop()
        extra = set(e) - {k} - ({"inv"} if k == "sym" else set())
        if extra:
            raise InvalidArgError(f"unknown path expression field(s) {sorted(extra)}")
        if k == "sym":
            if not isinstance(e["sym"], str) or not e["sym"]:
                raise InvalidArgError("sym names a relationship type")
            inv = e.get("inv", False)
            if not isinstance(inv, bool):
                raise InvalidArgError("inv is true or false")
            return {"sym": e["sym"], "inv": True} if inv else {"sym": e["sym"]}
        if k in ("seq", "alt"):
            parts = e[k]
            if not isinstance(parts, list) or not parts:
                raise InvalidArgError(f"{k} takes a non-empty list")
            return {k: [walk(p) for p in parts]}
        return {k: walk(e[k])}

    return walk(spec)


def text(spec: dict[str, Any]) -> str:
    """The canonical JSON text of a canonical expression."""
    return json.dumps(spec, sort_keys=True, separators=(",", ":"))


@dataclass(frozen=True)
class NFA:
    """States `0..n-1`; `eps[q]` the empty moves, `moves[q]` the
    `(rel_type, inverse, q')` moves; one start and one accepting state."""

    n: int
    start: int
    accept: int
    eps: tuple[tuple[int, ...], ...]
    moves: tuple[tuple[tuple[str, bool, int], ...], ...]

    def closure(self, states: set[int]) -> set[int]:
        out = set(states)
        stack = list(states)
        while stack:
            q = stack.pop()
            for r in self.eps[q]:
                if r not in out:
                    out.add(r)
                    stack.append(r)
        return out


def nfa(spec: dict[str, Any]) -> NFA:
    """Thompson's construction over a canonical expression."""
    eps: list[list[int]] = []
    moves: list[list[tuple[str, bool, int]]] = []

    def state() -> int:
        eps.append([])
        moves.append([])
        return len(eps) - 1

    def build(e: dict[str, Any]) -> tuple[int, int]:
        if "sym" in e:
            s, t = state(), state()
            moves[s].append((e["sym"], bool(e.get("inv", False)), t))
            return s, t
        if "seq" in e:
            first = last = None
            for p in e["seq"]:
                s, t = build(p)
                if first is None:
                    first = s
                else:
                    eps[last].append(s)          # type: ignore[index]
                last = t
            return first, last                   # type: ignore[return-value]
        if "alt" in e:
            s, t = state(), state()
            for p in e["alt"]:
                ps, pt = build(p)
                eps[s].append(ps)
                eps[pt].append(t)
            return s, t
        inner = e.get("star") or e.get("plus") or e.get("opt")
        s, t = state(), state()
        ps, pt = build(inner)
        eps[s].append(ps)
        eps[pt].append(t)
        if "star" in e or "opt" in e:
            eps[s].append(t)
        if "star" in e or "plus" in e:
            eps[pt].append(ps)
        return s, t

    start, accept = build(spec)
    return NFA(len(eps), start, accept, tuple(tuple(x) for x in eps),
               tuple(tuple(x) for x in moves))


def reverse(spec: dict[str, Any]) -> dict[str, Any]:
    """The expression read from its other end: each edge inverted, each
    concatenation reversed."""
    if "sym" in spec:
        return {"sym": spec["sym"]} if spec.get("inv") else {"sym": spec["sym"], "inv": True}
    if "seq" in spec:
        return {"seq": [reverse(p) for p in reversed(spec["seq"])]}
    if "alt" in spec:
        return {"alt": [reverse(p) for p in spec["alt"]]}
    (k, inner), = spec.items()
    return {k: reverse(inner)}


def symbols(spec: dict[str, Any]) -> list[tuple[str, bool]]:
    """The distinct `(rel_type, inverse)` an expression traverses."""
    out: list[tuple[str, bool]] = []

    def walk(e: dict[str, Any]) -> None:
        if "sym" in e:
            key = (e["sym"], bool(e.get("inv", False)))
            if key not in out:
                out.append(key)
            return
        for k in ("seq", "alt"):
            for p in e.get(k, []):
                walk(p)
        for k in ("star", "plus", "opt"):
            if k in e:
                walk(e[k])

    walk(spec)
    return out


__all__ = ["MAX_NODES", "NFA", "canonical", "nfa", "reverse", "symbols", "text"]
