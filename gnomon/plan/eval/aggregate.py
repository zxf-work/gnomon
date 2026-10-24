"""`Aggregate`.

Four properties the spec fixes, each of which an implementation loses first:

- **Group-by arity is unrestricted.** Today's two-slot cap is an
  operator-boundary artifact, not a semantic gap. `group_by = []` yields
  exactly one row; `aggregates = []` is `DISTINCT` over the key.
- **Non-empty groups only.** There is no densified group axis and no bucket
  generator in v1, which is exactly why `graph_metric_timeseries`,
  `burst_detection` and `neighborhood_evolution`'s degree series stay opaque:
  their series are *dense*, zeros included.
- **`mean` is an atomic aggregate** — defined as `sum/count` but computed as
  one aggregate rather than composed from two aggregate columns, because
  written the composed way it would be `arithmetic-over-aggregates` and beyond
  v1. It calls `ops_aggregate._mean`, "the one blessed mean": a second
  implementation would diverge in the ninth decimal and break digest stability
  across the Rust/Python/oracle triad. An **empty group emits no row at all**,
  so `mean` never yields NaN and never raises a division error.
- **Canonical order: by group key values** — numeric order for numeric keys,
  code-point for strings, **nulls first** — matching `aggregate_events` today.

The execution-completeness precondition ("an aggregate is computed over a
relation, never over a page") is enforced by `propagate.meta_for` before this
runs; the *static* half — an `Aggregate` over a page-cut `Limit` — was already
refused at construction.
"""

from __future__ import annotations

from typing import Any

import numpy as np

from gnomon.core.errors import InvalidArgError
from gnomon.plan.eval.expr_eval import eval_expr
from gnomon.plan.node import Aggregate
from gnomon.plan.relation import Relation, array_for
from gnomon.plan.types import Column, Schema, T_FLOAT, T_INT


def eval_aggregate(node: Aggregate, rel: Relation) -> Relation:
    keys, key_nulls, key_columns = _key_columns(node, rel)
    groups, order = _group(keys, key_nulls, rel.n)

    out_cols: dict[str, np.ndarray] = {}
    out_nulls: dict[str, np.ndarray] = {}
    columns: list[Column] = []

    for position, (name, _expr) in enumerate(node.group_by):
        tau = key_columns[position].tau
        values = [keys[position][rows[0]] for rows in groups]
        nulls = np.array([bool(key_nulls[position][rows[0]])
                          if key_nulls[position] is not None else False
                          for rows in groups])
        columns.append(Column(name, tau))
        out_cols[name] = array_for(tau, values) if not nulls.any() \
            else np.array(values, dtype=object)
        if nulls.any():
            out_nulls[name] = nulls

    for agg in node.aggregates:
        tau = agg.tau(rel.schema)
        if agg.fn == "collect":
            columns.append(Column(agg.alias, tau))
            lists = np.empty(len(groups), dtype=object)    # one list per group, 1-D
            lists[:] = _collect(agg, rel, groups)
            out_cols[agg.alias] = lists
            continue
        values, nulls = _apply(agg, rel, groups)
        columns.append(Column(agg.alias, tau))
        out_cols[agg.alias] = array_for(tau, values)
        if nulls is not None:
            out_nulls[agg.alias] = nulls

    del order
    return Relation(Schema(tuple(columns)), out_cols, len(groups), out_nulls)


def _key_columns(node: Aggregate, rel: Relation) -> tuple[list, list, list[Column]]:
    keys, nulls, columns = [], [], []
    for name, expr in node.group_by:
        values, null_mask = eval_expr(expr, rel)
        keys.append(values)
        nulls.append(null_mask)
        columns.append(Column(name, expr.tau(rel.schema)))
    return keys, nulls, columns


def _group(keys: list, key_nulls: list, n: int) -> tuple[list[list[int]], np.ndarray]:
    """Group rows by key tuple, then order the groups canonically.

    Ordering is over **integer codes**, never over the key values themselves —
    the idiom `ops_aggregate._portable_dim_codes` already uses: codes
    are never strings, so the group order is a sort over ints, and `-1` is the
    null code, which sorts first.
    """
    if not keys:
        # `group_by = []` yields exactly one row — and **no** row for an empty
        # input, since non-empty groups only
        return ([list(range(n))] if n else []), np.zeros(0, dtype=np.int64)

    buckets: dict[tuple, list[int]] = {}
    for row in range(n):
        key = tuple(None if (key_nulls[i] is not None and key_nulls[i][row])
                    else _scalar(keys[i][row]) for i in range(len(keys)))
        buckets.setdefault(key, []).append(row)

    ordered = sorted(buckets, key=_sort_key)
    return [buckets[key] for key in ordered], np.zeros(0, dtype=np.int64)


def _sort_key(key: tuple) -> tuple:
    """Nulls first, then numerics in numeric order, then strings by code point.

    A tuple of `(rank, value)` pairs keeps mixed-type key columns totally
    ordered without ever comparing a str to an int — which is what a bare
    `sorted()` over the raw tuples would do, and what raises.
    """
    out = []
    for value in key:
        if value is None:
            out.append((0, 0))
        elif isinstance(value, bool):
            out.append((1, int(value)))
        elif isinstance(value, (int, float)):
            out.append((1, value))
        else:
            out.append((2, str(value)))
    return tuple(out)


def _apply(agg: Any, rel: Relation, groups: list[list[int]]) -> tuple[list, Any]:
    from gnomon.temporal.ops_aggregate import _mean

    if agg.fn == "count":
        return [len(rows) for rows in groups], None

    values, nulls = eval_expr(agg.of, rel)
    defined = (np.ones(rel.n, dtype=bool) if nulls is None else ~nulls)

    out: list[Any] = []
    out_nulls = np.zeros(len(groups), dtype=bool)
    for position, rows in enumerate(groups):
        live = [_scalar(values[r]) for r in rows if defined[r]]
        if agg.fn == "count_distinct":
            out.append(len(set(live)))
            continue
        if not live:
            # every contributing value was null: there is no value to report,
            # and inventing 0 would be a densified group by the back door
            out.append(None)
            out_nulls[position] = True
            continue
        if agg.fn == "min":
            out.append(min(live))
        elif agg.fn == "max":
            out.append(max(live))
        elif agg.fn == "sum":
            out.append(sum(live))
        elif agg.fn == "sum_exact":
            if all(isinstance(v, int) and not isinstance(v, bool) for v in live):
                out.append(sum(live))                     # exact integer sum
            else:
                out.append(math_fsum(live))               # correctly rounded
        elif agg.fn == "mean":
            if all(isinstance(v, int) and not isinstance(v, bool) for v in live):
                out.append(_mean(sum(live), len(live)))   # the one blessed mean
            else:
                out.append(math_fsum(live) / len(live))
        else:  # pragma: no cover - the node layer validates the function
            raise InvalidArgError(f"unknown aggregate {agg.fn!r}")
    return out, (out_nulls if out_nulls.any() else None)


def _collect(agg: Any, rel: Relation, groups: list[list[int]]) -> list[list[Any]]:
    """`collect`: per group, the elements of the rows whose components are
    not all null (a one-component element is its value, a longer one the
    list of its values), ordered by the aggregate's keys and then by the
    components themselves (ascending, nulls first), equal elements kept once
    when `distinct`, cut at `limit`."""
    import functools

    items = [eval_expr(e, rel) for e in agg.of.items]
    keys = [(eval_expr(k.key, rel), k) for k in agg.order]

    def val(col: Any, i: int) -> Any:
        values, nulls = col
        return None if (nulls is not None and nulls[i]) else _scalar(values[i])

    def cmp_one(x: Any, y: Any, desc: bool, nulls_first: bool) -> int:
        if x is None and y is None:
            return 0
        if x is None or y is None:
            first = (x is None) == nulls_first
            return -1 if first else 1
        if x == y:
            return 0
        c = -1 if x < y else 1
        return -c if desc else c

    def cmp(a: int, b: int) -> int:
        for col, k in keys:
            c = cmp_one(val(col, a), val(col, b), k.direction == "desc",
                        k.nulls == "nulls_first")
            if c:
                return c
        for col in items:
            c = cmp_one(val(col, a), val(col, b), False, True)
            if c:
                return c
        return (a > b) - (a < b)

    out: list[list[Any]] = []
    for rows in groups:
        live = [r for r in rows if any(val(c, r) is not None for c in items)]
        live.sort(key=functools.cmp_to_key(cmp))
        elems: list[Any] = []
        seen: list[Any] = []
        for r in live:
            comps = [val(c, r) for c in items]
            e = comps[0] if len(comps) == 1 else comps
            if agg.distinct:
                if e in seen:
                    continue
                seen.append(e)
            elems.append(e)
            if agg.limit is not None and len(elems) >= agg.limit:
                break
        out.append(elems)
    return out


def math_fsum(values: list[Any]) -> float:
    """The IR's blessed rule for the float case: "where any value is a `float`,
    sums use `math.fsum` and one division follows"."""
    import math

    return math.fsum(float(v) for v in values)


def _scalar(value: Any) -> Any:
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return float(value)
    if isinstance(value, np.bool_):
        return bool(value)
    return value


__all__ = ["eval_aggregate"]

_ = (T_INT, T_FLOAT)   # the aggregate τs the node layer assigns
