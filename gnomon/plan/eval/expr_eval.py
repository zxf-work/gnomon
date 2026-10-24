"""The expression evaluator — vectorized, one numpy operation per AST node.

`gnomon/plan/expr.py` carries the typed AST and no evaluator; this is the
evaluator. It returns `(values, null_mask_or_None)` so a caller never has to ask
whether a column can be null before reading it.

**Error semantics.** These are the
rules that decide what a *wrong* row does, and every one of them fails the plan
loudly rather than quietly producing a row:

- **Division by zero is `E_ARG`**, never a null and never a dropped row. Same
  for a non-finite float result, and for `cast(uid, int)` on an identity string
  that is not a canonical decimal integer (row-determining for the LDBC
  queries IC11, IC12, IS3, IC2 and IS2).
- **`if` short-circuits per row**: each branch is evaluated only on the row mask
  where it is selected. The IR requires every expression to be pure, row-local and
  **total**, and a vectorized `if(c, a/b, 0)` that evaluated both branches over
  all rows would raise on rows the guard excludes. Short-circuiting restores
  totality for an author who guards, at the cost of one mask.
- **A null operand yields a null result**, never an error — nulls are values,
  and `is_null`/`coalesce` are how a plan tests for them. The error forms above
  are about *defined* inputs producing no answer.

**The blessed arithmetic rule is reused, not reimplemented**:
where every contributing value is an `int`, a quotient is formed in exact
integer arithmetic and returned as `int` when exact, otherwise with exactly one
IEEE rounding in the form `q, r = divmod(num, den) → float(q) + r/den`. A second
implementation would diverge in the ninth decimal and break digest stability
across the Rust/Python/oracle triad, so `ops_compute._quotient` is imported
rather than copied.
"""

from __future__ import annotations

from typing import Any

import numpy as np

from gnomon.core.errors import InternalError, InvalidArgError
from gnomon.plan.expr import (
    Arith, BoolOp, Cast, Cmp, Coalesce, Col, Expr, If, IsNull, Lit, MathFn, Not,
    Calendar, PropRef, StartsWith, StrFn, TupleExpr, TypedProp,
)
from gnomon.plan.relation import Relation, combine_nulls

#: `(values, nulls)`; `nulls is None` means "nothing in this column is null".
Value = tuple[np.ndarray, "np.ndarray | None"]


def eval_expr(e: Expr, rel: Relation) -> Value:
    """Evaluate one expression over every row of `rel`."""
    return _eval(e, rel, None)


def eval_predicate(e: Expr, rel: Relation) -> np.ndarray:
    """A boolean predicate as a row mask.

    **A null result is not `true`**: rows whose predicate evaluates to
    null are dropped, exactly as SQL's three-valued logic would, and exactly as
    the spec's one sentence on the subject requires.
    """
    values, nulls = eval_expr(e, rel)
    mask = np.asarray(values, dtype=bool) if rel.n else np.zeros(0, dtype=bool)
    if nulls is not None:
        mask = mask & ~nulls
    return mask


def _eval(e: Expr, rel: Relation, rows: np.ndarray | None) -> Value:
    """`rows` is the sub-selection an `if` branch is evaluated over, or None for
    the whole relation."""
    sub = rel if rows is None else rel.take(rows)
    n = sub.n

    if isinstance(e, Lit):
        tau = e.lit_tau
        if e.value is None:
            return np.full(n, None, dtype=object), np.ones(n, dtype=bool)
        dtype = np.int64 if tau is not None and tau.base in ("int", "ts") else None
        if isinstance(e.value, bool):
            return np.full(n, e.value, dtype=bool), None
        if dtype is not None:
            return np.full(n, e.value, dtype=np.int64), None
        if isinstance(e.value, float):
            return np.full(n, e.value, dtype=np.float64), None
        return np.full(n, e.value, dtype=object), None

    if isinstance(e, Col):
        return sub.column(e.name), sub.null_mask(e.name)

    if isinstance(e, PropRef):
        # `var.props[k]` — the parsed bag is an object array of dicts, so this
        # is a per-row dict lookup and there is no vectorized form of it.
        bags = sub.column(e.column)
        bag_nulls = sub.null_mask(e.column)
        out = np.empty(n, dtype=object)
        missing = np.zeros(n, dtype=bool)
        for i in range(n):
            bag = bags[i]
            if (bag_nulls is not None and bag_nulls[i]) or not isinstance(bag, dict) \
                    or e.key not in bag:
                out[i], missing[i] = None, True
            else:
                out[i] = bag[e.key]
        return out, (missing if missing.any() else None)

    if isinstance(e, TypedProp):
        return _typed_prop(e, sub)

    if isinstance(e, StartsWith):
        return _starts_with(e, sub)

    if isinstance(e, Calendar):
        return _calendar(e, sub)

    if isinstance(e, StrFn):
        return _strfn(e, sub)

    if isinstance(e, Arith):
        return _arith(e, sub)

    if isinstance(e, MathFn):
        return _math_fn(e, sub)

    if isinstance(e, Cmp):
        return _compare(e, sub)

    if isinstance(e, Not):
        values, nulls = _eval(e.arg, sub, None)
        return ~np.asarray(values, dtype=bool), nulls

    if isinstance(e, BoolOp):
        left, left_nulls = _eval(e.left, sub, None)
        right, right_nulls = _eval(e.right, sub, None)
        left_b = np.asarray(left, dtype=bool)
        right_b = np.asarray(right, dtype=bool)
        out = (left_b & right_b) if e.op == "and" else (left_b | right_b)
        return out, combine_nulls((left_nulls, right_nulls), n)

    if isinstance(e, IsNull):
        _values, nulls = _eval(e.arg, sub, None)
        return (np.zeros(n, dtype=bool) if nulls is None else nulls.copy()), None

    if isinstance(e, Coalesce):
        left, left_nulls = _eval(e.left, sub, None)
        right, right_nulls = _eval(e.right, sub, None)
        if left_nulls is None:
            return left, None
        out = np.where(left_nulls, right, left)
        remaining = None if right_nulls is None else (left_nulls & right_nulls)
        return out, combine_nulls((remaining,), n)

    if isinstance(e, If):
        return _if(e, sub)

    if isinstance(e, TupleExpr):
        parts = [_eval(item, sub, None) for item in e.items]
        out = np.empty(n, dtype=object)
        for i in range(n):
            out[i] = tuple(_scalar(values[i]) for values, _ in parts)
        return out, combine_nulls([nulls for _, nulls in parts], n)

    if isinstance(e, Cast):
        return _cast(e, sub)

    raise InternalError(f"no evaluator for expression node {type(e).__name__}")


_INT64_MIN, _INT64_MAX = -(2 ** 63), 2 ** 63 - 1


def _qualifies(value: Any, base: str) -> bool:
    """`TypedProp`'s JSON type classes (`gnomon/temporal/props.py`'s rule)."""
    if base in ("int", "ts"):
        return isinstance(value, int) and not isinstance(value, bool)
    if base == "float":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if base == "str":
        return isinstance(value, str)
    return isinstance(value, bool)        # base == "bool"


def _typed_prop(e: TypedProp, rel: Relation) -> Value:
    """`var.props[k] as τ`: the value where its JSON class is τ's, else null."""
    bags = rel.column(e.column)
    bag_nulls = rel.null_mask(e.column)
    base = e.to.base
    n = rel.n
    missing = np.zeros(n, dtype=bool)
    raw: list[Any] = [None] * n
    for i in range(n):
        bag = bags[i]
        if (bag_nulls is not None and bag_nulls[i]) or not isinstance(bag, dict) \
                or e.key not in bag or not _qualifies(bag[e.key], base):
            missing[i] = True
            continue
        raw[i] = bag[e.key]
    if base in ("int", "ts"):
        out = np.zeros(n, dtype=np.int64)
        for i in np.flatnonzero(~missing):
            v = raw[i]
            if not (_INT64_MIN <= v <= _INT64_MAX):
                raise InvalidArgError(
                    f"property {e.key!r} holds the integer {v}, outside int64; a typed "
                    f"read refuses rather than wrapping or rounding it", row=int(i))
            out[i] = v
    elif base == "float":
        out = np.zeros(n, dtype=np.float64)
        for i in np.flatnonzero(~missing):
            out[i] = float(raw[i])
    elif base == "bool":
        out = np.array([bool(v) if v is not None else False for v in raw], dtype=bool)
    else:
        out = np.array(raw, dtype=object)
    return out, (missing if missing.any() else None)


def _starts_with(e: StartsWith, rel: Relation) -> Value:
    values, value_nulls = _eval(e.arg, rel, None)
    prefixes, prefix_nulls = _eval(e.prefix, rel, None)
    nulls = combine_nulls((value_nulls, prefix_nulls), rel.n)
    out = np.zeros(rel.n, dtype=bool)
    for i in range(rel.n):
        if nulls is not None and nulls[i]:
            continue
        v, p = values[i], prefixes[i]
        if not isinstance(v, str) or not isinstance(p, str):
            raise InvalidArgError("starts_with over a non-string value", row=i)
        out[i] = v.startswith(p)
    return out, nulls


def _civil(us: int) -> tuple[int, int, int]:
    """(year, month, day) of a UTC instant in microseconds, proleptic
    Gregorian (the days-from-civil inverse, exact for any int64)."""
    days = us // 86_400_000_000
    z = days + 719468
    era = z // 146097
    doe = z - era * 146097
    yoe = (doe - doe // 1460 + doe // 36524 - doe // 146096) // 365
    y = yoe + era * 400
    doy = doe - (365 * yoe + yoe // 4 - yoe // 100)
    mp = (5 * doy + 2) // 153
    d = doy - (153 * mp + 2) // 5 + 1
    m = mp + 3 if mp < 10 else mp - 9
    return (y + 1 if m <= 2 else y), m, d


def _calendar(e: Calendar, rel: Relation) -> Value:
    values, nulls = _eval(e.arg, rel, None)
    other, other_nulls = (_eval(e.other, rel, None) if e.other is not None else (None, None))
    nulls = combine_nulls((nulls, other_nulls), rel.n)
    out = np.zeros(rel.n, dtype=np.int64)
    for i in range(rel.n):
        if nulls is not None and nulls[i]:
            continue
        y, m, d = _civil(int(values[i]))
        if e.fn == "year":
            out[i] = y
        elif e.fn == "month":
            out[i] = m
        elif e.fn == "day":
            out[i] = d
        elif e.fn == "month_day":
            out[i] = m * 100 + d
        else:
            y2, m2, _ = _civil(int(other[i]))
            out[i] = (y2 - y) * 12 + (m2 - m)
    return out, nulls


def _like(text: str, pattern: str) -> bool:
    """SQL LIKE: `%` any run, `_` one character, everything else literal."""
    import re

    rx = "".join(".*" if c == "%" else "." if c == "_" else re.escape(c) for c in pattern)
    return re.fullmatch(rx, text, flags=re.DOTALL) is not None


def _strfn(e: StrFn, rel: Relation) -> Value:
    parts = [_eval(a, rel, None) for a in e.args]
    nulls = combine_nulls([n for _, n in parts], rel.n)
    out: list[Any] = [None] * rel.n
    for i in range(rel.n):
        if nulls is not None and nulls[i]:
            out[i] = False if e.fn != "substring" else None
            continue
        vals = [_scalar(v[i]) for v, _ in parts]
        if not isinstance(vals[0], str) or (e.fn != "substring" and not isinstance(vals[1], str)):
            raise InvalidArgError(f"{e.fn} over a non-string value", row=i)
        if e.fn == "contains":
            out[i] = vals[1] in vals[0]
        elif e.fn == "like":
            out[i] = _like(vals[0], vals[1])
        else:
            start, length = int(vals[1]), int(vals[2])
            begin = start - 1
            end = begin + length
            begin = max(begin, 0)
            out[i] = vals[0][begin:max(end, begin)] if end > 0 else ""
    if e.fn == "substring":
        return np.array(out, dtype=object), nulls
    return np.array(out, dtype=bool), nulls


def _if(e: If, rel: Relation) -> Value:
    """Row-wise short-circuit.

    Each branch runs over **only** the rows that select it, so a guarded
    `if(b != 0, a / b, 0)` never divides by the zero its guard excluded. A row
    whose condition is null selects neither branch and is null in the result.
    """
    cond, cond_nulls = _eval(e.cond, rel, None)
    cond_b = np.asarray(cond, dtype=bool)
    if cond_nulls is not None:
        cond_b = cond_b & ~cond_nulls

    then_rows = np.flatnonzero(cond_b)
    else_rows = np.flatnonzero(~cond_b if cond_nulls is None
                               else (~cond_b & ~cond_nulls))
    then_values, then_nulls = _eval(e.then, rel, then_rows)
    else_values, else_nulls = _eval(e.otherwise, rel, else_rows)

    out = np.empty(rel.n, dtype=_merge_dtype(then_values, else_values))
    nulls = np.zeros(rel.n, dtype=bool)
    if then_rows.size:
        out[then_rows] = then_values
        if then_nulls is not None:
            nulls[then_rows] = then_nulls
    if else_rows.size:
        out[else_rows] = else_values
        if else_nulls is not None:
            nulls[else_rows] = else_nulls
    if cond_nulls is not None:
        nulls |= cond_nulls
    return out, (nulls if nulls.any() else None)


def _merge_dtype(a: np.ndarray, b: np.ndarray) -> Any:
    if a.dtype == b.dtype:
        return a.dtype
    if a.dtype.kind in "if" and b.dtype.kind in "if":
        return np.result_type(a.dtype, b.dtype)
    return object


def _arith(e: Arith, rel: Relation) -> Value:
    left, left_nulls = _eval(e.left, rel, None)
    right, right_nulls = _eval(e.right, rel, None)
    nulls = combine_nulls((left_nulls, right_nulls), rel.n)
    defined = np.ones(rel.n, dtype=bool) if nulls is None else ~nulls

    if e.op == "+":
        out = _numeric(left) + _numeric(right)
    elif e.op == "-":
        out = _numeric(left) - _numeric(right)
    elif e.op == "*":
        out = _numeric(left) * _numeric(right)
    elif e.op == "floor_div":
        _check_no_zero_divisor(right, defined, "floor_div")
        out = _floor_div(_numeric(left), _numeric(right), defined)
    elif e.op == "/":
        _check_no_zero_divisor(right, defined, "/")
        out = _quotient_array(_numeric(left), _numeric(right), defined)
    else:  # pragma: no cover - the AST validates the operator
        raise InternalError(f"unknown arithmetic operator {e.op!r}")
    _check_finite(out, defined, e.op)
    return out, nulls


def _check_no_zero_divisor(divisor: np.ndarray, defined: np.ndarray, op: str) -> None:
    """Division by zero is an **error**, never a null.

    A silent null would change which rows exist, which is the same reason
    `cast` on a non-numeric identity is an error rather than a null.
    """
    values = _numeric(divisor)
    zero = (values == 0) & defined
    if zero.any():
        raise InvalidArgError(
            f"division by zero in `{op}` at {int(zero.sum())} row(s) — the IR makes "
            f"this an error, never a null: a null would silently change which "
            f"rows the plan returns",
            rows=int(zero.sum()), first_row=int(np.flatnonzero(zero)[0]))


def _check_finite(out: np.ndarray, defined: np.ndarray, where: str) -> None:
    if out.dtype.kind != "f":
        return
    bad = ~np.isfinite(out) & defined
    if bad.any():
        raise InvalidArgError(
            f"non-finite result from `{where}` at {int(bad.sum())} row(s); "
            f"canonical output must be platform-stable",
            rows=int(bad.sum()), first_row=int(np.flatnonzero(bad)[0]))


def _floor_div(left: np.ndarray, right: np.ndarray, defined: np.ndarray) -> np.ndarray:
    out = np.zeros(len(left), dtype=np.result_type(left.dtype, right.dtype))
    if defined.any():
        out[defined] = left[defined] // right[defined]
    return out


def _quotient_array(num: np.ndarray, den: np.ndarray,
                    defined: np.ndarray) -> np.ndarray:
    """The blessed arithmetic rule, applied per row through `ops_compute`'s own
    `_quotient` — so the ninth decimal matches the operators, the oracle and the
    SQL twins exactly. A vectorized `num / den` would round differently on the
    exactly-representable cases the rule exists to protect.

    **The result column is always `float`**, which is what `Arith.tau` declares
    for `/` (the IR infers each binding's τ from its expression). The IR's rule also
    says a quotient is "returned as `int` when exact", and that half cannot
    survive into a *column*: a column carries one type, so a relation whose
    `/` output were int64 for one store and float64 for another would have a
    data-dependent schema. The rounding is preserved; only the per-value int
    narrowing is not, and it is a plan-layer typing decision rather than an
    arithmetic one.
    """
    from gnomon.temporal.ops_compute import _quotient

    out = np.zeros(len(num), dtype=np.float64)
    for i in np.flatnonzero(defined):
        out[i] = float(_quotient(_scalar(num[i]), _scalar(den[i]), "/"))
    return out


def _math_fn(e: MathFn, rel: Relation) -> Value:
    values, nulls = _eval(e.arg, rel, None)
    numeric = _numeric(values)
    defined = np.ones(rel.n, dtype=bool) if nulls is None else ~nulls
    if e.fn == "abs":
        out = np.abs(numeric)
    elif e.fn == "round":
        out = np.rint(numeric.astype(np.float64)).astype(np.int64)
    elif e.fn == "floor":
        out = np.floor(numeric.astype(np.float64)).astype(np.int64)
    elif e.fn == "ceil":
        out = np.ceil(numeric.astype(np.float64)).astype(np.int64)
    elif e.fn == "sqrt":
        negative = (numeric < 0) & defined
        if negative.any():
            raise InvalidArgError(
                f"sqrt of a negative value at {int(negative.sum())} row(s)",
                rows=int(negative.sum()))
        out = np.sqrt(numeric.astype(np.float64))
    else:  # pragma: no cover - the AST validates the function
        raise InternalError(f"unknown function {e.fn!r}")
    _check_finite(out, defined, e.fn)
    return out, nulls


def _compare(e: Cmp, rel: Relation) -> Value:
    left, left_nulls = _eval(e.left, rel, None)
    right, right_nulls = _eval(e.right, rel, None)
    nulls = combine_nulls((left_nulls, right_nulls), rel.n)
    left_c, right_c = _comparable(left), _comparable(right)
    try:
        out = _compare_arrays(e.op, left_c, right_c)
    except TypeError:
        # an ordering comparison over an object column whose null rows hold
        # `None` (the outer side of a left join): compare the defined rows
        # only; the null rows are null in the result whatever they hold
        if nulls is None:
            raise
        out = np.zeros(rel.n, dtype=bool)
        idx = np.flatnonzero(~nulls)
        if idx.size:
            out[idx] = _compare_arrays(e.op, np.asarray(left_c)[idx],
                                       np.asarray(right_c)[idx])
    return np.asarray(out, dtype=bool), nulls


def _compare_arrays(op: str, left_c: Any, right_c: Any) -> Any:
    if op == "=":
        return left_c == right_c
    if op == "!=":
        return left_c != right_c
    if op == "<":
        return left_c < right_c
    if op == "<=":
        return left_c <= right_c
    if op == ">":
        return left_c > right_c
    if op == ">=":
        return left_c >= right_c
    raise InternalError(f"unknown comparison {op!r}")  # pragma: no cover


def _cast(e: Cast, rel: Relation) -> Value:
    values, nulls = _eval(e.arg, rel, None)
    defined = np.ones(rel.n, dtype=bool) if nulls is None else ~nulls
    target = e.to.base
    if target == "str":
        out = np.array([None if not defined[i] else str(_scalar(values[i]))
                        for i in range(rel.n)], dtype=object)
        return out, nulls
    if target == "float":
        return _numeric(values).astype(np.float64), nulls
    # int / ts: an identity string casts **iff** it is a canonical decimal
    # integer, and is an E_ARG error otherwise. A silent null here would
    # change which rows exist, and LDBC's `toInteger(id)` tie-break sorts under
    # a Limit on two of the gate-tested rows.
    out = np.zeros(rel.n, dtype=np.int64)
    for i in range(rel.n):
        if not defined[i]:
            continue
        raw = _scalar(values[i])
        if isinstance(raw, (int, np.integer)) and not isinstance(raw, bool):
            out[i] = int(raw)
            continue
        if isinstance(raw, float):
            raise InvalidArgError(
                "cast(float, int) truncates; the IR has no implicit rounding — "
                "use floor/ceil/round explicitly", row=i)
        text = str(raw)
        if not _is_canonical_decimal(text):
            raise InvalidArgError(
                f"cast to {target} of {text!r}, which is not a canonical decimal "
                f"integer — the IR makes this an error rather than a null, because "
                f"a null would change which rows the plan returns",
                row=i, value=text)
        out[i] = int(text)
    return out, nulls


def _is_canonical_decimal(text: str) -> bool:
    body = text[1:] if text.startswith("-") else text
    if not body.isdigit():
        return False
    # no leading zeros, no "-0": one spelling per value, or the cast would not
    # be a function of the value
    return body == "0" and not text.startswith("-") or not body.startswith("0")


def _numeric(values: np.ndarray) -> np.ndarray:
    if values.dtype.kind in "iuf":
        return values
    if values.dtype == bool:
        return values.astype(np.int64)
    converted = np.array([0 if v is None else v for v in values], dtype=object)
    try:
        return converted.astype(np.float64) if any(
            isinstance(v, float) for v in converted) else converted.astype(np.int64)
    except (TypeError, ValueError):
        raise InvalidArgError(
            "arithmetic over a non-numeric column; the plan layer types "
            "expressions at construction, so this is a property value of the "
            "wrong JSON type") from None


def _comparable(values: np.ndarray) -> np.ndarray:
    """Comparison operands. Object arrays of `str` compare **by code point**,
    which is what `np.sort`/`<` on object arrays give and what `COLLATE "C"`
    means in the SQL twins."""
    if values.dtype.kind in "iufb":
        return values
    return values


def _scalar(value: Any) -> Any:
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return float(value)
    if isinstance(value, np.bool_):
        return bool(value)
    return value


__all__ = ["Value", "eval_expr", "eval_predicate"]
