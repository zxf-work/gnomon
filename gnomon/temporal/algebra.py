"""Operator registry, JSON Schemas, and the self-describing result envelope.

Design rules enforced here for every operator:
1. Typed      — args validate against the JSON Schema before execution.
2. Determin.  — same store state + same args => byte-identical canonical output.
3. Bounded    — every operator takes `limit` (default 100, max 10,000) and
                returns `truncated` + `cursor`.
4. Bi-temporal— every operator takes `as_of_tt` (default OPEN_END = current).
5. Self-desc. — every result carries {op, args_echo, dataset_extent,
                result_digest, truncated}.

The registry is the single source of truth for operator names, argument
schemas and descriptions.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Callable

import jsonschema

from gnomon.core.errors import InvalidArgError, SchemaError
from gnomon.core.model import OPEN_END, digest
from gnomon.storage.base import StorageAdapter

DEFAULT_LIMIT = 100
MAX_LIMIT = 10_000

# shared schema fragments
TIMESTAMP = {"type": "integer", "minimum": 0, "maximum": OPEN_END,
             "description": "int64 epoch microseconds, UTC"}
WINDOW = {
    "type": "object",
    "properties": {"t_a": TIMESTAMP, "t_b": TIMESTAMP},
    "required": ["t_a", "t_b"],
    "additionalProperties": False,
    "description": "half-open valid-time window [t_a, t_b)",
}
AS_OF_TT = {"type": "integer", "minimum": 0, "maximum": OPEN_END, "default": OPEN_END,
            "description": "belief state to evaluate under; OPEN_END = current beliefs"}
LIMIT = {"type": "integer", "minimum": 1, "maximum": MAX_LIMIT, "default": DEFAULT_LIMIT}
CURSOR = {"type": ["string", "null"], "default": None,
          "description": "opaque pagination cursor from a previous call"}
UID = {"type": "string", "minLength": 1}
UID_LIST = {"type": "array", "items": UID, "minItems": 1, "maxItems": 10_000}


PAGINATED_FIELDS = ("rows", "rows_total", "truncated", "cursor")

#: Envelope keys that are call metadata rather than the answer. Everything
#: that projects an *answer* out of an envelope strips exactly these (plus
#: `result_digest`); they live on the envelope, never inside a kernel's
#: payload, so the digest never covers them.
ENVELOPE_META_FIELDS = ("op", "args_echo", "dataset_extent")


@dataclass
class OperatorSpec:
    name: str
    fn: Callable[[StorageAdapter, dict[str, Any]], dict[str, Any]]
    args_schema: dict[str, Any]
    description: str
    cost_fn: Callable[[dict[str, Any], dict[str, Any]], dict[str, int]] | None = None
    validators: list[Callable[[dict[str, Any]], None]] = field(default_factory=list)
    # top-level payload fields the operator emits; callers can rely on these
    # names
    output_fields: tuple[str, ...] = PAGINATED_FIELDS
    #: Compiled validator, built on first use. `jsonschema.validate` is a
    #: convenience wrapper that re-checks the *schema* and constructs a fresh
    #: validator on every call, re-resolving `$ref`s through `urljoin` as it
    #: goes. On a point lookup returning two rows that cost more than reading
    #: the data did. Not part of the spec's identity, so it is excluded from
    #: comparison and repr.
    _validator: Any = field(default=None, compare=False, repr=False)
    #: Filled arguments of calls that validated, keyed by `args_key` of the
    #: caller's arguments. Handed out as fresh copies, never shared.
    _filled_memo: dict[Any, dict[str, Any]] = field(
        default_factory=dict, compare=False, repr=False)


REGISTRY: dict[str, OperatorSpec] = {}


def operator(name: str, args_schema: dict[str, Any], description: str,
             cost_fn: Callable | None = None,
             validators: list[Callable] | None = None,
             output_fields: tuple[str, ...] = PAGINATED_FIELDS) -> Callable:
    """Register an operator kernel. The kernel receives (adapter, args) with
    schema defaults already filled in, and returns the payload dict; the
    envelope is added by `call_operator`."""

    def deco(fn: Callable) -> Callable:
        # copy each fragment dict so stripping "_required" below never mutates
        # the caller's (possibly shared) fragment objects
        properties = {k: (dict(v) if isinstance(v, dict) else v)
                      for k, v in args_schema.items()}
        schema = {"type": "object", "properties": properties,
                  "additionalProperties": False,
                  "required": [k for k, v in args_schema.items()
                               if isinstance(v, dict) and v.get("_required")]}
        # strip the internal _required marker
        for v in schema["properties"].values():
            if isinstance(v, dict):
                v.pop("_required", None)
        REGISTRY[name] = OperatorSpec(name=name, fn=fn, args_schema=schema,
                                      description=description, cost_fn=cost_fn,
                                      validators=list(validators or []),
                                      output_fields=output_fields)
        return fn

    return deco


def required(fragment: dict[str, Any]) -> dict[str, Any]:
    return {**fragment, "_required": True}


def _fill_defaults(schema: dict[str, Any], args: dict[str, Any]) -> dict[str, Any]:
    out = dict(args)
    for key, sub in schema["properties"].items():
        if key not in out and isinstance(sub, dict) and "default" in sub:
            out[key] = sub["default"]
    return out


#: Bound on each memo below, in entries; a full memo is simply cleared.
MEMO_ENTRIES = 4096

_SCALARS = (str, int, bool, type(None))


def args_key(obj: Any) -> Any:
    """A hashable key that equals another's exactly when the two argument
    values are the same JSON value *of the same Python types*, dict keys in
    the same order — so `1`, `1.0` and `True`, or a list and a tuple, never
    share a key, because the JSON Schema check can treat them differently,
    and `args_echo` keeps the caller's key order. `None` (do not memoize)
    for anything outside plain JSON-shaped values."""
    t = type(obj)
    if t is dict:
        items = []
        for k, v in obj.items():
            if type(k) is not str:
                return None
            kv = args_key(v)
            if kv is None:
                return None
            items.append((k, kv))
        return (dict, tuple(items))
    if t is list:
        out = []
        for v in obj:
            kv = args_key(v)
            if kv is None:
                return None
            out.append(kv)
        return (list, tuple(out))
    if t is float:
        # repr, not the value: 0.0 == -0.0, but they are different JSON
        return None if obj != obj else (t, repr(obj))
    if t in _SCALARS:
        return (t, obj)
    return None


def json_copy(obj: Any) -> Any:
    """Deep copy of a JSON-shaped value (dicts, lists and tuples over
    immutable scalars): `copy.deepcopy` without its memo bookkeeping."""
    t = type(obj)
    if t is dict:
        return {k: json_copy(v) for k, v in obj.items()}
    if t is list:
        return [json_copy(v) for v in obj]
    if t is tuple:
        return tuple(json_copy(v) for v in obj)
    return obj


def validate_args(name: str, args: dict[str, Any]) -> dict[str, Any]:
    return _validate_args_keyed(name, args)[0]


def _validate_args_keyed(name: str, args: dict[str, Any]) -> tuple[dict[str, Any], Any]:
    """`validate_args`, plus the memo key of the caller's arguments.

    A call whose arguments already validated is answered from the spec's
    memo: validation and default filling are pure functions of the
    arguments and the spec, and the memo lives on the spec, so re-registering
    an operator starts a fresh one. Only successful validations are kept;
    every invalid call is checked, and fails, exactly as before. The caller
    gets a fresh deep copy, so mutating it cannot reach the memo.
    """
    spec = REGISTRY.get(name)
    if spec is None:
        raise InvalidArgError(f"unknown operator: {name}",
                              known=sorted(REGISTRY))
    key = args_key(args)
    if key is not None:
        hit = spec._filled_memo.get(key)
        if hit is not None:
            return json_copy(hit), key
    filled = _validate_args(spec, name, args)
    if key is not None:
        if len(spec._filled_memo) >= MEMO_ENTRIES:
            spec._filled_memo.clear()
        spec._filled_memo[key] = json_copy(filled)
    return filled, key


def _validate_args(spec: OperatorSpec, name: str, args: dict[str, Any]) -> dict[str, Any]:
    if spec._validator is None:
        cls = jsonschema.validators.validator_for(spec.args_schema)
        cls.check_schema(spec.args_schema)
        spec._validator = cls(spec.args_schema)
    # `best_match` rather than the first error, so the message a caller sees is
    # the one `jsonschema.validate` would have chosen
    error = jsonschema.exceptions.best_match(spec._validator.iter_errors(args))
    if error is not None:
        # The failing subschema rides in the payload: the message names what
        # broke, `expected` shows the shape that passes. Message-only payloads
        # leave a caller guessing at the same arg-shape mistake, so the
        # payload carries the contract, not just the violation. Trimmed to
        # the discriminating keys — a full subschema can be pages long.
        sub = error.schema if isinstance(error.schema, dict) else {}
        expected = {k: sub[k] for k in ("type", "required", "enum",
                                        "additionalProperties") if k in sub}
        if isinstance(sub.get("properties"), dict):
            expected["properties"] = {
                p: {k: v for k, v in (ps or {}).items()
                    if k in ("type", "enum", "default")}
                for p, ps in sub["properties"].items()}
        raise SchemaError(f"invalid args for {name}: {error.message}",
                          path=list(error.absolute_path),
                          expected=expected) from None
    filled = _fill_defaults(spec.args_schema, args)
    for v in spec.validators:
        v(filled)
    return filled


def check_window(args: dict[str, Any]) -> None:
    w = args.get("window")
    if w is not None and not (w["t_a"] < w["t_b"]):
        raise InvalidArgError(f"window requires t_a < t_b, got [{w['t_a']}, {w['t_b']})")


def call_operator(adapter: StorageAdapter, name: str, args: dict[str, Any],
                  skip_cost_check: bool = False,
                  cost_ceilings: dict[str, int] | None = None,
                  tt_source: Any = None) -> dict[str, Any]:
    """Validate, execute, and wrap an operator call in the self-describing
    envelope. Every caller (the CLI, the benchmark harness, the tests) goes
    through here.

    `tt_source` is accepted for interface compatibility and ignored.
    """
    filled = validate_args(name, args)
    spec = REGISTRY[name]
    stats = adapter.stats()
    if spec.cost_fn is not None and not skip_cost_check:
        from gnomon.temporal.guardrails import (  # local import: avoid cycle
            add_time_estimate, enforce_cost,
        )
        enforce_cost(name, add_time_estimate(name, spec.cost_fn(filled, stats)),
                     cost_ceilings)
    payload = spec.fn(adapter, filled)
    payload = _canonicalize_floats(payload)
    envelope = {
        "op": name,
        "args_echo": filled,
        # informational, reflects *current* beliefs — deliberately excluded
        # from result_digest so results pinned to a past as_of_tt stay
        # byte-identical under later writes (bi-temporal immutability)
        "dataset_extent": {"vt_min": stats.get("vt_min"), "vt_max": stats.get("vt_max")},
        "truncated": payload.get("truncated", False),
        **payload,
        "result_digest": digest(payload),
    }
    return envelope


def _canonicalize_floats(obj: Any) -> Any:
    """Round floats to 9 decimals so canonical output is platform-stable."""
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            raise InvalidArgError("non-finite float in operator output")
        return round(obj, 9)
    if isinstance(obj, dict):
        return {k: _canonicalize_floats(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_canonicalize_floats(v) for v in obj]
    return obj


def paginate(rows: list[Any], limit: int, cursor: str | None) -> dict[str, Any]:
    """Deterministic offset pagination over an already-ordered row list."""
    try:
        offset = int(cursor) if cursor else 0
    except ValueError:
        raise InvalidArgError(f"bad cursor: {cursor!r}") from None
    window = rows[offset:offset + limit]
    truncated = offset + len(window) < len(rows)
    return {
        "rows": window,
        "rows_total": len(rows),
        "truncated": truncated,
        "cursor": str(offset + len(window)) if truncated else None,
    }


def ensure_all_registered() -> None:
    """Import all operator modules so REGISTRY is fully populated."""
    from gnomon.temporal import (  # noqa: F401
        ops_aggregate,
        ops_compute,
        ops_motifs,
        ops_paths,
        ops_series,
        ops_snapshot,
        ops_versions,
    )
