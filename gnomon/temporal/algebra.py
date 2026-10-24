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
    #: The schema engine-level callers validate against, when it is wider
    #: than the base one (`args_schema`). A caller that asks for the base
    #: schema (`base_schema=True`) is validated against `args_schema` and
    #: nothing else, so an engine-only argument or aggregate is invisible to
    #: it and refused exactly as before it existed. `None`: one schema.
    #: Engine-only properties carry no `default`, so a call that does not
    #: use them fills, echoes and digests exactly as before.
    engine_schema: dict[str, Any] | None = None
    _engine_validator: Any = field(default=None, compare=False, repr=False)
    _engine_memo: dict[Any, dict[str, Any]] = field(
        default_factory=dict, compare=False, repr=False)


REGISTRY: dict[str, OperatorSpec] = {}

#: The base operator set: the fifteen operators a front end that lists the
#: registry lists and dispatches. `REGISTRY` may hold more (`match` is
#: registered for the engine and for direct `call_operator` callers only);
#: adding an operator to this set is a separate, deliberate change, because
#: it changes what every such front end exposes.
BASE_OPERATORS: frozenset[str] = frozenset({
    "aggregate_events", "burst_detection", "co_active", "compute",
    "count_temporal_motifs", "diff_snapshots", "entity_history",
    "find_temporal_motif_instances", "graph_metric_timeseries",
    "neighborhood_evolution", "resolve_entities", "snapshot_subgraph",
    "temporal_paths", "temporal_reachability", "version_history",
})


def unknown_operator(name: str) -> InvalidArgError:
    """The error for a name outside the base operator set: an unregistered
    name, and, for a base-schema caller, a registered operator outside
    `BASE_OPERATORS`, which is answered exactly as an unregistered one."""
    return InvalidArgError(f"unknown operator: {name}",
                           known=sorted(n for n in REGISTRY if n in BASE_OPERATORS))


def operator(name: str, args_schema: dict[str, Any], description: str,
             cost_fn: Callable | None = None,
             validators: list[Callable] | None = None,
             output_fields: tuple[str, ...] = PAGINATED_FIELDS,
             engine_schema: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
             ) -> Callable:
    """Register an operator kernel. The kernel receives (adapter, args) with
    schema defaults already filled in, and returns the payload dict; the
    envelope is added by `call_operator`.

    `engine_schema`, when given, receives a deep copy of the base
    schema built here and returns the engine-level one (see
    `OperatorSpec.engine_schema`)."""

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
        REGISTRY[name] = OperatorSpec(
            name=name, fn=fn, args_schema=schema,
            description=description, cost_fn=cost_fn,
            validators=list(validators or []),
            output_fields=output_fields,
            engine_schema=(None if engine_schema is None
                           else engine_schema(json_copy(schema))))
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


def validate_args(name: str, args: dict[str, Any],
                  base_schema: bool = False) -> dict[str, Any]:
    return _validate_args_keyed(name, args, base_schema)[0]


def _validate_args_keyed(name: str, args: dict[str, Any],
                         base_schema: bool = False) -> tuple[dict[str, Any], Any]:
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
        # The registry fills lazily (importing `gnomon` registers nothing), so
        # a direct `call_operator` / `validate_args` caller that never went
        # through a surface that fills it would otherwise be told a real
        # operator is unknown. Fill it once and look again; a name still
        # missing is unknown, with the error it always had.
        ensure_all_registered()
        spec = REGISTRY.get(name)
    if spec is None:
        raise unknown_operator(name)
    # a base-schema caller, or an operator with one schema, keeps the
    # memo it always had; the engine schema has its own
    engine = spec.engine_schema is not None and not base_schema
    memo = spec._engine_memo if engine else spec._filled_memo
    key = args_key(args)
    if key is not None:
        hit = memo.get(key)
        if hit is not None:
            return json_copy(hit), key
    filled = _validate_args(spec, name, args, engine)
    if key is not None:
        if len(memo) >= MEMO_ENTRIES:
            memo.clear()
        memo[key] = json_copy(filled)
    return filled, key


def _validate_args(spec: OperatorSpec, name: str, args: dict[str, Any],
                   engine: bool = False) -> dict[str, Any]:
    if engine:
        if spec._engine_validator is None:
            cls = jsonschema.validators.validator_for(spec.engine_schema)
            cls.check_schema(spec.engine_schema)
            spec._engine_validator = cls(spec.engine_schema)
        validator = spec._engine_validator
    else:
        if spec._validator is None:
            cls = jsonschema.validators.validator_for(spec.args_schema)
            cls.check_schema(spec.args_schema)
            spec._validator = cls(spec.args_schema)
        validator = spec._validator
    # `best_match` rather than the first error, so the message a caller sees is
    # the one `jsonschema.validate` would have chosen
    error = jsonschema.exceptions.best_match(validator.iter_errors(args))
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
    filled = _fill_defaults(spec.engine_schema if engine else spec.args_schema,
                            args)
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
                  tt_source: Any = None,
                  base_schema: bool = False) -> dict[str, Any]:
    """Validate, execute, and wrap an operator call in the self-describing
    envelope. Every caller (the CLI, the benchmark harness, the tests) goes
    through here. `base=True` validates against the base schema
    (`OperatorSpec.args_schema`) instead of the engine one.

    `tt_source` is accepted for interface compatibility and ignored.
    """
    filled = validate_args(name, args, base_schema)
    spec = REGISTRY[name]
    stats = adapter.stats()
    if spec.cost_fn is not None and not skip_cost_check:
        from gnomon.temporal.guardrails import (  # local import: avoid cycle
            ROUTES_KEY, add_time_estimate, cost_routes, enforce_cost,
        )
        # the kernels this handle runs, for the cost functions that price one
        priced = {**stats, ROUTES_KEY: cost_routes(adapter)}
        enforce_cost(name, add_time_estimate(name, spec.cost_fn(filled, priced)),
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


#: Scalars `_canonicalize_floats` returns as they are, by exact type: a list
#: or a dict holding only these is copied without a call per item (a
#: 327k-row `match` page was 3.7M calls, more than the engine's own time).
_PLAIN_SCALARS = frozenset({str, int, bool, type(None)})


def _canonicalize_floats(obj: Any) -> Any:
    """Round floats to 9 decimals so canonical output is platform-stable."""
    if type(obj) in _PLAIN_SCALARS:
        return obj
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            raise InvalidArgError("non-finite float in operator output")
        return round(obj, 9)
    if isinstance(obj, dict):
        plain = _PLAIN_SCALARS
        for v in obj.values():
            if type(v) not in plain:
                return {k: _canonicalize_floats(v) for k, v in obj.items()}
        return dict(obj)
    if isinstance(obj, list):
        plain = _PLAIN_SCALARS
        for v in obj:
            if type(v) not in plain:
                return [_canonicalize_floats(v) for v in obj]
        return list(obj)
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
    # last, so the fifteen keep the registration order they always had
    # (seeded shuffles over `REGISTRY` read that order); the engine-level
    # operators after them, in the order they were registered
    from gnomon.temporal import ops_match  # noqa: F401
    from gnomon.temporal import ops_belief  # noqa: F401
    from gnomon.temporal import ops_general_paths  # noqa: F401
    from gnomon.temporal import ops_state_series  # noqa: F401
