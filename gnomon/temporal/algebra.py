"""Operator registry, JSON Schemas, and the self-describing result envelope.

Design rules enforced here for every operator:
1. Typed      — args validate against the JSON Schema before execution.
2. Determin.  — same store state + same args => byte-identical canonical output.
3. Bounded    — every operator takes `limit` (default 100, max 10,000) and
                returns `truncated` + `cursor`.
4. Bi-temporal— every operator takes `as_of_tt` (default OPEN_END = current).
5. Self-desc. — every result carries {op, args_echo, dataset_extent,
                result_digest, truncated}.

The tool schemas are generated from this registry — single source of
truth.
"""

from __future__ import annotations

import hashlib
import math
import os
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

#: Envelope keys that are call metadata or provenance rather than the answer:
#: every consumer that projects an *answer* out of an envelope strips exactly
#: these (`Executor._resolve_answer`), and the human-owned comparators strip the
#: same set plus `result_digest` (`tests/conftest.py::ENVELOPE_META_KEYS`). The
#: digest-excluded because they live here, on the envelope, and never inside a
#: kernel's payload.
ENVELOPE_META_FIELDS = ("op", "args_echo", "dataset_extent",
                        "tt_q", "pinned", "clamped", "dependency", "plan_ir")

#: Whether the envelope publishes the rest of the `R` tuple — the `plan_ir`
#: sub-object holding `completeness`, `exactness`, `provenance`, `schema` and
#: `(T_v, T_b)`.
#:
#: **Off until `"plan_ir"` joins `tests/conftest.py::ENVELOPE_META_KEYS`.** The
#: oracle-family comparator is `{k: v for k, v in envelope.items() if k not in
#: ENVELOPE_META_KEYS} == oracle_payload` at thirteen sites, so *any* envelope
#: key that tuple does not name fails every one of them — and `tests/` is
#: `dependency`, `pinned`, `clamped`); `plan_ir` was the IR's fifth recommendation
#: and is not among them. The metadata is computed and carried regardless — the
#: executor records it per step — so flipping this constant to True alongside
#: that one-word `[tests]` change is the whole of the remaining work.
EMIT_PLAN_IR_META = True


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
    #: than the base-schema one (`args_schema`). base-schema surfaces --
    #: the tool schemas, the router, the MCP server, the web app, the plan
    #: verifier and the fault injector -- read `args_schema` and nothing
    #: else, so an engine-only argument or aggregate is invisible to them and
    #: refused by them exactly as before it existed. `None`: one schema.
    #: Engine-only properties carry no `default`, so a call that does not
    #: use them fills, echoes and digests exactly as before.
    engine_schema: dict[str, Any] | None = None
    _engine_validator: Any = field(default=None, compare=False, repr=False)
    _engine_memo: dict[Any, dict[str, Any]] = field(
        default_factory=dict, compare=False, repr=False)
    #: `_CallPlan`s of calls that completed, by `args_key` of the caller's
    #: arguments, one map per schema as for the
    #: memos above; re-registering an operator starts fresh ones.
    _plans: dict[Any, Any] = field(default_factory=dict, compare=False, repr=False)
    _engine_plans: dict[Any, Any] = field(
        default_factory=dict, compare=False, repr=False)


REGISTRY: dict[str, OperatorSpec] = {}

#: The operators an base-schema surface lists and dispatches: the tool
#: schemas, the in-process router, the MCP server, the web app, the match plan's
#: manual and the plan verifier. `REGISTRY` may hold more (`match` is
#: registered for the engine and for direct `call_operator` callers only);
#: this set, because it changes every base-schema prompt.
BASE_OPERATORS: frozenset[str] = frozenset({
    "aggregate_events", "burst_detection", "co_active", "compute",
    "count_temporal_motifs", "diff_snapshots", "entity_history",
    "find_temporal_motif_instances", "graph_metric_timeseries",
    "neighborhood_evolution", "resolve_entities", "snapshot_subgraph",
    "temporal_paths", "temporal_reachability", "version_history",
})


def unknown_operator(name: str) -> InvalidArgError:
    """The error for a name no base-schema surface knows: an unregistered
    name, and on those surfaces a registered operator outside
    `BASE_OPERATORS`, which they answer exactly as an unregistered one."""
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

    `engine_schema`, when given, receives a deep copy of the base-schema
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


_ENGINE_FNS: dict[str, Any] = {}


def _engine_fn(name: str) -> Any:
    """A module-level function of `gnomon._engine`, or `None` (no engine)."""
    if name not in _ENGINE_FNS:
        try:
            import gnomon._engine as _e
            _ENGINE_FNS[name] = getattr(_e, name, None)
        except ImportError:
            _ENGINE_FNS[name] = None
    return _ENGINE_FNS[name]


def json_copy(obj: Any) -> Any:
    """Deep copy of a JSON-shaped value (dicts, lists and tuples over
    immutable scalars): `copy.deepcopy` without its memo bookkeeping.

    The same copy in the engine (`_engine.json_copy`);
    `GNOMON_JSON_COPY_RUST=0` copies in Python."""
    fast = _engine_fn("json_copy")
    if fast is not None and not _env_is("GNOMON_JSON_COPY_RUST", "0"):
        return fast(obj)
    return _json_copy_py(obj)


def _json_copy_py(obj: Any) -> Any:
    t = type(obj)
    if t is dict:
        return {k: _json_copy_py(v) for k, v in obj.items()}
    if t is list:
        return [_json_copy_py(v) for v in obj]
    if t is tuple:
        return tuple(_json_copy_py(v) for v in obj)
    return obj


def _payload_digest(payload: Any) -> str:
    """    the engine (`_engine.canonical_json_bytes`, the bytes `canonical_json`
    encodes to) for exact JSON-shaped values; anything else, or an error,
    takes `digest`. `GNOMON_DIGEST_RUST=0` always takes `digest`."""
    fast = _engine_fn("canonical_json_bytes")
    if fast is not None and not _env_is("GNOMON_DIGEST_RUST", "0"):
        try:
            raw = fast(payload)
        except Exception:  # noqa: BLE001 - digest() gives the answer or the error
            raw = None
        if raw is not None:
            return hashlib.sha256(raw).hexdigest()
    return digest(payload)


def _canonical_payload(payload: Any) -> tuple[Any, str]:
    """`(_canonicalize_floats(payload), _payload_digest(<that>))`.

    `GNOMON_DIGEST_RUST`), one engine pass (`_engine.canonicalize_json`) builds
    the canonicalised payload and writes its canonical JSON; wherever it
    declines (a value either walk hands back to Python, a non-finite float)
    or fails, the two functions run as before. `GNOMON_CANON_ONEPASS=0` always
    runs the two."""
    fast = _engine_fn("canonicalize_json")
    if fast is not None and not _env_is("GNOMON_CANON_ONEPASS", "0") \
            and not _env_is("GNOMON_CANON_RUST", "0") and not _env_is("GNOMON_DIGEST_RUST", "0"):
        try:
            got = fast(payload, round)
        except Exception:  # noqa: BLE001 - the two functions answer or raise
            got = None
        if got is not None:
            return got[0], hashlib.sha256(got[1]).hexdigest()
    payload = _canonicalize_floats(payload)
    return payload, _payload_digest(payload)


def validate_args(name: str, args: dict[str, Any],
                  agent: bool = False) -> dict[str, Any]:
    return _validate_args_keyed(name, args, agent)[0]


def _validate_args_keyed(name: str, args: dict[str, Any],
                         agent: bool = False) -> tuple[dict[str, Any], Any]:
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
    # an base-schema caller, or an operator with one schema, keeps the
    # memo it always had; the engine schema has its own
    engine = spec.engine_schema is not None and not agent
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
                  agent: bool = False) -> dict[str, Any]:
    """Validate, execute, and wrap an operator call in the self-describing
    envelope. All base-schema surfaces (ToolRouter, MCP) go through here,
    and pass `agent=True`: their calls validate against the base-schema
    schema (`OperatorSpec.args_schema`), never the engine one.

    `tt_source` is the authoritative frontier for `tt_q` when the caller has a
    `Store` (which knows what its backend has *applied*); with none, the
    adapter's own applied frontier is used, which every backend maintains
    through the shared `apply_ops`.

    `_call_fast`, which keeps this function's steps, order and values and
    reuses what the arguments alone decide; `GNOMON_CALL_FAST=0` runs every
    call through the body below.
    """
    fast = _call_fast_on()
    if fast:
        envelope = _call_fast(adapter, name, args, skip_cost_check,
                              cost_ceilings, tt_source, agent)
        if envelope is not None:
            return envelope
    filled, args_memo_key = _validate_args_keyed(name, args, agent)
    spec = REGISTRY[name]
    stats = adapter.stats()
    if spec.cost_fn is not None and not skip_cost_check:
        from gnomon.temporal.guardrails import (  # local import: avoid cycle
            ROUTES_KEY, ZONES_KEY, add_time_estimate, cost_routes, cost_zones, enforce_cost,
        )
        # the kernels this handle runs, for the cost functions that price one;
        # and the zone-map summary of the current generation
        priced = {**stats, ROUTES_KEY: cost_routes(adapter), ZONES_KEY: cost_zones(adapter)}
        enforce_cost(name, add_time_estimate(name, spec.cost_fn(filled, priced)),
                     cost_ceilings)
    # Captured **before** the read is issued, which is how "round down, never
    # up" is satisfied by construction: anything
    # applied after this instant has tt > tt_q and will be checked, never
    # assumed away. Local import: `gnomon.plan` reads this module's registry from
    from gnomon.plan.evaluate import evaluate_leaf
    from gnomon.plan.leaf import build_leaf
    from gnomon.plan.rollout import plan_path_enabled
    from gnomon.plan.ttq import envelope_metadata
    freshness = envelope_metadata(adapter, name, filled, tt_source, args_memo_key)
    plan_ir_meta: dict[str, Any] = {}
    if plan_path_enabled():
        # the bound args, the output schema and the adapter-withholding
        # decision, and `evaluate_leaf` calls exactly this kernel with exactly
        # these arguments. `GNOMON_PLAN_PATH=off` takes the branch below instead;
        # both call the same kernel, so the payload and its digest are
        # identical either way.
        leaf = build_leaf(name, filled, spec.output_fields)
        evaluation = evaluate_leaf(leaf, adapter)
        payload = evaluation.payload
        if EMIT_PLAN_IR_META:
            plan_ir_meta = {"plan_ir": _plan_ir_meta(leaf, payload, args_memo_key,
                                            _generation(adapter))}
    else:
        payload = spec.fn(adapter, filled)
    payload, result_digest = _canonical_payload(payload)
    envelope = {
        "op": name,
        "args_echo": filled,
        # informational, reflects *current* beliefs — deliberately excluded
        # from result_digest so results pinned to a past as_of_tt stay
        # byte-identical under later writes (bi-temporal immutability)
        "dataset_extent": {"vt_min": stats.get("vt_min"), "vt_max": stats.get("vt_max")},
        "truncated": payload.get("truncated", False),
        **payload,
        # reason `result_digest` does — the envelope's own value wins — and
        # they are digest-excluded *structurally*: `digest()` covers `payload`,
        # which they are not part of. A metadata field returned by a kernel
        # instead would silently rewrite every digest in the tree
        #.
        **freshness,
        **plan_ir_meta,
        "result_digest": result_digest,
    }
    if fast and args_memo_key is not None and plan_ir_meta:
        _record_plan(spec, name, args, args_memo_key, agent)
    return envelope


# --------------------------------------------------------------------------- #
# --------------------------------------------------------------------------- #
#
# `call_operator` derives four things from the caller's arguments alone: the
# filled arguments (validation and defaults), the plan leaf, the freshness
# block for a given read basis, and the PLAN_IR block for a given generation and
# pagination bit. The first two were memoised piecewise
# but still copied, re-keyed and rebuilt per call. `_CallPlan` keeps them
# together, keyed once by `args_key`; everything that depends on the store --
# `stats()`, the admission estimate, the frontier, the log cursor, the store
# identity, the kernel's payload, the generation, canonicalisation and the
# digest -- is read or computed on every call, in the order the full body
# runs it, and the freshness and PLAN_IR blocks are looked up by exactly what
# was read. A plan is recorded only after the full body completed a call, so
# a first call (and every invalid one) takes the full body and fails there
# exactly as before.

#: Freshness / PLAN_IR blocks kept per plan; a full map is simply cleared.
PLAN_STATE_ENTRIES = 16


@dataclass(slots=True)
class _CallPlan:
    filled: dict[str, Any]          # the filled arguments; never handed out
    leaf: Any                       # the plan leaf built from `filled`
    keys: tuple[str, ...]           # `filled`'s keys in the leaf's order
    out: tuple[str, ...]            # the spec's `output_fields` it was built with
    pykey: Any                      # `args_key` of the caller's arguments
    fresh: dict[Any, dict[str, Any]] = field(default_factory=dict)
    plan_ir: dict[Any, dict[str, Any]] = field(default_factory=dict)
    admitted: set[Any] = field(default_factory=set)


_FAST_DEPS: list[Any] = []


def _fast_deps() -> Any:
    """The plan_ir names `_call_fast` uses (local import: the cycle)."""
    if not _FAST_DEPS:
        from gnomon.plan import ttq
        from gnomon.plan.depscope import FULL_SCAN_CHECKPOINTS, UNANCHORED, Checkpoint
        from gnomon.plan.evaluate import meta_json
        from gnomon.plan.guard import adapter_for
        from gnomon.plan.rollout import PLAN_PATH_ENV, plan_path_enabled
        _FAST_DEPS.append((ttq, Checkpoint, FULL_SCAN_CHECKPOINTS, UNANCHORED,
                           meta_json, adapter_for, PLAN_PATH_ENV, plan_path_enabled))
    return _FAST_DEPS[0]


_ENVIRON = os.environ
_ENV_DATA = getattr(_ENVIRON, "_data", None)
_ENV_KEYS: dict[str, Any] = {}


def _env_raw(name: str) -> Any:
    """`os.environ.get(name)` undecoded: the stored value, `None` when unset
    (read from the mapping `os.environ` itself reads)."""
    if _ENV_DATA is None or getattr(_ENVIRON, "_data", None) is not _ENV_DATA:
        return _ENVIRON.get(name)
    k = _ENV_KEYS.get(name)
    if k is None:
        k = _ENV_KEYS[name] = _ENVIRON.encodekey(name)
    return _ENV_DATA.get(k)


def _env_is(name: str, value: str) -> bool:
    """`os.environ.get(name) == value`."""
    raw = _env_raw(name)
    if raw is None:
        return False
    return (raw if type(raw) is str else _ENVIRON.decodevalue(raw)) == value


def _call_fast_on() -> bool:
    """`GNOMON_CALL_FAST` (default on), and the repeat path's preconditions:
    the envelope memo (`GNOMON_CALL_MEMO`) on, the PLAN_IR plan path on and its
    block emitted. Otherwise every call takes the full body."""
    if not EMIT_PLAN_IR_META or _env_is("GNOMON_CALL_FAST", "0") \
            or _env_is("GNOMON_CALL_MEMO", "0"):
        return False
    deps = _fast_deps()
    return _env_raw(deps[6]) is None or deps[7]()


def _record_plan(spec: OperatorSpec, name: str, args: dict[str, Any],
                 key: Any, agent: bool) -> None:
    """Keep the plan of a call the full body just completed (its arguments
    validated, its leaf built and its kernel answered)."""
    if REGISTRY.get(name) is not spec:
        return
    from gnomon.plan.leaf import build_leaf

    filled = _validate_args_keyed(name, args, agent)[0]
    leaf = build_leaf(name, filled, spec.output_fields)
    plans = spec._engine_plans if (spec.engine_schema is not None and not agent) \
        else spec._plans
    if len(plans) >= MEMO_ENTRIES:
        plans.clear()
    plans[_plan_key(args, key)] = _CallPlan(
        filled, leaf, tuple(k for k, _ in leaf.bound_args), spec.output_fields, key)


_NO_GETTER = object()


def _admit_key(name: str, cost_fn: Any, stats: dict[str, Any], routes: Any,
               ceilings: dict[str, int] | None, zones: Any = None) -> Any:
    """The key of an admission decision of a kept plan, or `None` (decide
    `_gnomon_pure_cost` -- a function of the filled arguments (fixed by the
    plan), the priced stats, `GNOMON_TIME_COEFF_SCALE` and the module constants
    it names -- the key is every input of the decision: the stats and routes
    as read for this call, the ceilings, the scale's raw value, those
    constants and the ones `add_time_estimate` and `enforce_cost` read (the
    default ceilings, the operator's coefficient). Only admitted calls are kept, so
    a refusal is always re-priced and raised as before. `zones` is the
    generation and digest of the zone-map summary the estimate read.
    `GNOMON_ADMIT_MEMO=0` decides every call."""
    constants = getattr(cost_fn, "_gnomon_pure_cost", None)
    if not callable(constants) or _env_is("GNOMON_ADMIT_MEMO", "0"):
        return None
    fast = _engine_fn("args_key_bytes")
    if fast is None or _env_is("GNOMON_ARGS_KEY_RUST", "0"):
        k = args_key([stats, routes, ceilings, zones])
    else:
        try:
            k = fast([stats, routes, ceilings, zones])
        except Exception:  # noqa: BLE001 - decide the call
            k = None
    if k is None:
        return None
    from gnomon.temporal.guardrails import admission_constants

    return (cost_fn, k, _env_raw("GNOMON_TIME_COEFF_SCALE"), constants(),
            admission_constants(name))


def _plan_key(args: dict[str, Any], pykey: Any = _NO_GETTER) -> Any:
    """The key of the caller's arguments in the plan maps: `args_key` as
    bytes from the engine (`_engine.args_key_bytes`, equal exactly when
    the caller has it). `GNOMON_ARGS_KEY_RUST=0` always keys in Python."""
    fast = _engine_fn("args_key_bytes")
    if fast is not None and not _env_is("GNOMON_ARGS_KEY_RUST", "0"):
        try:
            got = fast(args)
        except Exception:  # noqa: BLE001 - the Python key answers
            got = None
        if got is not None:
            return got
    return args_key(args) if pykey is _NO_GETTER else pykey


def _fresh_block(ttq: Any, name: str, plan: _CallPlan, key: Any, frontier: Any,
                 checkpoints: Any, ident: str) -> dict[str, Any]:
    """`envelope_metadata`'s memoised value for this basis: the entry of its
    own memo (`ttq._ENVELOPE_MEMO`, the same key), computed and kept there
    when absent, so a basis is computed once whichever path asks first."""
    mkey = (name, key, frontier, checkpoints, ident)
    hit = ttq._ENVELOPE_MEMO.get(mkey)
    if hit is None:
        hit = ttq._envelope_from(name, plan.filled, frontier, checkpoints, ident)
        if len(ttq._ENVELOPE_MEMO) >= ttq._ENVELOPE_MEMO_ENTRIES:
            ttq._ENVELOPE_MEMO.clear()
        ttq._ENVELOPE_MEMO[mkey] = hit
    return hit


def _call_fast(adapter: StorageAdapter, name: str, args: dict[str, Any],
               skip_cost_check: bool, cost_ceilings: dict[str, int] | None,
               tt_source: Any, agent: bool) -> dict[str, Any] | None:
    """`call_operator`'s envelope for a call whose plan is kept, or `None`
    (no plan: the full body runs). Same steps, order and values as the full
    body; see the block comment above."""
    spec = REGISTRY.get(name)
    if spec is None:
        return None
    key = _plan_key(args)
    if key is None:
        return None
    plan = (spec._engine_plans if (spec.engine_schema is not None and not agent)
            else spec._plans).get(key)
    if plan is None or plan.out is not spec.output_fields:
        return None
    (ttq, Checkpoint, full_scan, unanchored, meta_json, adapter_for,
     _env, _pp) = _fast_deps()
    filled = json_copy(plan.filled)
    stats = adapter.stats()
    if spec.cost_fn is not None and not skip_cost_check:
        from gnomon.temporal.guardrails import (
            ROUTES_KEY, ZONES_KEY, add_time_estimate, cost_routes, cost_zones, enforce_cost,
            zones_key,
        )
        routes = cost_routes(adapter)
        zones = cost_zones(adapter)
        akey = _admit_key(name, spec.cost_fn, stats, routes, cost_ceilings, zones_key(zones))
        if akey is None or akey not in plan.admitted:
            priced = {**stats, ROUTES_KEY: routes, ZONES_KEY: zones}
            enforce_cost(name, add_time_estimate(name, spec.cost_fn(filled, priced)),
                         cost_ceilings)
            if akey is not None:
                if len(plan.admitted) >= PLAN_STATE_ENTRIES:
                    plan.admitted.clear()
                plan.admitted.add(akey)
    # the read basis, read as `envelope_metadata` reads it, before the kernel
    if tt_source is None:
        # `frontier_of`, `checkpoints_of` and `store_identity_of` for an
        # adapter-only read: the same getters, once each, in that order;
        # the block is keyed by what they returned
        getter = getattr(adapter, "frontier_tt", None)
        tt = getter() if getter is not None else _NO_GETTER
        cursor = getattr(adapter, "event_cursor", None)
        offset = chain = None
        if cursor is not None:
            try:
                offset, chain = cursor()
            except Exception:  # noqa: BLE001 - checkpoints_of's fallback
                offset = chain = None
        ident = getattr(adapter, "store_identity", None)
        if not (isinstance(ident, str) and ident):
            ident = unanchored
        bkey = (tt, offset, chain, ident)
        fm = plan.fresh.get(bkey)
        if fm is None:
            frontier = ttq.Frontier(None if tt is None or tt is _NO_GETTER
                                    else int(tt), True)
            checkpoints = ((Checkpoint(int(offset), chain),) if chain
                           else full_scan)
            fm = _fresh_block(ttq, name, plan, plan.pykey, frontier, checkpoints, ident)
            if len(plan.fresh) >= PLAN_STATE_ENTRIES:
                plan.fresh.clear()
            plan.fresh[bkey] = fm
    else:
        frontier = ttq.frontier_of(adapter, tt_source)
        checkpoints = ttq.checkpoints_of(adapter, tt_source)
        ident = ttq.store_identity_of(adapter, tt_source)
        bkey = (frontier, checkpoints, ident, 1)
        fm = plan.fresh.get(bkey)
        if fm is None:
            fm = _fresh_block(ttq, name, plan, plan.pykey, frontier, checkpoints, ident)
            if len(plan.fresh) >= PLAN_STATE_ENTRIES:
                plan.fresh.clear()
            plan.fresh[bkey] = fm
    # the kernel, with the arguments `evaluate_leaf` hands it: the leaf's
    # key order over this call's filled values
    payload = spec.fn(adapter_for(plan.leaf, adapter),
                      {k: filled[k] for k in plan.keys})
    tkey = (bool(payload.get("truncated") and payload.get("cursor") is not None),
            _generation(adapter))
    tm = plan.plan_ir.get(tkey)
    if tm is None:
        tm = meta_json(plan.leaf, payload)
        if len(plan.plan_ir) >= PLAN_STATE_ENTRIES:
            plan.plan_ir.clear()
        plan.plan_ir[tkey] = tm
    freshness, plan_ir = json_copy((fm, tm))
    payload, result_digest = _canonical_payload(payload)
    return {
        "op": name,
        "args_echo": filled,
        "dataset_extent": {"vt_min": stats.get("vt_min"), "vt_max": stats.get("vt_max")},
        "truncated": payload.get("truncated", False),
        **payload,
        **freshness,
        "plan_ir": plan_ir,
        "result_digest": result_digest,
    }


#: `meta_json` results by (op, argument key, output fields, completeness
_PLAN_IR_META_MEMO: dict[Any, dict[str, Any]] = {}


def _generation(adapter: Any) -> Any:
    """The store generation where the backend has one (native), else `None`.
    Only ever a memo-key component."""
    try:
        g = getattr(adapter, "generation", None)
        return g() if callable(g) else g
    except Exception:  # noqa: BLE001 - a key component, never an error source
        return None


def _plan_ir_meta(leaf: Any, payload: dict[str, Any], key: Any,
               generation: Any) -> dict[str, Any]:
    """`meta_json(leaf, payload)`, memoized.

    The block is a function of the leaf, which `build_leaf` derives from the
    operator and its filled arguments alone (and those from the caller's
    arguments alone), and of the payload only through `leaf_completeness`:
    whether the payload is truncated with a cursor. So that bit, the
    operator, the caller's argument key and the output fields key it; the
    store generation joins the key too, so nothing is reused across a write.
    The caller gets a fresh deep copy: the bytes are those `meta_json`
    emits, and no caller can mutate the memo through them.
    """
    from gnomon.plan.evaluate import meta_json

    if key is None:
        return meta_json(leaf, payload)
    paginated = bool(payload.get("truncated") and payload.get("cursor") is not None)
    mkey = (leaf.op, key, leaf.out_fields, paginated, generation)
    hit = _PLAN_IR_META_MEMO.get(mkey)
    if hit is None:
        hit = meta_json(leaf, payload)
        if len(_PLAN_IR_META_MEMO) >= MEMO_ENTRIES:
            _PLAN_IR_META_MEMO.clear()
        _PLAN_IR_META_MEMO[mkey] = json_copy(hit)
        return hit
    return json_copy(hit)


#: Scalars `_canonicalize_floats` returns as they are, by exact type: a list
#: or a dict holding only these is copied without a call per item (a
#: 327k-row `match` page was 3.7M calls, more than the engine's own time).
_PLAIN_SCALARS = frozenset({str, int, bool, type(None)})


_CANON_FAST: list[Any] = []


def _canonicalize_floats(obj: Any) -> Any:
    """Round floats to 9 decimals so canonical output is platform-stable.

    The walk runs in the engine (`_engine.canonicalize_floats`,
    the same rules over exact dicts, lists and scalars, `round` called
    through Python); any error there re-runs the Python walk, which raises
    its own. `GNOMON_CANON_RUST=0` keeps the Python walk."""
    if not _CANON_FAST:
        try:
            from gnomon._engine import canonicalize_floats as fast
        except ImportError:
            fast = None
        _CANON_FAST.append(fast)
    fast = _CANON_FAST[0]
    if fast is not None and not _env_is("GNOMON_CANON_RUST", "0"):
        try:
            return fast(obj, round, _canonicalize_floats_py)
        except Exception:  # noqa: BLE001 - the Python walk gives the answer or the error
            pass
    return _canonicalize_floats_py(obj)


def _canonicalize_floats_py(obj: Any) -> Any:
    """`_canonicalize_floats`, in Python."""
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
                return {k: _canonicalize_floats_py(v) for k, v in obj.items()}
        return dict(obj)
    if isinstance(obj, list):
        plain = _PLAIN_SCALARS
        for v in obj:
            if type(v) not in plain:
                return [_canonicalize_floats_py(v) for v in obj]
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
