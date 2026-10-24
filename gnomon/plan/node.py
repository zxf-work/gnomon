"""Plan nodes: the twelve compositional core operators and the `OpaqueLeaf`
that carries the fifteen base operators.

Every node is a **frozen dataclass**, and every node has the IR's common
shape: `node ::= Op(inputs…, args…) @ Σ`. Of the six obligations a node
carries, this module discharges the ones that are data:

1. *signature* — `out_schema`, propagated by the schema rules, validated at
   construction;
2. *bi-temporal evaluation rule under Σ* — `sigma`, with the IR's rule that an
   input subtree may narrow `T_v` and may never widen it, checked here;
3. *canonical output order* — recorded per node in `canonical_order`, as
   documentation of the contract rather than as an implementation;
4. *cost-guard hook* — not here: admission lives in `gnomon.plan.admission`;
5. *metadata propagation* — `gnomon.plan.metadata`;
6. *dependency-scope rule or an explicit `∅`* — `reads_store` here, the
   adapter-withholding guard in `gnomon.plan.guard`.

**No evaluators.** Nothing in this module reads a store; the reference
evaluator is `gnomon.plan.eval`.
"""

from __future__ import annotations

import abc
import contextlib
from collections.abc import Iterator
from dataclasses import dataclass, fields
from typing import Any, Literal

from gnomon.core.errors import InvalidArgError
from gnomon.core.model import digest
from gnomon.plan.expr import CMP_OPS, Expr
from gnomon.plan.types import (
    DEFAULT_SIGMA, EDGE_COLUMNS, NODE_COLUMNS, PATTERN_NODE_COLUMNS, Column,
    Schema, Sigma, T_FLOAT, T_INT, T_JSON, Tau, check_belief, check_vt_mode,
    edge_schema, node_schema,
)

JoinType = Literal["inner", "left_outer", "anti"]
JOIN_TYPES: frozenset[str] = frozenset({"inner", "left_outer", "anti"})

Direction = Literal["out", "in", "both"]
DIRECTIONS: frozenset[str] = frozenset({"out", "in", "both"})

AggFn = Literal["count", "count_distinct", "sum", "sum_exact", "min", "max", "mean",
                "collect"]
#: `sum_exact` is `sum` under the IR's blessed rule (exact over integers,
#: `math.fsum` as soon as one value is a float). It is a separate name so that
#: `sum`, which existing plans use, keeps its current fold byte for byte.
AGG_FNS: frozenset[str] = frozenset({"count", "count_distinct", "sum", "sum_exact",
                                     "min", "max", "mean", "collect"})

SortDir = Literal["asc", "desc"]
NullsOrder = Literal["nulls_first", "nulls_last"]

#: `compute` is the only one of the fifteen whose kernel never touches the adapter, so it is the only `∅`
#: leaf. See `gnomon.plan.guard` for the check that makes the classification
#: falsifiable rather than decorative.
EMPTY_SCOPE_OPS: frozenset[str] = frozenset({"compute"})


class Node(abc.ABC):
    """A node in a plan DAG. Subclasses are frozen dataclasses."""

    __slots__ = ()

    #: Does this node read store state? `False` is the `∅`
    #: classification and is a *checkable* property (`gnomon.plan.guard`).
    reads_store: bool = False

    #: The total order the node's output is defined to
    #: have, so plans are deterministic without an explicit `Order`.
    canonical_order: str = ""

    @property
    @abc.abstractmethod
    def sigma(self) -> Sigma: ...

    @property
    def inputs(self) -> tuple["Node", ...]:
        return ()

    @property
    @abc.abstractmethod
    def out_schema(self) -> Schema: ...

    @property
    def op(self) -> str:
        return type(self).__name__

    # -- validation ------------------------------------------------------
    def _check_inputs(self) -> None:
        """A subtree may narrow `T_v`, never widen it, and `T_b` is
        plan-global in v1."""
        for i in self.inputs:
            if not self.sigma.covers(i.sigma):
                raise InvalidArgError(
                    "an input's Σ widens its consumer's — no node may widen T_v, "
                    "and T_b is plan-global in v1",
                    node=self.op, outer=self.sigma.to_json(), inner=i.sigma.to_json(),
                )

    # -- provenance ------------------------------------------------------
    def canonical_args(self) -> dict[str, Any]:
        """The node's bound arguments, canonically. Inputs and Σ are excluded —
        `node_digest` carries them separately."""
        out: dict[str, Any] = {}
        for f in fields(self):  # type: ignore[arg-type]
            if f.name in ("sigma",):
                continue
            value = getattr(self, f.name)
            if isinstance(value, Node) or _is_node_seq(value):
                continue
            out[f.name] = _jsonify(value)
        return out

    @property
    def node_digest(self) -> str:
        """A content digest over `(op, canonical args with parameters
        bound, Σ, the input nodes' digests)` — a Merkle digest of the plan
        subtree. Every component is *data*, which is why the opaque leaf holds
        bound args rather than a kernel callable."""
        return digest({
            "op": self.op,
            "args": self.canonical_args(),
            "sigma": self.sigma.to_json(),
            "inputs": [i.node_digest for i in self.inputs],
        })


def _is_node_seq(value: Any) -> bool:
    return isinstance(value, tuple) and bool(value) and all(isinstance(v, Node) for v in value)


def _jsonify(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if hasattr(value, "to_json"):
        return value.to_json()
    if isinstance(value, (tuple, list)):
        return [_jsonify(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _jsonify(v) for k, v in value.items()}
    raise InvalidArgError(f"non-canonicalizable argument: {type(value).__name__}")


# ---------------------------------------------------------------------------
# hop forms
# ---------------------------------------------------------------------------

class HopSpec(abc.ABC):
    """`exact(k) | bounded(a, b) | unbounded(a)`.

    The two families are **different relations** and the spec keeps them apart:
    `exact(k)` is a *walk* relation —
    multiplicity-preserving and edge-bindable — while `bounded`/`unbounded` are
    *node* relations, deduplicated at minimum depth. `bounded(k, k)` is
    therefore **not** silently normalized to `exact(k)`; an implementation must
    not rewrite one into the other.
    """

    __slots__ = ()

    #: Variable-length forms add `<into>.depth` and bind no edge variable.
    variable_length: bool = True

    @abc.abstractmethod
    def to_json(self) -> Any: ...


@dataclass(frozen=True, slots=True)
class Exact(HopSpec):
    k: int
    variable_length = False

    def __post_init__(self) -> None:
        if self.k < 0:
            raise InvalidArgError(f"exact(k) needs k >= 0, got {self.k}")

    def to_json(self) -> Any:
        return {"exact": self.k}


@dataclass(frozen=True, slots=True)
class Bounded(HopSpec):
    a: int
    b: int

    def __post_init__(self) -> None:
        if not (0 <= self.a <= self.b):
            raise InvalidArgError(f"bounded(a, b) needs 0 <= a <= b, got ({self.a}, {self.b})")

    def to_json(self) -> Any:
        return {"bounded": [self.a, self.b]}


@dataclass(frozen=True, slots=True)
class Unbounded(HopSpec):
    a: int

    def __post_init__(self) -> None:
        if self.a not in (0, 1):
            raise InvalidArgError(f"unbounded(a) needs a in {{0, 1}}, got {self.a}")

    def to_json(self) -> Any:
        return {"unbounded": self.a}


# ---------------------------------------------------------------------------
# the two scans
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class NodeScan(Node):
    """`NodeScan(as, labels?, uids?, belief) → as.(uid, vid, label, vt_s, vt_e,
    tt_s, tt_e, props)`.

    `labels` is a **plain union list, not a hierarchy**: a version matches when its label is any member. It is sugar for a
    union rather than subtyping — the IR has no subtype relation.
    """

    as_: str
    labels: tuple[str, ...] | None = None
    uids: tuple[str, ...] | None = None
    belief: str = "current"
    vt_mode: str = "overlap"
    sigma_: Sigma = DEFAULT_SIGMA

    reads_store = True

    def __post_init__(self) -> None:
        if not self.as_:
            raise InvalidArgError("NodeScan.as is required — it binds the scan variable")
        check_belief(self.belief)
        check_vt_mode(self.vt_mode)
        if self.labels is not None and not self.labels:
            raise InvalidArgError("labels = [] matches nothing; omit it for an unrestricted scan")
        if self.uids is not None and not self.uids:
            raise InvalidArgError("uids = [] matches nothing; omit it for an unrestricted scan")

    @property
    def sigma(self) -> Sigma:
        return self.sigma_

    @property
    def out_schema(self) -> Schema:
        return node_schema(self.as_)

    @property
    def canonical_order(self) -> str:
        return "(tt_s, vid)" if self.belief != "current" else "(vt_s, vid)"


@dataclass(frozen=True, slots=True)
class EdgeScan(Node):
    """`EdgeScan(as, rel_types?, endpoints?, belief) → as.(eid, vid, src, dst,
    rel_type, disc, vt_s, vt_e, tt_s, tt_e, props)`.

    `endpoints` is an incidence pushdown and **the only cohort restriction in
    the algebra that is not a `Join`**: a `Join` against a uid list would
    multiply rows where an edge has both endpoints in the cohort, while
    `endpoints` selects each matching edge version exactly once.
    """

    as_: str
    rel_types: tuple[str, ...] | None = None
    endpoints: "Endpoints | None" = None
    belief: str = "current"
    vt_mode: str = "overlap"
    sigma_: Sigma = DEFAULT_SIGMA

    reads_store = True

    def __post_init__(self) -> None:
        if not self.as_:
            raise InvalidArgError("EdgeScan.as is required — it binds the scan variable")
        check_belief(self.belief)
        check_vt_mode(self.vt_mode)
        if self.rel_types is not None and not self.rel_types:
            raise InvalidArgError("rel_types = [] matches nothing; omit it instead")

    @property
    def sigma(self) -> Sigma:
        return self.sigma_

    @property
    def out_schema(self) -> Schema:
        return edge_schema(self.as_)

    @property
    def canonical_order(self) -> str:
        return "(tt_s, vid)" if self.belief != "current" else "(vt_s, vid)"


@dataclass(frozen=True, slots=True)
class Endpoints:
    """`{role, uids}` — the storage layer's own scan signature. All four
    roles are real; `both` is the one genuine narrowing the enum offers, and
    omitting it was a gate finding."""

    role: str
    uids: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.role not in INCIDENT_ROLES:
            raise InvalidArgError(f"unknown endpoint role: {self.role!r}",
                                  allowed=sorted(INCIDENT_ROLES))
        if not self.uids:
            raise InvalidArgError("endpoints.uids = [] matches nothing")

    def to_json(self) -> dict[str, Any]:
        return {"role": self.role, "uids": list(self.uids)}


#: One enum, four values, shared with the dependency wire format's `incident`
#: case.
INCIDENT_ROLES: frozenset[str] = frozenset({"src", "dst", "either", "both"})


# ---------------------------------------------------------------------------
# Expand
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class Expand(Node):
    """`Expand(input, from, rel_type?, dir, hops, into, edge_var?)`.

    Two restrictions are enforced here because they are structural:

    - **No edge bindings for variable-length forms**: a variable number
      of edges cannot bind into a fixed row schema without list values, and v1
      has no list type. This is precisely why the path family stays outside the
      core.
    - **Structural closure only** — every hop is evaluated under one Σ, with no
      constraint that hop *i+1*'s `vt_s` exceed hop *i*'s. R6's unbounded
      `Expand` is therefore **not** the reachability operator;
      `temporal_reachability` imposes the ordering constraint and an
      earliest-arrival semiring and stays an opaque leaf.
    """

    input: Node
    from_: str
    into: str
    hops: HopSpec
    rel_type: str | None = None
    dir: str = "out"
    edge_var: str | None = None
    sigma_: Sigma = DEFAULT_SIGMA

    reads_store = True

    def __post_init__(self) -> None:
        if self.dir not in DIRECTIONS:
            raise InvalidArgError(f"unknown direction: {self.dir!r}", allowed=sorted(DIRECTIONS))
        self.from_column  # noqa: B018 - resolves `from`, or fails at construction
        if self.edge_var is not None:
            if self.hops.variable_length:
                raise InvalidArgError(
                    "bounded/unbounded Expand binds `into` and `<into>.depth` only — "
                    "a variable number of edges cannot bind into a fixed row schema, "
                    "and v1 has no list type")
            if isinstance(self.hops, Exact) and self.hops.k == 0:
                raise InvalidArgError("exact(0) traverses no edge, so it binds no edge_var")
        self._check_inputs()
        self.out_schema  # noqa: B018 - fail at construction, not at first read

    @property
    def sigma(self) -> Sigma:
        return self.sigma_

    @property
    def inputs(self) -> tuple[Node, ...]:
        return (self.input,)

    @property
    def from_column(self) -> str:
        """The node-identity column `from` names. The IR writes `from: var`;
        the anchor rule is per *column*, and the case that matters expands
        from `EdgeScan`'s `src`, so both spellings resolve here."""
        from gnomon.plan.anchor import uid_column  # local import: anchor reads nodes
        return uid_column(self.input, self.from_)

    @property
    def out_schema(self) -> Schema:
        out = self.input.out_schema.concat(Schema(NODE_COLUMNS).prefixed(self.into))
        if self.edge_var is not None:
            out = out.concat(Schema(EDGE_COLUMNS).prefixed(self.edge_var))
        if self.hops.variable_length:
            # `<into>.depth` is prefixed like every other column the operator
            # adds, so two variable-length expansions in one plan do not
            # collide.
            out = out.concat(Schema.of(Column(f"{self.into}.depth", T_INT)))
        return out

    @property
    def canonical_order(self) -> str:
        if self.hops.variable_length:
            return "(input row position, into.depth, into)"
        return "(input row position, (vt_s, vid))"


# ---------------------------------------------------------------------------
# the selections
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class Filter(Node):
    """`Filter(input, pred) → input.schema`.

    `Filter` narrows the result's declared **domain**; it does not narrow
    Σ, and therefore never narrows a dependency scope. A later correction can make a row that was
    filtered *out* pass the predicate, and that row was inside the scan's region
    all along.
    """

    input: Node
    pred: Expr
    sigma_: Sigma = DEFAULT_SIGMA

    def __post_init__(self) -> None:
        t = self.pred.tau(self.input.out_schema)
        if t.base not in ("bool", "json"):
            raise InvalidArgError(f"Filter.pred must be boolean, got {t.to_json()}")
        self._check_inputs()

    @property
    def sigma(self) -> Sigma:
        return self.sigma_

    @property
    def inputs(self) -> tuple[Node, ...]:
        return (self.input,)

    @property
    def out_schema(self) -> Schema:
        return self.input.out_schema

    @property
    def canonical_order(self) -> str:
        return "the input's, restricted"


@dataclass(frozen=True, slots=True)
class PropertyPredicate(Node):
    """`PropertyPredicate(input, var, prop, cmp, value) → input.schema`.

    Named rather than sugar for `Filter` because of the **type-fit rule**:
    a value participates only if its JSON type fits the comparison, and rows
    excluded *by type mismatch* are counted and reported as `prop_coercion`
    metadata. An answer must not rest on a shrunken denominator without saying
    so.
    """

    input: Node
    var: str
    prop: str
    cmp: str
    value: Any
    sigma_: Sigma = DEFAULT_SIGMA

    def __post_init__(self) -> None:
        if self.cmp not in CMP_OPS:
            raise InvalidArgError(f"unknown comparison: {self.cmp!r}", allowed=sorted(CMP_OPS))
        if self.var not in self.input.out_schema.vars():
            raise InvalidArgError(f"PropertyPredicate names an unbound variable: {self.var!r}",
                                  bound=list(self.input.out_schema.vars()))
        if not self.prop:
            raise InvalidArgError("PropertyPredicate needs a property key")
        self._check_inputs()

    @property
    def sigma(self) -> Sigma:
        return self.sigma_

    @property
    def inputs(self) -> tuple[Node, ...]:
        return (self.input,)

    @property
    def out_schema(self) -> Schema:
        return self.input.out_schema

    @property
    def canonical_order(self) -> str:
        return "the input's, restricted"


@dataclass(frozen=True, slots=True)
class TypeConstraint(Node):
    """`TypeConstraint(input, var, labels)` for a node variable, or
    `TypeConstraint(input, var, rel_type)` for an edge variable.

    A node's label is a property of the *version* valid in Σ, not of the
    identity. `labels` is a union list and **no label hierarchy exists**:
    LDBC's `Message` is compiled at bind time as
    `labels: ["Post", "Comment"]`, so the pushdown survives without the IR
    carrying subtyping.
    """

    input: Node
    var: str
    labels: tuple[str, ...] | None = None
    rel_type: str | None = None
    sigma_: Sigma = DEFAULT_SIGMA

    def __post_init__(self) -> None:
        if (self.labels is None) == (self.rel_type is None):
            raise InvalidArgError(
                "TypeConstraint takes labels (node variable) or rel_type (edge variable), "
                "exactly one")
        if self.labels is not None and not self.labels:
            raise InvalidArgError("labels = [] matches nothing")
        if self.var not in self.input.out_schema.vars():
            raise InvalidArgError(f"TypeConstraint names an unbound variable: {self.var!r}",
                                  bound=list(self.input.out_schema.vars()))
        self._check_inputs()

    @property
    def sigma(self) -> Sigma:
        return self.sigma_

    @property
    def inputs(self) -> tuple[Node, ...]:
        return (self.input,)

    @property
    def out_schema(self) -> Schema:
        return self.input.out_schema

    @property
    def canonical_order(self) -> str:
        return "the input's, restricted"


# ---------------------------------------------------------------------------
# Project
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class Project(Node):
    """`Project(input, bindings, keep) → (listed names) [⧺ input.schema if
    keep = all]`.

    One restriction is statically checked here, and it is exactly the
    expression language's boundary: a `Project` directly above an `Aggregate`
    may reference **at most one** aggregate output column per expression, plus
    constants. Combining two or more aggregate output columns
    (`arithmetic-over-aggregates`) is beyond v1.
    """

    input: Node
    bindings: tuple[tuple[str, Expr], ...]
    keep: str = "listed"
    sigma_: Sigma = DEFAULT_SIGMA

    def __post_init__(self) -> None:
        if self.keep not in ("all", "listed"):
            raise InvalidArgError(f"Project.keep must be 'all' or 'listed', got {self.keep!r}")
        if not self.bindings:
            raise InvalidArgError("Project needs at least one binding")
        in_schema = self.input.out_schema
        seen: set[str] = set()
        for name, e in self.bindings:
            if not name:
                raise InvalidArgError("a projected binding needs a name")
            if name in seen:
                raise InvalidArgError(f"duplicate projected name: {name!r}")
            seen.add(name)
            e.tau(in_schema)
        if isinstance(self.input, Aggregate):
            agg_cols = set(self.input.aggregate_columns)
            for name, e in self.bindings:
                used = [c for c in e.columns() if c in agg_cols]
                if len(used) > 1:
                    raise InvalidArgError(
                        "an expression may reference at most one aggregate output column "
                        "(arithmetic-over-aggregates is beyond v1)",
                        binding=name, aggregates=sorted(used))
        self._check_inputs()
        self.out_schema  # noqa: B018 - collisions surface at construction

    @property
    def sigma(self) -> Sigma:
        return self.sigma_

    @property
    def inputs(self) -> tuple[Node, ...]:
        return (self.input,)

    @property
    def out_schema(self) -> Schema:
        in_schema = self.input.out_schema
        listed = Schema(tuple(Column(n, e.tau(in_schema)) for n, e in self.bindings))
        return in_schema.concat(listed) if self.keep == "all" else listed

    @property
    def canonical_order(self) -> str:
        return "the input's"


# ---------------------------------------------------------------------------
# Join
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class Join(Node):
    """`Join(left, right, on, type ∈ {inner, left_outer, anti})`.

    Two rulings ride on this node:

    - **Duplicate probe keys**: `anti` accepts them (duplicates
      cannot change an absence test) and `left_outer` accepts them *with
      multiplication*. Rejecting instead would turn two `yes` rows into
      refusals on any corrected store, since every correction produces several
      believed versions per identity.
    - **The completeness precondition**: `left_outer` and `anti`
      derive rows from *absence* on the right and refuse (`E_INCOMPLETE`)
      unless the right input is execution-complete. That is a runtime refusal —
      truncation is not knowable at plan time — so it is recorded in
      `gnomon.plan.metadata` rather than checked here.
    """

    left: Node
    right: Node
    on: tuple[tuple[str, str], ...]
    join_type: str = "inner"
    sigma_: Sigma = DEFAULT_SIGMA

    def __post_init__(self) -> None:
        if self.join_type not in JOIN_TYPES:
            raise InvalidArgError(f"unknown join type: {self.join_type!r}",
                                  allowed=sorted(JOIN_TYPES))
        if not self.on:
            raise InvalidArgError("Join needs at least one key pair")
        ls, rs = self.left.out_schema, self.right.out_schema
        for lcol, rcol in self.on:
            lt, rt = ls.tau_of(lcol), rs.tau_of(rcol)
            if not lt.same_base(rt):
                raise InvalidArgError(
                    "join key types are incompatible",
                    left=[lcol, lt.to_json()], right=[rcol, rt.to_json()])
        self._check_inputs()
        self.out_schema  # noqa: B018 - collisions surface at construction

    @property
    def sigma(self) -> Sigma:
        return self.sigma_

    @property
    def inputs(self) -> tuple[Node, ...]:
        return (self.left, self.right)

    @property
    def out_schema(self) -> Schema:
        if self.join_type == "anti":
            # the right relation contributes no columns; it is a *probe*
            return self.left.out_schema
        right = self.right.out_schema
        if self.join_type == "left_outer":
            right = right.nullable()
        return self.left.out_schema.concat(right)

    @property
    def canonical_order(self) -> str:
        return "left row position, then right row position"


# ---------------------------------------------------------------------------
# PatternMatch
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class NodePat:
    """`(v : Label?)`."""

    var: str
    label: str | None = None

    def __post_init__(self) -> None:
        if not self.var:
            raise InvalidArgError("a node pattern needs a variable")

    def to_json(self) -> dict[str, Any]:
        return {"var": self.var, "label": self.label}


@dataclass(frozen=True, slots=True)
class EdgePat:
    """`(u) -[ e : RelType? ]-> (w)` or its undirected form."""

    var: str
    src: str
    dst: str
    rel_type: str | None = None
    directed: bool = True

    def __post_init__(self) -> None:
        if not self.var:
            raise InvalidArgError("an edge pattern needs a variable")

    def to_json(self) -> dict[str, Any]:
        return {"var": self.var, "src": self.src, "dst": self.dst,
                "rel_type": self.rel_type, "directed": self.directed}


@dataclass(frozen=True, slots=True)
class Pattern:
    node_pats: tuple[NodePat, ...]
    edge_pats: tuple[EdgePat, ...]

    def __post_init__(self) -> None:
        if not self.node_pats:
            raise InvalidArgError("a pattern needs at least one node")
        if not self.edge_pats:
            raise InvalidArgError("a pattern needs at least one edge")
        names = [p.var for p in self.node_pats] + [p.var for p in self.edge_pats]
        dupes = sorted({n for n in names if names.count(n) > 1})
        if dupes:
            raise InvalidArgError(f"duplicate pattern variable(s): {dupes}")
        node_vars = {p.var for p in self.node_pats}
        for e in self.edge_pats:
            missing = [v for v in (e.src, e.dst) if v not in node_vars]
            if missing:
                raise InvalidArgError(f"edge {e.var!r} names undeclared node(s): {missing}")

    def to_json(self) -> dict[str, Any]:
        return {"nodes": [p.to_json() for p in self.node_pats],
                "edges": [p.to_json() for p in self.edge_pats]}


@dataclass(frozen=True, slots=True)
class Source:
    """`sources: {v → r}` or `{v → (r, col)}`.

    `sources` **rebinds, it does not match prefixes**: an edge variable
    takes `r`'s edge identity column and a node variable takes `r`'s node
    identity column, and if `r` carries more than one column of that kind the
    binding **must name it**, because silently picking one would be a
    schema-dependent answer.
    """

    var: str
    relation: Node
    column: str | None = None

    def to_json(self) -> dict[str, Any]:
        return {"var": self.var, "column": self.column,
                "relation": self.relation.node_digest}


@dataclass(frozen=True, slots=True)
class PatternMatch(Node):
    """`PatternMatch(sources?, pattern) @ Σ` — general multi-way matching with
    edge-time bindings, **fixed length only**.

    Matching discipline is **edge-isomorphism over identities, not versions**:
    no two edge variables bind versions of the same `eid`, and
    node variables are *not* implicitly distinct. Under a version-based rule a
    correction that changed no property value would manufacture pattern
    instances no uncorrected store has.
    """

    pattern: Pattern
    sources: tuple[Source, ...] = ()
    sigma_: Sigma = DEFAULT_SIGMA

    reads_store = True

    def __post_init__(self) -> None:
        declared = {p.var for p in self.pattern.node_pats} | {p.var for p in self.pattern.edge_pats}
        for s in self.sources:
            if s.var not in declared:
                raise InvalidArgError(f"sources names a variable not in the pattern: {s.var!r}")
            if s.column is not None and s.column not in s.relation.out_schema:
                raise InvalidArgError(f"sources column not in the relation: {s.column!r}")
            candidates = self._identity_columns(s)
            if s.column is None and len(candidates) > 1:
                raise InvalidArgError(
                    "the source relation carries more than one identity column of that kind; "
                    "the binding must name it",
                    var=s.var, candidates=candidates)
            if s.column is None and not candidates:
                raise InvalidArgError(
                    "the source relation carries no identity column of that kind", var=s.var)
        seen = [s.var for s in self.sources]
        if len(set(seen)) != len(seen):
            raise InvalidArgError("a pattern variable may take at most one source")
        self._check_inputs()
        self.out_schema  # noqa: B018 - collisions surface at construction

    def _identity_columns(self, s: Source) -> list[str]:
        wanted = "eid" if any(p.var == s.var for p in self.pattern.edge_pats) else "uid"
        return [c.name for c in s.relation.out_schema if c.tau.base == wanted]

    @property
    def sigma(self) -> Sigma:
        return self.sigma_

    @property
    def inputs(self) -> tuple[Node, ...]:
        """`PatternMatch` takes no `input` relation — its inputs are the
        `sources` relations, which is what `scope_of`'s `⊎ ins` ranges over."""
        return tuple(s.relation for s in self.sources)

    @property
    def out_schema(self) -> Schema:
        out = Schema()
        for p in self.pattern.node_pats:
            out = out.concat(Schema(PATTERN_NODE_COLUMNS).prefixed(p.var))
        for e in self.pattern.edge_pats:
            out = out.concat(Schema(EDGE_COLUMNS).prefixed(e.var))
        return out

    @property
    def canonical_order(self) -> str:
        return ("lexicographic over bound edge (vt_s, vid) in pattern declaration order, "
                "then bound node uid in declaration order")


# ---------------------------------------------------------------------------
# Aggregate
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class Agg:
    """`(fn, of?, alias)`.

    **`mean` is an *atomic* aggregate**: defined as
    `sum/count` but computed as one aggregate rather than composed from two
    aggregate columns — written the composed way it would be
    `arithmetic-over-aggregates` and beyond v1. Two consequences are normative:
    an empty group emits no row at all, so `mean` **never yields NaN and never
    raises a division error**, and its value is formed under the IR's blessed
    arithmetic rule.
    """

    fn: str
    alias: str
    of: Expr | None = None
    #: `collect` only (additive; omitted from `to_json` at their defaults):
    #: the order of the collected list, its length bound, and whether equal
    #: elements are kept once
    order: tuple["SortKey", ...] = ()
    limit: int | None = None
    distinct: bool = False

    def __post_init__(self) -> None:
        from gnomon.plan.expr import TupleExpr

        if self.fn not in AGG_FNS:
            raise InvalidArgError(f"unknown aggregate: {self.fn!r}", allowed=sorted(AGG_FNS))
        if not self.alias:
            raise InvalidArgError("an aggregate needs an alias")
        if self.fn == "count" and self.of is not None:
            raise InvalidArgError("count takes no `of`")
        if self.fn != "count" and self.of is None:
            raise InvalidArgError(f"{self.fn} needs an `of` expression")
        if self.fn == "collect":
            if not isinstance(self.of, TupleExpr):
                raise InvalidArgError("collect's `of` is a tuple of the element's components")
            if self.limit is not None and self.limit < 1:
                raise InvalidArgError("collect's limit must be >= 1")
        elif self.order or self.limit is not None or self.distinct:
            raise InvalidArgError("order, limit and distinct are collect's alone")

    def tau(self, in_schema: Schema) -> Tau:
        if self.fn == "collect":
            assert self.of is not None
            self.of.tau(in_schema)
            for k in self.order:
                k.key.tau(in_schema)
            # a list per group: of the element value, or of the component list
            return T_JSON
        if self.fn in ("count", "count_distinct"):
            if self.of is not None:
                self.of.tau(in_schema)
            return T_INT
        assert self.of is not None
        inner = self.of.tau(in_schema)
        if self.fn == "mean":
            # The schema rules ask for "int-or-float under the blessed rule";
            # the type lattice has no union type, so the wider of the two is
            # the static answer.
            return T_FLOAT
        if self.fn == "sum_exact":
            if inner.base == "float" or inner.base == "json":
                return T_FLOAT.optional() if inner.nullable else T_FLOAT
            if inner.base not in ("int", "ts"):
                raise InvalidArgError(f"sum_exact needs a numeric `of`, got {inner.to_json()}")
        return inner  # sum / min / max keep the input column's type

    def to_json(self) -> dict[str, Any]:
        out: dict[str, Any] = {"fn": self.fn, "alias": self.alias,
                               "of": self.of.to_json() if self.of is not None else None}
        if self.order:
            out["order"] = [k.to_json() for k in self.order]
        if self.limit is not None:
            out["limit"] = self.limit
        if self.distinct:
            out["distinct"] = True
        return out


@dataclass(frozen=True, slots=True)
class Aggregate(Node):
    """`Aggregate(input, group_by, aggregates)`.

    - **Group-by arity is unrestricted** — today's two-slot cap is an
      operator-boundary artifact, not a semantic gap. `group_by = []` yields
      exactly one row; `aggregates = []` is `DISTINCT` over the key.
    - **Non-empty groups only.** There is no densified group axis and no bucket
      generator in v1, which is exactly why `graph_metric_timeseries`,
      `burst_detection` and `neighborhood_evolution`'s degree series stay
      opaque: their series are *dense*, zeros included.
    - **Precondition.** The input must be execution-complete over its declared
      domain or the `Aggregate` refuses (`E_INCOMPLETE`). `Aggregate` consumes
      *relations*, never *pages* — a page cut belongs at the plan's output
      boundary, so a page-cut `Limit` beneath an `Aggregate` is a
      defect and is rejected here.
    """

    input: Node
    group_by: tuple[tuple[str, Expr], ...] = ()
    aggregates: tuple[Agg, ...] = ()
    sigma_: Sigma = DEFAULT_SIGMA

    def __post_init__(self) -> None:
        in_schema = self.input.out_schema
        names = [n for n, _ in self.group_by] + [a.alias for a in self.aggregates]
        dupes = sorted({n for n in names if names.count(n) > 1})
        if dupes:
            raise InvalidArgError(f"duplicate output name(s): {dupes}")
        for _, e in self.group_by:
            e.tau(in_schema)
        for a in self.aggregates:
            a.tau(in_schema)
        if isinstance(self.input, Limit) and not self.input.is_top_k:
            raise InvalidArgError(
                "Aggregate consumes relations, never pages: a page-cut Limit narrows "
                "delivery rather than the domain, so aggregating it would answer for the "
                "page while claiming the population")
        if isinstance(self.input, Aggregate):
            raise InvalidArgError("aggregate over aggregate is beyond v1")
        self._check_inputs()
        self.out_schema  # noqa: B018 - collisions surface at construction

    @property
    def sigma(self) -> Sigma:
        return self.sigma_

    @property
    def inputs(self) -> tuple[Node, ...]:
        return (self.input,)

    @property
    def aggregate_columns(self) -> tuple[str, ...]:
        return tuple(a.alias for a in self.aggregates)

    @property
    def out_schema(self) -> Schema:
        in_schema = self.input.out_schema
        keys = Schema(tuple(Column(n, e.tau(in_schema)) for n, e in self.group_by))
        aggs = Schema(tuple(Column(a.alias, a.tau(in_schema)) for a in self.aggregates))
        return keys.concat(aggs)

    @property
    def canonical_order(self) -> str:
        return "by group key values — numeric for numeric keys, code point for strings, nulls first"


# ---------------------------------------------------------------------------
# Order and Limit
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class SortKey:
    key: Expr
    direction: str = "asc"
    nulls: str = "nulls_last"

    def __post_init__(self) -> None:
        if self.direction not in ("asc", "desc"):
            raise InvalidArgError(f"unknown sort direction: {self.direction!r}")
        if self.nulls not in ("nulls_first", "nulls_last"):
            raise InvalidArgError(f"unknown nulls order: {self.nulls!r}")

    def to_json(self) -> dict[str, Any]:
        return {"key": self.key.to_json(), "dir": self.direction, "nulls": self.nulls}


@dataclass(frozen=True, slots=True)
class Order(Node):
    """`Order(input, keys)`.

    **A total order is required**: the declared keys are extended with the
    input's own canonical order as a final tiebreak, so the canonical result
    hash is well defined. `@OrderedBy(f)` is *reserved, not implemented* — a
    plan result may carry the ordering key in its provenance, but v1 certifies
    no ordering claim from it.
    """

    input: Node
    keys: tuple[SortKey, ...]
    sigma_: Sigma = DEFAULT_SIGMA

    def __post_init__(self) -> None:
        if not self.keys:
            raise InvalidArgError("Order needs at least one key")
        for k in self.keys:
            k.key.tau(self.input.out_schema)
        self._check_inputs()

    @property
    def sigma(self) -> Sigma:
        return self.sigma_

    @property
    def inputs(self) -> tuple[Node, ...]:
        return (self.input,)

    @property
    def out_schema(self) -> Schema:
        return self.input.out_schema

    @property
    def canonical_order(self) -> str:
        return "the declared keys, tiebroken by the input's own canonical order"


@dataclass(frozen=True, slots=True)
class Limit(Node):
    """`Limit(input, n, offset|cursor)`.

    Two distinct uses, and they produce different metadata:
    directly above an `Order` it is **top-k** — the declared domain narrows to
    "the `n` greatest rows under the recorded ranking key" — and otherwise it is
    a **page cut**, where delivery is incomplete and execution is not.
    `is_top_k` is exactly that syntactic test.
    """

    input: Node
    n: int
    offset: int | None = None
    cursor: str | None = None
    sigma_: Sigma = DEFAULT_SIGMA

    def __post_init__(self) -> None:
        if self.n < 1:
            raise InvalidArgError(f"Limit.n must be >= 1, got {self.n}")
        if self.offset is not None and self.cursor is not None:
            raise InvalidArgError("Limit takes an offset or a cursor, not both")
        if self.offset is not None and self.offset < 0:
            raise InvalidArgError(f"Limit.offset must be >= 0, got {self.offset}")
        self._check_inputs()

    @property
    def sigma(self) -> Sigma:
        return self.sigma_

    @property
    def inputs(self) -> tuple[Node, ...]:
        return (self.input,)

    @property
    def is_top_k(self) -> bool:
        return isinstance(self.input, Order)

    @property
    def out_schema(self) -> Schema:
        return self.input.out_schema

    @property
    def canonical_order(self) -> str:
        return "the input's, cut"


# ---------------------------------------------------------------------------
# the pattern operator's additive nodes (not core: see CORE_NODE_TYPES)
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class Union(Node):
    """`Union(inputs…)`: the bag union (`UNION ALL`) of two or more relations
    with the same column names in the same order. Column types unify as an
    `if`'s branches do (same base, or numeric widening; nullable if any
    input is). The output is each input's rows in input order. Additive: no
    existing plan contains it."""

    branches: tuple[Node, ...]
    sigma_: Sigma = DEFAULT_SIGMA

    def __post_init__(self) -> None:
        if len(self.branches) < 2:
            raise InvalidArgError("Union needs at least two inputs")
        names = self.branches[0].out_schema.names
        for b in self.branches[1:]:
            if b.out_schema.names != names:
                raise InvalidArgError("Union inputs must have the same columns in the same order",
                                      first=list(names), other=list(b.out_schema.names))
        self._check_inputs()
        self.out_schema  # noqa: B018 - type conflicts surface at construction

    @property
    def sigma(self) -> Sigma:
        return self.sigma_

    @property
    def inputs(self) -> tuple[Node, ...]:
        return self.branches

    @property
    def out_schema(self) -> Schema:
        from gnomon.plan.expr import _unify

        first = self.branches[0].out_schema
        cols = []
        for c in first:
            tau = c.tau
            for b in self.branches[1:]:
                tau = _unify(tau, b.out_schema.tau_of(c.name), f"union column {c.name!r}")
            cols.append(Column(c.name, tau))
        return Schema(tuple(cols))

    @property
    def canonical_order(self) -> str:
        return "input order, then each input's row order"


@dataclass(frozen=True, slots=True)
class TopK(Node):
    """`TopK(input, partition_by, keys, n)`: per partition (equal values of
    the `partition_by` expressions, null equal to null), the first `n` rows
    under `keys`, ties kept in input order; the kept rows in that ranking,
    partitions interleaved. Additive: no existing plan contains it."""

    input: Node
    partition_by: tuple[Expr, ...]
    keys: tuple[SortKey, ...]
    n: int
    sigma_: Sigma = DEFAULT_SIGMA

    def __post_init__(self) -> None:
        if self.n < 1:
            raise InvalidArgError(f"TopK.n must be >= 1, got {self.n}")
        if not self.partition_by or not self.keys:
            raise InvalidArgError("TopK needs partition expressions and sort keys")
        schema = self.input.out_schema
        for e in self.partition_by:
            e.tau(schema)
        for k in self.keys:
            k.key.tau(schema)
        self._check_inputs()

    @property
    def sigma(self) -> Sigma:
        return self.sigma_

    @property
    def inputs(self) -> tuple[Node, ...]:
        return (self.input,)

    @property
    def out_schema(self) -> Schema:
        return self.input.out_schema

    @property
    def canonical_order(self) -> str:
        return "the declared keys, tiebroken by the input's own order"


@dataclass(frozen=True, slots=True)
class Walk(Node):
    """`Walk(input, from, into, regex)`: a regular path step. Per input row,
    every node reachable from `from` along a path of edges whose labels spell
    a word of the expression (`gnomon.plan.regex`), each once: a node relation
    like a variable-length `Expand`'s, without a depth. `regex` is the
    expression's canonical JSON text. Every edge is one Σ admits; `into`
    binds as an `Expand` target does (null version columns where none is
    visible). Additive: no existing plan contains it."""

    input: Node
    from_: str
    into: str
    regex: str
    sigma_: Sigma = DEFAULT_SIGMA

    reads_store = True

    def __post_init__(self) -> None:
        import json

        from gnomon.plan.regex import canonical, text

        try:
            spec = json.loads(self.regex)
        except ValueError as e:
            raise InvalidArgError(f"Walk.regex is JSON text: {e}") from None
        if text(canonical(spec)) != self.regex:
            raise InvalidArgError("Walk.regex must be the expression's canonical text")
        self.from_column  # noqa: B018 - resolves `from`, or fails at construction
        self._check_inputs()
        self.out_schema  # noqa: B018

    @property
    def sigma(self) -> Sigma:
        return self.sigma_

    @property
    def inputs(self) -> tuple[Node, ...]:
        return (self.input,)

    @property
    def from_column(self) -> str:
        from gnomon.plan.anchor import uid_column
        return uid_column(self.input, self.from_)

    @property
    def out_schema(self) -> Schema:
        return self.input.out_schema.concat(Schema(NODE_COLUMNS).prefixed(self.into))

    @property
    def canonical_order(self) -> str:
        return "(input row position, into)"


#: `OrderedReach`'s orders: along the traversal, each edge's time after the
#: previous one's (strictly or not), before it, or unconstrained.
REACH_ORDERS: frozenset[str] = frozenset({"none", "strict_increasing", "increasing",
                                          "strict_decreasing", "decreasing"})


@dataclass(frozen=True, slots=True)
class OrderedReach(Node):
    """`OrderedReach(input, edges, from, into, …)`: a variable-length step over
    an explicit edge relation. `edges` has a source, a target and a time
    column (and an optional truncation key); traversal goes source to
    target. A path is valid when consecutive edges' times follow `order`
    (along the traversal). Per input row, every node at a valid path's
    shortest length `d` with `lo <= d <= hi` (`hi` None: unbounded) binds
    `into` and `<into>.depth`. With `limit`, a node's edges are first cut to
    the `limit` first under (`key`, `key_desc`), then time, then target uid.

    The search keeps, per node, the best arrival time so far (the earliest
    for an increasing order): a later state that arrives no better is
    dominated, since every continuation open to it is open to the better one
    at a smaller depth. Additive: no existing plan contains it."""

    input: Node
    edges: Node
    from_: str
    into: str
    src: str
    dst: str
    time: str
    lo: int = 1
    hi: int | None = None
    order: str = "none"
    limit: int | None = None
    key: str | None = None
    key_desc: bool = False
    sigma_: Sigma = DEFAULT_SIGMA

    reads_store = True

    def __post_init__(self) -> None:
        if self.order not in REACH_ORDERS:
            raise InvalidArgError(f"unknown order {self.order!r}", allowed=sorted(REACH_ORDERS))
        if self.lo < 0 or (self.hi is not None and self.hi < max(self.lo, 1)):
            raise InvalidArgError("OrderedReach needs 0 <= lo <= hi and hi >= 1")
        if self.limit is not None and (self.limit < 1 or self.key is None):
            raise InvalidArgError("a truncation needs limit >= 1 and a key")
        es = self.edges.out_schema
        for c in (self.src, self.dst):
            if es.tau_of(c).base != "uid":
                raise InvalidArgError(f"OrderedReach endpoint column {c!r} is not a uid")
        if es.tau_of(self.time).base not in ("ts", "int"):
            raise InvalidArgError("OrderedReach time column must be a timestamp")
        if self.key is not None and es.tau_of(self.key).base not in ("ts", "int", "float"):
            raise InvalidArgError("OrderedReach truncation key must be numeric")
        self.from_column  # noqa: B018
        self._check_inputs()
        self.out_schema  # noqa: B018

    @property
    def sigma(self) -> Sigma:
        return self.sigma_

    @property
    def inputs(self) -> tuple[Node, ...]:
        return (self.input, self.edges)

    @property
    def from_column(self) -> str:
        from gnomon.plan.anchor import uid_column
        return uid_column(self.input, self.from_)

    @property
    def out_schema(self) -> Schema:
        return self.input.out_schema.concat(Schema(NODE_COLUMNS).prefixed(self.into)).concat(
            Schema.of(Column(f"{self.into}.depth", T_INT)))

    @property
    def canonical_order(self) -> str:
        return "(input row position, into.depth, into)"


@dataclass(frozen=True, slots=True)
class Succ(Node):
    """`Succ(input, of, into, kind, axis, dir)`: per input row, the version
    that follows (`dir="next"`) or precedes (`"prev"`) the version bound to
    `of`, among the versions of the same identity (a node's uid, an edge's
    eid) that a `kind` scan with this node's belief, mode and Σ returns,
    ordered by the axis's start (`vt_s` or `tt_s`) then the version id. It
    binds `into` with the scan's columns; a row without one is dropped.
    Additive: no existing plan contains it."""

    input: Node
    of: str
    into: str
    kind: str
    axis: str = "valid"
    dir: str = "next"
    belief: str = "current"
    vt_mode: str = "overlap"
    sigma_: Sigma = DEFAULT_SIGMA

    reads_store = True

    def __post_init__(self) -> None:
        if self.kind not in ("node", "edge") or self.axis not in ("valid", "transaction") \
                or self.dir not in ("next", "prev"):
            raise InvalidArgError("Succ: kind is node|edge, axis valid|transaction, "
                                  "dir next|prev")
        check_belief(self.belief)
        check_vt_mode(self.vt_mode)
        if f"{self.of}.vid" not in self.input.out_schema:
            raise InvalidArgError(f"Succ: {self.of!r} binds no version")
        self._check_inputs()
        self.out_schema  # noqa: B018

    @property
    def sigma(self) -> Sigma:
        return self.sigma_

    @property
    def inputs(self) -> tuple[Node, ...]:
        return (self.input,)

    @property
    def out_schema(self) -> Schema:
        cols = NODE_COLUMNS if self.kind == "node" else EDGE_COLUMNS
        return self.input.out_schema.concat(Schema(cols).prefixed(self.into))

    @property
    def canonical_order(self) -> str:
        return "the input's"


#: The time type of a shared-instant fragment's bounds (a node's `vt_s`).
_T_TS = NODE_COLUMNS[3].tau


@dataclass(frozen=True, slots=True)
class CoValidExpand(Node):
    """`CoValidExpand(input, from, into, hops, rel_type, dir, belief)`: a
    variable-length step under one shared valid instant (the shared-instant rule).

    Per input row, a breadth-first search over (node, instant set) states
    from the bound end, starting from the instants of `from_iv` (the bound
    end's version interval) inside Σ's window, or the whole window when
    `from_iv` is None. An edge (in `dir`, of `rel_type`, admitted by `belief`,
    overlapping the window) carries the instants where it is valid and its far
    end has an admitted version valid; a node keeps, per instant, its shortest
    distance `d_t`. `into` binds every admitted version of a node kept at some
    instant with `lo <= d_t <= hi`, once per maximal interval of constant
    `d_t` inside that version: `<into>.depth` is that `d_t`, and
    `<into>.ps`/`<into>.pe` the interval. Additive: no existing plan
    contains it."""

    input: Node
    from_: str
    into: str
    hops: HopSpec
    rel_type: str | None = None
    dir: str = "out"
    belief: str = "current"
    from_iv: tuple[str, str] | None = None
    sigma_: Sigma = DEFAULT_SIGMA

    reads_store = True

    def __post_init__(self) -> None:
        if not self.hops.variable_length:
            raise InvalidArgError("CoValidExpand takes a bounded or unbounded hop form")
        if self.dir not in DIRECTIONS:
            raise InvalidArgError(f"unknown direction: {self.dir!r}", allowed=sorted(DIRECTIONS))
        check_belief(self.belief)
        if self.from_iv is not None:
            schema = self.input.out_schema
            for c in self.from_iv:
                if schema.tau_of(c).base not in ("ts", "int"):
                    raise InvalidArgError(f"CoValidExpand: {c!r} is not a time column")
        self.from_column  # noqa: B018
        self._check_inputs()
        self.out_schema  # noqa: B018

    @property
    def sigma(self) -> Sigma:
        return self.sigma_

    @property
    def inputs(self) -> tuple[Node, ...]:
        return (self.input,)

    @property
    def from_column(self) -> str:
        from gnomon.plan.anchor import uid_column
        return uid_column(self.input, self.from_)

    @property
    def out_schema(self) -> Schema:
        return self.input.out_schema.concat(Schema(NODE_COLUMNS).prefixed(self.into)).concat(
            Schema.of(Column(f"{self.into}.depth", T_INT), Column(f"{self.into}.ps", _T_TS),
                      Column(f"{self.into}.pe", _T_TS)))

    @property
    def canonical_order(self) -> str:
        return "(input row position, into.depth, into, into.ps)"


@dataclass(frozen=True, slots=True)
class CoValidJoin(Node):
    """`CoValidJoin(left, right, on, type, left_iv, right_iv, out_iv)`: an
    `optional` (`left_outer`) or `not` (`anti`) sub-pattern under one shared
    valid instant. Per left row, with fragment `left_iv`, the
    right rows on its keys are its extensions, each valid on `right_iv`.
    `left_outer` emits each extension that overlaps the fragment, on the
    intersection, then the left row with nulls on every maximal part of the
    fragment no extension covers; `anti` emits only the latter. `out_iv`
    names the resulting fragment. Additive: no existing plan contains it."""

    left: Node
    right: Node
    on: tuple[tuple[str, str], ...]
    join_type: str
    left_iv: tuple[str, str]
    right_iv: tuple[str, str]
    out_iv: tuple[str, str]
    sigma_: Sigma = DEFAULT_SIGMA

    def __post_init__(self) -> None:
        if self.join_type not in ("left_outer", "anti"):
            raise InvalidArgError("CoValidJoin is left_outer or anti")
        if not self.on:
            raise InvalidArgError("CoValidJoin needs at least one key pair")
        ls, rs = self.left.out_schema, self.right.out_schema
        for lcol, rcol in self.on:
            if not ls.tau_of(lcol).same_base(rs.tau_of(rcol)):
                raise InvalidArgError("join key types are incompatible", left=lcol, right=rcol)
        for schema, cols in ((ls, self.left_iv), (rs, self.right_iv)):
            for c in cols:
                if schema.tau_of(c).base not in ("ts", "int"):
                    raise InvalidArgError(f"CoValidJoin: {c!r} is not a time column")
        self._check_inputs()
        self.out_schema  # noqa: B018

    @property
    def sigma(self) -> Sigma:
        return self.sigma_

    @property
    def inputs(self) -> tuple[Node, ...]:
        return (self.left, self.right)

    @property
    def out_schema(self) -> Schema:
        out = self.left.out_schema
        if self.join_type == "left_outer":
            out = out.concat(self.right.out_schema.nullable())
        return out.concat(Schema.of(Column(self.out_iv[0], _T_TS), Column(self.out_iv[1], _T_TS)))

    @property
    def canonical_order(self) -> str:
        return "left row position, then right row position, then remainder fragments by start"


@dataclass(frozen=True, slots=True)
class Collapse(Node):
    """`Collapse(input, by, mins)`: one row per distinct value of the `by`
    columns (null equal to null), the first in input order, with each `mins`
    column replaced by its least non-null value in the group. Under the shared-instant rule it
    turns the fragments of one version tuple's valid set into one binding.
    Additive: no existing plan contains it."""

    input: Node
    by: tuple[str, ...]
    mins: tuple[str, ...] = ()
    sigma_: Sigma = DEFAULT_SIGMA

    def __post_init__(self) -> None:
        if not self.by:
            raise InvalidArgError("Collapse needs at least one key column")
        schema = self.input.out_schema
        for c in self.by:
            schema.tau_of(c)
        for c in self.mins:
            if schema.tau_of(c).base not in ("int", "ts"):
                raise InvalidArgError(f"Collapse: {c!r} is not an integer column")
        self._check_inputs()

    @property
    def sigma(self) -> Sigma:
        return self.sigma_

    @property
    def inputs(self) -> tuple[Node, ...]:
        return (self.input,)

    @property
    def out_schema(self) -> Schema:
        return self.input.out_schema

    @property
    def canonical_order(self) -> str:
        return "the first row of each group, in input order"


#: The pattern operator's additive node kinds, outside the IR's twelve.
MATCH_NODE_TYPES: tuple[type[Node], ...] = (Union, TopK, Walk, OrderedReach, Succ,
                                          CoValidExpand, CoValidJoin, Collapse)


# ---------------------------------------------------------------------------
# R7 — the opaque leaf
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class OpaqueLeaf(Node):
    """One of the fifteen existing operators, as a plan node.

    The opaque leaf is a **plan-node kind**, not a registry annotation and not a
    wrapper object around the kernel. Both alternatives were considered and
    rejected for reasons that are properties of this class:

    - Σ and `out_schema` are **per call**, not per operator —
      `entity_history`'s `edges` list exists only when `include_edges`, and Σ
      comes from `as_of_tt` / `window` / `t_valid` / `t1` / `t2`. An annotation
      has no instance to carry them.
    - The `∅` classification must be a *checkable* property: "an `∅`-classified kernel must not receive a live storage
      adapter". The adapter is passed **per call**, so only a plan node can
      carry the decision to withhold it — `withhold_adapter`, here.
    - It is not a wrapper holding the kernel callable: that would make the
      leaf's identity depend on a Python function object and break
      `node_digest`'s content addressing (it digests `(op, canonical args, Σ,
      input digests)` — all data).

    `withhold_adapter` may be widened (withheld where the classification does
    not require it) but never narrowed on a known-`∅` operator; constructing
    `compute` with `withhold_adapter = False` is refused rather than accepted
    quietly, because the whole point of the classification is that a
    misclassification fails loudly at the first read instead of rotting into
    silent unsoundness.
    """

    op_name: str
    bound_args: tuple[tuple[str, Any], ...]
    out_fields: tuple[str, ...]
    vt_mode: str = "overlap"
    withhold_adapter: bool | None = None
    sigma_: Sigma = DEFAULT_SIGMA

    def __post_init__(self) -> None:
        if not self.op_name:
            raise InvalidArgError("an opaque leaf names an operator")
        check_vt_mode(self.vt_mode)
        if not self.out_fields:
            raise InvalidArgError("an opaque leaf carries its operator's output_fields")
        keys = [k for k, _ in self.bound_args]
        dupes = sorted({k for k in keys if keys.count(k) > 1})
        if dupes:
            raise InvalidArgError(f"duplicate bound argument(s): {dupes}")
        required = self.op_name in EMPTY_SCOPE_OPS
        if self.withhold_adapter is None:
            object.__setattr__(self, "withhold_adapter", required)
        elif required and not self.withhold_adapter:
            raise InvalidArgError(
                f"{self.op_name!r} is ∅-classified: its kernel never touches the adapter, "
                "so the adapter must be withheld")

    @staticmethod
    def build(op: str, bound_args: dict[str, Any], out_fields: tuple[str, ...], *,
              sigma: Sigma = DEFAULT_SIGMA, vt_mode: str = "overlap") -> "OpaqueLeaf":
        """Construct from post-`validate_args`, post-`_fill_defaults` arguments —
        the shape `call_operator` already has at `algebra.py:145`."""
        return OpaqueLeaf(op, tuple(sorted(bound_args.items())), tuple(out_fields),
                          vt_mode, None, sigma)

    @property
    def sigma(self) -> Sigma:
        return self.sigma_

    @property
    def op(self) -> str:
        return self.op_name

    @property
    def args(self) -> dict[str, Any]:
        return dict(self.bound_args)

    @property
    def reads_store(self) -> bool:  # type: ignore[override]
        return not self.withhold_adapter

    @property
    def out_schema(self) -> Schema:
        """Derived from `OperatorSpec.output_fields`. The leaf is opaque, so
        the payload's *field* names are the contract and their types are
        not modelled: an operator's list field is `json`, and the two scalar
        pagination fields are typed because the completeness rules read
        them."""
        cols = []
        for f in self.out_fields:
            if f == "truncated":
                tau = Tau("bool")
            elif f.endswith("_total") or f == "rows_total":
                tau = T_INT
            elif f == "cursor":
                tau = Tau("str").optional()
            else:
                tau = Tau("json")
            cols.append(Column(f, tau))
        return Schema(tuple(cols))

    @property
    def canonical_order(self) -> str:
        return "inherited unchanged from the kernel; the leaf asserts nothing new"


# ---------------------------------------------------------------------------
# one compilation's memo of the derived properties
# ---------------------------------------------------------------------------

#: `id(node) -> (node, value)` per property while `schema_memo()` is active;
#: the node is held so its id cannot be reused within the memo's life.
_MEMO: dict[str, dict[int, tuple[Any, Any]]] | None = None


@contextlib.contextmanager
def schema_memo() -> Iterator[None]:
    """Within the block, each node's `out_schema` and `node_digest` are
    computed once. Both are pure functions of the (immutable) node, and both
    recurse over the whole subtree on every read, so a compiler that reads
    them for every node it builds is otherwise quadratic in the plan's depth.
    Nested blocks share the outermost memo."""
    global _MEMO
    outer = _MEMO
    if outer is None:
        _MEMO = {"out_schema": {}, "node_digest": {}}
    try:
        yield
    finally:
        if outer is None:
            _MEMO = None


def _memoized(name: str, prop: property) -> property:
    fget = prop.fget
    assert fget is not None

    def get(self: Any) -> Any:
        memo = _MEMO
        if memo is None:
            return fget(self)
        table = memo[name]
        hit = table.get(id(self))
        if hit is not None and hit[0] is self:
            return hit[1]
        value = fget(self)
        table[id(self)] = (self, value)
        return value

    get.__doc__ = fget.__doc__
    return property(get)


def _install_memo() -> None:
    seen: list[type] = [Node]
    for cls in seen:
        seen.extend(c for c in cls.__subclasses__() if c not in seen)
        for name in ("out_schema", "node_digest"):
            prop = cls.__dict__.get(name)
            if isinstance(prop, property):
                setattr(cls, name, _memoized(name, prop))


#: The twelve compositional core node types, in the IR's order.
CORE_NODE_TYPES: tuple[type[Node], ...] = (
    NodeScan, EdgeScan, Expand, Filter, PropertyPredicate, TypeConstraint,
    Project, Join, PatternMatch, Aggregate, Order, Limit,
)

#: The `∅` classification of the core: four store-reading, eight pure.
STORE_READING_CORE: tuple[type[Node], ...] = (NodeScan, EdgeScan, Expand, PatternMatch)


__all__ = [
    "AGG_FNS", "Agg", "AggFn", "Aggregate", "Bounded", "CORE_NODE_TYPES",
    "DIRECTIONS", "Direction", "EMPTY_SCOPE_OPS", "EdgePat", "EdgeScan",
    "Endpoints", "Exact", "Expand", "Filter", "HopSpec", "INCIDENT_ROLES",
    "JOIN_TYPES", "Join", "JoinType", "Limit", "Node", "NodePat", "NodeScan",
    "NullsOrder", "OpaqueLeaf", "Order", "Pattern", "PatternMatch", "Project",
    "PropertyPredicate", "STORE_READING_CORE", "SortDir", "SortKey", "Source",
    "TypeConstraint", "Unbounded", "Union", "MATCH_NODE_TYPES", "TopK", "Walk",
    "OrderedReach", "REACH_ORDERS", "Succ", "CoValidExpand", "CoValidJoin", "Collapse",
    "schema_memo",
]

_install_memo()
