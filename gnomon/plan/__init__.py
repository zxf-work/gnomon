"""PLAN_IR — the Temporal Graph Intermediate Representation.

nested inside neither, for a reason that is checkable rather than aesthetic:
`gnomon/temporal/algebra.py` already imports `gnomon.storage.base` and is imported
by `gnomon.tools.server` and `gnomon.agent.*`, while PLAN_IR must be importable by all
three *and* must import the registry. So it sits above `temporal` and below
`tools`/`agent`; putting it inside `temporal/` would create the import cycle
`algebra → plan_ir → algebra`.

every operator call and evaluates it here, so `leaf`, `evaluate`, `guard`,
`rollout` and `ttq` are on the live path; `node`, `types`, `expr`, `metadata`,
`depscope`, `anchor`, `scope_of` and `plan` support them. **No operator kernel
is touched** — the leaf delegates to `REGISTRY[op].fn` with the
same arguments, which is why every `result_digest` in the tree is unchanged.

What is here:

- `expr` — the IR's row-local expression language (the formal content of R2).
  that carries the fifteen existing operators, with the IR's schema
  propagation and the static plan errors.
- `metadata` — the IR's completeness lattice with its meet, `Exactness`,
  The IR's provenance, and the `R` tuple minus value.
  `⊎`.
- `guard` — the `∅`-kernel `NullAdapter`, which turns the IR's classification into
  a checkable property.
  kernel delegation, the `R` tuple, and the single-node plan every call becomes.
  four envelope keys `tt_q` / `pinned` / `clamped` / `dependency`.

Still to come: `leaves.py` and
`compiled/`.
"""

from gnomon.plan.anchor import Anchor, anchor_of, anchor_of_var, anchors_of
from gnomon.plan.depscope import (
    CARVE_PROPS, FULL_SCAN_CHECKPOINTS, INCIDENT_ROLES, KINDS, K_DENSE_ID, K_EDGE,
    K_NODE, PSEUDO_PROPS, SCHEMA_NAME, SCHEMA_VERSION, TOP, TOP_TERM, UNANCHORED,
    Checkpoint, DependencyScope, EdgeKey, Incident, ScopeTerm, Targets,
    store_identity, union_all, vt_carve, vt_closed, vt_from,
)
from gnomon.plan.expr import (
    Arith, BoolOp, Cast, Cmp, Coalesce, Col, Expr, If, IsNull, Lit, MathFn, Not,
    PropRef, StartsWith, TupleExpr, TypedProp,
)
from gnomon.plan.guard import NullAdapter, adapter_for, is_empty_scope_op
from gnomon.plan.metadata import (
    CERTIFICATION_LAYER, MIDDLE, Completeness, Exactness, Provenance, ResultMeta,
    ScanDescriptor, VidSet, comparable, compute_node_digest, le, meet, meet_all,
    meet_exactness,
)
from gnomon.plan.node import (
    AGG_FNS, CORE_NODE_TYPES, DIRECTIONS, EMPTY_SCOPE_OPS, JOIN_TYPES,
    STORE_READING_CORE, Agg, Aggregate, Bounded, EdgePat, EdgeScan, Endpoints,
    Exact, Expand, Filter, HopSpec, Join, Limit, Node, NodePat, NodeScan,
    OpaqueLeaf, Order, Pattern, PatternMatch, Project, PropertyPredicate, SortKey,
    Source, TypeConstraint, Unbounded,
)
#: `evaluate` and `scope_of` are **modules**, and the functions of the same
#: name inside them are deliberately *not* re-exported here: binding the
#: function to the package attribute shadows the submodule, so
#: `from gnomon.plan import evaluate` would hand back a function and
#: `gnomon.plan.evaluate.evaluate_leaf` would raise `AttributeError`. Import them
#: from their module (`from gnomon.plan.evaluate import evaluate`).
from gnomon.plan.evaluate import (
    Evaluation, evaluate_leaf, leaf_completeness, leaf_meta, leaf_provenance,
    meta_for, meta_json,
)
from gnomon.plan.leaf import LEAF_VT_MODE, build_leaf, sigma_for
from gnomon.plan.plan import Plan
from gnomon.plan.rollout import COMPILE_MODE, PLAN_PATH_ENV, compile_mode, plan_path_enabled
from gnomon.plan.scope_of import ScopeBasis, leaf_scope
from gnomon.plan.ttq import (
    Frontier, TtQ, as_of_tt_of, basis_of, checkpoints_of, clamp, dependency_of,
    envelope_metadata, frontier_of, store_identity_of,
)
from gnomon.plan.types import (
    BELIEF_MODES, DEFAULT_SIGMA, SCALAR_TAUS, VT_MODES, Column, Schema, Sigma, Tau,
    edge_schema, node_schema,
)

__all__ = [
    "adapter_for", "Agg", "AGG_FNS", "Aggregate", "Anchor", "anchor_of", "anchor_of_var",
    "anchors_of", "Arith", "as_of_tt_of", "basis_of", "BELIEF_MODES", "BoolOp", "Bounded",
    "build_leaf", "CARVE_PROPS", "Cast", "CERTIFICATION_LAYER", "Checkpoint",
    "checkpoints_of", "clamp", "Cmp", "Coalesce", "Col", "Column", "comparable",
    "COMPILE_MODE", "compile_mode", "Completeness", "compute_node_digest",
    "CORE_NODE_TYPES", "DEFAULT_SIGMA", "dependency_of", "DependencyScope", "DIRECTIONS",
    "edge_schema", "EdgeKey", "EdgePat", "EdgeScan", "EMPTY_SCOPE_OPS", "Endpoints",
    "envelope_metadata", "evaluate_leaf", "Evaluation", "Exact", "Exactness",
    "Expand", "Expr", "Filter", "Frontier", "frontier_of", "FULL_SCAN_CHECKPOINTS",
    "HopSpec", "If", "Incident", "INCIDENT_ROLES", "is_empty_scope_op", "IsNull", "Join",
    "JOIN_TYPES", "K_DENSE_ID", "K_EDGE", "K_NODE", "KINDS", "le", "leaf_completeness",
    "leaf_meta", "leaf_provenance", "leaf_scope", "LEAF_VT_MODE", "Limit", "Lit", "MathFn",
    "meet", "meet_all", "meet_exactness", "meta_for", "meta_json", "MIDDLE", "Node",
    "node_schema", "NodePat", "NodeScan", "Not", "NullAdapter", "OpaqueLeaf", "Order",
    "Pattern", "PatternMatch", "Plan", "plan_path_enabled", "PLAN_PATH_ENV", "Project",
    "PropertyPredicate", "PropRef", "Provenance", "PSEUDO_PROPS", "ResultMeta",
    "SCALAR_TAUS", "ScanDescriptor", "Schema", "SCHEMA_NAME", "SCHEMA_VERSION",
    "ScopeBasis", "ScopeTerm", "Sigma", "sigma_for", "SortKey", "Source", "store_identity",
    "StartsWith", "store_identity_of", "STORE_READING_CORE", "Targets", "Tau", "TOP",
    "TOP_TERM", "TtQ", "TupleExpr", "TypeConstraint", "TypedProp", "UNANCHORED", "Unbounded", "union_all", "VidSet",
    "vt_carve", "vt_closed", "vt_from", "VT_MODES"
]
