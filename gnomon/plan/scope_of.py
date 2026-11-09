"""`scope_of` — the compositionality interface.

```
scope_of : (op, bound_args, Σ, [input_scope]) → DependencyScope
scope_of(op, args, Σ, ins)  =  leaf_scope(op, args, Σ, out_schema)  ⊎  ⊎ ins
```

Union is list concatenation; normalization is optional and always
widening, and this implementation skips it. Three prohibitions carried verbatim

1. **Every node's scope enters the union — including nodes whose rows never
   reach the answer.** This is not merely about coverage: it is load-bearing for
   the soundness of `Expand{exact(1)}`'s narrowing, which is sound
   *because* the seed-supplying node's scope is unioned in. **A seeds-only input
   is never scope-elided.**
2. **A node's scope is derived from its recorded bound args**, never from
   hypothetical or re-resolved ones.
3. **A node that failed or was refused still contributes its scope** — a
   correction can make it succeed. This is the opposite of how `completeness`
   behaves, and it is deliberate.

fifteen opaque leaves delegate to `leaves.terms_for`, which is the same
derivation the live envelope uses — so a plan node and a call can never
disagree about one operator's scope. Operators without a derivation take the
everywhere is a valid v1 answer for any operator whose derivation is not yet
every later narrowing a strict improvement rather than a compatibility event.
"""

from __future__ import annotations

from dataclasses import dataclass

from gnomon.plan.anchor import anchor_of_var, anchors_of
from gnomon.plan.depscope import (
    FULL_SCAN_CHECKPOINTS, K_DENSE_ID, K_EDGE, K_NODE, TOP, Checkpoint,
    DependencyScope, Incident, ScopeTerm, Targets, _Top,
)
from gnomon.plan.node import (
    Bounded, EdgeScan, Exact, Expand, Node, NodeScan, OpaqueLeaf, PatternMatch,
)


@dataclass(frozen=True, slots=True)
class ScopeBasis:
    """    the store identity, the belief frontier the read was served from, and the
    log cursor. `scope_of` is plan-time; the basis is stamped per read."""

    store: str
    tt_q: int
    pinned: bool = False
    clamped: bool = False
    checkpoints: tuple[Checkpoint, ...] = FULL_SCAN_CHECKPOINTS
    tt_q_verified: bool = True

    def scope(self, *terms: ScopeTerm) -> DependencyScope:
        return DependencyScope(self.store, self.tt_q, tuple(terms), self.checkpoints,
                               self.pinned, self.clamped,
                               tt_q_verified=self.tt_q_verified)

    def empty_scope(self) -> DependencyScope:
        """∅ — `terms: []`, over this same basis."""
        return self.scope()


def _union_kinds(*groups: tuple[str, ...]) -> tuple[str, ...]:
    out: list[str] = []
    for g in groups:
        for k in g:
            if k not in out:
                out.append(k)
    return tuple(out)


def leaf_scope(node: Node, basis: ScopeBasis) -> DependencyScope:
    """The node's own scope, before its inputs' are unioned in.

    `∅` for every node that is a pure function of its inputs and bind-time
    arguments — the IR's obligation-6 classification, which is `Node.reads_store`.
    """
    if isinstance(node, NodeScan):
        #: `assert_edge`/`ingest_events` register a dense id without
        # writing a node version and thereby flip the consumer's *outcome*, but
        # both write **edge** footprints, which route to the edges/incident arms
        # only — so a term naming a `nodes` arm alone admits `𝒟` in its first
        # conjunct and can never satisfy its second.
        targets: _Top | Targets = TOP
        if node.uids:
            uids = tuple(node.uids)
            targets = Targets(nodes=uids, incident=Incident("either", uids))
        # row, so `@recut` is unconditional and there is no Level-0 escape by
        # projection.
        return basis.scope(ScopeTerm(
            kinds=_union_kinds(K_NODE, K_DENSE_ID), targets=targets, rel_types=TOP,
            vt=tuple((i.start, i.end) for i in node.sigma.t_v),
            vt_mode=node.vt_mode, props=TOP))

    if isinstance(node, EdgeScan):
        ep = node.endpoints
        targets = (Targets(incident=Incident(ep.role, tuple(ep.uids))) if ep is not None
                   else Targets(edges=TOP))
        return basis.scope(ScopeTerm(
            kinds=K_EDGE, targets=targets,
            rel_types=tuple(node.rel_types) if node.rel_types else TOP,
            vt=tuple((i.start, i.end) for i in node.sigma.t_v),
            vt_mode=node.vt_mode, props=TOP))

    if isinstance(node, Expand):
        return basis.scope(_expand_term(node))

    if isinstance(node, PatternMatch):
        rel_types: tuple[str, ...] | _Top = TOP
        declared = [e.rel_type for e in node.pattern.edge_pats]
        if all(r is not None for r in declared):
            rel_types = tuple(dict.fromkeys(r for r in declared if r is not None))
        # "`\"*\"` unless **every** node variable is anchored to a bound
        # relation". The anchors come from the `PatternMatch`'s own
        # bottom-up map, which resolves each source's identity column.
        node_vars = [p.var for p in node.pattern.node_pats]
        bound = anchors_of(node)
        targets = TOP
        if node_vars and all(f"{v}.uid" in bound for v in node_vars):
            uids: list[str] = []
            for v in node_vars:
                a = bound[f"{v}.uid"]
                if a is TOP:
                    uids = []
                    break
                uids.extend(sorted(a))  # type: ignore[arg-type]
            if uids:
                # comes **in addition to** `nodes: "*"`, never instead of it.
                targets = Targets(nodes=TOP,
                                  incident=Incident("either", tuple(dict.fromkeys(uids))))
        return basis.scope(ScopeTerm(
            kinds=_union_kinds(K_EDGE, K_NODE), targets=targets, rel_types=rel_types,
            vt=tuple((i.start, i.end) for i in node.sigma.t_v),
            vt_mode="overlap", props=TOP))

    if isinstance(node, OpaqueLeaf):
        if not node.reads_store:
            # `compute` — `terms: []`, the empty scope ∅.
            return basis.empty_scope()
        # the same derivation the live envelope uses (`ttq.dependency_of`), so
        # a plan node and a call can never disagree about one operator's scope
        from gnomon.plan.leaves import terms_for
        return basis.scope(*terms_for(node.op, node.args, node.sigma))

    # `Filter`, `PropertyPredicate`, `TypeConstraint`, `Project`, `Join`,
    # `Aggregate`, `Order`, `Limit` — `∅`. A filter narrows the
    # declared *domain* and never the scope, and the rule is
    # self-enforcing: every selection operator is `∅`-scoped and scopes only
    # ever union, so no code path exists in which a predicate could narrow one.
    return basis.empty_scope()


def _expand_term(node: Expand) -> ScopeTerm:
    """
    `"*"` at every *multi-hop* bound: a set of new edges can form an entirely
    new path from a seed, so no static endpoint set is sound for `exact(k≥2)`,
    `bounded` or `unbounded` — the concrete cost of R6, and the highest-value
    Level-1 refinement target.
    """
    vt = tuple((i.start, i.end) for i in node.sigma.t_v)
    kinds = _union_kinds(K_EDGE, K_NODE)
    rel_types: tuple[str, ...] | _Top = (node.rel_type,) if node.rel_type else TOP
    anchor = anchor_of_var(node.input, node.from_)
    targets: _Top | Targets = TOP

    if isinstance(node.hops, Exact) and anchor is not TOP:
        seeds = tuple(sorted(anchor))  # type: ignore[arg-type]
        if node.hops.k == 0:
            # A *node* arm, since it traverses no edge. The `𝒟` arm
            # exemption: a core scan has no unknown-uid outcome to flip.
            targets = Targets(nodes=seeds)
        elif node.hops.k == 1:
            # binds `into`'s node columns, so a node write on a *current*
            # neighbour of a seed changes an output row — and node footprints
            # test against the `nodes` arm only, which is why it stays `"*"`.
            role = {"out": "src", "in": "dst", "both": "either"}[node.dir]
            targets = Targets(nodes=TOP, incident=Incident(role, seeds))
    if isinstance(node.hops, Bounded) and node.hops.a == node.hops.b == 0:
        # `bounded(0, 0)` still traverses no edge, but it is a *node* relation
        # and is deliberately not normalized to `exact(0)`; its targets
        # stay `"*"` rather than being narrowed by the `exact(0)` rule.
        targets = TOP
    return ScopeTerm(kinds=kinds, targets=targets, rel_types=rel_types, vt=vt,
                     vt_mode="overlap", props=TOP)


def scope_of(node: Node, basis: ScopeBasis,
             input_scopes: tuple[DependencyScope, ...] | None = None) -> DependencyScope:
    """`leaf_scope(node) ⊎ ⊎ ins`.

    `input_scopes` defaults to recursing over `node.inputs`, which is the
    walker does when it has already computed each step's scope — and it must
    pass **every** input's, including seeds-only ones (prohibition 1).
    """
    ins = (input_scopes if input_scopes is not None
           else tuple(scope_of(i, basis) for i in node.inputs))
    out = leaf_scope(node, basis)
    for s in ins:
        out = out.union(s)
    return out


__all__ = ["ScopeBasis", "leaf_scope", "scope_of"]
