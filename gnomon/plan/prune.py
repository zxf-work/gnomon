"""Plan-time column pruning — `live_columns(root) → {node_digest: {column}}`.

The two scans emit eight and eleven columns, most of which most
plans never read — and two families are the expensive ones the storage layer's
projection pushdown exists to avoid: `props` is the whole canonical-JSON blob
per row, and node `props` have no columnar route at all.

So the evaluator computes, **once per plan before execution**, which columns each node's
output is actually read for, propagating backwards from the root. A scan
materializes exactly its live set.

**This is not an optimization the IR forbids.** It forbids *plan rewriting*
— fusing operators, pushing predicates into scan parameters, hoisting a `Limit`
— because a rewrite changes a node's recorded bound args and therefore its
dependency scope, and because filters narrow the declared domain and never
the scope. Pruning changes no
node's arguments, no row and no order: only which arrays get built. The node
digests, the scopes and the results are identical with it and without it.

A lazily-resolving `Relation` was considered and rejected: it puts store access
behind a property accessor, which is exactly what the IR's `∅`-classification
guard exists to make impossible to do by accident.
"""

from __future__ import annotations

from gnomon.plan.node import (
    Aggregate, Collapse, CoValidExpand, CoValidJoin, EdgeScan, Expand, Filter, Join, Limit,
    Node, NodeScan, Order,
    OrderedReach, PatternMatch, Project, PropertyPredicate, Succ, TopK, TypeConstraint,
    Walk,
)

#: Columns a node needs from its own inputs, beyond what its consumers ask for.
LiveMap = dict[str, frozenset[str]]


def live_columns(root: Node) -> LiveMap:
    """Live columns per node, keyed by `node_digest`.

    Walks top-down: the root is live in its whole output schema, and each node
    adds what its own arguments read before passing the demand to its inputs. A
    DAG node reached twice accumulates the union of both demands, which is why
    the walk revisits rather than memoizing on first sight.
    """
    live: dict[str, set[str]] = {}
    _walk(root, set(root.out_schema.names), live)
    return {digest: frozenset(names) for digest, names in live.items()}


def _walk(node: Node, demanded: set[str], live: dict[str, set[str]]) -> None:
    key = node.node_digest
    before = live.get(key)
    wanted = set(demanded) & set(node.out_schema.names)
    if before is not None and wanted <= before:
        return                      # this subtree has already seen this demand
    live.setdefault(key, set()).update(wanted)

    own = _own_reads(node)
    for position, i in enumerate(node.inputs):
        available = set(i.out_schema.names)
        # what this node passes through, plus what it reads itself, plus what
        # it reads *from this particular input* — `PatternMatch` is the one
        # node whose demand is per-input, because `sources` rebinds and each
        # source relation carries the identity columns under its own prefix
        demand = (live[key] | own | _input_reads(node, position)) & available
        _walk(i, demand, live)


def _input_reads(node: Node, position: int) -> set[str]:
    """What a node reads from **one specific** input.

    Only `PatternMatch` has such a thing: its `sources` are rebindings, so the
    columns it needs live under whatever prefix the source relation happens to
    use, and the search binds on the identity columns whether or not the plan
    projects them.
    """
    if not isinstance(node, PatternMatch) or position >= len(node.sources):
        return set()
    source = node.sources[position]
    is_edge = any(p.var == source.var for p in node.pattern.edge_pats)
    names = source.relation.out_schema.names
    if is_edge:
        prefix = next((n[: -len("eid")] for n in names if n.endswith(".eid")), None)
        if prefix is None:
            return set()
        return {f"{prefix}{c}" for c in ("eid", "src", "dst", "vt_s", "vid")}
    prefix = next((n[: -len("uid")] for n in names if n.endswith(".uid")), None)
    return {f"{prefix}uid"} if prefix else set()


def _own_reads(node: Node) -> set[str]:
    """The columns a node's own arguments read, independent of its consumers."""
    out: set[str] = set()
    if isinstance(node, Filter):
        out |= set(node.pred.columns())
    elif isinstance(node, PropertyPredicate):
        out.add(f"{node.var}.props")
    elif isinstance(node, TypeConstraint):
        out.add(f"{node.var}." + ("label" if node.labels is not None else "rel_type"))
    elif isinstance(node, Project):
        for _name, expr in node.bindings:
            out |= set(expr.columns())
    elif isinstance(node, Order):
        for key in node.keys:
            out |= set(key.key.columns())
    elif isinstance(node, Join):
        out |= {lhs for lhs, _ in node.on} | {rhs for _, rhs in node.on}
    elif isinstance(node, Aggregate):
        for _name, expr in node.group_by:
            out |= set(expr.columns())
        for agg in node.aggregates:
            if agg.of is not None:
                out |= set(agg.of.columns())
            for key in agg.order:
                out |= set(key.key.columns())
    elif isinstance(node, (Expand, Walk)):
        out.add(node.from_column)
    elif isinstance(node, Succ):
        out.add(f"{node.of}.vid")
    elif isinstance(node, OrderedReach):
        out |= {node.from_column, node.src, node.dst, node.time}
        if node.key is not None:
            out.add(node.key)
    elif isinstance(node, CoValidExpand):
        out.add(node.from_column)
        if node.from_iv is not None:
            out |= set(node.from_iv)
    elif isinstance(node, CoValidJoin):
        out |= {lhs for lhs, _ in node.on} | {rhs for _, rhs in node.on}
        out |= set(node.left_iv) | set(node.right_iv)
    elif isinstance(node, Collapse):
        out |= set(node.by) | set(node.mins)
    elif isinstance(node, TopK):
        for e in node.partition_by:
            out |= set(e.columns())
        for key in node.keys:
            out |= set(key.key.columns())
    elif isinstance(node, (NodeScan, EdgeScan, Limit, PatternMatch)):
        pass                        # no argument of these reads an input column
    return out


__all__ = ["LiveMap", "live_columns"]
