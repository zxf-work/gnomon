"""`Union`: the bag union of relations with one column list."""

from __future__ import annotations

import numpy as np

from gnomon.plan.node import Union
from gnomon.plan.relation import Relation, _py
from gnomon.plan.types import Schema


def eval_union(node: Union, inputs: tuple[Relation, ...]) -> Relation:
    """Each input's rows in input order. A column whose inputs share one
    numpy dtype concatenates as is; otherwise it is an object column of the
    Python values, floats made floats where the unified type is `float`."""
    # the declared schema, restricted to the columns every input realized
    # (column pruning drops the same ones from every branch)
    schema = Schema(tuple(c for c in node.out_schema
                          if all(c.name in r.cols for r in inputs)))
    n = sum(r.n for r in inputs)
    cols: dict[str, np.ndarray] = {}
    nulls: dict[str, np.ndarray] = {}
    for c in schema:
        arrays = [r.cols[c.name] for r in inputs]
        kinds = {a.dtype for a in arrays}
        if len(kinds) == 1 and next(iter(kinds)).kind != "O":
            col = np.concatenate(arrays)
        elif all(a.dtype.kind in "if" for a in arrays):
            col = np.concatenate([a.astype(np.float64) for a in arrays])
        else:
            values: list = []
            for r, a in zip(inputs, arrays):
                mask = r.nulls.get(c.name)
                for i in range(r.n):
                    v = None if (mask is not None and mask[i]) else _py(a[i])
                    if v is not None and c.tau.base == "float" and not isinstance(v, bool):
                        v = float(v)
                    values.append(v)
            col = np.empty(n, dtype=object)
            col[:] = values
        cols[c.name] = col
        if any(c.name in r.nulls for r in inputs):
            nulls[c.name] = np.concatenate([r.is_null(c.name) for r in inputs])
    return Relation.of(schema, cols, n, nulls)


__all__ = ["eval_union"]
