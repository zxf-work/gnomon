# Operators

Every question is asked through one entry point:

```python
from gnomon.temporal.algebra import call_operator, ensure_all_registered

ensure_all_registered()                       # import every operator module once
env = call_operator(store.adapter, "entity_history", {"uid": "alice"})
```

or from the shell, `gnomon call --store <path> <operator> '<json args>'`.
Arguments are validated against a JSON Schema before anything runs (unknown
keys are rejected, defaults are filled in and echoed back in `args_echo`).
A cost guardrail estimates the work from the store's statistics and refuses
calls above a ceiling with a structured error (`skip_cost_check=True`
bypasses it).

## Conventions

* **Times** are `int64` microseconds since the epoch, UTC.
* **`window`** is a half-open valid-time interval `{"t_a": ..., "t_b": ...}`.
* **`as_of_tt`** selects the belief state (transaction time); the default
  `OPEN_END` (`2**62`) means current beliefs. It is accepted by every
  operator except `compute`.
* **Pagination:** list results carry `rows`, `rows_total`, `truncated` and an
  opaque `cursor`; pass the cursor back to get the next page. `limit`
  defaults to 100 and is capped at 10,000.
* **Determinism:** the same store state and arguments give a byte-identical
  canonical result and `result_digest`.

## Registered operators

The "clocks" column says which time dimensions an operator's arguments
range over: **VT** = valid time (what was true in the world, via `window`,
`t_valid`, `t1`/`t2`), **TT** = belief time (via `as_of_tt`).

| operator | answers | key parameters (* required) | clocks |
|---|---|---|---|
| `entity_history` | all versions of one node (optionally with incident edge versions), ordered by `vt_s`, as believed at `as_of_tt` | `uid`*, `include_edges`, `as_of_tt` | TT; returns VT intervals |
| `version_history` | the belief log itself: node or edge version rows whose valid interval overlaps `window`, with both clocks on every row, ordered by transaction time; `belief` = `current` / `superseded` / `all` as of `as_of_tt` | `kind`*, `window`*, `belief`, `rel_types`, `as_of_tt` | VT window, TT selection; returns both |
| `snapshot_subgraph` | the k-hop subgraph (undirected expansion, induced edges) around seed nodes in the snapshot graph at valid time `t_valid` | `seeds`*, `t_valid`*, `hops`, `rel_types`, `as_of_tt` | VT instant, TT |
| `diff_snapshots` | nodes and edges added or removed, and properties changed, between the snapshots at `t1` and `t2` under one belief state | `t1`*, `t2`*, `scope`, `as_of_tt` | VT pair, TT |
| `neighborhood_evolution` | neighbours of a node gained and lost between `t1` and `t2`, plus a degree series | `uid`*, `t1`*, `t2`*, `stride`, `as_of_tt` | VT pair, TT |
| `temporal_paths` | up to `k` node-simple time-respecting paths between two nodes inside a window, ordered by arrival, hops, edge sequence | `src`*, `dst`*, `window`*, `k`, `max_hops`, `as_of_tt` | VT window, TT |
| `temporal_reachability` | earliest arrival time at every node reachable from (or, with `direction: "in"`, able to reach) a source via time-respecting paths | `src`*, `window`*, `delta_max_wait`, `direction`, `as_of_tt` | VT window, TT |
| `graph_metric_timeseries` | a per-bucket metric series over a window (instant metrics sampled at bucket starts, event metrics aggregated within buckets) | `metric`*, `window`*, `stride`*, `as_of_tt` | VT window, TT |
| `aggregate_events` | grouped aggregation over edge events in a window: group by time bucket, calendar unit, relation type, endpoint or label; count, count-distinct, sum, min, max, mean | `group_by`*, `aggregates`*, `window`*, `stride`, `rel_types`, `prop_filter`, `endpoint_filter`, `pair_mode`, `as_of_tt` | VT window, TT |
| `burst_detection` | buckets of an event-rate series flagged as bursts (z-score against a trailing mean, or ratio against a trailing median) | `target`*, `window`*, `stride`*, `method`, `params`, `as_of_tt` | VT window, TT |
| `co_active` | an interval join between two edge-version sets under an Allen relation (`overlaps`, `during`, `meets`, `before`) | `a_spec`*, `b_spec`*, `allen_relation`*, `as_of_tt` | VT intervals, TT |
| `count_temporal_motifs` | the exact number of δ-temporal motif instances among edge events in a window | `motif`*, `delta`*, `window`*, `node_filter`, `as_of_tt` | VT window, TT |
| `find_temporal_motif_instances` | the motif instances themselves, as ordered edge-event tuples | `motif`*, `delta`*, `window`*, `node_filter`, `as_of_tt` | VT window, TT |
| `resolve_entities` | node uids matching a query: exact uid first, then case-insensitive substring on uid and on the `name` property of any believed version | `query`*, `label`, `as_of_tt` | TT |
| `compute` | deterministic arithmetic over rows a previous call returned: count/sum/min/max/mean/median, top-k or top-percent, filter, joins, grouped reductions | `fn`*, `input`, `field`, ... | none (operates on given rows) |

The full argument schema of each operator, with descriptions, is in the
registry: `gnomon.temporal.algebra.REGISTRY[name].args_schema` and
`.description`.

## Engine calls outside the registry

Two engine capabilities are exposed on the native adapter rather than as
registered operators, and the benchmark harness calls them directly:

| call | answers | clocks |
|---|---|---|
| `adapter.lookup_name(query, mode, as_of_tt=OPEN_END, limit=None)` | nodes whose uid equals `query`, or whose `name` property equals (`mode="exact"`) or starts with (`mode="prefix"`) it, case-insensitively, through the per-segment name index | TT |
| `adapter.diff_belief(t1, t2, t_a, t_b, limit)` | every version whose belief state changed between transaction times `t1` and `t2`, inside the valid-time window `[t_a, t_b)`; cost `O(segments × (runs + closes))`, independent of the store size | two TT points, VT window |

See `scripts/eval_harness.py` (`_lookup_name_dispatch`,
`_diff_belief_dispatch`) for their exact argument shapes.

## The result envelope

```json
{
  "op": "entity_history",
  "args_echo": {"uid": "alice", "as_of_tt": 4611686018427387904, "...": "..."},
  "dataset_extent": {"vt_min": 0, "vt_max": 500},
  "rows": ["..."], "rows_total": 2, "truncated": false, "cursor": null,
  "result_digest": "3f1c..."
}
```

`result_digest` covers the payload only (not `op`, `args_echo` or
`dataset_extent`), so a result pinned to a past `as_of_tt` keeps its digest
under later writes.
