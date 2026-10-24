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
  defaults to 100 and is capped at 10,000 (`match` pages hold up to
  1,000,000 rows, and its `uids` lists up to 1,000,000 seeds).
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
| `aggregate_events` | grouped aggregation over edge events in a window: group by time bucket, calendar unit, relation type, endpoint or label; count, count-distinct, min, max, mean, and the gap and session statistics (`sum` is an engine-level aggregate, below) | `group_by`*, `aggregates`*, `window`*, `stride`, `rel_types`, `prop_filter`, `endpoint_filter`, `pair_mode`, `as_of_tt` | VT window, TT |
| `burst_detection` | buckets of an event-rate series flagged as bursts (z-score against a trailing mean, or ratio against a trailing median) | `target`*, `window`*, `stride`*, `method`, `params`, `as_of_tt` | VT window, TT |
| `co_active` | an interval join between two edge-version sets under an Allen relation (`overlaps`, `during`, `meets`, `before`) | `a_spec`*, `b_spec`*, `allen_relation`*, `as_of_tt` | VT intervals, TT |
| `count_temporal_motifs` | the exact number of δ-temporal motif instances among edge events in a window | `motif`*, `delta`*, `window`*, `node_filter`, `as_of_tt` | VT window, TT |
| `find_temporal_motif_instances` | the motif instances themselves, as ordered edge-event tuples | `motif`*, `delta`*, `window`*, `node_filter`, `as_of_tt` | VT window, TT |
| `resolve_entities` | node uids matching a query: exact uid first, then case-insensitive substring on uid and on the `name` property of any believed version | `query`*, `label`, `as_of_tt` | TT |
| `compute` | deterministic arithmetic over rows a previous call returned: count/sum/min/max/mean/median, top-k or top-percent, filter, joins, grouped reductions | `fn`*, `input`, `field`, ... | none (operates on given rows) |

The full argument schema of each operator, with descriptions, is in the
registry: `gnomon.temporal.algebra.REGISTRY[name].args_schema` and
`.description`.

## Engine-level operators and arguments

The fifteen operators above are the **base operator set**
(`gnomon.temporal.algebra.BASE_OPERATORS`), and `args_schema` is their
**base schema**. The registry also holds four engine-level operators, and
some base operators accept engine-level arguments; both are validated
against a wider **engine schema** (`REGISTRY[name].engine_schema`).
`call_operator` uses the engine schema by default; a caller that should see
only the base set passes `base_schema=True`, and then an engine-level
operator or argument is refused exactly as an unknown one. An engine-level
argument has no default, so a call that does not use it is filled, echoed
and digested exactly as before it existed.

| operator | answers | key parameters (* required) | clocks |
|---|---|---|---|
| `match` | typed pattern matching: node variables (by label or uid), steps of one hop, exactly `k` hops, a bounded or unbounded closure, or a regular path expression over relation types; predicates, `optional` and `not` sub-patterns, lookups and unions; then a projection or one or two aggregation stages, an order and a limit, fused into one plan | `pattern`, `return`*, `scope`* (`vt`: an `instant` or a `window`; `as_of_tt`; `belief` = `current` / `superseded` / `all`; `co_valid`), `params`, `limit` | VT instant or window, TT |
| `paths` | paths between sources and targets: hop or weight distances (`metric`), one or all optimal paths, the `k` best or all simple paths (`result`), optionally time-respecting (`non_strict` / `strict`); sources, targets and weights from uid lists, properties or inline `match` patterns; per-vertex truncation; a window form (`valid_window`) in which a path is valid at one shared instant | `sources`*, `targets`, `t_valid` or `valid_window`, `window`, `metric`, `result`, `k`, `min_hops`, `max_hops`, `rel_types`, `edge_where`, `truncate`, `as_of_tt` | VT instant or window, TT |
| `state_series` | an aggregate (`count`, `sum`, `min`, `max`, `mean`, optionally of a property, optionally grouped) over the node or edge versions alive in each bucket of a window: valid at the bucket's start or time-weighted over it (`axis: "valid"`), or believed at each bucket of a transaction-time window about one valid instant (`axis: "transaction"`) | `kind`*, `window`*, `stride`*, `agg`*, `axis`, `at`, `t_valid`, `labels`, `rel_types`, `where`, `group_by`, `as_of_tt` | VT window or TT window |
| `diff_belief` | the belief diff: every node and edge version whose belief state changed between transaction times `t1` and `t2`, inside a valid-time window. A version is `added` when its `tt_s` falls in `(t1, t2]` and `retracted` when its effective `tt_e` does; `added_total` and `retracted_total` count the whole population | `t1`*, `t2`*, `window`* | two TT points, VT window |

Engine-level arguments of base operators:

| operator | argument | effect |
|---|---|---|
| `aggregate_events` | `{"agg": "sum", "of": ...}` | an **exact** sum over `vt_s`, `duration` or a numeric property: an integer total over integers, and a correctly rounded total (`math.fsum`) once a float takes part, so no summation order moves it |
| `aggregate_events` | `valid_at` | keep only the events whose version is valid at that instant |
| `version_history` | `ref`, `include_props` | the belief log of **one** node (`{"uid"}`) or edge (`{"eid"}` or `{"src", "dst", "rel_type", "disc"}`): every version ever written, with both clocks, optionally with its props; versions closed by a bare retraction are included |
| `entity_history` | `t_valid` or `window` | only the believed versions valid at the instant or overlapping the window, found by a windowed point read instead of the whole history |

A `match` call, for example, finds whom `alice` reaches over one or two
`KNOWS` edges at valid time 500:

```json
{"pattern": {"nodes": [{"var": "a", "uids": ["alice"]}, {"var": "b"}],
             "steps": [{"from": "a", "to": "b", "rel_type": "KNOWS",
                        "hops": {"min": 1, "max": 2}}]},
 "return": {"rows": [{"as": "who", "expr": {"col": "b.uid"}}],
            "order_by": [{"expr": {"col": "who"}}]},
 "scope": {"vt": {"instant": 500}}}
```

Under a valid instant with current belief, a node variable binds the one
version valid at the instant, and the pattern is matched in the snapshot
graph at that instant. Under a window (with `co_valid: ["valid"]`), or under
a belief mode other than `current`, variables bind **versions**, and walks
and sub-patterns obey the **shared-instant rule**: every node and edge of a
binding, walks included, must be valid at one common instant, and the
answer is the union over those instants, one row per version tuple. No two
single-hop edges of one pattern bind the same edge identity; a bounded
closure `{min: a, max: b}` keeps each target once, at its shortest
distance. The order is completed with every output column and bound version
id, so it is total and a `limit` cuts it the same way on every executor.

## Engine calls outside the registry

One engine capability is exposed on the native adapter rather than as a
registered operator, and the benchmark harness calls it directly:

| call | answers | clocks |
|---|---|---|
| `adapter.lookup_name(query, mode, as_of_tt=OPEN_END, limit=None)` | nodes whose uid equals `query`, or whose `name` property equals (`mode="exact"`) or starts with (`mode="prefix"`) it, case-insensitively, through the per-segment name index | TT |

See `scripts/eval_harness.py` (`_lookup_name_dispatch`) for its exact
argument shape. The adapter also keeps `adapter.diff_belief(t1, t2, t_a,
t_b, limit)`, the engine call behind the `diff_belief` operator, which the
harness calls directly as well (`_diff_belief_dispatch`).

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
