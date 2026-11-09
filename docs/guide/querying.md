# Querying

Every question is an **operator** call. This page walks through the
operator families with runnable examples. The [operators](../operators.md)
reference lists every operator and parameter.

- [Calling an operator](#calling-an-operator)
- [Snapshots](#snapshots)
- [Paths and reachability](#paths-and-reachability)
- [History and belief](#history-and-belief)
- [Counts, series and windows](#counts-series-and-windows)
- [Name search](#name-search)
- [Patterns](#patterns)
- [Follow-up computation](#follow-up-computation)
- [Pagination](#pagination)
- [The cost check](#the-cost-check)

## Calling an operator

```python
from gnomon.temporal.algebra import call_operator, ensure_all_registered

ensure_all_registered()       # once per process
env = call_operator(store.adapter, "entity_history", {"uid": "alice"})
```

or from the shell:

```bash
gnomon call --store mystore entity_history '{"uid": "alice"}'
```

* Arguments are checked against a JSON Schema first. Unknown keys and
  wrong types are refused with `E_SCHEMA`; defaults are filled in and
  echoed in `args_echo`.
* Every operator except `compute` and `diff_belief` takes `as_of_tt`
  (default `OPEN_END`, the current beliefs). `match` takes it inside
  `scope`. See [what "as of" means](concepts.md#what-as-of-means).
* `window` is a half-open valid-time interval `{"t_a": ..., "t_b": ...}`.
* The registry describes every operator:

  ```python
  from gnomon.temporal.algebra import REGISTRY
  print(REGISTRY["snapshot_subgraph"].description)
  print(REGISTRY["snapshot_subgraph"].engine_schema["properties"].keys())
  ```

The examples below define `call = lambda op, args: call_operator(store.adapter, op, args)`
to keep the code short.

## Snapshots

Example: [`examples/query_snapshots.py`](../../examples/query_snapshots.py).
Five people work together over valid time 0 to 1000. After loading, the
store learns that ben stopped working with cal at 500 (a retraction) and
that dan became a lead at 700 (a new node version).

**`snapshot_subgraph`**: the graph at valid instant `t_valid`, `hops` steps
around `seeds` (expansion ignores edge direction; edges between reached
nodes are included). `nodes` carry their hop distance; `rows` are the
edges.

```python
call("snapshot_subgraph", {"seeds": ["ann"], "t_valid": 650, "hops": 3,
                           "as_of_tt": as_of})
```

**`diff_snapshots`**: nodes and edges added and removed, and properties
changed, between the snapshots at `t1` and `t2`. `scope: {"seeds", "hops"}`
restricts it to a neighbourhood.

```python
call("diff_snapshots", {"t1": 300, "t2": 900})
```

**`neighborhood_evolution`**: one node's neighbours gained and lost between
`t1` and `t2`, and its degree sampled every `stride`.

```python
call("neighborhood_evolution", {"uid": "cal", "t1": 300, "t2": 900, "stride": 150})
```

**`entity_history`** with `t_valid` or `window` returns only the versions
valid at that instant or overlapping that window.

<!-- example-output: query_snapshots.py -->
```text
snapshot at 650, now:
   nodes (uid, hop): [('ann', 0), ('ben', 1), ('eve', 1)]
   edges: [('ann', 'ben'), ('ann', 'eve')]
snapshot at 650, as loaded:
   nodes (uid, hop): [('ann', 0), ('ben', 1), ('eve', 1), ('cal', 2), ('dan', 3)]
   edges: [('ann', 'ben'), ('ann', 'eve'), ('ben', 'cal'), ('cal', 'dan')]
diff 300 -> 900:
   edges added:   [('ann', 'eve'), ('cal', 'dan')]
   edges removed: [('ben', 'cal')]
   props changed: [('dan', {'name': 'Dan', 'role': 'dev'}, {'name': 'Dan', 'role': 'lead'})]
cal, 300 -> 900: gained ['dan'] lost ['ben']
   degree series: [(300, 1), (450, 2), (600, 1), (750, 1)]
dan's versions overlapping [650, 750): [(0, 'dev'), (700, 'lead')]
```

At 650, ann's neighbourhood now stops at ben and eve, because the ben–cal
edge ended at 500. As loaded, before the retraction, the same snapshot
reached cal and dan through it.

## Paths and reachability

Example: [`examples/query_paths.py`](../../examples/query_paths.py). It
loads messages (instant events) and a road graph whose edges carry a
length `km`; the road C–D is valid only until 500.

**`temporal_reachability`**: the earliest arrival time at every node
reachable from `src` along **time-respecting** paths inside `window`: each
step starts no earlier than the previous one arrived.

```python
call("temporal_reachability", {"src": "ann", "window": {"t_a": 0, "t_b": 100}})
```

`direction: "in"` follows edges against their stored direction, still with
times that do not decrease along the walk; `"both"` ignores direction.
`delta_max_wait` bounds the wait at each intermediate node.

**`temporal_paths`**: up to `k` node-simple time-respecting paths from
`src` to `dst`, ordered by arrival, then hops (`k` up to 20, `max_hops` up
to 6).

```python
call("temporal_paths", {"src": "ann", "dst": "dan", "window": window, "k": 3})
```

**`paths`** (engine-level): paths in the snapshot graph at `t_valid`, or,
with `valid_window`, paths valid at one shared instant of a window.

| argument | values |
|---|---|
| `metric` | `hops` (default), `weight` (with `weight: {"prop": key}`), `arrival` |
| `result` | `distance` (default), `one_path`, `all_optimal`, `k_paths` (needs `k` and `max_hops`), `all_paths` (needs `max_hops`) |
| `sources`, `targets` | `{"uids": [...]}` or `{"match": <a match argument>}` |
| others | `rel_types`, `dir` (`out`, `in`, `both`), `min_hops`, `max_hops`, `edge_where`, `edge_window`, `time_respecting`, `truncate`, `pairs`, `order` |

```python
call("paths", {"sources": {"uids": ["A"]}, "targets": {"uids": ["D"]},
               "t_valid": 100, "metric": "weight", "weight": {"prop": "km"},
               "result": "one_path", "rel_types": ["ROAD"]})
```

<!-- example-output: query_paths.py -->
```text
reached from ann: [('ben', 10), ('cal', 20), ('dan', 40), ('eve', 50)]
reached from ann, from 25 on: [('cal', 30), ('dan', 40), ('eve', 50)]
ann -> dan, time-respecting:
   arrival 40, 2 hops: ann>cal@30 cal>dan@40
   arrival 40, 3 hops: ann>ben@10 ben>cal@20 cal>dan@40
after retracting ann>cal@30: 1 path(s)
as believed before:           2 path(s)
hops A -> D at 100: [('D', 2)]
shortest km A -> D at 100: [(11, ['A', 'B', 'C', 'D'])]
shortest km A -> D at 600: [(35, ['A', 'B', 'D'])]
3 shortest A -> D by hops at 100: [['A', 'B', 'D'], ['A', 'C', 'D'], ['A', 'B', 'C', 'D']]
```

* The message `cal>dan@15` never appears: it comes before anyone could
  have reached cal.
* After the message `ann>cal@30` is retracted, one path is left; asked as
  of the earlier belief time, both paths come back.
* At valid time 600 the road C–D no longer exists, so the shortest route
  changes.

## History and belief

Example: [`examples/query_history.py`](../../examples/query_history.py). It
replays the worked example of the [data model](../data-model.md#asking-questions-current-vs-as-of):
an edge written at `T1`, corrected at `T2`, retracted at `T3`.

**`entity_history`**: all versions of one node believed at `as_of_tt`,
ordered by `vt_s`. `include_edges: true` adds its incident edge versions.

**`version_history`**: the belief log, with both clocks on every row.

* `kind` (`node` or `edge`) and `window` are required.
* `belief`: `current` (still believed at `as_of_tt`), `superseded` (revised
  by then), or `all`. The number of corrections made is the number of
  `superseded` rows.
* `ref` restricts it to one identity: `{"uid"}` for a node, `{"src", "dst",
  "rel_type", "disc"?}` or `{"eid"}` for an edge. With `ref`,
  `include_props: true` adds each row's properties.

```python
call("version_history", {"kind": "edge", "window": {"t_a": 0, "t_b": 1000},
                         "ref": {"src": "alice", "dst": "acme", "rel_type": "WORKS_AT"},
                         "belief": "all", "include_props": True})
```

**`diff_belief`** (engine-level): every version whose belief changed
between belief times `t1` and `t2` (`t1 <= t2`), inside a valid-time
window. A version is `added` when its `tt_s` is in `(t1, t2]` and
`retracted` when its `tt_e` is.

```python
call("diff_belief", {"t1": T1, "t2": OPEN_END, "window": {"t_a": 0, "t_b": 1000}})
```

The example prints belief times as `T1`, `T2`, `T3` because their values
change on every run:

<!-- example-output: query_history.py -->
```text
alice's edges now: [('WORKS_AT', 'acme', 100, 300), ('WORKS_AT', 'acme', 300, 400)]
the edge's version rows (vt, role, tt):
   [100, 500)  engineer   [T1, T2)
   [100, 300)  engineer   [T2, open)
   [300, 500)  manager    [T2, T3)
   [300, 400)  manager    [T3, open)
as of T1: {'current': 1, 'superseded': 0, 'all': 1}
as of T2: {'current': 2, 'superseded': 1, 'all': 3}
as of open: {'current': 2, 'superseded': 2, 'all': 4}
believed at T1: [(100, 500, 'engineer')]
believed at T2: [(100, 300, 'engineer'), (300, 500, 'manager')]
believed at T3: [(100, 300, 'engineer'), (300, 400, 'manager')]
since T1: 3 added, 2 retracted
   added     [100, 300)  at T2
   added     [300, 500)  at T2
   retracted [100, 500)  at T2
   added     [300, 400)  at T3
   retracted [300, 500)  at T3
```

Rows with the same belief time come back in an order based on version ids,
which change from load to load; the example sorts them by valid time
before printing.

## Counts, series and windows

Example: [`examples/query_aggregates.py`](../../examples/query_aggregates.py).
Payments (instant events with an `amount`), logins every 10 time units with
a burst at 500, and club memberships with valid intervals. One payment is
corrected from 40 to 400.

| operator | answers | key arguments |
|---|---|---|
| `aggregate_events` | grouped aggregates over edge events with `t_a <= vt_s < t_b` | `group_by` (up to 2 of `time_bucket`, `calendar_unit`, `rel_type`, `endpoint`, `label`), `aggregates` (up to 4 of `count`, `count_distinct`, `min`, `max`, `mean`, `sum`, `max_gap`, `max_in_window`, `max_session_span`), `rel_types`, `prop_filter`, `endpoint_filter`, `valid_at` |
| `graph_metric_timeseries` | one metric per bucket: `node_count`, `edge_event_count`, `active_edge_count`, `mean_out_degree`, `new_node_rate`, `reciprocity` | `metric`, `window`, `stride` |
| `state_series` | `count`, `sum`, `min`, `max` or `mean` over the versions alive in each bucket | `kind`, `window`, `stride`, `agg`, `axis` (`valid` or `transaction`), `at` (`bucket_start` or `time_weighted`), `labels`, `rel_types`, `where`, `group_by` |
| `burst_detection` | buckets of an event-rate series flagged as bursts | `target` (`edge_event_rate` or `node_activity`), `method` (`zscore` or `ratio`), `params` (`w`, `z`, `r`) |
| `count_temporal_motifs`, `find_temporal_motif_instances` | δ-temporal motifs: `M_2node_pingpong`, `M_path_3`, `M_star_out_3`, `M_triangle_acyclic_1`, `M_triangle_cyclic` | `motif`, `delta`, `window`, `node_filter` |
| `co_active` | pairs of edge versions in an Allen relation (`overlaps`, `during`, `meets`, `before` with `gap`) | `a_spec`, `b_spec`, `allen_relation` |

A series has at most 2,000 buckets; a wider `window / stride` is refused
with `E_LIMIT`.

```python
call("aggregate_events", {"group_by": [{"dim": "endpoint", "role": "src"}],
                          "aggregates": [{"agg": "count"},
                                         {"agg": "sum", "of": "prop", "prop": "amount"}],
                          "window": {"t_a": 0, "t_b": 1000}, "rel_types": ["PAYS"]})
call("state_series", {"kind": "edge", "rel_types": ["MEMBER_OF"],
                      "window": {"t_a": 0, "t_b": 1000}, "stride": 250,
                      "agg": {"fn": "count"}})
```

<!-- example-output: query_aggregates.py -->
```text
paid by sender, now: [('ann', 7, 770), ('ben', 2, 15), ('cal', 2, 40), ('dan', 1, 15)]
paid by sender, before the correction: [('ann', 7, 410), ('ben', 2, 15), ('cal', 2, 40), ('dan', 1, 15)]
distinct payers per 250: [(0, 2), (250, 4), (500, 2)]
edge events (all types) per 100: [11, 13, 12, 12, 11, 25, 11, 11, 10, 10]
memberships alive at each bucket start: [(0, 1), (250, 3), (500, 2), (750, 2)]
time-weighted over each 500: [(0, 2), (500, 1.3)]
login bursts (bucket start, events): [(500, 15.0)]
payment cycles within 100: 1
    [('ann', 'cal', 250), ('cal', 'dan', 300), ('dan', 'ann', 320)]
chess memberships that overlap: [('ann', 'ben')]
```

* `sum` is exact: integers sum as integers, and floats are summed with
  correct rounding.
* Result column names follow the aggregate: `count`, `sum_prop_amount`,
  `distinct_src`. Events whose property is missing or of another type are
  left out of a property aggregate and counted in `prop_coercion`.
* `time_weighted` counts each version by the share of the bucket it was
  alive: over `[500, 1000)`, three memberships alive 100, 400 and 150 units
  give 1.3.
* `state_series` with `axis: "transaction"` counts what was **believed** at
  each bucket of a belief-time window, about one valid instant `t_valid`.
  [Scenarios](scenarios.md#a-revision-history-study) uses it.

## Name search

**`resolve_entities`** finds node uids: an exact uid first, then a
case-insensitive substring of the uid or of the `name` property of any
believed version. `label` restricts it.

**`store.adapter.lookup_name(query, mode, as_of_tt=OPEN_END, limit=None)`**
uses the per-segment name index: `mode="exact"` matches the whole `name`
(or uid), `mode="prefix"` its start, both case-insensitively. It returns
tuples `(uid, dense id, label, props as JSON text)`. It is a method of the
native engine's adapter, not a registered operator.

```python
call("resolve_entities", {"query": "lee"})
store.adapter.lookup_name("ann", "prefix")
```

## Patterns

**`match`** (engine-level) matches a typed pattern and returns a
projection or an aggregation. Its argument has three parts:

* `pattern`: node variables (`{"var", "labels"?, "uids"?}`), steps
  (`{"from", "to", "rel_type"?, "var"?, "dir"?, "hops"?}`) and `where`
  predicates. `hops` is `{"min": a, "max": b}` for a bounded walk (`min`
  may be 0). Patterns may also hold `optional` and `not` sub-patterns,
  lookups, and regular path expressions over relation types.
* `return`: `rows` (a projection) or `group` (`by` and `aggs`), with
  `order_by` and `limit`.
* `scope`: `vt` is `{"instant": t}` or `{"window": {...}}`, plus
  `as_of_tt`, `belief` (`current`, `superseded`, `all`) and `co_valid`.
  With a window, `"co_valid": ["valid"]` asks that every node and edge of
  a match be valid at one shared instant; the examples use it with every
  window.

Expressions are JSON: `{"col": "p.uid"}` (a column of a variable: `uid`,
`label`, `vt_s`, `vt_e`, `props`, and for edges `src`, `dst`, `rel_type`),
`{"prop": ["p", "city"]}` (a property), `{"tprop": ["o", "amount"], "tau":
"int"}` (a typed property, for arithmetic), `{"lit": "Lyon"}`, and
`{"cmp": "=", "l": ..., "r": ...}` with `=`, `!=`, `<`, `<=`, `>`, `>=`.
The [operators](../operators.md#engine-level-operators-and-arguments) page
explains the shared-instant rule that a window scope follows.

```python
call("match", {
    "pattern": {"nodes": [{"var": "p", "labels": ["Person"]}, {"var": "s"}],
                "steps": [{"var": "o", "from": "p", "to": "s", "rel_type": "ORDERED"}]},
    "return": {"group": {"by": [{"as": "city", "expr": {"prop": ["p", "city"]}}],
                         "aggs": [{"as": "orders", "fn": "count"},
                                  {"as": "total", "fn": "sum",
                                   "of": {"tprop": ["o", "amount"], "tau": "int"}}]},
               "order_by": [{"expr": {"col": "city"}}]},
    "scope": {"vt": {"window": {"t_a": 0, "t_b": 1000}}, "co_valid": ["valid"]}})
```

Under a window, each order is joined to the version of its buyer valid at
the order's time. In the example, a correction says p2 lived in Lyon all
along, so p2's orders move from Paris to Lyon; as of the load, they are
still counted for Paris.

## Follow-up computation

**`compute`** works on rows an earlier call returned, with no store access:
`count`, `sum`, `min`, `max`, `mean`, `median`, `topk` (by `k` or `pct`),
`filter`, `ratio`, `diff`, `percent`, set operations, `derive`, `join`, and
grouped reductions.

```python
spend = call("aggregate_events", {...})["rows"]
call("compute", {"fn": "topk", "input": spend, "field": "sum_prop_amount", "k": 2})
```

## Pagination

List results carry `rows`, `rows_total`, `truncated` and `cursor`. `limit`
defaults to 100 and is capped at 10,000 (`match` allows up to 1,000,000).
Pass the cursor back, with the same arguments, for the next page:

```python
page = call("version_history", {**args, "limit": 3})
while page["cursor"] is not None:
    page = call("version_history", {**args, "limit": 3, "cursor": page["cursor"]})
```

Example: [`examples/query_patterns.py`](../../examples/query_patterns.py)
covers the last four sections.

<!-- example-output: query_patterns.py -->
```text
resolve 'lee': [('p1', 'Ann Lee'), ('p3', 'Cal Lee')]
resolve 'market' (Shop): ['s2']
name index, prefix 'ann': [('p1', 'Person')]
Lyon buyers, order valid at 400: [('p1', 'Corner')]
orders per city, now: [('Lyon', 6, 205), ('Paris', 1, 90)]
orders per city, as loaded: [('Lyon', 5, 185), ('Paris', 2, 110)]
top 2 spenders: [('p1', 105), ('p4', 90)]
mean spend: 73.75
rows_total 7 read in pages of [3, 3, 1]
```

## The cost check

Example: [`examples/cost_check.py`](../../examples/cost_check.py). It loads
20,000 events and asks for time-respecting paths over the whole window
under a deliberately small budget of 100 ms of estimated time. (The
default budget, 10 seconds, admits the call.) The refusal names the
estimate, the ceiling it broke and what to try. Then the example narrows
the call, raises the budget, and skips the check:

```python
call_operator(store.adapter, "temporal_paths", args, cost_ceilings={"time_est_ms": 100})
call_operator(store.adapter, "temporal_paths", args, cost_ceilings={"time_est_ms": 60_000})
call_operator(store.adapter, "temporal_paths", args, skip_cost_check=True)
```

<!-- example-output: cost_check.py -->
```text
default budget: {'rows_scanned_est': 5120000000, 'expansions_est': 1280000000, 'time_est_ms': 10000}
refused: E_COST - estimated cost for temporal_paths exceeds ceilings
   estimate: {'rows_scanned_est': 20000, 'expansions_est': 2625640, 'time_est_ms': 449}
   over:     {'time_est_ms': 100}
   advice:   ['narrow the valid-time window', 'add a node_filter / seed set', 'restrict rel_types', 'reduce hops / max_hops']
max_hops 2: admitted, 5 of 820 paths returned
window 1/10: admitted, 5 of 10 paths returned
raised budget: 5 of 820 paths returned
skip_cost_check: 5 of 820 paths returned
```

The estimate is a function of the store's statistics and the arguments,
not of the machine, so it is the same on every run. It is an estimate, not
a measurement. `temporal_paths` returns at most `k` paths (5 by default);
`rows_total` counts all of them. See [Concepts](concepts.md#the-cost-check)
for every way to change the budget.
