# Concepts

This page explains the ideas every other page relies on. The
[data model](../data-model.md) reference states the same rules more
tersely, with a worked example.

## Two clocks

Every node version and every edge version carries two half-open intervals.

| clock | interval | set by | answers |
|---|---|---|---|
| valid time | `[vt_s, vt_e)` | you, when you write | when was the fact true in the world? |
| belief time (transaction time) | `[tt_s, tt_e)` | the store, when it commits | when did the store believe this version? |

The two are independent. "Alice lived in Lyon from 2019 to 2023" is a valid
interval. "We recorded that on 3 March and corrected it on 9 May" is a
belief interval.

* You choose valid times. They describe the world.
* You never choose belief times in a live store. Each write batch gets one
  belief time from the store's clock: strictly greater than every earlier
  one, close to the wall clock, never moving backwards. The write call
  returns it. (Replaying an event log is the one way to load history with
  belief times you chose; see [Loading data](loading-data.md#replay-an-event-log).)

## Time units and OPEN_END

* All times are `int64` **microseconds** since the Unix epoch, UTC. A
  second is `1_000_000`; a day is `86_400_000_000`.
* `OPEN_END = 2**62` (`4611686018427387904`) means "no end yet": a valid
  interval that is still open, or a version that is still believed. Import
  it with `from gnomon import OPEN_END`.
* Intervals are half-open. `[100, 300)` holds at 100 and 299, not at 300.
* Operator arguments accept times from `0` to `OPEN_END`. Keep valid times
  at or after the epoch.

Small integers such as `0, 100, 300` are valid times too, and the examples
in this guide use them where real dates would only add noise. With real
dates, convert once:

```python
from datetime import UTC, datetime

def us(iso):
    """ISO date or date-time (UTC) -> int64 microseconds since the epoch."""
    return int(datetime.fromisoformat(iso).replace(tzinfo=UTC).timestamp()) * 1_000_000

us("2021-03-02T09:30")   # 1614677400000000
```

A belief time from a live store is also in microseconds of wall-clock
time, so "as believed on 1 March" is `as_of_tt=us("2021-03-01")`.

## Identities

* A **node** is identified by a string `uid`. It has a `label` and a JSON
  object of properties.
* An **edge** is identified by `(src, dst, rel_type, disc)`. `disc` (the
  discriminator) separates two edges with the same endpoints and type, such
  as two payments between the same accounts. It is `""` for `assert_edge`
  unless you pass one. In a bulk `ingest_events` batch, an event without
  `disc` gets `"#<position in the stream>"`. The engine derives a 96-bit
  edge id, `eid`, from the four fields.
* `EntityRef` names a node or an edge in a write call:

  ```python
  EntityRef("node", uid="alice")
  EntityRef("edge", src="alice", dst="bob", rel_type="KNOWS")             # disc ""
  EntityRef("edge", src="acct-1", dst="acct-3", rel_type="PAYS", disc="tx-1")
  ```

If you plan to correct or retract individual events later, give them a
`disc` you can name. See [Loading data](loading-data.md#name-the-edges-you-will-correct).

## Versions

The store never updates a row. Each node or edge is a set of **versions**:

```
identity | vt_s | vt_e | tt_s | tt_e | label or rel_type | props
```

A version is *believed at* belief time `T` when `tt_s <= T < tt_e`. At any
belief time, the believed versions of one identity have disjoint valid
intervals. Each version has an id, `vid`, derived from its identity, its
`tt_s` and its `vt_s`; because `tt_s` comes from the clock, `vid` values
differ between two loads of the same data.

## Writes, and close records

There are three kinds of write. Each is one batch at one new belief time
`t`.

* **assert** (`assert_node`, `assert_edge`, `ingest_events`): a new belief.
  Every believed version of the same identity whose valid interval overlaps
  the new one stops being believed at `t`. The parts of its valid interval
  outside the new one are written again, with the old properties, believed
  from `t`. Then the new version is written.
* **correct** (`correct(ref, props, vt_s, vt_e)`): the store was wrong
  about `[vt_s, vt_e)`. It works like an assert over that interval, but it
  requires an existing believed version there and keeps its label.
* **retract** (`retract(ref, t_valid)`): the fact ended at `t_valid`. The
  believed version valid at `t_valid` stops being believed, and a copy that
  ends at `t_valid` is written. Retracting at a version's own `vt_s`
  withdraws it entirely.

"Stops being believed" does not rewrite the old row. The batch appends a
**close record**, `(row, tt_e = t)`, to a small immutable file (a close
run). Compaction later folds close records into the segments. The
superseded version stays in the store and keeps answering every question
asked as of a belief time before `t`.

Two consequences are worth knowing:

* An assert and a correction over the same interval store the same rows.
  The difference is intent, and that `correct` refuses when there is
  nothing to correct.
* A new version carries exactly the properties you pass. `correct(ref,
  {"city": "Paris"})` on a node that had `{"name": ..., "city": ...}`
  leaves a version without `name`. Pass the whole property object.

## What "as of" means

`as_of_tt` selects a belief state; valid-time arguments (`t_valid`,
`window`, `t1`/`t2`) select which facts hold in the world. The two combine
freely. The default `as_of_tt = OPEN_END` means the current beliefs.

| operator | `as_of_tt` selects | valid-time arguments |
|---|---|---|
| `entity_history` | the node's versions believed then | optional `t_valid` or `window` filter |
| `version_history` | the belief log as it stood then: `current` = still believed then, `superseded` = revised by then, `all` = written by then; a belief that ended later is reported as still open | `window` (rows overlapping it) |
| `snapshot_subgraph`, `diff_snapshots`, `neighborhood_evolution` | the beliefs the snapshot graph is built from | `t_valid`, or `t1` and `t2` |
| `temporal_reachability`, `temporal_paths`, `paths` | the edges believed then | `window`, `t_valid` or `valid_window` |
| `aggregate_events`, `graph_metric_timeseries`, `burst_detection`, motif operators, `co_active` | the events or versions believed then | `window` |
| `state_series` | with `axis: "valid"`, the belief state; with `axis: "transaction"`, the window itself is a belief-time window | `window`, or `t_valid` with the transaction axis |
| `resolve_entities`, `adapter.lookup_name` | the names of versions believed then | none |
| `match` | `scope.as_of_tt`, with `scope.belief` = `current`, `superseded` or `all` | `scope.vt`: an `instant` or a `window` |
| `diff_belief` | takes no `as_of_tt`: it compares belief times `t1` and `t2` | `window` |
| `compute` | none: it works on rows you pass in | none |

### Pinned answers

Every result envelope reports the belief time it was served at:

* `tt_q`: the belief time the read used.
* `pinned`: `true` when you asked for a belief time the store has already
  reached. Such an answer never changes: later writes have later belief
  times. Its `result_digest` stays the same forever.
* `clamped`: `true` when you asked for a belief time in the future. The
  read was served at the store's current belief time instead, so later
  writes can still change the answer.

To make an analysis reproducible, record the belief time you ran it at
(for example `store.frontier_tt()`, or the value a write returned) and pass
it as `as_of_tt` when you rerun it.

## Result envelopes

Every operator returns a dictionary, the **envelope**:

| key | meaning |
|---|---|
| `op`, `args_echo` | the operator, and its arguments with defaults filled in |
| `dataset_extent` | the store's valid-time extent of edges, `{vt_min, vt_max}` |
| `rows`, `rows_total`, `truncated`, `cursor` | the payload of list operators, and pagination (some operators add other payload keys, such as `nodes` or `added_total`) |
| `result_digest` | SHA-256 of the canonical payload; equal answers have equal digests |
| `tt_q`, `pinned`, `clamped` | the belief time served, as above |
| `dependency` | which later writes could change this answer; see `Store.check_result` in the [API reference](api-reference.md#freshness) |
| `plan_ir` | metadata of the plan that ran the call |

## The cost check

Before an operator runs, `call_operator` estimates its work from the
store's statistics: rows to scan (`rows_scanned_est`), expansions
(`expansions_est`), and from those an estimated time in milliseconds
(`time_est_ms`). A call over any ceiling is refused before it starts with
`CostError` (code `E_COST`). The default ceilings are:

```python
{"rows_scanned_est": 5_120_000_000, "expansions_est": 1_280_000_000, "time_est_ms": 10_000}
```

The refusal carries the estimate, the ceilings it broke and some advice.
You can:

* narrow the call: a smaller window, fewer hops, fewer seeds, `rel_types`;
* raise (or lower) the ceilings for one call:
  `call_operator(..., cost_ceilings={"time_est_ms": 60_000})`;
* skip the check for one call: `call_operator(..., skip_cost_check=True)`;
* scale every time estimate for a slower or faster machine with the
  environment variable `GNOMON_TIME_COEFF_SCALE` (default `1.0`), set
  before the process starts.

The `gnomon call` command applies the default ceilings and has no option to
change them. On very large stores the estimate is conservative and can
refuse calls that would be fast; see [limitations](../limitations.md#known-open-items)
and [configuration](../configuration.md#query-admission). A worked example
is in [Querying](querying.md#the-cost-check).

## Errors

Every error is a `gnomon.core.errors.GnomonError` with a stable `code`, a
`message` and structured `details`. `e.to_payload()` returns them as a
dictionary.

| code | class | typical cause |
|---|---|---|
| `E_SCHEMA` | `SchemaError` | an argument has the wrong type, or an unknown key |
| `E_INVALID_ARG` | `InvalidArgError` | arguments are well-typed but do not make sense (`t_a >= t_b`, a missing companion argument) |
| `E_NOT_FOUND` | `NotFoundError` | `retract` or `correct` found no believed version to act on |
| `E_COST` | `CostError` | the cost check refused the call |
| `E_LIMIT` | `LimitError` | a hard cap: too many buckets, hops or `k` |
| `E_STATE` | `StateError` | an illegal state, such as writing through a read-only handle; `WriterLockedError` (another process holds the writer lock) is a subclass |
| `E_INTERNAL` | `InternalError` | a bug; please report it |
