# Python API reference

The public entry points, with their parameters as defined in the code.
Times are `int64` microseconds; `OPEN_END = 2**62`. Operator arguments are
listed in [operators](../operators.md) and in each operator's schema
(`REGISTRY[name].engine_schema`).

- [The `gnomon` package](#the-gnomon-package)
- [Store](#store)
- [Operators](#operators)
- [The native adapter](#the-native-adapter)
- [Freshness](#freshness)
- [Group commit](#group-commit)
- [Event logs and replay](#event-logs-and-replay)
- [Errors](#errors)
- [Command line from Python](#command-line-from-python)

## The `gnomon` package

```python
import gnomon
from gnomon import OPEN_END, EntityRef, Interval, Store
```

| name | what it is |
|---|---|
| `gnomon.open(path, backend=None, paranoid=False, read_only=False)` | open or create a store; returns a `Store` |
| `gnomon.Store` | the store class; `gnomon.open` takes the same arguments |
| `gnomon.OPEN_END` | `2**62`: an open valid interval, or the current beliefs |
| `gnomon.EntityRef(kind, uid=None, src=None, dst=None, rel_type=None, disc="")` | names a node (`kind="node"`, `uid`) or an edge (`kind="edge"`, `src`, `dst`, `rel_type`, `disc`) for `retract` and `correct` |
| `gnomon.Interval(start, end)` | a half-open interval `[start, end)` with `contains(t)` and `overlaps(other)` |
| `gnomon.__version__` | the installed package version |

### `gnomon.open`

| parameter | default | meaning |
|---|---|---|
| `path` | | the store directory; created (with its parents) if absent, unless `read_only` |
| `backend` | `None` | `None` keeps an existing store's backend and uses the native engine for a new one. `"duckdb"` selects the DuckDB reference backend (needs the `duckdb` extra), which exists for cross-checking |
| `paranoid` | `False` | after each write batch, check that the believed versions of every touched identity have disjoint valid intervals. A debugging aid: it makes writes slower |
| `read_only` | `False` | open as a reader: no recovery, no writes, nothing written under the directory. The store must exist |

A read-write handle takes `writer.lock` and, if the last writer crashed,
replays the event-log suffix the engine had not applied.

## Store

### Writes

Each call is one batch and returns its belief time (`int`).

| method | parameters | does |
|---|---|---|
| `assert_node(uid, label, props=None, vt_s=0, vt_e=OPEN_END)` | | a node version over `[vt_s, vt_e)`; supersedes overlapping believed versions |
| `assert_edge(src, dst, rel_type, props=None, vt_s=0, vt_e=OPEN_END, disc="")` | | an edge version; does not create endpoint nodes |
| `correct(ref, new_props, vt_s=0, vt_e=OPEN_END)` | `ref`: an `EntityRef` | replaces the believed versions over `[vt_s, vt_e)` with `new_props`; `E_NOT_FOUND` if none overlap |
| `retract(ref, t)` | `ref`: an `EntityRef` | ends the version valid at `t` at `t`; `E_NOT_FOUND` if none is valid at `t` |
| `ingest_events(events, node_label="Node", nodes=None)` | `events`: iterable of `{src, dst, rel_type, vt_s, vt_e?, disc?, props?}`; `nodes`: iterable of `{uid, label, props?, vt_s, vt_e?}` | bulk load in batches of 50,000; returns the belief time of the last batch |

Writing through a read-only handle raises `StateError`.

### Reading and introspection

| member | returns |
|---|---|
| `adapter` | the storage adapter; pass it to `call_operator` |
| `frontier_tt()` | the latest belief time this handle serves |
| `stats()` | `{n_entities, n_node_versions, n_edge_versions, vt_min, vt_max, rel_type_counts, max_out_degree}`; version counts include superseded versions; `vt_min`/`vt_max` cover edges |
| `digest()` | the store digest: every version, belief times included |
| `digest_streaming(chunk_rows=200_000)` | the same value, in bounded memory |
| `store_identity` | a property: a hash of the event log's header and first batch, stable across replays |
| `path`, `read_only`, `backend` | as opened |
| `close()` | release the handle and the writer lock |

The content digest (`gnomon store digest`) is on the adapter, below.

## Operators

```python
from gnomon.temporal.algebra import call_operator, ensure_all_registered, REGISTRY, BASE_OPERATORS
```

| name | what it is |
|---|---|
| `ensure_all_registered()` | imports every operator module; call it once before `call_operator` or `REGISTRY` |
| `call_operator(...)` | validates `args`, runs the cost check, runs the operator and returns the result envelope; signature below |
| `REGISTRY[name]` | the operator's spec: `.description`, `.args_schema` (the base schema), `.engine_schema` (the full schema `call_operator` uses by default) |
| `BASE_OPERATORS` | the names of the fifteen base operators |

`call_operator` is defined as:

```python
call_operator(adapter: StorageAdapter, name: str, args: dict[str, Any],
              skip_cost_check: bool = False,
              cost_ceilings: dict[str, int] | None = None,
              tt_source: Any = None,
              agent: bool = False) -> dict[str, Any]
```

Its parameters:

| parameter | meaning |
|---|---|
| `adapter` | `store.adapter` |
| `name` | an operator name, such as `"snapshot_subgraph"` |
| `args` | a JSON-like dictionary; unknown keys are refused |
| `skip_cost_check` | `True` runs the call without the cost check |
| `cost_ceilings` | overrides some or all of the ceilings `rows_scanned_est`, `expansions_est`, `time_est_ms` for this call |
| `tt_source` | the `Store`; gives the envelope's freshness data the store's identity and applied frontier, which `Store.check_result` needs |
| `agent` | `True` validates against the base schema only, so engine-level arguments of base operators are refused as unknown; the engine-level operators themselves still run |

The envelope's keys are described in [Concepts](concepts.md#result-envelopes).
The default ceilings are `gnomon.temporal.guardrails.DEFAULT_CEILINGS`.

## The native adapter

`store.adapter` on the native engine is a `gnomon.storage.native.NativeAdapter`.
Besides serving operators, it offers:

| method | does |
|---|---|
| `lookup_name(query, mode, as_of_tt=OPEN_END, limit=None)` | name-index lookup; `mode` is `"exact"` or `"prefix"`; returns `(uid, dense id, label, props JSON)` tuples |
| `diff_belief(t1, t2, t_a, t_b, limit)` | the engine call behind the `diff_belief` operator |
| `compact(build_indexes=False)` | incremental compaction; returns a report |
| `settle(budget_bytes=None, build_indexes=True)` | one-shot merge for a read-optimised layout; run with no writer active |
| `gc(keep_last=2)` | delete superseded manifests and files |
| `build_indexes(tcsr=True, name=False, vidfilter=False)` | pre-build per-segment indexes |
| `verify(mode="fast")` | integrity check (`"fast"` or `"full"`); returns the report `gnomon store verify` prints |
| `content_digest()` | the content digest: every version minus `vid`, `tt_s`, `tt_e` |
| `content_digest_streaming(budget_bytes=None)` | the same, in bounded memory; returns a report with `digest` |
| `store_digest()` | the store digest (`Store.digest()`) |
| `designs()` | the engine designs in force for this handle |
| `small_commit()` | `{enabled, n, ms}` of the small-commit path |

Maintenance through the adapter needs a writer handle, as the
corresponding `gnomon store` commands do.

## Freshness

An envelope's `dependency` describes which later writes could change the
answer. The store can check it without rerunning the query:

| method | does |
|---|---|
| `Store.check_result(envelope, *, tt_now=OPEN_END, chain_cache=None)` | checks the writes in the event log after the answer's belief time |
| `Store.check_scope(scope, ...)` | the same for a stored `dependency` object |
| `Store.check_trace(record, ...)` | the same for a saved multi-step record, step by step |

The verdict has a `state`: `fresh` when nothing written since can change
the answer, `possibly-stale` with `witnesses` (the writes that might),
or `undecidable` with a `reason`. Pass `tt_source=store` to `call_operator`
for envelopes you will check; without it the verdict is `undecidable`
(`store-mismatch`).

```python
env = call_operator(store.adapter, "entity_history", {"uid": "alice"}, tt_source=store)
store.check_result(env).state                    # 'fresh'
store.correct(EntityRef("node", uid="alice"), {"name": "Alice B."}, vt_s=0)
store.check_result(env).state                    # 'possibly-stale'
```

## Group commit

```python
from gnomon.write import GroupCommitWriter, QueueFullError
```

`GroupCommitWriter(store, max_delay_s=0.0, max_batch=1000, max_queue=0, metrics=None)`
gathers write calls from many threads into shared batches.

| member | does |
|---|---|
| `start()` | starts the committer thread; returns the writer |
| `assert_node`, `assert_edge`, `retract`, `correct` | as on `Store`; block until the batch holding the call is durable, and return its belief time |
| `submit(op, *, block=False, timeout=None)` | queue a raw op (`make_op(...)`) |
| `close()` | commit everything already submitted, then stop |
| `stats()` | commits, submissions, largest group, and fallbacks |

`max_delay_s` waits that long to gather more calls (default: do not wait).
`max_batch` caps a batch. `max_queue` bounds the queue; when it is full,
`submit` raises `QueueFullError` (`E_QUEUE_FULL`) unless `block=True`.

## Event logs and replay

```python
from gnomon.storage.eventlog import EventLog, replay
from gnomon.storage.base import make_op
```

| name | does |
|---|---|
| `EventLog(path, *, create=True)` | open an event log, creating it with its header if absent |
| `EventLog.append(tt, ops)` | append one batch record at belief time `tt` and sync it |
| `EventLog.batches()` | iterate over the batch records (`{"batch_id", "tt", "ops"}`) |
| `make_op(kind, **fields)` | an op record in canonical form: `kind` is `assert_node`, `assert_edge`, `correct`, `retract` or `ingest_events` |
| `replay(eventlog_path, adapter, *, thread_cursor=False, compact_every=None, compact_trigger=False)` | apply a log to an empty store's adapter at its recorded belief times; returns the number of batches |

The simplest way to replay is the command line, which also places the log
inside the new store: `gnomon replay log.jsonl --store new`. See
[Loading data](loading-data.md#replay-an-event-log) for the op fields.

## Errors

```python
from gnomon.core.errors import (GnomonError, SchemaError, InvalidArgError, NotFoundError,
                                CostError, LimitError, StateError, InternalError)
from gnomon.store import WriterLockedError
```

Every error has `code`, `message`, `details` and `to_payload()`. The codes
are listed in [Concepts](concepts.md#errors).

## Command line from Python

`gnomon.cli.main(argv)` runs one `gnomon` command in the current process
and returns its exit status:

```python
from gnomon.cli import main as gnomon_cli
gnomon_cli(["replay", "revisions.jsonl", "--store", "kb"])
```
