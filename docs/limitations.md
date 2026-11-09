# Limitations and open items

Gnomon is a research prototype. What it does not do, and what is known to be
unfinished, stated plainly:

## Scope

* **Single node.** A store is a directory on one machine's file system.
  There is no replication, sharding or distributed query execution.
* **Single writer.** One process at a time may write a store; a second
  writer fails fast on `<store>/writer.lock` (`WriterLockedError`). Any
  number of reader processes may open the store with `read_only=True`; each
  sees a consistent snapshot (one manifest generation) and never blocks the
  writer.
* **Writes go through Python.** The bi-temporal write semantics (carving
  overlapping versions, retraction, correction) are implemented once, in
  `gnomon/storage/base.py`, on top of a small set of engine primitives. The
  engine core alone has no public write API with those semantics.
* **No query language.** Questions are asked through the operator registry
  ([operators](operators.md)); the richest of them, `match`, takes a typed
  JSON pattern, not a query text, and its compiler chooses join orders from
  label and relation counts rather than from a cost-based optimizer.
  Composing operators into larger plans is left to the caller.
* **POSIX file-system semantics are assumed.** Garbage collection's
  guarantee that a reader in another process keeps reading a generation it
  has opened relies on unlink-while-open behaviour of a local POSIX file
  system; it is not claimed for Windows or network file systems.

## Known open items

* **Cross-process reader pinning is bounded.** A reader maps every file of
  its generation at open, so a concurrent `gc` cannot remove them underneath
  it, but only for stores with at most `GNOMON_PIN_MAX_SEGMENTS` live
  segments (default 16,384). Above that a reader falls back to mapping files
  on first touch, and the Python adapter re-opens the store when a file has
  disappeared. There is no cross-process reader registry.
* **Maintenance is explicit.** Compaction, `settle` and `gc` run only when
  called: by the caller, by the benchmark driver's `--compact-every`, or
  through the CLI. There is no background scheduler. `settle` must run while no
  writer is active, and it leaves superseded files for `gc` to reclaim, so a
  store is transiently up to twice its size on disk.
* **The cost guardrail is conservative at large scale.** Operators estimate
  their cost from stored row counts before running and refuse calls above a
  ceiling. The estimate is linear in the number of stored rows regardless of
  how selective a time window is, so on stores of 100M rows and more it
  refuses some window-bounded queries whose actual cost is small. Where an
  engine kernel answers the call, `temporal_reachability`,
  `version_history`, `aggregate_events`, `co_active`,
  `neighborhood_evolution`, `diff_belief` and `state_series` price the work
  that kernel does instead (the adapter reports its routes,
  `guardrails.cost_routes`), and `paths` prices its own search rather than a
  whole-store scan; `snapshot_subgraph`, `diff_snapshots` and
  `resolve_entities` price the rows left after zone-map pruning and, for a
  seeded snapshot, the seeds, hops and largest out-degree
  ([configuration](configuration.md#query-admission)); the portable routes
  and the other operators keep the linear estimate. Callers can pass
  `skip_cost_check=True` (the benchmark harness exposes `--skip-cost-check`).
* **Multi-hop snapshots are priced for the largest out-degree.** The seeded
  snapshot estimate assumes every frontier vertex has the store's largest
  out-degree, so on a skewed graph a multi-hop snapshot from seeds that are
  not hubs can be refused although its real cost is small. Pass
  `skip_cost_check=True` for such a call, or raise the ceiling.
* **Some windowed reads still visit more than their window.** The cut below
  a window’s start is a prefix bound: it stops at the first block holding a
  closed version still valid at the window’s start, so one long-lived
  version early in a segment keeps the rows after it, up to the window, in
  the scan. Event segments get this cut from a structure derived when the
  segment is opened into the cache, which costs one pass over its `vt_e`
  column per cached open. The windowed belief diff and windowed traversals
  open every candidate segment before pruning it by its header bounds.
* **The engine kernels cover stated shapes only.** The range-native
  operators ([architecture](architecture.md#range-native-operators)) answer
  the shapes listed there, on the native engine, outside an open write
  batch. Everything else takes the general path, with the same answer and
  the cost it had before: `temporal_reachability` with `delta_max_wait`,
  `diff_snapshots` with `scope`, aggregations that group by relation or
  endpoint or compute more than counts, and every call on the DuckDB
  reference backend.
* **The engine's write path from the log record covers stated shapes.**
  `ingest_events` ops whose event `props` hold a float, and any op shape the
  record parser does not accept, are staged by the Python path; so is every
  write on the DuckDB backend.
* **The pattern executor covers stated forms.** The engine declines a
  `match` plan with a node or expression form it does not implement, or a
  row the reference evaluator would refuse, and inside an open write batch;
  those calls are answered by the NumPy reference evaluator, with the same
  answer and its cost.
* **The plan IR's encoding is internal.** Plans are data (`gnomon/plan`), but
  their JSON encoding carries a format version and is not a stable
  interface.
* **Design switches are fixed per handle.** The engine reads its design
  switches when a store is opened; changing the environment afterwards does
  not affect an open handle (`store.adapter.designs()` says what it runs
  with).
* **Close records have two physical representations, not three.** A
  segment's closes are folded into either an "all current" flag or a sorted
  sparse list; a dense per-row representation is reserved in the format but
  not implemented, and no measured workload has needed it.
* **Legacy formats are read-only.** Stores written with manifest formats 1
  or 2 open and answer queries but refuse writes until
  `gnomon store upgrade-manifests` converts them to format 3.
* **Independent loads differ in transaction time.** Transaction times come
  from the wall clock at commit, so two loads of the same events produce
  different `store_digest()` values. Compare loads with the content digest
  (`gnomon store digest`), or replay one frozen event log into both
  (`gnomon replay`, `scripts/freeze_synth_log.py`).
* **The DuckDB reference backend is partial.** It implements the storage
  interface for cross-checking and answers the registered operators (the
  engine-level ones through their general paths, `match` through the
  reference evaluator), but not the engine-only call `lookup_name`, which it
  reports as unsupported.
