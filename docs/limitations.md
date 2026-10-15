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
* **No query language.** Questions are asked through the fixed operator
  registry ([operators](operators.md)). Composing operators into larger plans
  is left to the caller.
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
  refuses some window-bounded queries whose actual cost is small. Callers can
  pass `skip_cost_check=True` (the benchmark harness exposes
  `--skip-cost-check`).
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
* **Fast bulk ingest covers one shape.** `ingest_events` ops that carry a
  `nodes` array or event `props` are staged by the Python row build, as are
  all other write operations; the engine’s own path from the log record
  covers only the plain bulk-event shape.
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
  interface for cross-checking and answers the registered operators, but not
  the engine-only calls `lookup_name` and `diff_belief`, which it reports as
  unsupported.
