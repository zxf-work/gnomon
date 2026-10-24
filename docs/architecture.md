# Architecture

Gnomon has three layers:

```
 Python  gnomon.Store ─ write-ahead event log (eventlog.jsonl), single-writer lock
         gnomon.storage.base ─ bi-temporal write semantics (carve / retract / correct)
         gnomon.temporal ─ operator registry, argument schemas, result envelopes
         gnomon.plan ─ the plan IR `match` compiles to, and its reference evaluator
           │ PyO3 (crates/gnomon-py → gnomon._engine)
 Rust    gnomon-core ─ segments, close runs, manifests, compaction, indexes, scans
           │
 Disk    <store>/native/  immutable files + one atomically replaced pointer
```

The store is a deterministic materialization of its event log. That shapes
the durability design: the engine never has to make a partial physical
update recoverable; it only has to never expose an inconsistent generation.
After a crash it rolls back to the last published generation and the Python
layer replays the event-log suffix the engine had not applied.

## Store layout

```
<store>/
  eventlog.jsonl          write-ahead log: a header line, then one JSON record per batch
  writer.lock             held (flock) by the single writer for its lifetime
  native/
    CURRENT               "<generation> <manifest_sha>" — the publication point
    manifests/<G>.json    one manifest record per generation (checkpoint or delta)
    seg/<id>.tgs          immutable columnar segments (node rows and edge rows)
    close/<id>.tgc        immutable close runs (belief-time ends recorded after the fact)
    dict.log              append-only dictionary: uid string -> dense u32 id
    idx/                  rebuildable per-segment indexes (never authoritative)
```

## Commit protocol

Each write batch commits in this order, each step durable before the next:

1. the batch record is appended to `eventlog.jsonl` and fsynced (by the
   Python `Store`, before the engine is called);
2. the batch's new segment files and close-run file are written and fsynced;
3. the dictionary tail is appended and fsynced;
4. the new manifest record is written, fsynced and renamed into place;
5. `CURRENT` is replaced atomically — the generation is now published.

A crash before step 5 is invisible: the next open reads the old `CURRENT`,
ignores the orphaned files and truncates the dictionary tail. Each manifest
records the event-log offset and a rolling hash chain over the log prefix it
has applied, so recovery replays exactly the missing suffix and refuses a log
that does not match.

### Bulk ingest and per-operation writes from the log record

The bi-temporal write semantics live in Python (`gnomon/storage/base.py`),
which turns each event of an `ingest_events` op into Python objects before
the engine sees it. For the common shapes the engine instead works straight
from the event-log record the batch was just logged as (replay hands it the
same bytes):

* **Bulk events.** An `ingest_events` op — with or without a `nodes` array,
  and with event `props` that hold no float — is parsed, its identity hashes
  derived, the same endpoint-existence probe run, the same dictionary
  entries inserted in the same order and the same rows staged.
* **Per-operation ops.** A transaction of `assert_node`, `assert_edge`,
  `retract` and `correct` ops is applied op by op with the engine primitives
  the Python path calls, in the same order: the windowed point read of the
  versions the op overlaps, the retires and closes that supersede them, then
  the staged rows, with the same version ids, intervals and canonical props.

Any other shape, or anything the parser does not accept, is refused before
anything is changed and takes the Python path, so the stored rows, dense ids
and digests are the same either way. The windowed point read (`pointread.rs`)
is what keeps a write's cost independent of its key's history: within a
segment a key's rows are in `vt_s` order, so the rows that can overlap the
written interval are a prefix of its postings, walked backwards and stopped
at the first believed row that ends before the interval (the believed
versions of one identity are disjoint in valid time).

### The small-commit path

By default every transaction publishes its own generation. With
`GNOMON_SMALL_COMMIT=1` a transaction is acknowledged as soon as its
event-log record is fsynced — the record is the source of truth, and a crash
before its generation is published is recovered by the ordinary suffix
replay — and the engine batch stays open across transactions: one generation
is published per `GNOMON_SMALL_COMMIT_N` transactions, once the oldest
unpublished transaction is `GNOMON_SMALL_COMMIT_MS` old, before any read on
the writing handle, and on close. Each transaction keeps its own log record
and its rows their own `tt_s`, so durability, the event log and both digests
are unchanged; what changes is that a reader in another process sees
published generations only and may lag the writer by that much. A failed
transaction rolls the batch back and re-applies its unpublished batch-mates
from their records, which is deterministic.

## Segments

A segment (`.tgs`) is written once and never modified. That one property
gives snapshot isolation, safe `mmap`, and lock-free concurrent readers.

* Rows are sorted by `(vt_s, vid)` and stored column by column in blocks of
  32,768 rows. Fixed-width hot columns (`vt_s`, `vt_e`, endpoints, relation
  code, ...) are 64-byte aligned for zero-copy typed views; variable-width
  values (properties, labels, discriminators) live in a per-segment string
  table referenced by index.
* Every column extent and the string table carry a CRC32C; the whole body
  carries a SHA-256 (`body_sha`), recorded in the footer and in the
  manifest entry. A trailing magic marks a complete file.
* Edge rows are routed at seal time into two physical classes: short-lived
  **event** rows, and long-lived **interval** rows with an explicit `vt_e`.
  The class is physical only: it never affects identity, answers or
  digests.
* Segment headers carry zone maps (the ranges of `vt_s`, `vt_e` and `tt_s`
  per segment, and per-block bounds where a column needs them) that let
  scans prune whole segments and blocks.

### Cutting below a window's start

Rows are sorted by `vt_s`, so a valid-time window `[t_a, t_b)` bounds a
segment's rows from above by binary search. From below it cannot: a row that
started long before `t_a` may still be valid at `t_a`. The scan planner therefore
uses a per-block running maximum of `vt_e` (a prefix-max "staircase"): every
block before the first one whose maximum reaches `t_a` holds only rows that
ended before the window, and is skipped. Rows with an open `vt_e` are kept
apart as row ranges, so they never raise the maximum.

* **Interval segments** carry the staircase in their header, written at seal
  time.
* **Event segments that store `vt_e`** get the same structure **derived when
  the segment is opened** into the segment cache: one pass over the stored
  `vt_e` column at 4,096-row blocks (8 bytes per block), kept with the cached
  segment and never written. Segment bytes and digests are unchanged, so an
  existing store gets the cut on its next open. Event segments whose rows
  are all instantaneous store no `vt_e` and need no cut.

The cut is a bound only: it never changes which rows are selected, only how
many are examined. Scans report the written and the derived structure's
`blocks_considered` / `blocks_skipped` separately.

### Time encoding

Times are `int64` microseconds. Columns are compressed per block:

* **Codec 1, frame of reference** — per block, the minimum and the bit width
  of `max - min`, then each value's delta bit-packed at that width. A
  constant block costs no payload at all.
* **Codec 2, linear frame of reference** — per block, an integer intercept
  and fixed-point slope, then each value's residual against that line. It
  suits `vt_s`, which rises steadily in sort order. The writer tries the
  candidate codecs on each column and keeps the smallest; no floating point
  is stored, so encodings are identical across machines.
* **Transaction time** (`tt_s`) is stored either as runs `(first_row, tt_s)`
  — one per batch, ideal for freshly sealed segments — or, once compaction
  has interleaved rows from many batches, as a dictionary of the distinct
  times plus a bit-packed per-row code column with per-block bounds. The
  writer measures and picks; readers see one view either way.

## Close runs and sidecars

Superseding a version means setting its `tt_e`, but a sealed segment cannot
be changed. Each commit instead appends an immutable **close run**: a list of
`(segment, row, tt_e)` records. The manifest lists the live close runs, and
because a reader pins one manifest generation, it sees exactly the closes
that existed at that generation — never a later one.

Compaction later folds close runs into a per-segment **sidecar** in the
segment header, in one of two representations: an `all_current` flag (no
row closed; costs nothing on a scan) or a sorted sparse list of
`(row, tt_e)` pairs. Folding moves where a `tt_e` is written; it never
drops a row.

### What a belief test costs

A read decides belief only for the rows it can use. Rows are stored in
`vt_s` order, so a valid-time window maps to a contiguous row range of each
segment, found by binary search; the belief state (the transaction-time
runs or dictionary codes, and the close records) is computed for that row
range alone. The cost of visibility is therefore proportional to the
`tt_s` runs, the straddling dictionary codes and the close records inside
the query’s row range, not to the segment’s totals, and a segment nothing
has corrected skips the work entirely. When a request covers most of a
segment, the engine computes the whole segment’s belief state once
instead. Traversals choose between building a segment’s believed row ranges
and testing belief per adjacency row, whichever reads less. The engine
counts the runs, codes and closes each belief test reads (`phase_stats`),
so this cost claim is checked by tests on counts rather than timings.

Traversals over a segment with closes can decide belief from a cached
**effective-`tt_e` column**: one value per row (the row's own `tt_e`, or the
close that ended it), built once per segment and kept within
`GNOMON_TT_E_COLUMN_BYTES`, so the test is one comparison per adjacency row
instead of building the segment's believed ranges per call.

## Range-native operators

Several operators are answered in the engine from the believed row ranges
the scan planner already computes, without first materialising the rows
they select. Each covers a stated shape; any other call takes the general
path, and both give the same answer, byte for byte (the tests compare them,
and each can be switched off: [configuration](configuration.md#design-switches)).

* **Counts by time bucket.** A count-only aggregation by time bucket — the
  event-rate series behind `graph_metric_timeseries` and `burst_detection`,
  and `aggregate_events` calls with one `time_bucket` dimension (or none)
  and only `count` aggregates — intersects each segment's believed,
  window-clipped row ranges with the bucket edges by binary search over
  `vt_s`. It never builds a selection vector or a group table. A relation
  filter that splits a segment is applied per row inside this path; the
  relation and endpoint dimensions, `count_distinct` and the other
  statistics take the row path.
* **Belief diff, top-k on binary keys.** `diff_belief` keeps every row that
  survives its filters as a binary sort key (kind, uid or edge identity,
  `tt`, change, version id) built in parallel, counts its totals from the
  keys, selects and sorts the requested page on them, and builds strings
  for the page's rows only. The binary order is the string order the
  answer is defined in.
* **Global snapshot diff on dense identities.** `diff_snapshots` without
  `scope` plans both point states as sorted row lists per segment and
  retires every row present in both with a merge, without hashing anything:
  the same stored row is the same version at both instants. Only the
  leftovers are joined on identity — nodes on their dense dictionary id,
  edges on their identity key, computed on the worker pool — and only
  candidates whose stored property text differs go back to the operator,
  which applies its own content comparison.
* **Earliest-arrival reachability.** `temporal_reachability` without
  `delta_max_wait` reads the window's believed rows into a by-source
  compressed sparse row array (built in parallel) and runs a label-setting
  search over a monotone radix heap. This is exact because traversing an
  edge never decreases an arrival and is monotone in it, so the first label
  taken off the heap is final; `delta_max_wait` breaks the second property
  and keeps the general multi-label search. The page, ordered by
  `(arrival, uid)`, is selected and sorted alone.
* **Few-node scans.** `snapshot_subgraph` expands from its seeds, one scan
  filtered to the frontier per hop, rather than building the whole snapshot
  at the instant. A scan filtered to a few nodes (at most 1,024 ids) takes,
  in each segment, the rows the adjacency index names for those ids
  whenever they are fewer than the rows the scan would otherwise examine.

## Manifest generations

A manifest names everything that defines visibility at one generation: the
live segments (with their key ranges, row counts and `body_sha`), the live
close runs, the dictionary length, and the event-log offset and hash chain.
Readers pin a generation and therefore see a coherent snapshot.

Manifests are JSON so an operator can read them with `cat`. On disk the
current format (3) is a chain: a **checkpoint** holds the whole logical
manifest (written at generation 0, every 512th generation, and by
compaction and gc); a **delta** holds only what changed since its parent.
Each record carries its own checksum and its parent's hash. `manifest_sha`,
the digest of a generation's full logical content, is a Merkle root over the
ordered segment set, so a commit costs `O(appended + log n)` hashing rather
than re-hashing every live segment. Stores written in formats 1 and 2 open
read-only until `gnomon store upgrade-manifests` converts them.

## Compaction, settle and gc

Each batch seals at least one new segment, so a store that is written in
many small batches accumulates many small segments and unfolded close runs.
Three explicit maintenance operations bound that; none runs in the
background.

* **Compaction (incremental, size-tiered).** Segments are grouped into
  *clusters* — maximal sets whose composite-key ranges overlap, computed
  from the manifest without opening any file. Within a size tier, a set of
  same-size segments is merged by a bounded-fan-in streaming k-way merge
  (one row resident per input) into new segments near the target size
  (128 MiB), folding their close runs on the way. Segments already at the
  target are left alone, so each row is rewritten about once per tier
  rather than once per compaction. `Store.adapter.compact()` and
  `gnomon store compact` run it.
* **Settle.** The tier rule leaves partially filled tiers unmerged, and those
  are exactly the segments a whole-extent query must open. `settle` is a
  one-shot major compaction run at quiescence: one bounded k-way merge over
  every live segment, in key order, in groups bounded by a byte budget
  (`--budget-bytes` / `GNOMON_SETTLE_BYTES`), rewriting each row once and
  ending at the store's byte floor. The logical content, transaction times
  and every digest are unchanged. It rebuilds the per-segment indexes of the
  segments it writes unless told not to.
* **Garbage collection.** Compaction and settle supersede files but delete
  nothing, because a reader may still hold an older generation. `gc` removes
  a manifest or file exactly when no retained generation names it; a
  generation is retained if `CURRENT` names it, if it is among the last
  `--keep` generations (default 2), or if a live reader in this process has
  pinned it. Readers map every file of their generation at open, so files
  stay readable to them even after `gc` unlinks them.

## Per-segment indexes

Indexes live under `native/idx/`, one file per segment and index kind. Each
is stamped with its segment's `body_sha`; any load failure or stamp mismatch
means "rebuild silently", and a failure to write one never fails a query. An
index is never authoritative.

* **Temporal adjacency (TCSR).** A compressed sparse row index over all
  stored rows of one edge segment, in both directions, holding only `u32`
  offsets and a row permutation; times and endpoints are read through the
  permutation from the segment's own mapped columns. Belief filtering
  happens at traversal time, so a commit invalidates nothing — new segments
  simply have no index yet. Used by neighbourhood, path and reachability
  operators.
* **Name index.** For node segments, a sorted array of
  `(casefolded name, row)` pairs, answering exact and prefix name lookups
  by binary search (`lookup_name`). It covers every stored row, not only
  current ones, so it is as immutable as its segment and also answers
  historical `as_of` probes; candidates are re-checked for belief and for
  the actual name at query time.
* **Identity filter.** A correction or retraction must first find the
  believed versions of one identity. Identities are hashes and the store is
  clustered on `(vt_s, vid)`, so key ranges cannot prune that probe; without
  help it would visit every segment. Each segment therefore gets an immutable
  membership filter over the identity keys of all its stored rows, built at
  seal time (with an explicit rebuild and a lazy build on first use as
  fallbacks for older stores). The filter is a binary fuse filter with 16-bit
  fingerprints (false-positive rate about 1.5e-5), falling back to a Bloom
  filter, and finally to no filter. False negatives are impossible by
  construction; a false positive costs one extra segment visit.

### Path kernels

* **Counted paths.** `temporal_paths` counts its time-respecting paths in
  the engine and materialises only the first `k`: the search reads the
  graph from both ends, a round at a time and the cheaper side first, and
  computes for every node and hop budget the latest arrival from which the
  target is still reachable. The total is exact, so a hub that would exhaust
  an enumeration's budget is answered.
* **Interval pages.** `co_active` counts its interval join and cuts the
  requested page in the engine, without materialising the whole join.
* **The window search of `paths`.** Under `valid_window` the least-cost
  search runs over (node, set of instants) states with exact costs:
  integer weights as integers, float weights as exactly scaled rationals, so
  ties are ties.
* **Frontier loading.** A bounded search from known sources reads the edges
  incident to its frontier, round by round, and switches to one read of the
  window when a round would read more than half of it.

## Pattern matching and the plan IR

`match` (`gnomon/temporal/ops_match.py`) compiles its argument to one plan
of the plan IR (`gnomon/plan`): a DAG of typed nodes — `NodeScan`,
`EdgeScan`, `Expand`, `Filter`, `TypeConstraint`, `Project`, `Join`,
`Aggregate`, `Order`, `Limit`, plus the shared-instant nodes `CoValidExpand`,
`CoValidJoin` and `Collapse` — with the scope Σ and the output columns.
**One plan, two executors:** the engine's plan executor
(`crates/gnomon-core/src/matcher.rs`) runs it by default, and the reference
evaluator (`gnomon/plan/eval`, NumPy) runs the very same plan with
`GNOMON_MATCH_KERNEL=python`, on the DuckDB backend, inside an open write
batch, or when the engine declines the plan. Because both consume one plan,
an answer difference between them can only be an execution difference, and
the tests compare them on random stores; an independent oracle checks the
compiler against the operator's definition.

The engine executor is range-native:

* a relation is a **binding table** — per pattern variable a column of
  dense ids and a column of version addresses (segment, row), per computed
  expression a typed column; no row carries a string;
* scans read each segment's believed, window-clipped row ranges, and
  `Expand` walks the per-segment adjacency index once per hop for the whole
  distinct frontier; a closure is a level-synchronous multi-source
  breadth-first search;
* node versions under Σ come from identity point reads for a few ids, or
  from one pass over the believed node rows for many;
* properties are read through **typed property columns**: each distinct
  stored payload is parsed once per key, never once per binding row;
* a join on a whole scan either **seeks** its left keys (through the
  adjacency index or point reads) or **scans** and joins through a
  compressed-sparse-row index of the smaller side's keys, whichever the
  measured cost model picks for the key count; a hash join covers the rest;
* aggregates fold with exact integer sums and a correctly rounded float sum;
  the order is a top-k selection and a sort of the page alone, and strings
  are built for the returned page only;
* large frontiers, joins, scans and folds run in chunks on the worker pool
  (`GNOMON_MATCH_THREADS`, from `GNOMON_MATCH_PAR_ROWS` rows), merged in
  order, so every width gives the same rows in the same order.

The engine **declines** a plan, and the reference evaluator answers it,
for a node or expression form it does not implement and for any row that
would make the evaluator raise (division by zero, a non-canonical uid cast,
an integer outside int64, a comparison across type classes): the error is
left to the one implementation that defines it.

Under a window, or a belief mode other than `current`, walks and
sub-patterns follow the **shared-instant rule**: a binding's nodes and edges
must all be valid at one common instant. The shared-instant nodes carry, per
binding, the set of instants (as disjoint intervals) at which everything
bound so far is valid, and narrow it hop by hop; a walk explores (node,
instant set) states breadth first.

## Readers, writers and concurrency

* One writer per store, enforced by `writer.lock`.
* Readers open with `gnomon.open(path, read_only=True)`. A read-only handle
  runs no recovery, writes nothing under the store directory (lazily built
  indexes are kept in memory for that handle), pins its generation, and
  never blocks or is blocked by the writer.
* Inside one process, `Store` serializes write batches; for many concurrent
  writer threads, `gnomon.write.GroupCommitWriter` coalesces submissions
  into shared commits.
* The parallel read stages (the scan planner and row loop, column
  materialisation, the aggregation fold, the reachability, diff, path and
  pattern kernels) run on one persistent worker pool per process: its threads are started
  once and parked between calls, so a fan-out costs a wake-up rather than a
  thread spawn. The caller claims work items alongside the pool threads, so
  concurrent callers on one handle, or a nested call from inside a worker,
  always finish (serially, if every worker is busy) and get the same answer
  as a single-threaded call.
* The width defaults to one worker per available core
  (`GNOMON_SCAN_THREADS` overrides it). A scan stage fans out only above 4M
  candidate rows; the grouped aggregation's own row loop and fold, whose
  per-row cost is several times a select's, fan out from 131,072 rows.
* Decoded segments are cached per process within a byte budget
  (`GNOMON_SEGMENT_CACHE_BYTES`).

## Digests

Several hashes exist, and they answer different questions:

| digest | over | changes when | use |
|---|---|---|---|
| `body_sha` | one segment file's bytes | never (files are immutable) | integrity, index stamps |
| `manifest_sha` | one generation's logical manifest | every commit | snapshot identity, `CURRENT` check |
| **store digest** (`Store.digest()`) | every node and edge version, all fields | any write | "is this exactly the same store?" — two replays of one event log agree |
| **content digest** (`gnomon store digest`) | every version minus `vid`, `tt_s`, `tt_e` | any logical change | "do these two independent loads hold the same data?" |
| `result_digest` | one operator payload | when the answer changes | comparing answers across runs and backends |

The store digest includes transaction times, which come from the wall clock
at commit, so two independent loads of the same events always differ on it.
The content digest drops exactly the clock-derived fields and sorts rows
totally, so it is layout-independent: it is unchanged by compaction or
settle and equal across independent loads. The engine computes it either in
memory or as a streaming external merge sort bounded by a byte budget
(`--streaming`), with identical results.

## Verification

`gnomon store verify` checks every file the current generation names against
its recorded checksums. `--full` (or `gnomon check`) adds the cross-file
invariants: the manifest hash chain across retained generations, dictionary
references, the bi-temporal row invariants, the event log's framing and hash
chain against the applied offset, and the persisted adjacency index. It is
read-only; a damaged store is rebuilt from its event log with
`gnomon replay`.
