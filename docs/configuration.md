# Configuration

The engine needs no configuration. Every knob below has a default chosen by
measurement, and every one is read from the environment of the process that
opens the store. Byte budgets accept `0` for “unbounded”. A value the engine
does not understand is treated as unset: a tuning knob never makes a read
fail.

## Memory budgets

| variable | default | bounds |
|---|---|---|
| `GNOMON_SEGMENT_CACHE_BYTES` | half of RAM, capped at 32 GiB | decoded segment columns kept in the per-process segment cache (anonymous heap only; the file mapping itself is not charged) |
| `GNOMON_POSTINGS_BYTES` | min(RAM/8, 16 GiB) | per-segment identity postings used by point reads and corrections |
| `GNOMON_VIDFILTER_BYTES` | min(RAM/64, 4 GiB) | mapped identity-filter files per handle |
| `GNOMON_SETTLE_BYTES` | 16 GiB | input bytes one `settle` merge group spans, and so settle’s peak memory (`--budget-bytes` overrides per call) |
| `GNOMON_DIGEST_BYTES` | 2 GiB | the sort buffer of the streaming content digest (`--budget-bytes` overrides per call) |
| `GNOMON_FOLD_BYTES` | 1/256 of the segment-cache cap | concurrent per-worker partial group tables of a grouped aggregation; it caps the fold width |
| `GNOMON_CHUNK_RESIDENT_BYTES` | 2 GiB | rows a single seal holds in memory before it flushes |
| `GNOMON_MATCH_PROPS_BYTES` | 1 GiB | the typed property columns the pattern executor keeps per handle (each distinct stored payload parsed once per key) |
| `GNOMON_TT_E_COLUMN_BYTES` | 256 MiB | the cached per-row effective-`tt_e` columns traversals decide belief from (8 bytes per row of a segment with closes) |

## Parallelism

The parallel read stages run on one persistent, process-wide worker pool
(see [architecture](architecture.md#readers-writers-and-concurrency)). These
variables are read per call, so a harness can sweep them in one process.

| variable | default | meaning |
|---|---|---|
| `GNOMON_SCAN_THREADS` | one per available core | worker width of the parallel scan and aggregation stages; a positive integer, clamped to 64. Widths below 4 run serial |
| `GNOMON_PARALLEL_MIN_ROWS` | 4,000,000 | candidate rows below which a scan stage stays serial |
| `GNOMON_AGG_PARALLEL_MIN_ROWS` | 131,072 | rows below which a grouped aggregation’s own two stages (the row loop of its select and its fold) stay serial; other scans keep `GNOMON_PARALLEL_MIN_ROWS` |
| `GNOMON_ROWS_PER_UNIT` | derived | rows per parallel work unit (overrides the derived size) |
| `GNOMON_UNITS_PER_THREAD`, `GNOMON_MIN_ROWS_PER_UNIT` | 4, 65,536 | the inputs to the derived unit size |
| `GNOMON_MATCH_THREADS` | 0 (the available cores) | threads the pattern and path kernels split a large frontier, join or scan over, 1 to 64; read at open. Every width gives the same rows in the same order |
| `GNOMON_MATCH_PAR_ROWS` | 131,072 | rows from which those kernels split a row loop over the threads; read at open |

## Commit path

| variable | default | meaning |
|---|---|---|
| `GNOMON_SMALL_COMMIT` | off | `1` turns on the small-commit path ([architecture](architecture.md#the-small-commit-path)): a transaction is acknowledged once its event-log record is fsynced, and generations are published in groups. Read at open |
| `GNOMON_SMALL_COMMIT_N` | 64 | transactions per published generation on that path |
| `GNOMON_SMALL_COMMIT_MS` | 50 | milliseconds a transaction may wait unpublished on that path |

The small-commit path changes no answer, no event-log byte and no digest;
it changes only how soon a reader **in another process** sees a commit (by
at most `GNOMON_SMALL_COMMIT_N` transactions or `GNOMON_SMALL_COMMIT_MS`
milliseconds). The writing handle always reads its own commits.
`store.adapter.small_commit()` reports what the open handle runs with.

## Layout and indexes

| variable | default | meaning |
|---|---|---|
| `GNOMON_MANIFEST_CHECKPOINT_EVERY` | 512 | generations between full manifest checkpoints |
| `GNOMON_PIN_MAX_SEGMENTS` | 16,384 | largest live-segment count for which a reader maps its whole generation at open |
| `GNOMON_VIDFILTER` | on | `0` stops building identity filters at seal time (they are then built lazily, and existing ones stay in use); `ignore` is a design switch, below |
| `GNOMON_TCSR` | `rust` | `python` selects the whole-store NumPy adjacency index instead of the per-segment one (and leaves `idx/tcsr/` untouched); `scan` is a design switch, below |
| `GNOMON_SEAL_ADAPTIVE` | on | `0` disables the measured seal-size correction |

## Design switches

Each switch below turns **one** engine design off and keeps every answer
identical, so the cost of a design can be measured by running with and
without it. Unset, every design is in force: that is the default engine.
The tests hold every switch to that contract: every query of the benchmark
registry, plus extra probes, must hash identically with the switch set, and
an engine work counter must move the way the switch says
(`crates/gnomon-core/src/ablation_tests.rs`,
`tests/test_ablation_switches.py`).

Read-time switches are read **once per handle, when it opens**, not per call:
set them before `gnomon.open`. `store.adapter.designs()` reports the designs
the open handle actually runs with, and the benchmark drivers record it
together with the environment (see [benchmarks](benchmarks.md#design-switches)).

| design | switch | read | what runs instead |
|---|---|---|---|
| belief on row ranges: visibility computed from `tt_s` runs or dictionary codes with per-block bounds, minus the segment’s sorted closes | `GNOMON_RANGE_VISIBILITY=0` | at open | each row the query can use gets a per-row belief test (its `tt_s`, its effective `tt_e`) |
| window-first visibility: belief computed for the query’s row range only | `GNOMON_WINDOW_FIRST=0` | at open | belief for the whole segment, then clipped to the window, in the scan planner, the belief diff, version history and traversal |
| segment pruning on manifest and header bounds (valid time, belief time, relation names, key range) | `GNOMON_SEGMENT_PRUNE=0` | at open | every candidate segment is opened and planned |
| per-segment temporal adjacency index | `GNOMON_TCSR=scan` | at open | each traversal call scans the endpoint columns of every candidate segment once for its frontier; no index is loaded, built or written |
| identity filter on the correction path (reads) | `GNOMON_VIDFILTER=ignore` | at open | no filter is consulted; every segment’s identity postings are asked. Filters are still built |
| believed-node count for the ingest endpoint check | `GNOMON_EXISTENCE_PROBE=walk` | at open | the endpoint check walks node postings or node versions instead of looking up a count |
| seed-driven snapshot: `snapshot_subgraph` expands from its seeds, one filtered scan per hop | `GNOMON_SEED_EXPANSION=0` | at open | the whole snapshot at the instant, then a breadth-first search over it |
| adjacency postings for a scan filtered to a few nodes (at most 1,024 ids) | `GNOMON_ADJ_FILTERED_SCAN=0` | at open | every row of each segment’s believed, window-clipped ranges gets the endpoint test |
| count by time bucket from believed row ranges | `GNOMON_AGG_RANGE_COUNT=0` | at open | counts come from selection vectors and the per-row fold |
| reachability kernel in the engine | `GNOMON_REACH_KERNEL=python` | at open | `temporal_reachability` runs its NumPy fixpoint and sorts every reached row in Python |
| snapshot-diff kernel in the engine | `GNOMON_DIFF_KERNEL=python` | at open | `diff_snapshots` materialises both point states and builds its identity maps in Python |
| raw-record bulk ingest | `GNOMON_INGEST_RAW=0` | at open | every `ingest_events` op is staged by the Python row build |
| `vt_e` staircase: the prefix cut below a window’s start, written for interval segments and derived at open for event segments that store `vt_e` | `GNOMON_NO_STAIRCASE=1` | at open | no segment’s staircase is consulted and none is derived; every row of the window’s `vt_s` range gets the per-row `vt_e` test. Only the exact value `1` turns it off |
| pattern-match executor in the engine | `GNOMON_MATCH_KERNEL=python` | at open | `match` runs its compiled plan in the plan IR's NumPy reference evaluator (`gnomon/plan/eval`) |
| temporal-paths kernel in the engine | `GNOMON_PATHS_KERNEL=python` | at open | `temporal_paths` enumerates every path depth first in Python, sorts them and keeps `k` |
| effective-`tt_e` column for traversal belief | `GNOMON_TT_E_COLUMN=0` | at open | traversals decide belief on a segment with closes by the per-row test or the segment's believed ranges, built per call, instead of one comparison per adjacency row against the cached column |
| time-column encoding: `tt_s` as runs or as a dictionary with per-block bounds, and the adaptive column codecs | `GNOMON_TIME_CODEC=runs` or `=raw` | **write time**, per segment written | `runs`: `tt_s` as a run list at every density. `raw`: `tt_s`, `vt_s` and `vt_e` stored raw; such segments are segment format 2. A store may mix segments written either way, and every build reads all of them |

For the boolean switches, `0` and `false` (any case) turn the design off;
for the others, the value shown does. The architecture document describes
each design: [architecture](architecture.md).

## Query admission

| variable | default | meaning |
|---|---|---|
| `GNOMON_TIME_COEFF_SCALE` | 1.0 | scales the cost guardrail’s per-operator time coefficients for a faster or slower host |

The coefficients price the portable routes. Where an engine kernel answers a
call, the operator's cost function prices that kernel's own work instead
(`gnomon.temporal.guardrails.cost_routes`), so admission refuses what would
be slow on the route the call actually takes.

For `snapshot_subgraph`, `diff_snapshots`, `resolve_entities`,
`neighborhood_evolution` and `co_active` on the native engine, the estimate
is the lower of the whole-store price and one from what the call can touch:

* the rows left after zone-map pruning: the manifest records each segment's
  least `vt_s`, so a read at instant `t` can touch only the rows of the
  segments that start at or before `t`, plus the rows no segment holds yet
  (`guardrails.cost_zones`, `zone_rows`);
* for a seeded snapshot, the seeds, the hops and the store's largest
  out-degree: each level reads at most twice the largest out-degree per
  frontier vertex, and all levels together read each version at most twice
  (`guardrails.expansion_rows`);
* for `resolve_entities`, the node versions and the identity dictionary
  only; for the point-anchored reads of `neighborhood_evolution` and
  `co_active`, the incident versions of the largest out-degree, bounded by
  the zone rows.

Each row count is converted to time at a fixed per-operator rate, scaled by
`GNOMON_TIME_COEFF_SCALE`. Without the zone summary (another backend, or a
call with no adapter) or without the engine route, the whole-store price is
used unchanged, so this estimate never refuses a call the whole-store price
admits.

## Diagnostics and testing

| variable | meaning |
|---|---|
| `GNOMON_AGG_EXT=python` | `aggregate_events` calls that read properties or restrict to a cohort take the portable path instead of the engine's extended fold (the paired control for that fold) |
| `GNOMON_PATHS_ROUNDS=0` | bounded path searches read the whole window instead of loading their frontier round by round (paired control) |
| `GNOMON_REACH_FRONTIER=0`, `GNOMON_REACH_FRONTIER_BUDGET=<nodes>,<arcs>` | reachability builds the window's adjacency array for every search instead of first trying a filtered frontier search; the budget after which the frontier search gives up (read once per process) |
| `GNOMON_MATCH_FOLD_ROWS` | rows from which a pattern aggregate folds on the worker pool (default 131,072; the tests set it low to hold the parallel fold to the serial one) |
| `GNOMON_MATCH_TRACE=1` | one line on stderr per node of each plan the engine executes, with its output rows and time (read once per process) |
| `GNOMON_PLAN_TELEMETRY_TIMING=0` | the reference evaluator skips its per-node timing (row counts are always recorded) |
| `GNOMON_SYNTH_ENDPOINTS=powerlaw` | the benchmark harness's synthetic dataset draws edge endpoints from a power law (skewed hubs) instead of uniformly |
| `GNOMON_FOLD_OVERFLOW` | selects the aggregation overflow policy (testing) |
| `GNOMON_LINFOR_LAZY`, `GNOMON_LINFOR_RANGE` | codec-2 decoding experiments (testing) |
| `GNOMON_CRASH_POINT=<name>` | kill the process at a named point in the write or recovery path (crash tests) |
| `GNOMON_TEST_BACKEND` | `native` (default) or `duckdb`: the backend the Python test suite runs against |
| `GNOMON_HYP_EXAMPLES` | examples per property-based test (default 25; larger values for a long sweep) |
| `GNOMON_DIFF_BELIEF_SCALE`, `GNOMON_READONLY_SCALE` | the store sizes of the belief-diff and read-only test modules |
| `GNOMON_ABLATION_FULL=1` | the design-switch tests also cover 0 % and 1 % corrections, a settled-before-corrections store and every pair of switches |
| `GNOMON_TEST_SAMPLING_COST_US` | an artificial per-sample cost for the memory-accounting test |
| `GNOMON_METRICS_PATH` | write group-commit metrics as JSON lines to this file |
| `GNOMON_COMMIT`, `GNOMON_COMMIT_DIRTY` | the commit the benchmark receipts record, for machines without `git` |
| `GNOMON_MEASUREMENT_HOST` | the host name the benchmark receipts compare against |

## Switches of the evaluated engine

The engine this repository ships reads the following switches as well. Each
one is on by default; setting it to `0` (or to the value named) runs the
general path the default stands in for, with the same answers. They exist
for the same reason as the design switches above: to measure what each fast
path is worth and to fall back if one misbehaves.

| switch | what the default does |
|---|---|
| `GNOMON_STORE_WARM_OPEN` | a read-only handle builds at open, in parallel and from one decode per segment, the per-segment structures the first queries would otherwise build one at a time (segment cache, statistics, label counts, adjacency); `GNOMON_STORE_WARM_THREADS` sets its width, `GNOMON_STORE_WARM_POSTINGS` (`cached`, `uncached`) also builds the identity postings |
| `GNOMON_PLAN_PATH` | every operator call runs as a single-leaf plan of the plan IR (`gnomon/plan`): the leaf calls the same kernel with the same arguments, and the envelope carries the plan's metadata; `off` calls the kernel directly, with the same payload and digest |
| `GNOMON_CALL_FAST`, `GNOMON_CALL_MEMO`, `GNOMON_ADMIT_MEMO` | the fixed per-call work of `call_operator` (argument validation, admission, envelope metadata) is memoized per store generation |
| `GNOMON_ARGS_KEY_RUST`, `GNOMON_CANON_RUST`, `GNOMON_CANON_ONEPASS`, `GNOMON_DIGEST_RUST`, `GNOMON_JSON_COPY_RUST` | the envelope's argument key, float canonicalisation, canonical JSON, result digest and deep copies run in the engine instead of in Python |
| `GNOMON_IDENTITY_LOG_COLS`, `GNOMON_NODES_VALID_BATCH` | an identity's belief log and node validity are read from the engine's columns in one call, without building a version object per row |
| `GNOMON_NBR_EVOLUTION_KERNEL`, `GNOMON_NBR_EVOLUTION_ONE_READ` | neighbourhood evolution runs in the engine, over one read of the window |
| `GNOMON_SNAPSHOT_PAGE`, `GNOMON_SNAPSHOT_ONECALL` | a snapshot's frontier is built as a page in the engine, its seed expansion and page in one call |
| `GNOMON_PATHS_PAGE_MEMO`, `GNOMON_PATHS_TR_COLUMNS` | later pages of a paths answer are served from the first page's work; time-respecting paths read columns directly |
| `GNOMON_NEIGHBORS_SPLIT`, `GNOMON_NEIGHBORS_SEEK` | a frontier of hubs is split across the worker pool; `GNOMON_NEIGHBORS_SEEK=bisect` selects the earlier way of locating each vertex's slot in a sparse adjacency (a search over the whole range) |
| `GNOMON_TT_E_COLUMN_EVICT` | the effective-`tt_e` column cache keeps the columns a scan is about to reuse; `lru` restores least-recently-used eviction |
| `GNOMON_MATCH_*` (about forty) | the pattern executor's plan and kernel choices (anchoring, pre-probes, semijoins, top-k into a hop, interval fusion, version evaluation, distinct and join forms, threads); each one's comment in `gnomon/temporal/ops_match.py` or `crates/gnomon-core/src/matcher.rs` names the general path it replaces |
