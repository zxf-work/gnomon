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
| time-column encoding: `tt_s` as runs or as a dictionary with per-block bounds, and the adaptive column codecs | `GNOMON_TIME_CODEC=runs` or `=raw` | **write time**, per segment written | `runs`: `tt_s` as a run list at every density. `raw`: `tt_s`, `vt_s` and `vt_e` stored raw; such segments are segment format 2. A store may mix segments written either way, and every build reads all of them |

For the boolean switches, `0` and `false` (any case) turn the design off;
for the others, the value shown does. The architecture document describes
each design: [architecture](architecture.md).

## Query admission

| variable | default | meaning |
|---|---|---|
| `GNOMON_TIME_COEFF_SCALE` | 1.0 | scales the cost guardrail’s per-operator time coefficients for a faster or slower host |

## Diagnostics and testing

| variable | meaning |
|---|---|
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
