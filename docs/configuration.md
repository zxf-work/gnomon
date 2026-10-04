# Configuration

The engine needs no configuration. Every knob below has a default chosen by
measurement, and every one is read from the environment of the process that
opens the store. Byte budgets accept `0` for “unbounded”.

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

| variable | default | meaning |
|---|---|---|
| `GNOMON_SCAN_THREADS` | min(cores, 16) | worker threads for scans and aggregation (at most 64) |
| `GNOMON_PARALLEL_MIN_ROWS` | 4,000,000 | candidate rows below which a scan stays serial |
| `GNOMON_ROWS_PER_UNIT` | derived | rows per parallel work unit (overrides the derived size) |
| `GNOMON_UNITS_PER_THREAD`, `GNOMON_MIN_ROWS_PER_UNIT` | 4, 65,536 | the inputs to the derived unit size |

## Layout and indexes

| variable | default | meaning |
|---|---|---|
| `GNOMON_MANIFEST_CHECKPOINT_EVERY` | 512 | generations between full manifest checkpoints |
| `GNOMON_PIN_MAX_SEGMENTS` | 16,384 | largest live-segment count for which a reader maps its whole generation at open |
| `GNOMON_VIDFILTER` | on | `0` disables building identity filters at seal time (they are then built lazily) |
| `GNOMON_TCSR` | `rust` | `python` selects the whole-store NumPy adjacency index instead of the per-segment one |
| `GNOMON_SEAL_ADAPTIVE` | on | `0` disables the measured seal-size correction |

## Query admission

| variable | default | meaning |
|---|---|---|
| `GNOMON_TIME_COEFF_SCALE` | 1.0 | scales the cost guardrail’s per-operator time coefficients for a faster or slower host |

## Diagnostics and testing

| variable | meaning |
|---|---|
| `GNOMON_NO_STAIRCASE=1` | disables the interval-row prefix-max cut (for A/B checks; answers are unchanged) |
| `GNOMON_FOLD_OVERFLOW` | selects the aggregation overflow policy (testing) |
| `GNOMON_LINFOR_LAZY`, `GNOMON_LINFOR_RANGE` | codec-2 decoding experiments (testing) |
| `GNOMON_CRASH_POINT=<name>` | kill the process at a named point in the write or recovery path (crash tests) |
| `GNOMON_TEST_BACKEND` | `native` (default) or `duckdb`: the backend the Python test suite runs against |
| `GNOMON_METRICS_PATH` | write group-commit metrics as JSON lines to this file |
| `GNOMON_COMMIT`, `GNOMON_COMMIT_DIRTY` | the commit the benchmark receipts record, for machines without `git` |
| `GNOMON_MEASUREMENT_HOST` | the host name the benchmark receipts compare against |
