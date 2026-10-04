# Benchmarks

Three scripts reproduce the engine measurements. Run them from a checkout
with the development install (`maturin develop --release`, see
[install](install.md)): the scripts import the package from the checkout
they live in.

| script | what it does |
|---|---|
| `scripts/build_synth_store.py` | generates the synthetic workload and loads it into a native store, streamed, timing ingest **per phase**, and writes a build record |
| `scripts/eval_harness.py` | runs the **query registry** — 19 named queries over the 15 operators — with warmups and repetitions, and records median/p95 latency and a hash of every answer |
| `scripts/freeze_synth_log.py` | writes one synthetic event log to a stable path, so several runs (or two engine builds) can replay identical events |

Receipts are JSON and validate against
`benchmarks/schema/result_manifest.schema.json`
(`python scripts/check_result_manifest.py <file>`).

## The synthetic workload

Deterministic from `(n_entities, seed)`: one edge event per tick, endpoints
drawn with splitmix64, a node population and an edge lifetime that both grow
with N (so the average degree stays constant), 50-node communities with 70%
of edges inside a community, one 10x burst band, and a second belief epoch —
a node assertion, corrections of edges and of one node, partial
mid-interval corrections, and retractions. `--seed 0` reproduces the
in-memory generator `eval_harness.py` uses, so stores from either path have
the same content digest.

## Ingest, with a per-phase breakdown

```bash
python scripts/build_synth_store.py --n-entities 100000 --out runs/synth-100k
```

The store is written to `--out`, with `build-record.json` beside it. The
build record carries:

* the parameters, the wall time of the bulk phase and of each finalisation
  step (compaction, gc, stats, digest), with RSS before and after each;
* `ingest_phase_totals`: ingest time split into exclusive phases, each with
  its seconds and share — event generation and driver bookkeeping on the
  client side; the Python row build and batch conversion; the PyO3 call
  boundary; the write-ahead event-log append; inside the engine, staging,
  the endpoint existence probe, correction resolution, sealing (sort,
  encode, write, fsync), compaction, and commit publication (manifest and
  fsyncs) — plus the residual no phase claims (typically well under 1%);
* one record per compaction (rows rewritten, segments before and after,
  wall time), peak memory by class (anonymous heap vs. file-mapped), and the
  store and content digests.

The console prints the phase table at the end, for example (100k
entities on a 32-core VM):

```
ingest_phase_totals: ingest 25.5s, residual 0.06%
   58.41%       14.87s  commit publish (manifest, fsyncs)
   16.98%        4.32s  event-log append
   15.11%        3.84s  seal (sort, encode, write, fsync)
    4.74%        1.21s  Python row build and batch conversion
    2.08%        0.53s  compaction
    ...
```

The store is written in many small batches at the defaults (`--batch 250`),
so each batch pays a durable commit; larger batches pay fewer of them. Useful options:

| option | effect |
|---|---|
| `--batch N` | write-batch size of the bulk phase (default 250) |
| `--compact-every N` | `0`: compact incrementally whenever the engine’s own trigger says so (the setting for large builds); positive (default 1,000,000): whole-store compaction every N ops; negative: no compaction until the end |
| `--digest {full,streaming,none}` | which digests to compute at the end (`full` below 10M entities, `none` at and above) |
| `--phase-accounting off` | run with no timing wrapper at all |
| `--stop-at-ops N`, `--resume` | split a long build across processes |

The engine timers can be compiled out with `--no-default-features` (see
[install](install.md)) for an overhead comparison.

## Queries

```bash
# a fresh in-memory dataset, loaded and queried in one process
python scripts/eval_harness.py --scale 100000 --json runs/registry-100k.json

# an existing store, exactly as it was built (compacted layout and all)
python scripts/eval_harness.py --store runs/synth-100k \
    --log runs/synth-100k/eventlog.jsonl --json runs/registry-store-100k.json
```

Protocol: 5 warmup calls, then 30 measured repetitions for a query whose
first call took under a second and 10 otherwise; the receipt records the
median, the p95 and every raw timing. `--systems native,duckdb` also runs the
DuckDB reference backend and checks that both return identical answer
hashes. `--skip-cost-check` bypasses the cost guardrail, which is needed for
some window-bounded queries on stores of 100M rows and more (see
[limitations](limitations.md)).

| query | operator | question |
|---|---|---|
| `hist.single` | `entity_history` | all versions of one node |
| `hist.asof` | `entity_history` | the same, under the pre-correction belief state |
| `snap.hop2` | `snapshot_subgraph` | 2-hop neighbourhood at an instant |
| `diff.global` | `diff_snapshots` | global difference between two instants |
| `reach.window` | `temporal_reachability` | time-respecting reachability in a window |
| `paths.k` | `temporal_paths` | k time-respecting paths |
| `series.count` | `graph_metric_timeseries` | bucketed event rate over the whole extent |
| `burst.zscore` | `burst_detection` | burst flags over the same buckets |
| `nbr.evolution` | `neighborhood_evolution` | neighbours gained and lost |
| `coactive.narrow` | `co_active` | interval join between two edge sets |
| `resolve.substr` | `resolve_entities` | entity resolution by substring |
| `agg.rel_bucket` | `aggregate_events` | count and distinct destinations by relation × time bucket |
| `motif.filtered` | `count_temporal_motifs` | δ-motif count over a filtered node set |
| `resolve.exact` | `lookup_name` | exact name lookup through the name index |
| `resolve.prefix` | `lookup_name` | name-prefix lookup through the name index |
| `series.count.w10` | `graph_metric_timeseries` | event rate over a fixed 10-tick window |
| `agg.rel_bucket.w10` | `aggregate_events` | grouped aggregation over the fixed window |
| `burst.zscore.w10` | `burst_detection` | burst flags over the fixed window |
| `diff.belief` | `diff_belief` | versions whose belief changed since the first epoch, in the fixed window |

The `*.w10` variants keep the window at a fixed absolute width, so their
cost should not grow with the store; the whole-extent queries grow by
construction. `hist.single`, `hist.asof`, `snap.hop2` and `coactive.narrow`
embed a transaction time in their answer, so their hashes differ between two
independently generated datasets; replay one frozen log
(`scripts/freeze_synth_log.py`, then `--log`) when comparing answers.

Two more measurement modes share `--store`: `--window-start 0.1,0.5,0.9`
reports the interval-row staircase’s block-skip fraction at several window
positions, and `--as-of-distance 1,10,100` sweeps `hist.asof` across belief
times that many generations back from the log’s tail.

## Small and large runs

| size | command | needs |
|---|---|---|
| 100k entities | `build_synth_store.py --n-entities 100000 --out runs/synth-100k` | a laptop; well under a minute |
| 1M | `--n-entities 1000000 --batch 250 --compact-every 0` | minutes |
| 10M | `--n-entities 10000000 --batch 250 --compact-every 0` | a server; tens of minutes, a few GB of RAM |
| 100M and up | the same, plus `--digest none` (the default there) | a large server: hours, tens of GB of RAM and disk |

For latency numbers on a large store:

1. build it with `--compact-every 0`;
2. optionally run `gnomon store settle --store <dir>` and
   `gnomon store gc --store <dir> --keep 2` for the read-optimised layout,
   and `gnomon store build-indexes --store <dir>` so the first queries do
   not pay for index builds;
3. run `eval_harness.py --store <dir> --log <dir>/eventlog.jsonl`, with
   `GNOMON_SEGMENT_CACHE_BYTES` set explicitly and recorded.

Run timed measurements on a quiet machine and compare configurations only
as interleaved pairs on the same host; the receipts record the host, the
CPU count and the commit so that numbers from different hosts are never
silently compared.
