# Running the tests

## Rust

```bash
cargo test --release                        # with the ingest-phase timers (default)
cargo test --release --no-default-features  # with the timers compiled out
cargo clippy --all-targets -- -D warnings
```

The engine tests are unit tests next to the code (`crates/gnomon-core/src`)
and integration tests in `crates/gnomon-core/tests`: round trips through the
file formats, byte-for-byte golden segments and identity filters of a
deterministic store, read-only opens, compaction and settle equivalence,
aggregation caching and pruning, the segment-size model, every design
switch (same answer, and a work counter that moves), and concurrent callers
sharing the worker pool. Some tests in
`src/` are marked `#[ignore]` because they measure rather than check; run
them with `cargo test --release -- --ignored` when wanted.

## Python

With the package installed in a virtual environment
([install](install.md)):

```bash
python -m pytest tests
```

The suite runs against the native engine. It includes:

* an independent pure-Python **oracle** (`gnomon/temporal/oracle.py`) and
  property-based tests (Hypothesis) that generate random sequences of
  asserts, retractions and corrections and check every operator's answer
  against the oracle at every belief time;
* storage invariants, metamorphic tests, replay equivalence and crash
  recovery (crash points are injected into a child process through
  `GNOMON_CRASH_POINT`);
* concurrency: a single writer with concurrent readers in other processes,
  writer-lock refusal, group commit, and readers that observe a torn log tail;
* the content digest, its streaming form, settle, compaction and gc;
* scaling guards that compare small and large inputs of the same shape on
  one machine (ratios, not absolute times), so they hold on any host;
* the design switches ([configuration](configuration.md#design-switches)):
  every query of the benchmark registry, plus extra probes, hashes the same
  with each switch set, and the engine kernels (reachability, temporal
  paths, snapshot diff, paged entity resolution, the pattern executor, bulk
  ingest and per-operation writes from the log record) answer exactly as the
  general path they stand in for;
* the engine-level operators: `match`'s compiled plan against an
  independent oracle that reads the operator's argument directly (a hand-
  checked corpus, then random stores and random arguments under every scope
  and belief mode, the shared-instant rule included), and the engine's plan
  executor against the reference evaluator on the same plans; `paths`,
  `state_series`, `diff_belief` and `version_history(ref)` against the
  oracle on both backends; the exact `sum`; the hub kernels against the
  enumerations they replace; the plans built on stores from the social-
  network, financial and TPC-BiH-shaped generators (`gnomon/data`);
* the commit path: the small-commit path's visibility contract, and crash
  points along it showing that durability, the event log and the digests
  are those of the default path;
* the benchmark scripts' own logic (`scripts/`), at tiny sizes.

To run the same suite against the DuckDB reference implementation of the
storage interface (needs the `duckdb` extra):

```bash
GNOMON_TEST_BACKEND=duckdb python -m pytest tests
```

Tests that exercise only native-engine features skip or select the native
backend themselves.

## The comparison kit

The kit's own tests (the parameter-file path, the 10M pruning rule, the
in-process worker's restart, and the tools of the studies) run with the same
environment:

```bash
python -m pytest tests/test_extcmp_p1.py tests/test_r10m.py tests/test_worker_restart.py
python -m pytest benchmarks/stages/studies/tests
```
