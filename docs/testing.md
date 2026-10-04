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
aggregation caching and pruning, and the segment-size model. Some tests in
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
* the benchmark scripts' own logic (`scripts/`), at tiny sizes.

To run the same suite against the DuckDB reference implementation of the
storage interface (needs the `duckdb` extra):

```bash
GNOMON_TEST_BACKEND=duckdb python -m pytest tests
```

Tests that exercise only native-engine features skip or select the native
backend themselves.
