# Install and build

Gnomon is a mixed Rust/Python project: a Rust engine (`crates/gnomon-core`),
its Python binding (`crates/gnomon-py`, built as the extension module
`gnomon._engine`) and the Python package `gnomon`. It is developed and
tested on Linux x86-64.

## Requirements

| tool | version | notes |
|---|---|---|
| Rust | 1.98.1 | pinned in `rust-toolchain.toml`; `rustup` installs it on first use |
| Python | 3.11 or newer | tested with 3.12 |
| maturin | 1.7 or newer, below 2.0 | builds the extension and the wheel |
| a C toolchain | any recent `cc` | needed by the Rust build for linking |

Python runtime dependencies are `numpy` and `jsonschema`. The optional
DuckDB reference backend needs `duckdb` and `pyarrow` (the `duckdb` extra);
the test suite needs the `dev` extra.

## Rust engine only

```bash
cargo build --release          # builds gnomon-core and the binding crate
cargo test --release           # engine unit and integration tests
```

`CARGO_TARGET_DIR` may point anywhere; nothing in the build depends on the
default `target/`.

## Python package (development install)

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install --upgrade pip
pip install maturin
maturin develop --release --extras dev     # builds gnomon._engine into the venv
python -c "import gnomon, gnomon._engine; print(gnomon.__version__)"
```

`maturin develop` compiles the extension and installs the package in
editable mode, so edits to Python files take effect without a rebuild;
edits to Rust files need `maturin develop` again.

## Building a wheel

```bash
maturin build --release --out dist
pip install dist/gnomon-*.whl
```

The wheel contains the compiled engine; installing it needs no Rust
toolchain.

## Build features

The engine carries per-phase ingest timers (`ingest-timers`, on by
default). Building with them compiled out:

```bash
cargo build --release --no-default-features
cargo test --release --no-default-features
```

The Python extension reports which way it was built in
`gnomon._engine.build_info()`.
