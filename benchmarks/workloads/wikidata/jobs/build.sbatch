#!/bin/bash
#SBATCH -J w5-build
#SBATCH -N 1
#SBATCH --ntasks-per-node=16
#SBATCH -t 02:00:00
#SBATCH -o build-%j.out
set -uo pipefail
P=${GNOMON_BENCH_ROOT}/w5-wikidata
source $P/site.env
cd $EXTCMP_REPO; unset CARGO_TARGET_DIR
[ -d .venv ] || uv venv -q --python 3.12 .venv
source .venv/bin/activate
uv sync --frozen --active --extra eval --extra duckdb 2>&1 | tail -3
uv pip install maturin 2>&1 | tail -1
maturin develop --release --extras eval,duckdb 2>&1 | tail -5
so=$(python -c "import gnomon._engine as e; print(e.__file__)") || exit 1
{ echo $so; git rev-parse HEAD; git rev-parse 2036504b; sha256sum $so; } > $P/engine-build.txt
cat $P/engine-build.txt
python -c "import duckdb; print('duckdb', duckdb.__version__)"
