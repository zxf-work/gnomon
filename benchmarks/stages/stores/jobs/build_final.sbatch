#!/bin/bash
#SBATCH -J ps-build
#SBATCH -N 1
#SBATCH --ntasks-per-node=16
#SBATCH -t 02:00:00
#SBATCH -o build-%j.out
# $P/srcf (branch prep-stores-final = prep-stores + the tag), its own .venv, offline (cargo vendor, uv cache: no new
# downloads), and record the commit and the _engine sha256 (kit/w6/jobs/build.sbatch's recipe).
set -uo pipefail
source ${GNOMON_BENCH_ROOT}/prep-stores/site.env
ENG=b3f10aeb; R=$P/srcf; cd $R; unset CARGO_TARGET_DIR; export CARGO_NET_OFFLINE=true UV_OFFLINE=1
echo "=== build $(git rev-parse HEAD) engine=$ENG host=$(hostname) $(date -u +%FT%TZ)"
git merge-base --is-ancestor $ENG HEAD || { echo "HEAD does not contain the engine"; exit 1; }
[ -z "$(git diff --stat $ENG HEAD -- gnomon crates Cargo.toml Cargo.lock pyproject.toml)" ] || { echo "engine sources differ from $ENG"; exit 1; }
uv venv --python 3.12 .venv 2>&1 | tail -1
source .venv/bin/activate
uv sync --frozen --active --extra eval --extra duckdb 2>&1 | tail -2
uv pip install maturin 2>&1 | tail -1
maturin develop --release --extras eval,duckdb 2>&1 | tail -3
[ ${PIPESTATUS[0]} = 0 ] || { echo "BUILD FAILED"; exit 1; }
SO=$(python -c "import gnomon._engine as e; print(e.__file__)") || exit 1
{ echo $SO; git rev-parse HEAD; git rev-parse $ENG; sha256sum $SO; } > $P/engine-build-$ENG.txt
cat $P/engine-build-$ENG.txt
python -c "import gnomon, duckdb, neo4j, psycopg, pandas, pyarrow; print('imports ok', duckdb.__version__, neo4j.__version__)"
echo "=== done $(date -u +%FT%TZ)"
