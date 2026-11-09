#!/bin/bash
#SBATCH -J w3-paramgen
#SBATCH -N 1
#SBATCH --ntasks-per-node=32
#SBATCH -t 03:00:00
#SBATCH -o paramgen-%j.out
# W3: LDBC's own parameter curation
# on our raw-mode factor tables; copies of the paramgen dirs, unmodified scripts.
set -uo pipefail
P=${GNOMON_BENCH_ROOT}/w3-snb
source $P/site.env
# separate venv for LDBC paramgen (unpinned upstream: duckdb pytz; iv2 path_selection needs networkit numpy pandas)
[ -d $P/params/venv311 ] || uv venv -q --python 3.11 $P/params/venv311
source $P/params/venv311/bin/activate
# pandas<2: path_selection.py groupby(as_index=False) with an unnamed key adds a column under pandas 2 (PROBLEMS P4)
uv pip install -q duckdb==1.5.4 pytz networkit "numpy<2" "pandas<2" pyarrow 2>&1 | tail -2
uv pip freeze > $P/params/venv-freeze.txt
for SF in ${SFS:-0.1 1 10}; do
  D=$P/data/sf$SF-raw; [ -d $D ] || { echo "no $D"; continue; }
  W=$P/params/work-sf$SF; rm -rf $W/iv2-paramgen; mkdir -p $W $P/params/ldbc-sf$SF/bi $P/params/ldbc-sf$SF/iv2
  cp -r $P/ext/ldbc_snb_bi/paramgen $W/bi-paramgen; mkdir -p $W/bi-paramgen/scratch $W/parameters/parameters-sf$SF
  rm -rf $W/bi-paramgen/scratch/factors; ln -s $D/factors/parquet/raw/composite-merged-fk $W/bi-paramgen/scratch/factors
  echo "=== BI paramgen sf$SF $(date -u +%T)"
  [ -s $P/params/ldbc-sf$SF/bi/bi-1.csv ] || (cd $W/bi-paramgen && SF=$SF python paramgen.py) > $P/logs/paramgen-bi-sf$SF.log 2>&1; echo "rc=$?"; tail -3 $P/logs/paramgen-bi-sf$SF.log
  cp $W/parameters/parameters-sf$SF/* $P/params/ldbc-sf$SF/bi/ 2>/dev/null
  cp -r $P/ext/ldbc_snb_interactive_v2_driver/paramgen $W/iv2-paramgen
  echo "=== IV2 paramgen sf$SF $(date -u +%T)"
  (cd $W/iv2-paramgen && LDBC_SNB_DATA_ROOT_DIRECTORY=$D LDBC_SNB_FACTOR_TABLES_DIR=$D/factors/parquet/raw/composite-merged-fk/ bash scripts/paramgen.sh) > $P/logs/paramgen-iv2-sf$SF.log 2>&1; echo "rc=$?"; tail -3 $P/logs/paramgen-iv2-sf$SF.log
  cp $W/iv2-paramgen/../parameters/* $P/params/ldbc-sf$SF/iv2/ 2>/dev/null || find $W -name "interactive-*.parquet" -exec cp {} $P/params/ldbc-sf$SF/iv2/ \;
  ls $P/params/ldbc-sf$SF/bi | wc -l; ls $P/params/ldbc-sf$SF/iv2 | wc -l
  (cd $P/params/ldbc-sf$SF && find . -type f | sort | xargs sha256sum > SHA256SUMS)
done
