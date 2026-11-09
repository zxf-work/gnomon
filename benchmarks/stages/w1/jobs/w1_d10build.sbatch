#!/bin/bash
# W1 timed stage, job B0 (untimed store build): the Gnomon-on-DuckDB-storage store at K10M-d10, by the v3 load method
# (window.sh load: the frozen log replayed into native, the duckdb store loaded from the native store), on the final
# job). Untimed: the store is the input of the gate and timed jobs; its load time (shared node) is recorded but is
# not comparable with v3's whole-node 12,269 s. The store is persisted only after the load returned rc=0 and its
# record shows a complete load (no "load exceeded budget").
#SBATCH -N 1
#SBATCH --ntasks-per-node=32
#SBATCH -t 10:00:00
#SBATCH --signal=B:USR1@600
#SBATCH -J w1-d10build
#SBATCH -o %x-%j.out
source ${W1_STAGE_ROOT:-${GNOMON_BENCH_ROOT}/final/w1}/src/benchmarks/stages/w1/jobs/w1_lib.sh
guard; hostinfo; say "B0 start"
OUT=$W1/results/b0-$SLURM_JOB_ID; mkdir -p $OUT
setup_root ${LOCAL:-${TMPDIR:-/tmp}}/w1b0; useroot build
trap 'echo "=== time limit near $(utc): syncing (store NOT persisted)"; "$KIT/lib/servers.sh" stopall; syncall; exit 0' USR1 TERM
getlogs K10M-d10
proto_v3; point_env K10M-d10
export EXTCMP_LOAD_BUDGET_S=34200
(
  "$KIT/lib/window.sh" load 10000000 K10M-d10 - - native; echo "native load rc=$?"
  "$KIT/lib/window.sh" load 10000000 K10M-d10 - - duckdb; echo "duckdb load rc=$?"
) &
wait $!
syncall
R=$RES/K10M-d10/load-duckdb.json
if load_ok $R; then
  save $STORES duckdb-K10M-d10 K10M-d10 && cp -a $R $STORE_DIR/K10M-d10/load-duckdb.json && \
    echo "{\"store\": \"duckdb-K10M-d10\", \"job\": \"$SLURM_JOB_ID\", \"node\": \"$(hostname)\", \"partition\": \"a shared node x 32\", \"persisted_utc\": \"$(utc)\"}" > $STORE_DIR/K10M-d10/duckdb-K10M-d10.provenance.json || \
    { echo "STORE SAVE FAILED"; syncall; exit 6; }
else
  echo "duckdb K10M-d10 load NOT complete: store not persisted"; cat $R 2>/dev/null | head -c 600; echo
  syncall; say "B0 done (store NOT persisted)"; exit 5
fi
syncall; say "B0 done"
