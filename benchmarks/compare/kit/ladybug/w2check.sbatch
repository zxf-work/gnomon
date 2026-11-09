#!/bin/bash
# prep-ladybug: W2 power-law untimed answer check on LadybugDB (registry K1M/K10M and the belief-time family B at
# d0/d1/d10), run_p1.sh: native loads (the reference store the verify reads), then LadybugDB loads,verify, then every LadybugDB cell against the recorded native, DuckDB (SQL) and PostgreSQL cells.
#SBATCH -N 1
#SBATCH --ntasks-per-node=8
#SBATCH -t 06:00:00
#SBATCH -J lb-w2check
#SBATCH -o %x-%j.out
source ${GNOMON_BENCH_ROOT}/ladybug/work/env.sh
set -uo pipefail
say() { echo "=== $* $(date -u +%FT%TZ)"; }
W2=$GNOMON_BENCH_ROOT/w2-powerlaw; KFT=$KIT/tools
export EXTCMP_PROJECT=$LB/val/w2-$SLURM_JOB_ID; OUT=$EXTCMP_PROJECT/campaign/results; mkdir -p $OUT
export EXTCMP_ROOT=${LOCAL:-${TMPDIR:-/tmp}}/lbw2 TMPDIR_OVERRIDE=${LOCAL:-${TMPDIR:-/tmp}}/lbw2/scratch EXTCMP_LADYBUG_BUFFER_POOL=$((10 << 30))
mkdir -p $EXTCMP_ROOT/{results,eventlogs,stores,srv,scratch}
cp -a $W2/check1m/campaign/eventlogs/. $EXTCMP_ROOT/eventlogs/
export EXTCMP_SYNC_HOOK="mkdir -p $OUT && cp -au $EXTCMP_ROOT/results/. $OUT/"
say "W2 LadybugDB check job $SLURM_JOB_ID on $(hostname); ladybug $($PYV -c 'import importlib.metadata as m; print(m.version("ladybug"))')"
for SZ in ${SIZES:-1000000 10000000}; do
  case $SZ in 1000000) PTS="K1M K1M-d0 K1M-d1 K1M-d10"; REC=$W2/check1m/campaign/results ;; *) PTS="K10M K10M-d0 K10M-d1 K10M-d10"; REC=$W2/check10m/campaign/results ;; esac
  PTS=${POINTS:-$PTS}     # a re-run of some points only (e.g. POINTS=K1M after a failed window)
  ( export P1_SIZE=$SZ P1_PARAMS_DIR=$W2/params EXTCMP_P1_CEILING_S=10 EXTCMP_SERVER_LIMIT_S=10 \
           EXTCMP_SERVER_TIMEOUT_S=30 EXTCMP_CALL_CEILING_S=30 EXTCMP_P1_SLOW_S=1.0 EXTCMP_WINDOW_TIMEOUT_S=${WT:-7200}
    for pt in $PTS; do
      P1_POINTS=$pt P1_SYSTEMS="native" "$KIT/run_p1.sh" --steps loads; echo "run_p1 native loads $pt rc=$?"
      # the recorded native answer check: the per-instance gate reads it (the LadybugDB cells are compared below)
      mkdir -p $EXTCMP_ROOT/results/$pt; cp $REC/$pt/verify-native*.json ${REC}-gnomon/$pt/verify-native*.json $EXTCMP_ROOT/results/$pt/ 2>/dev/null
      P1_POINTS=$pt P1_SYSTEMS="ladybug" "$KIT/run_p1.sh" --steps loads,verify; echo "run_p1 ladybug $pt rc=$?"
      rm -rf $EXTCMP_ROOT/stores/*
    done )
  cp -au $EXTCMP_ROOT/results/. $OUT/
  say "compare $SZ"
  for R in $REC ${REC}-gnomon; do [ -d $R ] || continue
    $PYV $KFT/kf_answer_compare.py --new $OUT --old $R --tags ${PTS// /,} --systems ladybug --vs native,duckdbsql,postgres 2>&1 | tee -a $OUT/compare-$SZ.txt | grep -v "^   " | head -80
  done
  rm -rf $EXTCMP_ROOT/stores/*
done
say done
