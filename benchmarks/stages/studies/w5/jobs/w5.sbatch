#!/bin/bash
# W5_SMOKE=1, results under smoke-*); W5_BASE the recorded answer-check run of the three baselines on the same log and parameter files
#; W5_FAMS registry families (a smoke's subset); W5_MATCH=0 skips the match patterns, W5_SYSTEMS narrows the systems, in a smoke.
# Protocol: as the row: prime per window, 2 warm-ups + 10 timed (1 + 3 above 1 s), 10 s ceiling, two rotations with the
# system order reversed, DuckDB (SQL) at 64 and 32 threads, the engine-time pass for Gnomon, DuckDB (SQL), PostgreSQL. A limit shorter
# than the signal offset stops the job at once: a smoke passes --signal=B:USR1@60.
#SBATCH --exclusive
#SBATCH -N 1
#SBATCH --ntasks-per-node=128
#SBATCH --signal=B:USR1@900
#SBATCH -J c4-w5
source ${NEXT_STAGE_ROOT:?}/src/benchmarks/stages/studies/lib.sh
source $REPO/$STG/body.sh
guard; hostinfo
: "${W5_TAG:?}"
O=$NEXT_STAGE_ROOT/results/${W5_SMOKE:+smoke-}$W5_TAG-$SLURM_JOB_ID
run_body $REPO/$STG/w5/jobs/w5_main.sh $O "for d in ${LOCAL:?}/w5/results-*; do mkdir -p $O/\${d##*/results-}; cp -au \$d/. $O/\${d##*/results-}/; done"
