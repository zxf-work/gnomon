#!/bin/bash
#   Cells: every cell above 1 s or not answered at 64 or at 32 threads. The W3 kit runner and protocol unchanged: prime per (query, setting), 1 warm-up + 5 (1 + 3 above 1 s),
#   10 s ceiling, 60 s prime kill, censor-once. Blocks: 64 threads control (recorded limit 20GB = 18.6 GiB) then 64 GB; 32 threads 64 GB then control. W3D64_SMOKE=1: results smoke-*.
# Exit codes: 2 missing input, 3 guard, 5 time limit, 7 an answer differs from the recorded one (records kept, printed at once).
#SBATCH -N 1
#SBATCH -J c4-w3d64
export W3_STAGE_ROOT=${NEXT_STAGE_ROOT:?}
source $NEXT_STAGE_ROOT/src/benchmarks/stages/studies/lib.sh
source $REPO/$STG/body.sh
guard; hostinfo
O=$NEXT_STAGE_ROOT/results/${W3D64_SMOKE:+smoke-}w3d64-$SLURM_JOB_ID
run_body $REPO/$STG/w3d64/jobs/w3d64_main.sh $O "cp -au ${LOCAL:?}/w3d64res/. $O/"
