#!/bin/bash
# matchpath: the same queries, stores and instances through the dedicated operator (native) and through
# MP_MODE smoke | timed. MP_POINTS "wl:tag ..." (default w1:K10M-d0 w2:K10M-d0). Smoke: its own parameter files (params-smoke), results smoke-*.
#SBATCH -N 1
#SBATCH -J c4-matchpath
source ${NEXT_STAGE_ROOT:?}/src/benchmarks/stages/studies/lib.sh
source $REPO/$STG/body.sh
guard; hostinfo
: "${MP_MODE:?}"
sm=""; [ "$MP_MODE" = smoke ] && sm=smoke-
O=$NEXT_STAGE_ROOT/results/$sm$MP_MODE-$SLURM_JOB_ID
run_body $REPO/$STG/matchpath/jobs/matchpath_main.sh $O "cp -au ${LOCAL:?}/mpres/. $O/"
