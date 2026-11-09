#!/bin/bash
#SBATCH -N 1
#SBATCH -J c4-raphlike
source ${NEXT_STAGE_ROOT:?}/src/benchmarks/stages/studies/lib.sh
source $REPO/$STG/body.sh
guard; hostinfo
: "${RL_MODE:?}"
sm=""; [ "$RL_MODE" = smoke ] && sm=smoke-
O=$NEXT_STAGE_ROOT/results/$sm$RL_MODE-$SLURM_JOB_ID
run_body $REPO/$STG/raphlike/jobs/raphlike_main.sh $O "cp -au ${LOCAL:?}/rlres/. $O/"
