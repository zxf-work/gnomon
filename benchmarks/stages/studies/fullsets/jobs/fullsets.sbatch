#!/bin/bash
#SBATCH -N 1
#SBATCH -J c4-fullsets
source ${NEXT_STAGE_ROOT:?}/src/benchmarks/stages/studies/lib.sh
source $REPO/$STG/body.sh
guard; hostinfo
O=$NEXT_STAGE_ROOT/results/${FS_SMOKE:+smoke-}fullsets-$SLURM_JOB_ID
run_body $REPO/$STG/fullsets/jobs/fullsets_main.sh $O "cp -au ${LOCAL:?}/fsres/. $O/"
