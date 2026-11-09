#!/bin/bash
#SBATCH -N 1
#SBATCH -J c4-raphfull
source ${NEXT_STAGE_ROOT:?}/src/benchmarks/stages/studies/lib.sh
source $REPO/$STG/body.sh
guard; hostinfo
: "${RF_CAUSE:?}"
O=$NEXT_STAGE_ROOT/results/${RF_SMOKE:+smoke-}fullsets-$SLURM_JOB_ID
run_body $REPO/$STG/raphtory/jobs/raphfull_main.sh $O "cp -au ${LOCAL:?}/rfres/. $O/"
