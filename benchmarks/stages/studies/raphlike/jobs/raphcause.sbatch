#!/bin/bash
#SBATCH -N 1
#SBATCH -J c4-raphcause
source ${NEXT_STAGE_ROOT:?}/src/benchmarks/stages/studies/lib.sh
source $REPO/$STG/body.sh
guard; hostinfo
O=$NEXT_STAGE_ROOT/results/cause-w5-$SLURM_JOB_ID
run_body $REPO/$STG/raphlike/jobs/raphcause_main.sh $O "cp -au ${LOCAL:?}/rcres/. $O/"
