#!/bin/bash
# (a smoke: W5-smallpedia, W5ABL_SMOKE=1, W5_BASE, W5ABL_FAMS, W5ABL_ROTS, W5ABL_ARMS, results under smoke-*). A limit shorter than the signal offset stops the job at once: a smoke passes --signal=B:USR1@60.
#SBATCH --exclusive
#SBATCH -N 1
#SBATCH --ntasks-per-node=128
#SBATCH --signal=B:USR1@600
#SBATCH -J c4-w5abl
source ${NEXT_STAGE_ROOT:?}/src/benchmarks/stages/studies/lib.sh
source $REPO/$STG/body.sh
guard; hostinfo
: "${W5_TAG:?}"
O=$NEXT_STAGE_ROOT/results/${W5ABL_SMOKE:+smoke-}w5abl-$SLURM_JOB_ID; mkdir -p $O
$PYV $TOOLS/ing_rss.py $O/rss.jsonl 5 & RSS=$!
trap 'kill $RSS 2>/dev/null' EXIT
export ROOT=${LOCAL:?}/w5abl
run_body $REPO/$STG/w5abl/w5abl_main.sh $O "mkdir -p $O && cp -au $ROOT/results/. $O/"
