#!/bin/bash
# W3 SF10 component study on C4 (audit #8b), whole RM node. W3ABL_POINT SF10 (a smoke: SF1, W3ABL_SMOKE=1), W3ABL_QUERIES, W3ABL_N, W3ABL_ARMS, W3ABL_REF (C4's recorded timed block directory).
#SBATCH --exclusive
#SBATCH -N 1
#SBATCH --ntasks-per-node=128
#SBATCH --signal=B:USR1@900
#SBATCH -J c4-w3abl
source ${NEXT_STAGE_ROOT:?}/src/benchmarks/stages/studies/lib.sh
source $REPO/$STG/body.sh
guard; hostinfo
O=$NEXT_STAGE_ROOT/results/${W3ABL_SMOKE:+smoke-}w3abl-$SLURM_JOB_ID; mkdir -p $O
$PYV $TOOLS/ing_rss.py $O/rss.jsonl 5 & RSS=$!
trap 'kill $RSS 2>/dev/null' EXIT
export ROOT=${LOCAL:?}/w3abl
run_body $REPO/$STG/w3abl/w3abl_main.sh $O "mkdir -p $O && cp -au $ROOT/res/. $O/"
