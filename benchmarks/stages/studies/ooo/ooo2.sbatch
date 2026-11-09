#!/bin/bash
# Out-of-order logs, explicit-node form, d0 and d10. a shared node x32. OOO_SMOKE=1 OOO_SIZE=1000000: results under smoke-*. A limit shorter than the signal offset stops the job at once: a smoke passes --signal=B:USR1@60.
#SBATCH -N 1
#SBATCH --ntasks-per-node=32
#SBATCH --signal=B:USR1@600
#SBATCH -J c4-ooo2
source ${NEXT_STAGE_ROOT:?}/src/benchmarks/stages/studies/lib.sh
source $REPO/$STG/body.sh
guard; hostinfo
O=$NEXT_STAGE_ROOT/results/${OOO_SMOKE:+smoke-}ooo2-$SLURM_JOB_ID; mkdir -p $O
$PYV $TOOLS/ing_rss.py $O/rss.jsonl 5 & RSS=$!
trap 'kill $RSS 2>/dev/null' EXIT
export ROOT=${LOCAL:?}/ooo2
run_body $REPO/$STG/ooo/ooo2_main.sh $O "mkdir -p $O && cp -au $ROOT/results/. $O/"
