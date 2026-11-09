#!/bin/bash
# Out-of-order logs on C4 (coverage audit item 5), a shared node x32: re-ordered logs, stores, answer gate, then paired windows. No server, so no ports.
# A limit shorter than the signal offset stops the job at once: a smoke passes --signal=B:USR1@60.
#SBATCH -N 1
#SBATCH --ntasks-per-node=32
#SBATCH --signal=B:USR1@600
#SBATCH -J c4-ooo
source ${NEXT_STAGE_ROOT:?}/src/benchmarks/stages/studies/lib.sh
source $REPO/$STG/body.sh
guard; hostinfo
O=$NEXT_STAGE_ROOT/results/${OOO_SMOKE:+smoke-}ooo-$SLURM_JOB_ID; mkdir -p $O
$PYV $TOOLS/ing_rss.py $O/rss.jsonl 5 & RSS=$!
trap 'kill $RSS 2>/dev/null' EXIT
export ROOT=${LOCAL:?}/ooo
run_body $REPO/$STG/ooo/ooo_main.sh $O "mkdir -p $O && cp -au $ROOT/results/. $O/"
