#!/bin/bash
# Durable commit cost on C4 (audit #7), a shared node x16 in the check form of w6-aeong/jobs/tmg_check.sbatch. Ports derive from the job id. COMMIT_WORK (a frozen build dir; default m320k-d1), COMMIT_NS, COMMIT_SYSTEMS, COMMIT_SMOKE=1 (results under smoke-*).
#SBATCH -N 1
#SBATCH --ntasks-per-node=16
#SBATCH --signal=B:USR1@300
#SBATCH -J c4-commit
source ${NEXT_STAGE_ROOT:?}/src/benchmarks/stages/studies/lib.sh
source $REPO/$STG/body.sh
guard; hostinfo
O=$NEXT_STAGE_ROOT/results/${COMMIT_SMOKE:+smoke-}commit-$SLURM_JOB_ID; mkdir -p $O
$PYV $TOOLS/ing_rss.py $O/rss.jsonl 5 & RSS=$!
trap 'kill $RSS 2>/dev/null' EXIT
export ROOT=${LOCAL:?}/commit
run_body $REPO/$STG/commit/commit_main.sh $O "true"
