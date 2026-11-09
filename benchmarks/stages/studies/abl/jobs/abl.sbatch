#!/bin/bash
# Ablation job on C4 (W1 or W2), whole RM node; body in abl_main.sh. Environment (sbatch --export):
#   ABL_WL            w1 | w2
#   ABL_SIZE          10000000 (1000000 for a smoke)
#   ABL_PLAN          density:families list, e.g. "0:P,T,S,W,D,N,R,M,B 1:P,T,S,W,D,N,R,M,B 10:P,T,S,W,D,N,R,M,B"
#   ABL_ROTS          rotations (2)
#   ABL_SMOKE=1       run_ablation --smoke: refused above 1M events, label smoke-*, gates bypassed and recorded as bypassed
# A limit shorter than the signal offset stops the job at once: a smoke passes --signal=B:USR1@60 on the sbatch line.
#SBATCH --exclusive
#SBATCH -N 1
#SBATCH --ntasks-per-node=128
#SBATCH --signal=B:USR1@900
#SBATCH -J c4-abl
source ${NEXT_STAGE_ROOT:?}/src/benchmarks/stages/studies/lib.sh
guard; hostinfo
: "${ABL_WL:?}" "${ABL_SIZE:?}" "${ABL_PLAN:?}" "${ABL_ARMS:?}"
case "${ABL_SMOKE:-0}${ABL_GATE_TEST:-0}" in 10|01) sm=smoke- ;; *) sm="" ;; esac
OUT=$NEXT_STAGE_ROOT/results/${sm}$ABL_WL-$SLURM_JOB_ID; mkdir -p $OUT
setup_root ${LOCAL:?}/abl
child=""
on_usr1() { echo "=== time limit near $(utc): stopping, syncing"; [ -n "$child" ] && kill -TERM -- -$child 2>/dev/null; sleep 5
            cp -au $ROOT/results/. $OUT/ 2>/dev/null; echo "STOPPED at the time limit $(utc)" > $OUT/STOPPED; exit 5; }
trap on_usr1 USR1 TERM
export OUT ROOT
setsid bash $REPO/$STG/abl/jobs/abl_main.sh &
child=$!
wait $child; rc=$?
cp -au $ROOT/results/. $OUT/ 2>/dev/null
say "ablation job done rc=$rc"
exit $rc
