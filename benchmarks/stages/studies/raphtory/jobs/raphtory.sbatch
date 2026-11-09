#!/bin/bash
#   RAP_MODE gate    untimed: frozen logs, Gnomon's C4 store, Raphtory's load, answer check of both, verdict; Raphtory's stores persisted. a shared node.
#   RAP_MODE timed   Gnomon and Raphtory in one job, two rotations, Gnomon's engine-time pass. Whole RM node. RAP_GATE names the gate job's result directory.
#   RAP_MODE smoke   both, on RAP_POINTS and the smoke parameter files; results under smoke-*, stores under smoke-stores. Not results.
#   RAP_MODE cause   untimed check of the differing reach answers on stored data; RAP_GATE names the gate job's results. a shared node. RAP_CAUSE_STORES defaults to the stage's stores.
#   RAP_POINTS       "wl:tag ...", default w1:K1M w1:K10M w1:K10M-d0 w1:K10M-d10 w2:K10M w2:K10M-d0
# Exit codes: 2 missing input, 3 guard, 4 stop rule, 5 time limit, 8 failed window. A differing answer is excluded from timing and printed; the operator reads the verdict before submitting timed.
#SBATCH -N 1
#SBATCH -J c4-raphtory
source ${NEXT_STAGE_ROOT:?}/src/benchmarks/stages/studies/lib.sh
source $REPO/$STG/body.sh
guard; hostinfo
: "${RAP_MODE:?gate, timed or smoke}"
sm=""; [ "$RAP_MODE" = smoke ] && sm=smoke-; [ -n "${RAP_SMOKE_CAUSE:-}" ] && sm=smoke-
O=$NEXT_STAGE_ROOT/results/$sm$RAP_MODE-$SLURM_JOB_ID
run_body $REPO/$STG/raphtory/jobs/raphtory_main.sh $O "cp -au ${LOCAL:?}/rapres/. $O/"
