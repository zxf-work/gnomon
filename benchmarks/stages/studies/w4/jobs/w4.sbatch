#!/bin/bash
# W4 on C4, one correction density per job, whole RM node.
#   W4_STEPS gate,timed (default); W4_SYSTEMS default "gnomon duckdbsql64 duckdbsql32 postgres neo4j memgraph" (the list;
# The protocol through the kit's runner: prime, 2 warm-ups + 10 timed, 1 + 3 above 1 s, 10 s ceiling, censor-once, two
# rotations with the order reversed, the engine-time pass for Gnomon, DuckDB (SQL), PostgreSQL. A limit shorter than the
# signal offset stops the job at once: a smoke passes --signal=B:USR1@60.
#SBATCH --exclusive
#SBATCH -N 1
#SBATCH --ntasks-per-node=128
#SBATCH --signal=B:USR1@900
#SBATCH -J c4-w4
source ${NEXT_STAGE_ROOT:?}/src/benchmarks/stages/studies/lib.sh
source $REPO/$STG/body.sh
guard; hostinfo
: "${W4_POINT:?}"
O=$NEXT_STAGE_ROOT/results/${W4_SMOKE:+smoke-}$W4_POINT-$SLURM_JOB_ID
run_body $REPO/$STG/w4/jobs/w4_main.sh $O "cp -au ${LOCAL:?}/w4res/. $O/"
