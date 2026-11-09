#!/bin/bash
# ING_RUNS P,F,L (default all); G = Neo4j and Memgraph on the full stream (ING_GRAPH: which, default "memgraph neo4j"). The job refuses measured runs until the protocol is in the kit. A limit shorter than the signal
# offset stops the job at once: a smoke passes --signal=B:USR1@60. Exit codes: 2 missing input, 3 guard or unsigned protocol, 5 time limit, 7 failed verification or refused batch, 8 failed load.
#SBATCH --exclusive
#SBATCH -N 1
#SBATCH --ntasks-per-node=128
#SBATCH --signal=B:USR1@900
#SBATCH -J c4-ingest
source ${NEXT_STAGE_ROOT:?}/src/benchmarks/stages/studies/lib.sh
source $REPO/$STG/body.sh
guard; hostinfo
O=$NEXT_STAGE_ROOT/results/${ING_SMOKE:+smoke-}ingest-$SLURM_JOB_ID
run_body $REPO/$STG/ingest/jobs/ingest_main.sh $O "cp -au ${LOCAL:?}/ingres/. $O/"
