#!/bin/bash
#   W5's stores were never persisted (each W5 job built them on ${LOCAL:-${TMPDIR:-/tmp}}): this job builds them on C4 by the W5 job's window.sh load and persists them under stores/W5, for this table and for the memory pass.
#   SZ_MATCH: a smoke's extended regexp over the item lines (results smoke-*); SZ_W5_TAG: W5 log (a smoke: W5-smallpedia; SZ_W5=0 skips).
# Exit codes: 2 missing input or failed restore, 3 guard, 5 time limit, 8 failed load.
#SBATCH -N 1
#SBATCH -J c4-sizes
source ${NEXT_STAGE_ROOT:?}/src/benchmarks/stages/studies/lib.sh
source $REPO/$STG/body.sh
guard; hostinfo
O=$NEXT_STAGE_ROOT/results/${SZ_SMOKE:+smoke-}sizes-$SLURM_JOB_ID
run_body $REPO/$STG/sizes/jobs/sizes_main.sh $O "cp -au ${LOCAL:?}/szres/. $O/"
