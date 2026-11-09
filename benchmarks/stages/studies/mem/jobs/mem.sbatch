#!/bin/bash
#   Stores reused; the recorded answered cells (mem/cells, written before submission); each cell once per the short protocol. Sampler tools/cov_rss.py (2 s RSS, VmHWM, PSS one sample in ten) over phase marks: open, prime, query.
#   MEM_SMOKE=1: results smoke-*, the smoke cell files, MEM_SYSTEMS narrows the systems.
# Exit codes: 2 missing input, 3 guard, 5 time limit, 8 failed block.
#SBATCH -N 1
#SBATCH -J c4-mem
export W3_STAGE_ROOT=${NEXT_STAGE_ROOT:?}
source $NEXT_STAGE_ROOT/src/benchmarks/stages/studies/lib.sh
source $REPO/$STG/body.sh
guard; hostinfo
: "${MEM_WL:?}"
O=$NEXT_STAGE_ROOT/results/${MEM_SMOKE:+smoke-}$MEM_WL-$SLURM_JOB_ID
run_body $REPO/$STG/mem/jobs/mem_main.sh $O "cp -au ${LOCAL:?}/memres/. $O/"
