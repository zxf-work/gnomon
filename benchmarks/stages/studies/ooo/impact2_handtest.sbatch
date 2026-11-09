#!/bin/bash
#SBATCH -N 1
#SBATCH --ntasks-per-node=1
#SBATCH -J c4-ooo-impact-handtest
ST=${NEXT_STAGE_ROOT:?}; O=$ST/impact-control/handtest-$SLURM_JOB_ID; mkdir -p $O
$ST/src/.venv/bin/python $ST/impact2-tool/impact2_handtest.py $ST/impact2-tool/ooo_impact2.py $O 2>&1 | tee $O/result.txt
exit ${PIPESTATUS[0]}
