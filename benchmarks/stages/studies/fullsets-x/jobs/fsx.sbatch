#!/bin/bash
# FSX_NP, FSX_ONLY (smoke keys), FSX_SMOKE=1 labels results smoke-*. The deadline is the job's limit minus FSX_MARGIN_S (default 600).
#SBATCH -N 1
#SBATCH --signal=B:USR1@300
#SBATCH -J c4-covx-fullsets
source ${NEXT_STAGE_ROOT:?}/src/benchmarks/stages/studies/lib.sh
source $REPO/$STG/body.sh
guard; hostinfo
lim=$(squeue -h -j $SLURM_JOB_ID -o %L); case $lim in *-*) d=${lim%%-*}; lim=${lim#*-} ;; *) d=0 ;; esac
IFS=: read -r h m s <<< "$(echo $lim | awk -F: '{ if (NF==3) print $1":"$2":"$3; else if (NF==2) print "0:"$1":"$2; else print "0:0:"$1 }')"
export FSX_DEADLINE=$(( $(date +%s) + d*86400 + 10#$h*3600 + 10#$m*60 + 10#$s - ${FSX_MARGIN_S:-600} ))
O=$NEXT_STAGE_ROOT/results/${FSX_SMOKE:+smoke-}fsx-$SLURM_JOB_ID
run_body $REPO/$STG/fullsets-x/jobs/fsx_main.sh $O "cp -au ${LOCAL:?}/fsxres/. $O/"
