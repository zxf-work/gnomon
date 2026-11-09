#!/bin/bash
# C4 causes (audit #12). CAUSES_PART w3|w4|w5. Partition and cores on the sbatch line. Ports derive from the job id (lib.sh sets EXTCMP_JOB_SAFE). A diagnostic run: enters no table of times.
#SBATCH -N 1
#SBATCH --signal=B:USR1@120
#SBATCH -J c4-causes
source ${NEXT_STAGE_ROOT:?}/src/benchmarks/stages/studies/lib.sh
source $REPO/$STG/body.sh
guard; hostinfo
: "${CAUSES_PART:?}"
O=$NEXT_STAGE_ROOT/results/${C_SMOKE:+smoke-}$CAUSES_PART-$SLURM_JOB_ID; mkdir -p $O
$PYV $TOOLS/ing_rss.py $O/rss.jsonl 5 & RSS=$!
trap 'kill $RSS 2>/dev/null' EXIT
export ROOT=${LOCAL:?}/causes
case $CAUSES_PART in
  w3) SY="mkdir -p $O && cp -au $ROOT/res/. $O/" ;;
  *) SY="mkdir -p $O && cp -au $ROOT/results/. $O/" ;;
esac
run_body $REPO/$STG/causes/causes_$CAUSES_PART.sh $O "$SY"
