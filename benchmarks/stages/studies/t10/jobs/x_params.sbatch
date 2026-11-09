#!/bin/bash
# The 100M single-parameter file, by the rule that produced K10M's (tools/x_params.py k100m), from a node-local copy of the frozen log, since the log's side
# files are written beside it. Also the 1M file of the smoke. a shared node: python reads the log, nothing is timed.
#SBATCH -N 1
#SBATCH --ntasks-per-node=8
#SBATCH -J c4-k100m-params
source ${NEXT_STAGE_ROOT:?}/src/benchmarks/stages/studies/t10/jobs/x_lib.sh
guard; hostinfo
L=${LOCAL:?}/xp; mkdir -p $L
for n in synth-1000000 synth-100000000; do
  cp -a $LOGSRC/$n.eventlog.jsonl $LOGSRC/$n.eventlog.meta.json $L/
  want=$(awk -v f="$n.eventlog.jsonl" '$2 == f {print $1}' $SUMS); got=$(sha256sum $L/$n.eventlog.jsonl | cut -d' ' -f1)
  [ "$want" = "$got" ] && echo "log $n sha256 $got OK" || { echo "FROZEN LOG CHECK FAILED $n"; exit 2; }
done
$PYV $XT/x_params.py k100m --log $L/synth-1000000.eventlog.jsonl --sums $SUMS --tag K1M-d0 --out $NEXT_STAGE_ROOT/params-smoke || exit 2
$PYV $XT/x_params.py k100m --log $L/synth-100000000.eventlog.jsonl --sums $SUMS --tag K100M-d0 --out $NEXT_STAGE_ROOT/params || exit 2
say "params done"
