#!/bin/bash
# with the base stage's side files beside them: K1M-d0 for the smoke and K100M-d0.
# a shared node: python reads the logs, nothing is timed.
#SBATCH -N 1
#SBATCH --ntasks-per-node=8
#SBATCH -J cz-k100m-params
source ${NEXT_STAGE_ROOT:?}/src/benchmarks/stages/studies/t10/jobs/x_lib.sh
guard; hostinfo
B=$E/studies/k100m
L=${LOCAL:?}/xp; mkdir -p $L
for pair in "synth-1000000:params-smoke:K1M-d0" "synth-100000000:params:K100M-d0"; do
  IFS=: read n bdir tag <<< "$pair"
  cp -a $LOGSRC/$n.eventlog.jsonl $LOGSRC/$n.eventlog.meta.json $L/
  want=$(awk -v f="$n.eventlog.jsonl" '$2 == f {print $1}' $SUMS); got=$(sha256sum $L/$n.eventlog.jsonl | cut -d' ' -f1)
  [ "$want" = "$got" ] && echo "log $n sha256 $got OK" || { echo "FROZEN LOG CHECK FAILED $n"; exit 2; }
  ( cd $B/$bdir && sha256sum -c SHA256SUMS ) || { echo "BASE PARAMETER FILE CHECK FAILED $bdir"; exit 2; }
  cp -a $B/$bdir/side/$n.eventlog.*.json $L/
  out=$NEXT_STAGE_ROOT/${bdir}; mkdir -p $out/side; cp -a $B/$bdir/side/$n.eventlog.*.json $out/side/
  $PYV $XT/x_extra.py --log $L/$n.eventlog.jsonl --base $B/$bdir/params-$tag.json --sums $SUMS --out $out --tag $tag || exit 2
done
say "extra params done"
