#!/bin/bash
# W5 component study: the Gnomon store built on C4 from the frozen log, and kept. W5_TAG W5-wikidata-v2 | W5-smallpedia (a smoke).
#SBATCH -N 1
#SBATCH --ntasks-per-node=32
#SBATCH --signal=B:USR1@120
#SBATCH -J c4-w5abl-build
source ${NEXT_STAGE_ROOT:?}/src/benchmarks/stages/studies/lib.sh
guard; hostinfo
: "${W5_TAG:?}"; TAG=$W5_TAG; W=$E/w5-wikidata
D=$NEXT_STAGE_ROOT/stores/$TAG; mkdir -p $D
$PYV $TOOLS/ing_rss.py $D/rss-$SLURM_JOB_ID.jsonl 5 & RSS=$!
trap 'kill $RSS 2>/dev/null' EXIT
setup_root ${LOCAL:-${TMPDIR:-/tmp}}/w5b; source $KIT/lib/env.sh
cp -a $W/campaign/eventlogs/$TAG.eventlog.jsonl $LOGS/; need_file $LOGS/$TAG.eventlog.jsonl
want=$(awk -v f="$TAG.eventlog.jsonl" '$2 == f {print $1}' $W/eventlogs/SHA256SUMS-$TAG); got=$(sha256sum $LOGS/$TAG.eventlog.jsonl | cut -d' ' -f1)
[ -n "$want" ] && [ "$want" = "$got" ] && echo "log $TAG sha256 $got OK" || { echo "FROZEN LOG CHECK FAILED $TAG ($got vs $want): stopping"; exit 2; }
"$KIT/lib/window.sh" load 1000000 $TAG - - native || { echo "LOAD FAILED"; exit 2; }
cp -a $RES/$TAG/load-native.json $D/
need_dir $STORES/native-$TAG
$REPO/.venv/bin/gnomon store digest --store $STORES/native-$TAG --json > $D/digest.json || echo "digest failed"
cat $D/digest.json | cut -c1-300
tar -C $STORES -cf - native-$TAG | zstd -q -3 -T8 -o $D/native-$TAG.tar.zst || { echo "ARCHIVE FAILED"; exit 2; }
( cd $D && sha256sum native-$TAG.tar.zst > native-$TAG.tar.zst.sha256 ); cat $D/native-$TAG.tar.zst.sha256
echo "engine so $(sha256sum $REPO/gnomon/_engine*.so | cut -c1-16) kit $(git -C $REPO rev-parse HEAD)" | tee $D/provenance.txt
say "W5 store build done"
