#!/bin/bash
source ${NEXT_STAGE_ROOT:?}/src/benchmarks/stages/studies/lib.sh
: "${OUT:?}"
TL=$REPO/$STG/raphtory/tools; RL=$E/studies/raphlike; T=W5-wikidata-v2; W=$E/w5-wikidata; WS=$E/studies/sizes/stores/$T
RESL=${LOCAL:-${TMPDIR:-/tmp}}/rcres; mkdir -p $RESL/cause
$PYV $REPO/$STG/tools/cov_rss.py $RESL/rss-samples.jsonl 2 &
RSSP=$!; trap 'kill -TERM $RSSP 2>/dev/null' EXIT
export EXTCMP_DUCKDBSQL_THREADS=64 EXTCMP_SERVER_LIMIT_S=1800 EXTCMP_SERVER_TIMEOUT_S=3660 EXTCMP_CALL_CEILING_S=3600
setup_root ${LOCAL:-${TMPDIR:-/tmp}}/rc; export TMPDIR=$ROOT/scratch; source $KIT/lib/env.sh
cp -a $W/campaign/eventlogs/$T.eventlog.jsonl $LOGS/; L=$LOGS/$T.eventlog.jsonl
want=$(awk -v f="$T.eventlog.jsonl" '$2 == f {print $1}' $W/eventlogs/SHA256SUMS-$T); got=$(sha256sum $L | cut -d' ' -f1)
[ -n "$want" ] && [ "$want" = "$got" ] || { echo "FROZEN LOG CHECK FAILED: stopping"; exit 2; }
restore_tar $WS/native-$T.tar.zst $STORES; restore_tar $WS/duckdbsql-$T.tar.zst $STORES; restore_tar $WS/postgres-$T.tar.zst $SRV
restore_tar $RL/stores/w5reg/$T/raphtory-$T.tar.zst $STORES
$KIT/lib/servers.sh start postgres $T | tail -1
$PY wait --system postgres --timeout 600 || { echo "POSTGRES WOULD NOT START: stopping"; exit 8; }
for wl in w5reg w5bel; do
  say "cause check $wl"
  ${EXTCMP_NUMA_CLIENT:+$EXTCMP_NUMA_CLIENT }$PYV $TL/rap_cause.py --wl $wl --tag $T --store $STORES/native-$T --raphtory $STORES/raphtory-$T --log $L --params $RL/params/$wl/params-$T.json --gate $RL/results/gate-merged/$wl/$T --E $E --hub 99 --other 99 --out $RESL/cause/cause-$wl-$T.json || { echo "CAUSE CHECK FAILED $wl" | tee -a $RESL/failed.txt; }
  say "whole sets $wl"
  $PYV $TL/rap_fullsets.py --cause $RESL/cause/cause-$wl-$T.json --wl $wl --tag $T --native $STORES/native-$T --root $EXTCMP_ROOT --out $RESL/fullsets-$wl-$T.json || { echo "FULL SETS FAILED $wl" | tee -a $RESL/failed.txt; }
  cp -au $RESL/. $OUT/
done
$KIT/lib/servers.sh stop postgres $T | tail -1
kill -TERM $RSSP 2>/dev/null
$PYV $TL/rap_cause_table.py $RESL/cause $RESL/cause/report.md; cut -c1-400 $RESL/cause/report.md | head -60
cp -au $RESL/. $OUT/
[ -s $RESL/failed.txt ] && exit 8
say "raphcause body done"
