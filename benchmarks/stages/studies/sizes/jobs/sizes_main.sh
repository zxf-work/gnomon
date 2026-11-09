#!/bin/bash
source ${NEXT_STAGE_ROOT:?}/src/benchmarks/stages/studies/lib.sh
: "${OUT:?}"
RESL=${LOCAL:-${TMPDIR:-/tmp}}/szres; mkdir -p $RESL; setup_root ${LOCAL:-${TMPDIR:-/tmp}}/sz; export TMPDIR=$ROOT/scratch
source $KIT/lib/env.sh
$PYV $REPO/$STG/tools/cov_rss.py $RESL/rss-samples.jsonl 2 &
RSSP=$!; trap 'kill -TERM $RSSP 2>/dev/null' EXIT
R=$RESL/sizes.jsonl; : > $R; F=${LOCAL:-${TMPDIR:-/tmp}}/szitem
W1=$E/final/w1/stores; W2=$E/final/w2/stores; C1=$E/confirm/w1/stores; C2=$E/confirm/w2/stores; W3=$E/w3-snb/w3stores; W4=$E/w4-finbench/w4stores
L1=$E/v3/eventlogs; L2=$E/w2-powerlaw/check1m/campaign/eventlogs; L3=$E/w3-snb/eventlogs; L4=$E/w4-finbench/eventlogs
items() {   # workload point system archive-or-file frozen-log note
  local t s
  for t in K10M K10M-d0 K10M-d10; do
    l=$t; [ $t = K10M ] && l=synth-10000000
    echo "W1 $t gnomon $C1/$t/native-$t.tar.zst $L1/$l.eventlog.jsonl C4"
    for s in duckdbsql duckdb clickhouse postgres; do echo "W1 $t $s $W1/$t/$s-$t.tar.zst - -"; done
    echo "W2 $t gnomon $C2/$t/native-$t.tar.zst $L2/$l.eventlog.jsonl C4"
    for s in duckdbsql duckdb clickhouse postgres neo4j memgraph ladybug; do echo "W2 $t $s $W2/$t/$s-$t.tar.zst - -"; done
  done
  echo "W2 K1M gnomon $C2/K1M/native-K1M.tar.zst $L2/synth-1000000.eventlog.jsonl C4"
  for s in duckdbsql postgres neo4j; do echo "W2 K1M $s $W2/K1M/$s-K1M.tar.zst - -"; done
  echo "W3 SF1 gnomon $E/kit/pilot/stores/w3/gnomon-SF1.tar.zst $L3/W3-SF1-h1-r1.eventlog.jsonl pilot-store"
  for s in postgres neo4j memgraph ladybug; do echo "W3 SF1 $s $W3/SF1-h1-r1/full/$s.tar.zst - -"; done
  echo "W3 SF1 duckdbsql $W3/SF1-h1-r1/full/duckdb.duckdb - -"
  echo "W3 SF10 gnomon $W3/SF10-h1-r1/full/gnomon-f10733cf-sodad393bd540d74c9.tar.zst $L3/W3-SF10-h1-r1.eventlog.jsonl f10733cf"
  for s in postgres neo4j memgraph ladybug; do echo "W3 SF10 $s $W3/SF10-h1-r1/full/$s.tar.zst - -"; done
  echo "W3 SF10 duckdbsql $W3/SF10-h1-r1/full/duckdb.duckdb - -"
  for r in r0 r10; do
    echo "W4 sf10-$r gnomon $W4/sf10-$r/gnomon-f10733cf-sodad393bd540d74c9.tar.zst $L4/w4-sf10-$r.eventlog.jsonl f10733cf"
    for s in postgres neo4j memgraph ladybug; do echo "W4 sf10-$r $s $W4/sf10-$r/$s.tar.zst - -"; done
    echo "W4 sf10-$r duckdbsql $W4/sf10-$r/duckdbsql.duckdb - -"
  done
  echo "W4 sf10-r10 gnomon-c4-rebuild $E/studies/w4-c4store/sf10-r10/gnomon-sf10-r10.tar.zst $L4/w4-sf10-r10.eventlog.jsonl C4"
}
measure() {   # workload point system source log note: unpack (or copy) to a fresh directory, du, one record, remove
  local wl=$1 pt=$2 s=$3 src=$4 lg=$5 note=$6 d=${LOCAL:-${TMPDIR:-/tmp}}/szu t0=$(date +%s) a=$src
  need_file $src
  if [ -e $src.sha256 ]; then ( cd $(dirname $src) && sha256sum -c --quiet $(basename $src).sha256 ) || { echo "RESTORE CHECK FAILED $src: stopping"; exit 2; }; chk=1; else chk=0; echo "no sha256 beside $src: unpacked unchecked"; fi
  rm -rf "${d:?}"; mkdir -p $d
  case $src in *.tar.zst) zstd -q -d -c $src | tar -C $d -xf - || { echo "RESTORE FAILED $src: stopping"; exit 2; } ;; *) cp $src $d/ ;; esac
  $PYV $REPO/$STG/sizes/tools/sz_one.py --dir $d --workload $wl --point $pt --system $s --source $src --log "$lg" --note "$note" --sha-checked $chk --out $R || { echo "SIZE RECORD FAILED $src: stopping"; exit 2; }
  rm -rf "${d:?}"; echo "$wl $pt $s measured in $(( $(date +%s) - t0 )) s $(utc)"
  cp -au $RESL/. $OUT/; }
items > $RESL/items.txt
grep -E "${SZ_MATCH:-.}" $RESL/items.txt > $F; say "$(wc -l < $F) items"
while read -r wl pt s src lg note; do measure $wl $pt $s $src $lg $note; done < $F
if [ "${SZ_W5:-1}" = 1 ]; then
  TAG=${SZ_W5_TAG:-W5-wikidata-v2}; W=$E/w5-wikidata; SIZE=1000000; WS=$NEXT_STAGE_ROOT/stores/$TAG; mkdir -p $WS
  say "W5 stores on C4: $TAG, replay and bulk copies as in the W5 job"
  cp -a $W/campaign/eventlogs/$TAG.eventlog.jsonl $W/eventlogs/$TAG.eventlog.jsonl.meta.json $LOGS/ 2>/dev/null; need_file $LOGS/$TAG.eventlog.jsonl
  want=$(awk -v f="$TAG.eventlog.jsonl" '$2 == f {print $1}' $W/eventlogs/SHA256SUMS-$TAG 2>/dev/null); got=$(sha256sum $LOGS/$TAG.eventlog.jsonl | cut -d' ' -f1)
  if [ "$TAG" = W5-wikidata-v2 ]; then [ -n "$want" ] && [ "$want" = "$got" ] || { echo "FROZEN LOG CHECK FAILED $TAG: stopping"; exit 2; }; fi
  export EXTCMP_SERVER_TIMEOUT_S=3660 EXTCMP_CALL_CEILING_S=3600 EXTCMP_VERIFY_CAP_S=3662
  export EXTCMP_SYNC_HOOK="mkdir -p $RESL/w5 && cp -au $ROOT/results/. $RESL/w5/ && cp -au $RESL/. $OUT/"
  for s in native duckdbsql postgres neo4j; do
    "$KIT/lib/window.sh" load $SIZE $TAG - - $s || echo "WINDOW FAILED load $s"
    $PYV -c "import json,sys; d=json.load(open(sys.argv[1])); sys.exit(0 if 'load' in d and not d.get('load exceeded budget') else 1)" $RES/$TAG/load-$s.json || { echo "LOAD NOT COMPLETE $s: stopping"; exit 8; }
    case $s in native|duckdbsql) par=$STORES ;; *) par=$SRV ;; esac
    tar -C $par -cf - $s-$TAG | zstd -1 -T16 -q -o $WS/$s-$TAG.tar.zst.tmp && mv $WS/$s-$TAG.tar.zst.tmp $WS/$s-$TAG.tar.zst && ( cd $WS && sha256sum $s-$TAG.tar.zst > $s-$TAG.tar.zst.sha256 ) || { echo "STORE SAVE FAILED $s: stopping"; exit 8; }
    cp -a $RES/$TAG/load-$s.json $WS/
    case $s in native) n=gnomon; lg=$LOGS/$TAG.eventlog.jsonl ;; *) n=$s; lg=- ;; esac
    measure W5 $TAG $n $WS/$s-$TAG.tar.zst $lg C4
  done
fi
kill -TERM $RSSP 2>/dev/null
$PYV $REPO/$STG/sizes/tools/sizes_table.py $R ${RAP_DU:-$NEXT_STAGE_ROOT/../raphtory/results} > $RESL/sizes.md || echo "table failed"; cat $RESL/sizes.md
cp -au $RESL/. $OUT/; say "sizes body done"
