#!/bin/bash
# Raphtory stage body. Per point, node-local root per workload (W1 and W2 share tag names).
source ${NEXT_STAGE_ROOT:?}/src/benchmarks/stages/studies/lib.sh
: "${OUT:?}" "${RAP_MODE:?}"
S=$NEXT_STAGE_ROOT; POINTS=${RAP_POINTS:-w1:K1M w1:K10M w1:K10M-d0 w1:K10M-d10 w2:K10M w2:K10M-d0}
PD=$S/params; STO=$S/stores; if [ "$RAP_MODE" = smoke ] || [ -n "${RAP_SMOKE_CAUSE:-}" ]; then PD=$S/params-smoke; STO=$S/smoke-stores; fi
TL=$REPO/$STG/raphtory/tools
RESL=${LOCAL:-${TMPDIR:-/tmp}}/rapres; mkdir -p $RESL
need_file $PD/SHA256SUMS; ( cd $PD && sha256sum -c --quiet SHA256SUMS ) || { echo "PARAMETER FILES DO NOT MATCH SHA256SUMS: stopping"; exit 2; }
PINSAMP=""; numactl --cpunodebind=1 true 2>/dev/null && PINSAMP="numactl --cpunodebind=1"
if [ "$RAP_MODE" = gate ]; then export COV_RSS_PSS_EVERY=10; else export COV_RSS_PSS_EVERY=0; fi
$PINSAMP $PYV $REPO/$STG/tools/cov_rss.py $RESL/rss-samples.jsonl 2 &
RSSP=$!; trap 'kill -TERM $RSSP 2>/dev/null' EXIT
size_of() { case $1 in K1M*) echo 1000000 ;; K10M*) echo 10000000 ;; esac; }
log_of() { case $1 in K1M) echo synth-1000000 ;; K10M) echo synth-10000000 ;; *) echo $1 ;; esac; }
sums_of() { case $1 in synth-1000000|K1M-d0) echo powerlaw-1000000 ;; synth-10000000|K10M-d0) echo powerlaw-10000000 ;; K10M-d10) echo W2-10M-d10 ;; esac; }
pt_setup() {   # wl:tag
  wl=${1%%:*}; tag=${1#*:}; size=$(size_of $tag); setup_root ${LOCAL:-${TMPDIR:-/tmp}}/rap-$wl
  export EXTCMP_SYNC_HOOK="mkdir -p $RESL/$wl && cp -au $ROOT/results/. $RESL/$wl/ && cp -au $RESL/. $OUT/"
  source $KIT/lib/env.sh
  case $tag in *-d*) export EXTCMP_BELIEFQ_INDEXES=1 EXTCMP_VERIFY_FAMILY=B; FAMS=B ;; *) unset EXTCMP_BELIEFQ_INDEXES EXTCMP_VERIFY_FAMILY; FAMS="T N" ;; esac
  PARAMS=$PD/$wl/params-$tag.json; need_file $PARAMS
  n=$(log_of $tag)
  if [ ! -s $LOGS/$n.eventlog.jsonl ]; then
    case $wl in
      w1) cp -a $E/v3/eventlogs/$n.eventlog.* $LOGS/; ( cd $LOGS && grep -E "  $n\.eventlog\.jsonl$" $E/v3/eventlogs/SHA256SUMS | sha256sum -c --quiet ) || { echo "FROZEN LOG CHECK FAILED $wl $n: stopping"; exit 2; } ;;
      w2) cp -a $E/w2-powerlaw/check1m/campaign/eventlogs/$n.eventlog.* $LOGS/
          want=$(awk -v f="$(sums_of $n).eventlog.jsonl" '$2 == f {print $1}' $E/w2-powerlaw/eventlogs/SHA256SUMS); got=$(sha256sum $LOGS/$n.eventlog.jsonl | cut -d' ' -f1)
          [ -n "$want" ] && [ "$want" = "$got" ] || { echo "FROZEN LOG CHECK FAILED $wl $n ($got vs $want): stopping"; exit 2; } ;;
    esac
    echo "log $wl $n checked"
  fi
  a=$E/confirm/$wl/stores/$tag/native-$tag.tar.zst; need_file $E/confirm/$wl/stores/$tag/load-native.json
  [ -d $STORES/native-$tag ] || restore_tar $a $STORES
  need_dir $STORES/native-$tag
  $PYV -c "import json,sys; m=json.load(open(sys.argv[1]))['manifest']; print('native store provenance', sys.argv[1], 'commit', m.get('commit'), 'engine_so', m.get('engine_so_sha256') or m.get('so_sha256'), 'host', m.get('hostname'))" $E/confirm/$wl/stores/$tag/load-native.json
}
load_ok() { $PYV -c "import json,sys; d=json.load(open(sys.argv[1])); sys.exit(0 if 'load' in d and not d.get('load exceeded budget') else 1)" "$1" 2>/dev/null; }
win() { timeout -k 60 3600 "$KIT/lib/window.sh" "$@" || echo "WINDOW FAILED $* $(utc)"; }
gate_point() {
  pt_setup $1
  say "gate $wl $tag: Raphtory's load, from Gnomon's C4 store"
  export EXTCMP_SERVER_TIMEOUT_S=3660 EXTCMP_CALL_CEILING_S=3600 EXTCMP_VERIFY_CAP_S=3662; unset EXTCMP_SERVER_LIMIT_S EXTCMP_P1_CEILING_S EXTCMP_P1_SLOW_S EXTCMP_INPROC_WORKER EXTCMP_WINDOW_TIMEOUT_S
  "$KIT/lib/window.sh" load $size $tag - - raphtory || echo "WINDOW FAILED load $wl $tag"
  load_ok $RES/$tag/load-raphtory.json || { echo "RAPHTORY LOAD NOT COMPLETE $wl $tag: stopping"; exit 8; }
  o=$STO/$wl/$tag; mkdir -p $o
  tar -C $STORES -cf - raphtory-$tag | zstd -1 -T16 -q -o $o/raphtory-$tag.tar.zst.tmp && mv $o/raphtory-$tag.tar.zst.tmp $o/raphtory-$tag.tar.zst && ( cd $o && sha256sum raphtory-$tag.tar.zst > raphtory-$tag.tar.zst.sha256 ) || { echo "STORE SAVE FAILED: stopping"; exit 8; }
  cp -a $RES/$tag/load-raphtory.json $o/
  echo "{\"wl\": \"$wl\", \"tag\": \"$tag\", \"du_sb\": $(du -sb $STORES/raphtory-$tag | cut -f1)}" >> $RESL/raphtory-du.jsonl
  proto_p1
  say "gate $wl $tag: answer check of both systems, parameter-file path"
  for s in native raphtory; do EXTCMP_PARAMS=$PARAMS win verify $size $tag - - $s; done
  cp $PARAMS $RES/$tag/params-used.json
  $PYV $TL/rap_gate.py --dir $RES/$tag --ceiling 10 --label "$wl $tag" --out $RES/$tag/excluded.json --md $RES/$tag/verdict.md; echo "gate $wl $tag rc=$?" | tee -a $RESL/gate-rc.txt
  eval "$EXTCMP_SYNC_HOOK"
}
fam_differs() { $PYV -c "
import json,sys
fam={'T':('snap.hop2','reach.window'),'N':('nbr.evolution',),'B':('b.snap.hop2@now','b.reach@now')}[sys.argv[1]]
v=json.load(open(sys.argv[2]))
sys.exit(0 if any(r['verdict']=='DIFFERS' and r['query'] in fam for r in v) else 1)" $1 $RES/$tag/verdicts-raphtory.json; }
tblock() {   # rot fam system engine
  local r=$1 fam=$2 s=$3 et=${4:-}
  EXTCMP_PARAMS=$PARAMS EXTCMP_EXCLUDE=$RES/$tag/excluded.json EXTCMP_ENGINE_TIME=$et win block $size $tag $r $fam $s
  if [ $s = native ] && [ "$r" != E ]; then
    $PYV $REPO/benchmarks/stages/w2/tools/w2_stopcheck.py $RES/$tag/rot$r/$fam-native.json $RES/$tag || { echo "STOP RULE: Gnomon $wl $tag rot$r $fam"; eval "$EXTCMP_SYNC_HOOK"; exit 4; }
  fi
}
timed_point() {
  pt_setup $1
  say "timed $wl $tag"
  local src=$GATED/$wl/$tag r b k i fam s
  need_file $src/excluded.json; need_file $STO/$wl/$tag/raphtory-$tag.tar.zst
  [ -e $STORES/raphtory-$tag ] || restore_tar $STO/$wl/$tag/raphtory-$tag.tar.zst $STORES
  mkdir -p $RES/$tag; cp -a $src/verify-*.json $src/excluded.json $src/label-only.json $src/verdicts-raphtory.json $src/verdict.md $src/load-raphtory.json $src/params-used.json $RES/$tag/
  proto_p1
  local -a SY=(native raphtory)
  for r in 1 2; do b=0
    for fam in $FAMS; do
      k=$(( b % 2 )); order=("${SY[@]:k}" "${SY[@]:0:k}")
      if [ $r = 2 ]; then rev=(); for (( i=${#order[@]}-1; i>=0; i-- )); do rev+=("${order[i]}"); done; order=("${rev[@]}"); fi
      for s in "${order[@]}"; do tblock $r $fam $s
        # the differing cells, same protocol, their own block and record (rotL<r>), label "different answer"; they stay excluded from rot<r> and from every ratio
        if [ $s = raphtory ] && fam_differs $fam; then EXTCMP_PARAMS=$PARAMS EXTCMP_EXCLUDE=$RES/$tag/label-only.json win block $size $tag L$r $fam raphtory; fi
      done
      b=$((b + 1))
    done
  done
  say "Gnomon's engine-time pass $wl $tag"
  for fam in $FAMS; do EXTCMP_P1_REPS=3 tblock E $fam native 1; done
  eval "$EXTCMP_SYNC_HOOK"
}
cause_point() {   # untimed: why the reach answers differ, node sets, sampled paths from the log, recorded answers of DuckDB (SQL) and PostgreSQL
  pt_setup $1
  [ -e $STORES/raphtory-$tag ] || restore_tar $STO/$wl/$tag/raphtory-$tag.tar.zst $STORES
  say "cause check $wl $tag"; mkdir -p $RESL/cause
  ${EXTCMP_NUMA_CLIENT:+$EXTCMP_NUMA_CLIENT }$PYV $TL/rap_cause.py --wl $wl --tag $tag --store $STORES/native-$tag --raphtory $STORES/raphtory-$tag --log $LOGS/$n.eventlog.jsonl --params $PARAMS --gate $GATED/$wl/$tag --E $E --out $RESL/cause/cause-$wl-$tag.json || { echo "CAUSE CHECK FAILED $wl $tag"; echo "$wl $tag" >> $RESL/cause/failed.txt; }
  cp -au $RESL/. $OUT/
}
case $RAP_MODE in
  gate) for p in $POINTS; do gate_point $p; done ;;
  timed) : "${RAP_GATE:?}"; GATED=$RAP_GATE; for p in $POINTS; do timed_point $p; done ;;
  smoke) for p in $POINTS; do gate_point $p; done; GATED=$RESL; for p in $POINTS; do timed_point $p; done ;;
  cause) : "${RAP_GATE:?}"; GATED=$RAP_GATE; for p in $POINTS; do cause_point $p; done; $PYV $TL/rap_cause_table.py $RESL/cause $RESL/cause/report.md; cat $RESL/cause/report.md | head -80 ;;
esac
kill -TERM $RSSP 2>/dev/null
if [ "$RAP_MODE" = timed ] || [ "$RAP_MODE" = smoke ]; then $PYV $TL/rap_table.py --res $RESL --ref $E --out $RESL/table.md || echo "table failed"; fi
cp -au $RESL/. $OUT/; say "raphtory body done"
