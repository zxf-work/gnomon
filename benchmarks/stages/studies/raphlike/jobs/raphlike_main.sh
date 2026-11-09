#!/bin/bash
source ${NEXT_STAGE_ROOT:?}/src/benchmarks/stages/studies/lib.sh
: "${OUT:?}" "${RL_MODE:?}"
S=$NEXT_STAGE_ROOT; W5T=W5-wikidata-v2
POINTS=${RL_POINTS:-w1:K1M w1:K10M w1:K10M-d0 w1:K10M-d10 w2:K10M w2:K10M-d0 w2:K10M-d10 w5reg:$W5T w5bel:$W5T}
PD=$S/params; STO=$S/stores; [ "$RL_MODE" = smoke ] && { PD=$S/params-smoke; STO=$S/smoke-stores; }
RTL=$REPO/$STG/raphtory/tools; TL=$REPO/$STG/raphlike/tools; OLDSTO=$E/studies/raphtory/stores
RESL=${LOCAL:-${TMPDIR:-/tmp}}/rlres; mkdir -p $RESL
need_file $PD/SHA256SUMS; ( cd $PD && sha256sum -c --quiet SHA256SUMS ) || { echo "PARAMETER FILES DO NOT MATCH SHA256SUMS: stopping"; exit 2; }
PINSAMP=""; numactl --cpunodebind=1 true 2>/dev/null && PINSAMP="numactl --cpunodebind=1"
if [ "$RL_MODE" = gate ]; then export COV_RSS_PSS_EVERY=10; else export COV_RSS_PSS_EVERY=0; fi
$PINSAMP $PYV $REPO/$STG/tools/cov_rss.py $RESL/rss-samples.jsonl 2 &
RSSP=$!; trap 'kill -TERM $RSSP 2>/dev/null' EXIT
size_of() { case $1 in K1M*|W5*) echo 1000000 ;; K10M*) echo 10000000 ;; esac; }
log_of() { case $1 in K1M) echo synth-1000000 ;; K10M) echo synth-10000000 ;; *) echo $1 ;; esac; }
sums_of() { case $1 in synth-1000000|K1M-d0) echo powerlaw-1000000 ;; synth-10000000|K10M-d0) echo powerlaw-10000000 ;; K10M-d10) echo W2-10M-d10 ;; K1M-d10) echo W2-1M-d10 ;; esac; }
pt_setup() {   # wl:tag
  wl=${1%%:*}; tag=${1#*:}; size=$(size_of $tag); setup_root ${LOCAL:-${TMPDIR:-/tmp}}/rl-$wl
  export EXTCMP_SYNC_HOOK="mkdir -p $RESL/$wl && cp -au $ROOT/results/. $RESL/$wl/ && cp -au $RESL/. $OUT/"
  source $KIT/lib/env.sh
  case $wl in w5bel) export EXTCMP_BELIEFQ_INDEXES=1 EXTCMP_VERIFY_FAMILY=B; FAMS=B ;; w5reg) unset EXTCMP_BELIEFQ_INDEXES EXTCMP_VERIFY_FAMILY; FAMS="T N" ;;
    *) case $tag in *-d*) export EXTCMP_BELIEFQ_INDEXES=1 EXTCMP_VERIFY_FAMILY=B; FAMS=B ;; *) unset EXTCMP_BELIEFQ_INDEXES EXTCMP_VERIFY_FAMILY; FAMS="T N" ;; esac ;; esac
  PARAMS=$PD/$wl/params-$tag.json; need_file $PARAMS
  n=$(log_of $tag)
  if [ ! -s $LOGS/$n.eventlog.jsonl ]; then
    case $wl in
      w1) cp -a $E/v3/eventlogs/$n.eventlog.* $LOGS/; ( cd $LOGS && grep -E "  $n\.eventlog\.jsonl$" $E/v3/eventlogs/SHA256SUMS | sha256sum -c --quiet ) || { echo "FROZEN LOG CHECK FAILED $wl $n: stopping"; exit 2; } ;;
      w2) cp -a $E/w2-powerlaw/check1m/campaign/eventlogs/$n.eventlog.* $LOGS/
          want=$(awk -v f="$(sums_of $n).eventlog.jsonl" '$2 == f {print $1}' $E/w2-powerlaw/eventlogs/SHA256SUMS); got=$(sha256sum $LOGS/$n.eventlog.jsonl | cut -d' ' -f1)
          [ -n "$want" ] && [ "$want" = "$got" ] || { echo "FROZEN LOG CHECK FAILED $wl $n ($got vs $want): stopping"; exit 2; } ;;
      w5*) W=$E/w5-wikidata; cp -a $W/campaign/eventlogs/$tag.eventlog.jsonl $W/eventlogs/$tag.eventlog.jsonl.meta.json $LOGS/ 2>/dev/null
          want=$(awk -v f="$tag.eventlog.jsonl" '$2 == f {print $1}' $W/eventlogs/SHA256SUMS-$tag); got=$(sha256sum $LOGS/$tag.eventlog.jsonl | cut -d' ' -f1)
          [ -n "$want" ] && [ "$want" = "$got" ] || { echo "FROZEN LOG CHECK FAILED $tag: stopping"; exit 2; }
          $PYV - "$LOGS/$tag.eventlog.jsonl" "$W/params-c3/params-$tag-bel.json" <<'PY' || { echo "SIDECAR FAILED: stopping"; exit 2; }
import json, os, sys
log, pf = sys.argv[1], sys.argv[2]; st = os.stat(log)
ins = {i["query"]: i["args"] for i in json.load(open(pf))["instances"]}
sn, rc, df, h = ins["b.snap.hop2@T"], ins["b.reach@T"], ins["b.diff"], ins["b.hist"]
d = {"size": st.st_size, "mtime_ns": st.st_mtime_ns, "snap_seeds": sn["seeds"], "t_star": sn["t_valid"],
     "seed_node": rc["src"], "reach_window": rc["window"], "T": sn["as_of_tt"], "T_base": df["t1"],
     "diff_window": df["window"], "e_star": h, "w5_placeholder": True}
open(log[:-len(".jsonl")] + ".beliefq-params.json", "w").write(json.dumps(d))
PY
          ;;
    esac
    echo "log $wl $n checked"
  fi
  case $wl in w5*) a=$E/studies/sizes/stores/$tag/native-$tag.tar.zst ;; *) a=$E/confirm/$wl/stores/$tag/native-$tag.tar.zst ;; esac
  [ -d $STORES/native-$tag ] || restore_tar $a $STORES
  need_dir $STORES/native-$tag
}
load_ok() { $PYV -c "import json,sys; d=json.load(open(sys.argv[1])); sys.exit(0 if 'load' in d and not d.get('load exceeded budget') else 1)" "$1" 2>/dev/null; }
win() { timeout -k 60 3600 "$KIT/lib/window.sh" "$@" || echo "WINDOW FAILED $* $(utc)"; }
restore_rap() {   # the first campaign's form of load, from Gnomon's C4 store; a store the Raphtory stage kept is reused
  local o=$STO/$wl/$tag old=$OLDSTO/$wl/$tag/raphtory-$tag.tar.zst
  if [ -e $STORES/raphtory-$tag ]; then return 0; fi
  if [ -s $o/raphtory-$tag.tar.zst ]; then restore_tar $o/raphtory-$tag.tar.zst $STORES; mkdir -p $RES/$tag; cp -a $o/load-raphtory.json $RES/$tag/ 2>/dev/null; return 0; fi
  if [ -s $old ]; then restore_tar $old $STORES; mkdir -p $RES/$tag; cp -a $OLDSTO/$wl/$tag/load-raphtory.json $RES/$tag/ 2>/dev/null; return 0; fi
  if [ "${1:-}" != load ]; then echo "NO RAPHTORY STORE $wl $tag: stopping"; exit 2; fi
  say "load Raphtory $wl $tag, from Gnomon's C4 store"
  export EXTCMP_SERVER_TIMEOUT_S=3660 EXTCMP_CALL_CEILING_S=3600 EXTCMP_VERIFY_CAP_S=3662; unset EXTCMP_SERVER_LIMIT_S EXTCMP_P1_CEILING_S EXTCMP_P1_SLOW_S EXTCMP_INPROC_WORKER EXTCMP_WINDOW_TIMEOUT_S
  "$KIT/lib/window.sh" load $size $tag - - raphtory || echo "WINDOW FAILED load $wl $tag"
  load_ok $RES/$tag/load-raphtory.json || { echo "RAPHTORY LOAD NOT COMPLETE $wl $tag: stopping"; exit 8; }
  mkdir -p $o; tar -C $STORES -cf - raphtory-$tag | zstd -1 -T16 -q -o $o/raphtory-$tag.tar.zst.tmp && mv $o/raphtory-$tag.tar.zst.tmp $o/raphtory-$tag.tar.zst && ( cd $o && sha256sum raphtory-$tag.tar.zst > raphtory-$tag.tar.zst.sha256 ) || { echo "STORE SAVE FAILED: stopping"; exit 8; }
  cp -a $RES/$tag/load-raphtory.json $o/
  echo "{\"wl\": \"$wl\", \"tag\": \"$tag\", \"du_sb\": $(du -sb $STORES/raphtory-$tag | cut -f1)}" >> $RESL/raphtory-du.jsonl
}
gate_point() {
  pt_setup $1
  restore_rap load
  proto_p1
  say "gate $wl $tag: answer check, three forms"
  mkdir -p $RES/$tag
  for s in native raphtory raphtoryeq; do EXTCMP_PARAMS=$PARAMS win verify $size $tag - - $s; done
  cp $PARAMS $RES/$tag/params-used.json
  $PYV $RTL/rap_gate.py --dir $RES/$tag --ceiling 10 --label "$wl $tag first form" --out $RES/$tag/excluded.json --md $RES/$tag/verdict.md; echo "gate $wl $tag first form rc=$?" | tee -a $RESL/gate-rc.txt
  $PYV $TL/rl_eq_check.py --dir $RES/$tag --store $STORES/native-$tag --raphtory $STORES/raphtory-$tag --params $PARAMS --label "$wl $tag" --md $RES/$tag/verdict-eq.md | tail -n 40 | cut -c1-400; echo "gate $wl $tag fuller form rc=${PIPESTATUS[0]}" | tee -a $RESL/gate-rc.txt
  eval "$EXTCMP_SYNC_HOOK"
}
fam_differs() { $PYV -c "
import json,sys
fam={'T':('snap.hop2','reach.window'),'N':('nbr.evolution',),'B':('b.snap.hop2@now','b.reach@now')}[sys.argv[1]]
v=json.load(open(sys.argv[2]))
sys.exit(0 if any(r['verdict']=='DIFFERS' and r['query'] in fam for r in v) else 1)" $1 $2; }
tblock() {   # rot fam system
  local r=$1 fam=$2 s=$3
  EXTCMP_PARAMS=$PARAMS EXTCMP_EXCLUDE=$RES/$tag/excluded.json EXTCMP_ENGINE_TIME= win block $size $tag $r $fam $s
  if [ $s = native ]; then
    $PYV $REPO/benchmarks/stages/w2/tools/w2_stopcheck.py $RES/$tag/rot$r/$fam-native.json $RES/$tag || { echo "STOP RULE: Gnomon $wl $tag rot$r $fam"; eval "$EXTCMP_SYNC_HOOK"; exit 4; }
  fi
  case $s in
    raphtory) fam_differs $fam $RES/$tag/verdicts-raphtory.json && EXTCMP_PARAMS=$PARAMS EXTCMP_EXCLUDE=$RES/$tag/label-only.json EXTCMP_ENGINE_TIME= win block $size $tag L$r $fam raphtory ;;
    raphtoryeq) fam_differs $fam $RES/$tag/verdicts-raphtoryeq.json && EXTCMP_PARAMS=$PARAMS EXTCMP_EXCLUDE=$RES/$tag/label-only.json EXTCMP_ENGINE_TIME= win block $size $tag L$r $fam raphtoryeq ;;
  esac
  return 0
}
timed_point() {
  pt_setup $1
  say "timed $wl $tag"
  local src=$GATED/$wl/$tag r b k i fam s
  need_file $src/excluded.json; need_file $src/verdicts-raphtoryeq.json
  restore_rap
  mkdir -p $RES/$tag; cp -a $src/verify-*.json $src/excluded.json $src/label-only.json $src/verdicts-raphtory.json $src/verdicts-raphtoryeq.json $src/verdict.md $src/verdict-eq.md $src/params-used.json $RES/$tag/
  proto_p1
  local -a SY=(native raphtory raphtoryeq)
  for r in 1 2; do b=0
    for fam in $FAMS; do
      k=$(( b % 3 )); order=("${SY[@]:k}" "${SY[@]:0:k}")
      if [ $r = 2 ]; then rev=(); for (( i=${#order[@]}-1; i>=0; i-- )); do rev+=("${order[i]}"); done; order=("${rev[@]}"); fi
      for s in "${order[@]}"; do tblock $r $fam $s; done
      b=$((b + 1))
    done
  done
  eval "$EXTCMP_SYNC_HOOK"
}
case $RL_MODE in
  gate) for p in $POINTS; do gate_point $p; done ;;
  timed) : "${RL_GATE:?}"; GATED=$RL_GATE; for p in $POINTS; do timed_point $p; done ;;
  smoke) for p in $POINTS; do gate_point $p; done; GATED=$RESL; for p in $POINTS; do timed_point $p; done ;;
esac
kill -TERM $RSSP 2>/dev/null
if [ "$RL_MODE" != gate ]; then $PYV $TL/rl_table.py --res $RESL --out $RESL/table.md | head -c 3000 || echo "table failed"; fi
cp -au $RESL/. $OUT/; say "raphlike body done"
