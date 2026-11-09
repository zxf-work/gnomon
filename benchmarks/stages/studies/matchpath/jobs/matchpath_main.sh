#!/bin/bash
# matchpath job body (run by matchpath.sbatch in its own process group). Per point (one C4 store per workload, K10M-d0):
#   1. frozen log copied and checked (its sha256 = the parameter file's), C4 store restored (confirm, sha256 sidecar);
#   2. answer check, untimed: the kit's verify step for both systems (page form, every instance; the timed records' hash),
#      then tools/mp_check.py, the whole-answer comparison of Step 1, on every instance; a differing instance is excluded
#      from nativematch's timing with its label (excluded.json) and reported;
#   3. two timed rotations, families T (snap.hop2), N (nbr.evolution), B (b.snap.hop2@now), native and nativematch
#      interleaved per family, order rotated per family and reversed in rotation 2; the W1/W2 timed protocol (proto_p1:
#      2 warm-ups + 10 timed calls, 3 above 1 s, 10 s ceiling, censor-once, 60 s prime kill, in-process killable worker,
#      no result cache), uninstrumented;
#   4. the engine-time pass (rotation E: EXTCMP_ENGINE_TIME=1, 2 warm-ups + 3 timed) for both systems.
source ${NEXT_STAGE_ROOT:?}/src/benchmarks/stages/studies/lib.sh
: "${OUT:?}" "${MP_MODE:?}"
S=$NEXT_STAGE_ROOT
POINTS=${MP_POINTS:-w1:K10M-d0 w2:K10M-d0 w1x:K10M-d0}   # w1x: the W1 extension (series.count, agg.rel_bucket, their .w10 forms, b.snap.hop2@T), last
PD=$S/params; [ "$MP_MODE" = smoke ] && PD=$S/params-smoke
TL=$REPO/$STG/matchpath/tools
RESL=${LOCAL:-${TMPDIR:-/tmp}}/mpres; mkdir -p $RESL
need_file $PD/SHA256SUMS; ( cd $PD && sha256sum -c --quiet SHA256SUMS ) || { echo "PARAMETER FILES DO NOT MATCH SHA256SUMS: stopping"; exit 2; }
export PYTHONPATH=$PYTHONPATH:$TL
SYS=(native nativematch)
pt_setup() {   # wl:tag
  wl=${1%%:*}; tag=${1#*:}; setup_root ${LOCAL:-${TMPDIR:-/tmp}}/mp-$wl-$tag
  case $tag in K1M*|W5*) size=1000000 ;; K10M*) size=10000000 ;; *) echo "UNKNOWN INPUT SIZE $tag: stopping"; exit 2 ;; esac
  base=${wl:0:2}   # w1x, w1y read W1's log and store; w2y, w2d W2's
  case $wl in w1x) FAMS="S W B" ;; w1y) FAMS="P T D B" ;; w2y) FAMS="T D B" ;; w5) FAMS="P T S W D N B" ;; *) FAMS="T N B" ;; esac
  export EXTCMP_SYNC_HOOK="mkdir -p $RESL/$wl && cp -au $ROOT/results/. $RESL/$wl/ && cp -au $RESL/. $OUT/"
  source $KIT/lib/env.sh
  PARAMS=$PD/$wl/params-$tag.json; need_file $PARAMS
  if [ ! -s $LOGS/$tag.eventlog.jsonl ]; then
    case $base in
      w1) cp -a $E/v3/eventlogs/$tag.eventlog.* $LOGS/
          ( cd $LOGS && grep -E "  $tag\.eventlog\.jsonl$" $E/v3/eventlogs/SHA256SUMS | sha256sum -c --quiet ) || { echo "FROZEN LOG CHECK FAILED $wl $tag: stopping"; exit 2; } ;;
      w2) cp -a $E/w2-powerlaw/check1m/campaign/eventlogs/$tag.eventlog.* $LOGS/
          case $tag in K10M-d10) sn=W2-10M-d10 ;; K1M-d10) sn=W2-1M-d10 ;; *) sn=powerlaw-$size ;; esac
          want=$(awk -v f="$sn.eventlog.jsonl" '$2 == f {print $1}' $E/w2-powerlaw/eventlogs/SHA256SUMS); got=$(sha256sum $LOGS/$tag.eventlog.jsonl | cut -d' ' -f1)
          [ -n "$want" ] && [ "$want" = "$got" ] || { echo "FROZEN LOG CHECK FAILED $wl $tag ($got vs $want): stopping"; exit 2; } ;;
      w5) W5D=$E/w5-wikidata; cp -a $W5D/campaign/eventlogs/$tag.eventlog.jsonl $W5D/eventlogs/$tag.eventlog.jsonl.meta.json $LOGS/ 2>/dev/null
          want=$(awk -v f="$tag.eventlog.jsonl" '$2 == f {print $1}' $W5D/eventlogs/SHA256SUMS-$tag); got=$(sha256sum $LOGS/$tag.eventlog.jsonl | cut -d' ' -f1)
          [ -n "$want" ] && [ "$want" = "$got" ] || { echo "FROZEN LOG CHECK FAILED $wl $tag ($got vs $want): stopping"; exit 2; }
          # the belief-time sidecar from the belief parameter file, as the W5 and Raphtory stages build it
          $PYV - "$LOGS/$tag.eventlog.jsonl" "$W5D/params-c3/params-$tag-bel.json" <<'PY' || { echo "SIDECAR FAILED: stopping"; exit 2; }
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
    # the parameter file names the log its instances were drawn from: the same bytes
    $PYV -c "import json,sys,hashlib; d=json.load(open(sys.argv[1])); w=d['log']['sha256']; h=hashlib.sha256(open(sys.argv[2],'rb').read()).hexdigest(); print('log', sys.argv[2], h, 'params log', w); sys.exit(0 if h == w else 1)" $PARAMS $LOGS/$tag.eventlog.jsonl \
      || { echo "PARAMETER FILE LOG IS NOT THIS LOG: stopping"; exit 2; }
    echo "log $wl $tag checked"
  fi
  case $base in w5) SD=$E/studies/sizes/stores/$tag ;; *) SD=$E/confirm/$base/stores/$tag ;; esac
  [ -d $STORES/native-$tag ] || restore_tar $SD/native-$tag.tar.zst $STORES
  need_dir $STORES/native-$tag
  cp -a $SD/load-native.json $RESL/load-native-$base-$tag.json
}
win() { timeout -k 60 3600 "$KIT/lib/window.sh" "$@" || echo "WINDOW FAILED $* $(utc)"; }
check_point() {
  proto_p1
  # MP_VERIFY_SYSTEMS (default both): the kit's verify step per system. "native" alone leaves nativematch's page check
  # to mp_check.py, which calls the same function with the same arguments without the verify step's 30 s cap
  say "answer check $wl $tag: verify (page form), ${MP_VERIFY_SYSTEMS:-native nativematch}"
  mkdir -p $RES/$tag; cp $PARAMS $RES/$tag/params-used.json
  for s in ${MP_VERIFY_SYSTEMS:-native nativematch}; do
    EXTCMP_PARAMS=$PARAMS P1_VTAG=reg win verify $size $tag - - $s
    EXTCMP_PARAMS=$PARAMS P1_VTAG=bel EXTCMP_VERIFY_FAMILY=B win verify $size $tag - - $s
  done
  say "answer check $wl $tag: whole answers (mp_check.py), every instance"
  # each check process under an address-space cap (MP_CHECK_GB, default 48 GB; MP_CHECK_WORKERS of them, default 4): an
  # answer that outgrows it ends that instance as an error, reported, instead of the node's memory (a W2 hub paths.k grew
  # past 100 GB on the development host)
  ( ulimit -v $(( ${MP_CHECK_GB:-48} * 1024 * 1024 )); $PYV $TL/mp_check.py --store $STORES/native-$tag --params $PARAMS --label "$wl $tag" \
       --per 1500 --workers ${MP_CHECK_WORKERS:-4} --out $RES/$tag/whole-cells.jsonl --md $RES/$tag/whole.md > $RES/$tag/whole.log 2>&1 ); echo "whole rc=$?" | tee $RES/$tag/whole.rc
  tail -n 20 $RES/$tag/whole.md
  $PYV $TL/mp_gate.py --dir $RES/$tag --out $RES/$tag/excluded.json --md $RES/$tag/verdict.md; echo "gate $wl $tag rc=$?" | tee -a $RESL/gate-rc.txt
  cat $RES/$tag/verdict.md | head -n 30
  eval "$EXTCMP_SYNC_HOOK"
}
tblock() {   # rot fam system [engine]
  local r=$1 fam=$2 s=$3 et=${4:-}
  EXTCMP_PARAMS=$PARAMS EXTCMP_EXCLUDE=$RES/$tag/excluded.json EXTCMP_ENGINE_TIME=$et win block $size $tag $r $fam $s
}
timed_point() {
  say "timed $wl $tag"
  local r b k i fam s
  for r in 1 2; do b=0
    for fam in $FAMS; do
      k=$(( b % 2 )); order=("${SYS[@]:k}" "${SYS[@]:0:k}")
      if [ $r = 2 ]; then order=("${order[1]}" "${order[0]}"); fi
      echo "rot$r $fam ${order[*]}" >> $RES/$tag/schedule.txt
      for s in "${order[@]}"; do tblock $r $fam $s; done
      b=$((b + 1))
    done
    [ $r = 1 ] && $PYV $TL/mp_rot1.py --dir $RES/$tag | tee -a $RES/$tag/rot1-censored.txt
  done
  say "engine-time pass $wl $tag"
  for fam in $FAMS; do for s in "${SYS[@]}"; do EXTCMP_P1_REPS=3 tblock E $fam $s 1; done; done
  eval "$EXTCMP_SYNC_HOOK"
}
for p in $POINTS; do pt_setup $p; check_point; timed_point; rm -rf $STORES/native-$tag; done
$PYV $TL/mp_table.py --res $RESL --out $RESL/table.md --duckdb-w1 $E/final/w1/results --duckdb-w2 $E/final/w2/results \
     --duckdb-w5 $E/studies/w5/results/W5-wikidata-v2-J49 --duckdb-w2d $E/studies/t10/results/timed-w2-J55 > $RESL/table.log 2>&1 || echo "table failed (see table.log)"
head -c 4000 $RESL/table.md 2>/dev/null
cp -au $RESL/. $OUT/; say "matchpath body done"
