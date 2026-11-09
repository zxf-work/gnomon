#!/bin/bash
# W3 timed job body (run by w3_timed.sbatch in its own process group).
source ${W3_STAGE_ROOT:-${GNOMON_BENCH_ROOT}/final/w3}/src/benchmarks/stages/w3/jobs/w3_lib.sh
: "${OUT:?}" "${W3_POINT:?}"
pt_config $W3_POINT || exit 2
# W3_SMOKE=1 only (results go to smoke-timed-<job>): a small cell subset and, for the hash-stop test, a doctored reference. Unset: nothing changes.
SMOKE_CELLS=""; SMOKE_N=""
if [ "${W3_SMOKE:-}" = 1 ]; then
  SMOKE_CELLS=${W3_SMOKE_CELLS:-}; SMOKE_N=${W3_SMOKE_N:-}
  if [ -n "$SMOKE_CELLS" ]; then BLOCKS=ALL; fi
  if [ -n "${W3_SMOKE_REF:-}" ]; then REF=$W3_SMOKE_REF; fi
  if [ -n "${W3_SMOKE_OLDT:-}" ]; then OLDT=$W3_SMOKE_OLDT; fi
fi
setup_root ${LOCAL:-${TMPDIR:-/tmp}}/w3t
RESL=${LOCAL:-${TMPDIR:-/tmp}}/w3t/res; mkdir -p $RESL
EXC=$GATED/exclusions-$PT.json
if [ "$C3" = 1 ]; then
  : "${C3_DRIFT_DIR:?C3 mode: C3_DRIFT_DIR must be set}" "${OLDT:?C3 mode: C3_OLD_TIMED_$PT must be set}"
  ( cd $C3_DRIFT_DIR && sha256sum -c SHA256SUMS ) || { echo "C3 DRIFT FILES DO NOT MATCH SHA256SUMS"; exit 2; }
  EXC=$C3_DRIFT_DIR/drift-exclude-W3-$PT.json; [ -s $REF ] || { echo "NO RECORDED REFERENCE $REF"; exit 2; }
fi
[ -s $EXC ] || { echo "NO GATE EXCLUSIONS $EXC"; exit 2; }
( cd $PARD && sha256sum -c SHA256SUMS --ignore-missing ) || { echo "W3 parameter files do not match SHA256SUMS"; exit 2; }
HISTL=${LOCAL:-${TMPDIR:-/tmp}}/w3hist.duckdb; cp $HIST $HISTL || exit 2
cp $EXC $OUT/exclusions-used.json; cp $PARAMS $OUT/params-used.jsonl; cp $([ "$C3" = 1 ] && echo $C3_W3_GATE || echo $GATED)/gate-$PT.md $OUT/gate-used.md
TR="${EXTCMP_NUMA_CLIENT:+$EXTCMP_NUMA_CLIENT }$PYV $TOOLS/w3_timed.py run"
syncall() { cp -au $RESL/. $OUT/; }
SKIPPED=0
isserver() { case $1 in postgres|neo4j|memgraph) return 0 ;; *) return 1 ;; esac; }

run_block() {   # token rot blk et engine(""|engine) [cells]
  local s=$1 r=$2 blk=$3 et=$4 eng=$5 cells=${6:-} base=$(base_of $1) th="" n inst out
  case $s in duckdb64) th=64 ;; duckdb32) th=32 ;; esac
  n=${SMOKE_N:-$(n_of $base)}; inst=$(seq -s, 0 $((n-1))); out=$RESL/rot$r; mkdir -p $out
  [ -n "$cells" ] || cells=${SMOKE_CELLS:-$(cells_of_block $blk)}
  local -a A=(--system $base --target "${TARGET}" --params $PARAMS --cells "$cells" --settings $(settings_of $base)
              --instances $inst --hist-db $HISTL --rotation $r --exclude $EXC --excl-system $base
              --out $out/$blk-$s.json --jsonl $out/$blk-$s-calls.jsonl --label "w3-final $PT rot$r $blk $s")
  if [ "$eng" = engine ]; then
    A=(--system $base --target "${TARGET}" --params $PARAMS --cells "$cells" --settings $(settings_of $base)
       --instances 0,1,2 --hist-db $HISTL --rotation E --exclude $EXC --excl-system $base --engine-pass
       --out $out/$blk-$s.json --jsonl $out/$blk-$s-calls.jsonl --label "w3-final $PT engine pass $blk $s")
  fi
  say "block $PT rot$r $blk $s ($eng et=$et threads=${th:-default})"
  env ${th:+W3_DUCKDB_THREADS=$th} EXTCMP_ENGINE_TIME=$et $TR "${A[@]}" 2>&1 | tail -n 3
}
tblock() {   # token rot blk: one timed block of one system (server started and stopped around it)
  local s=$1 r=$2 blk=$3 base=$(base_of $1) et=0 TARGET="" k
  case $base in neo4j|memgraph) et=1 ;; esac
  if isserver $base; then
    for k in 1 2; do srv_start $base && break; echo "SERVER START FAILED $base (try $k)"; srv_stop $base; sleep 30; TARGET=""; done
    [ -n "$TARGET" ] || { echo "BLOCK SKIPPED: $base would not start ($PT rot$r $blk)" | tee -a $RESL/skipped.txt; SKIPPED=$((SKIPPED+1)); return 0; }
  else
    restore_sys $base || { echo "BLOCK SKIPPED: no $base store" | tee -a $RESL/skipped.txt; return 0; }
  fi
  run_block $s $r $blk $et "" "" 
  isserver $base && srv_stop $base
  syncall
  if [ $base = gnomon ]; then
    $PYV $TOOLS/w3_stopcheck.py $RESL/rot$r/$blk-gnomon.json $PT || { echo "STOP RULE: Gnomon block $PT rot$r $blk"; syncall; exit 4; }
    if [ "$C3" = 1 ]; then   # the answer check from the timed hashes (no gate job)
      $PYV $C3T/c3_w3_hashcheck.py --point $PT --ref $REF --old-timed $OLDT --new $RESL/rot$r/$blk-gnomon.json \
        --out $RESL/rot$r/$blk-gnomon-hashcheck.md || { [ "${W3_C3_HASH_STOP:-1}" = 0 ] || { echo "HASH CHECK: Gnomon block $PT rot$r $blk differs from the recorded reference"; syncall; exit 4; }; }
    fi
  fi
}
rotations() {   # two rotations at SF1, one at SF10; order rotated per block, flipped in the even rotation (W2's schedule)
  local r b k s n order rev i blk
  local -a SY=($SYSTEMS); n=${#SY[@]}
  for r in $(seq 1 $ROT); do
    say "$PT rotation $r"; b=0
    for blk in $BLOCKS; do
      k=$(( b % n )); order=("${SY[@]:k}" "${SY[@]:0:k}")
      if [ $(( r % 2 )) = 0 ]; then rev=(); for (( i=${#order[@]}-1; i>=0; i-- )); do rev+=("${order[i]}"); done; order=("${rev[@]}"); fi
      echo "rot $r block $blk: ${order[*]}" | tee -a $RESL/schedule.txt
      for s in "${order[@]}"; do tblock $s $r $blk; done
      b=$((b + 1))
    done
  done
}
epass() {   # the engine-time pass: cells every instance of which answered in rotation 1, instances 0-2, 1 warm-up + 3 timed
  #           (W3_ROT1_DIR: read rotation 1 from there instead of from this job's own results; used by w3_etime.sbatch)
  local s blk base cells TARGET R1=${W3_ROT1_DIR:-$RESL/rot1}
  say "$PT engine pass"
  for blk in $BLOCKS; do
    for s in $ESYS; do
      case " $SYSTEMS " in *" $s "*) ;; *) continue ;; esac
      base=$(base_of $s); [ -f $R1/$blk-$s.json ] || continue
      cells=$($PYV $TOOLS/w3_timed.py answered $R1/$blk-$s.json); [ -n "$cells" ] || { echo "engine pass $blk $s: no answered cell"; continue; }
      if [ $s = postgres ]; then
        TARGET=""; EXTCMP_PG_STATS=1 srv_start postgres || { echo "ENGINE PASS: postgres would not start"; continue; }
        EXTCMP_PG_STATS=1 run_block $s E $blk 1 engine "$cells"; srv_stop postgres
      else
        restore_sys $base || continue
        run_block $s E $blk 1 engine "$cells"
      fi
      syncall
    done
  done
}
main_etime() {   # W3_ONLY_ENGINE_TIME=1 (opt-in; w3_etime.sbatch): only the engine-time pass, on rotation 1 results of an earlier job
  local s
  [ -d "${W3_ROT1_DIR:-}" ] || { echo "W3_ONLY_ENGINE_TIME needs W3_ROT1_DIR (rotation 1 results of the timed job)"; return 2; }
  say "$PT: restore (engine-time pass only; rotation 1 from $W3_ROT1_DIR)"
  for s in gnomon duckdb postgres; do restore_sys $s || { echo "RESTORE FAILED $s"; return 2; }; done
  epass
  syncall
  "$KIT/lib/servers.sh" stopall
  say "$PT engine-time pass done (blocks skipped: $SKIPPED)"
}
main() {
  local s
  say "$PT: restore"
  for s in $SYSTEMS; do restore_sys $(base_of $s) || { echo "RESTORE FAILED $s"; return 2; }; done
  rotations
  epass
  $PYV $TOOLS/w3_timed.py audit-dir $RESL | tee $RESL/censor-once-audit.txt
  if [ "$C3" = 1 ]; then
    $PYV $C3T/c3_w3_hashcheck.py --point $PT --ref $REF --old-timed $OLDT --new $RESL/rot1/*-gnomon.json --out $RESL/hashcheck-$PT.md \
      | tee $RESL/hashcheck-$PT.txt
    $PYV $C3T/c3_drift.py report --sample $C3_DRIFT_DIR/drift-W3.json --new $RESL --out $RESL/drift-report-job.md || echo "drift (this job): see drift-report-job.md"
  fi
  syncall
  "$KIT/lib/servers.sh" stopall
  say "$PT timed body done (blocks skipped: $SKIPPED)"
}
if [ "${W3_ONLY_ENGINE_TIME:-0}" = 1 ]; then main_etime; else main; fi
