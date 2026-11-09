#!/bin/bash
# Thread-matched control, timed job body (run by thr_timed.sbatch in its own process group).
source ${THR_STAGE_ROOT:-${GNOMON_BENCH_ROOT}/final/thr}/src/benchmarks/stages/threads/jobs/thr_lib.sh
: "${OUTB:?}" "${THR_WLS:?}" "${THR_GATE:?}"
seed_cfg() {   # tag cfg: the gate's exclusion list and parameter record of this configuration into the local root
  local src=$THR_GATE/$WL/cfg-$2/$1
  [ -s $src/excluded.json ] || { echo "NO GATE RECORDS for $WL $2 $1 ($src/excluded.json)"; return 1; }
  mkdir -p $ROOT/results-cfg-$2/$1; cp -a $src/excluded.json $src/params-used.json $ROOT/results-cfg-$2/$1/
  [ -e $src/verdicts.txt ] && cp -a $src/verdicts.txt $ROOT/results-cfg-$2/$1/
  return 0; }
tblock() {   # tag rot fam cfg [et]: one timed window (window.sh block), then the checks
  local tag=$1 r=$2 fam=$3 c=$4 et=${5:-} size s excl rec
  size=$(size_of $tag); set_cfg $c; s=$CSYS; useroot cfg-$c
  excl=$RES/$tag/excluded.json; [ -s $excl ] || excl=""
  if [ $s = postgres ] && [ "$et" = 1 ]; then
    EXTCMP_P1_REPS=3 EXTCMP_PG_STATS=1 EXTCMP_PARAMS=$PD/params-$tag.json EXTCMP_EXCLUDE=$excl EXTCMP_ENGINE_TIME=$et win block $size $tag $r $fam $s
  elif [ "$et" = 1 ]; then
    EXTCMP_P1_REPS=3 EXTCMP_PARAMS=$PD/params-$tag.json EXTCMP_EXCLUDE=$excl EXTCMP_ENGINE_TIME=$et win block $size $tag $r $fam $s
  else
    EXTCMP_PARAMS=$PD/params-$tag.json EXTCMP_EXCLUDE=$excl EXTCMP_ENGINE_TIME= win block $size $tag $r $fam $s
  fi
  rec=$RES/$tag/rot$r/$fam-$s.json
  $PYV $TOOLS/thr_check.py applied $rec $c || { echo "OVERRIDE NOT APPLIED: $WL $tag rot$r $fam $c ($CUR)"; syncall; exit 7; }
  case $c in
    g*) $PYV $TOOLS/thr_stopcheck.py $rec $c || { echo "STOP RULE: Gnomon window $WL $tag rot$r $fam $c"; syncall; exit 4; } ;;
  esac
}
rotations() {   # tag: two rotations over CFGS, run_p1.sh's schedule
  local tag=$1 r b k c order rev i n fam
  local -a CF=($CFGS); n=${#CF[@]}
  for r in 1 2; do
    say "rotations $WL $tag rotation $r"
    : > $OUT/schedule-$tag-$r.txt; b=0
    for fam in $(fams_of $tag); do
      k=$(( b % n )); order=("${CF[@]:k}" "${CF[@]:0:k}")
      if [ $(( r % 2 )) = 0 ]; then rev=(); for (( i=${#order[@]}-1; i>=0; i-- )); do rev+=("${order[i]}"); done; order=("${rev[@]}"); fi
      for c in "${order[@]}"; do
        echo "block $tag $r $fam $c" >> $OUT/schedule-$tag-$r.txt
        tblock $tag $r $fam $c
      done
      b=$((b + 1))
    done
  done
}
epass() {   # tag: the engine-time pass on every configuration
  local tag=$1 c fam
  say "engine pass $WL $tag"
  for fam in $(fams_of $tag); do for c in $CFGS; do tblock $tag E $fam $c 1; done; done
}
trap 'echo "=== TERM: syncing records $(utc)"; [ -n "${ROOT:-}" ] && syncall; exit 5' TERM
main() {
  local wl tag s c
  proto_p1; export EXTCMP_WINDOW_TIMEOUT_S=${THR_TIMED_WINDOW_S:-10800}      # a timed window is cut at 3 h (the W1/W2 limit of 1 h could cut a slow single-thread family window)
  for wl in $THR_WLS; do
    wl_set $wl; setup_root ${LOCAL:-${TMPDIR:-/tmp}}/thr-t-$wl; OUT=$OUTB/$wl; mkdir -p $OUT
    getlogs $WTAGS
    ( cd $PD && sha256sum -c SHA256SUMS ) || { echo "parameter files of $wl do not match SHA256SUMS"; return 2; }
    for tag in $WTAGS; do
      point_env $tag
      say "point $wl $tag: restore"
      for s in $SYSTEMS; do restore $tag $s-$tag $(parent_of $s) || return 2; done
      pg_conf $tag || return 2
      for c in $CFGS; do seed_cfg $tag $c || return 2; done
      cp -a $STORE_DIR/$tag/load-native.json $STORE_DIR/$tag/load-duckdbsql.json $STORE_DIR/$tag/load-postgres.json $OUT/ 2>/dev/null
      rotations $tag
      epass $tag
      syncall
      "$KIT/lib/servers.sh" stopall
      for s in $SYSTEMS; do rm -rf $(parent_of $s)/$s-$tag; done      # keep ${LOCAL:-${TMPDIR:-/tmp}} to one point
    done
    syncall
  done
  say "timed body done"
}
main
