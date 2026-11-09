#!/bin/bash
# Timed job body (run by y_timed.sbatch in its own process group).
source ${NEXT_STAGE_ROOT:?}/src/benchmarks/stages/studies/covy/jobs/y_lib.sh
: "${OUT:?}" "${ROOT:?}" "${X_GATE:?}"
x_root $ROOT
seed_root() {
  local src=$X_GATE/$1/$2; [ -d $src ] || { echo "NO GATE RECORDS for $1 $2"; return 1; }
  mkdir -p $ROOT/results-$1/$2; cp -a $src/*.json $src/*.txt $ROOT/results-$1/$2/ 2>/dev/null; echo "seeded $1/$2 from $src"; }
r10m_list() {   # tag system: this system's R-10M skip list (K1M for families P..M, K1M-dX for B), merged, once
  local tag=$1 s=$2 src sk=$RES/$tag/r10m-$s.json
  [ -e $sk ] && { echo $sk; return 0; }
  case $s in native) src=$R10M_NATIVE ;; *) src=$R10M_REG ;; esac
  $PYV $REPO/scripts/r10m.py build --from $src --point K1M --system $s --out $sk.reg 1>&2 || return 1
  $PYV $REPO/scripts/r10m.py build --from $src --point K1M${tag#K10M} --system $s --out $sk.b 1>&2 || return 1
  $PYV $XT/y_r10m_merge.py $sk.reg $sk.b $sk 1>&2 || return 1
  echo $sk
}
tblock() {   # tag rot fam sys et
  local tag=$1 r=$2 fam=$3 s=$4 et=${5:-} sk=""
  local excl=$RES/$tag/excluded.json; [ -s $excl ] || excl=""
  sk=$(r10m_list $tag $s) || { echo "R-10M BUILD FAILED $tag $CUR $s: the rule cannot be applied, stopping"; syncall; exit 6; }
  EXTCMP_PARAMS=$PARDIR/params-$tag.json EXTCMP_EXCLUDE=$excl EXTCMP_R10M_SKIP=$sk EXTCMP_ENGINE_TIME=$et win block $SIZE $tag $r $fam $s
  if [ $s = native ]; then
    $PYV $TT/x_stopcheck.py $RES/$tag/rot$r/$fam-native.json $RES/$tag || { echo "STOP RULE: Gnomon window $tag rot$r $fam ($CUR)"; syncall; exit 4; }
  fi
}
rotations() {
  local tag=$1 r b k s order rev i n fam
  local -a SY=($SYSTEMS); n=${#SY[@]}
  useroot reg
  for (( r=${X_ROT_FROM:-1}; r<=ROTS; r++ )); do
    say "reg $tag rotation $r"
    [ -s $RES/schedule-$tag-$r.txt ] || : > $RES/schedule-$tag-$r.txt; b=0
    for fam in $FAMS; do
      # X_DONE_FAMS (a continuation): families of the first rotation run here that a stopped job already finished; skipped, the schedule unchanged
      if [ $r = "${X_ROT_FROM:-1}" ] && [[ " ${X_DONE_FAMS:-} " == *" $fam "* ]]; then echo "continuation: rotation $r family $fam kept from $X_PREV"; b=$((b + 1)); continue; fi
      k=$(( b % n )); order=("${SY[@]:k}" "${SY[@]:0:k}")
      if [ $(( r % 2 )) = 0 ]; then rev=(); for (( i=${#order[@]}-1; i>=0; i-- )); do rev+=("${order[i]}"); done; order=("${rev[@]}"); fi
      for s in "${order[@]}"; do
        echo "block $SIZE $tag $r $fam $s" >> $RES/schedule-$tag-$r.txt
        tblock $tag $r $fam $s $(et_of $s)
      done
      b=$((b + 1))
    done
  done
}
epass() {
  local tag=$1 s fam
  useroot reg; say "reg $tag engine pass"
  for fam in $FAMS; do
    for s in native duckdbsql; do EXTCMP_P1_REPS=3 tblock $tag E $fam $s 1; done
  done
}
main() {
  local tag s
  getlogs
  proto_p1
  for tag in $POINTS; do
    export EXTCMP_BELIEFQ_INDEXES=1
    useroot reg
    say "point $tag: restore"; restore_all $tag
    seed_root reg $tag || return 2
    if [ -n "${X_PREV:-}" ]; then     # a continuation of a timed job stopped at its limit: its finished rotation records and schedules, labelled
      cp -a $X_PREV/reg/$tag/rot1 $X_PREV/reg/$tag/rot2 $RES/$tag/ 2>/dev/null; cp -a $X_PREV/reg/schedule-$tag-*.txt $RES/ 2>/dev/null
      echo "continuation of $X_PREV from rotation ${X_ROT_FROM:-1}, families kept [${X_DONE_FAMS:-}]" | tee $RES/$tag/CONTINUATION.txt
    fi
    rotations $tag
    epass $tag
    useroot reg; cp $PARDIR/params-$tag.json $RES/$tag/params-used.json
    $PYV $REPO/scripts/extcmp_p1.py report --dir $RES/$tag --rotations $ROTS --out $RES/$tag/p1-report > $RES/$tag/p1-report.md 2>&1 || echo "report failed $tag"
    syncall
    "$KIT/lib/servers.sh" stopall
    for s in $SYSTEMS; do rm -rf $(parent_of $s)/$s-$tag; done
  done
  syncall; say "timed body done"
}
main
