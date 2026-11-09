#!/bin/bash
# Timed job body (run by x_timed.sbatch in its own process group).
source ${NEXT_STAGE_ROOT:?}/src/benchmarks/stages/studies/t10/jobs/x_lib.sh
: "${OUT:?}" "${ROOT:?}" "${X_GATE:?}"
x_root $ROOT
seed_root() {   # root tag: the answer-check records (verify, excluded, verdicts) into the local root
  local src=$X_GATE/$1/$2; [ -d $src ] || { echo "NO GATE RECORDS for $1 $2"; return 1; }
  mkdir -p $ROOT/results-$1/$2; cp -a $src/*.json $src/*.txt $ROOT/results-$1/$2/ 2>/dev/null; echo "seeded $1/$2 from $src"; }
r10m_list() {   # tag system: this system's R-10M skip list, once per root
  local tag=$1 s=$2 src sk=$RES/$tag/r10m-$s.json
  [ -e $sk ] && { echo $sk; return 0; }
  case $CUR:$s in *:native) src=$R10M_NATIVE ;; t32reg:*) src=$R10M_T32 ;; *) src=$R10M_REG ;; esac
  $PYV $REPO/scripts/r10m.py build --from $src --point K1M --system $s --out $sk 1>&2 || return 1
  echo $sk
}
tblock() {   # tag rot fam sys et: one timed window, then the stop rule after Gnomon's
  local tag=$1 r=$2 fam=$3 s=$4 et=${5:-} sk=""
  local excl=$RES/$tag/excluded.json; [ -s $excl ] || excl=""
  if [ $X_SET = w2 ]; then
    sk=$(r10m_list $tag $s) || { echo "R-10M BUILD FAILED $tag $CUR $s: the rule cannot be applied, stopping"; syncall; exit 6; }
  fi
  EXTCMP_PARAMS=$PARDIR/params-$tag.json EXTCMP_EXCLUDE=$excl EXTCMP_R10M_SKIP=$sk EXTCMP_ENGINE_TIME=$et win block $SIZE $tag $r $fam $s
  if [ $s = native ]; then
    $PYV $XT/x_stopcheck.py $RES/$tag/rot$r/$fam-native.json $RES/$tag || { echo "STOP RULE: Gnomon window $tag rot$r $fam ($CUR)"; syncall; exit 4; }
  fi
  if [ -n "${X_ANSWER_STOP:-}" ]; then
    $PYV $XT/x_anscheck.py $RES/$tag/rot$r/$fam-$s.json $RES/$tag || { echo "ANSWER DIFFERENCE: stopping, $s $tag rot$r $fam ($CUR)"; syncall; exit 7; }
  fi
}
rotations() {   # tag
  local tag=$1 r b k s order rev i n fam
  local -a SY=($SYSTEMS); n=${#SY[@]}
  useroot reg
  for (( r=1; r<=ROTS; r++ )); do
    say "reg $tag rotation $r"
    : > $RES/schedule-$tag-$r.txt; b=0
    for fam in $FAMS; do
      k=$(( b % n )); order=("${SY[@]:k}" "${SY[@]:0:k}")
      if [ $(( r % 2 )) = 0 ]; then rev=(); for (( i=${#order[@]}-1; i>=0; i-- )); do rev+=("${order[i]}"); done; order=("${rev[@]}"); fi
      for s in "${order[@]}"; do
        echo "block $SIZE $tag $r $fam $s" >> $RES/schedule-$tag-$r.txt
        if [ $s = clickhouse ]; then tblock $tag $r $fam $s 1; else tblock $tag $r $fam $s; fi
      done
      b=$((b + 1))
    done
  done
}
t32() {   # tag
  local tag=$1 r fam
  useroot t32reg; say "t32reg $tag"
  for (( r=1; r<=ROTS; r++ )); do for fam in $FAMS; do EXTCMP_DUCKDBSQL_THREADS=32 tblock $tag $r $fam duckdbsql; done; done
}
epass() {   # tag
  local tag=$1 s fam
  useroot reg; say "reg $tag engine pass"
  for fam in $FAMS; do
    for s in native duckdbsql postgres; do
      if [ $s = postgres ]; then EXTCMP_P1_REPS=3 EXTCMP_PG_STATS=1 tblock $tag E $fam $s 1
      else EXTCMP_P1_REPS=3 tblock $tag E $fam $s 1; fi
    done
  done
}
main() {
  local tag s
  echo "timed body start $(utc) on $(hostname), ROOT $ROOT"
  getlogs
  proto_p1
  for tag in $POINTS; do
    export EXTCMP_BELIEFQ_INDEXES=1
    useroot reg
    say "point $tag: restore"; restore_all $tag
    seed_root reg $tag || return 2
    if [ "$X_T32" = 1 ]; then seed_root t32reg $tag || return 2; fi
    rotations $tag
    if [ "$X_T32" = 1 ]; then t32 $tag; fi
    epass $tag
    useroot reg; cp $PARDIR/params-$tag.json $RES/$tag/params-used.json
    $PYV $REPO/scripts/extcmp_p1.py report --dir $RES/$tag --rotations $ROTS --out $RES/$tag/p1-report > $RES/$tag/p1-report.md 2>&1 || echo "report failed $tag"
    syncall
    "$KIT/lib/servers.sh" stopall
    for s in $SYSTEMS; do rm -rf $(parent_of $s)/$s-$tag; done      # keep ${LOCAL:-${TMPDIR:-/tmp}} to one point
  done
  syncall; say "timed body done"
}
main
