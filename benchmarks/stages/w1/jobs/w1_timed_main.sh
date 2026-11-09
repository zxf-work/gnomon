#!/bin/bash
# W1 timed job body (run by w1_timed.sbatch in its own process group).
source ${W1_STAGE_ROOT:-${GNOMON_BENCH_ROOT}/final/w1}/src/benchmarks/stages/w1/jobs/w1_lib.sh
: "${OUT:?}" "${ROOT:?}" "${W1_POINTS:?}" "${W1_GATE:?}"
setup_root $ROOT
gate_dir() {   # root tag: the gate job's record directory of this point
  local g; for g in $W1_GATE; do [ -d $g/$1/$2 ] && { echo $g/$1/$2; return 0; }; done; return 1; }
seed_root() {   # root tag: the answer-check records (verify, excluded, verdicts, params-used) into the local root
  local src; src=$(gate_dir $1 $2) || { echo "NO GATE RECORDS for $1 $2"; return 1; }
  mkdir -p $ROOT/results-$1/$2; cp -a $src/*.json $src/*.txt $ROOT/results-$1/$2/ 2>/dev/null; echo "seeded $1/$2 from $src"; }
tblock() {   # size tag rot fam sys params et: one timed window (window.sh block), then the stop rule after Gnomon's
  local size=$1 tag=$2 r=$3 fam=$4 s=$5 params=$6 et=${7:-}
  local excl=$RES/$tag/excluded.json; [ -s $excl ] || excl=""
  if [ "$C3" = 1 ] && [ $s = duckdbsql ]; then   # the drift sample: DuckDB (SQL) times only its sampled cells
    excl=$C3_DRIFT_DIR/drift-exclude-W1-$CUR-$tag.json; [ -s $excl ] || { echo "NO DRIFT FILE $excl"; syncall; exit 2; }; fi
  EXTCMP_PARAMS=$params EXTCMP_EXCLUDE=$excl EXTCMP_ENGINE_TIME=$et win block $size $tag $r $fam $s
  if [ $s = native ]; then
    $PYV $TOOLS/w1_stopcheck.py $RES/$tag/rot$r/$fam-native.json $(if [ "$C3" = 1 ]; then c3_ref_dir $CUR $tag; else echo $RES/$tag; fi) || { echo "STOP RULE: Gnomon window $tag rot$r $fam ($CUR)"; syncall; exit 4; }
  fi
}
rotations() {   # mode tag params: two rotations over SYSTEMS, run_p1.sh's schedule
  local mode=$1 tag=$2 params=$3 size=$(size_of $2) r b k s order rev i n
  local -a SY=($TSYS); n=${#SY[@]}
  useroot $mode
  for r in 1 2; do
    say "$mode $tag rotation $r"
    : > $RES/schedule-$tag-$r.txt; b=0
    for fam in $(fams_of $tag); do
      k=$(( b % n )); order=("${SY[@]:k}" "${SY[@]:0:k}")
      if [ $(( r % 2 )) = 0 ]; then rev=(); for (( i=${#order[@]}-1; i>=0; i-- )); do rev+=("${order[i]}"); done; order=("${rev[@]}"); fi
      for s in "${order[@]}"; do
        echo "block $size $tag $r $fam $s" >> $RES/schedule-$tag-$r.txt
        if [ $s = clickhouse ]; then tblock $size $tag $r $fam $s $params 1; else tblock $size $tag $r $fam $s $params; fi
      done
      b=$((b + 1))
    done
  done
}
t32() {   # mode tag params: DuckDB (SQL) at 32 threads, two rotations
  local mode=$1 tag=$2 params=$3 size=$(size_of $2) r
  useroot t32$mode
  for r in 1 2; do for fam in $(fams_of $tag); do EXTCMP_DUCKDBSQL_THREADS=32 tblock $size $tag $r $fam duckdbsql $params; done; done
}
epass() {   # mode tag params: the engine-time pass
  local mode=$1 tag=$2 params=$3 size=$(size_of $2) s
  useroot $mode; say "$mode $tag engine pass"
  for fam in $(fams_of $tag); do
    for s in $ESYS; do
      if [ $s = postgres ]; then EXTCMP_P1_REPS=3 EXTCMP_PG_STATS=1 tblock $size $tag E $fam $s $params 1
      else EXTCMP_P1_REPS=3 tblock $size $tag E $fam $s $params 1; fi
    done
  done
}
main() {
  getlogs $W1_POINTS
  ( cd $SPDIR && sha256sum -c SHA256SUMS ) || { echo "single-parameter files do not match SHA256SUMS"; return 2; }
  ( cd $P1DIR && sha256sum -c SHA256SUMS ) || { echo "P1 parameter files do not match SHA256SUMS"; return 2; }
  c3_drift_check || return 2
  proto_p1
  for tag in $W1_POINTS; do
    point_env $tag
    say "point $tag: restore"
    for s in $TSYS; do sd=$(store_src $tag $s) || { echo "NO STORE $tag $s"; return 2; }; RESTORE_FROM=$sd restore $tag $s-$tag $(parent_of $s) || return 2; done
    modes="sp"; [ -e $P1DIR/params-$tag.json ] && modes="sp p1"
    for m in $modes; do seed_root $m $tag || return 2; [ "$C3" = 1 ] || { seed_root t32$m $tag || return 2; }; done
    mkdir -p $ROOT/results-sp/$tag; cp -a $STORE_DIR/$tag/load-*.json $ROOT/results-sp/$tag/
    if [ "$C3" = 1 ]; then for s in $C3_RESTORE; do [ -e $ROOT/results-sp/$tag/load-$s.json ] || cp -a $C3_STORE_SRC/$tag/load-$s.json $ROOT/results-sp/$tag/ 2>/dev/null; done; fi
    for m in $modes; do
      if [ $m = sp ]; then params=$SPDIR/params-$tag.json; else params=$P1DIR/params-$tag.json; fi
      rotations $m $tag $params
      [ "$C3" = 1 ] || t32 $m $tag $params
      epass $m $tag $params
      useroot $m; cp $params $RES/$tag/params-used.json   # the report reads the parameter file the cells ran on (run_p1.sh copies it too)
      $PYV $REPO/scripts/extcmp_p1.py report --dir $RES/$tag --rotations 2 --out $RES/$tag/p1-report > $RES/$tag/p1-report.md 2>&1 || echo "report failed $m $tag"
      syncall
    done
    "$KIT/lib/servers.sh" stopall
    for s in $SYSTEMS; do rm -rf $(parent_of $s)/$s-$tag; done      # keep ${LOCAL:-${TMPDIR:-/tmp}} to one point
  done
  if [ "$C3" = 1 ]; then   # this job's part of the drift sample (the stage figure is computed over every timed job of the stage)
    $PYV $C3T/c3_drift.py report --sample $C3_DRIFT_DIR/drift-W1.json --new $OUT --out $OUT/drift-report-job.md || echo "drift (this job): see $OUT/drift-report-job.md"
  fi
  syncall; say "timed body done"
}
main
