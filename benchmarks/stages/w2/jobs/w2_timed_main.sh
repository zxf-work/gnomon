#!/bin/bash
# W2 timed job body (run by w2_timed.sbatch in its own process group).
source ${W2_STAGE_ROOT:-${GNOMON_BENCH_ROOT}/final/w2}/src/benchmarks/stages/w2/jobs/w2_lib.sh
: "${OUT:?}" "${ROOT:?}" "${W2_POINTS:?}" "${W2_GATE:?}"
setup_root $ROOT
gate_dir() {   # root tag: the gate job's record directory of this point
  local g; for g in $W2_GATE; do [ -d $g/$1/$2 ] && { echo $g/$1/$2; return 0; }; done; return 1; }
seed_root() {   # root tag: the answer-check records (verify, excluded, verdicts, params-used, loads) into the local root
  local src; src=$(gate_dir $1 $2) || { echo "NO GATE RECORDS for $1 $2"; return 1; }
  mkdir -p $ROOT/results-$1/$2; cp -a $src/*.json $src/*.txt $ROOT/results-$1/$2/ 2>/dev/null; echo "seeded $1/$2 from $src"; }
tblock() {   # size tag rot fam sys et: one timed window (window.sh block), then the stop rule after Gnomon's; R-10M at the 10M points
  local size=$1 tag=$2 r=$3 fam=$4 s=$5 et=${6:-} sk=""
  local excl=$RES/$tag/excluded.json; [ -s $excl ] || excl=""
  if [ "$C3" = 1 ] && [ $s = duckdbsql ]; then   # the drift sample: DuckDB (SQL) times only its sampled cells, under its recorded R-10M list
    excl=$C3_DRIFT_DIR/drift-exclude-W2-$CUR-$tag.json; [ -s $excl ] || { echo "NO DRIFT FILE $excl"; syncall; exit 2; }
    if [ "${tag#K10M}" != "$tag" ] && [ ! -e $RES/$tag/r10m-duckdbsql.json ]; then
      local od; for od in ${C3_OLD_TIMED:-}; do [ -e $od/$CUR/$tag/r10m-duckdbsql.json ] && { cp $od/$CUR/$tag/r10m-duckdbsql.json $RES/$tag/; break; }; done
      [ -e $RES/$tag/r10m-duckdbsql.json ] || { echo "NO RECORDED R-10M LIST for duckdbsql $tag (C3_OLD_TIMED)"; syncall; exit 6; }
    fi
  fi
  if [ -n "${W2_R10M_JOB:-}" ] && [ "${tag#K10M}" != "$tag" ]; then
    # R-10M: this system's skip list from its 1M timed records of the same root (K10M <- K1M, K10M-dX <- K1M-dX)
    sk=$RES/$tag/r10m-$s.json
    [ -e $sk ] || $PYV $REPO/scripts/r10m.py build --from $W2_R10M_ROOT/$CUR --point K1M${tag#K10M} --system $s --out $sk \
      || { echo "R-10M BUILD FAILED $tag $CUR $s: the stage rule cannot be applied, stopping"; syncall; exit 6; }
  fi
  EXTCMP_PARAMS=$P1DIR/params-$tag.json EXTCMP_EXCLUDE=$excl EXTCMP_R10M_SKIP=$sk EXTCMP_ENGINE_TIME=$et win block $size $tag $r $fam $s
  if [ $s = native ]; then
    $PYV $TOOLS/w2_stopcheck.py $RES/$tag/rot$r/$fam-native.json $(if [ "$C3" = 1 ]; then c3_ref_dir $CUR $tag; else echo $RES/$tag; fi) || { echo "STOP RULE: Gnomon window $tag rot$r $fam ($CUR)"; syncall; exit 4; }
  fi
}
rotations() {   # tag: two rotations over SYSTEMS, run_p1.sh's schedule
  local tag=$1 size=$(size_of $1) r b k s order rev i n fam
  local -a SY=($TSYS); n=${#SY[@]}
  useroot p1
  for r in 1 2; do
    say "p1 $tag rotation $r"
    : > $RES/schedule-$tag-$r.txt; b=0
    for fam in $(fams_of $tag); do
      k=$(( b % n )); order=("${SY[@]:k}" "${SY[@]:0:k}")
      if [ $(( r % 2 )) = 0 ]; then rev=(); for (( i=${#order[@]}-1; i>=0; i-- )); do rev+=("${order[i]}"); done; order=("${rev[@]}"); fi
      for s in "${order[@]}"; do
        echo "block $size $tag $r $fam $s" >> $RES/schedule-$tag-$r.txt
        case $s in clickhouse|neo4j|memgraph) tblock $size $tag $r $fam $s 1 ;; *) tblock $size $tag $r $fam $s ;; esac
      done
      b=$((b + 1))
    done
  done
}
t32() {   # tag: DuckDB (SQL) at 32 threads, two rotations
  local tag=$1 size=$(size_of $1) r fam
  useroot t32p1
  for r in 1 2; do for fam in $(fams_of $tag); do EXTCMP_DUCKDBSQL_THREADS=32 tblock $size $tag $r $fam duckdbsql; done; done
}
epass() {   # tag: the engine-time pass
  local tag=$1 size=$(size_of $1) s fam
  useroot p1; say "p1 $tag engine pass"
  for fam in $(fams_of $tag); do
    for s in $ESYS; do
      if [ $s = postgres ]; then EXTCMP_P1_REPS=3 EXTCMP_PG_STATS=1 tblock $size $tag E $fam $s 1
      else EXTCMP_P1_REPS=3 tblock $size $tag E $fam $s 1; fi
    done
  done
}
main() {
  local tag s
  getlogs $W2_POINTS
  ( cd $P1DIR && sha256sum -c SHA256SUMS ) || { echo "W2 parameter files do not match SHA256SUMS"; return 2; }
  c3_drift_check || return 2
  proto_p1
  if [ -n "${W2_R10M_JOB:-}" ]; then [ -d $W2_R10M_ROOT/p1 ] || { echo "NO 1M TIMED RESULTS for R-10M: $W2_R10M_ROOT/p1"; return 2; }; fi
  for tag in $W2_POINTS; do
    point_env $tag
    say "point $tag: restore"
    for s in $TSYS; do sd=$(store_src $tag $s) || { echo "NO STORE $tag $s"; return 2; }; RESTORE_FROM=$sd restore $tag $s-$tag $(parent_of $s) || return 2; done
    seed_root p1 $tag || return 2; [ "$C3" = 1 ] || { seed_root t32p1 $tag || return 2; }
    mkdir -p $ROOT/results-p1/$tag; cp -a $STORE_DIR/$tag/load-*.json $ROOT/results-p1/$tag/
    if [ "$C3" = 1 ]; then for s in $C3_RESTORE; do [ -e $ROOT/results-p1/$tag/load-$s.json ] || cp -a $C3_STORE_SRC/$tag/load-$s.json $ROOT/results-p1/$tag/ 2>/dev/null; done; fi
    rotations $tag
    [ "$C3" = 1 ] || t32 $tag
    epass $tag
    useroot p1; $PYV $REPO/scripts/extcmp_p1.py report --dir $RES/$tag --rotations 2 --out $RES/$tag/p1-report > $RES/$tag/p1-report.md 2>&1 || echo "report failed p1 $tag"
    syncall
    "$KIT/lib/servers.sh" stopall
    for s in $SYSTEMS; do rm -rf $(parent_of $s)/$s-$tag; done      # keep ${LOCAL:-${TMPDIR:-/tmp}} to one point
  done
  if [ "$C3" = 1 ]; then   # this job's part of the drift sample (the stage figure is computed over every timed job of the stage)
    $PYV $C3T/c3_drift.py report --sample $C3_DRIFT_DIR/drift-W2.json --new $OUT --out $OUT/drift-report-job.md || echo "drift (this job): see $OUT/drift-report-job.md"
  fi
  syncall; say "timed body done"
}
main
