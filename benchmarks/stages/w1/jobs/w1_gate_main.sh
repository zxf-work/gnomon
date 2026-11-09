#!/bin/bash
# W1 gate job body (run by w1_gate.sbatch in its own process group, so the time-limit trap can stop it whole).
source ${W1_STAGE_ROOT:-${GNOMON_BENCH_ROOT}/final/w1}/src/benchmarks/stages/w1/jobs/w1_lib.sh
: "${OUT:?}" "${ROOT:?}" "${W1_POINTS:?}"
setup_root $ROOT
main() {
  if [ "$C3" = 1 ]; then : "${C3_REF_GATE:?C3 mode: C3_REF_GATE (the recorded gate result dirs) must be set}"; fi
  getlogs $W1_POINTS
  # the single-parameter parameter files (written before submission by tools/w1_single_params.py), checked here
  ( cd $SPDIR && sha256sum -c SHA256SUMS ) || { echo "single-parameter files do not match SHA256SUMS"; return 2; }
  ( cd $P1DIR && sha256sum -c SHA256SUMS ) || { echo "P1 parameter files do not match SHA256SUMS"; return 2; }
  for tag in $W1_POINTS; do
    size=$(size_of $tag); point_env $tag
    say "point $tag: loads"
    useroot sp; proto_v3
    for s in $GSYS; do
      sd=$(store_src $tag $s) || sd=""
      if [ -n "$sd" ] && [ -e $sd/$tag/load-$s.json ] && [ ! -e $RES/$tag/load-$s.json ]; then
        # a store persisted earlier (B0's duckdb K10M-d10, or a resubmitted gate job): restored, its load record kept
        RESTORE_FROM=$sd restore $tag $s-$tag $(parent_of $s) || return 2
        mkdir -p $RES/$tag; cp -a $sd/$tag/load-$s.json $RES/$tag/load-$s.json; continue
      fi
      if [ $s = duckdb ] && [ $tag = K10M-d10 ] && [ ! -e $RES/$tag/load-duckdb.json ]; then
        echo "NO B0 STORE for duckdb K10M-d10: refusing to load it here"; return 2
      fi
      "$KIT/lib/window.sh" load $size $tag - - $s || echo "WINDOW FAILED load $tag $s"
      if load_ok $RES/$tag/load-$s.json; then
        save $(parent_of $s) $s-$tag $tag && cp -a $RES/$tag/load-$s.json $STORE_DIR/$tag/ || { echo "STORE SAVE FAILED $tag $s"; return 2; }
      else
        echo "LOAD NOT COMPLETE $tag $s: not persisted"
      fi
    done
    say "point $tag: v3gate (v3's answer check)"
    useroot v3gate; proto_v3
    for s in $GSYS; do "$KIT/lib/window.sh" verify $size $tag - - $s || echo "WINDOW FAILED verify $tag $s"; done
    if [ "$C3" != 1 ]; then
    useroot v3gate-t32
    EXTCMP_DUCKDBSQL_THREADS=32 "$KIT/lib/window.sh" verify $size $tag - - duckdbsql || echo "WINDOW FAILED verify t32 $tag"
    fi
    say "point $tag: sp (single-parameter instances, timed protocol caps)"
    proto_p1
    useroot sp
    for s in $GSYS; do EXTCMP_PARAMS=$SPDIR/params-$tag.json win verify $size $tag - - $s; done
    $PYV $REPO/scripts/extcmp_p1.py gate --dir $RES/$tag --ceiling 10 --out $RES/$tag/excluded.json | tee $RES/$tag/verdicts.txt
    if [ "$C3" != 1 ]; then
    useroot t32sp
    for s in native duckdbsql; do EXTCMP_DUCKDBSQL_THREADS=32 EXTCMP_PARAMS=$SPDIR/params-$tag.json win verify $size $tag - - $s; done
    $PYV $REPO/scripts/extcmp_p1.py gate --dir $RES/$tag --ceiling 10 --out $RES/$tag/excluded.json | tee $RES/$tag/verdicts.txt
    fi
    if [ -e $P1DIR/params-$tag.json ]; then
      say "point $tag: p1 (curated instances)"
      useroot p1
      P1_SIZE=$size P1_POINTS=$tag P1_SYSTEMS="$GSYS" P1_PARAMS_DIR=$P1DIR "$KIT/run_p1.sh" --steps verify; echo "run_p1 verify rc=$?"
      if [ "$C3" != 1 ]; then
      useroot t32p1
      EXTCMP_DUCKDBSQL_THREADS=32 P1_SIZE=$size P1_POINTS=$tag P1_SYSTEMS="native duckdbsql" P1_PARAMS_DIR=$P1DIR "$KIT/run_p1.sh" --steps verify; echo "run_p1 t32 verify rc=$?"
      fi
    fi
    syncall
  done
  "$KIT/lib/servers.sh" stopall; syncall
  say "answer gate"
  $PYV $TOOLS/w1_gate.py --res $OUT --tags "$(echo $W1_POINTS | tr ' ' ',')" ${C3_REF_GATE:+--ref-gate $C3_REF_GATE} --out $W1/gate/${W1_SMOKE:+smoke-}gate-$SLURM_JOB_ID.md
  echo "gate rc=$?" | tee $W1/gate/${W1_SMOKE:+smoke-}gate-$SLURM_JOB_ID.rc
}
main
