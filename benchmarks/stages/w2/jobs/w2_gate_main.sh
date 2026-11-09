#!/bin/bash
# W2 gate job body (run by w2_gate.sbatch in its own process group, so the time-limit trap can stop it whole).
source ${W2_STAGE_ROOT:-${GNOMON_BENCH_ROOT}/final/w2}/src/benchmarks/stages/w2/jobs/w2_lib.sh
: "${OUT:?}" "${ROOT:?}" "${W2_POINTS:?}"
setup_root $ROOT
main() {
  if [ "$C3" = 1 ]; then : "${C3_REF_GATE:?C3 mode: C3_REF_GATE (the recorded gate result dirs) must be set}"; fi
  local tag size s
  getlogs $W2_POINTS
  ( cd $P1DIR && sha256sum -c SHA256SUMS ) || { echo "W2 parameter files do not match SHA256SUMS"; return 2; }
  for tag in $W2_POINTS; do
    size=$(size_of $tag); point_env $tag
    say "point $tag: loads"
    useroot p1; proto_v3
    for s in $GSYS; do
      sd=$(store_src $tag $s) || sd=""
      if [ -n "$sd" ] && [ -e $sd/$tag/load-$s.json ] && [ ! -e $RES/$tag/load-$s.json ]; then
        # a store persisted earlier: restored, its load record kept
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
    say "point $tag: p1 answer check (every instance, every system; ceiling 10 s, verify windows cut at $VWIN s)"
    proto_p1; useroot p1
    P1_SIZE=$size P1_POINTS=$tag P1_SYSTEMS="$GSYS" P1_PARAMS_DIR=$P1DIR EXTCMP_WINDOW_TIMEOUT_S=$VWIN "$KIT/run_p1.sh" --steps verify
    echo "run_p1 verify rc=$? (3: the kit's gate found a Gnomon cell not ok: judged by tools/w2_gate.py)"
    $PYV $TOOLS/w2_partial.py $RES/$tag $P1DIR/params-$tag.json
    $PYV $REPO/scripts/extcmp_p1.py gate --dir $RES/$tag --ceiling 10 --out $RES/$tag/excluded.json | tee $RES/$tag/verdicts.txt
    if [ "$C3" != 1 ]; then
    say "point $tag: t32p1 (DuckDB (SQL) at 32 threads, own Gnomon reference)"
    useroot t32p1
    EXTCMP_DUCKDBSQL_THREADS=32 P1_SIZE=$size P1_POINTS=$tag P1_SYSTEMS="native duckdbsql" P1_PARAMS_DIR=$P1DIR EXTCMP_WINDOW_TIMEOUT_S=$VWIN "$KIT/run_p1.sh" --steps verify
    echo "run_p1 t32 verify rc=$?"
    $PYV $TOOLS/w2_partial.py $RES/$tag $P1DIR/params-$tag.json
    $PYV $REPO/scripts/extcmp_p1.py gate --dir $RES/$tag --ceiling 10 --out $RES/$tag/excluded.json | tee $RES/$tag/verdicts.txt
    fi
    syncall
    "$KIT/lib/servers.sh" stopall
    for s in $SYSTEMS; do rm -rf $(parent_of $s)/$s-$tag; done      # keep ${LOCAL:-${TMPDIR:-/tmp}} to one point
  done
  "$KIT/lib/servers.sh" stopall; syncall
  say "answer gate"
  $PYV $TOOLS/w2_gate.py --res $OUT --tags "$(echo $W2_POINTS | tr ' ' ',')" ${C3_REF_GATE:+--ref-gate $C3_REF_GATE} --out $W2/gate/${W2_SMOKE:+smoke-}gate-$SLURM_JOB_ID.md
  local grc=$?
  echo "gate rc=$grc" | tee $W2/gate/${W2_SMOKE:+smoke-}gate-$SLURM_JOB_ID.rc
  return $grc
}
main
