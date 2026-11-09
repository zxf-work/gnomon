#!/bin/bash
# Thread-matched control, answer-check job body (run by thr_gate.sbatch in its own process group).
source ${THR_STAGE_ROOT:-${GNOMON_BENCH_ROOT}/final/thr}/src/benchmarks/stages/threads/jobs/thr_lib.sh
: "${OUTB:?}" "${THR_WLS:?}"
verify_cfg() {   # tag cfg: the answer check of every instance of the workload's parameter file on one configuration
  local tag=$1 c=$2 size fam s
  size=$(size_of $tag); set_cfg $c; s=$CSYS; useroot cfg-$c; mkdir -p $RES/$tag; cp $PD/params-$tag.json $RES/$tag/params-used.json
  say "verify $WL $tag $c ($s; GNOMON_MATCH_THREADS=$GNOMON_MATCH_THREADS EXTCMP_DUCKDBSQL_THREADS=$EXTCMP_DUCKDBSQL_THREADS EXTCMP_THR_PG_WORKERS=${EXTCMP_THR_PG_WORKERS:-unset})"
  [ $s = postgres ] && export EXTCMP_THR_PG_PLANS=1
  if [ "${tag#*-d}" != "$tag" ]; then      # a belief-time point: one family B, one window, record verify-<system>.json
    EXTCMP_PARAMS=$PD/params-$tag.json EXTCMP_WINDOW_TIMEOUT_S=$VWIN win verify $size $tag - - $s
    [ $s = postgres ] && cp $SRV/postgres-$tag/server.log $RES/$tag/pgplan-$c-B.log 2>/dev/null
  else                                      # the registry: one window per family, record verify-<system>-<family>.json
    for fam in $(fams_of $tag); do
      ( export EXTCMP_VERIFY_FAMILY=$fam P1_VTAG=$fam; EXTCMP_PARAMS=$PD/params-$tag.json EXTCMP_WINDOW_TIMEOUT_S=$VWIN win verify $size $tag - - $s )
      [ $s = postgres ] && cp $SRV/postgres-$tag/server.log $RES/$tag/pgplan-$c-$fam.log 2>/dev/null
    done
  fi
  unset EXTCMP_THR_PG_PLANS
  $PYV $TOOLS/thr_partial.py $RES/$tag $PD/params-$tag.json
  $PYV $TOOLS/thr_check.py applied-dir $RES/$tag $c || echo "OVERRIDE CHECK FAILED $WL $tag $c (judged by thr_gate.py)"
  rm -rf $RES/$tag/answers-*
  syncall
}
trap 'echo "=== TERM: syncing records $(utc)"; [ -n "${ROOT:-}" ] && syncall; exit 5' TERM
main() {
  local wl tag s c grc=0 rc
  for wl in $THR_WLS; do
    wl_set $wl; setup_root ${LOCAL:-${TMPDIR:-/tmp}}/thr-g-$wl; OUT=$OUTB/$wl; mkdir -p $OUT
    getlogs $WTAGS
    ( cd $PD && sha256sum -c SHA256SUMS ) || { echo "parameter files of $wl do not match SHA256SUMS"; return 2; }
    proto_p1
    for tag in $WTAGS; do
      point_env $tag
      say "point $wl $tag: restore"
      for s in $SYSTEMS; do restore $tag $s-$tag $(parent_of $s) || return 2; done
      pg_conf $tag || return 2
      for c in $CFGS; do verify_cfg $tag $c; done
      "$KIT/lib/servers.sh" stopall
      for s in $SYSTEMS; do rm -rf $(parent_of $s)/$s-$tag; done
    done
    syncall
    say "answer gate $wl"
    local -a refs=(); for tag in $WTAGS; do refs+=(--ref "$tag=$REFD/$tag"); done
    $PYV $TOOLS/thr_gate.py --res $OUT --params $PD --tags "$(echo $WTAGS | tr ' ' ',')" --cfgs "$CFGS" \
        "${refs[@]}" --md $THR/gate/${THR_SMOKE:+smoke-}gate-$SLURM_JOB_ID-$wl.md
    rc=$?; echo "gate $wl rc=$rc" | tee $THR/gate/${THR_SMOKE:+smoke-}gate-$SLURM_JOB_ID-$wl.rc
    [ $rc = 0 ] || grc=$rc
    $PYV $TOOLS/thr_pgplans.py --res $OUT --wl $wl > $OUT/pgplans.md 2>&1 || echo "pgplans summary failed $wl"
  done
  say "gate main done rc=$grc"
  return $grc
}
main
