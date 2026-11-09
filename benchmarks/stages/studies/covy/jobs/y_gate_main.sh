#!/bin/bash
# Gate job body (run by y_gate.sbatch in its own process group).
source ${NEXT_STAGE_ROOT:?}/src/benchmarks/stages/studies/covy/jobs/y_lib.sh
: "${OUT:?}" "${ROOT:?}"
x_root $ROOT
verify_all() {   # tag: one verify record per (system, family), then the unfinished ones recorded, then the kit gate
  local tag=$1 s fam
  useroot reg; mkdir -p $RES/$tag; cp $PARDIR/params-$tag.json $RES/$tag/params-used.json
  if [ -n "${X_RESUME:-}" ] && [ -d $X_RESUME/reg/$tag ]; then     # t10's resume: a gate job stopped at its limit; its finished windows stand, a window it cut or stopped runs again
    cp -au $X_RESUME/reg/$tag/. $RES/$tag/
    for f in $RES/$tag/verify-*.json; do [ -e "$f" ] && grep -q -E '"partial": true|"server_death"' "$f" && { echo "resume: not kept: ${f##*/}"; rm -f "$f" "$f.tmp.cells.jsonl"; }; done
    echo "resume: seeded reg/$tag from $X_RESUME: $(ls $RES/$tag | grep -c '^verify-.*\.json$') records"
  fi
  for s in $SYSTEMS; do
    for fam in $FAMS; do
      [ -n "${X_RESUME:-}" ] && [ -s $RES/$tag/verify-$s-$fam.json ] && { echo "resume: kept verify-$s-$fam.json"; continue; }
      ( export EXTCMP_VERIFY_FAMILY=$fam P1_VTAG=$fam EXTCMP_PARAMS=$PARDIR/params-$tag.json EXTCMP_WINDOW_TIMEOUT_S=$VWIN; win verify $SIZE $tag - - $s ); done
  done
  $PYV $TT/x_partial.py $RES/$tag $PARDIR/params-$tag.json
  $PYV $REPO/scripts/extcmp_p1.py gate --dir $RES/$tag --ceiling 10 --out $RES/$tag/excluded.json | tee $RES/$tag/verdicts.txt
}
main() {
  local tag s
  getlogs
  for tag in $POINTS; do
    export EXTCMP_BELIEFQ_INDEXES=1
    useroot reg
    say "point $tag: restore"; restore_all $tag
    proto_p1
    say "point $tag: answer check"
    verify_all $tag; syncall
    "$KIT/lib/servers.sh" stopall
    for s in $SYSTEMS; do rm -rf $(parent_of $s)/$s-$tag; done
  done
  syncall
  say "answer gate"
  local g=$NEXT_STAGE_ROOT/${SM}gate/gate-$SLURM_JOB_ID
  $PYV $XT/y_gate.py --res $OUT --tags "$(echo $POINTS | tr ' ' ',')" --fams "$(echo $FAMS | tr ' ' ',')" --native-from $NATIVE_FROM --others-from $OTHERS_FROM --out $g.md
  local grc=$?
  echo "gate rc=$grc" | tee $g.rc
  return $grc
}
main
