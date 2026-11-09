#!/bin/bash
# Gate job body (run by x_gate.sbatch in its own process group).
source ${NEXT_STAGE_ROOT:?}/src/benchmarks/stages/studies/t10/jobs/x_lib.sh
: "${OUT:?}" "${ROOT:?}"
x_root $ROOT
load_ok() { $PYV -c "import json,sys; d=json.load(open(sys.argv[1])); sys.exit(0 if 'load' in d and not d.get('load exceeded budget') else 1)" "$1" 2>/dev/null; }
save() {   # parent name tag: tar.zst of a store or server data directory under the stage, with its sha256
  local o=$NEXT_STAGE_ROOT/${SM}stores/$3; mkdir -p $o
  tar -C "$1" -cf - "$2" | zstd -1 -T32 -q -o "$o/$2.tar.zst.tmp" && mv "$o/$2.tar.zst.tmp" "$o/$2.tar.zst" && \
    ( cd $o && sha256sum $2.tar.zst > $2.tar.zst.sha256 ) && echo "saved $o/$2.tar.zst $(du -sh $o/$2.tar.zst | cut -f1) $(utc)"; }
verify_all() {   # root tag systems...: one verify record per (system, family), then the unfinished ones recorded, then the kit gate
  local r=$1 tag=$2 s fam; shift 2
  useroot $r; mkdir -p $RES/$tag; cp $PARDIR/params-$tag.json $RES/$tag/params-used.json
  if [ -n "${X_RESUME:-}" ] && [ -d $X_RESUME/$r/$tag ]; then     # a gate job stopped at its limit: its finished windows stand; a window it cut or stopped is run again
    cp -au $X_RESUME/$r/$tag/. $RES/$tag/
    for f in $RES/$tag/verify-*.json; do [ -e "$f" ] && grep -q -E '"partial": true|"server_death"' "$f" && { echo "resume: not kept: ${f##*/}"; rm -f "$f" "$f.tmp.cells.jsonl"; }; done
    echo "resume: seeded $r/$tag from $X_RESUME: $(ls $RES/$tag | grep -c '^verify-.*\.json$') records"
  fi
  for s in "$@"; do
    for fam in $FAMS; do ( export EXTCMP_VERIFY_FAMILY=$fam P1_VTAG=$fam EXTCMP_PARAMS=$PARDIR/params-$tag.json EXTCMP_WINDOW_TIMEOUT_S=$VWIN; win verify $SIZE $tag - - $s ); done
  done
  $PYV $XT/x_partial.py $RES/$tag $PARDIR/params-$tag.json
  $PYV $REPO/scripts/extcmp_p1.py gate --dir $RES/$tag --ceiling 10 --out $RES/$tag/excluded.json | tee $RES/$tag/verdicts.txt
}
main() {
  local tag s
  getlogs
  for tag in $POINTS; do
    export EXTCMP_BELIEFQ_INDEXES=1      # every point here is a density point; the kit reads it in the loads only (the belief-axis indexes)
    useroot reg
    if [ "${X_LOAD:-0}" = 1 ]; then
      say "point $tag: loads on C4"; proto_v3
      for s in $SYSTEMS; do
        "$KIT/lib/window.sh" load $SIZE $tag - - $s || echo "WINDOW FAILED load $tag $s"
        if load_ok $RES/$tag/load-$s.json; then
          save $(parent_of $s) $s-$tag $tag && cp -a $RES/$tag/load-$s.json $NEXT_STAGE_ROOT/${SM}stores/$tag/ || { echo "STORE SAVE FAILED $tag $s"; return 2; }
        else
          echo "LOAD NOT COMPLETE $tag $s: not persisted"
          [ $s = native ] && { echo "no Gnomon store at $tag: stopping"; syncall; return 2; }
        fi
        syncall
      done
    else
      say "point $tag: restore"; restore_all $tag
    fi
    proto_p1
    say "point $tag: answer check, 64 threads"
    verify_all reg $tag $SYSTEMS; syncall
    say "point $tag: answer check, DuckDB (SQL) 32 threads, own Gnomon reference"
    if [ "$X_T32" = 1 ]; then ( export EXTCMP_DUCKDBSQL_THREADS=32; verify_all t32reg $tag native duckdbsql ); useroot t32reg; syncall; fi
    "$KIT/lib/servers.sh" stopall
    [ "${X_LOAD:-0}" = 1 ] || for s in $SYSTEMS; do rm -rf $(parent_of $s)/$s-$tag; done      # keep ${LOCAL:-${TMPDIR:-/tmp}} to one point
  done
  if [ -n "${X_ACCEPT:-}" ]; then     # smoke: the whole parameter files through the kit's own input checks on C4 stores, without running them
    say "acceptance of the parameter files"; $PYV $XT/x_accept.py --work $ROOT/accept --stage $NEXT_STAGE_ROOT | tee $OUT/accept.txt
    echo "accept rc=${PIPESTATUS[0]}"
  fi
  syncall
  say "answer gate"
  local g=$NEXT_STAGE_ROOT/${SM}gate/gate-$X_SET-$SLURM_JOB_ID
  $PYV $XT/x_gate.py --res $OUT --set $X_SET --tags "$(echo $POINTS | tr ' ' ',')" --fams "$(echo $FAMS | tr ' ' ',')" --native-from $NATIVE_FROM --others-from $OTHERS_FROM --out $g.md
  local grc=$?
  echo "gate rc=$grc" | tee $g.rc
  return $grc
}
main
