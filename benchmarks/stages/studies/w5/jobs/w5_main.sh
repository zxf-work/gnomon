#!/bin/bash
# W5 job body. Stops: 2 a missing log, parameter file or incomplete load, 4 the stop rule, 7 the gate (a differing answer), 8 at the end.
source ${NEXT_STAGE_ROOT:?}/src/benchmarks/stages/studies/lib.sh
: "${OUT:?}" "${W5_TAG:?}"
W=$E/w5-wikidata; TAG=$W5_TAG; SIZE=1000000
# the text names params-c3 for W5-wikidata-v2; smallpedia, a smoke's input, has only params/
sg=""; case $TAG in W5-wikidata-v2) PD=$W/params-c3; sg=--pinned ;; *) PD=$W/params ;; esac
BASE=${W5_BASE:-$W/runs/W5-wikidata-v2-J04/$TAG}; FAMS=${W5_FAMS:-P T S W D N R M}; SYSTEMS=${W5_SYSTEMS:-native duckdbsql postgres neo4j}   # W5_SYSTEMS: a smoke's systems only
need_dir $BASE; for v in reg bel; do need_file $PD/params-$TAG-$v.json; done; need_file $PD/SHA256SUMS-$TAG
# before any load: the files, and every recorded baseline cell in the file Gnomon will read
$PYV $REPO/$STG/tools/w5_preflight.py --params-dir $PD --tag $TAG --base $BASE $sg || { echo "W5 PREFLIGHT FAILED: stopping before any load"; exit 2; }
( cd $PD && sha256sum -c --quiet --ignore-missing SHA256SUMS-$TAG ) || { echo "W5 PARAMETER FILES DO NOT MATCH SHA256SUMS: stopping"; exit 2; }
setup_root ${LOCAL:-${TMPDIR:-/tmp}}/w5
useroot() { CUR=$1; [ -d "$ROOT/results" ] && [ ! -L "$ROOT/results" ] && rmdir "$ROOT/results"; mkdir -p $ROOT/results-$CUR $OUT/$CUR; ln -sfn results-$CUR $ROOT/results
            export EXTCMP_SYNC_HOOK="mkdir -p $OUT/$CUR && cp -au $ROOT/results-$CUR/. $OUT/$CUR/"; RES=$ROOT/results-$CUR; }
useroot p1
cp -a $W/campaign/eventlogs/$TAG.eventlog.jsonl $W/eventlogs/$TAG.eventlog.jsonl.meta.json $LOGS/ 2>/dev/null
need_file $LOGS/$TAG.eventlog.jsonl
want=$(awk -v f="$TAG.eventlog.jsonl" '$2 == f {print $1}' $W/eventlogs/SHA256SUMS-$TAG); got=$(sha256sum $LOGS/$TAG.eventlog.jsonl | cut -d' ' -f1)
[ -n "$want" ] && [ "$want" = "$got" ] && echo "log $TAG sha256 $got OK" || { echo "FROZEN LOG CHECK FAILED $TAG ($got vs $want): stopping"; exit 2; }
source $KIT/lib/env.sh
win() { "$KIT/lib/window.sh" "$@" || echo "WINDOW FAILED $* $(utc)"; }
# the B family's sidecar: placeholders, every instance carries its own arguments
$PYV - "$LOGS/$TAG.eventlog.jsonl" "$PD/params-$TAG-bel.json" <<'PY' || { echo "SIDECAR FAILED: stopping"; exit 2; }
import json, os, sys
log, pf = sys.argv[1], sys.argv[2]; st = os.stat(log)
ins = {i["query"]: i["args"] for i in json.load(open(pf))["instances"]}
sn, rc, df, h = ins["b.snap.hop2@T"], ins["b.reach@T"], ins["b.diff"], ins["b.hist"]
d = {"size": st.st_size, "mtime_ns": st.st_mtime_ns, "snap_seeds": sn["seeds"], "t_star": sn["t_valid"],
     "seed_node": rc["src"], "reach_window": rc["window"], "T": sn["as_of_tt"], "T_base": df["t1"],
     "diff_window": df["window"], "e_star": h, "w5_placeholder": True}
open(log[:-len(".jsonl")] + ".beliefq-params.json", "w").write(json.dumps(d))
PY
load_ok() { $PYV -c "import json,sys; d=json.load(open(sys.argv[1])); sys.exit(0 if 'load' in d and not d.get('load exceeded budget') else 1)" "$1" 2>/dev/null; }
say "loads, reported per phase (the reported row)"
export EXTCMP_SERVER_TIMEOUT_S=3660 EXTCMP_CALL_CEILING_S=3600 EXTCMP_VERIFY_CAP_S=3662
for s in $SYSTEMS; do
  win load $SIZE $TAG - - $s
  load_ok $RES/$TAG/load-$s.json || { echo "LOAD NOT COMPLETE $s: stopping"; exit 2; }
done
need_dir $STORES/native-$TAG
proto_p1
say "gate: Gnomon on C4, every instance, against the recorded baselines on the same log and files"
for v in reg bel; do
  ( export EXTCMP_PARAMS=$PD/params-$TAG-$v.json P1_VTAG=$v; [ $v = bel ] && export EXTCMP_VERIFY_FAMILY=B; "$KIT/lib/window.sh" verify $SIZE $TAG - - native ) || { echo "GATE RUN FAILED $v: stopping"; exit 8; }
done
for s in duckdbsql postgres neo4j; do for v in reg bel; do need_file $BASE/verify-$s-$v.json; cp -a $BASE/verify-$s-$v.json $RES/$TAG/; done; done
$PYV $REPO/$STG/tools/w5_gate.py --new $RES/$TAG --out $RES/$TAG/gate.md; rc=$?
eval "$EXTCMP_SYNC_HOOK"
[ $rc = 0 ] || { echo "W5 GATE FAILED: stopping"; exit 7; }
tblock() {   # rot fam sys
  local r=$1 fam=$2 s=$3 v=reg; [ $fam = B ] && v=bel
  EXTCMP_PARAMS=$PD/params-$TAG-$v.json win block $SIZE $TAG $r $fam $s
  if [ $s = native ]; then
    $PYV $REPO/benchmarks/stages/w2/tools/w2_stopcheck.py $RES/$TAG/rot$r/$fam-native.json $RES/$TAG || { echo "STOP RULE: Gnomon window $TAG rot$r $fam"; eval "$EXTCMP_SYNC_HOOK"; exit 4; }
  fi
}
rotations() {   # the kit's schedule: order rotated per family, flipped in rotation 2
  local -a SY=($1); local n=${#SY[@]} r b k i fam s order rev
  for r in 1 2; do b=0
    for fam in $FAMS B; do
      k=$(( b % n )); order=("${SY[@]:k}" "${SY[@]:0:k}")
      if [ $r = 2 ]; then rev=(); for (( i=${#order[@]}-1; i>=0; i-- )); do rev+=("${order[i]}"); done; order=("${rev[@]}"); fi
      for s in "${order[@]}"; do tblock $r $fam $s; done
      b=$((b + 1))
    done
  done
}
say "timed"; rotations "$SYSTEMS"
say "DuckDB (SQL) at 32 threads"; useroot t32p1; mkdir -p $RES/$TAG; cp -a $ROOT/results-p1/$TAG/verify-*.json $ROOT/results-p1/$TAG/load-*.json $RES/$TAG/
export EXTCMP_DUCKDBSQL_THREADS=32; rotations "duckdbsql"; export EXTCMP_DUCKDBSQL_THREADS=64
useroot p1
say "engine-time pass"
for s in native duckdbsql postgres; do
  for fam in $FAMS B; do
    v=reg; [ $fam = B ] && v=bel
    if [ $s = postgres ]; then EXTCMP_PG_STATS=1 EXTCMP_P1_REPS=3 EXTCMP_ENGINE_TIME=1 EXTCMP_PARAMS=$PD/params-$TAG-$v.json win block $SIZE $TAG E $fam $s
    else EXTCMP_P1_REPS=3 EXTCMP_ENGINE_TIME=1 EXTCMP_PARAMS=$PD/params-$TAG-$v.json win block $SIZE $TAG E $fam $s; fi
  done
done
if [ "${W5_MATCH:-1}" = 1 ]; then
  say "match patterns (W5-3, Gnomon only)"
  need_file $PD/match-$TAG.json
  EXTCMP_ENGINE_TIME=1 ${EXTCMP_NUMA_CLIENT:+$EXTCMP_NUMA_CLIENT }$PYV $REPO/scripts/kf_pilot.py w5match --system gnomon --store $STORES/native-$TAG \
      --params $PD/match-$TAG.json --n-instances 100 --out $RES/$TAG/w5match-gnomon.jsonl --label "C4 W5 $TAG match" || echo "WINDOW FAILED match"
fi
"$KIT/lib/servers.sh" stopall
$PYV $REPO/$STG/tools/w5_gate.py --new $RES/$TAG --timed --out $RES/$TAG/timed-check.md; rc=$?
eval "$EXTCMP_SYNC_HOOK"
[ $rc = 0 ] || { echo "W5 TIMED ANSWERS DIFFER: see timed-check.md"; exit 8; }
say "W5 body done"
