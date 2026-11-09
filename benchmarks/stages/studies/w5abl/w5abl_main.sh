#!/bin/bash
# W5 component study, job body. Stops: 2 a missing input, 7 the gate or a pair's answers, 8 failed windows or the postcheck.
source ${NEXT_STAGE_ROOT:?}/src/benchmarks/stages/studies/lib.sh
: "${OUT:?}" "${ROOT:?}" "${W5_TAG:?}"
W=$E/w5-wikidata; TAG=$W5_TAG; SIZE=1000000
sg=""; case $TAG in W5-wikidata-v2) PD=$W/params-c3; sg=--pinned ;; *) PD=$W/params ;; esac
BASE=${W5_BASE:-$W/runs/W5-wikidata-v2-J04/$TAG}; FAMS=${W5ABL_FAMS:-P,T,S,W,D,N,R,M,B}; ROTS=${W5ABL_ROTS:-2}
ARMS=${W5ABL_ARMS:-aa,range_visibility_off,adjacency_seed_off,kernels_off,window_bound_off}
need_dir $BASE; for v in reg bel; do need_file $PD/params-$TAG-$v.json; done; need_file $PD/SHA256SUMS-$TAG
$PYV $REPO/$STG/tools/w5_preflight.py --params-dir $PD --tag $TAG --base $BASE $sg || { echo "W5 PREFLIGHT FAILED"; exit 2; }
( cd $PD && sha256sum -c --quiet --ignore-missing SHA256SUMS-$TAG ) || { echo "W5 PARAMETER FILES DO NOT MATCH SHA256SUMS: stopping"; exit 2; }
setup_root $ROOT; sync_hook $OUT
cp -a $W/campaign/eventlogs/$TAG.eventlog.jsonl $W/eventlogs/$TAG.eventlog.jsonl.meta.json $LOGS/ 2>/dev/null
need_file $LOGS/$TAG.eventlog.jsonl
want=$(awk -v f="$TAG.eventlog.jsonl" '$2 == f {print $1}' $W/eventlogs/SHA256SUMS-$TAG); got=$(sha256sum $LOGS/$TAG.eventlog.jsonl | cut -d' ' -f1)
[ -n "$want" ] && [ "$want" = "$got" ] && echo "log $TAG sha256 $got OK" || { echo "FROZEN LOG CHECK FAILED $TAG: stopping"; exit 2; }
source $KIT/lib/env.sh
# the B family's sidecar, as the W5 stage
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
say "the Gnomon store built on C4 by this stage"
SD=$NEXT_STAGE_ROOT/stores/$TAG; cat $SD/provenance.txt; restore_tar $SD/native-$TAG.tar.zst $STORES
need_dir $STORES/native-$TAG; ln -sfn native-$TAG $STORES/native-$TAG-u
proto_p1
say "gate: Gnomon, every instance, against the recorded baselines"
mkdir -p $RES/$TAG
for v in reg bel; do
  ( export EXTCMP_PARAMS=$PD/params-$TAG-$v.json P1_VTAG=$v; [ $v = bel ] && export EXTCMP_VERIFY_FAMILY=B; "$KIT/lib/window.sh" verify $SIZE $TAG - - native ) || { echo "GATE RUN FAILED $v: stopping"; exit 8; }
done
for s in duckdbsql postgres neo4j; do for v in reg bel; do need_file $BASE/verify-$s-$v.json; cp -a $BASE/verify-$s-$v.json $RES/$TAG/; done; done
$PYV $REPO/$STG/tools/w5_gate.py --new $RES/$TAG --out $RES/$TAG/gate.md; rc=$?
cat $RES/$TAG/gate.md | head -5; eval "$EXTCMP_SYNC_HOOK"
[ $rc = 0 ] || { echo "W5 GATE FAILED: stopping before any timing"; exit 7; }
# one parameter file for abl_window.sh
P=$ROOT/params; mkdir -p $P
$PYV - $PD/params-$TAG-reg.json $PD/params-$TAG-bel.json $P/params-$TAG.json <<'PY'
import json, sys
a, b = json.load(open(sys.argv[1])), json.load(open(sys.argv[2]))
a["instances"] = a["instances"] + b["instances"]
a["version"] = str(a.get("version")) + "+bel"
json.dump(a, open(sys.argv[3], "w"), indent=1)
print(len(a["instances"]), "instances")
PY
mkdir -p $RES/params; cp -a $P/. $RES/params/
export ABL_LABEL=w5abl ABL_PARAMS_DIR=$P ABL_CONFIG_GATE="n/a (descriptive)" ABL_CONFIG_GATE_1=n/a ABL_CONFIG_GATE_2=n/a ABL_CONFIG_GATE_C4=n/a
A=$RES/w5abl; mkdir -p $A/$TAG
{ echo "{\"engine_so\": \"$EXTCMP_ENGINE_SO_SHA256\", \"kit\": \"$EXTCMP_KITHEAD\", \"arms\": \"$ARMS\", \"families\": \"$FAMS\", \"rotations\": $ROTS, \"config_gate\": \"$ABL_CONFIG_GATE\"}"; } > $A/provenance.json
say "timed: paired windows"
for r in $(seq 1 $ROTS); do
  say "rotation $r"
  f=$A/$TAG/schedule-rot$r.txt; python3 $KIT/lib/abl_schedule.py $r $FAMS $ARMS > $f
  while read -r name fam variant envspec; do
    "$KIT/lib/abl_window.sh" $SIZE $TAG $r $name $fam $variant $envspec; wrc=$?
    [ $wrc = 2 ] && { echo "STOPPED rot$r $fam $name: missing input $(utc)"; exit 2; }
    [ $wrc = 0 ] || { echo "WINDOW FAILED rot$r $fam $name rc=$wrc $(utc)"; echo "$TAG rot$r $fam $name rc=$wrc" >> $A/window-failed.txt; }
    python3 $KIT/lib/abl_pair_check.py $A/$TAG/rot$r/$fam $name; prc=$?
    [ $prc = 0 ] || { echo "ANSWER MISMATCH rot$r $fam $name: stopping $(utc)"; eval "$EXTCMP_SYNC_HOOK"; exit 7; }
  done < $f
done
"$KIT/lib/servers.sh" stopall
ln -sfn $TAG $A/K$TAG      # the kit's report tools read directories named K*
python3 $KIT/lib/abl_report.py $A > $A/report.md || echo "abl_report failed"
$PYV $TOOLS/abl_hub_report.py $A --out $A/hub-report || echo "hub report failed"
eval "$EXTCMP_SYNC_HOOK"
$PYV $TOOLS/abl_postcheck.py $A || { echo "POSTCHECK FAILED $A"; exit 8; }
say "W5 component study body done"
