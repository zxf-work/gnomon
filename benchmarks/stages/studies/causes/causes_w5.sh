#!/bin/bash
# C4 causes, W5's six lost queries: Gnomon, DuckDB (SQL), PostgreSQL, Neo4j on the C4 store, on this node.
source ${NEXT_STAGE_ROOT:?}/src/benchmarks/stages/studies/lib.sh
: "${OUT:?}" "${ROOT:?}"; TAG=${C_W5_TAG:-W5-wikidata-v2}; W=$E/w5-wikidata; SIZE=1000000
case $TAG in W5-wikidata-v2) PD=$W/params-c3 ;; *) PD=$W/params ;; esac
ST=$REPO/$STG/causes; SD=$E/studies/w5abl/stores/$TAG
setup_root $ROOT; sync_hook $OUT; export EXTCMP_DUCKDBSQL_THREADS=${C_DUCK_THREADS:-32}
source $KIT/lib/env.sh
cp -a $W/campaign/eventlogs/$TAG.eventlog.jsonl $LOGS/; need_file $LOGS/$TAG.eventlog.jsonl
$PYV - "$LOGS/$TAG.eventlog.jsonl" "$PD/params-$TAG-bel.json" <<'PY' || { echo "SIDECAR FAILED"; exit 2; }
import json, os, sys
log, pf = sys.argv[1], sys.argv[2]; st = os.stat(log)
ins = {i["query"]: i["args"] for i in json.load(open(pf))["instances"]}
sn, rc, df, h = ins["b.snap.hop2@T"], ins["b.reach@T"], ins["b.diff"], ins["b.hist"]
d = {"size": st.st_size, "mtime_ns": st.st_mtime_ns, "snap_seeds": sn["seeds"], "t_star": sn["t_valid"], "seed_node": rc["src"], "reach_window": rc["window"], "T": sn["as_of_tt"],
     "T_base": df["t1"], "diff_window": df["window"], "e_star": h, "w5_placeholder": True}
open(log[:-len(".jsonl")] + ".beliefq-params.json", "w").write(json.dumps(d))
PY
cat $SD/provenance.txt; restore_tar $SD/native-$TAG.tar.zst $STORES; need_dir $STORES/native-$TAG
export EXTCMP_SERVER_TIMEOUT_S=3660 EXTCMP_CALL_CEILING_S=3600 EXTCMP_VERIFY_CAP_S=3662 EXTCMP_PG_STATS=1
for s in ${C_SYSTEMS:-duckdbsql postgres neo4j}; do "$KIT/lib/window.sh" load $SIZE $TAG - - $s || echo "LOAD FAILED $s"; done
proto_p1
P="--log $LOGS/$TAG.eventlog.jsonl --ref $STORES/native-$TAG --workdir $ROOT --size-tag $TAG --params $PD/params-$TAG-reg.json --params2 $PD/params-$TAG-bel.json --cases ${C_CASES:-$ST/cases-w5.json} --out $RES/probe"
export EXTCMP_ENGINE_TIME=1
for s in ${C_SYSTEMS:-duckdbsql postgres neo4j} native; do
  case $s in postgres|neo4j) "$KIT/lib/servers.sh" start $s $TAG | tail -1; $PY wait --system $s --timeout 900 || { echo "SERVER $s DID NOT START"; continue; } ;; esac
  say "probe $s"
  if [ $s = native ]; then GNOMON_MATCH_TRACE=1 $PYV $ST/causes_probe.py w5 --system $s $P || echo "PROBE FAILED $s"
  else $PYV $ST/causes_probe.py w5 --system $s $P || echo "PROBE FAILED $s"; fi
  case $s in postgres|neo4j) "$KIT/lib/servers.sh" stop $s $TAG | tail -1 ;; esac
  eval "$EXTCMP_SYNC_HOOK"
done
say "W5 causes done"
