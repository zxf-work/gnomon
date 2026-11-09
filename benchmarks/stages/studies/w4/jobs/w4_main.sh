#!/bin/bash
# W4 job body. Stops: 2 a missing or empty store or parameter file, 4 the stop rule, 7 the gate (a differing answer), 8 at the end, a
# timed answer that differs between systems or a failed block.
source ${NEXT_STAGE_ROOT:?}/src/benchmarks/stages/studies/lib.sh
: "${OUT:?}" "${W4_POINT:?}"
W=$E/w4-finbench; PT=$W4_POINT; SF=${PT%%-*}; RH=${PT##*-}; S=${W4_STORE_ROOT:-$W/w4stores}/$PT
CHK=${W4_CHECK_DIR:-$W/check-$SF/$RH}; PAR=$W/params/params-$SF.json
SYSTEMS=${W4_SYSTEMS:-gnomon duckdbsql64 duckdbsql32 postgres neo4j memgraph}; STEPS=${W4_STEPS:-gate,timed}
has() { case ",$STEPS," in *",$1,"*) return 0 ;; esac; return 1; }
need_dir $S; need_file $PAR; need_file $PAR.sha256; need_dir $CHK
( cd $(dirname $PAR) && sha256sum -c --quiet $(basename $PAR).sha256 ) || { echo "W4 PARAMETER FILE DOES NOT MATCH ITS sha256: stopping"; exit 2; }
setup_root ${LOCAL:-${TMPDIR:-/tmp}}/w4; export TMPDIR=$ROOT/scratch TR_CEILING_S=10 TR_GRACE_S=5 EXTCMP_MG_MEMORY_LIMIT_MB=118000
source $KIT/lib/env.sh
export EXTCMP_SERVER_TIMEOUT_S=10 EXTCMP_START_WAIT_S=7200
RESL=${LOCAL:-${TMPDIR:-/tmp}}/w4res; mkdir -p $RESL; cp $PAR $RESL/params-used.json
TR="${EXTCMP_NUMA_CLIENT:+$EXTCMP_NUMA_CLIENT }$PYV $REPO/scripts/timed_runner.py"
CK="${EXTCMP_NUMA_CLIENT:+$EXTCMP_NUMA_CLIENT }$PYV $REPO/scripts/w4_check.py"; TL=$REPO/$STG/tools/w4_gate.py
base_of() { case $1 in duckdbsql*) echo duckdbsql ;; *) echo $1 ;; esac; }
waitpg() { local i; for i in $(seq 1 300); do $PYV -c "import psycopg,os; psycopg.connect(os.environ['EXTCMP_PG_DSN']).close()" 2>/dev/null && { echo "postgres ready after ${i}x2 s"; return 0; }; sleep 2; done; return 1; }
waitbolt() { local i; for i in $(seq 1 ${3:-2400}); do $PYV -c "
import neo4j,sys
a={'neo4j':('neo4j','neo4j'),'none':None}[sys.argv[2]]
d=neo4j.GraphDatabase.driver(sys.argv[1], auth=a); d.verify_connectivity(); d.close()" "$1" "$2" 2>/dev/null && { echo "bolt $1 ready after ${i}x2 s"; return 0; }; sleep 2; done; return 1; }
restore_gnomon() {   # the persisted store, the sha256 beside it; the gate checks its answers on C4
  local g; g=$(ls $S/gnomon-*.tar.zst 2>/dev/null | head -1); [ -n "$g" ] || { echo "NO GNOMON STORE in $S: stopping"; exit 2; }
  [ -d $STORES/gnomon-$PT ] || restore_tar $g $STORES
  need_dir $STORES/gnomon-$PT/native; GSTORE=$STORES/gnomon-$PT; }
restore_duck() {
  need_file $S/duckdbsql.duckdb; need_file $S/duckdbsql.duckdb.sha256
  if [ ! -s ${LOCAL:-${TMPDIR:-/tmp}}/w4.duckdb ]; then ( cd $S && sha256sum -c --quiet duckdbsql.duckdb.sha256 ) || { echo "RESTORE CHECK FAILED duckdb: stopping"; exit 2; }; cp $S/duckdbsql.duckdb ${LOCAL:-${TMPDIR:-/tmp}}/w4.duckdb; fi; }
srv_up() {   # postgres | neo4j | memgraph: restore once, start, wait
  local s=$1
  [ -d $SRV/$s-w4 ] || restore_tar $S/$s.tar.zst $SRV
  need_dir $SRV/$s-w4
  $KIT/lib/servers.sh start $s w4 | tail -1
  case $s in
    postgres) waitpg ;;
    neo4j) waitbolt $GNOMON_NEO4J_URI neo4j ;;
    memgraph) waitbolt $GNOMON_MEMGRAPH_URI none ;;
  esac || { echo "SERVER $s would not start: stopping"; $KIT/lib/servers.sh logs $s w4 40; exit 2; }
}
srv_down() { $KIT/lib/servers.sh stop $1 w4 | tail -1; }
sel=(); [ -n "${W4_QUERIES:-}" ] && sel+=(--queries $W4_QUERIES); [ -n "${W4_N:-}" ] && sel+=(--n-instances $W4_N)
if has gate; then
  say "gate: Gnomon on C4, every instance, against the recorded answers of the other systems"
  restore_gnomon
  gq=(); [ -n "${W4_QUERIES:-}" ] && gq=(--queries $W4_QUERIES)
  $CK run --system gnomon --label "C4" --params $PAR --store $GSTORE "${gq[@]}" --out $RESL/answers-gnomon-c4.jsonl --alarm 60 || { echo "GATE RUN FAILED: stopping"; exit 8; }
  $PYV $TL gate --new $RESL/answers-gnomon-c4.jsonl --check-dir $CHK --out $RESL/gate.md; rc=$?
  cp -au $RESL/. $OUT/
  [ $rc = 0 ] || { echo "W4 GATE FAILED rc=$rc: stopping"; exit 7; }
fi
block() {   # token rot engine(0|1)
  local t=$1 r=$2 en=$3 s=$(base_of $1) th="" et=0 args
  case " $SYSTEMS " in *" $t "*) ;; *) echo "BLOCK TOKEN [$t] not in the system list: stopping"; exit 8 ;; esac
  case $t in duckdbsql64) th=64 ;; duckdbsql32) th=32 ;; esac
  case $s in neo4j|memgraph) et=1 ;; esac; [ $en = 1 ] && et=1
  args=(w4 --system $s --params $PAR "${sel[@]}" --beliefs current,historic --rotation $r --out $RESL/rot$r/$t.json --jsonl $RESL/rot$r/$t-calls.jsonl --label "c4 W4 $PT rot$r $t")
  [ $en = 1 ] && args+=(--engine-pass)
  case $s in gnomon) restore_gnomon; args+=(--store $GSTORE) ;; duckdbsql) restore_duck; args+=(--db ${LOCAL:-${TMPDIR:-/tmp}}/w4.duckdb) ;; esac
  mkdir -p $RESL/rot$r; say "block $PT rot$r $t (engine $en threads ${th:-default})"
  case $s in postgres) if [ $en = 1 ]; then EXTCMP_PG_STATS=1 srv_up postgres; else srv_up postgres; fi ;; neo4j|memgraph) srv_up $s ;; esac
  env ${th:+W4_DUCKDB_THREADS=$th} EXTCMP_ENGINE_TIME=$et $TR "${args[@]}" 2>&1 | tail -n 3
  [ "${PIPESTATUS[0]}" = 0 ] && [ -s $RESL/rot$r/$t.json ] || { echo "BLOCK FAILED $PT rot$r $t"; echo "$PT rot$r $t" >> $RESL/failed-blocks.txt; }
  case $s in postgres|neo4j|memgraph) srv_down $s ;; esac
  cp -au $RESL/. $OUT/
  if [ $s = gnomon ] && [ $en = 0 ]; then
    $PYV $TL stop --block $RESL/rot$r/$t.json --check-dir $CHK || { echo "STOP RULE: Gnomon block $PT rot$r"; exit 4; }
  fi
}
if has timed; then
  order=($SYSTEMS)
  for t in "${order[@]}"; do block $t 1 0; done
  rev=(); for (( k=${#order[@]}-1; k>=0; k-- )); do rev+=("${order[k]}"); done   # the list first: a function must not move this loop
  for t in "${rev[@]}"; do block $t 2 0; done
  for t in gnomon duckdbsql64 postgres; do case " $SYSTEMS " in *" $t "*) block $t E 1 ;; esac; done
  $PYV $TL timed-check --res $RESL --out $RESL/timed-check.md; rc=$?
  cp -au $RESL/. $OUT/
  [ -s $RESL/failed-blocks.txt ] && { echo "FAILED BLOCKS:"; cat $RESL/failed-blocks.txt; exit 8; }
  [ $rc = 0 ] || { echo "TIMED ANSWERS DIFFER: see timed-check.md"; exit 8; }
fi
say "W4 body done"
