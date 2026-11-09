#!/bin/bash
# C4 causes, W4 sf10-r10, historic belief: the six short reads and TCR3, first two instances: Gnomon, PostgreSQL, DuckDB (SQL), Memgraph, Neo4j; on the timed stage's stores.
source ${NEXT_STAGE_ROOT:?}/src/benchmarks/stages/studies/lib.sh
: "${OUT:?}" "${ROOT:?}"; W=$E/w4-finbench; PT=sf10-r10; SF=sf10; S=$W/w4stores/$PT; PAR=$W/params/params-$SF.json; ST=$REPO/$STG/causes
Q=${C_QUERIES:-TSR1,TSR2,TSR3,TSR4,TSR5,TSR6,TCR3}; SYS=${C_SYSTEMS:-gnomon postgres duckdbsql neo4j memgraph}; N=${C_N:-2}
setup_root $ROOT; export TMPDIR=$ROOT/scratch TR_CEILING_S=10 TR_GRACE_S=5 EXTCMP_MG_MEMORY_LIMIT_MB=${C_MG_MB:-118000} W4_DUCKDB_THREADS=${C_DUCK_THREADS:-32}
source $KIT/lib/env.sh
export EXTCMP_SERVER_TIMEOUT_S=10 EXTCMP_START_WAIT_S=7200 EXTCMP_PG_STATS=1 EXTCMP_ENGINE_TIME=1
PR=$RES/probe; mkdir -p $PR
waitbolt() { local i; for i in $(seq 1 600); do $PYV -c "
import neo4j,sys
a={'neo4j':('neo4j','neo4j'),'none':None}[sys.argv[2]]
d=neo4j.GraphDatabase.driver(sys.argv[1], auth=a); d.verify_connectivity(); d.close()" "$1" "$2" 2>/dev/null && { echo "bolt $1 ready"; return 0; }; sleep 2; done; return 1; }
waitpg() { local i; for i in $(seq 1 300); do $PYV -c "import psycopg,os; psycopg.connect(os.environ['EXTCMP_PG_DSN']).close()" 2>/dev/null && { echo "postgres ready"; return 0; }; sleep 2; done; return 1; }
P=(--params $PAR --queries $Q --beliefs historic --n-instances $N --out $PR)
for s in $SYS; do
  say "probe $s"
  case $s in
    gnomon) g=$(ls $S/gnomon-*.tar.zst | head -1); restore_tar $g $STORES; need_dir $STORES/gnomon-$PT/native
            GNOMON_MATCH_TRACE=1 $PYV $ST/causes_probe.py w4 --system gnomon --store $STORES/gnomon-$PT "${P[@]}" || echo "PROBE FAILED $s" ;;
    duckdbsql) ( cd $S && sha256sum -c --quiet duckdbsql.duckdb.sha256 ) || { echo "RESTORE CHECK FAILED"; exit 2; }; cp $S/duckdbsql.duckdb ${LOCAL:-${TMPDIR:-/tmp}}/w4.duckdb
            $PYV $ST/causes_probe.py w4 --system duckdbsql --db ${LOCAL:-${TMPDIR:-/tmp}}/w4.duckdb "${P[@]}" || echo "PROBE FAILED $s" ;;
    postgres|neo4j|memgraph)
            restore_tar $S/$s.tar.zst $SRV; need_dir $SRV/$s-w4; $KIT/lib/servers.sh start $s w4 | tail -1
            case $s in postgres) waitpg ;; neo4j) waitbolt $GNOMON_NEO4J_URI neo4j ;; memgraph) waitbolt $GNOMON_MEMGRAPH_URI none ;; esac || { echo "SERVER $s DID NOT START"; continue; }
            $PYV $ST/causes_probe.py w4 --system $s "${P[@]}" || echo "PROBE FAILED $s"
            $KIT/lib/servers.sh stop $s w4 | tail -1 ;;
  esac
  cp -au $RES/. $OUT/
done
say "W4 causes done"
