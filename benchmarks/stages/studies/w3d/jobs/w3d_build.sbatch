#!/bin/bash
# W3 correction-density, lag and history stores (F-W3-4, -6, -7) on C4, untimed: one event log into a Gnomon store, the check database, the DuckDB
# full-version store and (W3D_GRAPH=1, the density sweep) Memgraph's, as prep-stores' w3.sbatch built the SF1 and SF10 stores.
# Stores under $W3/stores/<log>/full, check database under $W3/check/<log>; W3D_SMOKE=1: under $W3/smoke-stores, smoke-check. Any failure stops the job.
#SBATCH -N 1
#SBATCH --ntasks-per-node=32
#SBATCH --signal=B:USR1@300
#SBATCH -J c4-w3d-build
export W3_STAGE_ROOT=${NEXT_STAGE_ROOT:?}
source $W3_STAGE_ROOT/src/benchmarks/stages/w3/jobs/w3_lib.sh
guard
: "${W3D_LOG:?}"
sm=""; [ "${W3D_SMOKE:-0}" = 1 ] && sm=smoke-
TAG=$W3D_LOG; OUTD=$W3/${sm}stores/$TAG/full; C=$W3/${sm}check/$TAG; mkdir -p $OUTD $C
SF=${TAG%%-*}; SF=${SF#SF}; H=${TAG#*-}; H=${H%%-*}
LOG=$P3/eventlogs/W3-$TAG.eventlog.jsonl; META=$P3/base/sf$SF-$H/meta.json; SUMS=$P3/eventlogs/SHA256SUMS-SF$SF
[ -s "$LOG" ] || { echo "MISSING LOG $LOG: stopping"; exit 2; }; [ -s "$META" ] || { echo "MISSING BASE META $META: stopping"; exit 2; }
setup_root ${LOCAL:-${TMPDIR:-/tmp}}/w3b; export EXTCMP_START_WAIT_S=7200 W3_STAGE_DIR=${LOCAL:-${TMPDIR:-/tmp}}/stage W3_SQL_DUCK_MEM=$(( ${SLURM_CPUS_ON_NODE:-32} * 2 * 6 / 10 ))GB; mkdir -p $W3_STAGE_DIR
NCPU=${SLURM_CPUS_ON_NODE:-32}; X="$PYV $REPO/scripts"; ST=${LOCAL:-${TMPDIR:-/tmp}}/gnomon-$TAG
trap 'echo "=== time limit near $(utc): stopping"; $KIT/lib/servers.sh stopall; echo "STOPPED at the time limit $(utc)" > $OUTD/STOPPED; exit 5' USR1 TERM
step() { say "$1"; shift; "$@" || { echo "STEP FAILED: $*: stopping"; $KIT/lib/servers.sh stopall; exit 2; }; }
step "copy log" cp $LOG ${LOCAL:-${TMPDIR:-/tmp}}/log.jsonl
want=$(awk -v f="W3-$TAG.eventlog.jsonl" '$2 == f {print $1}' $SUMS); got=$(sha256sum ${LOCAL:-${TMPDIR:-/tmp}}/log.jsonl | cut -d' ' -f1)
[ -n "$want" ] && [ "$want" = "$got" ] && echo "log sha256 $got OK" || { echo "FROZEN LOG CHECK FAILED $TAG ($got vs $want): stopping"; exit 2; }
replay() { ( cd $REPO && $PYV -c "
import sys, pathlib, time; sys.path.insert(0, 'scripts'); import eval_harness as H
t=time.time(); H.load_store(pathlib.Path('$ST'), 'native', pathlib.Path('${LOCAL:-${TMPDIR:-/tmp}}/log.jsonl')); print('replay_s', round(time.time()-t,1))
import gnomon; s=gnomon.open('$ST', backend='native', read_only=True); print(s.stats()); s.close()" ); }
step "replay on C4" replay
[ -n "$(ls -A $ST/native 2>/dev/null)" ] || { echo "REPLAY LEFT NO STORE: stopping"; exit 2; }
packg() { tar -C ${LOCAL:-${TMPDIR:-/tmp}} -cf - gnomon-$TAG | zstd -q -T$NCPU -3 -o $OUTD/gnomon-$TAG.tar.zst -f && ( cd $OUTD && sha256sum gnomon-$TAG.tar.zst > gnomon-$TAG.tar.zst.sha256 ); }
step "persist Gnomon store" packg
step export $PYV $REPO/scripts/ps_export_paged.py w3 --mode keys --repo $REPO --store $ST --out $C/rows
step hist $X/w3_snb_sql.py hist --rows $C/rows --out $C/hist
rm -f $C/w3.duckdb $C/w3.duckdb.wal
step checkdb $X/w3_snb_run.py duckdb-load --hist $C/hist --db $C/w3.duckdb
step "load duckdb" $X/w3_snb_ingest.py sql --system duckdb --hist $C/hist --base-meta $META --full --db $OUTD/duckdb.duckdb --out $OUTD/duckdb.load.json
( cd $OUTD && sha256sum duckdb.duckdb > duckdb.duckdb.sha256 )
step "digest duckdb" $X/w3_snb_ingest.py digest --system duckdb --target $OUTD/duckdb.duckdb --out $OUTD/duckdb.digest.json --parts 8
if [ "${W3D_GRAPH:-0}" = 1 ]; then
  step "prepare graph CSVs" env W3_GRAPH_DUCK_MEM=$W3_SQL_DUCK_MEM $X/w3_snb_ingest.py prepare --hist $C/hist --out ${LOCAL:-${TMPDIR:-/tmp}}/gcsv --duck-db ${LOCAL:-${TMPDIR:-/tmp}}/graph.duckdb
  step "load memgraph" $X/w3_snb_ingest.py graph --system memgraph --full --prepared ${LOCAL:-${TMPDIR:-/tmp}}/gcsv --hist $C/hist --base-meta $META --srv $SRV \
     --uri bolt://127.0.0.1:$EXTCMP_MG_PORT --servers $KIT/lib/servers.sh --neo4j-sif $SIF_DIR/neo4j.sif --out $OUTD/memgraph.load.json
  $KIT/lib/servers.sh stop memgraph w3 | tail -1; rm -rf $SRV/memgraph-w3/lib/import 2>/dev/null
  packm() { tar -C $SRV -cf - memgraph-w3 | zstd -q -T$NCPU -3 -o $OUTD/memgraph.tar.zst -f && ( cd $OUTD && sha256sum memgraph.tar.zst > memgraph.tar.zst.sha256 ); }
  step "persist memgraph" packm
fi
for f in $OUTD/gnomon-$TAG.tar.zst $OUTD/duckdb.duckdb $C/w3.duckdb; do [ -s $f ] || { echo "MISSING OUTPUT $f: stopping"; exit 2; }; done
say "build done $TAG"
