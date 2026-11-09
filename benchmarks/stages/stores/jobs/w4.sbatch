#!/bin/bash
#SBATCH -J ps-w4
#SBATCH -N 1
#SBATCH -o w4-%j.out
# (DuckDB (SQL), PostgreSQL, Neo4j, Memgraph, from the exported version rows, as at SF1), persisted with a
# read-back verification against the export and an order-independent digest, and the baseline-vs-baseline answer
# check on the SF10 parameter file under the cap. Partition, cores and wall time are given at submission.
#
# STEPS, in this order:
#   export   replay the frozen log into a Gnomon store on ${LOCAL:-${TMPDIR:-/tmp}} and export the typed version rows (w4-finbench/repo,
#            its venv and engine build, exactly as the SF1 export; the store is a transcription vehicle, not kept)
#   params   the SF10 parameter file from the rho = 0 export, as at SF1 (w4_check.py params)
#   duckdbsql postgres neo4j memgraph   load, verify, persist, (restart,) digest, answer check
#   compare  the answers against DuckDB (SQL)'s; the digest pairs
set -uo pipefail
source ${GNOMON_BENCH_ROOT}/prep-stores/site.env
export EXTCMP_ROOT=${LOCAL:-${TMPDIR:-/tmp}}/ps EXTCMP_DURABLE=1 TMPDIR=${LOCAL:-${TMPDIR:-/tmp}} EXTCMP_START_WAIT_S=${EXTCMP_START_WAIT_S:-3600}
unset EXTCMP_NEO4J_PLUGINS
CAP=${CAP:-120}
mkdir -p $EXTCMP_ROOT/srv $EXTCMP_ROOT/scratch; SRV=$EXTCMP_ROOT/srv; SRVSH=$KIT/lib/servers.sh
NCPU=${SLURM_CPUS_ON_NODE:-16}
PY4=$W4/repo/.venv/bin/python
CK="$PY4 $P/src/scripts/w4_check.py"
SF=${SF:-10}; RHO=${RHO:?set RHO}; EXP=$W4/exports/sf$SF-r$RHO; OUT=$W4/w4stores/sf$SF-r$RHO; A=$W4/check-sf$SF/r$RHO
PAR=$W4/params/params-sf$SF.json; LOG=$W4/eventlogs/w4-sf$SF-r$RHO.eventlog.jsonl
mkdir -p $OUT $A
say() { echo "=== $* $(date -u +%FT%TZ)"; }
has() { case " ${STEPS:?set STEPS} " in *" $1 "*) return 0 ;; esac; return 1; }
say "host $(hostname) job $SLURM_JOB_ID cpus $NCPU sf $SF rho $RHO steps [$STEPS] src $(git -C $P/src rev-parse --short HEAD) dirty=$(git -C $P/src status --porcelain --untracked-files=no | wc -l)"
echo "ports pg=$EXTCMP_PG_PORT mg=$EXTCMP_MG_PORT neo4j=$EXTCMP_NEO4J_PORT mg_memory_limit_mb=$EXTCMP_MG_MEMORY_LIMIT_MB"
declare -A DIRS=([postgres]=postgres-w4 [neo4j]=neo4j-w4 [memgraph]=memgraph-w4)
persist() {  # <sys>: stop the server, pack its data directory, checksum it
  local s=$1; $SRVSH stop $s w4
  local t0=$(date +%s)
  tar -C $SRV -cf - ${DIRS[$s]} | zstd -q -T$NCPU -3 -o $OUT/$s.tar.zst -f
  (cd $OUT && sha256sum $s.tar.zst > $s.tar.zst.sha256)
  echo "persisted $s $(du -b $OUT/$s.tar.zst | cut -f1) bytes (data dir $(du -sb $SRV/${DIRS[$s]} | cut -f1)) in $(( $(date +%s) - t0 )) s: $(cat $OUT/$s.tar.zst.sha256)"
}
timed() { local t0=$(date +%s); "$@"; local rc=$?; echo "  [$1 ... ${*: -1}] rc=$rc $(( $(date +%s) - t0 )) s"; return $rc; }

if has export; then
  say verify log; (cd $W4/eventlogs && grep " $(basename $LOG)\$" SHA256SUMS | sha256sum -c -) || exit 1
  say export; ST=${LOCAL:-${TMPDIR:-/tmp}}/native-sf$SF-r$RHO; rm -rf $ST $EXP
  # the replay as w4_check.py export does it (the W4 repo's eval_harness and engine); the rows through the engine's
  # paged iterator
  /usr/bin/time -v $PY4 $P/src/scripts/ps_export_paged.py w4 --repo $W4/repo --log $LOG --store $ST --out $EXP 2> $OUT/export.time \
    || { tail -20 $OUT/export.time; exit 1; }
  [ -e $EXP/export.json ] || { echo "EXPORT FAILED (no export.json)"; tail -20 $OUT/export.time; exit 1; }
  grep -E "Elapsed|Maximum resident" $OUT/export.time
  echo "engine $(cat $W4/engine-build.txt | tr '\n' ' ') store $(du -sb $ST | cut -f1) bytes"; cat $EXP/export.json; echo
  rm -rf $ST
fi
if has params; then
  say params
  [ -e $PAR ] && { echo "$PAR exists: not regenerated"; } || $CK params --dir $W4/exports/sf$SF-r0 --log-meta $W4/eventlogs/w4-sf$SF-r0.eventlog.meta.json --out $PAR || exit 1
  (cd $W4/params && sha256sum params-sf$SF.json | tee params-sf$SF.json.sha256)
fi
for sys in duckdbsql postgres neo4j memgraph; do
  has $sys || continue
  say load $sys
  db=""; [ $sys = duckdbsql ] && db="--db ${LOCAL:-${TMPDIR:-/tmp}}/check.duckdb"
  if [ $sys = duckdbsql ]; then
    rm -f $OUT/duckdbsql.duckdb $OUT/duckdbsql.duckdb.wal
    timed $CK load --system duckdbsql --dir $EXP --db $OUT/duckdbsql.duckdb > $OUT/duckdbsql.load.json || { echo "LOAD FAILED duckdbsql"; continue; }
    (cd $OUT && sha256sum duckdbsql.duckdb > duckdbsql.duckdb.sha256 && cat duckdbsql.duckdb.sha256)
    timed $CK verify --system duckdbsql --dir $EXP --db $OUT/duckdbsql.duckdb --out $OUT/duckdbsql.verify.json
    timed $CK digest --system duckdbsql --db $OUT/duckdbsql.duckdb --out $OUT/duckdbsql.digest.json
    cp $OUT/duckdbsql.duckdb ${LOCAL:-${TMPDIR:-/tmp}}/check.duckdb
  else
    $SRVSH wipe $sys w4
    EXTCMP_SERVER_TIMEOUT_S=14400 $SRVSH start $sys w4 || { echo "$sys did not start"; continue; }
    [ $sys = postgres ] && [ ! -e $A/repro-W4-P2.json ] && { $PY4 $P/src/scripts/ps_w4_alarm_repro.py > $A/repro-W4-P2.json 2>&1; cat $A/repro-W4-P2.json; }
    timed $CK load --system $sys --dir $EXP > $OUT/$sys.load.json 2> $OUT/$sys.load.err || { echo "LOAD FAILED $sys"; tail -5 $OUT/$sys.load.err; $SRVSH stop $sys w4; continue; }
    cat $OUT/$sys.load.json
    timed $CK verify --system $sys --dir $EXP --out $OUT/$sys.verify.json
    [ $sys = memgraph ] && timed $CK snapshot --system memgraph
    persist $sys
    EXTCMP_SERVER_TIMEOUT_S=$CAP $SRVSH start $sys w4 || { echo "$sys did not restart"; continue; }
    if [ $sys = postgres ]; then timed $CK digest --system postgres --out $OUT/postgres.digest.json
    else timed $CK digest --system $sys --out $OUT/$sys.digest.json --parts ${DPARTS:-128} --workers ${DWORKERS:-8}; fi
  fi
  say check $sys
  rm -f $A/answers-$sys.jsonl
  timed $CK run --system $sys --params $PAR $db --out $A/answers-$sys.jsonl --alarm $CAP
  $PY4 -c "import json, collections; print('$sys', dict(collections.Counter(json.loads(l)['status'] for l in open('$A/answers-$sys.jsonl'))))"
  [ $sys != duckdbsql ] && $SRVSH stop $sys w4
done
if has gnomon; then
  # Gnomon on the pinned engine (ENGREPO, default the final-engine tree $P/srcf): replay of the frozen log on node-local
  # NVMe (log copied there first), the store persisted to $PROJECT, the answer check under the cap, compared with
  # DuckDB (SQL). Untimed.
  ENGREPO=${ENGREPO:-$P/srcf}; PYE=$ENGREPO/.venv/bin/python; ENGC=$(git -C $ENGREPO rev-parse --short HEAD)
  ENGSO=$(sha256sum $ENGREPO/gnomon/_engine*.so | cut -c1-16); GT=gnomon-$ENGC-so$ENGSO.tar.zst; GS=${LOCAL:-${TMPDIR:-/tmp}}/gnomon-sf$SF-r$RHO
  export GNOMON_MATCH_THREADS=${GNOMON_MATCH_THREADS:-32}
  say gnomon replay $ENGC so $ENGSO
  cp $LOG ${LOCAL:-${TMPDIR:-/tmp}}/log.jsonl
  (cd $W4/eventlogs && grep " $(basename $LOG)\$" SHA256SUMS | sed "s# $(basename $LOG)# ${LOCAL:-${TMPDIR:-/tmp}}/log.jsonl#" | sha256sum -c -) || exit 1
  rm -rf $GS
  (cd $ENGREPO && /usr/bin/time -v $PYE -c "
import sys, pathlib, time; sys.path.insert(0, 'scripts'); import eval_harness as H
t=time.time(); H.load_store(pathlib.Path('$GS'), 'native', pathlib.Path('${LOCAL:-${TMPDIR:-/tmp}}/log.jsonl')); print('replay_s', round(time.time()-t,1), 'log and store on \${LOCAL:-${TMPDIR:-/tmp}}')") 2> $OUT/gnomon-replay.time || { tail $OUT/gnomon-replay.time; exit 1; }
  grep -E "replay_s|Elapsed|Maximum resident" $OUT/gnomon-replay.time; rm -f ${LOCAL:-${TMPDIR:-/tmp}}/log.jsonl
  t0=$(date +%s); tar -C ${LOCAL:-${TMPDIR:-/tmp}} -cf - $(basename $GS) | zstd -q -T$NCPU -3 -o $OUT/$GT -f; (cd $OUT && sha256sum $GT > $GT.sha256)
  echo "persisted $GT $(du -b $OUT/$GT | cut -f1) bytes in $(( $(date +%s) - t0 )) s: $(cat $OUT/$GT.sha256)"
  echo "{\"engine_commit\": \"$(git -C $ENGREPO rev-parse HEAD)\", \"engine_so_sha256\": \"$(sha256sum $ENGREPO/gnomon/_engine*.so | cut -d' ' -f1)\", \"log\": \"$LOG\", \"job\": $SLURM_JOB_ID, \"note\": \"untimed replay; log and store on node-local NVMe\"}" > $OUT/$GT.json
  say check gnomon
  rm -f $A/answers-gnomon-$ENGC.jsonl
  timed $PYE $ENGREPO/scripts/w4_check.py run --system gnomon --label "$ENGC so $ENGSO" --params $PAR --store $GS --out $A/answers-gnomon-$ENGC.jsonl --alarm $CAP
  $PY4 -c "import json, collections; print('gnomon', dict(collections.Counter(json.loads(l)['status'] for l in open('$A/answers-gnomon-$ENGC.jsonl'))))"
  $CK compare --base $A/answers-duckdbsql.jsonl $A/answers-gnomon-$ENGC.jsonl --json $A/verdicts-gnomon-$ENGC.json | tee $A/verdicts-gnomon-$ENGC.txt
fi
if has compare; then
  say compare
  $CK compare --base $A/answers-duckdbsql.jsonl $(ls $A/answers-postgres.jsonl $A/answers-neo4j.jsonl $A/answers-memgraph.jsonl 2>/dev/null) --json $A/verdicts.json | tee $A/verdicts.txt
  for pr in "duckdbsql postgres" "neo4j memgraph"; do set -- $pr
    [ -e $OUT/$1.digest.json ] && [ -e $OUT/$2.digest.json ] && { cmp -s $OUT/$1.digest.json $OUT/$2.digest.json && echo "digest $1 = $2" || echo "DIGEST DIFFERS $1 $2"; }
  done
fi
say done
