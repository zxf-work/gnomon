#!/bin/bash
#SBATCH -J ps-w3
#SBATCH -N 1
#SBATCH -o w3-%j.out
# bulk import, D-W3-9), the DuckDB check database, the guarded parameter regeneration (D-W3-8) and the
# baseline-vs-baseline answer check at the instant settings. Partition, cores and wall time are given at submission.
#
# STEPS (space-separated), in this order:
#   replay   replay the frozen log (node-local copy) into a Gnomon store on ${LOCAL:-${TMPDIR:-/tmp}} with ENGREPO's engine
#   gpersist the replayed store -> $OUT/gnomon-<commit>-so<sha>.tar.zst (+ sha256, engine record)
#   export   the store's canonical version rows -> $C/rows            (w3_snb_sql.py export's columns, paged)
#   hist     LDBC-schema history tables -> $C/hist                    (w3_snb_sql.py hist)
#   checkdb  the DuckDB check database -> $C/w3.duckdb                (w3_snb_run.py duckdb-load)
#   duckdb   the DuckDB full-version store, sha256, digest
#   prepare  the graph CSVs of the full-version load, once for Neo4j and Memgraph -> $OUT/graph-csv.tar.zst
#   params   the guarded regeneration: the as-of half of the alive-anchor rule and the IC13/IC14 b-file rule
#   dcheck   DuckDB answer check (current, asof) on the full store
#   postgres load, persist, restart, digest, answer check
#   gcheck   Gnomon (ENGREPO) answer check (current, asof) on the persisted Gnomon store, compared with DuckDB
#   neo4j | memgraph   load from the prepared CSVs, persist, restart (SKIP_LOAD=1: restore the persisted store), digest, answer check
# W: answer-check worker processes (disjoint query sets, one connection each; each cell under the cap).
# CDIR: the check directory (default $W3/check/<tag>); OUTDIR: the store directory (default $W3/w3stores/<tag>/full); NOCHECK=1: no answer check (the code validation).
set -uo pipefail
source ${GNOMON_BENCH_ROOT}/prep-stores/site.env
export EXTCMP_ROOT=${LOCAL:-${TMPDIR:-/tmp}}/ps EXTCMP_DURABLE=1 TMPDIR=${LOCAL:-${TMPDIR:-/tmp}} W3_EXT=$W3/ext EXTCMP_NEO4J_PLUGINS='["graph-data-science"]'
# a server start waits for its port, then verifies its listener; Memgraph's SF10 recovery takes most of an hour
export EXTCMP_START_WAIT_S=${EXTCMP_START_WAIT_S:-7200}
CAP=${CAP:-120}; export W3_CAP_S=$CAP
mkdir -p $EXTCMP_ROOT/srv $EXTCMP_ROOT/scratch; SRV=$EXTCMP_ROOT/srv; SRVSH=$KIT/lib/servers.sh
NCPU=${SLURM_CPUS_ON_NODE:-16}
PYW=$W3/src/.venv/bin/python
ENGREPO=${ENGREPO:-$W3/src}; PYE=$ENGREPO/.venv/bin/python
ENGSO=$(sha256sum $ENGREPO/gnomon/_engine*.so | cut -c1-16); ENGC=$(git -C $ENGREPO rev-parse --short HEAD)
X="$PYW $P/src/scripts"
SF=${SF:-10}; H=h1; R=${R:-1}; TAG=SF$SF-$H-r$R; LOG=$W3/eventlogs/W3-$TAG.eventlog.jsonl; C=${CDIR:-$W3/check/$TAG}
META=$W3/base/sf$SF-$H/meta.json; OUT=${OUTDIR:-$W3/w3stores/$TAG/full}; PAR=${PAR:-$W3/params/W3-SF$SF.params.jsonl}
mkdir -p $C $OUT $P/results/w3/$TAG
say() { echo "=== $* $(date -u +%FT%TZ)"; }
has() { case " ${STEPS:?set STEPS} " in *" $1 "*) return 0 ;; esac; return 1; }
declare -A DIRS=([postgres]=postgres-w3 [neo4j]=neo4j-w3 [memgraph]=memgraph-w3)
declare -A URIS=([neo4j]=bolt://127.0.0.1:$EXTCMP_NEO4J_PORT [memgraph]=bolt://127.0.0.1:$EXTCMP_MG_PORT)
say "host $(hostname) job $SLURM_JOB_ID cpus $NCPU tag $TAG steps [$STEPS] src $(git -C $P/src rev-parse --short HEAD) dirty=$(git -C $P/src status --porcelain --untracked-files=no | wc -l)"
echo "ports pg=$EXTCMP_PG_PORT mg=$EXTCMP_MG_PORT neo4j=$EXTCMP_NEO4J_PORT mg_memory_limit_mb=$EXTCMP_MG_MEMORY_LIMIT_MB"
waitpg() { $PYW - <<'PY'
import time, psycopg, os
for i in range(900):
    try:
        psycopg.connect(os.environ["EXTCMP_PG_DSN"]).close(); print("ready postgres", i); break
    except Exception: time.sleep(2)
PY
}
waitbolt() { $PYW -c "
import time, neo4j
for i in range(3600):
    try:
        d = neo4j.GraphDatabase.driver('${URIS[$1]}', auth=('neo4j','neo4j') if '$1' == 'neo4j' else ('','')); d.verify_connectivity(); d.close(); print('ready $1', i); break
    except Exception: time.sleep(2)"
}
persist() {  # <sys>: stop the server, pack its data directory, checksum it
  local s=$1; $SRVSH stop $s w3
  rm -rf $SRV/${DIRS[$s]}/lib/import 2>/dev/null
  local t0=$(date +%s)
  tar -C $SRV -cf - ${DIRS[$s]} | zstd -q -T$NCPU -3 -o $OUT/$s.tar.zst -f
  (cd $OUT && sha256sum $s.tar.zst > $s.tar.zst.sha256)
  echo "persisted $s $(du -b $OUT/$s.tar.zst | cut -f1) bytes (data dir $(du -sb $SRV/${DIRS[$s]} | cut -f1)) in $(( $(date +%s) - t0 )) s: $(cat $OUT/$s.tar.zst.sha256)"
}
restore() {  # <sys>: verify the checksum, unpack into ${LOCAL:-${TMPDIR:-/tmp}}
  local s=$1 t0=$(date +%s)
  (cd $OUT && sha256sum -c $s.tar.zst.sha256) || { echo "CHECKSUM MISMATCH $s"; return 1; }
  rm -rf $SRV/${DIRS[$s]}; zstd -q -d -c -T$NCPU $OUT/$s.tar.zst | tar -C $SRV -xf -
  echo "restored $s in $(( $(date +%s) - t0 )) s"
}
pcheck() {   # <sys> <target> <out>: the answer check on W worker processes (disjoint query sets, balanced by SF1 times)
  local sys=$1 tgt=$2 out=$3 w=${W:-1} k
  [ "${NOCHECK:-0}" = 1 ] && { echo "NOCHECK: no answer check for $sys"; return 0; }
  rm -f $out $out.part*
  local ref=$W3/check/SF1-h1-r1/$sys-v2.jsonl
  local groups; groups=$($PYW - "$PAR" "$ref" "$w" <<'PY'
import json, sys, os
par, ref, w = sys.argv[1], sys.argv[2], int(sys.argv[3])
qs = list(dict.fromkeys(json.loads(l)["qid"] for l in open(par)))
cost = {q: 1.0 for q in qs}
if os.path.exists(ref):
    for l in open(ref):
        x = json.loads(l)
        if x["setting"] in ("current", "asof") and x["qid"] in cost:
            cost[x["qid"]] += min(120.0, 12 * x.get("ms", 0) / 1e3) if x.get("ok") else 120.0
g = [[0.0, []] for _ in range(w)]
for q in sorted(qs, key=lambda q: -cost[q]):
    m = min(g, key=lambda x: x[0]); m[0] += cost[q]; m[1].append(q)
print(" ".join(",".join(x[1]) for x in g if x[1]))
PY
)
  local pids=() i=0
  for k in $groups; do
    $X/w3_snb_run.py run --system $sys --target "$tgt" --params $PAR --queries $k --settings current,asof \
      --hist-db $C/w3.duckdb --out $out.part$i > $P/results/w3/$TAG/run-$sys-$SLURM_JOB_ID-part$i.log 2>&1 &
    pids+=($!); i=$((i + 1))
  done
  echo "check $sys: ${#pids[@]} workers, pids ${pids[*]}"
  for p in "${pids[@]}"; do wait $p; echo "worker $p rc=$?"; done
  cat $out.part* > $out && rm -f $out.part*
  echo "cells $(wc -l < $out)"
  $PYW $P/src/scripts/w3_relabel_cap.py $out $CAP "SF$SF instant answer check, stage prep-stores, job $SLURM_JOB_ID" | tail -3
}

ST=${LOCAL:-${TMPDIR:-/tmp}}/gnomon-$TAG; GTAR=gnomon-$ENGC-so$ENGSO.tar.zst
if has replay; then
  # the frozen log is read from a node-local copy and the store is written to ${LOCAL:-${TMPDIR:-/tmp}} (node NVMe); only the
  # finished store is copied to $PROJECT (gpersist). Untimed.
  say copy log; t0=$(date +%s); cp $LOG ${LOCAL:-${TMPDIR:-/tmp}}/log.jsonl; echo "copied in $(( $(date +%s) - t0 )) s"
  (cd $W3/eventlogs && grep " W3-$TAG.eventlog.jsonl\$" SHA256SUMS-SF$SF | sed "s# W3-$TAG.eventlog.jsonl# ${LOCAL:-${TMPDIR:-/tmp}}/log.jsonl#" | sha256sum -c -) || exit 1
  say replay; rm -rf $ST
  (cd $ENGREPO && /usr/bin/time -v $PYE -c "
import sys, pathlib, time; sys.path.insert(0, 'scripts'); import eval_harness as H
t=time.time(); H.load_store(pathlib.Path('$ST'), 'native', pathlib.Path('${LOCAL:-${TMPDIR:-/tmp}}/log.jsonl')); print('replay_s', round(time.time()-t,1), 'store on \${LOCAL:-${TMPDIR:-/tmp}}, log on \${LOCAL:-${TMPDIR:-/tmp}}')
import gnomon; s=gnomon.open('$ST', backend='native', read_only=True); print(s.stats()); s.close()") 2> $P/results/w3/$TAG/replay-$SLURM_JOB_ID.time || exit 1
  grep -E "Elapsed|Maximum resident" $P/results/w3/$TAG/replay-$SLURM_JOB_ID.time
  echo "engine $ENGC so $ENGSO store $(du -sb $ST | cut -f1) bytes"; rm -f ${LOCAL:-${TMPDIR:-/tmp}}/log.jsonl
fi
if has gpersist; then
  say persist gnomon store; t0=$(date +%s)
  tar -C ${LOCAL:-${TMPDIR:-/tmp}} -cf - gnomon-$TAG | zstd -q -T$NCPU -3 -o $OUT/$GTAR -f
  (cd $OUT && sha256sum $GTAR > $GTAR.sha256) && echo "persisted $GTAR $(du -b $OUT/$GTAR | cut -f1) bytes in $(( $(date +%s) - t0 )) s: $(cat $OUT/$GTAR.sha256)"
  echo "{\"engine_commit\": \"$(git -C $ENGREPO rev-parse HEAD)\", \"engine_so_sha256\": \"$(sha256sum $ENGREPO/gnomon/_engine*.so | cut -d' ' -f1)\", \"log\": \"$LOG\", \"job\": $SLURM_JOB_ID, \"note\": \"untimed replay; log and store on node-local NVMe\"}" > $OUT/$GTAR.json
fi
if has export; then
  # an earlier job of the same tag must not be writing $C/rows (OLDJOB: wait until it has left the queue)
  for o in ${OLDJOB:-}; do while squeue -h -j $o 2>/dev/null | grep -q .; do echo "waiting for job $o to leave the queue $(date -u +%T)"; sleep 300; done; done
  if [ ! -d $ST ]; then   # the replay ran in an earlier job: its persisted store
    say restore gnomon store $GTAR; (cd $OUT && sha256sum -c $GTAR.sha256) || exit 1
    zstd -q -d -c -T$NCPU $OUT/$GTAR | tar -C ${LOCAL:-${TMPDIR:-/tmp}} -xf - || exit 1
  fi
  say export; rm -rf $C/rows
  # w3_snb_sql.py export's columns from the engine's keyed pages (scripts/ps_export_paged.py, the replaying engine's
  # gnomon): its all_*_versions lists do not fit a node at SF10
  /usr/bin/time -v $PYE $P/src/scripts/ps_export_paged.py w3 --mode keys --repo $ENGREPO --store $ST --out $C/rows \
    2> $P/results/w3/$TAG/export-$SLURM_JOB_ID.time || { tail -20 $P/results/w3/$TAG/export-$SLURM_JOB_ID.time; exit 1; }
  grep -E "Elapsed|Maximum resident" $P/results/w3/$TAG/export-$SLURM_JOB_ID.time
  rm -rf $ST
fi
if has hist; then
  say hist; rm -rf $C/hist
  $X/w3_snb_sql.py hist --rows $C/rows --out $C/hist | cut -c1-2000 || exit 1
fi
if has checkdb; then
  say checkdb; rm -f $C/w3.duckdb $C/w3.duckdb.wal
  $X/w3_snb_run.py duckdb-load --hist $C/hist --db $C/w3.duckdb || exit 1
  (cd $C && sha256sum w3.duckdb > w3.duckdb.sha256 && cat w3.duckdb.sha256)
fi
export W3_STAGE_DIR=${LOCAL:-${TMPDIR:-/tmp}}/stage W3_SQL_DUCK_MEM=${DUCKMEM:-$(( NCPU * 2 * 6 / 10 ))GB}; mkdir -p $W3_STAGE_DIR
if has duckdb; then
  say load duckdb
  $X/w3_snb_ingest.py sql --system duckdb --hist $C/hist --base-meta $META --full --db $OUT/duckdb.duckdb --out $OUT/duckdb.load.json || exit 1
  (cd $OUT && sha256sum duckdb.duckdb > duckdb.duckdb.sha256 && cat duckdb.duckdb.sha256)
  say digest duckdb
  $X/w3_snb_ingest.py digest --system duckdb --target $OUT/duckdb.duckdb --out $OUT/duckdb.digest.json --parts 8 || exit 1
fi
if has prepare; then
  say prepare graph CSVs; rm -rf ${LOCAL:-${TMPDIR:-/tmp}}/gcsv
  W3_GRAPH_DUCK_MEM=$W3_SQL_DUCK_MEM $X/w3_snb_ingest.py prepare --hist $C/hist --out ${LOCAL:-${TMPDIR:-/tmp}}/gcsv --duck-db ${LOCAL:-${TMPDIR:-/tmp}}/graph.duckdb || exit 1
  t0=$(date +%s); tar -C ${LOCAL:-${TMPDIR:-/tmp}} -cf - gcsv | zstd -q -T$NCPU -3 -o $OUT/graph-csv.tar.zst -f
  (cd $OUT && sha256sum graph-csv.tar.zst > graph-csv.tar.zst.sha256); cp ${LOCAL:-${TMPDIR:-/tmp}}/gcsv/prepared.json $OUT/graph-csv.prepared.json
  echo "packed graph CSVs $(du -sb ${LOCAL:-${TMPDIR:-/tmp}}/gcsv | cut -f1) -> $(du -b $OUT/graph-csv.tar.zst | cut -f1) bytes in $(( $(date +%s) - t0 )) s"
fi
if has params; then
  say params
  ALLOWED=IC1:IC2:IC3:IC4:IC5:IC6:IC7:IC8:IC9:IC10:IC11:IC12:IC13:IC14:IS1:IS2:IS3:IS4:IS5:IS6:IS7:BI10:BI15:BI20:TLDBC-IS1:TLDBC-IS2:TLDBC-IS3:TLDBC-IS4:TLDBC-IS5:TLDBC-IS6:BIH-K:BIH-VER:BIH-R7:BIH-R3
  SUPTAG=${SUPTAG:-pre-asof-anchor}
  for spec in ${PSPECS:-6:W3-SF$SF.params.jsonl 10:W3-SF$SF.timed.params.jsonl}; do
    n=${spec%%:*}; f=$W3/params/${spec#*:}
    W3_DUCKDB_THREADS=$NCPU W3_CAP_S=900 $X/w3_snb_params.py --ldbc $W3/params/ldbc-sf$SF --base $W3/base/sf$SF-h1 \
      --corrections $W3/eventlogs/W3-SF$SF-h1-r0p02.eventlog.jsonl.corrections.parquet --n $n --out $f.new \
      --nopath IC13,IC14 --nonempty-db $C/w3.duckdb > $P/results/w3/$TAG/params-$(basename $f)-$SLURM_JOB_ID.out 2>&1 || { echo "params failed: $f"; tail $P/results/w3/$TAG/params-$(basename $f)-$SLURM_JOB_ID.out; exit 1; }
    $PYW -I $P/src/scripts/w3_params_guard.py $f $f.new $ALLOWED || { echo "GUARD REFUSED $f: kept the old file, the new one is $f.new"; exit 1; }
    cp -p $f $W3/params/superseded/$(basename $f).$SUPTAG; cp -p $f.report.json $W3/params/superseded/$(basename $f).report.json.$SUPTAG
    mv $f.new $f; mv $f.new.report.json $f.report.json
    $PYW -c "import json; r=json.load(open('$f.report.json')); print('$(basename $f)', {q: r['strata'][q] for q in ('IC13', 'IC14')}, 'scanned', r['nonempty_scanned'], 'found', r['nonempty_found'], 'dead_anchor_skipped', r['dead_anchor_skipped'])"
  done
  (cd $W3/params && sha256sum W3-*.params.jsonl > SHA256SUMS && cat SHA256SUMS)
fi
if has dcheck; then
  say check duckdb
  (cd $OUT && sha256sum -c duckdb.duckdb.sha256) || exit 1
  cp $OUT/duckdb.duckdb ${LOCAL:-${TMPDIR:-/tmp}}/check.duckdb
  export W3_DUCKDB_THREADS=$(( NCPU / ${W:-1} )); pcheck duckdb ${LOCAL:-${TMPDIR:-/tmp}}/check.duckdb $C/duckdb-v2.jsonl
fi
if has postgres; then
  if [ "${SKIP_LOAD:-0}" = 1 ]; then say restore postgres; restore postgres || exit 1
  else
  say load postgres; $SRVSH wipe postgres w3
  EXTCMP_SERVER_TIMEOUT_S=14400 $SRVSH start postgres w3 && waitpg || exit 1
  $X/w3_snb_ingest.py sql --system postgres --hist $C/hist --base-meta $META --full --tmp ${LOCAL:-${TMPDIR:-/tmp}} --dsn "$EXTCMP_PG_DSN" \
     --out $OUT/postgres.load.json && persist postgres || { echo "LOAD FAILED postgres: not persisted"; $SRVSH stop postgres w3; exit 1; }
  fi
  say digest postgres
  $SRVSH start postgres w3 && waitpg || exit 1
  $X/w3_snb_ingest.py digest --system postgres --target "$EXTCMP_PG_DSN" --out $OUT/postgres.digest.json --parts 8
  $X/w3_snb_ingest.py cmp $OUT/duckdb.digest.json $OUT/postgres.digest.json
  say check postgres
  pcheck postgres "$EXTCMP_PG_DSN" $C/postgres-v2.jsonl
  $SRVSH stop postgres w3
fi
for sys in neo4j memgraph; do
  has $sys || continue
  if [ "${SKIP_LOAD:-0}" != 1 ]; then
    say unpack graph CSVs; rm -rf ${LOCAL:-${TMPDIR:-/tmp}}/gcsv
    (cd $OUT && sha256sum -c graph-csv.tar.zst.sha256) || exit 1
    zstd -q -d -c -T$NCPU $OUT/graph-csv.tar.zst | tar -C ${LOCAL:-${TMPDIR:-/tmp}} -xf -
    say load $sys
    $X/w3_snb_ingest.py graph --system $sys --full --prepared ${LOCAL:-${TMPDIR:-/tmp}}/gcsv --hist $C/hist --base-meta $META --srv $SRV \
       --uri ${URIS[$sys]} --servers $SRVSH --neo4j-sif $SIF_DIR/neo4j.sif --out $OUT/$sys.load.json \
       && persist $sys || { echo "LOAD FAILED $sys: not persisted"; cp -a $SRV/${DIRS[$sys]}/server.log $P/results/w3/$TAG/server-$sys-$SLURM_JOB_ID.log 2>/dev/null
                            tail -n 30 $SRV/${DIRS[$sys]}/server.log; $SRVSH stop $sys w3; exit 1; }
    cp -a $SRV/${DIRS[$sys]}/server.log $P/results/w3/$TAG/server-$sys-load-$SLURM_JOB_ID.log 2>/dev/null
    rm -rf ${LOCAL:-${TMPDIR:-/tmp}}/gcsv
  fi
  say digest $sys
  # loaded in this job: restart from the data directory just packed (its sha256 is recorded); otherwise restore it
  if [ "${SKIP_LOAD:-0}" = 1 ]; then restore $sys || exit 1; fi
  t0=$(date +%s); EXTCMP_SERVER_TIMEOUT_S=$CAP $SRVSH start $sys w3 && waitbolt $sys || exit 1
  echo "start (recovery) $sys $(( $(date +%s) - t0 )) s"
  t0=$(date +%s)
  $X/w3_snb_ingest.py digest --system $sys --target ${URIS[$sys]} --out $OUT/$sys.digest.json --parts ${DPARTS:-256} --workers ${DWORKERS:-12} \
    || echo "DIGEST FAILED $sys"
  echo "digest $sys $(( $(date +%s) - t0 )) s"
  for o in neo4j memgraph; do [ $o != $sys ] && [ -e $OUT/$o.digest.json ] && $X/w3_snb_ingest.py cmp $OUT/$o.digest.json $OUT/$sys.digest.json; done
  say check $sys
  pcheck $sys ${URIS[$sys]} $C/$sys-v2.jsonl
  $SRVSH stop $sys w3
done
if has gcheck; then
  say check gnomon $ENGC so $ENGSO
  (cd $OUT && sha256sum -c $GTAR.sha256) || exit 1
  rm -rf $ST; zstd -q -d -c -T$NCPU $OUT/$GTAR | tar -C ${LOCAL:-${TMPDIR:-/tmp}} -xf -
  export W3_GNOMON_BLOCKED=NONE GNOMON_MATCH_THREADS=${GNOMON_MATCH_THREADS:-32}
  rm -f $C/gnomon-$ENGC.jsonl
  (cd $ENGREPO && $PYE scripts/w3_snb_run.py run --system gnomon --target $ST --params $PAR --settings current,asof \
     --hist-db $C/w3.duckdb --out $C/gnomon-$ENGC.jsonl) | tail -3
  $PYW $P/src/scripts/w3_relabel_cap.py $C/gnomon-$ENGC.jsonl $CAP "SF$SF instant check, Gnomon $ENGC (so $ENGSO), stage prep-stores, job $SLURM_JOB_ID" | tail -2
  $X/w3_snb_run.py compare --base $C/duckdb-v2.jsonl $C/gnomon-$ENGC.jsonl --json $C/compare-gnomon-$ENGC.json > $C/compare-gnomon-$ENGC.md
  grep -c equivalent $C/compare-gnomon-$ENGC.md; grep -v "equivalent [0-9]* |$" $C/compare-gnomon-$ENGC.md | head -60
fi
if has compare; then
  say compare
  $X/w3_snb_run.py compare --base $C/duckdb-v2.jsonl $(ls $C/postgres-v2.jsonl $C/neo4j-v2.jsonl $C/memgraph-v2.jsonl 2>/dev/null) \
    --json $C/compare-v2.json > $C/compare-v2.md
  grep -c equivalent $C/compare-v2.md; grep -v "equivalent [0-9]* |$" $C/compare-v2.md | head -80
fi
say done
