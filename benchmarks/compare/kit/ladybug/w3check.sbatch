#!/bin/bash
# prep-ladybug: W3 untimed answer check on LadybugDB (a shared node, the 120 s check cap, the skip-after-2-caps budget rule),
# against the recorded DuckDB (SQL) answers and Gnomon's record. SF=SF1|SF10, SETTINGS (env).
#SBATCH -N 1
#SBATCH --ntasks-per-node=16
#SBATCH -t 08:00:00
#SBATCH -J lb-w3check
#SBATCH -o %x-%j.out
source ${GNOMON_BENCH_ROOT}/ladybug/work/env.sh
set -uo pipefail
say() { echo "=== $* $(date -u +%FT%TZ)"; }
SF=${SF:-SF1}; SETTINGS=${SETTINGS:-current,asof,window}
case $SF in
  SF1) LOG=SF1-h1-r1; C=$P3/check/SF1-h1-r1; BASE=$C/duckdb.jsonl; G=$C/gnomon-f77d607a.jsonl; PAR=$P3/params/W3-SF1.params.jsonl ;;
  SF10) LOG=SF10-h1-r1; C=$P3/check/SF10-h1-r1-f10733cf; BASE=$C/duckdb-v2.jsonl; G=$C/gnomon-f10733cf.jsonl; PAR=$P3/params/W3-SF10.params.jsonl ;;
esac
FS=$P3/w3stores/$LOG/full; W=${LOCAL:-${TMPDIR:-/tmp}}/lbchk; mkdir -p $W/db; O=$LB/val/w3-$SF-$SLURM_JOB_ID; mkdir -p $O; cd $REPO
export W3_CAP_S=${CAP:-120} W3_LADYBUG_BUFFER_POOL_GB=${BP:-20}
say "W3 $SF LadybugDB answer check job $SLURM_JOB_ID on $(hostname), settings $SETTINGS, params $(sha256sum $PAR | cut -c1-12)"
( cd $FS && sha256sum -c ladybug.tar.zst.sha256 ) || { echo "STORE CHECKSUM MISMATCH"; exit 1; }
zstd -q -d -c $FS/ladybug.tar.zst | tar -C $W/db -xf -
if [ "${DIGEST:-0}" = 1 ]; then   # the store's digest
  say "digest"; W3_LADYBUG_DIGEST_BP_GB=2 W3_LADYBUG_DIGEST_CHUNK=${CHUNK:-2000000} \
    $PYV scripts/w3_ladybug.py digest --db $W/db/ladybug.lbdb --out $O/ladybug.digest.json --procs ${DPROCS:-6}
  for s in neo4j memgraph; do $PYV scripts/w3_ladybug.py cmp $O/ladybug.digest.json $FS/$s.digest.json | tee -a $O/digest-cmp.txt | cut -c1-600; done
  [ -e $FS/ladybug.digest.json ] || cp $O/ladybug.digest.json $FS/ladybug.digest.json
fi
$PYV scripts/w3_ladybug_check.py --target $W/db/ladybug.lbdb --params $PAR --settings $SETTINGS ${QUERIES:+--queries $QUERIES} --out $O/ladybug.jsonl > $O/run.log 2>&1
tail -3 $O/run.log
say "vs DuckDB (SQL)"; $PYV $LB/work/summ.py $BASE $O/ladybug.jsonl | tee $O/vs-duckdb.txt | cut -c1-400
say "vs Gnomon"; $PYV $LB/work/summ.py $G $O/ladybug.jsonl > $O/vs-gnomon.txt; tail -1 $O/vs-gnomon.txt
say done
