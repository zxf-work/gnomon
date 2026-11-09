#!/bin/bash
# prep-ladybug: W3 full-version LadybugDB store (query stages), untimed, a shared node.
#   SF=SF1|SF10 (env). SF1: the graph CSVs rewritten from the history tables by w3_snb_ingest.write_csvs (the Neo4j /
#   Memgraph bulk input); SF10: the persisted graph-csv.tar.zst. Load (COPY FROM), read-back, digest, compare with the
#   Neo4j and Memgraph digests of the same log, persist ladybug.tar.zst + sha256 beside the other full stores.
#SBATCH -N 1
#SBATCH --ntasks-per-node=16
#SBATCH -t 03:00:00
#SBATCH -J lb-w3load
#SBATCH -o %x-%j.out
source ${GNOMON_BENCH_ROOT}/ladybug/work/env.sh
set -uo pipefail
say() { echo "=== $* $(date -u +%FT%TZ)"; }
SF=${SF:-SF1}; N=$SLURM_NTASKS_PER_NODE
case $SF in SF1) LOG=SF1-h1-r1; H=$P3/check/SF1-h1-r1/hist ;; SF10) LOG=SF10-h1-r1; H=$P3/check/SF10-h1-r1-f10733cf/hist ;; esac
FS=$P3/w3stores/$LOG/full; W=${LOCAL:-${TMPDIR:-/tmp}}/lbw3; mkdir -p $W; cd $REPO
export W3_LADYBUG_BUFFER_POOL_GB=${BP:-24}
say "W3 $SF LadybugDB load, job $SLURM_JOB_ID on $(hostname), $N cores, kit $(git rev-parse --short HEAD) (+ worktree edits)"
if [ $SF = SF10 ]; then
  ( cd $FS && sha256sum -c graph-csv.tar.zst.sha256 ) || { echo "CSV CHECKSUM MISMATCH"; exit 1; }
  mkdir -p $W/csv; zstd -q -d -c $FS/graph-csv.tar.zst | tar -C $W/csv -xf -; ls $W/csv | head; CSV=$(dirname $(ls $W/csv/*/n_Person.csv $W/csv/n_Person.csv 2>/dev/null | head -1))
else
  say "prep-csv from $H"; $PYV scripts/w3_ladybug.py prep-csv --hist $H --out $W/csv | tail -1; CSV=$W/csv
fi
say "load from $CSV"; $PYV scripts/w3_ladybug.py load --csv $CSV --db $W/db/ladybug.lbdb --stage $W/stage --out $W/load.json --threads $N --duck-mem ${DM:-20GB}; rc=$?
rm -rf $W/csv
[ $rc = 0 ] || { echo "LOAD NOT VERIFIED rc=$rc"; cp $W/load.json $LB/logs/w3load-$SF-$SLURM_JOB_ID.json; exit 1; }
say "digest"; $PYV scripts/w3_ladybug.py digest --db $W/db/ladybug.lbdb --out $W/ladybug.digest.json --procs $N
for s in neo4j memgraph; do $PYV scripts/w3_ladybug.py cmp $W/ladybug.digest.json $FS/$s.digest.json | cut -c1-600; done
say "persist"
( cd $W/db && tar -cf - ladybug.lbdb* | zstd -q -T$N -3 -o $FS/ladybug.tar.zst ) && ( cd $FS && sha256sum ladybug.tar.zst > ladybug.tar.zst.sha256 )
cp $W/load.json $FS/ladybug.load.json; cp $W/ladybug.digest.json $FS/ladybug.digest.json
ls -la $FS/ladybug.*; say done
