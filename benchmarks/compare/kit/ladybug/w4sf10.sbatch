#!/bin/bash
# prep-ladybug: W4 SF10 (rho 0 and 10) on LadybugDB, untimed, a shared node: load from the export, read-back, digest vs the
# Neo4j and Memgraph digests, persist ladybug.tar.zst + sha256 beside the other SF10 stores, answer check (120 s cap)
# against the recorded DuckDB (SQL) answers (check-sf10/r{0,10}).
#SBATCH -N 1
#SBATCH --ntasks-per-node=16
#SBATCH -t 04:00:00
#SBATCH -J lb-w4sf10
#SBATCH -o %x-%j.out
source ${GNOMON_BENCH_ROOT}/ladybug/work/env.sh
set -uo pipefail
say() { echo "=== $* $(date -u +%FT%TZ)"; }
N=$SLURM_NTASKS_PER_NODE; export W3_LADYBUG_BUFFER_POOL_GB=${BP:-20}
O=$LB/val/w4-sf10-$SLURM_JOB_ID; mkdir -p $O; cd $REPO
say "W4 SF10 LadybugDB job $SLURM_JOB_ID on $(hostname), $N cores"
for r in ${RHOS:-r0 r10}; do
  W=${LOCAL:-${TMPDIR:-/tmp}}/w4-$r; ST=$W4/w4stores/sf10-$r; mkdir -p $W
  say "load $r"; $PYV scripts/w4_ladybug.py load --dir $W4/exports/sf10-$r --db $W/db/ladybug.lbdb --stage $W/stage --out $O/load-$r.json --threads $N --duck-mem 20GB | cut -c1-500
  say "digest $r"; $PYV scripts/w4_ladybug.py digest --db $W/db/ladybug.lbdb --out $O/digest-$r.json --procs $N
  for s in neo4j memgraph; do $PYV scripts/w3_ladybug.py cmp $O/digest-$r.json $ST/$s.digest.json | cut -c1-500; done
  ( cd $W/db && tar -cf - ladybug.lbdb* | zstd -q -T$N -3 -o $ST/ladybug.tar.zst ) && ( cd $ST && sha256sum ladybug.tar.zst > ladybug.tar.zst.sha256 )
  cp $O/load-$r.json $ST/ladybug.load.json; cp $O/digest-$r.json $ST/ladybug.digest.json
  say "answer check $r"
  $PYV scripts/w4_check.py run --system ladybug --db $W/db/ladybug.lbdb --params $W4/params/params-sf10.json --out $O/answers-ladybug-$r.jsonl --alarm 120 --label "sf10 $r"
  $PYV scripts/w4_check.py compare --base $W4/check-sf10/$r/answers-duckdbsql.jsonl $O/answers-ladybug-$r.jsonl --json $O/verdicts-$r.json
  $PYV scripts/w4_check.py compare --base $W4/check-sf10/$r/answers-gnomon-f10733cf.jsonl $O/answers-ladybug-$r.jsonl --json $O/verdicts-gnomon-$r.json
  rm -rf $W
done
say done
