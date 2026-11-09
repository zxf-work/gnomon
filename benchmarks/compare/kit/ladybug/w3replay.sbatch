#!/bin/bash
# prep-ladybug: the W3 ingest-study loader on LadybugDB (bulk snapshot through COPY FROM, then one explicit LadybugDB
# transaction per W3 transaction time), validated on the SF0.1 prefix (7 days + their corrections) against the
# expected state at L; fsync / fdatasync calls counted by strace (the WAL's sync at commit). Untimed, a shared node.
#SBATCH -N 1
#SBATCH --ntasks-per-node=8
#SBATCH -t 01:30:00
#SBATCH -J lb-w3replay
#SBATCH -o %x-%j.out
source ${GNOMON_BENCH_ROOT}/ladybug/work/env.sh
set -u
W=${LOCAL:-${TMPDIR:-/tmp}}/replay; rm -rf $W; mkdir -p $W; cd $REPO; O=$LB/val/w3-replay-$SLURM_JOB_ID; mkdir -p $O
export W3_LADYBUG_BUFFER_POOL_GB=${BP:-8}
echo "=== W3 SF0.1 prefix replay on LadybugDB, job $SLURM_JOB_ID on $(hostname) $(date -u +%FT%TZ)"
strace -f -c -e trace=fsync,fdatasync,sync_file_range,msync -o $O/strace.txt \
  $PYV scripts/w3_ladybug.py replay --hist $P3/check/SF0.1-h1-r10/hist --base-meta $P3/base/sf0.1-h1/meta.json \
    --db $W/db/ladybug.lbdb --stage $W/stage --out $O/replay.json --threads 8 --duck-mem 6GB --prefix-days ${DAYS:-7} 2>&1 | tail -3
echo "=== strace summary (fsync-family calls of the whole run, bulk phase included)"; cat $O/strace.txt
$PYV - $O/replay.json <<'PY'
import json, sys
r = json.load(open(sys.argv[1]))
print({k: r.get(k) for k in ("transactions", "prepare_s", "stage_s", "bulk_s", "bulk_rows", "transactions_s", "inserted", "closed",
                             "verified", "final_checkpoint_s", "bytes", "settings", "by_kind")})
print("first transactions (tt, kind, inserted, closed, s):", r["per_tx"][:4])
print("parts not equal to the expected state at L:", {k: v for k, v in r["counts"].items() if v["db"] != v["input"]})
PY
echo "=== done $(date -u +%FT%TZ)"
