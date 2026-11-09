#!/bin/bash
#SBATCH -J w3-check
#SBATCH -N 1
#SBATCH --ntasks-per-node=16
#SBATCH -t 04:00:00
#SBATCH -o check-%j.out
# W3 answer check at one (SF, rho): replay -> export rows -> history tables -> DuckDB; Gnomon vs DuckDB.
set -uo pipefail
P=${GNOMON_BENCH_ROOT}/w3-snb
source $P/site.env; cd $EXTCMP_REPO; source .venv/bin/activate
export W3_EXT=$P/ext
SF=${SF:-0.1}; R=${R:-10}; TAG=SF$SF-h1-r$R; LOG=$P/eventlogs/W3-$TAG.eventlog.jsonl
C=$P/check/$TAG; mkdir -p $C
say() { echo "=== $* $(date -u +%FT%TZ)"; }
ST=$P/stores/W3-$TAG
if [ ! -e $ST/.done ]; then say replay; rm -rf $ST; mkdir -p $P/stores
  python -c "
import sys, pathlib, time; sys.path.insert(0, 'scripts'); import eval_harness as H
t=time.time(); H.load_store(pathlib.Path('$ST'), 'native', pathlib.Path('$LOG')); print('replay_s', round(time.time()-t,1))" && touch $ST/.done; fi
[ -e $C/rows/export.json ] || { say export; python scripts/w3_snb_sql.py export --store $ST --out $C/rows; }
[ -e $C/hist/hist.json ] || { say hist; python scripts/w3_snb_sql.py hist --rows $C/rows --out $C/hist | cut -c1-1500; }
[ -e $C/w3.duckdb ] || { say duckdb-load; python scripts/w3_snb_run.py duckdb-load --hist $C/hist --db $C/w3.duckdb; }
PAR=$P/params/W3-SF$SF.params.jsonl
[ -e $PAR ] || { say params; python scripts/w3_snb_params.py --ldbc $P/params/ldbc-sf$SF --base $P/base/sf$SF-h1 \
   --corrections $P/eventlogs/W3-SF$SF-h1-r0p02.eventlog.jsonl.corrections.parquet --n ${N:-6} --out $PAR; }
Q=${QUERIES:-}
export W3_GNOMON_BLOCKED=${W3_GNOMON_BLOCKED:-IC14,BI15,BI19} W3_GNOMON_BLOCKED_WHY="S21 (the reply-pair weight pattern exceeds 124 GB at SF1)"
for sys in ${CHECK_SYSTEMS:-gnomon duckdb}; do
  tgt=$ST; [ $sys = duckdb ] && tgt=$C/w3.duckdb
  rm -f $C/$sys.jsonl; say run $sys
  python scripts/w3_snb_run.py run --system $sys --target $tgt --params $PAR ${Q:+--queries $Q} --settings current,asof,window --hist-db $C/w3.duckdb --out $C/$sys.jsonl
done
say compare
python scripts/w3_snb_run.py compare --base $C/gnomon.jsonl $C/duckdb.jsonl --json $C/compare.json > $C/compare.md; cat $C/compare.md
say done
