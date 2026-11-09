#!/bin/bash
#SBATCH -J w3-msgparams
#SBATCH -N 1
#SBATCH --ntasks-per-node=16
#SBATCH -t 01:00:00
#SBATCH -o msgparams-%j.out
# Guarded parameter regeneration (D-W3-8): only the ALLOWED queries may change. SPECS: sf/n/out/nonempty/nopath
# (colon lists), e.g. 1/6/W3-SF1.params.jsonl/IC3:BI16:BI20/IC13:IC14.
set -euo pipefail
P=${GNOMON_BENCH_ROOT}/w3-snb; source $P/site.env; cd $EXTCMP_REPO; source .venv/bin/activate
ALLOWED=${ALLOWED:-IS4:IS5:IS6:IS7:TLDBC-IS4:TLDBC-IS5:TLDBC-IS6}; SUPTAG=${SUPTAG:-msg-iv2}
gen() {   # <sf> <n> <out> [nonempty] [nopath]
  local sf=$1 n=$2 out=$P/params/$3 ne=$(echo ${4:-} | tr : ,) np=$(echo ${5:-} | tr : ,)
  local db=$P/check/SF$sf-h1-r$([ $sf = 1 ] && echo 1 || echo 10)/w3.duckdb
  python scripts/w3_snb_params.py --ldbc $P/params/ldbc-sf$sf --base $P/base/sf$sf-h1 \
    --corrections $P/eventlogs/W3-SF$sf-h1-r0p02.eventlog.jsonl.corrections.parquet --n $n --out $out.new \
    ${ne:+--nonempty $ne} ${np:+--nopath $np} $([ -n "$ne$np" ] && echo --nonempty-db $db) > /dev/null
  python -I $EXTCMP_REPO/scripts/w3_params_guard.py $out $out.new $ALLOWED
  cp -p $out $P/params/superseded/$(basename $out).$SUPTAG; cp -p $out.report.json $P/params/superseded/$(basename $out).report.json.$SUPTAG
  mv $out.new $out; mv $out.new.report.json $out.report.json
  python -c "import json; r=json.load(open('$out.report.json')); print('$3', {q: r['strata'][q] for q in '$ALLOWED'.split(':')}, r['nonempty_scanned'], r['nonempty_found'])"
}
for spec in $SPECS; do IFS=/ read -r sf n out ne np <<< "$spec"; gen $sf $n $out "$ne" "$np"; done
cd $P/params && sha256sum W3-*.params.jsonl > SHA256SUMS && cat SHA256SUMS
