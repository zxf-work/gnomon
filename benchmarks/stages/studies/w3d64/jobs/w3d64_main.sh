#!/bin/bash
export W3_STAGE_ROOT=${NEXT_STAGE_ROOT:?}
source $NEXT_STAGE_ROOT/src/benchmarks/stages/studies/lib.sh
: "${OUT:?}"
MY=$REPO/$STG/w3d64; RSSTOOL=$REPO/$STG/tools/cov_rss.py
source $REPO/benchmarks/stages/w3/jobs/w3_lib.sh
pt_config SF10 || exit 2
setup_root ${LOCAL:-${TMPDIR:-/tmp}}/w3d64
RESL=${LOCAL:-${TMPDIR:-/tmp}}/w3d64res; mkdir -p $RESL
CD=$NEXT_STAGE_ROOT/${W3D64_SMOKE:+smoke-}cells
need_file $CD/SHA256SUMS; ( cd $CD && sha256sum -c --quiet SHA256SUMS ) || { echo "CELL FILES DO NOT MATCH SHA256SUMS: stopping"; exit 2; }
( cd $PARD && sha256sum -c SHA256SUMS --ignore-missing > /dev/null ) || { echo "W3 parameter files do not match SHA256SUMS: stopping"; exit 2; }
PINSAMP=""; numactl --cpunodebind=1 true 2>/dev/null && PINSAMP="numactl --cpunodebind=1"
COV_RSS_PSS_EVERY=0 $PINSAMP $PYV $RSSTOOL $RESL/rss-samples.jsonl 2 &
RSSP=$!; trap 'kill -TERM $RSSP 2>/dev/null' EXIT
need_file $HIST; HISTL=${LOCAL:-${TMPDIR:-/tmp}}/w3hist.duckdb; cp $HIST $HISTL || exit 2
restore_sys duckdb || { echo "RESTORE FAILED duckdb: stopping"; exit 2; }
PYC="${EXTCMP_NUMA_CLIENT:+$EXTCMP_NUMA_CLIENT }$PYV"
say "DuckDB's own default memory_limit on this node, with and without the client binding (the W4 driver sets none)"
for pre in "" "$EXTCMP_NUMA_CLIENT"; do $pre $PYV -c "import duckdb,sys; c=duckdb.connect(':memory:'); print('binding [' + sys.argv[1] + '] duckdb', duckdb.__version__, 'default memory_limit', c.execute(\"select current_setting('memory_limit')\").fetchone()[0], 'threads', c.execute(\"select current_setting('threads')\").fetchone()[0])" "$pre" | tee -a $RESL/duckdb-default-limit.txt; done
TR="$PYC $TOOLS/w3_timed.py run"
INST=0,1,2,3,4,5,6,7
blk() {   # name threads mem exclude-file
  local n=$1 th=$2 mem=$3 ex=$4 o=$RESL/$1
  mkdir -p $o; say "block $n threads $th memory_limit $mem"
  env W3_DUCKDB_THREADS=$th W3_DUCKDB_MEM=$mem EXTCMP_ENGINE_TIME=0 $TR --system duckdb --target ${LOCAL:-${TMPDIR:-/tmp}}/w3-duckdb.duckdb --params $PARAMS --cells "$(cells_of_block ALL)" --settings current,asof,window \
      --instances $INST --hist-db $HISTL --rotation $n --exclude $ex --excl-system duckdb --out $o/ALL-duckdb.json --jsonl $o/ALL-duckdb-calls.jsonl --label "w3d64 SF10 $n threads $th mem $mem" 2>&1 | tail -n 3
  [ "${PIPESTATUS[0]}" = 0 ] && [ -s $o/ALL-duckdb.json ] || echo "BLOCK FAILED $n" | tee -a $RESL/failed-blocks.txt
  cp -au $RESL/. $OUT/
  $PYV $MY/tools/w3d64_table.py --res $RESL --old $E/final/w3/results/timed-J40/rot1 --cells $CD/selected.json --out $RESL/report.md --hashes-only || { echo "AN ANSWER DIFFERS FROM THE RECORDED ONE, block $n: see report.md"; cp -au $RESL/. $OUT/; exit 7; }
}
blk C64 64 20GB $CD/exclude-control.json
blk F64 64 64GB $CD/exclude-selected.json
blk F32 32 64GB $CD/exclude-selected.json
blk C32 32 20GB $CD/exclude-control.json
kill -TERM $RSSP 2>/dev/null
$PYV $TOOLS/w3_timed.py audit-dir $RESL | tee $RESL/censor-once-audit.txt
$PYV $MY/tools/w3d64_table.py --res $RESL --old $E/final/w3/results/timed-J40/rot1 --cells $CD/selected.json --out $RESL/report.md; rc=$?
cat $RESL/report.md | head -60
cp -au $RESL/. $OUT/
[ -s $RESL/failed-blocks.txt ] && exit 8
[ $rc = 0 ] || exit 7
say "w3d64 body done"
