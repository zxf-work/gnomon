# SF0.1 W3 answer check on a dev allocation: ladybug at the given settings for the given queries (store unpacked if missing)
source ${GNOMON_BENCH_ROOT}/ladybug/work/env.sh
cd $REPO; W=/local/lb/sf01; C3=$P3/check/SF0.1-h1-r10; FS=$P3/w3stores/SF0.1-h1-r10/full
if [ ! -e $W/db/ladybug.lbdb ]; then mkdir -p $W/db; ( cd $FS && sha256sum -c ladybug.tar.zst.sha256 ) && zstd -q -d -c $FS/ladybug.tar.zst | tar -C $W/db -xf -; fi
export W3_CAP_S=${W3_CAP_S:-120} W3_LADYBUG_BUFFER_POOL_GB=${BP:-6}
OUT=$W/ans-${TAG:-x}.jsonl; rm -f $OUT
$PYV scripts/w3_snb_run.py run --system ladybug --target ${DB:-$W/db/ladybug.lbdb} --params $P3/params/W3-SF0.1.params.jsonl --settings ${SETTINGS:-current,asof} ${QUERIES:+--queries $QUERIES} --out $OUT --hist-db $C3/hist/hist.duckdb --keep 1000 2>&1 | tail -2
$PYV $LB/work/summ.py $C3/duckdb.jsonl $OUT
