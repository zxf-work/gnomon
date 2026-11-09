#!/bin/bash
# concurrently one Gnomon process, FSX_ND DuckDB (SQL) workers and FSX_NP PostgreSQL workers (fsx_check.py), then the comparison.
# Points in FSX_POINTS order; FSX_DEADLINE (epoch s) bounds every instance's limit. Exit 7 if differences.txt is not empty.
source ${NEXT_STAGE_ROOT:?}/src/benchmarks/stages/studies/lib.sh
: "${OUT:?}" "${FSX_DEADLINE:?}"
TL=$REPO/$STG/fullsets-x; MD=$NEXT_STAGE_ROOT/closure
RESL=${LOCAL:-${TMPDIR:-/tmp}}/fsxres; mkdir -p $RESL; : > $RESL/differences.txt
need_file $MD/SHA256SUMS; ( cd $MD && sha256sum -c --quiet SHA256SUMS ) || { echo "CLOSURE LISTS DO NOT MATCH SHA256SUMS: stopping"; exit 2; }
$PYV $TOOLS/ing_rss.py $RESL/rss.jsonl 5 &
RSSP=$!; trap 'kill -TERM $RSSP 2>/dev/null' EXIT
PER=${FSX_PER:-1800}
export EXTCMP_SERVER_LIMIT_S=$((PER + 60)) EXTCMP_SERVER_TIMEOUT_S=3660 EXTCMP_CALL_CEILING_S=3600
export EXTCMP_DUCKDBSQL_THREADS=${FSX_DTHREADS:-20} EXTCMP_DUCKDBSQL_MEMORY_LIMIT=${FSX_DMEM:-24GB}
POINTS=${FSX_POINTS:-w2-K10M-d0 w2-K10M-d10 w1-K10M w2-K10M}; ND=${FSX_ND:-3}; NP=${FSX_NP:-8}
if [ -n "${FSX_PRE_CLOSURE:-}" ]; then   # the comparison of an earlier run's kept answers (its records and pickles), before this run
  say "compare the kept answers of $FSX_PRE_DIR"
  $PYV $TL/fsx_check.py compare --closure $FSX_PRE_CLOSURE --out $FSX_PRE_DIR --cells $RESL/cells-${FSX_PRE_NAME:-pre}.jsonl --diff $RESL/differences.txt || echo "PRE-COMPARE FAILED" | tee -a $RESL/failed.txt
  cp -au $RESL/. $OUT/
fi
echo "per-instance limit $PER s; DuckDB workers $ND x $EXTCMP_DUCKDBSQL_THREADS threads, memory_limit $EXTCMP_DUCKDBSQL_MEMORY_LIMIT; PostgreSQL workers $NP, statement_timeout <= $EXTCMP_SERVER_LIMIT_S s; deadline $(date -u -d @$FSX_DEADLINE +%FT%TZ)"
for p in $POINTS; do
  wl=${p%%-*}; tag=${p#*-}; setup_root ${LOCAL:-${TMPDIR:-/tmp}}/fsx-$p; export TMPDIR=$ROOT/scratch; source $KIT/lib/env.sh
  say "restore $p"
  restore_tar $E/confirm/$wl/stores/$tag/native-$tag.tar.zst $STORES
  restore_tar $E/final/$wl/stores/$tag/duckdbsql-$tag.tar.zst $STORES
  restore_tar $E/final/$wl/stores/$tag/postgres-$tag.tar.zst $SRV
  $KIT/lib/servers.sh start postgres $tag | tail -1
  $PY wait --system postgres --timeout 600 || { echo "POSTGRES WOULD NOT START $p: stopping"; exit 8; }
  C=$MD/closure-$p${FSX_SUFFIX:-}.json; D=$RESL/$p${FSX_SUFFIX:-}; mkdir -p $D
  common="--closure $C --tag $tag --root $EXTCMP_ROOT --ref $STORES/native-$tag --out $D --per $PER --until $FSX_DEADLINE ${FSX_ONLY:+--only $FSX_ONLY} ${FSX_PER_SYS:+--per-sys $FSX_PER_SYS} ${FSX_ORDER:+--order $FSX_ORDER}"
  say "check $p"
  pids=""
  $PYV $TL/fsx_check.py gnomon $common > $D/gnomon.log 2>&1 & pids="$pids $!"
  for i in $(seq 1 $ND); do mkdir -p $ROOT/scratch/d$i; TMPDIR=$ROOT/scratch/d$i $PYV $TL/fsx_check.py system --system duckdbsql --worker $i $common > $D/duckdbsql-$i.log 2>&1 & pids="$pids $!"; done
  for i in $(seq 1 $NP); do mkdir -p $ROOT/scratch/p$i; TMPDIR=$ROOT/scratch/p$i $PYV $TL/fsx_check.py system --system postgres --worker $i $common > $D/postgres-$i.log 2>&1 & pids="$pids $!"; done
  rc=0; for x in $pids; do wait $x || rc=1; done
  [ $rc = 0 ] || { echo "A CHECK PROCESS FAILED $p"; echo "$p" >> $RESL/failed.txt; tail -3 $D/*.log; }
  $PYV $TL/fsx_check.py compare --closure $C --out $D --cells $RESL/cells-$p${FSX_SUFFIX:-}.jsonl --diff $RESL/differences.txt ${FSX_ONLY:+--only $FSX_ONLY} || echo "COMPARE FAILED $p" | tee -a $RESL/failed.txt
  $KIT/lib/servers.sh stop postgres $tag | tail -1
  find $D -name "*.pkl" -delete
  rm -rf $STORES $SRV
  cp -au $RESL/. $OUT/
  say "point $p${FSX_SUFFIX:-} done; differences so far $(wc -l < $RESL/differences.txt)"
done
kill -TERM $RSSP 2>/dev/null
$PYV $TL/fsx_table.py $RESL $MD > $RESL/closure.md || echo "table failed"; cat $RESL/closure.md
cp -au $RESL/. $OUT/
[ -s $RESL/differences.txt ] && { echo "DIFFERENCES: see differences.txt"; exit 7; }
[ -s $RESL/failed.txt ] && exit 8
say "closure body done"
