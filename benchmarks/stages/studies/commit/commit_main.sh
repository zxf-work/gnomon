#!/bin/bash
# Durable commit cost, T-mgBench write stream, per operation (audit #7). Commit cost only: no T-gMark, no W6 row. Each timed load runs without strace; strace passes are separate, for fsync counts only.
source ${NEXT_STAGE_ROOT:?}/src/benchmarks/stages/studies/lib.sh
: "${OUT:?}" "${ROOT:?}"
export W6=$E/w6-aeong P6_ROOT=$E/p6-aeong W6_PY=$PYV
unset EXTCMP_NUMA_SERVER EXTCMP_NUMA_CLIENT
export EXTCMP_JOB_SAFE=1; source $KIT/lib/ports.sh
export W6_AEONG_PORT=$EXTCMP_AEONG_PORT W6_NEO4J_PORT=$EXTCMP_NEO4J_PORT W6_NEO4J_HTTP=$EXTCMP_NEO4J_HTTP_PORT
AU=bolt://127.0.0.1:$W6_AEONG_PORT; NU=bolt://127.0.0.1:$W6_NEO4J_PORT
WORK=${COMMIT_WORK:-$W6/tmgbench/m320k-d1}; SMALL=$W6/smoke/m3k-d10; NS=${COMMIT_NS:-1 16 64 256}; SYSTEMS=${COMMIT_SYSTEMS:-gnomon aeong neo4j duckdbsql}; TAG=$(basename $WORK)
need_file $WORK/eventlog.jsonl; need_file $WORK/aeong_native.jsonl; need_file $SMALL/eventlog.jsonl
O=$OUT; mkdir -p $O; export W6_SRV=$ROOT/srv W6_CEILING_S=10; mkdir -p $W6_SRV $ROOT/stores
K=$KIT/w6; D="$PYV $REPO/scripts/w6_tmgbench.py"; export W6_ENGINE_SO_SHA=$(sha256sum $REPO/gnomon/_engine*.so | cut -c1-16)
IDX=$P6_ROOT/datasets/T-mgBench/cypher_index.cypher; need_file $IDX
has() { case " $SYSTEMS " in *" $1 "*) return 0 ;; esac; return 1; }
if [ "$WORK" = "$W6/tmgbench/m320k-d1" ]; then
  grep "m320k-d1/\(eventlog.jsonl\|aeong_native.jsonl\)" $W6/tmgbench/SHA256SUMS | ( cd $W6/tmgbench && sha256sum -c ) || { echo "FROZEN LOG CHECK FAILED: stopping"; exit 2; }
fi
say host $(hostname) work=$WORK ns=[$NS] systems=[$SYSTEMS] so=$W6_ENGINE_SO_SHA kit=$(git -C $REPO rev-parse --short HEAD)
agrees() { $PYV -c "
import json,sys
s=json.load(open(sys.argv[1]))['summary']; print(sys.argv[1].split('/')[-1], s)
sys.exit(0 if all(v in ('agrees','not expressible') for v in s.values()) else 1)" "$1"; }
mgpid() { local w; w=$(cat $W6_SRV/aeong-$1/server.pid); local front=$w desc=""
  while [ -n "$front" ]; do local nxt=""; for q in $front; do nxt="$nxt $(ps -o pid= --ppid $q | tr '\n' ' ')"; done; front=$(echo $nxt); desc="$desc $front"; done
  for p in $desc; do case "$(tr '\0' ' ' < /proc/$p/cmdline 2>/dev/null)" in *memgraph-_Release*) echo $p; return;; esac; done; }
javapid() { local w; w=$(cat $W6_SRV/neo4j-$1/server.pid); local front=$w desc=""
  while [ -n "$front" ]; do local nxt=""; for q in $front; do nxt="$nxt $(ps -o pid= --ppid $q | tr '\n' ' ')"; done; front=$(echo $nxt); desc="$desc $front"; done
  for p in $desc; do case "$(tr '\0' ' ' < /proc/$p/cmdline 2>/dev/null)" in *org.neo4j*|*java*) echo $p; return;; esac; done; }
if has gnomon; then
  ST=$ROOT/stores/gnomon-replay
  say "gnomon: the re-check on C4: replay, digest, every instance against the oracle"
  $D gnomon-load --repo $REPO --work $WORK --gran op --path replay --store $ST --out $O/load-gnomon-replay.json --digest | cut -c1-500 || { echo "REPLAY FAILED"; exit 2; }
  $D check --system gnomon --work $WORK --gran op --store $ST --out $O/check-gnomon.json | cut -c1-500
  agrees $O/check-gnomon.json || { echo "GNOMON ANSWER DIFFERS FROM THE ORACLE: stopping before any commit time $(utc)"; exit 7; }
  # the writer path assigns its own transaction times, so the oracle's TT parameters (the log's times) do not apply to its store; its check is the content digest, which leaves wall clock out, against the replay's
  same_digest() { $PYV -c "
import json,sys
a=json.load(open(sys.argv[1]))['content_digest']['digest']; b=json.load(open(sys.argv[2]))['content_digest']['digest']; print('digest', sys.argv[2].split('/')[-1], b[:16], 'replay', a[:16])
sys.exit(0 if a==b else 1)" $O/load-gnomon-replay.json $O/load-gnomon-live-$1.json || { echo "GNOMON WRITER-PATH DIGEST DIFFERS FROM REPLAY $1: stopping $(utc)"; return 1; }; }
  live() {  # tag, env...: one timed writer-path load, the digest, then the store is checked or removed by the caller
    local tag=$1; shift; L=$ROOT/stores/live-$tag; [ -e $L ] && mv $L $L.old.$$
    say "gnomon writer path $tag: $*"
    env "$@" $D gnomon-load --repo $REPO --work $WORK --gran op --path live --store $L --out $O/load-gnomon-live-$tag.json --digest | cut -c1-500
  }
  live off X=1
  same_digest off || exit 7
  for n in $NS; do
    live n$n GNOMON_SMALL_COMMIT=1 GNOMON_SMALL_COMMIT_N=$n
    same_digest n$n || exit 7
  done
  say "gnomon: strace passes, counts only"
  for cfg in off n64; do
    L=$ROOT/stores/strace-$cfg; if [ $cfg = n64 ]; then export GNOMON_SMALL_COMMIT=1 GNOMON_SMALL_COMMIT_N=64; else unset GNOMON_SMALL_COMMIT GNOMON_SMALL_COMMIT_N; fi
    strace -f -c -e trace=fsync,fdatasync -o $O/strace-gnomon-$cfg.txt $D gnomon-load --repo $REPO --work $WORK --gran op --path live --store $L --out $O/load-gnomon-strace-$cfg.json | cut -c1-200
    unset GNOMON_SMALL_COMMIT GNOMON_SMALL_COMMIT_N; head -5 $O/strace-gnomon-$cfg.txt
  done
fi
if has aeong; then
  export W6_AEONG_DURABLE=1 W6_AEONG_GC=authors W6_AEONG_EXTRA=""
  export W6_SERVER_FLAGS="$($K/aeong_srv.sh flags)" W6_DURABILITY="WAL on, fsync every commit, snapshot interval 86400 s, snapshot on exit"
  tag=c; say "aeong native durable: timed load"; $K/aeong_srv.sh start $tag | tee $O/server-start-aeong.txt
  $D load --system aeong-native --uri $AU --work $WORK --gran op --index $IDX --out $O/load-aeong-native.json > $O/load-aeong-native.out 2>&1; LRC=$?
  head -c 600 $O/load-aeong-native.out; echo
  if [ $LRC = 0 ]; then $D check --system aeong-native --uri $AU --work $WORK --gran op --gc-wait 65 --out $O/check-aeong-native.json | cut -c1-400; else echo "AEONG LOAD FAILED rc=$LRC"; fi
  $K/aeong_srv.sh state $tag; $K/aeong_srv.sh stop $tag; $K/aeong_srv.sh space $tag > $O/space-aeong-native.json; tail -n 30 $W6_SRV/aeong-$tag/server.log > $O/server-log-aeong-native.txt
  tag=s; say "aeong native durable: strace pass"; $K/aeong_srv.sh start $tag | tee $O/server-start-aeong-strace.txt
  pid=$(mgpid $tag); strace -f -c -e trace=fsync,fdatasync -o $O/strace-aeong-native.txt -p $pid > /dev/null 2>&1 & STP=$!; sleep 1
  $D load --system aeong-native --uri $AU --work $WORK --gran op --index $IDX --out $O/load-aeong-native-strace.json > /dev/null 2>&1
  kill -INT $STP 2>/dev/null; wait $STP 2>/dev/null; head -5 $O/strace-aeong-native.txt
  $K/aeong_srv.sh stop $tag
fi
if has neo4j; then
  export W6_SERVER_FLAGS="heap 8g, pagecache 8g, db.transaction.timeout ${W6_CEILING_S}s" W6_DURABILITY="Neo4j defaults: transaction log forced at commit"
  tag=c; say "neo4j: timed load"; $K/neo4j_srv.sh start $tag | tee $O/server-start-neo4j.txt
  $D load --system neo4j --uri $NU --work $WORK --gran op --mode maint --out $O/load-neo4j.json > $O/load-neo4j.out 2>&1; LRC=$?
  head -c 600 $O/load-neo4j.out; echo
  if [ $LRC = 0 ]; then
    $D check --system neo4j --uri $NU --work $WORK --gran op --out $O/check-neo4j.json | cut -c1-400; agrees $O/check-neo4j.json || { echo "NEO4J ANSWER DIFFERS FROM THE ORACLE: stopping $(utc)"; $K/neo4j_srv.sh stop $tag; exit 7; }
  else echo "NEO4J LOAD FAILED rc=$LRC"; fi
  $K/neo4j_srv.sh space $tag > $O/space-neo4j.json; $K/neo4j_srv.sh stop $tag
  tag=s; say "neo4j: strace pass on the small log, counts only ($SMALL)"; $K/neo4j_srv.sh start $tag | tee $O/server-start-neo4j-strace.txt
  pid=$(javapid $tag); echo "java pid $pid"; strace -f -c -e trace=fsync,fdatasync -o $O/strace-neo4j-small.txt -p $pid > /dev/null 2>&1 & STP=$!; sleep 1
  $D load --system neo4j --uri $NU --work $SMALL --gran op --mode maint --out $O/load-neo4j-small-strace.json > /dev/null 2>&1
  kill -INT $STP 2>/dev/null; wait $STP 2>/dev/null; head -5 $O/strace-neo4j-small.txt; $K/neo4j_srv.sh stop $tag
fi
if has duckdbsql; then
  export W6_SERVER_FLAGS="threads 16, memory_limit 24GB" W6_DURABILITY="file database, WAL, commit syncs the WAL"
  DB=$ROOT/ddb.duckdb
  say "duckdbsql: timed load"
  $D load --system duckdbsql --db $DB --threads 16 --memory-limit 24GB --work $WORK --gran op --mode maint --out $O/load-duckdbsql.json > $O/load-duckdbsql.out 2>&1; LRC=$?
  head -c 600 $O/load-duckdbsql.out; echo
  if [ $LRC = 0 ]; then
    $D check --system duckdbsql --db $DB --threads 16 --memory-limit 24GB --work $WORK --gran op --out $O/check-duckdbsql.json | cut -c1-400; agrees $O/check-duckdbsql.json || { echo "DUCKDB ANSWER DIFFERS FROM THE ORACLE: stopping $(utc)"; exit 7; }
  else echo "DUCKDB LOAD FAILED rc=$LRC"; fi
  DB=$ROOT/ddb-s.duckdb; say "duckdbsql: strace pass on the small log, counts only"
  strace -f -c -e trace=fsync,fdatasync -o $O/strace-duckdbsql-small.txt $D load --system duckdbsql --db $DB --threads 16 --memory-limit 24GB --work $SMALL --gran op --mode maint --out $O/load-duckdbsql-small-strace.json > /dev/null 2>&1; head -5 $O/strace-duckdbsql-small.txt
fi
$PYV $REPO/$STG/commit/commit_report.py --dir $O --out $O/report.md && cat $O/report.md
say "commit cost body done"
