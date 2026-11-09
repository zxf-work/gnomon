#!/bin/bash
export W3_STAGE_ROOT=${NEXT_STAGE_ROOT:?}
source $NEXT_STAGE_ROOT/src/benchmarks/stages/studies/lib.sh
: "${OUT:?}" "${MEM_WL:?}"
MY=$REPO/$STG/mem; RSSTOOL=$REPO/$STG/tools/cov_rss.py; REPOSTG=$REPO/$STG
RESL=${LOCAL:-${TMPDIR:-/tmp}}/memres; mkdir -p $RESL
CD=$NEXT_STAGE_ROOT/${MEM_SMOKE:+smoke-}cells
need_file $CD/SHA256SUMS; ( cd $CD && sha256sum -c --quiet SHA256SUMS ) || { echo "CELL FILES DO NOT MATCH SHA256SUMS: stopping"; exit 2; }
export COV_MARK_FILE=$RESL/marks.txt
mk() { echo "$(date +%s.%3N) $$ $*" >> $COV_MARK_FILE; }
PINSAMP=""; numactl --cpunodebind=1 true 2>/dev/null && PINSAMP="numactl --cpunodebind=1"
start_sampler() { COV_RSS_PSS_EVERY=10 $PINSAMP $PYV $RSSTOOL $RESL/rss-samples.jsonl 2 & RSSP=$!; trap 'kill -TERM $RSSP 2>/dev/null' EXIT; }
fail() { echo "$*" | tee -a $RESL/failed-blocks.txt; }
# ---- the kit's parameter-file path, one server start per system
p1_systems() {   # tag size parameter-file family-list: every family of every system; a server stays up across its families
  local tag=$1 size=$2 pf=$3 fams=$4 s fam rc
  for s in $SYSTEMS; do
    mk sys_start $tag $s
    case $s in postgres|neo4j|memgraph)
      $KIT/lib/servers.sh start $s $tag | tail -1
      $PY wait --system $s --timeout 3600 || { fail "SERVER $s would not start $tag"; $KIT/lib/servers.sh stop $s $tag; mk sys_end $tag $s; continue; }
      export EXTCMP_CONTAINER=extcmp-$s-$tag EXTCMP_STATE_CMD="$KIT/lib/servers.sh state $s $tag" EXTCMP_LOGS_CMD="$KIT/lib/servers.sh logs $s $tag 40" ;;
    *) unset EXTCMP_CONTAINER EXTCMP_STATE_CMD EXTCMP_LOGS_CMD ;; esac
    ref=$STORES/native-$tag
    for fam in $fams; do
      out=$RES/$tag/rotM; mkdir -p $out
      EXTCMP_EXCLUDE=$CD/$MEM_WL-exclude-$tag.json EXTCMP_PARAMS=$pf EXTCMP_P1_REPS=1 timeout -k 60 3000 $PY query --system $s --log $LOGF --ref $ref --workdir $EXTCMP_ROOT --size-tag $tag --mode timed --family $fam --answers $out/answers-$s --json $out/$fam-$s.json --params $pf; rc=$?
      [ $rc = 0 ] || fail "BLOCK FAILED $tag $fam $s rc=$rc"
      cp -au $RESL/. $OUT/ 2>/dev/null
    done
    case $s in postgres|neo4j|memgraph) $KIT/lib/servers.sh stop $s $tag | tail -1 ;; esac
    mk sys_end $tag $s
  done
}
case $MEM_WL in
w2)
  setup_root ${LOCAL:-${TMPDIR:-/tmp}}/memw2; export TMPDIR=$ROOT/scratch; source $KIT/lib/env.sh; RES=$RESL; proto_p1
  start_sampler
  SYSTEMS=${MEM_SYSTEMS:-native duckdbsql postgres neo4j memgraph}; export EXTCMP_DUCKDBSQL_THREADS=64 EXTCMP_MG_MEMORY_LIMIT_MB=${EXTCMP_MG_MEMORY_LIMIT_MB:-40960}
  ALLSYS=$SYSTEMS
  for tag in ${MEM_POINTS:-K10M K10M-d10}; do
    SYSTEMS=$ALLSYS
    if [ $tag = K10M-d10 ]; then SYSTEMS=${SYSTEMS//neo4j/}; SYSTEMS=${SYSTEMS//memgraph/}; fi   # the recorded W2 K10M-d10 run has no answered Neo4j or Memgraph cell
    case $tag in K1M) n=synth-1000000; size=1000000; want=powerlaw-1000000; fams="${MEM_FAMS:-T N}"; pf=$CD/w2-params-K1M.json; unset EXTCMP_BELIEFQ_INDEXES EXTCMP_VERIFY_FAMILY ;;
      K10M) n=synth-10000000; size=10000000; want=powerlaw-10000000; fams="${MEM_FAMS:-P T S W D N R M}"; pf=$E/studies/abl/params-w2/params-K10M.json; unset EXTCMP_BELIEFQ_INDEXES EXTCMP_VERIFY_FAMILY ;;
      K10M-d10) n=K10M-d10; size=10000000; want=W2-10M-d10; fams=B; pf=$E/studies/abl/params-w2/params-K10M-d10.json; export EXTCMP_BELIEFQ_INDEXES=1 ;;
    esac
    cp -a $E/w2-powerlaw/check1m/campaign/eventlogs/$n.eventlog.* $LOGS/; LOGF=$LOGS/$n.eventlog.jsonl
    w=$(awk -v f="$want.eventlog.jsonl" '$2 == f {print $1}' $E/w2-powerlaw/eventlogs/SHA256SUMS); g=$(sha256sum $LOGF | cut -d' ' -f1); [ -n "$w" ] && [ "$w" = "$g" ] || { echo "FROZEN LOG CHECK FAILED $tag: stopping"; exit 2; }
    say "restore $tag"
    restore_tar $E/confirm/w2/stores/$tag/native-$tag.tar.zst $STORES; need_dir $STORES/native-$tag
    for s in $SYSTEMS; do case $s in native) ;; duckdbsql) restore_tar $E/final/w2/stores/$tag/$s-$tag.tar.zst $STORES ;; *) restore_tar $E/final/w2/stores/$tag/$s-$tag.tar.zst $SRV ;; esac; done
    say "pass $tag"; p1_systems $tag $size $pf "$fams"
  done ;;
w1)
  # kept store: Gnomon (C4 store confirm/w1), DuckDB (SQL), duckdb, ClickHouse, PostgreSQL (final/w1/stores). Same short protocol and sampler
  # as W2; p1_systems_w1 is p1_systems with ClickHouse started as a server (p1_systems itself is unchanged). Untimed.
  setup_root ${LOCAL:-${TMPDIR:-/tmp}}/memw1; export TMPDIR=$ROOT/scratch; source $KIT/lib/env.sh; RES=$RESL; proto_p1
  start_sampler
  SYSTEMS=${MEM_SYSTEMS:-native duckdbsql duckdb clickhouse postgres}; export EXTCMP_DUCKDBSQL_THREADS=64
  tag=K10M; n=synth-10000000; size=10000000; fams="${MEM_FAMS:-P T S W D N R M}"; pf=$E/final/w1/params-single/params-$tag.json
  ( cd $E/final/w1/params-single && sha256sum -c --quiet --ignore-missing SHA256SUMS ) || { echo "PARAMETER FILE CHECK FAILED: stopping"; exit 2; }
  unset EXTCMP_BELIEFQ_INDEXES EXTCMP_VERIFY_FAMILY
  cp -a $E/v3/eventlogs/$n.eventlog.* $LOGS/; LOGF=$LOGS/$n.eventlog.jsonl
  w=$(awk -v f="$n.eventlog.jsonl" '$2 == f {print $1}' $E/v3/eventlogs/SHA256SUMS); g=$(sha256sum $LOGF | cut -d' ' -f1); [ -n "$w" ] && [ "$w" = "$g" ] || { echo "FROZEN LOG CHECK FAILED $tag: stopping"; exit 2; }
  say "restore $tag"
  for s in $SYSTEMS; do
    case $s in native) a=$E/confirm/w1/stores/$tag/native-$tag.tar.zst; d=$STORES ;; duckdbsql|duckdb) a=$E/final/w1/stores/$tag/$s-$tag.tar.zst; d=$STORES ;; *) a=$E/final/w1/stores/$tag/$s-$tag.tar.zst; d=$SRV ;; esac
    restore_tar $a $d
    $PYV -c "import json,sys; r=json.load(open(sys.argv[1])); m=r.get('manifest',{}); print('provenance', sys.argv[2], 'sha256', open(sys.argv[3]).read().split()[0][:16], 'load commit', m.get('commit'), 'host', m.get('hostname'), 'finished', r.get('host_after',{}).get('utc'))" $(dirname $a)/load-$s.json $a $a.sha256
  done
  need_dir $STORES/native-$tag
  p1_systems_w1() {
    local s fam rc
    for s in $SYSTEMS; do
      mk sys_start $tag $s
      case $s in postgres|clickhouse)
        $KIT/lib/servers.sh start $s $tag | tail -1
        $PY wait --system $s --timeout 3600 || { fail "SERVER $s would not start $tag"; $KIT/lib/servers.sh stop $s $tag; mk sys_end $tag $s; continue; }
        export EXTCMP_CONTAINER=extcmp-$s-$tag EXTCMP_STATE_CMD="$KIT/lib/servers.sh state $s $tag" EXTCMP_LOGS_CMD="$KIT/lib/servers.sh logs $s $tag 40" ;;
      *) unset EXTCMP_CONTAINER EXTCMP_STATE_CMD EXTCMP_LOGS_CMD ;; esac
      for fam in $fams; do
        out=$RES/$tag/rotM; mkdir -p $out
        EXTCMP_EXCLUDE=$CD/w1-exclude-$tag.json EXTCMP_PARAMS=$pf EXTCMP_P1_REPS=1 timeout -k 60 3000 $PY query --system $s --log $LOGF --ref $STORES/native-$tag --workdir $EXTCMP_ROOT --size-tag $tag --mode timed --family $fam --answers $out/answers-$s --json $out/$fam-$s.json --params $pf; rc=$?
        [ $rc = 0 ] || fail "BLOCK FAILED $tag $fam $s rc=$rc"
        cp -au $RESL/. $OUT/ 2>/dev/null
      done
      case $s in postgres|clickhouse) $KIT/lib/servers.sh stop $s $tag | tail -1 ;; esac
      mk sys_end $tag $s
    done
  }
  say "pass $tag"; p1_systems_w1 ;;
w5)
  setup_root ${LOCAL:-${TMPDIR:-/tmp}}/memw5; export TMPDIR=$ROOT/scratch; source $KIT/lib/env.sh; RES=$RESL; proto_p1
  start_sampler
  TAG=${MEM_W5_TAG:-W5-wikidata-v2}; W=$E/w5-wikidata; WS=$E/studies/sizes/stores/$TAG; SIZE=1000000; PD=$W/params-c3; [ $TAG = W5-wikidata-v2 ] || PD=$W/params
  SYSTEMS=${MEM_SYSTEMS:-native duckdbsql postgres neo4j}; export EXTCMP_DUCKDBSQL_THREADS=64
  cp -a $W/campaign/eventlogs/$TAG.eventlog.jsonl $W/eventlogs/$TAG.eventlog.jsonl.meta.json $LOGS/ 2>/dev/null; LOGF=$LOGS/$TAG.eventlog.jsonl; need_file $LOGF
  [ -z "${MEM_SMOKE:-}" ] && { w=$(awk -v f="$TAG.eventlog.jsonl" '$2 == f {print $1}' $W/eventlogs/SHA256SUMS-$TAG); g=$(sha256sum $LOGF | cut -d' ' -f1); [ -n "$w" ] && [ "$w" = "$g" ] || { echo "FROZEN LOG CHECK FAILED $TAG: stopping"; exit 2; }; }
  $PYV - "$LOGF" "$PD/params-$TAG-bel.json" <<'PY' || { echo "SIDECAR FAILED: stopping"; exit 2; }
import json, os, sys
log, pf = sys.argv[1], sys.argv[2]; st = os.stat(log)
ins = {i["query"]: i["args"] for i in json.load(open(pf))["instances"]}
sn, rc, df, h = ins["b.snap.hop2@T"], ins["b.reach@T"], ins["b.diff"], ins["b.hist"]
d = {"size": st.st_size, "mtime_ns": st.st_mtime_ns, "snap_seeds": sn["seeds"], "t_star": sn["t_valid"],
     "seed_node": rc["src"], "reach_window": rc["window"], "T": sn["as_of_tt"], "T_base": df["t1"],
     "diff_window": df["window"], "e_star": h, "w5_placeholder": True}
open(log[:-len(".jsonl")] + ".beliefq-params.json", "w").write(json.dumps(d))
PY
  say "restore"; for s in $SYSTEMS; do case $s in native|duckdbsql) restore_tar $WS/$s-$TAG.tar.zst $STORES ;; *) restore_tar $WS/$s-$TAG.tar.zst $SRV ;; esac; done
  need_dir $STORES/native-$TAG
  say "pass registry"; p1_systems $TAG $SIZE $PD/params-$TAG-reg.json "${MEM_FAMS:-P T S W D N R M}"
  say "pass belief"; export EXTCMP_BELIEFQ_INDEXES=1; p1_systems $TAG $SIZE $PD/params-$TAG-bel.json B ;;
w4)
  W=$E/w4-finbench; PT=${MEM_W4_POINT:-sf10-r10}; S=$W/w4stores/$PT; PAR=$W/params/params-sf10.json; need_dir $S; need_file $PAR
  setup_root ${LOCAL:-${TMPDIR:-/tmp}}/memw4; export TMPDIR=$ROOT/scratch TR_CEILING_S=10 TR_GRACE_S=5 EXTCMP_MG_MEMORY_LIMIT_MB=118000; source $KIT/lib/env.sh; export EXTCMP_SERVER_TIMEOUT_S=10 EXTCMP_START_WAIT_S=7200
  start_sampler
  PYC="${EXTCMP_NUMA_CLIENT:+$EXTCMP_NUMA_CLIENT }$PYV"
  waitpg() { local i; for i in $(seq 1 300); do $PYV -c "import psycopg,os; psycopg.connect(os.environ['EXTCMP_PG_DSN']).close()" 2>/dev/null && return 0; sleep 2; done; return 1; }
  waitbolt() { local i; for i in $(seq 1 2400); do $PYV -c "
import neo4j,sys
a={'neo4j':('neo4j','neo4j'),'none':None}[sys.argv[2]]
d=neo4j.GraphDatabase.driver(sys.argv[1], auth=a); d.verify_connectivity(); d.close()" "$1" "$2" 2>/dev/null && return 0; sleep 2; done; return 1; }
  for t in ${MEM_SYSTEMS:-gnomon duckdbsql postgres neo4j memgraph}; do
    args=(w4 --system $t --params $PAR --beliefs current,historic --rotation M --protocol 1,1,1,1 --out $RESL/$t.json --jsonl $RESL/$t-calls.jsonl --label "mem W4 $PT $t")
    case $t in
      gnomon) g=$(ls $S/gnomon-*.tar.zst | head -1); restore_tar $g $STORES; need_dir $STORES/gnomon-$PT/native; args+=(--store $STORES/gnomon-$PT) ;;
      duckdbsql) ( cd $S && sha256sum -c --quiet duckdbsql.duckdb.sha256 ) || { echo "RESTORE CHECK FAILED duckdb: stopping"; exit 2; }; cp $S/duckdbsql.duckdb ${LOCAL:-${TMPDIR:-/tmp}}/w4.duckdb; args+=(--db ${LOCAL:-${TMPDIR:-/tmp}}/w4.duckdb) ;;
      *) restore_tar $S/$t.tar.zst $SRV ;;
    esac
    mk sys_start $PT $t
    case $t in
      postgres) $KIT/lib/servers.sh start $t w4 | tail -1; waitpg || { fail "SERVER $t would not start"; continue; } ;;
      neo4j) $KIT/lib/servers.sh start $t w4 | tail -1; waitbolt $GNOMON_NEO4J_URI neo4j || { fail "SERVER $t would not start"; continue; } ;;
      memgraph) $KIT/lib/servers.sh start $t w4 | tail -1; waitbolt $GNOMON_MEMGRAPH_URI none || { fail "SERVER $t would not start"; continue; } ;;
    esac
    say "pass $PT $t"
    env W4_DUCKDB_THREADS=64 EXTCMP_ENGINE_TIME=0 $PYC $MY/tools/mem_tr.py --keep $CD/w4-keep-$t.json "${args[@]}" 2>&1 | tail -n 2
    [ "${PIPESTATUS[0]}" = 0 ] && [ -s $RESL/$t.json ] || fail "BLOCK FAILED $PT $t"
    case $t in postgres|neo4j|memgraph) $KIT/lib/servers.sh stop $t w4 | tail -1 ;; esac
    mk sys_end $PT $t
    cp -au $RESL/. $OUT/
  done ;;
w3)
  source $REPO/benchmarks/stages/w3/jobs/w3_lib.sh
  pt_config SF10 || exit 2
  setup_root ${LOCAL:-${TMPDIR:-/tmp}}/memw3; export W3_DUCKDB_THREADS=64
  start_sampler
  need_file $HIST; HISTL=${LOCAL:-${TMPDIR:-/tmp}}/w3hist.duckdb; cp $HIST $HISTL || exit 2
  PYC="${EXTCMP_NUMA_CLIENT:+$EXTCMP_NUMA_CLIENT }$PYV"; EXC=$E/final/w3/gate/exclusions-SF10.json
  for s in ${MEM_SYSTEMS:-gnomon duckdb postgres memgraph}; do
    say "restore $s"; restore_sys $s || { fail "RESTORE FAILED $s"; continue; }
    mk sys_start SF10 $s
    case $s in postgres|memgraph) TARGET=""; srv_start $s || { fail "SERVER $s would not start"; srv_stop $s; mk sys_end SF10 $s; continue; } ;; esac
    say "pass SF10 $s (target $TARGET)"
    env EXTCMP_ENGINE_TIME=0 $PYC $MY/tools/mem_tr.py --keep $CD/w3-keep-$s.json w3 --system $s --target "$TARGET" --params $PARAMS --cells "$(cells_of_block ALL)" --settings $(settings_of $s) --instances 0,1,2 --hist-db $HISTL --rotation M --protocol 1,1,1,1 --out $RESL/$s.json --jsonl $RESL/$s-calls.jsonl --label "mem W3 SF10 $s" 2>&1 | tail -n 2
    [ "${PIPESTATUS[0]}" = 0 ] && [ -s $RESL/$s.json ] || fail "BLOCK FAILED SF10 $s"
    case $s in postgres|memgraph) srv_stop $s ;; esac
    mk sys_end SF10 $s
    cp -au $RESL/. $OUT/
  done ;;
*) echo "unknown MEM_WL"; exit 2 ;;
esac
$KIT/lib/servers.sh stopall 2>/dev/null
kill -TERM $RSSP 2>/dev/null; sleep 3
$PYV $MY/tools/mem_table.py --res $RESL --wl $MEM_WL --out $RESL/mem.md || echo "table failed"; cat $RESL/mem.md
cp -au $RESL/. $OUT/
[ -s $RESL/failed-blocks.txt ] && { cat $RESL/failed-blocks.txt; exit 8; }
say "mem body done"
