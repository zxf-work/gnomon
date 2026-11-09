#!/bin/bash
# Ingest study body. Each run on a fresh store on ${LOCAL:-${TMPDIR:-/tmp}}, the store or server removed after its record and size.
source ${NEXT_STAGE_ROOT:?}/src/benchmarks/stages/studies/lib.sh
: "${OUT:?}"
P3=$E/w3-snb; SF=${ING_SF:-1}; RUNS=${ING_RUNS:-P,F,L}; PD=7
has() { case ",$RUNS," in *",$1,"*) return 0 ;; esac; return 1; }
if [ "${ING_SMOKE:-0}" != 1 ]; then
  [ "$SF" = 1 ] || { echo "a measured run is at SF1: refusing SF$SF"; exit 3; }
fi
case $SF in 1) Q=r1 ;; *) Q=r10 ;; esac
TAGQ=SF$SF-h1-$Q; META=$P3/base/sf$SF-h1/meta.json; HIST=${ING_HIST:-$P3/check/$TAGQ/hist}
need_file $META; need_dir $HIST
RESL=${LOCAL:-${TMPDIR:-/tmp}}/ingres; mkdir -p $RESL; setup_root ${LOCAL:-${TMPDIR:-/tmp}}/ing
export TMPDIR=$ROOT/scratch W3_EXT=$P3/ext EXTCMP_NEO4J_PLUGINS='["graph-data-science"]' EXTCMP_MG_MALLOC_CONF=retain:true EXTCMP_START_WAIT_S=7200 EXTCMP_SERVER_TIMEOUT_S=14400 W3_STAGE_DIR=${LOCAL:-${TMPDIR:-/tmp}}/stage
mkdir -p $W3_STAGE_DIR
source $KIT/lib/env.sh
PYC="${EXTCMP_NUMA_CLIENT:+$EXTCMP_NUMA_CLIENT }$PYV"; X=$REPO/scripts; SRVSH=$KIT/lib/servers.sh
cp -r $HIST ${LOCAL:-${TMPDIR:-/tmp}}/hist; HL=${LOCAL:-${TMPDIR:-/tmp}}/hist
# memory: every process of this job, servers included, every 2 s; started once, stopped when the body ends
$PYV $REPO/$STG/tools/ing_rss.py $RESL/rss-samples.jsonl 2 &
RSSP=$!; trap 'kill -TERM $RSSP 2>/dev/null' EXIT
getlog() {
  local t=$1 f=W3-$1.eventlog.jsonl want got
  [ -s ${LOCAL:-${TMPDIR:-/tmp}}/$f ] || cp $P3/eventlogs/$f ${LOCAL:-${TMPDIR:-/tmp}}/$f
  want=$(awk -v f="$f" '$2 == f {print $1}' $P3/eventlogs/SHA256SUMS-SF$SF); got=$(sha256sum ${LOCAL:-${TMPDIR:-/tmp}}/$f | cut -d' ' -f1)
  [ -n "$want" ] && [ "$want" = "$got" ] || { echo "FROZEN LOG CHECK FAILED $t ($got vs $want): stopping" >&2; exit 2; }
  echo ${LOCAL:-${TMPDIR:-/tmp}}/$f; }
verified() { $PYV -c "import json,sys; sys.exit(0 if json.load(open(sys.argv[1])).get('verified') else 1)" "$1" || { echo "VERIFICATION FAILED $1: stopping"; cp -au $RESL/. $OUT/; exit 7; }; }
sizeof() { du -sb "$1" 2>/dev/null | cut -f1; }
run_gnomon() {   # run tag prefix(0|1)
  local r=$1 t=$2 pf=$3 l st=$ROOT/gn-$1-$2 rec a=(); l=$(getlog $t) || exit 2; rec=$RESL/$r-gnomon-$t.json
  [ $pf = 1 ] && a=(--prefix-days $PD)
  say "$r gnomon $t prefix=$pf"; rm -rf $st
  local m=$P3/base/sf$SF-$(echo $t | cut -d- -f2)/meta.json
  $PYC $X/w3_ingest_gnomon.py --log $l --meta $m --store $st --out $rec "${a[@]}" | tail -c 700; [ "${PIPESTATUS[0]}" = 0 ] || { echo "REFUSED BATCH OR FAILED RUN $r gnomon $t: stopping"; cp -au $RESL/. $OUT/; exit 7; }
  verified $rec
  # a whole-log run's store is kept for the digest check at the end of the job: digests take minutes and no timing runs beside them
  if [ $pf = 0 ]; then DIGESTS="$DIGESTS $t:$st:$RESL/$r-gnomon-$t-digest.json"; else rm -rf $st; fi
  cp -au $RESL/. $OUT/; }
DIGESTS=""
ref_of() {   # tag: the replayed store of the same log
  case $1 in
    SF1-h1-r1) echo $E/kit/pilot/stores/w3/gnomon-SF1.tar.zst ;;
    SF1-h1-r0) echo $E/studies/w3d/stores/SF1-h1-r0/full/gnomon-SF1-h1-r0.tar.zst ;;
    SF1-h1-r10) echo $E/studies/w3d-r10/stores/SF1-h1-r10/full/gnomon-SF1-h1-r10.tar.zst ;;
    SF0.1-h1-r10) echo $P3/stores/W3-SF0.1-h1-r10 ;;
    SF0.1-h1-r0) echo $E/studies/w3d/smoke-stores/SF0.1-h1-r0/full/gnomon-SF0.1-h1-r0.tar.zst ;;
  esac; }
ref_digest() {   # tag store out: digest of this run's store against the replayed store's; a difference stops the job
  local t=$1 s=$2 o=$3 r=${ING_REF:-$(ref_of $1)} d=${LOCAL:-${TMPDIR:-/tmp}}/ref-$1-$$-$BASHPID tgt
  [ -e "$r" ] || { echo "NO REFERENCE STORE for $t ($r): stopping"; exit 2; }
  if [ -d "$r" ]; then tgt=$r; else rm -rf $d; mkdir -p $d; zstd -q -d -c $r | tar -C $d -xf - || { echo "REFERENCE UNPACK FAILED: stopping"; exit 2; }; tgt=$(ls -d $d/*/ | head -1); fi
  GNOMON_STORE_WARM_OPEN=0 $PYV -c "
import json, sys, gnomon
def dg(p):
    s = gnomon.open(p, backend='native', read_only=True); d = s.digest_streaming(); s.close(); return d
a, b = dg(sys.argv[1]), dg(sys.argv[2])
json.dump({'this_run': a, 'reference': b, 'reference_path': sys.argv[3], 'equal': a == b, 'verified': a == b}, open(sys.argv[4], 'w'), indent=1); print('digest', a[:16], b[:16], 'equal' if a == b else 'DIFFERENT')" "$s" "$tgt" "$r" "$o" || { echo "DIGEST FAILED: stopping"; exit 8; }
  rm -rf $d; }
run_duck() {   # run prefix threads
  local r=$1 pf=$2 th=$3 db=$ROOT/dd-$1.duckdb rec=$RESL/$1-duckdb$3.json a=(); [ $pf = 1 ] && a=(--prefix-days $PD)
  say "$r duckdb threads $th"; rm -f $db $db.wal
  W3_INGEST_DUCK_THREADS=$th $PYC $X/w3_snb_ingest.py sql --system duckdb --hist $HL --base-meta $META --db $db --out $rec "${a[@]}" | tail -c 500; [ "${PIPESTATUS[0]}" = 0 ] || { echo "LOAD FAILED duckdb$th"; verified $rec; exit 8; }
  verified $rec; bytes $rec $db; rm -f $db $db.wal; cp -au $RESL/. $OUT/; }
bytes() { $PYV -c "import json,sys; p=sys.argv[1]; d=json.load(open(p)); d['bytes_on_disk']=int(sys.argv[2] or 0); json.dump(d,open(p,'w'),indent=1)" "$1" "$(sizeof $2)"; }
waitpg() { local i; for i in $(seq 1 300); do $PYV -c "import psycopg,os; psycopg.connect(os.environ['EXTCMP_PG_DSN']).close()" 2>/dev/null && return 0; sleep 2; done; return 1; }
run_pg() {   # run prefix
  local r=$1 pf=$2 rec=$RESL/$1-postgres.json a=(); [ $pf = 1 ] && a=(--prefix-days $PD)
  say "$r postgres"; $SRVSH wipe postgres w3; $SRVSH start postgres w3 | tail -1; waitpg || { echo "postgres would not start: stopping"; exit 8; }
  $PYC $X/w3_snb_ingest.py sql --system postgres --hist $HL --base-meta $META --tmp ${LOCAL:-${TMPDIR:-/tmp}} --dsn "$EXTCMP_PG_DSN" --out $rec "${a[@]}" | tail -c 500; local rc=${PIPESTATUS[0]}
  $SRVSH stop postgres w3 | tail -1; [ $rc = 0 ] || { echo "LOAD FAILED postgres"; verified $rec; exit 8; }
  verified $rec; bytes $rec $SRV/postgres-w3; $SRVSH wipe postgres w3; cp -au $RESL/. $OUT/; }
run_graph() {   # run system
  local r=$1 s=$2 rec=$RESL/$1-$2.json uri
  case $s in neo4j) uri=bolt://127.0.0.1:$EXTCMP_NEO4J_PORT ;; memgraph) uri=bolt://127.0.0.1:$EXTCMP_MG_PORT ;; esac
  say "$r $s"; $SRVSH wipe $s w3
  $PYC $X/w3_snb_ingest.py graph --system $s --hist $HL --base-meta $META --srv $SRV --uri $uri --servers $SRVSH --neo4j-sif $SIF_DIR/neo4j.sif --out $rec --prefix-days $PD | tail -c 500; local rc=${PIPESTATUS[0]}
  $SRVSH stop $s w3 | tail -1; [ $rc = 0 ] || { echo "LOAD FAILED $s"; verified $rec; exit 8; }
  verified $rec; bytes $rec $SRV/$s-w3; $SRVSH wipe $s w3; cp -au $RESL/. $OUT/; }
run_graph_full() {   # system: run G (coverage row 7): the full stream, every transaction after T0, by run P's graph method with no prefix; one progress line per transaction
  local s=$1 rec=$RESL/G-$1.json uri
  case $s in neo4j) uri=bolt://127.0.0.1:$EXTCMP_NEO4J_PORT ;; memgraph) uri=bolt://127.0.0.1:$EXTCMP_MG_PORT ;; esac
  say "G $s full stream"; $SRVSH wipe $s w3
  W3_INGEST_PROGRESS=$RESL/G-$s-progress.jsonl $PYC $X/w3_snb_ingest.py graph --system $s --hist $HL --base-meta $META --srv $SRV --uri $uri --servers $SRVSH --neo4j-sif $SIF_DIR/neo4j.sif --out $rec | tail -c 500; local rc=${PIPESTATUS[0]}
  $SRVSH stop $s w3 | tail -1; [ $rc = 0 ] || { echo "LOAD FAILED $s"; verified $rec; exit 8; }
  verified $rec; bytes $rec $SRV/$s-w3; $SRVSH wipe $s w3; cp -au $RESL/. $OUT/; }
if has G; then
  for s in ${ING_GRAPH:-memgraph neo4j}; do run_graph_full $s; done
fi
if has P; then
  say "run P, repetition 1"
  run_gnomon P1 $TAGQ 1; run_duck P1 1 64; run_duck P1b 1 32; run_pg P1 1; run_graph P1 memgraph; run_graph P1 neo4j
  say "run P, repetition 2, order reversed"
  run_graph P2 memgraph; run_pg P2 1; run_duck P2b 1 32; run_duck P2 1 64; run_gnomon P2 $TAGQ 1
fi
if has F; then
  say "run F"
  run_gnomon F $TAGQ 0; run_duck F 0 64; run_duck Fb 0 32; run_pg F 0
fi
if has L; then
  say "run L, F-W3-5"
  run_gnomon L SF$SF-h1-r0 0; run_gnomon L SF$SF-h1-r10 0
fi
if [ -n "$DIGESTS" ]; then
  say "digests of the whole-log runs, against the replayed stores, side by side"
  pids=(); outs=()
  for x in $DIGESTS; do IFS=: read -r t st o <<< "$x"; ref_digest $t $st $o & pids+=($!); outs+=($o); done
  for p in "${pids[@]}"; do wait $p || { echo "DIGEST STEP FAILED: stopping"; cp -au $RESL/. $OUT/; exit 8; }; done
  for o in "${outs[@]}"; do verified $o; done
fi
$PYV $REPO/$STG/tools/ing_table.py $RESL > $RESL/phases.md || { echo "phase table failed"; exit 8; }
cp -au $RESL/. $OUT/; say "ingest body done"
