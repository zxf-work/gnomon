# W3 timed stage: shared setup of the stage's jobs (sourced by every job). Mirror of the W2 stage
# (benchmarks/stages/w2/jobs/w2_lib.sh) for the LDBC SNB-derived bi-temporal workload; the query runner is the kit's shared
# timed runner (scripts/timed_runner.py w3), unchanged, through tools/w3_timed.py.
#   this stage's directory benchmarks/stages/w3/). The job refuses any other kit code or engine build
#.
#   Gnomon as shipped: admission on (no cost-check bypass), every design switch at its default, GNOMON_MATCH_THREADS=32.
#   Protocol: per instance a priming call (unmeasured, outside the ceiling,
#   killed at 60 s, server caps 58 s for it, replayed after every worker restart), 1 warm-up + 5 timed calls (1 + 3 above
#   1 s), the 10 s ceiling on the client call (TR_CEILING_S), server cap 10 s (PostgreSQL, Neo4j, Memgraph, DuckDB interrupt,
#   LadybugDB), every system in a killable worker (the runner's), censor-once (tools/w3_timed.py).
set -uo pipefail
W3=${W3_STAGE_ROOT:-${GNOMON_BENCH_ROOT}/final/w3}; E=${GNOMON_BENCH_ROOT}
source $W3/site.env
export EXTCMP_JOB_SAFE=1 EXTCMP_DURABLE=1 GNOMON_MATCH_THREADS=32
export TR_CEILING_S=10 TR_GRACE_S=5 W3_CAP_S=10 W3_GNOMON_BLOCKED=NONE EXTCMP_SERVER_TIMEOUT_S=10
export W3_EXT=$E/w3-snb/ext EXTCMP_NEO4J_PLUGINS='["graph-data-science"]' EXTCMP_MG_MALLOC_CONF=retain:true EXTCMP_AEONG_MALLOC_CONF=retain:true
unset EXTCMP_ENGINE_TIME EXTCMP_PG_STATS W3_DUCKDB_THREADS
REPO=$EXTCMP_REPO; PYV=$REPO/.venv/bin/python
STG=benchmarks/stages/w3; PFD=benchmarks/compare/kit/kit/pilot-final
TOOLS=$REPO/$STG/tools; PARD=$REPO/$STG/params
P3=$E/w3-snb; KITHEAD=${EXTCMP_KITHEAD:-HEAD}
# the answer check's window limit (s) per (system, query group): a slower window is cut and its unfinished instances are
# recorded "unreached" (neither compared nor timed)
VWIN=${W3_VERIFY_WINDOW_S:-900}
GATED=${W3_GATE_DIR:-$W3/gate}
utc() { date -u +%FT%TZ; }
say() { echo "=== $* $(utc)"; }

# query groups: the answer-check windows (VWIN each) and the timed blocks (S = IS + T-LDBC, C = IC, B = BI + BIH)
Q_IS="IS1,IS2,IS3,IS4,IS5,IS6,IS7"
Q_TL="TLDBC-IS1,TLDBC-IS2,TLDBC-IS3,TLDBC-IS4,TLDBC-IS5,TLDBC-IS6"
Q_ICa="IC1,IC2,IC3,IC4,IC5,IC6,IC7"
Q_ICb="IC8,IC9,IC10,IC11,IC12,IC13,IC14"
Q_BIa="BI1,BI2,BI3,BI4,BI5"
Q_BIb="BI6,BI7,BI8,BI9,BI10"
Q_BIc="BI11,BI12,BI13,BI14,BI15"
Q_BId="BI16,BI17,BI18,BI19,BI20"
Q_BIH="BIH-K,BIH-VER,BIH-R7,BIH-R3,BIH-R5"
QGROUPS="IS TL ICa ICb BIa BIb BIc BId BIH"
q_of() { local v=Q_$1; echo ${!v}; }
cells_of_block() { case $1 in S) echo "$Q_IS,$Q_TL" ;; C) echo "$Q_ICa,$Q_ICb" ;; B) echo "$Q_BIa,$Q_BIb,$Q_BIc,$Q_BId,$Q_BIH" ;;
                    ALL) echo "$Q_IS,$Q_TL,$Q_ICa,$Q_ICb,$Q_BIa,$Q_BIb,$Q_BIc,$Q_BId,$Q_BIH" ;; esac; }

pt_config() {   # point: SF1 | SF10 | SF0.1 (the smoke job's point; not a result)
  PT=$1
  case $PT in
    SF1)  FULL=$P3/w3stores/SF1-h1-r1/full; GN_TAR=$KF/pilot/stores/w3/gnomon-SF1.tar.zst; GN_DIR=w3-gnomon-SF1
          HIST=$P3/check/SF1-h1-r1/w3.duckdb; PARAMS=$PARD/W3-SF1.timed.params.jsonl; NBIG=20; NSMALL=10; ROT=2
          BLOCKS="S C B"; SYSTEMS="gnomon duckdb64 duckdb32 postgres neo4j memgraph ladybug" ;;
    SF10) FULL=$P3/w3stores/SF10-h1-r1/full; GN_TAR=$FULL/gnomon-f10733cf-sodad393bd540d74c9.tar.zst; GN_DIR=gnomon-SF10-h1-r1
          HIST=$P3/check/SF10-h1-r1-f10733cf/w3.duckdb; PARAMS=$PARD/W3-SF10.timed.params.jsonl; NBIG=8; NSMALL=8; ROT=1
          BLOCKS="ALL"; SYSTEMS="gnomon duckdb64 duckdb32 postgres memgraph ladybug"
          # Memgraph's own --memory-limit: the kit default (40,960 MB) is below what recovering the SF10 store needs (83 GiB resident,
          # under which prep-stores built, recovered and checked this store.
          export EXTCMP_MG_MEMORY_LIMIT_MB=118000 ;;
    SF0.1) FULL=$P3/w3stores/SF0.1-h1-r10/full; GN_TAR=""; GN_DIR=""
          HIST=$P3/check/SF0.1-h1-r10/w3.duckdb; PARAMS=$P3/params/W3-SF0.1.params.jsonl; NBIG=3; NSMALL=3; ROT=2
          BLOCKS="S"; SYSTEMS="duckdb64 postgres neo4j" ;;
    D-*|G-*|H-*)
      # D-r0 D-r0p02 D-r0p3 D-r10; G-lagsame G-lagmonths G-lagyears; H-h3-r0 H-h3-r1 H-hmax-r0 H-hmax-r1; a smoke: D-SF0.1-r0
      local sw=${PT%%-*} rest=${PT#*-} sf=SF1 lg nn=10
      case $rest in SF0.1-*) sf=SF0.1; rest=${rest#SF0.1-}; nn=3 ;; esac
      case $sw in D) lg=$sf-h1-$rest ;; G) lg=$sf-h1-r1-$rest ;; H) lg=$sf-$rest ;; esac
      FULL=${W3D_STORES:-$W3/stores}/$lg/full; GN_TAR=$FULL/gnomon-$lg.tar.zst; GN_DIR=gnomon-$lg
      HIST=${W3D_CHECK:-$W3/check}/$lg/w3.duckdb; NBIG=$nn; NSMALL=$nn; ROT=1; BLOCKS="ALL"; ESYS="gnomon duckdb64"
      PARAMS=$PARD/W3-SF1.timed.params.jsonl; [ $sf = SF0.1 ] && PARAMS=$P3/params/W3-SF0.1.params.jsonl
      Q_W3D="IS1,IS2,IC2,IC6,IC9,IC13,BI2,BI8,BI14,BIH-VER,BIH-R3,TLDBC-IS3"; QGROUPS="W3D"
      cells_of_block() { echo "$Q_W3D"; }
      case $sw in D) SYSTEMS="gnomon duckdb64 duckdb32 memgraph"; W3D_SETTINGS=asof,window ;; G) SYSTEMS="gnomon duckdb64 duckdb32"; W3D_SETTINGS=asof,window ;;
                  H) SYSTEMS="gnomon duckdb64 duckdb32"; W3D_SETTINGS=current,asof ;; esac
      SYSTEMS=${W3D_SYSTEMS:-$SYSTEMS}
      [ -d "$FULL" ] || { echo "NO STORE DIRECTORY $FULL: stopping"; return 2; }; [ -s "$HIST" ] || { echo "NO CHECK DATABASE $HIST: stopping"; return 2; } ;;
    *) echo "unknown point $PT"; return 1 ;;
  esac
  if [ "$C3" = 1 ]; then   # the C3 confirmation: Gnomon and the DuckDB (SQL) drift sample (64 threads), one rotation
    SYSTEMS="gnomon duckdb64"; ROT=1; ESYS="gnomon"
    REF=$C3_W3_GATE/ref-$PT-gnomon.jsonl
    case $PT in SF1) OLDT=${C3_OLD_TIMED_SF1:-} ;; SF10) OLDT=${C3_OLD_TIMED_SF10:-} ;; *) OLDT="" ;; esac
    # the Gnomon store: the recorded tarball unless W3_GN_TAR_<point> names a C3-built one (decided after the pilot's store check)
    local v=W3_GN_TAR_${PT//./_}; if [ -n "${!v:-}" ]; then GN_TAR=${!v}; fi
  fi
}
base_of() { case $1 in duckdb*) echo duckdb ;; *) echo $1 ;; esac; }   # a timed-system token -> the kit system name
n_of() { case $1 in gnomon|duckdb) echo $NBIG ;; *) echo $NSMALL ;; esac; }
settings_of() { if [ -n "${W3D_SETTINGS:-}" ]; then echo $W3D_SETTINGS; return 0; fi; if [ "$1" = ladybug ] && [ "$PT" = SF10 ]; then echo current,asof; else echo current,asof,window; fi; }

guard() {   # the kit at the pinned code (later commits may add only pilot-final/ and this stage's directory) + the pinned engine build
  local head so got extra dirty imp
  head=$(git -C $REPO rev-parse --short=8 HEAD); so=$(ls $REPO/gnomon/_engine*.so | head -1); got=$(sha256sum $so | cut -d' ' -f1)
  extra=$(git -C $REPO diff --name-only $KITHEAD HEAD | grep -v -e "^$PFD/" -e "^$STG/" | wc -l)
  dirty=$(git -C $REPO status --short | grep -v '^??' | grep -v -e " $PFD/" -e " $STG/" | wc -l)
  imp=$(cd / && $PYV -c "import gnomon, gnomon._engine as e; print(gnomon.__file__, e.__file__)" 2>&1)
  echo "GUARD kit $head (pinned code $KITHEAD; files changed since outside pilot-final/ and $STG/: $extra), tracked changes outside them: $dirty; so $so sha256 $got (pinned $EXTCMP_ENGINE_SO_SHA256); engine commit $EXTCMP_ENGINE_COMMIT; import: $imp"
  git -C $REPO merge-base --is-ancestor $KITHEAD HEAD || { echo "GUARD: $KITHEAD is not an ancestor of the kit head: refusing (the guard's kit commit is EXTCMP_KITHEAD=${EXTCMP_KITHEAD:-unset, default HEAD}, set in $(dirname $REPO)/site.env)"; exit 3; }
  [ "$extra" = 0 ] || { echo "GUARD: kit code changed since $KITHEAD: refusing. EXTCMP_KITHEAD=${EXTCMP_KITHEAD:-unset, default HEAD} (in $(dirname $REPO)/site.env) names the kit commit this stage was checked on; after merging a later kit commit into the stage branch, set EXTCMP_KITHEAD to that commit, or revert the merge. Files changed outside pilot-final/ and $STG/: $(git -C $REPO diff --name-only $KITHEAD HEAD | grep -v -e "^$PFD/" -e "^$STG/" | head -5 | tr '\n' ' ')"; exit 3; }
  [ "$got" = "$EXTCMP_ENGINE_SO_SHA256" ] || { echo "GUARD: engine .so is not the pinned build: refusing"; exit 3; }
  [ "$dirty" = 0 ] || { echo "GUARD: tracked changes in the kit tree: refusing"; exit 3; }
  case "$imp" in "$REPO/gnomon/__init__.py $REPO/gnomon/_engine"*) ;; *) echo "GUARD: gnomon does not import from $REPO: refusing"; exit 3 ;; esac
  env | grep -q -E '^GNOMON_(SKIP_COST|NO_ADMISSION|ADMISSION)' && { echo "GUARD: an admission switch is set: refusing"; exit 3; }
  return 0
}
hostinfo() { echo "limits: max_map_count $(cat /proc/sys/vm/max_map_count) memgraph --memory-limit ${EXTCMP_MG_MEMORY_LIMIT_MB:-default 40960} MB; ulimit: $(ulimit -a | tr -s " " | tr "\n" ";")"; echo "host $(hostname) cores $(nproc) job ${SLURM_JOB_ID:-} partition ${SLURM_JOB_PARTITION:-?}; numa client=[${EXTCMP_NUMA_CLIENT:-}] server=[${EXTCMP_NUMA_SERVER:-}]; Gnomon env: $(env | grep ^GNOMON_ | sort | tr '\n' ' '); TR_CEILING_S=$TR_CEILING_S W3_CAP_S=$W3_CAP_S server cap ${EXTCMP_SERVER_TIMEOUT_S}"; }

setup_root() {   # dir: node-local root for stores and servers; sources the kit's environment (SRV, STORES, ports, server scripts)
  export EXTCMP_ROOT=$1 TMPDIR=$1/scratch
  mkdir -p $EXTCMP_ROOT/{srv,scratch,stores}
  source $KIT/lib/env.sh
}
unpack() { local t0=$(date +%s); mkdir -p "$2"; zstd -q -d -c "$1" | tar -C "$2" -xf - && echo "restored $(basename $1) in $(( $(date +%s) - t0 )) s"; }
waitpg() { for i in $(seq 1 300); do $PYV -c "import psycopg,os; psycopg.connect(os.environ['EXTCMP_PG_DSN']).close()" 2>/dev/null && { echo "postgres ready after ${i}x2 s"; return 0; }; sleep 2; done; echo "postgres NOT ready"; return 1; }
waitbolt() {   # <uri> <auth: neo4j|none|empty> [tries]
  for i in $(seq 1 ${3:-600}); do $PYV -c "
import neo4j,sys
a={'neo4j':('neo4j','neo4j'),'none':None,'empty':('','')}[sys.argv[2]]
d=neo4j.GraphDatabase.driver(sys.argv[1], auth=a); d.verify_connectivity(); d.close()" "$1" "$2" 2>/dev/null && { echo "bolt $1 ready after ${i}x2 s"; return 0; }; sleep 2; done; echo "bolt $1 NOT ready"; return 1; }

restore_sys() {   # system: its store from the persisted full stores into the node-local root (sha256 checked once); sets TARGET for in-process systems
  local s=$1
  case $s in
    gnomon) if [ ! -d $STORES/$GN_DIR ]; then
              if [ -e $GN_TAR.sha256 ]; then ( cd $(dirname $GN_TAR) && sha256sum -c --quiet $(basename $GN_TAR).sha256 ) || return 1
              else echo "(no .sha256 beside $GN_TAR: unpacked unchecked, as the pilot did)"; fi
              unpack $GN_TAR $STORES; fi
            [ -d $STORES/$GN_DIR ] || return 1; TARGET=$STORES/$GN_DIR ;;
    duckdb*) if [ ! -e ${LOCAL:-${TMPDIR:-/tmp}}/w3-duckdb.duckdb ]; then ( cd $FULL && sha256sum -c --quiet duckdb.duckdb.sha256 ) || return 1
              cp $FULL/duckdb.duckdb ${LOCAL:-${TMPDIR:-/tmp}}/w3-duckdb.duckdb; fi
            TARGET=${LOCAL:-${TMPDIR:-/tmp}}/w3-duckdb.duckdb ;;
    postgres|neo4j|memgraph) if [ ! -d $SRV/$s-w3 ]; then ( cd $FULL && sha256sum -c --quiet $s.tar.zst.sha256 ) || return 1
              unpack $FULL/$s.tar.zst $SRV; fi
            [ -d $SRV/$s-w3 ] || return 1 ;;
    ladybug) if [ ! -d ${LOCAL:-${TMPDIR:-/tmp}}/w3lb ]; then ( cd $FULL && sha256sum -c --quiet ladybug.tar.zst.sha256 ) || return 1
              unpack $FULL/ladybug.tar.zst ${LOCAL:-${TMPDIR:-/tmp}}/w3lb; fi
            TARGET=$(ls -d ${LOCAL:-${TMPDIR:-/tmp}}/w3lb/*.lbdb 2>/dev/null | head -1); [ -n "$TARGET" ] || return 1 ;;
    *) return 1 ;;
  esac
}
srv_start() {   # system: starts the (restored) server and waits until it answers; sets TARGET
  local s=$1 tries=600
  case $s in
    postgres) $KIT/lib/servers.sh start postgres w3 | tail -1; waitpg || return 1; TARGET="$EXTCMP_PG_DSN" ;;
    neo4j)    $KIT/lib/servers.sh start neo4j w3 | tail -1; TARGET=bolt://127.0.0.1:$EXTCMP_NEO4J_PORT; waitbolt $TARGET neo4j $tries || return 1 ;;
    memgraph) [ "$PT" = SF10 ] && tries=2400     # the SF10 store recovers for about 48 min on every start
              $KIT/lib/servers.sh start memgraph w3 | tail -1; TARGET=bolt://127.0.0.1:$EXTCMP_MG_PORT; waitbolt $TARGET empty $tries || return 1 ;;
    aeong)    $KIT/lib/servers.sh start aeong w3 | tail -1; TARGET=bolt://127.0.0.1:$EXTCMP_AEONG_PORT; waitbolt $TARGET neo4j $tries || return 1 ;;
  esac
}
srv_stop() {   # the server's own logs go to Ocean first ($OUT/srvlogs/<system>), then the server stops
  if [ -n "${OUT:-}" ] && [ -d $SRV/$1-w3 ]; then mkdir -p $OUT/srvlogs/$1; cp -a $SRV/$1-w3/server.log $OUT/srvlogs/$1/ 2>/dev/null; cp -a $SRV/$1-w3/log $OUT/srvlogs/$1/ 2>/dev/null; fi
  $KIT/lib/servers.sh stop $1 w3 | tail -1; }

# ---- C3 confirmation. C3_CONFIRM=1: Gnomon only, plus the DuckDB (SQL) drift
# sample (64 threads, C3_DRIFT_DIR/drift-exclude-W3-<point>.json: the recorded gate's exclusions, DuckDB's extended by every
# cell outside the sample); one rotation; the engine-time pass for Gnomon only; NO GATE JOB: after every Gnomon block the
# C3_W3_GATE/ref-<point>-gnomon.jsonl and the recorded timed run C3_OLD_TIMED_<point>); a DIFFERS stops the job (exit 4)
# unless W3_C3_HASH_STOP=0. Unset, the stage runs exactly as before.
C3=${C3_CONFIRM:-0}; ESYS="gnomon duckdb64 postgres"
C3T=$REPO/benchmarks/stages/confirm/tools
if [ "$C3" = 1 ]; then
  : "${EXTCMP_KITHEAD:?C3 mode: EXTCMP_KITHEAD must be set (stage site.env)}" "${C3_W3_GATE:?C3 mode: C3_W3_GATE (the recorded gate dir) must be set}"
fi
