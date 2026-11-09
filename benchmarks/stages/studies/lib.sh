#   under results/smoke-* and no later job reads it.
#   Gnomon as shipped: admission on, every design switch at its default unless a configuration sets it, GNOMON_MATCH_THREADS=32.
#   5 stopped at the time limit, 7 answer mismatch, 8 failed windows at the end.
set -uo pipefail
: "${NEXT_STAGE_ROOT:?NEXT_STAGE_ROOT must name studies/<stage>}"
E=${GNOMON_BENCH_ROOT}
source $NEXT_STAGE_ROOT/site.env
export EXTCMP_JOB_SAFE=1 EXTCMP_DURABLE=1 GNOMON_MATCH_THREADS=32
unset EXTCMP_ENGINE_TIME EXTCMP_PG_STATS EXTCMP_GATE_DIR EXTCMP_PARAMS EXTCMP_EXCLUDE EXTCMP_VERIFY_FAMILY \
      EXTCMP_BELIEFQ_INDEXES EXTCMP_P1_REPS EXTCMP_R10M_FROM EXTCMP_R10M_SKIP P1_VTAG EXTCMP_YIELD C3_CONFIRM
REPO=$EXTCMP_REPO; PYV=$REPO/.venv/bin/python
STG=benchmarks/stages/studies; TOOLS=$REPO/$STG/tools
utc() { date -u +%FT%TZ; }
say() { echo "=== $* $(utc)"; }
guard() {   # exactly the pinned kit commit, no tracked change, the pinned engine build, gnomon imported from this tree
  : "${EXTCMP_KITHEAD:?stage site.env must set EXTCMP_KITHEAD}"
  local head so got dirty imp
  head=$(git -C $REPO rev-parse HEAD); so=$(ls $REPO/gnomon/_engine*.so 2>/dev/null | head -1)
  [ -n "$so" ] || { echo "GUARD: no engine .so in $REPO/gnomon: refusing"; exit 3; }
  got=$(sha256sum $so | cut -d' ' -f1)
  dirty=$(git -C $REPO status --short | grep -v '^??' | wc -l)
  imp=$(cd /tmp && $PYV -c "import gnomon, gnomon._engine as e; print(gnomon.__file__, e.__file__)" 2>&1)
  echo "GUARD kit $head (pinned $EXTCMP_KITHEAD), tracked changes $dirty; so sha256 $got (pinned $EXTCMP_ENGINE_SO_SHA256); import $imp"
  case "$head" in "$EXTCMP_KITHEAD"*) ;; *) echo "GUARD: kit head is not the pinned commit: refusing"; exit 3 ;; esac
  [ "$got" = "$EXTCMP_ENGINE_SO_SHA256" ] || { echo "GUARD: engine .so is not the pinned build: refusing"; exit 3; }
  [ "$dirty" = 0 ] || { echo "GUARD: tracked changes in the kit tree: refusing"; exit 3; }
  case "$imp" in "$REPO/gnomon/__init__.py $REPO/gnomon/_engine"*) ;; *) echo "GUARD: gnomon does not import from $REPO: refusing"; exit 3 ;; esac
  env | grep -q -E '^GNOMON_(SKIP_COST|NO_ADMISSION|ADMISSION)' && { echo "GUARD: an admission switch is set: refusing"; exit 3; }
  return 0
}
hostinfo() { echo "host $(hostname) cores $(nproc) job ${SLURM_JOB_ID:-none} partition ${SLURM_JOB_PARTITION:-?}; numa client=[${EXTCMP_NUMA_CLIENT:-}] server=[${EXTCMP_NUMA_SERVER:-}]; LOCAL=${LOCAL:-?} free $(df -h ${LOCAL:-/tmp} | tail -1 | awk '{print $4}')"; }
need_file() {   # path: a non-empty file, or stop at once
  [ -s "$1" ] || { echo "MISSING OR EMPTY: $1: stopping $(utc)"; exit 2; }; }
need_dir() {    # path: a directory with something in it, or stop at once
  [ -d "$1" ] && [ -n "$(ls -A "$1" 2>/dev/null)" ] || { echo "MISSING OR EMPTY STORE: $1: stopping $(utc)"; exit 2; }; }
restore_tar() {   # archive parent: its sidecar sha256 checked, then unpacked; any failure stops at once
  local a=$1 p=$2 t0=$(date +%s)
  need_file "$a"; need_file "$a.sha256"
  ( cd "$(dirname "$a")" && sha256sum -c --quiet "$(basename "$a").sha256" ) || { echo "RESTORE CHECK FAILED $a: stopping"; exit 2; }
  mkdir -p "$p"; zstd -q -d -c "$a" | tar -C "$p" -xf - ; local st=("${PIPESTATUS[@]}")
  [ "${st[0]}" = 0 ] && [ "${st[1]}" = 0 ] || { echo "RESTORE FAILED $a: stopping"; exit 2; }
  echo "restored $(basename "$a") in $(( $(date +%s) - t0 )) s"; }
setup_root() {   # dir: node-local campaign root
  ROOT=$1; export EXTCMP_ROOT=$ROOT TMPDIR_OVERRIDE=$ROOT/scratch
  mkdir -p $ROOT/{eventlogs,stores,srv,scratch,results}; STORES=$ROOT/stores; SRV=$ROOT/srv; LOGS=$ROOT/eventlogs; RES=$ROOT/results
}
sync_hook() {   # out: records synced after every window
  export EXTCMP_SYNC_HOOK="mkdir -p $1 && cp -au $ROOT/results/. $1/"; }
proto_p1() {    # the timed protocol of the kit's parameter-file path (W1, W2, W5 timed jobs)
  export EXTCMP_P1_CEILING_S=${NEXT_P1_CEILING_S:-10} EXTCMP_SERVER_LIMIT_S=10 EXTCMP_CALL_CEILING_S=30 EXTCMP_SERVER_TIMEOUT_S=28 \
         EXTCMP_VERIFY_CAP_S=30 EXTCMP_P1_SLOW_S=1.0 EXTCMP_INPROC_WORKER=1 EXTCMP_PRIME_KILL_S=60 EXTCMP_WINDOW_TIMEOUT_S=3600; }
