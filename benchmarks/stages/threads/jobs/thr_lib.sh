# Thread-matched control: shared setup of the stage's jobs (sourced by every job).
#   commits may add only pilot-final/ and this stage's directory benchmarks/stages/threads/). Every job refuses to run on any
#   other kit code or any other engine build.
#   Protocol: the W1/W2 timed protocol, unchanged (kit parameter-file path, run_p1.sh's window.sh): a priming call per window
#   (unmeasured, outside the ceiling, killed at 60 s, server caps 58 s for it, replayed after every worker restart), 2
#   warm-ups + 10 timed calls (1 + 3 above 1 s), 10 s ceiling on the client call, server-side limit 10 s (PostgreSQL,
#   DuckDB (SQL)), client alarm 30 s / server timeout 28 s, Gnomon and DuckDB (SQL) in killable in-process workers, admission
#   on (no cost-check bypass), durable configurations. A call that reaches the ceiling is recorded "> 10 s" and the cell's
#   remaining calls are skipped (extcmp_p1.measure; the first call of a cell is retried once, as in W1/W2).
#   What differs between configurations is exactly one setting per system (set_cfg below):
#     g<n>  Gnomon:         GNOMON_MATCH_THREADS=n  (engine designs().match_threads; recorded in settings.designs)
#     d<n>  DuckDB (SQL):   EXTCMP_DUCKDBSQL_THREADS=n  (duckdb.connect config threads; recorded in settings.threads)
#     p<n>  PostgreSQL:     max_parallel_workers_per_gather=n at the session (pyshim/sitecustomize.py; recorded in settings)
set -uo pipefail
THR=${THR_STAGE_ROOT:-${GNOMON_BENCH_ROOT}/final/thr}; E=${GNOMON_BENCH_ROOT}; FIN=$E/final
source $THR/site.env
export EXTCMP_JOB_SAFE=1 EXTCMP_DURABLE=1 GNOMON_MATCH_THREADS=32
unset EXTCMP_ENGINE_TIME EXTCMP_PG_STATS EXTCMP_GATE_DIR EXTCMP_PARAMS EXTCMP_EXCLUDE EXTCMP_VERIFY_FAMILY \
      EXTCMP_BELIEFQ_INDEXES EXTCMP_P1_REPS EXTCMP_R10M_FROM EXTCMP_R10M_SKIP P1_VTAG EXTCMP_YIELD \
      EXTCMP_THR_PG_WORKERS EXTCMP_THR_PG_PLANS
REPO=$EXTCMP_REPO; PYV=$REPO/.venv/bin/python
STG=benchmarks/stages/threads; PFD=benchmarks/compare/kit/kit/pilot-final
TOOLS=$REPO/$STG/tools; PARAMS=$REPO/$STG/params
export PYTHONPATH=$REPO/$STG/pyshim:$PYTHONPATH      # sitecustomize: the PostgreSQL session setting (acts only when EXTCMP_THR_PG_WORKERS is set)
KITHEAD=${EXTCMP_KITHEAD:-HEAD}
# reference answer checks of W1 and W2 (the t32 / w4 / DuckDB (SQL) 64-thread answers, validated in those stages' gates)
REF_W1=$FIN/w1/results/gate-J19/sp; REF_W2=$FIN/w2/results/gate-J21/p1
VWIN=${THR_VERIFY_WINDOW_S:-2700}      # an answer-check window (one config, one family) is cut at this many seconds (unfinished instances: "unreached")
# the ten configurations, base order of the rotation (Gnomon / DuckDB (SQL) / PostgreSQL interleaved)
CFGS="g32 d64 p4 g4 d32 p32 g1 d4 p0 d1"
SYSTEMS="native duckdbsql postgres"
utc() { date -u +%FT%TZ; }
say() { echo "=== $* $(utc)"; }
size_of() { echo 10000000; }
log_of() { case $1 in K10M) echo synth-10000000 ;; *) echo $1 ;; esac; }
fams_of() { case $1 in *-d*) echo B ;; *) echo "P T S W D N R M" ;; esac; }
cfg_sys() { case $1 in g*) echo native ;; d*) echo duckdbsql ;; p*) echo postgres ;; esac; }
guard() {   # the kit at the pinned code (later commits may add only pilot-final/ and this stage's directory) + the pinned engine build
  local head so got extra dirty imp
  head=$(git -C $REPO rev-parse --short=8 HEAD); so=$(ls $REPO/gnomon/_engine*.so | head -1); got=$(sha256sum $so | cut -d' ' -f1)
  extra=$(git -C $REPO diff --name-only $KITHEAD HEAD | grep -v -e "^$PFD/" -e "^$STG/" | wc -l)
  dirty=$(git -C $REPO status --short | grep -v '^??' | grep -v -e " $PFD/" -e " $STG/" | wc -l)
  imp=$(cd /tmp && $PYV -c "import gnomon, gnomon._engine as e; print(gnomon.__file__, e.__file__)" 2>&1)
  echo "GUARD kit $head (pinned code $KITHEAD; files changed since outside pilot-final/ and $STG/: $extra), tracked changes outside them: $dirty; so $so sha256 $got (pinned $EXTCMP_ENGINE_SO_SHA256); engine commit $EXTCMP_ENGINE_COMMIT; import: $imp"
  git -C $REPO merge-base --is-ancestor $KITHEAD HEAD || { echo "GUARD: $KITHEAD is not an ancestor of the kit head: refusing (the guard's kit commit is EXTCMP_KITHEAD=${EXTCMP_KITHEAD:-unset, default HEAD}, set in $(dirname $REPO)/site.env)"; exit 3; }
  [ "$extra" = 0 ] || { echo "GUARD: kit code changed since $KITHEAD: refusing. EXTCMP_KITHEAD=${EXTCMP_KITHEAD:-unset, default HEAD} (in $(dirname $REPO)/site.env) names the kit commit this stage was checked on; after merging a later kit commit into the stage branch, set EXTCMP_KITHEAD to that commit, or revert the merge. Files changed outside pilot-final/ and $STG/: $(git -C $REPO diff --name-only $KITHEAD HEAD | grep -v -e "^$PFD/" -e "^$STG/" | head -5 | tr '\n' ' ')"; exit 3; }
  [ "$got" = "$EXTCMP_ENGINE_SO_SHA256" ] || { echo "GUARD: engine .so is not the pinned build: refusing"; exit 3; }
  [ "$dirty" = 0 ] || { echo "GUARD: tracked changes in the kit tree: refusing"; exit 3; }
  case "$imp" in "$REPO/gnomon/__init__.py $REPO/gnomon/_engine"*) ;; *) echo "GUARD: gnomon does not import from $REPO: refusing"; exit 3 ;; esac
  env | grep -q -E '^GNOMON_(SKIP_COST|NO_ADMISSION|ADMISSION)' && { echo "GUARD: an admission switch is set: refusing"; exit 3; }
  [ -e $REPO/$STG/pyshim/sitecustomize.py ] || { echo "GUARD: the stage's pyshim is missing: refusing"; exit 3; }
  return 0
}
hostinfo() { echo "host $(hostname) cores $(nproc) job $SLURM_JOB_ID partition ${SLURM_JOB_PARTITION:-?}; numa client=[${EXTCMP_NUMA_CLIENT:-}] server=[${EXTCMP_NUMA_SERVER:-}]; Gnomon env: $(env | grep ^GNOMON_ | sort | tr '\n' ' ')"; }
proto_p1() { export EXTCMP_P1_CEILING_S=10 EXTCMP_SERVER_LIMIT_S=10 EXTCMP_CALL_CEILING_S=30 EXTCMP_SERVER_TIMEOUT_S=28 \
                    EXTCMP_VERIFY_CAP_S=30 EXTCMP_P1_SLOW_S=1.0 EXTCMP_INPROC_WORKER=1 EXTCMP_PRIME_KILL_S=60 \
                    EXTCMP_WINDOW_TIMEOUT_S=3600; }
point_env() {   # tag: the belief-axis indexes and the verify family of a density point (run_p1.sh)
  case $1 in *-d*) export EXTCMP_BELIEFQ_INDEXES=1 EXTCMP_VERIFY_FAMILY=B ;; *) unset EXTCMP_BELIEFQ_INDEXES EXTCMP_VERIFY_FAMILY ;; esac; }

# ---- workloads: w1 (W1 stores and single-parameter instances, tags K10M and K10M-d1), w2 (W2 stores, subset of the curated set) ----
wl_set() {   # w1|w2: stores read in place (read-only), frozen logs, parameter files
  WL=$1
  case $WL in
    w1) WTAGS="K10M K10M-d1"; STORE_DIR=$FIN/w1/stores; LOGSRC=$E/v3/eventlogs; REFD=$REF_W1 ;;
    w2) WTAGS="K10M"; STORE_DIR=$FIN/w2/stores; LOGSRC=$E/w2-powerlaw/check1m/campaign/eventlogs; REFD=$REF_W2 ;;
    *) echo "unknown workload $1"; return 2 ;;
  esac
  PD=$PARAMS/$WL
}
setup_root() {   # dir: node-local campaign root of one workload
  ROOT=$1; export EXTCMP_ROOT=$ROOT TMPDIR_OVERRIDE=$ROOT/scratch
  mkdir -p $ROOT/{eventlogs,stores,srv,scratch}; STORES=$ROOT/stores; SRV=$ROOT/srv; LOGS=$ROOT/eventlogs
}
# results of one configuration: $ROOT/results-cfg-<c>, synced to $OUT/cfg-<c> after every window (cp -au, without the answers-*
# directories: the records carry the answer hashes; the answers themselves are not kept)
useroot() {   # name
  CUR=$1; mkdir -p $ROOT/results-$CUR $OUT/$CUR; ln -sfn results-$CUR $ROOT/results
  export EXTCMP_SYNC_HOOK="mkdir -p $OUT/$CUR && (cd $ROOT/results-$CUR && find . -name 'answers-*' -prune -o -type f -print0 | xargs -0 -r cp --parents -au -t $OUT/$CUR)"
  RES=$ROOT/results-$CUR
}
syncall() { local d; for d in $ROOT/results-*; do [ -d "$d" ] && mkdir -p $OUT/${d##*/results-} && \
              ( cd $d && find . -name 'answers-*' -prune -o -type f -print0 | xargs -0 -r cp --parents -au -t $OUT/${d##*/results-} ); done; }
getlogs() {   # tags...: node-local copies of the frozen logs, checked against their stage's SHA256SUMS
  local t n want got
  case $WL in
    w1) local pat=""
        for t in "$@"; do n=$(log_of $t); cp -a $LOGSRC/$n.eventlog.* $LOGS/; pat="$pat|$n"; done
        ( cd $LOGS && grep -E "  (${pat#|})\.eventlog\.jsonl$" $LOGSRC/SHA256SUMS | sha256sum -c ) || { echo "FROZEN LOG CHECK FAILED"; exit 2; } ;;
    w2) for t in "$@"; do
          n=$(log_of $t); cp -a $LOGSRC/$n.eventlog.* $LOGS/
          want=$(awk -v f="powerlaw-10000000.eventlog.jsonl" '$2 == f {print $1}' $E/w2-powerlaw/eventlogs/SHA256SUMS); got=$(sha256sum $LOGS/$n.eventlog.jsonl | cut -d' ' -f1)
          [ -n "$want" ] && [ "$want" = "$got" ] && echo "log $n = powerlaw-10000000 sha256 $got OK" || { echo "FROZEN LOG CHECK FAILED $n ($got vs $want)"; exit 2; }
        done ;;
  esac
}
win() {   # window.sh with the window timeout (EXTCMP_WINDOW_TIMEOUT_S, when set)
  ${EXTCMP_WINDOW_TIMEOUT_S:+timeout -k 60 $EXTCMP_WINDOW_TIMEOUT_S} "$KIT/lib/window.sh" "$@" || echo "WINDOW FAILED $* $(utc)"; }
restore() {   # tag name parent: the stage's persisted store (W1 or W2 $STORE_DIR/<tag>), sha256 checked, then unpacked
  local o=$STORE_DIR/$1 t0=$(date +%s)
  ( cd $o && sha256sum -c --quiet $2.tar.zst.sha256 ) || { echo "RESTORE CHECK FAILED $o/$2.tar.zst"; return 1; }
  mkdir -p "$3"; zstd -q -d -c $o/$2.tar.zst | tar -C "$3" -xf - && echo "restored $2 in $(( $(date +%s) - t0 )) s"; }
parent_of() { case $1 in native|duckdbsql) echo $STORES ;; *) echo $SRV ;; esac; }
# PostgreSQL server-level parallelism limits: written to the restored data directory's postgresql.auto.conf (a postmaster-level
# setting; the server's command line, which the kit fixes, carries neither). The same for all three PostgreSQL configurations:
# only the per-gather cap (a session setting, pyshim) differs between p0, p4 and p32. 64 background-worker slots and 48 parallel
# workers hold 32 per gather node with room to spare. Every other planner and cost setting stays at the kit's value.
pg_conf() {   # tag
  local f=$SRV/postgres-$1/pgdata/postgresql.auto.conf
  [ -e $f ] || { echo "PG CONF: no $f"; return 1; }
  sed -i -e '/^max_worker_processes[ =]/d' -e '/^max_parallel_workers[ =]/d' $f
  printf "max_worker_processes = '64'\nmax_parallel_workers = '48'\n" >> $f
  echo "pg_conf $1: $(grep -c . $f) lines; $(grep -E '^max_(worker|parallel)' $f | tr '\n' ' ')"
  cp $f $OUT/postgresql.auto.conf.$WL-$1 2>/dev/null; return 0
}
set_cfg() {   # config: exactly one thread setting differs; every other value is the W1/W2 one
  local c=$1 n=${1:1}
  export GNOMON_MATCH_THREADS=32 EXTCMP_DUCKDBSQL_THREADS=64; unset EXTCMP_THR_PG_WORKERS
  case $c in g*) export GNOMON_MATCH_THREADS=$n ;; d*) export EXTCMP_DUCKDBSQL_THREADS=$n ;; p*) export EXTCMP_THR_PG_WORKERS=$n ;; esac
  CSYS=$(cfg_sys $c)
}
