# W1 timed stage: shared setup of the stage's jobs (sourced by every job).
#   commits may add only this stage's own directory benchmarks/stages/w1/). The job refuses to run on any other
#   kit code or any other engine build.
#   Gnomon as shipped: admission on (no cost-check bypass), every design switch at its default, GNOMON_MATCH_THREADS=32.
#   Protocol (the kit's parameter-file path, as the pilot's W2 jobs): per window a priming call (prime_cap rule:
#   unmeasured, outside the ceiling, killed at 60 s, server caps 58 s for it, replayed after every worker restart),
#   2 warm-ups + 10 timed (1 + 3 above 1 s), the 10 s ceiling on the client call (EXTCMP_P1_CEILING_S), server-side
#   limit 10 s (PostgreSQL, ClickHouse, DuckDB (SQL)) under the 30 s client alarm (server timeout 28 s = alarm - 2 s),
#   Gnomon, Gnomon on DuckDB storage and DuckDB (SQL) in killable in-process workers (EXTCMP_INPROC_WORKER=1).
set -uo pipefail
W1=${W1_STAGE_ROOT:-${GNOMON_BENCH_ROOT}/final/w1}; E=${GNOMON_BENCH_ROOT}
source $W1/site.env
export EXTCMP_JOB_SAFE=1 EXTCMP_DURABLE=1 GNOMON_MATCH_THREADS=32
unset EXTCMP_ENGINE_TIME EXTCMP_PG_STATS EXTCMP_GATE_DIR EXTCMP_PARAMS EXTCMP_EXCLUDE EXTCMP_VERIFY_FAMILY \
      EXTCMP_BELIEFQ_INDEXES EXTCMP_P1_REPS EXTCMP_R10M_FROM EXTCMP_R10M_SKIP P1_VTAG EXTCMP_YIELD
REPO=$EXTCMP_REPO; PYV=$REPO/.venv/bin/python
STG=benchmarks/stages/w1; PFD=benchmarks/compare/kit/kit/pilot-final
TOOLS=$REPO/$STG/tools; KFT=$KIT/tools; P1DIR=$(cd $KIT/../p1 && pwd)
V3LOGS=$E/v3/eventlogs; SPDIR=$W1/params-single
# W1_STORE_DIR: where stores are persisted and restored (the smoke jobs use their own); W1_FAMS: registry families
# of a smoke run (unset: all)
STORE_DIR=${W1_STORE_DIR:-$W1/stores}
KITHEAD=${EXTCMP_KITHEAD:-HEAD}
SYSTEMS="native duckdbsql clickhouse postgres duckdb"     # run_p1.sh's default order (rotation base)
utc() { date -u +%FT%TZ; }
say() { echo "=== $* $(utc)"; }
size_of() { case $1 in K1M*) echo 1000000 ;; K10M*) echo 10000000 ;; esac; }
log_of() { case $1 in K1M) echo synth-1000000 ;; K10M) echo synth-10000000 ;; *) echo $1 ;; esac; }
fams_of() { case $1 in *-d*) echo B ;; *) echo "${W1_FAMS:-P T S W D N R M}" ;; esac; }
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
  return 0
}
hostinfo() { echo "host $(hostname) cores $(nproc) job $SLURM_JOB_ID partition ${SLURM_JOB_PARTITION:-?}; numa client=[${EXTCMP_NUMA_CLIENT:-}] server=[${EXTCMP_NUMA_SERVER:-}]; Gnomon env: $(env | grep ^GNOMON_ | sort | tr '\n' ' ')"; }
# the two cap settings: v3's answer check (no ceiling: every cell answers, so every hash is compared with v3), and the
# timed protocol (the 10 s ceiling)
proto_v3() { export EXTCMP_SERVER_TIMEOUT_S=3660 EXTCMP_CALL_CEILING_S=3600 EXTCMP_VERIFY_CAP_S=3662
             unset EXTCMP_SERVER_LIMIT_S EXTCMP_P1_CEILING_S EXTCMP_P1_SLOW_S EXTCMP_INPROC_WORKER EXTCMP_WINDOW_TIMEOUT_S; }
proto_p1() { export EXTCMP_P1_CEILING_S=10 EXTCMP_SERVER_LIMIT_S=10 EXTCMP_CALL_CEILING_S=30 EXTCMP_SERVER_TIMEOUT_S=28 \
                    EXTCMP_VERIFY_CAP_S=30 EXTCMP_P1_SLOW_S=1.0 EXTCMP_INPROC_WORKER=1 EXTCMP_PRIME_KILL_S=60 \
                    EXTCMP_WINDOW_TIMEOUT_S=3600; }
point_env() {   # tag: the belief-axis indexes and the verify family of a density point (run_p1.sh / run_duckdbsql.sh)
  case $1 in *-d*) export EXTCMP_BELIEFQ_INDEXES=1 EXTCMP_VERIFY_FAMILY=B ;; *) unset EXTCMP_BELIEFQ_INDEXES EXTCMP_VERIFY_FAMILY ;; esac; }
# node-local campaign root; one results directory per result root (v3gate, sp, p1, t32sp, t32p1, ...) behind the
# symlink $ROOT/results, so every root shares the same stores, server data and logs
setup_root() {   # dir
  ROOT=$1; export EXTCMP_ROOT=$ROOT TMPDIR_OVERRIDE=$ROOT/scratch
  mkdir -p $ROOT/{eventlogs,stores,srv,scratch}; STORES=$ROOT/stores; SRV=$ROOT/srv; LOGS=$ROOT/eventlogs
}
useroot() {   # name: results go to $ROOT/results-<name>, synced to $OUT/<name> after every window (cp -au)
  CUR=$1; mkdir -p $ROOT/results-$CUR $OUT/$CUR; ln -sfn results-$CUR $ROOT/results
  export EXTCMP_SYNC_HOOK="mkdir -p $OUT/$CUR && cp -au $ROOT/results-$CUR/. $OUT/$CUR/"
  RES=$ROOT/results-$CUR; echo "--- result root $CUR -> $OUT/$CUR"
}
syncall() { local d; for d in $ROOT/results-*; do [ -d "$d" ] && mkdir -p $OUT/${d##*/results-} && cp -au $d/. $OUT/${d##*/results-}/; done; }
getlogs() {   # tags...: node-local copies of the frozen logs, checked against v3's SHA256SUMS
  local t n pat=""
  for t in "$@"; do n=$(log_of $t); cp -a $V3LOGS/$n.eventlog.* $LOGS/; pat="$pat|$n"; done
  ( cd $LOGS && grep -E "  (${pat#|})\.eventlog\.jsonl$" $V3LOGS/SHA256SUMS | sha256sum -c ) || { echo "FROZEN LOG CHECK FAILED"; exit 2; }
}
win() {   # window.sh with the window timeout (EXTCMP_WINDOW_TIMEOUT_S, when set)
  ${EXTCMP_WINDOW_TIMEOUT_S:+timeout -k 60 $EXTCMP_WINDOW_TIMEOUT_S} "$KIT/lib/window.sh" "$@" || echo "WINDOW FAILED $* $(utc)"; }
load_ok() {   # record: a completed load (not over its budget)
  $PYV -c "import json,sys; d=json.load(open(sys.argv[1])); sys.exit(0 if 'load' in d and not d.get('load exceeded budget') else 1)" "$1" 2>/dev/null; }
save() {   # parent name tag: tar.zst of a store or server data directory to $W1/stores/<tag>/, with its sha256
  local o=$STORE_DIR/$3; mkdir -p $o
  tar -C "$1" -cf - "$2" | zstd -1 -T32 -q -o "$o/$2.tar.zst.tmp" && mv "$o/$2.tar.zst.tmp" "$o/$2.tar.zst" && \
    ( cd $o && sha256sum $2.tar.zst > $2.tar.zst.sha256 ) && echo "saved $o/$2.tar.zst $(du -sh $o/$2.tar.zst | cut -f1) $(utc)"; }
restore() {   # tag name parent: checks the archive's sha256, then unpacks (from RESTORE_FROM, default STORE_DIR)
  local o=${RESTORE_FROM:-$STORE_DIR}/$1 t0=$(date +%s)
  ( cd $o && sha256sum -c --quiet $2.tar.zst.sha256 ) || { echo "RESTORE CHECK FAILED $o/$2.tar.zst"; return 1; }
  mkdir -p "$3"; zstd -q -d -c $o/$2.tar.zst | tar -C "$3" -xf - && echo "restored $2 in $(( $(date +%s) - t0 )) s"; }
parent_of() { case $1 in native|duckdb|duckdbsql) echo $STORES ;; *) echo $SRV ;; esac; }

# ---- C3 confirmation. C3_CONFIRM=1 is the Gnomon-only mode; unset, the stage
# runs exactly as before. In the Gnomon-only mode:
#   gate:  Gnomon (native, rebuilt by replay) and Gnomon on DuckDB storage (duckdb, restored) only; no 32-thread roots; the
#          gate tool compares with the recorded gate jobs (C3_REF_GATE, tools/w1_gate.py --ref-gate);
#          tools/c3_drift.py: DuckDB (SQL) runs only its sampled cells, through EXTCMP_EXCLUDE); no 32-thread roots; the
#          engine-time pass for Gnomon only; the stop rule reads the recorded gate's records (the competitors answered there).
#   Stores of C3_RESTORE_SYSTEMS (default "duckdb duckdbsql") are restored from C3_STORE_SRC (the recorded stage's store
#   directory, read-only) when this stage's STORE_DIR has none; new stores (native) are saved to STORE_DIR only.
#   EXTCMP_KITHEAD must be set: the guard checks the kit against it.
C3=${C3_CONFIRM:-0}
GSYS=$SYSTEMS; TSYS=$SYSTEMS; ESYS="native duckdbsql postgres"
C3T=$REPO/benchmarks/stages/confirm/tools
if [ "$C3" = 1 ]; then
  : "${EXTCMP_KITHEAD:?C3 mode: EXTCMP_KITHEAD must be set (stage site.env)}" "${C3_STORE_SRC:?C3 mode: C3_STORE_SRC must be set}"
  GSYS="native duckdb"; TSYS="native duckdb duckdbsql"; ESYS="native"
fi
C3_RESTORE=${C3_RESTORE_SYSTEMS:-duckdb duckdbsql}
c3_has() { case " $C3_RESTORE " in *" $1 "*) return 0 ;; esac; return 1; }
store_src() {   # tag sys: the store directory holding <sys>-<tag>.tar.zst (STORE_DIR; in C3 mode then C3_STORE_SRC)
  if [ -e $STORE_DIR/$1/$2-$1.tar.zst ]; then echo $STORE_DIR; return 0; fi
  if [ "$C3" = 1 ] && c3_has $2 && [ -e $C3_STORE_SRC/$1/$2-$1.tar.zst ]; then echo $C3_STORE_SRC; return 0; fi
  return 1; }
c3_ref_dir() {   # root tag: the recorded gate's record directory (C3_REF_GATE) of this root and point
  local g; for g in ${C3_REF_GATE:-}; do [ -d $g/$1/$2 ] && { echo $g/$1/$2; return 0; }; done; return 1; }
c3_drift_check() { [ "$C3" = 1 ] || return 0; : "${C3_DRIFT_DIR:?C3 mode: C3_DRIFT_DIR must be set}"
  ( cd $C3_DRIFT_DIR && sha256sum -c SHA256SUMS ) || { echo "C3 DRIFT FILES DO NOT MATCH SHA256SUMS"; return 2; }; }
