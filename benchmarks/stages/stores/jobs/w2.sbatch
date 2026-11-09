#!/bin/bash
#SBATCH -J ps-w2
#SBATCH -N 1
#SBATCH --signal=B:USR1@900
#SBATCH -o w2-%j.out
# K10M-d0/d1/d10 (belief-time family B) on DuckDB (SQL), ClickHouse, PostgreSQL, Neo4j and Memgraph, as the 1M
# parameter files), with the job-safe kit of this worktree. Differences from the 1M check, both required by the
# read the native store's version rows); it is not answer-checked, so the kit's gate (vs native) is not the
# verdict. The systems are compared with each other (scripts/ps_crosscheck.py, the kit's cell identity and verdict
# rule). Caps: the P1 ceiling 10 s (PostgreSQL / ClickHouse cancel at it), the answer-check cap 120 s (server
# timeout and client alarm), EXTCMP_WINDOW_TIMEOUT_S per verify window (default 1,800 s, as at 1M).
# POINTS (default "K10M K10M-d0 K10M-d1 K10M-d10"); SYSTEMS (default the five baselines); GNOMON=1 (default): then
# native and duckdb-storage on the pinned engine, on the same instances, in the cross-check.
set -uo pipefail
source ${GNOMON_BENCH_ROOT}/prep-stores/site.env
unset EXTCMP_NUMA_SERVER EXTCMP_NUMA_CLIENT
export EXTCMP_MG_MALLOC_CONF=retain:true   # Memgraph's jemalloc keeps freed extents mapped
# the final-engine tree and its venv: the native
# reference store the baselines load from is replayed by the pinned engine (the version rows are engine-independent)
export EXTCMP_REPO=${W2_REPO:-$P/srcf}
export EXTCMP_ROOT=${LOCAL:-${TMPDIR:-/tmp}}/extcmp TMPDIR_OVERRIDE=${LOCAL:-${TMPDIR:-/tmp}}/extcmp/scratch EXTCMP_DURABLE=1
mkdir -p $EXTCMP_ROOT/{results,eventlogs,stores,srv,scratch}
OUTP=$W2/check10m/campaign; mkdir -p $OUTP/results
POINTS=${POINTS:-"K10M K10M-d0 K10M-d1 K10M-d10"}; SYSTEMS=${SYSTEMS-"duckdbsql clickhouse postgres neo4j memgraph"}
GN_SYSTEMS=${GN_SYSTEMS-"native duckdb"}
say() { echo "=== $* $(date -u +%FT%TZ)"; }
sync_out() { mkdir -p $OUTP/results && cp -au $EXTCMP_ROOT/results/. $OUTP/results/; }
export EXTCMP_SYNC_HOOK="mkdir -p $OUTP/results && cp -au $EXTCMP_ROOT/results/. $OUTP/results/"
unset EXTCMP_GATE_DIR                       # one job per node share: the job's own ports and instance names (lib/ports.sh)
on_warning() { echo "=== time limit near at $(date -u +%FT%TZ): syncing the records"; sync_out; }
trap on_warning USR1
say "host $(hostname) job $SLURM_JOB_ID cpus ${SLURM_CPUS_ON_NODE:-?} points [$POINTS] systems [$SYSTEMS] kit $(git -C $P/src rev-parse --short HEAD) dirty=$(git -C $P/src status --porcelain --untracked-files=no | wc -l) repo $(cat $W2/engine-build.txt | tr '\n' ' ')"
L=$W2/check1m/campaign/eventlogs
cp -a $L/synth-10000000.eventlog.jsonl $L/synth-10000000.eventlog.meta.json $L/synth-10000000.eventlog.extcmp-params.json \
      $L/K10M-d0.eventlog.jsonl $L/K10M-d1.eventlog.jsonl $L/K10M-d10.eventlog.jsonl $L/K10M-d*.eventlog.beliefq-params.json $EXTCMP_ROOT/eventlogs/
( cd $EXTCMP_ROOT/eventlogs
  # d0 is the base log itself (a hard link; SHA256SUMS lists it as powerlaw-10000000)
  for p in "powerlaw-10000000 synth-10000000" "powerlaw-10000000 K10M-d0" "W2-10M-d1 K10M-d1" "W2-10M-d10 K10M-d10"; do set -- $p
    want=$(awk -v f="$1.eventlog.jsonl" '$2 == f {print $1}' $W2/eventlogs/SHA256SUMS); got=$(sha256sum $2.eventlog.jsonl | cut -d' ' -f1)
    [ -n "$want" ] && [ "$want" = "$got" ] && echo "log $2 = $1 sha256 $got OK" || { echo "LOG MISMATCH $2 ($got vs $want)"; exit 1; }
  done ) || exit 1
export P1_SIZE=10000000 P1_PARAMS_DIR=$W2/params EXTCMP_P1_CEILING_S=10 EXTCMP_SERVER_LIMIT_S=10 \
       EXTCMP_SERVER_TIMEOUT_S=120 EXTCMP_CALL_CEILING_S=120 EXTCMP_P1_SLOW_S=1.0 EXTCMP_WINDOW_TIMEOUT_S=${WTO:-1800}
PYB=$EXTCMP_REPO/.venv/bin/python
for pt in $POINTS; do
  if [ "${PRESEED:-0}" = 1 ]; then
    # complete verify records of an earlier job (per family) are seeded, so the kit skips those windows ("skip (done)")
    # and only the missing families run; loads still run (the stores are not kept between jobs)
    mkdir -p $EXTCMP_ROOT/results/$pt
    for f in $OUTP/results/$pt/verify-*.json; do case $(basename $f) in verify-gn-*) ;; *) cp -p $f $EXTCMP_ROOT/results/$pt/; echo "seeded $(basename $f)";; esac; done
  fi
  if [ -n "$SYSTEMS" ]; then
  # loads without the window timeout (a 10M load can outlast it; window.sh's own load budget applies), verify with it
  say "point $pt: loads (native = the reference store only)"
  EXTCMP_WINDOW_TIMEOUT_S= P1_POINTS=$pt P1_SYSTEMS="native $SYSTEMS" "$KIT/run_p1.sh" --steps loads; echo "loads rc=$?"
  say "point $pt: answer check (every instance, every baseline)"
  P1_POINTS=$pt P1_SYSTEMS="$SYSTEMS" "$KIT/run_p1.sh" --steps verify
  echo "verify rc=$? (3 = the kit's gate found no native reference: expected, no Gnomon cell is checked here)"
  fi
  "$KIT/lib/servers.sh" stopall
  if [ "${GNOMON:-1}" = 1 ] && [ -n "$GN_SYSTEMS" ]; then
    # Gnomon on the pinned engine: native and Gnomon on DuckDB storage, loaded and checked with
    # the final-engine tree in a second kit root, on the same instances; their verify records join
    # the baselines' as verify-gn-<system>.json
    GR=${LOCAL:-${TMPDIR:-/tmp}}/extcmp-g; mkdir -p $GR/{results,eventlogs,stores,srv,scratch}
    [ -e $GR/eventlogs/synth-10000000.eventlog.jsonl ] || cp -al $EXTCMP_ROOT/eventlogs/. $GR/eventlogs/
    say "point $pt: Gnomon $(git -C $P/srcf rev-parse --short HEAD) so $(sha256sum $P/srcf/gnomon/_engine*.so | cut -c1-16): loads and answer check"
    ( export EXTCMP_ROOT=$GR EXTCMP_REPO=$P/srcf TMPDIR_OVERRIDE=$GR/scratch GNOMON_MATCH_THREADS=${GNOMON_MATCH_THREADS:-32}
      export EXTCMP_SYNC_HOOK="mkdir -p $OUTP/results-gnomon && cp -au $GR/results/. $OUTP/results-gnomon/"
      EXTCMP_WINDOW_TIMEOUT_S= P1_POINTS=$pt P1_SYSTEMS="$GN_SYSTEMS" "$KIT/run_p1.sh" --steps loads; echo "gnomon loads rc=$?"
      P1_POINTS=$pt P1_SYSTEMS="$GN_SYSTEMS" "$KIT/run_p1.sh" --steps verify; echo "gnomon verify rc=$?" )
    mkdir -p $OUTP/results-gnomon && cp -au $GR/results/. $OUTP/results-gnomon/
    mkdir -p $EXTCMP_ROOT/results/$pt   # (a Gnomon-only rerun has no baseline results directory yet)
    for f in $GR/results/$pt/verify-*.json $GR/results/$pt/verify-*.cells.jsonl; do [ -e "$f" ] && cp "$f" $EXTCMP_ROOT/results/$pt/verify-gn-$(basename ${f#*/verify-}); done
    rm -rf $GR/stores/* $GR/srv/* 2>/dev/null
  fi
  if [ "${REUSE:-0}" = 1 ]; then
    # a rerun of some systems: the earlier jobs' complete verify records of the others join the cross-check
    mkdir -p $EXTCMP_ROOT/results/$pt
    for f in $OUTP/results/$pt/verify-*.json $OUTP/results/$pt/verify-*.cells.jsonl; do [ -e "$f" ] || continue; b=$(basename $f); [ -e $EXTCMP_ROOT/results/$pt/$b ] || { cp -p $f $EXTCMP_ROOT/results/$pt/$b; echo "reused $b"; }; done
  fi
  $PYB $P/src/scripts/ps_crosscheck.py --dir $EXTCMP_ROOT/results/$pt --base duckdbsql --ceiling 10 \
       --out $EXTCMP_ROOT/results/$pt/crosscheck.json | tee $EXTCMP_ROOT/results/$pt/crosscheck.txt
  sync_out
  rm -rf $EXTCMP_ROOT/stores/* $EXTCMP_ROOT/srv/* 2>/dev/null
done
sync_out
say done
