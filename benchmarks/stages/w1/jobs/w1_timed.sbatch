#!/bin/bash
# W1 timed stage, timed job: restores the gate job's stores and answer-check records, then times.
#   W1_POINTS (sbatch --export): T-A "K1M K10M", T-B "K10M-d0 K10M-d1 K10M-d10"; W1_GATE: the gate jobs' result dirs
#   (space-separated), whose records (verify, excluded.json) the timed blocks read.
# Per point, whole RM node, nothing but this job on it:
#   sp  (single-parameter instances) and p1 (curated P1 instances, K10M / K10M-d0 / K10M-d10): two rotations,
#       system order rotated per family block and flipped in rotation 2 (run_p1.sh's schedule), timed pass
#       uninstrumented for Gnomon, DuckDB (SQL) and PostgreSQL, probes on for ClickHouse;
#   t32sp, t32p1: DuckDB (SQL) at 32 threads, two rotations (the better-of-two rule);
#   rotE: the engine-time pass (EXTCMP_ENGINE_TIME=1, EXTCMP_P1_REPS=3: 2 warm-ups + 3 timed, 1 + 3 above 1 s;
#       PostgreSQL restarted with EXTCMP_PG_STATS=1) for Gnomon, DuckDB (SQL), PostgreSQL;
#   after every Gnomon window tools/w1_stopcheck.py applies the stop rule (a Gnomon error, or a refusal of a call
#   another system answers inside 10 s, stops the job).
#SBATCH --exclusive
#SBATCH -N 1
#SBATCH --ntasks-per-node=128
#SBATCH --signal=B:USR1@900
#SBATCH -J w1-timed
#SBATCH -o %x-%j.out
source ${W1_STAGE_ROOT:-${GNOMON_BENCH_ROOT}/final/w1}/src/benchmarks/stages/w1/jobs/w1_lib.sh
guard; hostinfo
: "${W1_POINTS:?}" "${W1_GATE:?}"; say "timed start points [$W1_POINTS] gate records [$W1_GATE]"
OUT=$W1/results/${W1_SMOKE:+smoke-}timed-$SLURM_JOB_ID; mkdir -p $OUT
setup_root ${LOCAL:-${TMPDIR:-/tmp}}/w1t
child=""
on_usr1() { echo "=== time limit near $(utc): stopping, syncing"; [ -n "$child" ] && kill -TERM -- -$child 2>/dev/null; sleep 5
            "$KIT/lib/servers.sh" stopall; syncall; echo "STOPPED at the time limit $(utc)" > $OUT/STOPPED; exit 0; }
trap on_usr1 USR1 TERM
export OUT ROOT W1_POINTS STORE_DIR W1_GATE
setsid bash $REPO/$STG/jobs/w1_timed_main.sh &
child=$!
wait $child; rc=$?
"$KIT/lib/servers.sh" stopall; syncall
say "timed job done rc=$rc"
exit $rc
