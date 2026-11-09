#!/bin/bash
# after rotation 1, before its engine-time pass). Runs only the engine-time pass (1 warm-up + 3 timed, instances 0-2) for Gnomon,
# DuckDB (SQL, 64 threads) and PostgreSQL (EXTCMP_PG_STATS=1) on the cells every instance of which answered in rotation 1 of the
# timed job named by W3_ROT1_JOB (read-only); stores restored sha256-checked as in w3_timed.sbatch; results go to their own
# directory $W3/results/etime-<job>/rotE, rotation 1 results are not touched. The body is w3_timed_main.sh with W3_ONLY_ENGINE_TIME=1.
#SBATCH --exclusive
#SBATCH -N 1
#SBATCH --ntasks-per-node=128
#SBATCH --signal=B:USR1@900
#SBATCH -J w3-etime
#SBATCH -o %x-%j.out
source ${W3_STAGE_ROOT:-${GNOMON_BENCH_ROOT}/final/w3}/src/benchmarks/stages/w3/jobs/w3_lib.sh
guard; hostinfo
: "${W3_POINT:?}" "${W3_ROT1_JOB:?}"; say "engine-time pass start point [$W3_POINT] rotation 1 from timed-$W3_ROT1_JOB"
pt_config $W3_POINT || exit 2
[ "$(cat $W3/gate/gate-$PT.rc 2>/dev/null)" = 0 ] || { echo "NO PASSING GATE VERDICT for $PT: refusing"; exit 2; }
W3_ROT1_DIR=$W3/results/timed-$W3_ROT1_JOB/rot1; [ -d $W3_ROT1_DIR ] || { echo "no $W3_ROT1_DIR"; exit 2; }
OUT=$W3/results/etime-$SLURM_JOB_ID; mkdir -p $OUT
setup_root ${LOCAL:-${TMPDIR:-/tmp}}/w3t
child=""
on_usr1() { echo "=== time limit near $(utc): stopping, syncing"; [ -n "$child" ] && kill -TERM -- -$child 2>/dev/null; sleep 5
            "$KIT/lib/servers.sh" stopall; cp -au ${LOCAL:-${TMPDIR:-/tmp}}/w3t/res/. $OUT/ 2>/dev/null; echo "STOPPED at the time limit $(utc)" > $OUT/STOPPED; exit 5; }
trap on_usr1 USR1 TERM
export OUT W3_POINT W3_ROT1_DIR W3_ONLY_ENGINE_TIME=1
setsid bash $REPO/$STG/jobs/w3_timed_main.sh &
child=$!
wait $child; rc=$?
"$KIT/lib/servers.sh" stopall; cp -au ${LOCAL:-${TMPDIR:-/tmp}}/w3t/res/. $OUT/ 2>/dev/null
say "engine-time job done rc=$rc"
exit $rc
