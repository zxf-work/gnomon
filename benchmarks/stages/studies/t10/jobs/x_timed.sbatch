#!/bin/bash
# Timed job, whole RM node, nothing else on it: restores the stores and the gate job's records (X_GATE), then per point (K10M-d10 first):
#   reg: two rotations, system order rotated per family block and flipped in rotation 2 (run_p1.sh's schedule), uninstrumented except ClickHouse's probes;
#   t32reg: DuckDB (SQL) at 32 threads, two rotations; the engine-time pass (EXTCMP_ENGINE_TIME=1, EXTCMP_P1_REPS=3; PostgreSQL with EXTCMP_PG_STATS=1)
#   for Gnomon, DuckDB (SQL), PostgreSQL. After each Gnomon window the stop rule (exit 4). R-10M at X_SET w2. Exit 5 at the time limit, records kept.
#SBATCH --exclusive
#SBATCH -N 1
#SBATCH --ntasks-per-node=128
#SBATCH --signal=B:USR1@900
#SBATCH -J c4-x-timed
source ${NEXT_STAGE_ROOT:?}/src/benchmarks/stages/studies/t10/jobs/x_lib.sh
guard; hostinfo
: "${X_GATE:?X_GATE must name the gate job results directory whose verdict was read}"
say "timed start set $X_SET points [$POINTS] families [$FAMS] gate records [$X_GATE]"
OUT=$NEXT_STAGE_ROOT/results/${SM}timed-$X_SET-$SLURM_JOB_ID; mkdir -p $OUT
x_root ${LOCAL:?}/xt
rss_start
child=""
on_usr1() { echo "=== time limit near $(utc): stopping, syncing"; [ -n "$child" ] && kill -TERM -- -$child 2>/dev/null; sleep 5
            "$KIT/lib/servers.sh" stopall; syncall; rss_stop; echo "STOPPED at the time limit $(utc)" > $OUT/STOPPED; exit 5; }
trap on_usr1 USR1 TERM
export OUT ROOT X_GATE
setsid bash $XD/jobs/x_timed_main.sh &
child=$!
wait $child; rc=$?
"$KIT/lib/servers.sh" stopall; syncall; rss_stop
$PYV $XT/x_summary.py --set $X_SET --timed $OUT --tags "$(echo $POINTS | tr ' ' ',')" --out $OUT/summary || echo "summary failed: re-run it from the records"
say "timed job done rc=$rc"
exit $rc
