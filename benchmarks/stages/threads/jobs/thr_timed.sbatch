#!/bin/bash
# Thread-matched control, timed job: the W1/W2 timed protocol on the ten configurations, on a whole RM node, nothing else on it.
#   THR_WLS   (sbatch --export): "w1 w2" (default both)
#   THR_GATE  (sbatch --export, required): the gate job's results dir ($THR/results/gate-<jobid>), whose per-configuration
#             excluded.json files say which cells are not timed (and why)
# Per workload and input (w1: K10M, then K10M-d1; w2: K10M): restore the stores; two rotations over the ten configurations, the
# configuration order rotated per family block and flipped in rotation 2 (run_p1.sh's schedule, 10 configurations in place of
# the systems); then the engine-time pass (EXTCMP_ENGINE_TIME=1, EXTCMP_P1_REPS=3 as in W1/W2; PostgreSQL restarted with
# EXTCMP_PG_STATS=1) on every configuration. After each window: tools/thr_check.py (the configuration was applied: the record's
# own settings say so; else the job stops, exit 7) and, after a Gnomon window, tools/thr_stopcheck.py (the Gnomon stop rule at
# the configuration g32, exit 4). A job stopped at its time limit exits 5.
#SBATCH --exclusive
#SBATCH -N 1
#SBATCH --ntasks-per-node=128
#SBATCH --signal=B:USR1@900
#SBATCH -J thr-timed
#SBATCH -o %x-%j.out
source ${THR_STAGE_ROOT:-${GNOMON_BENCH_ROOT}/final/thr}/src/benchmarks/stages/threads/jobs/thr_lib.sh
guard; hostinfo
: "${THR_GATE:?}"; THR_WLS=${THR_WLS:-"w1 w2"}; say "timed start workloads [$THR_WLS] gate records [$THR_GATE]"
OUTB=$THR/results/${THR_SMOKE:+smoke-}timed-$SLURM_JOB_ID; mkdir -p $OUTB
child=""
on_usr1() { echo "=== time limit near $(utc): stopping (the job body syncs its records on TERM)"; [ -n "$child" ] && kill -TERM -- -$child 2>/dev/null
            for _ in $(seq 1 100); do kill -0 $child 2>/dev/null || break; sleep 3; done
            "$KIT/lib/servers.sh" stopall; echo "STOPPED at the time limit $(utc)" > $OUTB/STOPPED; exit 5; }
trap on_usr1 USR1 TERM
export OUTB THR_WLS THR_GATE
setsid bash $REPO/$STG/jobs/thr_timed_main.sh &
child=$!
wait $child; rc=$?
"$KIT/lib/servers.sh" stopall
say "timed job done rc=$rc"
exit $rc
