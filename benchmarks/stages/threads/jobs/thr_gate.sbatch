#!/bin/bash
# Thread-matched control, answer-check job (untimed). Every thread configuration's answers must equal the reference answers
# already validated in the W1 / W2 stages (Gnomon at 32 threads, DuckDB (SQL) at 64 threads, PostgreSQL at 4 workers); the
# job exits NON-ZERO on any FAIL (a differing answer, a missing record, an unexpected error, an override that did not reach the
# system, a job stopped at its time limit), so the timed job, submitted --dependency=afterok, does not start.
#   THR_WLS (sbatch --export): the workloads, "w1 w2" (default both)
# Per workload and per input (w1: K10M and K10M-d1; w2: K10M), on a whole RM node (the 10 s ceiling decides which cells are
# timed, so the answer check needs the timed conditions; the whole node costs ~130 SU an hour):
#   1. restores the stage's persisted stores (Gnomon, DuckDB (SQL), PostgreSQL) from the W1 / W2 stage directories, read-only;
#   2. for each of the ten configurations, the answer check of every instance (window.sh verify, one window per family,
#      each cut at THR_VERIFY_WINDOW_S = 2700 s; the instances a cut window did not reach are recorded "unreached");
#      PostgreSQL windows also log each statement's plan (auto_explain, no analyze) to record the workers planned;
#   3. tools/thr_gate.py compares every cell with the reference records and writes each configuration's excluded.json (the
#      cells the timed job does not time: wrong, errored, refused, above the ceiling, unreached) and the gate report.
# Nothing is timed.
#SBATCH --exclusive
#SBATCH -N 1
#SBATCH --ntasks-per-node=128
#SBATCH --signal=B:USR1@600
#SBATCH -J thr-gate
#SBATCH -o %x-%j.out
source ${THR_STAGE_ROOT:-${GNOMON_BENCH_ROOT}/final/thr}/src/benchmarks/stages/threads/jobs/thr_lib.sh
guard; hostinfo
THR_WLS=${THR_WLS:-"w1 w2"}; say "gate start workloads [$THR_WLS]"
OUTB=$THR/results/${THR_SMOKE:+smoke-}gate-$SLURM_JOB_ID; mkdir -p $OUTB
child=""
on_usr1() { echo "=== time limit near $(utc): stopping (the job body syncs its records on TERM)"; [ -n "$child" ] && kill -TERM -- -$child 2>/dev/null
            for _ in $(seq 1 100); do kill -0 $child 2>/dev/null || break; sleep 3; done
            "$KIT/lib/servers.sh" stopall; echo "STOPPED at the time limit $(utc)" > $OUTB/STOPPED; exit 5; }
trap on_usr1 USR1 TERM
export OUTB THR_WLS
setsid bash $REPO/$STG/jobs/thr_gate_main.sh &
child=$!
wait $child; rc=$?
"$KIT/lib/servers.sh" stopall
say "gate job done rc=$rc"
exit $rc
