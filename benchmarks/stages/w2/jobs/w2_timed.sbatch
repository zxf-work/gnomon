#!/bin/bash
# W2 timed stage, timed job: restores the gate job's stores and answer-check records, then times.
#   W2_POINTS (sbatch --export): T1 "K1M K1M-d0 K1M-d1 K1M-d10", T2a "K10M", T2b "K10M-d0 K10M-d1 K10M-d10";
#   W2_GATE: the gate jobs' result dirs (space-separated), whose records (verify, excluded.json) the timed blocks read;
#   W2_R10M_JOB (10M points only): the job id of the 1M timed job (T1), whose results dir is the source of the R-10M skip lists
#   (a (system, query family) pair whose every instance was censored at the 1M timed run is skipped at 10M and recorded
#   "> 10 s (R-10M ...)", for every system alike). The 10M jobs are submitted --dependency=afterok on T1 AND the gates.
# Per point, whole RM node, nothing but this job on it:
#   p1 (the W2 parameter file): two rotations, system order rotated per family block and flipped in rotation 2 (run_p1.sh's
#       schedule), the timed pass uninstrumented for Gnomon, Gnomon on DuckDB storage, DuckDB (SQL), PostgreSQL and LadybugDB,
#       probes on for ClickHouse, Neo4j and Memgraph;
#   t32p1: DuckDB (SQL) at 32 threads, two rotations (the better-of-two rule);
#   rotE: the engine-time pass (EXTCMP_ENGINE_TIME=1, EXTCMP_P1_REPS=3, as the W1 stage: 2 warm-ups + 3 timed, 1 + 3 above
#       1 s; PostgreSQL restarted with EXTCMP_PG_STATS=1) for Gnomon, DuckDB (SQL), PostgreSQL;
#   after every Gnomon window tools/w2_stopcheck.py applies the stop rule (a Gnomon error, or a refusal of a call another
#   system answers inside 10 s, stops the job, exit 4). A job stopped at its time limit exits 5, so afterok blocks the 10M jobs.
#SBATCH --exclusive
#SBATCH -N 1
#SBATCH --ntasks-per-node=128
#SBATCH --signal=B:USR1@900
#SBATCH -J w2-timed
#SBATCH -o %x-%j.out
source ${W2_STAGE_ROOT:-${GNOMON_BENCH_ROOT}/final/w2}/src/benchmarks/stages/w2/jobs/w2_lib.sh
guard; hostinfo
: "${W2_POINTS:?}" "${W2_GATE:?}"; say "timed start points [$W2_POINTS] gate records [$W2_GATE] r10m 1M job [${W2_R10M_JOB:-none}]"
OUT=$W2/results/${W2_SMOKE:+smoke-}timed-$SLURM_JOB_ID; mkdir -p $OUT
setup_root ${LOCAL:-${TMPDIR:-/tmp}}/w2t
child=""
on_usr1() { echo "=== time limit near $(utc): stopping, syncing"; [ -n "$child" ] && kill -TERM -- -$child 2>/dev/null; sleep 5
            "$KIT/lib/servers.sh" stopall; syncall; echo "STOPPED at the time limit $(utc)" > $OUT/STOPPED; exit 5; }
trap on_usr1 USR1 TERM
export OUT ROOT W2_POINTS STORE_DIR W2_GATE W2_R10M_JOB
[ -n "${W2_R10M_JOB:-}" ] && export W2_R10M_ROOT=$W2/results/timed-$W2_R10M_JOB
setsid bash $REPO/$STG/jobs/w2_timed_main.sh &
child=$!
wait $child; rc=$?
"$KIT/lib/servers.sh" stopall; syncall
say "timed job done rc=$rc"
exit $rc
