#!/bin/bash
# W1 timed stage, gate job (untimed): loads, store persistence and the answer gate, before any timing.
#   W1_POINTS (sbatch --export): G-A "K1M K10M K10M-d0 K10M-d1", G-B "K10M-d10" (after B0: the duckdb store is restored)
# Per point, whole RM node:
#   1. loads by the v3 method (window.sh load; native first, the baselines from the native store), each store
#      persisted to $W1/stores/<tag>/ (W1_STORE_DIR) only after a complete load (rc 0, no "load exceeded budget");
#   2. v3gate: the answer check exactly as v3 ran it (window.sh verify, no parameter file, v3's caps), Gnomon,
#      Gnomon on DuckDB storage, DuckDB (SQL) 64 threads, ClickHouse, PostgreSQL; v3gate-t32: DuckDB (SQL) 32 threads;
#   3. sp: the single-parameter instances through the parameter-file path at the timed protocol's caps
#      (params-single/params-<tag>.json, one instance per id = the registry's own arguments) + the kit gate
#      (extcmp_p1.py gate: per-instance exclusion list for the timed jobs);
#   4. p1: the curated P1 instances (K10M, K10M-d0, K10M-d10; kit p1/params-*.json, run_p1.sh --steps verify);
#   5. t32sp, t32p1: the same two for DuckDB (SQL) at 32 threads with their own Gnomon reference;
#   6. tools/w1_gate.py: every answer against v3 / the P1 study / Gnomon; the verdict goes to $W1/gate/.
# Nothing is timed. The timed jobs are submitted only after every gate job has passed.
#SBATCH --exclusive
#SBATCH -N 1
#SBATCH --ntasks-per-node=128
#SBATCH --signal=B:USR1@600
#SBATCH -J w1-gate
#SBATCH -o %x-%j.out
source ${W1_STAGE_ROOT:-${GNOMON_BENCH_ROOT}/final/w1}/src/benchmarks/stages/w1/jobs/w1_lib.sh
guard; hostinfo
: "${W1_POINTS:?}"; say "gate start points [$W1_POINTS]"
OUT=$W1/results/${W1_SMOKE:+smoke-}gate-$SLURM_JOB_ID; mkdir -p $OUT
setup_root ${LOCAL:-${TMPDIR:-/tmp}}/w1g
child=""
on_usr1() { echo "=== time limit near $(utc): stopping, syncing"; [ -n "$child" ] && kill -TERM -- -$child 2>/dev/null; sleep 5
            "$KIT/lib/servers.sh" stopall; syncall; echo "STOPPED at the time limit $(utc)" > $OUT/STOPPED; exit 0; }
trap on_usr1 USR1 TERM
export OUT ROOT W1_POINTS STORE_DIR
setsid bash $REPO/$STG/jobs/w1_gate_main.sh &
child=$!
wait $child; rc=$?
"$KIT/lib/servers.sh" stopall; syncall
say "gate job done rc=$rc"
exit $rc
