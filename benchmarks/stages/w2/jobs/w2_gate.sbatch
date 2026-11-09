#!/bin/bash
# W2 timed stage, gate job (untimed): loads, store persistence and the answer gate, before any timing.
#   W2_POINTS (sbatch --export): G1 "K1M K1M-d0 K1M-d1 K1M-d10", G2 "K10M", G3 "K10M-d0 K10M-d1", G4 "K10M-d10" (after B0: the
#   duckdb store is restored)
# Per point, whole RM node:
#   1. loads by the v3 method (window.sh load; native first, the baselines from the native store), the eight systems
#      (Gnomon, DuckDB (SQL), ClickHouse, PostgreSQL, Gnomon on DuckDB storage, Neo4j, Memgraph, LadybugDB), each store
#      persisted to $W2/stores/<tag>/ only after a complete load (rc 0, no "load exceeded budget"); a store persisted
#      earlier is restored instead of loaded;
#   2. p1: the answer check of every instance of the W2 parameter file (params/params-<tag>.json, 588 registry / 163-164
#      belief-time instances) on every system, at the timed protocol's caps (10 s ceiling; run_p1.sh --steps verify);
#      a verify window of one (system, family) is cut at W2_VERIFY_WINDOW_S (900 s) and its unfinished instances are
#      recorded "unreached" (tools/w2_partial.py); then the kit gate (extcmp_p1.py gate: every system against Gnomon in the
#      same job; the per-instance exclusion list of the timed jobs);
#   3. t32p1: the same for DuckDB (SQL) at 32 threads with its own Gnomon reference;
#      $W2/gate/. THE JOB EXITS NON-ZERO WHEN THE GATE FAILS (or when it was stopped at its time limit), so
#      --dependency=afterok blocks the timed jobs.
# Nothing is timed.
#SBATCH --exclusive
#SBATCH -N 1
#SBATCH --ntasks-per-node=128
#SBATCH --signal=B:USR1@600
#SBATCH -J w2-gate
#SBATCH -o %x-%j.out
source ${W2_STAGE_ROOT:-${GNOMON_BENCH_ROOT}/final/w2}/src/benchmarks/stages/w2/jobs/w2_lib.sh
guard; hostinfo
: "${W2_POINTS:?}"; say "gate start points [$W2_POINTS]"
OUT=$W2/results/${W2_SMOKE:+smoke-}gate-$SLURM_JOB_ID; mkdir -p $OUT
setup_root ${LOCAL:-${TMPDIR:-/tmp}}/w2g
child=""
on_usr1() { echo "=== time limit near $(utc): stopping, syncing"; [ -n "$child" ] && kill -TERM -- -$child 2>/dev/null; sleep 5
            "$KIT/lib/servers.sh" stopall; syncall; echo "STOPPED at the time limit $(utc)" > $OUT/STOPPED; exit 5; }
trap on_usr1 USR1 TERM
export OUT ROOT W2_POINTS STORE_DIR
setsid bash $REPO/$STG/jobs/w2_gate_main.sh &
child=$!
wait $child; rc=$?
"$KIT/lib/servers.sh" stopall; syncall
say "gate job done rc=$rc"
exit $rc
