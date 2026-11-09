#!/bin/bash
# W3 timed stage, timed job of one point (whole RM node, nothing but this job on it).
#   sbatch -t <limit> --dependency=afterok:<verdict>[:<SF1 timed job>] --export=ALL,W3_POINT=SF1|SF10
# Per point: restore the persisted full stores (sha256 checked) to ${LOCAL:-${TMPDIR:-/tmp}}; rotations over the systems (SF1: two, SF10: one),
# the system order rotated per query block and flipped in the second rotation, every system in a killable worker, one server
# at a time, interleaved by query block (SF1: S = IS + T-LDBC, C = IC, B = BI + BIH; SF10: one block);
# DuckDB (SQL) at 64 and at 32 threads as two systems (the better of the two is reported); Neo4j and Memgraph with probes on;
# then the engine-time pass (1 warm-up + 3 timed, instances 0-2) for Gnomon, DuckDB (SQL, 64 threads) and PostgreSQL (restarted
# with EXTCMP_PG_STATS=1) on the cells every instance of which answered. After every Gnomon block tools/w3_stopcheck.py applies
# the stop rule (exit 4). Per-instance gate exclusions: $W3/gate/exclusions-<point>.json. A job stopped at its time limit exits 5.
#SBATCH --exclusive
#SBATCH -N 1
#SBATCH --ntasks-per-node=128
#SBATCH --signal=B:USR1@900
#SBATCH -J w3-timed
#SBATCH -o %x-%j.out
source ${W3_STAGE_ROOT:-${GNOMON_BENCH_ROOT}/final/w3}/src/benchmarks/stages/w3/jobs/w3_lib.sh
guard; hostinfo
: "${W3_POINT:?}"; say "timed start point [$W3_POINT]"
pt_config $W3_POINT || exit 2
# C3 mode: no gate job (the timed hashes are the check); the recorded gate's verdict is the one the exclusions come from
[ "$(cat $([ "${C3_CONFIRM:-0}" = 1 ] && echo $C3_W3_GATE || echo $GATED)/gate-$PT.rc 2>/dev/null)" = 0 ] || { echo "NO PASSING GATE VERDICT for $PT: refusing"; exit 2; }
OUT=$W3/results/${W3_SMOKE:+smoke-}timed-$SLURM_JOB_ID; mkdir -p $OUT
setup_root ${LOCAL:-${TMPDIR:-/tmp}}/w3t
child=""
on_usr1() { echo "=== time limit near $(utc): stopping, syncing"; [ -n "$child" ] && kill -TERM -- -$child 2>/dev/null; sleep 5
            "$KIT/lib/servers.sh" stopall; cp -au ${LOCAL:-${TMPDIR:-/tmp}}/w3t/res/. $OUT/ 2>/dev/null; echo "STOPPED at the time limit $(utc)" > $OUT/STOPPED; exit 5; }
trap on_usr1 USR1 TERM
export OUT W3_POINT
setsid bash $REPO/$STG/jobs/w3_timed_main.sh &
child=$!
wait $child; rc=$?
"$KIT/lib/servers.sh" stopall; cp -au ${LOCAL:-${TMPDIR:-/tmp}}/w3t/res/. $OUT/ 2>/dev/null
say "timed job done rc=$rc"
exit $rc
