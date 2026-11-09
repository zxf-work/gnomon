#!/bin/bash
# W3 timed stage, gate job (untimed): the answer check of every instance of the timed parameter set, before any timing.
#   sbatch -p <partition> --ntasks-per-node=<cores> -t <limit> --export=ALL,W3_GATE_POINT=SF1|SF10,W3_GATE_SYS="gnomon duckdb ..."
# Per system (in parallel inside the job), on a partial node (a shared node) unless the store needs the memory:
#   restore the persisted full store (sha256 checked) -> start the server -> scripts/w3_snb_run.py run (the kit's W3 driver, the
#   check cap 10 s, Gnomon capped and in a killable worker, admission on) over the system's instances (the timed set: n = 20/10 at
#   SF1, 8 at SF10) at the settings current, asof, window, in nine query groups, each group a window of W3_VERIFY_WINDOW_S
#   (900 s): a window that ends first is cut and its unfinished instances are "unreached".
# Nothing is timed. The verdict is tools/w3_gate.py (job w3_verdict.sbatch), which exits non-zero on a FAIL.
#SBATCH -N 1
#SBATCH --signal=B:USR1@300
#SBATCH -J w3-gate
#SBATCH -o %x-%j.out
source ${W3_STAGE_ROOT:-${GNOMON_BENCH_ROOT}/final/w3}/src/benchmarks/stages/w3/jobs/w3_lib.sh
unset EXTCMP_NUMA_CLIENT EXTCMP_NUMA_SERVER          # partial nodes: no NUMA pinning for the untimed checks
guard; hostinfo
: "${W3_GATE_POINT:?}" "${W3_GATE_SYS:?}"; say "gate start point $W3_GATE_POINT systems [$W3_GATE_SYS]"
pt_config $W3_GATE_POINT || exit 2
OUT=$W3/results/${W3_SMOKE:+smoke-}gate-$SLURM_JOB_ID; mkdir -p $OUT/$PT
setup_root ${LOCAL:-${TMPDIR:-/tmp}}/w3g
child=""
on_usr1() { echo "=== time limit near $(utc): stopping"; [ -n "$child" ] && kill -TERM -- -$child 2>/dev/null; sleep 5
            "$KIT/lib/servers.sh" stopall; echo "STOPPED at the time limit $(utc)" > $OUT/STOPPED; exit 5; }
trap on_usr1 USR1 TERM
export OUT W3_GATE_POINT W3_GATE_SYS
setsid bash $REPO/$STG/jobs/w3_gate_main.sh &
child=$!
wait $child; rc=$?
"$KIT/lib/servers.sh" stopall
say "gate job done rc=$rc"
exit $rc
