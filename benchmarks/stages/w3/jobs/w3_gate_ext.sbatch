#!/bin/bash
# W3 SF10 gate extension (Gnomon only): the cells the 900 s answer-check windows left unreached, checked at the
# same 10 s cap, admission on, in N parallel streams (each its own copy of the Gnomon store and its own 32-thread engine), each
# task (query group x setting, <= 16 cells) in a window of 2 x its measured-rate estimate + 600 s (tools/w3_gate_ext_plan.py).
#   sbatch -p a shared node --ntasks-per-node=64 -t 07:15:00 [--export=ALL,W3_EXT_STREAMS=2]
# Records go to $W3/results/gateext-<job>/SF10/gnomon/ and are merged over the gate records by tools/w3_gate.py.
#SBATCH -N 1
#SBATCH --signal=B:USR1@300
#SBATCH -J w3-gate-ext
#SBATCH -o %x-%j.out
source ${W3_STAGE_ROOT:-${GNOMON_BENCH_ROOT}/final/w3}/src/benchmarks/stages/w3/jobs/w3_lib.sh
unset EXTCMP_NUMA_CLIENT EXTCMP_NUMA_SERVER
guard; hostinfo
pt_config SF10 || exit 2
OUT=$W3/results/gateext-$SLURM_JOB_ID; mkdir -p $OUT/SF10/gnomon
setup_root ${LOCAL:-${TMPDIR:-/tmp}}/w3x
child=""
on_usr1() { echo "=== time limit near $(utc): stopping"; [ -n "$child" ] && kill -TERM -- -$child 2>/dev/null; sleep 5; echo "STOPPED at the time limit $(utc)" > $OUT/STOPPED; exit 5; }
trap on_usr1 USR1 TERM
export OUT
setsid bash $REPO/$STG/jobs/w3_gate_ext_main.sh &
child=$!
wait $child; rc=$?
say "gate extension done rc=$rc"
exit $rc
