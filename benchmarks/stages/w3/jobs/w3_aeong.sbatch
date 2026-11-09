#!/bin/bash
# W3 timed stage, AeonG (F-W3-8: T-LDBC / IS as of transaction time, Gnomon against AeonG's native-TT form), whole RM node.
#   sbatch -t <limit> --dependency=afterok:<SF1 verdict> [--export=ALL,W3_SMOKE=1]
# AeonG reloads per job (its native transaction times do not survive a restart): split load from the SF1 history tables
# (chunks of 5,000 items, marker nodes, load-time time map), server start-time query timeout 10 s, the ceiling confirmed by
# a query longer than 10 s, the time map checked against the bisection. Then the answer check of the IS and T-LDBC cells (both
# forms, the timed instances 0-9) against Gnomon's gate records (ref-SF1-gnomon.jsonl): an AeonG cell that differs or errors is
# excluded and labelled (P13-P18 tolerated, not a gate failure). Then two rotations (order aeong, aeong-prop; flipped in rotation
# 2) of the timed protocol (1 warm-up + 5 timed, 1 + 3 above 1 s) on the cells that passed. AeonG is not interleaved with the
# other systems (its own job: its reload). If the ceiling is not enforceable its cells are "ceiling not enforceable" and not timed.
#SBATCH --exclusive
#SBATCH -N 1
#SBATCH --ntasks-per-node=128
#SBATCH --signal=B:USR1@600
#SBATCH -J w3-aeong
#SBATCH -o %x-%j.out
source ${W3_STAGE_ROOT:-${GNOMON_BENCH_ROOT}/final/w3}/src/benchmarks/stages/w3/jobs/w3_lib.sh
guard; hostinfo
pt_config SF1 || exit 2
[ "$(cat $W3/gate/gate-SF1.rc 2>/dev/null)" = 0 ] || { echo "NO PASSING SF1 GATE VERDICT: refusing"; exit 2; }
OUT=$W3/results/${W3_SMOKE:+smoke-}aeong-$SLURM_JOB_ID; mkdir -p $OUT
setup_root ${LOCAL:-${TMPDIR:-/tmp}}/w3a
child=""
on_usr1() { echo "=== time limit near $(utc): stopping, syncing"; [ -n "$child" ] && kill -TERM -- -$child 2>/dev/null; sleep 5
            "$KIT/lib/servers.sh" stopall; cp -au ${LOCAL:-${TMPDIR:-/tmp}}/w3a/res/. $OUT/ 2>/dev/null; echo "STOPPED at the time limit $(utc)" > $OUT/STOPPED; exit 5; }
trap on_usr1 USR1 TERM
export OUT
setsid bash $REPO/$STG/jobs/w3_aeong_main.sh &
child=$!
wait $child; rc=$?
"$KIT/lib/servers.sh" stopall; cp -au ${LOCAL:-${TMPDIR:-/tmp}}/w3a/res/. $OUT/ 2>/dev/null
say "aeong job done rc=$rc"
exit $rc
