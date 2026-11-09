#!/bin/bash
# Answer gate job (untimed), whole RM node. X_SET w1, w2: restores the stores, K10M-d10 first, then K10M-d0. X_SET k100m: loads every system on C4
# and saves each complete store under the stage. Then every instance on every system at the timed protocol's caps, the kit's per-instance gate, and
# tools/x_gate.py: every answer against Gnomon's in this job, and at d0 against the system's own earlier record. Nothing is timed. Exit 1 when the verdict
# fails, 5 at the time limit. The verdict file is read by hand before the timed job is submitted.
#SBATCH --exclusive
#SBATCH -N 1
#SBATCH --ntasks-per-node=128
#SBATCH --signal=B:USR1@600
#SBATCH -J c4-x-gate
source ${NEXT_STAGE_ROOT:?}/src/benchmarks/stages/studies/t10/jobs/x_lib.sh
guard; hostinfo
say "gate start set $X_SET points [$POINTS] families [$FAMS]"
OUT=$NEXT_STAGE_ROOT/results/${SM}gate-$X_SET-$SLURM_JOB_ID; mkdir -p $OUT $NEXT_STAGE_ROOT/${SM}gate
x_root ${LOCAL:?}/xg
rss_start
child=""
on_usr1() { echo "=== time limit near $(utc): stopping, syncing"; [ -n "$child" ] && kill -TERM -- -$child 2>/dev/null; sleep 5
            "$KIT/lib/servers.sh" stopall; syncall; rss_stop; echo "STOPPED at the time limit $(utc)" > $OUT/STOPPED; exit 5; }
trap on_usr1 USR1 TERM
export OUT ROOT
setsid bash $XD/jobs/x_gate_main.sh &
child=$!
wait $child; rc=$?
"$KIT/lib/servers.sh" stopall; syncall; rss_stop
say "gate job done rc=$rc"
exit $rc
