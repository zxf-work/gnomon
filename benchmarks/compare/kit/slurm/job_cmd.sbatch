#!/bin/bash
# One job on a whole the evaluation host node running a command given in the environment
# instead of a stages.tsv line: slurm/job.sbatch otherwise unchanged (records
# synced to $EXTCMP_PROJECT/campaign after every window and on the time-limit
# warning, frozen logs copied in, done/blocked markers). No state carried
# between jobs (STATE_KEY -): a command run this way is one job, self-contained.
#
#   EXTCMP_PROJECT=<its own directory> STAGE=<marker name> STAGE_CMD='<command>' \
#     sbatch -p RM -N 1 --ntasks-per-node=128 -t <limit> -A <account> --export=ALL kit/slurm/job_cmd.sbatch
#
# EXTCMP_PROJECT must be a directory no other running job syncs into.
#SBATCH -N 1
#SBATCH --exclusive
#SBATCH --signal=B:USR1@1800
#SBATCH -o extcmp-%x-%j.out
set -uo pipefail
: "${EXTCMP_PROJECT:?set EXTCMP_PROJECT to a directory of this job alone}"
source "$EXTCMP_PROJECT/site.env"
: "${STAGE:?}" "${STAGE_CMD:?}"
STATE_KEY=-
P=$EXTCMP_PROJECT/campaign; mkdir -p "$P"/{results,eventlogs,state,done,blocked}
utc() { date -u +%FT%TZ; }
cpsync() { mkdir -p "$2"; cp -a "$1/." "$2/" 2>/dev/null || true; }
if [ -e "$P/done/$STAGE" ]; then echo "stage $STAGE already done ($(cat "$P/done/$STAGE")); nothing to do"; exit 0; fi
if [ -e "$P/blocked/$STAGE" ]; then echo "stage $STAGE blocked: $(cat "$P/blocked/$STAGE")"; exit 0; fi

export EXTCMP_ROOT=${LOCAL:?no node-local \${LOCAL:-${TMPDIR:-/tmp}}}/extcmp
unset EXTCMP_GATE_DIR                       # per-job exclusivity replaces the gate lock
export TMPDIR_OVERRIDE=$EXTCMP_ROOT/scratch
mkdir -p "$EXTCMP_ROOT"/{results,eventlogs,stores,srv,scratch}
echo "=== job $SLURM_JOB_ID stage $STAGE on $(hostname) start $(utc)"
echo "=== command: $STAGE_CMD"
cpsync "$P/results" "$EXTCMP_ROOT/results"
cpsync "$P/eventlogs" "$EXTCMP_ROOT/eventlogs"           # node-local copies of the frozen logs
export EXTCMP_SYNC_HOOK="mkdir -p $P/results && cp -a $EXTCMP_ROOT/results/. $P/results/"
sync_out() { cpsync "$EXTCMP_ROOT/results" "$P/results"; }
census() { "$KIT/census.sh" "$EXTCMP_ROOT/results/census-jobs.jsonl" "$1"; }
child=""
on_warning() {
  echo "=== time limit near (USR1/TERM) at $(utc): stopping the runner, syncing"
  [ -n "$child" ] && kill -TERM "$child" 2>/dev/null; wait "$child" 2>/dev/null
  "$KIT/lib/servers.sh" stopall
  sync_out; census "job $SLURM_JOB_ID $STAGE end (time limit)"; sync_out
  exit 0
}
trap on_warning USR1 TERM
census "job $SLURM_JOB_ID $STAGE start"
bash -c "$STAGE_CMD" &
child=$!
wait "$child"; rc=$?
"$KIT/lib/servers.sh" stopall
census "job $SLURM_JOB_ID $STAGE end rc=$rc"
sync_out
if [ $rc = 0 ]; then
  echo "job $SLURM_JOB_ID $(utc)" > "$P/done/$STAGE"
else
  case $rc in 3) echo "rc=3 (answer check gate) job $SLURM_JOB_ID $(utc)" > "$P/blocked/$STAGE" ;; esac
fi
echo "=== job $SLURM_JOB_ID stage $STAGE end rc=$rc $(utc)"
exit $rc
