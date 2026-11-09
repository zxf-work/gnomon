#!/bin/bash
# One campaign job on a whole the evaluation host node. Submitted by submit_campaign.sh
# with partition (RM or a large-memory node), time limit, account and the stage spec:
#
#   sbatch -p RM -t 70:00:00 -A <account> --export=ALL,STAGE=<name> kit/slurm/job.sbatch
#
# The stage's command and state key come from kit/slurm/stages.tsv.
#
# Exclusivity is the job's: a whole node is ours (no gate lock; EXTCMP_GATE_DIR
# unset), and the census records the node, the job and its sharing setting.
# Timed work runs on node-local disk: EXTCMP_ROOT=${LOCAL:-${TMPDIR:-/tmp}}/extcmp. Persistent
# things live on $PROJECT (Ocean): images, repository and venv, frozen logs,
# records. At job start the records (so completed windows are skipped) and
# the frozen logs are copied in; after every window the records are synced
# back (EXTCMP_SYNC_HOOK); at the end, and on the time-limit warning
# (--signal=B:USR1@1800), the runner is stopped, records are synced, and a
# stage that spans several jobs persists its server data and stores
# (STATE_KEY) for the next job of the chain, which restores them.
#SBATCH -N 1
#SBATCH --exclusive
#SBATCH --signal=B:USR1@1800
#SBATCH -o extcmp-%x-%j.out
set -uo pipefail
export EXTCMP_PROJECT=${EXTCMP_PROJECT:-${PROJECT:?}/extcmp}
source "$EXTCMP_PROJECT/site.env"
: "${STAGE:?}"
line=$(awk -F'\t' -v s="$STAGE" '$1 == s' "$KIT/slurm/stages.tsv")
[ -n "$line" ] || { echo "stage $STAGE not in $KIT/slurm/stages.tsv"; exit 2; }
STATE_KEY=$(echo "$line" | cut -f5)
STAGE_CMD=$(echo "$line" | cut -f6-); STAGE_CMD=${STAGE_CMD#optional:}; STAGE_CMD=${STAGE_CMD#later:}
P=$EXTCMP_PROJECT/campaign; mkdir -p "$P"/{results,eventlogs,state,done,blocked}
utc() { date -u +%FT%TZ; }
# no rsync on the evaluation host: copy trees with cp (-a keeps times; files the
# destination already has are overwritten, which is what a sync needs here)
cpsync() { mkdir -p "$2"; cp -a "$1/." "$2/" 2>/dev/null || true; }
cpmirror() { rm -rf "$2"; mkdir -p "$2"; cp -a "$1/." "$2/"; }
if [ -e "$P/done/$STAGE" ]; then echo "stage $STAGE already done ($(cat "$P/done/$STAGE")); nothing to do"; exit 0; fi
if [ -e "$P/blocked/$STAGE" ]; then echo "stage $STAGE blocked: $(cat "$P/blocked/$STAGE")"; exit 0; fi

export EXTCMP_ROOT=${LOCAL:?no node-local \${LOCAL:-${TMPDIR:-/tmp}}}/extcmp
unset EXTCMP_GATE_DIR                       # per-job exclusivity replaces the gate lock
export TMPDIR_OVERRIDE=$EXTCMP_ROOT/scratch
mkdir -p "$EXTCMP_ROOT"/{results,eventlogs,stores,srv,scratch}
echo "=== job $SLURM_JOB_ID stage $STAGE on $(hostname) start $(utc)"
cpsync "$P/results" "$EXTCMP_ROOT/results"
cpsync "$P/eventlogs" "$EXTCMP_ROOT/eventlogs"           # node-local copies of the frozen logs
if [ "$STATE_KEY" != - ] && [ -d "$P/state/$STATE_KEY" ]; then
  echo "=== restoring state $STATE_KEY $(utc)"
  cpsync "$P/state/$STATE_KEY/srv" "$EXTCMP_ROOT/srv"
  cpsync "$P/state/$STATE_KEY/stores" "$EXTCMP_ROOT/stores"
  echo "=== restored $(du -sh "$EXTCMP_ROOT" | cut -f1) $(utc)"
fi
export EXTCMP_SYNC_HOOK="mkdir -p $P/results && cp -a $EXTCMP_ROOT/results/. $P/results/"
sync_out() {
  cpsync "$EXTCMP_ROOT/results" "$P/results"
  cpsync "$EXTCMP_ROOT/eventlogs" "$P/eventlogs"           # logs frozen in this job
}
persist() {
  [ "$STATE_KEY" = - ] && return 0
  echo "=== persisting state $STATE_KEY $(utc)"
  mkdir -p "$P/state/$STATE_KEY"
  cpmirror "$EXTCMP_ROOT/srv" "$P/state/$STATE_KEY/srv"
  cpmirror "$EXTCMP_ROOT/stores" "$P/state/$STATE_KEY/stores"
  echo "=== persisted $(du -sh "$P/state/$STATE_KEY" | cut -f1) $(utc)"
}
census() { "$KIT/census.sh" "$EXTCMP_ROOT/results/census-jobs.jsonl" "$1"; }
child=""
on_warning() {
  echo "=== time limit near (USR1/TERM) at $(utc): stopping the runner, syncing"
  [ -n "$child" ] && kill -TERM "$child" 2>/dev/null; wait "$child" 2>/dev/null
  "$KIT/lib/servers.sh" stopall
  sync_out; persist; census "job $SLURM_JOB_ID $STAGE end (time limit)"; sync_out
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
  [ "$STATE_KEY" != - ] && rm -rf "$P/state/$STATE_KEY"     # stage complete: no state to carry
else
  persist
  case $rc in 3) echo "rc=3 (answer check) job $SLURM_JOB_ID $(utc)" > "$P/blocked/$STAGE" ;; esac
fi
echo "=== job $SLURM_JOB_ID stage $STAGE end rc=$rc $(utc)"
exit $rc
