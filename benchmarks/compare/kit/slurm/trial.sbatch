#!/bin/bash
# Trial on one whole RM node (≤ 2 h): the kit on the Apptainer backend.
#   1. every server starts, loads and answers (external comparison at 200k:
#      loads + answer check, untimed);
#   2. the per-query liveness check: a server killed mid-step is recorded
#      ("server died", state with exit code);
#   3. the 200k kit smoke of run_ablation.sh (density 0) and
#      run_engine_series.sh at 1M (ingest + read latency).
# Records go to $GNOMON_BENCH_ROOT/trial/ at the end and on the time-limit warning.
#SBATCH --exclusive
#SBATCH -N 1
#SBATCH --exclusive
#SBATCH -t 02:00:00
#SBATCH --signal=B:USR1@600
#SBATCH -J trial
#SBATCH -o extcmp-%x-%j.out
set -uo pipefail
export EXTCMP_PROJECT=${EXTCMP_PROJECT:-${PROJECT:?}/extcmp}
source "$EXTCMP_PROJECT/site.env"
export EXTCMP_ROOT=${LOCAL:?}/trial TMPDIR_OVERRIDE=${LOCAL:-${TMPDIR:-/tmp}}/trial/scratch
export EXTCMP_RECORD_LABEL="whole node, kit trial — not for publication"
unset EXTCMP_GATE_DIR
OUT=$EXTCMP_PROJECT/trial/$SLURM_JOB_ID; mkdir -p "$OUT" "$EXTCMP_ROOT"/{results,eventlogs,stores,srv,scratch}
cp -a "$EXTCMP_PROJECT"/campaign/eventlogs/synth-{200000,1000000}.* "$EXTCMP_ROOT/eventlogs/"
utc() { date -u +%FT%TZ; }
save() { cp -a "$EXTCMP_ROOT/results/." "$OUT/" 2>/dev/null; }
trap 'echo "=== time limit near $(utc)"; "$KIT/lib/servers.sh" stopall; save; exit 0' USR1 TERM
step() { echo; echo "##### $1 $(utc)"; }
"$KIT/census.sh" "$EXTCMP_ROOT/results/census-trial.jsonl" "trial start job $SLURM_JOB_ID"

step "1. external comparison 200k: loads + answer check, every server on Apptainer"
"$KIT/run_campaign.sh" --sizes 200000 --steps loads,verify,tables; echo "ext200k rc=$?"
save

step "2. liveness: kill Memgraph during a step"
source "$KIT/lib/env.sh"       # loopback ports of the servers, $PY with the client NUMA policy
mkdir -p "$EXTCMP_ROOT/results/liveness"
"$KIT/lib/servers.sh" start memgraph K200000 && $PY wait --system memgraph --timeout 600
"$KIT/lib/servers.sh" state memgraph K200000
( export EXTCMP_CONTAINER=extcmp-memgraph-K200000 \
         EXTCMP_STATE_CMD="$KIT/lib/servers.sh state memgraph K200000" \
         EXTCMP_LOGS_CMD="$KIT/lib/servers.sh logs memgraph K200000 40"
  $PY query --system memgraph --log "$EXTCMP_ROOT/eventlogs/synth-200000.eventlog.jsonl" \
      --ref "$EXTCMP_ROOT/stores/native-K200000" --workdir "$EXTCMP_ROOT" --size-tag K200000 \
      --mode timed --family T --json "$EXTCMP_ROOT/results/liveness/T-memgraph.json" ) &
q=$!
sleep 20
wp=$(cat "$EXTCMP_ROOT/srv/memgraph-K200000/server.pid")
desc() { local c; for c in $(ps -o pid= --ppid "$1"); do desc "$c"; echo "$c"; done; }
kids=$(desc "$wp" | tr '\n' ' ')
echo "SIGKILL to the server's process tree under our wrapper $wp: $kids at $(utc)"
ps -o pid,ppid,comm -p ${kids// /,} 2>/dev/null
kill -KILL $kids
wait $q; echo "query step rc=$? (76 = server died, recorded)"
"$KIT/lib/servers.sh" state memgraph K200000
python3 - "$EXTCMP_ROOT/results/liveness/T-memgraph.json" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))
print("server_death:", json.dumps(d.get("server_death", {}))[:400])
print("cells:", [(c["query"], c.get("ok"), c.get("server_died")) for c in d["results"]])
PY
"$KIT/lib/servers.sh" stop memgraph K200000
save

step "3a. run_ablation.sh --smoke at 200k, density 0, one rotation, ingest 1 repetition"
"$KIT/run_ablation.sh" --smoke --sizes 200000 --densities 0 --rotations 1 --ingest-sizes 200000 \
    --ingest-reps 1 --steps logs,stores,timed,ingest,report; echo "ablation smoke rc=$?"
save
step "3b. run_engine_series.sh at 1M (ingest; read latency)"
"$KIT/run_engine_series.sh" --sizes 1000000 --readlat-sizes 1000000 --rotations 1 \
    --steps ingest,readlat,report; echo "series rc=$?"
"$KIT/lib/servers.sh" stopall
"$KIT/census.sh" "$EXTCMP_ROOT/results/census-trial.jsonl" "trial end job $SLURM_JOB_ID"
save
step "trial done"
