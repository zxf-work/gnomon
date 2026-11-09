#!/bin/bash
# Liveness check on the Apptainer backend (≤ 30 min, part of a shared node):
# load Memgraph at 200k, run a timed query family in the background, SIGKILL
# the server's process tree under our wrapper mid-step, and check that the
# step records the death (rc 76, server_death with the state, remaining cells
# "server died").
#SBATCH -N 1
#SBATCH --ntasks-per-node=16
#SBATCH -t 00:30:00
#SBATCH -J liveness
#SBATCH -o extcmp-%x-%j.out
set -uo pipefail
export EXTCMP_PROJECT=${EXTCMP_PROJECT:-${PROJECT:?}/extcmp}
source "$EXTCMP_PROJECT/site.env"
export EXTCMP_ROOT=${LOCAL:?}/liveness TMPDIR_OVERRIDE=${LOCAL:-${TMPDIR:-/tmp}}/liveness/scratch
export EXTCMP_RECORD_LABEL="HPC, liveness check — not for publication"
unset EXTCMP_GATE_DIR
mkdir -p "$EXTCMP_ROOT"/{results,eventlogs,stores,srv,scratch}
cp -a "$EXTCMP_PROJECT"/campaign/eventlogs/synth-200000.* "$EXTCMP_ROOT/eventlogs/"
source "$KIT/lib/env.sh"
OUT=$EXTCMP_PROJECT/trial/liveness-$SLURM_JOB_ID; mkdir -p "$OUT"
log=$EXTCMP_ROOT/eventlogs/synth-200000.eventlog.jsonl
utc() { date -u +%FT%TZ; }
echo "=== loads $(utc)"
$PY load --system native --log "$log" --workdir "$EXTCMP_ROOT" --size-tag K200000 --json "$OUT/load-native.json" > /dev/null
"$KIT/lib/servers.sh" start memgraph K200000 && $PY wait --system memgraph --timeout 600 || exit 1
$PY load --system memgraph --log "$log" --ref "$EXTCMP_ROOT/stores/native-K200000" --workdir "$EXTCMP_ROOT" \
    --size-tag K200000 --json "$OUT/load-memgraph.json" > /dev/null || exit 1
echo "state before: $("$KIT/lib/servers.sh" state memgraph K200000)"
echo "=== timed family T in the background, SIGKILL the server after 30 s $(utc)"
( export EXTCMP_CONTAINER=extcmp-memgraph-K200000 \
         EXTCMP_STATE_CMD="$KIT/lib/servers.sh state memgraph K200000" \
         EXTCMP_LOGS_CMD="$KIT/lib/servers.sh logs memgraph K200000 40"
  $PY query --system memgraph --log "$log" --ref "$EXTCMP_ROOT/stores/native-K200000" --workdir "$EXTCMP_ROOT" \
      --size-tag K200000 --mode timed --family T --json "$OUT/T-memgraph.json" ) &
q=$!
sleep 30
wp=$(cat "$EXTCMP_ROOT/srv/memgraph-K200000/server.pid")
desc() { local c; for c in $(ps -o pid= --ppid "$1"); do desc "$c"; echo "$c"; done; }
kids=$(desc "$wp" | tr '\n' ' ')
echo "SIGKILL to the process tree under our wrapper $wp: $kids at $(utc)"
kill -KILL $kids
wait $q; rc=$?
echo "query step rc=$rc (76 = server died, recorded)"
echo "state after: $("$KIT/lib/servers.sh" state memgraph K200000)"
python3 - "$OUT/T-memgraph.json" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))
sd = d.get("server_death") or {}
print("server_death.state:", sd.get("state"), "| after_query:", sd.get("after_query"))
print("cells:", [(c["query"], c.get("ok"), c.get("server_died"), (c.get("error") or "")[:60]) for c in d["results"]])
ok = bool(sd) and any(c.get("server_died") for c in d["results"])
print("LIVENESS CHECK", "PASS" if ok else "FAIL")
PY
"$KIT/lib/servers.sh" stop memgraph K200000
"$KIT/lib/servers.sh" stopall
echo "=== done $(utc)"
