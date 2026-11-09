#!/usr/bin/env bash
# Campaign configuration (sourced by every kit script). Override any value in
# the environment before calling run_campaign.sh.
: "${EXTCMP_ROOT:?set EXTCMP_ROOT to the campaign directory (on the NVMe volume)}"
KIT=${KIT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}
REPO=${EXTCMP_REPO:-$EXTCMP_ROOT/repo}                     # engine + comparison code, built here
source "$(dirname "${BASH_SOURCE[0]}")/engine_guard.sh"   # the built engine must exist (and match EXTCMP_ENGINE_SO_SHA256)
SRV=$EXTCMP_ROOT/srv                       # server data directories
RES=$EXTCMP_ROOT/results                   # every record the campaign writes
LOGS=$EXTCMP_ROOT/eventlogs                # frozen synthetic event logs
STORES=$EXTCMP_ROOT/stores                 # embedded stores (native, duckdb, ladybug, raphtory)
AEONG_SRC=$EXTCMP_ROOT/aeong               # AeonG source + build
export TMPDIR=${TMPDIR_OVERRIDE:-$EXTCMP_ROOT/scratch}
CAP=${EXTCMP_MEM_CAP:-48g}                 # per-container memory cap
SERVER_TIMEOUT_S=${EXTCMP_SERVER_TIMEOUT_S:-3660}
export EXTCMP_CALL_CEILING_S=${EXTCMP_CALL_CEILING_S:-3600}
if [ "$SERVER_TIMEOUT_S" -gt $((EXTCMP_CALL_CEILING_S - 2)) ]; then
  echo "NOTE caps: server timeout ${SERVER_TIMEOUT_S} s is not 2 s below the client alarm ${EXTCMP_CALL_CEILING_S} s (rule: EXTCMP_SERVER_TIMEOUT_S = EXTCMP_CALL_CEILING_S - 2)" >&2
fi
# caps and alarms: every server's own cap is the client alarm - 2 s,
# so the server's abort arrives first; the answer check's alarm is the server cap + 2 s
export EXTCMP_VERIFY_CAP_S=${EXTCMP_VERIFY_CAP_S:-$((SERVER_TIMEOUT_S + 2))}
export EXTCMP_RECORD_LABEL=${EXTCMP_RECORD_LABEL:-"$(hostname -s), campaign kit"}
# loopback ports: per job inside Slurm, the fixed non-default ones elsewhere (ports.sh)
source "$(dirname "${BASH_SOURCE[0]}")/ports.sh"
export GNOMON_MEASUREMENT_HOST=$(hostname -s) GNOMON_EXTERNAL_RUNTIME=docker
for v in EXTCMP_NUMA_CLIENT EXTCMP_NUMA_SERVER; do
  if [ -n "${!v:-}" ] && ! ${!v} true > /dev/null 2>&1; then
    echo "NUMA policy $v=[${!v}] not satisfiable in this job (shared node?): dropped" >&2
    export "${v}_DROPPED=${!v}"; unset "$v"
  fi
done
PY="${EXTCMP_NUMA_CLIENT:+$EXTCMP_NUMA_CLIENT }$REPO/.venv/bin/python $REPO/scripts/extcmp.py"
# EXTCMP_WIPE_AFTER=1: after a point's tables, remove its server data dirs and
# embedded stores (records stay); keeps the disk peak near one point at 100M
wipe_point() {   # tag
  local s
  for s in postgres clickhouse neo4j memgraph xtdb aeong; do "$KIT/lib/servers.sh" wipe "$s" "$1"; done
  for s in native duckdb ladybug raphtory; do rm -rf "$STORES/$s-$1"; done
  echo "=== wiped data of $1 $(date -u +%FT%TZ)"
}
# server backend: docker (default) or apptainer
export EXTCMP_BACKEND=${EXTCMP_BACKEND:-docker}
img() { awk -v k="$1" '$1 == k { print $2 }' "$KIT/images.lock"; }
mkdir -p "$SRV" "$RES" "$LOGS" "$STORES" "$TMPDIR"
