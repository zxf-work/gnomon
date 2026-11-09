#!/usr/bin/env bash
# One timed engine build: build_synth_store.py in the September configuration
# of the ingest series, its memory
# sampled every 5 s, inside the exclusive lock on a shared host.
#
#   runbuild.sh <record dir> <name> <n events> [extra build_synth_store args...]
#
# Environment (switch settings, e.g. GNOMON_TIME_CODEC=raw) is inherited.
# Writes <record dir>/<name>.build-record.json, .mem.jsonl, .store-bytes,
# .env and .log; the store goes to $STORES/build-<name> and is removed after
# its bytes are recorded unless KEEP_STORE=1.
set -uo pipefail
LIB=$(cd "$(dirname "$0")" && pwd)
source "$LIB/env.sh"
source "$LIB/lock.sh"
recdir=$1; name=$2; n=$3; shift 3
mkdir -p "$recdir"
[ -e "$recdir/$name.build-record.json" ] && { echo "skip (done) build $name"; exit 0; }
if [ -z "${EXTCMP_INNER:-}" ]; then EXTCMP_INNER=1 timed_run "$0" "$recdir" "$name" "$n" "$@"; exit $?; fi
store=$STORES/build-$name
rm -rf "$store"
"$KIT/census.sh" "$recdir/census.jsonl" "before build $name"
"$LIB/servers.sh" stopall
{ env | grep -E '^GNOMON_' | sort; echo "ABL_CONFIG_GATE=${ABL_CONFIG_GATE:-n/a}"
  echo "ABL_CONFIG_GATE_1=${ABL_CONFIG_GATE_1:-n/a}"
  echo "EXTCMP_RECORD_LABEL=${EXTCMP_RECORD_LABEL:-}"; } > "$recdir/$name.env"
echo "=== build $name n=$n start $(date -u +%FT%TZ) commit=$(git -C "$REPO" rev-parse HEAD)"
export GNOMON_SEGMENT_CACHE_BYTES=${GNOMON_SEGMENT_CACHE_BYTES:-32000000000}
${EXTCMP_NUMA_CLIENT:-} "$REPO/.venv/bin/python" "$REPO/scripts/build_synth_store.py" --n-entities "$n" --seed 0 \
    --batch 50000 --compact-every 0 --tick-us auto52w --interval-fraction 0.5 \
    --named-fraction 0.1 --backend native --digest none --phase-accounting on \
    "$@" --out "$store" > "$recdir/$name.log" 2>&1 &
bp=$!
( while [ -e /proc/$bp ]; do
    awk -v t="$(date -u +%s)" 'BEGIN { printf "{\"t\": %s", t }
         /^(VmRSS|VmHWM|RssAnon|RssFile|RssShmem):/ { k = tolower($1); sub(":", "", k); printf ", \"%s_kb\": %s", k, $2 }
         END { print "}" }' /proc/$bp/status >> "$recdir/$name.mem.jsonl" 2>/dev/null
    sleep 5
  done ) &
sp=$!
wait $bp; rc=$?
kill $sp 2>/dev/null; wait $sp 2>/dev/null
du -sb "$store" | awk '{print $1}' > "$recdir/$name.store-bytes"
if [ $rc = 0 ] && [ -f "$store/build-record.json" ]; then
  python3 - "$store/build-record.json" "$recdir/$name.build-record.json" <<'PY'
import json, os, sys
d = json.load(open(sys.argv[1]))
d["kit"] = {"config_gate": os.environ.get("ABL_CONFIG_GATE", "n/a"),
            "config_gate_1": os.environ.get("ABL_CONFIG_GATE_1", "n/a"),
            "record_label": os.environ.get("EXTCMP_RECORD_LABEL", "")}
json.dump(d, open(sys.argv[2], "w"), indent=1, sort_keys=True)
PY
else echo "{\"name\": \"$name\", \"rc\": $rc, \"failed\": true}" > "$recdir/$name.build-record.json"; fi
[ "${KEEP_STORE:-0}" = 1 ] || rm -rf "$store"
"$KIT/census.sh" "$recdir/census.jsonl" "after build $name rc=$rc"
echo "=== build $name end $(date -u +%FT%TZ) rc=$rc"
# on a batch host the job script copies the records to persistent storage here
[ -n "${EXTCMP_SYNC_HOOK:-}" ] && eval "$EXTCMP_SYNC_HOOK"
exit $rc
