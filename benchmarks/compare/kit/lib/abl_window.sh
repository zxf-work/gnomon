#!/usr/bin/env bash
# One ablation window: one query family on one native store with one switch
# setting, timed (protocol of extcmp.py: 5 warmups, 30 or 10
# repetitions, 300 s cap, 3600 s ceiling), inside the exclusive lock on a
# shared host. Census and CPU-outside-the-window records as in window.sh.
#
#   abl_window.sh <size> <tag> <rot> <name> <family> <variant> <env|->
set -uo pipefail
LIB=$(cd "$(dirname "$0")" && pwd)
source "$LIB/env.sh"
source "$LIB/lock.sh"
size=$1; tag=$2; rot=$3; name=$4; fam=$5; variant=$6; envspec=$7
log=$LOGS/$tag.eventlog.jsonl
out=$RES/${ABL_LABEL:-ablation}/$tag/rot$rot/$fam; mkdir -p "$out"
rec=$out/$name.json
[ -e "$rec" ] && { echo "skip (done) $tag rot$rot $fam $name"; exit 0; }
# ABL_PARAMS_DIR (W2 ablation): the kit's parameter-file path. Registry families read params-<size tag>.json, the B family
# params-<density tag>.json. A missing file or store stops at once (exit 2): run_ablation.sh stops on it.
pf=()
if [ -n "${ABL_PARAMS_DIR:-}" ]; then
  case $fam in B) _p=$ABL_PARAMS_DIR/params-$tag.json ;; *) _p=$ABL_PARAMS_DIR/params-${tag%%-d*}.json ;; esac
  [ -s "$_p" ] || { echo "ABLATION WINDOW: no parameter file $_p: stopping"; exit 2; }
  pf=(--params "$_p")
fi
_st=$STORES/native-$tag-$variant; [ "$variant" = duckdb ] && _st=$STORES/duckdb-$tag
[ -d "$_st" ] && [ -n "$(ls -A "$_st" 2>/dev/null)" ] || { echo "ABLATION WINDOW: store $_st is missing or empty: stopping"; exit 2; }
if [ -z "${EXTCMP_INNER:-}" ]; then EXTCMP_INNER=1 timed_run "$0" "$@"; exit $?; fi
utc() { date -u +%FT%TZ; }
base=$RES/${ABL_LABEL:-ablation}/$tag
"$KIT/census.sh" "$base/census.jsonl" "before $tag rot$rot $fam $name"
cpud=$base/cpu; mkdir -p "$cpud"; wid=rot$rot-$fam-$name-$(date -u +%Y%m%dT%H%M%SZ)
python3 "$LIB/cpu_others.py" snap "$cpud/$wid-acquire.json" $$ acquire >> "$base/census.jsonl"
"$LIB/servers.sh" stopall
# every switch unset, then this window's one setting
unset GNOMON_RANGE_VISIBILITY GNOMON_WINDOW_FIRST GNOMON_TIME_CODEC GNOMON_SEGMENT_PRUNE GNOMON_TCSR \
      GNOMON_VIDFILTER GNOMON_EXISTENCE_PROBE GNOMON_SEED_EXPANSION GNOMON_ADJ_FILTERED_SCAN \
      GNOMON_AGG_RANGE_COUNT GNOMON_REACH_KERNEL GNOMON_DIFF_KERNEL GNOMON_INGEST_RAW \
      GNOMON_NO_STAIRCASE GNOMON_AGG_PARALLEL_MIN_ROWS GNOMON_PATHS_KERNEL GNOMON_TT_E_COLUMN GNOMON_NBR_EVOLUTION_KERNEL \
      GNOMON_NBR_EVOLUTION_ONE_READ GNOMON_SNAPSHOT_PAGE GNOMON_SNAPSHOT_ONECALL
if [ "$envspec" != - ]; then IFS=, read -r -a _kv <<< "$envspec"; for _x in "${_kv[@]}"; do export "$_x"; done; fi
# store variant duckdb: the kit system duckdb (Gnomon's operators on the DuckDB
# backend), store stores/duckdb-<tag>; every other variant is a native store
qsys=native; qtag=$tag-$variant; qref=()
[ "$variant" = duckdb ] && { qsys=duckdb; qtag=$tag; qref=(--ref "$STORES/native-$tag-u"); }
echo "=== ablation window $tag rot$rot $fam $name store=$variant env=$envspec start $(utc)"
$PY query --system "$qsys" "${qref[@]}" --log "$log" --workdir "$EXTCMP_ROOT" --size-tag "$qtag" --mode timed \
    --family "$fam" --answers "$out/answers-$name" --json "$rec.tmp" "${pf[@]}"; rc=$?
if [ $rc = 0 ] && [ -e "$rec.tmp" ]; then
  python3 - "$rec.tmp" "$name" "$variant" "$envspec" <<'PY'
import json, sys
p, name, variant, env = sys.argv[1:]
d = json.load(open(p))
import os
d["ablation"] = {"window": name, "store_variant": variant, "env": env,
                 "config_gate": os.environ.get("ABL_CONFIG_GATE", "not recorded"),
                 "config_gate_1": os.environ.get("ABL_CONFIG_GATE_1", "not recorded"),
                 "config_gate_2": os.environ.get("ABL_CONFIG_GATE_2", "not recorded")}
json.dump(d, open(p, "w"), indent=1, sort_keys=True)
PY
  mv "$rec.tmp" "$rec"
fi
python3 "$LIB/cpu_others.py" snap "$cpud/$wid-release.json" $$ release >> "$base/census.jsonl"
cpuw=$(python3 "$LIB/cpu_others.py" window "$cpud/$wid-acquire.json" "$cpud/$wid-release.json")
[ $? = 1 ] && echo "=== CPU OUTSIDE WINDOW ablation $tag rot$rot $fam $name: $cpuw"
echo "$cpuw" >> "$base/census.jsonl"
"$KIT/census.sh" "$base/census.jsonl" "after $tag rot$rot $fam $name rc=$rc"
echo "=== ablation window $tag rot$rot $fam $name end $(utc) rc=$rc"
# on a batch host the job script copies the records to persistent storage here
[ -n "${EXTCMP_SYNC_HOOK:-}" ] && eval "$EXTCMP_SYNC_HOOK"
exit $rc
