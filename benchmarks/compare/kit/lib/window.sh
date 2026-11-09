#!/usr/bin/env bash
# One campaign window (kit copy of the rehearsal's phase_b_step.sh).
#   window.sh load        <size> <tag> - -   <system>   timed load (budgeted)
#   window.sh verify      <size> <tag> - -   <system>   untimed answer check vs native
#   window.sh aeongverify <size> <tag> - -   aeong      untimed: AeonG load + answer check
#   window.sh block       <size> <tag> <r> <family> <system>   timed family on one system
#   window.sh aeong       <size> <tag> <r> - aeong      timed: AeonG load + all families
# Inside a window: census, stop every extcmp container, start this system's
# server, run, stop it, census. Complete outputs are skipped (resumable).
# At acquire and release, lib/cpu_others.py records the top CPU consumers the
# window does not own and, over the window, the average cores they used; a
# window where they used more than one core is flagged in census.jsonl
# ("flag_other_over_1_core") and in the log. The gate lock (shared hosts) or
# its absence (dedicated hosts) cannot see processes outside the campaign.
# Server liveness: the server's container is checked after every query
# (EXTCMP_CONTAINER, read by extcmp.py). If it is gone, the query step
# records its state and log tail (`server_death`), marks the remaining cells
# "server died" and exits 76; in an AeonG window the families not yet run are
# recorded as "server died" too. Nothing is skipped silently.
# EXTCMP_PARAMS (P1, run_p1.sh): a parameter file passed to every verify and
# block step (`extcmp.py query --params`), and P1_VTAG a suffix of the verify
# record (one per family); both unset, the steps are unchanged.
set -uo pipefail
LIB=$(cd "$(dirname "$0")" && pwd)
source "$LIB/env.sh"
source "$LIB/lock.sh"
kind=$1; size=$2; tag=$3; rot=$4; fam=$5; sys=$6
log=$LOGS/synth-$size.eventlog.jsonl
# a density point of the correction-density study has its own frozen log
[ -e "$LOGS/$tag.eventlog.jsonl" ] && log=$LOGS/$tag.eventlog.jsonl
FAMARG=""; [ -n "${EXTCMP_VERIFY_FAMILY:-}" ] && FAMARG="--family $EXTCMP_VERIFY_FAMILY"
ref=$STORES/native-$tag
out=$RES/$tag; mkdir -p "$out"; [ "$rot" != - ] && mkdir -p "$out/rot$rot"
case $kind in
  load)        rec=$out/load-$sys.json ;;
  verify)      rec=$out/verify-$sys${P1_VTAG:+-$P1_VTAG}.json ;;
  aeongverify) rec=$out/verify-aeong.json ;;
  block)       rec=$out/rot$rot/$fam-$sys.json ;;
  aeong)       rec=$out/rot$rot/aeong-done.json ;;
esac
if [ -e "$rec" ] && ! grep -q '"yielded": true' "$rec"; then echo "skip (done) $kind $tag $rot $fam $sys"; exit 0; fi
if [ -z "${EXTCMP_INNER:-}" ]; then
  case $kind in
    verify|aeongverify) EXTCMP_INNER=1 heavy_run "$0" "$@"; exit $? ;;
    *)                  EXTCMP_INNER=1 timed_run "$0" "$@"; exit $? ;;
  esac
fi
utc() { date -u +%FT%TZ; }
died_note() {   # a query step that saw its server die (rc 76) has recorded it
  if [ "$rc" = 76 ]; then echo "=== SERVER DIED $sys $tag rot$rot $fam $(utc) (cells after the death: server died)"; rc=0; fi; }
"$KIT/census.sh" "$out/census.jsonl" "before $kind $tag $rot $fam $sys"
cpud=$out/cpu; mkdir -p "$cpud"
wid=$kind-rot$rot-$fam-$sys-$(date -u +%Y%m%dT%H%M%SZ)
python3 "$LIB/cpu_others.py" snap "$cpud/$wid-acquire.json" $$ acquire >> "$out/census.jsonl"
"$LIB/servers.sh" stopall
echo "=== window $kind $tag rot$rot $fam $sys start $(utc)"
server=0; case $sys in postgres|clickhouse|neo4j|memgraph|xtdb|aeong) server=1 ;; esac
if [ $server = 1 ]; then
  "$LIB/servers.sh" start "$sys" "$tag" || exit 1
  export EXTCMP_CONTAINER="extcmp-$sys-$tag"
  export EXTCMP_STATE_CMD="$LIB/servers.sh state $sys $tag" EXTCMP_LOGS_CMD="$LIB/servers.sh logs $sys $tag 40"
  $PY wait --system "$sys" --timeout 3600 || { "$LIB/servers.sh" logs "$sys" "$tag" 200 > "$out/serverlog-$sys-failed.txt" 2>&1
                                               "$LIB/servers.sh" stop "$sys" "$tag"; exit 1; }
fi
budget=3600; [ "$size" -ge 10000000 ] && budget=14400; [ "$size" -ge 100000000 ] && budget=43200
# EXTCMP_LOAD_BUDGET_S: an explicit load budget, e.g. for Gnomon-on-DuckDB-storage at
# 10M d10 on a shared node; unset, the size rule above applies
budget=${EXTCMP_LOAD_BUDGET_S:-$budget}
LOADRUN="timeout -k 60 $budget"
over=$out/load-$sys.json
rc=0
# a load that exceeded its budget left a partial store: neither its timed blocks nor its answer check may read it
#
if { [ "$kind" = block ] || [ "$kind" = verify ]; } && grep -q '"load exceeded budget": true' "$over" 2>/dev/null; then
  echo "{\"system\": \"$sys\", \"results\": [], \"not_run\": \"load exceeded budget\"}" > "$rec"
else
case $kind in
  load)
    if [ "$sys" = native ]; then $LOADRUN $PY load --system native --log "$log" --workdir "$EXTCMP_ROOT" --size-tag "$tag" --json "$rec.tmp"; rc=$?
    else $LOADRUN $PY load --system "$sys" --log "$log" --ref "$ref" --workdir "$EXTCMP_ROOT" --size-tag "$tag" --json "$rec.tmp"; rc=$?; fi
    if [ $rc = 124 ] || [ $rc = 137 ]; then
      echo "{\"system\": \"$sys\", \"load exceeded budget\": true, \"budget_s\": $budget, \"stopped_utc\": \"$(utc)\"}" > "$rec"; rc=0
    fi ;;
  verify)
    $PY query --system "$sys" --log "$log" --ref "$ref" --workdir "$EXTCMP_ROOT" --size-tag "$tag" --mode verify \
        $FAMARG --answers "$out/answers-$sys${P1_VTAG:+-$P1_VTAG}" --json "$rec.tmp" ${EXTCMP_PARAMS:+--params "$EXTCMP_PARAMS"}; rc=$?
    died_note ;;
  aeongverify)
    $LOADRUN $PY load --system aeong --log "$log" --ref "$ref" --workdir "$EXTCMP_ROOT" --size-tag "$tag" \
        --json "$out/load-aeong.json"; rc=$?
    [ $rc = 0 ] && { $PY query --system aeong --log "$log" --ref "$ref" --workdir "$EXTCMP_ROOT" --size-tag "$tag" \
        --mode verify $FAMARG --answers "$out/answers-aeong" --json "$rec.tmp"; rc=$?; } ;;
  block)
    $PY query --system "$sys" --log "$log" --ref "$ref" --workdir "$EXTCMP_ROOT" --size-tag "$tag" --mode timed \
        --family "$fam" --answers "$out/rot$rot/answers-$sys" --json "$rec.tmp" ${EXTCMP_PARAMS:+--params "$EXTCMP_PARAMS"}; rc=$?
    died_note ;;
  aeong)
    $LOADRUN $PY load --system aeong --log "$log" --ref "$ref" --workdir "$EXTCMP_ROOT" --size-tag "$tag" \
        --json "$out/rot$rot/load-aeong.json"; rc=$?
    if [ $rc = 0 ]; then
      dead=""
      for f in ${EXTCMP_FAMILIES:-P T S W D N R M}; do
        if [ -n "$dead" ]; then
          $PY died --system aeong --family "$f" --log "$log" --json "$out/rot$rot/$f-aeong.json" \
              --reason "AeonG server died during family $dead in this window"
          continue
        fi
        $PY query --system aeong --log "$log" --ref "$ref" --workdir "$EXTCMP_ROOT" --size-tag "$tag" --mode timed \
            --family "$f" --answers "$out/rot$rot/answers-aeong" --json "$out/rot$rot/$f-aeong.json"; frc=$?
        if [ $frc = 76 ]; then dead=$f; echo "=== SERVER DIED aeong $tag rot$rot family $f $(utc)"
        elif [ $frc != 0 ]; then rc=$frc; fi
      done
    elif [ $rc = 124 ] || [ $rc = 137 ]; then
      echo "{\"system\": \"aeong\", \"load exceeded budget\": true, \"budget_s\": $budget}" > "$out/rot$rot/load-aeong.json"; rc=0
    fi
    [ $rc = 0 ] && echo "{\"done\": \"$(utc)\"}" > "$rec.tmp" ;;
esac
fi
[ $rc = 0 ] && [ -e "$rec.tmp" ] && mv "$rec.tmp" "$rec"
if [ $server = 1 ]; then
  [ "$kind" = load ] && "$LIB/servers.sh" inspect "$sys" "$tag" > "$out/inspect-$sys.json" 2>&1
  "$LIB/servers.sh" logs "$sys" "$tag" 100 > "$out/serverlog-$sys-last.txt" 2>&1
  "$LIB/servers.sh" stop "$sys" "$tag"
  [ "$kind" = load ] && echo "{\"system\": \"$sys\", \"data_dir_bytes_after_stop\": $("$LIB/servers.sh" bytes "$sys" "$tag")}" > "$out/bytes-$sys.json"
fi
"$LIB/servers.sh" stopall
python3 "$LIB/cpu_others.py" snap "$cpud/$wid-release.json" $$ release >> "$out/census.jsonl"
cpuw=$(python3 "$LIB/cpu_others.py" window "$cpud/$wid-acquire.json" "$cpud/$wid-release.json")
[ $? = 1 ] && echo "=== CPU OUTSIDE WINDOW $kind $tag rot$rot $fam $sys: $cpuw"
echo "$cpuw" >> "$out/census.jsonl"
"$KIT/census.sh" "$out/census.jsonl" "after $kind $tag $rot $fam $sys rc=$rc"
echo "=== window $kind $tag rot$rot $fam $sys end $(utc) rc=$rc"
# on a batch host the job script copies the records to persistent storage here
[ -n "${EXTCMP_SYNC_HOOK:-}" ] && eval "$EXTCMP_SYNC_HOOK"
exit $rc
