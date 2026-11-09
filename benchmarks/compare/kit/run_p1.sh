#!/usr/bin/env bash
# P1 parameter-sensitivity study: every
# parameterisable query over the curated instances of a versioned parameter
# file (../p1/params-<point>.json, scripts/extcmp_p1.py), on every system of
# the job, with the same windows, servers and records as run_duckdbsql.sh.
#
#   EXTCMP_ROOT=... kit/run_p1.sh [--points "K10M K10M-d0 K10M-d10"]
#       [--rotations 2] [--steps loads,verify,timed,tables]
#   (P1_POINTS / P1_SYSTEMS in the environment; space- or plus-separated)
#
# Point -> log and families: K10M -> the registry families P T S W D N R M on
# eventlogs/synth-10000000.eventlog.jsonl; K10M-d<x> -> the belief-time family
# B on eventlogs/K10M-d<x>.eventlog.jsonl. Parameter file: P1_PARAMS_DIR
# (default ../p1 beside this kit)/params-<point>.json; its sha256 is checked
# against P1_PARAMS_DIR/SHA256SUMS before anything runs.
#
# Per point: budgeted loads (window.sh load), the untimed answer check of
# every instance on every system (window.sh verify with EXTCMP_PARAMS), the
# per-instance exclusion gate (a cell that differs from native, errs, or
# reaches the P1 ceiling at the answer check is not timed and carries its
# label; a failing native instance stops the point), the rotations, the
# tables (extcmp_p1.py report). Protocol per instance (extcmp_p1.measure):
# 2 warmups + 10 repetitions; first warmup > EXTCMP_P1_SLOW_S -> 1 warmup +
# 3 repetitions; a call longer than EXTCMP_P1_CEILING_S is recorded as above it
# (PostgreSQL/ClickHouse cancel it at EXTCMP_SERVER_LIMIT_S, set to the ceiling;
# the client alarm EXTCMP_CALL_CEILING_S / EXTCMP_SERVER_TIMEOUT_S sits above it).
#
# Rotation schedule: block b of rotation 1 runs the systems in P1_SYSTEMS
# order rotated by b; block b of rotation 2 runs rotation 1's block-b order
# reversed (the system order flipped).
set -uo pipefail
KIT=$(cd "$(dirname "$0")" && pwd); export KIT
POINTS=${P1_POINTS:-"K10M K10M-d0 K10M-d10"}; ROTS=2
STEPS="loads,verify,timed,tables"
while [ $# -gt 0 ]; do case $1 in
  --points) POINTS=$2; shift 2 ;; --rotations) ROTS=$2; shift 2 ;; --steps) STEPS=$2; shift 2 ;;
  *) echo "unknown argument $1"; exit 2 ;; esac; done
POINTS=${POINTS//+/ }; POINTS=${POINTS//,/ }
SYSLIST=${P1_SYSTEMS:-"native duckdbsql clickhouse postgres duckdb"}; SYSLIST=${SYSLIST//+/ }
read -r -a SYSTEMS <<< "$SYSLIST"
PDIR=${P1_PARAMS_DIR:-$(cd "$KIT/../p1" && pwd)}
source "$KIT/lib/env.sh"
source "$KIT/lib/lock.sh"
P1PY="$REPO/.venv/bin/python $REPO/scripts/extcmp_p1.py"
has() { case ",$STEPS," in *",$1,"*) return 0 ;; esac; return 1; }
say() { echo "=== $* $(date -u +%FT%TZ)"; }
fams_of() { case $1 in *-d*) echo B ;; *) echo P,T,S,W,D,N,R,M ;; esac; }
# EXTCMP_WINDOW_TIMEOUT_S: a window that outlives it is killed and
# recorded as failed (a DuckDB (SQL) verify window hung for 7 h after an interrupted call; PROBLEMS W2-K1)
win() { ${EXTCMP_WINDOW_TIMEOUT_S:+timeout -k 60 $EXTCMP_WINDOW_TIMEOUT_S} "$KIT/lib/window.sh" "$@" || echo "WINDOW FAILED $* $(date -u +%FT%TZ)"; }
( cd "$PDIR" && sha256sum -c SHA256SUMS ) || { echo "P1 parameter files do not match $PDIR/SHA256SUMS"; exit 2; }
export EXTCMP_P1_SLOW_S=${EXTCMP_P1_SLOW_S:-1.0} EXTCMP_P1_CEILING_S=${EXTCMP_P1_CEILING_S:-$EXTCMP_CALL_CEILING_S}
export EXTCMP_SERVER_LIMIT_S=${EXTCMP_SERVER_LIMIT_S:-${EXTCMP_P1_CEILING_S%.*}}
echo "P1 protocol: ceiling ${EXTCMP_P1_CEILING_S}s, server-side limit ${EXTCMP_SERVER_LIMIT_S}s, client alarm ${EXTCMP_CALL_CEILING_S}s (answer check ${EXTCMP_VERIFY_CAP_S}s), slow rule ${EXTCMP_P1_SLOW_S}s, systems [${SYSTEMS[*]}]"
"$KIT/census.sh" "$RES/census-campaign.jsonl" "P1 start points=[$POINTS] systems=[${SYSTEMS[*]}]"

for tag in $POINTS; do
  size=${P1_SIZE:-10000000}; fams=$(fams_of "$tag")
  export EXTCMP_PARAMS=$PDIR/params-$tag.json
  [ -e "$EXTCMP_PARAMS" ] || { echo "missing $EXTCMP_PARAMS"; exit 2; }
  if [ "$fams" = B ]; then
    export EXTCMP_BELIEFQ_INDEXES=1 EXTCMP_VERIFY_FAMILY=B
    [ -e "$LOGS/$tag.eventlog.jsonl" ] || { echo "missing $LOGS/$tag.eventlog.jsonl"; exit 2; }
  else
    unset EXTCMP_BELIEFQ_INDEXES EXTCMP_VERIFY_FAMILY
    [ -e "$LOGS/synth-$size.eventlog.jsonl" ] || { echo "missing $LOGS/synth-$size.eventlog.jsonl"; exit 2; }
  fi
  mkdir -p "$RES/$tag"
  cp "$EXTCMP_PARAMS" "$RES/$tag/params-used.json"
  if has loads; then
    say "loads $tag"
    for s in "${SYSTEMS[@]}"; do win load "$size" "$tag" - - "$s"; done
  fi
  if has verify; then
    say "answer check $tag (every instance, every system)"
    if [ "$fams" = B ]; then
      for s in "${SYSTEMS[@]}"; do win verify "$size" "$tag" - - "$s"; done
    else
      # the registry point: one verify record per family (a window each), so a
      # long answer check is resumable and the records stay small
      for s in "${SYSTEMS[@]}"; do
        for fam in ${fams//,/ }; do ( export EXTCMP_VERIFY_FAMILY=$fam P1_VTAG=$fam; win verify "$size" "$tag" - - "$s" ); done
      done
    fi
    $P1PY gate --dir "$RES/$tag" --ceiling "$EXTCMP_P1_CEILING_S" \
        --out "$RES/$tag/excluded.json" | tee "$RES/$tag/verdicts.txt"
    [ "${PIPESTATUS[0]}" = 0 ] || { echo "answer check FAILED at $tag (native reference): see verdicts.txt"; exit 3; }
  fi
  if has timed; then
    if [ -s "$RES/$tag/excluded.json" ]; then export EXTCMP_EXCLUDE="$RES/$tag/excluded.json"; else unset EXTCMP_EXCLUDE; fi
    n=${#SYSTEMS[@]}
    for r in $(seq 1 "$ROTS"); do
      say "rotation $r $tag"
      f=$RES/schedule-$tag-$r.txt; : > "$f"
      b=0
      for fam in ${fams//,/ }; do
        k=$(( b % n )); order=("${SYSTEMS[@]:k}" "${SYSTEMS[@]:0:k}")
        if [ $(( r % 2 )) = 0 ]; then
          rev=(); for (( i=${#order[@]}-1; i>=0; i-- )); do rev+=("${order[i]}"); done; order=("${rev[@]}")
        fi
        for s in "${order[@]}"; do echo "block $size $tag $r $fam $s" >> "$f"; done
        b=$((b + 1))
      done
      while read -r kind sz tg rot fam sys; do
        # R-10M: EXTCMP_R10M_FROM = the W2 1M timed results; at a 10M point every system's skip
        # list is built from its 1M records (K10M <- K1M, K10M-dX <- K1M-dX) and the block reads it
        if [ -n "${EXTCMP_R10M_FROM:-}" ] && [ "${tg#K10M}" != "$tg" ]; then
          sk=$RES/$tg/r10m-$sys.json
          [ -e "$sk" ] || $REPO/.venv/bin/python $REPO/scripts/r10m.py build --from "$EXTCMP_R10M_FROM" --point "K1M${tg#K10M}" --system "$sys" --out "$sk"
          EXTCMP_R10M_SKIP=$sk win "$kind" "$sz" "$tg" "$rot" "$fam" "$sys"
        else
          win "$kind" "$sz" "$tg" "$rot" "$fam" "$sys"
        fi
      done < "$f"
    done
  fi
  if has tables; then
    $P1PY report --dir "$RES/$tag" --rotations "$ROTS" \
        --out "$RES/$tag/p1-report" > "$RES/$tag/p1-report.md" || echo "report failed for $tag"
  fi
done
"$KIT/census.sh" "$RES/census-campaign.jsonl" "P1 end"
say "P1 done"
