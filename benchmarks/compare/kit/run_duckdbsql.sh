#!/usr/bin/env bash
# (scripts/extcmp_duckdbsql.py: every query as SQL inside DuckDB over the
# version tables), with the native engine timed in the same job as the
# same-node reference. Not the kit system `duckdb` (the engine's operators over
# the DuckDB storage backend), which this script never runs.
#
#   EXTCMP_ROOT=... kit/run_duckdbsql.sh [--points "K1M K10M K10M-d0 K10M-d1 K10M-d10"]
#       [--rotations 2] [--steps loads,verify,timed,tables]
#   (DDSQL_POINTS in the environment is the same as --points; points are
#   space- or plus-separated so they survive sbatch --export)
#
# Point -> input size and families: K1M, K10M -> the 16-id registry (families
# P T S W D N R M, log eventlogs/synth-<size>.eventlog.jsonl); K10M-d<x> -> the
# belief-time family B on the density point's frozen log
# eventlogs/K10M-d<x>.eventlog.jsonl (lib/window.sh picks a log named by the tag).
#
# Per point: timed budgeted loads (window.sh load), the untimed answer check
# (window.sh verify, `extcmp.py verdict`, the per-query gate of
# run_campaign.sh: a duckdbsql query that differs is not timed and is labelled
# "excluded: DIFFERS|error"; a failing native reference stops), the rotations,
# and the tables (`extcmp.py bverdict`). Windows, warmups, repetitions, the
# 300 s cap and the 3600 s ceiling are lib/window.sh and extcmp.py unchanged.
#
# DuckDB settings are environment settings recorded in every record
# (extcmp_duckdbsql.settings_report): EXTCMP_DUCKDBSQL_THREADS (default: the
# EXTCMP_DUCKDBSQL_MEMORY_LIMIT (default 32GB).
#
# Rotation schedule: per family block b of rotation r the system order is
# SYSTEMS rotated by (b + r - 1). lib/schedule.py rotates its nine systems by
# (b + 4(r - 1)); with two systems that offset is even, so the order would be
# the same in both rotations; (r - 1) flips it between rotations instead.
set -uo pipefail
KIT=$(cd "$(dirname "$0")" && pwd); export KIT
POINTS=${DDSQL_POINTS:-"K1M K10M K10M-d0 K10M-d1 K10M-d10"}; ROTS=2
STEPS="loads,verify,timed,tables"
while [ $# -gt 0 ]; do case $1 in
  --points) POINTS=$2; shift 2 ;; --rotations) ROTS=$2; shift 2 ;; --steps) STEPS=$2; shift 2 ;;
  *) echo "unknown argument $1"; exit 2 ;; esac; done
POINTS=${POINTS//+/ }; POINTS=${POINTS//,/ }
read -r -a SYSTEMS <<< "${DDSQL_SYSTEMS:-native duckdbsql}"
source "$KIT/lib/env.sh"
source "$KIT/lib/lock.sh"
has() { case ",$STEPS," in *",$1,"*) return 0 ;; esac; return 1; }
say() { echo "=== $* $(date -u +%FT%TZ)"; }
size_of() { case $1 in K1M*) echo 1000000 ;; K10M*) echo 10000000 ;; K100M*) echo 100000000 ;; K200k*) echo 200000 ;; *) echo "unknown point $1" >&2; exit 2 ;; esac; }
fams_of() { case $1 in *-d*) echo B ;; *) echo P,T,S,W,D,N,R,M ;; esac; }
win() { "$KIT/lib/window.sh" "$@" || echo "WINDOW FAILED $* $(date -u +%FT%TZ)"; }
"$KIT/census.sh" "$RES/census-campaign.jsonl" "duckdbsql stage start points=[$POINTS] systems=[${SYSTEMS[*]}]"

for tag in $POINTS; do
  size=$(size_of "$tag"); fams=$(fams_of "$tag")
  if [ "$fams" = B ]; then
    export EXTCMP_BELIEFQ_INDEXES=1 EXTCMP_VERIFY_FAMILY=B
    [ -e "$LOGS/$tag.eventlog.jsonl" ] || { echo "missing $LOGS/$tag.eventlog.jsonl"; exit 2; }
  else
    unset EXTCMP_BELIEFQ_INDEXES EXTCMP_VERIFY_FAMILY
    [ -e "$LOGS/synth-$size.eventlog.jsonl" ] || { echo "missing $LOGS/synth-$size.eventlog.jsonl"; exit 2; }
  fi
  mkdir -p "$RES/$tag"
  if has loads; then
    say "loads $tag"
    for s in "${SYSTEMS[@]}"; do win load "$size" "$tag" - - "$s"; done
  fi
  if has verify; then
    say "answer check $tag"
    for s in "${SYSTEMS[@]}"; do win verify "$size" "$tag" - - "$s"; done
    others=$(ls "$RES/$tag"/verify-*.json | grep -v verify-native.json)
    $PY verdict --base "$RES/$tag/verify-native.json" --base-answers "$RES/$tag/answers-native" \
        $others --json "$RES/$tag/verdicts.json" | tee "$RES/$tag/verdicts.txt"
    # the per-query gate of run_campaign.sh, unchanged
    rm -f "$RES/$tag/excluded.json"
    python3 - "$RES/$tag/verdicts.json" "$RES/$tag/verify-native.json" "$RES/$tag/excluded.json" \
        "${EXTCMP_GATE:-per-query}" <<'PY' || { echo "answer check FAILED at $tag: see verdicts.txt"; exit 3; }
import json, sys
verdicts, native, excl_path, mode = sys.argv[1:5]
bad = [(s, r["query"], r["verdict"]) for s, rows in json.load(open(verdicts)).items()
       for r in rows if r["verdict"] in ("DIFFERS", "error", "no native reference")]
print("unexpected:", bad or "none")
if mode == "strict":
    sys.exit(1 if bad else 0)
ref_bad = [r["query"] for r in json.load(open(native))["results"] if not r.get("ok")]
noref = [b for b in bad if b[2] == "no native reference"]
if ref_bad or noref:
    print("reference failed: native cells not ok", ref_bad, "; no native reference", noref)
    sys.exit(1)
excl = {}
for s, q, v in bad:
    excl.setdefault(s, {})[q] = f"excluded: {v}"
json.dump(excl, open(excl_path, "w"), indent=1, sort_keys=True)
print("excluded from timing (this system, this query only):", bad or "none")
PY
  fi
  if has timed; then
    if [ -s "$RES/$tag/excluded.json" ]; then export EXTCMP_EXCLUDE="$RES/$tag/excluded.json"; else unset EXTCMP_EXCLUDE; fi
    for r in $(seq 1 "$ROTS"); do
      say "rotation $r $tag"
      f=$RES/schedule-$tag-$r.txt; : > "$f"
      b=0
      for fam in ${fams//,/ }; do
        n=${#SYSTEMS[@]}; k=$(( (b + r - 1) % n ))
        for s in "${SYSTEMS[@]:k}" "${SYSTEMS[@]:0:k}"; do echo "block $size $tag $r $fam $s" >> "$f"; done
        b=$((b + 1))
      done
      while read -r kind sz tg rot fam sys; do win "$kind" "$sz" "$tg" "$rot" "$fam" "$sys"; done < "$f"
    done
  fi
  if has tables; then
    for r in $(seq 1 "$ROTS"); do
      [ -d "$RES/$tag/rot$r" ] && $PY bverdict --dir "$RES/$tag/rot$r" > "$RES/$tag/rot$r/table.md"
    done
  fi
done
"$KIT/census.sh" "$RES/census-campaign.jsonl" "duckdbsql stage end"
say "duckdbsql stage done"
