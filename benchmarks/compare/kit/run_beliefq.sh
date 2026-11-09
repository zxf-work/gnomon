#!/usr/bin/env bash
# Correction-density study (belief-time graph queries) with the campaign kit.
#
#   EXTCMP_ROOT=/nvme/extcmp kit/run_beliefq.sh --commit <engine commit> \
#       [--sizes 10000000,100000000] [--densities 0,1,10] [--rotations 2] \
#       [--steps setup,freeze,inject,loads,verify,timed,tables]
#
# Per input size: freeze the base log once. Per density point d: replay the
# base log into a scratch native store, inject d % corrections with the
# existing batched injector (seed 0, batch 1000; a timed window, receipt
# kept), and freeze base log + injected batches as that point's log
# (eventlogs/K<size>-d<d>.eventlog.jsonl). Then, per point, every system loads
# that one log (with the belief-axis indexes, EXTCMP_BELIEFQ_INDEXES=1), the
# belief-time family B is answer-checked against the native engine, and timed.
set -uo pipefail
KIT=$(cd "$(dirname "$0")" && pwd); export KIT
COMMIT=${EXTCMP_ENGINE_COMMIT:-}; SIZES="10000000,100000000"; DENS="0,1,10"; ROTS=2
STEPS="setup,freeze,inject,loads,verify,timed,tables"
while [ $# -gt 0 ]; do case $1 in
  --commit) COMMIT=$2; shift 2 ;; --sizes) SIZES=$2; shift 2 ;; --densities) DENS=$2; shift 2 ;;
  --rotations) ROTS=$2; shift 2 ;; --steps) STEPS=$2; shift 2 ;;
  *) echo "unknown argument $1"; exit 2 ;; esac; done
[ -n "$COMMIT" ] || { echo "--commit (or EXTCMP_ENGINE_COMMIT) is required"; exit 2; }
source "$KIT/lib/env.sh"
source "$KIT/lib/lock.sh"
has() { case ",$STEPS," in *",$1,"*) return 0 ;; esac; return 1; }
say() { echo "=== $* $(date -u +%FT%TZ)"; }
sz() { case $1 in 10000000) echo 10M ;; 100000000) echo 100M ;; 1000000) echo 1M ;; *) echo "$1" ;; esac; }
export EXTCMP_BELIEFQ_INDEXES=1 EXTCMP_VERIFY_FAMILY=B EXTCMP_FAMILIES=B

has setup && { "$KIT/run_campaign.sh" --commit "$COMMIT" --steps setup || exit 1; }

for size in ${SIZES//,/ }; do
  base=$LOGS/synth-$size.eventlog.jsonl
  if has freeze && [ ! -e "$base" ]; then
    say "freeze $size"
    heavy_run "$REPO/.venv/bin/python" "$REPO/scripts/freeze_synth_log.py" --scale "$size" --out "$base" || exit 1
  fi
  for d in ${DENS//,/ }; do
    tag=K$(sz "$size")-d$d
    dlog=$LOGS/$tag.eventlog.jsonl
    if has inject && [ ! -e "$dlog" ]; then
      say "inject $tag"
      if [ "$d" = 0 ]; then cp "$base" "$dlog"
      else
        st=$STORES/inject-$tag; rm -rf "$st"; mkdir -p "$RES/$tag"
        heavy_run "$REPO/.venv/bin/python" -c "
import sys; sys.path.insert(0, '$REPO/scripts')
import eval_harness as H; H.load_store(__import__('pathlib').Path('$st'), 'native', __import__('pathlib').Path('$base'))" || exit 1
        timed_run "$REPO/.venv/bin/python" "$REPO/scripts/inject_corrections.py" --store "$st" \
            --density "$d" --seed 0 --batch 1000 --record "$RES/$tag/inject-receipt.json" || exit 1
        { cat "$base"; grep '"tt"' "$st/eventlog.jsonl"; } > "$dlog"
        rm -rf "$st"
      fi
    fi
    run_part() {   # part
      local f=$RES/schedule-$tag-$1.txt
      python3 "$KIT/lib/schedule.py" "$size" "$tag" "$1" B > "$f"
      while read -r kind s_ tg rot fam sys; do
        "$KIT/lib/window.sh" "$kind" "$s_" "$tg" "$rot" "$fam" "$sys" || echo "WINDOW FAILED $kind $tg $rot $fam $sys $(date -u +%FT%TZ)"
      done < "$f"
    }
    has loads && { say "loads $tag"; run_part loads; }
    if has verify; then
      say "answer check $tag"; run_part verify
      others=$(ls "$RES/$tag"/verify-*.json | grep -v verify-native.json)
      $PY verdict --base "$RES/$tag/verify-native.json" --base-answers "$RES/$tag/answers-native" \
          $others --json "$RES/$tag/verdicts.json" | tee "$RES/$tag/verdicts.txt"
    fi
    if has timed; then for r in $(seq 1 "$ROTS"); do say "rotation $r $tag"; run_part "$r"; done; fi
    if has tables; then
      for r in $(seq 1 "$ROTS"); do
        [ -d "$RES/$tag/rot$r" ] && $PY bverdict --dir "$RES/$tag/rot$r" > "$RES/$tag/rot$r/table.md"
      done
    fi
    [ "${EXTCMP_WIPE_AFTER:-0}" = 1 ] && has tables && wipe_point "$tag"
  done
done
say "correction-density study done"
