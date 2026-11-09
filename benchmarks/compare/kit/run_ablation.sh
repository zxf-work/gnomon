#!/usr/bin/env bash
# ("Gnomon native: ablation configurations") against the all-unset engine.
#
#   EXTCMP_ROOT=/nvme/extcmp kit/run_ablation.sh --commit <engine commit> \
#       [--sizes 10000000[,100000000]] [--densities 0,1,10] [--rotations 2] \
#       [--families P,T,S,W,D,N,R,M,B] [--arms <list; default all, see lib/abl_schedule.py>] \
#       [--ingest-sizes 10000000] [--ingest-reps 3] [--label ablation] \
#       [--steps setup,logs,stores,timed,ingest,report] [--smoke]
#
# --smoke: a kit smoke test; refused unless every size (and ingest size) is <=
# 1,000,000 events; it forces the label to smoke-<label> and the record label to
# "... kit smoke — not for publication", and says so in every record.
#
# setup : as run_campaign.sh (clone at the engine commit, build, images)
# logs  : the frozen base log per size and one log per correction density
#         (run_beliefq.sh steps freeze,inject; shared with the study)
# stores: per density point, the native stores the arms read: u (replay,
#         size-tiered, all switches unset), settled (u + settle + gc), raw and
#         runs (replays with GNOMON_TIME_CODEC set: a write-time switch shapes
#         the store). Untimed, shared lock on a shared host; receipts kept.
# timed : per rotation, per family block, every arm paired with the unset
#         engine back to back (lib/abl_schedule.py), plus the A/A pair (unset
#         vs unset) that measures the noise every ratio is read against. The
#         engine's query registry (families P..M) and the belief-time family B.
# ingest: ingest timing for the ingest-side configurations, interleaved per
#         repetition: unset, unset again (A/A), GNOMON_TIME_CODEC=raw, =runs,
#         GNOMON_EXISTENCE_PROBE=walk, full-store compaction (--compact-every
#         1000000), GNOMON_INGEST_RAW=0; build_synth_store.py in the September
#         configuration. Then replay ingest: the frozen base log of each ingest
#         size replayed into a fresh native store (extcmp.py load, the
#         campaign's native load): unset, unset again (A/A), GNOMON_INGEST_RAW=0,
#         interleaved per repetition (<label>/ingest-replay/)
# report: lib/abl_report.py -> <label>/report.md, report.json (answers
#         identical per pair, median ratio per query and family, A/A band)
set -uo pipefail
KIT=$(cd "$(dirname "$0")" && pwd); export KIT
COMMIT=${EXTCMP_ENGINE_COMMIT:-}; SIZES="10000000"; DENS="0,1,10"; ROTS=2
FAMS="P,T,S,W,D,N,R,M,B"; ARMS=""; ISIZES="10000000"; IREPS=3; LABEL=ablation; SMOKE=0
STEPS="setup,logs,stores,timed,ingest,report"
while [ $# -gt 0 ]; do case $1 in
  --commit) COMMIT=$2; shift 2 ;; --sizes) SIZES=$2; shift 2 ;; --densities) DENS=$2; shift 2 ;;
  --rotations) ROTS=$2; shift 2 ;; --families) FAMS=$2; shift 2 ;; --arms) ARMS=$2; shift 2 ;;
  --ingest-sizes) ISIZES=$2; shift 2 ;; --ingest-reps) IREPS=$2; shift 2 ;;
  --label) LABEL=$2; shift 2 ;; --steps) STEPS=$2; shift 2 ;; --smoke) SMOKE=1; shift ;;
  *) echo "unknown argument $1"; exit 2 ;; esac; done
[ -n "$COMMIT" ] || { echo "--commit (or EXTCMP_ENGINE_COMMIT) is required"; exit 2; }
source "$KIT/lib/env.sh"
source "$KIT/lib/lock.sh"
if [ "$SMOKE" = 1 ]; then
  for n in ${SIZES//,/ } ${ISIZES//,/ }; do
    [ "$n" -le 1000000 ] || { echo "--smoke is for kit smoke tests only: size $n > 1,000,000 refused"; exit 2; }
  done
  LABEL=smoke-$LABEL
  case ${EXTCMP_RECORD_LABEL:-} in
    *"kit smoke — not for publication"*) ;;
    *) export EXTCMP_RECORD_LABEL="${EXTCMP_RECORD_LABEL:-$(hostname -s)}, kit smoke — not for publication" ;;
  esac
  export ABL_CONFIG_GATE="smoke"
else
  export ABL_CONFIG_GATE="none"
fi
# every configuration of lib/ablation_configs.json can be timed: no gate
export ABL_CONFIG_GATE_1=$ABL_CONFIG_GATE ABL_CONFIG_GATE_2=$ABL_CONFIG_GATE ABL_CONFIG_GATE_C4=$ABL_CONFIG_GATE
NEW_ARMS="agg_range_count_off reach_kernel_python diff_kernel_python ingest_raw_off exec_python duckdb_storage"
NEW_ARMS2="staircase_off staircase_off_settled"
NEW_ARMS6=$(python3 "$KIT/lib/abl_schedule.py" --new)
gate_ok() { return 0; }
configs_1_ok() { return 0; }
configs_2_ok() { return 0; }
configs_c4_ok() { return 0; }
export ABL_LABEL=$LABEL
has() { case ",$STEPS," in *",$1,"*) return 0 ;; esac; return 1; }
say() { echo "=== $* $(date -u +%FT%TZ)"; }
sz() { case $1 in 1000000) echo 1M ;; 10000000) echo 10M ;; 100000000) echo 100M ;; *) echo "$1" ;; esac; }
A=$RES/$LABEL; mkdir -p "$A"
arms_csv=${ARMS:-$(python3 -c "import sys; sys.path.insert(0, '$KIT/lib'); import abl_schedule as S; print(','.join(S.DEFAULT_ARMS))")}
need() { case ",$arms_csv," in *",$1,"*) return 0 ;; esac; return 1; }

has setup && { "$KIT/run_campaign.sh" --commit "$COMMIT" --steps setup || exit 1; }
has logs && { "$KIT/run_beliefq.sh" --commit "$COMMIT" --sizes "$SIZES" --densities "$DENS" \
                  --steps freeze,inject || exit 1; }
{ echo "{\"engine_commit\": \"$(git -C "$REPO" rev-parse HEAD)\", \"kit_commit\": \"$(git -C "$KIT" rev-parse HEAD)\","
  echo " \"sizes\": \"$SIZES\", \"densities\": \"$DENS\", \"rotations\": $ROTS, \"families\": \"$FAMS\", \"arms\": \"$arms_csv\","
  echo " \"config_gate\": \"$ABL_CONFIG_GATE\", \"config_gate_1\": \"$ABL_CONFIG_GATE_1\","
  echo " \"config_gate_2\": \"$ABL_CONFIG_GATE_2\","
  echo " \"record_label\": \"${EXTCMP_RECORD_LABEL:-}\"}"
} > "$A/provenance.json"

build_variant() {   # tag variant
  local tag=$1 v=$2 st=$STORES/native-$1-$2 log=$LOGS/$1.eventlog.jsonl r=$A/$1/stores
  mkdir -p "$r"
  [ -e "$r/$v.json" ] && return 0
  case $v in
    u)    heavy_run $PY load --system native --log "$log" --workdir "$EXTCMP_ROOT" --size-tag "$tag-u" --json "$r/u.json" ;;
    raw|runs)
          heavy_run env GNOMON_TIME_CODEC=$v $PY load --system native --log "$log" --workdir "$EXTCMP_ROOT" \
              --size-tag "$tag-$v" --json "$r/$v.json" ;;
    duckdb)
          build_variant "$tag" u || return 1
          heavy_run $PY load --system duckdb --log "$log" --ref "$STORES/native-$tag-u" --workdir "$EXTCMP_ROOT" \
              --size-tag "$tag" --json "$r/duckdb.json" ;;
    pyrows)
          heavy_run env GNOMON_INGEST_RAW=0 $PY load --system native --log "$log" --workdir "$EXTCMP_ROOT" \
              --size-tag "$tag-$v" --json "$r/$v.json" ;;
    settled)
          build_variant "$tag" u || return 1
          rm -rf "$st"; cp -a "$STORES/native-$tag-u" "$st" || return 1
          heavy_run bash -c '"$1/.venv/bin/gnomon" store settle --store "$2" --json > "$3" &&
                             "$1/.venv/bin/gnomon" store gc --store "$2" --keep 2 --json > "$4"' \
              _ "$REPO" "$st" "$r/settle-report.json" "$r/settle-gc.json" || return 1
          echo "{\"variant\": \"settled\", \"from\": \"native-$tag-u\", \"bytes\": $(du -sb "$st" | awk '{print $1}')}" > "$r/settled.json" ;;
  esac
}

for size in ${SIZES//,/ }; do
  for d in ${DENS//,/ }; do
    tag=K$(sz "$size")-d$d
    if has stores; then
      say "stores $tag"
      variants="u"
      { need full_store_compaction || need staircase_off_settled; } && variants="$variants settled"
      need time_codec_raw && variants="$variants raw"
      need time_codec_runs && variants="$variants runs"
      need ingest_raw_off && variants="$variants pyrows"
      need duckdb_storage && variants="$variants duckdb"
      for v in $variants; do
        build_variant "$tag" "$v" || { echo "STORE BUILD FAILED $tag $v"; exit 2; }
        _s=$STORES/native-$tag-$v; [ "$v" = duckdb ] && _s=$STORES/duckdb-$tag
        [ -d "$_s" ] && [ -n "$(ls -A "$_s" 2>/dev/null)" ] || { echo "STORE BUILD LEFT NOTHING $_s: stopping"; exit 2; }
      done
    fi
    if has timed; then
      for r in $(seq 1 "$ROTS"); do
        say "ablation rotation $r $tag"
        f=$A/$tag/schedule-rot$r.txt; mkdir -p "$A/$tag"
        python3 "$KIT/lib/abl_schedule.py" "$r" "$FAMS" "$arms_csv" > "$f"
        while read -r name fam variant envspec; do
          "$KIT/lib/abl_window.sh" "$size" "$tag" "$r" "$name" "$fam" "$variant" "$envspec"; wrc=$?
          [ $wrc = 2 ] && { echo "ABLATION STOPPED $tag rot$r $fam $name: missing input $(date -u +%FT%TZ)"; exit 2; }
          if [ $wrc != 0 ]; then
            echo "WINDOW FAILED ablation $tag rot$r $fam $name $(date -u +%FT%TZ)"
            echo "WINDOW FAILED ablation $tag rot$r $fam $name rc=$wrc" >> "$A/window-failed.txt"
          fi
          # ABL_PAIR_CHECK=1: a pair whose answers differ stops the run; the switches keep answers identical
          if [ "${ABL_PAIR_CHECK:-0}" = 1 ]; then
            python3 "$KIT/lib/abl_pair_check.py" "$A/$tag/rot$r/$fam" "$name"; prc=$?
            [ $prc = 0 ] || { echo "ANSWER MISMATCH $tag rot$r $fam $name: stopping $(date -u +%FT%TZ)"; [ -n "${EXTCMP_SYNC_HOOK:-}" ] && eval "$EXTCMP_SYNC_HOOK"; exit 7; }
          fi
        done < "$f"
      done
    fi
  done
done

if has ingest; then
  # ingest-side configurations, interleaved per repetition (order rotates)
  IARMS=("unset|" "unset_aa|" "time_codec_raw|GNOMON_TIME_CODEC=raw" "time_codec_runs|GNOMON_TIME_CODEC=runs"
         "existence_probe_walk|GNOMON_EXISTENCE_PROBE=walk" "full_store_compaction|--compact-every 1000000"
         "ingest_raw_off|GNOMON_INGEST_RAW=0")
  for size in ${ISIZES//,/ }; do
    for rep in $(seq 1 "$IREPS"); do
      say "ingest repetition $rep size $size"
      n=${#IARMS[@]}
      for i in $(seq 0 $((n - 1))); do
        spec=${IARMS[$(( (i + rep - 1) % n ))]}; arm=${spec%%|*}; set_=${spec#*|}
        name=$arm-$(sz "$size")-r$rep
        case $set_ in
          --*) "$KIT/lib/runbuild.sh" "$A/ingest" "$name" "$size" $set_ ;;
          "")  "$KIT/lib/runbuild.sh" "$A/ingest" "$name" "$size" ;;
          *)   env "$set_" "$KIT/lib/runbuild.sh" "$A/ingest" "$name" "$size" ;;
        esac || echo "BUILD FAILED $name $(date -u +%FT%TZ)"
      done
    done
  done
  # replay ingest (W32 F4 is a replay-path design): the size's frozen base log
  # replayed into a fresh native store by the campaign's native load, unset vs
  # unset again (A/A) vs GNOMON_INGEST_RAW=0, order rotating per repetition; one
  # record per replay (load.load_s), the store removed after it
  RARMS=("unset|" "unset_aa|" "ingest_raw_off|GNOMON_INGEST_RAW=0")
  mkdir -p "$A/ingest-replay"
  for size in ${ISIZES//,/ }; do
    rlog=$LOGS/synth-$size.eventlog.jsonl
    for rep in $(seq 1 "$IREPS"); do
      say "replay ingest repetition $rep size $size"
      for i in 0 1 2; do
        spec=${RARMS[$(( (i + rep - 1) % 3 ))]}; arm=${spec%%|*}; set_=${spec#*|}
        name=$arm-$(sz "$size")-r$rep; rec=$A/ingest-replay/$name.json
        [ -e "$rec" ] && { echo "skip (done) replay $name"; continue; }
        rm -rf "$STORES/native-replay-$name"
        if timed_run env ${set_:-ABL_REPLAY_ARM=$arm} $PY load --system native --log "$rlog" --workdir "$EXTCMP_ROOT" \
             --size-tag "replay-$name" --json "$rec.tmp" > /dev/null; then mv "$rec.tmp" "$rec"
        else echo "REPLAY FAILED $name $(date -u +%FT%TZ)"; fi
        rm -rf "$STORES/native-replay-$name"
        [ -n "${EXTCMP_SYNC_HOOK:-}" ] && eval "$EXTCMP_SYNC_HOOK"
      done
    done
  done
fi

if has report; then
  say report
  python3 "$KIT/lib/abl_report.py" "$A" > "$A/report.md" && cat "$A/report.md" | head -60
fi
say "ablation done"
