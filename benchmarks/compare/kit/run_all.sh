#!/usr/bin/env bash
# Every stage is resumable (completed windows and builds are skipped), so a
# re-run after an interruption continues where it stopped.
#
#   EXTCMP_ROOT=/nvme/extcmp EXTCMP_ENGINE_COMMIT=<engine-upgrade tip> \
#       kit/run_all.sh [--stages census,setup,series,ext1m,ext10m,ext100m,beliefq10m,beliefq100m,ablation10m,readlat,ablation100m]
#
# Default stages: everything except the optional ablation100m. The engine
# commit is never hard-coded: EXTCMP_ENGINE_COMMIT (or --commit) names it,
# and every record carries it (provenance.json).
set -uo pipefail
KIT=$(cd "$(dirname "$0")" && pwd); export KIT
COMMIT=${EXTCMP_ENGINE_COMMIT:-}
STAGES="census,setup,series,ext1m,ext10m,ext100m,beliefq10m,beliefq100m,ablation10m,readlat"
while [ $# -gt 0 ]; do case $1 in
  --commit) COMMIT=$2; shift 2 ;; --stages) STAGES=$2; shift 2 ;;
  *) echo "unknown argument $1"; exit 2 ;; esac; done
[ -n "$COMMIT" ] || { echo "set EXTCMP_ENGINE_COMMIT (or --commit) to the engine-upgrade tip"; exit 2; }
export EXTCMP_ENGINE_COMMIT=$COMMIT
source "$KIT/lib/env.sh"
has() { case ",$STAGES," in *",$1,"*) return 0 ;; esac; return 1; }
stage() { echo "##### stage $1 $(date -u +%FT%TZ)"; }
fail() { echo "##### stage $1 FAILED $(date -u +%FT%TZ)"; exit 1; }

if has census; then stage census; "$KIT/census.sh" "$RES/census-campaign.jsonl" "host census before the campaign" || fail census; fi
if has setup; then stage setup; "$KIT/run_campaign.sh" --commit "$COMMIT" --steps setup || fail setup; fi
if has series; then stage series; "$KIT/run_engine_series.sh" --commit "$COMMIT" --steps ingest,report || fail series; fi
if has ext1m; then stage ext1m; "$KIT/run_campaign.sh" --commit "$COMMIT" --sizes 1000000 \
    --steps freeze,loads,verify,timed,tables || fail ext1m; fi
if has ext10m; then stage ext10m; "$KIT/run_campaign.sh" --commit "$COMMIT" --sizes 10000000 \
    --steps freeze,loads,verify,timed,tables || fail ext10m; fi
# at 100M the in-memory systems need more than the 10M limits (48 GB container
# cap, 40 GB Memgraph/AeonG memory limit); the settings are recorded per window
BIG="EXTCMP_MEM_CAP=${EXTCMP_MEM_CAP_100M:-192g} EXTCMP_MG_MEMORY_LIMIT_MB=${EXTCMP_MG_MEMORY_LIMIT_MB_100M:-180000}"
if has ext100m; then stage ext100m; env $BIG EXTCMP_WIPE_AFTER=1 "$KIT/run_campaign.sh" --commit "$COMMIT" \
    --sizes 100000000 --steps freeze,loads,verify,timed,tables || fail ext100m; fi
if has beliefq10m; then stage beliefq10m; "$KIT/run_beliefq.sh" --commit "$COMMIT" --sizes 10000000 \
    --steps freeze,inject,loads,verify,timed,tables || fail beliefq10m; fi
if has beliefq100m; then stage beliefq100m; env $BIG EXTCMP_WIPE_AFTER=1 "$KIT/run_beliefq.sh" --commit "$COMMIT" \
    --sizes 100000000 --steps freeze,inject,loads,verify,timed,tables || fail beliefq100m; fi
if has ablation10m; then
  stage ablation10m
  "$KIT/run_ablation.sh" --commit "$COMMIT" --sizes 10000000 --steps logs,stores,timed,ingest,report || fail ablation10m
fi
if has readlat; then stage readlat; "$KIT/run_engine_series.sh" --commit "$COMMIT" --steps readlat || fail readlat; fi
if has ablation100m; then stage ablation100m; "$KIT/run_ablation.sh" --commit "$COMMIT" --sizes 100000000 \
    --ingest-sizes 100000000 --ingest-reps 2 --label ablation100m --steps logs,stores,timed,ingest,report || fail ablation100m; fi
"$KIT/census.sh" "$RES/census-campaign.jsonl" "host census after the campaign"
echo "##### campaign complete $(date -u +%FT%TZ)"
