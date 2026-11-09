#!/usr/bin/env bash
# Portable external-comparison campaign: one entry point, one target commit.
#
#   EXTCMP_ROOT=/nvme/extcmp kit/run_campaign.sh --commit <engine commit> \
#       [--repo <git url or path>] [--sizes 1000000,10000000] [--rotations 2] \
#       [--steps setup,freeze,loads,verify,timed,tables]
#
# setup : clone the repository at the engine commit, overlay the comparison
#         layer (scripts/) from the kit's own commit, install pinned Python
#         packages, build the engine (maturin --release), pull every image by
#         digest, build AeonG from pinned source
# freeze: one synthetic event log per input size (the same log for every system)
# loads : timed, budgeted loads (1 h at 1M, 4 h at >= 10M); AeonG loads in its own windows
# verify: untimed answer check of every system against the native engine;
#         a system failing it on a query is not timed on that query (labelled
#         "excluded: DIFFERS|error"); stops only when the native reference
#         fails (EXTCMP_GATE=strict: stops on any unexpected disagreement)
# timed : the rotations (lib/schedule.py), one window per (family, system)
# tables: median / p95 / verdict per rotation (results/<tag>/rot<r>/table.md)
set -uo pipefail
KIT=$(cd "$(dirname "$0")" && pwd); export KIT
COMMIT=${EXTCMP_ENGINE_COMMIT:-}; REPO_SRC=""; SIZES="1000000,10000000"; ROTS=2; STEPS="setup,freeze,loads,verify,timed,tables"
while [ $# -gt 0 ]; do case $1 in
  --commit) COMMIT=$2; shift 2 ;; --repo) REPO_SRC=$2; shift 2 ;;
  --sizes) SIZES=$2; shift 2 ;; --rotations) ROTS=$2; shift 2 ;; --steps) STEPS=$2; shift 2 ;;
  *) echo "unknown argument $1"; exit 2 ;; esac; done
[ -n "$COMMIT" ] || { echo "--commit (or EXTCMP_ENGINE_COMMIT) is required"; exit 2; }
source "$KIT/lib/env.sh"
source "$KIT/lib/lock.sh"
has() { case ",$STEPS," in *",$1,"*) return 0 ;; esac; return 1; }
tagof() { case $1 in 1000000) echo K1M ;; 10000000) echo K10M ;; 100000000) echo K100M ;; *) echo "K$1" ;; esac; }
say() { echo "=== $* $(date -u +%FT%TZ)"; }
"$KIT/census.sh" "$RES/census-campaign.jsonl" "campaign start commit=$COMMIT"

if has setup && [ "$EXTCMP_BACKEND" = apptainer ]; then
  say "setup on an Apptainer host: staged by stage_hpc.sh and the build job; checking"
  for k in postgres clickhouse neo4j memgraph xtdb aeong; do
    [ -e "$SIF_DIR/$k.sif" ] || { echo "missing $SIF_DIR/$k.sif (stage_hpc.sh)"; exit 2; }; done
  "$REPO/.venv/bin/python" -c "import gnomon._engine" || { echo "engine not built (slurm stage build)"; exit 2; }
  STEPS=${STEPS//setup/}
fi
if has setup; then
  say preflight
  for c in docker git python3 uv cargo flock timeout sha256sum findmnt; do
    command -v $c >/dev/null || { echo "missing: $c (see kit/README.md, prerequisites)"; exit 2; }; done
  docker info >/dev/null || { echo "docker daemon not reachable"; exit 2; }
  KIT_REPO=$(git -C "$KIT" rev-parse --show-toplevel); KIT_COMMIT=$(git -C "$KIT" rev-parse HEAD)
  REPO_SRC=${REPO_SRC:-$KIT_REPO}
  say "repository at $COMMIT (comparison layer from $KIT_COMMIT)"
  [ -d "$REPO/.git" ] || git clone -q --no-checkout "$REPO_SRC" "$REPO"
  git -C "$REPO" fetch -q "$KIT_REPO" "$KIT_COMMIT" 2>/dev/null || true
  git -C "$REPO" checkout -q --detach "$COMMIT"
  # overlay only the comparison layer (extcmp*.py, *_baseline.py) from the
  # kit's commit; every other script stays the engine commit's own, so a newer
  # engine tip is never rolled back to the kit's older engine scripts
  OVERLAY=$(git -C "$REPO" diff --name-only "$COMMIT" "$KIT_COMMIT" -- scripts |
            grep -E '^scripts/(extcmp[A-Za-z0-9_]*|[a-z0-9]+_baseline)\.py$' || true)
  ENGINE_OWNED=$(git -C "$REPO" diff --name-only "$COMMIT" "$KIT_COMMIT" -- scripts |
                 grep -vE '^scripts/(extcmp[A-Za-z0-9_]*|[a-z0-9]+_baseline)\.py$' | tr '\n' ' ' || true)
  [ -n "$OVERLAY" ] && git -C "$REPO" checkout -q "$KIT_COMMIT" -- $OVERLAY
  { echo "{\"engine_commit\": \"$(git -C "$REPO" rev-parse HEAD)\", \"kit_commit\": \"$KIT_COMMIT\","
    echo " \"overlay\": \"$(echo $OVERLAY)\", \"repo_source\": \"$REPO_SRC\","
    echo " \"engine_scripts_kept_from_engine_commit\": \"$ENGINE_OWNED\"}"
  } > "$RES/provenance.json"
  say "python environment + engine build"
  # builds are heavy, untimed work: on a shared host they take the shared lock
  heavy_run bash -c 'cd "$1" && unset CARGO_TARGET_DIR && uv sync --extra eval --extra duckdb \
      && uv pip install -r "$2/python.lock" \
      && uv run --no-sync maturin develop --release --extras eval,duckdb' _ "$REPO" "$KIT" || exit 1
  "$REPO/.venv/bin/python" -c "import gnomon, gnomon._engine as e, ladybug, raphtory; print(e.__file__, ladybug.__version__, raphtory.__version__)" \
      > "$RES/engine-build.txt" || exit 1
  say "images by digest"
  while read -r key ref; do
    case $key in ''|\#*) continue ;; esac
    docker pull -q "$ref" || exit 1
    docker image inspect "$ref" --format "$key {{.Id}} {{json .RepoDigests}}" >> "$RES/images.txt"
  done < "$KIT/images.lock"
  say "AeonG"
  [ -x "$AEONG_SRC/AeonG/build-release/memgraph-_Release" ] || heavy_run "$KIT/build_aeong.sh" || exit 1
fi

run_part() {   # size tag part
  local f=$RES/schedule-$2-$3.txt
  python3 "$KIT/lib/schedule.py" "$1" "$2" "$3" > "$f"
  while read -r kind sz tg rot fam sys; do
    "$KIT/lib/window.sh" "$kind" "$sz" "$tg" "$rot" "$fam" "$sys" || echo "WINDOW FAILED $kind $tg $rot $fam $sys $(date -u +%FT%TZ)"
  done < "$f"
}

for size in ${SIZES//,/ }; do
  tag=$(tagof "$size")
  if has freeze; then
    say "freeze $size"
    if [ ! -e "$LOGS/synth-$size.eventlog.jsonl" ]; then
      heavy_run "$REPO/.venv/bin/python" "$REPO/scripts/freeze_synth_log.py" --scale "$size" \
          --out "$LOGS/synth-$size.eventlog.jsonl" || exit 1
    fi
  fi
  has loads && { say "loads $tag"; run_part "$size" "$tag" loads; }
  if has verify; then
    say "answer check $tag"; run_part "$size" "$tag" verify
    others=$(ls "$RES/$tag"/verify-*.json | grep -v verify-native.json)
    $PY verdict --base "$RES/$tag/verify-native.json" --base-answers "$RES/$tag/answers-native" \
        $others --json "$RES/$tag/verdicts.json" | tee "$RES/$tag/verdicts.txt"
    # gate: by default a system that fails the answer check on a
    # query is excluded from timing on that query only ("excluded: DIFFERS" /
    # "excluded: error", <tag>/excluded.json, read by extcmp.py through
    # EXTCMP_EXCLUDE); every other system and query proceeds. The stage still
    # stops when the reference itself fails (a native cell not ok, or "no
    # native reference"). EXTCMP_GATE=strict: the old stop on any unexpected
    # disagreement.
    rm -f "$RES/$tag/excluded.json"
    python3 - "$RES/$tag/verdicts.json" "$RES/$tag/verify-native.json" "$RES/$tag/excluded.json" \
        "${EXTCMP_GATE:-per-query}" <<'PY' || { echo "answer check FAILED at $tag: see verdicts.txt"; exit 3; }
import json, sys
verdicts, native, excl_path, mode = sys.argv[1:5]
# the only disagreement expected by construction: Raphtory's reachability
# follows strictly increasing event times (a different question)
EXPECTED = {("raphtory", "reach.window")}
bad = [(s, r["query"], r["verdict"]) for s, rows in json.load(open(verdicts)).items()
       for r in rows if r["verdict"] in ("DIFFERS", "error", "no native reference")
       and (s, r["query"]) not in EXPECTED]
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
    for r in $(seq 1 "$ROTS"); do say "rotation $r $tag"; run_part "$size" "$tag" "$r"; done
  fi
  if has tables; then
    for r in $(seq 1 "$ROTS"); do
      [ -d "$RES/$tag/rot$r" ] && $PY bverdict --dir "$RES/$tag/rot$r" > "$RES/$tag/rot$r/table.md"
    done
  fi
  [ "${EXTCMP_WIPE_AFTER:-0}" = 1 ] && has tables && wipe_point "$tag"
done
"$KIT/census.sh" "$RES/census-campaign.jsonl" "campaign end"
say "campaign done"
