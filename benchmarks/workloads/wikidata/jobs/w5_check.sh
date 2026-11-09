#!/bin/bash
# W5 answer check (run inside kit/slurm/job_cmd.sbatch as STAGE_CMD): load the frozen log into
# every system, curate parameters on the native store, check every instance on every system
# against native (Gnomon), and the KG match patterns native vs reference.
#   W5_TAG=W5-smallpedia W5_SIZE=200000 W5_SYSTEMS="native duckdbsql postgres neo4j"
set -uo pipefail
source "$KIT/lib/env.sh"
tag=${W5_TAG:?}; size=${W5_SIZE:-200000}; SYSTEMS=(${W5_SYSTEMS:-native duckdbsql postgres neo4j})
say() { echo "=== $* $(date -u +%FT%TZ)"; }
win() { "$KIT/lib/window.sh" "$@" || echo "WINDOW FAILED $* $(date -u +%FT%TZ)"; }
export EXTCMP_P1_CEILING_S=${EXTCMP_P1_CEILING_S:-600} EXTCMP_SERVER_LIMIT_S=${EXTCMP_SERVER_LIMIT_S:-600} EXTCMP_P1_SLOW_S=1.0
PYV=$REPO/.venv/bin/python; L=$LOGS/$tag.eventlog.jsonl; PD=${W5_PARAMS_DIR:-$EXTCMP_PROJECT/params}; mkdir -p $PD
say load native; win load "$size" "$tag" - - native
say params
if [ ! -s $PD/params-$tag-reg.json ]; then
  $PYV $REPO/scripts/wd_params.py --log $L --out $PD || exit 2
  ( cd $PD && sha256sum params-$tag-*.json factors-$tag.json > SHA256SUMS-$tag )
fi
say match; $PYV $REPO/scripts/wd_match.py emit --factors $PD/factors-$tag.json --log $L --out $PD/match-$tag.json
mkdir -p $RES/$tag; $PYV $REPO/scripts/wd_match.py run --patterns $PD/match-$tag.json --store $STORES/native-$tag --json $RES/$tag/match-check.jsonl | tail -3
for s in "${SYSTEMS[@]}"; do [ $s = native ] || { say load $s; win load "$size" "$tag" - - "$s"; }; done
# the B family's template arguments come from extcmp_beliefq.derive_params, which assumes the
# synthetic generator; every W5 instance overrides them, so seed its sidecar cache
# with placeholders taken from the curated file (same keys, size and mtime of this log)
$PYV - "$L" "$PD/params-$tag-bel.json" <<'PY'
import json, os, sys
log, pf = sys.argv[1], sys.argv[2]; st = os.stat(log)
ins = {i["query"]: i["args"] for i in json.load(open(pf))["instances"]}
sn, rc, df, h = ins["b.snap.hop2@T"], ins["b.reach@T"], ins["b.diff"], ins["b.hist"]
d = {"size": st.st_size, "mtime_ns": st.st_mtime_ns, "snap_seeds": sn["seeds"], "t_star": sn["t_valid"],
     "seed_node": rc["src"], "reach_window": rc["window"], "T": sn["as_of_tt"], "T_base": df["t1"],
     "diff_window": df["window"], "e_star": h, "w5_placeholder": True}
open(log[:-len(".jsonl")] + ".beliefq-params.json", "w").write(json.dumps(d))
print("beliefq sidecar seeded (placeholders; overridden per instance)")
PY
for v in reg bel; do
  for s in "${SYSTEMS[@]}"; do say verify $v $s; ( export EXTCMP_PARAMS=$PD/params-$tag-$v.json P1_VTAG=$v; [ $v = bel ] && export EXTCMP_VERIFY_FAMILY=B; win verify "$size" "$tag" - - "$s" ); done
done
say gate; $PYV $REPO/scripts/extcmp_p1.py gate --dir "$RES/$tag" --ceiling "$EXTCMP_P1_CEILING_S" --out "$RES/$tag/excluded.json" | tee "$RES/$tag/verdicts.txt"
say done
