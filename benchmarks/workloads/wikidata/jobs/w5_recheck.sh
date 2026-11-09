#!/bin/bash
# W5 re-check on a new engine, untimed: replay into Gnomon, store digest, every Gnomon cell on the
# curated files, compared with the baseline answers recorded on the same frozen log and files.
#   W5_TAG W5_SIZE W5_PARAMS_DIR W5_BASE_RUN (records dir holding verify-<sys>-{reg,bel}.json)
set -uo pipefail
export EXTCMP_JOB_SAFE=1
source "$KIT/lib/env.sh"
tag=$W5_TAG; size=${W5_SIZE:-1000000}; PD=$W5_PARAMS_DIR; BASE=$W5_BASE_RUN
say() { echo "=== $* $(date -u +%FT%TZ)"; }
win() { "$KIT/lib/window.sh" "$@" || echo "WINDOW FAILED $* $(date -u +%FT%TZ)"; }
export EXTCMP_P1_CEILING_S=${EXTCMP_P1_CEILING_S:-600} EXTCMP_SERVER_LIMIT_S=${EXTCMP_SERVER_LIMIT_S:-600} EXTCMP_P1_SLOW_S=1.0
PYV=$REPO/.venv/bin/python; L=$LOGS/$tag.eventlog.jsonl
say engine; $PYV -c "import gnomon._engine as e, hashlib; print('engine_so', e.__file__, hashlib.sha256(open(e.__file__,'rb').read()).hexdigest())"; git -C $REPO log -1 --format='engine_commit %H %s'
say load native; win load "$size" "$tag" - - native
say digest; $PYV -c "
import gnomon; s = gnomon.open('$STORES/native-$tag', backend='native', read_only=True)
st = s.stats(); print('stats', {k: st[k] for k in ('n_entities', 'n_node_versions', 'n_edge_versions')}); print('store_digest', s.digest_streaming()); s.close()"
$PYV - "$L" "$PD/params-$tag-bel.json" <<'PY'
import json, os, sys
log, pf = sys.argv[1], sys.argv[2]; st = os.stat(log)
ins = {i["query"]: i["args"] for i in json.load(open(pf))["instances"]}
sn, rc, df, h = ins["b.snap.hop2@T"], ins["b.reach@T"], ins["b.diff"], ins["b.hist"]
d = {"size": st.st_size, "mtime_ns": st.st_mtime_ns, "snap_seeds": sn["seeds"], "t_star": sn["t_valid"],
     "seed_node": rc["src"], "reach_window": rc["window"], "T": sn["as_of_tt"], "T_base": df["t1"],
     "diff_window": df["window"], "e_star": h, "w5_placeholder": True}
open(log[:-len(".jsonl")] + ".beliefq-params.json", "w").write(json.dumps(d))
PY
for v in reg bel; do say verify $v native; ( export EXTCMP_PARAMS=$PD/params-$tag-$v.json P1_VTAG=$v; [ $v = bel ] && export EXTCMP_VERIFY_FAMILY=B; win verify "$size" "$tag" - - native ); done
say compare; $PYV - "$RES/$tag" "$BASE" <<'PY'
import json, sys, collections
new, base = sys.argv[1], sys.argv[2]
for v in ("reg", "bel"):
    N = {(r["query"], r["instance"]): r for r in json.load(open(f"{new}/verify-native-{v}.json"))["results"]}
    O = {(r["query"], r["instance"]): r for r in json.load(open(f"{base}/verify-native-{v}.json"))["results"]}
    ch = collections.Counter((k[0], "same as 2036504b" if O.get(k, {}).get("hash") == r.get("hash") else "changed") for k, r in N.items())
    print(v, "native cells", len(N), "ok", sum(bool(r.get("ok")) for r in N.values()), "vs 2036504b:", dict(sorted(ch.items())))
    for s in ("duckdbsql", "postgres", "neo4j"):
        c = collections.Counter(); diff = []
        for r in json.load(open(f"{base}/verify-{s}-{v}.json"))["results"]:
            k = (r["query"], r["instance"]); n = N.get(k)
            if not r.get("ok"): c["baseline not ok"] += 1
            elif n is None or not n.get("ok"): c["no native answer"] += 1; diff.append(f"{k[0]}#{k[1]}:{(n or {}).get('error','')[:60]}")
            elif n["hash"] == r["hash"] or (r.get("proj_hash") and r.get("proj_hash") == n.get("proj_hash")): c["equivalent"] += 1
            else: c["DIFFERS"] += 1; diff.append(f"{k[0]}#{k[1]}")
        print(" ", v, s, dict(c), diff[:12])
PY
say done
