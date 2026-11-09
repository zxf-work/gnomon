#!/bin/bash
source ${NEXT_STAGE_ROOT:?}/src/benchmarks/stages/studies/lib.sh
: "${OUT:?}" "${RF_CAUSE:?}"
TL=$REPO/$STG/raphtory/tools; POINTS=${RF_POINTS:-w1:K1M w1:K10M w1:K10M-d0 w1:K10M-d10 w2:K10M w2:K10M-d0}
RESL=${LOCAL:-${TMPDIR:-/tmp}}/rfres; mkdir -p $RESL
$PYV $REPO/$STG/tools/cov_rss.py $RESL/rss-samples.jsonl 2 &
RSSP=$!; trap 'kill -TERM $RSSP 2>/dev/null' EXIT
export EXTCMP_DUCKDBSQL_THREADS=32 EXTCMP_SERVER_LIMIT_S=1800 EXTCMP_SERVER_TIMEOUT_S=3660 EXTCMP_CALL_CEILING_S=3600
for p in $POINTS; do
  wl=${p%%:*}; tag=${p#*:}; c=$RF_CAUSE/cause-$wl-$tag.json; need_file $c
  setup_root ${LOCAL:-${TMPDIR:-/tmp}}/rf-$wl-$tag; export TMPDIR=$ROOT/scratch; source $KIT/lib/env.sh
  say "full sets $wl $tag"
  restore_tar $E/confirm/$wl/stores/$tag/native-$tag.tar.zst $STORES
  restore_tar $E/final/$wl/stores/$tag/duckdbsql-$tag.tar.zst $STORES
  restore_tar $E/final/$wl/stores/$tag/postgres-$tag.tar.zst $SRV
  $KIT/lib/servers.sh start postgres $tag | tail -1
  $PY wait --system postgres --timeout 600 || { echo "POSTGRES WOULD NOT START $wl $tag: stopping"; exit 8; }
  $PYV $TL/rap_fullsets.py --cause $c --wl $wl --tag $tag --native $STORES/native-$tag --root $EXTCMP_ROOT --out $RESL/fullsets-$wl-$tag.json || { echo "FULL SETS FAILED $wl $tag" | tee -a $RESL/failed.txt; }
  $KIT/lib/servers.sh stop postgres $tag | tail -1
  cp -au $RESL/. $OUT/
done
kill -TERM $RSSP 2>/dev/null
$PYV $TL/hash_cover.py > $RESL/hash-coverage.md || echo "coverage table failed"
$PYV - $RESL <<'PY' > $RESL/fullsets.md
import glob, json, sys
print("# DuckDB (SQL) and PostgreSQL whole reachable sets against Gnomon's\n\n| point | cell | stratum | Gnomon nodes | system | nodes | only in the system | only in Gnomon | arrivals differ | same row order | seconds |\n|---|---|---|---:|---|---:|---:|---:|---:|---|---:|")
for p in sorted(glob.glob(sys.argv[1] + "/fullsets-*.json")):
    pt = p.split("fullsets-")[-1][:-5]
    for c in json.load(open(p)):
        for s, r in c["systems"].items():
            if "error" in r:
                print(f"| {pt} | {c['key']} | {c['stratum']} | {c['gnomon_n']} | {s} | error: {r['error'][:80]} | | | | | {r['s']} |")
            else:
                print(f"| {pt} | {c['key']} | {c['stratum']} | {c['gnomon_n']} | {s} | {r['n']} | {r['only_system']} | {r['only_gnomon']} | {r['arrival_differs']} | {r['same_order_of_rows']} | {r['s']} |")
PY
cat $RESL/fullsets.md
cp -au $RESL/. $OUT/
[ -s $RESL/failed.txt ] && exit 8
say "full sets body done"
