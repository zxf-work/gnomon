#!/bin/bash
source ${NEXT_STAGE_ROOT:?}/src/benchmarks/stages/studies/lib.sh
: "${OUT:?}" "${FS_SECONDS:?}"
TL=$REPO/$STG/fullsets/tools; MD=$NEXT_STAGE_ROOT/manifest
RESL=${LOCAL:-${TMPDIR:-/tmp}}/fsres; mkdir -p $RESL; : > $RESL/differences.txt
need_file $MD/SHA256SUMS; ( cd $MD && sha256sum -c --quiet SHA256SUMS ) || { echo "MANIFESTS DO NOT MATCH SHA256SUMS: stopping"; exit 2; }
$PYV $REPO/$STG/tools/cov_rss.py $RESL/rss-samples.jsonl 2 &
RSSP=$!; trap 'kill -TERM $RSSP 2>/dev/null' EXIT
export EXTCMP_DUCKDBSQL_THREADS=${FS_THREADS:-64} EXTCMP_SERVER_LIMIT_S=300 EXTCMP_SERVER_TIMEOUT_S=3660 EXTCMP_CALL_CEILING_S=3600
POINTS=${FS_POINTS:-w1-K10M w1-K10M-d0 w1-K10M-d10 w5-W5-wikidata-v2 w2-K10M-d0 w2-K10M-d10 w2-K10M}
weight() { case $1 in w1-K10M) echo 15 ;; w1-*) echo 5 ;; w5-*) echo 10 ;; w2-K10M) echo 65 ;; w2-K10M-d0) echo 5 ;; *) echo 10 ;; esac; }
T_END=$(( $(date +%s) + FS_SECONDS ))
left=0; for p in $POINTS; do left=$(( left + $(weight $p) )); done
for p in $POINTS; do
  wl=${p%%-*}; tag=${p#*-}; setup_root ${LOCAL:-${TMPDIR:-/tmp}}/fs-$p; export TMPDIR=$ROOT/scratch; source $KIT/lib/env.sh
  say "restore $p"
  case $wl in
    w5) WS=$E/studies/sizes/stores/$tag; a=$WS/native-$tag.tar.zst; d=$WS/duckdbsql-$tag.tar.zst; g=$WS/postgres-$tag.tar.zst ;;
    *) a=$E/confirm/$wl/stores/$tag/native-$tag.tar.zst; d=$E/final/$wl/stores/$tag/duckdbsql-$tag.tar.zst; g=$E/final/$wl/stores/$tag/postgres-$tag.tar.zst ;;
  esac
  restore_tar $a $STORES; restore_tar $d $STORES; restore_tar $g $SRV
  $KIT/lib/servers.sh start postgres $tag | tail -1
  $PY wait --system postgres --timeout 600 || { echo "POSTGRES WOULD NOT START $p: stopping"; exit 8; }
  w=$(weight $p); now=$(date +%s); sec=$(( (T_END - now) * w / left )); left=$(( left - w ))
  say "check $p, $sec s"
  $PYV $TL/fs_check.py --manifest $MD/manifest-$p.json --tag $tag --root $EXTCMP_ROOT --native-ref $STORES/native-$tag --seconds $sec --out $RESL/cells-$p.jsonl --diff $RESL/differences.txt ${FS_RESUME:+--done $FS_RESUME/cells-$p.jsonl} || echo "CHECK FAILED $p" | tee -a $RESL/failed.txt
  $KIT/lib/servers.sh stop postgres $tag | tail -1
  cp -au $RESL/. $OUT/
done
kill -TERM $RSSP 2>/dev/null
$PYV $TL/fs_table.py ${FS_RESUME:-} $RESL > $RESL/fullsets.md || echo "table failed"; cat $RESL/fullsets.md | cut -c1-200
cp -au $RESL/. $OUT/
[ -s $RESL/failed.txt ] && exit 8
say "fullsets body done"
