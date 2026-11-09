# Coverage row 4: Neo4j and Memgraph on a deterministic 10 % sample of W2 K10M-d0 and K10M-d10 (tools/y_params.py), with
# Gnomon and DuckDB (SQL) 64 in the same job. Shared setup (sourced, after the stage's lib.sh). Descriptive: no forecasts, no verdict words.
#   The t10 stage's protocol with these systems: a priming call per window (unmeasured, killed at 60 s),
#   2 warm-ups + 10 timed (1 + 3 above 1 s), the 10 s ceiling on the client call, censor once, server-side limit 10 s (DuckDB (SQL)) and server timeout
#   28 s under the 30 s client alarm (Neo4j, Memgraph), no result cache in the engine. DuckDB (SQL)'s memory_limit stays the kit default (32GB).
#   Stores: Gnomon confirm/w2/stores (built on C4); DuckDB (SQL), Neo4j, Memgraph final/w2/stores (the October loads, sidecar sha256 checked).
#   R-10M as the October W2 records and t10: K10M families P..M from each system's K1M records, family B from K1M-dX (K10M-dX <- K1M-dX).
source ${NEXT_STAGE_ROOT:?}/src/benchmarks/stages/studies/lib.sh
[ -z "${EXTCMP_DUCKDBSQL_MEMORY_LIMIT:-}" ] || { echo "EXTCMP_DUCKDBSQL_MEMORY_LIMIT is set: the earlier records ran on the default: refusing"; exit 3; }
XD=$REPO/$STG/covy; XT=$XD/tools; TT=$REPO/$STG/t10/tools
export SIF_DIR=${X_SIF_DIR:-$E/studies/covy-sif}
[ "$(sha256sum $SIF_DIR/neo4j.sif | cut -d' ' -f1)" = "$(awk '$1 == "neo4j" {print $3}' $E/sif/images.txt)" ] || { echo "neo4j.sif in $SIF_DIR is not the image of sif/images.txt: refusing"; exit 3; }
SYSTEMS="native duckdbsql neo4j memgraph"
SM=${X_SMOKE:+smoke-}
ROTS=${X_ROTS:-2}
VWIN=${X_VERIFY_WINDOW_S:-900}
LOGSRC=$E/w2-powerlaw/check1m/campaign/eventlogs; SUMS=$E/w2-powerlaw/eventlogs/SHA256SUMS; PARDIR=${X_PARDIR:-$NEXT_STAGE_ROOT/params-w2s}; SIZE=10000000
POINTS=${X_POINTS:-K10M-d10 K10M-d0}; FAMS=${X_FAMS:-P T S W D N R M B}
NATIVE_FROM=$E/confirm/w2/stores; OTHERS_FROM=$E/final/w2/stores
R10M_NATIVE=$E/confirm/w2/results/timed-J45/p1; R10M_REG=$E/final/w2/results/timed-J23/p1
archive_of() { case $1 in native) echo $NATIVE_FROM/$2/native-$2.tar.zst ;; *) echo $OTHERS_FROM/$2/$1-$2.tar.zst ;; esac; }
loadrec_of() { case $1 in native) echo $NATIVE_FROM/$2/load-native.json ;; *) echo $OTHERS_FROM/$2/load-$1.json ;; esac; }
parent_of() { case $1 in native|duckdbsql) echo $STORES ;; *) echo $SRV ;; esac; }
sums_name() { case $1 in K10M-d0) echo powerlaw-10000000 ;; K10M-d10) echo W2-10M-d10 ;; esac; }
outdir() {   # dir: the job's record directory, laid out on one healthy storage target (X_OST, default 24)
  mkdir -p $1 && lfs setstripe -c 1 -i ${X_OST:-24} $1 2>/dev/null; true; }
x_root() {
  ROOT=$1; export EXTCMP_ROOT=$ROOT TMPDIR_OVERRIDE=$ROOT/scratch
  mkdir -p $ROOT/{eventlogs,stores,srv,scratch}; STORES=$ROOT/stores; SRV=$ROOT/srv; LOGS=$ROOT/eventlogs
}
useroot() {
  CUR=$1; mkdir -p $ROOT/results-$CUR $OUT/$CUR; ln -sfn results-$CUR $ROOT/results
  # X_SYNC_END=1: records stay on ${LOCAL:-${TMPDIR:-/tmp}} during the job and are copied once, at the end of each point and at the time-limit signal (syncall)
  if [ "${X_SYNC_END:-0}" = 1 ]; then export EXTCMP_SYNC_HOOK=":"; else export EXTCMP_SYNC_HOOK="mkdir -p $OUT/$CUR && cp -au $ROOT/results-$CUR/. $OUT/$CUR/"; fi
  RES=$ROOT/results-$CUR; echo "--- result root $CUR -> $OUT/$CUR"
}
syncall() { local d; for d in $ROOT/results-*; do [ -d "$d" ] && mkdir -p $OUT/${d##*/results-} && cp -au $d/. $OUT/${d##*/results-}/; done; }
getlogs() {
  local t want got
  for t in $POINTS; do
    cp -a $LOGSRC/$t.eventlog.* $LOGS/; need_file $LOGS/$t.eventlog.jsonl
    want=$(awk -v f="$(sums_name $t).eventlog.jsonl" '$2 == f {print $1}' $SUMS); got=$(sha256sum $LOGS/$t.eventlog.jsonl | cut -d' ' -f1)
    [ -n "$want" ] && [ "$want" = "$got" ] && echo "log $t = $(sums_name $t) sha256 $got OK" || { echo "FROZEN LOG CHECK FAILED $t ($got vs $want): stopping"; exit 2; }
  done
  need_file $PARDIR/SHA256SUMS; ( cd $PARDIR && sha256sum -c --quiet SHA256SUMS ) || { echo "PARAMETER FILES DO NOT MATCH SHA256SUMS: stopping"; exit 2; }
}
restore_all() {
  local tag=$1 s
  mkdir -p $RES/$tag
  for s in $SYSTEMS; do restore_tar $(archive_of $s $tag) $(parent_of $s); cp -a $(loadrec_of $s $tag) $RES/$tag/load-$s.json; done
}
win() { ${EXTCMP_WINDOW_TIMEOUT_S:+timeout -k 60 $EXTCMP_WINDOW_TIMEOUT_S} "$KIT/lib/window.sh" "$@" || echo "WINDOW FAILED $* $(utc)"; }
et_of() { case $1 in neo4j|memgraph) echo 1 ;; *) echo "" ;; esac; }     # the October W2 records: Bolt engine time in the timed rotations
rss_start() { $PYV $TT/ing_rss.py $OUT/rss-$SLURM_JOB_ID.jsonl 2 & RSSPID=$!; }
rss_stop() { [ -n "${RSSPID:-}" ] && kill -TERM $RSSPID 2>/dev/null; wait $RSSPID 2>/dev/null; RSSPID=""; }
