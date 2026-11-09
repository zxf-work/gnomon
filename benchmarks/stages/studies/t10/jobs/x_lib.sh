# Temporal queries at 10 % corrections and the 100M point:
# shared setup of this directory's jobs (sourced, after the stage's lib.sh). New descriptive runs on engine C4: no forecasts, no verdict words.
#   Gnomon as shipped; the kit's parameter-file path: a priming call per window (unmeasured, killed at 60 s), 2 warm-ups + 10 timed
#   (1 + 3 above 1 s), the 10 s ceiling on the client call, censor once, server-side limit 10 s under the 30 s client alarm, no result
#   cache in the engine. DuckDB (SQL)'s memory_limit stays the kit default (32GB, shown as 29.8 GiB in the settings of every record), as in
#   the earlier W1 and W2 records: a job with EXTCMP_DUCKDBSQL_MEMORY_LIMIT set refuses to run.
#   Result roots: reg (Gnomon, DuckDB (SQL) 64 threads, ClickHouse, PostgreSQL), t32reg (DuckDB (SQL) 32 threads; own Gnomon reference at the gate).
source ${NEXT_STAGE_ROOT:?}/src/benchmarks/stages/studies/lib.sh
: "${X_SET:?X_SET must be w1, w2 or k100m}"
[ -z "${EXTCMP_DUCKDBSQL_MEMORY_LIMIT:-}" ] || { echo "EXTCMP_DUCKDBSQL_MEMORY_LIMIT is set: the earlier records ran on the default: refusing"; exit 3; }
XD=$REPO/$STG/t10; XT=$XD/tools
SYSTEMS="native duckdbsql clickhouse postgres"
SM=${X_SMOKE:+smoke-}
ROTS=${X_ROTS:-2}
X_T32=${X_T32:-1}                   # 1: the DuckDB (SQL) 32-thread root (t32reg) at the gate and in the timed job; X_SET s100m sets 0
VWIN=${X_VERIFY_WINDOW_S:-900}      # the answer check's limit per (system, family) window; unfinished instances are "unreached": not compared, not timed
case $X_SET in
  w1)    LOGSRC=$E/v3/eventlogs; SUMS=$E/v3/eventlogs/SHA256SUMS; PARDIR=${X_PARDIR:-$NEXT_STAGE_ROOT/params-w1}; SIZE=10000000; DEFPOINTS="K10M-d10 K10M-d0"; DEFFAMS="P T S W D N R M"
         NATIVE_FROM=$E/confirm/w1/stores; OTHERS_FROM=$E/final/w1/stores ;;
  w2)    LOGSRC=$E/w2-powerlaw/check1m/campaign/eventlogs; SUMS=$E/w2-powerlaw/eventlogs/SHA256SUMS; PARDIR=${X_PARDIR:-$NEXT_STAGE_ROOT/params-w2}; SIZE=10000000; DEFPOINTS="K10M-d10 K10M-d0"; DEFFAMS="P T S W D N R M"
         NATIVE_FROM=$E/confirm/w2/stores; OTHERS_FROM=$E/final/w2/stores
         # R-10M: each system's 1M timed records at the registry point (K1M; the 1M registry was timed at d0 only): Gnomon's on C4, the others' as recorded
         R10M_NATIVE=$E/confirm/w2/results/timed-J45/p1; R10M_REG=$E/final/w2/results/timed-J23/p1; R10M_T32=$E/final/w2/results/timed-J23/t32p1 ;;
  k100m) LOGSRC=$E/v3/eventlogs; SUMS=$E/v3/eventlogs/SHA256SUMS; PARDIR=${X_PARDIR:-$NEXT_STAGE_ROOT/params}; SIZE=${X_SIZE:-100000000}; DEFPOINTS="${X_TAG:-K100M-d0}"; DEFFAMS="P T S W D N R M B"
         X_LOAD=${X_LOAD:-1}; NATIVE_FROM=${X_STORES_FROM:-$NEXT_STAGE_ROOT/${SM}stores}; OTHERS_FROM=$NATIVE_FROM ;;
  s100m)
         # Gnomon, DuckDB (SQL) 64 and PostgreSQL only (no ClickHouse, no DuckDB (SQL) 32). Logs, sums and parameter files are the stage's own
         # (X_LOGSRC, X_SUMS, X_PARDIR override them for the smoke). The log of point X_TAG is <X_TAG>.eventlog.jsonl.
         LOGSRC=${X_LOGSRC:-$NEXT_STAGE_ROOT/eventlogs}; SUMS=${X_SUMS:-$NEXT_STAGE_ROOT/eventlogs/SHA256SUMS}; SIZE=${X_SIZE:-100000000}
         DEFPOINTS="${X_TAG:?X_SET s100m needs X_TAG}"; PARDIR=${X_PARDIR:-$NEXT_STAGE_ROOT/params/$X_TAG}; DEFFAMS="P T S W D N R M B"
         SYSTEMS="native duckdbsql postgres"; X_T32=0
         X_LOAD=${X_LOAD:-1}; NATIVE_FROM=${X_STORES_FROM:-$NEXT_STAGE_ROOT/${SM}stores}; OTHERS_FROM=$NATIVE_FROM ;;
  *) echo "unknown X_SET $X_SET"; exit 2 ;;
esac
POINTS=${X_POINTS:-$DEFPOINTS}; FAMS=${X_FAMS:-$DEFFAMS}
export X_T32 X_GATE_SYSTEMS="$SYSTEMS"      # read by x_gate.py
archive_of() { case $1 in native) echo $NATIVE_FROM/$2/native-$2.tar.zst ;; *) echo $OTHERS_FROM/$2/$1-$2.tar.zst ;; esac; }
loadrec_of() { case $1 in native) echo $NATIVE_FROM/$2/load-native.json ;; *) echo $OTHERS_FROM/$2/load-$1.json ;; esac; }
parent_of() { case $1 in native|duckdbsql) echo $STORES ;; *) echo $SRV ;; esac; }
sums_name() { case $X_SET:$1 in w2:K10M-d0) echo powerlaw-10000000 ;; w2:K10M-d10) echo W2-10M-d10 ;; k100m:*) echo synth-$SIZE ;; *) echo $1 ;; esac; }
log_name() { case $X_SET in k100m) echo synth-$SIZE ;; *) echo $1 ;; esac; }
x_root() {   # dir: node-local campaign root, one results directory per result root behind the symlink $ROOT/results
  ROOT=$1; export EXTCMP_ROOT=$ROOT TMPDIR_OVERRIDE=$ROOT/scratch
  mkdir -p $ROOT/{eventlogs,stores,srv,scratch}; STORES=$ROOT/stores; SRV=$ROOT/srv; LOGS=$ROOT/eventlogs
}
useroot() {
  CUR=$1; mkdir -p $ROOT/results-$CUR $OUT/$CUR; ln -sfn results-$CUR $ROOT/results
  export EXTCMP_SYNC_HOOK="mkdir -p $OUT/$CUR && cp -au $ROOT/results-$CUR/. $OUT/$CUR/"
  RES=$ROOT/results-$CUR; echo "--- result root $CUR -> $OUT/$CUR"
}
syncall() { local d; for d in $ROOT/results-*; do [ -d "$d" ] && mkdir -p $OUT/${d##*/results-} && cp -au $d/. $OUT/${d##*/results-}/; done; }
proto_v3() { export EXTCMP_SERVER_TIMEOUT_S=3660 EXTCMP_CALL_CEILING_S=3600 EXTCMP_VERIFY_CAP_S=3662   # load-only caps
             unset EXTCMP_SERVER_LIMIT_S EXTCMP_P1_CEILING_S EXTCMP_P1_SLOW_S EXTCMP_INPROC_WORKER EXTCMP_WINDOW_TIMEOUT_S; }
getlogs() {   # node-local copies of the frozen logs of this job's points, checked against the frozen sums
  local t n want got
  for t in $POINTS; do
    n=$(log_name $t)
    # X_LOG_SOURCE: the bytes are read from another file for read speed (the frozen file's storage
    # target reads at about 0.4 MB/s); they land under the frozen name, with the frozen file's mtime so its side files stay valid, and the
    # sha256 below must equal the frozen sha256 recorded for $(sums_name $t), else exit 10 and nothing is timed
    src=${X_LOG_SOURCE:-$LOGSRC/$n.eventlog.jsonl}
    echo "getlogs: copy $src -> $LOGS/$n.eventlog.jsonl start $(utc), limit ${X_GETLOGS_LIMIT_S:-900} s"
    timeout -k 30 ${X_GETLOGS_LIMIT_S:-900} cp $src $LOGS/$n.eventlog.jsonl && { [ ! -e $LOGSRC/$n.eventlog.meta.json ] || cp -a $LOGSRC/$n.eventlog.meta.json $LOGS/; } \
      && touch -m -d @$(stat -c %Y $LOGSRC/$n.eventlog.jsonl) $LOGS/$n.eventlog.jsonl; rc=$?
    [ $rc = 0 ] || { echo "GETLOGS COPY DID NOT FINISH rc=$rc at $(utc) ($(du -b $LOGS/$n.eventlog.jsonl 2>/dev/null | cut -f1) bytes copied): stopping, exit 9"; exit 9; }
    echo "getlogs: copy done $(utc), $(du -b $LOGS/$n.eventlog.jsonl | cut -f1) bytes; checksum start"
    need_file $LOGS/$n.eventlog.jsonl
    want=$(awk -v f="$(sums_name $t).eventlog.jsonl" '$2 == f {print $1}' $SUMS); got=$(timeout -k 30 ${X_GETLOGS_LIMIT_S:-900} sha256sum $LOGS/$n.eventlog.jsonl | cut -d' ' -f1)
    [ -n "$got" ] || { echo "GETLOGS CHECKSUM DID NOT FINISH at $(utc): stopping, exit 9"; exit 9; }
    echo "getlogs: checksum done $(utc)"
    echo "getlogs: source $src sha256 $got; frozen sha256 of $(sums_name $t) $want"
    if [ -n "${X_LOG_SOURCE:-}" ]; then
      [ -n "$want" ] && [ "$want" = "$got" ] && echo "log $n = $(sums_name $t) sha256 $got OK (bytes read from $src)" || { echo "SUBSTITUTED SOURCE HASH DIFFERS FROM THE FROZEN SHA256 ($got vs $want): stopping, exit 10, nothing timed"; exit 10; }
    else
      [ -n "$want" ] && [ "$want" = "$got" ] && echo "log $n = $(sums_name $t) sha256 $got OK" || { echo "FROZEN LOG CHECK FAILED $n ($got vs $want): stopping"; exit 2; }
    fi
    # the side files this job's own parameter step wrote for the log, valid only while the log's size and mtime match
    [ -d $PARDIR/side ] && cp -a $PARDIR/side/$n.eventlog.*.json $LOGS/ 2>/dev/null
  done
  need_file $PARDIR/SHA256SUMS; ( cd $PARDIR && sha256sum -c --quiet SHA256SUMS ) || { echo "PARAMETER FILES DO NOT MATCH SHA256SUMS: stopping"; exit 2; }
}
restore_all() {   # tag: every system's store, its sidecar sha256 checked; the load records beside the results
  local tag=$1 s
  mkdir -p $RES/$tag
  for s in $SYSTEMS; do restore_tar $(archive_of $s $tag) $(parent_of $s); cp -a $(loadrec_of $s $tag) $RES/$tag/load-$s.json; done
}
win() { ${EXTCMP_WINDOW_TIMEOUT_S:+timeout -k 60 $EXTCMP_WINDOW_TIMEOUT_S} "$KIT/lib/window.sh" "$@" || echo "WINDOW FAILED $* $(utc)"; }
rss_start() { $PYV $XT/ing_rss.py $OUT/rss-$SLURM_JOB_ID.jsonl 2 & RSSPID=$!; }
rss_stop() { [ -n "${RSSPID:-}" ] && kill -TERM $RSSPID 2>/dev/null; wait $RSSPID 2>/dev/null; RSSPID=""; }
