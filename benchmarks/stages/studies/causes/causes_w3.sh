#!/bin/bash
# C4 causes, W3: at SF10 Gnomon and DuckDB (SQL) on the G subset, IC13, BIH-R3; at SF1 Gnomon and Memgraph on IC13. Instances 0 and 3.
source ${NEXT_STAGE_ROOT:?}/src/benchmarks/stages/studies/lib.sh
: "${OUT:?}" "${ROOT:?}"
ST=$REPO/$STG/causes; G10=${C_W3_CELLS:-IC3@current,IC5@current,IC9@current,IC12@current,BI3@current,BI4@current,BI9@current,BI11@current,BI18@current,BIH-R5@current,IC13@current+asof,BIH-R3@current+asof}
export W3_STAGE_ROOT=$NEXT_STAGE_ROOT
source $REPO/benchmarks/stages/w3/jobs/w3_lib.sh
export W3_DUCKDB_THREADS=${C_DUCK_THREADS:-32} EXTCMP_PG_STATS=1
setup_root $ROOT
RES=$ROOT/res; PR=$RES/probe; mkdir -p $PR
for pt in ${C_POINTS:-SF10 SF1}; do
  pt_config $pt || exit 2; ST_OUT=$PR/$pt; mkdir -p $ST_OUT
  if [ $pt = SF10 ]; then CELLS=$G10; SYSS=${C_SF10_SYS:-gnomon duckdb}; else CELLS=${C_W3_SF1_CELLS:-IC13@current+asof}; SYSS=${C_SF1_SYS:-gnomon memgraph}; fi
  P=(--params $PARAMS --cells "$CELLS" --instances ${C_INSTANCES:-0,3} --hist-db $HIST --out $ST_OUT)
  for s in $SYSS; do
    say "probe $pt $s"; TARGET=""
    restore_sys $s || { echo "NO STORE $s $pt"; exit 2; }
    case $s in memgraph) srv_start memgraph || { echo "SERVER DID NOT START"; continue; } ;; esac
    if [ $s = gnomon ]; then env EXTCMP_ENGINE_TIME=1 GNOMON_MATCH_TRACE=1 $PYV $ST/causes_probe.py w3 --system $s --target "$TARGET" "${P[@]}" || echo "PROBE FAILED $s"
    else env EXTCMP_ENGINE_TIME=1 GNOMON_MEMGRAPH_URI=$TARGET $PYV $ST/causes_probe.py w3 --system $s --target "$TARGET" "${P[@]}" || echo "PROBE FAILED $s"; fi
    case $s in memgraph) srv_stop memgraph ;; esac
    cp -au $RES/. $OUT/
  done
done
say "W3 causes done"
