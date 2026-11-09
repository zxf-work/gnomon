#!/bin/bash
source ${W3_STAGE_ROOT:-${GNOMON_BENCH_ROOT}/final/w3}/src/benchmarks/stages/w3/jobs/w3_lib.sh
unset EXTCMP_NUMA_CLIENT EXTCMP_NUMA_SERVER
: "${OUT:?}"; N=${W3_EXT_STREAMS:-2}
pt_config SF10 || exit 2
setup_root ${LOCAL:-${TMPDIR:-/tmp}}/w3x
D=$OUT/SF10/gnomon; PLAN=$OUT/plan
( cd $PARD && sha256sum -c SHA256SUMS --ignore-missing ) || exit 2
$PYV $TOOLS/w3_gate_ext_plan.py $W3/gate/sf10-gnomon-unreached.json $PARAMS $PLAN $N || exit 2
cp $PLAN/params.jsonl $PLAN/settings.txt $D/
HISTL=${LOCAL:-${TMPDIR:-/tmp}}/w3hist.duckdb; cp $HIST $HISTL || exit 2
[ -e $GN_TAR.sha256 ] && ( cd $(dirname $GN_TAR) && sha256sum -c --quiet $(basename $GN_TAR).sha256 ) || { echo "GNOMON TAR CHECK FAILED"; exit 2; }
for k in $(seq 1 $N); do unpack $GN_TAR ${LOCAL:-${TMPDIR:-/tmp}}/w3x/st$k || exit 2; done
run_stream() {   # k
  local k=$1 name st to
  while IFS='|' read -r name st to; do
    say "stream $k task $name ($st, window $to s)"
    timeout -k 30 $to $PYV $TOOLS/w3_run.py run --system gnomon --target ${LOCAL:-${TMPDIR:-/tmp}}/w3x/st$k/$GN_DIR --params $PLAN/params-$name.jsonl \
        --settings $st --hist-db $HISTL --out $D/ext-$name.jsonl >> $D/run-$k.log 2>&1
    rc=$?; [ $rc -ne 0 ] && echo "window $name cut or failed (rc $rc) $(utc)" >> $D/windows.txt
  done < $PLAN/stream-$k.txt
  say "stream $k done"
}
for k in $(seq 1 $N); do run_stream $k > $OUT/stream-$k.log 2>&1 & done
wait
cat $OUT/stream-*.log | tail -n 20
[ -e $D/windows.txt ] && { echo "SOME WINDOWS CUT:"; cat $D/windows.txt; }
exit 0
