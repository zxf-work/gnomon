#!/bin/bash
source ${W3_STAGE_ROOT:-${GNOMON_BENCH_ROOT}/final/w3}/src/benchmarks/stages/w3/jobs/w3_lib.sh
unset EXTCMP_NUMA_CLIENT EXTCMP_NUMA_SERVER
: "${OUT:?}"; N=${W3_EXT_STREAMS:-2}
BUDGET=${W3_EXT_BUDGET_S:-23400}      # the whole job's budget for the cells (6.5 h); no per-task window
UNR=${W3_EXT_UNREACHED:-$W3/gate/sf10b-gnomon-unreached.json}
pt_config SF10 || exit 2
setup_root ${LOCAL:-${TMPDIR:-/tmp}}/w3x
D=$OUT/SF10/gnomon; PLAN=$OUT/plan
( cd $PARD && sha256sum -c SHA256SUMS --ignore-missing ) || exit 2
echo "unreached list $UNR sha256 $(sha256sum $UNR | cut -d' ' -f1)"
$PYV $TOOLS/w3_gate_ext2_plan.py $UNR $PARAMS $PLAN $N || exit 2
cp $PLAN/params.jsonl $PLAN/settings.txt $D/
HISTL=${LOCAL:-${TMPDIR:-/tmp}}/w3hist.duckdb; cp $HIST $HISTL || exit 2
[ -e $GN_TAR.sha256 ] && ( cd $(dirname $GN_TAR) && sha256sum -c --quiet $(basename $GN_TAR).sha256 ) || { echo "GNOMON TAR CHECK FAILED"; exit 2; }
for k in $(seq 1 $N); do unpack $GN_TAR ${LOCAL:-${TMPDIR:-/tmp}}/w3x/st$k || exit 2; done
T0=$(date +%s); DEADLINE=$((T0 + BUDGET)); echo "budget $BUDGET s from $(utc)"
run_stream() {   # k
  local k=$1 name st rem
  while IFS='|' read -r name st; do
    rem=$((DEADLINE - $(date +%s)))
    if [ $rem -le 60 ]; then echo "task $name not started: budget exhausted $(utc)" >> $D/windows.txt; continue; fi
    say "stream $k task $name ($st, $rem s of the budget left)"
    timeout -k 30 $rem $PYV $TOOLS/w3_run.py run --system gnomon --target ${LOCAL:-${TMPDIR:-/tmp}}/w3x/st$k/$GN_DIR --params $PLAN/params-$name.jsonl \
        --settings $st --hist-db $HISTL --out $D/ext-$name.jsonl >> $D/run-$k.log 2>&1
    rc=$?; [ $rc -ne 0 ] && echo "task $name cut or failed (rc $rc) $(utc)" >> $D/windows.txt
  done < $PLAN/stream-$k.txt
  say "stream $k done"
}
for k in $(seq 1 $N); do run_stream $k > $OUT/stream-$k.log 2>&1 & done
wait
cat $OUT/stream-*.log | tail -n 20
[ -e $D/windows.txt ] && { echo "SOME TASKS CUT:"; cat $D/windows.txt; }
exit 0
