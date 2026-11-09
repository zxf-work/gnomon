#!/bin/bash
# readers with one live writer (the script's own two conditions), trials and duration on the line. Store: the C4-built W1 K10M Gnomon store
# (confirm/w1/stores/K10M, sha256 sidecar checked), node-local; every trial reads a private copy (the script's _pristine), so the writer
# never touches the restored store. Read mix: the script's own (hist.single, snap.hop2, series.count, coactive.narrow) on the frozen K10M log's
# dataset facts. Partition and cores on the sbatch line. Descriptive.
#SBATCH -N 1
#SBATCH --signal=B:USR1@120
#SBATCH -J cz-conc
source ${NEXT_STAGE_ROOT:?}/src/benchmarks/stages/studies/lib.sh
guard; hostinfo
E=${GNOMON_BENCH_ROOT}
O=$NEXT_STAGE_ROOT/results/${CONC_SMOKE:+smoke-}conc-$SLURM_JOB_ID; mkdir -p $O
L=${LOCAL:?}/conc; mkdir -p $L/stores $L/logs $L/tmp; export TMPDIR=$L/tmp
$PYV $REPO/benchmarks/stages/studies/t10/tools/ing_rss.py $O/rss.jsonl 5 & RSS=$!
trap 'kill $RSS 2>/dev/null' EXIT
n=synth-10000000
cp -a $E/v3/eventlogs/$n.eventlog.jsonl $E/v3/eventlogs/$n.eventlog.extcmp-params.json $L/logs/
w=$(awk -v f="$n.eventlog.jsonl" '$2 == f {print $1}' $E/v3/eventlogs/SHA256SUMS); g=$(sha256sum $L/logs/$n.eventlog.jsonl | cut -d' ' -f1)
[ -n "$w" ] && [ "$w" = "$g" ] && echo "log $n sha256 $g OK" || { echo "FROZEN LOG CHECK FAILED"; exit 2; }
A=$E/confirm/w1/stores/K10M/native-K10M.tar.zst
restore_tar $A $L/stores; need_dir $L/stores/native-K10M
$PYV -c "import json,sys; r=json.load(open(sys.argv[1])); m=r.get('manifest',{}); print('provenance', sys.argv[2], 'sha256', open(sys.argv[2]+'.sha256').read().split()[0][:16], 'load commit', m.get('commit'), 'host', m.get('hostname'), 'finished', r.get('host_after',{}).get('utc'))" $(dirname $A)/load-native.json $A
ST=$L/stores/native-K10M
echo "store path $ST: $(ls $ST | head -5 | tr '\n' ' ')"
say "mixed readers [${CONC_READERS:?}] trials ${CONC_TRIALS:?} duration ${CONC_DURATION:?} lead ${CONC_LEAD:-25}"
cd $REPO && timeout -k 60 $(( ${CONC_LIMIT_S:-3000} )) $PYV scripts/eval_concurrency.py mixed --log $L/logs/$n.eventlog.jsonl --store $ST --scale 10000000 \
  --readers $CONC_READERS --trials $CONC_TRIALS --duration $CONC_DURATION --lead ${CONC_LEAD:-25} --batch-rows ${CONC_BATCH:-100} --json $O/conc-mixed.json 2>&1 | tee $O/conc-mixed.txt
rc=${PIPESTATUS[0]}
say "conc done rc=$rc"
exit $rc
