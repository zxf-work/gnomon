#!/bin/bash
# W4 sf10-r10, W5 wikidata-v2, in one job per partition, order on off off on (ABBA) per workload, through the kits' own drivers:
#   W2, W5: extcmp.py query, the parameter-file path (one process per family window: open_s, the window prime, 2 warm-ups + 10 timed, 1 + 3
#           above 1 s, 10 s ceiling, censor once); W4: timed_runner.py w4 (prime per (query, setting), the same protocol), current and historic.
# The C4 stores of the timed records (sha256 sidecar checked, load records printed). Peak-memory sampler (ing_rss.py, 2 s). Descriptive.
# WARM_WLS narrows the workloads, WARM_ORDER the settings (smoke). Partition and cores on the sbatch line.
#SBATCH -N 1
#SBATCH --signal=B:USR1@120
#SBATCH -J cz-warm
source ${NEXT_STAGE_ROOT:?}/src/benchmarks/stages/studies/lib.sh
guard; hostinfo
E=${GNOMON_BENCH_ROOT}
O=$NEXT_STAGE_ROOT/results/${WARM_SMOKE:+smoke-}warm-$SLURM_JOB_ID; mkdir -p $O
setup_root ${LOCAL:?}/warm; export TMPDIR=$ROOT/scratch; source $KIT/lib/env.sh; proto_p1
MK=$O/marks.txt; mk() { echo "$(date +%s.%3N) $*" >> $MK; }
$PYV $REPO/benchmarks/stages/studies/t10/tools/ing_rss.py $O/rss.jsonl 2 & RSS=$!
trap 'kill $RSS 2>/dev/null; cp -au $RES/. $O/ 2>/dev/null' EXIT
trap 'echo "=== time limit near $(utc): stopping"; cp -au $RES/. $O/; echo STOPPED > $O/STOPPED; exit 5' USR1 TERM
prov() { $PYV -c "import json,sys; r=json.load(open(sys.argv[1])); m=r.get('manifest',{}); print('provenance', sys.argv[2], 'sha256', open(sys.argv[2]+'.sha256').read().split()[0][:16], 'load commit', m.get('commit'), 'engine', m.get('engine_commit'), 'host', m.get('hostname'), 'finished', r.get('host_after',{}).get('utc'))" "$1" "$2" 2>&1; }
ORDER=${WARM_ORDER:-1 0 0 1}
p1_pass() {   # tag log store pf fams label
  local tag=$1 log=$2 st=$3 pf=$4 fams=$5 lab=$6 fam
  for fam in $fams; do
    mkdir -p $RES/$lab; mk start $lab $fam
    EXTCMP_PARAMS=$pf timeout -k 60 1800 $PY query --system native --log $log --ref $st --workdir $EXTCMP_ROOT --size-tag $tag --mode timed --family $fam --answers $RES/$lab/answers --json $RES/$lab/$fam-native.json --params $pf > $RES/$lab/$fam.out 2>&1 || echo "WINDOW FAILED $lab $fam"
    mk end $lab $fam; tail -2 $RES/$lab/$fam.out | cut -c1-200
  done
  cp -au $RES/. $O/
}
for wl in ${WARM_WLS:-w2 w4 w5}; do
  case $wl in
  w2) n=synth-10000000; tag=K10M; cp -a $E/w2-powerlaw/check1m/campaign/eventlogs/$n.eventlog.* $LOGS/; LOGF=$LOGS/$n.eventlog.jsonl
      w=$(awk '$2 == "powerlaw-10000000.eventlog.jsonl" {print $1}' $E/w2-powerlaw/eventlogs/SHA256SUMS); g=$(sha256sum $LOGF | cut -d' ' -f1); [ -n "$w" ] && [ "$w" = "$g" ] || { echo "FROZEN LOG CHECK FAILED w2"; exit 2; }
      A=$E/confirm/w2/stores/$tag/native-$tag.tar.zst; restore_tar $A $STORES; prov $(dirname $A)/load-native.json $A
      pf=$E/studies/abl/params-w2/params-K10M.json
      for x in $ORDER; do say "W2 warm=$x"; export GNOMON_STORE_WARM_OPEN=$x; p1_pass $tag $LOGF $STORES/native-$tag $pf "${WARM_FAMS:-P T S W D N R M}" w2-warm$x-$(date +%s); done
      unset GNOMON_STORE_WARM_OPEN; rm -rf $STORES/native-$tag ;;
  w5) TAG=W5-wikidata-v2; W=$E/w5-wikidata; PD=$W/params-c3
      cp -a $W/campaign/eventlogs/$TAG.eventlog.jsonl $W/eventlogs/$TAG.eventlog.jsonl.meta.json $LOGS/; LOGF=$LOGS/$TAG.eventlog.jsonl
      w=$(awk -v f="$TAG.eventlog.jsonl" '$2 == f {print $1}' $W/eventlogs/SHA256SUMS-$TAG); g=$(sha256sum $LOGF | cut -d' ' -f1); [ -n "$w" ] && [ "$w" = "$g" ] || { echo "FROZEN LOG CHECK FAILED w5"; exit 2; }
      ( cd $PD && sha256sum -c --quiet --ignore-missing SHA256SUMS-$TAG ) || { echo "W5 PARAMETER FILES DO NOT MATCH: stopping"; exit 2; }
      $PYV - "$LOGF" "$PD/params-$TAG-bel.json" <<'PY' || { echo "SIDECAR FAILED"; exit 2; }
import json, os, sys
log, pf = sys.argv[1], sys.argv[2]; st = os.stat(log)
ins = {i["query"]: i["args"] for i in json.load(open(pf))["instances"]}
sn, rc, df, h = ins["b.snap.hop2@T"], ins["b.reach@T"], ins["b.diff"], ins["b.hist"]
d = {"size": st.st_size, "mtime_ns": st.st_mtime_ns, "snap_seeds": sn["seeds"], "t_star": sn["t_valid"],
     "seed_node": rc["src"], "reach_window": rc["window"], "T": sn["as_of_tt"], "T_base": df["t1"],
     "diff_window": df["window"], "e_star": h, "w5_placeholder": True}
open(log[:-len(".jsonl")] + ".beliefq-params.json", "w").write(json.dumps(d))
PY
      A=$E/studies/sizes/stores/$TAG/native-$TAG.tar.zst; restore_tar $A $STORES; prov $(dirname $A)/load-native.json $A
      for x in $ORDER; do say "W5 warm=$x"; export GNOMON_STORE_WARM_OPEN=$x; L=w5-warm$x-$(date +%s)
        unset EXTCMP_BELIEFQ_INDEXES; p1_pass $TAG $LOGF $STORES/native-$TAG $PD/params-$TAG-reg.json "${WARM_FAMS:-P T S W D N R M}" $L
        export EXTCMP_BELIEFQ_INDEXES=1; p1_pass $TAG $LOGF $STORES/native-$TAG $PD/params-$TAG-bel.json B $L; unset EXTCMP_BELIEFQ_INDEXES; done
      unset GNOMON_STORE_WARM_OPEN; rm -rf $STORES/native-$TAG ;;
  w4) W=$E/w4-finbench; PT=sf10-r10; S4=$W/w4stores/$PT; PAR=$W/params/params-sf10.json
      ( cd $(dirname $PAR) && sha256sum -c --quiet $(basename $PAR).sha256 ) || { echo "W4 PARAMETER FILE CHECK FAILED"; exit 2; }
      g=$(ls $S4/gnomon-*.tar.zst | head -1); restore_tar $g $STORES; need_dir $STORES/gnomon-$PT/native
      echo "provenance $g sha256 $(cut -c1-16 $g.sha256) (the W4 timed stores; digest equal to a C4 rebuild, J50)"
      TR="${EXTCMP_NUMA_CLIENT:+$EXTCMP_NUMA_CLIENT }$PYV $REPO/scripts/timed_runner.py"
      export TR_CEILING_S=10 TR_GRACE_S=5 EXTCMP_SERVER_TIMEOUT_S=10
      i=0; for x in $ORDER; do i=$((i+1)); say "W4 warm=$x"; export GNOMON_STORE_WARM_OPEN=$x; L=w4-warm$x-$i; mkdir -p $RES/$L; mk start $L
        env EXTCMP_ENGINE_TIME=0 timeout -k 60 1800 $TR w4 --system gnomon --params $PAR ${WARM_W4SEL:-} --beliefs current,historic --rotation $i --store $STORES/gnomon-$PT --out $RES/$L/gnomon.json --jsonl $RES/$L/gnomon-calls.jsonl --label "covz warm W4 $PT warm=$x" 2>&1 | tail -n 3
        mk end $L; cp -au $RES/. $O/; done
      unset GNOMON_STORE_WARM_OPEN; rm -rf $STORES/gnomon-$PT ;;
  esac
done
cp -au $RES/. $O/
say "warm done"
