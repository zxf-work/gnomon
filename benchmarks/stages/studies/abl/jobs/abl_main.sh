#!/bin/bash
# Ablation job body (run by abl.sbatch in its own process group).
source ${NEXT_STAGE_ROOT:?}/src/benchmarks/stages/studies/lib.sh
: "${OUT:?}" "${ROOT:?}" "${ABL_WL:?}" "${ABL_SIZE:?}" "${ABL_PLAN:?}" "${ABL_ARMS:?}"
setup_root $ROOT; sync_hook $OUT
sz() { case $1 in 1000000) echo 1M ;; 10000000) echo 10M ;; *) echo "$1" ;; esac; }
ROTS=${ABL_ROTS:-2}; STEPS=${ABL_STEPS:-stores,timed,report}
[ -z "${ABL_SMOKE:-}" ] && [ -z "${ABL_GATE_TEST:-}" ] && [ "$ABL_SIZE" != 10000000 ] && { echo "a measured ablation is at 10M: refusing $ABL_SIZE"; exit 3; }
if [ "${ABL_GATE_TEST:-0}" = 1 ] && [ "$ABL_WL" = w1 ]; then    # shows the gate refusing: only arms the gate covers may reach run_ablation here (W2's label is gated for every arm)
  for a in ${ABL_ARMS//,/ }; do $PYV $KIT/lib/abl_schedule.py --gated "$a" || { echo "ABL_GATE_TEST with an arm no gate covers ($a): refusing"; exit 3; }; done
fi
case $ABL_WL in
  w1) LABEL=ablation; LOGSRC=$E/v3/eventlogs; SUMS=$E/v3/eventlogs/SHA256SUMS ;;
  w2) LABEL=ablation-w2; LOGSRC=$E/w2-powerlaw/check1m/campaign/eventlogs; SUMS=$E/w2-powerlaw/eventlogs/SHA256SUMS
      proto_p1
      PD=${ABL_PARAMS_DIR:?W2 needs ABL_PARAMS_DIR}; need_file $PD/SHA256SUMS
      ( cd $PD && sha256sum -c --quiet SHA256SUMS ) || { echo "PARAMETER FILES DO NOT MATCH SHA256SUMS: stopping"; exit 2; }
      mkdir -p $OUT/params; cp -a $PD/. $OUT/params/; export ABL_PARAMS_DIR=$PD ;;
  *) echo "unknown ABL_WL $ABL_WL"; exit 2 ;;
esac
export ABL_PAIR_CHECK=1
sums_name() {
  local v=$(sz $ABL_SIZE)
  case $1 in *-d0) echo powerlaw-$ABL_SIZE ;; *-d1) echo W2-$v-d1 ;; *-d10) echo W2-$v-d10 ;; esac; }
getlog() {   # tag: node-local copy of the frozen log and its side files, checked
  local t=$1 want got
  case $ABL_WL in
    w1) if [ "$ABL_SIZE" = 1000000 ]; then      # no K1M density log is frozen for W1: a smoke's logs step makes K1M-d<d> from the base log
          cp -a $LOGSRC/synth-1000000.eventlog.* $LOGS/; need_file $LOGS/synth-1000000.eventlog.jsonl; return 0; fi
        cp -a $LOGSRC/$t.eventlog.* $LOGS/; need_file $LOGS/$t.eventlog.jsonl
        cp -a $LOGSRC/synth-$ABL_SIZE.eventlog.* $LOGS/; need_file $LOGS/synth-$ABL_SIZE.eventlog.jsonl   # run_ablation's replay ingest reads the base log
        want=$(awk -v f="$t.eventlog.jsonl" '$2 == f {print $1}' $SUMS) ;;
    w2) cp -a $LOGSRC/$t.eventlog.* $LOGS/; need_file $LOGS/$t.eventlog.jsonl
        cp -a $LOGSRC/synth-$ABL_SIZE.eventlog.* $LOGS/ 2>/dev/null
        want=$(awk -v f="$(sums_name $t).eventlog.jsonl" '$2 == f {print $1}' $SUMS) ;;
  esac
  got=$(sha256sum $LOGS/$t.eventlog.jsonl | cut -d' ' -f1)
  [ -n "$want" ] && [ "$want" = "$got" ] && echo "log $t sha256 $got OK" || { echo "FROZEN LOG CHECK FAILED $t ($got vs $want): stopping"; exit 2; }
}
for pl in $ABL_PLAN; do getlog K$(sz $ABL_SIZE)-d${pl%%:*}; done
# DuckDB storage: restored from the recorded W1 stage, not rebuilt
case ",$ABL_ARMS," in *,duckdb_storage,*)
  : "${ABL_DUCKDB_SRC:?duckdb_storage needs ABL_DUCKDB_SRC}"
  for pl in $ABL_PLAN; do t=K$(sz $ABL_SIZE)-d${pl%%:*}
    restore_tar $ABL_DUCKDB_SRC/$t/duckdb-$t.tar.zst $STORES
    need_dir $STORES/duckdb-$t
    mkdir -p $RES/smoke-$LABEL/$t/stores $RES/$LABEL/$t/stores
    need_file $ABL_DUCKDB_SRC/$t/load-duckdb.json
    cp $ABL_DUCKDB_SRC/$t/load-duckdb.json $RES/$LABEL/$t/stores/duckdb.json; cp $ABL_DUCKDB_SRC/$t/load-duckdb.json $RES/smoke-$LABEL/$t/stores/duckdb.json
  done ;; esac
[ "${ABL_SMOKE:-0}" = 1 ] && [ "${ABL_SMOKE_EMPTY_STORE:-0}" = 1 ] && { t=K$(sz $ABL_SIZE)-d${ABL_PLAN%%:*}; mkdir -p $STORES/native-$t-u; echo "smoke: an empty store at $STORES/native-$t-u"; }
smk=(); [ "${ABL_SMOKE:-0}" = 1 ] && smk=(--smoke)
isz=${ABL_INGEST_SIZE:-$ABL_SIZE}
for pl in $ABL_PLAN; do
  d=${pl%%:*}; f=${pl#*:}
  # the ingest step belongs to one pass only
  st=$STEPS; [ "$pl" = "${ABL_PLAN%% *}" ] || st=$(echo "$STEPS" | sed 's/,ingest//; s/,report//')
  [ "$pl" = "${ABL_PLAN##* }" ] && case ",$st," in *,report,*) ;; *) case ",$STEPS," in *,report,*) st=$st,report ;; esac ;; esac
  say "run_ablation $LABEL d$d families $f steps $st"
  $KIT/run_ablation.sh --commit $EXTCMP_ENGINE_COMMIT --sizes $ABL_SIZE --densities $d --families $f --rotations $ROTS \
      --arms "$ABL_ARMS" --label $LABEL --ingest-sizes $isz --ingest-reps ${ABL_INGEST_REPS:-3} --steps $st "${smk[@]}"
  rc=$?; [ $rc = 0 ] || { echo "run_ablation stopped rc=$rc"; exit $rc; }
done
A=$RES/$LABEL; [ "${ABL_SMOKE:-0}" = 1 ] && A=$RES/smoke-$LABEL
if [ "${ABL_ABL4:-0}" = 1 ]; then
  bash $REPO/$STG/abl4/abl4_run.sh $A/abl4 || { rc=$?; echo "abl4 windows stopped rc=$rc"; exit $rc; }
fi
if [ "$ABL_WL" = w2 ]; then
  $PYV $TOOLS/abl_hub_report.py $A --out $A/hub-report || { echo "hub report failed"; exit 8; }
fi
$PYV $TOOLS/abl_postcheck.py $A || { echo "POSTCHECK FAILED $A"; exit 8; }
say "ablation body done"
