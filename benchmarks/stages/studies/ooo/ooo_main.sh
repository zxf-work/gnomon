#!/bin/bash
# Out-of-order logs, job body. Stops: 2 a missing input or incomplete load, 7 an answer difference (gate or pair), 8 failed windows at the end.
source ${NEXT_STAGE_ROOT:?}/src/benchmarks/stages/studies/lib.sh
: "${OUT:?}" "${ROOT:?}"
SIZE=${OOO_SIZE:-10000000}; ROTS=${OOO_ROTS:-2}; FAMS=${OOO_FAMS:-P,T,S,W,D,N,M,B}; ARMS=${OOO_ARMS:-aa,range_visibility_off}
LOGSRC=$E/v3/eventlogs; SUMS=$LOGSRC/SHA256SUMS; ST=$REPO/$STG/ooo
case $SIZE in
  10000000) S=K10M; SRC=K10M-d0.eventlog.jsonl; FZ=(--frozen-reg $E/final/w1/params-single/params-K10M.json --frozen-bel $E/final/w1/params-single/params-K10M-d0.json) ;;
  1000000)  S=K1M; SRC=synth-1000000.eventlog.jsonl; FZ=(--frozen-reg $E/final/w1/params-single/params-K1M.json) ;;
  *) echo "size $SIZE: no frozen log"; exit 2 ;;
esac
LAG=$(( SIZE / 10 ))
SEED_LAG=${OOO_SEED_LAG:-20261107}; SEED_FULL=${OOO_SEED_FULL:-20261108}
T0=$S-d0; TL=${S}lag-d0; TF=${S}full-d0; TAGS="$T0 $TL $TF"
setup_root $ROOT; sync_hook $OUT
export EXTCMP_DUCKDBSQL_THREADS=${OOO_DUCK_THREADS:-32}
source $KIT/lib/env.sh
need_file $LOGSRC/$SRC
cp -a $LOGSRC/$SRC $LOGS/$T0.eventlog.jsonl
want=$(awk -v f="$SRC" '$2 == f {print $1}' $SUMS); got=$(sha256sum $LOGS/$T0.eventlog.jsonl | cut -d' ' -f1)
[ -n "$want" ] && [ "$want" = "$got" ] && echo "log $T0 sha256 $got OK" || { echo "FROZEN LOG CHECK FAILED $T0 ($got vs $want): stopping"; exit 2; }
M=$RES/shuffle; mkdir -p $M
say "re-ordering: lag $LAG seed $SEED_LAG; full seed $SEED_FULL"
if [ "$SIZE" = 1000000 ]; then    # tests of the new tool, on the small log only
  $PYV $ST/ooo_shuffle.py write --in $LOGS/$T0.eventlog.jsonl --out $ROOT/scratch/id.jsonl --mode identity --seed 0 --meta $M/identity.json > /dev/null || exit 2
  $PYV $ST/ooo_shuffle.py check --orig $LOGS/$T0.eventlog.jsonl --new $ROOT/scratch/id.jsonl --meta $M/identity.json || { echo "SHUFFLER TEST FAILED identity"; exit 2; }
  $PYV $ST/ooo_shuffle.py write --in $LOGS/$T0.eventlog.jsonl --out $ROOT/scratch/lag2.jsonl --mode lag --lag $LAG --seed $SEED_LAG --meta $M/lag2.json > /dev/null || exit 2
  rm -f $ROOT/scratch/id.jsonl
fi
$PYV $ST/ooo_shuffle.py write --in $LOGS/$T0.eventlog.jsonl --out $LOGS/$TL.eventlog.jsonl --mode lag --lag $LAG --seed $SEED_LAG --meta $M/$TL.json || { echo "SHUFFLE FAILED"; exit 2; }
$PYV $ST/ooo_shuffle.py write --in $LOGS/$T0.eventlog.jsonl --out $LOGS/$TF.eventlog.jsonl --mode full --seed $SEED_FULL --meta $M/$TF.json || { echo "SHUFFLE FAILED"; exit 2; }
if [ "$SIZE" = 1000000 ]; then
  cmp $ROOT/scratch/lag2.jsonl $LOGS/$TL.eventlog.jsonl && echo "deterministic: two runs of one seed are byte-identical" || { echo "SHUFFLER TEST FAILED: not deterministic"; exit 2; }
  rm -f $ROOT/scratch/lag2.jsonl
fi
for t in $TL $TF; do $PYV $ST/ooo_shuffle.py check --orig $LOGS/$T0.eventlog.jsonl --new $LOGS/$t.eventlog.jsonl --meta $M/$t.json || { echo "SHUFFLE CHECK FAILED $t: stopping"; exit 2; }; done
( cd $LOGS && sha256sum $T0.eventlog.jsonl $TL.eventlog.jsonl $TF.eventlog.jsonl ) | tee $M/SHA256SUMS
if [ -z "${OOO_SMOKE:-}" ]; then
  P=$NEXT_STAGE_ROOT/logs-reordered/$SLURM_JOB_ID; mkdir -p $P
  for t in $TL $TF; do zstd -q -3 -T8 -f $LOGS/$t.eventlog.jsonl -o $P/$t.eventlog.jsonl.zst || { echo "LOG SAVE FAILED $t"; exit 2; }; done
  cp -a $M/. $P/
fi
PD=$ROOT/params
$PYV $ST/ooo_params.py --base $LOGS/$T0.eventlog.jsonl --out $PD "${FZ[@]}" $T0=$LOGS/$T0.eventlog.jsonl $TL=$LOGS/$TL.eventlog.jsonl $TF=$LOGS/$TF.eventlog.jsonl || { echo "PARAMETER FILES FAILED: stopping"; exit 2; }
mkdir -p $RES/params; cp -a $PD/. $RES/params/; eval "$EXTCMP_SYNC_HOOK"
load_ok() { $PYV -c "import json,sys; d=json.load(open(sys.argv[1])); sys.exit(0 if 'load' in d and not d.get('load exceeded budget') else 1)" "$1" 2>/dev/null; }
win() { "$KIT/lib/window.sh" "$@" || echo "WINDOW FAILED $* $(utc)"; }
say "loads"
export EXTCMP_SERVER_TIMEOUT_S=3660 EXTCMP_CALL_CEILING_S=3600 EXTCMP_VERIFY_CAP_S=3662 EXTCMP_LOAD_BUDGET_S=${OOO_LOAD_BUDGET_S:-5400}
for t in $TAGS; do
  for s in native duckdbsql; do
    win load $SIZE $t - - $s
    load_ok $RES/$t/load-$s.json || { echo "LOAD NOT COMPLETE $t $s: stopping"; eval "$EXTCMP_SYNC_HOOK"; exit 2; }
  done
  need_dir $STORES/native-$t; ln -sfn native-$t $STORES/native-$t-u
  $PYV - "$STORES/native-$t" "$RES/$t/layout.json" <<'PY'
import json, sys
import gnomon
st = gnomon.open(sys.argv[1], backend="native", read_only=True)
lay = st.adapter.segment_layout()
json.dump(lay, open(sys.argv[2], "w"), default=str)
print(sys.argv[1], "segments", len(lay))
PY
done
proto_p1
say "gate: Gnomon and DuckDB (SQL), every instance, every log, before any timing"
for t in $TAGS; do
  for s in native duckdbsql; do
    ( export EXTCMP_PARAMS=$PD/params-$t.json P1_VTAG=reg; "$KIT/lib/window.sh" verify $SIZE $t - - $s ) || { echo "GATE RUN FAILED $t $s reg: stopping"; exit 8; }
    ( export EXTCMP_PARAMS=$PD/params-$t.json P1_VTAG=bel EXTCMP_VERIFY_FAMILY=B; "$KIT/lib/window.sh" verify $SIZE $t - - $s ) || { echo "GATE RUN FAILED $t $s bel: stopping"; exit 8; }
  done
  $PYV $REPO/scripts/extcmp_p1.py gate --dir $RES/$t --ceiling 10 --out $RES/$t/excluded.json | tee $RES/$t/verdicts.txt
done
REC=(); if [ "$SIZE" = 10000000 ]; then REC=(--recorded $(ls $E/final/w1/results/gate-*/sp/K10M/verify-native.json $E/final/w1/results/gate-*/sp/K10M-d0/verify-native.json 2>/dev/null)); fi
$PYV $ST/ooo_gate.py --res $RES --tags ${TAGS// /,} --base-tag $T0 --out $RES/gate.md "${REC[@]}"; rc=$?
eval "$EXTCMP_SYNC_HOOK"
echo "gate rc=$rc" | tee $RES/gate.rc
[ $rc = 0 ] || { echo "GATE FAILED: stopping before any timing $(utc)"; exit 7; }
say "timed: paired windows, block checks on and off, logs interleaved by rotation"
export ABL_LABEL=ooo ABL_PARAMS_DIR=$PD ABL_CONFIG_GATE="n/a (descriptive)" ABL_CONFIG_GATE_1=n/a ABL_CONFIG_GATE_2=n/a ABL_CONFIG_GATE_C4=n/a
A=$RES/ooo; mkdir -p $A
for r in $(seq 1 $ROTS); do
  order=$TAGS; [ $(( r % 2 )) = 0 ] && order="$TF $TL $T0"
  for t in $order; do
    mkdir -p $A/$t; f=$A/$t/schedule-rot$r.txt
    python3 $KIT/lib/abl_schedule.py $r $FAMS $ARMS > $f
    while read -r name fam variant envspec; do
      "$KIT/lib/abl_window.sh" $SIZE $t $r $name $fam $variant $envspec; wrc=$?
      [ $wrc = 2 ] && { echo "STOPPED $t rot$r $fam $name: missing input $(utc)"; exit 2; }
      [ $wrc = 0 ] || { echo "WINDOW FAILED $t rot$r $fam $name rc=$wrc $(utc)"; echo "$t rot$r $fam $name rc=$wrc" >> $A/window-failed.txt; }
      python3 $KIT/lib/abl_pair_check.py $A/$t/rot$r/$fam $name; prc=$?
      [ $prc = 0 ] || { echo "ANSWER MISMATCH $t rot$r $fam $name: stopping $(utc)"; eval "$EXTCMP_SYNC_HOOK"; exit 7; }
    done < $f
  done
done
"$KIT/lib/servers.sh" stopall
$PYV $ST/ooo_report.py --res $RES --tags ${TAGS// /,} --out $RES/report.md; eval "$EXTCMP_SYNC_HOOK"
[ -e $A/window-failed.txt ] && { echo "FAILED WINDOWS: see window-failed.txt"; exit 8; }
say "out-of-order body done"
