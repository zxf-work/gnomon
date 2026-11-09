#!/bin/bash
# Out-of-order logs, explicit-node form: original, lag and full at density 0 and 10, each with a `nodes` record for every uid in the first ingest op; the plain original as the comparison.
# Reordered: only the base edge events inside ingest_events ops, across the ingest batches. In place, byte for byte: every batch's tt and size, the base log's corrections and retractions, the node assert, and at d10 every injected correction batch.
# Stops: 2 a missing input or incomplete load, 7 Gnomon and DuckDB (SQL) differ on one log, or a pair differs; 8 failed windows. A difference between logs is reported, not a stop.
source ${NEXT_STAGE_ROOT:?}/src/benchmarks/stages/studies/lib.sh
: "${OUT:?}" "${ROOT:?}"
SIZE=${OOO_SIZE:-10000000}; ROTS=${OOO_ROTS:-2}; FAMS=${OOO_FAMS:-P,T,S,W,D,N,M,B}; ARMS=${OOO_ARMS:-aa,range_visibility_off}; DENS=${OOO_DENS:-0 10}
ST=$REPO/$STG/ooo; LAG=$(( SIZE / 10 )); SEED_LAG=20261107; SEED_FULL=20261108
setup_root $ROOT; sync_hook $OUT
export EXTCMP_DUCKDBSQL_THREADS=${OOO_DUCK_THREADS:-32}
source $KIT/lib/env.sh
M=$RES/shuffle; PD=$ROOT/params; mkdir -p $M $PD
srcof() { case $SIZE in 10000000) echo $E/v3/eventlogs/K10M-d$1.eventlog.jsonl ;; 1000000) echo $E/w2-powerlaw/check1m/campaign/eventlogs/K1M-d$1.eventlog.jsonl ;; esac; }
S=K10M; [ $SIZE = 1000000 ] && S=K1M
tg() { echo $S$1-d$2; }     # kind density: p plain original; xo, xl, xf explicit-node original, lag, full. One -d per tag: abl_window.sh cuts the tag there for the registry file.
for d in $DENS; do
  P=$(tg p$d $d); XO=$(tg x${d}o $d); XL=$(tg x${d}l $d); XF=$(tg x${d}f $d)
  L=$(srcof $d); need_file $L; cp -a $L $LOGS/$P.eventlog.jsonl
  if [ $SIZE = 10000000 ]; then want=$(awk -v f="K10M-d$d.eventlog.jsonl" '$2 == f {print $1}' $E/v3/eventlogs/SHA256SUMS); got=$(sha256sum $LOGS/$P.eventlog.jsonl | cut -d' ' -f1)
    [ -n "$want" ] && [ "$want" = "$got" ] && echo "log $P sha256 $got OK" || { echo "FROZEN LOG CHECK FAILED $P ($got vs $want): stopping"; exit 2; }; fi
  say "explicit-node logs d$d"
  $PYV $ST/ooo_shuffle.py write --in $LOGS/$P.eventlog.jsonl --out $LOGS/$XO.eventlog.jsonl --mode identity --seed 0 --explicit-nodes --meta $M/$XO.json > /dev/null || { echo "WRITE FAILED"; exit 2; }
  $PYV $ST/ooo_shuffle.py write --in $LOGS/$P.eventlog.jsonl --out $LOGS/$XL.eventlog.jsonl --mode lag --lag $LAG --seed $SEED_LAG --explicit-nodes --meta $M/$XL.json > /dev/null || { echo "WRITE FAILED"; exit 2; }
  $PYV $ST/ooo_shuffle.py write --in $LOGS/$P.eventlog.jsonl --out $LOGS/$XF.eventlog.jsonl --mode full --seed $SEED_FULL --explicit-nodes --meta $M/$XF.json > /dev/null || { echo "WRITE FAILED"; exit 2; }
  for t in $XO $XL $XF; do $PYV $ST/ooo_shuffle.py check --orig $LOGS/$P.eventlog.jsonl --new $LOGS/$t.eventlog.jsonl --meta $M/$t.json || { echo "LOG CHECK FAILED $t: stopping"; exit 2; }; done
  ( cd $LOGS && sha256sum $P.eventlog.jsonl $XO.eventlog.jsonl $XL.eventlog.jsonl $XF.eventlog.jsonl ) | tee -a $M/SHA256SUMS
  FZ=(); if [ $SIZE = 10000000 ]; then FZ=(--frozen-bel $E/final/w1/params-single/params-K10M-d$d.json); fi
  $PYV $ST/ooo_params.py --base $LOGS/$P.eventlog.jsonl --out $PD "${FZ[@]}" $P=$LOGS/$P.eventlog.jsonl $XO=$LOGS/$XO.eventlog.jsonl $XL=$LOGS/$XL.eventlog.jsonl $XF=$LOGS/$XF.eventlog.jsonl || { echo "PARAMETER FILES FAILED: stopping"; exit 2; }
done
if [ -z "${OOO_SMOKE:-}" ]; then
  Q=$NEXT_STAGE_ROOT/logs-reordered/$SLURM_JOB_ID; mkdir -p $Q
  for f in $LOGS/*x*.eventlog.jsonl; do zstd -q -3 -T8 -f $f -o $Q/$(basename $f).zst || { echo "LOG SAVE FAILED $f"; exit 2; }; done
  cp -a $M/. $Q/
fi
mkdir -p $RES/params; cp -a $PD/. $RES/params/; eval "$EXTCMP_SYNC_HOOK"
load_ok() { $PYV -c "import json,sys; d=json.load(open(sys.argv[1])); sys.exit(0 if 'load' in d and not d.get('load exceeded budget') else 1)" "$1" 2>/dev/null; }
win() { "$KIT/lib/window.sh" "$@" || echo "WINDOW FAILED $* $(utc)"; }
export EXTCMP_SERVER_TIMEOUT_S=3660 EXTCMP_CALL_CEILING_S=3600 EXTCMP_VERIFY_CAP_S=3662 EXTCMP_LOAD_BUDGET_S=${OOO_LOAD_BUDGET_S:-5400}
say "loads"
for d in $DENS; do
  for t in $(tg p$d $d) $(tg x${d}o $d) $(tg x${d}l $d) $(tg x${d}f $d); do
    win load $SIZE $t - - native
    load_ok $RES/$t/load-native.json || { echo "LOAD NOT COMPLETE $t native: stopping"; eval "$EXTCMP_SYNC_HOOK"; exit 2; }
    case $t in *p$d-d*) ;; *) win load $SIZE $t - - duckdbsql; load_ok $RES/$t/load-duckdbsql.json || { echo "LOAD NOT COMPLETE $t duckdbsql: stopping"; eval "$EXTCMP_SYNC_HOOK"; exit 2; } ;; esac
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
done
proto_p1
say "gate: every instance, every log, before any timing; the plain original is Gnomon only, for the comparison"
for d in $DENS; do
  P=$(tg p$d $d); XO=$(tg x${d}o $d); XL=$(tg x${d}l $d); XF=$(tg x${d}f $d)
  for t in $P $XO $XL $XF; do
    SYS="native duckdbsql"; [ $t = $P ] && SYS=native
    for s in $SYS; do
      ( export EXTCMP_PARAMS=$PD/params-$t.json P1_VTAG=reg; "$KIT/lib/window.sh" verify $SIZE $t - - $s ) || { echo "GATE RUN FAILED $t $s reg: stopping"; exit 8; }
      ( export EXTCMP_PARAMS=$PD/params-$t.json P1_VTAG=bel EXTCMP_VERIFY_FAMILY=B; "$KIT/lib/window.sh" verify $SIZE $t - - $s ) || { echo "GATE RUN FAILED $t $s bel: stopping"; exit 8; }
    done
    $PYV $REPO/scripts/extcmp_p1.py gate --dir $RES/$t --ceiling 10 --out $RES/$t/excluded.json | tee $RES/$t/verdicts.txt
  done
  REC=(); if [ "$SIZE" = 10000000 ]; then REC=(--recorded-tag $P --recorded $(ls $E/final/w1/results/gate-*/sp/K10M-d$d/verify-native.json 2>/dev/null) $( [ $d = 0 ] && ls $E/final/w1/results/gate-*/sp/K10M/verify-native.json 2>/dev/null)); fi
  $PYV $ST/ooo_gate.py --res $RES --tags $XO,$P,$XL,$XF --base-tag $XO --out $RES/gate-d$d.md "${REC[@]}"; rc=$?
  eval "$EXTCMP_SYNC_HOOK"; echo "gate d$d rc=$rc" | tee -a $RES/gate.rc
  [ $rc = 0 ] || { echo "GATE FAILED d$d: stopping before any timing $(utc)"; exit 7; }
done
say "timed: paired windows, block checks on and off, logs interleaved by rotation, density by density"
export ABL_LABEL=ooo ABL_PARAMS_DIR=$PD ABL_CONFIG_GATE="n/a (descriptive)" ABL_CONFIG_GATE_1=n/a ABL_CONFIG_GATE_2=n/a ABL_CONFIG_GATE_C4=n/a
A=$RES/ooo; mkdir -p $A
for r in $(seq 1 $ROTS); do
  for d in $DENS; do
    XO=$(tg x${d}o $d); XL=$(tg x${d}l $d); XF=$(tg x${d}f $d)
    order="$XO $XL $XF"; [ $(( r % 2 )) = 0 ] && order="$XF $XL $XO"
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
done
"$KIT/lib/servers.sh" stopall
for d in $DENS; do $PYV $ST/ooo_report.py --res $RES --tags $(tg x${d}o $d),$(tg x${d}l $d),$(tg x${d}f $d) --out $RES/report-d$d.md; done
eval "$EXTCMP_SYNC_HOOK"
[ -e $A/window-failed.txt ] && { echo "FAILED WINDOWS: see window-failed.txt"; exit 8; }
say "explicit-node body done"
