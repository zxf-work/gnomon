#!/bin/bash
# W3 AeonG job body (run by w3_aeong.sbatch in its own process group).
source ${W3_STAGE_ROOT:-${GNOMON_BENCH_ROOT}/final/w3}/src/benchmarks/stages/w3/jobs/w3_lib.sh
: "${OUT:?}"
pt_config SF1 || exit 2
setup_root ${LOCAL:-${TMPDIR:-/tmp}}/w3a
RESL=${LOCAL:-${TMPDIR:-/tmp}}/w3a/res; mkdir -p $RESL/gate/SF1
syncall() { cp -au $RESL/. $OUT/; }
( cd $PARD && sha256sum -c SHA256SUMS --ignore-missing ) || exit 2
HISTL=${LOCAL:-${TMPDIR:-/tmp}}/w3hist.duckdb; cp $HIST $HISTL || exit 2
AH=${LOCAL:-${TMPDIR:-/tmp}}/aeong-hist; say "copy the SF1 history tables"; cp -r $P3/check/SF1-h1-r1/hist $AH || exit 2
say "aeong: start (server query timeout ${EXTCMP_SERVER_TIMEOUT_S} s)"
TARGET=""; srv_start aeong || { echo "AEONG START FAILED"; exit 6; }
URI=$TARGET
say "aeong: split load"
$PYV $REPO/scripts/w3_snb_systems.py aeong-load --hist $AH --uri $URI --chunk 5000 2>&1 | cut -c1-1500 | tee $RESL/aeong-load.log
[ -s $AH/aeong-load.json ] || { echo "AEONG LOAD FAILED: no queries on a partial store"; syncall; exit 6; }
cp $AH/aeong-load.json $RESL/
say "aeong: ceiling check"
$PYV $REPO/scripts/w3_snb_systems.py aeong-ceiling --hist $AH --uri $URI --ceiling 10 --out $RESL/aeong-ceiling.json
crc=$?
if [ $crc -ne 0 ]; then echo "CEILING NOT ENFORCEABLE (rc $crc): AeonG cells are not timed"; syncall; exit 7; fi
Q="$Q_IS,$Q_TL"; SUB=$RESL/aeong-params.jsonl
$PYV $TOOLS/w3_timed.py subset --params $PARAMS --n $NSMALL --out $SUB
FORMS="aeong aeong-prop"
say "aeong: time map check (bounded sample, tools/w3_aeong_tmcheck.py)"
$PYV $TOOLS/w3_aeong_tmcheck.py --hist $AH --uri $URI --params $SUB --out $RESL/aeong-timemap.json --max-t 5 --deadline-s 900 2>&1 | tee $RESL/aeong-timemap.log | tail -n 40
trc=${PIPESTATUS[0]}
if [ $trc -ne 0 ]; then echo "TIME MAP NOT VERIFIED (rc $trc): the native form is not timed; the property form still is"; FORMS="aeong-prop"; fi
syncall
say "aeong: answer check (both forms, IS and T-LDBC)"
for f in $FORMS; do
  d=$RESL/gate/SF1/$f; mkdir -p $d; cp $SUB $d/params.jsonl; echo current,asof,window > $d/settings.txt
  timeout -k 30 $((VWIN * 2)) $PYV $TOOLS/w3_run.py run --system $f --target "$URI,$AH" --params $SUB --queries $Q \
      --settings current,asof,window --hist-db $HISTL --out $d/all.jsonl >> $d/run.log 2>&1 || echo "answer check window cut or failed (rc $?)" >> $d/windows.txt
done
$PYV $TOOLS/w3_gate.py --point SF1 --res $RESL/gate --gnomon-ref $W3/gate/ref-SF1-gnomon.jsonl --systems aeong,aeong-prop --out-dir $RESL/gate-verdict
syncall
TR="${EXTCMP_NUMA_CLIENT:+$EXTCMP_NUMA_CLIENT }$PYV $TOOLS/w3_timed.py run"
for r in 1 2; do
  order=($FORMS); [ $r = 2 ] && order=($(echo $FORMS | tr " " "\n" | tac | tr "\n" " "))
  for f in "${order[@]}"; do
    say "timed $f rotation $r"; mkdir -p $RESL/rot$r
    $TR --system $f --target "$URI,$AH" --params $PARAMS --cells "$Q" --settings current,asof,window --instances $(seq -s, 0 $((NSMALL-1))) \
        --hist-db $HISTL --rotation $r --exclude $RESL/gate-verdict/exclusions-SF1.json --excl-system $f \
        --out $RESL/rot$r/S-$f.json --jsonl $RESL/rot$r/S-$f-calls.jsonl --label "w3-final SF1 rot$r S $f" 2>&1 | tail -n 3
    syncall
  done
done
$PYV $TOOLS/w3_timed.py audit-dir $RESL | tee $RESL/censor-once-audit.txt
srv_stop aeong; syncall; say "aeong body done"
