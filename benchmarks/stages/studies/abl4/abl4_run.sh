#!/bin/bash
# per (store, configuration) block; rotation 1 blocks U P T T P U, rotation 2 blocks P T U U T P; 5 warm-ups + 30 timed calls per id
# (10 if the first timed call takes >= 1 s), n = 120 calls per configuration; direct neighbors_batch nb50 and nb800 with direction
# both, at T and at now (--nb). U unset, P GNOMON_PATHS_KERNEL=python, T GNOMON_TT_E_COLUMN=0; GNOMON_MATCH_THREADS=32 in every block.
# Stores: tiered (-u) and settled at d0, d1, d10 of 10M, from run_ablation's stores step; paths.k also on K1M. An untimed
#   abl4_run.sh <record dir>; ABL_SMOKE=1: K1M-d0 only, 3 ids, 1 + 3 calls, gate bypassed and said so.
source ${NEXT_STAGE_ROOT:?}/src/benchmarks/stages/studies/lib.sh
setup_root ${EXTCMP_ROOT:?}
source $KIT/lib/env.sh
source $KIT/lib/lock.sh
OUTD=${1:?record dir}; mkdir -p $OUTD
A4=$REPO/$STG/abl4; REG=hist.single,hist.asof,snap.hop2,diff.global,reach.window,paths.k,series.count,burst.zscore,nbr.evolution,coactive.narrow,resolve.substr,agg.rel_bucket,motif.filtered,series.count.w10,agg.rel_bucket.w10,burst.zscore.w10
B=b.snap.hop2@T,b.snap.hop2@now,b.reach@T,b.reach@now,b.diff,b.diff@ctl,b.hist,b.hist@now
PYC="${EXTCMP_NUMA_CLIENT:+$EXTCMP_NUMA_CLIENT }$PYV"
block() {   # file arm blk rot store tag log ids nb warm reps
  local f=$1 arm=$2 b=$3 r=$4 st=$5 tg=$6 lg=$7 ids=$8 nb=$9 w=${10} rp=${11} e
  case $arm in U) e=X_ABL4=1 ;; P) e=GNOMON_PATHS_KERNEL=python ;; T) e=GNOMON_TT_E_COLUMN=0 ;; esac
  env $e $PYC $A4/abl4_reg.py --store $st --tag $tg --log $LOGS/$lg.eventlog.jsonl --ids "$ids" --arm $arm --block $b --rot $r \
      --json $f --warm $w --reps $rp $( [ $nb = 1 ] && echo --nb ) || { echo "ABL4 BLOCK FAILED $st $arm b$b r$r: stopping"; exit 8; }
}
win() {     # name rot store tag log ids nb arms
  local name=$1 r=$2 st=$3 tg=$4 lg=$5 ids=$6 nb=$7 arms=$8 a b=0; local -a A order
  need_dir $STORES/$st/native; need_file $LOGS/$lg.eventlog.jsonl
  IFS=, read -ra A <<< "$arms"; order=("${A[@]}"); for ((i=${#A[@]}-1;i>=0;i--)); do order+=("${A[$i]}"); done
  say "abl4 window $name rot $r $st order ${order[*]}"
  for a in "${order[@]}"; do block $OUTD/$name.jsonl $a $b $r $st $tg $lg "$ids" $nb $W $RP; b=$((b+1)); done
  $PYV $A4/abl4_check.py $OUTD/$name.jsonl || { echo "ABL4 ANSWER MISMATCH $name: stopping"; exit 7; }
  [ -n "${EXTCMP_SYNC_HOOK:-}" ] && eval "$EXTCMP_SYNC_HOOK"
}
if [ "${ABL_SMOKE:-0}" = 1 ]; then
  SEL="native-K1M-d0-u:K1M-d0:K1M-d0 native-K1M-d0-settled:K1M-d0:K1M-d0"; IDS=hist.single,paths.k,nbr.evolution,b.hist; W=1; RP=3; K1=${ABL4_SMOKE_K1:-}
else
  SEL=""; for d in 0 1 10; do SEL="$SEL native-K10M-d$d-u:K10M-d$d:K10M-d$d native-K10M-d$d-settled:K10M-d$d:K10M-d$d"; done
  IDS=$REG,$B; W=5; RP=30; K1=1
fi
if [ -n "$K1" ]; then     # paths.k also on K1M
  cp -a $E/v3/eventlogs/synth-1000000.eventlog.* $LOGS/; need_file $LOGS/synth-1000000.eventlog.jsonl
  [ -d $STORES/native-K1M ] || heavy_run $PY load --system native --log $LOGS/synth-1000000.eventlog.jsonl --workdir "$EXTCMP_ROOT" --size-tag K1M --json $OUTD/load-K1M.json
  need_dir $STORES/native-K1M
fi
say "abl4 priming pass"
PW=$W; PR=$RP
for s in $SEL ${K1:+native-K1M:K1M:synth-1000000}; do
  IFS=: read -r st tg lg <<< "$s"; ids=$IDS; [ "$st" = native-K1M ] && ids=paths.k
  need_dir $STORES/$st/native
  for a in U P T; do block $OUTD/prime.jsonl $a 0 0 $st $tg $lg "$ids" 1 1 1; done
done
$PYV $A4/abl4_check.py $OUTD/prime.jsonl || { echo "ABL4 ANSWER MISMATCH in the priming pass: stopping"; exit 7; }
for r in 1 2; do
  [ $r = 1 ] && { arms=U,P,T; parms=U,P; } || { arms=P,T,U; parms=P,U; }
  [ -n "$K1" ] && win abl4-K1M $r native-K1M K1M synth-1000000 paths.k 0 $parms
  for s in $SEL; do IFS=: read -r st tg lg <<< "$s"; win abl4-$st $r $st $tg $lg "$IDS" 1 $arms; done
done
$PYV $A4/abl4_sum.py $OUTD/abl4-*.jsonl > $OUTD/summary.txt || { echo "abl4 summary failed"; exit 8; }
$PYV $A4/abl4_aa.py $OUTD/abl4-*.jsonl > $OUTD/aa.txt; $PYV $A4/abl4_fam.py $OUTD/abl4-*.jsonl > $OUTD/families.txt 2>&1
say "abl4 done"
