#!/bin/bash
#SBATCH -N 1
#SBATCH --ntasks-per-node=32
#SBATCH --signal=B:USR1@300
#SBATCH -J c4-w4-store
source ${NEXT_STAGE_ROOT:?}/src/benchmarks/stages/studies/lib.sh
guard; hostinfo
: "${W4_POINT:?}"
TOOL=$E/studies/src/$STG/tools/w4_store_compare.py; [ -s $TOOL ] || { echo "NO TOOL $TOOL"; exit 2; }
PT=$W4_POINT; SF=${PT%%-*}; RH=${PT##*-}; W=$E/w4-finbench; LOG=$W/eventlogs/w4-$SF-$RH.eventlog.jsonl; REC=$W/w4stores/$PT
O=$E/studies/w4-c4store/$PT; [ -e $O/gnomon-$PT.tar.zst ] && { echo "$O already holds a store: stopping"; exit 2; }; mkdir -p $O
trap 'echo "=== time limit near $(utc): stopping"; echo "STOPPED at the time limit $(utc)" > $O/STOPPED; exit 5' USR1 TERM
need_file $LOG; G=$(ls $REC/gnomon-*.tar.zst | head -1); need_file $G; need_file $G.sha256
step() { say "$1"; shift; "$@" || { echo "STEP FAILED: $*: stopping"; exit 2; }; }
step "copy log" cp $LOG ${LOCAL:-${TMPDIR:-/tmp}}/log.jsonl
want=$(awk -v f="$(basename $LOG)" '$2 == f {print $1}' $W/eventlogs/SHA256SUMS); got=$(sha256sum ${LOCAL:-${TMPDIR:-/tmp}}/log.jsonl | cut -d' ' -f1)
[ -n "$want" ] && [ "$want" = "$got" ] && echo "log sha256 $got OK" || { echo "FROZEN LOG CHECK FAILED ($got vs $want): stopping"; exit 2; }
ST=${LOCAL:-${TMPDIR:-/tmp}}/c4/gnomon-$PT; mkdir -p ${LOCAL:-${TMPDIR:-/tmp}}/c4 ${LOCAL:-${TMPDIR:-/tmp}}/rec
replay() { ( cd $REPO && $PYV -c "
import sys, pathlib, time; sys.path.insert(0, 'scripts'); import eval_harness as H
t=time.time(); H.load_store(pathlib.Path('$ST'), 'native', pathlib.Path('${LOCAL:-${TMPDIR:-/tmp}}/log.jsonl')); print('replay_s', round(time.time()-t,1), 'store and log on \${LOCAL:-${TMPDIR:-/tmp}}')" ); }
step "replay on C4" replay
need_dir $ST/native; rm -f ${LOCAL:-${TMPDIR:-/tmp}}/log.jsonl
pack() { tar -C ${LOCAL:-${TMPDIR:-/tmp}}/c4 -cf - gnomon-$PT | zstd -q -T32 -3 -o $O/gnomon-$PT.tar.zst -f && ( cd $O && sha256sum gnomon-$PT.tar.zst > gnomon-$PT.tar.zst.sha256 ); }
step "persist the C4 store" pack
echo "{\"engine\": \"$EXTCMP_ENGINE_COMMIT\", \"so\": \"$EXTCMP_ENGINE_SO_SHA256\", \"log_sha256\": \"$got\", \"job\": ${SLURM_JOB_ID:-0}}" > $O/gnomon-$PT.tar.zst.json
restore_tar $G ${LOCAL:-${TMPDIR:-/tmp}}/rec; need_dir ${LOCAL:-${TMPDIR:-/tmp}}/rec/gnomon-$PT/native
export GNOMON_STORE_WARM_OPEN=0     # the digest reads every row and no query: the warm open's structures are not needed
step "compare" $PYV $TOOL --a ${LOCAL:-${TMPDIR:-/tmp}}/rec/gnomon-$PT --b $ST --a-name f10733cf --b-name C4 --out $O/compare
say "store check done"
