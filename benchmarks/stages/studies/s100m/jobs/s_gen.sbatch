#!/bin/bash
#   GEN=freeze-w2  scripts/freeze_powerlaw_log.py --scale 100000000 --gamma 2.1 --seed 2101 (W2's 10M command in w2-powerlaw/jobs/prep.sbatch,
#                  --scale 10000000 there; |V| = scale/100 and edge lifetime = scale/20 follow from the scale, as for every size)
#   GEN=inject     BASE (sha256 BASE_SHA) replayed into a scratch native store (eval_harness.load_store), scripts/inject_corrections.py
#                  --density 10 --seed 0 --batch 1000, log = base + the store's "tt" lines (kit run_beliefq.sh and w2-powerlaw/jobs/prep.sbatch,
#                  the rule of K10M-d10 and W2-10M-d10)
#   GEN=params     no generation: LOGSRC/<TAG>.eventlog.jsonl checked against SUMS (the smoke's 1M log)
#   Then: tools/x_params.py k100m (one instance per registry id, family B included; the log's side files to params/<TAG>/side), the way the
#   K100M-d0 file was made, on a node-local copy whose mtime equals the stage copy's (getlogs' rule, so the side files stay valid), and
#   tools/x_extra.py (3 more instances per query, files only). Nothing is timed.
# Smoke of this script (not a result): GEN_EV, GEN_SUMS, GEN_PDIR, GEN_XDIR name other directories, GEN_SCALE another size.
#SBATCH -N 1
#SBATCH --ntasks-per-node=64
#SBATCH -J s100m-gen
set -uo pipefail
: "${NEXT_STAGE_ROOT:?}" "${GEN:?}" "${TAG:?}"
source $NEXT_STAGE_ROOT/gen.env
utc() { date -u +%FT%TZ; }; say() { echo "=== $* $(utc)"; }
XT=$REPO/benchmarks/stages/studies/t10/tools
REC=$NEXT_STAGE_ROOT/gen/$TAG; mkdir -p $REC
L=${LOCAL:?}/sg; mkdir -p $L/tmp; export TMPDIR=$L/tmp
EV=${GEN_EV:-$NEXT_STAGE_ROOT/eventlogs}; SUMS=${GEN_SUMS:-$EV/SHA256SUMS}
PDIR=${GEN_PDIR:-$NEXT_STAGE_ROOT/params/$TAG}; XDIR=${GEN_XDIR:-$NEXT_STAGE_ROOT/params-x3/$TAG}
head=$(git -C $REPO rev-parse HEAD); so=$(sha256sum $REPO/gnomon/_engine*.so | cut -d' ' -f1)
echo "host $(hostname) job ${SLURM_JOB_ID:-none} cores $(nproc) LOCAL ${LOCAL:-${TMPDIR:-/tmp}} free $(df -h ${LOCAL:-${TMPDIR:-/tmp}} | tail -1 | awk '{print $4}') mem $(free -g | awk '/Mem/{print $2}') GB; kit $head; so $so"
[ "$so" = "$EXTCMP_ENGINE_SO_SHA256" ] || { echo "GUARD: engine .so is not the pinned build: refusing"; exit 3; }
imp=$(cd /tmp && $PYV -c "import gnomon, gnomon._engine as e; print(gnomon.__file__, e.__file__)" 2>&1); echo "import $imp"
case "$imp" in "$REPO/gnomon/__init__.py $REPO/gnomon/_engine"*) ;; *) echo "GUARD: gnomon does not import from $REPO: refusing"; exit 3 ;; esac
env | grep -q -E '^GNOMON_' && { echo "GUARD: a GNOMON_ switch is set: refusing"; env | grep -E '^GNOMON_'; exit 3; }
for f in scripts/freeze_powerlaw_log.py scripts/inject_corrections.py scripts/eval_harness.py; do echo "script $f blob $(git -C $REPO hash-object $REPO/$f)"; done
$PYV $XT/ing_rss.py $REC/rss-${SLURM_JOB_ID:-none}.jsonl 5 & RSSPID=$!
trap 'kill -TERM $RSSPID 2>/dev/null' EXIT
out=$L/$TAG.eventlog.jsonl
case $GEN in
  freeze-w2)
    cmd=($PYV $REPO/scripts/freeze_powerlaw_log.py --scale ${GEN_SCALE:-100000000} --gamma 2.1 --seed 2101 --out $out)   # GEN_SCALE: the 1M smoke of this script only
    say "freeze: ${cmd[*]}"; /usr/bin/time -v "${cmd[@]}" 2> $REC/freeze.time; rc=$?
    [ $rc = 0 ] || { echo "FREEZE FAILED rc=$rc"; tail -30 $REC/freeze.time; exit 2; }
    grep -E "Elapsed|Maximum resident" $REC/freeze.time
    cp -a $L/$TAG.eventlog.meta.json $REC/ ;;
  inject)
    : "${BASE:?}" "${BASE_SHA:?}"
    # BASE_SHA=sums: the base is a log of this stage; its sha256 is its line in this stage's sums file (W2-100M-d10, after the freeze job)
    if [ "$BASE_SHA" = sums ]; then BASE_SHA=$(awk -v f="$(basename $BASE)" '$2 == f {print $1}' $SUMS); echo "base sha256 from $SUMS: $BASE_SHA"; fi
    [ -n "$BASE_SHA" ] || { echo "no sha256 for $BASE"; exit 2; }
    say "copy base $BASE"; cp $BASE $L/base.eventlog.jsonl || exit 2
    got=$(sha256sum $L/base.eventlog.jsonl | cut -d' ' -f1)
    [ "$got" = "$BASE_SHA" ] && echo "base sha256 $got OK" || { echo "BASE CHECK FAILED ($got vs $BASE_SHA)"; exit 2; }
    st=$L/inject-$TAG; rm -rf $st
    say "load_store native"
    /usr/bin/time -v $PYV -c "import sys, pathlib; sys.path.insert(0, '$REPO/scripts'); import eval_harness as H; H.load_store(pathlib.Path('$st'), 'native', pathlib.Path('$L/base.eventlog.jsonl'))" 2> $REC/load.time; rc=$?
    [ $rc = 0 ] || { echo "LOAD FAILED rc=$rc"; tail -30 $REC/load.time; exit 2; }
    grep -E "Elapsed|Maximum resident" $REC/load.time
    cmd=($PYV $REPO/scripts/inject_corrections.py --store $st --density 10 --seed 0 --batch 1000 --record $REC/inject-receipt.json)
    say "inject: ${cmd[*]}"; /usr/bin/time -v "${cmd[@]}" 2> $REC/inject.time; rc=$?
    [ $rc = 0 ] || { echo "INJECT FAILED rc=$rc"; tail -30 $REC/inject.time; exit 2; }
    grep -E "Elapsed|Maximum resident" $REC/inject.time; cat $REC/inject-receipt.json
    say "log = base + the store's tt lines"
    { cat $L/base.eventlog.jsonl; grep '"tt"' $st/eventlog.jsonl; } > $out.tmp && mv $out.tmp $out || { echo "LOG WRITE FAILED"; exit 2; }
    echo "lines: base $(wc -l < $L/base.eventlog.jsonl), store tt lines $(grep -c '"tt"' $st/eventlog.jsonl), store log lines $(wc -l < $st/eventlog.jsonl), new log $(wc -l < $out)"
    rm -rf $st $L/base.eventlog.jsonl ;;
  params)
    : "${LOGSRC:?}"; cp $LOGSRC/$TAG.eventlog.jsonl $out || exit 2 ;;
  *) echo "unknown GEN $GEN"; exit 2 ;;
esac
sha=$(sha256sum $out | cut -d' ' -f1); bytes=$(stat -c %s $out)
echo "log $TAG sha256 $sha bytes $bytes"
if [ $GEN = params ]; then
  want=$(awk -v f="$TAG.eventlog.jsonl" '$2 == f {print $1}' $SUMS)
  [ "$want" = "$sha" ] && echo "log $TAG = frozen sha256 OK" || { echo "FROZEN LOG CHECK FAILED ($sha vs $want)"; exit 2; }
  touch -m -d @$(stat -c %Y $LOGSRC/$TAG.eventlog.jsonl) $out
else
  mkdir -p $EV
  [ -e $EV/$TAG.eventlog.jsonl ] && { echo "$EV/$TAG.eventlog.jsonl exists: refusing to overwrite"; exit 2; }
  say "copy to $EV"
  cp $out $EV/$TAG.eventlog.jsonl.tmp && mv $EV/$TAG.eventlog.jsonl.tmp $EV/$TAG.eventlog.jsonl || exit 2
  [ -e $L/$TAG.eventlog.meta.json ] && cp $L/$TAG.eventlog.meta.json $EV/
  got=$(sha256sum $EV/$TAG.eventlog.jsonl | cut -d' ' -f1)
  [ "$got" = "$sha" ] && echo "stage copy sha256 OK" || { echo "STAGE COPY DIFFERS ($got vs $sha)"; exit 2; }
  echo "$sha  $TAG.eventlog.jsonl" >> $SUMS
  { echo "tag $TAG"; echo "gen $GEN ${cmd[*]:-}"; echo "base ${BASE:-none} ${BASE_SHA:-}"; echo "kit $head"; echo "engine so $so"
    echo "sha256 $sha"; echo "bytes $bytes"; echo "job ${SLURM_JOB_ID:-none} host $(hostname) finished $(utc)"; } > $REC/GEN.txt
  touch -m -d @$(stat -c %Y $EV/$TAG.eventlog.jsonl) $out
fi
cd $REPO; export PYTHONPATH=$REPO:$REPO/scripts
say "parameter file (x_params.py k100m)"
$PYV $XT/x_params.py k100m --log $out --sums $SUMS --tag $TAG --out $PDIR || { echo "PARAMS FAILED"; exit 2; }
say "extra instances (x_extra.py)"
$PYV $XT/x_extra.py --log $out --base $PDIR/params-$TAG.json --sums $SUMS --out $XDIR --tag $TAG > $REC/x_extra.out 2>&1 && head -1 $REC/x_extra.out || { echo "EXTRA PARAMS FAILED (the one-instance point is unaffected)"; tail -5 $REC/x_extra.out; }
say "gen done $TAG"
