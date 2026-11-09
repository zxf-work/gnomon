#!/bin/bash
# the branch to <commit> and re-pins; it refuses while a job of this stage is queued or running.
set -euo pipefail
stage=$1; c=$2; E=${GNOMON_BENCH_ROOT}; S=$E/studies/$stage; K=$E/kit-repin/src
if squeue -u "$USER" -h -o %j | grep -q "^c4-$stage"; then echo "a c4-$stage job is queued or running: not moving its tree"; exit 3; fi
c=$(git -C $E/studies/src rev-parse "$c")
mkdir -p $S/logs $S/results
if [ ! -d $S/src ]; then
  git -C $K worktree add -q -b studies-$stage $S/src $c
  ln -sfn $K/.venv $S/src/.venv
  cp -a $K/gnomon/_engine*.so $S/src/gnomon/
else
  git -C $S/src merge -q --ff-only $c
fi
sha=$(sha256sum $S/src/gnomon/_engine*.so | cut -d' ' -f1)
[ "$sha" = 0711ef20a0454673a6efe837b1742ddf1d2c60ebf192456d8a3e556811263a64 ] || { echo "not the pinned .so: $sha"; exit 3; }
grep -v -E '^export (C3_|EXTCMP_KITHEAD)|^# C3|^# W2|^# W1' $E/confirm/w2/site.env | sed "s#confirm/w2#studies/$stage#g" > $S/site.env.new
echo "export EXTCMP_KITHEAD=$c" >> $S/site.env.new
mv $S/site.env.new $S/site.env
echo "$stage: $c sha $sha"
