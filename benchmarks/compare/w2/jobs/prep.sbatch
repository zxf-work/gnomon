#!/bin/bash
#SBATCH -J w2-prep
#SBATCH -N 1
#SBATCH --ntasks-per-node=16
#SBATCH -t 08:00:00
#SBATCH -o w2-prep-%j.out
# W2 preparation: engine build, power-law logs at 1M and 10M,
# characterisation, density logs d1/d10 (existing injector, seed 0, batch 1000).
set -uo pipefail
P=${GNOMON_BENCH_ROOT}/w2-powerlaw
source $P/site.env
cd $EXTCMP_REPO; unset CARGO_TARGET_DIR; export CARGO_NET_OFFLINE=true
source .venv/bin/activate
say() { echo "=== $* $(date -u +%FT%TZ)"; }
if [ ! -e $P/engine-build.txt ]; then
  say build
  maturin develop --release --extras eval,duckdb 2>&1 | tail -5
  python -c "import gnomon._engine as e; print(e.__file__)" > $P/engine-build.txt || exit 1
  git rev-parse HEAD >> $P/engine-build.txt; sha256sum $(python -c "import gnomon._engine as e; print(e.__file__)") >> $P/engine-build.txt
fi
L=$P/eventlogs; mkdir -p $L $P/characterisation
for size in 1000000 10000000; do
  base=$L/powerlaw-$size.eventlog.jsonl
  say freeze $size
  /usr/bin/time -v python scripts/freeze_powerlaw_log.py --scale $size --gamma 2.1 --seed 2101 --out $base 2> $P/logs/freeze-$size.time || exit 1
  say characterise $size
  python scripts/w2_characterise.py --log $base --json $P/characterisation/powerlaw-$size.json || exit 1
  tag=W2-$((size/1000000))M
  [ -e $L/$tag-d0.eventlog.jsonl ] || ln $base $L/$tag-d0.eventlog.jsonl
  for d in 1 10; do
    dlog=$L/$tag-d$d.eventlog.jsonl; [ -e $dlog ] && continue
    say inject $tag d$d
    st=${LOCAL:-${TMPDIR:-/tmp}}/inject-$tag-d$d; rm -rf $st
    python -c "
import sys, pathlib; sys.path.insert(0, 'scripts')
import eval_harness as H; H.load_store(pathlib.Path('$st'), 'native', pathlib.Path('$base'))" || exit 1
    /usr/bin/time -v python scripts/inject_corrections.py --store $st --density $d --seed 0 --batch 1000 \
        --record $P/characterisation/inject-$tag-d$d.json 2> $P/logs/inject-$tag-d$d.time || exit 1
    { cat $base; grep '"tt"' $st/eventlog.jsonl; } > $dlog.tmp && mv $dlog.tmp $dlog
    rm -rf $st
  done
done
say checksums
cd $L && sha256sum powerlaw-*.eventlog.jsonl W2-*-d1.eventlog.jsonl W2-*-d10.eventlog.jsonl > SHA256SUMS && cat SHA256SUMS
say done
