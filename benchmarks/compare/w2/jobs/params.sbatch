#!/bin/bash
#SBATCH -J w2-params
#SBATCH -N 1
#SBATCH --ntasks-per-node=16
#SBATCH -t 06:00:00
#SBATCH -o w2-params-%j.out
# W2: factor tables, P1 selection and hub stratum at 1M and 10M (registry + d0/d1/d10), for the timed stage.
set -uo pipefail
P=${GNOMON_BENCH_ROOT}/w2-powerlaw; R=$P/repo; PY=$R/.venv/bin/python; X="$PY $R/scripts"
L=$P/check1m/campaign/eventlogs; PD=$P/params; mkdir -p $PD/factors
say() { echo "=== $* $(date -u +%FT%TZ)"; }
fact() { local f=$PD/factors/factors-$1.json st=${LOCAL:-${TMPDIR:-/tmp}}/fact-$1; [ -e $f ] && return 0; say factors $1
  $PY -c "import sys, pathlib; sys.path.insert(0, '$R/scripts'); import eval_harness as H; H.load_store(pathlib.Path('$st'), 'native', pathlib.Path('$3'))" || return 1
  $X/extcmp_p1.py factors --kind $2 --log $3 --store $st --out $f || return 1; rm -rf $st; }
for K in K1M K10M; do n=$([ $K = K1M ] && echo 1000000 || echo 10000000)
fact $K registry $L/synth-$n.eventlog.jsonl || exit 1
for d in 0 1 10; do fact $K-d$d belief $L/$K-d$d.eventlog.jsonl || exit 1; done
ch=$P/characterisation/powerlaw-$n.json
$X/extcmp_p1.py select --kind registry --factors $PD/factors/factors-$K.json --factors-d0 $PD/factors/factors-$K-d0.json --out $PD/params-$K.json || exit 1
$X/w2_hub_stratum.py --params $PD/params-$K.json --characterisation $ch --out $PD/params-$K.json || exit 1
for d in 0 1 10; do
  $X/extcmp_p1.py select --kind belief --tag $K-d$d --factors $PD/factors/factors-$K-d$d.json --out $PD/params-$K-d$d.json || exit 1
  $X/w2_hub_stratum.py --params $PD/params-$K-d$d.json --characterisation $ch --out $PD/params-$K-d$d.json || exit 1
done
done
( cd $PD && sha256sum params-K*.json > SHA256SUMS && cat SHA256SUMS )
say done
