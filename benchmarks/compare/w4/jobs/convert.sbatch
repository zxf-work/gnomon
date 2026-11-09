#!/bin/bash
#SBATCH -J w4-convert
#SBATCH -N 1
#SBATCH --ntasks-per-node=32
#SBATCH -t 10:00:00
#SBATCH -o w4-convert-%j.out
# W4: FinBench raw -> frozen Gnomon event logs at rho in {0, 0.02, 0.3, 1, 10} % (SFS="1 10").
set -uo pipefail
W=${GNOMON_BENCH_ROOT}/w4-finbench
R2=${GNOMON_BENCH_ROOT}/w2-powerlaw/repo
say() { echo "=== $* $(date -u +%FT%TZ)"; }
L=$W/eventlogs; mkdir -p $L
for sf in ${SFS:-1}; do
  for rho in 0 0.02 0.3 1 10; do
    out=$L/w4-sf$sf-r$rho.eventlog.jsonl
    [ -e $out ] && continue
    say convert SF$sf rho=$rho
    /usr/bin/time -v $R2/.venv/bin/python -I $R2/scripts/w4_finbench_log.py --raw $W/data/sf$sf --rho $rho --seed 0 \
      --threads 32 --out $out.tmp 2> $W/logs/convert-sf$sf-r$rho.time > /dev/null
    [ -s $out.tmp ] || { echo "CONVERT FAILED SF$sf rho=$rho"; tail -20 $W/logs/convert-sf$sf-r$rho.time; exit 1; }
    mv $out.tmp $out; mv ${out%.jsonl}.meta.json.tmp ${out%.jsonl}.meta.json
    python3 -c "import json,sys; m=json.load(open(sys.argv[1])); c=m['corrections']; print('batches', m['batches'], 'applied', c['applied'], 'pct', c['measured_pct'], 'wall_s', m['wall_s'], {k: (v.get('n'), v.get('lag_days_p50')) for k, v in c['by_kind'].items()})" ${out%.jsonl}.meta.json
  done
done
( cd $L && sha256sum w4-*.eventlog.jsonl > SHA256SUMS && cat SHA256SUMS )
say done
