#!/bin/bash
#SBATCH -J w3-freeze
#SBATCH -N 1
#SBATCH -o freeze-%j.out
# W3 frozen logs for one SF: base(s), nested densities at rho_gen = 10 % (lag mix), the lag sweep at 1 %,
# history 3x and max at rho {0, 1 %}. Seeds fixed: mix 1, lag sweep 2/3/4, history 5.
set -uo pipefail
P=${GNOMON_BENCH_ROOT}/w3-snb
source $P/site.env; cd $EXTCMP_REPO; source .venv/bin/activate
SF=${SF:?}; L=$P/eventlogs; mkdir -p $L $P/base
say() { echo "=== $* $(date -u +%FT%TZ)"; }
base() { local h=$1 B=$P/base/sf$SF-h$1; [ -e $B/meta.json ] || { say base h$h; /usr/bin/time -v python scripts/w3_snb_log.py base --raw $P/data/sf$SF-raw --out $B --history-mult $2 > $P/logs/base-sf$SF-h$h.json 2> $P/logs/base-sf$SF-h$h.time || { tail -20 $P/logs/base-sf$SF-h$h.time; exit 1; }; grep -E "Elapsed|Maximum res" $P/logs/base-sf$SF-h$h.time; }; }
inject() { say inject $*; /usr/bin/time -v python scripts/w3_snb_log.py inject "$@" 2> $P/logs/inject-$(date +%s%N).time || exit 1; }
base 1 1
inject --base $P/base/sf$SF-h1 --rhos 0,0.0002,0.003,0.01,0.1 --rho-gen 0.1 --lag mix --seed 1 --out-pattern "$L/W3-SF$SF-h1-r{r}.eventlog.jsonl"
if [ "${LAGSWEEP:-1}" = 1 ]; then
  s=2; for lag in same months years; do
    inject --base $P/base/sf$SF-h1 --rhos 0.01 --rho-gen 0.01 --lag $lag --seed $s --out-pattern "$L/W3-SF$SF-h1-r{r}-lag$lag.eventlog.jsonl"; s=$((s+1)); done
fi
if [ "${HIST:-1}" = 1 ]; then
  for h in 3 max; do m=$h; [ $h = max ] && m=100; base $h $m
    inject --base $P/base/sf$SF-h$h --rhos 0,0.01 --rho-gen 0.01 --lag mix --seed 5 --out-pattern "$L/W3-SF$SF-h$h-r{r}.eventlog.jsonl"; done
fi
say checksums; cd $L && sha256sum W3-SF$SF-*.eventlog.jsonl > SHA256SUMS-SF$SF && cat SHA256SUMS-SF$SF; du -sh $L
say done
