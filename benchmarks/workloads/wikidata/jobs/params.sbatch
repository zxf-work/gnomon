#!/bin/bash
#SBATCH -J w5-params
#SBATCH --ntasks-per-node=8
#SBATCH -t 01:00:00
#SBATCH -o params-%j.out
# usage: sbatch params.sbatch <tag> <outdir>   (curation from the frozen log only)
P=${GNOMON_BENCH_ROOT}/w5-wikidata
source $P/site.env; cd $P/src; source .venv/bin/activate
/usr/bin/time -v python scripts/wd_params.py --log $P/eventlogs/$1.eventlog.jsonl --out $2 2> $P/logs/params-$1.time | tail -40
grep -E "Elapsed|Maximum" $P/logs/params-$1.time; tail -3 $P/logs/params-$1.time
cd $2 && sha256sum params-$1-*.json factors-$1.json > SHA256SUMS-$1 && cat SHA256SUMS-$1
