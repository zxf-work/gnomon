#!/bin/bash
#SBATCH -J w5-overlap
#SBATCH --ntasks-per-node=16
#SBATCH -t 01:00:00
#SBATCH -o overlap-%j.out
P=${GNOMON_BENCH_ROOT}/w5-wikidata
source $P/site.env; cd $P/src; source .venv/bin/activate
for N in "$@"; do echo "== $N"; python scripts/wd_overlap.py --stagea $P/stageA/$N --log $P/eventlogs/W5-$N.eventlog.jsonl --tgb $P/raw/tgb/tkgl-$( [ $N = smallpedia ] && echo smallpedia || echo wikidata )_edgelist.csv --out $P/eventlogs/W5-$N.tgb-overlap.json; done
