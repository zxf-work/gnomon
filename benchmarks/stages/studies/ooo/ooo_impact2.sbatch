#!/bin/bash
# Impact scan of the logs the evaluation reports (read-only): ooo_impact2.py per log, no gnomon import, 8 logs at a time. IMPACT_SMOKE=1: the smallest logs.
#SBATCH -N 1
#SBATCH --ntasks-per-node=32
#SBATCH -J c4-ooo-impact2
E=${GNOMON_BENCH_ROOT}; ST=${NEXT_STAGE_ROOT:?}; O=$ST/impact2/$SLURM_JOB_ID; mkdir -p $O
PYV=$ST/src/.venv/bin/python; T=$ST/impact2-tool/ooo_impact2.py
V1=$E/v3/eventlogs; V2=$E/w2-powerlaw/check1m/campaign/eventlogs; V3=$E/w3-snb/eventlogs; V4=$E/w4-finbench/eventlogs; V5=$E/w5-wikidata/campaign/eventlogs
LIST=$O/list.txt
if [ -n "${IMPACT_SMOKE:-}" ]; then
  printf '%s\n' "W1-K1M $V1/synth-1000000.eventlog.jsonl" "W5-smallpedia $V5/W5-smallpedia.eventlog.jsonl" "W3-SF0.1-h1-r1 $V3/W3-SF0.1-h1-r1.eventlog.jsonl" "W4-sf1-r10 $V4/w4-sf1-r10.eventlog.jsonl" > $LIST
else
  { for t in K10M-d0 K10M-d1 K10M-d10; do echo "W1-$t $V1/$t.eventlog.jsonl"; done; echo "W1-K1M $V1/synth-1000000.eventlog.jsonl"
    for t in K1M-d0 K1M-d1 K1M-d10 K10M-d0 K10M-d1 K10M-d10; do echo "W2-$t $V2/$t.eventlog.jsonl"; done
    echo "W5-wikidata-v2 $V5/W5-wikidata-v2.eventlog.jsonl"
    for t in r0 r0p02 r0p3 r1 r10 r1-lagsame r1-lagmonths r1-lagyears; do echo "W3-SF1-h1-$t $V3/W3-SF1-h1-$t.eventlog.jsonl"; done
    for t in h3-r0 h3-r1 hmax-r0 hmax-r1; do echo "W3-SF1-$t $V3/W3-SF1-$t.eventlog.jsonl"; done
    echo "W4-sf10-r0 $V4/w4-sf10-r0.eventlog.jsonl"; echo "W4-sf10-r10 $V4/w4-sf10-r10.eventlog.jsonl"
    echo "W3-SF10-h1-r1 $V3/W3-SF10-h1-r1.eventlog.jsonl"; } > $LIST
fi
$PYV -c "import orjson; print('orjson', orjson.__version__)" 2>&1 | tail -1
while read -r n p; do [ -s "$p" ] || echo "MISSING $n $p"; done < $LIST
grep -v -c MISSING $LIST
cat $LIST | xargs -P ${IMPACT_P:-8} -L 1 bash -c "$PYV $T \"\$1\" $O/\$0.json || echo FAILED \$0"
date -u; ls $O/*.json | wc -l
