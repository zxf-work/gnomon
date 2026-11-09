#!/bin/bash
# W3 gate re-run of the IS4 and TLDBC-IS4 window cells of Neo4j and Memgraph with the stage's Cypher text override
# (cypher-override/window/IS4.cypher: column order of the instant text). Untimed. Records go to $W3/results/rerun-<job>/<point>/<system>/
# (params.jsonl, settings.txt = window, zz-rerun.jsonl) and are merged over the gate jobs' records by tools/w3_gate.py when the
# dirs are given in this order to --res. Nothing else about the comparison changes.
#   sbatch -p a shared node --ntasks-per-node=64 -t <limit> --export=ALL,W3_RERUN_POINT=SF1|SF10,W3_RERUN_SYS="neo4j memgraph"
#SBATCH -N 1
#SBATCH --signal=B:USR1@120
#SBATCH -J w3-rerun
#SBATCH -o %x-%j.out
source ${W3_STAGE_ROOT:-${GNOMON_BENCH_ROOT}/final/w3}/src/benchmarks/stages/w3/jobs/w3_lib.sh
unset EXTCMP_NUMA_CLIENT EXTCMP_NUMA_SERVER
guard; hostinfo
: "${W3_RERUN_POINT:?}" "${W3_RERUN_SYS:?}"
pt_config $W3_RERUN_POINT || exit 2
OUT=$W3/results/rerun-$SLURM_JOB_ID; mkdir -p $OUT/$PT
setup_root ${LOCAL:-${TMPDIR:-/tmp}}/w3r
trap '"$KIT/lib/servers.sh" stopall; exit 5' USR1 TERM
HISTL=${LOCAL:-${TMPDIR:-/tmp}}/w3hist.duckdb; cp $HIST $HISTL || exit 2
rc=0
for s in $W3_RERUN_SYS; do
  d=$OUT/$PT/$s; mkdir -p $d; TARGET=""
  $PYV - $PARAMS $(n_of $s) $d/params.jsonl <<'PY'
import json, sys
n = int(sys.argv[2])
with open(sys.argv[3], "w") as o:
    for l in open(sys.argv[1]):
        r = json.loads(l)
        if r["qid"] in ("IS4", "TLDBC-IS4") and r["i"] < n:
            o.write(l)
PY
  echo window > $d/settings.txt
  say "$s: restore and start"
  restore_sys $s && srv_start $s || { echo "RESTORE/START FAILED $s" | tee $d/FAILED; srv_stop $s; rc=2; continue; }
  say "$s: IS4 and TLDBC-IS4, window, with the text override"
  $PYV $TOOLS/w3_run.py run --system $s --target "$TARGET" --params $d/params.jsonl --queries IS4,TLDBC-IS4 --settings window \
      --hist-db $HISTL --out $d/zz-rerun.jsonl >> $d/run.log 2>&1 || { echo "RUN FAILED $s" | tee -a $d/FAILED; rc=2; }
  srv_stop $s
done
"$KIT/lib/servers.sh" stopall
say "rerun done rc=$rc"
exit $rc
