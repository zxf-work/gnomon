#!/bin/bash
# W3 timed stage, gate verdict: tools/w3_gate.py over the gate jobs' records of one point. Exit code 1 on a FAIL, so
# --dependency=afterok on this job blocks the timed jobs.
#   sbatch --dependency=afterok:<gate jobs> --export=ALL,W3_VERDICT_POINT=SF1,W3_VERDICT_DIRS="<gate result dirs>",W3_VERDICT_REQUIRE="gnomon,duckdb,..."
#SBATCH -N 1
#SBATCH --ntasks-per-node=2
#SBATCH -t 00:30:00
#SBATCH -J w3-verdict
#SBATCH -o %x-%j.out
source ${W3_STAGE_ROOT:-${GNOMON_BENCH_ROOT}/final/w3}/src/benchmarks/stages/w3/jobs/w3_lib.sh
guard
: "${W3_VERDICT_POINT:?}" "${W3_VERDICT_DIRS:?}" "${W3_VERDICT_REQUIRE:?}"
say "verdict $W3_VERDICT_POINT from [$W3_VERDICT_DIRS]"
$PYV $TOOLS/w3_gate.py --point $W3_VERDICT_POINT --res $W3_VERDICT_DIRS --require "$W3_VERDICT_REQUIRE" ${W3_VERDICT_BASE:+--base $W3_VERDICT_BASE} --out-dir $GATED
rc=$?
say "verdict rc=$rc"
exit $rc
