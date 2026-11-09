#!/bin/bash
# W3 stage smoke (not a result): the stage's scripts end to end on the SF0.1 stores (DuckDB, PostgreSQL, Neo4j; no Gnomon
# store exists for SF0.1 in this form), a shared node: gate -> verdict (base duckdb) -> timed job body (2 rotations, S block,
# server start/stop, exclusions, engine pass, censor-once audit). Writes smoke-* result dirs and gate/*SF0.1* only.
#SBATCH -N 1
#SBATCH --ntasks-per-node=16
#SBATCH -t 00:50:00
#SBATCH -J w3-smoke
#SBATCH -o %x-%j.out
source ${W3_STAGE_ROOT:-${GNOMON_BENCH_ROOT}/final/w3}/src/benchmarks/stages/w3/jobs/w3_lib.sh
unset EXTCMP_NUMA_CLIENT EXTCMP_NUMA_SERVER
guard; hostinfo
export W3_SMOKE=1 W3_VERIFY_WINDOW_S=240
G=$W3/results/smoke-gate-$SLURM_JOB_ID; T=$W3/results/smoke-timed-$SLURM_JOB_ID; mkdir -p $G/SF0.1 $T
if [ -z "${W3_SMOKE_TIMED_ONLY:-}" ]; then
say "smoke gate"
OUT=$G W3_GATE_POINT=SF0.1 W3_GATE_SYS="duckdb postgres neo4j" bash $REPO/$STG/jobs/w3_gate_main.sh; echo "gate main rc=$?"
say "smoke verdict (base duckdb)"
$PYV $TOOLS/w3_gate.py --point SF0.1 --res $G --require duckdb,postgres,neo4j --base duckdb --out-dir $W3/gate; echo "verdict rc=$?"
fi
say "smoke timed"
OUT=$T W3_POINT=SF0.1 bash $REPO/$STG/jobs/w3_timed_main.sh; echo "timed main rc=$?"
ls -R $T | head -40
say "smoke done"
