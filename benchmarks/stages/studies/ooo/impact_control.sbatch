#!/bin/bash
#SBATCH -N 1
#SBATCH --ntasks-per-node=8
#SBATCH -J c4-ooo-impact-control
ST=${NEXT_STAGE_ROOT:?}; O=$ST/impact-control/$SLURM_JOB_ID; mkdir -p $O; PYV=$ST/src/.venv/bin/python
export PATH=${GNOMON_BENCH_ROOT}/bin:$PATH
zstd -q -d -c $ST/logs-reordered/J52/K10Mfull-d0.eventlog.jsonl.zst > ${LOCAL:-${TMPDIR:-/tmp}}/full.jsonl
$PYV $ST/src/benchmarks/stages/studies/ooo/ooo_impact.py ${LOCAL:-${TMPDIR:-/tmp}}/full.jsonl $O/first-form.json --edges $O/first-form-edges.jsonl.gz | cut -c1-500
$PYV $ST/impact2-tool/ooo_impact2.py ${LOCAL:-${TMPDIR:-/tmp}}/full.jsonl $O/second-form.json | cut -c1-900
