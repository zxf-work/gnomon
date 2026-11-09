#!/bin/bash
#   The steps of Store._write_locked at the log's own transaction time: log append with fsync, apply, commit, compaction by the engine's trigger.
#   The tool's kinds are a W3 notion; a meta file with no days and an upper bound as snapshot end puts every batch under one kind. Each run on a fresh store on ${LOCAL:-${TMPDIR:-/tmp}}.
#   After the timed runs, side by side: the store's digest against the replayed C4 store of the same log. WR_RUNS "wl:tag ..."; WR_SMOKE=1 labels results smoke-*.
# Exit codes: 2 missing input, 3 guard, 5 time limit, 7 failed verification (refused batch, log differs, digest differs), 8 failed run.
#SBATCH -N 1
#SBATCH -J c4-writer
source ${NEXT_STAGE_ROOT:?}/src/benchmarks/stages/studies/lib.sh
source $REPO/$STG/body.sh
guard; hostinfo
O=$NEXT_STAGE_ROOT/results/${WR_SMOKE:+smoke-}writer-$SLURM_JOB_ID
run_body $REPO/$STG/writer/jobs/writer_main.sh $O "cp -au ${LOCAL:?}/wrres/. $O/"
