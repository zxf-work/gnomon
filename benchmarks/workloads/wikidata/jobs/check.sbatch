#!/bin/bash
#SBATCH -J w5-check
#SBATCH -N 1
#SBATCH --ntasks-per-node=64
#SBATCH -t 08:00:00
#SBATCH -o check-%j.out
# W5 answer check on a shared node (the kit's job_cmd.sbatch is --exclusive whole-node; this is its
# minimal shared-node form): node-local EXTCMP_ROOT, frozen logs copied in, records synced back.
#   W5_TAG=W5-smallpedia W5_SIZE=200000 sbatch --export=ALL check.sbatch
set -uo pipefail
export EXTCMP_PROJECT=${GNOMON_BENCH_ROOT}/w5-wikidata
source $EXTCMP_PROJECT/site.env
P=$EXTCMP_PROJECT/campaign; R=$EXTCMP_PROJECT/runs/$W5_TAG-$SLURM_JOB_ID; mkdir -p $R  # fresh records per job: a copied-in load record would make the kit skip the (node-local) load
export EXTCMP_ROOT=${LOCAL:-${TMPDIR:-/tmp}}/extcmp TMPDIR_OVERRIDE=${LOCAL:-${TMPDIR:-/tmp}}/extcmp/scratch
unset EXTCMP_GATE_DIR
mkdir -p $EXTCMP_ROOT/{results,eventlogs,stores,srv,scratch}
cp $P/eventlogs/$W5_TAG.eventlog.jsonl $EXTCMP_ROOT/eventlogs/
export EXTCMP_SYNC_HOOK="mkdir -p $R && cp -au $EXTCMP_ROOT/results/. $R/"
trap 'cp -au $EXTCMP_ROOT/results/. $R/; $KIT/lib/servers.sh stopall' EXIT
echo "=== job $SLURM_JOB_ID on $(hostname) $(date -u +%FT%TZ) tag $W5_TAG engine $(git -C $EXTCMP_REPO rev-parse HEAD)"
bash $EXTCMP_PROJECT/jobs/w5_check.sh
du -sh $EXTCMP_ROOT/stores/* $EXTCMP_ROOT/srv/* 2>/dev/null
