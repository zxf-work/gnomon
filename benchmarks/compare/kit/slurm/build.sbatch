#!/bin/bash
# The engine build: maturin --release into the staged venv, on part of a
# shared node (compiling is never done on the login node). Submitted first by
# submit_campaign.sh, or alone:  sbatch -A <account> kit/slurm/build.sbatch
#SBATCH -N 1
#SBATCH --ntasks-per-node=32
#SBATCH -t 02:00:00
#SBATCH -J build
#SBATCH -o extcmp-%x-%j.out
set -uo pipefail
export EXTCMP_PROJECT=${EXTCMP_PROJECT:-${PROJECT:?}/extcmp}
source "$EXTCMP_PROJECT/site.env"
P=$EXTCMP_PROJECT/campaign; mkdir -p "$P/done"
[ -e "$P/done/build" ] && { echo "build already done ($(cat "$P/done/build"))"; exit 0; }
export CARGO_BUILD_JOBS=32
"$KIT/slurm/build_engine.sh" && echo "job $SLURM_JOB_ID $(date -u +%FT%TZ)" > "$P/done/build"
