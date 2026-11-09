#!/bin/bash
# The AeonG image (apptainer/aeong.def: AeonG compiled inside its authors'
# image), built in a batch job, never on a login node. The build runs under
# node-local ${LOCAL:-${TMPDIR:-/tmp}}; the finished .sif is copied to $SIF_DIR on $PROJECT.
#   sbatch -A <account> --export=ALL,EXTCMP_PROJECT=$GNOMON_BENCH_ROOT kit/slurm/build_aeong.sbatch
#SBATCH -N 1
#SBATCH --ntasks-per-node=16
#SBATCH -t 04:00:00
#SBATCH -J build-aeong
#SBATCH -o extcmp-%x-%j.out
set -euo pipefail
export EXTCMP_PROJECT=${EXTCMP_PROJECT:-${PROJECT:?}/extcmp}
source "$EXTCMP_PROJECT/site.env"
: "${LOCAL:?no node-local \${LOCAL:-${TMPDIR:-/tmp}}}"
export APPTAINER_TMPDIR=${LOCAL:-${TMPDIR:-/tmp}}/apptainer-tmp APPTAINER_CACHEDIR=${LOCAL:-${TMPDIR:-/tmp}}/apptainer-cache
mkdir -p "$APPTAINER_TMPDIR" "$APPTAINER_CACHEDIR"
echo "=== AeonG image build on $(hostname), job $SLURM_JOB_ID, $(date -u +%FT%TZ)"
cd "$KIT"
apptainer build --fakeroot "${LOCAL:-${TMPDIR:-/tmp}}/aeong.sif" apptainer/aeong.def
cp "${LOCAL:-${TMPDIR:-/tmp}}/aeong.sif" "$SIF_DIR/aeong.sif.part"
mv "$SIF_DIR/aeong.sif.part" "$SIF_DIR/aeong.sif"
echo "aeong $(sha256sum "$SIF_DIR/aeong.sif" | cut -d' ' -f1) job $SLURM_JOB_ID $(date -u +%FT%TZ)" >> "$SIF_DIR/images.txt"
apptainer exec "$SIF_DIR/aeong.sif" ls -la /opt/aeong/memgraph-_Release
echo "=== done $(date -u +%FT%TZ)"
