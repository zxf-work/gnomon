#!/bin/bash
#SBATCH -J w3-datagen
#SBATCH -N 1
#SBATCH --ntasks-per-node=64
#SBATCH -t 04:00:00
#SBATCH -o datagen-%j.out
# W3 (prep-w3-snb): LDBC SNB Datagen 0.5.1, image
# ldbc/datagen-standalone:0.5.1-2.12_spark3.2 pinned by digest. SF1 and SF10,
# raw mode (+factor tables) and BI mode (composite-merged-fk, epoch millis).
set -euo pipefail
W=${GNOMON_BENCH_ROOT}/w3-snb
IMG=docker://ldbc/datagen-standalone@sha256:21bcfc0d8519bb98ac9ba2827494a07b8ab0dc753b9d4b1bafa1ebc7f611b3d1
SIF=$W/ext/datagen-0.5.1.sif
export APPTAINER_TMPDIR=${LOCAL:-${TMPDIR:-/tmp}}/apt-tmp APPTAINER_CACHEDIR=${LOCAL:-${TMPDIR:-/tmp}}/apt-cache TZ=UTC
mkdir -p $APPTAINER_TMPDIR $APPTAINER_CACHEDIR ${LOCAL:-${TMPDIR:-/tmp}}/spark $W/data
[ -s $SIF ] || apptainer pull $SIF $IMG
sha256sum $SIF > $SIF.sha256
run() {  # name, sf, mode-args...
  local name=$1 sf=$2; shift 2
  if [ -e $W/data/$name/.done ]; then echo "skip $name"; return; fi
  rm -rf ${LOCAL:-${TMPDIR:-/tmp}}/out/$name; mkdir -p ${LOCAL:-${TMPDIR:-/tmp}}/out
  local t0=$(date +%s)
  apptainer exec --cleanenv --env TZ=UTC,SPARK_LOCAL_DIRS=/sparktmp,TEMP_DIR=/sparktmp \
    --bind ${LOCAL:-${TMPDIR:-/tmp}}/spark:/sparktmp --bind ${LOCAL:-${TMPDIR:-/tmp}}/out:/out $SIF \
    run.py --cores 64 --memory 110G --parallelism 64 -- \
    --format csv --scale-factor $sf --output-dir /out/$name "$@"
  echo "$name wall_s=$(( $(date +%s) - t0 ))"
  rm -rf $W/data/$name; cp -a ${LOCAL:-${TMPDIR:-/tmp}}/out/$name $W/data/$name
  (cd $W/data/$name && find . -type f ! -name SHA256SUMS ! -name .done | sort | xargs sha256sum > SHA256SUMS)
  touch $W/data/$name/.done
  rm -rf ${LOCAL:-${TMPDIR:-/tmp}}/spark/*
}
run sf1-raw   1  --mode raw --generate-factors --epoch-millis
run sf1-bi    1  --mode bi --epoch-millis --format-options compression=gzip
run sf10-raw  10 --mode raw --generate-factors --epoch-millis
run sf10-bi   10 --mode bi --epoch-millis --format-options compression=gzip
du -sh $W/data/*
