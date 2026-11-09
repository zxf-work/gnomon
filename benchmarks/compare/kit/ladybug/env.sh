source ${GNOMON_BENCH_ROOT}/kit/site.env
export LB=${GNOMON_BENCH_ROOT}/ladybug
export REPO=$LB/src EXTCMP_REPO=$LB/src KIT=$LB/src/benchmarks/compare/kit
export PYV=$REPO/.venv/bin/python PYTHONPATH=$REPO:$REPO/scripts
export P3=${GNOMON_BENCH_ROOT}/w3-snb W3_EXT=${GNOMON_BENCH_ROOT}/w3-snb/ext
export W4=${GNOMON_BENCH_ROOT}/w4-finbench
unset EXTCMP_NUMA_SERVER EXTCMP_NUMA_CLIENT
