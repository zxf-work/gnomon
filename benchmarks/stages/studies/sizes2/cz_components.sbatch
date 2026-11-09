#!/bin/bash
# (tools/cz_components.py parses the segment files; the engine is not opened). a shared node, small.
#SBATCH -N 1
#SBATCH --ntasks-per-node=4
#SBATCH -J cz-components
source ${NEXT_STAGE_ROOT:?}/src/benchmarks/stages/studies/lib.sh
guard; hostinfo
E=${GNOMON_BENCH_ROOT}
O=$NEXT_STAGE_ROOT/results/${CZ_SMOKE:+smoke-}components-$SLURM_JOB_ID; mkdir -p $O
L=${LOCAL:?}/comp; mkdir -p $L
for wl in w1 w2; do
  A=$E/confirm/$wl/stores/K10M/native-K10M.tar.zst
  restore_tar $A $L/$wl
  echo "provenance $A sha256 $(cut -c1-16 $A.sha256) load record: $($PYV -c "import json,sys; r=json.load(open(sys.argv[1])); m=r.get('manifest',{}); print(m.get('commit'), m.get('hostname'), r.get('host_after',{}).get('utc'))" $(dirname $A)/load-native.json)"
  $PYV $REPO/benchmarks/stages/studies/sizes2/cz_components.py $L/$wl/native-K10M $O/components-$wl-K10M.json --md $O/components-$wl-K10M.md || echo "COMPONENTS FAILED $wl"
  du -sb $L/$wl/native-K10M | tee -a $O/du.txt
  rm -rf $L/$wl
done
say "components done"
