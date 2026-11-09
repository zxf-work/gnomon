#!/bin/bash
#SBATCH -J w4-datagen
#SBATCH -N 1
#SBATCH --ntasks-per-node=64
#SBATCH -t 05:00:00
#SBATCH -o w4-datagen-%j.out
# W4: build LDBC FinBench datagen 0.1.0, generate SF1 and SF10 raw
# data + factor tables (Spark 3.2.2 local mode), transform to snapshot +
# incremental writes (the datagen's own convert_data.py, duckdb 0.7.1; W4_TRANSFORM=0 skips it:
# it runs out of memory at SF10, and w4_trw_params.py derives the TRW streams from raw).
# No --generate-factors: the factor stage fails under the Spark 3.2.2 runtime ("Multiple sources
# found for csv") and at SF10 its failure cut the raw writes short; the W4
# curation does not use the factor tables.
set -uo pipefail
W=${GNOMON_BENCH_ROOT}/w4-finbench
D=$W/downloads; G=$D/ldbc_finbench_datagen
say() { echo "=== $* $(date -u +%FT%TZ)"; }
export JAVA_HOME=/usr/lib/jvm/java-1.8.0; [ -d $JAVA_HOME ] || unset JAVA_HOME
cd $G; git rev-parse HEAD
JAR=$G/target/ldbc_finbench_datagen-0.1.0-jar-with-dependencies.jar
if [ ! -e $JAR ]; then
  say mvn build
  mvn -q -B -Dmaven.repo.local=$W/m2 clean package -DskipTests 2>&1 | tail -20
  [ -e $JAR ] || { echo "BUILD FAILED"; exit 1; }
fi
sha256sum $JAR
SP=$W/spark-3.2.2-bin-hadoop3.2
[ -d $SP ] || tar -xzf $D/spark-3.2.2-bin-hadoop3.2.tgz -C $W
export PATH=$SP/bin:$PATH SPARK_LOCAL_DIRS=${LOCAL:-${TMPDIR:-/tmp}}/spark
TV=$W/tvenv
if [ ! -x $TV/bin/python ]; then
  say transform venv
  ${UV:-uv} venv -q -p 3.8 $TV && VIRTUAL_ENV=$TV ${UV:-uv} pip install -q duckdb==0.7.1 pandas pytz backports.zoneinfo || exit 1
fi
for sf in ${W4_DATAGEN_SFS:-1 10}; do
  O=$W/data/sf$sf
  if [ ! -e $O/.generated ]; then
    say generate SF$sf
    rm -rf $O; mkdir -p $O
    /usr/bin/time -v python3 scripts/run.py --jar $JAR --main-class ldbc.finbench.datagen.LdbcDatagen --cores ${W4_SPARK_CORES:-64} --memory ${W4_SPARK_MEM:-110g} \
      -- --scale-factor $sf --output-dir $O 2> $W/logs/datagen-sf$sf.time | tail -5
    # every entity directory must be complete (_SUCCESS); a partial write once passed as done
    n=$(ls $O/raw | wc -l); ok=$(ls $O/raw/*/_SUCCESS 2>/dev/null | wc -l)
    [ "$n" -ge 19 ] && [ "$ok" = "$n" ] || { echo "GENERATE FAILED SF$sf ($ok of $n complete)"; exit 1; }
    touch $O/.generated
  fi
  if [ "${W4_TRANSFORM:-1}" = 1 ] && [ ! -e $O/.transformed ]; then
    say transform SF$sf
    T=${LOCAL:-${TMPDIR:-/tmp}}/transform-sf$sf; rm -rf $T; cp -r $G/transformation $T; cd $T
    /usr/bin/time -v $TV/bin/python convert_data.py --raw_dir $O --output_dir $O > $W/logs/transform-sf$sf.log 2> $W/logs/transform-sf$sf.time || { echo "TRANSFORM FAILED SF$sf"; tail -20 $W/logs/transform-sf$sf.time; exit 1; }
    cd $G; touch $O/.transformed
  fi
  du -sh $O/raw $O/snapshot $O/incremental $O/factor_table 2>/dev/null
  ( cd $O && find raw snapshot incremental -type f -name "*.csv" | sort | xargs sha256sum > SHA256SUMS )
  wc -l $O/SHA256SUMS
done
say done
