#!/usr/bin/env bash
# Build AeonG (temporal graph DB on Memgraph v2.3 + RocksDB history) from its
export EXTCMP_ENGINE_GUARD=${EXTCMP_ENGINE_GUARD:-0}   # the AeonG build does not load the engine (lib/engine_guard.sh)
# pinned source, inside its authors' own toolchain image, with the three
# dependency patches its repository lacks (taken from memgraph v2.3.1, whose
# dependency pins AeonG's match; checked by SHA256SUMS) and its CMakeLists
# forced to a Release build (upstream line 2 forces Debug).
set -euo pipefail
source "$(dirname "$0")/lib/env.sh"
AEONG_COMMIT=${AEONG_COMMIT:-b780a0c33ae1c156bf4f1a2b2137dd8d16041d75}
mkdir -p "$AEONG_SRC"
( cd "$KIT/aeong" && sha256sum -c SHA256SUMS )
cp "$KIT/aeong/"*.patch "$AEONG_SRC/"
docker run --rm -v "$AEONG_SRC":/work "$(img aeong-toolchain)" bash -c "
set -ex
source /opt/toolchain-v4/activate
git config --global --add safe.directory '*'
cd /work
[ -d AeonG ] || git clone https://github.com/hououou/AeonG.git
cd AeonG && git checkout -q $AEONG_COMMIT && git log -1 --format=%H
cp /work/antlr4.patch /work/librdtsc.patch /work/rocksdb.patch libs/
sed -i '2s/SET(CMAKE_BUILD_TYPE \"Debug\")/SET(CMAKE_BUILD_TYPE \"Release\")/' CMakeLists.txt
sed -n 2p CMakeLists.txt
./init
mkdir -p build-release && cd build-release
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j\$(nproc) memgraph
ls -la memgraph-_Release
chown -R $(id -u):$(id -g) /work
"
test -x "$AEONG_SRC/AeonG/build-release/memgraph-_Release"
echo "AeonG built: $AEONG_SRC/AeonG/build-release/memgraph-_Release"
