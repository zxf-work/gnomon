#!/usr/bin/env bash
# Build the engine on a compute node, offline: the crates are vendored
# and the Python environment is already
# synced. Installs the release build into the staged venv and records it.
set -euo pipefail
source "${EXTCMP_PROJECT:?}/site.env"
cd "$EXTCMP_REPO"
unset CARGO_TARGET_DIR
export CARGO_NET_OFFLINE=true
source .venv/bin/activate
maturin develop --release --extras eval,duckdb 2>&1 | tail -20
.venv/bin/python -c "import gnomon, gnomon._engine as e, ladybug, raphtory; print(e.__file__, ladybug.__version__, raphtory.__version__)" \
  | tee "$EXTCMP_PROJECT/campaign/results/engine-build.txt"
git rev-parse HEAD >> "$EXTCMP_PROJECT/campaign/results/engine-build.txt"
