#!/usr/bin/env bash
# only place with documented outbound internet). Compute jobs then run fully
# offline from $EXTCMP_PROJECT (on $PROJECT, Ocean).
#
#   EXTCMP_ENGINE_COMMIT=<engine-upgrade tip> \
#       [--logs-from <dir or host:dir with frozen *.eventlog.jsonl>] [--steps rust,uv,repo,vendor,images,aeong,logs,env]
#
# rust   : rustup + the toolchain into $EXTCMP_PROJECT/{rustup,cargo} (RUSTUP_HOME, CARGO_HOME)
# uv     : uv into $EXTCMP_PROJECT/bin; Python and the uv cache under $EXTCMP_PROJECT
# repo   : clone at the engine commit; overlay the comparison layer (extcmp*.py,
#          *_baseline.py) from the kit's commit; `uv sync --extra eval --extra duckdb`;
#          python.lock (LadybugDB, Raphtory, maturin). The engine itself is
#          built in a compute job (slurm: stage `build`) from vendored crates.
# vendor : `cargo vendor` into $EXTCMP_PROJECT/vendor and an offline
#          .cargo/config.toml in the repository
# images : `apptainer pull` of every image in images.lock, by digest, into
#          $SIF_DIR/<key>.sif; sha256 of each .sif in $SIF_DIR/images.txt
# aeong  : submit slurm/build_aeong.sbatch (the AeonG image is compiled in a job,
# logs   : copy the frozen event logs (and their .meta.json) into
#          $EXTCMP_PROJECT/campaign/eventlogs (sizes not copied are frozen in
#          the jobs, deterministically from the size)
set -euo pipefail
KIT=$(cd "$(dirname "$0")" && pwd)
P=${EXTCMP_PROJECT:-${PROJECT:?no \$PROJECT}/extcmp}; REPO_SRC=""; LOGS_FROM=""
STEPS="rust,uv,repo,vendor,images,aeong,logs,env"
COMMIT=${EXTCMP_ENGINE_COMMIT:-}
while [ $# -gt 0 ]; do case $1 in
  --repo) REPO_SRC=$2; shift 2 ;; --project) P=$2; shift 2 ;; --logs-from) LOGS_FROM=$2; shift 2 ;;
  --steps) STEPS=$2; shift 2 ;; --commit) COMMIT=$2; shift 2 ;;
  *) echo "unknown argument $1"; exit 2 ;; esac; done
[ -n "$COMMIT" ] || { echo "set EXTCMP_ENGINE_COMMIT (or --commit)"; exit 2; }
has() { case ",$STEPS," in *",$1,"*) return 0 ;; esac; return 1; }
say() { echo "=== $* $(date -u +%FT%TZ)"; }
mkdir -p "$P"/{bin,campaign/results,campaign/eventlogs,sif}
export RUSTUP_HOME=$P/rustup CARGO_HOME=$P/cargo UV_CACHE_DIR=$P/uv-cache \
       UV_PYTHON_INSTALL_DIR=$P/python PATH=$P/bin:$P/cargo/bin:$PATH
SIF_DIR=$P/sif; REPO=$P/repo
KIT_REPO=$(git -C "$KIT" rev-parse --show-toplevel); KIT_COMMIT=$(git -C "$KIT" rev-parse HEAD)
REPO_SRC=${REPO_SRC:-$KIT_REPO}

if has rust; then
  say "rust toolchain into $RUSTUP_HOME"
  [ -x "$CARGO_HOME/bin/rustup" ] || curl -sSf https://sh.rustup.rs | sh -s -- -y --no-modify-path --profile minimal
  tc=$(sed -n 's/^channel *= *"\(.*\)"/\1/p' "$KIT_REPO/rust-toolchain.toml" 2>/dev/null || true)
  rustup toolchain install "${tc:-stable}" --profile minimal
  rustup default "${tc:-stable}"
fi
if has uv; then
  say "uv into $P/bin"
  [ -x "$P/bin/uv" ] || curl -LsSf https://astral.sh/uv/install.sh | env UV_INSTALL_DIR="$P/bin" INSTALLER_NO_MODIFY_PATH=1 sh
fi
if has repo; then
  say "repository at $COMMIT (comparison layer from $KIT_COMMIT)"
  [ -d "$REPO/.git" ] || git clone -q --no-checkout "$REPO_SRC" "$REPO"
  git -C "$REPO" fetch -q "$KIT_REPO" "$KIT_COMMIT" 2>/dev/null || true
  git -C "$REPO" checkout -q --detach "$COMMIT"
  OVERLAY=$(git -C "$REPO" diff --name-only "$COMMIT" "$KIT_COMMIT" -- scripts |
            grep -E '^scripts/(extcmp[A-Za-z0-9_]*|[a-z0-9]+_baseline)\.py$' || true)
  [ -n "$OVERLAY" ] && git -C "$REPO" checkout -q "$KIT_COMMIT" -- $OVERLAY
  # dependencies only: the engine itself is compiled in the Slurm build job,
  # never on the login node
  ( cd "$REPO" && unset CARGO_TARGET_DIR && uv sync --extra eval --extra duckdb --no-install-project \
      && uv pip install -r "$KIT/python.lock" )
  printf '{"engine_commit": "%s", "kit_commit": "%s", "overlay": "%s", "repo_source": "%s", "staged_utc": "%s"}\n' \
    "$(git -C "$REPO" rev-parse HEAD)" "$KIT_COMMIT" "$(echo $OVERLAY)" "$REPO_SRC" "$(date -u +%FT%TZ)" \
    > "$P/campaign/results/provenance-staging.json"
fi
if has repo; then
  # LadybugDB 0.21.2's native module links OpenSSL 3; the host (glibc 2.28)
  # has 1.1 only. Take libssl.so.3/libcrypto.so.3 (built for glibc <= 2.17)
  # from the site's Anaconda module into $P/lib
  mkdir -p "$P/lib"
  for l in libssl.so.3 libcrypto.so.3; do
    [ -e "$P/lib/$l" ] || cp "${EXTCMP_OPENSSL3_DIR:-/opt/packages/anaconda3-2024.10-1/lib}/$l" "$P/lib/"
  done
fi
if has vendor; then
  say "cargo vendor (offline engine build in the compute job)"
  mkdir -p "$REPO/.cargo"
  ( cd "$REPO" && cargo vendor --locked "$P/vendor" ) > "$REPO/.cargo/config.toml"
  printf '\n[net]\noffline = true\n' >> "$REPO/.cargo/config.toml"
fi
if has images; then
  say "images by digest into $SIF_DIR"
  : > "$SIF_DIR/images.txt"
  while read -r key ref; do
    case $key in ''|\#*) continue ;; esac
    [ "$key" = aeong-toolchain ] && continue          # aeong.sif is built from it below
    # apptainer wants repo@digest (a tag next to the digest is refused)
    pin=$(echo "$ref" | sed -E "s#:[^@/]+@#@#")
    [ -e "$SIF_DIR/$key.sif" ] || apptainer pull "$SIF_DIR/$key.sif" "docker://$pin"
    echo "$key $ref $(sha256sum "$SIF_DIR/$key.sif" | cut -d' ' -f1)" >> "$SIF_DIR/images.txt"
  done < "$KIT/images.lock"
fi
if has aeong; then
  # compiling is never done on a login node: the AeonG image is
  # built by a batch job on a shared node, under ${LOCAL:-${TMPDIR:-/tmp}}, and copied to $SIF_DIR
  say "AeonG image: submitting the build job (slurm/build_aeong.sbatch)"
  if [ -e "$SIF_DIR/aeong.sif" ]; then echo "aeong.sif exists"
  else sbatch -A "${EXTCMP_ACCOUNT:?set EXTCMP_ACCOUNT}" --export=ALL,EXTCMP_PROJECT="$P" "$KIT/slurm/build_aeong.sbatch"; fi
fi
if has logs && [ -n "$LOGS_FROM" ]; then
  say "frozen event logs from $LOGS_FROM"
  # local directory only; frozen logs are not
  # regenerable byte for byte (wall-clock tt), so they are copied, and checked
  # against SHA256SUMS when one is present
  cp -a "$LOGS_FROM"/synth-*.eventlog.jsonl "$LOGS_FROM"/synth-*.eventlog.meta.json "$P/campaign/eventlogs/"
  [ -e "$LOGS_FROM/SHA256SUMS" ] && ( cd "$P/campaign/eventlogs" && sha256sum -c "$LOGS_FROM/SHA256SUMS" )
  ls -la "$P/campaign/eventlogs"
fi
if has env; then
  say "writing $P/site.env"
  cat > "$P/site.env" <<EOF
# written by stage_hpc.sh $(date -u +%FT%TZ); sourced by kit/slurm/job.sbatch
export EXTCMP_PROJECT=$P
export EXTCMP_ENGINE_COMMIT=$COMMIT
export EXTCMP_BACKEND=apptainer
export SIF_DIR=$SIF_DIR
export EXTCMP_REPO=$REPO
export RUSTUP_HOME=$P/rustup CARGO_HOME=$P/cargo UV_CACHE_DIR=$P/uv-cache UV_PYTHON_INSTALL_DIR=$P/python
export PATH=$P/bin:$P/cargo/bin:\$PATH
export KIT=$KIT
export LD_LIBRARY_PATH=$P/lib\${LD_LIBRARY_PATH:+:\$LD_LIBRARY_PATH}
# NUMA (RM node: 2 sockets / 2 NUMA nodes): timed client processes on node 0
# with their memory bound there; servers on node 1, memory preferred there
# (a 100M in-memory server may spill to node 0 rather than fail)
export EXTCMP_NUMA_CLIENT="numactl --cpunodebind=0 --membind=0"
export EXTCMP_NUMA_SERVER="numactl --cpunodebind=1 --preferred=1"
EOF
  cat "$P/site.env"
fi
say "staging done; next: sbatch the build job (kit/slurm/submit_campaign.sh --only build)"
