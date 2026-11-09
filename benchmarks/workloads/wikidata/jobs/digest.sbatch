#!/bin/bash
#SBATCH -J w5-digest
#SBATCH --ntasks-per-node=8
#SBATCH -t 02:00:00
#SBATCH -o digest-%j.out
# usage: sbatch digest.sbatch <tag>   replay the frozen log into a fresh native store, print engine + store digest
P=${GNOMON_BENCH_ROOT}/w5-wikidata
source $P/site.env; cd $P/src; source .venv/bin/activate
L=$P/eventlogs/$1.eventlog.jsonl; st=${LOCAL:-${TMPDIR:-/tmp}}/st-$1; rm -rf $st
python - <<PY
import sys, time, hashlib, pathlib, subprocess
sys.path.insert(0, "scripts")
import eval_harness as H, gnomon, gnomon._engine as e
so = e.__file__
print("engine_commit", subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip())
print("engine_so", so, hashlib.sha256(open(so, "rb").read()).hexdigest())
t = time.time(); H.load_store(pathlib.Path("$st"), "native", pathlib.Path("$L")); print("replay_s", round(time.time() - t, 1))
s = gnomon.open("$st", backend="native", read_only=True)
st = s.stats(); print("stats", {k: st[k] for k in ("n_entities", "n_node_versions", "n_edge_versions")})
t = time.time()
d = s.digest_streaming() if hasattr(s, "digest_streaming") else s.digest()
print("store_digest", d, "digest_s", round(time.time() - t, 1)); s.close()
PY
