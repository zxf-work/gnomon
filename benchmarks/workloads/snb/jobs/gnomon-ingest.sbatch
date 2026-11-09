#!/bin/bash
#SBATCH -J w3-ingest-study
#SBATCH --exclusive
#SBATCH -N 1
#SBATCH --ntasks-per-node=128
#SBATCH -t 02:00:00
#SBATCH -o gnomon-ingest-%j.out
# W3 ingest study, Gnomon: the SF1 Q log replayed per batch, (1) the prefix "first 7 days +
# their corrections" (the identical prefix every system replays) and (2) the full stream. Per-batch profile by kind.
# ENGINE: the repo whose gnomon engine is timed (the final engine; never the preview for a timed record).
set -uo pipefail
P=${GNOMON_BENCH_ROOT}/w3-snb; source $P/site.env
ENGINE=${ENGINE:?set ENGINE to the final engine checkout}; cd $ENGINE; source .venv/bin/activate
SF=${SF:-1}; R=${R:-1}; LOG=$P/eventlogs/W3-SF$SF-h1-r$R.eventlog.jsonl; META=$P/base/sf$SF-h1/meta.json
OUT=$P/w3stores/SF$SF-h1-r$R/study-gnomon; mkdir -p $OUT
echo "engine $(git -C $ENGINE rev-parse HEAD) $(sha256sum gnomon/_engine*.so | cut -c1-16)"
L=$(python -c "import json; d=json.load(open('$META'))['days']; print(d[6]['day_end_us'] + 1000000)")
python -I - $LOG ${LOCAL:-${TMPDIR:-/tmp}}/prefix.eventlog.jsonl $L <<'PY'
import json, sys
L = int(sys.argv[3]); n = 0
with open(sys.argv[1]) as f, open(sys.argv[2], "w") as g:
    g.write(f.readline())
    for line in f:
        if json.loads(line)["tt"] <= L:
            g.write(line); n += 1
print("prefix records", n)
PY
# the profiler imports gnomon from its parent directory: run it from a scratch root whose gnomon is the engine's
mkdir -p ${LOCAL:-${TMPDIR:-/tmp}}/rc/scripts; cp $P/src/scripts/w3_replay_check.py ${LOCAL:-${TMPDIR:-/tmp}}/rc/scripts/; ln -sfn $ENGINE/gnomon ${LOCAL:-${TMPDIR:-/tmp}}/rc/gnomon
for which in prefix full; do
  log=$LOG; [ $which = prefix ] && log=${LOCAL:-${TMPDIR:-/tmp}}/prefix.eventlog.jsonl
  st=${LOCAL:-${TMPDIR:-/tmp}}/store-$which; rm -rf $st
  t0=$(date +%s.%N)
  python ${LOCAL:-${TMPDIR:-/tmp}}/rc/scripts/w3_replay_check.py --log $log --store $st | cut -c1-400
  echo "$which replay_s $(python -c "import time; print(round(time.time() - $t0, 1))")"
  cp $st.profile.jsonl $OUT/$which.profile.jsonl
done
