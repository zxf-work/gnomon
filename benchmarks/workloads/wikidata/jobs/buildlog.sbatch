#!/bin/bash
#SBATCH -J w5-log
#SBATCH -N 1
#SBATCH --ntasks-per-node=16
#SBATCH -t 06:00:00
#SBATCH -o buildlog-%j.out
# usage: sbatch buildlog.sbatch <name> <tgb-edgelist>   then replays the log into a native store as a check
set -uo pipefail
P=${GNOMON_BENCH_ROOT}/w5-wikidata
source $P/site.env; cd $EXTCMP_REPO; source .venv/bin/activate
N=$1; TGB=$2
/usr/bin/time -v python scripts/wd_build_log.py --name $N --stagea $P/stageA/$N --subset $P/subset/$N.q --tgb $TGB --out $P/eventlogs --threads 16 2> $P/logs/buildlog-$N.time | tail -150
grep -E "Maximum resident|Elapsed" $P/logs/buildlog-$N.time; tail -3 $P/logs/buildlog-$N.time
L=$P/eventlogs/W5-$N.eventlog.jsonl; ls -l $L*
st=${LOCAL:-${TMPDIR:-/tmp}}/st-$N; rm -rf $st
/usr/bin/time -v python -c "
import sys, pathlib, time; sys.path.insert(0, 'scripts')
import eval_harness as H, gnomon
t=time.time(); H.load_store(pathlib.Path('$st'), 'native', pathlib.Path('$L')); print('replay_s', round(time.time()-t,1))
s=gnomon.open('$st', backend='native', read_only=True); print(s.stats()); s.close()
" 2> $P/logs/replay-$N.time; grep -E "Maximum resident|Elapsed|Error|error" $P/logs/replay-$N.time | head
