#!/bin/bash
#SBATCH -J w5-extract
#SBATCH -N 1
#SBATCH --ntasks-per-node=32
#SBATCH -t 08:00:00
#SBATCH -o extract-%j.out
# usage: sbatch extract.sbatch <subset-name> <minpage> <maxpage> [parallel]
set -uo pipefail
P=${GNOMON_BENCH_ROOT}/w5-wikidata
NAME=$1; LO=$2; HI=$3; PAR=${4:-30}
module load anaconda3/2024.10-1 >/dev/null 2>&1
X=$P/src/scripts/wd_extract.py; S=$P/subset/$NAME.q; O=$P/stageA/$NAME; mkdir -p $O
cd $P/raw/wd20190101
ls *.7z | python3 -I -c "
import sys,re
for n in sys.stdin:
    n=n.strip(); m=re.search(r'p(\d+)p(\d+)\.7z$',n)
    if m and $LO<=int(m.group(1))<=$HI: print(m.group(2) if False else n)" > $O/files.txt
echo "files: $(wc -l < $O/files.txt)  $(date -u +%FT%TZ)"
# smoke: first 300 MB of the first file
f=$(head -1 $O/files.txt); 7za x -so $f 2>/dev/null | head -c 300000000 | python3 -I $X --subset $S --out $O/smoke.jsonl.gz; [ -s $O/smoke.jsonl.gz ] || { echo SMOKE FAIL; exit 1; }
zcat $O/smoke.jsonl.gz | head -c 600; echo; rm -f $O/smoke.jsonl.gz
one() { f=$1; b=${f%.7z}; [ -s $O/$b.jsonl.gz ] && { echo "have $b"; return; }
  s=$(date +%s); r=$( (7za x -so $f 2>/dev/null | python3 -I $X --subset $S --out $O/$b.jsonl.gz) 2>&1 | tail -1)
  echo "$b $(( $(date +%s)-s ))s $r"; }
export -f one; export O X S
xargs -P $PAR -I{} bash -c 'one {}' < $O/files.txt
echo "done $(date -u +%FT%TZ)"; du -sh $O; grep -c '"wall_s"' $P/logs/extract-$SLURM_JOB_ID.out
