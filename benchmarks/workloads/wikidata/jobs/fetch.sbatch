#!/bin/bash
#SBATCH -n 4
#SBATCH -t 08:00:00
#SBATCH -J w5-fetch
#SBATCH -o fetch-%j.out
# usage: sbatch fetch.sbatch <minpage> <maxpage>   -- downloads 20190101 history 7z files whose start page is in [min,max], md5-checked
set -uo pipefail
T=${GNOMON_BENCH_ROOT}/w5-wikidata
D=$T/raw/wd20190101; cd $D
python3 -I - "$1" "$2" > $T/raw/fetchlist-$SLURM_JOB_ID.txt <<'PY'
import json,re,sys
lo,hi=int(sys.argv[1]),int(sys.argv[2])
d=json.load(open("${GNOMON_BENCH_ROOT}/w5-wikidata/raw/ia-wikidatawiki-20190101.json"))
for f in d["files"]:
    m=re.search(r"pages-meta-history\d*\.xml-p(\d+)p(\d+)\.7z$",f["name"])
    if m and lo<=int(m.group(1))<=hi: print(f["md5"],f["name"])
PY
wc -l $T/raw/fetchlist-$SLURM_JOB_ID.txt
fetch1() { md5=$1; n=$2
  if [ -f "$n" ] && echo "$md5  $n" | md5sum -c --quiet 2>/dev/null; then echo "have $n"; return 0; fi
  for a in 1 2 3; do
    curl -sfL --retry 3 -o "$n.part" "https://archive.org/download/wikidatawiki-20190101/$n" && mv "$n.part" "$n" && echo "$md5  $n" | md5sum -c --quiet && { echo "ok $n"; return 0; }
    sleep 10
  done; echo "FAIL $n"; return 1; }
export -f fetch1
s=$(date +%s)
xargs -P 4 -L 1 bash -c 'fetch1 "$0" "$1"' < $T/raw/fetchlist-$SLURM_JOB_ID.txt
echo "elapsed $(( $(date +%s)-s )) s; fails: $(grep -c FAIL $T/logs/fetch-$SLURM_JOB_ID.out)"; du -sh $D
