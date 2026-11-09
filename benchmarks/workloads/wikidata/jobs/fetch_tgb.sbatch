#!/bin/bash
#SBATCH -n 2
#SBATCH -t 04:00:00
#SBATCH -J w5-tgb
#SBATCH -o fetch-tgb-%j.out
set -euo pipefail
cd ${GNOMON_BENCH_ROOT}/w5-wikidata/raw/tgb
curl -sfL --retry 5 -o tkgl-wikidata.zip https://object-arbutus.alliancecan.ca/swift/v1/14c95234f6cd4a21a47deafe20cce2a7/tgb/tkgl-wikidata.zip
ls -l tkgl-wikidata.zip; unzip -l tkgl-wikidata.zip
unzip -o -q tkgl-wikidata.zip 'tkgl-wikidata_edgelist.csv' || unzip -o -q tkgl-wikidata.zip '*edgelist.csv'
sha256sum tkgl-wikidata.zip tkgl-smallpedia.zip *.csv > SHA256SUMS
rm -f tkgl-wikidata.zip   # only the edge list is needed; sha recorded above
ls -l; cat SHA256SUMS
