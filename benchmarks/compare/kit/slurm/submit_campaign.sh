#!/usr/bin/env bash
# Submit the campaign on the evaluation host as one linear chain of whole-node jobs
# (slurm/stages.tsv, in order). Each job waits for the previous one
# (--dependency=afterany), so exactly one campaign job runs at a time; a job
# whose stage is already done (or blocked by an answer check or the
#
#   kit/slurm/submit_campaign.sh -A <account> [--only <stage,...>] [--with-later] [--with-optional] [--dry-run]
set -euo pipefail
SL=$(cd "$(dirname "$0")" && pwd); KIT=$(cd "$SL/.." && pwd)
ACCOUNT="${SBATCH_ACCOUNT:-}"; ONLY=""; OPT=0; LATER=0; DRY=0
while [ $# -gt 0 ]; do case $1 in
  -A|--account) ACCOUNT=$2; shift 2 ;; --only) ONLY=$2; shift 2 ;;
  --with-optional) OPT=1; shift ;; --with-later) LATER=1; shift ;; --dry-run) DRY=1; shift ;;
  *) echo "unknown argument $1"; exit 2 ;; esac; done
[ -n "$ACCOUNT" ] || { echo "-A <account> (or SBATCH_ACCOUNT) is required"; exit 2; }
partition() {   # node class -> the site's partition (site.env)
  case $1 in shared) echo "${SBATCH_PARTITION_SHARED:?set in site.env}" ;;
             whole) echo "${SBATCH_PARTITION_WHOLE:?set in site.env}" ;;
             bigmem) echo "${SBATCH_PARTITION_BIGMEM:?set in site.env}" ;; esac; }
: "${EXTCMP_PROJECT:?source \$GNOMON_BENCH_ROOT/site.env first}"
dep=""
while IFS=$'\t' read -r name part jobs limit state cmd; do
  case $name in ''|\#*) continue ;; esac
  [ -n "$ONLY" ] && case ",$ONLY," in *",$name,"*) ;; *) continue ;; esac
  case $cmd in optional:*) [ "$OPT" = 1 ] || continue ;; later:*) [ "$LATER" = 1 ] || continue ;; esac
  script=$SL/job.sbatch; [ "$name" = build ] && script=$SL/build.sbatch
  for j in $(seq 1 "$jobs"); do
    args=(-p "$(partition "$part")" -t "$limit:00:00" -A "$ACCOUNT" -J "$name-$j"
          --export="ALL,STAGE=$name,EXTCMP_PROJECT=$EXTCMP_PROJECT")      # the job reads its command and state key from stages.tsv
    [ -n "$dep" ] && args+=(--dependency="afterany:$dep")
    if [ "$DRY" = 1 ]; then
      echo "sbatch ${args[*]} $script"; dep="DRY$name$j"
    else
      dep=$(sbatch --parsable "${args[@]}" "$script")
      echo "submitted $name job $j/$jobs: $dep ($part, ${limit} h)"
    fi
  done
done < "$SL/stages.tsv"
