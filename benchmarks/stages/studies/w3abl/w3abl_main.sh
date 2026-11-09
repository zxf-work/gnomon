#!/bin/bash
# W3 SF10 component study (audit #8b), Gnomon only, the 12-query subset, windows paired by hand: for each switch and query, a back-to-back pair (unset, switched), order alternating; a window is one process, which opens the store under its environment (a handle reads the switches at open). Descriptive.
# Stops: 2 a missing input, 7 a pair's answers differ or an unset answer differs from C4's recorded timed answer. A cell censored or refused stays so: no ratio.
source ${NEXT_STAGE_ROOT:?}/src/benchmarks/stages/studies/lib.sh
: "${OUT:?}" "${ROOT:?}"
HERE=$REPO/benchmarks/stages/studies/w3abl; NEXT_ROOT=$NEXT_STAGE_ROOT
export W3_STAGE_ROOT=$NEXT_STAGE_ROOT
source $REPO/benchmarks/stages/w3/jobs/w3_lib.sh
PTT=${W3ABL_POINT:-SF10}; pt_config $PTT || exit 2
QUERIES=${W3ABL_QUERIES:-IS1 IS2 IC2 IC6 IC9 IC13 BI2 BI8 BI14 BIH-VER BIH-R3 TLDBC-IS3}; N=${W3ABL_N:-4}; SETTINGS=${W3ABL_SETTINGS:-current,asof,window}
ARMS=${W3ABL_ARMS:-range_visibility_off adjacency_seed_off window_bound_off}
env_of() { case $1 in
  unset|aa) echo X=1 ;;
  range_visibility_off) echo GNOMON_RANGE_VISIBILITY=0 ;;
  adjacency_seed_off) echo "GNOMON_TCSR=scan GNOMON_SEED_EXPANSION=0" ;;
  window_bound_off) echo "GNOMON_WINDOW_FIRST=0 GNOMON_NO_STAIRCASE=1" ;;
  *) echo "unknown configuration $1" >&2; exit 2 ;; esac; }
setup_root $ROOT
RESL=$ROOT/res; mkdir -p $RESL; OUTS() { cp -au $RESL/. $OUT/; }
TARGET=""; restore_sys gnomon || { echo "NO GNOMON STORE $PTT: stopping"; exit 2; }
HISTL=${LOCAL:-${TMPDIR:-/tmp}}/w3hist.duckdb; cp $HIST $HISTL || exit 2
( cd $PARD && sha256sum -c SHA256SUMS --ignore-missing ) | tail -2 || { echo "PARAMETER FILES DO NOT MATCH: stopping"; exit 2; }
cp $PARAMS $RESL/params-used.jsonl
TR="${EXTCMP_NUMA_CLIENT:+$EXTCMP_NUMA_CLIENT }$PYV $TOOLS/w3_timed.py run"
window() {   # arm name query tag
  local arm=$1 q=$2 tag=$3 inst=$4
  local out=$RESL/$arm/$q; mkdir -p $out
  say "window $q $tag $arm"
  env $(env_of $tag) $TR --system gnomon --target "$TARGET" --params $PARAMS --cells "$q" --settings $SETTINGS --instances $inst --hist-db $HISTL --rotation 1 \
      --out $out/$tag.json --jsonl $out/$tag-calls.jsonl --label "c4 W3 $PTT component study $q $tag" 2>&1 | tail -n 2
  [ "${PIPESTATUS[0]}" = 0 ] && [ -s $out/$tag.json ] || { echo "WINDOW FAILED $q $arm $tag"; echo "$q $arm $tag" >> $RESL/failed-windows.txt; }
  OUTS
}
# W3ABL_PLAN: configuration:instances, in order (second job: A/A on 0-3, the configurations on 4-7); unset, the first job's form. BI14 is at the ceiling unset: instances 0, 1 only, so a plan on 4-7 skips it.
PLAN=${W3ABL_PLAN:-}; if [ -z "$PLAN" ]; then for a in $ARMS; do PLAN="$PLAN $a:$(seq -s, 0 $((N-1)))"; done; fi
ai=0
for pl in $PLAN; do
  arm=${pl%%:*}; insts=${pl#*:}
  qi=0
  for q in $QUERIES; do
    inst=$insts
    if [ $q = BI14 ]; then case ",$insts," in *,0,*) inst=0,1 ;; *) qi=$((qi + 1)); continue ;; esac; fi
    if [ $(( (ai + qi) % 2 )) = 0 ]; then window $arm $q unset $inst; window $arm $q $arm $inst; else window $arm $q $arm $inst; window $arm $q unset $inst; fi
    $PYV $HERE/w3abl_report.py pair --dir $RESL/$arm/$q --arm $arm --ref ${W3ABL_REF:-} ; prc=$?
    [ $prc = 0 ] || { echo "ANSWER DIFFERENCE $arm $q: stopping $(utc)"; OUTS; exit 7; }
    qi=$((qi + 1))
  done
  ai=$((ai + 1))
  $PYV $HERE/w3abl_report.py table --res $RESL --out $RESL/report.md; OUTS
done
[ -s $RESL/failed-windows.txt ] && { echo "FAILED WINDOWS"; cat $RESL/failed-windows.txt; exit 8; }
say "W3 component study body done"
