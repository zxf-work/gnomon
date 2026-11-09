#!/bin/bash
source ${NEXT_STAGE_ROOT:?}/src/benchmarks/stages/studies/lib.sh
: "${OUT:?}"
RUNS=${WR_RUNS:-w1:K10M-d0 w1:K10M-d10 w2:K10M-d0 w2:K10M-d10}
RESL=${LOCAL:-${TMPDIR:-/tmp}}/wrres; mkdir -p $RESL; setup_root ${LOCAL:-${TMPDIR:-/tmp}}/wr; export TMPDIR=$ROOT/scratch
source $KIT/lib/env.sh
PYC="${EXTCMP_NUMA_CLIENT:+$EXTCMP_NUMA_CLIENT }$PYV"
$PYV $REPO/$STG/tools/cov_rss.py $RESL/rss-samples.jsonl 2 &
RSSP=$!; trap 'kill -TERM $RSSP 2>/dev/null' EXIT
echo '{"days": [], "snapshot": {"tt_last": 4611686018427387904}}' > $ROOT/meta.json
log_of() { case $1 in K1M) echo synth-1000000 ;; K10M) echo synth-10000000 ;; *) echo $1 ;; esac; }
sums_of() { case $1 in synth-1000000|K1M-d0) echo powerlaw-1000000 ;; synth-10000000|K10M-d0) echo powerlaw-10000000 ;; K10M-d10) echo W2-10M-d10 ;; K1M-d10) echo W2-1M-d10 ;; esac; }
getlog() {   # wl tag: node-local copy checked against the frozen sums; sets L
  local wl=$1 n=$(log_of $2) d=$ROOT/eventlogs/$1; mkdir -p $d; L=$d/$n.eventlog.jsonl
  case $wl in
    w1) cp -a $E/v3/eventlogs/$n.eventlog.jsonl $d/; ( cd $d && grep -E "  $n\.eventlog\.jsonl$" $E/v3/eventlogs/SHA256SUMS | sha256sum -c --quiet ) || { echo "FROZEN LOG CHECK FAILED $wl $n: stopping"; exit 2; } ;;
    w2) cp -a $E/w2-powerlaw/check1m/campaign/eventlogs/$n.eventlog.jsonl $d/
        want=$(awk -v f="$(sums_of $n).eventlog.jsonl" '$2 == f {print $1}' $E/w2-powerlaw/eventlogs/SHA256SUMS); got=$(sha256sum $L | cut -d' ' -f1)
        [ -n "$want" ] && [ "$want" = "$got" ] || { echo "FROZEN LOG CHECK FAILED $wl $n ($got vs $want): stopping"; exit 2; } ;;
  esac; echo "log $wl $n checked: $(stat -c %s $L) bytes"; }
verified() { $PYV -c "import json,sys; sys.exit(0 if json.load(open(sys.argv[1])).get('verified') else 1)" "$1" || { echo "VERIFICATION FAILED $1: stopping"; cp -au $RESL/. $OUT/; exit 7; }; }
DIGESTS=""
for x in $RUNS; do
  wl=${x%%:*}; tag=${x#*:}; id=$wl-$tag; getlog $wl $tag; st=$ROOT/gn-$id; rec=$RESL/run-$id.json
  say "writer path $id"
  $PYC $REPO/scripts/w3_ingest_gnomon.py --log $L --meta $ROOT/meta.json --store $st --out $rec | tail -c 900; echo
  [ "${PIPESTATUS[0]}" = 0 ] || { echo "REFUSED BATCH OR FAILED RUN $id: stopping"; cp -au $RESL/. $OUT/; exit 7; }
  verified $rec
  $PYV -c "import json,sys,os; p,s=sys.argv[1:3]; d=json.load(open(p)); d['du_sb_store']=int(os.popen('du -sb '+s).read().split()[0]); d['eventlog_bytes']=os.path.getsize(s+'/eventlog.jsonl'); json.dump(d,open(p,'w'),indent=1)" $rec $st
  DIGESTS="$DIGESTS $id:$st:$E/confirm/$wl/stores/$tag/native-$tag.tar.zst"
  cp -au $RESL/. $OUT/
done
ref_digest() {   # id store ref-archive: digest of this run's store against the replayed C4 store
  local id=$1 s=$2 r=$3 d=${LOCAL:-${TMPDIR:-/tmp}}/ref-$1 o=$RESL/digest-$1.json tgt
  need_file $r; rm -rf "${d:?}"; mkdir -p $d; zstd -q -d -c $r | tar -C $d -xf - || { echo "REFERENCE UNPACK FAILED $r: stopping"; exit 2; }; tgt=$(ls -d $d/*/ | head -1)
  GNOMON_STORE_WARM_OPEN=0 $PYV -c "
import json, sys, gnomon
def dg(p):
    s = gnomon.open(p, backend='native', read_only=True); d = s.digest_streaming(); s.close(); return d
a, b = dg(sys.argv[1]), dg(sys.argv[2])
json.dump({'this_run': a, 'reference': b, 'reference_path': sys.argv[3], 'equal': a == b, 'verified': a == b}, open(sys.argv[4], 'w'), indent=1); print('digest', sys.argv[3], a[:16], b[:16], 'equal' if a == b else 'DIFFERENT')" "$s" "$tgt" "$r" "$o" || { echo "DIGEST FAILED $id: stopping"; exit 8; }; }
say "digests, against the replayed stores, side by side"
pids=(); outs=()
for x in $DIGESTS; do IFS=: read -r id st r <<< "$x"; ref_digest $id $st $r & pids+=($!); outs+=($RESL/digest-$id.json); done
for p in "${pids[@]}"; do wait $p || { echo "DIGEST STEP FAILED: stopping"; cp -au $RESL/. $OUT/; exit 8; }; done
kill -TERM $RSSP 2>/dev/null
$PYV $REPO/$STG/writer/tools/writer_table.py $RESL $E > $RESL/phases.md || echo "table failed"; cat $RESL/phases.md
cp -au $RESL/. $OUT/
for o in "${outs[@]}"; do verified $o; done
say "writer body done"
