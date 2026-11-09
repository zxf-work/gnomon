#!/bin/bash
# W3 gate job body (run by w3_gate.sbatch in its own process group, so the time-limit trap can stop it whole).
source ${W3_STAGE_ROOT:-${GNOMON_BENCH_ROOT}/final/w3}/src/benchmarks/stages/w3/jobs/w3_lib.sh
unset EXTCMP_NUMA_CLIENT EXTCMP_NUMA_SERVER
: "${OUT:?}" "${W3_GATE_POINT:?}" "${W3_GATE_SYS:?}"
pt_config $W3_GATE_POINT || exit 2
setup_root ${LOCAL:-${TMPDIR:-/tmp}}/w3g
( cd $PARD && sha256sum -c SHA256SUMS --ignore-missing ) || { echo "W3 parameter files do not match SHA256SUMS"; exit 2; }
HISTL=${LOCAL:-${TMPDIR:-/tmp}}/w3hist.duckdb; cp $HIST $HISTL || exit 2
gate_one() {   # system
  local s=$1 d=$OUT/$PT/$1 n st g rc TARGET=""
  mkdir -p $d; n=$(n_of $s); st=$(settings_of $s)
  $PYV $TOOLS/w3_timed.py subset --params $PARAMS --n $n --out $d/params.jsonl || return 2
  echo $st > $d/settings.txt
  say "$s: restore (n=$n, settings $st)"
  restore_sys $s || { echo "RESTORE FAILED $s" | tee $d/FAILED; return 2; }
  case $s in postgres|neo4j|memgraph) srv_start $s || { echo "START FAILED $s" | tee $d/FAILED; srv_stop $s; return 2; } ;; esac
  say "$s: answer check, target $TARGET"
  for g in $QGROUPS; do
    timeout -k 30 $VWIN $PYV $TOOLS/w3_run.py run --system $s --target "$TARGET" --params $d/params.jsonl \
        --queries $(q_of $g) --settings $st --hist-db $HISTL --out $d/$g.jsonl >> $d/run.log 2>&1
    rc=$?
    [ $rc -ne 0 ] && echo "window $g cut or failed (rc $rc) $(utc)" >> $d/windows.txt
  done
  case $s in postgres|neo4j|memgraph) srv_stop $s ;; esac
  say "$s: done"
  return 0
}
main() {
  local s rcs=0; declare -A pid
  for s in $W3_GATE_SYS; do gate_one $s > $OUT/$PT.$s.log 2>&1 & pid[$s]=$!; done
  for s in $W3_GATE_SYS; do wait ${pid[$s]}; local r=$?; echo "gate $s rc=$r"; [ $r -ne 0 ] && rcs=$r; done
  "$KIT/lib/servers.sh" stopall
  cat $OUT/$PT.*.log | tail -n 80
  return $rcs
}
main
