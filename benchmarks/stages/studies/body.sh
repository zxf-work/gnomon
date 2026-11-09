# time-limit trap, exit 5 at the limit, the records synced. run_body <body> <stage out> <sync command>
run_body() {
  local body=$1 syncc=$3; OUT=$2; mkdir -p $OUT; export OUT
  child=""
  on_usr1() { echo "=== time limit near $(utc): stopping, syncing"; [ -n "$child" ] && kill -TERM -- -$child 2>/dev/null; sleep 5
              $KIT/lib/servers.sh stopall 2>/dev/null; eval "$syncc" 2>/dev/null; echo "STOPPED at the time limit $(utc)" > $OUT/STOPPED; exit 5; }
  trap on_usr1 USR1 TERM
  setsid bash $body &
  child=$!
  wait $child; local rc=$?
  $KIT/lib/servers.sh stopall 2>/dev/null; eval "$syncc" 2>/dev/null
  say "job done rc=$rc"
  exit $rc
}
