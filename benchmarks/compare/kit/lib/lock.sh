#!/usr/bin/env bash
# Optional host gate lock (sourced). EXTCMP_GATE_DIR unset => dedicated host,
# no locking: timed_run/heavy_run just run the command. With EXTCMP_GATE_DIR
# set, the append-only announce protocol of the shared rehearsal host:
#   heavy : wait while another live pid is announced, then flock -s
# Liveness by [ -e /proc/<pid> ]; an unparseable first field counts as live;
# the marker file and the lock file are never rewritten, truncated or removed.
LANE_NAME=${EXTCMP_LANE_NAME:-extcmp}
utc() { date -u +%Y-%m-%dT%H:%M:%SZ; }
if [ -z "${EXTCMP_GATE_DIR:-}" ]; then
  pending_others() { :; }
  gate_lockers() { :; }
  timed_run() { echo "=== timed (no gate) $(utc): $* ==="; "$@"; }
  heavy_run() { echo "=== heavy (no gate) $(utc): $* ==="; "$@"; }
  return 0 2>/dev/null || exit 0
fi
PEND=$EXTCMP_GATE_DIR/.timed-pending
GATE=$EXTCMP_GATE_DIR/.gate.lock
announce() { printf '%s %s timed %s\n' "$1" "$LANE_NAME" "$(utc)" >> "$PEND"; }
retract()  { printf -- '-%s %s retract %s\n' "$1" "$LANE_NAME" "$(utc)" >> "$PEND"; }
pending_others() {
  local me="${1:-}"
  [ -e "$PEND" ] || return 0
  if [ ! -f "$PEND" ] || [ ! -r "$PEND" ]; then echo BAD; return 0; fi
  awk '{ f = $1
         if (f ~ /^-[0-9]+$/)     p[substr(f, 2)] = 0
         else if (f ~ /^[0-9]+$/) p[f] = 1
         else if (f != "")        bad = 1 }
       END { if (bad) print "BAD"; for (x in p) if (p[x]) print x }' "$PEND" |
  while read -r p; do
    if [ "$p" = BAD ]; then echo BAD; continue; fi
    [ "$p" = "$me" ] && continue
    [ -e "/proc/$p" ] && echo "$p"
  done
}
gate_lockers() {
  local ino; ino=$(stat -c %i "$GATE") || return 0
  awk -v ino="$ino" '{ i = ($2 == "->") ? 3 : 2; n = split($(i + 4), a, ":")
                       if (a[n] == ino) print (($2 == "->") ? "waiter:" : "holder:") $(i + 3) }' /proc/locks
}
timed_run() {
  local me=$$ rc
  announce "$me"
  echo "=== timed: announced (pid $me), waiting for -x $(utc): $* ==="
  flock -x "$GATE" env TIMED_PID="$me" LANE_NAME="$LANE_NAME" PEND="$PEND" \
      bash -c 'printf -- "-%s %s retract %s\n" "$TIMED_PID" "$LANE_NAME" \
                   "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "$PEND"
               echo "=== timed: -x acquired $(date -u +%Y-%m-%dT%H:%M:%SZ) ==="
               exec "$@"' _ "$@"
  rc=$?
  retract "$me"
  echo "=== timed: released rc=$rc $(utc) ==="
  return $rc
}
heavy_run() {
  local rc w
  while :; do
    w=$(pending_others "$$" | tr '\n' ' ')
    [ -z "$w" ] && break
    echo "deferring to pending timed pids: $w $(utc)"; sleep 30
  done
  echo "=== heavy: taking -s $(utc): $* ==="
  flock -s "$GATE" "$@"; rc=$?
  echo "=== heavy: released rc=$rc $(utc) ==="
  return $rc
}
