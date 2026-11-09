#!/usr/bin/env bash
# Database servers under Apptainer (no Docker, no root, no cgroup limits):
# Docker backend in servers.sh, which dispatches here when
# EXTCMP_BACKEND=apptainer:
#
#   servers_apptainer.sh start|stop|wipe|bytes|inspect|state|logs <system> <tag> ; stopall
#
# EXTCMP_NEO4J_PLUGINS, e.g. '["graph-data-science"]': the Neo4j image installs the named plugins at start
# (LDBC's BI 15/19/20 and Interactive 14 texts require the Graph Data Science library).
# EXTCMP_DURABLE=1: Memgraph flushes its WAL at every commit, AeonG runs
# with its WAL on and flushed at every commit; PostgreSQL (fsync and
# synchronous_commit at their defaults), Neo4j (transaction log) and DuckDB
# are durable by default; ClickHouse durability is per table (W3 creates its
# tables with fsync_after_insert = 1, fsync_part_directory = 1).
#
# Each server is one Apptainer instance (`apptainer instance start`, host
# network, so the loopback ports of the Docker backend apply unchanged) with
# its data directory bound from $SRV (node-local ${LOCAL:-${TMPDIR:-/tmp}} in a Slurm job), and
# the server process started inside it with `apptainer exec instance://…`
# from a host-side wrapper that records its pid and, when it ends, its exit
# code. Memory is capped by each server's own configuration (no cgroups):
# PostgreSQL shared_buffers/work_mem, ClickHouse max_server_memory_usage,
# Neo4j heap + page cache, Memgraph/AeonG --memory-limit, XTDB -Xmx.
#
# `state` prints one line `<alive> status=… pid=… exit=… finished=…` (alive =
# true/false) — what the per-query liveness check reads; `logs` prints the
# server log tail. An exit code of 137 is a SIGKILL, the kernel OOM killer's
#
# Images: $SIF_DIR/<key>.sif for every key of images.lock, pulled by digest
# image (apptainer/aeong.def).
set -euo pipefail

# Durability: EXTCMP_DURABLE=1
# (the default, the primary configuration) = write-ahead log on and flushed at every commit
# for Memgraph and AeonG (Memgraph flushes its WAL file every 100,000 transactions by
# default); EXTCMP_DURABLE=0 = the earlier campaign settings (Memgraph WAL on with the
# default flush; AeonG WAL and snapshots off), reported as a labelled non-durable configuration.
durable_flags() { if [ "${EXTCMP_DURABLE:-1}" = 1 ]; then echo "--storage-wal-file-flush-every-n-tx=1"; fi; }
aeong_wal_flags() {
  if [ "${EXTCMP_DURABLE:-1}" = 1 ]; then
    echo "--storage-snapshot-interval-sec=86400 --storage-wal-enabled=true --storage-wal-file-flush-every-n-tx=1"
  else echo "--storage-snapshot-interval-sec=0 --storage-wal-enabled=false"; fi
}

# Engine time: EXTCMP_PG_STATS=1 loads
# pg_stat_statements (it ships with PostgreSQL) with plan-time tracking, for the per-call server
# plan + execution time. Unset, the server runs with the campaign's settings exactly.
pg_stats_flags() {
  if [ "${EXTCMP_PG_STATS:-0}" = 1 ]; then
    echo "-c shared_preload_libraries=pg_stat_statements -c pg_stat_statements.track_planning=on -c pg_stat_statements.max=10000"
  fi
}

# Memgraph-family allocator: jemalloc
# retain:true keeps unmapped extents instead of munmap-ing them, which against the evaluation host's vm.max_map_count
# (65,530) avoided the Memgraph crash (exit 134) of an 8.9M-row LOAD CSV. Memgraph reads
# JE_MALLOC_CONF, AeonG reads MALLOC_CONF. Unset: retain:true; set to the empty
# string: no option (the jemalloc default, retain:false in these builds).
export EXTCMP_MG_MALLOC_CONF=${EXTCMP_MG_MALLOC_CONF-retain:true}
export EXTCMP_AEONG_MALLOC_CONF=${EXTCMP_AEONG_MALLOC_CONF-retain:true}
source "$(dirname "$0")/env.sh"
: "${SIF_DIR:?set SIF_DIR to the directory of the .sif images (stage_hpc.sh)}"
# with EXTCMP_JOB_SAFE=1 the instance name carries the Slurm job id: two jobs on one node must never stop or
# reuse each other's instance (ports.sh)
name() { if [ "${EXTCMP_JOB_SAFE:-0}" = 1 ]; then echo "extcmp-$1-$2${SLURM_JOB_ID:+-j$SLURM_JOB_ID}"; else echo "extcmp-$1-$2"; fi; }
port_of() {   # the server's client port (ports.sh)
  case $1 in postgres) echo "$EXTCMP_PG_PORT" ;; clickhouse) echo "$EXTCMP_CH_HTTP_PORT" ;; neo4j) echo "$EXTCMP_NEO4J_PORT" ;;
             memgraph) echo "$EXTCMP_MG_PORT" ;; xtdb) echo "$EXTCMP_XTDB_PORT" ;; aeong) echo "$EXTCMP_AEONG_PORT" ;; esac; }
port_open() { (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; }
descendants() {   # every descendant of a pid, breadth first, by parent pid (no name patterns)
  local front=$1 desc="" nxt q
  while [ -n "$front" ]; do
    nxt=""; for q in $front; do nxt="$nxt $(ps -o pid= --ppid "$q" 2>/dev/null | tr '\n' ' ' || true)"; done
    front=$(echo $nxt); desc="$desc $front"
  done
  echo $desc
}
# own_listener <port> <wrapper pid>: 0 if a process listening on the port descends from our wrapper, 1 if the
# listener is another process, 2 if the listener's pid cannot be read
own_listener() {
  local lp p q
  lp=$(ss -ltnpH "sport = :$1" 2>/dev/null | grep -o 'pid=[0-9]*' | cut -d= -f2 | sort -u || true)
  [ -n "$lp" ] || return 2
  for p in $(descendants "$2"); do for q in $lp; do [ "$p" = "$q" ] && return 0; done; done
  return 1
}
sif() { local f=$SIF_DIR/$1.sif; [ -e "$f" ] || { echo "missing image $f (run stage_hpc.sh)" >&2; exit 2; }; echo "$f"; }
utc() { date -u +%FT%TZ; }
gb() { echo $(( ${1%g} * 1024 )); }   # 48g -> 49152 (MB)
CAP_MB=$(gb "${CAP:-48g}")

# run_server <instance> <dir> [apptainer exec flags...] -- <cmd...>: start the
# server process inside the instance (environment and working directory are
# exec flags: an instance's own environment does not reach exec'd processes)
# from a host-side wrapper that keeps its pid and, at the end, its exit code
run_server() {
  local n=$1 d=$2; shift 2
  local flags=()
  while [ $# -gt 0 ] && [ "$1" != -- ]; do flags+=("$1"); shift; done
  shift
  rm -f "$d/server.pid" "$d/server.exit"
  nohup bash -c 'd=$1; shift
                 ${EXTCMP_NUMA_SERVER:-} apptainer exec "$@" > "$d/server.log" 2>&1; rc=$?
                 echo "$rc $(date -u +%FT%TZ)" > "$d/server.exit"' \
      _ "$d" --cleanenv "${flags[@]}" "instance://$n" "$@" > /dev/null 2>&1 &
  echo $! > "$d/server.pid"
}

instance_running() { apptainer instance list 2>/dev/null | awk -v n="$1" '$1 == n {f=1} END {exit !f}'; }

# pg_stop <instance> <dir>: clean PostgreSQL shutdown. pg_ctl runs inside the
# instance (its PID namespace, where postmaster.pid's pid is valid), sends the
# postmaster SIGINT (fast shutdown) and waits until it has removed
# postmaster.pid. Without this, `apptainer instance stop` could kill the
# postmaster mid-shutdown and leave a stale postmaster.pid (see start).
pg_stop() {
  local n=$1 d=$2
  instance_running "$n" || return 0
  [ -e "$d/pgdata/postmaster.pid" ] || return 0
  if ! apptainer exec --cleanenv "instance://$n" pg_ctl stop -D /var/lib/postgresql/data -m fast -w -t 600 \
       > "$d/pg_ctl-stop.log" 2>&1; then
    echo "pg_ctl stop of $n failed $(utc): $(tail -n 3 "$d/pg_ctl-stop.log" | tr '\n' ' ')" >&2
  fi
}

instance_up() {   # <instance> <sif> [extra instance-start args...]
  local n=$1 img=$2; shift 2
  apptainer instance stop "$n" > /dev/null 2>&1 || true
  apptainer instance start --writable-tmpfs "$@" "$img" "$n" > /dev/null
}

start() {
  local sys=$1 tag=$2 n d
  n=$(name "$sys" "$tag"); d=$SRV/$sys-$tag
  command -v apptainer > /dev/null || { echo "apptainer not found on this host" >&2; exit 2; }
  local img; img=$(sif "$sys") || exit 2
  mkdir -p "$d"
  # the port must be free: a server still shutting down, or another job's server, would answer this start's client
  local port; port=$(port_of "$sys")
  for _ in $(seq 1 120); do port_open "$port" || break; sleep 1; done
  if port_open "$port"; then echo "port $port ($sys) still in use after 120 s: refusing to start $n" >&2; exit 1; fi
  case $sys in
    postgres)
      mkdir -p "$d/pgdata" "$d/run"
      # a postmaster.pid left by an unclean stop holds a pid of the old
      # instance's PID namespace; in the new instance that pid can belong to
      # another live process, and PostgreSQL then refuses to start ("lock file
      # postmaster.pid already exists"). With no instance of this name running,
      # no postmaster can own this data directory: the file is stale.
      if instance_running "$n"; then stop "$sys" "$tag"; fi
      if [ -e "$d/pgdata/postmaster.pid" ] && ! instance_running "$n"; then
        echo "removed stale $d/pgdata/postmaster.pid (no instance $n running; pid $(head -n 1 "$d/pgdata/postmaster.pid" 2>/dev/null)) $(utc)"
        rm -f "$d/pgdata/postmaster.pid"
      fi
      instance_up "$n" "$img" --bind "$d/pgdata:/var/lib/postgresql/data" \
          --bind "$d/run:/var/run/postgresql"
      run_server "$n" "$d" --env POSTGRES_HOST_AUTH_METHOD=trust \
          --env "POSTGRES_INITDB_ARGS=--locale=C --encoding=UTF8" --env PGDATA=/var/lib/postgresql/data \
          -- docker-entrypoint.sh postgres \
        -c listen_addresses=127.0.0.1 -c port=$EXTCMP_PG_PORT \
        -c shared_buffers=16GB -c effective_cache_size=64GB -c work_mem=256MB \
        -c maintenance_work_mem=2GB -c random_page_cost=1.1 -c effective_io_concurrency=200 \
        -c wal_buffers=16MB -c max_wal_size=8GB -c checkpoint_completion_target=0.9 \
        -c max_parallel_workers_per_gather=4 -c jit=on $(pg_stats_flags) ;;
    clickhouse)
      mkdir -p "$d/data" "$d/conf" "$d/log"
      cat > "$d/conf/extcmp.xml" <<X
<clickhouse>
  <listen_host replace="replace">127.0.0.1</listen_host>
  <http_port>$EXTCMP_CH_HTTP_PORT</http_port>
  <tcp_port>$EXTCMP_CH_TCP_PORT</tcp_port>
  <mysql_port remove="remove"/>
  <postgresql_port remove="remove"/>
  <interserver_http_port remove="remove"/>
  <mark_cache_size>5368709120</mark_cache_size>
  <max_server_memory_usage>34359738368</max_server_memory_usage>
</clickhouse>
X
      instance_up "$n" "$img" --bind "$d/data:/var/lib/clickhouse" \
          --bind "$d/log:/var/log/clickhouse-server" \
          --bind "$d/conf/extcmp.xml:/etc/clickhouse-server/config.d/extcmp.xml:ro"
      run_server "$n" "$d" -- clickhouse-server --config-file=/etc/clickhouse-server/config.xml ;;
    neo4j)
      mkdir -p "$d/data" "$d/logs"
      instance_up "$n" "$(sif neo4j)" --bind "$d/data:/data" --bind "$d/logs:/logs"
      run_server "$n" "$d" --env NEO4J_AUTH=none --env NEO4J_server_default__listen__address=127.0.0.1 \
          --env NEO4J_server_bolt_listen__address=127.0.0.1:$EXTCMP_NEO4J_PORT \
          --env NEO4J_server_http_listen__address=127.0.0.1:$EXTCMP_NEO4J_HTTP_PORT --env NEO4J_server_https_enabled=false \
          --env NEO4J_server_memory_heap_initial__size=16g --env NEO4J_server_memory_heap_max__size=16g \
          --env NEO4J_server_memory_pagecache_size=24g \
          --env NEO4J_db_transaction_timeout=${SERVER_TIMEOUT_S}s \
          ${EXTCMP_NEO4J_PLUGINS:+--env NEO4J_PLUGINS=$EXTCMP_NEO4J_PLUGINS --env NEO4J_dbms_security_procedures_unrestricted=gds.* --env NEO4J_dbms_security_procedures_allowlist=gds.*} \
          -- /startup/docker-entrypoint.sh neo4j ;;
    memgraph)
      mkdir -p "$d/lib" "$d/log"
      instance_up "$n" "$img" --bind "$d/lib:/var/lib/memgraph" --bind "$d/log:/var/log/memgraph"
      # EXTCMP_MG_MALLOC_CONF: a jemalloc option string for the server process (e.g.
      # retain:true). Memgraph 3.12's jemalloc reads JE_MALLOC_CONF; the
      # other two names are ignored by it and passed for other builds; unset, the start is unchanged
      run_server "$n" "$d" ${EXTCMP_MG_MALLOC_CONF:+--env MALLOC_CONF=$EXTCMP_MG_MALLOC_CONF --env JE_MALLOC_CONF=$EXTCMP_MG_MALLOC_CONF --env _RJEM_MALLOC_CONF=$EXTCMP_MG_MALLOC_CONF} \
          -- /usr/lib/memgraph/memgraph \
        --bolt-address=127.0.0.1 --bolt-port=$EXTCMP_MG_PORT --monitoring-port=$EXTCMP_MG_MONITOR_PORT --metrics-port=$EXTCMP_MG_METRICS_PORT \
        --memory-limit=${EXTCMP_MG_MEMORY_LIMIT_MB:-40960} --storage-properties-on-edges=true \
        --query-execution-timeout-sec=$SERVER_TIMEOUT_S --log-level=WARNING --data-recovery-on-startup=true \
        --storage-snapshot-interval-sec=86400 --storage-wal-enabled=true $(durable_flags) ;;
    xtdb)
      mkdir -p "$d/var" "$d/conf"
      printf "server:\n  host: '127.0.0.1'\n  port: $EXTCMP_XTDB_PORT\n\nlog: !Local\n  path: \"/var/lib/xtdb/log\"\n\nstorage: !Local\n  path: \"/var/lib/xtdb/buffers\"\n\nhealthz:\n  host: '127.0.0.1'\n  port: $EXTCMP_XTDB_HEALTH_PORT\n" \
        > "$d/conf/local_config.yaml"
      instance_up "$n" "$img" --bind "$d/var:/var/lib/xtdb" \
          --bind "$d/conf/local_config.yaml:/usr/local/lib/xtdb/local_config.yaml:ro"
      # the documented Apptainer invocation, heap
      # capped explicitly because no cgroup limits the JVM here
      run_server "$n" "$d" --pwd /usr/local/lib/xtdb -- java "-Xmx${EXTCMP_XTDB_HEAP:-32g}" \
        -Dclojure.main.report=stderr --add-opens=java.base/java.nio=ALL-UNNAMED \
        --enable-native-access=ALL-UNNAMED -Dio.netty.tryReflectionSetAccessible=true \
        -cp xtdb-standalone.jar clojure.main -m xtdb.main -f local_config.yaml ;;
    aeong)
      # its history survives a restart while its timestamps do not: always empty
      rm -rf "$d/data"; mkdir -p "$d/data"
      instance_up "$n" "$img" --bind "$d/data:/data"
      # EXTCMP_AEONG_MALLOC_CONF: a jemalloc option string for AeonG; its jemalloc reads
      # MALLOC_CONF; unset, the start is unchanged
      run_server "$n" "$d" ${EXTCMP_AEONG_MALLOC_CONF:+--env MALLOC_CONF=$EXTCMP_AEONG_MALLOC_CONF} -- /opt/aeong/memgraph-_Release \
        --bolt-address=127.0.0.1 --bolt-port=$EXTCMP_AEONG_PORT --data-directory=/data \
        --storage-properties-on-edges=true --memory-limit=${EXTCMP_MG_MEMORY_LIMIT_MB:-40960} \
        --query-execution-timeout-sec=$SERVER_TIMEOUT_S \
        $(aeong_wal_flags) \
        --log-level=WARNING --also-log-to-stderr=true ;;
    *) echo "unknown system $sys" >&2; exit 2 ;;
  esac
  # wait until the port answers, then verify that the listener is this job's own server
  local own=unknown
  for _ in $(seq 1 ${EXTCMP_START_WAIT_S:-900}); do
    if [ -e "$d/server.exit" ]; then
      echo "server $n exited at once: $(cat "$d/server.exit")" >&2; tail -n 30 "$d/server.log" >&2; exit 1
    fi
    port_open "$port" && break; sleep 1
  done
  sleep 2
  if [ -e "$d/server.exit" ] || grep -qiE "cannot bind|address already in use|failed to bind|bindexception" "$d/server.log" 2>/dev/null; then
    echo "server $n did not come up as this job's own server on port $port" >&2; tail -n 20 "$d/server.log" >&2; stop "$sys" "$tag" >&2; exit 1
  fi
  if port_open "$port"; then
    local rc=0; own_listener "$port" "$(cat "$d/server.pid")" || rc=$?
    case $rc in
      0) own=verified ;;
      1) echo "port $port is answered by a process that is not $n's: refusing" >&2; stop "$sys" "$tag" >&2; exit 1 ;;
      *) own="unverified (listener pid not readable; the server process is alive and its log has no bind error)" ;;
    esac
  else
    own="port not yet open after ${EXTCMP_START_WAIT_S:-900} s"
  fi
  echo "started $n $(utc) port=$port listener=$own (apptainer; memory capped by the server's own settings; node RAM $(free -g | awk '/Mem:/{print $2}') GB; numa=[${EXTCMP_NUMA_SERVER:-none}]; durable=${EXTCMP_DURABLE:-1}$(case $sys in memgraph) echo "; malloc_conf=${EXTCMP_MG_MALLOC_CONF:-default}" ;; aeong) echo "; malloc_conf=${EXTCMP_AEONG_MALLOC_CONF:-default}" ;; esac))"
}

state() {   # one line: <alive> status=… pid=… exit=… finished=…
  local n d pid ex; n=$(name "$1" "$2"); d=$SRV/$1-$2
  pid=$(cat "$d/server.pid" 2>/dev/null || true)
  if [ -e "$d/server.exit" ]; then
    read -r ex fin < "$d/server.exit"
    local why=""; [ "$ex" = 137 ] && why=" (SIGKILL: likely the kernel OOM killer)"
    echo "false status=exited pid=${pid:-?} exit=$ex$why finished=$fin"
  elif [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
    echo "true status=running pid=$pid instance=$(apptainer instance list 2>/dev/null | awk -v n="$n" '$1 == n {print "up"}')"
  elif apptainer instance list 2>/dev/null | awk -v n="$n" '$1 == n {f=1} END {exit !f}'; then
    echo "false status=server-process-gone pid=${pid:-?} exit=unknown"
  else
    echo "false status=missing exit=unknown"
  fi
}

logs() { tail -n "${3:-40}" "$SRV/$1-$2/server.log" 2>/dev/null || true; }

stop() {
  local n d st pid kids; n=$(name "$1" "$2"); d=$SRV/$1-$2
  st=$(state "$1" "$2")
  pid=$(cat "$d/server.pid" 2>/dev/null || true)
  [ "$1" = postgres ] && pg_stop "$n" "$d"
  if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
    # the wrapper's child is `apptainer exec`, which forwards TERM to the
    # server: signal exactly the children of our own wrapper pid
    kids=$(ps -o pid= --ppid "$pid" 2>/dev/null | tr -d ' ' || true)
    local all; all=$(descendants "$pid")
    [ -n "$kids" ] && kill -TERM $kids 2>/dev/null || true
    for _ in $(seq 1 120); do kill -0 "$pid" 2>/dev/null || break; sleep 1; done
    # every process of our own server (it may outlive `apptainer exec` while it writes its snapshot)
    local alive=""
    for _ in $(seq 1 600); do
      alive=""; for m in $all; do kill -0 "$m" 2>/dev/null && alive="$alive $m"; done
      [ -z "$alive" ] && break; sleep 1
    done
    if [ -n "$alive" ]; then echo "server processes [$alive] of $n still alive 600 s after the stop" >&2; fi
  fi
  apptainer instance stop "$n" > /dev/null 2>&1 || true
  local port; port=$(port_of "$1")
  for _ in $(seq 1 120); do port_open "$port" || break; sleep 1; done
  if port_open "$port"; then echo "port $port still open 120 s after stopping $n" >&2; fi
  if [ "$1" = postgres ] && [ -e "$d/pgdata/postmaster.pid" ]; then
    echo "postgres $n left postmaster.pid after stop (unclean shutdown; the next start removes it)" >&2
  fi
  echo "stopped $n $(utc) (state before stop: $st)"
}

stopall() {
  local n
  # only this job's own instances (the name carries the job id); another job's instances on the node are not ours
  for n in $(apptainer instance list 2>/dev/null | awk -v s="$( [ "${EXTCMP_JOB_SAFE:-0}" = 1 ] && echo "${SLURM_JOB_ID:+-j$SLURM_JOB_ID}" )" 'NR > 1 && $1 ~ /^extcmp-/ && (s == "" || substr($1, length($1) - length(s) + 1) == s) {print $1}'); do
    case $n in extcmp-postgres-*) local t=${n#extcmp-postgres-}; pg_stop "$n" "$SRV/postgres-${t%-j*}" ;; esac
    apptainer instance stop "$n" > /dev/null 2>&1 || true
    echo "stopped $n $(utc)"
  done
}
bytes() { du -sb "$SRV/$1-$2" | awk '{print $1}'; }
wipe() { rm -rf "$SRV/$1-$2"; }
inspect() {
  local n; n=$(name "$1" "$2")
  printf '{"backend": "apptainer", "instance": "%s", "state": "%s", "sif": "%s", "apptainer": "%s"}\n' \
    "$n" "$(state "$1" "$2")" "$(ls -l "$SIF_DIR/$1.sif" 2>/dev/null | awk '{print $5, $9}')" "$(apptainer --version)"
}
cmd=$1; shift; "$cmd" "$@"
