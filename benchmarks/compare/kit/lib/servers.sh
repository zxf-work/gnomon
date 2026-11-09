#!/usr/bin/env bash
# Database servers (kit copy of the rehearsal's host-scripts/servers.sh, paths
# and images from lib/env.sh and images.lock). One server at a time.
#   servers.sh start|stop|wipe|bytes|inspect <system> <tag> ; servers.sh stopall
set -euo pipefail
source "$(dirname "$0")/env.sh"
if [ "$EXTCMP_BACKEND" = apptainer ]; then exec "$(dirname "$0")/servers_apptainer.sh" "$@"; fi
AEONG_BUILD=$AEONG_SRC/AeonG/build-release
name() { echo "extcmp-$1-$2"; }
start() {
  local sys=$1 tag=$2 n d
  n=$(name "$sys" "$tag"); d=$SRV/$sys-$tag
  mkdir -p "$d"; docker rm -f "$n" >/dev/null 2>&1 || true
  case $sys in
    postgres)
      docker run -d --name "$n" --network host --memory "$CAP" --shm-size 16g \
        -e POSTGRES_HOST_AUTH_METHOD=trust -e POSTGRES_INITDB_ARGS="--locale=C --encoding=UTF8" \
        -v "$d":/var/lib/postgresql/data "$(img postgres)" \
        -c listen_addresses=127.0.0.1 -c port=$EXTCMP_PG_PORT \
        -c shared_buffers=16GB -c effective_cache_size=64GB -c work_mem=256MB \
        -c maintenance_work_mem=2GB -c random_page_cost=1.1 -c effective_io_concurrency=200 \
        -c wal_buffers=16MB -c max_wal_size=8GB -c checkpoint_completion_target=0.9 \
        -c max_parallel_workers_per_gather=4 -c jit=on >/dev/null ;;
    clickhouse)
      mkdir -p "$d/data" "$d/conf"
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
      docker run -d --name "$n" --network host --memory "$CAP" --ulimit nofile=262144:262144 \
        -v "$d/data":/var/lib/clickhouse -v "$d/conf/extcmp.xml":/etc/clickhouse-server/config.d/extcmp.xml:ro \
        "$(img clickhouse)" >/dev/null ;;
    neo4j)
      mkdir -p "$d/data" "$d/logs"
      docker run -d --name "$n" --network host --memory "$CAP" --user "$(id -u):$(id -g)" \
        -e NEO4J_AUTH=none -e NEO4J_server_default__listen__address=127.0.0.1 \
        -e NEO4J_server_bolt_listen__address=127.0.0.1:$EXTCMP_NEO4J_PORT \
        -e NEO4J_server_http_listen__address=127.0.0.1:$EXTCMP_NEO4J_HTTP_PORT -e NEO4J_server_https_enabled=false \
        -e NEO4J_server_memory_heap_initial__size=16g -e NEO4J_server_memory_heap_max__size=16g \
        -e NEO4J_server_memory_pagecache_size=24g -e NEO4J_db_transaction_timeout=${SERVER_TIMEOUT_S}s \
        -v "$d/data":/data -v "$d/logs":/logs "$(img neo4j)" >/dev/null ;;
    memgraph)
      mkdir -p "$d/lib" "$d/log"
      docker run -d --name "$n" --network host --memory "$CAP" --user "$(id -u):$(id -g)" \
        -v "$d/lib":/var/lib/memgraph -v "$d/log":/var/log/memgraph "$(img memgraph)" \
        --bolt-address=127.0.0.1 --bolt-port=$EXTCMP_MG_PORT --monitoring-port=$EXTCMP_MG_MONITOR_PORT --metrics-port=$EXTCMP_MG_METRICS_PORT \
        --memory-limit=${EXTCMP_MG_MEMORY_LIMIT_MB:-40960} --storage-properties-on-edges=true \
        --query-execution-timeout-sec=$SERVER_TIMEOUT_S --log-level=WARNING --data-recovery-on-startup=true \
        --storage-snapshot-interval-sec=86400 --storage-wal-enabled=true >/dev/null ;;
    xtdb)
      mkdir -p "$d/var" "$d/conf"
      printf "server:\n  host: '127.0.0.1'\n  port: $EXTCMP_XTDB_PORT\n\nlog: !Local\n  path: \"/var/lib/xtdb/log\"\n\nstorage: !Local\n  path: \"/var/lib/xtdb/buffers\"\n\nhealthz:\n  host: '127.0.0.1'\n  port: $EXTCMP_XTDB_HEALTH_PORT\n" \
        > "$d/conf/local_config.yaml"
      docker run -d --name "$n" --network host --memory "$CAP" --user "$(id -u):$(id -g)" \
        -v "$d/var":/var/lib/xtdb -v "$d/conf/local_config.yaml":/usr/local/lib/xtdb/local_config.yaml:ro \
        "$(img xtdb)" >/dev/null ;;
    aeong)
      # its history survives a restart while its timestamps do not: always empty
      rm -rf "$d/data"; mkdir -p "$d/data"
      docker run -d --name "$n" --network host --memory "$CAP" --user "$(id -u):$(id -g)" \
        -v "$AEONG_BUILD":/aeong:ro -v "$d/data":/data --entrypoint /aeong/memgraph-_Release \
        "$(img aeong-toolchain)" \
        --bolt-address=127.0.0.1 --bolt-port=$EXTCMP_AEONG_PORT --data-directory=/data \
        --storage-properties-on-edges=true --memory-limit=${EXTCMP_MG_MEMORY_LIMIT_MB:-40960} \
        --query-execution-timeout-sec=$SERVER_TIMEOUT_S \
        --storage-snapshot-interval-sec=0 --storage-wal-enabled=false \
        --log-level=WARNING --also-log-to-stderr=true >/dev/null ;;
    *) echo "unknown system $sys" >&2; exit 2 ;;
  esac
  echo "started $n $(date -u +%FT%TZ)"
}
stop() { local n st; n=$(name "$1" "$2")
         st=$(docker inspect -f '{{.State.Status}} exit={{.State.ExitCode}} oom={{.State.OOMKilled}} finished={{.State.FinishedAt}}' "$n" 2>/dev/null)   # a server that died in the window shows here
         docker stop -t 120 "$n" >/dev/null 2>&1 || true
         docker rm "$n" >/dev/null 2>&1 || true; echo "stopped $n $(date -u +%FT%TZ) (state before stop: ${st:-none})"; }
stopall() { local n; for n in $(docker ps -a --filter name=extcmp- --format '{{.Names}}'); do
              case $n in extcmp-*) docker stop -t 120 "$n" >/dev/null 2>&1; docker rm "$n" >/dev/null 2>&1
                                   echo "stopped $n $(date -u +%FT%TZ)" ;; esac; done; }
bytes() { docker run --rm -v "$SRV/$1-$2":/d --entrypoint du "$(img postgres)" -sb /d | awk '{print $1}'; }
wipe() { [ -d "$SRV/$1-$2" ] || return 0; docker run --rm -v "$SRV":/s --entrypoint rm "$(img postgres)" -rf "/s/$1-$2"; }
inspect() { docker inspect "$(name "$1" "$2")"; }
# one line, `<alive> status=… exit=… oom=… finished=…`: what the per-query
# liveness check reads (the Apptainer backend prints the same shape)
state() { docker inspect -f '{{.State.Running}} status={{.State.Status}} exit={{.State.ExitCode}} oom={{.State.OOMKilled}} finished={{.State.FinishedAt}}' \
            "$(name "$1" "$2")" 2>/dev/null || echo "false status=missing"; }
logs() { docker logs --tail "${3:-40}" "$(name "$1" "$2")" 2>&1 || true; }
cmd=$1; shift; "$cmd" "$@"
