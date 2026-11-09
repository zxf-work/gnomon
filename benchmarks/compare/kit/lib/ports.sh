#!/usr/bin/env bash
# Loopback ports of the kit's servers.
#
# Per-job ports: a shared node nodes host several jobs, and a client must never reach another job's
# server. Two jobs on one node that both used the fixed port 17689 led to the second job's client loading into the
# first job's AeonG. In a Slurm job whose script sets EXTCMP_JOB_SAFE=1 (every W3 job script
# 9000, in three disjoint families, so two concurrent jobs share no port unless their ids are 9000 apart:
#   30000 + 3k: AeonG, +1 Neo4j bolt, +2 Neo4j http;
#    2000 + 3k: PostgreSQL, +1 Memgraph bolt, +2 Memgraph monitoring;
#   57000 + (id mod 500) * 8: the rarely used ports (Memgraph metrics, ClickHouse http/tcp, XTDB, its health check).
# Otherwise (outside Slurm, or without EXTCMP_JOB_SAFE=1) the fixed ports apply. Servers always refuse to start on
# a port that is taken, and verify that the listener is their own (servers_apptainer.sh).
if [ -n "${SLURM_JOB_ID:-}" ] && [ "${EXTCMP_JOB_SAFE:-0}" = 1 ]; then
  _k=$(( SLURM_JOB_ID % 9000 )); _a=$(( 30000 + 3 * _k )); _b=$(( 2000 + 3 * _k )); _c=$(( 57000 + (SLURM_JOB_ID % 500) * 8 ))
  export EXTCMP_AEONG_PORT=$_a EXTCMP_NEO4J_PORT=$((_a + 1)) EXTCMP_NEO4J_HTTP_PORT=$((_a + 2)) \
         EXTCMP_PG_PORT=$_b EXTCMP_MG_PORT=$((_b + 1)) EXTCMP_MG_MONITOR_PORT=$((_b + 2)) \
         EXTCMP_MG_METRICS_PORT=$_c EXTCMP_CH_HTTP_PORT=$((_c + 1)) EXTCMP_CH_TCP_PORT=$((_c + 2)) \
         EXTCMP_XTDB_PORT=$((_c + 3)) EXTCMP_XTDB_HEALTH_PORT=$((_c + 4))
  unset _k _a _b _c
else
  export EXTCMP_PG_PORT=15432 EXTCMP_NEO4J_PORT=17687 EXTCMP_NEO4J_HTTP_PORT=17474 EXTCMP_MG_PORT=17688 \
         EXTCMP_MG_MONITOR_PORT=17444 EXTCMP_MG_METRICS_PORT=19091 EXTCMP_AEONG_PORT=17689 \
         EXTCMP_CH_HTTP_PORT=18123 EXTCMP_CH_TCP_PORT=19000 EXTCMP_XTDB_PORT=15433 EXTCMP_XTDB_HEALTH_PORT=18080
fi
export PGHOST=127.0.0.1 PGPORT=$EXTCMP_PG_PORT PGUSER=postgres
export GNOMON_NEO4J_URI=bolt://127.0.0.1:$EXTCMP_NEO4J_PORT GNOMON_MEMGRAPH_URI=bolt://127.0.0.1:$EXTCMP_MG_PORT
export GNOMON_AEONG_URI=bolt://127.0.0.1:$EXTCMP_AEONG_PORT GNOMON_CH_HTTP_PORT=$EXTCMP_CH_HTTP_PORT GNOMON_XTDB_PORT=$EXTCMP_XTDB_PORT
# the libpq connection string the W3 scripts use
export EXTCMP_PG_DSN="host=127.0.0.1 port=$EXTCMP_PG_PORT user=postgres dbname=postgres"
# Memgraph's and AeonG's own memory limit below the job's allocation (a shared node: 2,000 MB per core; RM: the node),
# so a large query meets the server's limit error and not the kernel's OOM kill
if [ -n "${SLURM_JOB_ID:-}" ]; then
  _mem=${SLURM_MEM_PER_NODE:-$(( ${SLURM_CPUS_ON_NODE:-1} * ${SLURM_MEM_PER_CPU:-2000} ))}
  export EXTCMP_MG_MEMORY_LIMIT_MB=${EXTCMP_MG_MEMORY_LIMIT_MB:-$(( _mem * 3 / 4 < 40960 ? _mem * 3 / 4 : 40960 ))}
  unset _mem
fi
