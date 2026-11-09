#!/bin/bash
#SBATCH -J w3-ingest
#SBATCH -N 1
#SBATCH --ntasks-per-node=32
#SBATCH -t 04:00:00
#SBATCH -o ingest-%j.out
# W3 load method v2: bulk snapshot + per-transaction replay for DuckDB, PostgreSQL, Neo4j, Memgraph;
# each loaded store is persisted to $P/w3stores/<TAG>/ (tar.zst + sha256 + load record with read-back counts).
# STUDY=<name>: an ingest-study run (its own output directory); PREFIX=N: replay only the first N days + corrections.
# STEPS (default "load check"), any of: load; digest (restore, order-independent store digest to $OUT/<sys>.digest.json);
# check (restore the persisted store into ${LOCAL:-${TMPDIR:-/tmp}}, run the answer check on it).
set -uo pipefail
export EXTCMP_JOB_SAFE=1   # per-job ports and instance names
P=${GNOMON_BENCH_ROOT}/w3-snb
source $P/site.env; cd $EXTCMP_REPO; source .venv/bin/activate
export W3_EXT=$P/ext EXTCMP_ROOT=${LOCAL:-${TMPDIR:-/tmp}}/w3srv EXTCMP_DURABLE=1 TMPDIR=${LOCAL:-${TMPDIR:-/tmp}} EXTCMP_NEO4J_PLUGINS='["graph-data-science"]' W3_CAP_S=${W3_CAP_S:-120} EXTCMP_SERVER_TIMEOUT_S=${W3_CAP_S:-120}
mkdir -p $EXTCMP_ROOT/scratch $EXTCMP_ROOT/srv
SF=${SF:-0.1}; R=${R:-10}; H=${H:-h1}; TAG=SF$SF-$H-r$R; C=$P/check/$TAG; PAR=${PAR:-$P/params/W3-SF$SF.params.jsonl}
META=$P/base/sf$SF-$H/meta.json; OUT=$P/w3stores/$TAG${FULL:+/full}${STUDY:+/$STUDY}; mkdir -p $OUT   # FULL=1: the query-stage store (every version via bulk)
SRVSH=$KIT/lib/servers.sh; SRV=$EXTCMP_ROOT/srv
say() { echo "=== $* $(date -u +%FT%TZ)"; }
declare -A DIRS=([postgres]=postgres-w3 [neo4j]=neo4j-w3 [memgraph]=memgraph-w3)
declare -A URIS=([neo4j]=bolt://127.0.0.1:$EXTCMP_NEO4J_PORT [memgraph]=bolt://127.0.0.1:$EXTCMP_MG_PORT)
waitpg() { python - <<'PY'
import time, psycopg
for i in range(600):
    try:
        psycopg.connect(__import__("os").environ["EXTCMP_PG_DSN"]).close(); print("ready postgres", i); break
    except Exception: time.sleep(2)
PY
}
waitbolt() { python -c "
import time, neo4j
for i in range(900):
    try:
        d = neo4j.GraphDatabase.driver('${URIS[$1]}', auth=('neo4j','neo4j') if '$1' == 'neo4j' else ('','')); d.verify_connectivity(); d.close(); print('ready $1', i); break
    except Exception: time.sleep(2)"
}
persist() {  # <sys>: stop the server, pack its data directory, checksum it
  local s=$1; $SRVSH stop $s w3
  rm -rf $SRV/${DIRS[$s]}/lib/import 2>/dev/null
  local t0=$(date +%s)
  tar -C $SRV -cf - ${DIRS[$s]} | zstd -q -T16 -3 -o $OUT/$s.tar.zst -f
  (cd $OUT && sha256sum $s.tar.zst > $s.tar.zst.sha256)
  echo "persisted $s $(du -b $OUT/$s.tar.zst | cut -f1) bytes in $(( $(date +%s) - t0 )) s: $(cat $OUT/$s.tar.zst.sha256)"
}
restore() {  # <sys>: verify the checksum, unpack into ${LOCAL:-${TMPDIR:-/tmp}}
  local s=$1 t0=$(date +%s)
  (cd $OUT && sha256sum -c $s.tar.zst.sha256) || { echo "CHECKSUM MISMATCH $s"; return 1; }
  rm -rf $SRV/${DIRS[$s]}; zstd -q -d -c $OUT/$s.tar.zst | tar -C $SRV -xf -
  echo "restored $s in $(( $(date +%s) - t0 )) s"
}
for sys in ${SYSTEMS:-duckdb postgres neo4j memgraph}; do
has() { case " ${STEPS:-load check} " in *" $1 "*) return 0 ;; esac; return 1; }
  if has load; then
    say load $sys
    case $sys in
      duckdb) python scripts/w3_snb_ingest.py sql --system duckdb --hist $C/hist --base-meta $META ${FULL:+--full} ${PREFIX:+--prefix-days $PREFIX} --db $OUT/duckdb.duckdb --out $OUT/duckdb.load.json
              (cd $OUT && sha256sum duckdb.duckdb > duckdb.duckdb.sha256) ;;
      postgres) $SRVSH wipe postgres w3; $SRVSH start postgres w3 && waitpg
              python scripts/w3_snb_ingest.py sql --system postgres --hist $C/hist --base-meta $META ${FULL:+--full} ${PREFIX:+--prefix-days $PREFIX} --tmp ${LOCAL:-${TMPDIR:-/tmp}} \
                 --dsn "$EXTCMP_PG_DSN" --out $OUT/postgres.load.json \
                 && persist postgres || { echo "LOAD FAILED postgres: not persisted"; $SRVSH stop postgres w3; continue; } ;;
      neo4j|memgraph) python scripts/w3_snb_ingest.py graph --system $sys --hist $C/hist --base-meta $META ${FULL:+--full} ${PREFIX:+--prefix-days $PREFIX} --srv $SRV \
                 --uri ${URIS[$sys]} --servers $SRVSH --neo4j-sif $SIF_DIR/neo4j.sif --out $OUT/$sys.load.json \
                 && persist $sys || { echo "LOAD FAILED $sys: not persisted"; $SRVSH stop $sys w3; continue; } ;;
    esac
  fi
  if has digest; then
    say digest $sys
    case $sys in
      duckdb) python scripts/w3_snb_ingest.py digest --system duckdb --target $OUT/duckdb.duckdb --out $OUT/duckdb.digest.json ;;
      postgres) restore postgres && $SRVSH start postgres w3 && waitpg
              python scripts/w3_snb_ingest.py digest --system postgres --target "$EXTCMP_PG_DSN" --out $OUT/postgres.digest.json
              $SRVSH stop postgres w3 ;;
      neo4j|memgraph) restore $sys && EXTCMP_SERVER_TIMEOUT_S=7200 $SRVSH start $sys w3; waitbolt $sys   # a digest reads every element
              python scripts/w3_snb_ingest.py digest --system $sys --target ${URIS[$sys]} --out $OUT/$sys.digest.json
              $SRVSH stop $sys w3 ;;
    esac
  fi
  if has check; then
    say check $sys
    case $sys in
      duckdb) (cd $OUT && sha256sum -c duckdb.duckdb.sha256); cp $OUT/duckdb.duckdb ${LOCAL:-${TMPDIR:-/tmp}}/check.duckdb; tgt=${LOCAL:-${TMPDIR:-/tmp}}/check.duckdb ;;
      postgres) restore postgres && $SRVSH start postgres w3 && waitpg; tgt="$EXTCMP_PG_DSN" ;;
      neo4j|memgraph) restore $sys && $SRVSH start $sys w3; waitbolt $sys; tgt=${URIS[$sys]} ;;
    esac
    name=$sys; [ $sys = duckdb ] && name=duckdb
    out=$C/$name-v2.jsonl; [ -n "${Q:-}" ] && out=$C/$name-v2-rerun.jsonl   # with Q: merged into <sys>-v2.jsonl
    rm -f $out
    python scripts/w3_snb_run.py run --system $sys --target "$tgt" --params $PAR ${Q:+--queries $(echo $Q | tr : ,)} \
       --settings $(echo ${SETTINGS:-current:asof} | tr : ,) --hist-db $C/w3.duckdb --out $out
    if [ -n "${Q:-}" ]; then python - $C/$name-v2.jsonl $out <<PY
import json, os, sys
new = {}
for l in open(sys.argv[2]):
    x = json.loads(l); new[(x["qid"], x["i"], x["setting"])] = l
old = open(sys.argv[1]).readlines() if os.path.exists(sys.argv[1]) else []
keep = [l for l in old if (lambda x: (x["qid"], x["i"], x["setting"]) not in new)(json.loads(l))]
open(sys.argv[1], "w").writelines(keep + list(new.values())); print("merged", len(new), "into", sys.argv[1])
PY
      rm -f $out; fi
    [ $sys != duckdb ] && $SRVSH stop $sys w3
  fi
done
has check || { say done; exit 0; }
say compare
python scripts/w3_snb_run.py compare --base $C/gnomon.jsonl $(ls $C/*-v2.jsonl) --json $C/compare-v2.json > $C/compare-v2.md
grep -v "equivalent" $C/compare-v2.md | head -60; grep -c equivalent $C/compare-v2.md
say done
