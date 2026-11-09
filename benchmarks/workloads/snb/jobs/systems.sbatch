#!/bin/bash
#SBATCH -J w3-systems
#SBATCH -N 1
#SBATCH --ntasks-per-node=32
#SBATCH -t 08:00:00
#SBATCH -o systems-%j.out
# W3 answer check, server systems: PostgreSQL, ClickHouse, Neo4j, Memgraph, AeonG (native TT + property form),
# durable mode (EXTCMP_DURABLE=1), each loaded from the same history tables, compared with Gnomon's answers.
set -uo pipefail
export EXTCMP_JOB_SAFE=1   # per-job ports and instance names
P=${GNOMON_BENCH_ROOT}/w3-snb
source $P/site.env; cd $EXTCMP_REPO; source .venv/bin/activate
export W3_EXT=$P/ext EXTCMP_ROOT=${LOCAL:-${TMPDIR:-/tmp}}/w3srv EXTCMP_DURABLE=1 TMPDIR=${LOCAL:-${TMPDIR:-/tmp}} EXTCMP_NEO4J_PLUGINS='["graph-data-science"]' W3_CAP_S=${W3_CAP_S:-900} EXTCMP_SERVER_TIMEOUT_S=${W3_CAP_S:-900}
mkdir -p $EXTCMP_ROOT/scratch
SF=${SF:-0.1}; R=${R:-10}; TAG=SF$SF-h1-r$R; C=$P/check/$TAG; PAR=$P/params/W3-SF$SF.params.jsonl
SRVSH=$KIT/lib/servers.sh
say() { echo "=== $* $(date -u +%FT%TZ)"; }
waitfor() { python - "$1" <<'PY'
import sys, time
s = sys.argv[1]
for i in range(600):
    try:
        if s == "postgres":
            import os, psycopg; psycopg.connect(os.environ["EXTCMP_PG_DSN"]).close()
        elif s == "clickhouse":
            import os, urllib.request; urllib.request.urlopen("http://127.0.0.1:%s/ping" % os.environ["EXTCMP_CH_HTTP_PORT"], timeout=5).read()
        else:
            import neo4j
            import os; port = {"neo4j": os.environ["EXTCMP_NEO4J_PORT"], "memgraph": os.environ["EXTCMP_MG_PORT"], "aeong": os.environ["EXTCMP_AEONG_PORT"]}[s]
            d = neo4j.GraphDatabase.driver(f"bolt://127.0.0.1:{port}", auth=("neo4j", "neo4j") if s in ("neo4j", "aeong") else ("", ""))
            d.verify_connectivity(); d.close()
        print("ready", s, i); sys.exit(0)
    except Exception as e:
        time.sleep(2)
print("NOT READY", s); sys.exit(1)
PY
}
Q=$(echo ${QUERIES:-} | tr : ,)   # colon list (sbatch --export splits on commas); with Q set, results merge into <sys>.jsonl
for sys in ${SYSTEMS:-postgres clickhouse neo4j memgraph aeong}; do
  # AeonG loads and queries on one server (its native transaction times do not survive a restart). It ignores
  # client-side timeouts, so its server timeout is the cap: AEONG_CHUNK > 0 (the default) splits the load into
  # bounded transactions with a marker-node time map; AEONG_CHUNK=0 is the earlier one-transaction-per-group load,
  # which needs the lifted timeout (14,400 s) and leaves the per-call cap unenforced
  st=${EXTCMP_SERVER_TIMEOUT_S}; AEONG_CHUNK=${AEONG_CHUNK:-5000}
  [ $sys = aeong ] && [ "$AEONG_CHUNK" = 0 ] && st=14400
  say start $sys; EXTCMP_SERVER_TIMEOUT_S=$st $SRVSH start $sys w3 || { echo "start failed $sys"; continue; }
  waitfor $sys || { $SRVSH logs $sys w3 60; $SRVSH stop $sys w3; continue; }
  case $sys in
    postgres) say load; python scripts/w3_snb_systems.py pg-load --hist $C/hist --dsn "$EXTCMP_PG_DSN" --tmp ${LOCAL:-${TMPDIR:-/tmp}} | cut -c1-1500
              runs="postgres:$EXTCMP_PG_DSN" ;;
    clickhouse) say load; python scripts/w3_snb_systems.py ch-load --hist $C/hist --port $EXTCMP_CH_HTTP_PORT | cut -c1-1500; runs="clickhouse:$EXTCMP_CH_HTTP_PORT" ;;
    neo4j) say load; python scripts/w3_snb_systems.py cypher-load --hist $C/hist --uri bolt://127.0.0.1:$EXTCMP_NEO4J_PORT --system neo4j | cut -c1-1500; runs="neo4j:bolt://127.0.0.1:$EXTCMP_NEO4J_PORT" ;;
    memgraph) say load; python scripts/w3_snb_systems.py cypher-load --hist $C/hist --uri bolt://127.0.0.1:$EXTCMP_MG_PORT --system memgraph | cut -c1-1500; runs="memgraph:bolt://127.0.0.1:$EXTCMP_MG_PORT" ;;
    aeong) say load
           if ! python scripts/w3_snb_systems.py aeong-load --hist $C/hist --uri bolt://127.0.0.1:$EXTCMP_AEONG_PORT --chunk $AEONG_CHUNK | cut -c1-1500; then
             say "aeong load FAILED: no queries on a partial store"; $SRVSH stop $sys w3; continue; fi
           if [ "${AEONG_CEILING_CHECK:-0}" = 1 ]; then say ceiling
             python scripts/w3_snb_systems.py aeong-ceiling --hist $C/hist --uri bolt://127.0.0.1:$EXTCMP_AEONG_PORT --ceiling $st --out $C/aeong-ceiling-$SLURM_JOB_ID.json; fi
           runs="aeong:bolt://127.0.0.1:$EXTCMP_AEONG_PORT,$C/hist"   # AEONG_FORMS=aeong: the native form only
           [ "${AEONG_FORMS:-both}" = both ] && runs="$runs|aeong-prop:bolt://127.0.0.1:$EXTCMP_AEONG_PORT,$C/hist" ;;
  esac
  IFS='|'; for r in $runs; do unset IFS; name=${r%%:*}; tgt=${r#*:}
    out=$C/$name.jsonl; [ -n "$Q" ] && out=$C/$name-rerun.jsonl
    rm -f $out; say run $name
    python scripts/w3_snb_run.py run --system $name --target "$tgt" --params $PAR ${Q:+--queries $Q} --settings $(echo ${SETTINGS:-current,asof,window} | tr : ,) --hist-db $C/w3.duckdb --out $out
    if [ -n "$Q" ]; then python - $C/$name.jsonl $out <<PY
import json, os, sys
new = {}
for l in open(sys.argv[2]):
    x = json.loads(l); new[(x["qid"], x["i"], x["setting"])] = l
old = [l for l in open(sys.argv[1])] if os.path.exists(sys.argv[1]) else []
keep = [l for l in old if (lambda x: (x["qid"], x["i"], x["setting"]) not in new)(json.loads(l))]
open(sys.argv[1], "w").writelines(keep + list(new.values())); print("merged", len(new))
PY
      rm -f $out; fi
    if [ -e $C/params-degen.jsonl ] && [ "${DEGEN:-1}" = 1 ]; then   # window texts at a 1-us window = the as-of answer
      rm -f $C/$name-degen.jsonl; say degen $name
      python scripts/w3_snb_run.py run --system $name --target "$tgt" --params $C/params-degen.jsonl ${Q:+--queries $Q} --settings window --out $C/$name-degen.jsonl
      python scripts/w3_degen_cmp.py $C/$name.jsonl $C/$name-degen.jsonl | grep -v " same" | head -40
    fi
  done; unset IFS
  $SRVSH bytes $sys w3 2>/dev/null; $SRVSH stop $sys w3
done
say compare
ls $C/*.jsonl
python scripts/w3_snb_run.py compare --base $C/gnomon.jsonl $(ls $C/*.jsonl | grep -v "gnomon\|degen\|-w.jsonl") --json $C/compare-all.json > $C/compare-all.md
python scripts/w3_coverage.py --dir $C --out $C/coverage.md > /dev/null
grep -c equivalent $C/compare-all.md
say done
