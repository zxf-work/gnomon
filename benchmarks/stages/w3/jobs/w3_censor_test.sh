#!/bin/bash
# Censor-once test on a tiny case (login node or a short a shared node job): DuckDB on the SF0.1 store, a ceiling of 0.03 s so
# that some instances reach it at their first call and others do not, then the per-call audit of tools/w3_timed.py.
source ${W3_STAGE_ROOT:-${GNOMON_BENCH_ROOT}/final/w3}/src/benchmarks/stages/w3/jobs/w3_lib.sh
pt_config SF0.1
T=${1:-$(mktemp -d /tmp/w3censor.XXXXXX)}; mkdir -p $T
cp $FULL/duckdb.duckdb $T/db.duckdb
export TR_CEILING_S=0.03 W3_CAP_S=10
$PYV $TOOLS/w3_timed.py run --system duckdb --target $T/db.duckdb --params $PARAMS \
    --cells IS1,IS3,IC2,IC6,IC8,BI2,BI8,BIH-VER --settings current,asof --instances 0,1,2 --hist-db $HIST \
    --rotation T --out $T/block.json --jsonl $T/calls.jsonl --label "censor-once test" 2>&1 | tail -n 4
$PYV $TOOLS/w3_timed.py audit $T/calls.jsonl; arc=$?
$PYV - $T/calls.jsonl $T/block.json <<'PY'
import json, sys, collections
seq = collections.defaultdict(list)
for l in open(sys.argv[1]):
    r = json.loads(l); seq[(r["query"], r["setting"], r["instance"])].append((r["phase"], r["status"]))
pat = collections.Counter(tuple(s) for s in seq.values())
for p, n in pat.most_common():
    print(n, "instances:", " ".join(f"{a}:{b}" for a, b in p))
b = json.load(open(sys.argv[2]))
print("block censor_once:", {k: (v if k != "audit" else {x: (y if x != "violations" else len(y)) for x, y in v.items()}) for k, v in b["censor_once"].items() if k != "rule"})
PY
echo "audit rc=$arc"; exit $arc
