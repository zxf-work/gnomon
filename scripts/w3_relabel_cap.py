"""Relabel cells that ran into the check cap as kind "timeout", "> cap (CAP s)", in place (one result file).

A cell is relabelled only if its runtime reached the cap (within 10 s) AND its recorded error is a timeout or
termination message (Neo4j TransactionTimedOut / terminated, PostgreSQL statement timeout, Memgraph/AeonG
execution timeout, the kit's own cap, DuckDB interrupt). Any other error stays "error" with its message."""
import json, re, sys
TIMEOUT = re.compile(r"TransactionTimedOut|transaction has been terminated|statement timeout|QueryCanceled"
                     r"|query-execution-timeout|asked to abort|TimeoutError|> cap|INTERRUPT|Interrupted", re.I)
f, cap, note = sys.argv[1], float(sys.argv[2]), sys.argv[3]
out, n, kept = [], 0, 0
for l in open(f):
    x = json.loads(l)
    if not x.get("ok") and x.get("kind") != "timeout" and x.get("ms", 0) >= (cap - 10) * 1e3:
        if TIMEOUT.search(str(x.get("err", "")) + str(x.get("error", ""))):
            x["kind"] = "timeout"; x["note"] = f"> cap ({cap:,.0f} s); {note}"; n += 1
            print("relabelled", x["qid"], x["i"], x["setting"], round(x["ms"] / 1e3))
        else:
            kept += 1
            print("kept as error (not a timeout message)", x["qid"], x["i"], x["setting"], str(x.get("err", ""))[:120])
    out.append(json.dumps(x) + "\n")
open(f, "w").writelines(out)
print("relabelled", n, "kept", kept)
