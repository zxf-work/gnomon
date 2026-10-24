"""Every operator is reachable through `call_operator` after a plain
`import gnomon`, while importing the package still registers nothing.

The registry fills lazily (`ensure_all_registered`). The surfaces that
dispatch fill it first; a direct engine caller did not, and was told that
`paths` (or any operator) is unknown until something else had filled it.
Each case runs in a fresh interpreter, so the registry really starts empty.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

SCRIPT = r"""
import json, sys, tempfile
import gnomon
from gnomon.temporal.algebra import REGISTRY, call_operator
before = len(REGISTRY)
st = gnomon.open(tempfile.mkdtemp() + "/s")
calls = {
    "paths": {"sources": {"uids": ["u1"]}, "t_valid": 5},
    "state_series": {"kind": "node", "window": {"t_a": 0, "t_b": 9}, "stride": 3,
                     "agg": {"fn": "count"}},
    "diff_belief": {"t1": 0, "t2": 5, "window": {"t_a": 0, "t_b": 9}},
    "match": {"pattern": {"nodes": [{"var": "a"}]},
              "return": {"rows": [{"as": "u", "expr": {"col": "a.uid"}}]},
              "scope": {"vt": {"instant": 1}}},
    "aggregate_events": {"group_by": [], "window": {"t_a": 0, "t_b": 9},
                         "aggregates": [{"agg": "sum", "of": "vt_s"}]},
    "version_history": {"kind": "node", "ref": {"uid": "u1"},
                        "window": {"t_a": 0, "t_b": 9}},
}
st.adapter.begin()
st.adapter.apply_ops([{"op": "assert_node", "uid": "u1", "label": "N", "props": {},
                       "vt_s": 0, "vt_e": 9}], 1)
st.adapter.commit()
out = {}
for op in sys.argv[1:]:
    try:
        out[op] = call_operator(st.adapter, op, calls[op])["op"]
    except Exception as e:
        out[op] = f"{type(e).__name__}: {e}"
try:
    call_operator(st.adapter, "no_such_op", {})
except Exception as e:
    out["no_such_op"] = type(e).__name__
st.close()
print(json.dumps({"before": before, "out": out}))
"""

OPS = ["paths", "state_series", "diff_belief", "match", "aggregate_events",
       "version_history"]


def test_a_plain_import_reaches_every_operator_and_registers_nothing(tmp_path):
    for op in OPS:
        r = subprocess.run([sys.executable, "-c", SCRIPT, op], capture_output=True,
                           text=True, timeout=120, cwd=tmp_path,
                           env={**os.environ, "PYTHONPATH": str(ROOT)})
        assert r.returncode == 0, r.stdout + r.stderr
        got = json.loads(r.stdout.strip().splitlines()[-1])
        assert got["before"] == 0, "importing gnomon must register nothing"
        assert got["out"][op] == op, got
        assert got["out"]["no_such_op"] == "InvalidArgError"
