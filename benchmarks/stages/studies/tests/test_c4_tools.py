""""""
import importlib.util
import json
from pathlib import Path

HERE = Path(__file__).resolve().parents[1] / "tools"


def load(name):
    spec = importlib.util.spec_from_file_location(name, HERE / f"{name}.py")
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


w4 = load("w4_gate")
w5 = load("w5_gate")


def rec(q, i, h="a", status="ok", ms=5.0):
    return {"query": q, "instance": i, "hash": h, "status": status, "ms": ms}


def wj(p, rows):
    p.write_text("\n".join(json.dumps(r) for r in rows) + "\n")


def test_w4_gate_equal_differ_error(tmp_path):
    wj(tmp_path / "answers-postgres.jsonl", [rec("TSR1", "a"), rec("TSR2", "b"), rec("TCR1", "c", ms=2000.0), rec("TCR3", "d", status="timeout")])
    new = [rec("TSR1", "a")]
    assert w4.gate(new, str(tmp_path))[0] == []
    assert w4.gate([rec("TSR1", "a", h="z")], str(tmp_path))[0]                    # differs
    assert w4.gate([rec("TCR1", "c", status="error")], str(tmp_path))[0]            # an error fails
    f, u, _ = w4.gate([rec("TCR3", "d")], str(tmp_path))
    assert not f and u                                                             # no reference: listed


def test_w4_stop_rule(tmp_path):
    wj(tmp_path / "answers-postgres.jsonl", [rec("TSR1", "a", ms=20.0)])
    r = w4.recorded(str(tmp_path))
    assert w4.stop([{"query": "TSR1", "instance": "a", "ok": True}], r) == []
    assert w4.stop([{"query": "TSR1", "instance": "a", "ok": False, "censored": True}], r) == []
    assert w4.stop([{"query": "TSR1", "instance": "a", "ok": False, "error": "boom"}], r)
    assert w4.stop([{"query": "TSR1", "instance": "a", "ok": False, "error": "refused (estimated > 10 s)"}], r)
    assert w4.stop([{"query": "TSR9", "instance": "a", "ok": False, "error": "refused (estimated > 10 s)"}], r) == []


def test_w5_gate(tmp_path):
    def wr(name, rows):
        (tmp_path / name).write_text(json.dumps({"results": rows}))
    good = {"query": "q", "instance": "i", "ok": True, "hash": "a", "wall_ms": 5}
    for v in ("reg", "bel"):
        wr(f"verify-native-{v}.json", [good])
        for s in ("duckdbsql", "postgres", "neo4j"):
            wr(f"verify-{s}-{v}.json", [good])
    assert w5.gate(str(tmp_path))[0] == []
    wr("verify-postgres-reg.json", [dict(good, hash="b")])
    assert w5.gate(str(tmp_path))[0]
    wr("verify-postgres-reg.json", [dict(good, hash="b", proj_hash="p")])
    wr("verify-native-reg.json", [dict(good, proj_hash="p")])
    assert w5.gate(str(tmp_path))[0] == []                                          # equal on the projection
