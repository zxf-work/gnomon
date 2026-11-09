""""""
import importlib.util
import json
import sys
import types
from pathlib import Path

for name, attrs in (("gnomon", {}), ("gnomon.core", {}), ("gnomon.core.errors", {"GnomonError": Exception}),
                    ("gnomon.storage", {}), ("gnomon.storage.eventlog", {"REPLAY_GC_EVERY_COMPACTIONS": 8, "EventLog": object, "extend_chain": None})):
    m = types.ModuleType(name)
    m.__dict__.update(attrs)
    sys.modules.setdefault(name, m)
HERE = Path(__file__).resolve()


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


t = load("ing_table", HERE.parents[1] / "tools" / "ing_table.py")
g = load("g2", HERE.parents[4] / "scripts" / "w3_ingest_gnomon.py")


def test_peaks_by_phase():
    ss = [{"t": 1, "pss": True, "by": {"client": {"rss_kb": 1048576, "pss_kb": 1048576, "hwm_kb": 0}}},
          {"t": 5, "pss": False, "by": {"client": {"rss_kb": 3 * 1048576, "pss_kb": 0, "hwm_kb": 0}, "postgres": {"rss_kb": 1048576, "pss_kb": 0, "hwm_kb": 0}}},
          {"t": 20, "pss": False, "by": {"client": {"rss_kb": 2 * 1048576, "pss_kb": 0, "hwm_kb": 0}}}]
    pk = t.phase_peaks({"marks": {"start": 0, "bulk_end": 10, "end": 30}}, ss)
    assert pk["phase1"] == (4.0, 1.0) and pk["phase2"] == (2.0, None)


def test_log_check(tmp_path):
    a, b = tmp_path / "a", tmp_path / "b"
    hdr = json.dumps({"format": "x"})
    r1 = json.dumps({"tt": 1, "ops": [{"op": "ingest_events", "events": [1, 2]}]})
    r2 = json.dumps({"tt": 2, "ops": [{"op": "retract"}]})
    a.write_text("\n".join([hdr, r1, r2]) + "\n")
    b.write_text("\n".join([json.dumps({"format": "y"}), json.dumps({"tt": 1, "batch_id": 9, "ops": [{"op": "ingest_events", "events": [1, 2]}]}), r2]) + "\n")
    c = g.log_check(str(a), str(b), None)
    assert c["input_records"] == 2 and c["written_records"] == 2 and c["input_items"] == 3 and c["equal"]
    assert g.log_check(str(a), str(b), 1)["input_records"] == 1
    b.write_text("\n".join([hdr, r1]) + "\n")
    assert not g.log_check(str(a), str(b), None)["equal"]
