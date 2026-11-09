""""""
import importlib.util
import sys
import types
from pathlib import Path

for name, attrs in (("gnomon", {}), ("gnomon.core", {}), ("gnomon.core.errors", {"GnomonError": Exception}),
                    ("gnomon.storage", {}), ("gnomon.storage.eventlog", {"REPLAY_GC_EVERY_COMPACTIONS": 8, "EventLog": object, "extend_chain": None})):
    m = types.ModuleType(name)
    m.__dict__.update(attrs)
    sys.modules.setdefault(name, m)
spec = importlib.util.spec_from_file_location("g", Path(__file__).resolve().parents[4] / "scripts" / "w3_ingest_gnomon.py")
g = importlib.util.module_from_spec(spec)
spec.loader.exec_module(g)


def test_kind():
    ends = {100 * g.SEC}
    assert g.kind_of(5, 10, ends) == "snapshot"
    assert g.kind_of(100 * g.SEC, 10, ends) == "day"
    assert g.kind_of(101 * g.SEC, 10, ends) == "corrections"
    assert g.kind_of(50, 10, ends) == "other"


def test_items_and_summary():
    assert g.items_of([{"op": "ingest_events", "events": [1, 2], "nodes": [3]}, {"op": "retract"}]) == 4
    s = g.summarize([{"kind": "day", "items": 10, "log_s": 1.0, "apply_s": 2.0, "commit_s": 1.0, "compact_s": 1.0}])
    assert s["day"]["s"] == 5.0 and s["day"]["items_per_s"] == 2.0
