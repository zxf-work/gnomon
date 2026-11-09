"""The load instrument in `scripts/eval_harness.py`: "no script in this
repo records a `load_s`/`ingest_s` field for the external systems ...
load-time forecasts cannot be *scored* without adding one."

Covers the receipt shape (`{system: {load_s, rows, bytes, runtime}}`) added
to `manifest()`'s `load` block — additive only, never touching the
query-timing paths (`run_system`'s per-query `_measure` loop is untouched;
only the store-open/load bracket around it is now optionally instrumented).

Tested against the two embedded backends this file's tests can run on a laptop
(native, duckdb); `postgres`/`clickhouse`/`neo4j`/`memgraph` need a running
server this environment does not have, so their `load_receipt` wiring is
exercised at the unit level (the docstrings' claims about what each
function times) rather than end-to-end here.
"""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parent.parent


def _load_module(name: str, relpath: str) -> Any:
    scripts_dir = str(ROOT / "scripts")
    if scripts_dir not in sys.path:
        sys.path.insert(0, scripts_dir)
    spec = importlib.util.spec_from_file_location(name, ROOT / relpath)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


H = _load_module("eval_harness", "scripts/eval_harness.py")


def test_dir_bytes_sums_files_recursively(tmp_path):
    (tmp_path / "a").write_bytes(b"x" * 10)
    sub = tmp_path / "sub"
    sub.mkdir()
    (sub / "b").write_bytes(b"y" * 20)
    assert H._dir_bytes(tmp_path) == 30


def test_dir_bytes_missing_dir_is_none_not_a_crash(tmp_path):
    assert H._dir_bytes(tmp_path / "does-not-exist") is None


def test_external_runtime_defaults_to_docker(monkeypatch):
    monkeypatch.delenv("GNOMON_EXTERNAL_RUNTIME", raising=False)
    assert H._external_runtime() == "docker"


def test_external_runtime_honours_env(monkeypatch):
    monkeypatch.setenv("GNOMON_EXTERNAL_RUNTIME", "apptainer")
    assert H._external_runtime() == "apptainer"


@pytest.fixture(scope="module")
def tiny_native_store(tmp_path_factory):
    import gnomon
    out = tmp_path_factory.mktemp("load-receipt") / "store"
    store = gnomon.open(out, backend="native")
    store.ingest_events([H._event(i, 500) for i in range(500)])
    store.close()
    return out


def test_run_system_populates_load_receipt(tiny_native_store):
    """`load_receipt` is opt-in (`None` by default, so every pre-existing
    call site is unaffected) — when given, `run_system` fills in rows/bytes/
    runtime from the store it already opened, plus the caller's own
    `load_s` measurement (the embedded backends load entirely inside the
    caller's `load_store` bracket, so nothing here re-times that)."""
    queries = H.registry(0, 500, 1, ("n0", "n1"))
    load: dict[str, Any] = {}
    results = H.run_system("native", tiny_native_store, queries,
                           load_receipt=load, load_s=1.234)
    assert results  # unaffected: still answers every query
    assert load["native"]["load_s"] == 1.234
    assert load["native"]["runtime"] == "embedded"
    assert load["native"]["rows"] > 0
    assert load["native"]["bytes"] > 0


def test_run_system_without_load_receipt_is_unaffected(tiny_native_store):
    """Omitting `load_receipt` reproduces every prior call site exactly —
    no side effect, no extra store walk."""
    queries = H.registry(0, 500, 1, ("n0", "n1"))
    results = H.run_system("native", tiny_native_store, queries)
    assert results


# --------------------------------------------------------------------------- #
# end-to-end: the manifest's "load" block, via the real CLI entry point
# --------------------------------------------------------------------------- #

def test_cli_json_receipt_carries_a_load_block_per_system(tmp_path):
    out = tmp_path / "receipt.json"
    subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "eval_harness.py"),
         "--scale", "500", "--systems", "native,duckdb", "--json", str(out)],
        cwd=ROOT, check=True, capture_output=True, text=True, timeout=120)
    data = json.loads(out.read_text())
    load = data["manifest"]["load"]
    assert set(load) == {"native", "duckdb"}
    for sys_name in ("native", "duckdb"):
        block = load[sys_name]
        assert block["runtime"] == "embedded"
        assert isinstance(block["load_s"], (int, float))
        assert block["load_s"] >= 0
        assert block["rows"] > 0
        assert block["bytes"] > 0
