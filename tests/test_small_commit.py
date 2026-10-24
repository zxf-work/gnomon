"""The small-commit path's visibility contract (`GNOMON_SMALL_COMMIT=1`).

* Off by default, and reported by the handle (`designs()`, `small_commit()`).
* The writer always reads its own commits: a read on the writing handle
  publishes what it holds first.
* A reader in another process sees published generations only, so it lags
  the writer by at most `GNOMON_SMALL_COMMIT_N` transactions or
  `GNOMON_SMALL_COMMIT_MS` milliseconds — and by no more.

Durability, event-log bytes and digests under the switch are held in
`tests/test_commit_path_crash.py`.
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

import gnomon

pytest.importorskip("gnomon._engine", reason="native engine extension not built")

_READER = r"""
import sys
import gnomon
s = gnomon.open(sys.argv[1], backend="native", read_only=True)
n = sum(1 for v in s.adapter.all_node_versions() if v.uid.startswith("w"))
s.close()
print(n)
"""


def _env(**kw: str) -> dict[str, str]:
    env = dict(os.environ)
    env["PYTHONPATH"] = str(Path(gnomon.__file__).resolve().parent.parent) + \
        os.pathsep + env.get("PYTHONPATH", "")
    for k in ("GNOMON_SMALL_COMMIT", "GNOMON_SMALL_COMMIT_N", "GNOMON_SMALL_COMMIT_MS"):
        env.pop(k, None)
    env.update(kw)
    return env


def _seen_elsewhere(tmp: Path, store: Path) -> int:
    """What a reader in another process sees: node versions written so far."""
    script = tmp / "reader.py"
    if not script.exists():
        script.write_text(_READER)
    out = subprocess.run([sys.executable, str(script), str(store)], capture_output=True,
                         text=True, timeout=120, env=_env())
    assert out.returncode == 0, out.stderr[-1500:]
    return int(out.stdout.strip())


def _open(path: Path, monkeypatch, n: int, ms: int):
    monkeypatch.setenv("GNOMON_SMALL_COMMIT", "1")
    monkeypatch.setenv("GNOMON_SMALL_COMMIT_N", str(n))
    monkeypatch.setenv("GNOMON_SMALL_COMMIT_MS", str(ms))
    return gnomon.open(path, backend="native")


def test_off_by_default_and_reported_by_the_handle(tmp_path, monkeypatch):
    for k in ("GNOMON_SMALL_COMMIT", "GNOMON_SMALL_COMMIT_N", "GNOMON_SMALL_COMMIT_MS"):
        monkeypatch.delenv(k, raising=False)
    s = gnomon.open(tmp_path / "off", backend="native")
    assert s.adapter.designs()["publish_per_transaction"] is True
    assert s.adapter.small_commit() == {"enabled": False, "n": 64, "ms": 50}
    g = s.adapter.generation
    s.assert_node("w0", "N", {}, vt_s=0)
    assert s.adapter.generation == g + 1, "off, every transaction publishes a generation"
    s.close()
    s = _open(tmp_path / "on", monkeypatch, 16, 5)
    assert s.adapter.designs()["publish_per_transaction"] is False
    assert s.adapter.small_commit() == {"enabled": True, "n": 16, "ms": 5}
    s.close()


def test_the_writer_always_reads_its_own_commits(tmp_path, monkeypatch):
    s = _open(tmp_path / "s", monkeypatch, 1000, 600_000)
    ad = s.adapter
    g = ad.generation
    for i in range(10):
        s.assert_node(f"w{i}", "N", {"i": i}, vt_s=0)
        s.assert_node(f"w{i}", "N", {"i": i + 100}, vt_s=10)
    assert ad.small_commit_pending()[0] == 20
    assert ad.generation == g, "nothing published yet"
    # every kind of read on the writing handle sees every acknowledged write
    got = ad.believed_node_versions("w3")
    assert [v.props["i"] for v in got] == [3, 103]
    assert ad.small_commit_pending()[0] == 0 and ad.generation == g + 1
    s.assert_node("w3", "N", {"i": 999}, vt_s=20)
    # [0,inf) closed, [0,10), [10,inf) closed, [10,20), [20,inf)
    assert sum(1 for v in ad.all_node_versions() if v.uid == "w3") == 5
    assert ad.believed_node_versions("w3")[-1].props["i"] == 999
    s.close()


def test_another_process_lags_by_at_most_n_transactions(tmp_path, monkeypatch):
    store = tmp_path / "s"
    s = _open(store, monkeypatch, 4, 600_000)
    s.assert_node("seed", "N", {}, vt_s=0)
    s.adapter.publish_pending()
    for i in range(3):
        s.assert_node(f"w{i}", "N", {}, vt_s=0)
    assert _seen_elsewhere(tmp_path, store) == 0, "3 < N transactions stay unpublished"
    s.assert_node("w3", "N", {}, vt_s=0)
    assert _seen_elsewhere(tmp_path, store) == 4, "the N-th transaction publishes all of them"
    s.close()


def test_another_process_lags_by_at_most_t_milliseconds(tmp_path, monkeypatch):
    store = tmp_path / "s"
    ms = 300
    s = _open(store, monkeypatch, 1000, ms)
    s.assert_node("seed", "N", {}, vt_s=0)
    s.adapter.publish_pending()
    t0 = time.monotonic()
    s.assert_node("w0", "N", {}, vt_s=0)
    s.assert_node("w1", "N", {}, vt_s=0)
    # an idle writer: the timer publishes once the oldest is T old
    deadline = t0 + ms / 1e3 + 5.0
    seen = 0
    while time.monotonic() < deadline:
        seen = _seen_elsewhere(tmp_path, store)
        if seen == 2:
            break
        time.sleep(0.05)
    waited = time.monotonic() - t0
    assert seen == 2, "the timer never published"
    assert waited < ms / 1e3 + 5.0
    assert s.adapter.small_commit_pending()[0] == 0
    s.close()


def test_close_publishes_everything(tmp_path, monkeypatch):
    store = tmp_path / "s"
    s = _open(store, monkeypatch, 1000, 600_000)
    for i in range(5):
        s.assert_node(f"w{i}", "N", {}, vt_s=0)
    s.close()
    assert _seen_elsewhere(tmp_path, store) == 5
