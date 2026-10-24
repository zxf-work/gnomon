"""The traversal's effective-tt_e column (`GNOMON_TT_E_COLUMN`).

`neighbors_batch` decides belief on a segment with closes from a cached column
of committed effective `tt_e`, consulting the open batch's pending closes
first. These tests hold it to the per-call path the switch restores: on a
corrected store, and on the small-commit path, where the writer's engine batch
stays open across transactions and carries closes no generation has published.
"""

from __future__ import annotations

import numpy as np
import pytest

import gnomon
from gnomon.core.model import EntityRef

pytest.importorskip("gnomon._engine", reason="native engine extension not built")

N = 60


def _write(store) -> None:
    for i in range(N):
        store.assert_node(f"u{i}", "N", {}, vt_s=0)
    for i in range(N):
        for d in (1, 2, 5):
            store.assert_edge(f"u{i}", f"u{(i + d) % N}", "K", {"w": d}, vt_s=0, disc=f"{d}")
    for i in range(0, N, 3):  # closes, at successive transaction times
        store.retract(EntityRef(kind="edge", src=f"u{i}", dst=f"u{(i + 1) % N}", rel_type="K",
                                disc="1"), 10 * i + 5)


#: A frontier small enough that the closes of a segment holding the whole
#: graph outnumber the adjacency rows it tests, so the column is the path
#: `neighbors_batch` takes there (the small-commit writer publishes the graph
#: as one segment, and a 60-vertex frontier keeps that segment's cheap ranges).
FEW = ("u0", "u3")


def _nbrs(ad, as_of, uids=None):
    uids = [f"u{i}" for i in range(N)] if uids is None else list(uids)
    us = np.array(sorted(ad.dense_ids(uids)), dtype=np.int64)
    out = {}
    for direction in ("out", "in", "both"):
        d = ad._store.neighbors_batch(us, direction, int(as_of), None, None, None)
        out[direction] = {k: list(v) for k, v in d.items()}
    return out


def _clean(monkeypatch):
    for k in ("GNOMON_SMALL_COMMIT", "GNOMON_SMALL_COMMIT_N", "GNOMON_SMALL_COMMIT_MS", "GNOMON_TT_E_COLUMN"):
        monkeypatch.delenv(k, raising=False)


def test_the_column_and_the_per_call_path_answer_the_same(tmp_path, monkeypatch):
    _clean(monkeypatch)
    s = gnomon.open(tmp_path / "s", backend="native")
    _write(s)
    tts = [b["tt"] for b, _, _ in s.eventlog.batches_from(0)]
    s.close()
    answers = {}
    for flag in ("1", "0"):
        monkeypatch.setenv("GNOMON_TT_E_COLUMN", flag)
        r = gnomon.open(tmp_path / "s", backend="native", read_only=True)
        assert r.adapter.designs()["tt_e_column"] is (flag == "1")
        answers[flag] = [_nbrs(r.adapter, t) for t in tts[::17] + [2 ** 62 - 1]]
        stats = r.adapter._store.tt_e_column_stats()
        if flag == "1":
            assert stats["builds"] > 0 and stats["bytes"] <= stats["budget"]
        else:
            assert stats["builds"] == 0
        r.close()
    assert answers["1"] == answers["0"]


def _small_commit_writer(path, log, monkeypatch, column: str):
    """A small-commit writer that has published the graph and half the closes
    (columns built) and holds the other half acknowledged but unpublished."""
    from gnomon.storage.eventlog import EventLog
    monkeypatch.setenv("GNOMON_SMALL_COMMIT", "1")
    monkeypatch.setenv("GNOMON_SMALL_COMMIT_N", "100000")
    monkeypatch.setenv("GNOMON_SMALL_COMMIT_MS", "600000")
    monkeypatch.setenv("GNOMON_TT_E_COLUMN", column)
    w = gnomon.open(path, backend="native")
    ad = w.adapter
    batches = list(EventLog(log, create=False).batches_from(0))
    n_retracts = sum(1 for b, _, _ in batches if b["ops"][0]["op"] == "retract")
    half = len(batches) - n_retracts // 2
    for k, (b, _, raw) in enumerate(batches):
        ad.begin()
        ad.apply_ops(b["ops"], b["tt"], raw=raw)
        ad.commit()
        if k + 1 == half:
            ad.publish_pending()
            _nbrs(ad, 2 ** 62 - 1, FEW)
    assert ad.small_commit_pending()[0] > 0 and ad._store.in_batch()
    return w


def test_unpublished_small_commit_closes_are_seen_through_the_batch_overlay(tmp_path, monkeypatch):
    """The small-commit writer's open engine batch carries closes no generation
    has published. The engine read on that handle (below the publish barrier)
    must answer with the column exactly as without it, and after the publish
    the column must be rebuilt for the closes that landed."""
    _clean(monkeypatch)
    ref = gnomon.open(tmp_path / "ref", backend="native")
    _write(ref)
    log = ref.path / "eventlog.jsonl"
    tts = [b["tt"] for b, _, _ in ref.eventlog.batches_from(0)]
    ref.close()
    on = _small_commit_writer(tmp_path / "on", log, monkeypatch, "1")
    off = _small_commit_writer(tmp_path / "off", log, monkeypatch, "0")
    points = tts[::13] + [2 ** 62 - 1]
    built = on.adapter._store.tt_e_column_stats()["builds"]
    assert built > 0 and off.adapter._store.tt_e_column_stats()["builds"] == 0
    assert [_nbrs(on.adapter, t, FEW) for t in points] == [_nbrs(off.adapter, t, FEW) for t in points], \
        "pending closes in the open small-commit batch were read differently"
    assert [_nbrs(on.adapter, t) for t in points] == [_nbrs(off.adapter, t) for t in points]
    assert on.adapter._store.tt_e_column_stats()["builds"] == built, \
        "unpublished closes rebuilt a committed column"
    for w in (on, off):
        w.adapter.publish_pending()
        assert not w.adapter._store.in_batch()
    assert [_nbrs(on.adapter, t, FEW) for t in points] == [_nbrs(off.adapter, t, FEW) for t in points]
    assert [_nbrs(on.adapter, t) for t in points] == [_nbrs(off.adapter, t) for t in points]
    assert on.adapter._store.tt_e_column_stats()["builds"] > built, \
        "published closes did not rebuild a column"
    on.close()
    off.close()
