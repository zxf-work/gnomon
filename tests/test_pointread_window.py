"""The windowed point read the write path uses (`believed_*_versions_overlapping`).

On every backend it must equal the believed history filtered to the window,
in the same order — the native engine answers it by seeking
(`crates/gnomon-core/src/pointread.rs`), every other backend by the base
class's filter. And switching the write path onto it must not change a single
version: the store written through it is checked version by version against
the same ops replayed with the old whole-history read patched back in.
"""

from __future__ import annotations

import random

import pytest

import gnomon
from gnomon.core.errors import GnomonError
from gnomon.core.model import OPEN_END, EntityRef, edge_eid
from gnomon.storage.base import StorageAdapter
from gnomon.storage.eventlog import replay


def _write_history(store, seed: int = 7, n: int = 400) -> None:
    rng = random.Random(seed)
    for i in range(12):
        store.assert_node(f"u{i}", "P", {"p": 0}, vt_s=0)
    for d in range(10):
        store.assert_edge("u0", f"u{d % 12}", "K", {"w": 0}, vt_s=0, disc=f"#{d}")
    for k in range(1, n):
        try:
            _one(store, rng, k)
        except GnomonError:
            pass


def _one(store, rng, k: int) -> None:
    if True:
        r = rng.random()
        u = f"u{min(int(rng.paretovariate(1.1)) - 1, 11)}"
        if r < 0.55:  # an update at a later valid time: the history grows
            store.assert_node(u, "P", {"p": k}, vt_s=10 * k)
        elif r < 0.7:  # a whole-line replacement
            store.assert_node(u, "P", {"p": -k}, vt_s=0)
        elif r < 0.8:
            store.correct(EntityRef(kind="node", uid=u), {"c": k}, vt_s=5 * k, vt_e=5 * k + 50)
        elif r < 0.9:
            d = rng.randrange(10)
            store.assert_edge("u0", f"u{d % 12}", "K", {"w": k}, vt_s=7 * k, disc=f"#{d}")
        else:
            d = rng.randrange(10)
            store.retract(EntityRef(kind="edge", src="u0", dst=f"u{d % 12}", rel_type="K",
                                    disc=f"#{d}"), 3 * k + 1)


WINDOWS = [(0, OPEN_END), (0, 1), (1, 2), (49, 51), (100, 101), (1_000, 1_001), (1_995, 2_100),
           (3_000, OPEN_END), (3_999, 4_000), (-10, 0), (OPEN_END - 1, OPEN_END)]


@pytest.mark.parametrize("backend", ["native", "duckdb"])
def test_the_window_read_is_the_filtered_history(tmp_path, backend):
    if backend == "native":
        pytest.importorskip("gnomon._engine", reason="native engine extension not built")
    else:
        pytest.importorskip("duckdb")
    store = gnomon.open(tmp_path / "s", backend=backend)
    _write_history(store)
    ad = store.adapter
    tts = [b["tt"] for b, _, _ in store.eventlog.batches_from(0)]
    for as_of in [OPEN_END] + tts[::37]:
        for i in range(13):
            hist = ad.believed_node_versions(f"u{i}", as_of)
            for lo, hi in WINDOWS:
                want = [v for v in hist if v.vt_s < hi and lo < v.vt_e]
                assert ad.believed_node_versions_overlapping(f"u{i}", lo, hi, as_of) == want
        for d in range(10):
            eid = edge_eid("u0", f"u{d % 12}", "K", f"#{d}")
            hist = ad.believed_edge_versions(eid, as_of)
            for lo, hi in WINDOWS:
                want = [v for v in hist if v.vt_s < hi and lo < v.vt_e]
                assert ad.believed_edge_versions_overlapping(eid, lo, hi, as_of) == want
    store.close()


def test_the_write_path_writes_identical_versions_through_the_window_read(tmp_path, monkeypatch):
    pytest.importorskip("gnomon._engine", reason="native engine extension not built")
    a = gnomon.open(tmp_path / "a", backend="native")
    _write_history(a, seed=11, n=500)
    log = a.path / "eventlog.jsonl"
    rows_a = sorted(v.to_json()["vid"] for v in a.adapter.all_node_versions()) + \
        sorted(v.to_json()["vid"] for v in a.adapter.all_edge_versions())
    dig_a = (a.digest(), a.adapter.content_digest())
    a.close()

    # the same log, replayed with the write path reading the whole history
    from gnomon.storage.native.adapter import NativeAdapter
    monkeypatch.setattr(NativeAdapter, "believed_node_versions_overlapping",
                        StorageAdapter.believed_node_versions_overlapping)
    monkeypatch.setattr(NativeAdapter, "believed_edge_versions_overlapping",
                        StorageAdapter.believed_edge_versions_overlapping)
    b = gnomon.open(tmp_path / "b", backend="native")
    replay(log, b.adapter)
    rows_b = sorted(v.to_json()["vid"] for v in b.adapter.all_node_versions()) + \
        sorted(v.to_json()["vid"] for v in b.adapter.all_edge_versions())
    assert rows_b == rows_a
    assert (b.digest(), b.adapter.content_digest()) == dig_a
    b.close()
