"""(GNOMON_STORE_WARM_OPEN, on by default; 0 keeps the lazy path) builds at open the
store-wide structures a first query used to build, and changes no answer.

Each arm opens the same store read-only in this process, with the switch set
before the open (the engine reads it per open). The warm arm also forces the
parallel dictionary load (GNOMON_DICT_FAST_MIN=1), which a store this small
would not otherwise take. Every answer, the statistics, the `match` compiler's
label and end counts must be identical. (A writer is never warmed: the
engine's own test, crates/gnomon-core/tests/warm_open.rs.)
"""

from __future__ import annotations

import pytest

from gnomon.storage.native import NativeAdapter

from .test_window_fixes import SCOPES, _answer, _mutual, _same_tag, _triangles, _world

pytest.importorskip("gnomon._engine", reason="native engine extension not built")


@pytest.fixture(scope="module")
def world():
    a = _world(58)
    yield a.path
    a.close()


def _readings(path, monkeypatch, warm: str):
    monkeypatch.setenv("GNOMON_STORE_WARM_OPEN", warm)
    if warm == "1":
        monkeypatch.setenv("GNOMON_DICT_FAST_MIN", "1")
    else:
        monkeypatch.delenv("GNOMON_DICT_FAST_MIN", raising=False)
    h = NativeAdapter(path, read_only=True)
    out = {"warm_us": h._store.warm_open_us()}
    out["stats"] = h.stats()
    out["labels"] = h.match_label_counts()
    out["ends"] = h.match_rel_ends()
    for name, sc in SCOPES.items():
        for q, fn in (("mutual", _mutual), ("same_tag", _same_tag), ("triangles", _triangles)):
            out[f"{q}/{name}"] = _answer(h, fn(sc))
    h.close()
    return out


def test_the_warm_open_answers_as_the_lazy_path(world, monkeypatch):
    lazy = _readings(world, monkeypatch, "0")
    warm = _readings(world, monkeypatch, "1")
    assert lazy.pop("warm_us") is None
    ph = warm.pop("warm_us")
    assert ph is not None and ph["complete"] == 1, ph
    assert ph["segments"] > 0 and ph["threads"] >= 1
    assert warm.keys() == lazy.keys()
    for k in lazy:
        assert warm[k] == lazy[k], k
    assert sum(lazy["labels"].values()) == lazy["stats"]["n_node_versions"]


def test_the_default_is_warm(world, monkeypatch):
    monkeypatch.delenv("GNOMON_STORE_WARM_OPEN", raising=False)
    h = NativeAdapter(world, read_only=True)
    try:
        ph = h._store.warm_open_us()
        assert ph is not None and ph["complete"] == 1
    finally:
        h.close()
