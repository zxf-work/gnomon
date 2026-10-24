"""The raw-record fast path takes `ingest_events` ops with a
`nodes` array (the shape of the external benchmark snapshots) and writes exactly what the Python row
build writes.

Each assertion compares a store written with the fast path on (the default)
against one written with `GNOMON_INGEST_RAW=0`: same content digest, same dense
ids, the same store digest on replay, and the same refusals. The engine
counters show which ops the fast path took, so the comparison is not vacuous:
props the fast path cannot promise to render as Python does (a float, an
integer past 64 bits) and per-record overrides fall back.
"""

from __future__ import annotations

import pytest

import gnomon
from gnomon.core.errors import GnomonError
from gnomon.storage.eventlog import replay

native = pytest.importorskip("gnomon.storage.native")

PROPS = [
    {"name": "plain", "id": 7},
    {"zeta": 1, "alpha": {"y": [1, "two", None, True, False], "x": {}}, "Émile": "café"},
    {"ctl": "a\tb\nc\rd\x08e\x0cf\x01g\x1fh", "q": 'say "hi" \\ there', "slash": "a/b"},
    {"emoji": "\U0001f600", "cjk": "漢字", "rtl": "שלום", "ls": "  ", "del": "\x7f"},
    {"neg": -5, "big": 2 ** 62, "zero": 0, "list": [[], [{}], [-1, 2 ** 63 - 1]]},
    {},
    None,
]


def _nodes(prefix: str, n: int, with_props: bool = True) -> list[dict]:
    out = []
    for i in range(n):
        rec = {"uid": f"{prefix}{i}", "label": "Thing" if i % 3 else "Other", "vt_s": 10 * i}
        if i % 4 == 1:
            rec["vt_e"] = 10 * i + 7
        p = PROPS[i % len(PROPS)] if with_props else None
        if p is not None or i % 2:
            rec["props"] = p
        out.append(rec)
    return out


def _batches() -> list[list[dict]]:
    return [
        # nodes only, every props shape the fast path renders itself
        [{"op": "ingest_events", "events": [], "nodes": _nodes("a", 40)}],
        # nodes and events together: an explicit endpoint is not auto-created
        [{"op": "ingest_events", "nodes": _nodes("b", 6),
          "events": [{"src": "b1", "dst": "c1", "rel_type": "R", "vt_s": 5},
                     {"src": "c2", "dst": "b4", "rel_type": "S", "vt_s": 3, "vt_e": 9},
                     {"src": "a3", "dst": "c1", "rel_type": "R", "vt_s": 1}],
          "source": "loader", "provenance_ref": "p7", "node_label": "Auto"}],
        # falls back: a float, an integer past 64 bits, a per-record source
        [{"op": "ingest_events", "nodes": [{"uid": "f1", "label": "N", "vt_s": 0, "props": {"w": 1.5}}]}],
        [{"op": "ingest_events", "nodes": [{"uid": "f2", "label": "N", "vt_s": 0, "props": {"w": 2 ** 70}}]}],
        [{"op": "ingest_events", "nodes": [{"uid": "f3", "label": "N", "vt_s": 0, "source": "x"}]}],
        # an empty and a null nodes array: no explicit node
        [{"op": "ingest_events", "nodes": [], "events": [{"src": "d1", "dst": "d2", "rel_type": "R", "vt_s": 2}]}],
        [{"op": "ingest_events", "nodes": None, "events": [{"src": "d2", "dst": "d3", "rel_type": "R", "vt_s": 4}]}],
        # writes over fast-path nodes
        [{"op": "correct", "ref": {"kind": "node", "uid": "a2"}, "props": {"name": "fixed"}, "vt_s": 20}],
        [{"op": "retract", "ref": {"kind": "node", "uid": "b3"}, "t": 31}],
    ]

REFUSED = [
    # a uid that already has a believed version
    [{"op": "ingest_events", "nodes": [{"uid": "a5", "label": "Thing", "vt_s": 900}]}],
    # a uid twice in one array
    [{"op": "ingest_events", "nodes": [{"uid": "e1", "label": "N", "vt_s": 1}, {"uid": "e1", "label": "N", "vt_s": 2}]}],
    # an empty interval
    [{"op": "ingest_events", "nodes": [{"uid": "e2", "label": "N", "vt_s": 5, "vt_e": 5}]}],
]


def _env(monkeypatch, on: bool):
    if on:
        monkeypatch.delenv("GNOMON_INGEST_RAW", raising=False)
    else:
        monkeypatch.setenv("GNOMON_INGEST_RAW", "0")


def _write(path, monkeypatch, on: bool):
    _env(monkeypatch, on)
    st = gnomon.open(path, backend="native")
    assert st.adapter.designs()["ingest_raw"] is on
    st.adapter._store.reset_ingest_phase_stats()
    for ops in _batches():
        st._write(ops)
    errors = []
    for ops in REFUSED:
        with pytest.raises(GnomonError) as e:
            st._write(ops)
        errors.append((type(e.value).__name__, str(e.value)))
    out = {"content": st.adapter.content_digest(), "uids": st.adapter.uids_for(range(st.adapter.num_entities())),
           "errors": errors, "counters": dict(st.adapter._store.ingest_phase_stats())}
    st.close()
    return out


def test_nodes_through_the_fast_path_write_what_the_python_row_build_writes(tmp_path, monkeypatch):
    on = _write(tmp_path / "on", monkeypatch, True)
    off = _write(tmp_path / "off", monkeypatch, False)
    for k in ("content", "uids", "errors"):
        assert on[k] == off[k], k
    if on["counters"]:          # timers compiled in
        # 40 + 6 explicit nodes and 2 auto-created endpoints (c1, c2), then d1,
        # d2, d3; the float, big-integer and override batches fell back
        assert on["counters"]["ingest_raw_node_rows"] == 40 + 6 + 2 + 3
        assert on["counters"]["ingest_raw_edge_rows"] == 5
        assert off["counters"].get("ingest_raw_node_rows", 0) == 0


def test_replay_of_node_batches_with_and_without_the_fast_path(tmp_path, monkeypatch):
    _write(tmp_path / "src", monkeypatch, False)
    got = {}
    for on in (True, False):
        _env(monkeypatch, on)
        a = native.NativeAdapter(tmp_path / f"replay-{on}")
        a._store.reset_ingest_phase_stats()
        replay(tmp_path / "src" / "eventlog.jsonl", a)
        got[on] = (a.content_digest(), a.store_digest(), a.uids_for(range(a.num_entities())),
                   dict(a._store.ingest_phase_stats()))
    assert got[True][:3] == got[False][:3]
    if got[True][3]:
        assert got[True][3]["ingest_raw_node_rows"] == 51
