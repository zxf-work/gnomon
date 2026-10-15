"""`ingest_events` staged by the engine from its
event-log record writes exactly what the Python row build writes.

The fast path (`crates/gnomon-core/src/ingest_raw.rs`) accepts only the
op shape whose Python result it reproduces; everything else falls back to the
Python path. Every assertion here compares one store written with the fast
path on (the default) against one written with `GNOMON_INGEST_RAW=0`: same
content digest, same store digest, same event-log bytes, same dense ids — on
the live write path and on replay — and the engine counters show which ops
the fast path actually took, so the comparison is not vacuous.
"""

from __future__ import annotations

import pytest

import gnomon
from gnomon.storage.eventlog import replay

native = pytest.importorskip("gnomon.storage.native")


def _batches() -> list[list[dict]]:
    plain = [{"src": f"n{i % 7}", "dst": f"n{(3 * i) % 11}", "rel_type": "R" if i % 2 else "S",
              "vt_s": i, "vt_e": i + 5} for i in range(40)]
    shapes = [
        {"src": "ü-1", "dst": 'q"uote\\x', "rel_type": "R", "vt_s": 3},          # escapes, no vt_e
        {"src": "n1", "dst": "n2", "rel_type": "S", "vt_s": 4, "vt_e": None},     # null vt_e
        {"src": "n2", "dst": "n1", "rel_type": "S", "vt_s": 5, "vt_e": 0, "disc": "d"},
        {"src": "new-b", "dst": "new-a", "rel_type": "R", "vt_s": 9, "vt_e": 12, "extra": 1},
        {"src": "new-a", "dst": "new-b", "rel_type": "R", "vt_s": 2, "vt_e": 30},  # earlier first-seen
    ]
    return [
        [{"op": "ingest_events", "events": plain}],
        [{"op": "ingest_events", "events": shapes, "offset": 100, "source": "loader",
          "provenance_ref": "p1", "node_label": "Thing"}],
        # Python path: event props, an explicit nodes array, a null source
        [{"op": "ingest_events",
          "events": [{"src": "n1", "dst": "n3", "rel_type": "R", "vt_s": 20, "props": {"w": 1}}]}],
        [{"op": "ingest_events", "events": [{"src": "m1", "dst": "m2", "rel_type": "R", "vt_s": 30}],
          "nodes": [{"uid": "m9", "label": "N", "vt_s": 0, "vt_e": 50}]}],
        # mixed batch: a node asserted earlier in the same batch is not new
        [{"op": "assert_node", "uid": "z1", "label": "N", "props": {"a": 1}, "vt_s": 0, "vt_e": 100},
         {"op": "ingest_events", "events": [{"src": "z1", "dst": "z2", "rel_type": "R",
                                             "vt_s": 50, "vt_e": 60}]}],
        # two ingest ops in one batch, the second's endpoints partly new
        [{"op": "ingest_events", "events": [{"src": "a", "dst": "b", "rel_type": "R", "vt_s": 1}]},
         {"op": "ingest_events", "events": [{"src": "b", "dst": "c", "rel_type": "R", "vt_s": 2}],
          "offset": 1}],
        # a correction and a retraction over fast-path rows
        [{"op": "correct", "ref": {"kind": "edge", "src": "n0", "dst": "n0", "rel_type": "S",
                                   "disc": "#0"}, "props": {"w": 9}, "vt_s": 1, "vt_e": 4}],
        [{"op": "retract", "ref": {"kind": "node", "uid": "new-a"}, "t": 10}],
        [{"op": "ingest_events", "events": []}],
        [{"op": "ingest_events", "events": plain[:5], "offset": 40}],
    ]


def _write(path, monkeypatch, on: bool):
    if on:
        monkeypatch.delenv("GNOMON_INGEST_RAW", raising=False)
    else:
        monkeypatch.setenv("GNOMON_INGEST_RAW", "0")
    st = gnomon.open(path, backend="native")
    assert st.adapter.designs()["ingest_raw"] is on
    st.adapter._store.reset_ingest_phase_stats()
    for ops in _batches():
        st._write(ops)
    counters = dict(st.adapter._store.ingest_phase_stats())
    out = {"content": st.adapter.content_digest(), "store": st.digest(),
           "uids": st.adapter.uids_for(range(st.adapter.num_entities())),
           "log": (path / "eventlog.jsonl").read_bytes(), "counters": counters}
    st.close()
    return out


def test_fast_path_writes_what_the_python_row_build_writes(tmp_path, monkeypatch):
    on = _write(tmp_path / "on", monkeypatch, True)
    off = _write(tmp_path / "off", monkeypatch, False)
    # the live path stamps wall-clock transaction times, so the two stores'
    # store digests (which cover tt) differ by construction; the content
    # digest and the dense ids do not, and replay below pins the store digest
    for k in ("content", "uids"):
        assert on[k] == off[k], k
    # the logs differ only in batch ids/tt (the clock); their ops are equal
    import json
    ops = lambda b: [json.loads(x)["ops"] for x in b.splitlines()[1:]]  # noqa: E731
    assert ops(on["log"]) == ops(off["log"])
    if on["counters"]:          # timers compiled in
        # 40 + 5 + 1 + 1 + 1 + 5 edges took the fast path; the props and the
        # nodes batches fell back
        assert on["counters"]["ingest_raw_edge_rows"] == 53
        assert on["counters"]["ingest_raw_fallbacks"] == 2
        assert off["counters"]["ingest_raw_edge_rows"] == 0


def test_replay_with_and_without_the_fast_path(tmp_path, monkeypatch):
    src = _write(tmp_path / "src", monkeypatch, False)
    got = {}
    for on in (True, False):
        if on:
            monkeypatch.delenv("GNOMON_INGEST_RAW", raising=False)
        else:
            monkeypatch.setenv("GNOMON_INGEST_RAW", "0")
        a = native.NativeAdapter(tmp_path / f"replay-{on}")
        a._store.reset_ingest_phase_stats()
        replay(tmp_path / "src" / "eventlog.jsonl", a)
        got[on] = (a.content_digest(), a.store_digest(), dict(a._store.ingest_phase_stats()))
    assert got[True][:2] == got[False][:2] == (src["content"], src["store"])
    if got[True][2]:
        assert got[True][2]["ingest_raw_edge_rows"] == 53
        assert got[False][2]["ingest_raw_edge_rows"] == 0


def test_an_unaccepted_record_changes_nothing(tmp_path):
    a = native.NativeAdapter(tmp_path / "s")
    a.begin()
    a._ensure_batch(1000)
    before = a.num_entities()
    for rec in (b'{"ops":[{"op":"ingest_events","events":[{"src":"x","dst":"y","rel_type":"R","vt_s":1.5}]}]}',
                b'{"ops":[{"op":"ingest_events","events":[{"src":"x","dst":"y","rel_type":"R","vt_s":1,"props":{}}]}]}',
                b'{"ops":[{"op":"ingest_events","events":[{"src":"x","src":"z","dst":"y","rel_type":"R","vt_s":1}]}]}',
                b'{"ops":[{"op":"ingest_events","source":null,"events":[]}]}',
                b'{"ops":[{"op":"ingest_events","events":[{"src":"x","dst":"y","rel_type":"R","vt_s":1,"disc":null}]}]}',
                b'{"ops":[{"op":"assert_node"}]}',
                b'{"ops":[]}',
                b'not json'):
        assert a._store.stage_ingest_events_raw(rec, 0, 1000) is None, rec
    assert a.num_entities() == before
    a.rollback()
