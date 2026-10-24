"""A transaction's per-operation ops applied by the engine from
its event-log record (`crates/gnomon-core/src/apply_raw.rs`) write
exactly what `base.py::apply_ops` writes.

A pseudo-random workload of transactions -- node and edge asserts that carve
what they overlap (and re-asserts inside one transaction, which retire), props
of every JSON shape the engine renders itself, corrections, retractions,
records that mix `ingest_events` in -- is written once with the raw paths on
(the default) and once with `GNOMON_INGEST_RAW=0`: same content digest, same
dense ids, same refusals (a retract or correct with nothing to act on, a
correct across disagreeing labels), and the same store digest on replay. The
engine's verdicts are counted, so the comparison is not vacuous; shapes it
must not take (a float in props, a null source) are declined to Python.
"""

from __future__ import annotations

import random

import pytest

import gnomon
from gnomon.core.errors import GnomonError
from gnomon.storage.eventlog import replay

native = pytest.importorskip("gnomon.storage.native")

OPEN = 2 ** 62
PROPS = [{"n": 1}, {"name": "é\t\"x\"", "tags": ["a", None, True], "deep": {"k": {"z": 0, "a": -3}}}, {}, None, [1, 2], "s"]


def _workload(seed: int = 11, n_txn: int = 300) -> list[list[dict]]:
    rng = random.Random(seed)
    uids = [f"u{i}" for i in range(12)]
    rels = ["R", "S"]
    txns: list[list[dict]] = []
    t = 1000
    for k in range(n_txn):
        ops = []
        for _ in range(rng.randint(1, 3)):
            t += rng.randint(1, 50)
            r = rng.random()
            u = rng.choice(uids)
            if r < 0.30:
                op = {"op": "assert_node", "uid": u, "label": "L" + str(int(u[1:]) % 3), "vt_s": t - rng.randint(0, 800)}
                if rng.random() < 0.4:
                    op["vt_e"] = op["vt_s"] + rng.randint(1, 900)
                if rng.random() < 0.8:
                    op["props"] = rng.choice(PROPS)
                if rng.random() < 0.2:
                    op["source"] = "agent"; op["provenance_ref"] = f"p{k}"
            elif r < 0.55:
                d = rng.choice(uids)
                op = {"op": "assert_edge", "src": u, "dst": d, "rel_type": rng.choice(rels), "vt_s": t - rng.randint(0, 800),
                      "src_label": "E", "props": rng.choice(PROPS[:4])}
                if rng.random() < 0.3:
                    op["disc"] = "d" + str(rng.randint(0, 2))
                if rng.random() < 0.4:
                    op["vt_e"] = op["vt_s"] + rng.randint(1, 900)
            elif r < 0.75:
                if rng.random() < 0.5:
                    ref = {"kind": "node", "uid": u}
                else:
                    ref = {"kind": "edge", "src": u, "dst": rng.choice(uids), "rel_type": rng.choice(rels)}
                op = {"op": "correct", "ref": ref, "props": rng.choice(PROPS[:3]), "vt_s": t - rng.randint(0, 900)}
                if rng.random() < 0.5:
                    op["vt_e"] = op["vt_s"] + rng.randint(1, 400)
            elif r < 0.92:
                if rng.random() < 0.5:
                    ref = {"kind": "node", "uid": u}
                else:
                    ref = {"kind": "edge", "src": u, "dst": rng.choice(uids), "rel_type": rng.choice(rels)}
                op = {"op": "retract", "ref": ref, "t": t - rng.randint(0, 600)}
            else:
                op = {"op": "ingest_events", "offset": k * 10,
                      "events": [{"src": rng.choice(uids), "dst": rng.choice(uids), "rel_type": "EV", "vt_s": t + i} for i in range(3)]}
            ops.append(op)
        txns.append(ops)
    # declined shapes: a float in props, a null source
    txns.append([{"op": "assert_node", "uid": "f1", "label": "L", "vt_s": 5, "props": {"w": 0.5}}])
    txns.append([{"op": "assert_edge", "src": "f1", "dst": "u1", "rel_type": "R", "vt_s": 6, "source": None}])
    return txns


class Counting:
    def __init__(self, inner, seen):
        object.__setattr__(self, "_inner", inner)
        object.__setattr__(self, "_seen", seen)

    def __getattr__(self, name):
        v = getattr(self._inner, name)
        if name != "apply_ops_raw":
            return v

        def w(*a, **k):
            r = v(*a, **k)
            self._seen[r] = self._seen.get(r, 0) + 1
            return r
        return w


def _env(monkeypatch, on: bool):
    if on:
        monkeypatch.delenv("GNOMON_INGEST_RAW", raising=False)
    else:
        monkeypatch.setenv("GNOMON_INGEST_RAW", "0")


def _write(path, monkeypatch, on: bool):
    _env(monkeypatch, on)
    st = gnomon.open(path, backend="native")
    seen: dict[str, int] = {}
    st.adapter._store = Counting(st.adapter._store, seen)
    errors = []
    for i, ops in enumerate(_workload()):
        try:
            st._write(ops)
        except (GnomonError, ValueError, TypeError) as e:
            errors.append((i, type(e).__name__, str(e)))
    st.adapter._store = st.adapter._store._inner
    out = {"content": st.adapter.content_digest(), "uids": st.adapter.uids_for(range(st.adapter.num_entities())),
           "errors": errors, "seen": seen}
    st.close()
    return out


def test_the_engine_applies_what_the_python_loop_applies(tmp_path, monkeypatch):
    on = _write(tmp_path / "on", monkeypatch, True)
    off = _write(tmp_path / "off", monkeypatch, False)
    for k in ("content", "uids", "errors"):
        assert on[k] == off[k], k
    assert on["errors"], "the workload exercises refusals"
    assert on["seen"].get("applied", 0) > 150, on["seen"]
    assert on["seen"].get("refused", 0) >= 1 and on["seen"].get("declined", 0) >= 2, on["seen"]
    assert set(off["seen"]) <= {"declined"}, off["seen"]


def test_replay_with_and_without_the_engine_apply(tmp_path, monkeypatch):
    _write(tmp_path / "src", monkeypatch, False)
    got = {}
    for on in (True, False):
        _env(monkeypatch, on)
        a = native.NativeAdapter(tmp_path / f"replay-{on}")
        replay(tmp_path / "src" / "eventlog.jsonl", a)
        got[on] = (a.content_digest(), a.store_digest(), a.uids_for(range(a.num_entities())))
    assert got[True] == got[False]
