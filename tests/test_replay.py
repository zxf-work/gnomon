"""Event-log replay reproduces identical store digests."""

from __future__ import annotations

import random

import gnomon
from gnomon.core.errors import NotFoundError
from gnomon.core.model import EntityRef
from gnomon.storage.duckdb_adapter import DuckDBAdapter
from gnomon.storage.eventlog import replay


def _random_workload(store: gnomon.Store, seed: int) -> None:
    rng = random.Random(seed)
    uids = ["u%d" % i for i in range(6)]
    for _ in range(60):
        kind = rng.choice(["an", "ae", "ae", "rt", "co"])
        u, v = rng.choice(uids), rng.choice(uids)
        s = rng.randrange(0, 50)
        e = s + rng.randrange(1, 20)
        try:
            if kind == "an":
                store.assert_node(u, "N", {"p": rng.randrange(3)}, vt_s=s, vt_e=e)
            elif kind == "ae":
                store.assert_edge(u, v, "R", {"w": rng.randrange(5)}, vt_s=s, vt_e=e)
            elif kind == "rt":
                store.retract(EntityRef(kind="edge", src=u, dst=v, rel_type="R"),
                              t=rng.randrange(0, 60))
            else:
                store.correct(EntityRef(kind="node", uid=u), {"p": 9}, vt_s=s, vt_e=e)
        except NotFoundError:
            pass
    store.ingest_events([
        {"src": rng.choice(uids), "dst": rng.choice(uids), "rel_type": "MSG",
         "vt_s": rng.randrange(0, 100)} for _ in range(200)])


def test_replay_reproduces_digest(tmp_path):
    store = gnomon.open(tmp_path / "s1", backend="duckdb", paranoid=True)
    _random_workload(store, seed=7)
    original = store.digest()
    store.close()

    fresh = DuckDBAdapter(":memory:")
    n = replay(tmp_path / "s1" / "eventlog.jsonl", fresh)
    assert n > 0
    assert fresh.store_digest() == original
    fresh.close()


def test_every_backend_agrees_on_a_batch_that_carves_its_own_write(tmp_path):
    """A batch whose second write carves its own first write: both adapters
    must agree on the resulting versions."""
    from gnomon.core.model import OPEN_END
    from gnomon.storage.native import NativeAdapter

    ops = [
        {"op": "assert_node", "uid": "x", "label": "N", "props": {"p": 1},
         "vt_s": 0, "vt_e": 100},
        {"op": "assert_node", "uid": "x", "label": "N", "props": {"p": 2},
         "vt_s": 50, "vt_e": 150},
        {"op": "assert_edge", "src": "x", "dst": "y", "rel_type": "R",
         "props": {"w": 1}, "vt_s": 10, "vt_e": 20, "disc": ""},
        {"op": "assert_edge", "src": "x", "dst": "y", "rel_type": "R",
         "props": {"w": 2}, "vt_s": 5, "vt_e": 15, "disc": ""},
    ]

    def rows(adapter):
        adapter.paranoid = True
        adapter.begin()
        adapter.apply_ops(ops, 1)
        adapter.commit()
        got = (sorted((v.uid, v.vt_s, v.vt_e, v.tt_s, v.tt_e, v.props["p"])
                      for v in adapter.all_node_versions()),
               sorted((v.vt_s, v.vt_e, v.tt_s, v.tt_e, v.props["w"])
                      for v in adapter.all_edge_versions()))
        adapter.close()
        return got

    expected = ([("x", 0, 50, 1, OPEN_END, 1), ("x", 50, 150, 1, OPEN_END, 2)],
                [(5, 15, 1, OPEN_END, 2), (15, 20, 1, OPEN_END, 1)])
    assert rows(DuckDBAdapter(":memory:")) == expected
    assert rows(NativeAdapter(str(tmp_path / "native"))) == expected


def test_reopen_continues_clock(tmp_path):
    store = gnomon.open(tmp_path / "s2")
    tt1 = store.assert_node("a", "N")
    store.close()
    store2 = gnomon.open(tmp_path / "s2")
    tt2 = store2.assert_node("b", "N")
    assert tt2 > tt1
    assert {v.uid for v in store2.adapter.all_node_versions()} == {"a", "b"}
    store2.close()


def test_ingest_events_columnar_roundtrip(tmp_path):
    store = gnomon.open(tmp_path / "s3")
    store.ingest_events([
        {"src": "a", "dst": "b", "rel_type": "MSG", "vt_s": 10},
        {"src": "b", "dst": "c", "rel_type": "MSG", "vt_s": 20},
        {"src": "a", "dst": "b", "rel_type": "MSG", "vt_s": 10},  # duplicate event ok
    ])
    cols = store.adapter.edges_columnar()
    assert len(cols["src_id"]) == 3
    assert list(cols["vt_s"]) == [10, 10, 20]  # sorted by vt_s
    # distinct logical edges even for identical (src, dst, rel, t)
    assert len(set(cols["eid"])) == 3
    ids = store.adapter.dense_ids(["a", "b", "c"])
    assert store.adapter.uids_for(ids) == ["a", "b", "c"]
    store.close()

