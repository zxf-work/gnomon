"""Bulk ingest's endpoint-existence probe, answered from the believed-node
count per dense id (engine `node_belief.rs`).

`_ingest_events` creates a bare node version for every endpoint the probe
calls absent, so the probe decides which node rows exist. The engine-side
tests hold the count to both walk strategies on randomized stores; these hold
the whole write path to the reference backend (DuckDB), whose probe shares no
code with the engine, over the cases where "absent" is subtle:

* a node whose every version was retracted is absent, and the next batch that
  names it as an endpoint gives it a fresh version;
* a batch of several `ingest_events` ops reads the nodes its earlier ops
  staged (read-your-own-writes), and creates each at most once;
* compaction and reopening a native store change nothing about the answer.

Stores are compared with transaction time projected out (see
`test_bulk_nodes._content`): two independent builds never share `tt`.
"""

from __future__ import annotations

import json

import gnomon
from gnomon.core.model import EntityRef
from gnomon.storage.base import make_op


def _content(store) -> str:
    def rows(vs, keys):
        return sorted(tuple(getattr(v, k) for k in keys) for v in vs)
    nodes = rows(store.adapter.all_node_versions(),
                 ("uid", "label", "vt_s", "vt_e", "source"))
    edges = rows(store.adapter.all_edge_versions(),
                 ("eid", "src", "dst", "rel_type", "disc", "vt_s", "vt_e"))
    props = sorted((v.uid, json.dumps(v.props, sort_keys=True))
                   for v in store.adapter.all_node_versions())
    return json.dumps([nodes, edges, props], sort_keys=True)


def _events(lo: int, hi: int, nodes: int) -> list[dict]:
    """Deterministic events over `nodes` uids, so later batches mostly name
    endpoints that already exist. `disc` is explicit because the default one
    counts positions per `Store` instance, and the native build reopens."""
    return [{"src": f"n{(i * 7) % nodes}", "dst": f"n{(i * 13 + 5) % nodes}",
             "rel_type": "R", "vt_s": 1_000 + i, "disc": f"e{i}"}
            for i in range(lo, hi)]


def _believed_uids(store) -> list[str]:
    return sorted({v.uid for v in store.adapter.all_node_versions()
                   if v.tt_e == gnomon.OPEN_END})


def _build(path, backend: str, native_maintenance: bool) -> gnomon.Store:
    s = gnomon.open(path, backend=backend)
    s.ingest_events(_events(0, 60, 25))
    s.ingest_events(_events(60, 120, 40))           # new and known endpoints
    # retract two nodes entirely (at their own vt_s, which leaves no
    # remainder), correct a third
    for uid in ("n3", "n4"):
        (v,) = s.adapter.believed_node_versions(uid)
        s.retract(EntityRef(kind="node", uid=uid), v.vt_s)
    s.correct(EntityRef(kind="node", uid="n5"), {"k": 1}, vt_s=1_010)
    if native_maintenance:
        s.adapter.compact_incremental()
        s.close()
        s = gnomon.open(path, backend=backend)
    # n3 and n4 are absent now: naming them again recreates them
    s.ingest_events([{"src": "n3", "dst": "n4", "rel_type": "R", "vt_s": 5_000, "disc": "r0"},
                     {"src": "n4", "dst": "n99", "rel_type": "R", "vt_s": 5_001, "disc": "r1"}])
    # one batch, two ingest ops: the second sees what the first staged
    s._write([
        make_op("ingest_events",
                events=[{"src": "m1", "dst": "m2", "rel_type": "R", "vt_s": 6_000}],
                offset=10_000, node_label="Node", source="ingest", provenance_ref=None),
        make_op("ingest_events",
                events=[{"src": "m2", "dst": "m1", "rel_type": "R", "vt_s": 6_001},
                        {"src": "m1", "dst": "n0", "rel_type": "R", "vt_s": 6_002}],
                offset=10_001, node_label="Node", source="ingest", provenance_ref=None),
    ])
    if native_maintenance:
        s.adapter.compact()
        s.close()
        s = gnomon.open(path, backend=backend)
    s.ingest_events(_events(120, 200, 60))
    return s


def test_native_matches_the_reference_backend(tmp_path):
    ref = _build(tmp_path / "duck", "duckdb", native_maintenance=False)
    nat = _build(tmp_path / "native", "native", native_maintenance=True)
    try:
        assert _content(nat) == _content(ref)
        uids = _believed_uids(nat)
        assert {"n3", "n4", "m1", "m2", "n99"} <= set(uids)
        # every endpoint was created once: one believed version per uid,
        # except n5, whose correction split it into two valid intervals
        per_uid: dict[str, int] = {}
        for v in nat.adapter.all_node_versions():
            if v.tt_e == gnomon.OPEN_END:
                per_uid[v.uid] = per_uid.get(v.uid, 0) + 1
        assert {u for u, c in per_uid.items() if c != 1} <= {"n5"}
        # and the probe itself agrees with the reference on a mixed ask
        ask = [f"n{i}" for i in range(70)] + ["m1", "m2", "ghost"]
        assert (nat.adapter.nodes_with_believed_versions(ask)
                == ref.adapter.nodes_with_believed_versions(ask))
    finally:
        ref.close()
        nat.close()


def test_the_same_history_builds_the_same_content(tmp_path):
    a = _build(tmp_path / "a", "native", native_maintenance=True)
    b = _build(tmp_path / "b", "native", native_maintenance=False)
    try:
        assert _content(a) == _content(b)
    finally:
        a.close()
        b.close()
