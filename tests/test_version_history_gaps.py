"""One identity's belief log includes the versions before a belief gap.

A gap -- a transaction time at which nothing of the identity is believed --
is ordinary history (a removal, then a rollback). A walk back through
`believed_edge_versions(eid, t)` stops at the first one and misses every
version before it; `version_history` with a `ref` reads the identity's
stored versions through its postings, so it must not. Each case is one edge
1 -> 2 of type R, on both backends: the log must equal every stored version
of the edge (`all_edge_versions`), and at every belief time its current
versions must be exactly the believed ones.
"""

from __future__ import annotations

import tempfile
from typing import Any

import pytest

from gnomon.core.model import OPEN_END, edge_eid
from gnomon.storage.duckdb_adapter import DuckDBAdapter
from gnomon.temporal.algebra import call_operator, ensure_all_registered

ensure_all_registered()

VS = 100
REF = {"src": "n1", "dst": "n2", "rel_type": "R", "disc": "#0"}
EID = edge_eid("n1", "n2", "R", "#0")


def _ingest(vt_e: int | None = None, props: dict | None = None) -> dict[str, Any]:
    ev: dict[str, Any] = {"src": "n1", "dst": "n2", "rel_type": "R", "vt_s": VS}
    if vt_e is not None:
        ev["vt_e"] = vt_e
    if props is not None:
        ev["props"] = props
    return {"op": "ingest_events", "events": [ev], "offset": 0, "node_label": "N"}


RETRACT_ALL = {"op": "retract", "ref": {"kind": "edge", **REF}, "t": VS}
ASSERT = {"op": "assert_edge", **REF, "props": {"k": 2}, "vt_s": VS, "vt_e": VS + 1}

#: case -> (batches, expected stored versions or None to take all_edge_versions')
CASES: dict[str, tuple[list[list[dict[str, Any]]], int | None]] = {
    "A ingest, retract, re-ingest": ([[_ingest()], [RETRACT_ALL], [_ingest()]], 2),
    "B ingest, retract, re-assert": ([[_ingest()], [RETRACT_ALL], [ASSERT]], 2),
    "C retract and assert in one batch": ([[_ingest()], [RETRACT_ALL, ASSERT]], None),
    "D open, closed by a retraction": (
        [[_ingest(vt_e=OPEN_END)], [{**RETRACT_ALL, "t": 150}]], 2),
    "E ingest, replace, remove, re-ingest": (
        [[_ingest()],
         [{"op": "correct", "ref": {"kind": "edge", **REF}, "props": {"k": 1},
           "vt_s": VS, "vt_e": VS + 1}],
         [RETRACT_ALL], [_ingest()]], 3),
}


def _adapter(kind: str):
    if kind == "duckdb":
        return DuckDBAdapter(":memory:")
    from gnomon.storage.native import NativeAdapter

    tmp = tempfile.TemporaryDirectory(prefix="gnomon-test-vhgap-")
    a = NativeAdapter(tmp.name)
    a._test_tmpdir = tmp
    return a


def _log(a, belief: str, as_of: int) -> list[tuple]:
    env = call_operator(a, "version_history", {
        "kind": "edge", "ref": dict(REF), "belief": belief, "as_of_tt": as_of,
        "window": {"t_a": 0, "t_b": OPEN_END}, "limit": 10_000})
    return [(r["vid"], r["vt_s"], r["vt_e"], r["tt_s"], r["tt_e"]) for r in env["rows"]]


@pytest.mark.parametrize("backend", ["native", "duckdb"])
@pytest.mark.parametrize("case", list(CASES))
def test_the_log_has_every_stored_version_across_belief_gaps(case, backend):
    batches, expected = CASES[case]
    a = _adapter(backend)
    for tt, ops in enumerate(batches, 1):
        a.begin()
        a.apply_ops(ops, tt)
        a.commit()
    stored = sorted(((v.vid, v.vt_s, v.vt_e, v.tt_s, v.tt_e)
                     for v in a.all_edge_versions() if v.eid == EID),
                    key=lambda r: (r[3], r[0]))
    if expected is not None:
        assert len(stored) == expected, stored
    # every stored version, both clocks, in (tt_s, vid) order
    assert _log(a, "all", OPEN_END) == stored
    # and at every belief time, the current ones are exactly the believed ones
    for t in range(0, len(batches) + 2):
        believed = sorted(v.vid for v in a.believed_edge_versions(EID, t))
        assert sorted(r[0] for r in _log(a, "current", t)) == believed, (case, t)
        written = sorted(r[0] for r in stored if r[3] <= t)
        assert sorted(r[0] for r in _log(a, "all", t)) == written, (case, t)
