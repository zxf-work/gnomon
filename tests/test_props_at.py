"""`diff_snapshots` reads changed-edge props by storage address.

`props_for_vids` sweeps every edge row for a handful of version ids, because
vids are hashes and no segment is ordered by them. The point-state scans
`diff_snapshots` already runs can project each row's `(seg_id, seg_row)`, and
`props_at` reads exactly those rows. Every envelope must equal the one the
vid sweep produces on the same store — including inside an open batch, where
a staged row has no segment address and the sweep is kept — and `props_at`
must agree with `props_for_vids` row for row and refuse foreign addresses.
"""

from __future__ import annotations

import numpy as np
import pytest

from gnomon.temporal.algebra import call_operator, ensure_all_registered

ensure_all_registered()


class VidSweepPath:
    """The native adapter with `props_at` hidden: the operator's vid sweep."""

    def __init__(self, inner):
        self._inner = inner

    def __getattr__(self, name):
        if name == "props_at":
            raise AttributeError(name)
        return getattr(self._inner, name)


def _store(tmp):
    native = pytest.importorskip("gnomon.storage.native")
    a = native.NativeAdapter(tmp)
    a.paranoid = False
    tt = 1000
    for g in range(10):
        tt += 1
        ops = [{"op": "assert_node", "uid": f"n{i}", "label": "N", "props": {"k": g},
                "vt_s": 0, "vt_e": 1000} for i in range(6)] if g == 0 else []
        for i in range(5):
            ops.append({"op": "assert_edge", "src": f"n{i}", "dst": f"n{(i + g) % 6}",
                        "rel_type": "R", "props": {"w": g * 10 + i, "tag": "x" * (g % 3)},
                        "vt_s": 10 * g, "vt_e": 10 * g + 25, "disc": f"d{i}"})
        if g % 3 == 2:
            ops.append({"op": "correct", "ref": {"kind": "edge", "src": "n1", "dst": f"n{(1 + g) % 6}",
                                                 "rel_type": "R", "disc": "d1"},
                        "props": {"w": -g}, "vt_s": 10 * g + 5, "vt_e": 10 * g + 20})
        a.begin()
        a.apply_ops(ops, tt)
        a.commit()
        if g == 6:
            a.compact()
    return a


PAIRS = [(5, 15), (12, 18), (20, 31), (0, 95), (44, 52), (61, 66), (85, 89)]


def _both(a, args):
    return (call_operator(a, "diff_snapshots", dict(args)),
            call_operator(VidSweepPath(a), "diff_snapshots", dict(args)))


def test_diff_reads_props_by_address_and_answers_the_same(tmp_path):
    a = _store(tmp_path / "s")
    changed = 0
    for t1, t2 in PAIRS:
        for limit in (100, 1):
            by_addr, by_vid = _both(a, {"t1": t1, "t2": t2, "limit": limit})
            assert by_addr == by_vid, (t1, t2, limit)
            changed += sum(1 for c in by_addr["props_changed"] if c["kind"] == "edge")
    assert changed > 0                              # not vacuous


def test_an_open_batch_keeps_the_vid_sweep(tmp_path):
    a = _store(tmp_path / "s")
    a.begin()
    a.apply_ops([{"op": "correct", "ref": {"kind": "edge", "src": "n0", "dst": "n0",
                                           "rel_type": "R", "disc": "d0"},
                  "props": {"w": 999}, "vt_s": 3, "vt_e": 12}], 5000)
    for t1, t2 in PAIRS:
        by_addr, by_vid = _both(a, {"t1": t1, "t2": t2})
        assert by_addr == by_vid, (t1, t2)
    a.commit()


def test_props_at_is_props_for_vids_row_for_row(tmp_path):
    a = _store(tmp_path / "s")
    e = a.edges_columnar(columns=("vid", "seg_id", "seg_row"))
    assert len(e["vid"]) > 0
    got = a.props_at("edge", e["seg_id"], e["seg_row"])
    want = a.props_for_vids("edge", list(e["vid"]))
    assert got == [want[v] for v in e["vid"]]
    assert a.props_at("edge", [], []) == []
    with pytest.raises(Exception, match="not in the current generation"):
        a.props_at("edge", [int(np.max(e["seg_id"])) + 10_000], [0])
    with pytest.raises(Exception, match="out of bounds"):
        a.props_at("edge", [int(e["seg_id"][0])], [10**9])
