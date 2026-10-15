"""`resolve_entities` pages in the engine and answers exactly as the
row-list path does.

The paged kernel (`resolve_entities_page`) moves the label filter, the
`(match, uid)` order and the cursor slice below the binding, so `props` is
parsed for the page's rows only. Every envelope must equal the one the
row-list path (`resolve_entities` plus `paginate`) produces on the same
store: same rows, `rows_total`, `truncated`, `cursor`, digest — over renames,
corrections, retractions, an uncommitted batch, label filters, cursors past
the end and negative cursors, and non-ASCII names.
"""

from __future__ import annotations

import pytest

from gnomon.core.errors import InvalidArgError
from gnomon.temporal.algebra import call_operator, ensure_all_registered

ensure_all_registered()

NAMES = ["Alice", "ALICE smith", "", "İstanbul", "ΣΑΣ office", "bob", "Ünï-σας", "u1 annex"]


class RowListPath:
    """The native adapter with the paged kernel hidden, so the operator takes
    its row-list path over the very same store."""

    def __init__(self, inner):
        self._inner = inner

    def __getattr__(self, name):
        if name == "resolve_entities_page":
            raise AttributeError(name)
        return getattr(self._inner, name)


def _uid(i: int) -> str:
    return f"u{i}" if i % 3 else f"Alice-{i}"


def _store(tmp):
    native = pytest.importorskip("gnomon.storage.native")
    a = native.NativeAdapter(tmp)
    a.paranoid = False
    tt = 1000
    for g in range(12):
        tt += 1
        ops = []
        for i in range(g, g + 9):
            props = {"name": NAMES[(i + g) % len(NAMES)], "v": g}
            if i % 5 == 0:
                props = {"name": i}             # not a string: never matched by name
            ops.append({"op": "assert_node", "uid": _uid(i),
                        "label": "Person" if i % 2 else "Org", "props": props,
                        "vt_s": 10 * g + i, "vt_e": 10 * g + i + 4})
        a.begin()
        a.apply_ops(ops, tt)
        a.commit()
        if g % 4 == 3:
            # a later belief: one entity retracted, one renamed
            tt += 1
            i, j = g + 1, g + 2
            a.begin()
            a.apply_ops([
                {"op": "retract", "ref": {"kind": "node", "uid": _uid(i)}, "t": 10 * g + i + 1},
                {"op": "correct", "ref": {"kind": "node", "uid": _uid(j)},
                 "props": {"name": "renamed alice"}, "vt_s": 10 * g + j, "vt_e": 10 * g + j + 50},
            ], tt)
            a.commit()
    return a


QUERIES = ["alice", "ALICE", "u1", "U", "σας", "i̇", "o", "zzz", "Alice-3"]
SHAPES = [
    {}, {"limit": 1}, {"limit": 3}, {"limit": 3, "cursor": "3"}, {"limit": 2, "cursor": "1"},
    {"cursor": "1000"}, {"limit": 2, "cursor": "-2"}, {"limit": 4, "cursor": "+1"},
]


def _both(a, args):
    return (call_operator(a, "resolve_entities", dict(args)),
            call_operator(RowListPath(a), "resolve_entities", dict(args)))


def test_paged_kernel_is_the_row_list_path(tmp_path):
    a = _store(tmp_path / "s")
    seen_truncated = seen_label_cut = False
    for q in QUERIES:
        for label in (None, "Person", "Org"):
            for shape in SHAPES:
                args = {"query": q, **shape}
                if label is not None:
                    args["label"] = label
                paged, rowlist = _both(a, args)
                assert paged == rowlist, args
                seen_truncated |= bool(paged["truncated"])
                if label is not None:
                    full = call_operator(a, "resolve_entities", {"query": q})
                    seen_label_cut |= paged["rows_total"] < full["rows_total"]
    assert seen_truncated and seen_label_cut      # the cases are not vacuous


def test_an_open_batch_reads_its_own_writes(tmp_path):
    a = _store(tmp_path / "s")
    a.begin()
    a.apply_ops([{"op": "assert_node", "uid": "fresh-alice", "label": "Person",
                  "props": {"name": "Alice staged"}, "vt_s": 5, "vt_e": 9}], 5000)
    for q in ("alice", "fresh", "staged"):
        paged, rowlist = _both(a, {"query": q, "limit": 2})
        assert paged == rowlist, q
    assert any(r["uid"] == "fresh-alice" for r in
               call_operator(a, "resolve_entities", {"query": "fresh"})["rows"])
    a.commit()


def test_a_bad_cursor_is_refused_the_same_way(tmp_path):
    a = _store(tmp_path / "s")
    for target in (a, RowListPath(a)):
        with pytest.raises(InvalidArgError, match="bad cursor"):
            call_operator(target, "resolve_entities", {"query": "alice", "cursor": "x"})
