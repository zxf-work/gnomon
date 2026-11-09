"""
The node lane pass of a batch version read reads a segment's first rows on
the calling thread and splits the rest over the pool only when, at their
pace, it would take 1 ms or more (GNOMON_MATCH_NODE_PASS=eager: always split,
=serial: one thread; GNOMON_MATCH_NODE_PASS_HEAD=<rows>, for the tests, sets
the head and always splits the rest). Every mode answers the same rows.
"""

from __future__ import annotations

import pytest

pytest.importorskip("gnomon._engine", reason="native engine extension not built")

from .test_sf10_fixes_1 import SCOPES, _friends_posts, _ic9_like  # noqa: E402
from .test_sf10_fixes_2 import _answer  # noqa: E402


@pytest.mark.parametrize("setting", sorted(SCOPES))
def test_the_paced_node_pass_keeps_the_rows(setting, monkeypatch):
    # the version reads of many targets: one pass over the node rows, split from 1 row
    monkeypatch.setenv("GNOMON_MATCH_PAR_ROWS", "1")
    a = _friends_posts(11, 60, 3)
    for limit in (3, 10_000):
        args = _ic9_like("date", limit, SCOPES[setting])
        got = set()
        for mode, head in (("serial", None), ("eager", None), ("paced", None), ("paced", "7"), ("paced", "1")):
            monkeypatch.setenv("GNOMON_MATCH_NODE_PASS", mode)
            if head is None:
                monkeypatch.delenv("GNOMON_MATCH_NODE_PASS_HEAD", raising=False)
            else:
                monkeypatch.setenv("GNOMON_MATCH_NODE_PASS_HEAD", head)
            got.add(_answer(a, args))
        assert len(got) == 1, (setting, limit)
