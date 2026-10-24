"""The SNB-, FinBench- and TPC-BiH-shaped generators the pattern operator's
gates run on: deterministic, invariant-keeping, and faithful to the mapping
rules the operator's semantics lean on (KNOWS once, multi-edges by ordinal
`disc`, valid time from creation, deletes as retractions)."""

from __future__ import annotations

import importlib
from collections import Counter

import pytest

from gnomon.core.model import OPEN_END

MODULES = ("snb_shaped", "finbench_shaped", "tpcbih_shaped")


def gen(name):
    return importlib.import_module(f"gnomon.data.{name}")


@pytest.mark.parametrize("name", MODULES)
def test_deterministic_and_corrections_come_last(name):
    m = gen(name)
    a = m.batches(30, seed=3)
    assert a == m.batches(30, seed=3)
    assert a != m.batches(30, seed=4)
    c = m.batches(30, seed=3, corrections_pct=10)
    # every batch before the corrections is unchanged by them
    assert c[:len(a)] == a and len(c) > len(a)
    assert {op["op"] for _, b in c[len(a):] for op in b} == {"correct"}
    tts = [tt for tt, _ in c]
    assert tts == sorted(set(tts)) and tts[0] == 1


@pytest.mark.parametrize("name", MODULES)
def test_now_is_after_every_write_and_names_are_alive(name):
    m = gen(name)
    ops = [op for _, b in m.batches(30, seed=0) for op in b]
    starts = [op["vt_s"] for op in ops if "vt_s" in op]
    assert max(starts) < m.NOW < OPEN_END
    names = m.names(30, seed=0)
    assert names and all(names.values())


def test_snb_knows_is_stored_once_with_ordered_ids():
    m = gen("snb_shaped")
    knows = [op for _, b in m.batches(40) for op in b
             if op["op"] == "assert_edge" and op["rel_type"] == "KNOWS"]
    pairs = Counter(frozenset((op["src"], op["dst"])) for op in knows)
    assert knows and max(pairs.values()) == 1
    assert all(int(op["src"]) < int(op["dst"]) for op in knows)
    assert any(op["op"] == "retract" for _, b in m.batches(40) for op in b)


def test_finbench_multi_edges_carry_ordinals():
    m = gen("finbench_shaped")
    seen: dict[tuple, list[str]] = {}
    for _, b in m.batches(40):
        for op in b:
            if op["op"] == "assert_edge":
                key = (op["src"], op["dst"], op["rel_type"])
                seen.setdefault(key, []).append(op["disc"])
    multi = {k: v for k, v in seen.items() if k[2] in m.MULTI_RELS}
    assert any(len(v) > 1 for v in multi.values())
    for k, v in multi.items():
        assert v == [str(i) for i in range(len(v))], k
    assert all(v == [""] for k, v in seen.items() if k[2] not in m.MULTI_RELS)


def test_tpcbih_part55_supplies_change_on_both_clocks():
    m = gen("tpcbih_shaped")
    ops = [op for _, b in m.batches(30) for op in b]
    sup = [op for op in ops if op.get("rel_type") == "SUPPLIES" and op.get("dst") == m.PART55]
    corrected = [op for op in ops if op["op"] == "correct"
                 and op["ref"].get("dst") == m.PART55]
    assert len({op["src"] for op in sup}) >= 3
    assert len(sup) > len({op["src"] for op in sup}) and corrected
