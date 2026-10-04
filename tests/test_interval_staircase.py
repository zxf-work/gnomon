"""The interval-lane prefix-max staircase, modelled on `tests/test_compacted_layout_scaling.py`'s four
anti-flakiness mechanisms:

(i) one untimed warm-up then `statistics.median` over >= 3 reps where timing
    matters at all (this file's statistic is a *counter* ratio, not wall
    time, so the warm-up/median machinery is not needed here the way it is
    for a latency gate — the counters `ScanStats.blocks_skipped` /
    `blocks_considered` are exact integers, not noisy);
(ii) a host-independent **ratio** as the primary assertion, with an absolute
     floor as a belt;
(iii) a **fixture-is-real** assertion before anything else is trusted:
      `blocks_considered > 0` on a store that genuinely has an interval-lane
      segment with more than one staircase block, checked first so a fixture
      that stopped exercising the mechanism fails loudly instead of passing
      vacuously;
(iv) `pytest.importorskip("gnomon.storage.native", ...)`, `adapter.paranoid =
     False`, a fresh store per test.

**Why the fixture is one big single-commit batch, not many small ones (unlike
`test_compacted_layout_scaling.py`'s interleaved-generation fixture).** The
staircase is built **per segment** at seal time, and each `commit()`
seals its own batch into its own fresh segment(s) (`staging.rs::seal`) — a
segment never grows across commits. `block_rows` (and therefore the
staircase's own block granularity, `stair_block_rows`, defaulting to it)
is 32,768. A batch of a few hundred rows per commit — the pattern the
compacted-layout gate uses to control `tt_s_runs` — would give every segment
exactly one staircase block, and a single-block segment can never be
*partially* cut: it is either pruned whole by the existing zone map
(`prunes`, `h.vt_e_max <= vt_min`, independent of the staircase) or kept whole, so
`blocks_skipped` would stay 0 or jump straight to `blocks_considered`
regardless of whether the staircase's own prefix-cut logic works at all.
Measured directly while writing this file: with 90 commits of 500 rows each,
`blocks_skipped` was 0 at every window position swept below. One commit of
70,000 rows in a single segment gives `ceil(70_000 / 32_768) = 3` blocks and
a real graded skip fraction (see `MIN_SKIP_RATIO_AT_P75` below) — this is
the smallest fixture that actually exercises the within-segment cut, not
just the segment-level zone map.

**Local fixtures stay tiny.** 70,000 edges in
one native `apply_ops` batch built in under a second locally (measured:
~0.9s on the development laptop) — comparable in scale to
`test_compacted_layout_scaling.py`'s own `N_BIG = 64,000`. The full-fidelity
`synth-li-10m` store (2,000,000 interval-lane rows, 62 blocks) is built on a
measurement host, never here; this file only
proves the mechanism fires and is correctness-preserving.
"""

from __future__ import annotations

import pathlib

import pytest


def _native_adapter(tmp: pathlib.Path):
    native = pytest.importorskip(
        "gnomon.storage.native",
        reason="the interval-lane staircase is a native-engine mechanism",
    )
    a = native.NativeAdapter(tmp)
    a.paranoid = False  # the paranoid disjointness check is O(versions) per op
    return a


#: `PartitionMap::DEFAULT_WIDTH` (`crates/gnomon-core/src/staging.rs`).
#: `lane_for` routes an edge to the interval lane once it crosses more than
#: `LANE_MAX_PARTITION_CROSSINGS = 2` adjacent partitions of this width.
PARTITION_WIDTH_US = 7 * 24 * 60 * 60 * 1_000_000

#: > block_rows (32,768) so a single segment carries >= 2 staircase blocks
#: (see module docstring for why one block is not a real test of the cut).
N_INTERVAL_ROWS = 70_000


def _build_interval_lane_fixture(tmp: pathlib.Path, n_rows: int = N_INTERVAL_ROWS):
    """One native store, one commit, `n_rows` edges every one of which
    crosses 3..9 partitions (the `pow2` length family: `L =
    PARTITION_WIDTH_US * 2**U`, `U` cycling deterministically through
    `{1, 2, 3}`) — every row is unconditionally interval-lane, so this is a
    pure interval-lane fixture with no event-lane rows to keep the numbers
    below legible. `vt_s` is spread evenly across an 8-partition-width span
    so the store's own valid-time extent gives a meaningful window-start
    sweep (see `_native_adapter` callers below)."""
    adapter = _native_adapter(tmp)
    span = 8 * PARTITION_WIDTH_US
    step = span // n_rows
    ops = [
        {
            "op": "assert_edge", "src": f"s{i}", "dst": f"d{i}", "rel_type": "R",
            "props": {},
            "vt_s": (vt_s := i * step),
            "vt_e": vt_s + PARTITION_WIDTH_US * (2 ** (1 + i % 3)),
        }
        for i in range(n_rows)
    ]
    adapter.begin()
    adapter.apply_ops(ops, 1001)
    adapter.commit()
    return adapter


def _extent(adapter) -> tuple[int, int]:
    """The store's actual `[vt_s.min(), vt_e.max())` — the window-start
    sweep below is expressed as a fraction of *this*, not of the generator's
    own `span`, because a window position is meaningful relative to what a
    query could actually ask for over the data, and `vt_e` reaches well
    past `vt_s`'s own span for the longest-lived rows."""
    cols = adapter.edges_columnar(columns=["vt_s", "vt_e"])
    return int(cols["vt_s"].min()), int(cols["vt_e"].max())


def _scan_stats(adapter, vt_min: int) -> dict:
    return adapter._read_op(lambda st: st.scan_edges(
        as_of_tt=1 << 62, vt_min=vt_min, vt_max=None, rel_types=None,
        touching_ids=None, touching_both=False, limit=None, columns=["vt_s"],
    ))


# --- 1. the fixture is real ------------------------------------------------ #


def test_fixture_is_genuinely_interval_lane_with_multiple_blocks(tmp_path):
    """Before anything else is trusted: the fixture actually produced an
    interval-lane segment carrying a staircase of more than one block. A
    fixture that silently routed everything to the event lane (a partition
    width drifting, a lane-crossing off-by-one) would make every assertion
    below pass vacuously."""
    adapter = _build_interval_lane_fixture(tmp_path / "s")
    t0, t1 = _extent(adapter)
    stats = _scan_stats(adapter, vt_min=t0)
    assert stats["blocks_considered"] >= 2, (
        f"expected >= 2 staircase blocks (n_rows={N_INTERVAL_ROWS}, "
        f"block_rows=32768), got blocks_considered={stats['blocks_considered']} "
        f"-- the fixture is not exercising a within-segment cut"
    )
    assert stats["rows_examined"] == N_INTERVAL_ROWS


# --- 2. the skip fraction is real and monotone in window position --------- #

# Measured on this machine (this file's own fixture, `N_INTERVAL_ROWS =
# 70,000`, 3 staircase blocks), post-fix engine:
#
#   p (window-start fraction of [vt_s.min(), vt_e.max()))   blocks_skipped/considered
#   0.10, 0.25, 0.50                                        0.000
#   0.75                                                    0.333  (1 of 3 blocks)
#   0.90                                                    0.333  (1 of 3 blocks)
#
# A 3-block fixture cannot reproduce the fine-grained curve
# expected of the 62-block `synth-li-10m` store (measured on a measurement
# host against the real store, not here) -- this is a floor check that the mechanism
# fires at all locally. 0.25 sits below the measured 0.333 with headroom for
# a fixture built on a different host/engine build to still land on a
# different block boundary and pass, while `> 0` at p=0.75 could not tell a
# working cut from a broken one that only ever returns 0.
MIN_SKIP_RATIO_AT_P75 = 0.25


def test_skip_ratio_is_nonzero_and_monotone_across_window_positions(tmp_path):
    adapter = _build_interval_lane_fixture(tmp_path / "s")
    t0, t1 = _extent(adapter)

    prev_ratio = 0.0
    ratio_at_p75 = None
    for p in (0.10, 0.25, 0.50, 0.75, 0.90):
        vt_min = t0 + int(p * (t1 - t0))
        stats = _scan_stats(adapter, vt_min)
        considered = stats["blocks_considered"]
        assert considered > 0, f"p={p}: staircase not consulted at all"
        ratio = stats["blocks_skipped"] / considered
        assert ratio >= prev_ratio - 1e-9, (
            f"p={p}: skip ratio {ratio} regressed below the previous "
            f"position's {prev_ratio} -- a stricter vt_min must never skip "
            f"fewer blocks"
        )
        prev_ratio = ratio
        if p == 0.75:
            ratio_at_p75 = ratio

    assert ratio_at_p75 is not None
    assert ratio_at_p75 >= MIN_SKIP_RATIO_AT_P75, (
        f"blocks_skipped/blocks_considered at p=0.75 was {ratio_at_p75}, "
        f"below the {MIN_SKIP_RATIO_AT_P75} floor (see the comment above "
        f"MIN_SKIP_RATIO_AT_P75 for the measured provenance)"
    )


# --- 3. correctness: the cut never changes the answer ---------------------- #


def test_staircase_is_sound_against_the_no_staircase_escape_hatch(tmp_path, monkeypatch):
    """`GNOMON_NO_STAIRCASE=1` (`scan.rs::staircase_disabled`) forces `lo = 0`
    on every interval-lane segment, i.e. the row loop runs over exactly the
    rows it would if the staircase field were empty. The contract: with the staircase on, the *answer* -- the selected `vt_s`
    multiset -- must be identical to the staircase-off run, and
    `rows_examined` must never be higher with the staircase on than off (the
    cut only ever removes candidates the per-row test would have rejected
    anyway, by the cut's soundness argument, which `scan::tests::
    staircase_cut_never_drops_a_matching_row` proves at the Rust level over
    many `vt_min` values; this is the one Python-visible corroboration)."""
    adapter = _build_interval_lane_fixture(tmp_path / "s")
    t0, t1 = _extent(adapter)
    vt_min = t0 + int(0.75 * (t1 - t0))

    on = _scan_stats(adapter, vt_min)
    monkeypatch.setenv("GNOMON_NO_STAIRCASE", "1")
    off = _scan_stats(adapter, vt_min)
    monkeypatch.delenv("GNOMON_NO_STAIRCASE", raising=False)
    on_again = _scan_stats(adapter, vt_min)

    assert on["blocks_skipped"] > 0, "fixture-is-real: the cut must actually fire when on"
    assert off["blocks_skipped"] == 0 and off["blocks_considered"] == 0, (
        "GNOMON_NO_STAIRCASE=1 must disable the mechanism entirely, not just narrow it"
    )
    assert sorted(on["vt_s"].tolist()) == sorted(off["vt_s"].tolist()), (
        "the staircase must never change which rows are selected"
    )
    assert on["rows_examined"] <= off["rows_examined"]
    # the escape hatch is read fresh per call (no cross-call caching to leak
    # state between an on/off/on sequence within one process)
    assert on_again["blocks_skipped"] == on["blocks_skipped"]
    assert on_again["rows_examined"] == on["rows_examined"]


def test_event_lane_never_carries_a_staircase(tmp_path):
    """Structural corroboration that event-lane `rows_examined` is
    exactly unchanged: an event-lane-only store (every edge instantaneous,
    crossing <= 2 partitions) reports zero for both counters, because
    `write_edge_segment` only ever builds a staircase for
    `Lane::Interval && !vt_e_elided` segments (`segment::tests::
    event_lane_and_elided_segments_carry_no_staircase` pins this at the
    Rust level). A zero-valued mechanism cannot have moved an integer
    counter; this is what makes that claim exact rather than approximate."""
    adapter = _native_adapter(tmp_path / "s")
    ops = [
        {"op": "assert_edge", "src": f"s{i}", "dst": f"d{i}", "rel_type": "R",
         "props": {}, "vt_s": i, "vt_e": i + 1}
        for i in range(2_000)
    ]
    adapter.begin()
    adapter.apply_ops(ops, 1001)
    adapter.commit()
    stats = _scan_stats(adapter, vt_min=0)
    assert stats["blocks_considered"] == 0
    assert stats["blocks_skipped"] == 0
    assert stats["rows_examined"] == 2_000


# --- 4. set_layout is physical-only: it can never move a digest ----------- #


def test_partition_width_does_not_move_a_digest(tmp_path):
    """`NativeStore.set_layout` (exposed at
    `crates/gnomon-py/src/lib.rs`) so the long-interval workload family
    can be built at a chosen partition width. Lane assignment is *physical*:
    the same log ingested under
    two different partition widths must produce the same logical content,
    routed through different lanes but never a different `store_digest()`
    (`StorageAdapter.store_digest`, backend-independent, built from the
    same `to_json()` rows either way)."""
    ops = [
        {"op": "assert_edge", "src": f"s{i}", "dst": f"d{i}", "rel_type": "R",
         "props": {"v": i}, "vt_s": i * 1000,
         "vt_e": i * 1000 + PARTITION_WIDTH_US * 5}
        for i in range(500)
    ]

    narrow = _native_adapter(tmp_path / "narrow")
    narrow._store.set_layout(0, PARTITION_WIDTH_US // 100, 128 * 1024 * 1024)
    narrow.begin()
    narrow.apply_ops(ops, 1001)
    narrow.commit()

    wide = _native_adapter(tmp_path / "wide")
    wide._store.set_layout(0, PARTITION_WIDTH_US * 100, 128 * 1024 * 1024)
    wide.begin()
    wide.apply_ops(ops, 1001)
    wide.commit()

    assert narrow.store_digest() == wide.store_digest()

    # and the widths genuinely produced different lane routing -- otherwise
    # this test would pass even if `set_layout` silently did nothing
    narrow_stats = narrow._read_op(lambda st: st.scan_edges(
        as_of_tt=1 << 62, vt_min=0, vt_max=None, rel_types=None,
        touching_ids=None, touching_both=False, limit=None, columns=["vt_s"]))
    wide_stats = wide._read_op(lambda st: st.scan_edges(
        as_of_tt=1 << 62, vt_min=0, vt_max=None, rel_types=None,
        touching_ids=None, touching_both=False, limit=None, columns=["vt_s"]))
    assert narrow_stats["blocks_considered"] > 0, "narrow layout must route to the interval lane"
    assert wide_stats["blocks_considered"] == 0, "wide layout must keep everything in the event lane"
