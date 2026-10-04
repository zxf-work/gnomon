"""A corrected store's scan costs about what an uncorrected one's does.

The native scan narrows a segment in three tiers: prune it from the header,
binary-search the valid-time window, then test what remains row by row.
Tier 3 is skipped entirely when the first two have already decided every row
— `select_range`'s contiguous-extend fast path, which reserves the range and
extends it without touching a predicate.

The belief predicate used to break that. `believed_ranges` answers only the
`tt_s <= as_of` half; the other half, `as_of < tt_e`, was a per-row test, and
the presence of *any* close in a segment set `needs_row_test`. So one
correction anywhere in a segment converted that segment's whole scan from a
range extend into a row loop, and the tax grew with scale as the fraction of
segments that still had the fast path shrank. Measured: 30-50 % on scan queries at the *first* corrections
(0.01 % density, 103 corrections in 1M events), 2-7x at 5 % density
(`nbr.evolution` 64.5 vs 9.2 ms), and 4.0-6.6x at 10M rows / 0.1 %
(`coactive.narrow` 1,103 vs 273 ms).

The fix (`crates/gnomon-core`: `visibility.rs`, `scan.rs`) resolves a
segment's effective closes once per segment into one row-sorted view and
*subtracts* them from the verdict ranges, so `select_range` gets ranges it
can extend contiguously again. It changes what the scan pays, never what it
returns: the closed rows are cut from the ranges instead of being skipped
inside the loop, and the precedence between a close run and a folded sidecar
entry — including the `tt_e == OPEN_END` run entry that is treated as having
nothing to say — is preserved exactly. The row-for-row equality is pinned in
Rust by `believed_ranges_closed_equals_the_row_loop`, a differential property
test against the retained pre-fix loop; this file gates the cost.

**Why this file scans edges and not nodes.** `nodes_columnar()` does not go
through `select_range` at all: `NativeStore::scan_node_versions`
(`read.rs`) is a flat per-row sweep over every node segment, with no ranges
and no fast path to lose, and the fix leaves it untouched. `edges_columnar()`
is the `ScanSet::select` path. A node-shaped gate here would have passed
flat on the pre-fix engine and measured nothing.

The scan is projected down to `vt_s`/`vt_e` deliberately. Unprojected string
columns are materialized per surviving row, which is real work the fix does
not touch and which would dilute the very signal this file exists to gate.

These run against the **native** engine specifically, whatever
GNOMON_TEST_BACKEND says — the close view and the verdict ranges are native
segment concepts, and a DuckDB run would pass every test here
vacuously.
"""

from __future__ import annotations

import json
import statistics
import time
from pathlib import Path

import pytest

# --- the thresholds, and the control that set them ------------------------ #
#
# These are not guesses. The pre-fix engine was built from the commit before
# the change and measured on this file's exact fixture
# and statistic on **two hosts**, because a gate that has never seen the
# defect it guards is an assertion about nothing, and a threshold set
# on one host is a threshold that has never met a different one.
#
# N_ROWS=24,000 built in ROWS_PER_BATCH=500 interleaved generations and
# compacted; corrected arm at CORRECTION_PCT=5 %, which folds 1,200 closes
# into the one live edge segment's sidecar. Three rounds of REPS interleaved
# reps per engine per host:
#
#   host              engine     clean us/row      corrected us/row   ratio
#   darwin/arm64      pre-fix    0.00139-0.00153   0.01348-0.01470    9.58x-9.79x
#   darwin/arm64      post-fix   0.00150-0.00156   0.00220-0.00226    1.44x-1.48x
#   Linux, 20 threads pre-fix    0.00428-0.00451   0.02389-0.02410    5.34x-5.59x
#   Linux, 20 threads post-fix   0.00417-0.00433   0.00515-0.00521    1.20x-1.23x
#
# and the mechanism, independent of the clock and identical on both hosts:
# `rows_examined` on the corrected arm is 25,200 pre-fix — every stored row,
# because the whole segment fell out of the contiguous extend and into the
# row loop — against 24,000 post-fix, the same count the clean arm examines
# on both engines. The corrected arm was never paying for the 5 % extra rows
# it stores; it was paying for the fast path it lost.
#
# The clean arm does not move: 0.00150-0.00156 post-fix against 0.00139-
# 0.00153 pre-fix on darwin, 0.00417-0.00433 against 0.00428-0.00451 on
# Linux. That is the property that matters most, since it is every
# uncorrected store in the system.
#
# **The residual is real and is the design's own prediction.** The closes do
# not disappear, they move into the range domain: 1,200 uniformly spread
# closes turn one verdict range into 1,201 short ones, and `select_range`
# pays a per-range extend instead of a single one. That is the O(closes) term
# replacing the O(rows) term, which is the whole trade. This fixture is
# deliberately its worst case — uniform spread maximizes fragmentation at a
# given density. The residual is *smaller* on the slower host (1.22x vs
# 1.46x) because the per-range cost is fixed while the per-row baseline is
# not, which is also why the ratio must be allowed more room than one host's
# reading suggests.
#
# MAX_RATIO = 3.0 sits 2.0x above the noisiest healthy rep across both hosts
# (1.48x) and 1.8x below the weakest defect rep across both (5.34x) — within
# a rounding of the geometric midpoint, 2.81x. Two hosts three times apart in
# absolute speed both land inside it with room, and a regression has to close
# most of a 3.6x gap before it slips through.
#
# NOTE, for the record: the design originally set
# MAX_RATIO = 1.5 against a *forecast* of "pre-fix 2-7x,
# post-fix <= 1.3x", and specified the statistic as `nodes_columnar()`. The
# statistic had to change: `nodes_columnar()` never reaches `select_range` at
# all (see the module docstring), so it cannot measure this fix. On the
# statistic that does reach it, the *forecast* is vindicated on the
# Linux host — 1.20x-1.23x, inside the <= 1.3x band —
# but 1.5 is still not a usable *gate*, because a gate has to hold on every
# host that runs it and darwin/arm64 measures 1.44x-1.48x, i.e. 1.01x under
# the limit. The threshold move is a change of gate, not of claim.
MAX_RATIO = 3.0

# A second bound, catching the one failure a ratio cannot see: both arms
# getting slow together. Healthy reps span 0.00220-0.00521 us/row across the
# two hosts and defect reps span 0.01348-0.02410, so the whole usable window
# for an absolute bound is 2.6x wide. 0.010 sits 1.9x above the slowest
# healthy rep and 1.35x below the fastest defect rep.
#
# That is thin in both directions and says so: an absolute per-row cost
# cannot be both generous to a slower host and tight against this defect when
# the two overlap that closely. The ratio above is the gate. The *real*
# second signal is the `rows_examined` assertion at the end of the timed
# test, which is exact, host-independent, and moves only when the verdict
# ranges themselves change.
MAX_ABS_US_PER_ROW = 0.010

N_ROWS = 24_000
ROWS_PER_BATCH = 500
CORRECTION_PCT = 5.0
REPS = 7


def _native_adapter(tmp):
    """The native engine, regardless of GNOMON_TEST_BACKEND (see module doc)."""
    native = pytest.importorskip(
        "gnomon.storage.native",
        reason="the corrected-scan gate measures the native scan path",
    )
    a = native.NativeAdapter(tmp)
    a.paranoid = False  # the paranoid disjointness check is O(versions) per op
    return a


def _edge_ops(vts, mark: int):
    return [
        {"op": "assert_edge", "src": f"s{vt}", "dst": f"d{vt}", "rel_type": "R",
         "props": {"v": vt * mark}, "vt_s": vt, "vt_e": vt + 1}
        for vt in vts
    ]


def _build(tmp: Path, n_rows: int, corr_pct: float):
    """One arm of the A/B: the same interleaved store, corrected or not.

    Generation `g` writes `vt = g, g + n_gens, g + 2*n_gens, ...`, so
    `compact()`'s global `(vt_s, vid)` sort visits every generation in
    rotation. That is the layout `test_compacted_layout_scaling.py` uses and
    the one a streaming ingest actually produces; it is not what carries this
    file's signal (a current-belief scan coalesces every believed run into
    one range regardless), but it keeps the two files' fixtures comparable
    and stops the compacted segment from being unrealistically tidy.

    The corrections re-assert an existing `(src, dst, rel_type, vt)` with new
    props, which is what closes the previous version. `compact()` afterwards
    folds the resulting close runs into the segment's sidecar, so the arm
    measures the folded representation — the one a long-lived store is in.
    """
    a = _native_adapter(tmp)
    n_gens = n_rows // ROWS_PER_BATCH
    assert n_gens >= 8, f"n_rows={n_rows} at rows_per_batch={ROWS_PER_BATCH} gives {n_gens} gens"
    tt = 1000
    for g in range(n_gens):
        tt += 1
        a.begin()
        a.apply_ops(_edge_ops([i * n_gens + g for i in range(ROWS_PER_BATCH)], 1), tt)
        a.commit()
    n_corr = round(n_rows * corr_pct / 100.0)
    if n_corr:
        step = max(1, n_rows // n_corr)
        tt += 1
        a.begin()
        a.apply_ops(_edge_ops(range(0, n_rows, step), -1), tt)
        a.commit()
    a.compact()
    return a


def _folded_closes(store: Path) -> tuple[int, bool]:
    """`(closes folded into sidecars, every live edge segment all_current)`,
    read from the manifest this store currently points at.

    `verify()` reports `closes`, but that counts entries in close *run*
    files, which is zero on both arms once `compact()` has folded them — so
    it cannot tell a corrected store from a clean one, which is exactly the
    distinction this file's fixture-is-real assertion needs. The manifest's
    per-segment `n_closed_folded` / `all_current` can (`manifest.rs`).
    """
    generation = int((store / "CURRENT").read_text().split()[0])
    m = json.loads((store / "manifests" / f"{generation:020d}.json").read_text())
    entries = m["edge_lanes"]["event"] + m["edge_lanes"]["interval"]
    assert entries, f"no live edge segments in generation {generation} of {store}"
    return (
        sum(e["n_closed_folded"] for e in entries),
        all(e["all_current"] for e in entries),
    )


def _scan_stats(adapter) -> dict:
    """The engine's own counters for the projected scan the gate times."""
    from gnomon.core.model import OPEN_END, clamp_tt

    return adapter._store.scan_edges(
        as_of_tt=clamp_tt(OPEN_END), vt_min=None, vt_max=None, rel_types=None,
        touching_ids=None, touching_both=False, limit=None,
        columns=["vt_s", "vt_e"],
    )


# --- 1. the fixture is real: the corrected arm really carries folded closes  #


def test_fixture_produces_a_folded_corrected_segment(tmp_path):
    clean = _build(tmp_path / "clean-fx", n_rows=4_000, corr_pct=0.0)
    assert clean.verify()["healthy"]
    folded, all_current = _folded_closes(tmp_path / "clean-fx")
    assert (folded, all_current) == (0, True), (
        f"the clean arm must stay on the all_current fast path: "
        f"n_closed_folded={folded}, all_current={all_current}"
    )
    clean.close()

    corr = _build(tmp_path / "corr-fx", n_rows=4_000, corr_pct=CORRECTION_PCT)
    report = corr.verify()
    assert report["healthy"], report["problems"]
    assert report["closes"] == 0, (
        f"compact() must fold every close run into a sidecar; "
        f"{report['closes']} are still in run files"
    )
    folded, all_current = _folded_closes(tmp_path / "corr-fx")
    assert folded > 0 and not all_current, (
        f"the corrected fixture produced no folded closes "
        f"(n_closed_folded={folded}, all_current={all_current}) — on a "
        f"store with nothing corrected, every gate in this file would be "
        f"measuring the same thing twice and passing vacuously"
    )
    # and the rows are retained, not deleted: a closed version is still the
    # answer to "what did we believe before the correction?"
    assert report["rows"] > 4_000, report["rows"]
    corr.close()


# --- 2. correctness: a correction changes the answer, not the row set ------ #


def test_the_corrected_arm_answers_with_the_corrected_versions(tmp_path):
    corr = _build(tmp_path / "corr-ans", n_rows=4_000, corr_pct=CORRECTION_PCT)
    cols = corr.edges_columnar(columns=["vt_s", "props"])
    assert len(cols["vt_s"]) == 4_000, (
        "a correction replaces a version rather than adding one: the current "
        f"belief must still be one row per edge, got {len(cols['vt_s'])}"
    )
    # every vt is present exactly once, and the corrected ones carry the
    # correction's props rather than the original's
    props = {int(vt): json.loads(p)["v"] for vt, p in zip(cols["vt_s"], cols["props"])}
    assert len(props) == 4_000
    corrected = set(range(0, 4_000, max(1, int(4_000 / round(4_000 * CORRECTION_PCT / 100)))))
    assert corrected, "the fixture corrected nothing"
    for vt in sorted(corrected)[:20]:
        assert props[vt] == -vt, f"vt {vt} still answers with the superseded version"
    untouched = [v for v in range(4_000) if v not in corrected][:20]
    for vt in untouched:
        assert props[vt] == vt, f"vt {vt} was never corrected but changed"
    corr.close()


# --- 3. the performance gate ----------------------------------------------- #


def _per_row_scan_costs(clean, corr, reps: int) -> tuple[float, float]:
    """Median wall-clock seconds per returned row for both arms.

    Both stores are warmed first — the first scan after opening or compacting
    may build caches (the segment-name map) a cold timing would wrongly
    charge to the scan — and then the reps are **interleaved**, one of each
    per round. The absolute cost here is tens of microseconds, small enough
    that timing one arm to completion and then the other lets host drift
    across the two builds land in the ratio; alternating puts that drift in
    both arms equally. That is an addition to the template's median-of-reps
    mechanism, not a replacement for it.
    """
    for a in (clean, corr):
        for _ in range(3):
            a.edges_columnar(columns=["vt_s", "vt_e"])
    times: dict[str, list[float]] = {"clean": [], "corr": []}
    n: dict[str, int] = {}
    for _ in range(reps):
        for name, a in (("clean", clean), ("corr", corr)):
            start = time.perf_counter()
            cols = a.edges_columnar(columns=["vt_s", "vt_e"])
            times[name].append(time.perf_counter() - start)
            n[name] = len(cols["vt_s"])
    return (
        statistics.median(times["clean"]) / n["clean"],
        statistics.median(times["corr"]) / n["corr"],
    )


def test_a_corrected_store_scans_at_about_the_cost_of_a_clean_one(tmp_path):
    clean = _build(tmp_path / "clean", n_rows=N_ROWS, corr_pct=0.0)
    corr = _build(tmp_path / "corr", n_rows=N_ROWS, corr_pct=CORRECTION_PCT)

    # fixture-is-real, before anything is timed: the two arms must actually
    # differ in the property under test
    assert _folded_closes(tmp_path / "clean") == (0, True)
    folded, all_current = _folded_closes(tmp_path / "corr")
    assert folded > 0 and not all_current, (
        f"the timed corrected arm carries no folded closes "
        f"(n_closed_folded={folded}) — the gate would be vacuous"
    )
    for a in (clean, corr):
        report = a.verify()
        assert report["healthy"], report["problems"]

    clean_stats = _scan_stats(clean)
    corr_stats = _scan_stats(corr)

    clean_per_row, corr_per_row = _per_row_scan_costs(clean, corr, reps=REPS)
    ratio = corr_per_row / clean_per_row

    assert ratio < MAX_RATIO, (
        f"scanning a {CORRECTION_PCT}%-corrected store cost {ratio:.2f}x per "
        f"returned row what scanning the same store uncorrected did (limit "
        f"{MAX_RATIO}x): {clean_per_row * 1e6:.4f}us/row -> "
        f"{corr_per_row * 1e6:.4f}us/row. Both arms hold the same "
        f"{N_ROWS} current edges in one compacted segment and differ only in "
        f"that {folded} of the corrected arm's rows carry a folded close, so "
        f"this is not a size difference — it is the contiguous-extend fast "
        f"path being forfeited because the segment has closes in it, which "
        f"is what moving the `as_of < tt_e` half of the belief predicate "
        f"into the range domain (believed_ranges_closed, close_view) "
        f"removed. rows_examined: clean={clean_stats['rows_examined']}, "
        f"corrected={corr_stats['rows_examined']}."
    )
    assert corr_per_row * 1e6 < MAX_ABS_US_PER_ROW, (
        f"the {CORRECTION_PCT}%-corrected store scanned at "
        f"{corr_per_row * 1e6:.4f}us/row, over the {MAX_ABS_US_PER_ROW}us/row "
        f"bound measured against the fixed engine's 0.0022-0.0052us/row "
        f"across darwin/arm64 and Linux"
    )

    # The mechanism, measured independently of the clock.
    # Pre-fix, the corrected arm examined every stored row, including the
    # superseded ones it was about to skip; post-fix those rows are not in a
    # believed range at all, so the two arms examine the same count.
    assert corr_stats["rows_examined"] <= clean_stats["rows_examined"], (
        f"the corrected arm examined {corr_stats['rows_examined']} rows "
        f"against the clean arm's {clean_stats['rows_examined']}: the closed "
        f"rows are still entering the row loop, i.e. they were not "
        f"subtracted from the verdict ranges"
    )
    assert len(corr_stats["vt_s"]) == len(clean_stats["vt_s"]) == N_ROWS, (
        "both arms must answer with the same current belief, or the two "
        "per-row costs are not comparable"
    )
    clean.close()
    corr.close()
